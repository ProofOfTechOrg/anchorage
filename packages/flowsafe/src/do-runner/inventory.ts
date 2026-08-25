// SPDX-License-Identifier: Apache-2.0
// The deployment drain inventory — the read an operator holds a fence open on
// while they prove this deployment is safe to leave behind.
//
// WHY it exists. The fence (execution-fence.ts) can stop a deployment minting
// work, but stopping is not finishing. A migration only becomes safe at the
// moment nobody can point at a run, an approval, a queued task, or a due
// dispatch that this deployment still owes. Before this module the only way to
// answer that was to name the tables you happened to remember and count them by
// hand — an answer whose failure mode is silence: a table nobody thought of
// holds a suspended run, the migration proceeds, and two deployments resume it.
//
// THE TAXONOMY IS THE CONTRACT, and completeness is the product. What is sold
// here is not a set of queries; it is the claim that these queries are ALL of
// them. That claim cannot be maintained by care, so it is maintained by the
// census: `FLOWSAFE_TABLES` below maps every flowsafe-owned table to either an
// inventory category or a written reason it holds no work, and the census test
// (do-runner/mastra-schema-guard.test.ts) fails CI when a table — ours or a new
// one arriving with a @mastra/core bump — appears in neither list. A future
// author adding a work-holding table therefore cannot ship it silently: they
// must either inventory it or state, in more than twenty characters, why an
// operator draining a deployment may ignore it. The guarantee an operator reads
// off an empty inventory is only ever as good as that obligation.
//
// TWO CLASSES, because a drain proof defined over everything can never pass.
// `work` is what must reach empty: runs, approvals awaiting a decision, queued
// tasks, due dispatches, unsettled reservations. `standing` is configuration
// that ARMS future work — schedules, provider subscriptions — and by design it
// never empties; a deployment with three cron schedules is drainable, and
// demanding otherwise would make the proof unreachable rather than strict.
//
// ZERO SIDE EFFECTS, MECHANICALLY. Every read here is a bare SELECT. Nothing
// calls `#ensureSchema`, because a lazy `CREATE TABLE IF NOT EXISTS` is a WRITE
// and the whole value of this surface is that an operator can run it against a
// deployment they are about to copy without changing what they are copying. A
// missing table is not a fault but an empty category (`missingTableReadsEmpty`,
// shared with the fence and the reservation store) — every table here is
// created lazily by the first feature that uses it, so "absent" and "holds
// nothing" are the same sentence. The database seam this module accepts
// (`InventoryDatabase`) deliberately has no `run()`: the read-only property is
// in the TYPE, not only in the review.
//
// WHY NOT REUSE THE EXISTING READERS. Every domain already has a list method —
// `statusFor`, the run-router's GET, `manager.listTasks`, the schedule store's
// `listTriggers`. None of them is usable here, and not for style reasons: each
// either initializes schema on the way in, or advances a cursor, or reaches a
// Durable Object that would wake and re-arm an alarm. An inventory built on
// them would perturb the very deployment it is measuring. So the queries below
// are new and pure, and each one's predicate is derived from the production
// reader or purge that owns the same notion — the terminal-run SQL is literally
// the retention purge's fragment, the due-notification predicate is
// `listDueNotifications`'s, the deferred-dispatch predicate is the schedule
// store's own dispatch guard — so "finished" cannot come to mean one thing to
// the code that acts and another to the code that certifies.

import {
  DEPLOYMENT_SENTINEL_TABLE,
  EXECUTION_FENCE_TABLE,
} from '#deployment-identity-protocol';
import { APPROVALS_TABLE, OPEN_STATUSES } from '../approval-api/types.js';
import { missingTableReadsEmpty } from './cause-chain.js';
import {
  BACKGROUND_TASK_TERMINAL_STATUSES,
  RESOURCE_OWNER_TABLE,
  RUN_TERMINAL_SNAPSHOT_SQL,
  RUN_TERMINAL_STATUSES,
} from './d1-storage.js';
import { DoStatusError } from './do-status-error.js';
import {
  EXECUTION_FENCE_SUSPEND_KEY,
  type ExecutionFenceState,
} from './execution-fence.js';
import { DUE_NOTIFICATION_SQL } from './notification-predicate.js';
import {
  START_IDEMPOTENCY_TABLE,
  START_RESERVATION_STATES,
} from './start-idempotency.js';
import { validateTablePrefix } from './table-prefix.js';

/**
 * The Mastra-owned tables this module reads, spelled here because they belong
 * to a layer this file may not import from.
 *
 * `mastra_schedules` and `mastra_schedule_triggers` are created by
 * `schedules/schedules-d1.ts`, and `mastra_notifications` by
 * `signals/notifications-d1.ts` — both of which are built ON do-runner, so
 * importing their storage modules would close a cycle and pull their runtime
 * dependencies into the public `./do-runner` graph. The pending-notification
 * predicate is therefore single-homed in the import-free
 * `do-runner/notification-predicate.ts` leaf, which both readers can safely
 * import. The census test can see both sides at once, and crosses every name
 * below against the storage inventory the schema guard pins, so a rename fails
 * there rather than turning a category permanently empty.
 * `mastra_workflow_snapshot` and `mastra_background_tasks` come from the
 * @mastra/cloudflare-d1 adapter and are pinned the same way.
 */
const SNAPSHOT_TABLE = 'mastra_workflow_snapshot';
const BACKGROUND_TASKS_TABLE = 'mastra_background_tasks';
const NOTIFICATIONS_TABLE = 'mastra_notifications';
const SCHEDULES_TABLE = 'mastra_schedules';
const SCHEDULE_TRIGGERS_TABLE = 'mastra_schedule_triggers';

/**
 * The subscription registry's table. Same reason as the block above:
 * `signal-providers/subscription-d1.ts` imports do-runner, so the name travels
 * to the census rather than to this file.
 */
const SIGNAL_SUBSCRIPTIONS_INVENTORY_TABLE = 'flowsafe_signal_subscriptions';

/**
 * Whether a category must reach empty before a deployment may be locked.
 *
 * `work` is outstanding obligation: something is expected to happen and has not
 * happened yet. `standing` is configuration that survives the migration and is
 * reported so an operator can reconcile it on the far side, never so they can
 * wait for it to disappear.
 */
export type InventoryCategoryClass = 'work' | 'standing';

/**
 * Every category name, in the order the index reports them: work first, then
 * standing.
 */
export const INVENTORY_CATEGORIES = [
  'runs',
  'approvals-waiting',
  'schedule-deferred-dispatches',
  'pending-notifications',
  'background-tasks',
  'resource-owners',
  'start-reservations',
  'schedules',
  'signal-subscriptions',
] as const;

/** One inventory category's name. */
export type InventoryCategory = (typeof INVENTORY_CATEGORIES)[number];

/** What the index says about one category, before anything is counted. */
export interface InventoryCategoryDescriptor {
  /** The category to pass as `?category=`. */
  readonly category: InventoryCategory;
  /** Whether a drain must see this category empty. */
  readonly class: InventoryCategoryClass;
  /** The table its rows live in, un-prefixed. */
  readonly table: string;
  /** What a row in this category means, in one sentence. */
  readonly holds: string;
}

/** One row, projected to what an operator needs to act on it. */
export interface InventoryEntry {
  /**
   * The row's primary-key components, in the order the keyset scan visits
   * them. Passing the LAST entry's key back as `cursor` resumes after it.
   */
  readonly key: readonly string[];
  /** Category-specific columns; absent values are omitted rather than nulled. */
  readonly detail: Readonly<Record<string, string | number | boolean>>;
}

/** One page of one category. */
export interface InventoryPage {
  readonly category: InventoryCategory;
  readonly class: InventoryCategoryClass;
  /** The table read, with the deployment's storage prefix applied. */
  readonly table: string;
  readonly entries: readonly InventoryEntry[];
  /**
   * Pass back as `?cursor=` to continue. Absent means this page reached the
   * end of the category as of this read — which, for a `work` category, is the
   * observation a drain proof is built from.
   */
  readonly cursor?: string;
  /**
   * Total rows matching this category, counted on the FIRST page of a sweep
   * only (a caller that is paging has already committed to walking them).
   * Absent on continuation pages, and absent when the table does not exist.
   */
  readonly count?: number;
  /**
   * Category-specific totals taken with `count`, in the same read: the numbers
   * that answer an operator's next question without a second sweep. See
   * `background-tasks` (`fenceSuspended`) and `pending-notifications`
   * (`notDue`).
   */
  readonly totals?: Readonly<Record<string, number>>;
}

/**
 * State that holds work but cannot be counted, declared rather than omitted.
 *
 * An inventory that silently skipped these would be reporting a smaller number
 * than the truth under the same name as a complete one, which is the single
 * most dangerous thing this surface could do. Each entry states what it holds
 * and the BOUND on how long it can stay invisible, so the drain procedure can
 * be built to outlast it.
 */
export interface UnenumerableState {
  /** Stable identifier for the invisible state. */
  readonly name: string;
  /** What it can hold. */
  readonly holds: string;
  /** Why no pure query can see it. */
  readonly because: string;
  /** How long it can stay invisible, and what makes it visible again. */
  readonly bound: string;
}

/** The rule an operator reads an empty inventory under. */
export interface DrainProofContract {
  /**
   * How to interpret point-in-time readings while draining still admits work.
   * Rows can enter or leave, but an empty result cannot over-count work.
   */
  readonly reading: string;
  /** What a proof actually is. */
  readonly proof: string;
  /** The fence states an empty `work` set is reachable from. */
  readonly reachableFrom: readonly ExecutionFenceState[];
  /** How to interpret a post-lock re-sweep and a read taken under the lock. */
  readonly note: string;
}

/** The no-category response: what can be asked for, and how to read the answer. */
export interface InventoryIndex {
  readonly categories: readonly InventoryCategoryDescriptor[];
  readonly unenumerable: readonly UnenumerableState[];
  readonly drainProof: DrainProofContract;
}

/**
 * The database surface an inventory read needs — and NOTHING ELSE.
 *
 * There is no `run()` on this seam, deliberately. Every other store in this
 * package takes a shape that can write, because it writes; this one cannot
 * express a write at all, so "the inventory performs no writes" is a fact about
 * the type rather than a promise about the code. `env.DB` satisfies it
 * structurally, as does a node:sqlite test double.
 */
export interface InventoryDatabase {
  prepare(query: string): InventoryStatement;
}

/** One prepared read. */
export interface InventoryStatement {
  bind(...values: unknown[]): InventoryStatement;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

/** How one flowsafe-owned table is accounted for by the inventory. */
export type InventoryTableAccounting =
  | {
      /** Rows of this table are reported under this category. */
      readonly category: InventoryCategory;
    }
  | {
      /**
       * This table holds no drainable work, and here is why. The census
       * requires more than twenty characters: "not work" and "nobody looked"
       * are indistinguishable at that length, and only one of them is a
       * decision.
       */
      readonly excluded: string;
    };

/** One flowsafe-owned table and its accounting. */
export interface FlowsafeTableEntry {
  /** The un-prefixed table name. */
  readonly table: string;
  /** What the table is for. */
  readonly purpose: string;
  /** Its inventory category, or the reason it has none. */
  readonly accounting: InventoryTableAccounting;
}

/**
 * THE FLOWSAFE TABLE CENSUS. Every `flowsafe_`-prefixed table this package
 * creates, mapped to an inventory category or to a justified exclusion.
 *
 * It exists because the schema guard's `mastra_%` sweep cannot see these: they
 * are not created by `createD1Storage`, and two of them (the deployment
 * sentinel and the fence) are created by the provisioning protocol before any
 * runtime code runs. Without an explicit list, a flowsafe table holding
 * suspended work could be added, purged, documented, and shipped without ever
 * meeting the question this module exists to answer.
 *
 * ADDING A TABLE: add it here in the same change. The census test fails until
 * you do, and it fails again if you claim an exclusion in fewer than twenty
 * characters.
 */
export const FLOWSAFE_TABLES: readonly FlowsafeTableEntry[] = [
  {
    table: APPROVALS_TABLE,
    purpose: 'the durable approval queue — one record per suspended decision',
    accounting: { category: 'approvals-waiting' },
  },
  {
    table: DEPLOYMENT_SENTINEL_TABLE,
    purpose:
      'the ownership sentinel naming the tenant this database belongs to',
    accounting: {
      excluded:
        'deployment identity, not work: one row written once at provisioning, read to prove the bindings match the database. It is COPIED by a migration, never drained.',
    },
  },
  {
    table: EXECUTION_FENCE_TABLE,
    purpose: 'the deployment execution fence — one row holding its state',
    accounting: {
      excluded:
        'the control this inventory is read under. Its single row is the instrument, not a measurement: counting it would make every drain proof fail on the fence that is holding the drain open.',
    },
  },
  {
    table: 'flowsafe_notification_sequence',
    purpose: 'the monotonic insertion ordinal the notification inbox orders by',
    accounting: {
      excluded:
        'a single counter row, not a unit of work. The notifications it orders are inventoried under pending-notifications; the counter itself has nothing to finish and is re-derivable from the inbox.',
    },
  },
  {
    table: RESOURCE_OWNER_TABLE,
    purpose:
      'the resource-ownership registry — who holds a run, thread, resource, or schedule',
    accounting: { category: 'resource-owners' },
  },
  {
    table: SIGNAL_SUBSCRIPTIONS_INVENTORY_TABLE,
    purpose: 'armed provider subscriptions routing external signals to threads',
    accounting: { category: 'signal-subscriptions' },
  },
  {
    table: START_IDEMPOTENCY_TABLE,
    purpose: 'owner-bound start reservations — one row per idempotency key',
    accounting: { category: 'start-reservations' },
  },
];

/** The largest page an inventory read will return, whatever `limit` asks for. */
export const INVENTORY_MAX_LIMIT = 200;

/** The page size used when `limit` is absent. */
export const INVENTORY_DEFAULT_LIMIT = 100;

/** Longest `cursor` string accepted, before it is parsed. */
const MAX_CURSOR_LENGTH = 1_024;

/**
 * Bound parameters D1 accepts in ONE statement. The retention purge caps its
 * own batch against the same limit (d1-storage.ts).
 */
export const D1_MAX_BOUND_PARAMETERS = 100;

/**
 * Bound parameters the run-owner lookup spends on anything OTHER than run ids.
 *
 * Zero today: `resource_kind = 'run'` is a SQL literal, not a bind. It is named
 * rather than assumed because the slice size below is `cap - this`, and a
 * future predicate that binds a value would otherwise push the statement one
 * parameter over the limit with nothing to catch it.
 */
export const RUN_OWNER_FIXED_BINDINGS = 0;

/**
 * Run ids per owner-lookup statement: the full parameter budget, less whatever
 * the statement spends on its own predicate. 100 - 0 = 100, so a maximum page
 * (INVENTORY_MAX_LIMIT = 200) costs exactly two statements.
 */
export const RUN_OWNER_LOOKUP_CHUNK =
  D1_MAX_BOUND_PARAMETERS - RUN_OWNER_FIXED_BINDINGS;

/**
 * A malformed inventory request: an unknown category, an unparseable cursor, or
 * a cursor whose shape does not match the category it was sent to.
 *
 * 400 rather than a silent restart from the beginning. A cursor that quietly
 * resets would make a sweep re-read rows it had already counted and never
 * reach the end, and an operator watching for two consecutive empty sweeps
 * would wait forever without being told why.
 */
export class InvalidInventoryRequestError extends DoStatusError {
  readonly status = 400;
  readonly reason: { readonly code: 'INVALID_INVENTORY_REQUEST' };

  constructor(message: string) {
    super(message);
    this.name = 'InvalidInventoryRequestError';
    this.reason = { code: 'INVALID_INVENTORY_REQUEST' };
  }
}

/** Is this a category the inventory serves? */
export function isInventoryCategory(
  value: unknown,
): value is InventoryCategory {
  return (INVENTORY_CATEGORIES as readonly string[]).includes(value as string);
}

/**
 * The state a pure D1 query cannot see, declared rather than omitted.
 *
 * THE SWEEP BEHIND THIS LIST. Every key a Durable Object in this package writes
 * was checked against the categories above, and all but one is backed by a row
 * a query here already returns:
 *
 *   `flowsafe:run-owner-recovery:v1` — DECLARED below. It is written BEFORE the
 *   D1 owner row, so a crash between the two leaves a run with no D1 record at
 *   all.
 *
 *   `flowsafe:agent-thread-binding:v1` / `flowsafe:agent-run:v1:*` — a thread's
 *   binding to its agent run and that run's own state. Both name a run whose
 *   ownership is RESERVED in D1 BEFORE execution begins
 *   (agent-host/thread-host.ts, durable-object.ts) and settled only AFTER a
 *   summary has persisted. The engine also writes a `running` snapshot as the
 *   run starts, so an executing run is normally reported under `runs` on its own
 *   — but that snapshot lands INSIDE `runtime.start`, after the reservation, and
 *   the unsettled reservation is what covers the gap between the two as well as
 *   any later moment the snapshot is behind the run. `resource-owners` is a WORK
 *   category for exactly that reason: an unsettled reservation means a start
 *   that has not durably settled, whatever the snapshot currently says.
 *   (Pinned behaviourally by inventory.test.ts's ownership-ordering test, which
 *   fails if settlement ever moves ahead of the persisted summary.)
 *
 *   `flowsafe:suspension-deadline:v1` — an armed wake for a run that is
 *   suspended, and therefore nonterminal, and therefore under `runs`.
 *
 *   `flowsafe:maintenance-health:v1`, `flowsafe:maintenance-nonces:v1`,
 *   `flowsafe:maintenance-deadline-cursor:v1` — the maintenance object's own
 *   health, replay protection, and scan position. None is a unit of work: they
 *   describe the sweeper, not what it sweeps.
 */
export const INVENTORY_UNENUMERABLE: readonly UnenumerableState[] = [
  {
    name: 'run-owner-recovery-journal',
    holds:
      'a run whose Durable Object journalled its recovery key but had not yet written its D1 owner row',
    because:
      'the journal lives in the run object own storage; D1 has no row for it yet, so no query over this database can observe it',
    bound:
      '60,000 ms (60 seconds), the RUN_OWNER_RECOVERY_DELAY_MS cadence in do-runner/durable-object.ts: the object reconciles the journal on its next wake, after which the run appears under `runs` (or has finished). Two sweeps at least that far apart therefore cover the window.',
  },
  {
    name: 'persisted-idle-signals',
    holds:
      'signals a draining deployment persisted instead of waking a run for',
    because:
      'a persisted signal is written into agent MEMORY as a message, carrying no consumption marker any predicate could test — it is indistinguishable, in storage, from ordinary conversation history',
    bound:
      'none is needed: these are deliberately NOT drainable. The fence degrades an idle wake to a persist precisely so the signal survives the migration and is picked up by the deployment that takes over, so an operator must not wait for them to disappear.',
  },
];

/**
 * The rule that turns inventory readings into a decision.
 *
 * Stated in the index response, and repeated in the route doc, because every
 * dangerous use of this surface is a use that skips it: one empty sweep is not
 * a proof, and an empty sweep taken under `migration-locked` is not even a
 * measurement of a drain.
 */
export const INVENTORY_DRAIN_PROOF: DrainProofContract = {
  reading:
    'Every reading is a point-in-time observation, not a snapshot: rows can enter or leave work categories while draining admits work. Empty results cannot over-count, and keyset pagination never skips a row that existed before the sweep started.',
  proof:
    'The proof is TWO consecutive full sweeps, at least 60,000 ms (60 seconds) apart — the RUN_OWNER_RECOVERY_DELAY_MS cadence in do-runner/durable-object.ts — in which every `work` category returns no entries. One sweep cannot cover the Durable Object journal window; two, spaced that far, can.',
  reachableFrom: ['draining'],
  note: "The two-sweep proof is taken from 'draining', where work can finish. A host that needs a hard guarantee can re-sweep once after transitioning to 'migration-locked': an empty post-lock sweep is conclusive; a non-empty one means work is still outstanding, either because it entered after the proof or because the lock parked it before it finished. Return to draining and repeat the proof. An inventory read taken under 'migration-locked' measures what the fence parked rather than what the deployment would otherwise be doing.",
};

/** Every category's descriptor, in index order. */
export const INVENTORY_CATEGORY_DESCRIPTORS: readonly InventoryCategoryDescriptor[] =
  [
    {
      category: 'runs',
      class: 'work',
      table: SNAPSHOT_TABLE,
      holds:
        'a workflow or agent run that has not reached a terminal state (cancelled and timed_out count as live until their lifecycle cleanup completes)',
    },
    {
      category: 'approvals-waiting',
      class: 'work',
      table: APPROVALS_TABLE,
      holds:
        'an approval still awaiting a decision — pending, claimed, or escalated',
    },
    {
      category: 'schedule-deferred-dispatches',
      class: 'work',
      table: SCHEDULE_TRIGGERS_TABLE,
      holds:
        'a schedule fire that was claimed and deferred but never dispatched; settled fire history in the same table is excluded',
    },
    {
      category: 'pending-notifications',
      class: 'work',
      table: NOTIFICATIONS_TABLE,
      holds:
        'an agent-inbox notification that is pending and already due for dispatch',
    },
    {
      category: 'background-tasks',
      class: 'work',
      table: BACKGROUND_TASKS_TABLE,
      holds:
        'a background task that has not settled — queued, running, or suspended (including parked by the fence)',
    },
    {
      category: 'resource-owners',
      class: 'work',
      table: RESOURCE_OWNER_TABLE,
      holds:
        'an ownership reservation that was taken but never settled or released',
    },
    {
      category: 'start-reservations',
      class: 'work',
      table: START_IDEMPOTENCY_TABLE,
      holds:
        'an idempotency key that has been reserved or claimed but whose run has not settled',
    },
    {
      category: 'schedules',
      class: 'standing',
      table: SCHEDULES_TABLE,
      holds:
        'a schedule that will arm future work — standing configuration a migration carries, never drains',
    },
    {
      category: 'signal-subscriptions',
      class: 'standing',
      table: SIGNAL_SUBSCRIPTIONS_INVENTORY_TABLE,
      holds:
        'an armed provider subscription routing an external resource to a thread — carried, not drained',
    },
  ];

/** The whole no-category answer, assembled once. */
export const INVENTORY_INDEX: InventoryIndex = {
  categories: INVENTORY_CATEGORY_DESCRIPTORS,
  unenumerable: INVENTORY_UNENUMERABLE,
  drainProof: INVENTORY_DRAIN_PROOF,
};

/** Construction-time options for a deployment inventory. */
export interface DeploymentInventoryOptions {
  /**
   * The same storage table prefix `createD1Storage` and the retention purges
   * take. It applies to the Mastra-owned tables only, exactly as it does
   * there — the flowsafe-owned registries are never prefixed.
   */
  tablePrefix?: string;
  /** Injectable clock, for the due-notification predicate. */
  now?: () => number;
}

/** Per-read options: where to resume, and how many rows to take. */
export interface InventoryReadOptions {
  /** The `key` of the last entry of the previous page, as JSON text. */
  cursor?: string;
  /** Rows to return, clamped to INVENTORY_MAX_LIMIT. */
  limit?: number;
}

/** How one category is scanned, before any row is read. */
interface CategoryQuery {
  /** Primary-key columns, in the order the keyset visits them. */
  readonly key: readonly string[];
  /** `expression AS alias` projections carried into `detail`. */
  readonly detail: readonly string[];
  /** Bindings the `detail` projections need, in order. */
  readonly detailBinds?: readonly unknown[];
  /**
   * Aliases from `detail` that are 1/0 flags, reported as booleans. SQLite has
   * no boolean type, so the projection cannot say so itself.
   */
  readonly flags?: readonly string[];
  /** The category's predicate, with `?` placeholders. */
  readonly where: string;
  /** Bindings for `where`, in order. */
  readonly binds: readonly unknown[];
  /**
   * Extra `SUM(...) AS alias` aggregates taken with the count on the first
   * page. Their predicate is the category's own, so a total here is always a
   * partition of `count` unless its doc says otherwise.
   */
  readonly totals?: readonly string[];
  /** Bindings the `totals` expressions need, in order. */
  readonly totalsBinds?: readonly unknown[];
  /**
   * A WIDER predicate for the aggregate pass, when a category's totals must
   * describe rows the page deliberately excludes — `pending-notifications`
   * pages only what is DUE, but "how many are pending and not due yet" is the
   * question an operator asks next and no amount of paging answers it.
   *
   * Widening the aggregate does NOT widen `count`: a category that sets this
   * must also set `countExpression`, so `count` still means "rows in this
   * category" everywhere. A count that meant one thing for eight categories
   * and something larger for the ninth is a number an operator would read
   * wrong exactly once.
   */
  readonly countWhere?: string;
  /**
   * How `count` is computed when `countWhere` is wider than `where` — a
   * conditional SUM that re-applies the category's own predicate.
   */
  readonly countExpression?: string;
  /** Bindings for `countExpression`, in order. */
  readonly countExpressionBinds?: readonly unknown[];
  /** Bindings for `countWhere`, when it differs. */
  readonly countBinds?: readonly unknown[];
  /**
   * A second, strictly optional read that decorates the page. Used for the run
   * owner annotation, whose table is a different feature's and may not exist —
   * so it is separate from the page query rather than joined into it: a
   * deployment that never wired ownership must still be able to prove it holds
   * no runs.
   */
  readonly annotate?: (
    db: InventoryDatabase,
    entries: readonly MutableInventoryEntry[],
  ) => Promise<void>;
}

/**
 * An entry while it is still being built. Identical to `InventoryEntry` except
 * that `detail` is writable, which is what lets an annotation add a column the
 * page query could not safely join.
 */
interface MutableInventoryEntry {
  readonly key: readonly string[];
  readonly detail: Record<string, string | number | boolean>;
}

type RawRow = Record<string, unknown>;

function placeholdersFor(values: readonly unknown[]): string {
  return values.map(() => '?').join(', ');
}

/**
 * A keyset predicate over an arbitrary number of key columns, written as
 * nested comparisons rather than SQLite's row-value syntax.
 *
 * Row values (`(a, b) > (?, ?)`) would be shorter and SQLite has supported them
 * since 3.15, but the expansion below is what every index in this package is
 * already shaped for and what any SQLite the adapter might sit on understands.
 * The shape is strictly-greater at every level, so a cursor never re-reads its
 * own row and never skips the one after it.
 */
function keysetPredicate(columns: readonly string[]): string {
  const clauses: string[] = [];
  for (let index = 0; index < columns.length; index += 1) {
    const equalities = columns
      .slice(0, index)
      .map((column) => `${column} = ?`)
      .join(' AND ');
    const greater = `${columns[index]} > ?`;
    clauses.push(
      equalities === '' ? greater : `(${equalities} AND ${greater})`,
    );
  }
  return `(${clauses.join(' OR ')})`;
}

/** The bindings `keysetPredicate` expects, for one cursor. */
function keysetBindings(cursor: readonly string[]): unknown[] {
  const binds: unknown[] = [];
  for (let index = 0; index < cursor.length; index += 1) {
    for (let prefix = 0; prefix < index; prefix += 1) {
      binds.push(cursor[prefix]);
    }
    binds.push(cursor[index]);
  }
  return binds;
}

/**
 * Parse a caller's cursor into the key it names, refusing anything that is not
 * exactly one key for THIS category.
 *
 * Refusing rather than ignoring: a cursor from a different category, or one
 * that lost a component, would resume the scan somewhere the caller did not
 * ask for — and a sweep that silently restarts is a sweep that never proves
 * emptiness.
 */
function parseCursor(
  cursor: string | undefined,
  category: InventoryCategory,
  arity: number,
): readonly string[] | undefined {
  if (cursor === undefined || cursor === '') return undefined;
  if (cursor.length > MAX_CURSOR_LENGTH) {
    throw new InvalidInventoryRequestError('inventory cursor exceeds limit');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(cursor);
  } catch {
    throw new InvalidInventoryRequestError(
      `inventory cursor for '${category}' is not valid JSON`,
    );
  }
  if (
    !Array.isArray(parsed) ||
    parsed.length !== arity ||
    !parsed.every((part) => typeof part === 'string')
  ) {
    throw new InvalidInventoryRequestError(
      `inventory cursor for '${category}' must name ${String(arity)} key component(s)`,
    );
  }
  return parsed as readonly string[];
}

function clampLimit(limit: number | undefined): number {
  if (limit === undefined) return INVENTORY_DEFAULT_LIMIT;
  if (!Number.isFinite(limit) || !Number.isInteger(limit) || limit < 1) {
    throw new InvalidInventoryRequestError(
      'inventory limit must be a positive integer',
    );
  }
  return Math.min(limit, INVENTORY_MAX_LIMIT);
}

/**
 * Project one row's non-key columns, dropping anything absent.
 *
 * NULL columns are omitted rather than reported as `null`, the same rule the
 * fence's reading payload follows: a caller reading `runId: null` off a
 * deferred dispatch would have to know that null means "not dispatched" rather
 * than "column missing", and the absent field says it without ambiguity.
 */
function detailOf(
  row: RawRow,
  keys: readonly string[],
  flags: readonly string[],
): Record<string, string | number | boolean> {
  const detail: Record<string, string | number | boolean> = {};
  for (const [column, value] of Object.entries(row)) {
    if (keys.includes(column)) continue;
    if (value === null || value === undefined) continue;
    if (flags.includes(column)) {
      detail[column] = value !== 0;
      continue;
    }
    if (
      typeof value === 'string' ||
      typeof value === 'number' ||
      typeof value === 'boolean'
    ) {
      detail[column] = value;
    }
  }
  return detail;
}

function keyOf(row: RawRow, keys: readonly string[]): readonly string[] {
  return keys.map((column) => String(row[column] ?? ''));
}

/**
 * 1 when a background task's suspend payload carries the fence marker.
 *
 * Written once and used in both the projection and the aggregate so a row
 * flagged on the page and a row counted in the total can never be decided by
 * two different expressions. `json_valid` guards a NULL or malformed payload,
 * which json_extract would otherwise throw on — a tool free to suspend with any
 * payload it likes is a tool that could otherwise wedge this sweep.
 */
function fenceParkedSql(): string {
  return `CASE WHEN json_valid(suspend_payload)
              AND json_extract(suspend_payload, '$."${EXECUTION_FENCE_SUSPEND_KEY}"') IS NOT NULL
            THEN 1 ELSE 0 END`;
}

/**
 * Decorate a page of runs with the principal that owns each one.
 *
 * A SECOND query rather than a join, for two reasons that both point the same
 * way. The ownership registry belongs to the approval layer and a deployment
 * that never wired it has no such table — a join would turn its absence into a
 * failure of the `runs` category, which is the one category a drain proof
 * cannot do without. And the annotation is decoration: an operator deciding
 * whether to lock needs the run, and wants the owner. So a missing table, and
 * only a missing table, degrades to unannotated rows rather than to no rows.
 *
 * Ownership is keyed by run id alone (`resource_kind = 'run'`), so the lookup
 * takes the page's run ids — never an unbounded scan.
 *
 * CHUNKED, because a page can be larger than one statement may bind. A caller
 * asking for `limit=200` (INVENTORY_MAX_LIMIT, a real knob) would otherwise
 * build a 200-parameter `IN` list, which real D1 refuses outright while
 * node:sqlite — with its 32766-variable ceiling — accepts happily. That
 * combination is the worst one available: the suite stays green and the route
 * answers a generic 500 on the deployment an operator is trying to drain.
 */
async function annotateRunOwners(
  db: InventoryDatabase,
  entries: readonly MutableInventoryEntry[],
): Promise<void> {
  if (entries.length === 0) return;
  // key = [workflow_name, run_id]; ownership is keyed on the run id.
  const runIds = entries.map((entry) => entry.key[1] ?? '');
  const owners = new Map<string, string>();
  for (
    let offset = 0;
    offset < runIds.length;
    offset += RUN_OWNER_LOOKUP_CHUNK
  ) {
    const slice = runIds.slice(offset, offset + RUN_OWNER_LOOKUP_CHUNK);
    let rows: RawRow[];
    try {
      ({ results: rows } = await db
        .prepare(
          `SELECT resource_id, owner_kind, owner_id
           FROM ${RESOURCE_OWNER_TABLE}
           WHERE resource_kind = 'run'
             AND resource_id IN (${placeholdersFor(slice)})`,
        )
        .bind(...slice)
        .all<RawRow>());
    } catch (error) {
      // A deployment with no ownership registry has none for ANY slice, so the
      // whole annotation is abandoned rather than the current chunk: continuing
      // would re-throw the same absence once per hundred rows.
      if (missingTableReadsEmpty(error, RESOURCE_OWNER_TABLE)) return;
      throw error;
    }
    for (const row of rows) {
      const kind = row.owner_kind;
      const id = row.owner_id;
      if (typeof kind !== 'string' || typeof id !== 'string') continue;
      owners.set(String(row.resource_id), `${kind}:${id}`);
    }
  }
  for (const entry of entries) {
    const owner = owners.get(entry.key[1] ?? '');
    if (owner !== undefined) entry.detail.owner = owner;
  }
}

/**
 * The deployment's read-only work inventory over one D1 database — the same
 * database its runs, approvals, and fence live in, so what it reports and what
 * the fence governs cannot be two different deployments.
 */
export class DeploymentInventory {
  readonly #db: InventoryDatabase;
  readonly #prefix: string;
  readonly #now: () => number;

  constructor(db: InventoryDatabase, options: DeploymentInventoryOptions = {}) {
    this.#db = db;
    this.#prefix =
      validateTablePrefix(options.tablePrefix, 'tablePrefix') ?? '';
    this.#now = options.now ?? Date.now;
  }

  /**
   * What this deployment can be asked about, what it cannot answer, and the
   * rule for reading an empty answer.
   *
   * A constant, and deliberately not a query: the index is the CONTRACT, and a
   * contract that changed with the data would let an operator conclude that a
   * category they cannot see does not apply to them.
   */
  index(): InventoryIndex {
    return INVENTORY_INDEX;
  }

  /** One page of one category. */
  async read(
    category: InventoryCategory,
    options: InventoryReadOptions = {},
  ): Promise<InventoryPage> {
    const descriptor = INVENTORY_CATEGORY_DESCRIPTORS.find(
      (entry) => entry.category === category,
    );
    if (!descriptor) {
      throw new InvalidInventoryRequestError(
        `unknown inventory category '${String(category)}'`,
      );
    }
    const query = this.#queryFor(category);
    const table = this.#tableFor(descriptor);
    const cursor = parseCursor(options.cursor, category, query.key.length);
    const limit = clampLimit(options.limit);
    const base: Pick<InventoryPage, 'category' | 'class' | 'table'> = {
      category,
      class: descriptor.class,
      table,
    };

    let rows: RawRow[];
    try {
      rows = await this.#page(table, query, cursor, limit);
    } catch (error) {
      // A table nothing has created yet holds nothing. Every table here is made
      // lazily by the first feature that writes it, so the absence is the same
      // observation as an empty scan — and answering it with a fault would make
      // a fresh deployment undrainable.
      if (missingTableReadsEmpty(error, table)) return { ...base, entries: [] };
      throw error;
    }
    const entries: MutableInventoryEntry[] = rows.map((row) => ({
      key: keyOf(row, query.key),
      detail: detailOf(row, query.key, query.flags ?? []),
    }));
    await query.annotate?.(this.#db, entries);
    // A cursor is offered only when the page filled: a short page reached the
    // end of the category as of this read, and that is the observation the
    // drain proof is built on.
    const next =
      entries.length === limit ? entries[entries.length - 1] : undefined;
    const counted =
      cursor === undefined ? await this.#counts(table, query) : undefined;
    return {
      ...base,
      entries,
      ...(next === undefined ? {} : { cursor: JSON.stringify(next.key) }),
      ...(counted === undefined ? {} : counted),
    };
  }

  /** Every category's first page, for a caller sweeping the whole deployment. */
  async sweep(
    options: { limit?: number } = {},
  ): Promise<readonly InventoryPage[]> {
    const pages: InventoryPage[] = [];
    for (const descriptor of INVENTORY_CATEGORY_DESCRIPTORS) {
      pages.push(
        await this.read(descriptor.category, {
          ...(options.limit === undefined ? {} : { limit: options.limit }),
        }),
      );
    }
    return pages;
  }

  #tableFor(descriptor: InventoryCategoryDescriptor): string {
    // The prefix belongs to the Mastra-owned storage domains only — the same
    // split createD1Storage and the retention purges take. A flowsafe registry
    // is never prefixed, so prefixing it here would read an empty category on
    // every prefixed deployment.
    return descriptor.table.startsWith('mastra_')
      ? `${this.#prefix}${descriptor.table}`
      : descriptor.table;
  }

  async #page(
    table: string,
    query: CategoryQuery,
    cursor: readonly string[] | undefined,
    limit: number,
  ): Promise<RawRow[]> {
    const projection = [...query.key, ...query.detail].join(', ');
    const keyset =
      cursor === undefined ? '' : ` AND ${keysetPredicate(query.key)}`;
    const { results } = await this.#db
      .prepare(
        `SELECT ${projection}
         FROM ${table}
         WHERE ${query.where}${keyset}
         ORDER BY ${query.key.join(', ')}
         LIMIT ?`,
      )
      .bind(
        ...(query.detailBinds ?? []),
        ...query.binds,
        ...(cursor === undefined ? [] : keysetBindings(cursor)),
        limit,
      )
      .all<RawRow>();
    return results;
  }

  async #counts(
    table: string,
    query: CategoryQuery,
  ): Promise<Pick<InventoryPage, 'count' | 'totals'> | undefined> {
    const extra = query.totals ?? [];
    // `count` is always "rows in this category", even when the aggregate scans
    // wider: a category that widens `countWhere` re-applies its own predicate
    // through `countExpression`.
    const total = query.countExpression ?? 'COUNT(*)';
    const { results } = await this.#db
      .prepare(
        `SELECT ${total} AS total${extra.length === 0 ? '' : `, ${extra.join(', ')}`}
         FROM ${table}
         WHERE ${query.countWhere ?? query.where}`,
      )
      .bind(
        ...(query.countExpressionBinds ?? []),
        ...(query.totalsBinds ?? []),
        ...(query.countBinds ?? query.binds),
      )
      .all<RawRow>();
    const row = results[0];
    if (row === undefined) return undefined;
    const totals: Record<string, number> = {};
    for (const [column, value] of Object.entries(row)) {
      if (column === 'total') continue;
      totals[column] = typeof value === 'number' ? value : 0;
    }
    return {
      count: typeof row.total === 'number' ? row.total : 0,
      ...(Object.keys(totals).length === 0 ? {} : { totals }),
    };
  }

  #queryFor(category: InventoryCategory): CategoryQuery {
    switch (category) {
      case 'runs':
        return {
          key: ['workflow_name', 'run_id'],
          detail: [
            // The same json_valid guard the retention purge uses, for the same
            // reason: json_extract THROWS on malformed JSON, and one corrupt
            // snapshot must not abort the sweep that is trying to account for
            // the deployment.
            `CASE WHEN json_valid(snapshot)
                  THEN json_extract(snapshot, '$.status') END AS status`,
            'updatedAt',
          ],
          // The predicate is the NEGATION of the retention purge's own terminal
          // fragment, so "finished" means one thing on this deployment. An
          // unclassifiable snapshot (invalid JSON, or valid JSON with no
          // status) counts as WORK: a row nobody can prove is finished is
          // exactly the row a migration must not walk away from.
          where: `CASE WHEN json_valid(snapshot)
                    THEN CASE WHEN (${RUN_TERMINAL_SNAPSHOT_SQL}) THEN 0 ELSE 1 END
                    ELSE 1 END`,
          binds: RUN_TERMINAL_STATUSES,
          annotate: annotateRunOwners,
        };
      case 'approvals-waiting':
        // OPEN_STATUSES, not just 'pending': a claimed or escalated approval is
        // still undecided, and its run is still suspended behind it. The same
        // set the store's open-uniqueness index and decide()'s CAS ride on.
        return {
          key: ['id'],
          detail: ['workflow_id', 'run_id', 'step_key', 'status', 'created_at'],
          where: `status IN (${placeholdersFor(OPEN_STATUSES)})`,
          binds: OPEN_STATUSES,
        };
      case 'schedule-deferred-dispatches':
        // 'deferred' is the ONLY outcome that is still work. The same table
        // holds every settled fire under schedule-trigger retention, and a
        // predicate that matched those would keep this category permanently
        // non-empty and make the drain unprovable. The schedule store's own
        // dispatch reads guard on exactly this value.
        return {
          key: ['id'],
          detail: ['scheduleId', 'runId', 'scheduledFireAt', 'actualFireAt'],
          where: "outcome = 'deferred'",
          binds: [],
        };
      case 'pending-notifications': {
        const now = new Date(this.#now()).toISOString();
        // listDueNotifications' predicate, verbatim in meaning: pending AND
        // (deliverAt or summaryAt has come due). A pending row that is not yet
        // due is NOT work a drain can finish — no dispatch pass will select
        // it — so it stays out of the page and is reported as `notDue`
        // instead. That total also covers pending rows carrying NEITHER
        // timestamp, which no dispatch pass will ever select at all.
        const due = DUE_NOTIFICATION_SQL;
        return {
          key: ['thread_id', 'id'],
          detail: ['source', 'kind', 'priority', 'agentId', 'deliverAt'],
          where: due,
          binds: [now, now],
          // The aggregate scans every PENDING row so `notDue` can exist at
          // all — but `count` re-applies the due predicate, so it still means
          // what it means in every other category: rows in this one.
          countWhere: "status = 'pending'",
          countBinds: [],
          countExpression: `SUM(CASE WHEN ${due} THEN 1 ELSE 0 END)`,
          countExpressionBinds: [now, now],
          totals: [`SUM(CASE WHEN ${due} THEN 0 ELSE 1 END) AS notDue`],
          totalsBinds: [now, now],
        };
      }
      case 'background-tasks':
        // Nonterminal is the complement of what the background-task TTL reaps,
        // so a task this reports as outstanding is exactly a task retention
        // will not touch.
        return {
          key: ['id'],
          detail: [
            'status',
            'tool_name',
            'agent_id',
            'run_id',
            // The distinction that decides an operator's next move: a task
            // parked BY THE FENCE resumes itself when the deployment reopens,
            // while a task suspended by its own tool is waiting for something
            // external and will still be waiting after the migration.
            `${fenceParkedSql()} AS fenceSuspended`,
          ],
          flags: ['fenceSuspended'],
          where: `status NOT IN (${placeholdersFor(BACKGROUND_TASK_TERMINAL_STATUSES)})`,
          binds: BACKGROUND_TASK_TERMINAL_STATUSES,
          totals: [`SUM(${fenceParkedSql()}) AS fenceSuspended`],
        };
      case 'resource-owners':
        // A non-null reservation token is an ownership claim that was taken and
        // never settled — settleReservation is what clears it, and `owner()`
        // deliberately reads only rows where it is already NULL.
        //
        // The token itself is never projected: it is the value a settle races
        // on, and an inventory is a read, not a way to learn one.
        return {
          key: ['resource_kind', 'resource_id'],
          detail: ['owner_kind', 'owner_id'],
          where: 'reservation_token IS NOT NULL',
          binds: [],
        };
      case 'start-reservations': {
        // Everything except 'terminal' — derived from the reservation state
        // list rather than spelled, so a new state joins this category by
        // default instead of vanishing from it.
        const unsettled = START_RESERVATION_STATES.filter(
          (state) => state !== 'terminal',
        );
        return {
          key: ['key'],
          detail: [
            'state',
            'owner_kind',
            'owner_id',
            'target_kind',
            'target_id',
            'run_id',
            // The agent surface's address. Present for agent reservations only,
            // and omitted rather than nulled for workflow ones — a retry of an
            // agent key that cannot be routed to a thread is unreachable, so
            // the field's absence is information.
            'thread_id',
            'updated_at',
          ],
          where: `state IN (${placeholdersFor(unsettled)})`,
          binds: unsettled,
        };
      }
      case 'schedules':
        // Standing configuration: every row, including paused ones. A paused
        // schedule arms nothing today and everything the day it resumes, and a
        // reconciling operator needs to see both.
        return {
          key: ['id'],
          detail: [
            'status',
            'nextFireAt',
            `CASE WHEN json_valid(target)
                  THEN json_extract(target, '$.type') END AS targetType`,
            `CASE WHEN json_valid(target)
                  THEN COALESCE(
                    json_extract(target, '$.workflowId'),
                    json_extract(target, '$.agentId')
                  ) END AS targetId`,
          ],
          where: '1 = 1',
          binds: [],
        };
      case 'signal-subscriptions':
        return {
          key: ['id'],
          detail: [
            'provider_id',
            'thread_id',
            'external_resource_id',
            'subscribed_at',
          ],
          where: '1 = 1',
          binds: [],
        };
    }
  }
}
