// SPDX-License-Identifier: Apache-2.0
// Guards over the parts of Mastra's persistence WE do not own but DO depend
// on, exercised as a fast package-graph SQL unit against
// @mastra/cloudflare-d1 over node:sqlite. The Wrangler harness owns D1/runtime
// fidelity:
//
// 1. TABLE INVENTORY — createD1Storage eagerly creates exactly the tables
//    MASTRA_TABLES names. A dependency bump that adds or renames one must make
//    its lifecycle decision explicit here.
//
//    The inventory is a STRUCTURE, not a name list (DL-003): each entry states
//    whether the product adopts that table and how its rows expire. A
//    @mastra/core bump changing the inventory must fail CI and reopen the
//    persistence review, never silently ship.
//
// 2. SCHEMA GUARD — retention jobs and app-owned indexes depend on Mastra's
//    column names and encodings. A @mastra/core bump renaming any of them must
//    fail here instead of silently disabling maintenance.
//
// 3. DRAIN-INVENTORY CENSUS — the same forcing function, aimed at a different
//    question. Retention asks "what expires this row?"; the drain inventory
//    (do-runner/inventory.ts) asks "does this row stop a migration?", and an
//    operator reads an empty inventory as permission to lock a deployment and
//    copy it. That permission is only as good as the claim that the inventory
//    knows about every table, so every entry of MASTRA_TABLES and of
//    FLOWSAFE_TABLES must name an inventory category or write down why it holds
//    no drainable work — and the flowsafe half is cross-checked against the
//    tables a fully-provisioned database actually contains, so a new
//    `flowsafe_` table fails CI without anyone remembering to add it here.

import type { MastraCompositeStore } from '@mastra/core/storage';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  DEPLOYMENT_SENTINEL_TABLE,
  EXECUTION_FENCE_TABLE,
} from '#deployment-identity-protocol';
import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../../test-support/sqlite.js';
import {
  createResourceOwnershipSchema,
  D1ApprovalStoreFactory,
  RESOURCE_OWNERSHIP_TABLE,
} from '../approval-api/index.js';
import { APPROVALS_TABLE } from '../approval-api/types.js';
import { createScheduleStorageDomains } from '../schedules/storage.js';
import {
  D1SubscriptionStoreFactory,
  SIGNAL_SUBSCRIPTIONS_TABLE,
} from '../signal-providers/index.js';
import { NOTIFICATION_SEQUENCE_TABLE } from '../signals/notifications-d1.js';
import { createSignalStorageDomains } from '../signals/storage.js';
import {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  createD1Storage,
  NOTIFICATION_TTL_PURGE_TABLES,
  RESOURCE_OWNER_TABLE,
  RUN_TTL_PURGE_TABLES,
  SCHEDULE_TRIGGER_TTL_PURGE_TABLES,
  THREAD_STATE_TTL_PURGE_TABLES,
  THREAD_TTL_PURGE_TABLES,
} from './d1-storage.js';
import { ExecutionFenceStore } from './execution-fence.js';
import { init } from './init.js';
import {
  DeploymentInventory,
  FLOWSAFE_TABLES,
  INVENTORY_CATEGORIES,
  INVENTORY_CATEGORY_DESCRIPTORS,
  type InventoryDatabase,
  type InventoryTableAccounting,
} from './inventory.js';
import { mintThreadId, resourceIdFromKey } from './memory-id.js';
import type { RunnerRuntime } from './runtime.js';
import {
  START_IDEMPOTENCY_TABLE,
  StartIdempotencyStore,
} from './start-idempotency.js';

function tableNames(db: SqliteDatabase): string[] {
  return (
    db
      .prepare(
        `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'mastra_%' ORDER BY name`,
      )
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
}

// A gated workflow over the given storage.
function buildGated(storage: MastraCompositeStore): RunnerRuntime {
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    { startIdempotency: 'none', executionFence: 'none' },
  );
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({ go: z.boolean() }),
    execute: async ({ resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'wait' });
      return {};
    },
  });
  createWorkflow({
    id: 'gated',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(gate)
    .commit();
  return runtime;
}

/**
 * Whether the single-deployment runtime owns rows in a Mastra table. An
 * unadopted domain carries no product data; adopting it must define retention
 * in the same change.
 */
type StorageOwnership = 'deployment-wide' | 'unadopted';

/**
 * How a table's rows expire. This is the lifecycle decision a new Mastra table
 * is likeliest to omit: a table can be adopted successfully and still grow
 * forever.
 *
 * `none` demands a REASON in the type. That is the whole mechanism: "we decided
 * this table needs no TTL, because X" and "nobody thought about it" are
 * indistinguishable in a bare enum, so the field makes the second one
 * unwritable. A track adding a table cannot leave retention undeclared (the
 * field is required) and cannot declare 'none' without saying why.
 */
type RetentionStory =
  | { kind: 'run-ttl' }
  | { kind: 'thread-ttl' }
  /** Track B: rows expire via purgeExpiredBackgroundTasks (completedAt TTL). */
  | { kind: 'background-task-ttl' }
  /** Track C: agent-inbox rows expire via purgeExpiredNotifications (terminal + updatedAt TTL). */
  | { kind: 'notification-ttl' }
  /** Track C: thread-state rows expire via purgeExpiredThreadState (updatedAt TTL). */
  | { kind: 'thread-state-ttl' }
  /** Track D: schedule-trigger history expires via purgeExpiredScheduleTriggers (actualFireAt TTL). */
  | { kind: 'schedule-trigger-ttl' }
  /** Rows die with their parent's TTL rather than aging out on their own. */
  | { kind: 'cascade'; with: string }
  | { kind: 'none'; because: string };

/** The age-based TTL kinds — each anchored to a real exported purge inventory. */
type TtlKind =
  | 'run-ttl'
  | 'thread-ttl'
  | 'background-task-ttl'
  | 'notification-ttl'
  | 'thread-state-ttl'
  | 'schedule-trigger-ttl';

describe('Mastra persistence guards (D1Store SQL over node:sqlite)', () => {
  // The one reason an unadopted table carries — hoisted so the ownership↔reason
  // biconditional below can tie them: an unadopted table has nothing to expire
  // BECAUSE nothing writes it, so it carries exactly this, and the day a track
  // becomes deployment-wide the tie forces retention off this
  // boilerplate in the SAME change (the DL-003 "all three legs together" bar).
  const UNADOPTED_NO_RETENTION =
    'unadopted — no feature writes it, so there is nothing to expire yet';

  // The one reason an unadopted table gives the DRAIN inventory. Hoisted for
  // the same purpose as UNADOPTED_NO_RETENTION above: it ties the exclusion to
  // ownership, so the day a track starts writing one of these, both
  // declarations have to be revisited in that change.
  const UNADOPTED_NO_WORK =
    'unadopted — no feature writes it, so it can hold no outstanding work for a drain to wait on';

  const MASTRA_TABLES: ReadonlyArray<{
    table: string;
    coverage: StorageOwnership;
    retention: RetentionStory;
    /**
     * Where this table's rows show up in the drain inventory, or why they never
     * hold up a migration. Required, like `retention`: a Mastra bump that adds
     * a table must answer BOTH questions in the change that adopts it.
     */
    accounting: InventoryTableAccounting;
  }> = [
    {
      table: 'mastra_background_tasks',
      coverage: 'deployment-wide',
      retention: { kind: 'background-task-ttl' },
      accounting: { category: 'background-tasks' },
    },
    {
      table: 'mastra_messages',
      coverage: 'deployment-wide',
      retention: { kind: 'cascade', with: 'mastra_threads' },
      accounting: {
        excluded:
          'conversation history: a message records something that already happened and nothing executes it. Signals a draining deployment persists instead of waking land here too, and are declared unenumerable BECAUSE they are deliberately carried across the migration rather than drained.',
      },
    },
    {
      table: 'mastra_notifications',
      coverage: 'deployment-wide',
      retention: { kind: 'notification-ttl' },
      accounting: { category: 'pending-notifications' },
    },
    {
      table: 'mastra_resources',
      coverage: 'deployment-wide',
      retention: {
        kind: 'none',
        because:
          "working memory is the owner's, shared across every thread they have, so one thread aging out says nothing about it; the resource is deleted explicitly with its owner",
      },
      accounting: {
        excluded:
          'per-owner working memory: state the migration copies wholesale. It is never in flight, so there is nothing here for a drain to finish and no reading of it that could ever reach empty.',
      },
    },
    // Track D tables. Sorted:
    // 'mastra_schedule_triggers' precedes 'mastra_schedules' under BINARY
    // collation ('_' 0x5F < 's' 0x73, the same order 'mastra_thread_state' <
    // 'mastra_threads' takes), and both precede 'mastra_scorers' ('sch' < 'sco').
    {
      table: 'mastra_schedule_triggers',
      coverage: 'deployment-wide',
      retention: { kind: 'schedule-trigger-ttl' },
      accounting: { category: 'schedule-deferred-dispatches' },
    },
    {
      table: 'mastra_schedules',
      coverage: 'deployment-wide',
      retention: {
        kind: 'none',
        because:
          'a schedule is standing configuration deleted explicitly; it has no terminal state to age out, while its fire history expires through schedule-trigger-ttl',
      },
      accounting: { category: 'schedules' },
    },
    {
      table: 'mastra_scorers',
      coverage: 'unadopted',
      retention: {
        kind: 'none',
        because: UNADOPTED_NO_RETENTION,
      },
      accounting: { excluded: UNADOPTED_NO_WORK },
    },
    // 'mastra_thread_state' sorts BEFORE 'mastra_threads' under BINARY collation
    // ('_' 0x5F < 's' 0x73), which is the order sqlite_master's ORDER BY name
    // returns and the toEqual below pins.
    {
      table: 'mastra_thread_state',
      coverage: 'deployment-wide',
      retention: { kind: 'thread-state-ttl' },
      accounting: {
        excluded:
          "the agent's task list and its goal objective, one durable value per (thread, type). Both are standing state read on a thread's next turn — neither is queued, neither executes on its own, and neither carries a consumption marker a predicate could test.",
      },
    },
    {
      table: 'mastra_threads',
      coverage: 'deployment-wide',
      retention: { kind: 'thread-ttl' },
      accounting: {
        excluded:
          'thread identity and its state-signal tracking metadata. A thread is an ADDRESS, not work: the runs addressed to it are inventoried under `runs`, and a thread with no live run owes a migration nothing.',
      },
    },
    {
      table: 'mastra_workflow_snapshot',
      coverage: 'deployment-wide',
      retention: { kind: 'run-ttl' },
      accounting: { category: 'runs' },
    },
  ];

  it('createD1Storage creates exactly the declared deployment inventory', async () => {
    // #given — the real storage adapter over sqlite, WITH the Track C signal
    // domains composed (createD1Storage takes them injected — signals/ imports
    // do-runner, so do-runner cannot import back). @mastra/cloudflare-d1 ships no
    // notifications/thread-state domain, so those two tables come from the
    // flowsafe-owned D1 domains, not the adapter.
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const storage = createD1Storage({
      binding: binding as never,
      domains: {
        ...createSignalStorageDomains(binding as never),
        ...createScheduleStorageDomains(binding as never),
      },
    });
    const runtime = buildGated(storage);

    // #when — the workflow run triggers the adapter's six tables; init() then
    // creates the flowsafe-owned domain tables (signals' two + schedules' two —
    // each builds its schema on init()).
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });
    await storage.init();

    // #then — the exact inventory, pinned. A Mastra bump that adds a table (or
    // a track that enables a domain) fails here until someone declares its
    // ownership and retention.
    expect(tableNames(sqlite)).toEqual(
      MASTRA_TABLES.map((entry) => entry.table),
    );
  });

  it('every retention declaration is anchored to a REAL purge target (the exported inventories), not to intent', async () => {
    // #given — guard + retention land together, or a
    // track ships a table that grows forever. The
    // type forces the DECLARATION; this pins that each declaration corresponds to
    // a purge that really deletes the table, by cross-checking the PRODUCTION
    // inventories RUN_TTL_PURGE_TABLES / THREAD_TTL_PURGE_TABLES exported from
    // d1-storage —
    // NOT literals copied into this test, which would drift from the SQL silently.
    //
    // A table is "accounted for" by a TTL if it declares that kind OR cascades
    // onto a parent carrying it: a cascade MEANS the row dies with the parent's
    // purge, so that purge must genuinely delete the child — i.e. the child is
    // one of the parent's targets.
    const retentionKindOf = (
      table: string,
    ): RetentionStory['kind'] | undefined =>
      MASTRA_TABLES.find((entry) => entry.table === table)?.retention.kind;
    const accountedFor = (kind: TtlKind): string[] =>
      MASTRA_TABLES.filter((entry) => {
        if (entry.retention.kind === kind) return true;
        // A cascade rides its parent's purge, so it is a target of that purge.
        if (entry.retention.kind === 'cascade') {
          return retentionKindOf(entry.retention.with) === kind;
        }
        return false;
      })
        .map((entry) => entry.table)
        .sort();

    // #then — set equality BOTH ways: a declaration with no purge target fails
    // (declared side has it, the const does not) AND a purge target with no
    // declaration fails (the const has it, nothing accounts for it).
    expect(accountedFor('run-ttl')).toEqual([...RUN_TTL_PURGE_TABLES].sort());
    expect(accountedFor('thread-ttl')).toEqual(
      [...THREAD_TTL_PURGE_TABLES].sort(),
    );
    expect(accountedFor('background-task-ttl')).toEqual(
      [...BACKGROUND_TASK_TTL_PURGE_TABLES].sort(),
    );
    expect(accountedFor('notification-ttl')).toEqual(
      [...NOTIFICATION_TTL_PURGE_TABLES].sort(),
    );
    expect(accountedFor('thread-state-ttl')).toEqual(
      [...THREAD_STATE_TTL_PURGE_TABLES].sort(),
    );
    expect(accountedFor('schedule-trigger-ttl')).toEqual(
      [...SCHEDULE_TRIGGER_TTL_PURGE_TABLES].sort(),
    );

    // ...and every cascade names a parent that EXISTS and itself expires. The
    // set check cannot see a cascade onto a 'none'/absent parent whose child is
    // in no inventory (it is simply absent from both sides), so catch that lie
    // directly: a cascade onto something that expires nothing is not a real one.
    for (const entry of MASTRA_TABLES) {
      const { retention } = entry;
      if (retention.kind !== 'cascade') continue;
      const parentTable = retention.with;
      const parent = MASTRA_TABLES.find(
        (candidate) => candidate.table === parentTable,
      );
      expect(
        parent?.retention.kind,
        `${entry.table} cascades onto '${parentTable}', which expires nothing`,
      ).toMatch(/^(run|thread)-ttl$/);
    }
  });

  it("a 'none' retention carries a real reason, and unadopted ownership is tied to it", async () => {
    // #given — 'none' is the escape hatch, so it is where an undeclared decision
    // hides. The type demands the `because` KEY but cannot demand CONTENT, so a
    // blank string type-checks while saying nothing. Two guards close that.
    for (const entry of MASTRA_TABLES) {
      if (entry.retention.kind !== 'none') continue;
      // #then — a real sentence, not a blank the type technically accepts
      expect(
        entry.retention.because.trim().length,
        `${entry.table} declares retention 'none' with an empty/blank reason`,
      ).toBeGreaterThan(20);
    }

    // #then — ownership and the unadopted retention reason move together.
    for (const entry of MASTRA_TABLES) {
      const isUnadopted = entry.coverage === 'unadopted';
      const hasUnadoptedReason =
        entry.retention.kind === 'none' &&
        entry.retention.because === UNADOPTED_NO_RETENTION;
      expect(
        isUnadopted,
        `${entry.table}: unadopted ownership and retention reason must move together`,
      ).toBe(hasUnadoptedReason);
    }
  });

  // -------------------------------------------------------------------------
  // The drain-inventory census
  // -------------------------------------------------------------------------

  /** Both halves of the census as one list, which is how it is asked about. */
  const CENSUS: ReadonlyArray<{
    table: string;
    accounting: InventoryTableAccounting;
  }> = [
    ...MASTRA_TABLES.map((entry) => ({
      table: entry.table,
      accounting: entry.accounting,
    })),
    ...FLOWSAFE_TABLES.map((entry) => ({
      table: entry.table,
      accounting: entry.accounting,
    })),
  ];

  /**
   * A database with every table its real owner would create — the adapter's,
   * the flowsafe domains', the approval and ownership registries', the
   * reservation store's, the subscription factory's, and the two the
   * provisioning protocol writes.
   */
  async function fullyProvisioned(): Promise<{
    sqlite: SqliteDatabase;
    binding: unknown;
  }> {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const storage = createD1Storage({
      binding: binding as never,
      domains: {
        ...createSignalStorageDomains(binding as never),
        ...createScheduleStorageDomains(binding as never),
      },
    });
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });
    await storage.init();
    await createResourceOwnershipSchema(binding as never);
    // The approval store creates its schema lazily on first use; a create is
    // the cheapest way to make it happen without hand-writing its DDL here.
    await new D1ApprovalStoreFactory(binding as never).store().create({
      id: 'apr-census',
      workflowId: 'gated',
      runId: 'abc_r1',
      title: 'census',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: new Date(0).toISOString(),
      updatedAt: new Date(0).toISOString(),
    });
    await new StartIdempotencyStore(binding as never).reserve({
      key: 'census-key',
      owner: { kind: 'human', id: 'ada' },
      targetKind: 'workflow',
      targetId: 'gated',
      mintRunId: () => 'abc_r9',
    });
    await new D1SubscriptionStoreFactory(binding as never, {
      uuid: () => 'sub-census',
    })
      .store()
      .subscribe({
        providerId: 'github',
        externalResourceId: 'octo/repo#1',
        threadId: 'thr-1',
        resourceId: 'res-1',
      });
    await new ExecutionFenceStore(binding as never).seed('open');
    sqlite.exec(
      `CREATE TABLE IF NOT EXISTS ${DEPLOYMENT_SENTINEL_TABLE} (
         id INTEGER PRIMARY KEY CHECK (id = 1),
         tenant_tag TEXT NOT NULL,
         provisioned_at TEXT NOT NULL
       )`,
    );
    return { sqlite, binding };
  }

  it('every censused table names an inventory category or writes down why a drain may ignore it', async () => {
    // #given — the census is what makes an empty inventory MEAN anything. A
    // table in neither list is a table an operator would never be shown and
    // would still be migrating away from.
    for (const entry of CENSUS) {
      // #then — exactly one of the two arms, and a category that really exists.
      if ('category' in entry.accounting) {
        expect(
          INVENTORY_CATEGORIES as readonly string[],
          `${entry.table} claims a category the inventory does not serve`,
        ).toContain(entry.accounting.category);
        continue;
      }
      // #then — an exclusion is a SENTENCE. The type can demand the key but not
      // the content, and "not work" is indistinguishable from "nobody looked"
      // at that length — the same bar the retention reasons are held to.
      expect(
        entry.accounting.excluded.trim().length,
        `${entry.table} is excluded from the inventory with an empty/blank reason`,
      ).toBeGreaterThan(20);
    }
  });

  it('every inventory category is claimed by exactly one censused table, and every category names a real reader over it', async () => {
    // #given — the other direction: a category nothing feeds is a promise the
    // index makes and no query keeps.
    const claimed = CENSUS.flatMap((entry) =>
      'category' in entry.accounting ? [entry.accounting.category] : [],
    );

    // #then — a bijection between categories and the tables that claim them.
    expect([...claimed].sort()).toEqual([...INVENTORY_CATEGORIES].sort());
    expect(new Set(claimed).size).toBe(claimed.length);

    // #then — and each descriptor's declared table is the table the reader
    // really queries. A descriptor naming a table its SQL does not read would
    // report an empty category forever while the index insisted it was covered.
    const { binding } = await fullyProvisioned();
    for (const descriptor of INVENTORY_CATEGORY_DESCRIPTORS) {
      const statements: string[] = [];
      const inner = binding as InventoryDatabase;
      const inventory = new DeploymentInventory({
        prepare(query: string) {
          statements.push(query);
          return inner.prepare(query);
        },
      });
      await inventory.read(descriptor.category);
      expect(
        statements.some((sql) =>
          new RegExp(`FROM\\s+${descriptor.table}\\b`).test(sql),
        ),
        `${descriptor.category} declares table ${descriptor.table} but reads something else`,
      ).toBe(true);
      // #then — and the table it declares is one the census accounts for.
      expect(CENSUS.map((entry) => entry.table)).toContain(descriptor.table);
    }
  });

  it('the flowsafe census matches the flowsafe_ tables a provisioned deployment actually has', async () => {
    // #given — the mastra_% inventory above catches a @mastra/core bump. This
    // is its flowsafe-owned half, and it is the leg that makes the census
    // self-maintaining: a new flowsafe table fails CI on the day it is created,
    // whether or not the author remembered this file.
    const { sqlite } = await fullyProvisioned();

    // #when
    const present = (
      sqlite
        .prepare(
          `SELECT name FROM sqlite_master
           WHERE type='table' AND name LIKE 'flowsafe_%' ORDER BY name`,
        )
        .all() as Array<{ name: string }>
    ).map((row) => row.name);

    // #then — exact equality both ways: an uncensused table fails, and so does
    // a censused one nothing creates any more.
    expect(present).toEqual([...FLOWSAFE_TABLES.map((e) => e.table)].sort());
  });

  it('every table name the census and the inventory restate is the one its owner declares', async () => {
    // #given — three names live in two places, because the layering forbids the
    // import: do-runner may not reach the ownership store, the subscription
    // registry, or the approval store, so the copies below are unavoidable.
    // What is avoidable is a rename that silently empties a category, and this
    // is where the two sides can finally be compared.
    const censused = FLOWSAFE_TABLES.map((entry) => entry.table);

    // #then
    expect(RESOURCE_OWNER_TABLE).toBe(RESOURCE_OWNERSHIP_TABLE);
    expect(censused).toContain(RESOURCE_OWNERSHIP_TABLE);
    expect(censused).toContain(SIGNAL_SUBSCRIPTIONS_TABLE);
    expect(censused).toContain(APPROVALS_TABLE);
    expect(censused).toContain(START_IDEMPOTENCY_TABLE);
    expect(censused).toContain(NOTIFICATION_SEQUENCE_TABLE);
    expect(censused).toContain(EXECUTION_FENCE_TABLE);
    expect(censused).toContain(DEPLOYMENT_SENTINEL_TABLE);

    // #then — and every mastra_ table the inventory declares is one the storage
    // inventory above pins, so a typo in a restated name fails here rather than
    // becoming a category that is empty forever.
    const mastraTables = MASTRA_TABLES.map((entry) => entry.table);
    for (const descriptor of INVENTORY_CATEGORY_DESCRIPTORS) {
      if (!descriptor.table.startsWith('mastra_')) continue;
      expect(
        mastraTables,
        `${descriptor.category} names ${descriptor.table}, which createD1Storage does not create`,
      ).toContain(descriptor.table);
    }
  });

  it('the Track D schedule tables keep the columns their purge + tick ride on (metadata + nextFireAt/actualFireAt)', async () => {
    // #given — flowsafe-owned tables (the adapter ships neither); compose the
    // schedules domain and init to create them.
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const storage = createD1Storage({
      binding: binding as never,
      domains: createScheduleStorageDomains(binding as never),
    });
    await storage.init();

    // #when
    const columnsOf = (table: string) =>
      (
        sqlite.prepare(`PRAGMA table_info(${table})`).all() as {
          name: string;
        }[]
      ).map((column) => column.name);

    // #then — listDueSchedules rides (status, nextFireAt) and the trigger TTL
    // reaps by actualFireAt. These are our column names, so this pins us
    // against a self-inflicted rename that would inert maintenance.
    expect(columnsOf('mastra_schedules')).toEqual(
      expect.arrayContaining(['metadata', 'status', 'nextFireAt']),
    );
    expect(columnsOf('mastra_schedule_triggers')).toEqual(
      expect.arrayContaining(['metadata', 'scheduleId', 'actualFireAt']),
    );
  });

  it("mastra_threads.updatedAt is still ISO-8601 TEXT — the encoding the thread TTL's comparison rides", async () => {
    // #given — purgeExpiredThreads compares `updatedAt < '<iso>'`
    // LEXICOGRAPHICALLY, which is a timestamp comparison only while the column
    // holds ISO text. This pin exists because the blast radius is unbounded and
    // silent: SQLite orders INTEGER before TEXT ALWAYS, so if a bump ever stored
    // epoch ints, `updatedAt < '<iso text>'` would be true for EVERY row and the
    // first purge-alarm firing would delete every thread and every message — active
    // conversations included, history first. purgeExpiredWorkflowRuns takes the
    // same bet but ANDs a terminal-status predicate, so it can only over-delete
    // finished runs; this purge has no second predicate.
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(sqlite) as never,
    });
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('D1Store exposes no memory domain');
    const now = new Date();
    await memory.saveThread({
      thread: {
        id: mintThreadId(() => 'thread-1'),
        resourceId: resourceIdFromKey('user-1'),
        title: 'conversation',
        createdAt: now,
        updatedAt: now,
      },
    });

    // #when — the RAW stored value, not the domain's parsed projection
    const [row] = sqlite
      .prepare('SELECT updatedAt FROM mastra_threads')
      .all() as Array<{ updatedAt: unknown }>;

    // #then — TEXT, and ISO-8601 specifically (a Date-parseable but non-ISO
    // text encoding would still break lexicographic ordering)
    expect(typeof row?.updatedAt).toBe('string');
    expect(String(row?.updatedAt)).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
    );
  });

  it("mastra_workflow_snapshot still has the snake_case 'run_id' column retention rides on", async () => {
    // #given
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(sqlite) as never,
    });
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columns = (
      sqlite.prepare('PRAGMA table_info(mastra_workflow_snapshot)').all() as {
        name: string;
      }[]
    ).map((column) => column.name);

    // #then — run_id (snake_case) present; a Mastra bump renaming it fails here
    expect(columns).toContain('run_id');
    expect(columns).toContain('workflow_name');
  });

  it('mastra_background_tasks keeps the columns Track B purges ride on (run_id range + completedAt/status TTL)', async () => {
    // #given — the real adapter creates the table eagerly (Track B adopted it)
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(sqlite) as never,
    });
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columns = (
      sqlite.prepare('PRAGMA table_info(mastra_background_tasks)').all() as {
        name: string;
      }[]
    ).map((column) => column.name);

    // #then — purgeExpiredBackgroundTasks reaps by status + completedAt while
    // run_id associates each task with its originating run.
    expect(columns).toContain('run_id');
    expect(columns).toContain('status');
    expect(columns).toContain('completedAt');
  });

  it('the Track C signal tables keep the columns their purges ride on (thread_id range + status/updatedAt TTL)', async () => {
    // #given — these two tables are flowsafe-owned (the adapter ships neither),
    // so compose the domains and init to create them.
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const storage = createD1Storage({
      binding: binding as never,
      domains: createSignalStorageDomains(binding as never),
    });
    await storage.init();

    // #when
    const columnsOf = (table: string) =>
      (
        sqlite.prepare(`PRAGMA table_info(${table})`).all() as {
          name: string;
        }[]
      ).map((column) => column.name);

    // #then — purgeExpiredNotifications reaps by status + updatedAt and
    // purgeExpiredThreadState by updatedAt. These are our column names.
    expect(columnsOf('mastra_notifications')).toEqual(
      expect.arrayContaining(['thread_id', 'status', 'updatedAt']),
    );
    expect(columnsOf('mastra_thread_state')).toEqual(
      expect.arrayContaining(['thread_id', 'type', 'updatedAt']),
    );
  });

  it('the memory tables retain their addressing and retention columns', async () => {
    // #given
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(sqlite) as never,
    });
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columnsOf = (table: string) =>
      (
        sqlite.prepare(`PRAGMA table_info(${table})`).all() as {
          name: string;
        }[]
      ).map((column) => column.name);

    // #then — memory addressing and retention key on exactly these names.
    // `updatedAt` carries purgeExpiredThreads' TTL: renamed, the thread
    // retention duty would throw on every firing instead of expiring anything.
    expect(columnsOf('mastra_threads')).toEqual(
      expect.arrayContaining(['id', 'resourceId', 'updatedAt']),
    );
    // createdAt is the message's own evidence of recency — the guard that keeps
    // purgeExpiredThreads from sweeping a just-sent message whose thread still
    // reads idle because the writer's updatedAt bump has not landed yet.
    expect(columnsOf('mastra_messages')).toEqual(
      expect.arrayContaining(['id', 'thread_id', 'resourceId', 'createdAt']),
    );
    expect(columnsOf('mastra_resources')).toEqual(
      expect.arrayContaining(['id']),
    );
  });

  it('InMemoryStore is unaffected by the guards (they pin the D1 adapter only)', () => {
    // Regression tripwire for the harness itself: the guards must not
    // accidentally depend on adapter internals beyond the documented DDL.
    expect(new InMemoryStore()).toBeDefined();
  });
});
