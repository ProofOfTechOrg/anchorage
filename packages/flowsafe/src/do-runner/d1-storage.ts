// SPDX-License-Identifier: Apache-2.0
// Roadmap Phase 1: "D1 storage adapter (wrap Mastra's)". The wrap pins
// flowsafe defaults and is the seam where audit export / Queues hooks
// attach in later phases. Table auto-creation (CREATE TABLE IF NOT EXISTS)
// happens lazily via Mastra's storage-init proxy once the store is handed
// to `new Mastra({ storage })` — no migration step needed for the runner.

import type { D1Database } from '@cloudflare/workers-types';
import { D1Store } from '@mastra/cloudflare-d1';
import {
  MastraCompositeStore,
  type MastraStorageDomains,
} from '@mastra/core/storage';

import type { D1DatabaseBinding } from './cf-types.js';
import { TENANT_ID_PATTERN } from './path-safe-id.js';

export interface D1StorageOptions {
  /** D1 binding from the Worker/DO environment. */
  binding: D1DatabaseBinding;
  /** Storage instance id. Default: 'flowsafe'. */
  id?: string;
  /** Table name prefix (letters, numbers, underscores). */
  tablePrefix?: string;
  /**
   * Additional storage domains composed OVER the D1Store default (Track C:
   * notifications + thread-state, which @mastra/cloudflare-d1 does not ship, so
   * they are flowsafe-owned D1 impls). Injected rather than imported so this
   * lower layer never depends on `signals/` (which imports do-runner) — build
   * them with `createSignalStorageDomains()` and pass them here. Absent ⇒ the
   * bare D1Store, byte-identical to before this seam existed.
   */
  domains?: MastraStorageDomains;
}

export function createD1Storage(
  options: D1StorageOptions,
): MastraCompositeStore {
  const d1 = new D1Store({
    id: options.id ?? 'flowsafe',
    // @mastra/cloudflare-d1's own D1Store signature wants the real
    // D1Database; D1DatabaseBinding is the structural subset this package
    // exposes instead, so consumers of its shipped types don't need
    // @cloudflare/workers-types installed.
    binding: options.binding as unknown as D1Database,
    ...(options.tablePrefix !== undefined
      ? { tablePrefix: options.tablePrefix }
      : {}),
  });
  // No extra domains ⇒ return the D1Store itself (it IS a MastraCompositeStore),
  // preserving byte-identical behavior for every host that does not opt into
  // signals. With domains, compose them OVER d1 as the default: its own init()
  // (all adapter tables, DDL ordering, coalesced callers) runs first via the
  // parentDefault path, THEN each override domain's init() — the composite never
  // double-inits a parent's domain (validated: chunk #runInit).
  if (!options.domains) return d1;
  return new MastraCompositeStore({
    id: options.id ?? 'flowsafe',
    default: d1,
    domains: options.domains,
  });
}

// Terminal statuses per WorkflowRunStatus (@mastra/core workflows/types).
// Deleting a live run (running/suspended/waiting/pending/paused) would kill
// a pending approval, so only these are ever purged.
const TERMINAL_STATUSES = [
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
] as const;

/**
 * Minimal structural D1 surface the purge uses — same posture as the
 * approval store: tests back it with node:sqlite, Workers pass env.DB.
 */
export interface SnapshotDatabase {
  prepare(query: string): SnapshotStatement;
}

export interface SnapshotStatement {
  bind(...values: unknown[]): SnapshotStatement;
  run(): Promise<unknown>;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

/** Structural: R2ArtifactStore.deleteRun, without importing the artifacts module. */
export interface TenantArtifactPurger {
  deleteRun(workflowId: string, runId: string): Promise<number>;
}

export interface PurgeExpiredRunsOptions {
  /** workflowOutputTTL: runs untouched for longer than this are eligible. */
  ttlMs: number;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /**
   * When set, each purged run's R2 artifacts are deleted WITH its snapshot
   * row. Hosts that store artifacts must pass the same store purgeTenant
   * gets: the snapshot row is the only enumerable record of a run's
   * artifact keys (R2 keys lead with workflowId — there is no run-level
   * listing without it), so a retention purge without this pairing strands
   * the run's artifacts beyond even purgeTenant's reach.
   */
  artifactStore?: TenantArtifactPurger;
  /**
   * Runs processed per call. Artifact-paired path: default 100 — the cron's
   * subrequest-budget guard, same batching as the demo-tenant reaper. Each
   * run costs ~2+N subrequests (R2 list, per-artifact deletes, its row's
   * DELETE), so an UNBOUNDED first backlog would blow the Workers
   * per-invocation cap mid-pass and log an error every firing until it
   * drained; size this to your plan's budget instead. Row-only path:
   * default 1000 — one LIMIT-batched DELETE statement per firing, bounding
   * D1 per-query cost instead of subrequests. Both paths use the shrinking
   * eligible set as the cursor — the next firing resumes at the survivors.
   */
  limit?: number;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * The tables `purgeExpiredWorkflowRuns` deletes from under the run TTL — the
 * production anchor the schema guard cross-checks every `run-ttl` retention
 * declaration against (DL-003), the retention-leg analog of
 * TENANT_RANGE_PURGE_TABLES for the offboarding leg. One table, because the run
 * TTL rides a `run_id` range paired with artifact deletion and an app-owned
 * index (its own block below), not the tenant-range map. The guard reads THIS,
 * not a literal copied into the test, so a purge that changes what it targets
 * and a guard that still blesses the old set cannot drift apart silently.
 */
export const RUN_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_workflow_snapshot',
];

/**
 * Data-retention purge: deletes TERMINAL runs (success/failed/tripwire/
 * canceled/bailed/skipped) whose updatedAt is older than the TTL from
 * mastra_workflow_snapshot — and, when `artifactStore` is wired, each purged
 * run's R2 artifacts with its row. Live runs (running/suspended/waiting/
 * pending/paused) are never touched — expiring a suspended run would kill a
 * pending approval. A missing snapshot table reads as zero purgeable runs
 * (Mastra creates it lazily with the first persisted run). TTL enforcement
 * is a storage-layer property, so it lives here; scheduling (Worker cron /
 * DO alarm) stays with the caller. Returns the number of deleted rows.
 */
export async function purgeExpiredWorkflowRuns(
  db: SnapshotDatabase,
  options: PurgeExpiredRunsOptions,
): Promise<number> {
  const now = options.now ?? Date.now;
  const prefix = options.tablePrefix ?? '';
  // @mastra/cloudflare-d1 stores updatedAt as ISO-8601 TEXT
  // (persistWorkflowSnapshot serializes via toISOString), so lexicographic
  // < against an ISO cutoff is a correct timestamp comparison.
  const cutoff = new Date(now() - options.ttlMs).toISOString();
  const placeholders = TERMINAL_STATUSES.map(() => '?').join(', ');
  // json_extract throws on malformed JSON and would abort the WHOLE delete —
  // one corrupt row must not stop every valid terminal row from being
  // reclaimed. The CASE guard (not `AND json_valid(...)`) is load-bearing:
  // SQLite does not guarantee AND short-circuit order in a WHERE, so a bare
  // conjunct could still evaluate the extract on the bad row. Unclassifiable
  // rows yield NULL and survive (fail safe: never delete what can't be
  // proven terminal).
  const eligible = `updatedAt < ?
         AND CASE
               WHEN json_valid(snapshot)
                 THEN json_extract(snapshot, '$.status')
             END IN (${placeholders})`;

  if (options.artifactStore) {
    // Artifact-paired path, run by run: artifacts BEFORE the row, because
    // the row is the only record of the run's artifact keys — dying between
    // the two leaves the row for the next sweep (deleteRun is idempotent),
    // while row-first would strand the artifacts forever. Each deleted row
    // is durable progress: a mid-pass crash or the LIMIT batch cap resumes
    // at the survivors on the next firing, and per-run failures are
    // isolated below so one wedged run cannot stall the rows behind it.
    let rows: Array<{ workflow_name: string; run_id: string }>;
    try {
      ({ results: rows } = await db
        .prepare(
          `SELECT workflow_name, run_id
           FROM ${prefix}mastra_workflow_snapshot
           WHERE ${eligible}
           LIMIT ?`,
        )
        .bind(cutoff, ...TERMINAL_STATUSES, options.limit ?? 100)
        .all<{ workflow_name: string; run_id: string }>());
    } catch (error) {
      if (!isMissingTable(error)) throw error;
      return 0;
    }
    let deleted = 0;
    const failures: Array<{ run: string; message: string }> = [];
    for (const row of rows) {
      try {
        await options.artifactStore.deleteRun(row.workflow_name, row.run_id);
      } catch (error) {
        // Isolate per run, same as the demo-tenant reaper: aborting the
        // loop would re-hit this run at the same scan position every firing
        // and stall every eligible row behind it forever. Its row survives
        // as its own retry cursor; the aggregate throw below keeps the
        // cron's error surface firing. A wedged run does occupy a batch
        // slot until fixed, so isolation holds while wedged runs number
        // fewer than `limit`.
        failures.push({
          run: `${row.workflow_name}/${row.run_id}`,
          message: errorMessageOf(error),
        });
        continue;
      }
      // Re-checking eligibility keys the delete to the row the SELECT saw;
      // terminal is absorbing, so this is belt-and-braces, not a race fix.
      deleted += d1Changes(
        await db
          .prepare(
            `DELETE FROM ${prefix}mastra_workflow_snapshot
             WHERE workflow_name = ? AND run_id = ? AND ${eligible}`,
          )
          .bind(row.workflow_name, row.run_id, cutoff, ...TERMINAL_STATUSES)
          .run(),
      );
    }
    if (failures.length > 0) {
      throw new Error(
        `purgeExpiredWorkflowRuns: artifact deletion failed for ${failures.length} of ${rows.length} eligible run(s), the rest were purged (${failures
          .map((failure) => `${failure.run}: ${failure.message}`)
          .join('; ')})`,
      );
    }
    return deleted;
  }

  // Row-only path: LIMIT-batched like the artifact path, but against D1's
  // per-QUERY budget rather than the subrequest cap (it stays one statement
  // per firing whatever the batch size, hence the larger default). An
  // unbounded DELETE over a huge first backlog can exceed the per-query
  // limits and then fail the same way on EVERY firing — retention silently
  // unenforced, the exact wedge the cron split exists to avoid. The
  // shrinking eligible set is the cursor: a backlog drains across firings.
  try {
    return d1Changes(
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_workflow_snapshot
           WHERE rowid IN (
             SELECT rowid FROM ${prefix}mastra_workflow_snapshot
             WHERE ${eligible}
             LIMIT ?
           )`,
        )
        .bind(cutoff, ...TERMINAL_STATUSES, options.limit ?? 1000)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return 0;
  }
}

export interface PurgeExpiredThreadsOptions {
  /** Threads untouched for longer than this are eligible, with their messages. */
  ttlMs: number;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /**
   * Threads processed per call. Default 100 — the shrinking eligible set is the
   * cursor, so a first backlog drains across firings instead of blowing one
   * invocation's budget (same batching rationale as purgeExpiredWorkflowRuns).
   */
  limit?: number;
  /** Clock override for tests. */
  now?: () => number;
}

export interface PurgeExpiredThreadsResult {
  threads: number;
  messages: number;
}

// D1 binds at most 100 parameters per query. Each id chunk below shares its
// statement with the cutoff bind of the eligibility re-check, so the chunk
// ceiling must leave room for it — 90 keeps comfortable headroom without
// making the batch loop meaningfully longer.
const D1_BIND_CHUNK = 90;

function chunked<T>(values: readonly T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

/**
 * The tables `purgeExpiredThreads` deletes from under the thread TTL — the
 * production anchor the schema guard cross-checks the `thread-ttl` retention
 * declaration against, AND the target set a `cascade` child must appear in to be
 * believed (DL-003). BOTH tables are here because the purge reaps a thread and
 * its messages together: `mastra_threads` by its own `updatedAt`,
 * `mastra_messages` by cascade (a message has no idleness signal of its own — see
 * the purge doc). So the inventory's `mastra_messages: { retention: cascade with
 * mastra_threads }` is only legal because `mastra_messages` is genuinely a delete
 * target here; a cascade naming a parent whose purge never touches the child is
 * the lie the guard now catches.
 */
export const THREAD_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_threads',
  'mastra_messages',
];

/**
 * Thread-level retention (docs/agent-memory-tenancy.md item 7): deletes agent
 * memory threads whose `updatedAt` is older than the TTL, each with its
 * messages. The terminal-status shape `purgeExpiredWorkflowRuns` uses does not
 * transfer — threads are not per-run and have no status, so there is nothing to
 * prove "finished" about one. Time since last write IS the only signal a thread
 * is done, which is why the TTL keys on `updatedAt` (the D1 memory domain stamps
 * it on every saveThread, so an active conversation never ages out).
 *
 * MESSAGES BEFORE THREADS: a message carries a `createdAt` but no `updatedAt`,
 * so there is no per-message IDLENESS signal — a message's lifetime is its
 * thread's, and its rows are reachable only through it. Deleting a thread first
 * would strand its messages beyond every later firing of this purge (only
 * purgeTenant, at offboarding, still ranges over their salted `thread_id`).
 *
 * EVERY statement re-checks the CURRENT row rather than trusting the SELECT's id
 * list, and that is load-bearing here in a way it is not for
 * purgeExpiredWorkflowRuns (where terminal status is absorbing, so its re-check
 * is belt-and-braces). "Idle" is precisely NOT absorbing — a thread can come
 * back to life mid-purge — and the writer is not atomic either: the memory
 * domain's saveMessages issues its message insert and its `UPDATE mastra_threads
 * SET updatedAt` as two INDEPENDENT calls under one Promise.all, so a send's
 * message can be visible while its thread still reads idle. That TORN state is
 * what each guard answers:
 *
 *  - The message DELETE re-reads the thread's current `updatedAt` (subquery) AND
 *    bounds itself to `createdAt < cutoff`. The subquery alone would sweep a
 *    just-arrived message into the same statement as the genuinely old ones —
 *    it keys on the THREAD's staleness, and the bump has not landed — silently
 *    destroying the message the user just sent. `createdAt` is the message's own
 *    evidence of recency, and the only guard that survives a torn write.
 *  - The thread DELETE re-checks `updatedAt < cutoff` AND fires only when `NOT
 *    EXISTS` any message for it. Sequence alone cannot carry "messages before
 *    threads": a message landing after the message DELETE leaves an idle-looking
 *    thread whose row would go out from under it. The NOT EXISTS makes the
 *    invariant — never delete a thread a message points at — a property of the
 *    statement rather than of timing.
 *
 * Together: a torn or concurrent send at ANY point in the sequence keeps both
 * its message and its thread; the thread's next firing (by then bumped, or by
 * then genuinely idle again) decides its fate on fresh evidence.
 *
 * Residuals, both accepted, neither reachable by a read path: (1) a send landing
 * between the two DELETEs loses the already-expiring history while the thread
 * and the new message survive — strictly better than the deletion that was
 * milliseconds away. (2) A message inserted AFTER its thread's row is already
 * gone is orphaned by the writer, not by this purge; D1 offers no cross-statement
 * transaction through this seam to close it. An orphan is unreachable by recall
 * (nothing lists a thread that does not exist), stays tenant-salted on its
 * `thread_id`, and is reaped by purgeTenant at offboarding — a hygiene cost, not
 * a leak.
 *
 * `mastra_resources` (working memory) is deliberately untouched: a resource is
 * the OWNER's, shared across every thread they have, so one thread aging out
 * says nothing about it. It is reaped at offboarding (purgeTenant).
 *
 * Missing tables read as zero (a deployment with no agent memory never created
 * them). Scheduling stays with the caller — see createFlowsafeWorker's
 * THREAD_RETENTION_DAYS duty.
 */
export async function purgeExpiredThreads(
  db: SnapshotDatabase,
  options: PurgeExpiredThreadsOptions,
): Promise<PurgeExpiredThreadsResult> {
  const now = options.now ?? Date.now;
  const prefix = options.tablePrefix ?? '';
  // @mastra/cloudflare-d1 stores the memory tables' TIMESTAMP columns as
  // ISO-8601 TEXT (the same serialization the snapshot rows use), so a
  // lexicographic < against an ISO cutoff is a correct timestamp comparison.
  const cutoff = new Date(now() - options.ttlMs).toISOString();
  let expired: Array<{ id: string }>;
  try {
    ({ results: expired } = await db
      .prepare(
        `SELECT id FROM ${prefix}mastra_threads
         WHERE updatedAt < ?
         ORDER BY updatedAt
         LIMIT ?`,
      )
      .bind(cutoff, options.limit ?? 100)
      .all<{ id: string }>());
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return { threads: 0, messages: 0 };
  }
  const ids = expired.map((row) => row.id);
  if (ids.length === 0) return { threads: 0, messages: 0 };
  const batches = chunked(ids, D1_BIND_CHUNK);
  const placeholders = (chunk: string[]): string =>
    chunk.map(() => '?').join(', ');
  // The eligibility re-check, as a subquery so the message delete rides the
  // thread's CURRENT updatedAt rather than the SELECT's stale membership.
  const stillExpired = (chunk: string[]): string =>
    `SELECT id FROM ${prefix}mastra_threads
     WHERE id IN (${placeholders(chunk)}) AND updatedAt < ?`;

  let messages = 0;
  let hasMessagesTable = true;
  for (const chunk of batches) {
    try {
      messages += d1Changes(
        await db
          .prepare(
            `DELETE FROM ${prefix}mastra_messages
             WHERE thread_id IN (${stillExpired(chunk)})
               AND createdAt < ?`,
          )
          .bind(...chunk, cutoff, cutoff)
          .run(),
      );
    } catch (error) {
      // A host whose memory domain never initialized has threads but no
      // messages table; anything else is a real failure and must reach the
      // cron's error surface rather than silently orphan a thread's history.
      if (!isMissingTable(error)) throw error;
      hasMessagesTable = false;
      break;
    }
  }
  // The structural invariant: never delete a thread a message still points at
  // (see the header — the writer's insert and its updatedAt bump are not atomic,
  // so updatedAt alone cannot carry this). Dropped only when the table does not
  // exist, where "no message points at it" is vacuously true and the subquery
  // would throw instead.
  const noMessagesLeft = hasMessagesTable
    ? ` AND NOT EXISTS (SELECT 1 FROM ${prefix}mastra_messages
                        WHERE thread_id = ${prefix}mastra_threads.id)`
    : '';
  let threads = 0;
  for (const chunk of batches) {
    threads += d1Changes(
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_threads
           WHERE id IN (${placeholders(chunk)}) AND updatedAt < ?${noMessagesLeft}`,
        )
        .bind(...chunk, cutoff)
        .run(),
    );
  }
  return { threads, messages };
}

/**
 * The table `purgeExpiredBackgroundTasks` deletes from under the background-task
 * TTL — the production anchor the schema guard cross-checks the
 * `background-task-ttl` retention declaration against (DL-003), the Track B
 * analog of RUN_TTL_PURGE_TABLES. One table: the TTL rides `completedAt` on
 * `mastra_background_tasks` alone, no cascade.
 */
export const BACKGROUND_TASK_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_background_tasks',
];

export interface PurgeExpiredBackgroundTasksOptions {
  /**
   * Completed task records untouched for longer than this expire. Default
   * 3_600_000 (1h) — core `BackgroundTaskManager.cleanup`'s `completedTtlMs`.
   */
  completedTtlMs?: number;
  /**
   * Failed / cancelled / timed_out task records older than this expire. Default
   * 86_400_000 (24h) — core's `failedTtlMs`. Kept longer than completed so a
   * failure stays inspectable.
   */
  failedTtlMs?: number;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

export interface PurgeExpiredBackgroundTasksResult {
  /** Deleted 'completed' rows. */
  completed: number;
  /** Deleted 'failed' | 'cancelled' | 'timed_out' rows. */
  failed: number;
}

/** Terminal statuses the failed-class TTL reaps together (core cleanup mirrors this set). */
const BACKGROUND_TASK_FAILED_STATUSES = [
  'failed',
  'cancelled',
  'timed_out',
] as const;

/**
 * Background-task TTL cleanup (DL-003 retention leg): deletes terminal task
 * rows from `mastra_background_tasks` once their `completedAt` is older than the
 * TTL, mirroring core `BackgroundTaskManager.cleanup()` at the storage layer so
 * a purge cron can reap them WITHOUT a live manager — the same posture as
 * purgeExpiredWorkflowRuns/purgeExpiredThreads (raw D1 binding, failure-isolated
 * as a cron duty). Two windows, exactly as core: completed rows expire fast
 * (default 1h), failed/cancelled/timed_out slowly (default 24h) so a failure
 * stays inspectable.
 *
 * `completedAt` is stored as ISO-8601 TEXT (`toISOString()`), so lexicographic
 * `<` against an ISO cutoff is a correct timestamp comparison — the same bet the
 * other purges take. `completedAt IS NOT NULL` is load-bearing: a row without a
 * completion stamp cannot be proven old, so it survives (fail safe). Live rows
 * (pending / running / suspended) are never matched — deleting a suspended task
 * mid-flight would strand its resume. A missing table reads as zero (background
 * tasks may never have run). Scheduling stays with the caller.
 */
export async function purgeExpiredBackgroundTasks(
  db: SnapshotDatabase,
  options: PurgeExpiredBackgroundTasksOptions = {},
): Promise<PurgeExpiredBackgroundTasksResult> {
  const now = options.now ?? Date.now;
  const prefix = options.tablePrefix ?? '';
  const table = `${prefix}mastra_background_tasks`;
  const completedCutoff = new Date(
    now() - (options.completedTtlMs ?? 3_600_000),
  ).toISOString();
  const failedCutoff = new Date(
    now() - (options.failedTtlMs ?? 86_400_000),
  ).toISOString();
  const failedPlaceholders = BACKGROUND_TASK_FAILED_STATUSES.map(
    () => '?',
  ).join(', ');

  try {
    const completed = d1Changes(
      await db
        .prepare(
          `DELETE FROM ${table}
           WHERE status = 'completed'
             AND completedAt IS NOT NULL AND completedAt < ?`,
        )
        .bind(completedCutoff)
        .run(),
    );
    const failed = d1Changes(
      await db
        .prepare(
          `DELETE FROM ${table}
           WHERE status IN (${failedPlaceholders})
             AND completedAt IS NOT NULL AND completedAt < ?`,
        )
        .bind(...BACKGROUND_TASK_FAILED_STATUSES, failedCutoff)
        .run(),
    );
    return { completed, failed };
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return { completed: 0, failed: 0 };
  }
}

/**
 * The table `purgeExpiredNotifications` deletes from under the notification TTL
 * — the anchor the schema guard cross-checks the `notification-ttl` retention
 * declaration against (DL-003), the Track C analog of
 * BACKGROUND_TASK_TTL_PURGE_TABLES.
 */
export const NOTIFICATION_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_notifications',
];

/** Terminal notification statuses the TTL reaps (a delivered/seen/… inbox item is done). */
const NOTIFICATION_TERMINAL_STATUSES = [
  'delivered',
  'seen',
  'dismissed',
  'archived',
  'discarded',
] as const;

export interface PurgeExpiredNotificationsOptions {
  /**
   * Resolved (delivered/seen/dismissed/archived/discarded) notifications whose
   * `updatedAt` is older than this expire. PENDING rows are never touched — one
   * may be waiting on a future `deliverAt`, and reaping it would drop a signal
   * the model has not seen. No default: the caller (createFlowsafeWorker's cron)
   * gates this on an opt-in retention window, since a durable inbox is meant to
   * be readable until the host says otherwise.
   */
  ttlMs: number;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Notification TTL cleanup (DL-003 retention leg): deletes TERMINAL agent-inbox
 * rows from `mastra_notifications` once their `updatedAt` is older than the TTL,
 * at the storage layer so a cron reaps them without a live agent — the same
 * posture as the other purges (raw D1 binding, failure-isolated as a cron duty).
 * `updatedAt` is ISO-8601 TEXT, so lexicographic `<` is a correct timestamp
 * comparison. A missing table reads as zero (notifications may never have been
 * sent). Scheduling stays with the caller.
 */
export async function purgeExpiredNotifications(
  db: SnapshotDatabase,
  options: PurgeExpiredNotificationsOptions,
): Promise<number> {
  const now = options.now ?? Date.now;
  const prefix = options.tablePrefix ?? '';
  const cutoff = new Date(now() - options.ttlMs).toISOString();
  const placeholders = NOTIFICATION_TERMINAL_STATUSES.map(() => '?').join(', ');
  try {
    return d1Changes(
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_notifications
           WHERE status IN (${placeholders}) AND updatedAt < ?`,
        )
        .bind(...NOTIFICATION_TERMINAL_STATUSES, cutoff)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return 0;
  }
}

/**
 * The table `purgeExpiredThreadState` deletes from under the thread-state TTL —
 * the anchor the schema guard cross-checks the `thread-state-ttl` retention
 * declaration against (DL-003).
 */
export const THREAD_STATE_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_thread_state',
];

export interface PurgeExpiredThreadStateOptions {
  /**
   * Thread-state rows (state-signal lanes + goals) whose `updatedAt` is older
   * than this expire. An actively-updated goal or task lane bumps `updatedAt`
   * on every write, so it never ages out; an abandoned thread's state does. No
   * default (opt-in, like the notification and thread TTLs — durable state is
   * kept until the host sets a window).
   */
  ttlMs: number;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Thread-state TTL cleanup (DL-003 retention leg): deletes thread-state rows
 * from `mastra_thread_state` once their `updatedAt` is older than the TTL. Keys
 * on `updatedAt` for the same reason `purgeExpiredThreads` does — thread state
 * has no terminal status, so time-since-last-write is the only "done" signal.
 * ISO-8601 TEXT encoding; missing table reads as zero; scheduling stays with the
 * caller. Independent of the thread TTL (an orphan row is reaped here or at
 * offboarding by purgeTenant), so it self-bounds without touching the delicate
 * messages-before-threads ordering of purgeExpiredThreads.
 */
export async function purgeExpiredThreadState(
  db: SnapshotDatabase,
  options: PurgeExpiredThreadStateOptions,
): Promise<number> {
  const now = options.now ?? Date.now;
  const prefix = options.tablePrefix ?? '';
  const cutoff = new Date(now() - options.ttlMs).toISOString();
  try {
    return d1Changes(
      await db
        .prepare(`DELETE FROM ${prefix}mastra_thread_state WHERE updatedAt < ?`)
        .bind(cutoff)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return 0;
  }
}

/**
 * The table `purgeExpiredScheduleTriggers` deletes from under the trigger-row TTL
 * — the anchor the schema guard cross-checks the `schedule-trigger-ttl` retention
 * declaration against (DL-003), the Track D analog of THREAD_STATE_TTL_PURGE_TABLES.
 * Only the trigger HISTORY expires; the schedule rows themselves are standing
 * config (retention 'none', reaped at offboarding by purgeTenant).
 */
export const SCHEDULE_TRIGGER_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_schedule_triggers',
];

export interface PurgeExpiredScheduleTriggersOptions {
  /**
   * Trigger-history rows whose `actualFireAt` is older than this expire. No
   * default (opt-in, like the notification/thread-state TTLs): a schedule's fire
   * history is inspectable until the host sets a window.
   */
  ttlMs: number;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Schedule-trigger TTL cleanup (DL-003 retention leg): deletes trigger-history
 * rows from `mastra_schedule_triggers` once their `actualFireAt` is older than the
 * TTL, at the storage layer so a cron reaps them without a live tick — the same
 * posture as the other purges (raw D1 binding, failure-isolated as a cron duty).
 *
 * `actualFireAt` is stored as INTEGER ms-epoch (core types
 * `ScheduleTrigger.actualFireAt` as `number`, unlike the ISO-TEXT timestamp
 * columns the other domains use), so the comparison is a NUMERIC `<` against a
 * numeric cutoff — a correct timestamp comparison over integers. A missing table
 * reads as zero (schedules may never have fired). Scheduling stays with the
 * caller. Deleting trigger history never touches a live schedule (its own row is
 * in `mastra_schedules`), so there is no ordering constraint.
 */
export async function purgeExpiredScheduleTriggers(
  db: SnapshotDatabase,
  options: PurgeExpiredScheduleTriggersOptions,
): Promise<number> {
  const now = options.now ?? Date.now;
  const prefix = options.tablePrefix ?? '';
  const cutoff = now() - options.ttlMs;
  try {
    return d1Changes(
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_schedule_triggers WHERE actualFireAt < ?`,
        )
        .bind(cutoff)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return 0;
  }
}

// One CREATE INDEX per (database, prefix) per isolate: purgeTenant runs this,
// and the demo reaper invokes purgeTenant in a LIMIT-batched loop — without
// the memo that is one DDL round-trip to the D1 primary per tenant per cron
// firing (24 no-ops out of 25), plus one on every expired-sandbox re-auth.
// Same clear-on-failure promise memo as D1ApprovalStoreFactory's
// #schemaReady: only SUCCESS memoizes, so a missing snapshot table (this call
// doubles as purgeTenant's table probe) or a transient error retries on the
// next call instead of pinning the database to a dead promise.
const snapshotIndexReady = new WeakMap<
  SnapshotDatabase,
  Map<string, Promise<void>>
>();

/**
 * Additive, app-owned index for the tenant range predicate. It couples us to
 * Mastra's snapshot column name (`run_id`, snake_case — its timestamps are
 * camelCase) — the schema-guard test pins that name so a @mastra/core bump
 * that renames it fails CI, not a purge.
 */
export async function ensureSnapshotRunIdIndex(
  db: SnapshotDatabase,
  tablePrefix = '',
): Promise<void> {
  let prefixes = snapshotIndexReady.get(db);
  if (!prefixes) {
    prefixes = new Map();
    snapshotIndexReady.set(db, prefixes);
  }
  const memo = prefixes;
  let ready = memo.get(tablePrefix);
  if (!ready) {
    ready = Promise.resolve(
      db
        .prepare(
          `CREATE INDEX IF NOT EXISTS idx_${tablePrefix}snapshot_run_id
           ON ${tablePrefix}mastra_workflow_snapshot (run_id)`,
        )
        .run(),
    ).then(
      () => undefined,
      (error: unknown) => {
        memo.delete(tablePrefix);
        throw error;
      },
    );
    memo.set(tablePrefix, ready);
  }
  await ready;
}

export interface PurgeTenantOptions {
  /** MUST satisfy INV-3 (^[a-z0-9]{3,32}$) — see the range note below. */
  tenantId: string;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /**
   * The tenant's R2 artifacts are deleted per surviving run BEFORE the
   * snapshot rows go (deleteRun needs the run list — snapshot rows are the
   * only enumerable record of a run's artifact keys). That is also why the
   * SAME store must ride purgeExpiredWorkflowRuns' `artifactStore`:
   * retention deletes rows this offboarding can then no longer see, so an
   * unpaired retention purge strands those runs' artifacts.
   */
  artifactStore?: TenantArtifactPurger;
}

export interface PurgeTenantResult {
  snapshots: number;
  /** Agent-memory rows (docs/agent-memory-tenancy.md): salted thread ids. */
  threads: number;
  /** Agent-memory rows: deleted by their salted thread_id. */
  messages: number;
  /** Agent-memory rows: salted resource ids (working memory). */
  resources: number;
  /** Track B: background-task rows, ranged over their INV-1 salted `run_id`. */
  backgroundTasks: number;
  /** Track C: agent-inbox rows, ranged over their salted `thread_id`. */
  notifications: number;
  /** Track C: thread-state rows (state lanes + goals), ranged over `thread_id`. */
  threadState: number;
  /** Track D: schedule rows, METADATA-filtered on their `metadata.tenantId`. */
  schedules: number;
  /** Track D: schedule-trigger rows, METADATA-filtered on their `metadata.tenantId`. */
  scheduleTriggers: number;
  approvals: number;
  /** Track E: signal-subscription rows, by the `tenant_id` column (the approvals leg). */
  subscriptions: number;
  artifacts: number;
}

/**
 * The `PurgeTenantResult` field a range-purged table reports into. A track
 * adopting a new `mastra_*` domain (notifications, thread-state, background
 * tasks, schedules) adds its counter HERE, its row to
 * TENANT_RANGE_PURGE_TABLES, and its field to PurgeTenantResult — all in the
 * SAME change (DL-003). The union is what makes that mechanical rather than
 * remembered: a counter with no result field fails the pin below, and a result
 * field with no counter fails the `memory` initializer in purgeTenant.
 */
export type TenantRangePurgeCounter =
  | 'threads'
  | 'messages'
  | 'resources'
  | 'backgroundTasks'
  | 'notifications'
  | 'threadState';

/** One table purgeTenant reaps by the INV-3 `[tid_, tid\x60)` range predicate. */
export interface TenantRangePurgeTable {
  /** The PurgeTenantResult field this table's deleted-row count lands in. */
  counter: TenantRangePurgeCounter;
  /** Unprefixed table name; purgeTenant applies the host's tablePrefix. */
  table: string;
  /**
   * The tenant-salted column the range rides. Thread-keyed tables use their
   * `thread_id` (a minted `${tenantId}_${uuid}`); row-keyed tables use their own
   * salted `id`. Any column whose values are salted ids works — that is the
   * whole reason the ids are salted at mint (docs/agent-memory-tenancy.md).
   */
  column: string;
}

/**
 * The tenant-range purge inventory — EXTENSIBLE by construction (DL-003): each
 * later track appends its adopted table here instead of editing purgeTenant's
 * body, so adoption is one additive row plus the counter/result pair the types
 * force alongside it. Ordering is not load-bearing (the deletes run
 * concurrently; see purgeTenant).
 *
 * `mastra_workflow_snapshot` is deliberately absent: it rides the same range but
 * over `run_id`, paired with artifact deletion and an app-owned index, so it
 * keeps its own block. `mastra_background_tasks` (Track B) rides the range over
 * its own `run_id` — the originating run's INV-1 salted id, always present on a
 * task row — without artifacts or an app index, so it lives here. Tables no
 * feature writes yet (`mastra_scorers`) are absent because nothing salts their
 * ids yet — the schema-guard's inventory pin is what forces that decision when a
 * track adopts one.
 */
export const TENANT_RANGE_PURGE_TABLES: readonly TenantRangePurgeTable[] = [
  { counter: 'messages', table: 'mastra_messages', column: 'thread_id' },
  { counter: 'threads', table: 'mastra_threads', column: 'id' },
  { counter: 'resources', table: 'mastra_resources', column: 'id' },
  {
    counter: 'backgroundTasks',
    table: 'mastra_background_tasks',
    column: 'run_id',
  },
  // Track C — both thread-keyed: their `thread_id` holds the salted memory
  // threadId (`${tenantId}_${uuid}`), so the same exact range applies.
  {
    counter: 'notifications',
    table: 'mastra_notifications',
    column: 'thread_id',
  },
  {
    counter: 'threadState',
    table: 'mastra_thread_state',
    column: 'thread_id',
  },
];

// Compile-time DL-003 pin: every counter must be a PurgeTenantResult field, so
// a track cannot adopt a table whose reaped rows are counted nowhere (an
// offboarding that silently under-reports is how "we purged them" becomes
// unfalsifiable). The reverse direction is enforced by purgeTenant's `memory`
// initializer, which must name every counter in the union.
type AssertTrue<T extends true> = T;
type _EveryPurgeCounterIsReported = AssertTrue<
  TenantRangePurgeCounter extends keyof PurgeTenantResult ? true : false
>;

/**
 * The `PurgeTenantResult` field a METADATA-filtered table reports into. Track D's
 * schedule rows key on slugified ids (`agent_<slug>`/`schedule_<slug>`), NOT
 * tenant-salted ids, so the `[tid_, tid\x60)` range predicate cannot reach them —
 * the tenant lives in `metadata.tenantId` (DL-013). This is the second
 * offboarding KIND alongside the range counter: a track adopting a metadata-keyed
 * domain adds its counter HERE, its row to TENANT_METADATA_PURGE_TABLES, and its
 * PurgeTenantResult field, all in the SAME change (DL-003), pinned the same way.
 */
export type TenantMetadataPurgeCounter = 'schedules' | 'scheduleTriggers';

/** One table purgeTenant reaps by `json_extract(metadata, '$.tenantId') = ?`. */
export interface TenantMetadataPurgeTable {
  /** The PurgeTenantResult field this table's deleted-row count lands in. */
  counter: TenantMetadataPurgeCounter;
  /** Unprefixed table name; purgeTenant applies the host's tablePrefix. */
  table: string;
}

/**
 * The metadata-filtered purge inventory (DL-003) — the offboarding coverage for
 * tables whose tenant is a JSON `metadata.tenantId` (stamped by the schedules
 * facade at create + the tick on every trigger), not a salted id column. Both
 * Track D tables are here: schedule rows and their trigger history. The DELETE is
 * `WHERE json_extract(metadata, '$.tenantId') = ?` (the same SQLite json_extract
 * the run-snapshot purge already uses on `snapshot`), so a NULL/absent metadata
 * never matches — every row our facade/tick writes carries it, so this reaps them
 * all. Ordering is not load-bearing (deletes run concurrently; see purgeTenant).
 */
export const TENANT_METADATA_PURGE_TABLES: readonly TenantMetadataPurgeTable[] =
  [
    { counter: 'schedules', table: 'mastra_schedules' },
    { counter: 'scheduleTriggers', table: 'mastra_schedule_triggers' },
  ];

// Compile-time DL-003 pin (the metadata analog of the range pin above): every
// metadata counter must be a PurgeTenantResult field, so a table whose reaped
// rows are counted nowhere cannot be adopted.
type _EveryMetadataCounterIsReported = AssertTrue<
  TenantMetadataPurgeCounter extends keyof PurgeTenantResult ? true : false
>;

/**
 * Complete tenant offboarding: reaps EVERY tenant-keyed store — snapshot
 * rows of ANY status (a tenant's suspended-and-abandoned runs are never
 * eligible for the terminal-only retention purge at any age), the tenant's
 * agent-memory rows (threads/messages/resources — salted per
 * docs/agent-memory-tenancy.md, so the same range predicate is exact), the
 * tenant's approval records (titles, summaries, payloads, decider
 * identities), and its R2 artifacts.
 *
 * The snapshot predicate is a RANGE over the INV-1 salted runId:
 * `run_id >= '<tid>_' AND run_id < '<tid>' || CHAR(0x60)`. tenantId is
 * re-validated against INV-3 here — not for injection safety (every query is
 * parameter-bound) but for RANGE-EXACTNESS: the predicate selects exactly one
 * tenant only because the charset excludes every character in [0x5F, 0x60]
 * ('_' is 0x5F; its successor 0x60, backtick, closes the range under BINARY
 * collation). 'abc' can never sweep 'abcdefg'.
 *
 * PURGE POLICY (the race): this deletes SUSPENDED rows, and a reviewer
 * approving at that moment would resume against a vanishing row (absorbed:
 * the runtime's pre-check throws before any step re-executes — pinned by the
 * purge-race regression test). Only purge tenants whose tokens have already
 * expired, so no live caller can be mid-resume by construction — or purge at
 * the tenant's own explicit request (the showcase's self-service /demo/reset).
 * That caller, uniquely, can race ITSELF: a run started between the artifact
 * SELECT and the snapshot DELETE is reaped by the DELETE (which re-reads the
 * range) with its artifacts unenumerated, and the three statements are not one
 * transaction — an R2 delete can never join a SQL one — so a mid-purge failure
 * leaves it partially applied. Both are pinned in d1-storage.test.ts. Neither
 * can escape the requester's own tenant: the range predicate is exact.
 */
export async function purgeTenant(
  db: SnapshotDatabase,
  options: PurgeTenantOptions,
): Promise<PurgeTenantResult> {
  const { tenantId } = options;
  if (typeof tenantId !== 'string' || !TENANT_ID_PATTERN.test(tenantId)) {
    throw new Error(
      `purgeTenant: tenantId '${tenantId}' violates INV-3 (^[a-z0-9]{3,32}$) — refusing (the range predicate is only exact over that charset)`,
    );
  }
  const prefix = options.tablePrefix ?? '';
  const lower = `${tenantId}_`;
  const upper = `${tenantId}\x60`;
  // The snapshot table exists only once Mastra lazily creates it for the
  // first persisted run. A tenant that never started one — routine for an
  // expired demo sandbox — has zero snapshots and artifacts, and its
  // approvals below MUST still be reaped: throwing here would wedge every
  // offboarding (the purge cron AND the demo re-auth's inline purge) until
  // some unrelated run initializes the schema.
  let hasSnapshotTable = true;
  try {
    await ensureSnapshotRunIdIndex(db, prefix);
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    hasSnapshotTable = false;
  }

  let artifacts = 0;
  let snapshots = 0;
  if (hasSnapshotTable) {
    // 1. Artifacts first — deleteRun needs the run list the snapshot DELETE
    // would destroy.
    if (options.artifactStore) {
      const { results } = await db
        .prepare(
          `SELECT workflow_name, run_id FROM ${prefix}mastra_workflow_snapshot
           WHERE run_id >= ? AND run_id < ?`,
        )
        .bind(lower, upper)
        .all<{ workflow_name: string; run_id: string }>();
      for (const row of results) {
        artifacts += await options.artifactStore.deleteRun(
          row.workflow_name,
          row.run_id,
        );
      }
    }

    // 2. Snapshots — ANY status.
    snapshots = d1Changes(
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_workflow_snapshot
           WHERE run_id >= ? AND run_id < ?`,
        )
        .bind(lower, upper)
        .run(),
    );
  }

  // 3. Every tenant-range table (TENANT_RANGE_PURGE_TABLES — today the
  // agent-memory rows) — the SAME exact range, over the salted ids the
  // memory-id chokepoint mints (threads/resources by id, messages by their
  // NOT-NULL thread_id; message ids themselves are unsalted by design). The
  // DELETEs run concurrently via Promise.all — child-before-parent order is
  // non-load-bearing (messages range on their own thread_id column, Mastra
  // declares no FK, every table independently re-sweepable after a mid-purge
  // failure in any order), the memory[counter] writes hit distinct keys so
  // there is no shared-state race, each statement keeps its own missing-table
  // tolerance, and a first non-missing-table error still rejects. A missing
  // table reads as empty — the tables exist wherever createD1Storage ran
  // (eager creation), but crafted test databases and non-D1Store hosts may
  // not have them.
  const memory: Record<TenantRangePurgeCounter, number> = {
    threads: 0,
    messages: 0,
    resources: 0,
    backgroundTasks: 0,
    notifications: 0,
    threadState: 0,
  };
  await Promise.all(
    TENANT_RANGE_PURGE_TABLES.map(async ({ counter, table, column }) => {
      try {
        memory[counter] = d1Changes(
          await db
            .prepare(
              `DELETE FROM ${prefix}${table} WHERE ${column} >= ? AND ${column} < ?`,
            )
            .bind(lower, upper)
            .run(),
        );
      } catch (error) {
        if (!isMissingTable(error)) throw error;
      }
    }),
  );

  // 3b. Metadata-filtered tables (Track D schedules + triggers): their tenant is
  // a JSON metadata.tenantId, not a salted id, so they cannot ride the range
  // above. Same posture — concurrent deletes, per-table missing-table tolerance,
  // a first real error still rejects. json_extract on a NULL metadata yields NULL
  // and matches nothing (a row our facade/tick never wrote).
  const metadataCounts: Record<TenantMetadataPurgeCounter, number> = {
    schedules: 0,
    scheduleTriggers: 0,
  };
  await Promise.all(
    TENANT_METADATA_PURGE_TABLES.map(async ({ counter, table }) => {
      try {
        metadataCounts[counter] = d1Changes(
          await db
            .prepare(
              `DELETE FROM ${prefix}${table} WHERE json_extract(metadata, '$.tenantId') = ?`,
            )
            .bind(tenantId)
            .run(),
        );
      } catch (error) {
        if (!isMissingTable(error)) throw error;
      }
    }),
  );

  // 4. Approvals — by the tenant_id column. Hosts without the approval queue
  // have no such table; a missing table is the only tolerated failure.
  let approvals = 0;
  try {
    approvals = d1Changes(
      await db
        .prepare('DELETE FROM flowsafe_approvals WHERE tenant_id = ?')
        .bind(tenantId)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
  }

  // 5. Signal subscriptions (Track E) — the same flowsafe-owned tenant_id-column
  // delete as approvals (NOT a mastra_* table, so not in the ranged/metadata
  // inventories above; retention is 'none' — standing config reaped only here).
  // The table name is the literal signal-providers/subscription-d1.ts owns
  // (SIGNAL_SUBSCRIPTIONS_TABLE); a do-runner import of signals/signal-providers
  // would cycle, so it is hardcoded like flowsafe_approvals. Missing table (a
  // host with no signal providers) is the only tolerated failure.
  let subscriptions = 0;
  try {
    subscriptions = d1Changes(
      await db
        .prepare(
          'DELETE FROM flowsafe_signal_subscriptions WHERE tenant_id = ?',
        )
        .bind(tenantId)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
  }

  return {
    snapshots,
    ...memory,
    ...metadataCounts,
    approvals,
    subscriptions,
    artifacts,
  };
}

/** Rows affected by a D1 write, read from its `{ meta: { changes } }` envelope. */
export function d1Changes(result: unknown): number {
  const changes = (result as { meta?: { changes?: number } } | undefined)?.meta
    ?.changes;
  return typeof changes === 'number' ? changes : 0;
}

function errorMessageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * SQLite/D1's "no such table". For the purges, a table that was never
 * created is an EMPTY table, not an error: Mastra creates the snapshot
 * table lazily with the first persisted run, and hosts without the approval
 * queue never create flowsafe_approvals. Matched on the message because the
 * structural SnapshotDatabase seam carries no error codes (D1 wraps the
 * SQLite text but preserves it).
 */
function isMissingTable(error: unknown): boolean {
  return /no such table/i.test(errorMessageOf(error));
}
