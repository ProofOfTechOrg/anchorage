// SPDX-License-Identifier: Apache-2.0
// The D1 adapter wraps Mastra's store to pin flowsafe defaults and provide the
// seam where audit export and Queues hooks attach. Table auto-creation
// (CREATE TABLE IF NOT EXISTS)
// happens lazily via Mastra's storage-init proxy once the store is handed
// to `new Mastra({ storage })` — no migration step needed for the runner.

import type { D1Database } from '@cloudflare/workers-types';
import { D1Store } from '@mastra/cloudflare-d1';
import {
  MastraCompositeStore,
  type MastraStorageDomains,
} from '@mastra/core/storage';

import type { D1DatabaseBinding } from './cf-types.js';

export interface D1StorageOptions {
  /** D1 binding from the Worker/DO environment. */
  binding: D1DatabaseBinding;
  /** Storage instance id. Default: 'flowsafe'. */
  id?: string;
  /** Table name prefix (letters, numbers, underscores). */
  tablePrefix?: string;
  /**
   * Additional storage domains composed over the D1Store default, such as
   * notifications and thread state, which @mastra/cloudflare-d1 does not ship, so
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
  /** D1 transactional batch, required when owner lifecycle cleanup is wired. */
  batch?(statements: SnapshotStatement[]): Promise<unknown[]>;
}

export interface SnapshotStatement {
  bind(...values: unknown[]): SnapshotStatement;
  run(): Promise<unknown>;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

/** Structural: R2ArtifactStore.deleteRun, without importing the artifacts module. */
export interface RunArtifactPurger {
  deleteRun(workflowId: string, runId: string): Promise<number>;
}

export interface PurgeExpiredRunsOptions {
  /** workflowOutputTTL: runs untouched for longer than this are eligible. */
  ttlMs: number;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /**
   * When set, each purged run's R2 artifacts are deleted WITH its snapshot
   * row. Hosts that store artifacts must wire this: the snapshot row is the
   * only enumerable record of a run's artifact keys (R2 keys lead with
   * workflowId — there is no run-level listing without it), so a retention
   * purge without this pairing strands the run's artifacts until the
   * deployment itself is decommissioned.
   */
  artifactStore?: RunArtifactPurger;
  /**
   * Deployment-local resource-owner table. When supplied, each snapshot delete
   * and its run-owner release commit in the same D1 transaction. The composed
   * Flowsafe Worker always supplies this; lower-level callers without the
   * resource registry omit it.
   */
  resourceOwnerTable?: string;
  /**
   * Runs processed per call. Artifact-paired path: default 100 — the cron's
   * subrequest-budget guard, same batching as any batched reaper. Each
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
 * declaration against. The guard reads THIS, not a literal copied into the
 * test, so a purge that changes what it targets and a guard that still
 * blesses the old set cannot drift apart silently.
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
  const resourceOwnerTable = options.resourceOwnerTable;
  if (
    resourceOwnerTable !== undefined &&
    !/^[A-Za-z_][A-Za-z0-9_]*$/.test(resourceOwnerTable)
  ) {
    throw new Error('resourceOwnerTable must be a safe SQL identifier');
  }
  const batch = resourceOwnerTable ? db.batch?.bind(db) : undefined;
  if (resourceOwnerTable && !batch) {
    throw new Error(
      'purgeExpiredWorkflowRuns requires database.batch() for atomic owner cleanup',
    );
  }

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
      if (!isMissingTable(error, `${prefix}mastra_workflow_snapshot`))
        throw error;
      return 0;
    }
    let deleted = 0;
    const failures: Array<{ run: string; message: string }> = [];
    for (const row of rows) {
      try {
        await options.artifactStore.deleteRun(row.workflow_name, row.run_id);
      } catch (error) {
        // Isolate per run: aborting the
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
      const deleteSnapshot = db
        .prepare(
          `DELETE FROM ${prefix}mastra_workflow_snapshot
           WHERE workflow_name = ? AND run_id = ? AND ${eligible}`,
        )
        .bind(row.workflow_name, row.run_id, cutoff, ...TERMINAL_STATUSES);
      if (resourceOwnerTable && batch) {
        const [result] = await batch([
          deleteSnapshot,
          db
            .prepare(
              `DELETE FROM ${resourceOwnerTable}
               WHERE resource_kind = 'run' AND resource_id = ?
                 AND NOT EXISTS (
                   SELECT 1 FROM ${prefix}mastra_workflow_snapshot
                   WHERE run_id = ?
                 )`,
            )
            .bind(row.run_id, row.run_id),
        ]);
        deleted += d1Changes(result);
      } else {
        deleted += d1Changes(await deleteSnapshot.run());
      }
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
    if (resourceOwnerTable && batch) {
      // D1 accepts at most 100 bound parameters per statement. Keep the exact
      // rowid + owner-id transaction below under that limit even when a caller
      // requests a larger generic purge batch.
      const limit = Math.min(options.limit ?? 1000, 90);
      const selected = await db
        .prepare(
          `SELECT rowid, run_id
           FROM ${prefix}mastra_workflow_snapshot
           WHERE ${eligible}
           LIMIT ?`,
        )
        .bind(cutoff, ...TERMINAL_STATUSES, limit)
        .all<{ rowid: number; run_id: string }>();
      if (selected.results.length === 0) return 0;
      const rowIds = selected.results.map((row) => row.rowid);
      const runIds = [...new Set(selected.results.map((row) => row.run_id))];
      const rowPlaceholders = rowIds.map(() => '?').join(', ');
      const runPlaceholders = runIds.map(() => '?').join(', ');
      const [result] = await batch([
        db
          .prepare(
            `DELETE FROM ${prefix}mastra_workflow_snapshot
             WHERE rowid IN (${rowPlaceholders}) AND ${eligible}`,
          )
          .bind(...rowIds, cutoff, ...TERMINAL_STATUSES),
        db
          .prepare(
            `DELETE FROM ${resourceOwnerTable}
             WHERE resource_kind = 'run'
               AND resource_id IN (${runPlaceholders})
               AND resource_id NOT IN (
                 SELECT run_id FROM ${prefix}mastra_workflow_snapshot
               )`,
          )
          .bind(...runIds),
      ]);
      return d1Changes(result);
    }
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
    if (!isMissingTable(error, `${prefix}mastra_workflow_snapshot`))
      throw error;
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
 * believed. Both tables are here because the purge reaps a thread and
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
 * Thread-level retention (docs/agent-memory-isolation.md#thread-retention): deletes agent
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
 * decommissioning the deployment's database would still reap them).
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
 * (nothing lists a thread that does not exist) and vanishes with the
 * deployment's database at decommission — a hygiene cost, not a leak.
 *
 * `mastra_resources` (working memory) is deliberately untouched: a resource is
 * the OWNER's, shared across every thread they have, so one thread aging out
 * says nothing about it. It lives until the deployment is decommissioned.
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
 * `background-task-ttl` retention declaration against. It is the background-task
 * counterpart to RUN_TTL_PURGE_TABLES. One table: the TTL rides `completedAt` on
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
 * Background-task TTL cleanup: deletes terminal task
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
    // Internal evented-engine snapshots use the UNSALTED task id. Consume that
    // association before deleting the task rows that make retention
    // ownership discoverable.
    try {
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_workflow_snapshot
           WHERE workflow_name = '__background-task'
             AND run_id IN (
               SELECT id FROM ${table}
               WHERE status = 'completed'
                 AND completedAt IS NOT NULL AND completedAt < ?
             )`,
        )
        .bind(completedCutoff)
        .run();
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_workflow_snapshot
           WHERE workflow_name = '__background-task'
             AND run_id IN (
               SELECT id FROM ${table}
               WHERE status IN (${failedPlaceholders})
                 AND completedAt IS NOT NULL AND completedAt < ?
             )`,
        )
        .bind(...BACKGROUND_TASK_FAILED_STATUSES, failedCutoff)
        .run();
    } catch (error) {
      // A deployment may have task rows before its workflow table is created;
      // no associated snapshots can exist in that case.
      if (!isMissingTable(error)) throw error;
    }
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
 * declaration against. It is the notification counterpart to
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
 * Notification TTL cleanup: deletes terminal agent-inbox
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
 * declaration against.
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
 * Thread-state TTL cleanup: deletes thread-state rows
 * from `mastra_thread_state` once their `updatedAt` is older than the TTL. Keys
 * on `updatedAt` for the same reason `purgeExpiredThreads` does — thread state
 * has no terminal status, so time-since-last-write is the only "done" signal.
 * ISO-8601 TEXT encoding; missing table reads as zero; scheduling stays with the
 * caller. Independent of the thread TTL (an orphan row is reaped here or when
 * the deployment is decommissioned), so it self-bounds without touching the
 * delicate messages-before-threads ordering of purgeExpiredThreads.
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
 * declaration against. It is the schedule-trigger counterpart to
 * THREAD_STATE_TTL_PURGE_TABLES.
 * Only the trigger HISTORY expires; the schedule rows themselves are standing
 * config (retention 'none' — they live until deleted over the schedule
 * surface or the deployment is decommissioned).
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
 * Schedule-trigger TTL cleanup: deletes terminal trigger-history
 * rows from `mastra_schedule_triggers` once their `actualFireAt` is older than the
 * TTL, at the storage layer so a cron reaps them without a live tick — the same
 * posture as the other purges (raw D1 binding, failure-isolated as a cron duty).
 *
 * `actualFireAt` is stored as INTEGER ms-epoch (core types
 * `ScheduleTrigger.actualFireAt` as `number`, unlike the ISO-TEXT timestamp
 * columns the other domains use), so the comparison is a NUMERIC `<` against a
 * numeric cutoff — a correct timestamp comparison over integers. A missing table
 * reads as zero (schedules may never have fired). Scheduling stays with the
 * caller. Deferred rows are live dispatch/reconciliation state and are never
 * TTL-purged; their eventual settlement also finalizes any pending schedule
 * deletion.
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
          `DELETE FROM ${prefix}mastra_schedule_triggers
           WHERE actualFireAt < ? AND outcome <> 'deferred'`,
        )
        .bind(cutoff)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return 0;
  }
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
function isMissingTable(error: unknown, expectedTable?: string): boolean {
  const message = errorMessageOf(error);
  if (!/no such table/i.test(message)) return false;
  return expectedTable === undefined || message.includes(expectedTable);
}
