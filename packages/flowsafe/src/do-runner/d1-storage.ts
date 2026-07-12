// SPDX-License-Identifier: Apache-2.0
// Roadmap Phase 1: "D1 storage adapter (wrap Mastra's)". The wrap pins
// flowsafe defaults and is the seam where audit export / Queues hooks
// attach in later phases. Table auto-creation (CREATE TABLE IF NOT EXISTS)
// happens lazily via Mastra's storage-init proxy once the store is handed
// to `new Mastra({ storage })` — no migration step needed for the runner.

import type { D1Database } from '@cloudflare/workers-types';
import { D1Store } from '@mastra/cloudflare-d1';

import type { D1DatabaseBinding } from './cf-types.js';
import { TENANT_ID_PATTERN } from './path-safe-id.js';

export interface D1StorageOptions {
  /** D1 binding from the Worker/DO environment. */
  binding: D1DatabaseBinding;
  /** Storage instance id. Default: 'flowsafe'. */
  id?: string;
  /** Table name prefix (letters, numbers, underscores). */
  tablePrefix?: string;
}

export function createD1Storage(options: D1StorageOptions): D1Store {
  return new D1Store({
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
  approvals: number;
  artifacts: number;
}

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
  if (!TENANT_ID_PATTERN.test(tenantId)) {
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

  // 3. Agent-memory rows — the SAME exact range, over the salted ids the
  // memory-id chokepoint mints (threads/resources by id, messages by their
  // NOT-NULL thread_id; message ids themselves are unsalted by design). The
  // three DELETEs run concurrently via Promise.all — child-before-parent order is
  // non-load-bearing (messages range on their own thread_id column, Mastra
  // declares no FK, every table independently re-sweepable after a mid-purge
  // failure in any order), the memory[counter] writes hit distinct keys so
  // there is no shared-state race, each statement keeps its own missing-table
  // tolerance, and a first non-missing-table error still rejects. A missing
  // table reads as empty — the tables exist wherever createD1Storage ran
  // (eager creation), but crafted test databases and non-D1Store hosts may
  // not have them.
  const memoryPurges = [
    ['messages', 'mastra_messages', 'thread_id'],
    ['threads', 'mastra_threads', 'id'],
    ['resources', 'mastra_resources', 'id'],
  ] as const;
  const memory = { threads: 0, messages: 0, resources: 0 };
  await Promise.all(
    memoryPurges.map(async ([counter, table, column]) => {
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

  return { snapshots, ...memory, approvals, artifacts };
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
