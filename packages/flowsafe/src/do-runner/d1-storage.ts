// Roadmap Phase 1: "D1 storage adapter (wrap Mastra's)". The wrap pins
// flowsafe defaults and is the seam where audit export / Queues hooks
// attach in later phases. Table auto-creation (CREATE TABLE IF NOT EXISTS)
// happens lazily via Mastra's storage-init proxy once the store is handed
// to `new Mastra({ storage })` — no migration step needed for the runner.

import type { D1Database } from '@cloudflare/workers-types';
import { D1Store } from '@mastra/cloudflare-d1';

import { TENANT_ID_PATTERN } from './path-safe-id.js';

export interface D1StorageOptions {
  /** D1 binding from the Worker/DO environment. */
  binding: D1Database;
  /** Storage instance id. Default: 'flowsafe'. */
  id?: string;
  /** Table name prefix (letters, numbers, underscores). */
  tablePrefix?: string;
}

export function createD1Storage(options: D1StorageOptions): D1Store {
  return new D1Store({
    id: options.id ?? 'flowsafe',
    binding: options.binding,
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

export interface PurgeExpiredRunsOptions {
  /** workflowOutputTTL: runs untouched for longer than this are eligible. */
  ttlMs: number;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Data-retention purge: deletes TERMINAL runs (success/failed/tripwire/
 * canceled/bailed/skipped) whose updatedAt is older than the TTL from
 * mastra_workflow_snapshot. Live runs (running/suspended/waiting/pending/
 * paused) are never touched — expiring a suspended run would kill a pending
 * approval. TTL enforcement is a storage-layer property, so it lives here;
 * scheduling (Worker cron / DO alarm) stays with the caller. Returns the
 * number of deleted rows.
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
  const result = await db
    .prepare(
      `DELETE FROM ${prefix}mastra_workflow_snapshot
       WHERE updatedAt < ?
         AND CASE
               WHEN json_valid(snapshot)
                 THEN json_extract(snapshot, '$.status')
             END IN (${placeholders})`,
    )
    .bind(cutoff, ...TERMINAL_STATUSES)
    .run();
  const changes = (result as { meta?: { changes?: number } } | undefined)?.meta
    ?.changes;
  return typeof changes === 'number' ? changes : 0;
}

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
  await db
    .prepare(
      `CREATE INDEX IF NOT EXISTS idx_${tablePrefix}snapshot_run_id
       ON ${tablePrefix}mastra_workflow_snapshot (run_id)`,
    )
    .run();
}

/** Structural: R2ArtifactStore.deleteRun, without importing the artifacts module. */
export interface TenantArtifactPurger {
  deleteRun(workflowId: string, runId: string): Promise<number>;
}

export interface PurgeTenantOptions {
  /** MUST satisfy INV-3 (^[a-z0-9]{3,32}$) — see the range note below. */
  tenantId: string;
  /** Must match createD1Storage's tablePrefix. */
  tablePrefix?: string;
  /**
   * The tenant's R2 artifacts are deleted per surviving run BEFORE the
   * snapshot rows go (deleteRun needs the run list). Artifacts of runs whose
   * snapshots were already retention-purged are reaped by that purge's own
   * deleteRun pairing, not here.
   */
  artifactStore?: TenantArtifactPurger;
}

export interface PurgeTenantResult {
  snapshots: number;
  approvals: number;
  artifacts: number;
}

/**
 * Complete tenant offboarding: reaps ALL THREE stores — snapshot rows of ANY
 * status (a tenant's suspended-and-abandoned runs are never eligible for the
 * terminal-only retention purge at any age), the tenant's approval records
 * (titles, summaries, payloads, decider identities), and its R2 artifacts.
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
 * expired, so no live caller can be mid-resume by construction.
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
  await ensureSnapshotRunIdIndex(db, prefix);

  // 1. Artifacts first — deleteRun needs the run list the snapshot DELETE
  // would destroy.
  let artifacts = 0;
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
  const snapshotResult = await db
    .prepare(
      `DELETE FROM ${prefix}mastra_workflow_snapshot
       WHERE run_id >= ? AND run_id < ?`,
    )
    .bind(lower, upper)
    .run();

  // 3. Approvals — by the tenant_id column. Hosts without the approval queue
  // have no such table; that is the only tolerated failure.
  let approvals = 0;
  try {
    const approvalResult = await db
      .prepare('DELETE FROM flowsafe_approvals WHERE tenant_id = ?')
      .bind(tenantId)
      .run();
    approvals = d1Changes(approvalResult);
  } catch (error) {
    if (!/no such table/i.test(errorMessageOf(error))) throw error;
  }

  return { snapshots: d1Changes(snapshotResult), approvals, artifacts };
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
