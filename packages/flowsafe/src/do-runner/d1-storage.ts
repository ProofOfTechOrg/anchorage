// Roadmap Phase 1: "D1 storage adapter (wrap Mastra's)". The wrap pins
// flowsafe defaults and is the seam where audit export / Queues hooks
// attach in later phases. Table auto-creation (CREATE TABLE IF NOT EXISTS)
// happens lazily via Mastra's storage-init proxy once the store is handed
// to `new Mastra({ storage })` — no migration step needed for the runner.

import type { D1Database } from '@cloudflare/workers-types';
import { D1Store } from '@mastra/cloudflare-d1';

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
