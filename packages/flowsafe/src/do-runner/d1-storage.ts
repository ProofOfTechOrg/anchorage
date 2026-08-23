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
import { isPathSafeId } from './path-safe-id.js';
import { START_IDEMPOTENCY_TABLE } from './start-idempotency.js';
import { validateTablePrefix } from './table-prefix.js';

export interface D1StorageOptions {
  /** D1 binding from the Worker/DO environment. */
  binding: D1DatabaseBinding;
  /** Storage instance id. Default: 'flowsafe'. */
  id?: string;
  /** Mastra-compatible SQL identifier prefix of at most 39 characters, or empty. */
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
  const tablePrefix = validateTablePrefix(options.tablePrefix);
  const d1 = new D1Store({
    id: options.id ?? 'flowsafe',
    // @mastra/cloudflare-d1's own D1Store signature wants the real
    // D1Database; D1DatabaseBinding is the structural subset this package
    // exposes instead, so consumers of its shipped types don't need
    // @cloudflare/workers-types installed.
    binding: options.binding as unknown as D1Database,
    ...(tablePrefix !== undefined ? { tablePrefix } : {}),
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

// Mastra workflow terminals plus FlowSafe's lifecycle-owned terminals.
// Deleting a live run (running/suspended/waiting/pending/paused) would kill a
// pending approval, so only these are ever purged.
const TERMINAL_STATUSES = [
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
  'cancelled',
  'timed_out',
] as const;

const DEADLINE_LIVE_STATUSES = [
  'running',
  'waiting',
  'pending',
  'paused',
  'waiting_callback',
  'waiting_signal',
  'retry_wait',
  'suspended',
] as const;

export interface RunDeadlineCandidate {
  workflowId: string;
  runId: string;
  revision: number;
  deadlineAt: number;
}

/** Persistent scan position used to rotate bounded deadline passes. */
export interface RunDeadlineCursor {
  workflowId: string;
  runId: string;
  deadlineAt: number;
}

export interface SweepExpiredRunDeadlinesOptions {
  /** Bounded rows per duty pass. Default 100. */
  limit?: number;
  /** Must satisfy createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  now?: () => number;
  /** Last selected row from the prior pass. */
  cursor?: RunDeadlineCursor;
  /** Persists progress after every selected row, including failed rows. */
  advanceCursor?(cursor: RunDeadlineCursor): Promise<void>;
  /** Routes the CAS through the run's owner Durable Object. */
  transition(candidate: RunDeadlineCandidate, now: number): Promise<void>;
}

/**
 * Read-only deadline enumeration. Every mutation is delegated to the owner DO;
 * timed-out rows whose terminal cleanup is incomplete remain resumable cursors.
 */
export async function sweepExpiredRunDeadlines(
  db: SnapshotDatabase,
  options: SweepExpiredRunDeadlinesOptions,
): Promise<number> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = (options.now ?? Date.now)();
  const limit = options.limit ?? 100;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000) {
    throw new Error('deadline sweep limit must be an integer from 1 to 1000');
  }
  if (options.cursor !== undefined && options.advanceCursor === undefined) {
    throw new Error('deadline sweep cursor requires advanceCursor');
  }
  const cursor = options.cursor;
  if (
    cursor &&
    (!Number.isFinite(cursor.deadlineAt) ||
      typeof cursor.workflowId !== 'string' ||
      typeof cursor.runId !== 'string')
  ) {
    throw new Error('deadline sweep cursor is malformed');
  }
  const livePlaceholders = DEADLINE_LIVE_STATUSES.map(() => '?').join(', ');
  const cursorOrder = cursor
    ? `CASE WHEN (
         deadline_at > ?
         OR (deadline_at = ? AND workflow_name > ?)
         OR (deadline_at = ? AND workflow_name = ? AND run_id > ?)
       ) THEN 0 ELSE 1 END,`
    : '';
  const cursorBindings = cursor
    ? [
        cursor.deadlineAt,
        cursor.deadlineAt,
        cursor.workflowId,
        cursor.deadlineAt,
        cursor.workflowId,
        cursor.runId,
      ]
    : [];
  let rows: Array<{
    workflow_name: string;
    run_id: string;
    revision: number;
    deadline_at: number;
  }>;
  try {
    ({ results: rows } = await db
      .prepare(
        `WITH deadline_candidates AS (
         SELECT workflow_name, run_id,
                CASE
                  WHEN json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".transitionIntent.status') = 'timed_out'
                  THEN json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".transitionIntent.expectedRevision')
                  ELSE json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".revision')
                END AS revision,
                json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".deadlineAt') AS deadline_at
         FROM ${prefix}mastra_workflow_snapshot
         WHERE CASE WHEN json_valid(snapshot) THEN
           (
             json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".deadlineAt') <= ?
             AND NOT EXISTS (
               SELECT 1
               FROM json_each(
                 snapshot,
                 '$.requestContext."flowsafe.runLifecycle".economicOperations'
               ) AS operation
               WHERE json_extract(operation.value, '$.settlementState') = 'disputed'
             )
             AND (
               json_extract(snapshot, '$.status') IN (${livePlaceholders})
               OR (
                 json_extract(snapshot, '$.status') = 'timed_out'
                 AND json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt') IS NULL
               )
               OR (
                 json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".transitionIntent.status') = 'timed_out'
                 AND json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal') IS NULL
               )
             )
           ) ELSE 0 END
         )
         SELECT workflow_name, run_id, revision, deadline_at
         FROM deadline_candidates
         ORDER BY ${cursorOrder} deadline_at, workflow_name, run_id
         LIMIT ?`,
      )
      .bind(now, ...DEADLINE_LIVE_STATUSES, ...cursorBindings, limit)
      .all<{
        workflow_name: string;
        run_id: string;
        revision: number;
        deadline_at: number;
      }>());
  } catch (error) {
    if (!isMissingTable(error, `${prefix}mastra_workflow_snapshot`))
      throw error;
    return 0;
  }
  const failures: string[] = [];
  let processed = 0;
  for (const row of rows) {
    const rowCursor = {
      workflowId: row.workflow_name,
      runId: row.run_id,
      deadlineAt: row.deadline_at,
    };
    const malformed =
      !isPathSafeId(row.workflow_name) ||
      !isPathSafeId(row.run_id) ||
      !Number.isSafeInteger(row.revision) ||
      row.revision < 1 ||
      !Number.isSafeInteger(row.deadline_at) ||
      row.deadline_at < 0;
    if (malformed) {
      failures.push(`${row.workflow_name}/${row.run_id}: malformed deadline`);
    } else {
      try {
        await options.transition(
          {
            workflowId: row.workflow_name,
            runId: row.run_id,
            revision: row.revision,
            deadlineAt: row.deadline_at,
          },
          now,
        );
        processed += 1;
      } catch (error) {
        failures.push(
          `${row.workflow_name}/${row.run_id}: ${errorMessageOf(error)}`,
        );
      }
    }
    try {
      await options.advanceCursor?.(rowCursor);
    } catch (error) {
      failures.push(
        `${row.workflow_name}/${row.run_id}: cursor ${errorMessageOf(error)}`,
      );
    }
  }
  if (failures.length > 0) {
    throw new Error(
      `sweepExpiredRunDeadlines: ${failures.length} of ${rows.length} run(s) failed (${failures.join('; ')})`,
    );
  }
  return processed;
}

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
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
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
   * The start-reservation table (`flowsafe_start_idempotency`). When supplied,
   * this purge is also what keeps idempotency keys finite.
   *
   * Wired the same way `resourceOwnerTable` is — by name, from the composed
   * Flowsafe Worker — for the same reason: this module owns the SQL of run
   * retention, and a reservation must be reaped in the same transaction that
   * removes the run it points at, never by a second sweep that could interleave
   * with it.
   */
  startIdempotencyTable?: string;
  /**
   * How long a spent idempotency key stays answerable after its run settled —
   * the KEY-VALIDITY HORIZON, and the only tuning decision this feature has.
   *
   * Until it elapses, a retry of a completed run is told ALREADY_SETTLED. After
   * it, the reservation is gone and the same key reads as brand new, so a retry
   * would START A SECOND RUN. That is the whole reason this exists as its own
   * knob rather than riding `ttlMs`: a host whose callers retry for longer than
   * its run retention (an overnight batch re-run, a queue with a multi-day
   * redrive) needs keys to outlive summaries, and a host whose keys are minted
   * per HTTP request does not.
   *
   * DEFAULTS TO `ttlMs`, and is floored at it: a reservation shorter-lived than
   * the snapshot it guards would be deleted while its run is still readable,
   * and the very next retry would mint a fresh run alongside the live one — the
   * exact double-execution this feature exists to prevent. A caller asking for
   * less gets `ttlMs`, silently, because there is no configuration in which the
   * smaller number is what anybody meant.
   */
  startIdempotencyTtlMs?: number;
  /**
   * Runs processed per call. Artifact-paired path: default 100 — the purge duty's
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
 * The MASTRA-OWNED tables `purgeExpiredWorkflowRuns` deletes from under the run
 * TTL — the production anchor the schema guard cross-checks every `run-ttl`
 * retention declaration against. The guard reads THIS, not a literal copied
 * into the test, so a purge that changes what it targets and a guard that still
 * blesses the old set cannot drift apart silently.
 *
 * Mastra-owned specifically: the purge ALSO deletes from flowsafe's own
 * registries when a caller wires them, and those live in
 * RUN_TTL_FLOWSAFE_PURGE_TABLES below rather than here — see its note for why
 * the two sets are not one.
 */
export const RUN_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_workflow_snapshot',
];

/**
 * The FLOWSAFE-owned tables this purge also deletes from when the caller wires
 * them, and the reason they are not in the list above.
 *
 * `RUN_TTL_PURGE_TABLES` is cross-checked against the `mastra_%` inventory in
 * mastra-schema-guard.test.ts — its job is to catch a @mastra/core bump that
 * changes what run retention targets. These two are ours, they are optional
 * (a lower-level caller without the registries omits both), and they are
 * deleted on a DIFFERENT predicate: `flowsafe_resource_owners` when its run's
 * last snapshot is gone, `flowsafe_start_idempotency` when its reservation is
 * settled AND past the key-validity horizon. Folding them into the Mastra
 * anchor would make that guard assert an equality it cannot mean.
 */
export const RUN_TTL_FLOWSAFE_PURGE_TABLES: readonly string[] = [
  'flowsafe_resource_owners',
  START_IDEMPOTENCY_TABLE,
];

/**
 * Data-retention purge: deletes TERMINAL runs (success/failed/tripwire/
 * canceled/bailed/skipped and cleanup-complete cancelled/timed_out) whose
 * updatedAt is older than the TTL from
 * mastra_workflow_snapshot — and, when `artifactStore` is wired, each purged
 * run's R2 artifacts with its row, plus (when their tables are wired) the run's
 * ownership row and its spent start reservation, each in the SAME transaction
 * as the snapshot delete. Live runs (running/suspended/waiting/
 * pending/paused) are never touched — expiring a suspended run would kill a
 * pending approval. A missing snapshot table reads as zero purgeable runs
 * (Mastra creates it lazily with the first persisted run). TTL enforcement
 * is a storage-layer property, so it lives here; alarm scheduling stays with
 * the caller. Returns the number of deleted rows.
 */
export async function purgeExpiredWorkflowRuns(
  db: SnapshotDatabase,
  options: PurgeExpiredRunsOptions,
): Promise<number> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
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
         AND CASE WHEN json_valid(snapshot) THEN
           (
             json_extract(snapshot, '$.status') IN (${placeholders})
             AND (
               json_extract(snapshot, '$.status') NOT IN ('cancelled', 'timed_out')
               OR json_extract(
                 snapshot,
                 '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt'
               ) IS NOT NULL
             )
           )
         ELSE 0 END`;
  const resourceOwnerTable = options.resourceOwnerTable;
  if (
    resourceOwnerTable !== undefined &&
    !/^[A-Za-z_][A-Za-z0-9_]*$/.test(resourceOwnerTable)
  ) {
    throw new Error('resourceOwnerTable must be a safe SQL identifier');
  }
  const startIdempotencyTable = options.startIdempotencyTable;
  if (
    startIdempotencyTable !== undefined &&
    !/^[A-Za-z_][A-Za-z0-9_]*$/.test(startIdempotencyTable)
  ) {
    throw new Error('startIdempotencyTable must be a safe SQL identifier');
  }
  // Floored at the run TTL, never below it — see startIdempotencyTtlMs. A
  // reservation deleted while its run is still readable would let the next
  // retry of that key start a SECOND run beside the live one.
  const reservationCutoff =
    now() -
    Math.max(options.startIdempotencyTtlMs ?? options.ttlMs, options.ttlMs);
  const batch =
    resourceOwnerTable || startIdempotencyTable
      ? db.batch?.bind(db)
      : undefined;
  if ((resourceOwnerTable || startIdempotencyTable) && !batch) {
    throw new Error(
      'purgeExpiredWorkflowRuns requires database.batch() for atomic owner cleanup',
    );
  }
  /**
   * The two reservation statements that ride a snapshot delete, in order.
   *
   * They run INSIDE the same `batch()` as the snapshot's own DELETE and AFTER
   * it, which is what makes the pairing atomic: by the time these execute, the
   * runs named here have no snapshot in this transaction, so neither statement
   * can act on a reservation whose run is still readable.
   *
   *  1. DELETE the reservations already past the horizon. This is the pairing
   *     the design asks for: a spent key and the run it named leave together.
   *  2. MARK the rest terminal. A reservation still inside its horizon must
   *     survive — that is what makes a late retry ALREADY_SETTLED rather than a
   *     fresh start — but its run is gone, so it is settled by definition. This
   *     also HEALS the reconcile a crash between a run's terminal persist and
   *     `settleRun` would have lost, and re-stamps `updated_at` so the horizon
   *     is measured from a point at which the reservation is definitely spent.
   */
  /**
   * Whether the reservation table has been seen to exist this pass.
   *
   * It is created lazily by the first `reserve()`, so a deployment on which no
   * idempotency key has ever been used has none — and a batch naming a missing
   * table fails as ONE TRANSACTION, taking the snapshot delete down with it.
   * That would turn "this host wired reservations and nobody has used one yet"
   * into "run retention is silently unenforced", so the first such failure
   * retries the batch WITHOUT the reservation statements and the pass carries
   * on with the pairing disabled. Nothing is lost: a table that does not exist
   * holds no reservation to reap.
   */
  let reservationsUnavailable = false;
  const reservationStatements = (
    runIds: readonly string[],
  ): SnapshotStatement[] => {
    if (
      !startIdempotencyTable ||
      reservationsUnavailable ||
      runIds.length === 0
    ) {
      return [];
    }
    const placeholders = runIds.map(() => '?').join(', ');
    return [
      db
        .prepare(
          `DELETE FROM ${startIdempotencyTable}
           WHERE run_id IN (${placeholders})
             AND state = 'terminal' AND updated_at < ?`,
        )
        .bind(...runIds, reservationCutoff),
      db
        .prepare(
          `UPDATE ${startIdempotencyTable}
             SET state = 'terminal', updated_at = ?
           WHERE run_id IN (${placeholders}) AND state <> 'terminal'`,
        )
        .bind(now(), ...runIds),
    ];
  };
  /**
   * Reap the reservations that OUTLIVED their snapshot.
   *
   * The paired statements above only ever see runs whose snapshot is expiring
   * in THIS pass, and a reservation is meant to survive that moment — the whole
   * point of a horizon longer than run retention is that a late retry still
   * finds ALREADY_SETTLED after the summary is gone. Which means the pairing
   * alone can never delete those rows: by the time they are old enough, their
   * snapshot has been gone for passes and nothing re-visits them. This sweep is
   * what keeps the table finite, and without it the reservation table would be
   * the one piece of this deployment's state that only ever grows.
   *
   * `NOT EXISTS (snapshot)` is not an optimization — it is the safety predicate
   * that makes this sweep structurally unable to delete a reservation whose run
   * is still readable, whatever a caller configured the horizon to be. The
   * LIMIT rides a rowid subselect for the same reason every other purge here
   * does: plain `DELETE ... LIMIT` needs a SQLite compile-time option D1 does
   * not guarantee.
   */
  /**
   * Run a snapshot-delete batch, retrying once without the reservation
   * statements if the reservation table turns out not to exist yet.
   *
   * `build(withReservations)` rather than a prepared array, because D1
   * statements are single-use once run: a retry has to re-prepare.
   */
  const runPurgeBatch = async (
    build: (withReservations: boolean) => SnapshotStatement[],
  ): Promise<unknown[]> => {
    if (!batch) throw new Error('purgeExpiredWorkflowRuns: batch unavailable');
    try {
      return await batch(build(true));
    } catch (error) {
      if (
        startIdempotencyTable === undefined ||
        reservationsUnavailable ||
        !isMissingTable(error, startIdempotencyTable)
      ) {
        throw error;
      }
      reservationsUnavailable = true;
      return batch(build(false));
    }
  };
  const sweepOrphanedStartReservations = async (): Promise<void> => {
    if (!startIdempotencyTable || reservationsUnavailable) return;
    try {
      await db
        .prepare(
          `DELETE FROM ${startIdempotencyTable}
           WHERE rowid IN (
             SELECT r.rowid FROM ${startIdempotencyTable} AS r
             WHERE r.state = 'terminal' AND r.updated_at < ?
               AND NOT EXISTS (
                 SELECT 1 FROM ${prefix}mastra_workflow_snapshot AS s
                 WHERE s.run_id = r.run_id
               )
             LIMIT ?
           )`,
        )
        .bind(reservationCutoff, options.limit ?? 1000)
        .run();
    } catch (error) {
      // Either table may legitimately not exist yet: the reservation table is
      // created by the first reserve() and the snapshot table by the first run.
      // Neither absence is a fault, and neither leaves anything to reap.
      if (
        isMissingTable(error, startIdempotencyTable) ||
        isMissingTable(error, `${prefix}mastra_workflow_snapshot`)
      ) {
        return;
      }
      throw error;
    }
  };

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
        // purge duty's error surface firing. A wedged run does occupy a batch
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
      if (batch) {
        const [result] = await runPurgeBatch((withReservations) => [
          db
            .prepare(
              `DELETE FROM ${prefix}mastra_workflow_snapshot
           WHERE workflow_name = ? AND run_id = ? AND ${eligible}`,
            )
            .bind(row.workflow_name, row.run_id, cutoff, ...TERMINAL_STATUSES),
          ...(resourceOwnerTable
            ? [
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
              ]
            : []),
          ...(withReservations ? reservationStatements([row.run_id]) : []),
        ]);
        deleted += d1Changes(result);
      } else {
        deleted += d1Changes(await deleteSnapshot.run());
      }
    }
    await sweepOrphanedStartReservations();
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
  // unenforced, the exact wedge the one-duty-per-alarm split exists to avoid. The
  // shrinking eligible set is the cursor: a backlog drains across firings.
  let deleted: number;
  try {
    if (batch) {
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
      if (selected.results.length === 0) {
        // No eligible run this pass, but reservations left behind by EARLIER
        // passes still age out — so the orphan sweep runs before the return.
        await sweepOrphanedStartReservations();
        return 0;
      }
      const rowIds = selected.results.map((row) => row.rowid);
      const runIds = [...new Set(selected.results.map((row) => row.run_id))];
      const rowPlaceholders = rowIds.map(() => '?').join(', ');
      const runPlaceholders = runIds.map(() => '?').join(', ');
      const [result] = await runPurgeBatch((withReservations) => [
        db
          .prepare(
            `DELETE FROM ${prefix}mastra_workflow_snapshot
             WHERE rowid IN (${rowPlaceholders}) AND ${eligible}`,
          )
          .bind(...rowIds, cutoff, ...TERMINAL_STATUSES),
        ...(resourceOwnerTable
          ? [
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
            ]
          : []),
        ...(withReservations ? reservationStatements(runIds) : []),
      ]);
      deleted = d1Changes(result);
    } else {
      deleted = d1Changes(
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
    }
  } catch (error) {
    if (!isMissingTable(error, `${prefix}mastra_workflow_snapshot`))
      throw error;
    return 0;
  }
  await sweepOrphanedStartReservations();
  return deleted;
}

export interface PurgeExpiredThreadsOptions {
  /** Threads untouched for longer than this are eligible, with their messages. */
  ttlMs: number;
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
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
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
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
      // purge duty's error surface rather than silently orphan a thread's history.
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
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
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
 * a maintenance purge can reap them WITHOUT a live manager: the same posture as
 * purgeExpiredWorkflowRuns/purgeExpiredThreads (raw D1 binding, failure-isolated
 * as a purge duty). Two windows, exactly as core: completed rows expire fast
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
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
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
   * the model has not seen. No default: the caller (createFlowsafeWorker's purge duty)
   * gates this on an opt-in retention window, since a durable inbox is meant to
   * be readable until the host says otherwise.
   */
  ttlMs: number;
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Notification TTL cleanup: deletes terminal agent-inbox
 * rows from `mastra_notifications` once their `updatedAt` is older than the TTL,
 * at the storage layer so alarm maintenance reaps them without a live agent —
 * the same posture as the other purges (raw D1 binding, failure-isolated duty).
 * `updatedAt` is ISO-8601 TEXT, so lexicographic `<` is a correct timestamp
 * comparison. A missing table reads as zero (notifications may never have been
 * sent). Scheduling stays with the caller.
 */
export async function purgeExpiredNotifications(
  db: SnapshotDatabase,
  options: PurgeExpiredNotificationsOptions,
): Promise<number> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
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
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
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
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
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
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Schedule-trigger TTL cleanup: deletes terminal trigger-history
 * rows from `mastra_schedule_triggers` once their `actualFireAt` is older than the
 * TTL, at the storage layer so alarm maintenance reaps them without a live tick —
 * the same posture as the other purges (raw D1 binding, failure-isolated duty).
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
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
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
