// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { D1FleetInventoryRunStore } from '../src/d1-fleet-inventory-run-store.js';
import {
  canonicalFleetInventoryRunOptions,
  emptyFleetInventoryRowCounts,
  FleetInventoryFindingValueError,
  type FleetInventoryLease,
  type FleetInventoryRowKind,
  type FleetInventoryRunRecord,
  type FleetInventoryStage,
  type FleetInventoryStagedFact,
  type FleetInventoryStagedRow,
  fleetInventoryOptionsDigest,
} from '../src/fleet-inventory-state.js';
import type { FleetStateDatabase } from '../src/state-store.js';

interface SqliteStatement {
  all(...bindings: readonly unknown[]): Readonly<Record<string, unknown>>[];
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
}

function openSqlite(): SqliteDatabase {
  // getBuiltinModule avoids vite's resolver, which cannot resolve node:sqlite;
  // node:sqlite has been unflagged since Node 22.13.
  const getBuiltin = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!getBuiltin) {
    throw new Error('node:sqlite unavailable — tests require node >= 22.13');
  }
  const sqlite = getBuiltin('node:sqlite') as {
    DatabaseSync: new (path: string) => SqliteDatabase;
  };
  return new sqlite.DatabaseSync(':memory:');
}

/**
 * Fake fleet state database port. It executes the store's real SQL, so every
 * guard, `json_extract` comparison, and count subquery is exercised, and a
 * batch is atomic exactly as the port contract promises.
 */
class MemoryD1 implements FleetStateDatabase {
  readonly sqlite = openSqlite();
  /** Statements the next batch drops after committing, for lost responses. */
  hideBatchResults = false;

  async query(
    sql: string,
    bindings: readonly unknown[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    return this.sqlite.prepare(sql).all(...bindings);
  }

  async execute(sql: string, bindings: readonly unknown[] = []): Promise<void> {
    this.sqlite.prepare(sql).all(...bindings);
  }

  async batch(
    statements: readonly Readonly<{
      sql: string;
      bindings?: readonly unknown[];
    }>[],
  ): Promise<readonly (readonly Readonly<Record<string, unknown>>[])[]> {
    if (statements.length === 0) return [];
    const results: Readonly<Record<string, unknown>>[][] = [];
    this.sqlite.exec('BEGIN IMMEDIATE');
    try {
      for (const { sql, bindings = [] } of statements) {
        results.push(this.sqlite.prepare(sql).all(...bindings));
      }
      this.sqlite.exec('COMMIT');
    } catch (error) {
      this.sqlite.exec('ROLLBACK');
      throw error;
    }
    if (this.hideBatchResults) {
      this.hideBatchResults = false;
      return results.map(() => []);
    }
    return results;
  }
}

const OPERATION_ID = '123e4567-e89b-42d3-a456-426614174000';
const SECOND_OPERATION_ID = '123e4567-e89b-42d3-a456-426614174001';
const THIRD_OPERATION_ID = '123e4567-e89b-42d3-a456-426614174002';
const OPTIONS = canonicalFleetInventoryRunOptions({
  hostRoutingKvId: 'kv-host-routing',
  databaseNamePrefix: 'anchorage-db',
  scriptNamePrefix: 'anchorage',
});
const DIGEST = fleetInventoryOptionsDigest(OPTIONS);
const OTHER_OPTIONS = canonicalFleetInventoryRunOptions({
  databaseNamePrefix: 'anchorage-db',
  scriptNamePrefix: 'anchorage',
});
const OTHER_DIGEST = fleetInventoryOptionsDigest(OTHER_OPTIONS);
const TABLES = [
  'anchorage_fleet_inventory_deployment_facts',
  'anchorage_fleet_inventory_heads',
  'anchorage_fleet_inventory_leases',
  'anchorage_fleet_inventory_pins',
  'anchorage_fleet_inventory_rows',
  'anchorage_fleet_inventory_runs',
];

function newStore(
  db: MemoryD1,
  accountId = 'account-primary',
): D1FleetInventoryRunStore {
  return new D1FleetInventoryRunStore(db, { accountId });
}

function stagedRow(
  kind: FleetInventoryRowKind,
  ordinal: number,
  payload: Readonly<Record<string, unknown>>,
): FleetInventoryStagedRow {
  return { kind, ordinal, payload };
}

function stagedFact(
  deploymentOrdinal: number,
  factOrdinal: number,
): FleetInventoryStagedFact {
  return {
    deploymentOrdinal,
    factKind: 'secret-name',
    factOrdinal,
    payload: { name: `ANCHORAGE_NAME_${factOrdinal}` },
  };
}

function countsOf(
  rows: readonly FleetInventoryStagedRow[],
): Record<FleetInventoryRowKind, number> {
  const counts = emptyFleetInventoryRowCounts() as Record<
    FleetInventoryRowKind,
    number
  >;
  for (const row of rows) counts[row.kind] += 1;
  return counts;
}

function committed(
  record: FleetInventoryRunRecord,
  rows: readonly FleetInventoryStagedRow[],
  facts: readonly FleetInventoryStagedFact[],
  stage: FleetInventoryStage = { step: 'finalize' },
): FleetInventoryRunRecord {
  return {
    ...record,
    progress: {
      ...record.progress,
      stage,
      revision: record.progress.revision + 1,
      stagedCounts: countsOf(rows),
      factCount: facts.length,
      providerRequests: record.progress.providerRequests + 1,
    },
    updatedAt: '2026-08-29T00:00:00.000Z',
  };
}

function start(
  lease: FleetInventoryLease,
  operationId = OPERATION_ID,
): Promise<FleetInventoryRunRecord> {
  return lease.startRun({
    operationId,
    options: OPTIONS,
    optionsDigest: DIGEST,
  });
}

const DEFAULT_ROWS = [
  stagedRow('registration', 0, { scriptName: 'anchorage-tenant-prod' }),
  stagedRow('deployment', 0, { scriptName: 'anchorage-tenant-prod' }),
  stagedRow('finding', 0, { detail: 'stale route for anchorage-tenant-prod' }),
];
const DEFAULT_FACTS = [stagedFact(0, 0), stagedFact(0, 1)];
/** Generations read back in `(kind, ordinal)` order, the store's read order. */
const DEFAULT_ROWS_READ_ORDER = [...DEFAULT_ROWS].sort((left, right) =>
  left.kind === right.kind
    ? left.ordinal - right.ordinal
    : left.kind < right.kind
      ? -1
      : 1,
);

async function seedGeneration(
  store: D1FleetInventoryRunStore,
  operationId = OPERATION_ID,
  rows: readonly FleetInventoryStagedRow[] = DEFAULT_ROWS,
  facts: readonly FleetInventoryStagedFact[] = DEFAULT_FACTS,
): Promise<number> {
  return store.withAccountInventoryLease(async (lease) => {
    const started = await start(lease, operationId);
    const record = await lease.commitChunk({
      operationId,
      expectedRevision: started.progress.revision,
      runRecord: committed(started, rows, facts),
      rows,
      facts,
    });
    const ref = await lease.finalizeRun({
      operationId,
      expectedRevision: record.progress.revision,
      manifest: record.progress.stagedCounts,
      factCount: record.progress.factCount,
    });
    return ref.generation;
  });
}

async function refusal(operation: Promise<unknown>): Promise<Error> {
  try {
    await operation;
  } catch (error) {
    return error as Error;
  }
  throw new Error('operation unexpectedly resolved');
}

describe('D1FleetInventoryRunStore', () => {
  it('creates the six inventory tables, verifies every column, and fails closed on drift', async () => {
    const db = new MemoryD1();
    await newStore(db).latestFinalizedGeneration();
    const tables = db.sqlite
      .prepare(
        `SELECT name FROM sqlite_master
          WHERE type = 'table' AND name LIKE 'anchorage_fleet_inventory_%'
          ORDER BY name`,
      )
      .all()
      .map((row) => String(row.name));
    expect(tables).toEqual(TABLES);
    const heads = db.sqlite
      .prepare('PRAGMA table_info(anchorage_fleet_inventory_heads)')
      .all()
      .map((row) => `${String(row.name)}:${String(row.type)}`);
    expect(heads).toEqual([
      'account_id:TEXT',
      'active_operation_id:TEXT',
      'latest_finalized_generation:INTEGER',
      'next_generation:INTEGER',
    ]);

    const drifted = new MemoryD1();
    drifted.sqlite.exec(`CREATE TABLE anchorage_fleet_inventory_pins (
      account_id TEXT NOT NULL,
      generation TEXT NOT NULL,
      pinned_by TEXT NOT NULL,
      pinned_at_ms INTEGER NOT NULL,
      PRIMARY KEY (account_id, generation, pinned_by)
    )`);
    const error = await refusal(newStore(drifted).latestFinalizedGeneration());
    expect(error.message).toBe(
      "fleet inventory table 'anchorage_fleet_inventory_pins' column 'generation' is absent or incompatible",
    );
  });

  it('allocates a generation and claims the head when a run starts', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const started = await store.withAccountInventoryLease((lease) =>
      start(lease),
    );
    expect(started).toMatchObject({
      version: 1,
      operationId: OPERATION_ID,
      optionsDigest: DIGEST,
      state: 'staging',
    });
    expect(started.progress.generation).toBe(1);
    expect(started.progress.stage).toEqual({ step: 'host-kv-keys' });
    expect(
      db.sqlite.prepare('SELECT * FROM anchorage_fleet_inventory_heads').all(),
    ).toEqual([
      {
        account_id: 'account-primary',
        active_operation_id: OPERATION_ID,
        latest_finalized_generation: null,
        next_generation: 2,
      },
    ]);
  });

  it('treats a replayed start as a no-op returning the same run', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const first = await store.withAccountInventoryLease((lease) =>
      start(lease),
    );
    const replay = await store.withAccountInventoryLease((lease) =>
      start(lease),
    );
    expect(replay).toEqual(first);
    expect(
      db.sqlite
        .prepare('SELECT COUNT(*) AS count FROM anchorage_fleet_inventory_runs')
        .all(),
    ).toEqual([{ count: 1 }]);
    expect(
      db.sqlite
        .prepare('SELECT next_generation FROM anchorage_fleet_inventory_heads')
        .all(),
    ).toEqual([{ next_generation: 2 }]);
  });

  it('conflicts when a start replays with a different options digest', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease((lease) => start(lease));
    const error = await refusal(
      store.withAccountInventoryLease((lease) =>
        lease.startRun({
          operationId: OPERATION_ID,
          options: OTHER_OPTIONS,
          optionsDigest: OTHER_DIGEST,
        }),
      ),
    );
    expect(error.message).toBe(
      `fleet inventory run '${OPERATION_ID}' was started with different options`,
    );
  });

  it('contends when another operation already owns the head', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease((lease) => start(lease));
    const error = await refusal(
      store.withAccountInventoryLease((lease) =>
        start(lease, SECOND_OPERATION_ID),
      ),
    );
    expect(error.message).toBe(
      `fleet inventory for account 'account-primary' has an active operation other than '${SECOND_OPERATION_ID}'`,
    );
    expect(
      db.sqlite
        .prepare('SELECT COUNT(*) AS count FROM anchorage_fleet_inventory_runs')
        .all(),
    ).toEqual([{ count: 1 }]);
  });

  it('refuses a chunk commit at a stale revision', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const error = await refusal(
      store.withAccountInventoryLease(async (lease) => {
        const started = await start(lease);
        const first = {
          operationId: OPERATION_ID,
          expectedRevision: started.progress.revision,
          runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS, {
            step: 'ordinary-scripts',
          }),
          rows: DEFAULT_ROWS,
          facts: DEFAULT_FACTS,
        };
        const record = await lease.commitChunk(first);
        const trailing = stagedRow('meta', 0, { stage: 'finalize' });
        await lease.commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: record.progress.revision,
          runRecord: committed(
            record,
            [...DEFAULT_ROWS, trailing],
            DEFAULT_FACTS,
          ),
          rows: [trailing],
          facts: [],
        });
        return lease.commitChunk(first);
      }),
    );
    expect(error.message).toBe(
      `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`,
    );
  });

  it('converges when a chunk commit replays byte-identically', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const converged = await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const input = {
        operationId: OPERATION_ID,
        expectedRevision: started.progress.revision,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      };
      db.hideBatchResults = true;
      const first = await lease.commitChunk(input);
      const second = await lease.commitChunk(input);
      return { first, second };
    });
    expect(converged.second).toEqual(converged.first);
    expect(
      db.sqlite
        .prepare('SELECT COUNT(*) AS count FROM anchorage_fleet_inventory_rows')
        .all(),
    ).toEqual([{ count: DEFAULT_ROWS.length }]);
  });

  it('raises corruption when a chunk commit replays with divergent bytes', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const error = await refusal(
      store.withAccountInventoryLease(async (lease) => {
        const started = await start(lease);
        const input = {
          operationId: OPERATION_ID,
          expectedRevision: started.progress.revision,
          runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
          rows: DEFAULT_ROWS,
          facts: DEFAULT_FACTS,
        };
        db.hideBatchResults = true;
        await lease.commitChunk(input);
        return lease.commitChunk({
          ...input,
          rows: [
            stagedRow('registration', 0, { scriptName: 'other-script' }),
            ...DEFAULT_ROWS.slice(1),
          ],
        });
      }),
    );
    expect(error.message).toBe(
      `fleet inventory run '${OPERATION_ID}' staged rows diverge from the persisted generation`,
    );
  });

  it('validates the manifest inside the finalize batch', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const generation = await seedGeneration(store);
    expect(generation).toBe(1);
    const ref = await store.latestFinalizedGeneration();
    expect(ref).toMatchObject({
      generation: 1,
      operationId: OPERATION_ID,
      factCount: DEFAULT_FACTS.length,
    });
    expect(ref?.rowManifest).toEqual(countsOf(DEFAULT_ROWS));
    expect(ref?.finalizedAtMs).toBeGreaterThan(0);
    const readback = await store.readFinalizedGeneration(1);
    expect(readback.rows).toEqual(DEFAULT_ROWS_READ_ORDER);
    expect(readback.facts).toEqual(DEFAULT_FACTS);
  });

  it('leaves the run staging when the finalize manifest mismatches', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const error = await refusal(
      store.withAccountInventoryLease(async (lease) => {
        const started = await start(lease);
        // The persisted record claims more findings than the rows it staged, so
        // the in-SQL count guard is the control under test.
        const overstated = committed(started, DEFAULT_ROWS, DEFAULT_FACTS);
        const record = await lease.commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: started.progress.revision,
          runRecord: {
            ...overstated,
            progress: {
              ...overstated.progress,
              stagedCounts: { ...overstated.progress.stagedCounts, finding: 9 },
            },
          },
          rows: DEFAULT_ROWS,
          facts: DEFAULT_FACTS,
        });
        return lease.finalizeRun({
          operationId: OPERATION_ID,
          expectedRevision: record.progress.revision,
          manifest: record.progress.stagedCounts,
          factCount: record.progress.factCount,
        });
      }),
    );
    expect(error.message).toBe(
      `fleet inventory run '${OPERATION_ID}' does not match its finalize manifest`,
    );
    const run = await newStore(db).readRunByOperation(OPERATION_ID);
    expect(run?.state).toBe('staging');
    expect(await newStore(db).latestFinalizedGeneration()).toBeUndefined();
  });

  it('converges by readback when finalize replays on a finalized run', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const refs = await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const record = await lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: started.progress.revision,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      });
      const input = {
        operationId: OPERATION_ID,
        expectedRevision: record.progress.revision,
        manifest: record.progress.stagedCounts,
        factCount: record.progress.factCount,
      };
      db.hideBatchResults = true;
      const first = await lease.finalizeRun(input);
      const replay = await lease.finalizeRun(input);
      return { first, replay };
    });
    expect(refs.replay).toEqual(refs.first);
    expect(
      db.sqlite
        .prepare(
          'SELECT latest_finalized_generation AS latest, active_operation_id AS active FROM anchorage_fleet_inventory_heads',
        )
        .all(),
    ).toEqual([{ latest: 1, active: null }]);
  });

  it('refuses staging bytes from a fenced-out writer and keeps the payload clean', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    // The lease escapes its callback, so its token is no longer live: exactly
    // the stale-lease writer the guarded inserts must fence out.
    const fenced = await store.withAccountInventoryLease(async (lease) => {
      await start(lease);
      return lease;
    });
    const poisoned = [
      stagedRow('registration', 0, { scriptName: 'poisoned' }),
      ...DEFAULT_ROWS.slice(1),
    ];
    const started = await store.readRunByOperation(OPERATION_ID);
    if (!started) throw new Error('run missing');
    const error = await refusal(
      fenced.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: started.progress.revision,
        runRecord: committed(started, poisoned, DEFAULT_FACTS),
        rows: poisoned,
        facts: DEFAULT_FACTS,
      }),
    );
    expect(error.message).toBe(
      `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`,
    );
    expect(
      db.sqlite
        .prepare(
          `SELECT
             (SELECT COUNT(*) FROM anchorage_fleet_inventory_rows) AS rows,
             (SELECT COUNT(*) FROM anchorage_fleet_inventory_deployment_facts) AS facts`,
        )
        .all(),
    ).toEqual([{ rows: 0, facts: 0 }]);
    expect(
      (await store.readRunByOperation(OPERATION_ID))?.progress.revision,
    ).toBe(started.progress.revision);

    const generation = await store.withAccountInventoryLease(async (lease) => {
      const record = await lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: started.progress.revision,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      });
      const ref = await lease.finalizeRun({
        operationId: OPERATION_ID,
        expectedRevision: record.progress.revision,
        manifest: record.progress.stagedCounts,
        factCount: record.progress.factCount,
      });
      return ref.generation;
    });
    expect((await store.readFinalizedGeneration(generation)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
  });

  it('refuses a finalize whose caller manifest disagrees with the persisted run record', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const error = await refusal(
      store.withAccountInventoryLease(async (lease) => {
        const started = await start(lease);
        const record = await lease.commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: started.progress.revision,
          runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
          rows: DEFAULT_ROWS,
          facts: DEFAULT_FACTS,
        });
        return lease.finalizeRun({
          operationId: OPERATION_ID,
          expectedRevision: record.progress.revision,
          manifest: { ...record.progress.stagedCounts, finding: 0 },
          factCount: record.progress.factCount,
        });
      }),
    );
    expect(error.message).toBe(
      `fleet inventory run '${OPERATION_ID}' finalize manifest disagrees with the persisted run record`,
    );
    expect((await store.readRunByOperation(OPERATION_ID))?.state).toBe(
      'staging',
    );
    expect(await store.latestFinalizedGeneration()).toBeUndefined();
  });

  it('does not re-claim the head or burn a generation when a start replays for a completed run', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store, OPERATION_ID);
    const finalizedReplay = await store.withAccountInventoryLease((lease) =>
      start(lease, OPERATION_ID),
    );
    expect(finalizedReplay.state).toBe('finalized');
    expect(finalizedReplay.progress.generation).toBe(1);

    const failed = await store.withAccountInventoryLease(async (lease) => {
      const record = await start(lease, SECOND_OPERATION_ID);
      await lease.failRun({
        operationId: SECOND_OPERATION_ID,
        expectedRevision: record.progress.revision,
        reason: 'operator-abandoned',
      });
      return record;
    });
    expect(failed.progress.generation).toBe(2);
    const failedReplay = await store.withAccountInventoryLease((lease) =>
      start(lease, SECOND_OPERATION_ID),
    );
    expect(failedReplay.state).toBe('failed');
    expect(
      db.sqlite
        .prepare(
          `SELECT active_operation_id AS active, next_generation AS next
             FROM anchorage_fleet_inventory_heads`,
        )
        .all(),
    ).toEqual([{ active: null, next: 3 }]);

    const fresh = await store.withAccountInventoryLease((lease) =>
      start(lease, THIRD_OPERATION_ID),
    );
    expect(fresh.progress.generation).toBe(3);
  });

  it('releases the head when a run fails', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      await lease.failRun({
        operationId: OPERATION_ID,
        expectedRevision: started.progress.revision,
        reason: 'operator-abandoned',
      });
    });
    expect((await store.readRunByOperation(OPERATION_ID))?.state).toBe(
      'failed',
    );
    expect(
      db.sqlite
        .prepare(
          'SELECT active_operation_id AS active FROM anchorage_fleet_inventory_heads',
        )
        .all(),
    ).toEqual([{ active: null }]);
    const next = await store.withAccountInventoryLease((lease) =>
      start(lease, SECOND_OPERATION_ID),
    );
    expect(next.progress.generation).toBe(2);
  });

  it('refuses to pin a generation that is not finalized', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease((lease) => start(lease));
    const error = await refusal(
      store.pinGeneration({ generation: 1, pinnedBy: 'audit' }),
    );
    expect(error.message).toBe('fleet inventory generation 1 is not finalized');
    expect(
      db.sqlite
        .prepare('SELECT COUNT(*) AS count FROM anchorage_fleet_inventory_pins')
        .all(),
    ).toEqual([{ count: 0 }]);
  });

  it('refuses to read an unpinned non-latest generation', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store, OPERATION_ID);
    await seedGeneration(
      store,
      SECOND_OPERATION_ID,
      [stagedRow('meta', 0, { stage: 'finalize' })],
      [],
    );
    const error = await refusal(store.readFinalizedGeneration(1));
    expect(error.message).toBe(
      'fleet inventory generation 1 requires a pin before it can be read',
    );
    await store.pinGeneration({ generation: 1, pinnedBy: 'audit' });
    expect((await store.readFinalizedGeneration(1)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
    await store.releasePin({ generation: 1, pinnedBy: 'audit' });
    expect((await refusal(store.readFinalizedGeneration(1))).message).toBe(
      'fleet inventory generation 1 requires a pin before it can be read',
    );
  });

  it('validates the prune limit', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    for (const limit of [0, 1_001, 1.5]) {
      expect(
        (await refusal(store.pruneInventoryGenerations({ limit }))).message,
      ).toBe('limit must be an integer from 1 to 1000');
    }
    await expect(
      store.pruneInventoryGenerations({ limit: 1_000 }),
    ).resolves.toEqual({ deleted: 0 });
  });

  it('protects the latest and pinned generations from pruning', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store, OPERATION_ID);
    await seedGeneration(
      store,
      SECOND_OPERATION_ID,
      [stagedRow('meta', 0, { stage: 'finalize' })],
      [],
    );
    await seedGeneration(
      store,
      THIRD_OPERATION_ID,
      [stagedRow('meta', 0, { stage: 'finalize' })],
      [],
    );
    await store.pinGeneration({ generation: 1, pinnedBy: 'audit' });
    expect(await store.pruneInventoryGenerations({ limit: 10 })).toEqual({
      deleted: 1,
    });
    expect(
      db.sqlite
        .prepare(
          'SELECT generation FROM anchorage_fleet_inventory_runs ORDER BY generation',
        )
        .all()
        .map((row) => Number(row.generation)),
    ).toEqual([1, 3]);
    expect((await store.readFinalizedGeneration(1)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
    await store.releasePin({ generation: 1, pinnedBy: 'audit' });
    expect(await store.pruneInventoryGenerations({ limit: 10 })).toEqual({
      deleted: 1,
    });
    expect(
      db.sqlite
        .prepare('SELECT COUNT(*) AS count FROM anchorage_fleet_inventory_rows')
        .all(),
    ).toEqual([{ count: 1 }]);
  });

  it('refuses a store pin or prune wrapper called inside an open lease', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    const errors = await store.withAccountInventoryLease(async () => [
      await refusal(store.pinGeneration({ generation: 1, pinnedBy: 'audit' })),
      await refusal(store.pruneInventoryGenerations({ limit: 1 })),
    ]);
    for (const error of errors) {
      expect(error.message).toBe(
        "fleet inventory for account 'account-primary' is already being modified",
      );
    }
  });

  it('leaves a foreign account inventory untouchable', async () => {
    const db = new MemoryD1();
    const primary = newStore(db);
    const foreign = newStore(db, 'account-foreign');
    await seedGeneration(primary);
    expect(await foreign.latestFinalizedGeneration()).toBeUndefined();
    expect(await foreign.readRunByOperation(OPERATION_ID)).toBeUndefined();
    expect((await refusal(foreign.readFinalizedGeneration(1))).message).toBe(
      'fleet inventory generation 1 requires a pin before it can be read',
    );
    expect(await foreign.pruneInventoryGenerations({ limit: 10 })).toEqual({
      deleted: 0,
    });
    expect((await primary.readFinalizedGeneration(1)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
  });

  it('accepts a base64-shaped resumption cursor and rejects a credential-shaped one', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const cursor = 'AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8g';
    const record = await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      return lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: started.progress.revision,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS, {
          step: 'ordinary-scripts',
          cursor,
        }),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      });
    });
    expect(record.progress.stage).toEqual({ step: 'ordinary-scripts', cursor });
    const persisted = db.sqlite
      .prepare('SELECT run_record FROM anchorage_fleet_inventory_runs')
      .all()
      .map((row) => String(row.run_record));
    expect(persisted[0]).toContain(cursor);
    const staged = db.sqlite
      .prepare(
        `SELECT payload FROM anchorage_fleet_inventory_rows
         UNION ALL
         SELECT payload FROM anchorage_fleet_inventory_deployment_facts`,
      )
      .all()
      .map((row) => String(row.payload));
    for (const payload of staged) expect(payload).not.toContain(cursor);
    const error = await refusal(
      store.withAccountInventoryLease((lease) =>
        lease.commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: record.progress.revision,
          runRecord: committed(record, DEFAULT_ROWS, DEFAULT_FACTS, {
            step: 'ordinary-scripts',
            cursor: 'Bearer eyJhbGciOiJIUzI1NiJ9',
          }),
          rows: DEFAULT_ROWS,
          facts: DEFAULT_FACTS,
        }),
      ),
    );
    expect(error).toBeInstanceOf(FleetInventoryFindingValueError);
  });
});
