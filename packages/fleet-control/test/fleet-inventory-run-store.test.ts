// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { D1FleetInventoryRunStore } from '../src/d1-fleet-inventory-run-store.js';
import { advanceFleetInventory } from '../src/fleet-inventory-advance.js';
import {
  canonicalFleetInventoryRunOptions,
  emptyFleetInventoryRowCounts,
  FleetInventoryFindingValueError,
  type FleetInventoryLease,
  type FleetInventoryProviderContext,
  type FleetInventoryRowKind,
  type FleetInventoryRunRecord,
  type FleetInventoryStage,
  type FleetInventoryStagedFact,
  type FleetInventoryStagedRow,
  FleetInventoryStateError,
  fleetInventoryOptionsDigest,
  materializeFleetInventoryGeneration,
} from '../src/fleet-inventory-state.js';
import type { FleetStateDatabase } from '../src/state-store.js';
import { deferred } from './fixtures/cloudflare-fetch-fixture.js';

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

class MemoryD1 implements FleetStateDatabase {
  readonly sqlite = openSqlite();
  /** Statements the next batch drops after committing, for lost responses. */
  hideBatchResults = false;
  beforeBatch: (() => void) | undefined;
  afterBatch: (() => void) | undefined;
  beforeStatement: ((index: number) => void) | undefined;
  batchCalls = 0;

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
    this.batchCalls += 1;
    const beforeBatch = this.beforeBatch;
    this.beforeBatch = undefined;
    beforeBatch?.();
    const results: Readonly<Record<string, unknown>>[][] = [];
    const beforeStatement = this.beforeStatement;
    this.beforeStatement = undefined;
    this.sqlite.exec('BEGIN IMMEDIATE');
    try {
      for (const { sql, bindings = [] } of statements) {
        beforeStatement?.(results.length);
        results.push(this.sqlite.prepare(sql).all(...bindings));
      }
      this.sqlite.exec('COMMIT');
    } catch (error) {
      this.sqlite.exec('ROLLBACK');
      throw error;
    }
    const afterBatch = this.afterBatch;
    this.afterBatch = undefined;
    afterBatch?.();
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

function inventoryState(db: MemoryD1): unknown {
  return TABLES.filter((table) => !table.endsWith('_leases')).map((table) =>
    db.sqlite.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all(),
  );
}

describe('D1FleetInventoryRunStore', () => {
  it('persists R2 availability separately from an empty bucket inventory', async () => {
    const store = newStore(new MemoryD1());
    const empty = await seedGeneration(store, OPERATION_ID, [], []);
    await store.pinGeneration({
      generation: empty,
      pinnedBy: 'availability-test',
    });
    const unavailable = await seedGeneration(
      store,
      SECOND_OPERATION_ID,
      [
        stagedRow('meta', 0, {
          record: 'unavailable-r2-jurisdiction',
          jurisdiction: 'fedramp',
        }),
      ],
      [],
    );
    const emptyState = await store.readFinalizedGeneration(empty);
    const unavailableState = await store.readFinalizedGeneration(unavailable);
    expect(JSON.stringify(unavailableState.rows)).not.toBe(
      JSON.stringify(emptyState.rows),
    );
    const materialize = (state: typeof emptyState) =>
      materializeFleetInventoryGeneration({ ...state, options: OPTIONS });
    expect(materialize(emptyState).unavailableR2Jurisdictions).toEqual([]);
    expect(materialize(unavailableState).unavailableR2Jurisdictions).toEqual([
      'fedramp',
    ]);
    expect(
      Object.isFrozen(materialize(unavailableState).unavailableR2Jurisdictions),
    ).toBe(true);
  });

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

describe('inventory chunk identity and immutable staging', () => {
  it.each([
    'failed',
    'finalized',
    'different record',
  ] as const)('refuses replay against a same-revision %s run before comparing payloads', async (state) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const input = {
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      };
      const first = await lease.commitChunk(input);
      if (state === 'failed') {
        await lease.failRun({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          reason: 'operator-abandoned',
        });
      } else if (state === 'finalized') {
        await lease.finalizeRun({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          manifest: first.progress.stagedCounts,
          factCount: first.progress.factCount,
        });
      }
      const intended =
        state === 'different record'
          ? { ...first, updatedAt: '2026-08-30T00:00:00.000Z' }
          : first;
      const before = inventoryState(db);
      const result = await lease
        .commitChunk({ ...input, runRecord: intended })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toEqual(
        new Error(
          `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`,
        ),
      );
      const divergent = await lease
        .commitChunk({
          ...input,
          runRecord: intended,
          rows: [stagedRow('registration', 0, { scriptName: 'divergent' })],
        })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(divergent).toEqual(result);
    });
  });

  it.each([
    'row',
    'fact',
  ] as const)('rolls back mixed siblings on an immutable %s conflict', async (kind) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const first = await lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      });
      const newRow = stagedRow('registration', 1, { scriptName: 'sibling' });
      const newFact = stagedFact(0, 2);
      const rows =
        kind === 'row'
          ? [newRow, stagedRow('registration', 0, { scriptName: 'divergent' })]
          : [newRow];
      const facts =
        kind === 'fact'
          ? [newFact, { ...stagedFact(0, 0), payload: { name: 'divergent' } }]
          : [newFact];
      const before = inventoryState(db);
      const result = await lease
        .commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          runRecord: committed(
            first,
            [...DEFAULT_ROWS, newRow],
            [...DEFAULT_FACTS, newFact],
          ),
          rows,
          facts,
        })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toMatchObject({
        code: 'ERR_SQLITE_ERROR',
        message: expect.stringContaining('UNIQUE constraint failed'),
      });
    });
  });

  it.each([
    'row',
    'fact',
  ] as const)('requires exact serialized %s payload bytes', async (kind) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const rows = [stagedRow('meta', 0, { first: 1, second: 2 })];
      const facts = [{ ...stagedFact(0, 0), payload: { first: 1, second: 2 } }];
      const first = await lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, rows, facts),
        rows,
        facts,
      });
      const before = inventoryState(db);
      const result = await lease
        .commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          runRecord: committed(first, rows, facts),
          rows:
            kind === 'row'
              ? [stagedRow('meta', 0, { second: 2, first: 1 })]
              : rows,
          facts:
            kind === 'fact'
              ? [{ ...stagedFact(0, 0), payload: { second: 2, first: 1 } }]
              : facts,
        })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toMatchObject({
        message: expect.stringContaining('UNIQUE constraint failed'),
      });
    });
  });

  it.each([
    'row exact',
    'row different',
    'fact exact',
    'fact different',
  ] as const)('rejects duplicate keys before SQL: %s', async (variant) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const rows = [stagedRow('registration', 0, { name: 'first' })];
      const facts = [stagedFact(0, 0)];
      if (variant.startsWith('row'))
        rows.push(
          stagedRow('registration', 0, {
            name: variant.endsWith('exact') ? 'first' : 'second',
          }),
        );
      else
        facts.push({
          ...stagedFact(0, 0),
          payload: variant.endsWith('exact')
            ? stagedFact(0, 0).payload
            : { name: 'second' },
        });
      const before = inventoryState(db);
      const batches = db.batchCalls;
      const result = await lease
        .commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: committed(started, rows, facts),
          rows,
          facts,
        })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(db.batchCalls).toBe(batches);
      expect(result).toBeInstanceOf(FleetInventoryStateError);
      expect(result).toMatchObject({
        name: 'FleetInventoryStateError',
        message: 'fleet inventory state is malformed',
      });
    });
  });

  it.each([
    'foreign account',
    'generation',
    'options',
    'missing run',
  ] as const)('refuses the wrong target: %s', async (target) => {
    const db = new MemoryD1();
    const store = newStore(db);
    const started = await store.withAccountInventoryLease((lease) =>
      start(lease),
    );
    const writer =
      target === 'foreign account' ? newStore(db, 'account-secondary') : store;
    await writer.withAccountInventoryLease(async (lease) => {
      if (target === 'foreign account') await start(lease, SECOND_OPERATION_ID);
      let intended = committed(started, DEFAULT_ROWS, DEFAULT_FACTS);
      if (target === 'generation')
        intended = {
          ...intended,
          progress: { ...intended.progress, generation: 99 },
        };
      if (target === 'options')
        intended = {
          ...intended,
          options: OTHER_OPTIONS,
          optionsDigest: OTHER_DIGEST,
        };
      if (target === 'missing run')
        intended = { ...intended, operationId: THIRD_OPERATION_ID };
      const before = inventoryState(db);
      const result = await lease
        .commitChunk({
          operationId: intended.operationId,
          expectedRevision: 0,
          runRecord: intended,
          rows: DEFAULT_ROWS,
          facts: DEFAULT_FACTS,
        })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toEqual(
        new Error(
          target === 'foreign account' || target === 'missing run'
            ? `no fleet inventory run for operation '${intended.operationId}'`
            : `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`,
        ),
      );
    });
  });

  it.each([
    'ordinary',
    'hidden results',
    'lost response',
  ] as const)('accepts exact restaging and replay with %s', async (response) => {
    const db = new MemoryD1();
    const store = newStore(db);
    const lost = new Error('lost inventory response');
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const input = {
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      };
      if (response === 'hidden results') db.hideBatchResults = true;
      if (response === 'lost response')
        db.afterBatch = () => {
          throw lost;
        };
      const result = await lease
        .commitChunk(input)
        .catch((error: unknown) => error);
      expect(await store.readRunByOperation(OPERATION_ID)).toEqual(
        input.runRecord,
      );
      expect(result).toEqual(
        response === 'lost response' ? lost : input.runRecord,
      );
      const before = inventoryState(db);
      expect(await lease.commitChunk(input)).toEqual(input.runRecord);
      expect(inventoryState(db)).toEqual(before);
      const next = committed(input.runRecord, DEFAULT_ROWS, DEFAULT_FACTS);
      expect(
        await lease.commitChunk({
          ...input,
          expectedRevision: 1,
          runRecord: next,
        }),
      ).toEqual(next);
      expect(
        db.sqlite
          .prepare(
            'SELECT payload FROM anchorage_fleet_inventory_rows ORDER BY rowid',
          )
          .all(),
      ).toEqual(
        DEFAULT_ROWS.map((row) => ({ payload: JSON.stringify(row.payload) })),
      );
      expect(
        db.sqlite
          .prepare(
            'SELECT payload FROM anchorage_fleet_inventory_deployment_facts ORDER BY rowid',
          )
          .all(),
      ).toEqual(
        DEFAULT_FACTS.map((fact) => ({
          payload: JSON.stringify(fact.payload),
        })),
      );
    });
  });

  it('captures validated input before awaiting the batch', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const input = structuredClone({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      });
      const intended = structuredClone(input.runRecord);
      db.hideBatchResults = true;
      db.afterBatch = () => {
        Object.assign(input.runRecord.progress, { generation: 99 });
        Object.assign(input.rows[0]?.payload ?? {}, { scriptName: 'changed' });
        Object.assign(input.facts[0]?.payload ?? {}, { name: 'changed' });
      };
      const result = await lease.commitChunk(input);
      expect(await store.readRunByOperation(OPERATION_ID)).toEqual(intended);
      expect(result).toEqual(intended);
    });
  });

  it.each([
    'row',
    'fact',
  ] as const)('does not converge with a missing or divergent %s payload', async (kind) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const input = {
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      };
      await lease.commitChunk(input);
      const table =
        kind === 'row'
          ? 'anchorage_fleet_inventory_rows'
          : 'anchorage_fleet_inventory_deployment_facts';
      db.sqlite
        .prepare(`UPDATE ${table} SET payload = ? WHERE rowid = 1`)
        .all('{"different":true}');
      let before = inventoryState(db);
      let result = await lease
        .commitChunk(input)
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toEqual(
        new Error(
          `fleet inventory run '${OPERATION_ID}' staged rows diverge from the persisted generation`,
        ),
      );
      db.sqlite.prepare(`DELETE FROM ${table} WHERE rowid = 1`).all();
      before = inventoryState(db);
      result = await lease.commitChunk(input).catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toEqual(
        new Error(
          `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`,
        ),
      );
    });
  });
});

describe('inventory lifecycle physical identity', () => {
  it.each([
    'operation',
    'generation',
    'options',
  ] as const)('refuses reads of inconsistent physical %s metadata', async (field) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    const record = await store.readRunByOperation(OPERATION_ID);
    if (!record) throw new Error('missing fixture run');
    const corrupt =
      field === 'operation'
        ? { ...record, operationId: SECOND_OPERATION_ID }
        : field === 'generation'
          ? { ...record, progress: { ...record.progress, generation: 99 } }
          : { ...record, options: OTHER_OPTIONS, optionsDigest: OTHER_DIGEST };
    db.sqlite
      .prepare(
        'UPDATE anchorage_fleet_inventory_runs SET run_record = ? WHERE operation_id = ?',
      )
      .all(JSON.stringify(corrupt), OPERATION_ID);
    const before = inventoryState(db);
    const results = await Promise.all([
      store.readRunByOperation(OPERATION_ID).catch((error: unknown) => error),
      store.latestFinalizedGeneration().catch((error: unknown) => error),
      store.readFinalizedGeneration(1).catch((error: unknown) => error),
      store
        .pinGeneration({ generation: 1, pinnedBy: 'reader' })
        .catch((error: unknown) => error),
    ]);
    expect(inventoryState(db)).toEqual(before);
    for (const result of results)
      expect(result).toEqual(
        new Error('fleet inventory generation 1 is corrupt'),
      );
  });

  it.each([
    { method: 'finalize', field: 'account_id', value: 'account-secondary' },
    { method: 'finalize', field: 'generation', value: 99 },
    { method: 'finalize', field: 'options_digest', value: OTHER_DIGEST },
    { method: 'fail', field: 'account_id', value: 'account-secondary' },
    { method: 'fail', field: 'generation', value: 99 },
    { method: 'fail', field: 'options_digest', value: OTHER_DIGEST },
  ])('does not $method a run whose physical $field changes after the read', async ({
    method,
    field,
    value,
  }) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      let before: unknown;
      db.beforeBatch = () => {
        db.sqlite
          .prepare(
            `UPDATE anchorage_fleet_inventory_runs SET ${field} = ? WHERE operation_id = ?`,
          )
          .all(value, OPERATION_ID);
        before = inventoryState(db);
      };
      const operation =
        method === 'finalize'
          ? lease.finalizeRun({
              operationId: OPERATION_ID,
              expectedRevision: 0,
              manifest: started.progress.stagedCounts,
              factCount: 0,
            })
          : lease.failRun({
              operationId: OPERATION_ID,
              expectedRevision: 0,
              reason: 'operator-abandoned',
            });
      const result = await operation.catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toBeInstanceOf(Error);
    });
  });

  it('does not rewind the latest generation or clear a newer active run on finalize replay', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    await seedGeneration(store, SECOND_OPERATION_ID);
    await store.withAccountInventoryLease(async (lease) => {
      await start(lease, THIRD_OPERATION_ID);
      const before = inventoryState(db);
      const ref = await lease.finalizeRun({
        operationId: OPERATION_ID,
        expectedRevision: 1,
        manifest: countsOf(DEFAULT_ROWS),
        factCount: DEFAULT_FACTS.length,
      });
      expect(inventoryState(db)).toEqual(before);
      expect(ref.generation).toBe(1);
    });
  });
});

describe('inventory lifecycle replay selection', () => {
  it('repairs failure replay after lease expiry between the failed run and head clear', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const staged = await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      return lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      });
    });
    const input = {
      operationId: OPERATION_ID,
      expectedRevision: staged.progress.revision,
      reason: 'operator-abandoned',
    } as const;
    db.beforeStatement = (index) => {
      if (index === 1)
        db.sqlite
          .prepare('UPDATE anchorage_fleet_inventory_leases SET expires_at = 0')
          .all();
    };
    const interrupted = await store
      .withAccountInventoryLease((lease) => lease.failRun(input))
      .catch((error: unknown) => error);
    expect(await store.readRunByOperation(OPERATION_ID)).toEqual({
      ...staged,
      state: 'failed',
    });
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
    expect(interrupted).toEqual(
      new Error(
        "fleet inventory for account 'account-primary' lease is no longer owned by this operation",
      ),
    );

    const beforeRepair = inventoryState(db);
    const stale = await store
      .withAccountInventoryLease((lease) =>
        lease.failRun({ ...input, expectedRevision: 0 }),
      )
      .catch((error: unknown) => error);
    expect(inventoryState(db)).toEqual(beforeRepair);
    expect(stale).toEqual(
      new Error(
        `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`,
      ),
    );
    await store.withAccountInventoryLease((lease) => lease.failRun(input));
    expect(
      db.sqlite.prepare('SELECT * FROM anchorage_fleet_inventory_heads').all(),
    ).toEqual([
      {
        account_id: 'account-primary',
        active_operation_id: null,
        latest_finalized_generation: null,
        next_generation: 2,
      },
    ]);
    expect(await store.readRunByOperation(OPERATION_ID)).toEqual({
      ...staged,
      state: 'failed',
    });
    expect(await seedGeneration(store, SECOND_OPERATION_ID)).toBe(2);
    await store.withAccountInventoryLease(async (lease) => {
      expect((await start(lease, THIRD_OPERATION_ID)).progress.generation).toBe(
        3,
      );
      const beforeReplay = inventoryState(db);
      await lease.failRun(input);
      expect(inventoryState(db)).toEqual(beforeReplay);
    });
  });

  it.each([
    'original',
    'stale',
    'fallback',
    'start',
  ] as const)('repairs coordinator finalization interrupted by lease expiry through the %s retry path', async (retry) => {
    const db = new MemoryD1();
    const store = newStore(db);
    const advanceStage = vi.fn<FleetInventoryProviderContext['advanceStage']>(
      async () => ({
        rows: [],
        facts: [],
        nextStage: { step: 'finalize' },
        providerRequests: 0,
        diagnostics: [],
      }),
    );
    const options = {
      context: { advanceStage },
      store,
      maxProviderRequests: 9,
    };
    const startAction = {
      kind: 'start',
      operationId: OPERATION_ID,
      options: OPTIONS,
    } as const;
    const pending = await advanceFleetInventory({
      ...options,
      action: startAction,
    });
    expect(pending.status).toBe('pending');
    const staged = await store.readRunByOperation(OPERATION_ID);
    db.beforeStatement = (index) => {
      if (index === 1)
        db.sqlite
          .prepare('UPDATE anchorage_fleet_inventory_leases SET expires_at = 0')
          .all();
    };
    const interrupted = await advanceFleetInventory({
      ...options,
      action: { kind: 'continue', token: pending.token },
    }).catch((error: unknown) => error);
    const finalized = { ...staged, state: 'finalized' };
    expect(await store.readRunByOperation(OPERATION_ID)).toEqual(finalized);
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
    expect(interrupted).toEqual(
      new Error(
        "fleet inventory for account 'account-primary' lease is no longer owned by this operation",
      ),
    );
    if (retry === 'fallback') {
      const withLease = store.withAccountInventoryLease.bind(store);
      vi.spyOn(store, 'withAccountInventoryLease').mockImplementation(
        (operation) =>
          withLease((lease) =>
            operation({ ...lease, readRun: async () => undefined }),
          ),
      );
    }
    const action =
      retry === 'start'
        ? startAction
        : ({
            kind: 'continue',
            token: {
              ...pending.token,
              revision: retry === 'stale' ? 0 : pending.token.revision,
            },
          } as const);
    const repaired = await advanceFleetInventory({ ...options, action }).catch(
      (error: unknown) => error,
    );
    expect(
      db.sqlite.prepare('SELECT * FROM anchorage_fleet_inventory_heads').all(),
    ).toEqual([
      {
        account_id: 'account-primary',
        active_operation_id: null,
        latest_finalized_generation: 1,
        next_generation: 2,
      },
    ]);
    expect(await store.readRunByOperation(OPERATION_ID)).toEqual(finalized);
    const first = (await store.readFinalizedGeneration(1)).ref;
    expect(repaired).toEqual({
      status: 'complete',
      token: pending.token,
      generation: first,
    });
    expect(advanceStage).toHaveBeenCalledTimes(1);

    expect(await seedGeneration(store, SECOND_OPERATION_ID)).toBe(2);
    await store.withAccountInventoryLease((lease) =>
      start(lease, THIRD_OPERATION_ID),
    );
    const beforeUnpinned = inventoryState(db);
    const unpinned = await advanceFleetInventory({ ...options, action }).catch(
      (error: unknown) => error,
    );
    expect(inventoryState(db)).toEqual(beforeUnpinned);
    expect(unpinned).toEqual(
      new Error(
        'fleet inventory generation 1 requires a pin before it can be read',
      ),
    );
    await store.pinGeneration({ generation: 1, pinnedBy: 'historical-reader' });
    const beforePinned = inventoryState(db);
    const pinned = await advanceFleetInventory({ ...options, action });
    expect(inventoryState(db)).toEqual(beforePinned);
    expect(pinned).toEqual(repaired);
    expect(advanceStage).toHaveBeenCalledTimes(1);
  });

  it.each([
    'revision',
    'row manifest',
    'fact manifest',
  ] as const)('refuses a finalized replay with a different %s', async (field) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    const before = inventoryState(db);
    const result = await store
      .withAccountInventoryLease((lease) =>
        lease.finalizeRun({
          operationId: OPERATION_ID,
          expectedRevision: field === 'revision' ? 0 : 1,
          manifest:
            field === 'row manifest'
              ? emptyFleetInventoryRowCounts()
              : countsOf(DEFAULT_ROWS),
          factCount: field === 'fact manifest' ? 0 : DEFAULT_FACTS.length,
        }),
      )
      .catch((error: unknown) => error);
    expect(inventoryState(db)).toEqual(before);
    expect(result).toEqual(
      new Error(
        field === 'revision'
          ? `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`
          : `fleet inventory run '${OPERATION_ID}' finalize manifest disagrees with the persisted run record`,
      ),
    );
  });

  it('accepts exact failure replay and refuses a different expected revision', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      await lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      });
      const input = {
        operationId: OPERATION_ID,
        expectedRevision: 1,
        reason: 'operator-abandoned',
      } as const;
      await lease.failRun(input);
      const before = inventoryState(db);
      await lease.failRun(input);
      expect(inventoryState(db)).toEqual(before);
      const result = await lease
        .failRun({ ...input, expectedRevision: 0 })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toEqual(
        new Error(
          `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`,
        ),
      );
    });
  });
});

describe('inventory chunk storage boundaries', () => {
  it('propagates a database failure without changing durable state', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const before = inventoryState(db);
      const unavailable = new Error('database unavailable');
      db.beforeBatch = () => {
        throw unavailable;
      };
      const result = await lease
        .commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
          rows: DEFAULT_ROWS,
          facts: DEFAULT_FACTS,
        })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(result).toBe(unavailable);
    });
  });

  it('retains earlier inserts when the lease expires before progress and permits exact recovery', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const expiry = db.sqlite
        .prepare('SELECT expires_at FROM anchorage_fleet_inventory_leases')
        .all()[0]?.expires_at;
      db.beforeStatement = (index) => {
        if (index === 1)
          db.sqlite
            .prepare(
              'UPDATE anchorage_fleet_inventory_leases SET expires_at = 0',
            )
            .all();
      };
      const input = {
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      };
      const result = await lease
        .commitChunk(input)
        .catch((error: unknown) => error);
      db.sqlite
        .prepare('UPDATE anchorage_fleet_inventory_leases SET expires_at = ?')
        .all(expiry);
      expect(await store.readRunByOperation(OPERATION_ID)).toEqual(started);
      expect(
        db.sqlite
          .prepare(
            'SELECT kind, ordinal, payload FROM anchorage_fleet_inventory_rows',
          )
          .all(),
      ).toEqual([
        {
          kind: 'registration',
          ordinal: 0,
          payload: JSON.stringify(DEFAULT_ROWS[0]?.payload),
        },
      ]);
      expect(
        db.sqlite
          .prepare('SELECT * FROM anchorage_fleet_inventory_deployment_facts')
          .all(),
      ).toEqual([]);
      expect(result).toEqual(
        new Error(
          `fleet inventory run '${OPERATION_ID}' is no longer at the expected revision`,
        ),
      );
      expect(await lease.commitChunk(input)).toEqual(input.runRecord);
      expect(
        await lease.finalizeRun({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          manifest: input.runRecord.progress.stagedCounts,
          factCount: input.runRecord.progress.factCount,
        }),
      ).toMatchObject({ generation: 1 });
    });
  });

  it('commits and replays a 2000-entry mixed chunk without combining payloads', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    const rows = Array.from({ length: 1000 }, (_, ordinal) =>
      stagedRow('meta', ordinal, { name: `row-${ordinal}` }),
    );
    const facts = Array.from({ length: 1000 }, (_, ordinal) =>
      stagedFact(ordinal, 0),
    );
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const input = {
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, rows, facts),
        rows,
        facts,
      };
      db.hideBatchResults = true;
      expect(await lease.commitChunk(input)).toEqual(input.runRecord);
      expect(await lease.commitChunk(input)).toEqual(input.runRecord);
      expect(
        await lease.finalizeRun({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          manifest: input.runRecord.progress.stagedCounts,
          factCount: facts.length,
        }),
      ).toMatchObject({ generation: 1 });
    });
    const generation = await store.readFinalizedGeneration(1);
    expect(generation.rows).toEqual(rows);
    expect(generation.facts).toEqual(facts);
  });
});

describe('inventory target recovery controls', () => {
  it('isolates equal row and fact ordinals in different accounts and generations', async () => {
    const db = new MemoryD1();
    const primary = newStore(db);
    const secondary = newStore(db, 'account-secondary');
    const first = await seedGeneration(primary);
    const differentRows = [
      stagedRow('registration', 0, { scriptName: 'different' }),
    ];
    const differentFacts = [
      { ...stagedFact(0, 0), payload: { name: 'different' } },
    ];
    const foreign = await seedGeneration(
      secondary,
      SECOND_OPERATION_ID,
      differentRows,
      differentFacts,
    );
    const next = await seedGeneration(
      primary,
      THIRD_OPERATION_ID,
      differentRows,
      differentFacts,
    );
    await primary.pinGeneration({ generation: first, pinnedBy: 'reader' });
    expect((await primary.readFinalizedGeneration(first)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
    expect((await primary.readFinalizedGeneration(first)).facts).toEqual(
      DEFAULT_FACTS,
    );
    expect((await secondary.readFinalizedGeneration(foreign)).rows).toEqual(
      differentRows,
    );
    expect((await secondary.readFinalizedGeneration(foreign)).facts).toEqual(
      differentFacts,
    );
    expect((await primary.readFinalizedGeneration(next)).rows).toEqual(
      differentRows,
    );
    expect((await primary.readFinalizedGeneration(next)).facts).toEqual(
      differentFacts,
    );
  });

  it('repairs an interrupted finalize head for the same operation', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    db.sqlite
      .prepare(
        'UPDATE anchorage_fleet_inventory_heads SET latest_finalized_generation = NULL, active_operation_id = ?',
      )
      .all(OPERATION_ID);
    const record = await store.readRunByOperation(OPERATION_ID);
    const ref = await store.withAccountInventoryLease((lease) =>
      lease.finalizeRun({
        operationId: OPERATION_ID,
        expectedRevision: 1,
        manifest: countsOf(DEFAULT_ROWS),
        factCount: DEFAULT_FACTS.length,
      }),
    );
    expect(await store.readRunByOperation(OPERATION_ID)).toEqual(record);
    expect(await store.latestFinalizedGeneration()).toEqual(ref);
    expect(
      db.sqlite
        .prepare(
          'SELECT active_operation_id FROM anchorage_fleet_inventory_heads',
        )
        .all(),
    ).toEqual([{ active_operation_id: null }]);
  });
});

describe('inventory start input integrity', () => {
  it.each([
    'absent',
    'finalized',
  ] as const)('rolls back a cross-account start operation-ID collision when the loser head is %s', async (head) => {
    const db = new MemoryD1();
    const primary = newStore(db);
    const secondary = newStore(db, 'account-secondary');
    await primary.withAccountInventoryLease((lease) => start(lease));
    if (head === 'finalized')
      await seedGeneration(secondary, SECOND_OPERATION_ID);
    const before = inventoryState(db);
    const collision = await secondary
      .withAccountInventoryLease((lease) => start(lease))
      .catch((error: unknown) => error);
    expect(inventoryState(db)).toEqual(before);
    expect(await secondary.readRunByOperation(OPERATION_ID)).toBeUndefined();
    expect(collision).toBeInstanceOf(Error);
    expect((collision as Error).message).toMatch(/UNIQUE constraint failed/);
    const fresh = await secondary.withAccountInventoryLease((lease) =>
      start(lease, THIRD_OPERATION_ID),
    );
    expect(fresh.progress.generation).toBe(head === 'finalized' ? 2 : 1);
    expect(await secondary.readRunByOperation(THIRD_OPERATION_ID)).toEqual(
      fresh,
    );
  });

  it('rolls back the loser of concurrent cross-account starts with the same operation ID', async () => {
    const db = new MemoryD1();
    const stores = [newStore(db), newStore(db, 'account-secondary')];
    await Promise.all(stores.map((store) => store.latestFinalizedGeneration()));
    const results = await Promise.allSettled(
      stores.map((store) =>
        store.withAccountInventoryLease((lease) => start(lease)),
      ),
    );
    const loserIndex = results.findIndex(
      (result) => result.status === 'rejected',
    );
    const loser = stores[loserIndex];
    if (!loser) throw new Error('concurrent starts did not select a loser');
    const loserAccount =
      loserIndex === 0 ? 'account-primary' : 'account-secondary';
    expect(
      db.sqlite
        .prepare(
          'SELECT * FROM anchorage_fleet_inventory_heads WHERE account_id = ?',
        )
        .all(loserAccount),
    ).toEqual([]);
    expect(await loser.readRunByOperation(OPERATION_ID)).toBeUndefined();
    expect(
      db.sqlite
        .prepare(
          'SELECT account_id, generation FROM anchorage_fleet_inventory_runs',
        )
        .all(),
    ).toEqual([
      {
        account_id: loserIndex === 0 ? 'account-secondary' : 'account-primary',
        generation: 1,
      },
    ]);
    expect(
      results.filter((result) => result.status === 'fulfilled'),
    ).toHaveLength(1);
    const collision = results[loserIndex];
    expect(collision).toMatchObject({
      status: 'rejected',
      reason: expect.any(Error),
    });
    if (collision?.status !== 'rejected')
      throw new Error('collision unexpectedly resolved');
    expect((collision.reason as Error).message).toMatch(
      /UNIQUE constraint failed/,
    );
    const fresh = await loser.withAccountInventoryLease((lease) =>
      start(lease, SECOND_OPERATION_ID),
    );
    expect(fresh.progress.generation).toBe(1);
    expect(await loser.readRunByOperation(SECOND_OPERATION_ID)).toEqual(fresh);
  });

  it.each([
    {
      label: 'operation id',
      operationId: 'not-an-operation-id',
      optionsDigest: DIGEST,
    },
    {
      label: 'options digest',
      operationId: OPERATION_ID,
      optionsDigest: OTHER_DIGEST,
    },
  ])('refuses invalid $label before persisting a run or claiming its head', async ({
    operationId,
    optionsDigest,
  }) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const before = inventoryState(db);
      let caught: unknown;
      try {
        await lease.startRun({ operationId, options: OPTIONS, optionsDigest });
      } catch (error) {
        caught = error;
      }
      expect(inventoryState(db)).toEqual(before);
      expect(caught).toBeInstanceOf(FleetInventoryStateError);
    });
  });
});

describe('inventory pruning preserves terminal recovery', () => {
  it.each([
    'failed',
    'finalized',
  ] as const)('retains an active %s generation until exact head repair', async (terminal) => {
    const db = new MemoryD1();
    const store = newStore(db);
    let staged: FleetInventoryRunRecord | undefined;
    const interrupted = await store
      .withAccountInventoryLease(async (lease) => {
        const started = await start(lease);
        staged = await lease.commitChunk({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
          rows: DEFAULT_ROWS,
          facts: DEFAULT_FACTS,
        });
        db.beforeStatement = (index) => {
          if (index === 1)
            db.sqlite
              .prepare(
                'UPDATE anchorage_fleet_inventory_leases SET expires_at = 0',
              )
              .all();
        };
        if (terminal === 'failed')
          await lease.failRun({
            operationId: OPERATION_ID,
            expectedRevision: 1,
            reason: 'operator-abandoned',
          });
        else
          await lease.finalizeRun({
            operationId: OPERATION_ID,
            expectedRevision: 1,
            manifest: staged.progress.stagedCounts,
            factCount: staged.progress.factCount,
          });
      })
      .catch((error: unknown) => error);
    expect(interrupted).toBeInstanceOf(Error);
    expect((await store.readRunByOperation(OPERATION_ID))?.state).toBe(
      terminal,
    );
    const before = inventoryState(db);
    const pruned = await store.pruneInventoryGenerations({ limit: 1 });
    expect(inventoryState(db)).toEqual(before);
    expect(pruned).toEqual({ deleted: 0 });
    const intended = staged;
    if (!intended) throw new Error('inventory fixture did not stage its rows');
    await store.withAccountInventoryLease(async (lease) => {
      if (terminal === 'failed')
        await lease.failRun({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          reason: 'operator-abandoned',
        });
      else
        await lease.finalizeRun({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          manifest: intended.progress.stagedCounts,
          factCount: intended.progress.factCount,
        });
    });
    expect(await seedGeneration(store, SECOND_OPERATION_ID)).toBe(2);
    await store.withAccountInventoryLease((lease) =>
      start(lease, THIRD_OPERATION_ID),
    );
    expect(await store.pruneInventoryGenerations({ limit: 1 })).toEqual({
      deleted: 1,
    });
    expect(await store.readRunByOperation(OPERATION_ID)).toBeUndefined();
    expect((await store.readFinalizedGeneration(2)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
    expect(
      db.sqlite
        .prepare(
          'SELECT active_operation_id, latest_finalized_generation, next_generation FROM anchorage_fleet_inventory_heads',
        )
        .all(),
    ).toEqual([
      {
        active_operation_id: THIRD_OPERATION_ID,
        latest_finalized_generation: 2,
        next_generation: 4,
      },
    ]);
  });

  it('rechecks active ownership in the row, fact and run delete batch', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      await lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, DEFAULT_ROWS, DEFAULT_FACTS),
        rows: DEFAULT_ROWS,
        facts: DEFAULT_FACTS,
      });
      await lease.failRun({
        operationId: OPERATION_ID,
        expectedRevision: 1,
        reason: 'operator-abandoned',
      });
    });
    let promoted: unknown;
    db.beforeBatch = () => {
      db.sqlite
        .prepare(
          'UPDATE anchorage_fleet_inventory_heads SET active_operation_id = ?',
        )
        .all(OPERATION_ID);
      promoted = inventoryState(db);
    };
    const result = await store.pruneInventoryGenerations({ limit: 1 });
    expect(promoted).toBeDefined();
    expect(inventoryState(db)).toEqual(promoted);
    expect(result).toEqual({ deleted: 0 });
  });
});

describe('inventory pin admission and concurrent reclamation', () => {
  it('refuses a pin when concurrent pruning wins without leaving an orphan pin', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    await seedGeneration(store, SECOND_OPERATION_ID);
    const outcomes = await store.withAccountInventoryLease((lease) =>
      Promise.allSettled([
        lease.pruneInventoryGenerations({ limit: 1 }),
        lease.pinGeneration({ generation: 1, pinnedBy: 'reader' }),
      ]),
    );
    expect(await store.readRunByOperation(OPERATION_ID)).toBeUndefined();
    expect(
      db.sqlite.prepare('SELECT * FROM anchorage_fleet_inventory_pins').all(),
    ).toEqual([]);
    expect(outcomes[0]).toEqual({ status: 'fulfilled', value: { deleted: 1 } });
    expect(outcomes[1]).toEqual({
      status: 'rejected',
      reason: new Error('fleet inventory generation 1 is not finalized'),
    });
    expect((await store.readFinalizedGeneration(2)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
  });

  it('preserves a generation pinned after pruning selected it', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    await seedGeneration(store, SECOND_OPERATION_ID);
    const selected = deferred<void>();
    const release = deferred<void>();
    const query = db.query.bind(db);
    db.query = async (sql, bindings) => {
      const result = await query(sql, bindings);
      if (sql.includes('ORDER BY r.generation ASC')) {
        selected.resolve();
        await release.promise;
      }
      return result;
    };
    await store.withAccountInventoryLease(async (lease) => {
      const pruning = lease.pruneInventoryGenerations({ limit: 1 });
      try {
        expect(
          await Promise.race([
            selected.promise.then(() => true),
            pruning.then(() => false),
          ]),
        ).toBe(true);
        await lease.pinGeneration({ generation: 1, pinnedBy: 'reader' });
      } finally {
        release.resolve();
      }
      expect(await pruning).toEqual({ deleted: 0 });
    });
    expect(
      db.sqlite
        .prepare(
          'SELECT generation, pinned_by FROM anchorage_fleet_inventory_pins',
        )
        .all(),
    ).toEqual([{ generation: 1, pinned_by: 'reader' }]);
    expect((await store.readFinalizedGeneration(1)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
    expect((await store.readFinalizedGeneration(1)).facts).toEqual(
      DEFAULT_FACTS,
    );
  });
});

describe('inventory retained payload availability', () => {
  it.each([
    1, 2,
  ])('refuses a pin after reclamation expires at payload boundary %s and lets cleanup finish', async (boundary) => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    await seedGeneration(store, SECOND_OPERATION_ID);
    db.beforeStatement = (index) => {
      if (index === boundary)
        db.sqlite
          .prepare('UPDATE anchorage_fleet_inventory_leases SET expires_at = 0')
          .all();
    };
    const interrupted = await store
      .pruneInventoryGenerations({ limit: 1 })
      .catch((error: unknown) => error);
    expect((await store.readRunByOperation(OPERATION_ID))?.state).toBe(
      'finalized',
    );
    expect(
      db.sqlite
        .prepare(
          'SELECT * FROM anchorage_fleet_inventory_rows WHERE generation = 1',
        )
        .all(),
    ).toEqual([]);
    expect(
      db.sqlite
        .prepare(
          'SELECT * FROM anchorage_fleet_inventory_deployment_facts WHERE generation = 1',
        )
        .all(),
    ).toHaveLength(boundary === 1 ? DEFAULT_FACTS.length : 0);
    expect(interrupted).toBeInstanceOf(Error);
    const partial = inventoryState(db);
    const pin = await store
      .pinGeneration({ generation: 1, pinnedBy: 'reader' })
      .catch((error: unknown) => error);
    expect(inventoryState(db)).toEqual(partial);
    expect(
      db.sqlite.prepare('SELECT * FROM anchorage_fleet_inventory_pins').all(),
    ).toEqual([]);
    expect(pin).toEqual(new Error('fleet inventory generation 1 is corrupt'));
    expect(await store.pruneInventoryGenerations({ limit: 1 })).toEqual({
      deleted: 1,
    });
    expect(await store.readRunByOperation(OPERATION_ID)).toBeUndefined();
    expect((await store.readFinalizedGeneration(2)).rows).toEqual(
      DEFAULT_ROWS_READ_ORDER,
    );
    expect((await store.readFinalizedGeneration(2)).facts).toEqual(
      DEFAULT_FACTS,
    );
  });

  it('checks payload availability in the pin write after a complete preflight', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await seedGeneration(store);
    await seedGeneration(store, SECOND_OPERATION_ID);
    let partial: unknown;
    db.beforeBatch = () => {
      db.sqlite
        .prepare(
          'DELETE FROM anchorage_fleet_inventory_rows WHERE generation = 1',
        )
        .all();
      partial = inventoryState(db);
    };
    const pin = await store
      .pinGeneration({ generation: 1, pinnedBy: 'reader' })
      .catch((error: unknown) => error);
    expect(partial).toBeDefined();
    expect(inventoryState(db)).toEqual(partial);
    expect(pin).toEqual(new Error('fleet inventory generation 1 is corrupt'));
  });

  it('refuses finalization with gapped row ordinals and accepts the completed prefix', async () => {
    const db = new MemoryD1();
    const store = newStore(db);
    await store.withAccountInventoryLease(async (lease) => {
      const started = await start(lease);
      const rows = [
        stagedRow('registration', 0, { scriptName: 'first-script' }),
        stagedRow('registration', 2, { scriptName: 'third-script' }),
      ];
      const current = await lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: committed(started, rows, []),
        rows,
        facts: [],
      });
      const before = inventoryState(db);
      const final = await lease
        .finalizeRun({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          manifest: current.progress.stagedCounts,
          factCount: 0,
        })
        .catch((error: unknown) => error);
      expect(inventoryState(db)).toEqual(before);
      expect(final).toEqual(
        new Error(
          `fleet inventory run '${OPERATION_ID}' does not match its finalize manifest`,
        ),
      );
      const missing = stagedRow('registration', 1, {
        scriptName: 'second-script',
      });
      const complete = await lease.commitChunk({
        operationId: OPERATION_ID,
        expectedRevision: 1,
        runRecord: committed(current, [...rows, missing], []),
        rows: [missing],
        facts: [],
      });
      await lease.finalizeRun({
        operationId: OPERATION_ID,
        expectedRevision: 2,
        manifest: complete.progress.stagedCounts,
        factCount: 0,
      });
    });
    expect(
      (await store.readFinalizedGeneration(1)).rows.map((row) => row.ordinal),
    ).toEqual([0, 1, 2]);
  });
});

it('refuses an existing pin over payloads made unreadable before this call', async () => {
  const db = new MemoryD1();
  const store = newStore(db);
  await seedGeneration(store);
  await seedGeneration(store, SECOND_OPERATION_ID);
  await db.execute(
    "UPDATE anchorage_fleet_inventory_rows SET ordinal = 1 WHERE generation = 1 AND kind = 'registration'",
  );
  await db.execute(
    'INSERT INTO anchorage_fleet_inventory_pins (account_id, generation, pinned_by, pinned_at_ms) VALUES (?, 1, ?, 0)',
    ['account-primary', 'reader'],
  );
  const before = inventoryState(db);
  const pin = await store
    .pinGeneration({ generation: 1, pinnedBy: 'reader' })
    .catch((error: unknown) => error);
  expect(inventoryState(db)).toEqual(before);
  expect(pin).toEqual(new Error('fleet inventory generation 1 is corrupt'));
  await store.releasePin({ generation: 1, pinnedBy: 'reader' });
  expect(await store.pruneInventoryGenerations({ limit: 1 })).toEqual({
    deleted: 1,
  });
});
