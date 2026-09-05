// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { D1FleetOperationStore } from '../src/d1-fleet-operation-store.js';
import type { FleetAuditProgress } from '../src/fleet-audit-state.js';
import type {
  FleetInventoryGeneration,
  FleetInventoryGenerationRef,
  FleetInventoryRunRecord,
  FleetInventoryRunStore,
} from '../src/fleet-inventory-state.js';
import type { FleetMigrationProgress } from '../src/fleet-migration-state.js';
import {
  classifyFleetOperationToken,
  type FleetOperationKind,
  type FleetOperationLease,
  type FleetOperationRowKind,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  FleetOperationStoreCapabilityError,
} from '../src/fleet-operation-state.js';
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
  /** Statement counts for each executed batch. */
  readonly batchSizes: number[] = [];
  /** Binding counts for every statement executed in batch order. */
  readonly bindingCounts: number[] = [];
  /** Makes the next committed batch drop its result rows, for lost responses. */
  hideBatchResults = false;
  /** Makes the next committed batch throw as if its response were lost. */
  failNextBatchAfterCommit = false;

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
    this.batchSizes.push(statements.length);
    this.bindingCounts.push(
      ...statements.map(({ bindings = [] }) => bindings.length),
    );
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
    if (this.failNextBatchAfterCommit) {
      this.failNextBatchAfterCommit = false;
      throw new Error('committed batch response lost');
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
const FOURTH_OPERATION_ID = '123e4567-e89b-42d3-a456-426614174003';
const FIFTH_OPERATION_ID = '123e4567-e89b-42d3-a456-426614174004';
const SIXTH_OPERATION_ID = '123e4567-e89b-42d3-a456-426614174005';
const DIGEST = 'a'.repeat(64);
const OTHER_DIGEST = 'b'.repeat(64);
const NOW = '2026-09-01T00:00:00.000Z';

function runRecord(
  kind: FleetOperationKind,
  revision = 0,
  state: 'running' | 'finalized' | 'failed' = 'running',
  operationId = OPERATION_ID,
): FleetOperationRunRecord {
  return {
    version: 1,
    operationId,
    kind,
    state,
    progress:
      kind === 'audit'
        ? ({
            kind,
            revision,
            stage: { step: 'provider-findings', rowOrdinal: 0 },
            generation: 1,
            auditTimeMs: 1_700_000_000_000,
            staleAfterMs: 60_000,
            recordCount: 2,
            findingCount: 0,
            factCount: 0,
            ...(state === 'failed'
              ? { failure: { reason: 'operator-abandoned' as const } }
              : {}),
          } as FleetAuditProgress)
        : ({
            kind,
            revision,
            itemCount: 1,
            activeItemOrdinal: 0,
            completedItemCount: state === 'finalized' ? 1 : 0,
            ...(state === 'failed'
              ? {
                  failure: {
                    reason: 'item-failed' as const,
                    itemOrdinal: 0,
                  },
                }
              : {}),
          } as FleetMigrationProgress),
    updatedAt: NOW,
  };
}

function advanced(
  record: FleetOperationRunRecord,
  state: 'running' | 'finalized' | 'failed' = 'running',
): FleetOperationRunRecord {
  const failure =
    state === 'failed'
      ? record.kind === 'audit'
        ? { reason: 'operator-abandoned' as const }
        : { reason: 'item-failed' as const, itemOrdinal: 0 }
      : undefined;
  return {
    ...record,
    state,
    progress: {
      ...record.progress,
      revision: record.progress.revision + 1,
      ...(failure === undefined ? {} : { failure }),
    },
  };
}

function recordRow(ordinal: number, label = `record-${ordinal}`) {
  return { rowKind: 'record' as const, ordinal, payload: { label } };
}

function findingRow(ordinal = 0): FleetOperationStagedRow {
  return {
    rowKind: 'finding',
    ordinal,
    payload: {
      tenantTag: 'tenant',
      environment: 'production',
      kind: 'audit-error',
      detail: `safe finding ${ordinal}`,
    },
  };
}

function payloadFindingRow(
  ordinal: number,
  detail: string,
): FleetOperationStagedRow {
  const row = findingRow(ordinal);
  return { ...row, payload: { ...row.payload, detail } };
}

function factRow(ordinal = 0): FleetOperationStagedRow {
  return {
    rowKind: 'fact',
    ordinal,
    payload: {
      factKind: 'database-owner',
      key: `database-${ordinal}`,
      tenantTag: 'tenant',
      environment: 'production',
    },
  };
}

function itemRow(
  status: 'pending' | 'active' | 'complete' | 'failed' = 'pending',
  ordinal = 0,
): FleetOperationStagedRow {
  return {
    rowKind: 'item',
    ordinal,
    payload: {
      ordinal,
      tenantTag: 'tenant',
      environment: 'production',
      entryRecordDigest: 'c'.repeat(64),
      ...(status === 'pending'
        ? {}
        : {
            targetSpecDigest: 'd'.repeat(64),
            plan: [{ step: 'promote' }],
            planCursor: status === 'complete' ? 1 : 0,
          }),
      status,
    },
  };
}

function store(
  db: MemoryD1,
  accountId = 'account-primary',
  inventoryStore?: FleetInventoryRunStore,
): D1FleetOperationStore {
  return new D1FleetOperationStore(db, { accountId, inventoryStore });
}

function start(
  lease: FleetOperationLease,
  kind: FleetOperationKind = 'audit',
  operationId = OPERATION_ID,
  intakeDigest = DIGEST,
) {
  return lease.startOperation({
    operationId,
    kind,
    runRecord: runRecord(kind, 0, 'running', operationId),
    intakeDigest,
  });
}

async function seedTerminal(
  target: D1FleetOperationStore,
  kind: FleetOperationKind,
  operationId: string,
  state: 'finalized' | 'failed',
): Promise<FleetOperationRunRecord> {
  return target.withAccountOperationLease(kind, async (lease) => {
    const created = await start(lease, kind, operationId);
    const terminal = advanced(created.record, state);
    if (state === 'finalized') {
      return lease.finalizeOperation({
        operationId,
        expectedRevision: 0,
        runRecord: terminal,
        expectedRowCounts: {},
      });
    }
    await lease.failOperation({
      operationId,
      expectedRevision: 0,
      runRecord: terminal,
    });
    const persisted = await lease.readOperation(operationId);
    if (!persisted) throw new Error('failed seed operation disappeared');
    return persisted;
  });
}

async function rejection(operation: Promise<unknown>): Promise<Error> {
  try {
    await operation;
  } catch (error) {
    return error as Error;
  }
  throw new Error('operation unexpectedly resolved');
}

function rowCount(db: MemoryD1): number {
  return Number(
    db.sqlite
      .prepare('SELECT COUNT(*) AS count FROM anchorage_fleet_operation_rows')
      .all()[0]?.count,
  );
}

class FakeInventoryStore implements FleetInventoryRunStore {
  readonly pins = new Set<string>();
  readonly releases: string[] = [];
  crashAfterRelease = false;

  withAccountInventoryLease<T>(): Promise<T> {
    throw new Error('unused inventory capability');
  }
  readFinalizedGeneration(): Promise<FleetInventoryGeneration> {
    throw new Error('unused inventory capability');
  }
  latestFinalizedGeneration(): Promise<
    FleetInventoryGenerationRef | undefined
  > {
    throw new Error('unused inventory capability');
  }
  readRunByOperation(): Promise<FleetInventoryRunRecord | undefined> {
    throw new Error('unused inventory capability');
  }
  pinGeneration(input: {
    generation: number;
    pinnedBy: string;
  }): Promise<void> {
    this.pins.add(`${input.generation}:${input.pinnedBy}`);
    return Promise.resolve();
  }
  releasePin(input: { generation: number; pinnedBy: string }): Promise<void> {
    const key = `${input.generation}:${input.pinnedBy}`;
    this.pins.delete(key);
    this.releases.push(key);
    if (this.crashAfterRelease) {
      this.crashAfterRelease = false;
      return Promise.reject(new Error('crash after pin release'));
    }
    return Promise.resolve();
  }
  pruneInventoryGenerations(): Promise<Readonly<{ deleted: number }>> {
    throw new Error('unused inventory capability');
  }
}

describe('D1FleetOperationStore', () => {
  it('four-table schema + PRAGMA drift fail-closed', async () => {
    const db = new MemoryD1();
    await store(db).readOperationById(OPERATION_ID);
    const tables = db.sqlite
      .prepare(
        `SELECT name FROM sqlite_master
          WHERE type = 'table' AND name LIKE 'anchorage_fleet_operation_%'
          ORDER BY name`,
      )
      .all()
      .map((row) => String(row.name));
    expect(tables).toEqual([
      'anchorage_fleet_operation_heads',
      'anchorage_fleet_operation_leases',
      'anchorage_fleet_operation_rows',
      'anchorage_fleet_operations',
    ]);
    const drifted = new MemoryD1();
    drifted.sqlite.exec(`CREATE TABLE anchorage_fleet_operation_heads (
      account_id TEXT, operation_kind TEXT, active_operation_id INTEGER
    )`);
    await expect(
      store(drifted).readOperationById(OPERATION_ID),
    ).rejects.toThrow("column 'active_operation_id' is absent or incompatible");
  });

  it("startOperation claims the head and returns outcome: 'created'", async () => {
    const db = new MemoryD1();
    const result = await store(db).withAccountOperationLease('audit', (lease) =>
      start(lease),
    );
    expect(result).toEqual({
      outcome: 'created',
      record: runRecord('audit', 0),
    });
    expect(
      db.sqlite
        .prepare(
          'SELECT active_operation_id FROM anchorage_fleet_operation_heads',
        )
        .all()[0]?.active_operation_id,
    ).toBe(OPERATION_ID);
  });

  it("start replay on a TERMINAL operation returns outcome: 'adopted-terminal' with the persisted record", async () => {
    const db = new MemoryD1();
    const target = store(db);
    const terminal = await seedTerminal(
      target,
      'audit',
      OPERATION_ID,
      'finalized',
    );
    const replay = await target.withAccountOperationLease('audit', (lease) =>
      start(lease),
    );
    expect(replay).toEqual({ outcome: 'adopted-terminal', record: terminal });
  });

  it("start probe on a RUNNING digest-match returns outcome: 'adopted-running' and the resume's revision-1 commit converges idempotently", async () => {
    const db = new MemoryD1();
    const target = store(db);
    let intended: FleetOperationRunRecord | undefined;
    await target.withAccountOperationLease('audit', async (lease) => {
      const created = await start(lease);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [recordRow(0)],
      });
      intended = advanced(created.record);
      await lease.commitProgress({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: intended,
        expectedRowWatermarks: { record: 1 },
      });
    });
    const replay = await target.withAccountOperationLease(
      'audit',
      async (lease) => {
        const adopted = await start(lease);
        const converged = await lease.commitProgress({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: intended as FleetOperationRunRecord,
          expectedRowWatermarks: { record: 1 },
        });
        return { adopted, converged };
      },
    );
    expect(replay.adopted.outcome).toBe('adopted-running');
    expect(replay.adopted.record.progress.revision).toBe(1);
    expect(replay.converged).toEqual(intended);
  });

  it('intake-digest mismatch conflict', async () => {
    const db = new MemoryD1();
    const target = store(db);
    await target.withAccountOperationLease('audit', (lease) => start(lease));
    await expect(
      target.withAccountOperationLease('audit', (lease) =>
        start(lease, 'audit', OPERATION_ID, OTHER_DIGEST),
      ),
    ).rejects.toThrow(
      `fleet operation '${OPERATION_ID}' already exists with a different intake`,
    );
  });

  it('same-kind foreign active operation contends', async () => {
    const db = new MemoryD1();
    const target = store(db);
    await target.withAccountOperationLease('audit', (lease) => start(lease));
    await expect(
      target.withAccountOperationLease('audit', (lease) =>
        start(lease, 'audit', SECOND_OPERATION_ID),
      ),
    ).rejects.toThrow(
      'another fleet audit operation is active for this account',
    );
  });

  it('a NULL head is not re-claimed for an existing operation', async () => {
    const db = new MemoryD1();
    const target = store(db);
    await seedTerminal(target, 'audit', OPERATION_ID, 'finalized');
    await target.withAccountOperationLease('audit', (lease) => start(lease));
    expect(
      db.sqlite
        .prepare(
          `SELECT active_operation_id FROM anchorage_fleet_operation_heads
            WHERE operation_kind = 'audit'`,
        )
        .all()[0]?.active_operation_id,
    ).toBeNull();
  });

  it('cross-kind operation-id reuse fails closed', async () => {
    const db = new MemoryD1();
    const target = store(db);
    await seedTerminal(target, 'audit', OPERATION_ID, 'failed');
    await expect(
      target.withAccountOperationLease('migration', (lease) =>
        start(lease, 'migration'),
      ),
    ).rejects.toThrow(
      `fleet operation '${OPERATION_ID}' belongs to the other operation kind`,
    );
  });

  it("the other kind's active operation does NOT contend", async () => {
    const db = new MemoryD1();
    const target = store(db);
    await target.withAccountOperationLease('audit', (lease) => start(lease));
    await expect(
      target.withAccountOperationLease('migration', (lease) =>
        start(lease, 'migration', SECOND_OPERATION_ID),
      ),
    ).resolves.toMatchObject({ outcome: 'created' });
  });

  it('two-account same-UUID isolation (composite PK)', async () => {
    const db = new MemoryD1();
    const first = store(db, 'account-one');
    const second = store(db, 'account-two');
    await first.withAccountOperationLease('audit', (lease) => start(lease));
    await second.withAccountOperationLease('audit', (lease) => start(lease));
    expect(
      db.sqlite
        .prepare(
          `SELECT account_id FROM anchorage_fleet_operations
            WHERE operation_id = ? ORDER BY account_id`,
        )
        .all(OPERATION_ID)
        .map((row) => row.account_id),
    ).toEqual(['account-one', 'account-two']);
  });

  it('stageRows guarded inserts land zero rows on stale lease/revision', async () => {
    const staleRevisionDb = new MemoryD1();
    const target = store(staleRevisionDb);
    await target.withAccountOperationLease('audit', async (lease) => {
      await start(lease);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 1,
        rows: [recordRow(0)],
      });
    });
    expect(rowCount(staleRevisionDb)).toBe(0);

    const staleLeaseDb = new MemoryD1();
    const staleTarget = store(staleLeaseDb);
    await rejection(
      staleTarget.withAccountOperationLease('audit', async (lease) => {
        await start(lease);
        await staleLeaseDb.execute(
          'DELETE FROM anchorage_fleet_operation_leases',
        );
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [recordRow(0)],
        });
      }),
    );
    expect(rowCount(staleLeaseDb)).toBe(0);

    const mismatchedItemDb = new MemoryD1();
    await expect(
      store(mismatchedItemDb).withAccountOperationLease(
        'migration',
        async (lease) => {
          await start(lease, 'migration');
          await lease.stageRows({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            rows: [{ ...itemRow(), ordinal: 1 }],
          });
        },
      ),
    ).rejects.toThrow('fleet operation state is malformed');
    expect(rowCount(mismatchedItemDb)).toBe(0);
  });

  it('stageRows splits at 100 statements per batch', async () => {
    const db = new MemoryD1();
    await store(db).withAccountOperationLease('audit', async (lease) => {
      await start(lease);
      db.batchSizes.length = 0;
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: Array.from({ length: 205 }, (_, index) => recordRow(index)),
      });
      expect(db.batchSizes).toEqual([100, 100, 5]);
    });
  });

  it('watermark guards hold under a retry with a DIFFERENT chunk size (surplus deterministic rows tolerated)', async () => {
    const db = new MemoryD1();
    const result = await store(db).withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: Array.from({ length: 5 }, (_, index) => recordRow(index)),
        });
        const intended = advanced(created.record);
        const transition = {
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: intended,
          rows: [
            ...Array.from({ length: 6 }, (_, index) => findingRow(index)),
            ...Array.from({ length: 6 }, (_, index) => factRow(index)),
          ],
          expectedRowWatermarks: { record: 2 },
        } as const;
        const bindingMark = db.bindingCounts.length;
        db.failNextBatchAfterCommit = true;
        await expect(lease.commitProgress(transition)).rejects.toThrow(
          'committed batch response lost',
        );
        // 12 finding/fact inserts at 17 bindings each (5 values + the 7-binding
        // operation guard + 5x1 dense-prefix watermark), then the run update at
        // 5 + 3 lease + 5x1 watermark.
        const commitBindingCounts = [
          ...Array.from({ length: 12 }, () => 17),
          13,
        ];
        expect(db.bindingCounts.slice(bindingMark)).toEqual(
          commitBindingCounts,
        );
        await expect(
          lease.commitProgress({
            ...transition,
            expectedRowWatermarks: { record: 6 },
          }),
        ).rejects.toThrow(
          `fleet operation '${OPERATION_ID}' is no longer at the expected revision`,
        );
        const converged = await lease.commitProgress(transition);
        expect(db.bindingCounts.slice(bindingMark)).toEqual([
          ...commitBindingCounts,
          ...commitBindingCounts,
          ...commitBindingCounts,
        ]);

        return converged;
      },
    );
    expect(result.progress.revision).toBe(1);
  });

  it('the watermark guard blocks a short intake at the revision-1 commit', async () => {
    const db = new MemoryD1();
    const target = store(db);
    const error = await target.withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [recordRow(0)],
        });
        return rejection(
          lease.commitProgress({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: advanced(created.record),
            expectedRowWatermarks: { record: 2 },
          }),
        );
      },
    );
    expect(error.message).toBe(
      `fleet operation '${OPERATION_ID}' is no longer at the expected revision`,
    );
    expect(
      (await target.readOperationById(OPERATION_ID))?.progress.revision,
    ).toBe(0);
  });

  it('commitProgress refuses a stale revision', async () => {
    const db = new MemoryD1();
    const target = store(db);
    let batchMark = 0;
    const error = await target.withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        batchMark = db.batchSizes.length;
        // A well-formed revision-2 -> 3 transition against a persisted
        // revision 0: the pre-SQL checks all pass, so the refusal comes from
        // the run update's own revision guard.
        return rejection(
          lease.commitProgress({
            operationId: OPERATION_ID,
            expectedRevision: 2,
            runRecord: advanced(advanced(advanced(created.record))),
          }),
        );
      },
    );
    expect(error.message).toBe(
      `fleet operation '${OPERATION_ID}' is no longer at the expected revision`,
    );
    expect(db.batchSizes.slice(batchMark)).toHaveLength(1);
    expect(
      (await target.readOperationById(OPERATION_ID))?.progress.revision,
    ).toBe(0);
  });

  it('commitProgress converges on byte-identical replay', async () => {
    const db = new MemoryD1();
    const result = await store(db).withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        const intended = advanced(created.record);
        const input = {
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: intended,
          rows: [findingRow()],
        };
        await lease.commitProgress(input);
        return lease.commitProgress(input);
      },
    );
    expect(result.progress.revision).toBe(1);
  });

  it('corruption on divergent replay', async () => {
    const db = new MemoryD1();
    const error = await store(db).withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        const intended = advanced(created.record);
        await lease.commitProgress({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [recordRow(0, 'persisted')],
          runRecord: intended,
        });
        return rejection(
          lease.commitProgress({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: intended,
            rows: [recordRow(0, 'different')],
          }),
        );
      },
    );
    expect(error.message).toBe(
      `fleet operation '${OPERATION_ID}' staged rows diverge from the persisted operation`,
    );
  });

  it('a commitProgress refused on a watermark leaves no row a later commit can converge over', async () => {
    const conflict = `fleet operation '${OPERATION_ID}' is no longer at the expected revision`;

    // Leg A: the watermark is of a kind the batch does not insert, so the
    // refusal comes from a claim the operation cannot satisfy yet.
    const legA = new MemoryD1();
    const legATarget = store(legA);
    await legATarget.withAccountOperationLease('audit', async (lease) => {
      const created = await start(lease);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [recordRow(0)],
      });
      const batchMark = legA.batchSizes.length;
      await expect(
        lease.commitProgress({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record),
          rows: [payloadFindingRow(0, 'payload A')],
          expectedRowWatermarks: { record: 2 },
        }),
      ).rejects.toThrow(conflict);
      expect(legA.batchSizes.slice(batchMark)).toHaveLength(1);
      expect(
        (
          await legATarget.readOperationRowsPage({
            operationId: OPERATION_ID,
            rowKind: 'finding',
            limit: 10,
          })
        ).rows,
      ).toEqual([]);
      expect(
        (await legATarget.readOperationById(OPERATION_ID))?.progress.revision,
      ).toBe(0);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [recordRow(1)],
      });
      const committed = await lease.commitProgress({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: advanced(created.record),
        rows: [payloadFindingRow(0, 'payload B')],
        expectedRowWatermarks: { record: 2 },
      });
      expect(committed.progress.revision).toBe(1);
    });
    expect(
      (
        await legATarget.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'finding',
          limit: 10,
        })
      ).rows[0]?.payload.detail,
    ).toBe('payload B');

    // Leg B: the watermark is the inserted row's own kind, so only the
    // pre-state dense prefix distinguishes the refusal from a commit that
    // lands the row and refuses the run update.
    const legB = new MemoryD1();
    const legBTarget = store(legB);
    await legBTarget.withAccountOperationLease('audit', async (lease) => {
      const created = await start(lease);
      const transition = {
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: advanced(created.record),
        rows: [findingRow(1)],
        expectedRowWatermarks: { finding: 2 },
      } as const;
      await expect(lease.commitProgress(transition)).rejects.toThrow(conflict);
      expect(
        (
          await legBTarget.readOperationRowsPage({
            operationId: OPERATION_ID,
            rowKind: 'finding',
            limit: 10,
          })
        ).rows,
      ).toEqual([]);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [findingRow(0)],
      });
      expect((await lease.commitProgress(transition)).progress.revision).toBe(
        1,
      );
    });
    expect(
      (
        await legBTarget.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'finding',
          limit: 10,
        })
      ).rows.map((row) => row.ordinal),
    ).toEqual([0, 1]);

    // Leg C is a regression guard, not coverage of the conjunct: it is green
    // under an unguarded insert too, and red only under a scheme that counts
    // the batch's own earlier inserts.
    const legC = new MemoryD1();
    const legCTarget = store(legC);
    await legCTarget.withAccountOperationLease('audit', async (lease) => {
      const created = await start(lease);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [findingRow(2)],
      });
      expect(
        (
          await lease.commitProgress({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: advanced(created.record),
            rows: [findingRow(0), findingRow(1), findingRow(2)],
            expectedRowWatermarks: { finding: 3 },
          })
        ).progress.revision,
      ).toBe(1);
    });
    expect(
      (
        await legCTarget.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'finding',
          limit: 10,
        })
      ).rows.map((row) => row.ordinal),
    ).toEqual([0, 1, 2]);

    // Leg D: the contiguous-run PRECONDITION the dense prefix rests on. The
    // batch's own finding inserts below the watermark are ordinals 0 and 2,
    // which is not the run [1, 3), so the store refuses before it composes a
    // statement. Delete the `below.some((row) => row.ordinal < prefix)` check
    // and finding 2 lands through a commit whose run update still refuses.
    const legD = new MemoryD1();
    const legDTarget = store(legD);
    await legDTarget.withAccountOperationLease('audit', async (lease) => {
      const created = await start(lease);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [findingRow(0)],
      });
      const batchMark = legD.batchSizes.length;
      const refused = await rejection(
        lease.commitProgress({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record),
          rows: [findingRow(0), findingRow(2)],
          expectedRowWatermarks: { finding: 3 },
        }),
      );
      expect(refused.message).toBe(
        'commitProgress finding rows below the watermark must be the contiguous run ending at it',
      );
      expect(legD.batchSizes.slice(batchMark)).toEqual([]);
    });
    expect(
      (
        await legDTarget.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'finding',
          limit: 10,
        })
      ).rows.map((row) => row.ordinal),
    ).toEqual([0]);

    // Leg E: the same conjunct on the row UPDATE, which no other title
    // reaches — title 19's watermark carries no inserts, so its `prefix`
    // equals the watermark and its UPDATE conjunct is indistinguishable from
    // the run update's. Here the UPDATE's own conjunct is the only thing
    // keeping the new payload out of the table, and the persisted `pending`
    // payload asserted after the lease is this leg's SOLE conjunct: delete
    // `${rowWatermarkSql}` AND its `...watermarkBindings.rowStatement`
    // bindings from the UPDATE and the row reads 'active' through a refused
    // commit. Deleting the SQL alone leaves the bindings over-supplied and
    // the statement raises `column index out of range` instead.
    const legE = new MemoryD1();
    const legETarget = store(legE);
    await legETarget.withAccountOperationLease('migration', async (lease) => {
      const created = await start(lease, 'migration');
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [itemRow('pending', 0)],
      });
      const batchMark = legE.batchSizes.length;
      const refused = await rejection(
        lease.commitProgress({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record),
          updateRows: [itemRow('active', 0)],
          expectedRowWatermarks: { item: 2 },
        }),
      );
      // The convergence read re-verifies the claimed watermarks BEFORE it
      // compares the persisted record, so an unsatisfiable `{item: 2}`
      // reports the conflict. That message does NOT discriminate the
      // UPDATE's own conjunct: delete the conjunct and its bindings and the
      // run update still refuses on its own watermark, and the convergence
      // read still reports the conflict.
      expect(refused.message).toBe(conflict);
      expect(legE.batchSizes.slice(batchMark)).toHaveLength(1);
    });
    expect(
      (
        await legETarget.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'item',
          limit: 10,
        })
      ).rows[0]?.payload.status,
    ).toBe('pending');
  });

  it('a guarded item-row update stands or falls with the run update', async () => {
    const db = new MemoryD1();
    const target = store(db);
    let error: Error | undefined;
    const result = await target.withAccountOperationLease(
      'migration',
      async (lease) => {
        const created = await start(lease, 'migration');
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [itemRow('pending')],
        });
        error = await rejection(
          lease.commitProgress({
            operationId: OPERATION_ID,
            expectedRevision: 1,
            runRecord: advanced(advanced(created.record)),
            updateRows: [itemRow('active')],
          }),
        );
        const pending = await lease.readOperation(OPERATION_ID);
        if (!pending) throw new Error('operation disappeared');
        return lease.commitProgress({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(pending),
          updateRows: [itemRow('active')],
          expectedRowWatermarks: { item: 1 },
        });
      },
    );
    // The refused commit intends revision 2 against a persisted revision 0,
    // so the convergence read's run-record comparison classifies it before
    // it compares the item row's bytes at all: a stale replay is a conflict,
    // not corruption.
    expect(error?.message).toBe(
      `fleet operation '${OPERATION_ID}' is no longer at the expected revision`,
    );
    expect(result.progress.revision).toBe(1);
    const page = await target.readOperationRowsPage({
      operationId: OPERATION_ID,
      rowKind: 'item',
      limit: 10,
    });
    expect(page.rows[0]?.payload.status).toBe('active');
  });

  it('a commitProgress losing the revision race to an abandon reports the conflict, not corruption', async () => {
    const db = new MemoryD1();
    const target = store(db);
    const error = await target.withAccountOperationLease(
      'migration',
      async (lease) => {
        const created = await start(lease, 'migration');
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [itemRow('pending', 0)],
        });
        // Actor A abandons, composed exactly as `abandonFleetAuditOperation`
        // composes it: the SAME revision 1 actor B's in-flight transition
        // below intends, and no `updateRows`, so A writes no row at all.
        const abandoned: FleetOperationRunRecord = {
          ...created.record,
          state: 'failed',
          progress: {
            ...created.record.progress,
            revision: 1,
            failure: { reason: 'operator-abandoned' },
          },
        };
        await lease.failOperation({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: abandoned,
        });
        const batchMark = db.batchSizes.length;
        // B derived its transition before the abandon landed. Its batch is
        // refused whole, the watermark still holds, and the target revision
        // MATCHES — only the run record's bytes and the item row's differ.
        // The run-record comparison is the sole conjunct: run the payload
        // loop first and this same call reports corruption instead.
        const refused = await rejection(
          lease.commitProgress({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: advanced(created.record),
            updateRows: [itemRow('active', 0)],
            expectedRowWatermarks: { item: 1 },
          }),
        );
        expect(db.batchSizes.slice(batchMark)).toHaveLength(1);
        return refused;
      },
    );
    expect(error.message).toBe(
      `fleet operation '${OPERATION_ID}' is no longer at the expected revision`,
    );
    // A's transition is the one that stands, and it wrote no row: the staged
    // `pending` payload is untouched.
    const persisted = await target.readOperationById(OPERATION_ID);
    expect(persisted?.state).toBe('failed');
    expect(persisted?.progress.revision).toBe(1);
    expect(
      (
        await target.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'item',
          limit: 10,
        })
      ).rows[0]?.payload.status,
    ).toBe('pending');
  });

  it('a commitProgress after the operation is pruned reports the unknown operation, not a watermark conflict', async () => {
    const db = new MemoryD1();
    const target = store(db);
    await target.withAccountOperationLease('migration', async (lease) => {
      const created = await start(lease, 'migration');
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [itemRow('pending', 0)],
      });
      // Terminal and head-released, which is what makes it a prune candidate.
      await lease.failOperation({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: advanced(created.record, 'failed'),
      });
    });
    // The shipped prune path, not a hand-rolled delete: one batch drops the
    // staged rows AND the operation record together.
    expect(
      await target.pruneFleetOperations({ kind: 'migration', limit: 10 }),
    ).toEqual({ deleted: 1, releasedPins: 0 });
    expect(await target.readOperationById(OPERATION_ID)).toBeUndefined();
    expect(rowCount(db)).toBe(0);
    const error = await target.withAccountOperationLease(
      'migration',
      async (lease) => {
        const batchMark = db.batchSizes.length;
        const refused = await rejection(
          lease.commitProgress({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: runRecord('migration', 1),
            expectedRowWatermarks: { item: 1 },
          }),
        );
        expect(db.batchSizes.slice(batchMark)).toHaveLength(1);
        return refused;
      },
    );
    // Sole conjunct: the operation-row read preceding the watermark loop.
    // Move the read back below that loop and the same call reports the
    // conflict, because every NON-ZERO watermark claim over a pruned
    // operation is unsatisfiable. The `{item: 1}` claim above is non-zero
    // ON PURPOSE: a claim of zero is satisfied by no rows at all, so under
    // the mutation it would fall through the loop and still report the
    // unknown operation, and this proof would show nothing.
    expect(error.message).toBe(`no fleet operation '${OPERATION_ID}'`);
  });

  it('the batch-budget refusal fires with its fixed message (rows + updates + 1 > 100)', async () => {
    const acceptedDb = new MemoryD1();
    await store(acceptedDb).withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        const batchMark = acceptedDb.batchSizes.length;
        const bindingMark = acceptedDb.bindingCounts.length;
        // 99 rows + 1 run-update statement = the 100-statement batch boundary.
        await lease.commitProgress({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record),
          rows: Array.from({ length: 99 }, (_, index) => recordRow(index)),
          expectedRowWatermarks: { record: 0 },
        });
        expect(acceptedDb.batchSizes.slice(batchMark)).toEqual([100]);
        // Per-statement binding counts: see the derivation comment on
        // "watermark guards hold under a retry..." above (17 per row under one
        // watermark, 13 for the run update).
        expect(acceptedDb.bindingCounts.slice(bindingMark)).toEqual([
          ...Array.from({ length: 99 }, () => 17),
          13,
        ]);
      },
    );

    const db = new MemoryD1();
    const target = store(db);
    const error = await target.withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        const batchMark = db.batchSizes.length;
        // 100 rows + 1 run-update statement is one over the boundary.
        const rejected = await rejection(
          lease.commitProgress({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: advanced(created.record),
            rows: Array.from({ length: 100 }, (_, index) => recordRow(index)),
          }),
        );
        expect(db.batchSizes.slice(batchMark)).toEqual([]);
        return rejected;
      },
    );
    expect(error.message).toBe(
      'commitProgress exceeds the operation batch budget of 100 statements',
    );
    const persisted = await target.readOperationById(OPERATION_ID);
    expect(persisted?.state).toBe('running');
    expect(persisted?.progress.revision).toBe(0);
  });

  it('finalize total-count guards (audit: finding + record + fact; migration: item)', async () => {
    const auditDb = new MemoryD1();
    const audit = await store(auditDb).withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [recordRow(0), findingRow(), factRow()],
        });
        for (const expectedRowCounts of [
          { record: 0, finding: 1, fact: 1 },
          { record: 1, finding: 0, fact: 1 },
          { record: 1, finding: 1, fact: 0 },
        ]) {
          await expect(
            lease.finalizeOperation({
              operationId: OPERATION_ID,
              expectedRevision: 0,
              runRecord: advanced(created.record, 'finalized'),
              expectedRowCounts,
            }),
          ).rejects.toThrow(
            `fleet operation '${OPERATION_ID}' does not match its finalize counts`,
          );
        }
        return lease.finalizeOperation({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record, 'finalized'),
          expectedRowCounts: { record: 1, finding: 1, fact: 1 },
        });
      },
    );
    expect(audit.state).toBe('finalized');

    const migrationDb = new MemoryD1();
    const migration = await store(migrationDb).withAccountOperationLease(
      'migration',
      async (lease) => {
        const created = await start(lease, 'migration');
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [itemRow('pending')],
        });
        await expect(
          lease.finalizeOperation({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: advanced(created.record, 'finalized'),
            expectedRowCounts: { item: 0 },
          }),
        ).rejects.toThrow(
          `fleet operation '${OPERATION_ID}' does not match its finalize counts`,
        );
        return lease.finalizeOperation({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record, 'finalized'),
          expectedRowCounts: { item: 1 },
        });
      },
    );
    expect(migration.state).toBe('finalized');
  });

  it('finalize requireAllItemsComplete SQL check', async () => {
    const incompleteDb = new MemoryD1();
    const incomplete = store(incompleteDb);
    await expect(
      incomplete.withAccountOperationLease('migration', async (lease) => {
        const created = await start(lease, 'migration');
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [itemRow('pending')],
        });
        return lease.finalizeOperation({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record, 'finalized'),
          expectedRowCounts: { item: 1 },
          requireAllItemsComplete: true,
        });
      }),
    ).rejects.toThrow(
      `fleet operation '${OPERATION_ID}' does not match its finalize counts`,
    );
    expect((await incomplete.readOperationById(OPERATION_ID))?.state).toBe(
      'running',
    );

    const db = new MemoryD1();
    const result = await store(db).withAccountOperationLease(
      'migration',
      async (lease) => {
        const created = await start(lease, 'migration');
        await lease.stageRows({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          rows: [itemRow('complete')],
        });
        return lease.finalizeOperation({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record, 'finalized'),
          expectedRowCounts: { item: 1 },
          requireAllItemsComplete: true,
        });
      },
    );
    expect(result.state).toBe('finalized');
  });

  it('finalize mismatch leaves running', async () => {
    const db = new MemoryD1();
    const target = store(db);
    const error = await target.withAccountOperationLease(
      'audit',
      async (lease) => {
        const created = await start(lease);
        return rejection(
          lease.finalizeOperation({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: advanced(created.record, 'finalized'),
            expectedRowCounts: { finding: 1 },
          }),
        );
      },
    );
    expect(error.message).toBe(
      `fleet operation '${OPERATION_ID}' does not match its finalize counts`,
    );
    expect((await target.readOperationById(OPERATION_ID))?.state).toBe(
      'running',
    );

    const staleLeaseDb = new MemoryD1();
    const staleLeaseTarget = store(staleLeaseDb);
    await expect(
      staleLeaseTarget.withAccountOperationLease('audit', async (lease) => {
        const created = await start(lease);
        await staleLeaseDb.execute(
          'DELETE FROM anchorage_fleet_operation_leases',
        );
        return lease.finalizeOperation({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record, 'finalized'),
          expectedRowCounts: {},
        });
      }),
    ).rejects.toThrow();
    expect(
      (await staleLeaseTarget.readOperationById(OPERATION_ID))?.state,
    ).toBe('running');
  });

  it('finalize probe/readback + the head-only repair', async () => {
    const db = new MemoryD1();
    const target = store(db);
    let input:
      | Parameters<FleetOperationLease['finalizeOperation']>[0]
      | undefined;
    await target.withAccountOperationLease('audit', async (lease) => {
      const created = await start(lease);
      input = {
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: advanced(created.record, 'finalized'),
        expectedRowCounts: {},
      };
      db.hideBatchResults = true;
      await lease.finalizeOperation(input);
    });
    db.sqlite
      .prepare(
        `UPDATE anchorage_fleet_operation_heads
          SET active_operation_id = ? WHERE operation_kind = 'audit'`,
      )
      .all(OPERATION_ID);
    await target.withAccountOperationLease('audit', (lease) =>
      lease.finalizeOperation(
        input as Parameters<FleetOperationLease['finalizeOperation']>[0],
      ),
    );
    expect(
      db.sqlite
        .prepare(
          `SELECT active_operation_id FROM anchorage_fleet_operation_heads
            WHERE operation_kind = 'audit'`,
        )
        .all()[0]?.active_operation_id,
    ).toBeNull();
  });

  it('failOperation commits run-failed + item update + head release + terminalAtMs in ONE batch', async () => {
    const db = new MemoryD1();
    const target = store(db);
    await target.withAccountOperationLease('migration', async (lease) => {
      const created = await start(lease, 'migration');
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [itemRow('pending')],
      });
      const mark = db.batchSizes.length;
      await lease.failOperation({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: advanced(created.record, 'failed'),
        updateRows: [itemRow('failed')],
      });
      expect(db.batchSizes.slice(mark)).toEqual([3]);
    });
    const persisted = await target.readOperationById(OPERATION_ID);
    expect(persisted).toMatchObject({
      state: 'failed',
      terminalAtMs: expect.any(Number),
    });
    expect(
      (
        await target.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'item',
          limit: 10,
        })
      ).rows[0]?.payload.status,
    ).toBe('failed');
    expect(
      db.sqlite
        .prepare(
          `SELECT active_operation_id FROM anchorage_fleet_operation_heads
            WHERE operation_kind = 'migration'`,
        )
        .all()[0]?.active_operation_id,
    ).toBeNull();

    const staleLeaseDb = new MemoryD1();
    const staleLeaseTarget = store(staleLeaseDb);
    await expect(
      staleLeaseTarget.withAccountOperationLease('migration', async (lease) => {
        const created = await start(lease, 'migration');
        await staleLeaseDb.execute(
          'DELETE FROM anchorage_fleet_operation_leases',
        );
        return lease.failOperation({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: advanced(created.record, 'failed'),
        });
      }),
    ).rejects.toThrow();
    expect(
      (await staleLeaseTarget.readOperationById(OPERATION_ID))?.state,
    ).toBe('running');
  });

  it('failOperation refuses more than one updateRow with its fixed message and leaves the operation running', async () => {
    const db = new MemoryD1();
    const target = store(db);
    const error = await target.withAccountOperationLease(
      'migration',
      async (lease) => {
        const created = await start(lease, 'migration');
        const mark = db.batchSizes.length;
        const rejected = await rejection(
          lease.failOperation({
            operationId: OPERATION_ID,
            expectedRevision: 0,
            runRecord: advanced(created.record, 'failed'),
            updateRows: [itemRow('failed', 0), itemRow('failed', 1)],
          }),
        );
        expect(db.batchSizes.length).toBe(mark);
        return rejected;
      },
    );
    expect(error.message).toBe('failOperation accepts at most one updateRow');
    expect((await target.readOperationById(OPERATION_ID))?.state).toBe(
      'running',
    );
    expect(
      (await target.readOperationById(OPERATION_ID))?.progress.revision,
    ).toBe(0);
  });

  it('terminal transitions advance the revision (stale-token discriminator)', async () => {
    const db = new MemoryD1();
    const target = store(db);
    const terminal = await seedTerminal(
      target,
      'audit',
      OPERATION_ID,
      'finalized',
    );
    expect(terminal.progress.revision).toBe(1);
    expect(
      classifyFleetOperationToken(
        { version: 1, operationId: OPERATION_ID, revision: 0 },
        terminal,
        'audit',
      ),
    ).toBe('stale');
    const failed = await seedTerminal(
      target,
      'migration',
      SECOND_OPERATION_ID,
      'failed',
    );
    expect(failed.progress.revision).toBe(1);
    await expect(
      target.withAccountOperationLease('audit', (lease) =>
        lease.commitProgress({
          operationId: OPERATION_ID,
          expectedRevision: 1,
          runRecord: advanced(terminal),
        }),
      ),
    ).rejects.toThrow(
      `fleet operation '${OPERATION_ID}' is no longer at the expected revision`,
    );
  });

  it('rows stay readable on terminal states', async () => {
    const db = new MemoryD1();
    const target = store(db);
    await target.withAccountOperationLease('audit', async (lease) => {
      const created = await start(lease);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [findingRow()],
      });
      await lease.finalizeOperation({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: advanced(created.record, 'finalized'),
        expectedRowCounts: { finding: 1 },
      });
    });
    await expect(
      target.readOperationRowsPage({
        operationId: OPERATION_ID,
        rowKind: 'finding',
        limit: 10,
      }),
    ).resolves.toMatchObject({ rows: [findingRow()], done: true });

    const failedDb = new MemoryD1();
    const failedTarget = store(failedDb);
    await failedTarget.withAccountOperationLease('migration', async (lease) => {
      const created = await start(lease, 'migration');
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [itemRow('pending')],
      });
      await lease.failOperation({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        runRecord: advanced(created.record, 'failed'),
        updateRows: [itemRow('failed')],
      });
    });
    await expect(
      failedTarget.readOperationRowsPage({
        operationId: OPERATION_ID,
        rowKind: 'item',
        limit: 10,
      }),
    ).resolves.toMatchObject({ rows: [itemRow('failed')], done: true });
  });

  it('readOperationRowsPage limit validation + ordinal order + done + payload parse fail-closed', async () => {
    const db = new MemoryD1();
    const target = store(db);
    await target.withAccountOperationLease('audit', async (lease) => {
      await start(lease);
      await lease.stageRows({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        rows: [findingRow(2), findingRow(0), findingRow(1)],
      });
    });
    await expect(
      target.readOperationRowsPage({
        operationId: OPERATION_ID,
        rowKind: 'finding',
        limit: 0,
      }),
    ).rejects.toThrow('limit must be an integer from 1 to 1000');
    for (const limit of [1001, 1.5]) {
      await expect(
        target.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'finding',
          limit,
        }),
      ).rejects.toThrow('limit must be an integer from 1 to 1000');
    }
    await expect(
      target.readOperationRowsPage({
        operationId: OPERATION_ID,
        rowKind: 'cursor' as FleetOperationRowKind,
        limit: 1,
      }),
    ).rejects.toThrow('rowKind must be one of record, finding, item, fact');
    for (const afterOrdinal of [-1, 0.5, Number.MAX_SAFE_INTEGER]) {
      await expect(
        target.readOperationRowsPage({
          operationId: OPERATION_ID,
          rowKind: 'finding',
          afterOrdinal,
          limit: 1,
        }),
      ).rejects.toThrow(
        'afterOrdinal must be a non-negative safe integer below Number.MAX_SAFE_INTEGER',
      );
    }
    const first = await target.readOperationRowsPage({
      operationId: OPERATION_ID,
      rowKind: 'finding',
      limit: 2,
    });
    expect(first.rows.map((row) => row.ordinal)).toEqual([0, 1]);
    expect(first.done).toBe(false);
    expect(
      await target.readOperationRowsPage({
        operationId: OPERATION_ID,
        rowKind: 'finding',
        afterOrdinal: 1,
        limit: 2,
      }),
    ).toMatchObject({ rows: [{ ordinal: 2 }], done: true });
    db.sqlite
      .prepare(
        `UPDATE anchorage_fleet_operation_rows SET payload = '{'
          WHERE row_kind = 'finding' AND ordinal = 2`,
      )
      .all();
    await expect(
      target.readOperationRowsPage({
        operationId: OPERATION_ID,
        rowKind: 'finding',
        afterOrdinal: 1,
        limit: 2,
      }),
    ).rejects.toThrow('fleet operation state is malformed');
  });

  it('prune protects the active and the latest finalized operation per kind', async () => {
    const db = new MemoryD1();
    const inventory = new FakeInventoryStore();
    const target = store(db, 'account-primary', inventory);
    await seedTerminal(target, 'audit', OPERATION_ID, 'failed');
    await seedTerminal(target, 'audit', SECOND_OPERATION_ID, 'finalized');
    await seedTerminal(target, 'migration', THIRD_OPERATION_ID, 'failed');
    await seedTerminal(target, 'migration', FOURTH_OPERATION_ID, 'finalized');
    await target.withAccountOperationLease('audit', (lease) =>
      start(lease, 'audit', FIFTH_OPERATION_ID),
    );
    await target.withAccountOperationLease('migration', (lease) =>
      start(lease, 'migration', SIXTH_OPERATION_ID),
    );
    expect(
      await target.pruneFleetOperations({ kind: 'audit', limit: 10 }),
    ).toEqual({
      deleted: 1,
      releasedPins: 1,
    });
    expect(await target.readOperationById(OPERATION_ID)).toBeUndefined();
    expect(await target.readOperationById(SECOND_OPERATION_ID)).toBeDefined();
    expect(await target.readOperationById(THIRD_OPERATION_ID)).toBeDefined();
    expect(await target.readOperationById(FOURTH_OPERATION_ID)).toBeDefined();
    expect(await target.readOperationById(FIFTH_OPERATION_ID)).toBeDefined();
    expect(await target.readOperationById(SIXTH_OPERATION_ID)).toBeDefined();
    expect(
      await target.pruneFleetOperations({ kind: 'migration', limit: 10 }),
    ).toEqual({ deleted: 1, releasedPins: 0 });
    expect(await target.readOperationById(THIRD_OPERATION_ID)).toBeUndefined();
    expect(await target.readOperationById(FOURTH_OPERATION_ID)).toBeDefined();
    expect(await target.readOperationById(SECOND_OPERATION_ID)).toBeDefined();
    expect(await target.readOperationById(FIFTH_OPERATION_ID)).toBeDefined();
  });

  it('prune deletes oldest-first in bounded batches', async () => {
    const db = new MemoryD1();
    const target = store(db);
    for (const [index, operationId] of [
      OPERATION_ID,
      SECOND_OPERATION_ID,
      THIRD_OPERATION_ID,
    ].entries()) {
      await seedTerminal(target, 'migration', operationId, 'failed');
      db.sqlite
        .prepare(
          `UPDATE anchorage_fleet_operations SET terminal_at_ms = ?
            WHERE operation_id = ?`,
        )
        .all(index + 1, operationId);
    }
    expect(
      await target.pruneFleetOperations({ kind: 'migration', limit: 2 }),
    ).toEqual({
      deleted: 2,
      releasedPins: 0,
    });
    expect(await target.readOperationById(OPERATION_ID)).toBeUndefined();
    expect(await target.readOperationById(SECOND_OPERATION_ID)).toBeUndefined();
    expect(await target.readOperationById(THIRD_OPERATION_ID)).toBeDefined();
  });

  it('prune releases the audit pin FIRST; the crash window leaves an unpinned terminal operation the next call deletes', async () => {
    const db = new MemoryD1();
    const inventory = new FakeInventoryStore();
    const target = store(db, 'account-primary', inventory);
    await seedTerminal(target, 'audit', FOURTH_OPERATION_ID, 'failed');
    await inventory.pinGeneration({
      generation: 1,
      pinnedBy: `fleet-audit:${FOURTH_OPERATION_ID}`,
    });
    inventory.crashAfterRelease = true;
    await expect(
      target.pruneFleetOperations({ kind: 'audit', limit: 1 }),
    ).rejects.toThrow('crash after pin release');
    expect(inventory.pins.size).toBe(0);
    expect(await target.readOperationById(FOURTH_OPERATION_ID)).toBeDefined();
    expect(
      await target.pruneFleetOperations({ kind: 'audit', limit: 1 }),
    ).toEqual({
      deleted: 1,
      releasedPins: 1,
    });
    expect(await target.readOperationById(FOURTH_OPERATION_ID)).toBeUndefined();
    expect(inventory.releases).toEqual([
      `1:fleet-audit:${FOURTH_OPERATION_ID}`,
      `1:fleet-audit:${FOURTH_OPERATION_ID}`,
    ]);
  });

  it('audit prune without inventoryStore throws FleetOperationStoreCapabilityError', async () => {
    await expect(
      store(new MemoryD1()).pruneFleetOperations({ kind: 'audit', limit: 1 }),
    ).rejects.toThrow(FleetOperationStoreCapabilityError);
  });

  it('finalize and fail refuse a cross-kind operation id at the terminal probe', async () => {
    const db = new MemoryD1();
    const target = store(db);
    const otherKind = `fleet operation '${OPERATION_ID}' belongs to the other operation kind`;
    const secondOtherKind = `fleet operation '${SECOND_OPERATION_ID}' belongs to the other operation kind`;
    await seedTerminal(target, 'migration', OPERATION_ID, 'finalized');
    await seedTerminal(target, 'migration', SECOND_OPERATION_ID, 'failed');

    await target.withAccountOperationLease('audit', async (lease) => {
      // Without the kind check the finalize half returns the migration record
      // as an audit success: its state is 'finalized' and its revision matches.
      await expect(
        lease.finalizeOperation({
          operationId: OPERATION_ID,
          expectedRevision: 0,
          runRecord: runRecord('audit', 1, 'finalized'),
          expectedRowCounts: {},
        }),
      ).rejects.toThrow(otherKind);
      // Without it the fail half returns normally: state 'failed', revisions
      // equal, so the audit caller believes it failed its own operation.
      await expect(
        lease.failOperation({
          operationId: SECOND_OPERATION_ID,
          expectedRevision: 0,
          runRecord: runRecord('audit', 1, 'failed', SECOND_OPERATION_ID),
        }),
      ).rejects.toThrow(secondOtherKind);
    });

    for (const [operationId, state] of [
      [OPERATION_ID, 'finalized'],
      [SECOND_OPERATION_ID, 'failed'],
    ] as const) {
      expect(await target.readOperationById(operationId)).toMatchObject({
        kind: 'migration',
        state,
        progress: { revision: 1 },
      });
    }
    const started = await target.withAccountOperationLease(
      'audit',
      async (lease) => start(lease, 'audit', THIRD_OPERATION_ID),
    );
    expect(started.outcome).toBe('created');
  });

  it('withAccountOperationLease and pruneFleetOperations refuse an unknown kind with its fixed message', async () => {
    const db = new MemoryD1();
    const target = store(db);
    const message = 'kind must be one of audit, migration';
    await expect(
      target.withAccountOperationLease(
        'bogus' as FleetOperationKind,
        async () => {
          throw new Error('callback must not run');
        },
      ),
    ).rejects.toThrow(message);
    await expect(
      target.pruneFleetOperations({
        kind: 'bogus' as FleetOperationKind,
        limit: 1,
      }),
    ).rejects.toThrow(message);
    // The kind check precedes schema initialization, so the refusal did no I/O
    // at all: not one table exists.
    expect(
      db.sqlite
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .all(),
    ).toEqual([]);
  });
});
