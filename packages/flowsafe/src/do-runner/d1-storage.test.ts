// SPDX-License-Identifier: Apache-2.0
// Fast SQL-unit coverage over node:sqlite: json_extract status filters and
// ISO-cutoff comparisons execute in SQLite, while the Wrangler harness owns
// D1 concurrency, transaction, and runtime fidelity.

import { describe, expect, it } from 'vitest';

import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../../test-support/sqlite.js';
import {
  D1ResourceOwnershipStore,
  RESOURCE_OWNERSHIP_TABLE,
  type ResourceOwnershipDatabase,
} from '../approval-api/resource-ownership.js';
import {
  createBackgroundTaskD1Domains,
  DurableObjectBackgroundTasksStorageD1,
  DurableObjectWorkflowsStorageD1,
} from '../background-tasks/d1-storage.js';
import {
  D1SchedulesStorage,
  type ScheduleDatabase,
} from '../schedules/schedules-d1.js';
import { createScheduleStorageDomains } from '../schedules/storage.js';
import { scheduleWithCreatorRole } from '../schedules/target-policy.js';
import type { SignalDatabase } from '../signals/d1-shared.js';
import { D1NotificationsStorage } from '../signals/notifications-d1.js';
import { createSignalStorageDomains } from '../signals/storage.js';
import { D1ThreadStateStorage } from '../signals/thread-state-d1.js';
import type { D1DatabaseBinding } from './cf-types.js';
import {
  createD1Storage,
  purgeExpiredBackgroundTasks,
  purgeExpiredNotifications,
  purgeExpiredScheduleTriggers,
  purgeExpiredThreadState,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
  type SnapshotDatabase,
  type SnapshotStatement,
} from './d1-storage.js';

// Domain-local result-envelope adapter for pure purge SQL units. It maps
// node:sqlite's affected-row count to the structural SnapshotDatabase seam;
// the Wrangler harness owns D1 runtime and concurrency fidelity.
function d1Like(db: SqliteDatabase): SnapshotDatabase {
  function statement(sql: string, params: unknown[]): SnapshotStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      all: async <T>() => ({
        results: db.prepare(sql).all(...params) as T[],
      }),
      run: async () => {
        const outcome = db.prepare(sql).run(...params) as {
          changes?: number | bigint;
        };
        return { meta: { changes: Number(outcome.changes ?? 0) } };
      },
    };
  }
  return { prepare: (sql: string) => statement(sql, []) };
}

function lifecycleStores(db: SqliteDatabase): {
  snapshots: SnapshotDatabase;
  resources: D1ResourceOwnershipStore;
} {
  const binding = sqliteUnitDatabase(db);
  return {
    snapshots: binding as SnapshotDatabase,
    resources: new D1ResourceOwnershipStore(
      binding as ResourceOwnershipDatabase,
    ),
  };
}

const NOW = Date.parse('2026-07-07T12:00:00.000Z');
const DAY_MS = 86_400_000;

// Column set per @mastra/core storage constants for mastra_workflow_snapshot
// (camelCase timestamps, snapshot serialized as JSON TEXT).
function createSnapshotTable(db: SqliteDatabase, prefix = ''): void {
  db.prepare(
    `CREATE TABLE ${prefix}mastra_workflow_snapshot (
      workflow_name TEXT NOT NULL,
      run_id TEXT NOT NULL,
      resourceId TEXT,
      snapshot TEXT NOT NULL,
      createdAt TEXT NOT NULL,
      updatedAt TEXT NOT NULL
    )`,
  ).run();
}

function seedRun(
  db: SqliteDatabase,
  options: {
    runId: string;
    status: string;
    updatedAt: number;
    prefix?: string;
  },
): void {
  const iso = new Date(options.updatedAt).toISOString();
  db.prepare(
    `INSERT INTO ${options.prefix ?? ''}mastra_workflow_snapshot
     (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
     VALUES (?, ?, NULL, ?, ?, ?)`,
  ).run(
    'wf',
    options.runId,
    JSON.stringify({ status: options.status, runId: options.runId }),
    iso,
    iso,
  );
}

function remainingRunIds(db: SqliteDatabase, prefix = ''): string[] {
  const rows = (
    db.prepare(
      `SELECT run_id FROM ${prefix}mastra_workflow_snapshot ORDER BY run_id`,
    ) as unknown as { all(): Array<{ run_id: string }> }
  ).all();
  return rows.map((row) => row.run_id);
}

const TERMINAL = [
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
];
const LIVE = ['running', 'suspended', 'waiting', 'pending', 'paused'];
const MAX_TABLE_PREFIX = 'p'.repeat(39);
const OVERLONG_TABLE_PREFIX = 'p'.repeat(40);

interface PublicStoragePrefixCase {
  name: string;
  construct: (binding: D1DatabaseBinding, tablePrefix: string) => unknown;
}

const PUBLIC_STORAGE_PREFIX_CASES = [
  {
    name: 'D1NotificationsStorage',
    construct: (binding, tablePrefix) =>
      new D1NotificationsStorage(
        binding as unknown as SignalDatabase,
        tablePrefix,
      ),
  },
  {
    name: 'D1ThreadStateStorage',
    construct: (binding, tablePrefix) =>
      new D1ThreadStateStorage(
        binding as unknown as SignalDatabase,
        tablePrefix,
      ),
  },
  {
    name: 'createSignalStorageDomains',
    construct: (binding, tablePrefix) =>
      createSignalStorageDomains(binding, tablePrefix),
  },
  {
    name: 'D1SchedulesStorage',
    construct: (binding, tablePrefix) =>
      new D1SchedulesStorage(
        binding as unknown as ScheduleDatabase,
        tablePrefix,
      ),
  },
  {
    name: 'createScheduleStorageDomains',
    construct: (binding, tablePrefix) =>
      createScheduleStorageDomains(binding, tablePrefix),
  },
  {
    name: 'createBackgroundTaskD1Domains',
    construct: (binding, tablePrefix) =>
      createBackgroundTaskD1Domains({ binding, tablePrefix }),
  },
  {
    name: 'DurableObjectWorkflowsStorageD1',
    construct: (binding, tablePrefix) =>
      new DurableObjectWorkflowsStorageD1({
        binding: binding as never,
        tablePrefix,
      }),
  },
  {
    name: 'DurableObjectBackgroundTasksStorageD1',
    construct: (binding, tablePrefix) =>
      new DurableObjectBackgroundTasksStorageD1(
        { binding: binding as never, tablePrefix },
        new DurableObjectWorkflowsStorageD1({ binding: binding as never }),
      ),
  },
] satisfies PublicStoragePrefixCase[];

interface PublicPurgeCase {
  name: string;
  run: (
    db: SnapshotDatabase,
    tablePrefix: string,
    now: () => number,
  ) => Promise<unknown>;
}

const PUBLIC_PURGE_CASES = [
  {
    name: 'purgeExpiredWorkflowRuns',
    run: (db, tablePrefix, now) =>
      purgeExpiredWorkflowRuns(db, { ttlMs: DAY_MS, tablePrefix, now }),
  },
  {
    name: 'purgeExpiredThreads',
    run: (db, tablePrefix, now) =>
      purgeExpiredThreads(db, { ttlMs: DAY_MS, tablePrefix, now }),
  },
  {
    name: 'purgeExpiredBackgroundTasks',
    run: (db, tablePrefix, now) =>
      purgeExpiredBackgroundTasks(db, { tablePrefix, now }),
  },
  {
    name: 'purgeExpiredNotifications',
    run: (db, tablePrefix, now) =>
      purgeExpiredNotifications(db, { ttlMs: DAY_MS, tablePrefix, now }),
  },
  {
    name: 'purgeExpiredThreadState',
    run: (db, tablePrefix, now) =>
      purgeExpiredThreadState(db, { ttlMs: DAY_MS, tablePrefix, now }),
  },
  {
    name: 'purgeExpiredScheduleTriggers',
    run: (db, tablePrefix, now) =>
      purgeExpiredScheduleTriggers(db, { ttlMs: DAY_MS, tablePrefix, now }),
  },
] satisfies PublicPurgeCase[];

describe('createD1Storage table prefix', () => {
  it('uses the shared Mastra-compatible identifier rule', () => {
    const binding = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;

    expect(() =>
      createD1Storage({ binding, tablePrefix: 'tenant-prod_' }),
    ).toThrow(
      'Invalid tablePrefix: use an empty prefix or start with a letter or underscore and continue with letters, numbers, or underscores.',
    );
    expect(() =>
      createD1Storage({ binding, tablePrefix: '01_tenant_' }),
    ).toThrow(/start with a letter or underscore/);
    expect(() =>
      createD1Storage({ binding, tablePrefix: 'tenant_01_' }),
    ).not.toThrow();
    expect(() =>
      createD1Storage({ binding, tablePrefix: '_tenant_01_' }),
    ).not.toThrow();
    expect(() => createD1Storage({ binding, tablePrefix: '' })).not.toThrow();
  });

  it('initializes the real adapter at the maximum compatible length', async () => {
    const binding = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;
    const storage = createD1Storage({
      binding,
      tablePrefix: MAX_TABLE_PREFIX,
    });

    await expect(storage.init()).resolves.toBeUndefined();
  });

  it('rejects prefixes that make a Mastra table name exceed 63 characters', () => {
    const binding = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;

    expect(() =>
      createD1Storage({ binding, tablePrefix: OVERLONG_TABLE_PREFIX }),
    ).toThrow(
      'Invalid tablePrefix: must be at most 39 characters so prefixed Mastra table names stay within the 63-character identifier limit.',
    );
  });
});

describe.each(PUBLIC_STORAGE_PREFIX_CASES)('$name table prefix', ({
  construct,
}) => {
  it('enforces the shared identifier contract at construction', () => {
    const binding = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;

    expect(() => construct(binding, 'tenant-prod_')).toThrow(
      /Invalid tablePrefix: use an empty prefix or start with a letter or underscore/,
    );
    expect(() => construct(binding, OVERLONG_TABLE_PREFIX)).toThrow(
      /Invalid tablePrefix: must be at most 39 characters/,
    );
    expect(() => construct(binding, MAX_TABLE_PREFIX)).not.toThrow();
  });
});

describe('public purge table-prefix validation', () => {
  it.each(
    PUBLIC_PURGE_CASES,
  )('$name rejects malformed prefixes before clock or D1 access', async ({
    run,
  }) => {
    let prepareCalls = 0;
    let nowCalls = 0;
    const db: SnapshotDatabase = {
      prepare: () => {
        prepareCalls += 1;
        throw new Error('prepare must not run');
      },
    };

    await expect(
      run(db, 'tenant-prod_', () => {
        nowCalls += 1;
        return NOW;
      }),
    ).rejects.toThrow(
      'Invalid tablePrefix: use an empty prefix or start with a letter or underscore and continue with letters, numbers, or underscores.',
    );
    expect(nowCalls).toBe(0);
    expect(prepareCalls).toBe(0);
  });

  it.each(
    PUBLIC_PURGE_CASES,
  )('$name rejects overlong prefixes before clock or D1 access', async ({
    run,
  }) => {
    let prepareCalls = 0;
    let nowCalls = 0;
    const db: SnapshotDatabase = {
      prepare: () => {
        prepareCalls += 1;
        throw new Error('prepare must not run');
      },
    };

    await expect(
      run(db, OVERLONG_TABLE_PREFIX, () => {
        nowCalls += 1;
        return NOW;
      }),
    ).rejects.toThrow(
      'Invalid tablePrefix: must be at most 39 characters so prefixed Mastra table names stay within the 63-character identifier limit.',
    );
    expect(nowCalls).toBe(0);
    expect(prepareCalls).toBe(0);
  });
});

describe('purgeExpiredWorkflowRuns', () => {
  it('deletes only stale TERMINAL runs and returns the count', async () => {
    // #given — every terminal status seeded fresh AND stale, every live
    // status seeded stale
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (const status of TERMINAL) {
      seedRun(sqlite, {
        runId: `stale-${status}`,
        status,
        updatedAt: NOW - 8 * DAY_MS,
      });
      seedRun(sqlite, {
        runId: `fresh-${status}`,
        status,
        updatedAt: NOW - 1 * DAY_MS,
      });
    }
    for (const status of LIVE) {
      seedRun(sqlite, {
        runId: `stale-${status}`,
        status,
        updatedAt: NOW - 30 * DAY_MS,
      });
    }

    // #when — 7-day TTL
    const deleted = await purgeExpiredWorkflowRuns(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
    });

    // #then — exactly the six stale terminal rows are gone; fresh terminal
    // rows and ALL live rows (however old — a stale suspended run is a
    // pending approval, not garbage) survive
    expect(deleted).toBe(TERMINAL.length);
    expect(remainingRunIds(sqlite)).toEqual(
      [
        ...TERMINAL.map((status) => `fresh-${status}`),
        ...LIVE.map((status) => `stale-${status}`),
      ].sort(),
    );
  });

  it('returns 0 when nothing qualifies', async () => {
    // #given
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'r1',
      status: 'success',
      updatedAt: NOW - 1 * DAY_MS,
    });

    // #when / #then
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
  });

  it('releases only the owners of snapshot rows the row-only purge deletes', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    const { snapshots, resources } = lifecycleStores(sqlite);
    seedRun(sqlite, {
      runId: 'stale-run',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'fresh-run',
      status: 'success',
      updatedAt: NOW - DAY_MS,
    });
    await resources.claim('run', 'stale-run', {
      kind: 'human',
      id: 'owner-1',
    });
    await resources.claim('run', 'fresh-run', {
      kind: 'human',
      id: 'owner-1',
    });

    expect(
      await purgeExpiredWorkflowRuns(snapshots, {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).toBe(1);
    expect(await resources.owner('run', 'stale-run')).toBeUndefined();
    expect(await resources.owner('run', 'fresh-run')).toEqual({
      kind: 'human',
      id: 'owner-1',
    });
  });

  it('rolls back run and owner deletion together when snapshot deletion fails', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    const { snapshots, resources } = lifecycleStores(sqlite);
    seedRun(sqlite, {
      runId: 'stale-run',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    await resources.claim('run', 'stale-run', {
      kind: 'human',
      id: 'owner-1',
    });
    sqlite.exec(`CREATE TRIGGER reject_snapshot_delete
      BEFORE DELETE ON mastra_workflow_snapshot
      BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END`);

    await expect(
      purgeExpiredWorkflowRuns(snapshots, {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).rejects.toThrow(/injected delete failure/);
    expect(remainingRunIds(sqlite)).toEqual(['stale-run']);
    expect(await resources.owner('run', 'stale-run')).toEqual({
      kind: 'human',
      id: 'owner-1',
    });
  });

  it('keeps a run owner when the row becomes ineligible between selection and the row-only batch', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    const binding = sqliteUnitDatabase(sqlite) as SnapshotDatabase &
      ResourceOwnershipDatabase;
    const resources = new D1ResourceOwnershipStore(binding);
    seedRun(sqlite, {
      runId: 'revived-run',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    await resources.claim('run', 'revived-run', {
      kind: 'human',
      id: 'owner-1',
    });
    const backingBatch = binding.batch?.bind(binding);
    if (!backingBatch) throw new Error('test D1 adapter must provide batch');
    const racing: SnapshotDatabase = {
      prepare: binding.prepare.bind(binding),
      batch: async (statements) => {
        sqlite
          .prepare(
            `UPDATE mastra_workflow_snapshot SET updatedAt = ? WHERE run_id = ?`,
          )
          .run(new Date(NOW).toISOString(), 'revived-run');
        return backingBatch(statements);
      },
    };

    expect(
      await purgeExpiredWorkflowRuns(racing, {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).toBe(0);
    expect(remainingRunIds(sqlite)).toEqual(['revived-run']);
    expect(await resources.owner('run', 'revived-run')).toEqual({
      kind: 'human',
      id: 'owner-1',
    });
  });

  it('respects the table prefix', async () => {
    // #given — two tables in one database, only the prefixed one targeted
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createSnapshotTable(sqlite, 'flowsafe_');
    seedRun(sqlite, {
      runId: 'unprefixed',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'prefixed',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
      prefix: 'flowsafe_',
    });

    // #when
    const deleted = await purgeExpiredWorkflowRuns(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      tablePrefix: 'flowsafe_',
      now: () => NOW,
    });

    // #then
    expect(deleted).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual(['unprefixed']);
    expect(remainingRunIds(sqlite, 'flowsafe_')).toEqual([]);
  });

  it('skips malformed snapshot rows instead of aborting the purge', async () => {
    // #given — a corrupt (non-JSON) snapshot beside a valid stale terminal
    // row and a valid stale live row
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'stale-ok',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'stale-live',
      status: 'suspended',
      updatedAt: NOW - 8 * DAY_MS,
    });
    const corruptIso = new Date(NOW - 9 * DAY_MS).toISOString();
    sqlite
      .prepare(
        `INSERT INTO mastra_workflow_snapshot
         (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
         VALUES ('wf', 'corrupt', NULL, 'not-json{oops', ?, ?)`,
      )
      .run(corruptIso, corruptIso);

    // #when — one corrupt row must not abort reclaiming the valid ones
    const deleted = await purgeExpiredWorkflowRuns(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
    });

    // #then — the valid stale terminal row is gone; the corrupt row (not
    // provably terminal — fail safe) and the live row survive
    expect(deleted).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual(['corrupt', 'stale-live']);
  });

  it('treats the TTL boundary exclusively: exactly-at-cutoff rows survive', async () => {
    // #given — a run whose updatedAt equals the cutoff instant
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'boundary',
      status: 'success',
      updatedAt: NOW - 7 * DAY_MS,
    });

    // #when / #then — strict < : the boundary row is not yet expired
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
  });

  it('treats a MISSING snapshot table as zero purgeable runs (Mastra creates it lazily)', async () => {
    // #given — a database where no run ever persisted, so Mastra's lazy
    // CREATE TABLE never happened
    const sqlite = openSqlite();

    // #when / #then — maintenance purge must not fail until some unrelated run
    // initializes the schema
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        artifactStore: {
          deleteRun: async () => {
            throw new Error('must not be called without a snapshot table');
          },
        },
      }),
    ).toBe(0);
  });

  it("pairs each purged run's artifact deletion with its snapshot row when artifactStore is wired", async () => {
    // #given — a stale terminal run beside a fresh terminal and a stale live
    // one; only the first is eligible
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'stale-done',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'fresh-done',
      status: 'success',
      updatedAt: NOW - 1 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'stale-open',
      status: 'suspended',
      updatedAt: NOW - 30 * DAY_MS,
    });
    const deletedArtifacts: string[] = [];
    const artifactStore = {
      deleteRun: async (workflowId: string, runId: string) => {
        deletedArtifacts.push(`${workflowId}/${runId}`);
        return 2;
      },
    };

    // #when
    const deleted = await purgeExpiredWorkflowRuns(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
      artifactStore,
    });

    // #then — exactly the purged run's artifacts went with its row; the
    // survivors keep theirs; deployment teardown deletes the bound bucket.
    expect(deleted).toBe(1);
    expect(deletedArtifacts).toEqual(['wf/stale-done']);
    expect(remainingRunIds(sqlite)).toEqual(['fresh-done', 'stale-open']);
  });

  it('keeps a run owner when the artifact path recheck leaves its snapshot row', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    const binding = sqliteUnitDatabase(sqlite) as SnapshotDatabase &
      ResourceOwnershipDatabase;
    const resources = new D1ResourceOwnershipStore(binding);
    seedRun(sqlite, {
      runId: 'revived-run',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    await resources.claim('run', 'revived-run', {
      kind: 'human',
      id: 'owner-1',
    });
    const backingBatch = binding.batch?.bind(binding);
    if (!backingBatch) throw new Error('test D1 adapter must provide batch');
    const racing: SnapshotDatabase = {
      prepare: binding.prepare.bind(binding),
      batch: async (statements) => {
        sqlite
          .prepare(
            `UPDATE mastra_workflow_snapshot SET updatedAt = ? WHERE run_id = ?`,
          )
          .run(new Date(NOW).toISOString(), 'revived-run');
        return backingBatch(statements);
      },
    };

    expect(
      await purgeExpiredWorkflowRuns(racing, {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        artifactStore: { deleteRun: async () => 1 },
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).toBe(0);
    expect(remainingRunIds(sqlite)).toEqual(['revived-run']);
    expect(await resources.owner('run', 'revived-run')).toEqual({
      kind: 'human',
      id: 'owner-1',
    });
  });

  it('LIMIT-batches the artifact-paired path; the shrinking eligible set is the cursor', async () => {
    // #given — three stale terminal runs, batch size 2 (the subrequest-
    // budget guard: an unbounded first backlog would blow the Workers
    // per-invocation cap)
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (const runId of ['stale-a', 'stale-b', 'stale-c']) {
      seedRun(sqlite, {
        runId,
        status: 'success',
        updatedAt: NOW - 8 * DAY_MS,
      });
    }
    const deletedArtifacts: string[] = [];
    const artifactStore = {
      deleteRun: async (_workflowId: string, runId: string) => {
        deletedArtifacts.push(runId);
        return 1;
      },
    };
    const options = {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
      artifactStore,
      limit: 2,
    };

    // #when — two passes
    const first = await purgeExpiredWorkflowRuns(d1Like(sqlite), options);
    const second = await purgeExpiredWorkflowRuns(d1Like(sqlite), options);

    // #then — the batches advance without a cursor row and stay paired
    expect(first).toBe(2);
    expect(second).toBe(1);
    expect(deletedArtifacts.sort()).toEqual(['stale-a', 'stale-b', 'stale-c']);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });

  it("a failing artifact delete leaves that run's snapshot row for the next sweep (artifacts-first ordering)", async () => {
    // #given — artifacts go BEFORE the row: if this order ever flips, a
    // crash between the two strands the artifacts forever (the row is their
    // only enumerable record)
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'stale-done',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    const artifactStore = {
      deleteRun: async () => {
        throw new Error('R2 unavailable');
      },
    };

    // #when / #then — the failure propagates (the purge duty logs it)...
    await expect(
      purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        artifactStore,
      }),
    ).rejects.toThrow('R2 unavailable');
    // ...and the row survives as the retry cursor
    expect(remainingRunIds(sqlite)).toEqual(['stale-done']);
  });

  it("one run's wedged artifact delete does not stall the eligible rows behind it", async () => {
    // #given — five stale terminal runs; only the middle one's deleteRun is
    // permanently broken. Without per-run isolation the loop aborts at the
    // same scan position EVERY firing and the runs behind it never purge.
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (const runId of ['r1-ok', 'r2-ok', 'r3-bad', 'r4-ok', 'r5-ok']) {
      seedRun(sqlite, {
        runId,
        status: 'success',
        updatedAt: NOW - 8 * DAY_MS,
      });
    }
    const artifactStore = {
      deleteRun: async (_workflowId: string, runId: string) => {
        if (runId === 'r3-bad') throw new Error('permanently broken');
        return 1;
      },
    };
    const options = { ttlMs: 7 * DAY_MS, now: () => NOW, artifactStore };

    // #when / #then — the pass purges the other four, then reports the
    // failure (naming the run) so the purge duty's error surface still fires
    await expect(
      purgeExpiredWorkflowRuns(d1Like(sqlite), options),
    ).rejects.toThrow('wf/r3-bad: permanently broken');
    expect(remainingRunIds(sqlite)).toEqual(['r3-bad']);

    // #then — a later pass with the store healed reaps the survivor
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ...options,
        artifactStore: { deleteRun: async () => 1 },
      }),
    ).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });
});

describe('purgeExpiredThreads (agent-memory thread TTL)', () => {
  // The mastra_threads/mastra_messages columns mastra-schema-guard.test.ts pins
  // against the real @mastra/cloudflare-d1 schema: TIMESTAMP columns hold
  // ISO-8601 TEXT, messages carry a NOT-NULL thread_id and NO updatedAt of
  // their own (why they can only be reached through their thread).
  function createThreadTables(db: SqliteDatabase, prefix = ''): void {
    db.prepare(
      `CREATE TABLE ${prefix}mastra_threads (
        id TEXT PRIMARY KEY,
        resourceId TEXT,
        updatedAt TEXT NOT NULL
      )`,
    ).run();
    db.prepare(
      `CREATE TABLE ${prefix}mastra_messages (
        id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        createdAt TEXT NOT NULL
      )`,
    ).run();
  }

  function seedThreadAt(
    db: SqliteDatabase,
    id: string,
    updatedAt: number,
    messageIds: string[] = [],
  ): void {
    db.prepare(
      `INSERT INTO mastra_threads (id, resourceId, updatedAt) VALUES (?, NULL, ?)`,
    ).run(id, new Date(updatedAt).toISOString());
    for (const messageId of messageIds) {
      // A message is as old as its thread's last write unless a test says
      // otherwise — the real invariant, since saveMessages bumps updatedAt.
      db.prepare(
        'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
      ).run(messageId, id, new Date(updatedAt).toISOString());
    }
  }

  function idsIn(db: SqliteDatabase, table: string, column: string): string[] {
    const rows = (
      db.prepare(
        `SELECT ${column} AS value FROM ${table} ORDER BY ${column}`,
      ) as unknown as { all(): Array<{ value: string }> }
    ).all();
    return rows.map((row) => row.value);
  }

  it('deletes idle threads WITH their messages and leaves active ones intact', async () => {
    // #given — one thread untouched past the TTL, one written to yesterday
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m1', 'm2']);
    seedThreadAt(sqlite, 'abc_active', NOW - 1 * DAY_MS, ['m3']);

    // #when — a 30-day TTL
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then — the idle thread and BOTH its messages are gone; the active
    // conversation is untouched
    expect(purged).toEqual({ threads: 1, messages: 2 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_active']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m3']);
  });

  it('keys on updatedAt, not createdAt: an OLD thread still being written to never expires', async () => {
    // #given — the distinction that makes a TTL safe for conversations: age is
    // not idleness. A year-old thread answered this morning must survive.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_ancient-but-live', NOW - 60_000, ['m1']);

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(purged).toEqual({ threads: 0, messages: 0 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual([
      'abc_ancient-but-live',
    ]);
  });

  it('is exact at the TTL boundary (strictly older expires)', async () => {
    // #given — one thread exactly at the cutoff, one a millisecond past it
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_at-cutoff', NOW - 30 * DAY_MS);
    seedThreadAt(sqlite, 'abc_past-cutoff', NOW - 30 * DAY_MS - 1);

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then — `<` cutoff: the boundary row survives
    expect(purged.threads).toBe(1);
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_at-cutoff']);
  });

  it('never touches mastra_resources — working memory is the OWNER’s, not the thread’s', async () => {
    // #given — a resource whose only thread ages out. The owner still exists;
    // their working memory outlives any one conversation and leaves only when
    // the deployment is decommissioned.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    sqlite.prepare('CREATE TABLE mastra_resources (id TEXT PRIMARY KEY)').run();
    sqlite
      .prepare('INSERT INTO mastra_resources (id) VALUES (?)')
      .run('abc_user-1');
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS);

    // #when
    await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(idsIn(sqlite, 'mastra_resources', 'id')).toEqual(['abc_user-1']);
  });

  it('preserves logical thread ownership after expiring its memory rows', async () => {
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'thread-idle', NOW - 40 * DAY_MS, ['message-old']);
    const { snapshots, resources } = lifecycleStores(sqlite);
    const owner = { kind: 'human', id: 'opal' } as const;
    await resources.claim('thread', 'thread-idle', owner);

    const purged = await purgeExpiredThreads(snapshots, {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    expect(purged).toEqual({ threads: 1, messages: 1 });
    expect(await resources.owner('thread', 'thread-idle')).toEqual(owner);
  });

  it('LIMIT-batches: one firing takes at most `limit`, the next resumes at the survivors', async () => {
    // #given — more idle threads than one batch
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    for (let index = 0; index < 5; index += 1) {
      seedThreadAt(sqlite, `abc_idle-${index}`, NOW - (40 + index) * DAY_MS, [
        `m${index}`,
      ]);
    }
    const db = d1Like(sqlite);

    // #when — two firings at limit 3
    const first = await purgeExpiredThreads(db, {
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });
    const second = await purgeExpiredThreads(db, {
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });

    // #then — the shrinking eligible set is the cursor across firings
    expect(first).toEqual({ threads: 3, messages: 3 });
    expect(second).toEqual({ threads: 2, messages: 2 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual([]);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual([]);
  });

  it('deletes messages BEFORE the thread, so a crash between them leaves a retry cursor — never an orphan', async () => {
    // #given — the ordering is load-bearing: mastra_messages has no updatedAt,
    // so its rows are reachable only via their thread. Thread-first would put
    // them beyond every later firing of this purge. Simulate the crash by
    // failing the thread DELETE after the message DELETE has committed.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m1']);
    const inner = d1Like(sqlite);
    const crashingDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        if (sql.includes('DELETE FROM mastra_threads')) {
          throw new Error('connection lost');
        }
        return inner.prepare(sql);
      },
    };

    // #when
    await expect(
      purgeExpiredThreads(crashingDb, { ttlMs: 30 * DAY_MS, now: () => NOW }),
    ).rejects.toThrow('connection lost');

    // #then — the thread survives as its own cursor (its messages are gone,
    // which is exactly recoverable)...
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual([]);

    // ...and the next firing completes the job rather than wedging
    const retried = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });
    expect(retried).toEqual({ threads: 1, messages: 0 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual([]);
  });

  // The purge races ordinary traffic, not just crashes: @mastra/cloudflare-d1's
  // saveMessages issues its message insert and its `UPDATE mastra_threads SET
  // updatedAt` CONCURRENTLY (Promise.all), so a message arriving mid-purge is
  // exactly the interleaving below. Both tests inject that write through the
  // .prepare wrapper used by the retention race tests.
  function resurrectOn(
    sqlite: SqliteDatabase,
    trigger: (sql: string) => boolean,
  ): SnapshotDatabase {
    const inner = d1Like(sqlite);
    let fired = false;
    const send = (): void => {
      if (fired) return;
      fired = true;
      // A saveMessages-shaped write: the new message AND the thread's bump.
      sqlite
        .prepare(
          'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
        )
        .run('m-during-race', 'abc_idle', new Date(NOW).toISOString());
      sqlite
        .prepare('UPDATE mastra_threads SET updatedAt = ? WHERE id = ?')
        .run(new Date(NOW).toISOString(), 'abc_idle');
    };
    return {
      prepare: (sql: string) => {
        if (trigger(sql)) send();
        return inner.prepare(sql);
      },
    };
  }

  it('spares a thread resurrected between the SELECT and the message DELETE — history intact', async () => {
    // #given — an idle thread whose user sends a message just as the purge
    // picks it up. Keying the deletes on the SELECT's stale id list would reap
    // the conversation AND the message that just arrived.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m-old']);

    // #when — the write lands before either DELETE runs
    const purged = await purgeExpiredThreads(
      resurrectOn(sqlite, (sql) => sql.includes('DELETE FROM mastra_messages')),
      { ttlMs: 30 * DAY_MS, now: () => NOW },
    );

    // #then — the re-check excludes it from BOTH statements: nothing is lost
    expect(purged).toEqual({ threads: 0, messages: 0 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual([
      'm-during-race',
      'm-old',
    ]);
  });

  it('never orphans a message when the resurrection lands between the two DELETEs', async () => {
    // #given — the narrower window: the messages DELETE has already committed
    // when the write arrives. Without the thread DELETE's own re-check, the
    // thread row would go on stale membership and leave 'm-during-race'
    // pointing at a thread that no longer exists and cannot be reached through
    // the ordinary thread index.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m-old']);

    // #when — the write lands as the thread DELETE is about to run
    const purged = await purgeExpiredThreads(
      resurrectOn(sqlite, (sql) => sql.includes('DELETE FROM mastra_threads')),
      { ttlMs: 30 * DAY_MS, now: () => NOW },
    );

    // #then — the thread survives with its new message; the expiring history
    // is gone (the accepted residual), and NO row is orphaned
    expect(purged).toEqual({ threads: 0, messages: 1 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m-during-race']);
    const orphans = (
      sqlite.prepare(
        `SELECT m.id AS value FROM mastra_messages m
         LEFT JOIN mastra_threads t ON t.id = m.thread_id
         WHERE t.id IS NULL`,
      ) as unknown as { all(): Array<{ value: string }> }
    ).all();
    expect(orphans).toEqual([]);
  });

  it('never destroys a just-sent message when the writer TEARS BEFORE the message DELETE', async () => {
    // #given — the same torn write as below, one statement earlier: the insert
    // half commits while the updatedAt bump is still in flight, and the purge's
    // message DELETE runs next. That DELETE's subquery keys on the THREAD's
    // staleness, which still reads idle — so an updatedAt-only guard sweeps the
    // message the user just sent into the same statement as the genuinely old
    // ones, and the now-empty thread follows it. No orphan, no trace: both
    // simply vanish. `createdAt` is the message's OWN evidence of recency and
    // the only guard that survives a torn write.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m-old']);
    const inner = d1Like(sqlite);
    let inserted = false;
    const tearingDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        if (sql.includes('DELETE FROM mastra_messages') && !inserted) {
          inserted = true;
          sqlite
            .prepare(
              'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
            )
            .run('m-just-sent', 'abc_idle', new Date(NOW).toISOString());
        }
        return inner.prepare(sql);
      },
    };

    // #when
    const purged = await purgeExpiredThreads(tearingDb, {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then — the expiring history goes, the just-sent message stays, and the
    // thread survives because a message still points at it
    expect(purged).toEqual({ threads: 0, messages: 1 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m-just-sent']);
  });

  it('never orphans when the writer TEARS: the message lands but its updatedAt bump is still in flight', async () => {
    // #given — the writer is not atomic either. saveMessages issues its message
    // insert and its `UPDATE mastra_threads SET updatedAt` as two INDEPENDENT
    // calls under one Promise.all, so the insert can commit while the bump is
    // still in flight. Model exactly that: the message appears after the message
    // DELETE, but the thread STILL looks idle when the thread DELETE re-checks
    // updatedAt — so an updatedAt-only guard deletes the thread out from under
    // the message that just arrived. Only the NOT EXISTS catches this.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m-old']);
    const inner = d1Like(sqlite);
    let inserted = false;
    const tearingDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        if (sql.includes('DELETE FROM mastra_threads') && !inserted) {
          inserted = true;
          // The insert half commits; the updatedAt half has NOT landed.
          sqlite
            .prepare(
              'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
            )
            .run('m-torn-write', 'abc_idle', new Date(NOW).toISOString());
        }
        return inner.prepare(sql);
      },
    };

    // #when
    const purged = await purgeExpiredThreads(tearingDb, {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then — the thread survives because a message points at it, even though
    // its updatedAt still reads idle. No orphan.
    expect(purged.threads).toBe(0);
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m-torn-write']);
    const orphans = (
      sqlite.prepare(
        `SELECT m.id AS value FROM mastra_messages m
         LEFT JOIN mastra_threads t ON t.id = m.thread_id
         WHERE t.id IS NULL`,
      ) as unknown as { all(): Array<{ value: string }> }
    ).all();
    expect(orphans).toEqual([]);
  });

  it('chunks the id lists under D1’s 100-bound-parameter ceiling', async () => {
    // #given — a batch larger than one bind chunk. An unchunked IN list would
    // be a D1 error at exactly the scale a first backlog produces, so the purge
    // would work in every test and fail in production.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    for (let index = 0; index < 120; index += 1) {
      seedThreadAt(sqlite, `abc_idle-${index}`, NOW - 40 * DAY_MS, [
        `m${index}`,
      ]);
    }
    const bindCounts: number[] = [];
    const inner = d1Like(sqlite);
    const countingDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        const statement = inner.prepare(sql);
        if (!sql.includes('DELETE')) return statement;
        return {
          ...statement,
          bind: (...values: unknown[]) => {
            bindCounts.push(values.length);
            return statement.bind(...values);
          },
        };
      },
    };

    // #when — one firing over all 120
    const purged = await purgeExpiredThreads(countingDb, {
      ttlMs: 30 * DAY_MS,
      limit: 120,
      now: () => NOW,
    });

    // #then — everything reaped, and no single statement bound over 100 params
    expect(purged).toEqual({ threads: 120, messages: 120 });
    expect(bindCounts.length).toBeGreaterThan(2);
    expect(Math.max(...bindCounts)).toBeLessThanOrEqual(100);
  });

  it('reads missing tables as empty, so a memory-less deployment purges unchanged', async () => {
    // #given — a fresh DB: no host has enabled agent memory, so Mastra never
    // created the tables. The duty must no-op, not wedge maintenance purge.
    const sqlite = openSqlite();

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(purged).toEqual({ threads: 0, messages: 0 });
  });

  it('reads a missing MESSAGES table as empty but still expires the threads', async () => {
    // #given — threads without the messages table (a host whose memory domain
    // never fully initialized)
    const sqlite = openSqlite();
    sqlite
      .prepare(
        `CREATE TABLE mastra_threads (id TEXT PRIMARY KEY, updatedAt TEXT NOT NULL)`,
      )
      .run();
    sqlite
      .prepare('INSERT INTO mastra_threads (id, updatedAt) VALUES (?, ?)')
      .run('abc_idle', new Date(NOW - 40 * DAY_MS).toISOString());

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(purged).toEqual({ threads: 1, messages: 0 });
  });

  it('honors the tablePrefix', async () => {
    // #given
    const sqlite = openSqlite();
    createThreadTables(sqlite, 'p_');
    sqlite
      .prepare(
        'INSERT INTO p_mastra_threads (id, resourceId, updatedAt) VALUES (?, NULL, ?)',
      )
      .run('abc_idle', new Date(NOW - 40 * DAY_MS).toISOString());

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      tablePrefix: 'p_',
      now: () => NOW,
    });

    // #then
    expect(purged.threads).toBe(1);
  });

  it('surfaces a NON-missing-table message failure rather than orphaning the history', async () => {
    // #given — a wedged messages table. Swallowing this would delete the
    // threads whose messages survived, stranding them forever.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m1']);
    const inner = d1Like(sqlite);
    const wedgedDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        if (sql.includes('DELETE FROM mastra_messages')) {
          throw new Error('database is locked');
        }
        return inner.prepare(sql);
      },
    };

    // #when / #then — the purge duty's error surface fires and the thread survives
    await expect(
      purgeExpiredThreads(wedgedDb, { ttlMs: 30 * DAY_MS, now: () => NOW }),
    ).rejects.toThrow('database is locked');
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m1']);
  });
});

describe('purgeExpiredWorkflowRuns row-only batching', () => {
  it('LIMIT-batches the bulk path: one firing reclaims at most `limit` rows; the next resumes at the survivors', async () => {
    // #given — more expired terminal rows than one batch
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (let index = 0; index < 5; index += 1) {
      seedRun(sqlite, {
        runId: `stale-${index}`,
        status: 'success',
        updatedAt: NOW - 40 * DAY_MS,
      });
    }
    const db = d1Like(sqlite);

    // #when — two firings at limit 3
    const first = await purgeExpiredWorkflowRuns(db, {
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });
    const survivors = remainingRunIds(sqlite).length;
    const second = await purgeExpiredWorkflowRuns(db, {
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });

    // #then — the shrinking eligible set is the cursor across firings
    expect(first).toBe(3);
    expect(survivors).toBe(2);
    expect(second).toBe(2);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });
});

function createBackgroundTasksTable(db: SqliteDatabase, prefix = ''): void {
  db.prepare(
    `CREATE TABLE ${prefix}mastra_background_tasks (
      id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      status TEXT NOT NULL,
      completedAt TEXT,
      createdAt TEXT NOT NULL
    )`,
  ).run();
}

function seedTask(
  db: SqliteDatabase,
  options: {
    id: string;
    runId: string;
    status: string;
    completedAt?: number | null;
    prefix?: string;
  },
): void {
  const completed =
    options.completedAt === undefined || options.completedAt === null
      ? null
      : new Date(options.completedAt).toISOString();
  db.prepare(
    `INSERT INTO ${options.prefix ?? ''}mastra_background_tasks
     (id, run_id, status, completedAt, createdAt)
     VALUES (?, ?, ?, ?, ?)`,
  ).run(
    options.id,
    options.runId,
    options.status,
    completed,
    new Date(NOW).toISOString(),
  );
}

function taskIds(db: SqliteDatabase, prefix = ''): string[] {
  return (
    db
      .prepare(`SELECT id FROM ${prefix}mastra_background_tasks ORDER BY id`)
      .all() as Array<{ id: string }>
  ).map((row) => row.id);
}

describe('purgeExpiredBackgroundTasks', () => {
  it('deletes each terminal task internal workflow snapshot before its row', async () => {
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    createSnapshotTable(sqlite);
    seedTask(sqlite, {
      id: 'task-old',
      runId: 'abc_r1',
      status: 'completed',
      completedAt: NOW - 2 * 3_600_000,
    });
    const iso = new Date(NOW - 2 * 3_600_000).toISOString();
    sqlite
      .prepare(
        `INSERT INTO mastra_workflow_snapshot
         (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
         VALUES ('__background-task', 'task-old', NULL, '{}', ?, ?)`,
      )
      .run(iso, iso);

    await purgeExpiredBackgroundTasks(d1Like(sqlite), { now: () => NOW });

    expect(taskIds(sqlite)).toEqual([]);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });

  it('reaps completed rows past completedTtlMs and keeps recent ones', async () => {
    // #given — one completed 2h ago, one 30m ago; default completedTtlMs 1h
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    seedTask(sqlite, {
      id: 'old',
      runId: 'abc_r1',
      status: 'completed',
      completedAt: NOW - 2 * 3_600_000,
    });
    seedTask(sqlite, {
      id: 'fresh',
      runId: 'abc_r2',
      status: 'completed',
      completedAt: NOW - 30 * 60_000,
    });

    // #when
    const result = await purgeExpiredBackgroundTasks(d1Like(sqlite), {
      now: () => NOW,
    });

    // #then — only the old one goes
    expect(result).toEqual({ completed: 1, failed: 0 });
    expect(taskIds(sqlite)).toEqual(['fresh']);
  });

  it('reaps failed / cancelled / timed_out on the SLOWER failed window, not the completed one', async () => {
    // #given — a failed row 2h old: past the 1h completed window but INSIDE the
    // 24h failed window, so it must survive (a failure stays inspectable)
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    seedTask(sqlite, {
      id: 'failed-2h',
      runId: 'abc_r1',
      status: 'failed',
      completedAt: NOW - 2 * 3_600_000,
    });
    seedTask(sqlite, {
      id: 'cancelled-2d',
      runId: 'abc_r2',
      status: 'cancelled',
      completedAt: NOW - 2 * DAY_MS,
    });
    seedTask(sqlite, {
      id: 'timedout-2d',
      runId: 'abc_r3',
      status: 'timed_out',
      completedAt: NOW - 2 * DAY_MS,
    });

    // #when
    const result = await purgeExpiredBackgroundTasks(d1Like(sqlite), {
      now: () => NOW,
    });

    // #then — the two 2-day rows go; the 2-hour failure survives its slow window
    expect(result).toEqual({ completed: 0, failed: 2 });
    expect(taskIds(sqlite)).toEqual(['failed-2h']);
  });

  it('never reaps live rows (pending / running / suspended) whatever their age', async () => {
    // #given — an ancient suspended task; deleting it would strand its resume
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    for (const status of ['pending', 'running', 'suspended']) {
      seedTask(sqlite, {
        id: status,
        runId: `abc_${status}`,
        status,
        completedAt: NOW - 10 * DAY_MS,
      });
    }

    // #when
    const result = await purgeExpiredBackgroundTasks(d1Like(sqlite), {
      now: () => NOW,
    });

    // #then — nothing terminal, nothing deleted
    expect(result).toEqual({ completed: 0, failed: 0 });
    expect(taskIds(sqlite)).toEqual(['pending', 'running', 'suspended']);
  });

  it('keeps a terminal row with a NULL completedAt (cannot be proven old — fail safe)', async () => {
    // #given
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    seedTask(sqlite, {
      id: 'no-stamp',
      runId: 'abc_r1',
      status: 'completed',
      completedAt: null,
    });

    // #when
    const result = await purgeExpiredBackgroundTasks(d1Like(sqlite), {
      now: () => NOW,
    });

    // #then
    expect(result).toEqual({ completed: 0, failed: 0 });
    expect(taskIds(sqlite)).toEqual(['no-stamp']);
  });

  it('reads a missing table as zero (background tasks may never have run)', async () => {
    // #given — no table created
    const sqlite = openSqlite();

    // #when / #then
    expect(
      await purgeExpiredBackgroundTasks(d1Like(sqlite), { now: () => NOW }),
    ).toEqual({ completed: 0, failed: 0 });
  });
});

async function signalDb(sqlite: SqliteDatabase): Promise<SignalDatabase> {
  const db = sqliteUnitDatabase(sqlite) as unknown as SignalDatabase;
  await new D1NotificationsStorage(db, '').init();
  await new D1ThreadStateStorage(db, '').init();
  return db;
}

function seedNotification(
  db: SqliteDatabase,
  row: { id: string; threadId: string; status: string; updatedAt: number },
): void {
  const iso = new Date(row.updatedAt).toISOString();
  db.prepare(
    `INSERT INTO mastra_notifications
       (id, thread_id, source, kind, priority, status, summary, coalescedCount,
        createdAt, updatedAt, deliveryAttempts)
     VALUES (?, ?, 'x', 'y', 'medium', ?, 'z', 1, ?, ?, 0)`,
  ).run(row.id, row.threadId, row.status, iso, iso);
}

function seedThreadState(
  db: SqliteDatabase,
  row: { threadId: string; type: string; updatedAt: number },
): void {
  const iso = new Date(row.updatedAt).toISOString();
  db.prepare(
    `INSERT INTO mastra_thread_state (thread_id, type, value, updatedAt)
     VALUES (?, ?, '{}', ?)`,
  ).run(row.threadId, row.type, iso);
}

describe('purgeExpiredNotifications', () => {
  it('reaps TERMINAL rows past the TTL and keeps pending ones', async () => {
    // #given — a delivered row long past the TTL, a fresh delivered row, and a
    // pending row (even an ancient one) that must survive.
    const sqlite = openSqlite();
    await signalDb(sqlite);
    seedNotification(sqlite, {
      id: 'old-delivered',
      threadId: 'abc_t1',
      status: 'delivered',
      updatedAt: NOW - 2 * DAY_MS,
    });
    seedNotification(sqlite, {
      id: 'fresh-delivered',
      threadId: 'abc_t1',
      status: 'delivered',
      updatedAt: NOW,
    });
    seedNotification(sqlite, {
      id: 'ancient-pending',
      threadId: 'abc_t1',
      status: 'pending',
      updatedAt: NOW - 30 * DAY_MS,
    });

    // #when — one-day TTL
    const deleted = await purgeExpiredNotifications(d1Like(sqlite), {
      ttlMs: DAY_MS,
      now: () => NOW,
    });

    // #then — only the old TERMINAL row went; pending is never reaped by age.
    expect(deleted).toBe(1);
    const ids = (
      sqlite
        .prepare('SELECT id FROM mastra_notifications ORDER BY id')
        .all() as { id: string }[]
    ).map((r) => r.id);
    expect(ids).toEqual(['ancient-pending', 'fresh-delivered']);
  });

  it('reads a missing table as zero', async () => {
    const sqlite = openSqlite();
    expect(
      await purgeExpiredNotifications(d1Like(sqlite), { ttlMs: DAY_MS }),
    ).toBe(0);
  });
});

describe('purgeExpiredThreadState', () => {
  it('reaps rows past the updatedAt TTL and keeps fresh ones', async () => {
    // #given — an abandoned goal (old) and an active one (fresh)
    const sqlite = openSqlite();
    await signalDb(sqlite);
    seedThreadState(sqlite, {
      threadId: 'abc_t1',
      type: 'goal',
      updatedAt: NOW - 10 * DAY_MS,
    });
    seedThreadState(sqlite, {
      threadId: 'abc_t2',
      type: 'goal',
      updatedAt: NOW,
    });

    // #when
    const deleted = await purgeExpiredThreadState(d1Like(sqlite), {
      ttlMs: DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(deleted).toBe(1);
    const rows = sqlite
      .prepare('SELECT thread_id FROM mastra_thread_state')
      .all() as { thread_id: string }[];
    expect(rows.map((r) => r.thread_id)).toEqual(['abc_t2']);
  });

  it('reads a missing table as zero', async () => {
    const sqlite = openSqlite();
    expect(
      await purgeExpiredThreadState(d1Like(sqlite), { ttlMs: DAY_MS }),
    ).toBe(0);
  });
});

describe('purgeExpiredScheduleTriggers', () => {
  it('purgeExpiredScheduleTriggers reaps trigger rows past the actualFireAt TTL, keeping recent ones (numeric compare)', async () => {
    // #given — one old + one recent trigger. actualFireAt is INTEGER ms-epoch, so
    // the TTL is a NUMERIC comparison (not the ISO-text bet the other purges take).
    const sqlite = openSqlite();
    const store = new D1SchedulesStorage(
      sqliteUnitDatabase(sqlite) as unknown as ScheduleDatabase,
    );
    await store.createSchedule({
      id: 'schedule_a',
      target: { type: 'workflow', workflowId: 'wf' },
      cron: '* * * * *',
      status: 'active',
      nextFireAt: NOW,
      createdAt: NOW,
      updatedAt: NOW,
      metadata: {},
    });
    await store.recordTrigger({
      id: 'old',
      scheduleId: 'schedule_a',
      runId: 'abc_r1',
      scheduledFireAt: NOW - 10 * DAY_MS,
      actualFireAt: NOW - 10 * DAY_MS,
      outcome: 'published',
      metadata: {},
    });
    await store.recordTrigger({
      id: 'old-deferred',
      scheduleId: 'schedule_a',
      runId: 'abc_pending',
      scheduledFireAt: NOW - 10 * DAY_MS,
      actualFireAt: NOW - 10 * DAY_MS,
      outcome: 'deferred',
      metadata: { reason: 'dispatch-indeterminate' },
    });
    await store.recordTrigger({
      id: 'recent',
      scheduleId: 'schedule_a',
      runId: 'abc_r2',
      scheduledFireAt: NOW - 1000,
      actualFireAt: NOW - 1000,
      outcome: 'published',
      metadata: {},
    });

    // #when — a 7-day window at NOW
    const deleted = await purgeExpiredScheduleTriggers(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
    });

    // #then — the 10-day-old row is reaped; the recent one stays.
    expect(deleted).toBe(1);
    const ids = (
      sqlite.prepare('SELECT id FROM mastra_schedule_triggers').all() as {
        id: string;
      }[]
    ).map((r) => r.id);
    expect(ids).toEqual(['old-deferred', 'recent']);
  });

  it('retains an old deferred row until it can finalize a pending schedule deletion', async () => {
    const sqlite = openSqlite();
    const store = new D1SchedulesStorage(
      sqliteUnitDatabase(sqlite) as unknown as ScheduleDatabase,
    );
    await store.createOwnedSchedule(
      scheduleWithCreatorRole(
        {
          id: 'schedule_pending_delete',
          target: { type: 'workflow', workflowId: 'wf' },
          cron: '* * * * *',
          status: 'active',
          nextFireAt: NOW,
          createdAt: NOW,
          updatedAt: NOW,
          metadata: {},
        },
        'operator',
      ),
      { kind: 'human', id: 'operator-1' },
      100,
    );
    const deferred = {
      id: 'old-deferred',
      scheduleId: 'schedule_pending_delete',
      runId: 'abc_pending',
      scheduledFireAt: NOW - 10 * DAY_MS,
      actualFireAt: NOW - 10 * DAY_MS,
      outcome: 'deferred' as const,
      metadata: { reason: 'dispatch-indeterminate' },
    };
    await store.recordTrigger(deferred);
    await expect(
      store.deleteOwnedSchedule('schedule_pending_delete'),
    ).resolves.toBe('pending');

    await expect(
      purgeExpiredScheduleTriggers(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).resolves.toBe(0);
    expect(
      sqlite
        .prepare('SELECT COUNT(*) AS count FROM mastra_schedule_triggers')
        .get(),
    ).toEqual({ count: 1 });

    await store.recordTrigger({
      ...deferred,
      outcome: 'failed',
      error: 'target absent',
    });
    expect(
      sqlite.prepare('SELECT COUNT(*) AS count FROM mastra_schedules').get(),
    ).toEqual({ count: 0 });
    expect(
      sqlite
        .prepare(
          `SELECT COUNT(*) AS count FROM ${RESOURCE_OWNERSHIP_TABLE}
           WHERE resource_kind = 'schedule'`,
        )
        .get(),
    ).toEqual({ count: 0 });
  });

  it('purgeExpiredScheduleTriggers reads a missing trigger table as zero', async () => {
    // #given — a fresh db, no schedule tables (schedules may never have fired)
    const sqlite = openSqlite();

    // #then — no throw, zero deleted
    expect(
      await purgeExpiredScheduleTriggers(d1Like(sqlite), { ttlMs: DAY_MS }),
    ).toBe(0);
  });
});
