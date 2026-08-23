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
  createResourceOwnershipSchema,
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
  RUN_TTL_FLOWSAFE_PURGE_TABLES,
  type RunDeadlineCursor,
  type SnapshotDatabase,
  type SnapshotStatement,
  sweepExpiredRunDeadlines,
} from './d1-storage.js';
import {
  START_IDEMPOTENCY_DDL,
  START_IDEMPOTENCY_TABLE,
} from './start-idempotency.js';

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
    cleanupCompletedAt?: number;
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
    JSON.stringify({
      status: options.status,
      runId: options.runId,
      ...(options.cleanupCompletedAt === undefined
        ? {}
        : {
            requestContext: {
              'flowsafe.runLifecycle': {
                version: 1,
                revision: 2,
                terminal: {
                  status: options.status,
                  error: {
                    code:
                      options.status === 'cancelled'
                        ? 'CANCELLED'
                        : 'TIMED_OUT',
                    message: 'terminal',
                  },
                  transitionedAt: options.cleanupCompletedAt - 1,
                  replayPrincipals: [{ kind: 'system', id: 'maintenance' }],
                  cleanupCompletedAt: options.cleanupCompletedAt,
                },
              },
            },
          }),
    }),
    iso,
    iso,
  );
}

function seedDeadlineRun(
  db: SqliteDatabase,
  options: {
    runId: string;
    status: string;
    revision: number;
    deadlineAt: number;
    transitionIntent?: 'timed_out';
    transitionExpectedRevision?: number;
    economicOperations?: Array<{ id: string; settlementState: string }>;
    cleanupCompletedAt?: number;
    prefix?: string;
  },
): void {
  const iso = new Date(NOW).toISOString();
  const lifecycle = {
    version: 1,
    revision: options.revision,
    deadlineAt: options.deadlineAt,
    ...(options.economicOperations === undefined
      ? {}
      : { economicOperations: options.economicOperations }),
    ...(options.transitionIntent
      ? {
          transitionIntent: {
            status: options.transitionIntent,
            requestedAt: NOW,
            replayPrincipals: [{ kind: 'system', id: 'maintenance' }],
            expectedRevision:
              options.transitionExpectedRevision ?? options.revision,
            expectedDeadlineAt: options.deadlineAt,
          },
        }
      : {}),
    ...(options.status === 'timed_out'
      ? {
          terminal: {
            status: 'timed_out',
            error: { code: 'TIMED_OUT', message: 'run deadline expired' },
            transitionedAt: NOW,
            replayPrincipals: [{ kind: 'system', id: 'maintenance' }],
            ...(options.cleanupCompletedAt !== undefined
              ? { cleanupCompletedAt: options.cleanupCompletedAt }
              : {}),
          },
        }
      : {}),
  };
  db.prepare(
    `INSERT INTO ${options.prefix ?? ''}mastra_workflow_snapshot
     (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
     VALUES (?, ?, NULL, ?, ?, ?)`,
  ).run(
    'wf',
    options.runId,
    JSON.stringify({
      status: options.status,
      runId: options.runId,
      requestContext: { 'flowsafe.runLifecycle': lifecycle },
    }),
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
  'cancelled',
  'timed_out',
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

describe('sweepExpiredRunDeadlines', () => {
  it('bounds a pass, isolates failures, and re-drives the failed row', async () => {
    const db = openSqlite();
    createSnapshotTable(db);
    seedDeadlineRun(db, {
      runId: 'first',
      status: 'suspended',
      revision: 1,
      deadlineAt: NOW - 2,
    });
    seedDeadlineRun(db, {
      runId: 'second',
      status: 'retry_wait',
      revision: 3,
      deadlineAt: NOW - 1,
    });
    seedDeadlineRun(db, {
      runId: 'future',
      status: 'running',
      revision: 1,
      deadlineAt: NOW + 1,
    });
    const attempts: string[] = [];
    let wedge = true;

    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        limit: 2,
        now: () => NOW,
        transition: async (candidate) => {
          attempts.push(candidate.runId);
          if (candidate.runId === 'first' && wedge) throw new Error('wedged');
        },
      }),
    ).rejects.toThrow(/first: wedged/);
    expect(attempts).toEqual(['first', 'second']);

    wedge = false;
    attempts.length = 0;
    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        limit: 1,
        now: () => NOW,
        transition: async (candidate) => {
          attempts.push(candidate.runId);
        },
      }),
    ).resolves.toBe(1);
    expect(attempts).toEqual(['first']);
  });

  it('advances the persistent cursor past a permanently failing head row', async () => {
    const db = openSqlite();
    createSnapshotTable(db);
    seedDeadlineRun(db, {
      runId: 'poison',
      status: 'suspended',
      revision: 1,
      deadlineAt: NOW - 2,
    });
    seedDeadlineRun(db, {
      runId: 'eligible',
      status: 'suspended',
      revision: 1,
      deadlineAt: NOW - 1,
    });
    let cursor: RunDeadlineCursor | undefined;
    const attempts: string[] = [];
    const advanceCursor = async (next: NonNullable<typeof cursor>) => {
      cursor = next;
    };

    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        limit: 1,
        now: () => NOW,
        advanceCursor,
        transition: async (candidate) => {
          attempts.push(candidate.runId);
          throw new Error('permanent failure');
        },
      }),
    ).rejects.toThrow(/poison: permanent failure/);
    expect(cursor).toEqual({
      workflowId: 'wf',
      runId: 'poison',
      deadlineAt: NOW - 2,
    });

    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        limit: 1,
        now: () => NOW,
        cursor,
        advanceCursor,
        transition: async (candidate) => {
          attempts.push(candidate.runId);
        },
      }),
    ).resolves.toBe(1);
    expect(attempts).toEqual(['poison', 'eligible']);
  });

  it('re-enumerates timeout crash precursors and terminal cleanup only until complete', async () => {
    const db = openSqlite();
    createSnapshotTable(db, 'tenant_');
    seedDeadlineRun(db, {
      prefix: 'tenant_',
      runId: 'core-canceled',
      status: 'canceled',
      revision: 2,
      deadlineAt: NOW - 3,
      transitionIntent: 'timed_out',
      transitionExpectedRevision: 1,
    });
    seedDeadlineRun(db, {
      prefix: 'tenant_',
      runId: 'cleanup-incomplete',
      status: 'timed_out',
      revision: 2,
      deadlineAt: NOW - 2,
    });
    seedDeadlineRun(db, {
      prefix: 'tenant_',
      runId: 'cleanup-complete',
      status: 'timed_out',
      revision: 3,
      deadlineAt: NOW - 1,
      cleanupCompletedAt: NOW,
    });
    const seen: string[] = [];

    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        tablePrefix: 'tenant_',
        now: () => NOW,
        transition: async (candidate) => {
          seen.push(candidate.runId);
          if (candidate.runId === 'core-canceled') {
            expect(candidate.revision).toBe(1);
          }
        },
      }),
    ).resolves.toBe(2);
    expect(seen).toEqual(['core-canceled', 'cleanup-incomplete']);
  });

  it.each([
    'success',
    'failed',
  ] as const)('re-enumerates a timeout intent after a late core %s precursor', async (status) => {
    const db = openSqlite();
    createSnapshotTable(db, 'tenant_');
    seedDeadlineRun(db, {
      prefix: 'tenant_',
      runId: `late-${status}`,
      status,
      revision: 5,
      deadlineAt: NOW - 1,
      transitionIntent: 'timed_out',
      transitionExpectedRevision: 4,
    });
    const candidates: Array<{ runId: string; revision: number }> = [];

    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        tablePrefix: 'tenant_',
        now: () => NOW,
        transition: async (candidate) => {
          candidates.push({
            runId: candidate.runId,
            revision: candidate.revision,
          });
        },
      }),
    ).resolves.toBe(1);
    expect(candidates).toEqual([{ runId: `late-${status}`, revision: 4 }]);
  });

  it('does not let disputed rows consume the bounded pass ahead of eligible deadlines', async () => {
    const db = openSqlite();
    createSnapshotTable(db);
    for (const [runId, deadlineAt] of [
      ['disputed-first', NOW - 3],
      ['disputed-second', NOW - 2],
    ] as const) {
      seedDeadlineRun(db, {
        runId,
        status: 'suspended',
        revision: 1,
        deadlineAt,
        economicOperations: [
          { id: `charge-${runId}`, settlementState: 'disputed' },
        ],
      });
    }
    seedDeadlineRun(db, {
      runId: 'eligible',
      status: 'suspended',
      revision: 1,
      deadlineAt: NOW - 1,
    });
    const seen: string[] = [];

    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        limit: 2,
        now: () => NOW,
        transition: async (candidate) => {
          seen.push(candidate.runId);
        },
      }),
    ).resolves.toBe(1);
    expect(seen).toEqual(['eligible']);
  });

  it('enforces the bounded limit and existing 39-character prefix contract', async () => {
    const db = openSqlite();
    createSnapshotTable(db, MAX_TABLE_PREFIX);
    seedDeadlineRun(db, {
      prefix: MAX_TABLE_PREFIX,
      runId: 'bounded',
      status: 'waiting_signal',
      revision: 1,
      deadlineAt: NOW,
    });

    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        tablePrefix: MAX_TABLE_PREFIX,
        limit: 1,
        now: () => NOW,
        transition: async () => undefined,
      }),
    ).resolves.toBe(1);
    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        tablePrefix: OVERLONG_TABLE_PREFIX,
        now: () => NOW,
        transition: async () => undefined,
      }),
    ).rejects.toThrow(/at most 39 characters/);
  });
});

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
        ...(status === 'cancelled' || status === 'timed_out'
          ? { cleanupCompletedAt: NOW - 8 * DAY_MS }
          : {}),
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

  it.each([
    'cancelled',
    'timed_out',
  ] as const)('retains incomplete %s lifecycle rows and purges only cleanup-complete rows', async (status) => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: `${status}-incomplete`,
      status,
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: `${status}-complete`,
      status,
      updatedAt: NOW - 8 * DAY_MS,
      cleanupCompletedAt: NOW - 8 * DAY_MS,
    });

    await expect(
      purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).resolves.toBe(1);
    expect(remainingRunIds(sqlite)).toEqual([`${status}-incomplete`]);
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

// ---------------------------------------------------------------------------
// F3 — start-reservation retention.
//
// The reservation is what makes a spent idempotency key answerable, so its
// retention has one hard rule and one soft one:
//
//   HARD  a reservation must NEVER be deleted while the run it names is still
//         readable. Break it and the very next retry of that key mints a fresh
//         run beside the live one — the exact double-execution the key was
//         bought to prevent.
//   SOFT  a reservation must eventually be deleted, or the one table this
//         deployment cannot drain grows forever.
// ---------------------------------------------------------------------------

function createReservationTable(db: SqliteDatabase): void {
  db.prepare(START_IDEMPOTENCY_DDL).run();
}

function seedReservation(
  db: SqliteDatabase,
  options: {
    key: string;
    runId: string;
    state: 'reserved' | 'started' | 'terminal';
    updatedAt: number;
  },
): void {
  db.prepare(
    `INSERT INTO ${START_IDEMPOTENCY_TABLE}
       (key, owner_kind, owner_id, target_kind, target_id, run_id, thread_id,
        state, created_at, updated_at)
     VALUES (?, 'human', 'operator-1', 'workflow', 'wf', ?, NULL, ?, ?, ?)`,
  ).run(
    options.key,
    options.runId,
    options.state,
    options.updatedAt,
    options.updatedAt,
  );
}

function reservationRows(
  db: SqliteDatabase,
): Array<{ key: string; run_id: string; state: string; updated_at: number }> {
  return (
    db.prepare(
      `SELECT key, run_id, state, updated_at FROM ${START_IDEMPOTENCY_TABLE}
       ORDER BY key`,
    ) as unknown as {
      all(): Array<{
        key: string;
        run_id: string;
        state: string;
        updated_at: number;
      }>;
    }
  ).all();
}

describe('purgeExpiredWorkflowRuns — start reservations', () => {
  it('deletes a spent reservation in the SAME batch as its run’s snapshot', async () => {
    // #given a completed run past both horizons, with its key already settled
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createReservationTable(sqlite);
    await createResourceOwnershipSchema(sqliteUnitDatabase(sqlite) as never);
    seedRun(sqlite, {
      runId: 'run-old',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedReservation(sqlite, {
      key: 'key-old',
      runId: 'run-old',
      state: 'terminal',
      updatedAt: NOW - 8 * DAY_MS,
    });

    // #when
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      ttlMs: 7 * DAY_MS,
      resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });

    // #then both are gone, and gone together
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(reservationRows(sqlite)).toEqual([]);
  });

  it('KEEPS a reservation whose horizon has not elapsed, so a late retry is told ALREADY_SETTLED', async () => {
    // #given a run at the run-TTL boundary but a key-validity horizon twice as
    // long — the configuration a host uses when its callers retry for longer
    // than it keeps run summaries
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createReservationTable(sqlite);
    await createResourceOwnershipSchema(sqliteUnitDatabase(sqlite) as never);
    seedRun(sqlite, {
      runId: 'run-old',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedReservation(sqlite, {
      key: 'key-old',
      runId: 'run-old',
      state: 'terminal',
      updatedAt: NOW - 8 * DAY_MS,
    });

    // #when
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      ttlMs: 7 * DAY_MS,
      startIdempotencyTtlMs: 30 * DAY_MS,
      resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });

    // #then the snapshot is reclaimed and the reservation OUTLIVES it. That
    // ordering is the whole point: a retry after this pass hits
    // ALREADY_SETTLED instead of looking like a brand-new key.
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(reservationRows(sqlite)).toEqual([
      expect.objectContaining({ key: 'key-old', state: 'terminal' }),
    ]);
  });

  it('floors the reservation horizon at the run TTL, whatever a caller asks for', async () => {
    // #given a caller asking for a horizon SHORTER than run retention — a
    // configuration in which a reservation would be reaped while its run is
    // still readable, and the next retry of that key would start a second run
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createReservationTable(sqlite);
    seedRun(sqlite, {
      runId: 'run-live',
      status: 'success',
      updatedAt: NOW - 1 * DAY_MS,
    });
    seedReservation(sqlite, {
      key: 'key-live',
      runId: 'run-live',
      state: 'terminal',
      updatedAt: NOW - 1 * DAY_MS,
    });

    // #when
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      ttlMs: 7 * DAY_MS,
      startIdempotencyTtlMs: 1,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });

    // #then the run is not eligible, and neither is its reservation: the floor
    // makes the dangerous configuration unreachable rather than merely unwise.
    expect(remainingRunIds(sqlite)).toEqual(['run-live']);
    expect(reservationRows(sqlite)).toHaveLength(1);
  });

  it('marks a reservation the terminal reconcile missed, instead of stranding it', async () => {
    // #given a run that completed and was purged, but whose reservation is
    // still 'started' — the shape a crash between the terminal persist and
    // settleRun leaves behind
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createReservationTable(sqlite);
    seedRun(sqlite, {
      runId: 'run-old',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedReservation(sqlite, {
      key: 'key-stranded',
      runId: 'run-old',
      state: 'started',
      updatedAt: NOW - 8 * DAY_MS,
    });

    // #when
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      ttlMs: 7 * DAY_MS,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });

    // #then it is terminal, its horizon re-stamped from THIS moment, and it
    // survives this pass — so it is both purgeable later and out of the drain
    // inventory now.
    expect(reservationRows(sqlite)).toEqual([
      {
        key: 'key-stranded',
        run_id: 'run-old',
        state: 'terminal',
        updated_at: NOW,
      },
    ]);
  });

  it('reaps a reservation ORPHANED by an earlier pass, once past its horizon', async () => {
    // #given a reservation whose run's snapshot was purged long ago. The
    // batch pairing can never see it again — its run is not in any eligible
    // set — so without a sweep of its own this row would live forever.
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createReservationTable(sqlite);
    seedReservation(sqlite, {
      key: 'key-orphan',
      runId: 'run-long-gone',
      state: 'terminal',
      updatedAt: NOW - 40 * DAY_MS,
    });
    seedReservation(sqlite, {
      key: 'key-young-orphan',
      runId: 'run-also-gone',
      state: 'terminal',
      updatedAt: NOW - 1 * DAY_MS,
    });

    // #when
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      ttlMs: 7 * DAY_MS,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });

    // #then only the one past its horizon
    expect(reservationRows(sqlite).map((row) => row.key)).toEqual([
      'key-young-orphan',
    ]);
  });

  it('never reaps an orphan candidate whose run is still readable', async () => {
    // #given a reservation older than every horizon whose run STILL EXISTS —
    // a live suspended run, which retention never touches
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createReservationTable(sqlite);
    seedRun(sqlite, {
      runId: 'run-suspended',
      status: 'suspended',
      updatedAt: NOW - 90 * DAY_MS,
    });
    seedReservation(sqlite, {
      key: 'key-suspended',
      runId: 'run-suspended',
      state: 'terminal',
      updatedAt: NOW - 90 * DAY_MS,
    });

    // #when
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      ttlMs: 7 * DAY_MS,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });

    // #then it survives. `NOT EXISTS (snapshot)` is not an optimization — it
    // is what makes the HARD rule structural rather than a consequence of
    // whatever a host configured the horizon to be.
    expect(reservationRows(sqlite)).toHaveLength(1);
  });

  it('still purges runs on a deployment where no key has ever been used', async () => {
    // #given the reservation table wired but never created — its DDL is lazy,
    // so a deployment on which nobody used a key has none. A batch naming a
    // missing table fails as ONE TRANSACTION, which would take run retention
    // down with it.
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    await createResourceOwnershipSchema(sqliteUnitDatabase(sqlite) as never);
    seedRun(sqlite, {
      runId: 'run-old',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });

    // #when
    const deleted = await purgeExpiredWorkflowRuns(
      sqliteUnitDatabase(sqlite) as never,
      {
        ttlMs: 7 * DAY_MS,
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
        startIdempotencyTable: START_IDEMPOTENCY_TABLE,
        now: () => NOW,
      },
    );

    // #then retention is enforced anyway: an absent table holds no reservation
    // to reap, which is not a reason to stop reclaiming runs.
    expect(deleted).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });

  it('pairs reservations on the artifact path too', async () => {
    // #given the per-run path a host with R2 artifacts takes — a different
    // batch, and therefore a second place the pairing could have been missed
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createReservationTable(sqlite);
    seedRun(sqlite, {
      runId: 'run-old',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedReservation(sqlite, {
      key: 'key-old',
      runId: 'run-old',
      state: 'terminal',
      updatedAt: NOW - 8 * DAY_MS,
    });

    // #when
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      ttlMs: 7 * DAY_MS,
      artifactStore: { deleteRun: async () => 0 },
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });

    // #then
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(reservationRows(sqlite)).toEqual([]);
  });

  it('refuses a reservation table name that is not a safe SQL identifier', async () => {
    // #given — the name is interpolated into every statement above
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);

    // #when / #then
    await expect(
      purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
        ttlMs: DAY_MS,
        startIdempotencyTable: 'reservations; DROP TABLE x',
      }),
    ).rejects.toThrow(/safe SQL identifier/);
  });
});

describe('RUN_TTL_FLOWSAFE_PURGE_TABLES', () => {
  it('names the production constants, not literals, so a rename fails here', () => {
    // #given — the flowsafe-owned half of what run retention deletes from.
    // It is separate from RUN_TTL_PURGE_TABLES because the schema guard's
    // biconditional is over the `mastra_%` inventory: folding ours in would
    // make that guard assert an equality it cannot mean.
    //
    // #then each entry is the EXPORTED name its purge statement interpolates.
    // A rename of either table changes both sides at once, so this cannot drift
    // the way a copied literal would.
    expect([...RUN_TTL_FLOWSAFE_PURGE_TABLES].sort()).toEqual(
      [RESOURCE_OWNERSHIP_TABLE, START_IDEMPOTENCY_TABLE].sort(),
    );
  });
});
