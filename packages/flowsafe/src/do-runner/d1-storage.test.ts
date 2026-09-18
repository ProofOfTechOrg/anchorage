// SPDX-License-Identifier: Apache-2.0
// Fast SQL-unit coverage over node:sqlite: json_extract status filters and
// ISO-cutoff comparisons execute in SQLite, while the Wrangler harness owns
// D1 concurrency, transaction, and runtime fidelity.

import { describe, expect, it, vi } from 'vitest';

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
  type PurgeExpiredRunsOptions,
  parseRunRetentionCursor,
  purgeExpiredBackgroundTasks,
  purgeExpiredNotifications,
  purgeExpiredScheduleTriggers,
  purgeExpiredThreadState,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
  RUN_TTL_FLOWSAFE_PURGE_TABLES,
  type RunDeadlineCursor,
  type RunRetentionCursor,
  type SnapshotDatabase,
  type SnapshotStatement,
  sweepExpiredRunDeadlines,
} from './d1-storage.js';
import { normalizeStartExecutionIdentity } from './execution-admission.js';
import { FENCED_WORKFLOW_STORAGE } from './fenced-workflow-capability.js';
import { FencedWorkflowsStorageD1 } from './fenced-workflows-d1.js';
import { parseRunLifecycle } from './run-lifecycle.js';
import { decodeRunStartIdentity } from './run-provenance.js';
import {
  START_IDEMPOTENCY_DDL,
  START_IDEMPOTENCY_TABLE,
} from './start-idempotency.js';
import { validateTablePrefix } from './table-prefix.js';

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

type RetentionTestDatabase = SnapshotDatabase &
  Required<Pick<SnapshotDatabase, 'batch'>>;

function retentionDb(db: SqliteDatabase): RetentionTestDatabase {
  return sqliteUnitDatabase(db) as RetentionTestDatabase;
}

function lifecycleStores(db: SqliteDatabase): {
  snapshots: RetentionTestDatabase;
  resources: D1ResourceOwnershipStore;
} {
  const binding = sqliteUnitDatabase(db);
  return {
    snapshots: binding as RetentionTestDatabase,
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
      updatedAt TEXT NOT NULL,
      UNIQUE(workflow_name, run_id)
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
    name: 'FencedWorkflowsStorageD1',
    construct: (binding, tablePrefix) =>
      new FencedWorkflowsStorageD1({ binding: binding as never, tablePrefix }),
  },
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
      purgeExpiredWorkflowRuns(db as RetentionTestDatabase, {
        advanceCursor: async () => {},
        ttlMs: DAY_MS,
        tablePrefix,
        now,
      }),
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
  it.each([
    'transition',
    'cursor',
  ] as const)('contains a throwing Error.message during %s failure reporting', async (boundary) => {
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
    const failure = Object.defineProperty(new Error(), 'message', {
      get() {
        throw new Error('message getter failed');
      },
    });
    const attempts: string[] = [];
    const advances: string[] = [];
    let cursor: RunDeadlineCursor | undefined;
    await expect(
      sweepExpiredRunDeadlines(d1Like(db), {
        now: () => NOW,
        transition: async (candidate) => {
          attempts.push(candidate.runId);
          if (boundary === 'transition' && candidate.runId === 'poison')
            throw failure;
        },
        advanceCursor: async (next) => {
          advances.push(next.runId);
          if (boundary === 'cursor' && next.runId === 'poison') throw failure;
          cursor = next;
        },
      }),
    ).rejects.toThrow(/1 of 2 run\(s\) failed \(wf\/poison:/);
    expect(attempts).toEqual(['poison', 'eligible']);
    expect(advances).toEqual(['poison', 'eligible']);
    expect(cursor).toEqual({
      workflowId: 'wf',
      runId: 'eligible',
      deadlineAt: NOW - 1,
    });
  });

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
  it('rejects non-string prefixes without coercion', () => {
    const coerce = vi.fn(() => 'safe_');
    const object = {
      toString: coerce,
      [Symbol.toPrimitive]: coerce,
      get length() {
        coerce();
        return 5;
      },
    };
    for (const value of [
      true,
      false,
      null,
      1,
      [],
      new String('safe_'),
      object,
    ]) {
      const prefix = value as unknown as string;
      expect(() => validateTablePrefix(prefix)).toThrow(
        'Invalid tablePrefix: use an empty prefix',
      );
      const prepare = vi.fn();
      const binding = { prepare } as unknown as D1DatabaseBinding;
      expect(() => createD1Storage({ binding, tablePrefix: prefix })).toThrow(
        'Invalid tablePrefix: use an empty prefix',
      );
      for (const { construct } of PUBLIC_STORAGE_PREFIX_CASES)
        expect(() => construct(binding, prefix)).toThrow(
          'Invalid tablePrefix: use an empty prefix',
        );
      expect(prepare).not.toHaveBeenCalled();
    }
    expect(coerce).not.toHaveBeenCalled();
    for (const value of [
      undefined,
      '',
      '_tenant_01_',
      'tenant_01_',
      MAX_TABLE_PREFIX,
    ])
      expect(validateTablePrefix(value)).toBe(value);
    expect(() => validateTablePrefix(OVERLONG_TABLE_PREFIX, 'custom')).toThrow(
      'Invalid custom: must be at most 39 characters',
    );
  });

  it('preserves inherited and non-enumerable disabled or custom domain overrides', async () => {
    const binding = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;
    const custom = new FencedWorkflowsStorageD1({ binding: binding as never });
    // @mastra/cloudflare-d1 backs neither of these domains, so a host-supplied
    // store is the only value either can resolve to.
    const definitions = { upsert: async () => undefined };
    const knowledge = { query: async () => [] };
    for (const mode of ['inherited', 'non-enumerable']) {
      for (const workflows of [false, custom]) {
        for (const supplied of [false, true]) {
          const values = {
            workflows,
            threadState: false,
            notifications: false,
            workflowDefinitions: supplied ? definitions : false,
            knowledge: supplied ? knowledge : false,
          };
          const domains =
            mode === 'inherited'
              ? Object.create(values)
              : Object.defineProperties(
                  {},
                  Object.fromEntries(
                    Object.entries(values).map(([key, value]) => [
                      key,
                      { value },
                    ]),
                  ),
                );
          Object.defineProperty(domains, 'ignored', {
            enumerable: true,
            get() {
              throw new Error('unknown getter');
            },
          });
          const storage = createD1Storage({ binding, domains });
          expect(await storage.getStore('workflows')).toBe(
            workflows === false ? undefined : custom,
          );
          expect(await storage.getStore('threadState')).toBeUndefined();
          expect(await storage.getStore('notifications')).toBeUndefined();
          expect(await storage.getStore('workflowDefinitions')).toBe(
            supplied ? definitions : undefined,
          );
          expect(await storage.getStore('knowledge')).toBe(
            supplied ? knowledge : undefined,
          );
        }
      }
    }
  });

  it('resolves the domains the D1 adapter does not back to undefined', async () => {
    const binding = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;

    const storage = createD1Storage({ binding });

    // Red if a core release auto-installs a fallback store for either domain,
    // the way it already does for threadState.
    expect(await storage.getStore('workflowDefinitions')).toBeUndefined();
    expect(await storage.getStore('knowledge')).toBeUndefined();
  });

  it('captures composition inputs before either storage constructor', async () => {
    const first = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;
    const second = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;
    let bindingReads = 0;
    let workflowReads = 0;
    let domainReads = 0;
    let prefixReads = 0;
    let idReads = 0;
    const domains = {
      get workflows() {
        workflowReads += 1;
        return workflowReads === 1 ? undefined : (false as const);
      },
    };
    const storage = createD1Storage({
      get binding() {
        return ++bindingReads === 1 ? first : second;
      },
      get id() {
        idReads += 1;
        return 'captured';
      },
      get tablePrefix() {
        return ++prefixReads === 1 ? 'First_' : 'second_';
      },
      get domains() {
        domainReads += 1;
        return domains;
      },
    });
    await storage.init();
    const workflows = await storage.getStore('workflows');
    expect(workflows).toBeInstanceOf(FencedWorkflowsStorageD1);
    if (!(workflows instanceof FencedWorkflowsStorageD1))
      throw new Error('missing owned workflow domain');
    expect(workflows[FENCED_WORKFLOW_STORAGE]?.database).toBe(first);
    expect(workflows[FENCED_WORKFLOW_STORAGE]?.tablePrefix).toBe('first_');
    expect([
      bindingReads,
      workflowReads,
      domainReads,
      prefixReads,
      idReads,
    ]).toEqual([1, 1, 1, 1, 1]);
  });
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
    const db: RetentionTestDatabase = {
      batch: async () => [],
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
    const db: RetentionTestDatabase = {
      batch: async () => [],
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
    const deleted = await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
    });
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
      purgeExpiredWorkflowRuns(retentionDb(sqlite), {
        advanceCursor: async () => {},
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).resolves.toBe(1);
    expect(remainingRunIds(sqlite)).toEqual([`${status}-incomplete`]);
  });

  it('returns 0 when nothing qualifies', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'r1',
      status: 'success',
      updatedAt: NOW - 1 * DAY_MS,
    });
    expect(
      await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
        advanceCursor: async () => {},
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
        advanceCursor: async () => {},
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
        advanceCursor: async () => {},
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
    const racing: RetentionTestDatabase = {
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
        advanceCursor: async () => {},
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
    const deleted = await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      tablePrefix: 'flowsafe_',
      now: () => NOW,
    });
    expect(deleted).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual(['unprefixed']);
    expect(remainingRunIds(sqlite, 'flowsafe_')).toEqual([]);
  });

  it('skips malformed snapshot rows instead of aborting the purge', async () => {
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
    const deleted = await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
    });
    expect(deleted).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual(['corrupt', 'stale-live']);
  });

  it('treats the TTL boundary exclusively: exactly-at-cutoff rows survive', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'boundary',
      status: 'success',
      updatedAt: NOW - 7 * DAY_MS,
    });
    expect(
      await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
        advanceCursor: async () => {},
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
  });

  it('treats a MISSING snapshot table as zero purgeable runs (Mastra creates it lazily)', async () => {
    const sqlite = openSqlite();
    expect(
      await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
        advanceCursor: async () => {},
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
    expect(
      await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
        advanceCursor: async () => {},
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
    const deleted = await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
      artifactStore,
    });
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
    const racing: RetentionTestDatabase = {
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
        advanceCursor: async () => {},
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

  it('bounds artifact work per invocation', async () => {
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
    const first = await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
      advanceCursor: async () => {},
      ...options,
    });
    const second = await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
      advanceCursor: async () => {},
      ...options,
    });
    expect(first).toBe(2);
    expect(second).toBe(1);
    expect(deletedArtifacts.sort()).toEqual(['stale-a', 'stale-b', 'stale-c']);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });

  it("a failing artifact delete leaves that run's snapshot row for the next sweep (artifacts-first ordering)", async () => {
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
    await expect(
      purgeExpiredWorkflowRuns(retentionDb(sqlite), {
        advanceCursor: async () => {},
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        artifactStore,
      }),
    ).rejects.toThrow('R2 unavailable');
    expect(remainingRunIds(sqlite)).toEqual(['stale-done']);
  });

  it("one run's wedged artifact delete does not stall the eligible rows behind it", async () => {
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
    await expect(
      purgeExpiredWorkflowRuns(retentionDb(sqlite), {
        advanceCursor: async () => {},
        ...options,
      }),
    ).rejects.toThrow('permanently broken');
    expect(remainingRunIds(sqlite)).toEqual(['r3-bad']);
    expect(
      await purgeExpiredWorkflowRuns(retentionDb(sqlite), {
        advanceCursor: async () => {},
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
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (let index = 0; index < 5; index += 1) {
      seedRun(sqlite, {
        runId: `stale-${index}`,
        status: 'success',
        updatedAt: NOW - 40 * DAY_MS,
      });
    }
    const db = retentionDb(sqlite);
    const first = await purgeExpiredWorkflowRuns(db, {
      advanceCursor: async () => {},
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });
    const survivors = remainingRunIds(sqlite).length;
    const second = await purgeExpiredWorkflowRuns(db, {
      advanceCursor: async () => {},
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });
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
  row: {
    id: string;
    threadId: string;
    status: string;
    updatedAt: number | string;
    tablePrefix?: string;
  },
): void {
  const iso =
    typeof row.updatedAt === 'string'
      ? row.updatedAt
      : new Date(row.updatedAt).toISOString();
  db.prepare(
    `INSERT INTO ${row.tablePrefix ?? ''}mastra_notifications
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

  describe.each(['', 'tenant_'])('table prefix %j', (tablePrefix) => {
    it.each([
      {
        name: 'ordinary year',
        now: '2026-07-07T12:00:00.000Z',
        oldOffset: '2026-07-06T15:59:59.999+04:00',
        futureOffset: '2026-07-06T07:00:00-06:00',
        equalOffset: '2026-07-06T16:00:00+0400',
      },
      {
        name: 'negative year',
        now: '-000100-01-02T12:00:00.000Z',
        oldOffset: '-000100-01-01T15:59:59.999+04:00',
        futureOffset: '-000100-01-01T07:00:00-06:00',
        equalOffset: '-000100-01-01T16:00:00+0400',
      },
    ])('applies notification TTL by Date chronology for $name', async ({
      now,
      oldOffset,
      futureOffset,
      equalOffset,
    }) => {
      const sqlite = openSqlite();
      const binding = sqliteUnitDatabase(sqlite) as SignalDatabase;
      await new D1NotificationsStorage(binding, tablePrefix).init();
      const instant = new Date(now).getTime();
      const cutoff = instant - DAY_MS;
      const rows = [
        {
          id: 'extended-future',
          status: 'delivered',
          updatedAt: '+010000-01-01T00:00:00.000Z',
        },
        {
          id: 'negative-past',
          status: 'seen',
          updatedAt: '-000200-01-01T00:00:00.000Z',
        },
        {
          id: 'old-canonical',
          status: 'dismissed',
          updatedAt: new Date(cutoff - 1).toISOString(),
        },
        { id: 'old-offset', status: 'archived', updatedAt: oldOffset },
        {
          id: 'future-offset',
          status: 'discarded',
          updatedAt: futureOffset,
        },
        { id: 'equal-offset', status: 'delivered', updatedAt: equalOffset },
        {
          id: 'equal-canonical',
          status: 'discarded',
          updatedAt: new Date(cutoff).toISOString(),
        },
        { id: 'pending-old', status: 'pending', updatedAt: oldOffset },
      ];
      for (const row of rows) {
        expect(Number.isFinite(new Date(row.updatedAt).getTime())).toBe(true);
        seedNotification(sqlite, { ...row, threadId: 'thread', tablePrefix });
      }
      if (tablePrefix !== '') {
        await new D1NotificationsStorage(binding, '').init();
        seedNotification(sqlite, {
          id: 'other-prefix',
          threadId: 'thread',
          status: 'delivered',
          updatedAt: oldOffset,
        });
      }
      const retained = rows.filter(
        (row) =>
          row.status === 'pending' ||
          new Date(row.updatedAt).getTime() >= cutoff,
      );
      expect(
        await purgeExpiredNotifications(d1Like(sqlite), {
          ttlMs: DAY_MS,
          tablePrefix,
          now: () => instant,
        }),
      ).toBe(rows.length - retained.length);
      expect(
        sqlite
          .prepare(
            `SELECT id, status, updatedAt FROM ${tablePrefix}mastra_notifications ORDER BY id`,
          )
          .all(),
      ).toEqual(retained.sort((a, b) => a.id.localeCompare(b.id)));
      if (tablePrefix !== '') {
        expect(
          sqlite.prepare('SELECT id FROM mastra_notifications').all(),
        ).toEqual([{ id: 'other-prefix' }]);
      }
    });

    it('reads a missing table as zero', async () => {
      const sqlite = openSqlite();
      expect(
        await purgeExpiredNotifications(d1Like(sqlite), {
          ttlMs: DAY_MS,
          tablePrefix,
        }),
      ).toBe(0);
    });
  });

  it.each([
    'invalid',
    '0',
    '2026-07-06T00:00:00',
    '2026-07-06T00:00:00Z\0',
    '+275760-09-13T00:00:00.001Z',
  ])('retains unsupported raw updatedAt %j', async (updatedAt) => {
    const sqlite = openSqlite();
    await signalDb(sqlite);
    seedNotification(sqlite, {
      id: 'unreadable',
      threadId: 'thread',
      status: 'delivered',
      updatedAt,
    });
    expect(
      await purgeExpiredNotifications(d1Like(sqlite), {
        ttlMs: DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
    expect(sqlite.prepare('SELECT id FROM mastra_notifications').all()).toEqual(
      [{ id: 'unreadable' }],
    );
  });

  it.each([
    { now: NaN, ttlMs: DAY_MS },
    { now: Infinity, ttlMs: DAY_MS },
    { now: 8_640_000_000_000_001, ttlMs: 0 },
    { now: -8_640_000_000_000_000, ttlMs: 1 },
  ])('rejects a cutoff outside the finite Date range: %j', async ({
    now,
    ttlMs,
  }) => {
    const sqlite = openSqlite();
    await signalDb(sqlite);
    seedNotification(sqlite, {
      id: 'retained',
      threadId: 'thread',
      status: 'delivered',
      updatedAt: NOW - 2 * DAY_MS,
    });
    await expect(
      purgeExpiredNotifications(d1Like(sqlite), { ttlMs, now: () => now }),
    ).rejects.toThrow();
    expect(sqlite.prepare('SELECT id FROM mastra_notifications').all()).toEqual(
      [{ id: 'retained' }],
    );
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
  it('removes an expired snapshot and legacy reservation', async () => {
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
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(reservationRows(sqlite)).toEqual([]);
  });

  it('KEEPS a reservation whose horizon has not elapsed, so a late retry is told ALREADY_SETTLED', async () => {
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
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      startIdempotencyTtlMs: 30 * DAY_MS,
      resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(reservationRows(sqlite)).toEqual([
      expect.objectContaining({ key: 'key-old', state: 'terminal' }),
    ]);
  });

  it('floors the reservation horizon at the run TTL, whatever a caller asks for', async () => {
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
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      startIdempotencyTtlMs: 1,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });
    expect(remainingRunIds(sqlite)).toEqual(['run-live']);
    expect(reservationRows(sqlite)).toHaveLength(1);
  });

  it('preserves an unsettled legacy reservation after snapshot expiry', async () => {
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
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });
    expect(reservationRows(sqlite)).toEqual([
      {
        key: 'key-stranded',
        run_id: 'run-old',
        state: 'started',
        updated_at: NOW - 8 * DAY_MS,
      },
    ]);
  });

  it('reaps a reservation ORPHANED by an earlier pass, once past its horizon', async () => {
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
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });
    expect(reservationRows(sqlite).map((row) => row.key)).toEqual([
      'key-young-orphan',
    ]);
  });

  it('never reaps an orphan candidate whose run is still readable', async () => {
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
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });
    expect(reservationRows(sqlite)).toHaveLength(1);
  });

  it('sweeps orphans on the strict side of the horizon, and never one whose snapshot survives', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createReservationTable(sqlite);
    const cutoff = NOW - 7 * DAY_MS;
    seedReservation(sqlite, {
      key: 'key-at-cutoff',
      runId: 'run-gone-a',
      state: 'terminal',
      updatedAt: cutoff,
    });
    seedReservation(sqlite, {
      key: 'key-past-cutoff',
      runId: 'run-gone-b',
      state: 'terminal',
      updatedAt: cutoff - 1,
    });
    seedRun(sqlite, {
      runId: 'run-still-here',
      status: 'suspended',
      updatedAt: NOW - 90 * DAY_MS,
    });
    seedReservation(sqlite, {
      key: 'key-with-snapshot',
      runId: 'run-still-here',
      state: 'terminal',
      updatedAt: NOW - 90 * DAY_MS,
    });
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });
    expect(reservationRows(sqlite).map((row) => row.key)).toEqual([
      'key-at-cutoff',
      'key-with-snapshot',
    ]);
  });

  it('still purges runs on a deployment where no key has ever been used', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    await createResourceOwnershipSchema(sqliteUnitDatabase(sqlite) as never);
    seedRun(sqlite, {
      runId: 'run-old',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    const deleted = await purgeExpiredWorkflowRuns(
      sqliteUnitDatabase(sqlite) as never,
      {
        advanceCursor: async () => {},
        ttlMs: 7 * DAY_MS,
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
        startIdempotencyTable: START_IDEMPOTENCY_TABLE,
        now: () => NOW,
      },
    );
    expect(deleted).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });

  it('removes an expired snapshot and legacy reservation with an artifact store', async () => {
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
    await purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
      advanceCursor: async () => {},
      ttlMs: 7 * DAY_MS,
      artifactStore: { deleteRun: async () => 0 },
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      now: () => NOW,
    });
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(reservationRows(sqlite)).toEqual([]);
  });

  it('refuses a reservation table name that is not a safe SQL identifier', async () => {
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    await expect(
      purgeExpiredWorkflowRuns(sqliteUnitDatabase(sqlite) as never, {
        advanceCursor: async () => {},
        ttlMs: DAY_MS,
        startIdempotencyTable: 'reservations; DROP TABLE x',
      }),
    ).rejects.toThrow(/safe SQL identifier/);
  });
});

describe('RUN_TTL_FLOWSAFE_PURGE_TABLES', () => {
  it('names the production constants, not literals, so a rename fails here', () => {
    expect([...RUN_TTL_FLOWSAFE_PURGE_TABLES].sort()).toEqual(
      [RESOURCE_OWNERSHIP_TABLE, START_IDEMPOTENCY_TABLE].sort(),
    );
  });
});

function retentionSnapshot(
  db: SqliteDatabase,
  runId: string,
  options: {
    prefix?: string;
    workflowId?: string;
    token?: string;
    provenance?: unknown;
    status?: string;
    padding?: string;
  } = {},
): void {
  const execution = normalizeStartExecutionIdentity({
    tablePrefix: options.prefix ?? '',
    workflowId: options.workflowId ?? 'wf',
    runId,
    startToken: options.token ?? 'S1',
    owner: { kind: 'human', id: 'initiator' },
    target: { kind: 'workflow', id: 'logical' },
  });
  const provenance = options.provenance ?? {
    version: 2,
    startToken: execution.startToken,
    startIdentity: { owner: execution.owner, target: execution.target },
  };
  if (options.provenance === undefined) decodeRunStartIdentity(provenance);
  db.prepare(`INSERT INTO "${options.prefix ?? ''}mastra_workflow_snapshot"
    (workflow_name,run_id,resourceId,snapshot,createdAt,updatedAt) VALUES (?,?,NULL,?,?,?)`).run(
    options.workflowId ?? 'wf',
    runId,
    JSON.stringify({
      status: options.status ?? 'success',
      requestContext: { 'flowsafe.runProvenance': provenance },
      padding: options.padding,
    }),
    new Date(NOW - 9 * DAY_MS).toISOString(),
    new Date(NOW - 8 * DAY_MS).toISOString(),
  );
}

function retentionReservation(
  db: SqliteDatabase,
  key: string,
  overrides: Record<string, unknown> = {},
): void {
  const row = {
    key,
    owner_kind: 'human',
    owner_id: 'initiator',
    target_kind: 'workflow',
    target_id: 'logical',
    run_id: 'run',
    thread_id: null,
    state: 'terminal',
    created_at: NOW - 10 * DAY_MS,
    updated_at: NOW - 8 * DAY_MS,
    start_token: 'S1',
    start_table_prefix: '',
    start_workflow_id: 'wf',
    ...overrides,
  };
  db.prepare(
    `INSERT INTO ${START_IDEMPOTENCY_TABLE} (${Object.keys(row).join(',')}) VALUES (${Object.keys(
      row,
    )
      .map(() => '?')
      .join(',')})`,
  ).run(...Object.values(row));
}

function retentionCycle(options: Partial<PurgeExpiredRunsOptions> = {}) {
  let cursor: RunRetentionCursor | undefined;
  const advances: RunRetentionCursor[] = [];
  return {
    advances,
    get cursor() {
      return cursor;
    },
    options(): PurgeExpiredRunsOptions {
      return {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        startIdempotencyTable: START_IDEMPOTENCY_TABLE,
        cursor,
        advanceCursor: async (next) => {
          cursor = structuredClone(next);
          advances.push(cursor);
        },
        ...options,
      };
    },
  };
}

function retentionIntercept(
  db: RetentionTestDatabase,
  hooks: {
    read?: (sql: string, result: unknown) => unknown;
    beforeBatch?: () => void;
    afterBatch?: (results: unknown[]) => unknown[];
    statement?: (sql: string, values: unknown[]) => void;
  },
): RetentionTestDatabase {
  function wrap(
    sql: string,
    statement: SnapshotStatement,
    values: unknown[],
  ): SnapshotStatement {
    return {
      ...statement,
      all: async <T>() => {
        hooks.statement?.(sql, values);
        const result = await statement.all<T>();
        return (hooks.read ? hooks.read(sql, result) : result) as {
          results: T[];
        };
      },
    };
  }
  const statements = new WeakMap<
    SnapshotStatement,
    { sql: string; values: unknown[] }
  >();
  function tracked(
    sql: string,
    statement: SnapshotStatement,
    values: unknown[],
  ): SnapshotStatement {
    const wrapped = wrap(sql, statement, values);
    wrapped.bind = (...bound) => tracked(sql, statement.bind(...bound), bound);
    statements.set(wrapped, { sql, values });
    return wrapped;
  }
  return {
    prepare: (sql) => tracked(sql, db.prepare(sql), []),
    batch: async (prepared) => {
      for (const statement of prepared) {
        const entry = statements.get(statement);
        if (!entry) throw new Error('untracked statement');
        hooks.statement?.(entry.sql, entry.values);
      }
      hooks.beforeBatch?.();
      const results = await db.batch(prepared);
      // The hooks hand back `unknown[]` on purpose: that is what a wrong
      // adapter returns.
      return hooks.afterBatch
        ? (hooks.afterBatch(results) as Awaited<
            ReturnType<RetentionTestDatabase['batch']>
          >)
        : results;
    },
  };
}

function retentionWorld() {
  const sqlite = openSqlite();
  createSnapshotTable(sqlite);
  createReservationTable(sqlite);
  return { sqlite, db: retentionDb(sqlite), cycle: retentionCycle() };
}

function replaceRetentionSnapshot(
  db: SqliteDatabase,
  snapshot: unknown,
  runId = 'run',
): void {
  db.prepare(
    'UPDATE mastra_workflow_snapshot SET snapshot=? WHERE run_id=?',
  ).run(
    typeof snapshot === 'string' ? snapshot : JSON.stringify(snapshot),
    runId,
  );
}

interface RetentionSnapshotFixture {
  status: string;
  padding: string;
  requestContext: {
    'flowsafe.runProvenance': {
      version: number | boolean;
      startToken: string;
      startIdentity: {
        owner: { kind: string; id: string };
        target: { kind: string; id: string; threadId?: string };
        padding?: string;
      } | null;
      agentStart?: unknown;
      attemptToken?: string;
      resumeCounts?: unknown;
    };
  };
}

function currentRetentionSnapshot(
  db: SqliteDatabase,
  runId = 'run',
): RetentionSnapshotFixture {
  const row = db
    .prepare('SELECT snapshot FROM mastra_workflow_snapshot WHERE run_id=?')
    .get(runId) as { snapshot: string };
  return JSON.parse(row.snapshot);
}

describe('generation-safe run retention', () => {
  it.each([
    null,
    false,
    0,
    '',
    [],
    {},
    { version: 2, tablePrefix: '' },
    {
      version: 1,
      tablePrefix: '',
      snapshots: { afterRowId: 2, highWaterRowId: 1 },
    },
    {
      version: 1,
      tablePrefix: '',
      snapshots: { afterRowId: -Infinity, highWaterRowId: 1 },
    },
    {
      version: 1,
      tablePrefix: '',
      reservations: { afterRowId: 0, highWaterRowId: 1 },
    },
    { version: 1, tablePrefix: '', extra: true },
  ])('rejects malformed persisted cursor %j before I/O', async (cursor) => {
    const prepare = vi.fn();
    await expect(
      purgeExpiredWorkflowRuns(
        { prepare, batch: vi.fn() },
        { ttlMs: 0, cursor: cursor as never, advanceCursor: vi.fn() },
      ),
    ).rejects.toThrow();
    expect(prepare).not.toHaveBeenCalled();
  });

  it('captures cursor scope and negative positions without invoking accessors', () => {
    const input = {
      version: 1 as const,
      tablePrefix: 'TENANT_',
      startIdempotencyTable: 'Keys',
      snapshots: { afterRowId: -9, highWaterRowId: -2 },
    };
    const parsed = parseRunRetentionCursor(input);
    input.snapshots.afterRowId = 0;
    expect(parsed).toEqual({
      version: 1,
      tablePrefix: 'tenant_',
      startIdempotencyTable: 'keys',
      snapshots: { afterRowId: -9, highWaterRowId: -2 },
    });
    expect(Object.isFrozen(parsed)).toBe(true);
    expect(Object.isFrozen(parsed?.snapshots)).toBe(true);
    const getter = vi.fn(() => 1);
    expect(() =>
      parseRunRetentionCursor(
        Object.defineProperty({ tablePrefix: '' }, 'version', { get: getter }),
      ),
    ).toThrow();
    expect(getter).not.toHaveBeenCalled();
  });

  it.each([
    { ttlMs: -1 },
    { ttlMs: Infinity },
    { startIdempotencyTtlMs: NaN },
    { limit: 0 },
    { limit: 0.5 },
    { limit: Infinity },
    { now: () => Infinity },
    { now: () => 8.64e15 },
    { now: null },
    { advanceCursor: undefined },
    { artifactStore: {} },
    { cursor: { version: 1, tablePrefix: 'other_' } },
    { resourceOwnerTable: 'x'.repeat(90_000) },
  ])('rejects invalid captured options before SQL %j', async (invalid) => {
    const prepare = vi.fn();
    await expect(
      purgeExpiredWorkflowRuns({ prepare, batch: vi.fn() }, {
        ttlMs: 0,
        advanceCursor: vi.fn(),
        ...invalid,
      } as never),
    ).rejects.toThrow();
    expect(prepare).not.toHaveBeenCalled();
  });

  it('requires batch for a snapshot-only purge', async () => {
    const prepare = vi.fn();
    await expect(
      purgeExpiredWorkflowRuns({ prepare } as never, {
        ttlMs: 0,
        advanceCursor: vi.fn(),
      }),
    ).rejects.toThrow(/batch/);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('captures callbacks, methods and one clock before the first read', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    const callback = vi.fn();
    const artifactStore = {
      deleteRun: async () => {
        callback();
        return 0;
      },
    };
    const now = vi.fn(() => NOW);
    const options = { ...cycle.options(), artifactStore, now };
    const capturedAdvance = options.advanceCursor;
    const intercepted = retentionIntercept(db, {
      read: (_sql, result) => {
        options.ttlMs = Infinity;
        options.advanceCursor = async () => {
          throw new Error('replaced cursor');
        };
        artifactStore.deleteRun = async () => {
          throw new Error('replaced artifacts');
        };
        intercepted.batch = async () => {
          throw new Error('replaced batch');
        };
        return result;
      },
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, options)).toBe(1);
    expect(now).toHaveBeenCalledTimes(1);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(cycle.advances).toHaveLength(2);
    expect(options.advanceCursor).not.toBe(capturedAdvance);
  });

  it.each([
    [
      'generation',
      (
        p: RetentionSnapshotFixture['requestContext']['flowsafe.runProvenance'],
      ) => {
        p.startToken = 'S2';
      },
    ],
    [
      'owner',
      (
        p: RetentionSnapshotFixture['requestContext']['flowsafe.runProvenance'],
      ) => {
        if (p.startIdentity) p.startIdentity.owner.id = 'other';
      },
    ],
    [
      'null versus missing',
      (
        p: RetentionSnapshotFixture['requestContext']['flowsafe.runProvenance'],
      ) => {
        p.agentStart = null;
      },
    ],
    [
      'boolean versus number',
      (
        p: RetentionSnapshotFixture['requestContext']['flowsafe.runProvenance'],
      ) => {
        p.version = true;
      },
    ],
    [
      'explicit null identity',
      (
        p: RetentionSnapshotFixture['requestContext']['flowsafe.runProvenance'],
      ) => {
        p.startIdentity = null;
      },
    ],
  ])('preserves current snapshot and key after changed %s', async (_name, change) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    retentionReservation(sqlite, 'key', { state: 'started' });
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        const snapshot = currentRetentionSnapshot(sqlite);
        change(snapshot.requestContext['flowsafe.runProvenance']);
        replaceRetentionSnapshot(sqlite, snapshot);
      },
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      0,
    );
    expect(remainingRunIds(sqlite)).toEqual(['run']);
    expect(reservationRows(sqlite)[0]?.state).toBe('started');
    expect(cycle.cursor?.snapshots).toBeUndefined();
  });

  it.each([
    '{"status":"success","requestContext":{},"requestContext":{"flowsafe.runProvenance":{"version":2,"startToken":"S2"}}}',
    '{"status":"success","requestContext":{"flowsafe.runProvenance":{"version":2,"startToken":"S1","version":1}}}',
    '{"status":"success","requestContext":{"flowsafe.runProvenance":{"version":2,"startToken":"S1"},"flowsafe.runProvenance":{"version":2,"startToken":"S2"}}}',
  ])('preserves ambiguous duplicate provenance paths during selection and recheck', async (raw) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => replaceRetentionSnapshot(sqlite, raw),
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      0,
    );
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
    expect(remainingRunIds(sqlite)).toEqual(['run']);
  });

  it('permits H, progress and large unowned payload changes with the same owned capsule', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run', { padding: 'x'.repeat(200_000) });
    let largestSelector = 0;
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        const snapshot = currentRetentionSnapshot(sqlite);
        snapshot.requestContext['flowsafe.runProvenance'].attemptToken =
          'other-H';
        snapshot.requestContext['flowsafe.runProvenance'].resumeCounts = [
          ['step', 2],
        ];
        snapshot.padding += 'more';
        replaceRetentionSnapshot(sqlite, snapshot);
      },
      statement: (sql, values) => {
        if (sql.includes('DELETE FROM "mastra_workflow_snapshot"'))
          largestSelector = String(values[3]).length;
      },
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      1,
    );
    expect(largestSelector).toBeLessThan(1000);
  });

  it.each([
    'snapshot',
    'createdAt',
    'updatedAt',
    'resourceId',
  ])('preserves a changed legacy %s field', async (field) => {
    const { sqlite, db, cycle } = retentionWorld();
    seedRun(sqlite, {
      runId: 'run',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        const value =
          field === 'snapshot'
            ? '{"status":"success","changed":true}'
            : field === 'resourceId'
              ? 'new-resource'
              : new Date(NOW - 7.5 * DAY_MS).toISOString();
        sqlite
          .prepare(`UPDATE mastra_workflow_snapshot SET ${field}=?`)
          .run(value);
      },
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      0,
    );
    expect(remainingRunIds(sqlite)).toEqual(['run']);
  });

  it('does not treat rowid reuse as snapshot identity', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    seedRun(sqlite, {
      runId: 'run',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        sqlite.exec('DELETE FROM mastra_workflow_snapshot');
        retentionSnapshot(sqlite, 'replacement');
      },
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      0,
    );
    expect(remainingRunIds(sqlite)).toEqual(['replacement']);
  });

  it.each([
    'sibling-prefix',
    'sibling-workflow',
    'reserved-owner',
  ])('protects %s ownership while removing an expired snapshot', async (kind) => {
    const { sqlite, db, cycle } = retentionWorld();
    await createResourceOwnershipSchema(db as never);
    const resources = new D1ResourceOwnershipStore(db as never);
    await resources.claim('run', 'run', {
      kind: 'human',
      id: 'resource-owner',
    });
    retentionSnapshot(sqlite, 'run');
    if (kind === 'reserved-owner')
      sqlite.exec(
        `UPDATE ${RESOURCE_OWNERSHIP_TABLE} SET reservation_token='claim-token'`,
      );
    else {
      if (kind === 'sibling-prefix') createSnapshotTable(sqlite, 'sibling_');
      retentionSnapshot(sqlite, 'run', {
        prefix: kind === 'sibling-prefix' ? 'sibling_' : '',
        workflowId: 'other',
      });
      sqlite.exec(
        `UPDATE ${kind === 'sibling-prefix' ? 'sibling_' : ''}mastra_workflow_snapshot SET snapshot='corrupt' WHERE workflow_name='other'`,
      );
    }
    expect(
      await purgeExpiredWorkflowRuns(db, {
        ...cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).toBe(1);
    expect(
      sqlite
        .prepare(
          `SELECT owner_kind,owner_id,reservation_token FROM ${RESOURCE_OWNERSHIP_TABLE} WHERE resource_kind='run' AND resource_id='run'`,
        )
        .get(),
    ).toEqual({
      owner_kind: 'human',
      owner_id: 'resource-owner',
      reservation_token: kind === 'reserved-owner' ? 'claim-token' : null,
    });
  });

  it('releases a resource owner distinct from the initiating principal', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    const resources = new D1ResourceOwnershipStore(db as never);
    await resources.claim('run', 'run', {
      kind: 'human',
      id: 'resource-owner',
    });
    retentionSnapshot(sqlite, 'run');
    expect(
      await purgeExpiredWorkflowRuns(db, {
        ...cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).toBe(1);
    expect(await resources.owner('run', 'run')).toBeUndefined();
  });

  it.each([
    ['start_token', 'S2'],
    ['start_table_prefix', 'other_'],
    ['start_workflow_id', 'other'],
    ['run_id', 'other'],
    ['owner_kind', 'agent'],
    ['owner_id', 'other'],
    ['target_kind', 'agent'],
    ['target_id', 'other'],
    ['thread_id', 'thread'],
  ])('does not terminalize a different bound tuple field %s', async (field, value) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    retentionReservation(sqlite, 'mismatch', {
      state: 'started',
      [field]: value,
    });
    retentionReservation(sqlite, 'matching', { state: 'started' });
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(1);
    expect(reservationRows(sqlite)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          key: 'matching',
          state: 'terminal',
          updated_at: NOW,
        }),
        expect.objectContaining({
          key: 'mismatch',
          state: 'started',
          updated_at: NOW - 8 * DAY_MS,
        }),
      ]),
    );
  });

  it('settles complete aliases and preserves terminal stamps, unbound and legacy records', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    for (const key of ['alias-one', 'alias-two'])
      retentionReservation(sqlite, key, { state: 'reserved' });
    retentionReservation(sqlite, 'terminal', { updated_at: NOW - DAY_MS });
    retentionReservation(sqlite, 'legacy', {
      state: 'started',
      start_token: null,
      start_table_prefix: null,
      start_workflow_id: null,
    });
    retentionReservation(sqlite, 'unbound', {
      state: 'reserved',
      start_token: '',
      start_table_prefix: null,
      start_workflow_id: null,
    });
    retentionReservation(sqlite, 'null-bound', {
      state: 'started',
      start_table_prefix: null,
    });
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(1);
    expect(reservationRows(sqlite)).toEqual(
      expect.arrayContaining([
        ...['alias-one', 'alias-two'].map((key) =>
          expect.objectContaining({ key, state: 'terminal', updated_at: NOW }),
        ),
        expect.objectContaining({ key: 'terminal', updated_at: NOW - DAY_MS }),
        expect.objectContaining({ key: 'legacy', state: 'started' }),
        expect.objectContaining({ key: 'unbound', state: 'reserved' }),
        expect.objectContaining({ key: 'null-bound', state: 'started' }),
      ]),
    );
  });

  it('pairs an inherited logical identity at its physical child address and leaves unattributed snapshots unpaired', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'child', { workflowId: 'physical-child' });
    retentionReservation(sqlite, 'child-key', {
      run_id: 'child',
      start_workflow_id: 'physical-child',
      state: 'started',
    });
    retentionReservation(sqlite, 'root-key', {
      run_id: 'child',
      start_workflow_id: 'logical',
      state: 'started',
    });
    retentionSnapshot(sqlite, 'unattributed', {
      provenance: { version: 2, startToken: 'S1' },
    });
    retentionReservation(sqlite, 'unattributed-key', {
      run_id: 'unattributed',
      state: 'started',
    });
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(2);
    expect(reservationRows(sqlite)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: 'child-key', state: 'terminal' }),
        expect.objectContaining({ key: 'root-key', state: 'started' }),
        expect.objectContaining({ key: 'unattributed-key', state: 'started' }),
      ]),
    );
  });

  it.each([
    'same',
    'different',
    'absent',
    'missing-namespace',
    'legacy',
    'malformed',
    'oversize',
    'unbound',
    'null-bound',
  ])('classifies orphan %s without inferring namespace or generation', async (kind) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionReservation(
      sqlite,
      'key',
      kind === 'missing-namespace'
        ? { start_table_prefix: 'missing_' }
        : kind === 'null-bound'
          ? { start_table_prefix: null }
          : kind === 'unbound'
            ? {
                start_token: '',
                start_table_prefix: null,
                start_workflow_id: null,
              }
            : {},
    );
    if (!['absent', 'missing-namespace'].includes(kind)) {
      retentionSnapshot(sqlite, 'run', {
        status: 'suspended',
        token: kind === 'different' ? 'S2' : 'S1',
      });
      if (kind === 'legacy')
        replaceRetentionSnapshot(sqlite, { status: 'suspended' });
      if (kind === 'malformed') replaceRetentionSnapshot(sqlite, 'broken');
      if (kind === 'oversize') {
        const snapshot = currentRetentionSnapshot(sqlite);
        const identity =
          snapshot.requestContext['flowsafe.runProvenance'].startIdentity;
        if (!identity) throw new Error('fixture requires start identity');
        identity.padding = 'x'.repeat(4096);
        replaceRetentionSnapshot(sqlite, snapshot);
      }
    }
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
    expect(reservationRows(sqlite)).toHaveLength(
      ['different', 'absent', 'missing-namespace'].includes(kind) ? 0 : 1,
    );
  });

  it('rechecks the observed different generation before orphan expiry', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run', { token: 'S2', status: 'suspended' });
    retentionReservation(sqlite, 'key');
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        const snapshot = currentRetentionSnapshot(sqlite);
        snapshot.requestContext['flowsafe.runProvenance'].startToken = 'S1';
        replaceRetentionSnapshot(sqlite, snapshot);
      },
    });
    await purgeExpiredWorkflowRuns(intercepted, cycle.options());
    expect(reservationRows(sqlite)).toHaveLength(1);
  });

  it('rechecks a raw orphan key when its timestamp or binding changes', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionReservation(sqlite, 'key');
    const intercepted = retentionIntercept(db, {
      beforeBatch: () =>
        sqlite.exec(
          `UPDATE ${START_IDEMPOTENCY_TABLE} SET start_token='S2',updated_at=updated_at+0.5`,
        ),
    });
    await purgeExpiredWorkflowRuns(intercepted, cycle.options());
    expect(reservationRows(sqlite)).toHaveLength(1);
  });

  it('expires independent orphans without a snapshot table and preserves legacy raw thread values', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    sqlite.exec('DROP TABLE mastra_workflow_snapshot');
    retentionReservation(sqlite, 'bound');
    retentionReservation(sqlite, 'legacy', {
      start_token: null,
      start_table_prefix: null,
      start_workflow_id: null,
      thread_id: 'invalid/thread',
      created_at: -2.5,
      updated_at: -1.5,
    });
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
    expect(reservationRows(sqlite)).toEqual([]);
  });
});

describe('run retention schema, progress and result contracts', () => {
  it.each([
    0, 1, 2, 3,
  ])('supports reservation schema stage %i with compatible legacy rows', async (stage) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionReservation(sqlite, 'legacy', {
      start_token: null,
      start_table_prefix: null,
      start_workflow_id: null,
      thread_id: 'not/a/path',
      updated_at: -1.5,
    });
    for (const column of [
      'start_workflow_id',
      'start_table_prefix',
      'start_token',
    ].slice(0, 3 - stage))
      sqlite.exec(
        `ALTER TABLE ${START_IDEMPOTENCY_TABLE} DROP COLUMN ${column}`,
      );
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
    expect(reservationRows(sqlite)).toEqual([]);
  });

  it.each([
    1, 2,
  ])('retains nonnull partial companion data at stage %i', async (stage) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionReservation(sqlite, 'partial');
    for (const column of ['start_workflow_id', 'start_table_prefix'].slice(
      0,
      3 - stage,
    ))
      sqlite.exec(
        `ALTER TABLE ${START_IDEMPOTENCY_TABLE} DROP COLUMN ${column}`,
      );
    await purgeExpiredWorkflowRuns(db, cycle.options());
    expect(reservationRows(sqlite)).toHaveLength(1);
  });

  it.each([
    'view',
    'bad-prefix',
    'overlong-prefix',
    'namespace-overflow',
    'reservation-view',
    'reservation-columns',
    'reservation-order',
  ])('refuses unsupported schema %s before artifact or mutation work', async (kind) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    if (kind === 'view')
      sqlite.exec(
        'CREATE VIEW other_mastra_workflow_snapshot AS SELECT * FROM mastra_workflow_snapshot',
      );
    if (kind === 'bad-prefix')
      sqlite.exec('CREATE TABLE "bad-prefix_mastra_workflow_snapshot" (x)');
    if (kind === 'overlong-prefix')
      sqlite.exec(
        `CREATE TABLE "${'x'.repeat(40)}mastra_workflow_snapshot" (x)`,
      );
    if (kind === 'namespace-overflow')
      for (let i = 0; i < 64; i++) createSnapshotTable(sqlite, `n${i}_`);
    if (kind === 'reservation-view') {
      sqlite.exec(
        `ALTER TABLE ${START_IDEMPOTENCY_TABLE} RENAME TO reserved_rows; CREATE VIEW ${START_IDEMPOTENCY_TABLE} AS SELECT * FROM reserved_rows`,
      );
    }
    if (kind === 'reservation-columns')
      sqlite.exec(
        `ALTER TABLE ${START_IDEMPOTENCY_TABLE} ADD COLUMN unexpected TEXT`,
      );
    if (kind === 'reservation-order')
      sqlite.exec(
        `ALTER TABLE ${START_IDEMPOTENCY_TABLE} RENAME COLUMN start_token TO other`,
      );
    const deleteRun = vi.fn(async () => 0);
    const beforeBatch = vi.fn();
    await expect(
      purgeExpiredWorkflowRuns(retentionIntercept(db, { beforeBatch }), {
        ...cycle.options(),
        artifactStore: { deleteRun },
      }),
    ).rejects.toThrow();
    expect(deleteRun).not.toHaveBeenCalled();
    expect(beforeBatch).not.toHaveBeenCalled();
    expect(remainingRunIds(sqlite)).toEqual(['run']);
    expect(cycle.advances).toEqual([]);
  });

  it('rebuilds a held group when a reservation table appears, without repeating artifacts', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    sqlite.exec(`DROP TABLE ${START_IDEMPOTENCY_TABLE}`);
    retentionSnapshot(sqlite, 'run');
    const deleteRun = vi.fn(async () => {
      createReservationTable(sqlite);
      retentionReservation(sqlite, 'key', { state: 'started' });
      return 0;
    });
    expect(
      await purgeExpiredWorkflowRuns(db, {
        ...cycle.options(),
        artifactStore: { deleteRun },
      }),
    ).toBe(1);
    expect(deleteRun).toHaveBeenCalledTimes(1);
    expect(reservationRows(sqlite)).toEqual([
      expect.objectContaining({
        key: 'key',
        state: 'terminal',
        updated_at: NOW,
      }),
    ]);
  });

  it.each([
    'create',
    'rename',
    'drop',
  ])('rebuilds pending snapshot work after namespace %s', async (kind) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    createSnapshotTable(sqlite, 'sibling_');
    let count = 0;
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        if (count++ > 0) return;
        if (kind === 'create') createSnapshotTable(sqlite, 'new_');
        if (kind === 'rename')
          sqlite.exec(
            'ALTER TABLE sibling_mastra_workflow_snapshot RENAME TO renamed_mastra_workflow_snapshot',
          );
        if (kind === 'drop') sqlite.exec('DROP TABLE mastra_workflow_snapshot');
      },
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      kind === 'drop' ? 0 : 1,
    );
    expect(count).toBe(2);
    expect(cycle.cursor?.snapshots).toBeUndefined();
  });

  it('uses one invocation-wide schema retry and retains the phase cursor on second churn', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    const deleteRun = vi.fn(async () => 0);
    let changes = 0;
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => createSnapshotTable(sqlite, `changed${changes++}_`),
    });
    await expect(
      purgeExpiredWorkflowRuns(intercepted, {
        ...cycle.options(),
        artifactStore: { deleteRun },
      }),
    ).rejects.toThrow(/repeatedly/);
    expect(changes).toBe(2);
    expect(deleteRun).toHaveBeenCalledTimes(1);
    expect(cycle.advances).toEqual([]);
    expect(remainingRunIds(sqlite)).toEqual(['run']);
  });

  it('rereads pending legacy keys through a schema upgrade and preserves changed keys', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    for (const key of ['unchanged', 'changed'])
      retentionReservation(sqlite, key, {
        start_token: null,
        start_table_prefix: null,
        start_workflow_id: null,
      });
    for (const column of [
      'start_workflow_id',
      'start_table_prefix',
      'start_token',
    ])
      sqlite.exec(
        `ALTER TABLE ${START_IDEMPOTENCY_TABLE} DROP COLUMN ${column}`,
      );
    let batches = 0;
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        if (batches++ > 0) return;
        for (const column of [
          'start_token',
          'start_table_prefix',
          'start_workflow_id',
        ])
          sqlite.exec(
            `ALTER TABLE ${START_IDEMPOTENCY_TABLE} ADD COLUMN ${column} TEXT`,
          );
        sqlite.exec(
          `UPDATE ${START_IDEMPOTENCY_TABLE} SET updated_at=updated_at+1 WHERE key='changed'`,
        );
      },
    });
    await purgeExpiredWorkflowRuns(intercepted, cycle.options());
    expect(reservationRows(sqlite).map((row) => row.key)).toEqual(['changed']);
  });

  it('does not reinterpret a bound orphan as legacy after schema downgrade', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionReservation(sqlite, 'key');
    let batches = 0;
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        if (batches++ > 0) return;
        for (const column of [
          'start_workflow_id',
          'start_table_prefix',
          'start_token',
        ])
          sqlite.exec(
            `ALTER TABLE ${START_IDEMPOTENCY_TABLE} DROP COLUMN ${column}`,
          );
      },
    });
    await expect(
      purgeExpiredWorkflowRuns(intercepted, cycle.options()),
    ).rejects.toThrow(/no such column/);
    expect(reservationRows(sqlite)).toHaveLength(1);
    expect(cycle.advances).toHaveLength(1);
  });

  it('reclassifies a mixed pending orphan group after namespace appearance', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionReservation(sqlite, 'absent', {
      run_id: 'absent',
      start_table_prefix: 'late_',
    });
    retentionReservation(sqlite, 'different', {
      run_id: 'different',
      start_table_prefix: 'late_',
    });
    retentionReservation(sqlite, 'same', {
      run_id: 'same',
      start_table_prefix: 'late_',
    });
    let batches = 0;
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        if (batches++ > 0) return;
        createSnapshotTable(sqlite, 'late_');
        retentionSnapshot(sqlite, 'different', {
          prefix: 'late_',
          token: 'S2',
          status: 'suspended',
        });
        retentionSnapshot(sqlite, 'same', {
          prefix: 'late_',
          status: 'suspended',
        });
      },
    });
    await purgeExpiredWorkflowRuns(intercepted, cycle.options());
    expect(reservationRows(sqlite).map((row) => row.key)).toEqual(['same']);
  });

  it('advances past more than a page of malformed and oversized capsules', async () => {
    const { sqlite, db } = retentionWorld();
    const cycle = retentionCycle({ limit: 90 });
    for (let i = 0; i < 95; i++)
      retentionSnapshot(sqlite, `poison-${i}`, {
        provenance:
          i % 2
            ? { version: true }
            : {
                version: 2,
                startToken: 'S1',
                startIdentity: { padding: 'x'.repeat(4096) },
              },
      });
    retentionSnapshot(sqlite, 'eligible');
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
    expect(cycle.cursor?.snapshots).toEqual({
      afterRowId: 90,
      highWaterRowId: 96,
    });
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(1);
    expect(remainingRunIds(sqlite)).not.toContain('eligible');
    expect(cycle.cursor?.snapshots).toBeUndefined();
  });

  it('bounds artifact failure diagnostics and reaches later rows with persisted progress', async () => {
    const { sqlite, db } = retentionWorld();
    for (let i = 0; i < 92; i++) retentionSnapshot(sqlite, `bad-${i}`);
    retentionSnapshot(sqlite, 'eligible');
    const cycle = retentionCycle({
      artifactStore: {
        deleteRun: async (_wf, run) => {
          if (run.startsWith('bad')) throw Object.create(null);
          return 0;
        },
      },
    });
    await expect(purgeExpiredWorkflowRuns(db, cycle.options())).rejects.toThrow(
      /artifact deletion failed/,
    );
    expect(cycle.cursor?.snapshots).toEqual({
      afterRowId: 90,
      highWaterRowId: 93,
    });
    await expect(purgeExpiredWorkflowRuns(db, cycle.options())).rejects.toThrow(
      /artifact deletion failed/,
    );
    expect(remainingRunIds(sqlite)).not.toContain('eligible');
    expect(remainingRunIds(sqlite)).toHaveLength(92);
    expect(cycle.cursor?.snapshots).toBeUndefined();
  });

  it('reports bounded diagnostics for unsupported snapshot and reservation pages', async () => {
    const { sqlite, db } = retentionWorld();
    const cycle = retentionCycle();
    const diagnostics = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      for (let index = 0; index < 95; index++) {
        retentionSnapshot(sqlite, `bad-snapshot-${index}`, {
          provenance: { version: true },
        });
        retentionReservation(sqlite, `bad-reservation-${index}`, {
          start_token: null,
          start_table_prefix: null,
          start_workflow_id: null,
          target_id: 'x'.repeat(5000),
        });
      }
      retentionSnapshot(sqlite, 'eligible');
      retentionReservation(sqlite, 'eligible');
      expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
      expect(diagnostics).toHaveBeenCalledTimes(2);
      expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(1);
      expect(diagnostics).toHaveBeenCalledTimes(4);
      expect(diagnostics.mock.calls.map(([line]) => JSON.parse(line))).toEqual([
        { type: 'run-retention-skip', kind: 'snapshot', tablePrefix: '' },
        { type: 'run-retention-skip', kind: 'reservation', tablePrefix: '' },
        { type: 'run-retention-skip', kind: 'snapshot', tablePrefix: '' },
        { type: 'run-retention-skip', kind: 'reservation', tablePrefix: '' },
      ]);
      expect(
        diagnostics.mock.calls.every(
          ([line]) => typeof line === 'string' && line.length < 256,
        ),
      ).toBe(true);
      expect(remainingRunIds(sqlite)).not.toContain('eligible');
      expect(reservationRows(sqlite).map((row) => row.key)).not.toContain(
        'eligible',
      );
    } finally {
      diagnostics.mockRestore();
    }
  });

  it('uses negative rowids and a fixed high water despite continuous inserts', async () => {
    const { sqlite, db } = retentionWorld();
    for (const [index, rid] of [-5, -3, -1].entries()) {
      retentionSnapshot(sqlite, `initial-${index}`, { status: 'suspended' });
      sqlite
        .prepare('UPDATE mastra_workflow_snapshot SET rowid=? WHERE run_id=?')
        .run(rid, `initial-${index}`);
    }
    const cycle = retentionCycle({ limit: 1 });
    await purgeExpiredWorkflowRuns(db, cycle.options());
    expect(cycle.cursor?.snapshots).toEqual({
      afterRowId: -5,
      highWaterRowId: -1,
    });
    retentionSnapshot(sqlite, 'new-one');
    await purgeExpiredWorkflowRuns(db, cycle.options());
    expect(cycle.cursor?.snapshots).toEqual({
      afterRowId: -3,
      highWaterRowId: -1,
    });
    retentionSnapshot(sqlite, 'new-two');
    await purgeExpiredWorkflowRuns(db, cycle.options());
    expect(cycle.cursor?.snapshots).toBeUndefined();
    expect(remainingRunIds(sqlite)).toContain('new-one');
  });

  it('advances independent reservation positions past malformed rows and oversized legacy keys', async () => {
    const { sqlite, db } = retentionWorld();
    for (let i = 0; i < 92; i++)
      retentionReservation(sqlite, `bad-${i}`, {
        start_token: null,
        start_table_prefix: null,
        start_workflow_id: null,
        target_id: 'x'.repeat(5000),
      });
    retentionReservation(sqlite, 'eligible');
    const cycle = retentionCycle();
    await purgeExpiredWorkflowRuns(db, cycle.options());
    expect(cycle.cursor?.reservations).toEqual({
      afterRowId: 90,
      highWaterRowId: 93,
    });
    await purgeExpiredWorkflowRuns(db, cycle.options());
    expect(cycle.cursor?.reservations).toBeUndefined();
    expect(reservationRows(sqlite).map((row) => row.key)).not.toContain(
      'eligible',
    );
    expect(reservationRows(sqlite)).toHaveLength(92);
  });

  it.each([
    'lost',
    'short',
    'sparse',
    'failure',
    'missing-meta',
    'negative',
    'fractional',
    'string-count',
    'schema-missing',
    'schema-count',
    'schema-false-with-changes',
  ])('does not checkpoint an uncertain batch result: %s', async (failure) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    const intercepted = retentionIntercept(db, {
      afterBatch: (results) => {
        if (failure === 'lost') throw new Error('response lost');
        if (failure === 'short') return results.slice(1);
        if (failure === 'sparse') {
          delete results[1];
          return results;
        }
        if (failure === 'failure')
          results[1] = { success: false, meta: { changes: 1 } };
        if (failure === 'missing-meta') results[1] = {};
        if (failure === 'negative') results[1] = { meta: { changes: -1 } };
        if (failure === 'fractional') results[1] = { meta: { changes: 0.5 } };
        if (failure === 'string-count') results[1] = { meta: { changes: '1' } };
        if (failure === 'schema-missing') results[0] = { results: [] };
        if (failure === 'schema-count')
          results[0] = { results: [{ schema_ok: true }] };
        if (failure === 'schema-false-with-changes')
          results[0] = { results: [{ schema_ok: 0 }] };
        return results;
      },
    });
    await expect(
      purgeExpiredWorkflowRuns(intercepted, cycle.options()),
    ).rejects.toThrow();
    expect(cycle.advances).toEqual([]);
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
  });

  it('stops before orphan work when snapshot cursor persistence fails after commit', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    retentionReservation(sqlite, 'orphan', { run_id: 'gone' });
    const advanceCursor = vi.fn(async () => {
      throw new Error('cursor storage failed');
    });
    await expect(
      purgeExpiredWorkflowRuns(db, { ...cycle.options(), advanceCursor }),
    ).rejects.toThrow('cursor storage failed');
    expect(advanceCursor).toHaveBeenCalledTimes(1);
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(reservationRows(sqlite)).toHaveLength(1);
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
    expect(reservationRows(sqlite)).toEqual([]);
  });

  it('rolls back snapshot, owner and key when a later paired mutation fails', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    const resources = new D1ResourceOwnershipStore(db as never);
    await resources.claim('run', 'run', {
      kind: 'human',
      id: 'resource-owner',
    });
    retentionSnapshot(sqlite, 'run');
    retentionReservation(sqlite, 'key', { state: 'started' });
    sqlite.exec(
      `CREATE TRIGGER reject_retention_key BEFORE UPDATE ON ${START_IDEMPOTENCY_TABLE} BEGIN SELECT RAISE(ABORT,'key failure'); END`,
    );
    await expect(
      purgeExpiredWorkflowRuns(db, {
        ...cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).rejects.toThrow('key failure');
    expect(remainingRunIds(sqlite)).toEqual(['run']);
    expect(await resources.owner('run', 'run')).toEqual({
      kind: 'human',
      id: 'resource-owner',
    });
    expect(reservationRows(sqlite)[0]?.state).toBe('started');
    expect(cycle.advances).toEqual([]);
  });
});

describe('run retention SQL boundaries', () => {
  it('preserves case-sensitive identity and state under NOCASE column declarations', async () => {
    const sqlite = openSqlite();
    sqlite.exec(`CREATE TABLE mastra_workflow_snapshot (workflow_name TEXT COLLATE NOCASE NOT NULL, run_id TEXT COLLATE NOCASE NOT NULL,
      resourceId TEXT COLLATE NOCASE, snapshot TEXT COLLATE NOCASE NOT NULL, createdAt TEXT NOT NULL, updatedAt TEXT NOT NULL, UNIQUE(workflow_name,run_id))`);
    sqlite.exec(
      START_IDEMPOTENCY_DDL.replaceAll('TEXT', 'TEXT COLLATE NOCASE'),
    );
    retentionSnapshot(sqlite, 'run');
    retentionReservation(sqlite, 'matching', { state: 'started' });
    retentionReservation(sqlite, 'token-case', {
      state: 'started',
      start_token: 's1',
    });
    retentionReservation(sqlite, 'owner-case', {
      state: 'started',
      owner_id: 'INITIATOR',
    });
    retentionReservation(sqlite, 'state-case', { state: 'STARTED' });
    const db = retentionDb(sqlite);
    const cycle = retentionCycle();
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(1);
    expect(reservationRows(sqlite)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: 'matching', state: 'terminal' }),
        expect.objectContaining({ key: 'token-case', state: 'started' }),
        expect.objectContaining({ key: 'owner-case', state: 'started' }),
        expect.objectContaining({ key: 'state-case', state: 'STARTED' }),
      ]),
    );
  });

  it('preserves a snapshot when its workflow name changes case', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    const intercepted = retentionIntercept(db, {
      beforeBatch: () =>
        sqlite.exec("UPDATE mastra_workflow_snapshot SET workflow_name='WF'"),
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      0,
    );
    expect(remainingRunIds(sqlite)).toEqual(['run']);
  });

  it('requires finite numeric terminal-key expiry and accepts negative fractional epochs', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    for (const key of [
      'negative-infinity',
      'positive-infinity',
      'text',
      'fractional',
      'at-cutoff',
    ])
      retentionReservation(sqlite, key);
    sqlite.exec(`UPDATE ${START_IDEMPOTENCY_TABLE} SET updated_at=-9e999 WHERE key='negative-infinity';
      UPDATE ${START_IDEMPOTENCY_TABLE} SET updated_at=9e999 WHERE key='positive-infinity';
      UPDATE ${START_IDEMPOTENCY_TABLE} SET updated_at='garbage' WHERE key='text';
      UPDATE ${START_IDEMPOTENCY_TABLE} SET created_at=-2.5,updated_at=-1.5 WHERE key='fractional'`);
    sqlite
      .prepare(
        `UPDATE ${START_IDEMPOTENCY_TABLE} SET updated_at=? WHERE key='at-cutoff'`,
      )
      .run(NOW - 7 * DAY_MS);
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(1);
    expect(reservationRows(sqlite).map((row) => row.key)).toEqual([
      'at-cutoff',
      'negative-infinity',
      'positive-infinity',
      'text',
    ]);
  });

  it('accepts registry names beyond 63 characters and rejects complete SQL overflow before artifacts', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    const name = `keys_${'x'.repeat(80)}`;
    sqlite.exec(`ALTER TABLE ${START_IDEMPOTENCY_TABLE} RENAME TO ${name}`);
    retentionSnapshot(sqlite, 'run');
    expect(
      await purgeExpiredWorkflowRuns(db, {
        ...cycle.options(),
        startIdempotencyTable: name,
      }),
    ).toBe(1);
    retentionSnapshot(sqlite, 'second');
    const deleteRun = vi.fn(async () => 0);
    await expect(
      purgeExpiredWorkflowRuns(db, {
        ...cycle.options(),
        startIdempotencyTable: undefined,
        cursor: undefined,
        resourceOwnerTable: 'x'.repeat(89_000),
        artifactStore: { deleteRun },
      }),
    ).rejects.toThrow(/budget/);
    expect(deleteRun).not.toHaveBeenCalled();
    expect(remainingRunIds(sqlite)).toEqual(['second']);
  });

  it('measures complete modern maximum-form statements and selectors over 64 namespaces', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    await createResourceOwnershipSchema(db as never);
    for (let i = 0; i < 63; i++) createSnapshotTable(sqlite, `namespace${i}_`);
    const largeIdentity = {
      owner: { kind: 'human', id: 'a'.repeat(200) },
      target: { kind: 'agent', id: 'a'.repeat(200), threadId: 't'.repeat(200) },
      padding: '\\"'.repeat(650),
    };
    for (let i = 0; i < 90; i++) {
      retentionSnapshot(sqlite, `run-${i}`, {
        workflowId: 'w'.repeat(200),
        provenance: {
          version: 2,
          startToken: 's'.repeat(200),
          startIdentity: largeIdentity,
          agentStart: { threaded: true },
        },
        padding: 'x'.repeat(20_000),
      });
    }
    const metrics = {
      statements: 0,
      maxSqlBytes: 0,
      maxBindings: 0,
      maxSelectorBytes: 0,
    };
    const intercepted = retentionIntercept(db, {
      statement: (sql, values) => {
        metrics.statements++;
        metrics.maxSqlBytes = Math.max(
          metrics.maxSqlBytes,
          new TextEncoder().encode(sql).length,
        );
        metrics.maxBindings = Math.max(metrics.maxBindings, values.length);
        for (const value of values)
          if (typeof value === 'string' && value.startsWith('['))
            metrics.maxSelectorBytes = Math.max(
              metrics.maxSelectorBytes,
              new TextEncoder().encode(value).length,
            );
        expect(new TextEncoder().encode(sql).length).toBeLessThanOrEqual(
          90_000,
        );
        expect(values.length).toBeLessThanOrEqual(100);
      },
    });
    expect(
      await purgeExpiredWorkflowRuns(intercepted, {
        ...cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
        limit: 1000,
      }),
    ).toBe(90);
    expect(metrics.maxSelectorBytes).toBeGreaterThan(500_000);
    expect(metrics.maxSelectorBytes).toBeLessThanOrEqual(1_000_000);
    expect(metrics.maxBindings).toBe(13);
    expect(metrics.statements).toBe(9);
    console.info('RETENTION_MODERN_MAX', JSON.stringify(metrics));
  });

  it('measures a complete legacy page and cross-namespace orphan observations', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    await createResourceOwnershipSchema(db as never);
    for (let i = 0; i < 63; i++) createSnapshotTable(sqlite, `n${i}_`);
    for (let i = 0; i < 90; i++) {
      seedRun(sqlite, {
        runId: `legacy-${i}`,
        status: 'success',
        updatedAt: NOW - 8 * DAY_MS,
      });
    }
    for (let i = 0; i < 90; i++) {
      const namespace = i % 64;
      const prefix = namespace === 0 ? '' : `n${namespace - 1}_`;
      retentionReservation(sqlite, `orphan-${i}`, {
        run_id: `current-${i}`,
        start_table_prefix: prefix,
      });
      if (i < 64)
        retentionSnapshot(sqlite, `current-${i}`, {
          prefix,
          token: 'S2',
          status: 'suspended',
        });
    }
    const metrics = {
      statements: 0,
      maxSqlBytes: 0,
      maxBindings: 0,
      legacyReads: 0,
    };
    const intercepted = retentionIntercept(db, {
      statement: (sql, values) => {
        metrics.statements++;
        metrics.maxSqlBytes = Math.max(
          metrics.maxSqlBytes,
          new TextEncoder().encode(sql).length,
        );
        metrics.maxBindings = Math.max(metrics.maxBindings, values.length);
        if (
          sql.startsWith('SELECT workflow_name, run_id, resourceId, snapshot')
        )
          metrics.legacyReads++;
        expect(values.length).toBeLessThanOrEqual(100);
        expect(new TextEncoder().encode(sql).length).toBeLessThanOrEqual(
          90_000,
        );
      },
    });
    expect(
      await purgeExpiredWorkflowRuns(intercepted, {
        ...cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).toBe(90);
    expect(reservationRows(sqlite)).toEqual([]);
    expect(metrics.legacyReads).toBe(90);
    expect(metrics.maxBindings).toBe(18);
    expect(metrics.statements).toBe(608);
    console.info('RETENTION_LEGACY_ORPHAN_MAX', JSON.stringify(metrics));
  });
});

describe('run retention cleanup timestamp contract', () => {
  const markers = [
    ['null', false],
    ['false', false],
    ['true', false],
    ['"done"', false],
    ['{}', false],
    ['[]', false],
    ['-1', false],
    ['0.5', false],
    ['9007199254740992', false],
    ['1e309', false],
    ['-1e309', false],
    ['0', true],
    ['1.0', true],
    ['1e0', true],
    ['9007199254740991', true],
  ] as const;

  async function cleanupFixture(
    kind: 'modern' | 'legacy',
    status: 'cancelled' | 'timed_out' = 'cancelled',
  ) {
    const world = retentionWorld();
    retentionSnapshot(world.sqlite, 'run', { status });
    retentionReservation(world.sqlite, 'key', {
      state: 'started',
      ...(kind === 'legacy'
        ? {
            start_token: null,
            start_table_prefix: null,
            start_workflow_id: null,
          }
        : {}),
    });
    const resources = new D1ResourceOwnershipStore(world.db as never);
    await resources.claim('run', 'run', {
      kind: 'human',
      id: 'resource-owner',
    });
    const snapshot = currentRetentionSnapshot(world.sqlite);
    const raw = JSON.stringify({
      ...snapshot,
      requestContext: {
        ...(kind === 'modern' ? snapshot.requestContext : {}),
        'flowsafe.runLifecycle': {
          version: 1,
          revision: 1,
          terminal: {
            status,
            error:
              status === 'cancelled'
                ? { code: 'CANCELLED', message: 'run was cancelled' }
                : { code: 'TIMED_OUT', message: 'run deadline expired' },
            transitionedAt: 0,
            replayPrincipals: [{ kind: 'human', id: 'initiator' }],
            cleanupCompletedAt: 0,
          },
        },
      },
    });
    return {
      ...world,
      resources,
      snapshot(marker: string) {
        return raw.replace(
          '"cleanupCompletedAt":0',
          `"cleanupCompletedAt":${marker}`,
        );
      },
    };
  }

  it.each(
    markers,
  )('classifies modern cleanup timestamp %s before artifacts', async (marker, complete) => {
    const h = await cleanupFixture('modern');
    const raw = h.snapshot(marker);
    const lifecycle = JSON.parse(raw).requestContext['flowsafe.runLifecycle'];
    if (complete)
      expect(parseRunLifecycle(lifecycle)?.terminal?.cleanupCompletedAt).toBe(
        JSON.parse(marker),
      );
    else expect(() => parseRunLifecycle(lifecycle)).toThrow();
    replaceRetentionSnapshot(h.sqlite, raw);
    const deleteRun = vi.fn(async () => 0);
    expect(
      await purgeExpiredWorkflowRuns(h.db, {
        ...h.cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
        artifactStore: { deleteRun },
      }),
    ).toBe(complete ? 1 : 0);
    expect(deleteRun).toHaveBeenCalledTimes(complete ? 1 : 0);
    expect(remainingRunIds(h.sqlite)).toEqual(complete ? [] : ['run']);
    expect(await h.resources.owner('run', 'run')).toEqual(
      complete ? undefined : { kind: 'human', id: 'resource-owner' },
    );
    expect(reservationRows(h.sqlite)).toEqual([
      expect.objectContaining({
        state: complete ? 'terminal' : 'started',
        updated_at: complete ? NOW : NOW - 8 * DAY_MS,
      }),
    ]);
  });

  it.each(
    markers,
  )('revalidates legacy cleanup timestamp %s before artifacts', async (marker, complete) => {
    const h = await cleanupFixture('legacy');
    replaceRetentionSnapshot(h.sqlite, h.snapshot('0'));
    const deleteRun = vi.fn(async () => 0);
    let rawReads = 0;
    const intercepted = retentionIntercept(h.db, {
      statement: (sql) => {
        if (
          sql.startsWith('SELECT workflow_name, run_id, resourceId, snapshot')
        ) {
          rawReads++;
          replaceRetentionSnapshot(h.sqlite, h.snapshot(marker));
        }
      },
    });
    expect(
      await purgeExpiredWorkflowRuns(intercepted, {
        ...h.cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
        artifactStore: { deleteRun },
      }),
    ).toBe(complete ? 1 : 0);
    expect(rawReads).toBe(1);
    expect(deleteRun).toHaveBeenCalledTimes(complete ? 1 : 0);
    expect(remainingRunIds(h.sqlite)).toEqual(complete ? [] : ['run']);
    expect(await h.resources.owner('run', 'run')).toEqual(
      complete ? undefined : { kind: 'human', id: 'resource-owner' },
    );
    expect(reservationRows(h.sqlite)).toEqual([
      expect.objectContaining({
        state: 'started',
        updated_at: NOW - 8 * DAY_MS,
      }),
    ]);
  });

  it.each([
    ['false', false],
    ['0', true],
  ] as const)('classifies initial legacy cleanup timestamp %s before artifacts', async (marker, complete) => {
    const h = await cleanupFixture('legacy', 'timed_out');
    replaceRetentionSnapshot(h.sqlite, h.snapshot(marker));
    const deleteRun = vi.fn(async () => 0);
    expect(
      await purgeExpiredWorkflowRuns(h.db, {
        ...h.cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
        artifactStore: { deleteRun },
      }),
    ).toBe(complete ? 1 : 0);
    expect(deleteRun).toHaveBeenCalledTimes(complete ? 1 : 0);
    expect(remainingRunIds(h.sqlite)).toEqual(complete ? [] : ['run']);
    expect(await h.resources.owner('run', 'run')).toEqual(
      complete ? undefined : { kind: 'human', id: 'resource-owner' },
    );
    expect(reservationRows(h.sqlite)).toEqual([
      expect.objectContaining({
        state: 'started',
        updated_at: NOW - 8 * DAY_MS,
      }),
    ]);
  });

  describe.each(['modern', 'legacy'] as const)('%s held mutation', (kind) => {
    it.each([
      'false',
      '1e309',
    ])('preserves a replacement with cleanup timestamp %s', async (marker) => {
      const h = await cleanupFixture(kind, 'timed_out');
      replaceRetentionSnapshot(h.sqlite, h.snapshot('0'));
      const deleteRun = vi.fn(async () => {
        replaceRetentionSnapshot(h.sqlite, h.snapshot(marker));
        return 0;
      });
      expect(
        await purgeExpiredWorkflowRuns(h.db, {
          ...h.cycle.options(),
          resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
          artifactStore: { deleteRun },
        }),
      ).toBe(0);
      expect(deleteRun).toHaveBeenCalledTimes(1);
      expect(remainingRunIds(h.sqlite)).toEqual(['run']);
      expect(await h.resources.owner('run', 'run')).toEqual({
        kind: 'human',
        id: 'resource-owner',
      });
      expect(reservationRows(h.sqlite)).toEqual([
        expect.objectContaining({
          state: 'started',
          updated_at: NOW - 8 * DAY_MS,
        }),
      ]);
    });
  });
});

describe('run retention rejection and preservation controls', () => {
  describe.each(['selection', 'held mutation'] as const)('%s', (boundary) => {
    it.each([
      'status',
      'escaped status',
      'requestContext',
      'flowsafe.runLifecycle',
      'terminal',
      'cleanupCompletedAt',
    ])('preserves duplicate eligibility path %s', async (path) => {
      const { sqlite, db, cycle } = retentionWorld();
      retentionSnapshot(sqlite, 'run', { status: 'cancelled' });
      retentionReservation(sqlite, 'key', {
        state: boundary === 'selection' ? 'terminal' : 'started',
      });
      const resources = new D1ResourceOwnershipStore(db as never);
      await resources.claim('run', 'run', {
        kind: 'human',
        id: 'resource-owner',
      });
      const terminal = {
        status: 'cancelled',
        error: { code: 'CANCELLED', message: 'run was cancelled' },
        transitionedAt: NOW - 9 * DAY_MS,
        replayPrincipals: [{ kind: 'human', id: 'initiator' }],
      };
      const incomplete = { version: 1, revision: 1, terminal };
      const complete = {
        ...incomplete,
        terminal: { ...terminal, cleanupCompletedAt: NOW - 8 * DAY_MS },
      };
      expect(parseRunLifecycle(incomplete)?.terminal).not.toHaveProperty(
        'cleanupCompletedAt',
      );
      expect(parseRunLifecycle(complete)?.terminal).toHaveProperty(
        'cleanupCompletedAt',
      );
      const snapshot = currentRetentionSnapshot(sqlite);
      const context = {
        ...snapshot.requestContext,
        'flowsafe.runLifecycle': complete,
      };
      const ordinary = JSON.stringify({ ...snapshot, requestContext: context });
      const replacements: Record<string, [string, string]> = {
        status: [
          '"status":"cancelled"',
          '"status":"cancelled","status":"running"',
        ],
        'escaped status': [
          '"status":"cancelled"',
          '"status":"cancelled","sta\\u0074us":"running"',
        ],
        requestContext: [
          `"requestContext":${JSON.stringify(context)}`,
          `"requestContext":${JSON.stringify(context)},"requestContext":${JSON.stringify({ ...context, 'flowsafe.runLifecycle': incomplete })}`,
        ],
        'flowsafe.runLifecycle': [
          `"flowsafe.runLifecycle":${JSON.stringify(complete)}`,
          `"flowsafe.runLifecycle":${JSON.stringify(complete)},"flowsafe.runLifecycle":${JSON.stringify(incomplete)}`,
        ],
        terminal: [
          `"terminal":${JSON.stringify(complete.terminal)}`,
          `"terminal":${JSON.stringify(complete.terminal)},"terminal":${JSON.stringify(terminal)}`,
        ],
        cleanupCompletedAt: [
          `"cleanupCompletedAt":${NOW - 8 * DAY_MS}`,
          `"cleanupCompletedAt":${NOW - 8 * DAY_MS},"cleanupCompletedAt":null`,
        ],
      };
      const replacement = replacements[path];
      if (!replacement) throw new Error('missing duplicate-path fixture');
      const ambiguous = ordinary.replace(...replacement);
      expect(ambiguous).not.toBe(ordinary);
      const decoded = JSON.parse(ambiguous);
      if (path.endsWith('status')) expect(decoded.status).toBe('running');
      else if (path === 'cleanupCompletedAt')
        expect(() =>
          parseRunLifecycle(decoded.requestContext['flowsafe.runLifecycle']),
        ).toThrow();
      else
        expect(
          parseRunLifecycle(decoded.requestContext['flowsafe.runLifecycle'])
            ?.terminal,
        ).not.toHaveProperty('cleanupCompletedAt');
      replaceRetentionSnapshot(
        sqlite,
        boundary === 'selection' ? ambiguous : ordinary,
      );
      const deleteRun = vi.fn(async () => 0);
      const intercepted = retentionIntercept(db, {
        beforeBatch: () => {
          if (boundary === 'held mutation')
            replaceRetentionSnapshot(sqlite, ambiguous);
        },
      });
      expect(
        await purgeExpiredWorkflowRuns(intercepted, {
          ...cycle.options(),
          resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
          artifactStore: { deleteRun },
        }),
      ).toBe(0);
      expect(remainingRunIds(sqlite)).toEqual(['run']);
      expect(reservationRows(sqlite)).toEqual([
        expect.objectContaining({
          key: 'key',
          state: boundary === 'selection' ? 'terminal' : 'started',
          updated_at: NOW - 8 * DAY_MS,
        }),
      ]);
      expect(await resources.owner('run', 'run')).toEqual({
        kind: 'human',
        id: 'resource-owner',
      });
      expect(deleteRun).toHaveBeenCalledTimes(boundary === 'selection' ? 0 : 1);

      replaceRetentionSnapshot(sqlite, ordinary);
      expect(
        await purgeExpiredWorkflowRuns(db, {
          ...cycle.options(),
          resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
        }),
      ).toBe(1);
      expect(remainingRunIds(sqlite)).toEqual([]);
      expect(await resources.owner('run', 'run')).toBeUndefined();
      expect(reservationRows(sqlite)).toEqual(
        boundary === 'selection'
          ? []
          : [expect.objectContaining({ state: 'terminal', updated_at: NOW })],
      );
    });
  });

  describe.each([
    'snapshot page',
    'orphan observation',
    'absent orphan observation',
  ] as const)('%s', (boundary) => {
    it.each([
      'missing',
      'numeric',
      'invalid JSON',
    ])('rejects a malformed owned capsule projection: %s', async (kind) => {
      const { sqlite, db, cycle } = retentionWorld();
      if (boundary !== 'absent orphan observation')
        retentionSnapshot(sqlite, 'run', {
          token: boundary === 'orphan observation' ? 'S2' : 'S1',
          status: boundary === 'orphan observation' ? 'suspended' : 'success',
        });
      retentionReservation(sqlite, 'key');
      let interceptedReads = 0;
      const intercepted = retentionIntercept(db, {
        read: (sql, result) => {
          const target =
            boundary === 'snapshot page'
              ? sql.includes('WITH bounds') && sql.includes(' AS eligible')
              : sql.includes('SELECT c.key AS candidate');
          if (!target) return result;
          const row = (result as { results: Record<string, unknown>[] })
            .results[0];
          if (!row) throw new Error('missing capsule projection fixture');
          interceptedReads++;
          if (kind === 'missing') delete row.owned_1;
          if (kind === 'numeric') row.owned_1 = 123;
          if (kind === 'invalid JSON') row.owned_1 = '{';
          return result;
        },
      });
      await expect(
        purgeExpiredWorkflowRuns(intercepted, cycle.options()),
      ).rejects.toThrow();
      expect(interceptedReads).toBe(1);
      expect(remainingRunIds(sqlite)).toEqual(
        boundary === 'absent orphan observation' ? [] : ['run'],
      );
      expect(reservationRows(sqlite)).toHaveLength(1);
      expect(cycle.advances).toHaveLength(boundary === 'snapshot page' ? 0 : 1);
      expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(
        boundary === 'snapshot page' ? 1 : 0,
      );
      expect(reservationRows(sqlite)).toEqual([]);
    });

    it('advances past an explicit null owned capsule projection', async () => {
      const { sqlite, db, cycle } = retentionWorld();
      if (boundary !== 'absent orphan observation')
        retentionSnapshot(sqlite, 'run', {
          token: boundary === 'orphan observation' ? 'S2' : 'S1',
          status: boundary === 'orphan observation' ? 'suspended' : 'success',
        });
      retentionReservation(sqlite, 'key');
      let interceptedReads = 0;
      const intercepted = retentionIntercept(db, {
        read: (sql, result) => {
          const target =
            boundary === 'snapshot page'
              ? sql.includes('WITH bounds') && sql.includes(' AS eligible')
              : sql.includes('SELECT c.key AS candidate');
          if (!target) return result;
          const row = (result as { results: Record<string, unknown>[] })
            .results[0];
          if (!row) throw new Error('missing capsule projection fixture');
          row.owned_1 = null;
          interceptedReads++;
          return result;
        },
      });
      expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
        0,
      );
      expect(interceptedReads).toBe(1);
      expect(cycle.advances).toHaveLength(2);
      expect(cycle.cursor?.snapshots).toBeUndefined();
      expect(cycle.cursor?.reservations).toBeUndefined();
      expect(remainingRunIds(sqlite)).toEqual(
        boundary === 'absent orphan observation' ? [] : ['run'],
      );
      expect(reservationRows(sqlite)).toHaveLength(
        boundary === 'absent orphan observation' ? 0 : 1,
      );
    });
  });

  describe.each([
    'present',
    'absent',
  ] as const)('%s orphan snapshot', (presence) => {
    it.each([
      'workflow_name',
      'run_id',
    ])('rejects a mismatched orphan observation address: %s', async (field) => {
      const { sqlite, db, cycle } = retentionWorld();
      if (presence === 'present')
        retentionSnapshot(sqlite, 'run', { token: 'S2', status: 'suspended' });
      retentionReservation(sqlite, 'key');
      let interceptedReads = 0;
      const intercepted = retentionIntercept(db, {
        read: (sql, result) => {
          if (!sql.includes('SELECT c.key AS candidate')) return result;
          const row = (result as { results: Record<string, unknown>[] })
            .results[0];
          if (!row) throw new Error('missing orphan observation fixture');
          row[field] = 'other';
          interceptedReads++;
          return result;
        },
      });
      await expect(
        purgeExpiredWorkflowRuns(intercepted, cycle.options()),
      ).rejects.toThrow();
      expect(interceptedReads).toBe(1);
      expect(cycle.advances).toHaveLength(1);
      expect(reservationRows(sqlite)).toHaveLength(1);
      expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
      expect(reservationRows(sqlite)).toEqual([]);
    });
  });

  describe.each(['initial page', 'schema retry'] as const)('%s', (boundary) => {
    it.each([
      'missing',
      'numeric',
      'invalid JSON',
      'array',
      'incomplete object',
      'null',
    ])('handles a reservation projection with %s raw', async (kind) => {
      const { sqlite, db, cycle } = retentionWorld();
      retentionReservation(sqlite, 'key');
      let batches = 0;
      let interceptedReads = 0;
      const intercepted = retentionIntercept(db, {
        beforeBatch: () => {
          if (boundary === 'schema retry' && batches++ === 0)
            createSnapshotTable(sqlite, 'late_');
        },
        read: (sql, result) => {
          const target =
            boundary === 'initial page'
              ? sql.includes('WITH bounds') && sql.includes(' AS raw')
              : sql.startsWith('SELECT CASE') && sql.includes(' AS raw');
          if (!target) return result;
          const row = (result as { results: Record<string, unknown>[] })
            .results[0];
          if (!row) throw new Error('missing reservation projection fixture');
          interceptedReads++;
          if (kind === 'missing') delete row.raw;
          if (kind === 'numeric') row.raw = 123;
          if (kind === 'invalid JSON') row.raw = '{';
          if (kind === 'array') row.raw = '[]';
          if (kind === 'incomplete object') row.raw = '{}';
          if (kind === 'null') row.raw = null;
          return result;
        },
      });
      const outcome = purgeExpiredWorkflowRuns(intercepted, cycle.options());
      if (kind === 'null') {
        await expect(outcome).resolves.toBe(0);
        expect(cycle.advances).toHaveLength(2);
        expect(cycle.cursor?.reservations).toBeUndefined();
      } else {
        await expect(outcome).rejects.toThrow();
        expect(cycle.advances).toHaveLength(1);
      }
      expect(interceptedReads).toBe(1);
      if (boundary === 'schema retry') expect(batches).toBeGreaterThan(0);
      expect(reservationRows(sqlite)).toHaveLength(1);
      expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
      expect(reservationRows(sqlite)).toEqual([]);
    });
  });

  it.each([
    'missing-rows',
    'sparse-page',
    'changed-high-water',
    'missing-position',
    'missing-capsule',
    'wrong-eligibility',
  ])('retains page progress on malformed read result %s', async (kind) => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    const intercepted = retentionIntercept(db, {
      read: (sql, result) => {
        if (!sql.includes('WITH bounds')) return result;
        const page = result as { results: Record<string, unknown>[] };
        if (kind === 'missing-rows') return { results: undefined };
        if (kind === 'sparse-page') return { results: new Array(1) };
        const row = page.results[0];
        if (!row) throw new Error('fixture page missing');
        if (kind === 'changed-high-water') row.h = NaN;
        if (kind === 'missing-position') delete row.rid;
        if (kind === 'missing-capsule') delete row.owned_1;
        if (kind === 'wrong-eligibility') row.eligible = '1';
        return page;
      },
    });
    await expect(
      purgeExpiredWorkflowRuns(intercepted, cycle.options()),
    ).rejects.toThrow();
    expect(remainingRunIds(sqlite)).toEqual(['run']);
    expect(cycle.advances).toEqual([]);
  });

  it.each([
    'failed-envelope',
    'missing-raw-field',
    'multiple-rows',
  ])('refuses malformed legacy read %s without advancing', async (kind) => {
    const { sqlite, db, cycle } = retentionWorld();
    seedRun(sqlite, {
      runId: 'run',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    const intercepted = retentionIntercept(db, {
      read: (sql, result) => {
        if (
          !sql.startsWith('SELECT workflow_name, run_id, resourceId, snapshot')
        )
          return result;
        const page = result as {
          results: Record<string, unknown>[];
          success: boolean;
        };
        if (kind === 'failed-envelope') page.success = false;
        if (kind === 'missing-raw-field' && page.results[0])
          delete page.results[0].snapshot;
        if (kind === 'multiple-rows' && page.results[0])
          page.results.push(page.results[0]);
        return page;
      },
    });
    await expect(
      purgeExpiredWorkflowRuns(intercepted, cycle.options()),
    ).rejects.toThrow();
    expect(remainingRunIds(sqlite)).toEqual(['run']);
    expect(cycle.advances).toEqual([]);
  });

  it('retains the reservation phase position after an orphan response is lost', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionReservation(sqlite, 'key');
    const intercepted = retentionIntercept(db, {
      afterBatch: () => {
        throw new Error('orphan response lost');
      },
    });
    await expect(
      purgeExpiredWorkflowRuns(intercepted, cycle.options()),
    ).rejects.toThrow('orphan response lost');
    expect(cycle.advances).toHaveLength(1);
    expect(reservationRows(sqlite)).toEqual([]);
    await purgeExpiredWorkflowRuns(db, cycle.options());
    expect(cycle.advances).toHaveLength(3);
  });

  it('does not retry an arbitrary missing-table message when its schema is unchanged', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    const beforeBatch = vi.fn(() => {
      throw new Error('no such table mastra_workflow_snapshot');
    });
    await expect(
      purgeExpiredWorkflowRuns(
        retentionIntercept(db, { beforeBatch }),
        cycle.options(),
      ),
    ).rejects.toThrow('no such table');
    expect(beforeBatch).toHaveBeenCalledTimes(1);
    expect(remainingRunIds(sqlite)).toEqual(['run']);
    expect(cycle.advances).toEqual([]);
  });

  it('preserves an agent capsule changed from boolean false to numeric zero', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    const provenance = {
      version: 2,
      startToken: 'S1',
      startIdentity: {
        owner: { kind: 'human', id: 'initiator' },
        target: { kind: 'agent', id: 'agent', threadId: 'thread' },
      },
      agentStart: { threaded: false },
    };
    decodeRunStartIdentity(provenance);
    retentionSnapshot(sqlite, 'run', { provenance });
    retentionReservation(sqlite, 'key', {
      state: 'started',
      target_kind: 'agent',
      target_id: 'agent',
      thread_id: 'thread',
    });
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        replaceRetentionSnapshot(sqlite, {
          status: 'success',
          requestContext: {
            'flowsafe.runProvenance': {
              ...provenance,
              agentStart: { threaded: 0 },
            },
          },
        });
      },
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      0,
    );
    expect(reservationRows(sqlite)[0]?.state).toBe('started');
    replaceRetentionSnapshot(sqlite, {
      status: 'success',
      requestContext: { 'flowsafe.runProvenance': provenance },
    });
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(1);
    expect(reservationRows(sqlite)[0]?.state).toBe('terminal');
  });

  it('keeps same-S orphan identity conservative after an exact current address insertion', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionReservation(sqlite, 'key');
    const intercepted = retentionIntercept(db, {
      beforeBatch: () =>
        retentionSnapshot(sqlite, 'run', { status: 'suspended' }),
    });
    await purgeExpiredWorkflowRuns(intercepted, cycle.options());
    expect(reservationRows(sqlite)).toHaveLength(1);
    expect(remainingRunIds(sqlite)).toEqual(['run']);
  });

  it('uses BINARY owner membership under NOCASE resource IDs', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    await createResourceOwnershipSchema(db as never);
    sqlite.exec(
      `ALTER TABLE ${RESOURCE_OWNERSHIP_TABLE} RENAME TO original_owners`,
    );
    const schema = sqlite
      .prepare("SELECT sql FROM sqlite_schema WHERE name='original_owners'")
      .get() as { sql: string };
    sqlite.exec(
      schema.sql
        .replace('"original_owners"', RESOURCE_OWNERSHIP_TABLE)
        .replaceAll('TEXT', 'TEXT COLLATE NOCASE'),
    );
    const resources = new D1ResourceOwnershipStore(db as never);
    await resources.claim('run', 'run', { kind: 'human', id: 'owner' });
    retentionSnapshot(sqlite, 'run');
    retentionSnapshot(sqlite, 'RUN', { status: 'suspended' });
    expect(
      await purgeExpiredWorkflowRuns(db, {
        ...cycle.options(),
        resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      }),
    ).toBe(1);
    expect(
      sqlite
        .prepare(`SELECT resource_id FROM ${RESOURCE_OWNERSHIP_TABLE}`)
        .all(),
    ).toEqual([]);
    expect(remainingRunIds(sqlite)).toEqual(['RUN']);
  });

  it('preserves escaped token bytes when decoding yields the same value', async () => {
    const { sqlite, db, cycle } = retentionWorld();
    retentionSnapshot(sqlite, 'run');
    const intercepted = retentionIntercept(db, {
      beforeBatch: () => {
        const snapshot = currentRetentionSnapshot(sqlite);
        replaceRetentionSnapshot(
          sqlite,
          JSON.stringify(snapshot).replace('"S1"', '"\\u00531"'),
        );
      },
    });
    expect(await purgeExpiredWorkflowRuns(intercepted, cycle.options())).toBe(
      0,
    );
    expect(remainingRunIds(sqlite)).toEqual(['run']);
  });

  it('honors zero TTL without deleting an exact-cutoff snapshot', async () => {
    const { sqlite, db } = retentionWorld();
    seedRun(sqlite, { runId: 'before', status: 'success', updatedAt: NOW - 1 });
    seedRun(sqlite, { runId: 'at', status: 'success', updatedAt: NOW });
    const cycle = retentionCycle({ ttlMs: 0 });
    expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual(['at']);
  });
});

it('retains a same-generation orphan key when its current token uses JSON escapes', async () => {
  const { sqlite, db, cycle } = retentionWorld();
  retentionSnapshot(sqlite, 'run', { status: 'suspended' });
  retentionReservation(sqlite, 'key');
  const snapshot = currentRetentionSnapshot(sqlite);
  replaceRetentionSnapshot(
    sqlite,
    JSON.stringify(snapshot).replace('"S1"', '"\\u00531"'),
  );
  expect(await purgeExpiredWorkflowRuns(db, cycle.options())).toBe(0);
  expect(reservationRows(sqlite)).toHaveLength(1);
  expect(remainingRunIds(sqlite)).toEqual(['run']);
});
