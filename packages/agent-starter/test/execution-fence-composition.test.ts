// SPDX-License-Identifier: Apache-2.0

import type { WorkflowRunState } from '@mastra/core/workflows';
import { humanPrincipal } from '@proofoftech/flowsafe/approval-api';
import {
  createD1Storage,
  ExecutionFenceStore,
  StartIdempotencyStore,
} from '@proofoftech/flowsafe/do-runner';
import { approvalStoreFactoryFor } from '@proofoftech/flowsafe/host-kit';
import {
  D1SchedulesStorage,
  FENCED_SCHEDULE_STORAGE,
} from '@proofoftech/flowsafe/schedules';
import { describe, expect, it } from 'vitest';

import { starterMaintenanceTick } from '../src/maintenance.js';
import { contextForPrincipal } from '../src/principal-context.js';
import { createComposedStorage, schedulesStore } from '../src/storage.js';

interface SqliteStatement {
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
}

function openSqlite(): SqliteDatabase {
  const getBuiltin = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!getBuiltin) {
    throw new Error('node:sqlite unavailable; tests require Node.js 22.13+');
  }
  const mod = getBuiltin('node:sqlite') as {
    DatabaseSync: new (path: string) => SqliteDatabase;
  };
  return new mod.DatabaseSync(':memory:');
}

function sqliteUnitDatabase(db: SqliteDatabase): unknown {
  const runSync = Symbol('runSync');

  function statement(sql: string, params: unknown[]): Record<string, unknown> {
    const execute = () => {
      const results = db.prepare(sql).all(...params);
      const outcome = db.prepare('SELECT changes() AS count').get() as {
        count: number | bigint;
      };
      return {
        success: true,
        results,
        meta: { changes: Number(outcome.count) },
      };
    };
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async (column?: string) => {
        const row = db.prepare(sql).get(...params) as
          | Record<string, unknown>
          | undefined;
        if (row === undefined) return null;
        return column === undefined ? row : (row[column] ?? null);
      },
      run: async () => execute(),
      [runSync]: execute,
      all: async () => ({
        success: true,
        results: db.prepare(sql).all(...params),
        meta: {},
      }),
    };
  }

  return {
    prepare: (sql: string) => statement(sql, []),
    batch: async (
      statements: Array<{
        run: () => Promise<unknown>;
        [runSync]?: () => unknown;
      }>,
    ) => {
      db.exec('BEGIN IMMEDIATE');
      try {
        const results = [];
        for (const prepared of statements) {
          results.push(
            prepared[runSync] ? prepared[runSync]() : await prepared.run(),
          );
        }
        db.exec('COMMIT');
        return results;
      } catch (error) {
        db.exec('ROLLBACK');
        throw error;
      }
    },
  };
}

const NOW = 1_750_000_000_000;

/**
 * A namespace stub. The tick builds its run/thread topologies eagerly, but a
 * fenced pass never addresses one — reaching this is the failure.
 */
function namespace(): Env['RUNNER'] {
  const unreachable = () => {
    throw new Error(
      'a fenced maintenance pass addressed a Durable Object — it claimed work',
    );
  };
  return {
    idFromName: unreachable,
    idFromString: unreachable,
    newUniqueId: unreachable,
    get: unreachable,
  } as unknown as Env['RUNNER'];
}

function starterEnv(db: Env['DB']): Env {
  return {
    DB: db,
    DEPLOYMENT_TENANT: 'acme',
    DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
    RUNNER: namespace(),
    THREAD: namespace(),
  } as unknown as Env;
}

describe('starter maintenance tick and the deployment execution fence', () => {
  it('uses the original database for direct and composed schedule capabilities', async () => {
    const db = sqliteUnitDatabase(openSqlite()) as Env['DB'];
    const direct = schedulesStore(db);
    expect(direct[FENCED_SCHEDULE_STORAGE]?.database).toBe(db);
    const storage = createComposedStorage(db);
    await storage.init();
    const domain = await storage.getStore('schedules');
    expect(domain).toBeInstanceOf(D1SchedulesStorage);
    if (!(domain instanceof D1SchedulesStorage))
      throw new Error('composed schedule domain is missing');
    expect(domain[FENCED_SCHEDULE_STORAGE]?.database).toBe(db);
  });

  it('leaves a due schedule row untouched while the deployment is migration-locked', async () => {
    const db = sqliteUnitDatabase(openSqlite()) as Env['DB'];
    const env = starterEnv(db);
    const fence = new ExecutionFenceStore(db);
    await fence.seed('open');

    const store = schedulesStore(db);
    const due = {
      id: 'schedule_fenced',
      target: {
        type: 'workflow' as const,
        workflowId: 'starter-echo',
        inputData: { topic: 'x' },
      },
      cron: '* * * * *',
      status: 'active' as const,
      nextFireAt: NOW - 1_000,
      createdAt: NOW,
      updatedAt: NOW,
      metadata: {},
    };
    await store.createSchedule(due);
    await fence.transition({ expected: 'open', next: 'draining' });
    await fence.transition({
      expected: 'draining',
      next: 'migration-locked',
    });
    const before = await store.getSchedule(due.id);

    await starterMaintenanceTick(env)();

    await expect(store.getSchedule(due.id)).resolves.toEqual(before);
    await expect(store.listDueSchedules(NOW, 10)).resolves.toHaveLength(1);
    await expect(store.listTriggers(due.id)).resolves.toEqual([]);
  });
});

describe('FS8 D3 proof activation in the starter', () => {
  it.each([
    '',
    'other_',
  ])('decides only the configured default workflow namespace when proof is %s', async (proofPrefix) => {
    const db = sqliteUnitDatabase(openSqlite()) as Env['DB'];
    const env = { DB: db, DEPLOYMENT_TENANT: 'acme' } as Env;
    const fence = new ExecutionFenceStore(db);
    const owner = { kind: 'human' as const, id: 'requester' };
    const workflowId = 'starter-proof';
    const runId = 'starter-proof-run';
    for (const prefix of new Set(['', proofPrefix])) {
      const storage = createD1Storage({ binding: db, tablePrefix: prefix });
      await storage.init();
      const workflows = await storage.getStore('workflows');
      if (!workflows) throw new Error('workflow fixture storage is missing');
      const snapshot: WorkflowRunState = {
        runId,
        status: 'suspended',
        value: {},
        context: {},
        serializedStepGraph: [],
        activePaths: [],
        activeStepsPath: {},
        suspendedPaths: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: 1_700_000_000_000,
        requestContext: {
          'flowsafe.runProvenance': {
            version: 2,
            startToken: `generation-${prefix || 'default'}`,
            attemptToken: 'leg',
            resumeCounts: [],
            startIdentity: {
              owner,
              target: { kind: 'workflow', id: workflowId },
            },
          },
        },
      };
      await workflows.persistWorkflowSnapshot({
        workflowName: workflowId,
        runId,
        snapshot,
      });
    }
    const current = await fence.readCurrentRunExecution({
      tablePrefix: proofPrefix,
      workflowId,
      runId,
    });
    if (!current) throw new Error('proof fixture generation is missing');
    const execution = {
      ...current,
      owner,
      target: { kind: 'workflow' as const, id: workflowId },
    };
    const reservations = new StartIdempotencyStore(db);
    const reserved = await reservations.reserve({
      key: 'starter-proof-key',
      owner,
      targetKind: 'workflow',
      targetId: workflowId,
      mintRunId: () => runId,
    });
    const bound = await reservations.associateReservation(
      reserved.reservation,
      execution,
    );
    await fence.transition({
      expected: 'open',
      next: 'proof-only',
      proofKey: reserved.reservation.key,
    });
    const reading = await fence.read();
    expect(
      await fence.rebindProofRun({
        reservation: bound,
        execution,
        proof: {
          key: bound.key,
          mutationEpoch: reading.mutationEpoch,
          transitionRevision: reading.transitionRevision,
        },
        reservationStore: reservations,
      }),
    ).toBe(true);
    const store = approvalStoreFactoryFor(db).store();
    const at = new Date(1_700_000_000_000).toISOString();
    await store.create({
      id: 'starter-proof-approval',
      workflowId,
      runId,
      title: 'Approve the selected generation',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      requestedBy: owner.id,
      requestedByKind: owner.kind,
      createdAt: at,
      updatedAt: at,
    });
    const reviewer = { id: 'reviewer', role: 'reviewer' as const };
    const context = contextForPrincipal(env, humanPrincipal(reviewer));
    const decision = context
      .service()
      .decide('starter-proof-approval', { decision: 'approve' }, reviewer);
    if (proofPrefix === '') {
      await expect(decision).resolves.toMatchObject({
        record: { status: 'approved' },
      });
    } else {
      await expect(decision).rejects.toMatchObject({
        reason: { code: 'EXECUTION_FENCED' },
      });
    }
    expect((await store.get('starter-proof-approval'))?.status).toBe(
      proofPrefix === '' ? 'approved' : 'pending',
    );
  });
});
