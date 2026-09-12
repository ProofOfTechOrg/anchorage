// SPDX-License-Identifier: Apache-2.0

import type { DurableObjectState } from '@cloudflare/workers-types';
import {
  type AgentThreadInstanceScope,
  createThreadAgentHost,
} from '@proofoftech/flowsafe/agent-host';
import { seedDeploymentIdentity } from '@proofoftech/flowsafe/do-runner';
import { describe, expect, it, vi } from 'vitest';

import {
  discardStarterScheduleDispatch,
  idleRunScheduleDispatch,
  StarterThread,
} from '../src/durable-objects.js';
import { executionFence, schedulesStore } from '../src/storage.js';

vi.mock('@proofoftech/flowsafe/agent-host', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('@proofoftech/flowsafe/agent-host')>();
  return {
    ...actual,
    createThreadAgentHost: vi.fn(actual.createThreadAgentHost),
  };
});

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

describe('starter run lifecycle wiring', () => {
  it('FS8 D3 host activation passes the verified instance to cold alarm recovery', async () => {
    const db = sqliteUnitDatabase(openSqlite()) as Env['DB'];
    await seedDeploymentIdentity(db, 'acme', 'open');
    const values = new Map<string, unknown>();
    let alarm: number | null = null;
    const state = {
      id: { name: 'starter-cold-thread' },
      storage: {
        async get<T>(key: string): Promise<T | undefined> {
          return values.get(key) as T | undefined;
        },
        async put(key: string, value: unknown) {
          values.set(key, structuredClone(value));
        },
        async delete(key: string) {
          return values.delete(key);
        },
        async list({ prefix }: { prefix: string }) {
          return new Map([...values].filter(([key]) => key.startsWith(prefix)));
        },
        async getAlarm() {
          return alarm;
        },
        async setAlarm(at: number | Date) {
          alarm = at instanceof Date ? at.getTime() : at;
        },
        async deleteAlarm() {
          alarm = null;
        },
      },
    } as unknown as DurableObjectState;
    const env = {
      DB: db,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET: 'starter-alarm-identity-secret-0001',
    } as Env;
    const createHost = vi.mocked(createThreadAgentHost);
    const actualCreate = createHost.getMockImplementation();
    if (!actualCreate) throw new Error('actual host factory is missing');
    const scopes: AgentThreadInstanceScope[] = [];
    createHost.mockImplementation((options) => {
      const host = actualCreate(options);
      const recover = host.recoverOwnership;
      vi.spyOn(host, 'recoverOwnership').mockImplementation((scope) => {
        scopes.push(scope);
        return recover.call(host, scope);
      });
      return host;
    });
    try {
      await new StarterThread(state, env).alarm();
      expect(scopes).toHaveLength(1);
      expect(scopes[0]).toMatchObject({
        threadId: 'starter-cold-thread',
        deploymentTag: 'acme',
      });
      expect(scopes[0]?.init.runtime.executionFence).toBe(executionFence(db));
      expect(scopes[0]).not.toHaveProperty('principal');
      expect(values.size).toBe(0);
      expect(alarm).toBeNull();
    } finally {
      createHost.mockImplementation(actualCreate);
    }
  });

  it('marks only a fully identified idle-run dispatch as lease-held', () => {
    expect(
      idleRunScheduleDispatch({
        scheduleId: 'schedule_lifecycle',
        dispatchId: 'dispatch_lifecycle',
      }),
    ).toEqual({
      scheduleId: 'schedule_lifecycle',
      dispatchId: 'dispatch_lifecycle',
      scheduleDispatchLease: 'executing',
    });
    expect(
      idleRunScheduleDispatch({ scheduleId: 'schedule_lifecycle' }),
    ).toEqual({ scheduleId: 'schedule_lifecycle' });
    expect(
      idleRunScheduleDispatch({ dispatchId: 'dispatch_lifecycle' }),
    ).toEqual({ dispatchId: 'dispatch_lifecycle' });
    expect(idleRunScheduleDispatch({})).toEqual({});
  });

  it('settles a live agent dispatch as discarded and replays the receipt', async () => {
    const db = sqliteUnitDatabase(openSqlite()) as Env['DB'];
    const env = { DB: db } as Env;
    const store = schedulesStore(db);
    const scheduleId = 'schedule_lifecycle';
    const dispatchId = 'dispatch_lifecycle';
    const runId = 'run_lifecycle';

    await store.createSchedule({
      id: scheduleId,
      target: {
        type: 'agent',
        agentId: 'starter-agent',
        prompt: 'scheduled lifecycle test',
      },
      cron: '* * * * *',
      status: 'active',
      nextFireAt: 1_750_000_000_000,
      createdAt: 1_750_000_000_000,
      updatedAt: 1_750_000_000_000,
      metadata: {},
    });
    await store.recordTrigger({
      id: dispatchId,
      scheduleId,
      runId,
      scheduledFireAt: 1_750_000_000_000,
      actualFireAt: 1_750_000_000_000,
      outcome: 'deferred',
      metadata: {
        dispatchState: 'prepared',
        dispatchRef: {
          scheduleId,
          dispatchId,
          runId,
          target: 'agent',
          mode: 'start',
          agentId: 'starter-agent',
        },
      },
    });
    await expect(
      store.beginAgentScheduleDispatch(scheduleId, dispatchId),
    ).resolves.toEqual({ state: 'ready' });

    await discardStarterScheduleDispatch(env, scheduleId, dispatchId, runId);

    const settled = {
      state: 'settled',
      receipt: { action: 'discard', outcome: 'discarded', runId },
    } as const;
    await expect(
      store.agentScheduleDispatchState(scheduleId, dispatchId),
    ).resolves.toEqual(settled);
    await expect(
      store.beginAgentScheduleDispatch(scheduleId, dispatchId),
    ).resolves.toEqual(settled);
  });
});
