// SPDX-License-Identifier: Apache-2.0
// The composition proof the unit tests cannot make. flowsafe pins what a fenced
// tick DOES; this pins that THIS HOST wired one — the failure the required
// `executionFence` option exists to prevent is a deployment where the runtime is
// fenced and the tick is not, and every flowsafe test would still pass.
//
// The loss is silent and total: an unfenced tick claims a due fire through the
// schedules CAS, which advances `nextFireAt`, and the fenced runtime then
// refuses the start. The fire is consumed and never runs, and nothing reports
// it — which is why the assertion below is on the schedule ROW, not on the
// tally the pass returned.

import { ExecutionFenceStore } from '@proofoftech/flowsafe/do-runner';
import { describe, expect, it } from 'vitest';

import { starterMaintenanceTick } from '../src/maintenance.js';
import { schedulesStore } from '../src/storage.js';

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
      const outcome = db.prepare(sql).run(...params) as {
        changes?: number | bigint;
      };
      return {
        success: true,
        meta: { changes: Number(outcome?.changes ?? 0) },
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
  it('leaves a due schedule row untouched while the deployment is migration-locked', async () => {
    // #given — this host's own tick over a database whose fence is locked. The
    // fence store is built from the SAME binding the tick's own
    // `executionFence(env.DB)` resolves, which is the wiring under test: a tick
    // pointed at another database would read `open` here and claim.
    const db = sqliteUnitDatabase(openSqlite()) as Env['DB'];
    const env = starterEnv(db);
    const fence = new ExecutionFenceStore(db);
    await fence.seed('migration-locked');

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
    const before = await store.getSchedule(due.id);

    // #when — the cron duty runs, exactly as the maintenance Durable Object
    // invokes it.
    await starterMaintenanceTick(env)();

    // #then — the row is byte-identical. Nothing was claimed, so the fire is
    // still due and the deployment taking over will run it.
    await expect(store.getSchedule(due.id)).resolves.toEqual(before);
    await expect(store.listDueSchedules(NOW, 10)).resolves.toHaveLength(1);
    await expect(store.listTriggers(due.id)).resolves.toEqual([]);
  });
});
