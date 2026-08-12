// SPDX-License-Identifier: Apache-2.0
// D1RateLimitStore against node:sqlite as a fast SQL facsimile. These are
// non-fidelity unit tests: the dedicated worker project exercises the atomic
// UPSERT ... RETURNING count against D1 inside workerd. The
// openSqlite()/d1Like() fixture is copied from d1-idempotency-store.test.ts
// on purpose: breakwater must not import across packages for tests.

import type { ToolExecutionContext } from '@mastra/core/tools';
import { describe, expect, it, vi } from 'vitest';

import { AuditLogger } from '../audit/index.js';
import {
  D1RateLimitStore,
  type RateLimitDatabase,
  type RateLimitStatement,
} from './d1-rate-limit-store.js';
import { createConnector } from './index.js';

// --- node:sqlite -> RateLimitDatabase adapter -------------------------------

interface SqliteStatement {
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): unknown;
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
}

// process.getBuiltinModule loads the builtin without import machinery, so
// neither vite's resolver (which cannot resolve node:sqlite) nor the
// tsconfig (no @types/node) ever sees the specifier. Available since node
// 22.3; node:sqlite itself is unflagged since 22.13.
function openSqlite(): SqliteDatabase {
  const getBuiltin = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!getBuiltin) {
    throw new Error('node:sqlite unavailable — tests require node >= 22.13');
  }
  const mod = getBuiltin('node:sqlite') as {
    DatabaseSync: new (path: string) => SqliteDatabase;
  };
  return new mod.DatabaseSync(':memory:');
}

function d1Like(db: SqliteDatabase): RateLimitDatabase {
  const prepared = new WeakMap<
    RateLimitStatement,
    { sql: string; params: unknown[] }
  >();
  function statement(sql: string, params: unknown[]): RateLimitStatement {
    const value: RateLimitStatement = {
      bind: (...values: unknown[]) => statement(sql, values),
      run: async () => db.prepare(sql).run(...params),
    };
    prepared.set(value, { sql, params });
    return value;
  }
  return {
    prepare: (sql: string) => statement(sql, []),
    batch: async <T>(statements: RateLimitStatement[]) => {
      db.exec('BEGIN');
      try {
        const results = statements.map((value) => {
          const entry = prepared.get(value);
          if (!entry) throw new Error('unknown prepared statement');
          if (/\bRETURNING\b/i.test(entry.sql)) {
            const row = db.prepare(entry.sql).get(...entry.params) as
              | T
              | undefined;
            return { results: row === undefined ? [] : [row] };
          }
          db.prepare(entry.sql).run(...entry.params);
          return { results: [] };
        });
        db.exec('COMMIT');
        return results;
      } catch (error) {
        db.exec('ROLLBACK');
        throw error;
      }
    },
  };
}

const T0 = Date.parse('2026-07-07T10:00:00.000Z');
const MINUTE = 60_000;

async function runConnector(tool: {
  execute?: (
    inputData: unknown,
    context: ToolExecutionContext,
  ) => Promise<unknown>;
}): Promise<unknown> {
  if (!tool.execute) throw new Error('tool has no execute');
  return tool.execute({}, {} as ToolExecutionContext);
}

describe('D1RateLimitStore (Node SQLite facsimile)', () => {
  it('counts calls within one fixed window', async () => {
    // #given
    const store = new D1RateLimitStore(d1Like(openSqlite()));

    // #when / #then — post-increment counts, matching InMemoryRateLimitStore
    expect(await store.increment('acme:crm', MINUTE, T0)).toBe(1);
    expect(await store.increment('acme:crm', MINUTE, T0 + 1_000)).toBe(2);
    expect(await store.increment('acme:crm', MINUTE, T0 + 59_999)).toBe(3);
  });

  it('shares one window across store instances over one database — the cross-isolate property', async () => {
    // #given — two "isolates" (two DO instances of one tenant's runs)
    const db = d1Like(openSqlite());
    const a = new D1RateLimitStore(db);
    const b = new D1RateLimitStore(db);

    // #when — each isolate spends from the same budget key
    await a.increment('acme:crm', MINUTE, T0);
    await b.increment('acme:crm', MINUTE, T0 + 1);

    // #then — the third call sees the SHARED count, not a per-isolate 1
    expect(await a.increment('acme:crm', MINUTE, T0 + 2)).toBe(3);
  });

  it('opens a fresh window on rollover and reaps the expired one', async () => {
    // #given — a spent window
    const sqlite = openSqlite();
    const store = new D1RateLimitStore(d1Like(sqlite));
    await store.increment('acme:crm', MINUTE, T0);
    await store.increment('acme:crm', MINUTE, T0 + 1_000);

    // #when — the next minute starts
    const next = await store.increment('acme:crm', MINUTE, T0 + MINUTE);

    // #then — count resets, and the old window's row was deleted
    expect(next).toBe(1);
    const rows = sqlite
      .prepare('SELECT COUNT(*) AS n FROM breakwater_rate_limit')
      .get() as { n: number };
    expect(rows.n).toBe(1);
  });

  it('rolls back the increment when expired-window cleanup fails', async () => {
    const sqlite = openSqlite();
    const store = new D1RateLimitStore(d1Like(sqlite));
    await store.increment('acme:crm', MINUTE, T0);
    sqlite.exec(
      `CREATE TRIGGER fail_rate_cleanup
       BEFORE DELETE ON breakwater_rate_limit
       BEGIN
         SELECT RAISE(ABORT, 'injected cleanup failure');
       END`,
    );

    await expect(
      store.increment('acme:crm', MINUTE, T0 + MINUTE),
    ).rejects.toThrow('injected cleanup failure');
    expect(
      sqlite
        .prepare(
          `SELECT window_start, count FROM breakwater_rate_limit
           WHERE budget_key = ? ORDER BY window_start`,
        )
        .get('acme:crm'),
    ).toEqual({ window_start: T0, count: 1 });

    sqlite.exec('DROP TRIGGER fail_rate_cleanup');
    expect(await store.increment('acme:crm', MINUTE, T0 + MINUTE)).toBe(1);
    expect(await store.increment('acme:crm', MINUTE, T0 + MINUTE + 1)).toBe(2);
  });

  it('rejects without execution or quota spend when cleanup fails, then retries at count one', async () => {
    vi.useFakeTimers();
    try {
      const sqlite = openSqlite();
      const rateLimitStore = new D1RateLimitStore(d1Like(sqlite));
      const audit = new AuditLogger();
      const execute = vi.fn(async () => ({ ok: true }));
      const tool = createConnector({
        id: 'crm.create',
        description: 'Create one CRM record',
        permissions: { sideEffect: 'write', rateLimit: '1/min' },
        policies: { audit, rateLimitStore },
        execute,
      });
      await rateLimitStore.increment('crm.create', MINUTE, T0);
      sqlite.exec(
        `CREATE TRIGGER fail_connector_rate_cleanup
         BEFORE DELETE ON breakwater_rate_limit
         BEGIN
           SELECT RAISE(ABORT, 'injected cleanup failure');
         END`,
      );
      vi.setSystemTime(T0 + MINUTE);

      await expect(runConnector(tool)).rejects.toThrow(
        'injected cleanup failure',
      );
      expect(execute).not.toHaveBeenCalled();
      expect(
        sqlite
          .prepare(
            `SELECT window_start, count FROM breakwater_rate_limit
             WHERE budget_key = ? ORDER BY window_start`,
          )
          .get('crm.create'),
      ).toEqual({ window_start: T0, count: 1 });
      expect(audit.events()).toMatchObject([
        {
          decision: 'error',
          reason: 'rate-limit store increment failed',
          detail: { stage: 'rate-limit-store' },
        },
      ]);

      sqlite.exec('DROP TRIGGER fail_connector_rate_cleanup');
      await expect(runConnector(tool)).resolves.toEqual({ ok: true });
      expect(execute).toHaveBeenCalledTimes(1);
      expect(
        sqlite
          .prepare(
            `SELECT window_start, count FROM breakwater_rate_limit
             WHERE budget_key = ?`,
          )
          .get('crm.create'),
      ).toEqual({ window_start: T0 + MINUTE, count: 1 });

      await expect(runConnector(tool)).rejects.toMatchObject({
        policy: 'rate-limit',
      });
      expect(execute).toHaveBeenCalledTimes(1);
      expect(audit.events()).toMatchObject([
        { decision: 'error', detail: { stage: 'rate-limit-store' } },
        { decision: 'allowed' },
        { decision: 'denied', detail: { policy: 'rate-limit' } },
      ]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('epoch-aligns windows so every caller computes the same boundary', async () => {
    // #given — two calls inside the same epoch-aligned minute
    const store = new D1RateLimitStore(d1Like(openSqlite()));
    const midWindow = T0 + 30_000;

    // #when / #then — same bucket regardless of the first caller's offset
    expect(await store.increment('k', MINUTE, midWindow)).toBe(1);
    expect(await store.increment('k', MINUTE, T0 + 59_000)).toBe(2);
  });

  it('keeps budget keys independent', async () => {
    // #given — two tenants' scoped keys (the wrapper's `${scope}:${id}`)
    const store = new D1RateLimitStore(d1Like(openSqlite()));

    // #when
    await store.increment('acme:crm', MINUTE, T0);
    await store.increment('acme:crm', MINUTE, T0);

    // #then — tenant B's budget is untouched by tenant A's spend
    expect(await store.increment('globex:crm', MINUTE, T0)).toBe(1);
  });

  it('respects a custom table name', async () => {
    // #given — two stores, distinct tables, one database
    const db = d1Like(openSqlite());
    const a = new D1RateLimitStore(db, { table: 'rate_a' });
    const b = new D1RateLimitStore(db, { table: 'rate_b' });

    // #when — the same key counts independently per table
    await a.increment('k', MINUTE, T0);

    // #then
    expect(await b.increment('k', MINUTE, T0)).toBe(1);
  });

  it('retries schema creation after a failed first attempt', async () => {
    // #given — a database that fails the first statement, then recovers
    const sqlite = openSqlite();
    const real = d1Like(sqlite);
    let failures = 1;
    const flaky: RateLimitDatabase = {
      prepare: (sql: string) => {
        if (failures > 0 && sql.startsWith('CREATE TABLE')) {
          failures -= 1;
          return {
            bind: () => flaky.prepare(sql),
            first: async () => {
              throw new Error('transient D1 error');
            },
            run: async () => {
              throw new Error('transient D1 error');
            },
          };
        }
        return real.prepare(sql);
      },
      batch: (statements) => real.batch(statements),
    };
    const store = new D1RateLimitStore(flaky);

    // #when — the first call fails on schema creation
    await expect(store.increment('k', MINUTE, T0)).rejects.toThrow(
      'transient D1 error',
    );

    // #then — the memo cleared; the retry creates the schema and counts
    expect(await store.increment('k', MINUTE, T0)).toBe(1);
  });
});
