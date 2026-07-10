// D1RateLimitStore against REAL SQLite via node:sqlite (D1 is SQLite), so the
// atomic UPSERT ... RETURNING count is exercised for real, not mocked. The
// openSqlite()/d1Like() fixture is copied from d1-idempotency-store.test.ts
// on purpose: breakwater must not import across packages for tests.

import { describe, expect, it } from 'vitest';

import {
  D1RateLimitStore,
  type RateLimitDatabase,
  type RateLimitStatement,
} from './d1-rate-limit-store.js';

// --- node:sqlite -> RateLimitDatabase adapter -------------------------------

interface SqliteStatement {
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): unknown;
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
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
  function statement(sql: string, params: unknown[]): RateLimitStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async <T>() =>
        (db.prepare(sql).get(...params) as T | undefined) ?? null,
      run: async () => db.prepare(sql).run(...params),
    };
  }
  return { prepare: (sql: string) => statement(sql, []) };
}

const T0 = Date.parse('2026-07-07T10:00:00.000Z');
const MINUTE = 60_000;

describe('D1RateLimitStore', () => {
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
