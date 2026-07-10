// Shared node:sqlite test fixture — ONE home for the openSqlite() probe and
// the D1-shaped adapter that six suites previously carried as byte-copies
// (any change to the Mastra D1 shape had to be edited in lockstep across
// them). Lives OUTSIDE src/ on purpose: tests import it relatively, and the
// build tsconfig (rootDir src) never compiles it into dist. flowsafe-only:
// breakwater keeps its own copy by its documented no-cross-package-test-
// imports rule.

export interface SqliteStatement {
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

export interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
}

// process.getBuiltinModule loads the builtin without import machinery, so
// neither vite's resolver (which cannot resolve node:sqlite) nor the
// workers-types tsconfig (no @types/node) ever sees the specifier. Available
// since node 22.3; node:sqlite itself is unflagged since 22.13.
export function openSqlite(): SqliteDatabase {
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

/**
 * A full D1Database-shaped adapter over node:sqlite — faithful enough for the
 * REAL @mastra/cloudflare-d1 D1Store and the worker templates to run against
 * (first/run/all/raw/exec/batch/dump, D1's { success, meta } envelopes).
 * Returns unknown: callers cast to the D1Database their seam needs.
 */
export function d1DatabaseLike(db: SqliteDatabase): unknown {
  function statement(sql: string, params: unknown[]): Record<string, unknown> {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async (column?: string) => {
        const row = db.prepare(sql).get(...params) as
          | Record<string, unknown>
          | undefined;
        if (row === undefined) return null;
        return column !== undefined ? (row[column] ?? null) : row;
      },
      run: async () => {
        const outcome = db.prepare(sql).run(...params) as {
          changes?: number | bigint;
        };
        return {
          success: true,
          meta: { changes: Number(outcome?.changes ?? 0) },
        };
      },
      all: async () => ({
        success: true,
        results: db.prepare(sql).all(...params),
        meta: {},
      }),
      raw: async () => {
        const rows = db.prepare(sql).all(...params) as Array<
          Record<string, unknown>
        >;
        return rows.map((row) => Object.values(row));
      },
    };
  }
  return {
    prepare: (sql: string) => statement(sql, []),
    exec: async (sql: string) => {
      db.exec(sql);
      return { count: 1, duration: 0 };
    },
    batch: async (statements: Array<{ run: () => Promise<unknown> }>) => {
      const results = [];
      for (const stmt of statements) results.push(await stmt.run());
      return results;
    },
    dump: async () => new ArrayBuffer(0),
  };
}
