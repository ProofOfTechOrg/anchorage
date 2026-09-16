// Shared node:sqlite test fixture for fast, deterministic SQL unit coverage.
// It is not D1, workerd, transaction-concurrency, or Worker-runtime evidence.
// Runtime and concurrency claims live in the Workers pool and Wrangler harness.

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

/** The rows and write metadata one statement resolves, in D1's envelope. */
export interface SqliteUnitResult<T = unknown> {
  success: boolean;
  results: T[];
  meta: { changes?: number };
}

/**
 * The prepared statement the facade hands a caller. Method syntax and the row
 * type parameter are the shape the structural D1 subsets of this package
 * declare (src/signals/d1-shared.ts, src/do-runner/workflow-snapshot-row.ts);
 * src/do-runner/sqlite-fixture.test.ts pins the facade against them.
 */
export interface SqliteUnitStatement {
  bind(...values: unknown[]): SqliteUnitStatement;
  first<T = unknown>(column?: string): Promise<T | null>;
  run<T = unknown>(): Promise<SqliteUnitResult<T>>;
  all<T = unknown>(): Promise<SqliteUnitResult<T>>;
}

/** The database surface a SQL unit test drives. */
export interface SqliteUnitDatabase {
  prepare(sql: string): SqliteUnitStatement;
  batch(statements: SqliteUnitStatement[]): Promise<SqliteUnitResult[]>;
}

/** A narrow prepared-statement facade for SQL unit tests only. */
export function sqliteUnitDatabase(db: SqliteDatabase): SqliteUnitDatabase {
  const runSync = Symbol('runSync');
  // The synchronous seam `batch` runs a statement of this fixture's through.
  // It stays off SqliteUnitStatement, so a statement built elsewhere still
  // satisfies the batch parameter.
  type SyncStatement = { [runSync]?: () => SqliteUnitResult };

  function statement(sql: string, params: unknown[]): SqliteUnitStatement {
    const execute = <T = unknown>(): SqliteUnitResult<T> => {
      const results = db.prepare(sql).all(...params) as T[];
      const outcome = db.prepare('SELECT changes() AS count').get() as {
        count: number | bigint;
      };
      return {
        success: true,
        results,
        meta: { changes: Number(outcome.count) },
      };
    };
    // Bound before it is returned: checked as a fresh literal against
    // SqliteUnitStatement, the seam key below reads as an unknown property.
    const prepared = {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async <T = unknown>(column?: string): Promise<T | null> => {
        const row = db.prepare(sql).get(...params) as
          | Record<string, unknown>
          | undefined;
        if (row === undefined) return null;
        return (column !== undefined ? (row[column] ?? null) : row) as T | null;
      },
      run: async <T = unknown>(): Promise<SqliteUnitResult<T>> => execute<T>(),
      [runSync]: execute,
      all: async <T = unknown>(): Promise<SqliteUnitResult<T>> => ({
        success: true,
        results: db.prepare(sql).all(...params) as T[],
        meta: {},
      }),
    };
    return prepared;
  }
  return {
    prepare: (sql: string) => statement(sql, []),
    batch: async (statements: SqliteUnitStatement[]) => {
      db.exec('BEGIN IMMEDIATE');
      try {
        const results: SqliteUnitResult[] = [];
        for (const prepared of statements) {
          const sync = (prepared as SyncStatement)[runSync];
          results.push(sync ? sync() : await prepared.run());
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
