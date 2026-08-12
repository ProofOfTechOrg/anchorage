// SPDX-License-Identifier: Apache-2.0
// D1-backed RateLimitStore — a durable fixed-window budget shared across
// isolates. The in-memory store is per-isolate, and under flowsafe's
// DO-per-run routing "per isolate" degrades to "per RUN": ten concurrent runs
// each get a fresh window, so a manifest `rateLimit: '5/min'` admits fifty
// executions a minute. Sharing the counter in D1 makes the declared budget
// mean what it says across every isolate that shares the database. The
// count-then-check contract matches InMemoryRateLimitStore exactly: the
// UPSERT always increments and returns the post-increment count; the
// connector wrapper compares it to the manifest limit. Structural typing on
// purpose (no @cloudflare/workers-types import), same posture as
// D1IdempotencyStore: tests back the interface with node:sqlite, Workers
// pass env.DB.

import type { RateLimitStore } from './index.js';

/** The subset of D1Database this store uses. */
export interface RateLimitDatabase {
  /** Prepare a SQL statement. */
  prepare(query: string): RateLimitStatement;
  /** Execute prepared statements atomically and in order. */
  batch<T = unknown>(
    statements: RateLimitStatement[],
  ): Promise<RateLimitBatchResult<T>[]>;
}

/** Result subset returned for one statement in a D1 batch. */
export interface RateLimitBatchResult<T = unknown> {
  /** Rows returned by statements with a RETURNING clause. */
  results?: T[];
}

/** Prepared-statement subset required by {@link D1RateLimitStore}. */
export interface RateLimitStatement {
  /** Bind positional parameters and return the bound statement. */
  bind(...values: unknown[]): RateLimitStatement;
  /** Execute a statement that does not need to return rows. */
  run(): Promise<unknown>;
}

/** Configuration for {@link D1RateLimitStore}. */
export interface D1RateLimitStoreOptions {
  /** Table name (default 'breakwater_rate_limit'). */
  table?: string;
}

const DEFAULT_TABLE = 'breakwater_rate_limit';

export class D1RateLimitStore implements RateLimitStore {
  readonly #db: RateLimitDatabase;
  readonly #table: string;
  #schemaReady?: Promise<void>;

  constructor(db: RateLimitDatabase, options: D1RateLimitStoreOptions = {}) {
    this.#db = db;
    this.#table = options.table ?? DEFAULT_TABLE;
  }

  async increment(key: string, windowMs: number, now: number): Promise<number> {
    await this.#ready();
    // Epoch-aligned bucketing, identical to InMemoryRateLimitStore: every
    // isolate computes the same window boundaries from the caller's clock.
    const windowStart = now - (now % windowMs);
    // D1 batch() is transactional. Keep the increment and rollover cleanup in
    // one batch so a cleanup failure rolls back the increment instead of
    // rejecting a connector call after its quota was already consumed.
    const [incrementResult] = await this.#db.batch<{ count: number }>([
      this.#db
        .prepare(
          `INSERT INTO ${this.#table} (budget_key, window_start, count)
           VALUES (?, ?, 1)
           ON CONFLICT(budget_key, window_start) DO UPDATE
             SET count = count + 1
           RETURNING count`,
        )
        .bind(key, windowStart),
      this.#db
        .prepare(
          `DELETE FROM ${this.#table}
           WHERE budget_key = ? AND window_start < ?
             AND EXISTS (
               SELECT 1 FROM ${this.#table}
               WHERE budget_key = ? AND window_start = ? AND count = 1
             )`,
        )
        .bind(key, windowStart, key, windowStart),
    ]);
    const row = incrementResult?.results?.[0];
    if (!row) {
      // INSERT-or-UPDATE always yields a row; no row means the database
      // misbehaved. Throw (the wrapper records it and fails the call closed)
      // rather than return a count that under-reports spend.
      throw new Error(
        `D1RateLimitStore: increment returned no row for '${key}'`,
      );
    }
    return row.count;
  }

  // Lazy, memoized schema creation; a failed attempt clears the memo so the
  // next call retries instead of pinning the store to a dead promise.
  #ready(): Promise<void> {
    this.#schemaReady ??= this.#createSchema().catch((error: unknown) => {
      this.#schemaReady = undefined;
      throw error;
    });
    return this.#schemaReady;
  }

  async #createSchema(): Promise<void> {
    await this.#db
      .prepare(
        `CREATE TABLE IF NOT EXISTS ${this.#table} (
          budget_key TEXT NOT NULL,
          window_start INTEGER NOT NULL,
          count INTEGER NOT NULL,
          PRIMARY KEY (budget_key, window_start)
        )`,
      )
      .run();
  }
}
