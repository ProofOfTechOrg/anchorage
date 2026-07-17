// SPDX-License-Identifier: Apache-2.0
// Track C (M-004), CI-M-004-003 — the D1 thread-state domain over
// TABLE_THREAD_STATE ('mastra_thread_state'), mirroring core's abstract
// ThreadStateStorage + InMemory reference. Each `(threadId, type)` pair owns one
// durable value: the state-signal lanes (`sendStateSignal`, snapshot/delta with
// cacheKey dedupe) and — reused by Track F — the goal objective record
// (GOAL_STATE_TYPE 'goal'). Composed into createD1Storage so the built-in task
// tools and the in-loop goal scorer persist to D1 rather than the composite's
// default InMemoryThreadStateStorage (which is lost on eviction).
//
// TENANCY: `thread_id` holds the tenant-salted threadId, so purgeTenant's range
// is exact over it (TENANT_RANGE_PURGE_TABLES) and the TTL reaps by `updatedAt`
// (purgeExpiredThreadState) — the encoding, again, ISO-8601 TEXT.

import { ThreadStateStorage } from '@mastra/core/storage';

import {
  jsonOrNull,
  parseJsonOrUndefined,
  type SignalDatabase,
} from './d1-shared.js';

interface ThreadStateRow {
  value: string | null;
}

export class D1ThreadStateStorage extends ThreadStateStorage {
  readonly #db: SignalDatabase;
  readonly #table: string;
  #ready?: Promise<void>;

  constructor(db: SignalDatabase, tablePrefix = '') {
    super();
    this.#db = db;
    this.#table = `${tablePrefix}mastra_thread_state`;
  }

  /** Lazy, memoized, clear-on-failure schema creation — same posture as the approval store. */
  #ensureSchema(): Promise<void> {
    if (!this.#ready) {
      this.#ready = Promise.resolve(
        this.#db
          .prepare(
            `CREATE TABLE IF NOT EXISTS ${this.#table} (
               thread_id TEXT NOT NULL,
               type TEXT NOT NULL,
               value TEXT,
               updatedAt TEXT NOT NULL,
               PRIMARY KEY (thread_id, type)
             )`,
          )
          .run()
          .then(() =>
            this.#db
              .prepare(
                `CREATE INDEX IF NOT EXISTS idx_${this.#table}_updated
                 ON ${this.#table} (updatedAt)`,
              )
              .run(),
          )
          .then(() => undefined),
      ).catch((error: unknown) => {
        this.#ready = undefined;
        throw error;
      });
    }
    return this.#ready;
  }

  async init(): Promise<void> {
    await this.#ensureSchema();
  }

  async getState<T = unknown>(args: {
    threadId: string;
    type: string;
  }): Promise<T | undefined> {
    await this.#ensureSchema();
    const row = await this.#db
      .prepare(
        `SELECT value FROM ${this.#table} WHERE thread_id = ? AND type = ?`,
      )
      .bind(args.threadId, args.type)
      .first<ThreadStateRow>();
    if (!row) return undefined;
    return parseJsonOrUndefined<T>(row.value);
  }

  async setState<T = unknown>(args: {
    threadId: string;
    type: string;
    value: T;
  }): Promise<void> {
    await this.#ensureSchema();
    // Full-replacement semantics (core's contract): the stored value becomes
    // exactly `value`. updatedAt is bumped so the thread-state TTL keys off it.
    await this.#db
      .prepare(
        `INSERT OR REPLACE INTO ${this.#table} (thread_id, type, value, updatedAt)
         VALUES (?, ?, ?, ?)`,
      )
      .bind(
        args.threadId,
        args.type,
        jsonOrNull(args.value),
        new Date().toISOString(),
      )
      .run();
  }

  async deleteState(args: { threadId: string; type: string }): Promise<void> {
    await this.#ensureSchema();
    await this.#db
      .prepare(`DELETE FROM ${this.#table} WHERE thread_id = ? AND type = ?`)
      .bind(args.threadId, args.type)
      .run();
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.#ensureSchema();
    await this.#db.prepare(`DELETE FROM ${this.#table}`).run();
  }
}
