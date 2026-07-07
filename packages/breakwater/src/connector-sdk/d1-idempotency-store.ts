// D1-backed AtomicIdempotencyStore — durable replay protection for
// production connectors. Cross-isolate atomicity comes from the database:
// reserve()'s INSERT ... ON CONFLICT DO NOTHING RETURNING admits exactly one
// isolate per key (the CAS shape flowsafe's D1ApprovalStore.transition
// proved out), and a stale-pending takeover keeps a crashed isolate from
// poisoning its key forever. Structural typing on purpose: breakwater stays
// platform-agnostic — no @cloudflare/workers-types import — so tests back
// the interfaces with node:sqlite and Workers pass env.DB directly.

import type {
  AtomicIdempotencyStore,
  IdempotencyRecord,
  IdempotencyReservation,
} from './index.js';

/** The subset of D1Database this store uses. */
export interface IdempotencyDatabase {
  prepare(query: string): IdempotencyStatement;
}

export interface IdempotencyStatement {
  bind(...values: unknown[]): IdempotencyStatement;
  first<T = unknown>(): Promise<T | null>;
  run(): Promise<unknown>;
}

interface IdempotencyRow {
  key: string;
  state: string;
  result: string | null;
  created_at: string;
  updated_at: string;
}

export interface D1IdempotencyStoreOptions {
  /** Table name (default 'breakwater_idempotency'). */
  table?: string;
  /**
   * Age (ms) after which a 'pending' reservation counts as abandoned — a
   * crashed isolate — and may be taken over. Must exceed the longest
   * expected execute duration, or a slow-but-alive execution races its own
   * takeover. Default 300 000.
   */
  pendingTtlMs?: number;
  /** Clock override for tests. */
  now?: () => number;
}

const DEFAULT_TABLE = 'breakwater_idempotency';
const DEFAULT_PENDING_TTL_MS = 300_000;

function parseRecord(json: string | null): IdempotencyRecord {
  // put() serializes the whole record, so a stored `undefined` result
  // round-trips as '{}'. A NULL column (defensive) reads the same way.
  if (json === null) return { result: undefined };
  return JSON.parse(json) as IdempotencyRecord;
}

export class D1IdempotencyStore implements AtomicIdempotencyStore {
  readonly #db: IdempotencyDatabase;
  readonly #table: string;
  readonly #pendingTtlMs: number;
  readonly #now: () => number;
  #schemaReady?: Promise<void>;

  constructor(
    db: IdempotencyDatabase,
    options: D1IdempotencyStoreOptions = {},
  ) {
    this.#db = db;
    this.#table = options.table ?? DEFAULT_TABLE;
    this.#pendingTtlMs = options.pendingTtlMs ?? DEFAULT_PENDING_TTL_MS;
    this.#now = options.now ?? Date.now;
  }

  async reserve(key: string): Promise<IdempotencyReservation> {
    await this.#ready();
    const nowIso = new Date(this.#now()).toISOString();
    // The INSERT is the atomic claim: exactly one isolate gets the row.
    const claimed = await this.#db
      .prepare(
        `INSERT INTO ${this.#table} (key, state, result, created_at, updated_at)
         VALUES (?, 'pending', NULL, ?, ?)
         ON CONFLICT(key) DO NOTHING
         RETURNING key`,
      )
      .bind(key, nowIso, nowIso)
      .first<{ key: string }>();
    if (claimed) return { state: 'reserved' };
    const row = await this.#db
      .prepare(`SELECT * FROM ${this.#table} WHERE key = ?`)
      .bind(key)
      .first<IdempotencyRow>();
    if (!row) {
      // The conflicting row was released between the claim and this read;
      // 'pending' is the honest answer — the caller's retry re-reserves.
      return { state: 'pending' };
    }
    if (row.state === 'done') {
      return { state: 'replay', record: parseRecord(row.result) };
    }
    // ISO-8601 strings compare lexicographically in timestamp order (both
    // sides come from toISOString above).
    const staleBefore = new Date(
      this.#now() - this.#pendingTtlMs,
    ).toISOString();
    if (row.updated_at >= staleBefore) return { state: 'pending' };
    // Stale-pending takeover (crash safety — a dead isolate must not poison
    // the key forever): refresh updated_at under the same CAS shape; losing
    // the takeover race means another isolate now holds the key.
    const takenOver = await this.#db
      .prepare(
        `UPDATE ${this.#table} SET updated_at = ?
         WHERE key = ? AND state = 'pending' AND updated_at < ?
         RETURNING key`,
      )
      .bind(nowIso, key, staleBefore)
      .first<{ key: string }>();
    return takenOver ? { state: 'reserved' } : { state: 'pending' };
  }

  async put(key: string, record: IdempotencyRecord): Promise<void> {
    await this.#ready();
    // Serialize before touching the row: a non-JSON-serializable result
    // throws here without corrupting state — the same JSON-safe posture as
    // the flowsafe approval store. UPSERT so a put() without a live
    // reservation still lands the record.
    const json = JSON.stringify(record);
    const nowIso = new Date(this.#now()).toISOString();
    await this.#db
      .prepare(
        `INSERT INTO ${this.#table} (key, state, result, created_at, updated_at)
         VALUES (?, 'done', ?, ?, ?)
         ON CONFLICT(key) DO UPDATE
           SET state = 'done', result = excluded.result, updated_at = excluded.updated_at`,
      )
      .bind(key, json, nowIso, nowIso)
      .run();
  }

  async release(key: string): Promise<void> {
    await this.#ready();
    await this.#db
      .prepare(`DELETE FROM ${this.#table} WHERE key = ? AND state = 'pending'`)
      .bind(key)
      .run();
  }

  async get(key: string): Promise<IdempotencyRecord | undefined> {
    await this.#ready();
    const row = await this.#db
      .prepare(`SELECT * FROM ${this.#table} WHERE key = ? AND state = 'done'`)
      .bind(key)
      .first<IdempotencyRow>();
    return row ? parseRecord(row.result) : undefined;
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
          key TEXT PRIMARY KEY,
          state TEXT NOT NULL,
          result TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )`,
      )
      .run();
  }
}
