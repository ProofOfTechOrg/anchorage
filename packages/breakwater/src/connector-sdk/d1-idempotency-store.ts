// SPDX-License-Identifier: Apache-2.0
// D1-backed AtomicIdempotencyStore — durable replay protection for
// production connectors. Cross-isolate atomicity comes from the database:
// reserve()'s INSERT ... ON CONFLICT DO NOTHING RETURNING admits exactly one
// isolate per key (the CAS shape flowsafe's D1ApprovalStore.transition
// proved out), and a stale-pending takeover keeps a crashed isolate from
// poisoning its key forever. Structural typing on purpose: breakwater stays
// platform-agnostic — no @cloudflare/workers-types import — so tests back
// the interfaces with node:sqlite and Workers pass env.DB directly.

import {
  ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION,
  type AtomicLegacyIdempotencyMigrationRequest,
  type AtomicLegacyIdempotencyMigrationResult,
} from './idempotency-migration.js';
import type {
  AtomicIdempotencyStore,
  IdempotencyInspection,
  IdempotencyRecord,
  IdempotencyReservation,
  InspectableIdempotencyStore,
} from './index.js';
import { newToken } from './new-token.js';

/** The subset of D1Database this store uses. */
export interface IdempotencyDatabase {
  /** Prepare a SQL statement. */
  prepare(query: string): IdempotencyStatement;
}

/** Result subset returned for one statement in a D1 batch. */
export interface IdempotencyBatchResult<T = unknown> {
  /** Rows returned by statements with a RETURNING clause. */
  results?: T[];
}

/** D1 subset additionally required for atomic legacy-record migration. */
export interface IdempotencyBatchDatabase extends IdempotencyDatabase {
  /** Execute prepared statements atomically and in order. */
  batch<T = unknown>(
    statements: IdempotencyStatement[],
  ): Promise<IdempotencyBatchResult<T>[]>;
}

/** Prepared-statement subset required by {@link D1IdempotencyStore}. */
export interface IdempotencyStatement {
  /** Bind positional parameters and return the bound statement. */
  bind(...values: unknown[]): IdempotencyStatement;
  /** Return the first result row, or `null` when no row matched. */
  first<T = unknown>(): Promise<T | null>;
  /** Execute a statement that does not need to return rows. */
  run(): Promise<unknown>;
}

interface IdempotencyRow {
  key: string;
  state: string;
  result: string | null;
  /** Reservation lease token; `NULL` on rows created before lease tokens existed. */
  token: string | null;
  created_at: string;
  updated_at: string;
}

/** Configuration for {@link D1IdempotencyStore}. */
export interface D1IdempotencyStoreOptions {
  /** Table name (default 'breakwater_idempotency'). */
  table?: string;
  /**
   * Age (ms) after which a 'pending' reservation counts as abandoned — a
   * crashed isolate — and may be taken over. Must exceed the longest
   * expected execute duration, or a slow-but-alive execution races its own
   * takeover. A takeover while the original holder is still running can
   * execute a write connector twice. Default 900 000 — above
   * agent-cli's default 600 000ms execute timeout; agent-cli's
   * `idempotencyKey` option additionally throws at definition time when a
   * configured store's `pendingTtlMs` does not clear its `timeoutMs`.
   */
  pendingTtlMs?: number;
  /** Clock override for tests. */
  now?: () => number;
}

const DEFAULT_TABLE = 'breakwater_idempotency';
const DEFAULT_PENDING_TTL_MS = 900_000;
const MAX_TIMESTAMP_MS = 8_640_000_000_000_000;

function parseRecord(json: string | null): IdempotencyRecord {
  // put() serializes the whole record, so a stored `undefined` result
  // round-trips as '{}'. A NULL column (defensive) reads the same way.
  if (json === null) return { result: undefined };
  return JSON.parse(json) as IdempotencyRecord;
}

function assertJsonNative(value: unknown, seen = new Set<object>()): void {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean'
  ) {
    return;
  }
  if (typeof value === 'number') {
    if (Number.isFinite(value) && !Object.is(value, -0)) return;
    throw new TypeError('D1 idempotency results must be JSON-native');
  }
  if (typeof value !== 'object') {
    throw new TypeError('D1 idempotency results must be JSON-native');
  }
  if (seen.has(value)) {
    throw new TypeError('D1 idempotency results must be JSON-native');
  }
  seen.add(value);
  if (Array.isArray(value)) {
    const names = Object.getOwnPropertyNames(value);
    if (
      names.length !== value.length + 1 ||
      names.some(
        (name) =>
          name !== 'length' &&
          (!/^(?:0|[1-9]\d*)$/.test(name) || Number(name) >= value.length),
      ) ||
      Object.getOwnPropertySymbols(value).length > 0
    ) {
      throw new TypeError('D1 idempotency results must be JSON-native');
    }
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
        throw new TypeError('D1 idempotency results must be JSON-native');
      }
      assertJsonNative(descriptor.value, seen);
    }
    return;
  }
  if (Object.getPrototypeOf(value) !== Object.prototype) {
    throw new TypeError('D1 idempotency results must be JSON-native');
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    throw new TypeError('D1 idempotency results must be JSON-native');
  }
  for (const name of Object.getOwnPropertyNames(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, name);
    if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
      throw new TypeError('D1 idempotency results must be JSON-native');
    }
    assertJsonNative(descriptor.value, seen);
  }
}

function assertSerializableResult(result: unknown): void {
  // Preserve the established top-level undefined result compatibility. Every
  // other value must survive D1's JSON round-trip without changing type or
  // structure; otherwise a replay could differ from the first public result.
  if (result !== undefined) assertJsonNative(result);
}

function serializeRecord(record: IdempotencyRecord): string {
  const result = record.result;
  assertSerializableResult(result);
  return JSON.stringify({ result });
}

function recordsMatch(
  left: IdempotencyRecord,
  right: IdempotencyRecord,
): boolean {
  return serializeRecord(left) === serializeRecord(right);
}

function isBatchDatabase(
  database: IdempotencyDatabase,
): database is IdempotencyBatchDatabase {
  return (
    typeof (database as Partial<IdempotencyBatchDatabase>).batch === 'function'
  );
}

// The `token` column is added by both CREATE (fresh DBs) and a defensive
// ALTER (pre-token DBs). Whichever runs second is a duplicate-column no-op;
// swallow only that error so a real schema failure still surfaces.
function isDuplicateColumn(error: unknown): boolean {
  return error instanceof Error && /duplicate column/i.test(error.message);
}

export class D1IdempotencyStore
  implements AtomicIdempotencyStore, InspectableIdempotencyStore
{
  readonly #db: IdempotencyDatabase;
  readonly #table: string;
  /** Stale-pending takeover threshold (ms) — see D1IdempotencyStoreOptions.pendingTtlMs. */
  readonly pendingTtlMs: number;
  readonly #now: () => number;
  #schemaReady?: Promise<void>;

  constructor(
    db: IdempotencyDatabase,
    options: D1IdempotencyStoreOptions = {},
  ) {
    this.#db = db;
    this.#table = options.table ?? DEFAULT_TABLE;
    this.pendingTtlMs = options.pendingTtlMs ?? DEFAULT_PENDING_TTL_MS;
    if (
      !Number.isSafeInteger(this.pendingTtlMs) ||
      this.pendingTtlMs <= 0 ||
      this.pendingTtlMs > MAX_TIMESTAMP_MS
    ) {
      throw new TypeError(
        `D1IdempotencyStore pendingTtlMs must be a positive safe integer no greater than ${MAX_TIMESTAMP_MS} milliseconds`,
      );
    }
    this.#now = options.now ?? Date.now;
  }

  /** Inspect a key without changing its reservation state. */
  async inspect(key: string): Promise<IdempotencyInspection> {
    await this.#ready();
    const row = await this.#db
      .prepare(`SELECT * FROM ${this.#table} WHERE key = ?`)
      .bind(key)
      .first<IdempotencyRow>();
    if (!row) return { state: 'absent' };
    return row.state === 'done'
      ? { state: 'replay', record: parseRecord(row.result) }
      : { state: 'pending' };
  }

  /** Atomically reserve a key, replay a completed result, or report contention. */
  async reserve(key: string): Promise<IdempotencyReservation> {
    await this.#ready();
    const nowIso = new Date(this.#now()).toISOString();
    // The reservation lease: put()/release() CAS on it (audit D2).
    const token = newToken();
    // The INSERT is the atomic claim: exactly one isolate gets the row.
    const claimed = await this.#db
      .prepare(
        `INSERT INTO ${this.#table} (key, state, result, token, created_at, updated_at)
         VALUES (?, 'pending', NULL, ?, ?, ?)
         ON CONFLICT(key) DO NOTHING
         RETURNING key`,
      )
      .bind(key, token, nowIso, nowIso)
      .first<{ key: string }>();
    if (claimed) return { state: 'reserved', token };
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
    const staleBefore = new Date(this.#now() - this.pendingTtlMs).toISOString();
    if (row.updated_at >= staleBefore) return { state: 'pending' };
    // Stale-pending takeover (crash safety — a dead isolate must not poison
    // the key forever): refresh updated_at AND rotate the lease token under
    // the same CAS shape; losing the takeover race means another isolate now
    // holds the key. Rotating the token is what closes audit D2: after this,
    // the previous holder's token no longer matches, so its put()/release()
    // become no-ops and cannot finalize or delete this new claim. tookOver is
    // reported (not just 'reserved') so the wrapper can raise a dedicated
    // audit signal — the "stale" holder was merely slow, not dead, whenever
    // pendingTtlMs was set too low relative to the real execute duration.
    const takenOver = await this.#db
      .prepare(
        `UPDATE ${this.#table} SET updated_at = ?, token = ?
         WHERE key = ? AND state = 'pending' AND updated_at < ?
         RETURNING key`,
      )
      .bind(nowIso, token, key, staleBefore)
      .first<{ key: string }>();
    return takenOver
      ? { state: 'reserved', token, tookOver: true }
      : { state: 'pending' };
  }

  async put(
    key: string,
    record: IdempotencyRecord,
    token?: string,
  ): Promise<void> {
    await this.#ready();
    // Validate and serialize before touching the row. JSON.stringify silently
    // changes Date, Map, non-finite numbers, holes, and undefined object
    // members; accepting those would make replay differ from the first public
    // result. A rejected result leaves the reservation pending, and the
    // wrapper reports degraded replay protection without inviting a duplicate.
    const json = serializeRecord(record);
    const nowIso = new Date(this.#now()).toISOString();
    if (token !== undefined) {
      // Owner-scoped finalize (audit D2 CAS): flip to 'done' ONLY if this
      // lease still owns the row. A stale holder whose reservation was taken
      // over matches no row and NO-OPs — it never overwrites the new holder's
      // claim, and deliberately does NOT re-INSERT (it lost ownership). The
      // token rotation on takeover is what makes the mismatch happen.
      await this.#db
        .prepare(
          `UPDATE ${this.#table} SET state = 'done', result = ?, updated_at = ?
           WHERE key = ? AND token = ?`,
        )
        .bind(json, nowIso, key, token)
        .run();
      return;
    }
    // Legacy get/put path (no reservation): UPSERT so a put() without a live
    // reservation still lands the record.
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

  async release(key: string, token?: string): Promise<void> {
    await this.#ready();
    // Owner-scoped when a lease is supplied (audit D2 CAS): only the matching
    // lease's pending row is dropped, so a taken-over stale holder cannot
    // delete the new holder's claim. The state='pending' guard (both branches)
    // preserves "release never deletes a done record".
    if (token !== undefined) {
      await this.#db
        .prepare(
          `DELETE FROM ${this.#table} WHERE key = ? AND state = 'pending' AND token = ?`,
        )
        .bind(key, token)
        .run();
      return;
    }
    await this.#db
      .prepare(`DELETE FROM ${this.#table} WHERE key = ? AND state = 'pending'`)
      .bind(key)
      .run();
  }

  /** Return a completed record for `key`, or `undefined` while absent or pending. */
  async get(key: string): Promise<IdempotencyRecord | undefined> {
    await this.#ready();
    const row = await this.#db
      .prepare(`SELECT * FROM ${this.#table} WHERE key = ? AND state = 'done'`)
      .bind(key)
      .first<IdempotencyRow>();
    return row ? parseRecord(row.result) : undefined;
  }

  /** @internal Connector-bound migration capability. */
  async [ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION](
    request: AtomicLegacyIdempotencyMigrationRequest,
  ): Promise<AtomicLegacyIdempotencyMigrationResult> {
    await this.#ready();
    if (!isBatchDatabase(this.#db)) {
      throw new TypeError(
        'D1IdempotencyStore legacy migration requires D1-compatible transactional batch() support',
      );
    }
    const expectedRecord = { result: request.expectedRecord.result };
    const targetRecord = { result: request.targetRecord.result };
    const targetJson = serializeRecord(targetRecord);
    const source = await this.#row(request.sourceKey);
    const target = await this.#row(request.targetKey);

    if (!source) {
      if (!target) return { state: 'source-absent' };
      if (
        target.state === 'done' &&
        recordsMatch(parseRecord(target.result), targetRecord)
      ) {
        return { state: 'already-migrated', record: targetRecord };
      }
      return { state: 'target-conflict', target: this.#inspection(target) };
    }
    if (source.state !== 'done') return { state: 'source-pending' };
    const sourceRecord = parseRecord(source.result);
    if (!recordsMatch(sourceRecord, expectedRecord)) {
      return { state: 'source-mismatch', record: sourceRecord };
    }
    if (
      target &&
      (target.state !== 'done' ||
        !recordsMatch(parseRecord(target.result), targetRecord))
    ) {
      return { state: 'target-conflict', target: this.#inspection(target) };
    }

    const nowIso = new Date(this.#now()).toISOString();
    const targetGuardJson = target ? target.result : targetJson;
    const [, deleted] = await this.#db.batch<{ key: string }>([
      this.#db
        .prepare(
          `INSERT INTO ${this.#table} (key, state, result, token, created_at, updated_at)
           SELECT ?, 'done', ?, NULL, created_at, ?
           FROM ${this.#table}
           WHERE key = ? AND state = 'done' AND result IS ?
           ON CONFLICT(key) DO NOTHING
           RETURNING key`,
        )
        .bind(
          request.targetKey,
          targetJson,
          nowIso,
          request.sourceKey,
          source.result,
        ),
      this.#db
        .prepare(
          `DELETE FROM ${this.#table}
           WHERE key = ? AND state = 'done' AND result IS ?
             AND EXISTS (
               SELECT 1 FROM ${this.#table}
               WHERE key = ? AND state = 'done' AND result IS ?
             )
           RETURNING key`,
        )
        .bind(
          request.sourceKey,
          source.result,
          request.targetKey,
          targetGuardJson,
        ),
    ]);
    if (deleted?.results?.[0]) {
      return { state: 'migrated', record: targetRecord };
    }

    const [currentSource, currentTarget] = await Promise.all([
      this.#row(request.sourceKey),
      this.#row(request.targetKey),
    ]);
    if (!currentSource) {
      if (
        currentTarget?.state === 'done' &&
        recordsMatch(parseRecord(currentTarget.result), targetRecord)
      ) {
        return { state: 'already-migrated', record: targetRecord };
      }
      return currentTarget
        ? {
            state: 'target-conflict',
            target: this.#inspection(currentTarget),
          }
        : { state: 'source-absent' };
    }
    if (currentSource.state !== 'done') return { state: 'source-pending' };
    const currentSourceRecord = parseRecord(currentSource.result);
    if (!recordsMatch(currentSourceRecord, expectedRecord)) {
      return { state: 'source-mismatch', record: currentSourceRecord };
    }
    if (currentTarget) {
      if (
        currentTarget.state === 'done' &&
        recordsMatch(parseRecord(currentTarget.result), targetRecord)
      ) {
        throw new Error(
          'D1IdempotencyStore: legacy migration left both source and target records present',
        );
      }
      return {
        state: 'target-conflict',
        target: this.#inspection(currentTarget),
      };
    }
    throw new Error(
      'D1IdempotencyStore: legacy migration made no progress despite matching source and target guards',
    );
  }

  async #row(key: string): Promise<IdempotencyRow | null> {
    return this.#db
      .prepare(`SELECT * FROM ${this.#table} WHERE key = ?`)
      .bind(key)
      .first<IdempotencyRow>();
  }

  #inspection(row: IdempotencyRow): IdempotencyInspection {
    return row.state === 'done'
      ? { state: 'replay', record: parseRecord(row.result) }
      : { state: 'pending' };
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
          token TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )`,
      )
      .run();
    // Backfill the token column on a pre-token table (a local/spike DB created
    // before audit D2). On a fresh table the CREATE already added it, so this
    // ALTER is a duplicate-column no-op — swallow only that. Mirrors flowsafe's
    // d1-store.ts additive-column idiom.
    try {
      await this.#db
        .prepare(`ALTER TABLE ${this.#table} ADD COLUMN token TEXT`)
        .run();
    } catch (error) {
      if (!isDuplicateColumn(error)) throw error;
    }
  }
}
