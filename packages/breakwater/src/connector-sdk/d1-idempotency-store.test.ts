// D1IdempotencyStore against REAL SQLite via node:sqlite (D1 is SQLite), so
// the atomic claim — INSERT ... ON CONFLICT DO NOTHING RETURNING — and the
// stale-pending takeover CAS are exercised for real, not mocked. The
// openSqlite()/d1Like() fixture is copied from flowsafe's store.test.ts
// pattern on purpose: breakwater must not import across packages for tests.

import { describe, expect, it } from 'vitest';

import {
  D1IdempotencyStore,
  type IdempotencyDatabase,
  type IdempotencyStatement,
} from './d1-idempotency-store.js';

// --- node:sqlite -> IdempotencyDatabase adapter ----------------------------

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

function d1Like(db: SqliteDatabase): IdempotencyDatabase {
  function statement(sql: string, params: unknown[]): IdempotencyStatement {
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

describe('D1IdempotencyStore', () => {
  it('round-trips reserve -> put -> replay', async () => {
    // #given
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });

    // #when
    const first = await store.reserve('conn:k1');
    await store.put('conn:k1', { result: { ok: true, n: 42 } });
    const second = await store.reserve('conn:k1');

    // #then
    expect(first).toEqual({ state: 'reserved', token: expect.any(String) });
    expect(second).toEqual({
      state: 'replay',
      record: { result: { ok: true, n: 42 } },
    });
    expect(await store.get('conn:k1')).toEqual({ result: { ok: true, n: 42 } });
  });

  it('admits exactly one winner when two stores race a key over one database', async () => {
    // #given — two store instances (two "isolates") sharing ONE database
    const db = d1Like(openSqlite());
    const a = new D1IdempotencyStore(db, { now: () => T0 });
    const b = new D1IdempotencyStore(db, { now: () => T0 });

    // #when — both claim the same key
    const [ra, rb] = await Promise.all([
      a.reserve('conn:k1'),
      b.reserve('conn:k1'),
    ]);

    // #then — exactly one 'reserved'; the loser sees 'pending'...
    const states = [ra.state, rb.state].sort();
    expect(states).toEqual(['pending', 'reserved']);
    // ...and after the winner's put, the loser's retry replays
    await a.put('conn:k1', { result: 'winner' });
    expect(await b.reserve('conn:k1')).toEqual({
      state: 'replay',
      record: { result: 'winner' },
    });
  });

  it('release() drops the reservation so the key re-reserves', async () => {
    // #given — a reserved key whose execute failed
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });
    expect(await store.reserve('conn:k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });

    // #when
    await store.release('conn:k1');

    // #then — the retry claims it fresh (failures are never cached)
    expect(await store.reserve('conn:k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });
  });

  it('release() never deletes a done record', async () => {
    // #given
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });
    await store.reserve('conn:k1');
    await store.put('conn:k1', { result: 'kept' });

    // #when — a stray release after completion
    await store.release('conn:k1');

    // #then
    expect(await store.get('conn:k1')).toEqual({ result: 'kept' });
  });

  it('takes over a stale pending reservation after the TTL', async () => {
    // #given — isolate A reserves, then dies; the clock advances past the TTL
    const db = d1Like(openSqlite());
    let now = T0;
    const clock = () => now;
    const a = new D1IdempotencyStore(db, { now: clock, pendingTtlMs: 60_000 });
    const b = new D1IdempotencyStore(db, { now: clock, pendingTtlMs: 60_000 });
    expect(await a.reserve('conn:k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });

    // #when — before the TTL the key is honestly pending...
    now = T0 + 59_000;
    expect(await b.reserve('conn:k1')).toEqual({ state: 'pending' });
    // ...after it, the takeover claims the key
    now = T0 + 61_000;
    const takeover = await b.reserve('conn:k1');

    // #then — the takeover is flagged distinctly from a fresh claim (audit
    // D2 — the wrapper uses this to emit a dedicated audit event), and the
    // takeover refreshed the row: a third claim is pending
    expect(takeover).toEqual({
      state: 'reserved',
      token: expect.any(String),
      tookOver: true,
    });
    expect(await a.reserve('conn:k1')).toEqual({ state: 'pending' });
  });

  it('does not take over a reservation still within the new 900s default TTL after a 600s execute (regression, audit D2)', async () => {
    // #given — default pendingTtlMs (900 000ms); agent-cli's default execute
    // timeout is 600 000ms. Under the OLD 300 000ms default this reserve
    // would have been wrongly taken over here, double-executing the call.
    const db = d1Like(openSqlite());
    let now = T0;
    const clock = () => now;
    const a = new D1IdempotencyStore(db, { now: clock });
    const b = new D1IdempotencyStore(db, { now: clock });
    expect(a.pendingTtlMs).toBe(900_000);
    expect(await a.reserve('conn:k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });

    // #when — a's execute is still running 600s later, and a retrying
    // caller b probes the same key
    now = T0 + 600_000;
    const probe = await b.reserve('conn:k1');

    // #then — still honestly pending: a's in-flight execution is never
    // taken over under the new default
    expect(probe).toEqual({ state: 'pending' });
  });

  it('round-trips a stored undefined result distinctly from a miss', async () => {
    // #given
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });
    await store.reserve('conn:void');
    await store.put('conn:void', { result: undefined });

    // #when / #then — a stored undefined replays; a missing key does not
    expect(await store.reserve('conn:void')).toEqual({
      state: 'replay',
      record: { result: undefined },
    });
    expect(await store.get('conn:void')).toEqual({ result: undefined });
    expect(await store.get('conn:missing')).toBeUndefined();
  });

  it('rejects a non-JSON-serializable result without corrupting the reservation', async () => {
    // #given — a circular result that JSON.stringify cannot encode
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });
    await store.reserve('conn:k1');
    const circular: { self?: unknown } = {};
    circular.self = circular;

    // #when / #then — put throws; the key is still pending, then releasable
    await expect(store.put('conn:k1', { result: circular })).rejects.toThrow();
    expect(await store.reserve('conn:k1')).toEqual({ state: 'pending' });
    await store.release('conn:k1');
    expect(await store.reserve('conn:k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });
  });

  it('respects a custom table name', async () => {
    // #given — two stores, distinct tables, one database
    const db = d1Like(openSqlite());
    const a = new D1IdempotencyStore(db, {
      table: 'idem_a',
      now: () => T0,
    });
    const b = new D1IdempotencyStore(db, {
      table: 'idem_b',
      now: () => T0,
    });

    // #when — the same key reserves independently per table
    expect(await a.reserve('conn:k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });

    // #then
    expect(await b.reserve('conn:k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });
  });

  it('binds finalize/release to the reservation lease — a taken-over holder cannot delete or finalize the winner (audit D2)', async () => {
    // #given — A reserves; the clock advances past the TTL; B takes over with a
    // rotated lease
    const db = d1Like(openSqlite());
    let now = T0;
    const clock = () => now;
    const a = new D1IdempotencyStore(db, { now: clock, pendingTtlMs: 60_000 });
    const b = new D1IdempotencyStore(db, { now: clock, pendingTtlMs: 60_000 });
    const ra = await a.reserve('conn:k1');
    now = T0 + 61_000;
    const rb = await b.reserve('conn:k1');
    if (ra.state !== 'reserved' || rb.state !== 'reserved') {
      throw new Error('expected both reservations to be reserved');
    }
    expect(rb).toEqual({
      state: 'reserved',
      token: expect.any(String),
      tookOver: true,
    });
    expect(rb.token).not.toBe(ra.token); // leases are distinct

    // #when — the stale holder A tries to release then finalize under its OLD lease
    await a.release('conn:k1', ra.token);
    await a.put('conn:k1', { result: 'A-stale' }, ra.token);

    // #then — A neither finalized its own result nor (proven below) deleted B's
    // pending row: the key is still open
    expect(await a.get('conn:k1')).toBeUndefined();

    // ...and B, the real owner, still finalizes — which only works if its
    // pending row survived A's release AND A's put never clobbered it
    await b.put('conn:k1', { result: 'B-wins' }, rb.token);
    expect(await a.get('conn:k1')).toEqual({ result: 'B-wins' });
  });

  it('backfills the token column on a pre-token table (schema migration, audit D2)', async () => {
    // #given — a table created by a pre-D2 release: the 5-column schema with NO
    // `token`. CREATE TABLE IF NOT EXISTS is a no-op against it, so only the
    // guarded ALTER can add the column.
    const raw = openSqlite();
    raw
      .prepare(
        `CREATE TABLE breakwater_idempotency (
          key TEXT PRIMARY KEY,
          state TEXT NOT NULL,
          result TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )`,
      )
      .run();
    const store = new D1IdempotencyStore(d1Like(raw), { now: () => T0 });

    // #when / #then — the guarded ALTER adds `token`, so the owner-scoped CAS
    // path works on the upgraded table: reserve → put(token) → replay
    const reservation = await store.reserve('conn:k1');
    expect(reservation).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });
    if (reservation.state !== 'reserved') throw new Error('unreachable');
    await store.put('conn:k1', { result: 'migrated' }, reservation.token);
    expect(await store.get('conn:k1')).toEqual({ result: 'migrated' });
  });
});
