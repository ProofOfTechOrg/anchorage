// SPDX-License-Identifier: Apache-2.0
// D1IdempotencyStore against node:sqlite as a fast SQL facsimile. These are
// non-fidelity unit tests: the dedicated worker project exercises the same
// atomic claim and stale-pending takeover against D1 inside workerd. The
// openSqlite()/d1Like() fixture is copied from flowsafe's store.test.ts
// pattern on purpose: breakwater must not import across packages for tests.

import { RequestContext } from '@mastra/core/request-context';
import type { ToolExecutionContext } from '@mastra/core/tools';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { ISOLATION_SCOPE_CONTEXT_KEY } from '../policy-engine/index.js';
import {
  D1IdempotencyStore,
  type IdempotencyBatchDatabase,
  type IdempotencyDatabase,
  type IdempotencyStatement,
} from './d1-idempotency-store.js';
import {
  ConnectorPolicyError,
  createConnector,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
  type IdempotencyRecord,
  inspectLegacyConnectorIdempotency,
  migrateLegacyConnectorIdempotency,
} from './index.js';

// --- node:sqlite -> IdempotencyDatabase adapter ----------------------------

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

function d1Like(db: SqliteDatabase): IdempotencyBatchDatabase {
  const prepared = new WeakMap<
    IdempotencyStatement,
    { sql: string; params: unknown[] }
  >();
  function statement(sql: string, params: unknown[]): IdempotencyStatement {
    const value: IdempotencyStatement = {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async <T>() =>
        (db.prepare(sql).get(...params) as T | undefined) ?? null,
      run: async () => db.prepare(sql).run(...params),
    };
    prepared.set(value, { sql, params });
    return value;
  }
  return {
    prepare: (sql: string) => statement(sql, []),
    batch: async <T>(statements: IdempotencyStatement[]) => {
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

function connectorContext(
  scope: string,
  idempotencyKey: string,
): ToolExecutionContext {
  const requestContext = new RequestContext();
  requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, scope);
  requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, idempotencyKey);
  return { requestContext } as unknown as ToolExecutionContext;
}

async function runConnector(
  tool: {
    execute?: (
      inputData: unknown,
      context: ToolExecutionContext,
    ) => Promise<unknown>;
  },
  context: ToolExecutionContext,
): Promise<unknown> {
  if (!tool.execute) throw new Error('tool has no execute');
  return tool.execute({}, context);
}

describe('D1IdempotencyStore (Node SQLite facsimile)', () => {
  it.each([
    0,
    -1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    1.5,
    Number.MAX_SAFE_INTEGER,
    8_640_000_000_000_001,
  ])('rejects invalid pendingTtlMs %s at construction', (pendingTtlMs) => {
    expect(
      () => new D1IdempotencyStore(d1Like(openSqlite()), { pendingTtlMs }),
    ).toThrow(/positive safe integer no greater than 8640000000000000/);
  });

  it('accepts the valid default and custom pending TTL boundaries', () => {
    expect(new D1IdempotencyStore(d1Like(openSqlite())).pendingTtlMs).toBe(
      900_000,
    );
    expect(
      new D1IdempotencyStore(d1Like(openSqlite()), { pendingTtlMs: 1 })
        .pendingTtlMs,
    ).toBe(1);
    expect(
      new D1IdempotencyStore(d1Like(openSqlite()), {
        pendingTtlMs: 8_640_000_000_000_000,
      }).pendingTtlMs,
    ).toBe(8_640_000_000_000_000);
  });

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

  it('inspects absent, pending, and completed legacy rows without mutation', async () => {
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });

    expect(await store.inspect('pay:missing')).toEqual({ state: 'absent' });
    const reservation = await store.reserve('pay:pending');
    expect(await store.inspect('pay:pending')).toEqual({ state: 'pending' });
    expect(await store.reserve('pay:pending')).toEqual({ state: 'pending' });
    if (reservation.state !== 'reserved') throw new Error('unreachable');
    await store.put('pay:done', { result: { source: 'legacy' } });
    expect(await store.inspect('pay:done')).toEqual({
      state: 'replay',
      record: { result: { source: 'legacy' } },
    });
  });

  it('keeps the reported scoped tuple collision distinct through the D1 store', async () => {
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });
    const execute = vi.fn(async () => ({
      execution: execute.mock.calls.length,
    }));
    const connector = createConnector({
      id: 'pay',
      description: 'Pay one invoice',
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        idempotencyStore: store,
        idempotencyKeyMigration: 'legacy-writers-drained',
      },
      execute,
    });
    const firstTuple = connectorContext('tenant', 'pay:invoice-1');
    const secondTuple = connectorContext('tenant:pay', 'invoice-1');

    const first = await runConnector(connector, firstTuple);
    const second = await runConnector(connector, secondTuple);
    expect(first).not.toEqual(second);
    await expect(runConnector(connector, firstTuple)).resolves.toEqual(first);
    await expect(runConnector(connector, secondTuple)).resolves.toEqual(second);
    expect(execute).toHaveBeenCalledTimes(2);
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

  it.each([
    ['Date', new Date('2026-08-12T00:00:00.000Z')],
    ['Map', new Map([['key', 'value']])],
    [
      'shared object reference',
      (() => {
        const shared = { value: true };
        return { first: shared, second: shared };
      })(),
    ],
    ['undefined object member', { missing: undefined }],
    ['undefined array member', [undefined]],
    ['non-finite number', Number.POSITIVE_INFINITY],
    ['negative zero', -0],
    [
      'null-prototype object',
      Object.assign(Object.create(null) as Record<string, unknown>, {
        value: true,
      }),
    ],
    ['symbol member', { [Symbol('hidden')]: true }],
  ])('rejects %s results that JSON would silently change', async (_name, result) => {
    // #given
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });
    const reservation = await store.reserve('conn:changed');
    if (reservation.state !== 'reserved') throw new Error('expected reserve');

    // #when / #then — no lossy replay record is finalized
    await expect(
      store.put('conn:changed', { result }, reservation.token),
    ).rejects.toThrow('D1 idempotency results must be JSON-native');
    expect(await store.reserve('conn:changed')).toEqual({ state: 'pending' });
  });

  it('snapshots a structural record result once before validation and storage', async () => {
    // #given — a public structural IdempotencyRecord may expose an accessor
    // whose later reads return a different, lossy value
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });
    const reservation = await store.reserve('conn:getter');
    if (reservation.state !== 'reserved') throw new Error('expected reserve');
    let reads = 0;
    const record = {
      get result() {
        reads += 1;
        return reads === 1
          ? { exact: true }
          : new Date('2026-08-12T00:00:00.000Z');
      },
      extra: 'not part of IdempotencyRecord',
    } as unknown as IdempotencyRecord;

    // #when
    await store.put('conn:getter', record, reservation.token);

    // #then — validation and serialization use the same captured result;
    // unrelated structural fields never enter the durable record
    expect(reads).toBe(1);
    expect(await store.get('conn:getter')).toEqual({ result: { exact: true } });
  });

  it('keeps a connector reservation pending when D1 rejects shared references', async () => {
    // #given — JSON duplicates the aliased object and would change identity on
    // replay even though the value remains otherwise JSON-shaped
    const store = new D1IdempotencyStore(d1Like(openSqlite()), {
      now: () => T0,
    });
    const shared = { value: true };
    const result = { first: shared, second: shared };
    const execute = vi.fn(async () => result);
    const tool = createConnector({
      id: 'pay',
      description: 'Perform a payment side effect',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        idempotencyStore: store,
        idempotencyKeyMigration: 'legacy-writers-drained',
      },
    });
    const context = connectorContext('tenant', 'shared-reference');

    // #when — the side effect succeeds but its lossy D1 replay record does not
    // finalize, so an immediate retry remains fail-closed
    const first = await runConnector(tool, context);
    const second = await runConnector(tool, context).catch(
      (error: unknown) => error,
    );

    // #then
    expect(first).toBe(result);
    expect(result.first).toBe(result.second);
    expect(second).toBeInstanceOf(ConnectorPolicyError);
    expect((second as ConnectorPolicyError).policy).toBe('idempotency');
    expect(execute).toHaveBeenCalledTimes(1);
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

  it('atomically migrates an inventoried ambiguous row through the connector output schema', async () => {
    const raw = openSqlite();
    const store = new D1IdempotencyStore(d1Like(raw), { now: () => T0 });
    await store.put('tenant:pay:pay:invoice-1', {
      result: { value: ' legacy ' },
    });
    const execute = vi.fn(async () => ({ value: 'fresh' }));
    const connector = createConnector({
      id: 'pay',
      description: 'Pay one invoice',
      outputSchema: z.object({
        value: z.string().transform((value) => value.trim().toUpperCase()),
      }),
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        idempotencyStore: store,
        idempotencyKeyMigration: 'legacy-writers-drained',
      },
      execute,
    });
    const identity = {
      idempotencyKey: 'pay:invoice-1',
      isolationScope: 'tenant',
    };
    const inventory = await inspectLegacyConnectorIdempotency(
      connector,
      identity,
    );
    if (inventory.state !== 'replay') throw new Error('expected legacy row');

    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        ...identity,
        expectedRecord: inventory.record,
      }),
    ).resolves.toEqual({
      state: 'migrated',
      record: { result: { value: 'LEGACY' } },
    });
    await expect(
      inspectLegacyConnectorIdempotency(connector, identity),
    ).resolves.toEqual({ state: 'absent' });
    await expect(
      runConnector(connector, connectorContext('tenant', 'pay:invoice-1')),
    ).resolves.toEqual({ value: 'LEGACY' });
    expect(execute).not.toHaveBeenCalled();

    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        ...identity,
        expectedRecord: inventory.record,
      }),
    ).resolves.toEqual({
      state: 'already-migrated',
      record: { result: { value: 'LEGACY' } },
    });
  });

  it('leaves pending, mismatched, invalid-output, and conflicting rows fail closed', async () => {
    const raw = openSqlite();
    const store = new D1IdempotencyStore(d1Like(raw), { now: () => T0 });
    const connector = createConnector({
      id: 'pay',
      description: 'Pay one invoice',
      outputSchema: z.object({ ok: z.boolean() }),
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        idempotencyStore: store,
        idempotencyKeyMigration: 'legacy-writers-drained',
      },
      execute: async () => ({ ok: true }),
    });

    await store.reserve('tenant:pay:pending:key');
    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'pending:key',
        isolationScope: 'tenant',
        expectedRecord: { result: { ok: true } },
      }),
    ).resolves.toEqual({ state: 'source-pending' });
    await expect(store.inspect('tenant:pay:pending:key')).resolves.toEqual({
      state: 'pending',
    });

    await store.put('tenant:pay:mismatch:key', { result: { ok: true } });
    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'mismatch:key',
        isolationScope: 'tenant',
        expectedRecord: { result: { ok: false } },
      }),
    ).resolves.toEqual({
      state: 'source-mismatch',
      record: { result: { ok: true } },
    });
    await expect(store.inspect('tenant:pay:mismatch:key')).resolves.toEqual({
      state: 'replay',
      record: { result: { ok: true } },
    });

    await store.put('tenant:pay:invalid:key', {
      result: { ok: 'not-boolean' },
    });
    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'invalid:key',
        isolationScope: 'tenant',
        expectedRecord: { result: { ok: 'not-boolean' } },
      }),
    ).resolves.toMatchObject({ state: 'output-invalid' });
    await expect(
      store.inspect('tenant:pay:invalid:key'),
    ).resolves.toMatchObject({ state: 'replay' });

    await runConnector(connector, connectorContext('tenant', 'conflict:key'));
    await store.put('tenant:pay:conflict:key', { result: { ok: false } });
    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'conflict:key',
        isolationScope: 'tenant',
        expectedRecord: { result: { ok: false } },
      }),
    ).resolves.toEqual({
      state: 'target-conflict',
      target: { state: 'replay', record: { result: { ok: true } } },
    });
    await expect(store.inspect('tenant:pay:conflict:key')).resolves.toEqual({
      state: 'replay',
      record: { result: { ok: false } },
    });
  });

  it('allows only one of two colliding tuple migrations to consume the source row', async () => {
    const raw = openSqlite();
    const store = new D1IdempotencyStore(d1Like(raw), { now: () => T0 });
    await store.put('tenant:pay:pay:invoice-1', {
      result: { source: 'legacy' },
    });
    const connector = createConnector({
      id: 'pay',
      description: 'Pay one invoice',
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        idempotencyStore: store,
        idempotencyKeyMigration: 'legacy-writers-drained',
      },
      execute: async () => ({ source: 'fresh' }),
    });
    const expectedRecord = { result: { source: 'legacy' } };

    const results = await Promise.all([
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'pay:invoice-1',
        isolationScope: 'tenant',
        expectedRecord,
      }),
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'invoice-1',
        isolationScope: 'tenant:pay',
        expectedRecord,
      }),
    ]);

    expect(results.map(({ state }) => state).sort()).toEqual([
      'migrated',
      'source-absent',
    ]);
    expect(
      raw
        .prepare(
          `SELECT count(*) AS count FROM breakwater_idempotency
           WHERE key LIKE 'bw2_i_s_%'`,
        )
        .get(),
    ).toEqual({ count: 1 });
    await expect(store.inspect('tenant:pay:pay:invoice-1')).resolves.toEqual({
      state: 'absent',
    });
  });

  it('rolls back both migration statements when the D1 batch fails', async () => {
    const raw = openSqlite();
    const real = d1Like(raw);
    const failing: IdempotencyBatchDatabase = {
      prepare: (sql) => real.prepare(sql),
      batch: <T>(statements: IdempotencyStatement[]) =>
        real.batch<T>([
          ...statements,
          real.prepare('INSERT INTO missing_migration_table(value) VALUES (1)'),
        ]),
    };
    const store = new D1IdempotencyStore(failing, { now: () => T0 });
    await store.put('tenant:pay:rollback:key', { result: { ok: true } });
    const connector = createConnector({
      id: 'pay',
      description: 'Pay one invoice',
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        idempotencyStore: store,
        idempotencyKeyMigration: 'legacy-writers-drained',
      },
      execute: async () => ({ ok: true }),
    });

    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'rollback:key',
        isolationScope: 'tenant',
        expectedRecord: { result: { ok: true } },
      }),
    ).rejects.toThrow(/missing_migration_table/);
    await expect(store.inspect('tenant:pay:rollback:key')).resolves.toEqual({
      state: 'replay',
      record: { result: { ok: true } },
    });
    expect(
      raw
        .prepare(
          `SELECT count(*) AS count FROM breakwater_idempotency
           WHERE key LIKE 'bw2_i_s_%'`,
        )
        .get(),
    ).toEqual({ count: 0 });
  });

  it('rejects safe identities and prepare-only adapters before migration', async () => {
    const raw = openSqlite();
    const database: IdempotencyDatabase = {
      prepare: (sql) => d1Like(raw).prepare(sql),
    };
    const store = new D1IdempotencyStore(database, { now: () => T0 });
    await store.put('pay:colon:key', { result: { ok: true } });
    const connector = createConnector({
      id: 'pay',
      description: 'Pay one invoice',
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        idempotencyStore: store,
        idempotencyKeyMigration: 'legacy-writers-drained',
      },
      execute: async () => ({ ok: true }),
    });

    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'plain',
        expectedRecord: { result: { ok: true } },
      }),
    ).rejects.toThrow(/scoped or colon-bearing/);
    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'colon:key',
        expectedRecord: { result: { ok: true } },
      }),
    ).rejects.toThrow(/transactional batch/);
    await expect(store.inspect('pay:colon:key')).resolves.toMatchObject({
      state: 'replay',
    });
  });

  it('rejects ambiguous migration until the legacy-writer drain is acknowledged', async () => {
    const raw = openSqlite();
    const store = new D1IdempotencyStore(d1Like(raw), { now: () => T0 });
    await store.put('tenant:pay:invoice:key', { result: { ok: true } });
    const connector = createConnector({
      id: 'pay',
      description: 'Pay one invoice',
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
      execute: async () => ({ ok: true }),
    });

    await expect(
      migrateLegacyConnectorIdempotency(connector, {
        idempotencyKey: 'invoice:key',
        isolationScope: 'tenant',
        expectedRecord: { result: { ok: true } },
      }),
    ).rejects.toThrow(/legacy-writers-drained/);
    await expect(store.inspect('tenant:pay:invoice:key')).resolves.toEqual({
      state: 'replay',
      record: { result: { ok: true } },
    });
  });
});
