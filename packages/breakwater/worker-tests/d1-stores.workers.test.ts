// SPDX-License-Identifier: Apache-2.0
// Runtime-fidelity coverage for Breakwater's structural D1 stores. These tests
// run inside workerd and use the Wrangler-configured D1 binding directly; the
// node:sqlite suites remain fast facsimile unit tests, not Worker/D1 evidence.

import { env } from 'cloudflare:workers';
import { describe, expect, it } from 'vitest';

import { D1IdempotencyStore } from '../src/connector-sdk/d1-idempotency-store.js';
import { D1RateLimitStore } from '../src/connector-sdk/d1-rate-limit-store.js';
import {
  idempotencyStorageKey,
  legacyIdempotencyStorageKey,
} from '../src/connector-sdk/idempotency-key.js';
import { ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION } from '../src/connector-sdk/idempotency-migration.js';

const T0 = Date.parse('2026-08-10T10:00:00.000Z');
const MINUTE = 60_000;

describe('D1IdempotencyStore in workerd', () => {
  it('admits one claimant under concurrent D1 reservations and replays its result', async () => {
    const table = 'breakwater_idem_workerd_claim';
    const key = 'connector:shared-claim';
    const stores = Array.from(
      { length: 16 },
      () => new D1IdempotencyStore(env.DB, { table, now: () => T0 }),
    );
    for (const [index, store] of stores.entries()) {
      await store.get(`warm:${index}`);
    }

    const reservations = await Promise.all(
      stores.map((store) => store.reserve(key)),
    );
    const winners = reservations
      .map((reservation, index) => ({ reservation, index }))
      .filter(({ reservation }) => reservation.state === 'reserved');

    expect(winners).toHaveLength(1);
    expect(
      reservations.filter((reservation) => reservation.state === 'pending'),
    ).toHaveLength(stores.length - 1);

    const winner = winners[0];
    if (winner?.reservation.state !== 'reserved') {
      throw new Error('expected exactly one reserved result');
    }
    await stores[winner.index]?.put(
      key,
      { result: { source: winner.index } },
      winner.reservation.token,
    );

    await expect(
      stores[(winner.index + 1) % stores.length]?.reserve(key),
    ).resolves.toEqual({
      state: 'replay',
      record: { result: { source: winner.index } },
    });
    await expect(
      env.DB.prepare(`SELECT state FROM ${table} WHERE key = ?`)
        .bind(key)
        .first<{ state: string }>(),
    ).resolves.toEqual({ state: 'done' });
  });

  it('allows one stale-lease takeover and rejects the original holder finalization', async () => {
    const table = 'breakwater_idem_workerd_takeover';
    const key = 'connector:stale-claim';
    let now = T0;
    const clock = () => now;
    const owner = new D1IdempotencyStore(env.DB, {
      table,
      now: clock,
      pendingTtlMs: MINUTE,
    });
    const contenders = Array.from(
      { length: 12 },
      () =>
        new D1IdempotencyStore(env.DB, {
          table,
          now: clock,
          pendingTtlMs: MINUTE,
        }),
    );
    await owner.get('warm:owner');
    for (const [index, store] of contenders.entries()) {
      await store.get(`warm:${index}`);
    }
    const original = await owner.reserve(key);
    if (original.state !== 'reserved') {
      throw new Error('expected the original reservation');
    }

    now = T0 + MINUTE + 1;
    const takeovers = await Promise.all(
      contenders.map((store) => store.reserve(key)),
    );
    const winners = takeovers
      .map((reservation, index) => ({ reservation, index }))
      .filter(({ reservation }) => reservation.state === 'reserved');

    expect(winners).toHaveLength(1);
    expect(winners[0]?.reservation).toEqual({
      state: 'reserved',
      token: expect.any(String),
      tookOver: true,
    });
    const winner = winners[0];
    if (winner?.reservation.state !== 'reserved') {
      throw new Error('expected exactly one takeover winner');
    }
    expect(winner.reservation.token).not.toBe(original.token);

    await owner.release(key, original.token);
    await owner.put(key, { result: 'stale' }, original.token);
    expect(await owner.get(key)).toBeUndefined();

    await contenders[winner.index]?.put(
      key,
      { result: 'winner' },
      winner.reservation.token,
    );
    expect(await owner.get(key)).toEqual({ result: 'winner' });
  });

  it('atomically moves one ambiguous legacy row and makes retry idempotent', async () => {
    const table = 'breakwater_idem_workerd_migration';
    const store = new D1IdempotencyStore(env.DB, {
      table,
      now: () => T0,
    });
    const identity = {
      connectorId: 'pay',
      idempotencyKey: 'invoice:1',
      isolationScope: 'tenant',
    };
    await store.put(legacyIdempotencyStorageKey(identity), {
      result: { status: 'legacy' },
    });
    const request = {
      sourceKey: legacyIdempotencyStorageKey(identity),
      targetKey: idempotencyStorageKey(identity),
      expectedRecord: { result: { status: 'legacy' } },
      targetRecord: { result: { status: 'legacy' } },
    };

    await expect(
      store[ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION](request),
    ).resolves.toEqual({
      state: 'migrated',
      record: { result: { status: 'legacy' } },
    });
    await expect(
      store[ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION](request),
    ).resolves.toEqual({
      state: 'already-migrated',
      record: { result: { status: 'legacy' } },
    });
    const { results } = await env.DB.prepare(
      `SELECT key, state FROM ${table} ORDER BY key`,
    ).all<{ key: string; state: string }>();
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({
      key: expect.stringMatching(/^bw2_i_s_/),
      state: 'done',
    });
  });

  it('lets only one colliding tuple consume an ambiguous legacy row', async () => {
    const table = 'breakwater_idem_workerd_migration_collision';
    const store = new D1IdempotencyStore(env.DB, {
      table,
      now: () => T0,
    });
    const firstIdentity = {
      connectorId: 'pay',
      idempotencyKey: 'pay:invoice-1',
      isolationScope: 'tenant',
    };
    const secondIdentity = {
      connectorId: 'pay',
      idempotencyKey: 'invoice-1',
      isolationScope: 'tenant:pay',
    };
    await store.put(legacyIdempotencyStorageKey(firstIdentity), {
      result: { source: 'legacy' },
    });
    const expectedRecord = { result: { source: 'legacy' } };

    const outcomes = await Promise.all([
      store[ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION]({
        sourceKey: legacyIdempotencyStorageKey(firstIdentity),
        targetKey: idempotencyStorageKey(firstIdentity),
        expectedRecord,
        targetRecord: expectedRecord,
      }),
      store[ATOMIC_LEGACY_IDEMPOTENCY_MIGRATION]({
        sourceKey: legacyIdempotencyStorageKey(secondIdentity),
        targetKey: idempotencyStorageKey(secondIdentity),
        expectedRecord,
        targetRecord: expectedRecord,
      }),
    ]);

    expect(outcomes.map(({ state }) => state).sort()).toEqual([
      'migrated',
      'source-absent',
    ]);
    const row = await env.DB.prepare(
      `SELECT count(*) AS count FROM ${table} WHERE key LIKE 'bw2_i_s_%'`,
    ).first<{ count: number }>();
    expect(row).toEqual({ count: 1 });
  });
});

describe('D1RateLimitStore in workerd', () => {
  it('returns every atomic post-increment count under concurrent D1 writes', async () => {
    const table = 'breakwater_rate_workerd_concurrent';
    const key = 'tenant:connector';
    const stores = Array.from(
      { length: 32 },
      () => new D1RateLimitStore(env.DB, { table }),
    );
    for (const [index, store] of stores.entries()) {
      await store.increment(`warm:${index}`, MINUTE, T0);
    }

    const counts = await Promise.all(
      stores.map((store) => store.increment(key, MINUTE, T0 + 1)),
    );

    expect([...counts].sort((a, b) => a - b)).toEqual(
      Array.from({ length: stores.length }, (_, index) => index + 1),
    );
    await expect(
      env.DB.prepare(
        `SELECT count FROM ${table} WHERE budget_key = ? AND window_start = ?`,
      )
        .bind(key, T0)
        .first<{ count: number }>(),
    ).resolves.toEqual({ count: stores.length });
  });

  it('resets the fixed window and removes the expired D1 row', async () => {
    const table = 'breakwater_rate_workerd_rollover';
    const key = 'tenant:rollover';
    const store = new D1RateLimitStore(env.DB, { table });
    await store.increment(key, MINUTE, T0);
    await store.increment(key, MINUTE, T0 + 1_000);

    expect(await store.increment(key, MINUTE, T0 + MINUTE)).toBe(1);
    const { results } = await env.DB.prepare(
      `SELECT window_start, count FROM ${table}
       WHERE budget_key = ? ORDER BY window_start`,
    )
      .bind(key)
      .all<{ window_start: number; count: number }>();

    expect(results).toEqual([{ window_start: T0 + MINUTE, count: 1 }]);
  });
});
