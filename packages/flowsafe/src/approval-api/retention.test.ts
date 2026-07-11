// SPDX-License-Identifier: Apache-2.0
// purgeExpiredApprovals — the approvals analog of do-runner's
// purgeExpiredWorkflowRuns test coverage: terminal-only, the exclusive TTL
// boundary, LIMIT-batching, and cross-tenant reach (the system view, NOT a
// tenant-bound store — mirrors store.test.ts's shared dual-backend contract
// suite). The free function's own clock/cutoff/default-limit math is
// exercised separately over a spy store.

import { describe, expect, it } from 'vitest';

import { openSqlite, type SqliteDatabase } from '../../test-support/sqlite.js';
import type {
  ApprovalDatabase,
  ApprovalPreparedStatement,
} from './d1-store.js';
import { purgeExpiredApprovals } from './retention.js';
import type { SystemApprovalStore } from './tenant-brand.js';
import {
  type ApprovalStoreFactory,
  D1ApprovalStoreFactory,
  InMemoryApprovalStoreFactory,
} from './tenant-store.js';
import type { ApprovalRecord } from './types.js';

let seq = 0;

function makeRecord(overrides: Partial<ApprovalRecord> = {}): ApprovalRecord {
  seq += 1;
  const at = new Date(1700000000000 + seq * 1000).toISOString();
  return {
    id: `apr-${seq}`,
    tenantId: 'acme',
    workflowId: 'wf',
    runId: `run-${seq}`,
    title: `approval ${seq}`,
    connectors: [],
    priority: 'normal',
    status: 'pending',
    createdAt: at,
    updatedAt: at,
    ...overrides,
  };
}

// --- node:sqlite -> ApprovalDatabase adapter (mirrors store.test.ts) ------

function d1Like(db: SqliteDatabase): ApprovalDatabase {
  function statement(
    sql: string,
    params: unknown[],
  ): ApprovalPreparedStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async <T>() =>
        (db.prepare(sql).get(...params) as T | undefined) ?? null,
      // D1-shaped envelope: purgeExpired's d1Changes() reads result.meta.changes
      // (real D1's run() shape) — the raw node:sqlite {changes} would silently
      // read as 0 through d1Changes' optional-chain fallback.
      run: async () => {
        const outcome = db.prepare(sql).run(...params) as {
          changes?: number | bigint;
        };
        return { meta: { changes: Number(outcome?.changes ?? 0) } };
      },
      all: async <T>() => ({ results: db.prepare(sql).all(...params) as T[] }),
    };
  }
  return { prepare: (sql: string) => statement(sql, []) };
}

const OLD = '2020-01-01T00:00:00.000Z';
const CUTOFF_NOW = Date.parse('2026-01-01T00:00:00.000Z');
const TTL_MS = 24 * 60 * 60 * 1000;

function describePurgeContract(
  name: string,
  makeBackend: () => ApprovalStoreFactory,
): void {
  describe(name, () => {
    it('purges only DECIDED (approved/rejected) records; open requests survive at any age', async () => {
      // #given — an old approved and an old rejected record, plus old
      // pending/claimed/escalated records
      const backend = makeBackend();
      const store = backend.forTenant('acme');
      const approved = await store.create(
        makeRecord({ status: 'approved', decidedAt: OLD, updatedAt: OLD }),
      );
      const rejected = await store.create(
        makeRecord({ status: 'rejected', decidedAt: OLD, updatedAt: OLD }),
      );
      const pending = await store.create(
        makeRecord({ status: 'pending', updatedAt: OLD }),
      );
      const claimed = await store.create(
        makeRecord({ status: 'claimed', updatedAt: OLD, claimedBy: 'ray' }),
      );
      const escalated = await store.create(
        makeRecord({ status: 'escalated', updatedAt: OLD, escalatedAt: OLD }),
      );

      // #when
      const purged = await purgeExpiredApprovals(backend.system(), {
        ttlMs: TTL_MS,
        now: () => CUTOFF_NOW,
      });

      // #then
      expect(purged).toBe(2);
      expect(await store.get(approved.record.id)).toBeNull();
      expect(await store.get(rejected.record.id)).toBeNull();
      expect(await store.get(pending.record.id)).not.toBeNull();
      expect(await store.get(claimed.record.id)).not.toBeNull();
      expect(await store.get(escalated.record.id)).not.toBeNull();
    });

    it('falls back to updatedAt for a decided record persisted without decidedAt', async () => {
      // #given — approved, no decidedAt (a direct-store write outside
      // service.decide(), which always sets both together)
      const backend = makeBackend();
      const store = backend.forTenant('acme');
      const created = await store.create(
        makeRecord({ status: 'approved', updatedAt: OLD }),
      );
      expect(created.record.decidedAt).toBeUndefined();

      // #when
      const purged = await purgeExpiredApprovals(backend.system(), {
        ttlMs: TTL_MS,
        now: () => CUTOFF_NOW,
      });

      // #then
      expect(purged).toBe(1);
      expect(await store.get(created.record.id)).toBeNull();
    });

    it('treats the TTL boundary exclusively: exactly-at-cutoff records survive', async () => {
      // #given — decidedAt lands EXACTLY on the computed cutoff instant
      const backend = makeBackend();
      const store = backend.forTenant('acme');
      const cutoffInstant = new Date(CUTOFF_NOW - TTL_MS).toISOString();
      const created = await store.create(
        makeRecord({
          status: 'approved',
          decidedAt: cutoffInstant,
          updatedAt: cutoffInstant,
        }),
      );

      // #when
      const purged = await purgeExpiredApprovals(backend.system(), {
        ttlMs: TTL_MS,
        now: () => CUTOFF_NOW,
      });

      // #then — strict "<", so the boundary record is not yet expired
      expect(purged).toBe(0);
      expect(await store.get(created.record.id)).not.toBeNull();
    });

    it('LIMIT-batches: one firing purges at most `limit` records; the next resumes at the survivors', async () => {
      // #given — three old decided records, batch size 2
      const backend = makeBackend();
      const store = backend.forTenant('acme');
      const created = await Promise.all(
        [0, 1, 2].map(() =>
          store.create(
            makeRecord({ status: 'approved', decidedAt: OLD, updatedAt: OLD }),
          ),
        ),
      );
      const options = { ttlMs: TTL_MS, now: () => CUTOFF_NOW, limit: 2 };

      // #when — two firings
      const first = await purgeExpiredApprovals(backend.system(), options);
      const second = await purgeExpiredApprovals(backend.system(), options);

      // #then — the shrinking eligible set is the cursor across firings
      expect(first).toBe(2);
      expect(second).toBe(1);
      for (const { record } of created) {
        expect(await store.get(record.id)).toBeNull();
      }
    });

    it('rejects a negative limit before it could ever reach the store (pins D1 can no longer reach LIMIT -1)', async () => {
      // #given
      const backend = makeBackend();

      // #when / #then
      await expect(
        purgeExpiredApprovals(backend.system(), {
          ttlMs: TTL_MS,
          now: () => CUTOFF_NOW,
          limit: -1,
        }),
      ).rejects.toThrow(TypeError);
    });

    it('rejects a negative ttlMs (a future cutoff would purge just-decided records)', async () => {
      // #given
      const backend = makeBackend();

      // #when / #then
      await expect(
        purgeExpiredApprovals(backend.system(), {
          ttlMs: -1,
          now: () => CUTOFF_NOW,
        }),
      ).rejects.toThrow(TypeError);
    });

    it('rejects NaN and Infinity for ttlMs — the guard is finiteness, not just sign (pins against an x <= 0 "simplification")', async () => {
      // #given
      const backend = makeBackend();

      // #when / #then — NaN compares false to everything, so a sign-only
      // guard would wave it through; Infinity is a real env-var product
      // (APPROVAL_RETENTION_DAYS=1e303 overflows the ms multiply)
      await expect(
        purgeExpiredApprovals(backend.system(), {
          ttlMs: Number.NaN,
          now: () => CUTOFF_NOW,
        }),
      ).rejects.toThrow(TypeError);
      await expect(
        purgeExpiredApprovals(backend.system(), {
          ttlMs: Number.POSITIVE_INFINITY,
          now: () => CUTOFF_NOW,
        }),
      ).rejects.toThrow(TypeError);
    });

    it('rejects NaN and Infinity for limit the same way', async () => {
      // #given
      const backend = makeBackend();

      // #when / #then
      await expect(
        purgeExpiredApprovals(backend.system(), {
          ttlMs: TTL_MS,
          now: () => CUTOFF_NOW,
          limit: Number.NaN,
        }),
      ).rejects.toThrow(TypeError);
      await expect(
        purgeExpiredApprovals(backend.system(), {
          ttlMs: TTL_MS,
          now: () => CUTOFF_NOW,
          limit: Number.POSITIVE_INFINITY,
        }),
      ).rejects.toThrow(TypeError);
    });

    it('still treats limit: 0 (no-op) and ttlMs: 0 (purge decided now) as sane', async () => {
      // #given — an old decided record
      const backend = makeBackend();
      const store = backend.forTenant('acme');
      const created = await store.create(
        makeRecord({ status: 'approved', decidedAt: OLD, updatedAt: OLD }),
      );

      // #when / #then — limit: 0 purges nothing...
      const zeroLimit = await purgeExpiredApprovals(backend.system(), {
        ttlMs: TTL_MS,
        now: () => CUTOFF_NOW,
        limit: 0,
      });
      expect(zeroLimit).toBe(0);
      expect(await store.get(created.record.id)).not.toBeNull();

      // ...and ttlMs: 0 purges everything decided up to "now"
      const zeroTtl = await purgeExpiredApprovals(backend.system(), {
        ttlMs: 0,
        now: () => CUTOFF_NOW,
      });
      expect(zeroTtl).toBe(1);
      expect(await store.get(created.record.id)).toBeNull();
    });

    it('reaches ACROSS TENANTS — the system view is not tenant-scoped', async () => {
      // #given — an old approved record under acme AND under bravo
      const backend = makeBackend();
      const storeA = backend.forTenant('acme');
      const storeB = backend.forTenant('bravo');
      const a = await storeA.create(
        makeRecord({
          status: 'approved',
          decidedAt: OLD,
          updatedAt: OLD,
          runId: 'acme_r1',
        }),
      );
      const b = await storeB.create(
        makeRecord({
          status: 'approved',
          decidedAt: OLD,
          updatedAt: OLD,
          runId: 'bravo_r1',
        }),
      );

      // #when — one call
      const purged = await purgeExpiredApprovals(backend.system(), {
        ttlMs: TTL_MS,
        now: () => CUTOFF_NOW,
      });

      // #then — both tenants' eligible records are gone
      expect(purged).toBe(2);
      expect(await storeA.get(a.record.id)).toBeNull();
      expect(await storeB.get(b.record.id)).toBeNull();
    });
  });
}

describePurgeContract(
  'InMemoryApprovalStoreFactory.system().purgeExpired',
  () => new InMemoryApprovalStoreFactory(),
);

describePurgeContract(
  'D1ApprovalStore system().purgeExpired (real SQLite via node:sqlite)',
  () => new D1ApprovalStoreFactory(d1Like(openSqlite())),
);

describe('purgeExpiredApprovals (free function)', () => {
  function fakeSystemStore(): SystemApprovalStore & {
    calls: Array<{ cutoffIso: string; limit: number }>;
  } {
    const calls: Array<{ cutoffIso: string; limit: number }> = [];
    return {
      calls,
      list: async () => [],
      transition: async () => null,
      purgeExpired: async (cutoffIso, limit) => {
        calls.push({ cutoffIso, limit });
        return 3;
      },
    };
  }

  it('converts ttlMs + the injected clock into an ISO cutoff and returns the store count', async () => {
    // #given
    const store = fakeSystemStore();

    // #when
    const purged = await purgeExpiredApprovals(store, {
      ttlMs: 60_000,
      now: () => Date.parse('2026-01-01T00:01:00.000Z'),
    });

    // #then
    expect(purged).toBe(3);
    expect(store.calls).toEqual([
      { cutoffIso: '2026-01-01T00:00:00.000Z', limit: 1000 },
    ]);
  });

  it('defaults the limit to 1000', async () => {
    // #given
    const store = fakeSystemStore();

    // #when
    await purgeExpiredApprovals(store, { ttlMs: 0, now: () => 0 });

    // #then
    expect(store.calls[0]?.limit).toBe(1000);
  });

  it('forwards an explicit limit', async () => {
    // #given
    const store = fakeSystemStore();

    // #when
    await purgeExpiredApprovals(store, { ttlMs: 0, now: () => 0, limit: 25 });

    // #then
    expect(store.calls[0]?.limit).toBe(25);
  });

  it('defaults now to the real clock when unset', async () => {
    // #given
    const store = fakeSystemStore();
    const before = Date.now();

    // #when
    await purgeExpiredApprovals(store, { ttlMs: 0 });

    // #then — the cutoff is a real, recent timestamp
    const cutoffMs = Date.parse(store.calls[0]?.cutoffIso ?? '');
    expect(cutoffMs).toBeGreaterThanOrEqual(before);
  });
});
