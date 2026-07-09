// One behavioral contract, two backends: InMemoryApprovalStore and
// D1ApprovalStore. The D1 store runs against REAL SQLite via node:sqlite
// (D1 is SQLite), so the CAS SQL — status-guarded UPDATE ... RETURNING —
// and the partial unique index are exercised for real, not mocked.
// (workerd-level verification happens in the demo spike, matching the
// Phase-1 precedent for d1-storage.ts.)
//
// The contract runs over FACTORIES, not stores: tenant isolation (INV-2) is
// only real when tenant A's and tenant B's views share ONE backend — two
// independent in-memory stores hold separate Maps and would pass a
// cross-tenant test without exercising any predicate.

import { describe, expect, it } from 'vitest';

import type {
  ApprovalDatabase,
  ApprovalPreparedStatement,
} from './d1-store.js';
import { approvedConnectorsForLeg } from './grants.js';
import type { ApprovalStore } from './store.js';
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
    // The store STAMPS the tenant from its own binding; this field only keeps
    // the literal type-complete (and documents what a read must return).
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

const T = '2026-07-06T12:00:00.000Z';

// --- node:sqlite -> ApprovalDatabase adapter ------------------------------

interface SqliteStatement {
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
}

// process.getBuiltinModule loads the builtin without import machinery, so
// neither vite's resolver (which cannot resolve node:sqlite) nor the
// workers-types tsconfig (no @types/node) ever sees the specifier. Available
// since node 22.3; node:sqlite itself is unflagged since 22.13.
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

function d1Like(db: SqliteDatabase): ApprovalDatabase {
  function statement(
    sql: string,
    params: unknown[],
  ): ApprovalPreparedStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async <T>() =>
        (db.prepare(sql).get(...params) as T | undefined) ?? null,
      run: async () => db.prepare(sql).run(...params),
      all: async <T>() => ({ results: db.prepare(sql).all(...params) as T[] }),
    };
  }
  return { prepare: (sql: string) => statement(sql, []) };
}

// --- shared contract -------------------------------------------------------

function describeStoreContract(
  name: string,
  makeBackend: () => ApprovalStoreFactory,
): void {
  // Single-tenant cases run over the default 'acme' view of a fresh backend.
  const makeStore = (): ApprovalStore => makeBackend().forTenant('acme');
  describe(name, () => {
    it('round-trips a record with every optional field through create/get', async () => {
      // #given
      const store = await makeStore();
      const record = makeRecord({
        stepPath: ['approval'],
        suspendedAt: 1751882400000,
        resumedAt: 1751882460000,
        resumeCount: 2,
        runScoped: true,
        summary: 'publish the launch post',
        payload: { reason: 'human approval required', nested: { n: 1 } },
        connectors: ['blog-publisher', 'mailer'],
        priority: 'high',
        requestedBy: 'system',
        slaDeadlineAt: T,
      });

      // #when
      const created = await store.create(record);

      // #then
      expect(created.created).toBe(true);
      expect(await store.get(record.id)).toEqual(record);
    });

    it('returns null for unknown ids', async () => {
      // #given
      const store = await makeStore();

      // #when / #then
      expect(await store.get('missing')).toBeNull();
    });

    it('collapses a duplicate open create to the existing record', async () => {
      // #given — same (workflowId, runId, stepPath), still open
      const store = await makeStore();
      const first = makeRecord({
        runId: 'run-dup',
        stepPath: ['approval'],
      });
      await store.create(first);

      // #when
      const second = await store.create(
        makeRecord({ runId: 'run-dup', stepPath: ['approval'] }),
      );

      // #then
      expect(second.created).toBe(false);
      expect(second.record.id).toBe(first.id);
    });

    it('opens a fresh request for the same step once the previous one is decided', async () => {
      // #given — a decided request for the step
      const store = await makeStore();
      const first = makeRecord({ runId: 'run-again', stepPath: ['approval'] });
      await store.create(first);
      await store.transition(first.id, ['pending'], {
        status: 'approved',
        updatedAt: T,
      });

      // #when
      const second = await store.create(
        makeRecord({ runId: 'run-again', stepPath: ['approval'] }),
      );

      // #then
      expect(second.created).toBe(true);
      expect(second.record.id).not.toBe(first.id);
    });

    it('keeps distinct steps of the same run independently open', async () => {
      // #given
      const store = await makeStore();
      await store.create(makeRecord({ runId: 'run-multi', stepPath: ['a'] }));

      // #when
      const other = await store.create(
        makeRecord({ runId: 'run-multi', stepPath: ['b'] }),
      );

      // #then
      expect(other.created).toBe(true);
    });

    it('rejects a duplicate id outright', async () => {
      // #given
      const store = await makeStore();
      await store.create(makeRecord({ id: 'fixed-id', runId: 'r1' }));

      // #when / #then — different step key, same primary key: a caller bug,
      // not open-uniqueness
      await expect(
        store.create(makeRecord({ id: 'fixed-id', runId: 'r2' })),
      ).rejects.toThrow(/already exists|UNIQUE constraint failed/);
    });

    it('returns the existing open record even when the duplicate also collides on id', async () => {
      // #given — an open record for the step, plus an unrelated record whose
      // id the duplicate collides with
      const store = await makeStore();
      const open = makeRecord({ runId: 'run-idc', stepPath: ['s'] });
      await store.create(open);
      await store.create(makeRecord({ id: 'taken-id', runId: 'run-other' }));

      // #when — same open step AND a colliding primary key
      const result = await store.create(
        makeRecord({ id: 'taken-id', runId: 'run-idc', stepPath: ['s'] }),
      );

      // #then — open-uniqueness wins identically on both backends
      expect(result.created).toBe(false);
      expect(result.record.id).toBe(open.id);
    });

    it('lists oldest-first and applies filters', async () => {
      // #given
      const store = await makeStore();
      const a = makeRecord({ workflowId: 'wf-1', runId: 'r-a' });
      const b = makeRecord({
        workflowId: 'wf-1',
        runId: 'r-b',
        claimedBy: 'alice',
        status: 'claimed',
      });
      const c = makeRecord({ workflowId: 'wf-2', runId: 'r-c' });
      await store.create(a);
      await store.create(b);
      await store.create(c);

      // #when / #then — createdAt ascending (a before b before c)
      expect((await store.list()).map((r) => r.id)).toEqual([a.id, b.id, c.id]);
      expect(
        (await store.list({ workflowId: 'wf-1' })).map((r) => r.id),
      ).toEqual([a.id, b.id]);
      expect((await store.list({ runId: 'r-c' })).map((r) => r.id)).toEqual([
        c.id,
      ]);
      expect(
        (await store.list({ claimedBy: 'alice' })).map((r) => r.id),
      ).toEqual([b.id]);
      expect(
        (await store.list({ status: 'claimed' })).map((r) => r.id),
      ).toEqual([b.id]);
      expect(
        (await store.list({ status: ['pending', 'claimed'] })).map((r) => r.id),
      ).toEqual([a.id, b.id, c.id]);
    });

    it('applies the patch when the status guard matches', async () => {
      // #given
      const store = await makeStore();
      const record = makeRecord();
      await store.create(record);

      // #when
      const updated = await store.transition(record.id, ['pending'], {
        status: 'claimed',
        claimedBy: 'alice',
        claimedAt: T,
        updatedAt: T,
      });

      // #then — patch applied, untouched fields preserved
      expect(updated).toMatchObject({
        id: record.id,
        status: 'claimed',
        claimedBy: 'alice',
        claimedAt: T,
        updatedAt: T,
        title: record.title,
        createdAt: record.createdAt,
      });
      expect(await store.get(record.id)).toEqual(updated);
    });

    it('returns null and leaves the record untouched when the guard fails', async () => {
      // #given — an approved record
      const store = await makeStore();
      const record = makeRecord();
      await store.create(record);
      await store.transition(record.id, ['pending'], {
        status: 'approved',
        updatedAt: T,
      });

      // #when — a claim races in after the decision
      const result = await store.transition(record.id, ['pending'], {
        status: 'claimed',
        claimedBy: 'bob',
        updatedAt: '2026-07-06T13:00:00.000Z',
      });

      // #then
      expect(result).toBeNull();
      expect(await store.get(record.id)).toMatchObject({
        status: 'approved',
        updatedAt: T,
      });
    });

    it('returns null for transitions on unknown ids', async () => {
      // #given
      const store = await makeStore();

      // #when / #then
      expect(
        await store.transition('missing', ['pending'], { updatedAt: T }),
      ).toBeNull();
    });

    it('resolves racing transitions to exactly one winner', async () => {
      // #given
      const store = await makeStore();
      const record = makeRecord();
      await store.create(record);

      // #when — two concurrent claims CAS from 'pending'
      const outcomes = await Promise.all([
        store.transition(record.id, ['pending'], {
          status: 'claimed',
          claimedBy: 'alice',
          updatedAt: T,
        }),
        store.transition(record.id, ['pending'], {
          status: 'claimed',
          claimedBy: 'bob',
          updatedAt: T,
        }),
      ]);

      // #then — one non-null result; the stored claimer is the winner's
      const winners = outcomes.filter((outcome) => outcome !== null);
      expect(winners).toHaveLength(1);
      const stored = await store.get(record.id);
      expect(stored?.claimedBy).toBe(winners[0]?.claimedBy);
    });

    it('round-trips a payload with unicode, quotes, backslashes, and nested nulls', async () => {
      // #given — the D1 store JSON-encodes the payload; a naive round-trip
      // would corrupt quotes/backslashes/newlines or drop null leaves
      const store = await makeStore();
      const payload = {
        unicode: 'café — 日本語 — 😀',
        quotes: `he said "hi" and 'bye'`,
        backslash: 'a\\b\\c',
        newline: 'line1\nline2\ttab',
        nested: { a: null, b: [1, null, 'x'], c: {}, d: [] },
      };
      const record = makeRecord({ payload });

      // #when
      await store.create(record);

      // #then — deep-equal through the store boundary
      expect((await store.get(record.id))?.payload).toEqual(payload);
    });

    it('preserves a null payload as null, distinct from an absent payload', async () => {
      // #given — null is a valid JSON payload value; the D1 store maps only
      // an *absent* payload to a SQL NULL column (payload === undefined ? null)
      const store = await makeStore();
      const withNull = makeRecord({ id: 'p-null', payload: null });
      const withAbsent = makeRecord({ id: 'p-absent' });
      await store.create(withNull);
      await store.create(withAbsent);

      // #when
      const gotNull = await store.get('p-null');
      const gotAbsent = await store.get('p-absent');

      // #then — the null survives as null; the absent one has no payload key
      expect(gotNull?.payload).toBeNull();
      expect('payload' in (gotNull as object)).toBe(true);
      expect(gotAbsent?.payload).toBeUndefined();
      expect('payload' in (gotAbsent as object)).toBe(false);
    });

    it('treats SQL metacharacters in ids and filter values as literals, not SQL', async () => {
      // #given — a real record, then queries whose values are SQL-injection
      // shaped; if any value were interpolated the table would drop
      const store = await makeStore();
      const record = makeRecord({ runId: 'safe-run' });
      await store.create(record);
      const inject = "'; DROP TABLE flowsafe_approvals; --";

      // #when — malicious id (get + transition) and malicious filter values
      const gotInjected = await store.get(inject);
      const transitioned = await store.transition(inject, ['pending'], {
        status: 'approved',
        updatedAt: T,
      });
      const byInjectWorkflow = await store.list({ workflowId: inject });
      const byInjectRun = await store.list({ runId: "safe-run' OR '1'='1" });
      const byInjectClaimed = await store.list({ claimedBy: inject });

      // #then — every injection is an inert literal that matches nothing, and
      // the store still works (table intact, original record retrievable)
      expect(gotInjected).toBeNull();
      expect(transitioned).toBeNull();
      expect(byInjectWorkflow).toEqual([]);
      expect(byInjectRun).toEqual([]);
      expect(byInjectClaimed).toEqual([]);
      expect(await store.get(record.id)).toMatchObject({ runId: 'safe-run' });
    });

    // ---- tenant isolation (INV-2), over ONE shared backend ----------------

    it("cross-tenant get/transition return null — B cannot read or move A's record", async () => {
      // #given — tenant A and tenant B views over the SAME backend
      const backend = makeBackend();
      const storeA = backend.forTenant('acme');
      const storeB = backend.forTenant('bravo');
      const record = makeRecord({ runId: 'acme_r1' });
      await storeA.create(record);

      // #when / #then — a wrong-tenant id behaves exactly like an unknown id
      expect(await storeB.get(record.id)).toBeNull();
      expect(
        await storeB.transition(record.id, ['pending'], {
          status: 'approved',
          updatedAt: T,
        }),
      ).toBeNull();
      // and A's record is untouched
      expect(await storeA.get(record.id)).toMatchObject({ status: 'pending' });
    });

    it("an EMPTY filter lists only the bound tenant's rows — the canonical fail-open, closed", async () => {
      // #given
      const backend = makeBackend();
      const storeA = backend.forTenant('acme');
      const storeB = backend.forTenant('bravo');
      const a = makeRecord({ runId: 'acme_r1' });
      const b = makeRecord({ runId: 'bravo_r1' });
      await storeA.create(a);
      await storeB.create(b);

      // #when / #then — list() with NO filter is a one-tenant scan
      expect((await storeA.list()).map((r) => r.id)).toEqual([a.id]);
      expect((await storeB.list()).map((r) => r.id)).toEqual([b.id]);
    });

    it("open-uniqueness is PER TENANT: B's create for A's open (wf, run, step) is a fresh record", async () => {
      // #given — A holds an open record for (wf, run-shared, gate). The old
      // 3-column unique index would collapse B's create into A's record —
      // handing B a reference to A's approval.
      const backend = makeBackend();
      const storeA = backend.forTenant('acme');
      const storeB = backend.forTenant('bravo');
      const a = makeRecord({ runId: 'run-shared', stepPath: ['gate'] });
      await storeA.create(a);

      // #when
      const b = await storeB.create(
        makeRecord({ runId: 'run-shared', stepPath: ['gate'] }),
      );

      // #then — created under B, not collapsed into A's
      expect(b.created).toBe(true);
      expect(b.record.id).not.toBe(a.id);
      expect(b.record.tenantId).toBe('bravo');
    });

    it('create() STAMPS the binding tenant — a spoofed record.tenantId cannot cross tenants', async () => {
      // #given — a record claiming tenant 'evil' handed to the acme-bound store
      const backend = makeBackend();
      const storeA = backend.forTenant('acme');
      const spoofed = makeRecord({ tenantId: 'evil' });

      // #when
      const created = await storeA.create(spoofed);

      // #then — stored under the BINDING, readable by acme, invisible to evil
      expect(created.record.tenantId).toBe('acme');
      expect((await storeA.get(spoofed.id))?.tenantId).toBe('acme');
      expect(await backend.forTenant('evil').get(spoofed.id)).toBeNull();
    });

    it('the system view sees every tenant (the one legitimate cross-tenant read, cron-only by type)', async () => {
      // #given
      const backend = makeBackend();
      await backend.forTenant('acme').create(makeRecord({ runId: 'acme_r' }));
      await backend.forTenant('bravo').create(makeRecord({ runId: 'bravo_r' }));

      // #when / #then
      const all = await backend.system().list();
      expect(all.map((r) => r.tenantId).sort()).toEqual(['acme', 'bravo']);
    });
  });
}

describeStoreContract(
  'InMemoryApprovalStore (via InMemoryApprovalStoreFactory)',
  () => new InMemoryApprovalStoreFactory(),
);

describeStoreContract(
  'D1ApprovalStore (real SQLite via node:sqlite, via D1ApprovalStoreFactory)',
  () => new D1ApprovalStoreFactory(d1Like(openSqlite())),
);

describe('D1ApprovalStore schema upgrade', () => {
  // The pre-tenant column set — what the 31 local .wrangler databases and any
  // pre-multi-tenant release carry. There is deliberately NO upgrade path:
  // tenant_id is TEXT NOT NULL with no legitimate backfill value (SQLite
  // rejects ADD COLUMN ... NOT NULL without a default, and a NULL/'' tenant
  // is an isolation hole), so the store must REFUSE to serve the table.
  const PRE_TENANT_TABLE_DDL = `CREATE TABLE flowsafe_approvals (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    step_key TEXT NOT NULL DEFAULT '',
    step_path TEXT,
    suspended_at INTEGER,
    resumed_at INTEGER,
    resume_count INTEGER,
    run_scoped INTEGER,
    title TEXT NOT NULL,
    summary TEXT,
    payload TEXT,
    connectors TEXT NOT NULL DEFAULT '[]',
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT,
    claimed_by TEXT,
    decided_by TEXT,
    decision TEXT,
    comment TEXT,
    delegated_to TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    claimed_at TEXT,
    decided_at TEXT,
    escalated_at TEXT,
    sla_deadline_at TEXT
  )`;

  // A tenant-ful table missing the nullable INTEGER columns — the only shape
  // the defensive ALTER loop still upgrades in place.
  const TENANTED_MINUS_INTEGERS_DDL = `CREATE TABLE flowsafe_approvals (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    step_key TEXT NOT NULL DEFAULT '',
    step_path TEXT,
    title TEXT NOT NULL,
    summary TEXT,
    payload TEXT,
    connectors TEXT NOT NULL DEFAULT '[]',
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT,
    claimed_by TEXT,
    decided_by TEXT,
    decision TEXT,
    comment TEXT,
    delegated_to TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    claimed_at TEXT,
    decided_at TEXT,
    escalated_at TEXT,
    sla_deadline_at TEXT
  )`;

  it('REFUSES a pre-tenant table with a loud error naming the fix (fail closed, no backfill)', async () => {
    // #given — a database created before the tenant column existed
    const sqlite = openSqlite();
    sqlite.prepare(PRE_TENANT_TABLE_DDL).run();

    // #when / #then — every operation refuses; nothing silently serves a
    // tenant-less approvals table
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).forTenant('acme');
    await expect(store.get('anything')).rejects.toThrow(/tenant_id/);
    await expect(store.list()).rejects.toThrow(/tenant_id/);
    await expect(store.create(makeRecord())).rejects.toThrow(/tenant_id/);
  });

  it('DROPS the old-name open-step index — the silent-no-op redefinition trap (B1)', async () => {
    // #given — a fresh tenant-ful table on which someone (an intermediate
    // build, a hand migration) created the OLD 3-column unique index under
    // the OLD name. `CREATE UNIQUE INDEX IF NOT EXISTS` matches on NAME
    // alone, so WITHOUT the explicit DROP the tenant-less shape would
    // silently survive and collapse tenant B\'s create into tenant A\'s open
    // record.
    const sqlite = openSqlite();
    sqlite.prepare(TENANTED_MINUS_INTEGERS_DDL).run();
    sqlite
      .prepare(
        `CREATE UNIQUE INDEX IF NOT EXISTS flowsafe_approvals_open_step
         ON flowsafe_approvals (workflow_id, run_id, step_key)
         WHERE status IN ('pending', 'claimed', 'escalated')`,
      )
      .run();
    const backend = new D1ApprovalStoreFactory(d1Like(sqlite));

    // #when — two tenants create the SAME (workflowId, runId, stepKey)
    const a = await backend
      .forTenant('acme')
      .create(makeRecord({ runId: 'run-shared', stepPath: ['gate'] }));
    const b = await backend
      .forTenant('bravo')
      .create(makeRecord({ runId: 'run-shared', stepPath: ['gate'] }));

    // #then — both create (the old index is gone; the v2 index is
    // tenant-first), and the old-name index no longer exists
    expect(a.created).toBe(true);
    expect(b.created).toBe(true);
    const indexes = sqlite
      .prepare(
        `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='flowsafe_approvals' AND sql IS NOT NULL`,
      )
      .all() as Array<{ name: string }>;
    const names = indexes.map((row) => row.name);
    expect(names).not.toContain('flowsafe_approvals_open_step');
    expect(names).toContain('flowsafe_approvals_open_step_v2');
  });

  it('still backfills the nullable INTEGER columns onto a tenant-ful table missing them', async () => {
    // #given — tenant_id present, suspended_at/resumed_at/resume_count/
    // run_scoped absent (the one in-place upgrade that remains legal)
    const sqlite = openSqlite();
    sqlite.prepare(TENANTED_MINUS_INTEGERS_DDL).run();
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).forTenant('acme');

    // #when — a fresh record round-trips all four backfilled columns
    const fresh = makeRecord({
      stepPath: ['gate'],
      suspendedAt: 1751882400000,
      resumedAt: 1751882460000,
      resumeCount: 2,
      runScoped: true,
    });
    await store.create(fresh);
    const readBack = await store.get(fresh.id);

    // #then
    expect(readBack?.suspendedAt).toBe(1751882400000);
    expect(readBack?.resumedAt).toBe(1751882460000);
    expect(readBack?.resumeCount).toBe(2);
    expect(readBack?.runScoped).toBe(true);
  });

  it('a legacy step-less approval (no run_scoped) mints NOTHING on the upgraded table', async () => {
    // #given — a tenant-ful pre-run_scoped table holding exactly the record
    // the OLD grant rule treated as a run-wide standing grant
    const sqlite = openSqlite();
    sqlite.prepare(TENANTED_MINUS_INTEGERS_DDL).run();
    sqlite
      .prepare(
        `INSERT INTO flowsafe_approvals
         (id, tenant_id, workflow_id, run_id, step_key, step_path, title,
          connectors, priority, status, decided_at, created_at, updated_at)
         VALUES ('legacy-1', 'acme', 'wf', 'run-legacy', '', NULL,
                 'legacy standing grant', '["release-deploy"]', 'normal',
                 'approved', ?, ?, ?)`,
      )
      .run(T, T, T);
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).forTenant('acme');

    // #when / #then — runScoped backfills to NULL => denies on every leg
    const legacy = await store.get('legacy-1');
    expect(legacy?.runScoped).toBeUndefined();
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-legacy', {
        kind: 'start',
      }),
    ).toEqual([]);
  });
});
