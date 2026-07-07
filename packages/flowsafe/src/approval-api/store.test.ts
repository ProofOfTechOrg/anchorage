// One behavioral contract, two backends: InMemoryApprovalStore and
// D1ApprovalStore. The D1 store runs against REAL SQLite via node:sqlite
// (D1 is SQLite), so the CAS SQL — status-guarded UPDATE ... RETURNING —
// and the partial unique index are exercised for real, not mocked.
// (workerd-level verification happens in the demo spike, matching the
// Phase-1 precedent for d1-storage.ts.)

import { describe, expect, it } from 'vitest';

import {
  type ApprovalDatabase,
  type ApprovalPreparedStatement,
  D1ApprovalStore,
} from './d1-store.js';
import { type ApprovalStore, InMemoryApprovalStore } from './store.js';
import type { ApprovalRecord } from './types.js';

let seq = 0;

function makeRecord(overrides: Partial<ApprovalRecord> = {}): ApprovalRecord {
  seq += 1;
  const at = new Date(1700000000000 + seq * 1000).toISOString();
  return {
    id: `apr-${seq}`,
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
  makeStore: () => Promise<ApprovalStore> | ApprovalStore,
): void {
  describe(name, () => {
    it('round-trips a record with every optional field through create/get', async () => {
      // #given
      const store = await makeStore();
      const record = makeRecord({
        stepPath: ['approval'],
        suspendedAt: 1751882400000,
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
  });
}

describeStoreContract(
  'InMemoryApprovalStore',
  () => new InMemoryApprovalStore(),
);

describeStoreContract(
  'D1ApprovalStore (real SQLite via node:sqlite)',
  () => new D1ApprovalStore(d1Like(openSqlite())),
);

describe('D1ApprovalStore schema upgrade', () => {
  // The pre-1.0 spike-era column set — no suspended_at. Mirrors the shipped
  // SCHEMA_STATEMENTS as of the Phase 3 release.
  const LEGACY_TABLE_DDL = `CREATE TABLE flowsafe_approvals (
    id TEXT PRIMARY KEY,
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

  it('backfills suspended_at onto a pre-1.0 table and keeps legacy rows fallback-scoped', async () => {
    // #given — a spike database created BEFORE the suspended_at column
    // existed, already holding a decided legacy row
    const sqlite = openSqlite();
    sqlite.prepare(LEGACY_TABLE_DDL).run();
    sqlite
      .prepare(
        `INSERT INTO flowsafe_approvals
         (id, workflow_id, run_id, step_key, step_path, title, priority,
          status, decided_at, created_at, updated_at)
         VALUES ('legacy-1', 'wf', 'run-legacy', 'gate', '["gate"]',
                 'legacy approval', 'normal', 'approved', ?, ?, ?)`,
      )
      .run(T, T, T);

    // #when — instantiating the store against it triggers the defensive
    // ALTER (CREATE IF NOT EXISTS skips the existing table)
    const store = new D1ApprovalStore(d1Like(sqlite));
    const legacy = await store.get('legacy-1');

    // #then — the legacy row reads back with NO suspendedAt, so grant
    // minting falls to the legacy decidedAt-after comparison for it...
    expect(legacy).toMatchObject({ id: 'legacy-1', status: 'approved' });
    expect(legacy?.suspendedAt).toBeUndefined();

    // ...and a fresh record round-trips the backfilled column
    const fresh = makeRecord({
      stepPath: ['gate'],
      suspendedAt: 1751882400000,
    });
    await store.create(fresh);
    expect((await store.get(fresh.id))?.suspendedAt).toBe(1751882400000);
  });
});
