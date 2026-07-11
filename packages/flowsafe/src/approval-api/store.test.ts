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

import { openSqlite, type SqliteDatabase } from '../../test-support/sqlite.js';
import type {
  ApprovalDatabase,
  ApprovalPreparedStatement,
} from './d1-store.js';
import { approvedConnectorsForLeg } from './grants.js';
import type { ApprovalStore } from './store.js';
import { computeApprovalMetrics } from './store.js';
import {
  type ApprovalStoreFactory,
  D1ApprovalStoreFactory,
  InMemoryApprovalStoreFactory,
} from './tenant-store.js';
import type { ApprovalRecord } from './types.js';
import { approvalCursor, MAX_APPROVAL_LIST_LIMIT } from './types.js';

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

function d1Like(db: SqliteDatabase): ApprovalDatabase {
  function statement(
    sql: string,
    params: unknown[],
  ): ApprovalPreparedStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      first: async <T>() =>
        (db.prepare(sql).get(...params) as T | undefined) ?? null,
      // D1-shaped envelope: real D1's run() resolves { meta: { changes } },
      // not node:sqlite's raw { changes } — a caller reading changes via
      // d1Changes()'s meta.changes optional chain would silently see 0
      // without this wrap (latent here since no test used to read it; see
      // retention.test.ts's purgeExpired coverage).
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

    it('breaks a createdAt tie by id BYTEWISE on both backends (FIFO collation parity)', async () => {
      // #given — one shared createdAt; ids differing only in case pin the
      // collation: 'B' (0x42) < 'a' (0x61) bytewise (SQLite BINARY, the
      // cursor row-value compare, compareStrings), but 'a' < 'B' under a
      // locale collation — the old in-memory localeCompare sort disagreed
      // with D1 here
      const store = await makeStore();
      const bytewiseFirst = makeRecord({ id: 'tie-B', runId: 'r-tie-1' });
      const bytewiseSecond = makeRecord({ id: 'tie-a', runId: 'r-tie-2' });
      bytewiseSecond.createdAt = bytewiseFirst.createdAt;
      bytewiseSecond.updatedAt = bytewiseFirst.updatedAt;
      await store.create(bytewiseSecond);
      await store.create(bytewiseFirst);

      // #when / #then
      expect((await store.list()).map((r) => r.id)).toEqual(['tie-B', 'tie-a']);
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

    // ---- pagination (D3: limit + cursor) -----------------------------------

    it('paginates with limit and a cursor, reconstructing the unpaged list with no gaps or dupes', async () => {
      // #given — 5 records under one tenant
      const store = await makeStore();
      const created: ApprovalRecord[] = [];
      for (let index = 0; index < 5; index += 1) {
        const record = makeRecord({ runId: `run-page-${index}` });
        await store.create(record);
        created.push(record);
      }
      const all = await store.list();
      expect(all.map((r) => r.id)).toEqual(created.map((r) => r.id));

      // #when — page through with limit 2, deriving each cursor from the
      // previous page's last record (the documented client contract)
      const page1 = await store.list({ limit: 2 });
      const page2 = await store.list({
        limit: 2,
        after: approvalCursor(page1[page1.length - 1] as ApprovalRecord),
      });
      const page3 = await store.list({
        limit: 2,
        after: approvalCursor(page2[page2.length - 1] as ApprovalRecord),
      });
      const pastTheEnd = await store.list({
        limit: 2,
        after: approvalCursor(page3[page3.length - 1] as ApprovalRecord),
      });

      // #then — three pages reconstruct the full ordered list, then nothing
      expect(page1.map((r) => r.id)).toEqual(all.slice(0, 2).map((r) => r.id));
      expect(page2.map((r) => r.id)).toEqual(all.slice(2, 4).map((r) => r.id));
      expect(page3.map((r) => r.id)).toEqual(all.slice(4, 5).map((r) => r.id));
      expect(pastTheEnd).toEqual([]);
    });

    it('clamps an out-of-range limit rather than ignoring it (defense in depth)', async () => {
      // #given
      const store = await makeStore();
      for (let index = 0; index < 3; index += 1) {
        await store.create(makeRecord({ runId: `run-clamp-${index}` }));
      }

      // #when / #then — 0 clamps up to 1
      expect((await store.list({ limit: 0 })).length).toBe(1);
    });

    it('rejects a malformed cursor', async () => {
      // #given
      const store = await makeStore();

      // #when / #then
      await expect(
        store.list({ after: 'not valid base64!!' }),
      ).rejects.toThrow();
    });

    it('bounds a bare tenant list() at MAX_APPROVAL_LIST_LIMIT while the system view stays complete (D3)', async () => {
      // #given — one tenant holding MORE than the cap in a single queue
      const backend = makeBackend();
      const store = backend.forTenant('acme');
      const total = MAX_APPROVAL_LIST_LIMIT + 1;
      for (let index = 0; index < total; index += 1) {
        await store.create(makeRecord({ runId: `run-bound-${index}` }));
      }

      // #when — a bare tenant list() (no limit)...
      const bounded = await store.list();
      // ...and an explicit cursor walk of MAX-sized pages
      const walked: string[] = [];
      let after: string | undefined;
      for (;;) {
        const page = await store.list({
          limit: MAX_APPROVAL_LIST_LIMIT,
          after,
        });
        walked.push(...page.map((r) => r.id));
        const last = page.at(-1);
        if (page.length < MAX_APPROVAL_LIST_LIMIT || !last) break;
        after = approvalCursor(last);
      }

      // #then — the bare list defaults to the cap (never an unbounded scan);
      // explicit cursor paging still retrieves everything with no gaps/dupes;
      // the cron-only system view stays complete (un-defaulted)
      expect(bounded).toHaveLength(MAX_APPROVAL_LIST_LIMIT);
      expect(walked).toHaveLength(total);
      expect(new Set(walked).size).toBe(total);
      expect(await backend.system().list()).toHaveLength(total);
    });

    // ---- reviewer ordering (orderBy: 'reviewer', 2026-07-11 review) -------

    it("orders priority → nearest SLA deadline (missing last) → FIFO under orderBy: 'reviewer'", async () => {
      // #given — creation order is deliberately the reverse of reviewer
      // relevance where possible (makeRecord stamps increasing createdAt)
      const store = await makeStore();
      const normalNoSla = makeRecord({ runId: 'r-rev-1' });
      const normalLate = makeRecord({
        runId: 'r-rev-2',
        slaDeadlineAt: '2099-01-01T00:00:00.000Z',
      });
      const normalSoon = makeRecord({
        runId: 'r-rev-3',
        slaDeadlineAt: '2027-01-01T00:00:00.000Z',
      });
      const criticalNewest = makeRecord({
        runId: 'r-rev-4',
        priority: 'critical',
      });
      const normalNoSlaLater = makeRecord({ runId: 'r-rev-5' });
      const lowSoon = makeRecord({
        runId: 'r-rev-6',
        priority: 'low',
        slaDeadlineAt: '2027-01-01T00:00:00.000Z',
      });
      for (const record of [
        normalNoSla,
        normalLate,
        normalSoon,
        criticalNewest,
        normalNoSlaLater,
        lowSoon,
      ]) {
        await store.create(record);
      }

      // #when
      const listed = await store.list({ orderBy: 'reviewer' });

      // #then — the D1 ORDER BY and the in-memory byReviewerOrder agree:
      // priority rank, deadline ascending with missing-deadline last, FIFO tie
      expect(listed.map((r) => r.id)).toEqual([
        criticalNewest.id,
        normalSoon.id,
        normalLate.id,
        normalNoSla.id,
        normalNoSlaLater.id,
        lowSoon.id,
      ]);
    });

    it('applies limit AFTER reviewer ordering — a fresh critical past the oldest page stays visible', async () => {
      // #given — three older 'normal' requests, then a critical arrival; the
      // FIFO-then-limit cut (the 2026-07-11 review finding) dropped the
      // newest record entirely
      const store = await makeStore();
      for (let index = 0; index < 3; index += 1) {
        await store.create(makeRecord({ runId: `r-lim-${index}` }));
      }
      const critical = makeRecord({
        runId: 'r-lim-critical',
        priority: 'critical',
      });
      await store.create(critical);

      // #when — the dashboard shape: open statuses, bounded, reviewer order
      const page = await store.list({
        status: ['pending'],
        limit: 2,
        orderBy: 'reviewer',
      });

      // #then — the bounded page leads with the critical record
      expect(page).toHaveLength(2);
      expect(page[0]?.id).toBe(critical.id);
    });

    it("rejects orderBy: 'reviewer' combined with an after cursor (cursors page FIFO order only)", async () => {
      // #given
      const store = await makeStore();
      const record = makeRecord({ runId: 'r-rev-after' });
      await store.create(record);

      // #when / #then
      await expect(
        store.list({ orderBy: 'reviewer', after: approvalCursor(record) }),
      ).rejects.toThrow(/orderBy 'reviewer'/);
    });

    it("the system view rejects orderBy: 'reviewer' + after too (shared guard, no drift)", async () => {
      // #given — the guard lives in approvalListOrder, which BOTH views of
      // both backends resolve through; this pins the system-view path
      const backend = makeBackend();
      const record = makeRecord({ runId: 'acme_r-sys-after' });
      await backend.forTenant('acme').create(record);

      // #when / #then
      await expect(
        backend
          .system()
          .list({ orderBy: 'reviewer', after: approvalCursor(record) }),
      ).rejects.toThrow(/orderBy 'reviewer'/);
    });

    it("ranks an out-of-enum priority LAST on both backends (JS '?? 4' fallback == SQL ELSE arm)", async () => {
      // #given — nothing validates priority at the store layer, so a rogue
      // writer or a future enum member read by old code can land any TEXT.
      // The rogue record is created FIRST: if its rank collapsed to
      // normal/low's, the FIFO tie-break would ALSO put it first, so the
      // expectation below only passes when the rank itself differs.
      const store = await makeStore();
      const rogue = makeRecord({
        runId: 'r-enum-rogue',
        priority: 'urgent' as ApprovalRecord['priority'],
      });
      const low = makeRecord({ runId: 'r-enum-low', priority: 'low' });
      await store.create(rogue);
      await store.create(low);

      // #when
      const listed = await store.list({ orderBy: 'reviewer' });

      // #then — unknown priority sorts after 'low' identically on both
      // backends (types.ts reviewerPriorityRank fallback; d1-store.ts CASE
      // ELSE arm)
      expect(listed.map((r) => r.id)).toEqual([low.id, rogue.id]);
    });

    // ---- metrics() (D3: SQL aggregate on D1, JS reduction in-memory) ------

    it('metrics() matches the reference JS computation — mixed statuses, undecided, and a missing-decidedAt edge case', async () => {
      // #given — breached open, fresh open, ever-escalated open, two clean
      // decisions (60s and 240s resolutions), a FRACTIONAL-second decision
      // (12.345s — pins the julianday-vs-Date-ms float divergence on a
      // non-round delta instead of leaving it untested), and a decided
      // record with NO decidedAt (the fallback edge case retention.ts also
      // has to handle)
      const store = await makeStore();
      const nowMs = Date.parse('2026-07-06T12:00:00.000Z');
      const fixture: ApprovalRecord[] = [
        makeRecord({
          id: 'm-1',
          status: 'pending',
          createdAt: T,
          updatedAt: T,
          slaDeadlineAt: '2020-01-01T00:00:00.000Z',
        }),
        makeRecord({
          id: 'm-2',
          status: 'claimed',
          createdAt: T,
          updatedAt: T,
          claimedBy: 'ray',
          slaDeadlineAt: '2099-01-01T00:00:00.000Z',
        }),
        makeRecord({
          id: 'm-3',
          status: 'escalated',
          createdAt: T,
          updatedAt: T,
          escalatedAt: T,
        }),
        makeRecord({
          id: 'm-4',
          status: 'approved',
          createdAt: '2026-07-06T12:00:00.000Z',
          decidedAt: '2026-07-06T12:01:00.000Z',
          updatedAt: '2026-07-06T12:01:00.000Z',
        }),
        makeRecord({
          id: 'm-5',
          status: 'rejected',
          createdAt: '2026-07-06T12:00:00.000Z',
          decidedAt: '2026-07-06T12:04:00.000Z',
          updatedAt: '2026-07-06T12:04:00.000Z',
        }),
        makeRecord({
          id: 'm-6',
          status: 'approved',
          createdAt: T,
          updatedAt: T,
        }),
        makeRecord({
          id: 'm-7',
          status: 'approved',
          createdAt: '2026-07-06T12:00:00.000Z',
          decidedAt: '2026-07-06T12:00:12.345Z',
          updatedAt: '2026-07-06T12:00:12.345Z',
        }),
      ];
      for (const record of fixture) await store.create(record);

      // #when
      const actual = await store.metrics(nowMs);

      // #then — the reference reduction over the SAME fixture (store.ts's
      // computeApprovalMetrics is the "old JS computation" both backends
      // must match). Counts compare exactly; avgResolutionSeconds allows a
      // small tolerance because D1's AVG(julianday()) and JS's
      // Date-ms-divided-by-1000 are different floating-point paths to the
      // same value — julianday's double-precision fractional-day
      // representation carries single-digit-microsecond rounding noise at
      // these magnitudes (empirically ~9e-6s on this fixture), comfortably
      // under the tolerance below.
      const expected = computeApprovalMetrics(fixture, nowMs);
      const { avgResolutionSeconds: expectedAvg, ...expectedRest } = expected;
      const { avgResolutionSeconds: actualAvg, ...actualRest } = actual;
      expect(actualRest).toEqual(expectedRest);
      if (expectedAvg === null) {
        expect(actualAvg).toBeNull();
      } else {
        expect(actualAvg).toBeCloseTo(expectedAvg, 3);
      }
    });

    it('metrics() reports zero counts and a null average on an empty store', async () => {
      // #given
      const store = await makeStore();

      // #when
      const metrics = await store.metrics(Date.now());

      // #then
      expect(metrics).toEqual({
        openCount: 0,
        slaBreachedCount: 0,
        escalationCount: 0,
        decidedCount: 0,
        approvedCount: 0,
        rejectedCount: 0,
        avgResolutionSeconds: null,
      });
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

    it('the system view honors reviewer ordering too (shared order helpers — the views must not drift)', async () => {
      // #given — the newest record is the most urgent, across tenants
      const backend = makeBackend();
      await backend
        .forTenant('acme')
        .create(makeRecord({ runId: 'acme_r-old' }));
      await backend
        .forTenant('bravo')
        .create(makeRecord({ runId: 'bravo_r-hot', priority: 'critical' }));

      // #when
      const listed = await backend
        .system()
        .list({ orderBy: 'reviewer', limit: 1 });

      // #then — reviewer order applied before the bound, same as the bound
      // store's list()
      expect(listed.map((r) => r.runId)).toEqual(['bravo_r-hot']);
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

describe('InMemoryApprovalStoreFactory.purgeTenant (in-memory offboarding)', () => {
  // The in-memory mirror of D1 purgeTenant's approvals delete — the shared-Map
  // factory makes the isolation assertion non-vacuous.
  it("deletes only the named tenant's records and returns the count", async () => {
    // #given — two acme records and one bravo record over ONE backend
    const backend = new InMemoryApprovalStoreFactory();
    const storeA = backend.forTenant('acme');
    const storeB = backend.forTenant('bravo');
    const a1 = makeRecord({ runId: 'acme_r1' });
    const a2 = makeRecord({ runId: 'acme_r2', stepPath: ['gate'] });
    const b1 = makeRecord({ runId: 'bravo_r1' });
    await storeA.create(a1);
    await storeA.create(a2);
    await storeB.create(b1);

    // #when
    const purged = backend.purgeTenant('acme');

    // #then — acme emptied, bravo untouched
    expect(purged).toBe(2);
    expect(await storeA.list()).toEqual([]);
    expect((await storeB.list()).map((r) => r.id)).toEqual([b1.id]);
  });

  it('returns 0 for a tenant with no records', () => {
    // #given
    const backend = new InMemoryApprovalStoreFactory();

    // #when / #then
    expect(backend.purgeTenant('ghost')).toBe(0);
  });

  it('rejects a non-INV-3 tenantId before touching any record', () => {
    // #given
    const backend = new InMemoryApprovalStoreFactory();

    // #when / #then — same guard as forTenant (and D1's purgeTenant)
    expect(() => backend.purgeTenant("a'; DROP--")).toThrow(/INV-3/);
  });
});

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
