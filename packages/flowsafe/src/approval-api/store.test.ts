// SPDX-License-Identifier: Apache-2.0
// One behavioral contract, two backends: InMemoryApprovalStore and
// D1ApprovalStore. These fast SQL units run against node:sqlite; the Wrangler
// harness owns D1 runtime, transaction, and concurrent-writer fidelity.
//
// The contract runs over factories so both production and in-memory assembly
// paths exercise their deployment-wide store selection.

import { describe, expect, it } from 'vitest';

import { openSqlite, type SqliteDatabase } from '../../test-support/sqlite.js';
import {
  type ApprovalDatabase,
  type ApprovalPreparedStatement,
  D1ApprovalStore,
} from './d1-store.js';
import { connectorGrantsForLeg } from './grants.js';
import type { ApprovalStore } from './store.js';
import { computeApprovalMetrics } from './store.js';
import {
  type ApprovalStoreFactory,
  D1ApprovalStoreFactory,
  InMemoryApprovalStoreFactory,
} from './store-factory.js';
import type { ApprovalRecord } from './types.js';
import { approvalCursor, MAX_APPROVAL_LIST_LIMIT } from './types.js';

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

// --- D1 create-race harness (F3) -------------------------------------------

// Fires at the instant the store-under-test executes a #openFor SELECT or an
// INSERT run(), letting a test interleave a concurrent decide-close (or
// fabricate a UNIQUE violation) with byte-real SQLite semantics underneath.
// ONLY the store-under-test's statements are wrapped; the seed store that
// mutates the shared DB inside a hook uses a plain d1Like, so its own
// statements never recurse through these counters.
interface CreateRaceHooks {
  onOpenForSelect?: (occurrence: number) => void | Promise<void>;
  onInsert?: (occurrence: number) => void | Promise<void>;
}

function racingD1(
  db: SqliteDatabase,
  hooks: CreateRaceHooks,
): ApprovalDatabase {
  const base = d1Like(db);
  let inserts = 0;
  let openForReads = 0;
  const isInsert = (sql: string): boolean => /^\s*INSERT INTO/i.test(sql);
  // The #openFor SELECT is the only query keyed on step_key with a LIMIT 1;
  // list()/get() never carry both, so this is an exact discriminator.
  const isOpenFor = (sql: string): boolean =>
    /step_key = \?/.test(sql) && /LIMIT 1/.test(sql);
  function wrap(
    sql: string,
    stmt: ApprovalPreparedStatement,
  ): ApprovalPreparedStatement {
    return {
      bind: (...values: unknown[]) => wrap(sql, stmt.bind(...values)),
      first: async <T = unknown>(): Promise<T | null> => {
        if (isOpenFor(sql)) {
          openForReads += 1;
          await hooks.onOpenForSelect?.(openForReads);
        }
        return stmt.first<T>();
      },
      run: async (): Promise<unknown> => {
        if (isInsert(sql)) {
          inserts += 1;
          await hooks.onInsert?.(inserts);
        }
        return stmt.run();
      },
      all: <T = unknown>(): Promise<{ results: T[] }> => stmt.all<T>(),
    };
  }
  return { prepare: (sql: string) => wrap(sql, base.prepare(sql)) };
}

// --- shared contract -------------------------------------------------------

function describeStoreContract(
  name: string,
  makeBackend: () => ApprovalStoreFactory,
): void {
  // Each case gets a fresh deployment store.
  const makeStore = (): ApprovalStore => makeBackend().store();
  describe(name, () => {
    it('round-trips a suspension-scoped record through create/get', async () => {
      // #given
      const store = await makeStore();
      const record = makeRecord({
        stepPath: ['approval'],
        suspendedAt: 1751882400000,
        resumedAt: 1751882460000,
        resumeCount: 2,
        grantScope: 'suspension',
        resumeTarget: {
          kind: 'thread',
          threadId: 'acme_thread-1',
          resourceId: 'acme_resource-1',
        },
        summary: 'publish the launch post',
        payload: { reason: 'human approval required', nested: { n: 1 } },
        connectors: ['blog-publisher', 'mailer'],
        priority: 'high',
        requestedBy: 'system',
        requestedByKind: 'system',
        slaDeadlineAt: T,
      });

      // #when
      const created = await store.create(record);

      // #then
      expect(created.created).toBe(true);
      expect(await store.get(record.id)).toEqual(record);
    });

    it('round-trips mutually exclusive run- and tool-call-scoped fields', async () => {
      const store = await makeStore();
      const runScoped = makeRecord({
        runScoped: true,
        grantScope: 'run',
        connectors: ['mailer'],
      });
      const toolCallScoped = makeRecord({
        stepPath: ['approval'],
        suspendedAt: 1751882400000,
        resumeCount: 1,
        grantScope: 'tool-call',
        toolCallId: 'call-1',
        connectors: ['mailer'],
      });

      await expect(store.create(runScoped)).resolves.toEqual({
        created: true,
        record: runScoped,
      });
      await expect(store.create(toolCallScoped)).resolves.toEqual({
        created: true,
        record: toolCallScoped,
      });
      await expect(store.get(runScoped.id)).resolves.toEqual(runScoped);
      await expect(store.get(toolCallScoped.id)).resolves.toEqual(
        toolCallScoped,
      );
    });

    it('round-trips a trusted agent-thread resume target', async () => {
      const store = await makeStore();
      const record = makeRecord({
        resumeTarget: {
          kind: 'agent-thread',
          agentId: 'writer',
          threadId: 'acme_thread-agent',
          resourceId: 'acme_resource-agent',
          principal: {
            kind: 'human',
            id: 'requester-1',
            role: 'operator',
          },
        },
      });

      const created = await store.create(record);

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
      // #given — a legacy/unbound decided request with no captured suspension
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

    it.each([
      'approved',
      'rejected',
    ] as const)('returns the terminal %s record for the same captured suspension fingerprint', async (status) => {
      // #given — reconciliation listed before another request filed + decided
      const store = await makeStore();
      expect(await store.list({ workflowId: 'wf', runId: 'run-race' })).toEqual(
        [],
      );
      const winner = makeRecord({
        runId: 'run-race',
        stepPath: ['approval'],
        suspendedAt: 1000,
      });
      await store.create(winner);
      await store.transition(winner.id, ['pending'], {
        status,
        updatedAt: T,
        decidedAt: T,
      });

      // #when — the stale reconciliation queues after that terminal decision
      const staleCreate = await store.create(
        makeRecord({
          runId: 'run-race',
          stepPath: ['approval'],
          suspendedAt: 1000,
        }),
      );

      // #then — NULL first-suspension resumeCount compares equal; no pending
      expect(staleCreate).toMatchObject({
        created: false,
        record: { id: winner.id, status },
      });
      expect(await store.list({ runId: 'run-race' })).toHaveLength(1);
      expect(
        await store.list({ runId: 'run-race', status: 'pending' }),
      ).toHaveLength(0);
    });

    it('opens fresh when the same step has a later captured suspension fingerprint', async () => {
      const store = await makeStore();
      const first = makeRecord({
        runId: 'run-resuspended',
        stepPath: ['approval'],
        suspendedAt: 1000,
      });
      await store.create(first);
      await store.transition(first.id, ['pending'], {
        status: 'approved',
        updatedAt: T,
        decidedAt: T,
      });

      const later = await store.create(
        makeRecord({
          runId: 'run-resuspended',
          stepPath: ['approval'],
          suspendedAt: 1000,
          resumeCount: 1,
        }),
      );

      expect(later.created).toBe(true);
      expect(later.record.id).not.toBe(first.id);
      expect(await store.list({ runId: 'run-resuspended' })).toHaveLength(2);
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

    it('filters by requestedBy and strict createdBefore/createdAfter bounds', async () => {
      // #given — three records 1s apart (makeRecord increments createdAt)
      const store = await makeStore();
      const early = makeRecord({ runId: 'r-t1', requestedBy: 'ada' });
      const middle = makeRecord({ runId: 'r-t2', requestedBy: 'opal' });
      const late = makeRecord({ runId: 'r-t3', requestedBy: 'ada' });
      await store.create(early);
      await store.create(middle);
      await store.create(late);

      // #when / #then — exact requestedBy match
      expect(
        (await store.list({ requestedBy: 'ada' })).map((r) => r.id),
      ).toEqual([early.id, late.id]);

      // Strict bounds: the boundary record itself is excluded either side.
      expect(
        (await store.list({ createdBefore: middle.createdAt })).map(
          (r) => r.id,
        ),
      ).toEqual([early.id]);
      expect(
        (await store.list({ createdAfter: middle.createdAt })).map((r) => r.id),
      ).toEqual([late.id]);

      // Combined filters intersect.
      expect(
        (
          await store.list({
            requestedBy: 'ada',
            createdAfter: early.createdAt,
          })
        ).map((r) => r.id),
      ).toEqual([late.id]);
    });

    it('treats a no-milliseconds ISO bound as the same instant (D1 canonicalization boundary case)', async () => {
      // #given — SQLite TEXT compare is bytewise: '…T…Z' vs '…T….000Z' would
      // misorder at the boundary unless the bound is canonicalized first
      const store = await makeStore();
      const early = makeRecord({ runId: 'r-c1' });
      const boundary = makeRecord({ runId: 'r-c2' });
      const late = makeRecord({ runId: 'r-c3' });
      await store.create(early);
      await store.create(boundary);
      await store.create(late);
      const noMillis = boundary.createdAt.replace('.000Z', 'Z');
      expect(noMillis).not.toBe(boundary.createdAt);

      // #when / #then — the variant spelling is the SAME strict bound
      expect(
        (await store.list({ createdBefore: noMillis })).map((r) => r.id),
      ).toEqual([early.id]);
      expect(
        (await store.list({ createdAfter: noMillis })).map((r) => r.id),
      ).toEqual([late.id]);
    });

    it('throws on an unparseable time bound identically on both backends', async () => {
      // #given
      const store = await makeStore();
      await store.create(makeRecord({ runId: 'r-bad' }));

      // #when / #then — loud error, not a silently-empty page
      await expect(
        store.list({ createdBefore: 'yesterday-ish' }),
      ).rejects.toThrow(/createdBefore/);
      await expect(store.list({ createdAfter: '' })).rejects.toThrow(
        /createdAfter/,
      );
    });

    it('throws on a garbage time bound with ZERO matching records too (F6: eager validation)', async () => {
      // #given — an EMPTY bound store: no record reaches matchesFilter, so the
      // in-memory list() used to skip the per-record parse and return [] where
      // D1's appendListFilters throws unconditionally (types.ts: both backends
      // fail identically). Eager validation closes that lazy hole.
      const store = await makeStore();

      // #when / #then — a loud throw on both backends, not a silent empty page
      await expect(store.list({ createdBefore: 'not-a-date' })).rejects.toThrow(
        /createdBefore/,
      );
      await expect(store.list({ createdAfter: 'not-a-date' })).rejects.toThrow(
        /createdAfter/,
      );
    });

    it('a fresh factory store throws on a garbage time bound with ZERO records too (F6)', async () => {
      // #given — an EMPTY backend. It must reject a garbage bound identically
      // to a populated store rather than skipping validation with no rows.
      const backend = makeBackend();

      // #when / #then
      await expect(
        backend.store().list({ createdBefore: 'not-a-date' }),
      ).rejects.toThrow(/createdBefore/);
      await expect(
        backend.store().list({ createdAfter: 'not-a-date' }),
      ).rejects.toThrow(/createdAfter/);
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
      // #given — 5 records in one deployment
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

    it('bounds a bare deployment list() while explicit cursor paging stays complete (D3)', async () => {
      // #given — one deployment holding more than the cap in a single queue
      const backend = makeBackend();
      const store = backend.store();
      const total = MAX_APPROVAL_LIST_LIMIT + 1;
      for (let index = 0; index < total; index += 1) {
        await store.create(makeRecord({ runId: `run-bound-${index}` }));
      }

      // #when — a bare list() (no limit)...
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
      // explicit cursor paging still retrieves everything with no gaps/dupes.
      expect(bounded).toHaveLength(MAX_APPROVAL_LIST_LIMIT);
      expect(walked).toHaveLength(total);
      expect(new Set(walked).size).toBe(total);
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
      await backend.store().create(record);

      // #when / #then
      await expect(
        backend
          .store()
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

    it('store aliases share deployment-wide records', async () => {
      const backend = makeBackend();
      const storeA = backend.store();
      const storeB = backend.store();
      const record = makeRecord({ runId: 'acme_r1' });
      await storeA.create(record);

      expect(await storeB.get(record.id)).toEqual(record);
      await expect(
        storeB.transition(record.id, ['pending'], {
          status: 'approved',
          updatedAt: T,
        }),
      ).resolves.toMatchObject({ status: 'approved' });
      await expect(storeA.get(record.id)).resolves.toMatchObject({
        status: 'approved',
      });
    });

    it('open-step uniqueness is deployment-wide', async () => {
      const backend = makeBackend();
      const storeA = backend.store();
      const storeB = backend.store();
      const a = makeRecord({ runId: 'run-shared', stepPath: ['gate'] });
      await storeA.create(a);

      const b = await storeB.create(
        makeRecord({ runId: 'run-shared', stepPath: ['gate'] }),
      );

      expect(b).toEqual({ created: false, record: a });
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

describe('D1ApprovalStore persisted resume-target validation', () => {
  it.each([
    ['malformed JSON', '{'],
    [
      // Exactly what the previous release persisted for an agent run. Reading
      // it as a human would resurrect the fabricated operator this removes.
      'pre-principal ApprovalActor shape',
      JSON.stringify({
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        principal: {
          id: 'flowsafe-system',
          role: 'operator',
        },
      }),
    ],
    [
      'automated principal with no purpose',
      JSON.stringify({
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        principal: { kind: 'system', id: 'sched' },
      }),
    ],
    [
      'path-unsafe agent id',
      JSON.stringify({
        kind: 'agent-thread',
        agentId: 'bad/agent',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        principal: { kind: 'human', id: 'actor', role: 'operator' },
      }),
    ],
    [
      'empty principal id',
      JSON.stringify({
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        principal: { kind: 'human', id: '', role: 'operator' },
      }),
    ],
    [
      'whitespace principal id',
      JSON.stringify({
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        principal: { kind: 'human', id: '   ', role: 'operator' },
      }),
    ],
    [
      'invalid principal role',
      JSON.stringify({
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        principal: { kind: 'human', id: 'actor', role: 'owner' },
      }),
    ],
  ])('fails closed on a persisted %s', async (_label, resumeTarget) => {
    const sqlite = openSqlite();
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();
    const record = makeRecord();
    await store.create(record);
    sqlite
      .prepare(`UPDATE flowsafe_approvals SET resume_target = ? WHERE id = ?`)
      .run(resumeTarget, record.id);

    await expect(store.get(record.id)).rejects.toThrow(
      /invalid resume_target JSON|invalid resume_target/,
    );
  });
});

describe('D1ApprovalStore.create — concurrent decide-close race (F3)', () => {
  // The idempotent-create contract ("Decided requests never block a new one")
  // must survive a decide() closing the conflicting open row between the failed
  // INSERT and the #openFor SELECT. D1-only (the synchronous in-memory store
  // cannot interleave), availability-only (no capability effect).
  it('retries the INSERT ONCE when a concurrent decide closes the conflicting open row, returning created:true', async () => {
    // #given — a shared SQLite; a seed store opens R1 for (wf, run, gate)
    const sqlite = openSqlite();
    const seed = new D1ApprovalStoreFactory(d1Like(sqlite)).store();
    const open1 = makeRecord({
      runId: 'race-run',
      stepPath: ['gate'],
      suspendedAt: 1000,
    });
    await seed.create(open1);

    // At the FIRST #openFor SELECT (between the failed INSERT and the read),
    // transition R1 to 'approved' — the concurrent decide-close. #openFor then
    // sees no open row (null), so create() retries the INSERT, which now
    // succeeds because the partial open-step index is clear.
    let closes = 0;
    const racing = racingD1(sqlite, {
      onOpenForSelect: async (occurrence) => {
        if (occurrence === 1) {
          closes += 1;
          await seed.transition(open1.id, ['pending'], {
            status: 'approved',
            decidedAt: T,
            updatedAt: T,
          });
        }
      },
    });
    const store = new D1ApprovalStore(racing, {});

    // #when — a fresh create for the SAME step
    const fresh = makeRecord({
      runId: 'race-run',
      stepPath: ['gate'],
      suspendedAt: 2000,
      resumeCount: 1,
    });
    const result = await store.create(fresh);

    // #then — no throw; a brand-new record was created (not R1)
    expect(closes).toBe(1);
    expect(result.created).toBe(true);
    expect(result.record.id).toBe(fresh.id);
  });

  it('on a SECOND unique violation returns the now-open record (created:false), bounded to one retry', async () => {
    // #given — seed opens R1 for (wf, run, gate)
    const sqlite = openSqlite();
    const seed = new D1ApprovalStoreFactory(d1Like(sqlite)).store();
    const open1 = makeRecord({
      id: 'r1-open',
      runId: 'race-run-2',
      stepPath: ['gate'],
      suspendedAt: 1000,
    });
    await seed.create(open1);

    // Race: at the first #openFor close R1 (=> null, forcing the retry); just
    // before the retry INSERT a DIFFERENT concurrent create opens R2 for the
    // same step, so INSERT#2 also hits UNIQUE; the second #openFor then finds
    // R2 and create() returns it with created:false (no third attempt).
    const open2 = makeRecord({
      id: 'r2-open',
      runId: 'race-run-2',
      stepPath: ['gate'],
      suspendedAt: 3000,
      resumeCount: 2,
    });
    const racing = racingD1(sqlite, {
      onOpenForSelect: async (occurrence) => {
        if (occurrence === 1) {
          await seed.transition(open1.id, ['pending'], {
            status: 'approved',
            decidedAt: T,
            updatedAt: T,
          });
        }
      },
      onInsert: async (occurrence) => {
        // occurrence 1 = the first INSERT (collides with R1); occurrence 2 =
        // the retry: open R2 first so it ALSO collides.
        if (occurrence === 2) await seed.create(open2);
      },
    });
    const store = new D1ApprovalStore(racing, {});

    // #when
    const fresh = makeRecord({
      id: 'fresh-loser',
      runId: 'race-run-2',
      stepPath: ['gate'],
      suspendedAt: 2000,
      resumeCount: 1,
    });
    const result = await store.create(fresh);

    // #then — the retry lost to R2; create returns R2, created:false, no throw
    expect(result.created).toBe(false);
    expect(result.record.id).toBe('r2-open');
  });

  it('rethrows the second unique violation when the open row stays gone — bounded to ONE retry, never a loop', async () => {
    // #given — an EMPTY table and a DB that fabricates a UNIQUE violation on
    // EVERY INSERT. #openFor therefore returns null on both reads: the
    // pathological "UNIQUE but nothing open" case the retry must not loop on.
    const sqlite = openSqlite();
    let inserts = 0;
    const racing = racingD1(sqlite, {
      onInsert: () => {
        inserts += 1;
        throw new Error('UNIQUE constraint failed: flowsafe_approvals.id');
      },
    });
    const store = new D1ApprovalStore(racing, {});

    // #when / #then — the second UNIQUE surfaces (no open record to return),
    // and the INSERT was attempted EXACTLY twice (one initial + one retry)
    await expect(
      store.create(makeRecord({ runId: 'race-run-3', stepPath: ['gate'] })),
    ).rejects.toThrow(/UNIQUE constraint failed/);
    expect(inserts).toBe(2);
  });
});

describe('D1ApprovalStore schema upgrade', () => {
  const SINGLE_DEPLOYMENT_LEGACY_DDL = `CREATE TABLE flowsafe_approvals (
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

  const RETIRED_POOLED_DDL = `CREATE TABLE flowsafe_approvals (
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

  it('hydrates a requested_by value exactly at the principal-id bound', async () => {
    const sqlite = openSqlite();
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();
    await store.list();
    const requestedBy = 'r'.repeat(200);
    sqlite
      .prepare(
        `INSERT INTO flowsafe_approvals
         (id, workflow_id, run_id, title, connectors, priority, status,
          requested_by, requested_by_kind, created_at, updated_at)
         VALUES ('bounded-requester', 'wf', 'run-bounded-requester', 'bounded',
                 '[]', 'normal', 'pending', ?, 'human', ?, ?)`,
      )
      .run(requestedBy, T, T);

    await expect(store.get('bounded-requester')).resolves.toMatchObject({
      requestedBy,
      requestedByKind: 'human',
    });
  });

  it.each([
    ['an overlong value', 'r'.repeat(201)],
    ['an all-whitespace value', ' '.repeat(200)],
    ['a control-bearing value', 'requester\u000aforged'],
  ])('fails closed when D1 hydrates requested_by with %s', async (_label, requestedBy) => {
    const sqlite = openSqlite();
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();
    await store.list();
    sqlite
      .prepare(
        `INSERT INTO flowsafe_approvals
         (id, workflow_id, run_id, title, connectors, priority, status,
          requested_by, requested_by_kind, created_at, updated_at)
         VALUES ('corrupt-requester', 'wf', 'run-corrupt-requester', 'corrupt',
                 '[]', 'normal', 'pending', ?, 'human', ?, ?)`,
      )
      .run(requestedBy, T, T);

    await expect(store.get('corrupt-requester')).rejects.toThrow(
      "approval 'corrupt-requester' has invalid requested_by",
    );
  });

  it('fails closed when D1 hydrates an invalid requested_by_kind', async () => {
    const sqlite = openSqlite();
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();
    await store.list();
    sqlite
      .prepare(
        `INSERT INTO flowsafe_approvals
         (id, workflow_id, run_id, title, connectors, priority, status,
          requested_by, requested_by_kind, created_at, updated_at)
         VALUES ('corrupt-requester-kind', 'wf', 'run-corrupt-kind', 'corrupt',
                 '[]', 'normal', 'pending', 'requester', 'operator', ?, ?)`,
      )
      .run(T, T);

    await expect(store.get('corrupt-requester-kind')).rejects.toThrow(
      "approval 'corrupt-requester-kind' has invalid requested_by_kind",
    );
  });

  it('refuses the retired pooled schema and names database recreation', async () => {
    const sqlite = openSqlite();
    sqlite.prepare(RETIRED_POOLED_DDL).run();

    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();
    await expect(store.get('anything')).rejects.toThrow(
      /retired pooled-tenant schema.*recreate the database/,
    );
    await expect(store.list()).rejects.toThrow(/recreate the database/);
    await expect(store.create(makeRecord())).rejects.toThrow(
      /recreate the database/,
    );
  });

  it('drops pooled-era index names before creating deployment-wide indexes', async () => {
    const sqlite = openSqlite();
    sqlite.prepare(SINGLE_DEPLOYMENT_LEGACY_DDL).run();
    sqlite
      .prepare(
        `CREATE UNIQUE INDEX IF NOT EXISTS flowsafe_approvals_open_step
         ON flowsafe_approvals (workflow_id, run_id, step_key)
         WHERE status IN ('pending', 'claimed', 'escalated')`,
      )
      .run();
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();
    await store.list();

    const indexes = sqlite
      .prepare(
        `SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='flowsafe_approvals' AND sql IS NOT NULL`,
      )
      .all() as Array<{ name: string }>;
    const names = indexes.map((row) => row.name);
    expect(names).not.toContain('flowsafe_approvals_open_step');
    expect(names).not.toContain('flowsafe_approvals_open_step_v2');
    expect(names).toContain('flowsafe_approvals_open_step_v3');
  });

  it('backfills nullable fields on an older single-deployment schema', async () => {
    const sqlite = openSqlite();
    sqlite.prepare(SINGLE_DEPLOYMENT_LEGACY_DDL).run();
    sqlite
      .prepare(
        `INSERT INTO flowsafe_approvals
         (id, workflow_id, run_id, title, connectors, priority, status,
          requested_by, created_at, updated_at)
         VALUES ('legacy-requester', 'wf', 'run-legacy-requester', 'legacy',
                 '[]', 'normal', 'pending', 'shared-principal', ?, ?)`,
      )
      .run(T, T);
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();

    // #when — a fresh record round-trips the backfilled columns
    const fresh = makeRecord({
      stepPath: ['gate'],
      suspendedAt: 1751882400000,
      resumedAt: 1751882460000,
      resumeCount: 2,
      grantScope: 'tool-call',
      toolCallId: 'call-1',
      requestedBy: 'schedule-runner',
      requestedByKind: 'system',
      resumeTarget: {
        kind: 'thread',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
      },
    });
    await store.create(fresh);
    const readBack = await store.get(fresh.id);

    // #then
    expect(readBack?.suspendedAt).toBe(1751882400000);
    expect(readBack?.resumedAt).toBe(1751882460000);
    expect(readBack?.resumeCount).toBe(2);
    expect(readBack?.grantScope).toBe('tool-call');
    expect(readBack?.toolCallId).toBe('call-1');
    expect(readBack?.requestedBy).toBe('schedule-runner');
    expect(readBack?.requestedByKind).toBe('system');
    expect(readBack?.resumeTarget).toEqual(fresh.resumeTarget);
    expect(
      (await store.get('legacy-requester'))?.requestedByKind,
    ).toBeUndefined();
    const columns = sqlite
      .prepare('PRAGMA table_info(flowsafe_approvals)')
      .all() as Array<{ name: string }>;
    expect(columns.map((column) => column.name)).toContain('requested_by_kind');
  });

  it('fails closed when legacy rows duplicate a captured suspension fingerprint', async () => {
    const sqlite = openSqlite();
    sqlite.prepare(SINGLE_DEPLOYMENT_LEGACY_DDL).run();
    sqlite
      .prepare(`ALTER TABLE flowsafe_approvals ADD COLUMN suspended_at INTEGER`)
      .run();
    sqlite
      .prepare(`ALTER TABLE flowsafe_approvals ADD COLUMN resume_count INTEGER`)
      .run();
    for (const id of ['duplicate-a', 'duplicate-b']) {
      sqlite
        .prepare(
          `INSERT INTO flowsafe_approvals
           (id, workflow_id, run_id, step_key, step_path,
            suspended_at, resume_count, title, connectors, priority, status,
            created_at, updated_at)
           VALUES (?, 'wf', 'run-duplicate', 'gate', '["gate"]',
                   1000, NULL, 'duplicate', '[]', 'normal', 'approved', ?, ?)`,
        )
        .run(id, T, T);
    }
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();

    await expect(store.list()).rejects.toThrow(
      /duplicate captured suspension fingerprints/,
    );
  });

  it('a legacy step-less approval (no run_scoped) mints NOTHING on the upgraded table', async () => {
    // #given — an older single-deployment table holding exactly the record
    // the OLD grant rule treated as a run-wide standing grant
    const sqlite = openSqlite();
    sqlite.prepare(SINGLE_DEPLOYMENT_LEGACY_DDL).run();
    sqlite
      .prepare(
        `INSERT INTO flowsafe_approvals
         (id, workflow_id, run_id, step_key, step_path, title,
          connectors, priority, status, decided_at, created_at, updated_at)
         VALUES ('legacy-1', 'wf', 'run-legacy', '', NULL,
                 'legacy standing grant', '["release-deploy"]', 'normal',
                 'approved', ?, ?, ?)`,
      )
      .run(T, T, T);
    const store = new D1ApprovalStoreFactory(d1Like(sqlite)).store();

    // #when / #then — runScoped backfills to NULL => denies on every leg
    const legacy = await store.get('legacy-1');
    expect(legacy?.runScoped).toBeUndefined();
    expect(
      await connectorGrantsForLeg(store, 'wf', 'run-legacy', {
        kind: 'start',
      }),
    ).toEqual([]);
  });
});
