// SPDX-License-Identifier: Apache-2.0
// Unit coverage for the host-kit approval bridge: the payload-shape edge cases
// of requestedConnectors, the (suspendedAt, resumeCount) capture in
// queueApprovalForSuspension, the multi-gate re-queue + fail-closed guard in
// resumeRunWithRequeue (plus its D4 audit signal on a re-queue failure), and
// the D4 self-healing reconcile (reconcileApprovalsForSummary). These are the
// pieces the showcase Worker and dev backend both depend on, so they get
// direct tests independent of any workflow.

import { describe, expect, it } from 'vitest';

import {
  type ApprovalActor,
  type ApprovalAuditEvent,
  type ApprovalRecord,
  ApprovalService,
  approvedConnectorsForLeg,
  InMemoryApprovalStore,
} from '../approval-api/index.js';
import type { RunSummary } from '../do-runner/index.js';
// requestedConnectors is module-internal (not on the barrel): it is the
// primitive beneath queueApprovalForSuspension, tested here directly.
import { requestedConnectors } from './approval-bridge.js';
import {
  queueApprovalForSuspension,
  type ResumeRunFn,
  reconcileApprovalsForSummary,
  resumeRunWithRequeue,
} from './index.js';

const SYSTEM: ApprovalActor = { id: 'sys', role: 'operator', tenantId: 'acme' };
const REVIEWER: ApprovalActor = {
  id: 'ray',
  role: 'reviewer',
  tenantId: 'acme',
};

describe('requestedConnectors', () => {
  it.each([
    ['null', null, []],
    ['undefined', undefined, []],
    ['a number', 42, []],
    ['a string', 'connectors', []],
    ['an object without connectors', { reason: 'x' }, []],
    ['connectors as a non-array', { connectors: 'a' }, []],
    ['connectors with a non-string element', { connectors: ['a', 2] }, []],
    ['a valid connectors array', { connectors: ['a', 'b'] }, ['a', 'b']],
    // Track A (R-003): an AGENT gate declares no connectors array — the tool the
    // model called is derived from its (flat or nested) suspend shape.
    [
      'a FLAT agent gate',
      { type: 'approval', toolCallId: 'c', toolName: 'send-email' },
      ['send-email'],
    ],
    [
      'a NESTED agent gate',
      {
        type: 'approval',
        requireToolApproval: { toolCallId: 'c', toolName: 'send-email' },
      },
      ['send-email'],
    ],
    // Collision narrowing: a bare {type:'approval', toolName} without the
    // toolCallId a real agent gate carries mints nothing (fail closed).
    [
      'an agent shape missing toolCallId',
      { type: 'approval', toolName: 'send-email' },
      [],
    ],
    // An explicit connectors array always wins, so a workflow gate is unaffected
    // even if it also carries a toolName.
    [
      'an explicit connectors array beside a toolName',
      { type: 'approval', toolName: 'ignored', connectors: ['real'] },
      ['real'],
    ],
    // A non-'approval' suspend type is not an agent gate.
    [
      'a non-approval type with a toolName',
      { type: 'suspension', toolName: 'x' },
      [],
    ],
  ])('returns %s -> the right list', (_label, payload, expected) => {
    expect(requestedConnectors(payload)).toEqual(expected);
  });
});

function suspendedSummary(
  runId: string,
  stepId: string,
  connectors: string[],
  suspendedAt: number,
  resumeCount?: number,
): RunSummary {
  return {
    runId,
    status: 'suspended',
    suspended: [[stepId]],
    suspendPayload: { [stepId]: { reason: `gate ${stepId}`, connectors } },
    suspendedAt: { [stepId]: suspendedAt },
    ...(resumeCount !== undefined
      ? { resumeCount: { [stepId]: resumeCount } }
      : {}),
  };
}

describe('queueApprovalForSuspension', () => {
  it('captures the suspended step, its (suspendedAt, resumeCount) pair, and connectors', async () => {
    // #given — a run suspended at gate2 on its second suspension
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const summary = suspendedSummary(
      'acme_run-1',
      'gate2',
      ['deploy-conn'],
      1717,
      2,
    );

    // #when — the bridge queues the approval
    const records = await queueApprovalForSuspension(
      service,
      'product-launch',
      summary,
      'reviewer-of-gate1',
      SYSTEM,
      {
        kind: 'thread',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
      },
    );

    // #then — the binding fingerprint is copied verbatim from the summary
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      workflowId: 'product-launch',
      runId: 'acme_run-1',
      stepPath: ['gate2'],
      suspendedAt: 1717,
      resumeCount: 2,
      connectors: ['deploy-conn'],
      requestedBy: 'reviewer-of-gate1',
      resumeTarget: {
        kind: 'thread',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
      },
      status: 'pending',
    });
  });

  it('files one record PER suspended path when parallel branches suspend together', async () => {
    // #given — a .parallel() run suspended at TWO gates in one summary
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const summary: RunSummary = {
      runId: 'acme_run-parallel',
      status: 'suspended',
      suspended: [['gateA'], ['gateB']],
      suspendPayload: {
        gateA: { reason: 'gate A', connectors: ['conn-a'] },
        gateB: { reason: 'gate B', connectors: ['conn-b'] },
      },
      suspendedAt: { gateA: 111, gateB: 222 },
    };

    // #when — the bridge queues the suspension
    const records = await queueApprovalForSuspension(
      service,
      'parallel-gates',
      summary,
      'starter',
      SYSTEM,
    );

    // #then — BOTH gates reach the queue, each bound to its own suspension
    // and carrying its own connectors. A gate that never files can never be
    // decided: its connector would deny on every resume with nothing telling
    // a reviewer why the run is stuck.
    expect(records).toHaveLength(2);
    expect(records.map((record) => record.stepPath)).toEqual([
      ['gateA'],
      ['gateB'],
    ]);
    expect(records[0]).toMatchObject({
      suspendedAt: 111,
      connectors: ['conn-a'],
    });
    expect(records[1]).toMatchObject({
      suspendedAt: 222,
      connectors: ['conn-b'],
    });
    expect(await store.list({ status: 'pending' })).toHaveLength(2);
  });

  it('isolates a failing gate filing: siblings still file and the error aggregates', async () => {
    // #given — a store that rejects gateA's record but accepts gateB's (a
    // transient D1 hiccup mid-loop). The run is already suspended by now and
    // a POST /runs retry mints a FRESH runId, so abandoning the loop on the
    // first failure would strand gateB with no record and no retry path.
    const store = new InMemoryApprovalStore('acme');
    const originalCreate = store.create.bind(store);
    (store as { create: typeof store.create }).create = async (record) => {
      if (record.stepPath?.[0] === 'gateA') throw new Error('d1 hiccup');
      return originalCreate(record);
    };
    const service = new ApprovalService({ store });
    const summary: RunSummary = {
      runId: 'acme_run-partial',
      status: 'suspended',
      suspended: [['gateA'], ['gateB']],
      suspendPayload: {
        gateA: { reason: 'gate A', connectors: ['conn-a'] },
        gateB: { reason: 'gate B', connectors: ['conn-b'] },
      },
      suspendedAt: { gateA: 111, gateB: 222 },
    };

    // #when / #then — the failure still surfaces, aggregated...
    await expect(
      queueApprovalForSuspension(
        service,
        'parallel-gates',
        summary,
        'starter',
        SYSTEM,
      ),
    ).rejects.toThrow(/1 of 2 gate filing\(s\) failed/);

    // ...but gateB's filing landed rather than being abandoned
    const open = await store.list({ status: 'pending' });
    expect(open).toHaveLength(1);
    expect(open[0]?.stepPath).toEqual(['gateB']);
  });
});

describe('resumeRunWithRequeue', () => {
  it('re-queues the next gate attributed to the decider (SoD across gates)', async () => {
    // #given — a service whose base resume re-suspends the run at a 2nd gate
    const store = new InMemoryApprovalStore('acme');
    const base: ResumeRunFn = async () =>
      suspendedSummary(
        'acme_run-1',
        'confirmRollout',
        ['deploy-conn'],
        2020,
        1,
      );
    // service forward-references itself in the resumeRun closure (invoked only
    // on a later decision); the same const-with-deferred-ref shape worker.ts uses.
    const service: ApprovalService = new ApprovalService({
      store,
      resumeRun: resumeRunWithRequeue(base, () => service, SYSTEM),
    });

    // a first-gate approval, requested by someone OTHER than the reviewer
    const { record: gate1 } = await service.create(
      {
        workflowId: 'product-launch',
        runId: 'acme_run-1',
        stepPath: ['approveLaunch'],
        suspendedAt: 1000,
        title: 'Approve launch',
        connectors: ['deploy-conn'],
        requestedBy: 'starter',
      },
      SYSTEM,
      {
        kind: 'thread',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
      },
    );

    // #when — the reviewer approves gate1; resume re-suspends at gate2
    await service.decide(gate1.id, { decision: 'approve' }, REVIEWER);

    // #then — a second approval exists, attributed to the gate1 decider, bound
    // to the new suspension. The decider therefore cannot decide it themselves.
    const open = await store.list({ status: 'pending' });
    expect(open).toHaveLength(1);
    expect(open[0]).toMatchObject({
      stepPath: ['confirmRollout'],
      suspendedAt: 2020,
      resumeCount: 1,
      requestedBy: REVIEWER.id,
      resumeTarget: {
        kind: 'thread',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
      },
    });
  });

  it('does not re-queue when the resumed run reaches a terminal status', async () => {
    // #given — a base resume that completes the run
    const store = new InMemoryApprovalStore('acme');
    const base: ResumeRunFn = async () => ({
      runId: 'acme_run-2',
      status: 'success',
      result: { done: true },
    });
    const service: ApprovalService = new ApprovalService({
      store,
      resumeRun: resumeRunWithRequeue(base, () => service, SYSTEM),
    });
    const { record } = await service.create(
      {
        workflowId: 'gtm-outbound',
        runId: 'acme_run-2',
        stepPath: ['reviewAndApprove'],
        suspendedAt: 5,
        title: 'Approve',
        connectors: ['outreach-email'],
        requestedBy: 'starter',
      },
      SYSTEM,
    );

    // #when — the reviewer approves and the run finishes
    await service.decide(record.id, { decision: 'approve' }, REVIEWER);

    // #then — no new pending approval was queued
    expect(await store.list({ status: 'pending' })).toHaveLength(0);
  });

  it('keeps the original agent principal across a reviewer-driven re-suspension', async () => {
    const store = new InMemoryApprovalStore('acme');
    const base: ResumeRunFn = async () =>
      suspendedSummary('acme_run-agent', 'gate2', ['connector'], 2020, 1);
    const service: ApprovalService = new ApprovalService({
      store,
      resumeRun: resumeRunWithRequeue(base, () => service, SYSTEM),
    });
    const { record } = await service.create(
      {
        workflowId: 'durable-agentic-loop',
        runId: 'acme_run-agent',
        stepPath: ['gate1'],
        suspendedAt: 1010,
        title: 'Approve agent action',
        connectors: ['connector'],
        requestedBy: 'starter',
      },
      SYSTEM,
      {
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        principal: {
          id: 'starter',
          role: 'operator',
          tenantId: 'acme',
        },
      },
    );

    await service.decide(record.id, { decision: 'approve' }, REVIEWER);

    const open = await store.list({ status: 'pending' });
    expect(open).toHaveLength(1);
    expect(open[0]).toMatchObject({
      stepPath: ['gate2'],
      requestedBy: 'starter',
      resumeTarget: {
        kind: 'agent-thread',
        principal: {
          id: 'starter',
          role: 'operator',
          tenantId: 'acme',
        },
      },
    });
  });

  it('fails closed: refuses to re-queue a suspension with no decider', async () => {
    // #given — the wrapper invoked directly with a record lacking decidedBy
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const base: ResumeRunFn = async () =>
      suspendedSummary('acme_run-3', 'gate2', ['c'], 9);
    const wrapped = resumeRunWithRequeue(base, () => service, SYSTEM);
    const record = { workflowId: 'wf', runId: 'acme_run-3' } as ApprovalRecord;

    // #when / #then — re-queue without a requester is a hard error, not a
    // silent fall back to the system id
    await expect(wrapped(record, 'approve')).rejects.toThrow(/decidedBy unset/);
    expect(await store.list({ status: 'pending' })).toHaveLength(0);
  });

  it('emits an audit event and reports resume.ok=false when the post-resume re-queue throws (D4)', async () => {
    // #given — deciding gate1 durably resumes to a re-suspended gate2, but
    // the store rejects gate2's filing (a transient D1 failure) — the base
    // resume has already landed by the time this fires
    const store = new InMemoryApprovalStore('acme');
    const originalCreate = store.create.bind(store);
    (store as { create: typeof store.create }).create = async (record) => {
      if (record.stepPath?.[0] === 'gate2') throw new Error('d1 hiccup');
      return originalCreate(record);
    };
    const events: ApprovalAuditEvent[] = [];
    const audit = (event: ApprovalAuditEvent) => events.push(event);
    const base: ResumeRunFn = async () =>
      suspendedSummary('acme_run-4', 'gate2', ['deploy-conn'], 3030, 1);
    const service: ApprovalService = new ApprovalService({
      store,
      audit,
      resumeRun: resumeRunWithRequeue(base, () => service, SYSTEM, audit),
    });
    const { record: gate1 } = await service.create(
      {
        workflowId: 'product-launch',
        runId: 'acme_run-4',
        stepPath: ['approveLaunch'],
        suspendedAt: 1000,
        title: 'Approve launch',
        connectors: ['deploy-conn'],
        requestedBy: 'starter',
      },
      SYSTEM,
    );

    // #when — the reviewer approves gate1; the base resume durably advances
    // the run, then filing gate2's approval fails
    const decided = await service.decide(
      gate1.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — the decision is durable regardless, but its resume is reported
    // failed rather than silently swallowed
    expect(decided.resume).toMatchObject({ attempted: true, ok: false });
    expect(decided.resume.error).toMatch(/gate filing\(s\) failed/);

    // #then — an explicit audit event flags the wedge: the run is now
    // suspended at gate2 with no approval record and no other signal
    const requeueEvents = events.filter(
      (event) => event.action === 'approval.requeue',
    );
    expect(requeueEvents).toHaveLength(1);
    expect(requeueEvents[0]).toMatchObject({
      decision: 'error',
      resource: `approval:${gate1.id}`,
      detail: {
        workflowId: 'product-launch',
        runId: 'acme_run-4',
        suspended: [['gate2']],
      },
    });
  });
});

describe('reconcileApprovalsForSummary', () => {
  it('files a fresh approval for a suspended step with no matching record at all', async () => {
    // #given — a run reported suspended, nothing ever queued for it (the D4
    // wedge: the original filing never landed)
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const summary = suspendedSummary(
      'acme_run-5',
      'gate1',
      ['deploy-conn'],
      1000,
    );

    // #when
    const filed = await reconcileApprovalsForSummary(
      service,
      'product-launch',
      summary,
      SYSTEM,
    );

    // #then
    expect(filed).toHaveLength(1);
    expect(filed[0]).toMatchObject({
      stepPath: ['gate1'],
      suspendedAt: 1000,
      connectors: ['deploy-conn'],
      status: 'pending',
    });
    expect(await store.list({ status: 'pending' })).toHaveLength(1);
  });

  it('attributes every reconcile-filed record to the SYSTEM actor, not a human', async () => {
    // #given — two gates suspended together, neither ever queued
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const summary: RunSummary = {
      runId: 'acme_run-parallel-2',
      status: 'suspended',
      suspended: [['gateA'], ['gateB']],
      suspendPayload: {
        gateA: { reason: 'gate A', connectors: ['conn-a'] },
        gateB: { reason: 'gate B', connectors: ['conn-b'] },
      },
      suspendedAt: { gateA: 111, gateB: 222 },
    };

    // #when
    const filed = await reconcileApprovalsForSummary(
      service,
      'parallel-gates',
      summary,
      SYSTEM,
    );

    // #then — unlike a human-attributed queueApprovalForSuspension call,
    // reconcile has no reviewer whose decision caused the suspension
    expect(filed).toHaveLength(2);
    expect(filed.every((record) => record.requestedBy === SYSTEM.id)).toBe(
      true,
    );
  });

  it('uses an explicitly recovered agent principal for reconcile attribution', async () => {
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const summary = suspendedSummary(
      'acme_agent-reconcile',
      'gate',
      ['connector'],
      333,
    );

    const filed = await reconcileApprovalsForSummary(
      service,
      'durable-agentic-loop',
      summary,
      SYSTEM,
      {
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        principal: {
          id: 'starter',
          role: 'operator',
          tenantId: 'acme',
        },
      },
      'starter',
    );

    expect(filed).toHaveLength(1);
    expect(filed[0]).toMatchObject({
      requestedBy: 'starter',
      resumeTarget: {
        kind: 'agent-thread',
        principal: { id: 'starter' },
      },
    });
  });

  it('does not file when a PENDING record already matches the current fingerprint', async () => {
    // #given — the normal path already queued gate1's approval
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const summary = suspendedSummary(
      'acme_run-6',
      'gate1',
      ['deploy-conn'],
      2000,
    );
    await queueApprovalForSuspension(
      service,
      'product-launch',
      summary,
      'starter',
      SYSTEM,
    );

    // #when — a status() poll reconciles the same still-suspended run
    const filed = await reconcileApprovalsForSummary(
      service,
      'product-launch',
      summary,
      SYSTEM,
    );

    // #then — nothing new queued; the existing pending record is untouched
    expect(filed).toHaveLength(0);
    expect(await store.list({ status: 'pending' })).toHaveLength(1);
  });

  it('does not file when a DECIDED record matches the current fingerprint (decide -> resume in-flight window)', async () => {
    // #given — gate1 was decided, but this summary still reports the run
    // suspended at the exact fingerprint the decision targeted — the window
    // between decide() landing and its resume completing
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const summary = suspendedSummary(
      'acme_run-7',
      'gate1',
      ['deploy-conn'],
      3000,
    );
    const [queued] = await queueApprovalForSuspension(
      service,
      'product-launch',
      summary,
      'starter',
      SYSTEM,
    );
    await service.decide(queued?.id ?? '', { decision: 'approve' }, REVIEWER);

    // #when
    const filed = await reconcileApprovalsForSummary(
      service,
      'product-launch',
      summary,
      SYSTEM,
    );

    // #then — no second record for the same suspension; re-filing here would
    // double-file a gate that is correctly waiting on its own resume
    expect(filed).toHaveLength(0);
    expect(
      await store.list({ workflowId: 'product-launch', runId: 'acme_run-7' }),
    ).toHaveLength(1);
  });

  it('files when the only existing record carries a PREVIOUS fingerprint (post-resume re-suspension)', async () => {
    // #given — gate1's first suspension was queued and decided...
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const firstSuspension = suspendedSummary(
      'acme_run-8',
      'gate1',
      ['deploy-conn'],
      4000,
    );
    const [queued] = await queueApprovalForSuspension(
      service,
      'product-launch',
      firstSuspension,
      'starter',
      SYSTEM,
    );
    await service.decide(queued?.id ?? '', { decision: 'approve' }, REVIEWER);
    // ...then the SAME step id suspended again with a NEW fingerprint
    // (suspendedAt advanced, resumeCount incremented) — the only record on
    // file is the decided one, still bound to the FIRST suspension
    const reSuspension = suspendedSummary(
      'acme_run-8',
      'gate1',
      ['deploy-conn'],
      5000,
      1,
    );

    // #when
    const filed = await reconcileApprovalsForSummary(
      service,
      'product-launch',
      reSuspension,
      SYSTEM,
    );

    // #then — the stale fingerprint does not suppress the new suspension
    expect(filed).toHaveLength(1);
    expect(filed[0]).toMatchObject({
      stepPath: ['gate1'],
      suspendedAt: 5000,
      resumeCount: 1,
      requestedBy: SYSTEM.id,
    });
  });

  it('heals a stale OPEN record in one reconcile pass; a second reconcile is a no-op (QA loop regression)', async () => {
    // #given — the QA probe: a stale open record survives 5 reconcile rounds
    // with no fingerprint healing and the same record id returned each time
    // (produced e.g. by the raw grant-free resume route re-suspending the
    // step while the old request still sits open)
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const staleSuspension = suspendedSummary(
      'acme_run-11',
      'gate1',
      ['deploy-conn'],
      10000,
    );
    const [stale] = await queueApprovalForSuspension(
      service,
      'product-launch',
      staleSuspension,
      'starter',
      SYSTEM,
    );
    const current = suspendedSummary(
      'acme_run-11',
      'gate1',
      ['deploy-conn'],
      11000,
      1,
    );

    // #when — first reconcile: supersedes the stale record and heals the
    // fingerprint in one pass
    const firstRound = await reconcileApprovalsForSummary(
      service,
      'product-launch',
      current,
      SYSTEM,
    );

    // #then — exactly one fresh record, bound to the CURRENT fingerprint
    expect(firstRound).toHaveLength(1);
    const freshId = firstRound[0]?.id;
    expect(freshId).not.toBe(stale?.id);

    // #when — four more reconcile rounds against the same still-suspended
    // summary (mirrors the QA probe's 5 rounds)
    for (let round = 0; round < 4; round += 1) {
      const again = await reconcileApprovalsForSummary(
        service,
        'product-launch',
        current,
        SYSTEM,
      );
      expect(again).toHaveLength(0);
    }

    // #then — the SAME fresh record the whole time; the stale one stayed
    // superseded, never re-touched
    const open = await store.list({ status: 'pending' });
    expect(open).toHaveLength(1);
    expect(open[0]?.id).toBe(freshId);
    expect((await store.get(stale?.id ?? ''))?.status).toBe('rejected');
  });

  it('supersedes a stale OPEN record — terminal, system decidedBy, audited — before filing fresh', async () => {
    // #given — a stale pending record for gate1, and audit wired
    const store = new InMemoryApprovalStore('acme');
    const events: ApprovalAuditEvent[] = [];
    const audit = (event: ApprovalAuditEvent) => events.push(event);
    const service = new ApprovalService({ store, audit });
    const staleSuspension = suspendedSummary(
      'acme_run-10',
      'gate1',
      ['deploy-conn'],
      8000,
    );
    const [stale] = await queueApprovalForSuspension(
      service,
      'product-launch',
      staleSuspension,
      'starter',
      SYSTEM,
    );
    const reSuspension = suspendedSummary(
      'acme_run-10',
      'gate1',
      ['deploy-conn'],
      9000,
      1,
    );

    // #when — a status() poll reconciles the run at its NEW suspension
    const filed = await reconcileApprovalsForSummary(
      service,
      'product-launch',
      reSuspension,
      SYSTEM,
    );

    // #then — a fresh record was filed, bound to the CURRENT fingerprint...
    expect(filed).toHaveLength(1);
    expect(filed[0]).toMatchObject({ suspendedAt: 9000, resumeCount: 1 });

    // ...and the stale one is terminal, attributed to the system actor —
    // never a human decision, never touching the run
    const supersededRecord = await store.get(stale?.id ?? '');
    expect(supersededRecord).toMatchObject({
      status: 'rejected',
      decidedBy: SYSTEM.id,
      decision: 'reject',
    });
    expect(supersededRecord?.comment).toMatch(/stale suspension fingerprint/);

    const supersedeEvents = events.filter(
      (event) => event.action === 'approval.supersede',
    );
    expect(supersedeEvents).toHaveLength(1);
    expect(supersedeEvents[0]).toMatchObject({
      decision: 'allowed',
      resource: `approval:${stale?.id}`,
    });
  });

  it('backs off when a concurrent decision wins the supersede CAS race (no clobber, no duplicate file)', async () => {
    // #given — a stale open record for gate1, and a store wrapped so the
    // FIRST attempt to CAS it to 'rejected' (supersedeStale's own transition)
    // instead simulates a REAL decision landing first — exactly what a
    // concurrent decide() between reconcile's list() and its supersede call
    // would produce: by the time supersedeStale's CAS runs, the record has
    // already left the OPEN set, so that CAS naturally loses.
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const staleSuspension = suspendedSummary(
      'acme_run-9',
      'gate1',
      ['deploy-conn'],
      6000,
    );
    const [stale] = await queueApprovalForSuspension(
      service,
      'product-launch',
      staleSuspension,
      'starter',
      SYSTEM,
    );
    const originalTransition = store.transition.bind(store);
    (store as { transition: typeof store.transition }).transition = async (
      id,
      from,
      patch,
    ) => {
      if (id === stale?.id && patch.status === 'rejected') {
        await originalTransition(id, ['pending', 'claimed', 'escalated'], {
          status: 'approved',
          decidedBy: REVIEWER.id,
          decision: 'approve',
          decidedAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        });
      }
      return originalTransition(id, from, patch);
    };
    const reSuspension = suspendedSummary(
      'acme_run-9',
      'gate1',
      ['deploy-conn'],
      7000,
      1,
    );

    // #when
    const filed = await reconcileApprovalsForSummary(
      service,
      'product-launch',
      reSuspension,
      SYSTEM,
    );

    // #then — reconcile backs off rather than double-filing over the record
    // a real decision just claimed
    expect(filed).toHaveLength(0);
    const after = await store.get(stale?.id ?? '');
    expect(after?.status).toBe('approved');
    expect(after?.decidedBy).toBe(REVIEWER.id);
  });

  it('heals the clean step in the same pass when a sibling step loses its supersede CAS (partial race, per-step back-off)', async () => {
    // #given — one run suspended at TWO gates, each with a stale open record;
    // gateA's supersede CAS will lose to a concurrent real decision, gateB's
    // will succeed. The back-off must be scoped per step, not per call.
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const [staleA] = await queueApprovalForSuspension(
      service,
      'product-launch',
      suspendedSummary('acme_run-14', 'gateA', ['deploy-conn'], 6000),
      'starter',
      SYSTEM,
    );
    const [staleB] = await queueApprovalForSuspension(
      service,
      'product-launch',
      suspendedSummary('acme_run-14', 'gateB', ['email-conn'], 6100),
      'starter',
      SYSTEM,
    );
    const originalTransition = store.transition.bind(store);
    (store as { transition: typeof store.transition }).transition = async (
      id,
      from,
      patch,
    ) => {
      if (id === staleA?.id && patch.status === 'rejected') {
        await originalTransition(id, ['pending', 'claimed', 'escalated'], {
          status: 'approved',
          decidedBy: REVIEWER.id,
          decision: 'approve',
          decidedAt: new Date().toISOString(),
          updatedAt: new Date().toISOString(),
        });
      }
      return originalTransition(id, from, patch);
    };
    const reSuspension: RunSummary = {
      runId: 'acme_run-14',
      status: 'suspended',
      suspended: [['gateA'], ['gateB']],
      suspendPayload: {
        gateA: { reason: 'gate gateA', connectors: ['deploy-conn'] },
        gateB: { reason: 'gate gateB', connectors: ['email-conn'] },
      },
      suspendedAt: { gateA: 7000, gateB: 7100 },
      resumeCount: { gateA: 1, gateB: 1 },
    };

    // #when
    const filed = await reconcileApprovalsForSummary(
      service,
      'product-launch',
      reSuspension,
      SYSTEM,
    );

    // #then — gateB healed (superseded + re-filed at the current
    // fingerprint) in the SAME call in which gateA backed off untouched
    expect(filed).toHaveLength(1);
    expect(filed[0]).toMatchObject({
      stepPath: ['gateB'],
      suspendedAt: 7100,
      resumeCount: 1,
    });
    const afterA = await store.get(staleA?.id ?? '');
    expect(afterA).toMatchObject({
      status: 'approved',
      decidedBy: REVIEWER.id,
    });
    const afterB = await store.get(staleB?.id ?? '');
    expect(afterB).toMatchObject({ status: 'rejected', decidedBy: SYSTEM.id });
  });

  it('excludes a superseded record from grant derivation even queried at its ORIGINAL fingerprint', async () => {
    // #given — an approval superseded via reconcile...
    const store = new InMemoryApprovalStore('acme');
    const service = new ApprovalService({ store });
    const staleSuspension = suspendedSummary(
      'acme_run-12',
      'gate1',
      ['deploy-conn'],
      12000,
    );
    const [stale] = await queueApprovalForSuspension(
      service,
      'product-launch',
      staleSuspension,
      'starter',
      SYSTEM,
    );
    const current = suspendedSummary(
      'acme_run-12',
      'gate1',
      ['deploy-conn'],
      13000,
      1,
    );
    await reconcileApprovalsForSummary(
      service,
      'product-launch',
      current,
      SYSTEM,
    );
    expect((await store.get(stale?.id ?? ''))?.status).toBe('rejected');

    // #when — grant derivation queried against the STALE record's OWN
    // original fingerprint: the most generous possible match — had the
    // record still carried status 'approved', this leg would bind to it
    // exactly (same stepPath, same suspendedAt, same resumeCount)
    const connectors = await approvedConnectorsForLeg(
      store,
      'product-launch',
      'acme_run-12',
      {
        kind: 'resume',
        step: ['gate1'],
        suspendedAt: 12000,
        resumeCount: undefined,
      },
    );

    // #then — grant derivation reads ONLY status: 'approved' records
    // (grants.ts), so the superseded record is excluded by its STATUS, not
    // merely because its fingerprint went stale
    expect(connectors).toEqual([]);
  });
});
