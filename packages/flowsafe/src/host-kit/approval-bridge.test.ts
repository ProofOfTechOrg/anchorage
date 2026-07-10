// Unit coverage for the host-kit approval bridge: the payload-shape edge cases
// of requestedConnectors, the (suspendedAt, resumeCount) capture in
// queueApprovalForSuspension, and the multi-gate re-queue + fail-closed guard in
// resumeRunWithRequeue. These are the pieces the showcase Worker and dev backend
// both depend on, so they get direct tests independent of any workflow.

import { describe, expect, it } from 'vitest';

import {
  type ApprovalActor,
  type ApprovalRecord,
  ApprovalService,
  InMemoryApprovalStore,
} from '../approval-api/index.js';
import type { RunSummary } from '../do-runner/index.js';
// requestedConnectors is module-internal (not on the barrel): it is the
// primitive beneath queueApprovalForSuspension, tested here directly.
import { requestedConnectors } from './approval-bridge.js';
import {
  queueApprovalForSuspension,
  type ResumeRunFn,
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
});
