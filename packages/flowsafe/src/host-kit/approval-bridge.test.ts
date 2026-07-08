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
import {
  queueApprovalForSuspension,
  requestedConnectors,
  type ResumeRunFn,
  resumeRunWithRequeue,
} from './index.js';

const SYSTEM: ApprovalActor = { id: 'sys', role: 'operator' };
const REVIEWER: ApprovalActor = { id: 'ray', role: 'reviewer' };

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
    const store = new InMemoryApprovalStore();
    const service = new ApprovalService({ store });
    const summary = suspendedSummary(
      'run-1',
      'gate2',
      ['deploy-conn'],
      1717,
      2,
    );

    // #when — the bridge queues the approval
    const record = await queueApprovalForSuspension(
      service,
      'product-launch',
      summary,
      'reviewer-of-gate1',
      SYSTEM,
    );

    // #then — the binding fingerprint is copied verbatim from the summary
    expect(record).toMatchObject({
      workflowId: 'product-launch',
      runId: 'run-1',
      stepPath: ['gate2'],
      suspendedAt: 1717,
      resumeCount: 2,
      connectors: ['deploy-conn'],
      requestedBy: 'reviewer-of-gate1',
      status: 'pending',
    });
  });
});

describe('resumeRunWithRequeue', () => {
  it('re-queues the next gate attributed to the decider (SoD across gates)', async () => {
    // #given — a service whose base resume re-suspends the run at a 2nd gate
    const store = new InMemoryApprovalStore();
    const base: ResumeRunFn = async () =>
      suspendedSummary('run-1', 'confirmRollout', ['deploy-conn'], 2020, 1);
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
        runId: 'run-1',
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
    const store = new InMemoryApprovalStore();
    const base: ResumeRunFn = async () => ({
      runId: 'run-2',
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
        runId: 'run-2',
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
    const store = new InMemoryApprovalStore();
    const service = new ApprovalService({ store });
    const base: ResumeRunFn = async () =>
      suspendedSummary('run-3', 'gate2', ['c'], 9);
    const wrapped = resumeRunWithRequeue(base, () => service, SYSTEM);
    const record = { workflowId: 'wf', runId: 'run-3' } as ApprovalRecord;

    // #when / #then — re-queue without a requester is a hard error, not a
    // silent fall back to the system id
    await expect(wrapped(record, 'approve')).rejects.toThrow(/decidedBy unset/);
    expect(await store.list({ status: 'pending' })).toHaveLength(0);
  });
});
