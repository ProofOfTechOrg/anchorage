import { describe, expect, it, vi } from 'vitest';

import type { RunLeg, RunnerRuntime } from '../do-runner/index.js';
import { BREAKWATER_APPROVED_CONNECTORS_KEY } from './contract.js';
import {
  approvalGrantProvider,
  approvedConnectorsForLeg,
  defaultResumeData,
  resumeViaRuntime,
} from './grants.js';
import { InMemoryApprovalStore } from './store.js';
import type { ApprovalRecord } from './types.js';

let seq = 0;

function record(overrides: Partial<ApprovalRecord>): ApprovalRecord {
  seq += 1;
  const at = new Date(1700000000000 + seq * 1000).toISOString();
  return {
    id: `apr-${seq}`,
    workflowId: 'wf',
    runId: 'run-1',
    title: `approval ${seq}`,
    connectors: [],
    priority: 'normal',
    status: 'pending',
    createdAt: at,
    updatedAt: at,
    ...overrides,
  };
}

// The current suspension of the resumed 'gate' step began at SUSPENDED_AT;
// decisions strictly after it belong to this suspension.
const SUSPENDED_AT = Date.parse('2026-07-06T12:00:00.000Z');
const DECIDED_DURING = '2026-07-06T12:05:00.000Z';
const DECIDED_BEFORE = '2026-07-06T11:00:00.000Z';

const RESUME_GATE: RunLeg = {
  kind: 'resume',
  step: ['gate'],
  suspendedAt: SUSPENDED_AT,
};

describe('approvedConnectorsForLeg', () => {
  it('unions run-scoped records with step records decided during the current suspension', async () => {
    // #given — approved for 'gate' during this suspension, approved
    // run-scoped (no stepPath), approved for a DIFFERENT step, and
    // pending/rejected for other steps
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        connectors: ['mailer', 'blog'],
        decidedAt: DECIDED_DURING,
      }),
    );
    await store.create(
      record({ status: 'approved', connectors: ['run-wide'] }),
    );
    await store.create(
      record({
        status: 'approved',
        stepPath: ['other'],
        connectors: ['cdn'],
        decidedAt: DECIDED_DURING,
      }),
    );
    await store.create(
      record({ status: 'pending', stepPath: ['gate2'], connectors: ['nuker'] }),
    );
    await store.create(
      record({
        status: 'rejected',
        stepPath: ['gate3'],
        connectors: ['deleter'],
      }),
    );

    // #when
    const connectors = await approvedConnectorsForLeg(
      store,
      'wf',
      'run-1',
      RESUME_GATE,
    );

    // #then — 'other' step's approval does NOT leak into this leg
    expect(connectors.sort()).toEqual(['blog', 'mailer', 'run-wide']);
  });

  it('never mints an approval decided before the current suspension began', async () => {
    // #given — the step suspended once, was approved, ran, and suspended
    // AGAIN at SUSPENDED_AT; the old approval predates the new suspension
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        connectors: ['mailer'],
        decidedAt: DECIDED_BEFORE,
      }),
    );

    // #when / #then — the earlier incarnation's approval is spent; the new
    // suspension needs its own decision (a rejected or pending re-quest must
    // never fall back to it). Equality is also excluded (strictly-after):
    // chronology under a shared clock then guarantees the deny direction.
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-1', RESUME_GATE),
    ).toEqual([]);
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate2'],
        connectors: ['boundary'],
        decidedAt: new Date(SUSPENDED_AT).toISOString(),
      }),
    );
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-1', {
        kind: 'resume',
        step: ['gate2'],
        suspendedAt: SUSPENDED_AT,
      }),
    ).toEqual([]);
  });

  it('fails closed for step grants when the suspension time is unknown', async () => {
    // #given
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        connectors: ['mailer'],
        decidedAt: DECIDED_DURING,
      }),
    );
    await store.create(
      record({ status: 'approved', connectors: ['run-wide'] }),
    );

    // #when — a resume leg whose snapshot carried no suspendedAt
    const connectors = await approvedConnectorsForLeg(store, 'wf', 'run-1', {
      kind: 'resume',
      step: ['gate'],
    });

    // #then — only the standing run-scoped grant applies
    expect(connectors).toEqual(['run-wide']);
  });

  it('mints only run-scoped approvals on start legs', async () => {
    // #given
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        connectors: ['blog'],
        decidedAt: DECIDED_DURING,
      }),
    );
    await store.create(
      record({ status: 'approved', connectors: ['run-wide'] }),
    );

    // #when
    const connectors = await approvedConnectorsForLeg(store, 'wf', 'run-1', {
      kind: 'start',
    });

    // #then
    expect(connectors).toEqual(['run-wide']);
  });
});

describe('approvedConnectorsForLeg — exact suspension binding', () => {
  it('mints on an exact suspendedAt match regardless of decide timing', async () => {
    // #given — the record is bound to this suspension; decidedAt even
    // PRECEDES the suspension (impossible under one clock, routine under
    // skewed service/runner clocks) — exact binding does not care
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        suspendedAt: SUSPENDED_AT,
        connectors: ['mailer'],
        decidedAt: DECIDED_BEFORE,
      }),
    );

    // #when / #then
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-1', RESUME_GATE),
    ).toEqual(['mailer']);
  });

  it('denies a mismatched suspendedAt even when decidedAt succeeds the suspension', async () => {
    // #given — bound to an EARLIER suspension of this step; under the legacy
    // chronology rule the later decidedAt would have minted (the clock-skew
    // leak shape); exact binding closes it
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        suspendedAt: SUSPENDED_AT - 60_000,
        connectors: ['mailer'],
        decidedAt: DECIDED_DURING,
      }),
    );

    // #when / #then
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-1', RESUME_GATE),
    ).toEqual([]);
  });

  it('fails closed when the leg has no suspension time, even for a bound record', async () => {
    // #given
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        suspendedAt: SUSPENDED_AT,
        connectors: ['mailer'],
        decidedAt: DECIDED_DURING,
      }),
    );

    // #when / #then — unresolvable suspension: step grants never mint
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-1', {
        kind: 'resume',
        step: ['gate'],
      }),
    ).toEqual([]);
  });

  it('run-scopes a step-less record even when it carries suspendedAt', async () => {
    // #given — step-less = the deliberate run-scoped opt-out; suspendedAt on
    // it is inert (the applies-rule keys binding off stepPath)
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        suspendedAt: SUSPENDED_AT - 60_000,
        connectors: ['run-wide'],
      }),
    );

    // #when / #then
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-1', RESUME_GATE),
    ).toEqual(['run-wide']);
  });
});

describe('approvalGrantProvider', () => {
  it('mints the grant key for the resumed suspension only', async () => {
    // #given
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        connectors: ['mailer'],
        decidedAt: DECIDED_DURING,
      }),
    );
    await store.create(
      record({
        status: 'approved',
        stepPath: ['other'],
        connectors: ['cdn'],
        decidedAt: DECIDED_DURING,
      }),
    );
    const provider = approvalGrantProvider(store);

    // #when / #then
    expect(await provider('wf', 'run-1', RESUME_GATE)).toEqual({
      [BREAKWATER_APPROVED_CONNECTORS_KEY]: ['mailer'],
    });
  });

  it('always returns the key — empty when nothing applies — so each leg overwrites stale grants', async () => {
    // #given — an approval for a different step exists, none for this leg
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['other'],
        connectors: ['cdn'],
        decidedAt: DECIDED_DURING,
      }),
    );
    const provider = approvalGrantProvider(store);

    // #when / #then — Mastra merges provided context OVER the persisted
    // snapshot, so an omitted key would leave an earlier leg's grant alive;
    // the empty list is the revocation.
    expect(await provider('wf', 'run-1', RESUME_GATE)).toEqual({
      [BREAKWATER_APPROVED_CONNECTORS_KEY]: [],
    });
    expect(await provider('wf', 'run-1', { kind: 'start' })).toEqual({
      [BREAKWATER_APPROVED_CONNECTORS_KEY]: [],
    });
    expect(await provider('wf', 'run-1', { kind: 'resume' })).toEqual({
      [BREAKWATER_APPROVED_CONNECTORS_KEY]: [],
    });
  });
});

describe('defaultResumeData', () => {
  it('carries the outcome and decision metadata', () => {
    // #given
    const decided = record({
      status: 'approved',
      decision: 'approve',
      decidedBy: 'ray',
      comment: 'lgtm',
    });

    // #when / #then
    expect(defaultResumeData(decided, 'approve')).toEqual({
      approved: true,
      comment: 'lgtm',
      decidedBy: 'ray',
    });
    expect(defaultResumeData(record({}), 'reject')).toEqual({
      approved: false,
    });
  });
});

describe('resumeViaRuntime', () => {
  it('resumes the decided run at its recorded step with the built resumeData', async () => {
    // #given
    const resume = vi.fn().mockResolvedValue({ status: 'success' });
    const runtime = { resume } as unknown as RunnerRuntime;
    const decided = record({
      status: 'approved',
      decision: 'approve',
      decidedBy: 'ray',
      stepPath: ['approval'],
    });

    // #when
    const summary = await resumeViaRuntime(runtime)(decided, 'approve');

    // #then
    expect(summary).toEqual({ status: 'success' });
    expect(resume).toHaveBeenCalledWith('wf', 'run-1', {
      step: ['approval'],
      resumeData: { approved: true, decidedBy: 'ray' },
    });
  });

  it('honors a custom resumeData builder', async () => {
    // #given
    const resume = vi.fn().mockResolvedValue({ status: 'success' });
    const runtime = { resume } as unknown as RunnerRuntime;

    // #when
    await resumeViaRuntime(runtime, {
      resumeData: (_record, decision) => ({ approvedBy: `bot:${decision}` }),
    })(record({ stepPath: ['approval'] }), 'approve');

    // #then
    expect(resume).toHaveBeenCalledWith('wf', 'run-1', {
      step: ['approval'],
      resumeData: { approvedBy: 'bot:approve' },
    });
  });
});
