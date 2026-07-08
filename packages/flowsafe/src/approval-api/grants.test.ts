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

// A RE-suspension of the same 'gate' step: the leg carries a DEFINED
// resumeCount (the runtime resumed the step once, so its ordinal is 1). A
// first suspension's leg has resumeCount undefined (RESUME_GATE above) — the
// categorical difference the pair binding leans on. resumeCount is
// runtime-owned and increments on every resume regardless of payload, so it is
// present even for a no-payload re-suspension (unlike the informational
// resumedAt, which Mastra stamps only on a payload-bearing resume).
const RESUME_COUNT = 1;
const RESUME_GATE_RESUSPENDED: RunLeg = {
  kind: 'resume',
  step: ['gate'],
  suspendedAt: SUSPENDED_AT,
  resumeCount: RESUME_COUNT,
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

describe('approvedConnectorsForLeg — same-step re-suspension pair binding', () => {
  it('denies a first-suspension approval on a re-suspension leg even when suspendedAt collides', async () => {
    // #given — an approval bound to the step's FIRST suspension (resumeCount
    // undefined); the leg is a RE-suspension of the SAME step at the SAME
    // suspendedAt (the in-process same-millisecond collision). Only resumeCount
    // separates them — undefined on the record, defined on the leg. On the
    // pre-fix suspendedAt-only rule this WOULD have minted (the flake); on the
    // superseded resumedAt binding a NO-PAYLOAD re-suspension would also leak
    // (resumedAt undefined on both), which resumeCount closes.
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

    // #when / #then — the spent round-1 approval must not mint into round 2
    expect(
      await approvedConnectorsForLeg(
        store,
        'wf',
        'run-1',
        RESUME_GATE_RESUSPENDED,
      ),
    ).toEqual([]);
  });

  it('mints a re-suspension approval whose (suspendedAt, resumeCount) pair matches', async () => {
    // #given — the approval is bound to THIS re-suspension: suspendedAt and the
    // resume ordinal captured from the same suspension
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        suspendedAt: SUSPENDED_AT,
        resumeCount: RESUME_COUNT,
        connectors: ['mailer'],
        decidedAt: DECIDED_DURING,
      }),
    );

    // #when / #then
    expect(
      await approvedConnectorsForLeg(
        store,
        'wf',
        'run-1',
        RESUME_GATE_RESUSPENDED,
      ),
    ).toEqual(['mailer']);
  });

  it('denies when resumeCount differs even though suspendedAt matches (deep-chain)', async () => {
    // #given — bound to a LATER re-suspension of this step (same suspendedAt,
    // resumeCount 2) — the depth-3+ deny direction. Because the ordinal
    // strictly increments, count 2 never collides with the count-1 leg, so the
    // deterministic truthy→falsy depth-3 leak the old timestamp binding left
    // open is closed.
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        suspendedAt: SUSPENDED_AT,
        resumeCount: RESUME_COUNT + 1,
        connectors: ['mailer'],
        decidedAt: DECIDED_DURING,
      }),
    );

    // #when / #then
    expect(
      await approvedConnectorsForLeg(
        store,
        'wf',
        'run-1',
        RESUME_GATE_RESUSPENDED,
      ),
    ).toEqual([]);
  });

  it('denies a re-suspension approval on a leg whose resumeCount is undefined (reset ledger / first-suspension leg fails closed)', async () => {
    // #given — a re-suspension approval (resumeCount 1) at this suspendedAt.
    // The leg presents resumeCount undefined: either a first-suspension leg, or
    // a re-suspension leg after a DO restart reset the in-memory ledger. Either
    // way the record must NOT mint — the "fail-closed, never a leak" guarantee
    // for the restart-reset residual, at the predicate level.
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        suspendedAt: SUSPENDED_AT,
        resumeCount: 1,
        connectors: ['mailer'],
        decidedAt: DECIDED_DURING,
      }),
    );

    // #when / #then — RESUME_GATE is a leg with resumeCount undefined
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-1', RESUME_GATE),
    ).toEqual([]);
  });

  it('mints the depth-3 approval on its own leg while the spent depth-2 approval stays denied', async () => {
    // #given — two same-step approvals from consecutive re-suspensions: the
    // spent count-1 approval and the live count-2 approval; the leg is the
    // third suspension (resumeCount 2), all sharing one suspendedAt.
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        suspendedAt: SUSPENDED_AT,
        resumeCount: 1,
        connectors: ['stale'],
        decidedAt: DECIDED_DURING,
      }),
    );
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        suspendedAt: SUSPENDED_AT,
        resumeCount: 2,
        connectors: ['fresh'],
        decidedAt: DECIDED_DURING,
      }),
    );

    // #when / #then — only the count-2 approval mints; the spent count-1 does not
    expect(
      await approvedConnectorsForLeg(store, 'wf', 'run-1', {
        kind: 'resume',
        step: ['gate'],
        suspendedAt: SUSPENDED_AT,
        resumeCount: 2,
      }),
    ).toEqual(['fresh']);
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
