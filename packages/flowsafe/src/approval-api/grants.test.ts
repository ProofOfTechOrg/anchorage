// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import type { RunLeg, RunnerRuntime } from '../do-runner/index.js';
import { BREAKWATER_CONNECTOR_GRANTS_KEY } from './contract.js';
import {
  approvalGrantProvider,
  connectorGrantsForLeg,
  defaultResumeData,
  resumeViaRuntime,
} from './grants.js';
import { InMemoryApprovalStore } from './store.js';
import { type ApprovalRecord, MAX_APPROVAL_LIST_LIMIT } from './types.js';

let seq = 0;

function record(overrides: Partial<ApprovalRecord>): ApprovalRecord {
  seq += 1;
  const at = new Date(1700000000000 + seq * 1000).toISOString();
  const connectorCount = overrides.connectors?.length ?? 0;
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
    ...(connectorCount > 0 && overrides.stepPath !== undefined
      ? {
          grantScope:
            overrides.toolCallId === undefined
              ? ('suspension' as const)
              : ('tool-call' as const),
          suspendedAt: SUSPENDED_AT,
        }
      : connectorCount > 0 && overrides.runScoped === true
        ? { grantScope: 'run' as const }
        : {}),
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

function suspensionGrant(
  connectorId: string,
  options: {
    stepPath?: readonly string[];
    suspendedAt?: number;
    resumeCount?: number;
    toolCallId?: string;
  } = {},
) {
  const suspension = {
    stepPath: options.stepPath ?? ['gate'],
    suspendedAt: options.suspendedAt ?? SUSPENDED_AT,
    ...(options.resumeCount === undefined
      ? {}
      : { resumeCount: options.resumeCount }),
  };
  return options.toolCallId === undefined
    ? {
        scope: 'suspension' as const,
        connectorId,
        workflowId: 'wf',
        runId: 'run-1',
        suspension,
      }
    : {
        scope: 'tool-call' as const,
        connectorId,
        workflowId: 'wf',
        runId: 'run-1',
        suspension,
        toolCallId: options.toolCallId,
      };
}

function runGrant(
  connectorId: string,
  options: {
    runId?: string;
  } = {},
) {
  return {
    scope: 'run' as const,
    connectorId,
    workflowId: 'wf',
    runId: options.runId ?? 'run-1',
  };
}

describe('connectorGrantsForLeg', () => {
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
      record({ status: 'approved', runScoped: true, connectors: ['run-wide'] }),
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
    const grants = await connectorGrantsForLeg(
      store,
      'wf',
      'run-1',
      RESUME_GATE,
    );

    // #then — 'other' step's approval does NOT leak into this leg
    expect(grants).toEqual(
      expect.arrayContaining([
        suspensionGrant('mailer'),
        suspensionGrant('blog'),
        runGrant('run-wide'),
      ]),
    );
    expect(grants).toHaveLength(3);
  });

  it('never falls back to decision chronology for a legacy capability record', async () => {
    // #given — a pre-upgrade record names a connector but has neither the
    // explicit grant scope nor an exact suspension fingerprint
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        stepPath: ['gate'],
        connectors: ['mailer'],
        grantScope: undefined,
        suspendedAt: undefined,
        decidedAt: DECIDED_BEFORE,
      }),
    );

    // #when / #then — even moving decidedAt after the leg would not mint:
    // legacy identity is inert rather than reconstructed from timestamps
    expect(
      await connectorGrantsForLeg(store, 'wf', 'run-1', RESUME_GATE),
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
      record({ status: 'approved', runScoped: true, connectors: ['run-wide'] }),
    );

    // #when — a resume leg whose snapshot carried no suspendedAt
    const grants = await connectorGrantsForLeg(store, 'wf', 'run-1', {
      kind: 'resume',
      step: ['gate'],
    });

    // #then — only the standing run-scoped grant applies
    expect(grants).toEqual([runGrant('run-wide')]);
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
      record({ status: 'approved', runScoped: true, connectors: ['run-wide'] }),
    );

    // #when
    const grants = await connectorGrantsForLeg(store, 'wf', 'run-1', {
      kind: 'start',
    });

    // #then
    expect(grants).toEqual([runGrant('run-wide')]);
  });

  it('fails closed: a step-less record without runScoped mints nothing', async () => {
    // #given — run-scope is EXPLICIT. An approved record that names neither a
    // step nor runScoped is inert on every leg: "absent field => maximal
    // privilege" was the inverted default that let an HTTP-authored record
    // become a standing grant.
    const store = new InMemoryApprovalStore();
    await store.create(
      record({ status: 'approved', connectors: ['smuggled'] }),
    );

    // #when / #then — start, resume-with-step, and unresolvable resume alike
    expect(
      await connectorGrantsForLeg(store, 'wf', 'run-1', { kind: 'start' }),
    ).toEqual([]);
    expect(
      await connectorGrantsForLeg(store, 'wf', 'run-1', RESUME_GATE),
    ).toEqual([]);
    expect(
      await connectorGrantsForLeg(store, 'wf', 'run-1', { kind: 'resume' }),
    ).toEqual([]);
  });

  it('denies a runScoped:false record exactly as it denies an absent flag', async () => {
    // #given — the flag is a tri-state on the wire (D1 stores 0/1/NULL); only
    // an explicit true opts in
    const store = new InMemoryApprovalStore();
    await store.create(
      record({ status: 'approved', runScoped: false, connectors: ['nope'] }),
    );

    // #when / #then
    expect(
      await connectorGrantsForLeg(store, 'wf', 'run-1', { kind: 'start' }),
    ).toEqual([]);
  });
});

describe('connectorGrantsForLeg — exact suspension binding', () => {
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
      await connectorGrantsForLeg(store, 'wf', 'run-1', RESUME_GATE),
    ).toEqual([suspensionGrant('mailer')]);
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
      await connectorGrantsForLeg(store, 'wf', 'run-1', RESUME_GATE),
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
      await connectorGrantsForLeg(store, 'wf', 'run-1', {
        kind: 'resume',
        step: ['gate'],
      }),
    ).toEqual([]);
  });

  it('fails closed when a run-scoped record also carries suspension identity', async () => {
    // #given — a malformed standing grant mixes broad run scope with an exact
    // suspension fingerprint
    const store = new InMemoryApprovalStore();
    await store.create(
      record({
        status: 'approved',
        runScoped: true,
        suspendedAt: SUSPENDED_AT - 60_000,
        connectors: ['run-wide'],
      }),
    );

    // #when / #then
    expect(
      await connectorGrantsForLeg(store, 'wf', 'run-1', RESUME_GATE),
    ).toEqual([]);
  });
});

describe('connectorGrantsForLeg — same-step re-suspension pair binding', () => {
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
      await connectorGrantsForLeg(
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
      await connectorGrantsForLeg(
        store,
        'wf',
        'run-1',
        RESUME_GATE_RESUSPENDED,
      ),
    ).toEqual([suspensionGrant('mailer', { resumeCount: RESUME_COUNT })]);
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
      await connectorGrantsForLeg(
        store,
        'wf',
        'run-1',
        RESUME_GATE_RESUSPENDED,
      ),
    ).toEqual([]);
  });

  it('denies a re-suspension approval on a leg whose resumeCount is undefined', async () => {
    // #given — a re-suspension approval (resumeCount 1) at this suspendedAt.
    // The leg presents resumeCount undefined, so it represents a first
    // suspension or missing provenance. Either way the record must NOT mint.
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
      await connectorGrantsForLeg(store, 'wf', 'run-1', RESUME_GATE),
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
      await connectorGrantsForLeg(store, 'wf', 'run-1', {
        kind: 'resume',
        step: ['gate'],
        suspendedAt: SUSPENDED_AT,
        resumeCount: 2,
      }),
    ).toEqual([suspensionGrant('fresh', { resumeCount: 2 })]);
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
      [BREAKWATER_CONNECTOR_GRANTS_KEY]: [suspensionGrant('mailer')],
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
      [BREAKWATER_CONNECTOR_GRANTS_KEY]: [],
    });
    expect(await provider('wf', 'run-1', { kind: 'start' })).toEqual({
      [BREAKWATER_CONNECTOR_GRANTS_KEY]: [],
    });
    expect(await provider('wf', 'run-1', { kind: 'resume' })).toEqual({
      [BREAKWATER_CONNECTOR_GRANTS_KEY]: [],
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
      requestedBy: 'ray',
      requestedByKind: 'human',
    });
  });

  it('honors a custom resumeData builder', async () => {
    // #given
    const resume = vi.fn().mockResolvedValue({ status: 'success' });
    const runtime = { resume } as unknown as RunnerRuntime;

    // #when
    await resumeViaRuntime(runtime, {
      resumeData: (_record, decision) => ({ approvedBy: `bot:${decision}` }),
    })(record({ stepPath: ['approval'], decidedBy: 'ray' }), 'approve');

    // #then
    expect(resume).toHaveBeenCalledWith('wf', 'run-1', {
      step: ['approval'],
      resumeData: { approvedBy: 'bot:approve' },
      requestedBy: 'ray',
      requestedByKind: 'human',
    });
  });
});

describe('grant derivation — exact run binding', () => {
  it('pins the LOAD-BEARING list filter: {workflowId, runId, status} — the runId predicate must never be optimized away', async () => {
    // #given — a spy store: the runId predicate keeps one run's decisions from
    // minting capabilities for another run in the deployment.
    const listCalls: unknown[] = [];
    const store = new InMemoryApprovalStore();
    const spy: typeof store = Object.create(store, {
      list: {
        value: (filter: unknown) => {
          listCalls.push(filter);
          return store.list(filter as never);
        },
      },
    });

    // #when
    await connectorGrantsForLeg(spy, 'wf', 'acme_run-1', { kind: 'start' });

    // #then — one page (the fixture is far under the cap) whose filter carries
    // all three load-bearing predicates; explicit paging adds limit/after but
    // must never drop workflowId/runId/status (the runId predicate is the
    // exact-run guard)
    expect(listCalls).toHaveLength(1);
    expect(listCalls[0]).toMatchObject({
      workflowId: 'wf',
      runId: 'acme_run-1',
      status: 'approved',
    });
  });

  it('mints from ALL approved records past the list cap — a run with >MAX approvals still unlocks the newest connector (D3 grant-path paging)', async () => {
    // #given — more approved records for one run than the D3 bare-list cap:
    // MAX run-scoped 'noise' records, then a NEWEST run-scoped record carrying
    // 'newest-connector'. A single bounded FIFO page (the D3 store
    // default) returns the oldest MAX and DROPS the newest, failing the grant
    // CLOSED. connectorGrantsForLeg must page the complete set instead.
    const store = new InMemoryApprovalStore();
    for (let index = 0; index < MAX_APPROVAL_LIST_LIMIT; index += 1) {
      await store.create(
        record({ status: 'approved', runScoped: true, connectors: ['noise'] }),
      );
    }
    await store.create(
      record({
        status: 'approved',
        runScoped: true,
        connectors: ['newest-connector'],
      }),
    );

    // #when — a start leg mints run-scoped grants
    const grants = await connectorGrantsForLeg(store, 'wf', 'run-1', {
      kind: 'start',
    });

    // #then — the newest record's connector is present (complete paging, not a
    // truncated first page)
    expect(grants).toContainEqual(runGrant('newest-connector'));
  });
});
