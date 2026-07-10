import { describe, expect, it } from 'vitest';

import type {
  ApprovalRecord,
  DecideResult,
} from '../../src/approval-api/types.js';
import {
  actorSwitchedEvent,
  decideEvents,
  deriveApprovalEvents,
  deriveRunEvents,
  interpretRunResult,
  type NarrationRunRef,
  shortId,
  startErrorEvent,
  startEvent,
} from './narration.js';
import { RunApiError, type RunSummary } from './run-client.js';

const RUN: NarrationRunRef = {
  workflowId: 'product-launch',
  runId: 'dmabc_11112222-3333-4444-5555-666677778888',
  title: 'Product Launch',
};

function summary(overrides: Partial<RunSummary> = {}): RunSummary {
  return { runId: RUN.runId, status: 'running', ...overrides };
}

function suspendedAt(
  step: string,
  overrides: Partial<RunSummary> = {},
): RunSummary {
  return summary({
    status: 'suspended',
    suspended: [[step]],
    suspendPayload: {
      [step]: { reason: `review ${step}`, connectors: ['release-deploy'] },
    },
    suspendedAt: { [step]: 1000 },
    ...overrides,
  });
}

function record(overrides: Partial<ApprovalRecord> = {}): ApprovalRecord {
  return {
    id: 'appr-1',
    tenantId: 'dmabc',
    workflowId: RUN.workflowId,
    runId: RUN.runId,
    stepPath: ['approveLaunch'],
    title: 'Approve launch',
    connectors: ['release-deploy'],
    priority: 'normal',
    status: 'pending',
    requestedBy: 'demo-operator',
    createdAt: '2026-07-10T00:00:00.000Z',
    updatedAt: '2026-07-10T00:00:00.000Z',
    ...overrides,
  };
}

describe('shortId', () => {
  it('takes the first 8 chars of the uuid segment of a tenant-prefixed id', () => {
    expect(shortId(RUN.runId)).toBe('11112222');
  });
  it('falls back to the first 8 chars of a plain id', () => {
    expect(shortId('abcdefghij')).toBe('abcdefgh');
  });
});

describe('deriveRunEvents', () => {
  it('yields nothing for a first sighting (prev undefined)', () => {
    expect(
      deriveRunEvents(undefined, suspendedAt('approveLaunch'), RUN),
    ).toEqual([]);
  });

  it('yields nothing when nothing changed', () => {
    const s = suspendedAt('approveLaunch');
    expect(deriveRunEvents(s, s, RUN)).toEqual([]);
  });

  it('narrates a suspension with reason, connectors, and fingerprint', () => {
    const events = deriveRunEvents(
      summary(),
      suspendedAt('approveLaunch'),
      RUN,
    );
    const suspendedEvent = events.find((e) => e.kind === 'run.suspended');
    expect(suspendedEvent).toBeDefined();
    expect(suspendedEvent?.key).toBe(
      `run:${RUN.runId}:status:suspended:approveLaunch:0`,
    );
    expect(suspendedEvent?.detail).toContain("'review approveLaunch'");
    expect(suspendedEvent?.detail).toContain('release-deploy');
    expect(suspendedEvent?.detail).toContain('suspension 1000 · resume #0');
    expect(suspendedEvent?.toast).toBe(true);
  });

  it('derives resume + grant events when a suspension ends toward success', () => {
    const prev = suspendedAt('approveLaunch');
    const next = summary({
      status: 'running',
      resumeCount: { approveLaunch: 1 },
    });
    const events = deriveRunEvents(prev, next, RUN);
    const resumed = events.find((e) => e.kind === 'run.resumed');
    const grant = events.find((e) => e.kind === 'grant.derived');
    expect(resumed?.key).toBe(`resumed:${RUN.runId}:approveLaunch:1`);
    expect(resumed?.detail).toContain('release-deploy');
    expect(grant?.key).toBe(`grant:${RUN.runId}:approveLaunch:1`);
    expect(grant?.observed).toBe(false);
  });

  it('handles gate1→gate2 in one poll: resume of gate 1 plus a "second gate" suspension', () => {
    const prev = suspendedAt('approveLaunch');
    const next = suspendedAt('confirmRollout', {
      resumeCount: { approveLaunch: 1 },
    });
    const events = deriveRunEvents(prev, next, RUN);
    const kinds = events.map((e) => e.kind);
    expect(kinds).toContain('run.resumed');
    expect(kinds).toContain('run.suspended');
    const gate2 = events.find((e) => e.kind === 'run.suspended');
    expect(gate2?.key).toBe(
      `run:${RUN.runId}:status:suspended:confirmRollout:0`,
    );
    expect(gate2?.title).toContain('Second gate');
  });

  it('titles a later gate reached across two polls via the decided-approval hint', () => {
    // running→suspended(confirmRollout): neither summary shows the spent
    // gate (the wire's fingerprints cover only current suspensions), so the
    // caller's records-derived hint is the only later-gate signal.
    const events = deriveRunEvents(
      summary(),
      suspendedAt('confirmRollout'),
      RUN,
      { laterGateHint: true },
    );
    const gate2 = events.find((e) => e.kind === 'run.suspended');
    expect(gate2?.title).toContain('Second gate');
    expect(gate2?.detail).toContain('its own decision');
  });

  it('keys a re-suspension of the SAME step by its bumped ordinal', () => {
    const prev = suspendedAt('approveLaunch');
    const next = suspendedAt('approveLaunch', {
      resumeCount: { approveLaunch: 1 },
    });
    const events = deriveRunEvents(prev, next, RUN);
    const suspendedEvent = events.find((e) => e.kind === 'run.suspended');
    expect(suspendedEvent?.key).toBe(
      `run:${RUN.runId}:status:suspended:approveLaunch:1`,
    );
    expect(suspendedEvent?.title).toContain('Second gate');
  });

  it('narrates a simulated success with the connector chain', () => {
    const prev = suspendedAt('approveLaunch');
    const next = summary({
      status: 'success',
      result: { outcome: 'simulated', healthy: true },
      resumeCount: { approveLaunch: 1 },
    });
    const events = deriveRunEvents(prev, next, RUN);
    const done = events.find((e) => e.kind === 'run.succeeded');
    expect(done?.key).toBe(`done:${RUN.runId}`);
    expect(done?.detail).toContain('simulated');
    expect(events.some((e) => e.kind === 'connector.executed')).toBe(true);
    expect(events.some((e) => e.kind === 'run.no-gate')).toBe(false);
  });

  it('narrates a declined outcome without resume toast, grant, or connector chain', () => {
    const prev = suspendedAt('approveLaunch');
    const next = summary({
      status: 'success',
      result: { outcome: 'declined' },
      resumeCount: { approveLaunch: 1 },
    });
    const events = deriveRunEvents(prev, next, RUN);
    expect(events.some((e) => e.kind === 'grant.derived')).toBe(false);
    expect(events.some((e) => e.kind === 'connector.executed')).toBe(false);
    const resumed = events.find((e) => e.kind === 'run.resumed');
    expect(resumed?.toast).toBe(false);
    const done = events.find((e) => e.kind === 'run.succeeded');
    expect(done?.detail).toContain('declined');
  });

  it('flags a success that never suspended as a gate short-circuit', () => {
    const events = deriveRunEvents(
      summary(),
      summary({ status: 'success', result: { assigned: 0 } }),
      RUN,
    );
    expect(events.some((e) => e.kind === 'run.no-gate')).toBe(true);
  });

  it("never claims a rejection for a short-circuited run the workflow labels 'declined'", () => {
    // lead-generation's all-cold result is {assigned: 0, outcome: 'declined'}
    // — nobody rejected anything; the gate never ran.
    const events = deriveRunEvents(
      summary(),
      summary({
        status: 'success',
        result: { assigned: 0, outcome: 'declined' },
      }),
      RUN,
    );
    const done = events.find((e) => e.kind === 'run.succeeded');
    expect(done?.detail).not.toContain('rejection');
    expect(done?.detail).toContain('without reaching its gate');
    expect(events.some((e) => e.kind === 'run.no-gate')).toBe(true);
  });

  it('suppresses the short-circuit line when an approval proves a gate ran', () => {
    // running→success with the resume ledger already dropped at terminal —
    // only the hint (an approval exists for this run) knows a gate suspended.
    const events = deriveRunEvents(
      summary(),
      summary({ status: 'success', result: { outcome: 'simulated' } }),
      RUN,
      { everSuspendedHint: true },
    );
    expect(events.some((e) => e.kind === 'run.no-gate')).toBe(false);
  });

  it('narrates a failed run with a sticky toast', () => {
    const events = deriveRunEvents(
      summary(),
      summary({ status: 'failed', error: 'boom' }),
      RUN,
    );
    const done = events.find((e) => e.kind === 'run.failed');
    expect(done?.toastSticky).toBe(true);
    expect(done?.detail).toContain('boom');
  });

  it('is key-idempotent: deriving the same transition twice yields identical keys', () => {
    const prev = suspendedAt('approveLaunch');
    const next = summary({
      status: 'success',
      result: { outcome: 'simulated' },
      resumeCount: { approveLaunch: 1 },
    });
    const first = deriveRunEvents(prev, next, RUN).map((e) => e.key);
    const second = deriveRunEvents(prev, next, RUN).map((e) => e.key);
    expect(second).toEqual(first);
  });

  it('produces deterministic keys for EVERY transition shape (the dedup contract)', () => {
    // The feed/toast layers dedup by key; a wall-clock or random component in
    // any snapshot-derived key would break StrictMode/poll-race safety. This
    // matrix guards the contract for every deriver path at once.
    const transitions: Array<[RunSummary | undefined, RunSummary]> = [
      [undefined, suspendedAt('approveLaunch')],
      [summary(), suspendedAt('approveLaunch')],
      [suspendedAt('approveLaunch'), summary({ status: 'running' })],
      [
        suspendedAt('approveLaunch'),
        suspendedAt('confirmRollout', { resumeCount: { approveLaunch: 1 } }),
      ],
      [
        suspendedAt('approveLaunch'),
        suspendedAt('approveLaunch', { resumeCount: { approveLaunch: 1 } }),
      ],
      [
        suspendedAt('approveLaunch'),
        summary({ status: 'success', result: { outcome: 'simulated' } }),
      ],
      [
        suspendedAt('approveLaunch'),
        summary({ status: 'success', result: { outcome: 'declined' } }),
      ],
      [summary(), summary({ status: 'success', result: { assigned: 0 } })],
      [summary(), summary({ status: 'failed', error: 'boom' })],
    ];
    for (const [prev, next] of transitions) {
      for (const options of [
        {},
        { everSuspendedHint: true, laterGateHint: true },
      ]) {
        const first = deriveRunEvents(prev, next, RUN, options).map(
          (e) => e.key,
        );
        const second = deriveRunEvents(prev, next, RUN, options).map(
          (e) => e.key,
        );
        expect(second).toEqual(first);
      }
    }
    const before = record();
    const flips = [
      record({ status: 'claimed' as const, claimedBy: 'demo-reviewer' }),
      record({
        status: 'approved' as const,
        decision: 'approve' as const,
        decidedBy: 'demo-reviewer',
      }),
      record({ status: 'escalated' as const }),
    ];
    for (const after of flips) {
      const prevMap = new Map([[before.id, before]]);
      const first = deriveApprovalEvents(prevMap, [after]).map((e) => e.key);
      const second = deriveApprovalEvents(prevMap, [after]).map((e) => e.key);
      expect(second).toEqual(first);
    }
  });
});

describe('deriveApprovalEvents', () => {
  it('yields nothing for the first snapshot (prev undefined)', () => {
    expect(deriveApprovalEvents(undefined, [record()])).toEqual([]);
  });

  it('narrates a new record as queued (plus the one-time cron mention)', () => {
    const events = deriveApprovalEvents(new Map(), [record()]);
    const queued = events.find((e) => e.kind === 'approval.queued');
    expect(queued?.key).toBe('approval:appr-1:status:pending');
    expect(queued?.toast).toBe(false);
    expect(events.some((e) => e.key === 'cron:mention')).toBe(true);
  });

  it('narrates claim, decision, escalation, and delegation flips', () => {
    const before = record();
    const claimed = deriveApprovalEvents(new Map([[before.id, before]]), [
      record({ status: 'claimed', claimedBy: 'demo-reviewer' }),
    ]);
    expect(claimed[0]?.key).toBe('approval:appr-1:status:claimed');
    expect(claimed[0]?.toast).toBe(true);

    const decided = deriveApprovalEvents(new Map([[before.id, before]]), [
      record({
        status: 'approved',
        decision: 'approve',
        decidedBy: 'demo-reviewer',
        comment: 'ship it',
      }),
    ]);
    expect(decided[0]?.key).toBe('decide:appr-1:approve');
    expect(decided[0]?.detail).toContain('ship it');

    const escalated = deriveApprovalEvents(new Map([[before.id, before]]), [
      record({ status: 'escalated' }),
    ]);
    expect(escalated[0]?.zone).toBe('cron');
    expect(escalated[0]?.toast).toBe(false);

    const wasClaimed = record({
      status: 'claimed',
      claimedBy: 'demo-reviewer',
    });
    const delegated = deriveApprovalEvents(
      new Map([[wasClaimed.id, wasClaimed]]),
      [record({ status: 'claimed', claimedBy: 'demo-admin' })],
    );
    expect(delegated[0]?.kind).toBe('approval.delegated');
    expect(delegated[0]?.key).toBe('approval:appr-1:claimedBy:demo-admin');
  });
});

describe('startEvent', () => {
  it('merges start+suspend: the suspension toasts, the start does not', () => {
    const response = {
      ...suspendedAt('approveLaunch'),
      approval: { id: 'appr-9' },
    };
    const events = startEvent(RUN, response, {
      actor: { id: 'demo-operator', role: 'operator' },
      steps: ['validateReadiness', 'approveLaunch'],
    });
    const started = events.find((e) => e.kind === 'run.started');
    const suspended = events.find((e) => e.kind === 'run.suspended');
    expect(started?.toast).toBe(false);
    expect(suspended?.toast).toBe(true);
    // Same key the poll would derive — the feed dedups either order.
    expect(suspended?.key).toBe(
      `run:${RUN.runId}:status:suspended:approveLaunch:0`,
    );
    expect(events.find((e) => e.kind === 'approval.queued')?.key).toBe(
      'approval:appr-9:status:pending',
    );
    expect(events.some((e) => e.kind === 'do.spawned' && !e.observed)).toBe(
      true,
    );
    expect(events.some((e) => e.kind === 'do.steps-executed')).toBe(true);
  });

  it('toasts a plain start and narrates an immediate terminal state', () => {
    const plain = startEvent(RUN, summary());
    expect(plain.find((e) => e.kind === 'run.started')?.toast).toBe(true);

    const immediate = startEvent(
      RUN,
      summary({ status: 'success', result: { assigned: 0 } }),
    );
    expect(immediate.find((e) => e.kind === 'run.started')?.toast).toBe(false);
    expect(immediate.some((e) => e.key === `done:${RUN.runId}`)).toBe(true);
  });
});

describe('startErrorEvent', () => {
  it('maps 429 to a sticky budget toast quoting the server message verbatim', () => {
    const event = startErrorEvent(
      'gtm-outbound',
      new RunApiError(429, 'demo run budget exhausted for this sandbox'),
    );
    expect(event.key).toBe('429');
    expect(event.toastSticky).toBe(true);
    expect(event.detail).toBe('demo run budget exhausted for this sandbox');
  });

  it('maps 403 to a role-gate event keyed by workflow and role', () => {
    const event = startErrorEvent(
      'access-request',
      new RunApiError(403, 'role operator may not start this workflow'),
      'operator',
    );
    expect(event.key).toBe('403:access-request:operator');
    expect(event.kind).toBe('authz.denied');
    expect(event.toastSticky).toBeUndefined();
  });

  it('maps 503 to the kill-switch event and anything else to a generic failure', () => {
    expect(startErrorEvent('x', new RunApiError(503, 'disabled')).key).toBe(
      '503',
    );
    const generic = startErrorEvent('x', new Error('network down'));
    expect(generic.kind).toBe('run.start-failed');
    expect(generic.detail).toBe('network down');
  });
});

describe('decideEvents', () => {
  function decideResult(overrides: {
    decision: 'approve' | 'reject';
    resume: DecideResult['resume'];
  }): DecideResult {
    return {
      record: record({
        status: overrides.decision === 'approve' ? 'approved' : 'rejected',
        decision: overrides.decision,
        decidedBy: 'demo-reviewer',
      }),
      resume: overrides.resume,
    };
  }

  it('narrates an approve with inline resume, pre-recording the poll keys', () => {
    const events = decideEvents(
      decideResult({
        decision: 'approve',
        resume: {
          attempted: true,
          ok: true,
          summary: summary({ resumeCount: { approveLaunch: 1 } }),
        },
      }),
    );
    expect(events[0]?.key).toBe('decide:appr-1:approve');
    expect(events[0]?.toast).toBe(true);
    const resumed = events.find((e) => e.kind === 'run.resumed');
    expect(resumed?.key).toBe(`resumed:${RUN.runId}:approveLaunch:1`);
    expect(resumed?.toast).toBe(false);
    expect(events.some((e) => e.kind === 'grant.derived')).toBe(true);
  });

  it('narrates a reject without a grant event', () => {
    const events = decideEvents(
      decideResult({
        decision: 'reject',
        resume: { attempted: true, ok: true, summary: summary() },
      }),
    );
    expect(events[0]?.title).toContain('no grant');
    expect(events.some((e) => e.kind === 'grant.derived')).toBe(false);
  });

  it('adds the resume-failed event when the inline resume did not complete', () => {
    const events = decideEvents(
      decideResult({
        decision: 'approve',
        resume: { attempted: true, ok: false, error: 'DO unreachable' },
      }),
    );
    const failed = events.find((e) => e.kind === 'run.resume-failed');
    expect(failed?.key).toBe('resume-failed:appr-1');
    expect(failed?.tone).toBe('danger');
  });

  it('emits no resume events when no resume was attempted', () => {
    const events = decideEvents(
      decideResult({ decision: 'approve', resume: { attempted: false } }),
    );
    expect(events).toHaveLength(1);
  });

  it('pins the stepless-record behavior: decide narrates, resume elaboration is skipped', () => {
    // Only reachable via a runScoped standing approval (no stepPath) — none
    // of the showcase workflows produce one. The decide toast still lands;
    // the per-leg resumed/grant lines need a step to key on and are dropped.
    const result = decideEvents({
      record: record({
        status: 'approved',
        decision: 'approve',
        decidedBy: 'demo-reviewer',
        stepPath: undefined,
      }),
      resume: { attempted: true, ok: true, summary: summary() },
    });
    expect(result.map((e) => e.kind)).toEqual(['approval.decided']);
  });
});

describe('interpretRunResult', () => {
  it('classifies the five showcase result shapes', () => {
    expect(interpretRunResult({ outcome: 'simulated' }).flavor).toBe(
      'simulated',
    );
    expect(interpretRunResult({ outcome: 'declined' }).flavor).toBe('declined');
    expect(interpretRunResult({ granted: false }).flavor).toBe('declined');
    expect(interpretRunResult({ outcome: 'preview' }).flavor).toBe('preview');
    expect(
      interpretRunResult({ published: true, key: 'content-pipeline/r/a.md' })
        .line,
    ).toContain('key content-pipeline/r/a.md');
    expect(interpretRunResult({ outcome: 'sent' }).flavor).toBe('delivered');
    // Only reachable with a live binding configured, so the line may say so.
    expect(interpretRunResult({ outcome: 'assigned' }).flavor).toBe(
      'delivered',
    );
    expect(interpretRunResult({ outcome: 'sent' }).line).toContain(
      'live connector binding',
    );
    expect(interpretRunResult('nope').flavor).toBe('plain');
  });

  it('detects replayed at the top level and one level down', () => {
    expect(interpretRunResult({ replayed: true }).replayed).toBe(true);
    expect(interpretRunResult({ publish: { replayed: true } }).replayed).toBe(
      true,
    );
    expect(interpretRunResult({ outcome: 'simulated' }).replayed).toBe(false);
  });
});

describe('actorSwitchedEvent', () => {
  it('replaces the live toast instead of stacking', () => {
    const event = actorSwitchedEvent('demo-reviewer', 'reviewer');
    expect(event.toastReplaceId).toBe('actor-switch');
    expect(event.toast).toBe(true);
  });
});
