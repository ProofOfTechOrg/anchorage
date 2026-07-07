import { describe, expect, it, vi } from 'vitest';

import type { ApprovalActor, ApprovalAuditEvent } from './contract.js';
import {
  ApprovalAuthzError,
  ApprovalConflictError,
  ApprovalService,
  type ApprovalServiceOptions,
  InvalidApprovalInputError,
  UnknownApprovalError,
} from './service.js';
import { InMemoryApprovalStore } from './store.js';
import type { ApprovalRecord, CreateApprovalInput } from './types.js';

const ADMIN: ApprovalActor = { id: 'ada', role: 'admin' };
const OPERATOR: ApprovalActor = { id: 'opal', role: 'operator' };
const REVIEWER: ApprovalActor = { id: 'ray', role: 'reviewer' };
const VIEWER: ApprovalActor = { id: 'vic', role: 'viewer' };

const T0 = Date.parse('2026-07-06T12:00:00.000Z');

interface Harness {
  service: ApprovalService;
  store: InMemoryApprovalStore;
  events: ApprovalAuditEvent[];
  advance: (ms: number) => void;
}

function makeHarness(options: Partial<ApprovalServiceOptions> = {}): Harness {
  const store = new InMemoryApprovalStore();
  const events: ApprovalAuditEvent[] = [];
  let nowMs = T0;
  const service = new ApprovalService({
    store,
    audit: (event) => events.push(event),
    now: () => new Date(nowMs),
    ...options,
  });
  return {
    service,
    store,
    events,
    advance: (ms) => {
      nowMs += ms;
    },
  };
}

function input(
  overrides: Partial<CreateApprovalInput> = {},
): CreateApprovalInput {
  return {
    workflowId: 'wf',
    runId: 'run-1',
    title: 'publish launch post',
    ...overrides,
  };
}

async function seedPending(
  harness: Harness,
  overrides: Partial<CreateApprovalInput> = {},
): Promise<ApprovalRecord> {
  const { record } = await harness.service.create(input(overrides), OPERATOR);
  return record;
}

describe('ApprovalService.create', () => {
  it('creates a pending record with an SLA deadline from slaSeconds', async () => {
    // #given
    const harness = makeHarness();

    // #when
    const { record, created } = await harness.service.create(
      input({
        connectors: ['blog-publisher'],
        priority: 'high',
        slaSeconds: 3600,
        stepPath: ['approval'],
        payload: { reason: 'needs sign-off' },
      }),
      OPERATOR,
    );

    // #then
    expect(created).toBe(true);
    expect(record).toMatchObject({
      workflowId: 'wf',
      runId: 'run-1',
      status: 'pending',
      priority: 'high',
      connectors: ['blog-publisher'],
      stepPath: ['approval'],
      createdAt: '2026-07-06T12:00:00.000Z',
      slaDeadlineAt: '2026-07-06T13:00:00.000Z',
    });
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.create',
        decision: 'allowed',
        actor: OPERATOR,
      }),
    );
  });

  it('applies defaultSlaSeconds when the input has none', async () => {
    // #given
    const harness = makeHarness({ defaultSlaSeconds: 600 });

    // #when
    const { record } = await harness.service.create(input(), OPERATOR);

    // #then
    expect(record.slaDeadlineAt).toBe('2026-07-06T12:10:00.000Z');
  });

  it('returns the existing open record for a duplicate create', async () => {
    // #given
    const harness = makeHarness();
    const first = await seedPending(harness);

    // #when
    const second = await harness.service.create(input(), OPERATOR);

    // #then
    expect(second.created).toBe(false);
    expect(second.record.id).toBe(first.id);
  });

  it('denies creation to roles outside operator/builder/admin', async () => {
    // #given
    const harness = makeHarness();

    // #when / #then
    await expect(
      harness.service.create(input(), VIEWER),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
    await expect(
      harness.service.create(input(), REVIEWER),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
    expect(harness.events).toContainEqual(
      expect.objectContaining({ decision: 'denied', actor: VIEWER }),
    );
  });

  it('denies an actor whose id is an empty string despite a valid role', async () => {
    // #given — a well-formed role but no identity; authz requires a non-empty id
    const harness = makeHarness();
    const anonymous = { id: '', role: 'admin' } as ApprovalActor;

    // #when / #then — fails closed, not treated as an authenticated admin
    await expect(
      harness.service.create(input(), anonymous),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
  });

  it('rejects invalid input field by field', async () => {
    // #given
    const harness = makeHarness();
    const bad: Array<Partial<CreateApprovalInput>> = [
      { title: '' },
      { workflowId: '' },
      { runId: '' },
      { priority: 'urgent' as CreateApprovalInput['priority'] },
      { slaSeconds: -5 },
      { slaSeconds: Number.NaN },
      { connectors: ['ok', ''] },
      { stepPath: [''] },
    ];

    // #when / #then
    for (const overrides of bad) {
      await expect(
        harness.service.create(input(overrides), OPERATOR),
      ).rejects.toBeInstanceOf(InvalidApprovalInputError);
    }
  });
});

describe('ApprovalService.claim', () => {
  it('claims a pending request for the reviewer', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when
    const claimed = await harness.service.claim(record.id, REVIEWER);

    // #then
    expect(claimed).toMatchObject({
      status: 'claimed',
      claimedBy: 'ray',
      claimedAt: '2026-07-06T12:00:00.000Z',
    });
  });

  it('conflicts on claiming an already-claimed request', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);
    await harness.service.claim(record.id, REVIEWER);

    // #when
    let caught: unknown;
    try {
      await harness.service.claim(record.id, ADMIN);
    } catch (error) {
      caught = error;
    }

    // #then — reassignment is delegate()'s job
    expect(caught).toBeInstanceOf(ApprovalConflictError);
    expect((caught as ApprovalConflictError).currentStatus).toBe('claimed');
  });

  it('throws UnknownApprovalError for a missing id', async () => {
    // #given
    const harness = makeHarness();

    // #when / #then
    await expect(
      harness.service.claim('missing', REVIEWER),
    ).rejects.toBeInstanceOf(UnknownApprovalError);
  });

  it('denies claim to non-reviewer roles', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when / #then
    await expect(
      harness.service.claim(record.id, OPERATOR),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
  });

  it('denies actors with unknown roles', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);
    const impostor = {
      id: 'mal',
      role: 'superuser',
    } as unknown as ApprovalActor;

    // #when / #then
    await expect(
      harness.service.claim(record.id, impostor),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
  });
});

describe('ApprovalService.decide', () => {
  it('approves with decision metadata and reports resume unattempted when unwired', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when
    const result = await harness.service.decide(
      record.id,
      { decision: 'approve', comment: 'lgtm' },
      REVIEWER,
    );

    // #then
    expect(result.record).toMatchObject({
      status: 'approved',
      decision: 'approve',
      decidedBy: 'ray',
      comment: 'lgtm',
      decidedAt: '2026-07-06T12:00:00.000Z',
    });
    expect(result.resume).toEqual({ attempted: false });
  });

  it('resumes the run after a decision and reports the summary', async () => {
    // #given
    const resumeRun = vi.fn().mockResolvedValue({ status: 'success' });
    const harness = makeHarness({ resumeRun });
    const record = await seedPending(harness);

    // #when
    const result = await harness.service.decide(
      record.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then
    expect(resumeRun).toHaveBeenCalledWith(
      expect.objectContaining({ id: record.id, status: 'approved' }),
      'approve',
    );
    expect(result.resume).toEqual({
      attempted: true,
      ok: true,
      summary: { status: 'success' },
    });
  });

  it('keeps the decision durable when the resume fails', async () => {
    // #given
    const harness = makeHarness({
      resumeRun: vi.fn().mockRejectedValue(new Error('DO unreachable')),
    });
    const record = await seedPending(harness);

    // #when
    const result = await harness.service.decide(
      record.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — decision persisted; failure reported, not thrown
    expect(result.resume).toEqual({
      attempted: true,
      ok: false,
      error: 'DO unreachable',
    });
    expect(await harness.store.get(record.id)).toMatchObject({
      status: 'approved',
    });
    expect(harness.events).toContainEqual(
      expect.objectContaining({ action: 'approval.resume', decision: 'error' }),
    );
  });

  it('resumes rejected decisions too — the workflow learns the outcome', async () => {
    // #given
    const resumeRun = vi.fn().mockResolvedValue({ status: 'success' });
    const harness = makeHarness({ resumeRun });
    const record = await seedPending(harness);

    // #when
    const result = await harness.service.decide(
      record.id,
      { decision: 'reject', comment: 'not yet' },
      REVIEWER,
    );

    // #then
    expect(result.record.status).toBe('rejected');
    expect(resumeRun).toHaveBeenCalledWith(
      expect.objectContaining({ decision: 'reject' }),
      'reject',
    );
  });

  it('conflicts on a second decision', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);
    await harness.service.decide(record.id, { decision: 'approve' }, REVIEWER);

    // #when / #then
    await expect(
      harness.service.decide(record.id, { decision: 'reject' }, ADMIN),
    ).rejects.toBeInstanceOf(ApprovalConflictError);
  });

  it('rejects an invalid decision value', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when / #then
    await expect(
      harness.service.decide(
        record.id,
        { decision: 'maybe' as 'approve' },
        REVIEWER,
      ),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
  });

  it('denies deciding to non-reviewer roles', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when / #then
    await expect(
      harness.service.decide(record.id, { decision: 'approve' }, VIEWER),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
  });
});

describe('ApprovalService.delegate', () => {
  it('reassigns the claim to the delegate', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);
    await harness.service.claim(record.id, REVIEWER);

    // #when
    const delegated = await harness.service.delegate(
      record.id,
      { to: 'quinn' },
      REVIEWER,
    );

    // #then
    expect(delegated).toMatchObject({
      status: 'claimed',
      claimedBy: 'quinn',
      delegatedTo: 'quinn',
    });
  });

  it('rejects an empty delegate target', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when / #then
    await expect(
      harness.service.delegate(record.id, { to: '' }, REVIEWER),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
  });
});

describe('ApprovalService.sweepSLA', () => {
  it('escalates only breached open requests and fires onEscalation', async () => {
    // #given — one breached, one within SLA
    const onEscalation = vi.fn();
    const harness = makeHarness({ onEscalation });
    const breached = await seedPending(harness, {
      runId: 'run-breach',
      slaSeconds: 60,
    });
    await seedPending(harness, { runId: 'run-fresh', slaSeconds: 3600 });
    harness.advance(120_000);

    // #when
    const escalated = await harness.service.sweepSLA(OPERATOR);

    // #then
    expect(escalated.map((record) => record.id)).toEqual([breached.id]);
    expect(escalated[0]).toMatchObject({
      status: 'escalated',
      escalatedAt: '2026-07-06T12:02:00.000Z',
    });
    expect(onEscalation).toHaveBeenCalledTimes(1);
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.escalate',
        decision: 'allowed',
      }),
    );
  });

  it('is idempotent — a second sweep escalates nothing new', async () => {
    // #given
    const onEscalation = vi.fn();
    const harness = makeHarness({ onEscalation });
    await seedPending(harness, { slaSeconds: 60 });
    harness.advance(120_000);
    await harness.service.sweepSLA(OPERATOR);

    // #when
    const second = await harness.service.sweepSLA(OPERATOR);

    // #then
    expect(second).toEqual([]);
    expect(onEscalation).toHaveBeenCalledTimes(1);
  });

  it('leaves escalated requests decidable', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness, { slaSeconds: 60 });
    harness.advance(120_000);
    await harness.service.sweepSLA(OPERATOR);

    // #when
    const result = await harness.service.decide(
      record.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then
    expect(result.record.status).toBe('approved');
  });

  it('survives a crashing onEscalation hook and audits the failure', async () => {
    // #given — two breached requests, hook crashes on the first
    const onEscalation = vi
      .fn()
      .mockImplementationOnce(() => {
        throw new Error('pager down');
      })
      .mockImplementation(() => undefined);
    const harness = makeHarness({ onEscalation });
    await seedPending(harness, { runId: 'r1', slaSeconds: 60 });
    await seedPending(harness, { runId: 'r2', slaSeconds: 60 });
    harness.advance(120_000);

    // #when
    const escalated = await harness.service.sweepSLA(OPERATOR);

    // #then — both escalated despite the crash; error audited
    expect(escalated).toHaveLength(2);
    expect(onEscalation).toHaveBeenCalledTimes(2);
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.escalate',
        decision: 'error',
        reason: 'onEscalation threw: pager down',
      }),
    );
  });

  it('escalates a request exactly at its SLA deadline (inclusive boundary)', async () => {
    // #given — deadline is createdAt + 60s
    const harness = makeHarness();
    const record = await seedPending(harness, {
      runId: 'run-boundary',
      slaSeconds: 60,
    });
    harness.advance(60_000); // now === deadline, to the millisecond

    // #when
    const escalated = await harness.service.sweepSLA(OPERATOR);

    // #then — the sweep uses <= (skip only when deadline > now), so AT the
    // deadline the request escalates; one ms earlier it would not
    expect(escalated.map((record_) => record_.id)).toEqual([record.id]);
  });

  it('does not escalate one millisecond before the deadline', async () => {
    // #given
    const harness = makeHarness();
    await seedPending(harness, { runId: 'run-early', slaSeconds: 60 });
    harness.advance(59_999);

    // #when / #then
    expect(await harness.service.sweepSLA(OPERATOR)).toEqual([]);
  });

  it('never escalates a request that has no SLA deadline', async () => {
    // #given — no slaSeconds and no service default: the request has no deadline
    const harness = makeHarness();
    await seedPending(harness, { runId: 'run-no-sla' });
    harness.advance(10_000_000);

    // #when / #then — an undefined deadline is skipped, not treated as breached
    expect(await harness.service.sweepSLA(OPERATOR)).toEqual([]);
  });

  it('denies the sweep to reviewer/viewer roles', async () => {
    // #given
    const harness = makeHarness();

    // #when / #then
    await expect(harness.service.sweepSLA(REVIEWER)).rejects.toBeInstanceOf(
      ApprovalAuthzError,
    );
  });
});

describe('ApprovalService.metrics', () => {
  it('computes queue metrics with a fixed clock', async () => {
    // #given — 4 requests: decided-fast, decided-slow, breached-open, fresh-open
    const harness = makeHarness();
    const fast = await seedPending(harness, { runId: 'fast' });
    const slow = await seedPending(harness, { runId: 'slow' });
    await seedPending(harness, { runId: 'breached', slaSeconds: 60 });
    await seedPending(harness, { runId: 'fresh', slaSeconds: 7200 });
    harness.advance(60_000);
    await harness.service.decide(fast.id, { decision: 'approve' }, REVIEWER);
    harness.advance(120_000);
    await harness.service.decide(slow.id, { decision: 'reject' }, REVIEWER);
    await harness.service.sweepSLA(OPERATOR);

    // #when
    const metrics = await harness.service.metrics(VIEWER);

    // #then — resolutions: 60s and 180s -> mean 120s; the breached request
    // is escalated (still open + breached); fresh stays unbreached
    expect(metrics).toEqual({
      openCount: 2,
      slaBreachedCount: 1,
      escalationCount: 1,
      decidedCount: 2,
      approvedCount: 1,
      rejectedCount: 1,
      avgResolutionSeconds: 120,
    });
  });

  it('reports null average resolution with no decided requests', async () => {
    // #given
    const harness = makeHarness();

    // #when
    const metrics = await harness.service.metrics(VIEWER);

    // #then
    expect(metrics.avgResolutionSeconds).toBeNull();
    expect(metrics.openCount).toBe(0);
  });

  it('counts an escalated-then-decided request in escalationCount only', async () => {
    // #given — a breached request escalated by the sweep, then decided
    const harness = makeHarness();
    const record = await seedPending(harness, {
      runId: 'run-esc-dec',
      slaSeconds: 60,
    });
    harness.advance(120_000);
    await harness.service.sweepSLA(OPERATOR);
    await harness.service.decide(record.id, { decision: 'approve' }, REVIEWER);

    // #when
    const metrics = await harness.service.metrics(VIEWER);

    // #then — escalationCount counts 'ever escalated' (escalatedAt persists
    // through the decision), but the decided request is neither pending nor
    // an open SLA breach anymore
    expect(metrics).toEqual({
      openCount: 0,
      slaBreachedCount: 0,
      escalationCount: 1,
      decidedCount: 1,
      approvedCount: 1,
      rejectedCount: 0,
      avgResolutionSeconds: 120,
    });
  });
});

describe('ApprovalService self-approval control', () => {
  it('denies the requester deciding their own request by default', async () => {
    // #given — created by opal; requestedBy defaults to the creating actor
    const harness = makeHarness();
    const record = await seedPending(harness);
    expect(record.requestedBy).toBe('opal');

    // #when — opal comes back wearing a decision-capable role
    let caught: unknown;
    try {
      await harness.service.decide(
        record.id,
        { decision: 'approve' },
        { id: 'opal', role: 'admin' },
      );
    } catch (error) {
      caught = error;
    }

    // #then — separation of duties: role alone is not enough
    expect(caught).toBeInstanceOf(ApprovalAuthzError);
    expect(await harness.store.get(record.id)).toMatchObject({
      status: 'pending',
    });
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.decide',
        decision: 'denied',
        reason: 'self-approval: decider is the requester',
      }),
    );
  });

  it('permits self-decision when allowSelfDecision is enabled', async () => {
    // #given
    const harness = makeHarness({ allowSelfDecision: true });
    const record = await seedPending(harness);

    // #when
    const result = await harness.service.decide(
      record.id,
      { decision: 'approve' },
      { id: 'opal', role: 'admin' },
    );

    // #then
    expect(result.record.status).toBe('approved');
  });

  it('gates on an explicitly-attributed requester too', async () => {
    // #given — a system bridge attributes the request to 'ray'
    const harness = makeHarness();
    const { record } = await harness.service.create(
      input({ requestedBy: 'ray' }),
      OPERATOR,
    );

    // #when / #then — ray cannot decide it; a different reviewer role can
    await expect(
      harness.service.decide(record.id, { decision: 'approve' }, REVIEWER),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
    const other = await harness.service.decide(
      record.id,
      { decision: 'approve' },
      ADMIN,
    );
    expect(other.record.status).toBe('approved');
  });
});

describe('ApprovalService payload contract', () => {
  it('rejects payloads JSON.stringify cannot represent', async () => {
    // #given
    const harness = makeHarness();

    // #when / #then — BigInt throws in JSON.stringify; refusing at the
    // service boundary keeps both stores inside the JSON-safe contract
    await expect(
      harness.service.create(input({ payload: { big: 1n } }), OPERATOR),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
  });
});

describe('ApprovalService.delegate concurrency', () => {
  it('is last-writer-wins by design: racing delegations both succeed', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when — two concurrent reassignments
    const [first, second] = await Promise.all([
      harness.service.delegate(record.id, { to: 'quinn' }, REVIEWER),
      harness.service.delegate(record.id, { to: 'pat' }, ADMIN),
    ]);

    // #then — no 409: delegation moves a pointer and guards no side effect;
    // the stored assignee is whichever write landed last
    expect(first.status).toBe('claimed');
    expect(second.status).toBe('claimed');
    const stored = await harness.store.get(record.id);
    expect(['quinn', 'pat']).toContain(stored?.claimedBy);
  });
});

describe('ApprovalService audit isolation', () => {
  it('does not fail actions when the audit sink throws', async () => {
    // #given
    const harness = makeHarness({
      audit: () => {
        throw new Error('sink down');
      },
    });

    // #when
    const { record } = await harness.service.create(input(), OPERATOR);

    // #then — the action succeeded despite the sink
    expect(record.status).toBe('pending');
  });
});
