import { describe, expect, it, vi } from 'vitest';

import type { ApprovalActor, ApprovalAuditEvent } from './contract.js';
import {
  ApprovalAuthzError,
  ApprovalConflictError,
  ApprovalService,
  type ApprovalServiceOptions,
  InvalidApprovalInputError,
  sweepSLA,
  UnknownApprovalError,
} from './service.js';
import type { InMemoryApprovalStore } from './store.js';
import { InMemoryApprovalStoreFactory } from './tenant-store.js';
import type { ApprovalRecord, CreateApprovalInput } from './types.js';

const ADMIN: ApprovalActor = { id: 'ada', role: 'admin', tenantId: 'acme' };
const OPERATOR: ApprovalActor = {
  id: 'opal',
  role: 'operator',
  tenantId: 'acme',
};
const REVIEWER: ApprovalActor = {
  id: 'ray',
  role: 'reviewer',
  tenantId: 'acme',
};
const VIEWER: ApprovalActor = { id: 'vic', role: 'viewer', tenantId: 'acme' };

const T0 = Date.parse('2026-07-06T12:00:00.000Z');

interface Harness {
  service: ApprovalService;
  store: InMemoryApprovalStore;
  backend: InMemoryApprovalStoreFactory;
  events: ApprovalAuditEvent[];
  now: () => Date;
  advance: (ms: number) => void;
}

function makeHarness(options: Partial<ApprovalServiceOptions> = {}): Harness {
  const backend = new InMemoryApprovalStoreFactory();
  const store = backend.forTenant('acme') as InMemoryApprovalStore;
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
    backend,
    events,
    now: () => new Date(nowMs),
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
    runId: 'acme_run-1',
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

/**
 * The cron-owned standalone sweep over the harness's SYSTEM store, sharing
 * the harness clock and audit sink — the shape every host's scheduled()
 * runs. There is deliberately no service.sweepSLA anymore.
 */
function runSweep(
  harness: Harness,
  options: { onEscalation?: (record: ApprovalRecord) => void } = {},
): Promise<ApprovalRecord[]> {
  return sweepSLA(harness.backend.system(), {
    systemActor: OPERATOR,
    audit: (event) => harness.events.push(event),
    onEscalation: options.onEscalation,
    now: harness.now,
  });
}

describe('ApprovalService.create', () => {
  it("rejects a runId that does not carry the store's tenant prefix (INV-1 belt)", async () => {
    // #given — every read path filters on the tenant_id column, so a foreign
    // prefix would only orphan a row; the belt makes it a loud error instead
    const harness = makeHarness();

    // #when / #then
    await expect(
      harness.service.create(input({ runId: 'bravo_run-1' }), OPERATOR),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
    await expect(
      harness.service.create(input({ runId: 'bare-run' }), OPERATOR),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
  });

  it('denies EVERY action to an actor whose tenant differs from the store binding', async () => {
    // #given — a valid role, wrong tenant: the service-layer INV-2 belt
    const harness = makeHarness();
    const intruder: ApprovalActor = {
      id: 'eve',
      role: 'admin',
      tenantId: 'bravo',
    };

    // #when / #then
    await expect(
      harness.service.create(input(), intruder),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
    await expect(harness.service.list({}, intruder)).rejects.toBeInstanceOf(
      ApprovalAuthzError,
    );
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        decision: 'denied',
        reason: expect.stringContaining('tenant'),
      }),
    );
  });

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
      runId: 'acme_run-1',
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
      // Attribution is what the separation-of-duties check compares. An empty
      // string is not an identity: it matches no actor, yet reads as
      // "attributed" to anything downstream inspecting the record.
      { requestedBy: '' },
      { requestedBy: 42 as unknown as string },
      // runScoped is a capability switch (mints on every leg of the run), so
      // only a real boolean opts in — never a truthy string from a lax caller.
      { runScoped: 'true' as unknown as boolean },
      { runScoped: 1 as unknown as boolean },
    ];

    // #when / #then
    for (const overrides of bad) {
      await expect(
        harness.service.create(input(overrides), OPERATOR),
      ).rejects.toBeInstanceOf(InvalidApprovalInputError);
    }
  });

  it('accepts an explicit requestedBy and copies runScoped through', async () => {
    // #given — the in-process bridge legitimately attributes the human who
    // advanced the run; only the HTTP boundary forbids it (router.ts)
    const harness = makeHarness();

    // #when
    const { record } = await harness.service.create(
      input({ requestedBy: 'starter', runScoped: true }),
      OPERATOR,
    );

    // #then
    expect(record.requestedBy).toBe('starter');
    expect(record.runScoped).toBe(true);
  });

  it('defaults runScoped to absent — a record is never run-scoped by omission', async () => {
    // #given / #when
    const harness = makeHarness();
    const { record } = await harness.service.create(input(), OPERATOR);

    // #then — grants.ts mints a step-less record only on runScoped === true
    expect(record.runScoped).toBeUndefined();
  });

  it('preserves an explicit runScoped:false rather than dropping it', async () => {
    // #given / #when
    const harness = makeHarness();
    const { record } = await harness.service.create(
      input({ runScoped: false }),
      OPERATOR,
    );

    // #then
    expect(record.runScoped).toBe(false);
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
  it('ApprovalServiceOptions carries NO onEscalation — escalation hooks belong to sweepSLA alone', () => {
    // #given — the option existed, was never read (its only reader was the
    // retired service.sweepSLA), and both hosts passed a hook that could
    // never fire. The pin: re-adding it makes this @ts-expect-error unused,
    // which is itself a compile error — so it cannot come back silently.
    const harness = makeHarness();
    const options: ApprovalServiceOptions = {
      store: harness.store,
      // @ts-expect-error — onEscalation is not an ApprovalService option;
      // wire SweepSLAOptions.onEscalation (the cron sweep) instead.
      onEscalation: () => {},
    };

    // #then — the literal above is the assertion; keep the value used
    expect(options.store).toBe(harness.store);
  });

  it('escalates only breached open requests and fires onEscalation', async () => {
    // #given — one breached, one within SLA
    const onEscalation = vi.fn();
    const harness = makeHarness();
    const breached = await seedPending(harness, {
      runId: 'acme_run-breach',
      slaSeconds: 60,
    });
    await seedPending(harness, { runId: 'acme_run-fresh', slaSeconds: 3600 });
    harness.advance(120_000);

    // #when
    const escalated = await runSweep(harness, { onEscalation });

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
    const harness = makeHarness();
    await seedPending(harness, { slaSeconds: 60 });
    harness.advance(120_000);
    await runSweep(harness, { onEscalation });

    // #when
    const second = await runSweep(harness, { onEscalation });

    // #then
    expect(second).toEqual([]);
    expect(onEscalation).toHaveBeenCalledTimes(1);
  });

  it('leaves escalated requests decidable', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness, { slaSeconds: 60 });
    harness.advance(120_000);
    await runSweep(harness);

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
    const harness = makeHarness();
    await seedPending(harness, { runId: 'acme_r1', slaSeconds: 60 });
    await seedPending(harness, { runId: 'acme_r2', slaSeconds: 60 });
    harness.advance(120_000);

    // #when
    const escalated = await runSweep(harness, { onEscalation });

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
      runId: 'acme_run-boundary',
      slaSeconds: 60,
    });
    harness.advance(60_000); // now === deadline, to the millisecond

    // #when
    const escalated = await runSweep(harness);

    // #then — the sweep uses <= (skip only when deadline > now), so AT the
    // deadline the request escalates; one ms earlier it would not
    expect(escalated.map((record_) => record_.id)).toEqual([record.id]);
  });

  it('does not escalate one millisecond before the deadline', async () => {
    // #given
    const harness = makeHarness();
    await seedPending(harness, { runId: 'acme_run-early', slaSeconds: 60 });
    harness.advance(59_999);

    // #when / #then
    expect(await runSweep(harness)).toEqual([]);
  });

  it('never escalates a request that has no SLA deadline', async () => {
    // #given — no slaSeconds and no service default: the request has no deadline
    const harness = makeHarness();
    await seedPending(harness, { runId: 'acme_run-no-sla' });
    harness.advance(10_000_000);

    // #when / #then — an undefined deadline is skipped, not treated as breached
    expect(await runSweep(harness)).toEqual([]);
  });

  it('sweeps ACROSS tenants — the one legitimate cross-tenant write, cron-only', async () => {
    // #given — breached records under two tenants sharing the backend
    const harness = makeHarness();
    await seedPending(harness, { slaSeconds: 60 });
    const bravoService = new ApprovalService({
      store: harness.backend.forTenant('bravo'),
      now: harness.now,
    });
    await bravoService.create(
      {
        workflowId: 'wf',
        runId: 'bravo_run-1',
        title: 'bravo approval',
        slaSeconds: 60,
      },
      { id: 'opal', role: 'operator', tenantId: 'bravo' },
    );
    harness.advance(120_000);

    // #when
    const escalated = await runSweep(harness);

    // #then — both tenants' breaches escalate, each audit event attributed
    // to its record's tenant
    expect(escalated.map((record) => record.tenantId).sort()).toEqual([
      'acme',
      'bravo',
    ]);
    for (const record of escalated) {
      expect(harness.events).toContainEqual(
        expect.objectContaining({
          action: 'approval.escalate',
          decision: 'allowed',
          detail: expect.objectContaining({ tenantId: record.tenantId }),
        }),
      );
    }
  });
});

describe('ApprovalService.metrics', () => {
  it('computes queue metrics with a fixed clock', async () => {
    // #given — 4 requests: decided-fast, decided-slow, breached-open, fresh-open
    const harness = makeHarness();
    const fast = await seedPending(harness, { runId: 'acme_fast' });
    const slow = await seedPending(harness, { runId: 'acme_slow' });
    await seedPending(harness, { runId: 'acme_breached', slaSeconds: 60 });
    await seedPending(harness, { runId: 'acme_fresh', slaSeconds: 7200 });
    harness.advance(60_000);
    await harness.service.decide(fast.id, { decision: 'approve' }, REVIEWER);
    harness.advance(120_000);
    await harness.service.decide(slow.id, { decision: 'reject' }, REVIEWER);
    await runSweep(harness);

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
      runId: 'acme_run-esc-dec',
      slaSeconds: 60,
    });
    harness.advance(120_000);
    await runSweep(harness);
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
        { id: 'opal', role: 'admin', tenantId: 'acme' },
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
      { id: 'opal', role: 'admin', tenantId: 'acme' },
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
