// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import type {
  ApprovalActor,
  ApprovalAuditEvent,
  ApprovalNotificationEvent,
  ApprovalNotificationSink,
  ApprovalStreamEvent,
  ApprovalStreamSink,
} from './contract.js';
import type { AutomatedExecutionPrincipal } from './principal.js';
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
import type { SystemApprovalStore } from './tenant-brand.js';
import { InMemoryApprovalStoreFactory } from './tenant-store.js';
import {
  type ApprovalRecord,
  type CreateApprovalInput,
  MAX_APPROVAL_BATCH_DECIDE,
  MAX_APPROVAL_LIST_LIMIT,
} from './types.js';

const ADMIN: ApprovalActor = { id: 'ada', role: 'admin', tenantId: 'acme' };
const SWEEP_PRINCIPAL: AutomatedExecutionPrincipal = {
  kind: 'system',
  id: 'sweeper',
  tenantId: 'system',
  purpose: 'approval-sla-maintenance',
};

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
  options: {
    onEscalation?: (record: ApprovalRecord) => void;
    notify?: ApprovalNotificationSink;
    stream?: ApprovalStreamSink;
  } = {},
): Promise<ApprovalRecord[]> {
  return sweepSLA(harness.backend.system(), {
    systemPrincipal: SWEEP_PRINCIPAL,
    audit: (event) => harness.events.push(event),
    onEscalation: options.onEscalation,
    notify: options.notify,
    stream: options.stream,
    now: harness.now,
  });
}

describe('ApprovalService.create', () => {
  it('accepts a tenant-owned trusted resume target only through the server seam', async () => {
    const harness = makeHarness();
    const target = {
      kind: 'thread' as const,
      threadId: 'acme_thread-1',
      resourceId: 'acme_resource-1',
    };

    const { record } = await harness.service.create(input(), OPERATOR, target);

    expect(record.resumeTarget).toEqual(target);
    expect(record.resumeTarget).not.toBe(target);
  });

  it('rejects a trusted resume target containing a foreign memory id', async () => {
    const harness = makeHarness();
    await expect(
      harness.service.create(input(), OPERATOR, {
        kind: 'thread',
        threadId: 'globex_thread-1',
      }),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
  });

  it('accepts an agent-thread target only with a same-tenant valid principal', async () => {
    const harness = makeHarness();
    const target = {
      kind: 'agent-thread' as const,
      agentId: 'writer',
      threadId: 'acme_thread-1',
      resourceId: 'acme_resource-1',
      principal: {
        kind: 'human' as const,
        id: 'starter',
        tenantId: 'acme',
        role: 'operator' as const,
      },
    };

    const { record } = await harness.service.create(input(), OPERATOR, target);

    expect(record.resumeTarget).toEqual(target);
    await expect(
      harness.service.create(input({ runId: 'acme_run-2' }), OPERATOR, {
        ...target,
        principal: { ...target.principal, tenantId: 'globex' },
      }),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
    await expect(
      harness.service.create(input({ runId: 'acme_run-3' }), OPERATOR, {
        ...target,
        agentId: '../writer',
      }),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
    await expect(
      harness.service.create(input({ runId: 'acme_run-4' }), OPERATOR, {
        ...target,
        principal: { ...target.principal, id: '   ' },
      }),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
  });

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

  it('escalates ALL breached requests across more than one page (>MAX), paging the system view (D3)', async () => {
    // #given — more breached requests than a single page holds, so the sweep
    // must cursor-page the (un-defaulted) system view instead of one SELECT;
    // pre-D3 this bare list() was unbounded, post-D3 a naive single list would
    // silently cap at MAX and leave the tail un-escalated
    const harness = makeHarness();
    const total = MAX_APPROVAL_LIST_LIMIT + 1;
    for (let index = 0; index < total; index += 1) {
      await seedPending(harness, {
        runId: `acme_run-sweep-${index}`,
        slaSeconds: 60,
      });
    }
    harness.advance(120_000);

    // #when
    const escalated = await runSweep(harness);

    // #then — every breached request escalated, none dropped past the first page
    expect(escalated).toHaveLength(total);
    expect(
      await harness.backend.system().list({ status: ['escalated'] }),
    ).toHaveLength(total);
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

  it('keeps canonical audit provenance on every event when the source principal mutates during store I/O', async () => {
    // #given — the caller retains a mutable alias and rewrites it while the
    // sweep is suspended in its first store read.
    const harness = makeHarness();
    await seedPending(harness, { slaSeconds: 60 });
    harness.advance(61_000);
    harness.events.length = 0;
    const source: Record<string, unknown> = { ...SWEEP_PRINCIPAL };
    const backing = harness.backend.system();
    const store: SystemApprovalStore = {
      list: async (filter) => {
        const records = await backing.list(filter);
        source.kind = 'human';
        source.role = 'admin';
        source.purpose = undefined;
        return records;
      },
      transition: (id, from, patch) => backing.transition(id, from, patch),
      purgeExpired: (cutoffIso, limit) =>
        backing.purgeExpired(cutoffIso, limit),
    };

    // #when
    const escalated = await sweepSLA(store, {
      systemPrincipal: source as unknown as AutomatedExecutionPrincipal,
      audit: (event) => harness.events.push(event),
      onEscalation: () => {
        throw new Error('pager down');
      },
      notify: () => {
        throw new Error('smtp down');
      },
      stream: () => {
        throw new Error('hub down');
      },
      now: harness.now,
    });

    // #then — attribution comes from the entry snapshot, not the mutated alias.
    expect(escalated).toHaveLength(1);
    expect(harness.events).toHaveLength(4);
    expect(
      harness.events.map((event) => [event.action, event.decision]),
    ).toEqual([
      ['approval.escalate', 'allowed'],
      ['approval.escalate', 'error'],
      ['approval.notify', 'error'],
      ['approval.stream', 'error'],
    ]);
    for (const event of harness.events) {
      expect(event).toMatchObject({
        actor: { id: 'sweeper', role: 'viewer', tenantId: 'system' },
        detail: {
          principalKind: 'system',
          principalId: 'sweeper',
          purpose: 'approval-sla-maintenance',
        },
      });
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

  it('permits self-decision when allowSelfDecision is enabled, and audits it', async () => {
    // #given
    const harness = makeHarness({ allowSelfDecision: true });
    const record = await seedPending(harness);

    // #when
    const result = await harness.service.decide(
      record.id,
      { decision: 'approve' },
      { id: 'opal', role: 'admin', tenantId: 'acme' },
    );

    // #then — permitted AND the exercised exemption leaves a trail
    expect(result.record.status).toBe('approved');
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.decide',
        decision: 'allowed',
        detail: expect.objectContaining({ selfDecision: true }),
      }),
    );
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

  it('role-scoped: an exempt role decides its own request; others still cannot', async () => {
    // #given — admin is exempt, reviewer is not (the demo's single-operator config)
    const harness = makeHarness({ allowSelfDecision: { roles: ['admin'] } });
    const adminReq = await harness.service.create(
      input({ requestedBy: ADMIN.id }),
      OPERATOR,
    );
    // Distinct runId: create() is idempotent per open (run, step), so reusing
    // acme_run-1 would return the admin record, not a second one.
    const reviewerReq = await harness.service.create(
      input({ runId: 'acme_run-2', requestedBy: REVIEWER.id }),
      OPERATOR,
    );

    // #when / #then — admin self-decides (audited); reviewer self-request 403s
    const decided = await harness.service.decide(
      adminReq.record.id,
      { decision: 'approve' },
      ADMIN,
    );
    expect(decided.record.status).toBe('approved');
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.decide',
        decision: 'allowed',
        detail: expect.objectContaining({ selfDecision: true }),
      }),
    );
    await expect(
      harness.service.decide(
        reviewerReq.record.id,
        { decision: 'approve' },
        REVIEWER,
      ),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
  });

  it('role-scoped: an exempt role deciding ANOTHER actor request is not flagged self', async () => {
    // #given — admin exempt, but this request was raised by ray
    const harness = makeHarness({ allowSelfDecision: { roles: ['admin'] } });
    const { record } = await harness.service.create(
      input({ requestedBy: REVIEWER.id }),
      OPERATOR,
    );

    // #when — admin decides someone else's request
    const decided = await harness.service.decide(
      record.id,
      { decision: 'approve' },
      ADMIN,
    );

    // #then — allowed, but NOT annotated as a self-decision
    expect(decided.record.status).toBe('approved');
    const decideEvent = harness.events.find(
      (event) =>
        event.action === 'approval.decide' && event.decision === 'allowed',
    );
    expect(decideEvent?.detail).not.toHaveProperty('selfDecision');
  });
});

describe('ApprovalService cross-gate separation of duties', () => {
  // The reconcile hole this closes: resumeRunWithRequeue attributes gate B to
  // the gate-A decider (so the requestedBy self-check already bars them), but
  // reconcileApprovalsForSummary files gate B as the SYSTEM actor — which the
  // requestedBy check never blocks. The bar below is derived from the run's
  // APPROVED history instead, so it catches BOTH filings.

  it('refuses the gate-A approver at gate B even when gate B is filed by the system actor (reconcile path)', async () => {
    // #given — ray approves gate A; the run advances and re-suspends at gate B,
    // which is reconcile-filed attributed to the system actor (NOT ray)
    const harness = makeHarness();
    const gateA = await seedPending(harness, {
      runId: 'acme_run-seq',
      stepPath: ['gateA'],
    });
    await harness.service.decide(gateA.id, { decision: 'approve' }, REVIEWER);
    // The run resumed and re-suspended; the reconcile files gate B later.
    harness.advance(1000);
    const gateB = await harness.service.create(
      input({
        runId: 'acme_run-seq',
        stepPath: ['gateB'],
        requestedBy: 'flowsafe-system',
      }),
      OPERATOR,
    );

    // #when / #then — ray (who advanced the run at gate A) cannot decide gate B,
    // even though requestedBy is the system actor, not ray
    let caught: unknown;
    try {
      await harness.service.decide(
        gateB.record.id,
        { decision: 'approve' },
        REVIEWER,
      );
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ApprovalAuthzError);
    expect(await harness.store.get(gateB.record.id)).toMatchObject({
      status: 'pending',
    });
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.decide',
        decision: 'denied',
        reason:
          'cross-gate separation of duties: this actor approved an earlier gate of the run',
      }),
    );
  });

  it('lets a DIFFERENT reviewer decide gate B', async () => {
    // #given — same run: ray approved gate A, gate B is reconcile-filed
    const harness = makeHarness();
    const gateA = await seedPending(harness, {
      runId: 'acme_run-seq',
      stepPath: ['gateA'],
    });
    await harness.service.decide(gateA.id, { decision: 'approve' }, REVIEWER);
    harness.advance(1000);
    const gateB = await harness.service.create(
      input({
        runId: 'acme_run-seq',
        stepPath: ['gateB'],
        requestedBy: 'flowsafe-system',
      }),
      OPERATOR,
    );

    // #when — a reviewer who did NOT advance the run (admin) decides gate B
    const decided = await harness.service.decide(
      gateB.record.id,
      { decision: 'approve' },
      ADMIN,
    );

    // #then — allowed: the bar is per-actor, not a blanket second-reviewer block
    expect(decided.record.status).toBe('approved');
  });

  it('does NOT block two independent parallel gates one reviewer clears', async () => {
    // #given — two gates filed TOGETHER before any decision (a .parallel() split)
    const harness = makeHarness();
    const gate1 = await seedPending(harness, {
      runId: 'acme_run-par',
      stepPath: ['branchA'],
    });
    const gate2 = await seedPending(harness, {
      runId: 'acme_run-par',
      stepPath: ['branchB'],
    });
    // Decisions happen AFTER both were filed, so a prior approval's decidedAt is
    // strictly LATER than the sibling's createdAt — the causal anchor never fires.
    harness.advance(1000);

    // #when — ray decides both
    const d1 = await harness.service.decide(
      gate1.id,
      { decision: 'approve' },
      REVIEWER,
    );
    const d2 = await harness.service.decide(
      gate2.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — both clear: independent parallel gates are not over-blocked
    expect(d1.record.status).toBe('approved');
    expect(d2.record.status).toBe('approved');
  });

  it('a prior same-actor REJECTION does not bar a later re-review gate (approved-only scope)', async () => {
    // #given — ray REJECTS gate A (reject -> revise); the re-review gate is filed
    const harness = makeHarness();
    const gateA = await seedPending(harness, {
      runId: 'acme_run-rej',
      stepPath: ['review'],
    });
    await harness.service.decide(gateA.id, { decision: 'reject' }, REVIEWER);
    harness.advance(1000);
    const reReview = await harness.service.create(
      input({
        runId: 'acme_run-rej',
        stepPath: ['review'],
        requestedBy: 'flowsafe-system',
      }),
      OPERATOR,
    );

    // #when — ray re-reviews the revised submission
    const decided = await harness.service.decide(
      reReview.record.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — allowed: a rejection is a denial, not an advancement, so the
    // approved-only history query excludes it and the normal cycle is unbroken
    expect(decided.record.status).toBe('approved');
  });

  it('still lets the demo admin ({ roles: [admin] }) clear both gates of a run', async () => {
    // #given — the single-operator config: admin is exempt from SoD entirely
    const harness = makeHarness({ allowSelfDecision: { roles: ['admin'] } });
    const gateA = await seedPending(harness, {
      runId: 'acme_run-adm',
      stepPath: ['gateA'],
    });
    await harness.service.decide(gateA.id, { decision: 'approve' }, ADMIN);
    harness.advance(1000);
    const gateB = await harness.service.create(
      input({
        runId: 'acme_run-adm',
        stepPath: ['gateB'],
        requestedBy: 'flowsafe-system',
      }),
      OPERATOR,
    );

    // #when — the SAME admin decides gate B
    const decided = await harness.service.decide(
      gateB.record.id,
      { decision: 'approve' },
      ADMIN,
    );

    // #then — the exemption skips BOTH the requestedBy self-check and this bar
    expect(decided.record.status).toBe('approved');
  });

  it('a run where the actor has no prior approval is unaffected (per-run scope)', async () => {
    // #given — ray approves gate A on run X
    const harness = makeHarness();
    const gateA = await seedPending(harness, {
      runId: 'acme_run-x',
      stepPath: ['gateA'],
    });
    await harness.service.decide(gateA.id, { decision: 'approve' }, REVIEWER);
    harness.advance(1000);
    // ...and a gate on a DIFFERENT run Y, where ray never approved anything
    const gateY = await seedPending(harness, {
      runId: 'acme_run-y',
      stepPath: ['gate'],
    });

    // #when — ray decides run Y's gate
    const decided = await harness.service.decide(
      gateY.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — allowed: the bar is scoped to the record's OWN run
    expect(decided.record.status).toBe('approved');
  });

  it('reads the COMPLETE approved history via after-cursor paging, barring a causally-prior approval past the default page', async () => {
    // #given — a run with MORE approved records than one default page holds. The
    // filler (page 1) was decided by admin; the ONE record decided by ray sits
    // on a LATER page (newest createdAt). A single default-bounded first page
    // would miss it and fail the bar OPEN — this pins the after-cursor paging.
    const harness = makeHarness();
    const runId = 'acme_run-page';
    const base = new Date(T0).toISOString();
    for (let index = 0; index < MAX_APPROVAL_LIST_LIMIT; index += 1) {
      await harness.store.create({
        id: `filler-${index}`,
        tenantId: 'acme',
        workflowId: 'wf',
        runId,
        stepPath: [`filler-${index}`],
        title: `filler ${index}`,
        connectors: [],
        priority: 'normal',
        status: 'approved',
        createdAt: base,
        updatedAt: base,
        decidedBy: ADMIN.id,
        decidedAt: base,
      });
    }
    // ray's causally-prior approval — newest createdAt, so it lands on page 2
    const rayApprovedAt = new Date(T0 + 1000).toISOString();
    await harness.store.create({
      id: 'ray-prior',
      tenantId: 'acme',
      workflowId: 'wf',
      runId,
      stepPath: ['gateRay'],
      title: 'ray approved an earlier gate',
      connectors: [],
      priority: 'normal',
      status: 'approved',
      createdAt: rayApprovedAt,
      updatedAt: rayApprovedAt,
      decidedBy: REVIEWER.id,
      decidedAt: rayApprovedAt,
    });

    // gate B, filed AFTER ray's approval (createdAt >= ray's decidedAt)
    harness.advance(2000);
    const gateB = await harness.service.create(
      input({ runId, stepPath: ['gateB'], requestedBy: 'flowsafe-system' }),
      OPERATOR,
    );

    // #when / #then — ray is barred: the paging read reached page 2 and found
    // ray's causally-prior approval past the 500-record default page
    await expect(
      harness.service.decide(
        gateB.record.id,
        { decision: 'approve' },
        REVIEWER,
      ),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
  });

  it('bars a same-actor approved prior whose decidedAt is unparseable (NaN causal anchor fails CLOSED)', async () => {
    // #given — a prior gate APPROVED by ray whose decidedAt is a non-ISO string,
    // so Date.parse(decidedAt) is NaN. A bare `priorAt <= gateAt` is FALSE for
    // NaN, so the un-hardened predicate drops the bar and lets ray clear gate B
    // (fail OPEN); the hardened predicate bars on NaN (fail CLOSED). Seeded
    // straight into the store so the garbage stamp bypasses create()'s
    // validation.
    const harness = makeHarness();
    const runId = 'acme_run-nan';
    const base = new Date(T0).toISOString();
    await harness.store.create({
      id: 'ray-garbage-prior',
      tenantId: 'acme',
      workflowId: 'wf',
      runId,
      stepPath: ['gateA'],
      title: 'ray approved an earlier gate (unparseable decidedAt)',
      connectors: [],
      priority: 'normal',
      status: 'approved',
      createdAt: base,
      updatedAt: base,
      decidedBy: REVIEWER.id,
      decidedAt: 'not-a-real-timestamp',
    });
    // gate B, filed after — a valid createdAt, so gateAt is a real number and
    // only the prior's NaN drives the fail-closed bar
    harness.advance(1000);
    const gateB = await harness.service.create(
      input({ runId, stepPath: ['gateB'], requestedBy: 'flowsafe-system' }),
      OPERATOR,
    );

    // #when / #then — ray is barred: the NaN anchor fails closed, not open
    await expect(
      harness.service.decide(
        gateB.record.id,
        { decision: 'approve' },
        REVIEWER,
      ),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
    // and gate B stays open — never silently approved
    expect(await harness.store.get(gateB.record.id)).toMatchObject({
      status: 'pending',
    });
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

describe('ApprovalService notification seam', () => {
  it('notifies once per actually-created record, with the record', async () => {
    // #given
    const notified: ApprovalNotificationEvent[] = [];
    const harness = makeHarness({
      notify: (event) => void notified.push(event),
    });

    // #when
    const record = await seedPending(harness);

    // #then
    expect(notified).toEqual([
      { type: 'created', record: expect.objectContaining({ id: record.id }) },
    ]);
  });

  it('does not re-notify the idempotent re-observation of an open step', async () => {
    // #given
    const notify = vi.fn();
    const harness = makeHarness({ notify });
    await seedPending(harness, { stepPath: ['gate'] });

    // #when — same (workflowId, runId, stepKey) while still open
    const second = await harness.service.create(
      input({ stepPath: ['gate'] }),
      OPERATOR,
    );

    // #then
    expect(second.created).toBe(false);
    expect(notify).toHaveBeenCalledTimes(1);
  });

  it('contains a throwing sink and audits approval.notify/error', async () => {
    // #given
    const harness = makeHarness({
      notify: () => {
        throw new Error('smtp down');
      },
    });

    // #when — the create must succeed regardless
    const record = await seedPending(harness);

    // #then
    expect(record.status).toBe('pending');
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.notify',
        decision: 'error',
        reason: expect.stringContaining('smtp down'),
      }),
    );
  });

  it('contains an async-rejecting sink likewise', async () => {
    // #given
    const harness = makeHarness({
      notify: () => Promise.reject(new Error('webhook 500')),
    });

    // #when
    await seedPending(harness);
    // The rejection handler runs off the microtask queue, after create returned.
    await new Promise((resolve) => setTimeout(resolve, 0));

    // #then
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.notify',
        decision: 'error',
        reason: expect.stringContaining('webhook 500'),
      }),
    );
  });

  it('notifies per escalated record from the sweep', async () => {
    // #given
    const harness = makeHarness();
    await seedPending(harness, { slaSeconds: 60 });
    await seedPending(harness, { slaSeconds: 60, runId: 'acme_run-2' });
    harness.advance(61_000);

    // #when
    const notified: ApprovalNotificationEvent[] = [];
    const escalated = await runSweep(harness, {
      notify: (event) => void notified.push(event),
    });

    // #then
    expect(escalated).toHaveLength(2);
    expect(notified.map((event) => event.type)).toEqual([
      'escalated',
      'escalated',
    ]);
    expect(new Set(notified.map((event) => event.record.id))).toEqual(
      new Set(escalated.map((record) => record.id)),
    );
  });

  it('a throwing sweep sink does not abort the sweep and audits each failure', async () => {
    // #given
    const harness = makeHarness();
    await seedPending(harness, { slaSeconds: 60 });
    await seedPending(harness, { slaSeconds: 60, runId: 'acme_run-2' });
    harness.advance(61_000);

    // #when
    const escalated = await runSweep(harness, {
      notify: () => {
        throw new Error('pager down');
      },
    });

    // #then — both records still escalated, evidence preserved
    expect(escalated).toHaveLength(2);
    expect(
      harness.events.filter(
        (event) =>
          event.action === 'approval.notify' && event.decision === 'error',
      ),
    ).toHaveLength(2);
  });
});

describe('ApprovalService stream seam', () => {
  it('emits one created event per actually-created record, carrying the record', async () => {
    // #given
    const streamed: ApprovalStreamEvent[] = [];
    const harness = makeHarness({
      stream: (event) => void streamed.push(event),
    });

    // #when
    const record = await seedPending(harness);

    // #then
    expect(streamed).toEqual([
      {
        type: 'created',
        record: expect.objectContaining({ id: record.id, status: 'pending' }),
      },
    ]);
  });

  it('does not emit on the idempotent created:false re-observation of an open step', async () => {
    // #given
    const stream = vi.fn();
    const harness = makeHarness({ stream });
    await seedPending(harness, { stepPath: ['gate'] });

    // #when — same (workflowId, runId, stepKey) while still open
    const second = await harness.service.create(
      input({ stepPath: ['gate'] }),
      OPERATOR,
    );

    // #then — the re-observed open record fires no second event
    expect(second.created).toBe(false);
    expect(stream).toHaveBeenCalledTimes(1);
  });

  it('emits one claimed event with the post-transition record', async () => {
    // #given
    const streamed: ApprovalStreamEvent[] = [];
    const harness = makeHarness({
      stream: (event) => void streamed.push(event),
    });
    const record = await seedPending(harness);
    streamed.length = 0; // drop the create event; isolate the claim

    // #when
    await harness.service.claim(record.id, REVIEWER);

    // #then
    expect(streamed).toEqual([
      {
        type: 'claimed',
        record: expect.objectContaining({ id: record.id, status: 'claimed' }),
      },
    ]);
  });

  it('emits one decided event with the post-transition record', async () => {
    // #given
    const streamed: ApprovalStreamEvent[] = [];
    const harness = makeHarness({
      stream: (event) => void streamed.push(event),
    });
    const record = await seedPending(harness);
    streamed.length = 0;

    // #when
    await harness.service.decide(record.id, { decision: 'approve' }, REVIEWER);

    // #then — fired after the transition, independent of #resume (unwired here)
    expect(streamed).toEqual([
      {
        type: 'decided',
        record: expect.objectContaining({ id: record.id, status: 'approved' }),
      },
    ]);
  });

  it('emits one delegated event with the post-transition record', async () => {
    // #given
    const streamed: ApprovalStreamEvent[] = [];
    const harness = makeHarness({
      stream: (event) => void streamed.push(event),
    });
    const record = await seedPending(harness);
    streamed.length = 0;

    // #when
    await harness.service.delegate(record.id, { to: 'quinn' }, REVIEWER);

    // #then
    expect(streamed).toEqual([
      {
        type: 'delegated',
        record: expect.objectContaining({ id: record.id, status: 'claimed' }),
      },
    ]);
  });

  it('emits one superseded event with the post-transition record', async () => {
    // #given
    const streamed: ApprovalStreamEvent[] = [];
    const harness = makeHarness({
      stream: (event) => void streamed.push(event),
    });
    const record = await seedPending(harness);
    streamed.length = 0;

    // #when — supersedeStale is CAN_CREATE (system reconcile), not a decision
    const updated = await harness.service.supersedeStale(
      record.id,
      OPERATOR,
      'stale suspension',
    );

    // #then
    expect(updated?.status).toBe('rejected');
    expect(streamed).toEqual([
      {
        type: 'superseded',
        record: expect.objectContaining({ id: record.id, status: 'rejected' }),
      },
    ]);
  });

  it('contains a sync-throwing stream sink and audits approval.stream/error', async () => {
    // #given — the dual-guard's try/catch arm
    const harness = makeHarness({
      stream: () => {
        throw new Error('hub crashed');
      },
    });

    // #when — the create must succeed regardless
    const record = await seedPending(harness);

    // #then
    expect(record.status).toBe('pending');
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.stream',
        decision: 'error',
        reason: expect.stringContaining('hub crashed'),
      }),
    );
  });

  it('contains an async-rejecting stream sink likewise', async () => {
    // #given — the dual-guard's returned-promise .catch arm
    const harness = makeHarness({
      stream: () => Promise.reject(new Error('hub 500')),
    });

    // #when
    await seedPending(harness);
    // The rejection handler runs off the microtask queue, after create returned.
    await new Promise((resolve) => setTimeout(resolve, 0));

    // #then
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.stream',
        decision: 'error',
        reason: expect.stringContaining('hub 500'),
      }),
    );
  });

  it('emits one escalated event per record from the sweep', async () => {
    // #given
    const harness = makeHarness();
    await seedPending(harness, { slaSeconds: 60 });
    await seedPending(harness, { slaSeconds: 60, runId: 'acme_run-2' });
    harness.advance(61_000);

    // #when
    const streamed: ApprovalStreamEvent[] = [];
    const escalated = await runSweep(harness, {
      stream: (event) => void streamed.push(event),
    });

    // #then
    expect(escalated).toHaveLength(2);
    expect(streamed.map((event) => event.type)).toEqual([
      'escalated',
      'escalated',
    ]);
    expect(new Set(streamed.map((event) => event.record.id))).toEqual(
      new Set(escalated.map((record) => record.id)),
    );
  });
});

describe('ApprovalService audit sink promise containment', () => {
  it('contains a promise-returning audit sink rejection on every recorder (create/decide/sweep)', async () => {
    // #given — ApprovalAuditSink may return a promise (a composed breakwater
    // sink with an async member); a rejection must never surface as an
    // unhandled rejection after the action already returned
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    const proc = (
      globalThis as unknown as {
        process: {
          on(event: 'unhandledRejection', fn: (r: unknown) => void): void;
          off(event: 'unhandledRejection', fn: (r: unknown) => void): void;
        };
      }
    ).process;
    proc.on('unhandledRejection', onUnhandled);
    try {
      const harness = makeHarness({
        audit: () => Promise.reject(new Error('siem down')),
      });

      // #when — the three recorders: create, decide, and the sweep's own
      const record = await seedPending(harness, { slaSeconds: 60 });
      await harness.service.decide(
        record.id,
        { decision: 'approve' },
        REVIEWER,
      );
      await seedPending(harness, { slaSeconds: 60, runId: 'acme_run-2' });
      harness.advance(61_000);
      await sweepSLA(harness.backend.system(), {
        systemPrincipal: SWEEP_PRINCIPAL,
        audit: () => Promise.reject(new Error('siem down')),
        now: harness.now,
      });
      // Rejection handlers run off the microtask queue; give them a tick.
      await new Promise((resolve) => setTimeout(resolve, 0));

      // #then
      expect(unhandled).toEqual([]);
    } finally {
      proc.off('unhandledRejection', onUnhandled);
    }
  });

  it.each([
    [
      'a human principal',
      { kind: 'human', id: 'ada', tenantId: 'system', role: 'admin' },
    ],
    [
      'a principal with no purpose',
      { kind: 'system', id: 'sweeper', tenantId: 'system' },
    ],
    ['a non-object', 'sweeper'],
  ])('refuses to sweep on behalf of %s', async (_label, principal) => {
    // #given — the sweep writes across EVERY tenant, so a bad attribution
    // identity makes every escalation it emits unattributable. The type
    // excludes a human; this is the erased-type half.
    const harness = makeHarness();
    await seedPending(harness, { slaSeconds: 60 });
    harness.advance(61_000);

    // #when / #then — refused before any store write, so nothing escalates.
    await expect(
      sweepSLA(harness.backend.system(), {
        systemPrincipal: principal as unknown as AutomatedExecutionPrincipal,
        audit: (event) => harness.events.push(event),
        now: harness.now,
      }),
    ).rejects.toThrow(/must be a valid automated execution principal/);
    expect(await harness.store.list({ status: ['escalated'] })).toEqual([]);
  });
});

describe('ApprovalService decide audit detail', () => {
  it('carries queue dwell time as durationSeconds (created -> decided)', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);
    harness.advance(90_000);

    // #when
    await harness.service.decide(record.id, { decision: 'approve' }, REVIEWER);

    // #then — feeds breakwater metricsAuditSink's histogram convention
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.decide',
        decision: 'allowed',
        detail: expect.objectContaining({ durationSeconds: 90 }),
      }),
    );
  });
});

describe('ApprovalService.decideBatch', () => {
  it('reports per-record outcomes: ok, SoD-forbidden, not-found, conflict', async () => {
    // #given
    const harness = makeHarness();
    const ok = await seedPending(harness);
    const sod = await seedPending(harness, {
      runId: 'acme_run-2',
      requestedBy: REVIEWER.id,
    });
    const decided = await seedPending(harness, { runId: 'acme_run-3' });
    await harness.service.decide(decided.id, { decision: 'reject' }, ADMIN);

    // #when
    const result = await harness.service.decideBatch(
      [ok.id, sod.id, 'missing', decided.id],
      { decision: 'approve', comment: 'triage sweep' },
      REVIEWER,
    );

    // #then — per-record fan-out, input order preserved
    expect(result.decided).toBe(1);
    expect(result.failed).toBe(3);
    expect(result.results.map((item) => [item.id, item.ok, item.code])).toEqual(
      [
        [ok.id, true, undefined],
        [sod.id, false, 'forbidden'],
        ['missing', false, 'not-found'],
        [decided.id, false, 'conflict'],
      ],
    );
    expect(result.results[0]?.record).toMatchObject({
      status: 'approved',
      comment: 'triage sweep',
    });
    expect(result.results[0]?.resume).toEqual({ attempted: false });
  });

  it('inherits the role-scoped self-decision exemption per record', async () => {
    // #given — admin exempt; a batch mixing admin's own request with ray's
    const harness = makeHarness({ allowSelfDecision: { roles: ['admin'] } });
    const ownByAdmin = await harness.service.create(
      input({ requestedBy: ADMIN.id }),
      OPERATOR,
    );
    const ownByReviewer = await harness.service.create(
      input({ runId: 'acme_run-2', requestedBy: REVIEWER.id }),
      OPERATOR,
    );

    // #when — admin runs the batch
    const result = await harness.service.decideBatch(
      [ownByAdmin.record.id, ownByReviewer.record.id],
      { decision: 'approve' },
      ADMIN,
    );

    // #then — admin's own request clears (exemption applied per record); ray's
    // is decidable by admin too (not a self-request for admin)
    expect(result.decided).toBe(2);
    expect(result.failed).toBe(0);
  });

  it('still 403s a NON-exempt actor own request inside a batch under a { roles } policy', async () => {
    // #given — admin is exempt, reviewer is NOT; the reviewer runs the batch
    const harness = makeHarness({ allowSelfDecision: { roles: ['admin'] } });
    const ownByReviewer = await harness.service.create(
      input({ requestedBy: REVIEWER.id }),
      OPERATOR,
    );
    const othersRecord = await harness.service.create(
      input({ runId: 'acme_run-2', requestedBy: ADMIN.id }),
      OPERATOR,
    );

    // #when — reviewer batch-decides its OWN request plus another's
    const result = await harness.service.decideBatch(
      [ownByReviewer.record.id, othersRecord.record.id],
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — the exemption is role-scoped: reviewer's own request is SoD-denied,
    // the other clears
    expect(result.decided).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.results.map((item) => [item.id, item.ok, item.code])).toEqual(
      [
        [ownByReviewer.record.id, false, 'forbidden'],
        [othersRecord.record.id, true, undefined],
      ],
    );
  });

  it('dedupes ids order-preservingly — a duplicate is not a spurious conflict', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when
    const result = await harness.service.decideBatch(
      [record.id, record.id, record.id],
      { decision: 'approve' },
      REVIEWER,
    );

    // #then
    expect(result.results).toHaveLength(1);
    expect(result.decided).toBe(1);
    expect(result.failed).toBe(0);
  });

  it('rejects a batch over MAX_APPROVAL_BATCH_DECIDE unique ids', async () => {
    // #given
    const harness = makeHarness();
    const ids = Array.from(
      { length: MAX_APPROVAL_BATCH_DECIDE + 1 },
      (_, index) => `id-${index}`,
    );

    // #when / #then
    await expect(
      harness.service.decideBatch(ids, { decision: 'approve' }, REVIEWER),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
  });

  it('rejects an empty batch and non-string or empty ids', async () => {
    // #given
    const harness = makeHarness();

    // #when / #then
    await expect(
      harness.service.decideBatch([], { decision: 'approve' }, REVIEWER),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
    await expect(
      harness.service.decideBatch(
        ['ok', ''],
        { decision: 'approve' },
        REVIEWER,
      ),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
  });

  it('rejects the whole batch for a non-reviewer role, touching nothing', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when / #then
    await expect(
      harness.service.decideBatch([record.id], { decision: 'approve' }, VIEWER),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
    expect(await harness.store.get(record.id)).toMatchObject({
      status: 'pending',
    });
  });

  it('validates the decision once at batch level', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when / #then
    await expect(
      harness.service.decideBatch(
        [record.id],
        { decision: 'maybe' as 'approve' },
        REVIEWER,
      ),
    ).rejects.toBeInstanceOf(InvalidApprovalInputError);
    expect(await harness.store.get(record.id)).toMatchObject({
      status: 'pending',
    });
  });

  it('decides exactly MAX_APPROVAL_BATCH_DECIDE records (the positive boundary)', async () => {
    // #given — the cap test above proves 101 rejects; this pins 100 working
    const harness = makeHarness();
    const ids: string[] = [];
    for (let index = 0; index < MAX_APPROVAL_BATCH_DECIDE; index++) {
      const record = await seedPending(harness, {
        runId: `acme_run-b${index}`,
      });
      ids.push(record.id);
    }

    // #when
    const result = await harness.service.decideBatch(
      ids,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then
    expect(result.decided).toBe(MAX_APPROVAL_BATCH_DECIDE);
    expect(result.failed).toBe(0);
  });

  it("reads another tenant's id as not-found with no content leak", async () => {
    // #given — a foreign-tenant record in the SAME shared backend (INV-2)
    const harness = makeHarness();
    const rivalStore = harness.backend.forTenant(
      'bravo',
    ) as InMemoryApprovalStore;
    const { record: rival } = await rivalStore.create({
      id: 'rival-1',
      tenantId: 'bravo',
      workflowId: 'wf',
      runId: 'bravo_run-1',
      title: 'RIVAL SECRET TITLE',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: new Date(T0).toISOString(),
      updatedAt: new Date(T0).toISOString(),
    });
    const own = await seedPending(harness);

    // #when
    const result = await harness.service.decideBatch(
      [own.id, rival.id],
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — foreign id behaves exactly like an unknown id, leaks nothing,
    // and the foreign record is untouched
    expect(result.decided).toBe(1);
    expect(result.results[1]).toMatchObject({ ok: false, code: 'not-found' });
    expect(JSON.stringify(result.results[1])).not.toContain(
      'RIVAL SECRET TITLE',
    );
    expect((await rivalStore.get(rival.id))?.status).toBe('pending');
  });

  it('emits one approval.decide.batch summary audit event with tallies', async () => {
    // #given
    const harness = makeHarness();
    const record = await seedPending(harness);

    // #when
    await harness.service.decideBatch(
      [record.id, 'missing'],
      { decision: 'approve' },
      REVIEWER,
    );

    // #then
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        action: 'approval.decide.batch',
        decision: 'allowed',
        detail: { requested: 2, decided: 1, failed: 1 },
      }),
    );
  });

  it('bars the SECOND of two sequential gates of one run in a single batch (inherits the cross-gate bar)', async () => {
    // #given — gate A and gate B of ONE run, both open. In one synchronous batch
    // the decider clears gate A first; gate B, filed at the same fixed-clock
    // instant, then satisfies the causal anchor (createdAt <= gate A's decidedAt)
    // and is barred — the fail-closed treatment of one reviewer clearing both
    // sequential gates of a run alone. (Production's real-clock reconcile files
    // gate B strictly after gate A's approval — the genuine sequential case.)
    const harness = makeHarness();
    const gateA = await seedPending(harness, {
      runId: 'acme_run-batch',
      stepPath: ['gateA'],
    });
    const gateB = await seedPending(harness, {
      runId: 'acme_run-batch',
      stepPath: ['gateB'],
    });

    // #when — one reviewer batch-decides both gates of the run
    const result = await harness.service.decideBatch(
      [gateA.id, gateB.id],
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — gate A clears; gate B is SoD-forbidden by the cross-gate bar
    expect(result.decided).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.results.map((item) => [item.id, item.ok, item.code])).toEqual(
      [
        [gateA.id, true, undefined],
        [gateB.id, false, 'forbidden'],
      ],
    );
    expect(await harness.store.get(gateB.id)).toMatchObject({
      status: 'pending',
    });
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
