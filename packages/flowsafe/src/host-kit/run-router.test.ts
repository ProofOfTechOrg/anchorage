// Unit coverage for the run surface every host mounts: the authorization ORDER
// (401 -> coarse RUN_START_ROLES -> per-workflow allowedRoles), the catalog, the
// start/status/resume routes and their error mapping, the suspension bridge's
// attribution (the starting actor becomes requestedBy, so they cannot decide
// their own run), and the D4 reconcileApprovals self-healing hook on status
// reads.
//
// Driven with real InMemoryApprovalStore + ApprovalService (no mocks) and
// fixture WorkflowMetas — depending on the showcase's modules here would invert
// the layering (showcase imports host-kit, not the reverse).

import { describe, expect, it, vi } from 'vitest';

import {
  type ApprovalActor,
  ApprovalService,
  createTenantResolver,
  InMemoryApprovalStoreFactory,
} from '../approval-api/index.js';
import {
  InvalidRunRequestError,
  RunNotSuspendedError,
  type RunSummary,
  UnknownRunError,
} from '../do-runner/index.js';
import { RunRouteError } from './run-route-error.js';
import { createRunRouter, type RunRouterOptions } from './run-router.js';
import type { WorkflowMeta } from './workflow-meta.js';

const SYSTEM: ApprovalActor = { id: 'sys', role: 'operator', tenantId: 'acme' };

/** Header-transported test identities; tenant defaults to 'acme' in req(). */
interface ReqActor {
  id: string;
  role: string;
  tenantId?: string;
}

const ADMIN: ReqActor = { id: 'ada', role: 'admin' };
const BUILDER: ReqActor = { id: 'bo', role: 'builder' };
const OPERATOR: ReqActor = { id: 'opal', role: 'operator' };
const REVIEWER: ReqActor = { id: 'ray', role: 'reviewer' };
const VIEWER: ReqActor = { id: 'vic', role: 'viewer' };

// The two shapes that matter: open-to-any-starter, and role-restricted.
const OPEN_FLOW: WorkflowMeta = {
  id: 'open-flow',
  title: 'Open flow',
  description: 'startable by any start-capable role',
  sampleInput: { topic: 'x' },
};

const RESTRICTED_FLOW: WorkflowMeta = {
  id: 'restricted-flow',
  title: 'Restricted flow',
  description: 'admin/builder only, like the showcase access-request',
  sampleInput: { subject: 'y' },
  allowedRoles: ['admin', 'builder'],
};

const WORKFLOWS = [OPEN_FLOW, RESTRICTED_FLOW];

function suspendedSummary(runId: string): RunSummary {
  return {
    runId,
    status: 'suspended',
    suspended: [['gate']],
    suspendPayload: {
      gate: { reason: 'approve me', connectors: ['deployer'] },
    },
    suspendedAt: { gate: 1717 },
  };
}

interface HarnessOptions {
  start?: (
    workflowId: string,
    runId: string,
    inputData: unknown,
  ) => Promise<RunSummary>;
  status?: (
    workflowId: string,
    runId: string,
  ) => Promise<RunSummary | undefined>;
  resume?: (
    workflowId: string,
    runId: string,
    body: unknown,
  ) => Promise<RunSummary>;
  reconcileApprovals?: RunRouterOptions['reconcileApprovals'];
}

function makeHarness(options: HarnessOptions = {}) {
  const backend = new InMemoryApprovalStoreFactory();
  const store = backend.forTenant('acme');
  // The bridge queues records through the request's TENANT-BOUND service;
  // exposing acme's service keeps the SoD assertions terse.
  const service = new ApprovalService({ store });
  const started: Array<{
    workflowId: string;
    runId: string;
    inputData: unknown;
  }> = [];
  const resumed: Array<{ workflowId: string; runId: string; body: unknown }> =
    [];
  const resolve = createTenantResolver({
    authenticate: (request) => {
      const id = request.headers.get('x-actor-id');
      const role = request.headers.get('x-actor-role');
      const tenantId = request.headers.get('x-actor-tenant') ?? 'acme';
      return id && role
        ? { id, role: role as ApprovalActor['role'], tenantId }
        : undefined;
    },
    storeFactory: backend,
    buildService: (boundStore) => new ApprovalService({ store: boundStore }),
    newRunId: () => 'generated-run-id',
  });
  const handle = createRunRouter({
    workflows: WORKFLOWS,
    resolve,
    systemActorId: SYSTEM.id,
    start:
      options.start ??
      (async (workflowId, runId, inputData) => {
        started.push({ workflowId, runId, inputData });
        return { runId, status: 'success', result: { ok: true } };
      }),
    status:
      options.status ??
      (async (_workflowId, runId) => ({ runId, status: 'running' })),
    resume:
      options.resume ??
      (async (workflowId, runId, body) => {
        resumed.push({ workflowId, runId, body });
        return { runId, status: 'success' };
      }),
    reconcileApprovals: options.reconcileApprovals,
  });
  return { store, service, handle, started, resumed };
}

interface ReqOptions {
  method?: string;
  body?: unknown;
  actor?: ReqActor | null;
}

function req(path: string, options: ReqOptions = {}): Request {
  const headers = new Headers();
  const actor = options.actor === undefined ? OPERATOR : options.actor;
  if (actor) {
    headers.set('x-actor-id', actor.id);
    headers.set('x-actor-role', actor.role);
    if (actor.tenantId !== undefined) {
      headers.set('x-actor-tenant', actor.tenantId);
    }
  }
  let body: string | undefined;
  if (options.body !== undefined) {
    headers.set('content-type', 'application/json');
    body =
      typeof options.body === 'string'
        ? options.body
        : JSON.stringify(options.body);
  }
  return new Request(`http://host.test${path}`, {
    method: options.method ?? (options.body === undefined ? 'GET' : 'POST'),
    headers,
    body,
  });
}

describe('createRunRouter — composition and auth', () => {
  it('returns null for paths it does not own', async () => {
    // #given
    const { handle } = makeHarness();

    // #when / #then — the host composes the approval router ahead of this one
    expect(await handle(req('/api/approvals'))).toBeNull();
    expect(await handle(req('/healthz'))).toBeNull();
    expect(await handle(req('/runsandmore'))).toBeNull();
  });

  it('rejects unauthenticated requests with 401 on every owned route', async () => {
    // #given
    const { handle } = makeHarness();

    // #when / #then
    expect((await handle(req('/workflows', { actor: null })))?.status).toBe(
      401,
    );
    expect(
      (
        await handle(
          req('/runs', { body: { workflowId: 'open-flow' }, actor: null }),
        )
      )?.status,
    ).toBe(401);
    expect(
      (await handle(req('/runs/open-flow/r1', { actor: null })))?.status,
    ).toBe(401);
  });

  it('sets no-store on responses (an authenticated API under the SPA asset origin)', async () => {
    // #given
    const { handle } = makeHarness();

    // #when
    const response = await handle(req('/workflows', { actor: VIEWER }));

    // #then
    expect(response?.headers.get('cache-control')).toBe('no-store');
  });
});

describe('createRunRouter — GET /workflows', () => {
  it('serves the catalog to any authenticated actor, including read-only roles', async () => {
    // #given
    const { handle } = makeHarness();

    // #when
    const response = await handle(req('/workflows', { actor: VIEWER }));

    // #then — the response carries the SERVER-derived identity; the SPA must
    // never guess its own role from a local token table (fail-open)
    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual({
      workflows: [OPEN_FLOW, RESTRICTED_FLOW],
      actor: { id: 'vic', role: 'viewer', tenantId: 'acme' },
    });
  });

  it('echoes exactly the authenticated actor, per identity', async () => {
    // #given
    const { handle } = makeHarness();

    // #when
    const asAdmin = await handle(req('/workflows', { actor: ADMIN }));
    const asReviewer = await handle(req('/workflows', { actor: REVIEWER }));

    // #then
    expect(await asAdmin?.json()).toMatchObject({
      actor: { id: 'ada', role: 'admin' },
    });
    expect(await asReviewer?.json()).toMatchObject({
      actor: { id: 'ray', role: 'reviewer' },
    });
  });
});

describe('createRunRouter — the coarse start-role gate', () => {
  it.each([
    ['viewer', VIEWER],
    ['reviewer', REVIEWER],
  ])('403s a %s POSTing a run start', async (_label, actor) => {
    // #given — reviewer/viewer may inspect runs, never advance them
    const { handle, started } = makeHarness();

    // #when
    const response = await handle(
      req('/runs', { body: { workflowId: 'open-flow' }, actor }),
    );

    // #then — denied before the workflow is even resolved
    expect(response?.status).toBe(403);
    expect(started).toEqual([]);
  });

  it('403s a review-only role POSTing a resume, too', async () => {
    // #given — the gate is method-keyed, so a new POST route cannot forget it
    const { handle, resumed } = makeHarness();

    // #when
    const response = await handle(
      req('/runs/open-flow/r1/resume', {
        body: { resumeData: { approved: true } },
        actor: REVIEWER,
      }),
    );

    // #then
    expect(response?.status).toBe(403);
    expect(resumed).toEqual([]);
  });

  it('lets a review-only role GET a run status', async () => {
    // #given
    const { handle } = makeHarness();

    // #when / #then
    expect(
      (await handle(req('/runs/open-flow/acme_r1', { actor: VIEWER })))?.status,
    ).toBe(200);
  });
});

describe('createRunRouter — per-workflow allowedRoles', () => {
  it('403s an operator starting a workflow whose meta restricts it to admin/builder', async () => {
    // #given — the operator passes the coarse gate and is stopped by the module
    const { handle, started } = makeHarness();

    // #when
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'restricted-flow' },
        actor: OPERATOR,
      }),
    );

    // #then
    expect(response?.status).toBe(403);
    expect(await response?.json()).toMatchObject({
      error: "role 'operator' may not start 'restricted-flow'",
    });
    expect(started).toEqual([]);
  });

  it.each([
    ['admin', ADMIN],
    ['builder', BUILDER],
  ])('lets a %s start it', async (_label, actor) => {
    // #given
    const { handle, started } = makeHarness();

    // #when
    const response = await handle(
      req('/runs', { body: { workflowId: 'restricted-flow' }, actor }),
    );

    // #then
    expect(response?.status).toBe(200);
    expect(started).toEqual([
      {
        workflowId: 'restricted-flow',
        runId: 'acme_generated-run-id',
        inputData: undefined,
      },
    ]);
  });

  it('403s an operator RESUMING a restricted workflow — allowedRoles binds every mutating route, not just start', async () => {
    // #given — an operator who was 403'd from starting restricted-flow must
    // not be able to drive an admin-started run to completion via resume
    const { handle, resumed } = makeHarness();

    // #when
    const response = await handle(
      req('/runs/restricted-flow/acme_r1/resume', {
        body: { resumeData: { approved: false } },
        actor: OPERATOR,
      }),
    );

    // #then — denied before the host thunk is consulted
    expect(response?.status).toBe(403);
    expect(await response?.json()).toMatchObject({
      error: "role 'operator' may not advance 'restricted-flow'",
    });
    expect(resumed).toEqual([]);
  });

  it('lets an allowed role resume the restricted workflow', async () => {
    // #given
    const { handle, resumed } = makeHarness();

    // #when
    const response = await handle(
      req('/runs/restricted-flow/acme_r1/resume', {
        body: { resumeData: { approved: true } },
        actor: BUILDER,
      }),
    );

    // #then
    expect(response?.status).toBe(200);
    expect(resumed).toHaveLength(1);
  });

  it('keeps restricted-workflow STATUS reads open to read-only roles — reads stay coarse', async () => {
    // #given — reviewer/viewer inspect runs they may not drive
    const { handle } = makeHarness();

    // #when / #then
    expect(
      (await handle(req('/runs/restricted-flow/acme_r1', { actor: VIEWER })))
        ?.status,
    ).toBe(200);
  });

  it("still 404s another tenant's run BEFORE the role check — no oracle for narrowed workflows", async () => {
    // #given — a beta operator (not in restricted-flow's allowedRoles)
    // probing an acme runId must see the same 404 an allowed role would
    const { handle } = makeHarness();

    // #when
    const response = await handle(
      req('/runs/restricted-flow/acme_r1/resume', {
        body: {},
        actor: { id: 'eve', role: 'operator', tenantId: 'beta' },
      }),
    );

    // #then — 404, not the 403 the role check would emit
    expect(response?.status).toBe(404);
  });
});

describe('createRunRouter — POST /runs', () => {
  it('400s a malformed body and 400s a body without workflowId', async () => {
    // #given
    const { handle } = makeHarness();

    // #when / #then
    expect(
      (await handle(req('/runs', { method: 'POST', body: '{not json' })))
        ?.status,
    ).toBe(400);
    expect(
      (await handle(req('/runs', { body: { inputData: {} } })))?.status,
    ).toBe(400);
  });

  it('404s an unknown workflow', async () => {
    // #given
    const { handle } = makeHarness();

    // #when / #then
    expect(
      (await handle(req('/runs', { body: { workflowId: 'ghost' } })))?.status,
    ).toBe(404);
  });

  it('400s a client-pinned runId and mints the tenant-salted id itself (INV-1)', async () => {
    // #given — the runId IS the tenant carrier; a client may never choose it
    const { handle, started } = makeHarness();

    // #when — a pinned runId is rejected, not silently overridden
    const pinned = await handle(
      req('/runs', { body: { workflowId: 'open-flow', runId: 'mine' } }),
    );

    // #then
    expect(pinned?.status).toBe(400);
    expect(await pinned?.json()).toEqual({ error: 'runId is server-assigned' });
    expect(started).toEqual([]);

    // #when — a normal start mints `${tenantId}_${uuid}`
    await handle(req('/runs', { body: { workflowId: 'open-flow' } }));

    // #then
    expect(started.map((entry) => entry.runId)).toEqual([
      'acme_generated-run-id',
    ]);
  });

  it("404s another tenant's run on status AND resume — 404, not 403, so the route is no existence oracle", async () => {
    // #given — tenant beta probing an acme-owned runId; the host thunks must
    // never even be consulted
    let statusCalls = 0;
    let resumeCalls = 0;
    const { handle } = makeHarness({
      status: async (_workflowId, runId) => {
        statusCalls += 1;
        return { runId, status: 'running' };
      },
      resume: async (_workflowId, runId) => {
        resumeCalls += 1;
        return { runId, status: 'success' };
      },
    });
    const beta = { id: 'eve', role: 'admin', tenantId: 'beta' };

    // #when / #then
    const status = await handle(
      req('/runs/open-flow/acme_r1', { actor: beta }),
    );
    expect(status?.status).toBe(404);
    expect(await status?.json()).toEqual({ error: 'run not found' });
    const resume = await handle(
      req('/runs/open-flow/acme_r1/resume', { body: {}, actor: beta }),
    );
    expect(resume?.status).toBe(404);
    expect(statusCalls).toBe(0);
    expect(resumeCalls).toBe(0);
  });

  it("403s an actor whose tenant claim violates INV-3 — 'undefined' must never concatenate", async () => {
    // #given — the tenant crosses an authentication boundary; the router
    // re-validates before minting or prefix-matching with it
    const { handle, started } = makeHarness();

    // #when / #then
    for (const tenantId of ['ACME', 'a_b', 'ab', '']) {
      const response = await handle(
        req('/runs', {
          body: { workflowId: 'open-flow' },
          actor: { id: 'eve', role: 'admin', tenantId },
        }),
      );
      expect(response?.status).toBe(403);
    }
    expect(started).toEqual([]);
  });

  it("403s an actor claiming the reserved identity 'system' — a custom verifier must not bind the maintenance identity", async () => {
    // #given — 'system' is INV-3-valid, so only the resolver's
    // reserved-identity check can catch a custom TokenVerifier (or hand-built
    // actor map) that never crossed toApprovalActor
    const { handle, started } = makeHarness();

    // #when
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow' },
        actor: { id: 'eve', role: 'admin', tenantId: 'system' },
      }),
    );

    // #then — no store bound, no system_* runId minted
    expect(response?.status).toBe(403);
    expect(started).toEqual([]);
  });

  it("still binds allocation-reserved tenants ('default') post-auth — only identities are refused at the resolver", async () => {
    // #given — the two-list split must hold at this second enforcement layer
    // too: refusing 'default' or 'api' here would break the single-tenant
    // host the allocation reservation exists to protect
    const { handle, started } = makeHarness();

    // #when
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow' },
        actor: { id: 'ray', role: 'admin', tenantId: 'default' },
      }),
    );

    // #then — run minted under the 'default' tenant
    expect(response?.ok).toBe(true);
    expect(started.map((entry) => entry.runId)).toEqual([
      'default_generated-run-id',
    ]);
  });

  it('returns the bare summary when the run does not suspend', async () => {
    // #given
    const { handle, store } = makeHarness();

    // #when
    const response = await handle(
      req('/runs', { body: { workflowId: 'open-flow' } }),
    );

    // #then — nothing queued
    expect(await response?.json()).toEqual({
      runId: 'acme_generated-run-id',
      status: 'success',
      result: { ok: true },
    });
    expect(await store.list()).toEqual([]);
  });

  it('queues an approval attributed to the STARTING actor on a suspension', async () => {
    // #given — the bridge's SoD contract: whoever advanced the run is the
    // requester, so they cannot also decide it
    const { handle, store } = makeHarness({
      start: async (_workflowId, runId) => suspendedSummary(runId),
    });

    // #when
    const response = await handle(
      req('/runs', { body: { workflowId: 'open-flow' }, actor: OPERATOR }),
    );

    // #then
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({
      status: 'suspended',
      approval: {
        workflowId: 'open-flow',
        stepPath: ['gate'],
        suspendedAt: 1717,
        connectors: ['deployer'],
        requestedBy: OPERATOR.id,
      },
    });
    expect(await store.list({ status: 'pending' })).toHaveLength(1);
  });

  it('the starting actor cannot decide the approval their own start queued', async () => {
    // #given — an admin (holding both CAN_CREATE and CAN_REVIEW) starts a run
    // that suspends
    const { handle, service, store } = makeHarness({
      start: async (_workflowId, runId) => suspendedSummary(runId),
    });
    await handle(
      req('/runs', { body: { workflowId: 'open-flow' }, actor: ADMIN }),
    );
    const [queued] = await store.list({ status: 'pending' });

    // #when / #then — separation of duties fires on the forced attribution
    await expect(
      service.decide(
        queued?.id ?? '',
        { decision: 'approve' },
        { id: ADMIN.id, role: 'admin', tenantId: 'acme' },
      ),
    ).rejects.toThrow(/cannot decide their own approval/);
  });
});

describe('createRunRouter — GET status and POST resume', () => {
  it('404s a status read for a workflow this host never registered', async () => {
    // #given — the start route checked the catalog; the passthrough routes
    // must too, or they answer for ids the host does not host
    const { handle } = makeHarness();

    // #when
    const response = await handle(req('/runs/ghost/r1'));

    // #then
    expect(response?.status).toBe(404);
    expect(await response?.json()).toMatchObject({
      error: "unknown workflow 'ghost'",
    });
  });

  it('404s an unknown run', async () => {
    // #given
    const { handle } = makeHarness({ status: async () => undefined });

    // #when / #then
    expect((await handle(req('/runs/open-flow/acme_r1')))?.status).toBe(404);
  });

  it('passes the resume body straight through, carrying no grants', async () => {
    // #given
    const { handle, resumed } = makeHarness();

    // #when
    const response = await handle(
      req('/runs/open-flow/acme_r1/resume', {
        body: { step: ['gate'], resumeData: { approved: true } },
      }),
    );

    // #then — the route is a transport; capability comes only from the
    // server-derived grant the runtime mints per leg
    expect(response?.status).toBe(200);
    expect(resumed).toEqual([
      {
        workflowId: 'open-flow',
        runId: 'acme_r1',
        body: { step: ['gate'], resumeData: { approved: true } },
      },
    ]);
  });

  it('treats a malformed resume body as an empty one', async () => {
    // #given — matches the DO's own readJson behavior
    const { handle, resumed } = makeHarness();

    // #when
    await handle(
      req('/runs/open-flow/acme_r1/resume', {
        method: 'POST',
        body: '{not json',
      }),
    );

    // #then
    expect(resumed[0]?.body).toEqual({});
  });

  it('404s an unknown subroute and a wrong method', async () => {
    // #given
    const { handle } = makeHarness();

    // #when / #then
    expect(
      (await handle(req('/runs/open-flow/acme_r1/bogus', { method: 'POST' })))
        ?.status,
    ).toBe(404);
    expect((await handle(req('/runs/open-flow/acme_r1/resume')))?.status).toBe(
      404,
    );
  });
});

describe('createRunRouter — reconcileApprovals hook (D4 self-healing)', () => {
  it('invokes reconcileApprovals when a status read reports the run suspended', async () => {
    // #given
    const calls: Array<{ workflowId: string; runId: string }> = [];
    const { handle } = makeHarness({
      status: async (_workflowId, runId) => suspendedSummary(runId),
      reconcileApprovals: async (_tenant, workflowId, summary) => {
        calls.push({ workflowId, runId: summary.runId });
      },
    });

    // #when
    const response = await handle(req('/runs/open-flow/acme_r1'));

    // #then — the status projection itself is unaffected...
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({ status: 'suspended' });
    // ...and the hook ran, told which run to check
    expect(calls).toEqual([{ workflowId: 'open-flow', runId: 'acme_r1' }]);
  });

  it('does not invoke reconcileApprovals for a non-suspended status', async () => {
    // #given — the default status thunk reports 'running'
    const calls: unknown[] = [];
    const { handle } = makeHarness({
      reconcileApprovals: async () => {
        calls.push(1);
      },
    });

    // #when
    await handle(req('/runs/open-flow/acme_r1'));

    // #then
    expect(calls).toEqual([]);
  });

  it('leaves status() behavior unchanged when reconcileApprovals is absent (the default)', async () => {
    // #given — no reconcileApprovals option at all, matching every other
    // status test in this file
    const { handle } = makeHarness({
      status: async (_workflowId, runId) => suspendedSummary(runId),
    });

    // #when
    const response = await handle(req('/runs/open-flow/acme_r1'));

    // #then
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({ status: 'suspended' });
  });

  it('a throwing reconcileApprovals is logged, not surfaced — a broken reconcile must not break status reads', async () => {
    // #given
    const { handle } = makeHarness({
      status: async (_workflowId, runId) => suspendedSummary(runId),
      reconcileApprovals: async () => {
        throw new Error('reconcile boom');
      },
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when
    const response = await handle(req('/runs/open-flow/acme_r1'));

    // #then — the read still succeeds...
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({ status: 'suspended' });
    // ...and the failure was logged for an operator, not swallowed silently
    expect(
      errorSpy.mock.calls.some(([line]) => {
        const text = String(line);
        return (
          text.includes('reconcile-error') && text.includes('reconcile boom')
        );
      }),
    ).toBe(true);
    errorSpy.mockRestore();
  });
});

describe('createRunRouter — error mapping', () => {
  it('surfaces a host RunRouteError with its own status', async () => {
    // #given — a DO stub answered non-ok; the DO already chose the status
    const { handle } = makeHarness({
      start: async () => {
        throw new RunRouteError(409, 'run already exists');
      },
    });

    // #when
    const response = await handle(
      req('/runs', { body: { workflowId: 'open-flow' } }),
    );

    // #then
    expect(response?.status).toBe(409);
    expect(await response?.json()).toEqual({ error: 'run already exists' });
  });

  it.each([
    ['UnknownRunError -> 404', new UnknownRunError('open-flow', 'r1'), 404],
    [
      'RunNotSuspendedError -> 409',
      new RunNotSuspendedError('open-flow', 'r1', 'success'),
      409,
    ],
    [
      'InvalidRunRequestError -> 400',
      new InvalidRunRequestError('bad step'),
      400,
    ],
  ])('maps %s', async (_label, error, status) => {
    // #given — an in-process host throws the do-runner's typed errors
    const { handle } = makeHarness({
      resume: async () => {
        throw error;
      },
    });

    // #when / #then
    expect(
      (await handle(req('/runs/open-flow/acme_r1/resume', { body: {} })))
        ?.status,
    ).toBe(status);
  });

  it('maps an unexpected failure to 500', async () => {
    // #given
    const { handle } = makeHarness({
      status: async () => {
        throw new Error('d1 exploded');
      },
    });

    // #when
    const response = await handle(req('/runs/open-flow/acme_r1'));

    // #then
    expect(response?.status).toBe(500);
    expect(await response?.json()).toEqual({ error: 'd1 exploded' });
  });
});
