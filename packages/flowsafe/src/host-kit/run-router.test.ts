// SPDX-License-Identifier: Apache-2.0
// Unit coverage for the run surface every host mounts: the authorization ORDER
// (401 -> coarse RUN_START_ROLES -> per-workflow allowedRoles), the catalog, the
// start/status/resume/terminate routes and their error mapping, the suspension bridge's
// attribution (the starting actor becomes requestedBy, so they cannot decide
// their own run), and the D4 reconcileApprovals self-healing hook on status
// reads.
//
// Driven with real InMemoryApprovalStore + ApprovalService (no mocks) and
// fixture WorkflowMetas — depending on the showcase's modules here would invert
// the layering (showcase imports host-kit, not the reverse).

import { describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  type ApprovalActor,
  ApprovalService,
  type ApprovalStore,
  createActorResolver,
  InMemoryApprovalStoreFactory,
  type SelfDecisionPolicy,
} from '../approval-api/index.js';
import {
  ExecutionFencedError,
  type ExecutionFenceWiring,
  InvalidRunRequestError,
  RunLifecycleBlockedError,
  RunNotSuspendedError,
  type RunSummary,
  RunTerminalConflictError,
  type StartIdempotencyDatabase,
  StartIdempotencyStore,
  UnknownRunError,
} from '../do-runner/index.js';
import { reconcileApprovalsOnStatus } from './approval-bridge.js';
import { RunRouteError } from './run-route-error.js';
import { createRunRouter, type RunRouterOptions } from './run-router.js';
import type { WorkflowMeta } from './workflow-meta.js';

const SYSTEM: ApprovalActor = { id: 'sys', role: 'operator' };

/** Header-transported test identities. */
interface ReqActor {
  id: string;
  role: string;
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
  start?: RunRouterOptions['start'];
  status?: (
    workflowId: string,
    runId: string,
  ) => Promise<RunSummary | undefined>;
  resume?: RunRouterOptions['resume'];
  terminate?: NonNullable<RunRouterOptions['terminate']>;
  reconcileApprovals?: RunRouterOptions['reconcileApprovals'];
  // F9: feeds the resolver's allowSelfDecision now (the run-router no longer
  // owns a selfDecision knob), driving the catalog's canSelfDecide echo.
  selfDecision?: SelfDecisionPolicy;
  resourceOwner?: ReqActor;
  approvalCreateFailures?: number;
  /**
   * How this harness honours `idempotencyKey`. Defaults to the typed opt-out,
   * which is what every pre-F3 case in this file wants: unkeyed starts are
   * unaffected by the wiring, and a keyed start on an unwired host must refuse.
   */
  startIdempotency?: RunRouterOptions['startIdempotency'];
}

function makeHarness(options: HarnessOptions = {}) {
  const backend = new InMemoryApprovalStoreFactory();
  const resourceOwner = options.resourceOwner ?? OPERATOR;
  for (const runId of ['r1', 'acme_r1']) {
    void backend.resources().claim('run', runId, {
      kind: 'human',
      id: resourceOwner.id,
    });
  }
  const store = backend.store();
  let approvalCreateFailures = options.approvalCreateFailures ?? 0;
  const requestStore = new Proxy(store, {
    get(target, property, receiver) {
      if (property === 'create') {
        return async (...args: Parameters<ApprovalStore['create']>) => {
          if (approvalCreateFailures > 0) {
            approvalCreateFailures -= 1;
            throw new Error('injected approval create failure');
          }
          return target.create(...args);
        };
      }
      const value = Reflect.get(target, property, receiver) as unknown;
      return typeof value === 'function' ? value.bind(target) : value;
    },
  }) as ApprovalStore;
  // The bridge queues records through the request-scoped deployment service.
  // In-memory store, no database to fence against: the opt-out is written down
  // rather than defaulted — see ExecutionFenceWiring.
  const service = new ApprovalService({ store, executionFence: 'none' });
  const started: Array<{
    workflowId: string;
    runId: string;
    inputData: unknown;
  }> = [];
  const resumed: Array<{ workflowId: string; runId: string; body: unknown }> =
    [];
  const resolve = createActorResolver({
    authenticate: (request) => {
      const id = request.headers.get('x-actor-id');
      const role = request.headers.get('x-actor-role');
      return id && role
        ? { id, role: role as ApprovalActor['role'] }
        : undefined;
    },
    storeFactory: backend,
    buildService: () =>
      new ApprovalService({ store: requestStore, executionFence: 'none' }),
    newRunId: () => 'generated-run-id',
    // F9: the SoD exemption policy now feeds the resolver, so the catalog echo
    // reads context.canSelfDecide (the run-router no longer takes its own knob).
    allowSelfDecision: options.selfDecision,
  });
  const handle = createRunRouter({
    workflows: WORKFLOWS,
    resolve,
    systemPrincipalId: SYSTEM.id,
    startIdempotency: options.startIdempotency ?? 'none',
    start: async (input) => {
      await backend.resources().claim('run', input.runId, {
        kind: input.principal.kind,
        id: input.principal.id,
      });
      if (options.start) return options.start(input);
      const { workflowId, runId, inputData } = input;
      started.push({ workflowId, runId, inputData });
      return { runId, status: 'success', result: { ok: true } };
    },
    status:
      options.status ??
      (async (_workflowId, runId) => ({ runId, status: 'running' })),
    resume:
      options.resume ??
      (async (workflowId, runId, body) => {
        resumed.push({ workflowId, runId, body });
        return { runId, status: 'success' };
      }),
    ...(options.terminate ? { terminate: options.terminate } : {}),
    reconcileApprovals: options.reconcileApprovals,
  });
  return {
    store,
    service,
    resources: backend.resources(),
    handle,
    started,
    resumed,
  };
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
    // never guess its own role from a local token table (fail-open). With no
    // exemption policy, canSelfDecide is false (SoD on).
    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual({
      workflows: [OPEN_FLOW, RESTRICTED_FLOW],
      actor: {
        id: 'vic',
        role: 'viewer',
        canSelfDecide: false,
      },
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

  it('echoes canSelfDecide from the deployment SoD exemption policy', async () => {
    // #given — admin is exempt (the single-operator config)
    const { handle } = makeHarness({ selfDecision: { roles: ['admin'] } });

    // #when
    const asAdmin = await handle(req('/workflows', { actor: ADMIN }));
    const asReviewer = await handle(req('/workflows', { actor: REVIEWER }));
    const asViewer = await handle(req('/workflows', { actor: VIEWER }));

    // #then — only the exempt role echoes true; the SPA suppresses its
    // "server will refuse" hint accordingly
    expect(await asAdmin?.json()).toMatchObject({
      actor: { role: 'admin', canSelfDecide: true },
    });
    expect(await asReviewer?.json()).toMatchObject({
      actor: { role: 'reviewer', canSelfDecide: false },
    });
    expect(await asViewer?.json()).toMatchObject({
      actor: { role: 'viewer', canSelfDecide: false },
    });
  });

  it('never echoes canSelfDecide:true for a non-decider role, even if the policy names it', async () => {
    // #given — a nonsensical policy exempting a role that cannot decide at all
    const asBuilder = (
      await makeHarness({ selfDecision: { roles: ['builder'] } }).handle(
        req('/workflows', { actor: BUILDER }),
      )
    )?.json();
    // #given — global `true` still cannot make a viewer a self-decider
    const asViewer = (
      await makeHarness({ selfDecision: true }).handle(
        req('/workflows', { actor: VIEWER }),
      )
    )?.json();

    // #then — the hint intersects DECIDER_ROLES, so it never affirms a role
    // the decide() gate would reject regardless
    expect(await asBuilder).toMatchObject({
      actor: { role: 'builder', canSelfDecide: false },
    });
    expect(await asViewer).toMatchObject({
      actor: { role: 'viewer', canSelfDecide: false },
    });
  });
});

describe('createRunRouter — the coarse start-role gate', () => {
  it('413s a run start body before JSON parsing', async () => {
    const { handle, started } = makeHarness();

    const response = await handle(
      req('/runs', { body: 'x'.repeat(1_048_577) }),
    );

    expect(response?.status).toBe(413);
    expect(started).toEqual([]);
  });

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

  it('404s a review-only role before the resume role gate', async () => {
    // #given — write ownership is resolved before role authorization
    const { handle, resumed } = makeHarness();

    // #when
    const response = await handle(
      req('/runs/open-flow/r1/resume', {
        body: { resumeData: { approved: true } },
        actor: REVIEWER,
      }),
    );

    // #then
    expect(response?.status).toBe(404);
    expect(resumed).toEqual([]);
  });

  it('lets a reviewer GET a run status', async () => {
    // #given
    const { handle } = makeHarness();

    // #when / #then
    expect(
      (await handle(req('/runs/open-flow/acme_r1', { actor: REVIEWER })))
        ?.status,
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
        runId: 'generated-run-id',
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
    const { handle, resumed } = makeHarness({ resourceOwner: BUILDER });

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

  it('keeps restricted-workflow status reads open to reviewers', async () => {
    // #given — reviewers inspect runs they may not drive
    const { handle } = makeHarness();

    // #when / #then
    expect(
      (await handle(req('/runs/restricted-flow/acme_r1', { actor: REVIEWER })))
        ?.status,
    ).toBe(200);
  });

  it('404s another operator before narrowed workflow roles', async () => {
    // #given — the id exists but belongs to a different operator
    const { handle } = makeHarness();

    // #when
    const response = await handle(
      req('/runs/restricted-flow/acme_r1/resume', {
        body: {},
        actor: { id: 'eve', role: 'operator' },
      }),
    );

    // #then
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

  it('400s a client-pinned runId and mints an opaque id itself (INV-1)', async () => {
    // #given — a client may never choose the addressing id
    const { handle, started } = makeHarness();

    // #when — a pinned runId is rejected, not silently overridden
    const pinned = await handle(
      req('/runs', { body: { workflowId: 'open-flow', runId: 'mine' } }),
    );

    // #then
    expect(pinned?.status).toBe(400);
    expect(await pinned?.json()).toEqual({ error: 'runId is server-assigned' });
    expect(started).toEqual([]);

    // #when — a normal start uses the context's opaque server minter
    await handle(req('/runs', { body: { workflowId: 'open-flow' } }));

    // #then
    expect(started.map((entry) => entry.runId)).toEqual(['generated-run-id']);
  });

  it('lets another authorized deployment actor inspect and resume a run', async () => {
    let statusCalls = 0;
    let resumeCalls = 0;
    let resumeRequester: { id: string; kind: string } | undefined;
    const { handle } = makeHarness({
      status: async (_workflowId, runId) => {
        statusCalls += 1;
        return { runId, status: 'running' };
      },
      resume: async (
        _workflowId,
        runId,
        _body,
        requestedBy,
        requestedByKind,
      ) => {
        resumeCalls += 1;
        resumeRequester = { id: requestedBy, kind: requestedByKind };
        return { runId, status: 'success' };
      },
    });
    const beta = { id: 'eve', role: 'admin' };

    // #when / #then
    const status = await handle(
      req('/runs/open-flow/acme_r1', { actor: beta }),
    );
    expect(status?.status).toBe(200);
    const resume = await handle(
      req('/runs/open-flow/acme_r1/resume', { body: {}, actor: beta }),
    );
    expect(resume?.status).toBe(200);
    expect(statusCalls).toBe(1);
    expect(resumeCalls).toBe(1);
    expect(resumeRequester).toEqual({ id: beta.id, kind: 'human' });
  });

  it('403s an invalid authenticated role before minting a run id', async () => {
    const { handle, started } = makeHarness();

    // #when / #then
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow' },
        actor: { id: 'eve', role: 'owner' },
      }),
    );
    expect(response?.status).toBe(403);
    expect(started).toEqual([]);
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
      runId: 'generated-run-id',
      status: 'success',
      result: { ok: true },
    });
    expect(await store.list()).toEqual([]);
  });

  it('queues an approval attributed to the STARTING actor on a suspension', async () => {
    // #given — the bridge's SoD contract: whoever advanced the run is the
    // requester, so they cannot also decide it
    const { handle, store } = makeHarness({
      start: async ({ runId, principal }) => ({
        ...suspendedSummary(runId),
        requestedBy: principal.id,
        requestedByKind: principal.kind,
      }),
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
        requestedByKind: 'human',
      },
    });
    expect(await store.list({ status: 'pending' })).toHaveLength(1);
  });

  it('the starting actor cannot decide the approval their own start queued', async () => {
    // #given — an admin (holding both CAN_CREATE and CAN_REVIEW) starts a run
    // that suspends
    const { handle, service, store } = makeHarness({
      start: async ({ runId }) => suspendedSummary(runId),
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
        { id: ADMIN.id, role: 'admin' },
      ),
    ).rejects.toThrow(/cannot decide their own approval/);
  });

  it('returns a committed suspended run when initial filing fails and heals with its durable requester', async () => {
    const { handle, service, store } = makeHarness({
      resourceOwner: ADMIN,
      approvalCreateFailures: 1,
      start: async ({ runId, principal }) => ({
        ...suspendedSummary(runId),
        requestedBy: principal.id,
      }),
      status: async (_workflowId, runId) => ({
        ...suspendedSummary(runId),
        requestedBy: ADMIN.id,
      }),
      reconcileApprovals: reconcileApprovalsOnStatus(SYSTEM.id),
    });

    const started = await handle(
      req('/runs', { body: { workflowId: 'open-flow' }, actor: ADMIN }),
    );

    expect(started?.status).toBe(200);
    expect(await started?.json()).toMatchObject({
      runId: 'generated-run-id',
      status: 'suspended',
      requestedBy: ADMIN.id,
    });
    expect(await store.list({ status: 'pending' })).toEqual([]);

    const status = await handle(
      req('/runs/open-flow/generated-run-id', { actor: ADMIN }),
    );
    expect(status?.status).toBe(200);
    const [healed] = await store.list({ status: 'pending' });
    expect(healed?.requestedBy).toBe(ADMIN.id);
    await expect(
      service.decide(
        healed?.id ?? '',
        { decision: 'approve' },
        { id: ADMIN.id, role: 'admin' },
      ),
    ).rejects.toThrow(/cannot decide their own approval/);
  });
});

describe('createRunRouter — POST terminate', () => {
  it('maps a direct lifecycle dispute to a structured 409 response', async () => {
    const { handle } = makeHarness({
      terminate: async () => {
        throw new RunLifecycleBlockedError({
          code: 'DISPUTED_SETTLEMENT',
          message:
            'run termination is blocked while an economic operation is disputed',
        });
      },
    });

    const response = await handle(
      req('/runs/open-flow/acme_r1/terminate', { method: 'POST' }),
    );

    expect(response?.status).toBe(409);
    await expect(response?.json()).resolves.toEqual({
      error:
        'run termination is blocked while an economic operation is disputed',
      reason: {
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      },
    });
  });

  it('maps a direct terminal conflict to 409', async () => {
    const { handle } = makeHarness({
      terminate: async () => {
        throw new RunTerminalConflictError('open-flow', 'acme_r1', 'success');
      },
    });

    const response = await handle(
      req('/runs/open-flow/acme_r1/terminate', { method: 'POST' }),
    );

    expect(response?.status).toBe(409);
    await expect(response?.json()).resolves.toEqual({
      error:
        "run 'acme_r1' of workflow 'open-flow' is already terminal with status 'success'",
    });
  });

  it('uses owner-DO replay authorization only after ownership is gone and keeps unrelated callers opaque', async () => {
    const terminate = vi.fn(
      async (
        _workflowId: string,
        runId: string,
        principal: { id: string },
        _replayOnly: boolean,
      ): Promise<RunSummary> => {
        if (principal.id !== OPERATOR.id) {
          throw new UnknownRunError('open-flow', runId);
        }
        return {
          runId,
          status: 'cancelled',
          errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
        };
      },
    );
    const { handle, resources } = makeHarness({ terminate });

    const owned = await handle(
      req('/runs/open-flow/acme_r1/terminate', { method: 'POST' }),
    );
    expect(owned?.status).toBe(200);
    expect(terminate).toHaveBeenLastCalledWith(
      'open-flow',
      'acme_r1',
      expect.objectContaining({ kind: 'human', id: OPERATOR.id }),
      false,
    );

    await expect(
      resources.release('run', 'acme_r1', {
        kind: 'human',
        id: OPERATOR.id,
      }),
    ).resolves.toBe(true);
    const replay = await handle(
      req('/runs/open-flow/acme_r1/terminate', { method: 'POST' }),
    );
    expect(replay?.status).toBe(200);
    expect(await replay?.json()).toEqual({
      runId: 'acme_r1',
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    });
    expect(terminate).toHaveBeenLastCalledWith(
      'open-flow',
      'acme_r1',
      expect.objectContaining({ kind: 'human', id: OPERATOR.id }),
      true,
    );

    const unrelated = await handle(
      req('/runs/open-flow/acme_r1/terminate', {
        method: 'POST',
        actor: { id: 'eve', role: 'operator' },
      }),
    );
    expect(unrelated?.status).toBe(404);
    expect(await unrelated?.json()).toEqual({
      error: "no run 'acme_r1' found for workflow 'open-flow'",
    });
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
      reconcileApprovals: async (_context, workflowId, summary) => {
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

// ---------------------------------------------------------------------------
// F3 — owner-bound idempotent start, on the workflow surface.
//
// Every case below is about a PAID first step: the assertions count
// EXECUTIONS, not responses, because a router that answers correctly while
// starting a second run has failed at exactly the thing the key was bought for.
// ---------------------------------------------------------------------------

/** A run router whose reservations live in a real table over node:sqlite. */
function keyedHarness(
  options: HarnessOptions & {
    live?: (workflowId: string, runId: string) => Promise<boolean>;
    executionFence?: ExecutionFenceWiring;
    now?: () => number;
  } = {},
) {
  const sqlite = openSqlite();
  const store = new StartIdempotencyStore(
    sqliteUnitDatabase(sqlite) as StartIdempotencyDatabase,
    options.now ? { now: options.now } : {},
  );
  const persisted = new Map<string, RunSummary>();
  const executions: string[] = [];
  // The stand-in for a run object's in-flight set: what a liveness probe
  // actually answers from. Populated for the whole of a start, so a sibling
  // request that arrives mid-execution sees the truth a DO would report.
  const inFlight = new Set<string>();
  let runSeq = 0;
  const harness = makeHarness({
    ...options,
    startIdempotency: {
      store,
      live: options.live ?? (async (_workflowId, runId) => inFlight.has(runId)),
      executionFence: options.executionFence ?? 'none',
    },
    start:
      options.start ??
      (async ({ runId, workflowId }) => {
        inFlight.add(runId);
        try {
          // The stand-in for the paid first step. The awaited tick is what lets
          // a parallel sibling actually interleave here.
          await Promise.resolve();
          executions.push(runId);
          const summary: RunSummary = {
            runId,
            status: 'success',
            result: { ok: true, workflowId },
          };
          persisted.set(runId, summary);
          return summary;
        } finally {
          inFlight.delete(runId);
        }
      }),
    status:
      options.status ?? (async (_workflowId, runId) => persisted.get(runId)),
  });
  return {
    ...harness,
    store,
    persisted,
    executions,
    /** Distinct ids per mint, so a converged retry is visible as convergence. */
    nextRunId: () => {
      runSeq += 1;
      return `mint_${runSeq}`;
    },
  };
}

describe('createRunRouter — idempotent start (F3)', () => {
  it('refuses a key on a host that wired no reservation store', async () => {
    // #given the typed opt-out — the default in this file
    const { handle } = makeHarness();

    // #when
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #then 503 with a code, never a silent unkeyed start: ignoring the key
    // would answer an exactly-once request with at-least-once behaviour.
    expect(response?.status).toBe(503);
    expect(await response?.json()).toMatchObject({
      reason: { code: 'IDEMPOTENT_START_UNSUPPORTED' },
    });
  });

  it('keeps refusing a caller-supplied runId even alongside a key', async () => {
    // #given — the key is the one identifier a client may choose; the runId
    // names a slot in every store this deployment has and stays server-minted.
    const { handle } = keyedHarness();

    // #when
    const response = await handle(
      req('/runs', {
        body: {
          workflowId: 'open-flow',
          runId: 'chosen',
          idempotencyKey: 'key-1',
        },
      }),
    );

    // #then the pinned 400 is untouched by F3
    expect(response?.status).toBe(400);
    expect(await response?.json()).toEqual({
      error: 'runId is server-assigned',
    });
  });

  it('starts once and replays the SAME run for a retry, with no second execution', async () => {
    // #given a host whose first step is expensive
    const { handle, executions } = keyedHarness();

    // #when the response to the first call is lost and the client retries
    const first = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );
    const retry = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #then one execution, and both answers describe that one run
    expect(executions).toHaveLength(1);
    const firstBody = (await first?.json()) as RunSummary;
    const retryBody = (await retry?.json()) as RunSummary;
    expect(retry?.status).toBe(200);
    expect(retryBody.runId).toBe(firstBody.runId);
    expect(retryBody).toEqual(firstBody);
  });

  it('starts ONE run for two same-key requests issued in parallel', async () => {
    // #given the cross-isolate race a kill-and-retry harness cannot fake: two
    // in-flight first calls against one database, neither having seen the other
    const { handle, executions } = keyedHarness();

    // #when
    const [a, b] = await Promise.all([
      handle(
        req('/runs', {
          body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
        }),
      ),
      handle(
        req('/runs', {
          body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
        }),
      ),
    ]);

    // #then exactly one execution. That is the invariant; the loser's exact
    // answer is not, and all three of its possible answers are honest:
    //   200 the winner already persisted, so the loser replays it
    //   503 the winner is executing, so the loser is told PENDING
    //   409 the loser probed inside the CLAIM-TO-DISPATCH WINDOW — the moment
    //       between the winning CAS landing and the winner's start request
    //       reaching the object that would report it live. Nothing is running
    //       yet from any observer's point of view, so UNRESOLVABLE is what the
    //       probe can honestly say. It is a false alarm, not a lost run: the
    //       claim still stands, the winner still executes, and the caller's
    //       next retry replays the persisted summary.
    // What must never appear is a second entry in `executions`.
    expect(executions).toHaveLength(1);
    for (const response of [a, b]) {
      expect([200, 409, 503]).toContain(response?.status);
    }
  });

  it('refuses the same key pointed at a different workflow', async () => {
    // #given a key already spent on open-flow by an actor who may start both
    const { handle, executions } = keyedHarness();
    await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
        actor: ADMIN,
      }),
    );

    // #when the SAME principal reuses it for another workflow — the same
    // principal, so this is a target mismatch and not an ownership one
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'restricted-flow', idempotencyKey: 'key-1' },
        actor: ADMIN,
      }),
    );

    // #then 409 naming what the key holds, and no second execution
    expect(response?.status).toBe(409);
    expect(await response?.json()).toMatchObject({
      reason: {
        code: 'IDEMPOTENT_START_TARGET_MISMATCH',
        targetId: 'open-flow',
      },
    });
    expect(executions).toHaveLength(1);
  });

  it('refuses a key another principal reserved, with 403 and nothing else', async () => {
    // #given
    const { handle } = keyedHarness();
    await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #when a different principal sends the same key
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
        actor: ADMIN,
      }),
    );

    // #then
    expect(response?.status).toBe(403);
    expect(await response?.json()).toMatchObject({
      reason: { code: 'IDEMPOTENT_START_OWNER_MISMATCH' },
    });
  });

  it('answers 503 PENDING while the reserved run is still executing', async () => {
    // #given a claimed reservation whose run has persisted nothing and whose
    // host reports it live
    const { handle, store } = keyedHarness({ live: async () => true });
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: OPERATOR.id },
      targetKind: 'workflow',
      targetId: 'open-flow',
      mintRunId: () => 'inflight_run',
    });
    await store.claim('key-1', 'inflight_run');

    // #when
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #then retryable, and it names the run and when the claim was taken
    expect(response?.status).toBe(503);
    expect(await response?.json()).toMatchObject({
      reason: { code: 'IDEMPOTENT_START_PENDING', runId: 'inflight_run' },
    });
  });

  it('answers 409 UNRESOLVABLE when the claim is held but nothing is running', async () => {
    // #given the crash window: claim taken, nothing persisted, host gone
    const { handle, store, executions } = keyedHarness({
      live: async () => false,
    });
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: OPERATOR.id },
      targetKind: 'workflow',
      targetId: 'open-flow',
      mintRunId: () => 'orphan_run',
    });
    await store.claim('key-1', 'orphan_run');

    // #when
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #then refused, never re-executed: whether the first step already took
    // effect is unknowable, so a human decides.
    expect(response?.status).toBe(409);
    expect(await response?.json()).toMatchObject({
      reason: { code: 'IDEMPOTENT_START_UNRESOLVABLE', runId: 'orphan_run' },
    });
    expect(executions).toEqual([]);
  });

  it('answers 409 ALREADY_SETTLED for a key whose run completed and aged out', async () => {
    // #given a settled reservation with no surviving snapshot
    const { handle, store, executions } = keyedHarness();
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: OPERATOR.id },
      targetKind: 'workflow',
      targetId: 'open-flow',
      mintRunId: () => 'settled_run',
    });
    await store.claim('key-1', 'settled_run');
    await store.settleRun('settled_run');

    // #when
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #then the work is done even though its outcome is gone
    expect(response?.status).toBe(409);
    expect(await response?.json()).toMatchObject({
      reason: { code: 'IDEMPOTENT_START_ALREADY_SETTLED' },
    });
    expect(executions).toEqual([]);
  });

  it('re-claims a reservation whose first caller died before claiming, keeping its run id', async () => {
    // #given a bare reservation — nothing has executed under it
    const { handle, store, executions } = keyedHarness();
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: OPERATOR.id },
      targetKind: 'workflow',
      targetId: 'open-flow',
      mintRunId: () => 'reserved_run',
    });

    // #when
    const response = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #then it proceeds under the RESERVED id rather than minting a new one
    expect(response?.status).toBe(200);
    expect(executions).toEqual(['reserved_run']);
    expect((await response?.json()) as RunSummary).toMatchObject({
      runId: 'reserved_run',
    });
  });

  it('gives the claim back when the execution fence refuses, and converges on retry after it reopens', async () => {
    // #given a deployment whose fence closes between the claim and the start
    let fenced = true;
    const executions: string[] = [];
    const { handle, store } = keyedHarness({
      start: async ({ runId }) => {
        if (fenced) {
          throw new ExecutionFencedError('migration-locked', 'run start');
        }
        executions.push(runId);
        return { runId, status: 'success' };
      },
    });

    // #when the fenced start is refused, the operator reopens, and the client
    // retries with the same key
    const refused = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );
    const reservedRunId = (await store.read('key-1'))?.runId;
    fenced = false;
    const retry = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #then the fence's own refusal reached the caller, the claim went back,
    // and the retry ran the SAME run — a fence transition mid-start must not
    // manufacture an unresolvable reservation, nor a second run.
    expect(refused?.status).toBe(503);
    expect(await refused?.json()).toMatchObject({
      reason: { code: 'EXECUTION_FENCED' },
    });
    expect(retry?.status).toBe(200);
    expect(executions).toEqual([reservedRunId]);
  });

  it('keeps the claim when a start fails for any other reason', async () => {
    // #given a start that failed somewhere it may already have executed
    const { handle, store } = keyedHarness({
      start: async () => {
        throw new Error('step exploded');
      },
    });

    // #when
    await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );

    // #then the claim is held, so the next retry is refused rather than handed
    // a fresh run
    expect((await store.read('key-1'))?.state).toBe('started');
  });

  it('files an approval once, and reconciles rather than re-files on the replay', async () => {
    // #given a run that suspends on its first (and only) start
    const reconciled: string[] = [];
    const persistedSuspensions = new Set<string>();
    const { handle, store, service } = keyedHarness({
      start: async ({ runId }) => {
        persistedSuspensions.add(runId);
        return suspendedSummary(runId);
      },
      status: async (_workflowId, runId) =>
        persistedSuspensions.has(runId) ? suspendedSummary(runId) : undefined,
      reconcileApprovals: async (_context, _workflowId, summary) => {
        reconciled.push(summary.runId);
      },
    });

    // #when the first start files an approval and the retry replays
    const first = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );
    const retry = await handle(
      req('/runs', {
        body: { workflowId: 'open-flow', idempotencyKey: 'key-1' },
      }),
    );
    const reservedRunId = (await store.read('key-1'))?.runId as string;

    // #then exactly ONE approval record exists. Re-running the start's
    // unconditional filing on the replay would create a second record for the
    // same gate on every retry — the exact duplication the key was bought to
    // prevent — so the replay takes the deduplicating reconcile instead.
    expect((await first?.json()) as { approvals?: unknown[] }).toMatchObject({
      approvals: [expect.objectContaining({ runId: reservedRunId })],
    });
    expect(retry?.status).toBe(200);
    expect(reconciled).toEqual([reservedRunId]);
    const listed = await service.list(
      { workflowId: 'open-flow', runId: reservedRunId },
      SYSTEM,
    );
    expect(listed).toHaveLength(1);
  });
});
