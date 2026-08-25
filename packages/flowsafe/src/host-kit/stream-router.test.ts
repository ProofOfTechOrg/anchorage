// SPDX-License-Identifier: Apache-2.0
// The stream surface: the authenticated ticket route (401 / ownership-404 /
// mint), the two ticket-verified WebSocket-upgrade routes (fail closed on a bad
// ticket, forward a good one to the right idFromName), and the composer opt-in
// (absent HUB/secret leaves the stage unmounted). Stub HubNamespaceLike /
// RunnerNamespaceLike record idFromName args and echo the forwarded path so the
// routing is observable without workerd.
//
// The hub FAN-OUT wiring (buildHostApprovalService forwards each mutation to
// idFromName(HUB_INSTANCE_NAME); the alarm-owned path collects the publish into
// pendingSends and awaits it) is proven in the final describe. It belongs to
// host-approval-service.ts but is exercised here because that module's own test
// file is outside that edit scope; the wiring it proves still needs coverage.

import { describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  type ActorResolver,
  type ApprovalActor,
  type ApprovalRecord,
  ApprovalService,
  type ApprovalStreamEvent,
  createActorResolver,
  InMemoryApprovalStoreFactory,
} from '../approval-api/index.js';
import {
  doErrorResponse,
  type ExecutionFenceDatabase,
  ExecutionFenceStore,
  HUB_INSTANCE_NAME,
} from '../do-runner/index.js';
import { doSummary } from './do-response.js';
import type { RunnerNamespaceLike } from './do-run-topology.js';
import {
  createFlowsafeWorker,
  type FlowsafeWorkerEnv,
} from './flowsafe-worker.js';
import {
  buildHostApprovalService,
  maintenancePrincipal,
  runSlaSweepMaintenance,
} from './host-approval-service.js';
import { createHubTopology, type HubNamespaceLike } from './hub-topology.js';
import { RunRouteError } from './run-route-error.js';
import { createStreamRouter } from './stream-router.js';
import { mintStreamTicket, verifyStreamTicket } from './stream-ticket.js';
import { staticTokenVerifier } from './verifier.js';
import type { WorkflowMeta } from './workflow-meta.js';

const SECRET = 'stream-router-secret';
const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';
const REVIEWER: ApprovalActor = {
  id: 'ray',
  role: 'reviewer',
};
const RUN_ID = 'acme_run-1';

interface HubHit {
  idName: string;
  request?: Request;
  body?: unknown;
}

function recordingHub(hits: HubHit[]): HubNamespaceLike {
  return {
    idFromName: (name: string) => name,
    get: (id: unknown) => ({
      fetch: async (input: Request | string, init?: { body?: string }) => {
        if (typeof input === 'string') {
          hits.push({
            idName: String(id),
            body: init?.body ? JSON.parse(init.body) : null,
          });
          return new Response(null, { status: 200 });
        }
        hits.push({ idName: String(id), request: input });
        const forwarded = new URL(input.url);
        return new Response('subscribed', {
          status: 200,
          headers: {
            'x-hub-id': String(id),
            'x-path': forwarded.pathname,
            'x-actor-id': forwarded.searchParams.get('actorId') ?? '',
            'x-role': forwarded.searchParams.get('role') ?? '',
            'x-upgrade': input.headers.get('upgrade') ?? '',
            'x-ticket': forwarded.searchParams.get('ticket') ?? '',
          },
        });
      },
    }),
  } as unknown as HubNamespaceLike;
}

interface RunnerHit {
  idName: string;
  path: string;
  upgrade: string;
}

function recordingRunner(hits: RunnerHit[]): RunnerNamespaceLike {
  return {
    idFromName: (name: string) => name,
    get: (id: unknown) => ({
      fetch: async (input: Request | string) => {
        const request = input as Request;
        const forwarded = new URL(request.url);
        hits.push({
          idName: String(id),
          path: forwarded.pathname,
          upgrade: request.headers.get('upgrade') ?? '',
        });
        return new Response('run-stream', {
          status: 200,
          headers: { 'x-run-id': String(id), 'x-path': forwarded.pathname },
        });
      },
    }),
  } as unknown as RunnerNamespaceLike;
}

/** Header-transported identity, mirroring run-router.test.ts. */
function makeResolve(): ActorResolver {
  const backend = new InMemoryApprovalStoreFactory();
  void backend.resources().claim('run', RUN_ID, {
    kind: 'human',
    id: REVIEWER.id,
  });
  return createActorResolver({
    authenticate: (request) => {
      const id = request.headers.get('x-actor-id');
      const role = request.headers.get('x-actor-role');
      return id && role
        ? { id, role: role as ApprovalActor['role'] }
        : undefined;
    },
    storeFactory: backend,
    // In-memory store, no database to fence against: the opt-out is written down
    // rather than defaulted — see ExecutionFenceWiring.
    buildService: (store) =>
      new ApprovalService({ store, executionFence: 'none' }),
  });
}

function makeRouter(hub: HubNamespaceLike, runner: RunnerNamespaceLike) {
  return createStreamRouter({
    resolve: makeResolve(),
    ticketSecret: SECRET,
    hub,
    runner,
    runStatus: async (workflowId, runId) =>
      workflowId === 'wf' && runId === RUN_ID
        ? { runId, status: 'suspended' }
        : undefined,
    deploymentIdentitySecret: DEPLOYMENT_IDENTITY_SECRET,
  });
}

function authedPost(body: unknown, actor = REVIEWER): Request {
  return new Request('http://host/api/stream/ticket', {
    method: 'POST',
    headers: {
      'x-actor-id': actor.id,
      'x-actor-role': actor.role,
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
  });
}

function wsGet(path: string): Request {
  return new Request(`http://host${path}`, {
    headers: { upgrade: 'websocket' },
  });
}

describe('createStreamRouter ticket route', () => {
  it('composes: returns null for a non-/api/stream path', async () => {
    // #given
    const router = makeRouter(recordingHub([]), recordingRunner([]));

    // #when / #then — the approval surface must fall through
    expect(await router(new Request('http://host/api/approvals'))).toBeNull();
    expect(await router(new Request('http://host/workflows'))).toBeNull();
  });

  it('401s an unauthenticated ticket request', async () => {
    // #given — no x-actor headers
    const router = makeRouter(recordingHub([]), recordingRunner([]));
    const request = new Request('http://host/api/stream/ticket', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ channel: 'hub' }),
    });

    // #when / #then
    expect((await router(request))?.status).toBe(401);
  });

  it('413s a ticket body before JSON parsing', async () => {
    const router = makeRouter(recordingHub([]), recordingRunner([]));

    const response = await router(authedPost({ padding: 'x'.repeat(4096) }));

    expect(response?.status).toBe(413);
  });

  it('mints a ~60s hub ticket that verifies to the deployment hub', async () => {
    // #given
    const router = makeRouter(recordingHub([]), recordingRunner([]));

    // #when
    const response = await router(authedPost({ channel: 'hub' }));
    const body = (await response?.json()) as {
      url: string;
      ticket: string;
      expiresAt: number;
    };

    // #then
    expect(response?.status).toBe(200);
    expect(body.url).toBe('/api/stream/hub');
    expect(body.expiresAt).toBeGreaterThan(Date.now());
    const claims = await verifyStreamTicket({
      secret: SECRET,
      token: body.ticket,
    });
    expect(claims).toMatchObject({
      channel: 'hub',
      actorId: 'ray',
      role: 'reviewer',
    });
  });

  it('mints a run ticket for an owned run and builds the wf-qualified url', async () => {
    // #given
    const router = makeRouter(recordingHub([]), recordingRunner([]));

    // #when
    const response = await router(
      authedPost({ channel: 'run', runId: RUN_ID, workflowId: 'wf' }),
    );
    const body = (await response?.json()) as { url: string; ticket: string };

    // #then
    expect(response?.status).toBe(200);
    expect(body.url).toBe(`/api/stream/run/wf/${RUN_ID}`);
    const claims = await verifyStreamTicket({
      secret: SECRET,
      token: body.ticket,
    });
    expect(claims).toMatchObject({
      channel: 'run',
      workflowId: 'wf',
      runId: RUN_ID,
    });
  });

  it('404s a path-safe run address the actor cannot access', async () => {
    // #given — ticket minting is an authorization boundary, not only syntax.
    const router = makeRouter(recordingHub([]), recordingRunner([]));

    // #when
    const response = await router(
      authedPost({ channel: 'run', runId: 'bravo_run-9' }),
    );

    // #then
    expect(response?.status).toBe(404);
  });

  it('400s an unknown channel and a run ticket with no runId', async () => {
    // #given
    const router = makeRouter(recordingHub([]), recordingRunner([]));

    // #when / #then
    expect((await router(authedPost({ channel: 'bogus' })))?.status).toBe(400);
    expect((await router(authedPost({ channel: 'run' })))?.status).toBe(400);
    expect(
      (await router(authedPost({ channel: 'run', runId: RUN_ID })))?.status,
    ).toBe(400);
  });
});

describe('createStreamRouter hub WebSocket upgrade', () => {
  async function hubTicket(): Promise<string> {
    return mintStreamTicket({
      secret: SECRET,
      channel: 'hub',
      actor: REVIEWER,
    });
  }

  it('routes a good ticket to the singleton hub rewritten to /subscribe with presence params', async () => {
    // #given
    const hits: HubHit[] = [];
    const router = makeRouter(recordingHub(hits), recordingRunner([]));
    const ticket = await hubTicket();

    // #when
    const response = await router(wsGet(`/api/stream/hub?ticket=${ticket}`));

    // #then — the hub's own 101/200 Response is returned unmodified
    expect(response?.status).toBe(200);
    expect(hits.map((hit) => hit.idName)).toEqual([HUB_INSTANCE_NAME]);
    expect(response?.headers.get('x-path')).toBe('/subscribe');
    expect(response?.headers.get('x-actor-id')).toBe('ray');
    expect(response?.headers.get('x-role')).toBe('reviewer');
    // the upgrade survives the forward, and the ticket is stripped
    expect(response?.headers.get('x-upgrade')).toBe('websocket');
    expect(response?.headers.get('x-ticket')).toBe('');
  });

  it('426s a non-upgrade GET, 401s a missing ticket, 403s a bad or wrong-channel ticket', async () => {
    // #given
    const hits: HubHit[] = [];
    const router = makeRouter(recordingHub(hits), recordingRunner([]));

    // #when / #then — no Upgrade header
    expect(
      (await router(new Request('http://host/api/stream/hub')))?.status,
    ).toBe(426);
    // missing ticket
    expect((await router(wsGet('/api/stream/hub')))?.status).toBe(401);
    // garbage ticket
    expect((await router(wsGet('/api/stream/hub?ticket=nope')))?.status).toBe(
      403,
    );
    // a RUN ticket presented on the HUB route (cross-channel)
    const runTicket = await mintStreamTicket({
      secret: SECRET,
      channel: 'run',
      workflowId: 'wf',
      runId: RUN_ID,
      actor: REVIEWER,
    });
    expect(
      (await router(wsGet(`/api/stream/hub?ticket=${runTicket}`)))?.status,
    ).toBe(403);
    // nothing was ever forwarded to the hub
    expect(hits).toHaveLength(0);
  });
});

describe('createStreamRouter run WebSocket upgrade', () => {
  async function runTicket(runId = RUN_ID): Promise<string> {
    return mintStreamTicket({
      secret: SECRET,
      channel: 'run',
      workflowId: 'wf',
      runId,
      actor: REVIEWER,
    });
  }

  it('routes a good run ticket to idFromName(wf:runId) rewritten to the run stream path', async () => {
    // #given
    const hits: RunnerHit[] = [];
    const router = makeRouter(recordingHub([]), recordingRunner(hits));
    const ticket = await runTicket();

    // #when
    const response = await router(
      wsGet(`/api/stream/run/wf/${RUN_ID}?ticket=${ticket}`),
    );

    // #then
    expect(response?.status).toBe(200);
    expect(hits).toEqual([
      {
        idName: `wf:${RUN_ID}`,
        path: `/runs/wf/${RUN_ID}/stream`,
        upgrade: 'websocket',
      },
    ]);
  });

  it('403s a ticket whose runId does not match the path (cross-run)', async () => {
    // #given — a ticket for RUN_ID used against acme_run-2
    const hits: RunnerHit[] = [];
    const router = makeRouter(recordingHub([]), recordingRunner(hits));
    const ticket = await runTicket();

    // #when / #then
    expect(
      (await router(wsGet(`/api/stream/run/wf/acme_run-2?ticket=${ticket}`)))
        ?.status,
    ).toBe(403);
    expect(hits).toHaveLength(0);
  });

  it('403s a garbage ticket and 401s a missing one', async () => {
    // #given
    const router = makeRouter(recordingHub([]), recordingRunner([]));

    // #when / #then
    expect(
      (await router(wsGet(`/api/stream/run/wf/${RUN_ID}?ticket=xxx`)))?.status,
    ).toBe(403);
    expect((await router(wsGet(`/api/stream/run/wf/${RUN_ID}`)))?.status).toBe(
      401,
    );
  });
});

describe('createFlowsafeWorker stream stage opt-in', () => {
  const WORKFLOWS: ReadonlyArray<WorkflowMeta> = [
    { id: 'wf', title: 'x', description: 'y', sampleInput: {} },
  ];
  const TOKENS = new Map([['tok-ray', REVIEWER]]);

  function makeEnv(withStreaming: boolean): FlowsafeWorkerEnv {
    const sqlite = openSqlite();
    sqlite
      .prepare(
        `CREATE TABLE flowsafe_deployment (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          tenant_tag TEXT NOT NULL,
          provisioned_at TEXT NOT NULL
        )`,
      )
      .run();
    sqlite
      .prepare(
        `INSERT INTO flowsafe_deployment (id, tenant_tag, provisioned_at)
         VALUES (1, 'acme', '2026-08-10T00:00:00.000Z')`,
      )
      .run();
    const env: FlowsafeWorkerEnv = {
      DB: sqliteUnitDatabase(sqlite) as FlowsafeWorkerEnv['DB'],
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET,
      RUNNER: recordingRunner([]) as FlowsafeWorkerEnv['RUNNER'],
      MAINTENANCE: {
        idFromName: (name: string) => name,
        get: () => ({ fetch: async () => new Response(null, { status: 204 }) }),
      },
    };
    if (withStreaming) {
      env.HUB = recordingHub([]);
      env.STREAM_TICKET_SECRET = SECRET;
    }
    return env;
  }

  function ticketRequest(): Request {
    return new Request('http://host/api/stream/ticket', {
      method: 'POST',
      headers: {
        authorization: 'Bearer tok-ray',
        'content-type': 'application/json',
      },
      body: JSON.stringify({ channel: 'hub' }),
    });
  }

  const worker = createFlowsafeWorker<FlowsafeWorkerEnv>({
    workflows: WORKFLOWS,
    systemPrincipalId: 'sys',
    buildVerifier: () => staticTokenVerifier(TOKENS),
    maintenance: {
      sweepIntervalMs: 15 * 60 * 1_000,
      purgeIntervalMs: 60 * 60 * 1_000,
    },
  });
  const ctx = { waitUntil: () => {} };

  it('leaves the stream stage UNMOUNTED without HUB + secret (poll-only)', async () => {
    // #given — no HUB/secret
    // #when — the stream path falls through to the composer's 404
    const response = await worker.fetch(ticketRequest(), makeEnv(false), ctx);

    // #then
    expect(response.status).toBe(404);
  });

  it('mounts the stream stage when BOTH HUB and secret are present', async () => {
    // #given — HUB + secret bound
    // #when
    const response = await worker.fetch(ticketRequest(), makeEnv(true), ctx);

    // #then — the ticket route answers 200 (mounted + authenticated)
    expect(response.status).toBe(200);
    const body = (await response.json()) as { ticket: string };
    expect(body.ticket).toBeTruthy();
  });
});

describe('hub fan-out wiring (host-approval-service, tested here — see file header)', () => {
  it('rejects a non-success hub response without parsing its body', async () => {
    const topology = createHubTopology(
      {
        idFromName: (name: string) => name,
        get: () => ({
          fetch: () =>
            Promise.resolve(
              new Response('private hub detail', { status: 503 }),
            ),
        }),
      },
      DEPLOYMENT_IDENTITY_SECRET,
    );

    await expect(
      topology.publish({
        type: 'created',
        record: {
          id: 'apr-1',
          workflowId: 'wf',
          runId: RUN_ID,
          title: 'approval',
          connectors: [],
          priority: 'normal',
          status: 'pending',
          createdAt: new Date(0).toISOString(),
          updatedAt: new Date(0).toISOString(),
        },
      }),
    ).rejects.toThrow('status 503');
  });

  it('buildHostApprovalService forwards each mutation to the deployment hub', async () => {
    // #given — a fetch-scope-shaped sink that publishes to a stub hub
    const hits: HubHit[] = [];
    const hubTopology = createHubTopology(
      recordingHub(hits),
      DEPLOYMENT_IDENTITY_SECRET,
    );
    const pending: Promise<unknown>[] = [];
    const store = new InMemoryApprovalStoreFactory().store();
    const service = buildHostApprovalService(store, {
      systemPrincipalId: 'sys',
      resumeRun: async (record) => ({ runId: record.runId, status: 'success' }),
      stream: (event) => {
        pending.push(hubTopology.publish(event));
      },
      // In-memory approval store — no database, nothing to fence.
      executionFence: 'none',
    });

    // #when — a create mutation fires the stream sink once
    await service.create(
      {
        workflowId: 'wf',
        runId: RUN_ID,
        title: 't',
        requestedBy: 'sys',
        requestedByKind: 'human',
      },
      { id: 'op', role: 'operator' },
    );
    await Promise.all(pending);

    // #then — the event reached the deployment singleton
    expect(hits.map((hit) => hit.idName)).toEqual([HUB_INSTANCE_NAME]);
    const event = hits[0]?.body as ApprovalStreamEvent;
    expect(event.type).toBe('created');
    expect(event.record.runId).toBe(RUN_ID);
  });

  it('the alarm sweep COLLECTS the escalation publish and AWAITS it (no float)', async () => {
    // #given — a hub whose publish blocks until released, and an overdue record
    vi.spyOn(console, 'log').mockImplementation(() => {});
    const hits: string[] = [];
    let releasePublish!: () => void;
    const gate = new Promise<void>((resolve) => {
      releasePublish = resolve;
    });
    const hub: HubNamespaceLike = {
      idFromName: (name: string) => name,
      get: (id: unknown) => ({
        fetch: async () => {
          hits.push(String(id));
          await gate;
          return new Response(null, { status: 200 });
        },
      }),
    } as unknown as HubNamespaceLike;
    const hubTopology = createHubTopology(hub, DEPLOYMENT_IDENTITY_SECRET);

    const factory = new InMemoryApprovalStoreFactory();
    const past = new Date(Date.now() - 60_000).toISOString();
    const record: ApprovalRecord = {
      id: 'apr-overdue',
      workflowId: 'wf',
      runId: 'acme_r1',
      title: 'overdue',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: past,
      updatedAt: past,
      slaDeadlineAt: past,
    };
    await factory.store().create(record);

    // #when — the sweep collects the (blocked) publish into pendingSends
    const order: string[] = [];
    const sweep = runSlaSweepMaintenance({
      store: factory.store(),
      systemPrincipal: maintenancePrincipal('sys'),
      trigger: 'sweep',
      stream: (event) => hubTopology.publish(event),
    }).then(() => order.push('sweep-resolved'));

    // drain the store I/O so the sweep reaches its terminal Promise.all(pendingSends)
    await new Promise((resolve) => setTimeout(resolve, 0));
    // the escalation publish was fired (collected) but is still pending on the gate
    expect(hits).toEqual([HUB_INSTANCE_NAME]);
    expect(order).toEqual([]);

    order.push('publish-released');
    releasePublish();
    await sweep;

    // #then — the sweep resolved ONLY AFTER the publish: it awaited, never floated
    expect(order).toEqual(['publish-released', 'sweep-resolved']);
  });
});

describe('createStreamRouter run-route passthrough', () => {
  function routerWith(runStatus: () => Promise<never>) {
    return createStreamRouter({
      resolve: makeResolve(),
      ticketSecret: SECRET,
      hub: recordingHub([]),
      runner: recordingRunner([]),
      runStatus,
      deploymentIdentitySecret: DEPLOYMENT_IDENTITY_SECRET,
    });
  }

  it('passes a 5xx refusal through with its status and reason intact', async () => {
    // #given — a run-status read refused because the deployment is fenced.
    // This router used to collapse every 5xx into a bare 500, so a stream
    // caller could not tell a migration from a broken deployment.
    const router = routerWith(() =>
      Promise.reject(
        new RunRouteError(
          503,
          "deployment execution is fenced ('migration-locked'): run resume is refused",
          { code: 'EXECUTION_FENCED', state: 'migration-locked' },
        ),
      ),
    );

    // #when
    const response = await router(
      authedPost({ channel: 'run', runId: RUN_ID, workflowId: 'wf' }),
    );

    // #then
    expect(response?.status).toBe(503);
    expect(await response?.json()).toEqual({
      error:
        "deployment execution is fenced ('migration-locked'): run resume is refused",
      reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
    });
  });

  it('passes an UNREADABLE fence through without the storage error behind it', async () => {
    // #given — the real chain: a fence store over a faulting database, the
    // refusal it raises, the DO's error mapping, and the reader that turns a DO
    // response back into a RunRouteError. The passthrough forwards a 5xx
    // message verbatim, so the only thing keeping the storage fault off the
    // wire is that the refusal never carries it in `message`.
    const secret = 'D1_ERROR: connect ECONNREFUSED 10.0.7.4:5432 db=acme-prod';
    const fence = new ExecutionFenceStore({
      prepare: () => ({
        bind: () => ({
          run: () => Promise.reject(new Error(secret)),
          all: () => Promise.reject(new Error(secret)),
        }),
        run: () => Promise.reject(new Error(secret)),
        all: () => Promise.reject(new Error(secret)),
      }),
    } as unknown as ExecutionFenceDatabase);
    const refusal = await fence.read().catch((error: unknown) => error);
    const routeError = await doSummary(doErrorResponse(refusal)).catch(
      (error: unknown) => error,
    );
    const router = routerWith(() => Promise.reject(routeError));

    // #when
    const response = await router(
      authedPost({ channel: 'run', runId: RUN_ID, workflowId: 'wf' }),
    );

    // #then
    expect(response?.status).toBe(503);
    const body = await response?.text();
    expect(JSON.parse(body ?? '')).toEqual({
      error: 'execution fence state is not readable',
      reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
    });
    expect(body).not.toContain('ECONNREFUSED');
    expect(body).not.toContain('acme-prod');
    expect(body).not.toContain('10.0.7.4');
  });

  it('still collapses a 5xx with NO structured reason', async () => {
    // #given — the passthrough is narrow on purpose (see run-route-error.ts).
    const router = routerWith(() =>
      Promise.reject(
        new RunRouteError(502, 'upstream exploded with connection details'),
      ),
    );

    // #then
    const response = await router(
      authedPost({ channel: 'run', runId: RUN_ID, workflowId: 'wf' }),
    );
    expect(response?.status).toBe(500);
    expect(await response?.json()).toEqual({
      error: 'upstream exploded with connection details',
    });
  });
});
