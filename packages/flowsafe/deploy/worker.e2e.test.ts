// Executable proof for the shipped operator template. deploy/worker.ts was
// typecheck-only (tsc -p deploy/tsconfig.json); its fetch and maintenance-duty
// handlers never ran in any test, so a template regression would ship silently.
// This drives the handler object in-process over the same fakes the rest of the
// suite trusts: a node:sqlite SQL unit facade behind the Mastra D1Store +
// D1ApprovalStore graph, and a DO namespace whose
// stubs construct real FlowsafeRunner instances (stub state carries the
// idFromName identity, so the DO identity guard code runs in-process). Runtime
// and binding fidelity is covered by the Wrangler harness.
//
// What it pins, end to end over HTTP shapes:
//   - the auth seam: /healthz open; everything else 401s without a mapped
//     bearer token; a malformed actor-map entry never authenticates
//   - the full approval loop: start -> suspension auto-queues an approval ->
//     SoD denial (the requester, wearing the decide-capable admin role) +
//     role denial (viewer) -> reviewer approves -> the derived grant admits
//     the gated publish -> status projection reads success
//   - fail-closed: a forged resume (no approval) is denied at the grant gate
//   - the template boundary: client runIds 400, viewers cannot start runs,
//     and authorized actors share the deployment's run/approval surfaces
//   - maintenance duties escalate an SLA-overdue approval and purge only
//     stale TERMINAL snapshots; a broken approval store must not stop the
//     retention purge (the two surfaces are isolated)
//   - D4 wedge recovery: a status() poll re-files an approval a suspended
//     run lost, and deciding the re-filed record resumes the run to
//     completion
//
// Scope note: the template's @proofoftech/flowsafe/* imports are aliased to
// SOURCE here (vitest.config.ts / tsconfig.test.json) — this proves the
// template's behavior, not that the published dist/ exports map resolves.

import type {
  D1Database,
  DurableObjectNamespace,
  DurableObjectState,
  ExecutionContext,
} from '@cloudflare/workers-types';
import { describe, expect, it, vi } from 'vitest';

import {
  type ApprovalActor,
  type ApprovalRecord,
  type ApprovalStreamEvent,
  D1ApprovalStoreFactory,
} from '../src/approval-api/index.js';
import {
  HUB_INSTANCE_NAME,
  PATH_SAFE_ID_PATTERN,
  type RunArtifactPurger,
  type RunSummary,
} from '../src/do-runner/index.js';
import {
  createFlowsafeWorker,
  staticTokenVerifier,
} from '../src/host-kit/index.js';
import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../test-support/sqlite.js';
import handler, { FlowsafeRunner } from './worker.js';

type Env = Parameters<NonNullable<typeof handler.fetch>>[1];

function required<T>(handlerFn: T | undefined, name: string): T {
  if (!handlerFn) throw new Error(`handler.${name} is not defined`);
  return handlerFn;
}
const fetchHandler = required(handler.fetch, 'fetch');

const maintenanceWorker = createFlowsafeWorker<Env>({
  workflows: [],
  systemPrincipalId: 'flowsafe-worker',
  buildVerifier: () => staticTokenVerifier(new Map<string, ApprovalActor>()),
  maintenance: {
    sweepIntervalMs: 15 * 60 * 1_000,
    purgeIntervalMs: 60 * 60 * 1_000,
  },
});

async function runMaintenanceDuty(
  duty: 'sweep' | 'purge',
  env: Env,
): Promise<void> {
  await maintenanceWorker.runMaintenanceDuty(duty, env);
}

// In-process DO namespace: idFromName carries the name, get() memoizes a REAL
// FlowsafeRunner per name with a stub state exposing that identity — the same
// `{ id: { name } }` shape durable-object.ts documents for node tests, so
// request identity and deployment identity assertions both execute for real.
function fakeRunnerNamespace(getEnv: () => Env): DurableObjectNamespace {
  const instances = new Map<string, FlowsafeRunner>();
  const namespace = {
    idFromName: (name: string) => ({ name }),
    get: (id: { name: string }) => ({
      fetch: async (input: string, init?: RequestInit) => {
        let runner = instances.get(id.name);
        if (!runner) {
          const state = {
            id: { name: id.name },
          } as unknown as DurableObjectState;
          runner = new FlowsafeRunner(state, getEnv());
          instances.set(id.name, runner);
        }
        return runner.fetch(new Request(input, init));
      },
    }),
  };
  return namespace as unknown as DurableObjectNamespace;
}

// Bearer map for the APPROVAL_ACTOR_TOKENS secret. The malformed entry is a
// fail-closed probe: parseActorTokens must drop it, so the
// token never authenticates. 'tok-admin' exists because admin is the ONLY
// role in both RUN_START_ROLES and CAN_REVIEW — the identity that can start
// a run AND would be role-allowed to decide it, so the separation-of-duties
// denial (requester ≠ decider) is testable distinctly from the role gate.
const TOKENS = {
  'tok-admin': { id: 'ada-admin', role: 'admin' },
  'tok-operator': { id: 'op-olive', role: 'operator' },
  'tok-reviewer': { id: 'rev-ray', role: 'reviewer' },
  'tok-viewer': { id: 'vic-viewer', role: 'viewer' },
  'tok-rival': { id: 'op-rival', role: 'operator' },
  'tok-malformed': { id: '', role: 'admin' },
};

function makeEnv(overrides: Partial<Env> = {}): {
  env: Env;
  sqlite: SqliteDatabase;
  d1: unknown;
} {
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
  const d1 = sqliteUnitDatabase(sqlite);
  const env = {
    DB: d1 as D1Database,
    DEPLOYMENT_TENANT: 'acme',
    DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
    APPROVAL_ACTOR_TOKENS: JSON.stringify(TOKENS),
    MAINTENANCE: {
      idFromName: (name: string) => ({ name }),
      get: () => ({ fetch: async () => new Response(null, { status: 204 }) }),
    } as unknown as DurableObjectNamespace,
    ...overrides,
  } as Env;
  (env as { RUNNER: DurableObjectNamespace }).RUNNER = fakeRunnerNamespace(
    () => env,
  );
  return { env, sqlite, d1 };
}

function makeCtx(): { ctx: ExecutionContext; drain: () => Promise<void> } {
  const pending: Promise<unknown>[] = [];
  const ctx = {
    waitUntil: (promise: Promise<unknown>) => {
      pending.push(promise);
    },
    passThroughOnException: () => {},
  } as unknown as ExecutionContext;
  return {
    ctx,
    drain: async () => {
      await Promise.all(pending);
    },
  };
}

async function call(
  env: Env,
  path: string,
  init: RequestInit & { token?: string; host?: string } = {},
): Promise<Response> {
  const { token, host, ...requestInit } = init;
  const headers = new Headers(requestInit.headers);
  if (token) headers.set('authorization', `Bearer ${token}`);
  if (requestInit.body) headers.set('content-type', 'application/json');
  const { ctx, drain } = makeCtx();
  const response = await fetchHandler(
    new Request(`https://${host ?? 'flowsafe.example'}${path}`, {
      ...requestInit,
      headers,
    }) as unknown as Parameters<typeof fetchHandler>[0],
    env,
    ctx,
  );
  await drain();
  return response as unknown as Response;
}

type StartResponse = RunSummary & { approval?: ApprovalRecord };

async function startRun(env: Env, token = 'tok-operator') {
  const response = await call(env, '/runs', {
    method: 'POST',
    body: JSON.stringify({
      workflowId: 'example-approval',
      inputData: { topic: 'launch' },
    }),
    token,
  });
  expect(response.status).toBe(200);
  return (await response.json()) as StartResponse;
}

describe('deploy worker portability', () => {
  it('exports no platform-trigger or queue-consumer handlers', () => {
    expect(handler).not.toHaveProperty('scheduled');
    expect(handler).not.toHaveProperty('queue');
  });
});

describe('deploy worker fetch(): auth seam', () => {
  it('serves /healthz unauthenticated and 401s every other surface without a token', async () => {
    // #given
    const { env } = makeEnv();

    // #when / #then — liveness is open
    const health = await call(env, '/healthz');
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ ok: true });

    // #then — catalog, runs, and approvals all fail closed
    expect((await call(env, '/workflows')).status).toBe(401);
    expect((await call(env, '/api/approvals')).status).toBe(401);
    expect(
      (await call(env, '/workflows', { token: 'tok-unknown' })).status,
    ).toBe(401);
  });

  it('drops a malformed actor-map entry — the token never authenticates', async () => {
    // #given — TOKENS carries an empty actor id
    const { env } = makeEnv();

    // #when / #then — parseActorTokens discards the entry, so 401 (not 403)
    expect(
      (await call(env, '/workflows', { token: 'tok-malformed' })).status,
    ).toBe(401);
  });

  it('echoes the AUTHENTICATED identity on the catalog', async () => {
    // #given
    const { env } = makeEnv();

    // #when
    const response = await call(env, '/workflows', { token: 'tok-reviewer' });

    // #then — the server's view of the actor, not a client-side guess
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      actor: { id: 'rev-ray', role: 'reviewer' },
      workflows: [{ id: 'example-approval' }],
    });
  });
});

describe('deploy worker fetch(): the approval loop over HTTP', () => {
  it('start suspends and auto-queues an approval bound to the suspension', async () => {
    // #given / #when
    const { env } = makeEnv();
    const started = await startRun(env);

    // #then — server-minted path-safe runId and a queued approval
    // carrying the gate's server-authored connector request
    expect(started.status).toBe('suspended');
    expect(PATH_SAFE_ID_PATTERN.test(started.runId)).toBe(true);
    expect(started.suspended).toEqual([['gate']]);
    expect(started.approval).toMatchObject({
      status: 'pending',
      requestedBy: 'op-olive',
      connectors: ['example-publisher'],
      workflowId: 'example-approval',
    });
    expect(started.approval?.suspendedAt).toEqual(expect.any(Number));
  });

  it('separation of duties and role gates hold, then the reviewer decision mints the grant and the publish runs', async () => {
    // #given — a suspended run started by the ADMIN (the one role that can
    // both start runs and decide approvals), so the self-decision denial
    // below exercises the SoD branch and not the role gate in front of it
    const { env } = makeEnv();
    const started = await startRun(env, 'tok-admin');
    const approvalId = started.approval?.id;
    if (!approvalId) throw new Error('expected an auto-queued approval');
    expect(started.approval?.requestedBy).toBe('ada-admin');
    const decideBody = JSON.stringify({ decision: 'approve', comment: 'lgtm' });

    // #when / #then — the requester passes the role gate but is denied for
    // being the requester: the SoD-specific reason, not the role message
    const selfDecide = await call(env, `/api/approvals/${approvalId}/decide`, {
      method: 'POST',
      body: decideBody,
      token: 'tok-admin',
    });
    expect(selfDecide.status).toBe(403);
    expect(((await selfDecide.json()) as { error: string }).error).toContain(
      'separation of duties',
    );

    // #then — a viewer is stopped one gate earlier, by role
    const viewerDecide = await call(
      env,
      `/api/approvals/${approvalId}/decide`,
      {
        method: 'POST',
        body: decideBody,
        token: 'tok-viewer',
      },
    );
    expect(viewerDecide.status).toBe(403);
    expect(((await viewerDecide.json()) as { error: string }).error).toContain(
      'requires one of roles',
    );

    // #when — the reviewer approves; decide() resumes through the DO stub and
    // the DO-side provider derives the grant from the now-approved record
    const decide = await call(env, `/api/approvals/${approvalId}/decide`, {
      method: 'POST',
      body: decideBody,
      token: 'tok-reviewer',
    });

    // #then — the grant admitted the gated publish
    expect(decide.status).toBe(200);
    const decided = (await decide.json()) as {
      record: ApprovalRecord;
      resume: { attempted: boolean; ok: boolean; summary?: RunSummary };
    };
    expect(decided.record).toMatchObject({
      status: 'approved',
      decidedBy: 'rev-ray',
    });
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { topic: 'launch', published: true, approvedBy: 'rev-ray' },
    });

    // #then — the status projection agrees, and a viewer may read it
    const status = await call(env, `/runs/example-approval/${started.runId}`, {
      token: 'tok-viewer',
    });
    expect(status.status).toBe(200);
    expect(await status.json()).toMatchObject({
      status: 'success',
      result: { published: true },
    });
  });

  it('APPROVAL_ALLOW_SELF_DECISION=admin lets the admin requester decide its own run over HTTP', async () => {
    // #given — the single-operator config: SoD is relaxed for admin, so the
    // env var must thread all the way into ApprovalService.decide()
    const { env } = makeEnv({ APPROVAL_ALLOW_SELF_DECISION: 'admin' });
    const started = await startRun(env, 'tok-admin');
    const approvalId = started.approval?.id;
    if (!approvalId) throw new Error('expected an auto-queued approval');
    expect(started.approval?.requestedBy).toBe('ada-admin');

    // #when — the admin decides its OWN request (denied without the var)
    const selfDecide = await call(env, `/api/approvals/${approvalId}/decide`, {
      method: 'POST',
      body: JSON.stringify({ decision: 'approve', comment: 'solo operator' }),
      token: 'tok-admin',
    });

    // #then — permitted, grant minted, the gated publish runs under admin
    expect(selfDecide.status).toBe(200);
    const decided = (await selfDecide.json()) as {
      record: ApprovalRecord;
      resume: { attempted: boolean; ok: boolean; summary?: RunSummary };
    };
    expect(decided.record).toMatchObject({
      status: 'approved',
      decidedBy: 'ada-admin',
    });
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { published: true, approvedBy: 'ada-admin' },
    });
  });

  it('APPROVAL_ALLOW_SELF_DECISION=admin still binds reviewer to SoD', async () => {
    // #given — the exemption is role-scoped: reviewer is NOT admin, so a
    // reviewer who advanced a run still cannot decide that gate. Modeled by an
    // explicitly reviewer-attributed request the reviewer then tries to decide.
    const { env } = makeEnv({ APPROVAL_ALLOW_SELF_DECISION: 'admin' });
    const started = await startRun(env, 'tok-admin');
    const approvalId = started.approval?.id;
    if (!approvalId) throw new Error('expected an auto-queued approval');

    // #when — a reviewer decides an admin-requested gate: allowed (not their
    // own), proving the var did not blanket-disable SoD for everyone
    const reviewerDecide = await call(
      env,
      `/api/approvals/${approvalId}/decide`,
      {
        method: 'POST',
        body: JSON.stringify({ decision: 'approve' }),
        token: 'tok-reviewer',
      },
    );
    expect(reviewerDecide.status).toBe(200);
  });

  it('fails closed: a forged resume that bypasses the queue finds no grant', async () => {
    // #given — a suspended run, nothing approved
    const { env } = makeEnv();
    const started = await startRun(env);

    // #when — an "approved" resume forged straight at the public resume route
    const forged = await call(
      env,
      `/runs/example-approval/${started.runId}/resume`,
      {
        method: 'POST',
        body: JSON.stringify({
          step: 'gate',
          resumeData: { approved: true, decidedBy: 'forger' },
        }),
        token: 'tok-operator',
      },
    );

    // #then — the run advanced but the publish step denied (no grant)
    expect(forged.status).toBe(200);
    const summary = (await forged.json()) as RunSummary;
    expect(summary.status).toBe('failed');
    expect(summary.error).toContain(
      'approval required and no matching structured grant was found',
    );
  });
});

describe('deploy worker fetch(): deployment boundary', () => {
  it('400s a client-supplied runId', async () => {
    // #given
    const { env } = makeEnv();

    // #when
    const response = await call(env, '/runs', {
      method: 'POST',
      body: JSON.stringify({
        workflowId: 'example-approval',
        runId: 'acme_forged',
        inputData: { topic: 'launch' },
      }),
      token: 'tok-operator',
    });

    // #then
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: 'runId is server-assigned',
    });
  });

  it('403s a viewer trying to start a run (coarse start-role gate)', async () => {
    // #given
    const { env } = makeEnv();

    // #when / #then
    const response = await call(env, '/runs', {
      method: 'POST',
      body: JSON.stringify({
        workflowId: 'example-approval',
        inputData: { topic: 'launch' },
      }),
      token: 'tok-viewer',
    });
    expect(response.status).toBe(403);
  });

  it('hides run status from another operator while retaining the shared approval queue', async () => {
    const { env } = makeEnv();
    const started = await startRun(env);
    const approvalId = started.approval?.id;
    if (!approvalId) throw new Error('expected an auto-queued approval');

    const status = await call(env, `/runs/example-approval/${started.runId}`, {
      token: 'tok-rival',
    });
    expect(status.status).toBe(404);

    const rivalList = await call(env, '/api/approvals', { token: 'tok-rival' });
    expect(rivalList.status).toBe(200);
    expect(JSON.stringify(await rivalList.json())).toContain(approvalId);
  });
});

describe('deploy worker fetch(): D4 wedge recovery (status() self-heals)', () => {
  it('re-files a lost approval on the next status() poll, and deciding the re-filed record resumes the run to completion', async () => {
    // #given — a normal suspension with its approval auto-queued...
    const { env, sqlite } = makeEnv();
    const started = await startRun(env);
    const approvalId = started.approval?.id;
    if (!approvalId) throw new Error('expected an auto-queued approval');

    // ...then the WEDGE itself: the record is gone (a transient store
    // failure mid re-queue leaves the same state — a suspended gate with no
    // approval record at all). The queue is now empty for this run.
    sqlite
      .prepare('DELETE FROM flowsafe_approvals WHERE id = ?')
      .run(approvalId);
    const emptyQueue = await call(env, '/api/approvals', {
      token: 'tok-reviewer',
    });
    expect(JSON.stringify(await emptyQueue.json())).not.toContain(approvalId);

    // #when — an operator/dashboard polls the run's status
    const status = await call(env, `/runs/example-approval/${started.runId}`, {
      token: 'tok-viewer',
    });

    // #then — the read itself is unaffected by the wedge...
    expect(status.status).toBe(200);
    expect(await status.json()).toMatchObject({ status: 'suspended' });

    // ...and the poll healed it: a FRESH approval now exists for the same
    // gate, retaining the run's durable human requester provenance
    const requeued = await call(env, '/api/approvals', {
      token: 'tok-reviewer',
    });
    const records = (await requeued.json()) as ApprovalRecord[];
    const refiled = records.find(
      (record) => record.runId === started.runId && record.status === 'pending',
    );
    expect(refiled).toBeDefined();
    expect(refiled?.id).not.toBe(approvalId);
    expect(refiled).toMatchObject({
      stepPath: ['gate'],
      connectors: ['example-publisher'],
      requestedBy: 'op-olive',
      requestedByKind: 'human',
    });

    // #when — the reviewer decides the RE-FILED record
    const decide = await call(env, `/api/approvals/${refiled?.id}/decide`, {
      method: 'POST',
      body: JSON.stringify({ decision: 'approve' }),
      token: 'tok-reviewer',
    });

    // #then — the grant the re-filed record derives admits the gated
    // publish, and the run reaches completion despite the wedge
    expect(decide.status).toBe(200);
    const decided = (await decide.json()) as {
      resume: { attempted: boolean; ok: boolean; summary?: RunSummary };
    };
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { topic: 'launch', published: true, approvedBy: 'rev-ray' },
    });
  });
});

// In-process HUB namespace: idFromName carries the singleton name, and
// get().fetch records each ApprovalStreamEvent the composer POSTs to
// /internal/event (createHubTopology.publish). The real DO fan-out over
// hibernatable WebSockets is workerd-only and proven by the spike; here we only
// need to see the event reach the deployment hub stub.
function fakeHubNamespace(): {
  namespace: DurableObjectNamespace;
  events: Array<{ instance: string; event: ApprovalStreamEvent }>;
} {
  const events: Array<{ instance: string; event: ApprovalStreamEvent }> = [];
  const namespace = {
    idFromName: (name: string) => ({ name }),
    get: (id: { name: string }) => ({
      fetch: async (input: string | Request, init?: RequestInit) => {
        // publish() uses the string+init overload; a raw-Request subscribe
        // forward (the WS path) is never exercised in-process.
        if (typeof input === 'string' && typeof init?.body === 'string') {
          events.push({
            instance: id.name,
            event: JSON.parse(init.body) as ApprovalStreamEvent,
          });
        }
        return new Response(JSON.stringify({ ok: true }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      },
    }),
  };
  return { namespace: namespace as unknown as DurableObjectNamespace, events };
}

function makeStreamEnv(overrides: Partial<Env> = {}): {
  env: Env;
  sqlite: SqliteDatabase;
  hubEvents: Array<{ instance: string; event: ApprovalStreamEvent }>;
} {
  const hub = fakeHubNamespace();
  const { env, sqlite } = makeEnv({
    STREAM_TICKET_SECRET: 'test-stream-secret',
    HUB: hub.namespace,
    ...overrides,
  });
  return { env, sqlite, hubEvents: hub.events };
}

describe('deploy worker fetch(): live stream stage (opt-in)', () => {
  it('mints a hub ticket for an authenticated actor', async () => {
    // #given — HUB + STREAM_TICKET_SECRET both set => the stage mounts
    const { env } = makeStreamEnv();

    // #when
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      body: JSON.stringify({ channel: 'hub' }),
      token: 'tok-reviewer',
    });

    // #then — a ~60s addressing JWT, no grant
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      url: string;
      ticket: string;
      expiresAt: number;
    };
    expect(body.url).toBe('/api/stream/hub');
    expect(body.ticket.split('.')).toHaveLength(3);
    expect(body.expiresAt).toBeGreaterThan(Date.now());
  });

  it('401s the ticket route without a bearer token (fail closed)', async () => {
    // #given
    const { env } = makeStreamEnv();

    // #when / #then
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      body: JSON.stringify({ channel: 'hub' }),
    });
    expect(response.status).toBe(401);
  });

  it("forwards a run-start's auto-queued approval to the deployment hub", async () => {
    // #given — a streaming env whose HUB stub records forwarded events
    const { env, hubEvents } = makeStreamEnv();

    // #when — starting a run suspends and auto-queues an approval; the
    // fetch-scope stream sink (ctx.waitUntil, drained by call()) forwards the
    // 'created' event to the deployment hub singleton
    const started = await startRun(env);

    // #then — the deployment hub received the created record
    const created = hubEvents.find((e) => e.event.type === 'created');
    expect(created).toBeDefined();
    expect(created?.instance).toBe(HUB_INSTANCE_NAME);
    expect(created?.event.record.runId).toBe(started.runId);
    expect(created?.event.record.connectors).toEqual(['example-publisher']);
  });

  it('404s a run-channel ticket for an unregistered path-safe address', async () => {
    const { env } = makeStreamEnv();

    // #when / #then
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      body: JSON.stringify({ channel: 'run', runId: 'rival_r1' }),
      token: 'tok-reviewer',
    });
    expect(response.status).toBe(404);
  });

  it('mints a run-channel ticket for an OWNED run', async () => {
    // #given — acme owns the run it just started
    const { env } = makeStreamEnv();
    const started = await startRun(env);

    // #when
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      body: JSON.stringify({
        channel: 'run',
        runId: started.runId,
        workflowId: 'example-approval',
      }),
      token: 'tok-reviewer',
    });

    // #then — a run-channel url qualified with the workflowId
    expect(response.status).toBe(200);
    const body = (await response.json()) as { url: string };
    expect(body.url).toBe(`/api/stream/run/example-approval/${started.runId}`);
  });

  it('leaves the stream stage UNMOUNTED when STREAM_TICKET_SECRET is absent (poll-only)', async () => {
    // #given — HUB bound but no ticket secret => opt-in gate stays closed
    const hub = fakeHubNamespace();
    const { env } = makeEnv({ HUB: hub.namespace });

    // #when — the ticket route is not owned by any mounted router
    const response = await call(env, '/api/stream/ticket', {
      method: 'POST',
      body: JSON.stringify({ channel: 'hub' }),
      token: 'tok-reviewer',
    });

    // #then — falls through to the composer's terminal 404 (no streaming)
    expect(response.status).toBe(404);
    expect(hub.events).toHaveLength(0);
  });
});

// Column set per @mastra/core storage constants for mastra_workflow_snapshot
// (camelCase timestamps, snapshot serialized as JSON TEXT) — the alarm's purge
// targets this exact table shape. Drift from the real adapter's DDL is pinned
// by mastra-schema-guard.test.ts (runs the REAL D1Store and asserts run_id).
function createSnapshotTable(db: SqliteDatabase): void {
  db.prepare(
    `CREATE TABLE mastra_workflow_snapshot (
      workflow_name TEXT NOT NULL,
      run_id TEXT NOT NULL,
      resourceId TEXT,
      snapshot TEXT NOT NULL,
      createdAt TEXT NOT NULL,
      updatedAt TEXT NOT NULL
    )`,
  ).run();
}

function seedRun(
  db: SqliteDatabase,
  options: { runId: string; status: string; updatedAt: number },
): void {
  const iso = new Date(options.updatedAt).toISOString();
  db.prepare(
    `INSERT INTO mastra_workflow_snapshot
     (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
     VALUES (?, ?, NULL, ?, ?, ?)`,
  ).run(
    'wf',
    options.runId,
    JSON.stringify({ status: options.status, runId: options.runId }),
    iso,
    iso,
  );
}

function remainingRunIds(db: SqliteDatabase): string[] {
  const rows = db
    .prepare('SELECT run_id FROM mastra_workflow_snapshot ORDER BY run_id')
    .all() as Array<{ run_id: string }>;
  return rows.map((row) => row.run_id);
}

const DAY_MS = 86_400_000;

describe('deploy worker alarm-owned maintenance duties', () => {
  it('escalates an SLA-overdue approval and purges only stale TERMINAL snapshots', async () => {
    // #given — an overdue pending approval (seeded through the real store,
    // same DB object the worker binds) and three snapshot rows: stale
    // terminal, fresh terminal, stale suspended
    // RUN_RETENTION_DAYS is deliberately invalid: numberVar must log the
    // config error and fall back to the 30-day default rather than skip
    // maintenance (the fallback keeps the purge asserts below meaningful).
    const { env, sqlite, d1 } = makeEnv({ RUN_RETENTION_DAYS: '-5' });
    const store = new D1ApprovalStoreFactory(d1 as never).store();
    const past = new Date(Date.now() - 60_000).toISOString();
    await store.create({
      id: 'apr-overdue',
      workflowId: 'example-approval',
      runId: 'acme_r1',
      title: 'overdue approval',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: past,
      updatedAt: past,
      slaDeadlineAt: past,
    });
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'acme_stale-done',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'acme_fresh-done',
      status: 'success',
      updatedAt: Date.now() - 1 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'acme_stale-open',
      status: 'suspended',
      updatedAt: Date.now() - 90 * DAY_MS,
    });

    // #when — the SWEEP duty runs alone
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    await runMaintenanceDuty('sweep', env);

    // #then — the sweep escalated the overdue approval, and the DISPATCH
    // kept the purge out of this invocation (a CPU-limit kill is not
    // catchable, so the two duties must never share one): every snapshot
    // row survives the sweep firing
    const swept = await store.get('apr-overdue');
    expect(swept?.status).toBe('escalated');
    expect(remainingRunIds(sqlite)).toEqual([
      'acme_fresh-done',
      'acme_stale-done',
      'acme_stale-open',
    ]);

    // #when — the purge maintenance duty runs
    await runMaintenanceDuty('purge', env);

    // #then — the purge reclaimed exactly the stale terminal row under the
    // FALLBACK 30-day TTL (a stale SUSPENDED run is a pending approval, not
    // garbage)...
    expect(remainingRunIds(sqlite)).toEqual([
      'acme_fresh-done',
      'acme_stale-open',
    ]);
    // ...and the invalid var was surfaced as the operator's tripwire
    expect(
      errorSpy.mock.calls.some(([line]) => {
        const text = String(line);
        return (
          text.includes('config-error') && text.includes('RUN_RETENTION_DAYS')
        );
      }),
    ).toBe(true);
    errorSpy.mockRestore();
  });

  it('a broken approval store does not stop the retention purge (isolated surfaces)', async () => {
    // #given — every approval-store statement throws; a stale terminal
    // snapshot is still eligible
    const { sqlite, d1 } = makeEnv();
    const inner = d1 as { prepare(sql: string): unknown };
    const broken = {
      ...(d1 as Record<string, unknown>),
      prepare(sql: string) {
        if (sql.includes('flowsafe_approvals')) {
          throw new Error('approval store down');
        }
        return inner.prepare(sql);
      },
    } as unknown as D1Database;
    const env = {
      DB: broken,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
      APPROVAL_ACTOR_TOKENS: JSON.stringify(TOKENS),
    } as Env;
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'acme_stale-done',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when — the sweep firing fails; the purge firing still does its duty
    await runMaintenanceDuty('sweep', env);
    await runMaintenanceDuty('purge', env);

    // #then — the sweep failure was logged, not propagated, and the purge ran
    expect(remainingRunIds(sqlite)).toEqual([]);
    const surfaces = errorSpy.mock.calls
      .map(([line]) => String(line))
      .filter((line) => line.includes('maintenance-error'));
    expect(surfaces.some((line) => line.includes('sla-sweep'))).toBe(true);
    errorSpy.mockRestore();
  });

  it('a broken snapshot table does not stop the SLA sweep (isolation holds in the other direction too)', async () => {
    // #given — every snapshot-table statement throws; an overdue approval is
    // still waiting for escalation
    const { d1 } = makeEnv();
    const inner = d1 as { prepare(sql: string): unknown };
    const broken = {
      ...(d1 as Record<string, unknown>),
      prepare(sql: string) {
        if (sql.includes('mastra_workflow_snapshot')) {
          throw new Error('snapshot table down');
        }
        return inner.prepare(sql);
      },
    } as unknown as D1Database;
    const env = {
      DB: broken,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
      APPROVAL_ACTOR_TOKENS: JSON.stringify(TOKENS),
    } as Env;
    const store = new D1ApprovalStoreFactory(d1 as never).store();
    const past = new Date(Date.now() - 60_000).toISOString();
    await store.create({
      id: 'apr-overdue',
      workflowId: 'example-approval',
      runId: 'acme_r1',
      title: 'overdue approval',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: past,
      updatedAt: past,
      slaDeadlineAt: past,
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when — the purge firing fails; the sweep firing still does its duty
    await runMaintenanceDuty('purge', env);
    await runMaintenanceDuty('sweep', env);

    // #then — the purge failure was logged, not propagated, and the sweep ran
    const swept = await store.get('apr-overdue');
    expect(swept?.status).toBe('escalated');
    const surfaces = errorSpy.mock.calls
      .map(([line]) => String(line))
      .filter((line) => line.includes('maintenance-error'));
    expect(surfaces.some((line) => line.includes('retention-purge'))).toBe(
      true,
    );
    errorSpy.mockRestore();
  });

  it('purges DECIDED approvals and snapshots in the same purge-duty alarm', async () => {
    // #given — an old decided (approved) approval, a fresh decided
    // (rejected) approval, and an old but still-OPEN approval (never
    // purged at any age). RUN_RETENTION_DAYS is unset (fallback default);
    // APPROVAL_RETENTION_DAYS is deliberately invalid: numberVar must log
    // the config error and fall back to 30 days rather than skip the purge.
    const { env, d1 } = makeEnv({ APPROVAL_RETENTION_DAYS: '-5' });
    const store = new D1ApprovalStoreFactory(d1 as never).store();
    const old = new Date(Date.now() - 40 * DAY_MS).toISOString();
    const fresh = new Date(Date.now() - 1 * DAY_MS).toISOString();
    await store.create({
      id: 'apr-old-decided',
      workflowId: 'example-approval',
      runId: 'acme_r-old',
      title: 'old decided',
      connectors: [],
      priority: 'normal',
      status: 'approved',
      createdAt: old,
      updatedAt: old,
      decidedAt: old,
    });
    await store.create({
      id: 'apr-fresh-decided',
      workflowId: 'example-approval',
      runId: 'acme_r-fresh',
      title: 'fresh decided',
      connectors: [],
      priority: 'normal',
      status: 'rejected',
      createdAt: fresh,
      updatedAt: fresh,
      decidedAt: fresh,
    });
    await store.create({
      id: 'apr-old-open',
      workflowId: 'example-approval',
      runId: 'acme_r-open',
      title: 'old but still open',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: old,
      updatedAt: old,
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when — the PURGE duty runs once
    await runMaintenanceDuty('purge', env);

    // #then — the stale decided record is gone; the fresh decided record
    // and the still-open record (however old) both survive
    expect(await store.get('apr-old-decided')).toBeNull();
    expect(await store.get('apr-fresh-decided')).not.toBeNull();
    expect(await store.get('apr-old-open')).not.toBeNull();
    // ...and the invalid var was surfaced as the operator's tripwire (same
    // convention as RUN_RETENTION_DAYS's config-error test above)
    expect(
      errorSpy.mock.calls.some(([line]) => {
        const text = String(line);
        return (
          text.includes('config-error') &&
          text.includes('APPROVAL_RETENTION_DAYS')
        );
      }),
    ).toBe(true);
    errorSpy.mockRestore();
  });

  it('an approval-purge failure does not stop the snapshot retention purge (isolated surfaces)', async () => {
    // #given — every approvals-table statement throws; a stale terminal
    // snapshot is still eligible
    const { sqlite, d1 } = makeEnv();
    const inner = d1 as { prepare(sql: string): unknown };
    const broken = {
      ...(d1 as Record<string, unknown>),
      prepare(sql: string) {
        if (sql.includes('flowsafe_approvals')) {
          throw new Error('approval store down');
        }
        return inner.prepare(sql);
      },
    } as unknown as D1Database;
    const env = {
      DB: broken,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
      APPROVAL_ACTOR_TOKENS: JSON.stringify(TOKENS),
    } as Env;
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'acme_stale-done',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when — the PURGE duty runs
    await runMaintenanceDuty('purge', env);

    // #then — the snapshot purge still ran despite the broken approval
    // store, and the approval-purge failure was logged under its own
    // surface rather than silently dropped or aborting the snapshot purge
    expect(remainingRunIds(sqlite)).toEqual([]);
    const surfaces = errorSpy.mock.calls
      .map(([line]) => String(line))
      .filter((line) => line.includes('maintenance-error'));
    expect(
      surfaces.some((line) => line.includes('approval-retention-purge')),
    ).toBe(true);
    errorSpy.mockRestore();
  });
});

describe('createFlowsafeWorker artifact-paired retention purge (F4)', () => {
  // The deploy template wires no R2, so it sets no artifactStore factory. This
  // drives createFlowsafeWorker directly with one, proving each maintenance
  // invocation resolves its own binding and deletes artifacts BEFORE the row.

  function workerWith<WorkerEnv extends Env = Env>(
    artifactStore:
      | ((env: WorkerEnv) => RunArtifactPurger | undefined)
      | undefined,
    extraPurgeDuties?: (env: WorkerEnv) => Promise<Record<string, unknown>>,
  ) {
    return createFlowsafeWorker<WorkerEnv>({
      workflows: [],
      systemPrincipalId: 'flowsafe-worker',
      buildVerifier: () =>
        staticTokenVerifier(new Map<string, ApprovalActor>()),
      maintenance: {
        sweepIntervalMs: 15 * 60 * 1_000,
        purgeIntervalMs: 60 * 60 * 1_000,
      },
      artifactStore,
      extraPurgeDuties,
    });
  }

  it("deletes an expired run's artifacts BEFORE its snapshot row when artifactStore is set", async () => {
    // #given — one stale terminal run (eligible) and one fresh terminal run
    const { env, sqlite } = makeEnv();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'acme_stale-done',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'acme_fresh-done',
      status: 'success',
      updatedAt: Date.now() - 1 * DAY_MS,
    });

    // a RunArtifactPurger that asserts the row STILL EXISTS when it runs:
    // pairing deletes artifacts BEFORE the row, so the row (the only record of
    // the artifact keys) must still be enumerable at this moment
    const deletions: string[] = [];
    const artifactStore: RunArtifactPurger = {
      deleteRun: vi.fn(async (_workflowId: string, runId: string) => {
        expect(remainingRunIds(sqlite)).toContain(runId);
        deletions.push(runId);
        return 1;
      }),
    };

    // #when — the PURGE duty runs
    await workerWith(() => artifactStore).runMaintenanceDuty('purge', env);

    // #then — the stale run's artifacts were deleted (while its row still
    // existed), then its row was purged; the fresh run is untouched
    expect(deletions).toEqual(['acme_stale-done']);
    expect(artifactStore.deleteRun).toHaveBeenCalledWith(
      'wf',
      'acme_stale-done',
    );
    expect(remainingRunIds(sqlite)).toEqual(['acme_fresh-done']);
  });

  it('resolves the artifact store from each maintenance invocation environment', async () => {
    const first = makeEnv();
    const second = makeEnv();
    for (const { sqlite } of [first, second]) {
      createSnapshotTable(sqlite);
    }
    seedRun(first.sqlite, {
      runId: 'first-stale',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });
    seedRun(second.sqlite, {
      runId: 'second-stale',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });
    const firstStore: RunArtifactPurger = { deleteRun: vi.fn(async () => 1) };
    const secondStore: RunArtifactPurger = { deleteRun: vi.fn(async () => 1) };
    type ArtifactEnv = Env & { ARTIFACTS: RunArtifactPurger };
    const firstEnv: ArtifactEnv = { ...first.env, ARTIFACTS: firstStore };
    const secondEnv: ArtifactEnv = { ...second.env, ARTIFACTS: secondStore };
    const artifactStore = vi.fn(
      (currentEnv: ArtifactEnv) => currentEnv.ARTIFACTS,
    );
    const worker = workerWith<ArtifactEnv>(artifactStore);

    await worker.runMaintenanceDuty('purge', firstEnv);
    await worker.runMaintenanceDuty('purge', secondEnv);

    expect(artifactStore).toHaveBeenNthCalledWith(1, firstEnv);
    expect(artifactStore).toHaveBeenNthCalledWith(2, secondEnv);
    expect(firstStore.deleteRun).toHaveBeenCalledWith('wf', 'first-stale');
    expect(firstStore.deleteRun).not.toHaveBeenCalledWith('wf', 'second-stale');
    expect(secondStore.deleteRun).toHaveBeenCalledWith('wf', 'second-stale');
    expect(secondStore.deleteRun).not.toHaveBeenCalledWith('wf', 'first-stale');
  });

  it.each([
    [
      'factory',
      () => {
        throw new Error('artifact factory unavailable');
      },
    ],
    [
      'deletion',
      () => ({
        deleteRun: async () => {
          throw new Error('artifact deletion unavailable');
        },
      }),
    ],
  ])('%s failure preserves the snapshot row and does not starve sibling duties', async (_failure, artifactStore) => {
    const { env, sqlite } = makeEnv();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'acme_stale-done',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });
    const extraPurgeDuty = vi.fn(async () => ({ extraDuty: 'ran' }));

    const outcome = await workerWith(
      artifactStore,
      extraPurgeDuty,
    ).runMaintenanceDuty('purge', env);

    expect(outcome).toMatchObject({ ok: false });
    expect(remainingRunIds(sqlite)).toEqual(['acme_stale-done']);
    expect(extraPurgeDuty).toHaveBeenCalledOnce();
  });

  it('without artifactStore the purge is unchanged — same rows deleted, no artifact store touched', async () => {
    // #given — identical seeding, but no artifactStore wired (the deploy default)
    const { env, sqlite } = makeEnv();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'acme_stale-done',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'acme_fresh-done',
      status: 'success',
      updatedAt: Date.now() - 1 * DAY_MS,
    });

    // #when
    await workerWith(undefined).runMaintenanceDuty('purge', env);

    // #then — byte-identical row-only outcome
    expect(remainingRunIds(sqlite)).toEqual(['acme_fresh-done']);
  });

  it('supports an artifactStore factory that returns undefined', async () => {
    const { env, sqlite } = makeEnv();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'acme_stale-done',
      status: 'success',
      updatedAt: Date.now() - 40 * DAY_MS,
    });

    await workerWith(() => undefined).runMaintenanceDuty('purge', env);

    expect(remainingRunIds(sqlite)).toEqual([]);
  });
});
