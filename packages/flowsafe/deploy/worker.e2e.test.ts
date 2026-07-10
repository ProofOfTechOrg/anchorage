// Executable proof for the shipped operator template. deploy/worker.ts was
// typecheck-only (tsc -p deploy/tsconfig.json); its fetch()/scheduled()/queue()
// handlers never ran in any test, so a template regression would ship silently.
// This drives the REAL handler object in-process over the same fakes the rest
// of the suite trusts: node:sqlite behind a D1-shaped adapter (real Mastra
// D1Store + real D1ApprovalStore over real SQLite) and a DO namespace whose
// stubs construct real FlowsafeRunner instances (stub state carries the
// idFromName identity, so INV-1 tenant recovery runs for real).
//
// What it pins, end to end over HTTP shapes:
//   - the auth seam: /healthz open; everything else 401s without a mapped
//     bearer token; a reserved `tenantId: "system"` map entry never
//     authenticates
//   - the full approval loop: start -> suspension auto-queues an approval ->
//     SoD denial (the requester, wearing the decide-capable admin role) +
//     role denial (viewer) -> reviewer approves -> the derived grant admits
//     the gated publish -> status projection reads success
//   - fail-closed: a forged resume (no approval) is denied at the grant gate
//   - INV-1/INV-2 at the template's boundary: client runIds 400, viewers
//     cannot start runs, another tenant's token 404s on status AND resume and
//     cannot see the approval in its queue
//   - scheduled(): the cron escalates an SLA-overdue approval and purges only
//     stale TERMINAL snapshots; a broken approval store must not stop the
//     retention purge (the two surfaces are isolated)
//   - queue(): no SIEM endpoint -> the batch retries (nothing acked);
//     configured -> one NDJSON POST with the auth header, then ack
//
// Scope note: the template's @proofoftech/flowsafe/* imports are aliased to
// SOURCE here (vitest.config.ts / tsconfig.test.json) — this proves the
// template's behavior, not that the published dist/ exports map resolves.

import type {
  D1Database,
  DurableObjectNamespace,
  DurableObjectState,
  ExecutionContext,
  MessageBatch,
  ScheduledController,
} from '@cloudflare/workers-types';
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  type ApprovalRecord,
  D1ApprovalStoreFactory,
} from '../src/approval-api/index.js';
import type { RunSummary } from '../src/do-runner/index.js';
import {
  d1DatabaseLike,
  openSqlite,
  type SqliteDatabase,
} from '../test-support/sqlite.js';
import { PURGE_CRON, SWEEP_CRON } from './crons.js';
import handler, { FlowsafeRunner } from './worker.js';

type Env = Parameters<NonNullable<typeof handler.fetch>>[1];

function required<T>(handlerFn: T | undefined, name: string): T {
  if (!handlerFn) throw new Error(`handler.${name} is not defined`);
  return handlerFn;
}
const fetchHandler = required(handler.fetch, 'fetch');
const scheduledHandler = required(handler.scheduled, 'scheduled');
const queueHandler = required(handler.queue, 'queue');

// In-process DO namespace: idFromName carries the name, get() memoizes a REAL
// FlowsafeRunner per name with a stub state exposing that identity — the same
// `{ id: { name } }` shape durable-object.ts documents for node tests, so
// tenant recovery (INV-1) and the identity assert both execute for real.
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

// Bearer map for the APPROVAL_ACTOR_TOKENS secret. The 'system' entry is a
// forgery probe: parseActorTokens must drop it (reserved tenant), so the
// token never authenticates. 'tok-admin' exists because admin is the ONLY
// role in both RUN_START_ROLES and CAN_REVIEW — the identity that can start
// a run AND would be role-allowed to decide it, so the separation-of-duties
// denial (requester ≠ decider) is testable distinctly from the role gate.
const TOKENS = {
  'tok-admin': { id: 'ada-admin', role: 'admin', tenantId: 'acme' },
  'tok-operator': { id: 'op-olive', role: 'operator', tenantId: 'acme' },
  'tok-reviewer': { id: 'rev-ray', role: 'reviewer', tenantId: 'acme' },
  'tok-viewer': { id: 'vic-viewer', role: 'viewer', tenantId: 'acme' },
  'tok-rival': { id: 'op-rival', role: 'operator', tenantId: 'rival' },
  'tok-system': { id: 'sneak', role: 'admin', tenantId: 'system' },
};

function makeEnv(overrides: Partial<Env> = {}): {
  env: Env;
  sqlite: SqliteDatabase;
  d1: unknown;
} {
  const sqlite = openSqlite();
  const d1 = d1DatabaseLike(sqlite);
  const env = {
    DB: d1 as D1Database,
    APPROVAL_ACTOR_TOKENS: JSON.stringify(TOKENS),
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

  it('drops a reserved `tenantId: "system"` map entry — the token never authenticates', async () => {
    // #given — TOKENS carries 'tok-system' with the TCB's own audit identity
    const { env } = makeEnv();

    // #when / #then — parseActorTokens discards the entry, so 401 (not 403)
    expect(
      (await call(env, '/workflows', { token: 'tok-system' })).status,
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
      actor: { id: 'rev-ray', role: 'reviewer', tenantId: 'acme' },
      workflows: [{ id: 'example-approval' }],
    });
  });
});

describe('deploy worker fetch(): the approval loop over HTTP', () => {
  it('start suspends and auto-queues an approval bound to the suspension', async () => {
    // #given / #when
    const { env } = makeEnv();
    const started = await startRun(env);

    // #then — server-minted tenant-salted runId (INV-1) and a queued approval
    // carrying the gate's server-authored connector request
    expect(started.status).toBe('suspended');
    expect(started.runId).toMatch(/^acme_/);
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
    expect(summary.error).toContain('approval required and not granted');
  });
});

describe('deploy worker fetch(): tenant boundary (INV-1/INV-2 at the template)', () => {
  it('400s a client-supplied runId — the id is the tenant carrier', async () => {
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

  it("another tenant's token 404s on status AND resume, and its queue never lists the approval", async () => {
    // #given — tenant acme owns a suspended run + its approval
    const { env } = makeEnv();
    const started = await startRun(env);
    const approvalId = started.approval?.id;
    if (!approvalId) throw new Error('expected an auto-queued approval');

    // #when / #then — 404 (not 403): no existence oracle for foreign runIds
    const status = await call(env, `/runs/example-approval/${started.runId}`, {
      token: 'tok-rival',
    });
    expect(status.status).toBe(404);
    const resume = await call(
      env,
      `/runs/example-approval/${started.runId}/resume`,
      {
        method: 'POST',
        body: JSON.stringify({
          step: 'gate',
          resumeData: { approved: true },
        }),
        token: 'tok-rival',
      },
    );
    expect(resume.status).toBe(404);

    // #then — rival's tenant-bound store cannot see acme's approval
    const rivalList = await call(env, '/api/approvals', { token: 'tok-rival' });
    expect(rivalList.status).toBe(200);
    expect(JSON.stringify(await rivalList.json())).not.toContain(approvalId);
    const acmeList = await call(env, '/api/approvals', {
      token: 'tok-reviewer',
    });
    expect(JSON.stringify(await acmeList.json())).toContain(approvalId);
  });

  it('TENANT_APEX_DOMAIN engages the subdomain cross-check: mismatched host denied, own host and off-apex hosts pass', async () => {
    // #given — the wrap is env-var-gated; this is the only host that wires it
    const { env } = makeEnv({ TENANT_APEX_DOMAIN: 'proof.example' });

    // #when / #then — acme's token on rival's subdomain: authenticated but
    // forbidden (the pasted-token confused-deputy the check exists to close)
    const mismatch = await call(env, '/workflows', {
      token: 'tok-reviewer',
      host: 'rival.proof.example',
    });
    expect(mismatch.status).toBe(403);

    // #then — the same token on its OWN subdomain passes
    const match = await call(env, '/workflows', {
      token: 'tok-reviewer',
      host: 'acme.proof.example',
    });
    expect(match.status).toBe(200);

    // #then — a host outside the apex skips the check (single-host topology)
    const offApex = await call(env, '/workflows', { token: 'tok-reviewer' });
    expect(offApex.status).toBe(200);
  });
});

// Column set per @mastra/core storage constants for mastra_workflow_snapshot
// (camelCase timestamps, snapshot serialized as JSON TEXT) — the cron's purge
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

describe('deploy worker scheduled(): cron-owned maintenance', () => {
  it('escalates an SLA-overdue approval and purges only stale TERMINAL snapshots', async () => {
    // #given — an overdue pending approval (seeded through the real store,
    // same DB object the worker binds) and three snapshot rows: stale
    // terminal, fresh terminal, stale suspended
    // RUN_RETENTION_DAYS is deliberately invalid: numberVar must log the
    // config error and fall back to the 30-day default rather than skip
    // maintenance (the fallback keeps the purge asserts below meaningful).
    const { env, sqlite, d1 } = makeEnv({ RUN_RETENTION_DAYS: '-5' });
    const store = new D1ApprovalStoreFactory(d1 as never).forTenant('acme');
    const past = new Date(Date.now() - 60_000).toISOString();
    await store.create({
      id: 'apr-overdue',
      tenantId: 'acme',
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

    // #when — the SWEEP cron fires alone
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const sweepCtx = makeCtx();
    await scheduledHandler(
      { cron: SWEEP_CRON } as unknown as ScheduledController,
      env,
      sweepCtx.ctx,
    );
    await sweepCtx.drain();

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

    // #when — the PURGE cron fires
    const purgeCtx = makeCtx();
    await scheduledHandler(
      { cron: PURGE_CRON } as unknown as ScheduledController,
      env,
      purgeCtx.ctx,
    );
    await purgeCtx.drain();

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
    const sweepCtx = makeCtx();
    await scheduledHandler(
      { cron: SWEEP_CRON } as unknown as ScheduledController,
      env,
      sweepCtx.ctx,
    );
    await sweepCtx.drain();
    const purgeCtx = makeCtx();
    await scheduledHandler(
      { cron: PURGE_CRON } as unknown as ScheduledController,
      env,
      purgeCtx.ctx,
    );
    await purgeCtx.drain();

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
      APPROVAL_ACTOR_TOKENS: JSON.stringify(TOKENS),
    } as Env;
    const store = new D1ApprovalStoreFactory(d1 as never).forTenant('acme');
    const past = new Date(Date.now() - 60_000).toISOString();
    await store.create({
      id: 'apr-overdue',
      tenantId: 'acme',
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
    const purgeCtx = makeCtx();
    await scheduledHandler(
      { cron: PURGE_CRON } as unknown as ScheduledController,
      env,
      purgeCtx.ctx,
    );
    await purgeCtx.drain();
    const sweepCtx = makeCtx();
    await scheduledHandler(
      { cron: SWEEP_CRON } as unknown as ScheduledController,
      env,
      sweepCtx.ctx,
    );
    await sweepCtx.drain();

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

  it('an unrecognized cron expression runs BOTH surfaces and logs the misconfig', async () => {
    // #given — ops edited wrangler.jsonc without updating the constants;
    // availability of both duties beats purity on a misconfig
    const { env, sqlite, d1 } = makeEnv();
    const store = new D1ApprovalStoreFactory(d1 as never).forTenant('acme');
    const past = new Date(Date.now() - 60_000).toISOString();
    await store.create({
      id: 'apr-overdue',
      tenantId: 'acme',
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
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when
    const { ctx, drain } = makeCtx();
    await scheduledHandler(
      { cron: '*/5 * * * *' } as unknown as ScheduledController,
      env,
      ctx,
    );
    await drain();

    // #then — swept AND purged, with the config-error tripwire on record
    expect((await store.get('apr-overdue'))?.status).toBe('escalated');
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(
      errorSpy.mock.calls.some(([line]) => {
        const text = String(line);
        return text.includes('config-error') && text.includes('triggers.crons');
      }),
    ).toBe(true);
    errorSpy.mockRestore();
  });
});

function fakeBatch(bodies: unknown[]): {
  batch: MessageBatch;
  acked: () => boolean;
  retried: () => boolean;
} {
  let ackedAll = false;
  let retriedAll = false;
  const batch = {
    queue: 'flowsafe-audit',
    messages: bodies.map((body, index) => ({
      id: `m${index}`,
      timestamp: new Date(),
      body,
      ack: () => {},
      retry: () => {},
    })),
    ackAll: () => {
      ackedAll = true;
    },
    retryAll: () => {
      retriedAll = true;
    },
  } as unknown as MessageBatch;
  return { batch, acked: () => ackedAll, retried: () => retriedAll };
}

describe('deploy worker queue(): audit export consumer', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('retries the batch (acking nothing) when SIEM_ENDPOINT is unset', async () => {
    // #given — the consumer is bound but the export target is not configured
    const { env } = makeEnv();
    const fetchSpy = vi.fn();
    vi.stubGlobal('fetch', fetchSpy);
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { batch, acked, retried } = fakeBatch([{ action: 'x' }]);

    // #when
    const { ctx } = makeCtx();
    await queueHandler(batch, env, ctx);

    // #then — nothing acked unconfirmed, nothing sent, misconfig logged
    expect(retried()).toBe(true);
    expect(acked()).toBe(false);
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(
      errorSpy.mock.calls.some(([line]) =>
        String(line).includes('config-error'),
      ),
    ).toBe(true);
    errorSpy.mockRestore();
  });

  it('ships the batch as one authenticated NDJSON POST and acks on 2xx', async () => {
    // #given
    const { env: baseEnv } = makeEnv();
    const env = {
      ...(baseEnv as unknown as Record<string, unknown>),
      SIEM_ENDPOINT: 'https://siem.example/collect',
      SIEM_AUTH_HEADER: 'Splunk secret-token',
    } as Env;
    const fetchSpy = vi.fn(async () => ({ ok: true, status: 200 }));
    vi.stubGlobal('fetch', fetchSpy);
    const events = [
      { action: 'approval.decide', decision: 'allowed' },
      { action: 'approval.escalate', decision: 'allowed' },
    ];
    const { batch, acked, retried } = fakeBatch(events);

    // #when
    const { ctx } = makeCtx();
    await queueHandler(batch, env, ctx);

    // #then — one POST, NDJSON body, auth header forwarded, batch acked
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const [url, init] = fetchSpy.mock.calls[0] as unknown as [
      string,
      { method: string; headers: Record<string, string>; body: string },
    ];
    expect(url).toBe('https://siem.example/collect');
    expect(init.method).toBe('POST');
    expect(init.headers.authorization).toBe('Splunk secret-token');
    expect(init.body).toBe(events.map((e) => JSON.stringify(e)).join('\n'));
    expect(acked()).toBe(true);
    expect(retried()).toBe(false);
  });
});
