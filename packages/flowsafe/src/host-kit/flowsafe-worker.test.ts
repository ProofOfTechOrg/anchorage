// SPDX-License-Identifier: Apache-2.0
// Unit proof for the composed Worker skeleton: the fetch pipeline order, the
// hook seams (preRoutes/beforeStart/beforeResume/notify/extra
// duties), and failure-isolated sweep, purge, and optional schedule-tick dispatch. The
// HEAVYWEIGHT behavior proof stays the two host e2e suites
// (deploy/worker.e2e.test.ts and the showcase worker e2e set), which drive
// the real hosts through this same composer — this file covers the composer's
// own contract over fakes: node:sqlite behind a narrow SQL unit facade, a stub DO
// namespace, and a static verifier.

import { afterEach, describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import type {
  ApprovalNotificationEvent,
  ApprovalRecord,
} from '../approval-api/index.js';
import { decodeExecutionPrincipal } from '../approval-api/index.js';
import {
  AUDIT_PROXY_INSTANCE_NAME,
  FlowsafeFleetAuditProxy,
} from '../audit-export/index.js';
import {
  EXECUTION_PRINCIPAL_HEADER,
  type RunSummary,
} from '../do-runner/index.js';
import type { ResumeRunFn } from './approval-bridge.js';
import {
  createFlowsafeWorker,
  type FlowsafeWorkerConfig,
  type FlowsafeWorkerEnv,
  MAINTENANCE_INSTANCE_NAME,
} from './flowsafe-worker.js';
import { approvalStoreFactoryFor } from './host-approval-service.js';
import { MAINTENANCE_RECEIPT_HEADER } from './maintenance-capability.js';
import { staticTokenVerifier } from './verifier.js';
import type { WorkflowMeta } from './workflow-meta.js';

const WORKFLOWS: ReadonlyArray<WorkflowMeta> = [
  {
    id: 'wf',
    title: 'Example',
    description: 'composer test workflow',
    sampleInput: {},
  },
];

const ACTORS = new Map([['tok-ada', { id: 'ada', role: 'admin' } as const]]);

function successSummary(runId: string): RunSummary {
  return { runId, status: 'success', result: { ok: true } };
}

/** A DO namespace whose stub echoes a success summary and commits run ownership. */
function fakeRunner(
  calls: string[],
  database: FlowsafeWorkerEnv['DB'],
): FlowsafeWorkerEnv['RUNNER'] {
  return {
    idFromName: (name: string) => name,
    get: () => ({
      fetch: async (
        url: string,
        init?: { body?: string; headers?: Record<string, string> },
      ) => {
        calls.push(url);
        const body = init?.body
          ? (JSON.parse(init.body) as {
              runId?: string;
            })
          : {};
        const runId =
          body.runId ?? /\/runs\/[^/]+\/([^/]+)/.exec(url)?.[1] ?? 'acme_x';
        const principal = decodeExecutionPrincipal(
          init?.headers?.[EXECUTION_PRINCIPAL_HEADER] ?? '',
        );
        if (body.runId && principal) {
          await approvalStoreFactoryFor(database)
            .resources()
            .claim('run', body.runId, {
              kind: principal.kind,
              id: principal.id,
            });
        }
        return {
          ok: true,
          status: 200,
          json: async () => successSummary(runId),
        };
      },
    }),
  };
}

interface Harness {
  env: FlowsafeWorkerEnv;
  ctx: { waitUntil: (promise: Promise<unknown>) => void };
  flush: () => Promise<void>;
  doCalls: string[];
}

function makeEnv(): Harness {
  const doCalls: string[] = [];
  const pending: Promise<unknown>[] = [];
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
  const database = sqliteUnitDatabase(sqlite) as FlowsafeWorkerEnv['DB'];
  return {
    env: {
      DB: database,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET: 'test-deployment-identity-secret-0001',
      RUNNER: fakeRunner(doCalls, database),
      MAINTENANCE: {
        idFromName: (name: string) => name,
        get: () => ({
          fetch: async () => new Response(null, { status: 204 }),
        }),
      },
    },
    ctx: { waitUntil: (promise) => pending.push(promise) },
    flush: async () => {
      await Promise.all(pending);
    },
    doCalls,
  };
}

function makeWorker(
  overrides: Partial<FlowsafeWorkerConfig<FlowsafeWorkerEnv>> = {},
): ReturnType<typeof createFlowsafeWorker<FlowsafeWorkerEnv>> {
  return createFlowsafeWorker<FlowsafeWorkerEnv>({
    workflows: WORKFLOWS,
    systemPrincipalId: 'composer-system',
    buildVerifier: () => staticTokenVerifier(ACTORS),
    maintenance: {
      sweepIntervalMs: 15 * 60 * 1_000,
      purgeIntervalMs: 60 * 60 * 1_000,
    },
    ...overrides,
  });
}

function authed(url: string, init: RequestInit = {}): Request {
  return new Request(url, {
    ...init,
    headers: {
      authorization: 'Bearer tok-ada',
      ...(init.body !== undefined && { 'content-type': 'application/json' }),
    },
  });
}

function capturedLogs(): { lines: () => string[]; errors: () => string[] } {
  const log = vi.spyOn(console, 'log').mockImplementation(() => {});
  const error = vi.spyOn(console, 'error').mockImplementation(() => {});
  return {
    lines: () => log.mock.calls.map((call) => String(call[0])),
    errors: () => error.mock.calls.map((call) => String(call[0])),
  };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('createFlowsafeWorker fetch pipeline', () => {
  it.each([
    ['missing DB', (env: FlowsafeWorkerEnv) => ({ ...env, DB: undefined })],
    [
      'missing deployment tag',
      (env: FlowsafeWorkerEnv) => ({ ...env, DEPLOYMENT_TENANT: undefined }),
    ],
    [
      'malformed deployment tag',
      (env: FlowsafeWorkerEnv) => ({ ...env, DEPLOYMENT_TENANT: 'ACME' }),
    ],
    [
      'missing deployment credential',
      (env: FlowsafeWorkerEnv) => ({
        ...env,
        DEPLOYMENT_IDENTITY_SECRET: undefined,
      }),
    ],
    [
      'mismatched deployment sentinel',
      (env: FlowsafeWorkerEnv) => ({ ...env, DEPLOYMENT_TENANT: 'other' }),
    ],
  ])('returns 503 before health or routes for a %s', async (_label, mutate) => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    const logs = capturedLogs();

    const response = await worker.fetch(
      new Request('http://host/healthz'),
      mutate(env) as FlowsafeWorkerEnv,
      ctx,
    );

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: 'deployment unavailable' });
    expect(
      logs.errors().some((line) => line.includes('deployment-identity-error')),
    ).toBe(true);
  });

  it.each([
    ['missing sentinel', `CREATE TABLE unrelated (id TEXT PRIMARY KEY)`],
    [
      'malformed sentinel schema',
      `CREATE TABLE flowsafe_deployment (
        id INTEGER PRIMARY KEY,
        tenant_tag TEXT NOT NULL,
        provisioned_at TEXT NOT NULL
      );
      INSERT INTO flowsafe_deployment VALUES
        (1, 'acme', '2026-08-10T00:00:00.000Z')`,
    ],
    [
      'malformed sentinel row',
      `CREATE TABLE flowsafe_deployment (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        tenant_tag TEXT NOT NULL,
        provisioned_at TEXT NOT NULL
      );
      INSERT INTO flowsafe_deployment VALUES
        (1, 'ACME', '2026-08-10T00:00:00.000Z')`,
    ],
  ])('returns 503 for a database with a %s', async (_label, sql) => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    const sqlite = openSqlite();
    sqlite.exec(sql);
    const logs = capturedLogs();

    const response = await worker.fetch(
      new Request('http://host/healthz'),
      {
        ...env,
        DB: sqliteUnitDatabase(sqlite) as FlowsafeWorkerEnv['DB'],
      },
      ctx,
    );

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: 'deployment unavailable' });
    expect(
      logs.errors().some((line) => line.includes('deployment-identity-error')),
    ).toBe(true);
  });

  it('serves /healthz unauthenticated and 404s the tail', async () => {
    // #given — a verifier that MUST NOT be consulted for healthz
    const buildVerifier = vi.fn(() => staticTokenVerifier(ACTORS));
    const worker = makeWorker({ buildVerifier });
    const { env, ctx } = makeEnv();

    // #when
    const health = await worker.fetch(
      new Request('http://host/healthz'),
      env,
      ctx,
    );
    // #then
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ ok: true });
    expect(buildVerifier).not.toHaveBeenCalled();

    // #when — an authenticated request to nowhere falls through both routers
    const missing = await worker.fetch(authed('http://host/nope'), env, ctx);
    // #then
    expect(missing.status).toBe(404);
  });

  it('does not mount trusted audit ingress on an ordinary public Worker', async () => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();

    const response = await worker.fetch(
      new Request('http://host/internal/audit', {
        method: 'POST',
        body: JSON.stringify({
          action: 'approval.decide',
          resource: 'approval:one',
          decision: 'allowed',
        }),
      }),
      env,
      ctx,
    );

    expect(response.status).toBe(404);
  });

  it('protects maintenance administration with its dedicated secret and fixed singleton address', async () => {
    const calls: Array<{
      name: string;
      url: string;
      init?: { method?: string; headers?: Record<string, string> };
    }> = [];
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    env.MAINTENANCE_ADMIN_SECRET = 'maintenance-admin-secret-0000000001';
    env.FLEET_SPEC_DIGEST = 'a'.repeat(64);
    env.MAINTENANCE = {
      idFromName: (name: string) => name,
      get: (name: string) => ({
        fetch: async (url, init) => {
          calls.push({ name, url, init });
          return Response.json({ ok: true });
        },
      }),
    };

    const missing = await worker.fetch(
      new Request('http://host/admin/ensure-maintenance', { method: 'POST' }),
      env,
      ctx,
    );
    expect(missing.status).toBe(401);

    const applicationToken = await worker.fetch(
      new Request('http://host/admin/ensure-maintenance', {
        method: 'POST',
        headers: { authorization: 'Bearer tok-ada' },
      }),
      env,
      ctx,
    );
    expect(applicationToken.status).toBe(401);

    const ensured = await worker.fetch(
      new Request('http://host/admin/ensure-maintenance?instance=evil', {
        method: 'POST',
        headers: {
          authorization: `Bearer ${env.MAINTENANCE_ADMIN_SECRET}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify({ instance: 'evil', deploymentTag: 'other' }),
      }),
      env,
      ctx,
    );
    expect(ensured.status).toBe(200);
    expect(await ensured.json()).toEqual({
      ok: true,
      deploymentSpecDigest: env.FLEET_SPEC_DIGEST,
    });
    expect(calls[0]).toMatchObject({
      name: MAINTENANCE_INSTANCE_NAME,
      url: 'http://maintenance/ensure',
      init: { method: 'POST' },
    });
    expect(calls[0]?.init?.headers).not.toHaveProperty('authorization');
    expect(calls[0]?.init?.headers).toHaveProperty(
      'x-flowsafe-deployment-identity',
      env.DEPLOYMENT_IDENTITY_SECRET,
    );

    const status = await worker.fetch(
      new Request('http://host/admin/maintenance-status', {
        headers: {
          authorization: `Bearer ${env.MAINTENANCE_ADMIN_SECRET}`,
        },
      }),
      env,
      ctx,
    );
    expect(status.status).toBe(200);
    expect(await status.json()).toEqual({
      ok: true,
      deploymentSpecDigest: env.FLEET_SPEC_DIGEST,
    });
    expect(calls[1]).toMatchObject({
      name: MAINTENANCE_INSTANCE_NAME,
      url: 'http://maintenance/status',
      init: { method: 'GET' },
    });

    delete env.FLEET_SPEC_DIGEST;
    const ordinaryHostStatus = await worker.fetch(
      new Request('http://host/admin/maintenance-status', {
        headers: {
          authorization: `Bearer ${env.MAINTENANCE_ADMIN_SECRET}`,
        },
      }),
      env,
      ctx,
    );
    expect(await ordinaryHostStatus.json()).toEqual({ ok: true });
  });

  it('fails maintenance administration closed when its dedicated secret is unset', async () => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    const logs = capturedLogs();

    const response = await worker.fetch(
      new Request('http://host/admin/maintenance-status', {
        headers: { authorization: 'Bearer any-value' },
      }),
      env,
      ctx,
    );

    expect(response.status).toBe(503);
    expect(logs.errors().some((line) => line.includes('config-error'))).toBe(
      true,
    );
  });

  it('relays a one-shot fleet capability without holding the signing secret', async () => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    env.FLEET_MAINTENANCE_CAPABILITIES = 'required';
    env.FLEET_SPEC_DIGEST = 'a'.repeat(64);
    const fetch = vi.fn(async () => {
      const response = Response.json({ alarmAt: 1 });
      response.headers.set(MAINTENANCE_RECEIPT_HEADER, 'signed-receipt');
      return response;
    });
    env.MAINTENANCE = {
      idFromName: (name: string) => name,
      get: () => ({ fetch }),
    };

    const response = await worker.fetch(
      new Request('http://host/admin/ensure-maintenance', {
        method: 'POST',
        headers: { authorization: 'Bearer one-shot-capability' },
      }),
      env,
      ctx,
    );

    expect(response.status).toBe(200);
    expect(response.headers.get(MAINTENANCE_RECEIPT_HEADER)).toBe(
      'signed-receipt',
    );
    expect(fetch).toHaveBeenCalledWith('http://maintenance/ensure', {
      method: 'POST',
      headers: { authorization: 'Bearer one-shot-capability' },
    });
    expect(env.MAINTENANCE_ADMIN_SECRET).toBeUndefined();
  });

  it('refuses to reuse the Worker-to-DO credential for maintenance administration', async () => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    env.MAINTENANCE_ADMIN_SECRET = env.DEPLOYMENT_IDENTITY_SECRET;
    const logs = capturedLogs();

    const response = await worker.fetch(
      new Request('http://host/admin/ensure-maintenance', {
        method: 'POST',
        headers: {
          authorization: `Bearer ${env.DEPLOYMENT_IDENTITY_SECRET}`,
        },
      }),
      env,
      ctx,
    );

    expect(response.status).toBe(503);
    expect(
      logs.errors().some((line) => line.includes('credentials must differ')),
    ).toBe(true);
  });

  it('fails authenticated maintenance closed for a malformed fleet digest', async () => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    env.MAINTENANCE_ADMIN_SECRET = 'maintenance-admin-secret-0000000001';
    env.FLEET_SPEC_DIGEST = 'not-a-digest';
    const logs = capturedLogs();

    const response = await worker.fetch(
      new Request('http://host/admin/maintenance-status', {
        headers: {
          authorization: `Bearer ${env.MAINTENANCE_ADMIN_SECRET}`,
        },
      }),
      env,
      ctx,
    );

    expect(response.status).toBe(503);
    expect(
      logs.errors().some((line) => line.includes('FLEET_SPEC_DIGEST')),
    ).toBe(true);
  });

  it('401s an unauthenticated API route (the resolver runs before any store exists)', async () => {
    // #given
    const worker = makeWorker();
    const { env, ctx } = makeEnv();

    // #when
    const response = await worker.fetch(
      new Request('http://host/api/approvals'),
      env,
      ctx,
    );

    // #then
    expect(response?.status).toBe(401);
  });

  it('threads APPROVAL_ALLOW_SELF_DECISION into the catalog canSelfDecide echo (fail-closed on garbage)', async () => {
    // #given
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    const canSelfDecide = async (): Promise<boolean | undefined> => {
      const response = await worker.fetch(
        authed('http://host/workflows'),
        env,
        ctx,
      );
      const body = (await response.json()) as {
        actor: { canSelfDecide?: boolean };
      };
      return body.actor.canSelfDecide;
    };

    // #when / #then — unset: SoD on, admin is not exempt
    expect(await canSelfDecide()).toBe(false);

    // #when / #then — exempt admin
    env.APPROVAL_ALLOW_SELF_DECISION = 'admin';
    expect(await canSelfDecide()).toBe(true);

    // #when / #then — garbage falls back to OFF (fail closed)
    vi.spyOn(console, 'error').mockImplementation(() => {});
    env.APPROVAL_ALLOW_SELF_DECISION = 'nonsense';
    expect(await canSelfDecide()).toBe(false);
  });

  it('runs preRoutes after /healthz and before the routers, handing over resolve + topology', async () => {
    // #given
    let kitSeen: { resolve: unknown; topology: { start: unknown } } | undefined;
    const worker = makeWorker({
      preRoutes: async (request, _env, _ctx, kit) => {
        kitSeen = kit;
        const url = new URL(request.url);
        return url.pathname === '/custom'
          ? new Response('pre', { status: 418 })
          : null;
      },
    });
    const { env, ctx } = makeEnv();

    // #when — the pre-route short-circuits its own path…
    const custom = await worker.fetch(
      new Request('http://host/custom'),
      env,
      ctx,
    );
    // #then
    expect(custom.status).toBe(418);
    expect(typeof kitSeen?.resolve).toBe('function');
    expect(typeof kitSeen?.topology.start).toBe('function');

    // #when — …and a null falls through to the approval router
    const list = await worker.fetch(
      authed('http://host/api/approvals'),
      env,
      ctx,
    );
    // #then
    expect(list.status).toBe(200);
    expect(await list.json()).toEqual([]);
  });

  it('mounts the agent router after preRoutes and before the signal router', async () => {
    const order: string[] = [];
    const worker = makeWorker({
      preRoutes: async () => {
        order.push('pre');
        return null;
      },
      buildAgentRouter: () => async () => {
        order.push('agent');
        return new Response('agent', { status: 299 });
      },
      buildSignalRouter: () => async () => {
        order.push('signal');
        return new Response('signal', { status: 298 });
      },
    });
    const { env, ctx } = makeEnv();

    const response = await worker.fetch(authed('http://host/agents'), env, ctx);

    expect(response.status).toBe(299);
    expect(order).toEqual(['pre', 'agent']);
  });

  it('uses buildResumeRun while constructing the request-scoped approval service', async () => {
    const buildResumeRun = vi.fn(
      (fallback: ResumeRunFn, _env: FlowsafeWorkerEnv) => fallback,
    );
    const worker = makeWorker({ buildResumeRun });
    const { env, ctx } = makeEnv();

    const response = await worker.fetch(
      authed('http://host/api/approvals'),
      env,
      ctx,
    );

    expect(response.status).toBe(200);
    expect(buildResumeRun).toHaveBeenCalledOnce();
    expect(buildResumeRun.mock.calls[0]?.[1]).toBe(env);
  });

  it('runs the context-aware start and resume policies before the topology thunks', async () => {
    // #given
    const wrapped: string[] = [];
    const worker = makeWorker({
      beforeStart: async (context, _env, workflowId) => {
        wrapped.push(`start:${workflowId}:${context.actor.id}`);
      },
      beforeResume: async (context, _env, workflowId, runId) => {
        wrapped.push(`resume:${workflowId}:${runId}:${context.actor.id}`);
      },
    });
    const { env, ctx, doCalls } = makeEnv();

    // #when — a real start through the run router (runId server-minted)
    const started = await worker.fetch(
      authed('http://host/runs', {
        method: 'POST',
        body: JSON.stringify({ workflowId: 'wf', inputData: {} }),
      }),
      env,
      ctx,
    );
    // #then
    expect(started.status).toBe(200);
    const summary = (await started.json()) as { runId: string };
    expect(summary.runId).toMatch(/^[0-9a-f-]{36}$/);
    expect(wrapped).toEqual(['start:wf:ada']);
    expect(doCalls.length).toBeGreaterThan(0);

    // #when — a raw resume of the minted run
    const resumed = await worker.fetch(
      authed(`http://host/runs/wf/${summary.runId}/resume`, {
        method: 'POST',
        body: JSON.stringify({ step: ['gate'], resumeData: {} }),
      }),
      env,
      ctx,
    );
    // #then
    expect(resumed.status).toBe(200);
    expect(wrapped).toEqual(['start:wf:ada', `resume:wf:${summary.runId}:ada`]);
  });

  it('mounts an opt-in buildObjectiveRouter; absent seam is byte-identical (unmounted)', async () => {
    // #given — a worker WITH a goal stage that claims /api/threads/:id/goal
    const worker = makeWorker({
      buildObjectiveRouter: () => async (request) => {
        const url = new URL(request.url);
        return url.pathname.endsWith('/goal')
          ? new Response('goal-stage', { status: 299 })
          : null;
      },
    });
    const { env, ctx } = makeEnv();

    // #when — a goal path reaches the mounted stage…
    const mounted = await worker.fetch(
      authed('http://host/api/threads/acme_t1/goal', {
        method: 'PUT',
        body: '{}',
      }),
      env,
      ctx,
    );
    // #then
    expect(mounted.status).toBe(299);

    // #given — a worker WITHOUT the seam
    const bare = makeWorker();
    const { env: env2, ctx: ctx2 } = makeEnv();
    // #when — the same path is not a goal stage; it falls through to the 404 tail
    const unmounted = await bare.fetch(
      authed('http://host/api/threads/acme_t1/goal', {
        method: 'PUT',
        body: '{}',
      }),
      env2,
      ctx2,
    );
    // #then — no goal handling, as before the seam existed
    expect(unmounted.status).toBe(404);
  });

  it('contains a throwing mounted router as a generic 500 (backstop), never an unhandled rejection', async () => {
    // #given — a mounted stage whose router THROWS (a URIError from an unguarded
    // path decode is the concrete case this backstops)
    const worker = makeWorker({
      buildSignalRouter: () => async () => {
        throw new URIError('URI malformed');
      },
    });
    const { env, ctx } = makeEnv();
    const logs = capturedLogs();

    // #when
    const res = await worker.fetch(
      authed('http://host/api/threads/acme_t1/message', {
        method: 'POST',
        body: '{}',
      }),
      env,
      ctx,
    );

    // #then — a generic 500 (no error.message leaked), and the fault is logged
    expect(res.status).toBe(500);
    expect(await res.json()).toEqual({ error: 'internal error' });
    expect(logs.errors().some((l) => l.includes('worker-fetch-error'))).toBe(
      true,
    );
  });

  it('mounts an opt-in buildScheduleRouter; absent seam is byte-identical (unmounted)', async () => {
    // #given — a worker WITH a schedule stage that claims /api/schedules
    const worker = makeWorker({
      buildScheduleRouter: () => async (request) => {
        const url = new URL(request.url);
        return url.pathname.startsWith('/api/schedules')
          ? new Response('schedule-stage', { status: 299 })
          : null;
      },
    });
    const { env, ctx } = makeEnv();

    // #when — a schedules path reaches the mounted stage…
    const mounted = await worker.fetch(
      authed('http://host/api/schedules'),
      env,
      ctx,
    );
    // #then
    expect(mounted.status).toBe(299);

    // #given — a worker WITHOUT the seam
    const bare = makeWorker();
    const { env: env2, ctx: ctx2 } = makeEnv();
    // #when — /api/schedules is not a run/approval route; it falls to the 404 tail
    const unmounted = await bare.fetch(
      authed('http://host/api/schedules'),
      env2,
      ctx2,
    );
    // #then — no schedule handling, as before the seam existed
    expect(unmounted.status).toBe(404);
  });
});

describe('createFlowsafeWorker maintenance duties', () => {
  it('validates the storage table prefix when the host is constructed', () => {
    expect(() => makeWorker({ storageTablePrefix: 'tenant-prod_' })).toThrow(
      'Invalid storageTablePrefix: use an empty prefix or start with a letter or underscore and continue with letters, numbers, or underscores.',
    );
    expect(() => makeWorker({ storageTablePrefix: '01_tenant_' })).toThrow(
      /start with a letter or underscore/,
    );
    expect(() =>
      makeWorker({ storageTablePrefix: 'tenant_01_' }),
    ).not.toThrow();
    expect(() =>
      makeWorker({ storageTablePrefix: 'p'.repeat(39) }),
    ).not.toThrow();
    expect(() => makeWorker({ storageTablePrefix: 'p'.repeat(40) })).toThrow(
      'Invalid storageTablePrefix: must be at most 39 characters so prefixed Mastra table names stay within the 63-character identifier limit.',
    );
  });

  it('refuses maintenance before running a duty when bindings are missing', async () => {
    const worker = makeWorker();
    const { env } = makeEnv();
    capturedLogs();

    await expect(
      worker.runMaintenanceDuty('sweep', {
        ...env,
        DB: undefined,
      } as unknown as FlowsafeWorkerEnv),
    ).resolves.toEqual({
      ok: false,
      error: expect.stringContaining('no valid DB binding'),
    });
  });

  function maintenanceLines(lines: string[]): Record<string, unknown>[] {
    return lines
      .map((line) => {
        try {
          return JSON.parse(line) as Record<string, unknown>;
        } catch {
          return undefined;
        }
      })
      .filter(
        (parsed): parsed is Record<string, unknown> =>
          parsed?.type === 'maintenance',
      );
  }

  it('the sweep duty runs ONLY the sweep', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();

    // #when
    await worker.runMaintenanceDuty('sweep', env);

    // #then — one maintenance line, the sweep's (escalated, never purged)
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toHaveProperty('escalated');
    expect(lines[0]).not.toHaveProperty('purged');
  });

  it('routes external maintenance audit through the authenticated proxy adapter', async () => {
    const candidateWorker = makeWorker();
    const { env: candidateEnv } = makeEnv();
    const past = new Date(Date.now() - 60_000).toISOString();
    await approvalStoreFactoryFor(candidateEnv.DB).store().create({
      id: 'apr-proxy-audit',
      workflowId: 'wf',
      runId: 'acme_proxy',
      title: 'proxy audit request',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: past,
      updatedAt: past,
      slaDeadlineAt: past,
    });
    const send = vi.fn(async () => {});
    const proxy = new FlowsafeFleetAuditProxy(
      { id: { name: AUDIT_PROXY_INSTANCE_NAME } },
      {
        AUDIT_QUEUE: { send },
        DEPLOYMENT_IDENTITY_SECRET: candidateEnv.DEPLOYMENT_IDENTITY_SECRET,
        DEPLOYMENT_TENANT: 'acme',
        FLEET_ENVIRONMENT: 'production',
        FLEET_DEPLOYMENT_SCRIPT: 'acme-prod',
      },
    );
    candidateEnv.FLEET_AUDIT_PROXY = 'required';
    candidateEnv.AUDIT_PROXY = {
      getByName: () => proxy,
    };

    await expect(
      candidateWorker.runMaintenanceDuty('sweep', candidateEnv),
    ).resolves.toMatchObject({ ok: true });

    expect(send).toHaveBeenCalledWith({
      fleetAttribution: {
        source: 'external-candidate-via-trusted-proxy',
        eventTrust: 'untrusted',
        tenantTag: 'acme',
        environment: 'production',
        scriptName: 'acme-prod',
      },
      event: expect.objectContaining({
        action: 'approval.escalate',
        resource: 'approval:apr-proxy-audit',
      }),
    });
  });

  it('rejects a service-shaped candidate audit binding before calling it', async () => {
    const worker = makeWorker();
    const { env } = makeEnv();
    const fetch = vi.fn(async () => new Response(null, { status: 204 }));
    env.FLEET_AUDIT_PROXY = 'required';
    env.AUDIT_PROXY = { fetch } as unknown as FlowsafeWorkerEnv['AUDIT_PROXY'];

    await expect(
      worker.runMaintenanceDuty('sweep', env),
    ).resolves.toMatchObject({ ok: false });
    expect(fetch).not.toHaveBeenCalled();
  });

  it('validates trusted state egress topology without calling the service', async () => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    const outboundFetch = vi.fn(async () => new Response(null));
    env.FLEET_RESOURCE_ROLE = 'platform-state';
    env.FLEET_ENVIRONMENT = 'production';
    env.FLEET_RESOURCE_GROUP = '0123456789abcdefabcd';
    env.OUTBOUND_PROXY = { fetch: outboundFetch };
    env.OUTBOUND_PROXY_CREDENTIAL = 'outbound-state-credential-0000000001';
    env.OUTBOUND_TENANT_ID = 'acme';
    env.OUTBOUND_ENVIRONMENT = 'production';
    env.OUTBOUND_RESOURCE_GROUP_ID = '0123456789abcdefabcd';
    env.OUTBOUND_STATE_SCRIPT_NAME =
      'acme-production-state-0123456789abcdefabcd';
    env.OUTBOUND_ROUTE_HOSTNAME = 'acme.example.test';
    env.OUTBOUND_POLICY_ID = '0123456789abcdefabcd';

    const response = await worker.fetch(
      new Request('http://host/healthz'),
      env,
      ctx,
    );

    expect(response.status).toBe(200);
    expect(outboundFetch).not.toHaveBeenCalled();
  });

  it('rejects candidate access to trusted state egress before service use', async () => {
    const worker = makeWorker();
    const { env, ctx } = makeEnv();
    const outboundFetch = vi.fn(async () => new Response(null));
    env.OUTBOUND_PROXY = { fetch: outboundFetch };

    const response = await worker.fetch(
      new Request('http://host/healthz'),
      env,
      ctx,
    );

    expect(response.status).toBe(500);
    expect(outboundFetch).not.toHaveBeenCalled();
  });

  it('the purge duty runs ONLY the purge duties', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();

    // #when
    await worker.runMaintenanceDuty('purge', env);

    // #then
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ purged: 0, approvalsPurged: 0 });
    expect(lines[0]).not.toHaveProperty('escalated');
  });

  it('isolates purge-duty failures: a broken snapshot purge stops neither the approval purge nor extra duties', async () => {
    // #given — DB whose snapshot-table statements THROW (not merely missing)
    const logs = capturedLogs();
    const { env } = makeEnv();
    const realDb = env.DB;
    const throwingDb = {
      batch: realDb.batch?.bind(realDb),
      prepare: (query: string) => {
        if (query.includes('mastra_workflow_snapshot')) {
          throw new Error('snapshot table wedged');
        }
        return realDb.prepare(query);
      },
    } as FlowsafeWorkerEnv['DB'];
    const worker = makeWorker({
      extraPurgeDuties: async () => ({ extraDuty: 'ran' }),
    });

    // #when
    await worker.runMaintenanceDuty('purge', { ...env, DB: throwingDb });

    // #then — the failure is on record and the OTHER duties still folded
    // into the one combined maintenance line
    expect(
      logs
        .errors()
        .some(
          (line) =>
            line.includes('maintenance-error') &&
            line.includes('retention-purge'),
        ),
    ).toBe(true);
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ approvalsPurged: 0, extraDuty: 'ran' });
    expect(lines[0]?.purged).toBeUndefined();
  });

  // The agent-memory thread TTL (docs/agent-memory-isolation.md#thread-retention) as the
  // purge alarm's third duty. Seeds the two memory tables the real
  // @mastra/cloudflare-d1 schema creates (mastra-schema-guard.test.ts pins the
  // column names); a fresh test DB has neither, which is itself the
  // memory-less-deployment case the first test below rides.
  async function seedIdleThread(env: FlowsafeWorkerEnv): Promise<void> {
    await env.DB.prepare(
      'CREATE TABLE mastra_threads (id TEXT PRIMARY KEY, updatedAt TEXT NOT NULL)',
    ).run();
    await env.DB.prepare(
      'CREATE TABLE mastra_messages (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, createdAt TEXT NOT NULL)',
    ).run();
    const idleSince = new Date(Date.now() - 90 * 86_400_000).toISOString();
    await env.DB.prepare(
      'INSERT INTO mastra_threads (id, updatedAt) VALUES (?, ?)',
    )
      .bind('acme_idle', idleSince)
      .run();
    await env.DB.prepare(
      'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
    )
      .bind('m1', 'acme_idle', idleSince)
      .run();
  }

  async function threadIds(env: FlowsafeWorkerEnv): Promise<string[]> {
    const { results } = await env.DB.prepare(
      'SELECT id FROM mastra_threads ORDER BY id',
    ).all<{ id: string }>();
    return results.map((row) => row.id);
  }

  it('leaves the thread TTL UNWIRED by default: no THREAD_RETENTION_DAYS, no duty', async () => {
    // #given — the opt-in posture. A hidden default would start deleting agent
    // memory the day a host enabled agents, so its absence must be inert.
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.runMaintenanceDuty('purge', env);

    // #then — the duty never ran: nothing in the log, nothing deleted
    const lines = maintenanceLines(logs.lines());
    expect(lines[0]).not.toHaveProperty('threadsPurged');
    expect(logs.errors()).toEqual([]);
  });

  it('THREAD_RETENTION_DAYS wires the thread purge into the purge alarm, folded into the ONE maintenance line', async () => {
    // #given — an agent-memory thread idle for 90 days under a 30-day TTL
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.runMaintenanceDuty('purge', {
      ...env,
      THREAD_RETENTION_DAYS: '30',
    });

    // #then — reaped WITH its messages, reported in the combined line
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({
      threadsPurged: 1,
      threadMessagesPurged: 1,
      approvalsPurged: 0,
    });
  });

  // Track B: the background-task TTL cleanup as the purge alarm's opt-in duty.
  async function seedOldCompletedTask(env: FlowsafeWorkerEnv): Promise<void> {
    await env.DB.prepare(
      `CREATE TABLE mastra_background_tasks (
        id TEXT PRIMARY KEY, run_id TEXT NOT NULL, status TEXT NOT NULL,
        completedAt TEXT, createdAt TEXT NOT NULL)`,
    ).run();
    const old = new Date(Date.now() - 2 * 3_600_000).toISOString();
    await env.DB.prepare(
      'INSERT INTO mastra_background_tasks (id, run_id, status, completedAt, createdAt) VALUES (?, ?, ?, ?, ?)',
    )
      .bind('bg1', 'acme_r1', 'completed', old, old)
      .run();
  }

  it('leaves the background-task TTL UNWIRED by default: no config.backgroundTasks, no duty', async () => {
    // #given — the opt-in posture; background tasks are unconfigured
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();
    await seedOldCompletedTask(env);

    // #when
    await worker.runMaintenanceDuty('purge', env);

    // #then — the duty never ran, byte-identical to before Track B
    const lines = maintenanceLines(logs.lines());
    expect(lines[0]).not.toHaveProperty('backgroundTasksCompletedPurged');
    expect(logs.errors()).toEqual([]);
  });

  it('config.backgroundTasks wires the background-task TTL cleanup into the purge alarm, folded into the ONE line', async () => {
    // #given — a completed task 2h old under the default 1h completed TTL
    const logs = capturedLogs();
    const worker = makeWorker({ backgroundTasks: {} });
    const { env } = makeEnv();
    await seedOldCompletedTask(env);

    // #when
    await worker.runMaintenanceDuty('purge', env);

    // #then — reaped, reported in the combined maintenance line
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({
      backgroundTasksCompletedPurged: 1,
      backgroundTasksFailedPurged: 0,
      approvalsPurged: 0,
    });
  });

  it('the SWEEP duty never runs the background-task purge', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker({ backgroundTasks: {} });
    const { env } = makeEnv();
    await seedOldCompletedTask(env);

    // #when
    await worker.runMaintenanceDuty('sweep', env);

    // #then — a CPU-limit kill is uncatchable, so the sweep carries no purge work
    const lines = maintenanceLines(logs.lines());
    expect(lines[0]).not.toHaveProperty('backgroundTasksCompletedPurged');
    expect(
      await env.DB.prepare('SELECT id FROM mastra_background_tasks').all(),
    ).toBeDefined();
  });

  it('the SWEEP duty never runs the thread purge', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.runMaintenanceDuty('sweep', {
      ...env,
      THREAD_RETENTION_DAYS: '30',
    });

    // #then — a CPU-limit kill is uncatchable, so the sweep must not carry
    // purge work; the idle thread waits for the purge alarm
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).not.toHaveProperty('threadsPurged');
  });

  it('isolates a THROWING thread purge: neither the snapshot purge, the approval purge, nor extra duties are starved', async () => {
    // #given — a DB whose mastra_threads statements THROW (not merely missing).
    // Isolating one duty while a sibling shares its failure is a defect class
    // this codebase has already shipped once.
    const logs = capturedLogs();
    const { env } = makeEnv();
    const realDb = env.DB;
    const throwingDb = {
      batch: realDb.batch?.bind(realDb),
      prepare: (query: string) => {
        if (query.includes('mastra_threads')) {
          throw new Error('threads table wedged');
        }
        return realDb.prepare(query);
      },
    } as FlowsafeWorkerEnv['DB'];
    const worker = makeWorker({
      extraPurgeDuties: async () => ({ extraDuty: 'ran' }),
    });

    // #when
    await worker.runMaintenanceDuty('purge', {
      ...env,
      DB: throwingDb,
      THREAD_RETENTION_DAYS: '30',
    });

    // #then — its own error surface, and every sibling duty still folded into
    // the one combined line
    expect(
      logs
        .errors()
        .some(
          (line) =>
            line.includes('maintenance-error') &&
            line.includes('thread-retention-purge'),
        ),
    ).toBe(true);
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({
      purged: 0,
      approvalsPurged: 0,
      extraDuty: 'ran',
    });
    expect(lines[0]?.threadsPurged).toBeUndefined();
  });

  it('a wedged SNAPSHOT purge does not starve the thread purge (the sibling direction)', async () => {
    // #given
    const logs = capturedLogs();
    const { env } = makeEnv();
    await seedIdleThread(env);
    const realDb = env.DB;
    const throwingDb = {
      batch: realDb.batch?.bind(realDb),
      prepare: (query: string) => {
        if (query.includes('mastra_workflow_snapshot')) {
          throw new Error('snapshot table wedged');
        }
        return realDb.prepare(query);
      },
    } as FlowsafeWorkerEnv['DB'];
    const worker = makeWorker();

    // #when
    await worker.runMaintenanceDuty('purge', {
      ...env,
      DB: throwingDb,
      THREAD_RETENTION_DAYS: '30',
    });

    // #then — the thread TTL ran anyway
    const lines = maintenanceLines(logs.lines());
    expect(lines[0]).toMatchObject({ threadsPurged: 1 });
  });

  it('SKIPS the duty on an EMPTY THREAD_RETENTION_DAYS rather than inventing a default', async () => {
    // #given — `''` is what an unset CI/CD variable interpolates to and what a
    // blank wrangler vars entry produces, and numberVar reads it as unset. A
    // `!== undefined` gate would admit it, silently resolve the fallback, and
    // start irreversibly deleting conversations nobody asked to expire.
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.runMaintenanceDuty('purge', {
      ...env,
      THREAD_RETENTION_DAYS: '',
    });

    // #then — inert, exactly as if unset
    expect(maintenanceLines(logs.lines())[0]).not.toHaveProperty(
      'threadsPurged',
    );
    expect(await threadIds(env)).toEqual(['acme_idle']);
  });

  it('SKIPS the duty on a typo’d THREAD_RETENTION_DAYS, logging the tripwire', async () => {
    // #given — the polarity that separates this var from RUN_RETENTION_DAYS:
    // there, falling back keeps a duty running that runs regardless; here it
    // would INVENT an irreversible delete at a threshold the operator never
    // named. Never expiring is recoverable; deleting an organization's conversations
    // because a var was mistyped is not.
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.runMaintenanceDuty('purge', {
      ...env,
      THREAD_RETENTION_DAYS: '-5',
    });

    // #then — the operator's tripwire fires and NOTHING was deleted
    expect(
      logs
        .errors()
        .some(
          (line) =>
            line.includes('config-error') &&
            line.includes('THREAD_RETENTION_DAYS'),
        ),
    ).toBe(true);
    expect(maintenanceLines(logs.lines())[0]).not.toHaveProperty(
      'threadsPurged',
    );
    expect(await threadIds(env)).toEqual(['acme_idle']);
  });

  it('isolates a THROWING extraPurgeDuties behind its own maintenance-error', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker({
      extraPurgeDuties: async () => {
        throw new Error('reaper wedged');
      },
    });
    const { env } = makeEnv();

    // #when
    const outcome = await worker.runMaintenanceDuty('purge', env);

    // #then — belt containment: the combined line still lands
    expect(
      logs
        .errors()
        .some(
          (line) =>
            line.includes('maintenance-error') &&
            line.includes('extra-purge-duties'),
        ),
    ).toBe(true);
    expect(maintenanceLines(logs.lines())).toHaveLength(1);
    expect(outcome).toEqual({
      ok: false,
      error: expect.stringContaining('reaper wedged'),
    });
  });

  it('threads config.notify into the sweep (escalations reach the transport)', async () => {
    // #given — an overdue open approval seeded straight into the store
    const logs = capturedLogs();
    const { env } = makeEnv();
    const store = approvalStoreFactoryFor(env.DB).store();
    const past = new Date(Date.now() - 60_000).toISOString();
    const record: ApprovalRecord = {
      id: 'apr-overdue',
      workflowId: 'wf',
      runId: 'acme_r1',
      title: 'overdue request',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: past,
      updatedAt: past,
      slaDeadlineAt: past,
    };
    await store.create(record);
    const notified: ApprovalNotificationEvent[] = [];
    const worker = makeWorker({
      notify: () => (event) => void notified.push(event),
    });

    // #when
    await worker.runMaintenanceDuty('sweep', env);

    // #then
    expect(notified).toEqual([
      {
        type: 'escalated',
        record: expect.objectContaining({
          id: 'apr-overdue',
          status: 'escalated',
        }),
      },
    ]);
    expect(logs.lines().length).toBeGreaterThan(0);
  });
});

describe('createFlowsafeWorker storage table prefix', () => {
  const PREFIX = 'tenant_';
  const OLD_ISO = new Date(Date.now() - 90 * 86_400_000).toISOString();

  async function seedMaintenanceDomains(
    env: FlowsafeWorkerEnv,
    prefix: string,
    suffix: string,
  ): Promise<void> {
    await env.DB.prepare(
      `CREATE TABLE ${prefix}mastra_workflow_snapshot (
        workflow_name TEXT NOT NULL,
        run_id TEXT NOT NULL,
        resourceId TEXT,
        snapshot TEXT NOT NULL,
        createdAt TEXT NOT NULL,
        updatedAt TEXT NOT NULL
      )`,
    ).run();
    await env.DB.prepare(
      `INSERT INTO ${prefix}mastra_workflow_snapshot
       (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
       VALUES (?, ?, NULL, ?, ?, ?)`,
    )
      .bind(
        'wf',
        `run-${suffix}`,
        JSON.stringify({ status: 'success' }),
        OLD_ISO,
        OLD_ISO,
      )
      .run();
    await env.DB.prepare(
      `CREATE TABLE ${prefix}mastra_threads (
        id TEXT PRIMARY KEY,
        updatedAt TEXT NOT NULL
      )`,
    ).run();
    await env.DB.prepare(
      `CREATE TABLE ${prefix}mastra_messages (
        id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        createdAt TEXT NOT NULL
      )`,
    ).run();
    await env.DB.prepare(
      `INSERT INTO ${prefix}mastra_threads (id, updatedAt) VALUES (?, ?)`,
    )
      .bind(`thread-${suffix}`, OLD_ISO)
      .run();
    await env.DB.prepare(
      `INSERT INTO ${prefix}mastra_messages (id, thread_id, createdAt)
       VALUES (?, ?, ?)`,
    )
      .bind(`message-${suffix}`, `thread-${suffix}`, OLD_ISO)
      .run();
    await env.DB.prepare(
      `CREATE TABLE ${prefix}mastra_background_tasks (
        id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL,
        status TEXT NOT NULL,
        completedAt TEXT,
        createdAt TEXT NOT NULL
      )`,
    ).run();
    await env.DB.prepare(
      `INSERT INTO ${prefix}mastra_background_tasks
       (id, run_id, status, completedAt, createdAt)
       VALUES (?, ?, 'completed', ?, ?)`,
    )
      .bind(`task-${suffix}`, `task-run-${suffix}`, OLD_ISO, OLD_ISO)
      .run();
    await env.DB.prepare(
      `CREATE TABLE ${prefix}mastra_notifications (
        id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        updatedAt TEXT NOT NULL
      )`,
    ).run();
    await env.DB.prepare(
      `INSERT INTO ${prefix}mastra_notifications (id, status, updatedAt)
       VALUES (?, 'delivered', ?)`,
    )
      .bind(`notification-${suffix}`, OLD_ISO)
      .run();
    await env.DB.prepare(
      `CREATE TABLE ${prefix}mastra_thread_state (
        id TEXT PRIMARY KEY,
        updatedAt TEXT NOT NULL
      )`,
    ).run();
    await env.DB.prepare(
      `INSERT INTO ${prefix}mastra_thread_state (id, updatedAt) VALUES (?, ?)`,
    )
      .bind(`state-${suffix}`, OLD_ISO)
      .run();
    await env.DB.prepare(
      `CREATE TABLE ${prefix}mastra_schedule_triggers (
        id TEXT PRIMARY KEY,
        actualFireAt INTEGER NOT NULL,
        outcome TEXT NOT NULL
      )`,
    ).run();
    await env.DB.prepare(
      `INSERT INTO ${prefix}mastra_schedule_triggers
       (id, actualFireAt, outcome) VALUES (?, ?, 'success')`,
    )
      .bind(`trigger-${suffix}`, Date.now() - 90 * 86_400_000)
      .run();
  }

  async function ids(
    env: FlowsafeWorkerEnv,
    table: string,
    idColumn = 'id',
  ): Promise<string[]> {
    const { results } = await env.DB.prepare(
      `SELECT ${idColumn} AS id FROM ${table} ORDER BY ${idColumn}`,
    ).all<{ id: string }>();
    return results.map((row) => row.id);
  }

  async function runEveryPurge(
    env: FlowsafeWorkerEnv,
    storageTablePrefix?: string,
  ): Promise<void> {
    const worker = makeWorker({
      storageTablePrefix,
      backgroundTasks: {},
    });
    await worker.runMaintenanceDuty('purge', {
      ...env,
      THREAD_RETENTION_DAYS: '30',
      NOTIFICATION_RETENTION_DAYS: '30',
      THREAD_STATE_RETENTION_DAYS: '30',
      SCHEDULE_TRIGGER_RETENTION_DAYS: '30',
    });
  }

  async function expectDomains(
    env: FlowsafeWorkerEnv,
    prefix: string,
    suffix: string,
  ): Promise<void> {
    expect(
      await ids(env, `${prefix}mastra_workflow_snapshot`, 'run_id'),
    ).toEqual([`run-${suffix}`]);
    expect(await ids(env, `${prefix}mastra_threads`)).toEqual([
      `thread-${suffix}`,
    ]);
    expect(await ids(env, `${prefix}mastra_messages`)).toEqual([
      `message-${suffix}`,
    ]);
    expect(await ids(env, `${prefix}mastra_background_tasks`)).toEqual([
      `task-${suffix}`,
    ]);
    expect(await ids(env, `${prefix}mastra_notifications`)).toEqual([
      `notification-${suffix}`,
    ]);
    expect(await ids(env, `${prefix}mastra_thread_state`)).toEqual([
      `state-${suffix}`,
    ]);
    expect(await ids(env, `${prefix}mastra_schedule_triggers`)).toEqual([
      `trigger-${suffix}`,
    ]);
  }

  async function expectDomainsEmpty(
    env: FlowsafeWorkerEnv,
    prefix: string,
  ): Promise<void> {
    expect(
      await ids(env, `${prefix}mastra_workflow_snapshot`, 'run_id'),
    ).toEqual([]);
    expect(await ids(env, `${prefix}mastra_threads`)).toEqual([]);
    expect(await ids(env, `${prefix}mastra_messages`)).toEqual([]);
    expect(await ids(env, `${prefix}mastra_background_tasks`)).toEqual([]);
    expect(await ids(env, `${prefix}mastra_notifications`)).toEqual([]);
    expect(await ids(env, `${prefix}mastra_thread_state`)).toEqual([]);
    expect(await ids(env, `${prefix}mastra_schedule_triggers`)).toEqual([]);
  }

  it('targets all six prefix-aware maintenance domains and leaves unprefixed twins untouched', async () => {
    capturedLogs();
    const { env } = makeEnv();
    await seedMaintenanceDomains(env, '', 'plain');
    await seedMaintenanceDomains(env, PREFIX, 'prefixed');

    await runEveryPurge(env, PREFIX);

    await expectDomains(env, '', 'plain');
    await expectDomainsEmpty(env, PREFIX);
  });

  it('keeps the existing unprefixed behavior by default', async () => {
    capturedLogs();
    const { env } = makeEnv();
    await seedMaintenanceDomains(env, '', 'plain');
    await seedMaintenanceDomains(env, PREFIX, 'prefixed');

    await runEveryPurge(env);

    await expectDomainsEmpty(env, '');
    await expectDomains(env, PREFIX, 'prefixed');
  });
});

describe('createFlowsafeWorker schedule tick duty (Track D)', () => {
  it('the tick duty runs ONLY the schedule tick, not the sweep/purge', async () => {
    // #given a worker with a tick interval + a scheduleTick builder
    const logs = capturedLogs();
    const tickFn = vi.fn(async () => ({
      due: 1,
      fired: 1,
      skipped: 0,
      failed: 0,
      lost: 0,
    }));
    const worker = makeWorker({
      maintenance: {
        sweepIntervalMs: 15 * 60 * 1_000,
        purgeIntervalMs: 60 * 60 * 1_000,
        tickIntervalMs: 2 * 60 * 1_000,
      },
      scheduleTick: () => tickFn,
    });
    const { env } = makeEnv();

    const outcome = await worker.runMaintenanceDuty('tick', env);

    // #then the tick ran once, logged its own line, and NO maintenance ran
    expect(tickFn).toHaveBeenCalledTimes(1);
    const tickLines = logs
      .lines()
      .map((l) => {
        try {
          return JSON.parse(l) as Record<string, unknown>;
        } catch {
          return undefined;
        }
      })
      .filter((p): p is Record<string, unknown> => p?.type === 'schedule-tick');
    expect(tickLines).toHaveLength(1);
    expect(tickLines[0]?.result).toMatchObject({ fired: 1 });
    expect(logs.lines().some((l) => l.includes('"type":"maintenance"'))).toBe(
      false,
    );
    expect(outcome).toEqual({ ok: true, value: undefined });
  });

  it('a tick duty with NO scheduleTick builder logs a config-error, runs nothing else', async () => {
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env } = makeEnv();

    const outcome = await worker.runMaintenanceDuty('tick', env);

    // #then a config-error naming the tick duty; NO other maintenance (the invocation is
    // the tick's — it does not fall through to sweep/purge)
    expect(logs.errors().some((l) => l.includes('config-error'))).toBe(true);
    expect(logs.lines().some((l) => l.includes('"type":"maintenance"'))).toBe(
      false,
    );
    expect(outcome).toEqual({
      ok: false,
      error: expect.stringContaining('no scheduleTick builder'),
    });
  });

  it('a throwing tick is CONTAINED (schedule-tick-error), never an unhandled rejection', async () => {
    // #given a tick that throws
    const logs = capturedLogs();
    const worker = makeWorker({
      maintenance: {
        sweepIntervalMs: 15 * 60 * 1_000,
        purgeIntervalMs: 60 * 60 * 1_000,
        tickIntervalMs: 2 * 60 * 1_000,
      },
      scheduleTick: () => async () => {
        throw new Error('D1 down');
      },
    });
    const { env } = makeEnv();

    // #when
    const outcome = await worker.runMaintenanceDuty('tick', env);

    // #then the error was contained + logged
    expect(
      logs
        .errors()
        .some(
          (l) => l.includes('schedule-tick-error') && l.includes('D1 down'),
        ),
    ).toBe(true);
    expect(outcome).toEqual({
      ok: false,
      error: expect.stringContaining('D1 down'),
    });
  });

  it('the purge duty never invokes a configured schedule tick', async () => {
    const logs = capturedLogs();
    const tickFn = vi.fn();
    const worker = makeWorker({
      maintenance: {
        sweepIntervalMs: 15 * 60 * 1_000,
        purgeIntervalMs: 60 * 60 * 1_000,
        tickIntervalMs: 2 * 60 * 1_000,
      },
      scheduleTick: () => tickFn,
    });
    const { env } = makeEnv();

    // #when the purge duty runs while the tick builder is present
    await worker.runMaintenanceDuty('purge', env);

    // #then the tick was never invoked; the purge ran as before
    expect(tickFn).not.toHaveBeenCalled();
    expect(logs.lines().some((l) => l.includes('"type":"maintenance"'))).toBe(
      true,
    );
  });
});
