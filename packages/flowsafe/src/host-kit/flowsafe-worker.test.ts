// SPDX-License-Identifier: Apache-2.0
// Unit proof for the composed Worker skeleton: the fetch pipeline order, the
// hook seams (preRoutes/wrapResolve/wrapStart/wrapResume/notify/extra
// duties), and the two-cron dispatch with per-duty failure isolation. The
// HEAVYWEIGHT behavior proof stays the two host e2e suites
// (deploy/worker.e2e.test.ts and the showcase worker e2e set), which drive
// the real hosts through this same composer — this file covers the composer's
// own contract over fakes: real SQLite behind the D1 adapter, a stub DO
// namespace, and a static verifier.

import { afterEach, describe, expect, it, vi } from 'vitest';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import type {
  ApprovalNotificationEvent,
  ApprovalRecord,
} from '../approval-api/index.js';
import type { RunSummary } from '../do-runner/index.js';
import {
  createFlowsafeWorker,
  type FlowsafeWorkerConfig,
  type FlowsafeWorkerEnv,
} from './flowsafe-worker.js';
import { approvalStoreFactoryFor } from './host-approval-service.js';
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

const SWEEP = '*/15 * * * *';
const PURGE = '7 * * * *';

const ACTORS = new Map([
  ['tok-ada', { id: 'ada', role: 'admin', tenantId: 'acme' } as const],
]);

function successSummary(runId: string): RunSummary {
  return { runId, status: 'success', result: { ok: true } };
}

/** A DO namespace whose stub echoes a success summary for the posted run. */
function fakeRunner(calls: string[]): FlowsafeWorkerEnv['RUNNER'] {
  return {
    idFromName: (name: string) => name,
    get: () => ({
      fetch: async (url: string, init?: { body?: string }) => {
        calls.push(url);
        const body = init?.body
          ? (JSON.parse(init.body) as { runId?: string })
          : {};
        const runId =
          body.runId ?? /\/runs\/[^/]+\/([^/]+)/.exec(url)?.[1] ?? 'acme_x';
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
  return {
    env: {
      DB: d1DatabaseLike(openSqlite()) as FlowsafeWorkerEnv['DB'],
      RUNNER: fakeRunner(doCalls),
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
    systemActorId: 'composer-system',
    buildVerifier: () => staticTokenVerifier(ACTORS),
    crons: { sweep: SWEEP, purge: PURGE },
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

  it('applies wrapResolve to the resolver both routers use', async () => {
    // #given
    const resolved: string[] = [];
    const worker = makeWorker({
      wrapResolve: (resolve) => async (request) => {
        resolved.push(new URL(request.url).pathname);
        return resolve(request);
      },
    });
    const { env, ctx } = makeEnv();

    // #when
    await worker.fetch(authed('http://host/api/approvals'), env, ctx);
    await worker.fetch(authed('http://host/workflows'), env, ctx);

    // #then — the wrapped resolver saw both surfaces
    expect(resolved).toEqual(['/api/approvals', '/workflows']);
  });

  it('wrapStart and wrapResume actually wrap the topology thunks', async () => {
    // #given
    const wrapped: string[] = [];
    const worker = makeWorker({
      wrapStart: (start) => async (workflowId, runId, inputData) => {
        wrapped.push(`start:${workflowId}`);
        return start(workflowId, runId, inputData);
      },
      wrapResume: (resume) => async (workflowId, runId, body) => {
        wrapped.push(`resume:${workflowId}:${runId}`);
        return resume(workflowId, runId, body);
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
    expect(summary.runId.startsWith('acme_')).toBe(true);
    expect(wrapped).toEqual(['start:wf']);
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
    expect(wrapped).toEqual(['start:wf', `resume:wf:${summary.runId}`]);
  });
});

describe('createFlowsafeWorker scheduled dispatch', () => {
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

  it('the sweep cron runs ONLY the sweep', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env, ctx, flush } = makeEnv();

    // #when
    await worker.scheduled({ cron: SWEEP }, env, ctx);
    await flush();

    // #then — one maintenance line, the sweep's (escalated, never purged)
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toHaveProperty('escalated');
    expect(lines[0]).not.toHaveProperty('purged');
  });

  it('the purge cron runs ONLY the purge duties', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env, ctx, flush } = makeEnv();

    // #when
    await worker.scheduled({ cron: PURGE }, env, ctx);
    await flush();

    // #then
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ purged: 0, approvalsPurged: 0 });
    expect(lines[0]).not.toHaveProperty('escalated');
  });

  it('an unknown cron runs BOTH duties and logs the config-error tripwire', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env, ctx, flush } = makeEnv();

    // #when
    await worker.scheduled({ cron: '* * * * *' }, env, ctx);
    await flush();

    // #then
    expect(
      logs
        .errors()
        .some(
          (line) =>
            line.includes('config-error') && line.includes('triggers.crons'),
        ),
    ).toBe(true);
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(2);
  });

  it('isolates purge-duty failures: a broken snapshot purge stops neither the approval purge nor extra duties', async () => {
    // #given — DB whose snapshot-table statements THROW (not merely missing)
    const logs = capturedLogs();
    const { env, ctx, flush } = makeEnv();
    const realDb = env.DB;
    const throwingDb = {
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
    await worker.scheduled({ cron: PURGE }, { ...env, DB: throwingDb }, ctx);
    await flush();

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

  // The agent-memory thread TTL (docs/agent-memory-tenancy.md item 7) as the
  // purge cron's third duty. Seeds the two memory tables the real
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
    const { env, ctx, flush } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.scheduled({ cron: PURGE }, env, ctx);
    await flush();

    // #then — the duty never ran: nothing in the log, nothing deleted
    const lines = maintenanceLines(logs.lines());
    expect(lines[0]).not.toHaveProperty('threadsPurged');
    expect(logs.errors()).toEqual([]);
  });

  it('THREAD_RETENTION_DAYS wires the thread purge into the purge cron, folded into the ONE maintenance line', async () => {
    // #given — an agent-memory thread idle for 90 days under a 30-day TTL
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env, ctx, flush } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.scheduled(
      { cron: PURGE },
      { ...env, THREAD_RETENTION_DAYS: '30' },
      ctx,
    );
    await flush();

    // #then — reaped WITH its messages, reported in the combined line
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({
      threadsPurged: 1,
      threadMessagesPurged: 1,
      approvalsPurged: 0,
    });
  });

  it('the SWEEP cron never runs the thread purge (the two-cron split holds)', async () => {
    // #given
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env, ctx, flush } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.scheduled(
      { cron: SWEEP },
      { ...env, THREAD_RETENTION_DAYS: '30' },
      ctx,
    );
    await flush();

    // #then — a CPU-limit kill is uncatchable, so the sweep must not carry
    // purge work; the idle thread waits for the purge cron
    const lines = maintenanceLines(logs.lines());
    expect(lines).toHaveLength(1);
    expect(lines[0]).not.toHaveProperty('threadsPurged');
  });

  it('isolates a THROWING thread purge: neither the snapshot purge, the approval purge, nor extra duties are starved', async () => {
    // #given — a DB whose mastra_threads statements THROW (not merely missing).
    // Isolating one duty while a sibling shares its failure is a defect class
    // this codebase has already shipped once.
    const logs = capturedLogs();
    const { env, ctx, flush } = makeEnv();
    const realDb = env.DB;
    const throwingDb = {
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
    await worker.scheduled(
      { cron: PURGE },
      { ...env, DB: throwingDb, THREAD_RETENTION_DAYS: '30' },
      ctx,
    );
    await flush();

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
    const { env, ctx, flush } = makeEnv();
    await seedIdleThread(env);
    const realDb = env.DB;
    const throwingDb = {
      prepare: (query: string) => {
        if (query.includes('mastra_workflow_snapshot')) {
          throw new Error('snapshot table wedged');
        }
        return realDb.prepare(query);
      },
    } as FlowsafeWorkerEnv['DB'];
    const worker = makeWorker();

    // #when
    await worker.scheduled(
      { cron: PURGE },
      { ...env, DB: throwingDb, THREAD_RETENTION_DAYS: '30' },
      ctx,
    );
    await flush();

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
    const { env, ctx, flush } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.scheduled(
      { cron: PURGE },
      { ...env, THREAD_RETENTION_DAYS: '' },
      ctx,
    );
    await flush();

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
    // named. Never expiring is recoverable; deleting a tenant's conversations
    // because a var was mistyped is not.
    const logs = capturedLogs();
    const worker = makeWorker();
    const { env, ctx, flush } = makeEnv();
    await seedIdleThread(env);

    // #when
    await worker.scheduled(
      { cron: PURGE },
      { ...env, THREAD_RETENTION_DAYS: '-5' },
      ctx,
    );
    await flush();

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
    const { env, ctx, flush } = makeEnv();

    // #when
    await worker.scheduled({ cron: PURGE }, env, ctx);
    await flush();

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
  });

  it('threads config.notify into the sweep (escalations reach the transport)', async () => {
    // #given — an overdue open approval seeded straight into the store
    const logs = capturedLogs();
    const { env, ctx, flush } = makeEnv();
    const store = approvalStoreFactoryFor(env.DB).forTenant('acme');
    const past = new Date(Date.now() - 60_000).toISOString();
    const record: ApprovalRecord = {
      id: 'apr-overdue',
      tenantId: 'acme',
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
    await worker.scheduled({ cron: SWEEP }, env, ctx);
    await flush();

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
