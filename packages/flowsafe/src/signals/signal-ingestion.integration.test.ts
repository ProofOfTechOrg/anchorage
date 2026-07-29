// SPDX-License-Identifier: Apache-2.0
// Track C (M-004) integration (SHOULD-FIX): one signal ingested through the FULL
// chain — createSignalRouter (the P6 gate) → real createThreadTopology → real
// ThreadDurableObject (its tenant-identity assertion) → the production thread
// signal routes → a runtime-driven reserve agent — with NO LLM. The unit suites
// each mock a seam; this one wires the real seams together so the ingestion
// boundary has one end-to-end proof, including the idle-wake run cap consulted
// both allowing and capping, and a foreign-threadId send failing closed at the
// topology before any DO is addressed.

import type { DurableObjectState } from '@cloudflare/workers-types';
import type { Agent } from '@mastra/core/agent';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import { RUNTIME_DRIVEN_AGENT } from '../agent-runner/index.js';
import type { ApprovalActor, TenantContext } from '../approval-api/index.js';
import { humanPrincipal } from '../approval-api/index.js';
import {
  createD1Storage,
  type InitResult,
  init,
  mintResourceId,
  mintThreadId,
  purgeTenant,
  type SnapshotDatabase,
  ThreadDurableObject,
  type ThreadScope,
} from '../do-runner/index.js';
import {
  createThreadTopology,
  type ThreadNamespaceLike,
} from '../host-kit/index.js';
import { createSignalRouter } from './router.js';
import {
  createThreadSignalRoutes,
  type RunCapConsult,
  type StartIdleRun,
} from './thread-do-routes.js';

interface TestEnv {
  agent: Agent;
  consultRunCap?: RunCapConsult;
  startIdleRun?: StartIdleRun;
}

// A minimal host thread DO: build() its init() wiring, route() the PRODUCTION
// signal routes over the env's reserve agent + run cap. The base class asserts
// the request's authenticated tenant against the name's prefix before route()
// runs, so reaching #routes at all proves that assertion passed.
class TestThread extends ThreadDurableObject<TestEnv> {
  #routes = createThreadSignalRoutes({
    resolveAgent: () => this.env.agent,
    resolveResourceId: (scope: ThreadScope) =>
      mintResourceId(scope.tenantId, 'itest'),
    consultRunCap: this.env.consultRunCap,
    startIdleRun: this.env.startIdleRun,
  });

  protected build(): InitResult {
    return init({ storage: new InMemoryStore() });
  }

  protected async route(
    request: Request,
    scope: ThreadScope,
  ): Promise<Response> {
    return (
      (await this.#routes(request, scope)) ??
      new Response(JSON.stringify({ error: 'not found' }), { status: 404 })
    );
  }
}

// A namespace over in-memory TestThread instances: idFromName(name)=name and
// get() memoizes one instance per thread name — its DO identity IS its id.name,
// exactly what the base class reads to assert the tenant.
function threadNamespace(env: TestEnv): ThreadNamespaceLike<string> {
  const instances = new Map<string, TestThread>();
  return {
    idFromName: (name) => name,
    get: (name) => {
      let inst = instances.get(name);
      if (!inst) {
        inst = new TestThread(
          { id: { name } } as unknown as DurableObjectState,
          env,
        );
        instances.set(name, inst);
      }
      const instance = inst;
      return {
        fetch: (
          input: Request | string,
          reqInit?: {
            method?: string;
            headers?: Record<string, string>;
            body?: string;
          },
        ) =>
          instance.fetch(
            typeof input === 'string' ? new Request(input, reqInit) : input,
          ),
      };
    },
  };
}

const THREAD_ID = mintThreadId('acme', () => 'itest'); // 'acme_itest'

function tenantCtx(): TenantContext {
  const actor: ApprovalActor = {
    id: 'opal',
    role: 'operator',
    tenantId: 'acme',
  };
  return {
    actor,
    principal: humanPrincipal(actor),
    tenantId: 'acme',
    ownsMemoryId: (id: string) => id === THREAD_ID,
  } as unknown as TenantContext;
}

// A runtime-driven reserve agent (no LLM): records the ifIdle target sendMessage
// received. The brand is what lets a wake pass the thread-route gate.
function reserveAgent(): {
  agent: Agent;
  targets: Array<{ ifIdle?: unknown }>;
} {
  const targets: Array<{ ifIdle?: unknown }> = [];
  const agent = {
    [RUNTIME_DRIVEN_AGENT]: true,
    __setPubSub: () => {},
    sendMessage: (_message: unknown, target: { ifIdle?: unknown }) => {
      targets.push(target);
      return {
        signal: { id: 'sig-1' },
        accepted: Promise.resolve({ action: 'deliver', runId: 'acme_run' }),
      };
    },
  } as unknown as Agent;
  return { agent, targets };
}

function wake(threadId: string): Request {
  return new Request(`http://host/api/threads/${threadId}/message`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ contents: 'hi', ifIdle: 'wake' }),
  });
}

describe('signal ingestion — full chain (router → topology → thread DO → agent)', () => {
  it('drives an idle WAKE through the whole chain and consults the run cap (allowed)', async () => {
    // #given
    const { agent, targets } = reserveAgent();
    const consultRunCap = vi.fn(async () => true);
    const startIdleRun = vi.fn(async ({ runId }) => ({ runId }));
    const topology = createThreadTopology(
      threadNamespace({ agent, consultRunCap, startIdleRun }),
    );
    const router = createSignalRouter({
      resolve: async () => tenantCtx(),
      topology,
    });

    // #when
    const res = await router(wake(THREAD_ID));

    // #then — the wake reached the thread routes THROUGH the DO's identity
    // assertion (which passes only because the topology stamped the header from
    // the resolved tenant), the run cap was consulted, and the runtime start
    // seam got a tenant-salted run id without asking core to mint one.
    expect(res?.status).toBe(200);
    expect(consultRunCap).toHaveBeenCalledWith('acme');
    expect(startIdleRun).toHaveBeenCalledWith(
      expect.objectContaining({ runId: expect.stringMatching(/^acme_/) }),
    );
    expect(targets).toHaveLength(0);
  });

  it("reaps the real snapshot created by an idle wake during the tenant's offboarding purge", async () => {
    // #given — a real D1-backed runtime behind the host-owned idle-start seam
    const sqlite = openSqlite();
    const binding = d1DatabaseLike(sqlite);
    const storage = createD1Storage({ binding: binding as never });
    const { createWorkflow, createStep, runtime } = init({ storage });
    const complete = createStep({
      id: 'complete',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    createWorkflow({
      id: 'idle-wake',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(complete)
      .commit();

    const { agent } = reserveAgent();
    let startedRunId: string | undefined;
    const startIdleRun: StartIdleRun = async ({ runId }) => {
      startedRunId = runId;
      return runtime.start('idle-wake', { runId, inputData: {} });
    };
    const topology = createThreadTopology(
      threadNamespace({
        agent,
        consultRunCap: async () => true,
        startIdleRun,
      }),
    );
    const router = createSignalRouter({
      resolve: async () => tenantCtx(),
      topology,
    });

    // #when — the production wake path mints the run id and persists its
    // workflow snapshot through the real Mastra D1 adapter
    const res = await router(wake(THREAD_ID));

    // #then — INV-1 places that exact row inside purgeTenant's range
    expect(res?.status).toBe(200);
    expect(startedRunId).toMatch(/^acme_/);
    expect(
      sqlite
        .prepare('SELECT run_id FROM mastra_workflow_snapshot WHERE run_id = ?')
        .all(startedRunId),
    ).toEqual([{ run_id: startedRunId }]);

    const purged = await purgeTenant(binding as SnapshotDatabase, {
      tenantId: 'acme',
    });
    expect(purged.snapshots).toBe(1);
    expect(
      sqlite.prepare('SELECT run_id FROM mastra_workflow_snapshot').all(),
    ).toEqual([]);
  });

  it('degrades the wake to persist when the tenant is over its run cap', async () => {
    // #given — the cap refuses
    const { agent, targets } = reserveAgent();
    const consultRunCap = vi.fn(async () => false);
    const startIdleRun = vi.fn(async ({ runId }) => ({ runId }));
    const topology = createThreadTopology(
      threadNamespace({ agent, consultRunCap, startIdleRun }),
    );
    const router = createSignalRouter({
      resolve: async () => tenantCtx(),
      topology,
    });

    // #when
    const res = await router(wake(THREAD_ID));

    // #then — over cap, so the agent received a durable persist, not a wake
    expect(res?.status).toBe(200);
    expect((await res?.json()) as { capped: boolean }).toMatchObject({
      capped: true,
    });
    expect(consultRunCap).toHaveBeenCalledWith('acme');
    expect(targets[0]?.ifIdle).toEqual({ behavior: 'persist' });
    expect(startIdleRun).not.toHaveBeenCalled();
  });

  it('404s a foreign threadId at the topology — the DO is never addressed', async () => {
    // #given — a threadId acme does not own
    const { agent } = reserveAgent();
    const consultRunCap = vi.fn(async () => true);
    const topology = createThreadTopology(
      threadNamespace({ agent, consultRunCap }),
    );
    const router = createSignalRouter({
      resolve: async () => tenantCtx(),
      topology,
    });

    // #when
    const res = await router(wake('other_t9'));

    // #then — refused at the topology ownership check, before the DO or its routes
    expect(res?.status).toBe(404);
    expect(consultRunCap).not.toHaveBeenCalled();
  });
});
