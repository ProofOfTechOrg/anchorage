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

import { RUNTIME_DRIVEN_AGENT } from '../agent-runner/index.js';
import type { ApprovalActor, TenantContext } from '../approval-api/index.js';
import {
  type InitResult,
  init,
  mintResourceId,
  mintThreadId,
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
} from './thread-do-routes.js';

interface TestEnv {
  agent: Agent;
  consultRunCap?: RunCapConsult;
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
    const topology = createThreadTopology(
      threadNamespace({ agent, consultRunCap }),
    );
    const router = createSignalRouter({
      resolve: async () => tenantCtx(),
      topology,
    });

    // #when
    const res = await router(wake(THREAD_ID));

    // #then — the wake reached the thread routes THROUGH the DO's identity
    // assertion (which passes only because the topology stamped the header from
    // the resolved tenant), the run cap was consulted, and the agent got a wake.
    expect(res?.status).toBe(200);
    expect(consultRunCap).toHaveBeenCalledWith('acme');
    expect(targets[0]?.ifIdle).toEqual({ behavior: 'wake' });
  });

  it('degrades the wake to persist when the tenant is over its run cap', async () => {
    // #given — the cap refuses
    const { agent, targets } = reserveAgent();
    const consultRunCap = vi.fn(async () => false);
    const topology = createThreadTopology(
      threadNamespace({ agent, consultRunCap }),
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
