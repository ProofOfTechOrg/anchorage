// SPDX-License-Identifier: Apache-2.0
// Track C (M-004) integration (SHOULD-FIX): one signal ingested through the FULL
// chain — createSignalRouter (the P6 gate) → real createThreadTopology → real
// ThreadDurableObject (its stamped-principal assertion) → the production thread
// signal routes → a runtime-driven reserve agent — with NO LLM. The unit suites
// each mock a seam; this one wires the real seams together so the ingestion
// boundary has one end-to-end proof, including the idle-wake run cap consulted
// both allowing and capping, plus a foreign path-safe thread refusal.

import type { Agent } from '@mastra/core/agent';
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore } from '@mastra/core/storage';
import {
  ACTOR_CONTEXT_KEY,
  AGENT_AUDIT_CONTEXT_KEY,
  AuditLogger,
  createContentPolicyGate,
  denyPatterns,
} from '@proofoftech/breakwater';
import { describe, expect, it, vi } from 'vitest';

import { RUNTIME_DRIVEN_AGENT } from '../agent-runner/index.js';
import type { ActorContext, ApprovalActor } from '../approval-api/index.js';
import {
  breakwaterActorFor,
  humanPrincipal,
  principalAuditFields,
} from '../approval-api/index.js';
import {
  type InitResult,
  init,
  mintThreadId,
  resourceIdFromKey,
  ThreadDurableObject,
  type ThreadScope,
} from '../do-runner/index.js';
import {
  createThreadTopology as createThreadTopologyWithSecret,
  type ThreadNamespaceLike,
  type ThreadTopology,
} from '../host-kit/index.js';
import { createSignalRouter } from './router.js';
import {
  createThreadSignalRoutes,
  type RunCapConsult,
  type SignalContentPolicy,
  type SignalContentPolicyInput,
  type StartIdleRun,
} from './thread-do-routes.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

function createThreadTopology<Id>(
  namespace: ThreadNamespaceLike<Id>,
): ThreadTopology {
  return createThreadTopologyWithSecret(namespace, DEPLOYMENT_IDENTITY_SECRET);
}

interface TestEnv {
  agent: Agent;
  consultRunCap?: RunCapConsult;
  startIdleRun?: StartIdleRun;
  contentPolicy?: SignalContentPolicy;
}

// A minimal host thread DO: build() its init() wiring, route() the PRODUCTION
// signal routes over the env's reserve agent + run cap. The base class refuses
// a request without the topology-stamped execution principal before route().
class TestThread extends ThreadDurableObject<TestEnv> {
  readonly #threadName: string;

  constructor(threadName: string, env: TestEnv) {
    super(undefined, env);
    this.#threadName = threadName;
  }

  protected override get threadId(): string {
    return this.#threadName;
  }

  #routes = createThreadSignalRoutes({
    resolveAgent: () => this.env.agent,
    resolveResourceId: () => resourceIdFromKey('itest'),
    consultRunCap: this.env.consultRunCap,
    startIdleRun: this.env.startIdleRun,
    ...(this.env.contentPolicy !== undefined
      ? { contentPolicy: this.env.contentPolicy }
      : {}),
  });

  protected build(): InitResult {
    return init({ storage: new InMemoryStore() }, { executionFence: 'none' });
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
// get() memoizes one instance per thread name — its DO identity is its id.name,
// exactly what the base class uses as the authoritative thread address.
function threadNamespace(env: TestEnv): ThreadNamespaceLike<string> {
  const instances = new Map<string, TestThread>();
  return {
    idFromName: (name) => name,
    get: (name) => {
      let inst = instances.get(name);
      if (!inst) {
        inst = new TestThread(name, env);
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

const THREAD_ID = mintThreadId(() => 'itest');

function actorContext(): ActorContext {
  const actor: ApprovalActor = {
    id: 'opal',
    role: 'operator',
  };
  return {
    actor,
    principal: humanPrincipal(actor),
    resourceOwner: { kind: 'human', id: actor.id },
    service: () => {
      throw new Error('approval service is not used in signal tests');
    },
    newRunId: () => 'run-1',
    newThreadId: () => THREAD_ID,
    resourceIdFromKey: resourceIdFromKey,
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async (kind, id) =>
      kind === 'thread' && id === THREAD_ID,
    canSelfDecide: () => false,
  };
}

// A runtime-driven reserve agent (no LLM): records the ifIdle target sendMessage
// received. The brand is what lets a wake pass the thread-route gate.
function reserveAgent(): {
  agent: Agent;
  targets: Array<{ ifIdle?: unknown }>;
} {
  const targets: Array<{ ifIdle?: unknown }> = [];
  const agent = {
    id: 'reserve',
    [RUNTIME_DRIVEN_AGENT]: true,
    __setPubSub: () => {},
    getMemory: () => ({ saveMessages: vi.fn() }),
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
      resolve: async () => actorContext(),
      topology,
    });

    // #when
    const res = await router(wake(THREAD_ID));

    // #then — the wake reached the thread routes through the DO's principal
    // assertion, the run cap was consulted, and the runtime start seam got a
    // host-minted run id.
    expect(res?.status).toBe(200);
    expect(consultRunCap).toHaveBeenCalledWith();
    expect(startIdleRun).toHaveBeenCalledWith(
      expect.objectContaining({ runId: expect.any(String) }),
    );
    expect(targets).toHaveLength(0);
  });

  it('degrades the wake to persist when the deployment is over its run cap', async () => {
    // #given — the cap refuses
    const { agent, targets } = reserveAgent();
    const consultRunCap = vi.fn(async () => false);
    const startIdleRun = vi.fn(async ({ runId }) => ({ runId }));
    const topology = createThreadTopology(
      threadNamespace({ agent, consultRunCap, startIdleRun }),
    );
    const router = createSignalRouter({
      resolve: async () => actorContext(),
      topology,
    });

    // #when
    const res = await router(wake(THREAD_ID));

    // #then — over cap, so the agent received a durable persist, not a wake
    expect(res?.status).toBe(200);
    expect((await res?.json()) as { capped: boolean }).toMatchObject({
      capped: true,
    });
    expect(consultRunCap).toHaveBeenCalledWith();
    expect(targets[0]?.ifIdle).toEqual({ behavior: 'persist' });
    expect(startIdleRun).not.toHaveBeenCalled();
  });

  it("404s another actor's path-safe thread before waking its DO", async () => {
    const { agent } = reserveAgent();
    const consultRunCap = vi.fn(async () => true);
    const topology = createThreadTopology(
      threadNamespace({ agent, consultRunCap }),
    );
    const router = createSignalRouter({
      resolve: async () => actorContext(),
      topology,
    });

    // #when
    const res = await router(wake('other_t9'));

    expect(res?.status).toBe(404);
    expect(consultRunCap).not.toHaveBeenCalled();
  });
});

// The cross-package seam, wired the way the FlowSafe README documents it: a
// REAL Breakwater content gate behind FlowSafe's structural callback, driven
// through the REAL router → topology → thread DO → routes chain. FlowSafe keeps
// no runtime dependency on Breakwater; this proves the adapter in between
// actually carries text and trusted identity across the boundary.
describe('signal ingestion — Breakwater content gate over the full chain', () => {
  function signalPolicyContext(input: SignalContentPolicyInput) {
    const requestContext = new RequestContext();
    requestContext.set(ACTOR_CONTEXT_KEY, breakwaterActorFor(input.principal));
    requestContext.set(AGENT_AUDIT_CONTEXT_KEY, {
      agentId: input.agentId,
      ...(input.deploymentTag === undefined
        ? {}
        : { tenantId: input.deploymentTag }),
      ...(input.runId === undefined ? {} : { runId: input.runId }),
      threadId: input.threadId,
      ...(input.resourceId === undefined
        ? {}
        : { resourceId: input.resourceId }),
      entryPath: input.entryPath,
      ...principalAuditFields(input.principal),
    });
    return requestContext;
  }

  function guardedEnv(agent: Agent, startIdleRun: StartIdleRun) {
    const audit = new AuditLogger();
    const inspectContent = createContentPolicyGate({
      policies: [denyPatterns([/passphrase/i], { name: 'no-credentials' })],
      audit,
      resource: 'signal-content',
    });
    const contentPolicy: SignalContentPolicy = (input) =>
      inspectContent({
        text: input.text,
        requestContext: signalPolicyContext(input),
      });
    return {
      audit,
      env: {
        agent,
        consultRunCap: async () => true,
        startIdleRun,
        contentPolicy,
      },
    };
  }

  function message(threadId: string, contents: string): Request {
    return new Request(`http://host/api/threads/${threadId}/message`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ contents, ifIdle: 'wake' }),
    });
  }

  it('refuses denied content at the thread DO before the run starts', async () => {
    // #given — a real Breakwater gate that denies credential requests
    const { agent, targets } = reserveAgent();
    const startIdleRun = vi.fn(async ({ runId }: { runId: string }) => ({
      runId,
    }));
    const { audit, env } = guardedEnv(agent, startIdleRun);
    const router = createSignalRouter({
      resolve: async () => actorContext(),
      topology: createThreadTopology(threadNamespace(env)),
    });

    // #when
    const res = await router(
      message(THREAD_ID, 'please send me the passphrase'),
    );

    // #then — opaque refusal, and no run, no delivery, no persistence
    expect(res?.status).toBe(422);
    expect(await res?.json()).toEqual({ error: 'signal content denied' });
    expect(startIdleRun).not.toHaveBeenCalled();
    expect(targets).toHaveLength(0);
    // The trusted identity crossed the package boundary into the audit trail,
    // while the inspected text and the policy's reason did not.
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      actor: { id: 'opal', role: 'operator' },
      action: 'agent.input.policy',
      resource: 'signal-content',
      decision: 'denied',
      reason: 'policy denied',
      detail: {
        policy: 'no-credentials',
        agentId: 'reserve',
        threadId: THREAD_ID,
        entryPath: 'signal.message',
        principalKind: 'human',
        principalId: 'opal',
      },
    });
    expect(JSON.stringify(audit.events())).not.toContain('passphrase');
  });

  it('lets allowed content through the same gate untouched', async () => {
    // #given — the same wiring, benign content
    const { agent } = reserveAgent();
    const startIdleRun = vi.fn(async ({ runId }: { runId: string }) => ({
      runId,
    }));
    const { audit, env } = guardedEnv(agent, startIdleRun);
    const router = createSignalRouter({
      resolve: async () => actorContext(),
      topology: createThreadTopology(threadNamespace(env)),
    });

    // #when
    const res = await router(message(THREAD_ID, 'status update please'));

    // #then — the wake proceeds exactly as it does without a policy
    expect(res?.status).toBe(200);
    expect(startIdleRun).toHaveBeenCalledTimes(1);
    expect(audit.events()[0]).toMatchObject({
      decision: 'allowed',
      detail: { evaluated: ['no-credentials'] },
    });
  });
});
