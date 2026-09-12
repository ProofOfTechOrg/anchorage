// SPDX-License-Identifier: Apache-2.0
// The unit suites each mock a seam; this file wires the real seams together.

import type { Agent, AgentSignal } from '@mastra/core/agent';
import type { NotificationRecord } from '@mastra/core/notifications';
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

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import { RUNTIME_DRIVEN_AGENT } from '../agent-runner/index.js';
import type { ActorContext, ApprovalActor } from '../approval-api/index.js';
import {
  breakwaterActorFor,
  createPrincipalActorContext,
  humanPrincipal,
  InMemoryApprovalStoreFactory,
  principalAuditFields,
  trustAutomationPrincipal,
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
import type { SignalDatabase } from './d1-shared.js';
import { createNotificationDispatchTick } from './notification-dispatch.js';
import { D1NotificationsStorage } from './notifications-d1.js';
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
  resolveNotificationsStorage?: () => D1NotificationsStorage;
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
    resolveNotificationsStorage: this.env.resolveNotificationsStorage,
    ...(this.env.contentPolicy !== undefined
      ? { contentPolicy: this.env.contentPolicy }
      : {}),
  });

  protected build(): InitResult {
    return init(
      { storage: new InMemoryStore() },
      { executionFence: 'none', startIdempotency: 'none' },
    );
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
function threadNamespace(
  env: TestEnv,
  afterResponse?: (response: Response) => Promise<void>,
): ThreadNamespaceLike<string> {
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
        fetch: async (
          input: Request | string,
          reqInit?: {
            method?: string;
            headers?: Record<string, string>;
            body?: string;
          },
        ) => {
          const response = await instance.fetch(
            typeof input === 'string' ? new Request(input, reqInit) : input,
          );
          await afterResponse?.(response);
          return response;
        },
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
function reserveAgent() {
  const targets: Array<{ ifIdle?: unknown }> = [];
  const sendSignal = vi.fn(
    (signal: AgentSignal, target: { ifIdle?: unknown }) => {
      targets.push(target);
      return {
        signal,
        accepted: Promise.resolve({ action: 'persist' as const }),
        persisted: Promise.resolve(),
      };
    },
  );
  const agent = {
    id: 'reserve',
    [RUNTIME_DRIVEN_AGENT]: true,
    __setPubSub: () => {},
    getMemory: () => ({ saveMessages: vi.fn() }),
    sendSignal,
    sendMessage: (_message: unknown, target: { ifIdle?: unknown }) => {
      targets.push(target);
      return {
        signal: { id: 'sig-1' },
        accepted: Promise.resolve({ action: 'deliver', runId: 'acme_run' }),
      };
    },
  } as unknown as Agent;
  return { agent, targets, sendSignal };
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

describe('notification dispatch — lost response after the thread DO handler', () => {
  it.each([
    'summary',
    'individual',
    'denial',
    'recorded-failure',
  ] as const)('preserves the actual %s receipt across the outer tick fallback', async (mode) => {
    const now = new Date('2026-07-20T12:00:00.000Z');
    const futureDelivery = new Date(now.getTime() + 60_000);
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(now);
    try {
      const sqlite = openSqlite();
      const storage = new D1NotificationsStorage(
        sqliteUnitDatabase(sqlite) as SignalDatabase,
      );
      const { agent, sendSignal } = reserveAgent();
      if (mode === 'recorded-failure') {
        sendSignal.mockImplementation(() => {
          throw new Error('original receiver refusal');
        });
      }
      const record = await storage.createNotification({
        id: `lost-${mode}`,
        threadId: THREAD_ID,
        resourceId: resourceIdFromKey('itest'),
        agentId: agent.id,
        source: 'provider',
        kind: 'changed',
        summary: 'notification input',
        priority: mode === 'summary' ? 'low' : 'urgent',
        deliverAt:
          mode === 'summary' ? futureDelivery : new Date(now.getTime() - 1),
        summaryAt: mode === 'summary' ? new Date(now.getTime() - 1) : undefined,
        payload: { version: 1 },
      });
      const lookup = { threadId: THREAD_ID, id: record.id };
      const rawReceipt = () =>
        sqlite
          .prepare(
            'SELECT * FROM mastra_notifications WHERE thread_id = ? AND id = ?',
          )
          .get(lookup.threadId, lookup.id);
      const before = await storage.getNotification(lookup);
      const beforeRaw = rawReceipt();
      let durableReceipt: NotificationRecord | null = null;
      let durableRaw: unknown;
      let routeResult: unknown;
      const responseReceived = vi.fn(async (response: Response) => {
        expect(response.status).toBe(200);
        routeResult = await response.clone().json();
        durableReceipt = await storage.getNotification(lookup);
        durableRaw = rawReceipt();
        throw new Error('thread handler response lost');
      });
      const contentPolicy = vi.fn<SignalContentPolicy>(() =>
        mode === 'denial'
          ? { allowed: false, outcome: 'denied' }
          : { allowed: true },
      );
      const topology = createThreadTopology(
        threadNamespace(
          {
            agent,
            resolveNotificationsStorage: () => storage,
            contentPolicy,
          },
          responseReceived,
        ),
      );
      const context = createPrincipalActorContext({
        principal: trustAutomationPrincipal({
          kind: 'system',
          id: 'notification-maintenance',
          purpose: 'notification.dispatch',
        }),
        storeFactory: new InMemoryApprovalStoreFactory(),
        buildService: () => {
          throw new Error('approval service is not used in notification tests');
        },
      });
      const conditional = vi.spyOn(
        storage,
        'updateNotificationDeliveryIfUnchanged',
      );
      const tick = createNotificationDispatchTick({
        storage,
        topology,
        resolveContext: () => context,
        executionFence: 'none',
        now: () => now,
      });

      expect(await tick()).toEqual({ due: 1, delivered: 0, failed: 1 });
      expect(responseReceived).toHaveBeenCalledOnce();
      expect(contentPolicy).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          principal: {
            kind: 'system',
            id: 'notification-maintenance',
            purpose: 'notification.dispatch',
          },
          threadId: THREAD_ID,
          resourceId: resourceIdFromKey('itest'),
          entryPath: 'notification.dispatch',
        }),
      );
      expect(durableReceipt).not.toEqual(before);
      expect(durableRaw).not.toEqual(beforeRaw);
      expect(durableReceipt).toMatchObject({ updatedAt: before?.updatedAt });
      expect(await storage.getNotification(lookup)).toEqual(durableReceipt);
      expect(rawReceipt()).toEqual(durableRaw);
      expect(conditional).toHaveBeenCalledTimes(
        mode === 'recorded-failure' ? 2 : 1,
      );
      const outerFailure = conditional.mock.calls.at(-1)?.[0];
      expect(outerFailure).toMatchObject({
        expected: {
          deliveryAttempts: 0,
          summarySignalId: null,
          deliveredSignalId: null,
        },
        failure: {
          type: 'retry',
          deliveryAttempts: 1,
          lastDeliveryError: 'thread handler response lost',
        },
      });
      expect(await conditional.mock.results.at(-1)?.value).toEqual({
        applied: false,
      });

      if (mode === 'summary') {
        expect(routeResult).toMatchObject({ delivered: 1, failed: 0 });
        expect(durableReceipt).toMatchObject({
          status: 'pending',
          summaryAt: undefined,
          summarySignalId: expect.any(String),
          deliverAt: futureDelivery,
          deliveryAttempts: 0,
          lastDeliveryError: undefined,
        });
        expect(sendSignal).toHaveBeenCalledOnce();
      } else if (mode === 'individual') {
        expect(routeResult).toMatchObject({ delivered: 1, failed: 0 });
        expect(durableReceipt).toMatchObject({
          status: 'delivered',
          deliveredSignalId: expect.any(String),
          deliveryAttempts: 0,
          lastDeliveryError: undefined,
        });
        expect(sendSignal).toHaveBeenCalledOnce();
      } else if (mode === 'denial') {
        expect(routeResult).toMatchObject({
          delivered: 0,
          failed: 0,
          discarded: 1,
        });
        expect(durableReceipt).toMatchObject({
          status: 'discarded',
          deliveryReason: 'content-policy-denied',
          deliveryAttempts: 0,
          lastDeliveryError: undefined,
          discardedAt: now,
        });
        expect(sendSignal).not.toHaveBeenCalled();
      } else {
        expect(routeResult).toMatchObject({ delivered: 0, failed: 1 });
        expect(durableReceipt).toMatchObject({
          status: 'pending',
          deliveryAttempts: 1,
          lastDeliveryError: 'original receiver refusal',
          lastDeliveryAttemptAt: now,
          deliverAt: new Date(now.getTime() + 1000),
        });
        expect(sendSignal).toHaveBeenCalledOnce();
      }
      expect(await storage.listDueNotifications({ now })).toEqual([]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('preserves the summary receipt for a batch naming an Object.prototype member', async () => {
    // #given — a due summary batch whose sources collide with the prototype
    const now = new Date('2026-07-20T12:00:00.000Z');
    const futureDelivery = new Date(now.getTime() + 60_000);
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(now);
    try {
      const sqlite = openSqlite();
      const storage = new D1NotificationsStorage(
        sqliteUnitDatabase(sqlite) as SignalDatabase,
      );
      const { agent, sendSignal } = reserveAgent();
      const ids: string[] = [];
      for (const source of ['constructor', '__proto__', 'crm']) {
        const created = await storage.createNotification({
          id: `lost-summary-${source}`,
          threadId: THREAD_ID,
          resourceId: resourceIdFromKey('itest'),
          agentId: agent.id,
          source,
          kind: 'changed',
          summary: `${source} input`,
          priority: 'low',
          deliverAt: futureDelivery,
          summaryAt: new Date(now.getTime() - 1),
          payload: { version: 1 },
        });
        ids.push(created.id);
      }
      const durableReceipts: Array<Record<string, unknown> | null> = [];
      const responseReceived = vi.fn(async (response: Response) => {
        expect(response.status).toBe(200);
        for (const id of ids) {
          durableReceipts.push(
            (await storage.getNotification({
              threadId: THREAD_ID,
              id,
            })) as unknown as Record<string, unknown> | null,
          );
        }
        throw new Error('thread handler response lost');
      });
      const topology = createThreadTopology(
        threadNamespace(
          { agent, resolveNotificationsStorage: () => storage },
          responseReceived,
        ),
      );
      const context = createPrincipalActorContext({
        principal: trustAutomationPrincipal({
          kind: 'system',
          id: 'notification-maintenance',
          purpose: 'notification.dispatch',
        }),
        storeFactory: new InMemoryApprovalStoreFactory(),
        buildService: () => {
          throw new Error('approval service is not used in notification tests');
        },
      });
      const tick = createNotificationDispatchTick({
        storage,
        topology,
        resolveContext: () => context,
        executionFence: 'none',
        now: () => now,
      });

      // #when — the thread DO succeeds and its response is lost
      expect(await tick()).toEqual({ due: 3, delivered: 0, failed: 3 });

      // #then — the emitted summary counted each colliding source once
      expect(sendSignal).toHaveBeenCalledOnce();
      const summary = sendSignal.mock.calls[0]?.[0] as unknown as {
        tagName: string;
        contents: string;
        metadata: Record<string, unknown>;
      };
      expect(summary.tagName).toBe('notification-summary');
      expect(summary.contents).toBe('__proto__: 1, constructor: 1, crm: 1');
      expect(summary.metadata.notification).toMatchObject({
        signal: 'summary',
        pending: 3,
        groups: [
          { source: '__proto__', count: 1 },
          { source: 'constructor', count: 1 },
          { source: 'crm', count: 1 },
        ],
        byPriority: { low: 3 },
      });

      // The durable summary receipts survive the outer tick fallback.
      for (const receipt of durableReceipts) {
        expect(receipt).toMatchObject({
          status: 'pending',
          summaryAt: undefined,
          summarySignalId: expect.any(String),
          deliverAt: futureDelivery,
          deliveryAttempts: 0,
          lastDeliveryError: undefined,
        });
      }
      for (const id of ids) {
        expect(
          await storage.getNotification({ threadId: THREAD_ID, id }),
        ).toEqual(durableReceipts[ids.indexOf(id)]);
      }
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('notification dispatch — chronological due window', () => {
  it.each([
    'deliverAt',
    'summaryAt',
  ] as const)('delivers a due sibling through the thread DO ahead of a future expanded-year %s', async (cursor) => {
    const now = new Date('2026-07-20T12:00:00.000Z');
    const storage = new D1NotificationsStorage(
      sqliteUnitDatabase(openSqlite()) as SignalDatabase,
    );
    const { agent, sendSignal } = reserveAgent();
    const future = await storage.createNotification({
      id: 'future',
      threadId: THREAD_ID,
      resourceId: resourceIdFromKey('itest'),
      agentId: agent.id,
      source: 'provider',
      kind: 'changed',
      summary: 'future input',
      priority: 'urgent',
      [cursor]: new Date('+010000-01-01T00:00:00.000Z'),
    });
    const due = await storage.createNotification({
      id: 'due',
      threadId: THREAD_ID,
      resourceId: resourceIdFromKey('itest'),
      agentId: agent.id,
      source: 'provider',
      kind: 'changed',
      summary: 'due input',
      priority: 'urgent',
      deliverAt: new Date(now.getTime() - 1),
    });
    const topology = createThreadTopology(
      threadNamespace({
        agent,
        resolveNotificationsStorage: () => storage,
      }),
    );
    const context = createPrincipalActorContext({
      principal: trustAutomationPrincipal({
        kind: 'system',
        id: 'notification-maintenance',
        purpose: 'notification.dispatch',
      }),
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('approval service is not used in notification tests');
      },
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology,
      resolveContext: () => context,
      executionFence: 'none',
      now: () => now,
      limit: 1,
    });

    expect(await tick()).toEqual({ due: 1, delivered: 1, failed: 0 });
    expect(sendSignal).toHaveBeenCalledOnce();
    expect(sendSignal.mock.calls[0]?.[0]).toMatchObject({
      contents: 'due input',
      metadata: { notification: { recordId: due.id } },
    });
    expect(await storage.getNotification(due)).toMatchObject({
      status: 'delivered',
      deliveredSignalId: expect.any(String),
    });
    expect(await storage.getNotification(future)).toEqual(future);
    expect(await tick()).toEqual({ due: 0, delivered: 0, failed: 0 });
    expect(sendSignal).toHaveBeenCalledOnce();
  });
});
