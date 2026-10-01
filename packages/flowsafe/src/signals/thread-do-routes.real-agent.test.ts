// SPDX-License-Identifier: Apache-2.0

import {
  type Agent,
  type CreatedAgentSignal,
  signalToXmlMarkup,
} from '@mastra/core/agent';
import { globalRunRegistry } from '@mastra/core/agent/durable';
import { isLeaseProvider } from '@mastra/core/events';
import type { MastraModelConfig } from '@mastra/core/llm';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore, MastraCompositeStore } from '@mastra/core/storage';
import {
  ACTOR_CONTEXT_KEY,
  AuditLogger,
  createGuardedAgent,
  denyPatterns,
  type PolicyEvaluator,
} from '@proofoftech/breakwater';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  BLOCKED_RUN_ENTRIES,
  FLOWSAFE_PERSISTENCE_FORBIDDEN,
} from '../agent-runner/durable-agent-runner.js';
import {
  createFlowsafeDurableAgent,
  type FlowsafeDurableAgent,
} from '../agent-runner/index.js';
import {
  type ExecutionPrincipal,
  humanPrincipal,
  trustAutomationPrincipal,
} from '../approval-api/index.js';
import type { ExecutionFenceDatabase } from '../do-runner/execution-fence.js';
import {
  createHostPubSub,
  InvalidRunRequestError,
  init,
  type RunnerRuntime,
  type RunStatus,
  type ThreadScope,
} from '../do-runner/index.js';
import type { SignalDatabase } from './d1-shared.js';
import { DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS } from './notification-dispatch.js';
import { D1NotificationsStorage } from './notifications-d1.js';
import {
  createThreadSignalRoutes,
  type SignalContentPolicy,
  type SignalContentPolicyInput,
  type StartIdleRunInput,
} from './thread-do-routes.js';

const RESOURCE_ID = 'resource-real';
const DRAIN_MARK = 'MKDRAINLEFTOVER';
const cacheShapes = [
  { name: 'default cache', cache: 'default' as const },
  { name: 'cache disabled', cache: false as const },
];

function drained(runId: string) {
  return globalRunRegistry.get(runId)?.drainPendingSignals?.('pending');
}

const OWNER = humanPrincipal({ id: 'operator', role: 'operator' });
const DISPATCHER = trustAutomationPrincipal({
  kind: 'system',
  id: 'notification-dispatch',
  purpose: 'notification.dispatch',
});
const PROVIDER = trustAutomationPrincipal({
  kind: 'service',
  id: 'signal-provider-delivery',
  purpose: 'signal-provider-delivery',
});

declare const process: {
  on(event: 'unhandledRejection', listener: (reason: unknown) => void): void;
  off(event: 'unhandledRejection', listener: (reason: unknown) => void): void;
};

function unreachableModel(): MastraModelConfig {
  const unreachable = () =>
    Promise.reject(new Error('real signal tests must not reach a model'));
  return {
    specificationVersion: 'v2',
    provider: 'flowsafe-test',
    modelId: 'unreachable',
    supportedUrls: {},
    doGenerate: unreachable,
    doStream: unreachable,
  };
}

function guardedTestAgent(
  memory: MockMemory,
  policies: readonly PolicyEvaluator[] = [],
): Agent {
  return createGuardedAgent({
    id: 'writer',
    name: 'Writer',
    instructions: 'Answer the request.',
    model: unreachableModel(),
    memory,
    allowedRoles: ['operator'],
    policies,
    audit: new AuditLogger(),
    maxSteps: 2,
    toolChoice: 'auto',
  }) as unknown as Agent;
}

function actorContext() {
  const context = new RequestContext();
  context.set(ACTOR_CONTEXT_KEY, { id: 'operator', role: 'operator' });
  return context;
}

function fakeRuntime(pubsub: ReturnType<typeof createHostPubSub> | undefined) {
  const registered: string[] = [];
  const start = vi.fn(
    async (_workflowId: string, options: { runId: string }) => ({
      runId: options.runId,
      status: 'failed' as const,
      error: 'host test terminal',
    }),
  );
  const runtime = {
    pubsub,
    registerAgent: vi.fn(),
    register: vi.fn((workflow: { id: string }) => registered.push(workflow.id)),
    workflowIds: vi.fn(() => [...registered]),
    start,
    resume: vi.fn(),
  } as unknown as RunnerRuntime;
  return { runtime, start };
}

interface HarnessBlockingRun {
  runId: string;
  principal: ExecutionPrincipal;
  status?: RunStatus;
}

async function createHarness(
  options: {
    blockingRun?: HarnessBlockingRun;
    contentPolicy?: SignalContentPolicy;
    runCapOpen?: boolean;
    policies?: readonly PolicyEvaluator[];
    cache?: 'default' | false;
  } = {},
) {
  const pubsub = createHostPubSub();
  const memory = new MockMemory();
  const { runtime, start } = fakeRuntime(pubsub);
  const notifications = new D1NotificationsStorage(
    sqliteUnitDatabase(openSqlite()) as SignalDatabase,
  );
  const storage = new MastraCompositeStore({
    id: 'real-notification-test',
    default: new InMemoryStore(),
    domains: { notifications },
  });
  const mastra = new Mastra({
    storage,
    logger: false,
    agents: { writer: guardedTestAgent(memory, options.policies) },
  });
  const agent = createFlowsafeDurableAgent({
    agent: mastra.getAgentById('writer'),
    runtime,
    pubsub,
    cache: options.cache === 'default' ? undefined : false,
    threadRuntime: mastra.agentThreadStreamRuntime,
  });
  const startIdleRun = vi.fn(async (input: StartIdleRunInput) => ({
    runId: input.runId,
  }));
  const consultRunCap = vi.fn(() => options.runCapOpen ?? true);
  const routes = createThreadSignalRoutes({
    resolveAgent: () => agent as unknown as Agent,
    resolveResourceId: () => RESOURCE_ID,
    resolveBlockingRun: () => options.blockingRun,
    serializeDispatch: async (_scope, operation) => operation(),
    // Mirrors canPersist in packages/agent-starter/src/durable-objects.ts.
    canPersist: (threadScope) =>
      threadScope.principal.kind === OWNER.kind &&
      threadScope.principal.id === OWNER.id,
    consultRunCap,
    startIdleRun,
    ...(options.contentPolicy ? { contentPolicy: options.contentPolicy } : {}),
    resolveNotificationsStorage: async () => {
      const notificationStorage = await mastra
        .getStorage()
        ?.getStore('notifications');
      if (!notificationStorage)
        throw new Error('notifications storage unavailable');
      return notificationStorage;
    },
  });
  return {
    agent,
    consultRunCap,
    mastra,
    memory,
    notifications,
    pubsub,
    routes,
    start,
    startIdleRun,
  };
}

function scope(
  pubsub: ReturnType<typeof createHostPubSub>,
  threadId: string,
  principal: ExecutionPrincipal = OWNER,
): ThreadScope {
  return {
    threadId,
    principal,
    init: init(
      { storage: new InMemoryStore() },
      { pubsub, executionFence: 'none', startIdempotency: 'none' },
    ),
  };
}

function post(path: string, body: unknown): Request {
  return new Request(`http://thread${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

async function seedThread(memory: MockMemory, threadId: string) {
  await memory.saveThread({
    thread: {
      id: threadId,
      resourceId: RESOURCE_ID,
      createdAt: new Date(),
      updatedAt: new Date(),
      metadata: {},
    },
  });
}

async function heldRun(
  fixture: Pick<Harness, 'agent' | 'memory' | 'start'>,
  threadId: string,
  runId = crypto.randomUUID(),
) {
  await seedThread(fixture.memory, threadId);
  let finish!: () => void;
  fixture.start.mockImplementationOnce(
    (_workflowId, options: { runId: string }) =>
      new Promise((resolve) => {
        finish = () =>
          resolve({
            runId: options.runId,
            status: 'failed' as const,
            error: 'host test terminal',
          });
      }),
  );
  const pending = fixture.agent.streamUntilPersisted(
    'host stream',
    {
      runId,
      memory: { thread: threadId, resource: RESOURCE_ID },
      requestContext: actorContext(),
    },
    'operator',
    'human',
    undefined,
    undefined,
    undefined,
    {
      startIdentity: {
        owner: { kind: 'human', id: 'operator' },
        target: { kind: 'agent', id: 'writer', threadId },
      },
      agentStart: { threaded: true },
      onPreparedStartIdentity: undefined,
    },
  );
  await vi.waitFor(() => expect(fixture.start).toHaveBeenCalledOnce());
  return { runId, pending, finish };
}

async function finishRun(run: Awaited<ReturnType<typeof heldRun>>) {
  run.finish();
  const result = await within(run.pending, 'held host run persistence');
  await within(result.output.consumeStream(), 'held host run terminal');
}

async function recalled(memory: MockMemory, threadId: string) {
  return (
    await memory.recall({
      threadId,
    })
  ).messages;
}

type Harness = Awaited<ReturnType<typeof createHarness>>;

async function registerInProcessRun(
  harness: Harness,
  threadId: string,
  runId: string,
  status: 'running' | 'suspended',
) {
  let finish!: () => void;
  const finished = new Promise<void>((resolve) => {
    finish = resolve;
  });
  await harness.mastra.agentThreadStreamRuntime.registerRun(
    harness.agent as never,
    {
      runId,
      status,
      fullStream: undefined,
      _waitUntilFinished: () => finished,
    } as never,
    { runId, memory: { thread: threadId, resource: RESOURCE_ID } },
    harness.pubsub,
  );
  return { runId, finish };
}

function canonicalMarkup(signal: CreatedAgentSignal): string {
  const { type, tagName, attributes, contents } = signal;
  if (typeof contents !== 'string') {
    throw new Error('this test only renders string-contents signals');
  }
  return signalToXmlMarkup({ type, tagName, attributes, contents });
}

function recordingPolicy(): {
  policy: SignalContentPolicy;
  inputs: SignalContentPolicyInput[];
} {
  const inputs: SignalContentPolicyInput[] = [];
  return {
    inputs,
    policy: (input) => {
      inputs.push(input);
      return { allowed: true };
    },
  };
}

async function waitForIdle(agent: FlowsafeDurableAgent, threadId: string) {
  await vi.waitFor(() =>
    expect(
      agent.getActiveThreadRunId({ threadId, resourceId: RESOURCE_ID }),
    ).toBeUndefined(),
  );
}

async function within<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timed out: ${label}`)),
          2_000,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

let unhandled: unknown[];
let onUnhandled: (reason: unknown) => void;

beforeEach(() => {
  unhandled = [];
  onUnhandled = (reason) => unhandled.push(reason);
  process.on('unhandledRejection', onUnhandled);
});

afterEach(() => {
  process.off('unhandledRejection', onUnhandled);
  vi.restoreAllMocks();
});

describe('thread signal routes with a real durable agent', () => {
  it.each(
    cacheShapes,
  )('routes and drains a signal into an HTTP-started run with $name', async ({
    cache,
  }) => {
    const threadId = crypto.randomUUID();
    const runId = crypto.randomUUID();
    const harness = await createHarness({
      cache,
      blockingRun: { runId, principal: OWNER, status: 'running' },
    });
    const run = await heldRun(harness, threadId, runId);
    try {
      const response = await harness.routes(
        post('/signal', { contents: 'routed signal' }),
        scope(harness.pubsub, threadId),
      );
      expect(response?.status).toBe(200);
      expect(await response?.json()).toMatchObject({
        decision: { action: 'deliver', runId },
      });
      expect(drained(runId)).toEqual([
        expect.objectContaining({ contents: 'routed signal' }),
      ]);
    } finally {
      await finishRun(run);
      expect(unhandled).toEqual([]);
    }
  }, 15_000);

  it.each([
    'deliver',
    'exhausted',
    'capped',
    'capped at the attempt bound',
  ] as const)('dispatches a non-owner row through D1 notification storage with a real agent: %s', async (mode) => {
    // #given — a due row the dispatch tick delivers as its own principal
    const harness = await createHarness({
      runCapOpen: mode !== 'capped' && mode !== 'capped at the attempt bound',
    });
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const now = new Date();
    const record = await harness.notifications.createNotification({
      threadId,
      resourceId: RESOURCE_ID,
      agentId: 'writer',
      source: 'provider',
      kind: 'changed',
      summary: 'notification input',
      deliverAt: now,
    });
    if (mode === 'exhausted' || mode === 'capped at the attempt bound') {
      await harness.notifications.updateNotification({
        threadId,
        id: record.id,
        deliveryAttempts:
          mode === 'exhausted'
            ? DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS
            : DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS - 1,
        lastDeliveryError: 'target refused',
        lastDeliveryAttemptAt: now,
      });
    }
    const send = vi.spyOn(harness.agent, 'sendSignal');

    // #when
    const response = await harness.routes(
      post('/signal/notifications/dispatch', {
        notificationIds: [record.id],
        resourceId: RESOURCE_ID,
        agentId: 'writer',
        now: now.toISOString(),
      }),
      scope(harness.pubsub, threadId, DISPATCHER),
    );

    // #then
    expect(response?.status).toBe(200);
    const persisted = await harness.notifications.getNotification({
      threadId,
      id: record.id,
    });
    expect(send).not.toHaveBeenCalled();
    if (mode === 'exhausted') {
      expect(await response?.json()).toMatchObject({
        delivered: 0,
        failed: 0,
        discarded: 1,
      });
      expect(harness.startIdleRun).not.toHaveBeenCalled();
      expect(persisted).toMatchObject({
        status: 'discarded',
        deliveryAttempts: DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS,
        lastDeliveryError: 'target refused',
        lastDeliveryAttemptAt: now,
        deliveryReason: 'delivery-attempts-exhausted',
        discardedAt: expect.any(Date),
      });
      expect(persisted?.deliverAt).toBeUndefined();
      expect(persisted?.summaryAt).toBeUndefined();
    } else if (mode === 'capped') {
      // A capped idle wake cannot fall back to a persist the dispatch
      // principal is not allowed, so the round fails and the row retries.
      expect(await response?.json()).toEqual({ delivered: 0, failed: 1 });
      expect(harness.consultRunCap).toHaveBeenCalledOnce();
      expect(harness.startIdleRun).not.toHaveBeenCalled();
      expect(persisted).toMatchObject({
        status: 'pending',
        deliveryAttempts: 1,
        deliverAt: expect.any(Date),
      });
    } else if (mode === 'capped at the attempt bound') {
      expect(await response?.json()).toMatchObject({
        delivered: 0,
        failed: 0,
        discarded: 1,
      });
      expect(harness.startIdleRun).not.toHaveBeenCalled();
      expect(persisted).toMatchObject({
        status: 'discarded',
        deliveryAttempts: DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS,
        deliveryReason: 'delivery-attempts-exhausted',
      });
    } else {
      // An idle thread wakes a run whose principal is the dispatch principal.
      expect(await response?.json()).toMatchObject({ delivered: 1, failed: 0 });
      expect(harness.startIdleRun).toHaveBeenCalledOnce();
      expect(harness.startIdleRun).toHaveBeenCalledWith(
        expect.objectContaining({
          threadId,
          resourceId: RESOURCE_ID,
          entryPath: 'notification.dispatch',
          principal: DISPATCHER,
          signal: expect.objectContaining({ contents: 'notification input' }),
        }),
      );
      expect(persisted).toMatchObject({
        status: 'delivered',
        deliveredSignalId: expect.any(String),
      });
    }
    expect(harness.start).not.toHaveBeenCalled();
  });

  it('persists an idle queue message without a run', async () => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    const response = await harness.routes(
      post('/signal/queue', { contents: 'queued' }),
      scope(harness.pubsub, threadId),
    );

    const body = (await response?.json()) as {
      decision: Record<string, unknown>;
      runId?: string;
    };
    expect(body).toMatchObject({
      decision: { action: 'persist' },
    });
    expect(body).not.toHaveProperty('runId');
    expect(body.decision).not.toHaveProperty('runId');
    expect(await recalled(harness.memory, threadId)).toHaveLength(1);
    expect(
      harness.agent.getActiveThreadRunId({
        threadId,
        resourceId: RESOURCE_ID,
      }),
    ).toBeUndefined();
    expect(harness.start).not.toHaveBeenCalled();
  });

  it('persists idle state and an owner notification without a run', async () => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);

    const stateResponse = await harness.routes(
      post('/signal/state', {
        id: 'state-1',
        cacheKey: 'cache-1',
        contents: 'state input',
        value: { ready: true },
      }),
      scope(harness.pubsub, threadId),
    );
    const notificationResponse = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'notification input',
      }),
      scope(harness.pubsub, threadId),
    );

    const stateBody = (await stateResponse?.json()) as {
      decision: Record<string, unknown>;
    };
    const notificationBody = await notificationResponse?.json();
    expect(stateBody).toMatchObject({
      decision: { action: 'persist' },
    });
    expect(stateBody.decision).not.toHaveProperty('runId');
    expect(notificationBody).toMatchObject({
      delivery: { action: 'persist', signalId: expect.any(String) },
      record: { status: 'delivered', deliveredSignalId: expect.any(String) },
    });
    // The state signal and the owner notification are both persisted.
    expect(await recalled(harness.memory, threadId)).toHaveLength(2);
    expect(
      harness.agent.getActiveThreadRunId({
        threadId,
        resourceId: RESOURCE_ID,
      }),
    ).toBeUndefined();
    expect(harness.startIdleRun).not.toHaveBeenCalled();
    expect(harness.start).not.toHaveBeenCalled();
  });

  it('delivers an owner notification at ingestion to a wrapper no Mastra registers', async () => {
    // #given — an idle thread with memory, and a wrapper that no Mastra
    // registers
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    expect(Object.values(harness.mastra.listAgents())).not.toContain(
      harness.agent,
    );
    const send = vi.spyOn(harness.agent, 'sendSignal');

    // #when — the thread owner posts a notification
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'owner notification input',
      }),
      scope(harness.pubsub, threadId),
    );

    // #then — it is persisted to memory under the owner's principal, the row
    // records the delivery, and neither a run nor the dispatcher is involved
    expect(response?.status).toBe(200);
    const body = (await response?.json()) as {
      record: Record<string, unknown>;
      delivery: Record<string, unknown>;
    };
    expect(Object.keys(body).sort()).toEqual(['delivery', 'record']);
    expect(body.delivery).toEqual({
      action: 'persist',
      signalId: expect.any(String),
    });
    expect(body.record).toMatchObject({
      status: 'delivered',
      deliveredSignalId: body.delivery.signalId,
    });
    expect(send).toHaveBeenCalledOnce();
    expect(send.mock.calls[0]?.[1]).toMatchObject({
      threadId,
      resourceId: RESOURCE_ID,
      ifActive: { behavior: 'deliver' },
      ifIdle: { behavior: 'persist' },
    });
    const rows = await harness.notifications.listNotifications({ threadId });
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      threadId,
      resourceId: RESOURCE_ID,
      agentId: 'writer',
      summary: 'owner notification input',
      status: 'delivered',
      deliveredSignalId: body.delivery.signalId,
      deliveredAt: expect.any(Date),
    });
    expect(rows[0]?.deliverAt).toBeUndefined();
    expect(rows[0]?.summaryAt).toBeUndefined();
    expect(await recalled(harness.memory, threadId)).toHaveLength(1);
    expect(harness.startIdleRun).not.toHaveBeenCalled();
    expect(harness.start).not.toHaveBeenCalled();
  });

  it("delivers an owner notification into the owner's running run", async () => {
    // #given — the owner's run is executing in this isolate
    const threadId = crypto.randomUUID();
    const runId = crypto.randomUUID();
    const harness = await createHarness({
      blockingRun: { runId, principal: OWNER, status: 'running' },
    });
    await seedThread(harness.memory, threadId);
    const run = await registerInProcessRun(harness, threadId, runId, 'running');
    const send = vi.spyOn(harness.agent, 'sendSignal');

    // #when
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'owner notification input',
      }),
      scope(harness.pubsub, threadId),
    );

    // #then — the signal joins that run and the row records the delivery
    expect(response?.status).toBe(200);
    const body = (await response?.json()) as {
      record: Record<string, unknown>;
      delivery: Record<string, unknown>;
    };
    expect(body.delivery).toEqual({
      action: 'deliver',
      runId,
      signalId: expect.any(String),
    });
    expect(body.record).toMatchObject({
      status: 'delivered',
      deliveredSignalId: body.delivery.signalId,
    });
    expect(send.mock.calls[0]?.[1]).toMatchObject({
      ifActive: { behavior: 'deliver' },
    });
    expect(await recalled(harness.memory, threadId)).toHaveLength(0);

    // #and — a delivery the run leaves undrained reaches the runner's
    // terminal refusal, which keeps it in memory and starts no run
    run.finish();
    await waitForIdle(harness.agent, threadId);
    await vi.waitFor(async () =>
      expect(await recalled(harness.memory, threadId)).toHaveLength(1),
    );
    expect(harness.startIdleRun).not.toHaveBeenCalled();
    expect(harness.start).not.toHaveBeenCalled();
    expect(unhandled).toEqual([]);
  });

  it("persists an owner notification instead of queueing it into the owner's suspended run", async () => {
    // #given — the owner's run is suspended but still registered in process
    const threadId = crypto.randomUUID();
    const runId = crypto.randomUUID();
    const harness = await createHarness({
      blockingRun: { runId, principal: OWNER, status: 'suspended' },
    });
    await seedThread(harness.memory, threadId);
    await registerInProcessRun(harness, threadId, runId, 'suspended');
    expect(
      harness.agent.getActiveThreadRunId({
        threadId,
        resourceId: RESOURCE_ID,
      }),
    ).toBe(runId);
    const send = vi.spyOn(harness.agent, 'sendSignal');

    // #when
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'owner notification input',
      }),
      scope(harness.pubsub, threadId),
    );

    // #then — the signal is written to memory rather than queued in the run
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({
      delivery: { action: 'persist', signalId: expect.any(String) },
      record: { status: 'delivered', deliveredSignalId: expect.any(String) },
    });
    expect(send.mock.calls[0]?.[1]).toMatchObject({
      ifActive: { behavior: 'persist' },
    });
    expect(await recalled(harness.memory, threadId)).toHaveLength(1);
    expect(harness.startIdleRun).not.toHaveBeenCalled();
    expect(harness.start).not.toHaveBeenCalled();
  });

  it('persists a direct signal into a suspended run', async () => {
    const threadId = crypto.randomUUID();
    const runId = crypto.randomUUID();
    const harness = await createHarness({
      blockingRun: { runId, principal: OWNER, status: 'suspended' },
    });
    await seedThread(harness.memory, threadId);
    await registerInProcessRun(harness, threadId, runId, 'suspended');
    const send = vi.spyOn(harness.agent, 'sendSignal');

    const response = await harness.routes(
      post('/signal', { contents: 'direct signal input' }),
      scope(harness.pubsub, threadId),
    );

    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({
      decision: { action: 'persist' },
      signalId: expect.any(String),
    });
    expect(send).toHaveBeenCalledOnce();
    expect(send.mock.calls[0]?.[1]).toMatchObject({
      ifActive: { behavior: 'persist' },
    });
    expect(
      (await harness.memory.recall({ threadId, hideSignals: [] })).messages,
    ).toMatchObject([{ role: 'signal', type: 'system-reminder' }]);
  });

  it("persists an owner notification when the owner's run survives only in storage", async () => {
    // #given — the owner's run is durable but not in this isolate
    const threadId = crypto.randomUUID();
    const harness = await createHarness({
      blockingRun: {
        runId: crypto.randomUUID(),
        principal: OWNER,
        status: 'suspended',
      },
    });
    await seedThread(harness.memory, threadId);

    // #when
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'owner notification input',
      }),
      scope(harness.pubsub, threadId),
    );

    // #then
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({
      delivery: { action: 'persist', signalId: expect.any(String) },
      record: { status: 'delivered', deliveredSignalId: expect.any(String) },
    });
    expect(await recalled(harness.memory, threadId)).toHaveLength(1);
    expect(harness.startIdleRun).not.toHaveBeenCalled();
    expect(harness.start).not.toHaveBeenCalled();
  });

  it("refuses an owner notification while another principal's run holds the thread", async () => {
    // #given — a nonterminal run that belongs to someone else
    const threadId = crypto.randomUUID();
    const { policy, inputs } = recordingPolicy();
    const harness = await createHarness({
      blockingRun: {
        runId: 'other-run',
        principal: humanPrincipal({ id: 'someone-else', role: 'operator' }),
        status: 'running',
      },
      contentPolicy: policy,
    });
    await seedThread(harness.memory, threadId);
    const send = vi.spyOn(harness.agent, 'sendSignal');

    // #when
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'owner notification input',
      }),
      scope(harness.pubsub, threadId),
    );

    // #then — refused before inspection, recording, or delivery
    expect(response?.status).toBe(409);
    expect(await response?.json()).toEqual({
      error: 'notification principal does not match the active run',
      reason: 'principal-mismatch',
      runId: 'other-run',
      retry: true,
    });
    expect(await harness.notifications.listNotifications({ threadId })).toEqual(
      [],
    );
    expect(inputs).toEqual([]);
    expect(send).not.toHaveBeenCalled();
  });

  it('refuses an owner notification to an idle thread without memory', async () => {
    // #given — an idle thread whose agent has no memory
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    vi.spyOn(harness.agent, 'getMemory').mockResolvedValue(undefined as never);
    const send = vi.spyOn(harness.agent, 'sendSignal');

    // #when
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'owner notification input',
      }),
      scope(harness.pubsub, threadId),
    );

    // #then
    expect(response?.status).toBe(409);
    expect(await response?.json()).toEqual({
      error: 'notification delivery requires agent memory',
      reason: 'memory-unavailable',
    });
    expect(await harness.notifications.listNotifications({ threadId })).toEqual(
      [],
    );
    expect(send).not.toHaveBeenCalled();
  });

  it('never makes an owner row due for the dispatcher', async () => {
    // #given — an owner row left pending because no settle write landed
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const settle = vi
      .spyOn(harness.notifications, 'updateNotification')
      .mockRejectedValue(new Error('inbox unavailable'));
    const failed = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'owner notification input',
      }),
      scope(harness.pubsub, threadId),
    );
    expect(failed?.status).toBe(502);
    settle.mockRestore();
    const [residue] = await harness.notifications.listNotifications({
      threadId,
    });
    expect(residue).toMatchObject({ status: 'pending' });
    expect(residue?.deliverAt).toBeUndefined();
    expect(residue?.summaryAt).toBeUndefined();
    const later = new Date(Date.now() + 86_400_000);

    // #when — the dispatcher is handed the row, and the due scan runs
    const dispatched = await harness.routes(
      post('/signal/notifications/dispatch', {
        notificationIds: [residue?.id],
        resourceId: RESOURCE_ID,
        agentId: 'writer',
        now: later.toISOString(),
      }),
      scope(harness.pubsub, threadId, DISPATCHER),
    );
    const due = await harness.notifications.listDueNotifications({
      now: later,
    });

    // #then — neither selects it
    expect(await dispatched?.json()).toEqual({
      delivered: 0,
      failed: 0,
      skipped: 1,
    });
    expect(due).toEqual([]);
    expect(harness.startIdleRun).not.toHaveBeenCalled();
  });

  it('refuses an owner notification whose key matches a pending dispatcher row', async () => {
    // #given — a non-owner row the dispatch tick will deliver
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const recorded = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'non-owner summary',
        coalesceKey: 'shared-key',
        attributes: { origin: 'provider' },
      }),
      scope(harness.pubsub, threadId, PROVIDER),
    );
    expect(await recorded?.json()).toMatchObject({
      delivery: { action: 'deferred', reason: 'dispatcher' },
    });
    const send = vi.spyOn(harness.agent, 'sendSignal');

    // #when — the owner posts with the same key
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'owner summary',
        coalesceKey: 'shared-key',
        attributes: { owner: 'yes' },
      }),
      scope(harness.pubsub, threadId),
    );

    // #then — nothing is sent or written, and the dispatcher's row is intact
    expect(response?.status).toBe(409);
    expect(await response?.json()).toEqual({
      error: 'a matching notification is already pending',
      reason: 'notification-pending',
    });
    expect(send).not.toHaveBeenCalled();
    const rows = await harness.notifications.listNotifications({ threadId });
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      status: 'pending',
      summary: 'non-owner summary',
      attributes: { origin: 'provider' },
      coalescedCount: 1,
      deliverAt: expect.any(Date),
    });
  });

  it('settles owner residue that an owner notification key matches', async () => {
    // #given — an owner row left pending with no due time
    const { policy, inputs } = recordingPolicy();
    const harness = await createHarness({ contentPolicy: policy });
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const residue = await harness.notifications.createNotification({
      threadId,
      resourceId: RESOURCE_ID,
      agentId: 'writer',
      source: 'provider',
      kind: 'changed',
      summary: 'first summary',
      coalesceKey: 'owner-key',
      attributes: { first: 'yes' },
    });
    const send = vi.spyOn(harness.agent, 'sendSignal');

    // #when — the owner posts again with the same key
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'second summary',
        coalesceKey: 'owner-key',
        attributes: { second: 'yes' },
      }),
      scope(harness.pubsub, threadId),
    );

    // #then — the merged residue row is inspected, sent, and settled
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({
      record: {
        id: residue.id,
        status: 'delivered',
        coalescedCount: 2,
        attributes: { first: 'yes', second: 'yes' },
      },
      delivery: { action: 'persist' },
    });
    expect(send).toHaveBeenCalledOnce();
    const sent = send.mock.calls[0]?.[0] as CreatedAgentSignal;
    expect(inputs).toHaveLength(1);
    expect(inputs[0]?.text).toBe(canonicalMarkup(sent));
    expect(inputs[0]?.text).toContain(`id="${residue.id}"`);
    expect(
      await harness.notifications.listNotifications({ threadId }),
    ).toHaveLength(1);
  });

  it('refuses a non-owner keyed create that matches owner residue', async () => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const residue = await harness.notifications.createNotification({
      threadId,
      resourceId: RESOURCE_ID,
      agentId: 'writer',
      source: 'provider',
      kind: 'changed',
      summary: 'owner summary',
      coalesceKey: 'shared-key',
      attributes: { origin: 'owner' },
    });
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'provider summary',
        coalesceKey: 'shared-key',
        attributes: { origin: 'provider' },
      }),
      scope(harness.pubsub, threadId, PROVIDER),
    );
    expect(response?.status).toBe(409);
    expect(await response?.json()).toMatchObject({
      reason: 'notification-pending',
    });
    expect(
      await harness.notifications.listNotifications({ threadId }),
    ).toMatchObject([
      {
        id: residue.id,
        status: 'pending',
        summary: 'owner summary',
        attributes: { origin: 'owner' },
        coalescedCount: 1,
      },
    ]);
    const [row] = await harness.notifications.listNotifications({ threadId });
    expect(row?.deliverAt).toBeUndefined();
    expect(row?.summaryAt).toBeUndefined();
    expect(
      await harness.notifications.listDueNotifications({
        now: new Date(Date.now() + 86_400_000),
      }),
    ).toEqual([]);
  });

  it.each([
    ['owner residue from a non-owner', false, PROVIDER],
    ['a due non-owner row from an owner', true, OWNER],
  ] as const)('refuses an empty dedupe key matching %s', async (_description, existingDue, incomingPrincipal) => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    await harness.notifications.createNotification({
      threadId,
      resourceId: RESOURCE_ID,
      agentId: 'writer',
      source: 'provider',
      kind: 'changed',
      summary: 'existing summary',
      dedupeKey: '',
      coalesceKey: 'old',
      attributes: { origin: 'existing' },
      ...(existingDue ? { deliverAt: new Date() } : {}),
    });
    const before = await harness.notifications.listNotifications({ threadId });
    expect(before).toHaveLength(1);
    expect(before[0]?.deliverAt !== undefined).toBe(existingDue);
    expect(before[0]?.summaryAt).toBeUndefined();

    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'incoming summary',
        dedupeKey: '',
        coalesceKey: 'new',
        attributes: { origin: 'incoming' },
      }),
      scope(harness.pubsub, threadId, incomingPrincipal),
    );

    expect(response?.status).toBe(409);
    expect(await response?.json()).toMatchObject({
      reason: 'notification-pending',
    });
    expect(await harness.notifications.listNotifications({ threadId })).toEqual(
      before,
    );
  });

  it('coalesces a non-owner keyed create into a due non-owner row', async () => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    const first = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'first',
        coalesceKey: 'shared-key',
      }),
      scope(harness.pubsub, threadId, PROVIDER),
    );
    expect(first?.status).toBe(200);
    const second = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'second',
        coalesceKey: 'shared-key',
      }),
      scope(harness.pubsub, threadId, PROVIDER),
    );
    expect(second?.status).toBe(200);
    expect(
      await harness.notifications.listNotifications({ threadId }),
    ).toMatchObject([
      {
        status: 'pending',
        summary: 'second',
        coalescedCount: 2,
        deliverAt: expect.any(Date),
      },
    ]);
  });

  it('records a non-owner notification for dispatcher delivery', async () => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'notification input',
      }),
      scope(harness.pubsub, threadId, PROVIDER),
    );

    expect(await response?.json()).toMatchObject({
      delivery: { action: 'deferred', reason: 'dispatcher' },
      record: { status: 'pending', deliverAt: expect.any(String) },
    });
    expect(await recalled(harness.memory, threadId)).toHaveLength(0);
    expect(
      harness.agent.getActiveThreadRunId({
        threadId,
        resourceId: RESOURCE_ID,
      }),
    ).toBeUndefined();
    expect(harness.startIdleRun).not.toHaveBeenCalled();
    expect(harness.start).not.toHaveBeenCalled();
  });

  it.each([
    ['owner leftover', {}, 1],
    [
      'marked non-owner leftover',
      { metadata: { [FLOWSAFE_PERSISTENCE_FORBIDDEN]: true } },
      0,
    ],
    ['transient leftover', { transient: true }, 0],
  ] as const)('terminally heals a completion drain for %s', async (_label, signalFields, expectedSavedMessages) => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    const previousRunId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const saveMessages = vi.spyOn(harness.memory, 'saveMessages');
    const publish = vi.spyOn(harness.pubsub, 'publish');
    let finish!: () => void;
    const finished = new Promise<void>((resolve) => {
      finish = resolve;
    });
    await harness.mastra.agentThreadStreamRuntime.registerRun(
      harness.agent as never,
      {
        runId: previousRunId,
        status: 'running',
        fullStream: undefined,
        _waitUntilFinished: () => finished,
      } as never,
      {
        runId: previousRunId,
        memory: { thread: threadId, resource: RESOURCE_ID },
      },
      harness.pubsub,
    );
    const sent = harness.agent.sendSignal(
      { type: 'reactive', contents: 'leftover', ...signalFields },
      {
        runId: previousRunId,
        threadId,
        resourceId: RESOURCE_ID,
        ifActive: { behavior: 'deliver' },
      },
    );
    await expect(sent.accepted).resolves.toMatchObject({ action: 'deliver' });

    finish();
    await waitForIdle(harness.agent, threadId);
    const completed = publish.mock.calls.find(
      ([, event]) =>
        event.type === 'run-completed' && event.runId !== previousRunId,
    )?.[1];
    const nextRunId = completed?.runId;
    expect(nextRunId).toEqual(expect.any(String));
    expect(
      publish.mock.calls.some(
        ([, event]) => event.type === 'error' && event.runId === nextRunId,
      ),
    ).toBe(true);
    expect(
      harness.mastra.agentThreadStreamRuntime.getThreadState(
        { threadId, resourceId: RESOURCE_ID },
        harness.pubsub,
      ),
    ).toBe('idle');
    expect(isLeaseProvider(harness.pubsub)).toBe(true);
    if (!isLeaseProvider(harness.pubsub)) throw new Error('lease unavailable');
    expect(
      await harness.pubsub.getLeaseOwner(`${RESOURCE_ID}\0${threadId}`),
    ).toBeUndefined();
    const savedMessages = saveMessages.mock.calls.flatMap(
      ([input]) => input.messages,
    );
    expect(savedMessages).toHaveLength(expectedSavedMessages);
    if (expectedSavedMessages > 0) {
      expect(savedMessages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            role: 'signal',
            threadId,
            resourceId: RESOURCE_ID,
          }),
        ]),
      );
    }
    expect(harness.start).not.toHaveBeenCalled();
    expect(unhandled).toEqual([]);
  });

  it.each([
    ['keeps a leftover the input chain allows', [], {}, 1],
    [
      'keeps nothing of a leftover an input policy denies',
      [denyPatterns([DRAIN_MARK])],
      {},
      0,
    ],
    [
      'keeps nothing of a transient leftover the input chain allows',
      [],
      { transient: true },
      0,
    ],
  ] as const)('terminally heals a completion drain after a run carrying the owner actor, and %s', async (_label, policies, signalFields, expectedSavedMessages) => {
    // #given — the previous run carries the owner's actor, as a host start
    // does, and the leftover carries the marker
    const harness = await createHarness({ policies });
    const threadId = crypto.randomUUID();
    const previousRunId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const saveMessages = vi.spyOn(harness.memory, 'saveMessages');
    const publish = vi.spyOn(harness.pubsub, 'publish');
    let finish!: () => void;
    const finished = new Promise<void>((resolve) => {
      finish = resolve;
    });
    await harness.mastra.agentThreadStreamRuntime.registerRun(
      harness.agent as never,
      {
        runId: previousRunId,
        status: 'running',
        fullStream: undefined,
        _waitUntilFinished: () => finished,
      } as never,
      {
        runId: previousRunId,
        memory: { thread: threadId, resource: RESOURCE_ID },
        requestContext: actorContext(),
      },
      harness.pubsub,
    );
    const sent = harness.agent.sendSignal(
      { type: 'reactive', contents: `${DRAIN_MARK} leftover`, ...signalFields },
      {
        runId: previousRunId,
        threadId,
        resourceId: RESOURCE_ID,
        ifActive: { behavior: 'deliver' },
      },
    );
    await expect(sent.accepted).resolves.toMatchObject({ action: 'deliver' });

    // #when — the previous run completes and core drains the leftover into
    // a run the host start seam never registered
    finish();
    await waitForIdle(harness.agent, threadId);

    // #then — the drain run was refused terminally, and memory holds the
    // leftover only when the input chain allowed it
    const nextRunId = publish.mock.calls.find(
      ([, event]) =>
        event.type === 'run-completed' && event.runId !== previousRunId,
    )?.[1].runId;
    expect(nextRunId).toEqual(expect.any(String));
    expect(
      publish.mock.calls.some(
        ([, event]) => event.type === 'error' && event.runId === nextRunId,
      ),
    ).toBe(true);
    const savedMessages = saveMessages.mock.calls.flatMap(
      ([input]) => input.messages,
    );
    expect(savedMessages).toHaveLength(expectedSavedMessages);
    expect(savedMessages).toEqual(
      Array.from({ length: expectedSavedMessages }, () =>
        expect.objectContaining({
          role: 'signal',
          threadId,
          resourceId: RESOURCE_ID,
        }),
      ),
    );
    // Recall hides reactive signals unless asked to include every signal.
    const { messages } = await harness.memory.recall({
      threadId,
      hideSignals: false,
    });
    expect(JSON.stringify(messages).includes(DRAIN_MARK)).toBe(
      expectedSavedMessages > 0,
    );
    expect(harness.start).not.toHaveBeenCalled();
    expect(unhandled).toEqual([]);
  });

  it.each([
    [
      'sendMessage',
      async (agent: FlowsafeDurableAgent, threadId: string) =>
        agent.sendMessage(
          { contents: 'message input' },
          {
            threadId,
            resourceId: RESOURCE_ID,
            ifIdle: {
              behavior: 'wake',
              streamOptions: { requestContext: actorContext() },
            },
          },
        ),
    ],
    [
      'sendSignal',
      async (agent: FlowsafeDurableAgent, threadId: string) =>
        agent.sendSignal(
          { type: 'reactive', contents: 'signal input' },
          {
            threadId,
            resourceId: RESOURCE_ID,
            ifIdle: {
              behavior: 'wake',
              streamOptions: { requestContext: actorContext() },
            },
          },
        ),
    ],
    [
      'sendStateSignal',
      async (agent: FlowsafeDurableAgent, threadId: string) =>
        agent.sendStateSignal(
          {
            id: 'state-direct',
            cacheKey: 'state-direct',
            contents: 'state input',
            mode: 'snapshot',
            value: { ready: true },
          },
          {
            threadId,
            resourceId: RESOURCE_ID,
            ifIdle: {
              behavior: 'wake',
              streamOptions: { requestContext: actorContext() },
            },
          },
        ),
    ],
    [
      'queueMessage',
      async (agent: FlowsafeDurableAgent, threadId: string) =>
        agent.queueMessage(
          { contents: 'queued input' },
          {
            threadId,
            resourceId: RESOURCE_ID,
            ifIdle: {
              behavior: 'wake',
              streamOptions: { requestContext: actorContext() },
            },
          },
        ),
    ],
  ])('terminally heals direct %s idle wakes', async (_name, invoke) => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const saveMessages = vi.spyOn(harness.memory, 'saveMessages');
    const publish = vi.spyOn(harness.pubsub, 'publish');
    const result = await invoke(harness.agent, threadId);
    const accepted = 'accepted' in result ? result.accepted : undefined;
    expect(accepted).toBeDefined();
    const decision = await accepted;
    expect(decision).toMatchObject({ action: 'wake' });
    if (!decision || !('runId' in decision)) {
      throw new Error('wake decision has no run id');
    }
    const { runId } = decision;

    await waitForIdle(harness.agent, threadId);
    expect(harness.start).not.toHaveBeenCalled();
    expect(
      publish.mock.calls.some(
        ([, event]) => event.type === 'error' && event.runId === runId,
      ),
    ).toBe(true);
    expect(
      publish.mock.calls.some(
        ([, event]) => event.type === 'run-completed' && event.runId === runId,
      ),
    ).toBe(true);
    expect(
      saveMessages.mock.calls.flatMap(([input]) => input.messages),
    ).not.toHaveLength(0);
    expect(unhandled).toEqual([]);
  });

  // The dispatcher delivers each row as its own signal, low priority included.
  // The source strings here name Object.prototype members.
  it('delivers prototype-colliding non-owner notifications through the dispatcher', async () => {
    // #given — deferred non-owner notifications with colliding source names
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const ids: string[] = [];
    for (const source of ['constructor', '__proto__']) {
      const response = await harness.routes(
        post('/signal/notification', {
          source,
          kind: 'changed',
          summary: `${source} input`,
          priority: 'low',
        }),
        scope(harness.pubsub, threadId, PROVIDER),
      );
      expect(response?.status).toBe(200);
      const body = (await response?.json()) as {
        record: { id: string; source: string };
        delivery: unknown;
      };
      expect(body.delivery).toEqual({
        action: 'deferred',
        reason: 'dispatcher',
      });
      expect(body.record.source).toBe(source);
      ids.push(body.record.id);
    }

    // #when — the dispatcher delivers the due rows
    const response = await harness.routes(
      post('/signal/notifications/dispatch', {
        notificationIds: ids,
        resourceId: RESOURCE_ID,
        agentId: 'writer',
        now: new Date(Date.now() + 60_000).toISOString(),
      }),
      scope(harness.pubsub, threadId, DISPATCHER),
    );

    // #then — each colliding source wakes the idle thread as its own signal
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({ delivered: 2, failed: 0 });
    expect(harness.startIdleRun).toHaveBeenCalledTimes(2);
    const signals = harness.startIdleRun.mock.calls.map(([input]) => {
      expect(input).toMatchObject({
        entryPath: 'notification.dispatch',
        principal: DISPATCHER,
      });
      return input.signal as unknown as {
        tagName: string;
        contents: string;
        attributes: Record<string, unknown>;
      };
    });
    expect(
      signals
        .map(({ tagName, contents, attributes }) => ({
          tagName,
          contents,
          source: attributes.source,
        }))
        .sort((left, right) => left.contents.localeCompare(right.contents)),
    ).toEqual([
      {
        tagName: 'notification',
        contents: '__proto__ input',
        source: '__proto__',
      },
      {
        tagName: 'notification',
        contents: 'constructor input',
        source: 'constructor',
      },
    ]);
    for (const id of ids) {
      expect(
        await harness.notifications.getNotification({ threadId, id }),
      ).toMatchObject({
        status: 'delivered',
        deliveredSignalId: expect.any(String),
      });
    }
    expect(harness.start).not.toHaveBeenCalled();
    expect(unhandled).toEqual([]);
  });

  it('keeps the wrapper off every Mastra, so a direct notification send starts nothing', async () => {
    // #given the thread host composition, whose host Mastra holds the
    // notifications store
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const refusal = `FlowsafeDurableAgent.__setMastra() is unavailable: ${BLOCKED_RUN_ENTRIES.__setMastra}`;

    // #when / #then neither registration route binds the wrapper to a Mastra
    expect(() =>
      harness.mastra.addAgent(harness.agent as unknown as Agent, 'wrapper'),
    ).toThrow(refusal);
    expect(
      () =>
        new Mastra({
          storage: new InMemoryStore(),
          logger: false,
          agents: { wrapper: harness.agent as unknown as Agent },
        }),
    ).toThrow(refusal);
    expect(harness.agent.getMastraInstance()).toBeUndefined();

    // #then so core's own notification send finds no notifications store and
    // rejects before it records a row or starts a run
    await expect(
      harness.agent.sendNotificationSignal(
        { source: 'provider', kind: 'changed', summary: 'direct notification' },
        { threadId, resourceId: RESOURCE_ID },
      ),
    ).rejects.toThrow(
      'sendNotificationSignal requires a notifications storage domain',
    );
    expect(await harness.notifications.listNotifications({ threadId })).toEqual(
      [],
    );
    expect(harness.start).not.toHaveBeenCalled();
    expect(harness.startIdleRun).not.toHaveBeenCalled();
    expect(unhandled).toEqual([]);
  });

  it('terminally closes direct calls and protects a registered host start', async () => {
    const harness = await createHarness();
    const directThreadId = crypto.randomUUID();
    await seedThread(harness.memory, directThreadId);
    const directId = crypto.randomUUID();
    const direct = await within(
      harness.agent.stream('direct stream', {
        runId: directId,
        requestContext: actorContext(),
        memory: { thread: directThreadId, resource: RESOURCE_ID },
      }),
      'direct stream setup',
    );
    await within(direct.output.consumeStream(), 'direct stream terminal');
    expect(direct.output.status).toBe('failed');
    expect(
      harness.agent.getActiveThreadRunId({
        threadId: directThreadId,
        resourceId: RESOURCE_ID,
      }),
    ).toBeUndefined();
    expect(
      harness.mastra.agentThreadStreamRuntime.getThreadState(
        { threadId: directThreadId, resourceId: RESOURCE_ID },
        harness.pubsub,
      ),
    ).toBe('idle');
    expect(isLeaseProvider(harness.pubsub)).toBe(true);
    if (!isLeaseProvider(harness.pubsub)) throw new Error('lease unavailable');
    expect(
      await harness.pubsub.getLeaseOwner(`${RESOURCE_ID}\0${directThreadId}`),
    ).toBeUndefined();
    expect(harness.start).not.toHaveBeenCalled();
    const threadless = await within(
      harness.agent.stream('thread-less direct stream', {
        runId: crypto.randomUUID(),
        requestContext: actorContext(),
      }),
      'thread-less direct stream setup',
    );
    await within(
      threadless.output.consumeStream(),
      'thread-less direct stream terminal',
    );
    expect(threadless.output.status).toBe('failed');
    expect(harness.start).not.toHaveBeenCalled();
    await expect(
      within(
        harness.agent.generate('direct generate', {
          runId: crypto.randomUUID(),
          requestContext: actorContext(),
        }),
        'direct generate terminal',
      ),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);

    await expect(
      within(
        harness.agent.streamUntilPersisted(
          'until idle',
          {
            runId: crypto.randomUUID(),
            untilIdle: true,
            requestContext: actorContext(),
          },
          'operator',
          'human',
          undefined,
          undefined,
          undefined,
          {
            startIdentity: {
              owner: { kind: 'human', id: 'operator' },
              target: { kind: 'agent', id: 'writer', threadId: directThreadId },
            },
            agentStart: { threaded: false },
            onPreparedStartIdentity: undefined,
          },
        ),
        'untilIdle refusal',
      ),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);

    let finishStart!: () => void;
    harness.start.mockImplementationOnce(
      (_workflowId, options: { runId: string }) =>
        new Promise((resolve) => {
          finishStart = () =>
            resolve({
              runId: options.runId,
              status: 'failed' as const,
              error: 'host test terminal',
            });
        }),
    );
    const hostId = crypto.randomUUID();
    const first = harness.agent.streamUntilPersisted(
      'host stream',
      { runId: hostId, requestContext: actorContext() },
      'operator',
      'human',
      undefined,
      undefined,
      undefined,
      {
        startIdentity: {
          owner: { kind: 'human', id: 'operator' },
          target: { kind: 'agent', id: 'writer', threadId: hostId },
        },
        agentStart: { threaded: false },
        onPreparedStartIdentity: undefined,
      },
    );
    await vi.waitFor(() => expect(harness.start).toHaveBeenCalledOnce());
    await expect(
      harness.agent.stream('collision', {
        runId: hostId,
        requestContext: actorContext(),
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    await expect(
      harness.agent.generate('collision', {
        runId: hostId,
        requestContext: actorContext(),
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    finishStart();
    const firstResult = await within(first, 'host stream persistence');
    await within(firstResult.output.consumeStream(), 'host stream terminal');
    expect(harness.start).toHaveBeenCalledOnce();
    expect(unhandled).toEqual([]);
  }, 15_000);

  it('rejects re-entry after the host waiter settles while the run registry stays live', async () => {
    const harness = await createHarness();
    harness.start.mockImplementationOnce(
      async () =>
        ({
          runId: 'suspended-run',
          status: 'suspended',
          suspended: [['gate']],
        }) as never,
    );
    const hostId = crypto.randomUUID();
    const first = await within(
      harness.agent.streamUntilPersisted(
        'host stream',
        { runId: hostId, requestContext: actorContext() },
        'operator',
        'human',
        undefined,
        undefined,
        undefined,
        {
          startIdentity: {
            owner: { kind: 'human', id: 'operator' },
            target: { kind: 'agent', id: 'writer', threadId: hostId },
          },
          agentStart: { threaded: false },
          onPreparedStartIdentity: undefined,
        },
      ),
      'suspended host stream persistence',
    );
    const liveEntry = globalRunRegistry.get(hostId);
    expect(liveEntry).toBeDefined();

    await expect(
      harness.agent.stream('collision', {
        runId: hostId,
        requestContext: actorContext(),
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(globalRunRegistry.get(hostId)).toBe(liveEntry);
    await expect(
      harness.agent.streamUntilPersisted(
        'second host start',
        { runId: hostId, requestContext: actorContext() },
        'operator',
        'human',
        undefined,
        undefined,
        undefined,
        {
          startIdentity: {
            owner: { kind: 'human', id: 'operator' },
            target: { kind: 'agent', id: 'writer', threadId: hostId },
          },
          agentStart: { threaded: false },
          onPreparedStartIdentity: undefined,
        },
      ),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(globalRunRegistry.get(hostId)).toBe(liveEntry);
    await expect(
      harness.agent.prepare('collision', {
        runId: hostId,
        requestContext: actorContext(),
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(globalRunRegistry.get(hostId)).toBe(liveEntry);
    await expect(
      harness.agent.generate('collision', {
        runId: hostId,
        requestContext: actorContext(),
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(globalRunRegistry.get(hostId)).toBe(liveEntry);

    await (
      harness.agent as unknown as {
        emitError(runId: string, error: Error): Promise<void>;
      }
    ).emitError(hostId, new Error('test cleanup'));
    await within(first.output.consumeStream(), 'suspended stream cleanup');
  }, 15_000);
});

describe('proof activation actual agent authority', () => {
  it('uses the actual wrapper workflow and refuses a replaced generation after content inspection', async () => {
    const sqlite = openSqlite();
    const db = sqliteUnitDatabase(sqlite) as ExecutionFenceDatabase;
    const pubsub = createHostPubSub();
    const runner = init({ DB: db }, { pubsub, tablePrefix: 'proof_' });
    const fence = runner.executionFence;
    if (!fence) throw new Error('fixture fence missing');
    const memory = new MockMemory();
    const agent = createFlowsafeDurableAgent({
      agent: guardedTestAgent(memory),
      runtime: runner.runtime,
      pubsub,
      cache: false,
    });
    const workflowId = agent.getWorkflow().id;
    const threadId = 'thread-real-proof';
    const runId = 'run-real-proof';
    const source = {
      version: 2,
      startToken: 'generation',
      attemptToken: 'attempt',
      resumeCounts: [],
      startIdentity: {
        owner: { kind: 'human', id: 'operator' },
        target: { kind: 'agent', id: 'writer', threadId },
      },
      agentStart: { threaded: true },
    };
    sqlite.exec(
      'CREATE TABLE proof_mastra_workflow_snapshot (workflow_name TEXT, run_id TEXT, resourceId TEXT, snapshot TEXT, createdAt TEXT, updatedAt TEXT, PRIMARY KEY (workflow_name,run_id))',
    );
    const write = () =>
      sqlite
        .prepare(
          'INSERT OR REPLACE INTO proof_mastra_workflow_snapshot VALUES (?,?,?,?,?,?)',
        )
        .run(
          workflowId,
          runId,
          threadId,
          JSON.stringify({
            runId,
            status: 'suspended',
            requestContext: { 'flowsafe.runProvenance': source },
            steps: {},
            suspendedPaths: {},
          }),
          '2026-09-07T00:00:00Z',
          '2026-09-07T00:00:00Z',
        );
    write();
    await fence.seed('migration-locked');
    await fence.transition({
      expected: 'migration-locked',
      next: 'proof-only',
      proofKey: 'proof',
    });
    sqlite
      .prepare(
        'UPDATE flowsafe_execution_fence SET proof_run_id = ?, proof_table_prefix = ?, proof_workflow_id = ?, proof_start_token = ?',
      )
      .run(runId, 'proof_', workflowId, 'generation');
    vi.spyOn(agent, 'getActiveThreadRunId').mockReturnValue(runId);
    const send = vi.spyOn(agent, 'sendMessage');
    const saves = vi.spyOn(memory, 'saveMessages');
    expect(
      await agent.proofExecutionFor(runner.runtime, threadId, runId),
    ).toMatchObject({
      workflowId,
      startToken: 'generation',
      tablePrefix: 'proof_',
    });
    const route = createThreadSignalRoutes({
      resolveAgent: () => agent as unknown as Agent,
      resolveResourceId: () => threadId,
      contentPolicy: async () => {
        source.startToken = 'replacement';
        write();
        return { allowed: true };
      },
    });
    const response = await route(
      post('/signal/queue', { contents: 'held content' }),
      {
        threadId,
        principal: humanPrincipal({ id: 'operator', role: 'operator' }),
        init: runner,
      },
    );
    expect(send).not.toHaveBeenCalled();
    expect(saves).not.toHaveBeenCalled();
    expect(response?.status).toBe(503);
  });
});

function wrapper(options: {
  cache?: 'default' | false;
  pubsub?: ReturnType<typeof createHostPubSub>;
  runtimePubsub?: ReturnType<typeof createHostPubSub>;
}) {
  const memory = new MockMemory();
  const mastra = new Mastra({
    logger: false,
    agents: { writer: guardedTestAgent(memory) },
    ...(options.runtimePubsub ? { pubsub: options.runtimePubsub } : {}),
  });
  const rawAgent = mastra.getAgentById('writer');
  const { runtime, start } = fakeRuntime(options.runtimePubsub);
  const agent = createFlowsafeDurableAgent({
    agent: rawAgent,
    runtime,
    ...(options.pubsub ? { pubsub: options.pubsub } : {}),
    ...(options.cache === false ? { cache: false } : {}),
    threadRuntime: mastra.agentThreadStreamRuntime,
  });
  return { agent, mastra, memory, start };
}

describe('durable wrapper direct abort and peer discovery refusals', () => {
  it.each([
    'abortRunStream',
    'abortThreadStream',
  ] as const)('refuses direct %s while a host run stays live', async (method) => {
    const threadId = crypto.randomUUID();
    const fixture = wrapper({});
    const run = await heldRun(fixture, threadId);
    try {
      const controller = globalRunRegistry.get(run.runId)?.abortController;
      expect(controller).toBeDefined();
      expect(controller?.signal.aborted).toBe(false);
      let refusal: unknown;
      try {
        method === 'abortRunStream'
          ? fixture.agent.abortRunStream(run.runId)
          : fixture.agent.abortThreadStream({
              threadId,
              resourceId: RESOURCE_ID,
            });
      } catch (error) {
        refusal = error;
      }
      expect(controller?.signal.aborted).toBe(false);
      expect(refusal).toBeInstanceOf(Error);
      expect((refusal as Error).message).toContain(
        `FlowsafeDurableAgent.${method}() is unavailable`,
      );
    } finally {
      await finishRun(run);
    }
  }, 15_000);

  it('keeps the stream result abort handle working for a host run', async () => {
    const fixture = wrapper({});
    const stream = vi.spyOn(fixture.agent, 'stream');
    const run = await heldRun(fixture, crypto.randomUUID());
    try {
      const controller = globalRunRegistry.get(run.runId)?.abortController;
      expect(controller?.signal.aborted).toBe(false);
      const result = await within(
        stream.mock.results[0]?.value as Promise<{
          abort: () => Promise<void>;
        }>,
        'held host stream result',
      );
      await result.abort();
      expect(controller?.signal.aborted).toBe(true);
    } finally {
      await finishRun(run);
    }
  }, 15_000);

  it('refuses peer discovery across wrappers sharing a pub/sub', async () => {
    const pubsub = createHostPubSub();
    const a = wrapper({ pubsub, runtimePubsub: pubsub });
    const b = wrapper({ pubsub, runtimePubsub: pubsub });
    const claim = await a.agent.claimThreadOwnership({
      threadId: crypto.randomUUID(),
      resourceId: RESOURCE_ID,
      peer: { label: 'advertised peer' },
    });
    expect(claim.claimed).toBe(true);
    try {
      await expect(b.agent.discoverThreadPeers()).rejects.toThrow(
        'FlowsafeDurableAgent.discoverThreadPeers() is unavailable',
      );
    } finally {
      claim.unsubscribe();
    }
  }, 15_000);
});

describe('durable wrapper pub/sub identity', () => {
  it.each(
    cacheShapes,
  )('isolates same-thread runs between wrappers on different pub/subs with $name', async ({
    cache,
  }) => {
    const threadId = crypto.randomUUID();
    const pubsubA = createHostPubSub();
    const pubsubB = createHostPubSub();
    const a = wrapper({ cache, pubsub: pubsubA, runtimePubsub: pubsubA });
    const b = wrapper({ cache, pubsub: pubsubB, runtimePubsub: pubsubB });
    const runA = await heldRun(a, threadId);
    let runB: Awaited<ReturnType<typeof heldRun>> | undefined;
    try {
      expect(
        b.agent.getActiveThreadRunId({ threadId, resourceId: RESOURCE_ID }),
      ).toBeUndefined();
      expect(
        a.agent.getActiveThreadRunId({ threadId, resourceId: RESOURCE_ID }),
      ).toBe(runA.runId);
      runB = await heldRun(b, threadId);
      expect(
        a.agent.getActiveThreadRunId({ threadId, resourceId: RESOURCE_ID }),
      ).toBe(runA.runId);
      expect(
        b.agent.getActiveThreadRunId({ threadId, resourceId: RESOURCE_ID }),
      ).toBe(runB.runId);
    } finally {
      await finishRun(runA);
      if (runB) await finishRun(runB);
      expect(unhandled).toEqual([]);
    }
  }, 15_000);

  it.each(
    cacheShapes.flatMap((shape) => [
      { ...shape, runtimePubsub: false as const },
      { ...shape, runtimePubsub: true as const },
    ]),
  )('delivers and drains a direct signal with $name and runtime pubsub $runtimePubsub', async ({
    cache,
    runtimePubsub,
  }) => {
    const threadId = crypto.randomUUID();
    const pubsub = runtimePubsub ? createHostPubSub() : undefined;
    const fixture = wrapper({ cache, runtimePubsub: pubsub });
    const run = await heldRun(fixture, threadId);
    try {
      expect(fixture.agent.getPubSub()).toBe(pubsub ?? fixture.agent.pubsub);
      const sent = fixture.agent.sendSignal(
        { type: 'reactive', contents: 'direct signal' },
        {
          runId: run.runId,
          threadId,
          resourceId: RESOURCE_ID,
          ifActive: { behavior: 'deliver' },
        },
      );
      await expect(sent.accepted).resolves.toMatchObject({
        action: 'deliver',
        runId: run.runId,
      });
      expect(drained(run.runId)).toEqual([
        expect.objectContaining({ contents: 'direct signal' }),
      ]);
    } finally {
      await finishRun(run);
      expect(unhandled).toEqual([]);
    }
  }, 15_000);

  it.each(
    cacheShapes,
  )('registers one stream on the signal delivery state with $name', async ({
    cache,
  }) => {
    const threadId = crypto.randomUUID();
    const pubsub = createHostPubSub();
    const fixture = wrapper({ cache, pubsub, runtimePubsub: pubsub });
    const registrations = vi.spyOn(
      fixture.mastra.agentThreadStreamRuntime,
      'registerRun',
    );
    const runId = crypto.randomUUID();
    const topic = `agent.thread-stream.${encodeURIComponent(`${RESOURCE_ID}\0${threadId}`)}`;
    const registrationsOnTopic: string[] = [];
    const onEvent = (event: { type: string; runId: string }) => {
      if (event.type === 'run-registered' && event.runId === runId)
        registrationsOnTopic.push(event.runId);
    };
    await pubsub.subscribe(topic, onEvent);
    const run = await heldRun(fixture, threadId, runId);
    try {
      expect(fixture.agent.getPubSub()).toBe(pubsub);
      const calls = registrations.mock.calls.filter(
        ([, , options]) => options.runId === run.runId,
      );
      expect(calls).toHaveLength(1);
      expect(calls[0]?.[3]).toBe(fixture.agent.getPubSub());
      expect(registrationsOnTopic).toEqual([runId]);
    } finally {
      await finishRun(run);
      await pubsub.unsubscribe(topic, onEvent);
      expect(unhandled).toEqual([]);
    }
  }, 15_000);
});
