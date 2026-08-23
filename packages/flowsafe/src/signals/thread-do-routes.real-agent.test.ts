// SPDX-License-Identifier: Apache-2.0

import type { Agent } from '@mastra/core/agent';
import { globalRunRegistry } from '@mastra/core/agent/durable';
import { isLeaseProvider } from '@mastra/core/events';
import type { MastraModelConfig } from '@mastra/core/llm';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore } from '@mastra/core/storage';
import {
  ACTOR_CONTEXT_KEY,
  AuditLogger,
  createGuardedAgent,
} from '@proofoftech/breakwater';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FLOWSAFE_PERSISTENCE_FORBIDDEN } from '../agent-runner/durable-agent-runner.js';
import {
  createFlowsafeDurableAgent,
  type FlowsafeDurableAgent,
} from '../agent-runner/index.js';
import { humanPrincipal } from '../approval-api/index.js';
import {
  createHostPubSub,
  InvalidRunRequestError,
  init,
  type RunnerRuntime,
  type ThreadScope,
} from '../do-runner/index.js';
import { createThreadSignalRoutes } from './thread-do-routes.js';

const RESOURCE_ID = 'resource-real';

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

function guardedTestAgent(memory: MockMemory): Agent {
  return createGuardedAgent({
    id: 'writer',
    name: 'Writer',
    instructions: 'Answer the request.',
    model: unreachableModel(),
    memory,
    allowedRoles: ['operator'],
    policies: [],
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

function fakeRuntime(pubsub: ReturnType<typeof createHostPubSub>) {
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

async function createHarness(options: { canPersist?: boolean } = {}) {
  const pubsub = createHostPubSub();
  const memory = new MockMemory();
  const { runtime, start } = fakeRuntime(pubsub);
  const mastra = new Mastra({ storage: new InMemoryStore(), logger: false });
  const agent = createFlowsafeDurableAgent({
    agent: guardedTestAgent(memory),
    runtime,
    pubsub,
    cache: false,
    threadRuntime: mastra.agentThreadStreamRuntime,
  });
  mastra.addAgent(agent);
  agent.__setPubSub(pubsub);
  const routes = createThreadSignalRoutes({
    resolveAgent: () => agent as unknown as Agent,
    resolveResourceId: () => RESOURCE_ID,
    resolveBlockingRun: () => undefined,
    serializeDispatch: async (_scope, operation) => operation(),
    canPersist: () => options.canPersist ?? true,
    resolveNotificationsStorage: async () => {
      const storage = await mastra.getStorage()?.getStore('notifications');
      if (!storage) throw new Error('notifications storage unavailable');
      return storage;
    },
  });
  return { agent, mastra, memory, pubsub, routes, start };
}

function scope(
  pubsub: ReturnType<typeof createHostPubSub>,
  threadId: string,
): ThreadScope {
  return {
    threadId,
    principal: humanPrincipal({ id: 'operator', role: 'operator' }),
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

async function recalled(memory: MockMemory, threadId: string) {
  return (
    await memory.recall({
      threadId,
    })
  ).messages;
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

  it('persists idle state and owner notifications without a run', async () => {
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
    const notificationBody = (await notificationResponse?.json()) as {
      delivery: Record<string, unknown>;
    };
    expect(stateBody).toMatchObject({
      decision: { action: 'persist' },
    });
    expect(stateBody.decision).not.toHaveProperty('runId');
    expect(notificationBody).toMatchObject({
      delivery: { action: 'persist' },
      record: {
        decision: { action: 'deliver' },
        record: { status: 'pending' },
      },
    });
    expect(notificationBody.delivery).not.toHaveProperty('runId');
    expect(await recalled(harness.memory, threadId)).toHaveLength(2);
    expect(
      harness.agent.getActiveThreadRunId({
        threadId,
        resourceId: RESOURCE_ID,
      }),
    ).toBeUndefined();
    expect(harness.start).not.toHaveBeenCalled();
  });

  it('records a non-owner notification for dispatcher delivery', async () => {
    const harness = await createHarness({ canPersist: false });
    const threadId = crypto.randomUUID();
    const response = await harness.routes(
      post('/signal/notification', {
        source: 'provider',
        kind: 'changed',
        summary: 'notification input',
      }),
      scope(harness.pubsub, threadId),
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
    expect(harness.start).not.toHaveBeenCalled();
  });

  it.each([
    ['owner leftover', undefined, 1],
    [
      'marked non-owner leftover',
      { [FLOWSAFE_PERSISTENCE_FORBIDDEN]: true },
      0,
    ],
  ] as const)('terminally heals a completion drain for %s', async (_label, signalMetadata, expectedSavedMessages) => {
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
      {
        type: 'reactive',
        contents: 'leftover',
        ...(signalMetadata ? { metadata: signalMetadata } : {}),
      },
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
      'sendNotificationSignal',
      async (agent: FlowsafeDurableAgent, threadId: string) =>
        agent.sendNotificationSignal(
          {
            source: 'provider',
            kind: 'changed',
            summary: 'notification input',
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
  ])('terminally heals direct %s idle wakes', async (name, invoke) => {
    const harness = await createHarness();
    const threadId = crypto.randomUUID();
    await seedThread(harness.memory, threadId);
    const saveMessages = vi.spyOn(harness.memory, 'saveMessages');
    const publish = vi.spyOn(harness.pubsub, 'publish');
    const result = await invoke(harness.agent, threadId);
    if (name === 'sendNotificationSignal') {
      expect(result).toMatchObject({ record: { status: 'delivered' } });
    }
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
