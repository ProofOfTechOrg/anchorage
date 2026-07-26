// SPDX-License-Identifier: Apache-2.0

import type { MastraCompositeStore } from '@mastra/core/storage';
import type { GuardedAgentHandle } from '@proofoftech/breakwater/agent';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  InitResult,
  RequestContextProvider,
  RunSummary,
  ThreadScope,
} from '../do-runner/index.js';
import { mintResourceId } from '../do-runner/index.js';
import {
  type AgentThreadInstanceScope,
  type AgentThreadStateStorage,
  createThreadAgentHost,
} from './thread-host.js';

const mocked = vi.hoisted(() => ({
  stream: vi.fn(),
  resumeViaRuntime: vi.fn(),
  observe: vi.fn(),
  getHistory: vi.fn(),
}));
const RESOURCE_ID = mintResourceId('acme', 'acme_thread');

vi.mock('@proofoftech/breakwater/agent', () => ({
  isGuardedAgentHandle: (value: unknown) =>
    typeof value === 'object' &&
    value !== null &&
    (value as { guarded?: unknown }).guarded === true,
}));

vi.mock('@mastra/core/mastra', () => ({
  Mastra: class {
    readonly agentThreadStreamRuntime = {};
    readonly agents: Record<string, unknown>;

    constructor(options: { agents: Record<string, unknown> }) {
      this.agents = options.agents;
    }

    getAgent(id: string) {
      return this.agents[id];
    }
  },
}));

vi.mock('../agent-runner/index.js', async (importOriginal) => {
  const original =
    await importOriginal<typeof import('../agent-runner/index.js')>();
  return {
    ...original,
    createFlowsafeDurableAgent: () => {
      const runIds = new Set<string>();
      return {
        streamUntilPersisted: async (...args: unknown[]) => {
          const options = args[1] as { runId?: string } | undefined;
          if (options?.runId) runIds.add(options.runId);
          return mocked.stream(...args);
        },
        resumeViaRuntime: async (...args: unknown[]) => {
          const options = args[0] as { runId?: string } | undefined;
          if (options?.runId) runIds.add(options.runId);
          return mocked.resumeViaRuntime(...args);
        },
        observe: mocked.observe,
        runRegistry: {
          has: (runId: string) => runIds.has(runId),
        },
        pubsub: {
          getHistory: mocked.getHistory,
        },
      };
    },
  };
});

interface Harness {
  host: ReturnType<typeof createThreadAgentHost>;
  state: Map<string, unknown>;
  scope: ThreadScope;
  moduleScopes: AgentThreadInstanceScope[];
  storageScopes: AgentThreadInstanceScope[];
  approvalScopes: AgentThreadInstanceScope[];
  setSummary(summary: RunSummary | null): void;
  setSnapshot(values?: {
    agentId?: string;
    threadId?: string;
    resourceId?: string;
    memory?: boolean;
    requestContext?: Record<string, unknown>;
  }): void;
}

function guarded(id = 'writer'): GuardedAgentHandle {
  return {
    guarded: true,
    id,
    allowedRoles: ['operator'],
    maxSteps: 1,
  } as unknown as GuardedAgentHandle;
}

function harness(agentIds: readonly string[] = ['writer']): Harness {
  const state = new Map<string, unknown>();
  const stateStorage: AgentThreadStateStorage = {
    get: async <T>(key: string) => state.get(key) as T | undefined,
    put: async (key, value) => {
      state.set(key, structuredClone(value));
    },
    delete: async (key) => state.delete(key),
  };
  let summary: RunSummary | null = {
    runId: 'acme_run',
    status: 'success',
  };
  let statusVisible = false;
  let snapshot: unknown;
  const setSnapshot: Harness['setSnapshot'] = (values = {}) => {
    const agentId = values.agentId ?? 'writer';
    const threadId = values.threadId ?? 'acme_thread';
    const resourceId = values.resourceId ?? RESOURCE_ID;
    snapshot = {
      requestContext: {
        ...values.requestContext,
        runId: 'acme_run',
        threadId,
        resourceId,
        'breakwater.auditContext': {
          agentId,
          threadId,
          resourceId,
        },
      },
      context: {
        input: {
          agentId,
          messageListState: {
            memoryInfo:
              values.memory === false ? null : { threadId, resourceId },
          },
        },
      },
    };
  };
  setSnapshot();
  const storage = {
    getStore: async () => ({
      loadWorkflowSnapshot: async () => snapshot,
    }),
  } as unknown as MastraCompositeStore;
  const runtime = {
    status: vi.fn(async (_workflowId: string, runId: string) => {
      const started = mocked.stream.mock.calls.some(
        (call) => call[1]?.runId === runId,
      );
      if (!statusVisible && !started) return null;
      return summary ? { ...summary, runId } : null;
    }),
  };
  const scope = {
    threadId: 'acme_thread',
    tenantId: 'acme',
    actor: { id: 'operator-1', role: 'operator', tenantId: 'acme' },
    requestedBy: 'operator-1',
    init: {
      runtime,
      pubsub: undefined,
    } as unknown as InitResult,
  } satisfies ThreadScope;
  const moduleScopes: AgentThreadInstanceScope[] = [];
  const storageScopes: AgentThreadInstanceScope[] = [];
  const approvalScopes: AgentThreadInstanceScope[] = [];
  const host = createThreadAgentHost({
    buildModules: (instanceScope) => {
      moduleScopes.push(instanceScope);
      return agentIds.map((agentId) => ({
        meta: {
          id: agentId,
          title: agentId,
          description: 'Writes an approved record',
          allowedRoles: ['operator'],
        },
        agent: guarded(agentId),
      }));
    },
    storage: (instanceScope) => {
      storageScopes.push(instanceScope);
      return storage;
    },
    stateStorage: () => stateStorage,
    approvalService: (instanceScope) => {
      approvalScopes.push(instanceScope);
      return {
        list: async () => [],
        create: async () => {
          throw new Error('unexpected approval creation');
        },
      } as unknown as import('../approval-api/index.js').ApprovalService;
    },
  });
  return {
    host,
    state,
    scope,
    moduleScopes,
    storageScopes,
    approvalScopes,
    setSummary: (value) => {
      summary = value;
      statusVisible = true;
    },
    setSnapshot,
  };
}

beforeEach(() => {
  mocked.stream.mockReset().mockResolvedValue({});
  mocked.resumeViaRuntime.mockReset();
  mocked.observe.mockReset();
  mocked.getHistory.mockReset().mockResolvedValue([]);
});

describe('createThreadAgentHost', () => {
  it('constructs cached dependencies with actor-free instance scope', async () => {
    const { host, scope, moduleScopes, storageScopes } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    expect(Object.keys(moduleScopes[0] ?? {}).sort()).toEqual([
      'init',
      'tenantId',
      'threadId',
    ]);
    expect(Object.keys(storageScopes[0] ?? {}).sort()).toEqual([
      'init',
      'tenantId',
      'threadId',
    ]);
    expect(moduleScopes[0]).toBe(storageScopes[0]);
    expect(Object.isFrozen(moduleScopes[0])).toBe(true);
  });

  it('derives trusted actor/correlation after the exact-leg grant', async () => {
    const { host, scope } = harness();
    let release: (() => void) | undefined;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    mocked.stream.mockImplementation(async () => {
      await blocked;
      return {};
    });
    const provider = host.requestContextForRun((async () => ({
      'breakwater.approvedConnectors': [],
      'breakwater.actor': { id: 'forged' },
    })) satisfies RequestContextProvider);
    const started = host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
      safeContext: {
        stored: 'survives',
        runId: 'forged',
        'breakwater.actor': { id: 'forged-from-stored-state' },
      },
    });
    await vi.waitFor(() => expect(mocked.stream).toHaveBeenCalledOnce());
    await expect(
      provider('durable-agentic-loop', 'acme_run', { kind: 'start' }),
    ).resolves.toMatchObject({
      'breakwater.approvedConnectors': [],
      stored: 'survives',
      'breakwater.actor': { id: 'operator-1', role: 'operator' },
      'breakwater.auditContext': {
        agentId: 'writer',
        tenantId: 'acme',
        entryPath: 'http.start',
      },
    });
    await expect(
      provider('unrelated-workflow', 'acme_run', { kind: 'start' }),
    ).resolves.toEqual({
      'breakwater.approvedConnectors': [],
      'breakwater.actor': { id: 'forged' },
    });
    release?.();
    await started;
    const streamOptions = mocked.stream.mock.calls[0]?.[1];
    expect(streamOptions.requestContext.get('stored')).toBe('survives');
    expect(streamOptions.requestContext.get('runId')).toBe('acme_run');
    expect(streamOptions.requestContext.get('breakwater.actor')).toEqual({
      id: 'operator-1',
      role: 'operator',
    });
  });

  it('rejects a simultaneous operation before a second actor can overwrite the persisted principal', async () => {
    const { host, scope, state } = harness();
    let release: (() => void) | undefined;
    mocked.stream.mockImplementation(
      () =>
        new Promise((resolve) => {
          release = () => resolve({});
        }),
    );
    const input = {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start' as const,
    };
    const first = host.start(scope, input);
    await vi.waitFor(() => expect(mocked.stream).toHaveBeenCalledOnce());
    const secondScope: ThreadScope = {
      ...scope,
      actor: { ...scope.actor, id: 'operator-2' },
      requestedBy: 'operator-2',
    };
    await expect(host.start(secondScope, input)).rejects.toMatchObject({
      status: 409,
    });
    expect(state.get('flowsafe:agent-run:v1:acme_run')).toMatchObject({
      principal: { id: 'operator-1' },
    });
    release?.();
    await first;
  });

  it('serializes first binding so concurrent different-agent starts cannot replace it', async () => {
    const { host, scope, state } = harness(['writer', 'reviewer']);
    const starts = [
      host.start(scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_run_writer',
        prompt: 'writer prompt',
        entryPath: 'http.start',
      }),
      host.start(scope, {
        agentId: 'reviewer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_run_reviewer',
        prompt: 'reviewer prompt',
        entryPath: 'http.start',
      }),
    ];
    const results = await Promise.allSettled(starts);
    expect(
      results.filter((result) => result.status === 'fulfilled'),
    ).toHaveLength(1);
    const rejected = results.find((result) => result.status === 'rejected');
    expect(rejected).toMatchObject({
      reason: { status: 409, message: 'thread is bound to another agent' },
    });
    const binding = structuredClone(
      state.get('flowsafe:agent-thread-binding:v1'),
    ) as { agentId: string };
    expect(['writer', 'reviewer']).toContain(binding.agentId);
    const losingAgent = binding.agentId === 'writer' ? 'reviewer' : 'writer';
    await expect(
      host.start(scope, {
        agentId: losingAgent,
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_run_loser_retry',
        prompt: 'retry',
        entryPath: 'http.start',
      }),
    ).rejects.toMatchObject({ status: 409 });
    expect(state.get('flowsafe:agent-thread-binding:v1')).toEqual(binding);
  });

  it('rejects a stored binding whose resource does not belong to the thread', async () => {
    const { host, scope, state } = harness();
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: 'globex_resource',
    });

    await expect(
      host.resolveBoundAgent(scope, {
        agentId: 'writer',
        entryPath: 'signal.message',
      }),
    ).rejects.toMatchObject({ status: 404 });
  });

  it('rejects replay of an existing durable run before a later actor can recreate metadata', async () => {
    const { host, scope, state } = harness();
    const input = {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start' as const,
    };
    await host.start(scope, input);
    const secondScope: ThreadScope = {
      ...scope,
      actor: { ...scope.actor, id: 'operator-2' },
      requestedBy: 'operator-2',
    };
    await expect(host.start(secondScope, input)).rejects.toMatchObject({
      status: 409,
    });
    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(false);
  });

  it.each([
    'running',
    'waiting',
  ] as const)('retains the execution principal while authoritative status is %s', async (status) => {
    const { host, scope, state, setSummary } = harness();
    setSummary({ runId: 'acme_run', status });
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 1,
      agentId: 'writer',
      principal: scope.actor,
      originEntryPath: 'http.start',
    });
    await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}`,
      ),
      scope,
    );
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(true);
  });

  it('deletes the execution principal after authoritative terminal status', async () => {
    const { host, scope, state, setSummary } = harness();
    setSummary({ runId: 'acme_run', status: 'success' });
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 1,
      agentId: 'writer',
      principal: scope.actor,
      originEntryPath: 'http.start',
    });
    await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}`,
      ),
      scope,
    );
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(false);
  });

  it('removes run metadata when start fails and authoritative state is absent', async () => {
    const { host, scope, state, setSummary } = harness();
    setSummary(null);
    mocked.stream.mockRejectedValue(new Error('model unavailable'));
    await expect(
      host.start(scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_run',
        prompt: 'go',
        entryPath: 'http.start',
      }),
    ).rejects.toThrow('model unavailable');
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(false);
  });

  it('does not create a reusable binding for an unthreaded ephemeral run', async () => {
    const { host, scope, state } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      threaded: false,
    });
    expect([...state.keys()]).not.toContain('flowsafe:agent-thread-binding:v1');
    expect(mocked.stream).toHaveBeenCalledWith(
      'scheduled',
      expect.not.objectContaining({ memory: expect.anything() }),
    );
  });

  it.each([
    ['absent', undefined],
    [
      'mismatched',
      {
        version: 1,
        agentId: 'reviewer',
        resourceId: RESOURCE_ID,
      },
    ],
  ] as const)('refuses a threaded scheduled start when the stored binding is %s', async (_label, binding) => {
    const { host, scope, state } = harness(['writer', 'reviewer']);
    if (binding) {
      state.set('flowsafe:agent-thread-binding:v1', binding);
    }
    await expect(
      host.start(scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_scheduled',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(state.get('flowsafe:agent-thread-binding:v1')).toEqual(binding);
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('starts a threaded schedule only through its matching stored binding', async () => {
    const { host, scope, state } = harness();
    const binding = {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    };
    state.set('flowsafe:agent-thread-binding:v1', binding);
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_scheduled',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
    });
    expect(state.get('flowsafe:agent-thread-binding:v1')).toEqual(binding);
    expect(mocked.stream).toHaveBeenCalledOnce();
  });

  it('rejects a snapshot whose thread correlation does not match the addressed DO', async () => {
    const { host, scope, setSnapshot } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    setSnapshot({
      threadId: 'acme_other-thread',
      resourceId: 'acme_other-resource',
    });
    await expect(
      host.route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}`,
        ),
        scope,
      ),
    ).rejects.toMatchObject({ status: 404 });
  });

  it('rehydrates a threaded approval resume with the validated memory binding', async () => {
    const { host, scope, state, approvalScopes, setSummary, setSnapshot } =
      harness();
    setSummary({ runId: 'acme_run', status: 'suspended' });
    setSnapshot({
      requestContext: {
        persistedSafe: 'survives-resume',
        runId: 'forged',
        'breakwater.actor': { id: 'forged' },
        'breakwater.approvedConnectors': ['stale'],
      },
    });
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 1,
      agentId: 'writer',
      principal: scope.actor,
      originEntryPath: 'http.start',
    });
    const provider = host.requestContextForRun(async () => ({
      'breakwater.approvedConnectors': [],
    }));
    let resumedContext: Record<string, unknown> | undefined;
    mocked.resumeViaRuntime.mockImplementation(async () => {
      resumedContext = await provider('durable-agentic-loop', 'acme_run', {
        kind: 'resume',
        step: ['tool'],
        resumeCount: 1,
      });
      return {
        runId: 'acme_run',
        status: 'success',
      };
    });
    const response = await host.route(
      new Request('https://thread/_flowsafe/agent-host/resume', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          entryPath: 'approval.resume',
          resumeData: { approved: true },
        }),
      }),
      scope,
    );
    expect(response?.status).toBe(200);
    expect(mocked.resumeViaRuntime).toHaveBeenCalledWith(
      expect.objectContaining({
        memory: { thread: 'acme_thread', resource: RESOURCE_ID },
      }),
    );
    expect(resumedContext).toMatchObject({
      persistedSafe: 'survives-resume',
      'breakwater.approvedConnectors': [],
      'breakwater.actor': { id: 'operator-1', role: 'operator' },
      runId: 'acme_run',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
    });
    expect(Object.keys(approvalScopes[0] ?? {}).sort()).toEqual([
      'init',
      'tenantId',
      'threadId',
    ]);
  });

  it('resumes an unthreaded suspended run without requiring or inventing memory', async () => {
    const { host, scope, state, setSummary, setSnapshot } = harness();
    setSummary({ runId: 'acme_run', status: 'suspended' });
    setSnapshot({ memory: false });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 1,
      agentId: 'writer',
      principal: scope.actor,
      originEntryPath: 'schedule.fire',
    });
    mocked.resumeViaRuntime.mockResolvedValue({
      runId: 'acme_run',
      status: 'success',
    });
    const response = await host.route(
      new Request('https://thread/_flowsafe/agent-host/resume', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          entryPath: 'approval.resume',
          resumeData: { approved: true },
        }),
      }),
      scope,
    );
    expect(response?.status).toBe(200);
    expect(mocked.resumeViaRuntime).toHaveBeenCalledWith(
      expect.not.objectContaining({ memory: expect.anything() }),
    );
  });

  it('emits next-cursor NDJSON and cancels only the observation reader', async () => {
    const { host, scope } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    let cancelled = false;
    const source = new ReadableStream<unknown>({
      start(controller) {
        controller.enqueue({ type: 'text-delta', text: 'one' });
      },
      cancel() {
        cancelled = true;
      },
    });
    const cleanup = vi.fn();
    mocked.observe.mockResolvedValue({ fullStream: source, cleanup });
    mocked.getHistory.mockResolvedValue(
      Array.from({ length: 6 }, (_, index) => ({ id: String(index) })),
    );
    const response = await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/stream?resourceId=${RESOURCE_ID}&offset=4`,
      ),
      scope,
    );
    expect(response?.headers.get('content-type')).toBe(
      'application/x-ndjson; charset=utf-8',
    );
    const reader = response?.body?.getReader();
    const first = await reader?.read();
    expect(new TextDecoder().decode(first?.value)).toContain('"offset":5');
    await reader?.cancel();
    expect(cancelled).toBe(true);
    expect(cleanup).not.toHaveBeenCalled();
  });

  it('returns 409 after replay state is lost while durable status remains', async () => {
    const first = harness();
    await first.host.start(first.scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });

    const restarted = harness();
    restarted.state.set(
      'flowsafe:agent-thread-binding:v1',
      first.state.get('flowsafe:agent-thread-binding:v1'),
    );
    await expect(
      restarted.host.route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run/stream?resourceId=${RESOURCE_ID}&offset=0`,
        ),
        restarted.scope,
      ),
    ).rejects.toMatchObject({
      status: 409,
      message: expect.stringContaining('status endpoint'),
    });
  });

  it('uses external cached history after an isolate restart', async () => {
    const first = harness();
    await first.host.start(first.scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });

    const restarted = harness();
    restarted.state.set(
      'flowsafe:agent-thread-binding:v1',
      first.state.get('flowsafe:agent-thread-binding:v1'),
    );
    mocked.getHistory.mockResolvedValue([
      { id: '0' },
      { id: '1' },
      { id: '2' },
    ]);
    mocked.observe.mockResolvedValue({
      fullStream: new ReadableStream<unknown>({
        start(controller) {
          controller.enqueue({ type: 'finish' });
          controller.close();
        },
      }),
      cleanup: vi.fn(),
    });
    const response = await restarted.host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/stream?resourceId=${RESOURCE_ID}&offset=1`,
      ),
      restarted.scope,
    );
    expect(response?.status).toBe(200);
    expect(mocked.observe).toHaveBeenCalledWith('acme_run', { offset: 1 });
  });

  it('closes immediately when a terminal reconnect cursor is at cached history end', async () => {
    const { host, scope } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    mocked.getHistory.mockResolvedValue([{ id: '0' }, { id: '1' }]);
    const response = await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/stream?resourceId=${RESOURCE_ID}&offset=2`,
      ),
      scope,
    );
    expect(response?.status).toBe(200);
    await expect(response?.text()).resolves.toBe('');
    expect(mocked.observe).not.toHaveBeenCalled();
  });
});
