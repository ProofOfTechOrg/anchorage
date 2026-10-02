// SPDX-License-Identifier: Apache-2.0
// The engine-leg-context-to-tool grant round-trip is proven end to end against
// the real runtime, connector, and grant provider in
// agent-gate-round-trip.test.ts.
// The suspended-run live-id window is pinned in
// thread-do-routes.real-agent.test.ts by
// "rejects re-entry after the host waiter settles while the run registry stays
// live".

import { Agent, createSignal } from '@mastra/core/agent';
import {
  AGENT_STREAM_TOPIC,
  AgentStreamEventTypes,
  DurableAgent,
  type DurableAgenticWorkflowInput,
  type ExtendedRunRegistry,
  globalRunRegistry,
  type RunRegistryEntry,
} from '@mastra/core/agent/durable';
import {
  type MastraDBMessage,
  type MastraToolInvocation,
  MessageList,
} from '@mastra/core/agent/message-list';
import { EventEmitterPubSub } from '@mastra/core/events';
import type { MastraModelConfig } from '@mastra/core/llm';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import {
  type OutputResult,
  type Processor,
  ProcessorRunner,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore } from '@mastra/core/storage';
import { createTool } from '@mastra/core/tools';
import type { AnyWorkflow } from '@mastra/core/workflows';
import {
  ACTOR_CONTEXT_KEY,
  AuditLogger,
  classifierPolicy,
  createGuardedAgent,
  denyPatterns,
  policyDenialReason,
  type Role,
} from '@proofoftech/breakwater';
import {
  afterEach,
  assert,
  describe,
  expect,
  expectTypeOf,
  it,
  vi,
} from 'vitest';
import { z } from 'zod';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import { createD1Storage } from '../do-runner/d1-storage.js';
import { DoStatusError } from '../do-runner/do-status-error.js';
import {
  type ExecutionFenceDatabase,
  ExecutionFenceStore,
} from '../do-runner/execution-fence.js';
import {
  FENCED_WORKFLOW_STORAGE,
  type FencedWorkflowAdmissionCapability,
} from '../do-runner/fenced-workflow-capability.js';
import type { FencedWorkflowsStorageD1 } from '../do-runner/fenced-workflows-d1.js';
import {
  createHostPubSub,
  doErrorResponse,
  InvalidExecutionIdentityError,
  InvalidMutationEpochError,
  InvalidRunRequestError,
  type RequestContextProvider,
  type RunnerRuntime,
  type StartRunOptions,
} from '../do-runner/index.js';
import { init } from '../do-runner/init.js';
import {
  RunStateUnreadableError,
  UnknownRunError,
} from '../do-runner/runtime.js';
import {
  AgentRunSelectorMismatchError,
  type AgentStartAuthority,
  type AuthoritativeAgentStartState,
  createFlowsafeDurableAgent,
  DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
  FlowsafeDurableAgent,
  isRuntimeDrivenAgent,
  type LegacyAgentRunState,
} from './durable-agent-runner.js';

// A fake runtime that records register() and start() and models the shared-id
// registry so the idempotency path is exercised. The cast to RunnerRuntime
// stands on what the literal below implements; a runner call to any other
// member of the interface reaches undefined here and throws. `startResult` overrides the summary start() resolves to (e.g. a
// 'failed' run); `pubsub` exposes an identity for the inheritance test.
function fakeRuntime(
  overrides: {
    pubsub?: unknown;
    startResult?: unknown;
    resumeContext?: RequestContext;
  } = {},
) {
  const registered: string[] = [];
  const registerAgent = vi.fn();
  const register = vi.fn((wf: { id: string }) => {
    registered.push(wf.id);
  });
  const workflowIds = vi.fn(() => [...registered]);
  const start = vi.fn(
    async (_workflowId: string, options: StartRunOptions) =>
      overrides.startResult ?? {
        runId: options.runId,
        status: 'suspended' as const,
        suspended: [['gate']],
      },
  );
  const resumeExecution = vi.fn(async (runId: string) => ({
    runId,
    status: 'success' as const,
  }));
  const resume = vi.fn(
    async (
      _workflowId: string,
      runId: string,
      options?: {
        prepareExecution?: (context: RequestContext) => Promise<void>;
      },
    ) => {
      await options?.prepareExecution?.(
        overrides.resumeContext ?? new RequestContext(),
      );
      return resumeExecution(runId);
    },
  );
  const runtime = {
    registerAgent,
    register,
    workflowIds,
    start,
    resume,
    ...(overrides.pubsub !== undefined ? { pubsub: overrides.pubsub } : {}),
  } as unknown as RunnerRuntime;
  return {
    runtime,
    registerAgent,
    register,
    workflowIds,
    start,
    resume,
    resumeExecution,
  };
}

function testAgent(id = 'writer', pubsub?: EventEmitterPubSub): Agent {
  return new Agent({
    id,
    name: id,
    instructions: 'You are a test agent.',
    // A model-router id string (never invoked): executeWorkflow drives the
    // runtime, not the LLM, so the agent only has to construct.
    model: 'openai/gpt-4o-mini',
    pubsub,
  });
}

function guardedTestAgent(
  options: {
    memory?: MockMemory | (() => MockMemory);
    audit?: AuditLogger;
  } = {},
): Agent {
  return createGuardedAgent({
    id: 'writer',
    name: 'Writer',
    instructions: 'Answer the request.',
    model: 'openai/gpt-4o-mini',
    allowedRoles: ['operator'],
    policies: [],
    memory: options.memory,
    audit: options.audit ?? new AuditLogger(),
    maxSteps: 2,
    toolChoice: 'auto',
  }) as unknown as Agent;
}

const UNOWNED_INPUT_MARK = 'MKUNOWNEDINPUT';

function actorContext(role: Role = 'operator'): RequestContext {
  const context = new RequestContext();
  context.set(ACTOR_CONTEXT_KEY, { id: 'actor-1', role });
  return context;
}

function registryFor(agent: FlowsafeDurableAgent): ExtendedRunRegistry {
  return (
    agent as unknown as {
      readonly runRegistryInternal: ExtendedRunRegistry;
    }
  ).runRegistryInternal;
}

function evictCoreRunEntry(fillers: string[]): void {
  for (let index = 0; index < 1000; index++) {
    const id = crypto.randomUUID();
    fillers.push(id);
    globalRunRegistry.set(id, { cleanup: () => undefined } as never);
  }
}

function spyOnRefusedExecute(
  agent: FlowsafeDurableAgent,
  evicted: boolean,
  fillers: string[],
  onTripwire?: (tripwire: RunRegistryEntry['tripwire']) => void,
) {
  const executable = agent as unknown as {
    executeWorkflow(
      runId: string,
      input: DurableAgenticWorkflowInput,
    ): Promise<void>;
  };
  const original = executable.executeWorkflow.bind(agent);
  return vi
    .spyOn(executable, 'executeWorkflow')
    .mockImplementation(async (runId, input) => {
      const tripwire = registryFor(agent).get(runId)?.tripwire;
      expect(tripwire?.processorId).toBe('breakwater-rbac');
      onTripwire?.(tripwire);
      if (evicted) {
        evictCoreRunEntry(fillers);
        expect(globalRunRegistry.has(runId)).toBe(false);
      }
      return original(runId, input);
    });
}

async function guardedResumeFixture(
  memory: MockMemory | (() => MockMemory),
  role: Role = 'operator',
) {
  const audit = new AuditLogger();
  const { runtime, resume, resumeExecution } = fakeRuntime({
    resumeContext: actorContext(role),
  });
  const agent = createFlowsafeDurableAgent({
    agent: guardedTestAgent({ memory, audit }),
    runtime,
    cache: false,
  });
  await agent.prepare('initial request', {
    runId: 'run-1',
    requestContext: actorContext(),
    memory: { thread: 'thread-1', resource: 'resource-1' },
  });
  registryFor(agent).clear();
  globalRunRegistry.clear();
  const observe = vi.spyOn(agent, 'observe').mockResolvedValue({
    output: { id: 'rehydrated' },
  } as never);
  return { agent, audit, resume, resumeExecution, observe };
}

function processorTestAgent(options: {
  inputInvocation: () => void;
  outputInvocation: () => void;
}): Agent {
  const guarded = createGuardedAgent({
    id: 'writer',
    name: 'Writer',
    instructions: 'You are a test agent.',
    model: 'openai/gpt-4o-mini',
    allowedRoles: ['operator', 'admin'],
    policies: [denyPatterns(['blocked-resume-output'], { phases: ['output'] })],
    audit: new AuditLogger(),
    maxSteps: 2,
    toolChoice: 'auto',
    applicationInputProcessors: [
      {
        id: 'application-input',
        processInput: (args) => {
          options.inputInvocation();
          if (args.messages.length === 0) {
            args.abort('application input processor received empty messages');
          }
          return args.messages;
        },
      },
    ],
    applicationOutputProcessors: [
      {
        id: 'application-output',
        processOutputStream: async (args) => args.part,
        processOutputResult: (args) => {
          options.outputInvocation();
          return args.messages;
        },
      },
    ],
  });
  return guarded as unknown as Agent;
}

async function runOutputResultProcessors(
  entry: RunRegistryEntry,
  text: string,
): Promise<void> {
  const message: MastraDBMessage = {
    id: 'output-message',
    role: 'assistant',
    createdAt: new Date(),
    content: { format: 2, parts: [{ type: 'text', text }] },
  };
  const result: OutputResult = {
    text,
    usage: {} as OutputResult['usage'],
    finishReason: 'stop',
    steps: [],
  };
  for (const item of entry.outputProcessors ?? []) {
    const processor = item as Processor;
    if (!processor.processOutputResult) continue;
    await processor.processOutputResult({
      messages: [message],
      messageList: new MessageList(),
      state: {},
      retryCount: 0,
      requestContext: actorContext(),
      abort: (reason) => {
        throw new Error(reason ?? 'output processor aborted');
      },
      result,
    });
  }
}

// executeWorkflow is protected. Reach it through a cast for the drive/guard
// assertions.
function drive(
  agent: FlowsafeDurableAgent,
  runId: unknown,
  input: DurableAgenticWorkflowInput,
): Promise<void> {
  return (
    agent as unknown as {
      executeWorkflow(
        runId: unknown,
        input: DurableAgenticWorkflowInput,
      ): Promise<void>;
    }
  ).executeWorkflow(runId, input);
}

function startAuthority(): AgentStartAuthority {
  return {
    mutationEpoch: 2,
    startIdentity: {
      owner: { kind: 'human', id: 'operator-1' },
      target: { kind: 'agent', id: 'writer', threadId: 'thread-1' },
    },
    agentStart: { threaded: false },
    onPreparedStartIdentity: undefined,
  };
}

const INPUT: DurableAgenticWorkflowInput = {
  __workflowKind: 'durable-agent',
  runId: 'run-1',
  agentId: 'writer',
  messageListState: new MessageList().serialize(),
  toolsMetadata: [],
  modelConfig: { provider: 'test', modelId: 'local' },
  options: {},
  state: {},
  messageId: 'message-1',
};

function bridgeFixture() {
  const fake = fakeRuntime();
  const agent = createFlowsafeDurableAgent({
    agent: testAgent(),
    runtime: fake.runtime,
  });
  const streamResult = { output: { id: 'output' } };
  const stream = vi
    .spyOn(agent, 'stream')
    .mockResolvedValue(streamResult as never);
  const start = (
    authority: AgentStartAuthority = startAuthority(),
    runId = 'run-1',
    dispatch?: { scheduleId: string; dispatchId: string },
  ) =>
    agent.streamUntilPersisted(
      'hello',
      { runId },
      'operator-1',
      'human',
      'attempt-original',
      dispatch,
      'key-original',
      authority,
    );
  return { ...fake, agent, stream, startHost: start, streamResult };
}

function bridgeDeferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function expectAuthorityRefusedBeforeStream(
  source: unknown,
  expected: Error,
) {
  const f = bridgeFixture();
  const nativeSet = Map.prototype.set;
  const installed: unknown[] = [];
  const set = vi.spyOn(Map.prototype, 'set').mockImplementation(function (
    this: Map<unknown, unknown>,
    key,
    value,
  ) {
    if (key === 'run-1') installed.push(value);
    return nativeSet.call(this, key, value);
  });
  const entered = bridgeDeferred();
  const stream = f.stream.getMockImplementation();
  if (!stream) throw new Error('missing bridge stream fixture');
  f.stream.mockImplementation((...args) => {
    entered.resolve();
    return stream.apply(f.agent, args);
  });
  const pending = f.startHost(source as AgentStartAuthority);
  const outcome = pending.catch((cause: unknown) => cause);
  try {
    const error = await Promise.race([
      outcome,
      entered.promise.then(() => Symbol('stream started before refusal')),
    ]);
    expect(error, 'authority must be refused before stream').toBeInstanceOf(
      Error,
    );
    expect(Object.getPrototypeOf(error)).toBe(Object.getPrototypeOf(expected));
    expect(error).toEqual(expected);
    if (
      expected instanceof InvalidExecutionIdentityError ||
      expected instanceof InvalidMutationEpochError
    ) {
      expect(error).toMatchObject({
        name: expected.name,
        message: expected.message,
        status: 400,
        reason: expected.reason,
      });
    } else expect(error).toBe(expected);
    expect(f.stream).not.toHaveBeenCalled();
    expect(f.start).not.toHaveBeenCalled();
    expect(installed).toEqual([]);
  } finally {
    set.mockRestore();
    if (f.stream.mock.calls.length)
      await drive(f.agent, 'run-1', INPUT).catch(() => undefined);
    await outcome;
    f.stream.mockImplementation(stream);
  }
  await expect(
    Promise.all([f.startHost(), drive(f.agent, 'run-1', INPUT)]),
  ).resolves.toHaveLength(2);
}

function localModelFixture(
  onCall: (prompt: unknown) => void,
  chunks: readonly string[] = ['done'],
): MastraModelConfig {
  const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };
  return {
    specificationVersion: 'v2',
    provider: 'flowsafe-test',
    modelId: 'c-local-text',
    supportedUrls: {},
    doGenerate: async (options) => {
      onCall(options.prompt);
      return {
        content: [{ type: 'text', text: chunks.join('') }],
        finishReason: 'stop',
        usage,
        warnings: [],
      };
    },
    doStream: async (options) => {
      onCall(options.prompt);
      return {
        stream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'stream-start', warnings: [] });
            controller.enqueue({ type: 'text-start', id: 'text-1' });
            for (const delta of chunks) {
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta });
            }
            controller.enqueue({ type: 'text-end', id: 'text-1' });
            controller.enqueue({ type: 'finish', finishReason: 'stop', usage });
            controller.close();
          },
        }),
      };
    },
  };
}

async function realAgentBridgeFixture(
  provider?: RequestContextProvider,
  modelFault?: Error,
  threaded = false,
  guarded?: {
    memory: MockMemory;
    prompts: unknown[];
    policies?: ReturnType<typeof denyPatterns>[];
    chunks?: readonly string[];
    tools?: Record<string, ReturnType<typeof createTool>>;
  },
) {
  const sql = openSqlite() as ReturnType<typeof openSqlite> & { close(): void };
  const binding = sqliteUnitDatabase(sql) as ExecutionFenceDatabase;
  const storage = createD1Storage({ binding });
  await storage.init();
  const fence = new ExecutionFenceStore(binding);
  await fence.seed('open');
  for (let index = 0; index < 2; index++) {
    const before = await fence.read();
    const draining = await fence.transition({
      expected: 'open',
      next: 'draining',
      expectedMutationEpoch: before.mutationEpoch,
      expectedRevision: before.transitionRevision,
      advanceMutationEpoch: true,
    });
    await fence.transition({
      expected: 'draining',
      next: 'open',
      expectedMutationEpoch: draining.mutationEpoch,
      expectedRevision: draining.transitionRevision,
    });
  }
  const workflows = (await storage.getStore(
    'workflows',
  )) as FencedWorkflowsStorageD1;
  const native = workflows[FENCED_WORKFLOW_STORAGE];
  if (!native) throw new Error('missing owned workflow capability');
  const counts = { model: 0, callback: 0, admission: 0, terminalization: 0 };
  const capability: FencedWorkflowAdmissionCapability = {
    ...native,
    withInitialAdmission: (input, create) => {
      counts.admission++;
      return native.withInitialAdmission(input, create);
    },
    terminalizeInitialAdmission: (input) => {
      counts.terminalization++;
      return native.terminalizeInitialAdmission(input);
    },
  };
  Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
    value: capability,
    configurable: true,
  });
  const { runtime } = init(
    { storage },
    {
      executionFence: fence,
      startIdempotency: 'none',
      pubsub: createHostPubSub(),
      requestContextForRun: provider,
    },
  );
  const model = localModelFixture((prompt) => {
    counts.model++;
    guarded?.prompts.push(prompt);
    if (modelFault) throw modelFault;
  }, guarded?.chunks);
  const agent = createFlowsafeDurableAgent({
    agent: guarded
      ? (createGuardedAgent({
          id: 'writer',
          name: 'Writer',
          instructions: 'Return done.',
          memory: guarded.memory,
          model,
          allowedRoles: ['operator'],
          policies: guarded.policies ?? [],
          ...(guarded.tools ? { tools: guarded.tools } : {}),
          audit: new AuditLogger(),
          maxSteps: 1,
          toolChoice: 'auto',
        }) as unknown as Agent)
      : new Agent({
          id: 'writer',
          name: 'Writer',
          instructions: 'Return done.',
          ...(threaded ? { memory: new MockMemory() } : {}),
          model,
        }),
    runtime,
    cache: false,
    maxSteps: 1,
  });
  const start = vi.spyOn(runtime, 'start');
  return { sql, fence, workflows, counts, runtime, agent, start };
}

const OUTCOME_MARKER = 'MKCLIENTOUTCOME';
const CLEAN_OUTCOME = 'clean client result';
const APPROVAL = { id: 'approval_1' };

function clientToolOutcome(
  state: MastraToolInvocation['state'],
  fields: Record<string, unknown> = {},
) {
  return {
    type: 'tool-invocation' as const,
    toolInvocation: {
      toolCallId: 'call_1',
      toolName: 'crm_lookup',
      args: { account: 'acme' },
      state,
      ...fields,
    },
  };
}

function assistantMessage(parts: unknown[], role = 'assistant') {
  return {
    id: 'm2',
    role,
    createdAt: new Date('2026-01-01T00:00:02Z'),
    content: { format: 2, parts },
  };
}

function pendingClientCall(approval = false) {
  return clientToolOutcome(
    approval ? 'approval-requested' : 'call',
    approval ? { approval: APPROVAL } : {},
  );
}

async function outcomeMemory({
  approval = false,
  storedResult,
}: {
  approval?: boolean;
  storedResult?: ReturnType<typeof clientToolOutcome>;
} = {}) {
  const memory = new MockMemory({ storage: new InMemoryStore() });
  const createdAt = new Date('2026-01-01T00:00:00Z');
  await memory.saveThread({
    thread: {
      id: 'thread-1',
      resourceId: 'thread-1',
      createdAt,
      updatedAt: createdAt,
      metadata: {},
    },
  });
  await memory.saveMessages({
    messages: [
      {
        id: 'm1',
        role: 'user',
        threadId: 'thread-1',
        resourceId: 'thread-1',
        createdAt: new Date('2026-01-01T00:00:01Z'),
        content: {
          format: 2,
          parts: [{ type: 'text', text: 'Who owns acme?' }],
        },
      },
      {
        ...assistantMessage([
          ...(storedResult ? [storedResult] : []),
          pendingClientCall(approval),
        ]),
        threadId: 'thread-1',
        resourceId: 'thread-1',
      } as never,
    ],
  });
  return memory;
}

async function savedClientCall(memory: MockMemory) {
  const { messages } = await memory.recall({
    threadId: 'thread-1',
    resourceId: 'thread-1',
  });
  const savedPart = messages
    .find(({ id }) => id === 'm2')
    ?.content.parts.find(
      (part) =>
        part.type === 'tool-invocation' &&
        part.toolInvocation.toolCallId === 'call_1',
    );
  return savedPart?.type === 'tool-invocation'
    ? savedPart.toolInvocation
    : undefined;
}

async function runGuarded({
  memory,
  messages,
  runId,
  policies,
  tools,
  chunks,
  readSubscriber = false,
  threaded = true,
}: Omit<
  NonNullable<Parameters<typeof realAgentBridgeFixture>[3]>,
  'prompts'
> & {
  messages: Parameters<FlowsafeDurableAgent['streamUntilPersisted']>[0];
  runId: string;
  readSubscriber?: boolean;
  threaded?: boolean;
}) {
  const prompts: unknown[] = [];
  const f = await realAgentBridgeFixture(
    () => ({ [ACTOR_CONTEXT_KEY]: { id: 'actor-1', role: 'operator' } }),
    undefined,
    false,
    { memory, prompts, policies, tools, chunks },
  );
  const saveMessages = vi.spyOn(memory, 'saveMessages');
  const nativeStream = f.agent.stream.bind(f.agent);
  let subscriber: Awaited<ReturnType<typeof f.agent.stream>> | undefined;
  const stream = vi
    .spyOn(f.agent, 'stream')
    .mockImplementation(async (...args) => {
      const output = await nativeStream(...args);
      subscriber = output;
      return output;
    });
  let result:
    | Awaited<ReturnType<typeof f.agent.streamUntilPersisted>>
    | undefined;
  try {
    result = await f.agent.streamUntilPersisted(
      messages,
      {
        runId,
        ...(threaded
          ? { memory: { thread: 'thread-1', resource: 'thread-1' } }
          : {}),
        requestContext: actorContext(),
        disableBackgroundTasks: true,
      },
      'operator-1',
      'human',
      `${runId}-attempt`,
      undefined,
      undefined,
      { ...startAuthority(), agentStart: { threaded } },
    );
    let subscriberOutput:
      | {
          receivedText: string;
          text: string;
          result: Awaited<ReturnType<typeof result.output.getFullOutput>>;
        }
      | undefined;
    if (readSubscriber) {
      assert(subscriber);
      const received: string[] = [];
      for await (const chunk of subscriber.fullStream) {
        if (chunk.type === 'text-delta') received.push(chunk.payload.text);
      }
      subscriberOutput = {
        receivedText: received.join(''),
        text: await subscriber.output.text,
        result: await subscriber.output.getFullOutput(),
      };
    }
    const text = await result.output.text;
    const tripwire = (await result.output.tripwire)?.reason;
    await globalRunRegistry.get(runId)?.workflowExecution;
    return {
      prompts,
      tripwire,
      saves: saveMessages.mock.calls.length,
      counts: f.counts,
      text,
      subscriber: subscriberOutput,
    };
  } finally {
    await globalRunRegistry
      .get(runId)
      ?.workflowExecution?.catch(() => undefined);
    result?.cleanup();
    registryFor(f.agent).clear();
    globalRunRegistry.delete(runId);
    stream.mockRestore();
    saveMessages.mockRestore();
    f.start.mockRestore();
    f.sql.close();
  }
}

async function runClientOutcome({
  memory,
  parts,
  role = 'assistant',
  mapped = false,
  tools,
}: {
  memory: MockMemory;
  parts: unknown[];
  role?: string;
  mapped?: boolean;
  tools?: Record<string, ReturnType<typeof createTool>>;
}) {
  return runGuarded({
    memory,
    messages: [assistantMessage(parts, role)] as never,
    runId: 'client-outcome',
    policies: [denyPatterns([OUTCOME_MARKER])],
    tools: {
      crm_lookup: createTool({
        id: 'crm_lookup',
        description: 'Look up an account on the client',
        inputSchema: z.object({ account: z.string() }),
        ...(mapped
          ? {
              toModelOutput: () => ({
                type: 'text',
                value: OUTCOME_MARKER,
              }),
            }
          : {}),
      } as never),
      ...tools,
    },
  });
}

describe('durable client tool outcomes merged into memory', () => {
  type OutcomeCase = {
    carries: string;
    state: MastraToolInvocation['state'];
    fields: Record<string, unknown>;
    approval?: boolean;
    role?: string;
    mapped?: boolean;
  };
  const DENIED: OutcomeCase[] = [
    {
      carries: 'a DB result',
      state: 'result',
      fields: { result: OUTCOME_MARKER },
    },
    {
      carries: 'an output-error errorText',
      state: 'output-error',
      fields: { errorText: OUTCOME_MARKER },
    },
    {
      carries: 'an output-denied approval reason',
      state: 'output-denied',
      fields: {
        approval: {
          ...APPROVAL,
          approved: false,
          reason: OUTCOME_MARKER,
        },
      },
      approval: true,
    },
    {
      carries: 'a user-role client result',
      state: 'result',
      fields: { result: OUTCOME_MARKER },
      role: 'user',
    },
    {
      carries: 'a clean result mapped to denied text',
      state: 'result',
      fields: { result: CLEAN_OUTCOME },
      mapped: true,
    },
  ];

  it.each(
    DENIED,
  )('denies $carries before the model and leaves the stored call pending on the durable loop', async (row) => {
    const memory = await outcomeMemory({ approval: row.approval });
    const run = await runClientOutcome({
      memory,
      parts: [clientToolOutcome(row.state, row.fields)],
      role: row.role,
      mapped: row.mapped,
    });
    expect(run.tripwire).toBe(policyDenialReason('deny-patterns', 'input'));
    expect(run.prompts).toEqual([]);
    expect(run.saves).toBe(0);
    expect(await savedClientCall(memory)).toEqual(
      pendingClientCall(row.approval).toolInvocation,
    );
  });

  it('refuses duplicate client tool outcomes before the model and leaves the stored call pending on the durable loop', async () => {
    const memory = await outcomeMemory();
    const run = await runClientOutcome({
      memory,
      parts: [
        clientToolOutcome('result', { result: CLEAN_OUTCOME }),
        clientToolOutcome('output-error', { errorText: 'failed' }),
      ],
    });
    expect(run.tripwire).toBe('input processor failed');
    expect(run.prompts).toEqual([]);
    expect(run.saves).toBe(0);
    expect(await savedClientCall(memory)).toEqual(
      pendingClientCall().toolInvocation,
    );
  });

  it('allows a clean caller result beside an unsent stored denied result and saves the client tool outcome on the durable loop', async () => {
    const memory = await outcomeMemory({
      storedResult: clientToolOutcome('result', {
        toolCallId: 'call_0',
        toolName: 'historical_lookup',
        args: {},
        result: OUTCOME_MARKER,
      }),
    });
    const run = await runClientOutcome({
      memory,
      parts: [clientToolOutcome('result', { result: CLEAN_OUTCOME })],
      tools: {
        historical_lookup: createTool({
          id: 'historical_lookup',
          description: 'Look up historical data on the client',
          inputSchema: z.object({}),
          toModelOutput: () => {
            throw new Error('unsent stored-result mapper called');
          },
        } as never),
      },
    });
    expect(
      run.tripwire,
      'unsent stored-result mapper throws if selected',
    ).toBeUndefined();
    expect(run.prompts).toHaveLength(1);
    expect(JSON.stringify(run.prompts[0])).toContain(CLEAN_OUTCOME);
    expect(JSON.stringify(run.prompts[0])).toContain(OUTCOME_MARKER);
    expect(await savedClientCall(memory)).toMatchObject({
      state: 'result',
      result: CLEAN_OUTCOME,
    });
  });
});

describe('durable caller-visible text', () => {
  it.each([
    [
      'excludes stream-denied output and following text from the subscriber result',
      false,
      'clean p',
    ],
    [
      'on denial at the end of the stream equals received text without the held tail',
      true,
      'clean prefix ',
    ],
  ] as const)('%s', async (_label, terminalDenied, expectedText) => {
    const marker = 'terminal output denied';
    const run = await runGuarded({
      memory: new MockMemory(),
      messages: 'hello',
      runId: terminalDenied ? 'terminal-denied-text' : 'stream-denied-text',
      threaded: false,
      readSubscriber: true,
      chunks: terminalDenied
        ? ['clean prefix ', 'blocked']
        : ['clean prefix ', 'blocked', ' after denial'],
      policies: terminalDenied
        ? [
            {
              ...classifierPolicy({
                name: 'deny-terminal-output',
                phases: ['output'],
                evaluateEveryChars: 1000,
                classify: async (text) =>
                  text.endsWith('blocked')
                    ? { allowed: false, reason: marker }
                    : { allowed: true },
              }),
              holdBackChars: 7,
            },
          ]
        : [denyPatterns(['blocked'], { phases: ['output'] })],
    });
    assert(run.subscriber);
    expect(run.subscriber.receivedText).toBe(expectedText);
    expect(run.subscriber.text).toBe(expectedText);
    expect(run.subscriber.result.text).toBe(expectedText);
    if (terminalDenied) {
      expect(run.subscriber.result.tripwire?.reason).not.toContain(marker);
    }
    expect(run.subscriber.result.tripwire).toMatchObject({
      processorId: 'breakwater-policy-engine',
      reason: terminalDenied
        ? policyDenialReason('deny-terminal-output', 'output')
        : policyDenialReason('deny-patterns', 'output'),
    });
    expect(run.subscriber.result.finishReason).toBe('other');
  });
});

describe('agent bridge capture', () => {
  it('loads stored thread history into a guarded durable model prompt', async () => {
    const memory = new MockMemory({ storage: new InMemoryStore() });
    const createdAt = new Date(Date.now() - 60_000);
    await memory.saveThread({
      thread: {
        id: 'thread-1',
        resourceId: 'thread-1',
        createdAt,
        updatedAt: createdAt,
        metadata: {},
      },
    });
    await memory.saveMessages({
      messages: [
        {
          id: 'stored-history',
          role: 'user',
          threadId: 'thread-1',
          resourceId: 'thread-1',
          createdAt,
          content: {
            format: 2,
            parts: [{ type: 'text', text: 'earlier stored question' }],
          },
        },
      ],
    });
    const run = await runGuarded({
      memory,
      messages: 'New question',
      runId: 'guarded-history',
    });
    expect(run.text).toBe('done');
    expect(run.counts.model).toBe(1);
    expect(run.prompts).toHaveLength(1);
    expect(JSON.stringify(run.prompts[0])).toContain('earlier stored question');
    expect(JSON.stringify(run.prompts[0])).toContain('New question');
  });

  it.each([
    null,
    '2',
    true,
    -1,
    0.5,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
  ])('bridge refuses malformed epoch before stream and permits clean retry: %s', async (epoch) => {
    await expectAuthorityRefusedBeforeStream(
      { ...startAuthority(), mutationEpoch: epoch },
      new InvalidMutationEpochError(),
    );
  });

  it.each([
    ['null', null, 'identity'],
    ['array', [], 'identity'],
    ['empty', {}, 'owner'],
    [
      'owner-null',
      {
        owner: null,
        target: { kind: 'agent', id: 'writer', threadId: 'thread' },
      },
      'owner',
    ],
    [
      'owner-kind',
      {
        owner: { kind: 'invalid', id: 'operator-1' },
        target: { kind: 'agent', id: 'writer', threadId: 'thread' },
      },
      'owner.kind',
    ],
    [
      'owner-id',
      {
        owner: { kind: 'human', id: '' },
        target: { kind: 'agent', id: 'writer', threadId: 'thread' },
      },
      'owner.id',
    ],
    [
      'target-missing',
      { owner: { kind: 'human', id: 'operator-1' } },
      'target',
    ],
    [
      'target-null',
      { owner: { kind: 'human', id: 'operator-1' }, target: null },
      'target',
    ],
    [
      'target-array',
      { owner: { kind: 'human', id: 'operator-1' }, target: [] },
      'target',
    ],
    [
      'target-kind',
      {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'invalid', id: 'writer' },
      },
      'target.kind',
    ],
    [
      'target-id',
      {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'agent', id: 'bad/path', threadId: 'thread' },
      },
      'target.id',
    ],
    [
      'thread-missing',
      {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'agent', id: 'writer' },
      },
      'target.threadId',
    ],
    [
      'thread-null',
      {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'agent', id: 'writer', threadId: null },
      },
      'target.threadId',
    ],
    [
      'thread-path',
      {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'agent', id: 'writer', threadId: 'bad/path' },
      },
      'target.threadId',
    ],
  ] as const)('bridge refuses malformed identity before stream and permits clean retry: %s', async (_label, startIdentity, field) => {
    await expectAuthorityRefusedBeforeStream(
      { ...startAuthority(), startIdentity },
      new InvalidExecutionIdentityError(field),
    );
  });

  it.each([
    {
      location: 'identity-owner',
      key: 'id',
      target: (source, _guard) => source.startIdentity.owner,
    },
    {
      location: 'identity-target',
      key: 'threadId',
      target: (source, _guard) => source.startIdentity.target,
    },
    {
      location: 'guard-owner',
      key: 'owner',
      target: (_source, guard) => guard,
    },
    {
      location: 'guard-id',
      key: 'id',
      target: (_source, guard) => guard.owner,
    },
    {
      location: 'guard-token',
      key: 'reservationToken',
      target: (_source, guard) => guard,
    },
  ] satisfies Array<{
    location: string;
    key: string;
    target: (
      source: ReturnType<typeof startAuthority>,
      guard: NonNullable<AgentStartAuthority['runOwnerGuard']>,
    ) => object;
  }>)('bridge preserves nested first getter faults before map installation: $location', async ({
    location,
    key,
    target,
  }) => {
    const fault = new Error(`first ${location} read`);
    const source = startAuthority();
    const guard = {
      owner: { kind: 'human' as const, id: 'resource-owner' },
      reservationToken: 'token',
    };
    Object.defineProperty(target(source, guard), key, {
      get() {
        throw fault;
      },
    });
    await expectAuthorityRefusedBeforeStream(
      { ...source, runOwnerGuard: guard },
      fault,
    );
  });

  it('bridge captures each owner-guard primitive once', async () => {
    const f = bridgeFixture();
    const once = <T extends string>(value: T) =>
      vi
        .fn<() => T>()
        .mockReturnValueOnce(value)
        .mockImplementation(() => {
          throw new Error('second guard read');
        });
    const kind = once('service');
    const id = once('resource-owner');
    const token = once('reservation');
    const source = {
      ...startAuthority(),
      runOwnerGuard: {
        owner: {
          get kind() {
            return kind();
          },
          get id() {
            return id();
          },
        },
        get reservationToken() {
          return token();
        },
      },
    };
    const pending = f.startHost(source);
    await expect(
      Promise.all([pending, drive(f.agent, 'run-1', INPUT)]),
    ).resolves.toHaveLength(2);
    expect(f.start.mock.calls[0]?.[1].runOwnerGuard).toEqual({
      owner: { kind: 'service', id: 'resource-owner' },
      reservationToken: 'reservation',
    });
    for (const read of [kind, id, token]) expect(read).toHaveBeenCalledTimes(1);
  });

  it.each([
    undefined,
    1,
    2,
    3,
  ])('real agent bridge enforces active mutation epoch at Runtime: %s', async (epoch) => {
    const { sql, fence, workflows, counts, runtime, agent, start } =
      await realAgentBridgeFixture();
    const runId = `real-epoch-${epoch ?? 'missing'}`;
    const attemptToken = `attempt-${epoch ?? 'missing'}`;
    const authority: AgentStartAuthority = {
      ...startAuthority(),
      onPreparedStartIdentity: () => {
        counts.callback++;
      },
    };
    if (epoch === undefined)
      delete (authority as { mutationEpoch?: number }).mutationEpoch;
    else Object.assign(authority, { mutationEpoch: epoch });
    let result:
      | Awaited<ReturnType<typeof agent.streamUntilPersisted>>
      | undefined;
    try {
      expect(await fence.read()).toMatchObject({
        state: 'open',
        mutationEpoch: 2,
        requireMutationEpoch: true,
      });
      const pending = agent.streamUntilPersisted(
        'Return done.',
        { runId, maxSteps: 1, disableBackgroundTasks: true },
        'operator-1',
        'human',
        attemptToken,
        undefined,
        undefined,
        authority,
      );
      if (epoch !== 2) {
        await expect(pending).rejects.toThrow('mutation epoch does not match');
        expect(counts.model).toBe(0);
        return;
      }
      result = await pending;
      expect(await result.output.text).toBe('done');
      await globalRunRegistry.get(runId)?.workflowExecution;
      expect(
        (await runtime.status(agent.getWorkflow().id, runId))?.status,
      ).toBe('success');
      expect(start).toHaveBeenCalledOnce();
      expect(start.mock.calls[0]?.[1].mutationEpoch).toBe(epoch);
      expect(start.mock.calls[0]?.[1].onPreparedStartIdentity).toBe(
        authority.onPreparedStartIdentity,
      );
      const snapshot = await workflows.loadWorkflowSnapshot({
        workflowName: agent.getWorkflow().id,
        runId,
      });
      expect(snapshot?.requestContext?.['flowsafe.runProvenance']).toEqual({
        version: 2,
        requestedBy: 'operator-1',
        requestedByKind: 'human',
        startToken: expect.any(String),
        mutationEpoch: 2,
        startIdentity: authority.startIdentity,
        agentStart: authority.agentStart,
        attemptToken,
        resumeCounts: [],
      });
      for (const key of [
        'mutationEpoch',
        'startIdentity',
        'agentStart',
        'execution',
        'onPreparedStartIdentity',
        'runOwnerGuard',
        'flowsafe.initialAdmission',
      ])
        expect(snapshot?.requestContext).not.toHaveProperty(key);
      expect(counts).toEqual({
        model: 1,
        callback: 1,
        admission: 1,
        terminalization: 0,
      });
    } finally {
      await globalRunRegistry
        .get(runId)
        ?.workflowExecution?.catch(() => undefined);
      result?.cleanup();
      globalRunRegistry.delete(runId);
      start.mockRestore();
      sql.close();
      expect(counts.callback).toBe(epoch === 2 ? 1 : 0);
      expect(counts.admission).toBe(epoch === 2 ? 1 : 0);
      expect(counts.terminalization).toBe(0);
    }
  });

  it.each([
    {
      phase: 'provider-failure',
      providerFails: true,
      modelFails: false,
      loseReceipt: false,
    },
    {
      phase: 'model-failure',
      providerFails: false,
      modelFails: true,
      loseReceipt: false,
    },
    {
      phase: 'lost-receipt',
      providerFails: false,
      modelFails: false,
      loseReceipt: true,
    },
  ] as const)('real agent failure and terminal recovery keep the verified v2 generation: $phase', async ({
    phase,
    providerFails,
    modelFails,
    loseReceipt,
  }) => {
    const fault = new Error(`real ${phase}`);
    const f = await realAgentBridgeFixture(
      providerFails
        ? () => {
            throw fault;
          }
        : undefined,
      modelFails ? fault : undefined,
    );
    const runId = `real-${phase}`;
    const attemptToken = `attempt-${phase}`;
    const workflow = f.agent.getWorkflow();
    let lostReceipts = 0;
    const persist = f.workflows.persistWorkflowSnapshot.bind(f.workflows);
    const persistence = vi
      .spyOn(f.workflows, 'persistWorkflowSnapshot')
      .mockImplementation(async (input) => {
        await persist(input);
        if (
          loseReceipt &&
          input.workflowName === workflow.id &&
          input.runId === runId &&
          input.snapshot.status === 'success' &&
          lostReceipts === 0
        ) {
          lostReceipts++;
          throw fault;
        }
      });
    const streams: Array<Awaited<ReturnType<typeof f.agent.stream>>> = [];
    const nativeStream = f.agent.stream.bind(f.agent);
    const stream = vi
      .spyOn(f.agent, 'stream')
      .mockImplementation(async (...args) => {
        const result = await nativeStream(...args);
        streams.push(result);
        return result;
      });
    const callback = () => {
      f.counts.callback++;
    };
    const authority: AgentStartAuthority = {
      ...startAuthority(),
      onPreparedStartIdentity: callback,
    };
    try {
      expect(await f.fence.read()).toMatchObject({
        state: 'open',
        mutationEpoch: 2,
        requireMutationEpoch: true,
      });
      const pending = f.agent.streamUntilPersisted(
        'Return done.',
        {
          runId,
          disableBackgroundTasks: true,
          modelSettings: { maxRetries: 0 },
        },
        'operator-1',
        'human',
        attemptToken,
        undefined,
        undefined,
        authority,
      );
      if (loseReceipt) {
        const result = await pending;
        expect(await result.output.text).toBe('done');
      } else if (providerFails) await expect(pending).rejects.toBe(fault);
      else await expect(pending).rejects.toThrow('real model-failure');
      const execution = globalRunRegistry.get(runId)?.workflowExecution;
      if (execution) await execution.catch(() => undefined);
      else expect(globalRunRegistry.has(runId)).toBe(false);
      expect(f.start).toHaveBeenCalledOnce();
      expect(f.start.mock.calls[0]?.[1].onPreparedStartIdentity).toBe(callback);
      expect(f.start.mock.calls[0]?.[1].mutationEpoch).toBe(2);
      const snapshot = await f.workflows.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId,
      });
      if (providerFails) {
        expect(snapshot).toBeNull();
        expect(f.counts.model).toBe(0);
        expect(persistence).not.toHaveBeenCalled();
      } else {
        expect(snapshot?.status).toBe('success');
        if (modelFails)
          expect(snapshot?.result).toMatchObject({
            stepResult: { reason: 'error' },
            output: { text: '', steps: [{ finishReason: 'error' }] },
          });
        expect((await f.runtime.status(workflow.id, runId))?.status).toBe(
          'success',
        );
        expect(snapshot?.requestContext?.['flowsafe.runProvenance']).toEqual({
          version: 2,
          requestedBy: 'operator-1',
          requestedByKind: 'human',
          startToken: expect.any(String),
          mutationEpoch: 2,
          startIdentity: authority.startIdentity,
          agentStart: authority.agentStart,
          attemptToken,
          resumeCounts: [],
        });
        for (const key of [
          'mutationEpoch',
          'startIdentity',
          'agentStart',
          'execution',
          'onPreparedStartIdentity',
          'runOwnerGuard',
          'flowsafe.initialAdmission',
        ])
          expect(snapshot?.requestContext).not.toHaveProperty(key);
        expect(f.counts.model).toBe(1);
      }
      expect(lostReceipts).toBe(loseReceipt ? 1 : 0);
    } finally {
      await globalRunRegistry
        .get(runId)
        ?.workflowExecution?.catch(() => undefined);
      for (const result of streams) result.cleanup();
      globalRunRegistry.delete(runId);
      stream.mockRestore();
      persistence.mockRestore();
      f.start.mockRestore();
      f.sql.close();
      expect(f.counts.callback).toBe(providerFails ? 0 : 1);
      expect(f.counts.admission).toBe(providerFails ? 0 : 1);
      expect(f.counts.terminalization).toBe(0);
    }
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('bridge forwards a frozen authority captured before stream', async () => {
    const f = bridgeFixture();
    const entered = bridgeDeferred();
    const release = bridgeDeferred();
    f.stream.mockImplementation(async () => {
      entered.resolve();
      await release.promise;
      return f.streamResult as never;
    });
    const callback = vi.fn();
    const source = {
      mutationEpoch: 2,
      startIdentity: {
        owner: { kind: 'human' as const, id: 'operator-1' },
        target: {
          kind: 'agent' as const,
          id: 'writer',
          threadId: 'original-thread',
        },
      },
      agentStart: { threaded: true },
      onPreparedStartIdentity: callback,
      runOwnerGuard: {
        owner: { kind: 'service' as const, id: 'resource-owner' },
        reservationToken: 'reservation-original',
      },
    };
    const pending = f.startHost(source);
    void pending.catch(() => undefined);
    try {
      await entered.promise;
      source.mutationEpoch = 3;
      source.startIdentity.owner.id = 'replacement';
      source.startIdentity.target.threadId = 'replacement-thread';
      source.agentStart.threaded = false;
      source.onPreparedStartIdentity = vi.fn();
      source.runOwnerGuard.owner.id = 'replacement-owner';
      source.runOwnerGuard.reservationToken = 'replacement-token';
      release.resolve();
      await drive(f.agent, 'run-1', INPUT);
      await pending;
      const forwarded = f.start.mock.calls[0]?.[1] as StartRunOptions;
      expect(forwarded).toMatchObject({
        mutationEpoch: 2,
        startIdentity: {
          owner: { kind: 'human', id: 'operator-1' },
          target: { kind: 'agent', id: 'writer', threadId: 'original-thread' },
        },
        agentStart: { threaded: true },
        runOwnerGuard: {
          owner: { kind: 'service', id: 'resource-owner' },
          reservationToken: 'reservation-original',
        },
      });
      expect(forwarded.onPreparedStartIdentity).toBe(callback);
      for (const value of [
        forwarded.startIdentity,
        forwarded.startIdentity?.owner,
        forwarded.startIdentity?.target,
        forwarded.agentStart,
        forwarded.runOwnerGuard,
        forwarded.runOwnerGuard?.owner,
      ])
        expect(Object.isFrozen(value)).toBe(true);
      for (const value of [
        source,
        source.startIdentity,
        source.startIdentity.owner,
        source.startIdentity.target,
        source.agentStart,
        source.runOwnerGuard,
        source.runOwnerGuard.owner,
        callback,
      ])
        expect(Object.isFrozen(value)).toBe(false);
      expect(callback).not.toHaveBeenCalled();
      expect(f.stream.mock.calls[0]?.[1]).not.toHaveProperty('startIdentity');
      expect(forwarded.inputData).not.toHaveProperty('onPreparedStartIdentity');
    } finally {
      release.resolve();
      await pending.catch(() => undefined);
    }
  });

  it('bridge never rereads authority after stream handoff', async () => {
    const f = bridgeFixture();
    const authority = startAuthority();
    const reads = new Map<string, number>();
    const once = <T extends object>(object: T, prefix: string): T => {
      for (const key of Object.keys(object)) {
        const value = object[key as keyof T];
        Object.defineProperty(object, key, {
          configurable: true,
          enumerable: false,
          get() {
            const name = `${prefix}.${key}`;
            const count = (reads.get(name) ?? 0) + 1;
            reads.set(name, count);
            if (count > 1) throw new Error(`second read: ${name}`);
            return value;
          },
        });
      }
      return object;
    };
    once(authority.startIdentity.owner, 'owner');
    once(authority.startIdentity.target, 'target');
    once(authority.startIdentity, 'identity');
    once(authority.agentStart, 'mode');
    once(authority, 'authority');
    const pending = f.startHost(authority);
    void pending.catch(() => undefined);
    let driven = false;
    try {
      expect([...reads.keys()].sort()).toEqual(
        [
          'owner.kind',
          'owner.id',
          'target.kind',
          'target.id',
          'target.threadId',
          'identity.owner',
          'identity.target',
          'mode.threaded',
          'authority.mutationEpoch',
          'authority.startIdentity',
          'authority.agentStart',
          'authority.onPreparedStartIdentity',
        ].sort(),
      );
      expect([...reads.values()]).toEqual(Array(reads.size).fill(1));
      driven = true;
      await expect(drive(f.agent, 'run-1', INPUT)).resolves.toBeUndefined();
      await expect(pending).resolves.toBe(f.streamResult);
      expect([...reads.values()]).toEqual(Array(reads.size).fill(1));
      expect(f.start).toHaveBeenCalledOnce();
      expect(f.start.mock.calls[0]?.[1]).toMatchObject({
        mutationEpoch: 2,
        agentStart: { threaded: false },
      });
    } finally {
      if (!driven) await drive(f.agent, 'run-1', INPUT).catch(() => undefined);
      await pending.catch(() => undefined);
    }
  });

  it('bridge captures schedule dispatch before installing stream state', async () => {
    const f = bridgeFixture();
    const ids = {
      scheduleId: 'schedule-original',
      dispatchId: 'dispatch-original',
    };
    const scheduleId = vi.fn(() => ids.scheduleId);
    const dispatchId = vi.fn(() => ids.dispatchId);
    const dispatch = {
      get scheduleId() {
        return scheduleId();
      },
      get dispatchId() {
        return dispatchId();
      },
    };
    const pending = f.startHost(startAuthority(), 'run-1', dispatch);
    ids.scheduleId = 'schedule-late';
    ids.dispatchId = 'dispatch-late';
    await Promise.all([drive(f.agent, 'run-1', INPUT), pending]);
    expect(f.start.mock.calls[0]?.[1]).toMatchObject({
      scheduleDispatch: {
        scheduleId: 'schedule-original',
        dispatchId: 'dispatch-original',
      },
    });
    expect(scheduleId).toHaveBeenCalledTimes(1);
    expect(dispatchId).toHaveBeenCalledTimes(1);
  });

  it('copies Core payload without rereading runId or agentId', async () => {
    const f = bridgeFixture();
    const runId = vi
      .fn()
      .mockReturnValueOnce('run-1')
      .mockImplementation(() => {
        throw new Error('second runId read');
      });
    const agentId = vi
      .fn()
      .mockReturnValueOnce('writer')
      .mockImplementation(() => {
        throw new Error('second agentId read');
      });
    const input: DurableAgenticWorkflowInput = {
      ...INPUT,
      get runId() {
        return runId();
      },
      get agentId() {
        return agentId();
      },
    };
    const pending = f.startHost();
    await expect(
      Promise.all([drive(f.agent, 'run-1', input), pending]),
    ).resolves.toHaveLength(2);
    expect(runId).toHaveBeenCalledTimes(1);
    expect(agentId).toHaveBeenCalledTimes(1);
    expect(f.start.mock.calls[0]?.[1]).toMatchObject({ inputData: INPUT });
  });

  it('bridge rejects authority supplied only through stream options', async () => {
    const f = bridgeFixture();
    const options = { runId: 'run-1', authority: startAuthority() };
    f.stream.mockImplementation(async () => {
      await drive(f.agent, 'run-1', INPUT);
      return f.streamResult as never;
    });
    const outcome = await f.agent
      .streamUntilPersisted(
        'hello',
        options,
        'operator-1',
        'human',
        undefined,
        undefined,
        undefined,
        undefined as never,
      )
      .catch((error: unknown) => error);
    expect(f.stream).not.toHaveBeenCalled();
    expect(f.start).not.toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(InvalidRunRequestError);
    await f.startHost();
  });

  it('bridge rejects authority supplied only through Core input', async () => {
    const f = bridgeFixture();
    const nativeSet = Map.prototype.set;
    const spy = vi.spyOn(Map.prototype, 'set').mockImplementation(function (
      this: Map<unknown, unknown>,
      key,
      value,
    ) {
      if (
        key === 'run-1' &&
        value &&
        typeof value === 'object' &&
        Object.hasOwn(value, 'startIdentity')
      )
        return this;
      return nativeSet.call(this, key, value);
    });
    try {
      const pending = f.startHost();
      const input = { ...INPUT, authority: startAuthority() };
      const results = await Promise.allSettled([
        pending,
        drive(f.agent, 'run-1', input),
      ]);
      expect(f.start).not.toHaveBeenCalled();
      expect(results.map((result) => result.status)).toEqual([
        'rejected',
        'rejected',
      ]);
      for (const result of results)
        if (result.status === 'rejected')
          expect(result.reason).toBeInstanceOf(InvalidRunRequestError);
    } finally {
      spy.mockRestore();
    }
  });

  it.each([
    {
      exit: 'success',
      streamThrows: false,
      callsOnError: false,
      runtimeRefuses: false,
      drives: true,
      succeeds: true,
    },
    {
      exit: 'stream-throw',
      streamThrows: true,
      callsOnError: false,
      runtimeRefuses: false,
      drives: false,
      succeeds: false,
    },
    {
      exit: 'onError',
      streamThrows: false,
      callsOnError: true,
      runtimeRefuses: false,
      drives: false,
      succeeds: false,
    },
    {
      exit: 'runtime-refusal',
      streamThrows: false,
      callsOnError: false,
      runtimeRefuses: true,
      drives: true,
      succeeds: false,
    },
  ] as const)('bridge removes authority on every exit and isolates same-run retries ($exit)', async ({
    streamThrows,
    callsOnError,
    runtimeRefuses,
    drives,
    succeeds,
  }) => {
    const f = bridgeFixture();
    const nativeSet = Map.prototype.set;
    const nativeDelete = Map.prototype.delete;
    let authorityMap: Map<unknown, unknown> | undefined;
    let deletions = 0;
    const set = vi.spyOn(Map.prototype, 'set').mockImplementation(function (
      this: Map<unknown, unknown>,
      key,
      value,
    ) {
      if (
        key === 'run-1' &&
        value &&
        typeof value === 'object' &&
        Object.hasOwn(value, 'startIdentity')
      )
        authorityMap = this;
      return nativeSet.call(this, key, value);
    });
    const remove = vi
      .spyOn(Map.prototype, 'delete')
      .mockImplementation(function (this: Map<unknown, unknown>, key) {
        if (this === authorityMap && key === 'run-1') deletions++;
        return nativeDelete.call(this, key);
      });
    const failure = new InvalidRunRequestError('test refusal');
    try {
      if (streamThrows) f.stream.mockRejectedValueOnce(failure);
      if (callsOnError)
        f.stream.mockImplementationOnce(async (_messages, options) => {
          await options?.onError?.({ error: failure } as never);
          return f.streamResult as never;
        });
      if (runtimeRefuses) f.start.mockRejectedValueOnce(failure);
      const pending = f.startHost();
      const outcomes = await Promise.allSettled(
        drives ? [pending, drive(f.agent, 'run-1', INPUT)] : [pending],
      );
      expect(outcomes[0]?.status).toBe(succeeds ? 'fulfilled' : 'rejected');
      expect(authorityMap).toBeDefined();
      expect(authorityMap?.size).toBe(0);
      expect(deletions).toBe(1);
      const next = { ...startAuthority(), mutationEpoch: 3 };
      await Promise.all([f.startHost(next), drive(f.agent, 'run-1', INPUT)]);
      expect(f.start.mock.lastCall?.[1]).toMatchObject({ mutationEpoch: 3 });
      expect(authorityMap?.size).toBe(0);
      expect(deletions).toBe(2);
    } finally {
      set.mockRestore();
      remove.mockRestore();
    }
  });

  it.each([
    undefined,
    null,
    1,
    [],
    {},
    { ...startAuthority(), onPreparedStartIdentity: 1 },
    { ...startAuthority(), agentStart: { threaded: 'true' } },
    {
      ...startAuthority(),
      runOwnerGuard: {
        owner: { kind: 'human', id: 'owner' },
        reservationToken: '../bad',
      },
    },
    {
      ...startAuthority(),
      startIdentity: {
        owner: { kind: 'human', id: 'other' },
        target: { kind: 'agent', id: 'writer', threadId: 'thread-1' },
      },
    },
    {
      ...startAuthority(),
      startIdentity: {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'workflow', id: 'writer' },
      },
    },
  ])('refuses malformed authority before stream and permits clean retry %#', async (authority) => {
    const f = bridgeFixture();
    await expect(
      f.agent.streamUntilPersisted(
        'hello',
        { runId: 'run-1' },
        'operator-1',
        'human',
        undefined,
        undefined,
        undefined,
        authority as never,
      ),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(f.stream).not.toHaveBeenCalled();
    expect(f.start).not.toHaveBeenCalled();
    await Promise.all([f.startHost(), drive(f.agent, 'run-1', INPUT)]);
  });

  it('refuses missing and inherited callback properties and preserves capture faults', async () => {
    const f = bridgeFixture();
    const { onPreparedStartIdentity: _callback, ...missing } = startAuthority();
    const inherited = Object.assign(
      Object.create({ onPreparedStartIdentity: undefined }),
      missing,
    );
    for (const source of [missing, inherited]) {
      await expect(
        f.startHost(source as AgentStartAuthority),
      ).rejects.toBeInstanceOf(InvalidRunRequestError);
    }
    const fault = new Error('first authority read');
    const source = {
      ...startAuthority(),
      get mutationEpoch(): number {
        throw fault;
      },
    };
    await expect(f.startHost(source)).rejects.toBe(fault);
    const dispatch = {
      get scheduleId(): string {
        throw fault;
      },
      dispatchId: 'dispatch',
    };
    await expect(f.startHost(startAuthority(), 'run-1', dispatch)).rejects.toBe(
      fault,
    );
    expect(f.stream).not.toHaveBeenCalled();
    await Promise.all([f.startHost(), drive(f.agent, 'run-1', INPUT)]);
  });

  it.each([
    {
      kind: 'run',
      inputOverrides: { runId: 'other' },
      wrappedAgent: false,
      firstRead: false,
    },
    {
      kind: 'core-agent',
      inputOverrides: { agentId: 'other' },
      wrappedAgent: false,
      firstRead: false,
    },
    {
      kind: 'wrapped-agent',
      inputOverrides: { agentId: 'other' },
      wrappedAgent: true,
      firstRead: false,
    },
    {
      kind: 'first-read',
      inputOverrides: {},
      wrappedAgent: false,
      firstRead: true,
    },
  ] as const)('refuses mismatched Core correlation and preserves first faults ($kind)', async ({
    inputOverrides,
    wrappedAgent,
    firstRead,
  }) => {
    const f = bridgeFixture();
    const fault = new Error('first Core read');
    const input = {
      ...INPUT,
      ...inputOverrides,
    };
    if (firstRead)
      Object.defineProperty(input, 'agentId', {
        get() {
          throw fault;
        },
      });
    const authority = startAuthority();
    const changed = wrappedAgent
      ? {
          ...authority,
          startIdentity: {
            ...authority.startIdentity,
            target: { ...authority.startIdentity.target, id: 'other' },
          },
        }
      : authority;
    const results = await Promise.allSettled([
      f.startHost(changed),
      drive(f.agent, 'run-1', input),
    ]);
    expect(f.start).not.toHaveBeenCalled();
    for (const result of results) {
      expect(result.status).toBe('rejected');
      if (result.status === 'rejected') {
        if (firstRead) expect(result.reason).toBe(fault);
        else expect(result.reason).toBeInstanceOf(InvalidRunRequestError);
      }
    }
  });

  it('isolates interleaved different-run authorities and uses the actual workflow id', async () => {
    const f = bridgeFixture();
    const workflow = f.agent.getWorkflow();
    const originalId = Object.getOwnPropertyDescriptor(workflow, 'id');
    Object.defineProperty(workflow, 'id', {
      value: 'actual-workflow',
      configurable: true,
    });
    try {
      const first = f.startHost(
        { ...startAuthority(), mutationEpoch: 1 },
        'run-1',
      );
      const second = f.startHost(
        { ...startAuthority(), mutationEpoch: 3 },
        'run-2',
      );
      await drive(f.agent, 'run-2', { ...INPUT, runId: 'run-2' });
      await second;
      await drive(f.agent, 'run-1', INPUT);
      await first;
      expect(
        f.start.mock.calls.map(([id, options]) => [id, options]),
      ).toMatchObject([
        ['actual-workflow', { runId: 'run-2', mutationEpoch: 3 }],
        ['actual-workflow', { runId: 'run-1', mutationEpoch: 1 }],
      ]);
    } finally {
      if (originalId) Object.defineProperty(workflow, 'id', originalId);
      else Reflect.deleteProperty(workflow, 'id');
    }
  });
});

describe('createFlowsafeDurableAgent', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('registers the raw agent and durable-agentic-loop workflow on the runtime', () => {
    // #given a runtime with nothing registered
    const { runtime, register, registerAgent } = fakeRuntime();
    const rawAgent = testAgent();
    // #when a durable agent is created
    createFlowsafeDurableAgent({ agent: rawAgent, runtime });
    // #then the raw agent and shared loop workflow are registered exactly once
    expect(registerAgent).toHaveBeenCalledOnce();
    expect(registerAgent).toHaveBeenCalledWith(rawAgent);
    expect(register).toHaveBeenCalledTimes(1);
    expect(register.mock.calls[0]?.[0]).toMatchObject({
      id: DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
    });
  });

  it('registers the shared workflow once for multiple agents', () => {
    // #given two agents sharing one runtime (both compile to the same loop id)
    const { runtime, register, registerAgent } = fakeRuntime();
    const first = testAgent('a');
    const second = testAgent('b');
    // #when
    createFlowsafeDurableAgent({ agent: first, runtime });
    createFlowsafeDurableAgent({ agent: second, runtime });
    // #then both agents register, but the shared workflow does so only once
    expect(registerAgent).toHaveBeenCalledTimes(2);
    expect(registerAgent).toHaveBeenNthCalledWith(1, first);
    expect(registerAgent).toHaveBeenNthCalledWith(2, second);
    expect(register).toHaveBeenCalledTimes(1);
  });

  it('carries the RUNTIME_DRIVEN_AGENT brand; a plain Agent does not', () => {
    // #given a durable agent + a plain core Agent
    const { runtime } = fakeRuntime();
    const durable = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #then the brand distinguishes the runtime-driven agent from a plain one —
    // the property the thread-DO wake gate requires (a plain Agent's wake
    // would run the loop OFF the runtime).
    expect(isRuntimeDrivenAgent(durable)).toBe(true);
    expect(isRuntimeDrivenAgent(testAgent())).toBe(false);
    expect(isRuntimeDrivenAgent({})).toBe(false);
    expect(isRuntimeDrivenAgent(undefined)).toBe(false);
  });

  it.each([
    [
      'channels configured',
      () => {
        const agent = testAgent();
        agent.setChannels({ __setAgent() {}, __setLogger() {} } as never);
        return agent;
      },
      'FlowsafeDurableAgent: the wrapped agent has channels configured: ',
    ],
    [
      "the 'durable' option",
      () =>
        new Agent({
          id: 'writer',
          name: 'writer',
          instructions: 'You are a test agent.',
          model: 'openai/gpt-4o-mini',
          durable: true,
        }),
      "FlowsafeDurableAgent: the wrapped agent sets the 'durable' option: ",
    ],
    [
      'a Mastra DurableAgent',
      () => new DurableAgent({ agent: testAgent() }) as unknown as Agent,
      'FlowsafeDurableAgent: the wrapped agent is already a durable agent: ',
    ],
    [
      'a FlowsafeDurableAgent',
      () =>
        createFlowsafeDurableAgent({
          agent: testAgent(),
          runtime: fakeRuntime().runtime,
        }) as unknown as Agent,
      'FlowsafeDurableAgent: the wrapped agent is already a durable agent: ',
    ],
    [
      'declared schedules',
      () => {
        const agent = testAgent();
        agent.__setDeclaredSchedules([{} as never]);
        return agent;
      },
      'FlowsafeDurableAgent: the wrapped agent declares schedules: ',
    ],
  ])('refuses to wrap an agent with %s before registering anything', (_label, wrapped, message) => {
    // #given a runtime with nothing registered
    const { runtime, register, registerAgent } = fakeRuntime();
    const viaFactory = () =>
      createFlowsafeDurableAgent({ agent: wrapped(), runtime });
    const direct = () =>
      new FlowsafeDurableAgent({ agent: wrapped(), runtime });

    // #when / #then the factory and direct construction both refuse with a
    // TypeError naming what the agent carries, and the runtime registers
    // nothing
    expect(viaFactory).toThrow(TypeError);
    expect(viaFactory).toThrow(message);
    expect(direct).toThrow(TypeError);
    expect(direct).toThrow(message);
    expect(registerAgent).not.toHaveBeenCalled();
    expect(register).not.toHaveBeenCalled();
  });

  it('refuses status and start once the loop is added to another Mastra', async () => {
    // #given a wrapper on a real runtime whose first operation built its Mastra
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none', startIdempotency: 'none' },
    );
    const durable = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    await expect(
      runtime.status(DURABLE_AGENTIC_LOOP_WORKFLOW_ID, 'first-read'),
    ).resolves.toBeNull();

    // #when the loop the wrapper hands out is added to another Mastra
    new Mastra({
      storage: new InMemoryStore(),
      logger: false,
      workflows: { loop: durable.getWorkflow() as unknown as AnyWorkflow },
    });

    // #then the runtime refuses the loop rather than read that Mastra's storage
    const refusal = `RunnerRuntime: workflow '${DURABLE_AGENTIC_LOOP_WORKFLOW_ID}' is not registered on this runtime's Mastra`;
    await expect(
      runtime.status(DURABLE_AGENTIC_LOOP_WORKFLOW_ID, 'first-read'),
    ).rejects.toThrow(refusal);
    await expect(
      runtime.start(DURABLE_AGENTIC_LOOP_WORKFLOW_ID, {
        runId: 'after-repoint',
        inputData: {},
      } as StartRunOptions),
    ).rejects.toThrow(refusal);
  });

  describe('guarded durable call options', () => {
    function guardedCore(
      method: 'stream' | 'generate' | 'prepare',
      guarded = guardedTestAgent(),
    ) {
      const durable = createFlowsafeDurableAgent({
        agent: guarded,
        runtime: fakeRuntime().runtime,
      });
      const core = vi
        .spyOn(DurableAgent.prototype, method)
        .mockResolvedValue({} as never);
      return { durable, core };
    }

    async function expectRefusedBeforeCore(
      pending: Promise<unknown>,
      core: ReturnType<typeof guardedCore>['core'],
      message: string,
    ) {
      await expect(pending).rejects.toBeInstanceOf(TypeError);
      await expect(pending).rejects.toThrow(message);
      expect(core).not.toHaveBeenCalled();
    }

    const refusedOptions = [
      ['structuredOutput', { schema: z.object({ answer: z.string() }) }],
      ['errorProcessors', []],
      ['clientTools', {}],
      ['toolsets', {}],
      ['outputProcessors', []],
      ['inputProcessors', []],
      ['instructions', 'override'],
      ['system', 'override'],
      ['context', []],
      ['prepareStep', () => ({})],
      ['hooks', {}],
      ['scorers', {}],
      ['savePerStep', true],
      ['versions', {}],
      ['autoResumeSuspendedTools', true],
      ['includeRawChunks', true],
      ['onChunk', () => undefined],
      ['delegation', {}],
      ['onIterationComplete', () => ({ continue: true })],
      ['isTaskComplete', {}],
      ['transform', {}],
      ['experimentalTransform', {}],
      ['requireToolApproval', true],
      ['backgroundTaskPolicy', {}],
      ['maxProcessorRetries', 3],
      ['eagerToolExecution', true],
    ] as const;

    it.each([
      { method: 'stream', coreMethod: 'stream' },
      { method: 'streamUntilPersisted', coreMethod: 'stream' },
      { method: 'generate', coreMethod: 'generate' },
      { method: 'prepare', coreMethod: 'prepare' },
    ] as const)('refuses an own __proto__ on $method before core dispatch', async ({
      method,
      coreMethod,
    }) => {
      const { durable, core } = guardedCore(coreMethod);
      core.mockRejectedValue(new Error('core dispatch reached'));
      const options = JSON.parse(
        '{"runId":"run-1","__proto__":{"outputProcessors":[],"instructions":"override"}}',
      );
      options.requestContext = actorContext();
      const pending =
        method === 'streamUntilPersisted'
          ? durable.streamUntilPersisted(
              'hello',
              options,
              'operator-1',
              'human',
              undefined,
              undefined,
              undefined,
              startAuthority(),
            )
          : durable[method]('hello', options);

      await expectRefusedBeforeCore(
        pending,
        core,
        '__proto__ is not supported',
      );
    });

    it.each([
      {
        shape: 'a refused eagerToolExecution key',
        override: { eagerToolExecution: false },
        omitBackgroundTasks: false,
      },
      {
        shape: 'background tasks enabled',
        override: { disableBackgroundTasks: false },
        omitBackgroundTasks: false,
      },
      {
        shape: 'an absent disableBackgroundTasks key',
        override: {},
        omitBackgroundTasks: true,
      },
    ])('refuses unusable guarded defaults with $shape at construction', async ({
      override,
      omitBackgroundTasks,
    }) => {
      const guarded = guardedTestAgent();
      const defaults: Awaited<ReturnType<Agent['getDefaultOptions']>> = {
        ...(await guarded.getDefaultOptions()),
        ...override,
      };
      if (omitBackgroundTasks) delete defaults.disableBackgroundTasks;
      vi.spyOn(guarded, 'getDefaultOptions').mockReturnValue(defaults);

      expect(() =>
        createFlowsafeDurableAgent({
          agent: guarded,
          runtime: fakeRuntime().runtime,
        }),
      ).toThrow(TypeError);
    });

    it.each(
      (['stream', 'generate', 'prepare'] as const).flatMap((method) =>
        refusedOptions.map(([key, value]) => ({ method, key, value })),
      ),
    )('refuses $key on $method before core dispatch', async ({
      method,
      key,
      value,
    }) => {
      const { durable, core } = guardedCore(method);
      const pending = durable[method]('hello', {
        runId: 'run-1',
        [key]: value,
      } as never);

      await expectRefusedBeforeCore(pending, core, `${key} is not supported`);
    });

    it.each([
      'stream',
      'generate',
      'prepare',
    ] as const)('refuses an own undefined clientTools on %s', async (method) => {
      const { durable, core } = guardedCore(method);
      const pending = durable[method]('hello', {
        runId: 'run-1',
        clientTools: undefined,
      } as never);

      await expectRefusedBeforeCore(
        pending,
        core,
        'clientTools is not supported',
      );
    });

    it('refuses non-enumerable hooks before core dispatch', async () => {
      const { durable, core } = guardedCore('stream');
      const options = { runId: 'run-1' };
      Object.defineProperty(options, 'hooks', { value: {}, enumerable: false });
      const pending = durable.stream('hello', options);

      await expectRefusedBeforeCore(pending, core, 'hooks is not supported');
    });

    it.each([
      'stream',
      'generate',
      'prepare',
    ] as const)('refuses memory configuration on %s', async (method) => {
      const { durable, core } = guardedCore(method);
      const pending = durable[method]('hello', {
        runId: 'run-1',
        memory: { thread: 't', resource: 'r', options: {} },
      } as never);

      await expectRefusedBeforeCore(pending, core, 'memory.options');
    });

    it.each([
      {
        memory: { thread: { id: 't', metadata: {} }, resource: 'r' },
        refusal: 'memory.thread.metadata',
      },
      {
        memory: { thread: { id: 't', title: 'x' }, resource: 'r' },
        refusal: 'memory.thread.title',
      },
      {
        memory: { thread: 't', resource: 'r', readOnly: true },
        refusal: 'memory.readOnly',
      },
      {
        memory: { thread: 't', resource: 'r', [Symbol('extra')]: true },
        refusal: 'memory.Symbol(extra)',
      },
      {
        memory: {
          thread: Object.assign(() => undefined, {
            id: 'thread-1',
            metadata: { workingMemory: 'x' },
          }),
          resource: 'r',
        },
        refusal: 'memory.thread.length',
      },
      { memory: 'x', refusal: 'memory must be an object' },
    ])('refuses $refusal before preparation', async ({ memory, refusal }) => {
      const { durable, core } = guardedCore('prepare');
      const pending = durable.prepare('hello', {
        runId: 'run-1',
        memory,
      } as never);

      await expectRefusedBeforeCore(pending, core, refusal);
    });

    it.each([
      { key: 'thread', path: 'memory.thread', onThread: false },
      { key: 'id', path: 'memory.thread.id', onThread: true },
    ] as const)('refuses accessor-backed memory $key without invoking the getter', async ({
      key,
      path,
      onThread,
    }) => {
      const { durable, core } = guardedCore('prepare');
      const getter = vi.fn(() => 't');
      const memory = { thread: { id: 't' }, resource: 'r' };
      Object.defineProperty(onThread ? memory.thread : memory, key, {
        get: getter,
      });
      const pending = durable.prepare('hello', { runId: 'run-1', memory });

      await expectRefusedBeforeCore(
        pending,
        core,
        `call option '${path}' must be a data property`,
      );
      expect(getter).not.toHaveBeenCalled();
    });

    it.each([
      ['maxSteps', 3],
      ['toolChoice', 'none'],
      ['disableBackgroundTasks', false],
      ['maxSteps', undefined],
      ['toolChoice', undefined],
      ['disableBackgroundTasks', undefined],
    ] as const)('refuses differing %s value %s before core dispatch', async (key, value) => {
      const { durable, core } = guardedCore('stream');
      const pending = durable.stream('hello', {
        runId: 'run-1',
        [key]: value,
      } as never);

      await expectRefusedBeforeCore(
        pending,
        core,
        `${key} must equal the guarded agent's own value`,
      );
    });

    it('forwards matching guarded defaults to core', async () => {
      const { durable, core } = guardedCore('stream');
      await durable.stream('hello', {
        runId: 'run-1',
        maxSteps: 2,
        toolChoice: 'auto',
        disableBackgroundTasks: true,
      });

      expect(core).toHaveBeenCalledWith('hello', {
        runId: 'run-1',
        maxSteps: 2,
        toolChoice: 'auto',
        disableBackgroundTasks: true,
      });
    });

    it.each([
      { type: 'tool', toolName: 'u' },
      'auto',
    ])('refuses a non-matching named-tool choice %s', async (choice) => {
      const guarded = createGuardedAgent({
        id: 'writer',
        name: 'Writer',
        instructions: 'Answer the request.',
        model: 'openai/gpt-4o-mini',
        allowedRoles: ['operator'],
        policies: [],
        audit: new AuditLogger(),
        maxSteps: 2,
        tools: {
          t: createTool({
            id: 't',
            description: 'Test tool',
            inputSchema: z.object({}),
            execute: async () => 'ok',
          }),
        },
        toolChoice: { type: 'tool', toolName: 't' },
      }) as unknown as Agent;
      const { durable, core } = guardedCore('stream', guarded);
      const pending = durable.stream('hello', {
        runId: 'run-1',
        toolChoice: choice,
      } as never);
      await expectRefusedBeforeCore(
        pending,
        core,
        "toolChoice must equal the guarded agent's own value",
      );
    });

    it('forwards a matching named-tool choice as a frozen copy', async () => {
      const guarded = createGuardedAgent({
        id: 'writer',
        name: 'Writer',
        instructions: 'Answer the request.',
        model: 'openai/gpt-4o-mini',
        allowedRoles: ['operator'],
        policies: [],
        audit: new AuditLogger(),
        maxSteps: 2,
        tools: {
          t: createTool({
            id: 't',
            description: 'Test tool',
            inputSchema: z.object({}),
            execute: async () => 'ok',
          }),
        },
        toolChoice: { type: 'tool', toolName: 't' },
      }) as unknown as Agent;
      const { durable, core } = guardedCore('stream', guarded);
      const choice = { type: 'tool' as const, toolName: 't' };
      await durable.stream('hello', { runId: 'run-1', toolChoice: choice });
      const forwarded = core.mock.calls[0]?.[1]?.toolChoice;
      expect(forwarded).toEqual({ type: 'tool', toolName: 't' });
      expect(forwarded).not.toBe(choice);
      expect(Object.isFrozen(forwarded)).toBe(true);
      choice.toolName = 'other';
      expect(forwarded).toEqual({ type: 'tool', toolName: 't' });
    });

    it.each([
      'stream',
      'generate',
      'prepare',
    ] as const)('freezes the memory binding before delegating %s to core', async (method) => {
      const { durable, core } = guardedCore(method);
      const memory = { thread: { id: 't' }, resource: 'r' };
      await durable[method]('hello', { runId: 'run-1', memory });
      const forwarded = core.mock.calls[0]?.[1]?.memory;

      expect(forwarded).toEqual({ thread: { id: 't' }, resource: 'r' });
      expect(forwarded).not.toBe(memory);
      expect(Object.isFrozen(forwarded)).toBe(true);
      expect(Object.isFrozen(forwarded?.thread)).toBe(true);
      memory.thread.id = 'other';
      expect(forwarded?.thread).toEqual({ id: 't' });
    });

    it('forwards client tools and execution limits for a raw agent', async () => {
      const durable = createFlowsafeDurableAgent({
        agent: testAgent(),
        runtime: fakeRuntime().runtime,
      });
      const core = vi
        .spyOn(DurableAgent.prototype, 'stream')
        .mockResolvedValue({} as never);
      await durable.stream('hello', {
        runId: 'run-1',
        clientTools: {},
        maxSteps: 7,
      });

      expect(core).toHaveBeenCalledWith('hello', {
        runId: 'run-1',
        clientTools: {},
        maxSteps: 7,
      });
    });
  });

  it('snapshots durable call options before delegating to core', async () => {
    const { runtime } = fakeRuntime();
    const durable = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime,
    });
    const superStream = vi
      .spyOn(DurableAgent.prototype, 'stream')
      .mockResolvedValue({ output: {} } as never);
    const superGenerate = vi
      .spyOn(DurableAgent.prototype, 'generate')
      .mockResolvedValue({} as never);
    const superPrepare = vi
      .spyOn(DurableAgent.prototype, 'prepare')
      .mockResolvedValue({} as never);

    for (const [method, coreMethod] of [
      ['stream', superStream],
      ['generate', superGenerate],
      ['prepare', superPrepare],
    ] as const) {
      const options: Record<string, unknown> = { runId: 'run-1' };
      const pending = (
        durable[method] as unknown as (
          messages: string,
          options: Record<string, unknown>,
        ) => Promise<unknown>
      )('hello', options);
      options.runId = 'mutated-run';
      options.structuredOutput = { schema: z.object({ answer: z.string() }) };

      await expect(pending).resolves.toBeDefined();
      const forwarded = coreMethod.mock.calls.at(-1)?.[1] as
        | Record<string, unknown>
        | undefined;
      expect(forwarded).toEqual({ runId: 'run-1' });
      expect(forwarded).not.toBe(options);
      expect(Object.isFrozen(forwarded)).toBe(true);
    }
  });

  it.each([
    'stream',
    'generate',
    'prepare',
  ] as const)('rejects accessor-backed %s options without invoking the accessor', async (method) => {
    const { runtime } = fakeRuntime();
    const durable = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime,
    });
    const structuredOutput = vi.fn(() => undefined);
    const options = { runId: 'run-1' } as Record<string, unknown>;
    Object.defineProperty(options, 'structuredOutput', {
      enumerable: true,
      get: structuredOutput,
    });

    await expect(
      (
        durable[method] as unknown as (
          messages: string,
          options: Record<string, unknown>,
        ) => Promise<unknown>
      )('hello', options),
    ).rejects.toThrow(/structuredOutput.*data property/);
    expect(structuredOutput).not.toHaveBeenCalled();
  });

  it('rejects an accessor-backed runId without reading it or delegating', async () => {
    const { runtime } = fakeRuntime();
    const durable = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime,
    });
    const superGenerate = vi.spyOn(DurableAgent.prototype, 'generate');
    const runId = vi.fn(() => 'run-1');
    const options: Record<string, unknown> = {};
    Object.defineProperty(options, 'runId', {
      enumerable: true,
      get: runId,
    });

    await expect(durable.generate('hello', options as never)).rejects.toThrow(
      /runId.*data property/,
    );
    expect(runId).not.toHaveBeenCalled();
    expect(superGenerate).not.toHaveBeenCalled();
  });
});

describe('FlowsafeDurableAgent.executeWorkflow', () => {
  async function guardedUnownedInputFixture() {
    const { runtime, start } = fakeRuntime();
    const memory = new MockMemory();
    await memory.saveThread({
      thread: {
        id: 'thread-1',
        resourceId: 'resource-1',
        createdAt: new Date(),
        updatedAt: new Date(),
        metadata: {},
      },
    });
    const audit = new AuditLogger();
    const agent = createFlowsafeDurableAgent({
      agent: createGuardedAgent({
        id: 'writer',
        name: 'Writer',
        instructions: 'Answer the request.',
        model: 'openai/gpt-4o-mini',
        memory,
        allowedRoles: ['operator'],
        policies: [denyPatterns([UNOWNED_INPUT_MARK])],
        audit,
        maxSteps: 2,
        toolChoice: 'auto',
      }) as unknown as Agent,
      runtime,
      cache: false,
    });
    const emitError = vi.spyOn(
      agent as unknown as {
        emitError: (id: string, error: Error) => Promise<void>;
      },
      'emitError',
    );
    return { agent, memory, audit, start, emitError };
  }

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each([
    ['is retained on an unthreaded start', false, false],
    ['is evicted on an unthreaded start', true, false],
    ['is retained on a threaded start', false, true],
    ['is evicted on a threaded start', true, true],
  ] as const)(
    'refuses a host start that RBAC refused at preparation with 403 before the runtime starts when its core run entry %s',
    async (_label, evicted, threaded) => {
      const memory = new MockMemory();
      if (threaded) {
        await memory.saveThread({
          thread: {
            id: 'thread-1',
            resourceId: 'thread-1',
            createdAt: new Date(),
            updatedAt: new Date(),
            metadata: {},
          },
        });
      }
      const f = await realAgentBridgeFixture(
        () => ({ [ACTOR_CONTEXT_KEY]: { id: 'actor-1', role: 'viewer' } }),
        undefined,
        threaded,
        { memory, prompts: [] },
      );
      const runId = crypto.randomUUID();
      const fillers: string[] = [];
      let refusalReason = '';
      const execute = spyOnRefusedExecute(
        f.agent,
        evicted,
        fillers,
        (tripwire) => {
          assert(tripwire);
          refusalReason = tripwire.reason;
        },
      );
      let result:
        | Awaited<ReturnType<typeof f.agent.streamUntilPersisted>>
        | undefined;
      try {
        const error = await f.agent
          .streamUntilPersisted(
            'hello',
            {
              runId,
              requestContext: actorContext('viewer'),
              disableBackgroundTasks: true,
              ...(threaded
                ? { memory: { thread: 'thread-1', resource: 'thread-1' } }
                : {}),
            },
            'operator-1',
            'human',
            `${runId}-attempt`,
            undefined,
            undefined,
            { ...startAuthority(), agentStart: { threaded } },
          )
          .then(
            (value) => {
              result = value;
              return value;
            },
            (refusal: unknown) => refusal,
          );
        expect(execute).toHaveBeenCalledOnce();
        expect(f.counts.model).toBe(0);
        expect(error).toBeInstanceOf(DoStatusError);
        expect(error).toMatchObject({ status: 403 });
        expect((error as Error).message).not.toContain(refusalReason);
        expect(doErrorResponse(error).status).toBe(403);
        expect(f.start).not.toHaveBeenCalled();
        if (threaded) {
          await vi.waitFor(() =>
            expect(
              f.agent.getActiveThreadRunId({
                threadId: 'thread-1',
                resourceId: 'thread-1',
              }),
            ).toBeUndefined(),
          );
        }
      } finally {
        result?.cleanup();
        registryFor(f.agent).clear();
        globalRunRegistry.delete(runId);
        for (const id of fillers) globalRunRegistry.delete(id);
        execute.mockRestore();
        f.start.mockRestore();
        f.sql.close();
      }
    },
    15_000,
  );

  it('terminally fails and persists input when the host seam did not register the run', async () => {
    // #given a real prepared input whose thread already exists
    const { runtime, start } = fakeRuntime();
    const memory = new MockMemory();
    await memory.saveThread({
      thread: {
        id: 'thread-1',
        resourceId: 'resource-1',
        createdAt: new Date(),
        updatedAt: new Date(),
        metadata: {},
      },
    });
    const rawAgent = new Agent({
      id: 'writer',
      name: 'writer',
      instructions: 'You are a test agent.',
      model: 'openai/gpt-4o-mini',
      memory,
    });
    const agent = createFlowsafeDurableAgent({ agent: rawAgent, runtime });
    const prepared = await agent.prepare('denied input', {
      runId: 'run-1',
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    const saveMessages = vi.spyOn(memory, 'saveMessages');
    const emitError = vi
      .spyOn(
        agent as unknown as {
          emitError: (id: string, error: Error) => Promise<void>;
        },
        'emitError',
      )
      .mockResolvedValue(undefined);

    // #when core drives the prepared run without the host start seam
    await drive(agent, 'run-1', prepared.workflowInput);

    // #then it closes terminally without creating a runtime run
    expect(start).not.toHaveBeenCalled();
    expect(emitError).toHaveBeenCalledWith(
      'run-1',
      expect.any(InvalidRunRequestError),
    );
    expect(saveMessages).toHaveBeenCalledOnce();
    expect(saveMessages.mock.calls[0]?.[0].messages).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ role: 'user', threadId: 'thread-1' }),
      ]),
    );
    expect(registryFor(agent).has('run-1')).toBe(false);
    expect(globalRunRegistry.has('run-1')).toBe(false);
  });

  it.each([
    ['a missing thread', { threadExists: false }],
    [
      'read-only memory',
      { threadExists: true, memoryConfig: { readOnly: true } },
    ],
  ])('does not preserve unowned input under %s', async (_label, state) => {
    const { runtime } = fakeRuntime();
    const memory = new MockMemory();
    const agent = createFlowsafeDurableAgent({
      agent: new Agent({
        id: 'writer',
        name: 'writer',
        instructions: 'You are a test agent.',
        model: 'openai/gpt-4o-mini',
        memory,
      }),
      runtime,
    });
    const prepared = await agent.prepare('denied input', { runId: 'run-1' });
    const saveMessages = vi.spyOn(memory, 'saveMessages');
    vi.spyOn(
      agent as unknown as {
        emitError: (id: string, error: Error) => Promise<void>;
      },
      'emitError',
    ).mockResolvedValue(undefined);

    await drive(agent, 'run-1', {
      ...prepared.workflowInput,
      state: {
        ...prepared.workflowInput.state,
        threadId: 'thread-1',
        resourceId: 'resource-1',
        ...state,
      },
    });

    expect(saveMessages).not.toHaveBeenCalled();
  });

  it.each([
    [
      'the RBAC gate refuses it for a missing actor',
      `${UNOWNED_INPUT_MARK} text`,
      undefined,
      'agent.input.authorize',
    ],
    [
      'an input policy refuses it',
      `${UNOWNED_INPUT_MARK} text`,
      actorContext,
      'agent.input.policy',
    ],
    [
      'the RBAC gate refuses a created signal streamed with a request context',
      createSignal({ type: 'user', contents: `${UNOWNED_INPUT_MARK} text` }),
      () => new RequestContext(),
      'agent.input.authorize',
    ],
  ] as const)('saves none of a direct unowned stream input when %s', async (_label, input, requestContext, refusingAction) => {
    // #given — a guarded agent whose input policy denies the marker, over a
    // thread that exists
    const { agent, memory, audit, start, emitError } =
      await guardedUnownedInputFixture();
    const saveMessages = vi.spyOn(memory, 'saveMessages');

    // #when — a caller streams its own input past the host start seam
    await agent.stream(input, {
      runId: 'run-1',
      memory: { thread: 'thread-1', resource: 'resource-1' },
      ...(requestContext ? { requestContext: requestContext() } : {}),
    });
    await vi.waitFor(() =>
      expect(emitError).toHaveBeenCalledWith(
        'run-1',
        expect.any(InvalidRunRequestError),
      ),
    );

    // #then — the named gate refused the call, and memory holds none of it
    expect(
      audit
        .events()
        .some(
          (event) =>
            event.action === refusingAction && event.decision === 'denied',
        ),
    ).toBe(true);
    expect(start).not.toHaveBeenCalled();
    expect(saveMessages).not.toHaveBeenCalled();
    const { messages } = await memory.recall({ threadId: 'thread-1' });
    expect(JSON.stringify(messages)).not.toContain(UNOWNED_INPUT_MARK);
  });

  it.each([
    ['is retained', false],
    ['is evicted', true],
  ] as const)('preserves a created signal refused for a missing actor when its core run entry %s', async (_label, evicted) => {
    const { agent, memory, audit, start, emitError } =
      await guardedUnownedInputFixture();
    const fillers: string[] = [];
    const execute = spyOnRefusedExecute(agent, evicted, fillers);
    try {
      await agent.stream(
        createSignal({ type: 'user', contents: `${UNOWNED_INPUT_MARK} text` }),
        {
          runId: 'run-1',
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      );
      await vi.waitFor(() =>
        expect(emitError).toHaveBeenCalledWith(
          'run-1',
          expect.any(InvalidRunRequestError),
        ),
      );
      expect(audit.events()).toContainEqual(
        expect.objectContaining({
          action: 'agent.input.authorize',
          decision: 'denied',
        }),
      );
      expect(start).not.toHaveBeenCalled();
      const { messages } = await memory.recall({ threadId: 'thread-1' });
      expect(JSON.stringify(messages)).toContain(`${UNOWNED_INPUT_MARK} text`);
    } finally {
      for (const id of fillers) globalRunRegistry.delete(id);
      execute.mockRestore();
    }
  });

  it('publishes the terminal error when unowned input persistence times out', async () => {
    vi.useFakeTimers();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    try {
      const { runtime } = fakeRuntime();
      const memory = new MockMemory();
      await memory.saveThread({
        thread: {
          id: 'thread-1',
          resourceId: 'resource-1',
          createdAt: new Date(),
          updatedAt: new Date(),
          metadata: {},
        },
      });
      const agent = createFlowsafeDurableAgent({
        agent: new Agent({
          id: 'writer',
          name: 'writer',
          instructions: 'You are a test agent.',
          model: 'openai/gpt-4o-mini',
          memory,
        }),
        runtime,
      });
      const prepared = await agent.prepare('denied input', {
        runId: 'run-1',
        memory: { thread: 'thread-1', resource: 'resource-1' },
      });
      vi.spyOn(memory, 'saveMessages').mockImplementation(
        () => new Promise(() => undefined),
      );
      const emitError = vi
        .spyOn(
          agent as unknown as {
            emitError: (id: string, error: Error) => Promise<void>;
          },
          'emitError',
        )
        .mockResolvedValue(undefined);
      const execution = drive(agent, 'run-1', prepared.workflowInput);

      await vi.advanceTimersByTimeAsync(5_000);
      await execution;

      expect(emitError).toHaveBeenCalledWith(
        'run-1',
        expect.any(InvalidRunRequestError),
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it('retries terminal publication once before handing failure back to core', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const prepared = await agent.prepare('denied input', { runId: 'run-1' });
    const emitError = vi
      .spyOn(
        agent as unknown as {
          emitError: (id: string, error: Error) => Promise<void>;
        },
        'emitError',
      )
      .mockRejectedValue(new Error('publication failed'));

    await expect(
      drive(agent, 'run-1', prepared.workflowInput),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(emitError).toHaveBeenCalledTimes(2);
    expect(registryFor(agent).has('run-1')).toBe(false);
    expect(globalRunRegistry.has('run-1')).toBe(false);
  });

  it('rejects an absent runId without a crypto.randomUUID fallback', async () => {
    // #given
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #when / #then a missing runId is a client error, never a generated one
    await expect(drive(agent, undefined, INPUT)).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    expect(start).not.toHaveBeenCalled();
  });

  it('rejects a non-path-safe runId', async () => {
    // #given
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #when / #then a ':' in the id would collide the DO-name join
    await expect(drive(agent, 'acme:run1', INPUT)).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    // and an empty id
    await expect(drive(agent, '', INPUT)).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    expect(start).not.toHaveBeenCalled();
  });
});

describe('FlowsafeDurableAgent.isRunLive', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each([
    { source: 'global', global: true },
    { source: 'internal', global: false },
  ] as const)('shares the refusal predicate for an isolated $source registry entry', async ({
    source,
    global,
  }) => {
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const seedRunId = `live-seed-${source}`;
    const runId = `live-${source}`;
    try {
      const prepared = await agent.prepare('hello', { runId: seedRunId });
      if (global) globalRunRegistry.set(runId, prepared.registryEntry);
      else registryFor(agent).register(runId, prepared.registryEntry);
      const stream = vi.spyOn(DurableAgent.prototype, 'stream');
      expect(globalRunRegistry.has(runId)).toBe(global);
      expect(registryFor(agent).has(runId)).toBe(!global);
      expect(agent.isRunLive(runId)).toBe(true);
      expect(agent.isRunLive('live-absent')).toBe(false);
      await expect(agent.stream('duplicate', { runId })).rejects.toThrow(
        'run id is live in the run registry — a registered run cannot be re-entered',
      );
      expect(stream).not.toHaveBeenCalled();
      expect(start).not.toHaveBeenCalled();
    } finally {
      for (const fixtureRunId of [runId, seedRunId]) {
        registryFor(agent).cleanup(fixtureRunId);
        globalRunRegistry.delete(fixtureRunId);
      }
    }
    expect(agent.isRunLive(runId)).toBe(false);
  });

  it('shares the refusal predicate while a host stream awaits Core registration', async () => {
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const runId = 'live-starting';
    const entered = bridgeDeferred();
    const release = bridgeDeferred();
    const failure = new Error('fixture stream refusal');
    const stream = vi
      .spyOn(DurableAgent.prototype, 'stream')
      .mockImplementation(async () => {
        entered.resolve();
        await release.promise;
        throw failure;
      });
    const pending = agent
      .streamUntilPersisted(
        'first',
        { runId },
        'operator-1',
        'human',
        undefined,
        undefined,
        undefined,
        startAuthority(),
      )
      .catch((error: unknown) => error);
    try {
      await entered.promise;
      expect(globalRunRegistry.has(runId)).toBe(false);
      expect(registryFor(agent).has(runId)).toBe(false);
      expect(agent.isRunLive(runId)).toBe(true);
      expect(agent.isRunLive('live-absent')).toBe(false);
      await expect(agent.stream('duplicate', { runId })).rejects.toThrow(
        'run id is live in the run registry — a registered run cannot be re-entered',
      );
      expect(stream).toHaveBeenCalledOnce();
      expect(start).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      await pending;
    }
    expect(await pending).toBe(failure);
    expect(agent.isRunLive(runId)).toBe(false);
  });
});

describe('FlowsafeDurableAgent.streamUntilPersisted', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('refuses untilIdle before registering a host start', async () => {
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const stream = vi.spyOn(agent, 'stream');

    await expect(
      agent.streamUntilPersisted(
        'hello',
        { runId: 'run-1', untilIdle: true },
        'operator-1',
        'human',
        undefined,
        undefined,
        undefined,
        startAuthority(),
      ),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);

    expect(stream).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });

  it('snapshots call options before installing persistence state', async () => {
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime,
    });
    const failure = new Error('stream unavailable');
    const stream = vi.spyOn(agent, 'stream').mockRejectedValue(failure);
    const options: Record<string, unknown> = { runId: 'run-1' };

    const pending = agent.streamUntilPersisted(
      'hello',
      options as never,
      'operator-1',
      'human',
      undefined,
      undefined,
      undefined,
      startAuthority(),
    );
    options.runId = 'mutated-run';
    options.structuredOutput = { schema: z.object({ answer: z.string() }) };

    await expect(pending).rejects.toBe(failure);
    expect(stream).toHaveBeenCalledOnce();
    expect(stream.mock.calls[0]?.[1]).toMatchObject({ runId: 'run-1' });
    expect(stream.mock.calls[0]?.[1]).not.toHaveProperty('structuredOutput');
  });

  it.each([
    { clientTools: {} },
    { maxSteps: 3 },
    { memory: { thread: 't', resource: 'r', options: {} } },
  ])('guarded durable call options refuse %j before installing persistence state', async (options) => {
    const agent = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime: fakeRuntime().runtime,
    });
    const stream = vi
      .spyOn(agent, 'stream')
      .mockRejectedValue(new Error('unexpected dispatch'));

    await expect(
      agent.streamUntilPersisted(
        'hello',
        { runId: 'run-1', ...options } as never,
        'operator-1',
        'human',
        undefined,
        undefined,
        undefined,
        startAuthority(),
      ),
    ).rejects.toBeInstanceOf(TypeError);
    expect(agent.isRunLive('run-1')).toBe(false);
    expect(stream).not.toHaveBeenCalled();
  });

  it('rejects accessor-backed call options before installing persistence state', async () => {
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime,
    });
    const stream = vi.spyOn(agent, 'stream');
    const structuredOutput = vi.fn(() => undefined);
    const options = { runId: 'run-1' } as Record<string, unknown>;
    Object.defineProperty(options, 'structuredOutput', {
      enumerable: true,
      get: structuredOutput,
    });

    await expect(
      agent.streamUntilPersisted(
        'hello',
        options as never,
        'operator-1',
        'human',
        undefined,
        undefined,
        undefined,
        startAuthority(),
      ),
    ).rejects.toThrow(/structuredOutput.*data property/);
    expect(structuredOutput).not.toHaveBeenCalled();
    expect(stream).not.toHaveBeenCalled();
  });

  it('does not resolve until the runtime has persisted the first summary', async () => {
    const { runtime, start } = fakeRuntime();
    let releaseStart!: () => void;
    start.mockImplementation(
      (_workflowId, options: { runId: string }) =>
        new Promise((resolve) => {
          releaseStart = () =>
            resolve({
              runId: options.runId,
              status: 'suspended' as const,
              suspended: [['gate']],
            });
        }),
    );
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const streamResult = { output: { id: 'output' } };
    vi.spyOn(agent, 'stream').mockResolvedValue(streamResult as never);
    let settled = false;

    const pending = agent
      .streamUntilPersisted(
        'hello',
        { runId: 'run-1' },
        'operator-1',
        'human',
        undefined,
        undefined,
        undefined,
        startAuthority(),
      )
      .finally(() => {
        settled = true;
      });
    await vi.waitFor(() => expect(agent.stream).toHaveBeenCalledOnce());
    const execution = drive(agent, 'run-1', INPUT);
    await vi.waitFor(() => expect(start).toHaveBeenCalledOnce());
    expect(start).toHaveBeenCalledWith(
      DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
      expect.objectContaining({
        runId: 'run-1',
        requestedBy: 'operator-1',
        requestedByKind: 'human',
        attemptToken: expect.any(String),
      }),
    );
    expect(settled).toBe(false);

    releaseStart();

    await expect(pending).resolves.toBe(streamResult);
    await execution;
  });

  it('rejects direct stream and generate collisions while a host start is registered', async () => {
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const streamResult = { output: { id: 'output' } };
    const superStream = vi
      .spyOn(DurableAgent.prototype, 'stream')
      .mockResolvedValue(streamResult as never);
    const superGenerate = vi.spyOn(DurableAgent.prototype, 'generate');
    let settled = false;
    const pending = agent
      .streamUntilPersisted(
        'first',
        { runId: 'run-1' },
        'operator-1',
        'human',
        undefined,
        undefined,
        undefined,
        startAuthority(),
      )
      .finally(() => {
        settled = true;
      });
    await vi.waitFor(() => expect(superStream).toHaveBeenCalledOnce());

    await expect(agent.stream('second', { runId: 'run-1' })).rejects.toThrow(
      'run id is live in the run registry — a registered run cannot be re-entered',
    );
    await expect(agent.generate('second', { runId: 'run-1' })).rejects.toThrow(
      'run id is live in the run registry — a registered run cannot be re-entered',
    );
    expect(superStream).toHaveBeenCalledOnce();
    expect(superGenerate).not.toHaveBeenCalled();
    expect(settled).toBe(false);

    await drive(agent, 'run-1', INPUT);
    await expect(pending).resolves.toBe(streamResult);
    expect(start).toHaveBeenCalledOnce();
  });

  it('rejects when durable persistence fails', async () => {
    const { runtime, start } = fakeRuntime();
    start.mockRejectedValue(new Error('D1 unavailable'));
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    vi.spyOn(agent, 'stream').mockResolvedValue({
      output: { id: 'output' },
    } as never);

    const pending = agent.streamUntilPersisted(
      'hello',
      {
        runId: 'run-1',
      },
      'operator-1',
      'human',
      undefined,
      undefined,
      undefined,
      startAuthority(),
    );
    const execution = drive(agent, 'run-1', INPUT);

    await expect(pending).rejects.toThrow('D1 unavailable');
    await expect(execution).rejects.toThrow('D1 unavailable');
  });

  it.each([
    ['an invalid requester', 'reviewer\u000aforged', 'human'],
    ['an invalid requester kind', 'reviewer-1', 'operator'],
  ])('rejects %s before starting the durable stream', async (_label, requestedBy, requestedByKind) => {
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const stream = vi.spyOn(agent, 'stream');

    await expect(
      agent.streamUntilPersisted(
        'hello',
        { runId: 'run-1' },
        requestedBy,
        requestedByKind as never,
        undefined,
        undefined,
        undefined,
        startAuthority(),
      ),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);

    expect(stream).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });
});

// The public entry points a host actually calls. The inherited stream()/generate()
// take an OPTIONAL runId; without the override, core mints an unowned
// crypto.randomUUID() upstream of executeWorkflow's guard.
// These pin that the boundary refuses an absent/non-path-safe runId before any
// run is registered.
describe('FlowsafeDurableAgent runId boundary (stream/generate)', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('does not re-badge an unrelated error that only copied the refusal name', async () => {
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const original = new Error('unrelated core failure');
    original.name = 'InvalidRunRequestError';
    vi.spyOn(DurableAgent.prototype, 'generate').mockRejectedValue(original);

    await expect(agent.generate('Hello!', { runId: 'run-1' })).rejects.toBe(
      original,
    );
  });

  it('restores the refusal prototype and preserves the rebuilt error as cause', async () => {
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const rebuilt = new Error(
      "Flowsafe durable-agent runner refused unregistered run: run 'run-1' was not registered by the host start seam",
    );
    rebuilt.name = 'InvalidRunRequestError';
    vi.spyOn(DurableAgent.prototype, 'generate').mockRejectedValue(rebuilt);

    const rejection = await agent
      .generate('Hello!', { runId: 'run-1' })
      .catch((error: unknown) => error);

    expect(rejection).toBeInstanceOf(InvalidRunRequestError);
    expect((rejection as Error & { cause?: unknown }).cause).toBe(rebuilt);
  });

  it('stream() without a runId rejects (no crypto.randomUUID upstream)', async () => {
    // #given
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #when / #then the idiomatic bare stream('...') Mastra documents is refused
    await expect(agent.stream('Hello!')).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    // never reached the runtime — no unowned run was started
    expect(start).not.toHaveBeenCalled();
  });

  it('stream() with a non-path-safe runId rejects', async () => {
    // #given
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #when / #then
    await expect(
      agent.stream('Hello!', { runId: 'acme:run1' }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(start).not.toHaveBeenCalled();
  });

  it('stream() with a numeric runId rejects without RegExp coercion', async () => {
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });

    await expect(
      agent.stream('Hello!', { runId: 123 as unknown as string }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(start).not.toHaveBeenCalled();
  });

  it('generate() without a runId rejects too', async () => {
    // #given generate() re-implements the durable setup with the same fallback
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #when / #then
    await expect(agent.generate('Hello!')).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    expect(start).not.toHaveBeenCalled();
  });
});

// prepare() is an inherited minting entry point: it forwards options?.runId
// into core's prepareForDurableExecution,
// which mints an unowned crypto.randomUUID() AND registers a run under it when
// runId is absent (@mastra/core 1.50.0 agent/durable/index.js:5980 -> :589 ->
// :5984). PATH_SAFE_ID_PATTERN accepts a bare UUID, so no downstream guard
// (executeWorkflow's re-guard, RunnerRuntime.start) can catch it — the override
// must refuse an absent/non-path-safe runId BEFORE super.prepare mints or registers
// anything. super.prepare is spied so the accept path proves delegation without
// driving core's real preparation (which resolves model/tools and touches the
// registry) — and so the reject paths prove it is never reached.
describe('FlowsafeDurableAgent runId boundary (prepare)', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('prepare() without a runId rejects before super.prepare mints/registers a run', async () => {
    // #given
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const superPrepare = vi.spyOn(DurableAgent.prototype, 'prepare');
    // #when / #then — the idiomatic bare prepare('...') would mint an unowned
    // UUID upstream; refuse it
    await expect(agent.prepare('Hello!')).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    // never delegated — no crypto.randomUUID mint, no registry write
    expect(superPrepare).not.toHaveBeenCalled();
  });

  it('prepare() with an empty runId rejects', async () => {
    // #given
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const superPrepare = vi.spyOn(DurableAgent.prototype, 'prepare');
    // #when / #then
    await expect(agent.prepare('Hello!', { runId: '' })).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    expect(superPrepare).not.toHaveBeenCalled();
  });

  it('prepare() with a non-path-safe (":"-bearing) runId rejects', async () => {
    // #given
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const superPrepare = vi.spyOn(DurableAgent.prototype, 'prepare');
    // #when / #then — ':' would collide the DO-name join
    await expect(
      agent.prepare('Hello!', { runId: 'acme:run1' }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(superPrepare).not.toHaveBeenCalled();
  });

  it('prepare() with a valid host-owned runId delegates to super.prepare', async () => {
    // #given a spied super.prepare so delegation is observable without core's
    // real preparation (model/tool resolution + registry writes)
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const prepared = { runId: 'run-1' } as unknown as Awaited<
      ReturnType<DurableAgent['prepare']>
    >;
    const superPrepare = vi
      .spyOn(DurableAgent.prototype, 'prepare')
      .mockResolvedValue(prepared);
    // #when — a caller-minted runId
    const result = await agent.prepare('Hello!', { runId: 'run-1' });
    // #then — the guard passed and the call reached super unchanged
    expect(superPrepare).toHaveBeenCalledTimes(1);
    expect(superPrepare).toHaveBeenCalledWith('Hello!', { runId: 'run-1' });
    expect(result).toBe(prepared);
  });
});

describe('FlowsafeDurableAgent pubsub identity', () => {
  it("defaults the agent's stream pubsub to the runtime's identity", () => {
    // #given a runtime carrying a pubsub identity, and no explicit pubsub option
    const pubsub = new EventEmitterPubSub();
    const { runtime } = fakeRuntime({ pubsub });
    // cache:false so the pubsub getter returns the inner instance directly
    // (a CachingPubSub wrapper would otherwise hide the identity).
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      cache: false,
    });
    // #then the agent publishes on the SAME feed the run's events use, so
    // observe()/emitError align (no dead feed) without the host wiring it twice
    expect(agent.pubsub).toBe(pubsub);
  });

  it('refuses a wrapped agent with a different pubsub of its own', () => {
    const ownPubsub = new EventEmitterPubSub();
    const { runtime } = fakeRuntime({ pubsub: new EventEmitterPubSub() });
    const rawAgent = testAgent('writer', ownPubsub);

    expect(() =>
      createFlowsafeDurableAgent({ agent: rawAgent, runtime }),
    ).toThrow(TypeError);
  });

  it('accepts a wrapped agent sharing the runtime pubsub', () => {
    const pubsub = new EventEmitterPubSub();
    const { runtime } = fakeRuntime({ pubsub });
    const rawAgent = testAgent('writer', pubsub);

    const agent = createFlowsafeDurableAgent({ agent: rawAgent, runtime });
    expect(agent.getPubSub()).toBe(pubsub);
  });

  it('accepts a registered agent using its Mastra pubsub fallback', () => {
    const pubsub = new EventEmitterPubSub();
    const mastra = new Mastra({ agents: { writer: testAgent() }, pubsub });
    const rawAgent = mastra.getAgentById('writer');
    const agent = createFlowsafeDurableAgent({
      agent: rawAgent,
      runtime: fakeRuntime({ pubsub }).runtime,
    });

    expect(agent.getPubSub()).toBe(pubsub);
  });
});

describe('FlowsafeDurableAgent thread runtime registration and rehydration', () => {
  afterEach(() => {
    globalRunRegistry.clear();
    vi.restoreAllMocks();
  });

  it('prepares guarded durable registries without error processors', async () => {
    const agent = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime: fakeRuntime().runtime,
      cache: false,
    });
    const runId = 'guarded-no-error-processors';

    await agent.prepare('hello', {
      runId,
      requestContext: actorContext(),
    });

    expect(registryFor(agent).get(runId)?.errorProcessors).toEqual([]);
    expect(globalRunRegistry.get(runId)?.errorProcessors).toEqual([]);
  });

  it('registers a resumed run on the agent pubsub with the default cache', async () => {
    const pubsub = new EventEmitterPubSub();
    const { runtime, resume } = fakeRuntime({
      pubsub,
      resumeContext: actorContext(),
    });
    const registerRun = vi.fn(async (..._args: unknown[]) => undefined);
    const agent = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime,
      threadRuntime: { registerRun } as never,
    });
    await agent.prepare('initial request', {
      runId: 'run-1',
      requestContext: actorContext(),
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    registryFor(agent).clear();
    globalRunRegistry.clear();
    const output = { id: 'rehydrated' };
    const memory = { thread: 'thread-1', resource: 'resource-1' };
    vi.spyOn(agent, 'observe').mockResolvedValue({ output } as never);

    await agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
      memory,
    });

    expect(resume).toHaveBeenCalledOnce();
    expect(registerRun).toHaveBeenCalledOnce();
    expect(registerRun).toHaveBeenCalledWith(
      agent,
      expect.objectContaining(output),
      { runId: 'run-1', memory },
      agent.getPubSub(),
    );
    expect(agent.getPubSub()).toBe(pubsub);
  });

  it('resumes observation after retained earlier events on the observer bus', async () => {
    const pubsub = new EventEmitterPubSub();
    const { runtime, resume } = fakeRuntime({
      pubsub,
      resumeContext: actorContext(),
    });
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
    });
    const runId = 'resume-observer-history';
    const topic = AGENT_STREAM_TOPIC(runId);
    await agent.pubsub.publish(topic, {
      type: AgentStreamEventTypes.CHUNK,
      runId,
      data: {
        type: 'tool-call-approval',
        runId,
        from: 'AGENT',
        payload: {
          toolCallId: 'earlier-approval',
          toolName: 'approve-action',
          args: {},
        },
      },
    });
    expect(agent.pubsub).not.toBe(agent.getPubSub());
    const innerPubsub = agent.getPubSub();
    assert(innerPubsub);
    expect((await innerPubsub.getHistory(topic)).length).not.toBe(1);
    const observe = agent.observe.bind(agent);
    const observer = vi
      .spyOn(agent, 'observe')
      .mockImplementation((...args) => observe(...args));

    await agent.resumeViaRuntime({ runId, requestedBy: 'reviewer-1' });

    expect(resume).toHaveBeenCalledOnce();
    expect(observer).toHaveBeenCalledWith(runId, { offset: 1 });
  });

  it('keeps private-field getters readable and reports the runtime outcome on a registered resumed output', async () => {
    // Core's stream output keeps `status` and other state in private fields read through the registered output.
    // The registered status follows the runtime's outcome once the resume returns.
    class PrivateStatusOutput {
      readonly #status = 'running';
      readonly #runId = 'run-1';
      get status(): string {
        return this.#status;
      }
      get runId(): string {
        return this.#runId;
      }
    }
    const { runtime } = fakeRuntime({ resumeContext: actorContext() });
    let registeredOutput: { status: string; runId: string } | undefined;
    let statusDuringResume: string | undefined;
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      threadRuntime: {
        registerRun: vi.fn(async (_agent, output) => {
          registeredOutput = output as typeof registeredOutput;
          statusDuringResume = (output as { status: string }).status;
        }),
      } as never,
    });
    vi.spyOn(agent, 'observe').mockResolvedValue({
      output: new PrivateStatusOutput(),
    } as never);

    await agent.resumeViaRuntime({ runId: 'run-1', requestedBy: 'reviewer-1' });

    expect(statusDuringResume).toBe('running');
    expect(registeredOutput?.status).toBe('success');
    expect(registeredOutput?.runId).toBe('run-1');
  });

  it('guarded durable call options refuse memory configuration before runtime resume', async () => {
    const { runtime, resume } = fakeRuntime();
    resume.mockResolvedValue({ runId: 'run-1', status: 'success' });
    const agent = createFlowsafeDurableAgent({
      agent: guardedTestAgent(),
      runtime,
    });
    const pending = agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
      memory: {
        thread: 't',
        resource: 'r',
        options: { workingMemory: { enabled: true } },
      },
    });

    await expect(pending).rejects.toBeInstanceOf(TypeError);
    await expect(pending).rejects.toThrow('memory.options');
    expect(resume).not.toHaveBeenCalled();
  });

  it('rehydrates guarded registries without replaying application input processors', async () => {
    const order: string[] = [];
    const pubsub = new EventEmitterPubSub();
    const { runtime, resume } = fakeRuntime({ pubsub });
    resume.mockImplementation(async (_workflowId, runId, options) => {
      order.push('context');
      await options?.prepareExecution?.(actorContext());
      order.push('resume');
      return { runId, status: 'success' as const };
    });
    const registerRun = vi.fn(async () => {
      order.push('register');
    });
    const inputInvocation = vi.fn();
    const outputInvocation = vi.fn();
    const agent = createFlowsafeDurableAgent({
      agent: processorTestAgent({ inputInvocation, outputInvocation }),
      runtime,
      cache: false,
      threadRuntime: { registerRun } as never,
    });
    const prepare = vi.spyOn(agent, 'prepare');
    await agent.prepare('initial request', {
      runId: 'run-1',
      requestContext: actorContext(),
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    expect(inputInvocation).toHaveBeenCalledTimes(1);
    prepare.mockClear();
    registryFor(agent).clear();
    globalRunRegistry.clear();
    vi.spyOn(agent, 'observe').mockImplementation(async () => {
      expect(registryFor(agent).has('run-1')).toBe(true);
      expect(globalRunRegistry.has('run-1')).toBe(true);
      order.push('observe');
      return { output: { id: 'rehydrated' } } as never;
    });

    const summary = await agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
      step: ['tool-call'],
      resumeData: { approved: true },
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });

    expect(order).toEqual(['context', 'observe', 'register', 'resume']);
    expect(prepare).not.toHaveBeenCalled();
    expect(resume).toHaveBeenCalledWith(
      DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
      'run-1',
      expect.objectContaining({
        resumeData: { approved: true },
        requestedBy: 'reviewer-1',
        prepareExecution: expect.any(Function),
      }),
    );
    expect(summary).toMatchObject({ runId: 'run-1', status: 'success' });
    expect(inputInvocation).toHaveBeenCalledTimes(1);

    const instanceEntry = registryFor(agent).get('run-1');
    const globalEntry = globalRunRegistry.get('run-1');
    expect(instanceEntry?.inputProcessors?.map(({ id }) => id)).toEqual([
      'breakwater-rbac',
      'breakwater-client-tool-outcome-recorder',
      'application-input',
      'breakwater-input-assets',
      'breakwater-client-tool-output',
      'breakwater-policy-engine',
    ]);
    expect(globalEntry?.inputProcessors?.map(({ id }) => id)).toEqual([
      'breakwater-rbac',
      'breakwater-client-tool-outcome-recorder',
      'application-input',
      'breakwater-input-assets',
      'breakwater-client-tool-output',
      'breakwater-policy-engine',
    ]);
    expect(
      instanceEntry?.llmRequestInputProcessors?.map(({ id }) => id),
    ).toEqual(['breakwater-client-tool-outcome-recorder', 'application-input']);
    expect(globalEntry?.llmRequestInputProcessors?.map(({ id }) => id)).toEqual(
      ['breakwater-client-tool-outcome-recorder', 'application-input'],
    );
    expect(instanceEntry?.outputProcessors?.map(({ id }) => id)).toEqual([
      'application-output',
      'breakwater-policy-engine',
    ]);
    expect(globalEntry?.outputProcessors?.map(({ id }) => id)).toEqual([
      'application-output',
      'breakwater-policy-engine',
    ]);

    await expect(
      runOutputResultProcessors(
        globalEntry as RunRegistryEntry,
        'blocked-resume-output',
      ),
    ).rejects.toThrowError(policyDenialReason('deny-patterns', 'output'));
    expect(outputInvocation).toHaveBeenCalledTimes(1);
  });

  it.each([
    {
      failure: 'a memory processor lookup fails',
      setupMemory: () => {
        const memory = new MockMemory();
        return {
          memory,
          enableResume: () => {
            vi.spyOn(memory, 'getInputProcessors').mockRejectedValue(
              new Error('private memory failure'),
            );
          },
        };
      },
    },
    {
      failure:
        'memory resolves with title generation enabled during processor resolution',
      setupMemory: () => {
        const disabled = new MockMemory();
        const enabled = new MockMemory({ options: { generateTitle: true } });
        let resumeLeg = false;
        let resumeMemoryResolutions = 0;
        return {
          memory: () => {
            if (!resumeLeg) return disabled;
            resumeMemoryResolutions += 1;
            return resumeMemoryResolutions === 1 ? disabled : enabled;
          },
          enableResume: () => {
            resumeLeg = true;
          },
        };
      },
    },
  ])('refuses guarded resume before installation or execution when $failure', async ({
    setupMemory,
  }) => {
    const { memory, enableResume } = setupMemory();
    const { agent, audit, resume, resumeExecution, observe } =
      await guardedResumeFixture(memory);
    enableResume();

    const pending = agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await expect
      .soft(pending)
      .rejects.toThrow(
        /^Durable agent registry rehydration denied: input processor failed$/,
      );

    expect(resume).toHaveBeenCalledOnce();
    expect(resumeExecution).not.toHaveBeenCalled();
    expect(observe).not.toHaveBeenCalled();
    expect(registryFor(agent).has('run-1')).toBe(false);
    expect(globalRunRegistry.has('run-1')).toBe(false);
    expect(
      audit
        .events()
        .filter((event) => event.action === 'agent.input.processor'),
    ).toMatchObject([
      { decision: 'error', detail: { processor: 'breakwater-memory' } },
    ]);
  });

  it('refuses title-enabled memory on the first guarded resume resolution before installation or execution', async () => {
    const disabled = new MockMemory();
    let resumeLeg = false;
    let resumeMemoryResolutions = 0;
    const { agent, audit, resume, resumeExecution, observe } =
      await guardedResumeFixture(() => {
        if (!resumeLeg) return disabled;
        resumeMemoryResolutions += 1;
        return resumeMemoryResolutions === 1
          ? new MockMemory({ options: { generateTitle: true } })
          : disabled;
      });
    resumeLeg = true;

    const pending = agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await expect.soft(pending).rejects.toBeInstanceOf(TypeError);
    await expect.soft(pending).rejects.toThrow(/generateTitle/);

    expect.soft(resume).toHaveBeenCalledOnce();
    expect.soft(resumeExecution).not.toHaveBeenCalled();
    expect.soft(observe).not.toHaveBeenCalled();
    expect.soft(registryFor(agent).has('run-1')).toBe(false);
    expect.soft(globalRunRegistry.has('run-1')).toBe(false);
    expect
      .soft(
        audit
          .events()
          .filter((event) => event.action === 'agent.input.processor'),
      )
      .toEqual([]);
  });

  it('refuses a disallowed actor before auditing a guarded resume memory failure', async () => {
    const memory = new MockMemory();
    const { agent, audit, resume, resumeExecution, observe } =
      await guardedResumeFixture(memory, 'viewer');
    vi.spyOn(memory, 'getInputProcessors').mockRejectedValue(
      new Error('private memory failure'),
    );

    const pending = agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });

    await expect.soft(pending).rejects.toBeInstanceOf(Error);
    await expect
      .soft(pending)
      .rejects.toHaveProperty(
        'message',
        expect.stringMatching(/^Durable agent registry rehydration denied: /),
      );
    await expect
      .soft(pending)
      .rejects.toHaveProperty(
        'message',
        expect.not.stringContaining('input processor failed'),
      );
    expect(resume).toHaveBeenCalledOnce();
    expect(resumeExecution).not.toHaveBeenCalled();
    expect(observe).not.toHaveBeenCalled();
    expect(registryFor(agent).has('run-1')).toBe(false);
    expect(globalRunRegistry.has('run-1')).toBe(false);
    expect(
      audit
        .events()
        .filter((event) => event.action === 'agent.input.processor'),
    ).toEqual([]);
  });

  it('preserves raw-agent step and LLM-request processors without replaying processInput', async () => {
    const processInput = vi.fn(
      (args: Parameters<NonNullable<Processor['processInput']>>[0]) =>
        args.messages,
    );
    const processInputStep = vi.fn(
      (_args: Parameters<NonNullable<Processor['processInputStep']>>[0]) =>
        undefined,
    );
    const processLLMRequest = vi.fn(
      (args: Parameters<NonNullable<Processor['processLLMRequest']>>[0]) => ({
        prompt: args.prompt,
      }),
    );
    const runtimeProcessor = {
      id: 'raw-runtime-processor',
      processInput,
      processInputStep,
      processLLMRequest,
    } satisfies Processor;
    const rawAgent = new Agent({
      id: 'raw-writer',
      name: 'Raw writer',
      instructions: 'You are a raw test agent.',
      model: 'openai/gpt-4o-mini',
      inputProcessors: [runtimeProcessor],
    });
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: rawAgent, runtime });
    await agent.prepare('initial request', {
      runId: 'run-raw',
      requestContext: new RequestContext(),
    });
    expect(processInput).toHaveBeenCalledTimes(1);
    const initialEntry = registryFor(agent).get('run-raw');
    const initialInputProcessorIds = initialEntry?.inputProcessors?.map(
      ({ id }) => id,
    );
    const initialLLMRequestProcessorIds =
      initialEntry?.llmRequestInputProcessors?.map(({ id }) => id);

    registryFor(agent).clear();
    globalRunRegistry.clear();
    vi.spyOn(agent, 'observe').mockResolvedValue({
      output: { id: 'rehydrated' },
    } as never);

    await agent.resumeViaRuntime({
      runId: 'run-raw',
      requestedBy: 'reviewer-1',
    });

    expect(processInput).toHaveBeenCalledTimes(1);
    const rehydratedEntry = globalRunRegistry.get('run-raw');
    expect(rehydratedEntry?.inputProcessors?.map(({ id }) => id)).toEqual(
      initialInputProcessorIds,
    );
    expect(
      rehydratedEntry?.llmRequestInputProcessors?.map(({ id }) => id),
    ).toEqual(initialLLMRequestProcessorIds);

    const messageList = new MessageList();
    messageList.add('follow-up', 'input');
    const stepRunner = new ProcessorRunner({
      inputProcessors: rehydratedEntry?.inputProcessors,
      logger: {} as never,
      agentName: rawAgent.name,
      processorStates: rehydratedEntry?.processorStates,
    });
    await stepRunner.runProcessInputStep({
      messageList,
      stepNumber: 1,
      steps: [],
      model: rehydratedEntry?.model as never,
      requestContext: new RequestContext(),
    });
    const llmRequestRunner = new ProcessorRunner({
      inputProcessors: rehydratedEntry?.llmRequestInputProcessors,
      logger: {} as never,
      agentName: rawAgent.name,
      processorStates: rehydratedEntry?.processorStates,
    });
    await llmRequestRunner.runProcessLLMRequest({
      prompt: [],
      model: {},
      stepNumber: 1,
      steps: [],
      requestContext: new RequestContext(),
    });

    expect(processInputStep).toHaveBeenCalledTimes(1);
    expect(processLLMRequest).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['missing actor', undefined],
    ['disallowed actor', 'viewer' as const],
  ])('denies registry rehydration for a %s before installation or resume', async (_label, role) => {
    const { runtime, resume, resumeExecution } = fakeRuntime({
      resumeContext:
        role === undefined ? new RequestContext() : actorContext(role),
    });
    const registerRun = vi.fn(async () => undefined);
    const inputInvocation = vi.fn();
    const agent = createFlowsafeDurableAgent({
      agent: processorTestAgent({
        inputInvocation,
        outputInvocation: vi.fn(),
      }),
      runtime,
      threadRuntime: { registerRun } as never,
    });
    const observe = vi.spyOn(agent, 'observe');

    await expect(
      agent.resumeViaRuntime({ runId: 'run-1', requestedBy: 'reviewer-1' }),
    ).rejects.toThrow(/^Durable agent registry rehydration denied: /);

    expect(inputInvocation).not.toHaveBeenCalled();
    expect(registryFor(agent).has('run-1')).toBe(false);
    expect(globalRunRegistry.has('run-1')).toBe(false);
    expect(observe).not.toHaveBeenCalled();
    expect(registerRun).not.toHaveBeenCalled();
    expect(resume).toHaveBeenCalledOnce();
    expect(resumeExecution).not.toHaveBeenCalled();
  });

  it('publishes a registration failure and rethrows the original object', async () => {
    const { runtime, resume, resumeExecution } = fakeRuntime();
    const original = new Error('registration failed');
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      threadRuntime: {
        registerRun: vi.fn(async () => {
          throw original;
        }),
      } as never,
    });
    vi.spyOn(agent, 'observe').mockResolvedValue({
      output: { id: 'rehydrated' },
    } as never);
    const emitError = vi
      .spyOn(
        agent as unknown as {
          emitError: (id: string, error: Error) => Promise<void>;
        },
        'emitError',
      )
      .mockResolvedValue(undefined);

    await expect(
      agent.resumeViaRuntime({ runId: 'run-1', requestedBy: 'reviewer-1' }),
    ).rejects.toBe(original);
    expect(emitError).toHaveBeenCalledWith('run-1', original);
    expect(resume).toHaveBeenCalledOnce();
    expect(resumeExecution).not.toHaveBeenCalled();
  });

  it('keeps the live registry available while real resume error publication runs', async () => {
    const { runtime, resume } = fakeRuntime();
    resume.mockImplementation(async (_workflowId, _runId, options) => {
      await options?.prepareExecution?.(actorContext());
      return {
        runId: 'run-1',
        status: 'failed',
        error: 'resume failed',
      } as never;
    });
    const prototype = DurableAgent.prototype as unknown as {
      emitError(runId: string, error: Error): Promise<void>;
    };
    const realEmitError = prototype.emitError;
    const registryWasLive: boolean[] = [];
    vi.spyOn(prototype, 'emitError').mockImplementation(function (
      this: DurableAgent,
      runId,
      error,
    ) {
      registryWasLive.push(globalRunRegistry.has(runId));
      return realEmitError.call(this, runId, error);
    });
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      threadRuntime: { registerRun: vi.fn(async () => undefined) } as never,
    });
    vi.spyOn(agent, 'observe').mockResolvedValue({
      output: {
        id: 'rehydrated',
        runId: 'run-1',
        status: 'failed',
        _waitUntilFinished: () => new Promise<void>(() => undefined),
      },
    } as never);

    const summary = await agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
    });

    expect(summary.status).toBe('failed');
    expect(registryWasLive).toEqual([true]);
    expect(registryFor(agent).has('run-1')).toBe(false);
    expect(globalRunRegistry.has('run-1')).toBe(false);
  });

  it('cleans up and rethrows the original resume error when publication fails', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const { runtime, resume } = fakeRuntime();
    const original = new Error('resume rejected');
    resume.mockImplementation(async (_workflowId, _runId, options) => {
      await options?.prepareExecution?.(actorContext());
      throw original;
    });
    let registeredOutput: { _waitUntilFinished(): Promise<void> } | undefined;
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      threadRuntime: {
        registerRun: vi.fn(async (_agent, output) => {
          registeredOutput = output as typeof registeredOutput;
        }),
      } as never,
    });
    vi.spyOn(agent, 'observe').mockResolvedValue({
      output: {
        id: 'rehydrated',
        runId: 'run-1',
        status: 'running',
        _waitUntilFinished: () => new Promise<void>(() => undefined),
      },
    } as never);
    const emitError = vi
      .spyOn(
        agent as unknown as {
          emitError: (id: string, error: Error) => Promise<void>;
        },
        'emitError',
      )
      .mockRejectedValue(new Error('publication failed'));

    await expect(
      agent.resumeViaRuntime({ runId: 'run-1', requestedBy: 'reviewer-1' }),
    ).rejects.toBe(original);
    expect(emitError).toHaveBeenCalledOnce();
    expect(emitError).toHaveBeenCalledWith('run-1', original);
    expect(registryFor(agent).has('run-1')).toBe(false);
    expect(globalRunRegistry.has('run-1')).toBe(false);
    await expect(
      registeredOutput?._waitUntilFinished(),
    ).resolves.toBeUndefined();
  });

  it('publishes a failed resume summary and returns it', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const { runtime, resume } = fakeRuntime();
    resume.mockImplementation(async (_workflowId, _runId, options) => {
      await options?.prepareExecution?.(actorContext());
      return {
        runId: 'run-1',
        status: 'failed',
        error: 'resume failed',
      } as never;
    });
    let registeredOutput: { _waitUntilFinished(): Promise<void> } | undefined;
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      threadRuntime: {
        registerRun: vi.fn(async (_agent, output) => {
          registeredOutput = output as typeof registeredOutput;
        }),
      } as never,
    });
    vi.spyOn(agent, 'observe').mockResolvedValue({
      output: {
        id: 'rehydrated',
        runId: 'run-1',
        status: 'failed',
        _waitUntilFinished: () => new Promise<void>(() => undefined),
      },
    } as never);
    const emitError = vi
      .spyOn(
        agent as unknown as {
          emitError: (id: string, error: Error) => Promise<void>;
        },
        'emitError',
      )
      .mockRejectedValue(new Error('publication failed'));

    const summary = await agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
    });

    expect(summary).toMatchObject({ status: 'failed', error: 'resume failed' });
    expect(emitError.mock.calls[0]?.[1]?.message).toBe('resume failed');
    expect(registryFor(agent).has('run-1')).toBe(false);
    expect(globalRunRegistry.has('run-1')).toBe(false);
    await expect(
      registeredOutput?._waitUntilFinished(),
    ).resolves.toBeUndefined();
  });
});

describe('FlowsafeDurableAgent.executeWorkflow failed run', () => {
  it('emits an error onto the stream when the run fails', async () => {
    // #given a runtime whose start() resolves to a failed run
    const { runtime } = fakeRuntime({
      startResult: { runId: 'run-1', status: 'failed', error: 'boom' },
    });
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const streamResult = { output: { id: 'output' } };
    vi.spyOn(agent, 'stream').mockResolvedValue(streamResult as never);
    const emitError = vi
      .spyOn(
        agent as unknown as {
          emitError: (id: string, e: Error) => Promise<void>;
        },
        'emitError',
      )
      .mockResolvedValue(undefined);
    const pending = agent.streamUntilPersisted(
      'hello',
      { runId: 'run-1' },
      'operator-1',
      'human',
      undefined,
      undefined,
      undefined,
      startAuthority(),
    );
    // #when the host-registered loop drives it
    await Promise.all([drive(agent, 'run-1', INPUT), pending]);
    // #then the failed status is surfaced to observe()/onError via emitError
    expect(emitError).toHaveBeenCalledWith('run-1', expect.any(Error));
    expect(emitError.mock.calls[0]?.[1]?.message).toBe('boom');
  });
});

async function agentObservationFixture(threaded: boolean, customIds = false) {
  const f = await realAgentBridgeFixture();
  const workflow = f.agent.getWorkflow();
  if (customIds) {
    Object.defineProperty(f.agent, 'id', { value: 'display-agent' });
    Object.defineProperty(workflow, 'id', { value: 'actual-agent-loop' });
    f.runtime.register(
      workflow as unknown as import('@mastra/core/workflows').AnyWorkflow,
    );
  }
  await f.runtime.status(workflow.id, 'observed-agent-run');
  const snapshot = {
    runId: 'observed-agent-run',
    status: 'pending',
    context: {},
    requestContext: {
      'flowsafe.runProvenance': {
        version: 2,
        startToken: 'S1',
        attemptToken: 'H',
        requestedBy: 'operator-1',
        requestedByKind: 'human',
        resumeCounts: [],
        startIdentity: {
          owner: { kind: 'human', id: 'operator-1' },
          target: { kind: 'agent', id: 'writer', threadId: 'thread-1' },
        },
        agentStart: { threaded },
      },
    },
    value: {},
    serializedStepGraph: [],
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    waitingPaths: {},
    timestamp: 100,
  };
  const seed = () =>
    f.workflows.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: 'observed-agent-run',
      snapshot:
        snapshot as unknown as import('@mastra/core/workflows').WorkflowRunState,
    });
  await seed();
  return { ...f, workflow, snapshot, seed };
}

describe('agent observation', () => {
  it.each([
    false,
    true,
  ])('reads initial mode %s without input or optional pruned context', async (threaded) => {
    const f = await agentObservationFixture(threaded, true);
    try {
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi.spyOn(capability, 'readSnapshot');
      const state = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'thread-1',
          'observed-agent-run',
        )
        .catch((error) => error);
      expect(state).toMatchObject({
        kind: 'initial',
        threaded,
        execution: {
          workflowId: 'actual-agent-loop',
          startToken: 'S1',
          owner: { id: 'operator-1' },
          target: { id: 'writer', threadId: 'thread-1' },
        },
      });
      expect(state).not.toHaveProperty('summary');
      expect(read).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
      await expect(
        f.agent.proofExecutionFor(f.runtime, 'thread-1', 'observed-agent-run'),
      ).resolves.toEqual({
        tablePrefix: '',
        workflowId: 'actual-agent-loop',
        runId: 'observed-agent-run',
        startToken: 'S1',
      });
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    {
      corruption: 'runtime',
      wrongRuntime: true,
      threadId: 'thread-1',
      selectorMismatch: false,
      mutate: (_snapshot) => {},
    },
    {
      corruption: 'thread',
      wrongRuntime: false,
      threadId: 'wrong',
      selectorMismatch: true,
      mutate: (_snapshot) => {},
    },
    {
      corruption: 'agent',
      wrongRuntime: false,
      threadId: 'thread-1',
      selectorMismatch: true,
      mutate: (snapshot) => {
        snapshot.requestContext[
          'flowsafe.runProvenance'
        ].startIdentity.target.id = 'wrong';
      },
    },
    {
      corruption: 'input',
      wrongRuntime: false,
      threadId: 'thread-1',
      selectorMismatch: false,
      mutate: (snapshot) => {
        Object.assign(snapshot.context, { input: { agentId: 'wrong' } });
      },
    },
    {
      corruption: 'memory',
      wrongRuntime: false,
      threadId: 'thread-1',
      selectorMismatch: false,
      mutate: (snapshot) => {
        Object.assign(snapshot.context, {
          input: { agentId: 'writer', messageListState: { memoryInfo: null } },
        });
      },
    },
    {
      corruption: 'audit',
      wrongRuntime: false,
      threadId: 'thread-1',
      selectorMismatch: false,
      mutate: (snapshot) => {
        Object.assign(snapshot.requestContext, {
          'breakwater.auditContext': { threadId: 'wrong' },
        });
      },
    },
  ] satisfies Array<{
    corruption: string;
    wrongRuntime: boolean;
    threadId: string;
    selectorMismatch: boolean;
    mutate: (
      snapshot: Awaited<ReturnType<typeof agentObservationFixture>>['snapshot'],
    ) => void;
  }>)('refuses present $corruption disagreements without engine work', async ({
    wrongRuntime,
    threadId,
    selectorMismatch,
    mutate,
  }) => {
    const f = await agentObservationFixture(true);
    try {
      mutate(f.snapshot);
      await f.seed();
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi.spyOn(capability, 'readSnapshot');
      const { RunStateUnreadableError: ReloadedRunStateUnreadableError } =
        await import('../do-runner/runtime.js');
      const outcome = await f.agent
        .authoritativeAgentStartState(
          wrongRuntime ? ({} as RunnerRuntime) : f.runtime,
          threadId,
          'observed-agent-run',
        )
        .catch((error) => error);
      expect(f.counts.model).toBe(0);
      if (wrongRuntime) expect(read).not.toHaveBeenCalled();
      else expect(read).toHaveBeenCalledOnce();
      expect(outcome).toBeInstanceOf(ReloadedRunStateUnreadableError);
      if (selectorMismatch)
        expect(outcome).toBeInstanceOf(AgentRunSelectorMismatchError);
      else expect(outcome).not.toBeInstanceOf(AgentRunSelectorMismatchError);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it('returns S1 and its selected value when S2 replaces storage after the read', async () => {
    const f = await agentObservationFixture(false);
    try {
      Object.assign(f.snapshot, {
        status: 'success',
        result: { generation: 'S1' },
      });
      await f.seed();
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = capability.readSnapshot;
      const selected = vi
        .spyOn(capability, 'readSnapshot')
        .mockImplementation(async (address) => {
          const row = await read(address);
          f.snapshot.requestContext['flowsafe.runProvenance'].startToken = 'S2';
          Object.assign(f.snapshot, { result: { generation: 'S2' } });
          await f.seed();
          return row;
        });
      const state = await f.agent.authoritativeAgentStartState(
        f.runtime,
        'thread-1',
        'observed-agent-run',
      );
      expect(state).toMatchObject({
        kind: 'result',
        execution: { startToken: 'S1' },
        summary: { status: 'success', result: { generation: 'S1' } },
      });
      expect(selected).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    false,
    true,
  ])('rejects the agent persistence waiter when mode %s only persists pending after engine completion', async (threaded) => {
    const f = await realAgentBridgeFixture(undefined, undefined, threaded);
    const runId = `agent-pending-${threaded}`;
    const streams: Array<Awaited<ReturnType<typeof f.agent.stream>>> = [];
    const stream = f.agent.stream.bind(f.agent);
    vi.spyOn(f.agent, 'stream').mockImplementation(async (...args) => {
      const value = await stream(...args);
      streams.push(value);
      return value;
    });
    try {
      const persist = f.workflows.persistWorkflowSnapshot.bind(f.workflows);
      vi.spyOn(f.workflows, 'persistWorkflowSnapshot').mockImplementation(
        (input) =>
          persist(
            input.snapshot.status === 'pending'
              ? input
              : {
                  ...input,
                  snapshot: { ...input.snapshot, status: 'pending' as const },
                },
          ),
      );
      const authority = { ...startAuthority(), agentStart: { threaded } };
      const result = await f.agent
        .streamUntilPersisted(
          'Return done.',
          {
            runId,
            maxSteps: 1,
            disableBackgroundTasks: true,
            ...(threaded
              ? { memory: { thread: 'thread-1', resource: 'thread-1' } }
              : {}),
          },
          'operator-1',
          'human',
          'H',
          undefined,
          undefined,
          authority,
        )
        .catch((error) => error);
      const row = await f.workflows.loadWorkflowSnapshot({
        workflowName: f.agent.getWorkflow().id,
        runId,
      });
      expect(row?.status).toBe('pending');
      expect(f.counts.model).toBe(1);
      const { RunStartPendingError } = await import(
        '../do-runner/execution-admission.js'
      );
      expect(result).toBeInstanceOf(RunStartPendingError);
    } finally {
      await globalRunRegistry
        .get(runId)
        ?.workflowExecution?.catch(() => undefined);
      for (const value of streams) value.cleanup();
      globalRunRegistry.delete(runId);
      f.start.mockRestore();
      f.sql.close();
    }
  });
});

async function legacyAgentFixture(version: 'v1' | 'absent', threaded: boolean) {
  const f = await agentObservationFixture(threaded, true);
  const snapshot = structuredClone(
    f.snapshot,
  ) as unknown as import('@mastra/core/workflows').WorkflowRunState;
  snapshot.status = 'success';
  snapshot.result = { legacy: true };
  snapshot.context.input = {
    agentId: 'writer',
    runId: 'observed-agent-run',
    messageListState: {
      memoryInfo: threaded
        ? { threadId: 'thread-1', resourceId: 'thread-1' }
        : null,
    },
  };
  snapshot.requestContext = {
    runId: 'observed-agent-run',
    threadId: 'thread-1',
    resourceId: 'thread-1',
    'breakwater.auditContext': {
      agentId: 'writer',
      threadId: 'thread-1',
      resourceId: 'thread-1',
    },
  };
  if (version === 'v1')
    snapshot.requestContext['flowsafe.runProvenance'] = {
      version: 1,
      attemptToken: 'legacy-H',
      requestedBy: 'current-reviewer',
      requestedByKind: 'service',
      resumeCounts: [],
    };
  const seed = () =>
    f.workflows.persistWorkflowSnapshot({
      workflowName: f.workflow.id,
      runId: 'observed-agent-run',
      snapshot,
    });
  await seed();
  return { ...f, snapshot, seed };
}

describe('agent selector mismatch classification', () => {
  it.each([
    {
      status: 'pending',
      threaded: true,
      metadata: false,
      foreign: 'agent',
      agentId: 'other-agent',
      threadId: 'thread-1',
    },
    {
      status: 'success',
      threaded: false,
      metadata: false,
      foreign: 'thread',
      agentId: 'writer',
      threadId: 'other-thread',
    },
    {
      status: 'pending',
      threaded: false,
      metadata: true,
      foreign: 'both',
      agentId: 'other-agent',
      threadId: 'other-thread',
    },
    {
      status: 'success',
      threaded: true,
      metadata: true,
      foreign: 'agent',
      agentId: 'other-agent',
      threadId: 'thread-1',
    },
    {
      status: 'success',
      threaded: false,
      metadata: true,
      foreign: 'thread',
      agentId: 'writer',
      threadId: 'other-thread',
    },
    {
      status: 'pending',
      threaded: true,
      metadata: true,
      foreign: 'both',
      agentId: 'other-agent',
      threadId: 'other-thread',
    },
  ] as const)('classifies a coherent foreign modern tuple from one row ($status threaded=$threaded metadata=$metadata $foreign)', async ({
    status,
    threaded,
    metadata,
    agentId,
    threadId,
  }) => {
    const f = await agentObservationFixture(threaded);
    try {
      Object.assign(f.snapshot, { status, result: { selected: 'S1' } });
      Object.assign(
        f.snapshot.requestContext['flowsafe.runProvenance'].startIdentity
          .target,
        { id: agentId, threadId },
      );
      if (metadata) {
        Object.assign(f.snapshot.requestContext, {
          runId: 'observed-agent-run',
          threadId,
          resourceId: threadId,
          'breakwater.auditContext': {
            agentId,
            threadId,
            resourceId: threadId,
          },
        });
        Object.assign(f.snapshot.context, {
          input: {
            agentId,
            runId: 'observed-agent-run',
            messageListState: {
              memoryInfo: threaded ? { threadId, resourceId: threadId } : null,
            },
          },
        });
      }
      await f.seed();
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi.spyOn(capability, 'readSnapshot');
      const ordinary = vi.spyOn(f.workflows, 'loadWorkflowSnapshot');
      const outcome = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'thread-1',
          'observed-agent-run',
        )
        .catch((error) => error);
      expect(outcome).toBeInstanceOf(AgentRunSelectorMismatchError);
      expect(outcome).toBeInstanceOf(RunStateUnreadableError);
      expect(doErrorResponse(outcome).status).toBe(503);
      expect(read).toHaveBeenCalledOnce();
      expect(ordinary).not.toHaveBeenCalled();
      read.mockClear();
      await expect(
        f.agent.proofExecutionFor(f.runtime, 'thread-1', 'observed-agent-run'),
      ).rejects.toBeInstanceOf(AgentRunSelectorMismatchError);
      expect(read).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    {
      version: 'v1',
      threaded: true,
      foreign: 'agent',
      agentId: 'other-agent',
      threadId: 'thread-1',
    },
    {
      version: 'v1',
      threaded: false,
      foreign: 'thread',
      agentId: 'writer',
      threadId: 'other-thread',
    },
    {
      version: 'absent',
      threaded: true,
      foreign: 'both',
      agentId: 'other-agent',
      threadId: 'other-thread',
    },
    {
      version: 'absent',
      threaded: false,
      foreign: 'agent',
      agentId: 'other-agent',
      threadId: 'thread-1',
    },
  ] as const)('classifies a coherent foreign legacy tuple ($version threaded=$threaded $foreign)', async ({
    version,
    threaded,
    agentId,
    threadId,
  }) => {
    const f = await legacyAgentFixture(version, threaded);
    try {
      Object.assign(f.snapshot.requestContext ?? {}, {
        threadId,
        resourceId: threadId,
        'breakwater.auditContext': { agentId, threadId, resourceId: threadId },
      });
      Object.assign(f.snapshot.context.input ?? {}, {
        agentId,
        messageListState: {
          memoryInfo: threaded ? { threadId, resourceId: threadId } : null,
        },
      });
      await f.seed();
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi.spyOn(capability, 'readSnapshot');
      const outcome = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'thread-1',
          'observed-agent-run',
          {
            includeLegacy: true,
          },
        )
        .catch((error) => error);
      expect(outcome).toBeInstanceOf(AgentRunSelectorMismatchError);
      expect(outcome).toBeInstanceOf(RunStateUnreadableError);
      expect(outcome).not.toHaveProperty('execution');
      expect(read).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    { shape: 'modern', modern: true, version: 'v1' },
    { shape: 'v1', modern: false, version: 'v1' },
    { shape: 'absent', modern: false, version: 'absent' },
  ] as const)('checks internal coherence before foreign lookup classification: $shape', async ({
    modern,
    version,
  }) => {
    const f = modern
      ? await agentObservationFixture(true)
      : await legacyAgentFixture(version, true);
    try {
      Object.assign(f.snapshot.requestContext ?? {}, {
        'breakwater.auditContext': { agentId: 'contradiction' },
      });
      await f.seed();
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi.spyOn(capability, 'readSnapshot');
      const outcome = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'other-thread',
          'observed-agent-run',
          {
            includeLegacy: true,
          },
        )
        .catch((error) => error);
      expect(outcome).toBeInstanceOf(RunStateUnreadableError);
      expect(outcome).not.toBeInstanceOf(AgentRunSelectorMismatchError);
      expect(read).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    { shape: 'modern', modern: true, version: 'v1' },
    { shape: 'v1', modern: false, version: 'v1' },
  ] as const)('classifies the selected foreign row when replacement storage matches the selector: $shape', async ({
    modern,
    version,
  }) => {
    const f = modern
      ? await agentObservationFixture(false)
      : await legacyAgentFixture(version, false);
    try {
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const native = capability.readSnapshot;
      const read = vi
        .spyOn(capability, 'readSnapshot')
        .mockImplementation(async (address) => {
          const row = await native(address);
          if (modern) {
            const provenance =
              f.snapshot.requestContext?.['flowsafe.runProvenance'];
            provenance.startIdentity.target.threadId = 'replacement-thread';
          } else {
            Object.assign(f.snapshot.requestContext ?? {}, {
              threadId: 'replacement-thread',
              resourceId: 'replacement-thread',
              'breakwater.auditContext': {
                agentId: 'writer',
                threadId: 'replacement-thread',
                resourceId: 'replacement-thread',
              },
            });
          }
          await f.seed();
          return row;
        });
      const outcome = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'replacement-thread',
          'observed-agent-run',
          { includeLegacy: true },
        )
        .catch((error) => error);
      expect(outcome).toBeInstanceOf(AgentRunSelectorMismatchError);
      expect(read).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    { failure: 'undefined', resolves: true, unknownRun: false },
    { failure: 'error', resolves: false, unknownRun: false },
    { failure: 'unknown run', resolves: false, unknownRun: true },
  ] as const)('keeps failed selected sources unreadable rather than classifying a lookup miss: $failure', async ({
    resolves,
    unknownRun,
  }) => {
    const f = await agentObservationFixture(false);
    try {
      const source = vi.spyOn(f.runtime, 'authoritativeStartState');
      if (resolves) source.mockResolvedValue(undefined as never);
      else
        source.mockRejectedValue(
          unknownRun
            ? new UnknownRunError(f.workflow.id, 'observed-agent-run')
            : new Error('source failed'),
        );
      const outcome = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'other-thread',
          'observed-agent-run',
          {
            includeLegacy: true,
          },
        )
        .catch((error) => error);
      expect(outcome).toBeInstanceOf(RunStateUnreadableError);
      expect(outcome).not.toBeInstanceOf(AgentRunSelectorMismatchError);
      expect(source).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });
});

describe('legacy agent observations', () => {
  it.each(
    (['v1', 'absent'] as const).flatMap((version) =>
      [false, true].map((threaded) => ({ version, threaded })),
    ),
  )('reads $version mode $threaded from the actual wrapper source once without generation authority', async ({
    version,
    threaded,
  }) => {
    const f = await legacyAgentFixture(version, threaded);
    try {
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi.spyOn(capability, 'readSnapshot');
      const ordinary = vi.spyOn(f.workflows, 'loadWorkflowSnapshot');
      const pending = f.agent.authoritativeAgentStartState(
        f.runtime,
        'thread-1',
        'observed-agent-run',
        { includeLegacy: true },
      );
      await expect(pending).resolves.toMatchObject({
        kind: 'legacy',
        provenanceVersion: version === 'v1' ? 1 : undefined,
        address: {
          tablePrefix: '',
          workflowId: 'actual-agent-loop',
          runId: 'observed-agent-run',
        },
        threaded,
        summary: { status: 'success', result: { legacy: true } },
      });
      const result = await pending;
      expect(result).not.toHaveProperty('execution');
      expect(read).toHaveBeenCalledOnce();
      expect(ordinary).not.toHaveBeenCalled();
      expect(f.counts.model).toBe(0);
      if (version === 'v1')
        expect(result?.summary).toMatchObject({
          requestedBy: 'current-reviewer',
          requestedByKind: 'service',
        });
      await expect(
        f.agent.authoritativeAgentStartState(
          f.runtime,
          'thread-1',
          'observed-agent-run',
        ),
      ).rejects.toBeInstanceOf(RunStateUnreadableError);
      await expect(
        f.agent.proofExecutionFor(f.runtime, 'thread-1', 'observed-agent-run'),
      ).rejects.toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    { field: 'input-agent', target: 'input', key: 'agentId' },
    { field: 'input-run', target: 'input', key: 'runId' },
    { field: 'context-run', target: 'context', key: 'runId' },
    { field: 'context-thread', target: 'context', key: 'threadId' },
    { field: 'context-resource', target: 'context', key: 'resourceId' },
    { field: 'audit-agent', target: 'audit', key: 'agentId' },
    { field: 'audit-thread', target: 'audit', key: 'threadId' },
    { field: 'audit-resource', target: 'audit', key: 'resourceId' },
    { field: 'memory-thread', target: 'memory', key: 'threadId' },
    { field: 'missing-input', target: 'missing', key: 'input' },
  ] as const)('rejects legacy $field contradictions in its one selected snapshot', async ({
    target,
    key,
  }) => {
    const f = await legacyAgentFixture('v1', true);
    try {
      const context = f.snapshot.requestContext;
      assert(context);
      const input = f.snapshot.context.input as unknown as {
        agentId: string;
        runId: string;
        messageListState: {
          memoryInfo: { threadId: string; resourceId: string };
        };
      };
      const targets = {
        input,
        context,
        audit: context['breakwater.auditContext'],
        memory: input.messageListState.memoryInfo,
        missing: undefined,
      };
      const changed = targets[target];
      if (changed) Object.assign(changed, { [key]: 'wrong' });
      else delete f.snapshot.context.input;
      await f.seed();
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi.spyOn(capability, 'readSnapshot');
      const result = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'thread-1',
          'observed-agent-run',
          {
            includeLegacy: true,
          },
        )
        .catch((error) => error);
      expect(read).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
      expect(result).toBeInstanceOf(RunStateUnreadableError);
      expect(result).not.toBeInstanceOf(AgentRunSelectorMismatchError);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it('captures the legacy option and keeps the selected S1-era value when storage advances during a read', async () => {
    const f = await legacyAgentFixture('v1', false),
      entered = bridgeDeferred(),
      release = bridgeDeferred();
    let pending: Promise<unknown> | undefined;
    try {
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const native = capability.readSnapshot;
      const read = vi
        .spyOn(capability, 'readSnapshot')
        .mockImplementation(async (address) => {
          const row = await native(address);
          entered.resolve();
          await release.promise;
          return row;
        });
      const options = { includeLegacy: true as const };
      pending = f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'thread-1',
          'observed-agent-run',
          options,
        )
        .catch((error) => error);
      await entered.promise;
      Object.assign(options, { includeLegacy: false });
      f.snapshot.result = { replacement: true };
      await f.seed();
      release.resolve();
      const result = await pending;
      expect(result).toMatchObject({
        kind: 'legacy',
        summary: { result: { legacy: true } },
      });
      expect(read).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
    } finally {
      release.resolve();
      await pending;
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it('rejects a different expected Runtime before legacy source I/O', async () => {
    const f = await legacyAgentFixture('absent', false);
    try {
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi.spyOn(capability, 'readSnapshot');
      const result = await f.agent
        .authoritativeAgentStartState(
          {} as RunnerRuntime,
          'thread-1',
          'observed-agent-run',
          { includeLegacy: true },
        )
        .catch((error) => error);
      expect(read).not.toHaveBeenCalled();
      expect(f.counts.model).toBe(0);
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it('does not fall back to another snapshot when the selected legacy-capable read fails', async () => {
    const f = await legacyAgentFixture('v1', false);
    try {
      const capability = f.workflows[FENCED_WORKFLOW_STORAGE];
      assert(capability);
      const read = vi
        .spyOn(capability, 'readSnapshot')
        .mockRejectedValue(new Error('source failed'));
      const ordinary = vi.spyOn(f.workflows, 'loadWorkflowSnapshot');
      const result = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'thread-1',
          'observed-agent-run',
          {
            includeLegacy: true,
          },
        )
        .catch((error) => error);
      expect(read).toHaveBeenCalledOnce();
      expect(ordinary).not.toHaveBeenCalled();
      expect(f.counts.model).toBe(0);
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it('keeps its default strict even if a Runtime override returns a legacy arm without opt-in', async () => {
    const f = await legacyAgentFixture('v1', false);
    try {
      const legacy = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'observed-agent-run',
        { includeLegacy: true },
      );
      assert(legacy?.kind === 'legacy');
      const read = vi
        .spyOn(f.runtime, 'authoritativeStartState')
        .mockResolvedValue(legacy as never);
      const result = await f.agent
        .authoritativeAgentStartState(
          f.runtime,
          'thread-1',
          'observed-agent-run',
        )
        .catch((error) => error);
      expect(read).toHaveBeenCalledOnce();
      expect(f.counts.model).toBe(0);
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.start.mockRestore();
      f.sql.close();
    }
  });

  it('retains strict wrapper inference and exposes no execution identity on the legacy type', () => {
    expectTypeOf<
      ReturnType<FlowsafeDurableAgent['authoritativeAgentStartState']>
    >().toEqualTypeOf<Promise<AuthoritativeAgentStartState | null>>();
    expectTypeOf<
      Parameters<FlowsafeDurableAgent['authoritativeAgentStartState']>
    >().toEqualTypeOf<[RunnerRuntime, string, string]>();
    const readLegacy = (agent: FlowsafeDurableAgent, runtime: RunnerRuntime) =>
      agent.authoritativeAgentStartState(runtime, 'thread', 'run', {
        includeLegacy: true,
      });
    expectTypeOf(readLegacy).returns.toEqualTypeOf<
      Promise<AuthoritativeAgentStartState | LegacyAgentRunState | null>
    >();
    expectTypeOf<
      Extract<
        keyof LegacyAgentRunState,
        'execution' | 'startToken' | 'attemptToken'
      >
    >().toEqualTypeOf<never>();
  });
});
