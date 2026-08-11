// SPDX-License-Identifier: Apache-2.0
// CI-M-002-001: FlowsafeDurableAgent drives the durable-agentic-loop through
// RunnerRuntime. These pin the three load-bearing properties without a live
// LLM (executeWorkflow never invokes the model — it only drives runtime.start):
//   - the loop is driven via runtime.start('durable-agentic-loop', { runId, inputData })
//   - runId is REQUIRED with INV-1 posture — NO crypto.randomUUID fallback
//   - the raw agent and loop workflow are registered on the runtime; workflow
//     registration is idempotent because the loop id is shared
//
// The engine-leg-context -> tool -> grant round-trip (S1/S2) is proven end to
// end against the REAL runtime + connector + grant provider in
// agent-gate-round-trip.test.ts.

import { Agent } from '@mastra/core/agent';
import {
  DurableAgent,
  type DurableAgenticWorkflowInput,
  type ExtendedRunRegistry,
  globalRunRegistry,
  type RunRegistryEntry,
} from '@mastra/core/agent/durable';
import {
  type MastraDBMessage,
  MessageList,
} from '@mastra/core/agent/message-list';
import { EventEmitterPubSub } from '@mastra/core/events';
import {
  type OutputResult,
  type Processor,
  ProcessorRunner,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import {
  ACTOR_CONTEXT_KEY,
  AuditLogger,
  createGuardedAgent,
  denyPatterns,
  type Role,
} from '@proofoftech/breakwater';
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  InvalidRunRequestError,
  type RunnerRuntime,
} from '../do-runner/index.js';
import {
  createFlowsafeDurableAgent,
  DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
  type FlowsafeDurableAgent,
  isRuntimeDrivenAgent,
} from './durable-agent-runner.js';

// A fake runtime that records register() and start() and models the shared-id
// registry so the idempotency path is exercised. Cast to RunnerRuntime because
// the runner only ever calls register/workflowIds/start/pubsub. `startResult`
// overrides the summary start() resolves to (e.g. a 'failed' run); `pubsub`
// exposes an identity for the inheritance test.
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
    async (_workflowId: string, options: { runId: string }) =>
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

function testAgent(id = 'writer'): Agent {
  return new Agent({
    id,
    name: id,
    instructions: 'You are a test agent.',
    // A model-router id string (never invoked): executeWorkflow drives the
    // runtime, not the LLM, so the agent only has to construct.
    model: 'openai/gpt-4o-mini',
  });
}

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

// executeWorkflow is protected — the durable loop calls it, and no route ever
// does. Reach it through a cast for the drive/guard assertions.
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

const INPUT = {
  __workflowKind: 'durable-agent',
  runId: 'run-1',
  agentId: 'writer',
} as unknown as DurableAgenticWorkflowInput;

describe('createFlowsafeDurableAgent', () => {
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
    // the property Track C's thread-DO wake gate requires (a plain Agent's wake
    // would run the loop OFF the runtime).
    expect(isRuntimeDrivenAgent(durable)).toBe(true);
    expect(isRuntimeDrivenAgent(testAgent())).toBe(false);
    expect(isRuntimeDrivenAgent({})).toBe(false);
    expect(isRuntimeDrivenAgent(undefined)).toBe(false);
  });
});

describe('FlowsafeDurableAgent.executeWorkflow', () => {
  it("drives runtime.start('durable-agentic-loop', { runId, inputData })", async () => {
    // #given
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #when the loop drives a run
    await drive(agent, 'run-1', INPUT);
    // #then it is routed through the runtime chokepoint, not createRun + start
    expect(start).toHaveBeenCalledTimes(1);
    expect(start).toHaveBeenCalledWith(DURABLE_AGENTIC_LOOP_WORKFLOW_ID, {
      runId: 'run-1',
      inputData: INPUT,
    });
  });

  it('rejects an absent runId (INV-1: no crypto.randomUUID fallback)', async () => {
    // #given
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #when / #then a missing runId is a client error, never a generated one
    await expect(drive(agent, undefined, INPUT)).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    expect(start).not.toHaveBeenCalled();
  });

  it('rejects a non-path-safe runId (INV-1 posture identical to RunnerRuntime.start)', async () => {
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

describe('FlowsafeDurableAgent.streamUntilPersisted', () => {
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
      .streamUntilPersisted('hello', { runId: 'run-1' }, 'operator-1', 'human')
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
      ),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);

    expect(stream).not.toHaveBeenCalled();
    expect(start).not.toHaveBeenCalled();
  });
});

// The public entry points a host actually calls. The inherited stream()/generate()
// take an OPTIONAL runId; without the override, core mints an unowned
// crypto.randomUUID() upstream of executeWorkflow's guard (INV-1 violation).
// These pin that the boundary refuses an absent/non-path-safe runId before any
// run is registered.
describe('FlowsafeDurableAgent INV-1 boundary (stream/generate)', () => {
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

// prepare() is the THIRD inherited minting entry point (stream/generate are the
// other two): it forwards options?.runId into core's prepareForDurableExecution,
// which mints an unowned crypto.randomUUID() AND registers a run under it when
// runId is absent (@mastra/core 1.50.0 agent/durable/index.js:5980 -> :589 ->
// :5984). PATH_SAFE_ID_PATTERN accepts a bare UUID, so no downstream guard
// (executeWorkflow's re-guard, RunnerRuntime.start) can catch it — the override
// must refuse an absent/non-INV-1 runId BEFORE super.prepare mints or registers
// anything. super.prepare is spied so the accept path proves delegation without
// driving core's real preparation (which resolves model/tools and touches the
// registry) — and so the reject paths prove it is never reached.
describe('FlowsafeDurableAgent INV-1 boundary (prepare)', () => {
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
    // #when — a caller-minted INV-1 runId
    const result = await agent.prepare('Hello!', { runId: 'run-1' });
    // #then — the guard passed and the call reached super unchanged
    expect(superPrepare).toHaveBeenCalledTimes(1);
    expect(superPrepare).toHaveBeenCalledWith('Hello!', { runId: 'run-1' });
    expect(result).toBe(prepared);
  });
});

describe('FlowsafeDurableAgent pubsub identity (DL-001)', () => {
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
});

describe('FlowsafeDurableAgent thread runtime registration and rehydration', () => {
  afterEach(() => {
    globalRunRegistry.clear();
    vi.restoreAllMocks();
  });

  it('registers a started stream output under the same pubsub and memory options', async () => {
    const pubsub = new EventEmitterPubSub();
    const { runtime } = fakeRuntime({ pubsub });
    const registerRun = vi.fn(async () => undefined);
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      cache: false,
      threadRuntime: { registerRun } as never,
    });
    const output = { id: 'output' };
    vi.spyOn(DurableAgent.prototype, 'stream').mockResolvedValue({
      output,
    } as never);
    const options = {
      runId: 'run-1',
      memory: { thread: 'thread-1', resource: 'resource-1' },
    } as never;

    await agent.stream('hello', options);

    expect(registerRun).toHaveBeenCalledWith(agent, output, options, pubsub);
  });

  it.each([
    ['boolean true', true],
    ['object-valued untilIdle', { maxWaitMs: 1000 }],
  ])('does not register the outer aggregate stream for %s', async (_label, untilIdle) => {
    const pubsub = new EventEmitterPubSub();
    const { runtime } = fakeRuntime({ pubsub });
    const registerRun = vi.fn(async () => undefined);
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      cache: false,
      threadRuntime: { registerRun } as never,
    });
    vi.spyOn(DurableAgent.prototype, 'stream').mockResolvedValue({
      output: { id: 'aggregate' },
    } as never);

    await agent.stream('hello', {
      runId: 'run-1',
      untilIdle,
    } as never);

    expect(registerRun).not.toHaveBeenCalled();
  });

  it('registers a concrete stream when untilIdle is explicitly false', async () => {
    const { runtime } = fakeRuntime();
    const registerRun = vi.fn(async () => undefined);
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      threadRuntime: { registerRun } as never,
    });
    vi.spyOn(DurableAgent.prototype, 'stream').mockResolvedValue({
      output: { id: 'concrete' },
    } as never);

    await agent.stream('hello', {
      runId: 'run-1',
      untilIdle: false,
    } as never);

    expect(registerRun).toHaveBeenCalledTimes(1);
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
      'application-input',
      'breakwater-policy-engine',
    ]);
    expect(globalEntry?.inputProcessors?.map(({ id }) => id)).toEqual([
      'breakwater-rbac',
      'application-input',
      'breakwater-policy-engine',
    ]);
    expect(
      instanceEntry?.llmRequestInputProcessors?.map(({ id }) => id),
    ).toEqual(['application-input']);
    expect(globalEntry?.llmRequestInputProcessors?.map(({ id }) => id)).toEqual(
      ['application-input'],
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
    ).rejects.toThrow('matched blocked pattern blocked-resume-output');
    expect(outputInvocation).toHaveBeenCalledTimes(1);
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

  it('publishes a resume rejection and preserves it if publication also fails', async () => {
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
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(
      agent as unknown as {
        emitError: (id: string, error: Error) => Promise<void>;
      },
      'emitError',
    ).mockRejectedValue(new Error('publication failed'));

    await expect(
      agent.resumeViaRuntime({ runId: 'run-1', requestedBy: 'reviewer-1' }),
    ).rejects.toBe(original);
    expect(log).toHaveBeenCalledWith(
      expect.stringContaining('durable-agent-resume-error-publication-failed'),
    );
    await expect(
      registeredOutput?._waitUntilFinished(),
    ).resolves.toBeUndefined();
  });

  it('publishes a failed resume summary and returns it', async () => {
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
      .mockResolvedValue(undefined);

    const summary = await agent.resumeViaRuntime({
      runId: 'run-1',
      requestedBy: 'reviewer-1',
    });

    expect(summary).toMatchObject({ status: 'failed', error: 'resume failed' });
    expect(emitError.mock.calls[0]?.[1]?.message).toBe('resume failed');
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
    const emitError = vi
      .spyOn(
        agent as unknown as {
          emitError: (id: string, e: Error) => Promise<void>;
        },
        'emitError',
      )
      .mockResolvedValue(undefined);
    // #when the loop drives it
    await drive(agent, 'run-1', INPUT);
    // #then the failed status is surfaced to observe()/onError via emitError
    expect(emitError).toHaveBeenCalledWith('run-1', expect.any(Error));
    expect(emitError.mock.calls[0]?.[1]?.message).toBe('boom');
  });
});
