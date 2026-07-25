// SPDX-License-Identifier: Apache-2.0
// CI-M-002-001: FlowsafeDurableAgent drives the durable-agentic-loop through
// RunnerRuntime. These pin the three load-bearing properties without a live
// LLM (executeWorkflow never invokes the model — it only drives runtime.start):
//   - the loop is driven via runtime.start('durable-agentic-loop', { runId, inputData })
//   - runId is REQUIRED with INV-1 posture — NO crypto.randomUUID fallback
//   - the loop workflow is registered on the runtime, idempotently (shared id)
//
// The engine-leg-context -> tool -> grant round-trip (S1/S2) is proven end to
// end against the REAL runtime + connector + grant provider in
// agent-gate-round-trip.test.ts.

import { Agent } from '@mastra/core/agent';
import {
  DurableAgent,
  type DurableAgenticWorkflowInput,
} from '@mastra/core/agent/durable';
import { EventEmitterPubSub } from '@mastra/core/events';
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
  overrides: { pubsub?: unknown; startResult?: unknown } = {},
) {
  const registered: string[] = [];
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
  const resume = vi.fn(async (_workflowId: string, runId: string) => ({
    runId,
    status: 'success' as const,
  }));
  const runtime = {
    register,
    workflowIds,
    start,
    resume,
    ...(overrides.pubsub !== undefined ? { pubsub: overrides.pubsub } : {}),
  } as unknown as RunnerRuntime;
  return { runtime, register, workflowIds, start, resume };
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
  runId: 'acme_run1',
  agentId: 'writer',
} as unknown as DurableAgenticWorkflowInput;

describe('createFlowsafeDurableAgent', () => {
  it('registers the durable-agentic-loop workflow on the runtime', () => {
    // #given a runtime with nothing registered
    const { runtime, register } = fakeRuntime();
    // #when a durable agent is created
    createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #then the shared loop workflow is registered exactly once
    expect(register).toHaveBeenCalledTimes(1);
    expect(register.mock.calls[0]?.[0]).toMatchObject({
      id: DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
    });
  });

  it('registers idempotently: a second agent on the same runtime does not re-register', () => {
    // #given two agents sharing one runtime (both compile to the same loop id)
    const { runtime, register } = fakeRuntime();
    // #when
    createFlowsafeDurableAgent({ agent: testAgent('a'), runtime });
    createFlowsafeDurableAgent({ agent: testAgent('b'), runtime });
    // #then register fires once, not twice ('duplicate workflow id' avoided)
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
    await drive(agent, 'acme_run1', INPUT);
    // #then it is routed through the runtime chokepoint, not createRun + start
    expect(start).toHaveBeenCalledTimes(1);
    expect(start).toHaveBeenCalledWith(DURABLE_AGENTIC_LOOP_WORKFLOW_ID, {
      runId: 'acme_run1',
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

// The public entry points a host actually calls. The inherited stream()/generate()
// take an OPTIONAL runId; without the override, core mints a tenant-less
// crypto.randomUUID() upstream of executeWorkflow's guard (INV-1 violation). These
// pin that the boundary override refuses an absent/non-INV-1 runId BEFORE any run.
describe('FlowsafeDurableAgent INV-1 boundary (stream/generate)', () => {
  it('stream() without a runId rejects (no crypto.randomUUID upstream)', async () => {
    // #given
    const { runtime, start } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    // #when / #then the idiomatic bare stream('...') Mastra documents is refused
    await expect(agent.stream('Hello!')).rejects.toBeInstanceOf(
      InvalidRunRequestError,
    );
    // never reached the runtime — no tenant-less run was started
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
// which mints a tenant-less crypto.randomUUID() AND registers a run under it when
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
    // #when / #then — the idiomatic bare prepare('...') would mint a tenant-less
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

  it('prepare() with a valid <tenantId>_<uuid> runId delegates to super.prepare', async () => {
    // #given a spied super.prepare so delegation is observable without core's
    // real preparation (model/tool resolution + registry writes)
    const { runtime } = fakeRuntime();
    const agent = createFlowsafeDurableAgent({ agent: testAgent(), runtime });
    const prepared = { runId: 'acme_run1' } as unknown as Awaited<
      ReturnType<DurableAgent['prepare']>
    >;
    const superPrepare = vi
      .spyOn(DurableAgent.prototype, 'prepare')
      .mockResolvedValue(prepared);
    // #when — a caller-minted INV-1 runId
    const result = await agent.prepare('Hello!', { runId: 'acme_run1' });
    // #then — the guard passed and the call reached super unchanged
    expect(superPrepare).toHaveBeenCalledTimes(1);
    expect(superPrepare).toHaveBeenCalledWith('Hello!', { runId: 'acme_run1' });
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
      runId: 'acme_run1',
      memory: { thread: 'acme_thread', resource: 'acme_resource' },
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
      runId: 'acme_run1',
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
      runId: 'acme_run1',
      untilIdle: false,
    } as never);

    expect(registerRun).toHaveBeenCalledTimes(1);
  });

  it('prepares, observes, registers, then resumes through RunnerRuntime', async () => {
    const order: string[] = [];
    const pubsub = new EventEmitterPubSub();
    const { runtime, resume } = fakeRuntime({ pubsub });
    resume.mockImplementation(async (_workflowId, runId) => {
      order.push('resume');
      return { runId, status: 'success' as const };
    });
    const registerRun = vi.fn(async () => {
      order.push('register');
    });
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      cache: false,
      threadRuntime: { registerRun } as never,
    });
    vi.spyOn(agent, 'prepare').mockImplementation(async () => {
      order.push('prepare');
      return {} as never;
    });
    vi.spyOn(agent, 'observe').mockImplementation(async () => {
      order.push('observe');
      return { output: { id: 'rehydrated' } } as never;
    });

    const summary = await agent.resumeViaRuntime({
      runId: 'acme_run1',
      step: ['tool-call'],
      resumeData: { approved: true },
      memory: { thread: 'acme_thread', resource: 'acme_resource' },
    });

    expect(order).toEqual(['prepare', 'observe', 'register', 'resume']);
    expect(resume).toHaveBeenCalledWith(
      DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
      'acme_run1',
      { step: ['tool-call'], resumeData: { approved: true } },
    );
    expect(summary).toMatchObject({ runId: 'acme_run1', status: 'success' });
  });

  it('publishes a registration failure and rethrows the original object', async () => {
    const { runtime, resume } = fakeRuntime();
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
    vi.spyOn(agent, 'prepare').mockResolvedValue({} as never);
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

    await expect(agent.resumeViaRuntime({ runId: 'acme_run1' })).rejects.toBe(
      original,
    );
    expect(emitError).toHaveBeenCalledWith('acme_run1', original);
    expect(resume).not.toHaveBeenCalled();
  });

  it('publishes a resume rejection and preserves it if publication also fails', async () => {
    const { runtime, resume } = fakeRuntime();
    const original = new Error('resume rejected');
    resume.mockRejectedValue(original);
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      threadRuntime: { registerRun: vi.fn(async () => undefined) } as never,
    });
    vi.spyOn(agent, 'prepare').mockResolvedValue({} as never);
    vi.spyOn(agent, 'observe').mockResolvedValue({
      output: { id: 'rehydrated' },
    } as never);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.spyOn(
      agent as unknown as {
        emitError: (id: string, error: Error) => Promise<void>;
      },
      'emitError',
    ).mockRejectedValue(new Error('publication failed'));

    await expect(agent.resumeViaRuntime({ runId: 'acme_run1' })).rejects.toBe(
      original,
    );
    expect(log).toHaveBeenCalledWith(
      expect.stringContaining('durable-agent-resume-error-publication-failed'),
    );
  });

  it('publishes a failed resume summary and returns it', async () => {
    const { runtime, resume } = fakeRuntime();
    resume.mockResolvedValue({
      runId: 'acme_run1',
      status: 'failed',
      error: 'resume failed',
    } as never);
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime,
      threadRuntime: { registerRun: vi.fn(async () => undefined) } as never,
    });
    vi.spyOn(agent, 'prepare').mockResolvedValue({} as never);
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

    const summary = await agent.resumeViaRuntime({ runId: 'acme_run1' });

    expect(summary).toMatchObject({ status: 'failed', error: 'resume failed' });
    expect(emitError.mock.calls[0]?.[1]?.message).toBe('resume failed');
  });
});

describe('FlowsafeDurableAgent.executeWorkflow failed run', () => {
  it('emits an error onto the stream when the run fails', async () => {
    // #given a runtime whose start() resolves to a failed run
    const { runtime } = fakeRuntime({
      startResult: { runId: 'acme_run1', status: 'failed', error: 'boom' },
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
    await drive(agent, 'acme_run1', INPUT);
    // #then the failed status is surfaced to observe()/onError via emitError
    expect(emitError).toHaveBeenCalledWith('acme_run1', expect.any(Error));
    expect(emitError.mock.calls[0]?.[1]?.message).toBe('boom');
  });
});
