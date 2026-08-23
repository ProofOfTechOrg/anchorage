// SPDX-License-Identifier: Apache-2.0
import { Agent } from '@mastra/core/agent';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { RunStateUnreadableError as BarrelRunStateUnreadableError } from './index.js';
import { init } from './init.js';
import { createHostPubSub } from './pubsub.js';
import {
  InvalidRunRequestError,
  type RequestContextProvider,
  RunAlreadyExistsError,
  type RunLeg,
  RunNotSuspendedError,
  type RunnerRuntime,
  RunStateUnreadableError,
  type RunSummary,
  UnknownWorkflowError,
} from './runtime.js';
import {
  isReadableRunSummary,
  isSuspensionTimeoutResumeData,
  MASTRA_WORKFLOW_META_KEY,
  SUSPENSION_DEADLINE_PAYLOAD_KEY,
  SUSPENSION_DEADLINE_PRINCIPAL_ID,
  type SuspensionDeadlineEntry,
  suspensionDeadlinesOf,
  suspensionTimeoutResumeData,
} from './suspension-deadline.js';

interface Counters {
  /** Times the approval step's post-approval body ran (the gated action). */
  approvalResumes: number;
  /** Times the echo step executed. */
  echoRuns: number;
}

function runtimeAgent(id: string): Agent {
  return new Agent({
    id,
    name: id,
    instructions: 'Test agent.',
    model: 'openai/gpt-4o-mini',
  });
}

// demo-approval: research -> approval (suspends; counts resumed executions).
// echo: single step, completes immediately (counts executions).
function buildRuntime(storage: InMemoryStore): {
  runtime: RunnerRuntime;
  counters: Counters;
} {
  const counters: Counters = { approvalResumes: 0, echoRuns: 0 };
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    { executionFence: 'none' },
  );

  const research = createStep({
    id: 'research',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string(), notes: z.string() }),
    execute: async ({ inputData }) => ({
      topic: inputData.topic,
      notes: `notes:${inputData.topic}`,
    }),
  });
  const approval = createStep({
    id: 'approval',
    inputSchema: z.object({ topic: z.string(), notes: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      notes: z.string(),
      approvedBy: z.string(),
    }),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({ approvedBy: z.string() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'human approval required' });
      counters.approvalResumes += 1;
      return { ...inputData, approvedBy: resumeData.approvedBy };
    },
  });
  createWorkflow({
    id: 'demo-approval',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      notes: z.string(),
      approvedBy: z.string(),
    }),
  })
    .then(research)
    .then(approval)
    .commit();

  const echo = createStep({
    id: 'echo',
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
    execute: async ({ inputData }) => {
      counters.echoRuns += 1;
      return inputData;
    },
  });
  createWorkflow({
    id: 'echo',
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
  })
    .then(echo)
    .commit();

  return { runtime, counters };
}

describe('RunnerRuntime host pubsub identity', () => {
  it('threads the pubsub instance from init() through to runtime.pubsub', () => {
    // #given — a host builds ONE pubsub identity for its DO
    const pubsub = createHostPubSub();

    // #when — init() threads it (InitOptions.pubsub -> RunnerRuntimeOptions.pubsub)
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { pubsub, executionFence: 'none' },
    );

    // #then — the SAME instance is reachable, so Track A's createRun sites and
    // observe() replay share one feed. Delete the thread in init.ts and this
    // fails: runtime.pubsub is undefined and the two createRun sites would each
    // let core default a separate emitter — the DL-001 bug this seam prevents.
    expect(runtime.pubsub).toBe(pubsub);
  });

  it('leaves runtime.pubsub undefined when the host configures none (byte-identical)', () => {
    // #when — no pubsub passed
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );

    // #then — undefined, the polling-fallback posture existing hosts keep
    expect(runtime.pubsub).toBeUndefined();
  });
});

describe('RunnerRuntime', () => {
  it('passes initial workflow state through to core execution', async () => {
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const stateSchema = z.object({ seed: z.string() });
    const inspect = createStep({
      id: 'inspect-state',
      stateSchema,
      inputSchema: z.object({}),
      outputSchema: z.object({ seed: z.string() }),
      execute: async ({ state }) => ({ seed: state.seed }),
    });
    createWorkflow({
      id: 'stateful',
      stateSchema,
      inputSchema: z.object({}),
      outputSchema: z.object({ seed: z.string() }),
    })
      .then(inspect)
      .commit();

    await expect(
      runtime.start('stateful', {
        runId: 'scheduled-state',
        inputData: {},
        initialState: { seed: 'from-schedule' },
      }),
    ).resolves.toMatchObject({
      status: 'success',
      result: { seed: 'from-schedule' },
    });
  });

  it('rejects numeric run and start-attempt ids without RegExp coercion', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: 123 as unknown as string,
        inputData: { value: 'x' },
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    await expect(
      runtime.start('echo', {
        runId: 'run-1',
        inputData: { value: 'x' },
        attemptToken: 123 as unknown as string,
      }),
    ).rejects.toThrow('attemptToken is malformed');
  });

  it('accepts a requester id exactly at the principal-id bound', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const requestedBy = 'r'.repeat(200);

    await expect(
      runtime.start('echo', {
        runId: 'bounded-requester',
        inputData: { value: 'x' },
        requestedBy,
        requestedByKind: 'human',
      }),
    ).resolves.toMatchObject({ requestedBy, requestedByKind: 'human' });
  });

  it.each([
    ['an overlong requester', 'r'.repeat(201)],
    ['an all-whitespace requester', ' '.repeat(200)],
    ['a control-bearing requester', 'reviewer\u000aforged'],
  ])('rejects %s on start', async (_label, requestedBy) => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: 'invalid-requester',
        inputData: { value: 'x' },
        requestedBy,
        requestedByKind: 'human',
      }),
    ).rejects.toThrow('requestedBy is malformed');
  });

  it.each([
    'human',
    'service',
    'agent',
    'system',
  ] as const)("accepts the '%s' requester kind", async (requestedByKind) => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: `requester-kind-${requestedByKind}`,
        inputData: { value: 'x' },
        requestedBy: 'requester',
        requestedByKind,
      }),
    ).resolves.toMatchObject({ requestedByKind });
  });

  it.each([
    'operator',
    '',
    null,
    1,
  ])('rejects an invalid requester kind (%s)', async (requestedByKind) => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: 'invalid-requester-kind',
        inputData: { value: 'x' },
        requestedBy: 'requester',
        requestedByKind: requestedByKind as never,
      }),
    ).rejects.toThrow('requestedByKind is malformed');
  });

  it.each([
    ['requestedBy without requestedByKind', { requestedBy: 'requester' }],
    ['requestedByKind without requestedBy', { requestedByKind: 'human' }],
  ])('rejects %s on start', async (_label, requester) => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: 'half-attributed-start',
        inputData: { value: 'x' },
        ...requester,
      } as never),
    ).rejects.toThrow(
      'requestedBy and requestedByKind must be provided together',
    );
  });

  it.each([
    ['an overlong requester', 'r'.repeat(201)],
    ['an all-whitespace requester', ' '.repeat(200)],
    ['a control-bearing requester', 'reviewer\u007fforged'],
  ])('rejects %s on resume', async (_label, requestedBy) => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'invalid-resume-requester',
      inputData: { topic: 'launch' },
      requestedBy: 'initiator',
      requestedByKind: 'human',
    });

    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'alice' },
        requestedBy,
        requestedByKind: 'human',
      }),
    ).rejects.toThrow('requestedBy is malformed');
  });

  it.each([
    ['requestedBy without requestedByKind', { requestedBy: 'requester' }],
    ['requestedByKind without requestedBy', { requestedByKind: 'human' }],
  ])('rejects %s on resume', async (_label, requester) => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'half-attributed-resume',
      inputData: { topic: 'launch' },
    });

    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'alice' },
        ...requester,
      } as never),
    ).rejects.toThrow(
      'requestedBy and requestedByKind must be provided together',
    );
  });

  it('inherits a complete stored requester pair when a resume supplies neither', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'inherited-requester-pair',
      inputData: { topic: 'launch' },
      requestedBy: 'initiator',
      requestedByKind: 'service',
    });

    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'alice' },
      }),
    ).resolves.toMatchObject({
      requestedBy: 'initiator',
      requestedByKind: 'service',
    });
  });

  it('rejects a numeric recovery token without RegExp coercion', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.recoverStartAttempt('echo', 'run-1', 123 as unknown as string),
    ).rejects.toThrow('attemptToken is malformed');
  });

  it('runs a workflow to suspension and resumes it to success', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'launch' },
    });

    // #then
    expect(started.status).toBe('suspended');
    expect(started.suspended).toEqual([['approval']]);
    // suspendPayload is keyed by suspended step id
    expect(started.suspendPayload).toEqual({
      approval: { reason: 'human approval required' },
    });

    // #when
    const resumed = await runtime.resume('demo-approval', started.runId, {
      step: 'approval',
      resumeData: { approvedBy: 'alice' },
    });

    // #then
    expect(resumed.status).toBe('success');
    expect(resumed.result).toEqual({
      topic: 'launch',
      notes: 'notes:launch',
      approvedBy: 'alice',
    });
  });

  it('resumes in a fresh runtime sharing only storage — the restart simulation', async () => {
    // #given — a run suspended by one runtime
    const storage = new InMemoryStore();
    const before = buildRuntime(storage).runtime;
    const started = await before.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'launch' },
    });
    expect(started.status).toBe('suspended');

    // #when — a fresh runtime (fresh Mastra instance and run registry, as
    // after a Worker restart; only the storage handle is shared) resumes it
    const after = buildRuntime(storage).runtime;
    const observed = await after.status('demo-approval', started.runId);
    const resumed = await after.resume('demo-approval', started.runId, {
      step: 'approval',
      resumeData: { approvedBy: 'alice' },
    });

    // #then
    expect(observed).toMatchObject({ status: 'suspended' });
    expect(resumed.status).toBe('success');
    expect(resumed.result).toMatchObject({
      approvedBy: 'alice',
      topic: 'launch',
    });
  });

  it('executes the gated step exactly once under concurrent resume', async () => {
    // #given — a suspended approval run
    const { runtime, counters } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'race' },
    });
    expect(started.status).toBe('suspended');

    // #when — two racing approvals for the same run
    const outcomes = await Promise.allSettled([
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'alice' },
      }),
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'bob' },
      }),
    ]);

    // #then — exactly one wins; the loser gets RunNotSuspendedError; the
    // gated action ran once. This is the product's core guarantee.
    const fulfilled = outcomes.filter((o) => o.status === 'fulfilled');
    const rejected = outcomes.filter((o) => o.status === 'rejected');
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(
      RunNotSuspendedError,
    );
    expect(counters.approvalResumes).toBe(1);
  });

  it('executes a caller-keyed run exactly once under concurrent start', async () => {
    // #given
    const { runtime, counters } = buildRuntime(new InMemoryStore());

    // #when — two racing starts sharing a caller-supplied runId
    const outcomes = await Promise.allSettled([
      runtime.start('echo', { runId: 'shared', inputData: { value: 'a' } }),
      runtime.start('echo', { runId: 'shared', inputData: { value: 'b' } }),
    ]);

    // #then — one executes, the other is rejected as already existing
    expect(outcomes.filter((o) => o.status === 'fulfilled')).toHaveLength(1);
    const rejection = outcomes.find((o) => o.status === 'rejected');
    expect((rejection as PromiseRejectedResult).reason).toBeInstanceOf(
      RunAlreadyExistsError,
    );
    expect(counters.echoRuns).toBe(1);
  });

  it('rejects a sequential start that reuses an existing runId', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    await runtime.start('echo', { runId: 'r1', inputData: { value: 'x' } });

    // #when / #then
    await expect(
      runtime.start('echo', { runId: 'r1', inputData: { value: 'y' } }),
    ).rejects.toBeInstanceOf(RunAlreadyExistsError);
  });

  it('completes non-suspending workflows and rejects resuming them', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when
    const done = await runtime.start('echo', {
      runId: crypto.randomUUID(),
      inputData: { value: 'hi' },
    });

    // #then
    expect(done.status).toBe('success');
    expect(done.result).toEqual({ value: 'hi' });
    await expect(
      runtime.resume('echo', done.runId, { resumeData: {} }),
    ).rejects.toBeInstanceOf(RunNotSuspendedError);
  });

  it('classifies resume-schema violations as InvalidRunRequestError and keeps the run resumable', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'validate' },
    });

    // #when / #then — bad resumeData is a client error, not a server fault
    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 123 },
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);

    // #then — the run is still suspended and a valid resume completes it
    expect(await runtime.status('demo-approval', started.runId)).toMatchObject({
      status: 'suspended',
    });
    const resumed = await runtime.resume('demo-approval', started.runId, {
      step: 'approval',
      resumeData: { approvedBy: 'carol' },
    });
    expect(resumed.status).toBe('success');
  });

  it('classifies a resume targeting a non-suspended step as InvalidRunRequestError', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'wrong-step' },
    });

    // #when / #then
    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'research',
        resumeData: { approvedBy: 'dave' },
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
  });

  it('honors caller-provided runIds', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when
    const started = await runtime.start('echo', {
      runId: 'fixed-run-id',
      inputData: { value: 'x' },
    });

    // #then
    expect(started.runId).toBe('fixed-run-id');
  });

  it('throws UnknownWorkflowError for unregistered workflows', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when / #then
    await expect(
      runtime.start('nope', { runId: 'r-unknown' }),
    ).rejects.toBeInstanceOf(UnknownWorkflowError);
  });

  it('returns null status for unknown runs', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when / #then
    expect(await runtime.status('echo', 'missing-run')).toBeNull();
  });

  it('lists registered workflow ids', () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when / #then
    expect(runtime.workflowIds().sort()).toEqual(['demo-approval', 'echo']);
  });

  it('rejects duplicate workflow ids at registration', () => {
    // #given
    const { createWorkflow } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    createWorkflow({
      id: 'wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    });

    // #when / #then
    expect(() =>
      createWorkflow({
        id: 'wf',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      }),
    ).toThrowError(/duplicate workflow id/);
  });

  it('rejects duplicate agent ids at registration', () => {
    // #given
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    runtime.registerAgent(runtimeAgent('writer'));

    // #when / #then
    expect(() => runtime.registerAgent(runtimeAgent('writer'))).toThrowError(
      /duplicate agent id/,
    );
  });

  it('exposes every registered agent to workflow execution', async () => {
    // #given
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const agents = [
      runtimeAgent('a'),
      runtimeAgent('b'),
      runtimeAgent('__proto__'),
      runtimeAgent('runtime-agent:__proto__'),
    ];
    for (const agent of agents) runtime.registerAgent(agent);
    const resolveAgents = createStep({
      id: 'resolve-agents',
      inputSchema: z.object({}),
      outputSchema: z.object({
        firstByKey: z.string(),
        secondById: z.string(),
        collisionById: z.string(),
        prefixedByKey: z.string(),
        workflowByKey: z.string(),
      }),
      execute: async ({ mastra }) => ({
        firstByKey: mastra.getAgent('a').id,
        secondById: mastra.getAgentById('b').id,
        collisionById: mastra.getAgentById('__proto__').id,
        prefixedByKey: mastra.getAgent('runtime-agent:__proto__').id,
        workflowByKey: mastra.getWorkflow('agent-resolution').id,
      }),
    });
    createWorkflow({
      id: 'agent-resolution',
      inputSchema: z.object({}),
      outputSchema: z.object({
        firstByKey: z.string(),
        secondById: z.string(),
        collisionById: z.string(),
        prefixedByKey: z.string(),
        workflowByKey: z.string(),
      }),
    })
      .then(resolveAgents)
      .commit();

    // #when
    const summary = await runtime.start('agent-resolution', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then
    expect(summary).toMatchObject({
      status: 'success',
      result: {
        firstByKey: 'a',
        secondById: 'b',
        collisionById: '__proto__',
        prefixedByKey: 'runtime-agent:__proto__',
        workflowByKey: 'agent-resolution',
      },
    });
  });

  it('passes the runtime pubsub to registered agents', async () => {
    // #given
    const pubsub = createHostPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { pubsub, executionFence: 'none' },
    );
    const agent = runtimeAgent('writer');
    runtime.registerAgent(agent);
    const noop = createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    createWorkflow({
      id: 'pubsub-agent',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(noop)
      .commit();
    await runtime.start('pubsub-agent', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    publish.mockClear();

    // #when
    const agentPubsub = agent.getPubSub();
    expect(agentPubsub).toBeDefined();
    await agentPubsub?.publish('runtime-agent-test', {
      type: 'runtime-agent-test',
      data: agent.id,
      runId: 'runtime-agent-test',
    });

    // #then
    expect(publish).toHaveBeenCalledOnce();
  });

  it.each([
    '__proto__',
    'constructor',
  ])("executes prototype-collision workflow id '%s'", async (workflowId) => {
    // #given
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const noop = createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({ resolved: z.string() }),
      execute: async ({ mastra }) => ({
        resolved: mastra.getWorkflowById(workflowId).id,
      }),
    });
    createWorkflow({
      id: workflowId,
      inputSchema: z.object({}),
      outputSchema: z.object({ resolved: z.string() }),
    })
      .then(noop)
      .commit();

    // #when
    const summary = await runtime.start(workflowId, {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then
    expect(summary).toMatchObject({
      status: 'success',
      result: { resolved: workflowId },
    });
  });

  it.each([
    'team:wf',
    'a/b',
    '.',
    '..',
    '',
  ])("rejects non-path-safe workflow id '%s' at registration", (id) => {
    // #given
    const { createWorkflow } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );

    // #when / #then — a ':' or '/' in the id would make the DO name join
    // (`${workflowId}:${runId}`) and the /runs/:workflowId/:runId path
    // ambiguous; fail at register(), before any run can be minted under it
    expect(() =>
      createWorkflow({
        id,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      }),
    ).toThrowError(/must be URL-path-safe/);
  });

  it('accepts path-safe workflow ids at registration', () => {
    // #given
    const { createWorkflow, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );

    // #when — unreserved-character id, including the '.' that only the bare
    // dot-segments '.' and '..' are barred from
    createWorkflow({
      id: 'demo-approval.v2_~ok',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    });

    // #then
    expect(runtime.workflowIds()).toContain('demo-approval.v2_~ok');
  });

  it('rejects registration after the first run', async () => {
    // #given
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const step = createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    createWorkflow({
      id: 'wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(step)
      .commit();
    await runtime.start('wf', { runId: crypto.randomUUID(), inputData: {} });

    // #when / #then
    expect(() =>
      createWorkflow({
        id: 'late',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      }),
    ).toThrowError(/before the first run/);
    expect(() => runtime.registerAgent(runtimeAgent('late'))).toThrowError(
      /before the first run/,
    );
  });
});

const ISO_8601 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

describe('RunnerRuntime.status projection', () => {
  it('persists a terminal status when a workflow retains only resume snapshots', async () => {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { executionFence: 'none' },
    );
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async ({ inputData, resumeData, suspend }) =>
        resumeData?.approved
          ? inputData
          : suspend({ reason: 'approval required' }),
    });
    createWorkflow({
      id: 'resume-artifact-only',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      options: {
        shouldPersistSnapshot: ({ workflowStatus }) =>
          workflowStatus === 'pending' || workflowStatus === 'suspended',
      },
    })
      .then(gate)
      .commit();
    const started = await runtime.start('resume-artifact-only', {
      runId: 'acme_terminal-status',
      inputData: { value: 'durable' },
    });

    const resumed = await runtime.resume(
      'resume-artifact-only',
      started.runId,
      {
        resumeData: { approved: true },
      },
    );
    const status = await runtime.status('resume-artifact-only', started.runId);

    expect(resumed).toMatchObject({
      status: 'success',
      result: { value: 'durable' },
    });
    expect(status).toMatchObject({
      status: 'success',
      result: { value: 'durable' },
    });
  });

  it('persists a terminal start when the engine omits terminal snapshots', async () => {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { executionFence: 'none' },
    );
    const echo = createStep({
      id: 'echo-once',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ inputData }) => inputData,
    });
    createWorkflow({
      id: 'start-artifact-only',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      options: {
        shouldPersistSnapshot: ({ workflowStatus }) =>
          workflowStatus === 'pending',
      },
    })
      .then(echo)
      .commit();

    const done = await runtime.start('start-artifact-only', {
      runId: 'acme_terminal-start',
      inputData: { value: 'durable' },
    });
    const status = await runtime.status('start-artifact-only', done.runId);

    expect(done).toMatchObject({
      status: 'success',
      result: { value: 'durable' },
    });
    expect(status).toMatchObject({
      status: 'success',
      result: { value: 'durable' },
    });
  });

  it('projects suspended detail — paths, payload, timestamps — from the snapshot', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'launch' },
    });
    expect(started.status).toBe('suspended');

    // #when
    const status = await runtime.status('demo-approval', started.runId);

    // #then
    expect(status).toMatchObject({
      status: 'suspended',
      suspended: [['approval']],
      suspendPayload: { approval: { reason: 'human approval required' } },
    });
    expect(status?.createdAt).toMatch(ISO_8601);
    expect(status?.updatedAt).toMatch(ISO_8601);
  });

  it('projects the result for a completed run', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const done = await runtime.start('echo', {
      runId: crypto.randomUUID(),
      inputData: { value: 'hi' },
    });

    // #when
    const status = await runtime.status('echo', done.runId);

    // #then
    expect(status).toMatchObject({
      status: 'success',
      result: { value: 'hi' },
    });
  });

  it('projects the failure message for a failed run', async () => {
    // #given
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const boom = createStep({
      id: 'boom',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => {
        throw new Error('boom');
      },
    });
    createWorkflow({
      id: 'failing',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(boom)
      .commit();
    const started = await runtime.start('failing', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('failed');

    // #when
    const status = await runtime.status('failing', started.runId);

    // #then
    expect(status?.status).toBe('failed');
    expect(status?.error).toContain('boom');
  });

  it('extracts the message from a thrown non-Error object', async () => {
    // #given — a step that throws a serialized-style { name, message, stack }
    // object, not an Error instance. This is the shape errorText() defends
    // against at an engine/persistence boundary; String() on it reads
    // '[object Object]', so a naive projection would lose the message.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const boom = createStep({
      id: 'boom-object',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => {
        throw {
          name: 'WeirdError',
          message: 'object-shaped failure',
          stack: 'x',
        };
      },
    });
    createWorkflow({
      id: 'obj-failing',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(boom)
      .commit();

    // #when
    const started = await runtime.start('obj-failing', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then — both the run summary and the status projection surface the
    // message field, never '[object Object]'
    expect(started.status).toBe('failed');
    expect(started.error).toBe('object-shaped failure');
    expect((await runtime.status('obj-failing', started.runId))?.error).toBe(
      'object-shaped failure',
    );
  });

  it('projects every branch of a multi-step (parallel) suspension', async () => {
    // #given — two parallel steps that both suspend in the same run
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const makeGate = (id: string, reason: string) =>
      createStep({
        id,
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
        suspendSchema: z.object({ reason: z.string() }),
        resumeSchema: z.object({ go: z.boolean() }),
        execute: async ({ resumeData, suspend }) =>
          resumeData ? { ok: true } : suspend({ reason }),
      });
    createWorkflow({
      id: 'parallel-suspend',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .parallel([makeGate('gateA', 'A waits'), makeGate('gateB', 'B waits')])
      .commit();
    const started = await runtime.start('parallel-suspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('suspended');

    // #when
    const status = await runtime.status('parallel-suspend', started.runId);

    // #then — both suspended paths and both keyed suspend payloads project
    expect(status?.suspended).toHaveLength(2);
    expect(status?.suspended).toContainEqual(['gateA']);
    expect(status?.suspended).toContainEqual(['gateB']);
    expect(status?.suspendPayload).toEqual({
      gateA: { reason: 'A waits' },
      gateB: { reason: 'B waits' },
    });
  });
});

describe('RunnerRuntime run lifecycle', () => {
  const principal = { kind: 'human' as const, id: 'operator-1' };

  function heldRuntime(
    options: {
      suspendFirst?: boolean;
      storage?: InMemoryStore;
      cancelFailures?: number;
      cancelNoop?: boolean;
    } = {},
  ): {
    runtime: RunnerRuntime;
    entered: Promise<void>;
    release: () => void;
    completed: () => boolean;
  } {
    let enter!: () => void;
    let release!: () => void;
    let completed = false;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { createWorkflow, createStep, runtime } = init(
      { storage: options.storage ?? new InMemoryStore() },
      { executionFence: 'none' },
    );
    const step = createStep({
      id: 'held',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend, abortSignal }) => {
        if (options.suspendFirst && !resumeData) {
          return suspend({ reason: 'resume to enter held work' });
        }
        enter();
        await Promise.race([
          held,
          new Promise<never>((_resolve, reject) => {
            abortSignal.addEventListener(
              'abort',
              () => reject(new Error('held work aborted')),
              { once: true },
            );
          }),
        ]);
        completed = true;
        return { ok: true };
      },
    });
    const workflow = createWorkflow({
      id: 'held-workflow',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
    })
      .then(step)
      .commit();
    if ((options.cancelFailures ?? 0) > 0 || options.cancelNoop) {
      let remaining = options.cancelFailures ?? 0;
      const originalCreateRun = workflow.createRun.bind(workflow);
      Object.defineProperty(workflow, 'createRun', {
        configurable: true,
        value: async (...args: Parameters<typeof workflow.createRun>) => {
          const run = await originalCreateRun(...args);
          const originalCancel = run.cancel.bind(run);
          Object.defineProperty(run, 'cancel', {
            configurable: true,
            value: async () => {
              if (options.cancelNoop) return;
              if (remaining > 0) {
                remaining -= 1;
                throw new Error('injected cancel failure');
              }
              await originalCancel();
            },
          });
          return run;
        },
      });
    }
    return { runtime, entered, release, completed: () => completed };
  }

  it('keeps ordinary start and resume snapshots lifecycle-free', async () => {
    const storage = new InMemoryStore();
    const { runtime } = buildRuntime(storage);
    const started = await runtime.start('demo-approval', {
      runId: 'lazy-lifecycle',
      inputData: { topic: 'plain' },
    });
    expect(started.status).toBe('suspended');

    const workflows = await storage.getStore('workflows');
    const before = await workflows?.loadWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: started.runId,
    });
    expect(before?.requestContext).not.toHaveProperty('flowsafe.runLifecycle');

    await runtime.resume('demo-approval', started.runId, {
      resumeData: { approvedBy: 'reviewer-1' },
    });
    const after = await workflows?.loadWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: started.runId,
    });
    expect(after?.requestContext).not.toHaveProperty('flowsafe.runLifecycle');
  });

  it('rejects a provider-forged lifecycle projection on start and resume', async () => {
    const now = vi.spyOn(Date, 'now');
    try {
      now.mockReturnValue(40_000);
      const storage = new InMemoryStore();
      const { createWorkflow, createStep, runtime } = init(
        { storage },
        {
          executionFence: 'none',
          requestContextForRun: () => ({
            'flowsafe.runLifecycle': {
              version: 1,
              revision: 999,
              deadlineAt: 1,
              economicOperations: [
                { id: 'forged', settlementState: 'disputed' },
              ],
              terminal: {
                status: 'cancelled',
                transitionedAt: 1,
                replayPrincipals: [{ kind: 'human', id: 'forged' }],
                error: { code: 'CANCELLED', message: 'forged' },
              },
            },
          }),
        },
      );
      const gate = createStep({
        id: 'gate',
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
        suspendSchema: z.object({ reason: z.string() }),
        execute: async ({ resumeData, suspend }) =>
          resumeData ? { ok: true } : suspend({ reason: 'wait' }),
      });
      createWorkflow({
        id: 'provider-lifecycle-fence',
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
      })
        .then(gate)
        .commit();

      const started = await runtime.start('provider-lifecycle-fence', {
        runId: 'provider-lifecycle-fence-run',
        inputData: {},
        deadlineMs: 100,
      });
      expect(started).toMatchObject({
        status: 'suspended',
        deadlineAt: 40_100,
      });
      now.mockReturnValue(40_050);
      const resumed = await runtime.resume(
        'provider-lifecycle-fence',
        started.runId,
        { resumeData: { approved: true }, deadlineMs: 1_000 },
      );
      expect(resumed).toMatchObject({
        status: 'success',
        deadlineAt: 41_050,
      });
      const workflows = await storage.getStore('workflows');
      const snapshot = await workflows?.loadWorkflowSnapshot({
        workflowName: 'provider-lifecycle-fence',
        runId: started.runId,
      });
      expect(snapshot?.requestContext?.['flowsafe.runLifecycle']).toMatchObject(
        {
          revision: 2,
          deadlineAt: 41_050,
        },
      );
      expect(
        snapshot?.requestContext?.['flowsafe.runLifecycle'],
      ).not.toHaveProperty('terminal');
      expect(
        snapshot?.requestContext?.['flowsafe.runLifecycle'],
      ).not.toHaveProperty('economicOperations');
    } finally {
      now.mockRestore();
    }
  });

  it('terminates a suspension exactly once with the stable structured shape', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'terminate-suspended',
      inputData: { topic: 'cancel' },
      requestedBy: principal.id,
      requestedByKind: principal.kind,
      deadlineMs: 60_000,
    });

    const first = await runtime.terminateAsPrincipal(
      'demo-approval',
      started.runId,
      principal,
      principal,
    );
    const second = await runtime.terminateAsPrincipal(
      'demo-approval',
      started.runId,
      principal,
      principal,
    );

    expect(first.transitioned).toBe(true);
    expect(second.transitioned).toBe(false);
    expect(second.summary).toEqual(first.summary);
    expect(first.summary).toMatchObject({
      runId: 'terminate-suspended',
      status: 'cancelled',
      requestedBy: principal.id,
      requestedByKind: principal.kind,
      deadlineAt: expect.any(Number),
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    });
    expect(first.summary).not.toHaveProperty('error');
  });

  it('refuses a persisted disputed settlement before cancellation', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'disputed-run',
      inputData: { topic: 'held' },
      economicOperations: [{ id: 'charge-1', settlementState: 'disputed' }],
    });

    await expect(
      runtime.cancelActiveExecution(
        'demo-approval',
        started.runId,
        'cancelled',
        [principal],
      ),
    ).rejects.toMatchObject({
      reason: {
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      },
    });
    await expect(
      runtime.terminateAsPrincipal(
        'demo-approval',
        started.runId,
        principal,
        principal,
      ),
    ).rejects.toMatchObject({
      reason: { code: 'DISPUTED_SETTLEMENT' },
    });
    await expect(
      runtime.status('demo-approval', started.runId),
    ).resolves.toMatchObject({ status: 'suspended' });
  });

  it('aborts a live running step before persisting the cancelled terminal', async () => {
    const { runtime, entered, completed } = heldRuntime();
    const starting = runtime.start('held-workflow', {
      runId: 'held-cancel',
      inputData: {},
      requestedBy: principal.id,
      requestedByKind: principal.kind,
    });
    await entered;

    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'held-cancel',
        'cancelled',
        [principal],
      ),
    ).resolves.toBe(true);
    await expect(starting).resolves.toMatchObject({ status: 'canceled' });
    await expect(
      runtime.terminateAsPrincipal(
        'held-workflow',
        'held-cancel',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: { status: 'cancelled', errorEnvelope: { code: 'CANCELLED' } },
    });
    expect(completed()).toBe(false);
    await expect(
      runtime.status('held-workflow', 'held-cancel'),
    ).resolves.toMatchObject({ status: 'cancelled' });
  });

  it('retries a live abort when the durable intent survived a transient cancel failure', async () => {
    const { runtime, entered, completed } = heldRuntime({
      cancelFailures: 1,
    });
    const starting = runtime.start('held-workflow', {
      runId: 'held-cancel-retry',
      inputData: {},
    });
    await entered;

    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'held-cancel-retry',
        'cancelled',
        [principal],
      ),
    ).rejects.toThrow('injected cancel failure');
    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'held-cancel-retry',
        'cancelled',
        [principal],
      ),
    ).resolves.toBe(true);
    await expect(starting).resolves.toMatchObject({ status: 'canceled' });
    await expect(
      runtime.terminateAsPrincipal(
        'held-workflow',
        'held-cancel-retry',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      summary: { status: 'cancelled' },
      transitioned: true,
    });
    expect(completed()).toBe(false);
  });

  it('does not abort a live running step initialized with a disputed settlement', async () => {
    const { runtime, entered, release, completed } = heldRuntime();
    const starting = runtime.start('held-workflow', {
      runId: 'held-disputed',
      inputData: {},
      economicOperations: [{ id: 'charge-live', settlementState: 'disputed' }],
    });
    await entered;

    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'held-disputed',
        'cancelled',
        [principal],
      ),
    ).rejects.toMatchObject({
      reason: { code: 'DISPUTED_SETTLEMENT' },
    });
    release();
    await expect(starting).resolves.toMatchObject({ status: 'success' });
    expect(completed()).toBe(true);
  });

  it('extends a persisted deadline on resume and times out only once', async () => {
    const now = vi.spyOn(Date, 'now');
    now.mockReturnValue(10_000);
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'deadline-resume',
      inputData: { topic: 'deadline' },
      deadlineMs: 100,
    });
    expect(started.deadlineAt).toBe(10_100);

    now.mockReturnValue(10_050);
    const resumed = await runtime.resume('demo-approval', started.runId, {
      resumeData: { approvedBy: 'reviewer-1' },
      deadlineMs: 1_000,
    });
    expect(resumed.deadlineAt).toBe(11_050);

    const timeoutRun = await runtime.start('demo-approval', {
      runId: 'deadline-once',
      inputData: { topic: 'timeout' },
      deadlineMs: 0,
    });
    const first = await runtime.timeOut(
      'demo-approval',
      timeoutRun.runId,
      { expectedRevision: 1, expectedDeadlineAt: 10_050 },
      10_050,
    );
    const second = await runtime.timeOut(
      'demo-approval',
      timeoutRun.runId,
      { expectedRevision: 1, expectedDeadlineAt: 10_050 },
      10_051,
    );
    expect(first.transitioned).toBe(true);
    expect(second.transitioned).toBe(false);
    expect(second.summary).toEqual(first.summary);
    expect(first.summary).toMatchObject({
      status: 'timed_out',
      errorEnvelope: { code: 'TIMED_OUT', message: 'run deadline expired' },
    });
    now.mockRestore();
  });

  it('rejects a stale deadline CAS while an extended resume leg is active', async () => {
    const now = vi.spyOn(Date, 'now');
    try {
      now.mockReturnValue(20_000);
      const { runtime, entered, release, completed } = heldRuntime({
        suspendFirst: true,
      });
      const started = await runtime.start('held-workflow', {
        runId: 'held-resume-extension',
        inputData: {},
        deadlineMs: 100,
      });
      expect(started).toMatchObject({
        status: 'suspended',
        deadlineAt: 20_100,
      });

      now.mockReturnValue(20_050);
      const resuming = runtime.resume(
        'held-workflow',
        'held-resume-extension',
        { resumeData: { approved: true }, deadlineMs: 1_000 },
      );
      await entered;
      await expect(
        runtime.cancelActiveExecution(
          'held-workflow',
          'held-resume-extension',
          'timed_out',
          [principal],
          { expectedRevision: 1, expectedDeadlineAt: 20_100 },
          20_100,
        ),
      ).resolves.toBe(false);

      release();
      await expect(resuming).resolves.toMatchObject({
        status: 'success',
        deadlineAt: 21_050,
      });
      expect(completed()).toBe(true);
    } finally {
      now.mockRestore();
    }
  });

  it('preserves an active resume extension and cancellation intent through fresh-runtime recovery', async () => {
    const now = vi.spyOn(Date, 'now');
    try {
      const storage = new InMemoryStore();
      now.mockReturnValue(30_000);
      const { runtime, entered } = heldRuntime({
        suspendFirst: true,
        storage,
      });
      await runtime.start('held-workflow', {
        runId: 'held-resume-cancel-recovery',
        inputData: {},
        deadlineMs: 100,
      });

      now.mockReturnValue(30_050);
      const resuming = runtime.resume(
        'held-workflow',
        'held-resume-cancel-recovery',
        {
          resumeData: { approved: true },
          deadlineMs: 1_000,
          economicOperations: [
            { id: 'charge-resume', settlementState: 'settled' },
          ],
        },
      );
      await entered;
      await expect(
        runtime.cancelActiveExecution(
          'held-workflow',
          'held-resume-cancel-recovery',
          'cancelled',
          [principal],
        ),
      ).resolves.toBe(true);
      await expect(resuming).resolves.toMatchObject({ status: 'canceled' });

      const workflows = await storage.getStore('workflows');
      const precursor = await workflows?.loadWorkflowSnapshot({
        workflowName: 'held-workflow',
        runId: 'held-resume-cancel-recovery',
      });
      expect(
        precursor?.requestContext?.['flowsafe.runLifecycle'],
      ).toMatchObject({
        revision: 3,
        deadlineAt: 31_050,
        economicOperations: [
          { id: 'charge-resume', settlementState: 'settled' },
        ],
        transitionIntent: { status: 'cancelled' },
      });

      const fresh = heldRuntime({ suspendFirst: true, storage }).runtime;
      await expect(
        fresh.terminateAsPrincipal(
          'held-workflow',
          'held-resume-cancel-recovery',
          principal,
          principal,
        ),
      ).resolves.toMatchObject({
        transitioned: true,
        summary: {
          status: 'cancelled',
          deadlineAt: 31_050,
          errorEnvelope: { code: 'CANCELLED' },
        },
      });
      const terminal = await workflows?.loadWorkflowSnapshot({
        workflowName: 'held-workflow',
        runId: 'held-resume-cancel-recovery',
      });
      expect(terminal?.requestContext?.['flowsafe.runLifecycle']).toMatchObject(
        {
          economicOperations: [
            { id: 'charge-resume', settlementState: 'settled' },
          ],
          terminal: { status: 'cancelled' },
        },
      );
    } finally {
      now.mockRestore();
    }
  });

  it('fences a resume that is still preparing when termination begins', async () => {
    const storage = new InMemoryStore();
    const { runtime, completed } = heldRuntime({
      suspendFirst: true,
      storage,
    });
    await runtime.start('held-workflow', {
      runId: 'resume-preparation-terminate',
      inputData: {},
      deadlineMs: 100,
    });
    let preparationEntered!: () => void;
    let releasePreparation!: () => void;
    const entered = new Promise<void>((resolve) => {
      preparationEntered = resolve;
    });
    const preparation = new Promise<void>((resolve) => {
      releasePreparation = resolve;
    });

    const resuming = runtime.resume(
      'held-workflow',
      'resume-preparation-terminate',
      {
        resumeData: { approved: true },
        deadlineMs: 1_000,
        prepareExecution: async () => {
          preparationEntered();
          await preparation;
        },
      },
    );
    await entered;

    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'resume-preparation-terminate',
        'cancelled',
        [principal],
      ),
    ).resolves.toBe(false);
    releasePreparation();
    await expect(resuming).rejects.toMatchObject({
      name: 'RunTerminalConflictError',
    });
    await expect(
      runtime.terminateAsPrincipal(
        'held-workflow',
        'resume-preparation-terminate',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: {
        status: 'cancelled',
        deadlineAt: expect.any(Number),
      },
    });
    expect(completed()).toBe(false);
  });

  it('re-drives a persisted core-canceled precursor after runtime eviction', async () => {
    const storage = new InMemoryStore();
    const before = buildRuntime(storage).runtime;
    await before.start('demo-approval', {
      runId: 'canceled-wedge',
      inputData: { topic: 'wedge' },
      deadlineMs: 1_000,
    });
    const workflows = await storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: 'canceled-wedge',
    });
    if (!workflows || !snapshot) throw new Error('snapshot missing');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: 'canceled-wedge',
      snapshot: {
        ...snapshot,
        status: 'canceled',
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runLifecycle': {
            version: 1,
            revision: 2,
            deadlineAt: snapshot.requestContext?.['flowsafe.runLifecycle']
              ? (
                  snapshot.requestContext['flowsafe.runLifecycle'] as {
                    deadlineAt: number;
                  }
                ).deadlineAt
              : 0,
            transitionIntent: {
              status: 'cancelled',
              requestedAt: Date.now(),
              replayPrincipals: [principal],
            },
          },
        },
      },
    });

    const after = buildRuntime(storage).runtime;
    await expect(
      after.terminateAsPrincipal(
        'demo-approval',
        'canceled-wedge',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: {
        status: 'cancelled',
        errorEnvelope: { code: 'CANCELLED' },
      },
    });
  });

  it('re-drives a persisted cancellation intent after core writes late success', async () => {
    const storage = new InMemoryStore();
    const before = buildRuntime(storage).runtime;
    await before.start('demo-approval', {
      runId: 'success-after-cancel-intent',
      inputData: { topic: 'late-success' },
      deadlineMs: 1_000,
    });
    const workflows = await storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: 'success-after-cancel-intent',
    });
    if (!workflows || !snapshot) throw new Error('snapshot missing');
    const lifecycle = snapshot.requestContext?.[
      'flowsafe.runLifecycle'
    ] as Record<string, unknown>;
    await workflows.persistWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: 'success-after-cancel-intent',
      snapshot: {
        ...snapshot,
        status: 'success',
        result: { late: true },
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runLifecycle': {
            ...lifecycle,
            revision: 2,
            transitionIntent: {
              status: 'cancelled',
              requestedAt: Date.now(),
              replayPrincipals: [principal],
            },
          },
        },
      },
    });

    const after = buildRuntime(storage).runtime;
    await expect(
      after.terminateAsPrincipal(
        'demo-approval',
        'success-after-cancel-intent',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: {
        status: 'cancelled',
        errorEnvelope: { code: 'CANCELLED' },
      },
    });
    await expect(
      after.status('demo-approval', 'success-after-cancel-intent'),
    ).resolves.toMatchObject({
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED' },
    });
  });

  it('repairs an intent dropped by one stale late-success snapshot write', async () => {
    const storage = new InMemoryStore();
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows storage missing');
    const originalPersist = workflows.persistWorkflowSnapshot.bind(workflows);
    let droppedIntent = false;
    Object.defineProperty(workflows, 'persistWorkflowSnapshot', {
      configurable: true,
      value: async (
        input: Parameters<typeof workflows.persistWorkflowSnapshot>[0],
      ) => {
        const lifecycle = input.snapshot.requestContext?.[
          'flowsafe.runLifecycle'
        ] as Record<string, unknown> | undefined;
        if (
          !droppedIntent &&
          input.snapshot.status === 'success' &&
          lifecycle?.transitionIntent
        ) {
          droppedIntent = true;
          const { transitionIntent: _intent, ...staleLifecycle } = lifecycle;
          return originalPersist({
            ...input,
            snapshot: {
              ...input.snapshot,
              requestContext: {
                ...input.snapshot.requestContext,
                'flowsafe.runLifecycle': staleLifecycle,
              },
            },
          });
        }
        return originalPersist(input);
      },
    });
    const { runtime, entered, release } = heldRuntime({
      storage,
      cancelNoop: true,
    });
    const starting = runtime.start('held-workflow', {
      runId: 'stale-success-intent-repair',
      inputData: {},
    });
    await entered;
    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'stale-success-intent-repair',
        'cancelled',
        [principal],
      ),
    ).resolves.toBe(true);
    release();
    await expect(starting).resolves.toMatchObject({ status: 'success' });
    expect(droppedIntent).toBe(true);

    const repaired = await workflows.loadWorkflowSnapshot({
      workflowName: 'held-workflow',
      runId: 'stale-success-intent-repair',
    });
    expect(repaired?.requestContext?.['flowsafe.runLifecycle']).toMatchObject({
      transitionIntent: { status: 'cancelled' },
    });

    const fresh = heldRuntime({ storage }).runtime;
    await expect(
      fresh.terminateAsPrincipal(
        'held-workflow',
        'stale-success-intent-repair',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: { status: 'cancelled' },
    });
  });

  it('serializes terminal reconciliation with a concurrent cancellation preflight', async () => {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { executionFence: 'none' },
    );
    const step = createStep({
      id: 'finish',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      execute: async () => ({ ok: true }),
    });
    const workflow = createWorkflow({
      id: 'reconcile-lock',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      options: {
        shouldPersistSnapshot: ({ workflowStatus }) =>
          workflowStatus === 'pending',
      },
    })
      .then(step)
      .commit();
    let coreReturned = false;
    const originalCreateRun = workflow.createRun.bind(workflow);
    Object.defineProperty(workflow, 'createRun', {
      configurable: true,
      value: async (...args: Parameters<typeof workflow.createRun>) => {
        const run = await originalCreateRun(...args);
        const originalStart = run.start.bind(run);
        Object.defineProperty(run, 'start', {
          configurable: true,
          value: async (...startArgs: Parameters<typeof run.start>) => {
            const result = await originalStart(...startArgs);
            coreReturned = true;
            return result;
          },
        });
        return run;
      },
    });
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows storage missing');
    const originalLoad = workflows.loadWorkflowSnapshot.bind(workflows);
    let reconciliationLoaded!: () => void;
    let releaseReconciliation!: () => void;
    const loaded = new Promise<void>((resolve) => {
      reconciliationLoaded = resolve;
    });
    const held = new Promise<void>((resolve) => {
      releaseReconciliation = resolve;
    });
    let blocked = false;
    vi.spyOn(workflows, 'loadWorkflowSnapshot').mockImplementation(
      async (input) => {
        const snapshot = await originalLoad(input);
        if (
          coreReturned &&
          !blocked &&
          input.workflowName === 'reconcile-lock' &&
          input.runId === 'reconcile-race'
        ) {
          blocked = true;
          reconciliationLoaded();
          await held;
        }
        return snapshot;
      },
    );

    const starting = runtime.start('reconcile-lock', {
      runId: 'reconcile-race',
      inputData: {},
    });
    await loaded;
    let cancellationSettled = false;
    const cancellation = runtime
      .cancelActiveExecution('reconcile-lock', 'reconcile-race', 'cancelled', [
        principal,
      ])
      .finally(() => {
        cancellationSettled = true;
      });
    await Promise.resolve();
    expect(cancellationSettled).toBe(false);

    releaseReconciliation();
    await expect(starting).resolves.toMatchObject({ status: 'success' });
    await expect(cancellation).rejects.toMatchObject({
      name: 'RunTerminalConflictError',
    });
    await expect(
      runtime.status('reconcile-lock', 'reconcile-race'),
    ).resolves.toMatchObject({ status: 'success' });
  });
});

describe('RunnerRuntime requestContextForRun', () => {
  interface Observation {
    leg: 'start' | 'resume';
    a: unknown;
    b: unknown;
  }

  // probe: first (records context) -> gate (suspends; records context on the
  // resumed execution). Proves what each execution leg actually observes.
  function buildContextProbe(provider: RequestContextProvider): {
    runtime: RunnerRuntime;
    seen: Observation[];
  } {
    const seen: Observation[] = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { requestContextForRun: provider, executionFence: 'none' },
    );
    const first = createStep({
      id: 'first',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push({
          leg: 'start',
          a: requestContext.get('test.a'),
          b: requestContext.get('test.b'),
        });
        return {};
      },
    });
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ resumeData, suspend, requestContext }) => {
        if (!resumeData) return suspend({ reason: 'wait' });
        seen.push({
          leg: 'resume',
          a: requestContext.get('test.a'),
          b: requestContext.get('test.b'),
        });
        return {};
      },
    });
    createWorkflow({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(first)
      .then(gate)
      .commit();
    return { runtime, seen };
  }

  it('merges stored schedule context below trusted provider and runtime values', async () => {
    const seen: Record<string, unknown> = {};
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      {
        requestContextForRun: () => ({ 'test.a': 'provider' }),
        executionFence: 'none',
      },
    );
    const inspect = createStep({
      id: 'inspect-scheduled-context',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.a = requestContext.get('test.a');
        seen.b = requestContext.get('test.b');
        seen.workflowScope = requestContext.get('breakwater.workflowScope');
        return {};
      },
    });
    createWorkflow({
      id: 'scheduled-context',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(inspect)
      .commit();

    await runtime.start('scheduled-context', {
      runId: 'scheduled-context-run',
      inputData: {},
      storedRequestContext: {
        'test.a': 'stored',
        'test.b': 'stored-only',
        'breakwater.workflowScope': 'forged',
      },
    });

    expect(seen).toEqual({
      a: 'provider',
      b: 'stored-only',
      workflowScope: 'scheduled-context',
    });
  });

  it('consults the provider on every start and resume leg', async () => {
    // #given — a provider that mints a distinct context per consult
    let calls = 0;
    const { runtime, seen } = buildContextProbe(() => {
      calls += 1;
      return { 'test.a': `mint-${calls}` };
    });

    // #when
    const started = await runtime.start('probe', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('suspended');
    const resumed = await runtime.resume('probe', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — start leg saw the first mint, resume leg the second: the
    // provider is re-consulted per leg and the resume-time context is what
    // the resumed step observes.
    expect(resumed.status).toBe('success');
    expect(calls).toBe(2);
    expect(seen).toEqual([
      { leg: 'start', a: 'mint-1', b: undefined },
      { leg: 'resume', a: 'mint-2', b: undefined },
    ]);
  });

  it('pins resume-context semantics: the provided context merges over the persisted one', async () => {
    // #given — start mints {a, b}; resume mints only {a}
    let leg = 0;
    const { runtime, seen } = buildContextProbe(() => {
      leg += 1;
      return leg === 1
        ? { 'test.a': 'start-a', 'test.b': 'start-b' }
        : { 'test.a': 'resume-a' };
    });

    // #when
    const started = await runtime.start('probe', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    await runtime.resume('probe', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — empirical pin (core 1.49.0): resume-provided keys override,
    // but persisted start-time keys SURVIVE ('test.b' is still visible).
    // Consequence: omitting a key at resume does not revoke it — a provider
    // that needs to withdraw a capability must overwrite the key (e.g. an
    // empty grant list), not omit it.
    expect(seen[1]).toEqual({ leg: 'resume', a: 'resume-a', b: 'start-b' });
  });

  it('propagates a provider failure instead of running without context', async () => {
    // #given
    const { runtime } = buildContextProbe(() => {
      throw new Error('grant store down');
    });

    // #when / #then
    await expect(
      runtime.start('probe', { runId: crypto.randomUUID(), inputData: {} }),
    ).rejects.toThrow('grant store down');
  });

  it('passes the execution leg: start, then resume with the explicit step', async () => {
    // #given
    const legs: RunLeg[] = [];
    const { runtime } = buildContextProbe((_workflowId, _runId, leg) => {
      legs.push(leg);
      return undefined;
    });

    // #when
    const started = await runtime.start('probe', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    await runtime.resume('probe', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — string step selections normalize to a path; the leg carries
    // the step's current suspension timestamp from the snapshot
    expect(legs).toEqual([
      { kind: 'start' },
      { kind: 'resume', step: ['gate'], suspendedAt: expect.any(Number) },
    ]);
  });

  it('resolves the resume-leg step from the snapshot when none is selected', async () => {
    // #given
    const legs: RunLeg[] = [];
    const { runtime } = buildContextProbe((_workflowId, _runId, leg) => {
      legs.push(leg);
      return undefined;
    });
    const started = await runtime.start('probe', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #when — no explicit step; 'gate' is the only suspended step
    await runtime.resume('probe', started.runId, { resumeData: { go: true } });

    // #then
    expect(legs[1]).toEqual({
      kind: 'resume',
      step: ['gate'],
      suspendedAt: expect.any(Number),
    });
  });

  it('mints the workflow-scope key on every leg, even without a provider', async () => {
    // #given — NO requestContextForRun provider; a step that records the
    // runtime-minted scope (breakwater's crossWorkflowIsolation reads it)
    const seen: unknown[] = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const probe = createStep({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push(requestContext.get('breakwater.workflowScope'));
        return {};
      },
    });
    createWorkflow({
      id: 'scoped-wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(probe)
      .commit();

    // #when
    await runtime.start('scoped-wf', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then — the executing workflow's own id, minted by the runtime
    expect(seen).toEqual(['scoped-wf']);
  });

  it('does not synthesize breakwater isolation scope from runId prefixes', async () => {
    // #given — a probe recording both server-minted keys
    const seen: Array<{ scope: unknown; isolation: unknown }> = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const probe = createStep({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push({
          scope: requestContext.get('breakwater.workflowScope'),
          isolation: requestContext.get('breakwater.isolationScope'),
        });
        return {};
      },
    });
    createWorkflow({
      id: 'scoped-wf2',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(probe)
      .commit();

    // #when — differently shaped opaque run ids
    await runtime.start('scoped-wf2', { runId: 'acme_r1', inputData: {} });
    await runtime.start('scoped-wf2', { runId: 'plain-run', inputData: {} });
    await runtime.start('scoped-wf2', { runId: 'AB_r1', inputData: {} });

    // #then — none becomes an implicit isolation key
    expect(seen).toEqual([
      { scope: 'scoped-wf2', isolation: undefined },
      { scope: 'scoped-wf2', isolation: undefined },
      { scope: 'scoped-wf2', isolation: undefined },
    ]);
  });

  it('does not let a provider override the runtime-minted workflow scope', async () => {
    // #given — a provider that attempts to override the scope key
    const seen: unknown[] = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      {
        executionFence: 'none',
        requestContextForRun: () => ({
          'breakwater.workflowScope': 'overridden',
        }),
      },
    );
    const probe = createStep({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push(requestContext.get('breakwater.workflowScope'));
        return {};
      },
    });
    createWorkflow({
      id: 'scoped-wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(probe)
      .commit();

    // #when
    await runtime.start('scoped-wf', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then — runtime identity merges over provider values
    expect(seen).toEqual(['scoped-wf']);
  });

  it('orders stored context before runtime scope, grants, and trusted identity', async () => {
    const seen: Array<{ keys: string[]; values: Record<string, unknown> }> = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      {
        executionFence: 'none',
        requestContextForRun: () => ({
          stored: 'kept',
          'breakwater.workflowScope': 'forged-workflow',
          'breakwater.isolationScope': 'forged-tenant',
          'breakwater.connectorExecution': { kind: 'start' },
          'breakwater.connectorGrants': [],
          'breakwater.principalPermissions': {
            permissions: ['reports.read'],
            policyVersion: 'permissions-v1',
          },
          runId: 'acme_forged',
          threadId: 'acme_thread',
          resourceId: 'acme_resource',
          'breakwater.actor': { id: 'operator-1', role: 'operator' },
          'breakwater.auditContext': {
            agentId: 'writer',
            entryPath: 'http.start',
          },
        }),
      },
    );
    const probe = createStep({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push({
          keys: [...requestContext.keys()],
          values: requestContext.toJSON(),
        });
        return {};
      },
    });
    createWorkflow({
      id: 'ordered-context',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(probe)
      .commit();

    await runtime.start('ordered-context', {
      runId: 'acme_ordered',
      inputData: {},
    });

    expect(seen[0]?.keys).toEqual([
      'stored',
      'breakwater.workflowScope',
      'runId',
      'flowsafe.runProvenance',
      'breakwater.connectorExecution',
      'breakwater.connectorGrants',
      'breakwater.principalPermissions',
      'threadId',
      'resourceId',
      'breakwater.actor',
      'breakwater.auditContext',
    ]);
    expect(seen[0]?.values).toMatchObject({
      'breakwater.workflowScope': 'ordered-context',
      // A trusted provider's projection passes through like the grant key —
      // this is the seam a workflow host uses to authorize its connectors.
      'breakwater.principalPermissions': {
        permissions: ['reports.read'],
        policyVersion: 'permissions-v1',
      },
    });
  });

  it('leaves nothing persisted when the provider fails on start — the runId stays retryable', async () => {
    // #given — a provider that fails once (e.g. the grant store's D1 is
    // briefly unreachable), then recovers
    let failures = 1;
    const { runtime } = buildContextProbe(() => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('grant store down');
      }
      return undefined;
    });

    // #when — the first start fails BEFORE createRun persists anything
    await expect(
      runtime.start('probe', { runId: 'retry-me', inputData: {} }),
    ).rejects.toThrow('grant store down');

    // #then — no orphaned pending run, and the same runId starts cleanly
    expect(await runtime.status('probe', 'retry-me')).toBeNull();
    const retried = await runtime.start('probe', {
      runId: 'retry-me',
      inputData: {},
    });
    expect(retried.status).toBe('suspended');
  });
});

describe('RunnerRuntime resumeCount projection (re-suspension)', () => {
  // The do-runner OWNS the RunSummary/RunLeg projection, so the layer that
  // publishes the resumeCount contract must pin it against the real engine.
  // resumeCount is the categorical signal flowsafe's grant binding depends on
  // ("undefined on first suspension, defined on re-suspension"); unlike the
  // informational resumedAt (which Mastra stamps only on a payload-bearing
  // resume) the runtime increments resumeCount on EVERY resume, so it holds
  // even for a no-payload re-suspension. gate2x suspends, and re-suspends on a
  // no-payload resume (round 1) or after a payload resume (round 2).
  function buildReSuspender(
    onLeg?: (leg: RunLeg) => void,
    storage = new InMemoryStore(),
  ): RunnerRuntime {
    let rounds = 0;
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      {
        executionFence: 'none',
        ...(onLeg
          ? {
              requestContextForRun: (
                _workflowId: string,
                _runId: string,
                leg: RunLeg,
              ) => {
                onLeg(leg);
                return undefined;
              },
            }
          : {}),
      },
    );
    const gate2x = createStep({
      id: 'gate2x',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      // No resumeSchema on purpose: with a required schema, core rejects a
      // no-payload resume before execute, so the falsy-resume re-suspension
      // (the bug's trigger) is unreachable. Without one, a falsy resume passes
      // validation and re-suspends via the guard below.
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'round 1' });
        rounds += 1;
        if (rounds < 2) return suspend({ reason: 'round 2' });
        return {};
      },
    });
    createWorkflow({
      id: 'resuspend',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gate2x)
      .commit();
    return runtime;
  }

  it('omits resumeCount on the first suspension and carries it on a re-suspension', async () => {
    // #given
    const runtime = buildReSuspender();

    // #when — first suspension
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then — a first suspension carries suspendedAt but NO resumeCount; this
    // undefined is the categorical tie-breaker the grant binding relies on
    expect(started.status).toBe('suspended');
    expect(started.suspendedAt?.gate2x).toBeTypeOf('number');
    expect(started.resumeCount?.gate2x).toBeUndefined();

    // #when — resume (payload); gate2x runs and re-suspends (round 2)
    const reSuspended = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
    });

    // #then — the re-suspension summary carries resumeCount 1 (one resume so
    // far); resumedAt is also present here because the resume carried a payload
    expect(reSuspended.suspended).toEqual([['gate2x']]);
    expect(reSuspended.resumeCount?.gate2x).toBe(1);
    expect(reSuspended.suspendedAt?.gate2x).toBeTypeOf('number');
    expect(reSuspended.resumedAt?.gate2x).toBeTypeOf('number');
  });

  it('prepares a host from one isolated copy of the trusted resume context', async () => {
    const legs: RunLeg[] = [];
    const storage = new InMemoryStore();
    const runtime = buildReSuspender(
      (leg) => legs.push(structuredClone(leg)),
      storage,
    );
    const started = await runtime.start('resuspend', {
      runId: 'acme_resume-context',
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });
    legs.length = 0;

    let preparedWorkflowScope: unknown;
    let preparedIsolationScope: unknown;
    await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
      requestedBy: 'reviewer-1',
      requestedByKind: 'human',
      prepareExecution: async (context) => {
        preparedWorkflowScope = context.get('breakwater.workflowScope');
        preparedIsolationScope = context.get('breakwater.isolationScope');
        const provenance = context.get('flowsafe.runProvenance') as {
          requestedBy?: string;
        };
        provenance.requestedBy = 'forged';
        context.clear();
      },
    });

    expect(preparedWorkflowScope).toBe('resuspend');
    expect(preparedIsolationScope).toBeUndefined();
    expect(legs).toHaveLength(1);
    expect(legs[0]).toMatchObject({
      kind: 'resume',
      step: ['gate2x'],
    });
    await expect(
      runtime.status('resuspend', started.runId),
    ).resolves.toMatchObject({
      requestedBy: 'reviewer-1',
      resumeCount: { gate2x: 1 },
    });
  });

  it('carries resumeCount on a NO-PAYLOAD re-suspension even though Mastra omits resumedAt', async () => {
    // #given — the regression: a falsy resume re-suspends via
    // `if (!resumeData) return suspend(...)`, so Mastra never stamps resumedAt.
    const runtime = buildReSuspender();
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #when — resume with NO resumeData; gate2x re-suspends (round 1 again)
    const reSuspended = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
    });

    // #then — resumedAt stays undefined (Mastra's payload-conditional stamp),
    // but the runtime-owned resumeCount is present, so the grant binding can
    // still tell this re-suspension apart from the first suspension. Pre-fix
    // (resumeCount did not exist) this re-suspension was indistinguishable.
    expect(reSuspended.suspended).toEqual([['gate2x']]);
    expect(reSuspended.resumedAt?.gate2x).toBeUndefined();
    expect(reSuspended.resumeCount?.gate2x).toBe(1);
  });

  it('a required resumeSchema rejects a no-payload resume (why the falsy path needs a schema-less step)', async () => {
    // Tripwire pinning the Mastra-version-dependent boundary the falsy-resume
    // fixtures rely on: with a REQUIRED resumeSchema, core validates resume
    // data and rejects a no-payload resume BEFORE execute, so the falsy-resume
    // re-suspension (the bug's trigger) is only reachable for schema-less /
    // optional-schema / validateInputs-off steps. If a Mastra bump changes
    // this, the "schema-less fixture required" assumption (buildReSuspender,
    // the relaunch-falsy e2e fixture) goes silently stale.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const schemaGate = createStep({
      id: 'schemaGate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'awaiting' });
        return {};
      },
    });
    createWorkflow({
      id: 'schema-gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(schemaGate)
      .commit();
    const started = await runtime.start('schema-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('suspended');

    // #when / #then — a no-payload resume is rejected as invalid resume data,
    // never reaching execute (so it cannot re-suspend without a resumedAt)
    await expect(
      runtime.resume('schema-gate', started.runId, { step: 'schemaGate' }),
    ).rejects.toThrow(/resume data/i);
  });

  it('passes resumeCount on the resume leg, incrementing per resume', async () => {
    // #given — a provider recording every leg
    const legs: RunLeg[] = [];
    const runtime = buildReSuspender((leg) => legs.push(leg));

    // #when — start (round-1 suspension) then resume, which re-suspends
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
    });

    // #then — the leg that reattached to the first suspension read snapshot
    // provenance before any resume, so its resumeCount is undefined
    expect(legs[1]).toMatchObject({
      kind: 'resume',
      step: ['gate2x'],
      suspendedAt: expect.any(Number),
    });
    expect((legs[1] as { resumeCount?: number }).resumeCount).toBeUndefined();

    // #when — resume again, reattaching to the RE-suspension
    const done = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
    });

    // #then — one prior resume happened, so this leg carries resumeCount 1
    expect(done.status).toBe('success');
    expect(legs[2]).toMatchObject({
      kind: 'resume',
      step: ['gate2x'],
      suspendedAt: expect.any(Number),
      resumeCount: 1,
    });
  });

  it('marks only the resumed branch, leaving a co-suspended branch a first suspension', async () => {
    // #given — two parallel gates both suspend; gateA re-suspends on a payload
    // resume (round 2), gateB stays at its first suspension.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    let aRounds = 0;
    const gateA = createStep({
      id: 'gateA',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'A round 1' });
        aRounds += 1;
        if (aRounds < 2) return suspend({ reason: 'A round 2' });
        return { ok: true };
      },
    });
    const gateB = createStep({
      id: 'gateB',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData ? { ok: true } : suspend({ reason: 'B waits' }),
    });
    createWorkflow({
      id: 'parallel-resuspend',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .parallel([gateA, gateB])
      .commit();
    const started = await runtime.start('parallel-resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.suspended).toHaveLength(2);
    expect(started.resumeCount?.gateA).toBeUndefined();
    expect(started.resumeCount?.gateB).toBeUndefined();

    // #when — resume ONLY gateA; it re-suspends (round 2), gateB is untouched
    const reSuspended = await runtime.resume(
      'parallel-resuspend',
      started.runId,
      { step: 'gateA', resumeData: { go: true } },
    );

    // #then — gateA carries resumeCount 1 (it was resumed); gateB, never
    // resumed, stays undefined — its own first suspension, not collapsed into
    // gateA's ordinal in snapshot provenance.
    expect(reSuspended.status).toBe('suspended');
    expect(reSuspended.resumeCount?.gateA).toBe(1);
    expect(reSuspended.resumeCount?.gateB).toBeUndefined();
  });
});

describe('RunnerRuntime resumeCount snapshot provenance (shared runId across workflows)', () => {
  // Two suspending workflows under DIFFERENT ids on ONE runtime, both driven with
  // the SAME caller runId. Mastra persists them as distinct runs (snapshots key on
  // `${workflowName}-${runId}`). wfA completes on its first payload resume;
  // wfB re-suspends once, so its snapshot retains count 1 while wfA becomes
  // terminal. Both gates share step id 'gate' to prove each snapshot owns its
  // own provenance even when caller run ids and inner step keys match.
  function buildSharedRunIdPair(
    onLeg?: (workflowId: string, leg: RunLeg) => void,
  ): RunnerRuntime {
    let bRounds = 0;
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      {
        executionFence: 'none',
        ...(onLeg
          ? {
              requestContextForRun: (
                workflowId: string,
                _runId: string,
                leg: RunLeg,
              ) => {
                onLeg(workflowId, leg);
                return undefined;
              },
            }
          : {}),
      },
    );
    const gateA = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData ? {} : suspend({ reason: 'A waits' }),
    });
    createWorkflow({
      id: 'wfA',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gateA)
      .commit();
    const gateB = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'B round 1' });
        bRounds += 1;
        if (bRounds < 2) return suspend({ reason: 'B round 2' });
        return {};
      },
    });
    createWorkflow({
      id: 'wfB',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gateB)
      .commit();
    return runtime;
  }

  it("preserves a sibling run's snapshot provenance when a run sharing its runId reaches terminal status", async () => {
    // #given — wfA and wfB both suspended under the SAME runId 'shared'
    const runtime = buildSharedRunIdPair();
    await runtime.start('wfA', { runId: 'shared', inputData: {} });
    await runtime.start('wfB', { runId: 'shared', inputData: {} });

    // #given — wfB resumed once, so its snapshot provenance holds gate -> 1
    const reSuspendedB = await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    expect(reSuspendedB.suspended).toEqual([['gate']]);
    expect(reSuspendedB.resumeCount?.gate).toBe(1);

    // #when — wfA with the same caller runId resumes to success
    const doneA = await runtime.resume('wfA', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    expect(doneA.status).toBe('success');

    // #then — wfB's still-suspended round-2 snapshot keeps resumeCount 1
    const statusB = await runtime.status('wfB', 'shared');
    expect(statusB).toMatchObject({
      status: 'suspended',
      suspended: [['gate']],
    });
    expect(statusB?.resumeCount?.gate).toBe(1);
  });

  it("reads a run's own snapshot provenance on the resume leg", async () => {
    // #given — a provider recording (workflowId, leg) for every consult
    const legs: Array<{ workflowId: string; leg: RunLeg }> = [];
    const runtime = buildSharedRunIdPair((workflowId, leg) =>
      legs.push({ workflowId, leg }),
    );
    await runtime.start('wfA', { runId: 'shared', inputData: {} });
    await runtime.start('wfB', { runId: 'shared', inputData: {} });

    // #given — wfB resumed once, bumping the gate ordinal in wfB's snapshot
    await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #when — wfA's first resume reads its snapshot provenance before incrementing it
    await runtime.resume('wfA', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — wfA never resumed before, so its own snapshot has no gate entry
    // and the leg's resumeCount is undefined
    const wfAResumeLeg = legs.find(
      (e) => e.workflowId === 'wfA' && e.leg.kind === 'resume',
    )?.leg as { resumeCount?: number } | undefined;
    expect(wfAResumeLeg).toBeDefined();
    expect(wfAResumeLeg?.resumeCount).toBeUndefined();
  });

  // A step that re-suspends on EVERY resume (never completes), so a run stays
  // suspended at any depth and status() keeps projecting its accumulating
  // ordinal — the deep-chain (3+ suspension) case the pair-binding relies on.
  function buildSharedDeepChain(): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    for (const id of ['wfA', 'wfB']) {
      const gate = createStep({
        id: 'gate',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        suspendSchema: z.object({ reason: z.string() }),
        execute: async ({ resumeData, suspend }) =>
          suspend({ reason: resumeData ? 'again' : 'first' }),
      });
      createWorkflow({
        id,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
        .then(gate)
        .commit();
    }
    return runtime;
  }

  it('accumulates per-workflow resume ordinals independently past depth 1 under a shared runId', async () => {
    // #given — wfA and wfB both suspended under the SAME runId 'shared'
    const runtime = buildSharedDeepChain();
    await runtime.start('wfA', { runId: 'shared', inputData: {} });
    await runtime.start('wfB', { runId: 'shared', inputData: {} });

    // #when — wfA resumed once (ordinal 1), wfB resumed twice (ordinal 2); both
    // re-suspend each time, so snapshot provenance must accumulate, not reset to 1
    await runtime.resume('wfA', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — each workflow's ordinal is its OWN accumulated count: wfA=1, wfB=2.
    // A fully shared bucket takes all 3 increments, so both reads see 3; a
    // get-or-create keyed wrong freezes both at 1 (the deep-chain leak: a round-2
    // approval minting into round 3).
    const statusA = await runtime.status('wfA', 'shared');
    const statusB = await runtime.status('wfB', 'shared');
    expect(statusA?.resumeCount?.gate).toBe(1);
    expect(statusB?.resumeCount?.gate).toBe(2);
  });
});

describe('RunnerRuntime snapshot provenance durability', () => {
  function buildDurable(
    storage: InMemoryStore,
    onLeg?: (leg: RunLeg) => void,
    providedContext?: Record<string, unknown>,
  ): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      {
        executionFence: 'none',
        requestContextForRun: (_workflowId, _runId, leg) => {
          onLeg?.(leg);
          return providedContext;
        },
      },
    );
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData ? {} : suspend({ reason: 'wait' }),
    });
    createWorkflow({
      id: 'durable-gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gate)
      .commit();
    return runtime;
  }

  function buildTerminalRepair(
    storage: InMemoryStore,
    requestContextForRun?: RequestContextProvider,
  ): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { requestContextForRun, executionFence: 'none' },
    );
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData ? {} : suspend({ reason: 'wait' }),
    });
    createWorkflow({
      id: 'terminal-repair',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      options: {
        shouldPersistSnapshot: ({ workflowStatus }) =>
          workflowStatus === 'pending' || workflowStatus === 'suspended',
      },
    })
      .then(gate)
      .commit();
    return runtime;
  }

  it('projects requester and resume ordinal after runtime eviction', async () => {
    const storage = new InMemoryStore();
    const before = buildDurable(storage);
    const started = await before.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });
    const reSuspended = await before.resume('durable-gate', started.runId, {
      step: 'gate',
      requestedBy: 'reviewer-1',
      requestedByKind: 'human',
    });
    expect(reSuspended).toMatchObject({
      status: 'suspended',
      requestedBy: 'reviewer-1',
      resumeCount: { gate: 1 },
    });

    const legs: RunLeg[] = [];
    const after = buildDurable(storage, (leg) => legs.push(leg));
    const recovered = await after.status('durable-gate', started.runId);
    expect(recovered).toMatchObject({
      status: 'suspended',
      requestedBy: 'reviewer-1',
      resumeCount: { gate: 1 },
    });
    const done = await after.resume('durable-gate', started.runId, {
      step: 'gate',
      resumeData: { go: true },
      requestedBy: 'reviewer-2',
      requestedByKind: 'human',
    });
    expect(done).toMatchObject({
      status: 'success',
      requestedBy: 'reviewer-2',
    });
    expect(legs.find((leg) => leg.kind === 'resume')).toMatchObject({
      resumeCount: 1,
    });
  });

  it.each([
    ['an overlong requester', { requestedBy: 'r'.repeat(201) }],
    ['an all-whitespace requester', { requestedBy: ' '.repeat(200) }],
    ['a control-bearing requester', { requestedBy: 'requester\u0000forged' }],
    ['an invalid requester kind', { requestedByKind: 'operator' }],
  ])('fails closed on stored run provenance with %s', async (_label, corruption) => {
    const storage = new InMemoryStore();
    const before = buildDurable(storage);
    const started = await before.start('durable-gate', {
      runId: 'corrupt-requester-provenance',
      inputData: {},
      requestedBy: 'initiator',
      requestedByKind: 'human',
    });
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows store missing');
    const snapshot = await workflows.loadWorkflowSnapshot({
      workflowName: 'durable-gate',
      runId: started.runId,
    });
    if (!snapshot) throw new Error('workflow snapshot missing');
    const provenance = snapshot.requestContext?.[
      'flowsafe.runProvenance'
    ] as Record<string, unknown>;
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-gate',
      runId: started.runId,
      snapshot: {
        ...snapshot,
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runProvenance': {
            ...provenance,
            ...corruption,
          },
        },
      },
    });

    await expect(
      buildDurable(storage).status('durable-gate', started.runId),
    ).rejects.toThrow('stored run provenance is malformed');
  });

  it('reads legacy id-only provenance but requires a new complete pair before resume writes', async () => {
    const storage = new InMemoryStore();
    const before = buildDurable(storage);
    const started = await before.start('durable-gate', {
      runId: 'legacy-requester-provenance',
      inputData: {},
      requestedBy: 'legacy-initiator',
      requestedByKind: 'human',
    });
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows store missing');
    const snapshot = await workflows.loadWorkflowSnapshot({
      workflowName: 'durable-gate',
      runId: started.runId,
    });
    if (!snapshot) throw new Error('workflow snapshot missing');
    const legacyProvenance = {
      ...(snapshot.requestContext?.['flowsafe.runProvenance'] as Record<
        string,
        unknown
      >),
    };
    delete legacyProvenance.requestedByKind;
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-gate',
      runId: started.runId,
      snapshot: {
        ...snapshot,
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runProvenance': legacyProvenance,
        },
      },
    });

    const after = buildDurable(storage);
    const legacyStatus = await after.status('durable-gate', started.runId);
    expect(legacyStatus).toMatchObject({ requestedBy: 'legacy-initiator' });
    expect(legacyStatus?.requestedByKind).toBeUndefined();
    await expect(
      after.resume('durable-gate', started.runId, { step: 'gate' }),
    ).rejects.toThrow(
      'legacy requestedBy provenance requires an explicit requestedBy and requestedByKind to resume',
    );
    expect(
      (await after.status('durable-gate', started.runId))?.requestedByKind,
    ).toBeUndefined();
    await expect(
      after.resume('durable-gate', started.runId, {
        step: 'gate',
        requestedBy: 'reviewer-1',
        requestedByKind: 'human',
      }),
    ).resolves.toMatchObject({
      status: 'suspended',
      requestedBy: 'reviewer-1',
      requestedByKind: 'human',
    });
  });

  it('reserves snapshot provenance against provider override', async () => {
    const storage = new InMemoryStore();
    const runtime = buildDurable(storage, undefined, {
      'flowsafe.runProvenance': {
        version: 1,
        requestedBy: 'forged',
        attemptToken: 'forged',
        resumeCounts: [['gate', 99]],
      },
    });
    const started = await runtime.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });

    expect(started.requestedBy).toBe('operator-1');
    await expect(
      runtime.status('durable-gate', started.runId),
    ).resolves.toMatchObject({ requestedBy: 'operator-1' });
  });

  it('repairs a terminal-only snapshot with the current resume provenance', async () => {
    const storage = new InMemoryStore();
    const requestContextForRun: RequestContextProvider = (
      _workflowId,
      _runId,
      leg,
    ) =>
      leg.kind === 'start'
        ? { 'test.a': 'start-a', 'test.b': 'start-b' }
        : { 'test.a': 'resume-a' };
    const before = buildTerminalRepair(storage, requestContextForRun);
    const started = await before.start('terminal-repair', {
      runId: 'terminal-provenance',
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });

    await expect(
      before.resume('terminal-repair', started.runId, {
        step: 'gate',
        resumeData: { approved: true },
        requestedBy: 'reviewer-1',
        requestedByKind: 'human',
      }),
    ).resolves.toMatchObject({
      status: 'success',
      requestedBy: 'reviewer-1',
    });

    const workflows = await storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'terminal-repair',
      runId: started.runId,
    });
    expect(snapshot?.requestContext).toMatchObject({
      'test.a': 'resume-a',
      'test.b': 'start-b',
      'flowsafe.runProvenance': expect.objectContaining({
        requestedBy: 'reviewer-1',
      }),
    });

    const after = buildTerminalRepair(storage, requestContextForRun);
    await expect(
      after.status('terminal-repair', started.runId),
    ).resolves.toMatchObject({
      status: 'success',
      requestedBy: 'reviewer-1',
    });
  });

  it('returns the committed terminal summary when repair acknowledgement is lost', async () => {
    const storage = new InMemoryStore();
    const runtime = buildTerminalRepair(storage);
    const started = await runtime.start('terminal-repair', {
      runId: 'terminal-lost-ack',
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows store missing');
    const persist = workflows.persistWorkflowSnapshot.bind(workflows);
    let loseAcknowledgement = true;
    vi.spyOn(workflows, 'persistWorkflowSnapshot').mockImplementation(
      async (input) => {
        await persist(input);
        if (loseAcknowledgement && input.snapshot.status === 'success') {
          loseAcknowledgement = false;
          throw new Error('terminal persist acknowledgement lost');
        }
      },
    );

    await expect(
      runtime.resume('terminal-repair', started.runId, {
        step: 'gate',
        resumeData: { approved: true },
        requestedBy: 'reviewer-1',
        requestedByKind: 'human',
      }),
    ).resolves.toMatchObject({
      status: 'success',
      requestedBy: 'reviewer-1',
    });
    expect(loseAcknowledgement).toBe(false);
  });
});

// A step arms a per-suspension deadline through Mastra's own suspend payload,
// so these tests exercise the whole author-facing contract at the runtime level:
// what the summary carries back, and what a Mastra suspendSchema does to the
// reserved key on the way through.
describe('per-suspension deadline contract', () => {
  const SUSPENSION_DEADLINE_MS = 900_000;

  function timedGateRuntime(
    id: string,
    suspendSchema?: z.ZodType,
    resumeSchema?: z.ZodType,
  ): {
    runtime: RunnerRuntime;
    storage: InMemoryStore;
    workflow: { getWorkflowRunById: (runId: string) => Promise<unknown> };
    start: () => Promise<RunSummary>;
  } {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { executionFence: 'none' },
    );
    // Built as a value so the reserved key survives a suspendSchema that does
    // not declare it — which is exactly what the stripping test measures.
    const suspendPayload: Record<string, unknown> = {
      reason: 'awaiting signal',
      [SUSPENSION_DEADLINE_PAYLOAD_KEY]: SUSPENSION_DEADLINE_MS,
    };
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
      ...(suspendSchema ? { suspendSchema } : {}),
      ...(resumeSchema ? { resumeSchema } : {}),
      execute: async ({ resumeData, suspend }) =>
        resumeData
          ? {
              settledBy: isSuspensionTimeoutResumeData(resumeData)
                ? 'timeout'
                : 'signal',
            }
          : suspend(suspendPayload),
    });
    const workflow = createWorkflow({
      id,
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
    })
      .then(gate)
      .commit();
    return {
      runtime,
      storage,
      workflow,
      start: () =>
        runtime.start(id, { runId: crypto.randomUUID(), inputData: {} }),
    };
  }

  /**
   * Blind the workflows store's row read — the exact seam Mastra falls back
   * from — and hand back the restore. Deliberately NOT the Workflow method:
   * stubbing that would FABRICATE the fallback, and what is under test is that
   * Mastra produces it and stamps it. File-local rather than shared: the DO
   * suite needs the same seam and keeping each copy beside the fixtures it
   * serves is cheaper than a new shared module for eight lines.
   */
  async function blindWorkflowRow(storage: InMemoryStore): Promise<() => void> {
    const store = (await storage.getStore('workflows')) as unknown as {
      getWorkflowRunById: (args: unknown) => Promise<unknown>;
    };
    const original = store.getWorkflowRunById;
    store.getWorkflowRunById = async () => null;
    return () => {
      store.getWorkflowRunById = original;
    };
  }

  it('lets the resumed step tell a timeout from a real signal', async () => {
    const { runtime, start } = timedGateRuntime('timed-gate');
    const started = await start();

    // #then — the summary carries everything the deadline is derived from
    const { entries, rejected } = suspensionDeadlinesOf(started);
    expect(rejected).toEqual([]);
    expect(entries[0]).toEqual({
      step: 'gate',
      deadlineAt:
        (started.suspendedAt?.gate as number) + SUSPENSION_DEADLINE_MS,
      suspendedAt: started.suspendedAt?.gate,
      resumeCount: 0,
    });

    // #when — the deadline elapses and flowsafe resumes the run itself
    const timedOut = await runtime.resume('timed-gate', started.runId, {
      step: ['gate'],
      resumeData: suspensionTimeoutResumeData(
        entries[0] as SuspensionDeadlineEntry,
        Date.now(),
      ),
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
      requestedByKind: 'system',
    });

    expect(timedOut).toMatchObject({
      status: 'success',
      result: { settledBy: 'timeout' },
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
      requestedByKind: 'system',
    });

    // #when — the same step reached by a genuine signal instead
    const signalled = await start();
    const resumed = await runtime.resume('timed-gate', signalled.runId, {
      step: ['gate'],
      resumeData: { approvedBy: 'bob' },
      requestedBy: 'reviewer-1',
      requestedByKind: 'human',
    });

    expect(resumed).toMatchObject({
      status: 'success',
      result: { settledBy: 'signal' },
      requestedBy: 'reviewer-1',
    });
  });

  it('pins Mastra stripping the reserved key from an undeclared suspendSchema', async () => {
    // Tripwire on the documented authoring caveat: Mastra validates the suspend
    // payload and SUBSTITUTES the parsed output, so a z.object() that does not
    // declare the reserved field drops it and nothing arms. If a Mastra upgrade
    // changes that, this fails instead of the caveat going silently stale.
    const { start } = timedGateRuntime(
      'stripped-gate',
      z.object({ reason: z.string() }),
    );

    const started = await start();

    const payload = (started.suspendPayload as Record<string, unknown>).gate;
    expect(payload).toEqual({ reason: 'awaiting signal' });
    expect(suspensionDeadlinesOf(started).entries).toEqual([]);
  });

  it('pins the reserved key surviving a suspendSchema that declares it', async () => {
    const { start } = timedGateRuntime(
      'declared-gate',
      z.object({
        reason: z.string(),
        [SUSPENSION_DEADLINE_PAYLOAD_KEY]: z.number(),
      }),
    );

    const started = await start();

    expect(
      (started.suspendPayload as Record<string, Record<string, unknown>>)
        .gate?.[SUSPENSION_DEADLINE_PAYLOAD_KEY],
    ).toBe(SUSPENSION_DEADLINE_MS);
    expect(suspensionDeadlinesOf(started).entries).toHaveLength(1);
  });

  it.each([
    ['loose-object-gate', z.looseObject({ reason: z.string() })],
    [
      'passthrough-gate',
      z.object({ reason: z.string() }).passthrough() as unknown as z.ZodType,
    ],
  ])('arms through the documented loose-schema escape hatch (%s)', async (id, suspendSchema) => {
    // The escape hatch the README and the design doc offer authors who do
    // not want to name a flowsafe key in their own schema. If a Mastra or
    // zod upgrade stops honouring it, that advice is wrong and this fails.
    const { start } = timedGateRuntime(id, suspendSchema);

    const started = await start();

    expect(suspensionDeadlinesOf(started).entries).toHaveLength(1);
  });

  it('fails the timeout resume of a step whose resumeSchema rejects the envelope', async () => {
    // The likelier of the two authoring footguns: every realistic approval
    // step declares a resumeSchema, and Mastra validates resume data before
    // the engine is touched, so a schema that does not accept the envelope
    // makes the timeout resume throw. The wake then charges its retry ledger
    // and eventually drops the deadline — documented, and pinned here.
    const { runtime, start } = timedGateRuntime(
      'resume-schema-gate',
      undefined,
      z.object({ approvedBy: z.string() }),
    );
    const started = await start();
    const entry = suspensionDeadlinesOf(started)
      .entries[0] as SuspensionDeadlineEntry;

    // The message matters: a bare rejection would also pass if the resume threw
    // for an unrelated reason, and then this would stop pinning the footgun.
    await expect(
      runtime.resume('resume-schema-gate', started.runId, {
        step: ['gate'],
        resumeData: suspensionTimeoutResumeData(entry, Date.now()),
        requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
        requestedByKind: 'system',
      }),
    ).rejects.toThrow('Invalid resume data');

    // #then — the run is untouched: the step never took its timeout branch
    const after = await runtime.status('resume-schema-gate', started.runId);
    expect(after?.status).toBe('suspended');
    expect(after?.resumeCount?.gate).toBeUndefined();
  });

  it('refuses to arm a nested suspension instead of arming nothing silently', async () => {
    // Mastra reports a nested suspension as the nested path but keys the
    // payload and the fence by the TOP-LEVEL step, so there is no fence for
    // the step that actually suspended. v1 refuses it, loudly, rather than
    // leaving an author to believe a deadline was accepted.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const approval = createStep({
      id: 'approval',
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData
          ? { settledBy: 'signal' }
          : suspend({
              reason: 'awaiting signal',
              [SUSPENSION_DEADLINE_PAYLOAD_KEY]: SUSPENSION_DEADLINE_MS,
            }),
    });
    const inner = createWorkflow({
      id: 'nested',
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
    })
      .then(approval)
      .commit();
    createWorkflow({
      id: 'nested-outer',
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
    })
      .then(inner)
      .commit();

    const started = await runtime.start('nested-outer', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    expect(started.suspended).toEqual([['nested', 'approval']]);
    expect(suspensionDeadlinesOf(started)).toEqual({
      entries: [],
      rejected: [
        {
          step: 'nested.approval',
          reason: 'nested suspension paths are not supported',
        },
      ],
    });

    // #then — and refused again on the projection the alarm and the recovered
    // start read, which reports the SAME suspension as the enclosing step
    // alone. A refusal on only one of the two projections is worse than none:
    // the entry arms from the projection that misses it and then resumes a
    // step whose fence describes a different suspension.
    const rehydrated = await runtime.status('nested-outer', started.runId);
    expect(rehydrated?.suspended).toEqual([['nested']]);
    expect(suspensionDeadlinesOf(rehydrated as RunSummary)).toEqual({
      entries: [],
      rejected: [
        {
          step: 'nested',
          reason: 'nested suspension paths are not supported',
        },
      ],
    });

    // Tripwire on the marker that refusal depends on: Mastra stamps the inner
    // path into the persisted payload, and that is the only thing telling this
    // suspension apart from an ordinary top-level one. A rename would make
    // nested deadlines arm again, so it fails here rather than there.
    expect(
      (rehydrated?.suspendPayload as Record<string, Record<string, unknown>>)
        .nested?.[MASTRA_WORKFLOW_META_KEY],
    ).toMatchObject({ path: ['approval'] });
  });

  it.each([
    ['a plain step id', 'plain-gate'],
    ['a step id containing a dot', 'dotted.gate'],
  ])('derives the same deadline from the live summary and from status() for %s', async (_label, stepId) => {
    // The contract the whole design rests on: a lifecycle boundary arms from
    // the live summary while the alarm fences against the rehydrated one, so
    // the two must derive identical entries. They key `suspended`
    // differently for a dotted id — ['a.b'] live, ['a','b'] rehydrated — and
    // an entry derived from one that the other cannot recognize is a deadline
    // that arms and then silently disappears.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );
    const gate = createStep({
      id: stepId,
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData
          ? { settledBy: 'signal' }
          : suspend({
              reason: 'awaiting signal',
              [SUSPENSION_DEADLINE_PAYLOAD_KEY]: SUSPENSION_DEADLINE_MS,
            }),
    });
    createWorkflow({
      id: `projection-${stepId}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
    })
      .then(gate)
      .commit();

    const live = await runtime.start(`projection-${stepId}`, {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    const rehydrated = await runtime.status(`projection-${stepId}`, live.runId);

    expect(suspensionDeadlinesOf(live).entries).toEqual([
      {
        step: stepId,
        deadlineAt:
          (live.suspendedAt?.[stepId] as number) + SUSPENSION_DEADLINE_MS,
        suspendedAt: live.suspendedAt?.[stepId],
        resumeCount: 0,
      },
    ]);
    expect(suspensionDeadlinesOf(rehydrated as RunSummary)).toEqual(
      suspensionDeadlinesOf(live),
    );
  });

  it('refuses to answer authoritatively from Mastra in-memory fallback state', async () => {
    // Tripwire on the marker the whole wake discipline keys on, pinned the way
    // __workflow_meta is: against the REAL producer. Mastra answers a state
    // read from the in-memory Run it still holds whenever the row lookup comes
    // back empty, and stamps `isFromInMemory` on exactly that answer. A rename
    // or a dropped stamp would put the wake back to concluding things from a
    // read that never reached storage — deleting a live record, spending an
    // abandonment budget, deleting a real row in recoverStartAttempt — so it
    // fails here rather than there.
    const { runtime, storage, workflow, start } =
      timedGateRuntime('marker-gate');
    const started = await start();
    expect(started.status).toBe('suspended');

    const restore = await blindWorkflowRow(storage);
    try {
      // #then — with the row read blinded, the state Mastra hands back is the
      // in-memory Run it still holds, carrying the marker
      const state = (await workflow.getWorkflowRunById(
        started.runId,
      )) as Record<string, unknown>;
      expect(state.isFromInMemory).toBe(true);
      expect(state.status).toBe('pending');
      expect(state.requestContext).toBeUndefined();
      const fallback = (await runtime.status(
        'marker-gate',
        started.runId,
      )) as RunSummary;
      expect(
        (fallback as unknown as { isFromInMemory?: boolean }).isFromInMemory,
      ).toBeUndefined();

      // #then — the authoritative read refuses it, naming both ids and NO
      // cause: the run object mints this same class for any read that did not
      // succeed, so a message claiming the in-memory fallback would name the
      // wrong one during a storage incident.
      await expect(
        runtime.authoritativeStatus('marker-gate', started.runId),
      ).rejects.toBeInstanceOf(RunStateUnreadableError);
      await expect(
        runtime.authoritativeStatus('marker-gate', started.runId),
      ).rejects.toThrow(
        new RegExp(
          `^run '${started.runId}' of workflow 'marker-gate' state is not readable$`,
        ),
      );

      // #then — while status() still serves the fabricated summary, which is
      // 'pending' for a run that has never been resumed and so passes the
      // self-consistency backstop: the marker is the only thing that catches
      // this shape.
      expect(fallback.status).toBe('pending');
      expect(fallback.suspended).toBeUndefined();
      expect(isReadableRunSummary(fallback)).toBe(true);
    } finally {
      restore();
    }

    // #then — and the run was suspended the whole time
    const healed = await runtime.status('marker-gate', started.runId);
    expect(healed?.status).toBe('suspended');
    expect(healed?.suspended).toEqual([['gate']]);
  });

  it('throws the class a consumer catches through the package barrel', async () => {
    // #given — a host catching this failure imports it from the barrel, never
    // from the module that throws it. Anything that turned the re-export into
    // a second declaration would leave every consumer `instanceof` false while
    // both files still compiled.
    const { runtime, storage, start } = timedGateRuntime('barrel-gate');
    const started = await start();
    const restore = await blindWorkflowRow(storage);

    // #when / #then
    try {
      await expect(
        runtime.authoritativeStatus('barrel-gate', started.runId),
      ).rejects.toBeInstanceOf(BarrelRunStateUnreadableError);
    } finally {
      restore();
    }
  });
});
