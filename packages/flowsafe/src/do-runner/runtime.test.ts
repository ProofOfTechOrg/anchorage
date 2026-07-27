// SPDX-License-Identifier: Apache-2.0
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { init } from './init.js';
import { createHostPubSub } from './pubsub.js';
import {
  DurableStorageResumeLedger,
  type ResumeLedgerStorage,
} from './resume-ledger.js';
import {
  InvalidRunRequestError,
  type RequestContextProvider,
  RunAlreadyExistsError,
  type RunLeg,
  RunNotSuspendedError,
  type RunnerRuntime,
  UnknownWorkflowError,
} from './runtime.js';

interface Counters {
  /** Times the approval step's post-approval body ran (the gated action). */
  approvalResumes: number;
  /** Times the echo step executed. */
  echoRuns: number;
}

// demo-approval: research -> approval (suspends; counts resumed executions).
// echo: single step, completes immediately (counts executions).
function buildRuntime(storage: InMemoryStore): {
  runtime: RunnerRuntime;
  counters: Counters;
} {
  const counters: Counters = { approvalResumes: 0, echoRuns: 0 };
  const { createWorkflow, createStep, runtime } = init({ storage });

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
    const { runtime } = init({ storage: new InMemoryStore() }, { pubsub });

    // #then — the SAME instance is reachable, so Track A's createRun sites and
    // observe() replay share one feed. Delete the thread in init.ts and this
    // fails: runtime.pubsub is undefined and the two createRun sites would each
    // let core default a separate emitter — the DL-001 bug this seam prevents.
    expect(runtime.pubsub).toBe(pubsub);
  });

  it('leaves runtime.pubsub undefined when the host configures none (byte-identical)', () => {
    // #when — no pubsub passed
    const { runtime } = init({ storage: new InMemoryStore() });

    // #then — undefined, the polling-fallback posture existing hosts keep
    expect(runtime.pubsub).toBeUndefined();
  });
});

describe('RunnerRuntime', () => {
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
    const { createWorkflow } = init({ storage: new InMemoryStore() });
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

  it.each([
    'team:wf',
    'a/b',
    '.',
    '..',
    '',
  ])("rejects non-path-safe workflow id '%s' at registration", (id) => {
    // #given
    const { createWorkflow } = init({ storage: new InMemoryStore() });

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
    const { createWorkflow, runtime } = init({ storage: new InMemoryStore() });

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
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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
  });
});

const ISO_8601 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

describe('RunnerRuntime.status projection', () => {
  it('persists a terminal status when a workflow retains only resume snapshots', async () => {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init({ storage });
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
    const { createWorkflow, createStep, runtime } = init({ storage });
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
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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
      { requestContextForRun: provider },
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
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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

  it('mints the isolation scope from a tenant-salted runId, and omits it otherwise', async () => {
    // #given — a probe recording both server-minted keys
    const seen: Array<{ scope: unknown; isolation: unknown }> = [];
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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

    // #when — one INV-1 tenant-salted run, one plain single-tenant run
    await runtime.start('scoped-wf2', { runId: 'acme_r1', inputData: {} });
    await runtime.start('scoped-wf2', { runId: 'plain-run', inputData: {} });
    // a prefix that fails INV-3 must NOT mint (e.g. underscore-led)
    await runtime.start('scoped-wf2', { runId: 'AB_r1', inputData: {} });

    // #then — the tenant prefix is server-authoritative (the runId was minted
    // from the authenticated tenant); non-tenant runs stay scope-less so the
    // single-tenant OSS keys are untouched
    expect(seen).toEqual([
      { scope: 'scoped-wf2', isolation: 'acme' },
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
        requestContextForRun: () => ({
          stored: 'kept',
          'breakwater.workflowScope': 'forged-workflow',
          'breakwater.isolationScope': 'forged-tenant',
          'breakwater.approvedConnectors': [],
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
      'breakwater.isolationScope',
      'breakwater.approvedConnectors',
      'runId',
      'threadId',
      'resourceId',
      'breakwater.actor',
      'breakwater.auditContext',
    ]);
    expect(seen[0]?.values).toMatchObject({
      'breakwater.workflowScope': 'ordered-context',
      'breakwater.isolationScope': 'acme',
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
  function buildReSuspender(onLeg?: (leg: RunLeg) => void): RunnerRuntime {
    let rounds = 0;
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      onLeg
        ? {
            requestContextForRun: (_workflowId, _runId, leg) => {
              onLeg(leg);
              return undefined;
            },
          }
        : undefined,
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

  it('derives the same trusted resume fingerprint for preparation and execution', async () => {
    const legs: RunLeg[] = [];
    const runtime = buildReSuspender((leg) => legs.push(structuredClone(leg)));
    const started = await runtime.start('resuspend', {
      runId: 'acme_resume-context',
      inputData: {},
    });
    legs.length = 0;

    const prepared = await runtime.trustedRequestContextForResume(
      'resuspend',
      started.runId,
      { step: 'gate2x' },
    );
    await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
    });

    expect(prepared.get('breakwater.workflowScope')).toBe('resuspend');
    expect(prepared.get('breakwater.isolationScope')).toBe('acme');
    expect(legs).toHaveLength(2);
    expect(legs[0]).toEqual(legs[1]);
    expect(legs[0]).toMatchObject({
      kind: 'resume',
      step: ['gate2x'],
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
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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

    // #then — the leg that reattached to the FIRST suspension read the ledger
    // before any resume, so its resumeCount is undefined
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
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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
    // gateA's ordinal by the shared ledger.
    expect(reSuspended.status).toBe('suspended');
    expect(reSuspended.resumeCount?.gateA).toBe(1);
    expect(reSuspended.resumeCount?.gateB).toBeUndefined();
  });
});

describe('RunnerRuntime resumeCount ledger keying (shared runId across workflows)', () => {
  // Two suspending workflows under DIFFERENT ids on ONE runtime, both driven with
  // the SAME caller runId. Mastra persists them as distinct runs (snapshots key on
  // `${workflowName}-${runId}`), so the ONLY thing that can conflate them is the
  // runtime's own resume ledger. wfA completes on its first payload resume
  // (terminal — triggers the ledger delete); wfB re-suspends once (round 1 ->
  // round 2), so it holds a live ledger entry across wfA's delete. Both gates share
  // step id 'gate' on purpose: an identical inner stepKey is what lets one run's
  // ordinal contaminate the other's leg read if the OUTER key is runId alone.
  function buildSharedRunIdPair(
    onLeg?: (workflowId: string, leg: RunLeg) => void,
  ): RunnerRuntime {
    let bRounds = 0;
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      onLeg
        ? {
            requestContextForRun: (workflowId, _runId, leg) => {
              onLeg(workflowId, leg);
              return undefined;
            },
          }
        : undefined,
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

  it("does not wipe a sibling run's resume ledger when a run sharing its runId reaches terminal status", async () => {
    // #given — wfA and wfB both suspended under the SAME runId 'shared'
    const runtime = buildSharedRunIdPair();
    await runtime.start('wfA', { runId: 'shared', inputData: {} });
    await runtime.start('wfB', { runId: 'shared', inputData: {} });

    // #given — wfB resumed once, so its ledger bucket holds gate -> 1
    const reSuspendedB = await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    expect(reSuspendedB.suspended).toEqual([['gate']]);
    expect(reSuspendedB.resumeCount?.gate).toBe(1);

    // #when — wfA (same runId) resumed to SUCCESS; a terminal run drops its ledger.
    // Keyed by runId alone (the bug) this delete('shared') wipes wfB's bucket too.
    const doneA = await runtime.resume('wfA', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    expect(doneA.status).toBe('success');

    // #then — wfB's still-suspended round-2 leg keeps resumeCount 1. Pre-fix this
    // projected undefined (wfA's delete erased it), letting a spent first-suspension
    // approval (resumeCount undefined) re-mint at the grant gate — the leak.
    const statusB = await runtime.status('wfB', 'shared');
    expect(statusB).toMatchObject({
      status: 'suspended',
      suspended: [['gate']],
    });
    expect(statusB?.resumeCount?.gate).toBe(1);
  });

  it("reads a run's own ledger bucket on the resume leg, not a sibling run's sharing the runId", async () => {
    // #given — a provider recording (workflowId, leg) for every consult
    const legs: Array<{ workflowId: string; leg: RunLeg }> = [];
    const runtime = buildSharedRunIdPair((workflowId, leg) =>
      legs.push({ workflowId, leg }),
    );
    await runtime.start('wfA', { runId: 'shared', inputData: {} });
    await runtime.start('wfB', { runId: 'shared', inputData: {} });

    // #given — wfB resumed once, bumping the 'gate' ordinal in wfB's bucket
    await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #when — wfA's FIRST resume of its FIRST suspension; the leg fingerprint reads
    // the ledger BEFORE this resume increments it
    await runtime.resume('wfA', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — wfA never resumed before, so its own bucket has no 'gate' entry: the
    // leg's resumeCount is undefined. Keyed by runId alone (the bug) it read wfB's
    // bucket and saw 1 — a first suspension masquerading as a re-suspension, which
    // lets a re-suspension approval mint into a first leg.
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
    const { createWorkflow, createStep, runtime } = init({
      storage: new InMemoryStore(),
    });
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
    // re-suspend each time, so the ledger must ACCUMULATE, not reset to 1
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

describe('RunnerRuntime resume ledger durability (DO eviction)', () => {
  // In-memory ledger state dies with the isolate on DO eviction/hibernation/
  // redeploy, while ctx.storage (and D1 snapshots) survive. Pre-seam, a
  // re-suspension resumed after an eviction read leg.resumeCount undefined
  // against the approval's captured ordinal — mismatch — and the APPROVED
  // action silently no-oped. These tests back the ledger with a fake
  // ctx.storage and rebuild the runtime around it, exactly the eviction
  // topology.
  function fakeLedgerStorage(): ResumeLedgerStorage & {
    keyCount(): number;
  } {
    const map = new Map<string, unknown>();
    return {
      async get<T>(key: string): Promise<T | undefined> {
        return map.get(key) as T | undefined;
      },
      async put<T>(key: string, value: T): Promise<void> {
        map.set(key, value);
      },
      async delete(key: string): Promise<boolean> {
        return map.delete(key);
      },
      keyCount: () => map.size,
    };
  }

  // gate completes on a payload resume and re-suspends on a falsy one; no
  // resumeSchema so the falsy resume reaches execute (see the tripwire test
  // above). Behavior is stateless, so a rebuilt runtime acts identically.
  function buildDurable(
    storage: InMemoryStore,
    ledgerStorage: ResumeLedgerStorage,
    onLeg?: (leg: RunLeg) => void,
  ): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      {
        resumeLedger: new DurableStorageResumeLedger(ledgerStorage),
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

  it('a re-suspension ordinal survives an eviction: the fresh runtime still sees resumeCount 1 on the resuming leg', async () => {
    // #given — a run re-suspended once (ledger: gate -> 1) under runtime A
    const storage = new InMemoryStore();
    const ledgerStorage = fakeLedgerStorage();
    const before = buildDurable(storage, ledgerStorage);
    const started = await before.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('suspended');
    const reSuspended = await before.resume('durable-gate', started.runId, {
      step: 'gate',
    });
    expect(reSuspended.status).toBe('suspended');
    expect(reSuspended.resumeCount?.gate).toBe(1);

    // #when — "eviction": a fresh runtime shares only the Mastra storage and
    // the ctx.storage-backed ledger (fresh in-memory maps), then resumes
    const legs: RunLeg[] = [];
    const after = buildDurable(storage, ledgerStorage, (leg) => legs.push(leg));
    const done = await after.resume('durable-gate', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — the resuming leg carries the persisted ordinal (pre-seam this
    // was undefined, so an approval bound to resumeCount 1 was denied and the
    // approved resume no-oped)
    expect(done.status).toBe('success');
    const resumeLeg = legs.find((leg) => leg.kind === 'resume') as
      | { resumeCount?: number }
      | undefined;
    expect(resumeLeg).toBeDefined();
    expect(resumeLeg?.resumeCount).toBe(1);
  });

  it('drops the persisted ledger entry once the run is terminal', async () => {
    // #given — a run with one re-suspension recorded durably
    const storage = new InMemoryStore();
    const ledgerStorage = fakeLedgerStorage();
    const runtime = buildDurable(storage, ledgerStorage);
    const started = await runtime.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    await runtime.resume('durable-gate', started.runId, { step: 'gate' });
    expect(ledgerStorage.keyCount()).toBe(1);

    // #when — the run completes
    const done = await runtime.resume('durable-gate', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — terminal status reaps the durable entry (no unbounded growth)
    expect(done.status).toBe('success');
    expect(ledgerStorage.keyCount()).toBe(0);
  });

  it('status() reads the ledger only for SUSPENDED runs, and a resume reads it exactly once', async () => {
    // #given — a ctx.storage that counts billed get()s
    const storage = new InMemoryStore();
    const base = fakeLedgerStorage();
    let gets = 0;
    const counting: ResumeLedgerStorage = {
      async get<T>(key: string): Promise<T | undefined> {
        gets += 1;
        return base.get<T>(key);
      },
      put: (key, value) => base.put(key, value),
      delete: (key) => base.delete(key),
    };
    const runtime = buildDurable(storage, counting);
    const started = await runtime.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #when — one resume: the fingerprint read is the ONLY ledger get (the
    // increment rides the prior counts the per-run lock guarantees unchanged)
    const beforeResume = gets;
    const done = await runtime.resume('durable-gate', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });
    expect(done.status).toBe('success');
    expect(gets).toBe(beforeResume + 1);

    // #when — the SPA-style poll of the now-terminal run
    const beforeStatus = gets;
    const status = await runtime.status('durable-gate', started.runId);

    // #then — no billed get for a projection that would discard it...
    expect(status?.status).toBe('success');
    expect(gets).toBe(beforeStatus);

    // ...while a suspended run still projects the ordinal (one read)
    const suspended = await runtime.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    const beforeSuspendedStatus = gets;
    const suspendedStatus = await runtime.status(
      'durable-gate',
      suspended.runId,
    );
    expect(suspendedStatus?.status).toBe('suspended');
    expect(gets).toBe(beforeSuspendedStatus + 1);
  });

  it('projects status() resumeCount from the durable ledger across the rebuild', async () => {
    // #given — a re-suspended run, then an eviction
    const storage = new InMemoryStore();
    const ledgerStorage = fakeLedgerStorage();
    const before = buildDurable(storage, ledgerStorage);
    const started = await before.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    await before.resume('durable-gate', started.runId, { step: 'gate' });

    // #when — a fresh runtime projects status
    const after = buildDurable(storage, ledgerStorage);
    const status = await after.status('durable-gate', started.runId);

    // #then — the ordinal is read back from storage, not from isolate memory
    expect(status?.status).toBe('suspended');
    expect(status?.resumeCount?.gate).toBe(1);
  });

  it("stores counts as entry pairs, so a step keyed '__proto__' is counted, not swallowed", async () => {
    // #given — Record-shaped storage would route a '__proto__' assignment to
    // the prototype setter and silently lose the count; pairs have no
    // reserved names
    const ledger = new DurableStorageResumeLedger(fakeLedgerStorage());

    // #when
    await ledger.increment('wf:run', '__proto__');
    const counts = await ledger.increment('wf:run', '__proto__');

    // #then
    expect(counts.get('__proto__')).toBe(2);
    expect((await ledger.counts('wf:run'))?.get('__proto__')).toBe(2);
  });
});
