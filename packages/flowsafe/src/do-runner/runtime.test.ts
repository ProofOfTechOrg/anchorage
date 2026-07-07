import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { init } from './init.js';
import {
  InvalidRunRequestError,
  type RequestContextProvider,
  RunAlreadyExistsError,
  type RunLeg,
  type RunnerRuntime,
  RunNotSuspendedError,
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

describe('RunnerRuntime', () => {
  it('runs a workflow to suspension and resumes it to success', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when
    const started = await runtime.start('demo-approval', {
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
    const done = await runtime.start('echo', { inputData: { value: 'hi' } });

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
    await expect(runtime.start('nope', {})).rejects.toBeInstanceOf(
      UnknownWorkflowError,
    );
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

  it.each(['team:wf', 'a/b', '.', '..', ''])(
    "rejects non-path-safe workflow id '%s' at registration",
    (id) => {
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
    },
  );

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
    await runtime.start('wf', { inputData: {} });

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
  it('projects suspended detail — paths, payload, timestamps — from the snapshot', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
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
    const done = await runtime.start('echo', { inputData: { value: 'hi' } });

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
    const started = await runtime.start('failing', { inputData: {} });
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
    const started = await runtime.start('obj-failing', { inputData: {} });

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
    const started = await runtime.start('parallel-suspend', { inputData: {} });
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
    const started = await runtime.start('probe', { inputData: {} });
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
    const started = await runtime.start('probe', { inputData: {} });
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
    await expect(runtime.start('probe', { inputData: {} })).rejects.toThrow(
      'grant store down',
    );
  });

  it('passes the execution leg: start, then resume with the explicit step', async () => {
    // #given
    const legs: RunLeg[] = [];
    const { runtime } = buildContextProbe((_workflowId, _runId, leg) => {
      legs.push(leg);
      return undefined;
    });

    // #when
    const started = await runtime.start('probe', { inputData: {} });
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
    const started = await runtime.start('probe', { inputData: {} });

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
    await runtime.start('scoped-wf', { inputData: {} });

    // #then — the executing workflow's own id, minted by the runtime
    expect(seen).toEqual(['scoped-wf']);
  });

  it('lets a provider override the minted workflow scope (merge-over)', async () => {
    // #given — a provider that deliberately overrides the scope key
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
    await runtime.start('scoped-wf', { inputData: {} });

    // #then — provider values merge OVER the runtime base
    expect(seen).toEqual(['overridden']);
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
