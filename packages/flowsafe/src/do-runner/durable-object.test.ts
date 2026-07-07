import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { DurableObjectRunner } from './durable-object.js';
import { init } from './init.js';
import type { RunnerRuntime, RunSummary } from './runtime.js';

interface TestEnv {
  storage: InMemoryStore;
}

class TestRunner extends DurableObjectRunner<TestEnv> {
  protected build(env: TestEnv): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init({
      storage: env.storage,
    });
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string(), approvedBy: z.string() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ approvedBy: z.string() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'awaiting approval' });
        return { topic: inputData.topic, approvedBy: resumeData.approvedBy };
      },
    });
    createWorkflow({
      id: 'gated',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string(), approvedBy: z.string() }),
    })
      .then(gate)
      .commit();
    return runtime;
  }
}

function makeRunner(): TestRunner {
  return new TestRunner(undefined, { storage: new InMemoryStore() });
}

function post(path: string, body: unknown): Request {
  return new Request(`http://do${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

async function startGated(runner: TestRunner): Promise<RunSummary> {
  const response = await runner.fetch(
    post('/runs', { workflowId: 'gated', inputData: { topic: 't' } }),
  );
  return (await response.json()) as RunSummary;
}

describe('DurableObjectRunner.fetch', () => {
  it('starts, reports, and resumes a run over the HTTP surface', async () => {
    // #given
    const runner = makeRunner();

    // #when — start
    const startResponse = await runner.fetch(
      post('/runs', { workflowId: 'gated', inputData: { topic: 't' } }),
    );

    // #then
    expect(startResponse.status).toBe(200);
    const started = (await startResponse.json()) as RunSummary;
    expect(started.status).toBe('suspended');

    // #when — status
    const statusResponse = await runner.fetch(
      new Request(`http://do/runs/gated/${started.runId}`),
    );

    // #then
    expect(((await statusResponse.json()) as RunSummary).status).toBe(
      'suspended',
    );

    // #when — resume
    const resumeResponse = await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { approvedBy: 'bob' },
      }),
    );

    // #then
    expect(resumeResponse.status).toBe(200);
    const resumed = (await resumeResponse.json()) as RunSummary;
    expect(resumed.status).toBe('success');
    expect(resumed.result).toEqual({ topic: 't', approvedBy: 'bob' });
  });

  it('returns 400 when workflowId is missing from a start request', async () => {
    // #when
    const response = await makeRunner().fetch(post('/runs', { inputData: {} }));

    // #then
    expect(response.status).toBe(400);
  });

  it('returns 404 for an unknown workflow', async () => {
    // #when
    const response = await makeRunner().fetch(
      post('/runs', { workflowId: 'nope' }),
    );

    // #then
    expect(response.status).toBe(404);
  });

  it('returns 404 when resuming an unknown run', async () => {
    // #when
    const response = await makeRunner().fetch(
      post('/runs/gated/absent/resume', {}),
    );

    // #then
    expect(response.status).toBe(404);
  });

  it('returns 404 for unknown routes', async () => {
    // #when
    const response = await makeRunner().fetch(new Request('http://do/other'));

    // #then
    expect(response.status).toBe(404);
  });

  it('returns 409 when resuming a run that already completed', async () => {
    // #given
    const runner = makeRunner();
    const started = await startGated(runner);
    await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { approvedBy: 'b' },
      }),
    );

    // #when
    const again = await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { approvedBy: 'b' },
      }),
    );

    // #then
    expect(again.status).toBe(409);
  });

  it('returns 409 when starting a run with an already-used runId', async () => {
    // #given
    const runner = makeRunner();
    await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'dup',
        inputData: { topic: 'x' },
      }),
    );

    // #when
    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'dup',
        inputData: { topic: 'y' },
      }),
    );

    // #then
    expect(response.status).toBe(409);
  });

  it('returns 400 when the resume body fails the resume schema', async () => {
    // #given
    const runner = makeRunner();
    const started = await startGated(runner);

    // #when — an array body yields no resumeData, failing the zod schema
    const response = await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, [1, 2, 3]),
    );

    // #then — client error, not a 500
    expect(response.status).toBe(400);
  });

  it('serializes concurrent resumes: one 200, one 409', async () => {
    // #given
    const runner = makeRunner();
    const started = await startGated(runner);

    // #when — two racing HTTP resumes for the same run
    const [first, second] = await Promise.all([
      runner.fetch(
        post(`/runs/gated/${started.runId}/resume`, {
          step: 'gate',
          resumeData: { approvedBy: 'alice' },
        }),
      ),
      runner.fetch(
        post(`/runs/gated/${started.runId}/resume`, {
          step: 'gate',
          resumeData: { approvedBy: 'bob' },
        }),
      ),
    ]);

    // #then
    expect([first.status, second.status].sort()).toEqual([200, 409]);
  });

  it('returns 400 when starting with a non-path-safe runId', async () => {
    // #given — ids that would break the URL path, the ':'-joined DO name,
    // percent-encoding, or normalize away as dot-segments, leaving the run
    // unaddressable
    const runner = makeRunner();

    // #when / #then — rejected at the mint boundary, before any run exists
    for (const runId of ['a b', 'a/b', 'a%2Fb', 'a:b', '.', '..']) {
      const response = await runner.fetch(
        post('/runs', {
          workflowId: 'gated',
          runId,
          inputData: { topic: 't' },
        }),
      );
      expect(response.status).toBe(400);
    }
  });

  it('returns 400 when starting with a non-string runId', async () => {
    // #given — a JSON start body reaches runtime.start through an unchecked
    // `as` cast, so runId can be any JSON type at runtime. RegExp.test() coerces
    // its argument (123 → '123', ['run-1'] → 'run-1'), so without a type guard
    // these pass the pattern and mint a run keyed by the raw value —
    // unreachable by the string the URL path carries (reported repro:
    // POST {runId:123} → 200, then GET /runs/gated/123 → 404).
    const runner = makeRunner();

    // #when / #then — every supplied non-string is rejected before a run exists
    for (const runId of [123, true, ['run-1']]) {
      const response = await runner.fetch(
        post('/runs', {
          workflowId: 'gated',
          runId,
          inputData: { topic: 't' },
        }),
      );
      expect(response.status).toBe(400);
    }
  });

  it('treats a null runId like an omitted one and generates a fresh id', async () => {
    // #given — JSON has no undefined, so a client with no id sends `null`; it
    // must behave like omitting runId (generate one), not 400. The non-string
    // guard must not reject this otherwise-valid request.
    const runner = makeRunner();

    // #when
    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: null,
        inputData: { topic: 't' },
      }),
    );

    // #then — a run was minted under a generated string id
    expect(response.status).toBe(200);
    const summary = (await response.json()) as RunSummary;
    expect(typeof summary.runId).toBe('string');
    expect(summary.runId.length).toBeGreaterThan(0);
  });

  it('round-trips a path-safe custom runId over status and resume', async () => {
    // #given — a caller-supplied URL-safe id must stay addressable end-to-end
    const runner = makeRunner();
    const startResponse = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-1',
        inputData: { topic: 't' },
      }),
    );
    const started = (await startResponse.json()) as RunSummary;
    expect(started.runId).toBe('run-1');

    // #when — address the run by its custom id
    const statusResponse = await runner.fetch(
      new Request('http://do/runs/gated/run-1'),
    );
    const resumeResponse = await runner.fetch(
      post('/runs/gated/run-1/resume', {
        step: 'gate',
        resumeData: { approvedBy: 'bob' },
      }),
    );

    // #then
    expect(((await statusResponse.json()) as RunSummary).status).toBe(
      'suspended',
    );
    expect(((await resumeResponse.json()) as RunSummary).status).toBe(
      'success',
    );
  });
});
