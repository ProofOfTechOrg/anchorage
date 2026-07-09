import type { DurableObjectState } from '@cloudflare/workers-types';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { DurableObjectRunner } from './durable-object.js';
import { init } from './init.js';
import type { ResumeLedgerStorage } from './resume-ledger.js';
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
    post('/runs', {
      workflowId: 'gated',
      runId: crypto.randomUUID(),
      inputData: { topic: 't' },
    }),
  );
  return (await response.json()) as RunSummary;
}

describe('DurableObjectRunner.fetch', () => {
  it('starts, reports, and resumes a run over the HTTP surface', async () => {
    // #given
    const runner = makeRunner();

    // #when — start (the Worker mints the runId; the DO never generates)
    const startResponse = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-http-1',
        inputData: { topic: 't' },
      }),
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
      post('/runs', { workflowId: 'nope', runId: 'r-nope' }),
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

  it('400s a start without a runId — the DO never generates one (INV-1)', async () => {
    // #given — the runId is the tenant carrier, minted by the run router
    // from the AUTHENTICATED tenant. A DO-side generation fallback would let
    // any caller that skips the router mint a bare, tenant-less run.
    const runner = makeRunner();

    // #when / #then — omitted and JSON-null both refuse
    for (const body of [
      { workflowId: 'gated', inputData: { topic: 't' } },
      { workflowId: 'gated', runId: null, inputData: { topic: 't' } },
    ]) {
      const response = await runner.fetch(post('/runs', body));
      expect(response.status).toBe(400);
    }
  });

  it('refuses to act outside its own identity when id.name is present (INV-1)', async () => {
    // #given — a runner whose DO identity names a DIFFERENT run than the
    // request. id.name is set by the trusted Worker via idFromName and is
    // unforgeable at this boundary, so a mismatch means someone routed
    // around the name join.
    const state = {
      id: { name: 'gated:run-A' },
    } as unknown as DurableObjectState;
    const runner = new TestRunner(state, { storage: new InMemoryStore() });

    // #when — the request names run-B on the instance whose identity is run-A
    const mismatched = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-B',
        inputData: { topic: 't' },
      }),
    );

    // #then — refused loudly; and the matching id works normally
    expect(mismatched.status).toBe(500);
    expect(
      (((await mismatched.json()) as { error?: string }).error ?? '').includes(
        'identity mismatch',
      ),
    ).toBe(true);
    const matching = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-A',
        inputData: { topic: 't' },
      }),
    );
    expect(matching.status).toBe(200);

    // #then — status and resume enforce the same identity
    expect(
      (await runner.fetch(new Request('http://do/runs/gated/run-B'))).status,
    ).toBe(500);
    expect(
      (await runner.fetch(post('/runs/gated/run-B/resume', {}))).status,
    ).toBe(500);
  });

  it('adopts a ctx.storage-backed resume ledger: the ordinal survives a DO instance swap', async () => {
    // #given — a runner whose state carries (fake) ctx.storage. The gate here
    // re-suspends on a falsy resume (schema-less), so the run can accrue a
    // resume ordinal.
    class ResuspendRunner extends DurableObjectRunner<TestEnv> {
      protected build(env: TestEnv): RunnerRuntime {
        const { createWorkflow, createStep, runtime } = init({
          storage: env.storage,
        });
        const gate = createStep({
          id: 'gate',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          suspendSchema: z.object({ reason: z.string() }),
          execute: async ({ resumeData, suspend }) =>
            resumeData ? {} : suspend({ reason: 'wait' }),
        });
        createWorkflow({
          id: 'resuspend',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
        })
          .then(gate)
          .commit();
        return runtime;
      }
    }
    const persisted = new Map<string, unknown>();
    const ledgerStorage: ResumeLedgerStorage = {
      async get<T>(key: string): Promise<T | undefined> {
        return persisted.get(key) as T | undefined;
      },
      async put<T>(key: string, value: T): Promise<void> {
        persisted.set(key, value);
      },
      async delete(key: string): Promise<boolean> {
        return persisted.delete(key);
      },
    };
    // Minimal stub: the shell only touches state.storage (and, for tenant
    // recovery, state.id.name — not exercised here).
    const state = {
      storage: ledgerStorage,
    } as unknown as DurableObjectState;
    const env: TestEnv = { storage: new InMemoryStore() };

    // #given — instance A re-suspends the run once (falsy resume)
    const before = new ResuspendRunner(state, env);
    const startResponse = await before.fetch(
      post('/runs', {
        workflowId: 'resuspend',
        runId: 'run-ledger-1',
        inputData: {},
      }),
    );
    const started = (await startResponse.json()) as RunSummary;
    expect(started.status).toBe('suspended');
    const reSuspendResponse = await before.fetch(
      post(`/runs/resuspend/${started.runId}/resume`, { step: 'gate' }),
    );
    const reSuspended = (await reSuspendResponse.json()) as RunSummary;
    expect(reSuspended.status).toBe('suspended');
    expect(reSuspended.resumeCount?.gate).toBe(1);

    // #when — "eviction": a NEW runner instance over the same env storage and
    // the same (surviving) ctx.storage projects the run's status
    const after = new ResuspendRunner(state, env);
    const statusResponse = await after.fetch(
      new Request(`http://do/runs/resuspend/${started.runId}`),
    );
    const status = (await statusResponse.json()) as RunSummary;

    // #then — the ordinal came back from ctx.storage; with the in-memory
    // default it would be undefined and an approval bound to this
    // re-suspension could never mint again
    expect(status.status).toBe('suspended');
    expect(status.resumeCount?.gate).toBe(1);

    // #then — the swapped instance completes the run normally
    const doneResponse = await after.fetch(
      post(`/runs/resuspend/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { go: true },
      }),
    );
    expect(((await doneResponse.json()) as RunSummary).status).toBe('success');
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
