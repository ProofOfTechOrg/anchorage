// Classic fetch-contract Durable Object (constructor(state, env) + fetch).
// Deliberately NOT `extends DurableObject` from 'cloudflare:workers': the
// classic contract needs no workers-only runtime import, so this module and
// everything under it load in node/vitest. Run state lives in D1, not DO
// storage — that is what lets a run started before a restart resume after
// one. Serialization of start/resume on a single run is enforced by
// RunnerRuntime's per-run FIFO lock; routing one DO instance per run
// (idFromName(`${workflowId}:${runId}`)) makes that lock authoritative,
// since all traffic for a run lands on one instance. `state` is captured
// for the Phase 2 alarm-chained engine (setAlarm/alarm) but unused today.

import type { DurableObjectState } from '@cloudflare/workers-types';

import {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunNotSuspendedError,
  type RunnerRuntime,
  UnknownRunError,
  UnknownWorkflowError,
} from './runtime.js';

interface StartBody {
  workflowId?: string;
  runId?: string;
  inputData?: unknown;
}

interface ResumeBody {
  step?: string | string[];
  resumeData?: unknown;
}

export abstract class DurableObjectRunner<TEnv = unknown> {
  protected readonly env: TEnv;
  /** Absent in node tests; present under workerd. Reserved for Phase 2 alarms. */
  protected readonly state?: DurableObjectState;
  #runtime?: RunnerRuntime;

  constructor(state: DurableObjectState | undefined, env: TEnv) {
    this.state = state;
    this.env = env;
  }

  /** Define workflows via init() and return its runtime. Called once, lazily. */
  protected abstract build(env: TEnv): RunnerRuntime;

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.#route(request);
    } catch (error) {
      return errorResponse(error);
    }
  }

  #ensureRuntime(): RunnerRuntime {
    this.#runtime ??= this.build(this.env);
    return this.#runtime;
  }

  async #route(request: Request): Promise<Response> {
    const segments = new URL(request.url).pathname.split('/').filter(Boolean);
    if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);
    const runtime = this.#ensureRuntime();
    const [, workflowId, runId, action] = segments;

    if (request.method === 'POST' && segments.length === 1) {
      const body = await readJson<StartBody>(request);
      if (!body || typeof body.workflowId !== 'string') {
        return json({ error: 'workflowId is required' }, 400);
      }
      return json(
        await runtime.start(body.workflowId, {
          runId: body.runId,
          inputData: body.inputData,
        }),
      );
    }
    if (
      request.method === 'GET' &&
      segments.length === 3 &&
      workflowId &&
      runId
    ) {
      const summary = await runtime.status(workflowId, runId);
      return summary ? json(summary) : json({ error: 'run not found' }, 404);
    }
    if (
      request.method === 'POST' &&
      segments.length === 4 &&
      action === 'resume' &&
      workflowId &&
      runId
    ) {
      const body = (await readJson<ResumeBody>(request)) ?? {};
      return json(
        await runtime.resume(workflowId, runId, {
          step: body.step,
          resumeData: body.resumeData,
        }),
      );
    }
    return json({ error: 'not found' }, 404);
  }
}

async function readJson<T>(request: Request): Promise<T | null> {
  try {
    return (await request.json()) as T;
  } catch {
    return null;
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function errorResponse(error: unknown): Response {
  if (
    error instanceof UnknownWorkflowError ||
    error instanceof UnknownRunError
  ) {
    return json({ error: error.message }, 404);
  }
  if (
    error instanceof RunNotSuspendedError ||
    error instanceof RunAlreadyExistsError
  ) {
    return json({ error: error.message }, 409);
  }
  if (error instanceof InvalidRunRequestError) {
    return json({ error: error.message }, 400);
  }
  return json(
    { error: error instanceof Error ? error.message : String(error) },
    500,
  );
}
