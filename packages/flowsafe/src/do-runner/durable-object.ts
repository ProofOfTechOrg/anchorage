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

import { tenantOfRunId } from './path-safe-id.js';
import { DurableStorageResumeLedger } from './resume-ledger.js';
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

  /**
   * The tenant this instance serves, recovered from the DO's OWN identity.
   * The name was set by the trusted Worker via
   * idFromName(`${workflowId}:${runId}`) and is unforgeable at this boundary
   * (`id.name` is populated only for idFromName-created ids). Safe to parse:
   * PATH_SAFE_ID_PATTERN excludes ':' from workflowId, so the first ':' is
   * the join; INV-3 excludes '_' from tenantId, so the first '_' in the runId
   * is the tenant boundary.
   *
   * THROWS rather than defaulting — an unscoped grant store is a cross-tenant
   * capability mint. Node tests that exercise tenant-bound wiring pass a stub
   * state `{ id: { name: 'wf:tenant_uuid' } }`; never soften this to a
   * default for the workerd path.
   */
  protected get tenantId(): string {
    const name = this.state?.id?.name;
    if (!name) {
      throw new Error(
        'DurableObjectRunner.tenantId: the DO has no id.name (not created via idFromName, or running without state) — tenant unresolvable, refusing to scope',
      );
    }
    const runId = name.slice(name.indexOf(':') + 1);
    const tenantId = tenantOfRunId(runId);
    if (tenantId === undefined) {
      throw new Error(
        `DurableObjectRunner.tenantId: runId '${runId}' carries no INV-3 tenant segment (INV-1 runIds are \`\${tenantId}_\${uuid}\`)`,
      );
    }
    return tenantId;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.#route(request);
    } catch (error) {
      return errorResponse(error);
    }
  }

  #ensureRuntime(): RunnerRuntime {
    if (!this.#runtime) {
      this.#runtime = this.build(this.env);
      // Durable resume ledger: ctx.storage survives eviction, hibernation,
      // and code deploys; in-memory class state does not. Adopting it HERE —
      // not via a host-threaded parameter — is what makes the guarantee
      // un-forgettable: every DO host gets eviction-proof resume ordinals.
      // Absent under node/vitest (state is undefined), the runtime keeps its
      // in-memory default.
      const storage = this.state?.storage;
      if (storage) {
        this.#runtime.adoptDefaultResumeLedger(
          new DurableStorageResumeLedger(storage),
        );
      }
    }
    return this.#runtime;
  }

  // INV-1 enforcement at the DO boundary: this instance was addressed as
  // idFromName(`${workflowId}:${runId}`) by the trusted Worker, and id.name
  // is unforgeable at this boundary. If a request asks the instance to act on
  // a DIFFERENT (workflowId, runId), someone routed around the name join —
  // acting on the request's ids would run outside this instance's identity
  // (and outside its per-run serialization). Refuse loudly. `id.name` is only
  // populated for idFromName-created ids; it is absent under node tests
  // (state undefined / minimal stubs), where the runtime's own validation
  // still applies.
  #assertRunIdentity(workflowId: string, runId: string): void {
    const name = this.state?.id?.name;
    if (name === undefined) return;
    if (name !== `${workflowId}:${runId}`) {
      throw new Error(
        `DO identity mismatch: instance is '${name}' but the request names '${workflowId}:${runId}' — refusing (INV-1)`,
      );
    }
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
      // The DO never generates a runId (INV-1): the trusted Worker mints the
      // tenant-salted id and addresses this instance with it. A start without
      // one is a caller bug, not a request for generation.
      if (typeof body.runId !== 'string') {
        return json(
          { error: 'runId is required (server-minted by the run router)' },
          400,
        );
      }
      this.#assertRunIdentity(body.workflowId, body.runId);
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
      this.#assertRunIdentity(workflowId, runId);
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
      this.#assertRunIdentity(workflowId, runId);
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
