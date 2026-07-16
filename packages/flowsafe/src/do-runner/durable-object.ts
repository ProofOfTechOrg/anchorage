// SPDX-License-Identifier: Apache-2.0
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

import type { DurableObjectRunnerState, WebSocketLike } from './cf-types.js';
import { newWebSocketPair, safeSend } from './cf-types.js';
import { doErrorResponse } from './do-error-response.js';
import { tenantOfRunId } from './path-safe-id.js';
import { DurableStorageResumeLedger } from './resume-ledger.js';
import type { RunnerRuntime, RunSummary } from './runtime.js';

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
  protected readonly state?: DurableObjectRunnerState;
  #runtime?: RunnerRuntime;

  constructor(state: DurableObjectRunnerState | undefined, env: TEnv) {
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
      return doErrorResponse(error);
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
      const summary = await runtime.start(body.workflowId, {
        runId: body.runId,
        inputData: body.inputData,
      });
      // DL-018: the authoritative RunSummary is the run-progress frame; push it
      // to any subscribed run-channel socket at this lifecycle boundary.
      this.#broadcastRunSummary(summary);
      return json(summary);
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
      request.method === 'GET' &&
      segments.length === 4 &&
      action === 'stream' &&
      workflowId &&
      runId
    ) {
      const isUpgrade =
        request.headers.get('upgrade')?.toLowerCase() === 'websocket';
      const state = this.state;
      if (!isUpgrade || !state?.acceptWebSocket) {
        // The per-run WS stream needs an Upgrade handshake AND the workerd
        // Hibernatable-WebSocket API; off workerd (node/vitest) or on a plain
        // GET, poll `GET /runs/:workflowId/:runId` instead. Fail with 426
        // (Upgrade Required), never a 500 — the WS path is proven by the spike.
        return json(
          {
            error:
              'websocket upgrade required for run streaming (workerd-only; poll GET /runs/:workflowId/:runId as the fallback)',
          },
          426,
        );
      }
      // The trusted Worker already verified the run ticket and routed by
      // ticket.runId to idFromName; re-bind to this instance's identity (INV-1)
      // before accepting so a mis-routed upgrade is refused.
      this.#assertRunIdentity(workflowId, runId);
      const { 0: client, 1: server } = newWebSocketPair();
      state.acceptWebSocket(server);
      // On-connect snapshot: seed the new subscriber with the current
      // authoritative summary (DL-018) so it need not wait for the next
      // lifecycle transition. Nothing to send if the run is not yet queryable.
      const snapshot = await runtime.status(workflowId, runId);
      if (snapshot) {
        safeSend(server, runFrame(snapshot));
      }
      return new Response(null, {
        status: 101,
        webSocket: client,
      } as unknown as ResponseInit);
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
      const summary = await runtime.resume(workflowId, runId, {
        step: body.step,
        resumeData: body.resumeData,
      });
      // DL-018: broadcast the post-resume authoritative summary.
      this.#broadcastRunSummary(summary);
      return json(summary);
    }
    return json({ error: 'not found' }, 404);
  }

  /**
   * Fan the authoritative RunSummary out to every subscribed run-channel
   * socket (DL-018: at each lifecycle boundary — after start()/resume() — plus
   * the on-connect snapshot). No-op when the DO exposes no getWebSockets
   * (node/vitest, or any host without the Hibernatable-WebSocket API), so the
   * HTTP surface is unchanged off workerd.
   */
  #broadcastRunSummary(summary: RunSummary): void {
    const sockets = this.state?.getWebSockets?.();
    if (!sockets) return;
    const frame = runFrame(summary);
    for (const ws of sockets) {
      safeSend(ws, frame);
    }
  }

  // Hibernation wake handlers — workerd invokes these BY NAME on the instance
  // when a hibernated run-channel socket receives a frame, closes, or errors,
  // so they must exist for a live socket to survive DO eviction. The run
  // channel is broadcast-only (progress flows server->client via
  // #broadcastRunSummary), so there is no client->server protocol beyond a
  // keepalive and no per-socket state to release — a closed socket simply
  // drops out of getWebSockets(). Only exercised under workerd (the spike).
  webSocketMessage(ws: WebSocketLike, message: string | ArrayBuffer): void {
    // Lightweight keepalive: answer a client 'ping' with 'pong'.
    if (message === 'ping') {
      ws.send('pong');
    }
  }

  webSocketClose(_ws: WebSocketLike, _code: number, _reason: string): void {
    // Nothing to reconcile; the closed socket leaves getWebSockets() on its own.
  }

  webSocketError(_ws: WebSocketLike, _error: unknown): void {
    // Nothing to reconcile on a broadcast-only channel.
  }
}

/** The run-channel wire frame — the authoritative RunSummary (DL-018). */
function runFrame(summary: RunSummary): string {
  return JSON.stringify({ type: 'run', summary });
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
