// SPDX-License-Identifier: Apache-2.0
// The stream surface every streaming host mounts (DL-010, DL-015, DL-009),
// mirroring createRunRouter's shape: plain fetch routing that returns `null` for
// paths outside its ownership so the composer can compose it, and `Response` for
// everything under `/api/stream/`. Because every route is namespaced under
// `/api/stream/`, the hosts' existing `/api/*` run_worker_first entry already
// routes them — no assets-block edit (DL-015).
//
// The Worker is the SOLE ticket-verification authority. The ticket route
// authenticates through the shared ActorResolver and mints a short-lived HMAC
// ticket; the two WebSocket-upgrade routes verify that ticket HERE and forward
// the raw upgrade to the addressed Durable Object, which re-binds the connection
// by its own idFromName identity. No grant ever crosses this surface — the
// ticket is ADDRESSING only.

import {
  ActorResolutionError,
  type ActorResolver,
} from '../approval-api/index.js';
import {
  isPathSafeId,
  type RunSummary,
  stampDeploymentIdentityRequest,
} from '../do-runner/index.js';
import { readBoundedBody } from '../http-body.js';
import type { RunnerNamespaceLike } from './do-run-topology.js';
import { createHubTopology, type HubNamespaceLike } from './hub-topology.js';
import { requireResourceAccess } from './resource-access.js';
import { RunRouteError } from './run-route-error.js';
import { mintStreamTicket, verifyStreamTicket } from './stream-ticket.js';

export interface StreamRouterOptions {
  /** The shared authenticate-and-validate resolver used by the ticket route. */
  resolve: ActorResolver;
  /** The dedicated stream-ticket signing secret (STREAM_TICKET_SECRET). */
  ticketSecret: string;
  /** The deployment hub namespace the hub channel forwards to. */
  hub: HubNamespaceLike;
  /** The runner namespace the per-run channel forwards to (idFromName wf:runId). */
  runner: RunnerNamespaceLike;
  /** Authoritative workflow/run lookup used before minting a run ticket. */
  runStatus(workflowId: string, runId: string): Promise<RunSummary | undefined>;
  /** Per-deployment Worker-to-DO credential. */
  deploymentIdentitySecret: string;
  /**
   * Optional origin for the ws:// URL returned by the ticket route (e.g.
   * `wss://host`). Omitted => a same-origin relative path the browser resolves
   * against the page origin.
   */
  wsBaseUrl?: string;
}

export type StreamRouter = (request: Request) => Promise<Response | null>;

/**
 * A DO stub that forwards a raw WS-upgrade Request and returns the 101 Response
 * unmodified. The runner's structural RunnerStubLike (do-run-topology.ts) models
 * only the JSON `fetch(url, init)` overload used for start/status/resume; the
 * REAL DurableObjectStub also forwards a raw `Request` -> `Response`, which the
 * WebSocket upgrade needs. This narrow local shape names exactly that overload
 * so the forward is typed without widening the shared RunnerStubLike seam. The
 * hub stub already declares this overload (HubStubLike), so only the runner is
 * coerced.
 */
interface UpgradeForwardStub {
  fetch(request: Request): Promise<Response>;
}

// A ticket is a short-lived credential; no-store keeps it out of shared caches.
function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

async function readJson(request: Request): Promise<unknown> {
  const raw = await readBoundedBody(
    request,
    4096,
    'stream ticket body exceeds limit',
  );
  if (!raw.ok && raw.reason === 'payload-too-large') {
    throw new RunRouteError(413, 'payload too large');
  }
  if (!raw.ok) return null;
  try {
    return raw.text === '' ? null : JSON.parse(raw.text);
  } catch {
    return null;
  }
}

function isWebSocketUpgrade(request: Request): boolean {
  return request.headers.get('upgrade')?.toLowerCase() === 'websocket';
}

const TICKET_TTL_SECONDS = 60;

export function createStreamRouter(options: StreamRouterOptions): StreamRouter {
  const { deploymentIdentitySecret, resolve, ticketSecret, hub, runner } =
    options;
  const base = options.wsBaseUrl ?? '';

  async function mintRoute(request: Request): Promise<Response> {
    const context = await resolve(request);
    if (!context) return json({ error: 'authentication required' }, 401);

    const body = (await readJson(request)) as {
      channel?: unknown;
      runId?: unknown;
      workflowId?: unknown;
    } | null;
    const channel = body?.channel;
    if (channel !== 'hub' && channel !== 'run') {
      return json({ error: "channel must be 'hub' or 'run'" }, 400);
    }

    let runId: string | undefined;
    let workflowId: string | undefined;
    let url: string;
    if (channel === 'run') {
      if (typeof body?.runId !== 'string') {
        return json({ error: 'runId is required for a run ticket' }, 400);
      }
      runId = body.runId;
      if (!isPathSafeId(runId)) {
        return json({ error: 'run not found' }, 404);
      }
      await requireResourceAccess(context, 'run', runId, 'read', 'run');
      if (
        typeof body.workflowId !== 'string' ||
        !isPathSafeId(body.workflowId)
      ) {
        return json({ error: 'workflowId is required for a run ticket' }, 400);
      }
      workflowId = body.workflowId;
      if (!(await options.runStatus(workflowId, runId))) {
        return json({ error: 'run not found' }, 404);
      }
      url = `${base}/api/stream/run/${workflowId}/${runId}`;
    } else {
      url = `${base}/api/stream/hub`;
    }

    const ticket = await mintStreamTicket({
      secret: ticketSecret,
      channel,
      workflowId,
      runId,
      actor: context.actor,
      ttlSeconds: TICKET_TTL_SECONDS,
    });
    return json({
      url,
      ticket,
      expiresAt: Date.now() + TICKET_TTL_SECONDS * 1000,
    });
  }

  async function hubUpgrade(request: Request, url: URL): Promise<Response> {
    if (!isWebSocketUpgrade(request)) {
      return json({ error: 'websocket upgrade required' }, 426);
    }
    const ticket = url.searchParams.get('ticket');
    if (!ticket) return json({ error: 'stream ticket required' }, 401);
    const claims = await verifyStreamTicket({
      secret: ticketSecret,
      token: ticket,
    });
    // Fail closed: an expired/forged/cross-channel ticket never subscribes.
    if (!claims) return json({ error: 'invalid stream ticket' }, 403);
    if (claims.channel !== 'hub') {
      return json({ error: 'invalid stream ticket' }, 403);
    }
    // Rewrite to /subscribe, dropping the ticket and passing the verified
    // actorId/role as query params (the hub reads presence from the query — it
    // performs no verification of its own, trusting the Worker's routing).
    const forwardUrl = new URL(url);
    forwardUrl.pathname = '/subscribe';
    forwardUrl.search = '';
    forwardUrl.searchParams.set('actorId', claims.actorId);
    forwardUrl.searchParams.set('role', claims.role);
    const forwardRequest = new Request(forwardUrl.toString(), request);
    return createHubTopology(hub, deploymentIdentitySecret).forwardSubscribe(
      forwardRequest,
    );
  }

  async function runUpgrade(
    request: Request,
    url: URL,
    workflowId: string,
    runId: string,
  ): Promise<Response> {
    if (!isWebSocketUpgrade(request)) {
      return json({ error: 'websocket upgrade required' }, 426);
    }
    const ticket = url.searchParams.get('ticket');
    if (!ticket) return json({ error: 'stream ticket required' }, 401);
    const claims = await verifyStreamTicket({
      secret: ticketSecret,
      token: ticket,
    });
    // The ticket must be a run ticket for THIS run. Any mismatch fails closed.
    if (!claims) return json({ error: 'invalid stream ticket' }, 403);
    if (
      !isPathSafeId(workflowId) ||
      !isPathSafeId(runId) ||
      claims.channel !== 'run' ||
      claims.workflowId !== workflowId ||
      claims.runId !== runId
    ) {
      return json({ error: 'invalid stream ticket' }, 403);
    }
    const forwardUrl = new URL(url);
    forwardUrl.pathname = `/runs/${workflowId}/${runId}/stream`;
    forwardUrl.search = '';
    const forwardRequest = new Request(forwardUrl.toString(), request);
    // The runner DO is keyed `${wf}:${runId}` (the same instance that runs the
    // workflow). See UpgradeForwardStub for why the stub is coerced here.
    const stub = runner.get(
      runner.idFromName(`${workflowId}:${runId}`),
    ) as unknown as UpgradeForwardStub;
    return stub.fetch(
      stampDeploymentIdentityRequest(forwardRequest, deploymentIdentitySecret),
    );
  }

  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    // Own only /api/stream/*; everything else falls through to the next router.
    if (segments[0] !== 'api' || segments[1] !== 'stream') return null;

    try {
      if (
        request.method === 'POST' &&
        segments.length === 3 &&
        segments[2] === 'ticket'
      ) {
        return await mintRoute(request);
      }
      if (
        request.method === 'GET' &&
        segments.length === 3 &&
        segments[2] === 'hub'
      ) {
        return await hubUpgrade(request, url);
      }
      if (
        request.method === 'GET' &&
        segments.length === 5 &&
        segments[2] === 'run' &&
        segments[3] &&
        segments[4]
      ) {
        return await runUpgrade(request, url, segments[3], segments[4]);
      }
      return json({ error: 'not found' }, 404);
    } catch (error) {
      // Malformed authenticated claims are a verifier bug -> 403 (mirrors run-router);
      // anything else is an unexpected 500.
      if (error instanceof ActorResolutionError) {
        return json({ error: 'forbidden' }, 403);
      }
      if (error instanceof RunRouteError && error.status < 500) {
        return json({ error: error.message }, error.status);
      }
      return json(
        { error: error instanceof Error ? error.message : String(error) },
        500,
      );
    }
  };
}
