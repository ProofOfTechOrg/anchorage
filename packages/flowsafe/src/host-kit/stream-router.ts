// SPDX-License-Identifier: Apache-2.0
// The stream surface every streaming host mounts (DL-010, DL-015, DL-009),
// mirroring createRunRouter's shape: plain fetch routing that returns `null` for
// paths outside its ownership so the composer can compose it, and `Response` for
// everything under `/api/stream/`. Because every route is namespaced under
// `/api/stream/`, the hosts' existing `/api/*` run_worker_first entry already
// routes them — no assets-block edit (DL-015).
//
// The Worker is the SOLE ticket-verification authority. The ticket route
// authenticates through the shared TenantResolver and mints a short-lived HMAC
// ticket; the two WebSocket-upgrade routes verify that ticket HERE and forward
// the raw upgrade to the addressed Durable Object, which re-binds the connection
// by its own idFromName identity (INV-1 for a run, id.name===tenantId for the
// hub). No grant ever crosses this surface — the ticket is ADDRESSING only.
//
// Ownership mirrors the run router: a run ticket for a run the tenant does not
// own gets 404 (not 403), so the route is never an existence oracle for another
// tenant's runIds.

import {
  TenantResolutionError,
  type TenantResolver,
} from '../approval-api/index.js';
import { tenantOfRunId } from '../do-runner/path-safe-id.js';
import type { RunnerNamespaceLike } from './do-run-topology.js';
import { createHubTopology, type HubNamespaceLike } from './hub-topology.js';
import { mintStreamTicket, verifyStreamTicket } from './stream-ticket.js';

export interface StreamRouterOptions {
  /** The shared authenticate, validate, and tenant-bind resolver used by the ticket route. */
  resolve: TenantResolver;
  /** The dedicated stream-ticket signing secret (STREAM_TICKET_SECRET). */
  ticketSecret: string;
  /** The per-tenant hub namespace the hub channel forwards to. */
  hub: HubNamespaceLike;
  /** The runner namespace the per-run channel forwards to (idFromName wf:runId). */
  runner: RunnerNamespaceLike;
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
  try {
    return await request.json();
  } catch {
    return null;
  }
}

function isWebSocketUpgrade(request: Request): boolean {
  return request.headers.get('upgrade')?.toLowerCase() === 'websocket';
}

const TICKET_TTL_SECONDS = 60;

export function createStreamRouter(options: StreamRouterOptions): StreamRouter {
  const { resolve, ticketSecret, hub, runner } = options;
  const base = options.wsBaseUrl ?? '';

  async function mintRoute(request: Request): Promise<Response> {
    const tenant = await resolve(request);
    if (!tenant) return json({ error: 'authentication required' }, 401);

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
    let url: string;
    if (channel === 'run') {
      if (typeof body?.runId !== 'string') {
        return json({ error: 'runId is required for a run ticket' }, 400);
      }
      runId = body.runId;
      // Ownership, not authorization: a run the tenant does not own is 404, not
      // 403, so the ticket route is not an existence oracle (mirrors run-router).
      if (!tenant.ownsRun(runId)) {
        return json({ error: 'run not found' }, 404);
      }
      // The run WS path needs the workflowId (the DO is keyed `${wf}:${runId}`);
      // the client, which drives the run, knows it. When supplied the returned
      // url is complete; otherwise it carries the runId for the client to
      // qualify. The ticket claims never carry the workflowId — the WS route
      // takes it from the path and binds by claims.runId + tenantOfRunId.
      const workflowId =
        typeof body.workflowId === 'string' ? body.workflowId : undefined;
      url =
        workflowId !== undefined
          ? `${base}/api/stream/run/${workflowId}/${runId}`
          : `${base}/api/stream/run/${runId}`;
    } else {
      url = `${base}/api/stream/hub`;
    }

    const ticket = await mintStreamTicket({
      secret: ticketSecret,
      tenantId: tenant.tenantId,
      channel,
      runId,
      actor: tenant.actor,
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
    return createHubTopology(hub).forwardSubscribe(
      claims.tenantId,
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
    // The ticket must be a run ticket for THIS run, and its tenant must be the
    // one the runId carries (INV-1) — belt over the ownership already baked into
    // the runId prefix. Any mismatch fails closed.
    if (!claims) return json({ error: 'invalid stream ticket' }, 403);
    if (
      claims.channel !== 'run' ||
      claims.runId !== runId ||
      claims.tenantId !== tenantOfRunId(runId)
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
    return stub.fetch(forwardRequest);
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
      // A malformed tenant claim is a verifier bug -> 403 (mirrors run-router);
      // anything else is an unexpected 500.
      if (error instanceof TenantResolutionError) {
        return json({ error: 'forbidden' }, 403);
      }
      return json(
        { error: error instanceof Error ? error.message : String(error) },
        500,
      );
    }
  };
}
