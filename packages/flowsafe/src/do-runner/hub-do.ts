// SPDX-License-Identifier: Apache-2.0
// Deployment hub Durable Object — the fan-out channel for deployment-wide
// approval stream events (queue inserts, claims, decisions, escalations) and
// reviewer presence. Run PROGRESS never flows through here (that rides the
// per-run WS on the runner DO, DL-009); the hub owns the deployment-wide feed
// with no natural per-run home.
//
// Addressed idFromName(HUB_INSTANCE_NAME) by the trusted Worker — ONE hub per
// deployment, the singleton the physical-isolation topology implies (one
// deployment = one organization = one reviewer feed). Classic
// constructor(state,env)+fetch contract — deliberately NOT `extends
// DurableObject` from 'cloudflare:workers' — so this module and its graph load
// in node/vitest; the Hibernatable-WebSocket path (acceptWebSocket + the
// webSocket* handler methods) only runs under workerd, guarded by
// acceptWebSocket presence, and is proven by the workerd spike (M-009). The
// hub holds NO durable D1/DO state: it is an ephemeral fan-out over the
// sockets currently attached, and performs NO ticket verification (the Worker
// is the sole ticket authority).

import type { HubDurableObjectState, WebSocketLike } from './cf-types.js';
import { newWebSocketPair, safeSend } from './cf-types.js';
import { verifyDurableObjectDeploymentRequest } from './deployment-identity.js';
import { doErrorResponse } from './do-error-response.js';

/**
 * The ONE name the trusted Worker addresses the deployment hub by
 * (`createHubTopology`). A fixed name rather than a per-request value: the hub
 * asserts its own `id.name` equals it, so an instance reached around the
 * exported topology (a hand-rolled idFromName with some other name) refuses to
 * serve instead of fanning events out from an unsanctioned identity.
 */
export const HUB_INSTANCE_NAME = 'deployment-hub';

/**
 * Minimal STRUCTURAL shape of the approval stream event this hub fans out.
 *
 * ACYCLIC LAYERING: approval-api already imports FROM do-runner, so do-runner
 * must NOT import ApprovalStreamEvent back from approval-api — that would be a
 * dependency cycle. The hub only re-serializes the whole event to subscribers,
 * so this local subset is sufficient. The real ApprovalStreamEvent
 * (approval-api/contract.ts) is bridged onto the hub's POST body by host-kit
 * through `createHubTopology`, which may import both packages.
 */
export interface HubStreamEvent {
  record: Record<string, unknown>;
  type?: string;
  [key: string]: unknown;
}

/** A reviewer currently subscribed to the hub, held in socket attachment. */
export interface PresenceMember {
  actorId: string;
  role: string;
}

/**
 * Deployment hub Durable Object base. Hosts subclass it (ShowcaseHub /
 * FlowsafeHub / DemoHub) and bind it under the wrangler HUB namespace; the
 * subclass body is typically empty. Left `abstract` to signal "extend, do not
 * instantiate directly".
 */
export abstract class HubDurableObject<TEnv = unknown> {
  protected readonly env: TEnv;
  /** Absent in node tests; present under workerd (Hibernatable-WebSocket API). */
  protected readonly state?: HubDurableObjectState;

  constructor(state: HubDurableObjectState | undefined, env: TEnv) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      // Deployment-identity check before any fan-out: a hub namespace bound to
      // the wrong deployment refuses rather than leaks its feed. No-op off
      // workerd (state undefined), memoized after first success.
      await verifyDurableObjectDeploymentRequest(request, this.state, this.env);
      return await this.#route(request);
    } catch (error) {
      // doErrorResponse keeps the taxonomy consistent with the other DO bases:
      // a DeploymentIdentityError answers 503, everything unrecognized a 500.
      return doErrorResponse(error);
    }
  }

  /**
   * Assert this instance IS the deployment hub — its own idFromName identity
   * equals HUB_INSTANCE_NAME. The trusted Worker only ever addresses that
   * name (`createHubTopology`), so anything else was reached around the
   * exported topology. Refuse loudly rather than fan out from an unsanctioned
   * identity. `id.name` is populated only for idFromName-created ids and is
   * unforgeable at this boundary.
   */
  #assertHubIdentity(): void {
    const name = this.state?.id?.name;
    if (this.state === undefined) return;
    if (name !== HUB_INSTANCE_NAME) {
      throw new Error(
        `HubDurableObject: instance name '${name}' is not the deployment hub '${HUB_INSTANCE_NAME}' — refusing to serve (address the hub through createHubTopology)`,
      );
    }
  }

  async #route(request: Request): Promise<Response> {
    const segments = new URL(request.url).pathname.split('/').filter(Boolean);

    // (1) POST /internal/event — the trusted host forwards each approval stream
    // event here (createHubTopology, M-006). Fan it out to the deployment's
    // subscribed sockets.
    if (
      request.method === 'POST' &&
      segments.length === 2 &&
      segments[0] === 'internal' &&
      segments[1] === 'event'
    ) {
      this.#assertHubIdentity();
      const event = await readJson<HubStreamEvent>(request);
      if (!event || typeof event.record !== 'object' || event.record === null) {
        return json({ error: 'a stream event with a record is required' }, 400);
      }
      const frame = JSON.stringify({ type: 'queue', event });
      for (const ws of this.#sockets()) {
        safeSend(ws, frame);
      }
      return json({ ok: true });
    }

    // (2) GET /subscribe (Upgrade: websocket) — a reviewer dashboard subscribes
    // to the deployment's live feed. Accept a hibernatable socket, record
    // presence in its attachment, and broadcast the refreshed roster. The
    // Worker passes the ticket's actorId/role as query params (it is the sole
    // ticket authority; the hub trusts its routing and identity, M-006).
    if (
      request.method === 'GET' &&
      segments.length === 1 &&
      segments[0] === 'subscribe'
    ) {
      const isUpgrade =
        request.headers.get('upgrade')?.toLowerCase() === 'websocket';
      const state = this.state;
      if (!isUpgrade || !state?.acceptWebSocket) {
        // Subscribing needs an Upgrade handshake AND the workerd Hibernatable-
        // WebSocket API; off workerd (node/vitest) or on a plain GET, the queue
        // poll route is the fallback. Fail with 426, never a 500.
        return json(
          {
            error:
              'websocket upgrade required to subscribe (workerd-only; poll the approval queue route as the fallback)',
          },
          426,
        );
      }
      // Bind this instance to its singleton identity before accepting (fail
      // closed on an unsanctioned instance); the role tag lets a future
      // targeted fan-out address sockets by role.
      this.#assertHubIdentity();
      const url = new URL(request.url);
      const actorId = url.searchParams.get('actorId') ?? 'anonymous';
      const role = url.searchParams.get('role') ?? 'unknown';
      const { 0: client, 1: server } = newWebSocketPair();
      state.acceptWebSocket(server, [HUB_INSTANCE_NAME, role]);
      server.serializeAttachment?.({ actorId, role });
      this.#broadcastPresence();
      return new Response(null, {
        status: 101,
        webSocket: client,
      } as unknown as ResponseInit);
    }

    return json({ error: 'not found' }, 404);
  }

  /** Sockets currently attached to this hub (empty off workerd / in node). */
  #sockets(): WebSocketLike[] {
    return this.state?.getWebSockets?.() ?? [];
  }

  /**
   * Recompute the presence roster from every attached socket's attachment and
   * push it to all of them, so each dashboard sees who else is reviewing. Pass
   * the departing socket as `exclude` on a close/error re-broadcast: workerd
   * may still return it from getWebSockets() mid-close, so excluding it keeps
   * the roster accurate.
   */
  #broadcastPresence(exclude?: WebSocketLike): void {
    const sockets = this.#sockets().filter((ws) => ws !== exclude);
    const roster: PresenceMember[] = [];
    for (const ws of sockets) {
      const attachment = readAttachment(ws);
      if (isPresenceMember(attachment)) {
        roster.push(attachment);
      }
    }
    const frame = JSON.stringify({ type: 'presence', roster });
    for (const ws of sockets) {
      safeSend(ws, frame);
    }
  }

  // Hibernation wake handlers — workerd invokes these BY NAME on the instance
  // when a hibernated socket receives a frame, closes, or errors, so they must
  // exist for a subscriber to survive DO eviction. Only exercised under workerd
  // (the spike); in node the WS path is never reached.
  webSocketMessage(_ws: WebSocketLike, _message: string | ArrayBuffer): void {
    // A client heartbeat/cursor keeps the socket warm; the roster is maintained
    // on connect and close, so no action is needed here.
  }

  webSocketClose(ws: WebSocketLike, _code: number, _reason: string): void {
    // A subscriber left: re-broadcast the roster without the departing socket.
    this.#broadcastPresence(ws);
  }

  webSocketError(ws: WebSocketLike, _error: unknown): void {
    // An errored socket drops: re-broadcast the roster without it.
    this.#broadcastPresence(ws);
  }
}

function isPresenceMember(value: unknown): value is PresenceMember {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as { actorId?: unknown }).actorId === 'string' &&
    typeof (value as { role?: unknown }).role === 'string'
  );
}

/** Read a socket's presence attachment, tolerating a deserialize throw. */
function readAttachment(ws: WebSocketLike): unknown {
  try {
    return ws.deserializeAttachment?.();
  } catch {
    return undefined;
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
