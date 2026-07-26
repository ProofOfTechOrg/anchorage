// SPDX-License-Identifier: Apache-2.0
// Per-tenant hub Durable Object — the fan-out channel for tenant-wide approval
// stream events (queue inserts, claims, decisions, escalations) and reviewer
// presence. Run PROGRESS never flows through here (that rides the per-run WS on
// the runner DO, DL-009); the hub owns the tenant-wide feed with no natural
// per-run home.
//
// Addressed idFromName(tenantId) by the trusted Worker, so `id.name` IS the
// bare tenantId — no ':' join and no runId decode, unlike DurableObjectRunner
// whose name is `${workflowId}:${runId}`. Classic constructor(state,env)+fetch
// contract — deliberately NOT `extends DurableObject` from 'cloudflare:workers'
// — so this module and its graph load in node/vitest; the Hibernatable-
// WebSocket path (acceptWebSocket + the webSocket* handler methods) only runs
// under workerd, guarded by acceptWebSocket presence, and is proven by the
// workerd spike (M-009), not node tests. The hub holds NO durable D1/DO state:
// it is an ephemeral fan-out over the sockets currently attached, and performs
// NO ticket verification (the Worker is the sole ticket authority and routes by
// ticket.tenantId to idFromName(tenantId), so id.name === tenantId here by
// construction).

import type { HubDurableObjectState, WebSocketLike } from './cf-types.js';
import { newWebSocketPair, safeSend } from './cf-types.js';

/**
 * Minimal STRUCTURAL shape of the approval stream event this hub fans out.
 *
 * ACYCLIC LAYERING: approval-api already imports FROM do-runner (e.g.
 * TENANT_ID_PATTERN), so do-runner must NOT import ApprovalStreamEvent back
 * from approval-api — that would be a dependency cycle. The hub only reads
 * `event.record.tenantId` (the defense-in-depth tenant assertion) and
 * re-serializes the whole event to subscribers, so this local subset is
 * sufficient. The real ApprovalStreamEvent (approval-api/contract.ts) is
 * bridged onto the hub's POST body by host-kit through `createHubTopology`,
 * which may import both packages.
 */
export interface HubStreamEvent {
  record: { tenantId: string };
  type?: string;
  [key: string]: unknown;
}

/** A reviewer currently subscribed to a tenant's hub, held in socket attachment. */
export interface PresenceMember {
  actorId: string;
  role: string;
}

/**
 * Per-tenant hub Durable Object base. Hosts subclass it (ShowcaseHub /
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

  /**
   * The tenant this hub serves, recovered from the DO's OWN identity. The
   * trusted Worker addressed this instance via idFromName(tenantId), so
   * `id.name` IS the bare tenantId (no ':' join, no runId decode). `id.name`
   * is populated only for idFromName-created ids and is unforgeable at this
   * boundary.
   *
   * THROWS rather than defaulting — a hub that cannot resolve its tenant must
   * refuse to fan out, never fan one tenant's event to another's sockets.
   * Mirrors DurableObjectRunner.tenantId's fail-closed posture.
   */
  protected get tenantId(): string {
    const name = this.state?.id?.name;
    if (!name) {
      throw new Error(
        'HubDurableObject.tenantId: the DO has no id.name (not created via idFromName, or running without state) — tenant unresolvable, refusing to fan out',
      );
    }
    return name;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.#route(request);
    } catch (error) {
      return json(
        { error: error instanceof Error ? error.message : String(error) },
        500,
      );
    }
  }

  async #route(request: Request): Promise<Response> {
    const segments = new URL(request.url).pathname.split('/').filter(Boolean);

    // (1) POST /internal/event — the trusted host forwards each approval stream
    // event here (createHubTopology, M-006). Fan it out to this tenant's
    // subscribed sockets.
    if (
      request.method === 'POST' &&
      segments.length === 2 &&
      segments[0] === 'internal' &&
      segments[1] === 'event'
    ) {
      const event = await readJson<HubStreamEvent>(request);
      if (!event || typeof event.record?.tenantId !== 'string') {
        return json(
          { error: 'a stream event with record.tenantId is required' },
          400,
        );
      }
      // Defense in depth: the Worker already routes by record.tenantId to
      // idFromName(tenantId), so id.name === the event tenant by construction.
      // Refuse a mismatch loudly rather than fan one tenant's event to
      // another's sockets.
      const tenantId = this.tenantId;
      if (event.record.tenantId !== tenantId) {
        return json(
          {
            error: `event tenant '${event.record.tenantId}' does not match this hub '${tenantId}'`,
          },
          400,
        );
      }
      const frame = JSON.stringify({ type: 'queue', event });
      for (const ws of this.#sockets()) {
        safeSend(ws, frame);
      }
      return json({ ok: true });
    }

    // (2) GET /subscribe (Upgrade: websocket) — a reviewer dashboard subscribes
    // to this tenant's live feed. Accept a hibernatable socket, record presence
    // in its attachment, and broadcast the refreshed roster. The Worker passes
    // the ticket's actorId/role as query params (it is the sole ticket
    // authority; the hub trusts its routing and identity, M-006).
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
      // Bind this instance to its tenant before accepting (fail closed if the
      // hub cannot resolve its identity); the tag lets a future targeted
      // fan-out address sockets by tenant/role.
      const tenantId = this.tenantId;
      const url = new URL(request.url);
      const actorId = url.searchParams.get('actorId') ?? 'anonymous';
      const role = url.searchParams.get('role') ?? 'unknown';
      const { 0: client, 1: server } = newWebSocketPair();
      state.acceptWebSocket(server, [tenantId, role]);
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
