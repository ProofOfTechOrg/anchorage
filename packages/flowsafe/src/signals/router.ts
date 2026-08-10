// SPDX-License-Identifier: Apache-2.0
// Track C (M-004), CI-M-004-004 — the P6 ingestion trust boundary (DL-006).
//
// Signals/notifications/messages inject XML-wrapped content INTO the model's
// context (core's signalToXmlMarkup), so every ingest is an UNTRUSTED input
// channel into the agent. This Worker-side router uses the same resource-first
// authorization rule as addressed run routes:
//
//   1. resolve (authenticate and bind actor context) -> 401 / 403
//   2. registry-backed thread ownership (requireResourceAccess) -> 404 (no existence oracle)
//   3. coarse role (RUN_START_ROLES by default)   -> 403   (reviewer/viewer read-only)
//   4. size cap on the raw body, THEN JSON parse   -> 413 / 400
//   5. body names NO client memory id (assertNoClientMemoryIds) -> 400
//   6. attribute-key allowlist                     -> 400
//   7. deployment rate cap                          -> 429
//   8. audit (signal.ingest) + forward via the topology
//
// Every ingest is AUDITED (signal.ingest), accepted OR rejected — and a rejection
// is audited at the step that refuses it, INCLUDING the three POST-auth denials
// that read like an attack on this untrusted channel: the role 403, the
// malformed thread 404 and the
// memory-id 400 (a smuggled TCB-only id). Pre-auth failures (401, or a resolver
// throw → 403) are NOT audited: the caller is unauthenticated, so auditing there
// would let an anonymous flood write the log.
//
// The threadId travels in the PATH (the client references its OWN thread, like a
// runId on the status/resume routes) and is 404'd if foreign BEFORE any DO is
// addressed — no wake, no oracle. The BODY may never name threadId/resourceId
// (assertNoClientMemoryIds): a client that picks its own memory id picks whose
// memory it reads. The forward goes through createThreadTopology, which
// overwrites the trusted principal header from the resolved context.
//
// XML-injection neutralization is CORE's: signalToXmlMarkup entity-escapes the
// contents and attribute VALUES and re-validates tag/attribute NAMES — a single
// layer, and core is a SOFT pin, so a regression there is caught by the C-S5
// render test (thread-do-routes.test.ts), which fails flowsafe CI if core stops
// escaping. The ROUTE adds its own line but does NOT re-escape the contents: the
// thread routes validate `tagName` as an XML name at ingest, and this gate
// allowlists attribute KEYS and size-caps the payload. A full content-level input
// policy engine is deferred (design it once, after the surfaces exist).
//
// sendToolApproval is deliberately NOT an ingress here (P8): the dashboard stays
// the sole approval decision path; this router never mints capability.

import {
  type ActorContext,
  ActorResolutionError,
  type ActorResolver,
  type ApprovalRole,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import {
  assertNoClientMemoryIds,
  RunRouteError,
  requireResourceAccess,
  type ThreadTopology,
} from '../host-kit/index.js';
import { safeDecodeSegment } from '../host-kit/route-path.js';
import { readBoundedBody } from '../http-body.js';
import { internalErrorResponse } from '../internal-error-response.js';
import { nonnegativeSafeInteger } from '../numeric-config.js';

/** The ingest channels, each mapped to a thread-DO route. */
const CHANNEL_PATHS = {
  signal: '/signal',
  message: '/signal/message',
  queue: '/signal/queue',
  state: '/signal/state',
  notification: '/signal/notification',
} as const;

export type SignalChannel = keyof typeof CHANNEL_PATHS;

/** The structured audit event every ingest emits (accepted OR rejected). */
export interface SignalIngestAuditEvent {
  type: 'signal.ingest';
  deploymentTag?: string;
  actorId: string;
  threadId: string;
  channel: SignalChannel;
  outcome: 'accepted' | 'rejected';
  /** Present for rejected outcomes — WHY the ingest was refused. */
  reason?: string;
  /** The ingest payload size in bytes (what the size cap measured). */
  contentBytes: number;
  timestamp: string;
}

/** The audit seam — a host bridges this to its AuditLogger / SIEM sink. */
export type SignalAuditSink = (
  event: SignalIngestAuditEvent,
) => void | Promise<void>;

/**
 * The deployment rate seam: returns false to REFUSE (over cap). Async so a
 * D1/KV-backed limiter fits. Absent means unmetered.
 */
export type SignalRateLimiter = () => boolean | Promise<boolean>;

export interface SignalRouterOptions {
  /** Authenticate and validate the actor; undefined means 401. */
  resolve: ActorResolver;
  /** The sanctioned reach into a thread DO — stamps the principal header. */
  topology: ThreadTopology;
  /** Who may signal. Default RUN_START_ROLES (operator/admin) — reviewers/viewers are read-only. */
  roles?: readonly ApprovalRole[];
  /** Every ingest is audited through this (accepted + rejected). Absent ⇒ no audit (wire one). */
  audit?: SignalAuditSink;
  /** Deployment rate cap. Absent means unmetered. */
  rateLimit?: SignalRateLimiter;
  /**
   * The attribute KEYS a signal body may carry. When set, an attributes object
   * naming any key outside it is 400'd (defense-in-depth over core's own
   * name validation). Absent ⇒ attributes pass through (the host opted out).
   */
  attributeAllowlist?: readonly string[];
  /**
   * Max ingest payload size in bytes. Must be a nonnegative safe integer; zero
   * denies every non-empty body. Default 16384 (16 KiB).
   */
  maxContentBytes?: number;
  /** Route prefix. Default '/api/threads'. */
  basePath?: string;
}

export type SignalRouter = (request: Request) => Promise<Response | null>;

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

export function createSignalRouter(options: SignalRouterOptions): SignalRouter {
  const { resolve, topology } = options;
  const roles = options.roles ?? RUN_START_ROLES;
  const maxContentBytes = nonnegativeSafeInteger(
    options.maxContentBytes ?? 16_384,
    'signal maxContentBytes',
  );
  const base = options.basePath ?? '/api/threads';
  const allowlist = options.attributeAllowlist
    ? new Set(options.attributeAllowlist)
    : undefined;

  return async (request) => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    // /api/threads/:threadId/:channel — the basePath minus its leading slash is
    // segments[0..n], the threadId next, the channel last.
    const baseSegments = base.split('/').filter(Boolean);
    if (
      segments.length !== baseSegments.length + 2 ||
      baseSegments.some((seg, i) => segments[i] !== seg)
    ) {
      return null;
    }
    // Malformed percent-encoding in the threadId is not a real route target —
    // route-absent, never a pre-auth decodeURIComponent throw out of the handler.
    const threadId = safeDecodeSegment(segments[baseSegments.length]);
    if (threadId === undefined) return null;
    const channelSeg = segments[baseSegments.length + 1] ?? '';
    if (!(channelSeg in CHANNEL_PATHS)) return null;
    const channel = channelSeg as SignalChannel;
    if (request.method !== 'POST') {
      return json({ error: 'method not allowed' }, 405);
    }

    // Hoisted ABOVE the try so the outer catch can audit the POST-auth denials
    // that surface as thrown RunRouteErrors (the target 404, the memory-id
    // 400). `context` is undefined until resolve succeeds and the closure no-ops
    // while it is, so a pre-auth throw is never audited. `contentBytes` is filled
    // at the size-cap step (0 for pre-parse rejections).
    let context: ActorContext | undefined;
    let contentBytes = 0;
    const audit = async (
      outcome: 'accepted' | 'rejected',
      reason?: string,
    ): Promise<void> => {
      if (!options.audit || !context) return;
      await options.audit({
        type: 'signal.ingest',
        ...(context.deploymentTag !== undefined
          ? { deploymentTag: context.deploymentTag }
          : {}),
        actorId: context.actor.id,
        threadId,
        channel,
        outcome,
        ...(reason !== undefined ? { reason } : {}),
        contentBytes,
        timestamp: new Date().toISOString(),
      });
    };

    try {
      // 1. Resolve and authenticate. ActorResolutionError maps to 403 in the
      // catch and is not audited (pre-auth).
      context = await resolve(request);
      if (!context) return json({ error: 'authentication required' }, 401);
      const actor = context.actor;

      // 2. Resolve ownership before the role gate. A foreign opaque id and a
      // missing one are the same 404, including for a read-only role.
      await requireResourceAccess(
        context,
        'thread',
        threadId,
        'write',
        'thread',
      );

      // 3. Coarse role: signalling mutates agent context.
      if (!roles.includes(actor.role)) {
        await audit('rejected', 'forbidden-role');
        return json({ error: 'forbidden' }, 403);
      }

      // 4. Size cap at the wire: read the body as text, bound it, THEN parse. A
      // 16 KiB signal is generous; an unbounded one is a context-stuffing vector.
      const rawBody = await readBoundedBody(
        request,
        maxContentBytes,
        'signal body exceeds limit',
      );
      if (!rawBody.ok && rawBody.reason === 'payload-too-large') {
        await audit('rejected', 'payload-too-large');
        return json(
          { error: `signal payload exceeds ${maxContentBytes} bytes` },
          413,
        );
      }
      if (!rawBody.ok) {
        await audit('rejected', 'malformed-body');
        return json({ error: 'a JSON object body is required' }, 400);
      }
      contentBytes = rawBody.bytes.byteLength;
      let body: Record<string, unknown>;
      try {
        const parsed = rawBody.text === '' ? {} : JSON.parse(rawBody.text);
        if (
          typeof parsed !== 'object' ||
          parsed === null ||
          Array.isArray(parsed)
        ) {
          await audit('rejected', 'malformed-body');
          return json({ error: 'a JSON object body is required' }, 400);
        }
        body = parsed as Record<string, unknown>;
      } catch {
        await audit('rejected', 'malformed-body');
        return json({ error: 'a JSON object body is required' }, 400);
      }

      // 5. No client memory id ANYWHERE in the body (assertNoClientMemoryIds 400s).
      assertNoClientMemoryIds(body);

      // 6. Attribute-key allowlist (defense-in-depth over core's name validation).
      if (allowlist && body.attributes !== undefined) {
        const attrs = body.attributes;
        if (
          typeof attrs !== 'object' ||
          attrs === null ||
          Array.isArray(attrs)
        ) {
          await audit('rejected', 'malformed-attributes');
          return json({ error: 'attributes must be an object' }, 400);
        }
        const offending = Object.keys(attrs).find((key) => !allowlist.has(key));
        if (offending !== undefined) {
          await audit('rejected', `attribute-not-allowlisted:${offending}`);
          return json(
            { error: `attribute '${offending}' is not allowlisted` },
            400,
          );
        }
      }

      // 7. Deployment rate cap.
      if (options.rateLimit) {
        const allowed = await options.rateLimit();
        if (!allowed) {
          await audit('rejected', 'rate-limited');
          return json({ error: 'rate limit exceeded' }, 429);
        }
      }

      // 8. Audit the accepted ingest, then forward through the topology (which
      // overwrites the principal header — a forged one cannot ride along).
      await audit('accepted');
      return await topology.send(context, threadId, CHANNEL_PATHS[channel], {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: rawBody.text === '' ? '{}' : rawBody.text,
      });
    } catch (error) {
      if (error instanceof RunRouteError) {
        // A post-auth denial: the target 404 or a smuggled memory-id 400.
        // audit the rejection before mapping the status the router surfaces.
        await audit(
          'rejected',
          error.status === 404
            ? 'invalid-thread'
            : error.status === 400
              ? 'client-memory-id'
              : `route-error-${error.status}`,
        );
        return json({ error: error.message }, error.status);
      }
      if (error instanceof ActorResolutionError) {
        // Pre-auth (the resolver itself threw): unauthenticated, so not audited.
        return json({ error: 'forbidden' }, 403);
      }
      return internalErrorResponse('signals.ingest', error);
    }
  };
}

/**
 * A minimal in-memory fixed-window deployment rate limiter — the default a
 * single-instance host can wire without a store. NOT cross-isolate: a
 * DO-per-run/thread host that needs a shared window uses a D1/KV-backed limiter
 * behind the same `SignalRateLimiter` seam (the store's reach IS the cap's
 * reach, exactly as breakwater's rate-limit policy documents).
 */
export function createInMemorySignalRateLimiter(config: {
  limit: number;
  windowMs: number;
  now?: () => number;
}): SignalRateLimiter {
  const now = config.now ?? Date.now;
  let window: { count: number; resetAt: number } | undefined;
  return () => {
    const current = now();
    if (!window || current >= window.resetAt) {
      window = { count: 1, resetAt: current + config.windowMs };
      return true;
    }
    if (window.count >= config.limit) return false;
    window.count += 1;
    return true;
  };
}
