// SPDX-License-Identifier: Apache-2.0

import { captureActorContext } from '../approval-api/actor-context.js';
import {
  type ActorContext,
  ActorResolutionError,
  type ActorResolver,
  type ApprovalRole,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import { hostErrorText } from '../host-kit/host-approval-service.js';
import {
  assertNoClientMemoryIds,
  type BoundThreadTargetValidator,
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

/** A structured signal ingestion outcome. */
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
  /** Require a bound thread before ingestion. Omission uses registry access policy. */
  validateThreadTarget?: BoundThreadTargetValidator;
  /** Who may signal. Default RUN_START_ROLES. */
  roles?: readonly ApprovalRole[];
  /** Receives authenticated ingestion outcomes; sink failures do not change responses. */
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
  const { resolve, topology, validateThreadTarget, audit: auditSink } = options;
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
    if (!Object.hasOwn(CHANNEL_PATHS, channelSeg)) return null;
    const channel = channelSeg as SignalChannel;
    if (request.method !== 'POST') {
      return json({ error: 'method not allowed' }, 405);
    }

    let context: ActorContext | undefined;
    let contentBytes = 0;
    const audit = async (
      outcome: 'accepted' | 'rejected',
      reason?: string,
    ): Promise<void> => {
      if (!auditSink || !context) return;
      try {
        await auditSink.call(options, {
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
      } catch (error) {
        try {
          console.error(
            JSON.stringify({
              type: 'signal.ingest-audit-error',
              threadId,
              channel,
              reason: hostErrorText(error, true),
            }),
            error,
          );
        } catch {
          // Diagnostics cannot change the request's selected outcome.
        }
      }
    };

    try {
      const resolved = await resolve(request);
      if (!resolved) return json({ error: 'authentication required' }, 401);
      context = captureActorContext(resolved);
      const actor = context.actor;

      await requireResourceAccess(
        context,
        'thread',
        threadId,
        'write',
        'thread',
      );
      await validateThreadTarget?.call(options, context, { threadId });

      if (!roles.includes(actor.role)) {
        await audit('rejected', 'forbidden-role');
        return json({ error: 'forbidden' }, 403);
      }

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

      assertNoClientMemoryIds(body);

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

      if (options.rateLimit) {
        const allowed = await options.rateLimit();
        if (!allowed) {
          await audit('rejected', 'rate-limited');
          return json({ error: 'rate limit exceeded' }, 429);
        }
      }

      const response = await topology.send(
        context,
        threadId,
        CHANNEL_PATHS[channel],
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: rawBody.text === '' ? '{}' : rawBody.text,
        },
      );
      await audit(
        response.ok ? 'accepted' : 'rejected',
        response.ok
          ? undefined
          : response.status === 404
            ? 'invalid-thread'
            : `downstream-${response.status}`,
      );
      return response.status === 404
        ? json({ error: 'thread not found' }, 404)
        : response;
    } catch (error) {
      if (error instanceof RunRouteError) {
        await audit(
          'rejected',
          error.status === 404
            ? 'invalid-thread'
            : error.status === 400
              ? 'client-memory-id'
              : `route-error-${error.status}`,
        );
        return json(
          { error: error.status === 404 ? 'thread not found' : error.message },
          error.status,
        );
      }
      if (error instanceof ActorResolutionError) {
        await audit('rejected', 'forbidden');
        return json({ error: 'forbidden' }, 403);
      }
      await audit('rejected', 'internal-error');
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
