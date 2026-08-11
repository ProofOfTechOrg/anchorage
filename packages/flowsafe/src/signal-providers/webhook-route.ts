// SPDX-License-Identifier: Apache-2.0
// Track E (M-007), CI-M-007-003 — webhook ingress that terminates on the Worker,
// plus the human-only subscribe/unsubscribe surface (DL-006/DL-017).
//
// THE WEBHOOK GATE (its "auth" IS the signature, not a bearer token):
//   1. path + method match (else null / 405)
//   2. provider registered AND its secret configured (else null — route absent,
//      byte-identical to an unconfigured deployment)
//   3. read the RAW bytes, size-capped (413)
//   4. VERIFY the provider signature over the raw bytes — BEFORE any parse, any
//      subscription lookup, any delivery. A forged signature is REJECTED (401)
//      and audited (E-S2). No state is touched on the reject path.
//   5. parse JSON (400 on malformed)
//   6. extract the external resource key(s) from the payload
//   7. map key -> deployment subscription rows — the payload NEVER names a
//      thread/resource; the row is the authority
//   8. per-provider deployment rate cap (429-equivalent: skip delivery, audited)
//   9. deliver each matched row through the topology (which validates the
//      path-safe thread address), audit the accepted ingest
//
// A forged-signature flood must not amplify into the audit log: the reject is
// UNBOUNDED (every forgery is refused) but the forgery AUDIT is bounded to
// `maxForgeryAuditsPerWindow` per provider per window (a fixed in-isolate
// window). So the log records that forgeries are happening without a flood
// writing one line per attempt. Build the router ONCE per isolate (it needs no
// per-request resolver) so that window persists across requests.
//
// Subscribe/unsubscribe are WRITE-CLASS and stay human-only HTTP (RA-009): never
// exposed as agent tools. They mint NO capability (P8) — a subscription row is
// addressing/config, not a grant.

import type { SendNotificationSignalInput } from '@mastra/core/notifications';

import {
  type ActorContext,
  ActorResolutionError,
  type ActorResolver,
  type ApprovalRole,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import {
  assertNoClientMemoryIds,
  type BoundThreadTargetValidator,
  RunRouteError,
  requireResourceAccess,
  type ThreadTopology,
} from '../host-kit/index.js';
import { safeDecodeSegment } from '../host-kit/route-path.js';
import { readBoundedBody, readBoundedBytes } from '../http-body.js';
import { internalErrorResponse } from '../internal-error-response.js';
import {
  nonnegativeSafeInteger,
  positiveSafeInteger,
} from '../numeric-config.js';
import { deliverNotification } from './delivery.js';
import type { ReconcileSignalProviderPolling } from './host-topology.js';
import { PROVIDER_ID_PATTERN, type SignalProviderAdapter } from './provider.js';
import type {
  StoredSubscription,
  SubscriptionStore,
  SubscriptionStoreFactory,
} from './subscription-d1.js';
import {
  isValidExternalResourceId,
  MAX_EXTERNAL_RESOURCE_ID_BYTES,
} from './subscription-d1.js';

// --- Audit ----------------------------------------------------------------

export interface WebhookAuditEvent {
  type: 'signal-provider.webhook';
  deploymentTag?: string;
  providerId: string;
  outcome: 'accepted' | 'rejected';
  /** WHY, for rejected outcomes (or a partial-accept note). */
  reason?: string;
  /** Matched subscription rows (accepted path). */
  matched?: number;
  /** Rows actually delivered (accepted path). */
  delivered?: number;
  /** Raw body size in bytes the size cap measured. */
  contentBytes: number;
  timestamp: string;
}

export interface SubscriptionAuditEvent {
  type: 'signal-provider.subscription';
  deploymentTag?: string;
  actorId: string;
  threadId: string;
  /** The attempted action — `list` is a read (GET), never mislabeled a mutation. */
  action: 'subscribe' | 'unsubscribe' | 'list';
  outcome: 'accepted' | 'rejected';
  providerId?: string;
  externalResourceId?: string;
  /** Present only when mutation-to-alarm reconciliation is configured. */
  pollingLifecycle?: 'reconciled' | 'failed';
  reason?: string;
  timestamp: string;
}

export type SignalProviderAuditEvent =
  | WebhookAuditEvent
  | SubscriptionAuditEvent;

export type SignalProviderAuditSink = (
  event: SignalProviderAuditEvent,
) => void | Promise<void>;

/** Per-provider deployment rate cap. false means refuse matched deliveries. */
export type WebhookRateLimiter = (
  providerId: string,
) => boolean | Promise<boolean>;

// --- Webhook router -------------------------------------------------------

export interface WebhookRouterOptions {
  /** Registered providers, keyed by id. Empty ⇒ every webhook is route-absent. */
  providers: Record<string, SignalProviderAdapter>;
  /** The deployment subscription store; matched rows are routing authority. */
  subscriptions: SubscriptionStore;
  /** The sanctioned reach into a thread DO. */
  topology: ThreadTopology;
  /** Infrastructure-verified deployment tag for audit attribution. */
  deploymentTag?: string;
  /** Per-provider signing secret (from a binding/env). undefined ⇒ provider route absent. */
  secretForProvider: (providerId: string) => string | undefined;
  /** Every ingest audited (accepted + rejected). Forgery audits are bounded (see below). */
  audit?: SignalProviderAuditSink;
  /** Per-provider deployment delivery rate cap. Absent means unmetered. */
  rateLimit?: WebhookRateLimiter;
  /** Route prefix. Default '/api/signal-providers'. */
  basePath?: string;
  /**
   * Max raw webhook body in bytes. Must be a nonnegative safe integer; zero
   * denies every non-empty body. Default 1 MiB.
   */
  maxBodyBytes?: number;
  /**
   * Forgery audits per provider per window. Must be a nonnegative safe integer;
   * zero disables forgery audit writes without accepting forgeries. Default 10.
   */
  maxForgeryAuditsPerWindow?: number;
  /** Positive safe-integer forgery-audit window in ms. Default 60000. */
  forgeryAuditWindowMs?: number;
  /** Epoch-ms clock for the forgery window, injectable for tests. Default Date.now. */
  now?: () => number;
}

export type WebhookRouter = (request: Request) => Promise<Response | null>;

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

export function createWebhookRouter(
  options: WebhookRouterOptions,
): WebhookRouter {
  const {
    providers,
    subscriptions,
    topology,
    secretForProvider,
    audit,
    rateLimit,
  } = options;
  const base = options.basePath ?? '/api/signal-providers';
  const baseSegments = base.split('/').filter(Boolean);
  const maxBodyBytes = nonnegativeSafeInteger(
    options.maxBodyBytes ?? 1_048_576,
    'webhook maxBodyBytes',
  );
  const maxForgeryAudits = nonnegativeSafeInteger(
    options.maxForgeryAuditsPerWindow ?? 10,
    'maxForgeryAuditsPerWindow',
  );
  const forgeryWindowMs = positiveSafeInteger(
    options.forgeryAuditWindowMs ?? 60_000,
    'forgeryAuditWindowMs',
  );
  const now = options.now ?? Date.now;

  // Bounded forgery-audit windows, per provider. A forgery is ALWAYS rejected;
  // it is audited at most `maxForgeryAudits` times per window, so a flood cannot
  // write one log line per attempt.
  const forgeryWindows = new Map<string, { count: number; resetAt: number }>();
  const shouldAuditForgery = (providerId: string): boolean => {
    if (maxForgeryAudits === 0) return false;
    const current = now();
    const window = forgeryWindows.get(providerId);
    if (!window || current >= window.resetAt) {
      forgeryWindows.set(providerId, {
        count: 1,
        resetAt: current + forgeryWindowMs,
      });
      return true;
    }
    if (window.count >= maxForgeryAudits) return false;
    window.count += 1;
    return true;
  };

  const auditWebhook = async (
    event: Omit<WebhookAuditEvent, 'type' | 'timestamp'>,
  ): Promise<void> => {
    if (!audit) return;
    await audit({
      type: 'signal-provider.webhook',
      ...(options.deploymentTag !== undefined
        ? { deploymentTag: options.deploymentTag }
        : {}),
      timestamp: new Date().toISOString(),
      ...event,
    });
  };

  return async (request) => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    // /api/signal-providers/:providerId/webhook
    if (
      segments.length !== baseSegments.length + 2 ||
      baseSegments.some((seg, i) => segments[i] !== seg) ||
      segments[baseSegments.length + 1] !== 'webhook'
    ) {
      return null;
    }
    // Malformed percent-encoding in the id segment is not a real provider —
    // route-absent (byte-identical), never a decodeURIComponent throw.
    const providerId = safeDecodeSegment(segments[baseSegments.length]);
    if (providerId === undefined) return null;
    if (request.method !== 'POST') {
      return json({ error: 'method not allowed' }, 405);
    }

    // Route-absent (byte-identical) when the provider or its secret is not
    // configured. An EMPTY secret ('') is treated as unconfigured too — never
    // verify with a zero-length HMAC key (Node WebCrypto throws on it, and an
    // empty key that another runtime accepts would be a trivial forgery bypass):
    // `!secret` catches both undefined and ''.
    const provider = providers[providerId];
    const secret = secretForProvider(providerId);
    if (!provider || !secret) return null;

    // Past the routing decision, everything is wrapped: a provider callback
    // (verify/extract/build) or the rate limiter that THROWS must become a 500,
    // never an unhandled rejection that crashes the mounting handler.
    try {
      // Raw bytes, size-capped, BEFORE any parse — the signature is verified over
      // exactly these bytes.
      const raw = await readBoundedBytes(
        request,
        maxBodyBytes,
        'webhook body exceeds limit',
      );
      if (!raw.ok) {
        await auditWebhook({
          providerId,
          outcome: 'rejected',
          reason: 'payload-too-large',
          contentBytes: raw.bytesRead,
        });
        return json({ error: 'payload too large' }, 413);
      }
      const rawBody = raw.bytes;

      // VERIFY BEFORE PARSE. A forged signature is refused here — no JSON.parse, no
      // subscription lookup, no delivery. The forgery audit is bounded.
      const verified = await Promise.resolve(
        provider.verifyWebhookSignature?.(rawBody, request.headers, secret) ??
          false,
      );
      if (!verified) {
        if (shouldAuditForgery(providerId)) {
          await auditWebhook({
            providerId,
            outcome: 'rejected',
            reason: 'forged-signature',
            contentBytes: rawBody.length,
          });
        }
        return json({ error: 'invalid signature' }, 401);
      }

      // Parse — only now that the payload is proven authentic.
      let payload: unknown;
      try {
        payload = JSON.parse(
          new TextDecoder('utf-8', { fatal: true, ignoreBOM: false }).decode(
            rawBody,
          ),
        );
      } catch {
        await auditWebhook({
          providerId,
          outcome: 'rejected',
          reason: 'malformed-body',
          contentBytes: rawBody.length,
        });
        return json({ error: 'a JSON body is required' }, 400);
      }

      // Map payload -> external resource key(s) -> authoritative subscription rows.
      const keys = provider.extractResourceIds?.(payload) ?? [];
      const byId = new Map<string, StoredSubscription>();
      for (const key of keys) {
        for (const row of await subscriptions.listByResource(providerId, key)) {
          byId.set(row.id, row);
        }
      }
      const matched = [...byId.values()];

      const deliveryAllowed =
        matched.length === 0 || !rateLimit
          ? true
          : await Promise.resolve(rateLimit(providerId));
      let delivered = 0;
      for (const row of matched) {
        if (!deliveryAllowed) continue;
        // buildNotification from the AUTHENTIC payload + the row; deliver through
        // the topology, which rejects a malformed thread address before
        // resolution. Catch notification construction and delivery together so
        // a provider bug on one row cannot turn earlier applied deliveries into
        // a retryable whole-webhook failure.
        try {
          const notification: SendNotificationSignalInput =
            provider.buildNotification(payload, row);
          const response = await deliverNotification(
            topology,
            row,
            notification,
          );
          if (response.ok) {
            delivered += 1;
          } else {
            // A matched, authentic delivery the thread DO REJECTED for content (a
            // 5xx/4xx from the thread DO): log
            // it so it is not silently folded into delivered < matched with no trail.
            console.error(
              JSON.stringify({
                type: 'signal-provider.webhook-delivery-rejected',
                providerId,
                status: response.status,
              }),
            );
          }
        } catch (error) {
          console.error(
            JSON.stringify({
              type: 'signal-provider.webhook-delivery-error',
              providerId,
              reason: error instanceof Error ? error.message : String(error),
            }),
          );
        }
      }

      try {
        await auditWebhook({
          providerId,
          outcome: 'accepted',
          matched: matched.length,
          delivered,
          ...(!deliveryAllowed ? { reason: 'rate-limited' } : {}),
          contentBytes: rawBody.length,
        });
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'signal-provider.webhook-audit-error',
            providerId,
            reason: error instanceof Error ? error.message : String(error),
          }),
        );
      }
      return json({ matched: matched.length, delivered });
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'signal-provider.webhook-error',
          providerId,
          reason: error instanceof Error ? error.message : String(error),
        }),
      );
      return json({ error: 'internal error' }, 500);
    }
  };
}

// --- Subscription CRUD (human-only HTTP; RA-009) --------------------------

export interface SubscriptionRouterOptions {
  /** Authenticate and validate the actor; undefined means 401. */
  resolve: ActorResolver;
  /** Deployment-wide subscription store factory. */
  subscriptions: SubscriptionStoreFactory;
  /** Prove subscriptions target durable bound memory, not ephemeral run ids. */
  validateThreadTarget: BoundThreadTargetValidator;
  /** Who may manage subscriptions. Default RUN_START_ROLES (operator/admin). */
  roles?: readonly ApprovalRole[];
  /** The provider ids a subscription may name. Absent ⇒ any PROVIDER_ID_PATTERN slug. */
  knownProviders?: readonly string[];
  audit?: SignalProviderAuditSink;
  /**
   * Reconcile the deployment provider-host alarm after a row commits.
   * Absent preserves the original row-only behavior.
   */
  reconcilePolling?: ReconcileSignalProviderPolling;
  /** Route prefix. Default '/api/threads'. Routes: `<base>/:threadId/subscriptions`. */
  basePath?: string;
  /**
   * Max request body in bytes. Must be a nonnegative safe integer; zero denies
   * every non-empty mutation body. Default 4096.
   */
  maxBodyBytes?: number;
  /** Deployment policy applied after bounded parse and thread authorization. */
  authorizeMutation?: (input: {
    context: ActorContext;
    method: 'POST' | 'DELETE';
    threadId: string;
    providerId: string;
    externalResourceId: string;
    resourceKey?: string;
  }) => void | Promise<void>;
}

export type SubscriptionRouter = (request: Request) => Promise<Response | null>;

export function createSubscriptionRouter(
  options: SubscriptionRouterOptions,
): SubscriptionRouter {
  const { resolve, subscriptions } = options;
  const roles = options.roles ?? RUN_START_ROLES;
  const base = options.basePath ?? '/api/threads';
  const baseSegments = base.split('/').filter(Boolean);
  const maxBodyBytes = nonnegativeSafeInteger(
    options.maxBodyBytes ?? 4096,
    'subscription maxBodyBytes',
  );
  const known = options.knownProviders
    ? new Set(options.knownProviders)
    : undefined;

  const providerAllowed = (providerId: string): boolean =>
    PROVIDER_ID_PATTERN.test(providerId) &&
    (known === undefined || known.has(providerId));

  return async (request) => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    // <base>/:threadId/subscriptions
    if (
      segments.length !== baseSegments.length + 2 ||
      baseSegments.some((seg, i) => segments[i] !== seg) ||
      segments[baseSegments.length + 1] !== 'subscriptions'
    ) {
      return null;
    }
    // Malformed percent-encoding in the threadId is a bad request, not a decode
    // throw — 400 (a syntax error, identity-independent, no existence oracle).
    const threadId = safeDecodeSegment(segments[baseSegments.length]);
    if (threadId === undefined) {
      return json({ error: 'malformed threadId' }, 400);
    }
    const method = request.method;
    if (method !== 'POST' && method !== 'DELETE' && method !== 'GET') {
      return json({ error: 'method not allowed' }, 405);
    }
    // The attempted action, method-accurate — a GET is a `list` READ, never
    // mislabeled a subscribe/unsubscribe in the audit trail.
    const action: 'subscribe' | 'unsubscribe' | 'list' =
      method === 'POST'
        ? 'subscribe'
        : method === 'DELETE'
          ? 'unsubscribe'
          : 'list';

    let context: ActorContext | undefined;
    const audit = async (
      outcome: 'accepted' | 'rejected',
      extra: {
        providerId?: string;
        externalResourceId?: string;
        pollingLifecycle?: 'reconciled' | 'failed';
        reason?: string;
      } = {},
    ): Promise<void> => {
      if (!options.audit || !context) return;
      await options.audit({
        type: 'signal-provider.subscription',
        ...(context.deploymentTag !== undefined
          ? { deploymentTag: context.deploymentTag }
          : {}),
        actorId: context.actor.id,
        threadId,
        action,
        outcome,
        ...extra,
        timestamp: new Date().toISOString(),
      });
    };

    const finishCommittedMutation = async (
      providerId: string,
      externalResourceId: string,
    ): Promise<void> => {
      // Both follow-ups run after the row mutation commits. Their failures are
      // observable through logs/audit metadata but cannot make the HTTP caller
      // retry an already-applied subscribe or unsubscribe.
      let pollingLifecycle: 'reconciled' | 'failed' | undefined;
      if (options.reconcilePolling) {
        try {
          await options.reconcilePolling();
          pollingLifecycle = 'reconciled';
        } catch (error) {
          pollingLifecycle = 'failed';
          console.error(
            JSON.stringify({
              type: 'signal-provider.polling-reconcile-error',
              ...(context?.deploymentTag !== undefined
                ? { deploymentTag: context.deploymentTag }
                : {}),
              providerId,
              action,
              reason: error instanceof Error ? error.message : String(error),
            }),
          );
        }
      }

      try {
        await audit('accepted', {
          providerId,
          externalResourceId,
          ...(pollingLifecycle === undefined ? {} : { pollingLifecycle }),
          ...(pollingLifecycle === 'failed'
            ? { reason: 'polling-reconcile-failed' }
            : {}),
        });
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'signal-provider.subscription-audit-error',
            ...(context?.deploymentTag !== undefined
              ? { deploymentTag: context.deploymentTag }
              : {}),
            providerId,
            action,
            reason: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    };

    try {
      // 1. Resolve.
      context = await resolve(request);
      if (!context) return json({ error: 'authentication required' }, 401);

      // 2. Resolve thread ownership before the role gate. Foreign and missing
      // ids collapse to the same 404.
      await requireResourceAccess(
        context,
        'thread',
        threadId,
        method === 'GET' ? 'read' : 'write',
        'thread',
      );

      // 3. Subscription management and reads both use the configured roles.
      if (!roles.includes(context.actor.role)) {
        await audit('rejected', { reason: 'forbidden-role' });
        return json({ error: 'forbidden' }, 403);
      }

      if (method === 'GET') {
        const list = await subscriptions.store().listForThread(threadId);
        await audit('accepted');
        return json({ subscriptions: list });
      }

      // Bodyful methods: size-cap, parse, and refuse a client-named memory id.
      const rawBody = await readBoundedBody(
        request,
        maxBodyBytes,
        'subscription body exceeds limit',
      );
      if (!rawBody.ok && rawBody.reason === 'payload-too-large') {
        return json({ error: 'payload too large' }, 413);
      }
      if (!rawBody.ok) {
        return json({ error: 'a JSON object body is required' }, 400);
      }
      let body: Record<string, unknown>;
      try {
        const parsed = rawBody.text === '' ? {} : JSON.parse(rawBody.text);
        if (
          typeof parsed !== 'object' ||
          parsed === null ||
          Array.isArray(parsed)
        ) {
          return json({ error: 'a JSON object body is required' }, 400);
        }
        body = parsed as Record<string, unknown>;
      } catch {
        return json({ error: 'a JSON object body is required' }, 400);
      }
      // 4. No client memory id anywhere (threadId is in the path; resourceId is
      // validated from the trusted resourceKey below).
      assertNoClientMemoryIds(body);

      const providerId = body.providerId;
      const externalResourceId = body.externalResourceId;
      if (typeof providerId !== 'string' || !providerAllowed(providerId)) {
        return json({ error: 'providerId is required and must be known' }, 400);
      }
      if (!isValidExternalResourceId(externalResourceId)) {
        return json(
          {
            error: `externalResourceId must be non-empty, control-safe, and at most ${MAX_EXTERNAL_RESOURCE_ID_BYTES} UTF-8 bytes`,
          },
          400,
        );
      }

      const store = subscriptions.store();

      if (method === 'DELETE') {
        await options.authorizeMutation?.({
          context,
          method,
          threadId,
          providerId,
          externalResourceId,
        });
        const removed = await store.unsubscribe(
          providerId,
          externalResourceId,
          threadId,
        );
        await finishCommittedMutation(providerId, externalResourceId);
        return json({ removed });
      }

      // POST subscribe. resourceKey is the thread OWNER's business key (NOT a
      // memory id — assertNoClientMemoryIds bans only threadId/resourceId): the
      // resourceId is validated and accepted only through the server context.
      const resourceKey = body.resourceKey;
      if (typeof resourceKey !== 'string' || resourceKey.length === 0) {
        return json(
          { error: 'resourceKey is required (the thread owner business key)' },
          400,
        );
      }
      await options.authorizeMutation?.({
        context,
        method,
        threadId,
        providerId,
        externalResourceId,
        resourceKey,
      });
      let resourceId: string;
      try {
        resourceId = context.resourceIdFromKey(resourceKey);
      } catch {
        return json({ error: 'resourceKey is not path-safe' }, 400);
      }
      await requireResourceAccess(
        context,
        'resource',
        resourceId,
        'write',
        'resource',
      );
      const [threadOwner, resourceOwner] = await Promise.all([
        context.resourceOwnerFor('thread', threadId),
        context.resourceOwnerFor('resource', resourceId),
      ]);
      if (
        !threadOwner ||
        !resourceOwner ||
        threadOwner.kind !== resourceOwner.kind ||
        threadOwner.id !== resourceOwner.id
      ) {
        throw new RunRouteError(404, 'target resource not found');
      }
      await options.validateThreadTarget(context, { threadId, resourceId });
      const subscription = await store.subscribe({
        providerId,
        externalResourceId,
        threadId,
        resourceId,
        ...(typeof body.metadata === 'object' &&
        body.metadata !== null &&
        !Array.isArray(body.metadata)
          ? { metadata: body.metadata as Record<string, unknown> }
          : {}),
      });
      await finishCommittedMutation(providerId, externalResourceId);
      return json({ subscription });
    } catch (error) {
      if (error instanceof RunRouteError) {
        await audit('rejected', {
          reason:
            error.status === 404
              ? 'resource-not-found'
              : error.status === 400
                ? 'client-memory-id'
                : `route-error-${error.status}`,
        });
        return json({ error: error.message }, error.status);
      }
      if (error instanceof ActorResolutionError) {
        return json({ error: 'forbidden' }, 403);
      }
      try {
        await audit('rejected', { reason: 'internal-error' });
      } catch {
        // Best-effort audit must not replace the generic response.
      }
      return internalErrorResponse('signal-providers.subscription', error);
    }
  };
}
