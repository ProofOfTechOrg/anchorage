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
  admitsDrainableExecution,
  type ExecutionFenceStore,
  executionFencedResponse,
  isExecutionFenceRefusal,
  OPEN_EXECUTION_FENCE,
} from '../do-runner/index.js';
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
import {
  classifyDeliveryError,
  classifyDeliveryResponse,
  deliverNotification,
  isTerminalDelivery,
} from './delivery.js';
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
  /** Rows the thread Durable Object refused on content (accepted path). */
  denied?: number;
  /** Rows a defect made undeliverable — a provider or stored-row bug (accepted path). */
  failed?: number;
  /** Rows the deployment could not decide, awaiting redelivery (accepted path). */
  deferred?: number;
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
  /**
   * The deployment execution fence, consulted AFTER signature verification.
   * A locked deployment answers 503 so the provider redelivers rather than
   * treating the event as accepted; putting the check before the verify would
   * turn the fence into a free oracle for unauthenticated callers, and would
   * spend the forgery-audit budget on requests the fence refused anyway.
   * Absent ⇒ unfenced.
   */
  executionFence?: ExecutionFenceStore;
}

export type WebhookRouter = (request: Request) => Promise<Response | null>;

/**
 * A digest of the raw bytes the signature already covered — the stable half of
 * a per-delivery dedupe key, computed once per webhook.
 */
async function webhookEventDigest(rawBody: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', rawBody);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('')
    .slice(0, 32);
}

/**
 * A stable identity for ONE event delivered to ONE subscription, so a
 * redelivery — ours, by answering a retryable failure with a 5xx, or the
 * provider's own at-least-once retry — coalesces into the still-pending row
 * instead of showing the agent the same event twice. It needs no provider
 * cooperation; a provider that supplies its own `dedupeKey` keeps it.
 *
 * The SUBSCRIPTION id is part of the key, not just the event: two subscriptions
 * for different external resources may legitimately name the same thread and
 * resource, and coalescing matches on (thread, source, kind, agent, resource,
 * dedupeKey). A key derived from the event alone would collapse those two
 * intended notifications into one.
 */
function webhookDeliveryDedupeKey(
  providerId: string,
  subscriptionId: string,
  eventDigest: string,
): string {
  return `${providerId}:${subscriptionId}:${eventDigest}`;
}

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

      // The execution fence, once per webhook and only for a payload already
      // proven authentic. A locked (or proof-only) deployment must not ingest
      // a delivery it cannot forward: 503 is the status every provider retries
      // on, so the event survives the migration in the PROVIDER's queue rather
      // than being half-landed in this database. Draining still delivers —
      // the thread routes degrade a wake to a persist there.
      const fenceReading = options.executionFence
        ? await options.executionFence.read()
        : OPEN_EXECUTION_FENCE;
      if (!admitsDrainableExecution(fenceReading)) {
        await auditWebhook({
          providerId,
          outcome: 'rejected',
          reason: 'execution-fenced',
          contentBytes: rawBody.length,
        });
        return executionFencedResponse(fenceReading.state, 'webhook delivery');
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
      // Each matched row ends in exactly one of three states, because only one
      // of them may make the whole webhook retryable:
      //   delivered — landed in the thread inbox;
      //   terminal  — a content denial or a provider/address defect, where
      //               redelivering the identical bytes can only repeat it;
      //   deferred  — the deployment could not decide (policy evaluator down,
      //               storage down, the Durable Object failing), which MUST NOT
      //               be silently dropped.
      let delivered = 0;
      let denied = 0;
      let failed = 0;
      let deferred = 0;
      const eventDigest = await webhookEventDigest(rawBody);
      for (const row of matched) {
        if (!deliveryAllowed) continue;
        // buildNotification runs in its own try: a provider bug is a permanent
        // defect in code, not a transient condition, so it must not turn
        // earlier applied deliveries into a retryable whole-webhook failure.
        let built: SendNotificationSignalInput;
        try {
          built = provider.buildNotification(payload, row);
        } catch (error) {
          failed += 1;
          console.error(
            JSON.stringify({
              type: 'signal-provider.webhook-delivery-error',
              providerId,
              terminal: true,
              reason: error instanceof Error ? error.message : String(error),
            }),
          );
          continue;
        }
        try {
          const response = await deliverNotification(topology, row, {
            ...built,
            dedupeKey:
              built.dedupeKey ??
              webhookDeliveryDedupeKey(providerId, row.id, eventDigest),
          });
          const outcome = classifyDeliveryResponse(response.status);
          if (outcome === 'delivered') {
            delivered += 1;
            continue;
          }
          if (outcome === 'denied') denied += 1;
          else if (outcome === 'failed') failed += 1;
          else deferred += 1;
          console.error(
            JSON.stringify({
              type: 'signal-provider.webhook-delivery-rejected',
              providerId,
              status: response.status,
              terminal: isTerminalDelivery(outcome),
            }),
          );
        } catch (error) {
          const outcome = classifyDeliveryError(error);
          if (outcome === 'denied') denied += 1;
          else if (outcome === 'failed') failed += 1;
          else deferred += 1;
          console.error(
            JSON.stringify({
              type: 'signal-provider.webhook-delivery-error',
              providerId,
              terminal: isTerminalDelivery(outcome),
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
          ...(denied > 0 ? { denied } : {}),
          ...(failed > 0 ? { failed } : {}),
          ...(deferred > 0 ? { deferred } : {}),
          ...(!deliveryAllowed
            ? { reason: 'rate-limited' }
            : deferred > 0
              ? { reason: 'delivery-deferred' }
              : {}),
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
      // A deferred row answers the sender with a 5xx so its own at-least-once
      // redelivery is what recovers the notification; this deployment has
      // nowhere durable to park an unvetted provider payload, and losing an
      // authentic event is worse than seeing it twice. The per-delivery dedupe
      // key collapses a redelivery into any row from this attempt that is still
      // PENDING; a row core already delivered (urgent, or high/medium into an
      // idle thread) no longer coalesces, so the agent can see that one twice.
      // Webhook delivery is at-least-once either way — the provider retries on
      // its own schedule regardless of what this route returns.
      return json(
        {
          matched: matched.length,
          delivered,
          ...(denied > 0 ? { denied } : {}),
          ...(failed > 0 ? { failed } : {}),
          ...(deferred > 0 ? { deferred } : {}),
        },
        deferred > 0 ? 503 : 200,
      );
    } catch (error) {
      // A fence that could not be READ is not evidence the deployment is open.
      // 503 rather than the 500 below so the provider's own at-least-once
      // redelivery is what recovers the event, exactly as for a refusal.
      if (isExecutionFenceRefusal(error)) {
        return json(
          { error: error.message, reason: error.reason },
          error.status,
        );
      }
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
