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
//   7. map key -> subscription ROWS (the cross-tenant system view) — the payload
//      NEVER names a tenant/thread/resource; the ROW is the authority
//   8. per provider+tenant rate cap (429-equivalent: skip that tenant, audited)
//   9. deliver each matched row through the topology (which re-checks ownership
//      and fails closed on a tampered row), audit the accepted ingest
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
  type ApprovalRole,
  RUN_START_ROLES,
  type TenantContext,
  TenantResolutionError,
  type TenantResolver,
} from '../approval-api/index.js';
import {
  assertNoClientMemoryIds,
  RunRouteError,
  requireOwnedMemoryId,
  type ThreadTopology,
} from '../host-kit/index.js';
import { safeDecodeSegment } from '../host-kit/route-path.js';
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
  SubscriptionStoreFactory,
  SystemSubscriptionStore,
} from './subscription-d1.js';

// --- Audit ----------------------------------------------------------------

export interface WebhookAuditEvent {
  type: 'signal-provider.webhook';
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
  tenantId: string;
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

/** Per provider+tenant rate cap. false ⇒ REFUSE that tenant's deliveries. */
export type WebhookRateLimiter = (
  providerId: string,
  tenantId: string,
) => boolean | Promise<boolean>;

// --- Webhook router -------------------------------------------------------

export interface WebhookRouterOptions {
  /** Registered providers, keyed by id. Empty ⇒ every webhook is route-absent. */
  providers: Record<string, SignalProviderAdapter>;
  /** The cross-tenant subscription view — the webhook's tenant authority. */
  subscriptions: SystemSubscriptionStore;
  /** The sanctioned reach into a thread DO (stamps the tenant header, 404s a foreign thread). */
  topology: ThreadTopology;
  /** Per-provider signing secret (from a binding/env). undefined ⇒ provider route absent. */
  secretForProvider: (providerId: string) => string | undefined;
  /** Every ingest audited (accepted + rejected). Forgery audits are bounded (see below). */
  audit?: SignalProviderAuditSink;
  /** Per provider+tenant delivery rate cap. Absent ⇒ unmetered. */
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
      const rawBody = new Uint8Array(await request.arrayBuffer());
      if (rawBody.length > maxBodyBytes) {
        await auditWebhook({
          providerId,
          outcome: 'rejected',
          reason: 'payload-too-large',
          contentBytes: rawBody.length,
        });
        return json({ error: 'payload too large' }, 413);
      }

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
        payload = JSON.parse(new TextDecoder().decode(rawBody));
      } catch {
        await auditWebhook({
          providerId,
          outcome: 'rejected',
          reason: 'malformed-body',
          contentBytes: rawBody.length,
        });
        return json({ error: 'a JSON body is required' }, 400);
      }

      // Map payload -> external resource key(s) -> subscription ROWS (cross-tenant).
      const keys = provider.extractResourceIds?.(payload) ?? [];
      const byId = new Map<string, StoredSubscription>();
      for (const key of keys) {
        for (const row of await subscriptions.listByResource(providerId, key)) {
          byId.set(row.id, row);
        }
      }
      const matched = [...byId.values()];

      // Per provider+tenant rate cap, evaluated once per unique tenant.
      const tenantAllowed = new Map<string, boolean>();
      let delivered = 0;
      for (const row of matched) {
        let allowed = tenantAllowed.get(row.tenantId);
        if (allowed === undefined) {
          allowed = rateLimit
            ? await Promise.resolve(rateLimit(providerId, row.tenantId))
            : true;
          tenantAllowed.set(row.tenantId, allowed);
        }
        if (!allowed) continue; // over cap for this tenant — skip, audited below.
        // buildNotification from the AUTHENTIC payload + the row; deliver through
        // the topology, which REJECTS a tampered (foreign-thread) row with a 404 —
        // caught here per-delivery so it fails closed (never delivers cross-tenant)
        // without aborting the rest of the batch.
        const notification: SendNotificationSignalInput =
          provider.buildNotification(payload, row);
        try {
          const response = await deliverNotification(
            topology,
            row,
            notification,
          );
          if (response.ok) {
            delivered += 1;
          } else {
            // A matched, authentic delivery the thread DO REJECTED for content (a
            // 5xx/4xx, NOT an ownership 404 — that throws and is caught below): log
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

      const rateLimitedTenants = [...tenantAllowed.values()].filter(
        (v) => !v,
      ).length;
      await auditWebhook({
        providerId,
        outcome: 'accepted',
        matched: matched.length,
        delivered,
        ...(rateLimitedTenants > 0
          ? { reason: `rate-limited-tenants:${rateLimitedTenants}` }
          : {}),
        contentBytes: rawBody.length,
      });
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
  /** Authenticate, validate the tenant ID, and bind it; undefined means 401. */
  resolve: TenantResolver;
  /** Tenant-bound store factory, bound per request to the resolved tenant. */
  subscriptions: SubscriptionStoreFactory;
  /** Who may manage subscriptions. Default RUN_START_ROLES (operator/admin). */
  roles?: readonly ApprovalRole[];
  /** The provider ids a subscription may name. Absent ⇒ any PROVIDER_ID_PATTERN slug. */
  knownProviders?: readonly string[];
  audit?: SignalProviderAuditSink;
  /**
   * Reconcile the tenant provider-host alarm after a subscription row commits.
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

    let tenant: TenantContext | undefined;
    const audit = async (
      outcome: 'accepted' | 'rejected',
      extra: {
        providerId?: string;
        externalResourceId?: string;
        pollingLifecycle?: 'reconciled' | 'failed';
        reason?: string;
      } = {},
    ): Promise<void> => {
      if (!options.audit || !tenant) return;
      await options.audit({
        type: 'signal-provider.subscription',
        tenantId: tenant.tenantId,
        actorId: tenant.actor.id,
        threadId,
        action,
        outcome,
        ...extra,
        timestamp: new Date().toISOString(),
      });
    };

    const finishCommittedMutation = async (
      resolvedTenant: TenantContext,
      providerId: string,
      externalResourceId: string,
    ): Promise<Response | undefined> => {
      let pollingLifecycle: 'reconciled' | 'failed' | undefined;
      let reconcileFailed = false;
      if (options.reconcilePolling) {
        try {
          await options.reconcilePolling(resolvedTenant);
          pollingLifecycle = 'reconciled';
        } catch (error) {
          pollingLifecycle = 'failed';
          reconcileFailed = true;
          console.error(
            JSON.stringify({
              type: 'signal-provider.polling-reconcile-error',
              tenantId: resolvedTenant.tenantId,
              providerId,
              action,
              reason: error instanceof Error ? error.message : String(error),
            }),
          );
        }
      }

      // The row mutation has committed. An audit-sink failure cannot turn that
      // success into a rejected audit or a generic 500 response.
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
            tenantId: resolvedTenant.tenantId,
            providerId,
            action,
            reason: error instanceof Error ? error.message : String(error),
          }),
        );
      }

      if (reconcileFailed) {
        return json(
          {
            error: 'polling lifecycle unavailable',
            mutationApplied: true,
          },
          502,
        );
      }
      return undefined;
    };

    try {
      // 1. Resolve.
      tenant = await resolve(request);
      if (!tenant) return json({ error: 'authentication required' }, 401);

      // 2. Coarse role: subscription management mutates config; reads are gated
      // the same (a subscription list reveals what a tenant watches).
      if (!roles.includes(tenant.actor.role)) {
        await audit('rejected', { reason: 'forbidden-role' });
        return json({ error: 'forbidden' }, 403);
      }

      // 3. Thread-prefix ownership: 404 (never 403) on a foreign threadId — the
      // run-router rule, no existence oracle.
      requireOwnedMemoryId(tenant, threadId, 'threadId');

      if (method === 'GET') {
        const list = await subscriptions
          .forTenant(tenant.tenantId)
          .listForThread(threadId);
        await audit('accepted');
        return json({ subscriptions: list });
      }

      // Bodyful methods: size-cap, parse, and refuse a client-named memory id.
      const rawBody = await request.text();
      if (new TextEncoder().encode(rawBody).length > maxBodyBytes) {
        return json({ error: 'payload too large' }, 413);
      }
      let body: Record<string, unknown>;
      try {
        const parsed = rawBody === '' ? {} : JSON.parse(rawBody);
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
      // 4. No client memory id anywhere (threadId is in the PATH; resourceId is
      // server-minted from resourceKey).
      assertNoClientMemoryIds(body);

      const providerId = body.providerId;
      const externalResourceId = body.externalResourceId;
      if (typeof providerId !== 'string' || !providerAllowed(providerId)) {
        return json({ error: 'providerId is required and must be known' }, 400);
      }
      if (
        typeof externalResourceId !== 'string' ||
        externalResourceId.length === 0
      ) {
        return json({ error: 'externalResourceId is required' }, 400);
      }

      const store = subscriptions.forTenant(tenant.tenantId);

      if (method === 'DELETE') {
        const removed = await store.unsubscribe(
          providerId,
          externalResourceId,
          threadId,
        );
        const lifecycleFailure = await finishCommittedMutation(
          tenant,
          providerId,
          externalResourceId,
        );
        if (lifecycleFailure) return lifecycleFailure;
        return json({ removed });
      }

      // POST subscribe. resourceKey is the thread OWNER's business key (NOT a
      // memory id — assertNoClientMemoryIds bans only threadId/resourceId): the
      // resourceId is server-minted from it, tenant-salted.
      const resourceKey = body.resourceKey;
      if (typeof resourceKey !== 'string' || resourceKey.length === 0) {
        return json(
          { error: 'resourceKey is required (the thread owner business key)' },
          400,
        );
      }
      let resourceId: string;
      try {
        resourceId = tenant.newResourceId(resourceKey);
      } catch {
        return json({ error: 'resourceKey is not path-safe' }, 400);
      }
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
      const lifecycleFailure = await finishCommittedMutation(
        tenant,
        providerId,
        externalResourceId,
      );
      if (lifecycleFailure) return lifecycleFailure;
      return json({ subscription });
    } catch (error) {
      if (error instanceof RunRouteError) {
        await audit('rejected', {
          reason:
            error.status === 404
              ? 'foreign-thread'
              : error.status === 400
                ? 'client-memory-id'
                : `route-error-${error.status}`,
        });
        return json({ error: error.message }, error.status);
      }
      if (error instanceof TenantResolutionError) {
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
