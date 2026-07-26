// SPDX-License-Identifier: Apache-2.0
// Track E (M-007) — the flowsafe signal-provider contract for the DO-host +
// webhook surface (DL-017).
//
// PRE-FLIGHT B/D FINDING (pinned against @mastra/core 1.50.0
// signals/signal-provider.d.ts). Core's SignalProvider delivers a signal into a
// thread IN-PROCESS: `poll()` / `handleWebhook()` call `this.notify(notification,
// target)`, which calls `this.agent.sendNotificationSignal(...)` on a CONNECTED
// agent, and it matches webhooks against a base-class subscription registry that
// lives in an in-memory Map. Neither fits a Cloudflare provider-host DO: the
// agent loop runs on a DIFFERENT (per-thread) DO, so there is no co-located
// agent to notify, and the in-memory registry is empty after eviction and
// tenant-blind. So flowsafe drives providers through THIS adapter seam and
// routes every delivery through `createThreadTopology.send` (host-kit) into
// Track C's `/signal/notification` thread-DO route — the DO alarm replaces
// core's `startPolling` timer and the D1 subscription store replaces its
// registry (DL-017).
//
// A provider built here IS a core `SignalProvider` (it extends the base), so
// `isSignalProvider(p)` holds and `new Agent({ signals: [p] })` still merges any
// processors/tools it exposes for IN-PROCESS use — the flowsafe DO host simply
// does not take that path. Core's `SignalSubscription` /
// `SendNotificationSignalInput` shapes are used DIRECTLY (both are
// exports-reachable — `@mastra/core/signals` and `@mastra/core/notifications`),
// so nothing is mirrored and there is no drift surface to pin.

import type { SendNotificationSignalInput } from '@mastra/core/notifications';
import type { SignalSubscription } from '@mastra/core/signals';
import { SignalProvider } from '@mastra/core/signals';

import { nonnegativeSafeInteger } from '../numeric-config.js';

export type { SendNotificationSignalInput } from '@mastra/core/notifications';
export type { SignalSubscription } from '@mastra/core/signals';

/**
 * The minimal header reader a webhook signature check needs — a `Headers` or any
 * case-insensitive `get(name) => string | null`. Structural so the module stays
 * free of the DOM/Workers `Headers` lib choice.
 */
export interface WebhookHeaders {
  get(name: string): string | null;
}

/**
 * One notification to deliver to one matched subscription — what a provider's
 * poll or webhook parse produces, before the host routes it through the
 * topology. The `subscription` carries the tenant/thread/resource the delivery
 * is bound to (never the payload — the row is the authority).
 */
export interface ProviderDelivery {
  subscription: SignalSubscription;
  notification: SendNotificationSignalInput;
}

/**
 * The DO-host + webhook contract a flowsafe-hosted provider exposes. The host DO
 * drives `pollForDeliveries`; the webhook route drives
 * `verifyWebhookSignature` (over the RAW bytes, before any parse),
 * `extractResourceIds`, and `buildNotification`. A provider implements only the
 * seams its transport needs — a webhook-only provider omits `pollForDeliveries`,
 * a poll-only one omits the webhook trio.
 */
export interface SignalProviderAdapter {
  readonly id: string;
  /**
   * Poll cadence in ms for the host DO's alarm. Absent/0 disables automatic
   * alarms; a supplied `pollForDeliveries` remains callable through `/poll`.
   */
  readonly pollInterval?: number;
  /**
   * Verify a webhook's provider signature over the RAW request bytes, BEFORE the
   * body is parsed — a forged signature must be refused without ever inspecting
   * the payload. MUST be constant-time. Absent ⇒ the provider accepts no
   * webhooks (every webhook fails closed), so a webhook provider MUST supply it.
   */
  verifyWebhookSignature?(
    rawBody: Uint8Array,
    headers: WebhookHeaders,
    secret: string,
  ): Promise<boolean> | boolean;
  /**
   * The external-resource key(s) a parsed webhook payload concerns, matched
   * against subscriptions' `externalResourceId`. Returns an empty array when the
   * payload names no resource this provider tracks.
   */
  extractResourceIds?(payload: unknown): string[];
  /** Shape the notification a matched subscription receives (webhook AND poll). */
  buildNotification(
    payload: unknown,
    subscription: SignalSubscription,
  ): SendNotificationSignalInput;
  /**
   * Check external state for a batch of REHYDRATED subscriptions (the host DO
   * passes the D1 rows, not core's in-memory registry) and return the deliveries.
   * The host routes each through the topology. Absent ⇒ webhook-only.
   */
  pollForDeliveries?(
    subscriptions: SignalSubscription[],
  ): Promise<ProviderDelivery[]>;
}

/**
 * Provider-id charset: a stable lowercase URL/config slug. The provider host is
 * addressed by the bare tenant id, not by a tenant/provider composite; this
 * deliberately strict grammar keeps provider ids portable across route
 * segments, configuration keys, audit dimensions, and durable rows.
 */
export const PROVIDER_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,63}$/;

/** Normalize `extractResourceId`'s string | string[] | undefined to an array. */
export function normalizeResourceIds(
  value: string | string[] | undefined,
): string[] {
  if (value === undefined) return [];
  return Array.isArray(value) ? value.filter((v) => v.length > 0) : [value];
}

/**
 * Config for the generic webhook provider — the core `WebhookSignalProvider`
 * knobs (`extractResourceId`, `buildNotification`) PLUS the signature check core
 * omits (a generic webhook has no signing scheme; a real one — GitHub — does).
 */
export interface WebhookSignalProviderConfig {
  id: string;
  name?: string;
  /** Optional poll cadence; a pure webhook provider omits it. */
  pollInterval?: number;
  /** Signature check (webhook providers). Omit ⇒ the provider accepts no webhooks. */
  verifyWebhookSignature?: (
    rawBody: Uint8Array,
    headers: WebhookHeaders,
    secret: string,
  ) => Promise<boolean> | boolean;
  /** The external-resource key(s) an incoming payload concerns (webhook providers). */
  extractResourceIds?: (payload: unknown) => string | string[] | undefined;
  /** Build the notification a matched subscription receives (REQUIRED — both paths). */
  buildNotification: (
    payload: unknown,
    subscription: SignalSubscription,
  ) => SendNotificationSignalInput;
  /** Poll seam (poll providers). Omit ⇒ webhook-only (no host-DO alarm). */
  pollForDeliveries?: (
    subscriptions: SignalSubscription[],
  ) => Promise<ProviderDelivery[]>;
}

/**
 * A generic webhook signal provider. Extends core's
 * `SignalProvider` so it remains a real, mergeable provider for in-process
 * `new Agent({ signals: [p] })` use, while implementing the flowsafe adapter the
 * DO host + webhook route drive. GitHub (github-provider.ts) is a specialization
 * of this with a concrete signature scheme.
 */
class WebhookSignalProviderAdapter
  extends SignalProvider
  implements SignalProviderAdapter
{
  readonly id: string;
  readonly name?: string;
  readonly pollInterval?: number;
  // The optional seams are PRESENCE-ACCURATE: assigned only when the config
  // supplies them, so a webhook-only provider genuinely has no pollForDeliveries
  // and a poll-only provider no verify/extract (the host + route optional-chain).
  readonly verifyWebhookSignature?: SignalProviderAdapter['verifyWebhookSignature'];
  readonly extractResourceIds?: SignalProviderAdapter['extractResourceIds'];
  readonly buildNotification: SignalProviderAdapter['buildNotification'];
  readonly pollForDeliveries?: SignalProviderAdapter['pollForDeliveries'];

  constructor(config: WebhookSignalProviderConfig) {
    super();
    if (!PROVIDER_ID_PATTERN.test(config.id)) {
      throw new Error(
        `signal provider id '${config.id}' must match ${PROVIDER_ID_PATTERN} (stable lowercase URL/config slug)`,
      );
    }
    this.id = config.id;
    this.name = config.name;
    this.pollInterval =
      config.pollInterval === undefined
        ? undefined
        : nonnegativeSafeInteger(
            config.pollInterval,
            `signal provider '${config.id}' pollInterval`,
          );
    this.buildNotification = config.buildNotification;
    const { verifyWebhookSignature, extractResourceIds, pollForDeliveries } =
      config;
    if (verifyWebhookSignature) {
      this.verifyWebhookSignature = verifyWebhookSignature;
    }
    if (extractResourceIds) {
      this.extractResourceIds = (payload) =>
        normalizeResourceIds(extractResourceIds(payload));
    }
    if (pollForDeliveries) this.pollForDeliveries = pollForDeliveries;
  }
}

/**
 * Build a generic webhook signal provider. Returns the flowsafe adapter the DO
 * host + webhook route depend on; the instance also IS a core `SignalProvider`.
 */
export function createWebhookSignalProvider(
  config: WebhookSignalProviderConfig,
): SignalProviderAdapter {
  return new WebhookSignalProviderAdapter(config);
}
