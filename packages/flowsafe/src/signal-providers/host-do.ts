// SPDX-License-Identifier: Apache-2.0
// Track E (M-007), CI-M-007-001 — the provider host Durable Object (DL-017).
//
// WHY a DO + alarm: core's SignalProvider expects a long-lived process — a
// `startPolling` setInterval loop over an in-memory subscription registry
// (signal-provider.d.ts). Workers has neither. So the DO ALARM drives polling
// (it wakes an evicted DO with no live request) and D1 holds the subscriptions
// (core's registry is lost on eviction) — the host rehydrates them from D1 at
// each boot/poll (the E-S3 in-memory-lost, D1-restored proof).
//
// ADDRESSING — per TENANT (`idFromName(tenantId)`, the HubDurableObject pattern):
// id.name IS the bare tenantId, so the instance is tenant-disjoint by
// construction (like the hub DO) and its subscription store binds to exactly that
// tenant (INV-2). It hosts ALL of the tenant's providers on one alarm, each
// provider's poll+delivery wrapped in its OWN try/catch — PER-PROVIDER FAILURE
// ISOLATION: a provider whose poll throws never starves its siblings, and one
// tampered/failing delivery never aborts the rest of the batch. (Per-tenant, not
// per-tenant×provider: a per-provider DO would make cross-provider isolation an
// untestable DO boundary and multiply instances; per-tenant keeps the isolation
// inside one alarm where it is exercised, and matches the hub DO's
// id.name===tenantId house pattern.)
//
// The host is reached ONLY internally — the subscribe route arms it, the alarm
// polls it — never by a client, so (like the hub DO) it needs no tenant-header
// assertion: id.name === the addressing tenant by construction, validated INV-3.
// Classic ctor(state, env) + fetch (+ alarm), NOT `extends DurableObject` — so it
// and its graph load in node/vitest, the ThreadDurableObject/HubDurableObject
// posture.

// Type-only workers-types import: erased at build (never in the emitted .d.ts),
// so consumers pull no workers-types dependency — the cf-types.ts convention.
import type { DurableObjectState } from '@cloudflare/workers-types';

import {
  DoStatusError,
  doErrorResponse,
  TENANT_ID_PATTERN,
} from '../do-runner/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import { deliverNotification } from './delivery.js';
import type { SignalProviderAdapter } from './provider.js';
import type {
  StoredSubscription,
  TenantBoundSubscriptionStore,
} from './subscription-d1.js';

/** The `ctx.storage` alarm subset the host DO arms itself through. */
export interface AlarmStorage {
  setAlarm(scheduledTime: number): void | Promise<void>;
  deleteAlarm(): void | Promise<void>;
}

/**
 * The DO-state subset the host reads: its own `id.name` (the tenant) and the
 * alarm storage. `storage` is OPTIONAL so a node/vitest stub that sets only
 * `id.name` satisfies it and drives `poll()` directly (no alarm off workerd).
 */
export interface SignalProviderHostState {
  readonly id: { readonly name?: string };
  readonly storage?: AlarmStorage;
}

// Drift pin (type-only, erased): the real DurableObjectState must still satisfy
// the subset — catches a workers-types rename of setAlarm/deleteAlarm, the same
// posture cf-types.ts takes for the runner/hub states.
type AssertTrue<T extends true> = T;
type _StateSatisfies = AssertTrue<
  DurableObjectState extends SignalProviderHostState ? true : false
>;

/**
 * A host DO whose name carries no INV-3 tenant. 403 — this boundary is internal
 * (not client-reachable), so there is no oracle to protect and a distinct status
 * keeps a routing bug from reading as a 500. Extends DoStatusError so
 * doErrorResponse recognizes it.
 */
export class SignalProviderHostIdentityError extends DoStatusError {
  readonly status = 403;
  constructor(message: string) {
    super(message);
    this.name = 'SignalProviderHostIdentityError';
  }
}

/** The per-instance wiring a subclass supplies, memoized by the base. */
export interface SignalProviderHostWiring {
  /** Subscription store BOUND to this host's tenant (the rehydration source). */
  store: TenantBoundSubscriptionStore;
  /** The sanctioned reach into a thread DO (delivery). */
  topology: ThreadTopology;
  /** Every provider this tenant hosts. */
  providers: readonly SignalProviderAdapter[];
}

export interface PollResult {
  providersPolled: number;
  delivered: number;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

/**
 * The provider host DO base. Subclass it, supply `build()` (the tenant-bound
 * subscription store + thread topology + provider list from env), and bind the
 * subclass under a wrangler namespace addressed `idFromName(tenantId)`.
 *
 * Routes: `POST /arm` (boot + arm the alarm), `POST /poll` (run one poll cycle —
 * the deterministic probe the spike drives so E-S3 does not depend on wrangler's
 * alarm timer). `alarm()` polls then re-arms.
 */
export abstract class SignalProviderHost<TEnv = unknown> {
  protected readonly env: TEnv;
  protected readonly state?: SignalProviderHostState;
  #wiring?: SignalProviderHostWiring;

  constructor(state: SignalProviderHostState | undefined, env: TEnv) {
    this.state = state;
    this.env = env;
  }

  /** Build the tenant-bound wiring from env + the asserted tenant. Memoized. */
  protected abstract build(
    env: TEnv,
    tenantId: string,
  ): SignalProviderHostWiring;

  /**
   * The tenant this host serves, recovered from its OWN idFromName identity —
   * `id.name` IS the bare tenantId (the hub DO posture). Validated INV-3 and
   * fail-closed: a name carrying no valid tenant cannot scope a store.
   */
  protected get tenantId(): string {
    const name = this.state?.id?.name;
    if (name === undefined || !TENANT_ID_PATTERN.test(name)) {
      throw new SignalProviderHostIdentityError(
        `SignalProviderHost: id.name '${name ?? '<none>'}' is not an INV-3 tenantId — the host is addressed idFromName(tenantId); refusing to serve`,
      );
    }
    return name;
  }

  #ensureWiring(): SignalProviderHostWiring {
    if (!this.#wiring) {
      this.#wiring = this.build(this.env, this.tenantId);
    }
    return this.#wiring;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const path = new URL(request.url).pathname;
      if (request.method === 'POST' && path === '/arm') {
        await this.#arm();
        return json({ armed: true });
      }
      if (request.method === 'POST' && path === '/poll') {
        return json(await this.poll());
      }
      return json({ error: 'not found' }, 404);
    } catch (error) {
      return doErrorResponse(error);
    }
  }

  /** DO alarm: poll, then re-arm regardless (availability — a failed poll must not stop polling). */
  async alarm(): Promise<void> {
    try {
      await this.poll();
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'signal-provider.alarm-error',
          reason: error instanceof Error ? error.message : String(error),
        }),
      );
    }
    // Re-arm in its OWN guard: #arm re-invokes build() via #ensureWiring, so a
    // persistently broken wiring must not throw uncaught out of the alarm handler.
    try {
      await this.#arm();
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'signal-provider.arm-error',
          reason: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  }

  /**
   * Run one poll cycle across this tenant's poll-capable providers, rehydrating
   * each provider's subscriptions from D1 first (the eviction-survivable path).
   * PER-PROVIDER isolation: a provider whose poll throws is logged and skipped,
   * the rest still run; PER-DELIVERY isolation: a failing/tampered delivery is
   * logged and skipped, the batch continues. Public so a host/test drives it
   * directly (the E-S3 probe).
   */
  async poll(): Promise<PollResult> {
    const { store, topology, providers } = this.#ensureWiring();
    const tenantId = this.tenantId;
    let providersPolled = 0;
    let delivered = 0;
    for (const provider of providers) {
      if (!provider.pollForDeliveries) continue;
      try {
        const subscriptions = await store.listForProvider(provider.id);
        const deliveries = await provider.pollForDeliveries(subscriptions);
        providersPolled += 1;
        for (const delivery of deliveries) {
          try {
            // This host's tenant is authoritative for the poll path (the store
            // was bound to it); attach it so delivery binds to the right tenant,
            // and the topology re-checks ownership (a foreign threadId 404s).
            const subscription: StoredSubscription = {
              ...delivery.subscription,
              tenantId,
            };
            const response = await deliverNotification(
              topology,
              subscription,
              delivery.notification,
            );
            if (response.ok) {
              delivered += 1;
            } else {
              // A matched delivery the thread DO rejected for content (not an
              // ownership 404 — that throws): log so it is not silently dropped.
              console.error(
                JSON.stringify({
                  type: 'signal-provider.delivery-rejected',
                  providerId: provider.id,
                  status: response.status,
                }),
              );
            }
          } catch (error) {
            console.error(
              JSON.stringify({
                type: 'signal-provider.delivery-error',
                providerId: provider.id,
                reason: error instanceof Error ? error.message : String(error),
              }),
            );
          }
        }
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'signal-provider.poll-error',
            providerId: provider.id,
            reason: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    }
    return { providersPolled, delivered };
  }

  /**
   * Arm the alarm at the MIN pollInterval of providers that both poll AND have
   * subscriptions. Nothing to poll ⇒ delete the alarm (self-terminating: a
   * tenant with no live subscriptions costs no wakeups). No storage (node/vitest)
   * ⇒ no-op, and tests drive `poll()` directly.
   */
  async #arm(): Promise<void> {
    const storage = this.state?.storage;
    if (!storage) return;
    const { store, providers } = this.#ensureWiring();
    let interval: number | undefined;
    for (const provider of providers) {
      if (!provider.pollForDeliveries || !provider.pollInterval) continue;
      const subscriptions = await store.listForProvider(provider.id);
      if (subscriptions.length === 0) continue;
      interval =
        interval === undefined
          ? provider.pollInterval
          : Math.min(interval, provider.pollInterval);
    }
    if (interval === undefined) {
      await storage.deleteAlarm();
      return;
    }
    await storage.setAlarm(Date.now() + interval);
  }
}
