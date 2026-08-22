// SPDX-License-Identifier: Apache-2.0
// Alarm-driven provider host for one physically isolated deployment.

import type { DurableObjectState } from '@cloudflare/workers-types';

import {
  DoStatusError,
  doErrorResponse,
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from '../do-runner/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import { positiveSafeInteger } from '../numeric-config.js';
import {
  classifyDeliveryError,
  classifyDeliveryResponse,
  deliverNotification,
  isTerminalDelivery,
} from './delivery.js';
import { SIGNAL_PROVIDER_HOST_INSTANCE_NAME } from './host-topology.js';
import type { SignalProviderAdapter } from './provider.js';
import type { SubscriptionStore } from './subscription-d1.js';

export interface AlarmStorage {
  setAlarm(scheduledTime: number): void | Promise<void>;
  deleteAlarm(): void | Promise<void>;
  getAlarm?(): number | null | Promise<number | null>;
}

export interface SignalProviderHostState {
  readonly id: { readonly name?: string };
  readonly storage?: AlarmStorage;
}

type AssertTrue<T extends true> = T;
type _StateSatisfies = AssertTrue<
  DurableObjectState extends SignalProviderHostState ? true : false
>;

export class SignalProviderHostIdentityError extends DoStatusError {
  readonly status = 403;

  constructor(message: string) {
    super(message);
    this.name = 'SignalProviderHostIdentityError';
  }
}

export interface SignalProviderHostWiring {
  store: SubscriptionStore;
  topology: ThreadTopology;
  providers: readonly SignalProviderAdapter[];
}

export interface PollResult {
  providersPolled: number;
  delivered: number;
  /** Deliveries the thread Durable Object refused on content. Omitted when zero. */
  denied?: number;
  /** Deliveries a defect made undeliverable — an adapter or stored-row bug. Omitted when zero. */
  failed?: number;
  /**
   * Deliveries this deployment could not decide. Omitted when zero. Recovery is
   * the provider's: a poll adapter re-reports state it has not seen accepted,
   * so the next poll re-delivers. An adapter that advances its own cursor on
   * every returned delivery drops these.
   */
  deferred?: number;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

export abstract class SignalProviderHost<TEnv = unknown> {
  protected readonly env: TEnv;
  protected readonly state?: SignalProviderHostState;
  #wiring?: SignalProviderHostWiring;
  #identityReady?: Promise<void>;
  #armTail = Promise.resolve();

  constructor(state: SignalProviderHostState | undefined, env: TEnv) {
    this.state = state;
    this.env = env;
  }

  protected abstract build(env: TEnv): SignalProviderHostWiring;

  #verifyIdentity(): Promise<void> {
    this.#identityReady ??= verifyDurableObjectDeploymentIdentity(
      this.state,
      this.env,
    )
      .then(() => {
        this.#verifyInstanceName();
      })
      .catch((error: unknown) => {
        this.#identityReady = undefined;
        throw error;
      });
    return this.#identityReady;
  }

  #verifyInstanceName(): void {
    if (
      this.state !== undefined &&
      this.state.id.name !== SIGNAL_PROVIDER_HOST_INSTANCE_NAME
    ) {
      throw new SignalProviderHostIdentityError(
        `SignalProviderHost must be addressed as '${SIGNAL_PROVIDER_HOST_INSTANCE_NAME}'`,
      );
    }
  }

  #ensureWiring(): SignalProviderHostWiring {
    this.#wiring ??= this.build(this.env);
    return this.#wiring;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      await verifyDurableObjectDeploymentRequest(request, this.state, this.env);
      await this.#verifyIdentity();
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

  async alarm(): Promise<void> {
    // The instance name is local and safe to reject before touching storage.
    // D1 identity verification can fail transiently, so persist the next wake
    // before that external I/O consumes this one-shot platform alarm.
    this.#verifyInstanceName();
    await this.#withArmLock(async () => {
      const prearmedAt = await this.#prearmUnlocked();
      await this.#verifyIdentity();
      await this.poll();
      await this.#armUnlocked(prearmedAt);
    });
  }

  async poll(): Promise<PollResult> {
    await this.#verifyIdentity();
    const { store, topology, providers } = this.#ensureWiring();
    let providersPolled = 0;
    let delivered = 0;
    let denied = 0;
    let failed = 0;
    let deferred = 0;
    for (const provider of providers) {
      if (!provider.pollForDeliveries) continue;
      providersPolled += 1;
      try {
        const subscriptions = await store.listForProvider(provider.id);
        const authorized = new Map(
          subscriptions.map((subscription) => [subscription.id, subscription]),
        );
        const deliveries = await provider.pollForDeliveries(subscriptions);
        for (const delivery of deliveries) {
          const subscription = authorized.get(delivery.subscription.id);
          if (!subscription) {
            // A provider handing back a row this host never authorized is a
            // defect in the adapter, not a transient condition; polling again
            // would return it again.
            failed += 1;
            console.error(
              JSON.stringify({
                type: 'signal-provider.delivery-error',
                providerId: provider.id,
                terminal: true,
                reason: `provider '${provider.id}' returned an unbound subscription`,
              }),
            );
            continue;
          }
          try {
            const response = await deliverNotification(
              topology,
              subscription,
              delivery.notification,
            );
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
                type: 'signal-provider.delivery-rejected',
                providerId: provider.id,
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
                type: 'signal-provider.delivery-error',
                providerId: provider.id,
                terminal: isTerminalDelivery(outcome),
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
    return {
      providersPolled,
      delivered,
      ...(denied > 0 ? { denied } : {}),
      ...(failed > 0 ? { failed } : {}),
      ...(deferred > 0 ? { deferred } : {}),
    };
  }

  async #arm(): Promise<void> {
    await this.#withArmLock(() => this.#armUnlocked());
  }

  async #withArmLock<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.#armTail;
    let release: () => void = () => undefined;
    this.#armTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  async #prearmUnlocked(): Promise<number | undefined> {
    const storage = this.state?.storage;
    if (!storage) return undefined;
    let interval: number | undefined;
    for (const provider of this.#ensureWiring().providers) {
      if (!provider.pollForDeliveries) continue;
      const configured = provider.pollInterval;
      if (configured === undefined || configured === 0) continue;
      const providerInterval = positiveSafeInteger(
        configured,
        `signal provider '${provider.id}' pollInterval`,
      );
      interval =
        interval === undefined
          ? providerInterval
          : Math.min(interval, providerInterval);
    }
    if (interval === undefined) return undefined;
    const desired = Date.now() + interval;
    const current = await storage.getAlarm?.();
    if (current === undefined || current === null || current > desired) {
      await storage.setAlarm(desired);
    }
    return current === undefined || current === null || current > desired
      ? desired
      : current;
  }

  async #armUnlocked(prearmedAt?: number): Promise<void> {
    await this.#verifyIdentity();
    const storage = this.state?.storage;
    if (!storage) return;
    const { store, providers } = this.#ensureWiring();
    let interval: number | undefined;
    for (const provider of providers) {
      if (!provider.pollForDeliveries) continue;
      const configured = provider.pollInterval;
      if (configured === undefined || configured === 0) continue;
      const providerInterval = positiveSafeInteger(
        configured,
        `signal provider '${provider.id}' pollInterval`,
      );
      const subscriptions = await store.listForProvider(provider.id);
      if (subscriptions.length === 0) continue;
      interval =
        interval === undefined
          ? providerInterval
          : Math.min(interval, providerInterval);
    }
    if (interval === undefined) {
      await storage.deleteAlarm();
      return;
    }
    const desired = Date.now() + interval;
    const current = (await storage.getAlarm?.()) ?? prearmedAt;
    if (current === undefined || current === null || current > desired) {
      await storage.setAlarm(desired);
    }
  }
}
