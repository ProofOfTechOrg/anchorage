// SPDX-License-Identifier: Apache-2.0
import type { SignalSubscription } from '@mastra/core/signals';
import { describe, expect, it, vi } from 'vitest';

import {
  createThreadTopology,
  type ThreadNamespaceLike,
  type ThreadTopology,
} from '../host-kit/index.js';
import {
  type PollResult,
  SignalProviderHost,
  type SignalProviderHostState,
  type SignalProviderHostWiring,
} from './host-do.js';
import type { ProviderDelivery, SignalProviderAdapter } from './provider.js';
import {
  InMemorySubscriptionStoreFactory,
  type SubscriptionStoreFactory,
} from './subscription-d1.js';

function stubTopology(addressed: string[]): ThreadTopology {
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: (name) => name,
    get: (name) => ({
      fetch: (input: Request | string) => {
        addressed.push(name);
        void input;
        return Promise.resolve(new Response('{}', { status: 200 }));
      },
    }),
  };
  return createThreadTopology(namespace);
}

/** A poll provider that emits one notification per subscription. */
function pollProvider(
  id: string,
  pollInterval?: number,
): SignalProviderAdapter {
  return {
    id,
    ...(pollInterval !== undefined ? { pollInterval } : {}),
    buildNotification: () => ({ source: id, kind: 'poll', summary: id }),
    pollForDeliveries: async (subscriptions) =>
      subscriptions.map((subscription) => ({
        subscription,
        notification: { source: id, kind: 'poll', summary: id },
      })),
  };
}

interface TestEnv {
  factory: SubscriptionStoreFactory;
  topology: ThreadTopology;
  providers: readonly SignalProviderAdapter[];
}

class TestHost extends SignalProviderHost<TestEnv> {
  protected build(env: TestEnv, tenantId: string): SignalProviderHostWiring {
    return {
      store: env.factory.forTenant(tenantId),
      topology: env.topology,
      providers: env.providers,
    };
  }
}

function state(
  name: string | undefined,
  storage?: SignalProviderHostState['storage'],
): SignalProviderHostState {
  return { id: { name }, ...(storage ? { storage } : {}) };
}

async function seed(
  factory: SubscriptionStoreFactory,
  tenant: string,
  providerId: string,
  threadId: string,
): Promise<void> {
  await factory.forTenant(tenant).subscribe({
    providerId,
    externalResourceId: `res:${threadId}`,
    threadId,
    resourceId: `${tenant}_owner`,
  });
}

describe('SignalProviderHost.poll', () => {
  it('rehydrates subscriptions from the store and delivers (the eviction-survivable path)', async () => {
    // #given — subscriptions persisted; a FRESH host (post-eviction) sees them
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    await seed(factory, 'acme', 'poller', 'acme_t2');
    const addressed: string[] = [];
    const host = new TestHost(state('acme'), {
      factory,
      topology: stubTopology(addressed),
      providers: [pollProvider('poller', 60_000)],
    });

    // #when
    const result: PollResult = await host.poll();

    // #then — rehydrated the two rows and delivered both
    expect(result).toEqual({ providersPolled: 1, delivered: 2 });
    expect(addressed.sort()).toEqual(['acme_t1', 'acme_t2']);
  });

  it('isolates a throwing provider — its siblings still poll (per-provider isolation)', async () => {
    // #given — provider A throws, provider B works; both have subscriptions
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'boom', 'acme_t1');
    await seed(factory, 'acme', 'ok', 'acme_t2');
    const boom: SignalProviderAdapter = {
      id: 'boom',
      buildNotification: () => ({ source: 'boom', kind: 'k', summary: 's' }),
      pollForDeliveries: () => Promise.reject(new Error('provider down')),
    };
    const addressed: string[] = [];
    const host = new TestHost(state('acme'), {
      factory,
      topology: stubTopology(addressed),
      providers: [boom, pollProvider('ok')],
    });

    // #when
    const result = await host.poll();

    // #then — B delivered; A did not crash the poll
    expect(result).toEqual({ providersPolled: 1, delivered: 1 });
    expect(addressed).toEqual(['acme_t2']);
  });

  it('isolates a failing delivery — the rest of the batch proceeds (per-delivery isolation)', async () => {
    // #given — a provider that emits one valid + one FOREIGN-thread delivery
    const factory = new InMemorySubscriptionStoreFactory();
    const valid: SignalSubscription = {
      id: 'acme_s1',
      providerId: 'mix',
      threadId: 'acme_ok',
      resourceId: 'acme_owner',
      externalResourceId: 'res:1',
      subscribedAt: new Date(0),
      metadata: {},
    };
    const foreign: SignalSubscription = { ...valid, threadId: 'globex_bad' };
    const mix: SignalProviderAdapter = {
      id: 'mix',
      buildNotification: () => ({ source: 'mix', kind: 'k', summary: 's' }),
      pollForDeliveries: async (): Promise<ProviderDelivery[]> => [
        {
          subscription: valid,
          notification: { source: 'mix', kind: 'k', summary: 's' },
        },
        {
          subscription: foreign,
          notification: { source: 'mix', kind: 'k', summary: 's' },
        },
      ],
    };
    const addressed: string[] = [];
    const host = new TestHost(state('acme'), {
      factory,
      topology: stubTopology(addressed),
      providers: [mix],
    });

    // #when
    const result = await host.poll();

    // #then — the foreign-thread delivery 404s (throws) and is skipped; the valid
    // one lands, and the foreign thread DO is never addressed (fail closed).
    expect(result).toEqual({ providersPolled: 1, delivered: 1 });
    expect(addressed).toEqual(['acme_ok']);
  });
});

describe('SignalProviderHost alarm + routes', () => {
  it('arms at the pollInterval when there are subscriptions, and re-arms even after a throwing poll', async () => {
    // #given — one provider that THROWS on poll but has subscriptions + interval
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'boom', 'acme_t1');
    const boom: SignalProviderAdapter = {
      id: 'boom',
      pollInterval: 30_000,
      buildNotification: () => ({ source: 'boom', kind: 'k', summary: 's' }),
      pollForDeliveries: () => Promise.reject(new Error('down')),
    };
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const host = new TestHost(state('acme', { setAlarm, deleteAlarm }), {
      factory,
      topology: stubTopology([]),
      providers: [boom],
    });

    // #when — the alarm fires (poll throws internally)
    await host.alarm();

    // #then — availability: it re-armed despite the poll failure
    expect(setAlarm).toHaveBeenCalledTimes(1);
    expect(deleteAlarm).not.toHaveBeenCalled();
  });

  it('self-terminates (deleteAlarm) when nothing is left to poll', async () => {
    // #given — a poll provider with an interval but NO subscriptions
    const factory = new InMemorySubscriptionStoreFactory();
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const host = new TestHost(state('acme', { setAlarm, deleteAlarm }), {
      factory,
      topology: stubTopology([]),
      providers: [pollProvider('poller', 30_000)],
    });

    // #when — arm via the route
    const res = await host.fetch(
      new Request('http://host/arm', { method: 'POST' }),
    );

    // #then — nothing to poll ⇒ the alarm is deleted, not set
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ armed: true });
    expect(deleteAlarm).toHaveBeenCalledTimes(1);
    expect(setAlarm).not.toHaveBeenCalled();
  });

  it('POST /poll runs a poll; an unknown route 404s', async () => {
    // #given
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const host = new TestHost(state('acme'), {
      factory,
      topology: stubTopology([]),
      providers: [pollProvider('poller')],
    });
    // #then
    const poll = await host.fetch(
      new Request('http://host/poll', { method: 'POST' }),
    );
    expect(poll.status).toBe(200);
    expect(await poll.json()).toMatchObject({ providersPolled: 1 });
    const missing = await host.fetch(new Request('http://host/nope'));
    expect(missing.status).toBe(404);
  });

  it('403s when the DO name carries no INV-3 tenant (fail closed)', async () => {
    // #given — a name that is not an INV-3 tenantId
    const host = new TestHost(state('Bad_Name'), {
      factory: new InMemorySubscriptionStoreFactory(),
      topology: stubTopology([]),
      providers: [],
    });
    // #then — the identity getter throws, mapped to 403 by the DO taxonomy
    const res = await host.fetch(
      new Request('http://host/poll', { method: 'POST' }),
    );
    expect(res.status).toBe(403);
  });
});
