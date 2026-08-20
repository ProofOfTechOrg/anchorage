// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  deploymentIdentityDatabase,
  deploymentIdentityRequest,
  TEST_DEPLOYMENT_IDENTITY_SECRET,
} from '../../test-support/deployment-identity.js';
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
import { SIGNAL_PROVIDER_HOST_INSTANCE_NAME } from './host-topology.js';
import type { ProviderDelivery, SignalProviderAdapter } from './provider.js';
import {
  InMemorySubscriptionStoreFactory,
  type StoredSubscription,
  type SubscriptionStoreFactory,
} from './subscription-d1.js';

function stubTopology(
  addressed: string[],
  failingThreadId?: string,
): ThreadTopology {
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: (name) => name,
    get: (name) => ({
      fetch: (input: Request | string) => {
        addressed.push(name);
        void input;
        if (name === failingThreadId) {
          return Promise.reject(new Error('thread delivery failed'));
        }
        return Promise.resolve(new Response('{}', { status: 200 }));
      },
    }),
  };
  return createThreadTopology(namespace, TEST_DEPLOYMENT_IDENTITY_SECRET);
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
  identityDatabase?: ReturnType<typeof deploymentIdentityDatabase>;
}

class TestHost extends SignalProviderHost<TestEnv> {
  constructor(state: SignalProviderHostState | undefined, env: TestEnv) {
    const identityDatabase =
      env.identityDatabase ?? deploymentIdentityDatabase();
    super(
      state,
      Object.assign(env, {
        DEPLOYMENT_TENANT: 'acme',
        DEPLOYMENT_IDENTITY_SECRET: TEST_DEPLOYMENT_IDENTITY_SECRET,
        DB: identityDatabase,
      }),
    );
  }

  protected build(env: TestEnv): SignalProviderHostWiring {
    return {
      store: env.factory.store(),
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
  ownerKey: string,
  providerId: string,
  threadId: string,
): Promise<StoredSubscription> {
  return factory.store().subscribe({
    providerId,
    externalResourceId: `res:${threadId}`,
    threadId,
    resourceId: `${ownerKey}_owner`,
  });
}

describe('SignalProviderHost.poll', () => {
  it('rehydrates subscriptions from the store and delivers (the eviction-survivable path)', async () => {
    // #given — subscriptions persisted; a FRESH host (post-eviction) sees them
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    await seed(factory, 'acme', 'poller', 'acme_t2');
    const addressed: string[] = [];
    const host = new TestHost(state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME), {
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
    const host = new TestHost(state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME), {
      factory,
      topology: stubTopology(addressed),
      providers: [boom, pollProvider('ok')],
    });

    // #when
    const result = await host.poll();

    // #then — B delivered; A did not crash the poll
    expect(result).toEqual({ providersPolled: 2, delivered: 1 });
    expect(addressed).toEqual(['acme_t2']);
  });

  it('isolates a failing delivery — the rest of the batch proceeds (per-delivery isolation)', async () => {
    // #given — a provider emits two bound deliveries; the first thread route
    // fails, while the second is healthy.
    const factory = new InMemorySubscriptionStoreFactory();
    const failing = await seed(factory, 'acme', 'mix', 'delivery-fails');
    const valid = await seed(factory, 'acme', 'mix', 'delivery-ok');
    const mix: SignalProviderAdapter = {
      id: 'mix',
      buildNotification: () => ({ source: 'mix', kind: 'k', summary: 's' }),
      pollForDeliveries: async (): Promise<ProviderDelivery[]> => [
        {
          subscription: failing,
          notification: { source: 'mix', kind: 'k', summary: 's' },
        },
        {
          subscription: valid,
          notification: { source: 'mix', kind: 'k', summary: 's' },
        },
      ],
    };
    const addressed: string[] = [];
    const host = new TestHost(state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME), {
      factory,
      topology: stubTopology(addressed, 'delivery-fails'),
      providers: [mix],
    });

    // #when
    const result = await host.poll();

    // #then — the throwing delivery is isolated and the next one still lands.
    // An unclassifiable throw counts as deferred: it may well be an outage.
    expect(result).toEqual({ providersPolled: 1, delivered: 1, deferred: 1 });
    expect(addressed).toEqual(['delivery-fails', 'delivery-ok']);
  });
});

describe('SignalProviderHost alarm + routes', () => {
  it('pre-arms before a deployment identity failure consumes the one-shot alarm', async () => {
    const pollForDeliveries = vi.fn();
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, { setAlarm, deleteAlarm }),
      {
        factory: new InMemorySubscriptionStoreFactory(),
        topology: stubTopology([]),
        providers: [
          {
            id: 'poller',
            pollInterval: 30_000,
            buildNotification: () => ({
              source: 'poller',
              kind: 'poll',
              summary: 'poller',
            }),
            pollForDeliveries,
          },
        ],
        identityDatabase: deploymentIdentityDatabase('globex'),
      },
    );

    await expect(host.alarm()).rejects.toThrow("belongs to 'globex'");
    expect(pollForDeliveries).not.toHaveBeenCalled();
    expect(setAlarm).toHaveBeenCalledTimes(1);
    expect(deleteAlarm).not.toHaveBeenCalled();
  });

  it('rejects a wrong fixed instance name before touching alarm storage', async () => {
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const host = new TestHost(
      state('wrong-instance', { setAlarm, deleteAlarm }),
      {
        factory: new InMemorySubscriptionStoreFactory(),
        topology: stubTopology([]),
        providers: [pollProvider('poller', 30_000)],
      },
    );

    await expect(host.alarm()).rejects.toThrow(
      `must be addressed as '${SIGNAL_PROVIDER_HOST_INSTANCE_NAME}'`,
    );
    expect(setAlarm).not.toHaveBeenCalled();
    expect(deleteAlarm).not.toHaveBeenCalled();
  });

  it('rejects when re-arming fails so workerd retries the alarm', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const setAlarm = vi.fn(() =>
      Promise.reject(new Error('alarm unavailable')),
    );
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, {
        setAlarm,
        deleteAlarm: vi.fn(),
      }),
      {
        factory,
        topology: stubTopology([]),
        providers: [pollProvider('poller', 30_000)],
      },
    );

    await expect(host.alarm()).rejects.toThrow('alarm unavailable');
  });

  it('rejects a cross-deployment caller before touching the fixed provider host', async () => {
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, { setAlarm, deleteAlarm }),
      {
        factory: new InMemorySubscriptionStoreFactory(),
        topology: stubTopology([]),
        providers: [pollProvider('poller', 30_000)],
      },
    );
    const response = await host.fetch(
      deploymentIdentityRequest(
        'http://host/arm',
        { method: 'POST' },
        'different-deployment-identity-secret',
      ),
    );

    expect(response.status).toBe(503);
    expect(setAlarm).not.toHaveBeenCalled();
    expect(deleteAlarm).not.toHaveBeenCalled();
  });

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
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, { setAlarm, deleteAlarm }),
      {
        factory,
        topology: stubTopology([]),
        providers: [boom],
      },
    );

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
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, { setAlarm, deleteAlarm }),
      {
        factory,
        topology: stubTopology([]),
        providers: [pollProvider('poller', 30_000)],
      },
    );

    // #when — arm via the route
    const res = await host.fetch(
      deploymentIdentityRequest('http://host/arm', { method: 'POST' }),
    );

    // #then — nothing to poll ⇒ the alarm is deleted, not set
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ armed: true });
    expect(deleteAlarm).toHaveBeenCalledTimes(1);
    expect(setAlarm).not.toHaveBeenCalled();
  });

  it('serializes a no-subscription disarm against a concurrent subscription arm', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    const store = factory.store();
    const originalList = store.listForProvider.bind(store);
    let releaseFirst: () => void = () => undefined;
    let firstListed: () => void = () => undefined;
    const firstStarted = new Promise<void>((resolve) => {
      firstListed = resolve;
    });
    const firstBlocked = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    vi.spyOn(store, 'listForProvider')
      .mockImplementationOnce(async () => {
        firstListed();
        await firstBlocked;
        return [];
      })
      .mockImplementation((providerId) => originalList(providerId));

    let alarm: number | null = 1;
    const events: string[] = [];
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, {
        getAlarm: async () => alarm,
        setAlarm: async (scheduledTime) => {
          events.push('set');
          alarm = scheduledTime;
        },
        deleteAlarm: async () => {
          events.push('delete');
          alarm = null;
        },
      }),
      {
        factory,
        topology: stubTopology([]),
        providers: [pollProvider('poller', 30_000)],
      },
    );

    const first = host.fetch(
      deploymentIdentityRequest('http://host/arm', { method: 'POST' }),
    );
    await firstStarted;
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const second = host.fetch(
      deploymentIdentityRequest('http://host/arm', { method: 'POST' }),
    );
    releaseFirst();
    await Promise.all([first, second]);

    expect(events).toEqual(['delete', 'set']);
    expect(alarm).not.toBeNull();
  });

  it('keeps an already earlier alarm instead of postponing it on subscription writes', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const getAlarm = vi.fn(async () => 20_000);
    const now = vi.spyOn(Date, 'now').mockReturnValue(10_000);
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, {
        setAlarm,
        deleteAlarm,
        getAlarm,
      }),
      {
        factory,
        topology: stubTopology([]),
        providers: [pollProvider('poller', 30_000)],
      },
    );

    try {
      const response = await host.fetch(
        deploymentIdentityRequest('http://host/arm', { method: 'POST' }),
      );
      expect(response.status).toBe(200);
      expect(getAlarm).toHaveBeenCalledTimes(1);
      expect(setAlarm).not.toHaveBeenCalled();
      expect(deleteAlarm).not.toHaveBeenCalled();
    } finally {
      now.mockRestore();
    }
  });

  it('moves an existing alarm earlier when a faster cadence requires it', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'fast', 'acme_t1');
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const getAlarm = vi.fn(async () => 100_000);
    const now = vi.spyOn(Date, 'now').mockReturnValue(10_000);
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, {
        setAlarm,
        deleteAlarm,
        getAlarm,
      }),
      {
        factory,
        topology: stubTopology([]),
        providers: [pollProvider('fast', 5_000)],
      },
    );

    try {
      const response = await host.fetch(
        deploymentIdentityRequest('http://host/arm', { method: 'POST' }),
      );
      expect(response.status).toBe(200);
      expect(setAlarm).toHaveBeenCalledWith(15_000);
      expect(deleteAlarm).not.toHaveBeenCalled();
    } finally {
      now.mockRestore();
    }
  });

  it('sets an alarm when storage reports none', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const getAlarm = vi.fn(async () => null);
    const now = vi.spyOn(Date, 'now').mockReturnValue(10_000);
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, {
        setAlarm,
        deleteAlarm,
        getAlarm,
      }),
      {
        factory,
        topology: stubTopology([]),
        providers: [pollProvider('poller', 30_000)],
      },
    );

    try {
      await host.fetch(
        deploymentIdentityRequest('http://host/arm', { method: 'POST' }),
      );
      expect(setAlarm).toHaveBeenCalledWith(40_000);
    } finally {
      now.mockRestore();
    }
  });

  it('treats zero as manual-only polling and disarms automatic wakeups', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'manual', 'acme_t1');
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, { setAlarm, deleteAlarm }),
      {
        factory,
        topology: stubTopology([]),
        providers: [pollProvider('manual', 0)],
      },
    );

    const response = await host.fetch(
      deploymentIdentityRequest('http://host/arm', { method: 'POST' }),
    );
    expect(response.status).toBe(200);
    expect(deleteAlarm).toHaveBeenCalledTimes(1);
    expect(setAlarm).not.toHaveBeenCalled();
  });

  it.each([
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
  ])('defensively refuses a hand-built provider with invalid interval %s', async (pollInterval) => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'invalid', 'acme_t1');
    const setAlarm = vi.fn();
    const deleteAlarm = vi.fn();
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, { setAlarm, deleteAlarm }),
      {
        factory,
        topology: stubTopology([]),
        providers: [pollProvider('invalid', pollInterval)],
      },
    );
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      const response = await host.fetch(
        deploymentIdentityRequest('http://host/arm', { method: 'POST' }),
      );
      expect(response.status).toBe(500);
      expect(setAlarm).not.toHaveBeenCalled();
      expect(deleteAlarm).not.toHaveBeenCalled();
    } finally {
      log.mockRestore();
    }
  });

  it('POST /poll runs a poll; an unknown route 404s', async () => {
    // #given
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const host = new TestHost(state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME), {
      factory,
      topology: stubTopology([]),
      providers: [pollProvider('poller')],
    });
    // #then
    const poll = await host.fetch(
      deploymentIdentityRequest('http://host/poll', { method: 'POST' }),
    );
    expect(poll.status).toBe(200);
    expect(await poll.json()).toMatchObject({ providersPolled: 1 });
    const missing = await host.fetch(
      deploymentIdentityRequest('http://host/nope'),
    );
    expect(missing.status).toBe(404);
  });

  it('403s when the DO is not addressed under the deployment singleton', async () => {
    const host = new TestHost(state('Bad_Name'), {
      factory: new InMemorySubscriptionStoreFactory(),
      topology: stubTopology([]),
      providers: [],
    });
    // #then — the identity getter throws, mapped to 403 by the DO taxonomy
    const res = await host.fetch(
      deploymentIdentityRequest('http://host/poll', { method: 'POST' }),
    );
    expect(res.status).toBe(403);
  });
});
