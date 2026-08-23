// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  deploymentIdentityDatabase,
  deploymentIdentityRequest,
  TEST_DEPLOYMENT_IDENTITY_SECRET,
} from '../../test-support/deployment-identity.js';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  type ExecutionFenceDatabase,
  ExecutionFencedError,
  type ExecutionFenceState,
  ExecutionFenceStore,
} from '../do-runner/index.js';
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

/**
 * The parsed JSON log line of one event type — and THE only one of that type.
 *
 * Reading `terminal` off this entry is what binds the verdict to the event: two
 * independent `stringContaining` asserts, one for the type and one for
 * `"terminal":false`, both pass when a different line happens to carry the flag.
 * Requiring exactly one match is what makes "binds to the event" literally true
 * rather than merely likely — several lanes here emit the same type, so a test
 * that logged two of them would otherwise assert against whichever came first.
 */
function loggedEvent(
  logged: { mock: { calls: readonly unknown[][] } },
  type: string,
): Record<string, unknown> {
  const entries = logged.mock.calls.map(
    ([line]) => JSON.parse(String(line)) as Record<string, unknown>,
  );
  const seen = entries.map((entry) => String(entry.type));
  const matches = entries.filter((entry) => entry.type === type);
  expect(
    matches.length,
    `expected exactly one ${type} log line, saw [${seen.join(', ')}] — with more than one, reading a field off "the" entry asserts against whichever came first`,
  ).toBe(1);
  return matches[0] as Record<string, unknown>;
}

/**
 * A thread topology whose route answers every delivery with `status`. A status
 * the DO chose deliberately is a different lane from `failingThreadId`, which
 * makes that one route THROW — the first is classified from the response, the
 * second from the error.
 */
function stubTopology(
  addressed: string[],
  failingThreadId?: string,
  status = 200,
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
        return Promise.resolve(new Response('{}', { status }));
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
  executionFence?: ExecutionFenceStore;
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
      // 'none' where a case has no fence: the subscription store is backed by
      // an in-memory factory, so there is no database to fence. The fence cases
      // supply a real store.
      executionFence: env.executionFence ?? 'none',
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
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      // #when
      const result = await host.poll();

      // #then — the throwing delivery is isolated and the next one still lands.
      // An unclassifiable throw counts as deferred: it may well be an outage,
      // and the log line has to say so or an operator cannot tell this from a
      // drop.
      expect(result).toEqual({ providersPolled: 1, delivered: 1, deferred: 1 });
      expect(addressed).toEqual(['delivery-fails', 'delivery-ok']);
      expect(
        loggedEvent(logged, 'signal-provider.delivery-error').terminal,
      ).toBe(false);
    } finally {
      logged.mockRestore();
    }
  });

  const providerId = 'provider';

  // The thread route refuses a poll delivery two different ways, and the log
  // line has to tell them apart: the poll host has no reply to shape, so
  // `terminal` is the only signal an operator gets about whether the next poll
  // could land the same event.
  it.each([
    // Redelivering the identical notification could only be denied again.
    {
      scenario: 'a content denial',
      status: 422,
      providerId,
      tally: { providersPolled: 1, delivered: 0, denied: 1 },
      terminal: true,
    },
    // The deployment could not decide, e.g. a policy evaluator down, so the
    // verdict can still change and the next poll is worth something.
    {
      scenario: 'a deployment outage',
      status: 503,
      providerId,
      tally: { providersPolled: 1, delivered: 0, deferred: 1 },
      terminal: false,
    },
  ])('records $scenario as terminal=$terminal (the DO answered $status)', async ({
    status,
    providerId,
    tally,
    terminal,
  }) => {
    // #given — a bound delivery the thread DO answers with that status
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', providerId, 'acme_t1');
    const addressed: string[] = [];
    const host = new TestHost(state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME), {
      factory,
      topology: stubTopology(addressed, undefined, status),
      providers: [pollProvider(providerId)],
    });
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      // #when
      const result = await host.poll();

      // #then — counted in its own bucket, and the logged verdict says
      // whether redelivering could change the outcome
      expect(result).toEqual(tally);
      expect(addressed).toEqual(['acme_t1']);
      expect(
        loggedEvent(logged, 'signal-provider.delivery-rejected').terminal,
      ).toBe(terminal);
    } finally {
      logged.mockRestore();
    }
  });

  it('drops a delivery bound to a row this host never authorized', async () => {
    // #given — the adapter hands back a subscription id the store never issued
    const factory = new InMemorySubscriptionStoreFactory();
    const row = await seed(factory, 'acme', 'rogue', 'acme_t1');
    const rogue: SignalProviderAdapter = {
      id: 'rogue',
      buildNotification: () => ({ source: 'rogue', kind: 'k', summary: 's' }),
      pollForDeliveries: async (): Promise<ProviderDelivery[]> => [
        {
          subscription: { ...row, id: 'never-issued' },
          notification: { source: 'rogue', kind: 'k', summary: 's' },
        },
      ],
    };
    const addressed: string[] = [];
    const host = new TestHost(state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME), {
      factory,
      topology: stubTopology(addressed),
      providers: [rogue],
    });
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      // #when
      const result = await host.poll();

      // #then — no thread was addressed, and the log says polling again could
      // only return the same adapter defect
      expect(result).toEqual({ providersPolled: 1, delivered: 0, failed: 1 });
      expect(addressed).toEqual([]);
      expect(
        loggedEvent(logged, 'signal-provider.delivery-error').terminal,
      ).toBe(true);
    } finally {
      logged.mockRestore();
    }
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

describe('SignalProviderHost and the deployment execution fence', () => {
  async function fenceAt(
    state_: ExecutionFenceState,
  ): Promise<ExecutionFenceStore> {
    const fence = new ExecutionFenceStore(
      sqliteUnitDatabase(openSqlite()) as ExecutionFenceDatabase,
    );
    await fence.seed(state_);
    return fence;
  }

  it('refuses the REQUEST poll path with 503 once locked', async () => {
    // #given
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const addressed: string[] = [];
    const host = new TestHost(state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME), {
      factory,
      topology: stubTopology(addressed),
      providers: [pollProvider('poller', 60_000)],
      executionFence: await fenceAt('migration-locked'),
    });

    // #then — a poll adapter re-reports state it has not seen accepted, so a
    // refused pass costs a redelivery rather than a lost notification.
    await expect(host.poll()).rejects.toBeInstanceOf(ExecutionFencedError);
    expect(addressed).toEqual([]);

    // #and — over the fetch surface it is the taxonomy's 503.
    const response = await host.fetch(
      deploymentIdentityRequest('http://host/poll', { method: 'POST' }),
    );
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({
      reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
    });
  });

  it('swallows the refusal on the ALARM path and keeps its re-arm', async () => {
    // #given — workerd retries a thrown alarm() up to six times, which would
    // answer a deliberate operational state with a wake storm.
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const alarms: number[] = [];
    const storage = {
      setAlarm: (at: number) => {
        alarms.push(at);
      },
      deleteAlarm: () => undefined,
      getAlarm: () => null,
    };
    const addressed: string[] = [];
    const host = new TestHost(
      state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME, storage),
      {
        factory,
        topology: stubTopology(addressed),
        providers: [pollProvider('poller', 60_000)],
        executionFence: await fenceAt('migration-locked'),
      },
    );
    const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);

    // #when / #then
    try {
      await expect(host.alarm()).resolves.toBeUndefined();
    } finally {
      log.mockRestore();
    }
    expect(addressed).toEqual([]);
    // #and — the prearm stands, so the next wake polls again once reopened.
    expect(alarms).toHaveLength(1);
  });

  it('keeps polling while draining', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'poller', 'acme_t1');
    const addressed: string[] = [];
    const host = new TestHost(state(SIGNAL_PROVIDER_HOST_INSTANCE_NAME), {
      factory,
      topology: stubTopology(addressed),
      providers: [pollProvider('poller', 60_000)],
      executionFence: await fenceAt('draining'),
    });

    await expect(host.poll()).resolves.toEqual({
      providersPolled: 1,
      delivered: 1,
    });
    expect(addressed).toEqual(['acme_t1']);
  });
});
