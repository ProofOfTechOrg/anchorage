// SPDX-License-Identifier: Apache-2.0

import {
  createD1Storage,
  ExecutionFenceStore,
} from '@proofoftech/flowsafe/do-runner';
import {
  createScheduleStorageDomains,
  D1SchedulesStorage,
  type ScheduleDatabase,
} from '@proofoftech/flowsafe/schedules';
import {
  D1SubscriptionStoreFactory,
  type SubscriptionStoreFactory,
} from '@proofoftech/flowsafe/signal-providers';
import {
  createSignalStorageDomains,
  D1NotificationsStorage,
  D1ThreadStateStorage,
  type SignalDatabase,
} from '@proofoftech/flowsafe/signals';

const subscriptionFactories = new WeakMap<
  Env['DB'],
  D1SubscriptionStoreFactory
>();
const notificationStores = new WeakMap<Env['DB'], D1NotificationsStorage>();
const threadStateStores = new WeakMap<Env['DB'], D1ThreadStateStorage>();
const scheduleStores = new WeakMap<Env['DB'], D1SchedulesStorage>();
const executionFences = new WeakMap<Env['DB'], ExecutionFenceStore>();

function signalDatabase(db: Env['DB']): SignalDatabase {
  return db as unknown as SignalDatabase;
}

function scheduleDatabase(db: Env['DB']): ScheduleDatabase {
  return db as unknown as ScheduleDatabase;
}

export function createComposedStorage(db: Env['DB']) {
  return createD1Storage({
    binding: db,
    domains: {
      ...createSignalStorageDomains(db),
      ...createScheduleStorageDomains(db),
    },
  });
}

export function subscriptionStoreFactory(
  db: Env['DB'],
): SubscriptionStoreFactory {
  let factory = subscriptionFactories.get(db);
  if (!factory) {
    factory = new D1SubscriptionStoreFactory(signalDatabase(db));
    subscriptionFactories.set(db, factory);
  }
  return factory;
}

export function notificationsStore(db: Env['DB']): D1NotificationsStorage {
  let store = notificationStores.get(db);
  if (!store) {
    store = new D1NotificationsStorage(signalDatabase(db));
    notificationStores.set(db, store);
  }
  return store;
}

export function threadStateStore(db: Env['DB']): D1ThreadStateStorage {
  let store = threadStateStores.get(db);
  if (!store) {
    store = new D1ThreadStateStorage(signalDatabase(db));
    threadStateStores.set(db, store);
  }
  return store;
}

export function schedulesStore(db: Env['DB']): D1SchedulesStorage {
  let store = scheduleStores.get(db);
  if (!store) {
    store = new D1SchedulesStorage(scheduleDatabase(db));
    scheduleStores.set(db, store);
  }
  return store;
}

/**
 * The deployment execution fence, one store per D1 binding.
 *
 * Every fenced surface in this host takes its fence from HERE rather than
 * building one: the schedule tick that must not claim a due fire, the routers
 * that refuse to author new work, the background-task host, the provider
 * poller, and the approval services that decide-then-resume all have to be
 * gating the SAME database as the runtime they sit in front of. Keyed on the
 * binding, like every other store in this file, because the fence belongs to
 * the database rather than to the request that reached it.
 */
export function executionFence(db: Env['DB']): ExecutionFenceStore {
  let store = executionFences.get(db);
  if (!store) {
    store = new ExecutionFenceStore(db);
    executionFences.set(db, store);
  }
  return store;
}
