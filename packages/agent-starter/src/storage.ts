// SPDX-License-Identifier: Apache-2.0

import { createD1Storage } from '@proofoftech/flowsafe/do-runner';
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
