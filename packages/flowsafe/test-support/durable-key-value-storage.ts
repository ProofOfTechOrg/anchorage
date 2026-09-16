// SPDX-License-Identifier: Apache-2.0
// The in-memory DurableKeyValueStorage every Durable Object suite drives.
//
// Durable Object storage serializes on write and hands back a fresh object on
// read, so this fixture clones in both directions. One that stored and returned
// live references passes while the code under test mutates a value it has
// already written, and workerd carries no such mutation to the next read.
//
// `events` records each call in order and `alarms` each armed time, so the
// order of the run-owner journal protocol — arm the wake, then write the
// journal — is assertable without a second stub. A caller supplies its own
// `events` array to interleave its own markers with the storage calls in a
// single ordering.

import type { DurableObjectState } from '@cloudflare/workers-types';

import type { DurableKeyValueStorage } from '../src/do-runner/cf-types.js';

export interface DurableKeyValueStorageFixture {
  /** The storage a Durable Object host reads, writes and arms alarms through. */
  storage: DurableKeyValueStorage;
  /** A DurableObjectState carrying that storage and no `id`. */
  state: DurableObjectState;
  /** The stored clones, for seeding a record or reading one back. */
  values: Map<string, unknown>;
  /** Each armed alarm time, in call order; `deleteAlarm` leaves them recorded. */
  alarms: number[];
  /** `get:<key>`, `put:<key>`, `delete:<key>`, `setAlarm`, `deleteAlarm`. */
  events: string[];
}

export function durableKeyValueStorageFixture(
  events: string[] = [],
): DurableKeyValueStorageFixture {
  const values = new Map<string, unknown>();
  const alarms: number[] = [];
  const storage: DurableKeyValueStorage = {
    async get<T>(key: string): Promise<T | undefined> {
      events.push(`get:${key}`);
      return structuredClone(values.get(key)) as T | undefined;
    },
    async put<T>(key: string, value: T): Promise<void> {
      events.push(`put:${key}`);
      values.set(key, structuredClone(value));
    },
    async delete(key: string): Promise<boolean> {
      events.push(`delete:${key}`);
      return values.delete(key);
    },
    async setAlarm(scheduledTime: number | Date): Promise<void> {
      events.push('setAlarm');
      alarms.push(
        scheduledTime instanceof Date ? scheduledTime.getTime() : scheduledTime,
      );
    },
    async deleteAlarm(): Promise<void> {
      events.push('deleteAlarm');
    },
  };
  return {
    storage,
    state: { storage } as unknown as DurableObjectState,
    values,
    alarms,
    events,
  };
}
