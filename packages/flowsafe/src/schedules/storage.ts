// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — the schedules storage domain packaged for injection into
// createD1Storage. do-runner's createD1Storage cannot import this directly
// (schedules/ imports do-runner, so a do-runner->schedules edge would cycle), so
// it takes an opaque `domains: MastraStorageDomains` and this helper supplies it
// — the SAME pattern Track C's createSignalStorageDomains uses.
//
// A host that wants D1-durable schedules composes them alongside the signal
// domains, e.g.:
//   createD1Storage({
//     binding,
//     domains: {
//       ...createSignalStorageDomains(binding),
//       ...createScheduleStorageDomains(binding),
//     },
//   })
// The 'schedules' domain key is what core's `mastra.schedules` resolves through
// getStore('schedules') — and what the tick and router build their store from.

import type { MastraStorageDomains } from '@mastra/core/storage';

import type { D1DatabaseBinding } from '../do-runner/index.js';
import { D1SchedulesStorage, type ScheduleDatabase } from './schedules-d1.js';

/**
 * Build the flowsafe-owned schedules storage domain over a D1 binding, ready to
 * hand to `createD1Storage({ domains })`. The binding is the SAME one
 * createD1Storage builds its D1Store from — the domain creates two tables
 * (`mastra_schedules`, `mastra_schedule_triggers`) the adapter does not own, so
 * they coexist on one database with no DDL-ordering conflict.
 */
export function createScheduleStorageDomains(
  binding: D1DatabaseBinding,
  tablePrefix = '',
): MastraStorageDomains {
  // The structural ScheduleDatabase subset (prepare->bind/first/all/run) is
  // exactly what a real D1Database exposes; the cast bridges the
  // workers-types-free seam (same convention as createSignalStorageDomains).
  const db = binding as unknown as ScheduleDatabase;
  return { schedules: new D1SchedulesStorage(db, tablePrefix) };
}
