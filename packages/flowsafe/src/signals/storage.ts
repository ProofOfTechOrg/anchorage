// SPDX-License-Identifier: Apache-2.0
// Track C (M-004) — the signal storage domains, packaged for injection into
// createD1Storage. do-runner's createD1Storage cannot import these directly
// (signals/ imports do-runner, so a do-runner→signals edge would cycle), so it
// takes an opaque `domains: MastraStorageDomains` and this helper supplies it.
// A host that wants D1-durable notifications + thread-state (the built-in task
// tools, the goal scorer, the durable inbox) passes
// `createD1Storage({ binding, domains: createSignalStorageDomains(binding) })`.

import type { MastraStorageDomains } from '@mastra/core/storage';

import type { D1DatabaseBinding } from '../do-runner/index.js';
import { validateTablePrefix } from '../do-runner/table-prefix.js';
import type { SignalDatabase } from './d1-shared.js';
import { D1NotificationsStorage } from './notifications-d1.js';
import { D1ThreadStateStorage } from './thread-state-d1.js';

/**
 * Build the flowsafe-owned signal storage domains (notifications + thread-state)
 * over a D1 binding, ready to hand to `createD1Storage({ domains })`. The binding
 * is the SAME one createD1Storage builds its D1Store from — the two D1 domains
 * create tables (`mastra_notifications`, `mastra_thread_state`) the adapter does
 * not own, so they coexist on one database with no DDL-ordering conflict.
 */
export function createSignalStorageDomains(
  binding: D1DatabaseBinding,
  tablePrefix = '',
): MastraStorageDomains {
  const prefix = validateTablePrefix(tablePrefix) ?? '';
  // The structural SignalDatabase subset (prepare→bind/first/all/run) is exactly
  // what a real D1Database exposes; the cast bridges the workers-types-free seam.
  const db = binding as unknown as SignalDatabase;
  return {
    notifications: new D1NotificationsStorage(db, prefix),
    threadState: new D1ThreadStateStorage(db, prefix),
  };
}
