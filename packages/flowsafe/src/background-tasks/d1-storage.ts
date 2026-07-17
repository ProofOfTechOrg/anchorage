// SPDX-License-Identifier: Apache-2.0
// Track B (M-003) background-tasks storage seam.
//
// The D1 BackgroundTasksStorage domain is deliberately NOT reimplemented here:
// @mastra/cloudflare-d1's D1Store already ships `BackgroundTasksStorageD1` — the
// full abstract domain (createTask/updateTask/getTask/listTasks/deleteTask(s)/
// getRunningCount[ByAgent]) over `mastra_background_tasks`, keyed run_id /
// completedAt / status — and returns it from `getStore('backgroundTasks')`, the
// exact store `BackgroundTaskManager` reads through its async `getStorage()`.
// Reimplementing it would be a second custom state store for a table Mastra's
// own adapter owns — the project's "What NOT to build: Custom state store —
// Mastra has 13+ storage adapters". So this module supplies the flowsafe-side
// integration the milestone actually needs: the async accessor onto that
// domain, plus (re-exported from do-runner, where the schema guard couples the
// table to its purge) the tenant-range offboarding coverage and the
// `completedAt` TTL cleanup — registered against `mastra_background_tasks` in
// the same change (DL-003).

import type { Mastra } from '@mastra/core/mastra';
import type { BackgroundTasksStorage } from '@mastra/core/storage';

export type {
  PurgeExpiredBackgroundTasksOptions,
  PurgeExpiredBackgroundTasksResult,
} from '../do-runner/index.js';
export {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  purgeExpiredBackgroundTasks,
} from '../do-runner/index.js';

/**
 * The D1 background-tasks storage domain the manager persists to, reached
 * through the manager's OWN access path — `mastra.getStorage()` then
 * `getStore('backgroundTasks')`, BOTH async, so this awaits (the CI-M-003-001
 * "getStorage() is async — await it" note). Fail-closed with a clear message
 * when a host wired a Mastra with no storage, or a storage adapter that does not
 * implement the `backgroundTasks` domain, rather than surfacing core's terser
 * "Background tasks storage is not available" only once the first task
 * dispatches.
 */
export async function backgroundTasksStore(
  mastra: Mastra,
): Promise<BackgroundTasksStorage> {
  const storage = mastra.getStorage();
  if (!storage) {
    throw new Error(
      'background-tasks: the hosting Mastra has no storage configured — background tasks need a D1 (or equivalent) composite store',
    );
  }
  const store = await storage.getStore('backgroundTasks');
  if (!store) {
    throw new Error(
      "background-tasks: the storage adapter does not implement the 'backgroundTasks' domain (use @mastra/cloudflare-d1, which ships BackgroundTasksStorageD1)",
    );
  }
  return store;
}
