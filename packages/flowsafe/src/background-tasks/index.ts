// SPDX-License-Identifier: Apache-2.0
// Track B (M-003) — background tasks. Subpath-only (`@proofoftech/flowsafe/
// background-tasks`), like agent-runner: host-side wiring a consumer opts into,
// not part of the root barrel. The tenant-range purge coverage + the completedAt
// TTL cleanup live in do-runner (coupled there to the schema guard) and are
// re-exported here for a single import surface.

export type {
  PurgeExpiredBackgroundTasksOptions,
  PurgeExpiredBackgroundTasksResult,
} from './d1-storage.js';
export {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  backgroundTasksStore,
  purgeExpiredBackgroundTasks,
} from './d1-storage.js';
export type { BackgroundTaskHostOptions } from './host.js';
export { BackgroundTaskHost } from './host.js';
export type {
  BackgroundTaskRouter,
  BackgroundTaskRoutesOptions,
} from './routes.js';
export { createBackgroundTaskRoutes } from './routes.js';
