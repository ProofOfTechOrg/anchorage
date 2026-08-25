// SPDX-License-Identifier: Apache-2.0
// Background tasks. Subpath-only (`@proofoftech/flowsafe/background-tasks`),
// like agent-runner: host-side wiring a consumer opts into, not part of the
// root barrel. The completedAt TTL cleanup lives in do-runner, coupled there to
// the schema guard, and is re-exported here for a single import surface.

export type {
  CreateBackgroundTaskD1DomainsOptions,
  PurgeExpiredBackgroundTasksOptions,
  PurgeExpiredBackgroundTasksResult,
} from './d1-storage.js';
export {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  backgroundTasksStore,
  createBackgroundTaskD1Domains,
  DURABLE_OBJECT_BACKGROUND_TASKS_D1,
  DurableObjectBackgroundTasksStorageD1,
  DurableObjectWorkflowsStorageD1,
  purgeExpiredBackgroundTasks,
  SERIALIZED_WORKFLOWS_D1,
} from './d1-storage.js';
export type { BackgroundTaskHostOptions } from './host.js';
// EXECUTION_FENCE_SUSPEND_KEY is the marker a host reads to tell a
// fence-parked row from a tool-suspended one — the one thing about the
// backstop's parking a consumer legitimately needs. The resume budgets and
// scan bounds beside it stay package-internal (they are tuning, not contract),
// and `backgroundTaskManagerForTests` stays internal for a stronger reason: it
// hands out the ungated manager.
export { BackgroundTaskHost, EXECUTION_FENCE_SUSPEND_KEY } from './host.js';
export type {
  BackgroundTaskReads,
  BackgroundTaskRouter,
  BackgroundTaskRoutesOptions,
} from './routes.js';
export { createBackgroundTaskRoutes } from './routes.js';
