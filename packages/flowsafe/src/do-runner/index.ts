// SPDX-License-Identifier: Apache-2.0
// Cloudflare Durable Objects workflow runner — the Cloudflare-native
// counterpart of @mastra/inngest / @mastra/temporal. init() returns DO-bound
// createWorkflow/createStep (import-swap); DurableObjectRunner hosts
// execution; D1 holds run snapshots so suspend/resume survives restarts.

export type {
  D1DatabaseBinding,
  DurableObjectRunnerState,
  HubDurableObjectState,
  WebSocketLike,
} from './cf-types.js';
export type {
  D1StorageOptions,
  PurgeExpiredBackgroundTasksOptions,
  PurgeExpiredBackgroundTasksResult,
  PurgeExpiredNotificationsOptions,
  PurgeExpiredRunsOptions,
  PurgeExpiredThreadStateOptions,
  PurgeExpiredThreadsOptions,
  PurgeExpiredThreadsResult,
  PurgeTenantOptions,
  PurgeTenantResult,
  SnapshotDatabase,
  SnapshotStatement,
  TenantArtifactPurger,
  TenantRangePurgeCounter,
  TenantRangePurgeTable,
} from './d1-storage.js';
export {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  createD1Storage,
  d1Changes,
  ensureSnapshotRunIdIndex,
  NOTIFICATION_TTL_PURGE_TABLES,
  purgeExpiredBackgroundTasks,
  purgeExpiredNotifications,
  purgeExpiredThreadState,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
  purgeTenant,
  RUN_TTL_PURGE_TABLES,
  TENANT_RANGE_PURGE_TABLES,
  THREAD_STATE_TTL_PURGE_TABLES,
  THREAD_TTL_PURGE_TABLES,
} from './d1-storage.js';
// The DO error taxonomy and its extension point: a host DO's own route states a
// status by extending DoStatusError (see do-error-response.ts).
export { DoStatusError, doErrorResponse } from './do-error-response.js';
export { DurableObjectRunner } from './durable-object.js';
export type { HubStreamEvent, PresenceMember } from './hub-do.js';
export { HubDurableObject } from './hub-do.js';
export type {
  DORunnerEnv,
  InitOptions,
  InitResult,
  InitSource,
} from './init.js';
export { init } from './init.js';
// Agent-memory tenancy chokepoint (docs/agent-memory-tenancy.md): the only
// constructors/decoders for salted threadId/resourceId values.
export {
  mintResourceId,
  mintThreadId,
  tenantOfMemoryId,
  tenantOwnsMemoryId,
} from './memory-id.js';
export {
  PATH_SAFE_ID_PATTERN,
  TENANT_ID_PATTERN,
  tenantOfRunId,
  tenantOwnsSaltedId,
} from './path-safe-id.js';
export type { HostPubSub } from './pubsub.js';
export { createHostPubSub } from './pubsub.js';
export type { ResumeLedger, ResumeLedgerStorage } from './resume-ledger.js';
export {
  DurableStorageResumeLedger,
  InMemoryResumeLedger,
} from './resume-ledger.js';
export type {
  RequestContextProvider,
  ResumeRunOptions,
  RunLeg,
  RunnerRuntimeOptions,
  RunSummary,
  StartRunOptions,
} from './runtime.js';
export {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunNotSuspendedError,
  RunnerRuntime,
  UnknownRunError,
  UnknownWorkflowError,
} from './runtime.js';
export type { ThreadScope } from './thread-do.js';
export { ThreadDurableObject, ThreadIdentityError } from './thread-do.js';
export { THREAD_TENANT_HEADER } from './thread-header.js';
