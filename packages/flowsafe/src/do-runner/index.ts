// SPDX-License-Identifier: Apache-2.0
// Cloudflare Durable Objects workflow runner — the Cloudflare-native
// counterpart of @mastra/inngest / @mastra/temporal. init() returns DO-bound
// createWorkflow/createStep (import-swap); DurableObjectRunner hosts
// execution; D1 holds run snapshots so suspend/resume survives restarts.

export type {
  D1DatabaseBinding,
  DurableKeyValueStorage,
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
  PurgeExpiredScheduleTriggersOptions,
  PurgeExpiredThreadStateOptions,
  PurgeExpiredThreadsOptions,
  PurgeExpiredThreadsResult,
  RunArtifactPurger,
  RunDeadlineCandidate,
  RunDeadlineCursor,
  SnapshotDatabase,
  SnapshotStatement,
  SweepExpiredRunDeadlinesOptions,
} from './d1-storage.js';
export {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  createD1Storage,
  d1Changes,
  NOTIFICATION_TTL_PURGE_TABLES,
  purgeExpiredBackgroundTasks,
  purgeExpiredNotifications,
  purgeExpiredScheduleTriggers,
  purgeExpiredThreadState,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
  RUN_TTL_PURGE_TABLES,
  SCHEDULE_TRIGGER_TTL_PURGE_TABLES,
  sweepExpiredRunDeadlines,
  THREAD_STATE_TTL_PURGE_TABLES,
  THREAD_TTL_PURGE_TABLES,
} from './d1-storage.js';
// Deployment identity (docs/security-threat-model.md, "The provisioning
// boundary"): the env-tag + D1-sentinel guard every entry surface asserts.
export type {
  DeploymentIdentityDatabase,
  DeploymentIdentityEnv,
  DeploymentIdentityStatement,
} from './deployment-identity.js';
export {
  assertDeploymentIdentity,
  assertDeploymentIdentitySecret,
  DEPLOYMENT_IDENTITY_HEADER,
  DEPLOYMENT_TAG_PATTERN,
  DeploymentIdentityError,
  deploymentIdentityHeaders,
  ensureDeploymentIdentity,
  ensureDeploymentIdentityBindings,
  readDeploymentIdentity,
  seedDeploymentIdentity,
  stampDeploymentIdentityRequest,
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from './deployment-identity.js';
// The DO error taxonomy and its extension point: a host DO's own route states a
// status by extending DoStatusError (see do-error-response.ts).
export { DoStatusError, doErrorResponse } from './do-error-response.js';
export {
  type DurableObjectRunLifecycleHooks,
  DurableObjectRunner,
  type DurableObjectRunOwner,
  type DurableObjectRunOwnershipStore,
} from './durable-object.js';
export {
  assertNoReservedExecutionContext,
  findReservedExecutionContextKey,
  isReservedExecutionContextKey,
  RESERVED_EXECUTION_CONTEXT_KEYS,
  ReservedExecutionContextError,
  stripReservedExecutionContext,
} from './execution-context.js';
export { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
export type { HubStreamEvent, PresenceMember } from './hub-do.js';
export { HUB_INSTANCE_NAME, HubDurableObject } from './hub-do.js';
export type {
  DORunnerEnv,
  InitOptions,
  InitResult,
  InitSource,
} from './init.js';
export { init } from './init.js';
// Agent-memory id chokepoint: mint server-owned thread ids and validate trusted
// host business keys used as resource ids (clients never supply either).
export { mintThreadId, resourceIdFromKey } from './memory-id.js';
export { isPathSafeId, PATH_SAFE_ID_PATTERN } from './path-safe-id.js';
export type { HostPubSub } from './pubsub.js';
export { createHostPubSub } from './pubsub.js';
export type {
  RunEconomicOperation,
  RunLifecyclePrincipal,
  RunScheduleDispatch,
  RunTerminalErrorEnvelope,
  RunTerminalStatus,
} from './run-lifecycle.js';
export type {
  RequestContextProvider,
  ResumeRunOptions,
  RunLeg,
  RunLifecycleBlockedReason,
  RunLifecycleCas,
  RunLifecycleTransitionResult,
  RunnerRuntimeOptions,
  RunStatus,
  RunSummary,
  StartRunOptions,
} from './runtime.js';
export {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunLifecycleBlockedError,
  RunNotSuspendedError,
  RunnerRuntime,
  RunTerminalConflictError,
  UnknownRunError,
  UnknownWorkflowError,
} from './runtime.js';
export type {
  ResolvedScheduleStart,
  ScheduleSourceAgentTarget,
  ScheduleSourceOwner,
  ScheduleSourceOwnershipStore,
  ScheduleSourceStore,
  ScheduleSourceTarget,
  ScheduleSourceWorkflowTarget,
  ScheduleStartTarget,
} from './schedule-source.js';
export { resolveScheduleStartOwner } from './schedule-source.js';
export type { ThreadScope } from './thread-do.js';
export { ThreadDurableObject, ThreadIdentityError } from './thread-do.js';
