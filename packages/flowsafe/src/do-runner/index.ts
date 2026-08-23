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
  InitialExecutionFenceState,
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
export type { DoRefusalReason } from './do-error-response.js';
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
// The deployment execution fence (docs/do-runner-design.md): the operational
// control that stops a deployment minting work while its state is migrated,
// plus the four admission predicates the semantics matrix is written as.
export type {
  ExecutionFenceDatabase,
  ExecutionFenceReading,
  ExecutionFenceRefusal,
  ExecutionFenceState,
  ExecutionFenceStatement,
  ExecutionFenceStoreOptions,
  ExecutionFenceTransition,
} from './execution-fence.js';
export {
  admitsDrainableExecution,
  admitsExistingRun,
  admitsRunStart,
  admitsWorkAuthoring,
  assertExecutionFenceState,
  EXECUTION_FENCE_STATES,
  EXECUTION_FENCE_TABLE,
  ExecutionFencedError,
  ExecutionFenceStore,
  ExecutionFenceUnreadableError,
  executionFencedResponse,
  FenceTransitionConflictError,
  InvalidExecutionFenceRequestError,
  isExecutionFenceRefusal,
  OPEN_EXECUTION_FENCE,
} from './execution-fence.js';
export { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
export type { HubStreamEvent, PresenceMember } from './hub-do.js';
export { HUB_INSTANCE_NAME, HubDurableObject } from './hub-do.js';
export type {
  DORunnerEnv,
  ExecutionFenceWiring,
  InitOptions,
  InitResult,
  InitSource,
  StorageInitOptions,
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
  RunStateUnreadableError,
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
// Per-suspension deadlines: the reserved suspend-payload key that arms one, the
// timeout envelope a resumed step branches on, and the bounds each is validated
// against (docs/do-runner-design.md, "Per-suspension deadlines"). The stored
// record, its parser, the envelope factory it feeds, and the wake arithmetic
// stay internal — they are the run object's own Durable-Object plumbing, and a
// consumer holding them could only misread state the alarm owns.
// MAX_SUSPENSION_DEADLINES_PER_RUN is the exception among the bounds: no single
// value is validated against it, and it ships as an operational figure — the
// per-run cap a host plans and documents against, which is how the README
// quotes it — not as something to check a deadline with before arming.
export type {
  SuspensionTimeoutEnvelope,
  SuspensionTimeoutResumeData,
} from './suspension-deadline.js';
export {
  isSuspensionTimeoutResumeData,
  MAX_SUSPENSION_DEADLINE_MS,
  MAX_SUSPENSION_DEADLINES_PER_RUN,
  MIN_SUSPENSION_DEADLINE_MS,
  SUSPENSION_DEADLINE_PAYLOAD_KEY,
  SUSPENSION_DEADLINE_PRINCIPAL_ID,
  SUSPENSION_TIMEOUT_RESUME_KEY,
} from './suspension-deadline.js';
export type { ThreadScope } from './thread-do.js';
export { ThreadDurableObject, ThreadIdentityError } from './thread-do.js';
