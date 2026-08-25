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
  RUN_TTL_FLOWSAFE_PURGE_TABLES,
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
//
// The RAW constants behind it — EXECUTION_FENCE_TABLE, EXECUTION_FENCE_STATES,
// EXECUTION_FENCE_ROW_ID, EXECUTION_FENCE_DDL — are deliberately absent: they
// are the provisioning protocol's, shipped on
// `@proofoftech/flowsafe/deployment-identity-protocol` where the provisioning
// CLI and fleet-control can reach them too. Publishing them twice would let a
// consumer pin the table name from one subpath and the DDL from the other and
// never learn they had drifted.
export type {
  ExecutionFenceDatabase,
  ExecutionFenceReading,
  ExecutionFenceRefusal,
  ExecutionFenceState,
  ExecutionFenceStatement,
  ExecutionFenceStoreOptions,
  ExecutionFenceTransition,
  ExecutionFenceWiring,
  // One arm of ExecutionFenceRefusal, published because that union is: a
  // consumer that catches a fence refusal on the far side of a Durable Object
  // boundary is handed THIS shape, and a union arm it cannot name is a surface
  // it cannot write a handler's type against.
  WireExecutionFenceRefusal,
} from './execution-fence.js';
export {
  admitsDrainableExecution,
  admitsExistingRun,
  admitsRunStart,
  admitsWorkAuthoring,
  assertExecutionFenceState,
  ExecutionFencedError,
  ExecutionFenceStore,
  ExecutionFenceUnreadableError,
  executionFencedResponse,
  // The one memo every host composes its fence through — see executionFenceFor.
  executionFenceFor,
  executionFenceReadingPayload,
  FenceTransitionConflictError,
  InvalidExecutionFenceRequestError,
  isExecutionFenceRefusal,
  // OPEN_EXECUTION_FENCE is deliberately NOT exported: `readExecutionFence`
  // is the only supported way to resolve an absent fence, so no consumer can
  // hand-roll a ternary that gets the open case subtly wrong.
  // EXECUTION_FENCE_SUSPEND_KEY is deliberately NOT exported here; it is
  // published only from `./background-tasks`, whose host stamps and reads it.
  readExecutionFence,
} from './execution-fence.js';
export { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
export type { HubStreamEvent, PresenceMember } from './hub-do.js';
export { HUB_INSTANCE_NAME, HubDurableObject } from './hub-do.js';
export type {
  DORunnerEnv,
  InitOptions,
  InitResult,
  InitSource,
  StorageInitOptions,
} from './init.js';
export { init } from './init.js';
// The drain inventory: the read-only surface an operator proves a deployment
// empty with, and the table census that keeps that proof complete as new
// tables arrive.
export type {
  DeploymentInventoryOptions,
  DrainProofContract,
  FlowsafeTableEntry,
  InventoryCategory,
  InventoryCategoryClass,
  InventoryCategoryDescriptor,
  InventoryDatabase,
  InventoryEntry,
  InventoryIndex,
  InventoryPage,
  InventoryReadOptions,
  InventoryStatement,
  InventoryTableAccounting,
  UnenumerableState,
} from './inventory.js';
export {
  DeploymentInventory,
  FLOWSAFE_TABLES,
  INVENTORY_CATEGORIES,
  INVENTORY_CATEGORY_DESCRIPTORS,
  INVENTORY_DEFAULT_LIMIT,
  INVENTORY_DRAIN_PROOF,
  INVENTORY_INDEX,
  INVENTORY_MAX_LIMIT,
  INVENTORY_UNENUMERABLE,
  InvalidInventoryRequestError,
  isInventoryCategory,
} from './inventory.js';
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
// Owner-bound idempotent start (docs/do-runner-design.md): the reservation that
// makes a retried start converge onto the run it already made instead of paying
// for a second one. Its eight structured reasons are five decision refusals —
// IDEMPOTENT_START_OWNER_MISMATCH (403), IDEMPOTENT_START_TARGET_MISMATCH
// (409), IDEMPOTENT_START_PENDING (503), IDEMPOTENT_START_UNRESOLVABLE (409),
// and IDEMPOTENT_START_ALREADY_SETTLED (409) — plus
// IDEMPOTENT_START_UNSUPPORTED (503), INVALID_START_IDEMPOTENCY_REQUEST (400),
// and IDEMPOTENT_START_UNREADABLE (503).
//
// START_IDEMPOTENCY_DDL and the two index statements are exported for the drain
// inventory and for a host that owns its own migrations; the table NAME rides
// with them because — unlike the fence's — this table is created by the store
// itself, so there is only ever one definition of it to import.
export type {
  IdempotentStartDecision,
  IdempotentStartSurface,
  StartIdempotencyDatabase,
  StartIdempotencyStatement,
  StartIdempotencyStoreOptions,
  StartIdempotencyWiring,
  StartReservation,
  StartReservationOutcome,
  StartReservationOwner,
  StartReservationRefusal,
  StartReservationRequest,
  StartReservationState,
  StartTargetKind,
} from './start-idempotency.js';
export {
  beginIdempotentStart,
  IdempotentStartAlreadySettledError,
  IdempotentStartPendingError,
  IdempotentStartUnresolvableError,
  InvalidStartIdempotencyRequestError,
  isStartReservationRefusal,
  requireStartIdempotency,
  rollbackFencedStart,
  START_IDEMPOTENCY_DDL,
  START_IDEMPOTENCY_RUN_INDEX_DDL,
  START_IDEMPOTENCY_STATE_INDEX_DDL,
  START_IDEMPOTENCY_TABLE,
  START_RESERVATION_STATES,
  START_TARGET_KINDS,
  StartIdempotencyStore,
  StartIdempotencyUnsupportedError,
  StartReservationOwnerMismatchError,
  StartReservationTargetMismatchError,
  StartReservationUnreadableError,
  // The one memo every host composes its reservation store through — same
  // reasoning as executionFenceFor: two stores over two bindings are two tables
  // answering the same key.
  startIdempotencyFor,
} from './start-idempotency.js';
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
