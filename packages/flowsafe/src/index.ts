// @proofoftech/flowsafe — Approval UX + Cloudflare-native durable execution
//
// flowsafe plugs into Mastra's suspend/resume workflow events and adds:
// 1. Approval API — queue, claim, decide, delegate, SLA tracking, escalation,
//    and store-derived breakwater grant minting
// 2. Approval dashboard — minimal React UI for the approval queue
//    (subpath export '@proofoftech/flowsafe/approval-ui' only — a standalone
//    app over the REST API)
// 3. DO runner — init()-based import-swap for Mastra on Cloudflare Durable Objects
// 4. Audit export — Queues producer sink + batch consumer shipping audit
//    events to a SIEM over HTTP
// 5. Artifacts — R2-backed workflow artifact storage keyed by run identity
// 6. Host kit — the shared run routes, bearer auth seam, and suspension→approval
//    bridge every host mounts (subpath export '@proofoftech/flowsafe/host-kit'
//    only — it is host glue, not part of the library's core surface)
//
// Export rule: this root barrel mirrors the approval-api, do-runner,
// artifacts, and audit-export subpath barrels COMPLETELY — anything those
// subpaths export is also importable from the package root. approval-ui and
// host-kit are subpath-only by design (React peer / host glue).

export type {
  ApprovalActor,
  ApprovalAuditEvent,
  ApprovalAuditSink,
  ApprovalCursor,
  ApprovalDatabase,
  ApprovalDecision,
  ApprovalListFilter,
  ApprovalListOrder,
  ApprovalMetrics,
  ApprovalPatch,
  ApprovalPreparedStatement,
  ApprovalPriority,
  ApprovalRecord,
  ApprovalRole,
  ApprovalRouter,
  ApprovalRouterOptions,
  ApprovalServiceOptions,
  ApprovalStatus,
  ApprovalStore,
  ApprovalStoreFactory,
  CreateApprovalInput,
  CreateResult,
  CreateTenantResolverOptions,
  DecideResult,
  PurgeExpiredApprovalsOptions,
  ResumeOutcome,
  SweepSLAOptions,
  SystemApprovalStore,
  TenantBoundApprovalStore,
  TenantContext,
  TenantResolver,
} from './approval-api/index.js';
export {
  APPROVAL_LIST_ORDERS,
  APPROVAL_PRIORITIES,
  APPROVAL_ROLES,
  APPROVAL_STATUSES,
  ApprovalAuthzError,
  ApprovalConflictError,
  ApprovalService,
  approvalCursor,
  approvalGrantProvider,
  approvalGrantProviderFromFactory,
  approvalListOrder,
  approvedConnectorsForLeg,
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
  byReviewerOrder,
  CLIENT_CREATE_FIELDS,
  createApprovalRouter,
  createTenantResolver,
  D1ApprovalStoreFactory,
  defaultResumeData,
  InMemoryApprovalStore,
  InMemoryApprovalStoreFactory,
  InvalidApprovalInputError,
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
  parseApprovalCursor,
  purgeExpiredApprovals,
  RUN_START_ROLES,
  resumeViaRuntime,
  stepKeyOf,
  sweepSLA,
  TCB_ONLY_CREATE_FIELDS,
  TENANT_BOUND,
  TERMINAL_APPROVAL_STATUSES,
  TenantResolutionError,
  UnknownApprovalError,
} from './approval-api/index.js';
export type {
  ArtifactBody,
  ArtifactBucket,
  ArtifactBucketListResult,
  ArtifactBucketObject,
  ArtifactBucketObjectBody,
  ArtifactContent,
  ArtifactRecord,
  ArtifactRef,
  ListArtifactsScope,
  PutArtifactOptions,
  R2ArtifactStoreOptions,
} from './artifacts/index.js';
export {
  InMemoryArtifactBucket,
  InvalidArtifactRefError,
  R2ArtifactStore,
} from './artifacts/index.js';
export type {
  AuditExportFetch,
  AuditExportOptions,
  AuditMessageBatch,
  AuditQueue,
  AuditQueueMessage,
} from './audit-export/index.js';
export {
  createAuditQueueConsumer,
  createAuditQueueHandler,
  queueAuditSink,
} from './audit-export/index.js';
export type {
  D1DatabaseBinding,
  D1StorageOptions,
  DORunnerEnv,
  DurableObjectRunnerState,
  InitOptions,
  InitResult,
  InitSource,
  PurgeExpiredRunsOptions,
  PurgeTenantOptions,
  PurgeTenantResult,
  RequestContextProvider,
  ResumeLedger,
  ResumeLedgerStorage,
  ResumeRunOptions,
  RunLeg,
  RunnerRuntimeOptions,
  RunSummary,
  SnapshotDatabase,
  SnapshotStatement,
  StartRunOptions,
  TenantArtifactPurger,
} from './do-runner/index.js';
export {
  createD1Storage,
  DurableObjectRunner,
  DurableStorageResumeLedger,
  d1Changes,
  ensureSnapshotRunIdIndex,
  InMemoryResumeLedger,
  InvalidRunRequestError,
  init,
  PATH_SAFE_ID_PATTERN,
  purgeExpiredWorkflowRuns,
  purgeTenant,
  RunAlreadyExistsError,
  RunNotSuspendedError,
  RunnerRuntime,
  TENANT_ID_PATTERN,
  tenantOfRunId,
  UnknownRunError,
  UnknownWorkflowError,
} from './do-runner/index.js';
