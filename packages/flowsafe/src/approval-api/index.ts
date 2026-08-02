// SPDX-License-Identifier: Apache-2.0
// Approval API — the Phase 3 enterprise-controls surface: a CAS-backed
// approval queue (claim / decide / delegate), SLA tracking with escalation,
// role-authorized REST routing, audit emission, and the grant-minting seam
// that turns an approved record into a breakwater connector grant at
// start/resume (grants.ts). Plugs into the DO runner via
// requestContextForRun + resumeViaRuntime; persistence is D1 (or the
// in-memory store for tests/dev).

export type {
  ApprovalActor,
  ApprovalAuditEvent,
  ApprovalAuditSink,
  ApprovalNotificationEvent,
  ApprovalNotificationSink,
  ApprovalRole,
  ApprovalStreamEvent,
  ApprovalStreamSink,
} from './contract.js';
export {
  APPROVAL_ROLES,
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_CONNECTOR_EXECUTION_KEY,
  BREAKWATER_CONNECTOR_GRANTS_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
  DECIDER_ROLES,
  RUN_START_ROLES,
} from './contract.js';
// D1ApprovalStore itself is deliberately NOT exported: hosts obtain bound
// stores through D1ApprovalStoreFactory.forTenant() (INV-2) — a directly
// constructable store is a bindable-to-nothing footgun.
export type {
  ApprovalDatabase,
  ApprovalPreparedStatement,
} from './d1-store.js';
export {
  approvalGrantProvider,
  approvalGrantProviderFromFactory,
  connectorGrantsForLeg,
  defaultResumeData,
  resumeViaRuntime,
} from './grants.js';
export type {
  AutomatedExecutionPrincipal,
  AutomatedPrincipalKind,
  ExecutionPrincipal,
  ExecutionPrincipalKind,
  TrustedAutomationPrincipal,
} from './principal.js';
export {
  AUTOMATED_PRINCIPAL_KINDS,
  AUTOMATED_PROJECTED_ROLE,
  assertExecutionPrincipal,
  breakwaterActorFor,
  decodeExecutionPrincipal,
  EXECUTION_PRINCIPAL_KINDS,
  encodeExecutionPrincipal,
  humanPrincipal,
  isExecutionPrincipal,
  principalActor,
  principalAuditFields,
  samePrincipal,
  // TRUSTED_AUTOMATION is deliberately NOT re-exported: `trustAutomationPrincipal`
  // is the sanctioned constructor and covers every legitimate case, so naming the
  // raw symbol here would only widen the public surface with plumbing. This is
  // API hygiene, not a capability boundary — the brand stays recoverable by
  // reflection from any vouched principal, and the threat model says so.
  trustAutomationPrincipal,
} from './principal.js';
export type { PurgeExpiredApprovalsOptions } from './retention.js';
export { purgeExpiredApprovals } from './retention.js';
export type { ApprovalRouter, ApprovalRouterOptions } from './router.js';
export {
  CLIENT_CREATE_FIELDS,
  createApprovalRouter,
  TCB_ONLY_CREATE_FIELDS,
} from './router.js';
export type {
  ApprovalServiceOptions,
  SelfDecisionPolicy,
  SweepSLAOptions,
} from './service.js';
export {
  ApprovalAuthzError,
  ApprovalConflictError,
  ApprovalService,
  InvalidApprovalInputError,
  selfDecisionExempts,
  sweepSLA,
  UnknownApprovalError,
} from './service.js';
export type { ApprovalPatch, ApprovalStore, CreateResult } from './store.js';
export { InMemoryApprovalStore, stepKeyOf } from './store.js';
export type {
  SystemApprovalStore,
  TenantBoundApprovalStore,
} from './tenant-brand.js';
export { TENANT_BOUND } from './tenant-brand.js';
export type {
  CreateTenantResolverOptions,
  TenantContext,
  TenantResolver,
} from './tenant-context.js';
export {
  createTenantResolver,
  TenantResolutionError,
} from './tenant-context.js';
export type { ApprovalStoreFactory } from './tenant-store.js';
export {
  D1ApprovalStoreFactory,
  InMemoryApprovalStoreFactory,
} from './tenant-store.js';
export type {
  ApprovalCursor,
  ApprovalDecision,
  ApprovalGrantScope,
  ApprovalListFilter,
  ApprovalListOrder,
  ApprovalMetrics,
  ApprovalPriority,
  ApprovalRecord,
  ApprovalResumeTarget,
  ApprovalStatus,
  BatchDecideItem,
  BatchDecideResult,
  ConnectorApprovalGrant,
  ConnectorApprovalGrantBase,
  ConnectorApprovalSuspension,
  CreateApprovalInput,
  DecideResult,
  ResumeOutcome,
} from './types.js';
export {
  APPROVAL_LIST_ORDERS,
  APPROVAL_PRIORITIES,
  APPROVAL_STATUSES,
  approvalCursor,
  approvalListOrder,
  byReviewerOrder,
  MAX_APPROVAL_BATCH_DECIDE,
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
  parseApprovalCursor,
  TERMINAL_APPROVAL_STATUSES,
} from './types.js';
