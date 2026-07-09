// Approval API — the Phase 3 enterprise-controls surface: a CAS-backed
// approval queue (claim / decide / delegate), SLA tracking with escalation,
// role-authorized REST routing, audit emission, and the grant-minting seam
// that turns an approved record into a breakwater connector grant at
// start/resume (grants.ts). Plugs into the DO runner via
// requestContextForRun + resumeViaRuntime; persistence is D1 (or the
// in-memory store for tests/dev).

export {
  APPROVAL_ROLES,
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
  RUN_START_ROLES,
} from './contract.js';
export type {
  ApprovalActor,
  ApprovalAuditEvent,
  ApprovalAuditSink,
  ApprovalRole,
} from './contract.js';
export { D1ApprovalStore } from './d1-store.js';
export type {
  ApprovalDatabase,
  ApprovalPreparedStatement,
} from './d1-store.js';
export {
  approvalGrantProvider,
  approvedConnectorsForLeg,
  defaultResumeData,
  resumeViaRuntime,
} from './grants.js';
export { createApprovalRouter } from './router.js';
export type { ApprovalRouter, ApprovalRouterOptions } from './router.js';
export {
  ApprovalAuthzError,
  ApprovalConflictError,
  ApprovalService,
  InvalidApprovalInputError,
  UnknownApprovalError,
} from './service.js';
export type { ApprovalServiceOptions } from './service.js';
export { InMemoryApprovalStore, stepKeyOf } from './store.js';
export type { ApprovalPatch, ApprovalStore, CreateResult } from './store.js';
export {
  APPROVAL_PRIORITIES,
  APPROVAL_STATUSES,
  OPEN_STATUSES,
} from './types.js';
export type {
  ApprovalDecision,
  ApprovalListFilter,
  ApprovalMetrics,
  ApprovalPriority,
  ApprovalRecord,
  ApprovalStatus,
  CreateApprovalInput,
  DecideResult,
  ResumeOutcome,
} from './types.js';
