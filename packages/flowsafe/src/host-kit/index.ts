// SPDX-License-Identifier: Apache-2.0
// host-kit — workflow-agnostic host glue shared by the showcase Worker, the
// reference deploy template, and the dev backend. It is intentionally NOT a
// generic host: the two resume topologies (DO-stub fetch vs in-process) stay in
// their hosts, injected as thunks. What lives here is everything a host must
// not re-derive — the auth seam, the run routes and their RBAC gate order, and
// the security-critical approval bridge (the (suspendedAt, resumeCount) capture
// and the separation-of-duties re-queue).
//
// This barrel is deliberately breakwater-free, so `@proofoftech/flowsafe/host-kit`
// typechecks for a consumer that mounts routes without authoring workflow
// modules. The module-authoring contract (WorkflowModule, WorkflowModuleContext)
// carries a breakwater AuditLogger, so it ships under its own subpath —
// `@proofoftech/flowsafe/host-kit/module` — which only authors (who already
// depend on breakwater for their connectors) need to reach for. See module.ts.

export {
  queueApprovalForSuspension,
  type ResumeRunFn,
  reconcileApprovalsForSummary,
  reconcileApprovalsOnStatus,
  resumeRunWithRequeue,
} from './approval-bridge.js';
export { bearerActorAuthenticator, parseActorTokens } from './bearer-auth.js';
export { type DoResponseLike, doSummary } from './do-response.js';
export {
  createDoRunTopology,
  type DoRunTopology,
  type RunnerNamespaceLike,
  type RunnerStubLike,
} from './do-run-topology.js';
export { boolVar, type NumberVarOptions, numberVar } from './env-vars.js';
export type {
  AgentRouter,
  BackgroundTasksCleanupConfig,
  FlowsafeWorker,
  FlowsafeWorkerConfig,
  FlowsafeWorkerContext,
  FlowsafeWorkerEnv,
} from './flowsafe-worker.js';
export { createFlowsafeWorker } from './flowsafe-worker.js';
// hostAuditSink stays module-internal for the same reason as
// requestedConnectors below: it is the primitive beneath
// buildHostApprovalService / runSlaSweepMaintenance, and no host consumes it
// directly.
export {
  type ApprovalRetentionPurgeOptions,
  approvalStoreFactoryFor,
  buildHostApprovalService,
  type HostApprovalServiceOptions,
  maintenancePrincipal,
  reconcileApprovalsOnStatusDetached,
  runApprovalRetentionPurge,
  runSlaSweepMaintenance,
  type SlaSweepMaintenanceOptions,
} from './host-approval-service.js';
export {
  createHubTopology,
  type HubNamespaceLike,
  type HubStubLike,
  type HubTopology,
} from './hub-topology.js';
// The agent-memory host boundary every memory-touching route calls
// (docs/agent-memory-tenancy.md item 5).
export {
  assertNoClientMemoryIds,
  requireOwnedMemoryId,
  TCB_ONLY_MEMORY_FIELDS,
  type TcbOnlyMemoryField,
} from './memory-boundary.js';
export type { WorkflowIdSource } from './registration.js';
export { assertWorkflowsRegistered } from './registration.js';
export { RunRouteError } from './run-route-error.js';
export type { RunRouter, RunRouterOptions } from './run-router.js';
export { createRunRouter } from './run-router.js';
export type { StreamRouter, StreamRouterOptions } from './stream-router.js';
export { createStreamRouter } from './stream-router.js';
export type {
  MintStreamTicketOptions,
  StreamChannel,
  StreamTicketClaims,
  VerifyStreamTicketOptions,
} from './stream-ticket.js';
export { mintStreamTicket, verifyStreamTicket } from './stream-ticket.js';
export type { SubdomainCrossCheckOptions } from './subdomain-check.js';
// requestedConnectors and subdomainTenantOf stay module-internal on purpose:
// they are the primitives beneath queueApprovalForSuspension /
// withSubdomainCrossCheck, and no host consumes them directly.
export { withSubdomainCrossCheck } from './subdomain-check.js';
export type {
  ProvisionTenantOptions,
  TenantRegistryDatabase,
  TenantRegistryStatement,
} from './tenant-registry.js';
export {
  provisionTenant,
  RESERVED_FOR_ALLOCATION,
  RESERVED_TENANT_IDS,
  TenantCollisionError,
} from './tenant-registry.js';
// The sanctioned way to reach a thread DO: it MINTS the tenant header
// ThreadDurableObject verifies, so a route never forwards a client's own
// (see thread-topology.ts — mint and verify ship together).
export {
  createThreadTopology,
  type ThreadNamespaceLike,
  type ThreadRequestInit,
  type ThreadStubLike,
  type ThreadTopology,
} from './thread-topology.js';
export type {
  HmacVerifierOptions,
  MintHmacTokenOptions,
  TokenVerifier,
} from './verifier.js';
export {
  base64UrlEncode,
  hmacSign,
  hmacVerifier,
  mintHmacToken,
  staticTokenVerifier,
  toApprovalActor,
} from './verifier.js';
export type { WorkflowMeta } from './workflow-meta.js';
