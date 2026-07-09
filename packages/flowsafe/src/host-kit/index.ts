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
  requestedConnectors,
  type ResumeRunFn,
  resumeRunWithRequeue,
} from './approval-bridge.js';
export { bearerActorAuthenticator, parseActorTokens } from './bearer-auth.js';
export { doSummary, type DoResponseLike } from './do-response.js';
export { assertWorkflowsRegistered } from './registration.js';
export { RunRouteError } from './run-route-error.js';
export { createRunRouter } from './run-router.js';
export type { RunRouter, RunRouterOptions } from './run-router.js';
export {
  subdomainTenantOf,
  withSubdomainCrossCheck,
} from './subdomain-check.js';
export type { SubdomainCrossCheckOptions } from './subdomain-check.js';
export {
  provisionTenant,
  RESERVED_TENANT_SLUGS,
  TenantCollisionError,
} from './tenant-registry.js';
export type {
  ProvisionTenantOptions,
  TenantRegistryDatabase,
  TenantRegistryStatement,
} from './tenant-registry.js';
export {
  base64UrlEncode,
  hmacSign,
  hmacVerifier,
  mintHmacToken,
  staticTokenVerifier,
  toApprovalActor,
} from './verifier.js';
export type {
  HmacVerifierOptions,
  MintHmacTokenOptions,
  TokenVerifier,
} from './verifier.js';
export type { WorkflowMeta } from './workflow-meta.js';
