// SPDX-License-Identifier: Apache-2.0
// breakwater requestContext keys, mirrored by value.
//
// flowsafe deliberately does NOT import @proofoftech/breakwater at runtime:
// the packages compose over documented requestContext keys
// (docs/security-threat-model.md, trust boundary 6), and a
// runtime dependency would force breakwater's dist to exist before flowsafe
// typechecks (the repo gate runs typecheck before build). Literal equality
// with breakwater's exported constants is enforced by the cross-package
// contract tests in end-to-end.test.ts — drift fails the suite, not
// production.
//
// The literals live HERE — a leaf module the do-runner owns — because the
// runtime mints the workflow-scope key itself on every leg. approval-api's
// contract.ts re-exports them, keeping approval-api -> do-runner as the only
// cross-directory dependency direction.

/** requestContext key containing structured connector approval grants. */
export const BREAKWATER_CONNECTOR_GRANTS_KEY = 'breakwater.connectorGrants';

/** requestContext key containing the runtime-owned current leg identity. */
export const BREAKWATER_CONNECTOR_EXECUTION_KEY =
  'breakwater.connectorExecution';

/** requestContext key breakwater actor resolution reads ({ id, role }). */
export const BREAKWATER_ACTOR_KEY = 'breakwater.actor';

/**
 * requestContext key breakwater's connector required-permissions gate reads:
 * the executing principal's server-resolved effective permissions plus the
 * policy snapshot version ({ permissions, policyVersion }), or null when no
 * resolution exists. Trusted-runtime-only, like the grant key above — the
 * agent thread host mints it from its PrincipalPermissionResolver on every
 * leg, and a workflow host may mint it from its own RequestContextProvider.
 */
export const BREAKWATER_PRINCIPAL_PERMISSIONS_KEY =
  'breakwater.principalPermissions';

/**
 * requestContext key breakwater's crossWorkflowIsolation evaluator reads:
 * the calling workflow's scope (its workflowId). RunnerRuntime mints it on
 * every start/resume leg — trusted-runtime-only, like the grant key above.
 */
export const BREAKWATER_WORKFLOW_SCOPE_KEY = 'breakwater.workflowScope';

/**
 * requestContext key breakwater's tenantIsolation evaluator and connector
 * key-scoping read: the caller's OPAQUE isolation scope. RunnerRuntime mints
 * the runId's tenant prefix here on every leg of a tenant-salted run,
 * so connector idempotency + rate-limit keys segment per tenant. Absent on
 * non-tenant runs — breakwater then preserves its single-tenant keys.
 */
export const BREAKWATER_ISOLATION_SCOPE_KEY = 'breakwater.isolationScope';
