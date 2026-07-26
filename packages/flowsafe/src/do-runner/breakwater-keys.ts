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

/**
 * requestContext key the breakwater connector write-gate reads. The value is
 * a plain array of approved connector ids (breakwater's approvalGranted does
 * `Array.isArray(value) && value.includes(connectorId)`).
 */
export const BREAKWATER_APPROVED_CONNECTORS_KEY =
  'breakwater.approvedConnectors';

/** requestContext key breakwater actor resolution reads ({ id, role }). */
export const BREAKWATER_ACTOR_KEY = 'breakwater.actor';

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
