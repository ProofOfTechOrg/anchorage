// SPDX-License-Identifier: Apache-2.0

// The direct reference lane's shared vocabulary, beside the scenario lane's in
// `direct-credentialed-scenario-budget.mjs`. It sits below bootstrap, teardown
// and persistence so each reads one definition instead of one of them owning
// another's names.

// The secrets the reference upload binds, in the order the upload pairs them
// with their values. Teardown asserts the set it observes against this list.
export const REFERENCE_SECRET_NAMES = Object.freeze([
  'CLOUDFLARE_API_TOKEN',
  'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  'DIRECT_DEPLOYMENT_SECRETS',
]);

export const DIRECT_RECONCILIATION_STATES = Object.freeze([
  'cancelled',
  'executed',
  'failed',
]);
export const DIRECT_RECONCILIATION_MAX = 8;

export const DIRECT_SWEEP_PHASES = Object.freeze([
  'sweeping',
  'complete',
  'refused',
]);

export const DIRECT_SWEEP_ACTIONS = Object.freeze([
  'none',
  'decommission',
  'cleanup',
  'force',
]);

export const DIRECT_SWEEP_CYCLES = Object.freeze(['original', 'reprovision']);

export const DIRECT_SWEEP_FAILURE_CODES = Object.freeze(['sweep-refused']);

export const DIRECT_SWEEP_CALL_ACTIONS = Object.freeze([
  'control-read',
  'object-delete',
  'decommission-start',
  'decommission-continue',
  'cleanup-start',
  'cleanup-continue',
  'force-recovery',
  'force-observe',
  'recover-force-residual',
]);

export const DIRECT_SWEEP_CALL_OUTCOMES = Object.freeze([
  'prepared',
  'returned',
  'invalid-input',
  'invocation-busy',
  'invocation-budget-exhausted',
  'outcome-unknown',
  'injected-response-loss',
  'reference-refused',
]);

export const DIRECT_SWEEP_FAILURES = Object.freeze([
  'bootstrap-refused',
  'blocked',
  'restart-blocked',
  'invocation-authorized',
  'legacy-phase-ambiguous',
  'carrier-phase-inconsistent',
  'malformed-carrier',
  'external-staging-evidence',
  'frozen-lifecycle-mismatch',
  'prerequisite-unavailable',
  'unsupported-lifecycle-phase',
  'corrupt-lifecycle-state',
  'incomplete-application-r2-reservation',
  'missing-force-evidence',
  'inconsistent-force-evidence',
  'identity-attestation-failed',
  'invocation-budget-exhausted',
  'outcome-unknown',
  'unrecognized-answer',
]);

export const DIRECT_SWEEP_BEFORE_PHASES = Object.freeze([
  'absent',
  'force-captured',
  'database-reserved',
  'database-create-authorized',
  'database-created',
  'identity-seeded',
  'migrated',
  'application-resources-create-authorized',
  'application-resources-deployed',
  'platform-resources-deployed',
  'worker-deployed',
  'maintenance-armed',
  'publishing',
  'migrating',
  'ready',
  'decommission-advancing',
  'cleanup-advancing',
  'decommissioning',
  'traffic-removed',
  'credentials-revoked',
  'worker-deleted',
  'platform-credentials-revoked',
  'platform-resources-deleted',
  'application-resources-deleting',
  'application-resources-deleted',
  'database-exported',
  'database-deleting',
  'decommissioned',
  'rolling-back',
]);

// A decommission has fewer than 24 advancing groups in the measured 70-call
// scenario loop. The sweep omits export reads, and the remaining margin keeps
// a corrupt or non-converging lifecycle from consuming the run budget.
export const DIRECT_SWEEP_MAX_CONTINUES = 32;

export const DIRECT_TEARDOWN_PHASES = Object.freeze([
  'refused',
  'ingress',
  'worker',
  'fleet',
  'quota',
  'export-objects',
  'exports',
  'residual',
  'complete',
]);

export const DIRECT_TEARDOWN_MUTATIONS = Object.freeze([
  'disable-reference-ingress',
  'delete-reference-worker',
  'delete-fleet-d1',
  'delete-quota-d1',
  'delete-export-object',
  'delete-export-r2',
]);

export const DIRECT_TEARDOWN_FAILURES = Object.freeze([
  'scenario-incomplete',
  'outcome-unknown',
  'unexpected-object',
  'identity-mismatch',
  'residual-present',
  'forbidden',
  'provider-unavailable',
  'budget-exhausted',
  'invalid-state',
]);

// A recorded refusal carrying one of these reasons is one a later run can
// clear: `scenario-incomplete` leaves a scenario the next run completes, and
// `outcome-unknown` leaves a mutation whose outcome a re-probe settles. Every
// other reason stays terminal for automation.
export const DIRECT_TEARDOWN_RECOVERABLE_FAILURES = Object.freeze([
  'scenario-incomplete',
  'outcome-unknown',
]);

export const DIRECT_RESIDUAL_SURFACES = Object.freeze([
  'databases',
  'durableObjectNamespaces',
  'scripts',
  'buckets',
  'domains',
  'routes',
  'queues',
]);

export const DIRECT_TEARDOWN_MAXIMA = Object.freeze({
  nameBytes: 255,
  keyBytes: 1024,
  prefixNames: 16,
  secretNames: 8,
  exportObjects: 3,
  settleAttempts: 5,
});
