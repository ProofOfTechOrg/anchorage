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
// clear: the scenario can complete, and a pending invocation or bootstrap
// mutation can settle. Every other reason stays terminal for automation.
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
  exportObjects: 2,
  settleAttempts: 5,
});
