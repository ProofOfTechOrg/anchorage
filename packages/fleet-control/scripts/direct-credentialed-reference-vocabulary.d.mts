// SPDX-License-Identifier: Apache-2.0

/**
 * The secrets the reference upload binds, in the order the upload pairs them
 * with their values.
 */
export const REFERENCE_SECRET_NAMES: readonly string[];

export type DirectTeardownPhase =
  | 'refused'
  | 'ingress'
  | 'worker'
  | 'fleet'
  | 'quota'
  | 'export-objects'
  | 'exports'
  | 'residual'
  | 'complete';

export type DirectTeardownMutation =
  | 'disable-reference-ingress'
  | 'delete-reference-worker'
  | 'delete-fleet-d1'
  | 'delete-quota-d1'
  | 'delete-export-object'
  | 'delete-export-r2';

export type DirectTeardownFailure =
  | 'scenario-incomplete'
  | 'outcome-unknown'
  | 'unexpected-object'
  | 'identity-mismatch'
  | 'residual-present'
  | 'forbidden'
  | 'provider-unavailable'
  | 'budget-exhausted'
  | 'invalid-state';

export type DirectResidualSurface =
  | 'databases'
  | 'durableObjectNamespaces'
  | 'scripts'
  | 'buckets'
  | 'domains'
  | 'routes'
  | 'queues';

export const DIRECT_TEARDOWN_PHASES: readonly DirectTeardownPhase[];

export const DIRECT_TEARDOWN_MUTATIONS: readonly DirectTeardownMutation[];

export const DIRECT_TEARDOWN_FAILURES: readonly DirectTeardownFailure[];

/**
 * The refusal reasons a later run can clear, so a recorded refusal carrying
 * one of them re-enters deletion once its precondition holds.
 */
export const DIRECT_TEARDOWN_RECOVERABLE_FAILURES: readonly DirectTeardownFailure[];

export const DIRECT_RESIDUAL_SURFACES: readonly DirectResidualSurface[];

export const DIRECT_TEARDOWN_MAXIMA: Readonly<{
  nameBytes: number;
  keyBytes: number;
  prefixNames: number;
  secretNames: number;
  exportObjects: number;
  settleAttempts: number;
}>;
