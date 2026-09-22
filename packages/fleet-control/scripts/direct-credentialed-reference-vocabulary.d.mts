// SPDX-License-Identifier: Apache-2.0

/**
 * The secrets the reference upload binds, in the order the upload pairs them
 * with their values.
 */
export const REFERENCE_SECRET_NAMES: readonly string[];

export type DirectReconciliationState = 'cancelled' | 'executed' | 'failed';
export const DIRECT_RECONCILIATION_STATES: readonly DirectReconciliationState[];
export const DIRECT_RECONCILIATION_MAX: number;

export type DirectSweepPhase = 'sweeping' | 'complete' | 'refused';
export type DirectSweepAction = 'none' | 'decommission' | 'cleanup' | 'force';
export type DirectSweepCycle = 'original' | 'reprovision';
export type DirectSweepFailureCode = 'sweep-refused';
export type DirectSweepCallAction =
  | 'control-read'
  | 'object-delete'
  | 'decommission-start'
  | 'decommission-continue'
  | 'cleanup-start'
  | 'cleanup-continue'
  | 'force-recovery'
  | 'force-observe'
  | 'recover-force-residual';
export type DirectSweepCallOutcome =
  | 'prepared'
  | 'returned'
  | 'invalid-input'
  | 'invocation-busy'
  | 'invocation-budget-exhausted'
  | 'outcome-unknown'
  | 'injected-response-loss'
  | 'reference-refused';
export type DirectSweepFailure =
  | 'bootstrap-refused'
  | 'blocked'
  | 'restart-blocked'
  | 'invocation-authorized'
  | 'legacy-phase-ambiguous'
  | 'carrier-phase-inconsistent'
  | 'malformed-carrier'
  | 'external-staging-evidence'
  | 'frozen-lifecycle-mismatch'
  | 'prerequisite-unavailable'
  | 'unsupported-lifecycle-phase'
  | 'corrupt-lifecycle-state'
  | 'incomplete-application-r2-reservation'
  | 'missing-force-evidence'
  | 'inconsistent-force-evidence'
  | 'identity-attestation-failed'
  | 'invocation-budget-exhausted'
  | 'outcome-unknown'
  | 'unrecognized-answer';
export type DirectSweepBeforePhase =
  | 'absent'
  | 'force-captured'
  | 'database-reserved'
  | 'database-create-authorized'
  | 'database-created'
  | 'identity-seeded'
  | 'migrated'
  | 'application-resources-create-authorized'
  | 'application-resources-deployed'
  | 'platform-resources-deployed'
  | 'worker-deployed'
  | 'maintenance-armed'
  | 'publishing'
  | 'migrating'
  | 'ready'
  | 'decommission-advancing'
  | 'cleanup-advancing'
  | 'decommissioning'
  | 'traffic-removed'
  | 'credentials-revoked'
  | 'worker-deleted'
  | 'platform-credentials-revoked'
  | 'platform-resources-deleted'
  | 'application-resources-deleting'
  | 'application-resources-deleted'
  | 'database-exported'
  | 'database-deleting'
  | 'decommissioned'
  | 'rolling-back';

export const DIRECT_SWEEP_PHASES: readonly DirectSweepPhase[];
export const DIRECT_SWEEP_ACTIONS: readonly DirectSweepAction[];
export const DIRECT_SWEEP_CYCLES: readonly DirectSweepCycle[];
export const DIRECT_SWEEP_FAILURE_CODES: readonly DirectSweepFailureCode[];
export const DIRECT_SWEEP_CALL_ACTIONS: readonly DirectSweepCallAction[];
export const DIRECT_SWEEP_CALL_OUTCOMES: readonly DirectSweepCallOutcome[];
export const DIRECT_SWEEP_FAILURES: readonly DirectSweepFailure[];
export const DIRECT_SWEEP_BEFORE_PHASES: readonly DirectSweepBeforePhase[];
export const DIRECT_SWEEP_MAX_CONTINUES: number;

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
