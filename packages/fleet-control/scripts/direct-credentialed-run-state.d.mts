// SPDX-License-Identifier: Apache-2.0

import type { DirectConformanceNames } from './direct-credentialed-conformance-config.mjs';
import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectScenarioState } from './direct-credentialed-scenario.mjs';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';

export type DirectRunStateErrorCode =
  | 'invalid-state'
  | 'run-exists'
  | 'run-missing'
  | 'lock-unavailable'
  | 'outcome-unknown'
  | 'invocation-budget-exhausted'
  | 'unsupported-scenario-version';

export class DirectRunStateError extends Error {
  readonly code: DirectRunStateErrorCode;
  constructor(code?: DirectRunStateErrorCode);
}

type WithoutToken<Action> = Action extends unknown
  ? Omit<Action, 'token'>
  : never;

export type DirectRunActionSummary = WithoutToken<DirectReferenceAction>;

export interface DirectRunBinding {
  readonly accountId: string;
  readonly configSha256: string;
  readonly referenceModuleSetSha256: string;
  readonly resourcePrefix: string;
  readonly maxInvocations: number;
}

export interface DirectInvocationReservation {
  readonly ordinal: number;
  readonly requestSha256: string;
}

export interface DirectRunSnapshot {
  readonly version: 2;
  readonly createdAt?: string;
  readonly resumeCount?: number;
  readonly binding: DirectRunBinding;
  readonly invocationCount: number;
  readonly lastInvocation:
    | (DirectInvocationReservation &
        Readonly<{
          action: DirectRunActionSummary;
          state: 'pending' | 'settled';
        }>)
    | null;
  readonly bootstrap: DirectBootstrapState | null;
  readonly scenario?: DirectScenarioState;
  readonly teardown?: DirectTeardownState;
}

export interface DirectBootstrapContext {
  readonly names: DirectConformanceNames;
  readonly zoneId: string;
  readonly zoneName: string;
  readonly accountWorkersDevSubdomain: string;
  readonly dispatch: Readonly<{
    kind: 'first-page-404' | 'empty' | 'enumerated';
    count: number;
  }>;
}

export interface DirectBootstrapD1Receipt {
  readonly uuid: string;
  readonly name: string;
}

export interface DirectBootstrapR2Receipt {
  readonly name: string;
  readonly jurisdiction: 'default';
  readonly creationDate: string;
}

export interface DirectBootstrapUploadReceipt {
  readonly scriptName: string;
  readonly tag: string | null;
  readonly etag: string | null;
}

export type DirectBootstrapMutation =
  | 'create-fleet-d1'
  | 'create-quota-d1'
  | 'create-export-r2'
  | 'upload-reference'
  | 'enable-reference-ingress';

export type DirectBootstrapMutationReceipt =
  | Readonly<{
      kind: 'create-fleet-d1' | 'create-quota-d1';
      receipt: DirectBootstrapD1Receipt;
    }>
  | Readonly<{ kind: 'create-export-r2'; receipt: DirectBootstrapR2Receipt }>
  | Readonly<{
      kind: 'upload-reference';
      receipt: DirectBootstrapUploadReceipt;
    }>
  | Readonly<{
      kind: 'enable-reference-ingress';
      receipt: Readonly<{ enabled: true; previewsEnabled: false }>;
    }>;

export type DirectBootstrapObservation =
  | Readonly<{ kind: 'active'; deploymentId: string; versionId: string }>
  | Readonly<{ kind: 'control-read'; ordinal: number }>;

export interface DirectBootstrapState {
  readonly context: DirectBootstrapContext;
  readonly fleet: DirectBootstrapD1Receipt | null;
  readonly quota: DirectBootstrapD1Receipt | null;
  readonly exports: DirectBootstrapR2Receipt | null;
  readonly upload: DirectBootstrapUploadReceipt | null;
  readonly active: Readonly<{ deploymentId: string; versionId: string }> | null;
  readonly ingress: Readonly<{ enabled: true; previewsEnabled: false }> | null;
  readonly controlReadOrdinal: number | null;
  readonly pending: DirectBootstrapMutation | null;
}

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
  | 'routes';

export interface DirectResidualObservation {
  readonly version: 1;
  readonly surfaces: Readonly<
    Record<
      DirectResidualSurface,
      Readonly<{
        prefixCount: number;
        prefixNames: readonly string[];
        globalCount: number | null;
        exhaustive: boolean;
      }>
    >
  >;
  readonly bucketJurisdictions: readonly ['default'];
  readonly dispatch: Readonly<{
    kind: 'first-page-404' | 'empty' | 'enumerated' | 'fail-closed';
    count: number;
    status: number | null;
    prefixCount: number;
  }>;
  readonly versionsGone: boolean | null;
  readonly settleAttempts: number;
}

interface DirectTeardownSettlement {
  readonly ordinal: number;
  readonly settledByReread: boolean;
}

export interface DirectTeardownReceipts {
  readonly ingress: DirectTeardownSettlement | null;
  readonly worker:
    | (DirectTeardownSettlement &
        Readonly<{ scriptName: string; secretNames: readonly string[] }>)
    | null;
  readonly fleet:
    | (DirectTeardownSettlement & Readonly<{ uuid: string }>)
    | null;
  readonly quota:
    | (DirectTeardownSettlement & Readonly<{ uuid: string }>)
    | null;
  readonly exportObjects: readonly (DirectTeardownSettlement &
    Readonly<{ key: string }>)[];
  readonly exports:
    | (DirectTeardownSettlement & Readonly<{ name: string }>)
    | null;
}

export interface DirectTeardownState {
  readonly version: 1;
  readonly phase: DirectTeardownPhase;
  readonly pending: Readonly<{
    kind: DirectTeardownMutation;
    key?: string;
  }> | null;
  readonly receipts: DirectTeardownReceipts;
  readonly residual: DirectResidualObservation | null;
  readonly providerRequests: number;
  readonly failure: DirectTeardownFailure | null;
}

export interface DirectRunJournal {
  readonly directory: string;
  snapshot(): DirectRunSnapshot;
  recordScenario(state: DirectScenarioState): Promise<void>;
  recordResume(): Promise<void>;
  recordTeardown(state: DirectTeardownState): Promise<void>;
  assertTeardownCapacity(worstCase: DirectTeardownState): Promise<void>;
  bindBootstrapContext(context: DirectBootstrapContext): Promise<void>;
  beginBootstrapMutation(kind: DirectBootstrapMutation): Promise<void>;
  confirmBootstrapMutation(
    receipt: DirectBootstrapMutationReceipt,
  ): Promise<void>;
  recordBootstrapObservation(
    observation: DirectBootstrapObservation,
  ): Promise<void>;
  reserveInvocation(
    serializedRequest: string,
  ): Promise<DirectInvocationReservation>;
  settleInvocation(reservation: DirectInvocationReservation): Promise<void>;
  close(): Promise<void>;
}

export const DIRECT_SCENARIO_FAILURES: readonly [
  'observation-mismatch',
  'outcome-unknown',
  'proof-unavailable',
  'budget-exhausted',
  'invocation-budget-exhausted',
  'reference-refused',
  'invalid-input',
  'provider-unavailable',
  'journal-failed',
  'blocked',
];

export const DIRECT_SCENARIO_FAILURE_DETAILS: readonly [
  'platform-page',
  'transport-failure',
  'non-contract-answer',
  'delivery-window-expired',
  'phase-ceiling',
  'run-reserve',
  'below-scenario-floor',
];

export const DIRECT_SCENARIO_OPERATION_SLOTS: readonly [
  'inventory-before',
  'inventory-after',
  'audit-before',
  'audit-after',
  'migration-next',
  'cleanup-a',
  'cleanup-b',
  'cleanup-recovery',
  'cleanup-recovery-initial',
  'decommission-a',
  'decommission-b',
  'decommission-recovery',
];

export const DIRECT_TEARDOWN_PHASES: readonly DirectTeardownPhase[];

export const DIRECT_TEARDOWN_MUTATIONS: readonly DirectTeardownMutation[];

export const DIRECT_TEARDOWN_FAILURES: readonly DirectTeardownFailure[];

export const DIRECT_RESIDUAL_SURFACES: readonly DirectResidualSurface[];

export const DIRECT_TEARDOWN_MAXIMA: Readonly<{
  nameBytes: number;
  keyBytes: number;
  prefixNames: number;
  secretNames: number;
  exportObjects: number;
  settleAttempts: number;
}>;

export const DIRECT_RUN_MAX_JOURNAL_BYTES: number;

export const DIRECT_SCENARIO_ARRAY_MAXIMA: Readonly<{
  health: number;
  steps: number;
  exportVerifications: number;
  auditFindings: number;
  footprintVersionIds: number;
  deploymentVersions: number;
  inventoryCategories: number;
  inventory: Readonly<{
    databaseIds: number;
    namespaceIds: number;
    scriptNames: number;
    bucketNames: number;
    findings: number;
  }>;
}>;

export function actionSummary(
  action: DirectReferenceAction,
): DirectRunActionSummary;

export function openDirectRunState(
  input: Readonly<{
    configPath: string;
    prepared: PreparedDirectConformance;
    accountId: string;
    mode: 'run' | 'resume';
    now?: number;
  }>,
): Promise<DirectRunJournal>;

export const DIRECT_RUN_MAX_RESUME_COUNT: number;

export type DirectRunStateInspection = Readonly<{
  snapshot: DirectRunSnapshot;
  close(): Promise<void>;
}>;

export function inspectDirectRunState(
  input: Readonly<{
    configPath: string;
    prepared: PreparedDirectConformance;
    accountId: string;
    mode: 'inspect';
  }>,
): Promise<DirectRunStateInspection>;
