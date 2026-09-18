// SPDX-License-Identifier: Apache-2.0

import type { Stats } from 'node:fs';
import type { DirectConformanceNames } from './direct-credentialed-conformance-config.mjs';
import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type {
  DirectResidualSurface,
  DirectTeardownFailure,
  DirectTeardownMutation,
  DirectTeardownPhase,
} from './direct-credentialed-reference-vocabulary.mjs';
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

export type {
  DirectResidualSurface,
  DirectTeardownFailure,
  DirectTeardownMutation,
  DirectTeardownPhase,
} from './direct-credentialed-reference-vocabulary.mjs';

export interface DirectResidualObservation {
  /**
   * One shape, one version, for as long as the journal has one shape: a
   * journal carrying another is refused by the schema rather than migrated,
   * so this field never moves off `1`.
   */
  readonly version: 1;
  readonly surfaces: Readonly<
    Record<
      DirectResidualSurface,
      Readonly<{
        prefixCount: number;
        prefixNames: readonly string[];
        globalCount: number | null;
        /**
         * One name over three derivations, so read it per surface.
         * `scripts`, `domains`, `routes` and `queues` are single pages and
         * carry the provider's own `result_info` attestation. `databases`,
         * `durableObjectNamespaces` and `buckets` are paged to an empty page
         * and carry `true` from that completed loop. A `queues` collection the
         * account does not have answers 404, which records `false`: an empty
         * page the provider never attested.
         */
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
    routeHostnames: number;
    bucketNames: number;
    findings: number;
  }>;
}>;

export function actionSummary(
  action: DirectReferenceAction,
): DirectRunActionSummary;

/**
 * True for a settled force-terminal `before` identity: `null`, or exactly
 * `databaseId` and `scriptName`, both inside the identifier grammar the
 * journal's own `before` shape decodes with. A caller guarding an observed
 * identity reads this instead of restating that rule.
 */
export function isForceIdentity(value: unknown): boolean;

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

/**
 * True while the journal does not know the outcome of an invocation or of a
 * bootstrap mutation. Teardown's own pending mutation is deliberately not part
 * of it: the paths that publish a teardown receipt are recording that
 * mutation.
 */
export function mutationPending(snapshot: DirectRunSnapshot): boolean;

/** The open flags every private journal and evidence handle carries. */
export function fileFlags(access: number): number;

/**
 * Refuses a handle whose owner, mode, type or link count is not the private
 * one this lane writes: `0700` for a directory, `0600` for a single-linked
 * file owned by the current user.
 */
export function assertPrivate(stat: Stats, directory: boolean): void;

/**
 * Removes the staging files a `prefix`/`suffix` pair names from `directory`,
 * which an interrupted publication leaves behind between a create and its
 * rename.
 */
export function sweepStagedFiles(
  directory: string,
  prefix: string,
  suffix: string,
): Promise<void>;

/**
 * The 24-byte ISO-8601 shape every journal and evidence timestamp carries.
 * Anchored and stateless, so callers share the one pattern.
 */
export const DIRECT_RUN_TIMESTAMP: RegExp;

export type DirectRunStateInspection = Readonly<{
  /** The run directory this module owns, so a caller derives no layout of its own. */
  directory: string;
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
