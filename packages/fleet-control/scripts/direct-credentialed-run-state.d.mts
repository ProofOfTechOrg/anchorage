// SPDX-License-Identifier: Apache-2.0

import type { DirectConformanceNames } from './direct-credentialed-conformance-config.mjs';
import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';

export type DirectRunStateErrorCode =
  | 'invalid-state'
  | 'run-exists'
  | 'run-missing'
  | 'lock-unavailable'
  | 'outcome-unknown'
  | 'invocation-budget-exhausted';

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

export interface DirectRunJournal {
  readonly directory: string;
  snapshot(): DirectRunSnapshot;
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

export function openDirectRunState(
  input: Readonly<{
    configPath: string;
    prepared: PreparedDirectConformance;
    accountId: string;
    mode: 'run' | 'resume';
  }>,
): Promise<DirectRunJournal>;
