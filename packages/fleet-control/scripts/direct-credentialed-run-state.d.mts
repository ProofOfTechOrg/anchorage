// SPDX-License-Identifier: Apache-2.0

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
  readonly version: 1;
  readonly binding: DirectRunBinding;
  readonly invocationCount: number;
  readonly lastInvocation:
    | (DirectInvocationReservation &
        Readonly<{
          action: DirectRunActionSummary;
          state: 'pending' | 'settled';
        }>)
    | null;
}

export interface DirectRunJournal {
  readonly directory: string;
  snapshot(): DirectRunSnapshot;
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
