// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectRunJournal } from './direct-credentialed-run-state.mjs';
import type { DirectReferenceAction } from './direct-reference-contract.mjs';

export type DirectInvocationErrorCode =
  | 'invalid-input'
  | 'invocation-busy'
  | 'invocation-budget-exhausted'
  | 'outcome-unknown'
  | 'injected-response-loss'
  | 'reference-refused';

export type DirectReferenceRefusalCode =
  | 'operation-refused'
  | 'wrong-operation'
  | 'missing-continuation'
  | 'budget-exhausted';

export class DirectInvocationError extends Error {
  readonly code: DirectInvocationErrorCode;
  readonly attempts: DirectInvocationAttempts | undefined;
  readonly referenceCode: DirectReferenceRefusalCode | undefined;
  constructor(
    code?: DirectInvocationErrorCode,
    attempts?: DirectInvocationAttempts,
    referenceCode?: DirectReferenceRefusalCode,
  );
}

export interface DirectInvocationAttempts {
  readonly provider: number;
  readonly maintenance: number;
  readonly application: number;
}

export interface DirectInvocationResult {
  readonly result: unknown;
  readonly attempts: DirectInvocationAttempts;
}

export interface DirectInvocationClient {
  invoke(action: DirectReferenceAction): Promise<DirectInvocationResult>;
}

export function createDirectInvocationClient(
  input: Readonly<{
    prepared: PreparedDirectConformance;
    journal: DirectRunJournal;
    accountWorkersDevSubdomain: string;
    invokeSecret: string;
    fetch?: typeof fetch;
  }>,
): DirectInvocationClient;
