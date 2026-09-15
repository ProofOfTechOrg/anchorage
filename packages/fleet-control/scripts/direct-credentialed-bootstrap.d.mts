// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type {
  DirectInvocationClient,
  DirectInvocationFailureDetail,
} from './direct-credentialed-invocation.mjs';
import type { DirectRunJournal } from './direct-credentialed-run-state.mjs';

export type DirectBootstrapErrorCode =
  | 'invalid-input'
  | 'provider-unavailable'
  | 'observation-mismatch'
  | 'name-collision'
  | 'outcome-unknown'
  | 'budget-exhausted'
  | 'invocation-budget-exhausted'
  | 'reference-refused';

export class DirectBootstrapError extends Error {
  readonly detail: DirectInvocationFailureDetail | undefined;
  readonly code: DirectBootstrapErrorCode;
  constructor(
    code?: DirectBootstrapErrorCode,
    detail?: DirectInvocationFailureDetail,
  );
}

export function bootstrapDirectConformance(
  input: Readonly<{
    prepared: PreparedDirectConformance;
    journal: DirectRunJournal;
    apiToken: string;
    invokeSecret: string;
    fetch?: typeof fetch;
  }>,
): Promise<DirectInvocationClient>;
