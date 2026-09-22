// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectInvocationClient } from './direct-credentialed-invocation.mjs';
import type {
  DirectRunJournal,
  DirectSweepFailure,
} from './direct-credentialed-run-state.mjs';

export type DirectSweepResult =
  | Readonly<{ status: 'complete' }>
  | Readonly<{ status: 'refused'; reason: DirectSweepFailure }>;

export function runDirectCredentialedSweep(
  input: Readonly<{
    prepared: PreparedDirectConformance;
    journal: DirectRunJournal;
    invocation: DirectInvocationClient;
    apiToken: string;
    fetch?: typeof fetch;
  }>,
): Promise<DirectSweepResult>;
