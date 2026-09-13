// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type {
  DirectResidualObservation,
  DirectRunJournal,
  DirectTeardownFailure,
  DirectTeardownPhase,
  DirectTeardownReceipts,
} from './direct-credentialed-run-state.mjs';

export type {
  DirectResidualObservation,
  DirectResidualSurface,
  DirectTeardownFailure,
  DirectTeardownMutation,
  DirectTeardownPhase,
  DirectTeardownReceipts,
  DirectTeardownState,
} from './direct-credentialed-run-state.mjs';

export class DirectTeardownError extends Error {
  readonly code: DirectTeardownFailure;
  constructor(code?: DirectTeardownFailure);
}

export interface DirectTeardownProofs {
  readonly retainedIdentities: Readonly<{
    fleetUuid: string | null;
    quotaUuid: string | null;
    exportBucket: string | null;
    scriptName: string | null;
    activeVersionId: string | null;
  }>;
  readonly receipts: DirectTeardownReceipts;
  readonly residual: DirectResidualObservation | null;
  readonly providerRequests: number;
  readonly failure: DirectTeardownFailure | null;
}

export type DirectTeardownOutcome =
  | Readonly<{ status: 'cleaned'; facts: DirectTeardownProofs }>
  | Readonly<{
      status: 'retained';
      reason: DirectTeardownFailure;
      phase: DirectTeardownPhase;
      facts: DirectTeardownProofs;
    }>;

export function teardownDirectReference(
  input: Readonly<{
    prepared: PreparedDirectConformance;
    journal: DirectRunJournal;
    apiToken: string;
    fetch?: typeof fetch;
    delay?: (milliseconds: number) => Promise<void>;
  }>,
): Promise<DirectTeardownOutcome>;
