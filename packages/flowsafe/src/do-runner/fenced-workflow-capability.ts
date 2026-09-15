// SPDX-License-Identifier: Apache-2.0

import type {
  D1RunExecutionIdentity,
  ProofEntryExpectation,
  StartIdentity,
} from './execution-admission.js';
import type { ExecutionFenceStore } from './execution-fence.js';
import type { RunTerminalCleanup } from './run-lifecycle.js';
import type {
  StartIdempotencyStore,
  StartReservationReading,
} from './start-idempotency.js';
import type {
  RawWorkflowSnapshot,
  SnapshotDatabase,
  SnapshotStatement,
} from './workflow-snapshot-row.js';

export const FENCED_WORKFLOW_STORAGE: unique symbol = Symbol(
  'flowsafe.fencedWorkflowStorage',
);

export interface InitialAdmissionDatabase extends SnapshotDatabase {
  /** `SnapshotDatabase.batch`, required: admission cannot run without it. */
  batch(
    statements: SnapshotStatement[],
  ): ReturnType<NonNullable<SnapshotDatabase['batch']>>;
}

export interface InitialRunAdmission {
  readonly execution: D1RunExecutionIdentity;
  readonly attemptToken: string;
  readonly mutationEpoch?: number;
  readonly startIdentity?: StartIdentity;
  readonly requestContext: Readonly<Record<string, unknown>>;
  readonly fence: ExecutionFenceStore;
  readonly reservationStore?: StartIdempotencyStore;
  readonly reservation?: StartReservationReading;
  readonly proof?: ProofEntryExpectation;
  readonly runOwnerGuard?: {
    readonly owner: StartIdentity['owner'];
    readonly reservationToken: string;
  };
  /** Plain-function invocation before the matching persist hook's first await. */
  readonly onInitialWriteAttempt: () => void;
}

export interface InitialAdmissionWitness {
  readonly execution: D1RunExecutionIdentity;
  readonly row: RawWorkflowSnapshot;
}

/** Exact admitted observation; the stored intent, not the caller, chooses disposition. */
export interface InitialTerminalizationRequest {
  readonly expected: RawWorkflowSnapshot;
  readonly execution: D1RunExecutionIdentity;
  readonly attemptToken: string;
  readonly nowMs: number;
}

/** A terminal-write observation, never admission or no-insert authority. */
export type InitialTerminalizationResult =
  | {
      readonly kind: 'terminalized' | 'already-terminalized' | 'progressed';
      readonly row: RawWorkflowSnapshot;
      readonly cleanup?: RunTerminalCleanup;
    }
  | { readonly kind: 'conflict'; readonly row?: RawWorkflowSnapshot };

/** Trusted storage primitives used by Runtime admission and owning recovery. */
export interface FencedWorkflowAdmissionCapability {
  readonly database: InitialAdmissionDatabase;
  readonly tablePrefix: string;
  /** Invokes only createRun as a plain function, never the returned Run's start. */
  withInitialAdmission<T>(
    input: InitialRunAdmission,
    createRun: () => Promise<T>,
  ): Promise<{ value: T; witness: InitialAdmissionWitness }>;
  readSnapshot(address: {
    workflowId: string;
    runId: string;
  }): Promise<RawWorkflowSnapshot | undefined>;
  /** Compare the expected initial row once, without engine execution or cleanup. */
  terminalizeInitialAdmission(
    request: InitialTerminalizationRequest,
  ): Promise<InitialTerminalizationResult>;
}
