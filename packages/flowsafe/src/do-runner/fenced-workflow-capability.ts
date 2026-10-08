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
  /**
   * Stamp `startOutcomeUnknownAt` on a no-intent terminalization, so the
   * settled-row guard refuses the start leg's later writes. Set it only with
   * evidence that the leg stopped.
   */
  readonly markOutcomeUnknown?: true;
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
  /**
   * Mark an executing leg's run row live by its `updatedAt` alone, and report
   * what the row holds: `live`, `settled` (another writer settled the run; the
   * row is left as it is), or `absent`. The runtime aborts the leg on
   * `settled`, so report it only when the same storage refuses that leg's later
   * writes over the row, as the settled-row guard does; a capability without
   * such a guard resolves nothing, which is no evidence and aborts nothing.
   * With `withStoredRun`, the runtime also aborts a leg on `absent` once that
   * leg stored its row, so report `absent` only for a row that is gone.
   * Without `touchRun` a run whose leg stops is never settled automatically.
   */
  touchRun?(
    address: { workflowId: string; runId: string },
    nowMs: number,
  ): Promise<
    // biome-ignore lint/suspicious/noConfusingVoidType: an implementation resolving nothing stays assignable
    'live' | 'settled' | 'absent' | void
  >;
  /**
   * Replace the snapshot and `updatedAt` of the exact row `expected` names, or
   * report `false` when any of its columns changed since. It writes the bytes
   * it is given, outside the settled-row guard, and rejects, writing nothing,
   * a `snapshot` that is not a JSON object or an `updatedAt` that is not in
   * `Date.prototype.toISOString()` form. It also rejects a `snapshot` SQLite
   * cannot parse, such as one nested past its JSON depth limit, over a row
   * SQLite can parse, with `RunStateNotStorableError`. Settlement needs it
   * beside `touchRun`: without it a run whose leg stops is never settled.
   */
  replaceSnapshot?(
    expected: RawWorkflowSnapshot,
    replacement: { snapshot: string; updatedAt: string },
  ): Promise<boolean>;
  /**
   * Write the run's lifecycle, `timestamp` and `updatedAt` and no other part of
   * its snapshot, but only while the stored status is `expected.status`, the
   * stored lifecycle's revision is `expected.lifecycleRevision` (or it has no
   * lifecycle, when that is omitted), the stored lifecycle is exactly
   * `expected.lifecycle` when that is given, the row is not settled and its
   * lifecycle records no disputed economic operation; otherwise
   * resolve `false`, writing nothing. Pass `expected.lifecycle` as read from the
   * row, with the `lifecycleRevision` it carries: another writer can reach the
   * same revision with a different lifecycle, and `lifecycle` without the
   * matching `lifecycleRevision` misses on every attempt.
   * `FencedWorkflowsStorageD1` compares the stored JSON text, so there a
   * lifecycle rebuilt from parsed fields, whose keys can come out in another
   * order, misses as well. A write that leaves the status and lifecycle alone,
   * such as a leg's step progress, does not make it miss. It rejects, writing
   * nothing, a malformed address, expectation or patch.
   */
  patchRunLifecycle?(
    address: { workflowId: string; runId: string },
    expected: {
      status: string;
      lifecycleRevision?: number;
      lifecycle?: object;
    },
    patch: {
      lifecycle: object;
      timestamp: number;
      updatedAt: string;
    },
  ): Promise<boolean>;
  /**
   * Run `operation`, a whole leg of the run `scope` names, inside `scope`.
   * When a write inside it stores the run's row, the storage sets
   * `scope.rowStored` on the `scope` object it is passed. The runtime also
   * sets it after a fenced admission and when a resume begins, and reads it in
   * the liveness touch and in the end-of-leg reconcile. Once it is `true`, a
   * write of that run that finds no row is refused, writing nothing, with
   * `RunSettledConflictError`, or with `RunStateNotStorableError` for a
   * snapshot SQLite cannot parse. Writes of any other run address are not
   * guarded. A capability that copies `scope` instead of using the object it
   * is passed loses the insert guard until its own first write.
   */
  withStoredRun?<T>(
    scope: {
      readonly workflowId: string;
      readonly runId: string;
      rowStored: boolean;
    },
    operation: () => Promise<T>,
  ): Promise<T>;
}
