// SPDX-License-Identifier: Apache-2.0

import {
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal-identity.js';
import { DoStatusError } from './do-status-error.js';
import { isPathSafeId } from './path-safe-id.js';

/** Runtime-owned request-context key for durable run lifecycle metadata. */
export const RUN_LIFECYCLE_CONTEXT_KEY = 'flowsafe.runLifecycle';

export type RunTerminalStatus = 'cancelled' | 'timed_out';

const RUN_TERMINAL_ERRORS = {
  cancelled: { name: 'RunCancelledError', message: 'run was cancelled' },
  timed_out: { name: 'RunTimedOutError', message: 'run deadline expired' },
} as const satisfies Record<
  RunTerminalStatus,
  { name: string; message: string }
>;

/** The stored error of a terminal transition: the name consumers match on, and its message. */
export function runTerminalError(
  status: RunTerminalStatus,
): (typeof RUN_TERMINAL_ERRORS)[RunTerminalStatus] {
  return { ...RUN_TERMINAL_ERRORS[status] };
}

/**
 * The reason the runtime aborts a leg with: named `AbortError`, which Mastra
 * and the AI SDK read as an abort, with `cause` as its cause.
 */
export function legAbortReason(cause: Error): Error {
  return Object.assign(new Error(cause.message, { cause }), {
    name: 'AbortError',
  });
}

/**
 * The reason a leg's abort carries when a cancellation or timeout ends it:
 * one handled in its isolate that cuts it, or one recorded before its engine
 * run exists, by any instance, that refuses it. Its cause is the run's
 * terminal error: the engine reports a cut leg only as `canceled`, whichever
 * transition cut it.
 */
export function terminalLegAbortReason(status: RunTerminalStatus): Error {
  const { name, message } = runTerminalError(status);
  return legAbortReason(Object.assign(new Error(message), { name }));
}

/** The terminal status a {@link terminalLegAbortReason} names, if `reason` is one. */
export function terminalStatusOfLegAbort(
  reason: unknown,
): RunTerminalStatus | undefined {
  if (!(reason instanceof Error) || !(reason.cause instanceof Error))
    return undefined;
  const { name } = reason.cause;
  return (Object.keys(RUN_TERMINAL_ERRORS) as RunTerminalStatus[]).find(
    (status) => RUN_TERMINAL_ERRORS[status].name === name,
  );
}

/**
 * Whether `reason` is a {@link legAbortReason} for a leg whose run another
 * instance settled or whose row is no longer stored, so a caller cannot assume
 * a stored row to read.
 */
export function isSettledLegAbort(reason: unknown): boolean {
  return (
    reason instanceof Error && reason.cause instanceof RunSettledConflictError
  );
}

const RUN_TERMINAL_ERROR_CODES = [
  'CANCELLED',
  'TIMED_OUT',
  'INTERRUPTED',
  'RUN_STATE_NOT_STORABLE',
] as const;

export interface RunTerminalErrorEnvelope {
  code: (typeof RUN_TERMINAL_ERROR_CODES)[number];
  message: string;
}

/** The envelope a stored terminal record carries; an interruption has none. */
type RunTerminalRecordError = RunTerminalErrorEnvelope & {
  code: 'CANCELLED' | 'TIMED_OUT';
};

export function isRunTerminalErrorCode(
  value: unknown,
): value is RunTerminalErrorEnvelope['code'] {
  return (RUN_TERMINAL_ERROR_CODES as readonly unknown[]).includes(value);
}

/** The `INTERRUPTED` envelope message of a run whose execution leg ended mid-step. */
export const RUN_INTERRUPTED_MESSAGE =
  'Run execution stopped mid-step before a durable outcome was recorded; external effects may have occurred. This run will not be automatically re-executed.';

/** The `RUN_STATE_NOT_STORABLE` envelope message of a run whose state could not be stored. */
export const RUN_STATE_NOT_STORABLE_MESSAGE =
  'Run state could not be stored because SQLite cannot parse it as JSON; the step that produced it may have had external effects. This run will not be automatically re-executed.';

export interface RunLifecycleBlockedReason {
  code: 'DISPUTED_SETTLEMENT';
  message: string;
}

export class RunLifecycleBlockedError extends Error {
  readonly reason: RunLifecycleBlockedReason;

  constructor(reason: RunLifecycleBlockedReason) {
    super(reason.message);
    this.name = 'RunLifecycleBlockedError';
    this.reason = reason;
  }
}

/**
 * A snapshot write over a settled run row that does not advance the row's
 * lifecycle: the settlement stands, and the writer (typically an execution leg
 * still running on another instance) is refused.
 */
export class RunSettledConflictError extends Error {
  constructor(workflowId: string, runId: string) {
    super(
      `run '${runId}' of workflow '${workflowId}' is already settled; the write does not advance its lifecycle`,
    );
    this.name = 'RunSettledConflictError';
  }
}

/** A run snapshot SQLite cannot parse as JSON, refused before it is stored. */
export class RunStateNotStorableError extends DoStatusError {
  readonly status = 422;
  readonly reason = { code: 'RUN_STATE_NOT_STORABLE' } as const;

  constructor(workflowId: string, runId: string) {
    super(
      `run '${runId}' of workflow '${workflowId}' state cannot be stored: SQLite cannot parse it as JSON`,
    );
    this.name = 'RunStateNotStorableError';
  }
}

export interface RunTerminalCleanup {
  revision: number;
  status: RunTerminalStatus;
  cleanupCompleted: boolean;
  scheduleDispatch?: RunScheduleDispatch;
}

/**
 * Trusted settlement projection supplied by an economic-operation host.
 * Flowsafe only interprets `disputed`; every other state remains host-defined.
 */
export interface RunEconomicOperation {
  id: string;
  settlementState: string;
}

export interface RunScheduleDispatch {
  scheduleId: string;
  dispatchId: string;
}

export interface RunLifecyclePrincipal {
  kind: ExecutionPrincipalKind;
  id: string;
}

export interface RunLifecycleState {
  version: 1;
  /** Monotonic compare-and-swap revision owned by the run's Durable Object. */
  revision: number;
  /** Epoch milliseconds. */
  deadlineAt?: number;
  /**
   * Epoch milliseconds at which the run object settled a run whose execution
   * leg ended mid-step as `failed`. Only the run object writes it. A settling
   * marker: see RUN_SETTLING_MARKERS.
   */
  interruptedAt?: number;
  /**
   * Epoch milliseconds at which start recovery ended a run whose start leg
   * stopped as `StartOutcomeUnknown`. A settling marker only
   * (RUN_SETTLING_MARKERS), so the guard refuses that leg's later writes; it
   * is not part of the run's summary.
   */
  startOutcomeUnknownAt?: number;
  /**
   * Epoch milliseconds at which the runtime recorded as `failed` a run whose
   * leg's write was refused as state that cannot be stored. A settling marker
   * (RUN_SETTLING_MARKERS).
   */
  stateNotStorableAt?: number;
  economicOperations?: RunEconomicOperation[];
  scheduleDispatch?: RunScheduleDispatch;
  transitionIntent?: {
    status: RunTerminalStatus;
    requestedAt: number;
    replayPrincipals: RunLifecyclePrincipal[];
    expectedRevision?: number;
    expectedDeadlineAt?: number;
  };
  terminal?: {
    status: RunTerminalStatus;
    error: RunTerminalRecordError;
    transitionedAt: number;
    /** Exact identities allowed to replay this terminal transition after ownership release. */
    replayPrincipals: RunLifecyclePrincipal[];
    /** Set only after approval/dispatch/ownership cleanup has completed. */
    cleanupCompletedAt?: number;
  };
}

/**
 * Lifecycle fields whose presence settles a run row. A write over a settled
 * row is admitted only when it advances the revision and carries
 * RUN_SETTLED_IDENTITY_PATHS unchanged; `FencedWorkflowsStorageD1` renders its
 * settled-row guard from both lists.
 */
export const RUN_SETTLING_MARKERS = [
  'terminal',
  'interruptedAt',
  'startOutcomeUnknownAt',
  'stateNotStorableAt',
] as const satisfies readonly (keyof RunLifecycleState)[];

type SettlingTimeMarker = Exclude<
  (typeof RUN_SETTLING_MARKERS)[number],
  'terminal'
>;

const SETTLING_TIME_MARKERS = RUN_SETTLING_MARKERS.filter(
  (marker): marker is SettlingTimeMarker => marker !== 'terminal',
);

type RunSettledIdentityPath =
  | SettlingTimeMarker
  | `terminal.${keyof NonNullable<RunLifecycleState['terminal']>}`;

/**
 * The failure a settling marker records for a run the runtime fails itself:
 * the error the snapshot stores and the envelope its summary carries.
 */
export const RUN_FAILURE_MARKERS = {
  interruptedAt: {
    errorName: 'RunInterruptedError',
    envelope: { code: 'INTERRUPTED', message: RUN_INTERRUPTED_MESSAGE },
  },
  stateNotStorableAt: {
    errorName: 'RunStateNotStorableError',
    envelope: {
      code: 'RUN_STATE_NOT_STORABLE',
      message: RUN_STATE_NOT_STORABLE_MESSAGE,
    },
  },
} as const satisfies Partial<
  Record<
    SettlingTimeMarker,
    { errorName: string; envelope: RunTerminalErrorEnvelope }
  >
>;

/** The envelope of the failure marker a lifecycle carries, if any. */
export function failureEnvelope(
  lifecycle: RunLifecycleState | undefined,
): RunTerminalErrorEnvelope | undefined {
  for (const marker of Object.keys(
    RUN_FAILURE_MARKERS,
  ) as (keyof typeof RUN_FAILURE_MARKERS)[])
    if (lifecycle?.[marker] !== undefined)
      return { ...RUN_FAILURE_MARKERS[marker].envelope };
  return undefined;
}

export const RUN_SETTLED_IDENTITY_PATHS = [
  ...SETTLING_TIME_MARKERS,
  'terminal.status',
  'terminal.transitionedAt',
] as const satisfies readonly RunSettledIdentityPath[];

export function nextLifecycleRevision(current: number): number {
  if (
    !Number.isSafeInteger(current) ||
    current < 0 ||
    current === Number.MAX_SAFE_INTEGER
  )
    throw new Error('run lifecycle revision cannot advance');
  return current + 1;
}

/** The next revision of a lifecycle, or of a new one, with `patch` applied. */
export function advanceLifecycle(
  lifecycle: RunLifecycleState | undefined,
  patch: Omit<Partial<RunLifecycleState>, 'version' | 'revision'>,
): RunLifecycleState {
  return {
    ...lifecycle,
    ...patch,
    version: 1,
    revision: nextLifecycleRevision(lifecycle?.revision ?? 0),
  };
}

export function terminalCleanupFor(
  lifecycle: RunLifecycleState | undefined,
): RunTerminalCleanup | undefined {
  const terminalState = lifecycle?.terminal;
  if (!lifecycle || !terminalState) return undefined;
  return {
    revision: lifecycle.revision,
    status: terminalState.status,
    cleanupCompleted: terminalState.cleanupCompletedAt !== undefined,
    ...(lifecycle.scheduleDispatch
      ? { scheduleDispatch: lifecycle.scheduleDispatch }
      : {}),
  };
}

export function projectTerminalLifecycle(
  lifecycle: RunLifecycleState | undefined,
  status: RunTerminalStatus,
  nowMs: number,
  replayPrincipals: RunLifecyclePrincipal[],
): RunLifecycleState & {
  terminal: NonNullable<RunLifecycleState['terminal']>;
} {
  const base = lifecycle
    ? Object.fromEntries(
        Object.entries(lifecycle).filter(([key]) => key !== 'transitionIntent'),
      )
    : { version: 1 as const, revision: 0 };
  return {
    ...base,
    version: 1,
    revision: nextLifecycleRevision(lifecycle?.revision ?? 0),
    terminal: {
      status,
      error: {
        code: status === 'cancelled' ? 'CANCELLED' : 'TIMED_OUT',
        message: runTerminalError(status).message,
      },
      transitionedAt: nowMs,
      replayPrincipals,
    },
  };
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function validTime(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function economicOperations(
  value: unknown,
): RunEconomicOperation[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value))
    throw new Error('stored run lifecycle is malformed');
  const length = value.length;
  if (!Number.isSafeInteger(length) || length < 0 || length > 0xffff_ffff) {
    throw new Error('stored run lifecycle is malformed');
  }
  const operations: RunEconomicOperation[] = [];
  for (let index = 0; index < length; index++) {
    if (!(index in value)) throw new Error('stored run lifecycle is malformed');
    const operation = record(value[index]);
    if (!operation) throw new Error('stored run lifecycle is malformed');
    const { id, settlementState } = operation;
    if (
      !isPathSafeId(id) ||
      typeof settlementState !== 'string' ||
      settlementState.length === 0 ||
      settlementState.length > 100
    ) {
      throw new Error('stored run lifecycle is malformed');
    }
    operations.push({ id, settlementState });
  }
  return operations;
}

function scheduleDispatch(value: unknown): RunScheduleDispatch | undefined {
  if (value === undefined) return undefined;
  const dispatch = record(value);
  if (
    !dispatch ||
    !isPathSafeId(dispatch.scheduleId) ||
    !isPathSafeId(dispatch.dispatchId)
  ) {
    throw new Error('stored run lifecycle is malformed');
  }
  return {
    scheduleId: dispatch.scheduleId,
    dispatchId: dispatch.dispatchId,
  };
}

function terminal(value: unknown): RunLifecycleState['terminal'] | undefined {
  if (value === undefined) return undefined;
  const stored = record(value);
  const error = record(stored?.error);
  if (
    !stored ||
    (stored.status !== 'cancelled' && stored.status !== 'timed_out') ||
    !validTime(stored.transitionedAt) ||
    !error ||
    (error.code !== 'CANCELLED' && error.code !== 'TIMED_OUT') ||
    typeof error.message !== 'string' ||
    error.message.length === 0 ||
    error.message.length > 500 ||
    (stored.status === 'cancelled' && error.code !== 'CANCELLED') ||
    (stored.status === 'timed_out' && error.code !== 'TIMED_OUT') ||
    !Array.isArray(stored.replayPrincipals) ||
    (stored.cleanupCompletedAt !== undefined &&
      !validTime(stored.cleanupCompletedAt))
  ) {
    throw new Error('stored run lifecycle is malformed');
  }
  const replayPrincipals = stored.replayPrincipals.map((principalValue) => {
    const principal = record(principalValue);
    if (
      !principal ||
      !isExecutionPrincipalKind(principal.kind) ||
      !isExecutionPrincipalId(principal.id)
    ) {
      throw new Error('stored run lifecycle is malformed');
    }
    return { kind: principal.kind, id: principal.id };
  });
  if (replayPrincipals.length === 0 || replayPrincipals.length > 2) {
    throw new Error('stored run lifecycle is malformed');
  }
  return {
    status: stored.status,
    error: { code: error.code, message: error.message },
    transitionedAt: stored.transitionedAt,
    replayPrincipals,
    ...(stored.cleanupCompletedAt === undefined
      ? {}
      : { cleanupCompletedAt: stored.cleanupCompletedAt }),
  };
}

function transitionIntent(
  value: unknown,
): RunLifecycleState['transitionIntent'] | undefined {
  if (value === undefined) return undefined;
  const stored = record(value);
  if (
    !stored ||
    (stored.status !== 'cancelled' && stored.status !== 'timed_out') ||
    !validTime(stored.requestedAt) ||
    !Array.isArray(stored.replayPrincipals) ||
    (stored.expectedRevision !== undefined &&
      (!Number.isSafeInteger(stored.expectedRevision) ||
        (stored.expectedRevision as number) < 1)) ||
    (stored.expectedDeadlineAt !== undefined &&
      !validTime(stored.expectedDeadlineAt))
  ) {
    throw new Error('stored run lifecycle is malformed');
  }
  const replayPrincipals = canonicalReplayPrincipals(
    stored.replayPrincipals as RunLifecyclePrincipal[],
  );
  return {
    status: stored.status,
    requestedAt: stored.requestedAt,
    replayPrincipals,
    ...(stored.expectedRevision === undefined
      ? {}
      : { expectedRevision: stored.expectedRevision as number }),
    ...(stored.expectedDeadlineAt === undefined
      ? {}
      : { expectedDeadlineAt: stored.expectedDeadlineAt as number }),
  };
}

function sameLifecyclePrincipal(
  left: RunLifecyclePrincipal,
  right: RunLifecyclePrincipal,
): boolean {
  return left.kind === right.kind && left.id === right.id;
}

export function canonicalReplayPrincipals(
  values: readonly RunLifecyclePrincipal[],
): RunLifecyclePrincipal[] {
  const principals: RunLifecyclePrincipal[] = [];
  for (const value of values) {
    if (
      !isExecutionPrincipalKind(value.kind) ||
      !isExecutionPrincipalId(value.id)
    ) {
      throw new Error('run lifecycle principal is malformed');
    }
    if (!principals.some((stored) => sameLifecyclePrincipal(stored, value))) {
      principals.push({ kind: value.kind, id: value.id });
    }
  }
  if (principals.length === 0 || principals.length > 2) {
    throw new Error('run lifecycle requires one or two replay principals');
  }
  return principals;
}

export function parseRunLifecycle(
  value: unknown,
): RunLifecycleState | undefined {
  if (value === undefined) return undefined;
  const stored = record(value);
  if (
    stored?.version !== 1 ||
    !Number.isSafeInteger(stored.revision) ||
    (stored.revision as number) < 1 ||
    (stored.deadlineAt !== undefined && !validTime(stored.deadlineAt)) ||
    SETTLING_TIME_MARKERS.some(
      (marker) => stored[marker] !== undefined && !validTime(stored[marker]),
    )
  ) {
    throw new Error('stored run lifecycle is malformed');
  }
  return {
    version: 1,
    revision: stored.revision as number,
    ...(stored.deadlineAt === undefined
      ? {}
      : { deadlineAt: stored.deadlineAt as number }),
    ...Object.fromEntries(
      SETTLING_TIME_MARKERS.filter(
        (marker) => stored[marker] !== undefined,
      ).map((marker) => [marker, stored[marker] as number]),
    ),
    ...(stored.economicOperations === undefined
      ? {}
      : { economicOperations: economicOperations(stored.economicOperations) }),
    ...(stored.scheduleDispatch === undefined
      ? {}
      : { scheduleDispatch: scheduleDispatch(stored.scheduleDispatch) }),
    ...(stored.transitionIntent === undefined
      ? {}
      : { transitionIntent: transitionIntent(stored.transitionIntent) }),
    ...(stored.terminal === undefined
      ? {}
      : { terminal: terminal(stored.terminal) }),
  };
}

export function lifecycleFromRequestContext(
  requestContext: Record<string, unknown> | undefined,
): RunLifecycleState | undefined {
  return parseRunLifecycle(requestContext?.[RUN_LIFECYCLE_CONTEXT_KEY]);
}

export function canonicalEconomicOperations(
  value: readonly RunEconomicOperation[] | undefined,
): RunEconomicOperation[] | undefined {
  return economicOperations(value);
}

export function canonicalScheduleDispatch(
  value: RunScheduleDispatch | undefined,
): RunScheduleDispatch | undefined {
  return scheduleDispatch(value);
}

export function hasDisputedSettlement(
  lifecycle: RunLifecycleState | undefined,
): boolean {
  return (
    lifecycle?.economicOperations?.some(
      (operation) => operation.settlementState === 'disputed',
    ) ?? false
  );
}
