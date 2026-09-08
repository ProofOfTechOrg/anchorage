// SPDX-License-Identifier: Apache-2.0

import {
  EXECUTION_PRINCIPAL_KINDS,
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal-identity.js';
import {
  InvalidExecutionIdentityError,
  normalizeRunExecutionIdentity,
  normalizeStartIdentity,
  type RunExecutionIdentity,
} from './execution-admission.js';
import { isPathSafeId } from './path-safe-id.js';

export const START_RESERVATION_STATES = [
  'reserved',
  'started',
  'terminal',
] as const;

/**
 * Where a reservation is in its life:
 *
 *   reserved  the key means this runId, and nobody has started it yet
 *   started   one caller won the claim and is (or was) executing
 *   terminal  the run reached a terminal state; the key is spent
 *
 * States move forward except for an exact, unbound claim released after local
 * preflight or definitive no-insert evidence proves no execution was admitted.
 */
export type StartReservationState = (typeof START_RESERVATION_STATES)[number];

export const START_TARGET_KINDS = ['workflow', 'agent'] as const;

/** Which execution family a key names — a workflow run, or an agent run. */
export type StartTargetKind = (typeof START_TARGET_KINDS)[number];

/**
 * WHO a key belongs to. An execution principal, projected to the same two
 * fields `ResourceOwner` carries, and for the same reason: a key is a
 * capability to converge on somebody's run, so it must be scoped to whoever
 * created it and unforgeable from tenant traffic.
 */
export interface StartReservationOwner {
  readonly kind: ExecutionPrincipalKind;
  readonly id: string;
}

/** One reservation row, as every surface reads it. */
export interface StartReservation {
  readonly key: string;
  readonly owner: StartReservationOwner;
  readonly targetKind: StartTargetKind;
  readonly targetId: string;
  readonly runId: string;
  /**
   * The agent run's thread, when the target is an agent. It is the run's
   * ADDRESS: a workflow run is reachable from (workflowId, runId) alone, but an
   * agent run lives in a thread object and a retry that minted a fresh thread
   * would otherwise have no way back to the original. Absent for workflows,
   * where storing a derivable address would be a second source of truth.
   */
  readonly threadId?: string;
  readonly state: StartReservationState;
  /**
   * Epoch ms of the reserve that created this row. Provenance only: the purge
   * horizon is measured from `updatedAt`, so that a key's validity runs from
   * the moment it was SPENT rather than from the moment it was first used —
   * a long run must not age its own reservation out while it is still running.
   */
  readonly createdAt: number;
  /**
   * Epoch ms of the last state change — `pendingSince` on a live claim, and the
   * column the purge horizon is measured from once the row is terminal.
   */
  readonly updatedAt: number;
  readonly binding?: StartReservationBinding;
}

export type StartReservationBinding =
  | { readonly kind: 'legacy' }
  | { readonly kind: 'unbound' }
  | { readonly kind: 'bound'; readonly execution: RunExecutionIdentity };

export interface StartReservationReading extends StartReservation {
  readonly binding: StartReservationBinding;
}

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new InvalidExecutionIdentityError('admission');
  return value as Record<string, unknown>;
}

export function captureReservation(
  value: StartReservationReading,
  expectedState: 'reserved' | 'started' | 'nonterminal',
): StartReservationReading {
  const {
    key,
    owner,
    targetKind,
    targetId,
    runId,
    threadId,
    state,
    createdAt,
    updatedAt,
    binding,
  } = record(value);
  const identity = normalizeStartIdentity({
    owner,
    target: { kind: targetKind, id: targetId, threadId },
  });
  if (
    record(binding).kind !== 'unbound' ||
    (state !== 'reserved' && state !== 'started') ||
    (expectedState !== 'nonterminal' && state !== expectedState) ||
    !isPathSafeId(key) ||
    !isPathSafeId(runId) ||
    typeof createdAt !== 'number' ||
    !Number.isFinite(createdAt) ||
    typeof updatedAt !== 'number' ||
    !Number.isFinite(updatedAt)
  )
    throw new InvalidExecutionIdentityError('admission');
  return Object.freeze({
    key,
    runId,
    owner: identity.owner,
    targetKind: identity.target.kind,
    targetId: identity.target.id,
    ...(identity.target.kind === 'agent'
      ? { threadId: identity.target.threadId }
      : {}),
    state,
    createdAt,
    updatedAt,
    binding: Object.freeze({ kind: 'unbound' as const }),
  });
}

export function sameReservationIdentity(
  actual: StartReservationReading,
  expected: StartReservationReading,
): boolean {
  return (
    actual.key === expected.key &&
    actual.runId === expected.runId &&
    actual.owner.kind === expected.owner.kind &&
    actual.owner.id === expected.owner.id &&
    actual.targetKind === expected.targetKind &&
    actual.targetId === expected.targetId &&
    actual.threadId === expected.threadId &&
    actual.createdAt === expected.createdAt
  );
}

export const START_IDEMPOTENCY_TABLE = 'flowsafe_start_idempotency';

const STATE_CHECK = START_RESERVATION_STATES.map((state) => `'${state}'`).join(
  ', ',
);
const TARGET_CHECK = START_TARGET_KINDS.map((kind) => `'${kind}'`).join(', ');
/**
 * Built from the principal vocabulary rather than hand-written, so a kind added
 * to `EXECUTION_PRINCIPAL_KINDS` cannot leave this constraint behind. The
 * failure a stale literal would cause is not a compile error and not a rejected
 * write on an existing deployment: `CREATE TABLE IF NOT EXISTS` is a no-op
 * against a table that already exists, so the drift would show up only as an
 * INSERT refused on whichever database happened to be created after the new
 * kind shipped.
 */
const OWNER_KIND_CHECK = EXECUTION_PRINCIPAL_KINDS.map(
  (kind) => `'${kind}'`,
).join(', ');

/**
 * The reservation schema.
 *
 * The CHECK constraints are load-bearing, not decoration: every compare-and-set
 * below is stated as `WHERE ... AND state = '<literal>'`, which is only a TOTAL
 * decision while the column cannot hold a fourth value. A row hand-edited into
 * an unknown state would otherwise be a reservation no CAS can advance and no
 * purge can reap — a permanently wedged key.
 */
const START_IDEMPOTENCY_BASE_COLUMNS = `
    key TEXT PRIMARY KEY,
    owner_kind TEXT NOT NULL CHECK (owner_kind IN (${OWNER_KIND_CHECK})),
    owner_id TEXT NOT NULL,
    target_kind TEXT NOT NULL CHECK (target_kind IN (${TARGET_CHECK})),
    target_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    thread_id TEXT,
    state TEXT NOT NULL CHECK (state IN (${STATE_CHECK})),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL`;
export const START_IDEMPOTENCY_ADDITIONS = [
  'start_token TEXT',
  'start_table_prefix TEXT',
  'start_workflow_id TEXT',
] as const;
export const START_IDEMPOTENCY_COLUMNS = [
  ['key', 'TEXT', 0, 1],
  ['owner_kind', 'TEXT', 1, 0],
  ['owner_id', 'TEXT', 1, 0],
  ['target_kind', 'TEXT', 1, 0],
  ['target_id', 'TEXT', 1, 0],
  ['run_id', 'TEXT', 1, 0],
  ['thread_id', 'TEXT', 0, 0],
  ['state', 'TEXT', 1, 0],
  ['created_at', 'INTEGER', 1, 0],
  ['updated_at', 'INTEGER', 1, 0],
  ['start_token', 'TEXT', 0, 0],
  ['start_table_prefix', 'TEXT', 0, 0],
  ['start_workflow_id', 'TEXT', 0, 0],
] as const;
export type StartReservationSchemaStage = 0 | 1 | 2 | 3;

export const START_IDEMPOTENCY_DDL = `CREATE TABLE IF NOT EXISTS ${START_IDEMPOTENCY_TABLE} (${START_IDEMPOTENCY_BASE_COLUMNS},
    ${START_IDEMPOTENCY_ADDITIONS.join(',\n    ')}
  )`;

/**
 * `run_id` is how the RUNTIME finds a reservation (terminal reconcile knows the
 * run, never the key) and how the purge pairs a reservation with the snapshot
 * it outlived. Without the index both degrade to a table scan on every terminal
 * run.
 */
export const START_IDEMPOTENCY_RUN_INDEX_DDL = `CREATE INDEX IF NOT EXISTS ${START_IDEMPOTENCY_TABLE}_run
    ON ${START_IDEMPOTENCY_TABLE} (run_id)`;

/** The purge's own access path: terminal rows past the key-validity horizon. */
export const START_IDEMPOTENCY_STATE_INDEX_DDL = `CREATE INDEX IF NOT EXISTS ${START_IDEMPOTENCY_TABLE}_state
    ON ${START_IDEMPOTENCY_TABLE} (state, updated_at)`;

export type StartReservationRow = Readonly<Record<string, unknown>>;

export class ReservationSchemaError extends Error {
  constructor(reason: string) {
    super(
      `${START_IDEMPOTENCY_TABLE} has an invalid reservation schema (${reason})`,
    );
    this.name = 'ReservationSchemaError';
  }
}

export function reservationResultRows(result: unknown): StartReservationRow[] {
  if (
    result === null ||
    typeof result !== 'object' ||
    ('success' in result && result.success !== true) ||
    !('results' in result)
  ) {
    throw new Error('reservation statement returned an invalid result');
  }
  const rows: unknown = result.results;
  if (!Array.isArray(rows))
    throw new Error('reservation statement returned an invalid result');
  const length = rows.length;
  if (!Number.isSafeInteger(length) || length < 0)
    throw new Error('reservation statement returned an invalid result');
  return Array.from({ length }, (_, index) => {
    if (!Object.hasOwn(rows, index))
      throw new Error('reservation statement returned an invalid row');
    const row = rows[index];
    if (row === null || typeof row !== 'object' || Array.isArray(row)) {
      throw new Error('reservation statement returned an invalid row');
    }
    return row;
  });
}

function reservationBinding(
  row: StartReservationRow,
  schemaStage: StartReservationSchemaStage,
): StartReservationBinding {
  let stage = 0;
  let missing = false;
  for (const [name] of START_IDEMPOTENCY_COLUMNS.slice(10)) {
    if (Object.hasOwn(row, name)) {
      if (missing)
        throw new ReservationSchemaError('binding prefix has a hole');
      stage += 1;
    } else missing = true;
  }
  if (stage > schemaStage)
    throw new ReservationSchemaError('schema observation precedes row binding');
  if (stage < 3) {
    for (const [name] of START_IDEMPOTENCY_COLUMNS.slice(10, 10 + stage)) {
      if (row[name] !== null)
        throw new ReservationSchemaError(
          'partial binding is not legacy defaults',
        );
    }
    return { kind: 'legacy' };
  }
  const {
    start_token: token,
    start_table_prefix: prefix,
    start_workflow_id: workflowId,
  } = row;
  if (prefix === null && workflowId === null) {
    if (token === null) return { kind: 'legacy' };
    if (token === '') return { kind: 'unbound' };
  }
  const execution = normalizeRunExecutionIdentity({
    tablePrefix: prefix,
    workflowId,
    runId: row.run_id,
    startToken: token,
  });
  if (execution.tablePrefix !== prefix)
    throw new ReservationSchemaError('binding prefix is not canonical');
  normalizeStartIdentity({
    owner: { kind: row.owner_kind, id: row.owner_id },
    target: {
      kind: row.target_kind,
      id: row.target_id,
      ...(row.thread_id === null ? {} : { threadId: row.thread_id }),
    },
  });
  return { kind: 'bound', execution };
}

function isStartReservationState(
  value: unknown,
): value is StartReservationState {
  return (
    typeof value === 'string' &&
    (START_RESERVATION_STATES as readonly string[]).includes(value)
  );
}

export function isStartTargetKind(value: unknown): value is StartTargetKind {
  return (
    typeof value === 'string' &&
    (START_TARGET_KINDS as readonly string[]).includes(value)
  );
}

function isEpochMs(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

/**
 * Project a stored row, or refuse it.
 *
 * A malformed row throws rather than reading as absent, and that direction is
 * deliberate: "there is no reservation" is the answer that STARTS A RUN, so it
 * must never be reachable from a row this build cannot parse. The CHECK
 * constraints make this unreachable on a database this package created; it
 * exists for the one that was hand-edited.
 *
 * The TIMESTAMPS are in that strict set too, rather than coerced to 0 as an
 * unparseable number once was. Neither column is decoration: `updated_at` is
 * the horizon the purge measures from, so a corrupt one on a terminal row reads
 * as epoch 0 and makes the reservation immediately reapable — which deletes a
 * spent key early and turns the next retry of it into a fresh start. It is also
 * `pendingSince` on a live claim, where 0 tells an operator a run has been
 * starting since 1970. Refusing the row keeps both faults visible as the 503
 * they are.
 */
export function reservationFromRow(
  row: StartReservationRow,
  schemaStage: StartReservationSchemaStage,
): StartReservationReading {
  for (const [name] of START_IDEMPOTENCY_COLUMNS.slice(0, 10)) {
    if (!Object.hasOwn(row, name))
      throw new ReservationSchemaError(`row is missing ${name}`);
  }
  const {
    key,
    owner_kind: ownerKind,
    owner_id: ownerId,
    target_kind: targetKind,
    target_id: targetId,
    run_id: runId,
    thread_id: threadId,
    state,
    created_at: createdAt,
    updated_at: updatedAt,
  } = row;
  if (
    typeof key !== 'string' ||
    !isExecutionPrincipalKind(ownerKind) ||
    !isExecutionPrincipalId(ownerId) ||
    !isStartTargetKind(targetKind) ||
    typeof targetId !== 'string' ||
    !isPathSafeId(runId) ||
    !isStartReservationState(state) ||
    !isEpochMs(createdAt) ||
    !isEpochMs(updatedAt)
  ) {
    throw new Error('start reservation row is malformed');
  }
  return {
    key,
    owner: { kind: ownerKind, id: ownerId },
    targetKind,
    targetId,
    runId,
    ...(isPathSafeId(threadId) ? { threadId } : {}),
    state,
    createdAt,
    updatedAt,
    binding: reservationBinding(row, schemaStage),
  };
}

export function admissionReservationFromRow(
  row: StartReservationRow,
  stage: StartReservationSchemaStage,
): StartReservationReading {
  if (stage !== 3)
    throw new ReservationSchemaError('admission requires current schema');
  const captured: Record<string, unknown> = {};
  for (const [name] of START_IDEMPOTENCY_COLUMNS) {
    if (!Object.hasOwn(row, name))
      throw new ReservationSchemaError(`row is missing ${name}`);
    captured[name] = row[name];
  }
  if (!isPathSafeId(captured.key))
    throw new ReservationSchemaError('admission key is invalid');
  if (!isPathSafeId(captured.run_id))
    throw new ReservationSchemaError('admission run id is invalid');
  if (!isPathSafeId(captured.target_id))
    throw new ReservationSchemaError('admission target id is invalid');
  if (captured.target_kind === 'workflow' && captured.thread_id !== null)
    throw new ReservationSchemaError('admission workflow thread must be null');
  if (captured.target_kind === 'agent' && !isPathSafeId(captured.thread_id))
    throw new ReservationSchemaError('admission agent thread is invalid');
  normalizeStartIdentity({
    owner: { kind: captured.owner_kind, id: captured.owner_id },
    target: {
      kind: captured.target_kind,
      id: captured.target_id,
      ...(captured.thread_id === null ? {} : { threadId: captured.thread_id }),
    },
  });
  const reservation = reservationFromRow(captured, stage);
  return Object.freeze({
    ...reservation,
    owner: Object.freeze(reservation.owner),
    binding: Object.freeze(reservation.binding),
  });
}

/** @internal Decode current-stage data before compatible thread normalization. */
export function decodeStartReservationAdmissionResult(
  result: unknown,
): StartReservationReading | undefined {
  const rows = reservationResultRows(result);
  if (rows.length > 1)
    throw new ReservationSchemaError('admission returned multiple rows');
  return rows[0] === undefined
    ? undefined
    : admissionReservationFromRow(rows[0], 3);
}

export function reservationSchemaStage(
  result: unknown,
): StartReservationSchemaStage | undefined {
  const columns = reservationResultRows(result);
  if (columns.length === 0) return undefined;
  if (columns.length < 10 || columns.length > START_IDEMPOTENCY_COLUMNS.length)
    throw new ReservationSchemaError('unexpected columns');
  for (const [index, actual] of columns.entries()) {
    const expected = START_IDEMPOTENCY_COLUMNS[index];
    if (expected === undefined)
      throw new ReservationSchemaError('unexpected columns');
    const [name, type, notnull, pk] = expected;
    if (
      actual.name !== name ||
      actual.type !== type ||
      actual.notnull !== notnull ||
      actual.pk !== pk ||
      actual.dflt_value !== null ||
      actual.hidden !== 0
    )
      throw new ReservationSchemaError(`column ${name} differs`);
  }
  return (columns.length - 10) as StartReservationSchemaStage;
}

/** @internal Validate already-observed PRAGMA data without another query. */
export function validateStartReservationAdmissionSchema(result: unknown): void {
  if (reservationSchemaStage(result) !== 3)
    throw new ReservationSchemaError('admission requires current schema');
}

export function captureBoundReservation(
  value: StartReservationReading,
): StartReservationReading {
  const {
    key,
    owner,
    targetKind,
    targetId,
    runId,
    threadId,
    state,
    createdAt,
    updatedAt,
    binding,
  } = record(value);
  const bound = record(binding);
  if (bound.kind !== 'bound')
    throw new InvalidExecutionIdentityError('admission');
  const {
    tablePrefix,
    workflowId,
    runId: physicalRunId,
    startToken,
  } = record(bound.execution);
  const execution = normalizeRunExecutionIdentity({
    tablePrefix,
    workflowId,
    runId: physicalRunId,
    startToken,
  });
  if (execution.tablePrefix !== tablePrefix || execution.runId !== runId)
    throw new InvalidExecutionIdentityError('admission');
  const identity = normalizeStartIdentity({
    owner,
    target: { kind: targetKind, id: targetId, threadId },
  });
  return admissionReservationFromRow(
    {
      key,
      owner_kind: identity.owner.kind,
      owner_id: identity.owner.id,
      target_kind: identity.target.kind,
      target_id: identity.target.id,
      run_id: runId,
      thread_id: threadId ?? null,
      state,
      created_at: createdAt,
      updated_at: updatedAt,
      start_token: execution.startToken,
      start_table_prefix: execution.tablePrefix,
      start_workflow_id: execution.workflowId,
    },
    3,
  );
}
