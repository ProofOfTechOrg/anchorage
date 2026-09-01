// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { cloneBoundedPlainData } from './strict-plain-data.js';

export const FLEET_OPERATION_RECORD_BYTE_BOUND = 96 * 1024;
export const FLEET_OPERATION_TOKEN_BYTE_BOUND = 1024;
export const FLEET_OPERATION_STRING_BYTE_BOUND = 4096;
export const FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND = 16 * 1024;
export const FLEET_OPERATION_RECORD_ROW_BYTE_BOUND = 96 * 1024;
const DEPTH_BOUND = 64;
const NODE_BOUND = 8192;
export const FLEET_OPERATION_ITEM_BOUND = 10_000;
/**
 * Total serialized intake bytes per operation. The operative per-call memory
 * envelope also includes the materialized inventory generation.
 */
export const FLEET_OPERATION_INTAKE_BYTE_BOUND = 16 * 1024 * 1024;
/** Statements per D1 batch used by the staging protocol. */
export const FLEET_OPERATION_STAGE_BATCH_STATEMENTS = 100;
/** Frozen plan length cap (fixed steps plus pending D1 versions). */
export const FLEET_MIGRATION_PLAN_BOUND = 64;

const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const CREDENTIAL_SUBSTRINGS = Object.freeze([
  'authorization',
  'bearer',
  'x-auth',
  'api_token',
]);
const STRUCTURED_CLONE = structuredClone;

export const FLEET_OPERATION_KINDS = Object.freeze([
  'audit',
  'migration',
] as const);
export type FleetOperationKind = (typeof FLEET_OPERATION_KINDS)[number];
/** Every staged row kind. */
export const FLEET_OPERATION_ROW_KINDS = Object.freeze([
  'record',
  'finding',
  'item',
  'fact',
] as const);
export type FleetOperationRowKind = (typeof FLEET_OPERATION_ROW_KINDS)[number];

export interface FleetOperationToken {
  readonly version: 1;
  readonly operationId: string;
  readonly revision: number;
}

export interface FleetOperationProgress {
  readonly kind: FleetOperationKind;
  readonly revision: number;
  readonly failure?: FleetOperationFailure;
}

export interface FleetOperationFailure {
  readonly reason:
    | 'item-failed'
    | 'target-drift'
    | 'emission-bound-exceeded'
    | 'generation-unavailable'
    | 'operator-abandoned';
  readonly itemOrdinal?: number;
}

export interface FleetOperationRunRecord {
  readonly version: 1;
  readonly operationId: string;
  readonly kind: FleetOperationKind;
  readonly state: 'running' | 'finalized' | 'failed';
  readonly progress: FleetOperationProgress;
  readonly updatedAt: string;
  readonly terminalAtMs?: number;
}

export interface FleetOperationStagedRow {
  readonly rowKind: FleetOperationRowKind;
  readonly ordinal: number;
  readonly payload: Readonly<Record<string, unknown>>;
}

export class FleetOperationStateError extends Error {
  constructor() {
    super('fleet operation state is malformed');
    this.name = 'FleetOperationStateError';
  }
}

export class FleetOperationTokenError extends Error {
  constructor() {
    super('fleet operation token is malformed');
    this.name = 'FleetOperationTokenError';
  }
}

export class FleetOperationTokenOperationError extends Error {
  constructor(readonly operationId: string) {
    super(`no fleet operation '${operationId}'`);
    this.name = 'FleetOperationTokenOperationError';
  }
}

export class FleetOperationTokenKindError extends Error {
  constructor() {
    super('fleet operation token targets another operation kind');
    this.name = 'FleetOperationTokenKindError';
  }
}

export class FleetOperationTokenFutureError extends Error {
  constructor() {
    super('fleet operation token is ahead of the persisted operation');
    this.name = 'FleetOperationTokenFutureError';
  }
}

export class FleetOperationStoreCapabilityError extends Error {
  constructor() {
    super(
      'fleet operation store requires an inventory store to release audit pins',
    );
    this.name = 'FleetOperationStoreCapabilityError';
  }
}

export interface FleetOperationStore {
  withAccountOperationLease<T>(
    kind: FleetOperationKind,
    operation: (lease: FleetOperationLease) => Promise<T>,
  ): Promise<T>;
  readOperationById(
    operationId: string,
  ): Promise<FleetOperationRunRecord | undefined>;
  readOperationRowsPage(
    input: Readonly<{
      operationId: string;
      rowKind: FleetOperationRowKind;
      afterOrdinal?: number;
      limit: number;
    }>,
  ): Promise<
    Readonly<{
      rows: readonly FleetOperationStagedRow[];
      done: boolean;
    }>
  >;
  pruneFleetOperations(
    input: Readonly<{
      kind: FleetOperationKind;
      limit: number;
    }>,
  ): Promise<Readonly<{ deleted: number; releasedPins: number }>>;
}

export interface FleetOperationLease {
  assertOwned(): Promise<void>;
  startOperation(
    input: Readonly<{
      operationId: string;
      kind: FleetOperationKind;
      runRecord: FleetOperationRunRecord;
      intakeDigest: string;
    }>,
  ): Promise<
    Readonly<{
      outcome: 'created' | 'adopted-running' | 'adopted-terminal';
      record: FleetOperationRunRecord;
    }>
  >;
  readOperation(
    operationId: string,
  ): Promise<FleetOperationRunRecord | undefined>;
  stageRows(
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      rows: readonly FleetOperationStagedRow[];
    }>,
  ): Promise<void>;
  commitProgress(
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      runRecord: FleetOperationRunRecord;
      rows?: readonly FleetOperationStagedRow[];
      updateRows?: readonly FleetOperationStagedRow[];
      expectedRowWatermarks?: Readonly<
        Partial<Record<FleetOperationRowKind, number>>
      >;
    }>,
  ): Promise<FleetOperationRunRecord>;
  finalizeOperation(
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      runRecord: FleetOperationRunRecord;
      expectedRowCounts: Readonly<
        Partial<Record<FleetOperationRowKind, number>>
      >;
      requireAllItemsComplete?: boolean;
    }>,
  ): Promise<FleetOperationRunRecord>;
  failOperation(
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      runRecord: FleetOperationRunRecord;
      updateRows?: readonly FleetOperationStagedRow[];
    }>,
  ): Promise<void>;
}

/** Rejects malformed durable operation state with FleetOperationStateError. */
export function malformed(): never {
  throw new FleetOperationStateError();
}

const TEXT_ENCODER = new TextEncoder();

function utf8Length(value: string): number {
  return TEXT_ENCODER.encode(value).byteLength;
}

export function fleetOperationTextHasControlBytes(value: string): boolean {
  for (const character of value) {
    const codePoint = character.codePointAt(0);
    if (
      codePoint !== undefined &&
      (codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f))
    ) {
      return true;
    }
  }
  return false;
}

export function fleetOperationSafeInteger(
  value: unknown,
  minimum = 0,
): value is number {
  return Number.isSafeInteger(value) && Number(value) >= minimum;
}

export function fleetOperationPlainRecord(
  value: unknown,
): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return malformed();
  }
  return value as Record<string, unknown>;
}

export function assertFleetOperationExactKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[] = [],
): void {
  const allowed = new Set([...required, ...optional]);
  if (
    !required.every((key) => Object.hasOwn(value, key)) ||
    Object.keys(value).some((key) => !allowed.has(key))
  ) {
    malformed();
  }
}

export function fleetOperationBoundedString(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    utf8Length(value) <= FLEET_OPERATION_STRING_BYTE_BOUND
  );
}

export function fleetOperationSha256(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{64}$/u.test(value);
}

function canonicalIso(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    Number.isFinite(Date.parse(value)) &&
    new Date(value).toISOString() === value
  );
}

function fleetOperationBoundedPlain(value: unknown, maxBytes: number): unknown {
  try {
    const plain = cloneBoundedPlainData(value, {
      maxDepth: DEPTH_BOUND,
      maxNodes: NODE_BOUND,
      maxScalarBytes: maxBytes,
      maxSerializedBytes: maxBytes,
      error: () => new FleetOperationStateError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
    const pending = [plain];
    while (pending.length > 0) {
      const current = pending.pop();
      if (
        typeof current === 'string' &&
        utf8Length(current) > FLEET_OPERATION_STRING_BYTE_BOUND
      ) {
        return malformed();
      }
      if (Array.isArray(current)) pending.push(...current);
      else if (current && typeof current === 'object') {
        for (const [key, entry] of Object.entries(current)) {
          if (utf8Length(key) > FLEET_OPERATION_STRING_BYTE_BOUND) {
            return malformed();
          }
          pending.push(entry);
        }
      }
    }
    return plain;
  } catch {
    return malformed();
  }
}

export function fleetOperationFailureFromUnknown(
  value: unknown,
): FleetOperationFailure {
  const candidate = fleetOperationPlainRecord(value);
  assertFleetOperationExactKeys(candidate, ['reason'], ['itemOrdinal']);
  if (
    candidate.reason !== 'item-failed' &&
    candidate.reason !== 'target-drift' &&
    candidate.reason !== 'emission-bound-exceeded' &&
    candidate.reason !== 'generation-unavailable' &&
    candidate.reason !== 'operator-abandoned'
  ) {
    return malformed();
  }
  if (
    candidate.itemOrdinal !== undefined &&
    (!fleetOperationSafeInteger(candidate.itemOrdinal) ||
      candidate.itemOrdinal >= FLEET_OPERATION_ITEM_BOUND)
  ) {
    return malformed();
  }
  return {
    reason: candidate.reason,
    ...(candidate.itemOrdinal === undefined
      ? {}
      : { itemOrdinal: candidate.itemOrdinal }),
  };
}

function progressEnvelopeFromUnknown(value: unknown): FleetOperationProgress {
  const candidate = fleetOperationPlainRecord(value);
  if (
    !FLEET_OPERATION_KINDS.includes(candidate.kind as FleetOperationKind) ||
    !fleetOperationSafeInteger(candidate.revision)
  ) {
    return malformed();
  }
  return {
    ...candidate,
    kind: candidate.kind as FleetOperationKind,
    revision: candidate.revision,
    ...(candidate.failure === undefined
      ? {}
      : { failure: fleetOperationFailureFromUnknown(candidate.failure) }),
  } as FleetOperationProgress;
}

/** Strict operation envelope codec; kind-specific fields remain bounded data. */
export function fleetOperationRunRecordFromUnknown(
  value: unknown,
): FleetOperationRunRecord {
  const candidate = fleetOperationPlainRecord(
    fleetOperationBoundedPlain(value, FLEET_OPERATION_RECORD_BYTE_BOUND),
  );
  assertFleetOperationExactKeys(
    candidate,
    ['version', 'operationId', 'kind', 'state', 'progress', 'updatedAt'],
    ['terminalAtMs'],
  );
  if (
    candidate.version !== 1 ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    !FLEET_OPERATION_KINDS.includes(candidate.kind as FleetOperationKind) ||
    (candidate.state !== 'running' &&
      candidate.state !== 'finalized' &&
      candidate.state !== 'failed') ||
    !canonicalIso(candidate.updatedAt) ||
    (candidate.terminalAtMs !== undefined &&
      !fleetOperationSafeInteger(candidate.terminalAtMs))
  ) {
    return malformed();
  }
  const progress = progressEnvelopeFromUnknown(candidate.progress);
  if (progress.kind !== candidate.kind) return malformed();
  return {
    version: 1,
    operationId: candidate.operationId,
    kind: candidate.kind as FleetOperationKind,
    state: candidate.state,
    progress,
    updatedAt: candidate.updatedAt,
    ...(candidate.terminalAtMs === undefined
      ? {}
      : { terminalAtMs: candidate.terminalAtMs }),
  };
}

/** Strict staged-row envelope codec with the larger record-row allowance. */
export function fleetOperationStagedRowFromUnknown(
  value: unknown,
): FleetOperationStagedRow {
  const candidate = fleetOperationPlainRecord(value);
  assertFleetOperationExactKeys(candidate, ['rowKind', 'ordinal', 'payload']);
  if (
    typeof candidate.rowKind !== 'string' ||
    !FLEET_OPERATION_ROW_KINDS.includes(
      candidate.rowKind as FleetOperationRowKind,
    ) ||
    !fleetOperationSafeInteger(candidate.ordinal)
  ) {
    return malformed();
  }
  const maxBytes =
    candidate.rowKind === 'record'
      ? FLEET_OPERATION_RECORD_ROW_BYTE_BOUND
      : FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND;
  const payload = fleetOperationPlainRecord(
    fleetOperationBoundedPlain(candidate.payload, maxBytes),
  );
  return {
    rowKind: candidate.rowKind as FleetOperationRowKind,
    ordinal: candidate.ordinal,
    payload: { ...payload },
  };
}

/** Strict token codec; the token carries no account, kind, intake, or cursor. */
export function parseFleetOperationToken(value: unknown): FleetOperationToken {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: 4,
      maxNodes: 16,
      maxScalarBytes: FLEET_OPERATION_TOKEN_BYTE_BOUND,
      maxSerializedBytes: FLEET_OPERATION_TOKEN_BYTE_BOUND,
      error: () => new FleetOperationTokenError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    throw new FleetOperationTokenError();
  }
  try {
    const candidate = fleetOperationPlainRecord(plain);
    assertFleetOperationExactKeys(candidate, [
      'version',
      'operationId',
      'revision',
    ]);
    if (
      candidate.version !== 1 ||
      typeof candidate.operationId !== 'string' ||
      !UUID_V4.test(candidate.operationId) ||
      !fleetOperationSafeInteger(candidate.revision)
    ) {
      throw new FleetOperationTokenError();
    }
    return {
      version: 1,
      operationId: candidate.operationId,
      revision: candidate.revision,
    };
  } catch {
    throw new FleetOperationTokenError();
  }
}

/** Validates caller-chosen operation identity before any store mutation. */
export function assertFleetOperationId(value: unknown): void {
  if (typeof value !== 'string' || !UUID_V4.test(value)) {
    throw new FleetOperationStateError();
  }
}

export function classifyFleetOperationToken(
  token: FleetOperationToken,
  run: FleetOperationRunRecord | undefined,
  expectedKind: FleetOperationKind,
): 'current' | 'stale' {
  const parsed = parseFleetOperationToken(token);
  if (!run || run.operationId !== parsed.operationId) {
    throw new FleetOperationTokenOperationError(parsed.operationId);
  }
  if (run.kind !== expectedKind) throw new FleetOperationTokenKindError();
  if (parsed.revision > run.progress.revision) {
    throw new FleetOperationTokenFutureError();
  }
  return parsed.revision === run.progress.revision ? 'current' : 'stale';
}

function canonicalValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => (left < right ? -1 : 1))
        .map(([key, entry]) => [key, canonicalValue(entry)]),
    );
  }
  return value;
}

/** Recursive sorted-key JSON used for intake digests and record rows. */
export function canonicalFleetOperationBytes(value: unknown): string {
  const plain = fleetOperationBoundedPlain(
    value,
    FLEET_OPERATION_INTAKE_BYTE_BOUND,
  );
  return JSON.stringify(canonicalValue(plain));
}

/** SHA-256 of canonical intake, refusing an operation above 16 MiB. */
export function fleetOperationIntakeDigest(value: unknown): string {
  return createHash('sha256')
    .update(canonicalFleetOperationBytes(value))
    .digest('hex');
}

/** Non-throwing write gate for composed audit finding details. */
export function isDurableAuditDetailSafe(value: unknown): boolean {
  if (
    typeof value !== 'string' ||
    utf8Length(value) > FLEET_OPERATION_STRING_BYTE_BOUND ||
    fleetOperationTextHasControlBytes(value)
  ) {
    return false;
  }
  const lowered = value.toLowerCase();
  return !CREDENTIAL_SUBSTRINGS.some((marker) => lowered.includes(marker));
}
