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
 * Total canonical intake bytes per operation, measured as the sum of per-item
 * canonical bytes. Item depth and node bounds apply per item; no aggregate
 * node bound exists. The operative per-call memory envelope also includes the
 * materialized inventory generation.
 */
export const FLEET_OPERATION_INTAKE_BYTE_BOUND = 16 * 1024 * 1024;
/** Statements per D1 batch used by the staging protocol. */
export const FLEET_OPERATION_STAGE_BATCH_STATEMENTS = 100;
/** At most 99 non-record rows per record times 10,000 records. */
export const FLEET_OPERATION_ROW_READ_BOUND =
  (FLEET_OPERATION_STAGE_BATCH_STATEMENTS - 1) * FLEET_OPERATION_ITEM_BOUND;
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

export interface FleetOperationItemsIntake {
  readonly envelope: Record<string, unknown>;
  readonly items: readonly unknown[];
  readonly itemByteBound: number;
}

export type FleetOperationIntakeRefusal =
  | { readonly reason: 'item-count' }
  | { readonly reason: 'item-structure'; readonly itemOrdinal: number }
  | { readonly reason: 'item-bytes'; readonly itemOrdinal: number }
  | { readonly reason: 'aggregate-bytes' };

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
  /**
   * Reads at most `limit` rows of the requested kind whose ordinal is strictly
   * greater than the exclusive `afterOrdinal`; an absent cursor starts at the
   * beginning. `done` means no matching rows remain beyond this page. Callers
   * do not rely on the ordering of rows within a page. A page contains the
   * smallest qualifying ordinals; omitting a row whose ordinal is below one
   * the page returns is non-conforming.
   */
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

/**
 * Reads and ordinal-sorts every contiguous-from-zero staged row of one kind.
 * Fails closed on an empty unfinished page, any row at or below the requested
 * exclusive cursor, a duplicate ordinal, or a gap. Record reads cap at
 * `FLEET_OPERATION_ITEM_BOUND`; other kinds cap at
 * `FLEET_OPERATION_ROW_READ_BOUND`.
 */
export async function readAllFleetOperationRows(
  store: FleetOperationStore,
  operationId: string,
  rowKind: FleetOperationRowKind,
): Promise<FleetOperationStagedRow[]> {
  const rows: FleetOperationStagedRow[] = [];
  const rowReadBound =
    rowKind === 'record'
      ? FLEET_OPERATION_ITEM_BOUND
      : FLEET_OPERATION_ROW_READ_BOUND;
  let afterOrdinal: number | undefined;
  for (;;) {
    const page = await store.readOperationRowsPage({
      operationId,
      rowKind,
      limit: 1_000,
      ...(afterOrdinal === undefined ? {} : { afterOrdinal }),
    });
    // A surviving row exceeds every ordinal collected from prior pages, so
    // only duplicates within this page need an explicit set.
    const pageOrdinals = new Set<number>();
    let maximumOrdinal: number | undefined;
    for (const row of page.rows) {
      if (
        (afterOrdinal !== undefined && row.ordinal <= afterOrdinal) ||
        pageOrdinals.has(row.ordinal)
      ) {
        return malformed();
      }
      pageOrdinals.add(row.ordinal);
      if (maximumOrdinal === undefined || row.ordinal > maximumOrdinal) {
        maximumOrdinal = row.ordinal;
      }
    }
    rows.push(...page.rows);
    if (rows.length > rowReadBound) return malformed();
    if (page.done) break;
    if (maximumOrdinal === undefined) return malformed();
    afterOrdinal = maximumOrdinal;
  }
  const sortedRows = [...rows].sort(
    (left, right) => left.ordinal - right.ordinal,
  );
  for (const [index, row] of sortedRows.entries()) {
    if (row.ordinal !== index) return malformed();
  }
  return sortedRows;
}

/** Constructs the public continuation token for one durable run record. */
export function fleetOperationTokenOf(
  run: FleetOperationRunRecord,
): FleetOperationToken {
  return {
    version: 1,
    operationId: run.operationId,
    revision: run.progress.revision,
  };
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

function stagedRowMaxBytes(rowKind: FleetOperationRowKind): number {
  return rowKind === 'record'
    ? FLEET_OPERATION_RECORD_ROW_BYTE_BOUND
    : FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND;
}

/** Tests a payload against the exact bounds enforced by the staged-row codec. */
export function fleetOperationStagedRowPayloadFitsEnvelope(
  rowKind: FleetOperationRowKind,
  payload: unknown,
): boolean {
  try {
    fleetOperationBoundedPlain(payload, stagedRowMaxBytes(rowKind));
    return true;
  } catch {
    // fleetOperationBoundedPlain normalizes every violation to this false path.
    return false;
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
  const rowKind = candidate.rowKind as FleetOperationRowKind;
  const payload = fleetOperationPlainRecord(
    fleetOperationBoundedPlain(candidate.payload, stagedRowMaxBytes(rowKind)),
  );
  return {
    rowKind,
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

/**
 * SHA-256 of one canonical value under the module's per-value structure bounds:
 * depth 64, 8,192 nodes, and 4 KiB string values or object keys. Multi-item
 * intakes must use `fleetOperationItemsIntake`.
 */
export function fleetOperationIntakeDigest(value: unknown): string {
  return createHash('sha256')
    .update(canonicalFleetOperationBytes(value))
    .digest('hex');
}

/**
 * Canonicalizes each item under the per-item depth and node bounds, then
 * applies byte-only per-item and aggregate limits without cloning the intake
 * as one tree. A successful result includes JSON-parsed canonical snapshots
 * so later awaits cannot observe mutation through the caller's aliases.
 */
export function fleetOperationItemsIntake(
  intake: FleetOperationItemsIntake,
):
  | { readonly digest: string; readonly items: readonly unknown[] }
  | FleetOperationIntakeRefusal {
  if (intake.items.length > FLEET_OPERATION_ITEM_BOUND) {
    return { reason: 'item-count' };
  }
  const hash = createHash('sha256').update(
    canonicalFleetOperationBytes(intake.envelope),
  );
  let aggregateBytes = 0;
  const items: unknown[] = [];
  for (const [itemOrdinal, item] of intake.items.entries()) {
    let canonical: string;
    try {
      canonical = canonicalFleetOperationBytes(item);
    } catch (error) {
      if (!(error instanceof FleetOperationStateError)) throw error;
      try {
        const serialized = JSON.stringify(item);
        if (
          typeof serialized === 'string' &&
          utf8Length(serialized) > intake.itemByteBound
        ) {
          return { reason: 'item-bytes', itemOrdinal };
        }
      } catch {
        return { reason: 'item-structure', itemOrdinal };
      }
      return { reason: 'item-structure', itemOrdinal };
    }
    const itemBytes = utf8Length(canonical);
    if (itemBytes > intake.itemByteBound) {
      return { reason: 'item-bytes', itemOrdinal };
    }
    aggregateBytes += itemBytes;
    if (aggregateBytes > FLEET_OPERATION_INTAKE_BYTE_BOUND) {
      return { reason: 'aggregate-bytes' };
    }
    hash.update(String(itemBytes)).update(':').update(canonical);
    items.push(JSON.parse(canonical) as unknown);
  }
  return { digest: hash.digest('hex'), items };
}

/** Non-throwing write gate for every durable audit finding detail. */
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
