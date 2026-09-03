// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { cloneBoundedPlainData } from './strict-plain-data.js';

/**
 * Envelope bound for one operation RUN RECORD — the `{version, operationId,
 * kind, state, progress, updatedAt, terminalAtMs?}` document
 * `fleetOperationRunRecordFromUnknown` parses. Not a staged-row bound.
 */
export const FLEET_OPERATION_RECORD_BYTE_BOUND = 96 * 1024;
export const FLEET_OPERATION_TOKEN_BYTE_BOUND = 1024;
export const FLEET_OPERATION_STRING_BYTE_BOUND = 4096;
/** Envelope bound for a `finding`, `fact`, or `item` staged-row payload. */
export const FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND = 16 * 1024;
/**
 * Envelope bound for a `record` STAGED-ROW payload — one caller-supplied
 * fleet record. It currently holds the same value as
 * `FLEET_OPERATION_RECORD_BYTE_BOUND`, but neither is derived from the other:
 * that one bounds the run-record document, this one bounds a staged row.
 * Picking the wrong one type-checks and passes every test.
 */
export const FLEET_OPERATION_RECORD_ROW_BYTE_BOUND = 96 * 1024;
/** Maximum nesting depth of one bounded plain-data value. */
export const FLEET_OPERATION_DEPTH_BOUND = 64;
/** Maximum node count of one bounded plain-data value. */
export const FLEET_OPERATION_NODE_BOUND = 8192;
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
/**
 * Per-kind cap on the non-`record` staged rows one read may return: at most
 * 99 rows per record times 10,000 records. The 99 is the per-record
 * batch ceiling the audit coordinator enforces over the `finding` and `fact`
 * rows one record emits together, the remaining statement of the batch being
 * the run record's own update. It derives no bound for a global stage's
 * findings, nor for R4-C.2's `item` rows, where this constant is a plain
 * ceiling rather than a derived bound.
 */
export const FLEET_OPERATION_ROW_READ_BOUND =
  (FLEET_OPERATION_STAGE_BATCH_STATEMENTS - 1) * FLEET_OPERATION_ITEM_BOUND;
/**
 * Rows requested per page by `readAllFleetOperationRows`. The guide's
 * documented aggregate O(records²/1,000) re-page term is this divisor, so
 * changing it changes that published cost figure.
 */
const FLEET_OPERATION_ROW_PAGE_LIMIT = 1_000;
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

/** The argument bag `fleetOperationItemsIntake` takes; not its result. */
export interface FleetOperationItemsIntakeInput {
  readonly envelope: Readonly<Record<string, unknown>>;
  readonly items: readonly unknown[];
  readonly itemByteBound: number;
}

/**
 * Why an intake was refused. `itemOrdinal` names the offending item for the
 * per-item reasons; the audit coordinator maps every reason to a fixed
 * message and does not read it, but it is carried so R4-C.2's migration
 * intake — which refuses one item out of a batch — can report which.
 */
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

/**
 * The fixed refusal message the audit coordinator raises when a persisted
 * operation carries the other operation kind. It lives here so that
 * coordinator's sites and R4-C.2's migration coordinator emit byte-identical
 * text; `D1FleetOperationStore` still carries its own copy of the literal.
 */
export function fleetOperationOtherKindMessage(operationId: string): string {
  return `fleet operation '${operationId}' belongs to the other operation kind`;
}

export interface FleetOperationStore {
  /**
   * Runs `operation` under an account-wide exclusive lease for one operation
   * kind. The lease is acquired before the callback runs and released after
   * the returned promise settles, whether it resolves or rejects. Contention
   * is refused, not queued: a caller that cannot take the lease receives an
   * error rather than waiting for the holder.
   */
  withAccountOperationLease<T>(
    kind: FleetOperationKind,
    operation: (lease: FleetOperationLease) => Promise<T>,
  ): Promise<T>;
  /**
   * Reads the persisted run record outside any lease. A missing operation is
   * reported as `undefined`, not as an error; a present but unparseable one
   * still refuses through the run-record codec.
   */
  readOperationById(
    operationId: string,
  ): Promise<FleetOperationRunRecord | undefined>;
  /**
   * Reads at most `limit` rows of the requested kind whose ordinal is strictly
   * greater than the exclusive `afterOrdinal`; an absent cursor starts at the
   * beginning. `done` means no matching rows remain beyond this page. Callers
   * do not rely on the ordering of rows within a page. A page contains the
   * smallest qualifying ordinals; omitting a row whose ordinal is below one
   * the page returns is non-conforming. An implementation must accept any
   * `limit` from 1 through 1,000, and refuses one outside the range it
   * supports rather than clamping it, so an out-of-range `limit` fails closed
   * at the store. The upper end is a hard requirement, not a preference:
   * `readAllFleetOperationRows` passes this module's
   * `FLEET_OPERATION_ROW_PAGE_LIMIT` — 1,000, and unexported, so the bound is
   * restated here as a literal — as the `limit` on every page it requests,
   * with no negotiation, so a store supporting a narrower range throws on
   * every whole-set row read.
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
  /**
   * Deletes up to `limit` prunable terminal operations of one kind and reports
   * the deletion count beside `releasedPins`, the number of pin-release CALLS
   * the pass made. One such call is issued per audit candidate whether or not
   * that operation still holds a pin, the release being a no-op when it does
   * not, so the counter reports audit candidates rather than reclaimed pins and
   * stays 0 for `kind: 'migration'`. Which terminal operations are prunable is
   * the implementation's own retention policy — it must never delete one that
   * is still an active head.
   */
  pruneFleetOperations(
    input: Readonly<{
      kind: FleetOperationKind;
      limit: number;
    }>,
  ): Promise<Readonly<{ deleted: number; releasedPins: number }>>;
}

export interface FleetOperationLease {
  /**
   * Throws unless this lease is still held, so a coordinator that is about to
   * do durable work can fail closed on a lost lease instead of racing the new
   * holder.
   */
  assertOwned(): Promise<void>;
  /**
   * Creates the operation, or adopts an existing one of the same id. The
   * outcome is `created` for a new operation, `adopted-running` when an
   * operation with a matching `intakeDigest` is already RUNNING, and
   * `adopted-terminal` when it has already finished; `record` is the
   * authoritative run record in every case. A different `intakeDigest` for
   * the same id is a conflict the implementation refuses.
   */
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
  /**
   * The read available while holding the lease; `undefined` when no such
   * operation exists. Implementations may serve it from the same unleased row
   * read as `readOperationById`.
   */
  readOperation(
    operationId: string,
  ): Promise<FleetOperationRunRecord | undefined>;
  /**
   * Appends staged rows without advancing the revision, refusing unless the
   * persisted revision still equals `expectedRevision`.
   *
   * WRITER OBLIGATION: rows must be supplied in ascending ordinal order
   * within each row kind, and the implementation must persist them in array
   * order. `readAllFleetOperationRows`'s contiguity assertion is correct only
   * because both halves hold — a batch persisted out of order can leave a gap
   * visible to a reader that pages mid-write.
   */
  stageRows(
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      rows: readonly FleetOperationStagedRow[];
    }>,
  ): Promise<void>;
  /**
   * Compare-and-set advance of one operation: refuses unless the persisted
   * revision equals `expectedRevision`, then writes `runRecord`, appends
   * `rows`, and replaces the payloads of `updateRows` — `item` rows only — in
   * the same transaction. For each named kind `expectedRowWatermarks` asserts
   * that the first N ordinals are all present after the write — a dense-prefix
   * check rather than a total count — so a partially applied batch is refused
   * while a retry that already staged rows at higher ordinals still commits;
   * a later call's higher watermark, or `finalizeOperation`'s totals, close
   * those surplus ordinals out. Returns the persisted record, which is the
   * only authoritative post-commit state.
   */
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
  /**
   * The same CAS as `commitProgress`, moving the operation to FINALIZED and
   * stamping its terminal time. `expectedRowCounts` asserts the FINAL row
   * count per kind, so a run that lost or double-wrote rows cannot finalize.
   * `requireAllItemsComplete` additionally demands that the number of `item`
   * rows in a complete state equals the progress item count — R4-C.2's
   * per-item migration contract, unused by the audit coordinator. Returns the
   * persisted terminal record.
   */
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
  /**
   * The same CAS, moving the operation to FAILED. Staged rows are kept so a
   * failed operation stays readable; `updateRows` replaces individual `item`
   * row payloads in the same transaction.
   */
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

/** UTF-8 byte length of one string, over a module-level encoder. */
export function utf8Length(value: string): number {
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
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: FLEET_OPERATION_DEPTH_BOUND,
      maxNodes: FLEET_OPERATION_NODE_BOUND,
      maxScalarBytes: maxBytes,
      maxSerializedBytes: maxBytes,
      error: () => new FleetOperationStateError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    return malformed();
  }
  // The string and key walk runs outside the try because it operates on the
  // already-cloned plain tree — no getters, no cycles — and because its own
  // `malformed()` calls would otherwise throw into a catch that only calls
  // `malformed()` again.
  const pending = [plain];
  while (pending.length > 0) {
    const current = pending.pop();
    if (
      typeof current === 'string' &&
      utf8Length(current) > FLEET_OPERATION_STRING_BYTE_BOUND
    ) {
      return malformed();
    }
    // The spread is bounded: `cloneBoundedPlainData` admits no array longer
    // than `FLEET_OPERATION_NODE_BOUND`, so it stays far below the engine's
    // argument limit and cannot raise the `RangeError` that would escape this
    // walk unconverted.
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
}

function stagedRowMaxBytes(rowKind: FleetOperationRowKind): number {
  return rowKind === 'record'
    ? FLEET_OPERATION_RECORD_ROW_BYTE_BOUND
    : FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND;
}

/**
 * Tests a payload against the exact predicates the staged-row codec enforces:
 * the bounded-plain walk AND the plain-record check, so a bounded plain array
 * cannot pass this preflight and then fail `fleetOperationStagedRowFromUnknown`.
 */
export function fleetOperationStagedRowPayloadFitsEnvelope(
  rowKind: FleetOperationRowKind,
  payload: unknown,
): boolean {
  try {
    fleetOperationPlainRecord(
      fleetOperationBoundedPlain(payload, stagedRowMaxBytes(rowKind)),
    );
    return true;
  } catch {
    // Both helpers normalize every violation to this false path.
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

/**
 * Classifies an already-parsed token against the persisted run record. The
 * caller parses untrusted input with `parseFleetOperationToken` first; this
 * function trusts its typed parameter and never re-parses it.
 */
export function classifyFleetOperationToken(
  token: FleetOperationToken,
  run: FleetOperationRunRecord | undefined,
  expectedKind: FleetOperationKind,
): 'current' | 'stale' {
  if (!run || run.operationId !== token.operationId) {
    throw new FleetOperationTokenOperationError(token.operationId);
  }
  if (run.kind !== expectedKind) throw new FleetOperationTokenKindError();
  if (token.revision > run.progress.revision) {
    throw new FleetOperationTokenFutureError();
  }
  return token.revision === run.progress.revision ? 'current' : 'stale';
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
  intake: FleetOperationItemsIntakeInput,
):
  | { readonly digest: string; readonly items: readonly unknown[] }
  | FleetOperationIntakeRefusal {
  if (intake.items.length > FLEET_OPERATION_ITEM_BOUND) {
    return { reason: 'item-count' };
  }
  // The envelope is hashed without the `String(bytes) + ':'` frame every item
  // carries, and that is deliberate. `envelope` is typed
  // `Readonly<Record<string, unknown>>`, so its canonical text is always a
  // JSON OBJECT, and no JSON object text is a proper prefix of another —
  // the closing brace of one cannot fall inside another. The leading
  // envelope is therefore already unambiguous ahead of the netstring-framed
  // items. (The unqualified claim "canonical JSON is self-delimiting" would
  // be false: JSON numbers are not prefix-free.)
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
      // First-true-predicate classification, not actual-cause: an item that
      // trips several bounds is reported as `item-bytes` when a plain
      // re-serialization is over the per-item bound and as `item-structure`
      // otherwise. That re-serialization walks the RAW item a second time, so
      // a caller getter runs twice on this refusal path.
      try {
        const serialized = JSON.stringify(item);
        if (
          typeof serialized === 'string' &&
          utf8Length(serialized) > intake.itemByteBound
        ) {
          return { reason: 'item-bytes', itemOrdinal };
        }
      } catch {
        // A throwing or circular item is a structure refusal, exactly like a
        // serializable one that is not over the byte bound.
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

// ---------------------------------------------------------------------------
// Store-facing helpers. These sit below the pure codec primitives, and the
// section above must stay free of store IO.
// ---------------------------------------------------------------------------

/**
 * Reads and ordinal-sorts every contiguous-from-zero staged row of one kind.
 * Fails closed on a page larger than the requested limit, an empty unfinished
 * page, any row at or below the requested exclusive cursor, a duplicate
 * ordinal, or a gap. Record reads cap at `FLEET_OPERATION_ITEM_BOUND`; other
 * kinds cap at `FLEET_OPERATION_ROW_READ_BOUND`.
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
      limit: FLEET_OPERATION_ROW_PAGE_LIMIT,
      ...(afterOrdinal === undefined ? {} : { afterOrdinal }),
    });
    // The port promises at most `limit` rows. An over-sized page is durable
    // non-conformance in its own right, not something the row cap absorbs.
    if (page.rows.length > FLEET_OPERATION_ROW_PAGE_LIMIT) return malformed();
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
    for (const row of page.rows) rows.push(row);
    // The cap is checked after the append, so the accumulator can reach
    // `rowReadBound + FLEET_OPERATION_ROW_PAGE_LIMIT` before refusing. The
    // overshoot is one page and it keeps the refusal on whole-page boundaries.
    if (rows.length > rowReadBound) return malformed();
    if (page.done) break;
    if (maximumOrdinal === undefined) return malformed();
    afterOrdinal = maximumOrdinal;
  }
  // Sorted in place: this array was built here and is never aliased.
  rows.sort((left, right) => left.ordinal - right.ordinal);
  for (const [index, row] of rows.entries()) {
    if (row.ordinal !== index) return malformed();
  }
  return rows;
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
