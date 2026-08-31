// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { cloneBoundedPlainData } from './strict-plain-data.js';

/** Byte bound for one persisted inventory run record. */
export const FLEET_INVENTORY_RUN_RECORD_BYTE_BOUND = 96 * 1024;
/** Byte bound for one bounded inventory continuation token. */
export const FLEET_INVENTORY_TOKEN_BYTE_BOUND = 1024;
/** Byte bound for any single string inside persisted inventory state. */
export const FLEET_INVENTORY_STRING_BYTE_BOUND = 4096;
/** Byte bound for one staged row or deployment fact payload. */
export const FLEET_INVENTORY_STAGED_PAYLOAD_BYTE_BOUND = 16 * 1024;
/**
 * Byte bound for every durable inventory string, matching Cloudflare's own KV
 * key-name limit and staying well under the string bound.
 */
export const FLEET_INVENTORY_DURABLE_TEXT_BYTE_BOUND = 512;

const DEPTH_BOUND = 64;
const NODE_BOUND = 8192;
const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const SHA256 = /^[0-9a-f]{64}$/u;
const STRUCTURED_CLONE = structuredClone;
const CREDENTIAL_SUBSTRINGS: readonly string[] = Object.freeze([
  'authorization',
  'bearer',
  'x-auth',
  'api_token',
]);
const PRINTABLE_NON_WHITESPACE = /^[^\s\p{C}]+$/u;
const BASE64_SHAPE = /^[A-Za-z0-9+/_-]+={0,2}$/u;
const BASE64_SHAPE_MIN_LENGTH = 32;

/** Options accepted by `collectFleetInventory`, named for the bounded API. */
export interface CollectFleetInventoryOptions {
  readonly hostRoutingKvId?: string;
  readonly databaseNamePrefix: string;
  readonly scriptNamePrefix: string;
  readonly includeDispatchNamespace?: boolean;
  readonly includeR2Buckets?: boolean;
}

/** Canonical inventory options after defaults resolve at run start. */
export interface FleetInventoryRunOptions {
  readonly hostRoutingKvId?: string;
  readonly databaseNamePrefix: string;
  readonly scriptNamePrefix: string;
  readonly includeDispatchNamespace: boolean;
  readonly includeR2Buckets: boolean;
}

/** Bounded continuation token; it carries no account, options, or cursor. */
export interface FleetInventoryRunToken {
  readonly version: 1;
  readonly operationId: string;
  readonly revision: number;
}

/**
 * One bounded stage of an account inventory run, in provider encounter order.
 * Ordinals address items inside a stage; cursors are provider resumption text.
 */
export type FleetInventoryStage =
  | Readonly<{ step: 'host-kv-keys'; cursor?: string }>
  | Readonly<{ step: 'host-kv-values'; keyOrdinal: number }>
  | Readonly<{ step: 'dispatch-pages'; cursor?: string; pageOrdinal: number }>
  | Readonly<{ step: 'registration-checks'; registrationOrdinal: number }>
  | Readonly<{ step: 'registration-postprocess' }>
  | Readonly<{ step: 'custom-domains' }>
  | Readonly<{ step: 'zone-authority' }>
  | Readonly<{ step: 'zone-routes'; zoneOrdinal: number }>
  | Readonly<{ step: 'ordinary-scripts'; cursor?: string }>
  | Readonly<{ step: 'ordinary-script-detail'; scriptOrdinal: number }>
  | Readonly<{ step: 'route-claims' }>
  | Readonly<{ step: 'd1-databases' }>
  | Readonly<{ step: 'do-namespaces' }>
  | Readonly<{
      step: 'r2-buckets';
      jurisdictionOrdinal: 0 | 1 | 2;
      startAfter?: string;
    }>
  | Readonly<{ step: 'finalize' }>;

/** The step discriminant of {@link FleetInventoryStage}. */
export type FleetInventoryStageStep = FleetInventoryStage['step'];

export type FleetInventoryRowKind =
  | 'registration'
  | 'deployment'
  | 'finding'
  | 'database-id'
  | 'namespace-id'
  | 'r2-bucket'
  | 'route'
  | 'dispatch-script'
  | 'meta';

export type FleetInventoryDeploymentFactKind =
  | 'database-id'
  | 'durable-object-binding'
  | 'service-binding'
  | 'queue-producer-binding'
  | 'kv-binding'
  | 'r2-binding'
  | 'secret-name'
  | 'plain-text-binding'
  | 'route-hostname'
  | 'zone-route';

/** Every staged row kind, in manifest order. */
export const FLEET_INVENTORY_ROW_KINDS: readonly FleetInventoryRowKind[] =
  Object.freeze([
    'registration',
    'deployment',
    'finding',
    'database-id',
    'namespace-id',
    'r2-bucket',
    'route',
    'dispatch-script',
    'meta',
  ]);

/** Every deployment fact kind. */
export const FLEET_INVENTORY_DEPLOYMENT_FACT_KINDS: readonly FleetInventoryDeploymentFactKind[] =
  Object.freeze([
    'database-id',
    'durable-object-binding',
    'service-binding',
    'queue-producer-binding',
    'kv-binding',
    'r2-binding',
    'secret-name',
    'plain-text-binding',
    'route-hostname',
    'zone-route',
  ]);

/** Every inventory stage step, in provider encounter order. */
export const FLEET_INVENTORY_STAGE_ORDER: readonly FleetInventoryStageStep[] =
  Object.freeze([
    'host-kv-keys',
    'host-kv-values',
    'dispatch-pages',
    'registration-checks',
    'registration-postprocess',
    'custom-domains',
    'zone-authority',
    'zone-routes',
    'ordinary-scripts',
    'ordinary-script-detail',
    'route-claims',
    'd1-databases',
    'do-namespaces',
    'r2-buckets',
    'finalize',
  ]);

interface StageShape {
  readonly ordinal?: string;
  readonly maxOrdinal?: number;
  readonly text?: 'cursor' | 'startAfter';
}

const STAGE_SHAPES: Readonly<Record<FleetInventoryStageStep, StageShape>> =
  Object.freeze({
    'host-kv-keys': { text: 'cursor' },
    'host-kv-values': { ordinal: 'keyOrdinal' },
    'dispatch-pages': { ordinal: 'pageOrdinal', text: 'cursor' },
    'registration-checks': { ordinal: 'registrationOrdinal' },
    'registration-postprocess': {},
    'custom-domains': {},
    'zone-authority': {},
    'zone-routes': { ordinal: 'zoneOrdinal' },
    'ordinary-scripts': { text: 'cursor' },
    'ordinary-script-detail': { ordinal: 'scriptOrdinal' },
    'route-claims': {},
    'd1-databases': {},
    'do-namespaces': {},
    'r2-buckets': {
      ordinal: 'jurisdictionOrdinal',
      maxOrdinal: 2,
      text: 'startAfter',
    },
    finalize: {},
  });

export interface FleetInventoryRunProgress {
  readonly stage: FleetInventoryStage;
  readonly generation: number;
  readonly revision: number;
  readonly stagedCounts: Readonly<Record<FleetInventoryRowKind, number>>;
  readonly factCount: number;
  readonly lastPageDigest?: string;
  readonly providerRequests: number;
}

export interface FleetInventoryRunRecord {
  readonly version: 1;
  readonly operationId: string;
  readonly optionsDigest: string;
  readonly options: FleetInventoryRunOptions;
  readonly state: 'staging' | 'finalized' | 'failed';
  readonly progress: FleetInventoryRunProgress;
  readonly updatedAt: string;
}

export interface FleetInventoryGenerationRef {
  readonly generation: number;
  readonly operationId: string;
  readonly finalizedAtMs: number;
  readonly rowManifest: Readonly<Record<FleetInventoryRowKind, number>>;
  readonly factCount: number;
}

export interface FleetInventoryStagedRow {
  readonly kind: FleetInventoryRowKind;
  readonly ordinal: number;
  readonly payload: Readonly<Record<string, unknown>>;
}

export interface FleetInventoryStagedFact {
  readonly deploymentOrdinal: number;
  readonly factKind: FleetInventoryDeploymentFactKind;
  readonly factOrdinal: number;
  readonly payload: Readonly<Record<string, unknown>>;
}

/** One bounded provider stage chunk request. */
export interface FleetInventoryStageInput {
  readonly stage: FleetInventoryStage;
  readonly options: FleetInventoryRunOptions;
  readonly progress: FleetInventoryRunProgress;
  readonly maxProviderRequests: number;
  readonly signal?: AbortSignal;
}

/** One bounded provider stage chunk result; it contains no D1 knowledge. */
export interface FleetInventoryStageResult {
  readonly rows: readonly FleetInventoryStagedRow[];
  readonly facts: readonly FleetInventoryStagedFact[];
  readonly nextStage: FleetInventoryStage;
  readonly pageDigest?: string;
  readonly providerRequests: number;
  /** Call-local sanitized provider diagnostics; NEVER persisted. */
  readonly diagnostics: readonly string[];
}

/** Injected provider port; the only provider seam the coordinator sees. */
export interface FleetInventoryProviderContext {
  advanceStage(
    input: FleetInventoryStageInput,
  ): Promise<FleetInventoryStageResult>;
}

export type FleetInventoryFailureReason =
  | 'cursor-drift'
  | 'provider-bound-exceeded'
  | 'operator-abandoned';

/** Every inventory run failure reason. */
export const FLEET_INVENTORY_FAILURE_REASONS: readonly FleetInventoryFailureReason[] =
  Object.freeze([
    'cursor-drift',
    'provider-bound-exceeded',
    'operator-abandoned',
  ]);

/** Materialization source for one finalized generation. */
export interface FleetInventoryGeneration {
  readonly ref: FleetInventoryGenerationRef;
  readonly rows: readonly FleetInventoryStagedRow[];
  readonly facts: readonly FleetInventoryStagedFact[];
}

/**
 * Lease-scoped inventory mutations. Every member serializes under the one
 * account lease that produced it, including pin, release, and prune.
 */
export interface FleetInventoryLease {
  assertOwned(): Promise<void>;
  startRun(
    input: Readonly<{
      operationId: string;
      options: FleetInventoryRunOptions;
      optionsDigest: string;
    }>,
  ): Promise<FleetInventoryRunRecord>;
  readRun(operationId: string): Promise<FleetInventoryRunRecord | undefined>;
  commitChunk(
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      runRecord: FleetInventoryRunRecord;
      rows: readonly FleetInventoryStagedRow[];
      facts: readonly FleetInventoryStagedFact[];
    }>,
  ): Promise<FleetInventoryRunRecord>;
  finalizeRun(
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      manifest: Readonly<Record<FleetInventoryRowKind, number>>;
      factCount: number;
    }>,
  ): Promise<FleetInventoryGenerationRef>;
  failRun(
    input: Readonly<{
      operationId: string;
      expectedRevision: number;
      reason: FleetInventoryFailureReason;
    }>,
  ): Promise<void>;
  pinGeneration(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void>;
  releasePin(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void>;
  pruneInventoryGenerations(
    input: Readonly<{ limit: number }>,
  ): Promise<Readonly<{ deleted: number }>>;
}

/** Durable run-store port consumed by the bounded inventory coordinator. */
export interface FleetInventoryRunStore {
  withAccountInventoryLease<T>(
    operation: (lease: FleetInventoryLease) => Promise<T>,
  ): Promise<T>;
  readFinalizedGeneration(
    generation: number,
  ): Promise<FleetInventoryGeneration>;
  latestFinalizedGeneration(): Promise<FleetInventoryGenerationRef | undefined>;
  readRunByOperation(
    operationId: string,
  ): Promise<FleetInventoryRunRecord | undefined>;
  pinGeneration(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void>;
  releasePin(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void>;
  pruneInventoryGenerations(
    input: Readonly<{ limit: number }>,
  ): Promise<Readonly<{ deleted: number }>>;
}

/** Fixed refusal shared by both durable-text controls. */
export class InventoryFindingValueError extends Error {
  constructor(readonly field: string) {
    super(`inventory finding value for '${field}' is not durable-safe`);
    this.name = 'InventoryFindingValueError';
  }
}

export class FleetInventoryStateError extends Error {
  constructor() {
    super('fleet inventory state is malformed');
    this.name = 'FleetInventoryStateError';
  }
}

export class FleetInventoryRunTokenError extends Error {
  constructor() {
    super('fleet inventory run token is malformed');
    this.name = 'FleetInventoryRunTokenError';
  }
}

export class FleetInventoryRunTokenOperationError extends Error {
  constructor(readonly operationId: string) {
    super(`no fleet inventory run for operation '${operationId}'`);
    this.name = 'FleetInventoryRunTokenOperationError';
  }
}

export class FleetInventoryRunTokenFutureError extends Error {
  constructor() {
    super('fleet inventory run token is ahead of the persisted run');
    this.name = 'FleetInventoryRunTokenFutureError';
  }
}

function malformed(): never {
  throw new FleetInventoryStateError();
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function plainRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return malformed();
  }
  return value as Record<string, unknown>;
}

function exactKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[] = [],
): void {
  const keys = Object.keys(value).sort();
  const allowed = new Set([...required, ...optional]);
  if (
    !required.every((key) => Object.hasOwn(value, key)) ||
    keys.some((key) => !allowed.has(key))
  ) {
    malformed();
  }
}

function boundedString(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    utf8Length(value) <= FLEET_INVENTORY_STRING_BYTE_BOUND
  );
}

function safeIntegerAtLeast(value: unknown, minimum = 0): value is number {
  return Number.isSafeInteger(value) && Number(value) >= minimum;
}

function canonicalIso(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    Number.isFinite(Date.parse(value)) &&
    new Date(value).toISOString() === value
  );
}

function sha256Hex(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

/**
 * Credential and length hygiene for EVERY durable inventory string, including
 * provider resumption cursors, which are legitimately opaque and therefore
 * never face a name grammar.
 */
export function assertNoCredentialInInventoryText(
  value: string,
  field: string,
): void {
  if (
    typeof value !== 'string' ||
    utf8Length(value) > FLEET_INVENTORY_DURABLE_TEXT_BYTE_BOUND
  ) {
    throw new InventoryFindingValueError(field);
  }
  const lowered = value.toLowerCase();
  if (CREDENTIAL_SUBSTRINGS.some((marker) => lowered.includes(marker))) {
    throw new InventoryFindingValueError(field);
  }
}

/**
 * Validator for every value interpolated into a durable finding detail except
 * the raw host-routing KV key name: the credential control plus the
 * printable/non-whitespace rule. It deliberately omits the KV-only base64
 * shape rule so a long dotless script name or dispatch namespace is accepted.
 * Hostnames are normalized to ASCII by the caller before validation.
 */
export function assertInventoryFindingValue(
  value: string,
  field: string,
): void {
  assertNoCredentialInInventoryText(value, field);
  if (!PRINTABLE_NON_WHITESPACE.test(value)) {
    throw new InventoryFindingValueError(field);
  }
}

/**
 * Non-throwing shape predicate used ONLY for the raw host-routing KV key name:
 * true when the value is printable, non-whitespace, and not base64 or
 * high-entropy shaped. Credential and length refusals are not part of this
 * predicate; that site asserts them first and only then consults this shape.
 */
export function isInventoryKeyNameShape(value: string): boolean {
  if (typeof value !== 'string' || !PRINTABLE_NON_WHITESPACE.test(value)) {
    return false;
  }
  return !(
    value.length >= BASE64_SHAPE_MIN_LENGTH &&
    BASE64_SHAPE.test(value) &&
    !value.includes('.')
  );
}

/** Zeroed staged-row counts, one entry per row kind. */
export function emptyFleetInventoryRowCounts(): Readonly<
  Record<FleetInventoryRowKind, number>
> {
  const counts = {} as Record<FleetInventoryRowKind, number>;
  for (const kind of FLEET_INVENTORY_ROW_KINDS) counts[kind] = 0;
  return counts;
}

/**
 * Canonicalizes caller options, resolving both defaults at start and refusing
 * missing prefixes with today's `collectFleetInventory` message. An empty
 * `hostRoutingKvId` is absent, matching the truthiness check the drain uses.
 */
export function canonicalFleetInventoryRunOptions(
  options: CollectFleetInventoryOptions,
): FleetInventoryRunOptions {
  const candidate = plainRecord(options);
  exactKeys(
    candidate,
    ['databaseNamePrefix', 'scriptNamePrefix'],
    ['hostRoutingKvId', 'includeDispatchNamespace', 'includeR2Buckets'],
  );
  if (!candidate.databaseNamePrefix || !candidate.scriptNamePrefix) {
    throw new Error(
      'databaseNamePrefix and scriptNamePrefix are required for fleet inventory',
    );
  }
  if (
    typeof candidate.databaseNamePrefix !== 'string' ||
    typeof candidate.scriptNamePrefix !== 'string' ||
    (candidate.hostRoutingKvId !== undefined &&
      typeof candidate.hostRoutingKvId !== 'string') ||
    (candidate.includeDispatchNamespace !== undefined &&
      typeof candidate.includeDispatchNamespace !== 'boolean') ||
    (candidate.includeR2Buckets !== undefined &&
      typeof candidate.includeR2Buckets !== 'boolean')
  ) {
    return malformed();
  }
  const hostRoutingKvId = candidate.hostRoutingKvId
    ? candidate.hostRoutingKvId
    : undefined;
  assertNoCredentialInInventoryText(
    candidate.databaseNamePrefix,
    'databaseNamePrefix',
  );
  assertNoCredentialInInventoryText(
    candidate.scriptNamePrefix,
    'scriptNamePrefix',
  );
  if (hostRoutingKvId !== undefined) {
    assertNoCredentialInInventoryText(hostRoutingKvId, 'hostRoutingKvId');
  }
  return {
    ...(hostRoutingKvId === undefined ? {} : { hostRoutingKvId }),
    databaseNamePrefix: candidate.databaseNamePrefix,
    scriptNamePrefix: candidate.scriptNamePrefix,
    includeDispatchNamespace:
      candidate.includeDispatchNamespace ?? hostRoutingKvId !== undefined,
    includeR2Buckets: candidate.includeR2Buckets ?? false,
  };
}

/** Digest over the canonical options, stable under caller key order. */
export function fleetInventoryOptionsDigest(
  options: FleetInventoryRunOptions,
): string {
  const canonical = fleetInventoryRunOptionsFromUnknown(options);
  const entries = Object.entries(canonical).sort(([left], [right]) =>
    left < right ? -1 : 1,
  );
  return sha256Hex(JSON.stringify(entries));
}

function fleetInventoryRunOptionsFromUnknown(
  value: unknown,
): FleetInventoryRunOptions {
  const candidate = plainRecord(value);
  exactKeys(
    candidate,
    [
      'databaseNamePrefix',
      'scriptNamePrefix',
      'includeDispatchNamespace',
      'includeR2Buckets',
    ],
    ['hostRoutingKvId'],
  );
  if (
    !boundedString(candidate.databaseNamePrefix) ||
    !boundedString(candidate.scriptNamePrefix) ||
    (candidate.hostRoutingKvId !== undefined &&
      !boundedString(candidate.hostRoutingKvId)) ||
    typeof candidate.includeDispatchNamespace !== 'boolean' ||
    typeof candidate.includeR2Buckets !== 'boolean'
  ) {
    return malformed();
  }
  assertNoCredentialInInventoryText(
    candidate.databaseNamePrefix,
    'databaseNamePrefix',
  );
  assertNoCredentialInInventoryText(
    candidate.scriptNamePrefix,
    'scriptNamePrefix',
  );
  if (candidate.hostRoutingKvId !== undefined) {
    assertNoCredentialInInventoryText(
      candidate.hostRoutingKvId,
      'hostRoutingKvId',
    );
  }
  return {
    ...(candidate.hostRoutingKvId === undefined
      ? {}
      : { hostRoutingKvId: candidate.hostRoutingKvId }),
    databaseNamePrefix: candidate.databaseNamePrefix,
    scriptNamePrefix: candidate.scriptNamePrefix,
    includeDispatchNamespace: candidate.includeDispatchNamespace,
    includeR2Buckets: candidate.includeR2Buckets,
  };
}

function stageStep(value: unknown): FleetInventoryStageStep {
  if (
    typeof value !== 'string' ||
    !FLEET_INVENTORY_STAGE_ORDER.includes(value as FleetInventoryStageStep)
  ) {
    return malformed();
  }
  return value as FleetInventoryStageStep;
}

/** Strict stage codec; ordinals and resumption cursors are bounded. */
export function fleetInventoryStageFromUnknown(
  value: unknown,
): FleetInventoryStage {
  const candidate = plainRecord(value);
  const step = stageStep(candidate.step);
  const shape = STAGE_SHAPES[step];
  exactKeys(
    candidate,
    ['step', ...(shape.ordinal ? [shape.ordinal] : [])],
    shape.text ? [shape.text] : [],
  );
  const ordinal = shape.ordinal ? candidate[shape.ordinal] : undefined;
  if (
    shape.ordinal &&
    (!safeIntegerAtLeast(ordinal) ||
      (shape.maxOrdinal !== undefined && ordinal > shape.maxOrdinal))
  ) {
    return malformed();
  }
  const text = shape.text ? candidate[shape.text] : undefined;
  if (text !== undefined) {
    if (!boundedString(text)) return malformed();
    // The cursor carve-out: resumption text is opaque, so it faces the
    // credential control only, never the finding-detail grammar.
    assertNoCredentialInInventoryText(text, `stage.${String(shape.text)}`);
  }
  // The validated candidate keys are exactly one union member's keys.
  return {
    step,
    ...(shape.ordinal ? { [shape.ordinal]: ordinal } : {}),
    ...(shape.text && text !== undefined ? { [shape.text]: text } : {}),
  } as FleetInventoryStage;
}

function stageEnabled(
  step: FleetInventoryStageStep,
  options: FleetInventoryRunOptions,
  counts: Readonly<Record<FleetInventoryRowKind, number>>,
): boolean {
  // `includeDispatchNamespace` deliberately gates NO stage: it only skips the
  // namespace attestation INSIDE `registration-postprocess`, which is the
  // provider engine's work, so the stage itself is still entered.
  const hostRouting = options.hostRoutingKvId !== undefined;
  switch (step) {
    case 'host-kv-keys':
    case 'dispatch-pages':
    case 'registration-postprocess':
      return hostRouting;
    case 'host-kv-values':
    case 'registration-checks':
      return hostRouting && counts.registration > 0;
    case 'zone-routes':
      return counts.meta > 0;
    case 'ordinary-script-detail':
      return counts.deployment > 0;
    case 'r2-buckets':
      return options.includeR2Buckets;
    default:
      return true;
  }
}

function stageEntry(step: FleetInventoryStageStep): FleetInventoryStage {
  const shape = STAGE_SHAPES[step];
  // Entering a stage always starts at its first item.
  return {
    step,
    ...(shape.ordinal ? { [shape.ordinal]: 0 } : {}),
  } as FleetInventoryStage;
}

/**
 * Pure stage successor: no provider or D1 input. Stage skipping uses the same
 * conditions as the single-pass drain, read from the canonical options and the
 * staged-row counts that feed the per-item stages. Intra-stage ordinal and
 * cursor advancement belongs to the provider engine, not to this successor.
 */
export function nextStage(
  stage: FleetInventoryStage,
  options: FleetInventoryRunOptions,
  counts: Readonly<Record<FleetInventoryRowKind, number>>,
): FleetInventoryStage {
  const current = fleetInventoryStageFromUnknown(stage);
  const canonicalOptions = fleetInventoryRunOptionsFromUnknown(options);
  const canonicalCounts = rowCountsFromUnknown(counts);
  if (current.step === 'finalize') return { step: 'finalize' };
  for (
    let index = FLEET_INVENTORY_STAGE_ORDER.indexOf(current.step) + 1;
    index < FLEET_INVENTORY_STAGE_ORDER.length;
    index += 1
  ) {
    const step = FLEET_INVENTORY_STAGE_ORDER[index];
    if (step && stageEnabled(step, canonicalOptions, canonicalCounts)) {
      return stageEntry(step);
    }
  }
  return { step: 'finalize' };
}

/** The first enabled stage for a run that has staged nothing yet. */
export function initialFleetInventoryStage(
  options: FleetInventoryRunOptions,
): FleetInventoryStage {
  const canonicalOptions = fleetInventoryRunOptionsFromUnknown(options);
  const counts = emptyFleetInventoryRowCounts();
  for (const step of FLEET_INVENTORY_STAGE_ORDER) {
    if (stageEnabled(step, canonicalOptions, counts)) return stageEntry(step);
  }
  return { step: 'finalize' };
}

function rowCountsFromUnknown(
  value: unknown,
): Readonly<Record<FleetInventoryRowKind, number>> {
  const candidate = plainRecord(value);
  exactKeys(candidate, FLEET_INVENTORY_ROW_KINDS);
  const counts = {} as Record<FleetInventoryRowKind, number>;
  for (const kind of FLEET_INVENTORY_ROW_KINDS) {
    const count = candidate[kind];
    if (!safeIntegerAtLeast(count)) return malformed();
    counts[kind] = count;
  }
  return counts;
}

function progressFromUnknown(value: unknown): FleetInventoryRunProgress {
  const candidate = plainRecord(value);
  exactKeys(
    candidate,
    [
      'stage',
      'generation',
      'revision',
      'stagedCounts',
      'factCount',
      'providerRequests',
    ],
    ['lastPageDigest'],
  );
  if (
    !safeIntegerAtLeast(candidate.generation, 1) ||
    !safeIntegerAtLeast(candidate.revision) ||
    !safeIntegerAtLeast(candidate.factCount) ||
    !safeIntegerAtLeast(candidate.providerRequests) ||
    (candidate.lastPageDigest !== undefined &&
      (typeof candidate.lastPageDigest !== 'string' ||
        !SHA256.test(candidate.lastPageDigest)))
  ) {
    return malformed();
  }
  return {
    stage: fleetInventoryStageFromUnknown(candidate.stage),
    generation: candidate.generation,
    revision: candidate.revision,
    stagedCounts: rowCountsFromUnknown(candidate.stagedCounts),
    factCount: candidate.factCount,
    ...(candidate.lastPageDigest === undefined
      ? {}
      : { lastPageDigest: candidate.lastPageDigest }),
    providerRequests: candidate.providerRequests,
  };
}

function boundedPlain(value: unknown, maxBytes: number): unknown {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: DEPTH_BOUND,
      maxNodes: NODE_BOUND,
      maxScalarBytes: maxBytes,
      maxSerializedBytes: maxBytes,
      error: () => new FleetInventoryStateError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    return malformed();
  }
  return plain;
}

/** Strict run-record codec; every bound fails closed and never truncates. */
export function fleetInventoryRunRecordFromUnknown(
  value: unknown,
): FleetInventoryRunRecord {
  const candidate = plainRecord(
    boundedPlain(value, FLEET_INVENTORY_RUN_RECORD_BYTE_BOUND),
  );
  exactKeys(candidate, [
    'version',
    'operationId',
    'optionsDigest',
    'options',
    'state',
    'progress',
    'updatedAt',
  ]);
  if (
    candidate.version !== 1 ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    typeof candidate.optionsDigest !== 'string' ||
    !SHA256.test(candidate.optionsDigest) ||
    (candidate.state !== 'staging' &&
      candidate.state !== 'finalized' &&
      candidate.state !== 'failed') ||
    !canonicalIso(candidate.updatedAt)
  ) {
    return malformed();
  }
  const options = fleetInventoryRunOptionsFromUnknown(candidate.options);
  if (fleetInventoryOptionsDigest(options) !== candidate.optionsDigest) {
    return malformed();
  }
  return {
    version: 1,
    operationId: candidate.operationId,
    optionsDigest: candidate.optionsDigest,
    options,
    state: candidate.state,
    progress: progressFromUnknown(candidate.progress),
    updatedAt: candidate.updatedAt,
  };
}

function payloadFromUnknown(
  value: unknown,
  field: string,
): Readonly<Record<string, unknown>> {
  const payload = plainRecord(
    boundedPlain(value, FLEET_INVENTORY_STAGED_PAYLOAD_BYTE_BOUND),
  );
  for (const [key, entry] of Object.entries(payload)) {
    assertNoCredentialInInventoryText(key, `${field}.key`);
    if (typeof entry === 'string') {
      assertNoCredentialInInventoryText(entry, `${field}.${key}`);
    }
  }
  return { ...payload };
}

/** Strict staged-row codec with an exact-key table and a bounded payload. */
export function fleetInventoryStagedRowFromUnknown(
  value: unknown,
): FleetInventoryStagedRow {
  const candidate = plainRecord(value);
  exactKeys(candidate, ['kind', 'ordinal', 'payload']);
  if (
    typeof candidate.kind !== 'string' ||
    !FLEET_INVENTORY_ROW_KINDS.includes(
      candidate.kind as FleetInventoryRowKind,
    ) ||
    !safeIntegerAtLeast(candidate.ordinal)
  ) {
    return malformed();
  }
  return {
    kind: candidate.kind as FleetInventoryRowKind,
    ordinal: candidate.ordinal,
    payload: payloadFromUnknown(candidate.payload, 'row.payload'),
  };
}

/** Strict deployment-fact codec with an exact-key table. */
export function fleetInventoryStagedFactFromUnknown(
  value: unknown,
): FleetInventoryStagedFact {
  const candidate = plainRecord(value);
  exactKeys(candidate, [
    'deploymentOrdinal',
    'factKind',
    'factOrdinal',
    'payload',
  ]);
  if (
    !safeIntegerAtLeast(candidate.deploymentOrdinal) ||
    typeof candidate.factKind !== 'string' ||
    !FLEET_INVENTORY_DEPLOYMENT_FACT_KINDS.includes(
      candidate.factKind as FleetInventoryDeploymentFactKind,
    ) ||
    !safeIntegerAtLeast(candidate.factOrdinal)
  ) {
    return malformed();
  }
  return {
    deploymentOrdinal: candidate.deploymentOrdinal,
    factKind: candidate.factKind as FleetInventoryDeploymentFactKind,
    factOrdinal: candidate.factOrdinal,
    payload: payloadFromUnknown(candidate.payload, 'fact.payload'),
  };
}

/** Strict generation-reference codec; the manifest carries every row kind. */
export function fleetInventoryGenerationRefFromUnknown(
  value: unknown,
): FleetInventoryGenerationRef {
  const candidate = plainRecord(value);
  exactKeys(candidate, [
    'generation',
    'operationId',
    'finalizedAtMs',
    'rowManifest',
    'factCount',
  ]);
  if (
    !safeIntegerAtLeast(candidate.generation, 1) ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    !safeIntegerAtLeast(candidate.finalizedAtMs) ||
    !safeIntegerAtLeast(candidate.factCount)
  ) {
    return malformed();
  }
  return {
    generation: candidate.generation,
    operationId: candidate.operationId,
    finalizedAtMs: candidate.finalizedAtMs,
    rowManifest: rowCountsFromUnknown(candidate.rowManifest),
    factCount: candidate.factCount,
  };
}

/** Strict token codec; the token carries nothing but version and position. */
export function parseFleetInventoryRunToken(
  value: unknown,
): FleetInventoryRunToken {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: 4,
      maxNodes: 16,
      maxScalarBytes: FLEET_INVENTORY_TOKEN_BYTE_BOUND,
      maxSerializedBytes: FLEET_INVENTORY_TOKEN_BYTE_BOUND,
      error: () => new FleetInventoryRunTokenError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    throw new FleetInventoryRunTokenError();
  }
  let candidate: Record<string, unknown>;
  try {
    candidate = plainRecord(plain);
    exactKeys(candidate, ['version', 'operationId', 'revision']);
  } catch {
    throw new FleetInventoryRunTokenError();
  }
  if (
    candidate.version !== 1 ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    !safeIntegerAtLeast(candidate.revision)
  ) {
    throw new FleetInventoryRunTokenError();
  }
  return {
    version: 1,
    operationId: candidate.operationId,
    revision: candidate.revision,
  };
}

/**
 * Throws `FleetInventoryRunTokenError` for a malformed token, then
 * `FleetInventoryRunTokenOperationError` when the run is absent or belongs to
 * another operation, then `FleetInventoryRunTokenFutureError` when the token is
 * ahead of the persisted progress; otherwise returns `current` on equality and
 * `stale` when the token is behind. Account identity is trusted config, so a
 * foreign token simply misses its run row.
 */
export function classifyFleetInventoryRunToken(
  token: FleetInventoryRunToken,
  run: FleetInventoryRunRecord | undefined,
): 'current' | 'stale' {
  const parsed = parseFleetInventoryRunToken(token);
  if (!run || run.operationId !== parsed.operationId) {
    throw new FleetInventoryRunTokenOperationError(parsed.operationId);
  }
  if (parsed.revision > run.progress.revision) {
    throw new FleetInventoryRunTokenFutureError();
  }
  return parsed.revision === run.progress.revision ? 'current' : 'stale';
}
