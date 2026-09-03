// SPDX-License-Identifier: Apache-2.0

import { structuralBackendSwitchFleetRecordFromUnknown } from './backend-switch.js';
import {
  isDeploymentEnvironment,
  isDeploymentTenantTag,
} from './deployment-context.js';
import type { DriftFinding } from './fleet.js';
import {
  auditDeploymentGapsStage,
  auditDeploymentOrphansStage,
  auditNamespaceExpectationsStage,
  auditNamespaceOrphansStage,
  auditOrphanDatabasesStage,
  auditOrphanRoutesStage,
  auditR2ExpectedStage,
  auditR2MissingIdentityStage,
  auditR2OrphansStage,
  auditRecordStep,
  auditRegistrationOrphansStage,
  type FleetAuditExpectedBucketEntry,
  type FleetAuditKnownSets,
  fleetAuditAuditedRecords,
  fleetAuditExpectedBucketsSeed,
  fleetAuditExpectedNamespaceIds,
  fleetAuditExpectedNamespaceOwnersSeed,
  fleetAuditExpectedRoutes,
  fleetAuditKnownSets,
  fleetAuditLiveByScript,
  fleetAuditLiveRoutesByHostname,
  fleetAuditRecordsByScript,
  fleetAuditRecordsDerivedDuplicateNamespaceIds,
  fleetAuditRegisteredDatabaseIds,
} from './fleet.js';
import {
  type DriftFindingRowPayload,
  driftFindingRowFromUnknown,
  type FleetAuditFactPayload,
  type FleetAuditFindingKind,
  type FleetAuditProgress,
  type FleetAuditStage,
  fleetAuditFactRowFromUnknown,
  fleetAuditProgressFromUnknown,
  fleetAuditStageOrdinal,
  nextAuditStage,
  withAuditStageOrdinal,
  withheldAuditDetail,
} from './fleet-audit-state.js';
import { readFleetInventoryGeneration } from './fleet-inventory-advance.js';
import type { FleetInventoryRunStore } from './fleet-inventory-state.js';
import {
  assertFleetOperationId,
  classifyFleetOperationToken,
  FLEET_OPERATION_ITEM_BOUND,
  FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  FLEET_OPERATION_STAGE_BATCH_STATEMENTS,
  type FleetOperationFailure,
  type FleetOperationLease,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  FleetOperationStateError,
  type FleetOperationStore,
  type FleetOperationToken,
  FleetOperationTokenOperationError,
  fleetOperationItemsIntake,
  fleetOperationOtherKindMessage,
  fleetOperationSafeInteger,
  fleetOperationStagedRowPayloadFitsEnvelope,
  fleetOperationTokenOf,
  isDurableAuditDetailSafe,
  malformed,
  parseFleetOperationToken,
  readAllFleetOperationRows,
} from './fleet-operation-state.js';
import type {
  DeploymentSpec,
  FleetRecord,
  FleetResourceInventory,
  FleetStateStore,
  ProvisioningBackend,
} from './types.js';

/** Default global-stage chunk size; the frozen range is 1..2,000. */
const DEFAULT_MAX_ITEMS_PER_CALL = 500;
const MIN_MAX_ITEMS_PER_CALL = 1;
const MAX_MAX_ITEMS_PER_CALL = 2_000;
/** Fixed refusal shared by the coordinator's own count check and the intake's. */
const RECORD_COUNT_MESSAGE = `fleet audit start accepts at most ${FLEET_OPERATION_ITEM_BOUND} records`;

/** One bounded audit step: begin an operation, or continue a persisted one. */
export type FleetAuditAdvanceAction =
  | Readonly<{
      kind: 'start';
      operationId: string;
      records: readonly FleetRecord[];
      staleAfterMs: number;
      generation?: number;
    }>
  | Readonly<{ kind: 'continue'; token: unknown }>;

/** Inputs for one bounded audit advance call. */
export interface AdvanceFleetAuditOptions {
  readonly operationStore: FleetOperationStore;
  readonly inventoryStore: FleetInventoryRunStore;
  readonly fleetStore: FleetStateStore;
  readonly backendFor: (record: FleetRecord) => ProvisioningBackend;
  readonly specFor: (record: FleetRecord) => DeploymentSpec;
  readonly maintenanceSecretFor: (record: FleetRecord) => string;
  readonly action: FleetAuditAdvanceAction;
  /** Global-stage chunk size; 1..2,000, default 500. */
  readonly maxItemsPerCall?: number;
  /**
   * Sampled at most once per start call, immediately before `startOperation`.
   * The sample must be a non-negative safe integer representable by `Date`.
   * Only a `created` outcome persists the sample as the frozen `auditTimeMs`;
   * adopted outcomes discard it, and continue calls never sample it. Defaults
   * to `Date.now`.
   */
  readonly auditClock?: () => number;
  /** Feeds only the re-arm's authority clock (§6.1); default `Date.now`. */
  readonly authorityClock?: () => number;
  /** Call-local only; never persisted. */
  readonly signal?: AbortSignal;
}

/** Authoritative summary of a finalized audit operation. */
export interface FleetAuditResultRef {
  readonly operationId: string;
  readonly generation: number;
  readonly recordCount: number;
  readonly findingCount: number;
  readonly finalizedAtMs: number;
}

/** Authoritative durable outcome after at most one bounded stage chunk. */
export type FleetAuditAdvanceResult =
  /**
   * The operation is still RUNNING. `stage` is the persisted stage the next
   * call resumes from, and `token` carries the committed revision.
   */
  | Readonly<{
      status: 'pending';
      token: FleetOperationToken;
      stage: FleetAuditStage;
    }>
  /**
   * The operation is FINALIZED. `result` summarizes the frozen run; the
   * findings themselves are read separately with
   * `readFleetAuditFindingsPage`.
   */
  | Readonly<{
      status: 'complete';
      token: FleetOperationToken;
      result: FleetAuditResultRef;
    }>
  /**
   * The operation is FAILED, and the failing call released its generation pin.
   * A pin that outlived a crash between the terminal commit and that release is
   * cleared by `abandonFleetAuditOperation()`. `failure` carries the durable
   * reason; failed operations are still readable.
   */
  | Readonly<{
      status: 'failed';
      token: FleetOperationToken;
      failure: FleetOperationFailure;
    }>;

/** Named capability whose absence makes bounded audit work fail closed. */
export type FleetAuditAdvanceCapability =
  | 'operation-store'
  | 'generation-read'
  | 'generation-pin';

const CAPABILITY_MESSAGES: Readonly<
  Record<FleetAuditAdvanceCapability, string>
> = Object.freeze({
  'operation-store': 'fleet audit advance requires an operation store',
  'generation-read':
    'fleet audit advance requires an inventory store that can read finalized generations',
  'generation-pin':
    'fleet audit advance requires an inventory store that can pin finalized generations',
});

const CAPABILITY_MEMBERS: Readonly<
  Record<FleetAuditAdvanceCapability, readonly string[]>
> = Object.freeze({
  'operation-store': Object.freeze([
    'withAccountOperationLease',
    'readOperationById',
    'readOperationRowsPage',
  ]),
  'generation-read': Object.freeze([
    'readFinalizedGeneration',
    'readRunByOperation',
  ]),
  'generation-pin': Object.freeze(['pinGeneration', 'releasePin']),
});

/** Fixed configuration refusal for one missing bounded capability. */
export class FleetAuditAdvanceCapabilityError extends Error {
  constructor(readonly capability: FleetAuditAdvanceCapability) {
    super(CAPABILITY_MESSAGES[capability]);
    this.name = 'FleetAuditAdvanceCapabilityError';
  }
}

/**
 * Probes one injected port for the members a capability names. `target` is
 * `object` rather than a port type because this coordinator gates two
 * unrelated ports (the operation store and the inventory store) through the
 * same table, and it is deliberately named for the audit capability set
 * rather than for one store — the R3 sibling's `assertStoreCapability` gates
 * a single store and keeps the narrower name.
 */
function assertCapability(
  target: object,
  capability: FleetAuditAdvanceCapability,
): void {
  for (const member of CAPABILITY_MEMBERS[capability]) {
    if (
      !Reflect.has(target, member) ||
      typeof (target as unknown as Record<string, unknown>)[member] !==
        'function'
    ) {
      throw new FleetAuditAdvanceCapabilityError(capability);
    }
  }
}

function assertMaxItemsPerCall(value: number): void {
  if (
    !Number.isSafeInteger(value) ||
    value < MIN_MAX_ITEMS_PER_CALL ||
    value > MAX_MAX_ITEMS_PER_CALL
  ) {
    throw new Error(
      `maxItemsPerCall must be an integer from ${MIN_MAX_ITEMS_PER_CALL} to ${MAX_MAX_ITEMS_PER_CALL}`,
    );
  }
}

function resultFromRun(run: FleetOperationRunRecord): FleetAuditAdvanceResult {
  const progress = fleetAuditProgressFromUnknown(run.progress);
  const token = fleetOperationTokenOf(run);
  if (run.state === 'finalized') {
    if (run.terminalAtMs === undefined) return malformed();
    return {
      status: 'complete',
      token,
      result: {
        operationId: run.operationId,
        generation: progress.generation,
        recordCount: progress.recordCount,
        findingCount: progress.findingCount,
        finalizedAtMs: run.terminalAtMs,
      },
    };
  }
  if (run.state === 'failed') {
    if (progress.failure === undefined) return malformed();
    return {
      status: 'failed',
      token,
      failure: progress.failure,
    };
  }
  return { status: 'pending', token, stage: progress.stage };
}

function pinnedBy(operationId: string): string {
  return `fleet-audit:${operationId}`;
}

/**
 * The `pending` result for a run record the store has just committed. Every
 * chunk that leaves the operation RUNNING reports the persisted stage read
 * back through the codec, never the stage it computed.
 */
function pendingFromCommitted(
  committed: FleetOperationRunRecord,
): FleetAuditAdvanceResult {
  return {
    status: 'pending',
    token: fleetOperationTokenOf(committed),
    stage: fleetAuditProgressFromUnknown(committed.progress).stage,
  };
}

type IsSubset<Left, Right> = [Left] extends [Right] ? true : false;
const DRIFT_FINDING_KINDS_ARE_AUDIT_KINDS: IsSubset<
  DriftFinding['kind'],
  FleetAuditFindingKind
> = true;
const AUDIT_FINDING_KINDS_ARE_DRIFT_KINDS: IsSubset<
  FleetAuditFindingKind,
  DriftFinding['kind']
> = true;
// Both assertions are compile-time only; these module-scope references keep
// them reachable so an unused-binding cleanup cannot delete the check.
void DRIFT_FINDING_KINDS_ARE_AUDIT_KINDS;
void AUDIT_FINDING_KINDS_ARE_DRIFT_KINDS;

function fleetAuditFindingKind(
  kind: DriftFinding['kind'],
): FleetAuditFindingKind {
  return kind;
}

/**
 * Non-throwing durable write gate (§5.1/§6.4): the bounded path gates; the
 * drain never does. Only `detail` is gated: `tenantTag` and `environment`
 * reach the row verbatim, which is the round-3 adjudication, so the name says
 * gated DETAIL rather than a sanitized row.
 */
function findingRowWithGatedDetail(
  finding: DriftFinding,
): DriftFindingRowPayload {
  const kind = fleetAuditFindingKind(finding.kind);
  return {
    tenantTag: finding.tenantTag,
    environment: finding.environment,
    kind,
    detail: isDurableAuditDetailSafe(finding.detail)
      ? finding.detail
      : withheldAuditDetail(kind),
  };
}

/**
 * Stages one durable row, overloading two distinct signals on one return so
 * every call site can treat them uniformly:
 *
 * - `undefined` means the payload exceeds the staged-row envelope. That is a
 *   caller-visible emission bound, so the site fails the operation durably
 *   through `failEmission`.
 * - a thrown `FleetOperationStateError` means the payload does not satisfy
 *   the row codec at all, which is durable corruption and propagates.
 *
 * No site needs to tell the two apart, which is why a discriminated result
 * would buy nothing here.
 */
function stagedAuditRow(
  rowKind: 'finding' | 'fact',
  ordinal: number,
  payload: DriftFindingRowPayload | FleetAuditFactPayload,
): FleetOperationStagedRow | undefined {
  if (!fleetOperationStagedRowPayloadFitsEnvelope(rowKind, payload)) {
    return undefined;
  }
  try {
    const validated =
      rowKind === 'finding'
        ? driftFindingRowFromUnknown(payload)
        : fleetAuditFactRowFromUnknown(payload);
    return {
      rowKind,
      ordinal,
      payload: { ...validated },
    };
  } catch (error) {
    // Only a codec refusal means the payload is malformed; a programming
    // fault must not be laundered into durable-corruption identity.
    if (!(error instanceof FleetOperationStateError)) throw error;
    return malformed();
  }
}

interface GlobalStageChunk {
  readonly findings: readonly DriftFinding[];
  readonly stage: FleetAuditStage;
}

type GlobalAuditStage = Exclude<
  FleetAuditStage,
  { step: 'per-record' | 'finalize' }
>;
type GlobalAuditStep = GlobalAuditStage['step'];
type PerRecordAuditStage = Extract<FleetAuditStage, { step: 'per-record' }>;

interface GlobalStageContext {
  readonly inventory: FleetResourceInventory;
  readonly auditedRecords: readonly FleetRecord[];
  readonly known: FleetAuditKnownSets;
  readonly recordsByScript: ReturnType<typeof fleetAuditRecordsByScript>;
}

/** The one accessor for the optional R2 inventory slice. */
function inventoryR2Buckets(
  context: GlobalStageContext,
): readonly NonNullable<FleetResourceInventory['r2Buckets']>[number][] {
  return context.inventory.r2Buckets ?? [];
}

function chunked<T>(
  stage: GlobalAuditStage,
  maxItemsPerCall: number,
  source: readonly T[],
  emit: (slice: readonly T[], ordinal: number) => readonly DriftFinding[],
): GlobalStageChunk {
  const ordinal = fleetAuditStageOrdinal(stage);
  if (ordinal === undefined) return malformed();
  // A global stage never persists a cursor at the end of its own source, so
  // the only admissible cursor for an empty source is zero.
  if (source.length === 0 ? ordinal > 0 : ordinal >= source.length) {
    return malformed();
  }
  const slice = source.slice(ordinal, ordinal + maxItemsPerCall);
  const nextOrdinal = ordinal + slice.length;
  const exhausted = nextOrdinal >= source.length;
  return {
    findings: emit(slice, ordinal),
    stage: exhausted
      ? nextAuditStage(stage, true)
      : withAuditStageOrdinal(stage.step, nextOrdinal),
  };
}

type GlobalStageDescriptor = (
  stage: GlobalAuditStage,
  maxItemsPerCall: number,
  context: GlobalStageContext,
) => GlobalStageChunk;

function globalStageDescriptor<T>(
  source: (context: GlobalStageContext) => readonly T[],
  emit: (
    slice: readonly T[],
    context: GlobalStageContext,
    ordinal: number,
  ) => readonly DriftFinding[],
): GlobalStageDescriptor {
  return (stage, maxItemsPerCall, context) =>
    chunked(stage, maxItemsPerCall, source(context), (slice, ordinal) =>
      emit(slice, context, ordinal),
    );
}

const GLOBAL_STAGE_DESCRIPTORS = Object.freeze({
  'provider-findings': globalStageDescriptor(
    (context) => context.inventory.findings,
    (slice) => slice,
  ),
  'registration-orphans': globalStageDescriptor(
    (context) => context.inventory.scriptRegistrations,
    (slice, context) =>
      auditRegistrationOrphansStage({
        scriptRegistrations: slice,
        deployments: context.inventory.deployments,
        recordsByScript: context.recordsByScript,
        knownScriptKeys: context.known.knownScriptKeys,
      }),
  ),
  'deployment-orphans': globalStageDescriptor(
    (context) => context.inventory.deployments,
    (slice, context) =>
      auditDeploymentOrphansStage({
        deployments: slice,
        recordsByScript: context.recordsByScript,
        knownScriptKeys: context.known.knownScriptKeys,
      }),
  ),
  'deployment-gaps': globalStageDescriptor(
    (context) => context.auditedRecords,
    (slice, context) =>
      auditDeploymentGapsStage({
        records: slice,
        liveByScript: fleetAuditLiveByScript(context.inventory.deployments),
        scriptRegistrations: context.inventory.scriptRegistrations,
      }),
  ),
  'orphan-databases': globalStageDescriptor(
    (context) => context.inventory.databaseIds,
    (slice, context) =>
      auditOrphanDatabasesStage({
        databaseIds: slice,
        registeredDatabaseIds: fleetAuditRegisteredDatabaseIds(
          context.auditedRecords,
        ),
        knownDatabaseIds: context.known.knownDatabaseIds,
      }),
  ),
  'orphan-routes': globalStageDescriptor(
    (context) => context.inventory.routes,
    (slice, context) =>
      auditOrphanRoutesStage({
        routes: slice,
        expectedRoutes: fleetAuditExpectedRoutes(context.auditedRecords),
        knownRouteKeys: context.known.knownRouteKeys,
      }),
  ),
  'namespace-orphans': globalStageDescriptor(
    (context) => context.inventory.namespaceIds,
    (slice, context) =>
      auditNamespaceOrphansStage({
        namespaceIds: slice,
        expectedNamespaceIds: fleetAuditExpectedNamespaceIds(
          context.auditedRecords,
        ),
        knownNamespaceIds: context.known.knownNamespaceIds,
      }),
  ),
  'namespace-expectations': globalStageDescriptor(
    (context) => context.auditedRecords,
    (slice, context, ordinal) =>
      auditNamespaceExpectationsStage({
        records: slice,
        inventoryNamespaceIds: context.inventory.namespaceIds,
        expectedNamespaceOwners: fleetAuditExpectedNamespaceOwnersSeed(
          context.auditedRecords.slice(0, ordinal),
        ),
      }),
  ),
  'r2-expected': globalStageDescriptor(
    (context) => context.auditedRecords,
    (slice, context, ordinal) =>
      auditR2ExpectedStage({
        records: slice,
        expectedBuckets: fleetAuditExpectedBucketsSeed(
          context.auditedRecords.slice(0, ordinal),
        ),
      }),
  ),
  'r2-orphans': globalStageDescriptor(
    (context) => inventoryR2Buckets(context),
    (slice, context) =>
      auditR2OrphansStage({
        r2Buckets: slice,
        expectedBuckets: fleetAuditExpectedBucketsSeed(context.auditedRecords),
        knownBucketNames: context.known.knownBucketNames,
      }),
  ),
  'r2-missing-identity': globalStageDescriptor(
    (context): readonly FleetAuditExpectedBucketEntry[] => [
      ...fleetAuditExpectedBucketsSeed(context.auditedRecords).values(),
    ],
    (slice, context) =>
      auditR2MissingIdentityStage({
        expectedBucketEntries: slice,
        r2Buckets: inventoryR2Buckets(context),
      }),
  ),
} satisfies Readonly<Record<GlobalAuditStep, GlobalStageDescriptor>>);

/** Advances one bounded chunk of the current global stage. */
function advanceGlobalStage(
  stage: GlobalAuditStage,
  maxItemsPerCall: number,
  context: GlobalStageContext,
): GlobalStageChunk {
  return GLOBAL_STAGE_DESCRIPTORS[stage.step](stage, maxItemsPerCall, context);
}

function buildInitialAuditRunRecord(
  input: Readonly<{
    operationId: string;
    staleAfterMs: number;
    recordCount: number;
  }>,
  generation: number,
  auditTimeMs: number,
): FleetOperationRunRecord {
  const progress: FleetAuditProgress = {
    kind: 'audit',
    revision: 0,
    stage: { step: 'provider-findings', rowOrdinal: 0 },
    generation,
    auditTimeMs,
    staleAfterMs: input.staleAfterMs,
    recordCount: input.recordCount,
    findingCount: 0,
    factCount: 0,
  };
  return {
    version: 1,
    operationId: input.operationId,
    kind: 'audit',
    state: 'running',
    progress,
    updatedAt: new Date(auditTimeMs).toISOString(),
  };
}

async function failAudit(
  options: AdvanceFleetAuditOptions,
  lease: FleetOperationLease,
  run: FleetOperationRunRecord,
  progress: FleetAuditProgress,
  failure: FleetOperationFailure,
): Promise<FleetAuditAdvanceResult> {
  const newProgress: FleetAuditProgress = {
    ...progress,
    revision: progress.revision + 1,
    failure,
  };
  await lease.failOperation({
    operationId: run.operationId,
    expectedRevision: progress.revision,
    runRecord: {
      ...run,
      state: 'failed',
      progress: newProgress,
      updatedAt: new Date().toISOString(),
    },
  });
  await options.inventoryStore.releasePin({
    generation: progress.generation,
    pinnedBy: pinnedBy(run.operationId),
  });
  return resultFromRun({
    ...run,
    state: 'failed',
    progress: newProgress,
  });
}

/** The one durable `emission-bound-exceeded` refusal every staging site takes. */
function failEmission(
  options: AdvanceFleetAuditOptions,
  lease: FleetOperationLease,
  run: FleetOperationRunRecord,
  progress: FleetAuditProgress,
  itemOrdinal?: number,
): Promise<FleetAuditAdvanceResult> {
  return failAudit(options, lease, run, progress, {
    reason: 'emission-bound-exceeded',
    ...(itemOrdinal === undefined ? {} : { itemOrdinal }),
  });
}

/** Every owner claim the record step added, in map order. */
function* ownedFactPayloads(
  factKind: 'database-owner' | 'namespace-owner',
  owners: ReadonlyMap<string, FleetRecord>,
  before: ReadonlySet<string>,
): Generator<FleetAuditFactPayload> {
  for (const [key, owner] of owners) {
    if (before.has(key)) continue;
    yield {
      factKind,
      key,
      tenantTag: owner.tenantTag,
      environment: owner.environment,
    };
  }
}

/** Every duplicate-namespace collision the record step added, in set order. */
function* duplicateNamespaceFactPayloads(
  keys: ReadonlySet<string>,
  before: ReadonlySet<string>,
): Generator<FleetAuditFactPayload> {
  for (const key of keys) {
    if (before.has(key)) continue;
    yield { factKind: 'duplicate-namespace', key };
  }
}

async function finalizeAudit(
  lease: FleetOperationLease,
  run: FleetOperationRunRecord,
  progress: FleetAuditProgress,
): Promise<FleetAuditAdvanceResult> {
  const newProgress: FleetAuditProgress = {
    ...progress,
    revision: progress.revision + 1,
  };
  const finalized = await lease.finalizeOperation({
    operationId: run.operationId,
    expectedRevision: progress.revision,
    runRecord: {
      ...run,
      state: 'finalized',
      progress: newProgress,
      updatedAt: new Date().toISOString(),
    },
    expectedRowCounts: {
      finding: progress.findingCount,
      record: progress.recordCount,
      fact: progress.factCount,
    },
  });
  return resultFromRun(finalized);
}

async function advancePerRecordChunk(
  options: AdvanceFleetAuditOptions,
  lease: FleetOperationLease,
  run: FleetOperationRunRecord,
  progress: FleetAuditProgress,
  stage: PerRecordAuditStage,
  inventory: FleetResourceInventory,
  records: readonly FleetRecord[],
  auditedRecords: readonly FleetRecord[],
): Promise<FleetAuditAdvanceResult> {
  const record = records[stage.recordOrdinal] as FleetRecord;
  const recordsByScript = fleetAuditRecordsByScript(auditedRecords);
  const liveByScript = fleetAuditLiveByScript(inventory.deployments);
  const liveRoutesByHostname = fleetAuditLiveRoutesByHostname(inventory.routes);
  const recordByKey = new Map<string, FleetRecord>(
    records.map((entry) => [`${entry.tenantTag}:${entry.environment}`, entry]),
  );

  const factRows = await readAllFleetOperationRows(
    options.operationStore,
    run.operationId,
    'fact',
  );
  const facts = factRows.map((row) =>
    fleetAuditFactRowFromUnknown(row.payload),
  );
  const databases = new Map<string, FleetRecord>();
  const liveNamespaceOwners = new Map<string, FleetRecord>();
  const duplicateNamespaceIds = new Set(
    fleetAuditRecordsDerivedDuplicateNamespaceIds(auditedRecords),
  );
  // A staged owner fact whose (tenantTag, environment) pair resolves to no
  // record row is dropped rather than refused. Unreachability is the WHOLE
  // safety argument: every owner fact this coordinator writes names a record
  // it staged in the same operation, and both row sets are read back in full
  // and contiguity-checked before this loop. Were it reachable it would
  // SUPPRESS findings, not add them — a missing claimant leaves the database
  // or namespace looking unclaimed, so `auditRecordStep` takes this record as
  // the first owner and emits no duplicate finding. `malformed()` on the miss
  // is the stricter alternative and was left out as a behavior change on a
  // converged checkpoint.
  for (const fact of facts) {
    if (fact.factKind === 'database-owner') {
      const owner = recordByKey.get(`${fact.tenantTag}:${fact.environment}`);
      if (owner) databases.set(fact.key, owner);
    } else if (fact.factKind === 'namespace-owner') {
      const owner = recordByKey.get(`${fact.tenantTag}:${fact.environment}`);
      if (owner) liveNamespaceOwners.set(fact.key, owner);
    } else {
      duplicateNamespaceIds.add(fact.key);
    }
  }
  const databasesBefore = new Set(databases.keys());
  const namespaceOwnersBefore = new Set(liveNamespaceOwners.keys());
  const duplicatesBefore = new Set(duplicateNamespaceIds);

  options.signal?.throwIfAborted();
  const result = await auditRecordStep({
    record,
    recordsByScript,
    liveByScript,
    liveRoutesByHostname,
    inventoryDatabaseIds: inventory.databaseIds,
    hostRoutingKvId: inventory.hostRoutingKvId,
    databases,
    liveNamespaceOwners,
    duplicateNamespaceIds,
    backendFor: options.backendFor,
    specFor: options.specFor,
    maintenanceSecretFor: options.maintenanceSecretFor,
    store: options.fleetStore,
    staleAfterMs: progress.staleAfterMs,
    auditNow: progress.auditTimeMs,
    authorityNowProvider: options.authorityClock ?? Date.now,
  });

  // The three fact kinds differ only in their source collection and payload
  // shape, so each yields its newly claimed payloads and one loop stages them.
  const newFactPayloads: readonly Iterable<FleetAuditFactPayload>[] = [
    ownedFactPayloads('database-owner', databases, databasesBefore),
    ownedFactPayloads(
      'namespace-owner',
      liveNamespaceOwners,
      namespaceOwnersBefore,
    ),
    duplicateNamespaceFactPayloads(duplicateNamespaceIds, duplicatesBefore),
  ];
  const newFacts: FleetOperationStagedRow[] = [];
  let factOrdinal = progress.factCount;
  for (const payloads of newFactPayloads) {
    for (const payload of payloads) {
      const row = stagedAuditRow('fact', factOrdinal++, payload);
      if (!row) {
        return failEmission(options, lease, run, progress, stage.recordOrdinal);
      }
      newFacts.push(row);
    }
  }

  const findingRows: FleetOperationStagedRow[] = [];
  for (const [index, finding] of result.findings.entries()) {
    const row = stagedAuditRow(
      'finding',
      progress.findingCount + index,
      findingRowWithGatedDetail(finding),
    );
    if (!row) {
      // Unreachable: the gate never shortens a detail, it SUBSTITUTES the
      // fixed withheld fallback whenever `isDurableAuditDetailSafe` rejects
      // one — which includes every detail over the 4 KiB string bound. What
      // reaches the row is therefore either an already-bounded detail or a
      // short fixed string, beside grammar-bounded identifiers, so the
      // composed payload cannot exceed the staged-row envelope.
      return failEmission(options, lease, run, progress, stage.recordOrdinal);
    }
    findingRows.push(row);
  }

  // §5.5 DETECTION MECHANISM: the coordinator computes the batch-budget
  // overflow itself and never lets the store's own guard fire.
  if (
    findingRows.length + newFacts.length + 1 >
    FLEET_OPERATION_STAGE_BATCH_STATEMENTS
  ) {
    return failEmission(options, lease, run, progress, stage.recordOrdinal);
  }

  const newProgress: FleetAuditProgress = {
    ...progress,
    revision: progress.revision + 1,
    stage: { step: 'per-record', recordOrdinal: stage.recordOrdinal + 1 },
    findingCount: progress.findingCount + findingRows.length,
    factCount: progress.factCount + newFacts.length,
  };
  const committed = await lease.commitProgress({
    operationId: run.operationId,
    expectedRevision: progress.revision,
    runRecord: {
      ...run,
      progress: newProgress,
      updatedAt: new Date().toISOString(),
    },
    rows: [...findingRows, ...newFacts],
    expectedRowWatermarks: {
      finding: newProgress.findingCount,
      fact: newProgress.factCount,
    },
  });
  return pendingFromCommitted(committed);
}

async function advanceOneChunk(
  options: AdvanceFleetAuditOptions,
  lease: FleetOperationLease,
  run: FleetOperationRunRecord,
  maxItemsPerCall: number,
): Promise<FleetAuditAdvanceResult> {
  await lease.assertOwned();
  const progress = fleetAuditProgressFromUnknown(run.progress);
  if (progress.stage.step === 'finalize') {
    return finalizeAudit(lease, run, progress);
  }
  let inventory: FleetResourceInventory;
  try {
    inventory = await readFleetInventoryGeneration(
      options.inventoryStore,
      progress.generation,
    );
  } catch {
    return failAudit(options, lease, run, progress, {
      reason: 'generation-unavailable',
    });
  }
  const recordRows = await readAllFleetOperationRows(
    options.operationStore,
    run.operationId,
    'record',
  );
  let records: readonly FleetRecord[];
  try {
    records = recordRows.map(
      (row) =>
        structuralBackendSwitchFleetRecordFromUnknown(row.payload).record,
    );
  } catch {
    return malformed();
  }
  if (progress.stage.step === 'per-record') {
    const perRecordStage = progress.stage;
    if (perRecordStage.recordOrdinal > records.length) return malformed();
    if (perRecordStage.recordOrdinal === records.length) {
      // Unlike a global stage, `per-record` does persist a cursor equal to its
      // source length and spends a whole extra stage-running call — a full
      // generation re-read and a record-row re-page included — on the pure
      // transition to `finalize`. That is deliberate: the call is the leading 1
      // in the documented per-call cost formula.
      const newProgress: FleetAuditProgress = {
        ...progress,
        revision: progress.revision + 1,
        stage: nextAuditStage(perRecordStage, true),
      };
      const committed = await lease.commitProgress({
        operationId: run.operationId,
        expectedRevision: progress.revision,
        runRecord: {
          ...run,
          progress: newProgress,
          updatedAt: new Date().toISOString(),
        },
      });
      return pendingFromCommitted(committed);
    }
  }
  const auditedRecords = fleetAuditAuditedRecords(records);

  if (progress.stage.step === 'per-record') {
    return advancePerRecordChunk(
      options,
      lease,
      run,
      progress,
      progress.stage,
      inventory,
      records,
      auditedRecords,
    );
  }

  const chunk = advanceGlobalStage(progress.stage, maxItemsPerCall, {
    inventory,
    auditedRecords,
    known: fleetAuditKnownSets(records),
    recordsByScript: fleetAuditRecordsByScript(auditedRecords),
  });
  const findingRows: FleetOperationStagedRow[] = [];
  for (const [index, finding] of chunk.findings.entries()) {
    const row = stagedAuditRow(
      'finding',
      progress.findingCount + index,
      findingRowWithGatedDetail(finding),
    );
    if (!row) {
      // A global finding ordinal can exceed FLEET_OPERATION_ITEM_BOUND, which
      // fleetOperationFailureFromUnknown rejects as an itemOrdinal, so this
      // failure carries no ordinal.
      return failEmission(options, lease, run, progress);
    }
    findingRows.push(row);
  }
  await lease.stageRows({
    operationId: run.operationId,
    expectedRevision: progress.revision,
    rows: findingRows,
  });
  const newFindingCount = progress.findingCount + findingRows.length;
  const newProgress: FleetAuditProgress = {
    ...progress,
    revision: progress.revision + 1,
    stage: chunk.stage,
    findingCount: newFindingCount,
  };
  const committed = await lease.commitProgress({
    operationId: run.operationId,
    expectedRevision: progress.revision,
    runRecord: {
      ...run,
      progress: newProgress,
      updatedAt: new Date().toISOString(),
    },
    expectedRowWatermarks: { finding: newFindingCount },
  });
  return pendingFromCommitted(committed);
}

async function startAudit(
  options: AdvanceFleetAuditOptions,
  action: Extract<FleetAuditAdvanceAction, { kind: 'start' }>,
): Promise<FleetAuditAdvanceResult> {
  const operationId = action.operationId;
  assertFleetOperationId(operationId);
  const staleAfterMs = action.staleAfterMs;
  if (!Number.isSafeInteger(staleAfterMs) || staleAfterMs < 1) {
    throw new Error('staleAfterMs must be a positive safe integer');
  }
  const requestedGeneration = action.generation;
  if (
    requestedGeneration !== undefined &&
    (!Number.isSafeInteger(requestedGeneration) || requestedGeneration < 1)
  ) {
    throw new Error('generation must be a positive safe integer');
  }
  const inputRecords = action.records;
  if (inputRecords.length > FLEET_OPERATION_ITEM_BOUND) {
    throw new Error(RECORD_COUNT_MESSAGE);
  }
  if (
    inputRecords.some((record) => record === null || typeof record !== 'object')
  ) {
    throw new Error('fleet audit record exceeds the intake structure bounds');
  }
  const intake = fleetOperationItemsIntake({
    envelope: {
      staleAfterMs,
      generation: requestedGeneration ?? null,
    },
    items: inputRecords,
    itemByteBound: FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  });
  if ('reason' in intake) {
    const { reason } = intake;
    switch (reason) {
      case 'item-count':
        // The coordinator count check precedes the intake, so this arm is
        // unreachable here; the helper retains it for other callers.
        throw new Error(RECORD_COUNT_MESSAGE);
      case 'item-structure':
        throw new Error(
          'fleet audit record exceeds the intake structure bounds',
        );
      case 'item-bytes':
        throw new Error('fleet audit record exceeds the staged row byte bound');
      case 'aggregate-bytes':
        throw new Error(
          'fleet audit start canonical intake exceeds the intake byte bound',
        );
      default: {
        const exhaustive: never = reason;
        // A widened refusal union must fail closed here rather than let a
        // bare string escape as this function's result.
        throw new Error(
          `unexpected fleet operation intake refusal '${String(exhaustive)}'`,
        );
      }
    }
  }
  const intakeDigest = intake.digest;
  const records = intake.items;
  // Narrows `unknown` intake items to `FleetRecord` on `tenantTag` and
  // `environment` alone, so the array asserts more shape than is checked.
  // Every entry is staged whole by the `payload` cast below, and fields
  // beyond those two are re-read only by the `advanceOneChunk` decode.
  if (
    !records.every((record): record is FleetRecord => {
      if (record === null || typeof record !== 'object') return false;
      const candidate = record as Record<string, unknown>;
      return (
        typeof candidate.tenantTag === 'string' &&
        isDeploymentTenantTag(candidate.tenantTag) &&
        typeof candidate.environment === 'string' &&
        isDeploymentEnvironment(candidate.environment)
      );
    })
  ) {
    throw new Error(
      'fleet audit record tenantTag and environment must satisfy the deployment identifier grammar',
    );
  }
  return options.operationStore.withAccountOperationLease(
    'audit',
    async (lease) => {
      await lease.assertOwned();
      const probed =
        await options.operationStore.readOperationById(operationId);
      let generation: number;
      if (probed) {
        if (probed.kind !== 'audit') {
          throw new Error(fleetOperationOtherKindMessage(operationId));
        }
        generation = fleetAuditProgressFromUnknown(probed.progress).generation;
      } else {
        const resolved =
          requestedGeneration ??
          (await options.inventoryStore.latestFinalizedGeneration())
            ?.generation;
        if (resolved === undefined) {
          throw new Error(
            'no finalized fleet inventory generation is available',
          );
        }
        generation = resolved;
      }
      const auditTimeMs = (options.auditClock ?? Date.now)();
      if (
        !fleetOperationSafeInteger(auditTimeMs) ||
        Number.isNaN(new Date(auditTimeMs).getTime())
      ) {
        throw new Error(
          'fleet audit auditClock sample must be a non-negative safe integer representable by Date',
        );
      }
      const initialRunRecord = buildInitialAuditRunRecord(
        {
          operationId,
          staleAfterMs,
          recordCount: records.length,
        },
        generation,
        auditTimeMs,
      );
      // Pre-persistence progress gate for coordinator-built state.
      fleetAuditProgressFromUnknown(initialRunRecord.progress);
      const started = await lease.startOperation({
        operationId,
        kind: 'audit',
        runRecord: initialRunRecord,
        intakeDigest,
      });
      if (started.outcome === 'adopted-terminal') {
        return resultFromRun(started.record);
      }
      const record = started.record;
      const recordProgress = fleetAuditProgressFromUnknown(record.progress);
      const pinGenerationValue =
        started.outcome === 'adopted-running'
          ? recordProgress.generation
          : generation;
      try {
        await options.inventoryStore.pinGeneration({
          generation: pinGenerationValue,
          pinnedBy: pinnedBy(operationId),
        });
      } catch {
        return failAudit(options, lease, record, recordProgress, {
          reason: 'generation-unavailable',
        });
      }
      const rows: FleetOperationStagedRow[] = records.map((entry, ordinal) => ({
        rowKind: 'record',
        ordinal,
        payload: entry as unknown as Record<string, unknown>,
      }));
      await lease.stageRows({
        operationId,
        expectedRevision: 0,
        rows,
      });
      const committedProgress: FleetAuditProgress = {
        ...recordProgress,
        revision: 1,
      };
      try {
        const committed = await lease.commitProgress({
          operationId,
          expectedRevision: 0,
          runRecord: {
            ...record,
            progress: committedProgress,
            updatedAt: new Date().toISOString(),
          },
          expectedRowWatermarks: { record: records.length },
        });
        return pendingFromCommitted(committed);
      } catch {
        // Every throw from the revision-1 replay resolves to the same answer,
        // and the catch is deliberately unnarrowed for that reason: for a
        // far-advanced running (or since-terminal) operation the CAS cannot
        // converge, and for a lost lease or a store fault the operation's
        // current authoritative state is still the only truthful reply. The
        // caller therefore receives exactly what a stale-token continue would
        // return (§5.5) — which is a report of durable state, never a claim
        // that this call succeeded. If no state can be read back at all, the
        // reads below throw rather than invent one.
        const current = await lease.readOperation(operationId);
        if (current) return resultFromRun(current);
        const persisted =
          await options.operationStore.readOperationById(operationId);
        if (persisted) return resultFromRun(persisted);
        throw new FleetOperationTokenOperationError(operationId);
      }
    },
  );
}

async function continueAudit(
  options: AdvanceFleetAuditOptions,
  token: unknown,
  maxItemsPerCall: number,
): Promise<FleetAuditAdvanceResult> {
  const parsed = parseFleetOperationToken(token);
  return options.operationStore.withAccountOperationLease(
    'audit',
    async (lease) => {
      await lease.assertOwned();
      let run = await lease.readOperation(parsed.operationId);
      if (!run) {
        const persisted = await options.operationStore.readOperationById(
          parsed.operationId,
        );
        if (!persisted) {
          throw new FleetOperationTokenOperationError(parsed.operationId);
        }
        run = persisted;
      }
      const classification = classifyFleetOperationToken(parsed, run, 'audit');
      if (classification === 'stale' || run.state !== 'running') {
        return resultFromRun(run);
      }
      return advanceOneChunk(options, lease, run, maxItemsPerCall);
    },
  );
}

/**
 * Performs at most ONE bounded audit stage chunk against the durable
 * operation store, then returns the authoritative token. Provider work is
 * reached only through the injected resolvers, so this coordinator stays
 * transport-neutral.
 *
 * A stale token returns the authoritative current result with ZERO
 * resolver/generation/provider work; a token ahead of the persisted
 * operation, a foreign-kind token, an unknown operation, and every missing
 * store capability all fail closed before any such work.
 */
export async function advanceFleetAudit(
  options: AdvanceFleetAuditOptions,
): Promise<FleetAuditAdvanceResult> {
  assertCapability(options.operationStore, 'operation-store');
  assertCapability(options.inventoryStore, 'generation-read');
  assertCapability(options.inventoryStore, 'generation-pin');
  options.signal?.throwIfAborted();
  const maxItemsPerCall = options.maxItemsPerCall ?? DEFAULT_MAX_ITEMS_PER_CALL;
  assertMaxItemsPerCall(maxItemsPerCall);
  const action = options.action;
  if (action.kind === 'start') {
    return startAudit(options, action);
  }
  return continueAudit(options, action.token, maxItemsPerCall);
}

/**
 * Reads one page of an operation's parsed drift findings. Terminal-only
 * (failed operations included); never touches the inventory store. The
 * findings come back in ordinal order whatever order the store's page
 * arrived in (the port lets a page arrive unordered).
 *
 * A caller pages the whole set with
 * `afterOrdinal = (afterOrdinal ?? -1) + findings.length` until `done`. That
 * idiom rests on the assumption `FleetOperationStore.readOperationRowsPage`
 * states as a conformance requirement: finding ordinals are contiguous from
 * zero, and a page holds the smallest qualifying ordinals. This reader
 * checks the assumption instead of trusting it, so a non-conforming store
 * cannot spin the loop forever.
 *
 * Refuses an unknown operation with `FleetOperationTokenOperationError`, an
 * operation of the other kind and a still-running operation with fixed
 * messages, and a non-conforming page — empty while unfinished, or not the
 * contiguous ordinal run following the cursor — with `malformed()`. `limit`
 * is deliberately NOT range-checked here: it is forwarded to the store, whose
 * own read guard owns that range. `maxItemsPerCall` is validated in this
 * module by contrast, because it drives this module's own chunking rather
 * than a store call.
 */
export async function readFleetAuditFindingsPage(
  store: FleetOperationStore,
  input: Readonly<{
    operationId: string;
    afterOrdinal?: number;
    limit: number;
  }>,
): Promise<Readonly<{ findings: readonly DriftFinding[]; done: boolean }>> {
  const { operationId, afterOrdinal, limit } = input;
  const run = await store.readOperationById(operationId);
  if (!run) {
    throw new FleetOperationTokenOperationError(operationId);
  }
  if (run.kind !== 'audit') {
    throw new Error(fleetOperationOtherKindMessage(operationId));
  }
  if (run.state === 'running') {
    throw new Error(`fleet audit operation '${operationId}' is not terminal`);
  }
  const page = await store.readOperationRowsPage({
    operationId,
    rowKind: 'finding',
    limit,
    ...(afterOrdinal === undefined ? {} : { afterOrdinal }),
  });
  if (page.rows.length === 0 && !page.done) return malformed();
  const sortedRows = [...page.rows].sort(
    (left, right) => left.ordinal - right.ordinal,
  );
  const firstOrdinal = (afterOrdinal ?? -1) + 1;
  for (const [index, row] of sortedRows.entries()) {
    if (row.ordinal !== firstOrdinal + index) return malformed();
  }
  return {
    findings: sortedRows.map((row) => driftFindingRowFromUnknown(row.payload)),
    done: page.done,
  };
}

/**
 * Unblocks a stuck RUNNING audit operation, or releases any surviving pin on
 * an already-terminal one (§5.5 ABANDONMENT). Idempotent throughout.
 */
export async function abandonFleetAuditOperation(
  input: Readonly<{
    operationStore: FleetOperationStore;
    inventoryStore: FleetInventoryRunStore;
    operationId: string;
  }>,
): Promise<void> {
  const { operationStore, inventoryStore, operationId } = input;
  await operationStore.withAccountOperationLease('audit', async (lease) => {
    const run = await lease.readOperation(operationId);
    if (run && run.state === 'running') {
      if (run.kind !== 'audit') {
        throw new Error(fleetOperationOtherKindMessage(operationId));
      }
      const progress = fleetAuditProgressFromUnknown(run.progress);
      const newProgress: FleetAuditProgress = {
        ...progress,
        revision: progress.revision + 1,
        failure: { reason: 'operator-abandoned' },
      };
      await lease.failOperation({
        operationId,
        expectedRevision: progress.revision,
        runRecord: {
          ...run,
          state: 'failed',
          progress: newProgress,
          updatedAt: new Date().toISOString(),
        },
      });
      await inventoryStore.releasePin({
        generation: progress.generation,
        pinnedBy: pinnedBy(operationId),
      });
      return;
    }
    const persisted =
      run ?? (await operationStore.readOperationById(operationId));
    if (!persisted) {
      throw new FleetOperationTokenOperationError(operationId);
    }
    if (persisted.kind !== 'audit') {
      throw new Error(fleetOperationOtherKindMessage(operationId));
    }
    const progress = fleetAuditProgressFromUnknown(persisted.progress);
    await inventoryStore.releasePin({
      generation: progress.generation,
      pinnedBy: pinnedBy(operationId),
    });
  });
}
