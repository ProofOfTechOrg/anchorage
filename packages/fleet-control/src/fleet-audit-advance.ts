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
  type FleetOperationStore,
  type FleetOperationToken,
  FleetOperationTokenOperationError,
  fleetOperationItemsIntake,
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
const OTHER_OPERATION_KIND_MESSAGE = (operationId: string): string =>
  `fleet operation '${operationId}' belongs to the other operation kind`;

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
  | Readonly<{
      status: 'pending';
      token: FleetOperationToken;
      stage: FleetAuditStage;
    }>
  | Readonly<{
      status: 'complete';
      token: FleetOperationToken;
      result: FleetAuditResultRef;
    }>
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
    throw new Error('maxItemsPerCall must be an integer from 1 to 2000');
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

type IsSubset<Left, Right> = [Left] extends [Right] ? true : false;
const DRIFT_FINDING_KINDS_ARE_AUDIT_KINDS: IsSubset<
  DriftFinding['kind'],
  FleetAuditFindingKind
> = true;
const AUDIT_FINDING_KINDS_ARE_DRIFT_KINDS: IsSubset<
  FleetAuditFindingKind,
  DriftFinding['kind']
> = true;

function fleetAuditFindingKind(
  kind: DriftFinding['kind'],
): FleetAuditFindingKind {
  void DRIFT_FINDING_KINDS_ARE_AUDIT_KINDS;
  void AUDIT_FINDING_KINDS_ARE_DRIFT_KINDS;
  return kind;
}

/** Non-throwing durable write gate (§5.1/§6.4): the bounded path gates; the drain never does. */
function sanitizedFindingRow(finding: DriftFinding): DriftFindingRowPayload {
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
 * Returns `undefined` when the payload exceeds the staged-row envelope; the
 * caller then fails the operation durably. Throws `malformed()` on corruption.
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
      payload: validated as unknown as Record<string, unknown>,
    };
  } catch {
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

interface GlobalStageContext {
  readonly inventory: FleetResourceInventory;
  readonly auditedRecords: readonly FleetRecord[];
  readonly known: ReturnType<typeof fleetAuditKnownSets>;
  readonly recordsByScript: ReturnType<typeof fleetAuditRecordsByScript>;
}

function chunked<T>(
  stage: GlobalAuditStage,
  maxItemsPerCall: number,
  source: readonly T[],
  emit: (slice: readonly T[], ordinal: number) => readonly DriftFinding[],
): GlobalStageChunk {
  const ordinal =
    'rowOrdinal' in stage
      ? stage.rowOrdinal
      : 'auditedOrdinal' in stage
        ? stage.auditedOrdinal
        : stage.expectedOrdinal;
  if (
    ordinal > source.length ||
    (source.length > 0 && ordinal === source.length)
  ) {
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

interface GlobalStageDescriptor {
  readonly advance: (
    stage: GlobalAuditStage,
    maxItemsPerCall: number,
    context: GlobalStageContext,
  ) => GlobalStageChunk;
}

function globalStageDescriptor<T>(
  source: (context: GlobalStageContext) => readonly T[],
  emit: (
    slice: readonly T[],
    context: GlobalStageContext,
    ordinal: number,
  ) => readonly DriftFinding[],
): GlobalStageDescriptor {
  return {
    advance: (stage, maxItemsPerCall, context) =>
      chunked(stage, maxItemsPerCall, source(context), (slice, ordinal) =>
        emit(slice, context, ordinal),
      ),
  };
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
    (context) => context.inventory.r2Buckets ?? [],
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
        r2Buckets: context.inventory.r2Buckets ?? [],
      }),
  ),
} satisfies Readonly<Record<GlobalAuditStep, GlobalStageDescriptor>>);

/** Advances one bounded chunk of the current global stage. */
function advanceGlobalStage(
  stage: GlobalAuditStage,
  maxItemsPerCall: number,
  inventory: FleetResourceInventory,
  auditedRecords: readonly FleetRecord[],
  known: ReturnType<typeof fleetAuditKnownSets>,
  recordsByScript: ReturnType<typeof fleetAuditRecordsByScript>,
): GlobalStageChunk {
  return GLOBAL_STAGE_DESCRIPTORS[stage.step].advance(stage, maxItemsPerCall, {
    inventory,
    auditedRecords,
    known,
    recordsByScript,
  });
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
  inventory: FleetResourceInventory,
  records: readonly FleetRecord[],
  auditedRecords: readonly FleetRecord[],
): Promise<FleetAuditAdvanceResult> {
  const stage = progress.stage as Extract<
    FleetAuditStage,
    { step: 'per-record' }
  >;
  if (stage.recordOrdinal > records.length) return malformed();
  if (stage.recordOrdinal === records.length) {
    const newProgress: FleetAuditProgress = {
      ...progress,
      revision: progress.revision + 1,
      stage: nextAuditStage(stage, true),
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
    const committedProgress = fleetAuditProgressFromUnknown(committed.progress);
    return {
      status: 'pending',
      token: fleetOperationTokenOf(committed),
      stage: committedProgress.stage,
    };
  }
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

  if (options.signal?.aborted) options.signal.throwIfAborted();
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
    authorityNowProvider: options.authorityClock ?? (() => Date.now()),
  });

  const newFacts: FleetOperationStagedRow[] = [];
  let factOrdinal = progress.factCount;
  for (const [key, owner] of databases) {
    if (!databasesBefore.has(key)) {
      const payload: FleetAuditFactPayload = {
        factKind: 'database-owner',
        key,
        tenantTag: owner.tenantTag,
        environment: owner.environment,
      };
      const row = stagedAuditRow('fact', factOrdinal++, payload);
      if (!row) {
        return failAudit(options, lease, run, progress, {
          reason: 'emission-bound-exceeded',
          itemOrdinal: stage.recordOrdinal,
        });
      }
      newFacts.push(row);
    }
  }
  for (const [key, owner] of liveNamespaceOwners) {
    if (!namespaceOwnersBefore.has(key)) {
      const payload: FleetAuditFactPayload = {
        factKind: 'namespace-owner',
        key,
        tenantTag: owner.tenantTag,
        environment: owner.environment,
      };
      const row = stagedAuditRow('fact', factOrdinal++, payload);
      if (!row) {
        return failAudit(options, lease, run, progress, {
          reason: 'emission-bound-exceeded',
          itemOrdinal: stage.recordOrdinal,
        });
      }
      newFacts.push(row);
    }
  }
  for (const key of duplicateNamespaceIds) {
    if (!duplicatesBefore.has(key)) {
      const payload: FleetAuditFactPayload = {
        factKind: 'duplicate-namespace',
        key,
      };
      const row = stagedAuditRow('fact', factOrdinal++, payload);
      if (!row) {
        return failAudit(options, lease, run, progress, {
          reason: 'emission-bound-exceeded',
          itemOrdinal: stage.recordOrdinal,
        });
      }
      newFacts.push(row);
    }
  }

  const findingRows: FleetOperationStagedRow[] = [];
  for (const [index, finding] of result.findings.entries()) {
    const row = stagedAuditRow(
      'finding',
      progress.findingCount + index,
      sanitizedFindingRow(finding),
    );
    if (!row) {
      // Unreachable: sanitizedFindingRow caps every detail at 4 KiB and strips
      // controls before measurement, while record identifiers are grammar-bounded.
      return failAudit(options, lease, run, progress, {
        reason: 'emission-bound-exceeded',
        itemOrdinal: stage.recordOrdinal,
      });
    }
    findingRows.push(row);
  }

  // §5.5 DETECTION MECHANISM: the coordinator computes the batch-budget
  // overflow itself and never lets the store's own guard fire.
  if (
    findingRows.length + newFacts.length + 1 >
    FLEET_OPERATION_STAGE_BATCH_STATEMENTS
  ) {
    return failAudit(options, lease, run, progress, {
      reason: 'emission-bound-exceeded',
      itemOrdinal: stage.recordOrdinal,
    });
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
  const committedProgress = fleetAuditProgressFromUnknown(committed.progress);
  return {
    status: 'pending',
    token: fleetOperationTokenOf(committed),
    stage: committedProgress.stage,
  };
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
  const auditedRecords = fleetAuditAuditedRecords(records);

  if (progress.stage.step === 'per-record') {
    return advancePerRecordChunk(
      options,
      lease,
      run,
      progress,
      inventory,
      records,
      auditedRecords,
    );
  }

  const known = fleetAuditKnownSets(records);
  const recordsByScript = fleetAuditRecordsByScript(auditedRecords);
  const chunk = advanceGlobalStage(
    progress.stage,
    maxItemsPerCall,
    inventory,
    auditedRecords,
    known,
    recordsByScript,
  );
  const findingRows: FleetOperationStagedRow[] = [];
  for (const [index, finding] of chunk.findings.entries()) {
    const row = stagedAuditRow(
      'finding',
      progress.findingCount + index,
      sanitizedFindingRow(finding),
    );
    if (!row) {
      // A global finding ordinal can exceed FLEET_OPERATION_ITEM_BOUND, which
      // fleetOperationFailureFromUnknown rejects as an itemOrdinal.
      return failAudit(options, lease, run, progress, {
        reason: 'emission-bound-exceeded',
      });
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
  const committedProgress = fleetAuditProgressFromUnknown(committed.progress);
  return {
    status: 'pending',
    token: fleetOperationTokenOf(committed),
    stage: committedProgress.stage,
  };
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
    throw new Error(
      `fleet audit start accepts at most ${FLEET_OPERATION_ITEM_BOUND} records`,
    );
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
        // The coordinator count check precedes the grammar check, so this arm
        // is unreachable here; the helper retains it for other callers.
        throw new Error(
          `fleet audit start accepts at most ${FLEET_OPERATION_ITEM_BOUND} records`,
        );
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
        return exhaustive;
      }
    }
  }
  const intakeDigest = intake.digest;
  const records = intake.items;
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
          throw new Error(OTHER_OPERATION_KIND_MESSAGE(operationId));
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
        ...fleetAuditProgressFromUnknown(record.progress),
        generation: pinGenerationValue,
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
        const finalProgress = fleetAuditProgressFromUnknown(committed.progress);
        return {
          status: 'pending',
          token: fleetOperationTokenOf(committed),
          stage: finalProgress.stage,
        };
      } catch {
        // A far-advanced running (or since-terminal) operation: the revision-1
        // replay cannot converge, so the caller receives the current
        // authoritative state exactly as a stale-token continue would (§5.5).
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
  if (options.action.kind === 'start') {
    return startAudit(options, options.action);
  }
  return continueAudit(options, options.action.token, maxItemsPerCall);
}

/**
 * Reads one page of an operation's parsed drift findings. Terminal-only
 * (failed operations included); never touches the inventory store. The
 * findings come back in ordinal order whatever order the store's page
 * arrived in (the port lets a page arrive unordered). Finding ordinals are
 * contiguous from zero and a page holds the smallest qualifying ordinals,
 * so a caller pages the whole set with
 * `afterOrdinal = (afterOrdinal ?? -1) + findings.length` until `done`.
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
    throw new Error(OTHER_OPERATION_KIND_MESSAGE(operationId));
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
  return {
    findings: [...page.rows]
      .sort((left, right) => left.ordinal - right.ordinal)
      .map(
        (row) =>
          driftFindingRowFromUnknown(row.payload) as unknown as DriftFinding,
      ),
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
        throw new Error(OTHER_OPERATION_KIND_MESSAGE(operationId));
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
      throw new Error(OTHER_OPERATION_KIND_MESSAGE(operationId));
    }
    const progress = fleetAuditProgressFromUnknown(persisted.progress);
    await inventoryStore.releasePin({
      generation: progress.generation,
      pinnedBy: pinnedBy(operationId),
    });
  });
}
