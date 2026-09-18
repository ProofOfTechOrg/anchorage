// SPDX-License-Identifier: Apache-2.0

import type { AttestConvergedActiveRouteOptions } from './active-route.js';
import type { FinalizedOrdinaryStateProvider } from './backend-switch.js';
import {
  isDeploymentEnvironment,
  isDeploymentTenantTag,
} from './deployment-context.js';
import {
  admitFleetMigrationItem,
  assertFleetMigrationPlanCompatibility,
  executeNextMigrationStep,
  revalidateFleetMigrationAdmission,
} from './fleet.js';
import {
  type FleetMigrationItem,
  type FleetMigrationProgress,
  fleetMigrationItemFromUnknown,
  fleetMigrationOperationRecordFromUnknown,
} from './fleet-migration-state.js';
import {
  assertFleetOperationId,
  canonicalFleetOperationBytes,
  classifyFleetOperationToken,
  FLEET_OPERATION_ITEM_BOUND,
  FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  FLEET_OPERATION_STORE_ADVANCE_MEMBERS,
  type FleetOperationFailure,
  type FleetOperationLease,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  FleetOperationStateError,
  type FleetOperationStore,
  type FleetOperationToken,
  FleetOperationTokenOperationError,
  fleetOperationIntakeDigest,
  fleetOperationItemsIntake,
  fleetOperationOtherKindMessage,
  fleetOperationTokenOf,
  malformed,
  parseFleetOperationToken,
  readAllFleetOperationRows,
} from './fleet-operation-state.js';
import type {
  DeploymentSecrets,
  DeploymentSpec,
  FleetRecord,
  FleetSettlementHost,
  FleetStateStore,
  ProvisioningBackend,
} from './types.js';

export type FleetMigrationAdvanceAction =
  | Readonly<{
      kind: 'start';
      operationId: string;
      records: readonly FleetRecord[];
      canaryTenantTags: readonly string[];
    }>
  | Readonly<{ kind: 'continue'; token: unknown }>;

export interface AdvanceFleetMigrationOptions {
  readonly operationStore: FleetOperationStore;
  readonly fleetStore: FleetStateStore;
  readonly backendFor: (record: FleetRecord) => ProvisioningBackend;
  readonly specFor: (record: FleetRecord) => DeploymentSpec;
  readonly secretsFor: (record: FleetRecord) => DeploymentSecrets;
  readonly finalizedStateProviderFor?: (
    record: FleetRecord,
  ) => FinalizedOrdinaryStateProvider | undefined;
  readonly settlementFor?: (
    record: FleetRecord,
  ) => FleetSettlementHost | undefined;
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  readonly clock?: () => number;
  /**
   * Call-local only; never persisted. Read at the entry and at the head of
   * each item advance; a step already in flight runs to completion.
   */
  readonly signal?: AbortSignal;
  /**
   * Runs on every call that returns `complete` — the call that finalizes the
   * operation, a later continue on the finalized operation, and a replayed
   * start carrying the same intake — after the finalization is durable and
   * before this call returns, with no operation lease held. Delivery follows
   * the durable outcome, so an abort raised after the call begins does not
   * suppress it. Delivery is therefore at least once; the host deduplicates
   * on `operationId`. A rejection propagates to the caller and leaves the
   * durable finalization intact.
   */
  readonly onComplete?: (
    result: FleetMigrationResultRef,
  ) => void | Promise<void>;
  readonly action: FleetMigrationAdvanceAction;
}

export interface FleetMigrationResultRef {
  readonly operationId: string;
  readonly itemCount: number;
  readonly completedItemCount: number;
  readonly finalizedAtMs: number;
}

export type FleetMigrationAdvanceResult =
  | Readonly<{
      status: 'pending';
      token: FleetOperationToken;
      itemOrdinal: number;
      planCursor?: number;
    }>
  | Readonly<{
      status: 'complete';
      token: FleetOperationToken;
      result: FleetMigrationResultRef;
    }>
  | Readonly<{
      status: 'failed';
      token: FleetOperationToken;
      failure: FleetOperationFailure;
    }>;

export type FleetMigrationAdvanceCapability = 'operation-store';

export class FleetMigrationAdvanceCapabilityError extends Error {
  readonly capability = 'operation-store';

  constructor() {
    super('fleet migration advance requires an operation store');
    this.name = 'FleetMigrationAdvanceCapabilityError';
  }
}

class FleetMigrationTargetDriftError extends Error {
  constructor() {
    super('fleet migration target specification changed after admission');
    this.name = 'FleetMigrationTargetDriftError';
  }
}

type MigrationRun = ReturnType<typeof fleetMigrationOperationRecordFromUnknown>;

function assertOperationStore(store: FleetOperationStore): void {
  for (const member of FLEET_OPERATION_STORE_ADVANCE_MEMBERS) {
    if (
      !store ||
      !Reflect.has(store, member) ||
      typeof store[member] !== 'function'
    ) {
      throw new FleetMigrationAdvanceCapabilityError();
    }
  }
}

function migrationRun(record: FleetOperationRunRecord): MigrationRun {
  const run = fleetMigrationOperationRecordFromUnknown(record);
  if (run.progress.completedItemCount !== run.progress.activeItemOrdinal)
    return malformed();
  return run;
}

function itemRow(item: FleetMigrationItem): FleetOperationStagedRow {
  return {
    rowKind: 'item',
    ordinal: item.ordinal,
    payload: { ...fleetMigrationItemFromUnknown(item) },
  };
}

async function readItems(
  store: FleetOperationStore,
  run: MigrationRun,
): Promise<FleetMigrationItem[]> {
  const rows = await readAllFleetOperationRows(store, run.operationId, 'item');
  if (
    rows.length > run.progress.itemCount ||
    (run.progress.revision > 0 && rows.length !== run.progress.itemCount)
  )
    return malformed();
  return rows.map((row) => {
    const item = fleetMigrationItemFromUnknown(row.payload);
    if (row.rowKind !== 'item' || item.ordinal !== row.ordinal)
      return malformed();
    return item;
  });
}

async function resultFromRun(
  store: FleetOperationStore,
  record: FleetOperationRunRecord,
): Promise<FleetMigrationAdvanceResult> {
  const run = migrationRun(record);
  const token = fleetOperationTokenOf(run);
  const { itemCount, completedItemCount, activeItemOrdinal } = run.progress;
  if (run.state === 'failed') {
    if (!run.progress.failure) return malformed();
    return { status: 'failed', token, failure: run.progress.failure };
  }
  if (run.state === 'finalized') {
    if (run.terminalAtMs === undefined || completedItemCount !== itemCount)
      return malformed();
    return {
      status: 'complete',
      token,
      result: {
        operationId: run.operationId,
        itemCount,
        completedItemCount,
        finalizedAtMs: run.terminalAtMs,
      },
    };
  }
  if (run.progress.revision === 0 || activeItemOrdinal === itemCount) {
    return { status: 'pending', token, itemOrdinal: activeItemOrdinal };
  }
  const item = (await readItems(store, run))[activeItemOrdinal];
  if (!item || (item.status !== 'pending' && item.status !== 'active'))
    return malformed();
  return {
    status: 'pending',
    token,
    itemOrdinal: activeItemOrdinal,
    ...(item.planCursor === undefined ? {} : { planCursor: item.planCursor }),
  };
}

/**
 * Operation records use a fresh wall-clock updatedAt, independent of the
 * deployment clock. Unequal recomposed bytes conflict; equal bytes (including
 * coincident millisecond stamps) reach the store's row comparison. A batch's
 * own lost response and an identical-object replay retain their original
 * bytes. Every new continue call derives its next transition from storage.
 */
function progressedRun(
  run: MigrationRun,
  progress: FleetMigrationProgress,
): MigrationRun {
  return { ...run, progress, updatedAt: new Date().toISOString() };
}

async function failItem(
  lease: FleetOperationLease,
  run: MigrationRun,
  item: FleetMigrationItem | undefined,
  reason: FleetOperationFailure['reason'],
): Promise<void> {
  const failure: FleetOperationFailure = {
    reason,
    ...(item ? { itemOrdinal: item.ordinal } : {}),
  };
  await lease.failOperation({
    operationId: run.operationId,
    expectedRevision: run.progress.revision,
    runRecord: {
      ...progressedRun(run, {
        ...run.progress,
        revision: run.progress.revision + 1,
        failure,
      }),
      state: 'failed',
    },
    ...(item ? { updateRows: [itemRow({ ...item, status: 'failed' })] } : {}),
  });
}

async function startMigration(
  options: AdvanceFleetMigrationOptions,
  action: Extract<FleetMigrationAdvanceAction, { kind: 'start' }>,
): Promise<FleetMigrationAdvanceResult> {
  const operationId = action.operationId;
  assertFleetOperationId(operationId);
  const inputRecords = action.records;
  if (inputRecords.length > FLEET_OPERATION_ITEM_BOUND) {
    throw new Error(
      `fleet migration start accepts at most ${FLEET_OPERATION_ITEM_BOUND} records`,
    );
  }
  if (
    inputRecords.some((record) => record === null || typeof record !== 'object')
  ) {
    throw new Error(
      'fleet migration record exceeds the intake structure bounds',
    );
  }
  let canaryTenantTags: unknown;
  try {
    canaryTenantTags = JSON.parse(
      canonicalFleetOperationBytes(action.canaryTenantTags),
    );
  } catch (error) {
    if (!(error instanceof FleetOperationStateError)) throw error;
    throw new Error(
      'fleet migration canaryTenantTags exceed the intake structure or byte bounds',
    );
  }
  if (
    !Array.isArray(canaryTenantTags) ||
    !canaryTenantTags.every((tag): tag is string => typeof tag === 'string')
  ) {
    throw new Error(
      'fleet migration canaryTenantTags must be an array of strings',
    );
  }
  const intake = fleetOperationItemsIntake({
    envelope: { canaryTenantTags },
    items: inputRecords,
    itemByteBound: FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  });
  if ('reason' in intake) {
    switch (intake.reason) {
      case 'item-count':
        throw new Error(
          `fleet migration start accepts at most ${FLEET_OPERATION_ITEM_BOUND} records`,
        );
      case 'item-structure':
        throw new Error(
          `fleet migration record ${intake.itemOrdinal + 1} exceeds the intake structure bounds`,
        );
      case 'item-bytes':
        throw new Error(
          `fleet migration record ${intake.itemOrdinal + 1} exceeds the staged row byte bound`,
        );
      case 'aggregate-bytes':
        throw new Error(
          'fleet migration start canonical intake exceeds the intake byte bound',
        );
    }
  }
  const records = intake.items.map((record) => {
    if (!record || typeof record !== 'object') return malformed();
    const candidate = record as Record<string, unknown>;
    if (
      typeof candidate.tenantTag !== 'string' ||
      !isDeploymentTenantTag(candidate.tenantTag) ||
      typeof candidate.environment !== 'string' ||
      !isDeploymentEnvironment(candidate.environment)
    ) {
      throw new Error(
        'fleet migration record tenantTag and environment must satisfy the deployment identifier grammar',
      );
    }
    return {
      snapshot: record,
      tenantTag: candidate.tenantTag,
      environment: candidate.environment,
    };
  });
  const canaryOrder = new Map(
    canaryTenantTags.map((tag, index) => [tag, index]),
  );
  records.sort((a, b) => {
    const aCanary = canaryOrder.get(a.tenantTag);
    const bCanary = canaryOrder.get(b.tenantTag);
    if (aCanary !== undefined || bCanary !== undefined) {
      if (aCanary === undefined) return 1;
      if (bCanary === undefined) return -1;
      return aCanary - bCanary;
    }
    return `${a.tenantTag}:${a.environment}`.localeCompare(
      `${b.tenantTag}:${b.environment}`,
    );
  });
  const rows = records.map((record, ordinal) =>
    itemRow({
      ordinal,
      tenantTag: record.tenantTag,
      environment: record.environment,
      ...(canaryOrder.has(record.tenantTag)
        ? { canaryRank: canaryOrder.get(record.tenantTag) }
        : {}),
      entryRecordDigest: fleetOperationIntakeDigest(record.snapshot),
      status: 'pending',
    }),
  );
  return options.operationStore.withAccountOperationLease(
    'migration',
    async (lease) => {
      await lease.assertOwned();
      const started = await lease.startOperation({
        operationId,
        kind: 'migration',
        intakeDigest: intake.digest,
        runRecord: {
          version: 1,
          operationId,
          kind: 'migration',
          state: 'running',
          progress: {
            kind: 'migration',
            revision: 0,
            itemCount: rows.length,
            activeItemOrdinal: 0,
            completedItemCount: 0,
          } as FleetMigrationProgress,
          updatedAt: new Date().toISOString(),
        },
      });
      const run = migrationRun(started.record);
      if (started.outcome === 'adopted-terminal' || run.progress.revision > 0) {
        return resultFromRun(options.operationStore, run);
      }
      await lease.stageRows({ operationId, expectedRevision: 0, rows });
      const committed = await lease.commitProgress({
        operationId,
        expectedRevision: 0,
        runRecord: progressedRun(run, { ...run.progress, revision: 1 }),
        expectedRowWatermarks: { item: rows.length },
      });
      return resultFromRun(options.operationStore, committed);
    },
  );
}

async function advanceItem(
  options: AdvanceFleetMigrationOptions,
  operationLease: FleetOperationLease,
  run: MigrationRun,
  item: FleetMigrationItem,
): Promise<FleetMigrationAdvanceResult> {
  // Outside the try below: its catch durably fails the item, so an abort
  // raised there would turn a cancellation into a permanent failure.
  options.signal?.throwIfAborted();
  let next: FleetMigrationItem;
  try {
    const attestationOptions: AttestConvergedActiveRouteOptions = {
      clock: options.clock ?? Date.now,
      ...options.routeAttestation,
    };
    next = await options.fleetStore.withDeploymentLease(
      item.tenantTag,
      item.environment,
      async (lease) => {
        await operationLease.assertOwned();
        await lease.assertOwned();
        const deps = {
          store: options.fleetStore,
          backendFor: (record: FleetRecord) => options.backendFor(record),
          specFor: (record: FleetRecord) => options.specFor(record),
          secretsFor: (record: FleetRecord) => options.secretsFor(record),
          finalizedStateProviderFor: (record: FleetRecord) =>
            options.finalizedStateProviderFor?.(record),
          settlementFor: (record: FleetRecord) =>
            options.settlementFor?.(record),
          lease,
          attestationOptions,
          get clock() {
            return options.clock ?? Date.now;
          },
          ordinal: item.ordinal + 1,
        };
        if (item.status === 'pending') {
          const { admitted, plan } = await admitFleetMigrationItem(
            deps,
            item.tenantTag,
            item.environment,
          );
          return {
            ...item,
            targetSpecDigest: admitted.targetDigest,
            plan,
            planCursor: 0,
            status: 'active' as const,
          };
        }
        const { plan, planCursor, targetSpecDigest } = item;
        if (
          item.status !== 'active' ||
          plan === undefined ||
          planCursor === undefined ||
          targetSpecDigest === undefined
        )
          return malformed();
        const admission = await revalidateFleetMigrationAdmission(
          deps,
          plan,
          targetSpecDigest,
          item.tenantTag,
          item.environment,
        );
        if ('reason' in admission) throw new FleetMigrationTargetDriftError();
        const { admitted, reread } = admission;
        await assertFleetMigrationPlanCompatibility(
          admitted,
          { plan, planCursor },
          reread,
        );
        await operationLease.assertOwned();
        const step = await executeNextMigrationStep(
          admitted,
          plan,
          planCursor,
          { entry: reread, current: reread },
        );
        return {
          ...item,
          planCursor: planCursor + 1,
          status: step.done ? ('complete' as const) : ('active' as const),
        };
      },
    );
  } catch (error) {
    await failItem(
      operationLease,
      run,
      item,
      error instanceof FleetMigrationTargetDriftError
        ? 'target-drift'
        : 'item-failed',
    );
    throw error;
  }
  const complete = next.status === 'complete';
  const committed = await operationLease.commitProgress({
    operationId: run.operationId,
    expectedRevision: run.progress.revision,
    runRecord: progressedRun(run, {
      ...run.progress,
      revision: run.progress.revision + 1,
      activeItemOrdinal: run.progress.activeItemOrdinal + (complete ? 1 : 0),
      completedItemCount: run.progress.completedItemCount + (complete ? 1 : 0),
    }),
    updateRows: [itemRow(next)],
    expectedRowWatermarks: { item: run.progress.itemCount },
  });
  return resultFromRun(options.operationStore, committed);
}

async function continueMigration(
  options: AdvanceFleetMigrationOptions,
  token: unknown,
): Promise<FleetMigrationAdvanceResult> {
  const parsed = parseFleetOperationToken(token);
  return options.operationStore.withAccountOperationLease(
    'migration',
    async (lease) => {
      await lease.assertOwned();
      const record =
        (await lease.readOperation(parsed.operationId)) ??
        (await options.operationStore.readOperationById(parsed.operationId));
      const classification = classifyFleetOperationToken(
        parsed,
        record,
        'migration',
      );
      if (!record)
        throw new FleetOperationTokenOperationError(parsed.operationId);
      const run = migrationRun(record);
      if (
        classification === 'stale' ||
        run.state !== 'running' ||
        run.progress.revision === 0
      ) {
        return resultFromRun(options.operationStore, run);
      }
      if (run.progress.activeItemOrdinal === run.progress.itemCount) {
        const finalized = await lease.finalizeOperation({
          operationId: run.operationId,
          expectedRevision: run.progress.revision,
          runRecord: {
            ...progressedRun(run, {
              ...run.progress,
              revision: run.progress.revision + 1,
            }),
            state: 'finalized',
          },
          expectedRowCounts: { item: run.progress.itemCount },
          requireAllItemsComplete: true,
        });
        return resultFromRun(options.operationStore, finalized);
      }
      const item = (await readItems(options.operationStore, run))[
        run.progress.activeItemOrdinal
      ];
      if (!item) return malformed();
      return advanceItem(options, lease, run, item);
    },
  );
}

/** Runs one admission or one frozen migration step; callers own re-entry. */
export async function advanceFleetMigration(
  options: AdvanceFleetMigrationOptions,
): Promise<FleetMigrationAdvanceResult> {
  assertOperationStore(options.operationStore);
  options.signal?.throwIfAborted();
  const action = options.action;
  const result =
    action.kind === 'start'
      ? await startMigration(options, action)
      : await continueMigration(options, action.token);
  if (result.status === 'complete') await options.onComplete?.(result.result);
  return result;
}

/** Reads ordered item metadata, including while the operation is running. */
export async function readFleetMigrationItemsPage(
  store: FleetOperationStore,
  input: Readonly<{
    operationId: string;
    afterOrdinal?: number;
    limit: number;
  }>,
): Promise<Readonly<{ items: readonly FleetMigrationItem[]; done: boolean }>> {
  const { operationId, afterOrdinal, limit } = input;
  const run = await store.readOperationById(operationId);
  if (!run) throw new FleetOperationTokenOperationError(operationId);
  if (run.kind !== 'migration')
    throw new Error(fleetOperationOtherKindMessage(operationId));
  const page = await store.readOperationRowsPage({
    operationId,
    rowKind: 'item',
    limit,
    ...(afterOrdinal === undefined ? {} : { afterOrdinal }),
  });
  if (page.rows.length > limit || (!page.done && page.rows.length === 0))
    return malformed();
  const rows = [...page.rows].sort((a, b) => a.ordinal - b.ordinal);
  const items = rows.map((row, index) => {
    const item = fleetMigrationItemFromUnknown(row.payload);
    if (
      row.rowKind !== 'item' ||
      row.ordinal !== (afterOrdinal ?? -1) + 1 + index ||
      row.ordinal !== item.ordinal
    )
      return malformed();
    return item;
  });
  return { items, done: page.done };
}

/** Fails a running operation and its active item; terminal calls are no-ops. */
export async function abandonFleetMigrationOperation(
  input: Readonly<{ operationStore: FleetOperationStore; operationId: string }>,
): Promise<void> {
  const { operationStore, operationId } = input;
  await operationStore.withAccountOperationLease('migration', async (lease) => {
    await lease.assertOwned();
    const record =
      (await lease.readOperation(operationId)) ??
      (await operationStore.readOperationById(operationId));
    if (!record) throw new FleetOperationTokenOperationError(operationId);
    if (record.kind !== 'migration')
      throw new Error(fleetOperationOtherKindMessage(operationId));
    if (record.state !== 'running') return;
    const run = migrationRun(record);
    const item = (await readItems(operationStore, run))[
      run.progress.activeItemOrdinal
    ];
    await failItem(lease, run, item, 'operator-abandoned');
  });
}
