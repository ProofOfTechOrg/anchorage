// SPDX-License-Identifier: Apache-2.0

import {
  isDeploymentEnvironment,
  isDeploymentTenantTag,
} from './deployment-context.js';
import {
  assertFleetOperationExactKeys,
  FLEET_MIGRATION_PLAN_BOUND,
  FLEET_OPERATION_ITEM_BOUND,
  type FleetOperationProgress,
  type FleetOperationRunRecord,
  FleetOperationStateError,
  fleetOperationFailureFromUnknown,
  fleetOperationPlainRecord,
  fleetOperationRunRecordFromUnknown,
  fleetOperationSafeInteger,
  fleetOperationSha256,
} from './fleet-operation-state.js';

export const FLEET_MIGRATION_STEPS = Object.freeze([
  'retire-pre',
  'ready-target-backfill',
  'ready-platform-resources',
  'ready-maintenance',
  'ready-promote',
  'ready-attest-settle',
  'ready-retire-post',
  'admit-migrating',
  'assert-migrating',
  'platform-only-schema',
  'platform-only-resources',
  'platform-only-maintenance',
  'platform-only-promote',
  'platform-only-ready',
  'seed-identity',
  'apply-migrations',
  'migration-schema-applied',
  'platform-resources',
  'pending-topology',
  'deploy-candidate',
  'arm-maintenance',
  'promote',
  'settle-ready',
  'retire-post',
] as const);

export type FleetMigrationStep = (typeof FLEET_MIGRATION_STEPS)[number];

export type FleetMigrationPlanEntry = Readonly<{
  step: FleetMigrationStep;
  targetSchemaVersion?: number;
}>;

export interface FleetMigrationItem {
  readonly ordinal: number;
  readonly tenantTag: string;
  readonly environment: string;
  readonly canaryRank?: number;
  readonly entryRecordDigest: string;
  readonly targetSpecDigest?: string;
  readonly plan?: readonly FleetMigrationPlanEntry[];
  readonly planCursor?: number;
  readonly status: 'pending' | 'active' | 'complete' | 'failed';
}

export interface FleetMigrationProgress extends FleetOperationProgress {
  readonly kind: 'migration';
  readonly itemCount: number;
  readonly activeItemOrdinal: number;
  readonly completedItemCount: number;
}

function malformed(): never {
  throw new FleetOperationStateError();
}

export function fleetMigrationPlanEntryFromUnknown(
  value: unknown,
): FleetMigrationPlanEntry {
  const candidate = fleetOperationPlainRecord(value);
  assertFleetOperationExactKeys(candidate, ['step'], ['targetSchemaVersion']);
  if (
    typeof candidate.step !== 'string' ||
    !FLEET_MIGRATION_STEPS.includes(candidate.step as FleetMigrationStep) ||
    (candidate.step !== 'apply-migrations' &&
      candidate.targetSchemaVersion !== undefined) ||
    (candidate.targetSchemaVersion !== undefined &&
      !fleetOperationSafeInteger(candidate.targetSchemaVersion, 1))
  ) {
    return malformed();
  }
  return {
    step: candidate.step as FleetMigrationStep,
    ...(candidate.targetSchemaVersion === undefined
      ? {}
      : { targetSchemaVersion: candidate.targetSchemaVersion }),
  };
}

export function fleetMigrationItemFromUnknown(
  value: unknown,
): FleetMigrationItem {
  const candidate = fleetOperationPlainRecord(value);
  assertFleetOperationExactKeys(
    candidate,
    ['ordinal', 'tenantTag', 'environment', 'entryRecordDigest', 'status'],
    ['canaryRank', 'targetSpecDigest', 'plan', 'planCursor'],
  );
  if (
    !fleetOperationSafeInteger(candidate.ordinal) ||
    candidate.ordinal >= FLEET_OPERATION_ITEM_BOUND ||
    typeof candidate.tenantTag !== 'string' ||
    !isDeploymentTenantTag(candidate.tenantTag) ||
    typeof candidate.environment !== 'string' ||
    !isDeploymentEnvironment(candidate.environment) ||
    (candidate.canaryRank !== undefined &&
      (!fleetOperationSafeInteger(candidate.canaryRank) ||
        candidate.canaryRank >= FLEET_OPERATION_ITEM_BOUND)) ||
    !fleetOperationSha256(candidate.entryRecordDigest) ||
    (candidate.status !== 'pending' &&
      candidate.status !== 'active' &&
      candidate.status !== 'complete' &&
      candidate.status !== 'failed')
  ) {
    return malformed();
  }
  const pending = candidate.status === 'pending';
  if (
    pending !== (candidate.targetSpecDigest === undefined) ||
    pending !== (candidate.plan === undefined) ||
    pending !== (candidate.planCursor === undefined)
  ) {
    return malformed();
  }
  let plan: readonly FleetMigrationPlanEntry[] | undefined;
  if (!pending) {
    if (
      !fleetOperationSha256(candidate.targetSpecDigest) ||
      !Array.isArray(candidate.plan) ||
      candidate.plan.length === 0 ||
      candidate.plan.length > FLEET_MIGRATION_PLAN_BOUND ||
      !fleetOperationSafeInteger(candidate.planCursor)
    ) {
      return malformed();
    }
    plan = candidate.plan.map(fleetMigrationPlanEntryFromUnknown);
    if (
      candidate.planCursor > plan.length ||
      (candidate.status === 'active' && candidate.planCursor >= plan.length) ||
      (candidate.status === 'complete' && candidate.planCursor !== plan.length)
    ) {
      return malformed();
    }
  }
  return {
    ordinal: candidate.ordinal,
    tenantTag: candidate.tenantTag,
    environment: candidate.environment,
    ...(candidate.canaryRank === undefined
      ? {}
      : { canaryRank: candidate.canaryRank }),
    entryRecordDigest: candidate.entryRecordDigest,
    ...(pending
      ? {}
      : {
          targetSpecDigest: candidate.targetSpecDigest as string,
          plan: plan as readonly FleetMigrationPlanEntry[],
          planCursor: candidate.planCursor as number,
        }),
    status: candidate.status,
  };
}

export function fleetMigrationProgressFromUnknown(
  value: unknown,
): FleetMigrationProgress {
  const candidate = fleetOperationPlainRecord(value);
  assertFleetOperationExactKeys(
    candidate,
    [
      'kind',
      'revision',
      'itemCount',
      'activeItemOrdinal',
      'completedItemCount',
    ],
    ['failure'],
  );
  if (
    candidate.kind !== 'migration' ||
    !fleetOperationSafeInteger(candidate.revision) ||
    !fleetOperationSafeInteger(candidate.itemCount) ||
    candidate.itemCount > FLEET_OPERATION_ITEM_BOUND ||
    !fleetOperationSafeInteger(candidate.activeItemOrdinal) ||
    candidate.activeItemOrdinal > candidate.itemCount ||
    !fleetOperationSafeInteger(candidate.completedItemCount) ||
    candidate.completedItemCount > candidate.itemCount
  ) {
    return malformed();
  }
  const failure =
    candidate.failure === undefined
      ? undefined
      : fleetOperationFailureFromUnknown(candidate.failure);
  return {
    kind: 'migration',
    revision: candidate.revision,
    itemCount: candidate.itemCount,
    activeItemOrdinal: candidate.activeItemOrdinal,
    completedItemCount: candidate.completedItemCount,
    ...(failure === undefined ? {} : { failure }),
  };
}

export function fleetMigrationOperationRecordFromUnknown(
  value: unknown,
): FleetOperationRunRecord & Readonly<{ progress: FleetMigrationProgress }> {
  const record = fleetOperationRunRecordFromUnknown(value);
  if (record.kind !== 'migration') return malformed();
  return {
    ...record,
    progress: fleetMigrationProgressFromUnknown(record.progress),
  };
}
