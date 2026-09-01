// SPDX-License-Identifier: Apache-2.0

import {
  isDeploymentEnvironment,
  isDeploymentTenantTag,
} from './deployment-context.js';
import {
  assertFleetOperationExactKeys,
  FLEET_OPERATION_ITEM_BOUND,
  type FleetOperationProgress,
  type FleetOperationRunRecord,
  fleetOperationBoundedString,
  fleetOperationFailureFromUnknown,
  fleetOperationPlainRecord,
  fleetOperationRunRecordFromUnknown,
  fleetOperationSafeInteger,
  fleetOperationTextHasControlBytes,
  malformed,
} from './fleet-operation-state.js';

export type FleetAuditStage =
  | Readonly<{ step: 'provider-findings'; rowOrdinal: number }>
  | Readonly<{ step: 'registration-orphans'; rowOrdinal: number }>
  | Readonly<{ step: 'deployment-orphans'; rowOrdinal: number }>
  | Readonly<{ step: 'deployment-gaps'; auditedOrdinal: number }>
  | Readonly<{ step: 'orphan-databases'; rowOrdinal: number }>
  | Readonly<{ step: 'orphan-routes'; rowOrdinal: number }>
  | Readonly<{ step: 'namespace-orphans'; rowOrdinal: number }>
  | Readonly<{ step: 'namespace-expectations'; auditedOrdinal: number }>
  | Readonly<{ step: 'r2-expected'; auditedOrdinal: number }>
  | Readonly<{ step: 'r2-orphans'; rowOrdinal: number }>
  | Readonly<{ step: 'r2-missing-identity'; expectedOrdinal: number }>
  | Readonly<{ step: 'per-record'; recordOrdinal: number }>
  | Readonly<{ step: 'finalize' }>;

export const FLEET_AUDIT_STAGE_ORDER = Object.freeze([
  'provider-findings',
  'registration-orphans',
  'deployment-orphans',
  'deployment-gaps',
  'orphan-databases',
  'orphan-routes',
  'namespace-orphans',
  'namespace-expectations',
  'r2-expected',
  'r2-orphans',
  'r2-missing-identity',
  'per-record',
  'finalize',
] as const) satisfies readonly FleetAuditStage['step'][];

const STAGE_ORDINAL = Object.freeze({
  'provider-findings': 'rowOrdinal',
  'registration-orphans': 'rowOrdinal',
  'deployment-orphans': 'rowOrdinal',
  'deployment-gaps': 'auditedOrdinal',
  'orphan-databases': 'rowOrdinal',
  'orphan-routes': 'rowOrdinal',
  'namespace-orphans': 'rowOrdinal',
  'namespace-expectations': 'auditedOrdinal',
  'r2-expected': 'auditedOrdinal',
  'r2-orphans': 'rowOrdinal',
  'r2-missing-identity': 'expectedOrdinal',
  'per-record': 'recordOrdinal',
  finalize: undefined,
} satisfies Readonly<Record<FleetAuditStage['step'], string | undefined>>);

export const FLEET_AUDIT_FINDING_KINDS = Object.freeze([
  'missing-deployment',
  'duplicate-deployment',
  'database-mismatch',
  'duplicate-database',
  'duplicate-namespace',
  'binding-drift',
  'route-drift',
  'orphan-deployment',
  'orphan-database',
  'missing-namespace',
  'orphan-namespace',
  'orphan-route',
  'missing-r2-bucket',
  'orphan-r2-bucket',
  'r2-bucket-drift',
  'duplicate-route',
  'incomplete-provisioning',
  'version-drift',
  'maintenance-stale',
  'audit-error',
  'malformed-script-registration',
  'stale-script-registration',
  'malformed-route',
  'stale-route',
  'incomplete-deployment',
  'trusted-dispatch-namespace',
  'unknown-dispatch-scripts',
] as const);

export type FleetAuditFindingKind = (typeof FLEET_AUDIT_FINDING_KINDS)[number];

export interface FleetAuditProgress extends FleetOperationProgress {
  readonly kind: 'audit';
  readonly stage: FleetAuditStage;
  readonly generation: number;
  readonly auditTimeMs: number;
  readonly staleAfterMs: number;
  readonly recordCount: number;
  readonly findingCount: number;
  readonly factCount: number;
}

export type DriftFindingRowPayload = Readonly<{
  tenantTag: string;
  environment: string;
  kind: FleetAuditFindingKind;
  detail: string;
}>;

export type FleetAuditFactPayload =
  | Readonly<{
      factKind: 'database-owner';
      key: string;
      tenantTag: string;
      environment: string;
    }>
  | Readonly<{
      factKind: 'namespace-owner';
      key: string;
      tenantTag: string;
      environment: string;
    }>
  | Readonly<{ factKind: 'duplicate-namespace'; key: string }>;

function structurallySafeText(value: unknown): value is string {
  return (
    fleetOperationBoundedString(value) &&
    !fleetOperationTextHasControlBytes(value)
  );
}

export function fleetAuditStageFromUnknown(value: unknown): FleetAuditStage {
  const candidate = fleetOperationPlainRecord(value);
  if (
    typeof candidate.step !== 'string' ||
    !FLEET_AUDIT_STAGE_ORDER.includes(candidate.step as FleetAuditStage['step'])
  ) {
    return malformed();
  }
  const step = candidate.step as FleetAuditStage['step'];
  const ordinal = STAGE_ORDINAL[step];
  assertFleetOperationExactKeys(
    candidate,
    ordinal === undefined ? ['step'] : ['step', ordinal],
  );
  if (ordinal !== undefined && !fleetOperationSafeInteger(candidate[ordinal])) {
    return malformed();
  }
  return {
    step,
    ...(ordinal === undefined ? {} : { [ordinal]: candidate[ordinal] }),
  } as FleetAuditStage;
}

function stageEntry(step: FleetAuditStage['step']): FleetAuditStage {
  const ordinal = STAGE_ORDINAL[step];
  return {
    step,
    ...(ordinal === undefined ? {} : { [ordinal]: 0 }),
  } as FleetAuditStage;
}

export function nextAuditStage(
  stage: FleetAuditStage,
  exhausted: boolean,
): FleetAuditStage {
  const current = fleetAuditStageFromUnknown(stage);
  if (!exhausted || current.step === 'finalize') return current;
  // 'finalize' is last in FLEET_AUDIT_STAGE_ORDER, so the early return
  // keeps this index in range.
  const next = FLEET_AUDIT_STAGE_ORDER[
    FLEET_AUDIT_STAGE_ORDER.indexOf(current.step) + 1
  ] as FleetAuditStage['step'];
  return stageEntry(next);
}

export function fleetAuditProgressFromUnknown(
  value: unknown,
): FleetAuditProgress {
  const candidate = fleetOperationPlainRecord(value);
  assertFleetOperationExactKeys(
    candidate,
    [
      'kind',
      'revision',
      'stage',
      'generation',
      'auditTimeMs',
      'staleAfterMs',
      'recordCount',
      'findingCount',
      'factCount',
    ],
    ['failure'],
  );
  if (
    candidate.kind !== 'audit' ||
    !fleetOperationSafeInteger(candidate.revision) ||
    !fleetOperationSafeInteger(candidate.generation, 1) ||
    !fleetOperationSafeInteger(candidate.auditTimeMs) ||
    !fleetOperationSafeInteger(candidate.staleAfterMs) ||
    !fleetOperationSafeInteger(candidate.recordCount) ||
    candidate.recordCount > FLEET_OPERATION_ITEM_BOUND ||
    !fleetOperationSafeInteger(candidate.findingCount) ||
    !fleetOperationSafeInteger(candidate.factCount)
  ) {
    return malformed();
  }
  const failure =
    candidate.failure === undefined
      ? undefined
      : fleetOperationFailureFromUnknown(candidate.failure);
  return {
    kind: 'audit',
    revision: candidate.revision,
    stage: fleetAuditStageFromUnknown(candidate.stage),
    generation: candidate.generation,
    auditTimeMs: candidate.auditTimeMs,
    staleAfterMs: candidate.staleAfterMs,
    recordCount: candidate.recordCount,
    findingCount: candidate.findingCount,
    factCount: candidate.factCount,
    ...(failure === undefined ? {} : { failure }),
  };
}

export function fleetAuditOperationRecordFromUnknown(
  value: unknown,
): FleetOperationRunRecord & Readonly<{ progress: FleetAuditProgress }> {
  const record = fleetOperationRunRecordFromUnknown(value);
  if (record.kind !== 'audit') return malformed();
  return {
    ...record,
    progress: fleetAuditProgressFromUnknown(record.progress),
  };
}

export function driftFindingRowFromUnknown(
  value: unknown,
): DriftFindingRowPayload {
  const candidate = fleetOperationPlainRecord(value);
  assertFleetOperationExactKeys(candidate, [
    'tenantTag',
    'environment',
    'kind',
    'detail',
  ]);
  // These are provider-claimed observations that the drain emits verbatim.
  if (
    !structurallySafeText(candidate.tenantTag) ||
    !structurallySafeText(candidate.environment) ||
    typeof candidate.kind !== 'string' ||
    !FLEET_AUDIT_FINDING_KINDS.includes(
      candidate.kind as FleetAuditFindingKind,
    ) ||
    !structurallySafeText(candidate.detail)
  ) {
    return malformed();
  }
  return {
    tenantTag: candidate.tenantTag,
    environment: candidate.environment,
    kind: candidate.kind as FleetAuditFindingKind,
    detail: candidate.detail,
  };
}

export function fleetAuditFactRowFromUnknown(
  value: unknown,
): FleetAuditFactPayload {
  const candidate = fleetOperationPlainRecord(value);
  if (candidate.factKind === 'duplicate-namespace') {
    assertFleetOperationExactKeys(candidate, ['factKind', 'key']);
    if (!structurallySafeText(candidate.key)) return malformed();
    return { factKind: 'duplicate-namespace', key: candidate.key };
  }
  if (
    candidate.factKind !== 'database-owner' &&
    candidate.factKind !== 'namespace-owner'
  ) {
    return malformed();
  }
  assertFleetOperationExactKeys(candidate, [
    'factKind',
    'key',
    'tenantTag',
    'environment',
  ]);
  if (
    !structurallySafeText(candidate.key) ||
    typeof candidate.tenantTag !== 'string' ||
    !isDeploymentTenantTag(candidate.tenantTag) ||
    typeof candidate.environment !== 'string' ||
    !isDeploymentEnvironment(candidate.environment)
  ) {
    return malformed();
  }
  return {
    factKind: candidate.factKind,
    key: candidate.key,
    tenantTag: candidate.tenantTag,
    environment: candidate.environment,
  };
}

export function withheldAuditDetail(kind: FleetAuditFindingKind): string {
  return `finding detail withheld: unsafe bytes (kind '${kind}')`;
}
