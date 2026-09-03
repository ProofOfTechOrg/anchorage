// SPDX-License-Identifier: Apache-2.0

import {
  isDeploymentEnvironment,
  isDeploymentTenantTag,
} from './deployment-context.js';
import {
  assertFleetOperationExactKeys,
  FLEET_OPERATION_ITEM_BOUND,
  FLEET_OPERATION_STRING_BYTE_BOUND,
  type FleetOperationProgress,
  type FleetOperationRunRecord,
  fleetOperationFailureFromUnknown,
  fleetOperationPlainRecord,
  fleetOperationRunRecordFromUnknown,
  fleetOperationSafeInteger,
  fleetOperationTextHasControlBytes,
  malformed,
  utf8Length,
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
} satisfies Readonly<{
  [K in FleetAuditStage['step']]:
    | Exclude<keyof Extract<FleetAuditStage, { step: K }>, 'step'>
    | undefined;
}>);

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

/**
 * Byte-bounded provider-claimed text: a string within the module's string
 * byte bound, with NO non-empty requirement.
 *
 * Admitting the empty string is the deliberate §5.1 EXCEPTION round 3
 * established, not an oversight — do not add a `value.length > 0` clause
 * back. `fleetOperationBoundedString`, which still carried that superseded
 * clause under a near-identical name, was deleted for exactly that reason;
 * the name here says what the predicate is for rather than what it bounds.
 */
function boundedProviderText(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    utf8Length(value) <= FLEET_OPERATION_STRING_BYTE_BOUND
  );
}

function structurallySafeText(value: unknown): value is string {
  return (
    boundedProviderText(value) && !fleetOperationTextHasControlBytes(value)
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
  return withAuditStageOrdinal(
    step,
    ordinal === undefined ? 0 : (candidate[ordinal] as number),
  );
}

/**
 * Builds the stage whose cursor field `STAGE_ORDINAL` names for `step`,
 * dropping the ordinal for the cursor-less `finalize` step.
 *
 * `step` is the full 13-step union because two in-module callers need it:
 * `stageEntry`, which walks all of `FLEET_AUDIT_STAGE_ORDER`, and
 * `fleetAuditStageFromUnknown`, which rebuilds any persisted stage. The only
 * caller outside this module passes a global step.
 */
export function withAuditStageOrdinal(
  step: FleetAuditStage['step'],
  ordinal: number,
): FleetAuditStage {
  const ordinalField = STAGE_ORDINAL[step];
  return {
    step,
    ...(ordinalField === undefined ? {} : { [ordinalField]: ordinal }),
  } as FleetAuditStage;
}

/**
 * Read-side counterpart of `withAuditStageOrdinal`. Resolving the cursor
 * through the same `STAGE_ORDINAL` table the write side uses keeps the
 * step-to-field mapping in one place; `finalize` carries no cursor.
 */
export function fleetAuditStageOrdinal(
  stage: FleetAuditStage,
): number | undefined {
  const ordinalField = STAGE_ORDINAL[stage.step];
  if (ordinalField === undefined) return undefined;
  const cursor: Readonly<Record<string, unknown>> = stage;
  return cursor[ordinalField] as number;
}

function stageEntry(step: FleetAuditStage['step']): FleetAuditStage {
  return withAuditStageOrdinal(step, 0);
}

/**
 * The successor of `stage` when its source is exhausted, or `stage` itself
 * when it is not.
 *
 * Both production call sites pass `exhausted: true` — a coordinator only ever
 * asks for the successor once it has drained the stage. The `false` case is
 * kept because it makes the successor chain total, and it is exercised only
 * by the codec's own `nextAuditStage` title. `stage` is re-parsed through
 * `fleetAuditStageFromUnknown` even though it arrives typed, so a stage
 * rebuilt from durable state is re-validated before it is advanced.
 */
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
    Number.isNaN(new Date(candidate.auditTimeMs).getTime()) ||
    !fleetOperationSafeInteger(candidate.staleAfterMs, 1) ||
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
    !boundedProviderText(candidate.tenantTag) ||
    !boundedProviderText(candidate.environment) ||
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
    if (!boundedProviderText(candidate.key)) return malformed();
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
    !boundedProviderText(candidate.key) ||
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
