// SPDX-License-Identifier: Apache-2.0

import {
  parseWorkerAttachmentScanProgress,
  WORKER_ATTACHMENT_EVIDENCE_BOUND,
  type WorkerAttachmentScanTarget,
} from './cloudflare-worker-attachment-scan-state.js';
import { cloneBoundedPlainData } from './strict-plain-data.js';
import type {
  DecommissionAdvanceIntent,
  DecommissionAdvanceToken,
  DecommissionAdvanceTokenClassification,
  DecommissionAttachmentProgress,
  DecommissionAttachmentPurpose,
  DecommissionBlockedAttachment,
  DecommissionIntentCommon,
  DecommissionOperationIdentity,
  DecommissionRecordIdentity,
  FleetRecord,
  NormalDecommissionLifecyclePhase,
} from './types.js';

export const DECOMMISSION_INTENT_BYTE_BOUND = 96 * 1024;
const TOKEN_BYTE_BOUND = 1024;
const STRING_BYTE_BOUND = 4096;
const DEPTH_BOUND = 64;
const NODE_BOUND = 8192;
const SHA256 = /^[0-9a-f]{64}$/u;
const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

const INITIAL_PHASES = new Set<NormalDecommissionLifecyclePhase>([
  'publishing',
  'ready',
  'migrating',
  'rolling-back',
]);
const TEARDOWN_PHASES = [
  'decommissioning',
  'traffic-removed',
  'credentials-revoked',
  'worker-deleted',
  'platform-credentials-revoked',
  'platform-resources-deleted',
  'application-resources-deleting',
  'application-resources-deleted',
  'database-exported',
  'database-deleting',
] as const satisfies readonly NormalDecommissionLifecyclePhase[];
const LIFECYCLE_PHASES = new Set<NormalDecommissionLifecyclePhase>([
  ...INITIAL_PHASES,
  ...TEARDOWN_PHASES,
]);

export class DecommissionAdvanceIntentError extends Error {
  constructor() {
    super('decommission advance intent is malformed');
    this.name = 'DecommissionAdvanceIntentError';
  }
}

export class DecommissionAdvanceTokenError extends Error {
  constructor() {
    super('decommission advance token is malformed');
    this.name = 'DecommissionAdvanceTokenError';
  }
}

export class DecommissionAdvanceTokenFutureError extends Error {
  constructor() {
    super('decommission advance token is from the future');
    this.name = 'DecommissionAdvanceTokenFutureError';
  }
}

export class DecommissionAdvanceTokenDeploymentError extends Error {
  constructor() {
    super('decommission advance token targets another deployment');
    this.name = 'DecommissionAdvanceTokenDeploymentError';
  }
}

export class DecommissionAdvanceTokenOperationError extends Error {
  constructor() {
    super('decommission advance token targets another operation');
    this.name = 'DecommissionAdvanceTokenOperationError';
  }
}

function malformed(): never {
  throw new DecommissionAdvanceIntentError();
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
): void {
  const keys = Object.keys(value).sort();
  const expected = [...required].sort();
  if (
    keys.length !== expected.length ||
    keys.some((key, index) => key !== expected[index])
  ) {
    malformed();
  }
}

function boundedString(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= STRING_BYTE_BOUND &&
    new TextEncoder().encode(value).byteLength <= STRING_BYTE_BOUND
  );
}

function safeIntegerAtLeast(value: unknown, minimum = 0): value is number {
  return Number.isSafeInteger(value) && Number(value) >= minimum;
}

function sha256(value: unknown): value is string {
  return typeof value === 'string' && SHA256.test(value);
}

function canonicalIso(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    Number.isFinite(Date.parse(value)) &&
    new Date(value).toISOString() === value
  );
}

function parseRecordIdentity(
  value: unknown,
  source: FleetRecord,
): DecommissionRecordIdentity {
  const candidate = plainRecord(value);
  exactKeys(candidate, [
    'tenantTag',
    'environment',
    'backend',
    'scriptName',
    'databaseId',
    'databaseName',
    'routeHostname',
  ]);
  const identity = {
    tenantTag: candidate.tenantTag,
    environment: candidate.environment,
    backend: candidate.backend,
    scriptName: candidate.scriptName,
    databaseId: candidate.databaseId,
    databaseName: candidate.databaseName,
    routeHostname: candidate.routeHostname,
  };
  if (
    !boundedString(identity.tenantTag) ||
    !boundedString(identity.environment) ||
    (identity.backend !== 'plain-worker' &&
      identity.backend !== 'workers-for-platforms') ||
    !boundedString(identity.scriptName) ||
    !boundedString(identity.databaseId) ||
    !boundedString(identity.databaseName) ||
    !boundedString(identity.routeHostname) ||
    identity.tenantTag !== source.tenantTag ||
    identity.environment !== source.environment ||
    identity.backend !== source.backend ||
    identity.scriptName !== source.scriptName ||
    identity.databaseId !== source.databaseId ||
    identity.databaseName !== source.databaseName ||
    identity.routeHostname !== source.routeHostname
  ) {
    return malformed();
  }
  return identity as DecommissionRecordIdentity;
}

function parseLifecyclePhase(value: unknown): NormalDecommissionLifecyclePhase {
  if (
    typeof value !== 'string' ||
    !LIFECYCLE_PHASES.has(value as NormalDecommissionLifecyclePhase)
  ) {
    return malformed();
  }
  return value as NormalDecommissionLifecyclePhase;
}

function reachesLifecyclePhase(
  entry: NormalDecommissionLifecyclePhase,
  current: NormalDecommissionLifecyclePhase,
): boolean {
  if (INITIAL_PHASES.has(entry)) {
    return entry === current || TEARDOWN_PHASES.includes(current as never);
  }
  const entryIndex = TEARDOWN_PHASES.indexOf(entry as never);
  const currentIndex = TEARDOWN_PHASES.indexOf(current as never);
  return entryIndex >= 0 && currentIndex >= entryIndex;
}

function migrationCarrier(
  source: FleetRecord,
): Readonly<{ digest: string; present: boolean }> | undefined {
  if (source.migrationIntent) {
    if (
      source.backend !== 'workers-for-platforms' ||
      (source.pendingSpecDigest !== undefined &&
        source.pendingSpecDigest !== source.migrationIntent.targetSpecDigest) ||
      source.pendingArtifactVersion !== undefined
    ) {
      return undefined;
    }
    return { digest: source.migrationIntent.targetSpecDigest, present: true };
  }
  if (source.pendingSpecDigest !== undefined) {
    if (
      source.backend !== 'plain-worker' ||
      !sha256(source.pendingSpecDigest) ||
      (source.pendingArtifactVersion !== undefined &&
        (typeof source.pendingArtifactVersion !== 'string' ||
          source.pendingArtifactVersion.length === 0 ||
          source.pendingArtifactVersion === 'pending'))
    ) {
      return undefined;
    }
    return { digest: source.pendingSpecDigest, present: true };
  }
  if (source.pendingArtifactVersion !== undefined) return undefined;
  return { digest: source.desiredSpecDigest, present: false };
}

function parseIdentity(
  value: unknown,
  source: FleetRecord,
  lifecyclePhase: NormalDecommissionLifecyclePhase | 'decommissioned',
): DecommissionOperationIdentity {
  const candidate = plainRecord(value);
  exactKeys(candidate, ['record', 'mode']);
  const stored = parseRecordIdentity(candidate.record, source);
  const mode = plainRecord(candidate.mode);
  if (mode.kind === 'backend-switch') return malformed();
  exactKeys(mode, ['kind', 'requestedSpecDigest', 'entryLifecyclePhase']);
  if (
    mode.kind !== 'normal' ||
    !sha256(mode.requestedSpecDigest) ||
    source.backendSwitchIntent !== undefined
  ) {
    return malformed();
  }
  const entry = parseLifecyclePhase(mode.entryLifecyclePhase);
  const carrier = migrationCarrier(source);
  if (
    (lifecyclePhase !== 'decommissioned' &&
      !reachesLifecyclePhase(entry, lifecyclePhase)) ||
    !carrier ||
    (entry === 'migrating' && mode.requestedSpecDigest !== carrier.digest) ||
    (entry === 'migrating' &&
      ((carrier.present && lifecyclePhase !== 'migrating') ||
        (!carrier.present && lifecyclePhase === 'migrating'))) ||
    (entry !== 'migrating' && carrier.present) ||
    (entry !== 'migrating' &&
      entry !== 'ready' &&
      entry !== 'rolling-back' &&
      mode.requestedSpecDigest !== source.desiredSpecDigest)
  ) {
    return malformed();
  }
  return {
    record: stored,
    mode: {
      kind: 'normal',
      requestedSpecDigest: mode.requestedSpecDigest,
      entryLifecyclePhase: entry,
    },
  };
}

function parsePurpose(
  value: unknown,
  source: FleetRecord,
  lifecyclePhase: NormalDecommissionLifecyclePhase,
): Readonly<{
  purpose: DecommissionAttachmentPurpose;
  target: WorkerAttachmentScanTarget;
}> {
  const candidate = plainRecord(value);
  if (candidate.kind === 'application-r2-detach') {
    exactKeys(candidate, [
      'kind',
      'resourceIndex',
      'name',
      'bucketName',
      'jurisdiction',
      'reservationNonce',
      'creationDate',
    ]);
    if (
      lifecyclePhase !== 'application-resources-deleting' ||
      !safeIntegerAtLeast(candidate.resourceIndex) ||
      !boundedString(candidate.name) ||
      !boundedString(candidate.bucketName) ||
      (candidate.jurisdiction !== 'default' &&
        candidate.jurisdiction !== 'eu' &&
        candidate.jurisdiction !== 'fedramp') ||
      !boundedString(candidate.reservationNonce) ||
      !canonicalIso(candidate.creationDate)
    ) {
      return malformed();
    }
    const resource = source.applicationResources?.[candidate.resourceIndex];
    if (
      resource?.state !== 'detach-authorized' ||
      resource.name !== candidate.name ||
      resource.bucketName !== candidate.bucketName ||
      resource.jurisdiction !== candidate.jurisdiction ||
      resource.reservationNonce !== candidate.reservationNonce ||
      resource.creationDate !== candidate.creationDate
    ) {
      return malformed();
    }
    return {
      purpose: {
        kind: 'application-r2-detach',
        resourceIndex: candidate.resourceIndex,
        name: candidate.name,
        bucketName: candidate.bucketName,
        jurisdiction: candidate.jurisdiction,
        reservationNonce: candidate.reservationNonce,
        creationDate: candidate.creationDate,
      },
      target: { kind: 'r2', bucketName: candidate.bucketName },
    };
  }
  if (candidate.kind === 'database-pre-export') {
    exactKeys(candidate, ['kind', 'databaseId']);
    if (
      lifecyclePhase !== 'application-resources-deleted' ||
      !boundedString(candidate.databaseId) ||
      candidate.databaseId !== source.databaseId
    ) {
      return malformed();
    }
    return {
      purpose: {
        kind: 'database-pre-export',
        databaseId: candidate.databaseId,
      },
      target: { kind: 'd1', databaseId: candidate.databaseId },
    };
  }
  if (candidate.kind === 'database-pre-delete') {
    exactKeys(candidate, [
      'kind',
      'databaseId',
      'exportLocation',
      'exportSha256',
      'exportSize',
    ]);
    if (
      !['database-exported', 'database-deleting'].includes(lifecyclePhase) ||
      !boundedString(candidate.databaseId) ||
      candidate.databaseId !== source.databaseId ||
      !boundedString(candidate.exportLocation) ||
      candidate.exportLocation !== source.databaseExportLocation ||
      !sha256(candidate.exportSha256) ||
      candidate.exportSha256 !== source.databaseExportSha256 ||
      !safeIntegerAtLeast(candidate.exportSize, 1) ||
      candidate.exportSize !== source.databaseExportSize
    ) {
      return malformed();
    }
    return {
      purpose: {
        kind: 'database-pre-delete',
        databaseId: candidate.databaseId,
        exportLocation: candidate.exportLocation,
        exportSha256: candidate.exportSha256,
        exportSize: candidate.exportSize,
      },
      target: { kind: 'd1', databaseId: candidate.databaseId },
    };
  }
  return malformed();
}

function parseAttachment(value: unknown): DecommissionBlockedAttachment {
  const candidate = plainRecord(value);
  if (candidate.plane === 'ordinary') {
    exactKeys(candidate, ['plane', 'scriptName']);
    if (!boundedString(candidate.scriptName)) return malformed();
    return { plane: 'ordinary', scriptName: candidate.scriptName };
  }
  exactKeys(candidate, ['plane', 'scriptName', 'dispatchNamespace']);
  if (
    candidate.plane !== 'dispatch' ||
    !boundedString(candidate.scriptName) ||
    !boundedString(candidate.dispatchNamespace)
  ) {
    return malformed();
  }
  return {
    plane: 'dispatch',
    scriptName: candidate.scriptName,
    dispatchNamespace: candidate.dispatchNamespace,
  };
}

function parseIntentCommon(
  candidate: Record<string, unknown>,
  source: FleetRecord,
): DecommissionIntentCommon {
  if (
    candidate.version !== 1 ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    !safeIntegerAtLeast(candidate.revision) ||
    !safeIntegerAtLeast(candidate.generation) ||
    !canonicalIso(candidate.updatedAt)
  ) {
    return malformed();
  }
  const lifecyclePhase = parseLifecyclePhase(candidate.lifecyclePhase);
  return {
    version: 1,
    operationId: candidate.operationId,
    revision: candidate.revision,
    generation: candidate.generation,
    updatedAt: candidate.updatedAt,
    identity: parseIdentity(candidate.identity, source, lifecyclePhase),
    lifecyclePhase,
  };
}

function assertCompleteRecord(source: FleetRecord): void {
  if (
    source.phase !== 'decommissioned' ||
    (source.applicationResources ?? []).some(
      (resource) => resource.state !== 'deleted',
    ) ||
    !boundedString(source.databaseExportLocation) ||
    !sha256(source.databaseExportSha256) ||
    !safeIntegerAtLeast(source.databaseExportSize, 1) ||
    source.pendingSpecDigest !== undefined ||
    source.pendingArtifactVersion !== undefined ||
    source.pendingRelease !== undefined ||
    source.migrationPriorRelease !== undefined ||
    source.rollbackRelease !== undefined ||
    source.retiringRelease !== undefined ||
    source.migrationIntent !== undefined ||
    source.backendSwitchIntent !== undefined
  ) {
    malformed();
  }
}

export function decommissionAdvanceIntentFromUnknown(
  value: unknown,
  source: FleetRecord,
): DecommissionAdvanceIntent {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: DEPTH_BOUND,
      maxNodes: NODE_BOUND,
      maxScalarBytes: DECOMMISSION_INTENT_BYTE_BOUND,
      maxSerializedBytes: DECOMMISSION_INTENT_BYTE_BOUND,
      error: () => new DecommissionAdvanceIntentError(),
    });
  } catch {
    return malformed();
  }
  const candidate = plainRecord(plain);
  if (candidate.state === 'complete') {
    exactKeys(candidate, [
      'version',
      'operationId',
      'revision',
      'generation',
      'updatedAt',
      'identity',
      'lifecyclePhase',
      'state',
    ]);
    if (
      candidate.version !== 1 ||
      typeof candidate.operationId !== 'string' ||
      !UUID_V4.test(candidate.operationId) ||
      !safeIntegerAtLeast(candidate.revision) ||
      !safeIntegerAtLeast(candidate.generation) ||
      !canonicalIso(candidate.updatedAt) ||
      candidate.lifecyclePhase !== 'decommissioned'
    ) {
      return malformed();
    }
    assertCompleteRecord(source);
    return {
      version: 1,
      operationId: candidate.operationId,
      revision: candidate.revision,
      generation: candidate.generation,
      updatedAt: candidate.updatedAt,
      identity: parseIdentity(candidate.identity, source, 'decommissioned'),
      lifecyclePhase: 'decommissioned',
      state: 'complete',
    };
  }
  const state = candidate.state;
  const stateKeys =
    state === 'transitioning'
      ? []
      : state === 'discover'
        ? ['purpose', 'progress']
        : state === 'verify'
          ? ['purpose', 'progress', 'discoverEvidence']
          : state === 'blocked'
            ? ['purpose', 'attachment']
            : undefined;
  if (!stateKeys) return malformed();
  exactKeys(candidate, [
    'version',
    'operationId',
    'revision',
    'generation',
    'updatedAt',
    'identity',
    'lifecyclePhase',
    'state',
    ...stateKeys,
  ]);
  if (source.phase !== 'decommission-advancing') return malformed();
  const parsedCommon = parseIntentCommon(candidate, source);
  if (state === 'transitioning') {
    return { ...parsedCommon, state };
  }
  const { purpose, target } = parsePurpose(
    candidate.purpose,
    source,
    parsedCommon.lifecyclePhase,
  );
  if (state === 'blocked') {
    return {
      ...parsedCommon,
      state,
      purpose,
      attachment: parseAttachment(candidate.attachment),
    };
  }
  let progress: DecommissionAttachmentProgress;
  try {
    progress = parseWorkerAttachmentScanProgress(candidate.progress, target);
  } catch {
    return malformed();
  }
  if (state === 'discover') {
    return {
      ...parsedCommon,
      state,
      purpose,
      progress,
    };
  }
  const evidence = plainRecord(candidate.discoverEvidence);
  exactKeys(evidence, ['evidenceSha256', 'evidenceCount']);
  if (
    !sha256(evidence.evidenceSha256) ||
    !safeIntegerAtLeast(evidence.evidenceCount, 2) ||
    evidence.evidenceCount > WORKER_ATTACHMENT_EVIDENCE_BOUND
  ) {
    return malformed();
  }
  return {
    ...parsedCommon,
    state: 'verify',
    purpose,
    progress,
    discoverEvidence: {
      evidenceSha256: evidence.evidenceSha256,
      evidenceCount: evidence.evidenceCount,
    },
  };
}

export function normalizeDecommissionAdvanceIntent(
  value: DecommissionAdvanceIntent,
  source: FleetRecord,
): DecommissionAdvanceIntent {
  return decommissionAdvanceIntentFromUnknown(value, source);
}

export function parseDecommissionAdvanceToken(
  value: unknown,
): DecommissionAdvanceToken {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: 4,
      maxNodes: 16,
      maxScalarBytes: TOKEN_BYTE_BOUND,
      maxSerializedBytes: TOKEN_BYTE_BOUND,
      error: () => new DecommissionAdvanceTokenError(),
    });
  } catch {
    throw new DecommissionAdvanceTokenError();
  }
  let candidate: Record<string, unknown>;
  try {
    candidate = plainRecord(plain);
    exactKeys(candidate, [
      'version',
      'tenantTag',
      'environment',
      'operationId',
      'revision',
    ]);
  } catch {
    throw new DecommissionAdvanceTokenError();
  }
  if (
    candidate.version !== 1 ||
    !boundedString(candidate.tenantTag) ||
    !boundedString(candidate.environment) ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    !safeIntegerAtLeast(candidate.revision)
  ) {
    throw new DecommissionAdvanceTokenError();
  }
  return {
    version: 1,
    tenantTag: candidate.tenantTag,
    environment: candidate.environment,
    operationId: candidate.operationId,
    revision: candidate.revision,
  };
}

export function classifyDecommissionAdvanceToken(
  value: unknown,
  source: FleetRecord,
): DecommissionAdvanceTokenClassification {
  const token = parseDecommissionAdvanceToken(value);
  if (
    token.tenantTag !== source.tenantTag ||
    token.environment !== source.environment
  ) {
    throw new DecommissionAdvanceTokenDeploymentError();
  }
  const intent = source.decommissionIntent;
  if (!intent || token.operationId !== intent.operationId) {
    throw new DecommissionAdvanceTokenOperationError();
  }
  if (token.revision > intent.revision) {
    throw new DecommissionAdvanceTokenFutureError();
  }
  return token.revision === intent.revision ? 'current' : 'stale';
}
