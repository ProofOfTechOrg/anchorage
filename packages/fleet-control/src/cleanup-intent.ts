// SPDX-License-Identifier: Apache-2.0

import {
  parseWorkerAttachmentScanProgress,
  WORKER_ATTACHMENT_EVIDENCE_BOUND,
} from './cloudflare-worker-attachment-scan-state.js';
import { cloneBoundedPlainData } from './strict-plain-data.js';
import type {
  CleanupAdvanceIntent,
  CleanupAdvanceState,
  CleanupAdvanceToken,
  CleanupAttachmentProgress,
  CleanupAttachmentPurpose,
  CleanupAttachmentScan,
  CleanupAuthority,
  CleanupReceiptEvidence,
  CleanupTerminalReceipt,
  DecommissionBlockedAttachment,
  FleetRecord,
  InvocationAuthorityCarrier,
  ProvisioningBackendKind,
  ProvisioningPhase,
} from './types.js';

export const CLEANUP_INTENT_BYTE_BOUND = 96 * 1024;
const TOKEN_BYTE_BOUND = 1024;
const STRING_BYTE_BOUND = 4096;
const DEPTH_BOUND = 64;
const NODE_BOUND = 8192;
const SHA256 = /^[0-9a-f]{64}$/u;
const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const STRUCTURED_CLONE = structuredClone;

/**
 * The exact phase set no-export cleanup may admit. Every later phase requires
 * export-backed decommissioning.
 */
const CLEANUP_ADMITTED_PHASES: readonly ProvisioningPhase[] = [
  'database-reserved',
  'database-create-authorized',
  'database-created',
  'identity-seeded',
  'migrated',
  'application-resources-create-authorized',
  'application-resources-deployed',
  'platform-resources-deployed',
  'worker-deployed',
  'maintenance-armed',
];
const RESERVATION_PHASES: readonly ProvisioningPhase[] = [
  'database-reserved',
  'database-create-authorized',
];
const LEGACY_IMPOSSIBLE_PHASES: readonly ProvisioningPhase[] = [
  'database-created',
  'identity-seeded',
  'migrated',
  'application-resources-create-authorized',
];

export class CleanupAdvanceIntentError extends Error {
  constructor() {
    super('cleanup advance intent is malformed');
    this.name = 'CleanupAdvanceIntentError';
  }
}

export class InvocationAuthorityCarrierError extends Error {
  constructor() {
    super('invocation authority carrier is malformed');
    this.name = 'InvocationAuthorityCarrierError';
  }
}

export class CleanupTerminalReceiptError extends Error {
  constructor() {
    super('cleanup terminal receipt is malformed');
    this.name = 'CleanupTerminalReceiptError';
  }
}

export class CleanupAdvanceTokenError extends Error {
  constructor() {
    super('cleanup advance token is malformed');
    this.name = 'CleanupAdvanceTokenError';
  }
}

export class CleanupAdvanceTokenDeploymentError extends Error {
  constructor() {
    super('cleanup advance token targets another deployment');
    this.name = 'CleanupAdvanceTokenDeploymentError';
  }
}

export class CleanupAdvanceTokenOperationError extends Error {
  constructor() {
    super('cleanup advance token targets another operation');
    this.name = 'CleanupAdvanceTokenOperationError';
  }
}

export class CleanupAdvanceTokenFutureError extends Error {
  constructor() {
    super('cleanup advance token is from the future');
    this.name = 'CleanupAdvanceTokenFutureError';
  }
}

function malformed(): never {
  throw new CleanupAdvanceIntentError();
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
): CleanupAdvanceIntent['identity']['record'] {
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
  return identity as CleanupAdvanceIntent['identity']['record'];
}

function parseIdentity(
  value: unknown,
  source: FleetRecord,
): CleanupAdvanceIntent['identity'] {
  const candidate = plainRecord(value);
  exactKeys(candidate, ['record', 'admittedPhase', 'externalArtifact']);
  const record = parseRecordIdentity(candidate.record, source);
  if (
    typeof candidate.admittedPhase !== 'string' ||
    !CLEANUP_ADMITTED_PHASES.includes(
      candidate.admittedPhase as ProvisioningPhase,
    ) ||
    typeof candidate.externalArtifact !== 'boolean'
  ) {
    return malformed();
  }
  return {
    record,
    admittedPhase: candidate.admittedPhase as ProvisioningPhase,
    externalArtifact: candidate.externalArtifact,
  };
}

function parseAuthority(value: unknown): CleanupAuthority {
  const candidate = plainRecord(value);
  if (candidate.kind === 'manual-cleanup') {
    exactKeys(candidate, ['kind']);
    return { kind: 'manual-cleanup' };
  }
  exactKeys(candidate, [
    'kind',
    'reservationOwned',
    'databaseOwned',
    'workerCreatedByAttempt',
    'workerResourceState',
    'requestedSpecDigest',
  ]);
  if (
    candidate.kind !== 'provisioning-rollback' ||
    typeof candidate.reservationOwned !== 'boolean' ||
    typeof candidate.databaseOwned !== 'boolean' ||
    typeof candidate.workerCreatedByAttempt !== 'boolean' ||
    (candidate.workerResourceState !== 'absent' &&
      candidate.workerResourceState !== 'present' &&
      candidate.workerResourceState !== 'unknown') ||
    !sha256(candidate.requestedSpecDigest)
  ) {
    return malformed();
  }
  return {
    kind: 'provisioning-rollback',
    reservationOwned: candidate.reservationOwned,
    databaseOwned: candidate.databaseOwned,
    workerCreatedByAttempt: candidate.workerCreatedByAttempt,
    workerResourceState: candidate.workerResourceState,
    requestedSpecDigest: candidate.requestedSpecDigest,
  };
}

function parsePurpose(
  value: unknown,
  source: FleetRecord,
  operationId: string,
): CleanupAttachmentPurpose {
  const candidate = plainRecord(value);
  exactKeys(candidate, ['kind', 'databaseId', 'operationId']);
  if (
    candidate.kind !== 'cleanup-database-pre-delete' ||
    !boundedString(candidate.databaseId) ||
    candidate.databaseId !== source.databaseId ||
    typeof candidate.operationId !== 'string' ||
    candidate.operationId !== operationId
  ) {
    return malformed();
  }
  return {
    kind: 'cleanup-database-pre-delete',
    databaseId: candidate.databaseId,
    operationId: candidate.operationId,
  };
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

function parseScanEvidence(
  value: unknown,
): Readonly<{ evidenceSha256: string; evidenceCount: number }> {
  const evidence = plainRecord(value);
  exactKeys(evidence, ['evidenceSha256', 'evidenceCount']);
  if (
    !sha256(evidence.evidenceSha256) ||
    !safeIntegerAtLeast(evidence.evidenceCount, 2) ||
    evidence.evidenceCount > WORKER_ATTACHMENT_EVIDENCE_BOUND
  ) {
    return malformed();
  }
  return {
    evidenceSha256: evidence.evidenceSha256,
    evidenceCount: evidence.evidenceCount,
  };
}

function parseScan(
  value: unknown,
  source: FleetRecord,
  operationId: string,
): CleanupAttachmentScan {
  const candidate = plainRecord(value);
  exactKeys(candidate, [
    'purpose',
    'pass',
    'progress',
    ...(candidate.pass === 'verify' ? ['discoverEvidence'] : []),
  ]);
  const purpose = parsePurpose(candidate.purpose, source, operationId);
  if (candidate.pass !== 'discover' && candidate.pass !== 'verify') {
    return malformed();
  }
  let progress: CleanupAttachmentProgress;
  try {
    progress = parseWorkerAttachmentScanProgress(candidate.progress, {
      kind: 'd1',
      databaseId: purpose.databaseId,
    });
  } catch {
    return malformed();
  }
  if (candidate.pass === 'discover') {
    return { purpose, pass: 'discover', progress };
  }
  return {
    purpose,
    pass: 'verify',
    progress,
    discoverEvidence: parseScanEvidence(candidate.discoverEvidence),
  };
}

function parseState(
  value: unknown,
  source: FleetRecord,
  operationId: string,
): CleanupAdvanceState {
  const candidate = plainRecord(value);
  switch (candidate.step) {
    case 'teardown-traffic':
    case 'teardown-worker':
    case 'teardown-platform':
    case 'database-deletion': {
      exactKeys(candidate, ['step']);
      return { step: candidate.step };
    }
    case 'r2-deletion': {
      exactKeys(candidate, [
        'step',
        'startResourceIndex',
        ...(Object.hasOwn(candidate, 'verifiedDetachmentResourceIndex')
          ? ['verifiedDetachmentResourceIndex']
          : []),
      ]);
      if (
        !safeIntegerAtLeast(candidate.startResourceIndex) ||
        (Object.hasOwn(candidate, 'verifiedDetachmentResourceIndex') &&
          !safeIntegerAtLeast(candidate.verifiedDetachmentResourceIndex))
      ) {
        return malformed();
      }
      return {
        step: 'r2-deletion',
        startResourceIndex: candidate.startResourceIndex,
        ...(safeIntegerAtLeast(candidate.verifiedDetachmentResourceIndex)
          ? {
              verifiedDetachmentResourceIndex:
                candidate.verifiedDetachmentResourceIndex,
            }
          : {}),
      };
    }
    case 'attachment-scan': {
      exactKeys(candidate, ['step', 'scan']);
      return {
        step: 'attachment-scan',
        scan: parseScan(candidate.scan, source, operationId),
      };
    }
    case 'blocked': {
      exactKeys(candidate, ['step', 'purpose', 'attachment']);
      return {
        step: 'blocked',
        purpose: parsePurpose(candidate.purpose, source, operationId),
        attachment: parseAttachment(candidate.attachment),
      };
    }
    default:
      return malformed();
  }
}

export function cleanupAdvanceIntentFromUnknown(
  value: unknown,
  source: FleetRecord,
): CleanupAdvanceIntent {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: DEPTH_BOUND,
      maxNodes: NODE_BOUND,
      maxScalarBytes: CLEANUP_INTENT_BYTE_BOUND,
      maxSerializedBytes: CLEANUP_INTENT_BYTE_BOUND,
      error: () => new CleanupAdvanceIntentError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    return malformed();
  }
  const candidate = plainRecord(plain);
  exactKeys(candidate, [
    'version',
    'operationId',
    'revision',
    'generation',
    'updatedAt',
    'authority',
    'identity',
    'state',
  ]);
  if (
    candidate.version !== 1 ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    !safeIntegerAtLeast(candidate.revision) ||
    !safeIntegerAtLeast(candidate.generation) ||
    !canonicalIso(candidate.updatedAt) ||
    source.phase !== 'cleanup-advancing'
  ) {
    return malformed();
  }
  return {
    version: 1,
    operationId: candidate.operationId,
    revision: candidate.revision,
    generation: candidate.generation,
    updatedAt: candidate.updatedAt,
    authority: parseAuthority(candidate.authority),
    identity: parseIdentity(candidate.identity, source),
    state: parseState(candidate.state, source, candidate.operationId),
  };
}

export function normalizeCleanupAdvanceIntent(
  value: CleanupAdvanceIntent,
  source: FleetRecord,
): CleanupAdvanceIntent {
  return cleanupAdvanceIntentFromUnknown(value, source);
}

export function invocationAuthorityCarrierFromUnknown(
  value: unknown,
): InvocationAuthorityCarrier {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: 4,
      maxNodes: 16,
      maxScalarBytes: TOKEN_BYTE_BOUND,
      maxSerializedBytes: TOKEN_BYTE_BOUND,
      error: () => new InvocationAuthorityCarrierError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    throw new InvocationAuthorityCarrierError();
  }
  let candidate: Record<string, unknown>;
  try {
    candidate = plainRecord(plain);
    exactKeys(candidate, ['version', 'authorizedAt']);
  } catch {
    throw new InvocationAuthorityCarrierError();
  }
  if (
    candidate.version !== 1 ||
    (candidate.authorizedAt !== null && !canonicalIso(candidate.authorizedAt))
  ) {
    throw new InvocationAuthorityCarrierError();
  }
  return {
    version: 1,
    authorizedAt: candidate.authorizedAt as string | null,
  };
}

export function parseCleanupAdvanceToken(value: unknown): CleanupAdvanceToken {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: 4,
      maxNodes: 16,
      maxScalarBytes: TOKEN_BYTE_BOUND,
      maxSerializedBytes: TOKEN_BYTE_BOUND,
      error: () => new CleanupAdvanceTokenError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    throw new CleanupAdvanceTokenError();
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
    throw new CleanupAdvanceTokenError();
  }
  if (
    candidate.version !== 1 ||
    !boundedString(candidate.tenantTag) ||
    !boundedString(candidate.environment) ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    !safeIntegerAtLeast(candidate.revision)
  ) {
    throw new CleanupAdvanceTokenError();
  }
  return {
    version: 1,
    tenantTag: candidate.tenantTag,
    environment: candidate.environment,
    operationId: candidate.operationId,
    revision: candidate.revision,
  };
}

/**
 * Pure token classification against the live record. Receipt-aware semantics
 * for an absent row or a foreign operation belong to the engine, which
 * consults `readCleanupReceipt` before adjudicating.
 */
export function classifyCleanupAdvanceToken(
  value: unknown,
  source: FleetRecord,
): 'current' | 'stale' {
  const token = parseCleanupAdvanceToken(value);
  if (
    token.tenantTag !== source.tenantTag ||
    token.environment !== source.environment
  ) {
    throw new CleanupAdvanceTokenDeploymentError();
  }
  const intent = source.cleanupIntent;
  if (!intent || token.operationId !== intent.operationId) {
    throw new CleanupAdvanceTokenOperationError();
  }
  if (token.revision > intent.revision) {
    throw new CleanupAdvanceTokenFutureError();
  }
  return token.revision === intent.revision ? 'current' : 'stale';
}

function receiptMalformed(): never {
  throw new CleanupTerminalReceiptError();
}

function parseReceiptEvidencePair(
  value: unknown,
): Readonly<{ evidenceSha256: string; evidenceCount: number }> {
  try {
    return parseScanEvidence(value);
  } catch {
    return receiptMalformed();
  }
}

export function cleanupReceiptEvidenceFromUnknown(
  value: unknown,
): CleanupReceiptEvidence {
  let candidate: Record<string, unknown>;
  try {
    candidate = plainRecord(value);
    exactKeys(candidate, [
      'eligibility',
      'ingressRemoved',
      'workerAbsent',
      'platformResourcesAbsent',
      'applicationR2Settled',
      'databaseAbsentReadback',
      ...(Object.hasOwn(candidate, 'scan') ? ['scan'] : []),
    ]);
  } catch {
    return receiptMalformed();
  }
  if (
    (candidate.eligibility !== 'carrier-null' &&
      candidate.eligibility !== 'legacy-phase-impossible' &&
      candidate.eligibility !== 'reservation-only') ||
    typeof candidate.ingressRemoved !== 'boolean' ||
    typeof candidate.workerAbsent !== 'boolean' ||
    typeof candidate.platformResourcesAbsent !== 'boolean' ||
    typeof candidate.applicationR2Settled !== 'boolean' ||
    typeof candidate.databaseAbsentReadback !== 'boolean'
  ) {
    return receiptMalformed();
  }
  let scan: CleanupReceiptEvidence['scan'];
  if (Object.hasOwn(candidate, 'scan')) {
    let scanCandidate: Record<string, unknown>;
    try {
      scanCandidate = plainRecord(candidate.scan);
      exactKeys(scanCandidate, ['discover', 'verify']);
    } catch {
      return receiptMalformed();
    }
    scan = {
      discover: parseReceiptEvidencePair(scanCandidate.discover),
      verify: parseReceiptEvidencePair(scanCandidate.verify),
    };
  }
  return {
    eligibility: candidate.eligibility,
    ingressRemoved: candidate.ingressRemoved,
    workerAbsent: candidate.workerAbsent,
    platformResourcesAbsent: candidate.platformResourcesAbsent,
    applicationR2Settled: candidate.applicationR2Settled,
    databaseAbsentReadback: candidate.databaseAbsentReadback,
    ...(scan ? { scan } : {}),
  };
}

export function cleanupTerminalReceiptFromUnknown(
  value: unknown,
): CleanupTerminalReceipt {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: DEPTH_BOUND,
      maxNodes: NODE_BOUND,
      maxScalarBytes: CLEANUP_INTENT_BYTE_BOUND,
      maxSerializedBytes: CLEANUP_INTENT_BYTE_BOUND,
      error: () => new CleanupTerminalReceiptError(),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    return receiptMalformed();
  }
  let candidate: Record<string, unknown>;
  try {
    candidate = plainRecord(plain);
    exactKeys(candidate, [
      'version',
      'operationId',
      'tenantTag',
      'environment',
      'backend',
      'scriptName',
      'databaseId',
      'databaseName',
      'authority',
      'admittedPhase',
      'disposition',
      'evidence',
      ...(Object.hasOwn(candidate, 'completedAtMs') ? ['completedAtMs'] : []),
    ]);
  } catch {
    return receiptMalformed();
  }
  if (
    candidate.version !== 1 ||
    typeof candidate.operationId !== 'string' ||
    !UUID_V4.test(candidate.operationId) ||
    !boundedString(candidate.tenantTag) ||
    !boundedString(candidate.environment) ||
    (candidate.backend !== 'plain-worker' &&
      candidate.backend !== 'workers-for-platforms') ||
    !boundedString(candidate.scriptName) ||
    !boundedString(candidate.databaseId) ||
    !boundedString(candidate.databaseName) ||
    (candidate.authority !== 'manual-cleanup' &&
      candidate.authority !== 'provisioning-rollback') ||
    typeof candidate.admittedPhase !== 'string' ||
    !CLEANUP_ADMITTED_PHASES.includes(
      candidate.admittedPhase as ProvisioningPhase,
    ) ||
    (candidate.disposition !== 'prepublication-owned-no-export' &&
      candidate.disposition !== 'reservation-cleared') ||
    (Object.hasOwn(candidate, 'completedAtMs') &&
      !safeIntegerAtLeast(candidate.completedAtMs))
  ) {
    return receiptMalformed();
  }
  return {
    version: 1,
    operationId: candidate.operationId,
    tenantTag: candidate.tenantTag,
    environment: candidate.environment,
    backend: candidate.backend as ProvisioningBackendKind,
    scriptName: candidate.scriptName,
    databaseId: candidate.databaseId,
    databaseName: candidate.databaseName,
    authority: candidate.authority,
    admittedPhase: candidate.admittedPhase as ProvisioningPhase,
    disposition: candidate.disposition,
    evidence: cleanupReceiptEvidenceFromUnknown(candidate.evidence),
    ...(safeIntegerAtLeast(candidate.completedAtMs)
      ? { completedAtMs: candidate.completedAtMs }
      : {}),
  };
}

function canonicalSortedKeyValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalSortedKeyValue);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(
    Object.keys(value as Record<string, unknown>)
      .sort((left, right) => (left < right ? -1 : left > right ? 1 : 0))
      .map((key) => [
        key,
        canonicalSortedKeyValue((value as Record<string, unknown>)[key]),
      ]),
  );
}

/**
 * @internal Canonical sorted-key serialization of receipt evidence, used both
 * when building the receipt insert and in the state-store convergence
 * comparison so evidence byte comparison never depends on object-literal key
 * order.
 */
export function canonicalCleanupEvidenceBytes(
  evidence: CleanupReceiptEvidence,
): string {
  return JSON.stringify(canonicalSortedKeyValue(evidence));
}

/**
 * Option A eligibility classifier for no-export database deletion. Pure: it
 * decides ELIGIBILITY only and has no provider-database input; the terminal
 * receipt disposition is decided inside the database-deletion group by the
 * `findDatabase`/reconcile outcome.
 */
export function classifyCleanupDatabaseEligibility(
  input: Readonly<{
    record: FleetRecord;
    /** backend.immutableExternalArtifacts === true, captured at admission and persisted in the intent. */
    externalArtifact: boolean;
  }>,
):
  | Readonly<{
      eligible: true;
      eligibility:
        | 'carrier-null'
        | 'legacy-phase-impossible'
        | 'reservation-only';
    }>
  | Readonly<{
      eligible: false;
      reason:
        | 'invocation-authorized'
        | 'legacy-phase-ambiguous'
        | 'carrier-phase-inconsistent'
        | 'malformed-carrier'
        | 'phase-requires-decommission'
        | 'untrusted-data-binding'
        | 'external-staging-evidence';
    }> {
  const { record } = input;
  if (!CLEANUP_ADMITTED_PHASES.includes(record.phase)) {
    return { eligible: false, reason: 'phase-requires-decommission' };
  }
  if (
    record.backend === 'workers-for-platforms' ||
    input.externalArtifact === true
  ) {
    return { eligible: false, reason: 'untrusted-data-binding' };
  }
  if (
    record.activeRelease !== undefined ||
    record.pendingRelease !== undefined ||
    record.migrationPriorRelease !== undefined ||
    record.rollbackRelease !== undefined ||
    record.retiringRelease !== undefined ||
    record.platformTarget !== undefined ||
    record.migrationIntent !== undefined ||
    // The spec scopes the pending-digest evidence to prepublication phases;
    // every admitted phase is prepublication, so no phase qualifier is needed.
    record.pendingArtifactVersion !== undefined ||
    record.pendingSpecDigest !== undefined
  ) {
    return { eligible: false, reason: 'external-staging-evidence' };
  }
  const reservation = RESERVATION_PHASES.includes(record.phase);
  if (Object.hasOwn(record, 'invocationAuthority')) {
    let carrier: InvocationAuthorityCarrier;
    try {
      carrier = invocationAuthorityCarrierFromUnknown(
        record.invocationAuthority,
      );
    } catch {
      return { eligible: false, reason: 'malformed-carrier' };
    }
    if (typeof carrier.authorizedAt === 'string') {
      return { eligible: false, reason: 'invocation-authorized' };
    }
    if (record.phase === 'maintenance-armed') {
      return { eligible: false, reason: 'carrier-phase-inconsistent' };
    }
    return reservation
      ? { eligible: true, eligibility: 'reservation-only' }
      : { eligible: true, eligibility: 'carrier-null' };
  }
  if (reservation) {
    return { eligible: true, eligibility: 'reservation-only' };
  }
  if (LEGACY_IMPOSSIBLE_PHASES.includes(record.phase)) {
    return { eligible: true, eligibility: 'legacy-phase-impossible' };
  }
  return { eligible: false, reason: 'legacy-phase-ambiguous' };
}
