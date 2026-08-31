// SPDX-License-Identifier: Apache-2.0

import { PENDING_ARTIFACT_VERSION } from './active-route.js';
import {
  advanceApplicationR2Deletion,
  applicationBindingTopology,
  applicationR2Bindings,
  assertApplicationR2EmptyBeforeDecommission,
  assertApplicationR2ReservationIdentity,
} from './application-bindings.js';
import {
  assertWorkerAttachmentProviderRequestBudget,
  initialWorkerAttachmentScan,
  parseWorkerAttachmentScanProgress,
  WORKER_ATTACHMENT_EVIDENCE_BOUND,
} from './cloudflare-worker-attachment-scan-state.js';
import { captureDatabaseExportReceiptCapability } from './database-export-store.js';
import {
  databaseExportFromUnknown,
  databaseExportReceiptIdentity,
  reconcilePersistedDatabaseFromCallbacks,
  settleDatabaseDeletionUnderBarrier,
} from './decommission-database.js';
import {
  classifyDecommissionAdvanceToken,
  DecommissionAdvanceIntentError,
  DecommissionAdvanceTokenDeploymentError,
  DecommissionAdvanceTokenOperationError,
  normalizeDecommissionAdvanceIntent,
  parseDecommissionAdvanceToken,
} from './decommission-intent.js';
import { deploymentSpecDigest } from './spec-digest.js';
import { cloneBoundedPlainData } from './strict-plain-data.js';
import type {
  ApplicationR2Resource,
  DatabaseReference,
  DecommissionAdvanceIntent,
  DecommissionAdvanceToken,
  DecommissionAttachmentProgress,
  DecommissionAttachmentPurpose,
  DecommissionAttachmentScanEvidence,
  DecommissionBlockedAttachment,
  DecommissionResult,
  DeploymentSpec,
  ExternalMutationFence,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  NormalDecommissionLifecyclePhase,
  ProvisioningBackend,
} from './types.js';
import { assertNoActiveCleanup, effectiveLifecyclePhase } from './types.js';
import { validateDeploymentSpec } from './validation.js';

const ACTION_ERROR = 'decommission advance action is malformed';
const RESULT_ERROR = 'bounded decommission attachment result is malformed';
const D1_APPLICATION_RESOURCES_ERROR =
  'normal decommission D1 work requires every application R2 resource to be deleted';
const ATTACHMENT_STRING_BYTE_BOUND = 4_096;
const RESULT_PLAIN_DATA_DEPTH_BOUND = 64;
const RESULT_PLAIN_DATA_NODE_BOUND = 8_192;
const RESULT_PLAIN_DATA_BYTE_BOUND = 96 * 1_024;
const SHA256 = /^[0-9a-f]{64}$/u;
const STRUCTURED_CLONE = structuredClone;
const NORMAL_ENTRY_PHASES = new Set<NormalDecommissionLifecyclePhase>([
  'publishing',
  'ready',
  'migrating',
  'rolling-back',
  'decommissioning',
  'traffic-removed',
  'credentials-revoked',
  'worker-deleted',
  'platform-credentials-revoked',
  'platform-resources-deleted',
  'application-resources-deleting',
  'application-resources-deleted',
]);

/** Caller command for one bounded normal-decommission invocation. */
export type DecommissionAdvanceAction =
  | Readonly<{
      /** Create a new operation, or read an existing operation's authority. */
      kind: 'start';
    }>
  | Readonly<{
      /** Advance a strictly parsed current token; stale returns current state. */
      kind: 'continue';
      token: unknown;
    }>
  | Readonly<{
      /** Explicitly restart an exact current blocked operation. */
      kind: 'restart-blocked';
      token: unknown;
    }>;

/** Inputs for one lease-scoped bounded action group. */
export interface AdvanceDecommissionDeploymentOptions {
  readonly backend: ProvisioningBackend;
  readonly store: FleetStateStore;
  readonly spec: DeploymentSpec;
  /** Typed command; its token remains an unknown strict-codec boundary. */
  readonly action: DecommissionAdvanceAction;
  /** Provider-fetch attempt budget for each bounded attachment scan, integer 9..1,000. */
  readonly maxProviderRequests: number;
  /** Call-local cancellation, never persisted. */
  readonly signal?: AbortSignal;
  /** Timestamp source; called once for each accepted write. */
  readonly clock?: () => number;
  /** Called exactly once only when a genuinely new operation starts. */
  readonly randomUUID: () => string;
}

/** Authoritative durable outcome after at most one bounded group. */
export type DecommissionAdvanceResult =
  | Readonly<{
      /** More work remains; the token, not this status, is continuation input. */
      status: 'pending';
      token: DecommissionAdvanceToken;
    }>
  | Readonly<{
      /** Provider attachment blocks deletion until explicit restart. */
      status: 'blocked';
      token: DecommissionAdvanceToken;
      purpose: DecommissionAttachmentPurpose;
      attachment: DecommissionBlockedAttachment;
    }>
  | Readonly<{
      /** Evidence-free terminal receipt with durable export result. */
      status: 'complete';
      token: DecommissionAdvanceToken;
      result: DecommissionResult;
    }>;

/** Named backend capability whose absence makes work fail closed. */
export type DecommissionAdvanceCapability =
  | 'attachment-scan'
  | 'database-residuals'
  | 'database-export-receipt'
  | 'database-read'
  | 'database-delete'
  | 'pending-artifact-inspection'
  | 'application-r2-inspection'
  | 'application-r2-empty'
  | 'application-r2-delete';

const CAPABILITY_MESSAGES: Readonly<
  Record<DecommissionAdvanceCapability, string>
> = Object.freeze({
  'attachment-scan':
    'backend cannot perform bounded decommission attachment scans',
  'database-residuals': 'backend cannot inspect database deletion residuals',
  'database-export-receipt':
    'backend cannot write idempotent database export receipts',
  'database-read': 'backend cannot read the database for bounded decommission',
  'database-delete':
    'backend cannot delete the database for bounded decommission',
  'pending-artifact-inspection':
    'backend cannot inspect pending ordinary Worker authority for bounded decommission',
  'application-r2-inspection':
    'backend cannot inspect application R2 resources',
  'application-r2-empty': 'backend cannot attest application R2 emptiness',
  'application-r2-delete': 'backend cannot delete application R2 resources',
});

/** Fixed configuration refusal for one missing bounded capability. */
export class DecommissionAdvanceCapabilityError extends Error {
  constructor(readonly capability: DecommissionAdvanceCapability) {
    super(CAPABILITY_MESSAGES[capability]);
    this.name = 'DecommissionAdvanceCapabilityError';
  }
}

/** Refusal for restart without an exact current blocked operation. */
export class DecommissionAdvanceRestartError extends Error {
  constructor() {
    super('decommission advance restart requires a current blocked operation');
    this.name = 'DecommissionAdvanceRestartError';
  }
}

function malformedAction(): never {
  throw new Error(ACTION_ERROR);
}

/** @internal Descriptor-safe Queue-action boundary. */
export function decommissionAdvanceActionFromUnknown(
  value: unknown,
): DecommissionAdvanceAction {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: 8,
      maxNodes: 32,
      maxScalarBytes: 2_048,
      maxSerializedBytes: 2_048,
      error: () => new Error(ACTION_ERROR),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    return malformedAction();
  }
  if (!plain || typeof plain !== 'object' || Array.isArray(plain)) {
    return malformedAction();
  }
  const candidate = plain as Record<string, unknown>;
  const keys = Object.keys(candidate).sort();
  if (candidate.kind === 'start' && keys.length === 1 && keys[0] === 'kind') {
    return { kind: 'start' };
  }
  if (
    (candidate.kind === 'continue' || candidate.kind === 'restart-blocked') &&
    keys.length === 2 &&
    keys[0] === 'kind' &&
    keys[1] === 'token'
  ) {
    return { kind: candidate.kind, token: candidate.token };
  }
  return malformedAction();
}

/** @internal Persisted active release used by both legacy and bounded teardown. */
export function activeExternalRelease(
  record: FleetRecord,
): ExternalReleaseSnapshot | undefined {
  return (
    record.activeRelease ??
    (record.backend === 'plain-worker' &&
    record.artifactVersion !== PENDING_ARTIFACT_VERSION
      ? {
          physicalScriptName: record.scriptName,
          specDigest: record.desiredSpecDigest,
          artifactVersion: record.artifactVersion,
          releaseSchemaVersion: record.schemaVersion,
          application: record.applicationBindings ?? {
            vars: [],
            secrets: [],
            r2Buckets: [],
          },
        }
      : undefined)
  );
}

/** @internal Persisted non-active releases used by teardown ownership checks. */
export function retainedExternalReleases(
  record: FleetRecord,
): readonly ExternalReleaseSnapshot[] {
  const active = activeExternalRelease(record);
  const releases = [
    record.pendingRelease,
    ...(record.backend === 'plain-worker' &&
    record.pendingArtifactVersion &&
    record.pendingSpecDigest
      ? [
          {
            physicalScriptName: record.scriptName,
            specDigest: record.pendingSpecDigest,
            artifactVersion: record.pendingArtifactVersion,
            releaseSchemaVersion: record.schemaVersion,
          },
        ]
      : []),
    record.rollbackRelease,
    record.retiringRelease,
    record.migrationPriorRelease,
  ].filter(
    (release): release is ExternalReleaseSnapshot =>
      release !== undefined &&
      (record.backend === 'plain-worker'
        ? release.artifactVersion !== active?.artifactVersion
        : release.physicalScriptName !== active?.physicalScriptName),
  );
  return releases.filter(
    (release, index) =>
      releases.findIndex(
        (candidate) =>
          (record.backend === 'plain-worker'
            ? candidate.artifactVersion
            : candidate.physicalScriptName) ===
          (record.backend === 'plain-worker'
            ? release.artifactVersion
            : release.physicalScriptName),
      ) === index,
  );
}

/** @internal Immutable deployment mapping shared with provisioning. */
export function assertImmutableDeploymentMapping(
  prior: FleetRecord,
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
): void {
  applicationR2Bindings(spec, prior.applicationResources ?? []);
  if (
    prior.tenantTag !== spec.tenantTag ||
    prior.environment !== spec.environment ||
    prior.backend !== backend.kind ||
    prior.scriptName !== spec.scriptName ||
    prior.databaseName !== spec.databaseName ||
    prior.routeHostname !== spec.routeHostname
  ) {
    throw new Error(
      `deployment '${spec.tenantTag}:${spec.environment}' already exists with a different immutable resource mapping`,
    );
  }
  if (
    prior.schemaVersion > spec.schemaVersion &&
    !(
      backend.immutableExternalArtifacts === true &&
      spec.authoredBy === 'external' &&
      prior.activeRelease?.releaseSchemaVersion === spec.schemaVersion
    )
  ) {
    throw new Error('provisioning refuses a schema downgrade');
  }
  const phase = effectiveLifecyclePhase(prior);
  if (
    phase !== 'ready' &&
    phase !== 'rolling-back' &&
    (phase === 'migrating'
      ? (prior.migrationIntent?.targetSpecDigest ??
        prior.pendingRelease?.specDigest ??
        prior.pendingSpecDigest)
      : prior.desiredSpecDigest) !== deploymentSpecDigest(spec)
  ) {
    throw new Error(
      `deployment '${spec.tenantTag}:${spec.environment}' retry uses a different desired specification`,
    );
  }
}

/** @internal Exact persisted-D1 reconciliation shared with provisioning. */
export async function reconcilePersistedDatabase(
  backend: ProvisioningBackend,
  record: Pick<FleetRecord, 'databaseId' | 'databaseName' | 'tenantTag'>,
  allowAbsent: boolean,
  fence: ExternalMutationFence,
  requireOwner = true,
): Promise<(DatabaseReference & { readonly created: false }) | undefined> {
  return reconcilePersistedDatabaseFromCallbacks({
    getDatabase: (databaseId) => backend.getDatabase(databaseId),
    readOwner: (database, currentFence) =>
      backend.readDeploymentIdentity(database, currentFence),
    record,
    allowAbsent,
    requireOwner,
    fence,
  });
}

function requireCapability(
  available: unknown,
  capability: DecommissionAdvanceCapability,
): asserts available is (...arguments_: never[]) => unknown {
  if (typeof available !== 'function') {
    throw new DecommissionAdvanceCapabilityError(capability);
  }
}

function requiredCapability<Value>(
  available: Value,
  capability: DecommissionAdvanceCapability,
): NonNullable<Value> {
  requireCapability(available, capability);
  return available as NonNullable<Value>;
}

type ReceiptCapability = Readonly<{
  authority: string;
  exportReceipt: NonNullable<ProvisioningBackend['exportDatabaseReceipt']>;
}>;

function receiptCapability(backend: ProvisioningBackend): ReceiptCapability {
  const captured = captureDatabaseExportReceiptCapability(backend, () => [
    backend.databaseExportReceiptAuthority,
    backend.exportDatabaseReceipt,
  ]);
  if (!captured) {
    throw new DecommissionAdvanceCapabilityError('database-export-receipt');
  }
  return {
    authority: captured.authority,
    exportReceipt: captured.method as ReceiptCapability['exportReceipt'],
  };
}

/** @internal Cross-field fence before normal-decommission D1 work. */
export function assertNormalDecommissionD1ResourcesDeleted(
  record: Pick<FleetRecord, 'applicationResources'>,
): void {
  if (
    (record.applicationResources ?? []).some(
      (resource) => resource.state !== 'deleted',
    )
  ) {
    throw new Error(D1_APPLICATION_RESOURCES_ERROR);
  }
}

function tokenFor(record: FleetRecord): DecommissionAdvanceToken {
  const intent = record.decommissionIntent;
  if (!intent) throw new DecommissionAdvanceTokenOperationError();
  return {
    version: 1,
    tenantTag: record.tenantTag,
    environment: record.environment,
    operationId: intent.operationId,
    revision: intent.revision,
  };
}

function authoritativeResult(record: FleetRecord): DecommissionAdvanceResult {
  const intent = record.decommissionIntent;
  if (!intent) throw new DecommissionAdvanceTokenOperationError();
  const token = tokenFor(record);
  if (intent.state === 'blocked') {
    return {
      status: 'blocked',
      token,
      purpose: intent.purpose,
      attachment: intent.attachment,
    };
  }
  if (intent.state === 'complete') {
    return {
      status: 'complete',
      token,
      result: {
        record,
        databaseExport: {
          databaseId: record.databaseId,
          location: record.databaseExportLocation as string,
          sha256: record.databaseExportSha256 as string,
          size: record.databaseExportSize as number,
        },
      },
    };
  }
  return { status: 'pending', token };
}

function omitIntent(
  record: FleetRecord,
): Omit<FleetRecord, 'decommissionIntent'> {
  const { decommissionIntent: _intent, ...source } = record;
  return source;
}

function normalizeIntent(
  intent: DecommissionAdvanceIntent,
  record: FleetRecord,
): DecommissionAdvanceIntent {
  return normalizeDecommissionAdvanceIntent(intent, omitIntent(record));
}

async function writeIntent(
  lease: FleetStateLease,
  record: FleetRecord,
  intent: DecommissionAdvanceIntent,
): Promise<FleetRecord> {
  const normalized = normalizeIntent(intent, record);
  const next: FleetRecord = {
    ...omitIntent(record),
    decommissionIntent: normalized,
  };
  await lease.put(next);
  return next;
}

type IntentTransition =
  | Readonly<{
      state: 'transitioning';
      databaseExportReceiptAuthority?: string;
      generation?: number;
      lifecyclePhase?: NormalDecommissionLifecyclePhase;
    }>
  | Readonly<{
      state: 'discover';
      purpose: DecommissionAttachmentPurpose;
      progress: DecommissionAttachmentProgress;
      databaseExportReceiptAuthority?: string;
      generation?: number;
      lifecyclePhase?: NormalDecommissionLifecyclePhase;
    }>
  | Readonly<{
      state: 'verify';
      purpose: DecommissionAttachmentPurpose;
      progress: DecommissionAttachmentProgress;
      discoverEvidence: DecommissionAttachmentScanEvidence;
      databaseExportReceiptAuthority?: string;
      generation?: number;
      lifecyclePhase?: NormalDecommissionLifecyclePhase;
    }>
  | Readonly<{
      state: 'blocked';
      purpose: DecommissionAttachmentPurpose;
      attachment: DecommissionBlockedAttachment;
      databaseExportReceiptAuthority?: string;
      generation?: number;
      lifecyclePhase?: NormalDecommissionLifecyclePhase;
    }>;

/** @internal State transition emitted by the shared bounded scan step. */
export type DecommissionIntentTransition = IntentTransition;

/** @internal Provider-neutral callbacks for one discover/verify scan chunk. */
export interface DecommissionAttachmentScanStepOptions {
  readonly intent: Extract<
    DecommissionAdvanceIntent,
    { readonly state: 'discover' | 'verify' }
  >;
  readonly scan: (
    input: import('./types.js').DecommissionAttachmentScanInput,
  ) => Promise<unknown>;
  readonly maxProviderRequests: number;
  readonly signal?: AbortSignal;
  readonly persist: (
    transition: DecommissionIntentTransition,
  ) => Promise<FleetRecord>;
  readonly consumeMatchingVerify: (
    input: Readonly<{
      intent: Extract<DecommissionAdvanceIntent, { readonly state: 'verify' }>;
      evidence: DecommissionAttachmentScanEvidence;
    }>,
  ) => Promise<FleetRecord>;
}

function nextIntent(
  intent: Exclude<DecommissionAdvanceIntent, { readonly state: 'complete' }>,
  timestamp: string,
  values: IntentTransition,
): Exclude<DecommissionAdvanceIntent, { readonly state: 'complete' }> {
  const databaseExportReceiptAuthority =
    intent.databaseExportReceiptAuthority ??
    values.databaseExportReceiptAuthority;
  const common = {
    version: 1 as const,
    operationId: intent.operationId,
    revision: intent.revision + 1,
    generation: values.generation ?? intent.generation,
    updatedAt: timestamp,
    identity: intent.identity,
    ...(databaseExportReceiptAuthority
      ? { databaseExportReceiptAuthority }
      : {}),
    lifecyclePhase: values.lifecyclePhase ?? intent.lifecyclePhase,
  };
  switch (values.state) {
    case 'transitioning':
      return { ...common, state: values.state };
    case 'discover':
      return {
        ...common,
        state: values.state,
        purpose: values.purpose,
        progress: values.progress,
      };
    case 'verify':
      return {
        ...common,
        state: values.state,
        purpose: values.purpose,
        progress: values.progress,
        discoverEvidence: values.discoverEvidence,
      };
    case 'blocked':
      return {
        ...common,
        state: values.state,
        purpose: values.purpose,
        attachment: values.attachment,
      };
  }
}

function nowIso(clock: () => number): string {
  return new Date(clock()).toISOString();
}

function assertNormalAuthority(
  record: FleetRecord,
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
  intent?: DecommissionAdvanceIntent,
): void {
  if (
    record.backendSwitchIntent ||
    intent?.identity.mode.kind === 'backend-switch'
  ) {
    throw new Error('normal decommission cannot consume a backend switch');
  }
  if (record.backend !== backend.kind) {
    throw new Error('decommission backend does not own this deployment');
  }
  assertImmutableDeploymentMapping(record, backend, spec);
  const digest = deploymentSpecDigest(spec);
  if (intent && intent.identity.mode.requestedSpecDigest !== digest) {
    throw new Error(
      'decommission retry uses a different requested specification',
    );
  }
  const phase = effectiveLifecyclePhase(record);
  if (
    !intent &&
    (phase === 'ready' || phase === 'rolling-back') &&
    record.desiredSpecDigest !== digest
  ) {
    throw new Error(
      'decommission specification does not match durable desired state',
    );
  }
}

function validateReservations(spec: DeploymentSpec, record: FleetRecord): void {
  for (const resource of record.applicationResources ?? []) {
    assertApplicationR2ReservationIdentity(spec, resource);
  }
}

function assertSupportedEntryLifecycle(record: FleetRecord): void {
  const currentPhase = effectiveLifecyclePhase(record);
  if (
    !NORMAL_ENTRY_PHASES.has(currentPhase as NormalDecommissionLifecyclePhase)
  ) {
    throw new Error(
      `cannot start bounded decommission in phase '${currentPhase}'`,
    );
  }
}

function requireStartCapabilities(
  backend: ProvisioningBackend,
  resources: readonly ApplicationR2Resource[],
): void {
  requireCapability(
    backend.advanceDecommissionAttachmentScan,
    'attachment-scan',
  );
  requireCapability(
    backend.assertDatabaseDeletionResidualsRemoved,
    'database-residuals',
  );
  if (resources.length > 0) {
    requireCapability(
      backend.findApplicationR2Bucket,
      'application-r2-inspection',
    );
  }
  if (resources.some((resource) => resource.state !== 'deleted')) {
    requireCapability(backend.assertApplicationR2Empty, 'application-r2-empty');
    requireCapability(
      backend.deleteApplicationR2Bucket,
      'application-r2-delete',
    );
  }
}

function assertCompleteApplicationR2Reservations(
  resources: readonly ApplicationR2Resource[],
): void {
  if (
    resources.some(
      (resource) =>
        resource.state === 'reserved' || resource.state === 'create-authorized',
    )
  ) {
    throw new Error(
      'normal decommission cannot consume incomplete application R2 reservation',
    );
  }
}

function malformedResult(): never {
  throw new Error(RESULT_ERROR);
}

function assertReservedAttempts(value: unknown, maximum: number): void {
  if (
    !Number.isSafeInteger(value) ||
    Number(value) < 0 ||
    Number(value) > maximum
  ) {
    malformedResult();
  }
}

function boundedString(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= ATTACHMENT_STRING_BYTE_BOUND &&
    new TextEncoder().encode(value).byteLength <= ATTACHMENT_STRING_BYTE_BOUND
  );
}

function safeAttachment(value: unknown): DecommissionBlockedAttachment {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return malformedResult();
  }
  const candidate = value as Record<string, unknown>;
  const plane = candidate.plane;
  const scriptName = candidate.scriptName;
  const dispatchNamespace = candidate.dispatchNamespace;
  if (plane === 'ordinary' && boundedString(scriptName)) {
    return { plane, scriptName };
  }
  if (
    plane === 'dispatch' &&
    boundedString(scriptName) &&
    boundedString(dispatchNamespace)
  ) {
    return {
      plane,
      scriptName,
      dispatchNamespace,
    };
  }
  return malformedResult();
}

function purposeTarget(purpose: DecommissionAttachmentPurpose) {
  return purpose.kind === 'application-r2-detach'
    ? ({ kind: 'r2', bucketName: purpose.bucketName } as const)
    : ({ kind: 'd1', databaseId: purpose.databaseId } as const);
}

/** @internal Advances one strict provider-neutral discover/verify scan step. */
export async function advanceDecommissionAttachmentScanStep(
  options: DecommissionAttachmentScanStepOptions,
): Promise<FleetRecord> {
  const { intent } = options;
  const raw = await options.scan({
    progress: intent.progress,
    maxProviderRequests: options.maxProviderRequests,
    signal: options.signal,
  });
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(raw, {
      maxDepth: RESULT_PLAIN_DATA_DEPTH_BOUND,
      maxNodes: RESULT_PLAIN_DATA_NODE_BOUND,
      maxScalarBytes: RESULT_PLAIN_DATA_BYTE_BOUND,
      maxSerializedBytes: RESULT_PLAIN_DATA_BYTE_BOUND,
      error: () => new Error(RESULT_ERROR),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [raw]);
  } catch {
    return malformedResult();
  }
  if (!plain || typeof plain !== 'object' || Array.isArray(plain)) {
    return malformedResult();
  }
  const result = plain as Record<string, unknown>;
  if (result.status === 'drift') {
    return options.persist({
      state: 'discover',
      purpose: intent.purpose,
      progress: initialWorkerAttachmentScan(purposeTarget(intent.purpose)),
      generation: intent.generation + 1,
    });
  }
  assertReservedAttempts(
    result.providerFetchAttemptsReserved,
    options.maxProviderRequests,
  );
  if (result.status === 'pending') {
    let progress: DecommissionAttachmentProgress;
    try {
      progress = parseWorkerAttachmentScanProgress(
        result.progress,
        purposeTarget(intent.purpose),
      );
    } catch {
      return malformedResult();
    }
    return options.persist(
      intent.state === 'verify'
        ? {
            state: 'verify',
            purpose: intent.purpose,
            progress,
            discoverEvidence: intent.discoverEvidence,
          }
        : { state: 'discover', purpose: intent.purpose, progress },
    );
  }
  if (result.status === 'attached') {
    return options.persist({
      state: 'blocked',
      purpose: intent.purpose,
      attachment: safeAttachment(result.attachment),
    });
  }
  if (
    result.status !== 'complete' ||
    typeof result.evidenceSha256 !== 'string' ||
    !Number.isSafeInteger(result.evidenceCount)
  ) {
    return malformedResult();
  }
  const evidence = {
    evidenceSha256: result.evidenceSha256,
    evidenceCount: Number(result.evidenceCount),
  };
  if (
    !/^[a-f0-9]{64}$/u.test(evidence.evidenceSha256) ||
    evidence.evidenceCount < 2 ||
    evidence.evidenceCount > WORKER_ATTACHMENT_EVIDENCE_BOUND
  ) {
    return malformedResult();
  }
  if (intent.state === 'discover') {
    return options.persist({
      state: 'verify',
      purpose: intent.purpose,
      progress: initialWorkerAttachmentScan(purposeTarget(intent.purpose)),
      discoverEvidence: evidence,
    });
  }
  if (
    evidence.evidenceSha256 !== intent.discoverEvidence.evidenceSha256 ||
    evidence.evidenceCount !== intent.discoverEvidence.evidenceCount
  ) {
    return options.persist({
      state: 'discover',
      purpose: intent.purpose,
      progress: initialWorkerAttachmentScan(purposeTarget(intent.purpose)),
      generation: intent.generation + 1,
    });
  }
  return options.consumeMatchingVerify({ intent, evidence });
}

async function commitShellOnly(
  lease: FleetStateLease,
  record: FleetRecord,
  intent: Exclude<DecommissionAdvanceIntent, { readonly state: 'complete' }>,
  clock: () => number,
  values: Parameters<typeof nextIntent>[2],
): Promise<FleetRecord> {
  const timestamp = nowIso(clock);
  return writeIntent(lease, record, nextIntent(intent, timestamp, values));
}

async function commitRecord(
  lease: FleetStateLease,
  record: FleetRecord,
  intent: Exclude<DecommissionAdvanceIntent, { readonly state: 'complete' }>,
  clock: () => number,
  recordValues: Readonly<
    Partial<
      Pick<
        FleetRecord,
        | 'applicationResources'
        | 'databaseExportLocation'
        | 'databaseExportSha256'
        | 'databaseExportSize'
        | 'phase'
      >
    >
  >,
  intentValues: Parameters<typeof nextIntent>[2],
): Promise<FleetRecord> {
  const timestamp = nowIso(clock);
  const nextRecord = { ...record, ...recordValues, updatedAt: timestamp };
  return writeIntent(
    lease,
    nextRecord,
    nextIntent(intent, timestamp, intentValues),
  );
}

function consumeMigrationCarrier(
  record: FleetRecord,
  spec: DeploymentSpec,
  intent: Exclude<DecommissionAdvanceIntent, { readonly state: 'complete' }>,
): FleetRecord {
  if (intent.lifecyclePhase !== 'migrating') return record;
  if (intent.identity.mode.kind !== 'normal') {
    throw new Error('normal decommission cannot consume a backend switch');
  }
  const priorActive = record.activeRelease ?? activeExternalRelease(record);
  const pendingRelease =
    record.pendingRelease ??
    (record.backend === 'plain-worker' &&
    record.pendingArtifactVersion !== undefined &&
    record.pendingSpecDigest !== undefined
      ? {
          physicalScriptName: record.scriptName,
          specDigest: record.pendingSpecDigest,
          artifactVersion: record.pendingArtifactVersion,
          releaseSchemaVersion: spec.schemaVersion,
          application: applicationBindingTopology(
            spec,
            record.applicationResources ?? [],
          ),
        }
      : undefined);
  const {
    migrationIntent: _migrationIntent,
    pendingSpecDigest: _pendingSpecDigest,
    pendingArtifactVersion: _pendingArtifactVersion,
    ...remaining
  } = record;
  return {
    ...remaining,
    desiredSpecDigest: intent.identity.mode.requestedSpecDigest,
    ...(priorActive ? { activeRelease: priorActive } : {}),
    ...(pendingRelease ? { pendingRelease } : {}),
  };
}

function clearRetainedReleases(record: FleetRecord): FleetRecord {
  const {
    pendingRelease: _pendingRelease,
    migrationPriorRelease: _migrationPriorRelease,
    rollbackRelease: _rollbackRelease,
    retiringRelease: _retiringRelease,
    ...remaining
  } = record;
  return remaining;
}

async function advanceLifecycle(
  options: AdvanceDecommissionDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: Exclude<DecommissionAdvanceIntent, { readonly state: 'complete' }>,
): Promise<FleetRecord> {
  const { backend, spec } = options;
  const clock = options.clock ?? Date.now;
  const phase = intent.lifecyclePhase;
  const resources = record.applicationResources ?? [];

  if (
    phase === 'publishing' ||
    phase === 'ready' ||
    phase === 'migrating' ||
    phase === 'rolling-back'
  ) {
    if (resources.length > 0) {
      const findApplicationR2Bucket = requiredCapability(
        backend.findApplicationR2Bucket,
        'application-r2-inspection',
      ).bind(backend);
      const needsEmptyAttestation = resources.some(
        (resource) =>
          resource.state !== 'reserved' && resource.state !== 'deleted',
      );
      const preflightBackend = needsEmptyAttestation
        ? {
            findApplicationR2Bucket,
            assertApplicationR2Empty: requiredCapability(
              backend.assertApplicationR2Empty,
              'application-r2-empty',
            ).bind(backend),
          }
        : { findApplicationR2Bucket };
      validateReservations(spec, record);
      await assertApplicationR2EmptyBeforeDecommission({
        resources,
        backend: preflightBackend,
        fence: lease,
      });
    }
    const consumed = consumeMigrationCarrier(record, spec, intent);
    return commitRecord(
      lease,
      consumed,
      intent,
      clock,
      {},
      { state: 'transitioning', lifecyclePhase: 'decommissioning' },
    );
  }

  if (phase === 'decommissioning') {
    const database = await reconcilePersistedDatabase(
      backend,
      record,
      false,
      lease,
      true,
    );
    await lease.assertOwned();
    await backend.removeTraffic(
      spec,
      retainedExternalReleases(record),
      activeExternalRelease(record),
      database as DatabaseReference,
      lease,
    );
    await backend.assertTrafficRemoved(spec);
    return commitRecord(
      lease,
      record,
      intent,
      clock,
      {},
      {
        state: 'transitioning',
        lifecyclePhase: 'traffic-removed',
      },
    );
  }

  if (phase === 'traffic-removed') {
    let preflightBackend:
      | Readonly<{
          findApplicationR2Bucket: NonNullable<
            ProvisioningBackend['findApplicationR2Bucket']
          >;
          assertApplicationR2Empty?: NonNullable<
            ProvisioningBackend['assertApplicationR2Empty']
          >;
        }>
      | undefined;
    if (resources.length > 0) {
      const findApplicationR2Bucket = requiredCapability(
        backend.findApplicationR2Bucket,
        'application-r2-inspection',
      ).bind(backend);
      const needsEmptyAttestation = resources.some(
        (resource) =>
          resource.state !== 'reserved' && resource.state !== 'deleted',
      );
      preflightBackend = needsEmptyAttestation
        ? {
            findApplicationR2Bucket,
            assertApplicationR2Empty: requiredCapability(
              backend.assertApplicationR2Empty,
              'application-r2-empty',
            ).bind(backend),
          }
        : { findApplicationR2Bucket };
      validateReservations(spec, record);
    }
    await backend.assertTrafficRemoved(spec);
    if (resources.length > 0) {
      await assertApplicationR2EmptyBeforeDecommission({
        resources,
        backend: preflightBackend as NonNullable<typeof preflightBackend>,
        fence: lease,
      });
    }
    const database = await reconcilePersistedDatabase(
      backend,
      record,
      false,
      lease,
      true,
    );
    await lease.assertOwned();
    await backend.revokeCredentials(
      spec,
      retainedExternalReleases(record),
      activeExternalRelease(record),
      database as DatabaseReference,
      lease,
    );
    return commitRecord(
      lease,
      record,
      intent,
      clock,
      {},
      {
        state: 'transitioning',
        lifecyclePhase: 'credentials-revoked',
      },
    );
  }

  if (phase === 'credentials-revoked') {
    const residual = !record.platformResources
      ? requiredCapability(
          backend.assertDatabaseDeletionResidualsRemoved,
          'database-residuals',
        )
      : undefined;
    const database = await reconcilePersistedDatabase(
      backend,
      record,
      false,
      lease,
      true,
    );
    await lease.assertOwned();
    await backend.deleteWorker(
      spec,
      retainedExternalReleases(record),
      database as DatabaseReference,
      activeExternalRelease(record),
      lease,
    );
    let nextRecord = record;
    if (residual) {
      await residual.call(
        backend,
        spec,
        record,
        database as DatabaseReference,
        lease,
      );
      nextRecord = clearRetainedReleases(record);
    }
    return commitRecord(
      lease,
      nextRecord,
      intent,
      clock,
      {},
      {
        state: 'transitioning',
        lifecyclePhase: 'worker-deleted',
      },
    );
  }

  if (phase === 'worker-deleted') {
    if (record.platformResources) {
      const revokePlatformResourceCredentialsCandidate =
        backend.revokePlatformResourceCredentials;
      if (typeof revokePlatformResourceCredentialsCandidate !== 'function') {
        throw new Error(
          'backend cannot revoke trusted platform resource credentials',
        );
      }
      const revokePlatformResourceCredentials =
        revokePlatformResourceCredentialsCandidate.bind(backend);
      const database = await reconcilePersistedDatabase(
        backend,
        record,
        false,
        lease,
        true,
      );
      await lease.assertOwned();
      await revokePlatformResourceCredentials(
        spec,
        record,
        database as DatabaseReference,
        lease,
      );
    }
    return commitRecord(
      lease,
      record,
      intent,
      clock,
      {},
      {
        state: 'transitioning',
        lifecyclePhase: 'platform-credentials-revoked',
      },
    );
  }

  if (phase === 'platform-credentials-revoked') {
    let nextRecord = record;
    if (record.platformResources) {
      const residual = requiredCapability(
        backend.assertDatabaseDeletionResidualsRemoved,
        'database-residuals',
      );
      const deletePlatformResourcesCandidate = backend.deletePlatformResources;
      if (typeof deletePlatformResourcesCandidate !== 'function') {
        throw new Error('backend cannot delete trusted platform resources');
      }
      const deletePlatformResources =
        deletePlatformResourcesCandidate.bind(backend);
      const database = await reconcilePersistedDatabase(
        backend,
        record,
        false,
        lease,
        true,
      );
      await lease.assertOwned();
      await deletePlatformResources(
        spec,
        record,
        database as DatabaseReference,
        lease,
      );
      await residual.call(
        backend,
        spec,
        record,
        database as DatabaseReference,
        lease,
      );
      nextRecord = clearRetainedReleases(record);
    }
    return commitRecord(
      lease,
      nextRecord,
      intent,
      clock,
      {},
      {
        state: 'transitioning',
        lifecyclePhase: 'platform-resources-deleted',
      },
    );
  }

  if (phase === 'platform-resources-deleted') {
    return commitRecord(
      lease,
      record,
      intent,
      clock,
      {},
      {
        state: 'transitioning',
        lifecyclePhase: 'application-resources-deleting',
      },
    );
  }
  throw new Error(`unsupported bounded decommission lifecycle '${phase}'`);
}

async function advanceR2Transition(
  options: AdvanceDecommissionDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: Exclude<DecommissionAdvanceIntent, { readonly state: 'complete' }>,
): Promise<FleetRecord> {
  const resources = record.applicationResources ?? [];
  const actionableIndex = resources.findIndex(
    (resource) => resource.state !== 'deleted',
  );
  const actionable =
    actionableIndex < 0 ? undefined : resources[actionableIndex];
  const needsInspection =
    resources.length > 0 &&
    (actionableIndex !== 0 ||
      (actionable !== undefined && actionable.state !== 'empty'));
  const findApplicationR2Bucket = needsInspection
    ? requiredCapability(
        options.backend.findApplicationR2Bucket,
        'application-r2-inspection',
      ).bind(options.backend)
    : undefined;
  const assertApplicationR2Empty =
    actionable?.state === 'detached' || actionable?.state === 'empty-authorized'
      ? requiredCapability(
          options.backend.assertApplicationR2Empty,
          'application-r2-empty',
        ).bind(options.backend)
      : undefined;
  const deleteApplicationR2Bucket =
    actionable?.state === 'empty' || actionable?.state === 'delete-authorized'
      ? requiredCapability(
          options.backend.deleteApplicationR2Bucket,
          'application-r2-delete',
        ).bind(options.backend)
      : undefined;
  if (actionable) {
    if (
      actionable.state === 'created' ||
      actionable.state === 'detach-authorized'
    ) {
      requireCapability(
        options.backend.advanceDecommissionAttachmentScan,
        'attachment-scan',
      );
    }
  }
  const result = await advanceApplicationR2Deletion({
    spec: options.spec,
    resources,
    backend: {
      ...(findApplicationR2Bucket ? { findApplicationR2Bucket } : {}),
      ...(assertApplicationR2Empty ? { assertApplicationR2Empty } : {}),
      ...(deleteApplicationR2Bucket ? { deleteApplicationR2Bucket } : {}),
    },
    fence: lease,
    startResourceIndex: 0,
  });
  const clock = options.clock ?? Date.now;
  if (result.status === 'complete') {
    return commitRecord(
      lease,
      record,
      intent,
      clock,
      {},
      {
        state: 'transitioning',
        lifecyclePhase: 'application-resources-deleted',
      },
    );
  }
  if (result.status === 'resource-advanced') {
    return commitRecord(
      lease,
      record,
      intent,
      clock,
      { applicationResources: result.resources },
      { state: 'transitioning' },
    );
  }
  const resource = result.resource;
  if (resource.creationDate === undefined) malformedResult();
  const purpose: DecommissionAttachmentPurpose = {
    kind: 'application-r2-detach',
    resourceIndex: result.resourceIndex,
    name: resource.name,
    bucketName: resource.bucketName,
    jurisdiction: resource.jurisdiction,
    reservationNonce: resource.reservationNonce,
    creationDate: resource.creationDate,
  };
  return commitRecord(
    lease,
    record,
    intent,
    clock,
    { applicationResources: result.resources },
    {
      state: 'discover',
      purpose,
      progress: initialWorkerAttachmentScan({
        kind: 'r2',
        bucketName: resource.bucketName,
      }),
      generation: intent.generation + 1,
    },
  );
}

type ActiveDecommissionIntent = Exclude<
  DecommissionAdvanceIntent,
  { readonly state: 'complete' }
>;

type ScanDecommissionIntent = Extract<
  DecommissionAdvanceIntent,
  { readonly state: 'discover' | 'verify' }
>;

function selectedReceiptAuthority(intent: ActiveDecommissionIntent): string {
  const authority = intent.databaseExportReceiptAuthority;
  if (typeof authority !== 'string' || authority.length === 0) {
    throw new DecommissionAdvanceIntentError();
  }
  return authority;
}

function databasePreDeletePurpose(
  record: FleetRecord,
): Extract<
  DecommissionAttachmentPurpose,
  { readonly kind: 'database-pre-delete' }
> {
  if (
    !boundedString(record.databaseExportLocation) ||
    typeof record.databaseExportSha256 !== 'string' ||
    !SHA256.test(record.databaseExportSha256) ||
    !Number.isSafeInteger(record.databaseExportSize) ||
    Number(record.databaseExportSize) < 1
  ) {
    throw new DecommissionAdvanceIntentError();
  }
  return {
    kind: 'database-pre-delete',
    databaseId: record.databaseId,
    exportLocation: record.databaseExportLocation,
    exportSha256: record.databaseExportSha256,
    exportSize: record.databaseExportSize as number,
  };
}

async function completeDatabaseDecommission(
  lease: FleetStateLease,
  record: FleetRecord,
  intent: ActiveDecommissionIntent,
  clock: () => number,
): Promise<FleetRecord> {
  const timestamp = nowIso(clock);
  const nextRecord: FleetRecord = {
    ...record,
    phase: 'decommissioned',
    updatedAt: timestamp,
  };
  const completeIntent: DecommissionAdvanceIntent = {
    version: 1,
    operationId: intent.operationId,
    revision: intent.revision + 1,
    generation: intent.generation,
    updatedAt: timestamp,
    identity: intent.identity,
    databaseExportReceiptAuthority: selectedReceiptAuthority(intent),
    lifecyclePhase: 'decommissioned',
    state: 'complete',
  };
  return writeIntent(lease, nextRecord, completeIntent);
}

async function advanceDatabaseTransition(
  options: AdvanceDecommissionDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: ActiveDecommissionIntent,
  receipt: ReceiptCapability,
): Promise<FleetRecord> {
  const clock = options.clock ?? Date.now;
  if (intent.lifecyclePhase === 'application-resources-deleted') {
    databaseExportReceiptIdentity(
      record,
      intent.operationId,
      receipt.authority,
      receipt.authority,
    );
    await reconcilePersistedDatabase(
      options.backend,
      record,
      false,
      lease,
      true,
    );
    return commitShellOnly(lease, record, intent, clock, {
      state: 'discover',
      databaseExportReceiptAuthority: receipt.authority,
      purpose: { kind: 'database-pre-export', databaseId: record.databaseId },
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: record.databaseId,
      }),
      generation: intent.generation + 1,
    });
  }
  if (intent.lifecyclePhase === 'database-exported') {
    await reconcilePersistedDatabase(
      options.backend,
      record,
      false,
      lease,
      true,
    );
    const purpose = databasePreDeletePurpose(record);
    return commitShellOnly(lease, record, intent, clock, {
      state: 'discover',
      purpose,
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: record.databaseId,
      }),
      generation: intent.generation + 1,
    });
  }
  if (intent.lifecyclePhase === 'database-deleting') {
    const database = await reconcilePersistedDatabase(
      options.backend,
      record,
      true,
      lease,
      true,
    );
    if (!database) {
      return completeDatabaseDecommission(lease, record, intent, clock);
    }
    const purpose = databasePreDeletePurpose(record);
    return commitShellOnly(lease, record, intent, clock, {
      state: 'discover',
      purpose,
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: record.databaseId,
      }),
      generation: intent.generation + 1,
    });
  }
  throw new Error(
    `unsupported bounded decommission database lifecycle '${intent.lifecyclePhase}'`,
  );
}

async function consumeDatabaseVerify(
  options: AdvanceDecommissionDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: Extract<DecommissionAdvanceIntent, { readonly state: 'verify' }>,
  receipt: ReceiptCapability,
): Promise<FleetRecord> {
  const allowAbsent = intent.lifecyclePhase === 'database-deleting';
  const database = await reconcilePersistedDatabase(
    options.backend,
    record,
    allowAbsent,
    lease,
    true,
  );
  const clock = options.clock ?? Date.now;
  if (!database) {
    return completeDatabaseDecommission(lease, record, intent, clock);
  }
  const residual = requiredCapability(
    options.backend.assertDatabaseDeletionResidualsRemoved,
    'database-residuals',
  ).bind(options.backend);
  await residual(options.spec, record, database, lease);
  if (intent.purpose.kind === 'database-pre-export') {
    await lease.assertOwned();
    const identity = databaseExportReceiptIdentity(
      record,
      intent.operationId,
      selectedReceiptAuthority(intent),
      receipt.authority,
    );
    const raw: unknown = await receipt.exportReceipt(identity, lease);
    const exported = databaseExportFromUnknown(raw, record.databaseId);
    return commitRecord(
      lease,
      record,
      intent,
      clock,
      {
        databaseExportLocation: exported.location,
        databaseExportSha256: exported.sha256,
        databaseExportSize: exported.size,
      },
      { state: 'transitioning', lifecyclePhase: 'database-exported' },
    );
  }
  if (intent.purpose.kind !== 'database-pre-delete') {
    return malformedResult();
  }
  const barrier = await commitRecord(
    lease,
    record,
    intent,
    clock,
    {},
    { state: 'transitioning', lifecyclePhase: 'database-deleting' },
  );
  return settleDatabaseDeletionUnderBarrier({
    lease,
    databaseId: record.databaseId,
    barrier,
    deleteDatabase: () => options.backend.deleteDatabase(database, lease),
    readDatabase: () => options.backend.getDatabase(record.databaseId),
  });
}

async function advanceAttachmentScan(
  options: AdvanceDecommissionDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: ScanDecommissionIntent,
  receipt: ReceiptCapability,
): Promise<FleetRecord> {
  let inspectionBackend:
    | Readonly<{
        findApplicationR2Bucket: NonNullable<
          ProvisioningBackend['findApplicationR2Bucket']
        >;
      }>
    | undefined;
  if (intent.purpose.kind === 'application-r2-detach') {
    const resource =
      record.applicationResources?.[intent.purpose.resourceIndex];
    if (!resource) malformedResult();
    assertApplicationR2ReservationIdentity(options.spec, resource);
    const findApplicationR2Bucket = requiredCapability(
      options.backend.findApplicationR2Bucket,
      'application-r2-inspection',
    ).bind(options.backend);
    inspectionBackend = { findApplicationR2Bucket };
    const preflight = await advanceApplicationR2Deletion({
      spec: options.spec,
      resources: record.applicationResources ?? [],
      backend: inspectionBackend,
      fence: lease,
      startResourceIndex: 0,
    });
    if (
      preflight.status !== 'detachment-required' ||
      preflight.resourceIndex !== intent.purpose.resourceIndex
    ) {
      malformedResult();
    }
  } else {
    const database = await reconcilePersistedDatabase(
      options.backend,
      record,
      intent.lifecyclePhase === 'database-deleting',
      lease,
      true,
    );
    if (!database) {
      return completeDatabaseDecommission(
        lease,
        record,
        intent,
        options.clock ?? Date.now,
      );
    }
  }
  const scan = requiredCapability(
    options.backend.advanceDecommissionAttachmentScan,
    'attachment-scan',
  ).bind(options.backend);
  const clock = options.clock ?? Date.now;
  return advanceDecommissionAttachmentScanStep({
    intent,
    scan,
    maxProviderRequests: options.maxProviderRequests,
    signal: options.signal,
    persist: (transition) =>
      commitShellOnly(lease, record, intent, clock, transition),
    consumeMatchingVerify: async ({ intent: verified }) => {
      if (verified.purpose.kind !== 'application-r2-detach') {
        return consumeDatabaseVerify(options, lease, record, verified, receipt);
      }
      await lease.assertOwned();
      const detached = await advanceApplicationR2Deletion({
        spec: options.spec,
        resources: record.applicationResources ?? [],
        backend: inspectionBackend as NonNullable<typeof inspectionBackend>,
        fence: lease,
        startResourceIndex: 0,
        verifiedDetachmentResourceIndex: verified.purpose.resourceIndex,
      });
      if (
        detached.status !== 'resource-advanced' ||
        detached.resourceIndex !== verified.purpose.resourceIndex ||
        detached.resources[detached.resourceIndex]?.state !== 'detached'
      ) {
        return malformedResult();
      }
      return commitRecord(
        lease,
        record,
        verified,
        clock,
        { applicationResources: detached.resources },
        { state: 'transitioning' },
      );
    },
  });
}

function startRecord(
  record: FleetRecord,
  spec: DeploymentSpec,
  operationId: string,
  timestamp: string,
): FleetRecord {
  const currentPhase = effectiveLifecyclePhase(record);
  if (
    !NORMAL_ENTRY_PHASES.has(currentPhase as NormalDecommissionLifecyclePhase)
  ) {
    throw new Error(
      `cannot start bounded decommission in phase '${currentPhase}'`,
    );
  }
  const lifecyclePhase = currentPhase as NormalDecommissionLifecyclePhase;
  const source: FleetRecord = {
    ...record,
    phase: 'decommission-advancing',
    updatedAt: timestamp,
  };
  const intent: DecommissionAdvanceIntent = {
    version: 1,
    operationId,
    revision: 0,
    generation: 0,
    updatedAt: timestamp,
    identity: {
      record: {
        tenantTag: record.tenantTag,
        environment: record.environment,
        backend: record.backend,
        scriptName: record.scriptName,
        databaseId: record.databaseId,
        databaseName: record.databaseName,
        routeHostname: record.routeHostname,
      },
      mode: {
        kind: 'normal',
        requestedSpecDigest: deploymentSpecDigest(spec),
        entryLifecyclePhase: lifecyclePhase,
      },
    },
    lifecyclePhase,
    state: 'transitioning',
  };
  return {
    ...source,
    decommissionIntent: normalizeIntent(intent, source),
  };
}

async function advanceUnderLease(
  options: AdvanceDecommissionDeploymentOptions,
  action: DecommissionAdvanceAction,
  parsedToken: DecommissionAdvanceToken | undefined,
  lease: FleetStateLease,
): Promise<DecommissionAdvanceResult> {
  const { backend, spec, store } = options;
  let record = await store.get(spec.tenantTag, spec.environment);
  if (!record) {
    if (action.kind === 'start')
      throw new Error('deployment is not registered');
    throw new DecommissionAdvanceTokenOperationError();
  }
  // Admission-path guard only: continue/restart against a cleanup-advancing
  // row already fails token classification (no decommission intent exists).
  if (action.kind === 'start') {
    assertNoActiveCleanup(record, 'advanceDecommissionDeployment');
  }
  const intent = record.decommissionIntent
    ? normalizeDecommissionAdvanceIntent(
        record.decommissionIntent,
        omitIntent(record),
      )
    : undefined;
  if (action.kind === 'start' && intent) {
    assertNormalAuthority(record, backend, spec, intent);
    if (
      intent.state !== 'blocked' &&
      intent.state !== 'complete' &&
      intent.databaseExportReceiptAuthority !== undefined
    ) {
      assertNormalDecommissionD1ResourcesDeleted(record);
      const receipt = receiptCapability(backend);
      databaseExportReceiptIdentity(
        record,
        intent.operationId,
        selectedReceiptAuthority(intent),
        receipt.authority,
      );
    }
    return authoritativeResult({ ...record, decommissionIntent: intent });
  }
  if (action.kind === 'start') {
    assertNormalAuthority(record, backend, spec);
    assertSupportedEntryLifecycle(record);
    if (effectiveLifecyclePhase(record) === 'application-resources-deleted') {
      assertNormalDecommissionD1ResourcesDeleted(record);
    }
    validateReservations(spec, record);
    const resources = record.applicationResources ?? [];
    assertCompleteApplicationR2Reservations(resources);
    requireStartCapabilities(backend, resources);
    const receipt = receiptCapability(backend);
    const operationId = options.randomUUID();
    parseDecommissionAdvanceToken({
      version: 1,
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      operationId,
      revision: 0,
    });
    databaseExportReceiptIdentity(
      record,
      operationId,
      receipt.authority,
      receipt.authority,
    );
    record = startRecord(
      record,
      spec,
      operationId,
      nowIso(options.clock ?? Date.now),
    );
    await lease.put(record);
    return authoritativeResult(record);
  }
  if (!parsedToken || !intent) {
    throw new DecommissionAdvanceTokenOperationError();
  }
  record = { ...record, decommissionIntent: intent };
  const classification = classifyDecommissionAdvanceToken(parsedToken, record);
  if (classification === 'stale') return authoritativeResult(record);
  if (action.kind === 'restart-blocked') {
    if (intent.state !== 'blocked') throw new DecommissionAdvanceRestartError();
    assertNormalAuthority(record, backend, spec, intent);
    assertCompleteApplicationR2Reservations(record.applicationResources ?? []);
    if (intent.purpose.kind !== 'application-r2-detach') {
      assertNormalDecommissionD1ResourcesDeleted(record);
    }
    const receipt = receiptCapability(backend);
    if (intent.purpose.kind !== 'application-r2-detach') {
      databaseExportReceiptIdentity(
        record,
        intent.operationId,
        selectedReceiptAuthority(intent),
        receipt.authority,
      );
    }
    requireCapability(
      backend.advanceDecommissionAttachmentScan,
      'attachment-scan',
    );
    if (intent.purpose.kind === 'application-r2-detach') {
      const resource =
        record.applicationResources?.[intent.purpose.resourceIndex];
      if (!resource) malformedResult();
      assertApplicationR2ReservationIdentity(spec, resource);
      if (intent.purpose.resourceIndex > 0) {
        const findApplicationR2Bucket = requiredCapability(
          backend.findApplicationR2Bucket,
          'application-r2-inspection',
        ).bind(backend);
        const prefix = await advanceApplicationR2Deletion({
          spec,
          resources:
            record.applicationResources?.slice(
              0,
              intent.purpose.resourceIndex,
            ) ?? [],
          backend: { findApplicationR2Bucket },
          fence: lease,
          startResourceIndex: 0,
        });
        if (prefix.status !== 'complete') malformedResult();
      }
    }
    record = await commitShellOnly(
      lease,
      record,
      intent,
      options.clock ?? Date.now,
      {
        state: 'discover',
        purpose: intent.purpose,
        progress: initialWorkerAttachmentScan(purposeTarget(intent.purpose)),
        generation: intent.generation + 1,
      },
    );
    return authoritativeResult(record);
  }
  if (intent.state === 'blocked' || intent.state === 'complete') {
    return authoritativeResult(record);
  }
  assertNormalAuthority(record, backend, spec, intent);
  assertCompleteApplicationR2Reservations(record.applicationResources ?? []);
  const isD1Action = intent.databaseExportReceiptAuthority !== undefined;
  if (isD1Action || intent.lifecyclePhase === 'application-resources-deleted') {
    assertNormalDecommissionD1ResourcesDeleted(record);
  }
  const receipt = receiptCapability(backend);
  if (isD1Action) {
    databaseExportReceiptIdentity(
      record,
      intent.operationId,
      selectedReceiptAuthority(intent),
      receipt.authority,
    );
  }
  if (intent.state === 'discover' || intent.state === 'verify') {
    record = await advanceAttachmentScan(
      options,
      lease,
      record,
      intent,
      receipt,
    );
  } else if (intent.lifecyclePhase === 'application-resources-deleting') {
    record = await advanceR2Transition(options, lease, record, intent);
  } else if (
    intent.lifecyclePhase === 'application-resources-deleted' ||
    intent.lifecyclePhase === 'database-exported' ||
    intent.lifecyclePhase === 'database-deleting'
  ) {
    record = await advanceDatabaseTransition(
      options,
      lease,
      record,
      intent,
      receipt,
    );
  } else {
    record = await advanceLifecycle(options, lease, record, intent);
  }
  return authoritativeResult(record);
}

/**
 * Starts, reads, or advances one normal decommission operation.
 *
 * One call performs at most one bounded scan or lifecycle/resource action group.
 */
export async function advanceDecommissionDeployment(
  options: AdvanceDecommissionDeploymentOptions,
): Promise<DecommissionAdvanceResult> {
  validateDeploymentSpec(options.spec);
  assertWorkerAttachmentProviderRequestBudget(options.maxProviderRequests);
  const action = decommissionAdvanceActionFromUnknown(options.action);
  if (typeof options.randomUUID !== 'function') {
    throw new Error(
      'advanceDecommissionDeployment requires a randomUUID function',
    );
  }
  const parsedToken =
    action.kind === 'start'
      ? undefined
      : parseDecommissionAdvanceToken(action.token);
  if (
    parsedToken &&
    (parsedToken.tenantTag !== options.spec.tenantTag ||
      parsedToken.environment !== options.spec.environment)
  ) {
    throw new DecommissionAdvanceTokenDeploymentError();
  }
  return options.store.withDeploymentLease(
    options.spec.tenantTag,
    options.spec.environment,
    (lease) => advanceUnderLease(options, action, parsedToken, lease),
  );
}
