// SPDX-License-Identifier: Apache-2.0

import {
  advanceApplicationR2Deletion,
  assertApplicationR2EmptyBeforeDecommission,
} from './application-bindings.js';
import {
  CleanupAdvanceTokenDeploymentError,
  CleanupAdvanceTokenOperationError,
  classifyCleanupAdvanceToken,
  classifyCleanupDatabaseEligibility,
  normalizeCleanupAdvanceIntent,
  parseCleanupAdvanceToken,
} from './cleanup-intent.js';
import {
  assertWorkerAttachmentProviderRequestBudget,
  initialWorkerAttachmentScan,
  parseWorkerAttachmentScanProgress,
  WORKER_ATTACHMENT_EVIDENCE_BOUND,
} from './cloudflare-worker-attachment-scan-state.js';
import {
  activeExternalRelease,
  assertImmutableDeploymentMapping,
  reconcilePersistedDatabase,
  retainedExternalReleases,
} from './decommission-advance.js';
import { settleDatabaseDeletionUnderBarrier } from './decommission-database.js';
import { deploymentSpecDigest } from './spec-digest.js';
import { cloneBoundedPlainData } from './strict-plain-data.js';
import type {
  ApplicationR2Resource,
  CleanupAdvanceIntent,
  CleanupAdvanceState,
  CleanupAdvanceToken,
  CleanupAttachmentProgress,
  CleanupAttachmentPurpose,
  CleanupAttachmentScan,
  CleanupAuthority,
  CleanupReceiptEvidence,
  CleanupTerminalReceipt,
  DatabaseReference,
  DecommissionBlockedAttachment,
  DeploymentSpec,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  ProvisioningBackend,
} from './types.js';
import { assertNoActiveDecommission } from './types.js';
import { validateDeploymentSpec } from './validation.js';

const ACTION_ERROR = 'cleanup advance action is malformed';
const RESULT_ERROR = 'bounded cleanup attachment result is malformed';
const ATTACHMENT_STRING_BYTE_BOUND = 4_096;
const RESULT_PLAIN_DATA_DEPTH_BOUND = 64;
const RESULT_PLAIN_DATA_NODE_BOUND = 8_192;
const RESULT_PLAIN_DATA_BYTE_BOUND = 96 * 1_024;
const SHA256 = /^[0-9a-f]{64}$/u;
const STRUCTURED_CLONE = structuredClone;

/** Caller command for one bounded cleanup invocation. */
export type CleanupAdvanceAction =
  | Readonly<{
      /**
       * Create a new manual-cleanup operation, or read/resume an existing
       * operation of EITHER authority. New operations are manual-cleanup only;
       * rollback operations are created internally by the failed-provision
       * path.
       */
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

/** Inputs for one lease-scoped bounded cleanup action group. */
export interface AdvanceCleanupDeploymentOptions {
  readonly backend: ProvisioningBackend;
  readonly store: FleetStateStore;
  readonly spec: DeploymentSpec;
  /** Typed command; its token remains an unknown strict-codec boundary. */
  readonly action: CleanupAdvanceAction;
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
export type CleanupAdvanceResult =
  | Readonly<{
      /** More work remains; the token, not this status, is continuation input. */
      status: 'pending';
      token: CleanupAdvanceToken;
    }>
  | Readonly<{
      /** Provider attachment blocks deletion until explicit restart. */
      status: 'blocked';
      token: CleanupAdvanceToken;
      purpose: CleanupAttachmentPurpose;
      attachment: DecommissionBlockedAttachment;
    }>
  | Readonly<{
      /** Terminal receipt persisted atomically with claims release and row deletion. */
      status: 'complete';
      token: CleanupAdvanceToken;
      receipt: CleanupTerminalReceipt;
    }>;

/** Named capability whose absence makes bounded cleanup work fail closed. */
export type CleanupAdvanceCapability =
  | 'attachment-scan'
  | 'database-residuals'
  | 'database-read'
  | 'database-delete'
  | 'application-r2-inspection'
  | 'application-r2-empty'
  | 'application-r2-detach'
  | 'application-r2-delete'
  | 'terminal-receipt'
  | 'receipt-read';

const CAPABILITY_MESSAGES: Readonly<Record<CleanupAdvanceCapability, string>> =
  Object.freeze({
    'attachment-scan':
      'backend cannot perform bounded cleanup attachment scans',
    'database-residuals': 'backend cannot inspect database deletion residuals',
    'database-read': 'backend cannot read the database for bounded cleanup',
    'database-delete': 'backend cannot delete the database for bounded cleanup',
    'application-r2-inspection':
      'backend cannot inspect application R2 resources',
    'application-r2-empty': 'backend cannot attest application R2 emptiness',
    'application-r2-detach': 'backend cannot attest application R2 detachment',
    'application-r2-delete': 'backend cannot delete application R2 resources',
    'terminal-receipt':
      'state lease cannot complete cleanup with an atomic terminal receipt',
    'receipt-read': 'state store cannot read cleanup terminal receipts',
  });

/** Fixed configuration refusal for one missing bounded capability. */
export class CleanupAdvanceCapabilityError extends Error {
  constructor(readonly capability: CleanupAdvanceCapability) {
    super(CAPABILITY_MESSAGES[capability]);
    this.name = 'CleanupAdvanceCapabilityError';
  }
}

/** Refusal for restart without an exact current blocked operation. */
export class CleanupAdvanceRestartError extends Error {
  constructor() {
    super('cleanup advance restart requires a current blocked operation');
    this.name = 'CleanupAdvanceRestartError';
  }
}

const REFUSAL_MESSAGES = Object.freeze({
  'invocation-authorized':
    'deployment candidate invocation was durably authorized; use export-backed decommissioning',
  'legacy-phase-ambiguous':
    'legacy deployment phase cannot rule out candidate invocation; use export-backed decommissioning',
  'carrier-phase-inconsistent':
    'invocation authority carrier is inconsistent with the deployment phase; use export-backed decommissioning',
  'malformed-carrier':
    'invocation authority carrier is malformed; use export-backed decommissioning',
  'untrusted-data-binding':
    'deployment carries an untrusted data binding; use export-backed decommissioning',
  'external-staging-evidence':
    'deployment carries external staging evidence; use export-backed decommissioning',
});

type CleanupEligibilityRefusal = Extract<
  ReturnType<typeof classifyCleanupDatabaseEligibility>,
  { readonly eligible: false }
>;

function eligibilityRefusal(
  classification: CleanupEligibilityRefusal,
  phase: FleetRecord['phase'],
): Error {
  if (classification.reason === 'phase-requires-decommission') {
    return new Error(
      `deployment in phase '${phase}' requires export-backed decommissioning`,
    );
  }
  return new Error(REFUSAL_MESSAGES[classification.reason]);
}

function requireCapability(
  available: unknown,
  capability: CleanupAdvanceCapability,
): asserts available is (...arguments_: never[]) => unknown {
  if (typeof available !== 'function') {
    throw new CleanupAdvanceCapabilityError(capability);
  }
}

function requiredCapability<Value>(
  available: Value,
  capability: CleanupAdvanceCapability,
): NonNullable<Value> {
  requireCapability(available, capability);
  return available as NonNullable<Value>;
}

function malformedAction(): never {
  throw new Error(ACTION_ERROR);
}

function cleanupActionFromUnknown(value: unknown): CleanupAdvanceAction {
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
    return { plane, scriptName, dispatchNamespace };
  }
  return malformedResult();
}

function nowIso(clock: () => number): string {
  return new Date(clock()).toISOString();
}

function omitIntent(record: FleetRecord): Omit<FleetRecord, 'cleanupIntent'> {
  const { cleanupIntent: _intent, ...source } = record;
  return source;
}

function normalizeIntent(
  intent: CleanupAdvanceIntent,
  record: FleetRecord,
): CleanupAdvanceIntent {
  return normalizeCleanupAdvanceIntent(intent, omitIntent(record));
}

function tokenFor(record: FleetRecord): CleanupAdvanceToken {
  const intent = record.cleanupIntent;
  if (!intent) throw new CleanupAdvanceTokenOperationError();
  return {
    version: 1,
    tenantTag: record.tenantTag,
    environment: record.environment,
    operationId: intent.operationId,
    revision: intent.revision,
  };
}

function authoritativeResult(record: FleetRecord): CleanupAdvanceResult {
  const intent = record.cleanupIntent;
  if (!intent) throw new CleanupAdvanceTokenOperationError();
  const token = tokenFor(record);
  if (intent.state.step === 'blocked') {
    return {
      status: 'blocked',
      token,
      purpose: intent.state.purpose,
      attachment: intent.state.attachment,
    };
  }
  return { status: 'pending', token };
}

function nextIntent(
  intent: CleanupAdvanceIntent,
  timestamp: string,
  state: CleanupAdvanceState,
  generation?: number,
): CleanupAdvanceIntent {
  return {
    version: 1,
    operationId: intent.operationId,
    revision: intent.revision + 1,
    generation: generation ?? intent.generation,
    updatedAt: timestamp,
    authority: intent.authority,
    identity: intent.identity,
    state,
  };
}

async function commit(
  lease: FleetStateLease,
  record: FleetRecord,
  intent: CleanupAdvanceIntent,
  clock: () => number,
  recordValues: Readonly<Partial<Pick<FleetRecord, 'applicationResources'>>>,
  state: CleanupAdvanceState,
  generation?: number,
): Promise<FleetRecord> {
  const timestamp = nowIso(clock);
  const source: FleetRecord = {
    ...omitIntent(record),
    ...recordValues,
    updatedAt: timestamp,
  };
  const next: FleetRecord = {
    ...source,
    cleanupIntent: normalizeCleanupAdvanceIntent(
      nextIntent(intent, timestamp, state, generation),
      source,
    ),
  };
  await lease.put(next);
  return next;
}

function assertBackendSwitchInactiveForCleanup(record: FleetRecord): void {
  // Byte-identical to backend-switch.ts assertBackendSwitchInactive; the
  // transport-neutral rule forbids importing that module from this engine.
  if (
    record.backendSwitchIntent &&
    record.backendSwitchIntent.subphase !== 'rolled-back' &&
    record.backendSwitchIntent.subphase !== 'finalized'
  ) {
    throw new Error(
      `deployment '${record.tenantTag}:${record.environment}' has active backend switch '${record.backendSwitchIntent.subphase}'`,
    );
  }
}

function assertCleanupCaller(
  record: FleetRecord,
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
  intent: CleanupAdvanceIntent,
): void {
  if (record.backend !== backend.kind) {
    throw new Error('cleanup backend does not own this deployment');
  }
  assertImmutableDeploymentMapping(record, backend, spec);
  if (
    intent.authority.kind === 'provisioning-rollback' &&
    intent.authority.requestedSpecDigest !== deploymentSpecDigest(spec)
  ) {
    throw new Error('cleanup retry uses a different requested specification');
  }
}

function assertRollbackDeletionAuthority(
  intent: CleanupAdvanceIntent,
  requireDatabaseOwned: boolean,
): void {
  if (intent.authority.kind !== 'provisioning-rollback') return;
  if (
    !intent.authority.reservationOwned ||
    (requireDatabaseOwned && !intent.authority.databaseOwned)
  ) {
    throw new Error(
      'provisioning rollback cannot delete a database the attempt does not own',
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
  if (
    resources.some(
      (resource) =>
        resource.state === 'created' || resource.state === 'detach-authorized',
    )
  ) {
    requireCapability(
      backend.assertApplicationR2Detached,
      'application-r2-detach',
    );
  }
}

function recheckEligibility(
  record: FleetRecord,
  intent: CleanupAdvanceIntent,
): CleanupReceiptEvidence['eligibility'] {
  // Synthetic input: the live phase is 'cleanup-advancing', which the
  // classifier would always refuse; the eligibility re-check replays the
  // persisted admitted phase and externalArtifact against the LIVE carrier.
  const classification = classifyCleanupDatabaseEligibility({
    record: { ...omitIntent(record), phase: intent.identity.admittedPhase },
    externalArtifact: intent.identity.externalArtifact,
  });
  if (!classification.eligible) {
    throw eligibilityRefusal(classification, intent.identity.admittedPhase);
  }
  return classification.eligibility;
}

function persistedDatabase(record: FleetRecord): DatabaseReference {
  return { id: record.databaseId, name: record.databaseName, created: false };
}

function scanPurpose(intent: CleanupAdvanceIntent): CleanupAttachmentPurpose {
  return {
    kind: 'cleanup-database-pre-delete',
    databaseId: intent.identity.record.databaseId,
    operationId: intent.operationId,
  };
}

async function completeTerminal(
  lease: FleetStateLease,
  record: FleetRecord,
  intent: CleanupAdvanceIntent,
  expectedRevision: number,
  eligibility: CleanupReceiptEvidence['eligibility'],
  disposition: CleanupTerminalReceipt['disposition'],
): Promise<CleanupAdvanceResult> {
  const completeCleanup = requiredCapability(
    lease.completeCleanup,
    'terminal-receipt',
  ).bind(lease);
  const teardown =
    intent.identity.admittedPhase !== 'database-reserved' &&
    intent.identity.admittedPhase !== 'database-create-authorized';
  const receipt: CleanupTerminalReceipt = {
    version: 1,
    operationId: intent.operationId,
    tenantTag: record.tenantTag,
    environment: record.environment,
    backend: record.backend,
    scriptName: record.scriptName,
    databaseId: record.databaseId,
    databaseName: record.databaseName,
    authority: intent.authority.kind,
    admittedPhase: intent.identity.admittedPhase,
    disposition,
    evidence: {
      eligibility,
      ingressRemoved: teardown,
      workerAbsent: teardown,
      platformResourcesAbsent: teardown,
      applicationR2Settled: teardown,
      databaseAbsentReadback: true,
    },
  };
  const persisted = await completeCleanup({ receipt, expectedRevision });
  return {
    status: 'complete',
    token: {
      version: 1,
      tenantTag: record.tenantTag,
      environment: record.environment,
      operationId: intent.operationId,
      revision: expectedRevision,
    },
    receipt: persisted,
  };
}

async function advanceDatabaseDeletion(
  options: AdvanceCleanupDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: CleanupAdvanceIntent,
): Promise<CleanupAdvanceResult> {
  const { backend, spec } = options;
  requireCapability(lease.completeCleanup, 'terminal-receipt');
  const admittedPhase = intent.identity.admittedPhase;
  if (
    admittedPhase === 'database-reserved' ||
    admittedPhase === 'database-create-authorized'
  ) {
    const eligibility = recheckEligibility(record, intent);
    const reserved = await backend.findDatabase(spec);
    if (admittedPhase === 'database-reserved') {
      if (reserved) {
        throw new Error(
          `refusing to clear an unauthorized database reservation while '${reserved.id}:${reserved.name}' exists`,
        );
      }
      return completeTerminal(
        lease,
        record,
        intent,
        intent.revision,
        eligibility,
        'reservation-cleared',
      );
    }
    if (!reserved) {
      return completeTerminal(
        lease,
        record,
        intent,
        intent.revision,
        eligibility,
        'reservation-cleared',
      );
    }
    requireCapability(backend.deleteDatabase, 'database-delete');
    if (reserved.name !== record.databaseName) {
      throw new Error(
        `authorized database '${record.databaseName}' resolved with unexpected identity '${reserved.id}:${reserved.name}'`,
      );
    }
    const owner = await backend.readDeploymentIdentity(reserved, lease);
    if (owner !== undefined) {
      throw new Error(
        `refusing reserved database cleanup for '${reserved.id}' owned by '${owner}'`,
      );
    }
    assertRollbackDeletionAuthority(intent, false);
    await lease.assertOwned();
    // A freshness PROOF, not a provisioning: the sentinel exists only so the
    // read-back below can show the database was empty, and it is deleted
    // immediately after; 'migration-locked' keeps a database that survives a
    // failed delete from ever coming back as one that executes.
    await backend.seedDeploymentIdentity(reserved, record.tenantTag, lease, {
      initialExecutionFenceState: 'migration-locked',
    });
    const seededOwner = await backend.readDeploymentIdentity(reserved, lease);
    if (seededOwner !== record.tenantTag) {
      throw new Error(
        `reserved database '${reserved.id}' could not be proven fresh before cleanup`,
      );
    }
    await lease.assertOwned();
    await backend.deleteDatabase(reserved, lease);
    const remaining = await backend.findDatabase(spec);
    if (remaining) {
      throw new Error(
        `reserved database '${remaining.id}' is still present after deletion`,
      );
    }
    return completeTerminal(
      lease,
      record,
      intent,
      intent.revision,
      eligibility,
      'prepublication-owned-no-export',
    );
  }
  requireCapability(backend.getDatabase, 'database-read');
  requireCapability(backend.deleteDatabase, 'database-delete');
  const residual = requiredCapability(
    backend.assertDatabaseDeletionResidualsRemoved,
    'database-residuals',
  ).bind(backend);
  const database = await reconcilePersistedDatabase(
    backend,
    record,
    true,
    lease,
    admittedPhase !== 'database-created',
  );
  if (!database) {
    const eligibility = recheckEligibility(record, intent);
    return completeTerminal(
      lease,
      record,
      intent,
      intent.revision,
      eligibility,
      'reservation-cleared',
    );
  }
  await residual(spec, record, database, lease);
  const eligibility = recheckEligibility(record, intent);
  assertRollbackDeletionAuthority(intent, true);
  await settleDatabaseDeletionUnderBarrier({
    lease,
    databaseId: record.databaseId,
    barrier: record,
    deleteDatabase: () => backend.deleteDatabase(database, lease),
    readDatabase: () => backend.getDatabase(record.databaseId),
  });
  return completeTerminal(
    lease,
    record,
    intent,
    intent.revision,
    eligibility,
    'prepublication-owned-no-export',
  );
}

async function advanceTeardown(
  options: AdvanceCleanupDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: CleanupAdvanceIntent,
): Promise<FleetRecord> {
  const { backend, spec } = options;
  const clock = options.clock ?? Date.now;
  const database = persistedDatabase(record);
  const step = intent.state.step;
  if (step === 'teardown-traffic') {
    const resources = record.applicationResources ?? [];
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
    }
    await lease.assertOwned();
    await backend.removeTraffic(
      spec,
      retainedExternalReleases(record),
      activeExternalRelease(record),
      database,
      lease,
    );
    await backend.assertTrafficRemoved(spec);
    if (preflightBackend) {
      await assertApplicationR2EmptyBeforeDecommission({
        resources,
        backend: preflightBackend,
        fence: lease,
      });
    }
    return commit(
      lease,
      record,
      intent,
      clock,
      {},
      { step: 'teardown-worker' },
    );
  }
  if (step === 'teardown-worker') {
    await lease.assertOwned();
    await backend.revokeCredentials(
      spec,
      retainedExternalReleases(record),
      activeExternalRelease(record),
      database,
      lease,
    );
    await lease.assertOwned();
    await backend.deleteWorker(
      spec,
      retainedExternalReleases(record),
      database,
      activeExternalRelease(record),
      lease,
    );
    return commit(
      lease,
      record,
      intent,
      clock,
      {},
      record.platformResources || record.platformTarget
        ? { step: 'teardown-platform' }
        : { step: 'r2-deletion', startResourceIndex: 0 },
    );
  }
  if (
    !backend.revokePlatformResourceCredentials ||
    !backend.deletePlatformResources
  ) {
    throw new Error(
      'backend cannot clean persisted trusted platform resources',
    );
  }
  await lease.assertOwned();
  await backend.revokePlatformResourceCredentials(
    spec,
    record,
    database,
    lease,
  );
  await lease.assertOwned();
  await backend.deletePlatformResources(spec, record, database, lease);
  return commit(
    lease,
    record,
    intent,
    clock,
    {},
    {
      step: 'r2-deletion',
      startResourceIndex: 0,
    },
  );
}

async function advanceR2Deletion(
  options: AdvanceCleanupDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: CleanupAdvanceIntent,
  state: Extract<CleanupAdvanceState, { readonly step: 'r2-deletion' }>,
): Promise<FleetRecord> {
  const { backend, spec } = options;
  const clock = options.clock ?? Date.now;
  const resources = record.applicationResources ?? [];
  const pending = resources.slice(state.startResourceIndex);
  const actionableOffset = pending.findIndex(
    (resource) => resource.state !== 'deleted',
  );
  const actionable =
    actionableOffset < 0 ? undefined : pending[actionableOffset];
  const needsInspection =
    resources.length > 0 &&
    (actionableOffset !== 0 ||
      (actionable !== undefined && actionable.state !== 'empty'));
  const findApplicationR2Bucket = needsInspection
    ? requiredCapability(
        backend.findApplicationR2Bucket,
        'application-r2-inspection',
      ).bind(backend)
    : undefined;
  const assertApplicationR2Empty =
    actionable?.state === 'detached' || actionable?.state === 'empty-authorized'
      ? requiredCapability(
          backend.assertApplicationR2Empty,
          'application-r2-empty',
        ).bind(backend)
      : undefined;
  const deleteApplicationR2Bucket =
    actionable?.state === 'empty' || actionable?.state === 'delete-authorized'
      ? requiredCapability(
          backend.deleteApplicationR2Bucket,
          'application-r2-delete',
        ).bind(backend)
      : undefined;
  const detachmentPossible =
    actionable !== undefined &&
    (actionable.state === 'created' ||
      (actionable.state === 'detach-authorized' &&
        state.verifiedDetachmentResourceIndex === undefined));
  if (detachmentPossible) {
    requireCapability(
      backend.assertApplicationR2Detached,
      'application-r2-detach',
    );
  }
  const result = await advanceApplicationR2Deletion({
    spec,
    resources,
    backend: {
      ...(findApplicationR2Bucket ? { findApplicationR2Bucket } : {}),
      ...(assertApplicationR2Empty ? { assertApplicationR2Empty } : {}),
      ...(deleteApplicationR2Bucket ? { deleteApplicationR2Bucket } : {}),
    },
    fence: lease,
    startResourceIndex: state.startResourceIndex,
    ...(state.verifiedDetachmentResourceIndex !== undefined
      ? {
          verifiedDetachmentResourceIndex:
            state.verifiedDetachmentResourceIndex,
        }
      : {}),
  });
  if (result.status === 'complete') {
    return commit(
      lease,
      record,
      intent,
      clock,
      {},
      {
        step: 'attachment-scan',
        scan: {
          purpose: scanPurpose(intent),
          pass: 'discover',
          progress: initialWorkerAttachmentScan({
            kind: 'd1',
            databaseId: record.databaseId,
          }),
        },
      },
      intent.generation + 1,
    );
  }
  if (result.status === 'resource-advanced') {
    const advanced = result.resources[result.resourceIndex];
    return commit(
      lease,
      record,
      intent,
      clock,
      { applicationResources: result.resources },
      {
        step: 'r2-deletion',
        startResourceIndex:
          advanced?.state === 'deleted'
            ? state.startResourceIndex + 1
            : state.startResourceIndex,
      },
    );
  }
  const assertApplicationR2Detached = requiredCapability(
    backend.assertApplicationR2Detached,
    'application-r2-detach',
  ).bind(backend);
  await lease.assertOwned();
  await assertApplicationR2Detached(result.resource, lease);
  return commit(
    lease,
    record,
    intent,
    clock,
    { applicationResources: result.resources },
    {
      step: 'r2-deletion',
      startResourceIndex: result.resourceIndex,
      verifiedDetachmentResourceIndex: result.resourceIndex,
    },
  );
}

async function advanceAttachmentScan(
  options: AdvanceCleanupDeploymentOptions,
  lease: FleetStateLease,
  record: FleetRecord,
  intent: CleanupAdvanceIntent,
  state: Extract<CleanupAdvanceState, { readonly step: 'attachment-scan' }>,
): Promise<CleanupAdvanceResult> {
  const scan = requiredCapability(
    options.backend.advanceDecommissionAttachmentScan,
    'attachment-scan',
  ).bind(options.backend);
  const clock = options.clock ?? Date.now;
  const target = {
    kind: 'd1',
    databaseId: state.scan.purpose.databaseId,
  } as const;
  const restartDiscover = () =>
    commit(
      lease,
      record,
      intent,
      clock,
      {},
      {
        step: 'attachment-scan',
        scan: {
          purpose: state.scan.purpose,
          pass: 'discover',
          progress: initialWorkerAttachmentScan(target),
        },
      },
      intent.generation + 1,
    );
  const raw = await scan({
    progress: state.scan.progress,
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
    return authoritativeResult(await restartDiscover());
  }
  assertReservedAttempts(
    result.providerFetchAttemptsReserved,
    options.maxProviderRequests,
  );
  if (result.status === 'pending') {
    let progress: CleanupAttachmentProgress;
    try {
      progress = parseWorkerAttachmentScanProgress(result.progress, target);
    } catch {
      return malformedResult();
    }
    let scanState: CleanupAttachmentScan;
    if (state.scan.pass === 'verify') {
      const discoverEvidence = state.scan.discoverEvidence;
      if (!discoverEvidence) return malformedResult();
      scanState = {
        purpose: state.scan.purpose,
        pass: 'verify',
        progress,
        discoverEvidence,
      };
    } else {
      scanState = { purpose: state.scan.purpose, pass: 'discover', progress };
    }
    return authoritativeResult(
      await commit(
        lease,
        record,
        intent,
        clock,
        {},
        {
          step: 'attachment-scan',
          scan: scanState,
        },
      ),
    );
  }
  if (result.status === 'attached') {
    const attachment = safeAttachment(result.attachment);
    return authoritativeResult(
      await commit(
        lease,
        record,
        intent,
        clock,
        {},
        {
          step: 'blocked',
          purpose: state.scan.purpose,
          attachment,
        },
      ),
    );
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
    !SHA256.test(evidence.evidenceSha256) ||
    evidence.evidenceCount < 2 ||
    evidence.evidenceCount > WORKER_ATTACHMENT_EVIDENCE_BOUND
  ) {
    return malformedResult();
  }
  if (state.scan.pass === 'discover') {
    return authoritativeResult(
      await commit(
        lease,
        record,
        intent,
        clock,
        {},
        {
          step: 'attachment-scan',
          scan: {
            purpose: state.scan.purpose,
            pass: 'verify',
            progress: initialWorkerAttachmentScan(target),
            discoverEvidence: evidence,
          },
        },
      ),
    );
  }
  const discoverEvidence = state.scan.discoverEvidence;
  if (!discoverEvidence) return malformedResult();
  if (
    evidence.evidenceSha256 !== discoverEvidence.evidenceSha256 ||
    evidence.evidenceCount !== discoverEvidence.evidenceCount
  ) {
    return authoritativeResult(await restartDiscover());
  }
  return authoritativeResult(
    await commit(
      lease,
      record,
      intent,
      clock,
      {},
      {
        step: 'database-deletion',
      },
    ),
  );
}

async function admitCleanup(
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
  lease: FleetStateLease,
  record: FleetRecord,
  authority: CleanupAuthority,
  randomUUID: () => string,
  clock: () => number,
): Promise<FleetRecord> {
  assertNoActiveDecommission(record, 'cleanupDeploymentArtifacts');
  assertBackendSwitchInactiveForCleanup(record);
  assertImmutableDeploymentMapping(record, backend, spec);
  const classification = classifyCleanupDatabaseEligibility({
    record,
    externalArtifact: backend.immutableExternalArtifacts === true,
  });
  if (!classification.eligible) {
    throw eligibilityRefusal(classification, record.phase);
  }
  const reservation =
    record.phase === 'database-reserved' ||
    record.phase === 'database-create-authorized';
  if (record.phase === 'database-reserved') {
    const reserved = await backend.findDatabase(spec);
    if (reserved) {
      throw new Error(
        `refusing to clear an unauthorized database reservation while '${reserved.id}:${reserved.name}' exists`,
      );
    }
  }
  if (!reservation) {
    requireStartCapabilities(backend, record.applicationResources ?? []);
  }
  const operationId = randomUUID();
  parseCleanupAdvanceToken({
    version: 1,
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    operationId,
    revision: 0,
  });
  const timestamp = nowIso(clock);
  const source: FleetRecord = {
    ...omitIntent(record),
    phase: 'cleanup-advancing',
    updatedAt: timestamp,
  };
  const intent: CleanupAdvanceIntent = {
    version: 1,
    operationId,
    revision: 0,
    generation: 0,
    updatedAt: timestamp,
    authority,
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
      admittedPhase: record.phase,
      externalArtifact: backend.immutableExternalArtifacts === true,
    },
    state: reservation
      ? { step: 'database-deletion' }
      : { step: 'teardown-traffic' },
  };
  const next: FleetRecord = {
    ...source,
    cleanupIntent: normalizeCleanupAdvanceIntent(intent, source),
  };
  await lease.put(next);
  return next;
}

/**
 * @internal Persists the provisioning-rollback authority under the caller's
 * held deployment lease; the failed-provision catch then drains through
 * `advanceCleanupUnderLease`. Refusals throw before any mutation and leave the
 * record untouched.
 */
export async function startProvisioningRollbackCleanup(
  lease: FleetStateLease,
  record: FleetRecord,
  authority: Extract<
    CleanupAuthority,
    { readonly kind: 'provisioning-rollback' }
  >,
  options: Readonly<{
    backend: ProvisioningBackend;
    spec: DeploymentSpec;
    randomUUID: () => string;
    clock?: () => number;
  }>,
): Promise<FleetRecord> {
  return admitCleanup(
    options.backend,
    options.spec,
    lease,
    record,
    authority,
    options.randomUUID,
    options.clock ?? Date.now,
  );
}

/**
 * @internal Advances one bounded cleanup group under an already-held
 * deployment lease. The public entry wraps this in its own
 * `withDeploymentLease`; already-lease-holding callers (the failed-provision
 * catch) call it directly.
 */
export async function advanceCleanupUnderLease(
  options: AdvanceCleanupDeploymentOptions,
  action: CleanupAdvanceAction,
  parsedToken: CleanupAdvanceToken | undefined,
  lease: FleetStateLease,
): Promise<CleanupAdvanceResult> {
  const { backend, spec, store } = options;
  const record = await store.get(spec.tenantTag, spec.environment);
  if (action.kind === 'start') {
    if (!record) throw new Error('deployment is not registered');
    const intent = record.cleanupIntent
      ? normalizeIntent(record.cleanupIntent, record)
      : undefined;
    if (intent) {
      assertCleanupCaller(record, backend, spec, intent);
      return authoritativeResult({ ...record, cleanupIntent: intent });
    }
    const admitted = await admitCleanup(
      backend,
      spec,
      lease,
      record,
      { kind: 'manual-cleanup' },
      options.randomUUID,
      options.clock ?? Date.now,
    );
    return authoritativeResult(admitted);
  }
  if (!parsedToken) throw new CleanupAdvanceTokenOperationError();
  const intent = record?.cleanupIntent
    ? normalizeIntent(record.cleanupIntent, record)
    : undefined;
  if (!record || !intent || intent.operationId !== parsedToken.operationId) {
    const readCleanupReceipt = requiredCapability(
      store.readCleanupReceipt,
      'receipt-read',
    ).bind(store);
    const receipt = await readCleanupReceipt(parsedToken.operationId);
    if (!receipt) throw new CleanupAdvanceTokenOperationError();
    if (
      receipt.tenantTag !== parsedToken.tenantTag ||
      receipt.environment !== parsedToken.environment
    ) {
      throw new CleanupAdvanceTokenDeploymentError();
    }
    return { status: 'complete', token: parsedToken, receipt };
  }
  const current: FleetRecord = { ...record, cleanupIntent: intent };
  const classification = classifyCleanupAdvanceToken(parsedToken, current);
  if (classification === 'stale') return authoritativeResult(current);
  if (action.kind === 'restart-blocked') {
    if (intent.state.step !== 'blocked') throw new CleanupAdvanceRestartError();
    assertCleanupCaller(current, backend, spec, intent);
    requireCapability(
      backend.advanceDecommissionAttachmentScan,
      'attachment-scan',
    );
    const restarted = await commit(
      lease,
      current,
      intent,
      options.clock ?? Date.now,
      {},
      {
        step: 'attachment-scan',
        scan: {
          purpose: intent.state.purpose,
          pass: 'discover',
          progress: initialWorkerAttachmentScan({
            kind: 'd1',
            databaseId: intent.state.purpose.databaseId,
          }),
        },
      },
      intent.generation + 1,
    );
    return authoritativeResult(restarted);
  }
  if (intent.state.step === 'blocked') return authoritativeResult(current);
  assertCleanupCaller(current, backend, spec, intent);
  const state = intent.state;
  if (
    state.step === 'teardown-traffic' ||
    state.step === 'teardown-worker' ||
    state.step === 'teardown-platform'
  ) {
    return authoritativeResult(
      await advanceTeardown(options, lease, current, intent),
    );
  }
  if (state.step === 'r2-deletion') {
    return authoritativeResult(
      await advanceR2Deletion(options, lease, current, intent, state),
    );
  }
  if (state.step === 'attachment-scan') {
    return advanceAttachmentScan(options, lease, current, intent, state);
  }
  return advanceDatabaseDeletion(options, lease, current, intent);
}

/**
 * Starts, reads, or advances one bounded no-export cleanup operation.
 *
 * One call performs at most one bounded scan chunk or one action group. The
 * terminal call persists the immutable operation-keyed receipt, releases the
 * deployment's ownership claims, and deletes the Fleet row in one atomic
 * batch.
 */
export async function advanceCleanupDeployment(
  options: AdvanceCleanupDeploymentOptions,
): Promise<CleanupAdvanceResult> {
  validateDeploymentSpec(options.spec);
  assertWorkerAttachmentProviderRequestBudget(options.maxProviderRequests);
  const action = cleanupActionFromUnknown(options.action);
  if (typeof options.randomUUID !== 'function') {
    throw new Error('advanceCleanupDeployment requires a randomUUID function');
  }
  const parsedToken =
    action.kind === 'start'
      ? undefined
      : parseCleanupAdvanceToken(action.token);
  if (
    parsedToken &&
    (parsedToken.tenantTag !== options.spec.tenantTag ||
      parsedToken.environment !== options.spec.environment)
  ) {
    throw new CleanupAdvanceTokenDeploymentError();
  }
  return options.store.withDeploymentLease(
    options.spec.tenantTag,
    options.spec.environment,
    (lease) => advanceCleanupUnderLease(options, action, parsedToken, lease),
  );
}
