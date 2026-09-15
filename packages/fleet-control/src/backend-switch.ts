// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID as nodeRandomUUID } from 'node:crypto';

import {
  advanceApplicationR2Deletion,
  applicationBindingTopology,
  assertApplicationR2EmptyBeforeDecommission,
  convergeApplicationR2Deletion,
} from './application-bindings.js';
import {
  invocationAuthorityCarrierFromUnknown,
  normalizeCleanupAdvanceIntent,
} from './cleanup-intent.js';
import {
  assertWorkerAttachmentProviderRequestBudget,
  initialWorkerAttachmentScan,
} from './cloudflare-worker-attachment-scan-state.js';
import {
  advanceDecommissionAttachmentScanStep,
  type DecommissionAdvanceAction,
  DecommissionAdvanceCapabilityError,
  DecommissionAdvanceRestartError,
  type DecommissionAdvanceResult,
  type DecommissionIntentTransition,
  decommissionAdvanceActionFromUnknown,
} from './decommission-advance.js';
import {
  databaseExportFromUnknown,
  databaseExportReceiptIdentity,
  reconcilePersistedDatabaseFromCallbacks,
  settleDatabaseDeletionUnderBarrier,
} from './decommission-database.js';
import {
  backendSwitchDecommissionLifecyclePhase,
  classifyDecommissionAdvanceToken,
  DecommissionAdvanceTokenDeploymentError,
  DecommissionAdvanceTokenOperationError,
  normalizeDecommissionAdvanceIntent,
  parseDecommissionAdvanceToken,
} from './decommission-intent.js';
import {
  isDeploymentEnvironment,
  isDeploymentScriptName,
  isDeploymentTenantTag,
  isSha256,
} from './deployment-context.js';
import type { HostRoutingTarget } from './host-routing.js';
import { d1MigrationHistoryDigest } from './migration-ledger.js';
import {
  assertExternalPlatformTargetCompatibility,
  assertPlatformResourcesMatchTarget,
  canonicalDeploymentEgressPolicy,
  canonicalMaintenanceCapabilityPublicKey,
  durableObjectMigrationHistoryDigest,
  type ExternalRouteExpectation,
  externalHostRoutingTarget,
  externalRouteExpectations,
} from './platform-resources.js';
import {
  applicationBindingTopologyFromUnknown,
  externalReleaseTopologyFromUnknown,
} from './release-topology.js';
import { deploymentSpecDigest } from './spec-digest.js';
import { cloneBoundedPlainData } from './strict-plain-data.js';
import type {
  BackendSwitchApplicationR2Progress,
  BackendSwitchCandidateSnapshot,
  BackendSwitchDecommissionRelease,
  BackendSwitchDecommissionRouteTarget,
  BackendSwitchDecommissionSnapshot,
  BackendSwitchIntent,
  BackendSwitchSubphase,
  BridgeMutationPlan,
  BridgeSnapshot,
  CleanupAdvanceIntent,
  DatabaseExportReceiptIdentity,
  DatabaseReference,
  DecommissionAdvanceIntent,
  DecommissionAdvanceToken,
  DecommissionAttachmentScanInput,
  DeploymentSecrets,
  DeploymentSpec,
  DurableObjectBindingInventory,
  ExternalMigrationIntent,
  ExternalMutationFence,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  InvocationAuthorityCarrier,
  PlainBackendSnapshot,
  ProvisioningPhase,
} from './types.js';
import {
  assertNoActiveCleanup,
  assertNoActiveDecommission,
  BACKEND_SWITCH_SUBPHASES,
  effectiveLifecyclePhase,
  PROVISIONING_PHASES,
} from './types.js';
import { validateDeploymentSpec } from './validation.js';

/** @internal Fixed refusal for malformed backend-switch Fleet authority. */
export const BACKEND_SWITCH_RECORD_ERROR =
  'backend switch decommission record is malformed';
const SWITCH_PLAIN_DATA_DEPTH_BOUND = 64;
const SWITCH_PLAIN_DATA_NODE_BOUND = 65_536;
const SWITCH_PLAIN_DATA_BYTE_BOUND = 4 * 1024 * 1024;
const STRUCTURED_CLONE = structuredClone;

export {
  BACKEND_SWITCH_SUBPHASES,
  type BackendSwitchApplicationR2Progress,
  type BackendSwitchCandidateSnapshot,
  type BackendSwitchDecommissionRelease,
  type BackendSwitchDecommissionRouteTarget,
  type BackendSwitchDecommissionSnapshot,
  type BackendSwitchIntent,
  type BackendSwitchSubphase,
  type BridgeMutationPlan,
  type BridgeSnapshot,
  type PlainBackendSnapshot,
} from './types.js';

/** @internal Immutable application-R2 identity in switch teardown authority. */
export type BackendSwitchApplicationR2Authority = Readonly<
  Pick<
    import('./types.js').ApplicationR2Resource,
    'name' | 'bucketName' | 'jurisdiction' | 'reservationNonce' | 'creationDate'
  >
>;

/** @internal Exact immutable projection covered by switch teardown authority. */
export interface BackendSwitchDecommissionAuthorityProjection {
  readonly version: 1;
  readonly prior: PlainBackendSnapshot;
  readonly restoredArtifactVersion: string | null;
  readonly entryPendingArtifactVersion: string | null;
  readonly entryPendingNamespaceIds: readonly string[] | null;
  readonly providerTargetSpecDigest: string;
  readonly routeHostname: string;
  readonly routeTargets: readonly BackendSwitchDecommissionRouteTarget[];
  readonly desiredSpecDigest: string;
  readonly target: ExternalPlatformTargetDescription;
  readonly releases: readonly ExternalReleaseSnapshot[];
  readonly applicationResources: readonly BackendSwitchApplicationR2Authority[];
  readonly bridge?: BridgeSnapshot;
  readonly resources?: ExternalPlatformResources;
  readonly bridgePlan?: BridgeMutationPlan;
}

/** @internal Exact provider observation for one pending ordinary Worker. */
export interface SwitchEntryPendingArtifactInspection {
  readonly artifactVersion: string;
  readonly specDigest: string;
  readonly databaseIds: readonly string[];
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly secretNames: readonly string[];
  readonly serviceBindings: readonly Readonly<{
    name: string;
    service: string;
    entrypoint?: string;
  }>[];
  readonly queueProducerBindings: readonly Readonly<{
    name: string;
    queueName: string;
  }>[];
  readonly application: import('./types.js').ApplicationBindingTopology;
}

/** @internal Reconstructs immutable switch teardown authority in fixed order. */
export function backendSwitchDecommissionAuthorityProjection(
  snapshot: BackendSwitchDecommissionSnapshot,
): BackendSwitchDecommissionAuthorityProjection {
  if (
    !snapshot.prior ||
    snapshot.restoredArtifactVersion === undefined ||
    snapshot.entryPendingArtifactVersion === undefined ||
    snapshot.entryPendingNamespaceIds === undefined ||
    !snapshot.providerTargetSpecDigest
  ) {
    throw new Error(
      'backend switch decommission snapshot lacks bounded authority',
    );
  }
  return {
    version: 1,
    prior: snapshot.prior,
    restoredArtifactVersion: snapshot.restoredArtifactVersion,
    entryPendingArtifactVersion: snapshot.entryPendingArtifactVersion,
    entryPendingNamespaceIds: snapshot.entryPendingNamespaceIds,
    providerTargetSpecDigest: snapshot.providerTargetSpecDigest,
    routeHostname: snapshot.routeHostname,
    routeTargets: snapshot.routeTargets,
    desiredSpecDigest: snapshot.desiredSpecDigest,
    target: snapshot.target,
    releases: snapshot.releases.map(({ release }) => baseRelease(release)),
    applicationResources: snapshot.applicationResources.map(
      ({ name, bucketName, jurisdiction, reservationNonce, creationDate }) => ({
        name,
        bucketName,
        jurisdiction,
        reservationNonce,
        ...(creationDate ? { creationDate } : {}),
      }),
    ),
    ...(snapshot.bridge ? { bridge: snapshot.bridge } : {}),
    ...(snapshot.resources ? { resources: snapshot.resources } : {}),
    ...(snapshot.bridgePlan ? { bridgePlan: snapshot.bridgePlan } : {}),
  };
}

/** @internal Lowercase SHA-256 over the exact immutable teardown projection. */
export function backendSwitchDecommissionSnapshotDigest(
  snapshot: BackendSwitchDecommissionSnapshot,
): string {
  return createHash('sha256')
    .update(
      JSON.stringify(backendSwitchDecommissionAuthorityProjection(snapshot)),
    )
    .digest('hex');
}

function requireExactKeys(
  value: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
  message: string,
): void {
  const keys = Object.keys(value);
  const allowed = new Set([...required, ...optional]);
  if (
    required.some((key) => !Object.hasOwn(value, key)) ||
    keys.some((key) => !allowed.has(key))
  ) {
    throw new Error(message);
  }
}

function stringArray(value: unknown, label: string): readonly string[] {
  if (
    !Array.isArray(value) ||
    value.some((item) => typeof item !== 'string' || item.length === 0) ||
    new Set(value).size !== value.length
  ) {
    throw new Error(`backend switch state has invalid ${label}`);
  }
  return value as string[];
}

function durableBindings(
  value: unknown,
  label: string,
): readonly DurableObjectBindingInventory[] {
  if (
    !Array.isArray(value) ||
    value.some(
      (item) =>
        !item ||
        typeof item !== 'object' ||
        (() => {
          try {
            requireExactKeys(
              item as Record<string, unknown>,
              ['name', 'className', 'namespaceId'],
              ['scriptName', 'dispatchNamespace'],
              `backend switch state has invalid ${label}`,
            );
            return false;
          } catch {
            return true;
          }
        })() ||
        typeof (item as Record<string, unknown>).name !== 'string' ||
        typeof (item as Record<string, unknown>).className !== 'string' ||
        typeof (item as Record<string, unknown>).namespaceId !== 'string' ||
        ((item as Record<string, unknown>).scriptName !== undefined &&
          typeof (item as Record<string, unknown>).scriptName !== 'string') ||
        ((item as Record<string, unknown>).dispatchNamespace !== undefined &&
          typeof (item as Record<string, unknown>).dispatchNamespace !==
            'string'),
    )
  ) {
    throw new Error(`backend switch state has invalid ${label}`);
  }
  return value as DurableObjectBindingInventory[];
}

function applicationTopology(
  value: unknown,
  label: string,
): import('./types.js').ApplicationBindingTopology | undefined {
  if (value === undefined) return undefined;
  try {
    return applicationBindingTopologyFromUnknown(value, label);
  } catch {
    throw new Error(`backend switch state has invalid ${label}`);
  }
}

function applicationResources(
  value: unknown,
): readonly import('./types.js').ApplicationR2Resource[] {
  if (!Array.isArray(value)) {
    throw new Error('backend switch state has invalid application resources');
  }
  for (const entry of value) {
    if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
      requireExactKeys(
        entry as Record<string, unknown>,
        ['name', 'bucketName', 'jurisdiction', 'state', 'reservationNonce'],
        ['creationDate'],
        'backend switch state has invalid application resources',
      );
    }
  }
  if (
    value.some(
      (entry) =>
        !entry ||
        typeof entry !== 'object' ||
        typeof (entry as Record<string, unknown>).name !== 'string' ||
        typeof (entry as Record<string, unknown>).bucketName !== 'string' ||
        !['default', 'eu', 'fedramp'].includes(
          String((entry as Record<string, unknown>).jurisdiction),
        ) ||
        ![
          'reserved',
          'create-authorized',
          'created',
          'detach-authorized',
          'detached',
          'empty-authorized',
          'empty',
          'delete-authorized',
          'deleted',
        ].includes(String((entry as Record<string, unknown>).state)) ||
        typeof (entry as Record<string, unknown>).reservationNonce !==
          'string' ||
        String((entry as Record<string, unknown>).reservationNonce).length ===
          0 ||
        ((entry as Record<string, unknown>).creationDate !== undefined &&
          typeof (entry as Record<string, unknown>).creationDate !== 'string'),
    )
  ) {
    throw new Error('backend switch state has invalid application resources');
  }
  for (const field of ['name', 'bucketName', 'reservationNonce'] as const) {
    const values = value.map((entry) =>
      String((entry as Record<string, unknown>)[field]),
    );
    if (new Set(values).size !== values.length) {
      throw new Error(
        `backend switch state has duplicate application resource ${field}`,
      );
    }
  }
  return value as readonly import('./types.js').ApplicationR2Resource[];
}

function applicationR2Progress(
  value: unknown,
): readonly BackendSwitchApplicationR2Progress[] {
  if (!Array.isArray(value)) {
    throw new Error('backend switch state has invalid application R2 progress');
  }
  for (const entry of value) {
    if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
      requireExactKeys(
        entry as Record<string, unknown>,
        ['resource', 'subphase'],
        [],
        'backend switch state has invalid application R2 progress',
      );
    }
  }
  const allowed = new Set([
    'reserved',
    'create-authorized',
    'created',
    'detach-authorized',
    'detached',
    'empty-authorized',
    'empty',
    'delete-authorized',
    'deleted',
  ]);
  const resources = applicationResources(
    value.map((entry) =>
      entry && typeof entry === 'object'
        ? (entry as Record<string, unknown>).resource
        : undefined,
    ),
  );
  if (
    value.some(
      (entry) =>
        !entry ||
        typeof entry !== 'object' ||
        !allowed.has(String((entry as Record<string, unknown>).subphase)),
    )
  ) {
    throw new Error('backend switch state has invalid application R2 progress');
  }
  return value.map((entry, index) => ({
    resource: resources[index] as import('./types.js').ApplicationR2Resource,
    subphase: (entry as Record<string, unknown>)
      .subphase as BackendSwitchApplicationR2Progress['subphase'],
  }));
}

function applicationResourceIdentity(
  resources: readonly import('./types.js').ApplicationR2Resource[],
): readonly Readonly<{
  name: string;
  bucketName: string;
  jurisdiction: import('./types.js').ApplicationR2Resource['jurisdiction'];
  reservationNonce: string;
  creationDate?: string;
}>[] {
  return resources.map(
    ({ name, bucketName, jurisdiction, reservationNonce, creationDate }) => ({
      name,
      bucketName,
      jurisdiction,
      reservationNonce,
      ...(creationDate ? { creationDate } : {}),
    }),
  );
}

function plainSnapshot(value: unknown): PlainBackendSnapshot {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid prior snapshot');
  }
  const prior = value as Record<string, unknown>;
  requireExactKeys(
    prior,
    [
      'scriptName',
      'artifactVersion',
      'specDigest',
      'databaseId',
      'databaseName',
      'durableObjectBindings',
      'namespaceIds',
      'secretNames',
      'applicationResources',
      'customDomain',
    ],
    ['application'],
    'backend switch state has invalid prior snapshot',
  );
  const domain = prior.customDomain;
  if (domain && typeof domain === 'object' && !Array.isArray(domain)) {
    requireExactKeys(
      domain as Record<string, unknown>,
      ['id', 'hostname'],
      [],
      'backend switch state has invalid prior snapshot',
    );
  }
  if (
    typeof prior.scriptName !== 'string' ||
    !isDeploymentScriptName(prior.scriptName) ||
    typeof prior.artifactVersion !== 'string' ||
    prior.artifactVersion.length === 0 ||
    typeof prior.specDigest !== 'string' ||
    !isSha256(prior.specDigest) ||
    typeof prior.databaseId !== 'string' ||
    prior.databaseId.length === 0 ||
    typeof prior.databaseName !== 'string' ||
    prior.databaseName.length === 0 ||
    !domain ||
    typeof domain !== 'object' ||
    Array.isArray(domain) ||
    typeof (domain as Record<string, unknown>).id !== 'string' ||
    typeof (domain as Record<string, unknown>).hostname !== 'string'
  ) {
    throw new Error('backend switch state has invalid prior snapshot');
  }
  const parsedApplication = applicationTopology(
    prior.application,
    'prior application topology',
  );
  const parsedResources = applicationResources(
    prior.applicationResources ?? [],
  );
  if (
    parsedResources.some(
      (resource) =>
        resource.state !== 'created' ||
        !resource.creationDate ||
        !Number.isFinite(Date.parse(resource.creationDate)),
    ) ||
    JSON.stringify(
      parsedResources.map(({ name, bucketName, jurisdiction }) => ({
        name,
        bucketName,
        jurisdiction,
      })),
    ) !==
      JSON.stringify(
        (parsedApplication?.r2Buckets ?? []).map(
          ({ name, bucketName, jurisdiction }) => ({
            name,
            bucketName,
            jurisdiction,
          }),
        ),
      )
  ) {
    throw new Error(
      'backend switch prior application R2 identity is inconsistent',
    );
  }
  return {
    scriptName: prior.scriptName,
    artifactVersion: prior.artifactVersion,
    specDigest: prior.specDigest,
    databaseId: prior.databaseId,
    databaseName: prior.databaseName,
    durableObjectBindings: durableBindings(
      prior.durableObjectBindings,
      'prior Durable Object bindings',
    ),
    namespaceIds: stringArray(prior.namespaceIds, 'prior namespaces'),
    secretNames: stringArray(prior.secretNames, 'prior secrets'),
    applicationResources: parsedResources,
    ...(parsedApplication ? { application: parsedApplication } : {}),
    customDomain: {
      id: String((domain as Record<string, unknown>).id),
      hostname: String((domain as Record<string, unknown>).hostname),
    },
  };
}

function bridgeSnapshot(value: unknown): BridgeSnapshot {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid bridge snapshot');
  }
  const bridge = value as Record<string, unknown>;
  requireExactKeys(
    bridge,
    [
      'scriptName',
      'artifactVersion',
      'artifactDigest',
      'databaseId',
      'durableObjectBindings',
      'namespaceIds',
      'secretNames',
      'publicRouteAttached',
      'stateOnly',
    ],
    ['application'],
    'backend switch state has invalid bridge snapshot',
  );
  if (
    typeof bridge.scriptName !== 'string' ||
    !isDeploymentScriptName(bridge.scriptName) ||
    typeof bridge.artifactVersion !== 'string' ||
    bridge.artifactVersion.length === 0 ||
    typeof bridge.artifactDigest !== 'string' ||
    !isSha256(bridge.artifactDigest) ||
    typeof bridge.databaseId !== 'string' ||
    bridge.databaseId.length === 0 ||
    typeof bridge.publicRouteAttached !== 'boolean' ||
    typeof bridge.stateOnly !== 'boolean'
  ) {
    throw new Error('backend switch state has invalid bridge snapshot');
  }
  const parsedApplication = applicationTopology(
    bridge.application,
    'bridge application topology',
  );
  return {
    scriptName: bridge.scriptName,
    artifactVersion: bridge.artifactVersion,
    artifactDigest: bridge.artifactDigest,
    databaseId: bridge.databaseId,
    durableObjectBindings: durableBindings(
      bridge.durableObjectBindings,
      'bridge Durable Object bindings',
    ),
    namespaceIds: stringArray(bridge.namespaceIds, 'bridge namespaces'),
    secretNames: stringArray(bridge.secretNames, 'bridge secrets'),
    ...(parsedApplication ? { application: parsedApplication } : {}),
    publicRouteAttached: bridge.publicRouteAttached,
    stateOnly: bridge.stateOnly,
  };
}

function bridgeMutationPlan(value: unknown): BridgeMutationPlan {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid bridge plan');
  }
  const plan = value as Record<string, unknown>;
  requireExactKeys(
    plan,
    [
      'artifactDigest',
      'durableObjectMigrations',
      'secretNames',
      'mutationDigest',
    ],
    ['priorDurableObjectTag', 'targetDurableObjectTag'],
    'backend switch state has invalid bridge plan',
  );
  if (
    typeof plan.artifactDigest !== 'string' ||
    !isSha256(plan.artifactDigest) ||
    typeof plan.mutationDigest !== 'string' ||
    !isSha256(plan.mutationDigest) ||
    (plan.priorDurableObjectTag !== undefined &&
      (typeof plan.priorDurableObjectTag !== 'string' ||
        plan.priorDurableObjectTag.length === 0)) ||
    (plan.targetDurableObjectTag !== undefined &&
      (typeof plan.targetDurableObjectTag !== 'string' ||
        plan.targetDurableObjectTag.length === 0)) ||
    !Array.isArray(plan.durableObjectMigrations) ||
    plan.durableObjectMigrations.some(
      (migration) =>
        !migration ||
        typeof migration !== 'object' ||
        typeof (migration as Record<string, unknown>).tag !== 'string',
    )
  ) {
    throw new Error('backend switch state has invalid bridge plan');
  }
  return {
    artifactDigest: plan.artifactDigest,
    durableObjectMigrations:
      plan.durableObjectMigrations as unknown as readonly import('./types.js').DurableObjectMigration[],
    ...(plan.priorDurableObjectTag !== undefined
      ? { priorDurableObjectTag: String(plan.priorDurableObjectTag) }
      : {}),
    ...(plan.targetDurableObjectTag !== undefined
      ? { targetDurableObjectTag: String(plan.targetDurableObjectTag) }
      : {}),
    secretNames: stringArray(plan.secretNames, 'bridge plan secrets'),
    mutationDigest: plan.mutationDigest,
  };
}

function releaseSnapshot(value: unknown): BackendSwitchCandidateSnapshot {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid candidate snapshot');
  }
  const release = value as Record<string, unknown>;
  requireExactKeys(
    release,
    [
      'physicalScriptName',
      'specDigest',
      'artifactVersion',
      'releaseSchemaVersion',
      'application',
      'topology',
      'maintenance',
    ],
    [],
    'backend switch state has invalid candidate snapshot',
  );
  const topology = release.topology;
  const maintenance = release.maintenance;
  if (
    maintenance &&
    typeof maintenance === 'object' &&
    !Array.isArray(maintenance)
  ) {
    requireExactKeys(
      maintenance as Record<string, unknown>,
      ['receipt', 'specDigest'],
      [],
      'backend switch state has invalid candidate snapshot',
    );
  }
  if (
    typeof release.physicalScriptName !== 'string' ||
    !isDeploymentScriptName(release.physicalScriptName) ||
    typeof release.specDigest !== 'string' ||
    !isSha256(release.specDigest) ||
    typeof release.artifactVersion !== 'string' ||
    release.artifactVersion.length === 0 ||
    typeof release.releaseSchemaVersion !== 'number' ||
    !Number.isSafeInteger(release.releaseSchemaVersion) ||
    !topology ||
    typeof topology !== 'object' ||
    Array.isArray(topology) ||
    !maintenance ||
    typeof maintenance !== 'object' ||
    Array.isArray(maintenance) ||
    typeof (maintenance as Record<string, unknown>).receipt !== 'string' ||
    String((maintenance as Record<string, unknown>).receipt).length === 0 ||
    typeof (maintenance as Record<string, unknown>).specDigest !== 'string' ||
    !isSha256(String((maintenance as Record<string, unknown>).specDigest))
  ) {
    throw new Error('backend switch state has invalid candidate snapshot');
  }
  let decodedTopology: NonNullable<ExternalReleaseSnapshot['topology']>;
  try {
    decodedTopology = externalReleaseTopologyFromUnknown(
      topology,
      'backend switch candidate topology',
    );
  } catch {
    throw new Error('backend switch state has invalid candidate topology');
  }
  const decodedApplication = applicationBindingTopologyFromUnknown(
    release.application,
    'backend switch candidate application',
  );
  if (
    !decodedTopology.application ||
    JSON.stringify(decodedTopology.application) !==
      JSON.stringify(decodedApplication)
  ) {
    throw new Error(
      'backend switch candidate application topology is inconsistent',
    );
  }
  return {
    physicalScriptName: release.physicalScriptName,
    specDigest: release.specDigest,
    artifactVersion: release.artifactVersion,
    releaseSchemaVersion: release.releaseSchemaVersion,
    application: decodedApplication,
    topology: decodedTopology,
    maintenance: {
      receipt: String((maintenance as Record<string, unknown>).receipt),
      specDigest: String((maintenance as Record<string, unknown>).specDigest),
    },
  };
}

function decommissionReleaseSnapshot(
  value: unknown,
  label: string,
): ExternalReleaseSnapshot {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`backend switch state has invalid ${label}`);
  }
  const release = value as Record<string, unknown>;
  requireExactKeys(
    release,
    [
      'physicalScriptName',
      'specDigest',
      'artifactVersion',
      'releaseSchemaVersion',
      'application',
    ],
    ['topology'],
    `backend switch state has invalid ${label}`,
  );
  if (
    typeof release.physicalScriptName !== 'string' ||
    !isDeploymentScriptName(release.physicalScriptName) ||
    typeof release.specDigest !== 'string' ||
    !isSha256(release.specDigest) ||
    typeof release.artifactVersion !== 'string' ||
    release.artifactVersion.length === 0 ||
    typeof release.releaseSchemaVersion !== 'number' ||
    !Number.isSafeInteger(release.releaseSchemaVersion) ||
    release.releaseSchemaVersion < 0
  ) {
    throw new Error(`backend switch state has invalid ${label}`);
  }
  const application = applicationBindingTopologyFromUnknown(
    release.application,
    `${label}.application`,
  );
  const topology =
    release.topology === undefined
      ? undefined
      : externalReleaseTopologyFromUnknown(
          release.topology,
          `${label}.topology`,
        );
  if (
    (release.artifactVersion !== 'pending' && !topology) ||
    (topology?.application &&
      JSON.stringify(topology.application) !== JSON.stringify(application))
  ) {
    throw new Error(`backend switch state has invalid ${label}`);
  }
  return {
    physicalScriptName: release.physicalScriptName,
    specDigest: release.specDigest,
    artifactVersion: release.artifactVersion,
    releaseSchemaVersion: release.releaseSchemaVersion,
    application,
    ...(topology ? { topology } : {}),
  };
}

function backendSwitchPlatformTarget(
  value: unknown,
  tenantTag: string,
  environment: string,
): ExternalPlatformTargetDescription {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid target');
  }
  const target = value as Record<string, unknown>;
  requireExactKeys(
    target,
    [
      'maintenanceCapabilityPublicKey',
      'stateArtifactDigest',
      'stateDurableObjectHistoryDigest',
      'd1SchemaVersion',
      'd1SchemaHistoryDigest',
      'outboundPolicy',
    ],
    [
      'auditQueueName',
      'stateDurableObjectTag',
      'stateEgressCredentialDigest',
      'egressArtifactDigest',
      'sharedOutboundWorkerName',
    ],
    'backend switch state has invalid target',
  );
  const outbound = target.outboundPolicy as Record<string, unknown> | undefined;
  if (
    typeof target.stateArtifactDigest !== 'string' ||
    !isSha256(target.stateArtifactDigest) ||
    typeof target.stateDurableObjectHistoryDigest !== 'string' ||
    !isSha256(target.stateDurableObjectHistoryDigest) ||
    typeof target.d1SchemaHistoryDigest !== 'string' ||
    !isSha256(target.d1SchemaHistoryDigest) ||
    typeof target.d1SchemaVersion !== 'number' ||
    !Number.isSafeInteger(target.d1SchemaVersion) ||
    target.d1SchemaVersion < 0 ||
    typeof target.maintenanceCapabilityPublicKey !== 'string' ||
    canonicalMaintenanceCapabilityPublicKey(
      target.maintenanceCapabilityPublicKey,
    ) !== target.maintenanceCapabilityPublicKey ||
    (target.stateDurableObjectTag !== undefined &&
      typeof target.stateDurableObjectTag !== 'string') ||
    (target.auditQueueName !== undefined &&
      (typeof target.auditQueueName !== 'string' ||
        target.auditQueueName.length === 0)) ||
    (target.stateEgressCredentialDigest !== undefined &&
      (typeof target.stateEgressCredentialDigest !== 'string' ||
        !isSha256(target.stateEgressCredentialDigest))) ||
    (target.sharedOutboundWorkerName !== undefined &&
      (typeof target.sharedOutboundWorkerName !== 'string' ||
        !isDeploymentScriptName(target.sharedOutboundWorkerName))) ||
    (target.egressArtifactDigest !== undefined &&
      (typeof target.egressArtifactDigest !== 'string' ||
        !isSha256(target.egressArtifactDigest))) ||
    (target.sharedOutboundWorkerName === undefined) ===
      (target.egressArtifactDigest === undefined) ||
    (target.sharedOutboundWorkerName !== undefined &&
      typeof target.stateEgressCredentialDigest !== 'string') ||
    !outbound ||
    typeof outbound.policyId !== 'string' ||
    !Array.isArray(outbound.policyHosts) ||
    outbound.policyHosts.some((host) => typeof host !== 'string') ||
    typeof outbound.policyDigest !== 'string'
  ) {
    throw new Error('backend switch state has invalid target');
  }
  const canonicalPolicy = canonicalDeploymentEgressPolicy({
    policyId: outbound.policyId,
    tenantTag,
    environment,
    allowedHosts: outbound.policyHosts as string[],
  });
  if (
    canonicalPolicy.policyDigest !== outbound.policyDigest ||
    JSON.stringify(canonicalPolicy.policyHosts) !==
      JSON.stringify(outbound.policyHosts)
  ) {
    throw new Error('backend switch state has invalid target policy');
  }
  return {
    ...(typeof target.auditQueueName === 'string'
      ? { auditQueueName: target.auditQueueName }
      : {}),
    maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
    stateArtifactDigest: target.stateArtifactDigest,
    stateDurableObjectHistoryDigest: target.stateDurableObjectHistoryDigest,
    ...(typeof target.stateDurableObjectTag === 'string'
      ? { stateDurableObjectTag: target.stateDurableObjectTag }
      : {}),
    ...(typeof target.stateEgressCredentialDigest === 'string'
      ? { stateEgressCredentialDigest: target.stateEgressCredentialDigest }
      : {}),
    ...(typeof target.sharedOutboundWorkerName === 'string'
      ? { sharedOutboundWorkerName: target.sharedOutboundWorkerName }
      : {}),
    ...(typeof target.egressArtifactDigest === 'string'
      ? { egressArtifactDigest: target.egressArtifactDigest }
      : {}),
    d1SchemaVersion: target.d1SchemaVersion,
    d1SchemaHistoryDigest: target.d1SchemaHistoryDigest,
    outboundPolicy: canonicalPolicy,
  };
}

function decommissionSnapshot(
  value: unknown,
  target: ExternalPlatformTargetDescription,
  tenantTag: string,
  environment: string,
  prior: PlainBackendSnapshot,
  restoredArtifactVersion: string | undefined,
  providerTargetSpecDigest: string,
): BackendSwitchDecommissionSnapshot {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid decommission snapshot');
  }
  const snapshot = value as Record<string, unknown>;
  requireExactKeys(
    snapshot,
    [
      'routeHostname',
      'routeTargets',
      'desiredSpecDigest',
      'target',
      'releases',
      'applicationResources',
    ],
    [
      'prior',
      'restoredArtifactVersion',
      'entryPendingArtifactVersion',
      'entryPendingNamespaceIds',
      'providerTargetSpecDigest',
      'bridge',
      'resources',
      'bridgePlan',
    ],
    'backend switch state has invalid decommission snapshot',
  );
  const compatibilityKeys = [
    'prior',
    'restoredArtifactVersion',
    'entryPendingArtifactVersion',
    'entryPendingNamespaceIds',
    'providerTargetSpecDigest',
  ] as const;
  const presentCompatibilityKeys = compatibilityKeys.filter((key) =>
    Object.hasOwn(snapshot, key),
  );
  if (
    presentCompatibilityKeys.length !== 0 &&
    presentCompatibilityKeys.length !== compatibilityKeys.length
  ) {
    throw new Error('backend switch state has invalid decommission snapshot');
  }
  const hasCompatibilityGroup =
    presentCompatibilityKeys.length === compatibilityKeys.length;
  const parsedCompatibilityPrior = hasCompatibilityGroup
    ? plainSnapshot(snapshot.prior)
    : undefined;
  const parsedRestoredArtifactVersion = hasCompatibilityGroup
    ? snapshot.restoredArtifactVersion === null
      ? null
      : typeof snapshot.restoredArtifactVersion === 'string' &&
          snapshot.restoredArtifactVersion.length > 0
        ? snapshot.restoredArtifactVersion
        : (() => {
            throw new Error(
              'backend switch state has invalid decommission snapshot',
            );
          })()
    : undefined;
  const parsedEntryPendingArtifactVersion = hasCompatibilityGroup
    ? snapshot.entryPendingArtifactVersion === null
      ? null
      : typeof snapshot.entryPendingArtifactVersion === 'string' &&
          snapshot.entryPendingArtifactVersion.length > 0
        ? snapshot.entryPendingArtifactVersion
        : (() => {
            throw new Error(
              'backend switch state has invalid decommission snapshot',
            );
          })()
    : undefined;
  const parsedEntryPendingNamespaceIds = hasCompatibilityGroup
    ? snapshot.entryPendingNamespaceIds === null
      ? null
      : stringArray(
          snapshot.entryPendingNamespaceIds,
          'entry pending namespaces',
        )
    : undefined;
  if (
    hasCompatibilityGroup &&
    (JSON.stringify(parsedCompatibilityPrior) !== JSON.stringify(prior) ||
      parsedRestoredArtifactVersion !== (restoredArtifactVersion ?? null) ||
      (parsedEntryPendingArtifactVersion === null) !==
        (parsedEntryPendingNamespaceIds === null) ||
      typeof snapshot.providerTargetSpecDigest !== 'string' ||
      !isSha256(snapshot.providerTargetSpecDigest) ||
      snapshot.providerTargetSpecDigest !== providerTargetSpecDigest ||
      (parsedEntryPendingNamespaceIds !== null &&
        JSON.stringify(parsedEntryPendingNamespaceIds) !==
          JSON.stringify(
            [...(parsedEntryPendingNamespaceIds ?? [])].sort((left, right) =>
              left < right ? -1 : left > right ? 1 : 0,
            ),
          )))
  ) {
    throw new Error('backend switch state has invalid decommission snapshot');
  }
  if (
    typeof snapshot.routeHostname !== 'string' ||
    snapshot.routeHostname.length === 0 ||
    snapshot.routeHostname !== snapshot.routeHostname.toLowerCase() ||
    typeof snapshot.desiredSpecDigest !== 'string' ||
    !isSha256(snapshot.desiredSpecDigest) ||
    !Array.isArray(snapshot.releases)
  ) {
    throw new Error('backend switch state has invalid decommission snapshot');
  }
  const parsedSnapshotTarget = backendSwitchPlatformTarget(
    snapshot.target,
    tenantTag,
    environment,
  );
  if (JSON.stringify(parsedSnapshotTarget) !== JSON.stringify(target)) {
    throw new Error('backend switch decommission target is not canonical');
  }
  const names = new Set<string>();
  const releases: BackendSwitchDecommissionRelease[] = snapshot.releases.map(
    (entry, index) => {
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
        throw new Error(
          'backend switch state has invalid decommission release progress',
        );
      }
      const progress = entry as Record<string, unknown>;
      requireExactKeys(
        progress,
        ['release', 'subphase'],
        [],
        'backend switch state has invalid decommission release progress',
      );
      if (
        progress.subphase !== 'present' &&
        progress.subphase !== 'delete-authorized' &&
        progress.subphase !== 'deleted'
      ) {
        throw new Error(
          'backend switch state has invalid decommission release progress',
        );
      }
      const release = decommissionReleaseSnapshot(
        progress.release,
        `decommission release ${index}`,
      );
      if (names.has(release.physicalScriptName)) {
        throw new Error('backend switch decommission releases are duplicated');
      }
      names.add(release.physicalScriptName);
      return {
        release,
        subphase:
          progress.subphase as BackendSwitchDecommissionRelease['subphase'],
      };
    },
  );
  if (
    JSON.stringify(
      releases.map(({ release }) => release.physicalScriptName),
    ) !==
    JSON.stringify(
      releases.map(({ release }) => release.physicalScriptName).sort(),
    )
  ) {
    throw new Error('backend switch decommission releases are not canonical');
  }
  const bridge =
    snapshot.bridge === undefined ? undefined : bridgeSnapshot(snapshot.bridge);
  const bridgePlan =
    snapshot.bridgePlan === undefined
      ? undefined
      : bridgeMutationPlan(snapshot.bridgePlan);
  const parsedApplicationResources = applicationResources(
    snapshot.applicationResources,
  );
  let resources: ExternalPlatformResources | undefined;
  if (snapshot.resources !== undefined) {
    if (
      !snapshot.resources ||
      typeof snapshot.resources !== 'object' ||
      Array.isArray(snapshot.resources)
    ) {
      throw new Error(
        'backend switch state has invalid decommission resources',
      );
    }
    const candidate = snapshot.resources as Record<string, unknown>;
    const state = candidate.stateWorker;
    if (!state || typeof state !== 'object' || Array.isArray(state)) {
      throw new Error(
        'backend switch state has invalid decommission resources',
      );
    }
    const stateWorker = state as Record<string, unknown>;
    if (
      typeof stateWorker.scriptName !== 'string' ||
      !isDeploymentScriptName(stateWorker.scriptName) ||
      typeof stateWorker.artifactVersion !== 'string' ||
      stateWorker.artifactVersion.length === 0 ||
      typeof stateWorker.artifactDigest !== 'string' ||
      !isSha256(stateWorker.artifactDigest) ||
      stateWorker.plane !== 'ordinary' ||
      stateWorker.dispatchNamespace !== undefined ||
      (stateWorker.durableObjectTag !== undefined &&
        typeof stateWorker.durableObjectTag !== 'string')
    ) {
      throw new Error(
        'backend switch state has invalid decommission resources',
      );
    }
    const bindings = durableBindings(
      stateWorker.durableObjectBindings,
      'decommission state bindings',
    );
    const namespaceIds = stringArray(
      stateWorker.namespaceIds,
      'decommission state namespaces',
    );
    resources = {
      ...(target.auditQueueName
        ? { auditQueueName: target.auditQueueName }
        : {}),
      maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
      stateWorker: {
        scriptName: stateWorker.scriptName,
        artifactVersion: stateWorker.artifactVersion,
        artifactDigest: stateWorker.artifactDigest,
        plane: 'ordinary',
        ...(typeof stateWorker.durableObjectTag === 'string'
          ? { durableObjectTag: stateWorker.durableObjectTag }
          : {}),
        durableObjectBindings: bindings,
        namespaceIds,
      },
      outboundPolicy: target.outboundPolicy,
      ...(target.sharedOutboundWorkerName
        ? { sharedOutboundWorkerName: target.sharedOutboundWorkerName }
        : {}),
    };
    try {
      assertPlatformResourcesMatchTarget(resources, {
        ...target,
        stateArtifactDigest: resources.stateWorker.artifactDigest,
      });
    } catch {
      throw new Error(
        'backend switch state has invalid decommission resources',
      );
    }
    if (
      JSON.stringify(resources) !== JSON.stringify(snapshot.resources) ||
      (bridge !== undefined &&
        (bridge.scriptName !== resources.stateWorker.scriptName ||
          bridge.artifactVersion !== resources.stateWorker.artifactVersion ||
          bridge.artifactDigest !== resources.stateWorker.artifactDigest ||
          JSON.stringify(bridge.durableObjectBindings) !==
            JSON.stringify(resources.stateWorker.durableObjectBindings) ||
          JSON.stringify(bridge.namespaceIds) !==
            JSON.stringify(resources.stateWorker.namespaceIds)))
    ) {
      throw new Error(
        'backend switch state has invalid decommission resources',
      );
    }
  }
  if (!Array.isArray(snapshot.routeTargets)) {
    throw new Error('backend switch state has invalid decommission routes');
  }
  const stateScriptName =
    resources?.stateWorker.scriptName ?? bridge?.scriptName;
  const serializedTargets = new Set<string>();
  const routeTargets = snapshot.routeTargets.map((value, index) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      throw new Error('backend switch state has invalid decommission route');
    }
    const entry = value as Record<string, unknown>;
    requireExactKeys(
      entry,
      ['release', 'target', 'routeTarget'],
      [],
      'backend switch state has invalid decommission route',
    );
    const release = decommissionReleaseSnapshot(
      entry.release,
      `decommission route release ${index}`,
    );
    const persistedRelease = releases.find(
      ({ release: candidate }) =>
        candidate.physicalScriptName === release.physicalScriptName,
    )?.release;
    const routePlatformTarget = backendSwitchPlatformTarget(
      entry.target,
      tenantTag,
      environment,
    );
    try {
      assertExternalPlatformTargetCompatibility(routePlatformTarget, target);
    } catch {
      throw new Error('backend switch state has invalid decommission route');
    }
    if (
      !persistedRelease ||
      JSON.stringify(persistedRelease) !== JSON.stringify(release)
    ) {
      throw new Error('backend switch state has invalid decommission route');
    }
    const routeTarget = externalHostRoutingTarget(
      { tenantTag, environment },
      { release, target: routePlatformTarget },
      stateScriptName,
    );
    if (JSON.stringify(entry.routeTarget) !== JSON.stringify(routeTarget)) {
      throw new Error('backend switch state has invalid decommission route');
    }
    const serialized = JSON.stringify(routeTarget);
    if (serializedTargets.has(serialized)) {
      throw new Error('backend switch decommission routes are duplicated');
    }
    serializedTargets.add(serialized);
    return { release, target: routePlatformTarget, routeTarget };
  });
  if (
    JSON.stringify(
      routeTargets.map(({ routeTarget }) => JSON.stringify(routeTarget)),
    ) !==
    JSON.stringify(
      routeTargets.map(({ routeTarget }) => JSON.stringify(routeTarget)).sort(),
    )
  ) {
    throw new Error('backend switch decommission routes are not canonical');
  }
  return {
    ...(hasCompatibilityGroup
      ? {
          prior: parsedCompatibilityPrior as PlainBackendSnapshot,
          restoredArtifactVersion: parsedRestoredArtifactVersion as
            | string
            | null,
          entryPendingArtifactVersion: parsedEntryPendingArtifactVersion as
            | string
            | null,
          entryPendingNamespaceIds: parsedEntryPendingNamespaceIds as
            | readonly string[]
            | null,
          providerTargetSpecDigest: snapshot.providerTargetSpecDigest as string,
        }
      : {}),
    routeHostname: snapshot.routeHostname,
    routeTargets,
    desiredSpecDigest: snapshot.desiredSpecDigest,
    target,
    releases,
    applicationResources: parsedApplicationResources,
    ...(bridge ? { bridge } : {}),
    ...(resources ? { resources } : {}),
    ...(bridgePlan ? { bridgePlan } : {}),
  };
}

function backendSwitchIntentFromPlain(value: unknown): BackendSwitchIntent {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid intent');
  }
  const intent = value as Record<string, unknown>;
  const allowedKeys = new Set([
    'kind',
    'tenantTag',
    'environment',
    'prior',
    'targetSpecDigest',
    'targetApplication',
    'target',
    'rollbackUntil',
    'subphase',
    'bridgePlan',
    'bridge',
    'candidate',
    'restoredArtifactVersion',
    'databaseExport',
    'applicationR2Progress',
    'stateReconcileIntent',
    'decommissionSnapshot',
    'decommissionSnapshotSha256',
    'decommissionEntrySubphase',
  ]);
  if (
    Reflect.ownKeys(intent).some(
      (key) =>
        typeof key !== 'string' ||
        !allowedKeys.has(key) ||
        intent[key] === undefined,
    )
  ) {
    throw new Error('backend switch state has invalid intent');
  }
  if (
    intent.kind !== 'backend-switch' ||
    typeof intent.tenantTag !== 'string' ||
    !isDeploymentTenantTag(intent.tenantTag) ||
    typeof intent.environment !== 'string' ||
    !isDeploymentEnvironment(intent.environment) ||
    typeof intent.targetSpecDigest !== 'string' ||
    !isSha256(intent.targetSpecDigest) ||
    typeof intent.rollbackUntil !== 'string' ||
    new Date(intent.rollbackUntil).toISOString() !== intent.rollbackUntil ||
    typeof intent.subphase !== 'string' ||
    !BACKEND_SWITCH_SUBPHASES.includes(intent.subphase as BackendSwitchSubphase)
  ) {
    throw new Error('backend switch state has invalid intent');
  }
  const target = backendSwitchPlatformTarget(
    intent.target,
    intent.tenantTag,
    intent.environment,
  );
  const parsedPrior = plainSnapshot(intent.prior);
  const providerTargetSpecDigest =
    intent.stateReconcileIntent &&
    typeof intent.stateReconcileIntent === 'object' &&
    !Array.isArray(intent.stateReconcileIntent) &&
    typeof (intent.stateReconcileIntent as Record<string, unknown>)
      .targetSpecDigest === 'string'
      ? String(
          (intent.stateReconcileIntent as Record<string, unknown>)
            .targetSpecDigest,
        )
      : intent.targetSpecDigest;
  const parsedDecommissionSnapshot =
    intent.decommissionSnapshot === undefined
      ? undefined
      : decommissionSnapshot(
          intent.decommissionSnapshot,
          target,
          intent.tenantTag,
          intent.environment,
          parsedPrior,
          typeof intent.restoredArtifactVersion === 'string'
            ? intent.restoredArtifactVersion
            : undefined,
          providerTargetSpecDigest,
        );
  const targetApplication = applicationBindingTopologyFromUnknown(
    intent.targetApplication,
    'backend switch target application',
  );
  const parsedApplicationR2Progress =
    intent.applicationR2Progress !== undefined
      ? applicationR2Progress(intent.applicationR2Progress)
      : undefined;
  const hasSnapshotDigest = intent.decommissionSnapshotSha256 !== undefined;
  const hasEntrySubphase = intent.decommissionEntrySubphase !== undefined;
  if (
    hasSnapshotDigest !== hasEntrySubphase ||
    (hasSnapshotDigest &&
      (!parsedDecommissionSnapshot ||
        typeof intent.decommissionSnapshotSha256 !== 'string' ||
        !isSha256(intent.decommissionSnapshotSha256) ||
        intent.decommissionSnapshotSha256 !==
          backendSwitchDecommissionSnapshotDigest(
            parsedDecommissionSnapshot as BackendSwitchDecommissionSnapshot,
          ) ||
        typeof intent.decommissionEntrySubphase !== 'string' ||
        !BACKEND_SWITCH_SUBPHASES.includes(
          intent.decommissionEntrySubphase as BackendSwitchSubphase,
        )))
  ) {
    throw new Error('backend switch state has invalid decommission authority');
  }
  if (
    parsedApplicationR2Progress &&
    JSON.stringify(
      applicationResourceIdentity(
        parsedApplicationR2Progress.map(({ resource }) => resource),
      ),
    ) !==
      JSON.stringify(
        applicationResourceIdentity(parsedPrior.applicationResources),
      ) &&
    JSON.stringify(
      applicationResourceIdentity(
        parsedApplicationR2Progress.map(({ resource }) => resource),
      ),
    ) !==
      JSON.stringify(
        applicationResourceIdentity(
          parsedDecommissionSnapshot?.applicationResources ?? [],
        ),
      )
  ) {
    throw new Error(
      'backend switch application R2 progress changed resource identity',
    );
  }
  return {
    kind: 'backend-switch',
    tenantTag: intent.tenantTag,
    environment: intent.environment,
    prior: parsedPrior,
    targetSpecDigest: intent.targetSpecDigest,
    targetApplication,
    target,
    rollbackUntil: intent.rollbackUntil,
    subphase: intent.subphase as BackendSwitchSubphase,
    ...(intent.bridgePlan !== undefined
      ? { bridgePlan: bridgeMutationPlan(intent.bridgePlan) }
      : {}),
    ...(intent.bridge !== undefined
      ? { bridge: bridgeSnapshot(intent.bridge) }
      : {}),
    ...(intent.candidate !== undefined
      ? { candidate: releaseSnapshot(intent.candidate) }
      : {}),
    ...(intent.restoredArtifactVersion !== undefined
      ? typeof intent.restoredArtifactVersion === 'string' &&
        intent.restoredArtifactVersion.length > 0
        ? { restoredArtifactVersion: intent.restoredArtifactVersion }
        : (() => {
            throw new Error(
              'backend switch state has invalid restored artifact',
            );
          })()
      : {}),
    ...(intent.databaseExport !== undefined
      ? { databaseExport: databaseExport(intent.databaseExport) }
      : {}),
    ...(parsedApplicationR2Progress !== undefined
      ? { applicationR2Progress: parsedApplicationR2Progress }
      : {}),
    ...(intent.stateReconcileIntent !== undefined
      ? (() => {
          if (
            !intent.stateReconcileIntent ||
            typeof intent.stateReconcileIntent !== 'object' ||
            Array.isArray(intent.stateReconcileIntent)
          ) {
            throw new Error(
              'backend switch state has invalid state reconciliation intent',
            );
          }
          const reconciliation = intent.stateReconcileIntent as Record<
            string,
            unknown
          >;
          requireExactKeys(
            reconciliation,
            ['targetSpecDigest', 'plan', 'subphase'],
            [],
            'backend switch state has invalid state reconciliation intent',
          );
          if (
            typeof reconciliation.targetSpecDigest !== 'string' ||
            !isSha256(reconciliation.targetSpecDigest) ||
            (reconciliation.subphase !== 'upload-authorized' &&
              reconciliation.subphase !== 'uploaded')
          ) {
            throw new Error(
              'backend switch state has invalid state reconciliation intent',
            );
          }
          return {
            stateReconcileIntent: {
              targetSpecDigest: reconciliation.targetSpecDigest,
              plan: bridgeMutationPlan(reconciliation.plan),
              subphase: reconciliation.subphase,
            },
          };
        })()
      : {}),
    ...(parsedDecommissionSnapshot !== undefined
      ? { decommissionSnapshot: parsedDecommissionSnapshot }
      : {}),
    ...(hasSnapshotDigest
      ? {
          decommissionSnapshotSha256:
            intent.decommissionSnapshotSha256 as string,
          decommissionEntrySubphase:
            intent.decommissionEntrySubphase as BackendSwitchSubphase,
        }
      : {}),
  };
}

export function backendSwitchIntentFromUnknown(
  value: unknown,
): BackendSwitchIntent {
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: SWITCH_PLAIN_DATA_DEPTH_BOUND,
      maxNodes: SWITCH_PLAIN_DATA_NODE_BOUND,
      maxScalarBytes: SWITCH_PLAIN_DATA_BYTE_BOUND,
      maxSerializedBytes: SWITCH_PLAIN_DATA_BYTE_BOUND,
      error: () => new Error('backend switch state has invalid intent'),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    throw new Error('backend switch state has invalid intent');
  }
  return backendSwitchIntentFromPlain(plain);
}

function databaseExport(value: unknown): import('./types.js').DatabaseExport {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid database export');
  }
  const candidate = value as Record<string, unknown>;
  requireExactKeys(
    candidate,
    ['databaseId', 'location', 'sha256', 'size'],
    [],
    'backend switch state has invalid database export',
  );
  if (
    typeof candidate.databaseId !== 'string' ||
    candidate.databaseId.length === 0 ||
    typeof candidate.location !== 'string' ||
    candidate.location.length === 0 ||
    typeof candidate.sha256 !== 'string' ||
    !isSha256(candidate.sha256) ||
    typeof candidate.size !== 'number' ||
    !Number.isSafeInteger(candidate.size) ||
    candidate.size < 1
  ) {
    throw new Error('backend switch state has invalid database export');
  }
  return candidate as unknown as import('./types.js').DatabaseExport;
}

function fleetOutboundPolicyFromUnknown(
  value: unknown,
  tenantTag: string,
  environment: string,
): import('./types.js').DeploymentEgressPolicy {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const policy = value as Record<string, unknown>;
  requireExactKeys(
    policy,
    ['policyId', 'policyHosts', 'policyDigest'],
    [],
    BACKEND_SWITCH_RECORD_ERROR,
  );
  if (
    typeof policy.policyId !== 'string' ||
    !policy.policyId ||
    !Array.isArray(policy.policyHosts) ||
    policy.policyHosts.some((host) => typeof host !== 'string') ||
    typeof policy.policyDigest !== 'string'
  ) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const canonical = canonicalDeploymentEgressPolicy({
    policyId: policy.policyId,
    tenantTag,
    environment,
    allowedHosts: policy.policyHosts as string[],
  });
  if (!sameCanonicalData(canonical, policy)) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  return canonical;
}

function fleetPlatformResourcesFromUnknown(
  value: unknown,
  tenantTag: string,
  environment: string,
  target: ExternalPlatformTargetDescription,
): ExternalPlatformResources {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const source = value as Record<string, unknown>;
  requireExactKeys(
    source,
    ['maintenanceCapabilityPublicKey', 'stateWorker'],
    [
      'auditQueueName',
      'outboundPolicy',
      'sharedOutboundWorkerName',
      'egressProxy',
    ],
    BACKEND_SWITCH_RECORD_ERROR,
  );
  const state = source.stateWorker;
  if (!state || typeof state !== 'object' || Array.isArray(state)) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const stateWorker = state as Record<string, unknown>;
  requireExactKeys(
    stateWorker,
    [
      'scriptName',
      'artifactVersion',
      'artifactDigest',
      'durableObjectBindings',
      'namespaceIds',
    ],
    ['plane', 'dispatchNamespace', 'durableObjectTag'],
    BACKEND_SWITCH_RECORD_ERROR,
  );
  if (
    typeof source.maintenanceCapabilityPublicKey !== 'string' ||
    canonicalMaintenanceCapabilityPublicKey(
      source.maintenanceCapabilityPublicKey,
    ) !== source.maintenanceCapabilityPublicKey ||
    typeof stateWorker.scriptName !== 'string' ||
    !isDeploymentScriptName(stateWorker.scriptName) ||
    typeof stateWorker.artifactVersion !== 'string' ||
    !stateWorker.artifactVersion ||
    typeof stateWorker.artifactDigest !== 'string' ||
    !isSha256(stateWorker.artifactDigest) ||
    (stateWorker.plane !== undefined &&
      stateWorker.plane !== 'ordinary' &&
      stateWorker.plane !== 'dispatch') ||
    (stateWorker.dispatchNamespace !== undefined &&
      (typeof stateWorker.dispatchNamespace !== 'string' ||
        !stateWorker.dispatchNamespace)) ||
    (stateWorker.plane === 'dispatch' &&
      typeof stateWorker.dispatchNamespace !== 'string') ||
    (stateWorker.durableObjectTag !== undefined &&
      (typeof stateWorker.durableObjectTag !== 'string' ||
        !stateWorker.durableObjectTag)) ||
    (source.auditQueueName !== undefined &&
      (typeof source.auditQueueName !== 'string' || !source.auditQueueName)) ||
    (source.sharedOutboundWorkerName !== undefined &&
      (typeof source.sharedOutboundWorkerName !== 'string' ||
        !isDeploymentScriptName(source.sharedOutboundWorkerName)))
  ) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const outboundPolicy =
    source.outboundPolicy === undefined
      ? undefined
      : fleetOutboundPolicyFromUnknown(
          source.outboundPolicy,
          tenantTag,
          environment,
        );
  let egressProxy: ExternalPlatformResources['egressProxy'];
  if (source.egressProxy !== undefined) {
    if (
      !source.egressProxy ||
      typeof source.egressProxy !== 'object' ||
      Array.isArray(source.egressProxy)
    ) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    const proxy = source.egressProxy as Record<string, unknown>;
    requireExactKeys(
      proxy,
      [
        'scriptName',
        'artifactVersion',
        'artifactDigest',
        'policyId',
        'policyHosts',
        'policyDigest',
      ],
      [],
      BACKEND_SWITCH_RECORD_ERROR,
    );
    const policy = fleetOutboundPolicyFromUnknown(
      {
        policyId: proxy.policyId,
        policyHosts: proxy.policyHosts,
        policyDigest: proxy.policyDigest,
      },
      tenantTag,
      environment,
    );
    if (
      typeof proxy.scriptName !== 'string' ||
      !isDeploymentScriptName(proxy.scriptName) ||
      typeof proxy.artifactVersion !== 'string' ||
      !proxy.artifactVersion ||
      typeof proxy.artifactDigest !== 'string' ||
      !isSha256(proxy.artifactDigest)
    ) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    egressProxy = {
      scriptName: proxy.scriptName,
      artifactVersion: proxy.artifactVersion,
      artifactDigest: proxy.artifactDigest,
      ...policy,
    };
  }
  const resources: ExternalPlatformResources = {
    ...(typeof source.auditQueueName === 'string'
      ? { auditQueueName: source.auditQueueName }
      : {}),
    maintenanceCapabilityPublicKey: source.maintenanceCapabilityPublicKey,
    stateWorker: {
      scriptName: stateWorker.scriptName,
      artifactVersion: stateWorker.artifactVersion,
      artifactDigest: stateWorker.artifactDigest,
      ...(stateWorker.plane === 'ordinary' || stateWorker.plane === 'dispatch'
        ? { plane: stateWorker.plane }
        : {}),
      ...(typeof stateWorker.dispatchNamespace === 'string'
        ? { dispatchNamespace: stateWorker.dispatchNamespace }
        : {}),
      ...(typeof stateWorker.durableObjectTag === 'string'
        ? { durableObjectTag: stateWorker.durableObjectTag }
        : {}),
      durableObjectBindings: durableBindings(
        stateWorker.durableObjectBindings,
        'platform state bindings',
      ),
      namespaceIds: stringArray(
        stateWorker.namespaceIds,
        'platform state namespaces',
      ),
    },
    ...(outboundPolicy ? { outboundPolicy } : {}),
    ...(typeof source.sharedOutboundWorkerName === 'string'
      ? { sharedOutboundWorkerName: source.sharedOutboundWorkerName }
      : {}),
    ...(egressProxy ? { egressProxy } : {}),
  };
  assertPlatformResourcesMatchTarget(resources, {
    ...target,
    stateArtifactDigest: resources.stateWorker.artifactDigest,
  });
  return resources;
}

function fleetMigrationIntentFromUnknown(
  value: unknown,
  tenantTag: string,
  environment: string,
): ExternalMigrationIntent {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const source = value as Record<string, unknown>;
  requireExactKeys(
    source,
    [
      'targetSpecDigest',
      'priorRelease',
      'priorTarget',
      'priorOutboundPolicy',
      'targetRelease',
      'target',
      'subphase',
    ],
    ['platformOnly'],
    BACKEND_SWITCH_RECORD_ERROR,
  );
  if (
    typeof source.targetSpecDigest !== 'string' ||
    !isSha256(source.targetSpecDigest) ||
    (source.platformOnly !== undefined && source.platformOnly !== true) ||
    ![
      'planned',
      'schema-applied',
      'platform-applied',
      'candidate-deployed',
      'candidate-armed',
      'route-published',
    ].includes(String(source.subphase))
  ) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  return {
    ...(source.platformOnly === true ? { platformOnly: true as const } : {}),
    targetSpecDigest: source.targetSpecDigest,
    priorRelease: decommissionReleaseSnapshot(
      source.priorRelease,
      'migration prior release',
    ),
    priorTarget: backendSwitchPlatformTarget(
      source.priorTarget,
      tenantTag,
      environment,
    ),
    priorOutboundPolicy: fleetOutboundPolicyFromUnknown(
      source.priorOutboundPolicy,
      tenantTag,
      environment,
    ),
    targetRelease: decommissionReleaseSnapshot(
      source.targetRelease,
      'migration target release',
    ),
    target: backendSwitchPlatformTarget(source.target, tenantTag, environment),
    subphase: source.subphase as ExternalMigrationIntent['subphase'],
  };
}

function fleetMigrationHistoryFromUnknown(
  value: unknown,
): readonly import('./types.js').DurableObjectMigration[] {
  if (!Array.isArray(value)) throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  for (const migration of value) {
    if (
      !migration ||
      typeof migration !== 'object' ||
      Array.isArray(migration)
    ) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    const item = migration as Record<string, unknown>;
    requireExactKeys(
      item,
      ['tag'],
      ['newSqliteClasses', 'newClasses', 'deletedClasses', 'renamedClasses'],
      BACKEND_SWITCH_RECORD_ERROR,
    );
    if (
      typeof item.tag !== 'string' ||
      !item.tag ||
      ['newSqliteClasses', 'newClasses', 'deletedClasses'].some(
        (key) =>
          item[key] !== undefined &&
          (!Array.isArray(item[key]) ||
            (item[key] as unknown[]).some(
              (entry) => typeof entry !== 'string',
            )),
      ) ||
      (item.renamedClasses !== undefined &&
        (!Array.isArray(item.renamedClasses) ||
          item.renamedClasses.some(
            (entry) =>
              !entry ||
              typeof entry !== 'object' ||
              Array.isArray(entry) ||
              typeof (entry as Record<string, unknown>).from !== 'string' ||
              typeof (entry as Record<string, unknown>).to !== 'string',
          )))
    ) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
  }
  return value as readonly import('./types.js').DurableObjectMigration[];
}

const BACKEND_SWITCH_RECORD_KEYS = [
  'tenantTag',
  'backend',
  'environment',
  'scriptName',
  'databaseId',
  'databaseName',
  'schemaVersion',
  'artifactVersion',
  'desiredSpecDigest',
  'pendingSpecDigest',
  'pendingArtifactVersion',
  'activeRelease',
  'pendingRelease',
  'migrationPriorRelease',
  'rollbackRelease',
  'retiringRelease',
  'outboundPolicy',
  'platformResources',
  'platformTarget',
  'migrationIntent',
  'backendSwitchIntent',
  'decommissionIntent',
  'cleanupIntent',
  'invocationAuthority',
  'applicationResources',
  'applicationBindings',
  'durableObjectTag',
  'durableObjectMigrationHistory',
  'durableObjectMigrationHistoryDigest',
  'durableObjectBindings',
  'routeHostname',
  'phase',
  'databaseExportLocation',
  'databaseExportSha256',
  'databaseExportSize',
  'settledSettlementKey',
  'updatedAt',
] as const;

const BACKEND_SWITCH_REQUIRED_RECORD_KEYS = new Set([
  'tenantTag',
  'backend',
  'environment',
  'scriptName',
  'databaseId',
  'databaseName',
  'schemaVersion',
  'artifactVersion',
  'desiredSpecDigest',
  'durableObjectBindings',
  'routeHostname',
  'phase',
  'updatedAt',
  'backendSwitchIntent',
]);

function canonicalJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalJsonValue);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(
    Object.keys(value as Record<string, unknown>)
      .sort((left, right) => (left < right ? -1 : left > right ? 1 : 0))
      .map((key) => [
        key,
        canonicalJsonValue((value as Record<string, unknown>)[key]),
      ]),
  );
}

function sameCanonicalData(left: unknown, right: unknown): boolean {
  return (
    JSON.stringify(canonicalJsonValue(left)) ===
    JSON.stringify(canonicalJsonValue(right))
  );
}

export interface CanonicalBackendSwitchFleetRecord {
  readonly record: FleetRecord;
  readonly comparisonBytes: string;
}

/** @internal Descriptor-safe structural Fleet record and switch-authority classification. */
export interface StructuralBackendSwitchFleetRecord {
  readonly record: FleetRecord;
  readonly carriesBackendSwitchAuthority: boolean;
}

/** @internal Single structural ingress policy for backend-switch-aware consumers. */
export function structuralBackendSwitchFleetRecordFromUnknown(
  value: unknown,
): StructuralBackendSwitchFleetRecord {
  try {
    const plain = cloneBoundedPlainData(value, {
      maxDepth: SWITCH_PLAIN_DATA_DEPTH_BOUND,
      maxNodes: SWITCH_PLAIN_DATA_NODE_BOUND,
      maxScalarBytes: SWITCH_PLAIN_DATA_BYTE_BOUND,
      maxSerializedBytes: SWITCH_PLAIN_DATA_BYTE_BOUND,
      error: () => new Error(BACKEND_SWITCH_RECORD_ERROR),
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
    if (!plain || typeof plain !== 'object' || Array.isArray(plain)) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    const source = plain as Record<string, unknown>;
    const shell = source.decommissionIntent;
    const identity =
      shell && typeof shell === 'object' && !Array.isArray(shell)
        ? (shell as Record<string, unknown>).identity
        : undefined;
    const mode =
      identity && typeof identity === 'object' && !Array.isArray(identity)
        ? (identity as Record<string, unknown>).mode
        : undefined;
    return {
      record: source as unknown as FleetRecord,
      carriesBackendSwitchAuthority:
        Object.hasOwn(source, 'backendSwitchIntent') ||
        Boolean(
          mode &&
            typeof mode === 'object' &&
            !Array.isArray(mode) &&
            (mode as Record<string, unknown>).kind === 'backend-switch',
        ),
    };
  } catch {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
}

function canonicalFleetRecordFromSource(
  source: Record<string, unknown>,
  switchIntent: BackendSwitchIntent,
  shell: DecommissionAdvanceIntent | undefined,
  cleanupIntent: CleanupAdvanceIntent | undefined,
  invocationAuthority: InvocationAuthorityCarrier | undefined,
): FleetRecord {
  // Cross-intent sources must not survive canonicalization even before
  // validateRecordCrossFields runs: an active cleanup owns the whole record,
  // and no switch-bearing record — settled or not — can legitimately carry a
  // cleanup intent (cleanup admission is restricted to prepublication phases,
  // which no switch record occupies).
  if (cleanupIntent) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  if (
    typeof source.tenantTag !== 'string' ||
    !isDeploymentTenantTag(source.tenantTag) ||
    typeof source.environment !== 'string' ||
    !isDeploymentEnvironment(source.environment) ||
    (source.backend !== 'plain-worker' &&
      source.backend !== 'workers-for-platforms') ||
    typeof source.scriptName !== 'string' ||
    !isDeploymentScriptName(source.scriptName) ||
    typeof source.databaseId !== 'string' ||
    !source.databaseId ||
    typeof source.databaseName !== 'string' ||
    !source.databaseName ||
    !Number.isSafeInteger(source.schemaVersion) ||
    Number(source.schemaVersion) < 0 ||
    typeof source.artifactVersion !== 'string' ||
    !source.artifactVersion ||
    typeof source.desiredSpecDigest !== 'string' ||
    !isSha256(source.desiredSpecDigest) ||
    typeof source.routeHostname !== 'string' ||
    !source.routeHostname ||
    source.routeHostname !== source.routeHostname.toLowerCase() ||
    typeof source.phase !== 'string' ||
    !PROVISIONING_PHASES.includes(source.phase as ProvisioningPhase) ||
    typeof source.updatedAt !== 'string' ||
    !Number.isFinite(Date.parse(source.updatedAt)) ||
    new Date(source.updatedAt).toISOString() !== source.updatedAt ||
    switchIntent.tenantTag !== source.tenantTag ||
    switchIntent.environment !== source.environment ||
    switchIntent.prior.scriptName !== source.scriptName ||
    switchIntent.prior.databaseId !== source.databaseId ||
    switchIntent.prior.databaseName !== source.databaseName ||
    source.routeHostname !==
      switchIntent.prior.customDomain.hostname.toLowerCase() ||
    (switchIntent.decommissionSnapshot !== undefined &&
      switchIntent.decommissionSnapshot.routeHostname !== source.routeHostname)
  ) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const tenantTag = source.tenantTag;
  const environment = source.environment;
  const applicationResources = Object.hasOwn(source, 'applicationResources')
    ? applicationResourcesFromRecord(source.applicationResources)
    : undefined;
  const applicationBindings = Object.hasOwn(source, 'applicationBindings')
    ? applicationBindingTopologyFromUnknown(
        source.applicationBindings,
        'Fleet application bindings',
      )
    : undefined;
  if (
    applicationBindings &&
    !sameCanonicalData(
      applicationBindings.r2Buckets,
      (applicationResources ?? []).map(
        ({ name, bucketName, jurisdiction }) => ({
          name,
          bucketName,
          jurisdiction,
        }),
      ),
    )
  ) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const release = (key: string) =>
    Object.hasOwn(source, key)
      ? decommissionReleaseSnapshot(source[key], `Fleet ${key}`)
      : undefined;
  const activeRelease = release('activeRelease');
  const pendingRelease = release('pendingRelease');
  const migrationPriorRelease = release('migrationPriorRelease');
  const rollbackRelease = release('rollbackRelease');
  const retiringRelease = release('retiringRelease');
  const outboundPolicy = Object.hasOwn(source, 'outboundPolicy')
    ? fleetOutboundPolicyFromUnknown(
        source.outboundPolicy,
        tenantTag,
        environment,
      )
    : undefined;
  const platformTarget = Object.hasOwn(source, 'platformTarget')
    ? reorderCanonicalObjectLikeSource(
        backendSwitchPlatformTarget(
          source.platformTarget,
          tenantTag,
          environment,
        ),
        source.platformTarget,
      )
    : undefined;
  const platformResources = Object.hasOwn(source, 'platformResources')
    ? platformTarget
      ? fleetPlatformResourcesFromUnknown(
          source.platformResources,
          tenantTag,
          environment,
          platformTarget,
        )
      : (() => {
          throw new Error(BACKEND_SWITCH_RECORD_ERROR);
        })()
    : undefined;
  const migrationIntent = Object.hasOwn(source, 'migrationIntent')
    ? fleetMigrationIntentFromUnknown(
        source.migrationIntent,
        tenantTag,
        environment,
      )
    : undefined;
  const durableObjectBindings = durableBindings(
    source.durableObjectBindings,
    'Fleet Durable Object bindings',
  );
  const migrationHistory = Object.hasOwn(
    source,
    'durableObjectMigrationHistory',
  )
    ? fleetMigrationHistoryFromUnknown(source.durableObjectMigrationHistory)
    : undefined;
  const migrationHistoryDigest = Object.hasOwn(
    source,
    'durableObjectMigrationHistoryDigest',
  )
    ? source.durableObjectMigrationHistoryDigest
    : undefined;
  if (
    (migrationHistory === undefined) !==
      (migrationHistoryDigest === undefined) ||
    (migrationHistory &&
      (typeof migrationHistoryDigest !== 'string' ||
        durableObjectMigrationHistoryDigest(migrationHistory) !==
          migrationHistoryDigest)) ||
    (source.durableObjectTag !== undefined &&
      (typeof source.durableObjectTag !== 'string' ||
        !source.durableObjectTag)) ||
    (source.pendingSpecDigest !== undefined &&
      (typeof source.pendingSpecDigest !== 'string' ||
        !isSha256(source.pendingSpecDigest))) ||
    (source.pendingArtifactVersion !== undefined &&
      (typeof source.pendingArtifactVersion !== 'string' ||
        !source.pendingArtifactVersion ||
        source.pendingArtifactVersion === 'pending')) ||
    (source.settledSettlementKey !== undefined &&
      (typeof source.settledSettlementKey !== 'string' ||
        !source.settledSettlementKey))
  ) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const exportFields = [
    source.databaseExportLocation,
    source.databaseExportSha256,
    source.databaseExportSize,
  ];
  if (
    exportFields.some((value) => value !== undefined) &&
    (typeof source.databaseExportLocation !== 'string' ||
      !source.databaseExportLocation ||
      typeof source.databaseExportSha256 !== 'string' ||
      !isSha256(source.databaseExportSha256) ||
      !Number.isSafeInteger(source.databaseExportSize) ||
      Number(source.databaseExportSize) < 1)
  ) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  const provisional: FleetRecord = {
    tenantTag,
    backend: source.backend,
    environment,
    scriptName: source.scriptName,
    databaseId: source.databaseId,
    databaseName: source.databaseName,
    schemaVersion: source.schemaVersion as number,
    artifactVersion: source.artifactVersion,
    desiredSpecDigest: source.desiredSpecDigest,
    ...(typeof source.pendingSpecDigest === 'string'
      ? { pendingSpecDigest: source.pendingSpecDigest }
      : {}),
    ...(typeof source.pendingArtifactVersion === 'string'
      ? { pendingArtifactVersion: source.pendingArtifactVersion }
      : {}),
    ...(activeRelease ? { activeRelease } : {}),
    ...(pendingRelease ? { pendingRelease } : {}),
    ...(migrationPriorRelease ? { migrationPriorRelease } : {}),
    ...(rollbackRelease ? { rollbackRelease } : {}),
    ...(retiringRelease ? { retiringRelease } : {}),
    ...(outboundPolicy ? { outboundPolicy } : {}),
    ...(platformResources ? { platformResources } : {}),
    ...(platformTarget ? { platformTarget } : {}),
    ...(migrationIntent ? { migrationIntent } : {}),
    backendSwitchIntent: switchIntent,
    ...(shell ? { decommissionIntent: shell } : {}),
    // cleanupIntent is unconditionally rejected above and never emitted here.
    ...(invocationAuthority ? { invocationAuthority } : {}),
    ...(applicationResources ? { applicationResources } : {}),
    ...(applicationBindings ? { applicationBindings } : {}),
    ...(typeof source.durableObjectTag === 'string'
      ? { durableObjectTag: source.durableObjectTag }
      : {}),
    ...(migrationHistory
      ? { durableObjectMigrationHistory: migrationHistory }
      : {}),
    ...(typeof migrationHistoryDigest === 'string'
      ? { durableObjectMigrationHistoryDigest: migrationHistoryDigest }
      : {}),
    durableObjectBindings,
    routeHostname: source.routeHostname,
    phase: source.phase as ProvisioningPhase,
    ...(typeof source.databaseExportLocation === 'string'
      ? { databaseExportLocation: source.databaseExportLocation }
      : {}),
    ...(typeof source.databaseExportSha256 === 'string'
      ? { databaseExportSha256: source.databaseExportSha256 }
      : {}),
    ...(typeof source.databaseExportSize === 'number'
      ? { databaseExportSize: source.databaseExportSize }
      : {}),
    ...(typeof source.settledSettlementKey === 'string'
      ? { settledSettlementKey: source.settledSettlementKey }
      : {}),
    updatedAt: source.updatedAt,
  };
  const phase = effectiveLifecyclePhase(provisional);
  const migrationInvalid = Boolean(
    migrationIntent &&
      (phase !== 'migrating' ||
        migrationIntent.targetSpecDigest !==
          migrationIntent.targetRelease.specDigest ||
        migrationIntent.target.d1SchemaVersion !==
          (migrationIntent.platformOnly === true
            ? provisional.schemaVersion
            : migrationIntent.targetRelease.releaseSchemaVersion) ||
        !sameCanonicalData(migrationIntent.priorRelease, activeRelease) ||
        (migrationIntent.platformOnly === true
          ? !sameCanonicalData(migrationIntent.targetRelease, activeRelease) ||
            migrationPriorRelease !== undefined ||
            pendingRelease !== undefined ||
            ['candidate-deployed', 'candidate-armed'].includes(
              migrationIntent.subphase,
            )
          : !sameCanonicalData(
              migrationIntent.priorRelease,
              migrationPriorRelease,
            ) ||
            !sameCanonicalData(migrationIntent.targetRelease, pendingRelease))),
  );
  const migrationTargetInvalid = Boolean(
    migrationIntent &&
      [
        'platform-applied',
        'candidate-deployed',
        'candidate-armed',
        'route-published',
      ].includes(migrationIntent.subphase) &&
      !sameCanonicalData(platformTarget, migrationIntent.target),
  );
  const resourcePolicy =
    platformResources?.outboundPolicy ?? platformResources?.egressProxy;
  const resourcePolicyInvalid = Boolean(
    platformResources &&
      (resourcePolicy?.policyId !== outboundPolicy?.policyId ||
        !sameCanonicalData(
          resourcePolicy?.policyHosts,
          outboundPolicy?.policyHosts,
        ) ||
        resourcePolicy?.policyDigest !== outboundPolicy?.policyDigest),
  );
  if (
    (source.backend === 'workers-for-platforms') !== Boolean(outboundPolicy) ||
    resourcePolicyInvalid ||
    (platformTarget &&
      !sameCanonicalData(platformTarget.outboundPolicy, outboundPolicy)) ||
    (source.backend === 'plain-worker' &&
      (platformTarget !== undefined || migrationIntent !== undefined)) ||
    (source.backend === 'workers-for-platforms' &&
      platformResources !== undefined &&
      platformTarget === undefined) ||
    (source.backend === 'workers-for-platforms' &&
      phase === 'migrating' &&
      migrationIntent === undefined) ||
    (source.pendingArtifactVersion !== undefined &&
      (source.backend !== 'plain-worker' ||
        phase !== 'migrating' ||
        typeof source.pendingSpecDigest !== 'string')) ||
    migrationInvalid ||
    migrationTargetInvalid
  ) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  if (shell) {
    if (
      (shell.state === 'complete'
        ? provisional.phase !== 'decommissioned'
        : provisional.phase !== 'decommission-advancing') ||
      source.pendingSpecDigest !== undefined ||
      source.pendingArtifactVersion !== undefined ||
      pendingRelease !== undefined ||
      migrationPriorRelease !== undefined ||
      rollbackRelease !== undefined ||
      retiringRelease !== undefined ||
      migrationIntent !== undefined
    ) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    const switchExport = switchIntent.databaseExport;
    if (
      (switchExport === undefined) !==
        (source.databaseExportLocation === undefined) ||
      (switchExport &&
        (switchExport.databaseId !== provisional.databaseId ||
          switchExport.location !== source.databaseExportLocation ||
          switchExport.sha256 !== source.databaseExportSha256 ||
          switchExport.size !== source.databaseExportSize))
    ) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
  }
  return provisional;
}

function reorderCanonicalObjectLikeSource<Value extends object>(
  canonical: Value,
  source: unknown,
): Value {
  if (!source || typeof source !== 'object' || Array.isArray(source)) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  return Object.fromEntries(
    Object.keys(source).map((key) => [
      key,
      (canonical as Record<string, unknown>)[key],
    ]),
  ) as Value;
}

function applicationResourcesFromRecord(
  value: unknown,
): readonly import('./types.js').ApplicationR2Resource[] {
  const resources = applicationResources(value);
  const sorted = [...resources].sort((left, right) =>
    left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
  );
  if (!sameCanonicalData(resources, sorted)) {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
  return resources;
}

/** @internal Strict canonical boundary for switch and decommission authority. */
export function backendSwitchFleetRecordFromUnknown(
  value: unknown,
): CanonicalBackendSwitchFleetRecord {
  try {
    const source = structuralBackendSwitchFleetRecordFromUnknown(value)
      .record as unknown as Record<string, unknown>;
    const actualKeys = Reflect.ownKeys(source);
    if (
      actualKeys.some(
        (key) =>
          typeof key !== 'string' ||
          !BACKEND_SWITCH_RECORD_KEYS.includes(
            key as (typeof BACKEND_SWITCH_RECORD_KEYS)[number],
          ) ||
          source[key] === undefined,
      ) ||
      [...BACKEND_SWITCH_REQUIRED_RECORD_KEYS].some(
        (key) => !Object.hasOwn(source, key),
      ) ||
      typeof source.updatedAt !== 'string' ||
      !Number.isFinite(Date.parse(source.updatedAt)) ||
      new Date(source.updatedAt).toISOString() !== source.updatedAt
    ) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    const switchIntent = backendSwitchIntentFromUnknown(
      source.backendSwitchIntent,
    );
    const provisional = {
      ...source,
      backendSwitchIntent: switchIntent,
    } as unknown as FleetRecord;
    const shell = Object.hasOwn(source, 'decommissionIntent')
      ? normalizeDecommissionAdvanceIntent(
          source.decommissionIntent as import('./types.js').DecommissionAdvanceIntent,
          provisional,
        )
      : undefined;
    if (shell && shell.updatedAt !== source.updatedAt) {
      throw new Error(BACKEND_SWITCH_RECORD_ERROR);
    }
    // Unlike the decommission shell, a cleanup intent's updatedAt tracks the
    // cleanup operation rather than the switch write, so no record-timestamp
    // equality is required here; the strict codec still validates identity.
    const cleanupIntent = Object.hasOwn(source, 'cleanupIntent')
      ? normalizeCleanupAdvanceIntent(
          source.cleanupIntent as CleanupAdvanceIntent,
          provisional,
        )
      : undefined;
    const invocationAuthority = Object.hasOwn(source, 'invocationAuthority')
      ? invocationAuthorityCarrierFromUnknown(source.invocationAuthority)
      : undefined;
    const record = canonicalFleetRecordFromSource(
      source,
      switchIntent,
      shell,
      cleanupIntent,
      invocationAuthority,
    );
    return {
      record,
      comparisonBytes: JSON.stringify(canonicalJsonValue(record)),
    };
  } catch {
    throw new Error(BACKEND_SWITCH_RECORD_ERROR);
  }
}

/** @internal Constructs the initial backend-switch operation shell. */
export function backendSwitchDecommissionShell(
  input: Readonly<{
    record: FleetRecord;
    intent: BackendSwitchIntent;
    operationId: string;
    snapshotSha256: string;
    entrySubphase: BackendSwitchSubphase;
    now: string;
  }>,
): import('./types.js').DecommissionAdvanceIntent {
  assertNoActiveCleanup(input.record, 'backend switch decommission');
  const snapshot = input.intent.decommissionSnapshot;
  if (
    !snapshot ||
    input.intent.decommissionSnapshotSha256 !== input.snapshotSha256 ||
    input.intent.decommissionEntrySubphase !== input.entrySubphase
  ) {
    throw new Error('backend switch decommission authority is incomplete');
  }
  const lifecyclePhase = backendSwitchDecommissionLifecyclePhase(
    input.intent.subphase,
    effectiveLifecyclePhase(
      input.record,
    ) as import('./types.js').NormalDecommissionLifecyclePhase,
  );
  if (lifecyclePhase === 'decommissioned') {
    throw new Error('backend switch decommission is already complete');
  }
  return {
    version: 1,
    operationId: input.operationId,
    revision: 0,
    generation: 0,
    updatedAt: input.now,
    identity: {
      record: {
        tenantTag: input.record.tenantTag,
        environment: input.record.environment,
        backend: input.record.backend,
        scriptName: input.record.scriptName,
        databaseId: input.record.databaseId,
        databaseName: input.record.databaseName,
        routeHostname: input.record.routeHostname,
      },
      mode: {
        kind: 'backend-switch',
        priorSpecDigest: input.intent.prior.specDigest,
        targetSpecDigest: input.intent.targetSpecDigest,
        decommissionSnapshotSha256: input.snapshotSha256,
        backendSwitchSubphase: input.entrySubphase,
      },
    },
    lifecyclePhase,
    state: 'transitioning',
  };
}

type ConsumedSwitchEntryCarrier =
  | 'pendingSpecDigest'
  | 'pendingArtifactVersion'
  | 'pendingRelease'
  | 'migrationPriorRelease'
  | 'rollbackRelease'
  | 'retiringRelease'
  | 'migrationIntent';

function withoutConsumedSwitchEntryCarriers(
  record: FleetRecord,
): Omit<FleetRecord, ConsumedSwitchEntryCarrier> {
  const {
    pendingSpecDigest: _pendingSpecDigest,
    pendingArtifactVersion: _pendingArtifactVersion,
    pendingRelease: _pendingRelease,
    migrationPriorRelease: _migrationPriorRelease,
    rollbackRelease: _rollbackRelease,
    retiringRelease: _retiringRelease,
    migrationIntent: _migrationIntent,
    ...stable
  } = record;
  return stable;
}

/** @internal Atomically consumes switch-entry carriers and installs its shell. */
export function normalizeSwitchDecommissionEntry(
  record: FleetRecord,
  intent: BackendSwitchIntent,
  shell: import('./types.js').DecommissionAdvanceIntent,
): FleetRecord {
  const snapshot = intent.decommissionSnapshot;
  if (!snapshot) {
    throw new Error('backend switch decommission authorization was lost');
  }
  const stable = withoutConsumedSwitchEntryCarriers(record);
  const applicationResources = (
    intent.applicationR2Progress ??
    snapshot.applicationResources.map((resource) => ({
      resource,
      subphase: resource.state,
    }))
  ).map(({ resource, subphase }) => ({ ...resource, state: subphase }));
  return {
    ...stable,
    desiredSpecDigest: snapshot.desiredSpecDigest,
    backendSwitchIntent: intent,
    decommissionIntent: shell,
    applicationResources,
    phase: 'decommission-advancing',
    updatedAt: shell.updatedAt,
  };
}

export interface BackendSwitchLease extends ExternalMutationFence {
  get(): Promise<BackendSwitchIntent | undefined>;
  put(intent: BackendSwitchIntent): Promise<void>;
  current(): FleetRecord;
  putOwnership(record: FleetRecord, intent: BackendSwitchIntent): Promise<void>;
}

export type BackendSwitchMutationFence = ExternalMutationFence;

export function finalizedBridgeForRecord(record: FleetRecord): BridgeSnapshot {
  const intent = record.backendSwitchIntent;
  const bridge = intent?.bridge;
  const state = record.platformResources?.stateWorker;
  if (
    intent?.subphase !== 'finalized' ||
    !bridge?.stateOnly ||
    bridge.publicRouteAttached ||
    record.backend !== 'workers-for-platforms' ||
    state?.plane !== 'ordinary' ||
    bridge.scriptName !== record.scriptName ||
    bridge.scriptName !== state.scriptName ||
    bridge.databaseId !== record.databaseId ||
    bridge.artifactVersion !== state.artifactVersion ||
    bridge.artifactDigest !== state.artifactDigest ||
    JSON.stringify(bridge.durableObjectBindings) !==
      JSON.stringify(state.durableObjectBindings) ||
    JSON.stringify(bridge.namespaceIds) !== JSON.stringify(state.namespaceIds)
  ) {
    throw new Error(
      'finalized backend switch does not own an exact ordinary state bridge',
    );
  }
  return bridge;
}

export function assertBackendSwitchInactive(record: FleetRecord): void {
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

/**
 * @internal Durably commits the candidate-invocation authority flip before a
 * candidate-invoking provider dispatch. A no-op when the carrier already
 * carries a timestamp; otherwise the dedicated flip put is awaited so a write
 * failure aborts the flow before the provider call dispatches. Legacy records
 * without a carrier receive the whole carrier at the flip.
 */
export async function commitInvocationAuthority(
  lease: Pick<FleetStateLease, 'put'>,
  record: FleetRecord,
  clock: () => number,
): Promise<FleetRecord> {
  if (typeof record.invocationAuthority?.authorizedAt === 'string') {
    return record;
  }
  const timestamp = new Date(clock()).toISOString();
  const flipped: FleetRecord = {
    ...record,
    invocationAuthority: { version: 1, authorizedAt: timestamp },
    updatedAt: timestamp,
  };
  await lease.put(flipped);
  return flipped;
}

/** @internal Package-private test/coordination seam; not a root export. */
export async function withBackendSwitchLease<T>(
  store: FleetStateStore,
  tenantTag: string,
  environment: string,
  clock: () => number,
  operation: (lease: BackendSwitchLease) => Promise<T>,
): Promise<T> {
  return store.withDeploymentLease(
    tenantTag,
    environment,
    async (fleetLease) => {
      const loaded = await store.get(tenantTag, environment);
      if (!loaded) {
        throw new Error(
          `deployment '${tenantTag}:${environment}' is not provisioned`,
        );
      }
      const structuralLoaded =
        structuralBackendSwitchFleetRecordFromUnknown(loaded);
      let record = structuralLoaded.carriesBackendSwitchAuthority
        ? backendSwitchFleetRecordFromUnknown(loaded).record
        : structuralLoaded.record;
      const writeCanonical = async (
        intended: CanonicalBackendSwitchFleetRecord,
      ): Promise<void> => {
        try {
          await fleetLease.put(intended.record);
          record = intended.record;
        } catch (writeError) {
          const reread = await store.get(tenantTag, environment);
          if (!reread) throw writeError;
          const canonicalReread = backendSwitchFleetRecordFromUnknown(reread);
          if (canonicalReread.comparisonBytes !== intended.comparisonBytes) {
            throw writeError;
          }
          record = canonicalReread.record;
        }
      };
      const lease: BackendSwitchLease = {
        mutationLeaseTtlMs: fleetLease.mutationLeaseTtlMs,
        assertOwned: () => fleetLease.assertOwned(),
        get: async () => record?.backendSwitchIntent,
        current: () => record as FleetRecord,
        put: async (rawIntent) => {
          if (record.decommissionIntent) {
            throw new Error(
              'backend switch lease put requires putOwnership when a decommission shell is present',
            );
          }
          const intent = backendSwitchIntentFromUnknown(rawIntent);
          if (
            intent.tenantTag !== tenantTag ||
            intent.environment !== environment
          ) {
            throw new Error('backend switch lease cannot write another intent');
          }
          const timestamp = new Date(clock()).toISOString();
          // The invocation-authority flip rides the candidate-invoking
          // authorization-transition put so the carrier is durable before the
          // external candidate upload or dispatch call it authorizes.
          const flip =
            (intent.subphase === 'candidate-deploy-authorized' ||
              intent.subphase === 'host-publish-authorized') &&
            typeof record.invocationAuthority?.authorizedAt !== 'string';
          const provisional = backendSwitchFleetRecordFromUnknown({
            ...record,
            ...(flip
              ? {
                  invocationAuthority: {
                    version: 1,
                    authorizedAt: timestamp,
                  } satisfies InvocationAuthorityCarrier,
                }
              : {}),
            backendSwitchIntent: intent,
          }).record;
          const intended = backendSwitchFleetRecordFromUnknown({
            ...provisional,
            updatedAt: timestamp,
          });
          await writeCanonical(intended);
        },
        putOwnership: async (rawNextRecord, rawIntent) => {
          const nextRecord =
            structuralBackendSwitchFleetRecordFromUnknown(rawNextRecord).record;
          if (
            record.decommissionIntent &&
            !Object.hasOwn(nextRecord, 'decommissionIntent')
          ) {
            throw new Error(BACKEND_SWITCH_RECORD_ERROR);
          }
          const intent = backendSwitchIntentFromUnknown(rawIntent);
          if (
            nextRecord.tenantTag !== tenantTag ||
            nextRecord.environment !== environment
          ) {
            throw new Error(
              'backend switch lease cannot commit another deployment',
            );
          }
          if (
            nextRecord.updatedAt !== record.updatedAt ||
            (nextRecord.decommissionIntent &&
              nextRecord.decommissionIntent.updatedAt !== record.updatedAt)
          ) {
            throw new Error(
              'backend switch lease ownership timestamp placeholder is stale',
            );
          }
          const provisional = backendSwitchFleetRecordFromUnknown({
            ...nextRecord,
            backendSwitchIntent: intent,
          }).record;
          const timestamp = new Date(clock()).toISOString();
          const intended = backendSwitchFleetRecordFromUnknown({
            ...provisional,
            updatedAt: timestamp,
            ...(provisional.decommissionIntent
              ? {
                  decommissionIntent: {
                    ...provisional.decommissionIntent,
                    updatedAt: timestamp,
                  },
                }
              : {}),
          });
          await writeCanonical(intended);
        },
      };
      return operation(lease);
    },
  );
}

export interface BackendSwitchProvider {
  /** Optional exact inspection used only when a live pending ordinary version exists. */
  captureSwitchEntryPendingArtifact?(
    input: Readonly<{
      readonly expectedArtifactVersion: string;
      readonly spec: DeploymentSpec;
      readonly currentRecord: FleetRecord;
      readonly fence: BackendSwitchMutationFence;
    }>,
  ): Promise<unknown>;
  /** Optional bounded attachment scanner used by the root-only advance API. */
  advanceSwitchDecommissionAttachmentScan?(
    input: DecommissionAttachmentScanInput,
  ): Promise<unknown>;
  /** Receipt storage authority paired with `exportSwitchDatabaseReceipt`. */
  readonly databaseExportReceiptAuthority?: string;
  /** Optional idempotent receipt export used by bounded teardown. */
  exportSwitchDatabaseReceipt?(
    identity: DatabaseExportReceiptIdentity,
    input: Readonly<{
      prior: PlainBackendSnapshot;
      targetSpec: DeploymentSpec;
      fence: BackendSwitchMutationFence;
    }>,
  ): Promise<import('./types.js').DatabaseExport>;
  getSwitchDatabase?(
    databaseId: string,
  ): Promise<DatabaseReference | undefined>;
  readSwitchDatabaseOwner?(
    database: DatabaseReference,
    fence: BackendSwitchMutationFence,
  ): Promise<string | undefined>;
  assertSwitchDatabaseDeletionResidualsRemoved?(
    input: Readonly<{
      prior: PlainBackendSnapshot;
      targetSpec: DeploymentSpec;
      currentRecord: FleetRecord;
      database: DatabaseReference;
      fence: BackendSwitchMutationFence;
    }>,
  ): Promise<void>;
  deleteSwitchDatabaseBounded?(
    input: Readonly<{
      prior: PlainBackendSnapshot;
      targetSpec: DeploymentSpec;
      database: DatabaseReference;
      fence: BackendSwitchMutationFence;
    }>,
  ): Promise<void>;
  describeFinalizedBridgeTarget(
    targetSpec: DeploymentSpec,
    currentRecord: FleetRecord,
  ): ExternalPlatformTargetDescription;
  describeFinalizedState(input: {
    readonly targetSpec: DeploymentSpec;
    readonly currentRecord: FleetRecord;
    readonly target: ExternalPlatformTargetDescription;
  }): BridgeMutationPlan;
  assertFinalizedState(input: {
    readonly targetSpec: DeploymentSpec;
    readonly currentRecord: FleetRecord;
    readonly target: ExternalPlatformTargetDescription;
    readonly plan: BridgeMutationPlan;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  ensureFinalizedState(input: {
    readonly targetSpec: DeploymentSpec;
    readonly currentRecord: FleetRecord;
    readonly target: ExternalPlatformTargetDescription;
    readonly plan: BridgeMutationPlan;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot>;
  describeBridge(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly prior: PlainBackendSnapshot;
  }): BridgeMutationPlan;
  snapshotPlainDeployment(
    priorSpec: DeploymentSpec,
    currentRecord: FleetRecord,
    fence: BackendSwitchMutationFence,
  ): Promise<PlainBackendSnapshot>;
  ensureBridge(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly secrets: DeploymentSecrets;
    readonly prior: PlainBackendSnapshot;
    readonly plan: BridgeMutationPlan;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot>;
  recoverBridge(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly prior: PlainBackendSnapshot;
    readonly plan: BridgeMutationPlan;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot | undefined>;
  ensureCandidate(input: {
    readonly targetSpec: DeploymentSpec;
    readonly secrets: DeploymentSecrets;
    readonly bridge: BridgeSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly currentRecord: FleetRecord;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BackendSwitchCandidateSnapshot>;
  publishCandidateHost(input: {
    readonly targetSpec: DeploymentSpec;
    readonly candidate: ExternalReleaseSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  assertCandidateHostPublished(input: {
    readonly targetSpec: DeploymentSpec;
    readonly candidate: ExternalReleaseSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  detachPlainCustomDomain(input: {
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  assertCandidateServing(input: {
    readonly targetSpec: DeploymentSpec;
    readonly candidate: ExternalReleaseSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  privatizeBridge(input: {
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot>;
  commitWorkersForPlatformsOwnership(input: {
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly candidate: ExternalReleaseSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly targetSpec: DeploymentSpec;
    readonly currentRecord: FleetRecord;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<FleetRecord>;
  routePlainDomainToBridge(input: {
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  assertPlainBridgeServing(input: {
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  removeCandidateHostAndDrain(input: {
    readonly targetSpec: DeploymentSpec;
    readonly candidate: ExternalReleaseSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  restorePlainDeployment(input: {
    readonly priorSpec: DeploymentSpec;
    readonly targetSpec: DeploymentSpec;
    readonly secrets: DeploymentSecrets;
    readonly prior: PlainBackendSnapshot;
    readonly bridge: BridgeSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<string>;
  commitPlainOwnership(input: {
    readonly prior: PlainBackendSnapshot;
    readonly restoredArtifactVersion: string;
    readonly currentRecord: FleetRecord;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<FleetRecord>;
  ensureStateOnlyBridge(input: {
    readonly targetSpec: DeploymentSpec;
    readonly bridge: BridgeSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<BridgeSnapshot>;
  commitFinalizedOwnership(input: {
    readonly currentRecord: FleetRecord;
    readonly bridge: BridgeSnapshot;
    readonly target: ExternalPlatformTargetDescription;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<FleetRecord>;
  removeSwitchTraffic(input: {
    readonly prior: PlainBackendSnapshot;
    readonly priorSpec: DeploymentSpec;
    readonly bridge?: BridgeSnapshot;
    readonly plan?: BridgeMutationPlan;
    readonly targetSpec: DeploymentSpec;
    readonly allowedArtifactVersions: readonly string[];
    readonly tenantTag: string;
    readonly environment: string;
    readonly routeHostname: string;
    readonly routeTargets: readonly HostRoutingTarget[];
    readonly entryPendingArtifact?: Readonly<{
      artifactVersion: string;
      namespaceIds: readonly string[];
      spec: DeploymentSpec;
    }>;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  assertSwitchTrafficRemoved(input: {
    readonly prior: PlainBackendSnapshot;
    readonly routeHostname: string;
  }): Promise<void>;
  removeSwitchRelease(input: {
    readonly prior: PlainBackendSnapshot;
    readonly tenantTag: string;
    readonly environment: string;
    readonly routeHostname: string;
    readonly release: ExternalReleaseSnapshot;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  removeSwitchBridge(input: {
    readonly prior: PlainBackendSnapshot;
    readonly priorSpec: DeploymentSpec;
    readonly bridge?: BridgeSnapshot;
    readonly plan?: BridgeMutationPlan;
    readonly targetSpec: DeploymentSpec;
    readonly allowedArtifactVersions: readonly string[];
    readonly entryPendingArtifact?: Readonly<{
      artifactVersion: string;
      namespaceIds: readonly string[];
      spec: DeploymentSpec;
    }>;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
  findSwitchApplicationR2(
    resource: import('./types.js').ApplicationR2Resource,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined>;
  assertSwitchApplicationR2Detached(
    resource: import('./types.js').ApplicationR2Resource,
    fence: BackendSwitchMutationFence,
  ): Promise<void>;
  assertSwitchApplicationR2Empty(
    resource: import('./types.js').ApplicationR2Resource,
    fence: BackendSwitchMutationFence,
  ): Promise<void>;
  deleteSwitchApplicationR2(
    resource: import('./types.js').ApplicationR2Resource,
    fence: BackendSwitchMutationFence,
  ): Promise<void>;
  exportSwitchDatabase(input: {
    readonly prior: PlainBackendSnapshot;
    readonly targetSpec: DeploymentSpec;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<import('./types.js').DatabaseExport>;
  deleteSwitchDatabase(input: {
    readonly prior: PlainBackendSnapshot;
    readonly targetSpec: DeploymentSpec;
    readonly fence: BackendSwitchMutationFence;
  }): Promise<void>;
}

export type FinalizedOrdinaryStateProvider = Pick<
  BackendSwitchProvider,
  | 'describeFinalizedBridgeTarget'
  | 'describeFinalizedState'
  | 'assertFinalizedState'
  | 'ensureFinalizedState'
  | 'commitFinalizedOwnership'
>;

function assertFinalizedOwnershipCommit(
  prior: FleetRecord,
  committed: FleetRecord,
  bridge: BridgeSnapshot,
  target: ExternalPlatformTargetDescription,
): void {
  const { platformResources: _priorResources, ...priorIdentity } = prior;
  const { platformResources, ...committedIdentity } = committed;
  if (
    JSON.stringify(committedIdentity) !== JSON.stringify(priorIdentity) ||
    !platformResources
  ) {
    throw new Error(
      'finalized ownership provider changed immutable deployment or release identity',
    );
  }
  assertPlatformResourcesMatchTarget(platformResources, target);
  const state = platformResources.stateWorker;
  if (
    state.plane !== 'ordinary' ||
    state.dispatchNamespace !== undefined ||
    state.scriptName !== bridge.scriptName ||
    state.artifactVersion !== bridge.artifactVersion ||
    state.artifactDigest !== bridge.artifactDigest ||
    state.durableObjectTag !== target.stateDurableObjectTag ||
    JSON.stringify(state.durableObjectBindings) !==
      JSON.stringify(bridge.durableObjectBindings) ||
    JSON.stringify(state.namespaceIds) !== JSON.stringify(bridge.namespaceIds)
  ) {
    throw new Error(
      'finalized ownership provider returned an inexact ordinary state bridge',
    );
  }
}

export async function reconcileFinalizedBackendSwitchState(input: {
  readonly provider: FinalizedOrdinaryStateProvider;
  readonly targetSpec: DeploymentSpec;
  readonly target: ExternalPlatformTargetDescription;
  readonly record: FleetRecord;
  readonly lease: FleetStateLease;
  readonly clock: () => number;
}): Promise<FleetRecord> {
  assertNoActiveDecommission(
    input.record,
    'reconcileFinalizedBackendSwitchState',
  );
  assertNoActiveCleanup(input.record, 'reconcileFinalizedBackendSwitchState');
  const priorBridge = finalizedBridgeForRecord(input.record);
  const switchIntent = input.record.backendSwitchIntent;
  if (!switchIntent) {
    throw new Error('finalized state reconciliation requires switch intent');
  }
  if (
    input.target.stateEgressCredentialDigest !==
    input.record.platformTarget?.stateEgressCredentialDigest
  ) {
    throw new Error(
      'state-egress credential digest is immutable; rotate it only through a coordinated credential migration',
    );
  }
  const plan = input.provider.describeFinalizedState({
    targetSpec: input.targetSpec,
    currentRecord: input.record,
    target: input.target,
  });
  const targetSpecDigest = deploymentSpecDigest(input.targetSpec);
  const persisted = input.record.backendSwitchIntent?.stateReconcileIntent;
  if (
    persisted?.subphase === 'upload-authorized' &&
    (persisted.targetSpecDigest !== targetSpecDigest ||
      JSON.stringify(persisted.plan) !== JSON.stringify(plan))
  ) {
    throw new Error(
      'finalized state retry differs from its durable upload authorization',
    );
  }
  let authorized = input.record;
  if (
    persisted?.targetSpecDigest !== targetSpecDigest ||
    JSON.stringify(persisted.plan) !== JSON.stringify(plan) ||
    persisted.subphase !== 'upload-authorized'
  ) {
    authorized = {
      ...input.record,
      backendSwitchIntent: {
        ...switchIntent,
        stateReconcileIntent: {
          targetSpecDigest,
          plan,
          subphase: 'upload-authorized',
        },
      },
      updatedAt: new Date(input.clock()).toISOString(),
    };
    await input.lease.put(authorized);
  }
  await input.lease.assertOwned();
  const bridge = await input.provider.ensureFinalizedState({
    targetSpec: input.targetSpec,
    currentRecord: authorized,
    target: input.target,
    plan,
    fence: input.lease,
  });
  if (
    !bridge.stateOnly ||
    bridge.publicRouteAttached ||
    bridge.scriptName !== priorBridge.scriptName ||
    bridge.databaseId !== priorBridge.databaseId ||
    priorBridge.namespaceIds.some(
      (namespaceId) => !bridge.namespaceIds.includes(namespaceId),
    )
  ) {
    throw new Error(
      'finalized state reconciliation changed bridge ownership or namespaces',
    );
  }
  const committed = await input.provider.commitFinalizedOwnership({
    currentRecord: authorized,
    bridge,
    target: input.target,
    fence: input.lease,
  });
  assertFinalizedOwnershipCommit(authorized, committed, bridge, input.target);
  const authorizedIntent = authorized.backendSwitchIntent;
  if (!authorizedIntent) {
    throw new Error('finalized state authorization lost switch intent');
  }
  const next: FleetRecord = {
    ...committed,
    backendSwitchIntent: {
      ...authorizedIntent,
      target: input.target,
      bridge,
      bridgePlan: plan,
      stateReconcileIntent: {
        targetSpecDigest,
        plan,
        subphase: 'uploaded',
      },
    },
    platformTarget: input.target,
    outboundPolicy: input.target.outboundPolicy,
    ...(input.target.stateDurableObjectTag
      ? { durableObjectTag: input.target.stateDurableObjectTag }
      : {}),
    durableObjectMigrationHistory: plan.durableObjectMigrations,
    durableObjectMigrationHistoryDigest:
      input.target.stateDurableObjectHistoryDigest,
    updatedAt: new Date(input.clock()).toISOString(),
  };
  await input.lease.put(next);
  return next;
}

function assertExternalTarget(spec: DeploymentSpec): void {
  if (spec.authoredBy !== 'external') {
    throw new Error('backend switch target must be externally authored');
  }
}

function assertPlainPrior(spec: DeploymentSpec): void {
  if (spec.authoredBy !== 'platform') {
    throw new Error('backend switch prior must be platform-authored');
  }
}

function assertSameDeployment(
  priorSpec: DeploymentSpec,
  targetSpec: DeploymentSpec,
): void {
  if (
    priorSpec.tenantTag !== targetSpec.tenantTag ||
    priorSpec.environment !== targetSpec.environment ||
    priorSpec.scriptName !== targetSpec.scriptName ||
    priorSpec.databaseName !== targetSpec.databaseName ||
    priorSpec.routeHostname.toLowerCase() !==
      targetSpec.routeHostname.toLowerCase()
  ) {
    throw new Error('backend switch cannot change deployment identity');
  }
  if (
    priorSpec.schemaVersion !== targetSpec.schemaVersion ||
    d1MigrationHistoryDigest(priorSpec.migrations) !==
      d1MigrationHistoryDigest(targetSpec.migrations)
  ) {
    throw new Error('backend switch cannot migrate D1 schema or history');
  }
}

function next(
  intent: BackendSwitchIntent,
  subphase: BackendSwitchSubphase,
  patch: Partial<BackendSwitchIntent> = {},
): BackendSwitchIntent {
  return { ...intent, ...patch, subphase };
}

function requiredBridge(intent: BackendSwitchIntent): BridgeSnapshot {
  if (!intent.bridge) throw new Error('backend switch has no bridge');
  return intent.bridge;
}

function requiredBridgePlan(intent: BackendSwitchIntent): BridgeMutationPlan {
  if (!intent.bridgePlan) throw new Error('backend switch has no bridge plan');
  return intent.bridgePlan;
}

function requiredCandidate(
  intent: BackendSwitchIntent,
): BackendSwitchCandidateSnapshot {
  if (!intent.candidate) throw new Error('backend switch has no candidate');
  return intent.candidate;
}

function canonicalCandidateRelease(
  intent: BackendSwitchIntent,
): ExternalReleaseSnapshot {
  const candidate = requiredCandidate(intent);
  return {
    physicalScriptName: candidate.physicalScriptName,
    specDigest: candidate.specDigest,
    artifactVersion: candidate.artifactVersion,
    releaseSchemaVersion: candidate.releaseSchemaVersion,
    application: candidate.application,
    ...(candidate.topology ? { topology: candidate.topology } : {}),
  };
}

function assertWorkersForPlatformsOwnership(
  record: FleetRecord,
  intent: BackendSwitchIntent,
): void {
  if (
    record.backend !== 'workers-for-platforms' ||
    record.scriptName !== intent.prior.scriptName ||
    record.databaseId !== intent.prior.databaseId ||
    record.phase !== 'ready' ||
    !sameCanonicalData(
      record.activeRelease,
      canonicalCandidateRelease(intent),
    ) ||
    !sameCanonicalData(record.platformTarget, intent.target) ||
    !sameCanonicalData(record.outboundPolicy, intent.target.outboundPolicy) ||
    record.platformResources?.stateWorker.scriptName !==
      requiredBridge(intent).scriptName ||
    record.migrationIntent !== undefined
  ) {
    throw new Error(
      'provider returned incomplete Workers for Platforms ownership',
    );
  }
}

function assertPlainOwnership(
  record: FleetRecord,
  intent: BackendSwitchIntent,
): void {
  if (
    record.backend !== 'plain-worker' ||
    record.scriptName !== intent.prior.scriptName ||
    record.databaseId !== intent.prior.databaseId ||
    record.desiredSpecDigest !== intent.prior.specDigest ||
    record.artifactVersion !== intent.restoredArtifactVersion ||
    record.phase !== 'ready' ||
    record.activeRelease !== undefined ||
    record.platformResources !== undefined ||
    record.platformTarget !== undefined ||
    record.outboundPolicy !== undefined ||
    record.migrationIntent !== undefined
  ) {
    throw new Error('provider returned incomplete plain Worker ownership');
  }
}

async function persistBeforeMutation(
  lease: BackendSwitchLease,
  intent: BackendSwitchIntent,
  subphase: BackendSwitchSubphase,
): Promise<BackendSwitchIntent> {
  const updated = next(intent, subphase);
  await lease.put(updated);
  await lease.assertOwned();
  return updated;
}

export async function switchPlainDeploymentToWorkersForPlatforms(options: {
  readonly store: FleetStateStore;
  readonly provider: BackendSwitchProvider;
  readonly priorSpec: DeploymentSpec;
  readonly targetSpec: DeploymentSpec;
  readonly target: ExternalPlatformTargetDescription;
  readonly secrets: DeploymentSecrets;
  readonly rollbackUntil: string;
}): Promise<BackendSwitchIntent> {
  assertPlainPrior(options.priorSpec);
  assertExternalTarget(options.targetSpec);
  assertSameDeployment(options.priorSpec, options.targetSpec);
  const rollbackUntil = new Date(options.rollbackUntil);
  if (!Number.isFinite(rollbackUntil.getTime())) {
    throw new Error('backend switch rollbackUntil is invalid');
  }
  return withBackendSwitchLease(
    options.store,
    options.priorSpec.tenantTag,
    options.priorSpec.environment,
    Date.now,
    async (lease) => {
      assertNoActiveDecommission(
        lease.current(),
        'switchPlainDeploymentToWorkersForPlatforms',
      );
      assertNoActiveCleanup(
        lease.current(),
        'switchPlainDeploymentToWorkersForPlatforms',
      );
      let intent = await lease.get();
      if (!intent) {
        const prior = await options.provider.snapshotPlainDeployment(
          options.priorSpec,
          lease.current(),
          lease,
        );
        if (
          prior.scriptName !== options.priorSpec.scriptName ||
          prior.specDigest !== deploymentSpecDigest(options.priorSpec)
        ) {
          throw new Error(
            'plain deployment snapshot does not match prior spec',
          );
        }
        intent = {
          kind: 'backend-switch',
          tenantTag: options.priorSpec.tenantTag,
          environment: options.priorSpec.environment,
          prior,
          targetSpecDigest: deploymentSpecDigest(options.targetSpec),
          targetApplication: applicationBindingTopology(
            options.targetSpec,
            prior.applicationResources,
          ),
          target: options.target,
          rollbackUntil: rollbackUntil.toISOString(),
          subphase: 'planned',
        };
        await lease.put(intent);
      }
      if (
        intent.targetSpecDigest !== deploymentSpecDigest(options.targetSpec) ||
        JSON.stringify(intent.targetApplication) !==
          JSON.stringify(
            applicationBindingTopology(
              options.targetSpec,
              intent.prior.applicationResources,
            ),
          ) ||
        !sameCanonicalData(intent.target, options.target)
      ) {
        throw new Error('backend switch request differs from durable intent');
      }
      if (
        intent.subphase === 'rolled-back' ||
        intent.subphase === 'finalized'
      ) {
        throw new Error(`backend switch is already ${intent.subphase}`);
      }
      if (intent.subphase === 'ready') return intent;

      if (
        intent.subphase === 'planned' ||
        intent.subphase === 'bridge-upload-authorized'
      ) {
        const bridgePlan =
          intent.bridgePlan ??
          options.provider.describeBridge({
            priorSpec: options.priorSpec,
            targetSpec: options.targetSpec,
            prior: intent.prior,
          });
        intent = next(intent, 'bridge-upload-authorized', { bridgePlan });
        await lease.put(intent);
        await lease.assertOwned();
        const bridge = await options.provider.ensureBridge({
          priorSpec: options.priorSpec,
          targetSpec: options.targetSpec,
          secrets: options.secrets,
          prior: intent.prior,
          plan: requiredBridgePlan(intent),
          fence: lease,
        });
        intent = next(intent, 'bridge-deployed', { bridge });
        await lease.put(intent);
      }
      if (!intent.bridge) throw new Error('backend switch has no bridge');

      if (
        intent.subphase === 'bridge-deployed' ||
        intent.subphase === 'candidate-deploy-authorized'
      ) {
        intent = await persistBeforeMutation(
          lease,
          intent,
          'candidate-deploy-authorized',
        );
        const candidate = await options.provider.ensureCandidate({
          targetSpec: options.targetSpec,
          secrets: options.secrets,
          bridge: requiredBridge(intent),
          target: intent.target,
          currentRecord: lease.current(),
          fence: lease,
        });
        if (
          JSON.stringify(candidate.application) !==
          JSON.stringify(intent.targetApplication)
        ) {
          throw new Error(
            'backend switch candidate application differs from durable intent',
          );
        }
        intent = next(intent, 'candidate-deployed', { candidate });
        await lease.put(intent);
      }
      if (!intent.candidate) throw new Error('backend switch has no candidate');
      if (
        intent.candidate.maintenance.specDigest !== intent.targetSpecDigest ||
        intent.candidate.maintenance.receipt.length === 0
      ) {
        throw new Error('backend switch candidate is not maintenance-armed');
      }

      if (
        intent.subphase === 'candidate-deployed' ||
        intent.subphase === 'host-publish-authorized'
      ) {
        intent = await persistBeforeMutation(
          lease,
          intent,
          'host-publish-authorized',
        );
        await options.provider.publishCandidateHost({
          targetSpec: options.targetSpec,
          candidate: requiredCandidate(intent),
          bridge: requiredBridge(intent),
          target: intent.target,
          fence: lease,
        });
        await options.provider.assertCandidateHostPublished({
          targetSpec: options.targetSpec,
          candidate: requiredCandidate(intent),
          target: intent.target,
          fence: lease,
        });
        intent = next(intent, 'host-published');
        await lease.put(intent);
      }

      if (
        intent.subphase === 'host-published' ||
        intent.subphase === 'domain-detach-authorized'
      ) {
        await options.provider.assertCandidateHostPublished({
          targetSpec: options.targetSpec,
          candidate: requiredCandidate(intent),
          target: intent.target,
          fence: lease,
        });
        intent = await persistBeforeMutation(
          lease,
          intent,
          'domain-detach-authorized',
        );
        await options.provider.detachPlainCustomDomain({
          prior: intent.prior,
          bridge: requiredBridge(intent),
          fence: lease,
        });
        await options.provider.assertCandidateServing({
          targetSpec: options.targetSpec,
          candidate: requiredCandidate(intent),
          fence: lease,
        });
        intent = next(intent, 'dispatch-serving');
        await lease.put(intent);
      }

      if (
        intent.subphase === 'dispatch-serving' ||
        intent.subphase === 'bridge-private-authorized'
      ) {
        await options.provider.assertCandidateServing({
          targetSpec: options.targetSpec,
          candidate: requiredCandidate(intent),
          fence: lease,
        });
        intent = await persistBeforeMutation(
          lease,
          intent,
          'bridge-private-authorized',
        );
        const bridge = await options.provider.privatizeBridge({
          bridge: requiredBridge(intent),
          fence: lease,
        });
        intent = next(intent, 'bridge-private', { bridge });
        await lease.put(intent);
      }

      if (
        intent.subphase === 'bridge-private' ||
        intent.subphase === 'ownership-commit-authorized'
      ) {
        intent = await persistBeforeMutation(
          lease,
          intent,
          'ownership-commit-authorized',
        );
        const ownership =
          await options.provider.commitWorkersForPlatformsOwnership({
            prior: intent.prior,
            bridge: requiredBridge(intent),
            candidate: requiredCandidate(intent),
            target: intent.target,
            targetSpec: options.targetSpec,
            currentRecord: lease.current(),
            fence: lease,
          });
        intent = next(intent, 'ready');
        assertWorkersForPlatformsOwnership(ownership, intent);
        await lease.putOwnership(ownership, intent);
      }
      return intent;
    },
  );
}

export async function rollbackBackendSwitch(options: {
  readonly store: FleetStateStore;
  readonly provider: BackendSwitchProvider;
  readonly priorSpec: DeploymentSpec;
  readonly targetSpec: DeploymentSpec;
  readonly secrets: DeploymentSecrets;
}): Promise<BackendSwitchIntent> {
  assertPlainPrior(options.priorSpec);
  assertExternalTarget(options.targetSpec);
  assertSameDeployment(options.priorSpec, options.targetSpec);
  return withBackendSwitchLease(
    options.store,
    options.priorSpec.tenantTag,
    options.priorSpec.environment,
    Date.now,
    async (lease) => {
      assertNoActiveDecommission(lease.current(), 'rollbackBackendSwitch');
      assertNoActiveCleanup(lease.current(), 'rollbackBackendSwitch');
      let intent = await lease.get();
      if (!intent) {
        throw new Error('backend switch has no rollback snapshot');
      }
      if (
        intent.prior.specDigest !== deploymentSpecDigest(options.priorSpec) ||
        intent.targetSpecDigest !== deploymentSpecDigest(options.targetSpec)
      ) {
        throw new Error(
          'backend switch rollback specs differ from durable intent',
        );
      }
      if (intent.subphase === 'finalized') {
        throw new Error('backend switch rollback window is finalized');
      }
      if (intent.subphase === 'rolled-back') return intent;
      if (new Date(intent.rollbackUntil).getTime() <= Date.now()) {
        throw new Error('backend switch rollback window has expired');
      }
      if (intent.subphase === 'planned') {
        intent = next(intent, 'rolled-back', {
          restoredArtifactVersion: intent.prior.artifactVersion,
        });
        assertPlainOwnership(lease.current(), intent);
        await lease.putOwnership(lease.current(), intent);
        return intent;
      }
      if (!intent.bridge) {
        const bridgePlan =
          intent.bridgePlan ??
          options.provider.describeBridge({
            priorSpec: options.priorSpec,
            targetSpec: options.targetSpec,
            prior: intent.prior,
          });
        if (!intent.bridgePlan) {
          intent = next(intent, intent.subphase, { bridgePlan });
          await lease.put(intent);
        }
        const bridge =
          (await options.provider.recoverBridge({
            priorSpec: options.priorSpec,
            targetSpec: options.targetSpec,
            prior: intent.prior,
            plan: requiredBridgePlan(intent),
            fence: lease,
          })) ??
          (await options.provider.ensureBridge({
            priorSpec: options.priorSpec,
            targetSpec: options.targetSpec,
            secrets: options.secrets,
            prior: intent.prior,
            plan: requiredBridgePlan(intent),
            fence: lease,
          }));
        intent = next(intent, 'bridge-deployed', { bridge });
        await lease.put(intent);
      }
      if (
        !intent.candidate &&
        intent.subphase === 'candidate-deploy-authorized'
      ) {
        const candidate = await options.provider.ensureCandidate({
          targetSpec: options.targetSpec,
          secrets: options.secrets,
          bridge: requiredBridge(intent),
          target: intent.target,
          currentRecord: lease.current(),
          fence: lease,
        });
        if (
          JSON.stringify(candidate.application) !==
          JSON.stringify(intent.targetApplication)
        ) {
          throw new Error(
            'backend switch candidate application differs from durable intent',
          );
        }
        intent = next(intent, 'candidate-deployed', { candidate });
        await lease.put(intent);
      }

      if (
        !intent.subphase.startsWith('rollback-') &&
        intent.subphase !== 'rolled-back'
      ) {
        intent = await persistBeforeMutation(
          lease,
          intent,
          'rollback-route-authorized',
        );
        await options.provider.routePlainDomainToBridge({
          prior: intent.prior,
          bridge: requiredBridge(intent),
          fence: lease,
        });
        await options.provider.assertPlainBridgeServing({
          prior: intent.prior,
          bridge: requiredBridge(intent),
          fence: lease,
        });
        intent = next(intent, 'rollback-routed');
        await lease.put(intent);
      }
      if (
        intent.subphase === 'rollback-routed' ||
        intent.subphase === 'rollback-drain-authorized'
      ) {
        await options.provider.assertPlainBridgeServing({
          prior: intent.prior,
          bridge: requiredBridge(intent),
          fence: lease,
        });
        intent = await persistBeforeMutation(
          lease,
          intent,
          'rollback-drain-authorized',
        );
        if (intent.candidate) {
          await options.provider.removeCandidateHostAndDrain({
            targetSpec: options.targetSpec,
            candidate: intent.candidate,
            fence: lease,
          });
        }
        intent = next(intent, 'rollback-drained');
        await lease.put(intent);
      }
      if (
        intent.subphase === 'rollback-drained' ||
        intent.subphase === 'rollback-restore-authorized'
      ) {
        if (
          deploymentSpecDigest(options.priorSpec) !== intent.prior.specDigest
        ) {
          throw new Error('rollback prior spec differs from durable snapshot');
        }
        intent = await persistBeforeMutation(
          lease,
          intent,
          'rollback-restore-authorized',
        );
        const restoredArtifactVersion =
          await options.provider.restorePlainDeployment({
            priorSpec: options.priorSpec,
            targetSpec: options.targetSpec,
            secrets: options.secrets,
            prior: intent.prior,
            bridge: requiredBridge(intent),
            fence: lease,
          });
        intent = next(intent, 'rollback-restored', {
          restoredArtifactVersion,
        });
        await lease.put(intent);
      }
      if (
        intent.subphase === 'rollback-restored' ||
        intent.subphase === 'rollback-ownership-authorized'
      ) {
        intent = await persistBeforeMutation(
          lease,
          intent,
          'rollback-ownership-authorized',
        );
        const ownership = await options.provider.commitPlainOwnership({
          prior: intent.prior,
          restoredArtifactVersion:
            intent.restoredArtifactVersion ??
            (() => {
              throw new Error('rollback has no restored artifact version');
            })(),
          currentRecord: lease.current(),
          fence: lease,
        });
        intent = next(intent, 'rolled-back');
        assertPlainOwnership(ownership, intent);
        await lease.putOwnership(ownership, intent);
      }
      return intent;
    },
  );
}

export async function finalizeBackendSwitch(options: {
  readonly store: FleetStateStore;
  readonly provider: BackendSwitchProvider;
  readonly targetSpec: DeploymentSpec;
  readonly now?: Date;
}): Promise<BackendSwitchIntent> {
  assertExternalTarget(options.targetSpec);
  return withBackendSwitchLease(
    options.store,
    options.targetSpec.tenantTag,
    options.targetSpec.environment,
    Date.now,
    async (lease) => {
      assertNoActiveDecommission(lease.current(), 'finalizeBackendSwitch');
      assertNoActiveCleanup(lease.current(), 'finalizeBackendSwitch');
      let intent = await lease.get();
      if (!intent?.bridge || !intent.candidate) {
        throw new Error('backend switch has no finalization snapshot');
      }
      if (
        intent.targetSpecDigest !== deploymentSpecDigest(options.targetSpec)
      ) {
        throw new Error(
          'backend switch finalization spec differs from durable intent',
        );
      }
      if (intent.subphase === 'finalized') return intent;
      if (
        intent.subphase !== 'ready' &&
        intent.subphase !== 'finalize-authorized'
      ) {
        throw new Error('backend switch is not ready to finalize');
      }
      if (
        (options.now ?? new Date()).getTime() <
        new Date(intent.rollbackUntil).getTime()
      ) {
        throw new Error('backend switch rollback window is still open');
      }
      intent = await persistBeforeMutation(
        lease,
        intent,
        'finalize-authorized',
      );
      const bridge = await options.provider.ensureStateOnlyBridge({
        targetSpec: options.targetSpec,
        bridge: requiredBridge(intent),
        target: intent.target,
        fence: lease,
      });
      if (
        bridge.scriptName !== requiredBridge(intent).scriptName ||
        bridge.databaseId !== requiredBridge(intent).databaseId ||
        JSON.stringify(bridge.namespaceIds) !==
          JSON.stringify(requiredBridge(intent).namespaceIds) ||
        !bridge.stateOnly
      ) {
        throw new Error('finalized state bridge changed physical ownership');
      }
      intent = next(intent, 'finalized', { bridge });
      const ownership = await options.provider.commitFinalizedOwnership({
        currentRecord: lease.current(),
        bridge,
        target: intent.target,
        fence: lease,
      });
      assertFinalizedOwnershipCommit(
        lease.current(),
        ownership,
        bridge,
        intent.target,
      );
      assertWorkersForPlatformsOwnership(ownership, intent);
      if (
        ownership.platformResources?.stateWorker.artifactDigest !==
        intent.target.stateArtifactDigest
      ) {
        throw new Error('provider returned incomplete finalized ownership');
      }
      await lease.putOwnership(ownership, intent);
      return intent;
    },
  );
}

function baseRelease(
  release: ExternalReleaseSnapshot | BackendSwitchCandidateSnapshot,
): ExternalReleaseSnapshot {
  return {
    physicalScriptName: release.physicalScriptName,
    specDigest: release.specDigest,
    artifactVersion: release.artifactVersion,
    releaseSchemaVersion: release.releaseSchemaVersion,
    ...(release.application ? { application: release.application } : {}),
    ...(release.topology ? { topology: release.topology } : {}),
  };
}

function authorizeDecommissionSnapshot(
  record: FleetRecord,
  intent: BackendSwitchIntent,
  authority?: Readonly<{
    desiredSpecDigest: string;
    entryPendingArtifactVersion: string | null;
    entryPendingNamespaceIds: readonly string[] | null;
    providerTargetSpecDigest: string;
  }>,
): BackendSwitchDecommissionSnapshot {
  if (
    record.tenantTag !== intent.tenantTag ||
    record.environment !== intent.environment ||
    record.databaseId !== intent.prior.databaseId ||
    record.databaseName !== intent.prior.databaseName
  ) {
    throw new Error(
      'backend switch decommission record changed deployment identity',
    );
  }
  const byName = new Map<string, ExternalReleaseSnapshot>();
  for (const release of [
    record.activeRelease,
    record.pendingRelease,
    record.migrationPriorRelease,
    record.rollbackRelease,
    record.retiringRelease,
    intent.candidate ? baseRelease(intent.candidate) : undefined,
  ]) {
    if (!release) continue;
    const normalized = baseRelease(release);
    const existing = byName.get(normalized.physicalScriptName);
    if (existing && JSON.stringify(existing) !== JSON.stringify(normalized)) {
      throw new Error(
        `backend switch decommission has conflicting snapshots for release '${normalized.physicalScriptName}'`,
      );
    }
    byName.set(normalized.physicalScriptName, normalized);
  }
  const releases = [...byName.values()]
    .sort((left, right) =>
      left.physicalScriptName < right.physicalScriptName
        ? -1
        : left.physicalScriptName > right.physicalScriptName
          ? 1
          : 0,
    )
    .map((release) => ({ release, subphase: 'present' as const }));
  const effectiveBridgePlan =
    intent.stateReconcileIntent?.plan ?? intent.bridgePlan;
  const resources = record.platformResources;
  const currentTarget = record.platformTarget ?? intent.target;
  if (resources) {
    assertPlatformResourcesMatchTarget(resources, {
      ...currentTarget,
      stateArtifactDigest: resources.stateWorker.artifactDigest,
    });
  }
  if (
    intent.subphase === 'finalized' &&
    (!intent.bridge || !resources || !effectiveBridgePlan)
  ) {
    throw new Error(
      'finalized backend switch has incomplete decommission ownership',
    );
  }
  const stateScriptName =
    resources?.stateWorker.scriptName ?? intent.bridge?.scriptName;
  const sharedExpectations = externalRouteExpectations(record);
  const routeExpectations: readonly ExternalRouteExpectation[] =
    sharedExpectations.length > 0
      ? sharedExpectations
      : intent.candidate
        ? [{ release: baseRelease(intent.candidate), target: intent.target }]
        : [];
  const routeTargets = [
    ...new Map(
      routeExpectations.map((expectation) => {
        const routeTarget = externalHostRoutingTarget(
          record,
          expectation,
          stateScriptName,
        );
        return [
          JSON.stringify(routeTarget),
          {
            release: expectation.release,
            target: expectation.target,
            routeTarget,
          },
        ] as const;
      }),
    ).values(),
  ].sort((left, right) =>
    JSON.stringify(left.routeTarget) < JSON.stringify(right.routeTarget)
      ? -1
      : JSON.stringify(left.routeTarget) > JSON.stringify(right.routeTarget)
        ? 1
        : 0,
  );
  return {
    ...(authority
      ? {
          prior: intent.prior,
          restoredArtifactVersion: intent.restoredArtifactVersion ?? null,
          entryPendingArtifactVersion: authority.entryPendingArtifactVersion,
          entryPendingNamespaceIds: authority.entryPendingNamespaceIds,
          providerTargetSpecDigest: authority.providerTargetSpecDigest,
        }
      : {}),
    routeHostname: record.routeHostname.toLowerCase(),
    routeTargets,
    desiredSpecDigest: authority?.desiredSpecDigest ?? record.desiredSpecDigest,
    target: currentTarget,
    releases,
    applicationResources: record.applicationResources ?? [],
    ...(intent.bridge ? { bridge: intent.bridge } : {}),
    ...(resources ? { resources } : {}),
    ...(effectiveBridgePlan ? { bridgePlan: effectiveBridgePlan } : {}),
  };
}

function normalizeLegacySwitchDecommissionEntry(
  record: FleetRecord,
  snapshot: BackendSwitchDecommissionSnapshot,
): FleetRecord {
  const stable = withoutConsumedSwitchEntryCarriers(record);
  return {
    ...stable,
    desiredSpecDigest: snapshot.desiredSpecDigest,
    phase: 'decommissioning',
  };
}

async function decommissionBackendSwitchLegacy(options: {
  readonly store: FleetStateStore;
  readonly provider: BackendSwitchProvider;
  readonly priorSpec: DeploymentSpec;
  readonly targetSpec: DeploymentSpec;
}): Promise<BackendSwitchIntent> {
  assertPlainPrior(options.priorSpec);
  assertExternalTarget(options.targetSpec);
  assertSameDeployment(options.priorSpec, options.targetSpec);
  return withBackendSwitchLease(
    options.store,
    options.priorSpec.tenantTag,
    options.priorSpec.environment,
    Date.now,
    async (lease) => {
      assertNoActiveDecommission(lease.current(), 'decommissionBackendSwitch');
      assertNoActiveCleanup(lease.current(), 'decommissionBackendSwitch');
      let intent = await lease.get();
      if (!intent)
        throw new Error('backend switch has no decommission snapshot');
      intent = assertBackendSwitchPriorSpecAuthority(
        lease.current(),
        options.priorSpec,
      );
      if (intent.subphase === 'decommissioned') return intent;

      if (!intent.decommissionSnapshot) {
        await assertApplicationR2EmptyBeforeDecommission({
          resources: lease.current().applicationResources ?? [],
          backend: {
            findApplicationR2Bucket: (resource) =>
              options.provider.findSwitchApplicationR2(resource),
            assertApplicationR2Empty: (resource, fence) =>
              options.provider.assertSwitchApplicationR2Empty(resource, fence),
          },
          fence: lease,
        });
        const snapshot = authorizeDecommissionSnapshot(lease.current(), intent);
        intent = next(intent, 'decommission-traffic-authorized', {
          decommissionSnapshot: snapshot,
        });
        await lease.putOwnership(
          normalizeLegacySwitchDecommissionEntry(lease.current(), snapshot),
          intent,
        );
      } else if (!intent.subphase.startsWith('decommission-')) {
        const existingSnapshot = intent.decommissionSnapshot;
        await assertApplicationR2EmptyBeforeDecommission({
          resources: existingSnapshot.applicationResources,
          backend: {
            findApplicationR2Bucket: (resource) =>
              options.provider.findSwitchApplicationR2(resource),
            assertApplicationR2Empty: (resource, fence) =>
              options.provider.assertSwitchApplicationR2Empty(resource, fence),
          },
          fence: lease,
        });
        intent = next(intent, 'decommission-traffic-authorized');
        await lease.putOwnership(
          normalizeLegacySwitchDecommissionEntry(
            lease.current(),
            existingSnapshot,
          ),
          intent,
        );
      }
      const snapshot = intent.decommissionSnapshot;
      if (!snapshot) {
        throw new Error('backend switch decommission authorization was lost');
      }
      if (intent.subphase === 'decommission-traffic-authorized') {
        await options.provider.removeSwitchTraffic({
          prior: intent.prior,
          priorSpec: options.priorSpec,
          bridge: snapshot.bridge,
          plan: snapshot.bridgePlan,
          targetSpec: options.targetSpec,
          allowedArtifactVersions: [
            intent.prior.artifactVersion,
            ...(snapshot.bridge ? [snapshot.bridge.artifactVersion] : []),
            ...(intent.restoredArtifactVersion
              ? [intent.restoredArtifactVersion]
              : []),
            ...(snapshot.resources
              ? [snapshot.resources.stateWorker.artifactVersion]
              : []),
          ],
          tenantTag: intent.tenantTag,
          environment: intent.environment,
          routeHostname: snapshot.routeHostname,
          routeTargets: snapshot.routeTargets.map(
            ({ routeTarget }) => routeTarget,
          ),
          fence: lease,
        });
        await options.provider.assertSwitchTrafficRemoved({
          prior: intent.prior,
          routeHostname: snapshot.routeHostname,
        });
        intent = next(intent, 'decommission-traffic-removed');
        await lease.put(intent);
      }
      if (
        intent.subphase === 'decommission-traffic-removed' ||
        intent.subphase === 'decommission-candidate-authorized'
      ) {
        if (intent.subphase === 'decommission-traffic-removed') {
          await options.provider.assertSwitchTrafficRemoved({
            prior: intent.prior,
            routeHostname: snapshot.routeHostname,
          });
          await assertApplicationR2EmptyBeforeDecommission({
            resources: snapshot.applicationResources,
            backend: {
              findApplicationR2Bucket: (resource) =>
                options.provider.findSwitchApplicationR2(resource),
              assertApplicationR2Empty: (resource, fence) =>
                options.provider.assertSwitchApplicationR2Empty(
                  resource,
                  fence,
                ),
            },
            fence: lease,
          });
        }
        intent = await persistBeforeMutation(
          lease,
          intent,
          'decommission-candidate-authorized',
        );
        let releaseIntent = intent;
        for (let index = 0; index < snapshot.releases.length; index += 1) {
          const progress = releaseIntent.decommissionSnapshot?.releases[index];
          if (!progress || progress.subphase === 'deleted') continue;
          if (progress.subphase === 'present') {
            const releases = [
              ...(releaseIntent.decommissionSnapshot?.releases ?? []),
            ];
            releases[index] = { ...progress, subphase: 'delete-authorized' };
            releaseIntent = next(
              releaseIntent,
              'decommission-candidate-authorized',
              {
                decommissionSnapshot: {
                  ...(releaseIntent.decommissionSnapshot as BackendSwitchDecommissionSnapshot),
                  releases,
                },
              },
            );
            intent = releaseIntent;
            await lease.put(releaseIntent);
          }
          await options.provider.removeSwitchRelease({
            prior: intent.prior,
            tenantTag: intent.tenantTag,
            environment: intent.environment,
            routeHostname: snapshot.routeHostname,
            release: progress.release,
            fence: lease,
          });
          const releases = [
            ...(releaseIntent.decommissionSnapshot?.releases ?? []),
          ];
          releases[index] = { ...progress, subphase: 'deleted' };
          releaseIntent = next(
            releaseIntent,
            'decommission-candidate-authorized',
            {
              decommissionSnapshot: {
                ...(releaseIntent.decommissionSnapshot as BackendSwitchDecommissionSnapshot),
                releases,
              },
            },
          );
          intent = releaseIntent;
          await lease.put(releaseIntent);
        }
        intent = next(intent, 'decommission-candidate-removed');
        await lease.put(intent);
      }
      if (
        intent.subphase === 'decommission-candidate-removed' ||
        intent.subphase === 'decommission-bridge-authorized'
      ) {
        intent = await persistBeforeMutation(
          lease,
          intent,
          'decommission-bridge-authorized',
        );
        await options.provider.removeSwitchBridge({
          prior: intent.prior,
          priorSpec: options.priorSpec,
          bridge: snapshot.bridge,
          plan: snapshot.bridgePlan,
          targetSpec: options.targetSpec,
          allowedArtifactVersions: [
            intent.prior.artifactVersion,
            ...(snapshot.bridge ? [snapshot.bridge.artifactVersion] : []),
            ...(intent.restoredArtifactVersion
              ? [intent.restoredArtifactVersion]
              : []),
            ...(snapshot.resources
              ? [snapshot.resources.stateWorker.artifactVersion]
              : []),
          ],
          fence: lease,
        });
        intent = next(intent, 'decommission-bridge-removed');
        await lease.put(intent);
      }
      if (
        intent.subphase === 'decommission-bridge-removed' ||
        intent.subphase === 'decommission-application-r2-authorized'
      ) {
        const applicationR2Progress =
          intent.applicationR2Progress ??
          snapshot.applicationResources.map((resource) => ({
            resource,
            subphase: resource.state,
          }));
        intent = next(intent, 'decommission-application-r2-authorized', {
          applicationR2Progress,
        });
        await lease.put(intent);
        let r2Intent: BackendSwitchIntent = intent;

        await convergeApplicationR2Deletion({
          spec: options.targetSpec,
          resources: applicationR2Progress.map(({ resource, subphase }) => ({
            ...resource,
            state: subphase,
          })),
          backend: {
            findApplicationR2Bucket: (resource) =>
              options.provider.findSwitchApplicationR2(resource),
            assertApplicationR2Detached: (resource, fence) =>
              options.provider.assertSwitchApplicationR2Detached(
                resource,
                fence,
              ),
            assertApplicationR2Empty: (resource, fence) =>
              options.provider.assertSwitchApplicationR2Empty(resource, fence),
            deleteApplicationR2Bucket: (resource, fence) =>
              options.provider.deleteSwitchApplicationR2(resource, fence),
          },
          fence: lease,
          persist: async (resources) => {
            r2Intent = next(
              r2Intent,
              'decommission-application-r2-authorized',
              {
                applicationR2Progress: resources.map((resource) => ({
                  resource,
                  subphase: resource.state,
                })),
              },
            );
            intent = r2Intent;
            await lease.put(r2Intent);
          },
        });
        intent = next(r2Intent, 'decommission-application-r2-removed');
        await lease.put(intent);
      }
      if (
        intent.subphase === 'decommission-application-r2-removed' ||
        intent.subphase === 'decommission-export-authorized'
      ) {
        intent = await persistBeforeMutation(
          lease,
          intent,
          'decommission-export-authorized',
        );
        const databaseExport = await options.provider.exportSwitchDatabase({
          prior: intent.prior,
          targetSpec: options.targetSpec,
          fence: lease,
        });
        if (databaseExport.databaseId !== intent.prior.databaseId) {
          throw new Error('backend switch export changed database identity');
        }
        intent = next(intent, 'decommission-exported', { databaseExport });
        await lease.put(intent);
      }
      if (
        intent.subphase === 'decommission-exported' ||
        intent.subphase === 'decommission-database-authorized'
      ) {
        if (!intent.databaseExport) {
          throw new Error('backend switch decommission has no durable export');
        }
        const durableExport = intent.databaseExport;
        intent = await persistBeforeMutation(
          lease,
          intent,
          'decommission-database-authorized',
        );
        await options.provider.deleteSwitchDatabase({
          prior: intent.prior,
          targetSpec: options.targetSpec,
          fence: lease,
        });
        intent = next(intent, 'decommissioned');
        const record: FleetRecord = {
          ...lease.current(),
          phase: 'decommissioned',
          databaseExportLocation: durableExport.location,
          databaseExportSha256: durableExport.sha256,
          databaseExportSize: durableExport.size,
        };
        await lease.putOwnership(record, intent);
      }
      return intent;
    },
  );
}

type ActiveBackendSwitchShell = Exclude<
  DecommissionAdvanceIntent,
  { readonly state: 'complete' }
>;

interface BackendSwitchCapabilitySet {
  readonly scan: NonNullable<
    BackendSwitchProvider['advanceSwitchDecommissionAttachmentScan']
  >;
  readonly receiptAuthority: string;
  readonly exportReceipt: NonNullable<
    BackendSwitchProvider['exportSwitchDatabaseReceipt']
  >;
  readonly getDatabase: NonNullable<BackendSwitchProvider['getSwitchDatabase']>;
  readonly readOwner: NonNullable<
    BackendSwitchProvider['readSwitchDatabaseOwner']
  >;
  readonly residuals: NonNullable<
    BackendSwitchProvider['assertSwitchDatabaseDeletionResidualsRemoved']
  >;
  readonly deleteDatabase: NonNullable<
    BackendSwitchProvider['deleteSwitchDatabaseBounded']
  >;
}

function readProviderMethod<Key extends keyof BackendSwitchProvider>(
  provider: BackendSwitchProvider,
  key: Key,
  capability: import('./decommission-advance.js').DecommissionAdvanceCapability,
): NonNullable<BackendSwitchProvider[Key]> {
  let value: BackendSwitchProvider[Key];
  try {
    value = Reflect.get(provider, key, provider) as BackendSwitchProvider[Key];
  } catch {
    throw new DecommissionAdvanceCapabilityError(capability);
  }
  if (typeof value !== 'function') {
    throw new DecommissionAdvanceCapabilityError(capability);
  }
  return value.bind(provider) as NonNullable<BackendSwitchProvider[Key]>;
}

function strictSwitchReceiptPair(
  provider: BackendSwitchProvider,
  authority: unknown,
  method: unknown,
  allowAbsent: boolean,
):
  | Readonly<{
      authority: string;
      exportReceipt: NonNullable<
        BackendSwitchProvider['exportSwitchDatabaseReceipt']
      >;
    }>
  | undefined {
  if (authority === undefined && method === undefined) {
    if (allowAbsent) return undefined;
    throw new DecommissionAdvanceCapabilityError('database-export-receipt');
  }
  if (
    typeof authority !== 'string' ||
    authority.length === 0 ||
    new TextEncoder().encode(authority).byteLength > 4_096 ||
    typeof method !== 'function'
  ) {
    throw new Error('database export receipt capability is malformed');
  }
  return {
    authority,
    exportReceipt: method.bind(provider) as NonNullable<
      BackendSwitchProvider['exportSwitchDatabaseReceipt']
    >,
  };
}

function captureBackendSwitchDecommissionCapabilities(
  provider: BackendSwitchProvider,
  observed?: Readonly<{
    scan: NonNullable<
      BackendSwitchProvider['advanceSwitchDecommissionAttachmentScan']
    >;
    receiptAuthority: string;
    exportReceipt: NonNullable<
      BackendSwitchProvider['exportSwitchDatabaseReceipt']
    >;
  }>,
): BackendSwitchCapabilitySet {
  const scan =
    observed?.scan ??
    readProviderMethod(
      provider,
      'advanceSwitchDecommissionAttachmentScan',
      'attachment-scan',
    );
  let receiptAuthority: string;
  let exportReceipt: NonNullable<
    BackendSwitchProvider['exportSwitchDatabaseReceipt']
  >;
  if (observed) {
    const pair = strictSwitchReceiptPair(
      provider,
      observed.receiptAuthority,
      observed.exportReceipt,
      false,
    ) as NonNullable<ReturnType<typeof strictSwitchReceiptPair>>;
    receiptAuthority = pair.authority;
    exportReceipt = pair.exportReceipt;
  } else {
    let rawAuthority: unknown;
    let rawExportReceipt: unknown;
    try {
      rawAuthority = Reflect.get(
        provider,
        'databaseExportReceiptAuthority',
        provider,
      );
      rawExportReceipt = Reflect.get(
        provider,
        'exportSwitchDatabaseReceipt',
        provider,
      );
    } catch {
      throw new Error('database export receipt capability is malformed');
    }
    const pair = strictSwitchReceiptPair(
      provider,
      rawAuthority,
      rawExportReceipt,
      false,
    ) as NonNullable<ReturnType<typeof strictSwitchReceiptPair>>;
    receiptAuthority = pair.authority;
    exportReceipt = pair.exportReceipt;
  }
  return {
    scan,
    receiptAuthority,
    exportReceipt,
    getDatabase: readProviderMethod(
      provider,
      'getSwitchDatabase',
      'database-read',
    ),
    readOwner: readProviderMethod(
      provider,
      'readSwitchDatabaseOwner',
      'database-read',
    ),
    residuals: readProviderMethod(
      provider,
      'assertSwitchDatabaseDeletionResidualsRemoved',
      'database-residuals',
    ),
    deleteDatabase: readProviderMethod(
      provider,
      'deleteSwitchDatabaseBounded',
      'database-delete',
    ),
  };
}

function backendSwitchToken(record: FleetRecord): DecommissionAdvanceToken {
  const shell = record.decommissionIntent;
  if (!shell) throw new DecommissionAdvanceTokenOperationError();
  return {
    version: 1,
    tenantTag: record.tenantTag,
    environment: record.environment,
    operationId: shell.operationId,
    revision: shell.revision,
  };
}

function backendSwitchAdvanceResult(
  record: FleetRecord,
): DecommissionAdvanceResult {
  const shell = record.decommissionIntent;
  if (!shell) throw new DecommissionAdvanceTokenOperationError();
  const token = backendSwitchToken(record);
  if (shell.state === 'blocked') {
    return {
      status: 'blocked',
      token,
      purpose: shell.purpose,
      attachment: shell.attachment,
    };
  }
  if (shell.state === 'complete') {
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

function effectiveBackendSwitchDigest(record: FleetRecord): string {
  const carriers = [
    record.migrationIntent?.targetSpecDigest,
    record.pendingSpecDigest,
    record.pendingRelease?.specDigest,
  ].filter((value): value is string => value !== undefined);
  if (carriers.some((value) => value !== carriers[0])) {
    throw new Error(
      'backend switch decommission specification carriers conflict',
    );
  }
  return carriers[0] ?? record.desiredSpecDigest;
}

function specForDigest(
  digest: string,
  options: Pick<
    AdvanceBackendSwitchDecommissionOptions,
    'priorSpec' | 'targetSpec' | 'currentSpec'
  >,
): DeploymentSpec {
  for (const candidate of [
    options.priorSpec,
    options.targetSpec,
    options.currentSpec,
  ]) {
    if (candidate && deploymentSpecDigest(candidate) === digest) {
      assertSameDeployment(options.priorSpec, candidate);
      return candidate;
    }
  }
  throw new Error(
    'backend switch decommission requires the exact current specification',
  );
}

function pendingInspectionFromUnknown(
  value: unknown,
  expectedArtifactVersion: string,
  spec: DeploymentSpec,
  record: FleetRecord,
): SwitchEntryPendingArtifactInspection {
  const malformed = () => {
    throw new Error('backend switch pending artifact inspection is malformed');
  };
  let plain: unknown;
  try {
    plain = cloneBoundedPlainData(value, {
      maxDepth: SWITCH_PLAIN_DATA_DEPTH_BOUND,
      maxNodes: SWITCH_PLAIN_DATA_NODE_BOUND,
      maxScalarBytes: SWITCH_PLAIN_DATA_BYTE_BOUND,
      maxSerializedBytes: SWITCH_PLAIN_DATA_BYTE_BOUND,
      error: malformed,
    });
    Reflect.apply(STRUCTURED_CLONE, undefined, [value]);
  } catch {
    return malformed();
  }
  if (!plain || typeof plain !== 'object' || Array.isArray(plain)) {
    return malformed();
  }
  const candidate = plain as Record<string, unknown>;
  const keys = Reflect.ownKeys(candidate);
  const expectedKeys = [
    'application',
    'artifactVersion',
    'databaseIds',
    'durableObjectBindings',
    'queueProducerBindings',
    'secretNames',
    'serviceBindings',
    'specDigest',
  ].sort();
  if (
    keys.some((key) => typeof key !== 'string') ||
    JSON.stringify((keys as string[]).sort()) !==
      JSON.stringify(expectedKeys) ||
    candidate.artifactVersion !== expectedArtifactVersion ||
    candidate.specDigest !== deploymentSpecDigest(spec) ||
    !Array.isArray(candidate.databaseIds) ||
    JSON.stringify(candidate.databaseIds) !==
      JSON.stringify([record.databaseId]) ||
    !Array.isArray(candidate.durableObjectBindings) ||
    !Array.isArray(candidate.secretNames) ||
    !Array.isArray(candidate.serviceBindings) ||
    !Array.isArray(candidate.queueProducerBindings)
  ) {
    return malformed();
  }
  let durableObjectBindings: readonly DurableObjectBindingInventory[];
  let application: import('./types.js').ApplicationBindingTopology;
  try {
    durableObjectBindings = durableBindings(
      candidate.durableObjectBindings,
      'pending artifact Durable Object bindings',
    );
    application = applicationBindingTopologyFromUnknown(
      candidate.application,
      'pending artifact application topology',
    );
  } catch {
    return malformed();
  }
  const namespaceIds = durableObjectBindings.map(
    ({ namespaceId }) => namespaceId,
  );
  const canonicalNamespaceIds = [...new Set(namespaceIds)].sort(
    (left, right) => (left < right ? -1 : left > right ? 1 : 0),
  );
  const services = candidate.serviceBindings.map((binding) => {
    if (!binding || typeof binding !== 'object' || Array.isArray(binding)) {
      return malformed();
    }
    const item = binding as Record<string, unknown>;
    requireExactKeys(
      item,
      ['name', 'service'],
      ['entrypoint'],
      'backend switch pending artifact inspection is malformed',
    );
    if (
      typeof item.name !== 'string' ||
      !item.name ||
      typeof item.service !== 'string' ||
      !item.service ||
      (item.entrypoint !== undefined &&
        (typeof item.entrypoint !== 'string' || !item.entrypoint))
    ) {
      return malformed();
    }
    return {
      name: item.name,
      service: item.service,
      ...(item.entrypoint === undefined
        ? {}
        : { entrypoint: item.entrypoint as string }),
    };
  });
  const queues = candidate.queueProducerBindings.map((binding) => {
    if (!binding || typeof binding !== 'object' || Array.isArray(binding)) {
      return malformed();
    }
    const item = binding as Record<string, unknown>;
    requireExactKeys(
      item,
      ['name', 'queueName'],
      [],
      'backend switch pending artifact inspection is malformed',
    );
    if (
      typeof item.name !== 'string' ||
      !item.name ||
      typeof item.queueName !== 'string' ||
      !item.queueName
    ) {
      return malformed();
    }
    return { name: item.name, queueName: item.queueName };
  });
  let secretNames: readonly string[];
  try {
    secretNames = stringArray(
      candidate.secretNames,
      'pending artifact secrets',
    );
  } catch {
    return malformed();
  }
  const byName = <T extends { readonly name: string }>(items: readonly T[]) =>
    [...items].sort((left, right) =>
      left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
    );
  const canonicalDurableObjectBindings = byName(durableObjectBindings);
  const expectedDurableObjectBindings = byName(
    spec.durableObjectBindings.map((binding) => ({ ...binding })),
  );
  const observedDurableAuthority = byName(
    durableObjectBindings.map(
      ({ name, className, scriptName, dispatchNamespace }) => ({
        name,
        className,
        ...(scriptName === undefined ? {} : { scriptName }),
        ...(dispatchNamespace === undefined ? {} : { dispatchNamespace }),
      }),
    ),
  );
  const expectedServices = spec.egressProxyService
    ? [{ name: 'EGRESS_PROXY', service: spec.egressProxyService }]
    : [];
  const expectedQueues = spec.queueProducer
    ? [
        {
          name: spec.queueProducer.binding,
          queueName: spec.queueProducer.queueName,
        },
      ]
    : [];
  const expectedApplication = applicationBindingTopology(
    spec,
    record.applicationResources ?? [],
  );
  const expectedSecrets = [
    'DEPLOYMENT_IDENTITY_SECRET',
    'MAINTENANCE_ADMIN_SECRET',
    ...expectedApplication.secrets.map(({ name }) => name),
  ].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
  if (
    record.tenantTag !== spec.tenantTag ||
    record.environment !== spec.environment ||
    record.scriptName !== spec.scriptName ||
    record.databaseName !== spec.databaseName ||
    namespaceIds.some((namespaceId) => namespaceId.length === 0) ||
    namespaceIds.length !== canonicalNamespaceIds.length ||
    !sameCanonicalData(durableObjectBindings, canonicalDurableObjectBindings) ||
    !sameCanonicalData(
      observedDurableAuthority,
      expectedDurableObjectBindings,
    ) ||
    !sameCanonicalData(services, expectedServices) ||
    !sameCanonicalData(queues, expectedQueues) ||
    !sameCanonicalData(secretNames, expectedSecrets) ||
    !sameCanonicalData(application, expectedApplication) ||
    !sameCanonicalData(record.applicationBindings, expectedApplication)
  ) {
    return malformed();
  }
  return {
    artifactVersion: expectedArtifactVersion,
    specDigest: deploymentSpecDigest(spec),
    databaseIds: [record.databaseId],
    durableObjectBindings,
    secretNames,
    serviceBindings: services,
    queueProducerBindings: queues,
    application,
  };
}

function assertBackendSwitchPriorSpecAuthority(
  record: FleetRecord,
  priorSpec: DeploymentSpec,
): BackendSwitchIntent {
  const intent = record.backendSwitchIntent;
  if (!intent) throw new Error('backend switch has no decommission snapshot');
  if (
    intent.prior.specDigest !== deploymentSpecDigest(priorSpec) ||
    intent.prior.customDomain.hostname.toLowerCase() !==
      priorSpec.routeHostname.toLowerCase() ||
    (intent.decommissionSnapshot !== undefined &&
      intent.decommissionSnapshot.routeHostname.toLowerCase() !==
        priorSpec.routeHostname.toLowerCase()) ||
    (intent.decommissionSnapshot?.prior !== undefined &&
      (intent.decommissionSnapshot.prior.specDigest !==
        deploymentSpecDigest(priorSpec) ||
        intent.decommissionSnapshot.prior.customDomain.hostname.toLowerCase() !==
          priorSpec.routeHostname.toLowerCase()))
  ) {
    throw new Error(
      'backend switch prior decommission spec differs from durable intent',
    );
  }
  return intent;
}

function assertBackendSwitchDecommissionAuthority(
  record: FleetRecord,
  options: Pick<
    AdvanceBackendSwitchDecommissionOptions,
    'priorSpec' | 'targetSpec' | 'currentSpec'
  >,
  capabilities: Pick<BackendSwitchCapabilitySet, 'receiptAuthority'>,
): void {
  const intent = assertBackendSwitchPriorSpecAuthority(
    record,
    options.priorSpec,
  );
  const shell = record.decommissionIntent;
  const snapshot = intent.decommissionSnapshot;
  const providerTargetSpecDigest =
    snapshot?.providerTargetSpecDigest ??
    intent.stateReconcileIntent?.targetSpecDigest ??
    intent.targetSpecDigest;
  if (providerTargetSpecDigest !== deploymentSpecDigest(options.targetSpec)) {
    throw new Error(
      'backend switch target decommission spec differs from durable intent',
    );
  }
  const desiredSpecDigest = snapshot
    ? snapshot.desiredSpecDigest
    : effectiveBackendSwitchDigest(record);
  if (options.currentSpec) {
    if (desiredSpecDigest !== deploymentSpecDigest(options.currentSpec)) {
      throw new Error(
        'backend switch current decommission spec differs from durable intent',
      );
    }
  } else {
    specForDigest(desiredSpecDigest, options);
  }
  if (shell && shell.state !== 'complete') {
    databaseExportReceiptIdentity(
      record,
      shell.operationId,
      shell.databaseExportReceiptAuthority ?? capabilities.receiptAuthority,
      capabilities.receiptAuthority,
    );
  }
}

function completeSnapshotGroup(
  snapshot: BackendSwitchDecommissionSnapshot | undefined,
): snapshot is BackendSwitchDecommissionSnapshot &
  Required<
    Pick<
      BackendSwitchDecommissionSnapshot,
      | 'prior'
      | 'restoredArtifactVersion'
      | 'entryPendingArtifactVersion'
      | 'entryPendingNamespaceIds'
      | 'providerTargetSpecDigest'
    >
  > {
  return Boolean(
    snapshot?.prior &&
      snapshot.restoredArtifactVersion !== undefined &&
      snapshot.entryPendingArtifactVersion !== undefined &&
      snapshot.entryPendingNamespaceIds !== undefined &&
      snapshot.providerTargetSpecDigest,
  );
}

function nextBackendSwitchShell(
  record: FleetRecord,
  switchIntent: BackendSwitchIntent,
  transition: DecommissionIntentTransition,
): ActiveBackendSwitchShell {
  const shell = record.decommissionIntent;
  if (!shell || shell.state === 'complete') {
    throw new Error('backend switch decommission shell is not active');
  }
  const receiptAuthority =
    shell.databaseExportReceiptAuthority ??
    transition.databaseExportReceiptAuthority;
  const common = {
    version: 1 as const,
    operationId: shell.operationId,
    revision: shell.revision + 1,
    generation: transition.generation ?? shell.generation,
    updatedAt: record.updatedAt,
    identity: shell.identity,
    ...(receiptAuthority
      ? { databaseExportReceiptAuthority: receiptAuthority }
      : {}),
    lifecyclePhase:
      transition.lifecyclePhase ??
      (backendSwitchDecommissionLifecyclePhase(
        switchIntent.subphase,
        shell.lifecyclePhase,
      ) as import('./types.js').NormalDecommissionLifecyclePhase),
  };
  switch (transition.state) {
    case 'transitioning':
      return { ...common, state: 'transitioning' };
    case 'discover':
      return {
        ...common,
        state: 'discover',
        purpose: transition.purpose,
        progress: transition.progress,
      };
    case 'verify':
      return {
        ...common,
        state: 'verify',
        purpose: transition.purpose,
        progress: transition.progress,
        discoverEvidence: transition.discoverEvidence,
      };
    case 'blocked':
      return {
        ...common,
        state: 'blocked',
        purpose: transition.purpose,
        attachment: transition.attachment,
      };
  }
}

async function putBackendSwitchOwnership(
  lease: BackendSwitchLease,
  switchIntent: BackendSwitchIntent,
  shell: DecommissionAdvanceIntent,
  patch: Partial<FleetRecord> = {},
): Promise<FleetRecord> {
  const current = lease.current();
  const applicationResources = (
    switchIntent.applicationR2Progress ??
    switchIntent.decommissionSnapshot?.applicationResources.map((resource) => ({
      resource,
      subphase: resource.state,
    })) ??
    []
  ).map(({ resource, subphase }) => ({ ...resource, state: subphase }));
  const nextRecord: FleetRecord = {
    ...current,
    ...patch,
    backendSwitchIntent: switchIntent,
    decommissionIntent: shell,
    applicationResources,
    phase:
      shell.state === 'complete' ? 'decommissioned' : 'decommission-advancing',
    updatedAt: current.updatedAt,
  };
  await lease.putOwnership(nextRecord, switchIntent);
  return lease.current();
}

function allowedSwitchArtifactVersions(
  snapshot: BackendSwitchDecommissionSnapshot,
): readonly string[] {
  return [
    snapshot.prior?.artifactVersion,
    snapshot.bridge?.artifactVersion,
    snapshot.restoredArtifactVersion ?? undefined,
    snapshot.resources?.stateWorker.artifactVersion,
  ].filter((value): value is string => Boolean(value));
}

function entryPendingArtifact(
  snapshot: BackendSwitchDecommissionSnapshot,
  options: AdvanceBackendSwitchDecommissionOptions,
):
  | Readonly<{
      artifactVersion: string;
      namespaceIds: readonly string[];
      spec: DeploymentSpec;
    }>
  | undefined {
  if (
    snapshot.entryPendingArtifactVersion === null ||
    snapshot.entryPendingNamespaceIds === null
  ) {
    return undefined;
  }
  if (
    snapshot.entryPendingArtifactVersion === undefined ||
    snapshot.entryPendingNamespaceIds === undefined
  ) {
    throw new Error(
      'backend switch decommission snapshot lacks entry authority',
    );
  }
  return {
    artifactVersion: snapshot.entryPendingArtifactVersion,
    namespaceIds: snapshot.entryPendingNamespaceIds,
    spec: specForDigest(snapshot.desiredSpecDigest, options),
  };
}

function purposeTargetForSwitch(
  purpose: import('./types.js').DecommissionAttachmentPurpose,
) {
  return purpose.kind === 'application-r2-detach'
    ? ({ kind: 'r2', bucketName: purpose.bucketName } as const)
    : ({ kind: 'd1', databaseId: purpose.databaseId } as const);
}

function databasePreDeletePurposeForSwitch(record: FleetRecord) {
  if (
    !record.databaseExportLocation ||
    !record.databaseExportSha256 ||
    !record.databaseExportSize
  ) {
    throw new Error('backend switch decommission has no durable export');
  }
  return {
    kind: 'database-pre-delete' as const,
    databaseId: record.databaseId,
    exportLocation: record.databaseExportLocation,
    exportSha256: record.databaseExportSha256,
    exportSize: record.databaseExportSize,
  };
}

async function reconcileSwitchDatabase(
  capabilities: BackendSwitchCapabilitySet,
  record: FleetRecord,
  lease: BackendSwitchLease,
  allowAbsent: boolean,
): Promise<(DatabaseReference & { readonly created: false }) | undefined> {
  return reconcilePersistedDatabaseFromCallbacks({
    getDatabase: capabilities.getDatabase,
    readOwner: capabilities.readOwner,
    record,
    allowAbsent,
    requireOwner: true,
    fence: lease,
  });
}

async function consumeSwitchVerify(
  options: AdvanceBackendSwitchDecommissionOptions,
  lease: BackendSwitchLease,
  capabilities: BackendSwitchCapabilitySet,
  verified: Extract<DecommissionAdvanceIntent, { readonly state: 'verify' }>,
): Promise<FleetRecord> {
  const record = lease.current();
  const switchIntent = record.backendSwitchIntent as BackendSwitchIntent;
  if (verified.purpose.kind === 'application-r2-detach') {
    await lease.assertOwned();
    const advanced = await advanceApplicationR2Deletion({
      spec: specForDigest(
        switchIntent.decommissionSnapshot?.desiredSpecDigest ??
          record.desiredSpecDigest,
        options,
      ),
      resources: record.applicationResources ?? [],
      backend: {
        findApplicationR2Bucket: (resource) =>
          options.provider.findSwitchApplicationR2(resource),
      },
      fence: lease,
      startResourceIndex: 0,
      verifiedDetachmentResourceIndex: verified.purpose.resourceIndex,
    });
    if (
      advanced.status !== 'resource-advanced' ||
      advanced.resourceIndex !== verified.purpose.resourceIndex ||
      advanced.resources[advanced.resourceIndex]?.state !== 'detached'
    ) {
      throw new Error('bounded decommission attachment result is malformed');
    }
    const nextSwitch = next(
      switchIntent,
      'decommission-application-r2-authorized',
      {
        applicationR2Progress: advanced.resources.map((resource) => ({
          resource,
          subphase: resource.state,
        })),
      },
    );
    return putBackendSwitchOwnership(
      lease,
      nextSwitch,
      nextBackendSwitchShell(record, nextSwitch, { state: 'transitioning' }),
    );
  }
  const database = await reconcileSwitchDatabase(
    capabilities,
    record,
    lease,
    verified.lifecyclePhase === 'database-deleting',
  );
  if (!database) {
    return completeBackendSwitchDecommission(lease, record, switchIntent);
  }
  await capabilities.residuals({
    prior: switchIntent.decommissionSnapshot?.prior ?? switchIntent.prior,
    targetSpec: options.targetSpec,
    currentRecord: record,
    database,
    fence: lease,
  });
  if (verified.purpose.kind === 'database-pre-export') {
    const identity = databaseExportReceiptIdentity(
      record,
      verified.operationId,
      verified.databaseExportReceiptAuthority as string,
      capabilities.receiptAuthority,
    );
    await lease.assertOwned();
    const exported = databaseExportFromUnknown(
      await capabilities.exportReceipt(identity, {
        prior: switchIntent.decommissionSnapshot?.prior ?? switchIntent.prior,
        targetSpec: options.targetSpec,
        fence: lease,
      }),
      record.databaseId,
    );
    const nextSwitch = next(switchIntent, 'decommission-exported', {
      databaseExport: exported,
    });
    return putBackendSwitchOwnership(
      lease,
      nextSwitch,
      nextBackendSwitchShell(record, nextSwitch, {
        state: 'transitioning',
        lifecyclePhase: 'database-exported',
      }),
      {
        databaseExportLocation: exported.location,
        databaseExportSha256: exported.sha256,
        databaseExportSize: exported.size,
      },
    );
  }
  if (verified.purpose.kind !== 'database-pre-delete') {
    throw new Error('bounded decommission attachment result is malformed');
  }
  const nextSwitch = next(switchIntent, 'decommission-database-authorized');
  const barrier = await putBackendSwitchOwnership(
    lease,
    nextSwitch,
    nextBackendSwitchShell(record, nextSwitch, {
      state: 'transitioning',
      lifecyclePhase: 'database-deleting',
    }),
  );
  await settleDatabaseDeletionUnderBarrier({
    lease,
    databaseId: record.databaseId,
    barrier,
    deleteDatabase: () =>
      capabilities.deleteDatabase({
        prior: switchIntent.decommissionSnapshot?.prior ?? switchIntent.prior,
        targetSpec: options.targetSpec,
        database,
        fence: lease,
      }),
    readDatabase: () => capabilities.getDatabase(record.databaseId),
  });
  return lease.current();
}

async function completeBackendSwitchDecommission(
  lease: BackendSwitchLease,
  record: FleetRecord,
  switchIntent: BackendSwitchIntent,
): Promise<FleetRecord> {
  const shell = record.decommissionIntent;
  if (
    !shell ||
    shell.state === 'complete' ||
    !switchIntent.databaseExport ||
    (record.applicationResources ?? []).some(
      (resource) => resource.state !== 'deleted',
    )
  ) {
    throw new Error(
      'backend switch decommission terminal authority is malformed',
    );
  }
  const completeShell: DecommissionAdvanceIntent = {
    version: 1,
    operationId: shell.operationId,
    revision: shell.revision + 1,
    generation: shell.generation,
    updatedAt: record.updatedAt,
    identity: shell.identity,
    databaseExportReceiptAuthority:
      shell.databaseExportReceiptAuthority as string,
    lifecyclePhase: 'decommissioned',
    state: 'complete',
  };
  return putBackendSwitchOwnership(
    lease,
    next(switchIntent, 'decommissioned'),
    completeShell,
    {
      phase: 'decommissioned',
      databaseExportLocation: switchIntent.databaseExport.location,
      databaseExportSha256: switchIntent.databaseExport.sha256,
      databaseExportSize: switchIntent.databaseExport.size,
    },
  );
}

async function startBoundedSwitchShell(
  options: AdvanceBackendSwitchDecommissionOptions,
  lease: BackendSwitchLease,
  capabilities: BackendSwitchCapabilitySet,
): Promise<FleetRecord> {
  const record = lease.current();
  let intent = assertBackendSwitchPriorSpecAuthority(record, options.priorSpec);
  if (
    intent.subphase === 'candidate-deploy-authorized' ||
    intent.subphase === 'rollback-restore-authorized' ||
    intent.subphase === 'finalize-authorized'
  ) {
    throw new Error(
      `backend switch decommission must settle '${intent.subphase}' before teardown`,
    );
  }
  if (intent.stateReconcileIntent?.subphase === 'upload-authorized') {
    throw new Error(
      "backend switch decommission must settle 'upload-authorized' before teardown",
    );
  }
  if (
    BACKEND_SWITCH_SUBPHASES.indexOf(intent.subphase) >=
    BACKEND_SWITCH_SUBPHASES.indexOf('decommission-export-authorized')
  ) {
    throw new Error(
      'bounded backend-switch decommission cannot adopt shell-less legacy D1 authorization',
    );
  }
  if (
    intent.decommissionSnapshot &&
    !completeSnapshotGroup(intent.decommissionSnapshot) &&
    record.backend === 'plain-worker' &&
    !record.pendingArtifactVersion
  ) {
    throw new Error(
      'bounded backend-switch decommission cannot adopt compatibility-ambiguous shell-less plain authority',
    );
  }
  if (intent.subphase === 'bridge-upload-authorized') {
    const recovered = await options.provider.recoverBridge({
      priorSpec: options.priorSpec,
      targetSpec: options.targetSpec,
      prior: intent.prior,
      plan: requiredBridgePlan(intent),
      fence: lease,
    });
    if (!recovered) {
      throw new Error(
        "backend switch decommission must settle 'bridge-upload-authorized' before teardown",
      );
    }
    intent = next(intent, 'bridge-deployed', {
      bridge: bridgeSnapshot(recovered),
    });
  }
  const desiredSpecDigest = effectiveBackendSwitchDigest(record);
  const currentSpec = specForDigest(desiredSpecDigest, options);
  let entryPendingArtifactVersion: string | null = null;
  let entryPendingNamespaceIds: readonly string[] | null = null;
  if (record.pendingArtifactVersion) {
    let capture: BackendSwitchProvider['captureSwitchEntryPendingArtifact'];
    try {
      capture = Reflect.get(
        options.provider,
        'captureSwitchEntryPendingArtifact',
        options.provider,
      ) as BackendSwitchProvider['captureSwitchEntryPendingArtifact'];
    } catch {
      throw new DecommissionAdvanceCapabilityError(
        'pending-artifact-inspection',
      );
    }
    if (typeof capture !== 'function') {
      throw new DecommissionAdvanceCapabilityError(
        'pending-artifact-inspection',
      );
    }
    const inspection = pendingInspectionFromUnknown(
      await capture.call(options.provider, {
        expectedArtifactVersion: record.pendingArtifactVersion,
        spec: currentSpec,
        currentRecord: record,
        fence: lease,
      }),
      record.pendingArtifactVersion,
      currentSpec,
      record,
    );
    entryPendingArtifactVersion = record.pendingArtifactVersion;
    entryPendingNamespaceIds = inspection.durableObjectBindings
      .map(({ namespaceId }) => namespaceId)
      .sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
  }
  const providerTargetSpecDigest =
    intent.stateReconcileIntent?.targetSpecDigest ?? intent.targetSpecDigest;
  const proposedSnapshot = intent.decommissionSnapshot
    ? {
        ...intent.decommissionSnapshot,
        prior: intent.prior,
        restoredArtifactVersion: intent.restoredArtifactVersion ?? null,
        entryPendingArtifactVersion,
        entryPendingNamespaceIds,
        providerTargetSpecDigest,
        desiredSpecDigest,
      }
    : authorizeDecommissionSnapshot(record, intent, {
        desiredSpecDigest,
        entryPendingArtifactVersion,
        entryPendingNamespaceIds,
        providerTargetSpecDigest,
      });
  const {
    decommissionSnapshotSha256: _priorSnapshotSha256,
    decommissionEntrySubphase: _priorEntrySubphase,
    ...unmarkedIntent
  } = intent;
  const canonicalSnapshot = backendSwitchIntentFromUnknown({
    ...unmarkedIntent,
    decommissionSnapshot: proposedSnapshot,
  }).decommissionSnapshot;
  if (!canonicalSnapshot) {
    throw new Error('backend switch decommission authorization was lost');
  }
  const snapshot = canonicalSnapshot;
  const entrySubphase = intent.subphase;
  const snapshotSha256 = backendSwitchDecommissionSnapshotDigest(snapshot);
  const startSubphase = intent.decommissionSnapshot
    ? intent.subphase
    : 'decommission-traffic-authorized';
  intent = next(intent, startSubphase, {
    decommissionSnapshot: snapshot,
    decommissionSnapshotSha256: snapshotSha256,
    decommissionEntrySubphase: entrySubphase,
  });
  const operationId = options.randomUUID();
  parseDecommissionAdvanceToken({
    version: 1,
    tenantTag: record.tenantTag,
    environment: record.environment,
    operationId,
    revision: 0,
  });
  databaseExportReceiptIdentity(
    record,
    operationId,
    capabilities.receiptAuthority,
    capabilities.receiptAuthority,
  );
  const shell = backendSwitchDecommissionShell({
    record,
    intent,
    operationId,
    snapshotSha256,
    entrySubphase,
    now: record.updatedAt,
  });
  await lease.putOwnership(
    normalizeSwitchDecommissionEntry(record, intent, shell),
    intent,
  );
  return lease.current();
}

async function advanceBoundedSwitchCurrent(
  options: AdvanceBackendSwitchDecommissionOptions,
  lease: BackendSwitchLease,
  capabilities: BackendSwitchCapabilitySet,
): Promise<FleetRecord> {
  const record = lease.current();
  const shell = record.decommissionIntent;
  const intent = record.backendSwitchIntent;
  if (!shell || shell.state === 'complete' || !intent) {
    return record;
  }
  const snapshot = intent.decommissionSnapshot;
  if (!snapshot || !completeSnapshotGroup(snapshot)) {
    throw new Error(
      'backend switch decommission snapshot lacks bounded authority',
    );
  }
  if (shell.state === 'discover' || shell.state === 'verify') {
    return advanceDecommissionAttachmentScanStep({
      intent: shell,
      scan: capabilities.scan,
      maxProviderRequests: options.maxProviderRequests,
      signal: options.signal,
      persist: (transition) =>
        putBackendSwitchOwnership(
          lease,
          intent,
          nextBackendSwitchShell(record, intent, transition),
        ),
      consumeMatchingVerify: ({ intent: verified }) =>
        consumeSwitchVerify(options, lease, capabilities, verified),
    });
  }
  if (intent.subphase === 'decommission-traffic-authorized') {
    const pendingArtifact = entryPendingArtifact(snapshot, options);
    await options.provider.removeSwitchTraffic({
      prior: snapshot.prior,
      priorSpec: options.priorSpec,
      bridge: snapshot.bridge,
      plan: snapshot.bridgePlan,
      targetSpec: options.targetSpec,
      allowedArtifactVersions: allowedSwitchArtifactVersions(snapshot),
      tenantTag: intent.tenantTag,
      environment: intent.environment,
      routeHostname: snapshot.routeHostname,
      routeTargets: snapshot.routeTargets.map(({ routeTarget }) => routeTarget),
      ...(pendingArtifact ? { entryPendingArtifact: pendingArtifact } : {}),
      fence: lease,
    });
    await options.provider.assertSwitchTrafficRemoved({
      prior: snapshot.prior,
      routeHostname: snapshot.routeHostname,
    });
    const nextSwitch = next(intent, 'decommission-traffic-removed');
    return putBackendSwitchOwnership(
      lease,
      nextSwitch,
      nextBackendSwitchShell(record, nextSwitch, { state: 'transitioning' }),
    );
  }
  if (
    intent.subphase === 'decommission-traffic-removed' ||
    intent.subphase === 'decommission-candidate-authorized'
  ) {
    const index = snapshot.releases.findIndex(
      ({ subphase }) => subphase !== 'deleted',
    );
    if (index >= 0) {
      const progress = snapshot.releases[
        index
      ] as BackendSwitchDecommissionRelease;
      let authorizedIntent = intent;
      if (progress.subphase === 'present') {
        const releases = [...snapshot.releases];
        releases[index] = { ...progress, subphase: 'delete-authorized' };
        authorizedIntent = next(intent, 'decommission-candidate-authorized', {
          decommissionSnapshot: { ...snapshot, releases },
        });
        await putBackendSwitchOwnership(
          lease,
          authorizedIntent,
          nextBackendSwitchShell(record, authorizedIntent, {
            state: 'transitioning',
          }),
        );
      }
      await options.provider.removeSwitchRelease({
        prior: snapshot.prior,
        tenantTag: intent.tenantTag,
        environment: intent.environment,
        routeHostname: snapshot.routeHostname,
        release: progress.release,
        fence: lease,
      });
      const current = lease.current();
      const currentSnapshot =
        authorizedIntent.decommissionSnapshot as BackendSwitchDecommissionSnapshot;
      const releases = [...currentSnapshot.releases];
      releases[index] = { ...progress, subphase: 'deleted' };
      const deletedIntent = next(
        authorizedIntent,
        'decommission-candidate-authorized',
        { decommissionSnapshot: { ...currentSnapshot, releases } },
      );
      return putBackendSwitchOwnership(
        lease,
        deletedIntent,
        nextBackendSwitchShell(current, deletedIntent, {
          state: 'transitioning',
        }),
      );
    }
    const nextSwitch = next(intent, 'decommission-candidate-removed');
    return putBackendSwitchOwnership(
      lease,
      nextSwitch,
      nextBackendSwitchShell(record, nextSwitch, { state: 'transitioning' }),
    );
  }
  if (
    intent.subphase === 'decommission-candidate-removed' ||
    intent.subphase === 'decommission-bridge-authorized'
  ) {
    let authorized = intent;
    if (intent.subphase === 'decommission-candidate-removed') {
      authorized = next(intent, 'decommission-bridge-authorized');
      await putBackendSwitchOwnership(
        lease,
        authorized,
        nextBackendSwitchShell(record, authorized, { state: 'transitioning' }),
      );
    }
    const pendingArtifact = entryPendingArtifact(snapshot, options);
    await options.provider.removeSwitchBridge({
      prior: snapshot.prior,
      priorSpec: options.priorSpec,
      bridge: snapshot.bridge,
      plan: snapshot.bridgePlan,
      targetSpec: options.targetSpec,
      allowedArtifactVersions: allowedSwitchArtifactVersions(snapshot),
      ...(pendingArtifact ? { entryPendingArtifact: pendingArtifact } : {}),
      fence: lease,
    });
    const current = lease.current();
    const nextSwitch = next(authorized, 'decommission-bridge-removed');
    return putBackendSwitchOwnership(
      lease,
      nextSwitch,
      nextBackendSwitchShell(current, nextSwitch, { state: 'transitioning' }),
    );
  }
  if (intent.subphase === 'decommission-bridge-removed') {
    const progress = snapshot.applicationResources.map((resource) => ({
      resource,
      subphase: resource.state,
    }));
    const nextSwitch = next(intent, 'decommission-application-r2-authorized', {
      applicationR2Progress: progress,
    });
    return putBackendSwitchOwnership(
      lease,
      nextSwitch,
      nextBackendSwitchShell(record, nextSwitch, {
        state: 'transitioning',
        lifecyclePhase: 'application-resources-deleting',
      }),
    );
  }
  if (intent.subphase === 'decommission-application-r2-authorized') {
    const resources = record.applicationResources ?? [];
    const advanced = await advanceApplicationR2Deletion({
      spec: specForDigest(snapshot.desiredSpecDigest, options),
      resources,
      backend: {
        findApplicationR2Bucket: (resource) =>
          options.provider.findSwitchApplicationR2(resource),
        assertApplicationR2Empty: (resource, fence) =>
          options.provider.assertSwitchApplicationR2Empty(resource, fence),
        deleteApplicationR2Bucket: (resource, fence) =>
          options.provider.deleteSwitchApplicationR2(resource, fence),
      },
      fence: lease,
      startResourceIndex: 0,
    });
    if (advanced.status === 'detachment-required') {
      const resource = advanced.resources[advanced.resourceIndex];
      if (!resource?.creationDate) {
        throw new Error('backend switch application R2 authority is malformed');
      }
      const nextSwitch = next(
        intent,
        'decommission-application-r2-authorized',
        {
          applicationR2Progress: advanced.resources.map((entry) => ({
            resource: entry,
            subphase: entry.state,
          })),
        },
      );
      return putBackendSwitchOwnership(
        lease,
        nextSwitch,
        nextBackendSwitchShell(record, nextSwitch, {
          state: 'discover',
          purpose: {
            kind: 'application-r2-detach',
            resourceIndex: advanced.resourceIndex,
            name: resource.name,
            bucketName: resource.bucketName,
            jurisdiction: resource.jurisdiction,
            reservationNonce: resource.reservationNonce,
            creationDate: resource.creationDate,
          },
          progress: initialWorkerAttachmentScan({
            kind: 'r2',
            bucketName: resource.bucketName,
          }),
          generation: shell.generation + 1,
        }),
      );
    }
    if (advanced.status === 'complete') {
      const nextSwitch = next(intent, 'decommission-application-r2-removed', {
        applicationR2Progress: resources.map((resource) => ({
          resource,
          subphase: resource.state,
        })),
      });
      return putBackendSwitchOwnership(
        lease,
        nextSwitch,
        nextBackendSwitchShell(record, nextSwitch, {
          state: 'transitioning',
          lifecyclePhase: 'application-resources-deleted',
        }),
      );
    }
    const nextSwitch = next(intent, 'decommission-application-r2-authorized', {
      applicationR2Progress: advanced.resources.map((resource) => ({
        resource,
        subphase: resource.state,
      })),
    });
    return putBackendSwitchOwnership(
      lease,
      nextSwitch,
      nextBackendSwitchShell(record, nextSwitch, { state: 'transitioning' }),
    );
  }
  if (intent.subphase === 'decommission-application-r2-removed') {
    const nextSwitch = next(intent, 'decommission-export-authorized');
    return putBackendSwitchOwnership(
      lease,
      nextSwitch,
      nextBackendSwitchShell(record, nextSwitch, {
        state: 'discover',
        lifecyclePhase: 'application-resources-deleted',
        databaseExportReceiptAuthority: capabilities.receiptAuthority,
        purpose: { kind: 'database-pre-export', databaseId: record.databaseId },
        progress: initialWorkerAttachmentScan({
          kind: 'd1',
          databaseId: record.databaseId,
        }),
        generation: shell.generation + 1,
      }),
    );
  }
  if (intent.subphase === 'decommission-exported') {
    return putBackendSwitchOwnership(
      lease,
      intent,
      nextBackendSwitchShell(record, intent, {
        state: 'discover',
        lifecyclePhase: 'database-exported',
        purpose: databasePreDeletePurposeForSwitch(record),
        progress: initialWorkerAttachmentScan({
          kind: 'd1',
          databaseId: record.databaseId,
        }),
        generation: shell.generation + 1,
      }),
    );
  }
  if (intent.subphase === 'decommission-database-authorized') {
    const database = await reconcileSwitchDatabase(
      capabilities,
      record,
      lease,
      true,
    );
    if (!database) {
      return completeBackendSwitchDecommission(lease, record, intent);
    }
    return putBackendSwitchOwnership(
      lease,
      intent,
      nextBackendSwitchShell(record, intent, {
        state: 'discover',
        lifecyclePhase: 'database-deleting',
        purpose: databasePreDeletePurposeForSwitch(record),
        progress: initialWorkerAttachmentScan({
          kind: 'd1',
          databaseId: record.databaseId,
        }),
        generation: shell.generation + 1,
      }),
    );
  }
  throw new Error(
    `unsupported bounded backend-switch decommission subphase '${intent.subphase}'`,
  );
}

/** Inputs for one root-only bounded backend-switch teardown action. */
export interface AdvanceBackendSwitchDecommissionOptions {
  readonly store: FleetStateStore;
  readonly provider: BackendSwitchProvider;
  readonly priorSpec: DeploymentSpec;
  readonly targetSpec: DeploymentSpec;
  readonly currentSpec?: DeploymentSpec;
  readonly action: DecommissionAdvanceAction;
  readonly maxProviderRequests: number;
  readonly signal?: AbortSignal;
  readonly clock?: () => number;
  readonly randomUUID: () => string;
}

async function advanceBackendSwitchDecommissionInternal(
  options: AdvanceBackendSwitchDecommissionOptions,
  precaptured?: BackendSwitchCapabilitySet,
): Promise<DecommissionAdvanceResult> {
  validateDeploymentSpec(options.priorSpec);
  validateDeploymentSpec(options.targetSpec);
  if (options.currentSpec) validateDeploymentSpec(options.currentSpec);
  assertPlainPrior(options.priorSpec);
  assertExternalTarget(options.targetSpec);
  assertSameDeployment(options.priorSpec, options.targetSpec);
  assertWorkerAttachmentProviderRequestBudget(options.maxProviderRequests);
  const action = decommissionAdvanceActionFromUnknown(options.action);
  if (typeof options.randomUUID !== 'function') {
    throw new Error(
      'advanceBackendSwitchDecommission requires a randomUUID function',
    );
  }
  const token =
    action.kind === 'start'
      ? undefined
      : parseDecommissionAdvanceToken(action.token);
  if (
    token &&
    (token.tenantTag !== options.priorSpec.tenantTag ||
      token.environment !== options.priorSpec.environment)
  ) {
    throw new DecommissionAdvanceTokenDeploymentError();
  }
  return withBackendSwitchLease(
    options.store,
    options.priorSpec.tenantTag,
    options.priorSpec.environment,
    options.clock ?? Date.now,
    async (lease) => {
      let record = lease.current();
      assertBackendSwitchPriorSpecAuthority(record, options.priorSpec);
      const existing = record.decommissionIntent;
      if (action.kind === 'start' && existing) {
        if (existing.state !== 'complete') {
          const capabilities =
            precaptured ??
            captureBackendSwitchDecommissionCapabilities(options.provider);
          assertBackendSwitchDecommissionAuthority(
            record,
            options,
            capabilities,
          );
        }
        return backendSwitchAdvanceResult(record);
      }
      if (action.kind !== 'start') {
        if (!token || !existing) {
          throw new DecommissionAdvanceTokenOperationError();
        }
        const classification = classifyDecommissionAdvanceToken(token, record);
        if (classification === 'stale')
          return backendSwitchAdvanceResult(record);
        if (action.kind === 'restart-blocked') {
          if (existing.state !== 'blocked') {
            throw new DecommissionAdvanceRestartError();
          }
          const capabilities =
            precaptured ??
            captureBackendSwitchDecommissionCapabilities(options.provider);
          assertBackendSwitchDecommissionAuthority(
            record,
            options,
            capabilities,
          );
          const switchIntent =
            record.backendSwitchIntent as BackendSwitchIntent;
          record = await putBackendSwitchOwnership(
            lease,
            switchIntent,
            nextBackendSwitchShell(record, switchIntent, {
              state: 'discover',
              purpose: existing.purpose,
              progress: initialWorkerAttachmentScan(
                purposeTargetForSwitch(existing.purpose),
              ),
              generation: existing.generation + 1,
            }),
          );
          return backendSwitchAdvanceResult(record);
        }
        if (existing.state === 'blocked' || existing.state === 'complete') {
          return backendSwitchAdvanceResult(record);
        }
      }
      if (!existing) {
        const switchIntent = record.backendSwitchIntent;
        if (
          switchIntent &&
          BACKEND_SWITCH_SUBPHASES.indexOf(switchIntent.subphase) >=
            BACKEND_SWITCH_SUBPHASES.indexOf('decommission-export-authorized')
        ) {
          throw new Error(
            'bounded backend-switch decommission cannot adopt shell-less legacy D1 authorization',
          );
        }
      }
      const capabilities =
        precaptured ??
        captureBackendSwitchDecommissionCapabilities(options.provider);
      assertBackendSwitchDecommissionAuthority(record, options, capabilities);
      if (!existing) {
        record = await startBoundedSwitchShell(options, lease, capabilities);
      } else {
        record = await advanceBoundedSwitchCurrent(
          options,
          lease,
          capabilities,
        );
      }
      return backendSwitchAdvanceResult(record);
    },
  );
}

/**
 * Starts, reads, or advances one root-only backend-switch teardown operation.
 * One call performs at most one bounded scan or lifecycle/resource action group.
 */
export function advanceBackendSwitchDecommission(
  options: AdvanceBackendSwitchDecommissionOptions,
): Promise<DecommissionAdvanceResult> {
  return advanceBackendSwitchDecommissionInternal(options);
}

function shouldUseBoundedCompatibility(
  provider: BackendSwitchProvider,
): BackendSwitchCapabilitySet | undefined {
  let scan: unknown;
  try {
    scan = Reflect.get(
      provider,
      'advanceSwitchDecommissionAttachmentScan',
      provider,
    );
  } catch {
    throw new DecommissionAdvanceCapabilityError('attachment-scan');
  }
  if (typeof scan !== 'function') return undefined;
  let authority: unknown;
  let receipt: unknown;
  try {
    authority = Reflect.get(
      provider,
      'databaseExportReceiptAuthority',
      provider,
    );
    receipt = Reflect.get(provider, 'exportSwitchDatabaseReceipt', provider);
  } catch {
    throw new Error('database export receipt capability is malformed');
  }
  const pair = strictSwitchReceiptPair(provider, authority, receipt, true);
  if (!pair) return undefined;
  return captureBackendSwitchDecommissionCapabilities(provider, {
    scan: scan.bind(provider) as NonNullable<
      BackendSwitchProvider['advanceSwitchDecommissionAttachmentScan']
    >,
    receiptAuthority: pair.authority,
    exportReceipt: pair.exportReceipt,
  });
}

/** Asynchronous one-call compatibility drain for backend-switch teardown. */
export async function decommissionBackendSwitch(options: {
  readonly store: FleetStateStore;
  readonly provider: BackendSwitchProvider;
  readonly priorSpec: DeploymentSpec;
  readonly targetSpec: DeploymentSpec;
  readonly currentSpec?: DeploymentSpec;
}): Promise<BackendSwitchIntent> {
  const rawCurrent = await options.store.get(
    options.priorSpec.tenantTag,
    options.priorSpec.environment,
  );
  const structuralCurrent = rawCurrent
    ? structuralBackendSwitchFleetRecordFromUnknown(rawCurrent)
    : undefined;
  const current = structuralCurrent?.carriesBackendSwitchAuthority
    ? backendSwitchFleetRecordFromUnknown(structuralCurrent.record).record
    : structuralCurrent?.record;
  if (current) {
    assertNoActiveCleanup(current, 'decommissionBackendSwitch');
  }
  if (!current?.backendSwitchIntent) {
    if (current) {
      assertNoActiveDecommission(current, 'decommissionBackendSwitch');
    }
    throw new Error('backend switch has no decommission snapshot');
  }
  if (current.decommissionIntent?.state === 'complete') {
    return assertBackendSwitchPriorSpecAuthority(current, options.priorSpec);
  }
  if (
    !current.decommissionIntent &&
    (BACKEND_SWITCH_SUBPHASES.indexOf(current.backendSwitchIntent.subphase) >=
      BACKEND_SWITCH_SUBPHASES.indexOf('decommission-export-authorized') ||
      (current.backendSwitchIntent.decommissionSnapshot &&
        !completeSnapshotGroup(
          current.backendSwitchIntent.decommissionSnapshot,
        ) &&
        current.backend === 'plain-worker' &&
        !current.pendingArtifactVersion))
  ) {
    return decommissionBackendSwitchLegacy(options);
  }
  const capabilities = current.decommissionIntent
    ? captureBackendSwitchDecommissionCapabilities(options.provider)
    : shouldUseBoundedCompatibility(options.provider);
  if (!capabilities) return decommissionBackendSwitchLegacy(options);
  let action: DecommissionAdvanceAction = { kind: 'start' };
  let restartedEntryBlock = false;
  for (let index = 0; index < 10_000; index += 1) {
    const result = await advanceBackendSwitchDecommissionInternal(
      {
        ...options,
        action,
        maxProviderRequests: 1_000,
        randomUUID: nodeRandomUUID,
      },
      index === 0 ? capabilities : undefined,
    );
    if (result.status === 'complete') {
      return result.result.record.backendSwitchIntent as BackendSwitchIntent;
    }
    if (result.status === 'blocked') {
      if (action.kind === 'start' && !restartedEntryBlock) {
        restartedEntryBlock = true;
        action = { kind: 'restart-blocked', token: result.token };
        continue;
      }
      throw new Error(
        'bounded backend-switch decommission remains blocked by a Worker attachment',
      );
    }
    action = { kind: 'continue', token: result.token };
  }
  throw new Error('bounded backend-switch decommission did not converge');
}
