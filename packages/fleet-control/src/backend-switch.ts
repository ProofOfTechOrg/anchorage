// SPDX-License-Identifier: Apache-2.0

import {
  applicationBindingTopology,
  assertApplicationR2EmptyBeforeDecommission,
  convergeApplicationR2Deletion,
} from './application-bindings.js';
import {
  isDeploymentEnvironment,
  isDeploymentScriptName,
  isDeploymentTenantTag,
  isSha256,
} from './deployment-context.js';
import { d1MigrationHistoryDigest } from './migration-ledger.js';
import {
  assertExternalPlatformTargetCompatibility,
  assertPlatformResourcesMatchTarget,
  canonicalDeploymentEgressPolicy,
  canonicalMaintenanceCapabilityPublicKey,
  type ExternalRouteExpectation,
  externalHostRoutingTarget,
  externalRouteExpectations,
} from './platform-resources.js';
import {
  applicationBindingTopologyFromUnknown,
  externalReleaseTopologyFromUnknown,
} from './release-topology.js';
import { deploymentSpecDigest } from './spec-digest.js';
import type {
  DeploymentSecrets,
  DeploymentSpec,
  DurableObjectBindingInventory,
  ExternalMutationFence,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
} from './types.js';
import type { HostRoutingTarget } from './workers/host-routing.js';

export const BACKEND_SWITCH_SUBPHASES = [
  'planned',
  'bridge-upload-authorized',
  'bridge-deployed',
  'candidate-deploy-authorized',
  'candidate-deployed',
  'host-publish-authorized',
  'host-published',
  'domain-detach-authorized',
  'dispatch-serving',
  'bridge-private-authorized',
  'bridge-private',
  'ownership-commit-authorized',
  'ready',
  'rollback-route-authorized',
  'rollback-routed',
  'rollback-drain-authorized',
  'rollback-drained',
  'rollback-restore-authorized',
  'rollback-restored',
  'rollback-ownership-authorized',
  'rolled-back',
  'finalize-authorized',
  'finalized',
  'decommission-traffic-authorized',
  'decommission-traffic-removed',
  'decommission-candidate-authorized',
  'decommission-candidate-removed',
  'decommission-bridge-authorized',
  'decommission-bridge-removed',
  'decommission-application-r2-authorized',
  'decommission-application-r2-removed',
  'decommission-export-authorized',
  'decommission-exported',
  'decommission-database-authorized',
  'decommissioned',
] as const;

export type BackendSwitchSubphase = (typeof BACKEND_SWITCH_SUBPHASES)[number];

export interface PlainBackendSnapshot {
  readonly scriptName: string;
  readonly artifactVersion: string;
  readonly specDigest: string;
  readonly databaseId: string;
  readonly databaseName: string;
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly namespaceIds: readonly string[];
  readonly secretNames: readonly string[];
  readonly application?: import('./types.js').ApplicationBindingTopology;
  readonly applicationResources: readonly import('./types.js').ApplicationR2Resource[];
  readonly customDomain: Readonly<{ id: string; hostname: string }>;
}

export interface BridgeSnapshot {
  readonly scriptName: string;
  readonly artifactVersion: string;
  readonly artifactDigest: string;
  readonly databaseId: string;
  readonly durableObjectBindings: readonly DurableObjectBindingInventory[];
  readonly namespaceIds: readonly string[];
  readonly secretNames: readonly string[];
  readonly application?: import('./types.js').ApplicationBindingTopology;
  readonly publicRouteAttached: boolean;
  readonly stateOnly: boolean;
}

export interface BridgeMutationPlan {
  readonly artifactDigest: string;
  readonly durableObjectMigrations: readonly import('./types.js').DurableObjectMigration[];
  readonly priorDurableObjectTag?: string;
  readonly targetDurableObjectTag?: string;
  readonly secretNames: readonly string[];
  readonly mutationDigest: string;
}

export interface BackendSwitchIntent {
  readonly kind: 'backend-switch';
  readonly tenantTag: string;
  readonly environment: string;
  readonly prior: PlainBackendSnapshot;
  readonly targetSpecDigest: string;
  readonly targetApplication: import('./types.js').ApplicationBindingTopology;
  readonly target: ExternalPlatformTargetDescription;
  readonly rollbackUntil: string;
  readonly subphase: BackendSwitchSubphase;
  readonly bridgePlan?: BridgeMutationPlan;
  readonly bridge?: BridgeSnapshot;
  readonly candidate?: BackendSwitchCandidateSnapshot;
  readonly restoredArtifactVersion?: string;
  readonly databaseExport?: import('./types.js').DatabaseExport;
  readonly applicationR2Progress?: readonly BackendSwitchApplicationR2Progress[];
  readonly stateReconcileIntent?: Readonly<{
    targetSpecDigest: string;
    plan: BridgeMutationPlan;
    subphase: 'upload-authorized' | 'uploaded';
  }>;
  readonly decommissionSnapshot?: BackendSwitchDecommissionSnapshot;
}

export interface BackendSwitchDecommissionRelease {
  readonly release: ExternalReleaseSnapshot;
  readonly subphase: 'present' | 'delete-authorized' | 'deleted';
}

export interface BackendSwitchDecommissionSnapshot {
  readonly routeHostname: string;
  readonly routeTargets: readonly BackendSwitchDecommissionRouteTarget[];
  readonly desiredSpecDigest: string;
  readonly target: ExternalPlatformTargetDescription;
  readonly releases: readonly BackendSwitchDecommissionRelease[];
  readonly applicationResources: readonly import('./types.js').ApplicationR2Resource[];
  readonly bridge?: BridgeSnapshot;
  readonly resources?: ExternalPlatformResources;
  readonly bridgePlan?: BridgeMutationPlan;
}

export interface BackendSwitchDecommissionRouteTarget {
  readonly release: ExternalReleaseSnapshot;
  readonly target: ExternalPlatformTargetDescription;
  readonly routeTarget: HostRoutingTarget;
}

export interface BackendSwitchApplicationR2Progress {
  readonly resource: import('./types.js').ApplicationR2Resource;
  readonly subphase: import('./types.js').ApplicationR2Resource['state'];
}

export interface BackendSwitchCandidateSnapshot
  extends ExternalReleaseSnapshot {
  readonly maintenance: Readonly<{
    receipt: string;
    specDigest: string;
  }>;
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
  if (
    !Array.isArray(value) ||
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
  const domain = prior.customDomain;
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
  const topology = release.topology;
  const maintenance = release.maintenance;
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
): BackendSwitchDecommissionSnapshot {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid decommission snapshot');
  }
  const snapshot = value as Record<string, unknown>;
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

export function backendSwitchIntentFromUnknown(
  value: unknown,
): BackendSwitchIntent {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid intent');
  }
  const intent = value as Record<string, unknown>;
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
  const parsedDecommissionSnapshot =
    intent.decommissionSnapshot === undefined
      ? undefined
      : decommissionSnapshot(
          intent.decommissionSnapshot,
          target,
          intent.tenantTag,
          intent.environment,
        );
  const targetApplication = applicationBindingTopologyFromUnknown(
    intent.targetApplication,
    'backend switch target application',
  );
  const parsedApplicationR2Progress =
    intent.applicationR2Progress !== undefined
      ? applicationR2Progress(intent.applicationR2Progress)
      : undefined;
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
  };
}

function databaseExport(value: unknown): import('./types.js').DatabaseExport {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('backend switch state has invalid database export');
  }
  const candidate = value as Record<string, unknown>;
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

async function withBackendSwitchLease<T>(
  store: FleetStateStore,
  tenantTag: string,
  environment: string,
  operation: (lease: BackendSwitchLease) => Promise<T>,
): Promise<T> {
  return store.withDeploymentLease(
    tenantTag,
    environment,
    async (fleetLease) => {
      let record = await store.get(tenantTag, environment);
      if (!record) {
        throw new Error(
          `deployment '${tenantTag}:${environment}' is not provisioned`,
        );
      }
      const lease: BackendSwitchLease = {
        mutationLeaseTtlMs: fleetLease.mutationLeaseTtlMs,
        assertOwned: () => fleetLease.assertOwned(),
        get: async () => record?.backendSwitchIntent,
        current: () => record as FleetRecord,
        put: async (intent) => {
          if (
            intent.tenantTag !== tenantTag ||
            intent.environment !== environment
          ) {
            throw new Error('backend switch lease cannot write another intent');
          }
          record = {
            ...(record as FleetRecord),
            backendSwitchIntent: intent,
            updatedAt: new Date().toISOString(),
          };
          await fleetLease.put(record);
        },
        putOwnership: async (nextRecord, intent) => {
          if (
            nextRecord.tenantTag !== tenantTag ||
            nextRecord.environment !== environment
          ) {
            throw new Error(
              'backend switch lease cannot commit another deployment',
            );
          }
          record = {
            ...nextRecord,
            backendSwitchIntent: intent,
            updatedAt: new Date().toISOString(),
          };
          await fleetLease.put(record);
        },
      };
      return operation(lease);
    },
  );
}

export interface BackendSwitchProvider {
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
    durableObjectTag: input.target.stateDurableObjectTag,
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
    JSON.stringify(record.activeRelease) !==
      JSON.stringify(canonicalCandidateRelease(intent)) ||
    JSON.stringify(record.platformTarget) !== JSON.stringify(intent.target) ||
    JSON.stringify(record.outboundPolicy) !==
      JSON.stringify(intent.target.outboundPolicy) ||
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
    async (lease) => {
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
        JSON.stringify(intent.target) !== JSON.stringify(options.target)
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
    async (lease) => {
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
    async (lease) => {
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
    JSON.stringify(left.routeTarget).localeCompare(
      JSON.stringify(right.routeTarget),
    ),
  );
  return {
    routeHostname: record.routeHostname.toLowerCase(),
    routeTargets,
    desiredSpecDigest: record.desiredSpecDigest,
    target: currentTarget,
    releases,
    applicationResources: record.applicationResources ?? [],
    ...(intent.bridge ? { bridge: intent.bridge } : {}),
    ...(resources ? { resources } : {}),
    ...(effectiveBridgePlan ? { bridgePlan: effectiveBridgePlan } : {}),
  };
}

export async function decommissionBackendSwitch(options: {
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
    async (lease) => {
      let intent = await lease.get();
      if (!intent)
        throw new Error('backend switch has no decommission snapshot');
      if (intent.prior.specDigest !== deploymentSpecDigest(options.priorSpec)) {
        throw new Error(
          'backend switch prior decommission spec differs from durable intent',
        );
      }
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
          { ...lease.current(), phase: 'decommissioning' },
          intent,
        );
      } else if (!intent.subphase.startsWith('decommission-')) {
        await assertApplicationR2EmptyBeforeDecommission({
          resources: intent.decommissionSnapshot.applicationResources,
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
          { ...lease.current(), phase: 'decommissioning' },
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
