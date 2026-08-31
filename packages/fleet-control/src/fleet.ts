// SPDX-License-Identifier: Apache-2.0

import type { AttestConvergedActiveRouteOptions } from './active-route.js';
import {
  applicationBindingTopology,
  DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
  liveApplicationTopologyMatches,
} from './application-bindings.js';
import {
  assertBackendSwitchInactive,
  commitInvocationAuthority,
  type FinalizedOrdinaryStateProvider,
  finalizedBridgeForRecord,
  reconcileFinalizedBackendSwitchState,
} from './backend-switch.js';
import {
  assertExternalPlatformTarget,
  assertExternalPlatformTargetCompatibility,
  assertPlatformResourcesMatchTarget,
  canonicalDurableObjectMigrationHistory,
  describeExternalPlatformTarget,
  durableObjectMigrationHistoryDigest,
  effectiveAppliedPlatformTarget,
  externalHostRoutingTarget,
  externalPlatformResourceGroupId,
  externalReleaseTopology,
  externalRouteExpectations,
} from './platform-resources.js';
import { buildPromotionGuard } from './promotion-guard.js';
import {
  assertExternalReleaseArtifactVersion,
  assertImmutableDeploymentMapping,
  assertLiveDeploymentMatches,
  assertPlatformDurableObjectHistory,
  reconcilePersistedDatabase,
} from './provision.js';
import { settlePromotedRoute } from './settlement.js';
import { deploymentSpecDigest } from './spec-digest.js';
import type {
  DeploymentEgressPolicy,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMigrationIntent,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  ExternalReleaseTopology,
  FleetInventoryFinding,
  FleetRecord,
  FleetResourceInventory,
  FleetSettlementHost,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  ProvisioningBackend,
} from './types.js';
import {
  assertNoActiveCleanup,
  assertNoActiveDecommission,
  effectiveLifecyclePhase,
} from './types.js';
import {
  targetDurableObjectTag,
  validateDeploymentSecrets,
  validateDeploymentSpec,
} from './validation.js';

async function convergeExternalPlatformResources(
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
  database: {
    readonly id: string;
    readonly name: string;
    readonly created: false;
  },
  secrets: DeploymentSecrets,
  expectedTarget: ExternalPlatformTargetDescription,
  record: FleetRecord,
  lease: FleetStateLease,
  clock: () => number,
  finalizedStateProvider?: FinalizedOrdinaryStateProvider,
): Promise<FleetRecord> {
  if (spec.authoredBy !== 'external') return record;
  if (record.platformResources?.stateWorker.plane === 'ordinary') {
    if (!finalizedStateProvider) {
      throw new Error(
        'finalized ordinary state requires its backend-switch provider',
      );
    }
    return reconcileFinalizedBackendSwitchState({
      provider: finalizedStateProvider,
      targetSpec: spec,
      target: expectedTarget,
      record,
      lease,
      clock,
    });
  }
  if (!backend.ensurePlatformResources) {
    throw new Error(
      'external backend cannot provision trusted platform resources',
    );
  }
  await lease.assertOwned();
  const result = await backend.ensurePlatformResources(
    spec,
    database,
    secrets,
    expectedTarget,
    record,
    lease,
  );
  assertPlatformResourcesMatchTarget(result.resources, expectedTarget);
  if (
    JSON.stringify(result.resources) ===
      JSON.stringify(record.platformResources) &&
    JSON.stringify(expectedTarget) === JSON.stringify(record.platformTarget) &&
    JSON.stringify(expectedTarget.outboundPolicy) ===
      JSON.stringify(record.outboundPolicy)
  ) {
    return record;
  }
  const updated = {
    ...record,
    platformResources: result.resources,
    platformTarget: expectedTarget,
    outboundPolicy: expectedTarget.outboundPolicy,
    updatedAt: new Date(clock()).toISOString(),
  };
  await lease.put(updated);
  return updated;
}

export interface DriftFinding {
  readonly tenantTag: string;
  readonly environment: string;
  readonly kind:
    | 'missing-deployment'
    | 'duplicate-deployment'
    | 'database-mismatch'
    | 'duplicate-database'
    | 'duplicate-namespace'
    | 'binding-drift'
    | 'route-drift'
    | 'orphan-deployment'
    | 'orphan-database'
    | 'missing-namespace'
    | 'orphan-namespace'
    | 'orphan-route'
    | 'missing-r2-bucket'
    | 'orphan-r2-bucket'
    | 'r2-bucket-drift'
    | 'duplicate-route'
    | 'incomplete-provisioning'
    | 'version-drift'
    | 'maintenance-stale'
    | 'audit-error'
    | FleetInventoryFinding['kind'];
  readonly detail: string;
}

function knownArtifactVersion(
  artifactVersion: string | undefined,
): string | undefined {
  return artifactVersion && artifactVersion !== 'pending'
    ? artifactVersion
    : undefined;
}

function activeArtifactVersion(record: FleetRecord): string {
  return record.activeRelease?.artifactVersion ?? record.artifactVersion;
}

function pendingArtifactVersion(record: FleetRecord): string | undefined {
  return knownArtifactVersion(
    record.pendingRelease?.artifactVersion ?? record.pendingArtifactVersion,
  );
}

function hasActiveCleanup(record: FleetRecord): boolean {
  return (
    record.phase === 'cleanup-advancing' || record.cleanupIntent !== undefined
  );
}

/**
 * Script keys a deployment under active bounded cleanup may still own live.
 * They feed the orphan suppressions only: the bounded engine, not the drift
 * audit, is the reconciliation authority while its teardown runs.
 */
function cleanupKnownScriptKeys(record: FleetRecord): readonly string[] {
  const keys = [
    `${record.backend}:${record.scriptName}`,
    ...[
      record.activeRelease,
      record.pendingRelease,
      record.rollbackRelease,
      record.retiringRelease,
      record.migrationPriorRelease,
    ]
      .filter((release) => release !== undefined)
      .map((release) => `${record.backend}:${release.physicalScriptName}`),
  ];
  if (record.platformResources) {
    keys.push(
      `${
        record.platformResources.stateWorker.plane === 'dispatch'
          ? 'workers-for-platforms'
          : 'plain-worker'
      }:${record.platformResources.stateWorker.scriptName}`,
    );
    if (record.platformResources.egressProxy) {
      keys.push(
        `plain-worker:${record.platformResources.egressProxy.scriptName}`,
      );
    }
  }
  return keys;
}

function expectsDatabase(record: FleetRecord): boolean {
  return effectiveLifecyclePhase(record) !== 'decommissioned';
}

function expectsWorker(record: FleetRecord): boolean {
  const phase = effectiveLifecyclePhase(record);
  return ![
    'database-reserved',
    'database-create-authorized',
    'database-created',
    'identity-seeded',
    'migrated',
    'worker-deleted',
    'platform-credentials-revoked',
    'platform-resources-deleted',
    'database-exported',
    'database-deleting',
    'decommissioned',
  ].includes(phase);
}

function expectsRoute(record: FleetRecord): boolean {
  const phase = effectiveLifecyclePhase(record);
  return [
    'publishing',
    'ready',
    'migrating',
    'rolling-back',
    'decommissioning',
    'credentials-revoked',
  ].includes(phase);
}

function expectsPlatformResources(record: FleetRecord): boolean {
  const phase = effectiveLifecyclePhase(record);
  return (
    record.platformResources !== undefined &&
    ![
      'platform-resources-deleted',
      'database-exported',
      'database-deleting',
      'decommissioned',
    ].includes(phase)
  );
}

function liveScriptName(record: FleetRecord): string {
  return record.activeRelease?.physicalScriptName ?? record.scriptName;
}

function expectedReleaseSnapshots(
  record: FleetRecord,
): readonly ExternalReleaseSnapshot[] {
  if (!expectsWorker(record) || record.backend === 'plain-worker') return [];
  const phase = effectiveLifecyclePhase(record);
  const snapshots = (() => {
    switch (phase) {
      case 'worker-deployed':
      case 'maintenance-armed':
      case 'publishing':
        return [record.pendingRelease, record.activeRelease];
      case 'migrating':
        return [
          record.activeRelease,
          record.pendingRelease,
          record.migrationPriorRelease,
          record.rollbackRelease,
        ];
      case 'rolling-back':
        return [
          record.activeRelease,
          record.pendingRelease,
          record.rollbackRelease,
        ];
      case 'ready':
        return [
          record.activeRelease,
          record.rollbackRelease,
          record.retiringRelease,
        ];
      case 'decommissioning':
      case 'traffic-removed':
      case 'credentials-revoked':
        return [
          record.activeRelease,
          record.pendingRelease,
          record.migrationPriorRelease,
          record.rollbackRelease,
          record.retiringRelease,
        ];
      default:
        return [];
    }
  })();
  return snapshots
    .filter(
      (release): release is ExternalReleaseSnapshot => release !== undefined,
    )
    .filter(
      (release, index, releases) =>
        releases.findIndex(
          (candidate) =>
            candidate.physicalScriptName === release.physicalScriptName,
        ) === index,
    );
}

function expectedScriptNames(record: FleetRecord): readonly string[] {
  if (!expectsWorker(record)) return [];
  if (record.backend === 'plain-worker') return [record.scriptName];
  const phase = effectiveLifecyclePhase(record);
  const releases = expectedReleaseSnapshots(record);
  const names = releases.map((release) => release.physicalScriptName);
  if (
    names.length === 0 &&
    [
      'worker-deployed',
      'maintenance-armed',
      'publishing',
      'migrating',
      'rolling-back',
      'ready',
      'decommissioning',
      'traffic-removed',
      'credentials-revoked',
    ].includes(phase)
  ) {
    names.push(record.scriptName);
  }
  return names;
}

function routePolicyMatches(
  route: FleetResourceInventory['routes'][number],
  record: FleetRecord,
): boolean {
  if (record.backend === 'plain-worker') return true;
  const policy = record.outboundPolicy;
  if (!policy) return false;
  return (
    route.policyId === policy.policyId &&
    route.policyDigest === policy.policyDigest &&
    JSON.stringify(route.policyHosts) === JSON.stringify(policy.policyHosts)
  );
}

function routeMatchesRecord(
  route: FleetResourceInventory['routes'][number],
  record: FleetRecord,
): boolean {
  if (record.backend === 'workers-for-platforms') {
    return externalRouteExpectations(record).some((expected) => {
      const target = externalHostRoutingTarget(record, expected);
      return (
        route.scriptName === target.scriptName &&
        route.policyId === target.policyId &&
        route.policyDigest === target.policyDigest &&
        JSON.stringify(route.policyHosts) ===
          JSON.stringify(target.policyHosts) &&
        JSON.stringify(route.stateEgress) === JSON.stringify(target.stateEgress)
      );
    });
  }
  return (
    allowedRouteScriptNames(record).includes(route.scriptName) &&
    routePolicyMatches(route, record)
  );
}

function expectedDeploymentKeys(record: FleetRecord): readonly Readonly<{
  backend: FleetRecord['backend'];
  scriptName: string;
}>[] {
  const expected = expectedScriptNames(record).map((scriptName) => ({
    backend: record.backend,
    scriptName,
  }));
  if (expectsPlatformResources(record) && record.platformResources) {
    expected.push({
      backend:
        record.platformResources.stateWorker.plane === 'dispatch'
          ? 'workers-for-platforms'
          : 'plain-worker',
      scriptName: record.platformResources.stateWorker.scriptName,
    });
    if (record.platformResources.egressProxy) {
      expected.push({
        backend: 'plain-worker',
        scriptName: record.platformResources.egressProxy.scriptName,
      });
    }
  }
  return expected;
}

function expectsNamespaces(record: FleetRecord): boolean {
  return (
    expectedScriptNames(record).length > 0 || expectsPlatformResources(record)
  );
}

function expectedNamespaceIdsForRecord(record: FleetRecord): readonly string[] {
  return [
    ...new Set([
      ...record.durableObjectBindings.map((binding) => binding.namespaceId),
      ...expectedReleaseSnapshots(record).flatMap(
        (release) =>
          release.topology?.durableObjectBindings.map(
            (binding) => binding.namespaceId,
          ) ?? [],
      ),
      ...(expectsPlatformResources(record)
        ? (record.platformResources?.stateWorker.namespaceIds ?? [])
        : []),
      ...(record.backendSwitchIntent?.subphase === 'rolled-back'
        ? (record.backendSwitchIntent.bridge?.namespaceIds ?? [])
        : []),
    ]),
  ].sort();
}

function allowedRouteScriptNames(record: FleetRecord): readonly string[] {
  if (record.backend === 'workers-for-platforms') {
    return externalRouteExpectations(record).map(
      (expected) => expected.release.physicalScriptName,
    );
  }
  return [record.scriptName];
}

interface DutyHealth {
  readonly name: 'sweep' | 'purge' | 'tick';
  readonly lastSuccessAt: number | null;
  readonly lastAttemptAt: number | null | undefined;
  readonly lastError: string | undefined;
}

function bindingKey(binding: {
  readonly name: string;
  readonly className: string;
  readonly namespaceId: string;
}): string {
  return `${binding.name}:${binding.className}:${binding.namespaceId}`;
}

function fullBindingKey(binding: {
  readonly name: string;
  readonly className: string;
  readonly namespaceId: string;
  readonly scriptName?: string;
  readonly dispatchNamespace?: string;
}): string {
  return [
    binding.name,
    binding.className,
    binding.namespaceId,
    binding.scriptName ?? '',
    binding.dispatchNamespace ?? '',
  ].join(':');
}

function namedTargetKeys(
  bindings: readonly Readonly<{
    name: string;
    service?: string;
    queueName?: string;
  }>[],
): readonly string[] {
  return bindings
    .map(
      (binding) =>
        `${binding.name}:${binding.service ?? binding.queueName ?? ''}`,
    )
    .sort();
}

function externalReleaseTopologyFromLive(
  live: LiveDeployment,
  intended: ExternalReleaseTopology,
): ExternalReleaseTopology {
  return {
    durableObjectBindings: live.durableObjectBindings,
    serviceBindings: live.serviceBindings ?? [],
    queueProducerBindings: live.queueProducerBindings ?? [],
    secretNames: intended.secretNames,
    ...(intended.application ? { application: intended.application } : {}),
  };
}

function configuredDuties(health: MaintenanceHealth): readonly DutyHealth[] {
  return [
    {
      name: 'sweep',
      lastSuccessAt: health.lastSweepAt,
      lastAttemptAt: health.lastSweepAttemptAt,
      lastError: health.lastSweepError,
    },
    {
      name: 'purge',
      lastSuccessAt: health.lastPurgeAt,
      lastAttemptAt: health.lastPurgeAttemptAt,
      lastError: health.lastPurgeError,
    },
    ...(health.lastTickAt === undefined
      ? []
      : [
          {
            name: 'tick' as const,
            lastSuccessAt: health.lastTickAt,
            lastAttemptAt: health.lastTickAttemptAt,
            lastError: health.lastTickError,
          },
        ]),
  ];
}

function staleDutyReason(
  duty: DutyHealth,
  deployedAt: number | undefined,
  now: number,
  staleAfterMs: number,
): string | undefined {
  if (duty.lastError !== undefined) {
    return `${duty.name} last attempt failed${
      duty.lastAttemptAt === undefined || duty.lastAttemptAt === null
        ? ''
        : ` at ${duty.lastAttemptAt}`
    }: ${duty.lastError}`;
  }
  const reference = duty.lastSuccessAt ?? deployedAt;
  if (reference === undefined) {
    return `${duty.name} has no success or deployment freshness reference`;
  }
  if (now - reference <= staleAfterMs) return undefined;
  return duty.lastSuccessAt === null
    ? `${duty.name} has not succeeded within the deployment grace period`
    : `${duty.name} last succeeded ${now - duty.lastSuccessAt}ms ago`;
}

export async function auditFleetDrift(options: {
  readonly store: FleetStateStore;
  readonly records: readonly FleetRecord[];
  readonly inventory: FleetResourceInventory;
  readonly backendFor: (record: FleetRecord) => ProvisioningBackend;
  readonly specFor: (record: FleetRecord) => DeploymentSpec;
  readonly maintenanceSecretFor: (record: FleetRecord) => string;
  readonly staleAfterMs: number;
  readonly now?: number;
}): Promise<readonly DriftFinding[]> {
  if (!Number.isSafeInteger(options.staleAfterMs) || options.staleAfterMs < 1) {
    throw new Error('staleAfterMs must be a positive safe integer');
  }
  const now = options.now ?? Date.now();
  const findings: DriftFinding[] = [...options.inventory.findings];
  // A deployment under active bounded cleanup is audit-suppressed in both
  // directions: it feeds no expectations (no missing/duplicate findings) and
  // its declared resource identities join the known sets below so its
  // still-present resources never read as orphans. The bounded engine is the
  // reconciliation authority; a long-blocked cleanup stays visible through
  // the record itself, never through drift findings.
  const auditedRecords = options.records.filter(
    (record) => !hasActiveCleanup(record),
  );
  const knownScriptKeys = new Set<string>();
  const knownRouteKeys = new Set<string>();
  const knownDatabaseIds = new Set<string>();
  const knownNamespaceIds = new Set<string>();
  const knownBucketNames = new Set<string>();
  for (const record of options.records) {
    if (!hasActiveCleanup(record)) continue;
    const scriptKeys = cleanupKnownScriptKeys(record);
    for (const key of scriptKeys) {
      knownScriptKeys.add(key);
      const scriptName = key.slice(key.indexOf(':') + 1);
      knownRouteKeys.add(`${record.routeHostname}:${scriptName}`);
    }
    knownDatabaseIds.add(record.databaseId);
    for (const binding of record.durableObjectBindings) {
      knownNamespaceIds.add(binding.namespaceId);
    }
    for (const namespaceId of record.platformResources?.stateWorker
      .namespaceIds ?? []) {
      knownNamespaceIds.add(namespaceId);
    }
    for (const resource of record.applicationResources ?? []) {
      knownBucketNames.add(resource.bucketName);
    }
  }
  const recordsByScript = new Map<string, FleetRecord[]>();
  for (const record of auditedRecords) {
    for (const expected of expectedDeploymentKeys(record)) {
      const key = `${expected.backend}:${expected.scriptName}`;
      const matches = recordsByScript.get(key) ?? [];
      matches.push(record);
      recordsByScript.set(key, matches);
    }
  }
  for (const registration of options.inventory.scriptRegistrations) {
    const key = `workers-for-platforms:${registration.scriptName}`;
    if (
      !recordsByScript.has(key) &&
      !knownScriptKeys.has(key) &&
      !options.inventory.deployments.some(
        (deployment) =>
          deployment.backend === 'workers-for-platforms' &&
          deployment.scriptName === registration.scriptName,
      )
    ) {
      findings.push({
        tenantTag: registration.tenantTag,
        environment: registration.environment,
        kind: 'orphan-deployment',
        detail: `registered script '${registration.scriptName}' has no live fleet owner`,
      });
    }
  }
  const liveByScript = new Map<
    string,
    FleetResourceInventory['deployments'][number][]
  >();
  for (const deployment of options.inventory.deployments) {
    const key = `${deployment.backend}:${deployment.scriptName}`;
    const matches = liveByScript.get(key) ?? [];
    matches.push(deployment);
    liveByScript.set(key, matches);
    if (!recordsByScript.has(key) && !knownScriptKeys.has(key)) {
      findings.push({
        tenantTag: deployment.tenantTag,
        environment: deployment.environment,
        kind: 'orphan-deployment',
        detail: `unregistered script '${deployment.scriptName}'`,
      });
    }
  }
  for (const record of auditedRecords) {
    for (const expected of expectedDeploymentKeys(record)) {
      const key = `${expected.backend}:${expected.scriptName}`;
      const liveMatches = liveByScript.get(key) ?? [];
      const registered =
        expected.backend !== 'workers-for-platforms' ||
        options.inventory.scriptRegistrations.some(
          (registration) =>
            registration.scriptName === expected.scriptName &&
            registration.tenantTag === record.tenantTag &&
            registration.environment === record.environment,
        );
      const absentFrom = [
        ...(liveMatches.length === 0 ? ['provider inventory'] : []),
        ...(!registered ? ['fleet registry'] : []),
      ];
      if (absentFrom.length > 0) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'missing-deployment',
          detail: `expected lifecycle Worker '${expected.scriptName}' is absent from ${absentFrom.join(' and ')}`,
        });
      }
      if (liveMatches.length > 1) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'duplicate-deployment',
          detail: `expected lifecycle Worker '${expected.scriptName}' appears ${liveMatches.length} times`,
        });
      }
    }
  }
  const registeredDatabaseIds = new Set(
    auditedRecords.filter(expectsDatabase).map((record) => record.databaseId),
  );
  for (const databaseId of options.inventory.databaseIds) {
    if (
      !registeredDatabaseIds.has(databaseId) &&
      !knownDatabaseIds.has(databaseId)
    ) {
      findings.push({
        tenantTag: 'unknown',
        environment: 'unknown',
        kind: 'orphan-database',
        detail: `unregistered fleet database '${databaseId}'`,
      });
    }
  }
  const expectedRoutes = new Map(
    auditedRecords
      .filter(expectsRoute)
      .map((record) => [
        record.routeHostname,
        { record, scriptNames: allowedRouteScriptNames(record) },
      ]),
  );
  const liveRoutesByHostname = new Map<
    string,
    FleetResourceInventory['routes'][number][]
  >();
  for (const route of options.inventory.routes) {
    const routeMatches = liveRoutesByHostname.get(route.hostname) ?? [];
    routeMatches.push(route);
    liveRoutesByHostname.set(route.hostname, routeMatches);
    const expected = expectedRoutes.get(route.hostname);
    if (knownRouteKeys.has(`${route.hostname}:${route.scriptName}`)) continue;
    if (
      !expected ||
      expected.record.backend !== route.backend ||
      !expected.scriptNames.includes(route.scriptName) ||
      expected.record.tenantTag !== route.tenantTag ||
      expected.record.environment !== route.environment ||
      !routeMatchesRecord(route, expected.record)
    ) {
      findings.push({
        tenantTag: route.tenantTag,
        environment: route.environment,
        kind: 'orphan-route',
        detail: `route '${route.hostname}' points to unregistered mapping '${route.scriptName}'`,
      });
    }
  }
  const databases = new Map<string, FleetRecord>();
  const expectedNamespaceOwners = new Map<string, FleetRecord>();
  const liveNamespaceOwners = new Map<string, FleetRecord>();
  const duplicateNamespaceIds = new Set<string>();
  const expectedNamespaceIds = new Set(
    auditedRecords
      .filter(expectsNamespaces)
      .flatMap(expectedNamespaceIdsForRecord),
  );
  for (const namespaceId of options.inventory.namespaceIds) {
    if (
      !expectedNamespaceIds.has(namespaceId) &&
      !knownNamespaceIds.has(namespaceId)
    ) {
      findings.push({
        tenantTag: 'unknown',
        environment: 'unknown',
        kind: 'orphan-namespace',
        detail: `unregistered Durable Object namespace '${namespaceId}'`,
      });
    }
  }
  for (const record of auditedRecords.filter(expectsNamespaces)) {
    for (const namespaceId of expectedNamespaceIdsForRecord(record)) {
      const namespaceOwner = expectedNamespaceOwners.get(namespaceId);
      if (namespaceOwner) {
        duplicateNamespaceIds.add(namespaceId);
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'duplicate-namespace',
          detail: `namespace '${namespaceId}' also bound to ${namespaceOwner.tenantTag}:${namespaceOwner.environment}`,
        });
      } else {
        expectedNamespaceOwners.set(namespaceId, record);
      }
      if (!options.inventory.namespaceIds.includes(namespaceId)) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'missing-namespace',
          detail: `expected Durable Object namespace '${namespaceId}' is absent from fleet inventory`,
        });
      }
    }
  }

  const expectedBuckets = new Map<
    string,
    {
      readonly record: FleetRecord;
      readonly resource: NonNullable<
        FleetRecord['applicationResources']
      >[number];
    }
  >();
  for (const record of auditedRecords) {
    const phase = effectiveLifecyclePhase(record);
    if (
      [
        'application-resources-deleted',
        'database-exported',
        'database-deleting',
        'decommissioned',
      ].includes(phase)
    ) {
      continue;
    }
    for (const resource of record.applicationResources ?? []) {
      if (resource.state !== 'created' || !resource.creationDate) continue;
      const prior = expectedBuckets.get(resource.bucketName);
      if (prior) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'r2-bucket-drift',
          detail: `R2 bucket '${resource.bucketName}' is claimed by more than one deployment`,
        });
      } else {
        expectedBuckets.set(resource.bucketName, { record, resource });
      }
    }
  }
  const liveBuckets = new Map(
    (options.inventory.r2Buckets ?? []).map((bucket) => [
      bucket.bucketName,
      bucket,
    ]),
  );
  for (const bucket of options.inventory.r2Buckets ?? []) {
    if (
      !expectedBuckets.has(bucket.bucketName) &&
      !knownBucketNames.has(bucket.bucketName)
    ) {
      findings.push({
        tenantTag: 'unknown',
        environment: 'unknown',
        kind: 'orphan-r2-bucket',
        detail: `unregistered fleet R2 bucket '${bucket.bucketName}'`,
      });
    }
  }
  for (const { record, resource } of expectedBuckets.values()) {
    const live = liveBuckets.get(resource.bucketName);
    if (!live) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'missing-r2-bucket',
        detail: `expected R2 bucket '${resource.bucketName}' is absent from fleet inventory`,
      });
    } else if (
      live.jurisdiction !== resource.jurisdiction ||
      live.creationDate !== resource.creationDate
    ) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'r2-bucket-drift',
        detail: `R2 bucket '${resource.bucketName}' changed its persisted creation identity`,
      });
    }
  }

  for (const record of options.records) {
    // A stale or blocked bounded cleanup must not read as
    // incomplete-provisioning, version, binding, or route drift.
    if (hasActiveCleanup(record)) continue;
    const phase = effectiveLifecyclePhase(record);
    const recordMatches =
      recordsByScript.get(`${record.backend}:${liveScriptName(record)}`) ?? [];
    if (recordMatches.length > 1) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'duplicate-deployment',
        detail: `script '${record.scriptName}' is registered ${recordMatches.length} times`,
      });
    }
    const inventoryMatches =
      liveByScript.get(`${record.backend}:${liveScriptName(record)}`) ?? [];
    const inventoryDeployment = inventoryMatches[0];
    const recordUpdatedAt = Date.parse(record.updatedAt);
    if (
      phase !== 'ready' &&
      (!Number.isFinite(recordUpdatedAt) ||
        now - recordUpdatedAt > options.staleAfterMs)
    ) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'incomplete-provisioning',
        detail: `phase '${phase}' has not advanced`,
      });
    }
    const expectedReleases = expectedReleaseSnapshots(record);
    for (const release of expectedReleases) {
      const matches =
        liveByScript.get(`${record.backend}:${release.physicalScriptName}`) ??
        [];
      if (matches.length !== 1) continue;
      const liveRelease = matches[0];
      if (!liveRelease) continue;
      if (
        liveRelease.tenantTag !== record.tenantTag ||
        liveRelease.environment !== record.environment ||
        liveRelease.artifactVersion !== release.artifactVersion ||
        liveRelease.schemaVersion !== release.releaseSchemaVersion ||
        liveRelease.desiredSpecDigest !== release.specDigest
      ) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'version-drift',
          detail: `lifecycle release '${release.physicalScriptName}' does not match its persisted identity, artifact, schema, and spec digest`,
        });
      }
      if (
        liveRelease.databaseIds.length !== 1 ||
        liveRelease.databaseIds[0] !== record.databaseId
      ) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'database-mismatch',
          detail: `lifecycle release '${release.physicalScriptName}' is not bound exactly to database '${record.databaseId}'`,
        });
      }
      if (!release.topology) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'audit-error',
          detail: `lifecycle release '${release.physicalScriptName}' has no durable binding topology`,
        });
      } else if (
        JSON.stringify(
          liveRelease.durableObjectBindings.map(fullBindingKey).sort(),
        ) !==
          JSON.stringify(
            release.topology.durableObjectBindings.map(fullBindingKey).sort(),
          ) ||
        JSON.stringify(namedTargetKeys(liveRelease.serviceBindings ?? [])) !==
          JSON.stringify(namedTargetKeys(release.topology.serviceBindings)) ||
        JSON.stringify(
          namedTargetKeys(liveRelease.queueProducerBindings ?? []),
        ) !==
          JSON.stringify(
            namedTargetKeys(release.topology.queueProducerBindings),
          ) ||
        JSON.stringify([...liveRelease.secretNames].sort()) !==
          JSON.stringify([...release.topology.secretNames].sort()) ||
        !liveApplicationTopologyMatches(
          release.topology.application,
          liveRelease,
          DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
        )
      ) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'binding-drift',
          detail: `lifecycle release '${release.physicalScriptName}' has drifted Durable Object, service, queue, application variable, R2, or secret topology`,
        });
      }
    }
    if (phase !== 'ready') continue;
    if (!inventoryDeployment) {
      continue;
    }
    if (
      inventoryDeployment.databaseIds.length !== 1 ||
      inventoryDeployment.databaseIds[0] !== record.databaseId ||
      !options.inventory.databaseIds.includes(record.databaseId)
    ) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'database-mismatch',
        detail: `fleet inventory does not contain exactly database '${record.databaseId}' for '${record.scriptName}'`,
      });
    }
    const routeOwnerDeployments = allowedRouteScriptNames(record).flatMap(
      (scriptName) => liveByScript.get(`${record.backend}:${scriptName}`) ?? [],
    );
    if (
      routeOwnerDeployments.filter(
        (deployment) =>
          deployment.routeHostnames.length === 1 &&
          deployment.routeHostnames[0] === record.routeHostname,
      ).length !== 1 ||
      routeOwnerDeployments.some((deployment) =>
        deployment.routeHostnames.some(
          (hostname) => hostname !== record.routeHostname,
        ),
      )
    ) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'route-drift',
        detail: `deployment inventory does not contain exactly route '${record.routeHostname}'`,
      });
    }
    const expectedBindingKeys = [...record.durableObjectBindings]
      .map(bindingKey)
      .sort();
    const liveBindingKeys = [...inventoryDeployment.durableObjectBindings]
      .map(bindingKey)
      .sort();
    if (
      JSON.stringify(expectedBindingKeys) !== JSON.stringify(liveBindingKeys)
    ) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'binding-drift',
        detail: `expected ${expectedBindingKeys.join(',') || 'no bindings'}, found ${liveBindingKeys.join(',') || 'no bindings'}`,
      });
    }
    const routeMatches = liveRoutesByHostname.get(record.routeHostname) ?? [];
    if (routeMatches.length > 1) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'duplicate-route',
        detail: `route '${record.routeHostname}' appears ${routeMatches.length} times`,
      });
    }
    const route = routeMatches[0];
    if (
      !route ||
      route.backend !== record.backend ||
      route.tenantTag !== record.tenantTag ||
      route.environment !== record.environment ||
      !routeMatchesRecord(route, record)
    ) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'route-drift',
        detail: `route '${record.routeHostname}' is missing or mismatched`,
      });
    }
    let backend: ProvisioningBackend;
    try {
      backend = options.backendFor(record);
    } catch (error) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'audit-error',
        detail: `backend resolver failed: ${String(error)}`,
      });
      continue;
    }
    let spec: DeploymentSpec;
    try {
      spec = options.specFor(record);
    } catch (error) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'audit-error',
        detail: `spec resolver failed: ${String(error)}`,
      });
      continue;
    }
    if (inventoryDeployment) {
      const expectedServiceBindings =
        spec.authoredBy === 'external'
          ? []
          : spec.egressProxyService
            ? [{ name: 'EGRESS_PROXY', service: spec.egressProxyService }]
            : [];
      const expectedQueueBindings =
        spec.authoredBy === 'external'
          ? []
          : spec.queueProducer
            ? [
                {
                  name: spec.queueProducer.binding,
                  queueName: spec.queueProducer.queueName,
                },
              ]
            : [];
      if (
        JSON.stringify(inventoryDeployment.serviceBindings ?? []) !==
          JSON.stringify(expectedServiceBindings) ||
        JSON.stringify(inventoryDeployment.queueProducerBindings ?? []) !==
          JSON.stringify(expectedQueueBindings)
      ) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'binding-drift',
          detail: `release '${inventoryDeployment.scriptName}' has drifted trusted channel bindings`,
        });
      }
    }
    let maintenanceSecret: string;
    try {
      maintenanceSecret = options.maintenanceSecretFor(record);
    } catch (error) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'audit-error',
        detail: `maintenance secret resolver failed: ${String(error)}`,
      });
      continue;
    }
    if (record.platformResources) {
      const groupId = externalPlatformResourceGroupId(spec);
      const platformExpectations = [
        {
          role: 'platform-state' as const,
          snapshot: record.platformResources.stateWorker,
          backend:
            record.platformResources.stateWorker.plane === 'dispatch'
              ? ('workers-for-platforms' as const)
              : ('plain-worker' as const),
        },
        ...(record.platformResources.egressProxy
          ? [
              {
                role: 'deployment-egress' as const,
                snapshot: record.platformResources.egressProxy,
                backend: 'plain-worker' as const,
              },
            ]
          : []),
      ];
      for (const expected of platformExpectations) {
        const matches =
          liveByScript.get(
            `${expected.backend}:${expected.snapshot.scriptName}`,
          ) ?? [];
        const resource = matches[0];
        if (matches.length !== 1 || !resource) continue;
        if (
          resource.resourceRole !== expected.role ||
          resource.resourceGroupId !== groupId ||
          resource.tenantTag !== record.tenantTag ||
          resource.environment !== record.environment ||
          resource.artifactVersion !== expected.snapshot.artifactVersion
        ) {
          findings.push({
            tenantTag: record.tenantTag,
            environment: record.environment,
            kind: 'version-drift',
            detail: `trusted Worker '${expected.snapshot.scriptName}' has drifted ownership or artifact metadata`,
          });
        }
        if (expected.role === 'platform-state') {
          const expectedDoKeys =
            record.platformResources.stateWorker.durableObjectBindings
              .map(
                (binding) =>
                  `${binding.name}:${binding.className}:${binding.namespaceId}`,
              )
              .sort();
          const liveDoKeys = resource.durableObjectBindings
            .map(
              (binding) =>
                `${binding.name}:${binding.className}:${binding.namespaceId}`,
            )
            .sort();
          if (
            resource.databaseIds.length !== 1 ||
            resource.databaseIds[0] !== record.databaseId ||
            JSON.stringify(expectedDoKeys) !== JSON.stringify(liveDoKeys) ||
            JSON.stringify(resource.serviceBindings ?? []) !==
              JSON.stringify(
                record.platformResources.sharedOutboundWorkerName
                  ? [
                      {
                        name: 'OUTBOUND_PROXY',
                        service:
                          record.platformResources.sharedOutboundWorkerName,
                        entrypoint: 'StateEgress',
                      },
                    ]
                  : record.platformResources.egressProxy
                    ? [
                        {
                          name: 'EGRESS_PROXY',
                          service:
                            record.platformResources.egressProxy.scriptName,
                        },
                      ]
                    : [],
              ) ||
            JSON.stringify(resource.queueProducerBindings ?? []) !==
              JSON.stringify(
                record.platformResources.auditQueueName
                  ? [
                      {
                        name: 'AUDIT_QUEUE',
                        queueName: record.platformResources.auditQueueName,
                      },
                    ]
                  : [],
              ) ||
            JSON.stringify(resource.secretNames) !==
              JSON.stringify(
                [
                  'DEPLOYMENT_IDENTITY_SECRET',
                  'MAINTENANCE_ADMIN_SECRET',
                  ...(record.platformResources.sharedOutboundWorkerName
                    ? ['OUTBOUND_PROXY_CREDENTIAL']
                    : []),
                ].sort(),
              ) ||
            resource.plainTextBindings?.FLEET_DEPLOYMENT_SCRIPT !==
              spec.scriptName ||
            resource.plainTextBindings?.FLEET_MAINTENANCE_CAPABILITIES !==
              'required' ||
            resource.plainTextBindings
              ?.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY !==
              record.platformResources.maintenanceCapabilityPublicKey ||
            (resource.plainTextBindings?.FLEET_AUDIT_PROXY_INGRESS ??
              undefined) !==
              (record.platformResources.auditQueueName ? 'required' : undefined)
          ) {
            findings.push({
              tenantTag: record.tenantTag,
              environment: record.environment,
              kind: 'binding-drift',
              detail: `trusted state Worker '${expected.snapshot.scriptName}' has drifted database, Durable Object, or egress bindings`,
            });
          }
        } else if (
          resource.databaseIds.length !== 0 ||
          resource.durableObjectBindings.length !== 0 ||
          (resource.serviceBindings?.length ?? 0) !== 0 ||
          resource.secretNames.length !== 0 ||
          resource.plainTextBindings?.policyId !==
            (
              record.platformResources.outboundPolicy ??
              record.platformResources.egressProxy
            )?.policyId ||
          resource.plainTextBindings?.routeHostname !==
            record.routeHostname.toLowerCase() ||
          resource.plainTextBindings?.scriptName !==
            record.platformResources.stateWorker.scriptName ||
          !options.inventory.hostRoutingKvId ||
          resource.plainTextBindings?.hostRoutingKvId !==
            options.inventory.hostRoutingKvId ||
          JSON.stringify(resource.kvNamespaceBindings ?? []) !==
            JSON.stringify([
              {
                name: 'HOSTS',
                namespaceId: options.inventory.hostRoutingKvId,
              },
            ])
        ) {
          findings.push({
            tenantTag: record.tenantTag,
            environment: record.environment,
            kind: 'binding-drift',
            detail: `trusted egress Worker '${expected.snapshot.scriptName}' has drifted policy or attribution bindings`,
          });
        }
      }
    }
    let live: Awaited<ReturnType<ProvisioningBackend['inspect']>>;
    try {
      live = await backend.inspect(
        spec,
        maintenanceSecret,
        activeArtifactVersion(record),
      );
    } catch (error) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'audit-error',
        detail: `inspection failed: ${String(error)}`,
      });
      continue;
    }
    if (!live) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'missing-deployment',
        detail: `script '${record.scriptName}' is absent`,
      });
      continue;
    }
    if (live.databaseId !== record.databaseId) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'database-mismatch',
        detail: `expected ${record.databaseId}, found ${live.databaseId}`,
      });
    }
    const databaseOwner = databases.get(live.databaseId);
    if (databaseOwner) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'duplicate-database',
        detail: `database also bound to ${databaseOwner.tenantTag}:${databaseOwner.environment}`,
      });
    } else {
      databases.set(live.databaseId, record);
    }
    for (const binding of live.durableObjectBindings) {
      const namespaceOwner = liveNamespaceOwners.get(binding.namespaceId);
      if (namespaceOwner && !duplicateNamespaceIds.has(binding.namespaceId)) {
        duplicateNamespaceIds.add(binding.namespaceId);
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'duplicate-namespace',
          detail: `namespace '${binding.namespaceId}' also bound to ${namespaceOwner.tenantTag}:${namespaceOwner.environment}`,
        });
      } else if (!namespaceOwner) {
        liveNamespaceOwners.set(binding.namespaceId, record);
      }
    }
    if (
      live.artifactVersion !== record.artifactVersion ||
      live.schemaVersion !==
        (record.activeRelease?.releaseSchemaVersion ?? record.schemaVersion)
    ) {
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'version-drift',
        detail: `expected artifact/schema ${record.artifactVersion}/${record.activeRelease?.releaseSchemaVersion ?? record.schemaVersion}, found ${live.artifactVersion}/${live.schemaVersion}`,
      });
    }
    const deployedAt = Date.parse(record.updatedAt);
    const dutyFailures = configuredDuties(live.maintenance)
      .map((duty) =>
        staleDutyReason(
          duty,
          Number.isFinite(deployedAt) ? deployedAt : undefined,
          now,
          options.staleAfterMs,
        ),
      )
      .filter((reason): reason is string => reason !== undefined);
    if (!live.maintenance.armed || dutyFailures.length > 0) {
      const reasons = [
        ...(!live.maintenance.armed
          ? ['maintenance scheduler is not armed']
          : []),
        ...dutyFailures,
      ];
      findings.push({
        tenantTag: record.tenantTag,
        environment: record.environment,
        kind: 'maintenance-stale',
        detail: reasons.join('; '),
      });
      try {
        await options.store.withDeploymentLease(
          record.tenantTag,
          record.environment,
          async (lease) => {
            const current = await options.store.get(
              record.tenantTag,
              record.environment,
            );
            if (
              !current ||
              current.phase !== record.phase ||
              current.desiredSpecDigest !== record.desiredSpecDigest ||
              current.updatedAt !== record.updatedAt
            ) {
              throw new Error(
                'deployment changed after audit inspection; maintenance re-arm aborted',
              );
            }
            await commitInvocationAuthority(
              lease,
              current,
              () => options.now ?? Date.now(),
            );
            await lease.assertOwned();
            await backend.ensureMaintenance(
              spec,
              maintenanceSecret,
              lease,
              activeArtifactVersion(record),
            );
          },
        );
      } catch (error) {
        findings.push({
          tenantTag: record.tenantTag,
          environment: record.environment,
          kind: 'audit-error',
          detail: `maintenance re-arm failed: ${String(error)}`,
        });
      }
    }
  }
  return findings;
}

export interface FleetVersionRow {
  readonly tenantTag: string;
  readonly environment: string;
  readonly backend: string;
  readonly artifactVersion: string;
  readonly schemaVersion: number;
  readonly phase: string;
}

export function fleetVersionReport(
  records: readonly FleetRecord[],
): readonly FleetVersionRow[] {
  return [...records]
    .sort((a, b) =>
      `${a.tenantTag}:${a.environment}`.localeCompare(
        `${b.tenantTag}:${b.environment}`,
      ),
    )
    .map((record) => ({
      tenantTag: record.tenantTag,
      environment: record.environment,
      backend: record.backend,
      artifactVersion: record.artifactVersion,
      schemaVersion: record.schemaVersion,
      phase: record.phase,
    }));
}

async function retireCommittedRelease(
  backend: ProvisioningBackend,
  spec: DeploymentSpec,
  database: { id: string; name: string; created: false },
  record: FleetRecord,
  lease: import('./types.js').FleetStateLease,
  clock: () => number,
): Promise<FleetRecord> {
  const retiring = record.retiringRelease;
  if (!retiring) return record;
  if (!backend.deleteRetainedRelease) {
    throw new Error(
      'immutable external backend cannot retire the expired rollback release',
    );
  }
  await lease.assertOwned();
  await backend.deleteRetainedRelease(spec, retiring, database, lease);
  const settled = { ...record };
  delete settled.retiringRelease;
  const cleared: FleetRecord = {
    ...settled,
    updatedAt: new Date(clock()).toISOString(),
  };
  await lease.put(cleared);
  return cleared;
}

export async function migrateFleet(options: {
  readonly store: FleetStateStore;
  readonly records: readonly FleetRecord[];
  readonly canaryTenantTags: readonly string[];
  readonly backendFor: (record: FleetRecord) => ProvisioningBackend;
  readonly specFor: (record: FleetRecord) => DeploymentSpec;
  readonly secretsFor: (record: FleetRecord) => DeploymentSecrets;
  readonly finalizedStateProviderFor?: (
    record: FleetRecord,
  ) => FinalizedOrdinaryStateProvider | undefined;
  /**
   * The host to hand each settled promotion to, per deployment.
   *
   * Optional, and its absence changes nothing about correctness: every promote
   * path attests what it published whether or not a host is settling, because
   * checking its own work is the package's obligation rather than a service it
   * performs for a caller.
   *
   * `provisionDeployment` deliberately consults nothing like this. A first
   * deploy returns synchronously to the caller that asked for it, so the host
   * already knows the moment it went live and can settle after the call using
   * `attestFleetRecordActiveRoute`; an in-lease settlement point there would
   * add a callback into the critical section to tell a caller something it is
   * about to be told anyway.
   */
  readonly settlementFor?: (
    record: FleetRecord,
  ) => FleetSettlementHost | undefined;
  /**
   * Tuning for the convergence wait each post-promote attestation performs.
   * The defaults suit every provider this package targets; a caller overrides
   * them to bound the wait differently or to drive it from an injected clock.
   */
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  readonly clock?: () => number;
}): Promise<readonly FleetRecord[]> {
  const canaryOrder = new Map(
    options.canaryTenantTags.map((tenantTag, index) => [tenantTag, index]),
  );
  const ordered = [...options.records].sort((a, b) => {
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
  const attestationOptions: AttestConvergedActiveRouteOptions = {
    clock: options.clock ?? Date.now,
    ...options.routeAttestation,
  };
  const updated: FleetRecord[] = [];
  for (const record of ordered) {
    const next = await options.store.withDeploymentLease(
      record.tenantTag,
      record.environment,
      async (lease) => {
        let stored = await options.store.get(
          record.tenantTag,
          record.environment,
        );
        if (!stored) throw new Error('fleet migration record disappeared');
        assertNoActiveDecommission(stored, 'migrateFleet');
        assertNoActiveCleanup(stored, 'migrateFleet');
        assertBackendSwitchInactive(stored);
        const storedSchemaVersion = stored.schemaVersion;
        const backend = options.backendFor(stored);
        const spec = options.specFor(stored);
        const secrets = options.secretsFor(stored);
        const finalizedOrdinaryState =
          stored.backendSwitchIntent?.subphase === 'finalized' &&
          stored.platformResources?.stateWorker.plane === 'ordinary';
        const finalizedStateProvider = finalizedOrdinaryState
          ? options.finalizedStateProviderFor?.(stored)
          : undefined;
        if (finalizedOrdinaryState) {
          finalizedBridgeForRecord(stored);
          if (!finalizedStateProvider) {
            throw new Error(
              'finalized ordinary state requires its backend-switch provider',
            );
          }
        }
        validateDeploymentSpec(spec);
        validateDeploymentSecrets(spec, secrets);
        assertImmutableDeploymentMapping(stored, backend, spec);
        assertPlatformDurableObjectHistory(stored, spec);
        if (stored.phase !== 'ready' && stored.phase !== 'migrating') {
          throw new Error(
            `cannot migrate deployment in phase '${stored.phase}'`,
          );
        }
        const targetDigest = deploymentSpecDigest(spec);
        const immutableExternal =
          backend.immutableExternalArtifacts === true &&
          spec.authoredBy === 'external';
        const targetPhysicalScriptName = immutableExternal
          ? backend.releaseScriptName?.(spec)
          : undefined;
        if (immutableExternal && !targetPhysicalScriptName) {
          throw new Error(
            'immutable external backend did not provide a physical release name',
          );
        }
        if (
          spec.migrations.some(
            (migration) =>
              migration.version > storedSchemaVersion &&
              migration.rollbackCompatible !== true,
          )
        ) {
          throw new Error(
            'staged D1 migrations must attest rollbackCompatible before candidate creation',
          );
        }
        const targetRelease: ExternalReleaseSnapshot | undefined =
          targetPhysicalScriptName
            ? {
                physicalScriptName: targetPhysicalScriptName,
                specDigest: targetDigest,
                artifactVersion: 'pending',
                releaseSchemaVersion: spec.schemaVersion,
                application: applicationBindingTopology(
                  spec,
                  stored.applicationResources ?? [],
                ),
              }
            : undefined;
        const targetPlatform = immutableExternal
          ? finalizedStateProvider
            ? finalizedStateProvider.describeFinalizedBridgeTarget(spec, stored)
            : describeExternalPlatformTarget(backend, spec)
          : undefined;
        if (stored.platformTarget && targetPlatform) {
          assertExternalPlatformTargetCompatibility(
            stored.platformTarget,
            targetPlatform,
          );
        }
        const platformOnlyTarget = targetPlatform
          ? effectiveAppliedPlatformTarget(stored, targetPlatform)
          : undefined;
        const platformOnlyChange =
          stored.desiredSpecDigest === targetDigest &&
          platformOnlyTarget !== undefined &&
          stored.platformTarget !== undefined &&
          JSON.stringify(stored.platformTarget) !==
            JSON.stringify(platformOnlyTarget);
        if (
          stored.phase === 'migrating' &&
          (immutableExternal
            ? stored.migrationIntent?.platformOnly === true
              ? stored.migrationIntent.targetSpecDigest !== targetDigest ||
                JSON.stringify(stored.migrationIntent.target) !==
                  JSON.stringify(platformOnlyTarget)
              : stored.pendingRelease?.specDigest !== targetDigest ||
                stored.pendingRelease?.physicalScriptName !==
                  targetPhysicalScriptName ||
                stored.pendingRelease.releaseSchemaVersion !==
                  spec.schemaVersion ||
                stored.migrationIntent?.targetSpecDigest !== targetDigest
            : stored.pendingSpecDigest !== targetDigest)
        ) {
          throw new Error(
            'migration retry uses a different desired specification',
          );
        }
        if (spec.previousDurableObjectTag !== stored.durableObjectTag) {
          throw new Error(
            `Durable Object migration base mismatch for ${stored.tenantTag}:${stored.environment}: expected '${stored.durableObjectTag ?? 'none'}'`,
          );
        }
        const database = await reconcilePersistedDatabase(
          backend,
          stored,
          false,
          lease,
        );
        if (!database) {
          throw new Error(
            `persisted database '${stored.databaseId}' is absent`,
          );
        }
        if (stored.phase === 'ready' && stored.retiringRelease) {
          stored = await retireCommittedRelease(
            backend,
            spec,
            database,
            stored,
            lease,
            options.clock ?? Date.now,
          );
        }
        if (
          stored.phase === 'ready' &&
          stored.desiredSpecDigest === targetDigest &&
          !platformOnlyChange
        ) {
          if (targetPlatform) {
            const rollbackCompatibleTarget =
              platformOnlyTarget ?? targetPlatform;
            if (!stored.platformTarget) {
              if (!stored.platformResources) {
                throw new Error(
                  'ready external deployment has no trusted platform resources',
                );
              }
              assertPlatformResourcesMatchTarget(
                stored.platformResources,
                targetPlatform,
              );
              stored = {
                ...stored,
                platformTarget: targetPlatform,
                outboundPolicy: targetPlatform.outboundPolicy,
                updatedAt: new Date(
                  (options.clock ?? Date.now)(),
                ).toISOString(),
              };
              await lease.put(stored);
            }
            assertExternalPlatformTarget(
              stored.platformTarget,
              rollbackCompatibleTarget,
              'ready deployment',
            );
            stored = await convergeExternalPlatformResources(
              backend,
              spec,
              database,
              secrets,
              rollbackCompatibleTarget,
              stored,
              lease,
              options.clock ?? Date.now,
              finalizedStateProvider,
            );
          }
          const live = await backend.inspect(
            spec,
            secrets.maintenanceAdmin,
            activeArtifactVersion(stored),
          );
          if (!live) throw new Error('ready migration target is missing');
          assertLiveDeploymentMatches(
            live,
            stored,
            spec,
            targetDigest,
            stored.activeRelease?.application,
          );
          if (immutableExternal && stored.activeRelease) {
            assertExternalReleaseArtifactVersion(
              live,
              stored.activeRelease,
              'ready migration',
            );
          }
          if (
            targetRelease &&
            (stored.activeRelease?.physicalScriptName !==
              targetRelease.physicalScriptName ||
              stored.activeRelease.releaseSchemaVersion !==
                targetRelease.releaseSchemaVersion ||
              stored.activeRelease.artifactVersion !== live.artifactVersion)
          ) {
            throw new Error(
              'ready immutable release metadata does not exactly match the target',
            );
          }
          let maintenance = live.maintenance;
          if (!maintenance.armed) {
            stored = await commitInvocationAuthority(
              lease,
              stored,
              options.clock ?? Date.now,
            );
            await lease.assertOwned();
            maintenance = await backend.ensureMaintenance(
              spec,
              secrets.maintenanceAdmin,
              lease,
              activeArtifactVersion(stored),
            );
          }
          if (!maintenance.armed) throw new Error('maintenance did not re-arm');
          stored = await commitInvocationAuthority(
            lease,
            stored,
            options.clock ?? Date.now,
          );
          await lease.assertOwned();
          await backend.promoteWorker(
            spec,
            buildPromotionGuard(
              stored,
              targetPhysicalScriptName ?? spec.scriptName,
            ),
            stored.outboundPolicy,
            lease,
            activeArtifactVersion(stored),
          );
          // The steady-state path: an unchanged deployment reconciled again.
          // It re-promotes because a crash could have left the route behind,
          // so it must re-attest — but it must not re-settle, or a fleet on a
          // reconcile schedule would settle forever.
          const convergence = await settlePromotedRoute({
            backend,
            spec,
            record: stored,
            entry: 'ready-convergence',
            target: stored.activeRelease,
            prior: stored.rollbackRelease,
            expectedSpecDigest: targetDigest,
            expectedArtifactVersion: activeArtifactVersion(stored),
            settlementHost: options.settlementFor?.(stored),
            attestation: attestationOptions,
            skipWhenAlreadySettled: true,
          });
          if (convergence.settled) {
            stored = {
              ...stored,
              settledSettlementKey: convergence.settlementKey,
              updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
            };
            await lease.put(stored);
          }
          return retireCommittedRelease(
            backend,
            spec,
            database,
            stored,
            lease,
            options.clock ?? Date.now,
          );
        }
        if (
          spec.schemaVersion < stored.schemaVersion &&
          !platformOnlyChange &&
          stored.migrationIntent?.platformOnly !== true
        ) {
          throw new Error(
            `schema downgrade refused for ${stored.tenantTag}:${stored.environment}`,
          );
        }
        if (immutableExternal && !stored.activeRelease) {
          throw new Error(
            'immutable external migration has no durable active release metadata',
          );
        }
        if (
          immutableExternal &&
          targetRelease &&
          targetPlatform &&
          (!stored.platformTarget || !stored.outboundPolicy)
        ) {
          throw new Error(
            'immutable external migration has no durable prior platform target and policy',
          );
        }
        const externalIntent: ExternalMigrationIntent | undefined =
          immutableExternal && targetRelease && targetPlatform
            ? platformOnlyChange
              ? {
                  platformOnly: true,
                  targetSpecDigest: targetDigest,
                  priorRelease: stored.activeRelease as ExternalReleaseSnapshot,
                  priorTarget:
                    stored.platformTarget as ExternalPlatformTargetDescription,
                  priorOutboundPolicy:
                    stored.outboundPolicy as DeploymentEgressPolicy,
                  targetRelease:
                    stored.activeRelease as ExternalReleaseSnapshot,
                  target:
                    platformOnlyTarget as ExternalPlatformTargetDescription,
                  subphase: 'planned',
                }
              : {
                  targetSpecDigest: targetDigest,
                  priorRelease: stored.activeRelease as ExternalReleaseSnapshot,
                  priorTarget:
                    stored.platformTarget as ExternalPlatformTargetDescription,
                  priorOutboundPolicy:
                    stored.outboundPolicy as DeploymentEgressPolicy,
                  targetRelease,
                  target: targetPlatform,
                  subphase: 'planned',
                }
            : undefined;
        let migrationRecord: FleetRecord =
          stored.phase === 'ready'
            ? {
                ...stored,
                phase: 'migrating',
                ...(externalIntent?.platformOnly
                  ? { migrationIntent: externalIntent }
                  : targetRelease
                    ? {
                        pendingRelease: targetRelease,
                        migrationPriorRelease: stored.activeRelease,
                        migrationIntent: externalIntent,
                      }
                    : {}),
                ...(!targetRelease ? { pendingSpecDigest: targetDigest } : {}),
                updatedAt: new Date(
                  (options.clock ?? Date.now)(),
                ).toISOString(),
              }
            : stored;
        if (stored.phase === 'ready') await lease.put(migrationRecord);
        if (
          immutableExternal &&
          (!migrationRecord.migrationIntent ||
            (migrationRecord.migrationIntent.platformOnly !== true &&
              (!migrationRecord.migrationPriorRelease ||
                !migrationRecord.pendingRelease)))
        ) {
          throw new Error(
            'immutable external migration lost its durable release intent',
          );
        }
        if (targetPlatform && migrationRecord.migrationIntent) {
          assertExternalPlatformTarget(
            migrationRecord.migrationIntent.target,
            migrationRecord.migrationIntent.platformOnly === true
              ? (platformOnlyTarget as ExternalPlatformTargetDescription)
              : targetPlatform,
            'migration retry',
          );
        }
        if (finalizedStateProvider && targetPlatform) {
          const finalizedPlan = finalizedStateProvider.describeFinalizedState({
            targetSpec: spec,
            currentRecord: migrationRecord,
            target: migrationRecord.migrationIntent?.target ?? targetPlatform,
          });
          await finalizedStateProvider.assertFinalizedState({
            targetSpec: spec,
            currentRecord: migrationRecord,
            target: migrationRecord.migrationIntent?.target ?? targetPlatform,
            plan: finalizedPlan,
            fence: lease,
          });
        }
        if (migrationRecord.migrationIntent?.platformOnly === true) {
          const platformMigrationTarget =
            migrationRecord.migrationIntent.target;
          const platformMigrationRelease =
            migrationRecord.migrationIntent.targetRelease;
          if (migrationRecord.migrationIntent.subphase === 'planned') {
            migrationRecord = {
              ...migrationRecord,
              migrationIntent: {
                ...migrationRecord.migrationIntent,
                subphase: 'schema-applied',
              },
              updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
            };
            await lease.put(migrationRecord);
          }
          migrationRecord = await convergeExternalPlatformResources(
            backend,
            spec,
            database,
            secrets,
            platformMigrationTarget,
            migrationRecord,
            lease,
            options.clock ?? Date.now,
            finalizedStateProvider,
          );
          if (migrationRecord.migrationIntent?.subphase === 'schema-applied') {
            migrationRecord = {
              ...migrationRecord,
              migrationIntent: {
                ...migrationRecord.migrationIntent,
                subphase: 'platform-applied',
              },
              updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
            };
            await lease.put(migrationRecord);
          }
          const maintenancePreflight = await backend.inspect(
            spec,
            secrets.maintenanceAdmin,
            platformMigrationRelease.artifactVersion,
          );
          if (!maintenancePreflight) {
            throw new Error('platform-only migration release is missing');
          }
          assertLiveDeploymentMatches(
            maintenancePreflight,
            stored,
            spec,
            targetDigest,
            platformMigrationRelease.application,
          );
          assertExternalReleaseArtifactVersion(
            maintenancePreflight,
            platformMigrationRelease,
            'platform-only maintenance',
          );
          migrationRecord = await commitInvocationAuthority(
            lease,
            migrationRecord,
            options.clock ?? Date.now,
          );
          await lease.assertOwned();
          const maintenance = await backend.ensureMaintenance(
            spec,
            secrets.maintenanceAdmin,
            lease,
            platformMigrationRelease.artifactVersion,
          );
          if (!maintenance.armed) {
            throw new Error(
              'platform-only migration maintenance is unarmed before route publication',
            );
          }
          if (
            migrationRecord.migrationIntent?.subphase === 'platform-applied'
          ) {
            const publicationPreflight = await backend.inspect(
              spec,
              secrets.maintenanceAdmin,
              platformMigrationRelease.artifactVersion,
            );
            if (!publicationPreflight) {
              throw new Error('platform-only migration release is missing');
            }
            assertLiveDeploymentMatches(
              publicationPreflight,
              stored,
              spec,
              targetDigest,
              platformMigrationRelease.application,
            );
            assertExternalReleaseArtifactVersion(
              publicationPreflight,
              platformMigrationRelease,
              'platform-only publication',
            );
            // No flip here: the unconditional maintenance flip above already
            // committed the carrier durably earlier in this same call.
            await lease.assertOwned();
            await backend.promoteWorker(
              spec,
              buildPromotionGuard(
                migrationRecord,
                targetPhysicalScriptName ?? spec.scriptName,
              ),
              platformMigrationTarget.outboundPolicy,
              lease,
              platformMigrationRelease.artifactVersion,
            );
            migrationRecord = {
              ...migrationRecord,
              migrationIntent: {
                ...migrationRecord.migrationIntent,
                subphase: 'route-published',
              },
              updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
            };
            await lease.put(migrationRecord);
          }
          const live = await backend.inspect(
            spec,
            secrets.maintenanceAdmin,
            platformMigrationRelease.artifactVersion,
          );
          if (!live)
            throw new Error('platform-only migration release is missing');
          assertLiveDeploymentMatches(
            live,
            stored,
            spec,
            targetDigest,
            platformMigrationRelease.application,
          );
          assertExternalReleaseArtifactVersion(
            live,
            platformMigrationRelease,
            'platform-only settlement',
          );
          const platformSettlement = await settlePromotedRoute({
            backend,
            spec,
            record: migrationRecord,
            entry: 'platform-only',
            target: platformMigrationRelease,
            prior: migrationRecord.rollbackRelease,
            expectedSpecDigest: targetDigest,
            expectedArtifactVersion: platformMigrationRelease.artifactVersion,
            settlementHost: options.settlementFor?.(migrationRecord),
            attestation: attestationOptions,
          });
          const settled = { ...migrationRecord };
          delete settled.migrationIntent;
          const migrated: FleetRecord = {
            ...settled,
            phase: 'ready',
            platformTarget: platformMigrationTarget,
            outboundPolicy: platformMigrationTarget.outboundPolicy,
            ...(platformSettlement.settled
              ? { settledSettlementKey: platformSettlement.settlementKey }
              : {}),
            updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
          };
          await lease.put(migrated);
          return migrated;
        }
        await lease.assertOwned();
        // Re-stamping a database this deployment already owns: the ownership
        // sentinel short-circuits, and the only thing that can still happen is
        // the fence row being CREATED where none exists.
        //
        // 'open' is hard-coded, and migrateFleet takes no fence option, for one
        // reason: the deployment being migrated is `ready` or `migrating` — it
        // is EXECUTING right now. A pre-0.20 database has no fence row and
        // therefore reads as open; materializing that row must record what the
        // deployment already IS, not impose something new. Seeding
        // 'migration-locked' here would silently stop a live deployment in the
        // middle of its own migration. Closing a fence is an operator action
        // through POST /admin/execution-fence, never a side effect of a
        // schema pass.
        await backend.seedDeploymentIdentity(
          database,
          stored.tenantTag,
          lease,
          {
            initialExecutionFenceState: 'open',
          },
        );
        const pendingMigrations = spec.migrations.filter(
          (candidate) => candidate.version > migrationRecord.schemaVersion,
        );
        if (pendingMigrations.length === 0) {
          await lease.assertOwned();
          await backend.applyMigrations(database, spec.migrations, lease);
        }
        for (const migration of pendingMigrations) {
          await lease.assertOwned();
          await backend.applyMigrations(
            database,
            spec.migrations.slice(0, migration.version),
            lease,
          );
          migrationRecord = {
            ...migrationRecord,
            schemaVersion: migration.version,
            updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
          };
          await lease.put(migrationRecord);
        }
        if (migrationRecord.schemaVersion !== spec.schemaVersion) {
          throw new Error(
            `missing D1 migration path from ${stored.schemaVersion} to ${spec.schemaVersion}`,
          );
        }
        if (migrationRecord.migrationIntent?.subphase === 'planned') {
          migrationRecord = {
            ...migrationRecord,
            migrationIntent: {
              ...migrationRecord.migrationIntent,
              subphase: 'schema-applied',
            },
            updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
          };
          await lease.put(migrationRecord);
        }
        if (targetPlatform) {
          migrationRecord = await convergeExternalPlatformResources(
            backend,
            spec,
            database,
            secrets,
            migrationRecord.migrationIntent?.target ?? targetPlatform,
            migrationRecord,
            lease,
            options.clock ?? Date.now,
            finalizedStateProvider,
          );
          if (migrationRecord.migrationIntent?.subphase === 'schema-applied') {
            migrationRecord = {
              ...migrationRecord,
              migrationIntent: {
                ...migrationRecord.migrationIntent,
                subphase: 'platform-applied',
              },
              updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
            };
            await lease.put(migrationRecord);
          }
        }
        if (
          migrationRecord.pendingRelease &&
          migrationRecord.platformResources
        ) {
          const topology = externalReleaseTopology(
            spec,
            migrationRecord.platformResources,
            migrationRecord.applicationResources,
          );
          migrationRecord = {
            ...migrationRecord,
            pendingRelease: { ...migrationRecord.pendingRelease, topology },
            ...(migrationRecord.migrationIntent
              ? {
                  migrationIntent: {
                    ...migrationRecord.migrationIntent,
                    targetRelease: {
                      ...migrationRecord.migrationIntent.targetRelease,
                      topology,
                    },
                  },
                }
              : {}),
            updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
          };
          await lease.put(migrationRecord);
        }
        let live: LiveDeployment | undefined;
        if (migrationRecord.migrationIntent?.subphase !== 'platform-applied') {
          live = await backend.inspect(
            spec,
            secrets.maintenanceAdmin,
            pendingArtifactVersion(migrationRecord),
          );
        }
        if (
          migrationRecord.pendingRelease &&
          migrationRecord.pendingRelease.artifactVersion !== 'pending'
        ) {
          assertExternalReleaseArtifactVersion(
            live,
            migrationRecord.pendingRelease,
            'migration candidate',
          );
        }
        if (
          migrationRecord.migrationIntent?.subphase === 'platform-applied' ||
          !live ||
          live.desiredSpecDigest !== targetDigest
        ) {
          migrationRecord = await commitInvocationAuthority(
            lease,
            migrationRecord,
            options.clock ?? Date.now,
          );
          await lease.assertOwned();
          const deployed = await backend.deployWorker(
            spec,
            database,
            secrets,
            migrationRecord.platformResources,
            lease,
            migrationRecord.pendingRelease?.artifactVersion ??
              migrationRecord.pendingArtifactVersion ??
              (immutableExternal ? 'pending' : undefined),
            migrationRecord.migrationIntent?.targetRelease.application ??
              migrationRecord.pendingRelease?.application ??
              applicationBindingTopology(
                spec,
                migrationRecord.applicationResources ?? [],
              ),
          );
          if (
            targetPhysicalScriptName &&
            deployed.physicalScriptName !== targetPhysicalScriptName
          ) {
            throw new Error('backend deployed an unexpected physical release');
          }
        }
        live = await backend.inspect(
          spec,
          secrets.maintenanceAdmin,
          pendingArtifactVersion(migrationRecord),
        );
        if (!live) throw new Error('migration candidate is missing');
        assertLiveDeploymentMatches(
          live,
          stored,
          spec,
          targetDigest,
          migrationRecord.migrationIntent?.targetRelease.application ??
            migrationRecord.pendingRelease?.application,
        );
        if (migrationRecord.pendingRelease) {
          assertExternalReleaseArtifactVersion(
            live,
            migrationRecord.pendingRelease,
            'migration candidate',
          );
        }
        if (
          targetPhysicalScriptName &&
          live.scriptName !== targetPhysicalScriptName
        ) {
          throw new Error(
            'migration candidate has an unexpected physical name',
          );
        }
        if (
          migrationRecord.migrationIntent &&
          migrationRecord.pendingRelease?.artifactVersion === 'pending'
        ) {
          const intendedTopology = migrationRecord.pendingRelease.topology;
          if (!intendedTopology) {
            throw new Error(
              'migration candidate has no intended binding topology',
            );
          }
          const pendingRelease = {
            ...migrationRecord.migrationIntent.targetRelease,
            artifactVersion: live.artifactVersion,
            topology: externalReleaseTopologyFromLive(live, intendedTopology),
          };
          migrationRecord = {
            ...migrationRecord,
            pendingRelease,
            migrationIntent: {
              ...migrationRecord.migrationIntent,
              targetRelease: pendingRelease,
              subphase: 'candidate-deployed',
            },
            updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
          };
          await lease.put(migrationRecord);
        } else if (
          !immutableExternal &&
          migrationRecord.pendingArtifactVersion === undefined
        ) {
          migrationRecord = {
            ...migrationRecord,
            pendingArtifactVersion: live.artifactVersion,
            updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
          };
          await lease.put(migrationRecord);
        }
        migrationRecord = await commitInvocationAuthority(
          lease,
          migrationRecord,
          options.clock ?? Date.now,
        );
        await lease.assertOwned();
        const maintenance = await backend.ensureMaintenance(
          spec,
          secrets.maintenanceAdmin,
          lease,
          pendingArtifactVersion(migrationRecord),
        );
        if (!maintenance.armed) throw new Error('maintenance did not re-arm');
        if (
          migrationRecord.migrationIntent?.subphase === 'candidate-deployed'
        ) {
          migrationRecord = {
            ...migrationRecord,
            migrationIntent: {
              ...migrationRecord.migrationIntent,
              subphase: 'candidate-armed',
            },
            updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
          };
          await lease.put(migrationRecord);
        }
        const publicationPreflight = await backend.inspect(
          spec,
          secrets.maintenanceAdmin,
          pendingArtifactVersion(migrationRecord),
        );
        if (!publicationPreflight) {
          throw new Error('migration candidate is missing before publication');
        }
        assertLiveDeploymentMatches(
          publicationPreflight,
          stored,
          spec,
          targetDigest,
          migrationRecord.migrationIntent?.targetRelease.application ??
            migrationRecord.pendingRelease?.application,
        );
        if (migrationRecord.pendingRelease) {
          assertExternalReleaseArtifactVersion(
            publicationPreflight,
            migrationRecord.pendingRelease,
            'migration publication',
          );
        }
        // No flip here: the unconditional candidate-maintenance flip above
        // already committed the carrier durably earlier in this same call.
        await lease.assertOwned();
        await backend.promoteWorker(
          spec,
          buildPromotionGuard(
            migrationRecord,
            targetPhysicalScriptName ?? spec.scriptName,
          ),
          migrationRecord.migrationIntent?.target.outboundPolicy ??
            migrationRecord.outboundPolicy,
          lease,
          pendingArtifactVersion(migrationRecord),
        );
        if (migrationRecord.migrationIntent) {
          migrationRecord = {
            ...migrationRecord,
            migrationIntent: {
              ...migrationRecord.migrationIntent,
              subphase: 'route-published',
            },
            updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
          };
          await lease.put(migrationRecord);
        }
        live = await backend.inspect(
          spec,
          secrets.maintenanceAdmin,
          pendingArtifactVersion(migrationRecord),
        );
        if (!live) {
          throw new Error(
            `deployment did not converge after migration for ${stored.tenantTag}:${stored.environment}`,
          );
        }
        assertLiveDeploymentMatches(
          live,
          stored,
          spec,
          targetDigest,
          migrationRecord.migrationIntent?.targetRelease.application ??
            migrationRecord.pendingRelease?.application,
        );
        if (migrationRecord.pendingRelease) {
          assertExternalReleaseArtifactVersion(
            live,
            migrationRecord.pendingRelease,
            'migration settlement',
          );
        }
        if (
          targetPhysicalScriptName &&
          live.scriptName !== targetPhysicalScriptName
        ) {
          throw new Error('promoted release has an unexpected physical name');
        }
        const rollbackRelease = migrationRecord.migrationPriorRelease;
        const retiringRelease = targetPhysicalScriptName
          ? stored.rollbackRelease
          : undefined;
        const committedTargetRelease = migrationRecord.pendingRelease;
        if (
          targetPhysicalScriptName &&
          (!committedTargetRelease ||
            committedTargetRelease.physicalScriptName !==
              targetPhysicalScriptName ||
            !committedTargetRelease.topology)
        ) {
          throw new Error(
            'promoted release has no exact persisted binding topology',
          );
        }
        const migrationSettlement = await settlePromotedRoute({
          backend,
          spec,
          record: migrationRecord,
          entry: 'migration',
          target: committedTargetRelease,
          prior: rollbackRelease,
          expectedSpecDigest: targetDigest,
          expectedArtifactVersion: live.artifactVersion,
          settlementHost: options.settlementFor?.(migrationRecord),
          attestation: attestationOptions,
        });
        const settled = { ...migrationRecord };
        delete settled.pendingRelease;
        delete settled.migrationPriorRelease;
        delete settled.pendingSpecDigest;
        delete settled.pendingArtifactVersion;
        delete settled.migrationIntent;
        const migrated: FleetRecord = {
          ...settled,
          phase: 'ready',
          desiredSpecDigest: targetDigest,
          schemaVersion: spec.schemaVersion,
          artifactVersion: live.artifactVersion,
          ...(targetPhysicalScriptName
            ? {
                activeRelease:
                  committedTargetRelease as ExternalReleaseSnapshot,
                rollbackRelease,
                ...(retiringRelease ? { retiringRelease } : {}),
              }
            : {}),
          ...(targetPlatform
            ? {
                platformTarget: targetPlatform,
                outboundPolicy: targetPlatform.outboundPolicy,
              }
            : {}),
          durableObjectTag: finalizedStateProvider
            ? migrationRecord.durableObjectTag
            : targetDurableObjectTag(spec),
          ...(spec.authoredBy === 'platform'
            ? {
                durableObjectMigrationHistory:
                  canonicalDurableObjectMigrationHistory(
                    spec.durableObjectMigrations,
                  ),
                durableObjectMigrationHistoryDigest:
                  durableObjectMigrationHistoryDigest(
                    spec.durableObjectMigrations,
                  ),
              }
            : {}),
          durableObjectBindings: live.durableObjectBindings,
          applicationBindings:
            committedTargetRelease?.application ??
            applicationBindingTopology(
              spec,
              migrationRecord.applicationResources ?? [],
            ),
          ...(migrationSettlement.settled
            ? { settledSettlementKey: migrationSettlement.settlementKey }
            : {}),
          updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
        };
        await lease.put(migrated);
        return retireCommittedRelease(
          backend,
          spec,
          database,
          migrated,
          lease,
          options.clock ?? Date.now,
        );
      },
    );
    updated.push(next);
  }
  return updated;
}

export async function rollbackExternalRelease(options: {
  readonly store: FleetStateStore;
  readonly backend: ProvisioningBackend;
  readonly currentSpec: DeploymentSpec;
  readonly rollbackSpec: DeploymentSpec;
  readonly secrets: DeploymentSecrets;
  readonly finalizedStateProvider?: FinalizedOrdinaryStateProvider;
  /**
   * The host to hand the reversal to. Singular because a rollback names one
   * deployment, where `migrateFleet` sweeps many.
   */
  readonly settlement?: FleetSettlementHost;
  /** Tuning for the convergence wait the post-promote attestation performs. */
  readonly routeAttestation?: AttestConvergedActiveRouteOptions;
  readonly clock?: () => number;
}): Promise<FleetRecord> {
  const { store, backend, currentSpec, rollbackSpec, secrets } = options;
  validateDeploymentSpec(currentSpec);
  validateDeploymentSpec(rollbackSpec);
  validateDeploymentSecrets(rollbackSpec, secrets);
  if (
    backend.immutableExternalArtifacts !== true ||
    currentSpec.authoredBy !== 'external' ||
    rollbackSpec.authoredBy !== 'external' ||
    !backend.releaseScriptName
  ) {
    throw new Error(
      'rollbackExternalRelease requires immutable external Workers for Platforms specs',
    );
  }
  const releaseScriptName = backend.releaseScriptName;
  return store.withDeploymentLease(
    currentSpec.tenantTag,
    currentSpec.environment,
    async (lease) => {
      let stored = await store.get(
        currentSpec.tenantTag,
        currentSpec.environment,
      );
      if (!stored) throw new Error('rollback deployment is not registered');
      assertNoActiveDecommission(stored, 'rollbackExternalRelease');
      assertNoActiveCleanup(stored, 'rollbackExternalRelease');
      assertBackendSwitchInactive(stored);
      const finalizedOrdinaryState =
        stored.backendSwitchIntent?.subphase === 'finalized' &&
        stored.platformResources?.stateWorker.plane === 'ordinary';
      const finalizedStateProvider = finalizedOrdinaryState
        ? options.finalizedStateProvider
        : undefined;
      if (finalizedOrdinaryState) {
        finalizedBridgeForRecord(stored);
        if (!finalizedStateProvider) {
          throw new Error(
            'finalized ordinary state requires its backend-switch provider',
          );
        }
      }
      const database = await reconcilePersistedDatabase(
        backend,
        stored,
        false,
        lease,
      );
      if (!database) {
        throw new Error(`persisted database '${stored.databaseId}' is absent`);
      }
      let currentPlatformTarget: ExternalPlatformTargetDescription;
      if (finalizedOrdinaryState) {
        if (!finalizedStateProvider) {
          throw new Error(
            'finalized ordinary state requires its backend-switch provider',
          );
        }
        currentPlatformTarget =
          finalizedStateProvider.describeFinalizedBridgeTarget(
            currentSpec,
            stored,
          );
      } else {
        currentPlatformTarget = effectiveAppliedPlatformTarget(
          stored,
          describeExternalPlatformTarget(backend, currentSpec),
        );
      }
      assertExternalPlatformTarget(
        stored.platformTarget,
        currentPlatformTarget,
        'rollback deployment',
      );
      if (!stored.platformResources) {
        throw new Error(
          'rollback deployment has no trusted platform resources',
        );
      }
      assertPlatformResourcesMatchTarget(
        stored.platformResources,
        currentPlatformTarget,
      );
      if (stored.phase === 'ready' && stored.retiringRelease) {
        stored = await retireCommittedRelease(
          backend,
          currentSpec,
          database,
          stored,
          lease,
          options.clock ?? Date.now,
        );
      }
      assertImmutableDeploymentMapping(stored, backend, currentSpec);
      if (stored.phase !== 'ready' && stored.phase !== 'rolling-back') {
        throw new Error(
          `cannot roll back deployment in phase '${stored.phase}'`,
        );
      }
      const currentDigest = deploymentSpecDigest(currentSpec);
      if (currentDigest !== stored.desiredSpecDigest) {
        throw new Error('current rollback specification does not match state');
      }
      if (
        !stored.activeRelease ||
        stored.activeRelease.specDigest !== currentDigest ||
        stored.activeRelease.physicalScriptName !==
          releaseScriptName(currentSpec) ||
        stored.activeRelease.releaseSchemaVersion !== currentSpec.schemaVersion
      ) {
        throw new Error('current active release metadata is incomplete');
      }
      const target = stored.rollbackRelease;
      if (!target)
        throw new Error('deployment has no retained rollback release');
      const rollbackDigest = deploymentSpecDigest(rollbackSpec);
      const rollbackPhysicalScriptName = releaseScriptName(rollbackSpec);
      if (
        rollbackDigest !== target.specDigest ||
        rollbackPhysicalScriptName !== target.physicalScriptName ||
        target.releaseSchemaVersion !== rollbackSpec.schemaVersion
      ) {
        throw new Error(
          'rollback specification does not exactly match the retained release',
        );
      }
      const activeLive = await backend.inspect(
        currentSpec,
        secrets.maintenanceAdmin,
        stored.activeRelease.artifactVersion,
      );
      if (!activeLive) throw new Error('current active release is missing');
      assertLiveDeploymentMatches(
        activeLive,
        stored,
        currentSpec,
        stored.activeRelease.specDigest,
        stored.activeRelease.application,
      );
      assertExternalReleaseArtifactVersion(
        activeLive,
        stored.activeRelease,
        'rollback source',
      );
      let intent = stored;
      if (stored.phase === 'ready') {
        intent = {
          ...stored,
          phase: 'rolling-back',
          pendingRelease: target,
          updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
        };
        await lease.put(intent);
      } else if (
        stored.pendingRelease?.specDigest !== rollbackDigest ||
        stored.pendingRelease.physicalScriptName !==
          rollbackPhysicalScriptName ||
        stored.pendingRelease.releaseSchemaVersion !==
          rollbackSpec.schemaVersion
      ) {
        throw new Error('rollback retry uses a different retained release');
      }
      if (finalizedOrdinaryState) {
        if (!finalizedStateProvider) {
          throw new Error(
            'finalized ordinary state requires its backend-switch provider',
          );
        }
        intent = await reconcileFinalizedBackendSwitchState({
          provider: finalizedStateProvider,
          targetSpec: rollbackSpec,
          target: currentPlatformTarget,
          record: intent,
          lease,
          clock: options.clock ?? Date.now,
        });
      }
      const retainedLive = await backend.inspect(
        rollbackSpec,
        secrets.maintenanceAdmin,
        target.artifactVersion,
      );
      if (!retainedLive)
        throw new Error('retained rollback release is missing');
      assertLiveDeploymentMatches(
        retainedLive,
        intent,
        rollbackSpec,
        rollbackDigest,
        target.application,
      );
      assertExternalReleaseArtifactVersion(
        retainedLive,
        target,
        'rollback target',
      );
      intent = await commitInvocationAuthority(
        lease,
        intent,
        options.clock ?? Date.now,
      );
      await lease.assertOwned();
      await backend.deployWorker(
        rollbackSpec,
        database,
        secrets,
        intent.platformResources,
        lease,
        target.artifactVersion,
        target.application ??
          applicationBindingTopology(
            rollbackSpec,
            stored.applicationResources ?? [],
          ),
      );
      let live = await backend.inspect(
        rollbackSpec,
        secrets.maintenanceAdmin,
        target.artifactVersion,
      );
      if (!live) throw new Error('retained rollback release is missing');
      assertLiveDeploymentMatches(
        live,
        stored,
        rollbackSpec,
        rollbackDigest,
        target.application,
      );
      assertExternalReleaseArtifactVersion(live, target, 'rollback target');
      // No flip here or before the promotion below: the unconditional
      // rollback-deploy flip above already committed the carrier durably
      // earlier in this same call.
      await lease.assertOwned();
      const health = await backend.ensureMaintenance(
        rollbackSpec,
        secrets.maintenanceAdmin,
        lease,
        target.artifactVersion,
      );
      if (!health.armed) throw new Error('rollback maintenance did not re-arm');
      live = await backend.inspect(
        rollbackSpec,
        secrets.maintenanceAdmin,
        target.artifactVersion,
      );
      if (!live) throw new Error('retained rollback release is missing');
      assertLiveDeploymentMatches(
        live,
        stored,
        rollbackSpec,
        rollbackDigest,
        target.application,
      );
      assertExternalReleaseArtifactVersion(live, target, 'rollback target');
      await lease.assertOwned();
      await backend.promoteWorker(
        rollbackSpec,
        buildPromotionGuard(intent, rollbackPhysicalScriptName),
        stored.outboundPolicy,
        lease,
        target.artifactVersion,
      );
      live = await backend.inspect(
        rollbackSpec,
        secrets.maintenanceAdmin,
        target.artifactVersion,
      );
      if (!live)
        throw new Error('rollback release disappeared after promotion');
      assertLiveDeploymentMatches(
        live,
        stored,
        rollbackSpec,
        rollbackDigest,
        target.application,
      );
      assertExternalReleaseArtifactVersion(live, target, 'rollback settlement');
      const nextRollback = stored.activeRelease;
      // `prior` is the release being ABANDONED here, not the one replaced.
      // A host reversing its own effects needs the snapshot traffic just left,
      // and on this path that is the release that was active on entry.
      const rollbackSettlement = await settlePromotedRoute({
        backend,
        spec: rollbackSpec,
        record: intent,
        entry: 'rollback',
        target,
        prior: nextRollback,
        expectedSpecDigest: target.specDigest,
        expectedArtifactVersion: target.artifactVersion,
        settlementHost: options.settlement,
        attestation: {
          clock: options.clock ?? Date.now,
          ...options.routeAttestation,
        },
      });
      const settled = { ...intent };
      delete settled.pendingRelease;
      const rolledBack: FleetRecord = {
        ...settled,
        phase: 'ready',
        activeRelease: target,
        rollbackRelease: nextRollback,
        desiredSpecDigest: target.specDigest,
        artifactVersion: live.artifactVersion,
        schemaVersion: stored.schemaVersion,
        durableObjectBindings: live.durableObjectBindings,
        applicationBindings:
          target.application ??
          applicationBindingTopology(
            rollbackSpec,
            stored.applicationResources ?? [],
          ),
        ...(rollbackSettlement.settled
          ? { settledSettlementKey: rollbackSettlement.settlementKey }
          : {}),
        updatedAt: new Date((options.clock ?? Date.now)()).toISOString(),
      };
      await lease.put(rolledBack);
      return rolledBack;
    },
  );
}
