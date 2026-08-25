// SPDX-License-Identifier: Apache-2.0

import { createHash, randomUUID } from 'node:crypto';
import { backendSwitchIntentFromUnknown } from './backend-switch.js';
import { isDeploymentScriptName, isSha256 } from './deployment-context.js';
import {
  assertPlatformResourcesMatchTarget,
  canonicalDeploymentEgressPolicy,
  canonicalDurableObjectMigrationHistory,
  canonicalMaintenanceCapabilityPublicKey,
  durableObjectMigrationHistoryDigest,
  externalStateScriptName,
} from './platform-resources.js';
import {
  applicationBindingTopologyFromUnknown,
  externalReleaseTopologyFromUnknown,
} from './release-topology.js';
import type {
  DeploymentEgressPolicy,
  DurableObjectMigration,
  ExternalMigrationIntent,
  ExternalMigrationSubphase,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  PlatformPlaneLease,
  PlatformPlaneResourceKind,
  PlatformPlaneResourceSet,
  PlatformPlaneStateStore,
  ProvisioningPhase,
} from './types.js';
import { PROVISIONING_PHASES } from './types.js';
import { deploymentKey } from './validation.js';

export interface FleetStateDatabase {
  /** Execute one statement and return its rows, including DML RETURNING rows. */
  query(
    sql: string,
    bindings?: readonly unknown[],
  ): Promise<readonly Readonly<Record<string, unknown>>[]>;
  execute(sql: string, bindings?: readonly unknown[]): Promise<void>;
  /** Execute statements atomically and return each statement's result rows. */
  batch(
    statements: readonly Readonly<{
      sql: string;
      bindings?: readonly unknown[];
    }>[],
  ): Promise<readonly (readonly Readonly<Record<string, unknown>>[])[]>;
}

function optionalOutboundPolicy(
  value: unknown,
  tenantTag: string,
  environment: string,
): DeploymentEgressPolicy | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value !== 'string') {
    throw new Error('fleet state row has invalid outbound_policy');
  }
  const parsed = JSON.parse(value) as unknown;
  if (!parsed || typeof parsed !== 'object') {
    throw new Error('fleet state row has invalid outbound_policy');
  }
  const policy = parsed as Record<string, unknown>;
  if (
    typeof policy.policyId !== 'string' ||
    !Array.isArray(policy.policyHosts) ||
    policy.policyHosts.some((host) => typeof host !== 'string') ||
    typeof policy.policyDigest !== 'string'
  ) {
    throw new Error('fleet state row has invalid outbound_policy');
  }
  const canonical = canonicalDeploymentEgressPolicy({
    policyId: policy.policyId,
    tenantTag,
    environment,
    allowedHosts: policy.policyHosts as string[],
  });
  if (
    JSON.stringify(policy.policyHosts) !==
      JSON.stringify(canonical.policyHosts) ||
    policy.policyDigest !== canonical.policyDigest
  ) {
    throw new Error('fleet state row has invalid outbound_policy');
  }
  return canonical;
}

function outboundPolicyFromUnknown(
  value: unknown,
  tenantTag: string,
  environment: string,
  key: string,
): DeploymentEgressPolicy {
  const policy = optionalOutboundPolicy(
    JSON.stringify(value),
    tenantTag,
    environment,
  );
  if (!policy) throw new Error(`fleet state row has invalid ${key}`);
  return policy;
}

const TABLE = 'anchorage_fleet_deployments';
const LEASE_TABLE = 'anchorage_fleet_leases';
const PLATFORM_CLAIM_TABLE = 'anchorage_platform_plane_claims';
const PLATFORM_LEASE_TABLE = 'anchorage_platform_plane_leases';
const LEASE_TTL_MS = 15 * 60_000;
const LEASE_RENEWAL_INTERVAL_MS = 5 * 60_000;
const DB_NOW_MS = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";
const FLEET_ROW_COLUMNS = [
  'tenant_tag',
  'environment',
  'backend',
  'script_name',
  'database_id',
  'database_name',
  'schema_version',
  'artifact_version',
  'desired_spec_digest',
  'pending_spec_digest',
  'pending_artifact_version',
  'active_release',
  'pending_release',
  'migration_prior_release',
  'rollback_release',
  'retiring_release',
  'outbound_policy',
  'platform_resources',
  'platform_target',
  'migration_intent',
  'backend_switch_intent',
  'durable_object_tag',
  'durable_object_migration_history',
  'durable_object_migration_history_digest',
  'durable_object_bindings',
  'application_resources',
  'application_bindings',
  'route_hostname',
  'phase',
  'database_export_location',
  'database_export_sha256',
  'database_export_size',
  'settled_settlement_key',
  'updated_at',
] as const;

/**
 * Nullable TEXT columns added to a table that already shipped, in the order
 * they were added. Each is created by ALTER on an existing database and by the
 * CREATE above on a new one, and each is asserted present afterwards: a column
 * that silently failed to appear would not fail a write, it would drop the
 * value on every write.
 */
const ADDED_NULLABLE_TEXT_COLUMNS = [
  'backend_switch_intent',
  'settled_settlement_key',
] as const;

function isDuplicateColumnError(error: unknown, column: string): boolean {
  const duplicate = new RegExp(`duplicate column name:\\s*${column}\\b`, 'iu');
  let current = error;
  const seen = new Set<Error>();
  while (current instanceof Error && !seen.has(current)) {
    seen.add(current);
    if (duplicate.test(current.message)) return true;
    current = current.cause;
  }
  return false;
}

function assertNullableTextColumn(
  columns: readonly Readonly<Record<string, unknown>>[],
  name: string,
): void {
  const column = columns.find((candidate) => candidate.name === name);
  if (
    !column ||
    String(column.type).toUpperCase() !== 'TEXT' ||
    Number(column.notnull) !== 0 ||
    Number(column.pk) !== 0
  ) {
    throw new Error(`fleet state ${name} column is absent or incompatible`);
  }
}

type DeploymentClaim = readonly [
  'worker-script' | 'dispatch-script' | 'r2-bucket',
  string,
  'deployment-worker' | 'deployment-state' | 'deployment-r2',
];

export interface D1FleetStateStoreOptions {
  readonly accountId: string;
  readonly leaseTtlMs?: number;
  readonly leaseRenewalIntervalMs?: number;
}

function rowString(
  row: Readonly<Record<string, unknown>>,
  key: string,
): string {
  const value = row[key];
  if (typeof value !== 'string')
    throw new Error(`fleet state row has invalid ${key}`);
  return value;
}

function rowNumber(
  row: Readonly<Record<string, unknown>>,
  key: string,
): number {
  const value = Number(row[key]);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`fleet state row has invalid ${key}`);
  }
  return value;
}

function optionalReleaseSnapshot(
  value: unknown,
  key: string,
): ExternalReleaseSnapshot | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value !== 'string') {
    throw new Error(`fleet state row has invalid ${key}`);
  }
  const release = JSON.parse(value) as unknown;
  if (
    !release ||
    typeof release !== 'object' ||
    !('physicalScriptName' in release) ||
    typeof release.physicalScriptName !== 'string' ||
    !('specDigest' in release) ||
    typeof release.specDigest !== 'string' ||
    !isSha256(release.specDigest) ||
    !('artifactVersion' in release) ||
    typeof release.artifactVersion !== 'string' ||
    !('releaseSchemaVersion' in release) ||
    typeof release.releaseSchemaVersion !== 'number' ||
    !Number.isSafeInteger(release.releaseSchemaVersion) ||
    release.releaseSchemaVersion < 0
  ) {
    throw new Error(`fleet state row has invalid ${key}`);
  }
  const topology =
    'topology' in release && release.topology !== undefined
      ? externalReleaseTopologyFromUnknown(release.topology, `${key}.topology`)
      : undefined;
  const application = applicationBindingTopologyFromUnknown(
    'application' in release ? release.application : undefined,
    `${key}.application`,
  );
  if (release.artifactVersion !== 'pending' && !topology) {
    throw new Error(`fleet state row has invalid ${key}`);
  }
  if (
    topology?.application &&
    JSON.stringify(topology.application) !== JSON.stringify(application)
  ) {
    throw new Error(`fleet state row has invalid ${key}`);
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

function releaseSnapshotFromUnknown(
  value: unknown,
  key: string,
): ExternalReleaseSnapshot {
  const release = optionalReleaseSnapshot(JSON.stringify(value), key);
  if (!release) throw new Error(`fleet state row has invalid ${key}`);
  return release;
}

function platformTargetFromUnknown(
  value: unknown,
  tenantTag: string,
  environment: string,
  key: string,
): ExternalPlatformTargetDescription {
  if (!value || typeof value !== 'object') {
    throw new Error(`fleet state row has invalid ${key}`);
  }
  const target = value as Record<string, unknown>;
  if (
    typeof target.stateArtifactDigest !== 'string' ||
    typeof target.maintenanceCapabilityPublicKey !== 'string' ||
    (target.auditQueueName !== undefined &&
      (typeof target.auditQueueName !== 'string' ||
        target.auditQueueName.length === 0)) ||
    !isSha256(target.stateArtifactDigest) ||
    typeof target.stateDurableObjectHistoryDigest !== 'string' ||
    !isSha256(target.stateDurableObjectHistoryDigest) ||
    (target.stateDurableObjectTag !== undefined &&
      typeof target.stateDurableObjectTag !== 'string') ||
    (target.stateEgressCredentialDigest !== undefined &&
      (typeof target.stateEgressCredentialDigest !== 'string' ||
        !isSha256(target.stateEgressCredentialDigest))) ||
    typeof target.egressArtifactDigest !== 'string' ||
    !isSha256(target.egressArtifactDigest) ||
    typeof target.d1SchemaVersion !== 'number' ||
    !Number.isSafeInteger(target.d1SchemaVersion) ||
    target.d1SchemaVersion < 0 ||
    typeof target.d1SchemaHistoryDigest !== 'string' ||
    !isSha256(target.d1SchemaHistoryDigest)
  ) {
    throw new Error(`fleet state row has invalid ${key}`);
  }
  const outboundPolicy = optionalOutboundPolicy(
    JSON.stringify(target.outboundPolicy),
    tenantTag,
    environment,
  );
  if (!outboundPolicy) {
    throw new Error(`fleet state row has invalid ${key}`);
  }
  if (
    canonicalMaintenanceCapabilityPublicKey(
      target.maintenanceCapabilityPublicKey,
    ) !== target.maintenanceCapabilityPublicKey
  ) {
    throw new Error(`fleet state row has invalid ${key}`);
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
      ? {
          stateEgressCredentialDigest: target.stateEgressCredentialDigest,
        }
      : {}),
    egressArtifactDigest: target.egressArtifactDigest,
    d1SchemaVersion: target.d1SchemaVersion,
    d1SchemaHistoryDigest: target.d1SchemaHistoryDigest,
    outboundPolicy,
  };
}

function optionalPlatformTarget(
  value: unknown,
  tenantTag: string,
  environment: string,
): ExternalPlatformTargetDescription | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value !== 'string') {
    throw new Error('fleet state row has invalid platform_target');
  }
  return platformTargetFromUnknown(
    JSON.parse(value) as unknown,
    tenantTag,
    environment,
    'platform_target',
  );
}

const EXTERNAL_MIGRATION_SUBPHASES = [
  'planned',
  'schema-applied',
  'platform-applied',
  'candidate-deployed',
  'candidate-armed',
  'route-published',
] as const satisfies readonly ExternalMigrationSubphase[];

function optionalMigrationIntent(
  value: unknown,
  tenantTag: string,
  environment: string,
): ExternalMigrationIntent | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value !== 'string') {
    throw new Error('fleet state row has invalid migration_intent');
  }
  const parsed = JSON.parse(value) as unknown;
  if (!parsed || typeof parsed !== 'object') {
    throw new Error('fleet state row has invalid migration_intent');
  }
  const intent = parsed as Record<string, unknown>;
  if (
    typeof intent.targetSpecDigest !== 'string' ||
    !isSha256(intent.targetSpecDigest) ||
    (intent.platformOnly !== undefined && intent.platformOnly !== true) ||
    typeof intent.subphase !== 'string' ||
    !EXTERNAL_MIGRATION_SUBPHASES.includes(
      intent.subphase as ExternalMigrationSubphase,
    )
  ) {
    throw new Error('fleet state row has invalid migration_intent');
  }
  return {
    ...(intent.platformOnly === true ? { platformOnly: true as const } : {}),
    targetSpecDigest: intent.targetSpecDigest,
    priorRelease: releaseSnapshotFromUnknown(
      intent.priorRelease,
      'migration_intent.priorRelease',
    ),
    priorTarget: platformTargetFromUnknown(
      intent.priorTarget,
      tenantTag,
      environment,
      'migration_intent.priorTarget',
    ),
    priorOutboundPolicy: outboundPolicyFromUnknown(
      intent.priorOutboundPolicy,
      tenantTag,
      environment,
      'migration_intent.priorOutboundPolicy',
    ),
    targetRelease: releaseSnapshotFromUnknown(
      intent.targetRelease,
      'migration_intent.targetRelease',
    ),
    target: platformTargetFromUnknown(
      intent.target,
      tenantTag,
      environment,
      'migration_intent.target',
    ),
    subphase: intent.subphase as ExternalMigrationSubphase,
  };
}

function optionalPlatformResources(
  value: unknown,
  tenantTag: string,
  environment: string,
): ExternalPlatformResources | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value !== 'string') {
    throw new Error('fleet state row has invalid platform_resources');
  }
  const resources = JSON.parse(value) as unknown;
  if (!resources || typeof resources !== 'object') {
    throw new Error('fleet state row has invalid platform_resources');
  }
  const candidate = resources as Record<string, unknown>;
  const stateWorker = candidate.stateWorker;
  const egressProxy = candidate.egressProxy;
  const policyValue = candidate.outboundPolicy ?? egressProxy;
  const validWorker = (
    worker: unknown,
  ): worker is Readonly<Record<string, unknown>> =>
    Boolean(
      worker &&
        typeof worker === 'object' &&
        typeof (worker as Record<string, unknown>).scriptName === 'string' &&
        isDeploymentScriptName(
          String((worker as Record<string, unknown>).scriptName),
        ) &&
        typeof (worker as Record<string, unknown>).artifactVersion ===
          'string' &&
        String((worker as Record<string, unknown>).artifactVersion).length >
          0 &&
        typeof (worker as Record<string, unknown>).artifactDigest ===
          'string' &&
        isSha256(String((worker as Record<string, unknown>).artifactDigest)),
    );
  if (
    !validWorker(stateWorker) ||
    (!validWorker(egressProxy) &&
      (typeof candidate.sharedOutboundWorkerName !== 'string' ||
        !isDeploymentScriptName(candidate.sharedOutboundWorkerName))) ||
    !policyValue ||
    typeof policyValue !== 'object' ||
    typeof (policyValue as Record<string, unknown>).policyId !== 'string' ||
    String((policyValue as Record<string, unknown>).policyId).length === 0 ||
    !Array.isArray((policyValue as Record<string, unknown>).policyHosts) ||
    ((policyValue as Record<string, unknown>).policyHosts as unknown[]).some(
      (host) => typeof host !== 'string',
    ) ||
    typeof (policyValue as Record<string, unknown>).policyDigest !== 'string' ||
    !isSha256(String((policyValue as Record<string, unknown>).policyDigest)) ||
    !Array.isArray(stateWorker.durableObjectBindings) ||
    !stateWorker.durableObjectBindings.every(
      (binding) =>
        binding &&
        typeof binding === 'object' &&
        typeof (binding as Record<string, unknown>).name === 'string' &&
        typeof (binding as Record<string, unknown>).className === 'string' &&
        typeof (binding as Record<string, unknown>).namespaceId === 'string',
    ) ||
    !Array.isArray(stateWorker.namespaceIds) ||
    stateWorker.namespaceIds.some(
      (namespaceId) => typeof namespaceId !== 'string' || !namespaceId,
    ) ||
    (stateWorker.durableObjectTag !== undefined &&
      typeof stateWorker.durableObjectTag !== 'string') ||
    (stateWorker.plane !== undefined &&
      stateWorker.plane !== 'ordinary' &&
      stateWorker.plane !== 'dispatch') ||
    (stateWorker.dispatchNamespace !== undefined &&
      (typeof stateWorker.dispatchNamespace !== 'string' ||
        stateWorker.dispatchNamespace.length === 0)) ||
    (stateWorker.plane === 'dispatch' &&
      typeof stateWorker.dispatchNamespace !== 'string') ||
    (candidate.auditQueueName !== undefined &&
      (typeof candidate.auditQueueName !== 'string' ||
        candidate.auditQueueName.length === 0)) ||
    typeof candidate.maintenanceCapabilityPublicKey !== 'string'
  ) {
    throw new Error('fleet state row has invalid platform_resources');
  }
  const canonicalPolicy = canonicalDeploymentEgressPolicy({
    policyId: String((policyValue as Record<string, unknown>).policyId),
    tenantTag,
    environment,
    allowedHosts: (policyValue as Record<string, unknown>)
      .policyHosts as string[],
  });
  if (
    canonicalMaintenanceCapabilityPublicKey(
      candidate.maintenanceCapabilityPublicKey,
    ) !== candidate.maintenanceCapabilityPublicKey
  ) {
    throw new Error(
      'fleet state row has invalid platform_resources capability verifier',
    );
  }
  if (
    JSON.stringify((policyValue as Record<string, unknown>).policyHosts) !==
      JSON.stringify(canonicalPolicy.policyHosts) ||
    (policyValue as Record<string, unknown>).policyDigest !==
      canonicalPolicy.policyDigest
  ) {
    throw new Error('fleet state row has invalid platform_resources policy');
  }
  return {
    ...(typeof candidate.auditQueueName === 'string'
      ? { auditQueueName: candidate.auditQueueName }
      : {}),
    maintenanceCapabilityPublicKey: candidate.maintenanceCapabilityPublicKey,
    stateWorker: {
      scriptName: String(stateWorker.scriptName),
      artifactVersion: String(stateWorker.artifactVersion),
      artifactDigest: String(stateWorker.artifactDigest),
      durableObjectBindings:
        stateWorker.durableObjectBindings as unknown as import('./types.js').DurableObjectBindingInventory[],
      namespaceIds: [...new Set(stateWorker.namespaceIds as string[])].sort(),
      ...(stateWorker.plane === 'ordinary' || stateWorker.plane === 'dispatch'
        ? { plane: stateWorker.plane }
        : {}),
      ...(typeof stateWorker.dispatchNamespace === 'string'
        ? { dispatchNamespace: stateWorker.dispatchNamespace }
        : {}),
      ...(typeof stateWorker.durableObjectTag === 'string'
        ? { durableObjectTag: stateWorker.durableObjectTag }
        : {}),
    },
    ...(candidate.outboundPolicy !== undefined
      ? { outboundPolicy: canonicalPolicy }
      : {}),
    ...(typeof candidate.sharedOutboundWorkerName === 'string'
      ? { sharedOutboundWorkerName: candidate.sharedOutboundWorkerName }
      : {}),
    ...(validWorker(egressProxy)
      ? {
          egressProxy: {
            scriptName: String(egressProxy.scriptName),
            artifactVersion: String(egressProxy.artifactVersion),
            artifactDigest: String(egressProxy.artifactDigest),
            ...canonicalPolicy,
          },
        }
      : {}),
  };
}

function toRecord(row: Readonly<Record<string, unknown>>): FleetRecord {
  const backend = rowString(row, 'backend');
  if (backend !== 'plain-worker' && backend !== 'workers-for-platforms') {
    throw new Error('fleet state row has invalid backend');
  }
  const phase = rowString(row, 'phase');
  if (!PROVISIONING_PHASES.includes(phase as ProvisioningPhase)) {
    throw new Error('fleet state row has invalid phase');
  }
  const schemaVersion = rowNumber(row, 'schema_version');
  const applicationResources = (() => {
    const value = row.application_resources ?? '[]';
    if (typeof value !== 'string')
      throw new Error('fleet state row has invalid application_resources');
    const parsed = JSON.parse(value) as unknown;
    const names = new Set<string>();
    const bucketNames = new Set<string>();
    if (
      !Array.isArray(parsed) ||
      parsed.some((resource) => {
        if (
          !resource ||
          typeof resource !== 'object' ||
          Array.isArray(resource)
        )
          return true;
        const item = resource as Record<string, unknown>;
        const creationDate = item.creationDate;
        const state = item.state;
        const validCreationDate =
          typeof creationDate === 'string' &&
          Number.isFinite(Date.parse(creationDate)) &&
          new Date(creationDate).toISOString() === creationDate;
        const duplicate =
          names.has(String(item.name)) ||
          bucketNames.has(String(item.bucketName));
        names.add(String(item.name));
        bucketNames.add(String(item.bucketName));
        const allowedKeys = new Set([
          'name',
          'bucketName',
          'jurisdiction',
          'state',
          'reservationNonce',
          ...(!['reserved', 'create-authorized'].includes(String(state))
            ? ['creationDate']
            : []),
        ]);
        const requiresCreationDate = [
          'created',
          'detach-authorized',
          'detached',
          'empty-authorized',
          'empty',
        ].includes(String(state));
        return (
          Object.keys(item).some((key) => !allowedKeys.has(key)) ||
          typeof item.name !== 'string' ||
          !/^[A-Z][A-Z0-9_]{0,63}$/u.test(item.name) ||
          typeof item.bucketName !== 'string' ||
          !/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/u.test(item.bucketName) ||
          !['default', 'eu', 'fedramp'].includes(String(item.jurisdiction)) ||
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
          ].includes(String(state)) ||
          typeof item.reservationNonce !== 'string' ||
          !/^[A-Za-z0-9_-]{32}$/u.test(item.reservationNonce) ||
          (requiresCreationDate && !validCreationDate) ||
          (!requiresCreationDate &&
            creationDate !== undefined &&
            !validCreationDate) ||
          duplicate
        );
      })
    ) {
      throw new Error('fleet state row has invalid application_resources');
    }
    const resources = parsed as import('./types.js').ApplicationR2Resource[];
    const sorted = [...resources].sort((left, right) =>
      left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
    );
    if (JSON.stringify(resources) !== JSON.stringify(sorted)) {
      throw new Error(
        'fleet state row has non-canonical application_resources',
      );
    }
    return resources;
  })();
  const applicationBindings = (() => {
    const value =
      row.application_bindings ?? '{"vars":[],"secrets":[],"r2Buckets":[]}';
    if (typeof value !== 'string')
      throw new Error('fleet state row has invalid application_bindings');
    const parsed = JSON.parse(value) as unknown;
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('fleet state row has invalid application_bindings');
    }
    const candidate = parsed as Record<string, unknown>;
    if (
      !Array.isArray(candidate.vars) ||
      !Array.isArray(candidate.secrets) ||
      !Array.isArray(candidate.r2Buckets)
    ) {
      throw new Error('fleet state row has invalid application_bindings');
    }
    const bindingName = /^[A-Z][A-Z0-9_]{0,63}$/u;
    const reserved = new Set([
      'AUDIT_PROXY',
      'AUDIT_QUEUE',
      'DB',
      'DEPLOYMENT_IDENTITY_SECRET',
      'EGRESS_PROXY',
      'HOSTS',
      'MAINTENANCE_ADMIN_SECRET',
    ]);
    const seen = new Set<string>();
    const vars = candidate.vars as unknown[];
    const secrets = candidate.secrets as unknown[];
    const buckets = candidate.r2Buckets as unknown[];
    const invalidName = (name: unknown): boolean => {
      if (
        typeof name !== 'string' ||
        !bindingName.test(name) ||
        reserved.has(name) ||
        name.startsWith('FLEET_') ||
        name.startsWith('DEPLOYMENT_') ||
        seen.has(name)
      ) {
        return true;
      }
      seen.add(name);
      return false;
    };
    if (
      vars.some(
        (entry) =>
          !entry ||
          typeof entry !== 'object' ||
          Array.isArray(entry) ||
          Object.keys(entry).some((key) => !['name', 'value'].includes(key)) ||
          invalidName((entry as Record<string, unknown>).name) ||
          typeof (entry as Record<string, unknown>).value !== 'string' ||
          new TextEncoder().encode(
            (entry as Record<string, unknown>).value as string,
          ).byteLength >
            5 * 1024,
      ) ||
      secrets.some(
        (entry) =>
          !entry ||
          typeof entry !== 'object' ||
          Array.isArray(entry) ||
          Object.keys(entry).some(
            (key) => !['name', 'valueSha256'].includes(key),
          ) ||
          invalidName((entry as Record<string, unknown>).name) ||
          !/^[a-f0-9]{64}$/u.test(
            String((entry as Record<string, unknown>).valueSha256),
          ),
      ) ||
      buckets.some(
        (entry) =>
          !entry ||
          typeof entry !== 'object' ||
          Array.isArray(entry) ||
          Object.keys(entry).some(
            (key) => !['name', 'bucketName', 'jurisdiction'].includes(key),
          ) ||
          invalidName((entry as Record<string, unknown>).name) ||
          typeof (entry as Record<string, unknown>).bucketName !== 'string' ||
          !['default', 'eu', 'fedramp'].includes(
            String((entry as Record<string, unknown>).jurisdiction),
          ) ||
          (entry as Record<string, unknown>).creationDate !== undefined,
      )
    ) {
      throw new Error('fleet state row has invalid application_bindings');
    }
    const topology = parsed as import('./types.js').ApplicationBindingTopology;
    const canonical = (items: readonly { readonly name: string }[]) =>
      [...items].sort((left, right) =>
        left.name < right.name ? -1 : left.name > right.name ? 1 : 0,
      );
    if (
      JSON.stringify(topology.vars) !==
        JSON.stringify(canonical(topology.vars)) ||
      JSON.stringify(topology.secrets) !==
        JSON.stringify(canonical(topology.secrets)) ||
      JSON.stringify(topology.r2Buckets) !==
        JSON.stringify(canonical(topology.r2Buckets)) ||
      JSON.stringify(topology.r2Buckets) !==
        JSON.stringify(
          applicationResources.map(({ name, bucketName, jurisdiction }) => ({
            name,
            bucketName,
            jurisdiction,
          })),
        )
    ) {
      throw new Error('fleet state row has inconsistent application_bindings');
    }
    return topology;
  })();
  const databaseExportLocation = row.database_export_location;
  const durableObjectTag = row.durable_object_tag;
  if (
    durableObjectTag !== null &&
    durableObjectTag !== undefined &&
    typeof durableObjectTag !== 'string'
  ) {
    throw new Error('fleet state row has invalid durable_object_tag');
  }
  const durableObjectMigrationHistory = (() => {
    const value = row.durable_object_migration_history;
    if (value === null || value === undefined) return undefined;
    if (typeof value !== 'string') {
      throw new Error(
        'fleet state row has invalid durable_object_migration_history',
      );
    }
    const parsed = JSON.parse(value) as unknown;
    if (
      !Array.isArray(parsed) ||
      parsed.some((migration) => {
        if (!migration || typeof migration !== 'object') return true;
        const candidate = migration as Record<string, unknown>;
        const stringArrays = [
          candidate.newSqliteClasses,
          candidate.newClasses,
          candidate.deletedClasses,
        ];
        return (
          typeof candidate.tag !== 'string' ||
          stringArrays.some(
            (items) =>
              items !== undefined &&
              (!Array.isArray(items) ||
                items.some((item) => typeof item !== 'string')),
          ) ||
          (candidate.renamedClasses !== undefined &&
            (!Array.isArray(candidate.renamedClasses) ||
              candidate.renamedClasses.some(
                (rename) =>
                  !rename ||
                  typeof rename !== 'object' ||
                  typeof (rename as Record<string, unknown>).from !==
                    'string' ||
                  typeof (rename as Record<string, unknown>).to !== 'string',
              )))
        );
      })
    ) {
      throw new Error(
        'fleet state row has invalid durable_object_migration_history',
      );
    }
    return parsed as DurableObjectMigration[];
  })();
  const durableObjectMigrationDigest =
    row.durable_object_migration_history_digest;
  if (
    (durableObjectMigrationHistory === undefined) !==
      (durableObjectMigrationDigest === null ||
        durableObjectMigrationDigest === undefined) ||
    (durableObjectMigrationHistory !== undefined &&
      (typeof durableObjectMigrationDigest !== 'string' ||
        durableObjectMigrationDigest !==
          durableObjectMigrationHistoryDigest(durableObjectMigrationHistory) ||
        JSON.stringify(durableObjectMigrationHistory) !==
          JSON.stringify(
            canonicalDurableObjectMigrationHistory(
              durableObjectMigrationHistory,
            ),
          ) ||
        durableObjectMigrationHistory.at(-1)?.tag !== durableObjectTag))
  ) {
    throw new Error(
      'fleet state row has inconsistent Durable Object migration history',
    );
  }
  if (
    databaseExportLocation !== null &&
    databaseExportLocation !== undefined &&
    typeof databaseExportLocation !== 'string'
  ) {
    throw new Error('fleet state row has invalid database_export_location');
  }
  const databaseExportSha256 = row.database_export_sha256;
  if (
    databaseExportSha256 !== null &&
    databaseExportSha256 !== undefined &&
    typeof databaseExportSha256 !== 'string'
  ) {
    throw new Error('fleet state row has invalid database_export_sha256');
  }
  const databaseExportSize = row.database_export_size;
  if (
    databaseExportSize !== null &&
    databaseExportSize !== undefined &&
    (!Number.isSafeInteger(Number(databaseExportSize)) ||
      Number(databaseExportSize) < 0)
  ) {
    throw new Error('fleet state row has invalid database_export_size');
  }
  const settledSettlementKey = row.settled_settlement_key;
  if (
    settledSettlementKey !== null &&
    settledSettlementKey !== undefined &&
    (typeof settledSettlementKey !== 'string' ||
      !isSha256(settledSettlementKey))
  ) {
    throw new Error('fleet state row has invalid settled_settlement_key');
  }
  const durableObjectBindings = JSON.parse(
    rowString(row, 'durable_object_bindings'),
  ) as unknown;
  if (
    !Array.isArray(durableObjectBindings) ||
    !durableObjectBindings.every(
      (binding) =>
        binding &&
        typeof binding === 'object' &&
        typeof binding.name === 'string' &&
        typeof binding.className === 'string' &&
        typeof binding.namespaceId === 'string',
    )
  ) {
    throw new Error('fleet state row has invalid durable_object_bindings');
  }
  const activeRelease = optionalReleaseSnapshot(
    row.active_release,
    'active_release',
  );
  const pendingRelease = optionalReleaseSnapshot(
    row.pending_release,
    'pending_release',
  );
  const migrationPriorRelease = optionalReleaseSnapshot(
    row.migration_prior_release,
    'migration_prior_release',
  );
  const rollbackRelease = optionalReleaseSnapshot(
    row.rollback_release,
    'rollback_release',
  );
  const retiringRelease = optionalReleaseSnapshot(
    row.retiring_release,
    'retiring_release',
  );
  const tenantTag = rowString(row, 'tenant_tag');
  const environment = rowString(row, 'environment');
  const platformResources = optionalPlatformResources(
    row.platform_resources,
    tenantTag,
    environment,
  );
  const outboundPolicy = optionalOutboundPolicy(
    row.outbound_policy,
    tenantTag,
    environment,
  );
  const platformTarget = optionalPlatformTarget(
    row.platform_target,
    tenantTag,
    environment,
  );
  let migrationIntent: ExternalMigrationIntent | undefined;
  let backendSwitchIntent:
    | import('./backend-switch.js').BackendSwitchIntent
    | undefined;
  if (typeof row.migration_intent === 'string') {
    const parsedIntent: unknown = JSON.parse(row.migration_intent);
    if (
      parsedIntent &&
      typeof parsedIntent === 'object' &&
      'kind' in parsedIntent &&
      parsedIntent.kind === 'backend-switch'
    ) {
      backendSwitchIntent = backendSwitchIntentFromUnknown(parsedIntent);
    } else {
      migrationIntent = optionalMigrationIntent(
        row.migration_intent,
        tenantTag,
        environment,
      );
    }
  } else if (
    row.migration_intent !== null &&
    row.migration_intent !== undefined
  ) {
    throw new Error('fleet state row has invalid migration_intent');
  }
  if (typeof row.backend_switch_intent === 'string') {
    if (backendSwitchIntent) {
      throw new Error('fleet state row has duplicate backend switch intent');
    }
    backendSwitchIntent = backendSwitchIntentFromUnknown(
      JSON.parse(row.backend_switch_intent),
    );
  } else if (
    row.backend_switch_intent !== null &&
    row.backend_switch_intent !== undefined
  ) {
    throw new Error('fleet state row has invalid backend_switch_intent');
  }
  if (
    (backend === 'workers-for-platforms' && !outboundPolicy) ||
    (backend === 'plain-worker' && outboundPolicy) ||
    (platformResources &&
      (platformResources.outboundPolicy ?? platformResources.egressProxy)
        ?.policyId !== outboundPolicy?.policyId) ||
    (platformResources &&
      JSON.stringify(
        (platformResources.outboundPolicy ?? platformResources.egressProxy)
          ?.policyHosts,
      ) !== JSON.stringify(outboundPolicy?.policyHosts)) ||
    (platformResources &&
      (platformResources.outboundPolicy ?? platformResources.egressProxy)
        ?.policyDigest !== outboundPolicy?.policyDigest)
  ) {
    throw new Error('fleet state row has inconsistent outbound_policy');
  }
  if (
    (platformTarget &&
      JSON.stringify(platformTarget.outboundPolicy) !==
        JSON.stringify(outboundPolicy)) ||
    (migrationIntent && phase !== 'migrating') ||
    (migrationIntent &&
      migrationIntent.targetSpecDigest !==
        migrationIntent.targetRelease.specDigest) ||
    (migrationIntent &&
      migrationIntent.target.d1SchemaVersion !==
        (migrationIntent.platformOnly === true
          ? schemaVersion
          : migrationIntent.targetRelease.releaseSchemaVersion)) ||
    (migrationIntent &&
      JSON.stringify(migrationIntent.priorRelease) !==
        JSON.stringify(activeRelease)) ||
    (migrationIntent?.platformOnly === true
      ? JSON.stringify(migrationIntent.targetRelease) !==
          JSON.stringify(activeRelease) ||
        migrationPriorRelease !== undefined ||
        pendingRelease !== undefined ||
        ['candidate-deployed', 'candidate-armed'].includes(
          migrationIntent.subphase,
        )
      : migrationIntent !== undefined &&
        (JSON.stringify(migrationIntent.priorRelease) !==
          JSON.stringify(migrationPriorRelease) ||
          JSON.stringify(migrationIntent.targetRelease) !==
            JSON.stringify(pendingRelease)))
  ) {
    throw new Error('fleet state row has inconsistent migration intent');
  }
  if (
    (backend === 'plain-worker' && (platformTarget || migrationIntent)) ||
    (backend === 'workers-for-platforms' &&
      platformResources !== undefined &&
      platformTarget === undefined) ||
    (backend === 'workers-for-platforms' &&
      phase === 'migrating' &&
      migrationIntent === undefined)
  ) {
    throw new Error('fleet state row has inconsistent platform target');
  }
  if (
    row.pending_artifact_version !== undefined &&
    row.pending_artifact_version !== null &&
    (backend !== 'plain-worker' ||
      phase !== 'migrating' ||
      typeof row.pending_spec_digest !== 'string' ||
      typeof row.pending_artifact_version !== 'string' ||
      row.pending_artifact_version.length === 0 ||
      row.pending_artifact_version === 'pending')
  ) {
    throw new Error('fleet state row has inconsistent pending artifact');
  }
  if (platformResources && platformTarget) {
    assertPlatformResourcesMatchTarget(platformResources, platformTarget);
  }
  if (
    migrationIntent &&
    [
      'platform-applied',
      'candidate-deployed',
      'candidate-armed',
      'route-published',
    ].includes(migrationIntent.subphase)
  ) {
    if (
      JSON.stringify(platformTarget) !== JSON.stringify(migrationIntent.target)
    ) {
      throw new Error('fleet state row has inconsistent migration target');
    }
  }
  return {
    tenantTag,
    environment,
    backend,
    scriptName: rowString(row, 'script_name'),
    databaseId: rowString(row, 'database_id'),
    databaseName: rowString(row, 'database_name'),
    schemaVersion,
    artifactVersion: rowString(row, 'artifact_version'),
    desiredSpecDigest: rowString(row, 'desired_spec_digest'),
    ...(typeof row.pending_spec_digest === 'string'
      ? { pendingSpecDigest: row.pending_spec_digest }
      : {}),
    ...(typeof row.pending_artifact_version === 'string'
      ? { pendingArtifactVersion: row.pending_artifact_version }
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
    ...(backendSwitchIntent ? { backendSwitchIntent } : {}),
    ...(typeof durableObjectTag === 'string' ? { durableObjectTag } : {}),
    ...(durableObjectMigrationHistory
      ? {
          durableObjectMigrationHistory,
          durableObjectMigrationHistoryDigest:
            durableObjectMigrationDigest as string,
        }
      : {}),
    durableObjectBindings,
    applicationResources,
    applicationBindings,
    routeHostname: rowString(row, 'route_hostname'),
    phase: phase as ProvisioningPhase,
    ...(typeof databaseExportLocation === 'string'
      ? { databaseExportLocation }
      : {}),
    ...(typeof databaseExportSha256 === 'string'
      ? { databaseExportSha256 }
      : {}),
    ...(databaseExportSize !== null && databaseExportSize !== undefined
      ? { databaseExportSize: Number(databaseExportSize) }
      : {}),
    ...(typeof settledSettlementKey === 'string' && settledSettlementKey
      ? { settledSettlementKey }
      : {}),
    updatedAt: rowString(row, 'updated_at'),
  };
}

export class D1FleetStateStore
  implements FleetStateStore, PlatformPlaneStateStore
{
  readonly #db: FleetStateDatabase;
  readonly #accountId: string;
  readonly #leaseTtlMs: number;
  readonly #leaseRenewalIntervalMs: number;
  #schemaReady: Promise<void> | undefined;

  constructor(db: FleetStateDatabase, options: D1FleetStateStoreOptions) {
    this.#db = db;
    if (!options.accountId) throw new Error('accountId is required');
    this.#accountId = options.accountId;
    this.#leaseTtlMs = options.leaseTtlMs ?? LEASE_TTL_MS;
    this.#leaseRenewalIntervalMs =
      options.leaseRenewalIntervalMs ?? LEASE_RENEWAL_INTERVAL_MS;
    if (!Number.isSafeInteger(this.#leaseTtlMs) || this.#leaseTtlMs < 1) {
      throw new Error('leaseTtlMs must be a positive integer');
    }
    if (
      !Number.isSafeInteger(this.#leaseRenewalIntervalMs) ||
      this.#leaseRenewalIntervalMs < 1 ||
      this.#leaseRenewalIntervalMs >= this.#leaseTtlMs
    ) {
      throw new Error(
        'leaseRenewalIntervalMs must be a positive integer below leaseTtlMs',
      );
    }
  }

  async #ensureSchema(): Promise<void> {
    const pending = this.#schemaReady ?? this.#initializeSchema();
    this.#schemaReady = pending;
    try {
      await pending;
    } catch (error) {
      if (this.#schemaReady === pending) this.#schemaReady = undefined;
      throw error;
    }
  }

  async #initializeSchema(): Promise<void> {
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${TABLE} (
      tenant_tag TEXT NOT NULL,
      environment TEXT NOT NULL,
      backend TEXT NOT NULL CHECK (backend IN ('plain-worker', 'workers-for-platforms')),
      script_name TEXT NOT NULL,
      database_id TEXT NOT NULL,
      database_name TEXT NOT NULL,
      schema_version INTEGER NOT NULL CHECK (schema_version >= 0),
      artifact_version TEXT NOT NULL,
      desired_spec_digest TEXT NOT NULL,
      pending_spec_digest TEXT,
      pending_artifact_version TEXT,
      active_release TEXT,
      pending_release TEXT,
      migration_prior_release TEXT,
      rollback_release TEXT,
      retiring_release TEXT,
      outbound_policy TEXT,
      platform_resources TEXT,
      platform_target TEXT,
      migration_intent TEXT,
      backend_switch_intent TEXT,
      durable_object_tag TEXT,
      durable_object_migration_history TEXT,
      durable_object_migration_history_digest TEXT,
      durable_object_bindings TEXT NOT NULL,
      application_resources TEXT NOT NULL DEFAULT '[]',
      application_bindings TEXT NOT NULL DEFAULT '{"vars":[],"secrets":[],"r2Buckets":[]}',
      route_hostname TEXT NOT NULL,
      phase TEXT NOT NULL,
      database_export_location TEXT,
      database_export_sha256 TEXT,
      database_export_size INTEGER,
      settled_settlement_key TEXT,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (tenant_tag, environment),
      UNIQUE (backend, script_name),
      UNIQUE (database_id),
      UNIQUE (database_name),
      UNIQUE (route_hostname)
    )`);
    let fleetColumns = await this.#db.query(`PRAGMA table_info(${TABLE})`);
    for (const name of ADDED_NULLABLE_TEXT_COLUMNS) {
      if (fleetColumns.some((column) => column.name === name)) continue;
      try {
        await this.#db.execute(`ALTER TABLE ${TABLE} ADD COLUMN ${name} TEXT`);
      } catch (error) {
        // A replica that added the same column between the read and the write.
        if (!isDuplicateColumnError(error, name)) throw error;
      }
      fleetColumns = await this.#db.query(`PRAGMA table_info(${TABLE})`);
    }
    for (const name of ADDED_NULLABLE_TEXT_COLUMNS) {
      assertNullableTextColumn(fleetColumns, name);
    }
    await this.#db.execute(`UPDATE ${TABLE}
        SET backend_switch_intent = migration_intent,
            migration_intent = NULL
        WHERE backend_switch_intent IS NULL
          AND json_extract(migration_intent, '$.kind') = 'backend-switch'`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${LEASE_TABLE} (
      tenant_tag TEXT NOT NULL,
      environment TEXT NOT NULL,
      owner_token TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      PRIMARY KEY (tenant_tag, environment)
      )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${PLATFORM_CLAIM_TABLE} (
      account_id TEXT NOT NULL,
      resource_type TEXT NOT NULL CHECK (resource_type IN ('dispatch-namespace', 'dispatch-script', 'worker-script', 'kv-namespace', 'r2-bucket', 'queue')),
      resource_name TEXT NOT NULL,
      resource_role TEXT NOT NULL CHECK (resource_role IN ('dispatch-namespace', 'shared-dispatch', 'shared-outbound', 'shared-audit', 'host-routing-kv', 'audit-queue', 'audit-dead-letter-queue', 'deployment-worker', 'deployment-state', 'deployment-egress', 'deployment-r2')),
      resource_set_key TEXT NOT NULL,
      platform_plane_identity TEXT NOT NULL,
      PRIMARY KEY (account_id, resource_type, resource_name)
    )`);
    await this.#db.execute(`CREATE TABLE IF NOT EXISTS ${PLATFORM_LEASE_TABLE} (
      resource_set_key TEXT PRIMARY KEY,
      owner_token TEXT NOT NULL,
      expires_at INTEGER NOT NULL
    )`);
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    deploymentKey(tenantTag, environment);
    await this.#ensureSchema();
    const token = randomUUID();
    const claimed = await this.#db.query(
      `INSERT INTO ${LEASE_TABLE} (
        tenant_tag, environment, owner_token, expires_at
      ) VALUES (?, ?, ?, ${DB_NOW_MS} + ?)
      ON CONFLICT (tenant_tag, environment) DO UPDATE SET
        owner_token = excluded.owner_token,
        expires_at = excluded.expires_at
      WHERE ${LEASE_TABLE}.expires_at <= ${DB_NOW_MS}
      RETURNING owner_token, expires_at`,
      [tenantTag, environment, token, this.#leaseTtlMs],
    );
    if (claimed.length !== 1 || claimed[0]?.owner_token !== token) {
      throw new Error(
        `deployment '${tenantTag}:${environment}' is already being modified`,
      );
    }

    const label = `deployment '${tenantTag}:${environment}'`;
    return this.#runRenewingLease({
      label,
      renew: () => this.#renewLease(tenantTag, environment, token),
      release: async () => {
        const released = await this.#db.query(
          `DELETE FROM ${LEASE_TABLE}
        WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
        RETURNING owner_token`,
          [tenantTag, environment, token],
        );
        if (released.length !== 1 || released[0]?.owner_token !== token) {
          throw this.#leaseLost(tenantTag, environment);
        }
      },
      createLease: (assertOwned) => ({
        tenantTag,
        environment,
        mutationLeaseTtlMs: this.#leaseTtlMs,
        assertOwned,
        renew: assertOwned,
        put: (record) =>
          this.#putUnderLease(tenantTag, environment, token, record),
        delete: () => this.#deleteUnderLease(tenantTag, environment, token),
      }),
      operation,
    });
  }

  async withPlatformPlaneLease<T>(
    resourceSet: PlatformPlaneResourceSet,
    platformPlaneIdentity: string,
    operation: (lease: PlatformPlaneLease) => Promise<T>,
  ): Promise<T> {
    if (resourceSet.accountId !== this.#accountId) {
      throw new Error(
        `platform resource account '${resourceSet.accountId}' does not match fleet state account '${this.#accountId}'`,
      );
    }
    const resourceSetKey = this.#platformResourceSetKey(resourceSet);
    if (!platformPlaneIdentity) {
      throw new Error('platformPlaneIdentity is required');
    }
    await this.#ensureSchema();
    await this.#reservePlatformResources(
      resourceSet,
      resourceSetKey,
      platformPlaneIdentity,
    );
    const token = randomUUID();
    const claimed = await this.#db.query(
      `INSERT INTO ${PLATFORM_LEASE_TABLE} (
        resource_set_key, owner_token, expires_at
      ) VALUES (?, ?, ${DB_NOW_MS} + ?)
      ON CONFLICT (resource_set_key) DO UPDATE SET
        owner_token = excluded.owner_token,
        expires_at = excluded.expires_at
      WHERE ${PLATFORM_LEASE_TABLE}.expires_at <= ${DB_NOW_MS}
      RETURNING owner_token, expires_at`,
      [resourceSetKey, token, this.#leaseTtlMs],
    );
    if (claimed.length !== 1 || claimed[0]?.owner_token !== token) {
      throw new Error(
        `platform resource set '${resourceSetKey}' is already being modified`,
      );
    }

    const label = `platform resource set '${resourceSetKey}'`;
    return this.#runRenewingLease({
      label,
      renew: () => this.#renewPlatformLease(resourceSetKey, token),
      release: async () => {
        const released = await this.#db.query(
          `DELETE FROM ${PLATFORM_LEASE_TABLE}
        WHERE resource_set_key = ? AND owner_token = ?
        RETURNING owner_token`,
          [resourceSetKey, token],
        );
        if (released.length !== 1 || released[0]?.owner_token !== token) {
          throw this.#platformLeaseLost(resourceSetKey);
        }
      },
      createLease: (assertOwned) => ({
        resourceSetKey,
        mutationLeaseTtlMs: this.#leaseTtlMs,
        assertOwned,
        renew: assertOwned,
      }),
      operation,
    });
  }

  async #runRenewingLease<TLease, T>(options: {
    readonly label: string;
    readonly renew: () => Promise<void>;
    readonly release: () => Promise<void>;
    readonly createLease: (assertOwned: () => Promise<void>) => TLease;
    readonly operation: (lease: TLease) => Promise<T>;
  }): Promise<T> {
    const heartbeatAbort = new AbortController();
    const renewalErrors: unknown[] = [];
    const assertOwned = async () => {
      const heartbeatError = renewalErrors[0];
      if (heartbeatError !== undefined) {
        throw new Error(`${options.label} heartbeat failed`, {
          cause: heartbeatError,
        });
      }
      await options.renew();
    };
    const lease = options.createLease(assertOwned);
    const heartbeat = this.#renewUntilAborted(
      options.renew,
      heartbeatAbort.signal,
    ).catch((error: unknown) => {
      renewalErrors.push(error);
    });

    let operationFailed = false;
    let operationError: unknown;
    let outcome: { readonly value: T } | undefined;
    try {
      outcome = { value: await options.operation(lease) };
    } catch (error) {
      operationFailed = true;
      operationError = error;
    }

    heartbeatAbort.abort();
    await heartbeat;
    if (!operationFailed && renewalErrors.length === 0) {
      try {
        await assertOwned();
      } catch (error) {
        renewalErrors.push(error);
      }
    }

    let releaseFailed = false;
    let releaseError: unknown;
    try {
      await options.release();
    } catch (error) {
      releaseFailed = true;
      releaseError = error;
    }

    const errors: unknown[] = [];
    if (operationFailed) errors.push(operationError);
    errors.push(...renewalErrors);
    if (releaseFailed) errors.push(releaseError);
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) {
      throw new AggregateError(
        errors,
        `${options.label} operation and lease cleanup failed`,
      );
    }
    if (!outcome) throw new Error(`${options.label} operation had no outcome`);
    return outcome.value;
  }

  #platformResourceSetKey(resourceSet: PlatformPlaneResourceSet): string {
    const values = [
      resourceSet.accountId,
      resourceSet.dispatchNamespace,
      resourceSet.dispatchScriptName,
      resourceSet.outboundScriptName,
      resourceSet.auditScriptName,
      resourceSet.hostRoutingKvId,
      resourceSet.auditQueueName,
      resourceSet.maintenanceCapabilityPublicKey,
    ];
    if (
      values.some((value) => typeof value !== 'string' || value.length === 0)
    ) {
      throw new Error('platform resource set fields are required');
    }
    const workerNames = [
      resourceSet.dispatchScriptName,
      resourceSet.outboundScriptName,
      resourceSet.auditScriptName,
    ];
    if (new Set(workerNames).size !== workerNames.length) {
      throw new Error('platform resource set Worker names must be distinct');
    }
    if (
      resourceSet.auditDeadLetterQueue !== undefined &&
      resourceSet.auditDeadLetterQueue.length === 0
    ) {
      throw new Error(
        'platform resource set dead-letter queue must be nonempty',
      );
    }
    if (resourceSet.auditDeadLetterQueue === resourceSet.auditQueueName) {
      throw new Error('platform resource set queue names must be distinct');
    }
    if (
      canonicalMaintenanceCapabilityPublicKey(
        resourceSet.maintenanceCapabilityPublicKey,
      ) !== resourceSet.maintenanceCapabilityPublicKey
    ) {
      throw new Error(
        'platform resource set maintenance capability public key must be canonical',
      );
    }
    return createHash('sha256')
      .update(
        JSON.stringify({
          accountId: resourceSet.accountId,
          dispatchNamespace: resourceSet.dispatchNamespace,
          dispatchScriptName: resourceSet.dispatchScriptName,
          outboundScriptName: resourceSet.outboundScriptName,
          auditScriptName: resourceSet.auditScriptName,
          hostRoutingKvId: resourceSet.hostRoutingKvId,
          auditQueueName: resourceSet.auditQueueName,
          maintenanceCapabilityPublicKey:
            resourceSet.maintenanceCapabilityPublicKey,
          auditDeadLetterQueue: resourceSet.auditDeadLetterQueue ?? null,
        }),
      )
      .digest('hex');
  }

  async #reservePlatformResources(
    resourceSet: PlatformPlaneResourceSet,
    resourceSetKey: string,
    platformPlaneIdentity: string,
  ): Promise<void> {
    type Claim = readonly [
      PlatformPlaneResourceKind,
      string,
      (
        | 'dispatch-namespace'
        | 'shared-dispatch'
        | 'shared-outbound'
        | 'shared-audit'
        | 'host-routing-kv'
        | 'audit-queue'
        | 'audit-dead-letter-queue'
      ),
    ];
    const claims: Claim[] = [
      [
        'dispatch-namespace',
        resourceSet.dispatchNamespace,
        'dispatch-namespace',
      ],
      ['worker-script', resourceSet.dispatchScriptName, 'shared-dispatch'],
      ['worker-script', resourceSet.outboundScriptName, 'shared-outbound'],
      ['worker-script', resourceSet.auditScriptName, 'shared-audit'],
      ['kv-namespace', resourceSet.hostRoutingKvId, 'host-routing-kv'],
      ['queue', resourceSet.auditQueueName, 'audit-queue'],
      ...(resourceSet.auditDeadLetterQueue
        ? [
            [
              'queue',
              resourceSet.auditDeadLetterQueue,
              'audit-dead-letter-queue',
            ] as Claim,
          ]
        : []),
    ];
    const bindings = claims.flatMap(([type, name, role]) => [
      resourceSet.accountId,
      type,
      name,
      role,
      resourceSetKey,
      platformPlaneIdentity,
    ]);
    try {
      const inserted = await this.#db.query(
        `INSERT INTO ${PLATFORM_CLAIM_TABLE} (
          account_id, resource_type, resource_name, resource_role,
          resource_set_key, platform_plane_identity
        ) VALUES ${claims.map(() => '(?, ?, ?, ?, ?, ?)').join(', ')}
        RETURNING resource_type, resource_name`,
        bindings,
      );
      if (inserted.length === claims.length) return;
    } catch {
      // A committed-but-unacknowledged insert and an idempotent retry both
      // converge through the exact claim read below.
    }
    const existing = await this.#db.query(
      `SELECT resource_type, resource_name, resource_role,
        resource_set_key, platform_plane_identity
      FROM ${PLATFORM_CLAIM_TABLE}
      WHERE account_id = ? AND (${claims
        .map(() => '(resource_type = ? AND resource_name = ?)')
        .join(' OR ')})`,
      [
        resourceSet.accountId,
        ...claims.reduce<string[]>((values, [type, name]) => {
          values.push(type, name);
          return values;
        }, []),
      ],
    );
    const expected = new Set(
      claims.map(([type, name, role]) => `${type}:${name}:${role}`),
    );
    const exact =
      existing.length === claims.length &&
      existing.every(
        (row) =>
          row.resource_set_key === resourceSetKey &&
          row.platform_plane_identity === platformPlaneIdentity &&
          expected.has(
            `${String(row.resource_type)}:${String(row.resource_name)}:${String(row.resource_role)}`,
          ),
      );
    if (!exact) {
      throw new Error(
        `platform resource set '${resourceSetKey}' overlaps another durable reservation`,
      );
    }
  }

  async #renewPlatformLease(
    resourceSetKey: string,
    token: string,
  ): Promise<void> {
    const renewed = await this.#db.query(
      `UPDATE ${PLATFORM_LEASE_TABLE}
      SET expires_at = ${DB_NOW_MS} + ?
      WHERE resource_set_key = ? AND owner_token = ?
        AND expires_at > ${DB_NOW_MS}
      RETURNING owner_token, expires_at`,
      [this.#leaseTtlMs, resourceSetKey, token],
    );
    if (renewed.length !== 1 || renewed[0]?.owner_token !== token) {
      throw this.#platformLeaseLost(resourceSetKey);
    }
  }

  #platformLeaseLost(resourceSetKey: string): Error {
    return new Error(
      `platform resource set '${resourceSetKey}' lease is no longer owned by this operation`,
    );
  }

  async #renewUntilAborted(
    renew: () => Promise<void>,
    signal: AbortSignal,
  ): Promise<void> {
    while (await this.#waitForRenewal(signal)) {
      await renew();
    }
  }

  #waitForRenewal(signal: AbortSignal): Promise<boolean> {
    if (signal.aborted) return Promise.resolve(false);
    return new Promise((resolve) => {
      const timeout = setTimeout(
        () => finish(true),
        this.#leaseRenewalIntervalMs,
      );
      const aborted = () => finish(false);
      const finish = (renew: boolean) => {
        clearTimeout(timeout);
        signal.removeEventListener('abort', aborted);
        resolve(renew);
      };
      signal.addEventListener('abort', aborted, { once: true });
    });
  }

  async #renewLease(
    tenantTag: string,
    environment: string,
    token: string,
  ): Promise<void> {
    const renewed = await this.#db.query(
      `UPDATE ${LEASE_TABLE}
      SET expires_at = ${DB_NOW_MS} + ?
      WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
        AND expires_at > ${DB_NOW_MS}
      RETURNING owner_token, expires_at`,
      [this.#leaseTtlMs, tenantTag, environment, token],
    );
    if (renewed.length !== 1 || renewed[0]?.owner_token !== token) {
      throw this.#leaseLost(tenantTag, environment);
    }
  }

  #leaseLost(tenantTag: string, environment: string): Error {
    return new Error(
      `deployment '${tenantTag}:${environment}' lease is no longer owned by this operation`,
    );
  }

  async get(
    tenantTag: string,
    environment: string,
  ): Promise<FleetRecord | undefined> {
    deploymentKey(tenantTag, environment);
    await this.#ensureSchema();
    const rows = await this.#db.query(
      `SELECT * FROM ${TABLE} WHERE tenant_tag = ? AND environment = ?`,
      [tenantTag, environment],
    );
    if (rows.length > 1) throw new Error('fleet state key is not unique');
    return rows[0] ? toRecord(rows[0]) : undefined;
  }

  async #putUnderLease(
    tenantTag: string,
    environment: string,
    token: string,
    record: FleetRecord,
  ): Promise<void> {
    deploymentKey(record.tenantTag, record.environment);
    if (record.tenantTag !== tenantTag || record.environment !== environment) {
      throw new Error(
        `deployment lease '${tenantTag}:${environment}' cannot write '${record.tenantTag}:${record.environment}'`,
      );
    }
    if (
      record.migrationIntent &&
      record.backendSwitchIntent &&
      record.backendSwitchIntent.subphase !== 'finalized' &&
      record.backendSwitchIntent.subphase !== 'rolled-back'
    ) {
      throw new Error(
        'only a settled backend switch can coexist with migration intent',
      );
    }
    const releases = [
      record.activeRelease,
      record.pendingRelease,
      record.migrationPriorRelease,
      record.rollbackRelease,
      record.retiringRelease,
      record.migrationIntent?.priorRelease,
      record.migrationIntent?.targetRelease,
    ].filter(
      (release): release is ExternalReleaseSnapshot => release !== undefined,
    );
    for (const release of releases) {
      if (!release.application) {
        throw new Error('external release has no owned application topology');
      }
      if (
        release.topology &&
        (!release.topology.application ||
          JSON.stringify(release.topology.application) !==
            JSON.stringify(release.application))
      ) {
        throw new Error(
          'external release physical and owned application topology diverge',
        );
      }
    }
    const identity = `deployment:${record.tenantTag}:${record.environment}`;
    const claims = this.#deploymentClaims(record);
    const desiredClaims = JSON.stringify(
      claims.map(([resourceType, resourceName, resourceRole]) => ({
        resourceType,
        resourceName,
        resourceRole,
      })),
    );
    const rowBindings = [
      record.tenantTag,
      record.environment,
      record.backend,
      record.scriptName,
      record.databaseId,
      record.databaseName,
      record.schemaVersion,
      record.artifactVersion,
      record.desiredSpecDigest,
      record.pendingSpecDigest ?? null,
      record.pendingArtifactVersion ?? null,
      record.activeRelease ? JSON.stringify(record.activeRelease) : null,
      record.pendingRelease ? JSON.stringify(record.pendingRelease) : null,
      record.migrationPriorRelease
        ? JSON.stringify(record.migrationPriorRelease)
        : null,
      record.rollbackRelease ? JSON.stringify(record.rollbackRelease) : null,
      record.retiringRelease ? JSON.stringify(record.retiringRelease) : null,
      record.outboundPolicy ? JSON.stringify(record.outboundPolicy) : null,
      record.platformResources
        ? JSON.stringify(record.platformResources)
        : null,
      record.platformTarget ? JSON.stringify(record.platformTarget) : null,
      record.migrationIntent ? JSON.stringify(record.migrationIntent) : null,
      record.backendSwitchIntent
        ? JSON.stringify(record.backendSwitchIntent)
        : null,
      record.durableObjectTag ?? null,
      record.durableObjectMigrationHistory
        ? JSON.stringify(record.durableObjectMigrationHistory)
        : null,
      record.durableObjectMigrationHistoryDigest ?? null,
      JSON.stringify(record.durableObjectBindings),
      JSON.stringify(record.applicationResources ?? []),
      JSON.stringify(
        record.applicationBindings ?? { vars: [], secrets: [], r2Buckets: [] },
      ),
      record.routeHostname,
      record.phase,
      record.databaseExportLocation ?? null,
      record.databaseExportSha256 ?? null,
      record.databaseExportSize ?? null,
      record.settledSettlementKey ?? null,
      record.updatedAt,
    ] as const;
    const upsertSql = `INSERT INTO ${TABLE} (
        tenant_tag, environment, backend, script_name, database_id,
        database_name, schema_version, artifact_version, desired_spec_digest,
        pending_spec_digest, pending_artifact_version, active_release, pending_release, migration_prior_release,
        rollback_release, retiring_release, outbound_policy, platform_resources,
        platform_target, migration_intent, backend_switch_intent, durable_object_tag,
        durable_object_migration_history, durable_object_migration_history_digest,
        durable_object_bindings, application_resources, application_bindings,
        route_hostname, phase,
        database_export_location, database_export_sha256,
        database_export_size, settled_settlement_key, updated_at
      ) SELECT CASE WHEN EXISTS (
        SELECT 1 FROM ${LEASE_TABLE}
        WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
          AND expires_at > ${DB_NOW_MS}
      ) THEN ? ELSE NULL END,
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      WHERE true
      ON CONFLICT (tenant_tag, environment) DO UPDATE SET
        backend = excluded.backend,
        script_name = excluded.script_name,
        database_id = excluded.database_id,
        database_name = excluded.database_name,
        schema_version = excluded.schema_version,
        artifact_version = excluded.artifact_version,
        desired_spec_digest = excluded.desired_spec_digest,
        pending_spec_digest = excluded.pending_spec_digest,
        pending_artifact_version = excluded.pending_artifact_version,
        active_release = excluded.active_release,
        pending_release = excluded.pending_release,
        migration_prior_release = excluded.migration_prior_release,
        rollback_release = excluded.rollback_release,
        retiring_release = excluded.retiring_release,
        outbound_policy = excluded.outbound_policy,
        platform_resources = excluded.platform_resources,
        platform_target = excluded.platform_target,
        migration_intent = excluded.migration_intent,
        backend_switch_intent = excluded.backend_switch_intent,
        durable_object_tag = excluded.durable_object_tag,
        durable_object_migration_history = excluded.durable_object_migration_history,
        durable_object_migration_history_digest = excluded.durable_object_migration_history_digest,
        durable_object_bindings = excluded.durable_object_bindings,
        application_resources = excluded.application_resources,
        application_bindings = excluded.application_bindings,
        route_hostname = excluded.route_hostname,
        phase = excluded.phase,
        database_export_location = excluded.database_export_location,
        database_export_sha256 = excluded.database_export_sha256,
        database_export_size = excluded.database_export_size,
        settled_settlement_key = excluded.settled_settlement_key,
        updated_at = excluded.updated_at
      RETURNING tenant_tag, environment`;
    let results: readonly (readonly Readonly<Record<string, unknown>>[])[];
    try {
      results = await this.#db.batch([
        {
          sql: `WITH desired AS (
            SELECT json_extract(value, '$.resourceType') AS resource_type,
              json_extract(value, '$.resourceName') AS resource_name,
              json_extract(value, '$.resourceRole') AS resource_role
            FROM json_each(?)
          )
          DELETE FROM ${PLATFORM_CLAIM_TABLE}
          WHERE account_id = ? AND resource_set_key = ?
            AND platform_plane_identity = ?
            AND EXISTS (SELECT 1 FROM desired
              WHERE desired.resource_type = ${PLATFORM_CLAIM_TABLE}.resource_type
                AND desired.resource_name = ${PLATFORM_CLAIM_TABLE}.resource_name)
            AND EXISTS (SELECT 1 FROM ${LEASE_TABLE}
              WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
                AND expires_at > ${DB_NOW_MS})
          RETURNING resource_type, resource_name`,
          bindings: [
            desiredClaims,
            this.#accountId,
            identity,
            identity,
            tenantTag,
            environment,
            token,
          ],
        },
        {
          sql: `WITH desired AS (
            SELECT json_extract(value, '$.resourceType') AS resource_type,
              json_extract(value, '$.resourceName') AS resource_name,
              json_extract(value, '$.resourceRole') AS resource_role
            FROM json_each(?)
          )
          INSERT INTO ${PLATFORM_CLAIM_TABLE} (
            account_id, resource_type, resource_name, resource_role,
            resource_set_key, platform_plane_identity
          ) SELECT ?, resource_type, resource_name, resource_role, ?, ?
          FROM desired
          WHERE EXISTS (SELECT 1 FROM ${LEASE_TABLE}
            WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
              AND expires_at > ${DB_NOW_MS})
          RETURNING resource_type, resource_name, resource_role`,
          bindings: [
            desiredClaims,
            this.#accountId,
            identity,
            identity,
            tenantTag,
            environment,
            token,
          ],
        },
        {
          sql: `WITH desired AS (
            SELECT json_extract(value, '$.resourceType') AS resource_type,
              json_extract(value, '$.resourceName') AS resource_name,
              json_extract(value, '$.resourceRole') AS resource_role
            FROM json_each(?)
          )
          DELETE FROM ${PLATFORM_CLAIM_TABLE}
          WHERE account_id = ? AND resource_set_key = ?
            AND platform_plane_identity = ?
            AND NOT EXISTS (SELECT 1 FROM desired
              WHERE desired.resource_type = ${PLATFORM_CLAIM_TABLE}.resource_type
                AND desired.resource_name = ${PLATFORM_CLAIM_TABLE}.resource_name
                AND desired.resource_role = ${PLATFORM_CLAIM_TABLE}.resource_role)
            AND EXISTS (SELECT 1 FROM ${LEASE_TABLE}
              WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
                AND expires_at > ${DB_NOW_MS})
          RETURNING resource_type, resource_name`,
          bindings: [
            desiredClaims,
            this.#accountId,
            identity,
            identity,
            tenantTag,
            environment,
            token,
          ],
        },
        {
          sql: upsertSql,
          bindings: [tenantTag, environment, token, ...rowBindings],
        },
      ]);
    } catch (error) {
      if (await this.#putConverged(record, claims, rowBindings, token)) return;
      throw error;
    }
    const written = results[3] ?? [];
    if (
      written.length !== 1 ||
      written[0]?.tenant_tag !== tenantTag ||
      written[0]?.environment !== environment
    ) {
      throw this.#leaseLost(tenantTag, environment);
    }
  }

  #deploymentClaims(record: FleetRecord): readonly DeploymentClaim[] {
    let workerClaim: DeploymentClaim;
    if (record.backend === 'plain-worker') {
      workerClaim = ['worker-script', record.scriptName, 'deployment-worker'];
    } else if (record.platformResources) {
      const state = record.platformResources.stateWorker;
      if (state.plane === 'ordinary') {
        workerClaim = ['worker-script', state.scriptName, 'deployment-state'];
      } else if (state.plane === 'dispatch' || state.dispatchNamespace) {
        workerClaim = ['dispatch-script', state.scriptName, 'deployment-state'];
      } else {
        throw new Error(
          'Workers for Platforms state resource has no durable ownership plane',
        );
      }
    } else {
      const reservingPhases: readonly ProvisioningPhase[] = [
        'database-reserved',
        'database-create-authorized',
        'database-created',
        'identity-seeded',
        'migrated',
        'application-resources-create-authorized',
        'application-resources-deployed',
      ];
      if (!reservingPhases.includes(record.phase)) {
        throw new Error(
          `Workers for Platforms deployment in phase '${record.phase}' has no persisted state resource`,
        );
      }
      workerClaim = [
        'dispatch-script',
        externalStateScriptName(record),
        'deployment-state',
      ];
    }
    return [
      workerClaim,
      ...(record.applicationResources ?? []).map(
        (resource): DeploymentClaim => [
          'r2-bucket',
          resource.bucketName,
          'deployment-r2',
        ],
      ),
    ];
  }

  async #putConverged(
    record: FleetRecord,
    claims: readonly DeploymentClaim[],
    rowBindings: readonly unknown[],
    token: string,
  ): Promise<boolean> {
    const owned = await this.#db.query(
      `SELECT owner_token FROM ${LEASE_TABLE}
      WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
        AND expires_at > ${DB_NOW_MS}`,
      [record.tenantTag, record.environment, token],
    );
    if (owned.length !== 1 || owned[0]?.owner_token !== token) {
      throw this.#leaseLost(record.tenantTag, record.environment);
    }
    const rows = await this.#db.query(
      `SELECT * FROM ${TABLE} WHERE tenant_tag = ? AND environment = ?`,
      [record.tenantTag, record.environment],
    );
    const exactRow =
      rows.length === 1 &&
      FLEET_ROW_COLUMNS.every(
        (column, index) => rows[0]?.[column] === rowBindings[index],
      );
    const identity = `deployment:${record.tenantTag}:${record.environment}`;
    const ownedClaims = await this.#db.query(
      `SELECT resource_type, resource_name, resource_role,
        resource_set_key, platform_plane_identity
      FROM ${PLATFORM_CLAIM_TABLE}
      WHERE account_id = ? AND resource_set_key = ?`,
      [this.#accountId, identity],
    );
    const desired = new Set(
      claims.map(([type, name, role]) => `${type}:${name}:${role}`),
    );
    const exactOwnedClaims =
      ownedClaims.length === claims.length &&
      ownedClaims.every(
        (claim) =>
          claim.platform_plane_identity === identity &&
          desired.has(
            `${String(claim.resource_type)}:${String(claim.resource_name)}:${String(claim.resource_role)}`,
          ),
      );
    const desiredClaims = JSON.stringify(
      claims.map(([resourceType, resourceName]) => ({
        resourceType,
        resourceName,
      })),
    );
    const occupants = await this.#db.query(
      `WITH desired AS (
        SELECT json_extract(value, '$.resourceType') AS resource_type,
          json_extract(value, '$.resourceName') AS resource_name
        FROM json_each(?)
      )
      SELECT claims.resource_type, claims.resource_name,
        claims.resource_role, claims.resource_set_key,
        claims.platform_plane_identity
      FROM ${PLATFORM_CLAIM_TABLE} claims
      JOIN desired USING (resource_type, resource_name)
      WHERE claims.account_id = ?`,
      [desiredClaims, this.#accountId],
    );
    const exactOccupants =
      occupants.length === claims.length &&
      occupants.every(
        (claim) =>
          claim.resource_set_key === identity &&
          claim.platform_plane_identity === identity &&
          desired.has(
            `${String(claim.resource_type)}:${String(claim.resource_name)}:${String(claim.resource_role)}`,
          ),
      );
    if (exactRow && exactOwnedClaims && exactOccupants) return true;
    if (
      occupants.some(
        (claim) =>
          claim.resource_set_key !== identity ||
          claim.platform_plane_identity !== identity,
      )
    ) {
      throw new Error(
        `deployment '${record.tenantTag}:${record.environment}' Worker names overlap another durable reservation`,
      );
    }
    if (exactRow || exactOwnedClaims || exactOccupants) {
      throw new Error(
        `deployment '${record.tenantTag}:${record.environment}' has a mixed atomic ownership commit`,
      );
    }
    return false;
  }

  async #deleteUnderLease(
    tenantTag: string,
    environment: string,
    token: string,
  ): Promise<void> {
    const deleted = await this.#db.query(
      `DELETE FROM ${TABLE}
      WHERE tenant_tag = ? AND environment = ?
        AND EXISTS (
          SELECT 1 FROM ${LEASE_TABLE}
          WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
            AND expires_at > ${DB_NOW_MS}
        )
      RETURNING tenant_tag, environment`,
      [tenantTag, environment, tenantTag, environment, token],
    );
    if (deleted.length === 1) return;
    const owned = await this.#db.query(
      `SELECT owner_token FROM ${LEASE_TABLE}
      WHERE tenant_tag = ? AND environment = ? AND owner_token = ?
        AND expires_at > ${DB_NOW_MS}`,
      [tenantTag, environment, token],
    );
    if (owned.length !== 1 || owned[0]?.owner_token !== token) {
      throw this.#leaseLost(tenantTag, environment);
    }
  }

  async list(): Promise<readonly FleetRecord[]> {
    await this.#ensureSchema();
    const rows = await this.#db.query(
      `SELECT * FROM ${TABLE} ORDER BY tenant_tag, environment`,
    );
    return rows.map(toRecord);
  }
}
