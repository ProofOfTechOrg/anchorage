// SPDX-License-Identifier: Apache-2.0

import {
  DEPLOYMENT_SENTINEL_COLUMNS,
  DEPLOYMENT_SENTINEL_DDL,
} from '@proofoftech/flowsafe/deployment-identity-protocol';
import type { MaintenanceCapabilityJwk } from '@proofoftech/flowsafe/host-kit';
import {
  MAINTENANCE_RECEIPT_HEADER,
  mintMaintenanceReceipt,
  verifyAsymmetricMaintenanceCapability,
} from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it, vi } from 'vitest';
import { WorkerDeploymentError } from '../src/deployment-error.js';
import {
  canonicalDeploymentEgressPolicy,
  externalPlatformResourceGroupId,
  externalReleaseTopology,
  externalStateScriptName,
} from '../src/platform-resources.js';
import { provisionDeployment } from '../src/provision.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  DatabaseExport,
  DatabaseReference,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
  ExternalPlatformProfile,
  ExternalPlatformResources,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  PromotionGuard,
} from '../src/types.js';
import {
  externalReleaseScriptName,
  type WorkersForPlatformsApi,
  WorkersForPlatformsBackend,
} from '../src/workers-for-platforms-backend.js';

const deployment: DeploymentSpec = {
  tenantTag: 'acme',
  environment: 'production',
  scriptName: 'acme-production',
  databaseName: 'acme-production',
  compatibilityDate: '2026-08-10',
  mainModule: 'worker.js',
  modules: [{ name: 'worker.js', content: 'export default {}' }],
  authoredBy: 'external',
  schemaVersion: 1,
  migrations: [{ version: 1, sql: 'CREATE TABLE example (id TEXT)' }],
  durableObjectMigrations: [],
  durableObjectBindings: [],
  maintenanceBaseUrl: 'https://control-acme.example.test',
  routeHostname: 'dispatch.example.test',
};

const platformResources: ExternalPlatformResources = {
  maintenanceCapabilityPublicKey:
    '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
  stateWorker: {
    scriptName: 'acme-production-state-0123456789abcdef0123',
    artifactVersion: 'state-v1',
    artifactDigest: 'a'.repeat(64),
    durableObjectBindings: [],
    namespaceIds: [],
  },
  egressProxy: {
    scriptName: 'acme-production-egress-0123456789abcdef0123',
    artifactVersion: 'proxy-v1',
    artifactDigest: 'b'.repeat(64),
    ...canonicalDeploymentEgressPolicy({
      policyId: externalPlatformResourceGroupId(deployment),
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
      allowedHosts: ['api.example.com'],
    }),
  },
};

const secrets: DeploymentSecrets = {
  deploymentIdentity: 'deployment-identity-secret-value-0001',
  maintenanceAdmin: 'maintenance-admin-secret-value-00001',
};
const MAINTENANCE_CAPABILITY_PRIVATE_KEY = {
  kty: 'OKP',
  crv: 'Ed25519',
  alg: 'EdDSA',
  kid: 'fleet-maintenance-v1',
  x: 'Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo',
  d: 'gkXf8_b8kcCJxZ33fUYUac7yCsxZAxQXgsgPbwDpnlM',
} as const satisfies MaintenanceCapabilityJwk;
const MAINTENANCE_CAPABILITY_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

const fence: ExternalMutationFence = {
  mutationLeaseTtlMs: 60_000,
  async assertOwned(): Promise<void> {},
};
const NAMESPACED_STATE = Object.freeze({
  dispatchNamespace: 'fleet-conformance',
  sharedOutboundWorkerName: 'fleet-shared-outbound',
  stateEgressRootSecret: 'state-egress-root-secret-value-0001',
});

class MemoryFleetStore implements FleetStateStore {
  record: FleetRecord | undefined;

  constructor(record?: FleetRecord) {
    this.record = record;
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    return operation({
      tenantTag,
      environment,
      mutationLeaseTtlMs: 60_000,
      assertOwned: async () => {},
      renew: async () => {},
      put: (record) => this.put(record),
      delete: () => this.delete(),
    });
  }

  async get(): Promise<FleetRecord | undefined> {
    return this.record;
  }

  async put(record: FleetRecord): Promise<void> {
    this.record = record;
  }

  async delete(): Promise<void> {
    this.record = undefined;
  }

  async list(): Promise<readonly FleetRecord[]> {
    return this.record ? [this.record] : [];
  }
}

function recordWithPlatformResources(
  backend: WorkersForPlatformsBackend,
  spec: DeploymentSpec,
  resources: ExternalPlatformResources,
  database: DatabaseReference,
  overrides: Partial<FleetRecord> = {},
): FleetRecord {
  return {
    tenantTag: spec.tenantTag,
    backend: 'workers-for-platforms',
    environment: spec.environment,
    scriptName: spec.scriptName,
    databaseId: database.id,
    databaseName: database.name,
    schemaVersion: spec.schemaVersion,
    artifactVersion: 'candidate-v1',
    desiredSpecDigest: deploymentSpecDigest(spec),
    outboundPolicy: resources.egressProxy,
    platformResources: resources,
    platformTarget: backend.describeExternalPlatformTarget(spec),
    durableObjectBindings: spec.durableObjectBindings.map((binding) => ({
      ...binding,
      namespaceId: `namespace-${binding.name.toLowerCase()}`,
    })),
    routeHostname: spec.routeHostname,
    phase: 'worker-deleted',
    updatedAt: '2026-08-11T00:00:00.000Z',
    ...overrides,
  };
}

function platformConvergenceRecord(
  backend: WorkersForPlatformsBackend,
  spec: DeploymentSpec,
  database: DatabaseReference,
  overrides: Partial<FleetRecord> = {},
): FleetRecord {
  return {
    tenantTag: spec.tenantTag,
    backend: 'workers-for-platforms',
    environment: spec.environment,
    scriptName: spec.scriptName,
    databaseId: database.id,
    databaseName: database.name,
    schemaVersion: spec.schemaVersion,
    artifactVersion: 'pending',
    desiredSpecDigest: deploymentSpecDigest(spec),
    outboundPolicy: backend.describeExternalPlatformTarget(spec).outboundPolicy,
    platformTarget: backend.describeExternalPlatformTarget(spec),
    durableObjectBindings: [],
    routeHostname: spec.routeHostname,
    phase: 'migrated',
    updatedAt: '2026-08-11T00:00:00.000Z',
    ...overrides,
  };
}

function platformProfile(
  overrides: Partial<ExternalPlatformProfile> = {},
): ExternalPlatformProfile {
  return {
    runtimeContractVersion: 1,
    backwardCompatibleWithRetainedReleases: true,
    maintenanceCapabilityPublicKey: MAINTENANCE_CAPABILITY_PUBLIC_KEY,
    maintenanceCapabilityPrivateKey: MAINTENANCE_CAPABILITY_PRIVATE_KEY,
    stateWorker: {
      mainModule: 'state.js',
      modules: [{ name: 'state.js', content: 'export default {state: 1}' }],
      compatibilityDate: '2026-08-10',
    },
    egressProxyWorker: {
      mainModule: 'proxy.js',
      modules: [{ name: 'proxy.js', content: 'export default {proxy: 1}' }],
      compatibilityDate: '2026-08-10',
    },
    stateDurableObjectMigrations: [
      { tag: 'state-v1', newSqliteClasses: ['Maintenance'] },
    ],
    organizationEgressHosts: ['api.example.com'],
    ...overrides,
  };
}

class FakeApi implements WorkersForPlatformsApi {
  readonly calls: string[] = [];
  failSecrets = false;
  failUpload = false;
  failDelete = false;
  exists = true;
  desiredSpecDigest = deploymentSpecDigest(deployment);
  inspectedScriptNames: string[] = [];
  readonly releaseDigests = new Map<string, string>();
  readonly releaseArtifactVersions = new Map<string, string>();
  readonly deletedScriptNames: string[] = [];
  readonly uploadedScriptNames: string[] = [];
  readonly uploadedNamespacedState: Array<
    Parameters<
      NonNullable<WorkersForPlatformsApi['uploadNamespacedStateWorker']>
    >[0]
  > = [];
  readonly scriptInventories = new Map<
    string,
    {
      scriptName: string;
      tenantTag: string;
      environment: string;
      databaseId: string;
      routeHostname: string;
    }
  >();
  readonly uploadedControlSpecs: Array<{
    scriptName: string;
    bindings: readonly Readonly<Record<string, unknown>>[];
    migrations?: Readonly<Record<string, unknown>>;
  }> = [];
  readonly controlWorkers = new Map<
    string,
    Awaited<ReturnType<WorkersForPlatformsApi['inspectControlWorker']>>
  >();
  readonly controlSecrets = new Map<string, Readonly<Record<string, string>>>();
  readonly controlSecretUpdates: Array<{
    scriptName: string;
    secrets: Readonly<Record<string, string>>;
  }> = [];
  dispatchPlatformResources: ExternalPlatformResources | undefined;
  dispatchArtifactVersion = 'etag-v1';
  failDisableScriptName: string | undefined;
  failControlUploadAfterCommitScriptName: string | undefined;
  failDatabaseCreateAfterCommit = false;
  database: DatabaseReference | undefined;
  databaseOwner: string | undefined;
  deploymentSentinelPresent = false;
  readonly migrationRows: Array<{
    version: number;
    sql_sha256: string;
  }> = [];
  readonly databaseIdsRead: string[] = [];
  readonly remainingNamespaceIds = new Set<string>();
  readonly namespaceIdsByScript = new Map<string, Set<string>>();
  readonly namespaceExistenceChecks: string[] = [];
  readonly databaseAttachments: Array<{
    scriptName: string;
    plane: 'ordinary' | 'dispatch';
  }> = [];
  readonly dispatchSecretOptions: Array<
    Readonly<{ includeMaintenanceAdmin?: boolean }> | undefined
  > = [];
  readonly dispatchWorkers = new Map<
    string,
    NonNullable<
      Awaited<ReturnType<WorkersForPlatformsApi['inspectDispatchWorker']>>
    >
  >();
  readonly dispatchSecretNames = new Map<string, readonly string[]>();
  mutationFenceEntries = 0;
  routeOwner:
    | {
        scriptName: string;
        tenantTag: string;
        environment: string;
        policyId?: string;
        policyDigest?: string;
        policyHosts?: readonly string[];
      }
    | undefined;
  lastPromotedRoute:
    | {
        scriptName: string;
        tenantTag: string;
        environment: string;
        policyId: string;
        policyDigest: string;
        policyHosts: readonly string[];
      }
    | undefined;

  async listWorkerDatabaseAttachments(): Promise<
    readonly Readonly<{
      scriptName: string;
      plane: 'ordinary' | 'dispatch';
    }>[]
  > {
    return this.databaseAttachments;
  }

  async withMutationFence<T>(
    _fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T> {
    this.mutationFenceEntries += 1;
    return operation();
  }

  async findDatabase(): Promise<DatabaseReference | undefined> {
    return this.database;
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    this.databaseIdsRead.push(databaseId);
    return {
      id: databaseId,
      name: deployment.databaseName,
      created: false,
    };
  }

  async createDatabase(): Promise<DatabaseReference> {
    this.database = {
      id: 'db-acme',
      name: 'acme-production',
      created: false,
    };
    if (this.failDatabaseCreateAfterCommit) {
      this.failDatabaseCreateAfterCommit = false;
      throw new Error('database create response lost');
    }
    return { ...this.database, created: true };
  }

  async queryDatabase(
    _databaseId: string,
    sql: string,
    bindings: readonly unknown[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    if (sql.includes("FROM sqlite_schema WHERE type = 'table' ORDER BY name")) {
      return [
        ...(this.deploymentSentinelPresent
          ? [{ name: 'flowsafe_deployment', sql: DEPLOYMENT_SENTINEL_DDL }]
          : []),
        ...(this.migrationRows.length > 0
          ? [{ name: 'anchorage_fleet_migrations', sql: 'owned by test D1' }]
          : []),
      ];
    }
    if (sql.includes("FROM sqlite_schema WHERE type = 'table' AND name = ?")) {
      return this.deploymentSentinelPresent
        ? [{ sql: DEPLOYMENT_SENTINEL_DDL }]
        : [];
    }
    if (sql.startsWith('PRAGMA table_info(flowsafe_deployment)')) {
      return DEPLOYMENT_SENTINEL_COLUMNS;
    }
    if (sql.includes('FROM flowsafe_deployment ORDER BY id')) {
      return this.databaseOwner
        ? [{ id: 1, tenant_tag: this.databaseOwner }]
        : [];
    }
    if (sql.startsWith('CREATE TABLE IF NOT EXISTS flowsafe_deployment')) {
      this.deploymentSentinelPresent = true;
      return [];
    }
    if (sql.startsWith('INSERT OR IGNORE INTO flowsafe_deployment')) {
      this.databaseOwner = String(bindings[0]);
      return [];
    }
    if (sql.includes('FROM anchorage_fleet_migrations ORDER BY version')) {
      return this.migrationRows;
    }
    return [];
  }

  async batchDatabase(
    _databaseId: string,
    statements: readonly Readonly<{
      sql: string;
      bindings?: readonly unknown[];
    }>[],
  ): Promise<void> {
    const ledgerWrite = statements.find((statement) =>
      statement.sql.startsWith('INSERT INTO anchorage_fleet_migrations'),
    );
    if (ledgerWrite?.bindings) {
      this.migrationRows.push({
        version: Number(ledgerWrite.bindings[0]),
        sql_sha256: String(ledgerWrite.bindings[1]),
      });
    }
  }

  async uploadControlWorker(spec: {
    scriptName: string;
    bindings: readonly Readonly<Record<string, unknown>>[];
    migrations?: Readonly<Record<string, unknown>>;
  }): Promise<string> {
    this.calls.push(`upload-control:${spec.scriptName}`);
    this.uploadedControlSpecs.push(spec);
    const plainTextBindings = Object.fromEntries(
      spec.bindings.flatMap((binding) =>
        binding.type === 'plain_text'
          ? [[String(binding.name), String(binding.text)] as const]
          : [],
      ),
    );
    const artifactVersion = `version:${spec.scriptName}:${this.uploadedControlSpecs.length}`;
    this.controlWorkers.set(spec.scriptName, {
      artifactVersion,
      databaseIds: spec.bindings.flatMap((binding) =>
        binding.type === 'd1' ? [String(binding.database_id)] : [],
      ),
      durableObjectBindings: spec.bindings.flatMap((binding) =>
        binding.type === 'durable_object_namespace'
          ? [
              {
                name: String(binding.name),
                className: String(binding.class_name),
                namespaceId: `namespace:${String(binding.class_name)}`,
              },
            ]
          : [],
      ),
      serviceBindings: spec.bindings.flatMap((binding) =>
        binding.type === 'service'
          ? [{ name: String(binding.name), service: String(binding.service) }]
          : [],
      ),
      queueProducerBindings: spec.bindings.flatMap((binding) =>
        binding.type === 'queue'
          ? [
              {
                name: String(binding.name),
                queueName: String(binding.queue_name),
              },
            ]
          : [],
      ),
      secretNames: this.controlWorkers.get(spec.scriptName)?.secretNames ?? [],
      kvNamespaceBindings: spec.bindings.flatMap((binding) =>
        binding.type === 'kv_namespace'
          ? [
              {
                name: String(binding.name),
                namespaceId: String(binding.namespace_id),
              },
            ]
          : [],
      ),
      plainTextBindings,
      workersDevEnabled: true,
      previewUrlsEnabled: true,
      routeHostnames: [],
      zoneRoutes: [],
    });
    const namespaceIds =
      this.namespaceIdsByScript.get(spec.scriptName) ?? new Set<string>();
    for (const binding of spec.bindings) {
      if (binding.type === 'durable_object_namespace') {
        namespaceIds.add(`namespace:${String(binding.class_name)}`);
      }
    }
    this.namespaceIdsByScript.set(spec.scriptName, namespaceIds);
    if (this.failControlUploadAfterCommitScriptName === spec.scriptName) {
      this.failControlUploadAfterCommitScriptName = undefined;
      throw new Error(`upload response lost for ${spec.scriptName}`);
    }
    return artifactVersion;
  }

  async putControlSecrets(
    scriptName: string,
    secrets: Readonly<Record<string, string>>,
  ): Promise<void> {
    this.calls.push('control-secrets');
    this.controlSecrets.set(scriptName, { ...secrets });
    this.controlSecretUpdates.push({ scriptName, secrets: { ...secrets } });
    const live = this.controlWorkers.get(scriptName);
    if (live) {
      this.controlWorkers.set(scriptName, {
        ...live,
        secretNames: Object.keys(secrets).sort(),
      });
    }
  }

  async inspectControlWorker(scriptName: string) {
    return this.controlWorkers.get(scriptName);
  }

  async revokeControlSecrets(): Promise<void> {
    this.calls.push('revoke-control');
  }

  async disableControlWorkerPublicAccess(scriptName: string): Promise<void> {
    this.calls.push(`disable-control-public:${scriptName}`);
    if (this.failDisableScriptName === scriptName) {
      throw new Error(`disable failed for ${scriptName}`);
    }
    const live = this.controlWorkers.get(scriptName);
    if (live)
      this.controlWorkers.set(scriptName, {
        ...live,
        workersDevEnabled: false,
        previewUrlsEnabled: false,
        routeHostnames: [],
        zoneRoutes: [],
      });
  }

  async deleteControlWorker(scriptName: string): Promise<void> {
    this.calls.push(`delete-control:${scriptName}`);
    this.controlWorkers.delete(scriptName);
    this.namespaceIdsByScript.delete(scriptName);
  }

  async hasDurableObjectNamespace(namespaceId: string): Promise<boolean> {
    this.namespaceExistenceChecks.push(namespaceId);
    return (
      this.remainingNamespaceIds.has(namespaceId) ||
      [...this.namespaceIdsByScript.values()].some((ids) =>
        ids.has(namespaceId),
      )
    );
  }

  async listDurableObjectNamespaces(
    scriptName: string,
  ): Promise<readonly string[]> {
    return [...(this.namespaceIdsByScript.get(scriptName) ?? [])].sort();
  }

  async uploadDispatchWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    physicalScriptName?: string,
    resources?: ExternalPlatformResources,
    application?: import('../src/types.js').ApplicationBindingTopology,
  ): Promise<{ artifactVersion: string }> {
    this.calls.push('upload');
    this.uploadedScriptNames.push(physicalScriptName ?? 'missing');
    this.dispatchPlatformResources = resources;
    if (this.failUpload) throw new Error('upload failed ambiguously');
    this.desiredSpecDigest = deploymentSpecDigest(spec);
    this.dispatchArtifactVersion = 'etag-v1';
    const scriptName = physicalScriptName ?? spec.scriptName;
    const externalTopology =
      spec.authoredBy === 'external'
        ? externalReleaseTopology(spec, resources)
        : undefined;
    this.dispatchWorkers.set(scriptName, {
      artifactVersion: this.dispatchArtifactVersion,
      databaseIds: [database.id],
      durableObjectBindings:
        externalTopology?.durableObjectBindings ??
        spec.durableObjectBindings.map((binding) => ({
          ...binding,
          namespaceId: `namespace:${binding.className}`,
        })),
      serviceBindings:
        externalTopology?.serviceBindings ??
        (spec.egressProxyService
          ? [{ name: 'EGRESS_PROXY', service: spec.egressProxyService }]
          : []),
      queueProducerBindings:
        externalTopology?.queueProducerBindings ??
        (spec.queueProducer
          ? [
              {
                name: spec.queueProducer.binding,
                queueName: spec.queueProducer.queueName,
              },
            ]
          : []),
      secretNames: this.dispatchSecretNames.get(scriptName) ?? [],
      r2BucketBindings: application?.r2Buckets ?? [],
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      schemaVersion: spec.schemaVersion,
      desiredSpecDigest: this.desiredSpecDigest,
    });
    return { artifactVersion: this.dispatchArtifactVersion };
  }

  async uploadNamespacedStateWorker(
    options: Parameters<
      NonNullable<WorkersForPlatformsApi['uploadNamespacedStateWorker']>
    >[0],
  ): Promise<{ artifactVersion: string }> {
    this.uploadedNamespacedState.push(options);
    const scriptName = externalStateScriptName(options.spec);
    const resourceGroupId = externalPlatformResourceGroupId(options.spec);
    const durableObjectBindings = [
      ...options.spec.durableObjectBindings,
      ...(options.auditQueueName
        ? [
            {
              name: 'FLEET_AUDIT_PROXY_OBJECT',
              className: 'FlowsafeFleetAuditProxy',
            },
          ]
        : []),
    ].map((binding) => ({
      ...binding,
      namespaceId: `namespace:${binding.className}`,
    }));
    const artifactVersion = `state-etag-${this.dispatchWorkers.size + 1}`;
    this.calls.push(`upload-namespaced:${scriptName}`);
    this.dispatchWorkers.set(scriptName, {
      artifactVersion,
      databaseIds: [options.database.id],
      durableObjectBindings,
      serviceBindings: [
        {
          name: 'OUTBOUND_PROXY',
          service: options.sharedOutboundWorkerName,
          entrypoint: 'StateEgress',
        },
      ],
      queueProducerBindings: options.auditQueueName
        ? [{ name: 'AUDIT_QUEUE', queueName: options.auditQueueName }]
        : [],
      secretNames: this.dispatchSecretNames.get(scriptName) ?? [],
      tenantTag: options.spec.tenantTag,
      environment: options.spec.environment,
      schemaVersion: options.spec.schemaVersion,
      desiredSpecDigest: deploymentSpecDigest(options.spec),
      durableObjectTag: options.spec.durableObjectMigrations.at(-1)?.tag,
      plainTextBindings: {
        DEPLOYMENT_TENANT: options.spec.tenantTag,
        FLEET_ENVIRONMENT: options.spec.environment,
        FLEET_SCHEMA_VERSION: String(options.spec.schemaVersion),
        FLEET_SPEC_DIGEST: deploymentSpecDigest(options.spec),
        FLEET_RESOURCE_GROUP: resourceGroupId,
        FLEET_RESOURCE_ROLE: 'platform-state',
        FLEET_DEPLOYMENT_SCRIPT: options.spec.scriptName,
        FLEET_MAINTENANCE_CAPABILITIES: 'required',
        FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY:
          options.maintenanceCapabilityPublicKey,
        FLEET_ARTIFACT_DIGEST: options.artifactDigest,
        FLEET_RUNTIME_CONTRACT: '1',
        OUTBOUND_TENANT_ID: options.spec.tenantTag,
        OUTBOUND_ENVIRONMENT: options.spec.environment,
        OUTBOUND_RESOURCE_GROUP_ID: resourceGroupId,
        OUTBOUND_STATE_SCRIPT_NAME: scriptName,
        OUTBOUND_ROUTE_HOSTNAME: options.spec.routeHostname.toLowerCase(),
        OUTBOUND_POLICY_ID: resourceGroupId,
        ...(options.auditQueueName
          ? { FLEET_AUDIT_PROXY_INGRESS: 'required' }
          : {}),
      },
    });
    this.namespaceIdsByScript.set(
      scriptName,
      new Set([
        ...(this.namespaceIdsByScript.get(scriptName) ?? []),
        ...durableObjectBindings.map(({ namespaceId }) => namespaceId),
      ]),
    );
    return { artifactVersion };
  }

  async putDispatchSecrets(
    scriptName: string,
    _secrets: DeploymentSecrets,
    options?: Readonly<{
      includeMaintenanceAdmin?: boolean;
      additionalSecrets?: Readonly<Record<string, string>>;
    }>,
  ): Promise<void> {
    this.calls.push('secrets');
    this.dispatchSecretOptions.push(options);
    if (this.failSecrets) throw new Error('secret failed');
    const secretNames = [
      ...(options?.includeMaintenanceAdmin === false
        ? ['DEPLOYMENT_IDENTITY_SECRET']
        : ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET']),
      ...Object.keys(options?.additionalSecrets ?? {}),
    ].sort();
    this.dispatchSecretNames.set(scriptName, secretNames);
    const stateWorker = this.dispatchWorkers.get(scriptName);
    if (stateWorker) {
      this.dispatchWorkers.set(scriptName, {
        ...stateWorker,
        secretNames,
      });
    }
  }

  async inspectDispatchWorker(scriptName: string): Promise<
    | {
        artifactVersion: string;
        databaseIds: readonly string[];
        durableObjectBindings: readonly {
          name: string;
          className: string;
          namespaceId: string;
        }[];
        secretNames: readonly string[];
        tenantTag: string;
        environment: string;
        schemaVersion: number;
        desiredSpecDigest: string;
      }
    | undefined
  > {
    this.inspectedScriptNames.push(scriptName);
    const stateWorker = this.dispatchWorkers.get(scriptName);
    if (stateWorker) return stateWorker;
    if (!this.exists || this.deletedScriptNames.includes(scriptName)) {
      return undefined;
    }
    if (scriptName === externalStateScriptName(deployment)) return undefined;
    return {
      artifactVersion:
        this.releaseArtifactVersions.get(scriptName) ??
        this.dispatchArtifactVersion,
      databaseIds: ['db-acme'],
      durableObjectBindings: [],
      secretNames: this.dispatchSecretNames.get(scriptName) ?? [
        'DEPLOYMENT_IDENTITY_SECRET',
      ],
      tenantTag: 'acme',
      environment: 'production',
      schemaVersion: 1,
      desiredSpecDigest:
        this.releaseDigests.get(scriptName) ?? this.desiredSpecDigest,
    };
  }

  async revokeDispatchSecrets(): Promise<void> {
    this.calls.push('revoke');
  }

  async deleteDispatchWorker(scriptName: string): Promise<void> {
    this.calls.push('delete');
    if (this.failDelete) throw new Error('delete failed');
    this.dispatchWorkers.delete(scriptName);
    this.namespaceIdsByScript.delete(scriptName);
    this.deletedScriptNames.push(scriptName);
  }

  async exportDatabase(databaseId: string): Promise<DatabaseExport> {
    return {
      databaseId,
      location: 'r2://fleet-exports/acme.sql',
      sha256: 'a'.repeat(64),
      size: 42,
    };
  }

  async deleteDatabase(): Promise<void> {
    this.calls.push('delete-db');
  }

  async putHostRouting(
    _namespaceId: string,
    _hostname: string,
    target: {
      scriptName: string;
      tenantTag: string;
      environment: string;
      policyId: string;
      policyDigest: string;
      policyHosts: readonly string[];
    },
    guard: PromotionGuard,
  ): Promise<void> {
    this.calls.push('route');
    if (
      this.routeOwner &&
      (!guard.allowedCurrentScriptNames.includes(this.routeOwner.scriptName) ||
        this.routeOwner.tenantTag !== target.tenantTag ||
        this.routeOwner.environment !== target.environment)
    ) {
      throw new Error('route is owned by another deployment');
    }
    this.routeOwner = target;
    this.lastPromotedRoute = target;
  }

  async deleteHostRouting(
    _namespaceId: string,
    _hostname: string,
    allowedTargets: readonly Readonly<{
      scriptName: string;
      tenantTag: string;
      environment: string;
    }>[],
  ): Promise<void> {
    this.calls.push('delete-route');
    if (!this.routeOwner) return;
    if (
      !allowedTargets.some(
        (target) =>
          target.scriptName === this.routeOwner?.scriptName &&
          target.tenantTag === this.routeOwner.tenantTag &&
          target.environment === this.routeOwner.environment,
      )
    ) {
      throw new Error('route is owned by another deployment or release');
    }
    this.routeOwner = undefined;
  }

  async getHostRouting(): Promise<string | undefined> {
    return this.routeOwner ? JSON.stringify(this.routeOwner) : undefined;
  }

  async putScriptInventory(
    _namespaceId: string,
    target: {
      scriptName: string;
      tenantTag: string;
      environment: string;
      databaseId: string;
      routeHostname: string;
    },
  ): Promise<void> {
    this.calls.push('inventory');
    this.scriptInventories.set(target.scriptName, target);
  }

  async deleteScriptInventory(
    _namespaceId: string,
    expected: { scriptName: string },
  ): Promise<void> {
    this.calls.push('delete-inventory');
    this.scriptInventories.delete(expected.scriptName);
  }

  async getScriptInventory(_namespaceId: string, scriptName: string) {
    return this.scriptInventories.get(scriptName);
  }
}

function healthResponse(): Response {
  return Response.json({
    nextSweepAt: 2_000,
    nextPurgeAt: 3_000,
    alarmAt: 2_000,
    lastSweepAt: 1_000,
    deploymentSpecDigest: deploymentSpecDigest(deployment),
  });
}

async function attestedHealthResponse(
  input: string | URL | Request,
  init?: RequestInit,
  rawBody: unknown = { armed: false, alarmAt: null },
): Promise<Response> {
  const url = new URL(
    typeof input === 'string'
      ? input
      : input instanceof URL
        ? input.href
        : input.url,
  );
  const operation = url.pathname.endsWith('/ensure-maintenance')
    ? 'ensure-maintenance'
    : 'maintenance-status';
  const authorization = new Headers(init?.headers).get('authorization');
  const token = authorization?.match(/^Bearer (.+)$/)?.[1] ?? '';
  const claims = await verifyAsymmetricMaintenanceCapability({
    publicKey: JSON.parse(
      MAINTENANCE_CAPABILITY_PUBLIC_KEY,
    ) as MaintenanceCapabilityJwk,
    token,
    operation,
    tenantTag: deployment.tenantTag,
    environment: deployment.environment,
  });
  if (!claims) return new Response(null, { status: 401 });
  const trustedResult = {
    nextSweepAt: 2_000,
    nextPurgeAt: 3_000,
    alarmAt: 2_000,
    lastSweepAt: 1_000,
  };
  const response = Response.json(rawBody);
  response.headers.set(
    MAINTENANCE_RECEIPT_HEADER,
    await mintMaintenanceReceipt(
      secrets.maintenanceAdmin,
      claims,
      trustedResult,
    ),
  );
  return response;
}

describe('WorkersForPlatformsBackend', () => {
  it('provisions dispatch-native state without an ordinary per-deployment Worker', async () => {
    const client = new FakeApi();
    const external = {
      ...deployment,
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'Maintenance' },
      ],
    };
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => platformProfile(),
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: true as const,
    };

    const result = await backend.ensurePlatformResources(
      external,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, external, database),
      fence,
    );

    const stateName = externalStateScriptName(external);
    expect(client.calls).toEqual([
      'inventory',
      `upload-namespaced:${stateName}`,
      'secrets',
    ]);
    expect(result).toMatchObject({
      created: { egressProxy: false, stateWorker: true },
      resources: {
        stateWorker: {
          scriptName: stateName,
          plane: 'dispatch',
          dispatchNamespace: NAMESPACED_STATE.dispatchNamespace,
          durableObjectTag: 'state-v1',
        },
        sharedOutboundWorkerName: NAMESPACED_STATE.sharedOutboundWorkerName,
      },
    });
    expect(client.uploadedControlSpecs).toEqual([]);
    expect(client.controlWorkers.size).toBe(0);
    expect(client.dispatchWorkers.get(stateName)).toMatchObject({
      databaseIds: ['db-acme'],
      durableObjectBindings: [
        expect.objectContaining({
          name: 'MAINTENANCE',
          className: 'Maintenance',
        }),
      ],
      serviceBindings: [
        {
          name: 'OUTBOUND_PROXY',
          service: NAMESPACED_STATE.sharedOutboundWorkerName,
          entrypoint: 'StateEgress',
        },
      ],
    });
  });

  it('removes retained secrets from an existing namespaced state script', async () => {
    const client = new FakeApi();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => platformProfile(),
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: true as const,
    };
    const initial = await backend.ensurePlatformResources(
      deployment,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, deployment, database),
      fence,
    );
    const stateName = initial.resources.stateWorker.scriptName;
    client.dispatchSecretNames.set(stateName, ['LATENT_STATE_SECRET']);

    await backend.ensurePlatformResources(
      deployment,
      database,
      secrets,
      backend.describeExternalPlatformTarget(deployment),
      recordWithPlatformResources(
        backend,
        deployment,
        initial.resources,
        database,
      ),
      fence,
    );

    expect(client.dispatchSecretNames.get(stateName)).toEqual([
      'DEPLOYMENT_IDENTITY_SECRET',
      'MAINTENANCE_ADMIN_SECRET',
      'OUTBOUND_PROXY_CREDENTIAL',
    ]);
    expect(client.controlWorkers.size).toBe(0);
  });

  it('persists the backend-owned audit queue in the platform target and state snapshot', async () => {
    const client = new FakeApi();
    const audited = {
      ...deployment,
      queueProducer: {
        binding: 'AUDIT_QUEUE',
        queueName: 'fleet-audit',
      },
    };
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      auditQueueName: 'fleet-audit',
      platformProfileFor: () =>
        platformProfile({
          stateDurableObjectMigrations: [
            {
              tag: 'state-v1',
              newSqliteClasses: ['Maintenance', 'FlowsafeFleetAuditProxy'],
            },
          ],
        }),
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false as const,
    };
    const target = backend.describeExternalPlatformTarget(audited);

    const result = await backend.ensurePlatformResources(
      audited,
      database,
      secrets,
      target,
      platformConvergenceRecord(backend, audited, database),
      fence,
    );

    expect(target.auditQueueName).toBe('fleet-audit');
    expect(result.resources.auditQueueName).toBe('fleet-audit');
    expect(
      client.dispatchWorkers.get(result.resources.stateWorker.scriptName),
    ).toMatchObject({
      durableObjectBindings: [
        expect.objectContaining({ name: 'FLEET_AUDIT_PROXY_OBJECT' }),
      ],
      queueProducerBindings: [
        { name: 'AUDIT_QUEUE', queueName: 'fleet-audit' },
      ],
      plainTextBindings: { FLEET_AUDIT_PROXY_INGRESS: 'required' },
    });
    const retargeted = {
      ...audited,
      queueProducer: { ...audited.queueProducer, queueName: 'victim-audit' },
    };
    const changedBackend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      auditQueueName: 'victim-audit',
      platformProfileFor: () => platformProfile(),
    });
    await expect(
      changedBackend.ensurePlatformResources(
        retargeted,
        database,
        secrets,
        target,
        platformConvergenceRecord(changedBackend, retargeted, database),
        fence,
      ),
    ).rejects.toThrow(/audit queue is immutable/);
  });

  it('provisions and audits the exact external remote audit Durable Object topology', async () => {
    const client = new FakeApi();
    client.exists = false;
    const audited = {
      ...deployment,
      queueProducer: {
        binding: 'AUDIT_QUEUE',
        queueName: 'fleet-audit',
      },
    };
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      fetch: attestedHealthResponse,
      hostRoutingKvId: 'host-routing',
      auditQueueName: 'fleet-audit',
      platformProfileFor: () =>
        platformProfile({
          stateDurableObjectMigrations: [
            {
              tag: 'state-v1',
              newSqliteClasses: ['Maintenance', 'FlowsafeFleetAuditProxy'],
            },
          ],
        }),
    });
    const result = await provisionDeployment({
      backend,
      store: new MemoryFleetStore(),
      spec: audited,
      secrets,
      clock: () => 1_000,
    });
    const releaseName = externalReleaseScriptName(audited);
    const exact = client.dispatchWorkers.get(releaseName);
    if (!exact) throw new Error('missing provisioned release fixture');
    const auditBinding = {
      name: 'AUDIT_PROXY',
      className: 'FlowsafeFleetAuditProxy',
      namespaceId: 'namespace:FlowsafeFleetAuditProxy',
      scriptName: externalStateScriptName(audited),
      dispatchNamespace: NAMESPACED_STATE.dispatchNamespace,
    };

    expect(result.record.phase).toBe('ready');
    expect(exact.durableObjectBindings).toEqual([auditBinding]);
    expect(exact.serviceBindings).toEqual([]);
    expect(exact.queueProducerBindings).toEqual([]);

    for (const durableObjectBindings of [
      [],
      [{ ...auditBinding, namespaceId: 'namespace:foreign' }],
    ]) {
      client.dispatchWorkers.set(releaseName, {
        ...exact,
        durableObjectBindings,
      });
      await expect(
        provisionDeployment({
          backend,
          store: new MemoryFleetStore(result.record),
          spec: audited,
          secrets,
          clock: () => 2_000,
        }),
      ).rejects.toThrow(/live state does not exactly match/);
    }

    client.dispatchWorkers.set(releaseName, {
      ...exact,
      serviceBindings: [
        {
          name: 'AUDIT_PROXY',
          service: externalStateScriptName(audited),
        },
      ],
    });
    await expect(
      provisionDeployment({
        backend,
        store: new MemoryFleetStore(result.record),
        spec: audited,
        secrets,
        clock: () => 2_000,
      }),
    ).rejects.toThrow(/live state does not exactly match/);
  });

  it('rejects a persisted maintenance verifier change before provider mutation', async () => {
    const client = new FakeApi();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => platformProfile(),
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false as const,
    };
    const target = backend.describeExternalPlatformTarget(deployment);
    const currentRecord = platformConvergenceRecord(
      backend,
      deployment,
      database,
      {
        platformTarget: {
          ...target,
          maintenanceCapabilityPublicKey:
            '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-previous","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
        },
      },
    );

    await expect(
      backend.ensurePlatformResources(
        deployment,
        database,
        secrets,
        target,
        currentRecord,
        fence,
      ),
    ).rejects.toThrow(/verifier is immutable/);
    expect(client.calls).toEqual([]);
    expect(client.uploadedControlSpecs).toEqual([]);
  });

  it('requires dispatch-native state configuration before any provider call', () => {
    const client = new FakeApi();

    expect(
      () =>
        new WorkersForPlatformsBackend({
          client,
          hostRoutingKvId: 'host-routing',
          platformProfileFor: () => platformProfile(),
        } as never),
    ).toThrow(/requires a dispatch namespace/);
    expect(client.calls).toEqual([]);
  });

  it.each([
    {
      label: 'empty dispatch namespace',
      config: { ...NAMESPACED_STATE, dispatchNamespace: '' },
    },
    {
      label: 'empty shared outbound Worker',
      config: { ...NAMESPACED_STATE, sharedOutboundWorkerName: '' },
    },
    {
      label: 'short state-egress secret',
      config: { ...NAMESPACED_STATE, stateEgressRootSecret: 'short' },
    },
  ])('rejects $label before any provider call', ({ config }) => {
    const client = new FakeApi();

    expect(
      () =>
        new WorkersForPlatformsBackend({
          namespacedState: config,
          client,
          hostRoutingKvId: 'host-routing',
        }),
    ).toThrow(/requires a dispatch namespace/);
    expect(client.calls).toEqual([]);
  });

  it.each([
    {
      label: 'tenant',
      tenantTag: 'victim',
      environment: 'production',
      id: 'db-acme',
    },
    {
      label: 'environment',
      tenantTag: 'acme',
      environment: 'staging',
      id: 'db-acme',
    },
    {
      label: 'database',
      tenantTag: 'acme',
      environment: 'production',
      id: 'db-victim',
    },
  ])('rejects a namespaced state collision with another $label', async (owner) => {
    const client = new FakeApi();
    const stateName = externalStateScriptName(deployment);
    client.dispatchWorkers.set(stateName, {
      artifactVersion: 'foreign',
      databaseIds: [owner.id],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      secretNames: [],
      tenantTag: owner.tenantTag,
      environment: owner.environment,
      schemaVersion: deployment.schemaVersion,
      desiredSpecDigest: deploymentSpecDigest(deployment),
      plainTextBindings: {
        FLEET_RESOURCE_ROLE: 'platform-state',
        FLEET_RESOURCE_GROUP: externalPlatformResourceGroupId(deployment),
      },
    });
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => platformProfile(),
    });

    await expect(
      backend.ensurePlatformResources(
        deployment,
        { id: 'db-acme', name: deployment.databaseName, created: false },
        secrets,
        undefined,
        platformConvergenceRecord(backend, deployment, {
          id: 'db-acme',
          name: deployment.databaseName,
          created: false,
        }),
        fence,
      ),
    ).rejects.toThrow(/another owner/);
    expect(client.calls).toEqual([]);
  });

  it('repairs an owned namespaced state script without creating an ordinary Worker', async () => {
    const client = new FakeApi();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => platformProfile(),
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    const initial = await backend.ensurePlatformResources(
      deployment,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, deployment, database),
      fence,
    );
    client.calls.length = 0;

    const repaired = await backend.ensurePlatformResources(
      deployment,
      database,
      secrets,
      backend.describeExternalPlatformTarget(deployment),
      recordWithPlatformResources(
        backend,
        deployment,
        initial.resources,
        database,
      ),
      fence,
    );

    expect(repaired.created).toEqual({
      stateWorker: false,
      egressProxy: false,
    });
    expect(client.calls).toEqual([
      `upload-namespaced:${externalStateScriptName(deployment)}`,
      'secrets',
    ]);
    expect(client.uploadedControlSpecs).toEqual([]);
  });

  it('rejects persisted ordinary platform resources before provider mutation', async () => {
    const client = new FakeApi();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => platformProfile(),
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };

    await expect(
      backend.ensurePlatformResources(
        deployment,
        database,
        secrets,
        undefined,
        recordWithPlatformResources(
          backend,
          deployment,
          platformResources,
          database,
        ),
        fence,
      ),
    ).rejects.toThrow(/dedicated backend-switch lifecycle/);
    expect(client.calls).toEqual([]);
  });

  it('requires compatibility attestation before trusted profile mutation', async () => {
    const client = new FakeApi();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () =>
        ({
          ...platformProfile(),
          backwardCompatibleWithRetainedReleases: false,
        }) as unknown as ExternalPlatformProfile,
    });

    await expect(
      backend.ensurePlatformResources(
        deployment,
        { id: 'db-acme', name: deployment.databaseName, created: false },
        secrets,
        undefined,
        {
          tenantTag: deployment.tenantTag,
          backend: 'workers-for-platforms',
          environment: deployment.environment,
          scriptName: deployment.scriptName,
          databaseId: 'db-acme',
          databaseName: deployment.databaseName,
          schemaVersion: deployment.schemaVersion,
          artifactVersion: 'pending',
          desiredSpecDigest: deploymentSpecDigest(deployment),
          durableObjectBindings: [],
          routeHostname: deployment.routeHostname,
          phase: 'migrated',
          updatedAt: '2026-08-11T00:00:00.000Z',
        },
        fence,
      ),
    ).rejects.toThrow(/attest compatibility/);
    expect(client.calls).toEqual([]);
  });

  it('reconciles trusted profile bytes and policy without changing the external spec', async () => {
    const client = new FakeApi();
    const external = {
      ...deployment,
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'Maintenance' },
      ],
    };
    let profile = platformProfile();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => profile,
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    const initial = await backend.ensurePlatformResources(
      external,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, external, database),
      fence,
    );
    const priorTarget = backend.describeExternalPlatformTarget(external);
    const stateName = externalStateScriptName(external);
    client.calls.length = 0;
    profile = platformProfile({
      stateWorker: {
        ...profile.stateWorker,
        modules: [{ name: 'state.js', content: 'export default {state: 2}' }],
      },
      organizationEgressHosts: ['new-api.example.com'],
    });
    const target = backend.describeExternalPlatformTarget(external);
    const priorRelease = {
      physicalScriptName: externalReleaseScriptName(external),
      specDigest: deploymentSpecDigest(external),
      artifactVersion: 'release-v1',
      releaseSchemaVersion: external.schemaVersion,
    };

    const updated = await backend.ensurePlatformResources(
      external,
      database,
      secrets,
      undefined,
      recordWithPlatformResources(
        backend,
        external,
        initial.resources,
        database,
        {
          phase: 'migrating',
          platformTarget: priorTarget,
          migrationIntent: {
            targetSpecDigest: priorRelease.specDigest,
            priorRelease,
            priorTarget,
            priorOutboundPolicy: priorTarget.outboundPolicy,
            targetRelease: priorRelease,
            target,
            subphase: 'schema-applied',
          },
        },
      ),
      fence,
    );

    expect(updated.created).toEqual({ stateWorker: false, egressProxy: false });
    expect(updated.resources.stateWorker.artifactDigest).not.toBe(
      initial.resources.stateWorker.artifactDigest,
    );
    expect(updated.resources.outboundPolicy?.policyDigest).not.toBe(
      initial.resources.outboundPolicy?.policyDigest,
    );
    expect(client.calls).toEqual([`upload-namespaced:${stateName}`, 'secrets']);
    expect(client.uploadedControlSpecs).toEqual([]);
  });

  it('changes the persisted route policy while retaining zero ordinary Workers', async () => {
    const client = new FakeApi();
    let profile = platformProfile();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => profile,
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    const initial = await backend.ensurePlatformResources(
      deployment,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, deployment, database),
      fence,
    );
    client.calls.length = 0;
    profile = platformProfile({
      organizationEgressHosts: ['new-api.example.com'],
    });

    const updated = await backend.ensurePlatformResources(
      deployment,
      database,
      secrets,
      undefined,
      recordWithPlatformResources(
        backend,
        deployment,
        initial.resources,
        database,
      ),
      fence,
    );

    expect(updated.resources.outboundPolicy?.policyHosts).toEqual([
      'new-api.example.com',
    ]);
    expect(updated.resources.outboundPolicy?.policyDigest).not.toBe(
      initial.resources.outboundPolicy?.policyDigest,
    );
    expect(client.calls).toEqual([
      `upload-namespaced:${externalStateScriptName(deployment)}`,
      'secrets',
    ]);
    expect(client.controlWorkers.size).toBe(0);
  });

  it('describes and enforces the persisted external platform target before mutation', async () => {
    const client = new FakeApi();
    let profile = platformProfile();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => profile,
    });
    const target = backend.describeExternalPlatformTarget(deployment);
    expect(target).toMatchObject({
      d1SchemaVersion: deployment.schemaVersion,
      stateDurableObjectTag: 'state-v1',
      sharedOutboundWorkerName: NAMESPACED_STATE.sharedOutboundWorkerName,
      outboundPolicy: { policyHosts: ['api.example.com'] },
    });
    expect(target.stateArtifactDigest).toMatch(/^[a-f0-9]{64}$/u);
    expect(target.stateDurableObjectHistoryDigest).toMatch(/^[a-f0-9]{64}$/u);
    expect(target.stateEgressCredentialDigest).toMatch(/^[a-f0-9]{64}$/u);
    expect(target.egressArtifactDigest).toBeUndefined();
    expect(target.d1SchemaHistoryDigest).toMatch(/^[a-f0-9]{64}$/u);
    profile = platformProfile({
      stateWorker: {
        ...profile.stateWorker,
        modules: [{ name: 'state.js', content: 'export default {state: 2}' }],
      },
    });

    await expect(
      backend.ensurePlatformResources(
        deployment,
        { id: 'db-acme', name: deployment.databaseName, created: false },
        secrets,
        target,
        platformConvergenceRecord(backend, deployment, {
          id: 'db-acme',
          name: deployment.databaseName,
          created: false,
        }),
        fence,
      ),
    ).rejects.toThrow(/persisted migration target/);
    expect(client.calls).toEqual([]);
  });

  it('attests exact trusted Worker ownership before revoke or deletion', async () => {
    const client = new FakeApi();
    const external = {
      ...deployment,
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'Maintenance' },
      ],
    };
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => platformProfile(),
    });
    const provisioned = await backend.ensurePlatformResources(
      external,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, external, database),
      fence,
    );
    const record = recordWithPlatformResources(
      backend,
      external,
      provisioned.resources,
      database,
    );
    const stateName = provisioned.resources.stateWorker.scriptName;
    const state = client.dispatchWorkers.get(stateName);
    if (!state) throw new Error('state Worker was not created');
    client.dispatchWorkers.set(stateName, {
      ...state,
      databaseIds: ['db-other'],
    });
    client.calls.length = 0;
    await expect(
      backend.revokePlatformResourceCredentials(
        external,
        record,
        database,
        fence,
      ),
    ).rejects.toThrow(/drifted exact bindings/);
    expect(client.calls).not.toContain('revoke');

    client.dispatchWorkers.set(stateName, state);
    const persistedResources = record.platformResources;
    if (!persistedResources) throw new Error('missing platform resources');
    const wrongRecord: FleetRecord = {
      ...record,
      platformResources: {
        ...persistedResources,
        stateWorker: {
          ...persistedResources.stateWorker,
          plane: 'ordinary' as const,
        },
      },
    };
    await expect(
      backend.deletePlatformResources(external, wrongRecord, database, fence),
    ).rejects.toThrow(/dedicated backend-switch lifecycle/);
    expect(client.calls).not.toContain('delete');
  });

  it.each([
    'persisted',
    'provider-committed',
  ] as const)('deletes the exact %s trusted-resource variant after an interrupted migration', async (liveVariant) => {
    const client = new FakeApi();
    const initialSpec = {
      ...deployment,
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'MaintenanceV1' },
      ],
    };
    const targetSpec = {
      ...initialSpec,
      schemaVersion: 2,
      migrations: [
        ...initialSpec.migrations,
        { version: 2, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
      ],
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'MaintenanceV2' },
      ],
    };
    let currentProfile = platformProfile({
      stateDurableObjectMigrations: [
        { tag: 'state-v1', newSqliteClasses: ['MaintenanceV1'] },
      ],
    });
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => currentProfile,
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    const initial = await backend.ensurePlatformResources(
      initialSpec,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, initialSpec, database),
      fence,
    );
    const priorTarget = backend.describeExternalPlatformTarget(initialSpec);
    currentProfile = platformProfile({
      stateWorker: {
        mainModule: 'state.js',
        modules: [{ name: 'state.js', content: 'export default {state: 2}' }],
        compatibilityDate: '2026-08-10',
      },
      stateDurableObjectMigrations: [
        { tag: 'state-v1', newSqliteClasses: ['MaintenanceV1'] },
        { tag: 'state-v2', newSqliteClasses: ['MaintenanceV2'] },
      ],
    });
    const target = backend.describeExternalPlatformTarget(targetSpec);
    const priorRelease = {
      physicalScriptName: externalReleaseScriptName(initialSpec),
      specDigest: deploymentSpecDigest(initialSpec),
      artifactVersion: 'release-v1',
      releaseSchemaVersion: 1,
    };
    const targetRelease = {
      physicalScriptName: externalReleaseScriptName(targetSpec),
      specDigest: deploymentSpecDigest(targetSpec),
      artifactVersion: 'pending',
      releaseSchemaVersion: 2,
    };
    const record = recordWithPlatformResources(
      backend,
      initialSpec,
      initial.resources,
      database,
      {
        phase: 'migrating',
        schemaVersion: 2,
        activeRelease: priorRelease,
        pendingRelease: targetRelease,
        migrationPriorRelease: priorRelease,
        platformTarget: priorTarget,
        migrationIntent: {
          targetSpecDigest: targetRelease.specDigest,
          priorRelease,
          priorTarget,
          priorOutboundPolicy: priorTarget.outboundPolicy,
          targetRelease,
          target,
          subphase: 'schema-applied',
        },
      },
    );

    if (liveVariant === 'provider-committed') {
      await backend.ensurePlatformResources(
        targetSpec,
        database,
        secrets,
        target,
        record,
        fence,
      );
    }

    await expect(
      backend.revokePlatformResourceCredentials(
        targetSpec,
        record,
        database,
        fence,
      ),
    ).resolves.toBeUndefined();
    await expect(
      backend.deletePlatformResources(targetSpec, record, database, fence),
    ).resolves.toBeUndefined();
    expect(client.dispatchWorkers.size).toBe(0);
    if (liveVariant === 'provider-committed') {
      expect(client.namespaceExistenceChecks).toEqual(
        expect.arrayContaining([
          'namespace:MaintenanceV1',
          'namespace:MaintenanceV2',
        ]),
      );
    }
  });

  it('converges an exact persisted schema and Durable Object variant, then retries a provider commit before state persistence', async () => {
    const client = new FakeApi();
    const initialSpec = {
      ...deployment,
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'MaintenanceV1' },
      ],
    };
    const targetSpec = {
      ...initialSpec,
      schemaVersion: 2,
      migrations: [
        ...initialSpec.migrations,
        { version: 2, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
      ],
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'MaintenanceV2' },
      ],
    };
    let profile = platformProfile({
      stateDurableObjectMigrations: [
        { tag: 'state-v1', newSqliteClasses: ['MaintenanceV1'] },
      ],
    });
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => profile,
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    const initial = await backend.ensurePlatformResources(
      initialSpec,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, initialSpec, database),
      fence,
    );
    const priorTarget = backend.describeExternalPlatformTarget(initialSpec);
    profile = platformProfile({
      stateWorker: {
        mainModule: 'state.js',
        modules: [{ name: 'state.js', content: 'export default {state: 2}' }],
        compatibilityDate: '2026-08-10',
      },
      stateDurableObjectMigrations: [
        { tag: 'state-v1', newSqliteClasses: ['MaintenanceV1'] },
        { tag: 'state-v2', newSqliteClasses: ['MaintenanceV2'] },
      ],
    });
    const target = backend.describeExternalPlatformTarget(targetSpec);
    const priorRelease = {
      physicalScriptName: externalReleaseScriptName(initialSpec),
      specDigest: deploymentSpecDigest(initialSpec),
      artifactVersion: 'release-v1',
      releaseSchemaVersion: 1,
    };
    const targetRelease = {
      physicalScriptName: externalReleaseScriptName(targetSpec),
      specDigest: deploymentSpecDigest(targetSpec),
      artifactVersion: 'pending',
      releaseSchemaVersion: 2,
    };
    const migrationRecord = recordWithPlatformResources(
      backend,
      initialSpec,
      initial.resources,
      database,
      {
        phase: 'migrating',
        schemaVersion: 2,
        activeRelease: priorRelease,
        pendingRelease: targetRelease,
        migrationPriorRelease: priorRelease,
        platformTarget: priorTarget,
        migrationIntent: {
          targetSpecDigest: targetRelease.specDigest,
          priorRelease,
          priorTarget,
          priorOutboundPolicy: priorTarget.outboundPolicy,
          targetRelease,
          target,
          subphase: 'schema-applied',
        },
      },
    );

    const converged = await backend.ensurePlatformResources(
      targetSpec,
      database,
      secrets,
      target,
      migrationRecord,
      fence,
    );

    expect(converged.resources.stateWorker).toMatchObject({
      artifactDigest: target.stateArtifactDigest,
      durableObjectTag: 'state-v2',
      durableObjectBindings: [
        expect.objectContaining({
          name: 'MAINTENANCE',
          className: 'MaintenanceV2',
        }),
      ],
      namespaceIds: ['namespace:MaintenanceV1', 'namespace:MaintenanceV2'],
    });
    expect(client.uploadedNamespacedState.at(-1)?.spec).toMatchObject({
      previousDurableObjectTag: 'state-v1',
      durableObjectMigrations: [
        { tag: 'state-v1', newSqliteClasses: ['MaintenanceV1'] },
        { tag: 'state-v2', newSqliteClasses: ['MaintenanceV2'] },
      ],
    });

    client.calls.length = 0;
    const retried = await backend.ensurePlatformResources(
      targetSpec,
      database,
      secrets,
      target,
      migrationRecord,
      fence,
    );
    expect(retried.resources.stateWorker.artifactVersion).toBe(
      converged.resources.stateWorker.artifactVersion,
    );
    expect(client.calls).toContain(
      `upload-namespaced:${externalStateScriptName(targetSpec)}`,
    );
  });

  it('rejects a same-tag rewrite of persisted trusted-state migration history', async () => {
    const client = new FakeApi();
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    let profile = platformProfile();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => profile,
    });
    const initial = await backend.ensurePlatformResources(
      deployment,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, deployment, database),
      fence,
    );
    const record = recordWithPlatformResources(
      backend,
      deployment,
      initial.resources,
      database,
    );
    profile = platformProfile({
      stateDurableObjectMigrations: [
        {
          tag: 'state-v1',
          newSqliteClasses: ['Maintenance', 'LegacyState'],
        },
      ],
    });

    await expect(
      backend.ensurePlatformResources(
        deployment,
        database,
        secrets,
        backend.describeExternalPlatformTarget(deployment),
        record,
        fence,
      ),
    ).rejects.toThrow(/not an exact append-only extension/);
  });

  it.each([
    {
      label: 'delete',
      migration: { tag: 'state-v2', deletedClasses: ['LegacyState'] },
    },
    {
      label: 'rename',
      migration: {
        tag: 'state-v2',
        renamedClasses: [{ from: 'LegacyState', to: 'RenamedState' }],
      },
    },
  ] as const)('rejects a trusted-state $label migration while retained releases may use the class', async ({
    migration,
  }) => {
    const client = new FakeApi();
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    let profile = platformProfile({
      stateDurableObjectMigrations: [
        {
          tag: 'state-v1',
          newSqliteClasses: ['Maintenance', 'LegacyState'],
        },
      ],
    });
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => profile,
    });
    const initial = await backend.ensurePlatformResources(
      deployment,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, deployment, database),
      fence,
    );
    const record = recordWithPlatformResources(
      backend,
      deployment,
      initial.resources,
      database,
    );
    profile = platformProfile({
      stateDurableObjectMigrations: [
        {
          tag: 'state-v1',
          newSqliteClasses: ['Maintenance', 'LegacyState'],
        },
        migration,
      ],
    });

    await expect(
      backend.ensurePlatformResources(
        deployment,
        database,
        secrets,
        backend.describeExternalPlatformTarget(deployment),
        record,
        fence,
      ),
    ).rejects.toThrow(/cannot delete or rename/);
  });

  it('preserves applied schema and state bindings during a rolled-back release profile update', async () => {
    const client = new FakeApi();
    const activeSpec = {
      ...deployment,
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'MaintenanceV1' },
      ],
    };
    const appliedSpec = {
      ...activeSpec,
      schemaVersion: 2,
      migrations: [
        ...activeSpec.migrations,
        { version: 2, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
      ],
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'MaintenanceV2' },
      ],
    };
    let profile = platformProfile({
      stateDurableObjectMigrations: [
        { tag: 'state-v1', newSqliteClasses: ['MaintenanceV1'] },
        { tag: 'state-v2', newSqliteClasses: ['MaintenanceV2'] },
      ],
    });
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
      platformProfileFor: () => profile,
    });
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    const initial = await backend.ensurePlatformResources(
      appliedSpec,
      database,
      secrets,
      undefined,
      platformConvergenceRecord(backend, appliedSpec, database),
      fence,
    );
    const priorTarget = backend.describeExternalPlatformTarget(appliedSpec);
    profile = platformProfile({
      stateWorker: {
        mainModule: 'state.js',
        modules: [{ name: 'state.js', content: 'export default {state: 3}' }],
        compatibilityDate: '2026-08-10',
      },
      stateDurableObjectMigrations: [
        { tag: 'state-v1', newSqliteClasses: ['MaintenanceV1'] },
        { tag: 'state-v2', newSqliteClasses: ['MaintenanceV2'] },
      ],
    });
    const expectedTarget = {
      ...backend.describeExternalPlatformTarget(activeSpec),
      d1SchemaVersion: priorTarget.d1SchemaVersion,
      d1SchemaHistoryDigest: priorTarget.d1SchemaHistoryDigest,
    };
    const activeRelease = {
      physicalScriptName: externalReleaseScriptName(activeSpec),
      specDigest: deploymentSpecDigest(activeSpec),
      artifactVersion: 'release-v1',
      releaseSchemaVersion: 1,
    };
    const record = recordWithPlatformResources(
      backend,
      appliedSpec,
      initial.resources,
      database,
      {
        schemaVersion: 2,
        desiredSpecDigest: activeRelease.specDigest,
        activeRelease,
        platformTarget: priorTarget,
        durableObjectBindings: [
          {
            name: 'MAINTENANCE',
            className: 'MaintenanceV1',
            namespaceId: 'candidate-maintenance-v1',
          },
        ],
        phase: 'migrating',
        migrationIntent: {
          platformOnly: true,
          targetSpecDigest: activeRelease.specDigest,
          priorRelease: activeRelease,
          priorTarget,
          priorOutboundPolicy: priorTarget.outboundPolicy,
          targetRelease: activeRelease,
          target: expectedTarget,
          subphase: 'schema-applied',
        },
      },
    );

    const converged = await backend.ensurePlatformResources(
      activeSpec,
      database,
      secrets,
      expectedTarget,
      record,
      fence,
    );

    expect(converged.resources.stateWorker.durableObjectBindings).toEqual([
      expect.objectContaining({
        name: 'MAINTENANCE',
        className: 'MaintenanceV2',
      }),
    ]);
    const stateUpload = client.uploadedNamespacedState.at(-1);
    expect(stateUpload?.spec).toMatchObject({
      schemaVersion: 2,
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'MaintenanceV2' },
      ],
    });
  });

  it('delegates immutable database ID lookup to the Cloudflare client', async () => {
    const client = new FakeApi();
    const subject = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
    });

    await expect(subject.getDatabase('db-persisted')).resolves.toEqual({
      id: 'db-persisted',
      name: deployment.databaseName,
      created: false,
    });
    expect(client.databaseIdsRead).toEqual(['db-persisted']);
  });

  it('recovers a D1 create committed before the provider response was lost', async () => {
    const client = new FakeApi();
    client.failDatabaseCreateAfterCommit = true;
    const subject = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
    });

    await expect(subject.ensureDatabase(deployment, fence)).resolves.toEqual({
      id: 'db-acme',
      name: deployment.databaseName,
      created: true,
    });
  });

  it('rejects an authorized D1 create race that resolves to another owner', async () => {
    const client = new FakeApi();
    client.failDatabaseCreateAfterCommit = true;
    const subject = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
    });
    subject.readDeploymentIdentity = async () => 'other-tenant';

    await expect(subject.ensureDatabase(deployment, fence)).rejects.toThrow(
      /owned by 'other-tenant'/,
    );
    expect(client.database).toMatchObject({ name: deployment.databaseName });
  });

  it('runs D1 ownership reads inside the provider mutation fence', async () => {
    const client = new FakeApi();
    const subject = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client,
      hostRoutingKvId: 'host-routing',
    });

    await expect(
      subject.readDeploymentIdentity(
        { id: 'db-persisted', name: deployment.databaseName, created: false },
        fence,
      ),
    ).resolves.toBeUndefined();
    expect(client.mutationFenceEntries).toBe(1);
  });

  it('uses authenticated fixed maintenance endpoints', async () => {
    const fetch = vi.fn(attestedHealthResponse);
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: new FakeApi(),
      fetch,
      hostRoutingKvId: 'host-routes',
      platformProfileFor: () => platformProfile(),
    });

    await expect(
      backend.ensureMaintenance(
        deployment,
        secrets.maintenanceAdmin,
        fence,
        'etag-v1',
      ),
    ).resolves.toMatchObject({ armed: true, nextAlarmAt: 2_000 });
    await backend.inspect(deployment, secrets.maintenanceAdmin);

    expect(String(fetch.mock.calls[0]?.[0])).toBe(
      `https://control-acme.example.test/.well-known/anchorage/maintenance/acme/production/${externalReleaseScriptName(deployment)}/${deploymentSpecDigest(deployment)}/ensure-maintenance`,
    );
    expect(String(fetch.mock.calls[1]?.[0])).toBe(
      `https://control-acme.example.test/.well-known/anchorage/maintenance/acme/production/${externalReleaseScriptName(deployment)}/${deploymentSpecDigest(deployment)}/maintenance-status`,
    );
    for (const [, init] of fetch.mock.calls) {
      expect(new Headers(init?.headers).get('authorization')).toMatch(
        /^Bearer ey/,
      );
      expect(init?.signal).toBeInstanceOf(AbortSignal);
    }
  });

  it('uses only the signed trusted result when candidate response body is forged', async () => {
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: new FakeApi(),
      fetch: (input, init) =>
        attestedHealthResponse(input, init, {
          armed: true,
          alarmAt: 999_999,
          lastSweepAt: 999_999,
        }),
      hostRoutingKvId: 'host-routes',
      platformProfileFor: () => platformProfile(),
    });

    await expect(
      backend.ensureMaintenance(
        deployment,
        secrets.maintenanceAdmin,
        fence,
        'etag-v1',
      ),
    ).resolves.toMatchObject({
      armed: true,
      nextAlarmAt: 2_000,
      lastSweepAt: 1_000,
    });
  });

  it('rejects maintenance timeout compositions that outlive the lease or capability', async () => {
    const fetch = vi.fn(attestedHealthResponse);
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: new FakeApi(),
      fetch,
      hostRoutingKvId: 'host-routes',
      platformProfileFor: () => platformProfile(),
      maintenanceRequestTimeoutMs: 10_000,
    });
    await expect(
      backend.ensureMaintenance(deployment, secrets.maintenanceAdmin, {
        mutationLeaseTtlMs: 10_000,
        assertOwned: vi.fn(),
      }),
    ).rejects.toThrow(/shorter than the mutation lease/);
    expect(fetch).not.toHaveBeenCalled();
    expect(
      () =>
        new WorkersForPlatformsBackend({
          namespacedState: NAMESPACED_STATE,
          client: new FakeApi(),
          hostRoutingKvId: 'host-routes',
          maintenanceRequestTimeoutMs: 55_001,
        }),
    ).toThrow(/one-minute capability lifetime/);
  });

  it('rejects a control dispatcher response from the wrong physical release', async () => {
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: new FakeApi(),
      fetch: async () =>
        Response.json({
          alarmAt: 2_000,
          deploymentSpecDigest: 'b'.repeat(64),
        }),
      hostRoutingKvId: 'host-routes',
      platformProfileFor: () => platformProfile(),
    });

    await expect(
      backend.ensureMaintenance(
        deployment,
        secrets.maintenanceAdmin,
        fence,
        'etag-v1',
      ),
    ).rejects.toThrow(/did not attest fleet specification digest/);
    await expect(
      backend.inspect(deployment, secrets.maintenanceAdmin),
    ).rejects.toThrow(/does not match dispatch Worker/);
  });

  it('deletes an uploaded script when credential installation fails', async () => {
    const api = new FakeApi();
    api.failSecrets = true;
    api.exists = false;
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });

    await expect(
      backend.deployWorker(
        deployment,
        { id: 'db-acme', name: 'acme-production', created: true },
        secrets,
        platformResources,
        fence,
        'pending',
      ),
    ).rejects.toThrow(/secret failed/);
    expect(api.calls).toEqual([
      'inventory',
      'upload',
      'secrets',
      'revoke',
      'delete-route',
      'delete',
      'delete-inventory',
    ]);
  });

  it('does not replace or delete an existing artifact when secret rotation fails', async () => {
    const api = new FakeApi();
    api.failSecrets = true;
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });

    await expect(
      backend.deployWorker(
        deployment,
        { id: 'db-acme', name: 'acme-production', created: false },
        secrets,
        platformResources,
        fence,
        'etag-v1',
      ),
    ).rejects.toThrow(/secret failed/);
    expect(api.calls).toEqual(['inventory', 'secrets']);
    expect(api.dispatchSecretOptions).toEqual([
      { includeMaintenanceAdmin: false },
    ]);
  });

  it('reuses an exact immutable release without uploading and keeps platform-authored uploads mutable', async () => {
    const api = new FakeApi();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });
    await expect(
      backend.deployWorker(
        deployment,
        { id: 'db-acme', name: 'acme-production', created: false },
        secrets,
        platformResources,
        fence,
        'etag-v1',
      ),
    ).resolves.toMatchObject({
      created: false,
      physicalScriptName: externalReleaseScriptName(deployment),
    });
    expect(api.uploadedScriptNames).toEqual([]);
    expect(api.dispatchSecretOptions).toEqual([
      { includeMaintenanceAdmin: false },
    ]);
    api.calls.length = 0;

    const platform = { ...deployment, authoredBy: 'platform' as const };
    await backend.deployWorker(
      platform,
      { id: 'db-acme', name: 'acme-production', created: false },
      secrets,
      undefined,
      fence,
      undefined,
    );
    expect(api.uploadedScriptNames).toEqual([platform.scriptName]);
    expect(api.calls.slice(0, 3)).toEqual(['inventory', 'secrets', 'upload']);
    expect(api.dispatchSecretOptions.at(-1)).toEqual({
      includeMaintenanceAdmin: true,
    });
  });

  it('rejects an out-of-band immutable dispatch artifact overwrite before secrets or upload', async () => {
    const api = new FakeApi();
    api.dispatchArtifactVersion = 'out-of-band-overwrite';
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });

    await expect(
      backend.deployWorker(
        deployment,
        { id: 'db-acme', name: 'acme-production', created: false },
        secrets,
        platformResources,
        fence,
        'etag-v1',
      ),
    ).rejects.toThrow(/persisted artifact version/);
    expect(api.calls).toEqual([]);
  });

  it('reuploads exact external bytes when only a pending provider outcome is durable', async () => {
    const api = new FakeApi();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });

    await expect(
      backend.deployWorker(
        deployment,
        { id: 'db-acme', name: 'acme-production', created: false },
        secrets,
        platformResources,
        fence,
        'pending',
      ),
    ).resolves.toMatchObject({ artifactVersion: 'etag-v1', created: false });
    expect(api.calls).toContain('upload');
    expect(api.uploadedScriptNames).toEqual([
      externalReleaseScriptName(deployment),
    ]);
  });

  it('reports uncertain partial resources when new-upload cleanup cannot prove deletion', async () => {
    const api = new FakeApi();
    api.exists = false;
    api.failSecrets = true;
    api.failDelete = true;
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });

    const failure = await backend
      .deployWorker(
        deployment,
        { id: 'db-acme', name: 'acme-production', created: true },
        secrets,
        platformResources,
        fence,
        'pending',
      )
      .catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(WorkerDeploymentError);
    expect(failure).toMatchObject({
      createdByAttempt: true,
      resourceState: 'unknown',
    });
  });

  it('reserves inventory before upload and retains it after an ambiguous upload failure', async () => {
    const api = new FakeApi();
    api.exists = false;
    api.failUpload = true;
    api.failDelete = true;
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });

    await expect(
      backend.deployWorker(
        deployment,
        { id: 'db-acme', name: 'acme-production', created: true },
        secrets,
        platformResources,
        fence,
        'pending',
      ),
    ).rejects.toMatchObject({ resourceState: 'unknown' });
    expect(api.calls.slice(0, 2)).toEqual(['inventory', 'upload']);
    expect(api.calls).not.toContain('delete-inventory');
  });

  it('derives a bounded content-addressed script name and rejects a colliding build', async () => {
    const api = new FakeApi();
    api.desiredSpecDigest = 'b'.repeat(64);
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });
    const longSpec = {
      ...deployment,
      scriptName: 'a'.repeat(63),
    };
    const physical = externalReleaseScriptName(longSpec);

    expect(physical).toHaveLength(63);
    await expect(
      backend.deployWorker(
        longSpec,
        { id: 'db-acme', name: 'acme-production', created: false },
        secrets,
        platformResources,
        fence,
        'pending',
      ),
    ).rejects.toThrow(/different build/);
    expect(api.calls).toEqual([]);
    expect(api.inspectedScriptNames).toEqual([physical]);
  });

  it('rejects ambiguous D1 bindings during drift inspection', async () => {
    const api = new FakeApi();
    api.inspectDispatchWorker = async () => ({
      artifactVersion: 'etag-v1',
      databaseIds: ['db-one', 'db-two'],
      durableObjectBindings: [],
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
      tenantTag: 'acme',
      environment: 'production',
      schemaVersion: 1,
      desiredSpecDigest: 'a'.repeat(64),
    });
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      fetch: vi.fn(async () => healthResponse()),
      hostRoutingKvId: 'host-routes',
    });
    await expect(
      backend.inspect(deployment, secrets.maintenanceAdmin),
    ).rejects.toThrow(/exactly one D1 binding/);
  });

  it('refuses cross-owner route promotion and removes active plus retained releases', async () => {
    const api = new FakeApi();
    api.routeOwner = {
      scriptName: 'beta-release',
      tenantTag: 'beta',
      environment: 'production',
    };
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
      platformProfileFor: () => platformProfile(),
    });
    await expect(
      backend.promoteWorker(
        deployment,
        {
          allowedCurrentScriptNames: [externalReleaseScriptName(deployment)],
          allowUnrouted: true,
        },
        platformResources.egressProxy,
        fence,
        'etag-v1',
      ),
    ).rejects.toThrow(/another deployment/);

    api.routeOwner = undefined;
    await backend.promoteWorker(
      deployment,
      {
        allowedCurrentScriptNames: [],
        allowUnrouted: true,
      },
      platformResources.egressProxy,
      fence,
      'etag-v1',
    );
    expect(api.routeOwner).toMatchObject({
      tenantTag: 'acme',
      environment: 'production',
      policyHosts: ['api.example.com'],
    });
    expect(api.lastPromotedRoute?.policyDigest).toMatch(/^[a-f0-9]{64}$/u);

    api.routeOwner = undefined;
    const retained = {
      physicalScriptName: 'acme-production-retained000000',
      specDigest: 'b'.repeat(64),
      artifactVersion: 'etag-retained',
      releaseSchemaVersion: 1,
    };
    api.releaseDigests.set(retained.physicalScriptName, retained.specDigest);
    api.releaseArtifactVersions.set(
      retained.physicalScriptName,
      retained.artifactVersion,
    );
    const database = {
      id: 'db-acme',
      name: 'acme-production',
      created: false,
    };
    await backend.revokeCredentials(
      deployment,
      [retained],
      undefined,
      database,
      fence,
    );
    api.routeOwner = {
      scriptName: retained.physicalScriptName,
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
    };
    await backend.removeTraffic(
      deployment,
      [retained],
      undefined,
      database,
      fence,
    );
    await backend.deleteWorker(
      deployment,
      [retained],
      database,
      undefined,
      fence,
    );
    expect(api.deletedScriptNames).toEqual([
      externalReleaseScriptName(deployment),
      retained.physicalScriptName,
    ]);
    expect(api.routeOwner).toBeUndefined();
    expect(api.calls.indexOf('delete-route')).toBeLessThan(
      api.calls.indexOf('delete'),
    );
    expect(
      api.calls.filter((call) => call === 'delete-inventory'),
    ).toHaveLength(2);
  });

  it('aborts cleanup before script deletion for an unexpected route and converges after a partial deletion retry', async () => {
    const api = new FakeApi();
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });
    const active = {
      physicalScriptName: externalReleaseScriptName(deployment),
      specDigest: deploymentSpecDigest(deployment),
      artifactVersion: 'etag-active',
      releaseSchemaVersion: 1,
    };
    const pending = {
      physicalScriptName: 'acme-pending-release',
      specDigest: 'd'.repeat(64),
      artifactVersion: 'etag-pending',
      releaseSchemaVersion: 1,
    };
    api.releaseDigests.set(active.physicalScriptName, active.specDigest);
    api.releaseDigests.set(pending.physicalScriptName, pending.specDigest);
    api.releaseArtifactVersions.set(
      active.physicalScriptName,
      active.artifactVersion,
    );
    api.releaseArtifactVersions.set(
      pending.physicalScriptName,
      pending.artifactVersion,
    );
    api.routeOwner = {
      scriptName: 'acme-unexpected-release',
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
    };

    await expect(
      backend.removeTraffic(
        deployment,
        [pending],
        active,
        { id: 'db-acme', name: deployment.databaseName, created: false },
        fence,
      ),
    ).rejects.toThrow(/another deployment or release/);
    expect(api.deletedScriptNames).toEqual([]);

    api.routeOwner = {
      scriptName: pending.physicalScriptName,
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
    };
    api.failDelete = true;
    await backend.removeTraffic(
      deployment,
      [pending],
      active,
      { id: 'db-acme', name: deployment.databaseName, created: false },
      fence,
    );
    await expect(
      backend.deleteWorker(
        deployment,
        [pending],
        { id: 'db-acme', name: deployment.databaseName, created: false },
        active,
        fence,
      ),
    ).rejects.toThrow(/failed to delete 2 Worker artifact/);
    expect(api.routeOwner).toBeUndefined();

    api.failDelete = false;
    await expect(
      backend.deleteWorker(
        deployment,
        [pending],
        { id: 'db-acme', name: deployment.databaseName, created: false },
        active,
        fence,
      ),
    ).resolves.toBeUndefined();
    expect(api.deletedScriptNames).toEqual([
      active.physicalScriptName,
      pending.physicalScriptName,
    ]);
  });

  it('refuses release deletion when the live D1 binding has another owner', async () => {
    const api = new FakeApi();
    api.inspectDispatchWorker = async (scriptName) => ({
      artifactVersion: 'etag-v1',
      databaseIds: ['db-other'],
      durableObjectBindings: [],
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
      schemaVersion: deployment.schemaVersion,
      desiredSpecDigest:
        api.releaseDigests.get(scriptName) ?? deploymentSpecDigest(deployment),
    });
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });

    await expect(
      backend.deleteWorker(
        deployment,
        undefined,
        { id: 'db-acme', name: deployment.databaseName, created: false },
        undefined,
        fence,
      ),
    ).rejects.toThrow(/owned by another build or deployment/);
    expect(api.calls).not.toContain('delete-route');
    expect(api.calls).not.toContain('delete');
  });

  it('does not retire inventory until script absence is positively observed', async () => {
    const api = new FakeApi();
    api.deleteDispatchWorker = async () => {
      api.calls.push('delete');
    };
    const release = {
      physicalScriptName: externalReleaseScriptName(deployment),
      specDigest: deploymentSpecDigest(deployment),
      artifactVersion: 'etag-v1',
      releaseSchemaVersion: deployment.schemaVersion,
    };
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });

    await expect(
      backend.deleteRetainedRelease(
        deployment,
        release,
        { id: 'db-acme', name: deployment.databaseName, created: false },
        fence,
      ),
    ).rejects.toThrow(/remains after deletion/);
    expect(api.calls).not.toContain('delete-inventory');
  });

  it('requires positive route, script, and inventory absence before D1 deletion', async () => {
    const api = new FakeApi();
    const physicalScriptName = externalReleaseScriptName(deployment);
    const database = {
      id: 'db-acme',
      name: deployment.databaseName,
      created: false,
    };
    const record: FleetRecord = {
      tenantTag: deployment.tenantTag,
      backend: 'workers-for-platforms',
      environment: deployment.environment,
      scriptName: deployment.scriptName,
      databaseId: database.id,
      databaseName: database.name,
      schemaVersion: deployment.schemaVersion,
      artifactVersion: 'etag-v1',
      desiredSpecDigest: deploymentSpecDigest(deployment),
      activeRelease: {
        physicalScriptName,
        specDigest: deploymentSpecDigest(deployment),
        artifactVersion: 'etag-v1',
        releaseSchemaVersion: deployment.schemaVersion,
      },
      durableObjectBindings: [
        {
          name: 'MAINTENANCE',
          className: 'Maintenance',
          namespaceId: 'namespace-maintenance',
        },
      ],
      routeHostname: deployment.routeHostname,
      phase: 'database-deleting',
      updatedAt: '2026-08-11T00:00:00.000Z',
    };
    const backend = new WorkersForPlatformsBackend({
      namespacedState: NAMESPACED_STATE,
      client: api,
      hostRoutingKvId: 'host-routes',
    });
    api.routeOwner = {
      scriptName: physicalScriptName,
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
    };
    await expect(
      backend.assertDatabaseDetached(deployment, record, database, fence),
    ).rejects.toThrow(/host route.*remains/);

    api.routeOwner = undefined;
    await expect(
      backend.assertDatabaseDetached(deployment, record, database, fence),
    ).rejects.toThrow(/dispatch release.*remains/);

    api.exists = false;
    api.scriptInventories.set(physicalScriptName, {
      scriptName: physicalScriptName,
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
      databaseId: database.id,
      routeHostname: deployment.routeHostname,
    });
    await expect(
      backend.assertDatabaseDetached(deployment, record, database, fence),
    ).rejects.toThrow(/script inventory.*remains/);

    api.scriptInventories.clear();
    api.remainingNamespaceIds.add('namespace-maintenance');
    await expect(
      backend.assertDatabaseDetached(deployment, record, database, fence),
    ).rejects.toThrow(/Durable Object namespace.*remains/);

    api.remainingNamespaceIds.clear();
    api.databaseAttachments.push({
      scriptName: 'unrelated-ordinary-worker',
      plane: 'ordinary',
    });
    await expect(
      backend.assertDatabaseDetached(deployment, record, database, fence),
    ).rejects.toThrow(/ordinary:unrelated-ordinary-worker/);

    api.databaseAttachments.length = 0;
    api.databaseAttachments.push({
      scriptName: 'unrelated-dispatch-worker',
      plane: 'dispatch',
    });
    await expect(
      backend.assertDatabaseDetached(deployment, record, database, fence),
    ).rejects.toThrow(/dispatch:unrelated-dispatch-worker/);

    api.databaseAttachments.length = 0;
    await expect(
      backend.assertDatabaseDetached(deployment, record, database, fence),
    ).resolves.toBeUndefined();
  });
});
