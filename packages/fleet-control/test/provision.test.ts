// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type {
  BridgeMutationPlan,
  BridgeSnapshot,
  FinalizedOrdinaryStateProvider,
} from '../src/backend-switch.js';
import {
  canonicalDeploymentEgressPolicy,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalReleaseTopology,
  externalStateScriptName,
  FLEET_AUDIT_PROXY_CLASS_NAME,
  FLEET_AUDIT_PROXY_STATE_BINDING,
} from '../src/platform-resources.js';
import {
  assertLiveDeploymentMatches,
  cleanupDeploymentArtifacts,
  decommissionDeployment,
  ProvisioningError,
  provisionDeployment,
} from '../src/provision.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  ApplicationR2BucketSnapshot,
  ApplicationR2Resource,
  DatabaseReference,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  ProvisioningBackend,
  ProvisioningBackendKind,
} from '../src/types.js';
import { externalReleaseScriptName } from '../src/workers-for-platforms-backend.js';

const secrets: DeploymentSecrets = {
  deploymentIdentity: 'deployment-identity-secret-value-0001',
  maintenanceAdmin: 'maintenance-admin-secret-value-00001',
};

const MAINTENANCE_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

function spec(overrides: Partial<DeploymentSpec> = {}): DeploymentSpec {
  return {
    tenantTag: 'acme',
    environment: 'production',
    scriptName: 'acme-production',
    databaseName: 'acme-production',
    compatibilityDate: '2026-08-10',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'platform',
    schemaVersion: 3,
    migrations: [
      { version: 1, sql: 'CREATE TABLE example (id TEXT PRIMARY KEY)' },
      { version: 2, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
      { version: 3, sql: 'ALTER TABLE example ADD COLUMN note TEXT' },
    ],
    durableObjectMigrations: [{ tag: 'v1', newSqliteClasses: ['Maintenance'] }],
    durableObjectBindings: [{ name: 'MAINTENANCE', className: 'Maintenance' }],
    egressProxyService: 'fleet-egress-proxy',
    maintenanceBaseUrl: 'https://control-acme.example.test',
    routeHostname: 'acme.example.test',
    ...overrides,
  };
}

class MemoryStore implements FleetStateStore {
  record: FleetRecord | undefined;
  leased = false;
  readonly phases: string[] = [];
  failPutPhase: string | undefined;

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    if (this.leased) throw new Error('deployment is already being modified');
    this.leased = true;
    try {
      return await operation({
        tenantTag,
        environment,
        mutationLeaseTtlMs: 15 * 60_000,
        assertOwned: async () => {},
        renew: async () => {},
        put: (record) => this.put(record),
        delete: () => this.delete(),
      });
    } finally {
      this.leased = false;
    }
  }

  async get(): Promise<FleetRecord | undefined> {
    return this.record;
  }

  async put(record: FleetRecord): Promise<void> {
    if (this.failPutPhase === record.phase) {
      this.failPutPhase = undefined;
      throw new Error(`failed state write at ${record.phase}`);
    }
    this.record = record;
    this.phases.push(record.phase);
  }

  async delete(): Promise<void> {
    this.record = undefined;
  }

  async list(): Promise<readonly FleetRecord[]> {
    return this.record ? [this.record] : [];
  }
}

class CommitThenThrowStore extends MemoryStore {
  readonly applicationStateWrites: Array<
    readonly Readonly<{
      name: string;
      state: ApplicationR2Resource['state'];
    }>[]
  > = [];
  failAfterCommittedApplicationState:
    | Readonly<{ name: string; state: ApplicationR2Resource['state'] }>
    | undefined;
  failAfterCommittedPhase: FleetRecord['phase'] | undefined;

  override async put(record: FleetRecord): Promise<void> {
    await super.put(record);
    this.applicationStateWrites.push(
      (record.applicationResources ?? []).map(({ name, state }) => ({
        name,
        state,
      })),
    );
    if (this.failAfterCommittedPhase === record.phase) {
      this.failAfterCommittedPhase = undefined;
      throw new Error(
        `state write response was lost after committing ${record.phase}`,
      );
    }
    const failure = this.failAfterCommittedApplicationState;
    if (
      failure &&
      record.applicationResources?.some(
        (resource) =>
          resource.name === failure.name && resource.state === failure.state,
      )
    ) {
      this.failAfterCommittedApplicationState = undefined;
      throw new Error(
        `state write response was lost after committing ${failure.name}:${failure.state}`,
      );
    }
  }
}

const maintenance: MaintenanceHealth = {
  armed: true,
  nextAlarmAt: 2_000,
  lastSweepAt: 1_000,
  lastPurgeAt: 1_000,
};

class FakeBackend implements ProvisioningBackend {
  readonly kind: ProvisioningBackendKind;
  readonly immutableExternalArtifacts?: true;
  readonly events: string[] = [];
  failAt: string | undefined;
  cleanupFailAt: string | undefined;
  live: LiveDeployment | undefined;
  exportLocation = 'r2://fleet-exports/acme.sql';
  databaseExists = false;
  databaseId = 'database-id';
  databaseName = 'acme-production';
  databaseOwner: string | undefined;
  readonly databaseIdsRead: string[] = [];
  findDatabaseCalls = 0;
  retainedReleases: readonly ExternalReleaseSnapshot[] = [];
  activeRelease: ExternalReleaseSnapshot | undefined;
  platformStateDigest = 'a'.repeat(64);
  platformPolicyHosts: readonly string[] = ['api.example.com'];
  deletedPlatformNamespaceIds: readonly string[] = [];
  platformBootstrapPresent = false;
  trafficRemoved = false;
  trafficDrift = false;
  removeTrafficCalls = 0;
  assertTrafficRemovedCalls = 0;
  failRemoveTrafficResponseOnce = false;

  constructor(kind: ProvisioningBackendKind = 'workers-for-platforms') {
    this.kind = kind;
    this.immutableExternalArtifacts =
      kind === 'workers-for-platforms' ? true : undefined;
  }

  async findDatabase(): Promise<DatabaseReference | undefined> {
    this.findDatabaseCalls += 1;
    return this.databaseExists
      ? {
          id: this.databaseId,
          name: this.databaseName,
          created: false,
        }
      : undefined;
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    this.databaseIdsRead.push(databaseId);
    return this.databaseExists && databaseId === this.databaseId
      ? {
          id: this.databaseId,
          name: this.databaseName,
          created: false,
        }
      : undefined;
  }

  #event(name: string): void {
    this.events.push(name);
    if (this.failAt === name || this.cleanupFailAt === name) {
      throw new Error(`failed at ${name}`);
    }
  }

  async ensureDatabase(): Promise<DatabaseReference> {
    if (this.databaseExists) {
      if (this.databaseOwner !== undefined) {
        throw new Error(
          `refusing authorized database reconciliation for '${this.databaseId}' owned by '${this.databaseOwner}'`,
        );
      }
      return {
        id: this.databaseId,
        name: this.databaseName,
        created: true,
      };
    }
    this.databaseExists = true;
    this.#event('database');
    return { id: 'database-id', name: 'acme-production', created: true };
  }

  async seedDeploymentIdentity(
    _database: DatabaseReference,
    tenantTag: string,
  ): Promise<void> {
    this.#event('identity');
    this.databaseOwner = tenantTag;
  }

  async readDeploymentIdentity(): Promise<string | undefined> {
    return this.databaseOwner;
  }

  async applyMigrations(): Promise<void> {
    this.#event('migrations');
  }

  describeExternalPlatformTarget(deployment: DeploymentSpec) {
    return {
      ...(deployment.queueProducer
        ? { auditQueueName: deployment.queueProducer.queueName }
        : {}),
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateArtifactDigest: this.platformStateDigest,
      stateDurableObjectHistoryDigest: 'd'.repeat(64),
      egressArtifactDigest: 'b'.repeat(64),
      d1SchemaVersion: deployment.schemaVersion,
      d1SchemaHistoryDigest: deploymentSpecDigest(deployment),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: externalPlatformResourceGroupId(deployment),
        tenantTag: deployment.tenantTag,
        environment: deployment.environment,
        allowedHosts: this.platformPolicyHosts,
      }),
    };
  }

  async ensurePlatformResources(deployment: DeploymentSpec) {
    this.#event('platform-resources');
    if (this.failAt === 'platform-privatization') {
      this.platformBootstrapPresent = true;
      this.#event('platform-privatization');
    }
    const stateScriptName = externalStateScriptName(deployment);
    const target = this.describeExternalPlatformTarget(deployment);
    const durableObjectBindings = [
      ...deployment.durableObjectBindings.map((binding) => ({
        ...binding,
        namespaceId: `state-${deployment.scriptName}-${binding.name}`,
      })),
      ...(deployment.queueProducer
        ? [
            {
              name: FLEET_AUDIT_PROXY_STATE_BINDING,
              className: FLEET_AUDIT_PROXY_CLASS_NAME,
              namespaceId: `state-${deployment.scriptName}-${FLEET_AUDIT_PROXY_STATE_BINDING}`,
            },
          ]
        : []),
    ];
    this.platformBootstrapPresent = false;
    const resources = {
      ...(deployment.queueProducer
        ? { auditQueueName: deployment.queueProducer.queueName }
        : {}),
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: stateScriptName,
        artifactVersion: 'state-v1',
        artifactDigest: this.platformStateDigest,
        durableObjectBindings,
        namespaceIds: durableObjectBindings.map(
          ({ namespaceId }) => namespaceId,
        ),
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(deployment),
        artifactVersion: 'egress-v1',
        artifactDigest: 'b'.repeat(64),
        ...target.outboundPolicy,
      },
    };
    if (this.live) {
      const topology = externalReleaseTopology(deployment, resources);
      this.live = {
        ...this.live,
        durableObjectBindings: topology.durableObjectBindings,
        serviceBindings: topology.serviceBindings,
        queueProducerBindings: topology.queueProducerBindings,
      };
    }
    return {
      resources,
      created: { stateWorker: true, egressProxy: true },
    };
  }

  async deployWorker(
    deployment: DeploymentSpec,
    _database?: DatabaseReference,
    _secrets?: DeploymentSecrets,
    platformResources?: FleetRecord['platformResources'],
  ): Promise<{
    artifactVersion: string;
    created: boolean;
    physicalScriptName?: string;
  }> {
    this.#event('worker');
    const externalTopology =
      deployment.authoredBy === 'external'
        ? externalReleaseTopology(deployment, platformResources)
        : undefined;
    this.live = {
      tenantTag: 'acme',
      environment: 'production',
      scriptName:
        deployment.authoredBy === 'external'
          ? externalReleaseScriptName(deployment)
          : deployment.scriptName,
      databaseId: 'database-id',
      durableObjectBindings:
        externalTopology?.durableObjectBindings ??
        deployment.durableObjectBindings.map((binding) => ({
          ...binding,
          namespaceId: 'maintenance-namespace',
        })),
      serviceBindings:
        externalTopology?.serviceBindings ??
        (deployment.authoredBy === 'external'
          ? []
          : deployment.egressProxyService
            ? [
                {
                  name: 'EGRESS_PROXY',
                  service: deployment.egressProxyService,
                },
              ]
            : []),
      queueProducerBindings:
        externalTopology?.queueProducerBindings ??
        (deployment.authoredBy === 'platform' && deployment.queueProducer
          ? [
              {
                name: deployment.queueProducer.binding,
                queueName: deployment.queueProducer.queueName,
              },
            ]
          : []),
      plainTextBindings: {},
      secretNames: externalTopology?.secretNames ?? [
        'DEPLOYMENT_IDENTITY_SECRET',
        'MAINTENANCE_ADMIN_SECRET',
      ],
      artifactVersion: 'artifact-v3',
      desiredSpecDigest: deploymentSpecDigest(deployment),
      schemaVersion: 3,
      maintenance,
    };
    return {
      artifactVersion: 'artifact-v3',
      created: true,
      ...(deployment.authoredBy === 'external'
        ? { physicalScriptName: externalReleaseScriptName(deployment) }
        : {}),
    };
  }

  releaseScriptName(deployment: DeploymentSpec): string {
    return externalReleaseScriptName(deployment);
  }

  async promoteWorker(): Promise<void> {
    this.#event('promote');
  }

  async ensureMaintenance(): Promise<MaintenanceHealth> {
    this.#event('maintenance');
    return maintenance;
  }

  async inspect(): Promise<LiveDeployment | undefined> {
    this.#event('inspect');
    return this.live;
  }

  async removeTraffic(): Promise<void> {
    this.removeTrafficCalls += 1;
    this.trafficRemoved = true;
    if (this.failRemoveTrafficResponseOnce) {
      this.failRemoveTrafficResponseOnce = false;
      throw new Error('traffic removal response lost after commit');
    }
  }

  async assertTrafficRemoved(): Promise<void> {
    this.assertTrafficRemovedCalls += 1;
    if (!this.trafficRemoved || this.trafficDrift) {
      throw new Error('traffic remains');
    }
  }

  async revokeCredentials(
    _deployment: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] = [],
    activeRelease?: ExternalReleaseSnapshot,
  ): Promise<void> {
    this.retainedReleases = retainedReleases;
    this.activeRelease = activeRelease;
    this.#event('revoke');
  }

  async revokePlatformResourceCredentials(): Promise<void> {
    this.#event('revoke-platform');
  }

  async deletePlatformResources(
    _deployment: DeploymentSpec,
    record: FleetRecord,
  ): Promise<void> {
    this.deletedPlatformNamespaceIds =
      record.platformResources?.stateWorker.namespaceIds ?? [];
    this.#event('delete-platform');
    this.platformBootstrapPresent = false;
  }

  async deleteWorker(
    _deployment: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] = [],
    _database?: DatabaseReference,
    activeRelease?: ExternalReleaseSnapshot,
  ): Promise<void> {
    this.retainedReleases = retainedReleases;
    this.activeRelease = activeRelease;
    this.#event('delete-worker');
  }

  async assertDatabaseDetached(): Promise<void> {}

  async exportDatabase(): Promise<{
    databaseId: string;
    location: string;
    sha256: string;
    size: number;
  }> {
    this.#event('export');
    return {
      databaseId: 'database-id',
      location: this.exportLocation,
      sha256: 'a'.repeat(64),
      size: 42,
    };
  }

  async deleteDatabase(): Promise<void> {
    this.#event('delete-database');
    this.databaseExists = false;
  }
}

class R2RollbackBackend extends FakeBackend {
  readonly buckets = new Map<string, ApplicationR2BucketSnapshot>();
  failDetachOnceFor: string | undefined;
  nonempty = false;
  emptyChecks = 0;
  writeAfterTrafficRemovalOnce = false;

  override async removeTraffic(): Promise<void> {
    await super.removeTraffic();
    if (this.writeAfterTrafficRemovalOnce) {
      this.writeAfterTrafficRemovalOnce = false;
      this.nonempty = true;
    }
  }

  async findApplicationR2Bucket(
    resource: ApplicationR2Resource,
  ): Promise<ApplicationR2BucketSnapshot | undefined> {
    return this.buckets.get(resource.bucketName);
  }

  async ensureApplicationR2Bucket(
    resource: ApplicationR2Resource,
  ): Promise<ApplicationR2BucketSnapshot> {
    const existing = this.buckets.get(resource.bucketName);
    if (existing) return existing;
    const created = {
      name: resource.name,
      bucketName: resource.bucketName,
      jurisdiction: resource.jurisdiction,
      creationDate: `2026-08-11T00:00:${String(this.buckets.size).padStart(2, '0')}.000Z`,
    };
    this.buckets.set(resource.bucketName, created);
    return created;
  }

  override async deployWorker(
    deployment: DeploymentSpec,
    database?: DatabaseReference,
    deploymentSecrets?: DeploymentSecrets,
    platformResources?: FleetRecord['platformResources'],
  ) {
    const deployed = await super.deployWorker(
      deployment,
      database,
      deploymentSecrets,
      platformResources,
    );
    if (this.live) {
      this.live = {
        ...this.live,
        r2BucketBindings: [...this.buckets.values()].map(
          ({ name, bucketName, jurisdiction }) => ({
            name,
            bucketName,
            jurisdiction,
          }),
        ),
      };
    }
    return deployed;
  }

  async assertApplicationR2Detached(
    resource: ApplicationR2Resource,
  ): Promise<void> {
    if (this.failDetachOnceFor === resource.name) {
      this.failDetachOnceFor = undefined;
      throw new Error(`R2 bucket '${resource.bucketName}' remains attached`);
    }
  }

  async assertApplicationR2Empty(): Promise<void> {
    this.emptyChecks += 1;
    if (this.nonempty) throw new Error('R2 bucket is not empty');
  }

  async deleteApplicationR2Bucket(
    resource: ApplicationR2Resource,
  ): Promise<void> {
    this.buckets.delete(resource.bucketName);
  }
}

describe('fleet provisioning', () => {
  it('attests empty application bindings exactly while allowing only system-owned variables', () => {
    const deployment = spec();
    const digest = deploymentSpecDigest(deployment);
    const record = {
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
      databaseId: 'database-id',
      applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
    } satisfies Pick<
      FleetRecord,
      'tenantTag' | 'environment' | 'databaseId' | 'applicationBindings'
    >;
    const live: LiveDeployment = {
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
      scriptName: deployment.scriptName,
      databaseId: record.databaseId,
      durableObjectBindings: deployment.durableObjectBindings.map(
        (binding) => ({ ...binding, namespaceId: 'maintenance-namespace' }),
      ),
      serviceBindings: [
        { name: 'EGRESS_PROXY', service: 'fleet-egress-proxy' },
      ],
      queueProducerBindings: [],
      plainTextBindings: {
        DEPLOYMENT_TENANT: deployment.tenantTag,
        FLEET_ENVIRONMENT: deployment.environment,
        FLEET_SCHEMA_VERSION: String(deployment.schemaVersion),
        FLEET_SPEC_DIGEST: digest,
      },
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
      artifactVersion: 'artifact-v3',
      desiredSpecDigest: digest,
      schemaVersion: deployment.schemaVersion,
      maintenance,
    };

    expect(() =>
      assertLiveDeploymentMatches(live, record, deployment, digest),
    ).not.toThrow();
    expect(() =>
      assertLiveDeploymentMatches(
        {
          ...live,
          plainTextBindings: {
            ...live.plainTextBindings,
            OUT_OF_BAND_VARIABLE: 'unexpected',
          },
        },
        record,
        deployment,
        digest,
      ),
    ).toThrow(/does not exactly match/u);
    expect(() =>
      assertLiveDeploymentMatches(
        {
          ...live,
          secretNames: [...live.secretNames, 'OUT_OF_BAND_SECRET'],
        },
        record,
        deployment,
        digest,
      ),
    ).toThrow(/does not exactly match/u);
    for (const missing of ['plainTextBindings', 'secretNames'] as const) {
      const incomplete = { ...live } as Record<string, unknown>;
      delete incomplete[missing];
      expect(() =>
        assertLiveDeploymentMatches(
          incomplete as unknown as LiveDeployment,
          record,
          deployment,
          digest,
        ),
      ).toThrow(/live binding inventory is incomplete/u);
    }
  });

  it('refuses ordinary convergence while a backend switch intent is active', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const initial = await provisionDeployment({
      backend,
      store,
      spec: spec(),
      secrets,
      clock: () => 1_000,
    });
    store.record = {
      ...initial.record,
      backendSwitchIntent: {
        subphase: 'domain-detach-authorized',
      } as FleetRecord['backendSwitchIntent'],
    };
    backend.events.length = 0;

    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).rejects.toThrow(/active backend switch 'domain-detach-authorized'/);
    expect(backend.events).toEqual([]);
  });

  it('persists the ordered create phases and returns a ready deployment', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const result = await provisionDeployment({
      backend,
      store,
      spec: spec(),
      secrets,
      clock: () => 1_000,
    });

    expect(backend.events).toEqual([
      'database',
      'identity',
      'migrations',
      'migrations',
      'migrations',
      'worker',
      'inspect',
      'maintenance',
      'inspect',
      'promote',
      'inspect',
    ]);
    expect(result.record).toMatchObject({
      phase: 'ready',
      databaseId: 'database-id',
      artifactVersion: 'artifact-v3',
    });
    expect(store.record).toEqual(result.record);
    expect(store.phases).toEqual([
      'database-reserved',
      'database-create-authorized',
      'database-created',
      'identity-seeded',
      'identity-seeded',
      'identity-seeded',
      'identity-seeded',
      'migrated',
      'application-resources-create-authorized',
      'application-resources-deployed',
      'worker-deployed',
      'maintenance-armed',
      'publishing',
      'ready',
    ]);
  });

  it('rejects incomplete or noncontiguous D1 migration history before creating resources', async () => {
    for (const invalid of [
      spec({
        schemaVersion: 3,
        migrations: [
          { version: 1, sql: 'CREATE TABLE example (id TEXT)' },
          { version: 3, sql: 'ALTER TABLE example ADD COLUMN note TEXT' },
        ],
      }),
      spec({
        schemaVersion: 3,
        migrations: [
          { version: 1, sql: 'CREATE TABLE example (id TEXT)' },
          { version: 2, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
        ],
      }),
    ]) {
      const backend = new FakeBackend();
      await expect(
        provisionDeployment({
          backend,
          store: new MemoryStore(),
          spec: invalid,
          secrets,
        }),
      ).rejects.toThrow(/D1 migration/);
      expect(backend.events).toEqual([]);
    }
  });

  it('pins the immutable spec before D1 creation and recovers a committed create with a lost response', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'database';
    const store = new MemoryStore();
    const deployment = spec();

    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record).toMatchObject({
      phase: 'database-create-authorized',
      databaseName: deployment.databaseName,
      desiredSpecDigest: deploymentSpecDigest(deployment),
    });
    expect(backend.events).not.toContain('delete-database');

    await expect(
      provisionDeployment({
        backend,
        store,
        spec: {
          ...deployment,
          modules: [
            { name: 'worker.js', content: 'export default {changed: true}' },
          ],
        },
        secrets,
      }),
    ).rejects.toThrow(/different desired specification/);

    backend.failAt = undefined;
    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });
  });

  it('performs no provider mutation when the durable database-name reservation loses', async () => {
    class ConflictingDatabaseNameStore extends MemoryStore {
      override async put(record: FleetRecord): Promise<void> {
        if (record.phase === 'database-reserved') {
          throw new Error(
            'UNIQUE constraint failed: anchorage_fleet_deployments.database_name',
          );
        }
        await super.put(record);
      }
    }
    const backend = new FakeBackend();
    const store = new ConflictingDatabaseNameStore();

    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).rejects.toThrow(/UNIQUE constraint failed.*database_name/);
    expect(backend.events).toEqual([]);
    expect(store.record).toBeUndefined();
  });

  it('clears a database reservation only after the exact reserved name is positively absent', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const deployment = spec();
    const findDatabase = backend.findDatabase.bind(backend);
    backend.findDatabase = async () => {
      throw new Error('D1 lookup unavailable before create authorization');
    };
    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('database-reserved');

    backend.findDatabase = findDatabase;
    backend.events.length = 0;
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).resolves.toBeUndefined();
    expect(backend.events).toEqual([]);
    expect(store.record).toBeUndefined();
  });

  it('deletes an unseeded exact-name database left by an ambiguous reserved create', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'database';
    const store = new MemoryStore();
    const deployment = spec();
    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);

    backend.failAt = undefined;
    backend.databaseExists = true;
    backend.databaseOwner = undefined;
    backend.events.length = 0;
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).resolves.toBeUndefined();
    expect(backend.events).toEqual(['identity', 'delete-database']);
    expect(backend.databaseExists).toBe(false);
    expect(store.record).toBeUndefined();
  });

  it('refuses a pre-existing same-name database before create authorization', async () => {
    const backend = new FakeBackend();
    backend.databaseExists = true;
    backend.databaseOwner = undefined;
    const store = new MemoryStore();
    const deployment = spec();
    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('database-reserved');
    backend.events.length = 0;
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).rejects.toThrow(/unauthorized database reservation/);
    expect(backend.events).toEqual([]);
    expect(store.record?.phase).toBe('database-reserved');
  });

  it('preserves an authorized reservation when a create race resolves to another owner', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'database';
    const store = new MemoryStore();
    const deployment = spec();
    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('database-create-authorized');

    backend.failAt = undefined;
    backend.databaseOwner = 'other-tenant';
    backend.events.length = 0;
    const failure = await provisionDeployment({
      backend,
      store,
      spec: deployment,
      secrets,
    }).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as Error).cause).toBeInstanceOf(Error);
    expect(((failure as Error).cause as Error).message).toMatch(
      /owned by 'other-tenant'/,
    );
    expect(backend.events).not.toContain('delete-database');
    expect(store.record?.phase).toBe('database-create-authorized');
  });

  it.each([
    'identity',
    'migrations',
    'worker',
    'maintenance',
  ])('rolls back resources when %s fails', async (failure) => {
    const backend = new FakeBackend();
    backend.failAt = failure;
    const store = new MemoryStore();

    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);

    if (failure === 'identity') {
      expect(backend.events).not.toContain('delete-database');
      expect(store.record?.phase).toBe('database-created');
    } else {
      expect(backend.events.at(-1)).toBe('delete-database');
      expect(store.record).toBeUndefined();
    }
    if (failure === 'maintenance') {
      expect(backend.events).toContain('revoke');
      expect(backend.events).toContain('delete-worker');
    }
  });

  it('resumes per-bucket R2 rollback without rewinding completed deletion progress', async () => {
    const backend = new R2RollbackBackend('plain-worker');
    backend.failAt = 'worker';
    backend.failDetachOnceFor = 'FILES';
    const store = new MemoryStore();
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'ARCHIVE' }, { name: 'FILES' }],
      },
    });

    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);

    expect(
      store.record?.applicationResources?.map(({ name, state }) => ({
        name,
        state,
      })),
    ).toEqual([
      { name: 'ARCHIVE', state: 'deleted' },
      { name: 'FILES', state: 'detach-authorized' },
    ]);
    expect([...backend.buckets.values()].map(({ name }) => name)).toEqual([
      'FILES',
    ]);

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).resolves.toBeUndefined();

    expect(backend.buckets.size).toBe(0);
    expect(store.record).toBeUndefined();
  });

  it('does not rewind R2 rollback after a committed state write response is lost', async () => {
    const backend = new R2RollbackBackend('plain-worker');
    backend.failAt = 'worker';
    const store = new CommitThenThrowStore();
    store.failAfterCommittedApplicationState = {
      name: 'ARCHIVE',
      state: 'deleted',
    };
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'ARCHIVE' }, { name: 'FILES' }],
      },
    });

    const failure = await provisionDeployment({
      backend,
      store,
      spec: deployment,
      secrets,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as ProvisioningError).cleanupErrors).toHaveLength(1);
    expect(
      store.record?.applicationResources?.map(({ name, state }) => ({
        name,
        state,
      })),
    ).toEqual([
      { name: 'ARCHIVE', state: 'deleted' },
      { name: 'FILES', state: 'created' },
    ]);
    expect([...backend.buckets.values()].map(({ name }) => name)).toEqual([
      'FILES',
    ]);
    const firstDeletedWrite = store.applicationStateWrites.findIndex(
      (resources) =>
        resources.some(
          (resource) =>
            resource.name === 'ARCHIVE' && resource.state === 'deleted',
        ),
    );
    expect(firstDeletedWrite).toBeGreaterThanOrEqual(0);
    expect(
      store.applicationStateWrites
        .slice(firstDeletedWrite)
        .every((resources) =>
          resources.some(
            (resource) =>
              resource.name === 'ARCHIVE' && resource.state === 'deleted',
          ),
        ),
    ).toBe(true);

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).resolves.toBeUndefined();
    expect(backend.buckets.size).toBe(0);
    expect(store.record).toBeUndefined();
  });

  it('does not rewind R2 creation after a committed state write response is lost', async () => {
    const backend = new R2RollbackBackend('plain-worker');
    backend.failDetachOnceFor = 'ARCHIVE';
    const store = new CommitThenThrowStore();
    store.failAfterCommittedApplicationState = {
      name: 'ARCHIVE',
      state: 'created',
    };
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'ALBUMS' }, { name: 'ARCHIVE' }],
      },
    });

    const failure = await provisionDeployment({
      backend,
      store,
      spec: deployment,
      secrets,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as ProvisioningError).cleanupErrors).toHaveLength(1);
    expect(
      store.record?.applicationResources?.map(({ name, state }) => ({
        name,
        state,
      })),
    ).toEqual([
      { name: 'ALBUMS', state: 'deleted' },
      { name: 'ARCHIVE', state: 'detach-authorized' },
    ]);
    expect([...backend.buckets.values()].map(({ name }) => name)).toEqual([
      'ARCHIVE',
    ]);
    const createdWrite = store.applicationStateWrites.findIndex((resources) =>
      resources.some(
        (resource) =>
          resource.name === 'ARCHIVE' && resource.state === 'created',
      ),
    );
    expect(createdWrite).toBeGreaterThanOrEqual(0);
    expect(
      store.applicationStateWrites.slice(createdWrite).every((resources) => {
        const archive = resources.find(({ name }) => name === 'ARCHIVE');
        return (
          archive !== undefined &&
          archive.state !== 'reserved' &&
          archive.state !== 'create-authorized'
        );
      }),
    ).toBe(true);

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).resolves.toBeUndefined();
    expect(backend.buckets.size).toBe(0);
    expect(backend.databaseExists).toBe(false);
    expect(store.record).toBeUndefined();
  });

  it('preserves completed R2 rollback progress when later database cleanup fails', async () => {
    const backend = new R2RollbackBackend('plain-worker');
    backend.failAt = 'worker';
    backend.cleanupFailAt = 'delete-database';
    const store = new MemoryStore();
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'ARCHIVE' }, { name: 'FILES' }],
      },
    });

    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);

    expect(
      store.record?.applicationResources?.map(({ state }) => state),
    ).toEqual(['deleted', 'deleted']);
    expect(backend.buckets.size).toBe(0);

    backend.cleanupFailAt = undefined;
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).resolves.toBeUndefined();

    expect(store.record).toBeUndefined();
  });

  it('persists trusted state namespace ownership before candidate failure cleanup', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'worker';
    const store = new MemoryStore();
    const external = spec({
      authoredBy: 'external',
      durableObjectMigrations: [],
      egressProxyService: undefined,
    });

    await expect(
      provisionDeployment({ backend, store, spec: external, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);

    expect(store.phases).toContain('platform-resources-deployed');
    expect(backend.events).toEqual(
      expect.arrayContaining([
        'platform-resources',
        'worker',
        'revoke-platform',
        'delete-platform',
        'delete-database',
      ]),
    );
    expect(backend.deletedPlatformNamespaceIds).toEqual([
      'state-acme-production-MAINTENANCE',
    ]);
    expect(store.record).toBeUndefined();
  });

  it('cleans an exact private bootstrap after platform privatization fails before the resource snapshot', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'platform-privatization';
    const store = new MemoryStore();
    const external = spec({
      authoredBy: 'external',
      durableObjectMigrations: [],
      egressProxyService: undefined,
    });

    await expect(
      provisionDeployment({ backend, store, spec: external, secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);

    expect(backend.events).toEqual(
      expect.arrayContaining([
        'platform-resources',
        'platform-privatization',
        'revoke-platform',
        'delete-platform',
        'delete-database',
      ]),
    );
    expect(backend.platformBootstrapPresent).toBe(false);
    expect(backend.databaseExists).toBe(false);
    expect(store.record).toBeUndefined();
  });

  it('rejects external artifacts on the plain backend before creating resources', async () => {
    const backend = new FakeBackend('plain-worker');
    await expect(
      provisionDeployment({
        backend,
        store: new MemoryStore(),
        spec: spec({ authoredBy: 'external' }),
        secrets,
      }),
    ).rejects.toThrow(/refuses externally authored/);
    expect(backend.events).toEqual([]);
  });

  it('requires a pre-publication control origin distinct from the tenant hostname', async () => {
    const backend = new FakeBackend('plain-worker');
    await expect(
      provisionDeployment({
        backend,
        store: new MemoryStore(),
        spec: spec({
          maintenanceBaseUrl: 'https://acme.example.test',
          routeHostname: 'acme.example.test',
        }),
        secrets,
      }),
    ).rejects.toThrow(/control-plane hostname distinct from routeHostname/);
    expect(backend.events).toEqual([]);
  });

  it('allows external artifacts to bind only platform-owned Durable Objects', async () => {
    const external = spec({
      authoredBy: 'external',
      egressProxyService: undefined,
      durableObjectMigrations: [],
      durableObjectBindings: [
        {
          name: 'MAINTENANCE',
          className: 'Maintenance',
        },
      ],
    });
    await expect(
      provisionDeployment({
        backend: new FakeBackend(),
        store: new MemoryStore(),
        spec: external,
        secrets,
      }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });

    for (const invalid of [
      {
        ...external,
        durableObjectMigrations: [{ tag: 'v1', newClasses: ['Owned'] }],
      },
      {
        ...external,
        egressProxyService: 'customer-selected-proxy',
      },
      {
        ...external,
        durableObjectBindings: [
          {
            name: 'MAINTENANCE',
            className: 'Maintenance',
            scriptName: external.scriptName,
          },
        ],
      },
    ]) {
      const backend = new FakeBackend();
      await expect(
        provisionDeployment({
          backend,
          store: new MemoryStore(),
          spec: invalid,
          secrets,
        }),
      ).rejects.toThrow(/externally authored|cannot select/);
      expect(backend.events).toEqual([]);
    }
  });

  it('requires migrateFleet before changing a trusted platform target', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const external = spec({
      authoredBy: 'external',
      egressProxyService: undefined,
      durableObjectMigrations: [],
      durableObjectBindings: [
        { name: 'MAINTENANCE', className: 'Maintenance' },
      ],
    });
    const first = await provisionDeployment({
      backend,
      store,
      spec: external,
      secrets,
    });
    expect(first.record.platformResources?.stateWorker.artifactDigest).toBe(
      'a'.repeat(64),
    );
    backend.platformStateDigest = 'd'.repeat(64);
    backend.platformPolicyHosts = ['narrow.example.com'];
    backend.events.length = 0;

    await expect(
      provisionDeployment({ backend, store, spec: external, secrets }),
    ).rejects.toThrow(/do not match the persisted platform target/);
    expect(backend.events).toEqual([]);
  });

  it('reconciles a finalized ordinary state bridge without calling the normal platform provisioner', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const external = spec({
      authoredBy: 'external',
      egressProxyService: undefined,
      durableObjectMigrations: [],
    });
    await provisionDeployment({ backend, store, spec: external, secrets });
    const current = store.record;
    if (
      !current?.platformResources ||
      !current.platformTarget ||
      !backend.live
    ) {
      throw new Error('missing external deployment fixture');
    }
    const finalizedTarget = current.platformTarget;
    const bridge: BridgeSnapshot = {
      scriptName: current.scriptName,
      artifactVersion: current.platformResources.stateWorker.artifactVersion,
      artifactDigest: current.platformTarget.stateArtifactDigest,
      databaseId: current.databaseId,
      durableObjectBindings:
        current.platformResources.stateWorker.durableObjectBindings,
      namespaceIds: current.platformResources.stateWorker.namespaceIds,
      secretNames: [
        'DEPLOYMENT_IDENTITY_SECRET',
        'MAINTENANCE_ADMIN_SECRET',
        'OUTBOUND_PROXY_CREDENTIAL',
      ],
      publicRouteAttached: false,
      stateOnly: true,
    };
    store.record = {
      ...current,
      platformResources: {
        ...current.platformResources,
        stateWorker: {
          ...current.platformResources.stateWorker,
          scriptName: current.scriptName,
          artifactDigest: current.platformTarget.stateArtifactDigest,
          plane: 'ordinary',
        },
      },
      backendSwitchIntent: {
        kind: 'backend-switch',
        tenantTag: current.tenantTag,
        environment: current.environment,
        prior: {
          scriptName: current.scriptName,
          artifactVersion: 'plain-v1',
          specDigest: current.desiredSpecDigest,
          databaseId: current.databaseId,
          databaseName: current.databaseName,
          durableObjectBindings: bridge.durableObjectBindings,
          namespaceIds: bridge.namespaceIds,
          secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
          applicationResources: [],
          customDomain: { id: 'domain-acme', hostname: current.routeHostname },
        },
        targetSpecDigest: current.desiredSpecDigest,
        targetApplication: { vars: [], secrets: [], r2Buckets: [] },
        target: current.platformTarget,
        rollbackUntil: '2026-08-20T00:00:00.000Z',
        subphase: 'finalized',
        bridge,
      },
    };
    backend.live = {
      ...backend.live,
      durableObjectBindings: backend.live.durableObjectBindings.map(
        (binding) => ({ ...binding, scriptName: current.scriptName }),
      ),
    };
    const plan: BridgeMutationPlan = {
      artifactDigest: bridge.artifactDigest,
      durableObjectMigrations: [],
      secretNames: bridge.secretNames,
      mutationDigest: 'f'.repeat(64),
    };
    let reconciliations = 0;
    const finalizedStateProvider: FinalizedOrdinaryStateProvider = {
      describeFinalizedBridgeTarget: () => finalizedTarget,
      describeFinalizedState: () => plan,
      assertFinalizedState: async () => {},
      ensureFinalizedState: async () => {
        reconciliations += 1;
        return bridge;
      },
      commitFinalizedOwnership: async ({ currentRecord }) => currentRecord,
    };
    backend.events.length = 0;

    await expect(
      provisionDeployment({
        backend,
        store,
        spec: external,
        secrets,
        finalizedStateProvider,
      }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });
    expect(reconciliations).toBe(1);
    expect(backend.events).not.toContain('platform-resources');
    expect(
      store.record?.backendSwitchIntent?.stateReconcileIntent?.subphase,
    ).toBe('uploaded');
  });

  it('converges a rollback-compatible old active release without lowering applied D1 schema', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const activeSpec = spec({
      authoredBy: 'external',
      egressProxyService: undefined,
      schemaVersion: 1,
      migrations: [{ version: 1, sql: 'CREATE TABLE example (id TEXT)' }],
      durableObjectMigrations: [],
      durableObjectBindings: [
        {
          name: 'MAINTENANCE',
          className: 'Maintenance',
        },
      ],
    });
    const digest = deploymentSpecDigest(activeSpec);
    const activeBinding = activeSpec.durableObjectBindings[0];
    if (!activeBinding) throw new Error('expected a durable object binding');
    const activeRelease = {
      physicalScriptName: 'acme-production-release-v1',
      specDigest: digest,
      artifactVersion: 'artifact-v1',
      releaseSchemaVersion: 1,
    };
    const platformTarget = {
      ...backend.describeExternalPlatformTarget(activeSpec),
      d1SchemaVersion: 2,
      d1SchemaHistoryDigest: 'f'.repeat(64),
    };
    const platformResources = {
      maintenanceCapabilityPublicKey:
        platformTarget.maintenanceCapabilityPublicKey,
      stateWorker: {
        scriptName: externalStateScriptName(activeSpec),
        artifactVersion: 'state-v1',
        artifactDigest: platformTarget.stateArtifactDigest,
        durableObjectBindings: [
          {
            ...activeBinding,
            namespaceId: 'maintenance-namespace',
          },
        ],
        namespaceIds: ['maintenance-namespace'],
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(activeSpec),
        artifactVersion: 'egress-v1',
        artifactDigest: platformTarget.egressArtifactDigest,
        ...platformTarget.outboundPolicy,
      },
    };
    store.record = {
      tenantTag: activeSpec.tenantTag,
      backend: 'workers-for-platforms',
      environment: activeSpec.environment,
      scriptName: activeSpec.scriptName,
      databaseId: 'database-id',
      databaseName: activeSpec.databaseName,
      schemaVersion: 2,
      artifactVersion: 'artifact-v1',
      desiredSpecDigest: digest,
      activeRelease,
      platformResources,
      platformTarget,
      outboundPolicy: platformTarget.outboundPolicy,
      durableObjectBindings: [
        {
          ...activeBinding,
          scriptName: platformResources.stateWorker.scriptName,
          namespaceId: 'maintenance-namespace',
        },
      ],
      routeHostname: activeSpec.routeHostname,
      phase: 'ready',
      updatedAt: '2026-08-10T00:00:00.000Z',
    };
    backend.live = {
      tenantTag: activeSpec.tenantTag,
      environment: activeSpec.environment,
      scriptName: activeRelease.physicalScriptName,
      databaseId: store.record.databaseId,
      durableObjectBindings: store.record.durableObjectBindings,
      plainTextBindings: {},
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
      artifactVersion: store.record.artifactVersion,
      desiredSpecDigest: digest,
      schemaVersion: 1,
      maintenance,
    };
    backend.databaseExists = true;
    backend.databaseOwner = activeSpec.tenantTag;

    const result = await provisionDeployment({
      backend,
      store,
      spec: activeSpec,
      secrets,
    });
    expect(result.record.schemaVersion).toBe(2);
    expect(result.record.activeRelease?.releaseSchemaVersion).toBe(1);
    expect(backend.events).toEqual(['platform-resources', 'inspect']);
  });

  it('resumes decommissioning after export failure without repeating prior steps', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    backend.events.length = 0;
    backend.failAt = 'export';

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).rejects.toThrow(/failed at export/);
    expect(backend.events).toEqual(['revoke', 'delete-worker', 'export']);
    expect(store.record?.phase).toBe('application-resources-deleted');

    backend.failAt = undefined;
    const result = await decommissionDeployment({
      backend,
      store,
      spec: spec(),
    });
    expect(backend.events).toEqual([
      'revoke',
      'delete-worker',
      'export',
      'export',
      'delete-database',
    ]);
    expect(result.record.phase).toBe('decommissioned');
    expect(result.databaseExport.location).toBe(backend.exportLocation);
  });

  it.each([
    'plain-worker',
    'workers-for-platforms',
  ] as const)('refuses %s decommission before provider mutation while application R2 is nonempty', async (kind) => {
    const backend = new R2RollbackBackend(kind);
    const store = new MemoryStore();
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'FILES' }],
      },
    });
    await provisionDeployment({ backend, store, spec: deployment, secrets });
    backend.events.length = 0;
    backend.nonempty = true;

    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).rejects.toThrow(/not empty/u);
    expect(backend.emptyChecks).toBe(1);
    expect(backend.events).toEqual([]);
    expect(store.record?.phase).toBe('ready');

    backend.nonempty = false;
    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
  });

  it.each([
    'plain-worker',
    'workers-for-platforms',
  ] as const)('blocks %s deletion when R2 receives a write after traffic removal and resumes after evacuation', async (kind) => {
    const backend = new R2RollbackBackend(kind);
    const store = new MemoryStore();
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'FILES' }],
      },
    });
    await provisionDeployment({ backend, store, spec: deployment, secrets });
    backend.events.length = 0;
    backend.writeAfterTrafficRemovalOnce = true;

    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).rejects.toThrow(/not empty/u);
    expect(store.record?.phase).toBe('traffic-removed');
    expect(backend.events).toEqual([]);
    expect(backend.live).toBeDefined();
    expect(backend.databaseExists).toBe(true);
    expect(backend.buckets.size).toBe(1);
    expect(backend.removeTrafficCalls).toBe(1);

    backend.nonempty = false;
    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(backend.removeTrafficCalls).toBe(1);
    expect(backend.assertTrafficRemovedCalls).toBeGreaterThanOrEqual(2);
  });

  it('retries traffic removal after a committed provider response loss before the post-drain R2 check', async () => {
    const backend = new R2RollbackBackend('plain-worker');
    const store = new MemoryStore();
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'FILES' }],
      },
    });
    await provisionDeployment({ backend, store, spec: deployment, secrets });
    backend.events.length = 0;
    backend.failRemoveTrafficResponseOnce = true;

    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).rejects.toThrow(/traffic removal response lost/u);
    expect(store.record?.phase).toBe('decommissioning');
    expect(backend.removeTrafficCalls).toBe(1);
    expect(backend.events).toEqual([]);

    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(backend.removeTrafficCalls).toBe(2);
    expect(backend.emptyChecks).toBeGreaterThanOrEqual(2);
  });

  it('reconciles a committed traffic-removed state write before destructive teardown', async () => {
    const backend = new R2RollbackBackend('plain-worker');
    const store = new CommitThenThrowStore();
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'FILES' }],
      },
    });
    await provisionDeployment({ backend, store, spec: deployment, secrets });
    backend.events.length = 0;
    store.failAfterCommittedPhase = 'traffic-removed';

    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).rejects.toThrow(/committing traffic-removed/u);
    expect(store.record?.phase).toBe('traffic-removed');
    expect(backend.events).toEqual([]);

    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(backend.assertTrafficRemovedCalls).toBeGreaterThanOrEqual(2);
    expect(backend.emptyChecks).toBeGreaterThanOrEqual(2);
  });

  it('preserves publishing resources after a promote response loss and routes cleanup through export-backed decommission', async () => {
    const backend = new R2RollbackBackend('plain-worker');
    const store = new MemoryStore();
    const deployment = spec({
      application: {
        vars: [],
        secrets: [],
        r2Buckets: [{ name: 'FILES' }],
      },
    });
    backend.failAt = 'promote';

    await expect(
      provisionDeployment({ backend, store, spec: deployment, secrets }),
    ).rejects.toThrow(/publishing state is preserved/u);
    expect(store.record?.phase).toBe('publishing');
    expect(backend.live).toBeDefined();
    expect(backend.databaseExists).toBe(true);
    expect(backend.buckets.size).toBe(1);

    backend.failAt = undefined;
    backend.events.length = 0;
    backend.nonempty = true;
    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).rejects.toThrow(/not empty/u);
    expect(store.record?.phase).toBe('publishing');
    expect(backend.events).toEqual([]);

    backend.nonempty = false;
    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
  });

  it('refuses manual prepublication cleanup when unexpected ingress remains', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    const deployment = spec();
    await provisionDeployment({ backend, store, spec: deployment, secrets });
    if (!store.record) throw new Error('missing provisioned record');
    store.record = { ...store.record, phase: 'worker-deployed' };
    backend.events.length = 0;
    backend.trafficDrift = true;

    const failure = await cleanupDeploymentArtifacts({
      backend,
      store,
      spec: deployment,
    }).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    expect((failure as AggregateError).errors).toEqual([
      expect.objectContaining({ message: 'traffic remains' }),
    ]);
    expect(backend.events).toEqual([]);
    expect(backend.live).toBeDefined();
    expect(backend.databaseExists).toBe(true);
    expect(store.record.phase).toBe('worker-deployed');
  });

  it('never deletes the database when export fails', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    backend.events.length = 0;
    backend.failAt = 'export';
    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).rejects.toThrow();
    expect(backend.events).not.toContain('delete-database');
  });

  it.each([
    'migrating',
    'rolling-back',
  ] as const)('decommissions the active and pending releases from %s route-flip state', async (phase) => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const deployment = spec();
    await provisionDeployment({ backend, store, spec: deployment, secrets });
    if (!store.record) throw new Error('missing provisioned record');
    const activeRelease: ExternalReleaseSnapshot = {
      physicalScriptName: 'acme-active-release',
      specDigest: 'b'.repeat(64),
      artifactVersion: 'active-etag',
      releaseSchemaVersion: 2,
    };
    const pendingRelease: ExternalReleaseSnapshot = {
      physicalScriptName: 'acme-pending-release',
      specDigest: deploymentSpecDigest(deployment),
      artifactVersion: 'pending-etag',
      releaseSchemaVersion: deployment.schemaVersion,
    };
    store.record = {
      ...store.record,
      phase,
      activeRelease,
      pendingRelease,
      migrationPriorRelease: activeRelease,
      rollbackRelease: pendingRelease,
    };
    backend.events.length = 0;

    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(backend.activeRelease).toEqual(activeRelease);
    expect(
      backend.retainedReleases.map((release) => release.physicalScriptName),
    ).toEqual(['acme-pending-release']);
  });

  it.each([
    'planned',
    'schema-applied',
    'platform-applied',
    'candidate-deployed',
    'candidate-armed',
    'route-published',
  ] as const)('decommissions an external deployment interrupted at migration subphase %s', async (subphase) => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const initialSpec = spec({
      authoredBy: 'external',
      egressProxyService: undefined,
      durableObjectMigrations: [],
    });
    await provisionDeployment({
      backend,
      store,
      spec: initialSpec,
      secrets,
    });
    const current = store.record;
    if (!current?.platformResources || !current.platformTarget) {
      throw new Error('missing external deployment state');
    }
    const priorRelease: ExternalReleaseSnapshot = current.activeRelease ?? {
      physicalScriptName: 'acme-production-prior-release',
      specDigest: deploymentSpecDigest(initialSpec),
      artifactVersion: current.artifactVersion,
      releaseSchemaVersion: current.schemaVersion,
    };
    const targetSpec = spec({
      ...initialSpec,
      schemaVersion: 4,
      migrations: [
        ...initialSpec.migrations,
        {
          version: 4,
          sql: 'ALTER TABLE example ADD COLUMN migrated TEXT',
          rollbackCompatible: true,
        },
      ],
    });
    const targetRelease: ExternalReleaseSnapshot = {
      physicalScriptName: 'acme-production-target-release',
      specDigest: deploymentSpecDigest(targetSpec),
      artifactVersion: 'pending',
      releaseSchemaVersion: targetSpec.schemaVersion,
    };
    store.record = {
      ...current,
      phase: 'migrating',
      schemaVersion:
        subphase === 'planned'
          ? current.schemaVersion
          : targetSpec.schemaVersion,
      pendingRelease: targetRelease,
      activeRelease: priorRelease,
      migrationPriorRelease: priorRelease,
      migrationIntent: {
        targetSpecDigest: targetRelease.specDigest,
        priorRelease,
        priorTarget: current.platformTarget,
        priorOutboundPolicy: current.platformTarget.outboundPolicy,
        targetRelease,
        target: backend.describeExternalPlatformTarget(targetSpec),
        subphase,
      },
    };
    backend.events.length = 0;

    await expect(
      decommissionDeployment({ backend, store, spec: targetSpec }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(backend.events).toEqual([
      'revoke',
      'delete-worker',
      'revoke-platform',
      'delete-platform',
      'export',
      'delete-database',
    ]);
  });

  it('recovers when final state persistence fails after D1 deletion', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    store.failPutPhase = 'decommissioned';

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).rejects.toThrow(/failed state write/);
    expect(store.record?.phase).toBe('database-deleting');
    expect(backend.databaseExists).toBe(false);

    const findDatabaseCalls = backend.findDatabaseCalls;
    backend.databaseExists = true;
    backend.databaseId = 'replacement-database-id';

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(
      backend.events.filter((event) => event === 'delete-database'),
    ).toHaveLength(1);
    expect(backend.databaseExists).toBe(true);
    expect(backend.databaseIdsRead.at(-1)).toBe('database-id');
    expect(backend.findDatabaseCalls).toBe(findDatabaseCalls);
  });

  it('rejects a persisted database ID that now has a different name', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    backend.databaseName = 'unexpected-database-name';
    backend.events.length = 0;

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).rejects.toThrow(/resolved with unexpected identity/);
    expect(backend.events).toEqual([]);
    expect(store.record?.phase).toBe('ready');
  });

  it('rejects database cleanup when the persisted ID has another sentinel owner', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    if (!store.record) throw new Error('missing test record');
    store.record = { ...store.record, phase: 'worker-deployed' };
    backend.databaseOwner = 'other-tenant';
    backend.events.length = 0;

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).rejects.toThrow(/owned by 'other-tenant'/);
    expect(backend.events).toEqual([]);
    expect(store.record.phase).toBe('worker-deployed');
  });

  it('converges cleanup when the persisted database ID is positively absent', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    if (!store.record) throw new Error('missing test record');
    store.record = { ...store.record, phase: 'worker-deployed' };
    backend.databaseExists = false;
    backend.events.length = 0;

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).resolves.toBeUndefined();
    expect(backend.events).toEqual(['revoke', 'delete-worker']);
    expect(store.record).toBeUndefined();
  });

  it('does not treat a persisted-ID lookup failure as database absence', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    if (!store.record) throw new Error('missing test record');
    store.record = { ...store.record, phase: 'worker-deployed' };
    backend.getDatabase = async () => {
      throw new Error('D1 lookup unavailable');
    };
    backend.events.length = 0;

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).rejects.toThrow(/D1 lookup unavailable/);
    expect(backend.events).toEqual([]);
    expect(store.record.phase).toBe('worker-deployed');
  });

  it('preserves the database and resumable record when Worker cleanup fails', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'maintenance';
    backend.cleanupFailAt = 'delete-worker';
    const store = new MemoryStore();

    const failure = await provisionDeployment({
      backend,
      store,
      spec: spec(),
      secrets,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect(backend.events).not.toContain('delete-database');
    expect(store.record?.phase).toBe('worker-deployed');
  });

  it('resumes from a persisted phase after cleanup could not remove the database', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'identity';
    backend.cleanupFailAt = 'delete-database';
    const store = new MemoryStore();

    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('database-created');

    backend.failAt = undefined;
    backend.cleanupFailAt = undefined;
    backend.events.length = 0;
    backend.databaseIdsRead.length = 0;
    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });
    expect(backend.events).not.toContain('database');
    expect(backend.events).toContain('identity');
    expect(backend.databaseIdsRead).toContain('database-id');
  });

  it('does not delete a database after an export without integrity evidence', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    backend.events.length = 0;
    backend.exportDatabase = async () => ({
      databaseId: 'database-id',
      location: backend.exportLocation,
      sha256: '',
      size: 0,
    });

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).rejects.toThrow(/durable, non-empty database export/);
    expect(backend.events).not.toContain('delete-database');
  });

  it('rejects an export for a different database before persisting or deleting', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    backend.events.length = 0;
    backend.exportDatabase = async () => ({
      databaseId: 'replacement-database-id',
      location: backend.exportLocation,
      sha256: 'a'.repeat(64),
      size: 42,
    });

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).rejects.toThrow(/unexpected database 'replacement-database-id'/);
    expect(store.record?.phase).toBe('application-resources-deleted');
    expect(backend.events).not.toContain('delete-database');
  });

  it('rejects a concurrent lifecycle operation for the same deployment', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    let releaseDatabase: ((database: DatabaseReference) => void) | undefined;
    backend.ensureDatabase = () =>
      new Promise<DatabaseReference>((resolve) => {
        releaseDatabase = resolve;
      });

    const first = provisionDeployment({
      backend,
      store,
      spec: spec(),
      secrets,
    });
    await Promise.resolve();
    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).rejects.toThrow(/already being modified/);
    releaseDatabase?.({
      id: 'database-id',
      name: 'acme-production',
      created: true,
    });
    await expect(first).resolves.toMatchObject({ record: { phase: 'ready' } });
  });

  it('recreates a missing Worker when resuming a persisted deployment phase', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'maintenance';
    backend.cleanupFailAt = 'delete-worker';
    const store = new MemoryStore();
    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('worker-deployed');

    backend.failAt = undefined;
    backend.cleanupFailAt = undefined;
    backend.live = undefined;
    backend.events.length = 0;
    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });
    expect(backend.events.filter((event) => event === 'worker')).toHaveLength(
      1,
    );
    expect(backend.events).toContain('promote');
  });

  it('rejects a changed bundle while resuming an in-flight specification', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'maintenance';
    backend.cleanupFailAt = 'delete-worker';
    const store = new MemoryStore();
    await expect(
      provisionDeployment({ backend, store, spec: spec(), secrets }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    backend.failAt = undefined;
    backend.cleanupFailAt = undefined;
    backend.events.length = 0;

    await expect(
      provisionDeployment({
        backend,
        store,
        spec: spec({
          modules: [
            { name: 'worker.js', content: 'export default {changed:true}' },
          ],
        }),
        secrets,
      }),
    ).rejects.toThrow(/different desired specification/);
    expect(backend.events).toEqual([]);
  });

  it('does not delete D1 when partial cleanup cannot remove the Worker', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({ backend, store, spec: spec(), secrets });
    if (!store.record) throw new Error('missing test record');
    store.record = { ...store.record, phase: 'worker-deployed' };
    backend.cleanupFailAt = 'delete-worker';
    backend.events.length = 0;

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).rejects.toThrow(/failed to clean/);
    expect(backend.events).not.toContain('delete-database');
    expect(store.record?.phase).toBe('worker-deployed');
  });
});
