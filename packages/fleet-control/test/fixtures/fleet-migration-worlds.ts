// SPDX-License-Identifier: Apache-2.0

import type { AttestConvergedActiveRouteOptions } from '../../src/active-route.js';
import { migrateFleet } from '../../src/fleet.js';
import {
  canonicalDeploymentEgressPolicy,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalStateScriptName,
} from '../../src/platform-resources.js';
import { providerBindingIdentitiesForInspection } from '../../src/provider-binding-inventory.js';
import { fleetSettlementKey } from '../../src/settlement.js';
import { deploymentSpecDigest } from '../../src/spec-digest.js';
import type {
  ActiveRouteAttestation,
  ApplicationBindingTopology,
  D1Migration,
  DatabaseReference,
  DeploymentEgressPolicy,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetSettlementContext,
  FleetSettlementHost,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  PromotionGuard,
  ProvisioningBackend,
  ProvisioningBackendKind,
} from '../../src/types.js';
import { externalReleaseScriptName } from '../../src/workers-for-platforms-backend.js';

const ENVIRONMENT = 'production';

const MIGRATION_NOW = Date.parse('2026-06-01T00:00:00.000Z');
const FROZEN_UPDATED_AT = new Date(MIGRATION_NOW).toISOString();
const MIGRATION_CLOCK = () => MIGRATION_NOW;

const ORIGIN_UPDATED_AT = '2026-05-01T00:00:00.000Z';

const MAINTENANCE_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

const STATE_ARTIFACT_DIGEST = 'a'.repeat(64);
const MOVED_STATE_ARTIFACT_DIGEST = 'd'.repeat(64);
const EGRESS_ARTIFACT_DIGEST = 'b'.repeat(64);
const STATE_DURABLE_OBJECT_HISTORY_DIGEST = 'c'.repeat(64);
const PLATFORM_ONLY_PRIOR_D1_HISTORY_DIGEST = 'f'.repeat(64);

const HEALTHY_MAINTENANCE: MaintenanceHealth = {
  armed: true,
  nextAlarmAt: MIGRATION_NOW + 60_000,
  lastSweepAt: MIGRATION_NOW - 60_000,
  lastPurgeAt: MIGRATION_NOW - 60_000,
};

const UNARMED_MAINTENANCE: MaintenanceHealth = {
  armed: false,
  nextAlarmAt: null,
  lastSweepAt: null,
  lastPurgeAt: null,
};

const EMPTY_APPLICATION: ApplicationBindingTopology = {
  vars: [],
  secrets: [],
  r2Buckets: [],
};

const D1_MIGRATIONS: readonly D1Migration[] = [
  { version: 1, sql: 'CREATE TABLE example (id TEXT PRIMARY KEY)' },
  {
    version: 2,
    sql: 'ALTER TABLE example ADD COLUMN value TEXT',
    rollbackCompatible: true,
  },
  {
    version: 3,
    sql: 'ALTER TABLE example ADD COLUMN expanded TEXT',
    rollbackCompatible: true,
  },
];

export type MigrationOpLogEntry =
  | 'withDeploymentLease'
  | 'get'
  | 'assertOwned'
  | 'releaseScriptName'
  | 'getDatabase'
  | 'readDeploymentIdentity'
  | 'seedDeploymentIdentity'
  | 'ensurePlatformResources'
  | 'describeExternalPlatformTarget'
  | 'describeFinalizedState'
  | 'describeFinalizedBridgeTarget'
  | 'assertFinalizedState'
  | 'ensureFinalizedState'
  | 'commitFinalizedOwnership'
  | 'deployWorker'
  | 'promoteWorker'
  | 'ensureMaintenance'
  | 'inspect'
  | 'attestActiveRoute'
  | 'deleteRetainedRelease'
  | `put:${string}`
  | `resolver:${string}`
  | `applyMigrations:${string}`
  | `settle:${string}`;

function deploymentKey(record: {
  readonly tenantTag: string;
  readonly environment: string;
}): string {
  return `${record.tenantTag}:${record.environment}`;
}

function secretsForTenant(tenantTag: string): DeploymentSecrets {
  return {
    deploymentIdentity: `deployment-identity-secret-${tenantTag}-00000001`,
    maintenanceAdmin: `maintenance-admin-secret-${tenantTag}-00000001`,
  };
}

function putToken(
  previous: FleetRecord | undefined,
  next: FleetRecord,
): string {
  if (!previous) return next.phase;
  if (next.phase !== previous.phase) return next.phase;
  const migrationSubphase = next.migrationIntent?.subphase;
  if (
    migrationSubphase !== undefined &&
    migrationSubphase !== previous.migrationIntent?.subphase
  ) {
    return migrationSubphase;
  }
  // Distinct reconcile tokens expose an unexpected reconciliation path.
  const reconcileSubphase =
    next.backendSwitchIntent?.stateReconcileIntent?.subphase;
  if (
    reconcileSubphase !== undefined &&
    reconcileSubphase !==
      previous.backendSwitchIntent?.stateReconcileIntent?.subphase
  ) {
    return reconcileSubphase;
  }
  return next.phase;
}

class RecordingFleetStore implements FleetStateStore {
  readonly #records = new Map<string, FleetRecord>();
  readonly #ops: MigrationOpLogEntry[];
  readonly #fenceViolations: string[];

  constructor(
    records: readonly FleetRecord[],
    ops: MigrationOpLogEntry[],
    fenceViolations: string[],
  ) {
    this.#ops = ops;
    this.#fenceViolations = fenceViolations;
    for (const record of records) {
      this.#records.set(deploymentKey(record), record);
    }
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    this.#ops.push('withDeploymentLease');
    const key = deploymentKey({ tenantTag, environment });
    return operation({
      tenantTag,
      environment,
      mutationLeaseTtlMs: 900_000,
      assertOwned: async () => {
        this.#ops.push('assertOwned');
      },
      renew: async () => {
        throw new Error('unused');
      },
      put: async (record) => {
        // Recording the fault keeps the stop runner from accepting it as a
        // migration refusal.
        if (record.updatedAt !== FROZEN_UPDATED_AT) {
          const message = `put payload updatedAt '${record.updatedAt}' for '${key}' does not match the frozen migration clock`;
          this.#fenceViolations.push(message);
          throw new Error(message);
        }
        if (deploymentKey(record) !== key) {
          const message = `put payload for '${deploymentKey(record)}' arrived under the lease for '${key}'`;
          this.#fenceViolations.push(message);
          throw new Error(message);
        }
        this.#ops.push(`put:${putToken(this.#records.get(key), record)}`);
        this.#records.set(key, record);
      },
      delete: async () => {
        throw new Error('unused');
      },
      completeCleanup: async () => {
        throw new Error('unused');
      },
      deleteReleasingClaims: async () => {
        throw new Error('unused');
      },
    });
  }

  async get(
    tenantTag: string,
    environment: string,
  ): Promise<FleetRecord | undefined> {
    this.#ops.push('get');
    return this.#records.get(deploymentKey({ tenantTag, environment }));
  }

  async list(): Promise<never> {
    throw new Error('unused');
  }

  async readCleanupReceipt(): Promise<never> {
    throw new Error('unused');
  }

  async pruneCleanupReceipts(): Promise<never> {
    throw new Error('unused');
  }
}

class RecordingPlainBackend implements ProvisioningBackend {
  readonly kind: ProvisioningBackendKind = 'plain-worker';
  protected readonly ops: MigrationOpLogEntry[];
  protected readonly specsByDatabaseId: ReadonlyMap<string, DeploymentSpec>;
  protected readonly fenceViolations: string[];
  protected readonly live = new Map<string, LiveDeployment>();
  protected readonly routed = new Map<string, ActiveRouteAttestation>();

  constructor(
    ops: MigrationOpLogEntry[],
    specsByDatabaseId: ReadonlyMap<string, DeploymentSpec>,
    fenceViolations: string[],
  ) {
    this.ops = ops;
    this.specsByDatabaseId = specsByDatabaseId;
    this.fenceViolations = fenceViolations;
  }

  protected assertOwnCredential(
    spec: DeploymentSpec,
    secret: string,
    credential: 'maintenanceAdmin' | 'deploymentIdentity' = 'maintenanceAdmin',
  ): void {
    const expected = secretsForTenant(spec.tenantTag)[credential];
    if (secret !== expected) {
      const label =
        credential === 'maintenanceAdmin'
          ? 'maintenance'
          : 'deployment identity';
      const message = `${label} credential for '${deploymentKey(spec)}' reached a call for another deployment`;
      this.fenceViolations.push(message);
      throw new Error(message);
    }
  }

  seedLive(tenantTag: string, live: LiveDeployment): void {
    this.live.set(tenantTag, live);
  }

  async findDatabase(): Promise<never> {
    throw new Error('unused');
  }

  async getDatabase(databaseId: string): Promise<DatabaseReference> {
    this.ops.push('getDatabase');
    return {
      id: databaseId,
      name: `database-${databaseId.replace(/^db-/u, '')}`,
      created: false,
    };
  }

  async ensureDatabase(): Promise<never> {
    throw new Error('unused');
  }

  async seedDeploymentIdentity(): Promise<void> {
    this.ops.push('seedDeploymentIdentity');
  }

  async readDeploymentIdentity(
    database: DatabaseReference,
  ): Promise<string | undefined> {
    this.ops.push('readDeploymentIdentity');
    return database.id.replace(/^db-/u, '');
  }

  async applyMigrations(
    database: DatabaseReference,
    migrations: readonly D1Migration[],
  ): Promise<void> {
    const spec = this.specsByDatabaseId.get(database.id);
    if (!spec) {
      throw new Error(`no spec fixture for database '${database.id}'`);
    }
    // The final per-version slice can equal spec.migrations by content.
    this.ops.push(
      migrations === spec.migrations
        ? 'applyMigrations:verify'
        : `applyMigrations:${migrations.length}`,
    );
  }

  async deployWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    secrets: DeploymentSecrets,
    _platformResources: ExternalPlatformResources | undefined,
    _fence: ExternalMutationFence,
    _expectedArtifactVersion: string | undefined,
    application?: ApplicationBindingTopology,
  ): Promise<Readonly<{ artifactVersion: string; created: boolean }>> {
    this.ops.push('deployWorker');
    this.assertOwnCredential(
      spec,
      secrets.deploymentIdentity,
      'deploymentIdentity',
    );
    this.assertOwnCredential(spec, secrets.maintenanceAdmin);
    const artifactVersion = `v${spec.schemaVersion}`;
    this.live.set(
      spec.tenantTag,
      liveDeployment({
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        scriptName: spec.scriptName,
        databaseId: database.id,
        durableObjectBindings: [],
        serviceBindings: [],
        queueProducerBindings: [],
        plainTextBindings: {},
        secretNames: [
          'DEPLOYMENT_IDENTITY_SECRET',
          ...(spec.authoredBy === 'platform'
            ? ['MAINTENANCE_ADMIN_SECRET']
            : []),
          ...(application?.secrets ?? []).map(({ name }) => name),
        ].sort(),
        artifactVersion,
        desiredSpecDigest: deploymentSpecDigest(spec),
        schemaVersion: spec.schemaVersion,
        maintenance: HEALTHY_MAINTENANCE,
      }),
    );
    return { artifactVersion, created: false };
  }

  async promoteWorker(
    spec: DeploymentSpec,
    _guard: PromotionGuard,
    _outboundPolicy: DeploymentEgressPolicy | undefined,
    _fence: ExternalMutationFence,
    expectedArtifactVersion: string | undefined,
  ): Promise<void> {
    this.ops.push('promoteWorker');
    this.routed.set(spec.tenantTag, {
      specDigest: deploymentSpecDigest(spec),
      artifactVersion:
        expectedArtifactVersion ??
        this.live.get(spec.tenantTag)?.artifactVersion ??
        `v${spec.schemaVersion}`,
      physicalScriptName: spec.scriptName,
      source: 'workers-deployments',
      observedAt: FROZEN_UPDATED_AT,
    });
  }

  async ensureMaintenance(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<MaintenanceHealth> {
    this.ops.push('ensureMaintenance');
    this.assertOwnCredential(spec, maintenanceAdminSecret);
    const live = this.live.get(spec.tenantTag);
    if (live) {
      this.live.set(spec.tenantTag, {
        ...live,
        maintenance: HEALTHY_MAINTENANCE,
      });
    }
    return HEALTHY_MAINTENANCE;
  }

  async inspect(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<LiveDeployment | undefined> {
    this.ops.push('inspect');
    this.assertOwnCredential(spec, maintenanceAdminSecret);
    return this.live.get(spec.tenantTag);
  }

  async attestActiveRoute(
    spec: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    this.ops.push('attestActiveRoute');
    const attestation = this.routed.get(spec.tenantTag);
    if (!attestation) throw new Error(`no route serves '${spec.tenantTag}'`);
    return attestation;
  }

  async removeTraffic(): Promise<never> {
    throw new Error('unused');
  }

  async assertTrafficRemoved(): Promise<never> {
    throw new Error('unused');
  }

  async revokeCredentials(): Promise<never> {
    throw new Error('unused');
  }

  async deleteWorker(): Promise<never> {
    throw new Error('unused');
  }

  async assertDatabaseDetached(): Promise<never> {
    throw new Error('unused');
  }

  async exportDatabase(): Promise<never> {
    throw new Error('unused');
  }

  async deleteDatabase(): Promise<never> {
    throw new Error('unused');
  }
}

// Feature detection distinguishes absent capabilities from methods that throw.
class RecordingImmutableBackend extends RecordingPlainBackend {
  override readonly kind: ProvisioningBackendKind = 'workers-for-platforms';
  readonly immutableExternalArtifacts = true as const;
  readonly releases = new Map<string, LiveDeployment>();
  readonly routedScriptNames = new Map<string, string>();
  readonly stateArtifactDigest: string;
  readonly policyHosts: readonly string[];

  constructor(
    ops: MigrationOpLogEntry[],
    specsByDatabaseId: ReadonlyMap<string, DeploymentSpec>,
    fenceViolations: string[],
    profile: Readonly<{
      stateArtifactDigest: string;
      policyHosts: readonly string[];
    }> = {
      stateArtifactDigest: STATE_ARTIFACT_DIGEST,
      policyHosts: ['api.example.test'],
    },
  ) {
    super(ops, specsByDatabaseId, fenceViolations);
    this.stateArtifactDigest = profile.stateArtifactDigest;
    this.policyHosts = [...profile.policyHosts];
  }

  releaseScriptName(spec: DeploymentSpec): string {
    this.ops.push('releaseScriptName');
    return externalReleaseScriptName(spec);
  }

  describeExternalPlatformTarget(
    spec: DeploymentSpec,
  ): ExternalPlatformTargetDescription {
    this.ops.push('describeExternalPlatformTarget');
    return this.platformTargetFor(spec);
  }

  platformTargetFor(spec: DeploymentSpec): ExternalPlatformTargetDescription {
    return {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateArtifactDigest: this.stateArtifactDigest,
      stateDurableObjectHistoryDigest: STATE_DURABLE_OBJECT_HISTORY_DIGEST,
      egressArtifactDigest: EGRESS_ARTIFACT_DIGEST,
      d1SchemaVersion: spec.schemaVersion,
      d1SchemaHistoryDigest: deploymentSpecDigest(spec),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: externalPlatformResourceGroupId(spec),
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        allowedHosts: this.policyHosts,
      }),
    };
  }

  platformResourcesFor(spec: DeploymentSpec): ExternalPlatformResources {
    const target = this.platformTargetFor(spec);
    return {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: externalStateScriptName(spec),
        artifactVersion: 'state-v1',
        artifactDigest: target.stateArtifactDigest,
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(spec),
        artifactVersion: 'egress-v1',
        artifactDigest: EGRESS_ARTIFACT_DIGEST,
        ...target.outboundPolicy,
      },
    };
  }

  async ensurePlatformResources(spec: DeploymentSpec): Promise<
    Readonly<{
      resources: ExternalPlatformResources;
      created: Readonly<{ stateWorker: boolean; egressProxy: boolean }>;
    }>
  > {
    this.ops.push('ensurePlatformResources');
    return {
      resources: this.platformResourcesFor(spec),
      created: { stateWorker: false, egressProxy: false },
    };
  }

  override async deployWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    secrets: DeploymentSecrets,
    _platformResources: ExternalPlatformResources | undefined,
    _fence: ExternalMutationFence,
    _expectedArtifactVersion: string | undefined,
    application?: ApplicationBindingTopology,
  ): Promise<
    Readonly<{
      artifactVersion: string;
      created: boolean;
      physicalScriptName: string;
    }>
  > {
    this.ops.push('deployWorker');
    this.assertOwnCredential(
      spec,
      secrets.deploymentIdentity,
      'deploymentIdentity',
    );
    this.assertOwnCredential(spec, secrets.maintenanceAdmin);
    const physicalScriptName = externalReleaseScriptName(spec);
    const existing = this.releases.get(physicalScriptName);
    if (!existing) {
      this.releases.set(
        physicalScriptName,
        liveDeployment({
          tenantTag: spec.tenantTag,
          environment: spec.environment,
          scriptName: physicalScriptName,
          databaseId: database.id,
          durableObjectBindings: [],
          serviceBindings: [],
          queueProducerBindings: [],
          plainTextBindings: {},
          secretNames: [
            'DEPLOYMENT_IDENTITY_SECRET',
            ...(application?.secrets ?? []).map(({ name }) => name),
          ].sort(),
          artifactVersion: `etag:${physicalScriptName}`,
          desiredSpecDigest: deploymentSpecDigest(spec),
          schemaVersion: spec.schemaVersion,
          maintenance: HEALTHY_MAINTENANCE,
        }),
      );
    }
    return {
      artifactVersion: `etag:${physicalScriptName}`,
      created: !existing,
      physicalScriptName,
    };
  }

  override async promoteWorker(
    spec: DeploymentSpec,
    guard: PromotionGuard,
  ): Promise<void> {
    this.ops.push('promoteWorker');
    const physical = externalReleaseScriptName(spec);
    const routed = this.routedScriptNames.get(spec.tenantTag);
    if (
      (routed === undefined && !guard.allowUnrouted) ||
      (routed !== undefined &&
        !guard.allowedCurrentScriptNames.includes(routed))
    ) {
      throw new Error('route changed after lifecycle intent was persisted');
    }
    this.routedScriptNames.set(spec.tenantTag, physical);
  }

  override async ensureMaintenance(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<MaintenanceHealth> {
    this.ops.push('ensureMaintenance');
    this.assertOwnCredential(spec, maintenanceAdminSecret);
    const physical = externalReleaseScriptName(spec);
    const release = this.releases.get(physical);
    if (release) {
      this.releases.set(physical, {
        ...release,
        maintenance: HEALTHY_MAINTENANCE,
      });
    }
    return HEALTHY_MAINTENANCE;
  }

  override async inspect(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<LiveDeployment | undefined> {
    this.ops.push('inspect');
    this.assertOwnCredential(spec, maintenanceAdminSecret);
    return this.releases.get(externalReleaseScriptName(spec));
  }

  override async attestActiveRoute(
    spec: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    this.ops.push('attestActiveRoute');
    const routed = this.routedScriptNames.get(spec.tenantTag);
    const release = routed ? this.releases.get(routed) : undefined;
    if (!routed || !release) {
      throw new Error(`no release serves '${spec.routeHostname}'`);
    }
    return {
      specDigest: release.desiredSpecDigest,
      artifactVersion: release.artifactVersion,
      physicalScriptName: routed,
      source: 'dispatch-route',
      observedAt: FROZEN_UPDATED_AT,
    };
  }

  async deleteRetainedRelease(
    _spec: DeploymentSpec,
    release: ExternalReleaseSnapshot,
  ): Promise<void> {
    this.ops.push('deleteRetainedRelease');
    this.releases.delete(release.physicalScriptName);
  }
}

class RecordingSettlementHost implements FleetSettlementHost {
  readonly #ops: MigrationOpLogEntry[];

  constructor(ops: MigrationOpLogEntry[]) {
    this.#ops = ops;
  }

  async settle(context: FleetSettlementContext): Promise<void> {
    this.#ops.push(`settle:${context.settlementKey}`);
  }
}

function liveDeployment(
  live: Omit<LiveDeployment, 'providerBindingIdentities'>,
): LiveDeployment {
  return {
    ...live,
    providerBindingIdentities: providerBindingIdentitiesForInspection({
      ...live,
      databaseIds: [live.databaseId],
    }),
  };
}

function baseSpec(
  tenantTag: string,
  overrides: Partial<DeploymentSpec> = {},
): DeploymentSpec {
  const schemaVersion = overrides.schemaVersion ?? 1;
  return {
    tenantTag,
    environment: ENVIRONMENT,
    scriptName: `worker-${tenantTag}`,
    databaseName: `database-${tenantTag}`,
    compatibilityDate: '2026-05-01',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'external',
    schemaVersion,
    migrations: D1_MIGRATIONS.slice(0, schemaVersion),
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: `https://control-worker-${tenantTag}.example.test`,
    routeHostname: `worker-${tenantTag}.example.test`,
    ...overrides,
  };
}

function baseRecord(
  tenantTag: string,
  backend: ProvisioningBackendKind,
  overrides: Partial<FleetRecord> = {},
): FleetRecord {
  return {
    tenantTag,
    backend,
    environment: ENVIRONMENT,
    scriptName: `worker-${tenantTag}`,
    databaseId: `db-${tenantTag}`,
    databaseName: `database-${tenantTag}`,
    schemaVersion: 1,
    artifactVersion: 'v1',
    desiredSpecDigest: 'e'.repeat(64),
    durableObjectBindings: [],
    routeHostname: `worker-${tenantTag}.example.test`,
    phase: 'ready',
    updatedAt: ORIGIN_UPDATED_AT,
    ...overrides,
  };
}

function externalRelease(
  spec: DeploymentSpec,
  overrides: Partial<ExternalReleaseSnapshot> = {},
): ExternalReleaseSnapshot {
  const physicalScriptName = externalReleaseScriptName(spec);
  return {
    physicalScriptName,
    specDigest: deploymentSpecDigest(spec),
    artifactVersion: `etag:${physicalScriptName}`,
    releaseSchemaVersion: spec.schemaVersion,
    application: EMPTY_APPLICATION,
    ...overrides,
  };
}

function seededRelease(
  spec: DeploymentSpec,
  record: FleetRecord,
  maintenance: MaintenanceHealth = HEALTHY_MAINTENANCE,
): LiveDeployment {
  const physicalScriptName = externalReleaseScriptName(spec);
  return liveDeployment({
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    scriptName: physicalScriptName,
    databaseId: record.databaseId,
    durableObjectBindings: [],
    serviceBindings: [],
    queueProducerBindings: [],
    plainTextBindings: {},
    secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
    artifactVersion: `etag:${physicalScriptName}`,
    desiredSpecDigest: deploymentSpecDigest(spec),
    schemaVersion: spec.schemaVersion,
    maintenance,
  });
}

interface WorldRun {
  readonly records: readonly FleetRecord[];
  readonly specs: ReadonlyMap<string, DeploymentSpec>;
  readonly backends: ReadonlyMap<string, ProvisioningBackend>;
  readonly ops: MigrationOpLogEntry[];
  readonly fenceViolations: string[];
}

async function runWorld(world: WorldRun): Promise<readonly FleetRecord[]> {
  const store = new RecordingFleetStore(
    world.records,
    world.ops,
    world.fenceViolations,
  );
  const settlementHost = new RecordingSettlementHost(world.ops);
  const resolve = <Value>(
    kind: string,
    record: FleetRecord,
    source: ReadonlyMap<string, Value>,
  ): Value => {
    const key = deploymentKey(record);
    world.ops.push(`resolver:${kind}:${key}`);
    const value = source.get(key);
    if (value === undefined) {
      throw new Error(`no ${kind} fixture for '${key}'`);
    }
    return value;
  };
  return migrateFleet({
    store,
    records: world.records,
    canaryTenantTags: [],
    backendFor: (record) => resolve('backendFor', record, world.backends),
    specFor: (record) => resolve('specFor', record, world.specs),
    secretsFor: (record) => {
      world.ops.push(`resolver:secretsFor:${deploymentKey(record)}`);
      return secretsForTenant(record.tenantTag);
    },
    settlementFor: (record) => {
      world.ops.push(`resolver:settlementFor:${deploymentKey(record)}`);
      return settlementHost;
    },
    clock: MIGRATION_CLOCK,
    routeAttestation: {
      sleep: async () => {},
    } satisfies Omit<AttestConvergedActiveRouteOptions, 'clock'>,
  });
}

function successWorld(): WorldRun {
  const ops: MigrationOpLogEntry[] = [];
  const fenceViolations: string[] = [];
  const specs = new Map<string, DeploymentSpec>();
  const backends = new Map<string, ProvisioningBackend>();
  const specsByDatabaseId = new Map<string, DeploymentSpec>();
  const steady = new RecordingImmutableBackend(
    ops,
    specsByDatabaseId,
    fenceViolations,
  );
  const moved = new RecordingImmutableBackend(
    ops,
    specsByDatabaseId,
    fenceViolations,
    {
      stateArtifactDigest: MOVED_STATE_ARTIFACT_DIGEST,
      policyHosts: ['narrow.example.test'],
    },
  );
  const plain = new RecordingPlainBackend(
    ops,
    specsByDatabaseId,
    fenceViolations,
  );

  const extfullOrigin = baseSpec('extfull');
  const extfullSpec = baseSpec('extfull', {
    modules: [{ name: 'worker.js', content: 'export default { release: 2 }' }],
  });
  const extfullActive = externalRelease(extfullOrigin);
  const extfullRollback = externalRelease(
    baseSpec('extfull', {
      modules: [
        { name: 'worker.js', content: 'export default { release: 0 }' },
      ],
    }),
  );
  const extfullTarget = steady.platformTargetFor(extfullOrigin);
  const extfull = baseRecord('extfull', 'workers-for-platforms', {
    artifactVersion: extfullActive.artifactVersion,
    desiredSpecDigest: extfullActive.specDigest,
    activeRelease: extfullActive,
    rollbackRelease: extfullRollback,
    platformTarget: extfullTarget,
    outboundPolicy: extfullTarget.outboundPolicy,
    platformResources: steady.platformResourcesFor(extfullOrigin),
    applicationBindings: EMPTY_APPLICATION,
    applicationResources: [],
  });

  const plainmultiOrigin = baseSpec('plainmulti', {
    authoredBy: 'platform',
  });
  const plainmultiSpec = baseSpec('plainmulti', {
    authoredBy: 'platform',
    schemaVersion: 3,
  });
  const plainmulti = baseRecord('plainmulti', 'plain-worker', {
    desiredSpecDigest: deploymentSpecDigest(plainmultiOrigin),
  });
  plain.seedLive(
    'plainmulti',
    liveDeployment({
      tenantTag: 'plainmulti',
      environment: ENVIRONMENT,
      scriptName: plainmulti.scriptName,
      databaseId: plainmulti.databaseId,
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      plainTextBindings: {},
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
      artifactVersion: 'v1',
      desiredSpecDigest: deploymentSpecDigest(plainmultiOrigin),
      schemaVersion: 1,
      maintenance: HEALTHY_MAINTENANCE,
    }),
  );

  // Platform-only changes preserve the applied D1 history.
  const platformonlySpec = baseSpec('platformonly');
  const platformonlyActive = externalRelease(platformonlySpec);
  const platformonlyPriorTarget: ExternalPlatformTargetDescription = {
    ...steady.platformTargetFor(platformonlySpec),
    d1SchemaVersion: 2,
    d1SchemaHistoryDigest: PLATFORM_ONLY_PRIOR_D1_HISTORY_DIGEST,
  };
  const platformonly = baseRecord('platformonly', 'workers-for-platforms', {
    schemaVersion: 2,
    artifactVersion: platformonlyActive.artifactVersion,
    desiredSpecDigest: platformonlyActive.specDigest,
    activeRelease: platformonlyActive,
    platformTarget: platformonlyPriorTarget,
    outboundPolicy: platformonlyPriorTarget.outboundPolicy,
    platformResources: {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: externalStateScriptName(platformonlySpec),
        artifactVersion: 'state-v1',
        artifactDigest: platformonlyPriorTarget.stateArtifactDigest,
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(platformonlySpec),
        artifactVersion: 'egress-v1',
        artifactDigest: EGRESS_ARTIFACT_DIGEST,
        ...platformonlyPriorTarget.outboundPolicy,
      },
    },
    applicationBindings: EMPTY_APPLICATION,
    applicationResources: [],
  });

  const readysteadySpec = baseSpec('readysteady');
  const readysteadyActive = externalRelease(readysteadySpec);
  const readysteadyRollback = externalRelease(
    baseSpec('readysteady', {
      modules: [
        { name: 'worker.js', content: 'export default { release: 0 }' },
      ],
    }),
  );
  const readysteadyRetiring = externalRelease(
    baseSpec('readysteady', {
      modules: [
        { name: 'worker.js', content: 'export default { release: -1 }' },
      ],
    }),
  );
  const readysteadyTarget = steady.platformTargetFor(readysteadySpec);
  const readysteady = baseRecord('readysteady', 'workers-for-platforms', {
    artifactVersion: readysteadyActive.artifactVersion,
    desiredSpecDigest: readysteadyActive.specDigest,
    activeRelease: readysteadyActive,
    rollbackRelease: readysteadyRollback,
    retiringRelease: readysteadyRetiring,
    platformTarget: readysteadyTarget,
    outboundPolicy: readysteadyTarget.outboundPolicy,
    platformResources: steady.platformResourcesFor(readysteadySpec),
    applicationBindings: EMPTY_APPLICATION,
    applicationResources: [],
    settledSettlementKey: fleetSettlementKey({
      tenantTag: 'readysteady',
      environment: ENVIRONMENT,
      specDigest: readysteadyActive.specDigest,
      artifactVersion: readysteadyActive.artifactVersion,
    }),
  });

  const records = [extfull, plainmulti, platformonly, readysteady];
  for (const [record, spec, backend] of [
    [extfull, extfullSpec, steady],
    [plainmulti, plainmultiSpec, plain],
    [platformonly, platformonlySpec, moved],
    [readysteady, readysteadySpec, steady],
  ] as const) {
    specs.set(deploymentKey(record), spec);
    backends.set(deploymentKey(record), backend);
    specsByDatabaseId.set(record.databaseId, spec);
  }

  steady.releases.set(
    extfullActive.physicalScriptName,
    seededRelease(extfullOrigin, extfull),
  );
  steady.releases.set(
    readysteadyActive.physicalScriptName,
    seededRelease(readysteadySpec, readysteady, UNARMED_MAINTENANCE),
  );
  steady.routedScriptNames.set('extfull', extfullActive.physicalScriptName);
  steady.routedScriptNames.set(
    'readysteady',
    readysteadyActive.physicalScriptName,
  );
  moved.releases.set(
    platformonlyActive.physicalScriptName,
    seededRelease(platformonlySpec, platformonly),
  );
  moved.routedScriptNames.set(
    'platformonly',
    platformonlyActive.physicalScriptName,
  );

  return { records, specs, backends, ops, fenceViolations };
}

export async function runFleetMigrationSuccessBaseline(): Promise<{
  readonly result: readonly FleetRecord[];
  readonly ops: readonly MigrationOpLogEntry[];
}> {
  const world = successWorld();
  const result = await runWorld(world);
  if (world.fenceViolations.length > 0) {
    throw new Error(`fence violated: ${world.fenceViolations.join('; ')}`);
  }
  return { result, ops: world.ops };
}

const STOP_REFUSAL =
  "deployment 'bravo:production' has active backend switch 'candidate-deployed'";

function stopWorld(): WorldRun {
  const ops: MigrationOpLogEntry[] = [];
  const fenceViolations: string[] = [];
  const specs = new Map<string, DeploymentSpec>();
  const backends = new Map<string, ProvisioningBackend>();
  const specsByDatabaseId = new Map<string, DeploymentSpec>();
  const plain = new RecordingPlainBackend(
    ops,
    specsByDatabaseId,
    fenceViolations,
  );

  const alphaOrigin = baseSpec('alpha', { authoredBy: 'platform' });
  const alphaSpec = baseSpec('alpha', {
    authoredBy: 'platform',
    schemaVersion: 2,
  });
  const alpha = baseRecord('alpha', 'plain-worker', {
    desiredSpecDigest: deploymentSpecDigest(alphaOrigin),
  });
  plain.seedLive(
    'alpha',
    liveDeployment({
      tenantTag: 'alpha',
      environment: ENVIRONMENT,
      scriptName: alpha.scriptName,
      databaseId: alpha.databaseId,
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      plainTextBindings: {},
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
      artifactVersion: 'v1',
      desiredSpecDigest: deploymentSpecDigest(alphaOrigin),
      schemaVersion: 1,
      maintenance: HEALTHY_MAINTENANCE,
    }),
  );

  // A backend-switch refusal must stay distinguishable from migration progress
  // even when their subphase literals match.
  const bravoSpec = baseSpec('bravo');
  const bravo = baseRecord('bravo', 'plain-worker', {
    desiredSpecDigest: deploymentSpecDigest(bravoSpec),
    backendSwitchIntent: {
      kind: 'backend-switch',
      tenantTag: 'bravo',
      environment: ENVIRONMENT,
      prior: {
        scriptName: 'worker-bravo',
        artifactVersion: 'plain-v1',
        specDigest: deploymentSpecDigest(bravoSpec),
        databaseId: 'db-bravo',
        databaseName: 'database-bravo',
        durableObjectBindings: [],
        namespaceIds: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        applicationResources: [],
        customDomain: {
          id: 'domain-bravo',
          hostname: 'worker-bravo.example.test',
        },
      },
      targetSpecDigest: deploymentSpecDigest(bravoSpec),
      targetApplication: EMPTY_APPLICATION,
      target: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateArtifactDigest: STATE_ARTIFACT_DIGEST,
        stateDurableObjectHistoryDigest: STATE_DURABLE_OBJECT_HISTORY_DIGEST,
        egressArtifactDigest: EGRESS_ARTIFACT_DIGEST,
        d1SchemaVersion: bravoSpec.schemaVersion,
        d1SchemaHistoryDigest: deploymentSpecDigest(bravoSpec),
        outboundPolicy: canonicalDeploymentEgressPolicy({
          policyId: externalPlatformResourceGroupId(bravoSpec),
          tenantTag: 'bravo',
          environment: ENVIRONMENT,
          allowedHosts: ['api.example.test'],
        }),
      },
      rollbackUntil: '2026-06-08T00:00:00.000Z',
      subphase: 'candidate-deployed',
    },
  });

  const charlieSpec = baseSpec('charlie', { authoredBy: 'platform' });
  const charlie = baseRecord('charlie', 'plain-worker', {
    desiredSpecDigest: deploymentSpecDigest(charlieSpec),
  });

  const records = [alpha, bravo, charlie];
  for (const [record, spec] of [
    [alpha, alphaSpec],
    [bravo, bravoSpec],
    [charlie, charlieSpec],
  ] as const) {
    specs.set(deploymentKey(record), spec);
    backends.set(deploymentKey(record), plain);
    specsByDatabaseId.set(record.databaseId, spec);
  }

  return { records, specs, backends, ops, fenceViolations };
}

export async function runFleetMigrationStopBaseline(): Promise<{
  readonly error: string;
  readonly ops: readonly MigrationOpLogEntry[];
}> {
  const world = stopWorld();
  let caught: unknown;
  let settled = false;
  try {
    await runWorld(world);
    settled = true;
  } catch (error) {
    caught = error;
  }
  if (world.fenceViolations.length > 0) {
    throw new Error(`fence violated: ${world.fenceViolations.join('; ')}`);
  }
  if (settled) {
    throw new Error('the stop world resolved instead of refusing');
  }
  if (!(caught instanceof Error) || caught.message !== STOP_REFUSAL) {
    throw new Error(
      `the stop world refused with '${String(caught)}' instead of '${STOP_REFUSAL}'`,
    );
  }
  return { error: caught.message, ops: world.ops };
}
