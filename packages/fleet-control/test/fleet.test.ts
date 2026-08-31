// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { ActiveRouteAttestationError } from '../src/active-route.js';
import {
  canonicalApplicationBindings,
  reserveApplicationR2Resources,
} from '../src/application-bindings.js';
import type {
  BridgeMutationPlan,
  BridgeSnapshot,
  FinalizedOrdinaryStateProvider,
} from '../src/backend-switch.js';
import {
  auditFleetDrift,
  fleetVersionReport,
  migrateFleet,
  rollbackExternalRelease,
} from '../src/fleet.js';
import {
  canonicalDeploymentEgressPolicy,
  durableObjectMigrationHistoryDigest,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalReleaseTopology,
  externalStateScriptName,
  FLEET_AUDIT_PROXY_CLASS_NAME,
  FLEET_AUDIT_PROXY_STATE_BINDING,
} from '../src/platform-resources.js';
import { providerBindingIdentitiesForInspection } from '../src/provider-binding-inventory.js';
import { provisionDeployment } from '../src/provision.js';
import { fleetSettlementKey } from '../src/settlement.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  ActiveRouteAttestation,
  DatabaseReference,
  DeploymentSpec,
  ExternalMutationFence,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetResourceInventory,
  FleetSettlementContext,
  FleetSettlementHost,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  PromotionGuard,
  ProvisioningBackend,
  ProvisioningBackendKind,
  SeedDeploymentIdentityOptions,
} from '../src/types.js';
import { externalReleaseScriptName } from '../src/workers-for-platforms-backend.js';
import { decommissionAdvancingRecordFixture } from './fixtures/decommission-intent-fixture.js';

const MAINTENANCE_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

function completeLiveDeployment(
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
const ROTATED_MAINTENANCE_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v2","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

function record(
  tenantTag: string,
  databaseId = `db-${tenantTag}`,
): FleetRecord {
  const base: FleetRecord = {
    tenantTag,
    backend: 'workers-for-platforms',
    environment: 'production',
    scriptName: `worker-${tenantTag}`,
    databaseId,
    databaseName: `database-${tenantTag}`,
    schemaVersion: 1,
    artifactVersion: 'v1',
    desiredSpecDigest: 'a'.repeat(64),
    durableObjectBindings: [
      {
        name: 'RUNNER',
        className: 'Runner',
        namespaceId: `do-${tenantTag}`,
      },
    ],
    routeHostname: `worker-${tenantTag}.example.test`,
    phase: 'ready',
    updatedAt: '1970-01-01T00:00:00.000Z',
  };
  const outboundPolicy = canonicalDeploymentEgressPolicy({
    policyId: externalPlatformResourceGroupId(spec(base, 1)),
    tenantTag: base.tenantTag,
    environment: base.environment,
    allowedHosts: [],
  });
  return {
    ...base,
    activeRelease: {
      physicalScriptName: base.scriptName,
      specDigest: base.desiredSpecDigest,
      artifactVersion: base.artifactVersion,
      releaseSchemaVersion: base.schemaVersion,
      topology: releaseTopologyFor(base),
    },
    outboundPolicy,
    platformTarget: {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateArtifactDigest: 'a'.repeat(64),
      stateDurableObjectHistoryDigest: 'c'.repeat(64),
      egressArtifactDigest: 'b'.repeat(64),
      d1SchemaVersion: 1,
      d1SchemaHistoryDigest: base.desiredSpecDigest,
      outboundPolicy,
    },
  };
}

function spec(item: FleetRecord, schemaVersion = 2): DeploymentSpec {
  return {
    tenantTag: item.tenantTag,
    environment: item.environment,
    scriptName: item.scriptName,
    databaseName: item.databaseName,
    compatibilityDate: '2026-08-10',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'external',
    schemaVersion,
    migrations: [
      { version: 1, sql: 'CREATE TABLE example (id TEXT PRIMARY KEY)' },
      {
        version: 2,
        sql: 'ALTER TABLE example ADD COLUMN value TEXT',
        rollbackCompatible: true as const,
      },
      {
        version: 3,
        sql: 'ALTER TABLE example ADD COLUMN expanded TEXT',
        rollbackCompatible: true as const,
      },
    ].filter((migration) => migration.version <= schemaVersion),
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: `https://control-${item.scriptName}.example.test`,
    routeHostname: item.routeHostname,
  };
}

/** Pinned so an attestation these fakes return is comparable by value. */
const ATTESTED_AT = '2026-08-11T00:00:00.000Z';

const healthy: MaintenanceHealth = {
  armed: true,
  nextAlarmAt: 11_000,
  lastSweepAt: 9_500,
  lastPurgeAt: 9_000,
};

class FleetBackend implements ProvisioningBackend {
  readonly kind: ProvisioningBackendKind;
  readonly immutableExternalArtifacts?: true;
  readonly calls: string[] = [];
  readonly live = new Map<string, LiveDeployment | undefined>();
  /** What the route names per tenant, written by promotion. */
  readonly routed = new Map<string, ActiveRouteAttestation>();
  /** Strands the route on something else, to drive a settlement mismatch. */
  routeDrift: ActiveRouteAttestation | undefined;
  failTenant: string | undefined;
  inspectFailureTenant: string | undefined;
  maintenanceFailureTenant: string | undefined;
  platformPolicyHosts: readonly string[] = ['api.example.com'];
  platformStateDigest = 'a'.repeat(64);
  platformMaintenanceCapabilityPublicKey = MAINTENANCE_PUBLIC_KEY;
  platformAuditQueueName: string | undefined;

  constructor(kind: ProvisioningBackendKind = 'workers-for-platforms') {
    this.kind = kind;
  }

  async findDatabase(): Promise<DatabaseReference | undefined> {
    throw new Error('unused');
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    const tenantTag = databaseId.replace(/^db-/u, '');
    return {
      id: databaseId,
      name: `database-${tenantTag}`,
      created: false,
    };
  }

  async ensureDatabase(): Promise<DatabaseReference> {
    throw new Error('unused');
  }

  // Declares the fence state so it appears in the call log. A fake that simply
  // omitted the options argument would still satisfy the interface, and an
  // unthreaded fence state would be invisible to every assertion below.
  async seedDeploymentIdentity(
    _database: DatabaseReference,
    tenantTag: string,
    _fence: ExternalMutationFence,
    options: SeedDeploymentIdentityOptions,
  ): Promise<void> {
    this.calls.push(
      `identity:${tenantTag}:${options.initialExecutionFenceState}`,
    );
  }

  async readDeploymentIdentity(
    database: DatabaseReference,
  ): Promise<string | undefined> {
    return database.id.replace(/^db-/u, '');
  }

  async applyMigrations(database: DatabaseReference): Promise<void> {
    this.calls.push(`migrate:${database.name.replace('database-', '')}`);
  }

  describeExternalPlatformTarget(deployment: DeploymentSpec) {
    return {
      ...(this.platformAuditQueueName
        ? { auditQueueName: this.platformAuditQueueName }
        : {}),
      maintenanceCapabilityPublicKey:
        this.platformMaintenanceCapabilityPublicKey,
      stateArtifactDigest: this.platformStateDigest,
      stateDurableObjectHistoryDigest: 'c'.repeat(64),
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
    this.calls.push(`platform:${deployment.tenantTag}`);
    const policy = canonicalDeploymentEgressPolicy({
      policyId: externalPlatformResourceGroupId(deployment),
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
      allowedHosts: this.platformPolicyHosts,
    });
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
    const resources = {
      ...(this.platformAuditQueueName
        ? { auditQueueName: this.platformAuditQueueName }
        : {}),
      maintenanceCapabilityPublicKey:
        this.platformMaintenanceCapabilityPublicKey,
      stateWorker: {
        scriptName: externalStateScriptName(deployment),
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
        ...policy,
      },
    };
    return {
      resources,
      created: { stateWorker: false, egressProxy: false },
    };
  }

  async deployWorker(
    deployment: DeploymentSpec,
    database: DatabaseReference,
    _secrets?: import('../src/types.js').DeploymentSecrets,
    platformResources?: FleetRecord['platformResources'],
  ): Promise<{ artifactVersion: string; created: boolean }> {
    this.calls.push(`deploy:${deployment.tenantTag}`);
    if (this.failTenant === deployment.tenantTag)
      throw new Error('canary failed');
    const externalTopology =
      deployment.authoredBy === 'external' && platformResources
        ? externalReleaseTopology(deployment, platformResources)
        : undefined;
    const application = canonicalApplicationBindings(deployment);
    this.live.set(
      deployment.tenantTag,
      completeLiveDeployment({
        tenantTag: deployment.tenantTag,
        environment: deployment.environment,
        scriptName: deployment.scriptName,
        databaseId: database.id,
        durableObjectBindings:
          externalTopology?.durableObjectBindings ??
          deployment.durableObjectBindings.map((binding) => ({
            ...binding,
            namespaceId: `${deployment.scriptName}-${binding.name}`,
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
        plainTextBindings: Object.fromEntries(
          application.vars.map(({ name, value }) => [name, value]),
        ),
        secretNames: [
          'DEPLOYMENT_IDENTITY_SECRET',
          ...(deployment.authoredBy === 'platform'
            ? ['MAINTENANCE_ADMIN_SECRET']
            : []),
          ...application.secrets.map(({ name }) => name),
        ].sort(),
        artifactVersion: `v${deployment.schemaVersion}`,
        desiredSpecDigest: deploymentSpecDigest(deployment),
        schemaVersion: deployment.schemaVersion,
        maintenance: healthy,
      }),
    );
    return { artifactVersion: `v${deployment.schemaVersion}`, created: false };
  }

  // Promotion is what makes a release routed, here as in the provider, so the
  // attestation below reads what promotion published rather than a value the
  // fake was handed separately.
  async promoteWorker(
    deployment: DeploymentSpec,
    _guard: PromotionGuard,
    _outboundPolicy?: FleetRecord['outboundPolicy'],
    _fence?: unknown,
    expectedArtifactVersion?: string,
  ): Promise<void> {
    this.calls.push(`promote:${deployment.tenantTag}`);
    this.routed.set(deployment.tenantTag, {
      specDigest: deploymentSpecDigest(deployment),
      artifactVersion:
        expectedArtifactVersion ??
        this.live.get(deployment.tenantTag)?.artifactVersion ??
        `v${deployment.schemaVersion}`,
      physicalScriptName: deployment.scriptName,
      source: 'workers-deployments',
      observedAt: ATTESTED_AT,
    });
  }

  async ensureMaintenance(
    deployment: DeploymentSpec,
  ): Promise<MaintenanceHealth> {
    this.calls.push(`maintenance:${deployment.tenantTag}`);
    if (this.maintenanceFailureTenant === deployment.tenantTag) {
      throw new Error('maintenance failed');
    }
    return healthy;
  }

  async inspect(
    deployment: DeploymentSpec,
  ): Promise<LiveDeployment | undefined> {
    this.calls.push(`inspect:${deployment.tenantTag}`);
    if (this.inspectFailureTenant === deployment.tenantTag) {
      throw new Error('inspect failed');
    }
    const inspected = this.live.get(deployment.tenantTag);
    if (!inspected) return undefined;
    const { providerBindingIdentities: _inventory, ...live } = inspected;
    return completeLiveDeployment(live);
  }

  async attestActiveRoute(
    deployment: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    this.calls.push(`attest:${deployment.tenantTag}`);
    const attestation =
      this.routeDrift ?? this.routed.get(deployment.tenantTag);
    if (!attestation) {
      throw new ActiveRouteAttestationError(
        `no deployment serves '${deployment.routeHostname}'`,
        {},
      );
    }
    return attestation;
  }

  async revokeCredentials(): Promise<void> {
    throw new Error('unused');
  }

  async removeTraffic(): Promise<void> {
    throw new Error('unused');
  }

  async assertTrafficRemoved(): Promise<void> {
    throw new Error('unused');
  }

  async deleteWorker(): Promise<void> {
    throw new Error('unused');
  }

  async assertDatabaseDetached(): Promise<void> {}

  async exportDatabase(): Promise<{
    databaseId: string;
    location: string;
    sha256: string;
    size: number;
  }> {
    throw new Error('unused');
  }

  async deleteDatabase(): Promise<void> {
    throw new Error('unused');
  }
}

class FleetStore implements FleetStateStore {
  readonly records = new Map<string, FleetRecord>();
  readonly leases = new Set<string>();
  /** Every accepted write, so a test can place one relative to a callback. */
  readonly puts: FleetRecord[] = [];
  failNextReadyPut = false;
  failNextMigratingSchemaPut = false;
  failNextPutWhen:
    | ((
        next: FleetRecord,
        current: FleetRecord | undefined,
      ) => string | undefined)
    | undefined;

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    const key = `${tenantTag}:${environment}`;
    if (this.leases.has(key)) throw new Error('deployment is already leased');
    this.leases.add(key);
    try {
      return await operation({
        tenantTag,
        environment,
        mutationLeaseTtlMs: 15 * 60_000,
        assertOwned: async () => {},
        renew: async () => {},
        put: (record) => this.put(record),
        delete: () => this.delete(tenantTag),
      });
    } finally {
      this.leases.delete(key);
    }
  }

  async get(
    tenantTag: string,
    _environment: string,
  ): Promise<FleetRecord | undefined> {
    return this.records.get(tenantTag);
  }

  async put(value: FleetRecord): Promise<void> {
    const putFailure = this.failNextPutWhen?.(
      value,
      this.records.get(value.tenantTag),
    );
    if (putFailure) {
      this.failNextPutWhen = undefined;
      throw new Error(putFailure);
    }
    if (
      this.failNextMigratingSchemaPut &&
      value.phase === 'migrating' &&
      value.schemaVersion >
        (this.records.get(value.tenantTag)?.schemaVersion ?? 0)
    ) {
      this.failNextMigratingSchemaPut = false;
      throw new Error('state write failed after D1 migration');
    }
    if (this.failNextReadyPut && value.phase === 'ready') {
      this.failNextReadyPut = false;
      throw new Error('state write failed after route promotion');
    }
    this.records.set(value.tenantTag, value);
    this.puts.push(value);
  }

  async delete(tenantTag: string, _environment?: string): Promise<void> {
    this.records.delete(tenantTag);
  }

  async list(): Promise<readonly FleetRecord[]> {
    return [...this.records.values()];
  }
}

function storeFor(records: readonly FleetRecord[]): FleetStore {
  const store = new FleetStore();
  for (const item of records) {
    store.records.set(item.tenantTag, item);
  }
  return store;
}

class ImmutableFleetBackend extends FleetBackend {
  override readonly immutableExternalArtifacts = true as const;
  readonly releases = new Map<string, LiveDeployment>();
  routedScriptName: string | undefined;
  /** Answers this attestation before the real route, N reads long. */
  staleRoute: ActiveRouteAttestation | undefined;
  staleRouteReads = 0;
  invalidateCandidate = false;
  failNextRetirement = false;
  readonly retiredScriptNames: string[] = [];
  lastPromotedPolicyHosts: readonly string[] | undefined;

  releaseScriptName(deployment: DeploymentSpec): string {
    return externalReleaseScriptName(deployment);
  }

  override async deployWorker(
    deployment: DeploymentSpec,
    database: DatabaseReference,
    _secrets?: unknown,
    _platformResources?: unknown,
    _fence?: unknown,
    _expectedArtifactVersion?: string,
    application?: import('../src/types.js').ApplicationBindingTopology,
  ): Promise<{
    artifactVersion: string;
    created: boolean;
    physicalScriptName: string;
  }> {
    const physicalScriptName = this.releaseScriptName(deployment);
    this.calls.push(`deploy:${deployment.tenantTag}:${physicalScriptName}`);
    const existing = this.releases.get(physicalScriptName);
    if (!existing) {
      this.releases.set(
        physicalScriptName,
        completeLiveDeployment({
          tenantTag: deployment.tenantTag,
          environment: deployment.environment,
          scriptName: physicalScriptName,
          databaseId: this.invalidateCandidate ? 'wrong-database' : database.id,
          durableObjectBindings: [],
          artifactVersion: `etag:${physicalScriptName}`,
          desiredSpecDigest: deploymentSpecDigest(deployment),
          schemaVersion: deployment.schemaVersion,
          plainTextBindings: Object.fromEntries(
            (application?.vars ?? []).map(({ name, value }) => [name, value]),
          ),
          r2BucketBindings: application?.r2Buckets ?? [],
          secretNames: [
            'DEPLOYMENT_IDENTITY_SECRET',
            ...(application?.secrets ?? []).map(({ name }) => name),
          ].sort(),
          maintenance: healthy,
        }),
      );
    }
    return {
      artifactVersion: `etag:${physicalScriptName}`,
      created: !existing,
      physicalScriptName,
    };
  }

  override async inspect(
    deployment: DeploymentSpec,
  ): Promise<LiveDeployment | undefined> {
    this.calls.push(`inspect:${deployment.tenantTag}`);
    const inspected = this.releases.get(this.releaseScriptName(deployment));
    if (!inspected) return undefined;
    const { providerBindingIdentities: _inventory, ...live } = inspected;
    return completeLiveDeployment(live);
  }

  override async promoteWorker(
    deployment: DeploymentSpec,
    guard: PromotionGuard,
    outboundPolicy?: FleetRecord['outboundPolicy'],
  ): Promise<void> {
    const physical = this.releaseScriptName(deployment);
    this.calls.push(`promote:${deployment.tenantTag}:${physical}`);
    if (
      (this.routedScriptName === undefined && !guard.allowUnrouted) ||
      (this.routedScriptName !== undefined &&
        !guard.allowedCurrentScriptNames.includes(this.routedScriptName))
    ) {
      throw new Error('route changed after lifecycle intent was persisted');
    }
    this.routedScriptName = physical;
    this.lastPromotedPolicyHosts = outboundPolicy?.policyHosts;
  }

  // Attests the release the ROUTE names, not the one the spec expects, so a
  // test can put the two out of step and see the attestation say so.
  override async attestActiveRoute(
    deployment: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    this.calls.push(`attest:${deployment.tenantTag}`);
    if (this.routeDrift) return this.routeDrift;
    if (this.staleRouteReads > 0) {
      this.staleRouteReads -= 1;
      const stale = this.staleRoute;
      if (stale) return stale;
    }
    const routed = this.routedScriptName;
    if (routed === undefined) {
      throw new ActiveRouteAttestationError(
        `host route '${deployment.routeHostname}' dispatches to no release`,
        {},
      );
    }
    const release = this.releases.get(routed);
    if (!release) {
      throw new ActiveRouteAttestationError(
        `host route '${deployment.routeHostname}' dispatches to absent release '${routed}'`,
        { routedScriptName: routed },
      );
    }
    return {
      specDigest: release.desiredSpecDigest,
      artifactVersion: release.artifactVersion,
      physicalScriptName: routed,
      source: 'dispatch-route',
      observedAt: ATTESTED_AT,
    };
  }

  async deleteRetainedRelease(
    _deployment: DeploymentSpec,
    release: import('../src/types.js').ExternalReleaseSnapshot,
  ): Promise<void> {
    if (this.failNextRetirement) {
      this.failNextRetirement = false;
      throw new Error('retirement failed');
    }
    this.retiredScriptNames.push(release.physicalScriptName);
    this.releases.delete(release.physicalScriptName);
  }
}

class DispatchSecretRecoveryBackend extends ImmutableFleetBackend {
  failNextDispatchSecretWrite = true;
  readonly incompleteCandidates = new Set<string>();

  override async deployWorker(
    deployment: DeploymentSpec,
    database: DatabaseReference,
  ): Promise<{
    artifactVersion: string;
    created: boolean;
    physicalScriptName: string;
  }> {
    const deployed = await super.deployWorker(deployment, database);
    if (this.failNextDispatchSecretWrite) {
      this.failNextDispatchSecretWrite = false;
      this.incompleteCandidates.add(deployed.physicalScriptName);
      throw new Error('dispatch secret write failed after provider upload');
    }
    this.incompleteCandidates.delete(deployed.physicalScriptName);
    return deployed;
  }

  override async inspect(
    deployment: DeploymentSpec,
  ): Promise<LiveDeployment | undefined> {
    if (this.incompleteCandidates.has(this.releaseScriptName(deployment))) {
      throw new Error('candidate maintenance authentication failed');
    }
    return super.inspect(deployment);
  }
}

function liveFor(
  item: FleetRecord,
  overrides: Partial<LiveDeployment> = {},
): LiveDeployment {
  return completeLiveDeployment({
    tenantTag: item.tenantTag,
    environment: item.environment,
    scriptName: item.scriptName,
    databaseId: item.databaseId,
    durableObjectBindings: item.durableObjectBindings,
    plainTextBindings: {},
    secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
    artifactVersion: item.artifactVersion,
    desiredSpecDigest: item.desiredSpecDigest,
    schemaVersion: item.schemaVersion,
    maintenance: healthy,
    ...overrides,
  });
}

function inventoryFor(records: readonly FleetRecord[]): FleetResourceInventory {
  return {
    findings: [],
    dispatchScriptCount: records.filter(
      (item) => item.backend === 'workers-for-platforms',
    ).length,
    scriptRegistrations: records.map((item) => ({
      scriptName: item.scriptName,
      tenantTag: item.tenantTag,
      environment: item.environment,
      databaseId: item.databaseId,
      routeHostname: item.routeHostname,
    })),
    deployments: records.map((item) => ({
      backend: item.backend,
      scriptName: item.scriptName,
      tenantTag: item.tenantTag,
      environment: item.environment,
      databaseIds: [item.databaseId],
      durableObjectBindings: item.durableObjectBindings,
      plainTextBindings: {},
      secretNames:
        item.backend === 'workers-for-platforms'
          ? ['DEPLOYMENT_IDENTITY_SECRET']
          : [],
      routeHostnames: [item.routeHostname],
      artifactVersion: item.artifactVersion,
      desiredSpecDigest: item.desiredSpecDigest,
      schemaVersion: item.schemaVersion,
    })),
    databaseIds: records.map((item) => item.databaseId),
    namespaceIds: [
      ...new Set(
        records.flatMap((item) => [
          ...item.durableObjectBindings.map((binding) => binding.namespaceId),
          ...(item.platformResources?.stateWorker.namespaceIds ?? []),
        ]),
      ),
    ],
    routes: records.map((item) => ({
      backend: item.backend,
      hostname: item.routeHostname,
      scriptName: item.scriptName,
      tenantTag: item.tenantTag,
      environment: item.environment,
      ...(item.outboundPolicy ?? {}),
    })),
  };
}

function releaseTopologyFor(item: FleetRecord) {
  return {
    durableObjectBindings: item.durableObjectBindings,
    serviceBindings: [],
    queueProducerBindings: [],
    secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
  };
}

describe('fleet operations', () => {
  it('finds duplicate ownership, version drift, and re-arms stale maintenance', async () => {
    const acme = record('acme');
    const beta = record('beta');
    const backend = new FleetBackend();
    backend.live.set(acme.tenantTag, liveFor(acme));
    backend.live.set(
      beta.tenantTag,
      liveFor(beta, {
        databaseId: acme.databaseId,
        durableObjectBindings: acme.durableObjectBindings,
        artifactVersion: 'v0',
        maintenance: {
          armed: false,
          nextAlarmAt: null,
          lastSweepAt: null,
          lastPurgeAt: null,
        },
      }),
    );
    const findings = await auditFleetDrift({
      store: storeFor([acme, beta]),
      records: [acme, beta],
      inventory: inventoryFor([acme, beta]),
      backendFor: () => backend,
      specFor: (item) => spec(item),
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(findings.map((finding) => finding.kind)).toEqual([
      'database-mismatch',
      'duplicate-database',
      'duplicate-namespace',
      'version-drift',
      'maintenance-stale',
    ]);
    expect(backend.calls).toContain('maintenance:beta');
  });

  it('reports host-route drift when the live policy differs from persisted deployment policy', async () => {
    const acme = record('acme');
    const deployment = spec(acme);
    const persistedPolicy = canonicalDeploymentEgressPolicy({
      policyId: externalPlatformResourceGroupId(deployment),
      tenantTag: acme.tenantTag,
      environment: acme.environment,
      allowedHosts: ['api.example.com'],
    });
    const withPolicy: FleetRecord = {
      ...acme,
      platformResources: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: externalStateScriptName(deployment),
          artifactVersion: 'state-v1',
          artifactDigest: 'a'.repeat(64),
          durableObjectBindings: [],
          namespaceIds: [],
        },
        egressProxy: {
          scriptName: externalEgressProxyScriptName(deployment),
          artifactVersion: 'egress-v1',
          artifactDigest: 'b'.repeat(64),
          ...persistedPolicy,
        },
      },
    };
    const baseInventory = inventoryFor([withPolicy]);
    const otherPolicy = canonicalDeploymentEgressPolicy({
      policyId: persistedPolicy.policyId,
      tenantTag: acme.tenantTag,
      environment: acme.environment,
      allowedHosts: ['other.example.com'],
    });
    const inventory: FleetResourceInventory = {
      ...baseInventory,
      routes: baseInventory.routes.map((route) => ({
        ...route,
        ...otherPolicy,
      })),
    };
    const backend = new FleetBackend();
    backend.live.set(acme.tenantTag, liveFor(withPolicy));

    const findings = await auditFleetDrift({
      store: storeFor([withPolicy]),
      records: [withPolicy],
      inventory,
      backendFor: () => backend,
      specFor: () => deployment,
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(findings.map((finding) => finding.kind)).toContain('route-drift');
  });

  it('treats persisted trusted-state namespace history as owned and reports its absence', async () => {
    const acme = record('acme');
    const deployment = spec(acme);
    const policy = acme.outboundPolicy;
    if (!policy) throw new Error('expected WfP outbound policy');
    const historicalNamespaceId = 'state-acme-maintenance-v1';
    const withPlatformNamespaces: FleetRecord = {
      ...acme,
      platformResources: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: externalStateScriptName(deployment),
          artifactVersion: 'state-v2',
          artifactDigest: 'a'.repeat(64),
          durableObjectBindings: [],
          namespaceIds: [historicalNamespaceId],
        },
        egressProxy: {
          scriptName: externalEgressProxyScriptName(deployment),
          artifactVersion: 'egress-v1',
          artifactDigest: 'b'.repeat(64),
          ...policy,
        },
      },
    };
    const inventory = inventoryFor([withPlatformNamespaces]);
    const backend = new FleetBackend();
    backend.live.set(acme.tenantTag, liveFor(withPlatformNamespaces));
    const audit = (namespaceIds: readonly string[]) =>
      auditFleetDrift({
        store: storeFor([withPlatformNamespaces]),
        records: [withPlatformNamespaces],
        inventory: { ...inventory, namespaceIds },
        backendFor: () => backend,
        specFor: () => deployment,
        maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
        staleAfterMs: 1_000,
        now: 10_000,
      });

    const healthy = await audit(inventory.namespaceIds);
    expect(healthy.map((finding) => finding.kind)).not.toContain(
      'orphan-namespace',
    );
    expect(healthy.map((finding) => finding.kind)).not.toContain(
      'missing-namespace',
    );

    const missing = await audit(
      inventory.namespaceIds.filter((id) => id !== historicalNamespaceId),
    );
    expect(missing).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'missing-namespace',
          detail: expect.stringContaining(historicalNamespaceId),
        }),
      ]),
    );

    const beta = record('beta');
    const betaSpec = spec(beta);
    if (!beta.outboundPolicy) throw new Error('expected beta WfP policy');
    const betaWithSharedHistory: FleetRecord = {
      ...beta,
      platformResources: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: externalStateScriptName(betaSpec),
          artifactVersion: 'state-v2',
          artifactDigest: 'a'.repeat(64),
          durableObjectBindings: [],
          namespaceIds: [historicalNamespaceId],
        },
        egressProxy: {
          scriptName: externalEgressProxyScriptName(betaSpec),
          artifactVersion: 'egress-v1',
          artifactDigest: 'b'.repeat(64),
          ...beta.outboundPolicy,
        },
      },
    };
    backend.live.set(beta.tenantTag, liveFor(betaWithSharedHistory));
    const duplicateRecords = [withPlatformNamespaces, betaWithSharedHistory];
    const duplicateFindings = await auditFleetDrift({
      store: storeFor(duplicateRecords),
      records: duplicateRecords,
      inventory: inventoryFor(duplicateRecords),
      backendFor: () => backend,
      specFor: (item) => (item.tenantTag === 'beta' ? betaSpec : deployment),
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });
    expect(duplicateFindings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          tenantTag: 'beta',
          kind: 'duplicate-namespace',
          detail: expect.stringContaining(historicalNamespaceId),
        }),
      ]),
    );
  });

  it('reports broadened route policy for platform-authored WfP deployments', async () => {
    const acme = record('acme');
    if (!acme.outboundPolicy) throw new Error('expected WfP outbound policy');
    const baseInventory = inventoryFor([acme]);
    const broadened = canonicalDeploymentEgressPolicy({
      policyId: acme.outboundPolicy.policyId,
      tenantTag: acme.tenantTag,
      environment: acme.environment,
      allowedHosts: ['attacker.example.com'],
    });
    const backend = new FleetBackend();
    backend.live.set(acme.tenantTag, liveFor(acme));

    const findings = await auditFleetDrift({
      store: storeFor([acme]),
      records: [acme],
      inventory: {
        ...baseInventory,
        routes: baseInventory.routes.map((route) => ({
          ...route,
          ...broadened,
        })),
      },
      backendFor: () => backend,
      specFor: (item) => ({ ...spec(item), authoredBy: 'platform' }),
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(findings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'route-drift',
          detail: expect.stringContaining(acme.routeHostname),
        }),
      ]),
    );
  });

  it('checks each configured duty so a fresh sweep cannot mask stale purge or failed tick', async () => {
    const acme = record('acme');
    const backend = new FleetBackend();
    backend.live.set(
      acme.tenantTag,
      liveFor(acme, {
        maintenance: {
          armed: true,
          nextAlarmAt: 10_100,
          lastSweepAt: 9_900,
          lastPurgeAt: 7_000,
          lastTickAt: 9_900,
          lastTickAttemptAt: 9_950,
          lastTickError: 'tick database unavailable',
        },
      }),
    );
    const findings = await auditFleetDrift({
      store: storeFor([acme]),
      records: [acme],
      inventory: inventoryFor([acme]),
      backendFor: () => backend,
      specFor: (item) => spec(item),
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(findings).toEqual([
      expect.objectContaining({
        kind: 'maintenance-stale',
        detail: expect.stringMatching(
          /purge last succeeded 3000ms ago; tick last attempt failed at 9950: tick database unavailable/,
        ),
      }),
    ]);
    expect(findings[0]?.detail).not.toContain('sweep');
    expect(backend.calls.filter((call) => call === 'maintenance:acme')).toEqual(
      ['maintenance:acme'],
    );
  });

  it('audits recorded and live resources in both directions', async () => {
    const acme = record('acme');
    const backend = new FleetBackend();
    backend.live.set(acme.tenantTag, liveFor(acme));
    const inventory = inventoryFor([acme]);
    const registeredDeployment = inventory.deployments[0];
    if (!registeredDeployment) throw new Error('missing test deployment');

    const findings = await auditFleetDrift({
      store: storeFor([acme]),
      records: [acme],
      inventory: {
        findings: inventory.findings,
        dispatchScriptCount: inventory.dispatchScriptCount,
        scriptRegistrations: [
          ...inventory.scriptRegistrations,
          {
            scriptName: 'orphan-worker',
            tenantTag: 'orphan',
            environment: 'production',
            databaseId: 'db-orphan',
            routeHostname: 'orphan.example.test',
          },
        ],
        deployments: [
          ...inventory.deployments,
          {
            ...registeredDeployment,
            scriptName: 'orphan-worker',
            tenantTag: 'orphan',
            databaseIds: ['db-orphan'],
            routeHostnames: ['orphan.example.test'],
          },
        ],
        databaseIds: [...inventory.databaseIds, 'db-orphan'],
        namespaceIds: inventory.namespaceIds,
        routes: [
          ...inventory.routes,
          {
            backend: 'workers-for-platforms',
            hostname: 'orphan.example.test',
            scriptName: 'orphan-worker',
            tenantTag: 'orphan',
            environment: 'production',
          },
        ],
      },
      backendFor: () => backend,
      specFor: (item) => spec(item),
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(findings.map((finding) => finding.kind)).toEqual([
      'orphan-deployment',
      'orphan-database',
      'orphan-route',
    ]);
  });

  it('treats every durable lifecycle release as owned while limiting route targets to the phase transition', async () => {
    const base = record('acme');
    const release = (physicalScriptName: string, version: number) => ({
      physicalScriptName,
      specDigest: String(version).repeat(64),
      artifactVersion: `etag-${version}`,
      releaseSchemaVersion: version,
      topology: {
        ...releaseTopologyFor(base),
        durableObjectBindings:
          version === 1
            ? base.durableObjectBindings
            : [
                {
                  name: `RUNNER_V${version}`,
                  className: `RunnerV${version}`,
                  namespaceId: `do-acme-v${version}`,
                },
              ],
      },
    });
    const active = release('fleet-acme-active', 1);
    const pending = release('fleet-acme-pending', 2);
    const olderRollback = release('fleet-acme-older-rollback', 1);
    const retiring = release('fleet-acme-retiring', 1);
    const scenarios: Array<{
      phase: FleetRecord['phase'];
      routeScriptName: string;
    }> = [
      { phase: 'migrating', routeScriptName: active.physicalScriptName },
      { phase: 'migrating', routeScriptName: pending.physicalScriptName },
      { phase: 'rolling-back', routeScriptName: active.physicalScriptName },
      { phase: 'rolling-back', routeScriptName: pending.physicalScriptName },
      { phase: 'ready', routeScriptName: active.physicalScriptName },
    ];

    for (const scenario of scenarios) {
      const acme: FleetRecord = {
        ...base,
        artifactVersion: active.artifactVersion,
        activeRelease: active,
        rollbackRelease: olderRollback,
        ...(scenario.phase === 'ready'
          ? { retiringRelease: retiring }
          : {
              pendingRelease: pending,
              migrationPriorRelease: active,
            }),
        ...(scenario.phase === 'migrating' && base.platformTarget
          ? {
              migrationIntent: {
                targetSpecDigest: pending.specDigest,
                priorRelease: active,
                priorTarget: base.platformTarget,
                priorOutboundPolicy: base.platformTarget.outboundPolicy,
                targetRelease: pending,
                target: base.platformTarget,
                subphase: 'candidate-armed' as const,
              },
            }
          : {}),
        phase: scenario.phase,
        updatedAt: '1970-01-01T00:00:10.000Z',
      };
      const releases = [
        acme.activeRelease,
        acme.pendingRelease,
        acme.migrationPriorRelease,
        acme.rollbackRelease,
        acme.retiringRelease,
      ].filter(
        (
          candidate,
          index,
          candidates,
        ): candidate is NonNullable<typeof candidate> =>
          candidate !== undefined &&
          candidates.findIndex(
            (other) =>
              other?.physicalScriptName === candidate.physicalScriptName,
          ) === index,
      );
      const inventory: FleetResourceInventory = {
        findings: [],
        dispatchScriptCount: releases.length,
        scriptRegistrations: releases.map((candidate) => ({
          scriptName: candidate.physicalScriptName,
          tenantTag: acme.tenantTag,
          environment: acme.environment,
          databaseId: acme.databaseId,
          routeHostname: acme.routeHostname,
        })),
        deployments: releases.map((candidate) => {
          if (!candidate.topology) throw new Error('missing test topology');
          return {
            backend: acme.backend,
            scriptName: candidate.physicalScriptName,
            tenantTag: acme.tenantTag,
            environment: acme.environment,
            databaseIds: [acme.databaseId],
            durableObjectBindings: candidate.topology.durableObjectBindings,
            plainTextBindings: {},
            secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
            routeHostnames:
              candidate.physicalScriptName === scenario.routeScriptName
                ? [acme.routeHostname]
                : [],
            artifactVersion: candidate.artifactVersion,
            desiredSpecDigest: candidate.specDigest,
            schemaVersion: candidate.releaseSchemaVersion,
          };
        }),
        databaseIds: [acme.databaseId],
        namespaceIds: [
          ...acme.durableObjectBindings.map((binding) => binding.namespaceId),
          ...releases.flatMap((candidate) => {
            if (!candidate.topology) throw new Error('missing test topology');
            return candidate.topology.durableObjectBindings.map(
              (binding) => binding.namespaceId,
            );
          }),
        ],
        routes: [
          {
            backend: acme.backend,
            hostname: acme.routeHostname,
            scriptName: scenario.routeScriptName,
            tenantTag: acme.tenantTag,
            environment: acme.environment,
            ...acme.outboundPolicy,
          },
        ],
      };
      const backend = new FleetBackend();
      backend.live.set(
        acme.tenantTag,
        liveFor(acme, {
          scriptName: active.physicalScriptName,
          schemaVersion: active.releaseSchemaVersion,
        }),
      );

      const findings = await auditFleetDrift({
        store: storeFor([acme]),
        records: [acme],
        inventory,
        backendFor: () => backend,
        specFor: (item) => spec(item),
        maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
        staleAfterMs: 1_000,
        now: 10_000,
      });

      expect(findings, `${scenario.phase}:${scenario.routeScriptName}`).toEqual(
        [],
      );
    }
  });

  it('audits external audit access as an exact remote Durable Object binding with no service edge', async () => {
    const base = record('acme');
    const stateScriptName = externalStateScriptName(base);
    const auditBinding = {
      name: 'AUDIT_PROXY',
      className: 'FlowsafeFleetAuditProxy',
      namespaceId: 'state-acme-audit-proxy',
      scriptName: stateScriptName,
      dispatchNamespace: 'fleet-conformance',
    };
    const activeRelease = {
      physicalScriptName: base.scriptName,
      specDigest: base.desiredSpecDigest,
      artifactVersion: base.artifactVersion,
      releaseSchemaVersion: base.schemaVersion,
      topology: {
        durableObjectBindings: [auditBinding],
        serviceBindings: [],
        queueProducerBindings: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
      },
    };
    const current: FleetRecord = {
      ...base,
      activeRelease,
      durableObjectBindings: [auditBinding],
    };
    const auditedSpec: DeploymentSpec = {
      ...spec(current, 1),
      queueProducer: { binding: 'AUDIT_QUEUE', queueName: 'fleet-audit' },
    };
    const baseInventory = inventoryFor([current]);
    const candidate = baseInventory.deployments[0];
    if (!candidate) throw new Error('missing candidate inventory fixture');
    const backend = new FleetBackend();
    backend.live.set(current.tenantTag, liveFor(current));
    const audit = (
      durableObjectBindings: FleetResourceInventory['deployments'][number]['durableObjectBindings'],
      serviceBindings: NonNullable<
        FleetResourceInventory['deployments'][number]['serviceBindings']
      > = [],
    ) =>
      auditFleetDrift({
        store: storeFor([current]),
        records: [current],
        inventory: {
          ...baseInventory,
          deployments: [
            {
              ...candidate,
              durableObjectBindings,
              serviceBindings,
              queueProducerBindings: [],
              desiredSpecDigest: activeRelease.specDigest,
            },
          ],
        },
        backendFor: () => backend,
        specFor: () => auditedSpec,
        maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
        staleAfterMs: 1_000,
        now: 10_000,
      });

    await expect(audit([auditBinding])).resolves.toEqual([]);
    for (const drifted of [
      [],
      [{ ...auditBinding, dispatchNamespace: 'foreign-namespace' }],
    ]) {
      await expect(audit(drifted)).resolves.toEqual(
        expect.arrayContaining([
          expect.objectContaining({ kind: 'binding-drift' }),
        ]),
      );
    }
    await expect(
      audit(
        [auditBinding],
        [{ name: 'AUDIT_PROXY', service: stateScriptName }],
      ),
    ).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ kind: 'binding-drift' }),
      ]),
    );
  });

  it.each([
    ['rollback', 'database', 'database-mismatch'],
    ['rollback', 'topology', 'binding-drift'],
    ['rollback', 'artifact', 'version-drift'],
    ['rollback', 'schema', 'version-drift'],
    ['rollback', 'digest', 'version-drift'],
    ['retiring', 'database', 'database-mismatch'],
    ['retiring', 'topology', 'binding-drift'],
    ['retiring', 'artifact', 'version-drift'],
    ['retiring', 'schema', 'version-drift'],
    ['retiring', 'digest', 'version-drift'],
  ] as const)('detects %s lifecycle release %s drift', async (releaseRole, drift, expectedKind) => {
    const base = record('acme');
    const release = (physicalScriptName: string, version: number) => ({
      physicalScriptName,
      specDigest: String(version).repeat(64),
      artifactVersion: `etag-${version}`,
      releaseSchemaVersion: version,
      topology: releaseTopologyFor(base),
    });
    const active = release('fleet-acme-active', 1);
    const rollback = release('fleet-acme-rollback', 2);
    const retiring = release('fleet-acme-retiring', 3);
    const acme: FleetRecord = {
      ...base,
      artifactVersion: active.artifactVersion,
      desiredSpecDigest: active.specDigest,
      activeRelease: active,
      rollbackRelease: rollback,
      retiringRelease: retiring,
    };
    const releases = [active, rollback, retiring];
    const driftedName =
      releaseRole === 'rollback'
        ? rollback.physicalScriptName
        : retiring.physicalScriptName;
    const inventory = inventoryFor([acme]);
    const lifecycleInventory: FleetResourceInventory = {
      ...inventory,
      dispatchScriptCount: releases.length,
      scriptRegistrations: releases.map((candidate) => ({
        scriptName: candidate.physicalScriptName,
        tenantTag: acme.tenantTag,
        environment: acme.environment,
        databaseId: acme.databaseId,
        routeHostname: acme.routeHostname,
      })),
      deployments: releases.map((candidate) => ({
        backend: acme.backend,
        scriptName: candidate.physicalScriptName,
        tenantTag: acme.tenantTag,
        environment: acme.environment,
        databaseIds:
          candidate.physicalScriptName === driftedName && drift === 'database'
            ? ['db-wrong']
            : [acme.databaseId],
        durableObjectBindings: acme.durableObjectBindings,
        plainTextBindings: {},
        serviceBindings:
          candidate.physicalScriptName === driftedName && drift === 'topology'
            ? [{ name: 'FORGED_PROXY', service: 'attacker-worker' }]
            : [],
        queueProducerBindings: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        routeHostnames:
          candidate.physicalScriptName === active.physicalScriptName
            ? [acme.routeHostname]
            : [],
        artifactVersion:
          candidate.physicalScriptName === driftedName && drift === 'artifact'
            ? 'etag-wrong'
            : candidate.artifactVersion,
        desiredSpecDigest:
          candidate.physicalScriptName === driftedName && drift === 'digest'
            ? 'f'.repeat(64)
            : candidate.specDigest,
        schemaVersion:
          candidate.physicalScriptName === driftedName && drift === 'schema'
            ? 99
            : candidate.releaseSchemaVersion,
      })),
      routes: [
        {
          backend: acme.backend,
          hostname: acme.routeHostname,
          scriptName: active.physicalScriptName,
          tenantTag: acme.tenantTag,
          environment: acme.environment,
          ...acme.outboundPolicy,
        },
      ],
    };
    const backend = new FleetBackend();
    backend.live.set(
      acme.tenantTag,
      liveFor(acme, {
        scriptName: active.physicalScriptName,
        schemaVersion: active.releaseSchemaVersion,
      }),
    );

    const findings = await auditFleetDrift({
      store: storeFor([acme]),
      records: [acme],
      inventory: lifecycleInventory,
      backendFor: () => backend,
      specFor: (item) => spec(item),
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(findings).toContainEqual(
      expect.objectContaining({
        kind: expectedKind,
        detail: expect.stringContaining(driftedName),
      }),
    );
  });

  it.each([
    ['planned', 'prior', 'prior', false],
    ['planned', 'target', 'target', true],
    ['schema-applied', 'prior', 'prior', false],
    ['schema-applied', 'prior', 'target', true],
    ['platform-applied', 'prior', 'prior', false],
    ['platform-applied', 'target', 'target', true],
    ['candidate-deployed', 'prior', 'prior', false],
    ['candidate-deployed', 'target', 'target', true],
    ['candidate-armed', 'target', 'target', false],
    ['candidate-armed', 'prior', 'prior', false],
    ['candidate-armed', 'prior', 'target', true],
    ['route-published', 'target', 'target', false],
    ['route-published', 'prior', 'prior', true],
  ] as const)('audits a broad-to-narrow migration at %s with the %s route and %s policy', async (subphase, routeVariant, policyVariant, expectDrift) => {
    const base = record('acme');
    const targetSpec = spec(base, 2);
    const backend = new ImmutableFleetBackend();
    const priorPolicy = canonicalDeploymentEgressPolicy({
      policyId: externalPlatformResourceGroupId(targetSpec),
      tenantTag: base.tenantTag,
      environment: base.environment,
      allowedHosts: ['api.example.com', 'legacy.example.com'],
    });
    backend.platformPolicyHosts = ['api.example.com'];
    const target = {
      ...backend.describeExternalPlatformTarget(targetSpec),
      sharedOutboundWorkerName: 'fleet-outbound',
      stateEgressCredentialDigest: 'd'.repeat(64),
    };
    const priorTarget = { ...target, outboundPolicy: priorPolicy };
    const priorRelease = {
      physicalScriptName: 'release-prior',
      specDigest: 'a'.repeat(64),
      artifactVersion: 'etag-prior',
      releaseSchemaVersion: 1,
    };
    const targetRelease = {
      physicalScriptName: 'release-target',
      specDigest: deploymentSpecDigest(targetSpec),
      artifactVersion: 'etag-target',
      releaseSchemaVersion: 2,
    };
    const migrating: FleetRecord = {
      ...base,
      schemaVersion: 2,
      desiredSpecDigest: targetRelease.specDigest,
      activeRelease: priorRelease,
      pendingRelease: targetRelease,
      migrationPriorRelease: priorRelease,
      platformTarget: target,
      platformResources: {
        maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: 'state-acme',
          artifactVersion: 'state-v1',
          artifactDigest: target.stateArtifactDigest,
          plane: 'dispatch',
          dispatchNamespace: 'fleet',
          durableObjectBindings: [],
          namespaceIds: [],
        },
        sharedOutboundWorkerName: target.sharedOutboundWorkerName,
        outboundPolicy: target.outboundPolicy,
      },
      outboundPolicy: target.outboundPolicy,
      phase: 'migrating',
      migrationIntent: {
        targetSpecDigest: targetRelease.specDigest,
        priorRelease,
        priorTarget,
        priorOutboundPolicy: priorPolicy,
        targetRelease,
        target,
        subphase,
      },
      updatedAt: '1970-01-01T00:00:10.000Z',
    };
    const routeRelease =
      routeVariant === 'prior' ? priorRelease : targetRelease;
    const routePolicy =
      policyVariant === 'prior' ? priorPolicy : target.outboundPolicy;
    const inventory: FleetResourceInventory = {
      findings: [],
      dispatchScriptCount: 2,
      scriptRegistrations: [priorRelease, targetRelease].map((release) => ({
        scriptName: release.physicalScriptName,
        tenantTag: migrating.tenantTag,
        environment: migrating.environment,
        databaseId: migrating.databaseId,
        routeHostname: migrating.routeHostname,
      })),
      deployments: [priorRelease, targetRelease].map((release) => ({
        backend: migrating.backend,
        scriptName: release.physicalScriptName,
        tenantTag: migrating.tenantTag,
        environment: migrating.environment,
        databaseIds: [migrating.databaseId],
        durableObjectBindings: migrating.durableObjectBindings,
        plainTextBindings: {},
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        routeHostnames:
          release === routeRelease ? [migrating.routeHostname] : [],
        artifactVersion: release.artifactVersion,
        desiredSpecDigest: release.specDigest,
        schemaVersion: release.releaseSchemaVersion,
      })),
      databaseIds: [migrating.databaseId],
      namespaceIds: migrating.durableObjectBindings.map(
        (binding) => binding.namespaceId,
      ),
      routes: [
        {
          backend: migrating.backend,
          hostname: migrating.routeHostname,
          scriptName: routeRelease.physicalScriptName,
          tenantTag: migrating.tenantTag,
          environment: migrating.environment,
          ...routePolicy,
          stateEgress: {
            resourceGroupId: routePolicy.policyId,
            stateScriptName: 'state-acme',
            credentialDigest: target.stateEgressCredentialDigest,
          },
        },
      ],
    };

    const findings = await auditFleetDrift({
      store: storeFor([migrating]),
      records: [migrating],
      inventory,
      backendFor: () => backend,
      specFor: () => targetSpec,
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });
    expect(findings.some((finding) => finding.kind === 'orphan-route')).toBe(
      expectDrift,
    );
  });

  it.each(
    (
      [
        'publishing',
        'rolling-back',
        'decommissioning',
        'credentials-revoked',
      ] as const
    ).flatMap((phase) =>
      (['exact', 'state-script', 'resource-group', 'credential'] as const).map(
        (drift) => [phase, drift] as const,
      ),
    ),
  )('audits exact state-egress route context in %s with %s', async (phase, drift) => {
    const base = record('acme');
    const backend = new ImmutableFleetBackend();
    const currentSpec = spec(base, 2);
    const target = {
      ...backend.describeExternalPlatformTarget(currentSpec),
      sharedOutboundWorkerName: 'fleet-outbound',
      stateEgressCredentialDigest: 'd'.repeat(64),
    };
    const activeRelease = {
      physicalScriptName: 'release-active',
      specDigest: 'a'.repeat(64),
      artifactVersion: 'etag-active',
      releaseSchemaVersion: 2,
    };
    const pendingRelease = {
      physicalScriptName: 'release-pending',
      specDigest: 'b'.repeat(64),
      artifactVersion: 'etag-pending',
      releaseSchemaVersion: 2,
    };
    const lifecycle: FleetRecord = {
      ...base,
      phase,
      activeRelease,
      pendingRelease,
      platformTarget: target,
      outboundPolicy: target.outboundPolicy,
      platformResources: {
        maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: 'state-acme',
          artifactVersion: 'state-v1',
          artifactDigest: target.stateArtifactDigest,
          plane: 'dispatch',
          dispatchNamespace: 'fleet',
          durableObjectBindings: [],
          namespaceIds: [],
        },
        sharedOutboundWorkerName: target.sharedOutboundWorkerName,
        outboundPolicy: target.outboundPolicy,
      },
    };
    const expectedRelease =
      phase === 'publishing' ? pendingRelease : activeRelease;
    const stateEgress = {
      resourceGroupId: target.outboundPolicy.policyId,
      stateScriptName: 'state-acme',
      credentialDigest: target.stateEgressCredentialDigest,
    };
    const inventory: FleetResourceInventory = {
      findings: [],
      dispatchScriptCount: 0,
      scriptRegistrations: [],
      deployments: [],
      databaseIds: [],
      namespaceIds: [],
      routes: [
        {
          backend: 'workers-for-platforms',
          hostname: lifecycle.routeHostname,
          scriptName: expectedRelease.physicalScriptName,
          tenantTag: lifecycle.tenantTag,
          environment: lifecycle.environment,
          ...target.outboundPolicy,
          stateEgress: {
            ...stateEgress,
            ...(drift === 'state-script'
              ? { stateScriptName: 'foreign-state' }
              : {}),
            ...(drift === 'resource-group'
              ? { resourceGroupId: 'foreign-group' }
              : {}),
            ...(drift === 'credential'
              ? { credentialDigest: 'e'.repeat(64) }
              : {}),
          },
        },
      ],
    };

    const findings = await auditFleetDrift({
      store: storeFor([lifecycle]),
      records: [lifecycle],
      inventory,
      backendFor: () => backend,
      specFor: () => currentSpec,
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: Number.MAX_SAFE_INTEGER,
      now: 10_000,
    });

    expect(findings.some((finding) => finding.kind === 'orphan-route')).toBe(
      drift !== 'exact',
    );
  });

  it('reports missing retained lifecycle releases and namespaces without registry entries', async () => {
    const release = (physicalScriptName: string, version: number) => ({
      physicalScriptName,
      specDigest: String(version).repeat(64),
      artifactVersion: `etag-${version}`,
      releaseSchemaVersion: version,
    });
    const active = release('fleet-acme-active', 2);
    const rollback = release('fleet-acme-rollback', 1);
    const retiring = release('fleet-acme-retiring', 1);
    const acme: FleetRecord = {
      ...record('acme'),
      schemaVersion: 2,
      artifactVersion: active.artifactVersion,
      activeRelease: active,
      rollbackRelease: rollback,
      retiringRelease: retiring,
      phase: 'ready',
      updatedAt: '1970-01-01T00:00:10.000Z',
    };
    const inventory: FleetResourceInventory = {
      findings: [],
      dispatchScriptCount: 1,
      scriptRegistrations: [
        {
          scriptName: active.physicalScriptName,
          tenantTag: acme.tenantTag,
          environment: acme.environment,
          databaseId: acme.databaseId,
          routeHostname: acme.routeHostname,
        },
      ],
      deployments: [
        {
          backend: acme.backend,
          scriptName: active.physicalScriptName,
          tenantTag: acme.tenantTag,
          environment: acme.environment,
          databaseIds: [acme.databaseId],
          durableObjectBindings: acme.durableObjectBindings,
          plainTextBindings: {},
          secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
          routeHostnames: [acme.routeHostname],
          artifactVersion: active.artifactVersion,
          desiredSpecDigest: active.specDigest,
          schemaVersion: active.releaseSchemaVersion,
        },
      ],
      databaseIds: [acme.databaseId],
      namespaceIds: [],
      routes: [
        {
          backend: acme.backend,
          hostname: acme.routeHostname,
          scriptName: active.physicalScriptName,
          tenantTag: acme.tenantTag,
          environment: acme.environment,
        },
      ],
    };
    const backend = new FleetBackend();
    backend.live.set(
      acme.tenantTag,
      liveFor(acme, {
        scriptName: active.physicalScriptName,
        artifactVersion: active.artifactVersion,
        schemaVersion: active.releaseSchemaVersion,
      }),
    );
    const expectedNamespace = acme.durableObjectBindings[0]?.namespaceId;
    if (!expectedNamespace) throw new Error('expected test namespace');

    const findings = await auditFleetDrift({
      store: storeFor([acme]),
      records: [acme],
      inventory,
      backendFor: () => backend,
      specFor: (item) => spec(item),
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(findings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'missing-deployment',
          detail: expect.stringContaining(rollback.physicalScriptName),
        }),
        expect.objectContaining({
          kind: 'missing-deployment',
          detail: expect.stringContaining(retiring.physicalScriptName),
        }),
        expect.objectContaining({
          kind: 'missing-namespace',
          detail: expect.stringContaining(expectedNamespace),
        }),
      ]),
    );
  });

  it('does not invent a candidate outside release-bearing phases', async () => {
    const base = record('acme');
    const deployment = spec(base);
    const policy = canonicalDeploymentEgressPolicy({
      policyId: externalPlatformResourceGroupId(deployment),
      tenantTag: base.tenantTag,
      environment: base.environment,
      allowedHosts: ['api.example.com'],
    });
    const platformResources: NonNullable<FleetRecord['platformResources']> = {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: externalStateScriptName(deployment),
        artifactVersion: 'state-v1',
        artifactDigest: 'a'.repeat(64),
        durableObjectBindings: base.durableObjectBindings,
        namespaceIds: base.durableObjectBindings.map(
          (binding) => binding.namespaceId,
        ),
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(deployment),
        artifactVersion: 'egress-v1',
        artifactDigest: 'b'.repeat(64),
        ...policy,
      },
    };
    const phases: FleetRecord['phase'][] = [
      'platform-resources-deployed',
      'platform-credentials-revoked',
      'platform-resources-deleted',
    ];
    const egressProxy = platformResources.egressProxy;
    if (!egressProxy) throw new Error('legacy egress proxy is missing');

    for (const phase of phases) {
      const current: FleetRecord = {
        ...base,
        phase,
        platformResources,
        updatedAt: '1970-01-01T00:00:10.000Z',
      };
      const expectsTrustedWorkers = phase !== 'platform-resources-deleted';
      const inventory: FleetResourceInventory = {
        findings: [],
        hostRoutingKvId: 'host-routing-kv',
        dispatchScriptCount: 0,
        scriptRegistrations: [],
        deployments: expectsTrustedWorkers
          ? [
              {
                backend: 'plain-worker',
                scriptName: platformResources.stateWorker.scriptName,
                tenantTag: current.tenantTag,
                environment: current.environment,
                databaseIds: [current.databaseId],
                durableObjectBindings: current.durableObjectBindings,
                plainTextBindings: {},
                secretNames: [
                  'DEPLOYMENT_IDENTITY_SECRET',
                  'MAINTENANCE_ADMIN_SECRET',
                ],
                serviceBindings: [
                  {
                    name: 'EGRESS_PROXY',
                    service: egressProxy.scriptName,
                  },
                ],
                routeHostnames: [],
                artifactVersion: platformResources.stateWorker.artifactVersion,
                schemaVersion: current.schemaVersion,
                resourceRole: 'platform-state',
                resourceGroupId: externalPlatformResourceGroupId(deployment),
              },
              {
                backend: 'plain-worker',
                scriptName: egressProxy.scriptName,
                tenantTag: current.tenantTag,
                environment: current.environment,
                databaseIds: [],
                durableObjectBindings: [],
                secretNames: [],
                serviceBindings: [],
                kvNamespaceBindings: [
                  { name: 'HOSTS', namespaceId: 'host-routing-kv' },
                ],
                plainTextBindings: {
                  policyId: egressProxy.policyId,
                  routeHostname: current.routeHostname,
                  hostRoutingKvId: 'host-routing-kv',
                  scriptName: platformResources.stateWorker.scriptName,
                },
                routeHostnames: [],
                artifactVersion: egressProxy.artifactVersion,
                schemaVersion: 0,
                resourceRole: 'deployment-egress',
                resourceGroupId: externalPlatformResourceGroupId(deployment),
              },
            ]
          : [],
        databaseIds: [current.databaseId],
        namespaceIds: expectsTrustedWorkers
          ? current.durableObjectBindings.map((binding) => binding.namespaceId)
          : [],
        routes: [],
      };

      const findings = await auditFleetDrift({
        store: storeFor([current]),
        records: [current],
        inventory,
        backendFor: () => new FleetBackend(),
        specFor: () => deployment,
        maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
        staleAfterMs: 1_000,
        now: 10_000,
      });

      expect(
        findings.filter(
          (finding) =>
            finding.kind === 'missing-deployment' &&
            finding.detail.includes(current.scriptName),
        ),
        phase,
      ).toEqual([]);
      expect(
        findings.filter(
          (finding) =>
            finding.kind === 'binding-drift' ||
            finding.kind === 'version-drift',
        ),
        phase,
      ).toEqual([]);
    }

    const releaseBearing: FleetRecord = {
      ...base,
      phase: 'worker-deployed',
      updatedAt: '1970-01-01T00:00:10.000Z',
    };
    const findings = await auditFleetDrift({
      store: storeFor([releaseBearing]),
      records: [releaseBearing],
      inventory: {
        findings: [],
        dispatchScriptCount: 0,
        scriptRegistrations: [],
        deployments: [],
        databaseIds: [releaseBearing.databaseId],
        namespaceIds: [],
        routes: [],
      },
      backendFor: () => new FleetBackend(),
      specFor: () => spec(releaseBearing),
      maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
      staleAfterMs: 1_000,
      now: 10_000,
    });
    expect(findings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'missing-deployment',
          detail: expect.stringContaining(releaseBearing.scriptName),
        }),
      ]),
    );
  });

  it('detects HOSTS binding and metadata drifting together from the canonical namespace', async () => {
    const base = record('acme');
    const deployment = spec(base, 1);
    const policy = base.outboundPolicy;
    if (!policy) throw new Error('missing test outbound policy');
    const platformResources: NonNullable<FleetRecord['platformResources']> = {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: externalStateScriptName(deployment),
        artifactVersion: 'state-v1',
        artifactDigest: 'a'.repeat(64),
        durableObjectBindings: base.durableObjectBindings,
        namespaceIds: base.durableObjectBindings.map(
          (binding) => binding.namespaceId,
        ),
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(deployment),
        artifactVersion: 'egress-v1',
        artifactDigest: 'b'.repeat(64),
        ...policy,
      },
    };
    const egressProxy = platformResources.egressProxy;
    if (!egressProxy) throw new Error('legacy egress proxy is missing');
    const current: FleetRecord = { ...base, platformResources };
    const groupId = externalPlatformResourceGroupId(deployment);
    const inventory: FleetResourceInventory = {
      findings: [],
      hostRoutingKvId: 'host-routing-kv',
      dispatchScriptCount: 1,
      scriptRegistrations: [
        {
          scriptName: current.scriptName,
          tenantTag: current.tenantTag,
          environment: current.environment,
          databaseId: current.databaseId,
          routeHostname: current.routeHostname,
        },
      ],
      deployments: [
        {
          backend: current.backend,
          scriptName: current.scriptName,
          tenantTag: current.tenantTag,
          environment: current.environment,
          databaseIds: [current.databaseId],
          durableObjectBindings: current.durableObjectBindings,
          plainTextBindings: {},
          secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
          routeHostnames: [current.routeHostname],
          artifactVersion: current.artifactVersion,
          desiredSpecDigest: current.desiredSpecDigest,
          schemaVersion: current.schemaVersion,
        },
        {
          backend: 'plain-worker',
          resourceRole: 'platform-state',
          resourceGroupId: groupId,
          scriptName: platformResources.stateWorker.scriptName,
          tenantTag: current.tenantTag,
          environment: current.environment,
          databaseIds: [current.databaseId],
          durableObjectBindings: current.durableObjectBindings,
          secretNames: [
            'DEPLOYMENT_IDENTITY_SECRET',
            'MAINTENANCE_ADMIN_SECRET',
          ],
          serviceBindings: [
            {
              name: 'EGRESS_PROXY',
              service: egressProxy.scriptName,
            },
          ],
          plainTextBindings: {
            FLEET_DEPLOYMENT_SCRIPT: current.scriptName,
            FLEET_MAINTENANCE_CAPABILITIES: 'required',
            FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY: MAINTENANCE_PUBLIC_KEY,
          },
          routeHostnames: [],
          artifactVersion: platformResources.stateWorker.artifactVersion,
          schemaVersion: current.schemaVersion,
        },
        {
          backend: 'plain-worker',
          resourceRole: 'deployment-egress',
          resourceGroupId: groupId,
          scriptName: egressProxy.scriptName,
          tenantTag: current.tenantTag,
          environment: current.environment,
          databaseIds: [],
          durableObjectBindings: [],
          secretNames: [],
          serviceBindings: [],
          kvNamespaceBindings: [
            { name: 'HOSTS', namespaceId: 'host-routing-kv' },
          ],
          plainTextBindings: {
            policyId: policy.policyId,
            routeHostname: current.routeHostname,
            hostRoutingKvId: 'host-routing-kv',
            scriptName: platformResources.stateWorker.scriptName,
          },
          routeHostnames: [],
          artifactVersion: egressProxy.artifactVersion,
          schemaVersion: 0,
        },
      ],
      databaseIds: [current.databaseId],
      namespaceIds: current.durableObjectBindings.map(
        (binding) => binding.namespaceId,
      ),
      routes: [
        {
          backend: current.backend,
          surface: 'host-registry',
          hostname: current.routeHostname,
          scriptName: current.scriptName,
          tenantTag: current.tenantTag,
          environment: current.environment,
          ...policy,
        },
      ],
    };
    const backend = new FleetBackend();
    backend.live.set(current.tenantTag, liveFor(current));

    const audit = (subject: FleetResourceInventory) =>
      auditFleetDrift({
        store: storeFor([current]),
        records: [current],
        inventory: subject,
        backendFor: () => backend,
        specFor: () => deployment,
        maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
        staleAfterMs: 1_000,
        now: 10_000,
      });
    const healthyFindings = await audit(inventory);
    expect(
      healthyFindings.filter(
        (finding) =>
          finding.kind === 'binding-drift' || finding.kind === 'version-drift',
      ),
    ).toEqual([]);
    const secretDrift: FleetResourceInventory = {
      ...inventory,
      deployments: inventory.deployments.map((resource) =>
        resource.resourceRole === 'deployment-egress'
          ? { ...resource, secretNames: ['LATENT_PROXY_SECRET'] }
          : resource,
      ),
    };
    await expect(audit(secretDrift)).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'binding-drift',
          detail: expect.stringContaining('trusted egress Worker'),
        }),
      ]),
    );
    const pairedKvDrift: FleetResourceInventory = {
      ...inventory,
      deployments: inventory.deployments.map((resource) =>
        resource.resourceRole === 'deployment-egress'
          ? {
              ...resource,
              kvNamespaceBindings: [
                { name: 'HOSTS', namespaceId: 'rogue-routing-kv' },
              ],
              plainTextBindings: {
                ...resource.plainTextBindings,
                hostRoutingKvId: 'rogue-routing-kv',
              },
            }
          : resource,
      ),
    };
    const findings = await audit(pairedKvDrift);

    expect(findings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: 'binding-drift',
          detail: expect.stringContaining('trusted egress Worker'),
        }),
      ]),
    );
  });

  it('does not expect a deleted plain Worker during platform-only teardown phases', async () => {
    const base: FleetRecord = {
      ...record('plain-teardown'),
      backend: 'plain-worker',
    };
    for (const phase of [
      'platform-credentials-revoked',
      'platform-resources-deleted',
    ] as const) {
      const current: FleetRecord = { ...base, phase };
      const findings = await auditFleetDrift({
        store: storeFor([current]),
        records: [current],
        inventory: {
          findings: [],
          scriptRegistrations: [],
          deployments: [],
          databaseIds: [current.databaseId],
          namespaceIds: [],
          routes: [],
        },
        backendFor: () => new FleetBackend('plain-worker'),
        specFor: () => ({ ...spec(current), authoredBy: 'platform' }),
        maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
        staleAfterMs: 1_000,
        now: 10_000,
      });

      expect(
        findings.filter((finding) =>
          ['missing-deployment', 'missing-namespace'].includes(finding.kind),
        ),
        phase,
      ).toEqual([]);
    }
  });

  it('uses effective decommission phases for fleet and R2 expectations', async () => {
    for (const phase of [
      'ready',
      'worker-deleted',
      'platform-resources-deleted',
      'application-resources-deleted',
      'database-exported',
    ] as const) {
      const legacy: FleetRecord = {
        ...record(`effective-${phase}`),
        phase,
        applicationResources: [
          {
            name: 'FILES',
            bucketName: `effective-${phase}-bucket`,
            jurisdiction: 'default',
            state: 'created',
            reservationNonce: 'a'.repeat(32),
            creationDate: '2026-08-11T00:00:00.000Z',
          },
        ],
      };
      const inventory = inventoryFor([legacy]);
      const audit = (current: FleetRecord) =>
        auditFleetDrift({
          store: storeFor([current]),
          records: [current],
          inventory,
          backendFor: () => new FleetBackend(current.backend),
          specFor: () => spec(legacy),
          maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
          staleAfterMs: 1_000,
          now: 10_000,
        });

      expect(
        await audit(decommissionAdvancingRecordFixture(legacy, phase)),
        phase,
      ).toEqual(await audit(legacy));
    }
  });

  it('preserves collection findings and applies publishing and database-deleting ownership semantics', async () => {
    const publishingBase = record('publishing');
    const publishing = {
      ...publishingBase,
      pendingRelease: publishingBase.activeRelease,
      phase: 'publishing' as const,
      updatedAt: '1970-01-01T00:00:10.000Z',
    };
    const databaseDeleting = {
      ...record('deleting'),
      phase: 'database-deleting' as const,
      updatedAt: '1970-01-01T00:00:10.000Z',
    };
    const inventory = inventoryFor([publishing]);
    const findings = await auditFleetDrift({
      store: storeFor([publishing, databaseDeleting]),
      records: [publishing, databaseDeleting],
      inventory: {
        ...inventory,
        findings: [
          {
            tenantTag: 'unknown',
            environment: 'unknown',
            kind: 'malformed-route',
            detail: "host route 'broken' is not valid JSON",
          },
        ],
        databaseIds: [...inventory.databaseIds, databaseDeleting.databaseId],
        namespaceIds: [
          ...inventory.namespaceIds,
          ...databaseDeleting.durableObjectBindings.map(
            (binding) => binding.namespaceId,
          ),
        ],
        routes: [
          ...inventory.routes,
          {
            backend: databaseDeleting.backend,
            hostname: databaseDeleting.routeHostname,
            scriptName: databaseDeleting.scriptName,
            tenantTag: databaseDeleting.tenantTag,
            environment: databaseDeleting.environment,
          },
        ],
      },
      backendFor: () => {
        throw new Error('non-ready deployments must not resolve a backend');
      },
      specFor: () => {
        throw new Error('non-ready deployments must not resolve a spec');
      },
      maintenanceSecretFor: () => {
        throw new Error('non-ready deployments must not resolve a secret');
      },
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(findings).toEqual([
      expect.objectContaining({ kind: 'malformed-route' }),
      expect.objectContaining({
        tenantTag: databaseDeleting.tenantTag,
        kind: 'orphan-route',
      }),
      expect.objectContaining({ kind: 'orphan-namespace' }),
    ]);
    expect(findings).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          tenantTag: publishing.tenantTag,
          kind: 'orphan-route',
        }),
      ]),
    );
  });

  it('contains resolver, inspection, and maintenance re-arm failures per deployment', async () => {
    const tenants = [
      'backend',
      'spec',
      'secret',
      'inspect',
      'rearm',
      'healthy',
    ];
    const records = tenants.map((tenant) => record(tenant));
    const backend = new FleetBackend();
    backend.inspectFailureTenant = 'inspect';
    backend.maintenanceFailureTenant = 'rearm';
    for (const item of records) {
      backend.live.set(
        item.tenantTag,
        liveFor(item, {
          maintenance:
            item.tenantTag === 'rearm'
              ? {
                  armed: false,
                  nextAlarmAt: null,
                  lastSweepAt: 9_500,
                  lastPurgeAt: 9_500,
                }
              : healthy,
        }),
      );
    }

    const findings = await auditFleetDrift({
      store: storeFor(records),
      records,
      inventory: inventoryFor(records),
      backendFor: (item) => {
        if (item.tenantTag === 'backend') throw new Error('backend failed');
        return backend;
      },
      specFor: (item) => {
        if (item.tenantTag === 'spec') throw new Error('spec failed');
        return spec(item);
      },
      maintenanceSecretFor: (item) => {
        if (item.tenantTag === 'secret') throw new Error('secret failed');
        return 'maintenance-admin-secret-value-00001';
      },
      staleAfterMs: 1_000,
      now: 10_000,
    });

    expect(
      findings
        .filter((finding) => finding.kind === 'audit-error')
        .map((finding) => finding.detail),
    ).toEqual([
      'backend resolver failed: Error: backend failed',
      'spec resolver failed: Error: spec failed',
      'maintenance secret resolver failed: Error: secret failed',
      'inspection failed: Error: inspect failed',
      'maintenance re-arm failed: Error: maintenance failed',
    ]);
    expect(findings).toContainEqual(
      expect.objectContaining({
        tenantTag: 'rearm',
        kind: 'maintenance-stale',
      }),
    );
    expect(backend.calls).toContain('inspect:healthy');
  });

  it('suppresses drift findings in both directions for a deployment under active bounded cleanup', async () => {
    const cleaningBase = record('acme');
    const [reserved] = reserveApplicationR2Resources({
      ...spec(cleaningBase, 1),
      application: { vars: [], secrets: [], r2Buckets: [{ name: 'DATA' }] },
    });
    if (!reserved) throw new Error('missing reserved application resource');
    const dataResource = {
      ...reserved,
      state: 'created' as const,
      creationDate: '2026-08-01T00:00:00.000Z',
    };
    const cleaning: FleetRecord = {
      ...cleaningBase,
      phase: 'cleanup-advancing',
      applicationResources: [dataResource],
    };
    const withIntent: FleetRecord = {
      ...cleaning,
      cleanupIntent: {
        version: 1,
        operationId: '5b8e2f1a-3c4d-4e5f-8a9b-0c1d2e3f4a5b',
        revision: 2,
        generation: 0,
        updatedAt: cleaning.updatedAt,
        authority: { kind: 'manual-cleanup' },
        identity: {
          record: {
            tenantTag: cleaning.tenantTag,
            environment: cleaning.environment,
            backend: cleaning.backend,
            scriptName: cleaning.scriptName,
            databaseId: cleaning.databaseId,
            databaseName: cleaning.databaseName,
            routeHostname: cleaning.routeHostname,
          },
          admittedPhase: 'worker-deployed',
          externalArtifact: false,
        },
        state: { step: 'teardown-worker' },
      },
    };
    const beta = record('beta');
    const backend = new FleetBackend();
    backend.live.set(beta.tenantTag, liveFor(beta));
    const audit = (inventory: FleetResourceInventory) =>
      auditFleetDrift({
        store: storeFor([withIntent, beta]),
        records: [withIntent, beta],
        inventory,
        backendFor: (item) => {
          if (item.cleanupIntent) {
            throw new Error('suppressed cleanup record must not be audited');
          }
          return backend;
        },
        specFor: (item) => spec(item),
        maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
        staleAfterMs: 1_000,
        now: 10_000,
      });

    // Still-present declared resources are known, never orphans.
    const present = inventoryFor([cleaningBase, beta]);
    await expect(
      audit({
        ...present,
        r2Buckets: [
          {
            bucketName: dataResource.bucketName,
            jurisdiction: dataResource.jurisdiction ?? 'default',
            creationDate: dataResource.creationDate,
          },
        ],
      }),
    ).resolves.toEqual([]);

    // Already-removed resources raise no expectation-based findings either:
    // the bounded engine, not the drift audit, reconciles this deployment.
    await expect(audit(inventoryFor([beta]))).resolves.toEqual([]);
  });

  it('reports no incomplete provisioning for a stale blocked cleanup record', async () => {
    const base = record('acme');
    const blocked: FleetRecord = {
      ...base,
      phase: 'cleanup-advancing',
      cleanupIntent: {
        version: 1,
        operationId: '5b8e2f1a-3c4d-4e5f-8a9b-0c1d2e3f4a5b',
        revision: 7,
        generation: 1,
        updatedAt: base.updatedAt,
        authority: { kind: 'manual-cleanup' },
        identity: {
          record: {
            tenantTag: base.tenantTag,
            environment: base.environment,
            backend: base.backend,
            scriptName: base.scriptName,
            databaseId: base.databaseId,
            databaseName: base.databaseName,
            routeHostname: base.routeHostname,
          },
          admittedPhase: 'worker-deployed',
          externalArtifact: false,
        },
        state: {
          step: 'blocked',
          purpose: {
            kind: 'cleanup-database-pre-delete',
            databaseId: base.databaseId,
            operationId: '5b8e2f1a-3c4d-4e5f-8a9b-0c1d2e3f4a5b',
          },
          attachment: { plane: 'ordinary', scriptName: 'holder-script' },
        },
      },
    };
    const backend = new FleetBackend();
    const audit = (records: readonly FleetRecord[]) =>
      auditFleetDrift({
        store: storeFor(records),
        records,
        inventory: inventoryFor([base]),
        backendFor: () => backend,
        specFor: (item) => spec(item),
        maintenanceSecretFor: () => 'maintenance-admin-secret-value-00001',
        staleAfterMs: 1_000,
        now: 10_000,
      });

    // A long-blocked cleanup stays visible through the record itself, never
    // through incomplete-provisioning or other drift findings.
    await expect(audit([blocked])).resolves.toEqual([]);

    const stale: FleetRecord = { ...base, phase: 'worker-deployed' };
    backend.live.set(stale.tenantTag, liveFor(stale));
    const findings = await audit([stale]);
    expect(findings.map(({ kind }) => kind)).toContain(
      'incomplete-provisioning',
    );
  });

  it('commits the invocation authority before migration staging, candidate maintenance, and promotion dispatches', async () => {
    class TimelineFleetStore extends FleetStore {
      constructor(private readonly timeline: string[]) {
        super();
      }

      override async put(value: FleetRecord): Promise<void> {
        await super.put(value);
        const carrier = value.invocationAuthority;
        this.timeline.push(
          `put:${value.phase}:${
            carrier
              ? carrier.authorizedAt === null
                ? 'null'
                : 'authorized'
              : 'absent'
          }`,
        );
      }
    }
    const acme = record('acme');
    const initialSpec = spec(acme, 1);
    const activePhysicalScriptName = externalReleaseScriptName(initialSpec);
    const backend = new ImmutableFleetBackend();
    const priorTarget = backend.describeExternalPlatformTarget(initialSpec);
    // A legacy record carries no invocation-authority carrier at all.
    const current: FleetRecord = {
      ...acme,
      durableObjectBindings: [],
      desiredSpecDigest: deploymentSpecDigest(initialSpec),
      platformTarget: priorTarget,
      outboundPolicy: priorTarget.outboundPolicy,
      activeRelease: {
        physicalScriptName: activePhysicalScriptName,
        specDigest: deploymentSpecDigest(initialSpec),
        artifactVersion: acme.artifactVersion,
        releaseSchemaVersion: initialSpec.schemaVersion,
      },
    };
    const target = spec(current, 2);
    backend.routedScriptName = activePhysicalScriptName;
    backend.releases.set(activePhysicalScriptName, {
      ...liveFor(current),
      scriptName: activePhysicalScriptName,
      durableObjectBindings: [],
    });
    const store = new TimelineFleetStore(backend.calls);
    await store.put(current);
    backend.calls.length = 0;

    await migrateFleet({
      store,
      records: [current],
      canaryTenantTags: [],
      backendFor: () => backend,
      specFor: () => target,
      secretsFor: () => ({
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      }),
    });

    const timeline = backend.calls;
    const flip = timeline.indexOf('put:migrating:authorized');
    const deploy = timeline.indexOf(
      `deploy:acme:${externalReleaseScriptName(target)}`,
    );
    const maintenance = timeline.indexOf('maintenance:acme');
    const promote = timeline.indexOf(
      `promote:acme:${externalReleaseScriptName(target)}`,
    );
    // Staging puts never carry the flip; a dedicated durable put commits the
    // carrier before the candidate upload, and maintenance plus promotion
    // dispatch only after that same committed authority.
    expect(timeline.slice(0, flip)).toContain('put:migrating:absent');
    expect(flip).toBeGreaterThanOrEqual(0);
    expect(flip).toBeLessThan(deploy);
    expect(deploy).toBeLessThan(maintenance);
    expect(maintenance).toBeLessThan(promote);
    const migrated = await store.get('acme', 'production');
    expect(migrated?.phase).toBe('ready');
    expect(typeof migrated?.invocationAuthority?.authorizedAt).toBe('string');
  });

  it('migrates explicit canaries first and stops before the remaining fleet', async () => {
    const acme = record('acme');
    const beta = record('beta');
    const gamma = record('gamma');
    const backend = new FleetBackend();
    backend.failTenant = 'beta';
    const store = new FleetStore();
    for (const item of [gamma, beta, acme]) await store.put(item);

    await expect(
      migrateFleet({
        store,
        records: [gamma, beta, acme],
        canaryTenantTags: ['acme', 'beta'],
        backendFor: () => backend,
        specFor: (item) => spec(item),
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/canary failed/);

    expect(backend.calls.filter((call) => call.startsWith('deploy:'))).toEqual([
      'deploy:acme',
      'deploy:beta',
    ]);
    expect(backend.calls).not.toContain('deploy:gamma');
  });

  it.each([
    {
      label: 'maintenance verifier rotation',
      mutate: (backend: FleetBackend) => {
        backend.platformMaintenanceCapabilityPublicKey =
          ROTATED_MAINTENANCE_PUBLIC_KEY;
      },
      error: /maintenance capability verifier is immutable/,
    },
    {
      label: 'audit queue retargeting',
      mutate: (backend: FleetBackend) => {
        backend.platformAuditQueueName = 'audit-retargeted';
      },
      error: /audit queue is immutable/,
    },
  ])('rejects $label before persisting a migration intent', async ({
    mutate,
    error,
  }) => {
    const base = record('acme');
    const targetSpec = spec(base, 1);
    const backend = new ImmutableFleetBackend();
    backend.platformAuditQueueName = 'audit-primary';
    const currentTarget = backend.describeExternalPlatformTarget(targetSpec);
    const activeRelease = {
      physicalScriptName: externalReleaseScriptName(targetSpec),
      specDigest: deploymentSpecDigest(targetSpec),
      artifactVersion: base.artifactVersion,
      releaseSchemaVersion: targetSpec.schemaVersion,
    };
    const current: FleetRecord = {
      ...base,
      desiredSpecDigest: activeRelease.specDigest,
      activeRelease,
      platformTarget: currentTarget,
      outboundPolicy: currentTarget.outboundPolicy,
      platformResources: {
        auditQueueName: 'audit-primary',
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: externalStateScriptName(targetSpec),
          artifactVersion: 'state-v1',
          artifactDigest: currentTarget.stateArtifactDigest,
          durableObjectBindings: base.durableObjectBindings,
          namespaceIds: base.durableObjectBindings.map(
            (binding) => binding.namespaceId,
          ),
        },
        egressProxy: {
          scriptName: externalEgressProxyScriptName(targetSpec),
          artifactVersion: 'egress-v1',
          artifactDigest: currentTarget.egressArtifactDigest,
          ...currentTarget.outboundPolicy,
        },
      },
    };
    const store = new FleetStore();
    await store.put(current);
    mutate(backend);

    await expect(
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => targetSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(error);
    expect(await store.get('acme', 'production')).toEqual(current);
    expect(backend.calls).toEqual([]);
  });

  it('durably converges a platform-only profile change without replacing the active release', async () => {
    const base = record('acme');
    const targetSpec = spec(base, 1);
    const backend = new ImmutableFleetBackend();
    const initialTarget = {
      ...backend.describeExternalPlatformTarget(targetSpec),
      d1SchemaVersion: 2,
      d1SchemaHistoryDigest: 'f'.repeat(64),
    };
    const activeRelease = {
      physicalScriptName: externalReleaseScriptName(targetSpec),
      specDigest: deploymentSpecDigest(targetSpec),
      artifactVersion: 'etag:active',
      releaseSchemaVersion: 1,
    };
    const current: FleetRecord = {
      ...base,
      schemaVersion: 2,
      desiredSpecDigest: activeRelease.specDigest,
      artifactVersion: activeRelease.artifactVersion,
      activeRelease,
      durableObjectBindings: [],
      platformTarget: initialTarget,
      outboundPolicy: initialTarget.outboundPolicy,
      platformResources: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: externalStateScriptName(targetSpec),
          artifactVersion: 'state-v1',
          artifactDigest: initialTarget.stateArtifactDigest,
          durableObjectBindings: [],
          namespaceIds: [],
        },
        egressProxy: {
          scriptName: externalEgressProxyScriptName(targetSpec),
          artifactVersion: 'egress-v1',
          artifactDigest: initialTarget.egressArtifactDigest,
          ...initialTarget.outboundPolicy,
        },
      },
    };
    backend.releases.set(activeRelease.physicalScriptName, {
      ...liveFor(current),
      scriptName: activeRelease.physicalScriptName,
      durableObjectBindings: [],
      schemaVersion: activeRelease.releaseSchemaVersion,
      maintenance: { ...healthy, armed: false },
    });
    backend.routedScriptName = activeRelease.physicalScriptName;
    backend.platformStateDigest = 'd'.repeat(64);
    backend.platformPolicyHosts = ['narrow.example.com'];
    const targetPlatform = {
      ...backend.describeExternalPlatformTarget(targetSpec),
      d1SchemaVersion: initialTarget.d1SchemaVersion,
      d1SchemaHistoryDigest: initialTarget.d1SchemaHistoryDigest,
    };
    const store = new FleetStore();
    await store.put(current);
    store.failNextPutWhen = (next, previous) =>
      next.phase === 'migrating' &&
      next.platformTarget?.stateArtifactDigest ===
        targetPlatform.stateArtifactDigest &&
      previous?.platformTarget?.stateArtifactDigest ===
        initialTarget.stateArtifactDigest
        ? 'state write failed after platform convergence'
        : undefined;
    const migrate = () =>
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => targetSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      });

    await expect(migrate()).rejects.toThrow(
      /state write failed after platform convergence/,
    );
    expect(await store.get('acme', 'production')).toMatchObject({
      phase: 'migrating',
      activeRelease,
      migrationIntent: {
        platformOnly: true,
        subphase: 'schema-applied',
        target: targetPlatform,
      },
    });

    store.failNextPutWhen = (next) =>
      next.migrationIntent?.subphase === 'route-published'
        ? 'state write failed after platform route publication'
        : undefined;
    await expect(migrate()).rejects.toThrow(
      /state write failed after platform route publication/,
    );
    expect(await store.get('acme', 'production')).toMatchObject({
      phase: 'migrating',
      activeRelease,
      migrationIntent: {
        platformOnly: true,
        subphase: 'platform-applied',
        target: targetPlatform,
      },
    });

    const [settled] = await migrate();
    expect(settled).toMatchObject({
      phase: 'ready',
      activeRelease,
      platformTarget: targetPlatform,
      outboundPolicy: targetPlatform.outboundPolicy,
    });
    expect(settled?.pendingRelease).toBeUndefined();
    expect(settled?.rollbackRelease).toBeUndefined();
    expect(backend.calls).not.toEqual(
      expect.arrayContaining([expect.stringMatching(/^deploy:/u)]),
    );
    expect(backend.calls).toContain('maintenance:acme');
    expect(backend.lastPromotedPolicyHosts).toEqual(['narrow.example.com']);
  });

  it('persists the advanced Durable Object tag and rejects a stale migration base', async () => {
    const priorHistory = [{ tag: 'v1', newClasses: ['Runner'] }];
    const acme = {
      ...record('acme'),
      durableObjectTag: 'v1',
      durableObjectMigrationHistory: priorHistory,
      durableObjectMigrationHistoryDigest:
        durableObjectMigrationHistoryDigest(priorHistory),
    };
    const backend = new FleetBackend();
    const store = new FleetStore();
    await store.put(acme);
    const target = {
      ...spec(acme),
      authoredBy: 'platform' as const,
      previousDurableObjectTag: 'v1',
      durableObjectMigrations: [
        { tag: 'v1', newClasses: ['Runner'] },
        { tag: 'v2', newSqliteClasses: ['Maintenance'] },
      ],
    };

    await expect(
      migrateFleet({
        store,
        records: [acme],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => ({
          ...target,
          durableObjectMigrations: [
            { tag: 'v1', newClasses: ['RewrittenRunner'] },
            { tag: 'v2', newSqliteClasses: ['Maintenance'] },
          ],
        }),
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/not an exact append-only extension/);
    expect(backend.calls).toEqual([]);

    const [updated] = await migrateFleet({
      store,
      records: [acme],
      canaryTenantTags: [],
      backendFor: () => backend,
      specFor: () => target,
      secretsFor: () => ({
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      }),
    });
    expect(updated?.durableObjectTag).toBe('v2');

    await expect(
      migrateFleet({
        store,
        records: [acme],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => target,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/migration base mismatch/);
  });

  it('keeps the old route live when an immutable candidate fails exact validation', async () => {
    const acme = record('acme');
    const initialSpec = spec(acme, 1);
    const activePhysicalScriptName = externalReleaseScriptName(initialSpec);
    const backend = new ImmutableFleetBackend();
    const priorTarget = backend.describeExternalPlatformTarget(initialSpec);
    const current = {
      ...acme,
      desiredSpecDigest: deploymentSpecDigest(initialSpec),
      platformTarget: priorTarget,
      outboundPolicy: priorTarget.outboundPolicy,
      activeRelease: {
        physicalScriptName: activePhysicalScriptName,
        specDigest: deploymentSpecDigest(initialSpec),
        artifactVersion: acme.artifactVersion,
        releaseSchemaVersion: initialSpec.schemaVersion,
      },
    };
    backend.routedScriptName = activePhysicalScriptName;
    backend.releases.set(
      activePhysicalScriptName,
      liveFor(current, {
        scriptName: activePhysicalScriptName,
        desiredSpecDigest: current.desiredSpecDigest,
        durableObjectBindings: [],
      }),
    );
    backend.invalidateCandidate = true;
    const store = new FleetStore();
    await store.put(current);

    await expect(
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => spec(current, 2),
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/exactly match/);
    expect(backend.routedScriptName).toBe(activePhysicalScriptName);
    expect(backend.calls.some((call) => call.startsWith('promote:'))).toBe(
      false,
    );
    expect((await store.get('acme', 'production'))?.phase).toBe('migrating');

    const target = spec(current, 2);
    backend.invalidateCandidate = false;
    backend.releases.delete(externalReleaseScriptName(target));
    await expect(
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => target,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).resolves.toMatchObject([{ phase: 'ready' }]);
  });

  it('rejects an out-of-band overwrite of a persisted active immutable release before mutation', async () => {
    const acme = record('acme');
    const currentSpec = spec(acme, 1);
    const physicalScriptName = externalReleaseScriptName(currentSpec);
    const backend = new ImmutableFleetBackend();
    const target = backend.describeExternalPlatformTarget(currentSpec);
    const current: FleetRecord = {
      ...acme,
      desiredSpecDigest: deploymentSpecDigest(currentSpec),
      platformTarget: target,
      outboundPolicy: target.outboundPolicy,
      activeRelease: {
        physicalScriptName,
        specDigest: deploymentSpecDigest(currentSpec),
        artifactVersion: acme.artifactVersion,
        releaseSchemaVersion: currentSpec.schemaVersion,
      },
    };
    backend.routedScriptName = physicalScriptName;
    backend.releases.set(physicalScriptName, {
      ...liveFor(current),
      scriptName: physicalScriptName,
      artifactVersion: 'out-of-band-overwrite',
      durableObjectBindings: [],
    });
    const store = new FleetStore();
    await store.put(current);

    await expect(
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => currentSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/does not match persisted artifact version/);
    expect(backend.calls).toEqual(['platform:acme', 'inspect:acme']);
  });

  it('reconciles an uploaded immutable candidate before maintenance-aware inspection on retry', async () => {
    const acme = record('acme');
    const initialSpec = spec(acme, 1);
    const activePhysicalScriptName = externalReleaseScriptName(initialSpec);
    const backend = new DispatchSecretRecoveryBackend();
    const priorTarget = backend.describeExternalPlatformTarget(initialSpec);
    const current: FleetRecord = {
      ...acme,
      durableObjectBindings: [],
      desiredSpecDigest: deploymentSpecDigest(initialSpec),
      platformTarget: priorTarget,
      outboundPolicy: priorTarget.outboundPolicy,
      activeRelease: {
        physicalScriptName: activePhysicalScriptName,
        specDigest: deploymentSpecDigest(initialSpec),
        artifactVersion: acme.artifactVersion,
        releaseSchemaVersion: initialSpec.schemaVersion,
      },
    };
    const target = spec(current, 2);
    backend.routedScriptName = activePhysicalScriptName;
    backend.releases.set(activePhysicalScriptName, {
      ...liveFor(current),
      scriptName: activePhysicalScriptName,
      durableObjectBindings: [],
    });
    const store = new FleetStore();
    await store.put(current);
    const migrate = () =>
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => target,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      });

    await expect(migrate()).rejects.toThrow(
      /dispatch secret write failed after provider upload/,
    );
    expect(await store.get('acme', 'production')).toMatchObject({
      phase: 'migrating',
      pendingRelease: { artifactVersion: 'pending' },
      migrationIntent: {
        subphase: 'platform-applied',
        targetRelease: { artifactVersion: 'pending' },
      },
    });

    const retryCallsStart = backend.calls.length;
    await expect(migrate()).resolves.toMatchObject([
      {
        phase: 'ready',
        activeRelease: {
          physicalScriptName: externalReleaseScriptName(target),
          artifactVersion: `etag:${externalReleaseScriptName(target)}`,
        },
      },
    ]);
    const retryCalls = backend.calls.slice(retryCallsStart);
    expect(
      retryCalls.indexOf(`deploy:acme:${externalReleaseScriptName(target)}`),
    ).toBeLessThan(retryCalls.indexOf('inspect:acme'));
    expect(backend.incompleteCandidates).toEqual(new Set());
  });

  it('rejects an external schema change without rollback-compatibility attestation before mutation', async () => {
    const acme = record('acme');
    const initialSpec = spec(acme, 1);
    const current: FleetRecord = {
      ...acme,
      durableObjectBindings: [],
      desiredSpecDigest: deploymentSpecDigest(initialSpec),
      activeRelease: {
        physicalScriptName: externalReleaseScriptName(initialSpec),
        specDigest: deploymentSpecDigest(initialSpec),
        artifactVersion: acme.artifactVersion,
        releaseSchemaVersion: initialSpec.schemaVersion,
      },
    };
    const target = spec(current, 2);
    const unsafeTarget = {
      ...target,
      migrations: target.migrations.map(
        ({ rollbackCompatible: _, ...migration }) => migration,
      ),
    };
    const backend = new ImmutableFleetBackend();
    const store = new FleetStore();
    await store.put(current);

    await expect(
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => unsafeTarget,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/rollbackCompatible/);
    expect(await store.get('acme', 'production')).toEqual(current);
    expect(backend.calls).toEqual([]);
  });

  it('rejects an ordinary staged Worker schema change without rollback compatibility before the old live version can diverge', async () => {
    const currentSpec = {
      ...spec(record('plain'), 1),
      authoredBy: 'platform' as const,
    };
    const current: FleetRecord = {
      ...record('plain'),
      backend: 'plain-worker',
      desiredSpecDigest: deploymentSpecDigest(currentSpec),
    };
    const target = {
      ...spec(current, 2),
      authoredBy: 'platform' as const,
      migrations: spec(current, 2).migrations.map(
        ({ rollbackCompatible: _, ...migration }) => migration,
      ),
    };
    const backend = new FleetBackend('plain-worker');
    const store = new FleetStore();
    await store.put(current);

    await expect(
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => target,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/rollbackCompatible/);
    expect(await store.get('plain', 'production')).toEqual(current);
    expect(backend.calls).toEqual([]);
  });

  it('recovers route flips and keeps a narrowed deployment policy across release rollback', async () => {
    const acme = record('acme');
    const initialSecret = 'initial-application-secret';
    const targetSecret = 'target-application-secret';
    const initialSpec: DeploymentSpec = {
      ...spec(acme, 1),
      application: {
        vars: [{ name: 'RELEASE_VALUE', value: 'v1' }],
        secrets: [
          {
            name: 'API_ONE',
            valueSha256: createHash('sha256')
              .update(initialSecret)
              .digest('hex'),
          },
        ],
        r2Buckets: [],
      },
    };
    const initialBindings = initialSpec.application;
    if (!initialBindings) throw new Error('initial application is missing');
    const initialApplication = {
      vars: initialBindings.vars,
      secrets: initialBindings.secrets,
      r2Buckets: [],
    };
    const broadPolicy = canonicalDeploymentEgressPolicy({
      policyId: externalPlatformResourceGroupId(initialSpec),
      tenantTag: initialSpec.tenantTag,
      environment: initialSpec.environment,
      allowedHosts: ['api.example.com', 'legacy.example.com'],
    });
    const activePhysicalScriptName = externalReleaseScriptName(initialSpec);
    const backend = new ImmutableFleetBackend();
    const priorTarget = {
      ...backend.describeExternalPlatformTarget(initialSpec),
      outboundPolicy: broadPolicy,
    };
    const current: FleetRecord = {
      ...acme,
      durableObjectBindings: [],
      desiredSpecDigest: deploymentSpecDigest(initialSpec),
      applicationBindings: initialApplication,
      platformTarget: priorTarget,
      outboundPolicy: broadPolicy,
      activeRelease: {
        physicalScriptName: activePhysicalScriptName,
        specDigest: deploymentSpecDigest(initialSpec),
        artifactVersion: acme.artifactVersion,
        releaseSchemaVersion: initialSpec.schemaVersion,
        application: initialApplication,
      },
      platformResources: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: externalStateScriptName(initialSpec),
          artifactVersion: 'state-v1',
          artifactDigest: 'a'.repeat(64),
          durableObjectBindings: [],
          namespaceIds: [],
        },
        egressProxy: {
          scriptName: externalEgressProxyScriptName(initialSpec),
          artifactVersion: 'egress-v1',
          artifactDigest: 'b'.repeat(64),
          ...broadPolicy,
        },
      },
    };
    const targetSpec: DeploymentSpec = {
      ...spec(current, 2),
      application: {
        vars: [{ name: 'RELEASE_VALUE', value: 'v2' }],
        secrets: [
          {
            name: 'API_TWO',
            valueSha256: createHash('sha256')
              .update(targetSecret)
              .digest('hex'),
          },
        ],
        r2Buckets: [],
      },
    };
    const targetPhysicalScriptName = externalReleaseScriptName(targetSpec);
    backend.platformPolicyHosts = ['api.example.com'];
    backend.routedScriptName = activePhysicalScriptName;
    backend.releases.set(activePhysicalScriptName, {
      ...liveFor(current),
      scriptName: activePhysicalScriptName,
      durableObjectBindings: [],
      plainTextBindings: { RELEASE_VALUE: 'v1' },
      r2BucketBindings: [],
      secretNames: ['API_ONE', 'DEPLOYMENT_IDENTITY_SECRET'],
    });
    const store = new FleetStore();
    await store.put(current);
    const migrate = () =>
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => targetSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
          application: { API_TWO: targetSecret },
        }),
      });

    store.failNextMigratingSchemaPut = true;
    await expect(migrate()).rejects.toThrow(/after D1 migration/);
    expect(backend.routedScriptName).toBe(activePhysicalScriptName);
    expect(await store.get('acme', 'production')).toMatchObject({
      phase: 'migrating',
      schemaVersion: 1,
      migrationIntent: { subphase: 'planned' },
      pendingRelease: {
        application: {
          vars: [{ name: 'RELEASE_VALUE', value: 'v2' }],
          secrets: [{ name: 'API_TWO' }],
        },
      },
      migrationPriorRelease: {
        physicalScriptName: activePhysicalScriptName,
        releaseSchemaVersion: 1,
        application: initialApplication,
      },
    });

    store.failNextPutWhen = (next, current) =>
      next.migrationIntent?.subphase === 'schema-applied' &&
      next.platformResources?.egressProxy?.policyDigest !==
        current?.platformResources?.egressProxy?.policyDigest
        ? 'state write failed after platform mutation'
        : undefined;
    await expect(migrate()).rejects.toThrow(/after platform mutation/);
    expect(backend.routedScriptName).toBe(activePhysicalScriptName);
    expect(await store.get('acme', 'production')).toMatchObject({
      phase: 'migrating',
      schemaVersion: 2,
      migrationIntent: { subphase: 'schema-applied' },
    });

    store.failNextPutWhen = (next) =>
      next.migrationIntent?.subphase === 'candidate-deployed'
        ? 'state write failed after candidate deployment'
        : undefined;
    await expect(migrate()).rejects.toThrow(/after candidate deployment/);
    expect(backend.routedScriptName).toBe(activePhysicalScriptName);
    expect(await store.get('acme', 'production')).toMatchObject({
      migrationIntent: { subphase: 'platform-applied' },
    });

    store.failNextPutWhen = (next) =>
      next.migrationIntent?.subphase === 'candidate-armed'
        ? 'state write failed after candidate maintenance'
        : undefined;
    await expect(migrate()).rejects.toThrow(/after candidate maintenance/);
    expect(backend.routedScriptName).toBe(activePhysicalScriptName);
    expect(await store.get('acme', 'production')).toMatchObject({
      migrationIntent: { subphase: 'candidate-deployed' },
    });
    const persistedCandidate = backend.releases.get(targetPhysicalScriptName);
    if (!persistedCandidate) throw new Error('candidate was not persisted');
    backend.releases.set(targetPhysicalScriptName, {
      ...persistedCandidate,
      artifactVersion: 'out-of-band-pending-overwrite',
    });
    await expect(migrate()).rejects.toThrow(
      /does not match persisted artifact version/,
    );
    backend.releases.set(targetPhysicalScriptName, persistedCandidate);

    store.failNextPutWhen = (next) =>
      next.migrationIntent?.subphase === 'route-published'
        ? 'state write failed after route publication'
        : undefined;
    await expect(migrate()).rejects.toThrow(/after route publication/);
    expect(backend.routedScriptName).toBe(targetPhysicalScriptName);
    expect(await store.get('acme', 'production')).toMatchObject({
      migrationIntent: { subphase: 'candidate-armed' },
    });

    store.failNextReadyPut = true;
    await expect(migrate()).rejects.toThrow(/state write failed/);
    expect(backend.routedScriptName).toBe(targetPhysicalScriptName);
    expect((await store.get('acme', 'production'))?.phase).toBe('migrating');

    const [migrated] = await migrate();
    expect(migrated).toMatchObject({
      phase: 'ready',
      schemaVersion: 2,
      activeRelease: {
        physicalScriptName: targetPhysicalScriptName,
        releaseSchemaVersion: 2,
        application: {
          vars: [{ name: 'RELEASE_VALUE', value: 'v2' }],
          secrets: [{ name: 'API_TWO' }],
        },
      },
      rollbackRelease: {
        physicalScriptName: activePhysicalScriptName,
        specDigest: deploymentSpecDigest(initialSpec),
        releaseSchemaVersion: 1,
      },
      platformResources: {
        egressProxy: { policyHosts: ['api.example.com'] },
      },
    });

    const rollbackHost = new RecordingSettlementHost(store);
    const rollback = () =>
      rollbackExternalRelease({
        store,
        backend,
        currentSpec: targetSpec,
        rollbackSpec: initialSpec,
        secrets: {
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
          application: { API_ONE: initialSecret },
        },
        settlement: rollbackHost,
      });
    const persistedRollback = backend.releases.get(activePhysicalScriptName);
    if (!persistedRollback)
      throw new Error('rollback release was not retained');
    backend.releases.set(activePhysicalScriptName, {
      ...persistedRollback,
      artifactVersion: 'out-of-band-rollback-overwrite',
    });
    await expect(rollback()).rejects.toThrow(
      /does not match persisted artifact version/,
    );
    backend.releases.set(activePhysicalScriptName, persistedRollback);
    store.failNextReadyPut = true;
    await expect(rollback()).rejects.toThrow(/state write failed/);
    expect(backend.routedScriptName).toBe(activePhysicalScriptName);
    expect((await store.get('acme', 'production'))?.phase).toBe('rolling-back');
    // The settle-succeeded-then-crashed window: traffic moved back and the
    // host was told, but the write recording it was lost.
    expect(rollbackHost.settlements).toHaveLength(1);
    expect(
      (await store.get('acme', 'production'))?.settledSettlementKey,
    ).toBeUndefined();
    const rolledBack = await rollback();
    // #then the re-entry re-delivers the SAME settlement, and still names the
    // abandoned release as `prior` rather than the one it replaced.
    expect(rollbackHost.settlements).toHaveLength(2);
    expect(rollbackHost.settlements[1]?.settlementKey).toBe(
      rollbackHost.settlements[0]?.settlementKey,
    );
    expect(
      rollbackHost.settlements.map((context) => context.alreadySettled),
    ).toEqual([false, false]);
    expect(
      rollbackHost.settlements.map(
        (context) => context.prior?.physicalScriptName,
      ),
    ).toEqual([targetPhysicalScriptName, targetPhysicalScriptName]);
    expect(backend.routedScriptName).toBe(activePhysicalScriptName);
    expect(backend.lastPromotedPolicyHosts).toEqual(['api.example.com']);
    expect(rolledBack).toMatchObject({
      phase: 'ready',
      schemaVersion: 2,
      activeRelease: {
        physicalScriptName: activePhysicalScriptName,
        releaseSchemaVersion: 1,
      },
      desiredSpecDigest: deploymentSpecDigest(initialSpec),
      applicationBindings: initialApplication,
      rollbackRelease: {
        physicalScriptName: targetPhysicalScriptName,
        specDigest: deploymentSpecDigest(targetSpec),
        releaseSchemaVersion: 2,
      },
    });
    // `prior` on a reversal is the release traffic just LEFT, not the one it
    // replaced: a host undoing its own effects has to undo the right ones.
    expect(rollbackHost.settlements.at(-1)).toMatchObject({
      entry: 'rollback',
      target: { physicalScriptName: activePhysicalScriptName },
      prior: {
        physicalScriptName: targetPhysicalScriptName,
        specDigest: deploymentSpecDigest(targetSpec),
      },
    });
    expect(rollbackHost.leaseHeld.at(-1)).toBe(true);
    expect(rolledBack.settledSettlementKey).toBe(
      rollbackHost.settlements.at(-1)?.settlementKey,
    );

    const deployCallsBeforeConvergence = backend.calls.filter((call) =>
      call.startsWith('deploy:'),
    ).length;
    await expect(
      migrateFleet({
        store,
        records: [rolledBack],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => initialSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
          application: { API_ONE: initialSecret },
        }),
      }),
    ).resolves.toMatchObject([
      {
        phase: 'ready',
        schemaVersion: 2,
        activeRelease: { releaseSchemaVersion: 1 },
      },
    ]);
    expect(
      backend.calls.filter((call) => call.startsWith('deploy:')).length,
    ).toBe(deployCallsBeforeConvergence);

    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: initialSpec,
        secrets: {
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
          application: { API_ONE: initialSecret },
        },
      }),
    ).resolves.toMatchObject({
      record: {
        phase: 'ready',
        schemaVersion: 2,
        activeRelease: { releaseSchemaVersion: 1 },
      },
    });

    const rolledForward = await rollbackExternalRelease({
      store,
      backend,
      currentSpec: initialSpec,
      rollbackSpec: targetSpec,
      secrets: {
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        application: { API_TWO: targetSecret },
      },
    });
    expect(rolledForward).toMatchObject({
      phase: 'ready',
      schemaVersion: 2,
      activeRelease: {
        physicalScriptName: targetPhysicalScriptName,
        releaseSchemaVersion: 2,
        application: {
          vars: [{ name: 'RELEASE_VALUE', value: 'v2' }],
          secrets: [{ name: 'API_TWO' }],
        },
      },
      rollbackRelease: {
        physicalScriptName: activePhysicalScriptName,
        releaseSchemaVersion: 1,
      },
    });

    const thirdSpec = spec(rolledBack, 3);
    const thirdPhysicalScriptName = externalReleaseScriptName(thirdSpec);
    backend.failNextRetirement = true;
    await expect(
      migrateFleet({
        store,
        records: [rolledBack],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => thirdSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/retirement failed/);
    const awaitingRetirement = await store.get('acme', 'production');
    expect(awaitingRetirement).toMatchObject({
      phase: 'ready',
      schemaVersion: 3,
      activeRelease: {
        physicalScriptName: thirdPhysicalScriptName,
        releaseSchemaVersion: 3,
      },
      rollbackRelease: {
        physicalScriptName: targetPhysicalScriptName,
        releaseSchemaVersion: 2,
      },
      retiringRelease: {
        physicalScriptName: activePhysicalScriptName,
        releaseSchemaVersion: 1,
      },
    });
    if (!awaitingRetirement) throw new Error('missing retirement state');
    const fourthSpec: DeploymentSpec = {
      ...spec(awaitingRetirement, 3),
      modules: [
        { name: 'worker.js', content: 'export default { release: 4 }' },
      ],
    };
    const fourthPhysicalScriptName = externalReleaseScriptName(fourthSpec);
    const fourthDeployCall = `deploy:acme:${fourthPhysicalScriptName}`;
    backend.failNextRetirement = true;
    await expect(
      migrateFleet({
        store,
        records: [awaitingRetirement],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => fourthSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      }),
    ).rejects.toThrow(/retirement failed/);
    expect(backend.calls).not.toContain(fourthDeployCall);
    expect(await store.get('acme', 'production')).toMatchObject({
      phase: 'ready',
      activeRelease: { physicalScriptName: thirdPhysicalScriptName },
      retiringRelease: { physicalScriptName: activePhysicalScriptName },
    });

    const [retired] = await migrateFleet({
      store,
      records: [awaitingRetirement],
      canaryTenantTags: [],
      backendFor: () => backend,
      specFor: () => fourthSpec,
      secretsFor: () => ({
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      }),
    });
    expect(retired).toMatchObject({
      phase: 'ready',
      schemaVersion: 3,
      activeRelease: { physicalScriptName: fourthPhysicalScriptName },
      rollbackRelease: { physicalScriptName: thirdPhysicalScriptName },
    });
    expect(retired?.retiringRelease).toBeUndefined();
    expect(backend.retiredScriptNames).toEqual([
      activePhysicalScriptName,
      targetPhysicalScriptName,
    ]);
  });

  it('migrates and rolls back candidate-only releases while retaining finalized ordinary state', async () => {
    const acme = record('acme');
    const initialSpec = spec(acme, 1);
    const targetSpec: DeploymentSpec = {
      ...initialSpec,
      modules: [
        { name: 'worker.js', content: 'export default { release: 2 }' },
      ],
    };
    const backend = new ImmutableFleetBackend();
    const stateHistory: readonly import('../src/types.js').DurableObjectMigration[] =
      [];
    const platformTarget = {
      ...backend.describeExternalPlatformTarget(initialSpec),
      stateDurableObjectHistoryDigest:
        durableObjectMigrationHistoryDigest(stateHistory),
    };
    const activePhysicalScriptName = externalReleaseScriptName(initialSpec);
    const activeArtifactVersion = `etag:${activePhysicalScriptName}`;
    const bridge: BridgeSnapshot = {
      scriptName: acme.scriptName,
      artifactVersion: 'state-v1',
      artifactDigest: platformTarget.stateArtifactDigest,
      databaseId: acme.databaseId,
      durableObjectBindings: [],
      namespaceIds: ['namespace-retained'],
      secretNames: [
        'DEPLOYMENT_IDENTITY_SECRET',
        'MAINTENANCE_ADMIN_SECRET',
        'OUTBOUND_PROXY_CREDENTIAL',
      ],
      publicRouteAttached: false,
      stateOnly: true,
    };
    const current: FleetRecord = {
      ...acme,
      artifactVersion: activeArtifactVersion,
      desiredSpecDigest: deploymentSpecDigest(initialSpec),
      durableObjectBindings: [],
      durableObjectMigrationHistory: stateHistory,
      durableObjectMigrationHistoryDigest:
        durableObjectMigrationHistoryDigest(stateHistory),
      activeRelease: {
        physicalScriptName: activePhysicalScriptName,
        specDigest: deploymentSpecDigest(initialSpec),
        artifactVersion: activeArtifactVersion,
        releaseSchemaVersion: initialSpec.schemaVersion,
        application: { vars: [], secrets: [], r2Buckets: [] },
      },
      platformTarget,
      outboundPolicy: platformTarget.outboundPolicy,
      platformResources: {
        maintenanceCapabilityPublicKey:
          platformTarget.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: bridge.scriptName,
          artifactVersion: bridge.artifactVersion,
          artifactDigest: bridge.artifactDigest,
          plane: 'ordinary',
          durableObjectBindings: bridge.durableObjectBindings,
          namespaceIds: bridge.namespaceIds,
        },
        egressProxy: {
          scriptName: externalEgressProxyScriptName(initialSpec),
          artifactVersion: 'egress-v1',
          artifactDigest: 'b'.repeat(64),
          ...platformTarget.outboundPolicy,
        },
      },
      backendSwitchIntent: {
        kind: 'backend-switch',
        tenantTag: acme.tenantTag,
        environment: acme.environment,
        prior: {
          scriptName: acme.scriptName,
          artifactVersion: 'plain-v1',
          specDigest: deploymentSpecDigest(initialSpec),
          databaseId: acme.databaseId,
          databaseName: acme.databaseName,
          durableObjectBindings: [],
          namespaceIds: bridge.namespaceIds,
          secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
          applicationResources: [],
          customDomain: { id: 'domain-acme', hostname: acme.routeHostname },
        },
        targetSpecDigest: deploymentSpecDigest(initialSpec),
        targetApplication: { vars: [], secrets: [], r2Buckets: [] },
        target: platformTarget,
        rollbackUntil: '2026-08-20T00:00:00.000Z',
        subphase: 'finalized',
        bridge,
      },
      applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
      applicationResources: [],
    };
    backend.routedScriptName = activePhysicalScriptName;
    backend.releases.set(activePhysicalScriptName, {
      ...liveFor(current),
      scriptName: activePhysicalScriptName,
      artifactVersion: activeArtifactVersion,
      desiredSpecDigest: deploymentSpecDigest(initialSpec),
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
    });
    const plan: BridgeMutationPlan = {
      artifactDigest: bridge.artifactDigest,
      durableObjectMigrations: stateHistory,
      secretNames: bridge.secretNames,
      mutationDigest: 'f'.repeat(64),
    };
    const appendedHistory = [
      { tag: 'state-v2', newClasses: ['StateV2'] },
    ] as const;
    const appendedTarget = {
      ...platformTarget,
      stateArtifactDigest: '1'.repeat(64),
      stateDurableObjectHistoryDigest:
        durableObjectMigrationHistoryDigest(appendedHistory),
      stateDurableObjectTag: 'state-v2',
      d1SchemaVersion: 2,
      d1SchemaHistoryDigest: '2'.repeat(64),
    };
    const appendedBridge: BridgeSnapshot = {
      ...bridge,
      artifactVersion: 'state-v2',
      artifactDigest: appendedTarget.stateArtifactDigest,
      namespaceIds: [...bridge.namespaceIds, 'namespace-state-v2'],
    };
    const appendedPlan: BridgeMutationPlan = {
      artifactDigest: appendedBridge.artifactDigest,
      durableObjectMigrations: appendedHistory,
      targetDurableObjectTag: 'state-v2',
      secretNames: bridge.secretNames,
      mutationDigest: 'e'.repeat(64),
    };
    let stateInspections = 0;
    let stateUploads = 0;
    const finalizedStateProvider: FinalizedOrdinaryStateProvider = {
      describeFinalizedBridgeTarget: (deployment) =>
        deployment.schemaVersion === 2 ? appendedTarget : platformTarget,
      describeFinalizedState: ({ target: expected }) =>
        expected.stateDurableObjectTag === 'state-v2' ? appendedPlan : plan,
      assertFinalizedState: async () => {
        stateInspections += 1;
        backend.calls.push('state-preflight');
      },
      ensureFinalizedState: async ({ target: expected }) => {
        stateInspections += 1;
        if (expected.stateDurableObjectTag === 'state-v2') {
          stateUploads += 1;
          backend.calls.push('state-upload');
          return appendedBridge;
        }
        return bridge;
      },
      commitFinalizedOwnership: async ({
        currentRecord,
        bridge: committedBridge,
        target: committedTarget,
      }) => {
        const resources = currentRecord.platformResources;
        if (!resources) throw new Error('missing finalized state resources');
        return {
          ...currentRecord,
          platformResources: {
            ...resources,
            stateWorker: {
              ...resources.stateWorker,
              artifactVersion: committedBridge.artifactVersion,
              artifactDigest: committedBridge.artifactDigest,
              ...(committedTarget.stateDurableObjectTag
                ? {
                    durableObjectTag: committedTarget.stateDurableObjectTag,
                  }
                : {}),
              durableObjectBindings: committedBridge.durableObjectBindings,
              namespaceIds: committedBridge.namespaceIds,
            },
          },
        };
      },
    };
    const store = storeFor([current]);
    const migrate = () =>
      migrateFleet({
        store,
        records: [current],
        canaryTenantTags: [],
        backendFor: () => backend,
        finalizedStateProviderFor: () => finalizedStateProvider,
        specFor: () => targetSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
      });

    const [migrated] = await migrate();
    const targetPhysicalScriptName = externalReleaseScriptName(targetSpec);
    expect(migrated).toMatchObject({
      phase: 'ready',
      activeRelease: { physicalScriptName: targetPhysicalScriptName },
      rollbackRelease: { physicalScriptName: activePhysicalScriptName },
      platformResources: {
        stateWorker: {
          scriptName: acme.scriptName,
          artifactVersion: bridge.artifactVersion,
          namespaceIds: bridge.namespaceIds,
        },
      },
      backendSwitchIntent: { subphase: 'finalized' },
    });
    expect(backend.calls.some((call) => call.startsWith('platform:'))).toBe(
      false,
    );
    expect(stateInspections).toBeGreaterThan(0);
    expect(stateUploads).toBe(0);

    const rolledBack = await rollbackExternalRelease({
      store,
      backend,
      currentSpec: targetSpec,
      rollbackSpec: initialSpec,
      secrets: {
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      },
      finalizedStateProvider,
    });
    expect(rolledBack).toMatchObject({
      phase: 'ready',
      activeRelease: { physicalScriptName: activePhysicalScriptName },
      rollbackRelease: { physicalScriptName: targetPhysicalScriptName },
      platformResources: {
        stateWorker: {
          scriptName: acme.scriptName,
          artifactVersion: bridge.artifactVersion,
          namespaceIds: bridge.namespaceIds,
        },
      },
      backendSwitchIntent: { subphase: 'finalized' },
    });
    expect(stateUploads).toBe(0);

    backend.calls.length = 0;
    const stateMigrationSpec = spec(rolledBack, 2);
    const [stateMigrated] = await migrateFleet({
      store,
      records: [rolledBack],
      canaryTenantTags: [],
      backendFor: () => backend,
      finalizedStateProviderFor: () => finalizedStateProvider,
      specFor: () => stateMigrationSpec,
      secretsFor: () => ({
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      }),
    });
    expect(stateMigrated).toMatchObject({
      phase: 'ready',
      schemaVersion: 2,
      durableObjectTag: 'state-v2',
      durableObjectMigrationHistory: appendedHistory,
      platformResources: {
        stateWorker: {
          artifactVersion: 'state-v2',
          durableObjectTag: 'state-v2',
          namespaceIds: ['namespace-retained', 'namespace-state-v2'],
        },
      },
      backendSwitchIntent: { subphase: 'finalized' },
    });
    expect(stateMigrated?.migrationIntent).toBeUndefined();
    expect(stateUploads).toBe(1);
    const migrationIndex = backend.calls.indexOf('migrate:acme');
    const stateIndex = backend.calls.indexOf('state-upload');
    const deployIndex = backend.calls.findIndex((call) =>
      call.startsWith('deploy:acme:'),
    );
    expect(migrationIndex).toBeGreaterThanOrEqual(0);
    expect(stateIndex).toBeGreaterThan(migrationIndex);
    expect(deployIndex).toBeGreaterThan(stateIndex);
    // A migration re-stamps a database this deployment already owns, and asks
    // for 'open' — the state a live, executing deployment already has. Seeding
    // 'migration-locked' here would stop a running deployment mid-migration,
    // which is why migrateFleet takes no fence option at all.
    expect(
      backend.calls.filter((call) => call.startsWith('identity:')),
    ).toEqual(['identity:acme:open']);
  });

  it('returns a stable sorted version report', () => {
    expect(fleetVersionReport([record('beta'), record('acme')])).toEqual([
      expect.objectContaining({ tenantTag: 'acme', artifactVersion: 'v1' }),
      expect.objectContaining({ tenantTag: 'beta', artifactVersion: 'v1' }),
    ]);
  });
});

class RecordingSettlementHost implements FleetSettlementHost {
  readonly settlements: FleetSettlementContext[] = [];
  readonly leaseHeld: boolean[] = [];
  readonly writesBefore: number[] = [];
  throwOnce: string | undefined;
  readonly #store: FleetStore;

  constructor(store: FleetStore) {
    this.#store = store;
  }

  async settle(context: FleetSettlementContext): Promise<void> {
    this.settlements.push(context);
    this.leaseHeld.push(
      this.#store.leases.has(`${context.tenantTag}:${context.environment}`),
    );
    this.writesBefore.push(this.#store.puts.length);
    if (this.throwOnce) {
      const message = this.throwOnce;
      this.throwOnce = undefined;
      throw new Error(message);
    }
  }
}

/** A ready immutable deployment already serving the release it desires. */
function readyImmutableFleet(): {
  backend: ImmutableFleetBackend;
  current: FleetRecord;
  targetSpec: DeploymentSpec;
  activeRelease: ExternalReleaseSnapshot;
} {
  const base = record('acme');
  const targetSpec = spec(base, 1);
  const backend = new ImmutableFleetBackend();
  const platformTarget = backend.describeExternalPlatformTarget(targetSpec);
  const activeRelease: ExternalReleaseSnapshot = {
    physicalScriptName: externalReleaseScriptName(targetSpec),
    specDigest: deploymentSpecDigest(targetSpec),
    artifactVersion: 'etag:active',
    releaseSchemaVersion: 1,
  };
  const current: FleetRecord = {
    ...base,
    schemaVersion: 1,
    desiredSpecDigest: activeRelease.specDigest,
    artifactVersion: activeRelease.artifactVersion,
    activeRelease,
    durableObjectBindings: [],
    platformTarget,
    outboundPolicy: platformTarget.outboundPolicy,
    platformResources: {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: externalStateScriptName(targetSpec),
        artifactVersion: 'state-v1',
        artifactDigest: platformTarget.stateArtifactDigest,
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(targetSpec),
        artifactVersion: 'egress-v1',
        artifactDigest: platformTarget.egressArtifactDigest,
        ...platformTarget.outboundPolicy,
      },
    },
  };
  backend.releases.set(activeRelease.physicalScriptName, {
    ...liveFor(current),
    scriptName: activeRelease.physicalScriptName,
    durableObjectBindings: [],
    schemaVersion: 1,
  });
  backend.routedScriptName = activeRelease.physicalScriptName;
  return { backend, current, targetSpec, activeRelease };
}

/** A ready immutable deployment whose PLATFORM profile has moved on. */
function platformOnlyChangeFleet(): {
  backend: ImmutableFleetBackend;
  current: FleetRecord;
  targetSpec: DeploymentSpec;
} {
  const base = record('acme');
  const targetSpec = spec(base, 1);
  const backend = new ImmutableFleetBackend();
  const initialTarget = {
    ...backend.describeExternalPlatformTarget(targetSpec),
    d1SchemaVersion: 2,
    d1SchemaHistoryDigest: 'f'.repeat(64),
  };
  const activeRelease: ExternalReleaseSnapshot = {
    physicalScriptName: externalReleaseScriptName(targetSpec),
    specDigest: deploymentSpecDigest(targetSpec),
    artifactVersion: 'etag:active',
    releaseSchemaVersion: 1,
  };
  const current: FleetRecord = {
    ...base,
    schemaVersion: 2,
    desiredSpecDigest: activeRelease.specDigest,
    artifactVersion: activeRelease.artifactVersion,
    activeRelease,
    durableObjectBindings: [],
    platformTarget: initialTarget,
    outboundPolicy: initialTarget.outboundPolicy,
    platformResources: {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: externalStateScriptName(targetSpec),
        artifactVersion: 'state-v1',
        artifactDigest: initialTarget.stateArtifactDigest,
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(targetSpec),
        artifactVersion: 'egress-v1',
        artifactDigest: initialTarget.egressArtifactDigest,
        ...initialTarget.outboundPolicy,
      },
    },
  };
  backend.releases.set(activeRelease.physicalScriptName, {
    ...liveFor(current),
    scriptName: activeRelease.physicalScriptName,
    durableObjectBindings: [],
    schemaVersion: activeRelease.releaseSchemaVersion,
  });
  backend.routedScriptName = activeRelease.physicalScriptName;
  backend.platformStateDigest = 'd'.repeat(64);
  backend.platformPolicyHosts = ['narrow.example.com'];
  return { backend, current, targetSpec };
}

function platformOnlyMigrate(
  store: FleetStore,
  backend: ImmutableFleetBackend,
  current: FleetRecord,
  targetSpec: DeploymentSpec,
  host: FleetSettlementHost,
): Promise<readonly FleetRecord[]> {
  return migrateFleet({
    store,
    records: [current],
    canaryTenantTags: [],
    backendFor: () => backend,
    specFor: () => targetSpec,
    secretsFor: () => ({
      deploymentIdentity: 'deployment-identity-secret-value-0001',
      maintenanceAdmin: 'maintenance-admin-secret-value-00001',
    }),
    settlementFor: () => host,
  });
}

describe('lease-held settlement', () => {
  it('settles a migration after attestation and before the settling write', async () => {
    // #given a plain deployment with a newer specification to migrate to
    const acme = record('acme');
    const backend = new FleetBackend();
    const store = new FleetStore();
    await store.put(acme);
    const target = spec(acme, 2);
    const host = new RecordingSettlementHost(store);

    // #when the fleet migrates with a settling host
    const [migrated] = await migrateFleet({
      store,
      records: [acme],
      canaryTenantTags: [],
      backendFor: () => backend,
      specFor: () => target,
      secretsFor: () => ({
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      }),
      settlementFor: () => host,
    });

    // #then it settled once, while the lease was held, after the route was
    // attested and before the write that records the settlement
    expect(host.settlements).toHaveLength(1);
    expect(host.leaseHeld).toEqual([true]);
    expect(backend.calls.indexOf('attest:acme')).toBeGreaterThan(
      backend.calls.indexOf('promote:acme'),
    );
    expect(host.writesBefore[0]).toBe(store.puts.length - 1);

    const settlementKey = fleetSettlementKey({
      tenantTag: 'acme',
      environment: 'production',
      specDigest: deploymentSpecDigest(target),
      artifactVersion: 'v2',
    });
    expect(host.settlements[0]).toMatchObject({
      tenantTag: 'acme',
      environment: 'production',
      entry: 'migration',
      settlementKey,
      alreadySettled: false,
      attestation: { artifactVersion: 'v2', source: 'workers-deployments' },
      target: { specDigest: deploymentSpecDigest(target) },
    });
    expect(migrated?.settledSettlementKey).toBe(settlementKey);
  });

  it('attests without a settling host, and refuses a route serving something else', async () => {
    // #given a converge whose route was left on a foreign release, and no host
    const { backend, current, targetSpec } = readyImmutableFleet();
    const store = new FleetStore();
    await store.put(current);
    backend.routeDrift = {
      specDigest: 'f'.repeat(64),
      artifactVersion: 'etag:stranger',
      physicalScriptName: 'acme-production-stranger',
      source: 'dispatch-route',
      observedAt: '2026-08-11T00:00:00.000Z',
    };

    // #when the fleet converges
    const converge = migrateFleet({
      store,
      records: [current],
      canaryTenantTags: [],
      backendFor: () => backend,
      specFor: () => targetSpec,
      secretsFor: () => ({
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      }),
      routeAttestation: { convergenceBudgetMs: 1, initialRetryDelayMs: 1 },
    });

    // #then attestation ran anyway and failed the converge closed
    await expect(converge).rejects.toThrow(/did not converge/);
    expect(backend.calls).toContain('attest:acme');
  });

  it('attests every routine converge but settles only the first', async () => {
    // #given a ready deployment already serving what it desires
    const { backend, current, targetSpec, activeRelease } =
      readyImmutableFleet();
    const store = new FleetStore();
    await store.put(current);
    const host = new RecordingSettlementHost(store);
    const converge = (record_: FleetRecord) =>
      migrateFleet({
        store,
        records: [record_],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => targetSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
        settlementFor: () => host,
      });

    // #when the same unchanged deployment is reconciled twice
    const [first] = await converge(current);
    if (!first) throw new Error('convergence returned no record');
    const writesAfterFirst = store.puts.length;
    const [second] = await converge(first);

    // #then the first settled, the second attested and did neither settle nor
    // write — a fleet on a reconcile schedule settles once, not forever
    expect(host.settlements).toHaveLength(1);
    expect(host.settlements[0]).toMatchObject({
      entry: 'ready-convergence',
      alreadySettled: false,
      target: { artifactVersion: activeRelease.artifactVersion },
    });
    expect(first.settledSettlementKey).toBe(host.settlements[0]?.settlementKey);
    expect(second?.settledSettlementKey).toBe(first.settledSettlementKey);
    expect(store.puts.length).toBe(writesAfterFirst);
    expect(backend.calls.filter((call) => call === 'attest:acme')).toHaveLength(
      2,
    );
  });

  it('waits out a route that has not converged before settling', async () => {
    // #given host routing answering once with the release just replaced
    const { backend, current, targetSpec } = readyImmutableFleet();
    const store = new FleetStore();
    await store.put(current);
    backend.staleRoute = {
      specDigest: 'c'.repeat(64),
      artifactVersion: 'etag:prior',
      physicalScriptName: 'acme-production-prior',
      source: 'dispatch-route',
      observedAt: '2026-08-11T00:00:00.000Z',
    };
    backend.staleRouteReads = 1;
    const host = new RecordingSettlementHost(store);

    // #when the fleet converges
    const [converged] = await migrateFleet({
      store,
      records: [current],
      canaryTenantTags: [],
      backendFor: () => backend,
      specFor: () => targetSpec,
      secretsFor: () => ({
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      }),
      settlementFor: () => host,
      routeAttestation: { initialRetryDelayMs: 1, maxRetryDelayMs: 1 },
    });

    // #then the stale read was waited out rather than read as drift
    expect(backend.calls.filter((call) => call === 'attest:acme')).toHaveLength(
      2,
    );
    expect(host.settlements).toHaveLength(1);
    expect(converged?.settledSettlementKey).toBe(
      host.settlements[0]?.settlementKey,
    );
  });

  it('leaves a platform-only migration resumable when settlement throws', async () => {
    // #given a platform profile change on a ready immutable deployment
    const { backend, current, targetSpec } = platformOnlyChangeFleet();
    const store = new FleetStore();
    await store.put(current);
    const host = new RecordingSettlementHost(store);
    host.throwOnce = 'settlement ledger is unavailable';
    const migrate = () =>
      platformOnlyMigrate(store, backend, current, targetSpec, host);

    // #when settlement throws, then the migration is re-entered
    await expect(migrate()).rejects.toThrow(/settlement ledger is unavailable/);
    const stranded = await store.get('acme', 'production');
    const [settled] = await migrate();

    // #then the throw left the migration where a retry resumes it, and the
    // retry delivered the same settlement rather than a new one
    expect(stranded).toMatchObject({
      phase: 'migrating',
      migrationIntent: { platformOnly: true, subphase: 'route-published' },
    });
    expect(stranded?.settledSettlementKey).toBeUndefined();
    expect(host.settlements).toHaveLength(2);
    expect(host.settlements[1]?.settlementKey).toBe(
      host.settlements[0]?.settlementKey,
    );
    expect(host.settlements.map((context) => context.entry)).toEqual([
      'platform-only',
      'platform-only',
    ]);
    expect(settled?.settledSettlementKey).toBe(
      host.settlements[0]?.settlementKey,
    );
  });

  it('re-fires a platform-only settlement whose settling write was lost', async () => {
    // #given a platform-only migration whose settlement SUCCEEDS and whose
    // settling write is then lost — the window a throw cannot reach
    const { backend, current, targetSpec } = platformOnlyChangeFleet();
    const store = new FleetStore();
    await store.put(current);
    const host = new RecordingSettlementHost(store);
    const migrate = () =>
      platformOnlyMigrate(store, backend, current, targetSpec, host);
    store.failNextReadyPut = true;

    // #when the settling write is lost, then the migration is re-entered
    await expect(migrate()).rejects.toThrow(/state write failed/);
    const stranded = await store.get('acme', 'production');
    const [settled] = await migrate();

    // #then the lost write left the migration re-enterable, and both deliveries
    // carry alreadySettled: false because neither settling write was durable
    expect(stranded).toMatchObject({
      phase: 'migrating',
      migrationIntent: { platformOnly: true, subphase: 'route-published' },
    });
    expect(stranded?.settledSettlementKey).toBeUndefined();
    expect(host.settlements).toHaveLength(2);
    expect(host.settlements[1]?.settlementKey).toBe(
      host.settlements[0]?.settlementKey,
    );
    expect(host.settlements.map((context) => context.alreadySettled)).toEqual([
      false,
      false,
    ]);
    expect(settled?.settledSettlementKey).toBe(
      host.settlements[0]?.settlementKey,
    );
  });

  it('re-fires a migration settlement whose settling write was lost', async () => {
    // #given a migration whose settlement succeeds and whose migrated write
    // is then lost
    const acme = record('acme');
    const backend = new FleetBackend();
    const store = new FleetStore();
    await store.put(acme);
    const target = spec(acme, 2);
    const host = new RecordingSettlementHost(store);
    const migrate = () =>
      migrateFleet({
        store,
        records: [acme],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => target,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
        settlementFor: () => host,
      });
    store.failNextReadyPut = true;

    // #when the settling write is lost, then the migration is re-entered
    await expect(migrate()).rejects.toThrow(/state write failed/);
    const stranded = await store.get('acme', 'production');
    const [migrated] = await migrate();

    // #then the same settlement is delivered again rather than a new one, and
    // the retry reaches the ready state the lost write was carrying
    expect(stranded?.phase).toBe('migrating');
    expect(stranded?.settledSettlementKey).toBeUndefined();
    expect(host.settlements).toHaveLength(2);
    expect(host.settlements[1]?.settlementKey).toBe(
      host.settlements[0]?.settlementKey,
    );
    expect(host.settlements.map((context) => context.alreadySettled)).toEqual([
      false,
      false,
    ]);
    expect(migrated).toMatchObject({
      phase: 'ready',
      settledSettlementKey: host.settlements[0]?.settlementKey,
    });
  });

  it('re-fires a convergence settlement whose settling write was lost', async () => {
    // #given a ready deployment whose convergence settles and whose added
    // settling write is then lost
    const { backend, current, targetSpec } = readyImmutableFleet();
    const store = new FleetStore();
    await store.put(current);
    const host = new RecordingSettlementHost(store);
    const converge = (from: FleetRecord) =>
      migrateFleet({
        store,
        records: [from],
        canaryTenantTags: [],
        backendFor: () => backend,
        specFor: () => targetSpec,
        secretsFor: () => ({
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'maintenance-admin-secret-value-00001',
        }),
        settlementFor: () => host,
      });
    store.failNextPutWhen = (next) =>
      next.settledSettlementKey
        ? 'state write failed after settlement'
        : undefined;

    // #when the settling write is lost, then the deployment is reconciled
    // twice more
    await expect(converge(current)).rejects.toThrow(/after settlement/);
    const stranded = await store.get('acme', 'production');
    const [recovered] = await converge(current);
    if (!recovered) throw new Error('convergence returned no record');
    const writesAfterRecovery = store.puts.length;
    await converge(recovered);

    // #then the lost write meant the settlement re-fired under the same key,
    // and once it was recorded the next reconcile settled nothing and wrote
    // nothing at all
    expect(stranded?.settledSettlementKey).toBeUndefined();
    expect(host.settlements).toHaveLength(2);
    expect(host.settlements[1]?.settlementKey).toBe(
      host.settlements[0]?.settlementKey,
    );
    expect(host.settlements.map((context) => context.alreadySettled)).toEqual([
      false,
      false,
    ]);
    expect(recovered.settledSettlementKey).toBe(
      host.settlements[0]?.settlementKey,
    );
    expect(store.puts.length).toBe(writesAfterRecovery);
  });
});
