// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { ActiveRouteAttestationError } from '../src/active-route.js';
import type {
  BridgeMutationPlan,
  BridgeSnapshot,
  FinalizedOrdinaryStateProvider,
} from '../src/backend-switch.js';
import {
  type AdvanceDecommissionDeploymentOptions,
  advanceDecommissionDeployment,
  DecommissionAdvanceCapabilityError,
  DecommissionAdvanceRestartError,
  type DecommissionAdvanceResult,
} from '../src/decommission-advance.js';
import { normalizeDecommissionAdvanceIntent } from '../src/decommission-intent.js';
import { migrateFleet, rollbackExternalRelease } from '../src/fleet.js';
import {
  canonicalDeploymentEgressPolicy,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalReleaseTopology,
  externalStateScriptName,
  FLEET_AUDIT_PROXY_CLASS_NAME,
  FLEET_AUDIT_PROXY_STATE_BINDING,
} from '../src/platform-resources.js';
import { providerBindingIdentitiesForInspection } from '../src/provider-binding-inventory.js';
import {
  assertLiveDeploymentMatches,
  cleanupDeploymentArtifacts,
  decommissionDeployment,
  forceDecommissionDeployment,
  ProvisioningError,
  provisionDeployment,
} from '../src/provision.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  ActiveRouteAttestation,
  ApplicationR2BucketSnapshot,
  ApplicationR2Resource,
  DatabaseReference,
  DecommissionAttachmentScanInput,
  DecommissionAttachmentScanResult,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  ForceDecommissionStep,
  InitialExecutionFenceState,
  LiveDeployment,
  MaintenanceHealth,
  PlainWorkerRouteApi,
  ProvisioningBackend,
  ProvisioningBackendKind,
  SeedDeploymentIdentityOptions,
} from '../src/types.js';
import { externalReleaseScriptName } from '../src/workers-for-platforms-backend.js';
import { WranglerLoopBackend } from '../src/wrangler-loop-backend.js';
import type { CommandResult, CommandRunner } from '../src/wrangler-runner.js';
import { decommissionAdvancingRecordFixture } from './fixtures/decommission-intent-fixture.js';
import { memoryStore, routeApi } from './fixtures/plain-worker-port-probe.js';
import {
  type PlainWorkerFsControl,
  registerScratchCleanup,
} from './fixtures/wrangler-fs-mock.js';

const fsControl = vi.hoisted<PlainWorkerFsControl>(() => ({
  failFleetCleanup: false,
  residualDirectory: undefined,
  cleanupError: new Error('provision upload cleanup failed'),
}));

vi.mock('node:fs/promises', async () => {
  const { createFsPromisesMock } = await import(
    './fixtures/wrangler-fs-mock.js'
  );
  return createFsPromisesMock(fsControl);
});

const exportDirectories = registerScratchCleanup(fsControl, {
  cleanupError: fsControl.cleanupError,
});

const secrets: DeploymentSecrets = {
  deploymentIdentity: 'deployment-identity-secret-value-0001',
  maintenanceAdmin: 'maintenance-admin-secret-value-00001',
};

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
  leaseCalls = 0;
  readonly phases: string[] = [];
  failPutPhase: string | undefined;
  failPutApplicationState:
    | Readonly<{ name: string; state: ApplicationR2Resource['state'] }>
    | undefined;
  assertOwnedFailure: unknown;

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    this.leaseCalls += 1;
    if (this.leased) throw new Error('deployment is already being modified');
    this.leased = true;
    try {
      return await operation({
        tenantTag,
        environment,
        mutationLeaseTtlMs: 15 * 60_000,
        assertOwned: async () => {
          if (this.assertOwnedFailure !== undefined) {
            throw this.assertOwnedFailure;
          }
        },
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
    if (record.decommissionIntent) {
      const { decommissionIntent, ...source } = record;
      record = {
        ...source,
        decommissionIntent: normalizeDecommissionAdvanceIntent(
          decommissionIntent,
          source,
        ),
      };
    }
    if (this.failPutPhase === record.phase) {
      this.failPutPhase = undefined;
      throw new Error(`failed state write at ${record.phase}`);
    }
    const applicationFailure = this.failPutApplicationState;
    if (
      applicationFailure &&
      record.applicationResources?.some(
        (resource) =>
          resource.name === applicationFailure.name &&
          resource.state === applicationFailure.state,
      )
    ) {
      this.failPutApplicationState = undefined;
      throw new Error(
        `failed state write at ${applicationFailure.name}:${applicationFailure.state}`,
      );
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

/** Pinned so an attestation these fakes return is comparable by value. */
const ATTESTED_AT = '2026-08-11T00:00:00.000Z';

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
  /** Strands the route on something else; unset attests the live deployment. */
  activeRoute: ActiveRouteAttestation | undefined;
  exportLocation = 'r2://fleet-exports/acme.sql';
  databaseExists = false;
  databaseId = 'database-id';
  databaseName = 'acme-production';
  databaseOwner: string | undefined;
  /** Every fence state provisioning asked for, in call order. */
  readonly seededFenceStates: InitialExecutionFenceState[] = [];
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
  readonly forceSteps: ForceDecommissionStep[] = [];
  forceFailOnceAt: ForceDecommissionStep | undefined;
  forceStepGate: Promise<void> | undefined;
  forceStepStarted: (() => void) | undefined;
  readonly scanInputs: DecommissionAttachmentScanInput[] = [];
  readonly scanResults: DecommissionAttachmentScanResult[] = [];
  scanFailure: unknown;
  residualCalls = 0;

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

  // The fence state is DECLARED here, not dropped: a fake that omits the
  // options argument still satisfies the interface (TypeScript accepts a
  // fewer-argument function in a more-argument slot), so a silently unthreaded
  // fence state would leave every one of these tests green.
  async seedDeploymentIdentity(
    _database: DatabaseReference,
    tenantTag: string,
    _fence: ExternalMutationFence,
    options: SeedDeploymentIdentityOptions,
  ): Promise<void> {
    this.seededFenceStates.push(options.initialExecutionFenceState);
    this.#event('identity');
    this.databaseOwner = tenantTag;
  }

  async readDeploymentIdentity(): Promise<string | undefined> {
    return this.databaseOwner;
  }

  async applyMigrations(): Promise<void> {
    this.#event('migrations');
  }

  async advanceDecommissionAttachmentScan(
    input: DecommissionAttachmentScanInput,
  ): Promise<DecommissionAttachmentScanResult> {
    this.scanInputs.push(input);
    if (this.scanFailure !== undefined) throw this.scanFailure;
    return (
      this.scanResults.shift() ?? {
        status: 'complete',
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 2,
        providerFetchAttemptsReserved: 3,
      }
    );
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
    this.live = completeLiveDeployment({
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
    });
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
    if (!this.live) return undefined;
    const { providerBindingIdentities: _inventory, ...live } = this.live;
    return completeLiveDeployment(live);
  }

  async attestActiveRoute(
    deployment: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    this.#event('attest');
    const live = this.live;
    const attestation =
      this.activeRoute ??
      (live
        ? {
            specDigest: live.desiredSpecDigest,
            artifactVersion: live.artifactVersion,
            physicalScriptName: live.scriptName,
            source: 'workers-deployments' as const,
            observedAt: ATTESTED_AT,
          }
        : undefined);
    if (!attestation) {
      throw new ActiveRouteAttestationError(
        `nothing serves '${deployment.routeHostname}'`,
        {},
      );
    }
    return attestation;
  }

  async removeTraffic(
    _deployment?: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] = [],
    activeRelease?: ExternalReleaseSnapshot,
  ): Promise<void> {
    this.retainedReleases = retainedReleases;
    this.activeRelease = activeRelease;
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

  async assertDatabaseDeletionResidualsRemoved(): Promise<void> {
    this.residualCalls += 1;
  }

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

  async forceDecommissionStep(
    _record: FleetRecord,
    step: ForceDecommissionStep,
  ): Promise<void> {
    this.forceSteps.push(step);
    this.forceStepStarted?.();
    await this.forceStepGate;
    if (this.forceFailOnceAt === step) {
      this.forceFailOnceAt = undefined;
      throw new Error(`failed force step ${step}`);
    }
    if (step === 'delete-database') this.databaseExists = false;
  }
}

class R2RollbackBackend extends FakeBackend {
  readonly buckets = new Map<string, ApplicationR2BucketSnapshot>();
  failDetachOnceFor: string | undefined;
  nonempty = false;
  emptyChecks = 0;
  writeAfterTrafficRemovalOnce = false;
  r2FindCalls = 0;
  readonly r2FindNames: string[] = [];
  deleteCalls = 0;
  deleteFailureBeforeCommit: unknown;
  deleteFailureAfterCommit: unknown;

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
    this.r2FindCalls += 1;
    this.r2FindNames.push(resource.name);
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
    this.deleteCalls += 1;
    if (this.deleteFailureBeforeCommit !== undefined) {
      throw this.deleteFailureBeforeCommit;
    }
    this.buckets.delete(resource.bucketName);
    if (this.deleteFailureAfterCommit !== undefined) {
      throw this.deleteFailureAfterCommit;
    }
  }
}

async function wranglerLoopHarness(deployment: DeploymentSpec) {
  const digest = deploymentSpecDigest(deployment);
  const state = {
    databaseExists: false,
    workerExists: false,
    deploymentExists: false,
    preexistingVersion: false,
    candidateVersion: false,
    publicAccessEnabled: false,
    sentinelExists: false,
    databaseOwner: undefined as string | undefined,
    appliedMigrations: 0,
  };
  const runnerCalls: string[][] = [];
  const runner: CommandRunner = {
    maxDurationMs: 5 * 60_000,
    async run(arguments_: readonly string[]): Promise<CommandResult> {
      runnerCalls.push([...arguments_]);
      const command = arguments_.slice(0, 2).join(' ');
      if (command === 'd1 list') {
        return {
          stdout: JSON.stringify(
            state.databaseExists
              ? [{ uuid: 'database-id', name: deployment.databaseName }]
              : [],
          ),
          stderr: '',
        };
      }
      if (command === 'd1 create') {
        state.databaseExists = true;
        return { stdout: '', stderr: '' };
      }
      if (command === 'deployments status') {
        if (!state.deploymentExists) throw new Error('deployment not found');
        return {
          stdout: JSON.stringify({
            versions: [{ id: 'candidate', percentage: 100 }],
          }),
          stderr: '',
        };
      }
      if (command === 'versions list') {
        if (!state.workerExists) throw new Error('script not found');
        return {
          stdout: JSON.stringify([
            ...(state.preexistingVersion
              ? [
                  {
                    id: 'existing',
                    annotations: { 'workers/tag': digest },
                  },
                ]
              : []),
            ...(state.candidateVersion
              ? [
                  {
                    id: 'candidate',
                    annotations: { 'workers/tag': digest },
                  },
                ]
              : []),
          ]),
          stderr: '',
        };
      }
      if (command === 'versions view') {
        return {
          stdout: JSON.stringify({
            id: 'candidate',
            annotations: { 'workers/tag': digest },
            resources: {
              bindings: [
                { type: 'd1', name: 'DB', id: 'database-id' },
                {
                  type: 'durable_object_namespace',
                  name: 'MAINTENANCE',
                  class_name: 'Maintenance',
                  namespace_id: 'namespace-maintenance',
                },
                {
                  type: 'plain_text',
                  name: 'DEPLOYMENT_TENANT',
                  text: deployment.tenantTag,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_ENVIRONMENT',
                  text: deployment.environment,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_SCHEMA_VERSION',
                  text: String(deployment.schemaVersion),
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_SPEC_DIGEST',
                  text: digest,
                },
                {
                  type: 'plain_text',
                  name: 'FLEET_INGRESS_CONTRACT',
                  text: 'guarded-object-v1',
                },
              ],
            },
          }),
          stderr: '',
        };
      }
      if (arguments_[0] === 'deploy') {
        state.workerExists = true;
        state.deploymentExists = true;
        state.candidateVersion = true;
        state.publicAccessEnabled = true;
        return { stdout: '', stderr: '' };
      }
      if (arguments_[0] === 'delete') {
        state.workerExists = false;
        state.deploymentExists = false;
        state.preexistingVersion = false;
        state.candidateVersion = false;
        state.publicAccessEnabled = false;
        return { stdout: '', stderr: '' };
      }
      throw new Error(`unexpected command ${arguments_.join(' ')}`);
    },
  };
  const plainRouteApi: PlainWorkerRouteApi = routeApi({
    async queryDatabase(_databaseId, sql, bindings = []) {
      if (
        sql.includes("FROM sqlite_schema WHERE type = 'table' ORDER BY name")
      ) {
        return state.sentinelExists
          ? [
              {
                name: 'flowsafe_deployment',
                sql: 'CREATE TABLE IF NOT EXISTS flowsafe_deployment (id INTEGER PRIMARY KEY CHECK (id = 1), tenant_tag TEXT NOT NULL, provisioned_at TEXT NOT NULL)',
              },
            ]
          : [];
      }
      if (
        sql.includes("FROM sqlite_schema WHERE type = 'table' AND name = ?")
      ) {
        return state.sentinelExists
          ? [
              {
                sql: 'CREATE TABLE IF NOT EXISTS flowsafe_deployment (id INTEGER PRIMARY KEY CHECK (id = 1), tenant_tag TEXT NOT NULL, provisioned_at TEXT NOT NULL)',
              },
            ]
          : [];
      }
      if (sql.startsWith('PRAGMA table_info(flowsafe_deployment)')) {
        return [
          { name: 'id', type: 'INTEGER', notnull: 0, pk: 1 },
          { name: 'tenant_tag', type: 'TEXT', notnull: 1, pk: 0 },
          { name: 'provisioned_at', type: 'TEXT', notnull: 1, pk: 0 },
        ];
      }
      if (sql.includes('SELECT id, tenant_tag FROM flowsafe_deployment')) {
        return state.databaseOwner
          ? [{ id: 1, tenant_tag: state.databaseOwner }]
          : [];
      }
      if (sql.startsWith('CREATE TABLE IF NOT EXISTS flowsafe_deployment')) {
        state.sentinelExists = true;
        return [];
      }
      if (sql.startsWith('INSERT OR IGNORE INTO flowsafe_deployment')) {
        state.databaseOwner = bindings[0];
        return [];
      }
      if (sql.includes('SELECT version, sql_sha256')) {
        return deployment.migrations
          .slice(0, state.appliedMigrations)
          .map((migration) => ({
            version: migration.version,
            sql_sha256: createHash('sha256')
              .update(migration.sql)
              .digest('hex'),
          }));
      }
      return [];
    },
    async batchDatabase() {
      state.appliedMigrations += 1;
    },
    async getDatabase(databaseId) {
      return state.databaseExists && databaseId === 'database-id'
        ? {
            id: 'database-id',
            name: deployment.databaseName,
            created: false,
          }
        : undefined;
    },
    async deleteDatabase(databaseId) {
      if (databaseId === 'database-id') state.databaseExists = false;
    },
    async inspectOrdinaryWorkerFootprint() {
      return {
        scriptPresent: state.workerExists,
        workersDevEnabled: state.publicAccessEnabled,
        previewUrlsEnabled: false,
        customDomains: [],
        zoneRoutes: [],
      };
    },
    async disableOrdinaryWorkerPublicAccess() {
      state.publicAccessEnabled = false;
    },
    async listDurableObjectNamespaces() {
      return state.workerExists ? ['namespace-maintenance'] : [];
    },
  });

  const exportDirectory = await mkdtemp(join(tmpdir(), 'provision-export-'));
  exportDirectories.add(exportDirectory);
  const backend = new WranglerLoopBackend({
    runner,
    routeApi: plainRouteApi,
    exportDirectory,
    exportStore: memoryStore(),
  });
  const store = new MemoryStore();

  return { backend, store, runnerCalls, state };
}

const DECOMMISSION_OPERATION_ID = '12345678-1234-4abc-8def-1234567890ab';

interface BoundedDecommissionHarness {
  readonly backend: R2RollbackBackend;
  readonly store: MemoryStore;
  readonly deployment: DeploymentSpec;
  readonly clock: () => number;
  readonly randomUUID: () => string;
}

async function boundedDecommissionHarness(
  options: {
    readonly kind?: ProvisioningBackendKind;
    readonly r2Names?: readonly string[];
    readonly store?: MemoryStore;
    readonly external?: boolean;
  } = {},
): Promise<BoundedDecommissionHarness> {
  const backend = new R2RollbackBackend(options.kind ?? 'plain-worker');
  const store = options.store ?? new MemoryStore();
  const deployment = spec({
    ...(options.external
      ? {
          authoredBy: 'external' as const,
          durableObjectMigrations: [],
          egressProxyService: undefined,
        }
      : {}),
    application: {
      vars: [],
      secrets: [],
      r2Buckets: (options.r2Names ?? []).map((name) => ({ name })),
    },
  });
  await provisionDeployment({
    initialExecutionFenceState: 'open',
    backend,
    store,
    spec: deployment,
    secrets,
  });
  backend.events.length = 0;
  let tick = Date.parse('2026-08-30T00:00:00.000Z');
  return {
    backend,
    store,
    deployment,
    clock: () => {
      tick += 1_000;
      return tick;
    },
    randomUUID: () => DECOMMISSION_OPERATION_ID,
  };
}

function boundedAdvanceOptions(
  harness: BoundedDecommissionHarness,
  action: AdvanceDecommissionDeploymentOptions['action'],
  overrides: Partial<AdvanceDecommissionDeploymentOptions> = {},
): AdvanceDecommissionDeploymentOptions {
  return {
    backend: harness.backend,
    store: harness.store,
    spec: harness.deployment,
    action,
    maxProviderRequests: 12,
    clock: harness.clock,
    randomUUID: harness.randomUUID,
    ...overrides,
  };
}

async function startBoundedDecommission(
  harness: BoundedDecommissionHarness,
): Promise<DecommissionAdvanceResult> {
  return advanceDecommissionDeployment(
    boundedAdvanceOptions(harness, { kind: 'start' }),
  );
}

async function continueBoundedDecommission(
  harness: BoundedDecommissionHarness,
  result: DecommissionAdvanceResult,
): Promise<DecommissionAdvanceResult> {
  return advanceDecommissionDeployment(
    boundedAdvanceOptions(harness, {
      kind: 'continue',
      token: result.token,
    }),
  );
}

async function driveBoundedUntil(
  harness: BoundedDecommissionHarness,
  predicate: (record: FleetRecord) => boolean,
): Promise<DecommissionAdvanceResult> {
  let result = await startBoundedDecommission(harness);
  for (let step = 0; step < 64; step += 1) {
    const record = harness.store.record;
    if (record && predicate(record)) return result;
    result = await continueBoundedDecommission(harness, result);
  }
  throw new Error('bounded decommission did not reach the requested state');
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
    const live = completeLiveDeployment({
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
    });

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
    ).toThrow(/provider binding|does not exactly match/u);
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
    ).toThrow(/provider binding|does not exactly match/u);
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
      initialExecutionFenceState: 'open',
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toThrow(/active backend switch 'domain-detach-authorized'/);
    expect(backend.events).toEqual([]);
  });

  it('refuses every root lifecycle mutation while decommission advances', async () => {
    const deployment = spec();
    const external = spec({
      authoredBy: 'external',
      durableObjectMigrations: [],
      egressProxyService: undefined,
    });
    const digest = deploymentSpecDigest(deployment);
    const operationId = '00000000-0000-4000-8000-000000000001';
    const advancingRecord = (backend: FakeBackend) =>
      decommissionAdvancingRecordFixture(
        {
          tenantTag: deployment.tenantTag,
          environment: deployment.environment,
          backend: backend.kind,
          scriptName: deployment.scriptName,
          databaseId: backend.databaseId,
          databaseName: deployment.databaseName,
          schemaVersion: deployment.schemaVersion,
          artifactVersion: 'artifact-v3',
          desiredSpecDigest: digest,
          durableObjectBindings: [],
          routeHostname: deployment.routeHostname,
          phase: 'ready',
          updatedAt: '2026-08-11T00:00:00.000Z',
        },
        'ready',
        { operationId },
      );
    const cases: readonly Readonly<{
      name: string;
      run(input: {
        backend: FakeBackend;
        store: MemoryStore;
        current: FleetRecord;
      }): Promise<unknown>;
    }>[] = [
      {
        name: 'provisionDeployment',
        run: ({ backend, store }) =>
          provisionDeployment({
            initialExecutionFenceState: 'open',
            backend,
            store,
            spec: deployment,
            secrets,
          }),
      },
      {
        name: 'migrateFleet',
        run: ({ backend, store, current }) =>
          migrateFleet({
            store,
            records: [current],
            canaryTenantTags: [],
            backendFor: () => backend,
            specFor: () => deployment,
            secretsFor: () => secrets,
          }),
      },
      {
        name: 'rollbackExternalRelease',
        run: ({ backend, store }) =>
          rollbackExternalRelease({
            store,
            backend,
            currentSpec: external,
            rollbackSpec: external,
            secrets,
          }),
      },
      {
        name: 'cleanupDeploymentArtifacts',
        run: ({ backend, store }) =>
          cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
      },
      {
        name: 'forceDecommissionDeployment',
        run: ({ backend, store }) =>
          forceDecommissionDeployment({
            backend,
            store,
            tenantTag: deployment.tenantTag,
            environment: deployment.environment,
          }),
      },
      {
        name: 'decommissionDeployment',
        run: ({ backend, store }) =>
          decommissionDeployment({ backend, store, spec: deployment }),
      },
    ];

    for (const item of cases) {
      const backend = new FakeBackend();
      const store = new MemoryStore();
      const current = advancingRecord(backend);
      store.record = current;

      await expect(
        item.run({ backend, store, current }),
        item.name,
      ).rejects.toThrow(
        `${item.name} cannot run during an active decommission`,
      );
      expect(backend.events, item.name).toEqual([]);
      expect(backend.findDatabaseCalls, item.name).toBe(0);
      expect(backend.databaseIdsRead, item.name).toEqual([]);
    }

    const raceBackend = new FakeBackend();
    const advancing = advancingRecord(raceBackend);
    const { decommissionIntent: _decommissionIntent, ...ready } = advancing;
    const raceStore = new MemoryStore();
    raceStore.record = advancing;
    let reads = 0;
    raceStore.get = async () => {
      reads += 1;
      return reads === 1
        ? ({ ...ready, phase: 'ready' } as FleetRecord)
        : raceStore.record;
    };
    await expect(
      decommissionDeployment({
        backend: raceBackend,
        store: raceStore,
        spec: deployment,
      }),
    ).rejects.toThrow(
      'decommissionDeployment cannot run during an active decommission',
    );
    expect(reads).toBe(2);
    expect(raceBackend.events).toEqual([]);

    const completeStore = new MemoryStore();
    const activeIntent = advancing.decommissionIntent;
    if (!activeIntent || activeIntent.state === 'complete') {
      throw new Error('missing active decommission intent');
    }
    completeStore.record = {
      ...advancing,
      phase: 'decommissioned',
      applicationResources: [],
      databaseExportLocation: 'r2://exports/database.sqlite',
      databaseExportSha256: 'f'.repeat(64),
      databaseExportSize: 128,
      decommissionIntent: {
        ...activeIntent,
        lifecyclePhase: 'decommissioned',
        state: 'complete',
      },
    };
    await expect(
      forceDecommissionDeployment({
        backend: raceBackend,
        store: completeStore,
        tenantTag: deployment.tenantTag,
        environment: deployment.environment,
      }),
    ).resolves.toBeUndefined();
    expect(completeStore.record).toBeUndefined();
    expect(raceBackend.events).toEqual([]);
  });

  it('persists the ordered create phases and returns a ready deployment', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const result = await provisionDeployment({
      initialExecutionFenceState: 'open',
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
      // The ready commit records what is ROUTED, so the last provider read on
      // the create path is the attestation, after the promotion, not before.
      'attest',
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
    expect(backend.seededFenceStates).toEqual(['open']);
  });

  it('commits the routed artifact version, and refuses when it is foreign', async () => {
    // #given a first deploy whose route ends up serving a different artifact
    const backend = new FakeBackend();
    const store = new MemoryStore();
    backend.activeRoute = {
      specDigest: deploymentSpecDigest(spec()),
      artifactVersion: 'artifact-someone-else-promoted',
      physicalScriptName: 'acme-production',
      source: 'workers-deployments',
      observedAt: ATTESTED_AT,
    };

    // #when the deployment is provisioned
    const provision = provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
      clock: () => 1_000,
      routeAttestation: { convergenceBudgetMs: 1, initialRetryDelayMs: 1 },
    });

    // #then the ready commit refuses rather than recording a version this
    // deployment uploaded as the version it serves, and leaves the deployment
    // in the phase a retry resumes from
    const failure = await provision.catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(ProvisioningError);
    expect(String((failure as ProvisioningError).cause)).toMatch(
      /did not converge/,
    );
    expect(store.record?.phase).toBe('publishing');

    // #and once the route serves the deployed artifact, that is what commits
    backend.activeRoute = undefined;
    const settled = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
      clock: () => 1_000,
    });
    expect(settled.record).toMatchObject({
      phase: 'ready',
      artifactVersion: 'artifact-v3',
    });
  });

  it('provisions a migration target already locked, exactly once', async () => {
    // The capability the fence exists for: a deployment that will RECEIVE a
    // migration comes up unable to execute, so nothing starts on it between
    // provisioning and the operator's own reopen.
    const backend = new FakeBackend();
    const store = new MemoryStore();
    const result = await provisionDeployment({
      initialExecutionFenceState: 'migration-locked',
      backend,
      store,
      spec: spec(),
      secrets,
      clock: () => 1_000,
    });

    expect(result.record.phase).toBe('ready');
    expect(backend.seededFenceStates).toEqual(['migration-locked']);
  });

  it('rejects an illegal birth fence state before creating ANY resource', async () => {
    // #given — the protocol validates this too, but only when the seeding
    // statements are built, which is after `database-created`. Refusing at the
    // entry is what keeps a typo from costing a Worker and a D1 database first
    // and leaving a half-provisioned deployment behind. 'draining' and
    // 'proof-only' are real fence states but not coherent BIRTH states: they
    // are transitions out of something that already exists.
    for (const illegal of ['draining', 'proof-only', 'open ', '', undefined]) {
      const backend = new FakeBackend();
      const store = new MemoryStore();
      await expect(
        provisionDeployment({
          initialExecutionFenceState:
            illegal as unknown as InitialExecutionFenceState,
          backend,
          store,
          spec: spec(),
          secrets,
          clock: () => 1_000,
        }),
      ).rejects.toThrow(/initialExecutionFenceState must be one of/);
      // #then — no lease taken, no provider call made, nothing persisted.
      expect(store.leaseCalls).toBe(0);
      expect(backend.events).toEqual([]);
      expect(store.record).toBeUndefined();
    }
  });

  it('carries the requested fence state on a failed provisioning pass', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'migrations';
    const store = new MemoryStore();
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'migration-locked',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(backend.seededFenceStates).toEqual(['migration-locked']);
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
          initialExecutionFenceState: 'open',
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record).toMatchObject({
      phase: 'database-create-authorized',
      databaseName: deployment.databaseName,
      desiredSpecDigest: deploymentSpecDigest(deployment),
    });
    expect(backend.events).not.toContain('delete-database');

    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);

    backend.failAt = undefined;
    backend.databaseExists = true;
    backend.databaseOwner = undefined;
    backend.events.length = 0;
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).resolves.toBeUndefined();
    expect(backend.events).toEqual(['identity', 'delete-database']);
    // The freshness proof seeds the most restrictive fence it can: this
    // database is about to be deleted, and one that survives a failed delete
    // must never come back executing.
    expect(backend.seededFenceStates).toEqual(['migration-locked']);
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('database-create-authorized');

    backend.failAt = undefined;
    backend.databaseOwner = 'other-tenant';
    backend.events.length = 0;
    const failure = await provisionDeployment({
      initialExecutionFenceState: 'open',
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
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

  it('passes the persisted plain-Worker release through post-deploy rollback', async () => {
    const backend = new FakeBackend('plain-worker');
    backend.failAt = 'maintenance';
    const store = new MemoryStore();
    const deployment = spec();

    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);

    expect(backend.activeRelease).toEqual({
      physicalScriptName: deployment.scriptName,
      specDigest: deploymentSpecDigest(deployment),
      artifactVersion: 'artifact-v3',
      releaseSchemaVersion: deployment.schemaVersion,
      application: { vars: [], secrets: [], r2Buckets: [] },
    });
    expect(backend.events).toContain('revoke');
    expect(backend.events).toContain('delete-worker');
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
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
      initialExecutionFenceState: 'open',
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
      initialExecutionFenceState: 'open',
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: external,
        secrets,
      }),
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: external,
        secrets,
      }),
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
        initialExecutionFenceState: 'open',
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
        initialExecutionFenceState: 'open',
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
        initialExecutionFenceState: 'open',
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
          initialExecutionFenceState: 'open',
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
      initialExecutionFenceState: 'open',
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: external,
        secrets,
      }),
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: external,
      secrets,
    });
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
    backend.live = completeLiveDeployment({
      ...backend.live,
      durableObjectBindings: backend.live.durableObjectBindings.map(
        (binding) => ({ ...binding, scriptName: current.scriptName }),
      ),
    });
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
        initialExecutionFenceState: 'open',
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
    backend.live = completeLiveDeployment({
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
    });
    backend.databaseExists = true;
    backend.databaseOwner = activeSpec.tenantTag;

    const result = await provisionDeployment({
      initialExecutionFenceState: 'open',
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: deployment,
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: deployment,
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: deployment,
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: deployment,
      secrets,
    });
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: deployment,
        secrets,
      }),
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: deployment,
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: deployment,
      secrets,
    });
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
      initialExecutionFenceState: 'open',
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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
      initialExecutionFenceState: 'open',
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('database-created');

    backend.failAt = undefined;
    backend.cleanupFailAt = undefined;
    backend.events.length = 0;
    backend.databaseIdsRead.length = 0;
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });
    expect(backend.events).not.toContain('database');
    expect(backend.events).toContain('identity');
    expect(backend.databaseIdsRead).toContain('database-id');
  });

  it('does not delete a database after an export without integrity evidence', async () => {
    const backend = new FakeBackend();
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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

  it('force-decommissions a deployment without a specification and removes its ledger row', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    backend.events.length = 0;
    const auditEvents: unknown[] = [];

    await expect(
      forceDecommissionDeployment({
        backend,
        store,
        tenantTag: 'acme',
        environment: 'production',
        options: {
          audit: (event) => {
            auditEvents.push(event);
          },
        },
      }),
    ).resolves.toBeUndefined();

    expect(backend.forceSteps).toEqual([
      'remove-traffic',
      'revoke-credentials',
      'delete-database',
    ]);
    expect(backend.events).toEqual([]);
    expect(store.record).toBeUndefined();
    await expect(store.get()).resolves.toBeUndefined();
    expect(auditEvents).toEqual([
      expect.objectContaining({
        action: 'deployment-decommissioned',
        tenantTag: 'acme',
        environment: 'production',
        forced: true,
      }),
    ]);
  });

  it('clears a pre-authorization database reservation without provider mutation', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    backend.findDatabase = async () => {
      throw new Error('D1 lookup unavailable before create authorization');
    };
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('database-reserved');
    backend.events.length = 0;

    await expect(
      forceDecommissionDeployment({
        backend,
        store,
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).resolves.toBeUndefined();
    expect(backend.events).toEqual([]);
    expect(backend.forceSteps).toEqual([]);
    expect(store.record).toBeUndefined();
  });

  it('retains an unresolved create authorization after D1 creation loses its response', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    backend.failAt = 'database';
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record).toMatchObject({
      phase: 'database-create-authorized',
      databaseId: expect.stringMatching(/^reserved-/u),
    });
    expect(backend.databaseExists).toBe(true);
    backend.failAt = undefined;
    backend.events.length = 0;

    await expect(
      forceDecommissionDeployment({
        backend,
        store,
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).rejects.toThrow(/exact D1 creation outcome is unresolved/);
    expect(store.record).toMatchObject({
      phase: 'database-create-authorized',
      databaseId: expect.stringMatching(/^reserved-/u),
    });
    expect(backend.databaseExists).toBe(true);
    expect(backend.events).toEqual([]);
    expect(backend.forceSteps).toEqual([]);
  });

  it('leases absent and terminal deployments before no-op success', async () => {
    const backend = new FakeBackend();
    const absentStore = new MemoryStore();

    await expect(
      forceDecommissionDeployment({
        backend,
        store: absentStore,
        tenantTag: 'absent',
        environment: 'production',
      }),
    ).resolves.toBeUndefined();
    expect(absentStore.leaseCalls).toBe(1);
    expect(backend.forceSteps).toEqual([]);

    const terminalStore = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store: terminalStore,
      spec: spec(),
      secrets,
    });
    const auditFlags: boolean[] = [];
    await decommissionDeployment({
      backend,
      store: terminalStore,
      spec: spec(),
      audit: (event) => {
        auditFlags.push(event.forced);
      },
    });
    Object.defineProperty(backend, 'forceDecommissionStep', {
      value: undefined,
    });
    const priorLeaseCalls = terminalStore.leaseCalls;

    await expect(
      forceDecommissionDeployment({
        backend,
        store: terminalStore,
        tenantTag: 'acme',
        environment: 'production',
        options: {
          audit: (event) => {
            auditFlags.push(event.forced);
          },
        },
      }),
    ).resolves.toBeUndefined();
    expect(terminalStore.leaseCalls).toBe(priorLeaseCalls + 1);
    expect(terminalStore.record).toBeUndefined();
    expect(backend.forceSteps).toEqual([]);
    expect(auditFlags).toEqual([false]);
  });

  it('uses one audit event shape for normal and forced decommission', async () => {
    const normalBackend = new FakeBackend();
    const normalStore = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend: normalBackend,
      store: normalStore,
      spec: spec(),
      secrets,
    });
    const forceBackend = new FakeBackend('plain-worker');
    const forceStore = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend: forceBackend,
      store: forceStore,
      spec: spec(),
      secrets,
    });
    const auditEvents: Array<Record<string, unknown>> = [];

    await decommissionDeployment({
      backend: normalBackend,
      store: normalStore,
      spec: spec(),
      audit: (event) => {
        auditEvents.push(event as unknown as Record<string, unknown>);
      },
    });
    await forceDecommissionDeployment({
      backend: forceBackend,
      store: forceStore,
      tenantTag: 'acme',
      environment: 'production',
      options: {
        audit: (event) => {
          auditEvents.push(event as unknown as Record<string, unknown>);
        },
      },
    });

    expect(auditEvents).toHaveLength(2);
    expect(Object.keys(auditEvents[0] ?? {}).sort()).toEqual(
      Object.keys(auditEvents[1] ?? {}).sort(),
    );
    expect(auditEvents.map(({ forced }) => forced)).toEqual([false, true]);
  });

  it('re-enters a force decommission wedged after traffic removal', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    backend.forceFailOnceAt = 'revoke-credentials';

    await expect(
      forceDecommissionDeployment({
        backend,
        store,
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).rejects.toThrow(/failed force step revoke-credentials/);
    expect(store.record?.phase).toBe('traffic-removed');

    await expect(
      forceDecommissionDeployment({
        backend,
        store,
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).resolves.toBeUndefined();
    expect(backend.forceSteps).toEqual([
      'remove-traffic',
      'revoke-credentials',
      'remove-traffic',
      'revoke-credentials',
      'delete-database',
    ]);
    expect(store.record).toBeUndefined();
  });

  it('converges after D1 deletion succeeds but the terminal state write fails', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    store.failPutPhase = 'decommissioned';

    await expect(
      forceDecommissionDeployment({
        backend,
        store,
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).rejects.toThrow(/failed state write at decommissioned/);
    expect(store.record?.phase).toBe('database-deleting');
    expect(backend.databaseExists).toBe(false);

    await expect(
      forceDecommissionDeployment({
        backend,
        store,
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).resolves.toBeUndefined();
    expect(
      backend.forceSteps.filter((step) => step === 'delete-database'),
    ).toHaveLength(2);
    expect(store.record).toBeUndefined();
  });

  it('serializes force decommission against concurrent provisioning', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    let releaseStep: (() => void) | undefined;
    let stepStarted: (() => void) | undefined;
    const started = new Promise<void>((resolve) => {
      stepStarted = resolve;
    });
    backend.forceStepStarted = () => stepStarted?.();
    backend.forceStepGate = new Promise<void>((resolve) => {
      releaseStep = resolve;
    });

    const force = forceDecommissionDeployment({
      backend,
      store,
      tenantTag: 'acme',
      environment: 'production',
    });
    await started;
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toThrow(/already being modified/);
    releaseStep?.();
    await expect(force).resolves.toBeUndefined();
  });

  it('silently removes a completed force teardown after audit delivery fails', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    let auditAttempts = 0;
    const input = {
      backend,
      store,
      tenantTag: 'acme',
      environment: 'production',
      options: {
        audit: () => {
          auditAttempts += 1;
          if (auditAttempts === 1) throw new Error('audit unavailable');
        },
      },
    } as const;

    await expect(forceDecommissionDeployment(input)).rejects.toThrow(
      /audit unavailable/,
    );
    expect(store.record?.phase).toBe('decommissioned');
    await expect(forceDecommissionDeployment(input)).resolves.toBeUndefined();
    expect(backend.forceSteps).toEqual([
      'remove-traffic',
      'revoke-credentials',
      'delete-database',
    ]);
    expect(store.record).toBeUndefined();
    expect(auditAttempts).toBe(1);
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
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    await Promise.resolve();
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    expect(store.record?.phase).toBe('worker-deployed');

    backend.failAt = undefined;
    backend.cleanupFailAt = undefined;
    backend.live = undefined;
    backend.events.length = 0;
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
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
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toBeInstanceOf(ProvisioningError);
    backend.failAt = undefined;
    backend.cleanupFailAt = undefined;
    backend.events.length = 0;

    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
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
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
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

  it('rolls back a Worker this attempt created when upload scratch cleanup fails', async () => {
    const deployment = spec({
      egressProxyService: undefined,
    });
    const { backend, store, runnerCalls, state } =
      await wranglerLoopHarness(deployment);
    fsControl.failFleetCleanup = true;

    const failure = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: deployment,
      secrets,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as ProvisioningError).cause).toMatchObject({
      message: expect.stringContaining(
        'but failed to clean up the adapter credential scratch',
      ),
      createdByAttempt: true,
      resourceState: 'present',
    });
    expect(state.workerExists).toBe(false);
    expect(state.databaseExists).toBe(false);
    expect(store.record).toBeUndefined();
    expect(runnerCalls.some(([command]) => command === 'delete')).toBe(true);
  });

  it('leaves a pre-existing Worker in place when upload scratch cleanup fails', async () => {
    const deployment = spec({
      egressProxyService: undefined,
    });
    const { backend, store, runnerCalls, state } =
      await wranglerLoopHarness(deployment);
    state.workerExists = true;
    state.preexistingVersion = true;
    state.publicAccessEnabled = true;
    fsControl.failFleetCleanup = true;

    const failure = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: deployment,
      secrets,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as ProvisioningError).cause).toMatchObject({
      message: expect.stringContaining(
        'but failed to clean up the adapter credential scratch',
      ),
      createdByAttempt: false,
      resourceState: 'present',
    });
    expect(runnerCalls.some(([command]) => command === 'delete')).toBe(false);
    expect(state.workerExists).toBe(true);
    expect(state.databaseExists).toBe(true);
    expect(store.record).toBeUndefined();
  });

  it('starts stable operation before I/O and recovers lost start response', async () => {
    const store = new CommitThenThrowStore();
    const harness = await boundedDecommissionHarness({ store });
    store.failAfterCommittedPhase = 'decommission-advancing';
    const randomUUID = vi.fn(() => DECOMMISSION_OPERATION_ID);
    const clock = vi.fn(() => Date.parse('2026-08-30T01:00:00.000Z'));
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          harness,
          { kind: 'start' },
          { randomUUID, clock },
        ),
      ),
    ).rejects.toThrow(/response was lost/u);
    expect(store.record?.decommissionIntent).toMatchObject({
      operationId: DECOMMISSION_OPERATION_ID,
      revision: 0,
      generation: 0,
      state: 'transitioning',
    });
    expect(randomUUID).toHaveBeenCalledTimes(1);
    expect(clock).toHaveBeenCalledTimes(1);

    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          harness,
          { kind: 'start' },
          { randomUUID, clock },
        ),
      ),
    ).resolves.toMatchObject({ status: 'pending', token: { revision: 0 } });
    expect(randomUUID).toHaveBeenCalledTimes(1);
    expect(clock).toHaveBeenCalledTimes(1);

    const missingUuid = await boundedDecommissionHarness();
    const missingUuidRecord = missingUuid.store.record;
    const missingUuidLeases = missingUuid.store.leaseCalls;
    await expect(
      advanceDecommissionDeployment({
        ...boundedAdvanceOptions(missingUuid, { kind: 'start' }),
        randomUUID: undefined as never,
      }),
    ).rejects.toThrow(
      'advanceDecommissionDeployment requires a randomUUID function',
    );
    expect(missingUuid.store.record).toBe(missingUuidRecord);
    expect(missingUuid.store.leaseCalls).toBe(missingUuidLeases);
    const invalidUuid = await boundedDecommissionHarness();
    const invalidUuidRecord = invalidUuid.store.record;
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          invalidUuid,
          { kind: 'start' },
          {
            randomUUID: () => 'not-a-uuid',
          },
        ),
      ),
    ).rejects.toThrow('decommission advance token is malformed');
    expect(invalidUuid.store.record).toBe(invalidUuidRecord);

    const throwingUuid = await boundedDecommissionHarness();
    const uuidFailure = new Error('uuid source failed');
    const throwingUuidRecord = throwingUuid.store.record;
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          throwingUuid,
          { kind: 'start' },
          {
            randomUUID() {
              throw uuidFailure;
            },
          },
        ),
      ),
    ).rejects.toBe(uuidFailure);
    expect(throwingUuid.store.record).toBe(throwingUuidRecord);

    const unsupported = await boundedDecommissionHarness();
    unsupported.store.record = {
      ...(unsupported.store.record as FleetRecord),
      phase: 'decommissioned',
    };
    const unsupportedRecord = unsupported.store.record;
    const unsupportedEvents = [...unsupported.backend.events];
    const unsupportedUuid = vi.fn(() => DECOMMISSION_OPERATION_ID);
    const unsupportedClock = vi.fn(() => Date.now());
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          unsupported,
          { kind: 'start' },
          { randomUUID: unsupportedUuid, clock: unsupportedClock },
        ),
      ),
    ).rejects.toThrow(
      "cannot start bounded decommission in phase 'decommissioned'",
    );
    expect(unsupportedUuid).not.toHaveBeenCalled();
    expect(unsupportedClock).not.toHaveBeenCalled();
    expect(unsupported.store.record).toBe(unsupportedRecord);
    expect(unsupported.backend.events).toEqual(unsupportedEvents);
    expect(unsupported.backend.databaseIdsRead).toHaveLength(0);
  });

  it('invalid budgets and missing capabilities refuse before the first write', async () => {
    const invalid = await boundedDecommissionHarness();
    const leases = invalid.store.leaseCalls;
    for (const maxProviderRequests of [8, 1_001, 9.5]) {
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(
            invalid,
            { kind: 'start' },
            { maxProviderRequests },
          ),
        ),
      ).rejects.toThrow(
        'maxProviderRequests must be an integer from 9 to 1000',
      );
    }
    expect(invalid.store.leaseCalls).toBe(leases);

    const rows = [
      [
        'advanceDecommissionAttachmentScan',
        'attachment-scan',
        'backend cannot perform bounded decommission attachment scans',
      ],
      [
        'assertDatabaseDeletionResidualsRemoved',
        'database-residuals',
        'backend cannot inspect database deletion residuals',
      ],
      [
        'findApplicationR2Bucket',
        'application-r2-inspection',
        'backend cannot inspect application R2 resources',
      ],
      [
        'assertApplicationR2Empty',
        'application-r2-empty',
        'backend cannot attest application R2 emptiness',
      ],
      [
        'deleteApplicationR2Bucket',
        'application-r2-delete',
        'backend cannot delete application R2 resources',
      ],
    ] as const;
    for (const [property, capability, message] of rows) {
      const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
      Object.defineProperty(harness.backend, property, {
        configurable: true,
        value: undefined,
      });
      const before = harness.store.record;
      const failure = await advanceDecommissionDeployment(
        boundedAdvanceOptions(harness, { kind: 'start' }),
      ).catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(DecommissionAdvanceCapabilityError);
      if (!(failure instanceof DecommissionAdvanceCapabilityError)) {
        throw new Error('missing typed decommission capability refusal');
      }
      expect(Object.getPrototypeOf(failure)).toBe(
        DecommissionAdvanceCapabilityError.prototype,
      );
      expect(failure).toMatchObject({
        name: 'DecommissionAdvanceCapabilityError',
        message,
        capability,
      });
      expect(harness.store.record).toBe(before);
    }

    const currentScan = await boundedDecommissionHarness({
      r2Names: ['FILES'],
    });
    const discover = await driveBoundedUntil(
      currentScan,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    Object.defineProperty(
      currentScan.backend,
      'advanceDecommissionAttachmentScan',
      { configurable: true, value: undefined },
    );
    const snapshot = currentScan.store.record;
    await expect(
      continueBoundedDecommission(currentScan, discover),
    ).rejects.toMatchObject({ capability: 'attachment-scan' });
    expect(currentScan.store.record).toBe(snapshot);

    const currentResidual = await boundedDecommissionHarness();
    const credentials = await driveBoundedUntil(
      currentResidual,
      (record) =>
        record.decommissionIntent?.lifecyclePhase === 'credentials-revoked',
    );
    Object.defineProperty(
      currentResidual.backend,
      'assertDatabaseDeletionResidualsRemoved',
      { configurable: true, value: undefined },
    );
    const reads = currentResidual.backend.databaseIdsRead.length;
    await expect(
      continueBoundedDecommission(currentResidual, credentials),
    ).rejects.toMatchObject({ capability: 'database-residuals' });
    expect(currentResidual.backend.databaseIdsRead).toHaveLength(reads);

    const accessorHarness = await boundedDecommissionHarness({
      r2Names: ['FILES'],
    });
    const findImplementation = accessorHarness.backend.findApplicationR2Bucket;
    const emptyImplementation =
      accessorHarness.backend.assertApplicationR2Empty;
    let findReads = 0;
    let emptyReads = 0;
    Object.defineProperty(accessorHarness.backend, 'findApplicationR2Bucket', {
      configurable: true,
      get() {
        findReads += 1;
        return findImplementation;
      },
    });
    Object.defineProperty(accessorHarness.backend, 'assertApplicationR2Empty', {
      configurable: true,
      get() {
        emptyReads += 1;
        return emptyImplementation;
      },
    });
    const accessorStart = await startBoundedDecommission(accessorHarness);
    expect(findReads).toBe(1);
    expect(emptyReads).toBe(1);
    await continueBoundedDecommission(accessorHarness, accessorStart);
    expect(findReads).toBe(2);
    expect(emptyReads).toBe(2);

    for (const [state, property, capability] of [
      ['detached', 'assertApplicationR2Empty', 'application-r2-empty'],
      ['empty', 'deleteApplicationR2Bucket', 'application-r2-delete'],
    ] as const) {
      const branch = await boundedDecommissionHarness({ r2Names: ['FILES'] });
      const current = await driveBoundedUntil(
        branch,
        (record) => record.applicationResources?.[0]?.state === state,
      );
      Object.defineProperty(branch.backend, property, {
        configurable: true,
        value: undefined,
      });
      const before = branch.store.record;
      await expect(
        continueBoundedDecommission(branch, current),
      ).rejects.toMatchObject({ capability });
      expect(branch.store.record).toBe(before);
    }

    const exactVerify = await boundedDecommissionHarness({
      r2Names: ['FILES'],
    });
    const exactDiscover = await driveBoundedUntil(
      exactVerify,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const exactToken = await continueBoundedDecommission(
      exactVerify,
      exactDiscover,
    );
    const exactFind = exactVerify.backend.findApplicationR2Bucket;
    let exactFindReads = 0;
    Object.defineProperty(exactVerify.backend, 'findApplicationR2Bucket', {
      configurable: true,
      get() {
        exactFindReads += 1;
        return exactFind;
      },
    });
    await expect(
      continueBoundedDecommission(exactVerify, exactToken),
    ).resolves.toMatchObject({ status: 'pending' });
    expect(exactFindReads).toBe(1);
    expect(exactVerify.store.record?.applicationResources?.[0]?.state).toBe(
      'detached',
    );

    const deletedPrefix = await boundedDecommissionHarness({
      r2Names: ['FILES'],
    });
    await driveBoundedUntil(
      deletedPrefix,
      (record) => record.applicationResources?.[0]?.state === 'deleted',
    );
    const deletedCurrent = await startBoundedDecommission(deletedPrefix);
    Object.defineProperty(deletedPrefix.backend, 'findApplicationR2Bucket', {
      configurable: true,
      value: undefined,
    });
    await expect(
      continueBoundedDecommission(deletedPrefix, deletedCurrent),
    ).rejects.toMatchObject({ capability: 'application-r2-inspection' });

    for (const lifecyclePhase of ['ready', 'traffic-removed'] as const) {
      const deletedOnly = await boundedDecommissionHarness({
        r2Names: ['FILES'],
      });
      const resource = deletedOnly.store.record?.applicationResources?.[0];
      if (!resource) throw new Error('missing deleted-only R2 resource');
      deletedOnly.backend.buckets.delete(resource.bucketName);
      deletedOnly.store.record = {
        ...(deletedOnly.store.record as FleetRecord),
        phase: lifecyclePhase,
        applicationResources: [{ ...resource, state: 'deleted' }],
      };
      if (lifecyclePhase === 'traffic-removed') {
        deletedOnly.backend.trafficRemoved = true;
      }
      Object.defineProperties(deletedOnly.backend, {
        assertApplicationR2Empty: {
          configurable: true,
          value: undefined,
        },
        deleteApplicationR2Bucket: {
          configurable: true,
          value: undefined,
        },
      });
      const initialFinds = deletedOnly.backend.r2FindCalls;
      const initialEvents = deletedOnly.backend.events.length;
      const initialDatabaseReads = deletedOnly.backend.databaseIdsRead.length;
      const initialTrafficChecks =
        deletedOnly.backend.assertTrafficRemovedCalls;
      const initialWrites = deletedOnly.store.phases.length;
      const started = await startBoundedDecommission(deletedOnly);
      expect(deletedOnly.backend.r2FindCalls, lifecyclePhase).toBe(
        initialFinds,
      );
      expect(deletedOnly.backend.events, lifecyclePhase).toHaveLength(
        initialEvents,
      );
      expect(deletedOnly.backend.databaseIdsRead, lifecyclePhase).toHaveLength(
        initialDatabaseReads,
      );
      expect(
        deletedOnly.backend.assertTrafficRemovedCalls,
        lifecyclePhase,
      ).toBe(initialTrafficChecks);
      expect(deletedOnly.store.phases, lifecyclePhase).toHaveLength(
        initialWrites + 1,
      );

      await continueBoundedDecommission(deletedOnly, started);
      expect(
        deletedOnly.store.record?.decommissionIntent?.lifecyclePhase,
        lifecyclePhase,
      ).toBe(
        lifecyclePhase === 'ready' ? 'decommissioning' : 'credentials-revoked',
      );
      expect(
        deletedOnly.backend.r2FindNames.slice(initialFinds),
        lifecyclePhase,
      ).toEqual(['FILES']);
      expect(
        deletedOnly.backend.events.slice(initialEvents),
        lifecyclePhase,
      ).toEqual(lifecyclePhase === 'ready' ? [] : ['revoke']);
      expect(
        deletedOnly.backend.databaseIdsRead.length - initialDatabaseReads,
        lifecyclePhase,
      ).toBe(lifecyclePhase === 'ready' ? 0 : 1);
      expect(
        deletedOnly.backend.assertTrafficRemovedCalls - initialTrafficChecks,
        lifecyclePhase,
      ).toBe(lifecyclePhase === 'ready' ? 0 : 1);
      expect(deletedOnly.store.phases, lifecyclePhase).toHaveLength(
        initialWrites + 2,
      );
    }

    for (const state of ['reserved', 'create-authorized'] as const) {
      const incomplete = await boundedDecommissionHarness({
        r2Names: ['FILES'],
      });
      const resource = incomplete.store.record?.applicationResources?.[0];
      if (!resource) throw new Error('missing incomplete R2 resource');
      const { creationDate: _creationDate, ...withoutCreation } = resource;
      incomplete.store.record = {
        ...(incomplete.store.record as FleetRecord),
        applicationResources: [{ ...withoutCreation, state }],
      };
      const uuid = vi.fn(() => DECOMMISSION_OPERATION_ID);
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(
            incomplete,
            { kind: 'start' },
            {
              randomUUID: uuid,
            },
          ),
        ),
      ).rejects.toThrow(/incomplete application R2 reservation/u);
      expect(uuid).not.toHaveBeenCalled();
    }

    for (const lifecyclePhase of ['ready', 'traffic-removed'] as const) {
      for (const state of ['reserved', 'create-authorized'] as const) {
        const incomplete = await boundedDecommissionHarness({
          r2Names: ['FILES'],
        });
        const current =
          lifecyclePhase === 'ready'
            ? await startBoundedDecommission(incomplete)
            : await driveBoundedUntil(
                incomplete,
                (record) =>
                  record.decommissionIntent?.lifecyclePhase ===
                  'traffic-removed',
              );
        const resource = incomplete.store.record?.applicationResources?.[0];
        if (!resource)
          throw new Error('missing current incomplete R2 resource');
        const { creationDate: _creationDate, ...reservation } = resource;
        incomplete.store.record = {
          ...(incomplete.store.record as FleetRecord),
          applicationResources: [{ ...reservation, state }],
        };
        const capabilityImplementations = {
          advanceDecommissionAttachmentScan:
            incomplete.backend.advanceDecommissionAttachmentScan,
          findApplicationR2Bucket: incomplete.backend.findApplicationR2Bucket,
          assertApplicationR2Empty: incomplete.backend.assertApplicationR2Empty,
          deleteApplicationR2Bucket:
            incomplete.backend.deleteApplicationR2Bucket,
        };
        const capabilityReads: string[] = [];
        for (const property of Object.keys(
          capabilityImplementations,
        ) as (keyof typeof capabilityImplementations)[]) {
          Object.defineProperty(incomplete.backend, property, {
            configurable: true,
            get() {
              capabilityReads.push(property);
              return capabilityImplementations[property];
            },
          });
        }
        const before = JSON.stringify(incomplete.store.record);
        const r2Finds = incomplete.backend.r2FindCalls;
        const scans = incomplete.backend.scanInputs.length;
        const emptyChecks = incomplete.backend.emptyChecks;
        const deletes = incomplete.backend.deleteCalls;
        const databaseReads = incomplete.backend.databaseIdsRead.length;
        const trafficChecks = incomplete.backend.assertTrafficRemovedCalls;
        const writes = incomplete.store.phases.length;
        const events = [...incomplete.backend.events];
        const failure = await continueBoundedDecommission(
          incomplete,
          current,
        ).catch((error: unknown) => error);
        expect(failure, `${lifecyclePhase}:${state}`).toMatchObject({
          name: 'Error',
          message:
            'normal decommission cannot consume incomplete application R2 reservation',
        });
        expect(capabilityReads, `${lifecyclePhase}:${state}`).toEqual([]);
        expect(
          incomplete.backend.r2FindCalls,
          `${lifecyclePhase}:${state}`,
        ).toBe(r2Finds);
        expect(
          incomplete.backend.scanInputs,
          `${lifecyclePhase}:${state}`,
        ).toHaveLength(scans);
        expect(
          incomplete.backend.emptyChecks,
          `${lifecyclePhase}:${state}`,
        ).toBe(emptyChecks);
        expect(
          incomplete.backend.deleteCalls,
          `${lifecyclePhase}:${state}`,
        ).toBe(deletes);
        expect(
          incomplete.backend.databaseIdsRead,
          `${lifecyclePhase}:${state}`,
        ).toHaveLength(databaseReads);
        expect(
          incomplete.backend.assertTrafficRemovedCalls,
          `${lifecyclePhase}:${state}`,
        ).toBe(trafficChecks);
        expect(incomplete.backend.events, `${lifecyclePhase}:${state}`).toEqual(
          events,
        );
        expect(
          incomplete.store.phases,
          `${lifecyclePhase}:${state}`,
        ).toHaveLength(writes);
        expect(
          JSON.stringify(incomplete.store.record),
          `${lifecyclePhase}:${state}`,
        ).toBe(before);
      }
    }

    const platformRevoke = await boundedDecommissionHarness({
      kind: 'workers-for-platforms',
      external: true,
    });
    await driveBoundedUntil(
      platformRevoke,
      (record) =>
        record.decommissionIntent?.lifecyclePhase === 'worker-deleted',
    );
    const revokeImplementation =
      platformRevoke.backend.revokePlatformResourceCredentials;
    let revokeReads = 0;
    Object.defineProperty(
      platformRevoke.backend,
      'revokePlatformResourceCredentials',
      {
        configurable: true,
        get() {
          revokeReads += 1;
          return revokeImplementation;
        },
      },
    );
    await continueBoundedDecommission(
      platformRevoke,
      await startBoundedDecommission(platformRevoke),
    );
    expect(revokeReads).toBe(1);

    const platformDelete = await boundedDecommissionHarness({
      kind: 'workers-for-platforms',
      external: true,
    });
    await driveBoundedUntil(
      platformDelete,
      (record) =>
        record.decommissionIntent?.lifecyclePhase ===
        'platform-credentials-revoked',
    );
    const deleteImplementation = platformDelete.backend.deletePlatformResources;
    const residualImplementation =
      platformDelete.backend.assertDatabaseDeletionResidualsRemoved;
    let deleteReads = 0;
    let residualReads = 0;
    Object.defineProperty(platformDelete.backend, 'deletePlatformResources', {
      configurable: true,
      get() {
        deleteReads += 1;
        return deleteImplementation;
      },
    });
    Object.defineProperty(
      platformDelete.backend,
      'assertDatabaseDeletionResidualsRemoved',
      {
        configurable: true,
        get() {
          residualReads += 1;
          return residualImplementation;
        },
      },
    );
    await continueBoundedDecommission(
      platformDelete,
      await startBoundedDecommission(platformDelete),
    );
    expect(deleteReads).toBe(1);
    expect(residualReads).toBe(1);

    for (const [lifecyclePhase, property, message] of [
      [
        'worker-deleted',
        'revokePlatformResourceCredentials',
        'backend cannot revoke trusted platform resource credentials',
      ],
      [
        'platform-credentials-revoked',
        'deletePlatformResources',
        'backend cannot delete trusted platform resources',
      ],
    ] as const) {
      const malformed = await boundedDecommissionHarness({
        kind: 'workers-for-platforms',
        external: true,
      });
      const current = await driveBoundedUntil(
        malformed,
        (record) =>
          record.decommissionIntent?.lifecyclePhase === lifecyclePhase,
      );
      let getterReads = 0;
      Object.defineProperty(malformed.backend, property, {
        configurable: true,
        get() {
          getterReads += 1;
          return {};
        },
      });
      const before = malformed.store.record;
      const databaseReads = malformed.backend.databaseIdsRead.length;
      const events = [...malformed.backend.events];
      await expect(
        continueBoundedDecommission(malformed, current),
      ).rejects.toThrow(message);
      expect(getterReads).toBe(1);
      expect(malformed.backend.databaseIdsRead).toHaveLength(databaseReads);
      expect(malformed.backend.events).toEqual(events);
      expect(malformed.store.record).toBe(before);
    }
  });

  it('classifies token errors before I/O; stale is inert; current advances one group', async () => {
    const harness = await boundedDecommissionHarness();
    const getter = vi.fn(() => 'continue');
    const hostile: Record<string, unknown> = {};
    Object.defineProperty(hostile, 'kind', { enumerable: true, get: getter });
    const leases = harness.store.leaseCalls;
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(harness, hostile as never),
      ),
    ).rejects.toThrow('decommission advance action is malformed');
    expect(getter).not.toHaveBeenCalled();
    expect(harness.store.leaseCalls).toBe(leases);

    const tokenGetter = vi.fn(() => ({}));
    const hostileToken: Record<string, unknown> = { kind: 'continue' };
    Object.defineProperty(hostileToken, 'token', {
      enumerable: true,
      get: tokenGetter,
    });
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(harness, hostileToken as never),
      ),
    ).rejects.toThrow('decommission advance action is malformed');
    expect(tokenGetter).not.toHaveBeenCalled();
    expect(harness.store.leaseCalls).toBe(leases);

    const cyclicToken: Record<string, unknown> = {};
    cyclicToken.self = cyclicToken;
    let deepToken: unknown = 'leaf';
    for (let depth = 0; depth < 10; depth += 1) {
      deepToken = { next: deepToken };
    }
    for (const action of [
      { kind: 'start', extra: true },
      { kind: 'unknown' },
      { kind: 'continue' },
      { kind: 'continue', token: {}, extra: true },
      { kind: 'restart-blocked' },
      { kind: 'restart-blocked', token: {}, extra: true },
      { kind: 'continue', token: cyclicToken },
      { kind: 'continue', token: deepToken },
      { kind: 'continue', token: Array.from({ length: 40 }, () => 0) },
      { kind: 'continue', token: 'x'.repeat(2_049) },
    ]) {
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(harness, action as never),
        ),
      ).rejects.toThrow('decommission advance action is malformed');
      expect(harness.store.leaseCalls).toBe(leases);
    }

    const started = await startBoundedDecommission(harness);
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(harness, {
          kind: 'continue',
          token: { ...started.token, tenantTag: 'other' },
        }),
      ),
    ).rejects.toThrow('decommission advance token targets another deployment');
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(harness, {
          kind: 'continue',
          token: { ...started.token, operationId: crypto.randomUUID() },
        }),
      ),
    ).rejects.toThrow('decommission advance token targets another operation');
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(harness, {
          kind: 'continue',
          token: { ...started.token, revision: 99 },
        }),
      ),
    ).rejects.toThrow('decommission advance token is from the future');

    const advanced = await continueBoundedDecommission(harness, started);
    const events = [...harness.backend.events];
    await expect(
      continueBoundedDecommission(harness, started),
    ).resolves.toEqual(advanced);
    expect(harness.backend.events).toEqual(events);

    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          harness,
          { kind: 'start' },
          {
            spec: spec({
              modules: [{ name: 'worker.js', content: 'changed' }],
            }),
          },
        ),
      ),
    ).rejects.toThrow(/different|match/u);
    const replacementBackend = new R2RollbackBackend('workers-for-platforms');
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          harness,
          { kind: 'start' },
          { backend: replacementBackend },
        ),
      ),
    ).rejects.toThrow('decommission backend does not own this deployment');
    expect(replacementBackend.events).toHaveLength(0);

    const absent = await boundedDecommissionHarness();
    absent.store.record = undefined;
    await expect(startBoundedDecommission(absent)).rejects.toThrow(
      'deployment is not registered',
    );
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(absent, {
          kind: 'continue',
          token: started.token,
        }),
      ),
    ).rejects.toThrow('decommission advance token targets another operation');

    const intent = harness.store.record?.decommissionIntent;
    if (!intent) throw new Error('missing decommission intent');
    harness.store.record = {
      ...(harness.store.record as FleetRecord),
      phase: 'decommissioned',
      applicationResources: [],
      databaseExportLocation: 'r2://exports/db.sqlite',
      databaseExportSha256: 'a'.repeat(64),
      databaseExportSize: 32,
      decommissionIntent: {
        version: 1,
        operationId: intent.operationId,
        revision: 2,
        generation: intent.generation,
        updatedAt: intent.updatedAt,
        identity: intent.identity,
        lifecyclePhase: 'decommissioned',
        state: 'complete',
      },
    };
    const terminalRecord = harness.store.record as FleetRecord;
    const currentToken = { ...started.token, revision: 2 };
    const expectedComplete = {
      status: 'complete',
      token: currentToken,
      result: {
        record: terminalRecord,
        databaseExport: {
          databaseId: terminalRecord.databaseId,
          location: 'r2://exports/db.sqlite',
          sha256: 'a'.repeat(64),
          size: 32,
        },
      },
    } as const;
    const terminalEvents = [...harness.backend.events];
    const terminalWrites = harness.store.phases.length;
    const terminalUuid = vi.fn(() => crypto.randomUUID());
    const terminalClock = vi.fn(() => Date.now());
    for (const action of [
      { kind: 'continue', token: currentToken },
      { kind: 'continue', token: { ...currentToken, revision: 1 } },
      { kind: 'start' },
    ] as const) {
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(harness, action, {
            randomUUID: terminalUuid,
            clock: terminalClock,
          }),
        ),
      ).resolves.toEqual(expectedComplete);
    }
    expect(terminalUuid).not.toHaveBeenCalled();
    expect(terminalClock).not.toHaveBeenCalled();
    expect(harness.backend.events).toEqual(terminalEvents);
    expect(harness.store.phases).toHaveLength(terminalWrites);
  });

  it('a duplicate current token becomes stale after one lifecycle group', async () => {
    const harness = await boundedDecommissionHarness();
    let tick = Date.parse('2026-08-30T02:00:00.000Z');
    const clock = vi.fn(() => (tick += 1_000));
    const started = await advanceDecommissionDeployment(
      boundedAdvanceOptions(harness, { kind: 'start' }, { clock }),
    );
    expect(clock).toHaveBeenCalledTimes(1);
    const first = await advanceDecommissionDeployment(
      boundedAdvanceOptions(
        harness,
        { kind: 'continue', token: started.token },
        { clock },
      ),
    );
    expect(first).toMatchObject({ status: 'pending', token: { revision: 1 } });
    expect(clock).toHaveBeenCalledTimes(2);
    const phase = harness.store.record?.decommissionIntent?.lifecyclePhase;
    const duplicate = await advanceDecommissionDeployment(
      boundedAdvanceOptions(
        harness,
        { kind: 'continue', token: started.token },
        { clock },
      ),
    );
    expect(duplicate).toEqual(first);
    expect(clock).toHaveBeenCalledTimes(2);
    expect(harness.store.record?.decommissionIntent?.lifecyclePhase).toBe(
      phase,
    );
  });

  it('persists exact progress budget signal and scan-only timestamps', async () => {
    const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    const atDiscover = await driveBoundedUntil(
      harness,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const before = harness.store.record as FleetRecord;
    const scan = before.decommissionIntent;
    if (scan?.state !== 'discover')
      throw new Error('missing discover progress');
    harness.backend.scanResults.push({
      status: 'pending',
      progress: scan.progress,
      providerFetchAttemptsReserved: 12,
    });
    const controller = new AbortController();
    const next = await advanceDecommissionDeployment(
      boundedAdvanceOptions(
        harness,
        { kind: 'continue', token: atDiscover.token },
        { signal: controller.signal },
      ),
    );
    expect(harness.backend.scanInputs.at(-1)).toMatchObject({
      progress: scan.progress,
      maxProviderRequests: 12,
      signal: controller.signal,
    });
    expect(harness.store.record?.updatedAt).toBe(before.updatedAt);
    expect(next.token.revision).toBe(atDiscover.token.revision + 1);

    const sentinel = new Error('scan transport failed');
    harness.backend.scanFailure = sentinel;
    const snapshot = harness.store.record;
    await expect(continueBoundedDecommission(harness, next)).rejects.toBe(
      sentinel,
    );
    expect(harness.store.record).toBe(snapshot);

    const aborted = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    const abortedDiscover = await driveBoundedUntil(
      aborted,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const abortedController = new AbortController();
    const abortReason = new Error('scan aborted by caller');
    abortedController.abort(abortReason);
    aborted.backend.scanFailure = abortReason;
    const abortedSnapshot = aborted.store.record;
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          aborted,
          { kind: 'continue', token: abortedDiscover.token },
          { signal: abortedController.signal },
        ),
      ),
    ).rejects.toBe(abortReason);
    expect(aborted.backend.scanInputs.at(-1)?.signal).toBe(
      abortedController.signal,
    );
    expect(aborted.backend.scanInputs.at(-1)?.signal?.aborted).toBe(true);
    expect(aborted.backend.scanInputs.at(-1)?.signal?.reason).toBe(abortReason);
    expect(aborted.store.record).toBe(abortedSnapshot);
  });

  it('discover completion starts a fresh verify pass without an action', async () => {
    const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    const discover = await driveBoundedUntil(
      harness,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const resourceBefore = harness.store.record?.applicationResources?.[0];
    const verified = await continueBoundedDecommission(harness, discover);
    expect(harness.store.record?.decommissionIntent).toMatchObject({
      state: 'verify',
      discoverEvidence: {
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 2,
      },
    });
    expect(harness.store.record?.applicationResources?.[0]).toEqual(
      resourceBefore,
    );
    expect(verified.status).toBe('pending');
  });

  it('matching verify atomically detaches and consumes scan payload', async () => {
    const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    const discover = await driveBoundedUntil(
      harness,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const verify = await continueBoundedDecommission(harness, discover);
    await continueBoundedDecommission(harness, verify);
    expect(harness.store.record?.applicationResources?.[0]?.state).toBe(
      'detached',
    );
    expect(harness.store.record?.decommissionIntent).toMatchObject({
      state: 'transitioning',
    });
    expect(harness.store.record?.decommissionIntent).not.toHaveProperty(
      'discoverEvidence',
    );
  });

  it('drift and independent digest or count mismatch restart discovery', async () => {
    for (const mode of ['drift', 'digest', 'count'] as const) {
      const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
      const discover = await driveBoundedUntil(
        harness,
        (record) => record.decommissionIntent?.state === 'discover',
      );
      const initialGeneration =
        harness.store.record?.decommissionIntent?.generation ?? -1;
      if (mode === 'drift') {
        harness.backend.scanResults.push({ status: 'drift' });
        await continueBoundedDecommission(harness, discover);
      } else {
        const verify = await continueBoundedDecommission(harness, discover);
        harness.backend.scanResults.push({
          status: 'complete',
          evidenceSha256: mode === 'digest' ? 'b'.repeat(64) : 'a'.repeat(64),
          evidenceCount: mode === 'count' ? 3 : 2,
          providerFetchAttemptsReserved: 3,
        });
        await continueBoundedDecommission(harness, verify);
      }
      expect(harness.store.record?.decommissionIntent).toMatchObject({
        state: 'discover',
        generation: initialGeneration + 1,
      });
    }
  });

  it('blocks from either pass and only an explicit current restart proceeds', async () => {
    for (const pass of ['discover', 'verify'] as const) {
      const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
      let result = await driveBoundedUntil(
        harness,
        (record) => record.decommissionIntent?.state === 'discover',
      );
      if (pass === 'verify')
        result = await continueBoundedDecommission(harness, result);
      harness.backend.scanResults.push({
        status: 'attached',
        attachment: { plane: 'ordinary', scriptName: 'consumer' },
        providerFetchAttemptsReserved: 3,
      });
      const blocked = await continueBoundedDecommission(harness, result);
      expect(blocked).toMatchObject({
        status: 'blocked',
        attachment: { plane: 'ordinary', scriptName: 'consumer' },
      });
      const calls = harness.backend.scanInputs.length;
      await expect(
        continueBoundedDecommission(harness, blocked),
      ).resolves.toEqual(blocked);
      expect(harness.backend.scanInputs).toHaveLength(calls);
      const restarted = await advanceDecommissionDeployment(
        boundedAdvanceOptions(harness, {
          kind: 'restart-blocked',
          token: blocked.token,
        }),
      );
      expect(restarted.status).toBe('pending');
      expect(harness.store.record?.decommissionIntent?.state).toBe('discover');
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(harness, {
            kind: 'restart-blocked',
            token: restarted.token,
          }),
        ),
      ).rejects.toBeInstanceOf(DecommissionAdvanceRestartError);
    }

    const prefixed = await boundedDecommissionHarness({
      r2Names: ['ARCHIVE', 'FILES'],
    });
    const secondDiscover = await driveBoundedUntil(
      prefixed,
      (record) =>
        record.decommissionIntent?.state === 'discover' &&
        record.decommissionIntent.purpose.kind === 'application-r2-detach' &&
        record.decommissionIntent.purpose.resourceIndex === 1,
    );
    prefixed.backend.scanResults.push({
      status: 'attached',
      attachment: { plane: 'ordinary', scriptName: 'consumer' },
      providerFetchAttemptsReserved: 3,
    });
    const secondBlocked = await continueBoundedDecommission(
      prefixed,
      secondDiscover,
    );
    const prior = prefixed.store.record?.applicationResources?.[0];
    if (!prior?.creationDate)
      throw new Error('missing deleted prefix identity');
    prefixed.backend.buckets.set(prior.bucketName, {
      name: prior.name,
      bucketName: prior.bucketName,
      jurisdiction: prior.jurisdiction,
      creationDate: prior.creationDate,
    });
    const prefixFindImplementation = prefixed.backend.findApplicationR2Bucket;
    let prefixFindGetterReads = 0;
    Object.defineProperty(prefixed.backend, 'findApplicationR2Bucket', {
      configurable: true,
      get() {
        prefixFindGetterReads += 1;
        return prefixFindImplementation;
      },
    });
    const prefixSnapshot = prefixed.store.record;
    const prefixScans = prefixed.backend.scanInputs.length;
    const prefixFinds = prefixed.backend.r2FindNames.length;
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(prefixed, {
          kind: 'restart-blocked',
          token: secondBlocked.token,
        }),
      ),
    ).rejects.toThrow(/reappeared after deletion/u);
    expect(prefixed.store.record).toBe(prefixSnapshot);
    expect(prefixed.backend.scanInputs).toHaveLength(prefixScans);
    expect(prefixFindGetterReads).toBe(1);
    expect(prefixed.backend.r2FindNames.slice(prefixFinds)).toEqual([
      'ARCHIVE',
    ]);

    const d1Blocked = await boundedDecommissionHarness();
    const d1Started = await startBoundedDecommission(d1Blocked);
    const d1Intent = d1Blocked.store.record?.decommissionIntent;
    if (!d1Intent) throw new Error('missing D1 blocked intent');
    d1Blocked.store.record = {
      ...(d1Blocked.store.record as FleetRecord),
      decommissionIntent: {
        ...d1Intent,
        lifecyclePhase: 'application-resources-deleted',
        state: 'blocked',
        purpose: {
          kind: 'database-pre-export',
          databaseId: d1Blocked.store.record?.databaseId as string,
        },
        attachment: { plane: 'ordinary', scriptName: 'consumer' },
      },
    };
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(d1Blocked, {
          kind: 'restart-blocked',
          token: d1Started.token,
        }),
      ),
    ).rejects.toBeInstanceOf(DecommissionAdvanceRestartError);
    expect(d1Blocked.backend.scanInputs).toHaveLength(0);
  });

  it('rejects hostile provider results before persistence', async () => {
    const hostile = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    const hostileDiscover = await driveBoundedUntil(
      hostile,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const hostileIntent = hostile.store.record?.decommissionIntent;
    if (hostileIntent?.state !== 'discover') {
      throw new Error('missing hostile-result discover progress');
    }
    const validProgress = hostileIntent.progress;
    const rows: readonly DecommissionAttachmentScanResult[] = [
      {
        status: 'pending',
        progress: {} as never,
        providerFetchAttemptsReserved: 3,
      },
      {
        status: 'pending',
        progress: {
          ...validProgress,
          target: { kind: 'r2', bucketName: 'another-bucket' },
        },
        providerFetchAttemptsReserved: 3,
      },
      {
        status: 'pending',
        progress: validProgress,
      } as never,
      {
        status: 'pending',
        progress: validProgress,
        providerFetchAttemptsReserved: -1,
      },
      {
        status: 'pending',
        progress: validProgress,
        providerFetchAttemptsReserved: 1.5,
      },
      {
        status: 'pending',
        progress: validProgress,
        providerFetchAttemptsReserved: 13,
      },
      {
        status: 'attached',
        attachment: { plane: 'dispatch', scriptName: 'broken' } as never,
        providerFetchAttemptsReserved: 3,
      },
      {
        status: 'attached',
        attachment: {
          plane: 'ordinary',
          scriptName: 'é'.repeat(2_049),
        },
        providerFetchAttemptsReserved: 3,
      },
      {
        status: 'complete',
        evidenceSha256: 'not-a-digest',
        evidenceCount: 2,
        providerFetchAttemptsReserved: 3,
      },
      {
        status: 'complete',
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 1,
        providerFetchAttemptsReserved: 3,
      },
      {
        status: 'complete',
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 1_000_001,
        providerFetchAttemptsReserved: 3,
      },
      {
        status: 'complete',
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 2,
        providerFetchAttemptsReserved: 13,
      },
    ];
    for (const row of rows) {
      hostile.backend.scanResults.push(row);
      const snapshot = hostile.store.record;
      await expect(
        continueBoundedDecommission(hostile, hostileDiscover),
      ).rejects.toThrow(/malformed/u);
      expect(hostile.store.record).toBe(snapshot);
    }

    let topLevelGetterReads = 0;
    const topLevelAccessor: Record<string, unknown> = {};
    Object.defineProperty(topLevelAccessor, 'status', {
      enumerable: true,
      get() {
        topLevelGetterReads += 1;
        return 'drift';
      },
    });
    let proxyTrapCalls = 0;
    const hostileProxy = new Proxy(
      {},
      {
        getPrototypeOf() {
          proxyTrapCalls += 1;
          throw new Error('proxy trap must be normalized');
        },
      },
    );
    let nestedGetterReads = 0;
    const nestedAccessor: Record<string, unknown> = {
      scriptName: 'consumer',
    };
    Object.defineProperty(nestedAccessor, 'plane', {
      enumerable: true,
      get() {
        nestedGetterReads += 1;
        return 'ordinary';
      },
    });
    let alternatingScriptNameReads = 0;
    const alternatingAttachment: Record<string, unknown> = {
      plane: 'ordinary',
    };
    Object.defineProperty(alternatingAttachment, 'scriptName', {
      enumerable: true,
      get() {
        alternatingScriptNameReads += 1;
        return alternatingScriptNameReads % 2 === 1 ? 'consumer' : '';
      },
    });
    const symbolResult = { status: 'drift' } as Record<
      string | symbol,
      unknown
    >;
    symbolResult[Symbol('hostile')] = true;
    const cyclicResult: Record<string, unknown> = { status: 'drift' };
    cyclicResult.self = cyclicResult;
    const hostileRows = [
      {
        label: 'top-level accessor',
        row: topLevelAccessor,
        reads: () => topLevelGetterReads,
        expectedReads: 0,
      },
      {
        label: 'top-level proxy',
        row: hostileProxy,
        reads: () => proxyTrapCalls,
        expectedReads: 1,
      },
      {
        label: 'nested accessor',
        row: {
          status: 'attached',
          attachment: nestedAccessor,
          providerFetchAttemptsReserved: 3,
        },
        reads: () => nestedGetterReads,
        expectedReads: 0,
      },
      {
        label: 'alternating scriptName accessor',
        row: {
          status: 'attached',
          attachment: alternatingAttachment,
          providerFetchAttemptsReserved: 3,
        },
        reads: () => alternatingScriptNameReads,
        expectedReads: 0,
      },
      {
        label: 'symbol key',
        row: symbolResult,
        reads: () => 0,
        expectedReads: 0,
      },
      {
        label: 'cycle',
        row: cyclicResult,
        reads: () => 0,
        expectedReads: 0,
      },
      {
        label: 'scalar bound',
        row: { status: 'drift', extra: 'x'.repeat(96 * 1_024) },
        reads: () => 0,
        expectedReads: 0,
      },
      {
        label: 'node bound',
        row: {
          status: 'drift',
          extra: Array.from({ length: 8_192 }, () => null),
        },
        reads: () => 0,
        expectedReads: 0,
      },
    ] as const;
    for (const row of hostileRows) {
      hostile.backend.scanResults.push(row.row as never);
      const before = JSON.stringify(hostile.store.record);
      const writes = hostile.store.phases.length;
      const failure = await continueBoundedDecommission(
        hostile,
        hostileDiscover,
      ).catch((error: unknown) => error);
      expect(failure, row.label).toMatchObject({
        name: 'Error',
        message: 'bounded decommission attachment result is malformed',
      });
      expect(row.reads(), row.label).toBe(row.expectedReads);
      expect(hostile.store.phases, row.label).toHaveLength(writes);
      expect(JSON.stringify(hostile.store.record), row.label).toBe(before);
    }

    const progressBoundary = await boundedDecommissionHarness({
      r2Names: ['FILES'],
    });
    const progressBoundaryDiscover = await driveBoundedUntil(
      progressBoundary,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const progressBoundaryIntent =
      progressBoundary.store.record?.decommissionIntent;
    if (progressBoundaryIntent?.state !== 'discover') {
      throw new Error('missing boundary discover progress');
    }
    const digest = (value: string) =>
      createHash('sha256').update(value).digest('hex');
    const pageStartCursor = 'p'.repeat(4_096);
    const nextCursor = 'n'.repeat(4_096);
    const seenCursorSha256 = [
      ...Array.from({ length: 97 }, (_, index) => digest(`cursor-${index}`)),
      digest(pageStartCursor),
      digest(nextCursor),
    ];
    const legalBoundaryProgress = {
      version: 1,
      target: progressBoundaryIntent.progress.target,
      evidenceSha256: 'a'.repeat(64),
      evidenceCount: 1_000_000,
      stage: 'dispatch-script-settings',
      ordinaryInventorySha256: 'b'.repeat(64),
      namespaceInventorySha256: 'c'.repeat(64),
      namespaceIndex: 0,
      namespaceName: 'm'.repeat(4_096),
      pageStartCursor,
      nextCursor,
      pageSha256: 'd'.repeat(64),
      pageItemCount: 1,
      itemOffset: 0,
      pageNumber: 98,
      seenCursorSha256,
      totalDispatchItems: 10_000,
      dispatchEvidenceSum256: 'e'.repeat(64),
      dispatchEvidenceCount: 9_999,
    } as const;
    progressBoundary.backend.scanResults.push({
      status: 'pending',
      progress: legalBoundaryProgress,
      providerFetchAttemptsReserved: 12,
    });
    const progressBoundaryPending = await continueBoundedDecommission(
      progressBoundary,
      progressBoundaryDiscover,
    );
    expect(progressBoundaryPending).toMatchObject({ status: 'pending' });
    expect(progressBoundary.store.record?.decommissionIntent).toMatchObject({
      state: 'discover',
      progress: legalBoundaryProgress,
    });
    progressBoundary.backend.scanResults.push({
      status: 'complete',
      evidenceSha256: 'f'.repeat(64),
      evidenceCount: 1_000_000,
      providerFetchAttemptsReserved: 3,
    });
    await expect(
      continueBoundedDecommission(progressBoundary, progressBoundaryPending),
    ).resolves.toMatchObject({ status: 'pending' });
    expect(progressBoundary.store.record?.decommissionIntent).toMatchObject({
      state: 'verify',
      discoverEvidence: {
        evidenceSha256: 'f'.repeat(64),
        evidenceCount: 1_000_000,
      },
    });

    const boundary = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    const boundaryDiscover = await driveBoundedUntil(
      boundary,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    boundary.backend.scanResults.push({
      status: 'attached',
      attachment: {
        plane: 'ordinary',
        scriptName: 'x'.repeat(4_096),
        token: 'must-not-persist',
      } as never,
      providerFetchAttemptsReserved: 3,
      token: 'must-not-persist',
    } as never);
    await expect(
      continueBoundedDecommission(boundary, boundaryDiscover),
    ).resolves.toMatchObject({
      status: 'blocked',
      attachment: { plane: 'ordinary', scriptName: 'x'.repeat(4_096) },
    });
    expect(
      JSON.stringify(boundary.store.record?.decommissionIntent),
    ).not.toContain('must-not-persist');
  });

  it('lease contention and loss fence scan consumption and destructive actions', async () => {
    const contended = await boundedDecommissionHarness();
    contended.store.leased = true;
    await expect(startBoundedDecommission(contended)).rejects.toThrow(
      /already being modified/u,
    );
    contended.store.leased = false;

    const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    const discover = await driveBoundedUntil(
      harness,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const verify = await continueBoundedDecommission(harness, discover);
    const lost = new Error('lease lost before detach');
    harness.store.assertOwnedFailure = lost;
    await expect(continueBoundedDecommission(harness, verify)).rejects.toBe(
      lost,
    );
    expect(harness.store.record?.decommissionIntent?.state).toBe('verify');
    expect(harness.store.record?.applicationResources?.[0]?.state).toBe(
      'detach-authorized',
    );
    harness.store.assertOwnedFailure = undefined;
    await continueBoundedDecommission(harness, verify);
    expect(harness.backend.scanInputs.length).toBeGreaterThanOrEqual(2);
  });

  it('executes one lifecycle group per call through application resource deletion', async () => {
    const harness = await boundedDecommissionHarness();
    let result = await startBoundedDecommission(harness);
    const expected = [
      {
        lifecyclePhase: 'decommissioning',
        events: [],
        databaseReads: 0,
        removeTraffic: 0,
        assertTraffic: 0,
        residuals: 0,
      },
      {
        lifecyclePhase: 'traffic-removed',
        events: [],
        databaseReads: 1,
        removeTraffic: 1,
        assertTraffic: 1,
        residuals: 0,
      },
      {
        lifecyclePhase: 'credentials-revoked',
        events: ['revoke'],
        databaseReads: 1,
        removeTraffic: 0,
        assertTraffic: 1,
        residuals: 0,
      },
      {
        lifecyclePhase: 'worker-deleted',
        events: ['delete-worker'],
        databaseReads: 1,
        removeTraffic: 0,
        assertTraffic: 0,
        residuals: 1,
      },
      {
        lifecyclePhase: 'platform-credentials-revoked',
        events: [],
        databaseReads: 0,
        removeTraffic: 0,
        assertTraffic: 0,
        residuals: 0,
      },
      {
        lifecyclePhase: 'platform-resources-deleted',
        events: [],
        databaseReads: 0,
        removeTraffic: 0,
        assertTraffic: 0,
        residuals: 0,
      },
      {
        lifecyclePhase: 'application-resources-deleting',
        events: [],
        databaseReads: 0,
        removeTraffic: 0,
        assertTraffic: 0,
        residuals: 0,
      },
      {
        lifecyclePhase: 'application-resources-deleted',
        events: [],
        databaseReads: 0,
        removeTraffic: 0,
        assertTraffic: 0,
        residuals: 0,
      },
    ] as const;
    for (const step of expected) {
      const eventCount = harness.backend.events.length;
      const databaseReads = harness.backend.databaseIdsRead.length;
      const removeTraffic = harness.backend.removeTrafficCalls;
      const assertTraffic = harness.backend.assertTrafficRemovedCalls;
      const residuals = harness.backend.residualCalls;
      const writes = harness.store.phases.length;
      result = await continueBoundedDecommission(harness, result);
      expect(harness.store.record?.decommissionIntent?.lifecyclePhase).toBe(
        step.lifecyclePhase,
      );
      expect(harness.backend.events.slice(eventCount)).toEqual(step.events);
      expect(harness.backend.databaseIdsRead.length - databaseReads).toBe(
        step.databaseReads,
      );
      expect(harness.backend.removeTrafficCalls - removeTraffic).toBe(
        step.removeTraffic,
      );
      expect(harness.backend.assertTrafficRemovedCalls - assertTraffic).toBe(
        step.assertTraffic,
      );
      expect(harness.backend.residualCalls - residuals).toBe(step.residuals);
      expect(harness.store.phases).toHaveLength(writes + 1);
      expect(harness.store.phases.at(-1)).toBe('decommission-advancing');
    }
    expect(result.status).toBe('pending');
    expect(harness.backend.events).not.toContain('export');
    expect(harness.backend.events).not.toContain('delete-database');

    const platform = await boundedDecommissionHarness({
      kind: 'workers-for-platforms',
      external: true,
    });
    expect(platform.store.record?.platformResources).toBeDefined();
    const platformTrace: string[] = [];
    const getDatabaseImplementation = platform.backend.getDatabase.bind(
      platform.backend,
    );
    const revokePlatformImplementation =
      platform.backend.revokePlatformResourceCredentials.bind(platform.backend);
    const deletePlatformImplementation =
      platform.backend.deletePlatformResources.bind(platform.backend);
    const residualImplementation =
      platform.backend.assertDatabaseDeletionResidualsRemoved.bind(
        platform.backend,
      );
    const putImplementation = platform.store.put.bind(platform.store);
    Object.defineProperties(platform.backend, {
      getDatabase: {
        configurable: true,
        value: async (databaseId: string) => {
          platformTrace.push('d1-read');
          return getDatabaseImplementation(databaseId);
        },
      },
      revokePlatformResourceCredentials: {
        configurable: true,
        value: async () => {
          platformTrace.push('revoke-platform');
          await revokePlatformImplementation();
        },
      },
      deletePlatformResources: {
        configurable: true,
        value: async (deployment: DeploymentSpec, record: FleetRecord) => {
          platformTrace.push('delete-platform');
          await deletePlatformImplementation(deployment, record);
        },
      },
      assertDatabaseDeletionResidualsRemoved: {
        configurable: true,
        value: async () => {
          platformTrace.push('residual');
          await residualImplementation();
        },
      },
    });
    Object.defineProperty(platform.store, 'put', {
      configurable: true,
      value: async (record: FleetRecord) => {
        platformTrace.push('write');
        await putImplementation(record);
      },
    });
    let platformResult = await startBoundedDecommission(platform);
    const platformExpected = [
      {
        lifecyclePhase: 'decommissioning',
        events: [],
        databaseReads: 0,
        residuals: 0,
      },
      {
        lifecyclePhase: 'traffic-removed',
        events: [],
        databaseReads: 1,
        residuals: 0,
      },
      {
        lifecyclePhase: 'credentials-revoked',
        events: ['revoke'],
        databaseReads: 1,
        residuals: 0,
      },
      {
        lifecyclePhase: 'worker-deleted',
        events: ['delete-worker'],
        databaseReads: 1,
        residuals: 0,
      },
      {
        lifecyclePhase: 'platform-credentials-revoked',
        events: ['revoke-platform'],
        databaseReads: 1,
        residuals: 0,
      },
      {
        lifecyclePhase: 'platform-resources-deleted',
        events: ['delete-platform'],
        databaseReads: 1,
        residuals: 1,
      },
    ] as const;
    for (const step of platformExpected) {
      const eventCount = platform.backend.events.length;
      const databaseReads = platform.backend.databaseIdsRead.length;
      const residuals = platform.backend.residualCalls;
      const writes = platform.store.phases.length;
      const traceStart = platformTrace.length;
      platformResult = await continueBoundedDecommission(
        platform,
        platformResult,
      );
      expect(platform.store.record?.decommissionIntent?.lifecyclePhase).toBe(
        step.lifecyclePhase,
      );
      expect(platform.backend.events.slice(eventCount)).toEqual(step.events);
      expect(platform.backend.databaseIdsRead.length - databaseReads).toBe(
        step.databaseReads,
      );
      expect(platform.backend.residualCalls - residuals).toBe(step.residuals);
      expect(platform.store.phases).toHaveLength(writes + 1);
      expect(platform.store.phases.at(-1)).toBe('decommission-advancing');
      if (step.lifecyclePhase === 'platform-credentials-revoked') {
        expect(platformTrace.slice(traceStart)).toEqual([
          'd1-read',
          'revoke-platform',
          'write',
        ]);
      }
      if (step.lifecyclePhase === 'platform-resources-deleted') {
        expect(platformTrace.slice(traceStart)).toEqual([
          'd1-read',
          'delete-platform',
          'residual',
          'write',
        ]);
      }
    }
    expect(platform.backend.events).toEqual([
      'revoke',
      'delete-worker',
      'revoke-platform',
      'delete-platform',
    ]);
    expect(platform.backend.databaseIdsRead).toEqual([
      'database-id',
      'database-id',
      'database-id',
      'database-id',
      'database-id',
    ]);
    expect(platform.backend.residualCalls).toBe(1);

    for (const drift of ['absent', 'name', 'owner'] as const) {
      const exact = await boundedDecommissionHarness();
      const decommissioning = await driveBoundedUntil(
        exact,
        (record) =>
          record.decommissionIntent?.lifecyclePhase === 'decommissioning',
      );
      if (drift === 'absent') exact.backend.databaseExists = false;
      if (drift === 'name') exact.backend.databaseName = 'wrong-name';
      if (drift === 'owner') exact.backend.databaseOwner = 'other-owner';
      const before = exact.store.record;
      await expect(
        continueBoundedDecommission(exact, decommissioning),
      ).rejects.toThrow(/absent|unexpected identity|owned by/u);
      expect(exact.store.record).toBe(before);
      expect(exact.backend.removeTrafficCalls).toBe(0);
    }

    for (const lifecyclePhase of [
      'worker-deleted',
      'platform-credentials-revoked',
    ] as const) {
      for (const drift of ['absent', 'name', 'owner'] as const) {
        const exact = await boundedDecommissionHarness({
          kind: 'workers-for-platforms',
          external: true,
        });
        await driveBoundedUntil(
          exact,
          (record) =>
            record.decommissionIntent?.lifecyclePhase === lifecyclePhase,
        );
        if (drift === 'absent') exact.backend.databaseExists = false;
        if (drift === 'name') exact.backend.databaseName = 'wrong-name';
        if (drift === 'owner') exact.backend.databaseOwner = 'other-owner';
        const authoritative = await startBoundedDecommission(exact);
        const before = exact.store.record;
        const eventCount = exact.backend.events.length;
        await expect(
          continueBoundedDecommission(exact, authoritative),
        ).rejects.toThrow(/absent|unexpected identity|owned by/u);
        expect(exact.store.record).toBe(before);
        expect(exact.backend.events).toHaveLength(eventCount);
      }
    }
  });

  it('starts exact created and detach-authorized live R2 targets', async () => {
    const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    const discover = await driveBoundedUntil(
      harness,
      (record) => record.decommissionIntent?.state === 'discover',
    );
    const intent = harness.store.record?.decommissionIntent;
    const resource = harness.store.record?.applicationResources?.[0];
    expect(intent).toMatchObject({
      state: 'discover',
      purpose: {
        kind: 'application-r2-detach',
        resourceIndex: 0,
        name: resource?.name,
        bucketName: resource?.bucketName,
        reservationNonce: resource?.reservationNonce,
      },
    });
    expect(discover.status).toBe('pending');

    if (!resource) throw new Error('missing R2 resource');
    const mutated = {
      ...resource,
      reservationNonce: 'z'.repeat(32),
    };
    const invalid = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    invalid.store.record = {
      ...(invalid.store.record as FleetRecord),
      applicationResources: [mutated],
    };
    const reads = invalid.backend.r2FindCalls;
    await expect(startBoundedDecommission(invalid)).rejects.toThrow(
      /reservation nonce/u,
    );
    expect(invalid.backend.r2FindCalls).toBe(reads);

    const postTraffic = await boundedDecommissionHarness({
      r2Names: ['FILES'],
    });
    const trafficRemoved = await driveBoundedUntil(
      postTraffic,
      (record) =>
        record.decommissionIntent?.lifecyclePhase === 'traffic-removed',
    );
    const postTrafficResource =
      postTraffic.store.record?.applicationResources?.[0];
    if (!postTrafficResource) {
      throw new Error('missing post-traffic R2 resource');
    }
    postTraffic.store.record = {
      ...(postTraffic.store.record as FleetRecord),
      applicationResources: [
        { ...postTrafficResource, reservationNonce: 'z'.repeat(32) },
      ],
    };
    const trafficReads = postTraffic.backend.assertTrafficRemovedCalls;
    await expect(
      continueBoundedDecommission(postTraffic, trafficRemoved),
    ).rejects.toThrow(/reservation nonce/u);
    expect(postTraffic.backend.assertTrafficRemovedCalls).toBe(trafficReads);
  });

  it('advances detached to empty-authorized and empty-authorized to empty one step each', async () => {
    const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    let result = await driveBoundedUntil(
      harness,
      (record) => record.applicationResources?.[0]?.state === 'detached',
    );
    result = await continueBoundedDecommission(harness, result);
    expect(harness.store.record?.applicationResources?.[0]?.state).toBe(
      'empty-authorized',
    );
    harness.backend.nonempty = true;
    const snapshot = harness.store.record;
    await expect(continueBoundedDecommission(harness, result)).rejects.toThrow(
      /not empty/u,
    );
    expect(harness.store.record).toBe(snapshot);
    harness.backend.nonempty = false;
    await continueBoundedDecommission(harness, result);
    expect(harness.store.record?.applicationResources?.[0]?.state).toBe(
      'empty',
    );
  });

  it('persists empty to delete-authorized before physical deletion', async () => {
    const harness = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    let result = await driveBoundedUntil(
      harness,
      (record) => record.applicationResources?.[0]?.state === 'empty',
    );
    const calls = harness.backend.deleteCalls;
    result = await continueBoundedDecommission(harness, result);
    expect(harness.store.record?.applicationResources?.[0]?.state).toBe(
      'delete-authorized',
    );
    expect(harness.backend.deleteCalls).toBe(calls);
    await continueBoundedDecommission(harness, result);
    expect(harness.backend.deleteCalls).toBe(calls + 1);
  });

  it('reconciles lost delete responses and deleted writes without duplicate mutation', async () => {
    const beforeCommit = await boundedDecommissionHarness({
      r2Names: ['FILES'],
    });
    const authorized = await driveBoundedUntil(
      beforeCommit,
      (record) =>
        record.applicationResources?.[0]?.state === 'delete-authorized',
    );
    const sentinel = new Error('delete failed before commit');
    beforeCommit.backend.deleteFailureBeforeCommit = sentinel;
    await expect(
      continueBoundedDecommission(beforeCommit, authorized),
    ).rejects.toBe(sentinel);
    expect(beforeCommit.store.record?.applicationResources?.[0]?.state).toBe(
      'delete-authorized',
    );

    const precommitStore = new MemoryStore();
    const precommitWrite = await boundedDecommissionHarness({
      r2Names: ['FILES'],
      store: precommitStore,
    });
    const precommitAuthorized = await driveBoundedUntil(
      precommitWrite,
      (record) =>
        record.applicationResources?.[0]?.state === 'delete-authorized',
    );
    precommitStore.failPutApplicationState = {
      name: 'FILES',
      state: 'deleted',
    };
    const successfulDeletes = precommitWrite.backend.deleteCalls;
    await expect(
      continueBoundedDecommission(precommitWrite, precommitAuthorized),
    ).rejects.toThrow('failed state write at FILES:deleted');
    expect(precommitWrite.backend.deleteCalls).toBe(successfulDeletes + 1);
    expect(precommitWrite.store.record?.applicationResources?.[0]?.state).toBe(
      'delete-authorized',
    );
    await continueBoundedDecommission(precommitWrite, precommitAuthorized);
    expect(precommitWrite.backend.deleteCalls).toBe(successfulDeletes + 1);
    expect(precommitWrite.store.record?.applicationResources?.[0]?.state).toBe(
      'deleted',
    );

    const afterCommit = await boundedDecommissionHarness({
      r2Names: ['FILES'],
      store: new CommitThenThrowStore(),
    });
    const deleteToken = await driveBoundedUntil(
      afterCommit,
      (record) =>
        record.applicationResources?.[0]?.state === 'delete-authorized',
    );
    afterCommit.backend.deleteFailureAfterCommit = new Error(
      'delete response lost',
    );
    (
      afterCommit.store as CommitThenThrowStore
    ).failAfterCommittedApplicationState = {
      name: 'FILES',
      state: 'deleted',
    };
    await expect(
      continueBoundedDecommission(afterCommit, deleteToken),
    ).rejects.toThrow(/response was lost/u);
    expect(afterCommit.store.record?.applicationResources?.[0]?.state).toBe(
      'deleted',
    );
    expect(afterCommit.backend.deleteCalls).toBe(1);
    const replayed = await continueBoundedDecommission(
      afterCommit,
      deleteToken,
    );
    await expect(
      continueBoundedDecommission(afterCommit, deleteToken),
    ).resolves.toEqual(replayed);
    expect(afterCommit.backend.deleteCalls).toBe(1);
  });

  it('orders two resources without rewind and stops inertly at the D1 boundary', async () => {
    const harness = await boundedDecommissionHarness({
      r2Names: ['ARCHIVE', 'FILES'],
    });
    let result = await startBoundedDecommission(harness);
    for (let step = 0; step < 64; step += 1) {
      const resources = harness.store.record?.applicationResources ?? [];
      if (
        resources[0]?.state === 'deleted' &&
        resources[1]?.state !== 'deleted'
      ) {
        const first = resources[0];
        harness.backend.buckets.set(first.bucketName, {
          name: first.name,
          bucketName: first.bucketName,
          jurisdiction: first.jurisdiction,
          creationDate: first.creationDate as string,
        });
        let authoritative: DecommissionAdvanceResult | undefined;
        let refusal: unknown;
        try {
          authoritative = await continueBoundedDecommission(harness, result);
        } catch (error) {
          refusal = error;
        }
        if (refusal !== undefined) {
          expect(refusal).toMatchObject({
            message: expect.stringMatching(/reappeared/u),
          });
        } else {
          if (!authoritative) throw new Error('missing authoritative token');
          await expect(
            continueBoundedDecommission(harness, authoritative),
          ).rejects.toThrow(/reappeared/u);
          result = authoritative;
        }
        harness.backend.buckets.delete(first.bucketName);
      }
      if (
        harness.store.record?.decommissionIntent?.lifecyclePhase ===
        'application-resources-deleted'
      ) {
        break;
      }
      result = await continueBoundedDecommission(harness, result);
    }
    expect(
      harness.store.record?.applicationResources?.map(({ state }) => state),
    ).toEqual(['deleted', 'deleted']);
    expect(harness.store.record?.decommissionIntent?.generation).toBe(2);
    const scans = harness.backend.scanInputs.length;
    const inert = await continueBoundedDecommission(harness, result);
    expect(inert).toEqual(result);
    expect(harness.backend.scanInputs).toHaveLength(scans);

    for (const mode of [
      'discover',
      'verify',
      'pending',
      'drift',
      'attached',
    ] as const) {
      const prefixed = await boundedDecommissionHarness({
        r2Names: ['ARCHIVE', 'FILES'],
      });
      let current = await driveBoundedUntil(
        prefixed,
        (record) =>
          record.decommissionIntent?.state === 'discover' &&
          record.decommissionIntent.purpose.kind === 'application-r2-detach' &&
          record.decommissionIntent.purpose.resourceIndex === 1,
      );
      const scanIntent = prefixed.store.record?.decommissionIntent;
      if (scanIntent?.state !== 'discover') {
        throw new Error('missing second-resource discover state');
      }
      if (mode === 'verify') {
        current = await continueBoundedDecommission(prefixed, current);
      } else if (mode === 'pending') {
        prefixed.backend.scanResults.push({
          status: 'pending',
          progress: scanIntent.progress,
          providerFetchAttemptsReserved: 3,
        });
        current = await continueBoundedDecommission(prefixed, current);
      } else if (mode === 'drift') {
        prefixed.backend.scanResults.push({ status: 'drift' });
      } else if (mode === 'attached') {
        prefixed.backend.scanResults.push({
          status: 'attached',
          attachment: { plane: 'ordinary', scriptName: 'consumer' },
          providerFetchAttemptsReserved: 3,
        });
      }
      const deleted = prefixed.store.record?.applicationResources?.[0];
      if (!deleted?.creationDate) {
        throw new Error('missing first-resource deletion identity');
      }
      prefixed.backend.buckets.set(deleted.bucketName, {
        name: deleted.name,
        bucketName: deleted.bucketName,
        jurisdiction: deleted.jurisdiction,
        creationDate: deleted.creationDate,
      });
      const snapshot = prefixed.store.record;
      const scanCount = prefixed.backend.scanInputs.length;
      const findCount = prefixed.backend.r2FindNames.length;
      const writeCount = prefixed.store.phases.length;
      await expect(
        continueBoundedDecommission(prefixed, current),
        mode,
      ).rejects.toThrow(/reappeared after deletion/u);
      expect(prefixed.store.record, mode).toBe(snapshot);
      expect(prefixed.backend.scanInputs, mode).toHaveLength(scanCount);
      expect(prefixed.store.phases, mode).toHaveLength(writeCount);
      expect(prefixed.backend.r2FindNames.slice(findCount), mode).toEqual([
        'ARCHIVE',
      ]);
    }

    for (const state of ['reserved', 'create-authorized'] as const) {
      for (const mode of ['discover', 'verify', 'restart'] as const) {
        const hostile = await boundedDecommissionHarness({
          r2Names: ['ARCHIVE', 'FILES'],
        });
        let current = await driveBoundedUntil(
          hostile,
          (record) =>
            record.decommissionIntent?.state === 'discover' &&
            record.decommissionIntent.purpose.kind ===
              'application-r2-detach' &&
            record.decommissionIntent.purpose.resourceIndex === 1,
        );
        if (mode === 'verify') {
          current = await continueBoundedDecommission(hostile, current);
        } else if (mode === 'restart') {
          hostile.backend.scanResults.push({
            status: 'attached',
            attachment: { plane: 'ordinary', scriptName: 'consumer' },
            providerFetchAttemptsReserved: 3,
          });
          current = await continueBoundedDecommission(hostile, current);
          expect(current.status).toBe('blocked');
        }
        const resources = hostile.store.record?.applicationResources;
        const prefix = resources?.[0];
        if (!prefix || !resources?.[1]) {
          throw new Error('missing hostile prefix resources');
        }
        const { creationDate: _creationDate, ...reservation } = prefix;
        hostile.store.record = {
          ...(hostile.store.record as FleetRecord),
          applicationResources: [
            { ...reservation, state },
            ...resources.slice(1),
          ],
        };
        const findImplementation = hostile.backend.findApplicationR2Bucket;
        const scanImplementation =
          hostile.backend.advanceDecommissionAttachmentScan;
        let findReads = 0;
        let scanReads = 0;
        Object.defineProperties(hostile.backend, {
          findApplicationR2Bucket: {
            configurable: true,
            get() {
              findReads += 1;
              return findImplementation;
            },
          },
          advanceDecommissionAttachmentScan: {
            configurable: true,
            get() {
              scanReads += 1;
              return scanImplementation;
            },
          },
        });
        const before = JSON.stringify(hostile.store.record);
        const findCalls = hostile.backend.r2FindCalls;
        const scanCalls = hostile.backend.scanInputs.length;
        const emptyCalls = hostile.backend.emptyChecks;
        const deleteCalls = hostile.backend.deleteCalls;
        const writes = hostile.store.phases.length;
        const events = [...hostile.backend.events];
        const attempt =
          mode === 'restart'
            ? advanceDecommissionDeployment(
                boundedAdvanceOptions(hostile, {
                  kind: 'restart-blocked',
                  token: current.token,
                }),
              )
            : continueBoundedDecommission(hostile, current);
        await expect(attempt, `${state}:${mode}`).rejects.toThrow(
          'normal decommission cannot consume incomplete application R2 reservation',
        );
        expect(JSON.stringify(hostile.store.record), `${state}:${mode}`).toBe(
          before,
        );
        expect(findReads, `${state}:${mode}:find getter`).toBe(0);
        expect(scanReads, `${state}:${mode}:scan getter`).toBe(0);
        expect(hostile.backend.r2FindCalls, `${state}:${mode}`).toBe(findCalls);
        expect(hostile.backend.scanInputs, `${state}:${mode}`).toHaveLength(
          scanCalls,
        );
        expect(hostile.backend.emptyChecks, `${state}:${mode}`).toBe(
          emptyCalls,
        );
        expect(hostile.backend.deleteCalls, `${state}:${mode}`).toBe(
          deleteCalls,
        );
        expect(hostile.store.phases, `${state}:${mode}`).toHaveLength(writes);
        expect(hostile.backend.events, `${state}:${mode}`).toEqual(events);
      }
    }
  });

  it('atomically consumes plain and WFP migration carriers while preserving snapshots', async () => {
    for (const kind of ['plain-worker', 'workers-for-platforms'] as const) {
      const harness = await boundedDecommissionHarness({ kind });
      const record = harness.store.record as FleetRecord;
      const targetDigest = deploymentSpecDigest(harness.deployment);
      const oldDigest = 'f'.repeat(64);
      const activeRelease: ExternalReleaseSnapshot = {
        physicalScriptName: record.scriptName,
        specDigest: oldDigest,
        artifactVersion: 'artifact-old',
        releaseSchemaVersion: record.schemaVersion,
        application: record.applicationBindings,
      };
      const pendingRelease: ExternalReleaseSnapshot = {
        physicalScriptName:
          kind === 'plain-worker'
            ? record.scriptName
            : `${record.scriptName}-next`,
        specDigest: targetDigest,
        artifactVersion: 'artifact-next',
        releaseSchemaVersion: harness.deployment.schemaVersion,
        application: record.applicationBindings,
      };
      harness.store.record = {
        ...record,
        phase: 'migrating',
        desiredSpecDigest: oldDigest,
        activeRelease,
        ...(kind === 'plain-worker'
          ? {
              pendingSpecDigest: targetDigest,
              pendingArtifactVersion: pendingRelease.artifactVersion,
            }
          : {
              pendingSpecDigest: targetDigest,
              pendingRelease,
              migrationPriorRelease: activeRelease,
              migrationIntent: {
                targetSpecDigest: targetDigest,
                priorRelease: activeRelease,
                priorTarget: record.platformTarget as never,
                priorOutboundPolicy: record.outboundPolicy as never,
                targetRelease: pendingRelease,
                target: record.platformTarget as never,
                subphase: 'route-published',
              },
            }),
      };
      const started = await startBoundedDecommission(harness);
      await continueBoundedDecommission(harness, started);
      expect(harness.store.record).toMatchObject({
        desiredSpecDigest: targetDigest,
        activeRelease,
        pendingRelease,
        decommissionIntent: {
          lifecyclePhase: 'decommissioning',
          identity: { mode: { entryLifecyclePhase: 'migrating' } },
        },
      });
      expect(harness.store.record).not.toHaveProperty('pendingSpecDigest');
      expect(harness.store.record).not.toHaveProperty('pendingArtifactVersion');
      expect(harness.store.record).not.toHaveProperty('migrationIntent');
    }

    const withoutArtifact = await boundedDecommissionHarness({
      kind: 'plain-worker',
    });
    const source = withoutArtifact.store.record as FleetRecord;
    const targetDigest = deploymentSpecDigest(withoutArtifact.deployment);
    const activeRelease: ExternalReleaseSnapshot = {
      physicalScriptName: source.scriptName,
      specDigest: 'e'.repeat(64),
      artifactVersion: 'artifact-old',
      releaseSchemaVersion: source.schemaVersion,
      application: source.applicationBindings,
    };
    withoutArtifact.store.record = {
      ...source,
      phase: 'migrating',
      desiredSpecDigest: activeRelease.specDigest,
      pendingSpecDigest: targetDigest,
      activeRelease,
    };
    const started = await startBoundedDecommission(withoutArtifact);
    await continueBoundedDecommission(withoutArtifact, started);
    expect(withoutArtifact.store.record).toMatchObject({
      desiredSpecDigest: targetDigest,
      activeRelease,
    });
    expect(withoutArtifact.store.record).not.toHaveProperty('pendingRelease');
    expect(withoutArtifact.store.record).not.toHaveProperty(
      'pendingSpecDigest',
    );
  });
});
