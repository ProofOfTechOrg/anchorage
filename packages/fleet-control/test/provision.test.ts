// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { ActiveRouteAttestationError } from '../src/active-route.js';
import type {
  BackendSwitchProvider,
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
  reconcilePersistedDatabase,
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
  CleanupTerminalReceipt,
  DatabaseExport,
  DatabaseExportReceiptIdentity,
  DatabaseReference,
  DecommissionAdvanceIntent,
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
import { effectiveLifecyclePhase } from '../src/types.js';
import { externalReleaseScriptName } from '../src/workers-for-platforms-backend.js';
import { WranglerLoopBackend } from '../src/wrangler-loop-backend.js';
import type { CommandResult, CommandRunner } from '../src/wrangler-runner.js';
import {
  backendSwitchDecommissionRecordFixture,
  decommissionAdvancingRecordFixture,
} from './fixtures/decommission-intent-fixture.js';
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

const DATABASE_ID = '00000000-0000-0000-0000-000000000101';
const REPLACEMENT_DATABASE_ID = '00000000-0000-0000-0000-000000000102';
const RECEIPT_AUTHORITY = 'memory://fleet-exports/receipts/v1';

type BoxedOutcome<Value> =
  | Readonly<{ status: 'fulfilled'; value: Value; commit?: boolean }>
  | Readonly<{ status: 'rejected'; reason: unknown; commit?: boolean }>;

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
  readonly receipts = new Map<string, CleanupTerminalReceipt>();
  completedAtMs = 1_000;
  supportsDeleteReleasingClaims = false;
  deleteReleasingClaimsCalls = 0;
  deleteCalls = 0;
  readonly phases: string[] = [];
  failPutPhase: string | undefined;
  failPutApplicationState:
    | Readonly<{ name: string; state: ApplicationR2Resource['state'] }>
    | undefined;
  assertOwnedFailure: unknown;
  assertOwnedFailureAt: number | undefined;
  assertOwnedObserved: (() => void) | undefined;
  assertOwnedCalls = 0;

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
          this.assertOwnedCalls += 1;
          this.assertOwnedObserved?.();
          if (this.assertOwnedFailure !== undefined) {
            throw this.assertOwnedFailure;
          }
          if (this.assertOwnedFailureAt === this.assertOwnedCalls) {
            throw new Error(`lease assertion ${this.assertOwnedCalls} failed`);
          }
        },
        renew: async () => {},
        put: (record) => this.put(record),
        delete: () => this.delete(),
        completeCleanup: (input) => this.completeCleanup(input),
        ...(this.supportsDeleteReleasingClaims
          ? {
              deleteReleasingClaims: async () => {
                this.deleteReleasingClaimsCalls += 1;
                this.record = undefined;
              },
            }
          : {}),
      });
    } finally {
      this.leased = false;
    }
  }

  async completeCleanup(input: {
    receipt: CleanupTerminalReceipt;
    expectedRevision: number;
  }): Promise<CleanupTerminalReceipt> {
    const current = this.record;
    if (
      current?.phase !== 'cleanup-advancing' ||
      current.cleanupIntent?.operationId !== input.receipt.operationId ||
      current.cleanupIntent.revision !== input.expectedRevision
    ) {
      const existing = this.receipts.get(input.receipt.operationId);
      if (existing) return existing;
      throw new Error(
        `cleanup receipt conflict for operation '${input.receipt.operationId}'`,
      );
    }
    const persisted = { ...input.receipt, completedAtMs: this.completedAtMs };
    this.completedAtMs += 1;
    this.receipts.set(persisted.operationId, persisted);
    this.record = undefined;
    return persisted;
  }

  async readCleanupReceipt(
    operationId: string,
  ): Promise<CleanupTerminalReceipt | undefined> {
    return this.receipts.get(operationId);
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
    const lifecyclePhase = record.decommissionIntent
      ? effectiveLifecyclePhase(record)
      : record.phase;
    if (
      this.failPutPhase === record.phase ||
      this.failPutPhase === lifecyclePhase
    ) {
      const failedPhase = this.failPutPhase;
      this.failPutPhase = undefined;
      throw new Error(`failed state write at ${failedPhase}`);
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
    this.deleteCalls += 1;
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
    const lifecyclePhase = record.decommissionIntent
      ? effectiveLifecyclePhase(record)
      : record.phase;
    if (
      this.failAfterCommittedPhase === record.phase ||
      this.failAfterCommittedPhase === lifecyclePhase
    ) {
      const failedPhase = this.failAfterCommittedPhase;
      this.failAfterCommittedPhase = undefined;
      throw new Error(
        `state write response was lost after committing ${failedPhase}`,
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
  readonly databaseExportReceiptAuthority = RECEIPT_AUTHORITY;
  readonly events: string[] = [];
  failAt: string | undefined;
  cleanupFailAt: string | undefined;
  live: LiveDeployment | undefined;
  /** Strands the route on something else; unset attests the live deployment. */
  activeRoute: ActiveRouteAttestation | undefined;
  exportLocation = 'r2://fleet-exports/acme.sql';
  databaseExists = false;
  databaseId = DATABASE_ID;
  databaseName = 'acme-production';
  databaseOwner: unknown;
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
  scanAfter: (() => void) | undefined;
  readonly databaseReadOutcomes: BoxedOutcome<unknown>[] = [];
  readonly deleteOutcomes: BoxedOutcome<void>[] = [];
  readonly receiptOutcomes: BoxedOutcome<unknown>[] = [];
  readonly receiptCalls: DatabaseExportReceiptIdentity[] = [];
  readonly receiptWinners = new Map<string, DatabaseExport>();
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
    const outcome = this.databaseReadOutcomes.shift();
    if (outcome?.status === 'rejected') throw outcome.reason;
    if (outcome?.status === 'fulfilled') {
      return outcome.value as DatabaseReference | undefined;
    }
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
    return { id: DATABASE_ID, name: 'acme-production', created: true };
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
    return this.databaseOwner as string | undefined;
  }

  async applyMigrations(): Promise<void> {
    this.#event('migrations');
  }

  async advanceDecommissionAttachmentScan(
    input: DecommissionAttachmentScanInput,
  ): Promise<DecommissionAttachmentScanResult> {
    this.scanInputs.push(input);
    if (this.scanFailure !== undefined) throw this.scanFailure;
    const result = this.scanResults.shift() ?? {
      status: 'complete',
      evidenceSha256: 'a'.repeat(64),
      evidenceCount: 2,
      providerFetchAttemptsReserved: 3,
    };
    const after = this.scanAfter;
    this.scanAfter = undefined;
    after?.();
    return result;
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
      databaseId: DATABASE_ID,
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
      databaseId: DATABASE_ID,
      location: this.exportLocation,
      sha256: 'a'.repeat(64),
      size: 42,
    };
  }

  async exportDatabaseReceipt(
    identity: DatabaseExportReceiptIdentity,
  ): Promise<DatabaseExport> {
    this.#event('export');
    this.receiptCalls.push(structuredClone(identity));
    const key = JSON.stringify(identity);
    const canonical: DatabaseExport = {
      databaseId: identity.databaseId,
      location: `${this.exportLocation}/${identity.operationId}`,
      sha256: 'a'.repeat(64),
      size: 42,
    };
    const outcome = this.receiptOutcomes.shift();
    if (outcome?.status === 'rejected') {
      if (outcome.commit === true) this.receiptWinners.set(key, canonical);
      throw outcome.reason;
    }
    if (outcome?.status === 'fulfilled') {
      return outcome.value as DatabaseExport;
    }
    const winner = this.receiptWinners.get(key);
    if (winner) return structuredClone(winner);
    this.receiptWinners.set(key, canonical);
    return structuredClone(canonical);
  }

  async deleteDatabase(): Promise<void> {
    this.#event('delete-database');
    const outcome = this.deleteOutcomes.shift();
    if (outcome) {
      if (outcome.commit === true) this.databaseExists = false;
      if (outcome.status === 'rejected') throw outcome.reason;
      return;
    }
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
              ? [{ uuid: DATABASE_ID, name: deployment.databaseName }]
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
                { type: 'd1', name: 'DB', id: DATABASE_ID },
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
      return state.databaseExists && databaseId === DATABASE_ID
        ? {
            id: DATABASE_ID,
            name: deployment.databaseName,
            created: false,
          }
        : undefined;
    },
    async deleteDatabase(databaseId) {
      if (databaseId === DATABASE_ID) state.databaseExists = false;
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

async function driveToPreExportVerify(
  harness: BoundedDecommissionHarness,
): Promise<DecommissionAdvanceResult> {
  let result = await driveBoundedUntil(
    harness,
    (record) =>
      record.decommissionIntent?.lifecyclePhase ===
        'application-resources-deleted' &&
      record.decommissionIntent.state === 'transitioning',
  );
  result = await continueBoundedDecommission(harness, result);
  expect(harness.store.record?.decommissionIntent).toMatchObject({
    lifecyclePhase: 'application-resources-deleted',
    state: 'discover',
    purpose: { kind: 'database-pre-export', databaseId: DATABASE_ID },
    databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
  });
  result = await continueBoundedDecommission(harness, result);
  expect(harness.store.record?.decommissionIntent).toMatchObject({
    lifecyclePhase: 'application-resources-deleted',
    state: 'verify',
  });
  return result;
}

async function driveToPreDeleteVerify(
  harness: BoundedDecommissionHarness,
): Promise<DecommissionAdvanceResult> {
  let result = await driveToPreExportVerify(harness);
  result = await continueBoundedDecommission(harness, result);
  expect(harness.store.record?.decommissionIntent).toMatchObject({
    lifecyclePhase: 'database-exported',
    state: 'transitioning',
  });
  result = await continueBoundedDecommission(harness, result);
  expect(harness.store.record?.decommissionIntent).toMatchObject({
    lifecyclePhase: 'database-exported',
    state: 'discover',
    purpose: { kind: 'database-pre-delete', databaseId: DATABASE_ID },
  });
  result = await continueBoundedDecommission(harness, result);
  expect(harness.store.record?.decommissionIntent).toMatchObject({
    lifecyclePhase: 'database-exported',
    state: 'verify',
  });
  return result;
}

function legacyOnlyBackend(backend: FakeBackend): ProvisioningBackend {
  return new Proxy({} as ProvisioningBackend, {
    has(_target, property) {
      if (
        property === 'advanceDecommissionAttachmentScan' ||
        property === 'databaseExportReceiptAuthority' ||
        property === 'exportDatabaseReceipt'
      ) {
        return false;
      }
      return Reflect.has(backend, property);
    },
    get(_target, property) {
      if (
        property === 'advanceDecommissionAttachmentScan' ||
        property === 'databaseExportReceiptAuthority' ||
        property === 'exportDatabaseReceipt'
      ) {
        return undefined;
      }
      const value = Reflect.get(backend, property, backend);
      return typeof value === 'function' ? value.bind(backend) : value;
    },
  });
}

describe('fleet provisioning', () => {
  it('attests empty application bindings exactly while allowing only system-owned variables', () => {
    const deployment = spec();
    const digest = deploymentSpecDigest(deployment);
    const record = {
      tenantTag: deployment.tenantTag,
      environment: deployment.environment,
      databaseId: DATABASE_ID,
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

  it('refuses non-advance root lifecycle mutations while decommission advances', async () => {
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

    const completeStore = new MemoryStore();
    const raceBackend = new FakeBackend();
    const advancing = advancingRecord(raceBackend);
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
        databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
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

  it('fails closed when a decommission record carries a cleanup intent', async () => {
    const base = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'plain-worker',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 1,
      artifactVersion: 'artifact-v1',
      desiredSpecDigest: 'a'.repeat(64),
      durableObjectBindings: [],
      applicationResources: [],
      applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
      routeHostname: 'acme.example.test',
      phase: 'ready',
      updatedAt: '2026-08-29T00:00:00.000Z',
    } as const satisfies FleetRecord;
    const hostile = {
      ...decommissionAdvancingRecordFixture(base, 'ready', {
        operationId: '123e4567-e89b-42d3-a456-426614174000',
        revision: 0,
        generation: 0,
        updatedAt: '2026-08-29T00:00:01.000Z',
      }),
      cleanupIntent: { version: 1 },
    };
    const store = {
      get: async () => hostile,
      list: async () => [hostile],
      withDeploymentLease: async () => {
        throw new Error('lease must not be acquired for a hostile record');
      },
    };
    await expect(
      decommissionDeployment({
        backend: {} as never,
        store: store as never,
        spec: {
          tenantTag: 'acme',
          environment: 'production',
        } as never,
      }),
    ).rejects.toThrow('backend switch decommission record is malformed');
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
      databaseId: DATABASE_ID,
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
      // The invocation-authority flip commits on a dedicated put before the
      // external candidate upload dispatches.
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
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'publishing',
    );

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
    const backend = new FakeBackend('plain-worker');
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
    expect([...store.receipts.values()]).toMatchObject([
      { disposition: 'reservation-cleared', authority: 'manual-cleanup' },
    ]);
  });

  it('deletes an unseeded exact-name database left by an ambiguous reserved create', async () => {
    const backend = new FakeBackend('plain-worker');
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
    const backend = new FakeBackend('plain-worker');
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
  ])('rolls back resources when %s fails', async (failure) => {
    const backend = new FakeBackend('plain-worker');
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
      // Ownership was never proven, so rollback keeps the resumable row
      // instead of admitting an engine that could not delete the database.
      expect(backend.events).not.toContain('delete-database');
      expect(store.record?.phase).toBe('database-created');
      expect(store.receipts.size).toBe(0);
    } else {
      expect(backend.events.at(-1)).toBe('delete-database');
      expect(store.record).toBeUndefined();
      expect([...store.receipts.values()]).toMatchObject([
        {
          disposition: 'prepublication-owned-no-export',
          authority: 'provisioning-rollback',
        },
      ]);
    }
    if (failure === 'worker') {
      expect(backend.events).toContain('revoke');
      expect(backend.events).toContain('delete-worker');
    }
  });

  it('passes the persisted plain-Worker release through post-deploy rollback', async () => {
    const backend = new FakeBackend('plain-worker');
    // Fail after the worker-deployed commit but before the maintenance flip:
    // once maintenance was requested, rollback refuses no-export teardown.
    backend.failAt = 'inspect';
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
      // The failed detachment assertion persists nothing; replay re-derives
      // the detachment requirement from the durable 'created' state.
      { name: 'FILES', state: 'created' },
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
      // The failed detachment assertion persists nothing; replay re-derives
      // the detachment requirement from the durable 'created' state.
      { name: 'ARCHIVE', state: 'created' },
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
    // The customer-data-capable external candidate is preserved whole:
    // rollback refuses before any mutation and routes teardown to
    // export-backed decommissioning.
    expect(backend.events).toEqual(
      expect.arrayContaining(['platform-resources', 'worker']),
    );
    expect(backend.events).not.toContain('revoke-platform');
    expect(backend.events).not.toContain('delete-platform');
    expect(backend.events).not.toContain('delete-database');
    expect(store.record?.phase).toBe('platform-resources-deployed');
    expect(store.record?.platformResources?.stateWorker.namespaceIds).toEqual([
      'state-acme-production-MAINTENANCE',
    ]);
  });

  it('preserves a private bootstrap for export-backed decommission after privatization fails before the resource snapshot', async () => {
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

    // The private bootstrap fails before the resource snapshot commits, so
    // the durable row never left 'application-resources-deployed'; the WFP
    // rollback refuses no-export teardown and preserves the deployment.
    expect(backend.events).toEqual(
      expect.arrayContaining(['platform-resources', 'platform-privatization']),
    );
    expect(backend.events).not.toContain('delete-database');
    expect(backend.databaseExists).toBe(true);
    expect(store.record?.phase).toBe('application-resources-deployed');
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
      databaseId: DATABASE_ID,
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
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'application-resources-deleted',
    );

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
    expect(result.databaseExport.location).toContain(backend.exportLocation);
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
      decommissionDeployment({
        backend,
        store,
        spec: deployment,
      }),
    ).rejects.toThrow(/not empty/u);
    expect(backend.emptyChecks).toBe(1);
    expect(backend.events).toEqual([]);
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe('ready');

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
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'traffic-removed',
    );
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
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'decommissioning',
    );
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
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'traffic-removed',
    );
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
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'publishing',
    );
    expect(backend.live).toBeDefined();
    expect(backend.databaseExists).toBe(true);
    expect(backend.buckets.size).toBe(1);

    backend.failAt = undefined;
    backend.events.length = 0;
    backend.nonempty = true;
    await expect(
      decommissionDeployment({ backend, store, spec: deployment }),
    ).rejects.toThrow(/not empty/u);
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'publishing',
    );
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
    store.record = {
      ...store.record,
      phase: 'worker-deployed',
      invocationAuthority: { version: 1, authorizedAt: null },
    };
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
    // The failed group leaves a durable intent for retry; remediation then
    // resumes the same operation to its terminal receipt.
    expect(store.record?.phase).toBe('cleanup-advancing');
    expect(store.record?.cleanupIntent?.identity.admittedPhase).toBe(
      'worker-deployed',
    );
    backend.trafficDrift = false;
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: deployment }),
    ).resolves.toBeUndefined();
    expect(store.record).toBeUndefined();
    expect(store.receipts.size).toBe(1);
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
      decommissionDeployment({
        backend: legacyOnlyBackend(backend),
        store,
        spec: deployment,
      }),
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
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'database-deleting',
    );
    expect(backend.databaseExists).toBe(false);

    const findDatabaseCalls = backend.findDatabaseCalls;
    backend.databaseExists = true;
    backend.databaseId = REPLACEMENT_DATABASE_ID;

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(
      backend.events.filter((event) => event === 'delete-database'),
    ).toHaveLength(1);
    expect(backend.databaseExists).toBe(true);
    expect(backend.databaseIdsRead.at(-1)).toBe(DATABASE_ID);
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
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'decommissioning',
    );
  });

  it('rejects database cleanup when the persisted ID has another sentinel owner', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    if (!store.record) throw new Error('missing test record');
    store.record = {
      ...store.record,
      phase: 'worker-deployed',
      invocationAuthority: { version: 1, authorizedAt: null },
    };
    backend.databaseOwner = 'other-tenant';
    backend.events.length = 0;

    // The engine reconciles the persisted database at the deletion boundary,
    // after the absence-tolerant teardown groups; the foreign sentinel then
    // refuses deletion and the durable intent stays resumable.
    const failure = await cleanupDeploymentArtifacts({
      backend,
      store,
      spec: spec(),
    }).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    expect((failure as AggregateError).errors).toEqual([
      expect.objectContaining({
        message: expect.stringMatching(/owned by 'other-tenant'/),
      }),
    ]);
    expect(backend.events).not.toContain('delete-database');
    expect(backend.databaseExists).toBe(true);
    expect(store.record?.phase).toBe('cleanup-advancing');
  });

  it('converges cleanup when the persisted database ID is positively absent', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    if (!store.record) throw new Error('missing test record');
    store.record = {
      ...store.record,
      phase: 'worker-deployed',
      invocationAuthority: { version: 1, authorizedAt: null },
    };
    backend.databaseExists = false;
    backend.events.length = 0;

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).resolves.toBeUndefined();
    expect(backend.events).toEqual(['revoke', 'delete-worker']);
    expect(store.record).toBeUndefined();
    expect([...store.receipts.values()]).toMatchObject([
      { disposition: 'reservation-cleared', authority: 'manual-cleanup' },
    ]);
  });

  it('does not treat a persisted-ID lookup failure as database absence', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    if (!store.record) throw new Error('missing test record');
    store.record = {
      ...store.record,
      phase: 'worker-deployed',
      invocationAuthority: { version: 1, authorizedAt: null },
    };
    backend.getDatabase = async () => {
      throw new Error('D1 lookup unavailable');
    };
    backend.events.length = 0;

    const failure = await cleanupDeploymentArtifacts({
      backend,
      store,
      spec: spec(),
    }).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    expect((failure as AggregateError).errors).toEqual([
      expect.objectContaining({
        message: expect.stringMatching(/D1 lookup unavailable/),
      }),
    ]);
    expect(backend.events).not.toContain('delete-database');
    expect(backend.databaseExists).toBe(true);
    expect(store.record?.phase).toBe('cleanup-advancing');
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
    expect(backend.databaseIdsRead).toContain(DATABASE_ID);
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
    backend.receiptOutcomes.push({
      status: 'fulfilled',
      value: {
        databaseId: DATABASE_ID,
        location: backend.exportLocation,
        sha256: '',
        size: 0,
      },
    });

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).rejects.toThrow(
      'bounded decommission database export result is malformed',
    );
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
    backend.receiptOutcomes.push({
      status: 'fulfilled',
      value: {
        databaseId: REPLACEMENT_DATABASE_ID,
        location: backend.exportLocation,
        sha256: 'a'.repeat(64),
        size: 42,
      },
    });

    await expect(
      decommissionDeployment({ backend, store, spec: spec() }),
    ).rejects.toThrow(
      'bounded decommission database export result is malformed',
    );
    expect(effectiveLifecyclePhase(store.record as FleetRecord)).toBe(
      'application-resources-deleted',
    );
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
      id: DATABASE_ID,
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
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    if (!store.record) throw new Error('missing test record');
    store.record = {
      ...store.record,
      phase: 'worker-deployed',
      invocationAuthority: { version: 1, authorizedAt: null },
    };
    backend.cleanupFailAt = 'delete-worker';
    backend.events.length = 0;

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).rejects.toThrow(/failed to clean/);
    expect(backend.events).not.toContain('delete-database');
    expect(backend.databaseExists).toBe(true);
    expect(store.record?.phase).toBe('cleanup-advancing');
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

  it('ignores cleanup for an unregistered deployment without touching the lease or backend', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).resolves.toBeUndefined();
    expect(store.leaseCalls).toBe(0);
    expect(backend.events).toEqual([]);
    expect(backend.findDatabaseCalls).toBe(0);
  });

  it('resumes an active cleanup intent of either authority through the manual drain', async () => {
    const backend = new FakeBackend('plain-worker');
    backend.failAt = 'worker';
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
    const intent = store.record?.cleanupIntent;
    if (!intent) throw new Error('missing durable rollback intent');
    expect(intent.authority).toMatchObject({ kind: 'provisioning-rollback' });
    expect(store.record?.phase).toBe('cleanup-advancing');

    backend.failAt = undefined;
    backend.cleanupFailAt = undefined;
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).resolves.toBeUndefined();
    expect(store.record).toBeUndefined();
    // The drain resumed the persisted rollback operation instead of minting
    // a new manual one.
    expect(store.receipts.get(intent.operationId)).toMatchObject({
      authority: 'provisioning-rollback',
      disposition: 'prepublication-owned-no-export',
    });
  });

  it('reports cleanup-advancing lifecycle phases exactly and fails closed on inconsistent pairs', () => {
    const base: FleetRecord = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'plain-worker',
      scriptName: 'acme-production',
      databaseId: DATABASE_ID,
      databaseName: 'acme-production',
      schemaVersion: 3,
      artifactVersion: 'artifact-v1',
      desiredSpecDigest: 'a'.repeat(64),
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase: 'cleanup-advancing',
      updatedAt: '2026-08-29T00:00:00.000Z',
    };
    const cleanupIntent: NonNullable<FleetRecord['cleanupIntent']> = {
      version: 1,
      operationId: '9c7b1de2-4c8f-4b9a-8f3e-2a6d5c4b3a21',
      revision: 0,
      generation: 0,
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
      state: { step: 'teardown-traffic' },
    };

    expect(effectiveLifecyclePhase({ ...base, cleanupIntent })).toBe(
      'cleanup-advancing',
    );
    expect(() => effectiveLifecyclePhase(base)).toThrow(
      'cleanup-advancing record has no active cleanup intent',
    );
    expect(() =>
      effectiveLifecyclePhase({ ...base, phase: 'ready', cleanupIntent }),
    ).toThrow('fleet record has inconsistent cleanup intent state');
  });

  it('refuses external-candidate rollback before mutation and preserves the deployment for export-backed decommissioning', async () => {
    const backend = new FakeBackend();
    backend.failAt = 'maintenance';
    const store = new MemoryStore();
    const external = spec({
      authoredBy: 'external',
      durableObjectMigrations: [],
      egressProxyService: undefined,
    });

    const failure = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: external,
      secrets,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as ProvisioningError).cleanupErrors).toEqual([
      expect.objectContaining({
        message:
          'deployment carries an untrusted data binding; use export-backed decommissioning',
      }),
    ]);
    expect(store.record?.phase).toBe('worker-deployed');
    expect(store.record?.cleanupIntent).toBeUndefined();
    expect(backend.events).not.toContain('revoke');
    expect(backend.events).not.toContain('delete-worker');
    expect(backend.events).not.toContain('delete-database');

    // The row kept its phase, so a provisioning retry still succeeds.
    backend.failAt = undefined;
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: external,
        secrets,
      }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });
  });

  it('writes the never-authorized invocation carrier on the first durable put', async () => {
    class FirstPutProbeStore extends MemoryStore {
      firstPut: FleetRecord | undefined;

      override async put(record: FleetRecord): Promise<void> {
        this.firstPut ??= structuredClone(record);
        await super.put(record);
      }
    }
    const backend = new FakeBackend('plain-worker');
    const store = new FirstPutProbeStore();

    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });

    expect(store.firstPut?.phase).toBe('database-reserved');
    expect(store.firstPut?.invocationAuthority).toEqual({
      version: 1,
      authorizedAt: null,
    });
    expect(typeof store.record?.invocationAuthority?.authorizedAt).toBe(
      'string',
    );
  });

  it('redirects provisioning of a cleanup-advancing row to the bounded cleanup drain', async () => {
    const backend = new FakeBackend('plain-worker');
    backend.failAt = 'worker';
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
    expect(store.record?.phase).toBe('cleanup-advancing');

    backend.failAt = undefined;
    backend.cleanupFailAt = undefined;
    backend.events.length = 0;
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).rejects.toThrow(
      "deployment 'acme:production' has an active bounded cleanup; complete it with cleanupDeploymentArtifacts() or advanceCleanupDeployment() before provisioning again",
    );
    expect(backend.events).toEqual([]);

    // Complete the cleanup, then the key reprovisions fresh.
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).resolves.toBeUndefined();
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });
  });

  it('returns the bounded rollback outcome through ProvisioningError.cleanup when failureCleanup is bounded', async () => {
    const backend = new FakeBackend('plain-worker');
    backend.failAt = 'worker';
    const store = new MemoryStore();

    const failure = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
      failureCleanup: 'bounded',
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as ProvisioningError).message).toBe(
      "failed to provision 'acme:production'",
    );
    expect((failure as ProvisioningError).cleanup).toMatchObject({
      status: 'pending',
      token: {
        version: 1,
        tenantTag: 'acme',
        environment: 'production',
        revision: 1,
      },
    });
    // Exactly one bounded group advanced.
    expect(store.record?.cleanupIntent?.state).toEqual({
      step: 'teardown-worker',
    });

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).resolves.toBeUndefined();
    expect(store.record).toBeUndefined();

    // The default drain keeps the historical error shape.
    const drainBackend = new FakeBackend('plain-worker');
    drainBackend.failAt = 'worker';
    const drainStore = new MemoryStore();
    const drainFailure = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend: drainBackend,
      store: drainStore,
      spec: spec(),
      secrets,
    }).catch((error: unknown) => error);
    expect(drainFailure).toBeInstanceOf(ProvisioningError);
    expect((drainFailure as ProvisioningError).cleanup).toBeUndefined();
    expect(drainStore.record).toBeUndefined();
  });

  it('commits the invocation authority durably before each candidate-invoking dispatch', async () => {
    class FlipTimelineStore extends MemoryStore {
      constructor(private readonly timeline: string[]) {
        super();
      }

      override async put(record: FleetRecord): Promise<void> {
        await super.put(record);
        const carrier = record.invocationAuthority;
        this.timeline.push(
          `put:${record.phase}:${
            carrier
              ? carrier.authorizedAt === null
                ? 'null'
                : 'authorized'
              : 'absent'
          }`,
        );
      }
    }

    const plain = new FakeBackend('plain-worker');
    const plainStore = new FlipTimelineStore(plain.events);
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend: plain,
      store: plainStore,
      spec: spec(),
      secrets,
    });
    const timeline = plain.events;
    const workerDeployedPut = timeline.indexOf('put:worker-deployed:null');
    const flipPut = timeline.indexOf('put:worker-deployed:authorized');
    // The flip never rides the worker-deployed put: that phase stays
    // no-export-eligible until the maintenance request is authorized.
    expect(workerDeployedPut).toBeGreaterThanOrEqual(0);
    expect(flipPut).toBeGreaterThan(workerDeployedPut);
    expect(flipPut).toBeLessThan(timeline.indexOf('maintenance'));
    expect(timeline).not.toContain('put:publishing:null');
    expect(timeline.indexOf('put:publishing:authorized')).toBeLessThan(
      timeline.indexOf('promote'),
    );
    expect(timeline[0]).toBe('put:database-reserved:null');

    const external = new FakeBackend();
    const externalStore = new FlipTimelineStore(external.events);
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend: external,
      store: externalStore,
      spec: spec(),
      secrets,
    });
    const externalTimeline = external.events;
    // External uploads dispatch the candidate, so the flip commits before
    // deployWorker on immutable-external backends.
    expect(
      externalTimeline.indexOf('put:application-resources-deployed:authorized'),
    ).toBeLessThan(externalTimeline.indexOf('worker'));
  });

  it('aborts before dispatch when the invocation-authority flip cannot commit', async () => {
    class FlipFailureStore extends MemoryStore {
      failFlipOnce = true;

      override async put(record: FleetRecord): Promise<void> {
        if (
          this.failFlipOnce &&
          record.phase === 'worker-deployed' &&
          typeof record.invocationAuthority?.authorizedAt === 'string'
        ) {
          this.failFlipOnce = false;
          throw new Error('flip write rejected');
        }
        await super.put(record);
      }
    }
    const backend = new FakeBackend('plain-worker');
    const store = new FlipFailureStore();

    const failure = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as Error).cause).toMatchObject({
      message: 'flip write rejected',
    });
    // The rejected flip aborted the flow before the maintenance dispatch.
    expect(backend.events).not.toContain('maintenance');
  });

  it('refuses no-export rollback after the maintenance flip and preserves the worker-deployed row', async () => {
    const backend = new FakeBackend('plain-worker');
    backend.failAt = 'maintenance';
    const store = new MemoryStore();

    const failure = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as ProvisioningError).cleanupErrors).toEqual([
      expect.objectContaining({
        message:
          'deployment candidate invocation was durably authorized; use export-backed decommissioning',
      }),
    ]);
    // The provider failure landed after the committed flip: the carrier
    // stays authorized and the deployment is preserved whole.
    expect(store.record?.phase).toBe('worker-deployed');
    expect(typeof store.record?.invocationAuthority?.authorizedAt).toBe(
      'string',
    );
    expect(backend.events).not.toContain('delete-worker');
    expect(backend.events).not.toContain('delete-database');

    backend.failAt = undefined;
    await expect(
      provisionDeployment({
        initialExecutionFenceState: 'open',
        backend,
        store,
        spec: spec(),
        secrets,
      }),
    ).resolves.toMatchObject({ record: { phase: 'ready' } });
  });

  it('errors after a second blocked drain result and leaves the blocked intent restartable', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    if (!store.record) throw new Error('missing provisioned record');
    store.record = {
      ...store.record,
      phase: 'worker-deployed',
      invocationAuthority: { version: 1, authorizedAt: null },
    };
    const attached: DecommissionAttachmentScanResult = {
      status: 'attached',
      attachment: { plane: 'ordinary', scriptName: 'holder-script' },
      providerFetchAttemptsReserved: 3,
    };
    backend.scanResults.push(attached, attached);

    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).rejects.toThrow('bounded cleanup remains blocked by a Worker attachment');
    expect(store.record?.cleanupIntent?.state).toMatchObject({
      step: 'blocked',
      attachment: { plane: 'ordinary', scriptName: 'holder-script' },
    });

    // After remediation the drain restarts the blocked operation itself.
    await expect(
      cleanupDeploymentArtifacts({ backend, store, spec: spec() }),
    ).resolves.toBeUndefined();
    expect(store.record).toBeUndefined();
  });

  it('releases current claims on force decommission while preserving receipts and the legacy fallback', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    store.supportsDeleteReleasingClaims = true;
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    const receipt: CleanupTerminalReceipt = {
      version: 1,
      operationId: '3d1f6a70-58c8-4b52-9d68-0f1f6f1c2ab3',
      tenantTag: 'acme',
      environment: 'production',
      backend: 'plain-worker',
      scriptName: 'acme-production',
      databaseId: DATABASE_ID,
      databaseName: 'acme-production',
      authority: 'manual-cleanup',
      admittedPhase: 'worker-deployed',
      disposition: 'prepublication-owned-no-export',
      evidence: {
        eligibility: 'carrier-null',
        ingressRemoved: true,
        workerAbsent: true,
        platformResourcesAbsent: true,
        applicationR2Settled: true,
        databaseAbsentReadback: true,
      },
      completedAtMs: 7,
    };
    store.receipts.set(receipt.operationId, receipt);

    await forceDecommissionDeployment({
      backend,
      store,
      tenantTag: 'acme',
      environment: 'production',
    });
    expect(store.record).toBeUndefined();
    expect(store.deleteReleasingClaimsCalls).toBe(1);
    expect(store.deleteCalls).toBe(0);
    // Force never reads or deletes historical receipts.
    expect(store.receipts.get(receipt.operationId)).toEqual(receipt);

    const legacyBackend = new FakeBackend('plain-worker');
    const legacyStore = new MemoryStore();
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend: legacyBackend,
      store: legacyStore,
      spec: spec(),
      secrets,
    });
    await forceDecommissionDeployment({
      backend: legacyBackend,
      store: legacyStore,
      tenantTag: 'acme',
      environment: 'production',
    });
    // A legacy lease without deleteReleasingClaims keeps tombstone claims
    // through the plain row delete.
    expect(legacyStore.record).toBeUndefined();
    expect(legacyStore.deleteReleasingClaimsCalls).toBe(0);
    expect(legacyStore.deleteCalls).toBe(1);
  });

  it('refuses force decommission during an active cleanup', async () => {
    const backend = new FakeBackend('plain-worker');
    backend.failAt = 'worker';
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
    expect(store.record?.phase).toBe('cleanup-advancing');
    const before = store.record;

    await expect(
      forceDecommissionDeployment({
        backend,
        store,
        tenantTag: 'acme',
        environment: 'production',
      }),
    ).rejects.toThrow(
      'forceDecommissionDeployment cannot run during an active cleanup',
    );
    expect(backend.forceSteps).toEqual([]);
    expect(store.record).toEqual(before);
  });

  it('refuses reprovisioning over foreign physical residue after a forced decommission', async () => {
    const backend = new FakeBackend('plain-worker');
    const store = new MemoryStore();
    store.supportsDeleteReleasingClaims = true;
    await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    });
    await forceDecommissionDeployment({
      backend,
      store,
      tenantTag: 'acme',
      environment: 'production',
    });
    expect(store.record).toBeUndefined();

    // A residual physical database with the reserved name survives force;
    // provisioning fails closed instead of adopting it.
    backend.databaseExists = true;
    backend.databaseOwner = undefined;
    const failure = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: spec(),
      secrets,
    }).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(ProvisioningError);
    expect((failure as Error).cause).toMatchObject({
      message: expect.stringMatching(/refusing to claim pre-existing database/),
    });
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
    const transparentAction = new Proxy({ kind: 'start' as const }, {});
    const revokedAction = Proxy.revocable({ kind: 'start' as const }, {});
    revokedAction.revoke();
    for (const action of [
      transparentAction,
      revokedAction.proxy,
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
        databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
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
    let d1Current = await driveBoundedUntil(
      d1Blocked,
      (record) =>
        record.decommissionIntent?.lifecyclePhase ===
          'application-resources-deleted' &&
        record.decommissionIntent.state === 'transitioning',
    );
    d1Current = await continueBoundedDecommission(d1Blocked, d1Current);
    d1Blocked.backend.scanResults.push({
      status: 'attached',
      attachment: { plane: 'ordinary', scriptName: 'consumer' },
      providerFetchAttemptsReserved: 3,
    });
    const d1BlockedResult = await continueBoundedDecommission(
      d1Blocked,
      d1Current,
    );
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(d1Blocked, {
          kind: 'restart-blocked',
          token: d1BlockedResult.token,
        }),
      ),
    ).resolves.toMatchObject({ status: 'pending' });
    expect(d1Blocked.store.record?.decommissionIntent).toMatchObject({
      state: 'discover',
      purpose: { kind: 'database-pre-export' },
    });
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
    const transparentResult = new Proxy({ status: 'drift' as const }, {});
    let revokeResult!: () => void;
    const revokedResult = Proxy.revocable(
      { status: 'drift' as const },
      {
        ownKeys(target) {
          const keys = Reflect.ownKeys(target);
          revokeResult();
          return keys;
        },
      },
    );
    revokeResult = revokedResult.revoke;
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
        label: 'transparent proxy',
        row: transparentResult,
        reads: () => 0,
        expectedReads: 0,
      },
      {
        label: 'revoked proxy',
        row: revokedResult.proxy,
        reads: () => 0,
        expectedReads: 0,
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
      DATABASE_ID,
      DATABASE_ID,
      DATABASE_ID,
      DATABASE_ID,
      DATABASE_ID,
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

  it('orders two resources without rewind and selects authority at the D1 boundary', async () => {
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
    const selected = await continueBoundedDecommission(harness, result);
    expect(selected.token.revision).toBe(result.token.revision + 1);
    expect(harness.store.record?.decommissionIntent).toMatchObject({
      state: 'discover',
      purpose: { kind: 'database-pre-export' },
      databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
    });
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

  it('selects one immutable receipt authority before bounded D1 work', async () => {
    const startedHarness = await boundedDecommissionHarness();
    const started = await startBoundedDecommission(startedHarness);
    expect(startedHarness.store.record?.decommissionIntent).not.toHaveProperty(
      'databaseExportReceiptAuthority',
    );
    expect(startedHarness.backend.receiptCalls).toEqual([]);

    let boundary = started;
    for (let step = 0; step < 16; step += 1) {
      if (
        startedHarness.store.record?.decommissionIntent?.lifecyclePhase ===
        'application-resources-deleted'
      ) {
        break;
      }
      boundary = await continueBoundedDecommission(startedHarness, boundary);
      expect(
        startedHarness.store.record?.decommissionIntent,
      ).not.toHaveProperty('databaseExportReceiptAuthority');
    }
    const beforeSelection = startedHarness.store.record;
    const selected = await continueBoundedDecommission(
      startedHarness,
      boundary,
    );
    expect(startedHarness.store.record).toMatchObject({
      phase: 'decommission-advancing',
      decommissionIntent: {
        revision: (beforeSelection?.decommissionIntent?.revision as number) + 1,
        generation:
          (beforeSelection?.decommissionIntent?.generation as number) + 1,
        databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
        lifecyclePhase: 'application-resources-deleted',
        state: 'discover',
        purpose: { kind: 'database-pre-export', databaseId: DATABASE_ID },
      },
    });
    expect(selected).toMatchObject({ status: 'pending' });

    for (const row of [
      {
        label: 'absent pair',
        authority: undefined,
        method: undefined,
        message: 'backend cannot write idempotent database export receipts',
      },
      {
        label: 'authority only',
        authority: RECEIPT_AUTHORITY,
        method: undefined,
        message: 'database export receipt capability is malformed',
      },
      {
        label: 'method only',
        authority: undefined,
        method: async () => ({}),
        message: 'database export receipt capability is malformed',
      },
      {
        label: 'non-callable',
        authority: RECEIPT_AUTHORITY,
        method: 1,
        message: 'database export receipt capability is malformed',
      },
      {
        label: 'empty authority',
        authority: '',
        method: async () => ({}),
        message: 'database export receipt capability is malformed',
      },
      {
        label: 'over-bound authority',
        authority: 'x'.repeat(4_097),
        method: async () => ({}),
        message: 'database export receipt capability is malformed',
      },
    ] as const) {
      const harness = await boundedDecommissionHarness();
      const backend = new Proxy(harness.backend, {
        get(target, property) {
          if (property === 'databaseExportReceiptAuthority') {
            return row.authority;
          }
          if (property === 'exportDatabaseReceipt') return row.method;
          const value = Reflect.get(target, property, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(harness, { kind: 'start' }, { backend }),
        ),
        row.label,
      ).rejects.toThrow(row.message);
      expect(
        harness.store.record?.decommissionIntent,
        row.label,
      ).toBeUndefined();
    }

    const throwing = await boundedDecommissionHarness();
    const backend = new Proxy(throwing.backend, {
      get(target, property) {
        if (property === 'databaseExportReceiptAuthority') {
          throw new Error('receipt getter trap');
        }
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(throwing, { kind: 'start' }, { backend }),
      ),
    ).rejects.toThrow('database export receipt capability is malformed');

    const invalidIdentity = await boundedDecommissionHarness();
    invalidIdentity.backend.databaseId = 'not-a-uuid';
    invalidIdentity.store.record = {
      ...(invalidIdentity.store.record as FleetRecord),
      databaseId: 'not-a-uuid',
    };
    await expect(startBoundedDecommission(invalidIdentity)).rejects.toThrow(
      'database export receipt identity is malformed',
    );
    expect(invalidIdentity.store.record?.decommissionIntent).toBeUndefined();

    for (const state of [
      'reserved',
      'create-authorized',
      'created',
      'detach-authorized',
      'detached',
      'empty-authorized',
      'empty',
      'delete-authorized',
    ] as const) {
      const fenced = await boundedDecommissionHarness({ r2Names: ['FILES'] });
      const record = fenced.store.record as FleetRecord;
      fenced.store.record = {
        ...record,
        phase: 'application-resources-deleted',
        applicationResources: record.applicationResources?.map((resource) => ({
          ...resource,
          state,
        })) as FleetRecord['applicationResources'],
      };
      let receiptGetterReads = 0;
      const fencedBackend = new Proxy(fenced.backend, {
        get(target, property) {
          if (
            property === 'databaseExportReceiptAuthority' ||
            property === 'exportDatabaseReceipt'
          ) {
            receiptGetterReads += 1;
          }
          const value = Reflect.get(target, property, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(
            fenced,
            { kind: 'start' },
            { backend: fencedBackend },
          ),
        ),
        state,
      ).rejects.toThrow(
        'normal decommission D1 work requires every application R2 resource to be deleted',
      );
      expect(receiptGetterReads, state).toBe(0);
      expect(fenced.backend.databaseIdsRead, state).toEqual([]);
      expect(fenced.store.record?.decommissionIntent, state).toBeUndefined();
    }

    const changed = new Proxy(startedHarness.backend, {
      get(target, property) {
        if (property === 'databaseExportReceiptAuthority') {
          return 'memory://different-authority/receipts/v1';
        }
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const readsBeforeChangedAuthority =
      startedHarness.backend.databaseIdsRead.length;
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          startedHarness,
          { kind: 'continue', token: selected.token },
          { backend: changed },
        ),
      ),
    ).rejects.toThrow(
      'database export receipt authority differs from configured authority',
    );
    expect(startedHarness.backend.databaseIdsRead).toHaveLength(
      readsBeforeChangedAuthority,
    );

    for (const mutation of ['backend', 'mapping', 'digest'] as const) {
      const authority = await boundedDecommissionHarness();
      let current = await driveBoundedUntil(
        authority,
        (record) =>
          record.decommissionIntent?.lifecyclePhase ===
            'application-resources-deleted' &&
          record.decommissionIntent.state === 'transitioning',
      );
      current = await continueBoundedDecommission(authority, current);
      let receiptPropertyReads = 0;
      const mutatedBackend = new Proxy(authority.backend, {
        get(target, property) {
          if (
            property === 'databaseExportReceiptAuthority' ||
            property === 'exportDatabaseReceipt'
          ) {
            receiptPropertyReads += 1;
          }
          if (mutation === 'backend' && property === 'kind') {
            return 'workers-for-platforms';
          }
          const value = Reflect.get(target, property, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      const mutatedSpec =
        mutation === 'mapping'
          ? { ...authority.deployment, databaseName: 'different-database' }
          : mutation === 'digest'
            ? {
                ...authority.deployment,
                modules: [
                  {
                    name: 'worker.js',
                    content: 'export default { changed: true }',
                  },
                ],
              }
            : authority.deployment;
      const databaseReads = authority.backend.databaseIdsRead.length;
      const scans = authority.backend.scanInputs.length;
      const residuals = authority.backend.residualCalls;
      const exports = authority.backend.receiptCalls.length;
      const deletes = authority.backend.events.filter(
        (event) => event === 'delete-database',
      ).length;
      const writes = authority.store.phases.length;
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(
            authority,
            { kind: 'continue', token: current.token },
            { backend: mutatedBackend, spec: mutatedSpec },
          ),
        ),
        mutation,
      ).rejects.toThrow();
      expect(receiptPropertyReads, mutation).toBe(0);
      expect(authority.backend.databaseIdsRead, mutation).toHaveLength(
        databaseReads,
      );
      expect(authority.backend.scanInputs, mutation).toHaveLength(scans);
      expect(authority.backend.residualCalls, mutation).toBe(residuals);
      expect(authority.backend.receiptCalls, mutation).toHaveLength(exports);
      expect(
        authority.backend.events.filter((event) => event === 'delete-database'),
        mutation,
      ).toHaveLength(deletes);
      expect(authority.store.phases, mutation).toHaveLength(writes);
    }
  });

  it('advances pre-export and pre-delete scans without persisting absence authority', async () => {
    const harness = await boundedDecommissionHarness();
    let result = await driveBoundedUntil(
      harness,
      (record) =>
        record.decommissionIntent?.lifecyclePhase ===
          'application-resources-deleted' &&
        record.decommissionIntent.state === 'transitioning',
    );
    result = await continueBoundedDecommission(harness, result);
    const preExportDiscover = harness.store.record?.decommissionIntent;
    expect(preExportDiscover).toMatchObject({
      state: 'discover',
      purpose: { kind: 'database-pre-export', databaseId: DATABASE_ID },
      databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
    });
    expect(preExportDiscover).not.toHaveProperty('discoverEvidence');

    const signal = AbortSignal.abort(new Error('scan cancelled'));
    harness.backend.scanResults.push({
      status: 'pending',
      progress: (
        preExportDiscover as Extract<
          DecommissionAdvanceIntent,
          { state: 'discover' }
        >
      ).progress,
      providerFetchAttemptsReserved: 3,
    });
    result = await advanceDecommissionDeployment(
      boundedAdvanceOptions(
        harness,
        { kind: 'continue', token: result.token },
        { signal },
      ),
    );
    expect(harness.backend.scanInputs.at(-1)?.signal).toBe(signal);
    expect(harness.store.record?.decommissionIntent).toMatchObject({
      state: 'discover',
      purpose: { kind: 'database-pre-export' },
    });
    expect(harness.store.record?.decommissionIntent).not.toHaveProperty(
      'discoverEvidence',
    );

    result = await continueBoundedDecommission(harness, result);
    expect(harness.store.record?.decommissionIntent).toMatchObject({
      state: 'verify',
      purpose: { kind: 'database-pre-export' },
      discoverEvidence: { evidenceSha256: 'a'.repeat(64), evidenceCount: 2 },
    });
    result = await continueBoundedDecommission(harness, result);
    result = await continueBoundedDecommission(harness, result);
    expect(harness.store.record?.decommissionIntent).toMatchObject({
      state: 'discover',
      purpose: {
        kind: 'database-pre-delete',
        databaseId: DATABASE_ID,
        exportLocation: expect.any(String),
        exportSha256: 'a'.repeat(64),
        exportSize: 42,
      },
    });
    expect(harness.store.record?.decommissionIntent).not.toHaveProperty(
      'discoverEvidence',
    );
    result = await continueBoundedDecommission(harness, result);
    expect(harness.store.record?.decommissionIntent).toMatchObject({
      state: 'verify',
      purpose: { kind: 'database-pre-delete' },
    });
    expect(result.status).toBe('pending');
  });

  it('blocks D1 teardown on attachments drift and independent evidence mismatch', async () => {
    const drift = await boundedDecommissionHarness();
    let driftResult = await driveBoundedUntil(
      drift,
      (record) =>
        record.decommissionIntent?.lifecyclePhase ===
          'application-resources-deleted' &&
        record.decommissionIntent.state === 'transitioning',
    );
    driftResult = await continueBoundedDecommission(drift, driftResult);
    const driftGeneration = drift.store.record?.decommissionIntent?.generation;
    drift.backend.scanResults.push({ status: 'drift' });
    driftResult = await continueBoundedDecommission(drift, driftResult);
    expect(drift.store.record?.decommissionIntent).toMatchObject({
      state: 'discover',
      generation: (driftGeneration as number) + 1,
      purpose: { kind: 'database-pre-export' },
    });
    expect(driftResult.status).toBe('pending');

    for (const mismatch of [
      { evidenceSha256: 'b'.repeat(64), evidenceCount: 2 },
      { evidenceSha256: 'a'.repeat(64), evidenceCount: 3 },
    ]) {
      const harness = await boundedDecommissionHarness();
      const verify = await driveToPreExportVerify(harness);
      const generation = harness.store.record?.decommissionIntent?.generation;
      harness.backend.scanResults.push({
        status: 'complete',
        ...mismatch,
        providerFetchAttemptsReserved: 3,
      });
      await continueBoundedDecommission(harness, verify);
      expect(harness.store.record?.decommissionIntent).toMatchObject({
        state: 'discover',
        generation: (generation as number) + 1,
        purpose: { kind: 'database-pre-export' },
      });
      expect(harness.backend.receiptCalls).toEqual([]);
    }

    for (const attachment of [
      { plane: 'ordinary' as const, scriptName: 'foreign-worker' },
      {
        plane: 'dispatch' as const,
        scriptName: 'foreign-dispatch',
        dispatchNamespace: 'fleet',
      },
    ]) {
      const harness = await boundedDecommissionHarness();
      let result = await driveBoundedUntil(
        harness,
        (record) =>
          record.decommissionIntent?.lifecyclePhase ===
            'application-resources-deleted' &&
          record.decommissionIntent.state === 'transitioning',
      );
      result = await continueBoundedDecommission(harness, result);
      harness.backend.scanResults.push({
        status: 'attached',
        attachment,
        providerFetchAttemptsReserved: 3,
      });
      result = await continueBoundedDecommission(harness, result);
      expect(result).toMatchObject({
        status: 'blocked',
        purpose: { kind: 'database-pre-export' },
        attachment,
      });
      const scans = harness.backend.scanInputs.length;
      const writes = harness.store.phases.length;
      const inert = await continueBoundedDecommission(harness, result);
      expect(inert).toEqual(result);
      expect(harness.backend.scanInputs).toHaveLength(scans);
      expect(harness.store.phases).toHaveLength(writes);
      const restarted = await advanceDecommissionDeployment(
        boundedAdvanceOptions(harness, {
          kind: 'restart-blocked',
          token: result.token,
        }),
      );
      expect(restarted).toMatchObject({ status: 'pending' });
      expect(harness.store.record?.decommissionIntent).toMatchObject({
        state: 'discover',
        purpose: { kind: 'database-pre-export' },
      });
    }
  });

  it('consumes matching pre-export verification through residuals and one stable receipt', async () => {
    const harness = await boundedDecommissionHarness();
    const verify = await driveToPreExportVerify(harness);
    const residualsBeforeExport = harness.backend.residualCalls;
    harness.backend.receiptOutcomes.push({
      status: 'fulfilled',
      value: {
        databaseId: DATABASE_ID,
        location: 'memory://receipt/export.sql',
        sha256: 'a'.repeat(64),
        size: 42,
        secret: 'must-not-persist',
      },
    });
    await continueBoundedDecommission(harness, verify);
    expect(harness.backend.residualCalls).toBe(residualsBeforeExport + 1);
    expect(harness.backend.receiptCalls).toEqual([
      {
        version: 1,
        authority: RECEIPT_AUTHORITY,
        databaseId: DATABASE_ID,
        operationId: DECOMMISSION_OPERATION_ID,
      },
    ]);
    expect(harness.store.record).toMatchObject({
      databaseExportLocation: 'memory://receipt/export.sql',
      databaseExportSha256: 'a'.repeat(64),
      databaseExportSize: 42,
      decommissionIntent: {
        lifecyclePhase: 'database-exported',
        state: 'transitioning',
        databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
      },
    });
    expect(JSON.stringify(harness.store.record)).not.toContain(
      'must-not-persist',
    );

    const residual = await boundedDecommissionHarness();
    const residualVerify = await driveToPreExportVerify(residual);
    const residualFailure = new Error('residual check failed');
    residual.backend.assertDatabaseDeletionResidualsRemoved = async () => {
      throw residualFailure;
    };
    await expect(
      continueBoundedDecommission(residual, residualVerify),
    ).rejects.toBe(residualFailure);
    expect(residual.backend.receiptCalls).toEqual([]);
    expect(residual.store.record?.decommissionIntent?.state).toBe('verify');

    const lease = await boundedDecommissionHarness();
    const leaseVerify = await driveToPreExportVerify(lease);
    const exportOrder: string[] = [];
    lease.backend.assertDatabaseDeletionResidualsRemoved = async () => {
      lease.backend.residualCalls += 1;
      exportOrder.push('residual');
    };
    lease.store.assertOwnedCalls = 0;
    lease.store.assertOwnedFailureAt = 1;
    lease.store.assertOwnedObserved = () => exportOrder.push('fence');
    const leaseWrites = lease.store.phases.length;
    await expect(
      continueBoundedDecommission(lease, leaseVerify),
    ).rejects.toThrow('lease assertion 1 failed');
    expect(exportOrder).toEqual(['residual', 'fence']);
    expect(lease.backend.receiptCalls).toEqual([]);
    expect(lease.store.phases).toHaveLength(leaseWrites);
    expect(lease.store.record?.decommissionIntent?.state).toBe('verify');

    const preScanIdentity = await boundedDecommissionHarness();
    const preScanVerify = await driveToPreExportVerify(preScanIdentity);
    preScanIdentity.backend.databaseName = 'unexpected-name';
    const preScanCount = preScanIdentity.backend.scanInputs.length;
    await expect(
      continueBoundedDecommission(preScanIdentity, preScanVerify),
    ).rejects.toThrow('resolved with unexpected identity');
    expect(preScanIdentity.backend.scanInputs).toHaveLength(preScanCount);
    expect(preScanIdentity.backend.receiptCalls).toEqual([]);

    const postScanIdentity = await boundedDecommissionHarness();
    const postScanVerify = await driveToPreExportVerify(postScanIdentity);
    postScanIdentity.backend.scanAfter = () => {
      postScanIdentity.backend.databaseName = 'changed-after-scan';
    };
    await expect(
      continueBoundedDecommission(postScanIdentity, postScanVerify),
    ).rejects.toThrow('resolved with unexpected identity');
    expect(postScanIdentity.backend.receiptCalls).toEqual([]);

    const malformedReference = await boundedDecommissionHarness();
    const malformedReferenceVerify =
      await driveToPreExportVerify(malformedReference);
    malformedReference.backend.databaseReadOutcomes.push({
      status: 'fulfilled',
      value: new Proxy(
        { id: DATABASE_ID, name: 'acme-production', created: false },
        {},
      ),
    });
    await expect(
      continueBoundedDecommission(malformedReference, malformedReferenceVerify),
    ).rejects.toThrow('persisted database reference is malformed');

    const malformedOwner = await boundedDecommissionHarness();
    const malformedOwnerVerify = await driveToPreExportVerify(malformedOwner);
    malformedOwner.backend.databaseOwner = false;
    await expect(
      continueBoundedDecommission(malformedOwner, malformedOwnerVerify),
    ).rejects.toThrow('persisted database owner is malformed');
    expect(malformedOwner.backend.receiptCalls).toEqual([]);

    const exactReferenceText = 'é'.repeat(2_048);
    const exactOwnerText = 'é'.repeat(2_048);
    expect(exactReferenceText).toHaveLength(2_048);
    expect(new TextEncoder().encode(exactReferenceText)).toHaveLength(4_096);
    expect(exactOwnerText).toHaveLength(2_048);
    expect(new TextEncoder().encode(exactOwnerText)).toHaveLength(4_096);
    const exactReferenceBackend = new FakeBackend();
    exactReferenceBackend.getDatabase = async () => ({
      id: exactReferenceText,
      name: exactReferenceText,
      created: false,
    });
    exactReferenceBackend.databaseOwner = exactOwnerText;
    await expect(
      reconcilePersistedDatabase(
        exactReferenceBackend,
        {
          databaseId: exactReferenceText,
          databaseName: exactReferenceText,
          tenantTag: exactOwnerText,
        },
        false,
        {
          mutationLeaseTtlMs: 1_000,
          assertOwned: async () => {},
        },
        true,
      ),
    ).resolves.toEqual({
      id: exactReferenceText,
      name: exactReferenceText,
      created: false,
    });

    const multibyteOverBound = `${'é'.repeat(2_048)}x`;
    expect(multibyteOverBound).toHaveLength(2_049);
    expect(new TextEncoder().encode(multibyteOverBound)).toHaveLength(4_097);
    const overReferenceBackend = new FakeBackend();
    overReferenceBackend.getDatabase = async () => ({
      id: multibyteOverBound,
      name: 'database',
      created: false,
    });
    await expect(
      reconcilePersistedDatabase(
        overReferenceBackend,
        {
          databaseId: multibyteOverBound,
          databaseName: 'database',
          tenantTag: 'acme',
        },
        false,
        {
          mutationLeaseTtlMs: 1_000,
          assertOwned: async () => {},
        },
        true,
      ),
    ).rejects.toThrow('persisted database reference is malformed');
    expect(overReferenceBackend.receiptCalls).toEqual([]);

    const overOwnerBackend = new FakeBackend();
    overOwnerBackend.databaseExists = true;
    overOwnerBackend.databaseOwner = multibyteOverBound;
    await expect(
      reconcilePersistedDatabase(
        overOwnerBackend,
        {
          databaseId: DATABASE_ID,
          databaseName: 'acme-production',
          tenantTag: multibyteOverBound,
        },
        false,
        {
          mutationLeaseTtlMs: 1_000,
          assertOwned: async () => {},
        },
        true,
      ),
    ).rejects.toThrow('persisted database owner is malformed');
    expect(overOwnerBackend.receiptCalls).toEqual([]);

    const dishonestResults: readonly unknown[] = [
      {
        databaseId: REPLACEMENT_DATABASE_ID,
        location: 'memory://receipt/export.sql',
        sha256: 'a'.repeat(64),
        size: 42,
      },
      {
        databaseId: DATABASE_ID,
        location: '',
        sha256: 'a'.repeat(64),
        size: 42,
      },
      {
        databaseId: DATABASE_ID,
        location: 'memory://receipt/export.sql',
        sha256: 'A'.repeat(64),
        size: 42,
      },
      {
        databaseId: DATABASE_ID,
        location: 'memory://receipt/export.sql',
        sha256: 'a'.repeat(64),
        size: 0,
      },
      new Proxy(
        {
          databaseId: DATABASE_ID,
          location: 'memory://receipt/export.sql',
          sha256: 'a'.repeat(64),
          size: 42,
        },
        {},
      ),
      Object.defineProperty(
        {
          databaseId: DATABASE_ID,
          location: 'memory://receipt/export.sql',
          sha256: 'a'.repeat(64),
        },
        'size',
        { enumerable: true, get: () => 42 },
      ),
      new (class PrototypeDatabaseExport {
        readonly databaseId = DATABASE_ID;
        readonly location = 'memory://receipt/export.sql';
        readonly sha256 = 'a'.repeat(64);
        readonly size = 42;
      })(),
    ];
    for (const dishonest of dishonestResults) {
      const candidate = await boundedDecommissionHarness();
      const candidateVerify = await driveToPreExportVerify(candidate);
      candidate.backend.receiptOutcomes.push({
        status: 'fulfilled',
        value: dishonest,
      });
      await expect(
        continueBoundedDecommission(candidate, candidateVerify),
      ).rejects.toThrow(
        'bounded decommission database export result is malformed',
      );
      expect(candidate.store.record?.decommissionIntent?.state).toBe('verify');
    }
  });

  it('converges receipt commits and database-exported writes without a second artifact', async () => {
    const receiptLoss = await boundedDecommissionHarness();
    const verify = await driveToPreExportVerify(receiptLoss);
    const receiptFailure = new Error('receipt response lost after commit');
    receiptLoss.backend.receiptOutcomes.push({
      status: 'rejected',
      reason: receiptFailure,
      commit: true,
    });
    await expect(continueBoundedDecommission(receiptLoss, verify)).rejects.toBe(
      receiptFailure,
    );
    expect(receiptLoss.store.record?.decommissionIntent?.state).toBe('verify');
    expect(receiptLoss.backend.receiptWinners.size).toBe(1);
    await continueBoundedDecommission(receiptLoss, verify);
    expect(receiptLoss.backend.receiptWinners.size).toBe(1);
    expect(receiptLoss.backend.receiptCalls).toHaveLength(2);
    expect(receiptLoss.backend.receiptCalls[1]).toEqual(
      receiptLoss.backend.receiptCalls[0],
    );

    const precommit = await boundedDecommissionHarness();
    const precommitVerify = await driveToPreExportVerify(precommit);
    precommit.store.failPutPhase = 'database-exported';
    await expect(
      continueBoundedDecommission(precommit, precommitVerify),
    ).rejects.toThrow('failed state write at database-exported');
    expect(precommit.store.record?.decommissionIntent?.state).toBe('verify');
    expect(precommit.backend.receiptWinners.size).toBe(1);
    await continueBoundedDecommission(precommit, precommitVerify);
    expect(precommit.backend.receiptWinners.size).toBe(1);
    expect(precommit.backend.receiptCalls).toHaveLength(2);

    const committedStore = new CommitThenThrowStore();
    const committed = await boundedDecommissionHarness({
      store: committedStore,
    });
    const committedVerify = await driveToPreExportVerify(committed);
    committedStore.failAfterCommittedPhase = 'database-exported';
    await expect(
      continueBoundedDecommission(committed, committedVerify),
    ).rejects.toThrow('committing database-exported');
    expect(committed.store.record?.decommissionIntent).toMatchObject({
      lifecyclePhase: 'database-exported',
      state: 'transitioning',
    });
    const receiptCalls = committed.backend.receiptCalls.length;
    const stale = await continueBoundedDecommission(committed, committedVerify);
    expect(stale.token).toEqual(
      expect.objectContaining({
        revision: committed.store.record?.decommissionIntent?.revision,
      }),
    );
    expect(committed.backend.receiptCalls).toHaveLength(receiptCalls);

    for (const Store of [MemoryStore, CommitThenThrowStore] as const) {
      const store = new Store();
      const selection = await boundedDecommissionHarness({ store });
      const atBoundary = await driveBoundedUntil(
        selection,
        (record) =>
          record.decommissionIntent?.lifecyclePhase ===
            'application-resources-deleted' &&
          record.decommissionIntent.state === 'transitioning',
      );
      if (store instanceof CommitThenThrowStore) {
        store.failAfterCommittedPhase = 'application-resources-deleted';
      } else {
        store.failPutPhase = 'application-resources-deleted';
      }
      await expect(
        continueBoundedDecommission(selection, atBoundary),
      ).rejects.toThrow(/application-resources-deleted/u);
      if (store instanceof CommitThenThrowStore) {
        expect(store.record?.decommissionIntent).toMatchObject({
          state: 'discover',
          databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
        });
      } else {
        expect(store.record?.decommissionIntent).toMatchObject({
          state: 'transitioning',
        });
        expect(store.record?.decommissionIntent).not.toHaveProperty(
          'databaseExportReceiptAuthority',
        );
      }
    }
  });

  it('consumes matching pre-delete verification and reconciles delete loss', async () => {
    const liveReference = {
      id: DATABASE_ID,
      name: 'acme-production',
      created: false as const,
    };
    const deletePresentFailure = new Error('delete failed before commit');
    const readbackFailure = new Error('readback unavailable');
    const losingDeleteFailure = new Error('delete rejection must lose');
    const winningReadbackFailure = new Error('readback rejection wins');
    const rows: readonly Readonly<{
      label: string;
      deletion: BoxedOutcome<void>;
      readback: BoxedOutcome<unknown>;
      expected?: unknown;
    }>[] = [
      {
        label: 'fulfill absent',
        deletion: { status: 'fulfilled', value: undefined, commit: true },
        readback: { status: 'fulfilled', value: undefined },
      },
      {
        label: 'reject absent',
        deletion: {
          status: 'rejected',
          reason: new Error('delete response lost'),
          commit: true,
        },
        readback: { status: 'fulfilled', value: undefined },
      },
      {
        label: 'reject present',
        deletion: {
          status: 'rejected',
          reason: deletePresentFailure,
        },
        readback: { status: 'fulfilled', value: liveReference },
        expected: deletePresentFailure,
      },
      {
        label: 'fulfill present',
        deletion: { status: 'fulfilled', value: undefined },
        readback: { status: 'fulfilled', value: liveReference },
        expected: `database '${DATABASE_ID}' remains after deletion`,
      },
      {
        label: 'readback rejects',
        deletion: { status: 'fulfilled', value: undefined, commit: true },
        readback: {
          status: 'rejected',
          reason: readbackFailure,
        },
        expected: readbackFailure,
      },
      {
        label: 'delete and readback reject',
        deletion: {
          status: 'rejected',
          reason: losingDeleteFailure,
        },
        readback: {
          status: 'rejected',
          reason: winningReadbackFailure,
        },
        expected: winningReadbackFailure,
      },
    ];
    for (const row of rows) {
      const harness = await boundedDecommissionHarness();
      const verify = await driveToPreDeleteVerify(harness);
      harness.backend.deleteOutcomes.push(row.deletion);
      harness.backend.databaseReadOutcomes.push(
        { status: 'fulfilled', value: liveReference },
        { status: 'fulfilled', value: liveReference },
        row.readback,
      );
      const operation = continueBoundedDecommission(harness, verify);
      if (row.expected === undefined) {
        await expect(operation, row.label).resolves.toMatchObject({
          status: 'pending',
        });
      } else if (row.expected instanceof Error) {
        const [settled] = await Promise.allSettled([operation]);
        expect(settled.status, row.label).toBe('rejected');
        if (settled.status === 'rejected') {
          expect(settled.reason, row.label).toBe(row.expected);
        }
      } else {
        await expect(operation, row.label).rejects.toThrow(
          String(row.expected),
        );
      }
      expect(harness.store.record?.decommissionIntent, row.label).toMatchObject(
        {
          lifecyclePhase: 'database-deleting',
          state: 'transitioning',
        },
      );
      expect(
        harness.backend.events.filter((event) => event === 'delete-database'),
        row.label,
      ).toHaveLength(1);
    }

    for (const reason of [null, undefined]) {
      const harness = await boundedDecommissionHarness();
      const verify = await driveToPreDeleteVerify(harness);
      harness.backend.deleteOutcomes.push({
        status: 'rejected',
        reason: new Error('delete rejection must lose'),
      });
      harness.backend.databaseReadOutcomes.push(
        { status: 'fulfilled', value: liveReference },
        { status: 'fulfilled', value: liveReference },
        { status: 'rejected', reason },
      );
      const [settled] = await Promise.allSettled([
        continueBoundedDecommission(harness, verify),
      ]);
      expect(settled).toEqual({ status: 'rejected', reason });
    }

    let hostileReadbackFields = 0;
    const hostilePresent = new Proxy(
      {},
      {
        get(_target, property) {
          if (property === 'then') return undefined;
          hostileReadbackFields += 1;
          throw new Error('existence-only readback inspected a field');
        },
        getOwnPropertyDescriptor() {
          hostileReadbackFields += 1;
          throw new Error('existence-only readback inspected a descriptor');
        },
        ownKeys() {
          hostileReadbackFields += 1;
          throw new Error('existence-only readback enumerated fields');
        },
      },
    );
    const hostileReadback = await boundedDecommissionHarness();
    const hostileVerify = await driveToPreDeleteVerify(hostileReadback);
    hostileReadback.backend.deleteOutcomes.push({
      status: 'fulfilled',
      value: undefined,
    });
    hostileReadback.backend.databaseReadOutcomes.push(
      { status: 'fulfilled', value: liveReference },
      { status: 'fulfilled', value: liveReference },
      { status: 'fulfilled', value: hostilePresent },
    );
    await expect(
      continueBoundedDecommission(hostileReadback, hostileVerify),
    ).rejects.toThrow(`database '${DATABASE_ID}' remains after deletion`);
    expect(hostileReadbackFields).toBe(0);

    const barrier = await boundedDecommissionHarness();
    const barrierVerify = await driveToPreDeleteVerify(barrier);
    barrier.store.failPutPhase = 'database-deleting';
    await expect(
      continueBoundedDecommission(barrier, barrierVerify),
    ).rejects.toThrow('failed state write at database-deleting');
    expect(barrier.backend.events).not.toContain('delete-database');

    const committedBarrierStore = new CommitThenThrowStore();
    const committedBarrier = await boundedDecommissionHarness({
      store: committedBarrierStore,
    });
    const committedBarrierVerify =
      await driveToPreDeleteVerify(committedBarrier);
    const deletesBeforeBarrier = committedBarrier.backend.events.filter(
      (event) => event === 'delete-database',
    ).length;
    committedBarrierStore.failAfterCommittedPhase = 'database-deleting';
    await expect(
      continueBoundedDecommission(committedBarrier, committedBarrierVerify),
    ).rejects.toThrow('committing database-deleting');
    expect(committedBarrier.store.record?.decommissionIntent).toMatchObject({
      lifecyclePhase: 'database-deleting',
      state: 'transitioning',
    });
    expect(
      committedBarrier.backend.events.filter(
        (event) => event === 'delete-database',
      ),
    ).toHaveLength(deletesBeforeBarrier);
    let retry = await continueBoundedDecommission(
      committedBarrier,
      committedBarrierVerify,
    );
    for (let index = 0; index < 8 && retry.status !== 'complete'; index += 1) {
      retry = await continueBoundedDecommission(committedBarrier, retry);
    }
    expect(retry.status).toBe('complete');
    expect(
      committedBarrier.backend.events.filter(
        (event) => event === 'delete-database',
      ),
    ).toHaveLength(deletesBeforeBarrier + 1);

    for (const ordinal of [1, 2, 3]) {
      const harness = await boundedDecommissionHarness();
      const verify = await driveToPreDeleteVerify(harness);
      harness.store.assertOwnedCalls = 0;
      harness.store.assertOwnedFailureAt = ordinal;
      const databaseReads = harness.backend.databaseIdsRead.length;
      await expect(
        continueBoundedDecommission(harness, verify),
      ).rejects.toThrow(`lease assertion ${ordinal} failed`);
      expect(harness.store.record?.decommissionIntent).toMatchObject({
        lifecyclePhase: 'database-deleting',
        state: 'transitioning',
      });
      expect(
        harness.backend.events.filter((event) => event === 'delete-database'),
      ).toHaveLength(ordinal === 1 ? 0 : 1);
      expect(harness.backend.databaseIdsRead.length - databaseReads).toBe(
        ordinal === 3 ? 3 : 2,
      );
    }
  });

  it('recovers terminal writes and keeps token classifications authoritative', async () => {
    const terminal = await boundedDecommissionHarness();
    let result = await driveToPreDeleteVerify(terminal);
    result = await continueBoundedDecommission(terminal, result);
    expect(terminal.store.record?.decommissionIntent).toMatchObject({
      lifecyclePhase: 'database-deleting',
      state: 'transitioning',
    });
    expect(terminal.backend.databaseExists).toBe(false);
    const barrierToken = result.token;
    result = await continueBoundedDecommission(terminal, result);
    expect(result).toMatchObject({
      status: 'complete',
      result: {
        record: {
          phase: 'decommissioned',
          decommissionIntent: {
            lifecyclePhase: 'decommissioned',
            state: 'complete',
            databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
          },
        },
      },
    });
    const completeToken = result.token;

    let terminalReceiptGetterReads = 0;
    const capabilityTrap = new Proxy(terminal.backend, {
      get(target, property) {
        if (
          property === 'databaseExportReceiptAuthority' ||
          property === 'exportDatabaseReceipt'
        ) {
          terminalReceiptGetterReads += 1;
          throw new Error('complete read touched receipt capability');
        }
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          terminal,
          { kind: 'start' },
          {
            backend: capabilityTrap,
          },
        ),
      ),
    ).resolves.toMatchObject({ status: 'complete', token: completeToken });
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          terminal,
          { kind: 'continue', token: barrierToken },
          { backend: capabilityTrap },
        ),
      ),
    ).resolves.toMatchObject({ status: 'complete', token: completeToken });
    const terminalEvents = terminal.backend.events.length;
    const terminalDatabaseReads = terminal.backend.databaseIdsRead.length;
    const terminalScans = terminal.backend.scanInputs.length;
    const terminalResiduals = terminal.backend.residualCalls;
    const terminalExports = terminal.backend.receiptCalls.length;
    const terminalWrites = terminal.store.phases.length;
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          terminal,
          { kind: 'continue', token: completeToken },
          { backend: capabilityTrap },
        ),
      ),
    ).resolves.toMatchObject({ status: 'complete', token: completeToken });
    expect(terminal.backend.events).toHaveLength(terminalEvents);
    expect(terminal.backend.databaseIdsRead).toHaveLength(
      terminalDatabaseReads,
    );
    expect(terminal.backend.scanInputs).toHaveLength(terminalScans);
    expect(terminal.backend.residualCalls).toBe(terminalResiduals);
    expect(terminal.backend.receiptCalls).toHaveLength(terminalExports);
    expect(terminal.store.phases).toHaveLength(terminalWrites);

    const leasesBeforeMalformedToken = terminal.store.leaseCalls;
    await expect(
      advanceDecommissionDeployment(
        boundedAdvanceOptions(
          terminal,
          {
            kind: 'continue',
            token: { ...completeToken, revision: -1 },
          },
          { backend: capabilityTrap },
        ),
      ),
    ).rejects.toThrow('decommission advance token is malformed');
    expect(terminal.store.leaseCalls).toBe(leasesBeforeMalformedToken);
    expect(terminal.backend.events).toHaveLength(terminalEvents);
    expect(terminal.backend.databaseIdsRead).toHaveLength(
      terminalDatabaseReads,
    );
    expect(terminal.backend.scanInputs).toHaveLength(terminalScans);
    expect(terminal.backend.residualCalls).toBe(terminalResiduals);
    expect(terminal.backend.receiptCalls).toHaveLength(terminalExports);
    expect(terminal.store.phases).toHaveLength(terminalWrites);
    for (const row of [
      {
        action: { kind: 'continue', token: completeToken, extra: true },
        message: 'decommission advance action is malformed',
      },
      {
        action: {
          kind: 'continue',
          token: { ...completeToken, tenantTag: 'other' },
        },
        message: 'decommission advance token targets another deployment',
      },
      {
        action: {
          kind: 'continue',
          token: { ...completeToken, operationId: crypto.randomUUID() },
        },
        message: 'decommission advance token targets another operation',
      },
      {
        action: {
          kind: 'continue',
          token: { ...completeToken, revision: completeToken.revision + 1 },
        },
        message: 'decommission advance token is from the future',
      },
    ] as const) {
      await expect(
        advanceDecommissionDeployment(
          boundedAdvanceOptions(
            terminal,
            row.action as AdvanceDecommissionDeploymentOptions['action'],
            { backend: capabilityTrap },
          ),
        ),
      ).rejects.toThrow(row.message);
    }
    expect(terminalReceiptGetterReads).toBe(0);

    const precommit = await boundedDecommissionHarness();
    let precommitResult = await driveToPreDeleteVerify(precommit);
    precommitResult = await continueBoundedDecommission(
      precommit,
      precommitResult,
    );
    precommit.store.failPutPhase = 'decommissioned';
    await expect(
      continueBoundedDecommission(precommit, precommitResult),
    ).rejects.toThrow('failed state write at decommissioned');
    expect(precommit.store.record?.decommissionIntent).toMatchObject({
      lifecyclePhase: 'database-deleting',
      state: 'transitioning',
    });
    await expect(
      continueBoundedDecommission(precommit, precommitResult),
    ).resolves.toMatchObject({ status: 'complete' });

    const committedStore = new CommitThenThrowStore();
    const committed = await boundedDecommissionHarness({
      store: committedStore,
    });
    let committedResult = await driveToPreDeleteVerify(committed);
    committedResult = await continueBoundedDecommission(
      committed,
      committedResult,
    );
    committedStore.failAfterCommittedPhase = 'decommissioned';
    await expect(
      continueBoundedDecommission(committed, committedResult),
    ).rejects.toThrow('committing decommissioned');
    expect(committed.store.record?.decommissionIntent?.state).toBe('complete');
    await expect(
      continueBoundedDecommission(committed, committedResult),
    ).resolves.toMatchObject({ status: 'complete' });

    for (const malformed of [null, false, 0, '']) {
      const candidate = await boundedDecommissionHarness();
      let candidateResult = await driveToPreDeleteVerify(candidate);
      candidateResult = await continueBoundedDecommission(
        candidate,
        candidateResult,
      );
      candidate.backend.databaseExists = true;
      candidate.backend.databaseReadOutcomes.push({
        status: 'fulfilled',
        value: malformed,
      });
      await expect(
        continueBoundedDecommission(candidate, candidateResult),
      ).rejects.toThrow('persisted database reference is malformed');
      expect(candidate.store.record?.decommissionIntent?.state).toBe(
        'transitioning',
      );
    }

    for (const terminalState of ['discover', 'verify'] as const) {
      const candidate = await boundedDecommissionHarness();
      let candidateResult = await driveToPreDeleteVerify(candidate);
      candidateResult = await continueBoundedDecommission(
        candidate,
        candidateResult,
      );
      candidate.backend.databaseExists = true;
      candidateResult = await continueBoundedDecommission(
        candidate,
        candidateResult,
      );
      expect(candidate.store.record?.decommissionIntent).toMatchObject({
        lifecyclePhase: 'database-deleting',
        state: 'discover',
      });
      if (terminalState === 'verify') {
        candidateResult = await continueBoundedDecommission(
          candidate,
          candidateResult,
        );
        expect(candidate.store.record?.decommissionIntent?.state).toBe(
          'verify',
        );
      }
      candidate.backend.databaseExists = false;
      const scans = candidate.backend.scanInputs.length;
      const residuals = candidate.backend.residualCalls;
      const deletes = candidate.backend.events.filter(
        (event) => event === 'delete-database',
      ).length;
      await expect(
        continueBoundedDecommission(candidate, candidateResult),
      ).resolves.toMatchObject({ status: 'complete' });
      expect(candidate.backend.scanInputs).toHaveLength(scans);
      expect(candidate.backend.residualCalls).toBe(residuals);
      expect(
        candidate.backend.events.filter((event) => event === 'delete-database'),
      ).toHaveLength(deletes);
    }

    const secondReconciliation = await boundedDecommissionHarness();
    let secondResult = await driveToPreDeleteVerify(secondReconciliation);
    secondResult = await continueBoundedDecommission(
      secondReconciliation,
      secondResult,
    );
    secondReconciliation.backend.databaseExists = true;
    secondResult = await continueBoundedDecommission(
      secondReconciliation,
      secondResult,
    );
    secondResult = await continueBoundedDecommission(
      secondReconciliation,
      secondResult,
    );
    const scans = secondReconciliation.backend.scanInputs.length;
    const residuals = secondReconciliation.backend.residualCalls;
    const deletes = secondReconciliation.backend.events.filter(
      (event) => event === 'delete-database',
    ).length;
    secondReconciliation.backend.databaseReadOutcomes.push(
      {
        status: 'fulfilled',
        value: {
          id: DATABASE_ID,
          name: 'acme-production',
          created: false,
        },
      },
      { status: 'fulfilled', value: undefined },
    );
    await expect(
      continueBoundedDecommission(secondReconciliation, secondResult),
    ).resolves.toMatchObject({ status: 'complete' });
    expect(secondReconciliation.backend.scanInputs).toHaveLength(scans + 1);
    expect(secondReconciliation.backend.residualCalls).toBe(residuals);
    expect(
      secondReconciliation.backend.events.filter(
        (event) => event === 'delete-database',
      ),
    ).toHaveLength(deletes);
  });

  it('drains bounded and legacy normal decommission with one audit contract', async () => {
    const bounded = await boundedDecommissionHarness();
    const boundedAudit = vi.fn();
    const boundedResult = await decommissionDeployment({
      backend: bounded.backend,
      store: bounded.store,
      spec: bounded.deployment,
      audit: boundedAudit,
    });
    expect(boundedResult.record).toMatchObject({
      phase: 'decommissioned',
      decommissionIntent: { state: 'complete' },
    });
    expect(bounded.backend.receiptCalls).toHaveLength(1);
    expect(boundedAudit).toHaveBeenCalledTimes(1);

    const active = await boundedDecommissionHarness();
    await startBoundedDecommission(active);
    const activeAudit = vi.fn();
    const activeHasTrap = new Proxy(active.backend, {
      has() {
        throw new Error('active normal intent executed has trap');
      },
      get(target, property) {
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await expect(
      decommissionDeployment({
        backend: activeHasTrap,
        store: active.store,
        spec: active.deployment,
        audit: activeAudit,
      }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(activeAudit).toHaveBeenCalledTimes(1);

    const malformedNormal = await boundedDecommissionHarness();
    await startBoundedDecommission(malformedNormal);
    const normalRecord = malformedNormal.store.record;
    const normalIntent = normalRecord?.decommissionIntent;
    if (!normalRecord || !normalIntent) {
      throw new Error('normal decommission fixture lacks its active intent');
    }
    const { identity: _missingIdentity, ...withoutIdentity } = normalIntent;
    const { mode: _missingMode, ...identityWithoutMode } =
      normalIntent.identity;
    let identityGetterCalls = 0;
    const accessorIntent = { ...normalIntent };
    Object.defineProperty(accessorIntent, 'identity', {
      configurable: true,
      enumerable: true,
      get() {
        identityGetterCalls += 1;
        return normalIntent.identity;
      },
    });
    const malformedNormalRows: readonly (readonly [string, unknown])[] = [
      ['empty shell', {}],
      ['missing identity', withoutIdentity],
      ['missing mode', { ...normalIntent, identity: identityWithoutMode }],
      [
        'unknown mode',
        {
          ...normalIntent,
          identity: { ...normalIntent.identity, mode: { kind: 'future' } },
        },
      ],
      ['malformed normal shell', { ...normalIntent, revision: -1 }],
      ['accessor shell', accessorIntent],
      ['transparent proxy shell', new Proxy(normalIntent, {})],
    ];
    let normalBackendTrapCalls = 0;
    const normalBackendTrap = new Proxy({} as ProvisioningBackend, {
      get() {
        normalBackendTrapCalls += 1;
        throw new Error('malformed normal shell read the provider');
      },
      has() {
        normalBackendTrapCalls += 1;
        throw new Error('malformed normal shell reflected the provider');
      },
    });
    const malformedNormalAudit = vi.fn();
    for (const [label, shell] of malformedNormalRows) {
      const hostileStore = new MemoryStore();
      hostileStore.record = {
        ...normalRecord,
        decommissionIntent: shell,
      } as FleetRecord;
      let failure: unknown;
      try {
        await decommissionDeployment({
          backend: normalBackendTrap,
          store: hostileStore,
          spec: malformedNormal.deployment,
          audit: malformedNormalAudit,
        });
      } catch (error) {
        failure = error;
      }
      expect(failure, label).toBeInstanceOf(Error);
      expect((failure as Error).name, label).toBe('Error');
      expect((failure as Error).message, label).toBe(
        'backend switch decommission record is malformed',
      );
      expect(hostileStore.leaseCalls, label).toBe(0);
    }
    expect(identityGetterCalls).toBe(0);
    expect(normalBackendTrapCalls).toBe(0);
    expect(malformedNormalAudit).not.toHaveBeenCalled();

    const activeSwitch = await boundedDecommissionHarness();
    activeSwitch.store.record = {
      ...(activeSwitch.store.record as FleetRecord),
      backendSwitchIntent: {
        subphase: 'domain-detach-authorized',
      } as FleetRecord['backendSwitchIntent'],
    };
    const switchHasTrap = new Proxy(activeSwitch.backend, {
      has() {
        throw new Error('active backend switch executed has trap');
      },
      get(target, property) {
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await expect(
      decommissionDeployment({
        backend: switchHasTrap,
        store: activeSwitch.store,
        spec: activeSwitch.deployment,
      }),
    ).rejects.toThrow('backend switch decommission record is malformed');

    const race = await boundedDecommissionHarness();
    const ready = race.store.record as FleetRecord;
    const started = await startBoundedDecommission(race);
    const advancing = race.store.record as FleetRecord;
    let reads = 0;
    race.store.get = async () => {
      reads += 1;
      return reads === 1 ? ready : (race.store.record ?? advancing);
    };
    race.store.record = advancing;
    await expect(
      decommissionDeployment({
        backend: race.backend,
        store: race.store,
        spec: race.deployment,
      }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(reads).toBeGreaterThan(1);
    expect(started.status).toBe('pending');

    const legacy = await boundedDecommissionHarness();
    await expect(
      decommissionDeployment({
        backend: legacyOnlyBackend(legacy.backend),
        store: legacy.store,
        spec: legacy.deployment,
      }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
    expect(legacy.backend.receiptCalls).toEqual([]);
    expect(legacy.backend.events).toContain('export');

    for (const mode of ['partial', 'non-callable', 'throwing'] as const) {
      const malformed = await boundedDecommissionHarness();
      const malformedBackend = new Proxy(malformed.backend, {
        get(target, property) {
          if (property === 'exportDatabaseReceipt') {
            if (mode === 'throwing') throw new Error('receipt getter trap');
            return mode === 'partial' ? undefined : 1;
          }
          const value = Reflect.get(target, property, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      await expect(
        decommissionDeployment({
          backend: malformedBackend,
          store: malformed.store,
          spec: malformed.deployment,
        }),
        mode,
      ).rejects.toThrow('database export receipt capability is malformed');
      expect(malformed.store.record?.decommissionIntent, mode).toBeUndefined();
    }

    for (const present of ['authority', 'exporter'] as const) {
      const partial = await boundedDecommissionHarness();
      const hasCalls: PropertyKey[] = [];
      const partialBackend = new Proxy(partial.backend, {
        has(target, property) {
          hasCalls.push(property);
          if (property === 'advanceDecommissionAttachmentScan') return true;
          if (property === 'databaseExportReceiptAuthority') {
            return present === 'authority';
          }
          if (property === 'exportDatabaseReceipt') {
            return present === 'exporter';
          }
          return Reflect.has(target, property);
        },
        get(target, property) {
          if (
            property === 'databaseExportReceiptAuthority' &&
            present !== 'authority'
          ) {
            return undefined;
          }
          if (property === 'exportDatabaseReceipt' && present !== 'exporter') {
            return undefined;
          }
          const value = Reflect.get(target, property, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      await expect(
        decommissionDeployment({
          backend: partialBackend,
          store: partial.store,
          spec: partial.deployment,
        }),
        present,
      ).rejects.toThrow('database export receipt capability is malformed');
      expect(hasCalls, present).toContain('advanceDecommissionAttachmentScan');
      expect(hasCalls, present).toContain(
        present === 'authority'
          ? 'databaseExportReceiptAuthority'
          : 'exportDatabaseReceipt',
      );
      expect(partial.backend.events, present).not.toContain('export');
      expect(partial.store.record?.decommissionIntent, present).toBeUndefined();
    }

    const lateFenced = await boundedDecommissionHarness({ r2Names: ['FILES'] });
    lateFenced.store.record = {
      ...(lateFenced.store.record as FleetRecord),
      phase: 'database-exported',
      databaseExportLocation: 'memory://legacy/export.sql',
      databaseExportSha256: 'a'.repeat(64),
      databaseExportSize: 42,
    };
    lateFenced.backend.events.length = 0;
    await expect(
      decommissionDeployment({
        backend: lateFenced.backend,
        store: lateFenced.store,
        spec: lateFenced.deployment,
      }),
    ).rejects.toThrow(
      'normal decommission D1 work requires every application R2 resource to be deleted',
    );
    expect(lateFenced.backend.events).toEqual([]);
    expect(lateFenced.backend.receiptCalls).toEqual([]);

    for (const phase of [
      'database-exported',
      'database-deleting',
      'decommissioned',
    ] as const) {
      const late = await boundedDecommissionHarness();
      late.store.record = {
        ...(late.store.record as FleetRecord),
        phase,
        applicationResources: [],
        databaseExportLocation: 'memory://legacy/export.sql',
        databaseExportSha256: 'a'.repeat(64),
        databaseExportSize: 42,
      };
      late.backend.events.length = 0;
      await expect(
        decommissionDeployment({
          backend: late.backend,
          store: late.store,
          spec: late.deployment,
        }),
        phase,
      ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
      expect(late.backend.receiptCalls, phase).toEqual([]);
      if (phase === 'decommissioned') {
        expect(late.backend.events, phase).toEqual([]);
      } else {
        expect(late.backend.events, phase).toContain('delete-database');
      }
    }

    const blocked = await boundedDecommissionHarness();
    let blockedResult = await driveBoundedUntil(
      blocked,
      (record) =>
        record.decommissionIntent?.lifecyclePhase ===
          'application-resources-deleted' &&
        record.decommissionIntent.state === 'transitioning',
    );
    blockedResult = await continueBoundedDecommission(blocked, blockedResult);
    blocked.backend.scanResults.push({
      status: 'attached',
      attachment: { plane: 'ordinary', scriptName: 'removed-before-restart' },
      providerFetchAttemptsReserved: 3,
    });
    blockedResult = await continueBoundedDecommission(blocked, blockedResult);
    expect(blockedResult.status).toBe('blocked');
    await expect(
      decommissionDeployment({
        backend: blocked.backend,
        store: blocked.store,
        spec: blocked.deployment,
      }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });

    const newlyBlocked = await boundedDecommissionHarness();
    const newlyBlockedAudit = vi.fn();
    newlyBlocked.backend.scanResults.push({
      status: 'attached',
      attachment: { plane: 'ordinary', scriptName: 'still-attached' },
      providerFetchAttemptsReserved: 3,
    });
    await expect(
      decommissionDeployment({
        backend: newlyBlocked.backend,
        store: newlyBlocked.store,
        spec: newlyBlocked.deployment,
        audit: newlyBlockedAudit,
      }),
    ).rejects.toThrow(
      'bounded decommission remains blocked by a Worker attachment',
    );
    expect(newlyBlockedAudit).not.toHaveBeenCalled();

    const complete = await boundedDecommissionHarness();
    await decommissionDeployment({
      backend: complete.backend,
      store: complete.store,
      spec: complete.deployment,
    });
    const hasTrap = new Proxy(complete.backend, {
      has() {
        throw new Error('complete wrapper executed has trap');
      },
      get(target, property) {
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await expect(
      decommissionDeployment({
        backend: hasTrap,
        store: complete.store,
        spec: complete.deployment,
      }),
    ).resolves.toMatchObject({ record: { phase: 'decommissioned' } });
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

  it('drains bounded backend-switch teardown and preserves legacy late recovery', async () => {
    const harness = await boundedDecommissionHarness({
      kind: 'workers-for-platforms',
      external: true,
    });
    const current = harness.store.record;
    if (!current?.platformTarget || !current.platformResources) {
      throw new Error(
        'backend-switch wrapper fixture lacks platform authority',
      );
    }
    const priorSpec: DeploymentSpec = {
      ...harness.deployment,
      authoredBy: 'platform',
    };
    const prior = {
      scriptName: current.scriptName,
      artifactVersion: current.artifactVersion,
      specDigest: deploymentSpecDigest(priorSpec),
      databaseId: current.databaseId,
      databaseName: current.databaseName,
      durableObjectBindings: current.durableObjectBindings,
      namespaceIds: current.durableObjectBindings.map(
        ({ namespaceId }) => namespaceId,
      ),
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
      application: current.applicationBindings,
      applicationResources: [],
      customDomain: {
        id: 'domain-acme',
        hostname: current.routeHostname,
      },
    } as const;
    const databaseExport: DatabaseExport = {
      databaseId: current.databaseId,
      location: 'memory://fleet-exports/backend-switch.sql',
      sha256: 'f'.repeat(64),
      size: 37,
    };
    const decommissionSnapshot = {
      prior,
      restoredArtifactVersion: null,
      entryPendingArtifactVersion: null,
      entryPendingNamespaceIds: null,
      providerTargetSpecDigest: current.desiredSpecDigest,
      routeHostname: current.routeHostname,
      routeTargets: [],
      desiredSpecDigest: current.desiredSpecDigest,
      target: current.platformTarget,
      releases: [],
      applicationResources: [],
    } as const;
    harness.store.record = backendSwitchDecommissionRecordFixture(
      JSON.parse(JSON.stringify(current)) as FleetRecord,
      {
        kind: 'backend-switch',
        tenantTag: current.tenantTag,
        environment: current.environment,
        prior,
        targetSpecDigest: current.desiredSpecDigest,
        targetApplication: current.applicationBindings ?? {
          vars: [],
          secrets: [],
          r2Buckets: [],
        },
        target: current.platformTarget,
        rollbackUntil: '2026-09-30T00:00:00.000Z',
        subphase: 'decommission-application-r2-removed',
        applicationR2Progress: [],
        decommissionSnapshot,
      },
      { subphase: 'decommission-application-r2-removed' },
    );
    const audit = vi.fn();
    const providerTrap = new Proxy({} as ProvisioningBackend, {
      get() {
        throw new Error('terminal switch read the normal provider');
      },
      has() {
        throw new Error('terminal switch reflected the normal provider');
      },
    });
    let databasePresent = true;
    let scanCalls = 0;
    let receiptCalls = 0;
    let boundedDeleteCalls = 0;
    let legacyExportCalls = 0;
    let legacyDeleteCalls = 0;
    const boundedProvider = {
      databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
      async advanceSwitchDecommissionAttachmentScan() {
        scanCalls += 1;
        return {
          status: 'complete',
          evidenceSha256: 'a'.repeat(64),
          evidenceCount: 2,
          providerFetchAttemptsReserved: 3,
        };
      },
      async exportSwitchDatabaseReceipt() {
        receiptCalls += 1;
        return databaseExport;
      },
      async getSwitchDatabase(databaseId: string) {
        return databasePresent && databaseId === current.databaseId
          ? {
              id: current.databaseId,
              name: current.databaseName,
              created: false as const,
            }
          : undefined;
      },
      async readSwitchDatabaseOwner() {
        return current.tenantTag;
      },
      async assertSwitchDatabaseDeletionResidualsRemoved() {},
      async deleteSwitchDatabaseBounded() {
        boundedDeleteCalls += 1;
        databasePresent = false;
      },
      async exportSwitchDatabase() {
        legacyExportCalls += 1;
        return databaseExport;
      },
      async deleteSwitchDatabase() {
        legacyDeleteCalls += 1;
      },
    } as unknown as BackendSwitchProvider;
    const activeRecord = harness.store.record as FleetRecord;
    const rejectedAudit = vi.fn();
    const missingProviderStore = new MemoryStore();
    missingProviderStore.record = activeRecord;
    await expect(
      decommissionDeployment({
        backend: providerTrap,
        store: missingProviderStore,
        spec: harness.deployment,
        audit: rejectedAudit,
      }),
    ).rejects.toThrow(
      'active backend switch decommission requires its dedicated provider and both specifications',
    );
    const partialStore = new MemoryStore();
    partialStore.record = activeRecord;
    await expect(
      decommissionDeployment({
        backend: providerTrap,
        store: partialStore,
        spec: harness.deployment,
        audit: rejectedAudit,
        backendSwitch: {
          provider: {
            advanceSwitchDecommissionAttachmentScan:
              boundedProvider.advanceSwitchDecommissionAttachmentScan,
            databaseExportReceiptAuthority: RECEIPT_AUTHORITY,
          } as unknown as BackendSwitchProvider,
          priorSpec,
          targetSpec: harness.deployment,
        },
      }),
    ).rejects.toThrow('database export receipt capability is malformed');
    expect(rejectedAudit).not.toHaveBeenCalled();

    let backendSwitchGetterCalls = 0;
    const accessorRecord = { ...activeRecord };
    Object.defineProperty(accessorRecord, 'backendSwitchIntent', {
      configurable: true,
      enumerable: true,
      get() {
        backendSwitchGetterCalls += 1;
        return activeRecord.backendSwitchIntent;
      },
    });
    const hidingProxy = new Proxy(activeRecord, {
      ownKeys(target) {
        return Reflect.ownKeys(target).filter(
          (key) => key !== 'backendSwitchIntent',
        );
      },
    });
    let revokeAfterResolution = () => {};
    const revoked = Proxy.revocable(activeRecord, {
      get(target, property, receiver) {
        if (property === 'then') {
          queueMicrotask(revokeAfterResolution);
          return undefined;
        }
        return Reflect.get(target, property, receiver);
      },
    });
    revokeAfterResolution = revoked.revoke;
    const {
      backendSwitchIntent: _removedSwitchIntent,
      ...backendShellWithoutSwitch
    } = activeRecord;
    const hostileRows = [
      ['accessor', accessorRecord],
      ['symbol', { ...activeRecord, [Symbol('hostile')]: true }],
      ['transparent proxy', new Proxy(activeRecord, {})],
      ['hiding proxy', hidingProxy],
      ['revoked proxy', revoked.proxy],
      ['backend shell without switch', backendShellWithoutSwitch],
    ] as const;
    for (const [label, hostile] of hostileRows) {
      const hostileStore = new MemoryStore();
      hostileStore.record = hostile as FleetRecord;
      await expect(
        decommissionDeployment({
          backend: providerTrap,
          store: hostileStore,
          spec: harness.deployment,
          audit: rejectedAudit,
        }),
        label,
      ).rejects.toThrow('backend switch decommission record is malformed');
      expect(hostileStore.leaseCalls, label).toBe(0);
    }
    expect(backendSwitchGetterCalls).toBe(0);
    expect(scanCalls).toBe(0);
    expect(receiptCalls).toBe(0);
    expect(boundedDeleteCalls).toBe(0);
    expect(rejectedAudit).not.toHaveBeenCalled();

    const boundedResult = await decommissionDeployment({
      backend: providerTrap,
      store: harness.store,
      spec: harness.deployment,
      audit,
      backendSwitch: {
        provider: boundedProvider,
        priorSpec,
        targetSpec: harness.deployment,
      },
    });
    expect(boundedResult).toEqual({
      record: harness.store.record,
      databaseExport,
    });
    expect(audit).toHaveBeenCalledTimes(1);
    expect(scanCalls).toBe(4);
    expect(receiptCalls).toBe(1);
    expect(boundedDeleteCalls).toBe(1);
    expect(legacyExportCalls).toBe(0);
    expect(legacyDeleteCalls).toBe(0);

    const auditFailure = new Error('backend-switch audit rejected');
    await expect(
      decommissionDeployment({
        backend: providerTrap,
        store: harness.store,
        spec: harness.deployment,
        audit: () => Promise.reject(auditFailure),
      }),
    ).rejects.toBe(auditFailure);

    const legacyStore = new MemoryStore();
    legacyStore.record = {
      ...(JSON.parse(JSON.stringify(current)) as FleetRecord),
      backendSwitchIntent: {
        kind: 'backend-switch',
        tenantTag: current.tenantTag,
        environment: current.environment,
        prior,
        targetSpecDigest: current.desiredSpecDigest,
        targetApplication: current.applicationBindings ?? {
          vars: [],
          secrets: [],
          r2Buckets: [],
        },
        target: current.platformTarget,
        rollbackUntil: '2026-09-30T00:00:00.000Z',
        subphase: 'decommission-exported',
        databaseExport,
        applicationR2Progress: [],
        decommissionSnapshot: {
          routeHostname: current.routeHostname,
          routeTargets: [],
          desiredSpecDigest: current.desiredSpecDigest,
          target: current.platformTarget,
          releases: [],
          applicationResources: [],
        },
      },
    };
    databasePresent = true;
    const lateGetter = vi.fn(() => {
      throw new Error('legacy late row read a bounded capability');
    });
    const legacyProvider = new Proxy(boundedProvider, {
      get(target, property, receiver) {
        if (
          property === 'advanceSwitchDecommissionAttachmentScan' ||
          property === 'databaseExportReceiptAuthority' ||
          property === 'exportSwitchDatabaseReceipt'
        ) {
          return lateGetter();
        }
        return Reflect.get(target, property, receiver);
      },
    });
    const legacyAudit = vi.fn();
    const legacyResult = await decommissionDeployment({
      backend: providerTrap,
      store: legacyStore,
      spec: harness.deployment,
      audit: legacyAudit,
      backendSwitch: {
        provider: legacyProvider,
        priorSpec,
        targetSpec: harness.deployment,
      },
    });
    expect(legacyResult).toEqual({
      record: legacyStore.record,
      databaseExport,
    });
    expect(lateGetter).not.toHaveBeenCalled();
    expect(legacyAudit).toHaveBeenCalledTimes(1);
    expect(legacyExportCalls).toBe(0);
    expect(legacyDeleteCalls).toBe(1);

    const malformed = harness.store.record;
    harness.store.record = {
      ...malformed,
      backendSwitchIntent: undefined,
    };
    await expect(
      decommissionDeployment({
        backend: providerTrap,
        store: harness.store,
        spec: harness.deployment,
      }),
    ).rejects.toThrow('backend switch decommission record is malformed');
  });
});
