// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  applicationBindingTopology,
  reserveApplicationR2Resources,
} from '../src/application-bindings.js';
import {
  advanceBackendSwitchDecommission,
  type BackendSwitchIntent,
  type BackendSwitchProvider,
  type BridgeSnapshot,
  backendSwitchDecommissionShell,
  backendSwitchDecommissionSnapshotDigest,
  backendSwitchIntentFromUnknown,
  decommissionBackendSwitch,
  finalizeBackendSwitch,
  type PlainBackendSnapshot,
  reconcileFinalizedBackendSwitchState,
  rollbackBackendSwitch,
  switchPlainDeploymentToWorkersForPlatforms,
  withBackendSwitchLease,
} from '../src/backend-switch.js';
import {
  canonicalDeploymentEgressPolicy,
  externalHostRoutingTarget,
  externalRouteExpectations,
} from '../src/platform-resources.js';
import {
  decommissionDeployment,
  provisionDeployment,
} from '../src/provision.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  ApplicationR2Resource,
  DecommissionAdvanceIntent,
  DeploymentSpec,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  ProvisioningBackend,
} from '../src/types.js';
import {
  composeLegacyBridgeArtifact,
  LEGACY_APPLICATION_MODULE_PLACEHOLDER,
} from '../src/workers-for-platforms-backend-switch-provider.js';
import { decommissionAdvancingRecordFixture } from './fixtures/decommission-intent-fixture.js';

function spec(authoredBy: 'platform' | 'external'): DeploymentSpec {
  return {
    tenantTag: 'acme',
    environment: 'production',
    scriptName: 'acme-production',
    databaseName: 'acme-production',
    compatibilityDate: '2026-08-11',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy,
    schemaVersion: 1,
    migrations: [{ version: 1, sql: 'SELECT 1' }],
    durableObjectMigrations:
      authoredBy === 'platform' ? [{ tag: 'v1', newClasses: ['Runner'] }] : [],
    durableObjectBindings: [{ name: 'RUNNER', className: 'Runner' }],
    maintenanceBaseUrl: 'https://control-acme.example.test',
    routeHostname: 'acme.example.test',
  };
}

const priorSpec = spec('platform');
const targetSpec = spec('external');

function switchApplicationR2Resource(
  name: string,
  jurisdiction: ApplicationR2Resource['jurisdiction'],
  creationDate: string,
): ApplicationR2Resource {
  const [reserved] = reserveApplicationR2Resources({
    ...targetSpec,
    application: {
      vars: [],
      secrets: [],
      r2Buckets: [{ name, jurisdiction }],
    },
  });
  if (!reserved) throw new Error('missing reserved application R2 resource');
  return { ...reserved, state: 'created', creationDate };
}
const target: ExternalPlatformTargetDescription = {
  maintenanceCapabilityPublicKey:
    '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
  stateArtifactDigest: 'a'.repeat(64),
  stateDurableObjectHistoryDigest: 'b'.repeat(64),
  stateDurableObjectTag: 'v1',
  sharedOutboundWorkerName: 'fleet-shared-outbound',
  stateEgressCredentialDigest: 'c'.repeat(64),
  d1SchemaVersion: 1,
  d1SchemaHistoryDigest: 'd'.repeat(64),
  outboundPolicy: canonicalDeploymentEgressPolicy({
    policyId: 'policy-acme',
    tenantTag: 'acme',
    environment: 'production',
    allowedHosts: ['api.example.com'],
  }),
};
const secrets = {
  deploymentIdentity: 'identity-secret-012345678901234567890123',
  maintenanceAdmin: 'maintenance-secret-0123456789012345678901',
};
const DECOMMISSION_ROUTE_PHASES = [
  ['planned', false, ['acme-candidate:api.example.com']],
  ['schema-applied', false, ['acme-candidate:api.example.com']],
  ['platform-applied', false, ['acme-candidate:api.example.com']],
  ['candidate-deployed', false, ['acme-candidate:api.example.com']],
  [
    'candidate-armed',
    false,
    ['acme-candidate:api.example.com', 'acme-next:next.example.com'],
  ],
  ['route-published', false, ['acme-next:next.example.com']],
  [
    'platform-applied',
    true,
    ['acme-candidate:api.example.com', 'acme-candidate:next.example.com'],
  ],
  ['route-published', true, ['acme-candidate:next.example.com']],
] as const;

class MemorySwitchStore implements FleetStateStore {
  record: FleetRecord = {
    tenantTag: priorSpec.tenantTag,
    environment: priorSpec.environment,
    backend: 'plain-worker',
    scriptName: priorSpec.scriptName,
    databaseId: 'db-acme',
    databaseName: priorSpec.databaseName,
    schemaVersion: 1,
    artifactVersion: 'plain-v1',
    desiredSpecDigest: deploymentSpecDigest(priorSpec),
    durableObjectBindings: [],
    routeHostname: priorSpec.routeHostname,
    phase: 'ready',
    updatedAt: '2026-08-11T00:00:00.000Z',
  };
  readonly phases: string[] = [];
  failAfterCommittedStateReconcile = false;
  failAfterCommittedReleaseDelete = false;
  failAfterCommittedTrafficAuthorization = false;
  failAfterCommittedTrafficRemoval = false;
  failNextPutBeforeCommit: Error | undefined;
  failNextPutAfterCommit: Error | undefined;
  nextGet: FleetRecord | undefined;
  nextGetAfterCommittedFailure: FleetRecord | undefined;
  getCalls = 0;
  putCalls = 0;

  get intent(): BackendSwitchIntent | undefined {
    return this.record.backendSwitchIntent;
  }

  async get(): Promise<FleetRecord> {
    this.getCalls += 1;
    if (this.nextGet) {
      const next = this.nextGet;
      this.nextGet = undefined;
      return next;
    }
    return this.record;
  }

  async list(): Promise<readonly FleetRecord[]> {
    return [this.record];
  }

  async withDeploymentLease<T>(
    _tenantTag: string,
    _environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    return operation({
      tenantTag: priorSpec.tenantTag,
      environment: priorSpec.environment,
      mutationLeaseTtlMs: 60_000,
      assertOwned: async () => {},
      renew: async () => {},
      delete: async () => {},
      put: async (record) => {
        this.putCalls += 1;
        if (this.failNextPutBeforeCommit) {
          const error = this.failNextPutBeforeCommit;
          this.failNextPutBeforeCommit = undefined;
          throw error;
        }
        this.record = structuredClone(record);
        if (this.failNextPutAfterCommit) {
          const error = this.failNextPutAfterCommit;
          this.failNextPutAfterCommit = undefined;
          this.nextGet = this.nextGetAfterCommittedFailure;
          this.nextGetAfterCommittedFailure = undefined;
          throw error;
        }
        if (record.backendSwitchIntent) {
          this.phases.push(record.backendSwitchIntent.subphase);
        }
        if (
          this.failAfterCommittedTrafficAuthorization &&
          record.backendSwitchIntent?.subphase ===
            'decommission-traffic-authorized'
        ) {
          this.failAfterCommittedTrafficAuthorization = false;
          throw new Error(
            'traffic authorization write response lost after commit',
          );
        }
        if (
          this.failAfterCommittedTrafficRemoval &&
          record.backendSwitchIntent?.subphase ===
            'decommission-traffic-removed'
        ) {
          this.failAfterCommittedTrafficRemoval = false;
          throw new Error('traffic removal write response lost after commit');
        }
        if (
          this.failAfterCommittedStateReconcile &&
          record.backendSwitchIntent?.stateReconcileIntent?.subphase ===
            'uploaded'
        ) {
          this.failAfterCommittedStateReconcile = false;
          throw new Error('finalized state write response lost after commit');
        }
        if (
          this.failAfterCommittedReleaseDelete &&
          record.backendSwitchIntent?.decommissionSnapshot?.releases.some(
            ({ subphase }) => subphase === 'deleted',
          )
        ) {
          this.failAfterCommittedReleaseDelete = false;
          throw new Error('release deletion write response lost after commit');
        }
      },
    });
  }
}

class FakeSwitchProvider implements BackendSwitchProvider {
  readonly calls: string[] = [];
  readonly removedRouteTargets: Array<
    readonly import('../src/host-routing.js').HostRoutingTarget[]
  > = [];
  failBridgeResponseOnce = false;
  failR2DeleteResponseOnce = false;
  switchTrafficRemoved = false;
  failReleaseDeleteResponseOnce = false;
  failTrafficRemovalResponseOnce = false;
  writeR2AfterTrafficRemovalOnce = false;
  switchTrafficDrift = false;
  applicationResources: readonly ApplicationR2Resource[] = [];
  readonly r2Buckets = new Map<string, ApplicationR2Resource>();
  readonly attachedR2 = new Set<string>();
  readonly nonemptyR2 = new Set<string>();
  readonly deletedReleases = new Set<string>();
  readonly releaseDeleteMutations = new Map<string, number>();
  bridge: BridgeSnapshot = {
    scriptName: priorSpec.scriptName,
    artifactVersion: 'bridge-v1',
    artifactDigest: 'e'.repeat(64),
    databaseId: 'db-acme',
    durableObjectBindings: [
      {
        name: 'RUNNER',
        className: 'Runner',
        namespaceId: 'namespace-runner',
      },
    ],
    namespaceIds: ['namespace-runner'],
    secretNames: [
      'DEPLOYMENT_IDENTITY_SECRET',
      'MAINTENANCE_ADMIN_SECRET',
      'OUTBOUND_PROXY_CREDENTIAL',
    ],
    publicRouteAttached: true,
    stateOnly: false,
  };

  async snapshotPlainDeployment(
    selectedPriorSpec: DeploymentSpec,
  ): Promise<PlainBackendSnapshot> {
    this.calls.push('snapshot');
    return {
      scriptName: priorSpec.scriptName,
      artifactVersion: 'plain-v1',
      specDigest: deploymentSpecDigest(selectedPriorSpec),
      databaseId: 'db-acme',
      databaseName: priorSpec.databaseName,
      durableObjectBindings: this.bridge.durableObjectBindings,
      namespaceIds: this.bridge.namespaceIds,
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
      applicationResources: this.applicationResources,
      ...(this.applicationResources.length > 0
        ? {
            application: applicationBindingTopology(
              selectedPriorSpec,
              this.applicationResources,
            ),
          }
        : {}),
      customDomain: { id: 'domain-1', hostname: priorSpec.routeHostname },
    };
  }

  describeBridge() {
    return {
      artifactDigest: this.bridge.artifactDigest,
      durableObjectMigrations: priorSpec.durableObjectMigrations,
      priorDurableObjectTag: 'v1',
      targetDurableObjectTag: 'v1',
      secretNames: this.bridge.secretNames,
      mutationDigest: '9'.repeat(64),
    };
  }

  describeFinalizedBridgeTarget() {
    return target;
  }

  describeFinalizedState() {
    return this.describeBridge();
  }

  async ensureFinalizedState(): Promise<BridgeSnapshot> {
    this.calls.push('finalized-state');
    return { ...this.bridge, publicRouteAttached: false, stateOnly: true };
  }

  async assertFinalizedState(): Promise<void> {}

  async ensureBridge(): Promise<BridgeSnapshot> {
    this.calls.push('bridge');
    if (this.failBridgeResponseOnce) {
      this.failBridgeResponseOnce = false;
      throw new Error('bridge upload response lost');
    }
    return this.bridge;
  }

  async recoverBridge(): Promise<BridgeSnapshot | undefined> {
    return this.failBridgeResponseOnce ? undefined : this.bridge;
  }

  async ensureCandidate(
    input: Parameters<BackendSwitchProvider['ensureCandidate']>[0],
  ) {
    this.calls.push('candidate');
    const application = applicationBindingTopology(
      input.targetSpec,
      this.applicationResources,
    );
    return {
      physicalScriptName: 'acme-candidate',
      specDigest: deploymentSpecDigest(input.targetSpec),
      artifactVersion: 'candidate-v1',
      releaseSchemaVersion: 1,
      application,
      topology: {
        durableObjectBindings: this.bridge.durableObjectBindings,
        serviceBindings: [],
        queueProducerBindings: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        application,
      },
      maintenance: {
        receipt: 'maintenance-receipt-v1',
        specDigest: deploymentSpecDigest(input.targetSpec),
      },
    };
  }

  async publishCandidateHost() {
    this.calls.push('host-publish');
  }
  async assertCandidateHostPublished() {
    this.calls.push('host-assert');
  }
  async detachPlainCustomDomain() {
    this.calls.push('domain-detach');
  }
  async assertCandidateServing() {
    this.calls.push('candidate-assert');
  }
  async privatizeBridge(): Promise<BridgeSnapshot> {
    this.calls.push('bridge-private');
    this.bridge = { ...this.bridge, publicRouteAttached: false };
    return this.bridge;
  }
  async commitWorkersForPlatformsOwnership(
    input: Parameters<
      BackendSwitchProvider['commitWorkersForPlatformsOwnership']
    >[0],
  ): Promise<FleetRecord> {
    this.calls.push('commit-wfp');
    return {
      ...input.currentRecord,
      backend: 'workers-for-platforms' as const,
      artifactVersion: input.candidate.artifactVersion,
      desiredSpecDigest: input.candidate.specDigest,
      activeRelease: {
        physicalScriptName: input.candidate.physicalScriptName,
        specDigest: input.candidate.specDigest,
        artifactVersion: input.candidate.artifactVersion,
        releaseSchemaVersion: input.candidate.releaseSchemaVersion,
        application: input.candidate.application,
        ...(input.candidate.topology
          ? { topology: input.candidate.topology }
          : {}),
      },
      outboundPolicy: input.target.outboundPolicy,
      platformTarget: input.target,
      platformResources: {
        maintenanceCapabilityPublicKey:
          input.target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: input.bridge.scriptName,
          artifactVersion: input.bridge.artifactVersion,
          artifactDigest: input.bridge.artifactDigest,
          plane: 'ordinary' as const,
          ...(input.target.stateDurableObjectTag
            ? { durableObjectTag: input.target.stateDurableObjectTag }
            : {}),
          durableObjectBindings: input.bridge.durableObjectBindings,
          namespaceIds: input.bridge.namespaceIds,
        },
        outboundPolicy: input.target.outboundPolicy,
        sharedOutboundWorkerName:
          input.target.sharedOutboundWorkerName ?? 'fleet-shared-outbound',
      },
      phase: 'ready' as const,
    };
  }
  async routePlainDomainToBridge() {
    this.calls.push('rollback-route');
  }
  async assertPlainBridgeServing() {
    this.calls.push('rollback-route-assert');
  }
  async removeCandidateHostAndDrain() {
    this.calls.push('rollback-drain');
  }
  async restorePlainDeployment() {
    this.calls.push('rollback-restore');
    return 'plain-restored-v2';
  }
  async commitPlainOwnership(
    input: Parameters<BackendSwitchProvider['commitPlainOwnership']>[0],
  ): Promise<FleetRecord> {
    this.calls.push('commit-plain');
    const {
      activeRelease: _activeRelease,
      outboundPolicy: _outboundPolicy,
      platformResources: _platformResources,
      platformTarget: _platformTarget,
      migrationIntent: _migrationIntent,
      ...current
    } = input.currentRecord;
    return {
      ...current,
      backend: 'plain-worker' as const,
      scriptName: input.prior.scriptName,
      databaseId: input.prior.databaseId,
      desiredSpecDigest: input.prior.specDigest,
      artifactVersion: input.restoredArtifactVersion,
      phase: 'ready' as const,
    };
  }
  async ensureStateOnlyBridge(
    input: Parameters<BackendSwitchProvider['ensureStateOnlyBridge']>[0],
  ): Promise<BridgeSnapshot> {
    this.calls.push('finalize');
    this.bridge = {
      ...this.bridge,
      artifactVersion: 'state-v2',
      artifactDigest: input.target.stateArtifactDigest,
      stateOnly: true,
    };
    return this.bridge;
  }
  async commitFinalizedOwnership(
    input: Parameters<BackendSwitchProvider['commitFinalizedOwnership']>[0],
  ): Promise<FleetRecord> {
    return {
      ...input.currentRecord,
      platformResources: {
        ...(input.target.auditQueueName
          ? { auditQueueName: input.target.auditQueueName }
          : {}),
        maintenanceCapabilityPublicKey:
          input.target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: input.bridge.scriptName,
          artifactVersion: input.bridge.artifactVersion,
          artifactDigest: input.bridge.artifactDigest,
          plane: 'ordinary',
          ...(input.target.stateDurableObjectTag
            ? { durableObjectTag: input.target.stateDurableObjectTag }
            : {}),
          durableObjectBindings: input.bridge.durableObjectBindings,
          namespaceIds: input.bridge.namespaceIds,
        },
        outboundPolicy: input.target.outboundPolicy,
        sharedOutboundWorkerName: input.target.sharedOutboundWorkerName,
      },
    };
  }
  async removeSwitchTraffic(
    input: Parameters<BackendSwitchProvider['removeSwitchTraffic']>[0],
  ) {
    this.calls.push('decommission-traffic');
    this.removedRouteTargets.push(input.routeTargets);
    this.switchTrafficRemoved = true;
    const applicationBucket = this.r2Buckets.keys().next().value;
    if (applicationBucket && this.writeR2AfterTrafficRemovalOnce) {
      this.writeR2AfterTrafficRemovalOnce = false;
      this.nonemptyR2.add(applicationBucket);
    }
    if (this.failTrafficRemovalResponseOnce) {
      this.failTrafficRemovalResponseOnce = false;
      throw new Error('switch traffic removal response lost after commit');
    }
  }
  async assertSwitchTrafficRemoved() {
    if (!this.switchTrafficRemoved || this.switchTrafficDrift) {
      throw new Error('backend switch traffic remains during decommission');
    }
  }
  async removeSwitchRelease(
    input: Parameters<BackendSwitchProvider['removeSwitchRelease']>[0],
  ) {
    this.calls.push(`decommission-release:${input.release.physicalScriptName}`);
    if (!this.deletedReleases.has(input.release.physicalScriptName)) {
      this.deletedReleases.add(input.release.physicalScriptName);
      this.releaseDeleteMutations.set(
        input.release.physicalScriptName,
        (this.releaseDeleteMutations.get(input.release.physicalScriptName) ??
          0) + 1,
      );
    }
    if (this.failReleaseDeleteResponseOnce) {
      this.failReleaseDeleteResponseOnce = false;
      throw new Error('release delete response lost');
    }
  }
  async removeSwitchBridge() {
    this.calls.push('decommission-bridge');
  }
  async findSwitchApplicationR2(resource: ApplicationR2Resource) {
    const found = this.r2Buckets.get(resource.bucketName);
    return found
      ? { ...found, creationDate: found.creationDate as string }
      : undefined;
  }
  async assertSwitchApplicationR2Detached(resource: ApplicationR2Resource) {
    this.calls.push(`r2-detached:${resource.name}`);
    if (this.attachedR2.has(resource.bucketName)) {
      throw new Error(`R2 bucket '${resource.bucketName}' remains attached`);
    }
  }
  async assertSwitchApplicationR2Empty(resource: ApplicationR2Resource) {
    this.calls.push(`r2-empty:${resource.name}`);
    if (this.nonemptyR2.has(resource.bucketName)) {
      throw new Error(`R2 bucket '${resource.bucketName}' is not empty`);
    }
  }
  async deleteSwitchApplicationR2(resource: ApplicationR2Resource) {
    this.calls.push(`r2-delete:${resource.name}`);
    this.r2Buckets.delete(resource.bucketName);
    if (this.failR2DeleteResponseOnce) {
      this.failR2DeleteResponseOnce = false;
      throw new Error('R2 delete response lost');
    }
  }
  async exportSwitchDatabase() {
    this.calls.push('decommission-export');
    return {
      databaseId: 'db-acme',
      location: 'r2://exports/acme.sql',
      sha256: 'f'.repeat(64),
      size: 123,
    };
  }
  async deleteSwitchDatabase() {
    this.calls.push('decommission-database');
  }
}

function switchOptions(store: MemorySwitchStore, provider: FakeSwitchProvider) {
  return {
    store,
    provider,
    priorSpec,
    targetSpec,
    target,
    secrets,
    rollbackUntil: '2099-01-01T00:00:00.000Z',
  };
}

class BoundedFakeSwitchProvider extends FakeSwitchProvider {
  readonly databaseExportReceiptAuthority = 'test-receipts';
  databasePresent = true;
  scanCalls = 0;
  receiptCalls = 0;
  boundedDeleteCalls = 0;
  blockScans = false;

  async advanceSwitchDecommissionAttachmentScan() {
    this.scanCalls += 1;
    if (this.blockScans) {
      return {
        status: 'attached' as const,
        attachment: {
          plane: 'ordinary' as const,
          scriptName: 'foreign-worker',
        },
        providerFetchAttemptsReserved: 1,
      };
    }
    return {
      status: 'complete' as const,
      evidenceSha256: '7'.repeat(64),
      evidenceCount: 2,
      providerFetchAttemptsReserved: 1,
    };
  }

  async exportSwitchDatabaseReceipt() {
    this.receiptCalls += 1;
    return {
      databaseId: BOUNDED_DATABASE_ID,
      location: 'r2://exports/acme-bounded.sql',
      sha256: '6'.repeat(64),
      size: 456,
    };
  }

  async getSwitchDatabase() {
    return this.databasePresent
      ? {
          id: BOUNDED_DATABASE_ID,
          name: priorSpec.databaseName,
          created: false as const,
        }
      : undefined;
  }

  async readSwitchDatabaseOwner() {
    return priorSpec.tenantTag;
  }

  async assertSwitchDatabaseDeletionResidualsRemoved() {}

  async deleteSwitchDatabaseBounded() {
    this.boundedDeleteCalls += 1;
    this.databasePresent = false;
  }

  override async exportSwitchDatabase() {
    this.calls.push('decommission-export');
    return {
      databaseId: BOUNDED_DATABASE_ID,
      location: 'r2://exports/acme-legacy.sql',
      sha256: '5'.repeat(64),
      size: 321,
    };
  }

  override async deleteSwitchDatabase() {
    this.calls.push('decommission-database');
    this.databasePresent = false;
  }
}

const BOUNDED_DATABASE_ID = '11111111-1111-1111-1111-111111111111';

function bindBoundedDatabase(
  store: MemorySwitchStore,
  provider: BoundedFakeSwitchProvider,
): void {
  const intent = store.intent;
  if (!intent) throw new Error('switch intent is missing');
  const bridge = intent.bridge
    ? { ...intent.bridge, databaseId: BOUNDED_DATABASE_ID }
    : undefined;
  provider.bridge = { ...provider.bridge, databaseId: BOUNDED_DATABASE_ID };
  store.record = {
    ...store.record,
    databaseId: BOUNDED_DATABASE_ID,
    backendSwitchIntent: {
      ...intent,
      prior: { ...intent.prior, databaseId: BOUNDED_DATABASE_ID },
      ...(bridge ? { bridge } : {}),
    },
  };
}

function boundedOptions(
  store: MemorySwitchStore,
  provider: BoundedFakeSwitchProvider,
  action: import('../src/decommission-advance.js').DecommissionAdvanceAction,
  selectedTargetSpec: DeploymentSpec = targetSpec,
  selectedPriorSpec: DeploymentSpec = priorSpec,
) {
  return {
    store,
    provider,
    priorSpec: selectedPriorSpec,
    targetSpec: selectedTargetSpec,
    currentSpec: selectedTargetSpec,
    action,
    maxProviderRequests: 9,
    clock: () => Date.parse('2026-08-13T00:00:00.000Z'),
    randomUUID: () => '123e4567-e89b-42d3-a456-426614174000',
  };
}

async function driveBoundedSwitch(
  store: MemorySwitchStore,
  provider: BoundedFakeSwitchProvider,
  selectedTargetSpec: DeploymentSpec = targetSpec,
  selectedPriorSpec: DeploymentSpec = priorSpec,
) {
  let action: import('../src/decommission-advance.js').DecommissionAdvanceAction =
    {
      kind: 'start',
    };
  for (let index = 0; index < 64; index += 1) {
    const result = await advanceBackendSwitchDecommission(
      boundedOptions(
        store,
        provider,
        action,
        selectedTargetSpec,
        selectedPriorSpec,
      ),
    );
    if (result.status === 'complete') return result;
    if (result.status === 'blocked') {
      throw new Error('bounded switch unexpectedly blocked');
    }
    action = { kind: 'continue', token: result.token };
  }
  throw new Error('bounded switch did not converge');
}

describe('backend switch state machine', () => {
  it('composes the bridge around the byte-identical prior application graph', () => {
    const nestedPrior = {
      ...priorSpec,
      mainModule: 'app/worker.js',
      modules: [
        { name: 'app/worker.js', content: 'export default { fetch() {} }' },
      ],
    };
    const artifact = composeLegacyBridgeArtifact(nestedPrior, {
      mainModule: 'trusted/bridge.js',
      modules: [
        {
          name: 'trusted/bridge.js',
          content: `import application from '${LEGACY_APPLICATION_MODULE_PLACEHOLDER}'; export { FlowsafeFleetAuditProxy } from './state.js'; export default { fetch: application.fetch };`,
        },
        {
          name: 'trusted/state.js',
          content: 'export class FlowsafeFleetAuditProxy {}',
        },
      ],
      compatibilityDate: '2026-08-11',
    });

    expect(artifact.mainModule).toBe('trusted/bridge.js');
    expect(artifact.modules[0]).toEqual(nestedPrior.modules[0]);
    expect(artifact.modules[1]?.content).toContain("'../app/worker.js'");
    expect(() =>
      composeLegacyBridgeArtifact(priorSpec, {
        mainModule: 'bridge.js',
        modules: [{ name: 'bridge.js', content: 'export default {}' }],
        compatibilityDate: '2026-08-11',
      }),
    ).toThrow(/placeholder/);
    expect(() =>
      composeLegacyBridgeArtifact(nestedPrior, {
        mainModule: 'bridge.js',
        modules: [
          {
            name: 'bridge.js',
            content: `${LEGACY_APPLICATION_MODULE_PLACEHOLDER} ${LEGACY_APPLICATION_MODULE_PLACEHOLDER}`,
          },
        ],
        compatibilityDate: '2026-08-11',
      }),
    ).toThrow(/exactly one/);
  });

  it('refuses every backend-switch entry while decommission advances', async () => {
    const cases: readonly Readonly<{
      name: string;
      run(
        store: MemorySwitchStore,
        provider: FakeSwitchProvider,
      ): Promise<unknown>;
    }>[] = [
      {
        name: 'reconcileFinalizedBackendSwitchState',
        run: (store, provider) =>
          reconcileFinalizedBackendSwitchState({
            provider,
            targetSpec,
            target,
            record: store.record,
            lease: {
              tenantTag: priorSpec.tenantTag,
              environment: priorSpec.environment,
              mutationLeaseTtlMs: 15 * 60_000,
              assertOwned: async () => {},
              renew: async () => {},
              put: async () => {},
              delete: async () => {},
            },
            clock: () => 1_000,
          }),
      },
      {
        name: 'switchPlainDeploymentToWorkersForPlatforms',
        run: (store, provider) =>
          switchPlainDeploymentToWorkersForPlatforms(
            switchOptions(store, provider),
          ),
      },
      {
        name: 'rollbackBackendSwitch',
        run: (store, provider) =>
          rollbackBackendSwitch({
            store,
            provider,
            priorSpec,
            targetSpec,
            secrets,
          }),
      },
      {
        name: 'finalizeBackendSwitch',
        run: (store, provider) =>
          finalizeBackendSwitch({ store, provider, targetSpec }),
      },
      {
        name: 'decommissionBackendSwitch',
        run: (store, provider) =>
          decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
      },
    ];

    for (const item of cases) {
      const store = new MemorySwitchStore();
      const provider = new FakeSwitchProvider();
      const current = store.record;
      store.record = decommissionAdvancingRecordFixture(current, 'ready');

      await expect(item.run(store, provider), item.name).rejects.toThrow(
        `${item.name} cannot run during an active decommission`,
      );
      expect(provider.calls, item.name).toEqual([]);
    }
  });

  it('persists authorization before every mutation and resumes a lost bridge response', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    provider.failBridgeResponseOnce = true;

    await expect(
      switchPlainDeploymentToWorkersForPlatforms(
        switchOptions(store, provider),
      ),
    ).rejects.toThrow(/response lost/);
    expect(store.intent?.subphase).toBe('bridge-upload-authorized');

    const completed = await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );

    expect(completed.subphase).toBe('ready');
    expect(backendSwitchIntentFromUnknown(structuredClone(completed))).toEqual(
      completed,
    );
    const cloned = structuredClone(completed);
    if (!cloned.candidate?.topology) {
      throw new Error('completed switch has no candidate topology');
    }
    const candidateApplication = {
      vars: [{ name: 'RELEASE_VALUE', value: 'candidate' }],
      secrets: [],
      r2Buckets: [],
    };
    const restarted = {
      ...cloned,
      candidate: {
        ...cloned.candidate,
        application: candidateApplication,
        topology: {
          ...cloned.candidate.topology,
          application: candidateApplication,
        },
      },
    };
    expect(
      backendSwitchIntentFromUnknown(restarted).candidate?.application,
    ).toEqual(candidateApplication);
    const divergent = {
      ...restarted,
      candidate: {
        ...restarted.candidate,
        topology: {
          ...restarted.candidate.topology,
          application: {
            ...candidateApplication,
            vars: [{ name: 'RELEASE_VALUE', value: 'different' }],
          },
        },
      },
    };
    expect(() => backendSwitchIntentFromUnknown(divergent)).toThrow(
      /application topology is inconsistent/,
    );
    expect(store.record).toMatchObject({
      backend: 'workers-for-platforms',
      activeRelease: { physicalScriptName: 'acme-candidate' },
      backendSwitchIntent: { subphase: 'ready' },
    });
    expect(provider.calls.indexOf('host-assert')).toBeLessThan(
      provider.calls.indexOf('domain-detach'),
    );
    expect(provider.calls.indexOf('candidate-assert')).toBeLessThan(
      provider.calls.indexOf('bridge-private'),
    );
    expect(store.phases).toEqual(
      expect.arrayContaining([
        'bridge-upload-authorized',
        'candidate-deploy-authorized',
        'host-publish-authorized',
        'domain-detach-authorized',
        'bridge-private-authorized',
        'ownership-commit-authorized',
      ]),
    );
  });

  it('routes to the bridge before draining dispatch and restoring the plain release', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    provider.calls.length = 0;

    const rolledBack = await rollbackBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
      secrets,
    });

    expect(rolledBack.subphase).toBe('rolled-back');
    expect(store.record).toMatchObject({
      backend: 'plain-worker',
      desiredSpecDigest: deploymentSpecDigest(priorSpec),
      backendSwitchIntent: { subphase: 'rolled-back' },
    });
    expect(store.record.platformResources).toBeUndefined();
    expect(provider.calls).toEqual([
      'rollback-route',
      'rollback-route-assert',
      'rollback-route-assert',
      'rollback-drain',
      'rollback-restore',
      'commit-plain',
    ]);
  });

  it('rolls back and cleans up from a provider-authorized bridge crash', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    provider.failBridgeResponseOnce = true;
    await expect(
      switchPlainDeploymentToWorkersForPlatforms(
        switchOptions(store, provider),
      ),
    ).rejects.toThrow(/response lost/);

    const rolledBack = await rollbackBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
      secrets,
    });

    expect(rolledBack.subphase).toBe('rolled-back');
    expect(store.record).toMatchObject({
      backend: 'plain-worker',
      artifactVersion: 'plain-restored-v2',
      backendSwitchIntent: {
        subphase: 'rolled-back',
        bridge: { namespaceIds: ['namespace-runner'] },
      },
    });
    expect(provider.calls).toEqual([
      'snapshot',
      'bridge',
      'rollback-route',
      'rollback-route-assert',
      'rollback-route-assert',
      'rollback-restore',
      'commit-plain',
    ]);
  });

  it('does not commit an incomplete canonical ownership record', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    provider.commitWorkersForPlatformsOwnership = async (input) => ({
      ...input.currentRecord,
      backend: 'workers-for-platforms' as const,
    });

    await expect(
      switchPlainDeploymentToWorkersForPlatforms(
        switchOptions(store, provider),
      ),
    ).rejects.toThrow(/incomplete Workers for Platforms ownership/);

    expect(store.record.backend).toBe('plain-worker');
    expect(store.intent?.subphase).toBe('ownership-commit-authorized');
  });

  it('finalizes only after the rollback window without changing physical namespaces', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    const options = {
      ...switchOptions(store, provider),
      rollbackUntil: '2026-08-12T00:00:00.000Z',
    };
    await switchPlainDeploymentToWorkersForPlatforms(options);

    await expect(
      finalizeBackendSwitch({
        store,
        provider,
        targetSpec,
        now: new Date('2026-08-11T23:59:59.000Z'),
      }),
    ).rejects.toThrow(/still open/);
    const finalized = await finalizeBackendSwitch({
      store,
      provider,
      targetSpec,
      now: new Date('2026-08-12T00:00:00.000Z'),
    });

    expect(finalized.subphase).toBe('finalized');
    expect(finalized.bridge?.scriptName).toBe(priorSpec.scriptName);
    expect(finalized.bridge?.namespaceIds).toEqual(['namespace-runner']);
    expect(finalized.bridge?.stateOnly).toBe(true);
    expect(store.record.platformResources?.stateWorker).toMatchObject({
      scriptName: priorSpec.scriptName,
      artifactVersion: 'state-v2',
      artifactDigest: target.stateArtifactDigest,
    });
  });

  it('adopts finalized bridge state after the fleet write response is lost', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms({
      ...switchOptions(store, provider),
      rollbackUntil: '2026-08-12T00:00:00.000Z',
    });
    await finalizeBackendSwitch({
      store,
      provider,
      targetSpec,
      now: new Date('2026-08-12T00:00:00.000Z'),
    });
    store.failAfterCommittedStateReconcile = true;

    await expect(
      store.withDeploymentLease('acme', 'production', (lease) =>
        reconcileFinalizedBackendSwitchState({
          provider,
          targetSpec,
          target,
          record: store.record,
          lease,
          clock: () => Date.parse('2026-08-13T00:00:00.000Z'),
        }),
      ),
    ).rejects.toThrow(/write response lost/);
    expect(
      store.record.backendSwitchIntent?.stateReconcileIntent?.subphase,
    ).toBe('uploaded');

    await expect(
      store.withDeploymentLease('acme', 'production', (lease) =>
        reconcileFinalizedBackendSwitchState({
          provider,
          targetSpec,
          target,
          record: store.record,
          lease,
          clock: () => Date.parse('2026-08-13T00:00:01.000Z'),
        }),
      ),
    ).resolves.toMatchObject({
      backendSwitchIntent: {
        subphase: 'finalized',
        stateReconcileIntent: { subphase: 'uploaded' },
      },
    });
    expect(
      provider.calls.filter((call) => call === 'finalized-state'),
    ).toHaveLength(2);
    expect(store.record.backendSwitchIntent?.bridgePlan).toEqual(
      store.record.backendSwitchIntent?.stateReconcileIntent?.plan,
    );
  });

  it('rejects a finalized state-egress credential change before provider mutation', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms({
      ...switchOptions(store, provider),
      rollbackUntil: '2026-08-12T00:00:00.000Z',
    });
    await finalizeBackendSwitch({
      store,
      provider,
      targetSpec,
      now: new Date('2026-08-12T00:00:00.000Z'),
    });
    provider.calls.length = 0;
    const before = structuredClone(store.record);

    await expect(
      store.withDeploymentLease('acme', 'production', (lease) =>
        reconcileFinalizedBackendSwitchState({
          provider,
          targetSpec,
          target: {
            ...target,
            stateEgressCredentialDigest: 'f'.repeat(64),
          },
          record: store.record,
          lease,
          clock: () => Date.parse('2026-08-13T00:00:00.000Z'),
        }),
      ),
    ).rejects.toThrow(/coordinated credential migration/);

    expect(provider.calls).toEqual([]);
    expect(store.record).toEqual(before);
  });

  it('rebuilds policy-only finalized resources and rejects a malformed ownership projection', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms({
      ...switchOptions(store, provider),
      rollbackUntil: '2026-08-12T00:00:00.000Z',
    });
    await finalizeBackendSwitch({
      store,
      provider,
      targetSpec,
      now: new Date('2026-08-12T00:00:00.000Z'),
    });
    const policyTarget = {
      ...target,
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: target.outboundPolicy.policyId,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        allowedHosts: ['new-api.example.com'],
      }),
    };

    const reconciled = await store.withDeploymentLease(
      'acme',
      'production',
      (lease) =>
        reconcileFinalizedBackendSwitchState({
          provider,
          targetSpec,
          target: policyTarget,
          record: store.record,
          lease,
          clock: () => Date.parse('2026-08-13T00:00:00.000Z'),
        }),
    );
    expect(structuredClone(store.record).platformResources).toEqual({
      maintenanceCapabilityPublicKey:
        policyTarget.maintenanceCapabilityPublicKey,
      stateWorker: {
        scriptName: priorSpec.scriptName,
        artifactVersion: 'state-v2',
        artifactDigest: policyTarget.stateArtifactDigest,
        plane: 'ordinary',
        durableObjectTag: policyTarget.stateDurableObjectTag,
        durableObjectBindings: provider.bridge.durableObjectBindings,
        namespaceIds: provider.bridge.namespaceIds,
      },
      outboundPolicy: policyTarget.outboundPolicy,
      sharedOutboundWorkerName: policyTarget.sharedOutboundWorkerName,
    });
    expect(reconciled.platformTarget).toEqual(policyTarget);

    const exactCommit = provider.commitFinalizedOwnership.bind(provider);
    provider.commitFinalizedOwnership = async (input) => ({
      ...(await exactCommit(input)),
      applicationBindings: {
        vars: [{ name: 'FOREIGN', value: 'changed' }],
        secrets: [],
        r2Buckets: [],
      },
    });
    await expect(
      store.withDeploymentLease('acme', 'production', (lease) =>
        reconcileFinalizedBackendSwitchState({
          provider,
          targetSpec,
          target: policyTarget,
          record: store.record,
          lease,
          clock: () => Date.parse('2026-08-13T00:00:01.000Z'),
        }),
      ),
    ).rejects.toThrow(/immutable deployment or release identity/);
    expect(store.record.applicationBindings).toEqual(
      reconciled.applicationBindings,
    );
  });

  it('provisions an unchanged switched and finalized deployment through the ordinary bridge lifecycle', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms({
      ...switchOptions(store, provider),
      rollbackUntil: '2026-08-12T00:00:00.000Z',
    });
    await finalizeBackendSwitch({
      store,
      provider,
      targetSpec,
      now: new Date('2026-08-12T00:00:00.000Z'),
    });
    provider.calls.length = 0;
    let normalPlatformCalls = 0;
    const backend = {
      kind: 'workers-for-platforms' as const,
      immutableExternalArtifacts: true as const,
      releaseScriptName: () => 'acme-candidate',
      getDatabase: async () => ({
        id: store.record.databaseId,
        name: store.record.databaseName,
        created: false,
      }),
      readDeploymentIdentity: async () => targetSpec.tenantTag,
      ensurePlatformResources: async () => {
        normalPlatformCalls += 1;
        throw new Error('normal platform resources must remain dispatch-only');
      },
      inspect: async () => ({
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        scriptName: 'acme-candidate',
        databaseId: store.record.databaseId,
        durableObjectBindings: targetSpec.durableObjectBindings.map(
          (binding) => ({
            ...binding,
            namespaceId: 'namespace-runner',
            scriptName: targetSpec.scriptName,
          }),
        ),
        serviceBindings: [],
        queueProducerBindings: [],
        plainTextBindings: {},
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        providerBindingIdentities: [
          { type: 'd1', name: 'DB' },
          { type: 'durable_object_namespace', name: 'RUNNER' },
          { type: 'secret_text', name: 'DEPLOYMENT_IDENTITY_SECRET' },
        ],
        artifactVersion: 'candidate-v1',
        desiredSpecDigest: deploymentSpecDigest(targetSpec),
        schemaVersion: targetSpec.schemaVersion,
        maintenance: {
          armed: true,
          nextAlarmAt: 2_000,
          lastSweepAt: 1_000,
          lastPurgeAt: 1_000,
        },
      }),
    } as unknown as ProvisioningBackend;

    const result = await provisionDeployment({
      initialExecutionFenceState: 'open',
      backend,
      store,
      spec: targetSpec,
      secrets,
      finalizedStateProvider: provider,
    });

    expect(result.record).toMatchObject({
      phase: 'ready',
      activeRelease: { physicalScriptName: 'acme-candidate' },
      platformResources: {
        stateWorker: {
          plane: 'ordinary',
          scriptName: priorSpec.scriptName,
          namespaceIds: ['namespace-runner'],
        },
      },
      backendSwitchIntent: {
        subphase: 'finalized',
        stateReconcileIntent: { subphase: 'uploaded' },
      },
    });
    expect(normalPlatformCalls).toBe(0);
    expect(provider.calls).toEqual(['finalized-state']);
  });

  it('rejects a shallow or malformed durable intent', () => {
    const malformed = {
      kind: 'backend-switch',
      tenantTag: 'acme',
      environment: 'production',
      subphase: 'ready',
    };
    expect(() => backendSwitchIntentFromUnknown(malformed)).toThrow(
      /invalid intent/,
    );
  });

  it.each([
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
  ] as const)('decommissions safely after a lost provider response in %s', async (subphase) => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    const completed = await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    const { bridge, candidate, ...withoutBridgeOrCandidate } = completed;
    const { candidate: _candidate, ...withoutCandidate } = completed;
    store.record = {
      ...store.record,
      backendSwitchIntent: {
        ...(subphase === 'bridge-upload-authorized'
          ? withoutBridgeOrCandidate
          : subphase === 'candidate-deploy-authorized'
            ? withoutCandidate
            : completed),
        subphase,
      },
    };
    void bridge;
    void candidate;
    provider.calls.length = 0;

    const decommissioned = await decommissionBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
    });

    expect(decommissioned.subphase).toBe('decommissioned');
    expect(store.record).toMatchObject({
      phase: 'decommissioned',
      databaseExportLocation: 'r2://exports/acme.sql',
      databaseExportSha256: 'f'.repeat(64),
      databaseExportSize: 123,
    });
    expect(provider.calls).toEqual([
      'decommission-traffic',
      'decommission-release:acme-candidate',
      'decommission-bridge',
      'decommission-export',
      'decommission-database',
    ]);
    expect(store.phases).toEqual(
      expect.arrayContaining([
        'decommission-traffic-authorized',
        'decommission-candidate-authorized',
        'decommission-bridge-authorized',
        'decommission-export-authorized',
        'decommission-database-authorized',
      ]),
    );
  });

  it.each(
    DECOMMISSION_ROUTE_PHASES,
  )('freezes exact route ambiguity while decommissioning %s with platformOnly=%s', async (subphase, platformOnly, expectedRoutes) => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms({
      ...switchOptions(store, provider),
      rollbackUntil: '2026-08-12T00:00:00.000Z',
    });
    await finalizeBackendSwitch({
      store,
      provider,
      targetSpec,
      now: new Date('2026-08-12T00:00:00.000Z'),
    });
    const priorRelease = store.record.activeRelease;
    const priorResources = store.record.platformResources;
    if (!priorRelease || !priorResources) {
      throw new Error('finalized switch has incomplete route authority');
    }
    const targetRelease = platformOnly
      ? priorRelease
      : {
          ...priorRelease,
          physicalScriptName: 'acme-next',
          specDigest: '6'.repeat(64),
          artifactVersion: 'candidate-v2',
        };
    const nextTarget: ExternalPlatformTargetDescription = {
      ...target,
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: target.outboundPolicy.policyId,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        allowedHosts: ['next.example.com'],
      }),
    };
    const currentTarget =
      subphase === 'planned' || subphase === 'schema-applied'
        ? target
        : nextTarget;
    const switchIntent = store.intent;
    if (!switchIntent) throw new Error('finalized switch intent is missing');
    store.record = {
      ...store.record,
      phase: 'migrating',
      activeRelease: priorRelease,
      ...(platformOnly
        ? {}
        : {
            pendingRelease: targetRelease,
            migrationPriorRelease: priorRelease,
          }),
      platformTarget: currentTarget,
      outboundPolicy: currentTarget.outboundPolicy,
      platformResources: {
        ...priorResources,
        outboundPolicy: currentTarget.outboundPolicy,
      },
      migrationIntent: {
        ...(platformOnly ? { platformOnly: true as const } : {}),
        targetSpecDigest: targetRelease.specDigest,
        priorRelease,
        priorTarget: target,
        priorOutboundPolicy: target.outboundPolicy,
        targetRelease,
        target: nextTarget,
        subphase,
      },
      backendSwitchIntent: {
        ...switchIntent,
        target: currentTarget,
      },
    };
    provider.calls.length = 0;

    const completed = await decommissionBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
    });
    const describe = (
      value: import('../src/host-routing.js').HostRoutingTarget,
    ) => `${value.scriptName}:${value.policyHosts.join(',')}`;
    const expectedRouteSet = [...expectedRoutes].sort();

    expect(provider.removedRouteTargets).toHaveLength(1);
    expect(provider.removedRouteTargets[0]?.map(describe).sort()).toEqual(
      expectedRouteSet,
    );
    expect(
      completed.decommissionSnapshot?.routeTargets
        .map(({ routeTarget }) => describe(routeTarget))
        .sort(),
    ).toEqual(expectedRouteSet);
    const conflictingFallback = store.record.migrationIntent
      ? {
          ...store.record,
          migrationIntent: {
            ...store.record.migrationIntent,
            subphase:
              store.record.migrationIntent.subphase === 'route-published'
                ? ('planned' as const)
                : ('route-published' as const),
          },
        }
      : store.record;
    expect(
      externalRouteExpectations(conflictingFallback)
        .map((expectation) =>
          describe(externalHostRoutingTarget(conflictingFallback, expectation)),
        )
        .sort(),
    ).toEqual(expectedRouteSet);
    expect(backendSwitchIntentFromUnknown(structuredClone(completed))).toEqual(
      completed,
    );
  });

  it.each([
    ['rollback precommit', false],
    ['rollback promotion response lost after commit', false],
    ['rollback post-promotion inspection', false],
    ['rollback final state write before commit', false],
    ['rollback final state write after commit', true],
  ] as const)('freezes rollback route authority after %s and decommissions', async (_boundary, settled) => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms({
      ...switchOptions(store, provider),
      rollbackUntil: '2026-08-12T00:00:00.000Z',
    });
    await finalizeBackendSwitch({
      store,
      provider,
      targetSpec,
      now: new Date('2026-08-12T00:00:00.000Z'),
    });
    const activeRelease = store.record.activeRelease;
    if (!activeRelease || !store.intent) {
      throw new Error('finalized switch has incomplete rollback authority');
    }
    const rollbackRelease: ExternalReleaseSnapshot = {
      ...activeRelease,
      physicalScriptName: 'acme-rollback',
      specDigest: '7'.repeat(64),
      artifactVersion: 'rollback-v1',
    };
    store.record = settled
      ? {
          ...store.record,
          phase: 'ready',
          activeRelease: rollbackRelease,
          rollbackRelease: activeRelease,
          desiredSpecDigest: rollbackRelease.specDigest,
        }
      : {
          ...store.record,
          phase: 'rolling-back',
          activeRelease,
          pendingRelease: rollbackRelease,
          rollbackRelease,
        };
    provider.calls.length = 0;

    await decommissionBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
    });

    expect(
      provider.removedRouteTargets[0]?.map(({ scriptName }) => scriptName),
    ).toEqual(
      settled ? ['acme-rollback'] : ['acme-candidate', 'acme-rollback'],
    );
    expect(store.record.phase).toBe('decommissioned');
  });

  it('snapshots and deletes every current release after finalized migrations and rollback', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    const original = store.record.activeRelease;
    if (!original) throw new Error('switch did not persist an active release');
    const release = (
      physicalScriptName: string,
      digestCharacter: string,
      artifactVersion = `${physicalScriptName}-v1`,
    ): ExternalReleaseSnapshot => ({
      ...original,
      physicalScriptName,
      specDigest: digestCharacter.repeat(64),
      artifactVersion,
    });
    store.record = {
      ...store.record,
      desiredSpecDigest: '8'.repeat(64),
      activeRelease: release('acme-active', '1'),
      pendingRelease: release('acme-pending', '2', 'pending'),
      migrationPriorRelease: release('acme-migration-prior', '3'),
      rollbackRelease: release('acme-rollback', '4'),
      retiringRelease: release('acme-retiring', '5'),
    };
    provider.calls.length = 0;

    const completed = await decommissionBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
    });

    const expectedNames = [
      'acme-active',
      'acme-candidate',
      'acme-migration-prior',
      'acme-pending',
      'acme-retiring',
      'acme-rollback',
    ];
    expect(completed.decommissionSnapshot).toMatchObject({
      desiredSpecDigest: '8'.repeat(64),
      routeTargets: [
        {
          release: { physicalScriptName: 'acme-active' },
          routeTarget: { scriptName: 'acme-active' },
        },
      ],
      releases: expectedNames.map((physicalScriptName) => ({
        release: { physicalScriptName },
        subphase: 'deleted',
      })),
    });
    expect(
      provider.calls.filter((call) => call.startsWith('decommission-release:')),
    ).toEqual(expectedNames.map((name) => `decommission-release:${name}`));
    expect(provider.calls).toContain('decommission-bridge');
    expect(store.record.phase).toBe('decommissioned');
    expect(backendSwitchIntentFromUnknown(structuredClone(completed))).toEqual(
      completed,
    );
  });

  it('rejects conflicting release snapshots before decommission mutation', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    const active = store.record.activeRelease;
    if (!active) throw new Error('switch did not persist an active release');
    store.record = {
      ...store.record,
      rollbackRelease: { ...active, specDigest: '7'.repeat(64) },
    };
    provider.calls.length = 0;

    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/conflicting snapshots/);

    expect(provider.calls).toEqual([]);
    expect(store.record.phase).toBe('ready');
    expect(store.intent?.decommissionSnapshot).toBeUndefined();
  });

  it('rejects duplicated, foreign, and non-exact decommission route authority', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    const completed = await decommissionBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
    });
    const route = completed.decommissionSnapshot?.routeTargets[0];
    if (!route) throw new Error('decommission route authority is missing');

    const decommissionSnapshot = completed.decommissionSnapshot;
    if (!decommissionSnapshot) {
      throw new Error('decommission snapshot is missing');
    }
    const duplicated = {
      ...completed,
      decommissionSnapshot: {
        ...decommissionSnapshot,
        routeTargets: [route, structuredClone(route)],
      },
    };
    expect(() => backendSwitchIntentFromUnknown(duplicated)).toThrow(
      /routes are duplicated/,
    );

    const foreign = {
      ...completed,
      decommissionSnapshot: {
        ...decommissionSnapshot,
        routeTargets: [
          {
            ...route,
            release: {
              ...route.release,
              physicalScriptName: 'foreign-release',
            },
            routeTarget: {
              ...route.routeTarget,
              scriptName: 'foreign-release',
            },
          },
        ],
      },
    };
    expect(() => backendSwitchIntentFromUnknown(foreign)).toThrow(
      /invalid decommission route/,
    );

    const drifted = {
      ...completed,
      decommissionSnapshot: {
        ...decommissionSnapshot,
        routeTargets: [
          {
            ...route,
            routeTarget: {
              ...route.routeTarget,
              policyDigest: 'f'.repeat(64),
            },
          },
        ],
      },
    };
    expect(() => backendSwitchIntentFromUnknown(drifted)).toThrow(
      /invalid decommission route/,
    );
  });

  it('resumes release deletion after provider and fleet-write response loss without repeating the physical delete', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    provider.calls.length = 0;
    provider.failReleaseDeleteResponseOnce = true;

    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/release delete response lost/);
    expect(store.intent?.decommissionSnapshot?.releases[0]?.subphase).toBe(
      'delete-authorized',
    );
    expect(store.record.phase).toBe('decommissioning');

    store.failAfterCommittedReleaseDelete = true;
    const completed = await decommissionBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
    });
    expect(completed.subphase).toBe('decommissioned');
    expect(provider.releaseDeleteMutations.get('acme-candidate')).toBe(1);
    expect(
      provider.calls.filter(
        (call) => call === 'decommission-release:acme-candidate',
      ),
    ).toHaveLength(2);
  });

  it('persists per-bucket R2 teardown across attachments, contents, partial progress, and a lost delete response', async () => {
    const resources: readonly ApplicationR2Resource[] = [
      switchApplicationR2Resource('ARCHIVE', 'eu', '2026-08-11T00:00:01.000Z'),
      switchApplicationR2Resource(
        'FILES',
        'default',
        '2026-08-11T00:00:00.000Z',
      ),
    ];
    const application = {
      vars: [],
      secrets: [],
      r2Buckets: resources.map(({ name, bucketName, jurisdiction }) => ({
        name,
        bucketName,
        jurisdiction,
      })),
    };
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    for (const resource of resources) {
      provider.r2Buckets.set(resource.bucketName, resource);
    }
    const switched = await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    store.record = {
      ...store.record,
      applicationResources: resources,
      applicationBindings: application,
      backendSwitchIntent: {
        ...switched,
        prior: {
          ...switched.prior,
          applicationResources: resources,
          application,
        },
      },
    };
    provider.calls.length = 0;
    provider.attachedR2.add(resources[0]?.bucketName as string);

    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/remains attached/);
    expect(store.intent?.applicationR2Progress?.[0]?.subphase).toBe(
      'detach-authorized',
    );

    provider.attachedR2.clear();
    provider.nonemptyR2.add(resources[0]?.bucketName as string);
    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/not empty/);
    expect(store.intent?.applicationR2Progress?.[0]?.subphase).toBe(
      'empty-authorized',
    );

    provider.nonemptyR2.clear();
    provider.attachedR2.add(resources[1]?.bucketName as string);
    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/remains attached/);
    expect(
      store.intent?.applicationR2Progress?.map((entry) => entry.subphase),
    ).toEqual(['deleted', 'detach-authorized']);

    provider.attachedR2.clear();
    provider.failR2DeleteResponseOnce = true;
    const completed = await decommissionBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
    });
    expect(completed.subphase).toBe('decommissioned');
    expect(
      completed.applicationR2Progress?.map((entry) => entry.subphase),
    ).toEqual(['deleted', 'deleted']);
    expect(backendSwitchIntentFromUnknown(structuredClone(completed))).toEqual(
      completed,
    );
  });

  it('leaves backend-switch traffic and durable state unchanged when R2 evacuation fails', async () => {
    const resource = switchApplicationR2Resource(
      'FILES',
      'default',
      '2026-08-11T00:00:00.000Z',
    );
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    provider.r2Buckets.set(resource.bucketName, resource);
    const switched = await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    store.record = {
      ...store.record,
      applicationResources: [resource],
      backendSwitchIntent: {
        ...switched,
        prior: {
          ...switched.prior,
          applicationResources: [resource],
          application: {
            vars: [],
            secrets: [],
            r2Buckets: [
              {
                name: resource.name,
                bucketName: resource.bucketName,
                jurisdiction: resource.jurisdiction,
              },
            ],
          },
        },
      },
    };
    provider.calls.length = 0;
    provider.nonemptyR2.add(resource.bucketName);

    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/not empty/u);
    expect(provider.calls).toEqual(['r2-empty:FILES']);
    expect(store.intent?.subphase).toBe(switched.subphase);
    expect(store.record?.phase).toBe('ready');
    expect(store.intent?.decommissionSnapshot).toBeUndefined();
  });

  it('preserves the traffic-removed switch when R2 receives a late write and resumes after direct evacuation', async () => {
    const resource = switchApplicationR2Resource(
      'FILES',
      'default',
      '2026-08-11T00:00:00.000Z',
    );
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    provider.r2Buckets.set(resource.bucketName, resource);
    const switched = await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    store.record = {
      ...store.record,
      applicationResources: [resource],
      backendSwitchIntent: {
        ...switched,
        prior: {
          ...switched.prior,
          applicationResources: [resource],
          application: {
            vars: [],
            secrets: [],
            r2Buckets: [
              {
                name: resource.name,
                bucketName: resource.bucketName,
                jurisdiction: resource.jurisdiction,
              },
            ],
          },
        },
      },
    };
    provider.calls.length = 0;
    provider.writeR2AfterTrafficRemovalOnce = true;

    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/not empty/u);
    expect(store.intent?.subphase).toBe('decommission-traffic-removed');
    expect(store.record.phase).toBe('decommissioning');
    expect(provider.switchTrafficRemoved).toBe(true);
    expect(provider.deletedReleases.size).toBe(0);
    expect(provider.r2Buckets.has(resource.bucketName)).toBe(true);

    provider.nonemptyR2.clear();
    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).resolves.toMatchObject({ subphase: 'decommissioned' });
    expect(
      provider.calls.filter((call) => call === 'decommission-traffic'),
    ).toHaveLength(1);
  });

  it('reconciles traffic authorization, provider response loss, and traffic-removed commit loss before switch deletion', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    provider.calls.length = 0;
    store.failAfterCommittedTrafficAuthorization = true;

    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).resolves.toMatchObject({ subphase: 'decommissioned' });
    expect(
      provider.calls.filter((call) => call === 'decommission-traffic'),
    ).toHaveLength(1);

    const providerLossStore = new MemorySwitchStore();
    const providerLoss = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(providerLossStore, providerLoss),
    );
    providerLoss.calls.length = 0;
    providerLoss.failTrafficRemovalResponseOnce = true;
    await expect(
      decommissionBackendSwitch({
        store: providerLossStore,
        provider: providerLoss,
        priorSpec,
        targetSpec,
      }),
    ).rejects.toThrow(/switch traffic removal response lost/u);
    expect(providerLossStore.intent?.subphase).toBe(
      'decommission-traffic-authorized',
    );
    expect(providerLoss.switchTrafficRemoved).toBe(true);
    await expect(
      decommissionBackendSwitch({
        store: providerLossStore,
        provider: providerLoss,
        priorSpec,
        targetSpec,
      }),
    ).resolves.toMatchObject({ subphase: 'decommissioned' });
    expect(
      providerLoss.calls.filter((call) => call === 'decommission-traffic'),
    ).toHaveLength(2);

    const commitLossStore = new MemorySwitchStore();
    const commitLossProvider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(commitLossStore, commitLossProvider),
    );
    commitLossProvider.calls.length = 0;
    commitLossStore.failAfterCommittedTrafficRemoval = true;
    await expect(
      decommissionBackendSwitch({
        store: commitLossStore,
        provider: commitLossProvider,
        priorSpec,
        targetSpec,
      }),
    ).resolves.toMatchObject({ subphase: 'decommissioned' });
    expect(
      commitLossProvider.calls.filter(
        (call) => call === 'decommission-traffic',
      ),
    ).toHaveLength(1);
  });

  it('blocks switch deletion on post-removal ingress drift and reasserts after operator repair', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    provider.calls.length = 0;
    provider.switchTrafficDrift = true;

    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/traffic remains/u);
    expect(store.intent?.subphase).toBe('decommission-traffic-authorized');
    expect(provider.deletedReleases.size).toBe(0);
    expect(store.record.phase).toBe('decommissioning');

    provider.switchTrafficDrift = false;
    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).resolves.toMatchObject({ subphase: 'decommissioned' });
    expect(
      provider.calls.filter((call) => call === 'decommission-traffic'),
    ).toHaveLength(2);
  });

  it('routes ordinary decommission through the dedicated switch teardown', async () => {
    const store = new MemorySwitchStore();
    const provider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    provider.calls.length = 0;

    const result = await decommissionDeployment({
      backend: {} as ProvisioningBackend,
      store,
      spec: targetSpec,
      backendSwitch: { provider, priorSpec, targetSpec },
    });

    expect(result.record.phase).toBe('decommissioned');
    expect(result.databaseExport.location).toBe('r2://exports/acme.sql');
    expect(provider.calls).toEqual([
      'decommission-traffic',
      'decommission-release:acme-candidate',
      'decommission-bridge',
      'decommission-export',
      'decommission-database',
    ]);
  });

  it('advances one bounded backend-switch action under one stable operation', async () => {
    const store = new MemorySwitchStore();
    const provider = new BoundedFakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    bindBoundedDatabase(store, provider);
    provider.calls.length = 0;

    const started = await advanceBackendSwitchDecommission(
      boundedOptions(store, provider, { kind: 'start' }),
    );
    expect(started).toMatchObject({
      status: 'pending',
      token: { operationId: '123e4567-e89b-42d3-a456-426614174000' },
    });
    expect(store.record.decommissionIntent).toMatchObject({
      revision: 0,
      generation: 0,
      state: 'transitioning',
    });
    expect(provider.calls).toEqual([]);

    Object.defineProperty(provider, 'getSwitchDatabase', {
      configurable: true,
      value: undefined,
    });
    await expect(
      advanceBackendSwitchDecommission(
        boundedOptions(store, provider, { kind: 'start' }),
      ),
    ).rejects.toMatchObject({ capability: 'database-read' });
    Reflect.deleteProperty(provider, 'getSwitchDatabase');

    if (started.status !== 'pending') throw new Error('missing start token');
    const advanced = await advanceBackendSwitchDecommission(
      boundedOptions(store, provider, {
        kind: 'continue',
        token: started.token,
      }),
    );
    expect(advanced.status).toBe('pending');
    expect(provider.calls).toEqual(['decommission-traffic']);
    expect(store.intent?.subphase).toBe('decommission-traffic-removed');
    await expect(
      withBackendSwitchLease(
        store,
        priorSpec.tenantTag,
        priorSpec.environment,
        Date.now,
        (lease) => lease.put(store.intent as BackendSwitchIntent),
      ),
    ).rejects.toThrow(
      'backend switch lease put requires putOwnership when a decommission shell is present',
    );

    const currentIntent = store.intent as BackendSwitchIntent;
    const currentRecord = structuredClone(store.record);
    const { decommissionIntent: _shell, ...shellless } = currentRecord;
    let refusedClockCalls = 0;
    const currentShell = currentRecord.decommissionIntent as Exclude<
      DecommissionAdvanceIntent,
      { readonly state: 'complete' }
    >;
    const withoutOuterUpdatedAt = structuredClone(currentRecord) as Partial<
      typeof currentRecord
    >;
    Reflect.deleteProperty(withoutOuterUpdatedAt, 'updatedAt');
    const withoutShellUpdatedAt = structuredClone(currentShell) as Partial<
      typeof currentShell
    >;
    Reflect.deleteProperty(withoutShellUpdatedAt, 'updatedAt');
    const placeholderCases: readonly Readonly<{
      label: string;
      candidate: unknown;
      message: string;
    }>[] = [
      {
        label: 'shell removal',
        candidate: shellless,
        message: 'backend switch decommission record is malformed',
      },
      {
        label: 'transparent proxy',
        candidate: new Proxy(currentRecord, {}),
        message: 'backend switch decommission record is malformed',
      },
      {
        label: 'missing outer placeholder',
        candidate: withoutOuterUpdatedAt,
        message:
          'backend switch lease ownership timestamp placeholder is stale',
      },
      {
        label: 'invalid outer placeholder',
        candidate: { ...currentRecord, updatedAt: 'invalid' },
        message:
          'backend switch lease ownership timestamp placeholder is stale',
      },
      {
        label: 'stale outer placeholder',
        candidate: {
          ...currentRecord,
          updatedAt: '2026-08-12T23:59:59.000Z',
        },
        message:
          'backend switch lease ownership timestamp placeholder is stale',
      },
      {
        label: 'missing shell placeholder',
        candidate: {
          ...currentRecord,
          decommissionIntent: withoutShellUpdatedAt,
        },
        message:
          'backend switch lease ownership timestamp placeholder is stale',
      },
      {
        label: 'invalid shell placeholder',
        candidate: {
          ...currentRecord,
          decommissionIntent: { ...currentShell, updatedAt: 'invalid' },
        },
        message:
          'backend switch lease ownership timestamp placeholder is stale',
      },
      {
        label: 'stale shell placeholder',
        candidate: {
          ...currentRecord,
          decommissionIntent: {
            ...currentShell,
            updatedAt: '2026-08-12T23:59:59.000Z',
          },
        },
        message:
          'backend switch lease ownership timestamp placeholder is stale',
      },
      {
        label: 'both equal stale placeholders',
        candidate: {
          ...currentRecord,
          updatedAt: '2026-08-12T23:59:59.000Z',
          decommissionIntent: {
            ...currentShell,
            updatedAt: '2026-08-12T23:59:59.000Z',
          },
        },
        message:
          'backend switch lease ownership timestamp placeholder is stale',
      },
    ];
    for (const { label, candidate, message } of placeholderCases) {
      const getCalls = store.getCalls;
      const putCalls = store.putCalls;
      await expect(
        withBackendSwitchLease(
          store,
          priorSpec.tenantTag,
          priorSpec.environment,
          () => {
            refusedClockCalls += 1;
            return Date.parse('2026-08-13T00:00:01.000Z');
          },
          (lease) =>
            lease.putOwnership(
              candidate as unknown as FleetRecord,
              currentIntent,
            ),
        ),
        label,
      ).rejects.toThrow(message);
      expect(store.getCalls - getCalls, label).toBe(1);
      expect(store.putCalls - putCalls, label).toBe(0);
    }
    expect(refusedClockCalls).toBe(0);

    let successClockCalls = 0;
    let getCalls = store.getCalls;
    let putCalls = store.putCalls;
    await withBackendSwitchLease(
      store,
      priorSpec.tenantTag,
      priorSpec.environment,
      () => {
        successClockCalls += 1;
        return Date.parse('2026-08-13T00:00:01.000Z');
      },
      (lease) => lease.putOwnership(lease.current(), currentIntent),
    );
    expect(successClockCalls).toBe(1);
    expect(store.record.updatedAt).toBe('2026-08-13T00:00:01.000Z');
    expect(store.record.decommissionIntent?.updatedAt).toBe(
      store.record.updatedAt,
    );
    expect(store.getCalls - getCalls).toBe(1);
    expect(store.putCalls - putCalls).toBe(1);

    const precommit = new Error('switch write failed before commit');
    store.failNextPutBeforeCommit = precommit;
    getCalls = store.getCalls;
    putCalls = store.putCalls;
    let observedPrecommit: unknown;
    try {
      await withBackendSwitchLease(
        store,
        priorSpec.tenantTag,
        priorSpec.environment,
        () => Date.parse('2026-08-13T00:00:02.000Z'),
        (lease) => lease.putOwnership(lease.current(), currentIntent),
      );
    } catch (error) {
      observedPrecommit = error;
    }
    expect(observedPrecommit).toBe(precommit);
    expect(store.record.updatedAt).toBe('2026-08-13T00:00:01.000Z');
    expect(store.getCalls - getCalls).toBe(2);
    expect(store.putCalls - putCalls).toBe(1);

    const committedResponseLoss = new Error('switch write response lost');
    store.failNextPutAfterCommit = committedResponseLoss;
    getCalls = store.getCalls;
    putCalls = store.putCalls;
    await withBackendSwitchLease(
      store,
      priorSpec.tenantTag,
      priorSpec.environment,
      () => Date.parse('2026-08-13T00:00:03.000Z'),
      (lease) => lease.putOwnership(lease.current(), currentIntent),
    );
    expect(store.record.updatedAt).toBe('2026-08-13T00:00:03.000Z');
    expect(store.getCalls - getCalls).toBe(2);
    expect(store.putCalls - putCalls).toBe(1);

    const malformedRereadLoss = new Error(
      'switch malformed reread response lost',
    );
    store.failNextPutAfterCommit = malformedRereadLoss;
    store.nextGetAfterCommittedFailure = {
      ...store.record,
      routeHostname: 'foreign.example.test',
    };
    getCalls = store.getCalls;
    putCalls = store.putCalls;
    let observedMalformedReread: unknown;
    try {
      await withBackendSwitchLease(
        store,
        priorSpec.tenantTag,
        priorSpec.environment,
        () => Date.parse('2026-08-13T00:00:04.000Z'),
        (lease) => lease.putOwnership(lease.current(), currentIntent),
      );
    } catch (error) {
      observedMalformedReread = error;
    }
    expect(observedMalformedReread).toEqual(
      new Error('backend switch decommission record is malformed'),
    );
    expect(observedMalformedReread).not.toBe(malformedRereadLoss);
    expect(store.getCalls - getCalls).toBe(2);
    expect(store.putCalls - putCalls).toBe(1);

    provider.blockScans = true;
    let blocked = await advanceBackendSwitchDecommission(
      boundedOptions(store, provider, { kind: 'start' }),
    );
    for (
      let index = 0;
      blocked.status === 'pending' && index < 32;
      index += 1
    ) {
      blocked = await advanceBackendSwitchDecommission(
        boundedOptions(store, provider, {
          kind: 'continue',
          token: blocked.token,
        }),
      );
    }
    expect(blocked.status).toBe('blocked');
    let blockedCapabilityReads = 0;
    Object.defineProperty(provider, 'getSwitchDatabase', {
      configurable: true,
      get() {
        blockedCapabilityReads += 1;
        return BoundedFakeSwitchProvider.prototype.getSwitchDatabase;
      },
    });
    const repeatedBlocked = await advanceBackendSwitchDecommission(
      boundedOptions(store, provider, { kind: 'start' }),
    );
    expect(repeatedBlocked.status).toBe('blocked');
    expect(blockedCapabilityReads).toBe(1);

    Object.defineProperty(provider, 'getSwitchDatabase', {
      configurable: true,
      value: BoundedFakeSwitchProvider.prototype.getSwitchDatabase,
    });
    if (blocked.status !== 'blocked') throw new Error('missing blocked token');
    const wrongRestartTarget: DeploymentSpec = {
      ...targetSpec,
      modules: [
        { name: 'worker.js', content: 'export default { changed: true }' },
      ],
    };
    getCalls = store.getCalls;
    putCalls = store.putCalls;
    const providerCalls = provider.calls.length;
    await expect(
      advanceBackendSwitchDecommission(
        boundedOptions(
          store,
          provider,
          { kind: 'restart-blocked', token: blocked.token },
          wrongRestartTarget,
        ),
      ),
    ).rejects.toThrow(
      'backend switch target decommission spec differs from durable intent',
    );
    expect(store.getCalls - getCalls).toBe(1);
    expect(store.putCalls - putCalls).toBe(0);
    expect(provider.calls).toHaveLength(providerCalls);
  });

  it('binds one immutable switch snapshot and captured entry subphase', async () => {
    const store = new MemorySwitchStore();
    const provider = new BoundedFakeSwitchProvider();
    const switched = await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    bindBoundedDatabase(store, provider);

    const started = await advanceBackendSwitchDecommission(
      boundedOptions(store, provider, { kind: 'start' }),
    );
    if (started.status !== 'pending') throw new Error('missing start token');
    const intent = store.intent;
    const snapshot = intent?.decommissionSnapshot;
    if (!intent || !snapshot) throw new Error('snapshot was not persisted');
    expect(intent.decommissionEntrySubphase).toBe(switched.subphase);
    expect(intent.decommissionSnapshotSha256).toBe(
      backendSwitchDecommissionSnapshotDigest(snapshot),
    );
    expect(snapshot).toMatchObject({
      prior: { ...switched.prior, databaseId: BOUNDED_DATABASE_ID },
      restoredArtifactVersion: null,
      entryPendingArtifactVersion: null,
      entryPendingNamespaceIds: null,
      providerTargetSpecDigest: switched.targetSpecDigest,
    });
    const routeAuthorityCase = async (
      active: boolean,
      mutation: 'outer-only' | 'snapshot-only' | 'outer-and-shell',
    ) => {
      const routeStore = new MemorySwitchStore();
      const routeProvider = new BoundedFakeSwitchProvider();
      await switchPlainDeploymentToWorkersForPlatforms(
        switchOptions(routeStore, routeProvider),
      );
      bindBoundedDatabase(routeStore, routeProvider);
      if (active) {
        await advanceBackendSwitchDecommission(
          boundedOptions(routeStore, routeProvider, { kind: 'start' }),
        );
      }
      const routeIntent = routeStore.intent;
      if (!routeIntent) throw new Error('route switch intent is missing');
      const foreignRoute = 'foreign.example.test';
      if (mutation === 'outer-only') {
        routeStore.record = {
          ...routeStore.record,
          routeHostname: foreignRoute,
        };
      } else {
        const routeSnapshot = routeIntent.decommissionSnapshot;
        const routeShell = routeStore.record.decommissionIntent;
        if (
          !routeSnapshot ||
          !routeShell ||
          routeShell.state === 'complete' ||
          routeShell.identity.mode.kind !== 'backend-switch'
        ) {
          throw new Error('active route authority is missing');
        }
        const changedSnapshot = {
          ...routeSnapshot,
          routeHostname: foreignRoute,
        };
        const changedSnapshotSha256 =
          backendSwitchDecommissionSnapshotDigest(changedSnapshot);
        const changedIntent = {
          ...routeIntent,
          decommissionSnapshot: changedSnapshot,
          decommissionSnapshotSha256: changedSnapshotSha256,
        };
        routeStore.record = {
          ...routeStore.record,
          ...(mutation === 'outer-and-shell'
            ? { routeHostname: foreignRoute }
            : {}),
          backendSwitchIntent: changedIntent,
          decommissionIntent: {
            ...routeShell,
            identity: {
              ...routeShell.identity,
              record: {
                ...routeShell.identity.record,
                ...(mutation === 'outer-and-shell'
                  ? { routeHostname: foreignRoute }
                  : {}),
              },
              mode: {
                ...routeShell.identity.mode,
                decommissionSnapshotSha256: changedSnapshotSha256,
              },
            },
          },
        };
      }
      routeProvider.calls.length = 0;
      const beforePutCalls = routeStore.putCalls;
      await expect(
        advanceBackendSwitchDecommission(
          boundedOptions(routeStore, routeProvider, { kind: 'start' }),
        ),
        `${active ? 'active' : 'shellless'} ${mutation}`,
      ).rejects.toThrow('backend switch decommission record is malformed');
      expect(routeStore.putCalls - beforePutCalls).toBe(0);
      expect(routeProvider.calls).toEqual([]);
    };
    await routeAuthorityCase(false, 'outer-only');
    await routeAuthorityCase(true, 'outer-only');
    await routeAuthorityCase(true, 'snapshot-only');
    await routeAuthorityCase(true, 'outer-and-shell');
    const changedRoutePrior = {
      ...priorSpec,
      routeHostname: 'foreign.example.test',
    } satisfies DeploymentSpec;
    const changedRouteTarget = {
      ...targetSpec,
      routeHostname: 'foreign.example.test',
    } satisfies DeploymentSpec;
    const beforeRouteSpecPutCalls = store.putCalls;
    const beforeRouteSpecProviderCalls = provider.calls.length;
    await expect(
      advanceBackendSwitchDecommission(
        boundedOptions(
          store,
          provider,
          { kind: 'start' },
          changedRouteTarget,
          changedRoutePrior,
        ),
      ),
    ).rejects.toThrow(
      'backend switch prior decommission spec differs from durable intent',
    );
    expect(store.putCalls - beforeRouteSpecPutCalls).toBe(0);
    expect(provider.calls).toHaveLength(beforeRouteSpecProviderCalls);
    const legacyRouteStore = new MemorySwitchStore();
    const legacyRouteProvider = new FakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(legacyRouteStore, legacyRouteProvider),
    );
    legacyRouteProvider.calls.length = 0;
    const legacyRoutePutCalls = legacyRouteStore.putCalls;
    await expect(
      decommissionBackendSwitch({
        store: legacyRouteStore,
        provider: legacyRouteProvider,
        priorSpec: changedRoutePrior,
        targetSpec: changedRouteTarget,
      }),
    ).rejects.toThrow(
      'backend switch prior decommission spec differs from durable intent',
    );
    expect(legacyRouteStore.putCalls - legacyRoutePutCalls).toBe(0);
    expect(legacyRouteProvider.calls).toEqual([]);
    const mutableProgress = {
      ...snapshot,
      releases: snapshot.releases.map((entry) => ({
        ...entry,
        subphase: 'deleted' as const,
      })),
    };
    expect(backendSwitchDecommissionSnapshotDigest(mutableProgress)).toBe(
      intent.decommissionSnapshotSha256,
    );

    for (const [label, authority] of [
      ['empty', ''],
      ['overbound UTF-8', 'é'.repeat(2_049)],
    ] as const) {
      const malformedStore = new MemorySwitchStore();
      const malformedProvider = new BoundedFakeSwitchProvider();
      await switchPlainDeploymentToWorkersForPlatforms(
        switchOptions(malformedStore, malformedProvider),
      );
      bindBoundedDatabase(malformedStore, malformedProvider);
      Object.defineProperty(
        malformedProvider,
        'databaseExportReceiptAuthority',
        { configurable: true, value: authority },
      );
      await expect(
        advanceBackendSwitchDecommission(
          boundedOptions(malformedStore, malformedProvider, { kind: 'start' }),
        ),
        label,
      ).rejects.toThrow('database export receipt capability is malformed');
      expect(malformedStore.record.decommissionIntent, label).toBeUndefined();
    }

    const wrongSpec = {
      ...targetSpec,
      modules: [
        { name: 'worker.js', content: 'export default { changed: true }' },
      ],
    } satisfies DeploymentSpec;
    const wrongPriorSpec = {
      ...priorSpec,
      modules: [
        { name: 'worker.js', content: 'export default { changed: true }' },
      ],
    } satisfies DeploymentSpec;
    const expectAuthorityRefusal = async (input: {
      label: string;
      selectedStore: MemorySwitchStore;
      selectedProvider: BoundedFakeSwitchProvider;
      action: import('../src/decommission-advance.js').DecommissionAdvanceAction;
      selectedPrior?: DeploymentSpec;
      selectedTarget?: DeploymentSpec;
      selectedCurrent?: DeploymentSpec;
      message: string;
    }) => {
      const beforeGetCalls = input.selectedStore.getCalls;
      const beforePutCalls = input.selectedStore.putCalls;
      const beforeGeneration =
        input.selectedStore.record.decommissionIntent?.generation;
      const beforeProvider = {
        calls: [...input.selectedProvider.calls],
        scanCalls: input.selectedProvider.scanCalls,
        receiptCalls: input.selectedProvider.receiptCalls,
        boundedDeleteCalls: input.selectedProvider.boundedDeleteCalls,
      };
      let clockCalls = 0;
      let uuidCalls = 0;
      await expect(
        advanceBackendSwitchDecommission({
          ...boundedOptions(
            input.selectedStore,
            input.selectedProvider,
            input.action,
            input.selectedTarget ?? targetSpec,
            input.selectedPrior ?? priorSpec,
          ),
          ...(input.selectedCurrent === undefined
            ? {}
            : { currentSpec: input.selectedCurrent }),
          clock: () => {
            clockCalls += 1;
            return Date.parse('2026-08-13T00:00:00.000Z');
          },
          randomUUID: () => {
            uuidCalls += 1;
            return '123e4567-e89b-42d3-a456-426614174000';
          },
        }),
        input.label,
      ).rejects.toThrow(input.message);
      expect(input.selectedStore.getCalls - beforeGetCalls, input.label).toBe(
        1,
      );
      expect(input.selectedStore.putCalls - beforePutCalls, input.label).toBe(
        0,
      );
      expect(
        input.selectedStore.record.decommissionIntent?.generation,
        input.label,
      ).toBe(beforeGeneration);
      expect(
        {
          calls: input.selectedProvider.calls,
          scanCalls: input.selectedProvider.scanCalls,
          receiptCalls: input.selectedProvider.receiptCalls,
          boundedDeleteCalls: input.selectedProvider.boundedDeleteCalls,
        },
        input.label,
      ).toEqual(beforeProvider);
      expect(clockCalls, input.label).toBe(0);
      expect(uuidCalls, input.label).toBe(0);
    };
    for (const [
      label,
      action,
      selectedPrior,
      selectedTarget,
      selectedCurrent,
      message,
    ] of [
      [
        'active start prior',
        { kind: 'start' as const },
        wrongPriorSpec,
        targetSpec,
        targetSpec,
        'backend switch prior decommission spec differs from durable intent',
      ],
      [
        'active start target',
        { kind: 'start' as const },
        priorSpec,
        wrongSpec,
        targetSpec,
        'backend switch target decommission spec differs from durable intent',
      ],
      [
        'active start current',
        { kind: 'start' as const },
        priorSpec,
        targetSpec,
        wrongSpec,
        'backend switch current decommission spec differs from durable intent',
      ],
      [
        'current work prior',
        { kind: 'continue' as const, token: started.token },
        wrongPriorSpec,
        targetSpec,
        targetSpec,
        'backend switch prior decommission spec differs from durable intent',
      ],
      [
        'current work target',
        { kind: 'continue' as const, token: started.token },
        priorSpec,
        wrongSpec,
        targetSpec,
        'backend switch target decommission spec differs from durable intent',
      ],
      [
        'current work current',
        { kind: 'continue' as const, token: started.token },
        priorSpec,
        targetSpec,
        wrongSpec,
        'backend switch current decommission spec differs from durable intent',
      ],
    ] as const) {
      await expectAuthorityRefusal({
        label,
        selectedStore: store,
        selectedProvider: provider,
        action,
        selectedPrior,
        selectedTarget,
        selectedCurrent,
        message,
      });
    }

    const priorDesiredStore = new MemorySwitchStore();
    const priorDesiredProvider = new BoundedFakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(priorDesiredStore, priorDesiredProvider),
    );
    bindBoundedDatabase(priorDesiredStore, priorDesiredProvider);
    priorDesiredStore.record = {
      ...priorDesiredStore.record,
      desiredSpecDigest: deploymentSpecDigest(priorSpec),
    };
    const {
      currentSpec: _shelllessCurrentSpec,
      ...shelllessPriorDesiredOptions
    } = boundedOptions(priorDesiredStore, priorDesiredProvider, {
      kind: 'start',
    });
    const priorDesiredStarted = await advanceBackendSwitchDecommission(
      shelllessPriorDesiredOptions,
    );
    expect(priorDesiredStarted.status).toBe('pending');
    const { currentSpec: _activeCurrentSpec, ...activePriorDesiredOptions } =
      boundedOptions(priorDesiredStore, priorDesiredProvider, {
        kind: 'start',
      });
    await expect(
      advanceBackendSwitchDecommission(activePriorDesiredOptions),
    ).resolves.toMatchObject({ status: 'pending' });

    const missingCurrentStore = new MemorySwitchStore();
    const missingCurrentProvider = new BoundedFakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(missingCurrentStore, missingCurrentProvider),
    );
    bindBoundedDatabase(missingCurrentStore, missingCurrentProvider);
    missingCurrentStore.record = {
      ...missingCurrentStore.record,
      desiredSpecDigest: 'e'.repeat(64),
    };
    const { currentSpec: _missingCurrentSpec, ...missingCurrentOptions } =
      boundedOptions(missingCurrentStore, missingCurrentProvider, {
        kind: 'start',
      });
    const missingCurrentPutCalls = missingCurrentStore.putCalls;
    await expect(
      advanceBackendSwitchDecommission(missingCurrentOptions),
    ).rejects.toThrow(
      'backend switch decommission requires the exact current specification',
    );
    expect(missingCurrentStore.putCalls - missingCurrentPutCalls).toBe(0);

    const blockedAuthorityStore = new MemorySwitchStore();
    const blockedAuthorityProvider = new BoundedFakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(blockedAuthorityStore, blockedAuthorityProvider),
    );
    bindBoundedDatabase(blockedAuthorityStore, blockedAuthorityProvider);
    blockedAuthorityProvider.blockScans = true;
    let blockedAuthority = await advanceBackendSwitchDecommission(
      boundedOptions(blockedAuthorityStore, blockedAuthorityProvider, {
        kind: 'start',
      }),
    );
    for (
      let index = 0;
      blockedAuthority.status === 'pending' && index < 32;
      index += 1
    ) {
      blockedAuthority = await advanceBackendSwitchDecommission(
        boundedOptions(blockedAuthorityStore, blockedAuthorityProvider, {
          kind: 'continue',
          token: blockedAuthority.token,
        }),
      );
    }
    if (blockedAuthority.status !== 'blocked') {
      throw new Error('authority fixture did not block');
    }
    for (const [
      label,
      action,
      selectedPrior,
      selectedTarget,
      selectedCurrent,
      message,
    ] of [
      [
        'blocked start prior',
        { kind: 'start' as const },
        wrongPriorSpec,
        targetSpec,
        targetSpec,
        'backend switch prior decommission spec differs from durable intent',
      ],
      [
        'blocked start target',
        { kind: 'start' as const },
        priorSpec,
        wrongSpec,
        targetSpec,
        'backend switch target decommission spec differs from durable intent',
      ],
      [
        'blocked start current',
        { kind: 'start' as const },
        priorSpec,
        targetSpec,
        wrongSpec,
        'backend switch current decommission spec differs from durable intent',
      ],
      [
        'restart blocked prior',
        { kind: 'restart-blocked' as const, token: blockedAuthority.token },
        wrongPriorSpec,
        targetSpec,
        targetSpec,
        'backend switch prior decommission spec differs from durable intent',
      ],
      [
        'restart blocked target',
        { kind: 'restart-blocked' as const, token: blockedAuthority.token },
        priorSpec,
        wrongSpec,
        targetSpec,
        'backend switch target decommission spec differs from durable intent',
      ],
      [
        'restart blocked current',
        { kind: 'restart-blocked' as const, token: blockedAuthority.token },
        priorSpec,
        targetSpec,
        wrongSpec,
        'backend switch current decommission spec differs from durable intent',
      ],
    ] as const) {
      await expectAuthorityRefusal({
        label,
        selectedStore: blockedAuthorityStore,
        selectedProvider: blockedAuthorityProvider,
        action,
        selectedPrior,
        selectedTarget,
        selectedCurrent,
        message,
      });
    }

    const receiptStore = new MemorySwitchStore();
    const receiptProvider = new BoundedFakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(receiptStore, receiptProvider),
    );
    bindBoundedDatabase(receiptStore, receiptProvider);
    let receiptResult = await advanceBackendSwitchDecommission(
      boundedOptions(receiptStore, receiptProvider, { kind: 'start' }),
    );
    for (
      let index = 0;
      receiptStore.record.decommissionIntent?.databaseExportReceiptAuthority ===
        undefined &&
      receiptResult.status === 'pending' &&
      index < 32;
      index += 1
    ) {
      receiptResult = await advanceBackendSwitchDecommission(
        boundedOptions(receiptStore, receiptProvider, {
          kind: 'continue',
          token: receiptResult.token,
        }),
      );
    }
    expect(
      receiptStore.record.decommissionIntent?.databaseExportReceiptAuthority,
    ).toBe('test-receipts');
    Object.defineProperty(receiptProvider, 'databaseExportReceiptAuthority', {
      configurable: true,
      value: 'changed-receipts',
    });
    await expectAuthorityRefusal({
      label: 'active start selected receipt',
      selectedStore: receiptStore,
      selectedProvider: receiptProvider,
      action: { kind: 'start' },
      selectedCurrent: targetSpec,
      message:
        'database export receipt authority differs from configured authority',
    });
    if (receiptResult.status !== 'pending') {
      throw new Error('receipt authority fixture has no current token');
    }
    await expectAuthorityRefusal({
      label: 'current work selected receipt',
      selectedStore: receiptStore,
      selectedProvider: receiptProvider,
      action: { kind: 'continue', token: receiptResult.token },
      selectedCurrent: targetSpec,
      message:
        'database export receipt authority differs from configured authority',
    });
    Object.defineProperty(receiptProvider, 'databaseExportReceiptAuthority', {
      configurable: true,
      value: 'test-receipts',
    });
    receiptProvider.blockScans = true;
    const receiptBlocked = await advanceBackendSwitchDecommission(
      boundedOptions(receiptStore, receiptProvider, {
        kind: 'continue',
        token: receiptResult.token,
      }),
    );
    if (receiptBlocked.status !== 'blocked') {
      throw new Error('selected receipt authority fixture did not block');
    }
    Object.defineProperty(receiptProvider, 'databaseExportReceiptAuthority', {
      configurable: true,
      value: 'changed-receipts',
    });
    for (const [label, action] of [
      ['blocked start selected receipt', { kind: 'start' as const }],
      [
        'restart blocked selected receipt',
        { kind: 'restart-blocked' as const, token: receiptBlocked.token },
      ],
    ] as const) {
      await expectAuthorityRefusal({
        label,
        selectedStore: receiptStore,
        selectedProvider: receiptProvider,
        action,
        selectedCurrent: targetSpec,
        message:
          'database export receipt authority differs from configured authority',
      });
    }

    const pendingCurrentSpec = {
      ...targetSpec,
      authoredBy: 'platform' as const,
      durableObjectMigrations: [{ tag: 'v1', newClasses: ['Alpha', 'Runner'] }],
      durableObjectBindings: [
        { name: 'ALPHA', className: 'Alpha' },
        { name: 'RUNNER', className: 'Runner' },
      ],
      egressProxyService: 'egress-service',
      queueProducer: { binding: 'EVENTS', queueName: 'events' },
      application: {
        vars: [{ name: 'APP_VAR', value: 'application-value' }],
        secrets: [{ name: 'APP_SECRET', valueSha256: 'c'.repeat(64) }],
        r2Buckets: [],
      },
    } satisfies DeploymentSpec;
    const pendingInspection = {
      application: applicationBindingTopology(pendingCurrentSpec, []),
      artifactVersion: 'pending-v2',
      databaseIds: [BOUNDED_DATABASE_ID],
      durableObjectBindings: [
        {
          name: 'ALPHA',
          className: 'Alpha',
          namespaceId: 'namespace-z',
        },
        {
          name: 'RUNNER',
          className: 'Runner',
          namespaceId: 'namespace-a',
        },
      ],
      queueProducerBindings: [{ name: 'EVENTS', queueName: 'events' }],
      secretNames: [
        'APP_SECRET',
        'DEPLOYMENT_IDENTITY_SECRET',
        'MAINTENANCE_ADMIN_SECRET',
      ],
      serviceBindings: [{ name: 'EGRESS_PROXY', service: 'egress-service' }],
      specDigest: deploymentSpecDigest(pendingCurrentSpec),
    };
    const pendingEntry = async (
      inspection: unknown,
      recordApplication = applicationBindingTopology(pendingCurrentSpec, []),
    ) => {
      const pendingStore = new MemorySwitchStore();
      const pendingProvider = new BoundedFakeSwitchProvider();
      await switchPlainDeploymentToWorkersForPlatforms(
        switchOptions(pendingStore, pendingProvider),
      );
      bindBoundedDatabase(pendingStore, pendingProvider);
      const switchIntent = pendingStore.intent;
      if (!switchIntent) throw new Error('switch intent is missing');
      pendingStore.record = {
        tenantTag: priorSpec.tenantTag,
        environment: priorSpec.environment,
        backend: 'plain-worker',
        scriptName: priorSpec.scriptName,
        databaseId: BOUNDED_DATABASE_ID,
        databaseName: priorSpec.databaseName,
        schemaVersion: priorSpec.schemaVersion,
        artifactVersion: 'plain-v1',
        desiredSpecDigest: deploymentSpecDigest(targetSpec),
        pendingSpecDigest: deploymentSpecDigest(pendingCurrentSpec),
        pendingArtifactVersion: 'pending-v2',
        applicationBindings: recordApplication,
        durableObjectBindings: [],
        routeHostname: priorSpec.routeHostname,
        phase: 'migrating',
        updatedAt: pendingStore.record.updatedAt,
        backendSwitchIntent: switchIntent,
      };
      Object.defineProperty(
        pendingProvider,
        'captureSwitchEntryPendingArtifact',
        {
          configurable: true,
          value: async () => inspection,
        },
      );
      return { pendingStore, pendingProvider };
    };
    const validPending = await pendingEntry(pendingInspection);
    await expect(
      advanceBackendSwitchDecommission({
        ...boundedOptions(
          validPending.pendingStore,
          validPending.pendingProvider,
          { kind: 'start' },
        ),
        currentSpec: pendingCurrentSpec,
      }),
    ).resolves.toMatchObject({ status: 'pending' });
    expect(
      validPending.pendingStore.intent?.decommissionSnapshot
        ?.entryPendingNamespaceIds,
    ).toEqual(['namespace-a', 'namespace-z']);

    let hostileGetterCalls = 0;
    let hostileProxyGetCalls = 0;
    const withoutApplication = { ...pendingInspection } as Partial<
      typeof pendingInspection
    >;
    Reflect.deleteProperty(withoutApplication, 'application');
    const accessorInspection = { ...pendingInspection };
    Object.defineProperty(accessorInspection, 'artifactVersion', {
      configurable: true,
      enumerable: true,
      get() {
        hostileGetterCalls += 1;
        return 'pending-v2';
      },
    });
    const cyclicInspection = structuredClone(
      pendingInspection,
    ) as unknown as Record<string, unknown>;
    cyclicInspection.application = cyclicInspection;
    let deepApplication: unknown = { value: 'leaf' };
    for (let depth = 0; depth < 70; depth += 1) {
      deepApplication = { value: deepApplication };
    }
    const oversizedPendingArtifactVersion = 'x'.repeat(4 * 1024 * 1024 + 1);
    const pendingCases: readonly Readonly<{
      label: string;
      inspection: unknown;
      recordApplication?: import('../src/types.js').ApplicationBindingTopology;
    }>[] = [
      { label: 'missing top-level key', inspection: withoutApplication },
      {
        label: 'extra top-level key',
        inspection: { ...pendingInspection, extra: true },
      },
      {
        label: 'symbol top-level key',
        inspection: { ...pendingInspection, [Symbol('extra')]: true },
      },
      {
        label: 'artifact version',
        inspection: { ...pendingInspection, artifactVersion: 'foreign-v2' },
      },
      {
        label: 'spec digest',
        inspection: { ...pendingInspection, specDigest: 'f'.repeat(64) },
      },
      {
        label: 'database identifier',
        inspection: { ...pendingInspection, databaseIds: ['foreign-db'] },
      },
      {
        label: 'extra database identifier',
        inspection: {
          ...pendingInspection,
          databaseIds: [BOUNDED_DATABASE_ID, 'foreign-db'],
        },
      },
      {
        label: 'durable binding name',
        inspection: {
          ...pendingInspection,
          durableObjectBindings: pendingInspection.durableObjectBindings.map(
            (binding, index) =>
              index === 0 ? { ...binding, name: 'FOREIGN' } : binding,
          ),
        },
      },
      {
        label: 'durable binding class',
        inspection: {
          ...pendingInspection,
          durableObjectBindings: pendingInspection.durableObjectBindings.map(
            (binding, index) =>
              index === 0 ? { ...binding, className: 'Foreign' } : binding,
          ),
        },
      },
      {
        label: 'durable binding script selector',
        inspection: {
          ...pendingInspection,
          durableObjectBindings: pendingInspection.durableObjectBindings.map(
            (binding, index) =>
              index === 0
                ? { ...binding, scriptName: 'foreign-worker' }
                : binding,
          ),
        },
      },
      {
        label: 'durable binding dispatch selector',
        inspection: {
          ...pendingInspection,
          durableObjectBindings: pendingInspection.durableObjectBindings.map(
            (binding, index) =>
              index === 0
                ? { ...binding, dispatchNamespace: 'foreign-dispatch' }
                : binding,
          ),
        },
      },
      {
        label: 'empty namespace identifier',
        inspection: {
          ...pendingInspection,
          durableObjectBindings: pendingInspection.durableObjectBindings.map(
            (binding, index) =>
              index === 0 ? { ...binding, namespaceId: '' } : binding,
          ),
        },
      },
      {
        label: 'duplicate namespace identifier',
        inspection: {
          ...pendingInspection,
          durableObjectBindings: pendingInspection.durableObjectBindings.map(
            (binding) => ({ ...binding, namespaceId: 'namespace-a' }),
          ),
        },
      },
      {
        label: 'noncanonical binding-name order',
        inspection: {
          ...pendingInspection,
          durableObjectBindings: [
            ...pendingInspection.durableObjectBindings,
          ].reverse(),
        },
      },
      {
        label: 'service binding name',
        inspection: {
          ...pendingInspection,
          serviceBindings: [{ name: 'FOREIGN', service: 'egress-service' }],
        },
      },
      {
        label: 'service binding service',
        inspection: {
          ...pendingInspection,
          serviceBindings: [
            { name: 'EGRESS_PROXY', service: 'foreign-service' },
          ],
        },
      },
      {
        label: 'service binding entrypoint',
        inspection: {
          ...pendingInspection,
          serviceBindings: [
            {
              name: 'EGRESS_PROXY',
              service: 'egress-service',
              entrypoint: 'ForeignEntrypoint',
            },
          ],
        },
      },
      {
        label: 'service binding extra key',
        inspection: {
          ...pendingInspection,
          serviceBindings: [
            { name: 'EGRESS_PROXY', service: 'egress-service', extra: true },
          ],
        },
      },
      {
        label: 'queue binding name',
        inspection: {
          ...pendingInspection,
          queueProducerBindings: [{ name: 'FOREIGN', queueName: 'events' }],
        },
      },
      {
        label: 'queue binding queue',
        inspection: {
          ...pendingInspection,
          queueProducerBindings: [
            { name: 'EVENTS', queueName: 'foreign-events' },
          ],
        },
      },
      {
        label: 'queue binding extra key',
        inspection: {
          ...pendingInspection,
          queueProducerBindings: [
            { name: 'EVENTS', queueName: 'events', extra: true },
          ],
        },
      },
      {
        label: 'missing secret',
        inspection: {
          ...pendingInspection,
          secretNames: pendingInspection.secretNames.slice(1),
        },
      },
      {
        label: 'noncanonical secret order',
        inspection: {
          ...pendingInspection,
          secretNames: [...pendingInspection.secretNames].reverse(),
        },
      },
      {
        label: 'extra secret',
        inspection: {
          ...pendingInspection,
          secretNames: [...pendingInspection.secretNames, 'FOREIGN_SECRET'],
        },
      },
      {
        label: 'application topology',
        inspection: {
          ...pendingInspection,
          application: {
            ...pendingInspection.application,
            vars: [{ name: 'APP_VAR', value: 'changed' }],
          },
        },
      },
      {
        label: 'record application topology',
        inspection: pendingInspection,
        recordApplication: {
          ...pendingInspection.application,
          vars: [{ name: 'APP_VAR', value: 'changed' }],
        },
      },
      { label: 'hostile accessor', inspection: accessorInspection },
      {
        label: 'hostile transparent proxy',
        inspection: new Proxy(structuredClone(pendingInspection), {
          get(target, key, receiver) {
            hostileProxyGetCalls += 1;
            return Reflect.get(target, key, receiver);
          },
        }),
      },
      { label: 'hostile cycle', inspection: cyclicInspection },
      {
        label: 'depth bound',
        inspection: { ...pendingInspection, application: deepApplication },
      },
      {
        label: 'node bound',
        inspection: {
          ...pendingInspection,
          application: Array.from({ length: 65_537 }, () => null),
        },
      },
      {
        label: 'scalar bound',
        inspection: {
          ...pendingInspection,
          artifactVersion: oversizedPendingArtifactVersion,
        },
      },
      {
        label: 'serialized bound',
        inspection: {
          ...pendingInspection,
          application: Array.from({ length: 60_000 }, () => 'x'.repeat(67)),
        },
      },
    ];
    for (const { label, inspection, recordApplication } of pendingCases) {
      const malformedPending = await pendingEntry(
        inspection,
        recordApplication,
      );
      const beforePutCalls = malformedPending.pendingStore.putCalls;
      const beforeRecord = structuredClone(
        malformedPending.pendingStore.record,
      );
      const beforeProvider = {
        calls: [...malformedPending.pendingProvider.calls],
        scanCalls: malformedPending.pendingProvider.scanCalls,
        receiptCalls: malformedPending.pendingProvider.receiptCalls,
        boundedDeleteCalls: malformedPending.pendingProvider.boundedDeleteCalls,
      };
      let clockCalls = 0;
      let uuidCalls = 0;
      const encode =
        label === 'scalar bound'
          ? vi.spyOn(TextEncoder.prototype, 'encode')
          : undefined;
      try {
        await expect(
          advanceBackendSwitchDecommission({
            ...boundedOptions(
              malformedPending.pendingStore,
              malformedPending.pendingProvider,
              { kind: 'start' },
            ),
            currentSpec: pendingCurrentSpec,
            clock: () => {
              clockCalls += 1;
              return Date.parse('2026-08-13T00:00:00.000Z');
            },
            randomUUID: () => {
              uuidCalls += 1;
              return '123e4567-e89b-42d3-a456-426614174000';
            },
          }),
          label,
        ).rejects.toThrow(
          'backend switch pending artifact inspection is malformed',
        );
        if (encode) {
          const serializedOversizedScalar = JSON.stringify(
            oversizedPendingArtifactVersion,
          );
          expect(
            encode.mock.calls.some(
              ([value]) => value === serializedOversizedScalar,
            ),
            label,
          ).toBe(false);
        }
      } finally {
        encode?.mockRestore();
      }
      expect(
        malformedPending.pendingStore.putCalls - beforePutCalls,
        label,
      ).toBe(0);
      expect(malformedPending.pendingStore.record, label).toEqual(beforeRecord);
      expect(
        {
          calls: malformedPending.pendingProvider.calls,
          scanCalls: malformedPending.pendingProvider.scanCalls,
          receiptCalls: malformedPending.pendingProvider.receiptCalls,
          boundedDeleteCalls:
            malformedPending.pendingProvider.boundedDeleteCalls,
        },
        label,
      ).toEqual(beforeProvider);
      expect(clockCalls, label).toBe(0);
      expect(uuidCalls, label).toBe(0);
    }
    expect(hostileGetterCalls).toBe(0);
    expect(hostileProxyGetCalls).toBe(1);
  });

  it('resumes one release or bridge action after provider and Fleet response loss', async () => {
    const store = new MemorySwitchStore();
    const provider = new BoundedFakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    bindBoundedDatabase(store, provider);
    let result = await advanceBackendSwitchDecommission(
      boundedOptions(store, provider, { kind: 'start' }),
    );
    if (result.status !== 'pending') throw new Error('missing start token');
    result = await advanceBackendSwitchDecommission(
      boundedOptions(store, provider, {
        kind: 'continue',
        token: result.token,
      }),
    );
    if (result.status !== 'pending') throw new Error('missing traffic token');
    provider.failReleaseDeleteResponseOnce = true;
    await expect(
      advanceBackendSwitchDecommission(
        boundedOptions(store, provider, {
          kind: 'continue',
          token: result.token,
        }),
      ),
    ).rejects.toThrow('release delete response lost');
    expect(store.intent?.decommissionSnapshot?.releases[0]?.subphase).toBe(
      'delete-authorized',
    );

    const completed = await decommissionBackendSwitch({
      store,
      provider,
      priorSpec,
      targetSpec,
      currentSpec: targetSpec,
    });
    expect(completed.subphase).toBe('decommissioned');
    expect(provider.releaseDeleteMutations.get('acme-candidate')).toBe(1);
  });

  it('consumes matching R2 verification one resource at a time', async () => {
    const application = {
      vars: [],
      secrets: [],
      r2Buckets: [{ name: 'FILES', jurisdiction: 'default' as const }],
    };
    const selectedPriorSpec: DeploymentSpec = {
      ...priorSpec,
      application,
    };
    const selectedTargetSpec: DeploymentSpec = {
      ...targetSpec,
      application,
    };
    const resource = switchApplicationR2Resource(
      'FILES',
      'default',
      '2026-08-11T00:00:00.000Z',
    );
    const store = new MemorySwitchStore();
    const provider = new BoundedFakeSwitchProvider();
    store.record = {
      ...store.record,
      desiredSpecDigest: deploymentSpecDigest(selectedPriorSpec),
      applicationResources: [resource],
      applicationBindings: applicationBindingTopology(selectedPriorSpec, [
        resource,
      ]),
    };
    provider.applicationResources = [resource];
    provider.r2Buckets.set(resource.bucketName, resource);
    await switchPlainDeploymentToWorkersForPlatforms({
      ...switchOptions(store, provider),
      priorSpec: selectedPriorSpec,
      targetSpec: selectedTargetSpec,
    });
    bindBoundedDatabase(store, provider);
    store.record = {
      ...store.record,
      applicationResources: [resource],
      applicationBindings: applicationBindingTopology(selectedTargetSpec, [
        resource,
      ]),
    };

    const completed = await driveBoundedSwitch(
      store,
      provider,
      selectedTargetSpec,
      selectedPriorSpec,
    );
    expect(completed.result.record.applicationResources).toMatchObject([
      { name: 'FILES', state: 'deleted' },
    ]);
    expect(provider.r2Buckets.size).toBe(0);
    expect(provider.scanCalls).toBe(6);
  });

  it('converges receipt export and fenced D1 deletion without adopting legacy export authorization', async () => {
    const store = new MemorySwitchStore();
    const provider = new BoundedFakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(store, provider),
    );
    bindBoundedDatabase(store, provider);

    const completed = await driveBoundedSwitch(store, provider);
    expect(completed.result).toMatchObject({
      record: { phase: 'decommissioned' },
      databaseExport: {
        location: 'r2://exports/acme-bounded.sql',
        sha256: '6'.repeat(64),
        size: 456,
      },
    });
    expect(provider.receiptCalls).toBe(1);
    expect(provider.boundedDeleteCalls).toBe(1);
    expect(provider.calls).not.toContain('decommission-export');
    expect(provider.calls).not.toContain('decommission-database');

    let terminalCapabilityReads = 0;
    Object.defineProperty(provider, 'advanceSwitchDecommissionAttachmentScan', {
      configurable: true,
      get() {
        terminalCapabilityReads += 1;
        throw new Error('terminal capability getter must stay inert');
      },
    });
    await expect(
      decommissionBackendSwitch({
        store,
        provider,
        priorSpec,
        targetSpec,
        currentSpec: targetSpec,
      }),
    ).resolves.toMatchObject({ subphase: 'decommissioned' });
    expect(terminalCapabilityReads).toBe(0);
    for (const [label, selectedPrior] of [
      [
        'completed prior digest',
        {
          ...priorSpec,
          modules: [
            { name: 'worker.js', content: 'export default { changed: true }' },
          ],
        },
      ],
      [
        'completed prior route',
        { ...priorSpec, routeHostname: 'foreign.example.test' },
      ],
    ] as const) {
      const beforePutCalls = store.putCalls;
      const beforeProviderCalls = [...provider.calls];
      const beforeRecord = structuredClone(store.record);
      await expect(
        decommissionBackendSwitch({
          store,
          provider,
          priorSpec: selectedPrior,
          targetSpec,
          currentSpec: targetSpec,
        }),
        label,
      ).rejects.toThrow(
        'backend switch prior decommission spec differs from durable intent',
      );
      expect(store.putCalls - beforePutCalls, label).toBe(0);
      expect(store.record, label).toEqual(beforeRecord);
      expect(provider.calls, label).toEqual(beforeProviderCalls);
      expect(terminalCapabilityReads, label).toBe(0);
    }

    const lateStore = new MemorySwitchStore();
    const lateProvider = new BoundedFakeSwitchProvider();
    await switchPlainDeploymentToWorkersForPlatforms(
      switchOptions(lateStore, lateProvider),
    );
    bindBoundedDatabase(lateStore, lateProvider);
    await advanceBackendSwitchDecommission(
      boundedOptions(lateStore, lateProvider, { kind: 'start' }),
    );
    const lateIntent = lateStore.intent as BackendSwitchIntent;
    const { decommissionIntent: _lateShell, ...lateRecord } = lateStore.record;
    lateStore.record = {
      ...lateRecord,
      phase: 'decommissioning',
      backendSwitchIntent: {
        ...lateIntent,
        subphase: 'decommission-export-authorized',
      },
    };
    let lateCapabilityReads = 0;
    Object.defineProperty(
      lateProvider,
      'advanceSwitchDecommissionAttachmentScan',
      {
        configurable: true,
        get() {
          lateCapabilityReads += 1;
          throw new Error('late compatibility must not read capabilities');
        },
      },
    );
    await expect(
      advanceBackendSwitchDecommission(
        boundedOptions(lateStore, lateProvider, { kind: 'start' }),
      ),
    ).rejects.toThrow(
      'bounded backend-switch decommission cannot adopt shell-less legacy D1 authorization',
    );
    expect(lateCapabilityReads).toBe(0);
    await expect(
      decommissionBackendSwitch({
        store: lateStore,
        provider: lateProvider,
        priorSpec,
        targetSpec,
        currentSpec: targetSpec,
      }),
    ).resolves.toMatchObject({ subphase: 'decommissioned' });
    expect(lateCapabilityReads).toBe(0);
    expect(lateProvider.calls).toContain('decommission-export');
    expect(lateProvider.calls).toContain('decommission-database');
  });
  it('refuses backend switch decommission shells during an active cleanup', () => {
    const record = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'workers-for-platforms',
      scriptName: 'acme-production',
      databaseId: '00000000-0000-0000-0000-000000000001',
      databaseName: 'acme-production',
      schemaVersion: 1,
      artifactVersion: 'artifact-v1',
      desiredSpecDigest: 'a'.repeat(64),
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase: 'cleanup-advancing',
      updatedAt: '2026-08-29T00:00:00.000Z',
      cleanupIntent: {
        version: 1,
        operationId: '12345678-1234-4abc-8def-1234567890ab',
        revision: 0,
        generation: 0,
        updatedAt: '2026-08-29T00:00:00.000Z',
        authority: { kind: 'manual-cleanup' },
        identity: {
          record: {
            tenantTag: 'acme',
            environment: 'production',
            backend: 'workers-for-platforms',
            scriptName: 'acme-production',
            databaseId: '00000000-0000-0000-0000-000000000001',
            databaseName: 'acme-production',
            routeHostname: 'acme.example.test',
          },
          admittedPhase: 'worker-deployed',
          externalArtifact: true,
        },
        state: { step: 'teardown-traffic' },
      },
    } as const satisfies import('../src/types.js').FleetRecord;
    expect(() =>
      backendSwitchDecommissionShell({
        record,
        intent: {} as never,
        operationId: '12345678-1234-4abc-8def-1234567890ab',
        snapshotSha256: 'a'.repeat(64),
        entrySubphase: 'decommission-database' as never,
        now: '2026-08-29T00:00:00.000Z',
      }),
    ).toThrow('cannot run during an active cleanup');
  });
});
