// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  type BackendSwitchIntent,
  type BackendSwitchProvider,
  type BridgeSnapshot,
  backendSwitchIntentFromUnknown,
  decommissionBackendSwitch,
  finalizeBackendSwitch,
  type PlainBackendSnapshot,
  reconcileFinalizedBackendSwitchState,
  rollbackBackendSwitch,
  switchPlainDeploymentToWorkersForPlatforms,
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

  get intent(): BackendSwitchIntent | undefined {
    return this.record.backendSwitchIntent;
  }

  async get(): Promise<FleetRecord> {
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
        this.record = structuredClone(record);
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

  async snapshotPlainDeployment(): Promise<PlainBackendSnapshot> {
    this.calls.push('snapshot');
    return {
      scriptName: priorSpec.scriptName,
      artifactVersion: 'plain-v1',
      specDigest: deploymentSpecDigest(priorSpec),
      databaseId: 'db-acme',
      databaseName: priorSpec.databaseName,
      durableObjectBindings: this.bridge.durableObjectBindings,
      namespaceIds: this.bridge.namespaceIds,
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
      applicationResources: this.applicationResources,
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

  async ensureCandidate() {
    this.calls.push('candidate');
    return {
      physicalScriptName: 'acme-candidate',
      specDigest: deploymentSpecDigest(targetSpec),
      artifactVersion: 'candidate-v1',
      releaseSchemaVersion: 1,
      application: { vars: [], secrets: [], r2Buckets: [] },
      topology: {
        durableObjectBindings: this.bridge.durableObjectBindings,
        serviceBindings: [],
        queueProducerBindings: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        application: { vars: [], secrets: [], r2Buckets: [] },
      },
      maintenance: {
        receipt: 'maintenance-receipt-v1',
        specDigest: deploymentSpecDigest(targetSpec),
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
      migrationIntent: undefined,
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
    store.record = {
      ...store.record,
      backendSwitchIntent: {
        ...completed,
        subphase,
        ...(subphase === 'bridge-upload-authorized'
          ? { bridge: undefined, candidate: undefined }
          : subphase === 'candidate-deploy-authorized'
            ? { candidate: undefined }
            : {}),
      },
    };
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
        ? { pendingRelease: undefined, migrationPriorRelease: undefined }
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
    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/release deletion write response lost/);
    expect(store.intent?.decommissionSnapshot?.releases[0]?.subphase).toBe(
      'deleted',
    );

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
      {
        name: 'ARCHIVE',
        bucketName: 'acme-archive',
        jurisdiction: 'eu',
        state: 'created',
        reservationNonce: 'nonce-archive',
        creationDate: '2026-08-11T00:00:01.000Z',
      },
      {
        name: 'FILES',
        bucketName: 'acme-files',
        jurisdiction: 'default',
        state: 'created',
        reservationNonce: 'nonce-files',
        creationDate: '2026-08-11T00:00:00.000Z',
      },
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
    const resource: ApplicationR2Resource = {
      name: 'FILES',
      bucketName: 'acme-files',
      jurisdiction: 'default',
      state: 'created',
      reservationNonce: 'nonce-files',
      creationDate: '2026-08-11T00:00:00.000Z',
    };
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
    const resource: ApplicationR2Resource = {
      name: 'FILES',
      bucketName: 'acme-files',
      jurisdiction: 'default',
      state: 'created',
      reservationNonce: 'nonce-files',
      creationDate: '2026-08-11T00:00:00.000Z',
    };
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
    ).rejects.toThrow(/traffic authorization write response lost/u);
    expect(store.intent?.subphase).toBe('decommission-traffic-authorized');
    expect(provider.calls).not.toContain('decommission-traffic');

    provider.failTrafficRemovalResponseOnce = true;
    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/switch traffic removal response lost/u);
    expect(store.intent?.subphase).toBe('decommission-traffic-authorized');
    expect(provider.switchTrafficRemoved).toBe(true);

    store.failAfterCommittedTrafficRemoval = true;
    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).rejects.toThrow(/traffic removal write response lost/u);
    expect(store.intent?.subphase).toBe('decommission-traffic-removed');
    expect(provider.deletedReleases.size).toBe(0);

    await expect(
      decommissionBackendSwitch({ store, provider, priorSpec, targetSpec }),
    ).resolves.toMatchObject({ subphase: 'decommissioned' });
    expect(
      provider.calls.filter((call) => call === 'decommission-traffic'),
    ).toHaveLength(2);
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
});
