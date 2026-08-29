// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  canonicalDeploymentEgressPolicy,
  deploymentEgressPolicyDigest,
  durableObjectMigrationHistoryDigest,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalReleaseTopology,
  externalRouteExpectations,
  externalStateScriptName,
  trustedArtifactDigest,
  validateExternalPlatformProfile,
} from '../src/platform-resources.js';
import type {
  DeploymentSpec,
  ExternalPlatformProfile,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
} from '../src/types.js';
import { decommissionAdvancingRecordFixture } from './fixtures/decommission-intent-fixture.js';

const spec: DeploymentSpec = {
  tenantTag: 'acme',
  environment: 'production',
  scriptName: 'acme-production-project-with-a-deliberately-long-logical-name',
  databaseName: 'acme-production',
  compatibilityDate: '2026-08-11',
  mainModule: 'candidate.js',
  modules: [{ name: 'candidate.js', content: 'export default {}' }],
  authoredBy: 'external',
  schemaVersion: 0,
  migrations: [],
  durableObjectMigrations: [],
  durableObjectBindings: [{ name: 'MAINTENANCE', className: 'Maintenance' }],
  maintenanceBaseUrl: 'https://control.example.test',
  routeHostname: 'acme.example.test',
};

const profile: ExternalPlatformProfile = {
  runtimeContractVersion: 1,
  backwardCompatibleWithRetainedReleases: true,
  maintenanceCapabilityPublicKey:
    '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
  maintenanceCapabilityPrivateKey: {
    kty: 'OKP',
    crv: 'Ed25519',
    alg: 'EdDSA',
    kid: 'fleet-maintenance-v1',
    x: 'Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo',
    d: 'gkXf8_b8kcCJxZ33fUYUac7yCsxZAxQXgsgPbwDpnlM',
  },
  stateWorker: {
    mainModule: 'state.js',
    modules: [{ name: 'state.js', content: 'export class Maintenance {}' }],
    compatibilityDate: '2026-08-11',
  },
  egressProxyWorker: {
    mainModule: 'egress.js',
    modules: [{ name: 'egress.js', content: 'export default {}' }],
    compatibilityDate: '2026-08-11',
  },
  stateDurableObjectMigrations: [
    { tag: 'v1', newSqliteClasses: ['Maintenance'] },
  ],
  organizationEgressHosts: ['api.example.com'],
};
const ROUTE_EXPECTATION_PHASES = [
  ['planned', false, 'migrating', ['prior']],
  ['schema-applied', false, 'migrating', ['prior']],
  ['platform-applied', false, 'migrating', ['prior']],
  ['candidate-deployed', false, 'migrating', ['prior']],
  ['candidate-armed', false, 'migrating', ['prior', 'target']],
  ['route-published', false, 'migrating', ['target']],
  ['platform-applied', true, 'migrating', ['prior', 'target']],
  ['route-published', true, 'migrating', ['target']],
  ['candidate-armed', false, 'decommissioning', ['prior', 'target']],
  ['route-published', false, 'credentials-revoked', ['target']],
] as const;

describe('external platform resource identity', () => {
  it.each(
    ROUTE_EXPECTATION_PHASES,
  )('derives exact %s migration route expectations with platformOnly=%s in %s', (subphase, platformOnly, phase, expected) => {
    const release = (name: 'prior' | 'target'): ExternalReleaseSnapshot => ({
      physicalScriptName: `release-${name}`,
      specDigest: (name === 'prior' ? 'a' : 'b').repeat(64),
      artifactVersion: `etag-${name}`,
      releaseSchemaVersion: name === 'prior' ? 1 : 2,
    });
    const policy = (host: string) =>
      canonicalDeploymentEgressPolicy({
        policyId: 'policy-acme',
        tenantTag: 'acme',
        environment: 'production',
        allowedHosts: [host],
      });
    const target = (
      fill: string,
      host: string,
    ): ExternalPlatformTargetDescription => ({
      maintenanceCapabilityPublicKey: profile.maintenanceCapabilityPublicKey,
      stateArtifactDigest: fill.repeat(64),
      stateDurableObjectHistoryDigest: 'c'.repeat(64),
      sharedOutboundWorkerName: 'shared-outbound',
      stateEgressCredentialDigest: 'd'.repeat(64),
      d1SchemaVersion: host === 'prior.example.com' ? 1 : 2,
      d1SchemaHistoryDigest: 'e'.repeat(64),
      outboundPolicy: policy(host),
    });
    const priorRelease = release('prior');
    const targetRelease = platformOnly ? priorRelease : release('target');
    const priorTarget = target('1', 'prior.example.com');
    const nextTarget = target('2', 'target.example.com');
    const record: FleetRecord = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'workers-for-platforms',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 2,
      artifactVersion: priorRelease.artifactVersion,
      desiredSpecDigest: targetRelease.specDigest,
      activeRelease: priorRelease,
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase,
      migrationIntent: {
        ...(platformOnly ? { platformOnly: true as const } : {}),
        targetSpecDigest: targetRelease.specDigest,
        priorRelease,
        priorTarget,
        priorOutboundPolicy: priorTarget.outboundPolicy,
        targetRelease,
        target: nextTarget,
        subphase,
      },
      updatedAt: '2026-08-11T00:00:00.000Z',
    };

    expect(
      externalRouteExpectations(record).map(({ target: item }) =>
        item === priorTarget ? 'prior' : 'target',
      ),
    ).toEqual(expected);
  });

  it('derives the active release and current target after migration settles', () => {
    const activeRelease: ExternalReleaseSnapshot = {
      physicalScriptName: 'release-active',
      specDigest: 'a'.repeat(64),
      artifactVersion: 'etag-active',
      releaseSchemaVersion: 2,
    };
    const target: ExternalPlatformTargetDescription = {
      maintenanceCapabilityPublicKey: profile.maintenanceCapabilityPublicKey,
      stateArtifactDigest: '1'.repeat(64),
      stateDurableObjectHistoryDigest: '2'.repeat(64),
      sharedOutboundWorkerName: 'shared-outbound',
      stateEgressCredentialDigest: '3'.repeat(64),
      d1SchemaVersion: 2,
      d1SchemaHistoryDigest: '4'.repeat(64),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: 'policy-acme',
        tenantTag: 'acme',
        environment: 'production',
        allowedHosts: ['api.example.com'],
      }),
    };
    const record: FleetRecord = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'workers-for-platforms',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 2,
      artifactVersion: activeRelease.artifactVersion,
      desiredSpecDigest: activeRelease.specDigest,
      activeRelease,
      platformTarget: target,
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase: 'ready',
      updatedAt: '2026-08-11T00:00:00.000Z',
    };

    expect(externalRouteExpectations(record)).toEqual([
      { release: activeRelease, target },
    ]);
  });

  it.each([
    ['publishing', false, ['pending']],
    ['rolling-back', false, ['active', 'pending']],
    ['rolling-back', true, ['active']],
    ['decommissioning', false, ['active', 'pending']],
    ['traffic-removed', false, []],
    ['credentials-revoked', false, ['active', 'pending']],
  ] as const)('derives current-target route authority in %s and deduplicates=%s', (phase, duplicate, expected) => {
    const release = (name: 'active' | 'pending'): ExternalReleaseSnapshot => ({
      physicalScriptName: duplicate ? 'release-active' : `release-${name}`,
      specDigest: (name === 'active' ? 'a' : 'b').repeat(64),
      artifactVersion: `etag-${name}`,
      releaseSchemaVersion: 2,
    });
    const activeRelease = release('active');
    const pendingRelease = release('pending');
    const target: ExternalPlatformTargetDescription = {
      maintenanceCapabilityPublicKey: profile.maintenanceCapabilityPublicKey,
      stateArtifactDigest: '1'.repeat(64),
      stateDurableObjectHistoryDigest: '2'.repeat(64),
      sharedOutboundWorkerName: 'shared-outbound',
      stateEgressCredentialDigest: '3'.repeat(64),
      d1SchemaVersion: 2,
      d1SchemaHistoryDigest: '4'.repeat(64),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: 'policy-acme',
        tenantTag: 'acme',
        environment: 'production',
        allowedHosts: ['api.example.com'],
      }),
    };
    const record: FleetRecord = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'workers-for-platforms',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 2,
      artifactVersion: activeRelease.artifactVersion,
      desiredSpecDigest: activeRelease.specDigest,
      activeRelease,
      pendingRelease,
      platformTarget: target,
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase,
      updatedAt: '2026-08-11T00:00:00.000Z',
    };

    expect(
      externalRouteExpectations(record).map(({ release: item }) =>
        item === activeRelease ? 'active' : 'pending',
      ),
    ).toEqual(expected);
  });

  it('uses the effective lifecycle phase for route authority and diagnostics', () => {
    const activeRelease: ExternalReleaseSnapshot = {
      physicalScriptName: 'release-active',
      specDigest: 'a'.repeat(64),
      artifactVersion: 'etag-active',
      releaseSchemaVersion: 2,
    };
    const target: ExternalPlatformTargetDescription = {
      maintenanceCapabilityPublicKey: profile.maintenanceCapabilityPublicKey,
      stateArtifactDigest: '1'.repeat(64),
      stateDurableObjectHistoryDigest: '2'.repeat(64),
      sharedOutboundWorkerName: 'shared-outbound',
      stateEgressCredentialDigest: '3'.repeat(64),
      d1SchemaVersion: 2,
      d1SchemaHistoryDigest: '4'.repeat(64),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: 'policy-acme',
        tenantTag: 'acme',
        environment: 'production',
        allowedHosts: ['api.example.com'],
      }),
    };
    const ready: FleetRecord = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'workers-for-platforms',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 2,
      artifactVersion: activeRelease.artifactVersion,
      desiredSpecDigest: activeRelease.specDigest,
      activeRelease,
      platformTarget: target,
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase: 'ready',
      updatedAt: '2026-08-11T00:00:00.000Z',
    };

    expect(
      externalRouteExpectations(
        decommissionAdvancingRecordFixture(ready, 'ready'),
      ),
    ).toEqual([{ release: activeRelease, target }]);
    expect(() =>
      externalRouteExpectations(
        decommissionAdvancingRecordFixture(
          {
            ...ready,
            pendingRelease: activeRelease,
            platformTarget: undefined,
          },
          'publishing',
        ),
      ),
    ).toThrow(
      'external publishing route authority has no persisted platform target',
    );
    expect(() =>
      externalRouteExpectations(
        decommissionAdvancingRecordFixture(
          { ...ready, activeRelease: undefined },
          'ready',
        ),
      ),
    ).toThrow('external ready route authority has no persisted release');
  });

  it('rejects publishing without its intended release and platform target', () => {
    const incomplete: FleetRecord = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'workers-for-platforms',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 2,
      artifactVersion: 'etag-active',
      desiredSpecDigest: 'a'.repeat(64),
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase: 'publishing',
      updatedAt: '2026-08-11T00:00:00.000Z',
    };

    expect(() => externalRouteExpectations(incomplete)).toThrow(
      /no persisted platform target/,
    );
  });

  it('uses a remote Durable Object, never a broad service binding, for ordinary bridge audit', () => {
    const queuedSpec: DeploymentSpec = {
      ...spec,
      queueProducer: { binding: 'AUDIT_QUEUE', queueName: 'audit-acme' },
    };
    const topology = externalReleaseTopology(queuedSpec, {
      auditQueueName: 'audit-acme',
      maintenanceCapabilityPublicKey: profile.maintenanceCapabilityPublicKey,
      stateWorker: {
        scriptName: queuedSpec.scriptName,
        artifactVersion: 'bridge-v1',
        artifactDigest: 'a'.repeat(64),
        plane: 'ordinary',
        durableObjectBindings: [
          {
            name: 'MAINTENANCE',
            className: 'Maintenance',
            namespaceId: 'namespace-maintenance',
          },
          {
            name: 'FLEET_AUDIT_PROXY_OBJECT',
            className: 'FlowsafeFleetAuditProxy',
            namespaceId: 'namespace-audit',
          },
        ],
        namespaceIds: ['namespace-audit', 'namespace-maintenance'],
      },
    });

    expect(topology.serviceBindings).toEqual([]);
    expect(topology.queueProducerBindings).toEqual([]);
    expect(topology.durableObjectBindings).toContainEqual({
      name: 'AUDIT_PROXY',
      className: 'FlowsafeFleetAuditProxy',
      namespaceId: 'namespace-audit',
      scriptName: queuedSpec.scriptName,
    });
  });

  it('derives stable bounded names from immutable deployment identity', () => {
    expect(externalPlatformResourceGroupId(spec)).toMatch(/^[a-f0-9]{20}$/u);
    expect(externalStateScriptName(spec)).toMatch(/-state-[a-f0-9]{20}$/u);
    expect(externalEgressProxyScriptName(spec)).toMatch(
      /-egress-[a-f0-9]{20}$/u,
    );
    expect(externalStateScriptName(spec).length).toBeLessThanOrEqual(63);
    expect(externalEgressProxyScriptName(spec).length).toBeLessThanOrEqual(63);
    const changedArtifact = {
      ...spec,
      modules: [{ name: 'candidate.js', content: 'changed' }],
    };
    expect(externalStateScriptName(changedArtifact)).toBe(
      externalStateScriptName(spec),
    );
  });

  it('digests trusted artifacts and normalized egress policy', () => {
    expect(trustedArtifactDigest(profile.stateWorker)).toMatch(
      /^[a-f0-9]{64}$/u,
    );
    const policy = {
      policyId: 'policy-acme',
      tenantTag: 'acme',
      environment: 'production',
      allowedHosts: ['api.example.com'],
    };
    expect(deploymentEgressPolicyDigest(policy)).not.toBe(
      deploymentEgressPolicyDigest({ ...policy, tenantTag: 'other' }),
    );
  });

  it('canonicalizes unordered class sets in the Durable Object history digest', () => {
    const left = durableObjectMigrationHistoryDigest([
      {
        tag: 'v1',
        newSqliteClasses: ['Beta', 'Alpha'],
        deletedClasses: ['LegacyB', 'LegacyA'],
      },
    ]);
    const right = durableObjectMigrationHistoryDigest([
      {
        tag: 'v1',
        newSqliteClasses: ['Alpha', 'Beta'],
        deletedClasses: ['LegacyA', 'LegacyB'],
      },
    ]);
    expect(left).toBe(right);
    expect(left).toMatch(/^[a-f0-9]{64}$/u);
  });

  it('rejects profiles that do not provision every requested state class', () => {
    expect(() => validateExternalPlatformProfile(spec, profile)).not.toThrow();
    expect(() =>
      validateExternalPlatformProfile(spec, {
        ...profile,
        stateDurableObjectMigrations: [],
      }),
    ).toThrow(/does not provision Durable Object class/);
  });

  it('rejects customer-controlled or ambiguous policy inputs', () => {
    expect(() =>
      validateExternalPlatformProfile(spec, {
        ...profile,
        organizationEgressHosts: ['API.example.com'],
      }),
    ).toThrow(/egress host/);
    expect(() =>
      validateExternalPlatformProfile(spec, {
        ...profile,
        stateWorker: { ...profile.stateWorker, modules: spec.modules },
      }),
    ).toThrow(/mainModule is absent/);
  });
});
