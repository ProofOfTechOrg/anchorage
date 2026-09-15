// SPDX-License-Identifier: Apache-2.0

/**
 * GENERATED FILE. DO NOT EDIT BY HAND.
 */

import type { FleetRecord } from '../../src/types.js';
import type { MigrationOpLogEntry } from './fleet-migration-worlds.js';

/**
 * An absent `durableObjectTag` key differs from a present key whose value
 * is `undefined`.
 */
export const MIGRATION_SUCCESS_BASELINE_RESULT = [
  {
    tenantTag: 'extfull',
    backend: 'workers-for-platforms',
    environment: 'production',
    scriptName: 'worker-extfull',
    databaseId: 'db-extfull',
    databaseName: 'database-extfull',
    schemaVersion: 1,
    artifactVersion:
      'etag:worker-extfull-d52caa3d429aa0ace5a74c7ee292cf130b26073edb205d49',
    desiredSpecDigest:
      'd52caa3d429aa0ace5a74c7ee292cf130b26073edb205d491d9ccac284437743',
    durableObjectBindings: [],
    routeHostname: 'worker-extfull.example.test',
    phase: 'ready',
    updatedAt: '2026-06-01T00:00:00.000Z',
    activeRelease: {
      physicalScriptName:
        'worker-extfull-d52caa3d429aa0ace5a74c7ee292cf130b26073edb205d49',
      specDigest:
        'd52caa3d429aa0ace5a74c7ee292cf130b26073edb205d491d9ccac284437743',
      artifactVersion:
        'etag:worker-extfull-d52caa3d429aa0ace5a74c7ee292cf130b26073edb205d49',
      releaseSchemaVersion: 1,
      application: { vars: [], secrets: [], r2Buckets: [] },
      topology: {
        durableObjectBindings: [],
        serviceBindings: [],
        queueProducerBindings: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        application: { vars: [], secrets: [], r2Buckets: [] },
      },
    },
    rollbackRelease: {
      physicalScriptName:
        'worker-extfull-4fd765a654b3aa105049637349a4a410505828acacb0c290',
      specDigest:
        '4fd765a654b3aa105049637349a4a410505828acacb0c2909968c85abe72ec4a',
      artifactVersion:
        'etag:worker-extfull-4fd765a654b3aa105049637349a4a410505828acacb0c290',
      releaseSchemaVersion: 1,
      application: { vars: [], secrets: [], r2Buckets: [] },
    },
    platformTarget: {
      maintenanceCapabilityPublicKey:
        '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
      stateArtifactDigest:
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      stateDurableObjectHistoryDigest:
        'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
      egressArtifactDigest:
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      d1SchemaVersion: 1,
      d1SchemaHistoryDigest:
        'd52caa3d429aa0ace5a74c7ee292cf130b26073edb205d491d9ccac284437743',
      outboundPolicy: {
        policyId: '3f9fb725d669c5442027',
        policyHosts: ['api.example.test'],
        policyDigest:
          'cade8643815e4925c439a055aab56460f3cb24ff8a742365d25db2573c9d6f58',
      },
    },
    outboundPolicy: {
      policyId: '3f9fb725d669c5442027',
      policyHosts: ['api.example.test'],
      policyDigest:
        'cade8643815e4925c439a055aab56460f3cb24ff8a742365d25db2573c9d6f58',
    },
    platformResources: {
      maintenanceCapabilityPublicKey:
        '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
      stateWorker: {
        scriptName: 'worker-extfull-state-3f9fb725d669c5442027',
        artifactVersion: 'state-v1',
        artifactDigest:
          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: 'worker-extfull-egress-3f9fb725d669c5442027',
        artifactVersion: 'egress-v1',
        artifactDigest:
          'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        policyId: '3f9fb725d669c5442027',
        policyHosts: ['api.example.test'],
        policyDigest:
          'cade8643815e4925c439a055aab56460f3cb24ff8a742365d25db2573c9d6f58',
      },
    },
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
    applicationResources: [],
    invocationAuthority: {
      version: 1,
      authorizedAt: '2026-06-01T00:00:00.000Z',
    },
    durableObjectTag: undefined,
    settledSettlementKey:
      '0b32b3081b897947a1db852b033a86fad2e38700dc2824e91cc292904e642b6c',
  },
  {
    tenantTag: 'plainmulti',
    backend: 'plain-worker',
    environment: 'production',
    scriptName: 'worker-plainmulti',
    databaseId: 'db-plainmulti',
    databaseName: 'database-plainmulti',
    schemaVersion: 3,
    artifactVersion: 'v3',
    desiredSpecDigest:
      'a14a55db26a2a30823bf42d881a3208c6a7f5320b86a15cd5b97dec8116b69ad',
    durableObjectBindings: [],
    routeHostname: 'worker-plainmulti.example.test',
    phase: 'ready',
    updatedAt: '2026-06-01T00:00:00.000Z',
    invocationAuthority: {
      version: 1,
      authorizedAt: '2026-06-01T00:00:00.000Z',
    },
    durableObjectTag: undefined,
    durableObjectMigrationHistory: [],
    durableObjectMigrationHistoryDigest:
      '4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945',
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
    settledSettlementKey:
      '2c4bf4db5f076b148e247b473983a304137a0ff1a1a5e717ecc9b148f10e7d11',
  },
  {
    tenantTag: 'platformonly',
    backend: 'workers-for-platforms',
    environment: 'production',
    scriptName: 'worker-platformonly',
    databaseId: 'db-platformonly',
    databaseName: 'database-platformonly',
    schemaVersion: 2,
    artifactVersion:
      'etag:worker-platfor-d4e2b8009b6fe3b0d1db07781d51d8acd2032c0a4cf3f754',
    desiredSpecDigest:
      'd4e2b8009b6fe3b0d1db07781d51d8acd2032c0a4cf3f754b65a0839530cc9e7',
    durableObjectBindings: [],
    routeHostname: 'worker-platformonly.example.test',
    phase: 'ready',
    updatedAt: '2026-06-01T00:00:00.000Z',
    activeRelease: {
      physicalScriptName:
        'worker-platfor-d4e2b8009b6fe3b0d1db07781d51d8acd2032c0a4cf3f754',
      specDigest:
        'd4e2b8009b6fe3b0d1db07781d51d8acd2032c0a4cf3f754b65a0839530cc9e7',
      artifactVersion:
        'etag:worker-platfor-d4e2b8009b6fe3b0d1db07781d51d8acd2032c0a4cf3f754',
      releaseSchemaVersion: 1,
      application: { vars: [], secrets: [], r2Buckets: [] },
    },
    platformTarget: {
      maintenanceCapabilityPublicKey:
        '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
      stateArtifactDigest:
        'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
      stateDurableObjectHistoryDigest:
        'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
      egressArtifactDigest:
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      d1SchemaVersion: 2,
      d1SchemaHistoryDigest:
        'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
      outboundPolicy: {
        policyId: '48d210fc661a8eabf890',
        policyHosts: ['narrow.example.test'],
        policyDigest:
          '378801bb2f0900c3180f762ae01e58af4ad69f724cf72300f492c2cb17989788',
      },
    },
    outboundPolicy: {
      policyId: '48d210fc661a8eabf890',
      policyHosts: ['narrow.example.test'],
      policyDigest:
        '378801bb2f0900c3180f762ae01e58af4ad69f724cf72300f492c2cb17989788',
    },
    platformResources: {
      maintenanceCapabilityPublicKey:
        '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
      stateWorker: {
        scriptName: 'worker-platformonly-state-48d210fc661a8eabf890',
        artifactVersion: 'state-v1',
        artifactDigest:
          'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: 'worker-platformonly-egress-48d210fc661a8eabf890',
        artifactVersion: 'egress-v1',
        artifactDigest:
          'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        policyId: '48d210fc661a8eabf890',
        policyHosts: ['narrow.example.test'],
        policyDigest:
          '378801bb2f0900c3180f762ae01e58af4ad69f724cf72300f492c2cb17989788',
      },
    },
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
    applicationResources: [],
    invocationAuthority: {
      version: 1,
      authorizedAt: '2026-06-01T00:00:00.000Z',
    },
    settledSettlementKey:
      'be9c10cc1282c29405ae83ce4aea196b0d2aeea40968af5815a0d5cdafa9f066',
  },
  {
    tenantTag: 'readysteady',
    backend: 'workers-for-platforms',
    environment: 'production',
    scriptName: 'worker-readysteady',
    databaseId: 'db-readysteady',
    databaseName: 'database-readysteady',
    schemaVersion: 1,
    artifactVersion:
      'etag:worker-readyst-37dfa9ca61e263d1efa4845ea58e1474d4a3f86c7b3e4fe0',
    desiredSpecDigest:
      '37dfa9ca61e263d1efa4845ea58e1474d4a3f86c7b3e4fe0152367854b05e8bb',
    durableObjectBindings: [],
    routeHostname: 'worker-readysteady.example.test',
    phase: 'ready',
    updatedAt: '2026-06-01T00:00:00.000Z',
    activeRelease: {
      physicalScriptName:
        'worker-readyst-37dfa9ca61e263d1efa4845ea58e1474d4a3f86c7b3e4fe0',
      specDigest:
        '37dfa9ca61e263d1efa4845ea58e1474d4a3f86c7b3e4fe0152367854b05e8bb',
      artifactVersion:
        'etag:worker-readyst-37dfa9ca61e263d1efa4845ea58e1474d4a3f86c7b3e4fe0',
      releaseSchemaVersion: 1,
      application: { vars: [], secrets: [], r2Buckets: [] },
    },
    rollbackRelease: {
      physicalScriptName:
        'worker-readyst-1c9ff30e744df5fd3e600c49fdfcd2eaa4940b35432bb456',
      specDigest:
        '1c9ff30e744df5fd3e600c49fdfcd2eaa4940b35432bb456df79439af56cca1d',
      artifactVersion:
        'etag:worker-readyst-1c9ff30e744df5fd3e600c49fdfcd2eaa4940b35432bb456',
      releaseSchemaVersion: 1,
      application: { vars: [], secrets: [], r2Buckets: [] },
    },
    platformTarget: {
      maintenanceCapabilityPublicKey:
        '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
      stateArtifactDigest:
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      stateDurableObjectHistoryDigest:
        'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
      egressArtifactDigest:
        'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      d1SchemaVersion: 1,
      d1SchemaHistoryDigest:
        '37dfa9ca61e263d1efa4845ea58e1474d4a3f86c7b3e4fe0152367854b05e8bb',
      outboundPolicy: {
        policyId: 'ab62bd5bf4ae76f23a5b',
        policyHosts: ['api.example.test'],
        policyDigest:
          '008311f7e91a3a3de1d9684b4404deb1b8e2b3a3e3d7bdff33f010eaf677163a',
      },
    },
    outboundPolicy: {
      policyId: 'ab62bd5bf4ae76f23a5b',
      policyHosts: ['api.example.test'],
      policyDigest:
        '008311f7e91a3a3de1d9684b4404deb1b8e2b3a3e3d7bdff33f010eaf677163a',
    },
    platformResources: {
      maintenanceCapabilityPublicKey:
        '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
      stateWorker: {
        scriptName: 'worker-readysteady-state-ab62bd5bf4ae76f23a5b',
        artifactVersion: 'state-v1',
        artifactDigest:
          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: 'worker-readysteady-egress-ab62bd5bf4ae76f23a5b',
        artifactVersion: 'egress-v1',
        artifactDigest:
          'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        policyId: 'ab62bd5bf4ae76f23a5b',
        policyHosts: ['api.example.test'],
        policyDigest:
          '008311f7e91a3a3de1d9684b4404deb1b8e2b3a3e3d7bdff33f010eaf677163a',
      },
    },
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
    applicationResources: [],
    settledSettlementKey:
      'db2a02ad3af863decedc58561b6b1b5119f7c821b0a83d879416bd1ec2d186c5',
    invocationAuthority: {
      version: 1,
      authorizedAt: '2026-06-01T00:00:00.000Z',
    },
  },
] as const satisfies readonly FleetRecord[];

/**
 * `applyMigrations:verify` depends on the spec array's reference identity;
 * a per-version slice can contain the same migrations.
 */
export const MIGRATION_SUCCESS_BASELINE_OPS = [
  'withDeploymentLease',
  'get',
  'resolver:backendFor:extfull:production',
  'resolver:specFor:extfull:production',
  'resolver:secretsFor:extfull:production',
  'releaseScriptName',
  'describeExternalPlatformTarget',
  'getDatabase',
  'readDeploymentIdentity',
  'put:migrating',
  'assertOwned',
  'seedDeploymentIdentity',
  'assertOwned',
  'applyMigrations:verify',
  'put:schema-applied',
  'assertOwned',
  'ensurePlatformResources',
  'put:migrating',
  'put:platform-applied',
  'put:migrating',
  'put:migrating',
  'assertOwned',
  'deployWorker',
  'inspect',
  'put:candidate-deployed',
  'assertOwned',
  'ensureMaintenance',
  'put:candidate-armed',
  'inspect',
  'assertOwned',
  'promoteWorker',
  'put:route-published',
  'inspect',
  'resolver:settlementFor:extfull:production',
  'attestActiveRoute',
  'settle:0b32b3081b897947a1db852b033a86fad2e38700dc2824e91cc292904e642b6c',
  'put:ready',
  'assertOwned',
  'deleteRetainedRelease',
  'put:ready',
  'withDeploymentLease',
  'get',
  'resolver:backendFor:plainmulti:production',
  'resolver:specFor:plainmulti:production',
  'resolver:secretsFor:plainmulti:production',
  'getDatabase',
  'readDeploymentIdentity',
  'put:migrating',
  'assertOwned',
  'seedDeploymentIdentity',
  'assertOwned',
  'applyMigrations:2',
  'put:migrating',
  'assertOwned',
  'applyMigrations:3',
  'put:migrating',
  'inspect',
  'put:migrating',
  'assertOwned',
  'deployWorker',
  'inspect',
  'put:migrating',
  'assertOwned',
  'ensureMaintenance',
  'inspect',
  'assertOwned',
  'promoteWorker',
  'inspect',
  'resolver:settlementFor:plainmulti:production',
  'attestActiveRoute',
  'settle:2c4bf4db5f076b148e247b473983a304137a0ff1a1a5e717ecc9b148f10e7d11',
  'put:ready',
  'withDeploymentLease',
  'get',
  'resolver:backendFor:platformonly:production',
  'resolver:specFor:platformonly:production',
  'resolver:secretsFor:platformonly:production',
  'releaseScriptName',
  'describeExternalPlatformTarget',
  'getDatabase',
  'readDeploymentIdentity',
  'put:migrating',
  'put:schema-applied',
  'assertOwned',
  'ensurePlatformResources',
  'put:migrating',
  'put:platform-applied',
  'inspect',
  'put:migrating',
  'assertOwned',
  'ensureMaintenance',
  'inspect',
  'assertOwned',
  'promoteWorker',
  'put:route-published',
  'inspect',
  'resolver:settlementFor:platformonly:production',
  'attestActiveRoute',
  'settle:be9c10cc1282c29405ae83ce4aea196b0d2aeea40968af5815a0d5cdafa9f066',
  'put:ready',
  'withDeploymentLease',
  'get',
  'resolver:backendFor:readysteady:production',
  'resolver:specFor:readysteady:production',
  'resolver:secretsFor:readysteady:production',
  'releaseScriptName',
  'describeExternalPlatformTarget',
  'getDatabase',
  'readDeploymentIdentity',
  'assertOwned',
  'deleteRetainedRelease',
  'put:ready',
  'assertOwned',
  'ensurePlatformResources',
  'inspect',
  'put:ready',
  'assertOwned',
  'ensureMaintenance',
  'assertOwned',
  'promoteWorker',
  'resolver:settlementFor:readysteady:production',
  'attestActiveRoute',
] as const satisfies readonly MigrationOpLogEntry[];

export const MIGRATION_STOP_BASELINE_ERROR =
  "deployment 'bravo:production' has active backend switch 'candidate-deployed'" as const satisfies string;

export const MIGRATION_STOP_BASELINE_OPS = [
  'withDeploymentLease',
  'get',
  'resolver:backendFor:alpha:production',
  'resolver:specFor:alpha:production',
  'resolver:secretsFor:alpha:production',
  'getDatabase',
  'readDeploymentIdentity',
  'put:migrating',
  'assertOwned',
  'seedDeploymentIdentity',
  'assertOwned',
  'applyMigrations:2',
  'put:migrating',
  'inspect',
  'put:migrating',
  'assertOwned',
  'deployWorker',
  'inspect',
  'put:migrating',
  'assertOwned',
  'ensureMaintenance',
  'inspect',
  'assertOwned',
  'promoteWorker',
  'inspect',
  'resolver:settlementFor:alpha:production',
  'attestActiveRoute',
  'settle:fed662c15639242a95f2a01b42394e5ce93bfdacc9891bf9d3582d10d16c6df8',
  'put:ready',
  'withDeploymentLease',
  'get',
] as const satisfies readonly MigrationOpLogEntry[];
