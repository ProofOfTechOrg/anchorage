// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type {
  BridgeMutationPlan,
  BridgeSnapshot,
  PlainBackendSnapshot,
} from '../src/backend-switch.js';
import {
  canonicalDeploymentEgressPolicy,
  durableObjectMigrationHistoryDigest,
  externalPlatformResourceGroupId,
} from '../src/platform-resources.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  DeploymentSpec,
  ExternalMutationFence,
  ExternalPlatformProfile,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
} from '../src/types.js';
import type { WorkersForPlatformsBackend } from '../src/workers-for-platforms-backend.js';
import type { BackendSwitchApi } from '../src/workers-for-platforms-backend-switch-provider.js';
import { WorkersForPlatformsBackendSwitchProvider } from '../src/workers-for-platforms-backend-switch-provider.js';

const fence: ExternalMutationFence = {
  mutationLeaseTtlMs: 60_000,
  async assertOwned() {},
};

const targetSpec: DeploymentSpec = {
  tenantTag: 'acme',
  environment: 'production',
  scriptName: 'acme-production',
  databaseName: 'acme-production',
  compatibilityDate: '2026-08-11',
  mainModule: 'candidate.js',
  modules: [{ name: 'candidate.js', content: 'export default {}' }],
  authoredBy: 'external',
  schemaVersion: 1,
  migrations: [{ version: 1, sql: 'SELECT 1' }],
  durableObjectMigrations: [{ tag: 'v1', newClasses: ['Runner'] }],
  durableObjectBindings: [{ name: 'RUNNER', className: 'Runner' }],
  maintenanceBaseUrl: 'https://control.example.test',
  routeHostname: 'acme.example.test',
};
const priorSpec: DeploymentSpec = { ...targetSpec, authoredBy: 'platform' };

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
    modules: [{ name: 'state.js', content: 'export class State {}' }],
    compatibilityDate: '2026-08-11',
  },
  legacyBridgeWorker: {
    mainModule: 'bridge.js',
    modules: [
      {
        name: 'bridge.js',
        content:
          "import application from '__ANCHORAGE_LEGACY_APPLICATION_MODULE__'; export class State {}; export default application;",
      },
    ],
    compatibilityDate: '2026-08-11',
  },
  stateDurableObjectMigrations: [
    { tag: 'v1', newClasses: ['Runner'] },
    { tag: 'v2', newClasses: ['State'] },
  ],
  organizationEgressHosts: ['api.example.com'],
};

const target: ExternalPlatformTargetDescription = {
  maintenanceCapabilityPublicKey: profile.maintenanceCapabilityPublicKey,
  stateArtifactDigest: 'a'.repeat(64),
  stateDurableObjectHistoryDigest: 'b'.repeat(64),
  stateDurableObjectTag: 'v2',
  sharedOutboundWorkerName: 'shared-outbound',
  stateEgressCredentialDigest: 'c'.repeat(64),
  d1SchemaVersion: 1,
  d1SchemaHistoryDigest: 'd'.repeat(64),
  outboundPolicy: canonicalDeploymentEgressPolicy({
    policyId: externalPlatformResourceGroupId(targetSpec),
    tenantTag: targetSpec.tenantTag,
    environment: targetSpec.environment,
    allowedHosts: profile.organizationEgressHosts,
  }),
};

const prior: PlainBackendSnapshot = {
  scriptName: 'acme-production',
  artifactVersion: 'plain-v1',
  specDigest: 'a'.repeat(64),
  databaseId: 'db-acme',
  databaseName: 'acme-production',
  durableObjectBindings: [
    { name: 'RUNNER', className: 'Runner', namespaceId: 'namespace-runner' },
  ],
  namespaceIds: ['namespace-runner'],
  secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
  applicationResources: [],
  customDomain: { id: 'domain-acme', hostname: 'acme.example.test' },
};

const release: ExternalReleaseSnapshot = {
  physicalScriptName: 'candidate-v1',
  specDigest: deploymentSpecDigest(targetSpec),
  artifactVersion: 'pending',
  releaseSchemaVersion: targetSpec.schemaVersion,
  application: { vars: [], secrets: [], r2Buckets: [] },
  topology: {
    durableObjectBindings: [],
    serviceBindings: [],
    queueProducerBindings: [],
    secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
    application: { vars: [], secrets: [], r2Buckets: [] },
  },
};

const trafficAuthority = {
  priorSpec,
  targetSpec,
  allowedArtifactVersions: [prior.artifactVersion],
};

function providerBindingIdentitiesForTest(inspection: {
  databaseIds: readonly string[];
  durableObjectBindings: readonly { name: string }[];
  serviceBindings?: readonly { name: string }[];
  queueProducerBindings?: readonly { name: string }[];
  kvNamespaceBindings?: readonly { name: string }[];
  r2BucketBindings?: readonly { name: string }[];
  secretNames: readonly string[];
  plainTextBindings: Readonly<Record<string, string>>;
}) {
  return [
    ...inspection.databaseIds.map(() => ({ type: 'd1', name: 'DB' })),
    ...inspection.durableObjectBindings.map(({ name }) => ({
      type: 'durable_object_namespace',
      name,
    })),
    ...(inspection.serviceBindings ?? []).map(({ name }) => ({
      type: 'service',
      name,
    })),
    ...(inspection.queueProducerBindings ?? []).map(({ name }) => ({
      type: 'queue',
      name,
    })),
    ...(inspection.kvNamespaceBindings ?? []).map(({ name }) => ({
      type: 'kv_namespace',
      name,
    })),
    ...(inspection.r2BucketBindings ?? []).map(({ name }) => ({
      type: 'r2_bucket',
      name,
    })),
    ...inspection.secretNames.map((name) => ({ type: 'secret_text', name })),
    ...Object.keys(inspection.plainTextBindings).map((name) => ({
      type: 'plain_text',
      name,
    })),
  ];
}

function completeProviderBindingInspection<
  T extends Parameters<typeof providerBindingIdentitiesForTest>[0],
>(
  inspection: T,
): T & {
  providerBindingIdentities: ReturnType<
    typeof providerBindingIdentitiesForTest
  >;
} {
  return {
    ...inspection,
    providerBindingIdentities: providerBindingIdentitiesForTest(inspection),
  };
}

function exactPriorWorker(): NonNullable<
  Awaited<ReturnType<BackendSwitchApi['inspectControlWorker']>>
> {
  return completeProviderBindingInspection({
    artifactVersion: prior.artifactVersion,
    databaseIds: [prior.databaseId],
    durableObjectBindings: prior.durableObjectBindings,
    serviceBindings: [],
    queueProducerBindings: [],
    kvNamespaceBindings: [],
    secretNames: prior.secretNames,
    plainTextBindings: {
      DEPLOYMENT_TENANT: targetSpec.tenantTag,
      FLEET_ENVIRONMENT: targetSpec.environment,
      FLEET_SPEC_DIGEST: prior.specDigest,
    },
    workersDevEnabled: true,
    previewUrlsEnabled: true,
    routeHostnames: [targetSpec.routeHostname],
    zoneRoutes: [],
  });
}

function ordinaryFootprint(
  overrides: Partial<
    Awaited<ReturnType<BackendSwitchApi['inspectOrdinaryWorkerFootprint']>>
  > = {},
): Awaited<ReturnType<BackendSwitchApi['inspectOrdinaryWorkerFootprint']>> {
  return {
    scriptPresent: true,
    workersDevEnabled: true,
    previewUrlsEnabled: true,
    customDomains: [],
    zoneRoutes: [],
    ...overrides,
  };
}

function provider(
  client: Partial<BackendSwitchApi>,
  backend: Partial<WorkersForPlatformsBackend> = {},
  platformProfileFor: (
    spec: DeploymentSpec,
  ) => ExternalPlatformProfile = () => {
    throw new Error('profile must not be read during teardown authorization');
  },
): WorkersForPlatformsBackendSwitchProvider {
  const inspectControlWorker = client.inspectControlWorker;
  const inspectDispatchWorker = client.inspectDispatchWorker;
  return new WorkersForPlatformsBackendSwitchProvider({
    client: {
      ...client,
      ...(inspectControlWorker
        ? {
            inspectControlWorker: async (scriptName: string) => {
              const inspection = await inspectControlWorker(scriptName);
              return inspection
                ? {
                    ...inspection,
                    providerBindingIdentities:
                      providerBindingIdentitiesForTest(inspection),
                  }
                : undefined;
            },
          }
        : {}),
      ...(inspectDispatchWorker
        ? {
            inspectDispatchWorker: async (scriptName: string) => {
              const inspection = await inspectDispatchWorker(scriptName);
              return inspection
                ? {
                    ...inspection,
                    providerBindingIdentities:
                      providerBindingIdentitiesForTest(inspection),
                  }
                : undefined;
            },
          }
        : {}),
    } as BackendSwitchApi,
    backend: backend as WorkersForPlatformsBackend,
    hostRoutingKvId: 'hosts-kv',
    sharedOutboundWorkerName: 'shared-outbound',
    stateEgressRootSecret: 'root-secret-012345678901234567890123456789',
    platformProfileFor,
    assertServing: async () => {},
    drainCandidate: async () => {},
  });
}

type ControlWorkerInspection = NonNullable<
  Awaited<ReturnType<BackendSwitchApi['inspectControlWorker']>>
>;

function exactRestoredWorker(artifactVersion: string): ControlWorkerInspection {
  return {
    ...exactPriorWorker(),
    artifactVersion,
    serviceBindings: [],
    queueProducerBindings: [],
    plainTextBindings: {
      DEPLOYMENT_TENANT: priorSpec.tenantTag,
      FLEET_ENVIRONMENT: priorSpec.environment,
      FLEET_SCHEMA_VERSION: String(priorSpec.schemaVersion),
      FLEET_SPEC_DIGEST: prior.specDigest,
    },
  };
}

async function committedPlanOnlyBridge(
  plannedTargetSpec: DeploymentSpec = targetSpec,
): Promise<{
  readonly subject: WorkersForPlatformsBackendSwitchProvider;
  readonly plan: BridgeMutationPlan;
  readonly mutations: string[];
  readonly live: () => ControlWorkerInspection;
  readonly replaceLive: (live: ControlWorkerInspection) => void;
  readonly replaceNamespaces: (namespaceIds: readonly string[]) => void;
  readonly replaceNamespaceListings: (
    namespaceIds: readonly (readonly string[])[],
  ) => void;
  readonly namespaceAbsenceChecks: readonly Readonly<{
    namespaceId: string;
    workerPresent: boolean;
  }>[];
}> {
  let bridgePresent = true;
  let publicAccessEnabled = true;
  let loseFirstMutationResponse = true;
  let inspection = exactPriorWorker();
  let namespaceIds = ['namespace-runner', 'namespace-state'];
  let namespaceListings: readonly (readonly string[])[] | undefined;
  let namespaceListingIndex = 0;
  const namespaceAbsenceChecks: {
    namespaceId: string;
    workerPresent: boolean;
  }[] = [];
  const mutations: string[] = [];
  const client: Partial<BackendSwitchApi> = {
    inspectControlWorker: async () => (bridgePresent ? inspection : undefined),
    withMutationFence: async (_fence, operation) => {
      const result = await operation();
      if (loseFirstMutationResponse) {
        loseFirstMutationResponse = false;
        throw new Error('provider response lost after commit');
      }
      return result;
    },
    uploadControlWorker: async (upload) => {
      inspection = completeProviderBindingInspection({
        artifactVersion: 'provider-committed-v2',
        databaseIds: upload.bindings.flatMap((binding) =>
          binding.type === 'd1' ? [String(binding.database_id)] : [],
        ),
        durableObjectBindings: upload.bindings.flatMap((binding) =>
          binding.type === 'durable_object_namespace'
            ? [
                {
                  name: String(binding.name),
                  className: String(binding.class_name),
                  namespaceId: `namespace-${String(binding.class_name).toLowerCase()}`,
                },
              ]
            : [],
        ),
        serviceBindings: upload.bindings.flatMap((binding) =>
          binding.type === 'service'
            ? [
                {
                  name: String(binding.name),
                  service: String(binding.service),
                  ...(binding.entrypoint
                    ? { entrypoint: String(binding.entrypoint) }
                    : {}),
                },
              ]
            : [],
        ),
        queueProducerBindings: upload.bindings.flatMap((binding) =>
          binding.type === 'queue'
            ? [
                {
                  name: String(binding.name),
                  queueName: String(binding.queue_name),
                },
              ]
            : [],
        ),
        r2BucketBindings: upload.bindings.flatMap((binding) =>
          binding.type === 'r2_bucket'
            ? [
                {
                  name: String(binding.name),
                  bucketName: String(binding.bucket_name),
                  jurisdiction: 'default' as const,
                },
              ]
            : [],
        ),
        kvNamespaceBindings: [],
        secretNames: inspection.secretNames,
        plainTextBindings: Object.fromEntries(
          upload.bindings.flatMap((binding) =>
            binding.type === 'plain_text'
              ? [[String(binding.name), String(binding.text)] as const]
              : [],
          ),
        ),
        workersDevEnabled: publicAccessEnabled,
        previewUrlsEnabled: publicAccessEnabled,
        routeHostnames: [],
        zoneRoutes: [],
      });
      return inspection.artifactVersion;
    },
    putControlSecrets: async (_scriptName, secrets) => {
      inspection = completeProviderBindingInspection({
        ...inspection,
        secretNames: Object.keys(secrets).sort(),
      });
    },
    inspectOrdinaryWorkerFootprint: async () => ({
      scriptPresent: bridgePresent,
      workersDevEnabled: bridgePresent ? publicAccessEnabled : undefined,
      previewUrlsEnabled: bridgePresent ? publicAccessEnabled : undefined,
      customDomains: [],
      zoneRoutes: [],
    }),
    getHostRouting: async () => undefined,
    deleteHostRouting: async () => {
      mutations.push('ingress:hosts');
    },
    listCustomDomains: async () => [],
    detachCustomDomain: async () => {
      mutations.push('ingress:domain');
    },
    disableOrdinaryWorkerPublicAccess: async () => {
      mutations.push('ingress:public-access');
      publicAccessEnabled = false;
    },
    listDurableObjectNamespaces: async () => {
      if (!bridgePresent) return [];
      const listed = namespaceListings?.[namespaceListingIndex];
      namespaceListingIndex += 1;
      return listed ?? namespaceIds;
    },
    revokeControlSecrets: async () => {
      mutations.push('credential:revoke');
    },
    deleteControlWorker: async () => {
      mutations.push('worker:delete');
      bridgePresent = false;
    },
    hasDurableObjectNamespace: async (namespaceId) => {
      namespaceAbsenceChecks.push({
        namespaceId,
        workerPresent: bridgePresent,
      });
      return bridgePresent;
    },
  };
  const subject = provider(client, {}, () => profile);
  const plan = subject.describeBridge({
    priorSpec,
    targetSpec: plannedTargetSpec,
    prior,
  });
  try {
    await subject.ensureBridge({
      priorSpec,
      targetSpec: plannedTargetSpec,
      prior,
      plan,
      secrets: {
        deploymentIdentity: 'deployment-identity-secret-0001',
        maintenanceAdmin: 'maintenance-admin-secret-0000001',
      },
      fence,
    });
    throw new Error('expected provider response loss');
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes('response lost')) {
      throw error;
    }
  }
  return {
    subject,
    plan,
    mutations,
    live: () => inspection,
    replaceLive: (live) => {
      inspection = live;
    },
    replaceNamespaces: (nextNamespaceIds) => {
      namespaceIds = [...nextNamespaceIds];
    },
    replaceNamespaceListings: (nextNamespaceIds) => {
      namespaceListings = nextNamespaceIds;
      namespaceListingIndex = 0;
    },
    namespaceAbsenceChecks,
  };
}

function persistedBridgeSnapshot(
  fixture: Awaited<ReturnType<typeof committedPlanOnlyBridge>>,
): BridgeSnapshot {
  return {
    scriptName: prior.scriptName,
    artifactVersion: fixture.live().artifactVersion,
    artifactDigest: fixture.plan.artifactDigest,
    databaseId: prior.databaseId,
    durableObjectBindings: fixture.live().durableObjectBindings,
    namespaceIds: ['namespace-runner', 'namespace-state'],
    secretNames: fixture.plan.secretNames,
    publicRouteAttached: true,
    stateOnly: false,
  };
}

async function removePlanOnlyBridge(
  fixture: Awaited<ReturnType<typeof committedPlanOnlyBridge>>,
  path: 'traffic' | 'bridge',
  bridge?: BridgeSnapshot,
  plan: BridgeMutationPlan = fixture.plan,
  allowedArtifactVersions: readonly string[] = [
    prior.artifactVersion,
    ...(bridge ? [bridge.artifactVersion] : []),
  ],
): Promise<void> {
  const authority = {
    prior,
    priorSpec,
    targetSpec,
    plan,
    ...(bridge ? { bridge } : {}),
    allowedArtifactVersions,
    fence,
  };
  if (path === 'traffic') {
    await fixture.subject.removeSwitchTraffic({
      ...authority,
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      routeHostname: targetSpec.routeHostname,
      routeTargets: [],
    });
    return;
  }
  await fixture.subject.removeSwitchBridge(authority);
}

describe('backend switch provider teardown authority', () => {
  it.each([
    'traffic',
    'bridge',
  ] as const)('adopts an exact plan-only %s teardown after provider-committed response loss', async (path) => {
    const fixture = await committedPlanOnlyBridge();

    await expect(removePlanOnlyBridge(fixture, path)).resolves.toBeUndefined();
    await expect(removePlanOnlyBridge(fixture, path)).resolves.toBeUndefined();

    expect(fixture.mutations).toEqual(
      path === 'traffic'
        ? [
            'ingress:hosts',
            'ingress:public-access',
            'ingress:hosts',
            'ingress:public-access',
          ]
        : ['credential:revoke', 'worker:delete'],
    );
  });

  it.each([
    'traffic',
    'bridge',
  ] as const)('adopts an exact persisted snapshot for committed-response and idempotent %s teardown', async (path) => {
    const fixture = await committedPlanOnlyBridge();
    const bridge = persistedBridgeSnapshot(fixture);

    await expect(
      removePlanOnlyBridge(fixture, path, bridge),
    ).resolves.toBeUndefined();
    await expect(
      removePlanOnlyBridge(fixture, path, bridge),
    ).resolves.toBeUndefined();
  });

  it.each([
    ['traffic', 'changed namespace identity'],
    ['traffic', 'extra namespace'],
    ['traffic', 'missing namespace'],
    ['traffic', 'missing fixed variable'],
    ['traffic', 'altered fixed variable'],
    ['bridge', 'changed namespace identity'],
    ['bridge', 'extra namespace'],
    ['bridge', 'missing namespace'],
    ['bridge', 'missing fixed variable'],
    ['bridge', 'altered fixed variable'],
  ] as const)('rejects persisted snapshot %s teardown with %s before mutation', async (path, drift) => {
    const fixture = await committedPlanOnlyBridge();
    const bridge = persistedBridgeSnapshot(fixture);
    const live = fixture.live();
    if (drift === 'changed namespace identity') {
      fixture.replaceLive({
        ...live,
        durableObjectBindings: live.durableObjectBindings.map((binding) => ({
          ...binding,
          namespaceId: `${binding.namespaceId}-foreign`,
        })),
      });
    } else if (drift === 'extra namespace') {
      fixture.replaceNamespaces([...bridge.namespaceIds, 'namespace-foreign']);
    } else if (drift === 'missing namespace') {
      fixture.replaceNamespaces(bridge.namespaceIds.slice(1));
    } else if (drift === 'missing fixed variable') {
      const { FLEET_RESOURCE_ROLE: _removed, ...plainTextBindings } =
        live.plainTextBindings;
      fixture.replaceLive({ ...live, plainTextBindings });
    } else {
      fixture.replaceLive({
        ...live,
        plainTextBindings: {
          ...live.plainTextBindings,
          OUTBOUND_POLICY_ID: 'foreign-resource-group',
        },
      });
    }

    await expect(removePlanOnlyBridge(fixture, path, bridge)).rejects.toThrow(
      /foreign backend-switch bridge/,
    );
    expect(fixture.mutations).toEqual([]);
  });

  it.each([
    ['traffic', 'missing secret'],
    ['traffic', 'extra secret'],
    ['traffic', 'altered fixed variable'],
    ['bridge', 'missing secret'],
    ['bridge', 'extra secret'],
    ['bridge', 'altered fixed variable'],
  ] as const)('rejects plan-only %s teardown with %s before mutation', async (path, drift) => {
    const fixture = await committedPlanOnlyBridge();
    const live = fixture.live();
    fixture.replaceLive(
      drift === 'missing secret'
        ? { ...live, secretNames: live.secretNames.slice(1) }
        : drift === 'extra secret'
          ? {
              ...live,
              secretNames: [...live.secretNames, 'UNRECORDED_SECRET'],
            }
          : {
              ...live,
              plainTextBindings: {
                ...live.plainTextBindings,
                OUTBOUND_POLICY_ID: 'foreign-resource-group',
              },
            },
    );

    await expect(removePlanOnlyBridge(fixture, path)).rejects.toThrow(
      /foreign backend-switch bridge/,
    );
    expect(fixture.mutations).toEqual([]);
  });

  it.each([
    ['traffic', 'an omitted binding namespace', ['namespace-state']],
    [
      'traffic',
      'duplicate namespace inventory',
      ['namespace-runner', 'namespace-state', 'namespace-state'],
    ],
    ['bridge', 'an omitted binding namespace', ['namespace-state']],
    [
      'bridge',
      'duplicate namespace inventory',
      ['namespace-runner', 'namespace-state', 'namespace-state'],
    ],
  ] as const)('rejects plan-only %s teardown with %s before mutation', async (path, _case, namespaceIds) => {
    const fixture = await committedPlanOnlyBridge();
    fixture.replaceNamespaces(namespaceIds);

    await expect(removePlanOnlyBridge(fixture, path)).rejects.toThrow(
      /foreign backend-switch bridge/,
    );
    expect(fixture.mutations).toEqual([]);
  });

  it('does not return a bridge snapshot when initial inspection omits a live binding namespace', async () => {
    const fixture = await committedPlanOnlyBridge();
    fixture.replaceNamespaces(['namespace-state']);

    await expect(
      fixture.subject.ensureBridge({
        priorSpec,
        targetSpec,
        prior,
        plan: fixture.plan,
        secrets: {
          deploymentIdentity: 'deployment-identity-secret-0001',
          maintenanceAdmin: 'maintenance-admin-secret-0000001',
        },
        fence,
      }),
    ).rejects.toThrow(/did not converge exactly/);
  });

  it('proves every authorized live binding namespace absent after bridge deletion', async () => {
    const plannedTargetSpec: DeploymentSpec = {
      ...targetSpec,
      durableObjectBindings: [
        ...targetSpec.durableObjectBindings,
        { name: 'STATE', className: 'State' },
      ],
    };
    const fixture = await committedPlanOnlyBridge(plannedTargetSpec);
    fixture.replaceNamespaceListings([
      ['namespace-runner', 'namespace-state'],
      ['namespace-runner'],
    ]);

    await expect(
      fixture.subject.removeSwitchBridge({
        prior,
        priorSpec,
        targetSpec: plannedTargetSpec,
        plan: fixture.plan,
        allowedArtifactVersions: [prior.artifactVersion],
        fence,
      }),
    ).resolves.toBeUndefined();
    expect(fixture.namespaceAbsenceChecks).toEqual([
      { namespaceId: 'namespace-runner', workerPresent: false },
      { namespaceId: 'namespace-state', workerPresent: false },
    ]);
  });

  it.each([
    'traffic',
    'bridge',
  ] as const)('does not let a plan authorize an unrecorded %s bridge version when a snapshot exists', async (path) => {
    const fixture = await committedPlanOnlyBridge();
    const live = fixture.live();
    const bridge: BridgeSnapshot = {
      scriptName: prior.scriptName,
      artifactVersion: 'persisted-bridge-v1',
      artifactDigest: fixture.plan.artifactDigest,
      databaseId: prior.databaseId,
      durableObjectBindings: live.durableObjectBindings,
      namespaceIds: prior.namespaceIds,
      secretNames: fixture.plan.secretNames,
      publicRouteAttached: true,
      stateOnly: false,
    };

    await expect(removePlanOnlyBridge(fixture, path, bridge)).rejects.toThrow(
      /foreign backend-switch bridge/,
    );
    expect(fixture.mutations).toEqual([]);
  });

  it.each([
    'traffic',
    'bridge',
  ] as const)('rejects a noncanonical persisted plan before %s mutation', async (path) => {
    const fixture = await committedPlanOnlyBridge();

    await expect(
      removePlanOnlyBridge(fixture, path, undefined, {
        ...fixture.plan,
        mutationDigest: '0'.repeat(64),
      }),
    ).rejects.toThrow(/plan differs from durable intent/);
    expect(fixture.mutations).toEqual([]);
  });

  it.each([
    'traffic',
    'bridge',
  ] as const)('rejects a noncanonical plan paired with a persisted snapshot before %s mutation', async (path) => {
    const fixture = await committedPlanOnlyBridge();
    const bridge = persistedBridgeSnapshot(fixture);

    await expect(
      removePlanOnlyBridge(fixture, path, bridge, {
        ...fixture.plan,
        mutationDigest: '0'.repeat(64),
      }),
    ).rejects.toThrow(/plan differs from durable intent/);
    expect(fixture.mutations).toEqual([]);
  });

  it.each([
    'traffic',
    'bridge',
  ] as const)('accepts an explicitly allowed restored-prior version with exact topology for %s teardown', async (path) => {
    const fixture = await committedPlanOnlyBridge();
    const planned = fixture.live();
    const restoredArtifactVersion = 'restored-prior-v3';
    const bridge: BridgeSnapshot = {
      scriptName: prior.scriptName,
      artifactVersion: 'persisted-bridge-v2',
      artifactDigest: fixture.plan.artifactDigest,
      databaseId: prior.databaseId,
      durableObjectBindings: planned.durableObjectBindings,
      namespaceIds: ['namespace-runner', 'namespace-state'],
      secretNames: fixture.plan.secretNames,
      publicRouteAttached: true,
      stateOnly: false,
    };
    fixture.replaceLive(exactRestoredWorker(restoredArtifactVersion));

    await expect(
      removePlanOnlyBridge(fixture, path, bridge, fixture.plan, [
        prior.artifactVersion,
        bridge.artifactVersion,
        restoredArtifactVersion,
      ]),
    ).resolves.toBeUndefined();
  });

  it.each([
    'traffic',
    'bridge',
  ] as const)('rejects fixed-variable drift in an allowed restored-prior version before %s mutation', async (path) => {
    const fixture = await committedPlanOnlyBridge();
    const planned = fixture.live();
    const restoredArtifactVersion = 'restored-prior-v3';
    const bridge: BridgeSnapshot = {
      scriptName: prior.scriptName,
      artifactVersion: 'persisted-bridge-v2',
      artifactDigest: fixture.plan.artifactDigest,
      databaseId: prior.databaseId,
      durableObjectBindings: planned.durableObjectBindings,
      namespaceIds: ['namespace-runner', 'namespace-state'],
      secretNames: fixture.plan.secretNames,
      publicRouteAttached: true,
      stateOnly: false,
    };
    const restored = exactRestoredWorker(restoredArtifactVersion);
    fixture.replaceLive({
      ...restored,
      plainTextBindings: {
        ...restored.plainTextBindings,
        FLEET_SCHEMA_VERSION: '999',
      },
    });

    await expect(
      removePlanOnlyBridge(fixture, path, bridge, fixture.plan, [
        prior.artifactVersion,
        bridge.artifactVersion,
        restoredArtifactVersion,
      ]),
    ).rejects.toThrow(/foreign backend-switch bridge/);
    expect(fixture.mutations).toEqual([]);
  });

  it('rejects a foreign same-name Worker before any ingress mutation', async () => {
    const mutations: string[] = [];
    const subject = provider({
      inspectControlWorker: async () => ({
        ...exactPriorWorker(),
        databaseIds: ['db-foreign'],
      }),
      deleteHostRouting: async () => {
        mutations.push('hosts');
      },
      detachCustomDomain: async () => {
        mutations.push('domain');
      },
      disableOrdinaryWorkerPublicAccess: async () => {
        mutations.push('subdomain');
      },
    });

    await expect(
      subject.removeSwitchTraffic({
        prior,
        ...trafficAuthority,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        routeHostname: targetSpec.routeHostname,
        routeTargets: [],
        fence,
      }),
    ).rejects.toThrow(/foreign backend-switch bridge/);
    expect(mutations).toEqual([]);
  });

  it('removes ingress from the exact persisted prior identity', async () => {
    let domains = [
      {
        id: prior.customDomain.id,
        hostname: prior.customDomain.hostname,
        service: prior.scriptName,
      },
    ];
    const mutations: string[] = [];
    const subject = provider({
      inspectControlWorker: async () => ({
        ...exactPriorWorker(),
      }),
      inspectOrdinaryWorkerFootprint: async () =>
        ordinaryFootprint({ customDomains: domains }),
      getHostRouting: async () => undefined,
      listCustomDomains: async () => domains,
      withMutationFence: async (_fence, operation) => operation(),
      deleteHostRouting: async () => {
        mutations.push('hosts');
      },
      detachCustomDomain: async (domainId) => {
        mutations.push(`domain:${domainId}`);
        domains = [];
      },
      disableOrdinaryWorkerPublicAccess: async () => {
        mutations.push('subdomain');
      },
    });

    await expect(
      subject.removeSwitchTraffic({
        prior,
        ...trafficAuthority,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        routeHostname: targetSpec.routeHostname,
        routeTargets: [],
        fence,
      }),
    ).resolves.toBeUndefined();
    expect(mutations).toEqual([
      'hosts',
      `domain:${prior.customDomain.id}`,
      'subdomain',
    ]);
  });

  it('converges an absent bridge retry without calling its subdomain endpoint', async () => {
    let serialized: string | undefined;
    let publicAccessDisables = 0;
    const subject = provider({
      inspectControlWorker: async () => undefined,
      inspectOrdinaryWorkerFootprint: async () =>
        ordinaryFootprint({
          scriptPresent: false,
          workersDevEnabled: undefined,
          previewUrlsEnabled: undefined,
        }),
      getHostRouting: async () => serialized,
      listCustomDomains: async () => [],
      withMutationFence: async (_fence, operation) => operation(),
      deleteHostRouting: async () => {
        serialized = undefined;
      },
      disableOrdinaryWorkerPublicAccess: async () => {
        publicAccessDisables += 1;
        throw new Error('404 Worker not found');
      },
    });

    await expect(
      subject.removeSwitchTraffic({
        prior,
        ...trafficAuthority,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        routeHostname: targetSpec.routeHostname,
        routeTargets: [],
        fence,
      }),
    ).resolves.toBeUndefined();
    expect(publicAccessDisables).toBe(0);
  });

  it.each([
    [
      'custom domain',
      ordinaryFootprint({
        customDomains: [
          {
            id: 'unexpected',
            hostname: 'unexpected.example.test',
            service: prior.scriptName,
          },
        ],
      }),
    ],
    [
      'zone route',
      ordinaryFootprint({
        zoneRoutes: [
          {
            zoneId: 'zone-id',
            routeId: 'route-id',
            pattern: 'unexpected.example.test/*',
          },
        ],
      }),
    ],
  ])('rejects unexpected %s ingress before mutation', async (_case, footprint) => {
    let mutations = 0;
    const subject = provider({
      inspectControlWorker: async () => exactPriorWorker(),
      inspectOrdinaryWorkerFootprint: async () => footprint,
      getHostRouting: async () => undefined,
      listCustomDomains: async () => [],
      deleteHostRouting: async () => {
        mutations += 1;
      },
      disableOrdinaryWorkerPublicAccess: async () => {
        mutations += 1;
      },
    });

    await expect(
      subject.removeSwitchTraffic({
        prior,
        ...trafficAuthority,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        routeHostname: targetSpec.routeHostname,
        routeTargets: [],
        fence,
      }),
    ).rejects.toThrow(/unexpected backend-switch ingress footprint/);
    expect(mutations).toBe(0);
  });

  it('rejects a same-host custom domain owned by another service', async () => {
    const subject = provider(
      {
        inspectControlWorker: async () => exactPriorWorker(),
        inspectOrdinaryWorkerFootprint: async () => ordinaryFootprint(),
        withMutationFence: async (_fence, operation) => operation(),
        deleteHostRouting: async () => {},
        getHostRouting: async () => undefined,
        listCustomDomains: async () => [
          {
            id: 'foreign-domain',
            hostname: prior.customDomain.hostname,
            service: 'foreign-worker',
          },
        ],
      },
      { releaseScriptName: () => 'candidate-v1' },
    );

    await expect(
      subject.removeSwitchTraffic({
        prior,
        ...trafficAuthority,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        routeHostname: targetSpec.routeHostname,
        routeTargets: [
          {
            scriptName: release.physicalScriptName,
            tenantTag: targetSpec.tenantTag,
            environment: targetSpec.environment,
            policyId: target.outboundPolicy.policyId,
            policyDigest: target.outboundPolicy.policyDigest,
            policyHosts: target.outboundPolicy.policyHosts,
            stateEgress: {
              resourceGroupId: target.outboundPolicy.policyId,
              stateScriptName: prior.scriptName,
              credentialDigest: target.stateEgressCredentialDigest as string,
            },
          },
        ],
        fence,
      }),
    ).rejects.toThrow(/foreign same-host custom domain/);
  });

  it('rejects a same-script host route whose serialized policy context differs', async () => {
    let deleted = false;
    const foreignPolicy = canonicalDeploymentEgressPolicy({
      policyId: target.outboundPolicy.policyId,
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      allowedHosts: ['foreign.example.com'],
    });
    const routeTarget = {
      scriptName: release.physicalScriptName,
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      policyId: target.outboundPolicy.policyId,
      policyDigest: target.outboundPolicy.policyDigest,
      policyHosts: target.outboundPolicy.policyHosts,
      stateEgress: {
        resourceGroupId: target.outboundPolicy.policyId,
        stateScriptName: prior.scriptName,
        credentialDigest: target.stateEgressCredentialDigest as string,
      },
    };
    const subject = provider({
      inspectControlWorker: async () => exactPriorWorker(),
      inspectOrdinaryWorkerFootprint: async () => ordinaryFootprint(),
      getHostRouting: async () =>
        JSON.stringify({
          ...routeTarget,
          policyDigest: foreignPolicy.policyDigest,
          policyHosts: foreignPolicy.policyHosts,
        }),
      deleteHostRouting: async () => {
        deleted = true;
      },
    });

    await expect(
      subject.removeSwitchTraffic({
        prior,
        ...trafficAuthority,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        routeHostname: targetSpec.routeHostname,
        routeTargets: [routeTarget],
        fence,
      }),
    ).rejects.toThrow(/outside the decommission snapshot/);
    expect(deleted).toBe(false);
  });

  it('resumes exact ambiguous route removal after precommit and committed response loss', async () => {
    const priorRoute = {
      scriptName: 'candidate-prior',
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      policyId: target.outboundPolicy.policyId,
      policyDigest: target.outboundPolicy.policyDigest,
      policyHosts: target.outboundPolicy.policyHosts,
      stateEgress: {
        resourceGroupId: target.outboundPolicy.policyId,
        stateScriptName: prior.scriptName,
        credentialDigest: target.stateEgressCredentialDigest as string,
      },
    };
    const nextPolicy = canonicalDeploymentEgressPolicy({
      policyId: target.outboundPolicy.policyId,
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      allowedHosts: ['next.example.com'],
    });
    const targetRoute = {
      ...priorRoute,
      scriptName: 'candidate-target',
      policyDigest: nextPolicy.policyDigest,
      policyHosts: nextPolicy.policyHosts,
    };
    let serialized: string | undefined = JSON.stringify(targetRoute);
    const durableTargets = [priorRoute, targetRoute];
    const observedAuthorities: unknown[] = [];
    let publicAccessDisables = 0;
    let attempt = 0;
    const subject = provider({
      inspectControlWorker: async () => exactPriorWorker(),
      inspectOrdinaryWorkerFootprint: async () => ordinaryFootprint(),
      getHostRouting: async () => serialized,
      withMutationFence: async (_fence, operation) => operation(),
      deleteHostRouting: async (_namespace, _hostname, authorities) => {
        observedAuthorities.push(authorities);
        attempt += 1;
        if (attempt === 1) throw new Error('provider failed before commit');
        serialized = undefined;
        if (attempt === 2)
          throw new Error('provider response lost after commit');
      },
      listCustomDomains: async () => [],
      disableOrdinaryWorkerPublicAccess: async () => {
        publicAccessDisables += 1;
      },
    });
    const input = {
      prior,
      ...trafficAuthority,
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      routeHostname: targetSpec.routeHostname,
      routeTargets: durableTargets,
      fence,
    };

    await expect(subject.removeSwitchTraffic(input)).rejects.toThrow(
      /before commit/,
    );
    expect(serialized).toBe(JSON.stringify(targetRoute));
    await expect(subject.removeSwitchTraffic(input)).rejects.toThrow(
      /response lost after commit/,
    );
    expect(serialized).toBeUndefined();
    await expect(subject.removeSwitchTraffic(input)).resolves.toBeUndefined();
    expect(observedAuthorities).toEqual([
      durableTargets,
      durableTargets,
      durableTargets,
    ]);
    expect(publicAccessDisables).toBe(1);
  });

  it('requires HOSTS, every ordinary ingress surface, workers.dev, and previews to be absent', async () => {
    const subject = provider({
      getHostRouting: async () => undefined,
      listCustomDomains: async () => [],
      inspectOrdinaryWorkerFootprint: async () => ({
        scriptPresent: true,
        workersDevEnabled: false,
        previewUrlsEnabled: false,
        customDomains: [],
        zoneRoutes: [
          {
            zoneId: 'zone-id',
            routeId: 'route-id',
            pattern: 'unexpected.example.test/*',
          },
        ],
      }),
    });

    await expect(
      subject.assertSwitchTrafficRemoved({
        prior,
        routeHostname: targetSpec.routeHostname,
      }),
    ).rejects.toThrow(/traffic remains/u);
  });

  it('rejects a foreign owner-checked candidate registry entry', async () => {
    const subject = provider(
      {
        inspectDispatchWorker: async () => undefined,
        getScriptInventory: async () => ({
          scriptName: 'candidate-v1',
          tenantTag: 'other',
          environment: targetSpec.environment,
          databaseId: prior.databaseId,
          routeHostname: targetSpec.routeHostname,
        }),
      },
      { releaseScriptName: () => 'candidate-v1' },
    );

    await expect(
      subject.removeSwitchRelease({
        prior,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        routeHostname: targetSpec.routeHostname,
        release,
        fence,
      }),
    ).rejects.toThrow(/foreign release registry/);
  });

  it('deletes a commit-unknown release only from its exact topology and adopts response-loss absence', async () => {
    let live:
      | Awaited<ReturnType<BackendSwitchApi['inspectDispatchWorker']>>
      | undefined = completeProviderBindingInspection({
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      artifactVersion: 'provider-v9',
      desiredSpecDigest: release.specDigest,
      schemaVersion: release.releaseSchemaVersion,
      databaseIds: [prior.databaseId],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      r2BucketBindings: [],
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
      plainTextBindings: {
        DEPLOYMENT_TENANT: targetSpec.tenantTag,
        FLEET_AUDIT_PROXY: 'remote-do',
        FLEET_ENVIRONMENT: targetSpec.environment,
        FLEET_INGRESS_CONTRACT: 'v1',
        FLEET_MAINTENANCE_CAPABILITIES: 'v1',
        FLEET_SCHEMA_VERSION: String(targetSpec.schemaVersion),
        FLEET_SPEC_DIGEST: release.specDigest,
      },
    });
    let inventory:
      | Awaited<ReturnType<BackendSwitchApi['getScriptInventory']>>
      | undefined = {
      scriptName: release.physicalScriptName,
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      databaseId: prior.databaseId,
      routeHostname: targetSpec.routeHostname,
    };
    let physicalDeletes = 0;
    let loseDeleteResponse = true;
    const subject = provider({
      inspectDispatchWorker: async () => live,
      getScriptInventory: async () => inventory,
      withMutationFence: async (_fence, operation) => operation(),
      revokeDispatchSecrets: async () => {},
      deleteDispatchWorker: async () => {
        physicalDeletes += 1;
        live = undefined;
        if (loseDeleteResponse) {
          loseDeleteResponse = false;
          throw new Error('provider response lost');
        }
      },
      deleteScriptInventory: async () => {
        inventory = undefined;
      },
    });
    const input = {
      prior,
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      routeHostname: targetSpec.routeHostname,
      release,
      fence,
    };

    await expect(subject.removeSwitchRelease(input)).rejects.toThrow(
      /provider response lost/,
    );
    await expect(subject.removeSwitchRelease(input)).resolves.toBeUndefined();
    expect(physicalDeletes).toBe(1);
    expect(live).toBeUndefined();
    expect(inventory).toBeUndefined();
  });

  it('rejects a commit-unknown release when any live topology edge differs', async () => {
    let revoked = false;
    const subject = provider({
      inspectDispatchWorker: async () =>
        completeProviderBindingInspection({
          tenantTag: targetSpec.tenantTag,
          environment: targetSpec.environment,
          artifactVersion: 'provider-v9',
          desiredSpecDigest: release.specDigest,
          schemaVersion: release.releaseSchemaVersion,
          databaseIds: [prior.databaseId],
          durableObjectBindings: [
            {
              name: 'FOREIGN',
              className: 'Foreign',
              namespaceId: 'namespace-foreign',
            },
          ],
          serviceBindings: [],
          queueProducerBindings: [],
          r2BucketBindings: [],
          secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
          plainTextBindings: {
            DEPLOYMENT_TENANT: targetSpec.tenantTag,
            FLEET_AUDIT_PROXY: 'remote-do',
            FLEET_ENVIRONMENT: targetSpec.environment,
            FLEET_INGRESS_CONTRACT: 'v1',
            FLEET_MAINTENANCE_CAPABILITIES: 'v1',
            FLEET_SCHEMA_VERSION: String(targetSpec.schemaVersion),
            FLEET_SPEC_DIGEST: release.specDigest,
          },
        }),
      getScriptInventory: async () => ({
        scriptName: release.physicalScriptName,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        databaseId: prior.databaseId,
        routeHostname: targetSpec.routeHostname,
      }),
      revokeDispatchSecrets: async () => {
        revoked = true;
      },
    });

    await expect(
      subject.removeSwitchRelease({
        prior,
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        routeHostname: targetSpec.routeHostname,
        release,
        fence,
      }),
    ).rejects.toThrow(/foreign backend-switch release/);
    expect(revoked).toBe(false);
  });

  it('rebuilds finalized ownership from the exact target and bridge', async () => {
    const subject = provider({});
    const bridge: BridgeSnapshot = {
      scriptName: prior.scriptName,
      artifactVersion: 'state-v3',
      artifactDigest: target.stateArtifactDigest,
      databaseId: prior.databaseId,
      durableObjectBindings: prior.durableObjectBindings,
      namespaceIds: prior.namespaceIds,
      secretNames: prior.secretNames,
      publicRouteAttached: false,
      stateOnly: true,
    };
    const currentRecord: FleetRecord = {
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      backend: 'workers-for-platforms',
      scriptName: prior.scriptName,
      databaseId: prior.databaseId,
      databaseName: prior.databaseName,
      schemaVersion: targetSpec.schemaVersion,
      artifactVersion: release.artifactVersion,
      desiredSpecDigest: release.specDigest,
      durableObjectBindings: [],
      routeHostname: targetSpec.routeHostname,
      phase: 'ready',
      platformResources: {
        auditQueueName: 'stale-audit-queue',
        maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: bridge.scriptName,
          artifactVersion: 'stale-version',
          artifactDigest: 'f'.repeat(64),
          plane: 'ordinary',
          dispatchNamespace: 'stale-dispatch',
          durableObjectBindings: [],
          namespaceIds: [],
        },
        outboundPolicy: target.outboundPolicy,
        sharedOutboundWorkerName: 'stale-outbound',
      },
      updatedAt: '2026-08-11T00:00:00.000Z',
    };

    const committed = await subject.commitFinalizedOwnership({
      currentRecord,
      bridge,
      target,
    });

    expect(committed.platformResources).toEqual({
      maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
      stateWorker: {
        scriptName: bridge.scriptName,
        artifactVersion: bridge.artifactVersion,
        artifactDigest: bridge.artifactDigest,
        plane: 'ordinary',
        durableObjectTag: target.stateDurableObjectTag,
        durableObjectBindings: bridge.durableObjectBindings,
        namespaceIds: bridge.namespaceIds,
      },
      outboundPolicy: target.outboundPolicy,
      sharedOutboundWorkerName: target.sharedOutboundWorkerName,
    });
  });

  it('rejects a bridge version outside the persisted artifact variants', async () => {
    const subject = provider({
      inspectControlWorker: async () =>
        completeProviderBindingInspection({
          artifactVersion: 'foreign-v9',
          databaseIds: [prior.databaseId],
          durableObjectBindings: prior.durableObjectBindings,
          serviceBindings: [],
          queueProducerBindings: [],
          kvNamespaceBindings: [],
          secretNames: prior.secretNames,
          plainTextBindings: {
            DEPLOYMENT_TENANT: targetSpec.tenantTag,
            FLEET_ENVIRONMENT: targetSpec.environment,
          },
          workersDevEnabled: false,
          previewUrlsEnabled: false,
          routeHostnames: [],
          zoneRoutes: [],
        }),
      listDurableObjectNamespaces: async () => prior.namespaceIds,
    });

    await expect(
      subject.removeSwitchBridge({
        prior,
        priorSpec,
        targetSpec,
        allowedArtifactVersions: [prior.artifactVersion],
        fence,
      }),
    ).rejects.toThrow(/foreign backend-switch bridge/);
  });

  it('rejects a reused snapshotted bridge version with different artifact identity', async () => {
    let revoked = false;
    const bridge: BridgeSnapshot = {
      scriptName: prior.scriptName,
      artifactVersion: 'bridge-v1',
      artifactDigest: '8'.repeat(64),
      databaseId: prior.databaseId,
      durableObjectBindings: prior.durableObjectBindings,
      namespaceIds: prior.namespaceIds,
      secretNames: prior.secretNames,
      publicRouteAttached: false,
      stateOnly: false,
    };
    const subject = provider(
      {
        inspectControlWorker: async () =>
          completeProviderBindingInspection({
            artifactVersion: bridge.artifactVersion,
            databaseIds: [prior.databaseId],
            durableObjectBindings: prior.durableObjectBindings,
            serviceBindings: [],
            queueProducerBindings: [],
            kvNamespaceBindings: [],
            secretNames: prior.secretNames,
            plainTextBindings: {
              DEPLOYMENT_TENANT: targetSpec.tenantTag,
              FLEET_ENVIRONMENT: targetSpec.environment,
              FLEET_ARTIFACT_DIGEST: '7'.repeat(64),
            },
            workersDevEnabled: false,
            previewUrlsEnabled: false,
            routeHostnames: [],
            zoneRoutes: [],
          }),
        listDurableObjectNamespaces: async () => prior.namespaceIds,
        revokeControlSecrets: async () => {
          revoked = true;
        },
      },
      {},
      () => profile,
    );

    await expect(
      subject.removeSwitchBridge({
        prior,
        priorSpec,
        bridge,
        targetSpec,
        allowedArtifactVersions: [bridge.artifactVersion],
        fence,
      }),
    ).rejects.toThrow(/foreign backend-switch bridge/);
    expect(revoked).toBe(false);
  });

  it.each([
    ['name', { id: prior.databaseId, name: 'foreign-database' }, 'acme', []],
    [
      'sentinel',
      { id: prior.databaseId, name: prior.databaseName },
      'other',
      [],
    ],
    [
      'attachment',
      { id: prior.databaseId, name: prior.databaseName },
      'acme',
      [{ scriptName: 'foreign', plane: 'ordinary' as const }],
    ],
  ])('rejects foreign database %s reuse before export', async (_case, database, owner, attachments) => {
    const subject = provider(
      {
        getDatabase: async () => ({ ...database, created: false }),
        listWorkerDatabaseAttachments: async () => attachments,
      },
      { readDeploymentIdentity: async () => owner },
    );

    await expect(
      subject.exportSwitchDatabase({ prior, targetSpec, fence }),
    ).rejects.toThrow(/database|sentinel|attached/);
  });
});

describe('backend switch provider response-loss recovery', () => {
  it('appends a platform profile after a disjoint persisted plain history without replaying platform tags', () => {
    const externalSpec: DeploymentSpec = {
      ...targetSpec,
      durableObjectMigrations: [],
    };
    const combinedHistory = [
      { tag: 'plain-v1', newClasses: ['LegacyRunner'] },
      ...profile.stateDurableObjectMigrations,
    ];
    const appendedProfile: ExternalPlatformProfile = {
      ...profile,
      stateDurableObjectMigrations: [
        ...profile.stateDurableObjectMigrations,
        { tag: 'v3', newClasses: ['Archive'] },
      ],
    };
    const bridge = {
      scriptName: externalSpec.scriptName,
      artifactVersion: 'state-v2',
      artifactDigest: '8'.repeat(64),
      databaseId: prior.databaseId,
      durableObjectBindings: prior.durableObjectBindings,
      namespaceIds: ['namespace-legacy', ...prior.namespaceIds],
      secretNames: [
        'DEPLOYMENT_IDENTITY_SECRET',
        'MAINTENANCE_ADMIN_SECRET',
        'OUTBOUND_PROXY_CREDENTIAL',
      ],
      publicRouteAttached: false,
      stateOnly: true,
    } as const;
    const currentRecord: FleetRecord = {
      tenantTag: externalSpec.tenantTag,
      environment: externalSpec.environment,
      backend: 'workers-for-platforms',
      scriptName: externalSpec.scriptName,
      databaseId: prior.databaseId,
      databaseName: prior.databaseName,
      schemaVersion: externalSpec.schemaVersion,
      artifactVersion: 'candidate-v1',
      desiredSpecDigest: deploymentSpecDigest(externalSpec),
      durableObjectTag: 'v2',
      durableObjectMigrationHistory: combinedHistory,
      durableObjectMigrationHistoryDigest:
        durableObjectMigrationHistoryDigest(combinedHistory),
      durableObjectBindings: prior.durableObjectBindings,
      applicationResources: [],
      applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
      routeHostname: externalSpec.routeHostname,
      phase: 'ready',
      updatedAt: '2026-08-11T00:00:00.000Z',
      outboundPolicy: target.outboundPolicy,
      platformTarget: target,
      platformResources: {
        maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: bridge.scriptName,
          artifactVersion: bridge.artifactVersion,
          artifactDigest: bridge.artifactDigest,
          plane: 'ordinary',
          durableObjectTag: 'v2',
          durableObjectBindings: bridge.durableObjectBindings,
          namespaceIds: bridge.namespaceIds,
        },
        outboundPolicy: target.outboundPolicy,
        sharedOutboundWorkerName: 'shared-outbound',
      },
      backendSwitchIntent: {
        kind: 'backend-switch',
        tenantTag: externalSpec.tenantTag,
        environment: externalSpec.environment,
        prior,
        targetSpecDigest: deploymentSpecDigest(externalSpec),
        targetApplication: { vars: [], secrets: [], r2Buckets: [] },
        target,
        rollbackUntil: '2026-08-12T00:00:00.000Z',
        subphase: 'finalized',
        bridge,
      },
    };
    const subject = provider({}, {}, () => appendedProfile);
    const nextTarget = subject.describeFinalizedBridgeTarget(
      externalSpec,
      currentRecord,
    );
    const plan = subject.describeFinalizedState({
      targetSpec: externalSpec,
      currentRecord,
      target: nextTarget,
    });

    expect(plan.priorDurableObjectTag).toBe('v2');
    expect(plan.targetDurableObjectTag).toBe('v3');
    expect(plan.durableObjectMigrations.map(({ tag }) => tag)).toEqual([
      'plain-v1',
      'v1',
      'v2',
      'v3',
    ]);
  });

  it('appends finalized state migrations from the persisted live tag and adopts a committed upload response loss', async () => {
    type Inspection = NonNullable<
      Awaited<ReturnType<BackendSwitchApi['inspectControlWorker']>>
    >;
    let currentProfile = profile;
    let namespaces = [...prior.namespaceIds];
    let live: Inspection = completeProviderBindingInspection({
      artifactVersion: prior.artifactVersion,
      databaseIds: [prior.databaseId],
      durableObjectBindings: prior.durableObjectBindings,
      serviceBindings: [],
      queueProducerBindings: [],
      r2BucketBindings: [],
      kvNamespaceBindings: [],
      secretNames: prior.secretNames,
      plainTextBindings: {
        DEPLOYMENT_TENANT: targetSpec.tenantTag,
        FLEET_ENVIRONMENT: targetSpec.environment,
      },
      workersDevEnabled: false,
      previewUrlsEnabled: false,
      routeHostnames: [],
      zoneRoutes: [],
    });
    const uploads: Parameters<BackendSwitchApi['uploadControlWorker']>[0][] =
      [];
    let loseFinalizedUploadResponse = false;
    const subject = provider(
      {
        inspectControlWorker: async () => live,
        withMutationFence: async (_fence, operation) => operation(),
        uploadControlWorker: async (upload) => {
          uploads.push(upload);
          if (uploads.length === 1) {
            namespaces = [...namespaces, 'namespace-state'];
          } else if (uploads.length === 3) {
            namespaces = [...namespaces, 'namespace-archive'];
          }
          live = completeProviderBindingInspection({
            artifactVersion: `bridge-v${uploads.length}`,
            databaseIds: upload.bindings.flatMap((binding) =>
              binding.type === 'd1' ? [String(binding.database_id)] : [],
            ),
            durableObjectBindings: upload.bindings.flatMap((binding) =>
              binding.type === 'durable_object_namespace'
                ? [
                    {
                      name: String(binding.name),
                      className: String(binding.class_name),
                      namespaceId: `namespace-${String(binding.class_name).toLowerCase()}`,
                    },
                  ]
                : [],
            ),
            serviceBindings: upload.bindings.flatMap((binding) =>
              binding.type === 'service'
                ? [
                    {
                      name: String(binding.name),
                      service: String(binding.service),
                      ...(binding.entrypoint
                        ? { entrypoint: String(binding.entrypoint) }
                        : {}),
                    },
                  ]
                : [],
            ),
            queueProducerBindings: [],
            r2BucketBindings: [],
            kvNamespaceBindings: [],
            secretNames: live.secretNames,
            plainTextBindings: Object.fromEntries(
              upload.bindings.flatMap((binding) =>
                binding.type === 'plain_text'
                  ? [[String(binding.name), String(binding.text)] as const]
                  : [],
              ),
            ),
            workersDevEnabled: false,
            previewUrlsEnabled: false,
            routeHostnames: [],
            zoneRoutes: [],
          });
          if (loseFinalizedUploadResponse) {
            loseFinalizedUploadResponse = false;
            throw new Error('finalized upload response lost after commit');
          }
          return live.artifactVersion;
        },
        putControlSecrets: async (_scriptName, values) => {
          live = completeProviderBindingInspection({
            ...live,
            secretNames: Object.keys(values).sort(),
          });
        },
        deleteControlSecrets: async () => {},
        listDurableObjectNamespaces: async () => namespaces,
        listCustomDomains: async () => [],
      },
      {},
      () => currentProfile,
    );
    const bridgePlan = subject.describeBridge({ priorSpec, targetSpec, prior });
    const bridge = await subject.ensureBridge({
      priorSpec,
      targetSpec,
      prior,
      plan: bridgePlan,
      secrets: {
        deploymentIdentity: 'deployment-identity-secret-0001',
        maintenanceAdmin: 'maintenance-admin-secret-0000001',
      },
      fence,
    });
    const initialTarget = subject.describeFinalizedBridgeTarget(targetSpec, {
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      backend: 'workers-for-platforms',
      scriptName: targetSpec.scriptName,
      databaseId: prior.databaseId,
      databaseName: prior.databaseName,
      schemaVersion: targetSpec.schemaVersion,
      artifactVersion: 'candidate-v1',
      desiredSpecDigest: deploymentSpecDigest(targetSpec),
      durableObjectTag: bridgePlan.targetDurableObjectTag,
      durableObjectMigrationHistory: bridgePlan.durableObjectMigrations,
      durableObjectMigrationHistoryDigest: durableObjectMigrationHistoryDigest(
        bridgePlan.durableObjectMigrations,
      ),
      durableObjectBindings: prior.durableObjectBindings,
      applicationResources: [],
      applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
      routeHostname: targetSpec.routeHostname,
      phase: 'ready',
      updatedAt: '2026-08-11T00:00:00.000Z',
      backendSwitchIntent: {
        kind: 'backend-switch',
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        prior,
        targetSpecDigest: deploymentSpecDigest(targetSpec),
        targetApplication: { vars: [], secrets: [], r2Buckets: [] },
        target,
        rollbackUntil: '2026-08-12T00:00:00.000Z',
        subphase: 'finalized',
        bridge: { ...bridge, stateOnly: true },
      },
      platformResources: {
        maintenanceCapabilityPublicKey: target.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: bridge.scriptName,
          artifactVersion: bridge.artifactVersion,
          artifactDigest: bridge.artifactDigest,
          plane: 'ordinary',
          durableObjectBindings: bridge.durableObjectBindings,
          namespaceIds: bridge.namespaceIds,
        },
        outboundPolicy: target.outboundPolicy,
        sharedOutboundWorkerName: 'shared-outbound',
      },
      platformTarget: target,
      outboundPolicy: target.outboundPolicy,
    });
    const state = await subject.ensureStateOnlyBridge({
      targetSpec,
      bridge,
      target: initialTarget,
      fence,
    });
    const currentRecord: FleetRecord = {
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      backend: 'workers-for-platforms',
      scriptName: targetSpec.scriptName,
      databaseId: prior.databaseId,
      databaseName: prior.databaseName,
      schemaVersion: targetSpec.schemaVersion,
      artifactVersion: 'candidate-v1',
      desiredSpecDigest: deploymentSpecDigest(targetSpec),
      durableObjectTag: bridgePlan.targetDurableObjectTag,
      durableObjectMigrationHistory: bridgePlan.durableObjectMigrations,
      durableObjectMigrationHistoryDigest: durableObjectMigrationHistoryDigest(
        bridgePlan.durableObjectMigrations,
      ),
      durableObjectBindings: prior.durableObjectBindings,
      applicationResources: [],
      applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
      routeHostname: targetSpec.routeHostname,
      phase: 'ready',
      updatedAt: '2026-08-11T00:00:00.000Z',
      backendSwitchIntent: {
        kind: 'backend-switch',
        tenantTag: targetSpec.tenantTag,
        environment: targetSpec.environment,
        prior,
        targetSpecDigest: deploymentSpecDigest(targetSpec),
        targetApplication: { vars: [], secrets: [], r2Buckets: [] },
        target: initialTarget,
        rollbackUntil: '2026-08-12T00:00:00.000Z',
        subphase: 'finalized',
        bridge: state,
      },
      platformResources: {
        maintenanceCapabilityPublicKey:
          initialTarget.maintenanceCapabilityPublicKey,
        stateWorker: {
          scriptName: state.scriptName,
          artifactVersion: state.artifactVersion,
          artifactDigest: state.artifactDigest,
          plane: 'ordinary',
          durableObjectBindings: state.durableObjectBindings,
          namespaceIds: state.namespaceIds,
        },
        outboundPolicy: initialTarget.outboundPolicy,
        sharedOutboundWorkerName: 'shared-outbound',
      },
      platformTarget: initialTarget,
      outboundPolicy: initialTarget.outboundPolicy,
    };
    const currentPlan = subject.describeFinalizedState({
      targetSpec,
      currentRecord,
      target: initialTarget,
    });
    const candidateOnlySpec: DeploymentSpec = {
      ...targetSpec,
      modules: [
        { name: 'candidate.js', content: 'export default { version: 2 }' },
      ],
    };
    const candidateOnlyTarget = subject.describeFinalizedBridgeTarget(
      candidateOnlySpec,
      currentRecord,
    );
    const candidateOnlyPlan = subject.describeFinalizedState({
      targetSpec: candidateOnlySpec,
      currentRecord,
      target: candidateOnlyTarget,
    });
    expect(candidateOnlyPlan).toEqual(currentPlan);
    await expect(
      subject.ensureFinalizedState({
        targetSpec: candidateOnlySpec,
        currentRecord,
        target: candidateOnlyTarget,
        plan: candidateOnlyPlan,
        fence,
      }),
    ).resolves.toMatchObject({ artifactVersion: state.artifactVersion });
    expect(uploads).toHaveLength(2);

    currentProfile = {
      ...profile,
      stateWorker: {
        ...profile.stateWorker,
        modules: [{ name: 'state.js', content: 'export class StateV2 {}' }],
      },
      stateDurableObjectMigrations: [
        ...profile.stateDurableObjectMigrations,
        { tag: 'v3', newClasses: ['Archive'] },
      ],
    };
    const nextTarget = subject.describeFinalizedBridgeTarget(
      targetSpec,
      currentRecord,
    );
    const plan = subject.describeFinalizedState({
      targetSpec,
      currentRecord,
      target: nextTarget,
    });
    loseFinalizedUploadResponse = true;
    const reconciled = await subject.ensureFinalizedState({
      targetSpec,
      currentRecord,
      target: nextTarget,
      plan,
      fence,
    });
    await expect(
      subject.ensureFinalizedState({
        targetSpec,
        currentRecord,
        target: nextTarget,
        plan,
        fence,
      }),
    ).resolves.toEqual(reconciled);

    expect(uploads).toHaveLength(3);
    expect(uploads[2]?.migrations).toMatchObject({
      old_tag: 'v2',
      new_tag: 'v3',
    });
    expect(plan.durableObjectMigrations.at(-1)).toMatchObject({
      tag: 'v3',
      newClasses: ['Archive'],
    });
    expect([...reconciled.namespaceIds].sort()).toEqual(
      ['namespace-archive', 'namespace-runner', 'namespace-state'].sort(),
    );
    expect(reconciled.artifactVersion).toBe('bridge-v3');
  });

  it('derives the bridge migration suffix from the committed live tag and reconverges metadata and secrets', async () => {
    type Inspection = NonNullable<
      Awaited<ReturnType<BackendSwitchApi['inspectControlWorker']>>
    >;
    let live: Inspection = completeProviderBindingInspection({
      artifactVersion: prior.artifactVersion,
      databaseIds: [prior.databaseId],
      durableObjectBindings: prior.durableObjectBindings,
      serviceBindings: [],
      queueProducerBindings: [],
      r2BucketBindings: [],
      kvNamespaceBindings: [],
      secretNames: prior.secretNames,
      plainTextBindings: {
        DEPLOYMENT_TENANT: targetSpec.tenantTag,
        FLEET_ENVIRONMENT: targetSpec.environment,
      },
      workersDevEnabled: false,
      previewUrlsEnabled: false,
      routeHostnames: [],
      zoneRoutes: [],
    });
    const uploads: Parameters<BackendSwitchApi['uploadControlWorker']>[0][] =
      [];
    let loseFirstUploadResponse = true;
    let deleted = false;
    const client: Partial<BackendSwitchApi> = {
      inspectControlWorker: async () => (deleted ? undefined : live),
      withMutationFence: async (_fence, operation) => operation(),
      uploadControlWorker: async (upload) => {
        uploads.push(upload);
        const bindings = upload.bindings;
        live = completeProviderBindingInspection({
          artifactVersion: `bridge-v${uploads.length}`,
          databaseIds: bindings.flatMap((binding) =>
            binding.type === 'd1' ? [String(binding.database_id)] : [],
          ),
          durableObjectBindings: bindings.flatMap((binding) =>
            binding.type === 'durable_object_namespace'
              ? [
                  {
                    name: String(binding.name),
                    className: String(binding.class_name),
                    namespaceId: `namespace-${String(binding.class_name).toLowerCase()}`,
                  },
                ]
              : [],
          ),
          serviceBindings: bindings.flatMap((binding) =>
            binding.type === 'service'
              ? [
                  {
                    name: String(binding.name),
                    service: String(binding.service),
                    ...(binding.entrypoint
                      ? { entrypoint: String(binding.entrypoint) }
                      : {}),
                  },
                ]
              : [],
          ),
          queueProducerBindings: [],
          r2BucketBindings: [],
          kvNamespaceBindings: [],
          secretNames: live.secretNames,
          plainTextBindings: Object.fromEntries(
            bindings.flatMap((binding) =>
              binding.type === 'plain_text'
                ? [[String(binding.name), String(binding.text)] as const]
                : [],
            ),
          ),
          workersDevEnabled: false,
          previewUrlsEnabled: false,
          routeHostnames: [],
          zoneRoutes: [],
        });
        if (loseFirstUploadResponse) {
          loseFirstUploadResponse = false;
          throw new Error('upload response lost after commit');
        }
        return live.artifactVersion;
      },
      putControlSecrets: async (_scriptName, secrets) => {
        live = completeProviderBindingInspection({
          ...live,
          secretNames: Object.keys(secrets).sort(),
        });
      },
      listDurableObjectNamespaces: async () => [
        'namespace-runner',
        'namespace-state',
      ],
      listCustomDomains: async () => [
        {
          id: prior.customDomain.id,
          hostname: prior.customDomain.hostname,
          service: prior.scriptName,
        },
      ],
      revokeControlSecrets: async () => {},
      deleteControlWorker: async () => {
        deleted = true;
      },
      hasDurableObjectNamespace: async () => !deleted,
    };
    const subject = provider(client, {}, () => profile);
    const plan = subject.describeBridge({ priorSpec, targetSpec, prior });
    const input = {
      priorSpec,
      targetSpec,
      prior,
      plan,
      secrets: {
        deploymentIdentity: 'deployment-identity-secret-0001',
        maintenanceAdmin: 'maintenance-admin-secret-0000001',
      },
      fence,
    };

    await expect(subject.ensureBridge(input)).rejects.toThrow(/response lost/);
    await expect(
      subject.recoverBridge({ priorSpec, targetSpec, prior, plan, fence }),
    ).resolves.toBeUndefined();
    live = { ...live, serviceBindings: [], secretNames: [] };
    const recovered = await subject.ensureBridge(input);

    expect(uploads[0]?.migrations).toMatchObject({
      old_tag: 'v1',
      new_tag: 'v2',
    });
    expect(uploads[1]?.migrations).toBeUndefined();
    expect(recovered.artifactVersion).toBe('bridge-v2');
    expect(recovered.secretNames).toEqual([...plan.secretNames]);
    expect(recovered.namespaceIds).toEqual(
      expect.arrayContaining([...prior.namespaceIds]),
    );
    await expect(
      subject.removeSwitchBridge({
        prior,
        priorSpec,
        targetSpec,
        plan,
        allowedArtifactVersions: [prior.artifactVersion],
        fence,
      }),
    ).resolves.toBeUndefined();
    expect(deleted).toBe(true);
  });

  it('reconciles an exact committed HOSTS target and rejects same-script serialized drift', async () => {
    let serialized: string | undefined;
    let puts = 0;
    let loseFirstPutResponse = true;
    const subject = provider(
      {
        getHostRouting: async () => serialized,
        withMutationFence: async (_fence, operation) => operation(),
        putHostRouting: async (_namespace, _hostname, desired) => {
          puts += 1;
          serialized = JSON.stringify(desired);
          if (loseFirstPutResponse) {
            loseFirstPutResponse = false;
            throw new Error('HOSTS response lost after commit');
          }
        },
      },
      {},
      () => profile,
    );
    const candidate = {
      physicalScriptName: 'acme-candidate-v1',
      specDigest: 'e'.repeat(64),
      artifactVersion: 'candidate-version-v1',
      releaseSchemaVersion: 1,
    };
    const bridge = {
      scriptName: prior.scriptName,
      artifactVersion: 'bridge-v1',
      artifactDigest: 'f'.repeat(64),
      databaseId: prior.databaseId,
      durableObjectBindings: prior.durableObjectBindings,
      namespaceIds: prior.namespaceIds,
      secretNames: [],
      publicRouteAttached: true,
      stateOnly: false,
    };
    const input = { targetSpec, candidate, bridge, target, fence };

    await expect(subject.publishCandidateHost(input)).rejects.toThrow(
      /response lost/,
    );
    await expect(subject.publishCandidateHost(input)).resolves.toBeUndefined();
    expect(puts).toBe(1);

    serialized = JSON.stringify({
      ...(JSON.parse(serialized as string) as Record<string, unknown>),
      extra: 'same-script-drift',
    });
    await expect(subject.publishCandidateHost(input)).rejects.toThrow(
      /complete durable target/,
    );
  });
});
