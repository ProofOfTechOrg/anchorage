// SPDX-License-Identifier: Apache-2.0

import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import {
  lstat,
  mkdir,
  mkdtemp,
  readlink,
  realpath,
  rm,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  CONFORMANCE_CONTRACT_VERSION,
  validateConformanceConfig,
} from '../scripts/credentialed-conformance-config.mjs';
import type { CredentialedConformanceDependencies } from '../scripts/credentialed-conformance-runtime.mjs';
import {
  cleanupCredentialedDeployment,
  credentialedPlainWorkerDurableObjectBindings,
  credentialedWranglerVersionIds,
  loadCredentialedConformanceArtifacts,
  runCredentialedConformance,
  validateOperationalConformance,
} from '../scripts/credentialed-conformance-runtime.mjs';
import { plainWorkerIngressModule } from '../src/plain-worker-backend.js';
import {
  canonicalMaintenanceCapabilityPublicKey,
  FLEET_AUDIT_PROXY_CLASS_NAME,
  FLEET_AUDIT_PROXY_STATE_BINDING,
  validateExternalPlatformProfile,
} from '../src/platform-resources.js';
import type {
  DeploymentSecrets,
  DeploymentSpec,
  ExternalPlatformProfile,
} from '../src/types.js';
import {
  validateDeploymentSecrets,
  validateDeploymentSpec,
} from '../src/validation.js';

const scriptPath = fileURLToPath(
  new URL('../scripts/credentialed-conformance.mjs', import.meta.url),
);
const examplePath = fileURLToPath(
  new URL('../scripts/credentialed-conformance.example.json', import.meta.url),
);
const validConfig = JSON.parse(readFileSync(examplePath, 'utf8')) as Record<
  string,
  unknown
>;

function withoutPath(value: Record<string, unknown>, path: string) {
  const copy = structuredClone(value);
  const segments = path.split('.');
  let parent: unknown = copy;
  for (const segment of segments.slice(0, -1)) {
    parent = Array.isArray(parent)
      ? parent[Number(segment)]
      : (parent as Record<string, unknown>)[segment];
  }
  const final = segments.at(-1) as string;
  if (Array.isArray(parent)) {
    parent.splice(Number(final), 1);
  } else {
    delete (parent as Record<string, unknown>)[final];
  }
  return copy;
}

const maintenancePrivateKey = {
  kty: 'OKP',
  crv: 'Ed25519',
  alg: 'EdDSA',
  kid: 'fleet-maintenance-v1',
  x: 'Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo',
  d: 'gkXf8_b8kcCJxZ33fUYUac7yCsxZAxQXgsgPbwDpnlM',
} as const;
const maintenancePublicKey = canonicalMaintenanceCapabilityPublicKey(
  JSON.stringify({
    kty: maintenancePrivateKey.kty,
    crv: maintenancePrivateKey.crv,
    alg: maintenancePrivateKey.alg,
    kid: maintenancePrivateKey.kid,
    x: maintenancePrivateKey.x,
  }),
);
const wasmBytes = Uint8Array.from([0, 97, 115, 109, 1, 0, 0, 0]);
const wasmDescriptor = {
  name: 'hash.wasm',
  file: './artifacts/hash.wasm',
  sha256: '93a44bbb96c751218e4c00d479e4c14358122a389acca16205b1e4d0dc5f9476',
};

function operationalFixture(): {
  plans: Array<{
    initialSpec: DeploymentSpec;
    nextSpec: DeploymentSpec;
    initialProfile: ExternalPlatformProfile;
    nextProfile: ExternalPlatformProfile;
    secrets: DeploymentSecrets;
  }>;
  maintenanceCapabilityPrivateKey: typeof maintenancePrivateKey;
} {
  const spec: DeploymentSpec = {
    tenantTag: 'tenanta',
    environment: 'conformance',
    scriptName: 'tenanta-conformance',
    databaseName: 'tenanta-conformance',
    compatibilityDate: '2026-08-11',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'external',
    schemaVersion: 1,
    migrations: [{ version: 1, sql: 'CREATE TABLE proof (id TEXT)' }],
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: 'https://tenanta.example.test',
    routeHostname: 'tenanta.example.test',
  };
  const profile: ExternalPlatformProfile = {
    runtimeContractVersion: 1,
    backwardCompatibleWithRetainedReleases: true,
    maintenanceCapabilityPublicKey: maintenancePublicKey,
    maintenanceCapabilityPrivateKey: maintenancePrivateKey,
    stateWorker: {
      mainModule: 'state.js',
      modules: [{ name: 'state.js', content: 'export default {}' }],
      compatibilityDate: '2026-08-11',
    },
    stateDurableObjectMigrations: [],
    organizationEgressHosts: ['allowed.example.test'],
  };
  const secrets: DeploymentSecrets = {
    deploymentIdentity: 'deployment-identity-secret-value-0001',
    maintenanceAdmin: 'maintenance-admin-secret-value-00001',
  };
  return {
    plans: [
      {
        initialSpec: spec,
        nextSpec: { ...spec },
        initialProfile: profile,
        nextProfile: { ...profile },
        secrets,
      },
      {
        initialSpec: { ...spec, tenantTag: 'tenantb' },
        nextSpec: { ...spec, tenantTag: 'tenantb' },
        initialProfile: { ...profile },
        nextProfile: { ...profile },
        secrets: { ...secrets },
      },
    ],
    maintenanceCapabilityPrivateKey: maintenancePrivateKey,
  };
}

function runOperationalValidation(
  fixture: ReturnType<typeof operationalFixture>,
) {
  validateOperationalConformance({
    ...fixture,
    maintenanceCapabilityPublicKey: maintenancePublicKey,
    validateDeploymentSpec,
    validateDeploymentSecrets,
    validateExternalPlatformProfile,
    canonicalMaintenanceCapabilityPublicKey,
  });
}

function operationalPlan(
  fixture: ReturnType<typeof operationalFixture>,
  index: 0 | 1,
) {
  const plan = fixture.plans[index];
  if (!plan) throw new Error(`operational fixture has no plan ${index}`);
  return plan;
}

describe('credentialed conformance command', () => {
  it('accepts the documented strict v1 configuration', () => {
    expect(CONFORMANCE_CONTRACT_VERSION).toBe(1);
    expect(validateConformanceConfig(structuredClone(validConfig))).toEqual(
      validConfig,
    );
  });

  // The structural validator never looks at tenant-tag SHAPE, so a config can
  // pass stage one and still be rejected by validateDeploymentSpec in stage two,
  // after the operator has provisioned a scratch account. The example is what
  // docs/fleet-control.md tells them to start from, so it has to survive both.
  it('ships an example whose tenant tags survive production spec validation', () => {
    const tenantTags = (validConfig.tenantTags as string[]).map(
      (tenantTag) => ({
        tenantTag,
        environment: validConfig.environment as string,
        scriptName: `conformance-${tenantTag}-abc123`,
        databaseName: `conformance-${tenantTag}-abc123`,
        compatibilityDate: validConfig.compatibilityDate as string,
        mainModule: validConfig.mainModule as string,
        modules: [
          {
            name: validConfig.mainModule as string,
            content: 'export default {}',
          },
        ],
        authoredBy: 'external' as const,
        schemaVersion: validConfig.schemaVersion as number,
        migrations: [],
        durableObjectMigrations: [],
        durableObjectBindings: [],
        maintenanceBaseUrl: (
          validConfig.maintenanceBaseUrls as Record<string, string>
        )[tenantTag] as string,
        routeHostname: (validConfig.routeHostnames as Record<string, string>)[
          tenantTag
        ] as string,
      }),
    );
    for (const spec of tenantTags) {
      expect(() => validateDeploymentSpec(spec)).not.toThrow();
    }
  });

  it.each([
    'contractVersion',
    'tenantTags',
    'environment',
    'dispatchNamespace',
    'hostRoutingKvId',
    'sharedOutboundWorkerName',
    'auditQueueName',
    'workerBundle',
    'mainModule',
    'exportDirectory',
    'compatibilityDate',
    'schemaVersion',
    'migrations',
    'cpuLimitMs',
    'subrequestLimit',
    'maintenanceBaseUrls.tenanta',
    'maintenanceBaseUrls.tenantb',
    'routeHostnames.tenanta',
    'routeHostnames.tenantb',
    'plainWorkerRouteHostname',
    'durableObjectBindings',
    'durableObjectBindings.0.name',
    'durableObjectBindings.0.className',
    'application',
    'application.vars',
    'application.vars.0.name',
    'application.vars.0.value',
    'application.secrets',
    'application.r2Buckets',
    'application.r2Buckets.0.name',
    'applicationSecretBinding',
    'conformance',
    'conformance.httpPath',
    'conformance.webSocketPath',
    'conformance.allowedUpstreamUrl',
    'conformance.deniedUpstreamUrl',
    'conformance.deniedUpstreamStatus',
    'conformance.cpuOverLimitStatus',
    'conformance.applicationVariableName',
    'conformance.applicationVariableValue',
    'conformance.newDurableObjectBinding',
    'conformance.newDurableObjectBinding.name',
    'conformance.newDurableObjectBinding.className',
    'platformProfile',
    'platformProfile.runtimeContractVersion',
    'platformProfile.backwardCompatibleWithRetainedReleases',
    'platformProfile.maintenanceCapabilityPublicKey',
    'platformProfile.organizationEgressHosts',
    'platformProfile.stateProfiles',
    'platformProfile.stateProfiles.0.name',
    'platformProfile.stateProfiles.0.stateWorker',
    'platformProfile.stateProfiles.0.stateWorker.bundle',
    'platformProfile.stateProfiles.0.stateWorker.mainModule',
    'platformProfile.stateProfiles.0.stateWorker.compatibilityDate',
    'platformProfile.stateProfiles.0.stateDurableObjectMigrations',
    'platformProfile.stateProfiles.1.name',
    'platformProfile.stateProfiles.1.stateWorker',
    'platformProfile.stateProfiles.1.stateWorker.bundle',
    'platformProfile.stateProfiles.1.stateWorker.mainModule',
    'platformProfile.stateProfiles.1.stateWorker.compatibilityDate',
    'platformProfile.stateProfiles.1.stateDurableObjectMigrations',
  ])('rejects missing %s before provider construction', (path) => {
    expect(() =>
      validateConformanceConfig(withoutPath(validConfig, path)),
    ).toThrow();
  });

  it.each([
    null,
    42,
    '',
    'localhost',
    'https://plain.example.test',
    'PLAIN.example.test',
    'plain.example.test.',
    'plain.example.test:443',
    'plain.example.test:invalid',
    'plain.example.test/path',
    'operator@plain.example.test',
    'plain.example.test?probe=1',
    'plain.example.test#probe',
    ' plain.example.test',
  ])('rejects noncanonical ordinary probe hostname %j', (plainWorkerRouteHostname) => {
    expect(() =>
      validateConformanceConfig({ ...validConfig, plainWorkerRouteHostname }),
    ).toThrow(/plainWorkerRouteHostname/u);
  });

  it.each([
    'tenanta',
    'tenantb',
  ])('rejects an ordinary probe hostname that aliases %s', (tenantTag) => {
    const routeHostnames = validConfig.routeHostnames as Record<string, string>;
    const hostname = routeHostnames[tenantTag] as string;
    for (const alias of [hostname, hostname.toUpperCase(), `${hostname}.`]) {
      expect(() =>
        validateConformanceConfig({
          ...validConfig,
          plainWorkerRouteHostname: hostname,
          routeHostnames: { ...routeHostnames, [tenantTag]: alias },
        }),
      ).toThrow(/plainWorkerRouteHostname.*distinct/u);
    }
  });

  it('requires an append-only second state profile with the new class', () => {
    const replaced = structuredClone(validConfig);
    const profile = replaced.platformProfile as {
      stateProfiles: Array<{ stateDurableObjectMigrations: unknown[] }>;
    };
    const second = profile.stateProfiles[1];
    if (!second) throw new Error('example has no v2 state profile');
    second.stateDurableObjectMigrations = [
      { tag: 'v2', newSqliteClasses: ['ConformanceV2'] },
    ];
    expect(() => validateConformanceConfig(replaced)).toThrow(/append/u);
  });

  it.each([
    'tenant',
    'state',
  ] as const)('rejects unsafe or ambiguous %s Wasm descriptors', (role) => {
    const configuration = structuredClone(validConfig);
    const artifact =
      role === 'tenant'
        ? configuration
        : (
            configuration.platformProfile as {
              stateProfiles: Array<{ stateWorker: Record<string, unknown> }>;
            }
          ).stateProfiles[0]?.stateWorker;
    if (!artifact) throw new Error('example has no state artifact');
    artifact.auxiliaryWasm = [wasmDescriptor];
    expect(() => validateConformanceConfig(configuration)).not.toThrow();
    for (const invalid of [
      null,
      {},
      [{ ...wasmDescriptor, name: '../hash.wasm' }],
      [{ ...wasmDescriptor, name: 'hash.js' }],
      [{ ...wasmDescriptor, name: 'CON.wasm' }],
      [{ ...wasmDescriptor, file: '' }],
      [{ ...wasmDescriptor, sha256: 'invalid' }],
      [wasmDescriptor, wasmDescriptor],
    ]) {
      artifact.auxiliaryWasm = invalid;
      expect(() => validateConformanceConfig(configuration)).toThrow(
        /auxiliaryWasm/u,
      );
    }
    artifact.auxiliaryWasm = [wasmDescriptor];
    artifact.mainModule = wasmDescriptor.name;
    expect(() => validateConformanceConfig(configuration)).toThrow(
      /auxiliaryWasm/u,
    );
  });

  it('loads tenant and state Wasm modules with their upload names and unchanged bytes', async () => {
    const stateWasm = Uint8Array.from([
      0, 97, 115, 109, 1, 0, 0, 0, 0, 2, 1, 120,
    ]);
    const artifactPaths: string[] = [];
    const loaded = await loadCredentialedConformanceArtifacts({
      privateJwk: JSON.stringify(maintenancePrivateKey),
      publicJwk: maintenancePublicKey,
      canonicalizePublicKey: canonicalMaintenanceCapabilityPublicKey,
      workerBundle: './tenant.mjs',
      stateWorkerBundles: ['./state-v1.mjs', './state-v2.mjs'],
      auxiliaryWasm: [wasmDescriptor],
      stateWorkerAuxiliaryWasm: [
        undefined,
        [
          {
            name: 'state.wasm',
            file: './artifacts/state.wasm',
            sha256:
              'c50e86a2eac362a08107aabb3dfcba703070886e64810653a07b57c6da6a1307',
          },
        ],
      ],
      readArtifact: (path) => {
        artifactPaths.push(path);
        if (path === './artifacts/hash.wasm') return wasmBytes;
        if (path === './artifacts/state.wasm') return stateWasm;
        return new TextEncoder().encode(`export default '${path}'`);
      },
    });
    expect(loaded.workerAdditionalModules).toEqual([
      {
        name: 'hash.wasm',
        content: wasmBytes,
        contentType: 'application/wasm',
      },
    ]);
    expect(loaded.stateWorkerAdditionalModules).toEqual([
      [],
      [
        {
          name: 'state.wasm',
          content: stateWasm,
          contentType: 'application/wasm',
        },
      ],
    ]);
    expect(new TextDecoder().decode(loaded.workerContent)).toBe(
      "export default './tenant.mjs'",
    );
    expect(
      loaded.stateWorkerContents.map((bytes) =>
        new TextDecoder().decode(bytes),
      ),
    ).toEqual([
      "export default './state-v1.mjs'",
      "export default './state-v2.mjs'",
    ]);
    expect(artifactPaths).toEqual(
      expect.arrayContaining([
        './artifacts/hash.wasm',
        './artifacts/state.wasm',
      ]),
    );
  });

  it.each([
    ['digest mismatch', wasmBytes, '0'.repeat(64), /Wasm digest/u],
    [
      'invalid binary',
      Uint8Array.from([1, 2, 3]),
      '039058c6f2c0cb492c533b0a4d14ef77cc0f78abccced5287d84a1a2011cfb81',
      /Wasm format/u,
    ],
    ['non-binary content', 'not bytes', wasmDescriptor.sha256, /Wasm bytes/u],
  ])('rejects Wasm %s before provider construction', async (_case, content, sha256, error) => {
    let providerConstructions = 0;
    await expect(async () => {
      await loadCredentialedConformanceArtifacts({
        privateJwk: JSON.stringify(maintenancePrivateKey),
        publicJwk: maintenancePublicKey,
        canonicalizePublicKey: canonicalMaintenanceCapabilityPublicKey,
        workerBundle: './tenant.mjs',
        stateWorkerBundles: [],
        auxiliaryWasm: [{ ...wasmDescriptor, sha256 }],
        readArtifact: () => content,
      });
      providerConstructions += 1;
    }).rejects.toThrow(error);
    expect(providerConstructions).toBe(0);
  });

  it('rejects a malformed signer before nonexistent artifacts are read or a provider is constructed', async () => {
    const configuration = structuredClone(validConfig);
    configuration.workerBundle = '/does/not/exist/worker.mjs';
    const platformProfile = configuration.platformProfile as {
      stateProfiles: Array<{ stateWorker: { bundle: string } }>;
    };
    for (const profile of platformProfile.stateProfiles) {
      profile.stateWorker.bundle = '/does/not/exist/state-worker.mjs';
    }
    validateConformanceConfig(configuration);
    let artifactReads = 0;
    let providerConstructions = 0;
    await expect(async () => {
      await loadCredentialedConformanceArtifacts({
        privateJwk: '{}',
        publicJwk: maintenancePublicKey,
        canonicalizePublicKey: canonicalMaintenanceCapabilityPublicKey,
        workerBundle: configuration.workerBundle as string,
        stateWorkerBundles: platformProfile.stateProfiles.map(
          (profile) => profile.stateWorker.bundle,
        ),
        auxiliaryWasm: [wasmDescriptor],
        readArtifact: async (path) => {
          artifactReads += 1;
          return readFileSync(path);
        },
      });
      providerConstructions += 1;
    }).rejects.toThrow(/canonical Ed25519 private signing JWK/u);
    expect(artifactReads).toBe(0);
    expect(providerConstructions).toBe(0);
  });

  it('cryptographically rejects syntactically valid mismatched Ed25519 d and x before provider construction', async () => {
    const mismatchedPrivateKey = {
      ...maintenancePrivateKey,
      d: `A${maintenancePrivateKey.d.slice(1)}`,
    };
    let providerConstructions = 0;
    await expect(async () => {
      await loadCredentialedConformanceArtifacts({
        privateJwk: JSON.stringify(mismatchedPrivateKey),
        publicJwk: maintenancePublicKey,
        canonicalizePublicKey: canonicalMaintenanceCapabilityPublicKey,
        workerBundle: '/does/not/exist/worker.mjs',
        stateWorkerBundles: ['/does/not/exist/state-worker.mjs'],
        readArtifact: async (path) => readFileSync(path),
      });
      providerConstructions += 1;
    }).rejects.toThrow(/key-pair verification/u);
    expect(providerConstructions).toBe(0);
  });

  it.each([
    {
      failure: 'migration failure before intent',
      record: { phase: 'ready', desiredSpecDigest: 'initial' },
      expectedSpec: 'initial',
    },
    {
      failure: 'external migration failure',
      record: {
        phase: 'migrating',
        desiredSpecDigest: 'initial',
        migrationIntent: { targetSpecDigest: 'next' },
        pendingRelease: { specDigest: 'next' },
        pendingSpecDigest: 'next',
      },
      expectedSpec: 'next',
    },
    {
      failure: 'migration with a retained pending release',
      record: {
        phase: 'migrating',
        desiredSpecDigest: 'initial',
        pendingRelease: { specDigest: 'next' },
      },
      expectedSpec: 'next',
    },
    {
      failure: 'mutable migration failure',
      record: {
        phase: 'migrating',
        desiredSpecDigest: 'initial',
        pendingSpecDigest: 'next',
      },
      expectedSpec: 'next',
    },
    {
      failure: 'bounded decommission admission response loss',
      record: {
        phase: 'decommission-advancing',
        desiredSpecDigest: 'initial',
        migrationIntent: { targetSpecDigest: 'next' },
        decommissionIntent: {
          lifecyclePhase: 'migrating',
          identity: {
            mode: { kind: 'normal', requestedSpecDigest: 'next' },
          },
        },
      },
      expectedSpec: 'next',
    },
    {
      failure: 'bounded decommission transition response loss',
      record: {
        phase: 'decommission-advancing',
        desiredSpecDigest: 'next',
        decommissionIntent: {
          lifecyclePhase: 'traffic-removed',
          identity: {
            mode: { kind: 'normal', requestedSpecDigest: 'next' },
          },
        },
      },
      expectedSpec: 'next',
    },
  ])('dispatches cleanup with durable authority after $failure', async ({
    record,
    expectedSpec,
  }) => {
    const initialSpec = {
      tenantTag: 'tenant-a',
      environment: 'conformance',
      digest: 'initial',
    };
    const nextSpec = { ...initialSpec, digest: 'next' };
    const dispatched: string[] = [];
    await cleanupCredentialedDeployment(
      {
        initialSpec,
        nextSpec,
        currentSpec: nextSpec,
        secrets: {},
        store: { get: async () => record },
      },
      {
        backend: {},
        deploymentSpecDigest: (spec) => spec.digest,
        decommissionDeployment: async ({ spec }) => {
          dispatched.push(`decommission:${spec.digest}`);
        },
        cleanupDeploymentArtifacts: async ({ spec }) => {
          dispatched.push(`artifacts:${spec.digest}`);
        },
      },
    );
    expect(dispatched).toEqual([`decommission:${expectedSpec}`]);
  });

  it.each([
    {
      failure: 'missing migration target',
      record: { phase: 'migrating', desiredSpecDigest: 'initial' },
    },
    {
      failure: 'foreign migration target with a known pending release',
      record: {
        phase: 'migrating',
        desiredSpecDigest: 'initial',
        migrationIntent: { targetSpecDigest: 'foreign' },
        pendingRelease: { specDigest: 'next' },
      },
    },
    {
      failure: 'invalid migration target with a known fallback',
      record: {
        phase: 'migrating',
        desiredSpecDigest: 'initial',
        migrationIntent: { targetSpecDigest: '' },
        pendingSpecDigest: 'next',
      },
    },
    {
      failure: 'missing bounded decommission authority',
      record: { phase: 'decommission-advancing', desiredSpecDigest: 'next' },
    },
    {
      failure: 'foreign bounded decommission authority',
      record: {
        phase: 'decommission-advancing',
        desiredSpecDigest: 'next',
        decommissionIntent: {
          identity: {
            mode: { kind: 'normal', requestedSpecDigest: 'foreign' },
          },
        },
      },
    },
    {
      failure: 'invalid bounded decommission authority',
      record: {
        phase: 'decommission-advancing',
        desiredSpecDigest: 'next',
        decommissionIntent: { identity: { mode: { kind: 'normal' } } },
      },
    },
    {
      failure: 'backend-switch decommission authority',
      record: {
        phase: 'decommission-advancing',
        desiredSpecDigest: 'next',
        decommissionIntent: {
          identity: {
            mode: { kind: 'backend-switch', requestedSpecDigest: 'next' },
          },
        },
      },
    },
  ])('refuses cleanup before callbacks for $failure', async ({ record }) => {
    const initialSpec = {
      tenantTag: 'tenant-a',
      environment: 'conformance',
      digest: 'initial',
    };
    const nextSpec = { ...initialSpec, digest: 'next' };
    const callbacks: string[] = [];
    await expect(
      cleanupCredentialedDeployment(
        {
          initialSpec,
          nextSpec,
          currentSpec: nextSpec,
          secrets: {},
          store: { get: async () => record },
        },
        {
          backend: {},
          deploymentSpecDigest: (spec) => spec.digest,
          beforeCleanup: async () => {
            callbacks.push('before');
          },
          decommissionDeployment: async () => {
            callbacks.push('decommission');
          },
          cleanupDeploymentArtifacts: async () => {
            callbacks.push('artifacts');
          },
        },
      ),
    ).rejects.toThrow(/credentialed cleanup refuses/u);
    expect(callbacks).toEqual([]);
  });

  it('selects a durably settled next spec and fails closed on an unknown digest', async () => {
    const initialSpec = {
      tenantTag: 'tenant-a',
      environment: 'conformance',
      digest: 'initial',
    };
    const nextSpec = { ...initialSpec, digest: 'next' };
    let record = { phase: 'ready', desiredSpecDigest: nextSpec.digest };
    const removed: string[] = [];
    const deployment = {
      initialSpec,
      nextSpec,
      currentSpec: initialSpec,
      secrets: {},
      store: { get: async () => record },
    };
    const dependencies = {
      backend: {},
      deploymentSpecDigest: (spec: typeof initialSpec) => spec.digest,
      decommissionDeployment: async ({
        spec,
      }: {
        spec: typeof initialSpec;
      }) => {
        removed.push(spec.digest);
      },
      cleanupDeploymentArtifacts: async ({
        spec,
      }: {
        spec: typeof initialSpec;
      }) => {
        removed.push(spec.digest);
      },
    };
    await cleanupCredentialedDeployment(deployment, dependencies);
    expect(removed).toEqual(['next']);

    record = { ...record, desiredSpecDigest: 'unknown' };
    await expect(
      cleanupCredentialedDeployment(deployment, dependencies),
    ).rejects.toThrow(/refuses unknown desired specification digest/u);
    expect(removed).toEqual(['next']);
  });

  it.each([
    [
      'tenant',
      (fixture: ReturnType<typeof operationalFixture>) => {
        Object.assign(operationalPlan(fixture, 0).initialSpec, {
          tenantTag: 'INVALID',
        });
      },
    ],
    [
      'environment',
      (fixture: ReturnType<typeof operationalFixture>) => {
        Object.assign(operationalPlan(fixture, 0).initialSpec, {
          environment: '../production',
        });
      },
    ],
    [
      'compatibility date',
      (fixture: ReturnType<typeof operationalFixture>) => {
        Object.assign(operationalPlan(fixture, 0).nextSpec, {
          compatibilityDate: '11-08-2026',
        });
      },
    ],
    [
      'route',
      (fixture: ReturnType<typeof operationalFixture>) => {
        Object.assign(operationalPlan(fixture, 0).nextSpec, {
          routeHostname: 'https://invalid.example.test',
        });
      },
    ],
    [
      'migration history',
      (fixture: ReturnType<typeof operationalFixture>) => {
        Object.assign(operationalPlan(fixture, 1).nextSpec, {
          migrations: [{ version: 2, sql: 'CREATE TABLE proof (id TEXT)' }],
        });
      },
    ],
    [
      'profile',
      (fixture: ReturnType<typeof operationalFixture>) => {
        Object.assign(operationalPlan(fixture, 1).nextProfile, {
          runtimeContractVersion: 2,
        });
      },
    ],
    [
      'JWK',
      (fixture: ReturnType<typeof operationalFixture>) => {
        Object.assign(operationalPlan(fixture, 1).initialProfile, {
          maintenanceCapabilityPublicKey: '{}',
        });
      },
    ],
  ] as const)('rejects invalid operational %s before provider construction', (_label, mutate) => {
    const fixture = operationalFixture();
    mutate(fixture);
    let providerConstructions = 0;
    expect(() => {
      runOperationalValidation(fixture);
      providerConstructions += 1;
    }).toThrow();
    expect(providerConstructions).toBe(0);
  });

  // Positive control for the case above: an unmutated fixture must PASS. Without
  // it a fixture that is itself invalid makes every mutation "reject" for the
  // wrong reason and the negative suite proves nothing.
  it('accepts an unmutated operational fixture', () => {
    expect(() => runOperationalValidation(operationalFixture())).not.toThrow();
  });

  it('parses nonempty unique Wrangler version-ID observations independently', () => {
    expect(
      credentialedWranglerVersionIds(
        JSON.stringify({
          result: [{ id: 'version-b' }, { version_id: 'version-a' }],
        }),
      ),
    ).toEqual(['version-a', 'version-b']);
    expect(
      credentialedWranglerVersionIds(
        JSON.stringify([{ id: 'version-window-new' }]),
      ),
    ).toEqual(['version-window-new']);
    for (const output of [
      JSON.stringify([]),
      JSON.stringify([{ id: 'version-a' }, { id: 'version-a' }]),
      JSON.stringify([{ id: '' }]),
    ]) {
      expect(() => credentialedWranglerVersionIds(output)).toThrow();
    }
  });

  it('exports every live trusted-state migration class from the plain Worker entrypoint', () => {
    const durableObjectMigrations = [
      {
        tag: 'v1',
        newSqliteClasses: ['Maintenance', 'FlowsafeFleetAuditProxy'],
      },
    ];
    const durableObjectBindings = credentialedPlainWorkerDurableObjectBindings(
      [{ name: 'MAINTENANCE', className: 'Maintenance' }],
      durableObjectMigrations,
      {
        name: FLEET_AUDIT_PROXY_STATE_BINDING,
        className: FLEET_AUDIT_PROXY_CLASS_NAME,
      },
    );
    const spec: DeploymentSpec = {
      tenantTag: 'tenanta',
      environment: 'conformance',
      scriptName: 'plain-conformance',
      databaseName: 'plain-conformance',
      compatibilityDate: '2026-08-11',
      mainModule: 'state.js',
      modules: [{ name: 'state.js', content: 'export default {}' }],
      authoredBy: 'platform',
      schemaVersion: 1,
      migrations: [],
      durableObjectMigrations,
      durableObjectBindings,
      maintenanceBaseUrl: 'https://plain-conformance.workers.dev',
      routeHostname: 'plain-conformance.example.test',
    };

    expect(durableObjectBindings).toEqual([
      { name: 'MAINTENANCE', className: 'Maintenance' },
      {
        name: 'FLEET_AUDIT_PROXY_OBJECT',
        className: 'FlowsafeFleetAuditProxy',
      },
    ]);
    expect(plainWorkerIngressModule(spec).content).toContain(
      'export { FlowsafeFleetAuditProxy, Maintenance } from "./state.js";',
    );
    expect(() =>
      credentialedPlainWorkerDurableObjectBindings(
        [],
        [
          {
            tag: 'v1',
            newSqliteClasses: ['UnknownStateClass'],
          },
        ],
        {
          name: FLEET_AUDIT_PROXY_STATE_BINDING,
          className: FLEET_AUDIT_PROXY_CLASS_NAME,
        },
      ),
    ).toThrow(/missing \[UnknownStateClass\]/u);
  });

  it('runs every mandatory probe in release order and returns only asserted truth', async () => {
    const deployments = [{ id: 'a' }, { id: 'b' }];
    const calls: string[] = [];
    const operation = (name: string) => async (deployment?: { id: string }) => {
      calls.push(deployment ? `${name}:${deployment.id}` : name);
    };
    const result = await runCredentialedConformance(
      { deployments },
      {
        provisionV1: operation('provision-v1'),
        probeV1: operation('probe-v1'),
        assertTenantIsolation: async () => {
          calls.push('tenant-isolation');
        },
        activateV2: operation('activate-v2'),
        migrateV2: operation('migrate-v2'),
        probeV2: operation('probe-v2'),
        completeFlowSafe: operation('flowsafe-resume'),
        rollback: operation('rollback'),
        proveNonemptyDecommission: operation('nonempty-refusal'),
        decommission: operation('decommission'),
        provePlainWorkerSecretVersionChurnTeardown: operation(
          'plain-worker-version-churn-teardown',
        ),
        assertZeroResiduals: operation('zero-residuals'),
        cleanup: operation('cleanup'),
      },
    );
    expect(calls).toEqual([
      'provision-v1:a',
      'probe-v1:a',
      'provision-v1:b',
      'probe-v1:b',
      'tenant-isolation',
      'activate-v2',
      'migrate-v2:a',
      'probe-v2:a',
      'flowsafe-resume:a',
      'rollback:a',
      'migrate-v2:b',
      'probe-v2:b',
      'flowsafe-resume:b',
      'rollback:b',
      'nonempty-refusal:a',
      'decommission:a',
      'decommission:b',
      'plain-worker-version-churn-teardown',
      'zero-residuals',
      'cleanup:a',
      'cleanup:b',
    ]);
    expect(Object.values(result).every((value) => value === true)).toBe(true);
    expect(result.plainWorkerSecretVersionChurnTeardown).toBe(true);
  });

  it('cannot return success after a mandatory failure and still cleans both deployments', async () => {
    const deployments = [{ id: 'a' }, { id: 'b' }];
    const calls: string[] = [];
    const success = async () => {};
    await expect(
      runCredentialedConformance(
        { deployments },
        {
          provisionV1: success,
          probeV1: success,
          assertTenantIsolation: success,
          activateV2: success,
          migrateV2: async (deployment: { id: string }) => {
            calls.push(`migrate:${deployment.id}`);
            throw new Error('mandatory migration failed');
          },
          probeV2: success,
          completeFlowSafe: success,
          rollback: success,
          proveNonemptyDecommission: success,
          decommission: success,
          provePlainWorkerSecretVersionChurnTeardown: success,
          assertZeroResiduals: success,
          cleanup: async (deployment: { id: string }) => {
            calls.push(`cleanup:${deployment.id}`);
          },
        },
      ),
    ).rejects.toThrow(/mandatory migration failed/);
    expect(calls).toEqual(['migrate:a', 'cleanup:a', 'cleanup:b']);
  });

  it.each([
    'completeFlowSafe',
    'provePlainWorkerSecretVersionChurnTeardown',
  ] as const)('rejects a skipped mandatory %s operation before running any probe', async (missingOperation) => {
    const calls: string[] = [];
    const dependencies = Object.fromEntries(
      [
        'provisionV1',
        'probeV1',
        'assertTenantIsolation',
        'activateV2',
        'migrateV2',
        'probeV2',
        'completeFlowSafe',
        'rollback',
        'proveNonemptyDecommission',
        'decommission',
        'provePlainWorkerSecretVersionChurnTeardown',
        'assertZeroResiduals',
        'cleanup',
      ].map((name) => [name, async () => calls.push(name)]),
    );
    delete dependencies[missingOperation];
    await expect(
      runCredentialedConformance(
        { deployments: [{ id: 'a' }, { id: 'b' }] },
        dependencies as unknown as CredentialedConformanceDependencies<{
          id: string;
        }>,
      ),
    ).rejects.toThrow(new RegExp(`requires ${missingOperation}`, 'u'));
    expect(calls).toEqual([]);
  });

  it.each([
    'missing',
    'file',
    'symlink',
    'relative',
  ] as const)('checks the %s export root before provider access', async (kind) => {
    const root = await realpath(
      await mkdtemp(join(tmpdir(), 'fleet-conformance-export-root-')),
    );
    try {
      const directory = join(root, 'exports');
      const target = join(root, 'retained-exports');
      if (kind === 'file') await writeFile(directory, 'retained file');
      if (kind === 'symlink') {
        await mkdir(target, { mode: 0o700 });
        await symlink(target, directory);
      }
      if (kind === 'relative') await mkdir(directory, { mode: 0o700 });

      const plan = operationalFixture().plans[0];
      const candidate = plan?.initialSpec.modules[0];
      const state = plan?.initialProfile.stateWorker.modules[0];
      if (!candidate || !state) throw new Error('missing artifact fixture');
      const candidatePath = join(root, 'candidate.mjs');
      const statePath = join(root, 'state.mjs');
      await writeFile(candidatePath, candidate.content);
      await writeFile(statePath, state.content);
      const configuration = structuredClone(validConfig);
      configuration.workerBundle = candidatePath;
      configuration.exportDirectory =
        kind === 'relative' ? 'exports' : directory;
      const profile = configuration.platformProfile as {
        maintenanceCapabilityPublicKey: string;
        stateProfiles: Array<{ stateWorker: { bundle: string } }>;
      };
      profile.maintenanceCapabilityPublicKey = maintenancePublicKey;
      for (const entry of profile.stateProfiles) {
        entry.stateWorker.bundle = statePath;
      }
      const configPath = join(root, 'operator.json');
      await writeFile(configPath, JSON.stringify(configuration));

      const providerStarted = 'CONFORMANCE_PROVIDER_STARTED';
      const preload = `import { writeSync } from 'node:fs';
globalThis.fetch = () => {
  writeSync(2, ${JSON.stringify(providerStarted)});
  process.exit(86);
};`;
      const result = spawnSync(
        process.execPath,
        [
          '--import',
          `data:text/javascript,${encodeURIComponent(preload)}`,
          scriptPath,
        ],
        {
          cwd: root,
          encoding: 'utf8',
          timeout: 15_000,
          maxBuffer: 64 * 1024,
          env: {
            FLEET_CONFORMANCE_CONFIG: configPath,
            CLOUDFLARE_API_TOKEN: 'conformance-test-token',
            CLOUDFLARE_ACCOUNT_ID: 'conformance-test-account',
            FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK: JSON.stringify(
              maintenancePrivateKey,
            ),
            FLEET_STATE_EGRESS_ROOT_SECRET: 'conformance-test-root-secret-0001',
            FLEET_CONFORMANCE_APPLICATION_SECRET:
              'conformance-test-application-secret',
          },
        },
      );
      expect(result.error).toBeUndefined();
      if (kind === 'relative') {
        expect(result.status, result.stderr).toBe(86);
        expect(result.stderr).toContain(providerStarted);
        expect((await lstat(directory)).isDirectory()).toBe(true);
      } else {
        expect(result.status, result.stderr).toBe(1);
        expect(result.stderr).toContain(
          'conformance exportDirectory must be an existing canonical directory',
        );
        expect(result.stderr).not.toContain(providerStarted);
        if (kind === 'missing') {
          await expect(lstat(directory)).rejects.toMatchObject({
            code: 'ENOENT',
          });
        } else if (kind === 'file') {
          expect((await lstat(directory)).isFile()).toBe(true);
          expect(readFileSync(directory, 'utf8')).toBe('retained file');
        } else {
          expect((await lstat(directory)).isSymbolicLink()).toBe(true);
          expect(await readlink(directory)).toBe(target);
        }
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('fails before loading the client or making a request without credentials', () => {
    const {
      FLEET_CONFORMANCE_CONFIG: _config,
      CLOUDFLARE_API_TOKEN: _token,
      CLOUDFLARE_ACCOUNT_ID: _account,
      FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK: _maintenancePrivateJwk,
      FLEET_STATE_EGRESS_ROOT_SECRET: _stateEgressRootSecret,
      FLEET_CONFORMANCE_APPLICATION_SECRET: _applicationSecret,
      ...environment
    } = process.env;
    const result = spawnSync(process.execPath, [scriptPath], {
      encoding: 'utf8',
      env: environment,
    });
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain(
      'FLEET_CONFORMANCE_CONFIG, CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK, FLEET_STATE_EGRESS_ROOT_SECRET, and FLEET_CONFORMANCE_APPLICATION_SECRET are required',
    );
    expect(result.stderr).not.toContain('ERR_MODULE_NOT_FOUND');
  });
});
