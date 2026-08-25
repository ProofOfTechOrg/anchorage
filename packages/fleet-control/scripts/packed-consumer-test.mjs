// SPDX-License-Identifier: Apache-2.0
//
// Publishing gate for @proofoftech/fleet-control, matching the breakwater and
// flowsafe packed-consumer tests. This package has four export entries, three
// of which are Workers entry points that no in-repo consumer imports through
// the package boundary, so `pnpm build` proves nothing about whether the
// published export map resolves. This packs the real tarball and consumes it.
//
// It runs publint --strict and attw --profile esm-only over the tarball, then
// typechecks and executes a consumer that reaches every export entry, so a
// missing dist file, a stale exports key, or a workspace: specifier that
// survived packing fails here rather than on the registry.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import {
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  realpath,
  rm,
  writeFile,
} from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const workspaceRoot = resolve(packageRoot, '../..');
const temporaryRoot = await mkdtemp(
  join(tmpdir(), 'fleet-control-packed-consumer-'),
);

function run(command, args, options = {}) {
  execFileSync(command, args, {
    cwd: options.cwd ?? packageRoot,
    encoding: 'utf8',
    stdio: options.stdio ?? 'pipe',
  });
}

try {
  const packedDirectory = join(temporaryRoot, 'packed');
  const extractedDirectory = join(temporaryRoot, 'extracted');
  const consumerDirectory = join(temporaryRoot, 'consumer');
  await mkdir(packedDirectory);
  await mkdir(extractedDirectory);
  await mkdir(consumerDirectory);

  const staleBuildArtifact = join(
    packageRoot,
    'dist',
    'stale-package-probe.js',
  );
  await mkdir(dirname(staleBuildArtifact), { recursive: true });
  await writeFile(
    staleBuildArtifact,
    'throw new Error("stale build output");\n',
  );
  run('pnpm', ['run', 'build']);
  run('pnpm', ['pack', '--pack-destination', packedDirectory]);

  const tarballs = (await readdir(packedDirectory)).filter((name) =>
    name.endsWith('.tgz'),
  );
  assert.equal(
    tarballs.length,
    1,
    'pnpm pack must produce exactly one tarball',
  );
  const tarball = join(packedDirectory, tarballs[0]);
  run('pnpm', [
    '--workspace-root',
    'exec',
    'publint',
    'run',
    tarball,
    '--strict',
  ]);
  run('pnpm', [
    '--workspace-root',
    'exec',
    'attw',
    tarball,
    '--profile',
    'esm-only',
  ]);
  run('tar', ['-xzf', tarball, '-C', extractedDirectory]);

  const packedPackageRoot = join(extractedDirectory, 'package');
  await assert.rejects(
    readFile(join(packedPackageRoot, 'dist', 'stale-package-probe.js')),
    { code: 'ENOENT' },
  );
  const manifest = JSON.parse(
    await readFile(join(packedPackageRoot, 'package.json'), 'utf8'),
  );

  // Unscoped, this name would be squattable, and a granular token scoped to
  // @proofoftech would 403 at publish time.
  assert.equal(manifest.name, '@proofoftech/fleet-control');
  assert.equal(
    manifest.private,
    undefined,
    'a private manifest never publishes',
  );
  assert.equal(manifest.publishConfig?.access, 'public');
  // pnpm keeps devDependencies in the packed manifest and consumers never
  // install them, so pin the installed set instead: anything new here is a new
  // transitive dependency for every control plane that consumes this package.
  assert.deepEqual(Object.keys(manifest.dependencies ?? {}).sort(), [
    '@proofoftech/flowsafe',
    'cloudflare',
    'p-queue',
  ]);
  assert.equal(manifest.dependencies.cloudflare, '7.0.0');
  assert.equal(manifest.dependencies['p-queue'], '9.3.3');
  // pnpm rewrites workspace: specifiers on pack; one that survived would be an
  // install-time failure for every consumer. The exact shape also matters on
  // its own: fleet-control pins one Flowsafe release deliberately, because a
  // consumer running two copies gets two sets of Durable Object classes and two
  // maintenance-receipt audiences.
  assert.match(
    manifest.dependencies['@proofoftech/flowsafe'],
    /^\d+\.\d+\.\d+$/,
  );
  assert.equal(manifest.repository?.directory, 'packages/fleet-control');
  for (const documentation of ['README.md', 'CHANGELOG.md', 'LICENSE']) {
    await readFile(join(packedPackageRoot, documentation), 'utf8');
  }

  const flowsafeDirectory = resolve(workspaceRoot, 'packages/flowsafe');
  await writeFile(
    join(consumerDirectory, 'package.json'),
    `${JSON.stringify(
      {
        name: 'fleet-control-packed-consumer',
        private: true,
        type: 'module',
        dependencies: {
          '@proofoftech/fleet-control': `file:${tarball}`,
          '@proofoftech/flowsafe': `link:${flowsafeDirectory}`,
        },
      },
      null,
      2,
    )}\n`,
  );
  // The override is what makes the link: specifier load-bearing. Without it the
  // consumer resolves @proofoftech/flowsafe from the registry, so this gate
  // would validate the packed artifact against the PREVIOUSLY published
  // Flowsafe rather than the one in this tree, and would go red on dev for the
  // whole window between the version bump and the release publishing.
  await writeFile(
    join(consumerDirectory, 'pnpm-workspace.yaml'),
    `packages:\n  - "."\noverrides:\n  "@proofoftech/flowsafe": ${JSON.stringify(
      `link:${flowsafeDirectory}`,
    )}\n`,
  );
  await writeFile(
    join(consumerDirectory, 'tsconfig.json'),
    `${JSON.stringify(
      {
        compilerOptions: {
          target: 'ES2022',
          lib: ['ES2022'],
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          strict: true,
          noEmit: true,
          skipLibCheck: true,
          types: [],
        },
        include: ['consumer.ts'],
      },
      null,
      2,
    )}\n`,
  );
  await writeFile(
    join(consumerDirectory, 'consumer.ts'),
    `import {
  ActiveRouteAttestationError,
  CloudflareProvisioningClient,
  D1CloudflareApiRateCoordinator,
  ProcessLocalCloudflareApiRateCoordinator,
  ProvisioningError,
  WorkersForPlatformsBackend,
  WorkersForPlatformsBackendSwitchProvider,
  WranglerLoopBackend,
  attestConvergedActiveRoute,
  attestFleetRecordActiveRoute,
  auditFleetDrift,
  decommissionDeployment,
  forceDecommissionDeployment,
  deploymentSpecDigest,
  deriveStateEgressCredential,
  fleetSettlementKey,
  provisionDeployment,
  validateDeploymentSpec,
  type ActiveRouteAttestation,
  type ActiveRouteExpectation,
  type CloudflareApiRateCoordinator,
  type DeploymentEgressPolicy,
  type FleetSettlementContext,
  type FleetSettlementEntry,
  type FleetSettlementHost,
  type FleetStateDatabase,
  type InitialExecutionFenceState,
  type ObservedActiveRoute,
  type SeedDeploymentIdentityOptions,
  type WorkersForPlatformsApi,
} from '@proofoftech/fleet-control';
import type { FleetDispatchEnv } from '@proofoftech/fleet-control/workers/dispatch';
import {
  createEgressProxyFetch,
  StateEgress,
  type FleetOutboundEnv,
} from '@proofoftech/fleet-control/workers/outbound';
import type { FleetAuditConsumerEnv } from '@proofoftech/fleet-control/workers/audit-consumer';

declare const dispatchEnv: FleetDispatchEnv;
declare const outboundEnv: FleetOutboundEnv;
declare const auditEnv: FleetAuditConsumerEnv;
declare const database: FleetStateDatabase;
declare const api: WorkersForPlatformsApi;
declare const policy: DeploymentEgressPolicy;
declare const coordinator: CloudflareApiRateCoordinator;
// The provisioning-time fence state a control plane has to choose. Named here
// because it is a REQUIRED provisionDeployment option: a consumer that cannot
// import its type cannot type its own provisioning wrapper.
declare const initialExecutionFenceState: InitialExecutionFenceState;
const lockedAtBirth: InitialExecutionFenceState = 'migration-locked';
// The options object seedDeploymentIdentity takes. A consumer implementing its
// own ProvisioningBackend has to name this type to declare that method, and it
// is where future provisioning context lands without another positional.
const seedOptions: SeedDeploymentIdentityOptions = {
  initialExecutionFenceState: lockedAtBirth,
};
// A consumer implementing its own ProvisioningBackend has to name the
// attestation it returns, and a host reading one has to name what it compares
// against, so both the result and the expectation are part of the surface.
declare const routeAttestation: ActiveRouteAttestation;
const routeExpectation: ActiveRouteExpectation = {
  specDigest: routeAttestation.specDigest,
  artifactVersion: routeAttestation.artifactVersion,
};
// The refusal's payload, which is what a host logs when a route cannot be
// attested; unusable without its type.
declare const observedRoute: ObservedActiveRoute;
// A settling host is written entirely against these types: the callback shape,
// the context it receives, and the entry it must switch on to interpret
// \`prior\`. A consumer that cannot name all three cannot implement one.
const settlementHost: FleetSettlementHost = {
  async settle(context: FleetSettlementContext): Promise<void> {
    const entry: FleetSettlementEntry = context.entry;
    void entry;
    void context.settlementKey;
    void context.alreadySettled;
    void context.attestation.physicalScriptName;
    void context.target.specDigest;
    void context.prior?.physicalScriptName;
  },
};

void ActiveRouteAttestationError;
void CloudflareProvisioningClient;
void D1CloudflareApiRateCoordinator;
void ProcessLocalCloudflareApiRateCoordinator;
void ProvisioningError;
void WorkersForPlatformsBackend;
void WorkersForPlatformsBackendSwitchProvider;
void WranglerLoopBackend;
void StateEgress;
void attestConvergedActiveRoute;
void attestFleetRecordActiveRoute;
void auditFleetDrift;
void createEgressProxyFetch;
void decommissionDeployment;
void forceDecommissionDeployment;
void deploymentSpecDigest;
void deriveStateEgressCredential;
void fleetSettlementKey;
void provisionDeployment;
void validateDeploymentSpec;
void dispatchEnv;
void outboundEnv;
void auditEnv;
void database;
void api;
void policy;
void coordinator;
void initialExecutionFenceState;
void lockedAtBirth;
void seedOptions;
void routeAttestation;
void routeExpectation;
void observedRoute;
void settlementHost;
`,
  );
  await writeFile(
    join(consumerDirectory, 'runtime.mjs'),
    `import assert from 'node:assert/strict';
import {
  ProcessLocalCloudflareApiRateCoordinator,
  ProvisioningError,
  WorkersForPlatformsBackend,
  deploymentSpecDigest,
} from '@proofoftech/fleet-control';

// Every export entry must load. The three Workers entries are default-export
// module objects that no in-repo consumer imports across the package boundary,
// so this is the only place a broken exports key surfaces before the registry.
const [dispatch, outbound, auditConsumer] = await Promise.all([
  import('@proofoftech/fleet-control/workers/dispatch'),
  import('@proofoftech/fleet-control/workers/outbound'),
  import('@proofoftech/fleet-control/workers/audit-consumer'),
]);
assert.equal(typeof dispatch.default.fetch, 'function');
assert.equal(typeof outbound.default.fetch, 'function');
assert.equal(typeof outbound.createEgressProxyFetch, 'function');
assert.equal(typeof outbound.StateEgress, 'function');
assert.equal(typeof auditConsumer.default.queue, 'function');

assert.equal(typeof deploymentSpecDigest, 'function');
assert.equal(typeof ProcessLocalCloudflareApiRateCoordinator, 'function');
assert.ok(new ProvisioningError('probe') instanceof Error);

// The trusted-configuration constructor must fail closed. This is the barrier
// that makes a published fleet-control inert without control-plane inputs, so
// the packed artifact has to keep it.
//
// Each guard is reached deliberately and matched by message. Passing {} would
// stop at the FIRST guard, leaving the state-egress check below unexercised,
// and a string second argument to assert.throws is the assertion's own message,
// not a matcher: it accepts any error, including an unrelated crash.
const complete = {
  client: {},
  hostRoutingKvId: 'kv-id',
  namespacedState: {
    dispatchNamespace: 'anchorage-dispatch',
    sharedOutboundWorkerName: 'anchorage-outbound',
    stateEgressRootSecret: 's'.repeat(32),
  },
};
assert.throws(
  () => new WorkersForPlatformsBackend({ ...complete, hostRoutingKvId: '' }),
  /hostRoutingKvId is required/,
);
for (const [field, value] of [
  ['dispatchNamespace', ''],
  ['sharedOutboundWorkerName', ''],
  ['stateEgressRootSecret', 's'.repeat(31)],
]) {
  assert.throws(
    () =>
      new WorkersForPlatformsBackend({
        ...complete,
        namespacedState: { ...complete.namespacedState, [field]: value },
      }),
    /dispatch namespace, shared outbound Worker, and 32-byte state-egress root secret/,
    \`namespacedState.\${field} must fail closed\`,
  );
}
// Without this the gate cannot tell a correctly validating constructor from one
// that throws on every input, including a complete configuration.
assert.ok(new WorkersForPlatformsBackend(complete));
`,
  );

  // --prefer-offline, NOT --offline. The sibling gates can use --offline
  // because every dependency of their consumer is a local link: or file:
  // path, so nothing needs registry metadata. This tarball carries two real
  // registry dependencies, and CI's pnpm cache restores the store without the
  // metadata mirror, so --offline fails there with ERR_PNPM_NO_OFFLINE_META
  // while passing on a developer machine whose mirror is warm.
  //
  // Resolution is still pinned: cloudflare and p-queue come from the packed
  // manifest as exact versions and flowsafe is overridden to the workspace
  // tree, so nothing floats. The age-gate flag matches the sibling gates.
  run(
    'pnpm',
    [
      'install',
      '--prefer-offline',
      '--ignore-scripts',
      '--config.minimum-release-age=0',
    ],
    { cwd: consumerDirectory },
  );
  // Prove the override actually took. Without this, deleting the overrides
  // block above leaves this gate green while the consumer resolves the
  // previously published flowsafe instead of the one being released with it.
  // Resolve the way the INSTALLED fleet-control does, from its own dist, and
  // assert it landed on the workspace tree. A check rooted at the consumer
  // instead finds the top-level link: entry and would pass even if
  // fleet-control's own resolution went elsewhere.
  //
  // This is the assertion that keeps the gate honest about WHICH flowsafe it
  // validated against. The overrides block above is defense in depth matching
  // the sibling gates; on pnpm 10 the link: dependency alone already wins, so
  // do not read the override as the thing making this true.
  const flowsafeFromFleetControl = (() => {
    try {
      return createRequire(
        join(
          consumerDirectory,
          'node_modules/@proofoftech/fleet-control/dist/index.js',
        ),
      ).resolve('@proofoftech/flowsafe/package.json');
    } catch (cause) {
      throw new Error(
        'the packed fleet-control cannot resolve @proofoftech/flowsafe at all',
        { cause },
      );
    }
  })();
  assert.equal(
    await realpath(dirname(flowsafeFromFleetControl)),
    await realpath(flowsafeDirectory),
    'the packed consumer must resolve the workspace flowsafe, not a registry copy',
  );

  run(join(packageRoot, 'node_modules/.bin/tsc'), ['-p', 'tsconfig.json'], {
    cwd: consumerDirectory,
  });
  run(process.execPath, ['runtime.mjs'], { cwd: consumerDirectory });

  process.stdout.write(
    'fleet-control packed consumer: manifest, all four export entries, types, and the fail-closed constructor passed\n',
  );
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
