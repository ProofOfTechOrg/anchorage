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
  CloudflareApiPlainWorkerBackend,
  CloudflareProvisioningClient,
  DecommissionAdvanceCapabilityError,
  DecommissionAdvanceRestartError,
  DecommissionAdvanceTokenDeploymentError,
  DecommissionAdvanceTokenError,
  DecommissionAdvanceTokenFutureError,
  DecommissionAdvanceTokenOperationError,
  D1CloudflareApiRateCoordinator,
  FileSystemDatabaseExportStore,
  PlainWorkerBackend,
  ProcessLocalCloudflareApiRateCoordinator,
  ProvisioningError,
  WorkersForPlatformsBackend,
  WorkersForPlatformsBackendSwitchProvider,
  WranglerLoopBackend,
  attestConvergedActiveRoute,
  attestFleetRecordActiveRoute,
  auditFleetDrift,
  advanceDecommissionDeployment,
  decommissionDeployment,
  forceDecommissionDeployment,
  deploymentSpecDigest,
  deriveStateEgressCredential,
  fleetSettlementKey,
  provisionDeployment,
  validateDeploymentSpec,
  type ActiveRouteAttestation,
  type ActiveRouteExpectation,
  type AttestConvergedActiveRouteOptions,
  type AdvanceDecommissionDeploymentOptions,
  type CloudflareApiPlainWorkerBackendOptions,
  type CloudflareApiRateCoordinator,
  type DeploymentEgressPolicy,
  type DeploymentSpec,
  type DecommissionAdvanceIntent,
  type DecommissionAdvanceAction,
  type DecommissionAdvanceCapability,
  type DecommissionAdvanceResult,
  type DecommissionAdvanceToken,
  type DecommissionAdvanceTokenClassification,
  type DecommissionAttachmentProgress,
  type DecommissionAttachmentPurpose,
  type DecommissionAttachmentScanEvidence,
  type DecommissionAttachmentScanInput,
  type DecommissionAttachmentScanResult,
  type DecommissionBlockedAttachment,
  type DecommissionIntentCommon,
  type DecommissionOperationIdentity,
  type DecommissionOperationMode,
  type DecommissionRecordIdentity,
  type DatabaseExport,
  type DatabaseExportIntegrity,
  type DatabaseExportReceiptIdentity,
  type DurableDatabaseExportStore,
  type ExternalMutationFence,
  type FleetRecord,
  type FleetStateStore,
  type FleetSettlementContext,
  type FleetSettlementEntry,
  type FleetSettlementHost,
  type FleetStateDatabase,
  type InitialExecutionFenceState,
  type NormalDecommissionLifecyclePhase,
  type ObservedActiveRoute,
  type PlainWorkerBackendOptions,
  type PlainWorkerCleanupOutcome,
  type PlainWorkerCustomDomain,
  type PlainWorkerDatabaseExportResult,
  type PlainWorkerDatabaseInventoryEntry,
  type PlainWorkerDeploymentStatus,
  type PlainWorkerMutationOutcome,
  type PlainWorkerProvisioningApi,
  type PlainWorkerRouteApi,
  type PlainWorkerUploadIntent,
  type PlainWorkerUploadIntentBase,
  type PlainWorkerUploadOutcome,
  type PlainWorkerVersionBinding,
  type PlainWorkerVersionDetail,
  type PlainWorkerVersionSummary,
  type ProvisioningBackend,
  type SeedDeploymentIdentityOptions,
  type WorkersForPlatformsApi,
} from '@proofoftech/fleet-control';
// @ts-expect-error R1's client friend is package-private, not a root API.
import { advanceCloudflareWorkerAttachmentScan } from '@proofoftech/fleet-control';
// @ts-expect-error R1 provider attachments stay behind decommission types.
import type { WorkerAttachment } from '@proofoftech/fleet-control';
// @ts-expect-error R1 scan targets stay package-private.
import type { WorkerAttachmentScanTarget } from '@proofoftech/fleet-control';
// @ts-expect-error R1 progress stays package-private.
import type { WorkerAttachmentScanProgress } from '@proofoftech/fleet-control';
// @ts-expect-error R1 scan inputs stay package-private.
import type { WorkerAttachmentScanInput } from '@proofoftech/fleet-control';
// @ts-expect-error R1 scan chunks stay package-private.
import type { WorkerAttachmentScanChunk } from '@proofoftech/fleet-control';
// @ts-expect-error R1 provider context stays package-private.
import type { CloudflareWorkerAttachmentScanContext } from '@proofoftech/fleet-control';
// @ts-expect-error R1 progress errors stay package-private.
import { CloudflareAttachmentScanProgressError } from '@proofoftech/fleet-control';
// @ts-expect-error R1 drift errors stay package-private.
import { CloudflareAttachmentScanDriftError } from '@proofoftech/fleet-control';
// @ts-expect-error the pure mapper is a deep package-private seam.
import { mapDecommissionAttachmentScanChunk } from '@proofoftech/fleet-control';
// @ts-expect-error raw action parsing is package-private.
import { decommissionAdvanceActionFromUnknown } from '@proofoftech/fleet-control';
// @ts-expect-error token parsing is package-private.
import { parseDecommissionAdvanceToken } from '@proofoftech/fleet-control';
// @ts-expect-error token classification is package-private.
import { classifyDecommissionAdvanceToken } from '@proofoftech/fleet-control';
// @ts-expect-error intent normalization is package-private.
import { normalizeDecommissionAdvanceIntent } from '@proofoftech/fleet-control';
// @ts-expect-error provider budget validation is package-private.
import { assertWorkerAttachmentProviderRequestBudget } from '@proofoftech/fleet-control';
// @ts-expect-error one-step R2 deletion is package-private.
import { advanceApplicationR2Deletion } from '@proofoftech/fleet-control';
// @ts-expect-error one-step R2 deletion result is package-private.
import type { ApplicationR2DeletionAdvance } from '@proofoftech/fleet-control';
// @ts-expect-error release derivation is package-private.
import { activeExternalRelease } from '@proofoftech/fleet-control';
// @ts-expect-error release inventory derivation is package-private.
import { retainedExternalReleases } from '@proofoftech/fleet-control';
// @ts-expect-error immutable mapping assertion is package-private.
import { assertImmutableDeploymentMapping } from '@proofoftech/fleet-control';
// @ts-expect-error persisted database reconciliation is package-private.
import { reconcilePersistedDatabase } from '@proofoftech/fleet-control';
// @ts-expect-error intent codec errors are package-private.
import { DecommissionAdvanceIntentError } from '@proofoftech/fleet-control';
// @ts-expect-error receipt capability capture is package-private.
import { captureDatabaseExportReceiptCapability } from '@proofoftech/fleet-control';
// @ts-expect-error receipt authority normalization is package-private.
import { databaseExportReceiptAuthorityFromUnknown } from '@proofoftech/fleet-control';
// @ts-expect-error receipt identity normalization is package-private.
import { databaseExportReceiptIdentityFromUnknown } from '@proofoftech/fleet-control';
// @ts-expect-error receipt integrity normalization is package-private.
import { databaseExportIntegrityFromUnknown } from '@proofoftech/fleet-control';
// @ts-expect-error native receipt integrity capture is package-private.
import { captureDatabaseExportIntegrityPromise } from '@proofoftech/fleet-control';
// @ts-expect-error receipt body cancellation is package-private.
import { cancelBodyWithoutAwait } from '@proofoftech/fleet-control';
// @ts-expect-error tagged receipt errors are package-private.
import { databaseExportReceiptError } from '@proofoftech/fleet-control';
// @ts-expect-error the receipt-error classifier is package-private.
import { isDatabaseExportReceiptError } from '@proofoftech/fleet-control';
// @ts-expect-error captured receipt capabilities are package-private.
import type { CapturedDatabaseExportReceiptCapability } from '@proofoftech/fleet-control';
// @ts-expect-error filesystem receipt primitives are package-private.
import type { FileSystemDatabaseExportStoreReceiptPrimitives } from '@proofoftech/fleet-control';
// @ts-expect-error filesystem receipt overrides are package-private.
import type { FileSystemDatabaseExportStoreReceiptPrimitiveOverrides } from '@proofoftech/fleet-control';
// @ts-expect-error the filesystem publication seam is package-private.
import { createFileSystemDatabaseExportStoreWithReceiptPrimitives } from '@proofoftech/fleet-control';
// @ts-expect-error the R2 implementation remains a deep-only adapter.
import { R2DatabaseExportStore } from '@proofoftech/fleet-control';
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
declare const deploymentSpec: DeploymentSpec;
declare const provisioningBackend: ProvisioningBackend;
declare const fleetRecord: FleetRecord;
declare const fleetStateStore: FleetStateStore;
declare const decommissionIntent: DecommissionAdvanceIntent;
declare const decommissionToken: DecommissionAdvanceToken;
declare const decommissionClassification: DecommissionAdvanceTokenClassification;
declare const decommissionProgress: DecommissionAttachmentProgress;
declare const decommissionPurpose: DecommissionAttachmentPurpose;
declare const decommissionEvidence: DecommissionAttachmentScanEvidence;
declare const decommissionScanInput: DecommissionAttachmentScanInput;
declare const decommissionAttachment: DecommissionBlockedAttachment;
declare const decommissionCommon: DecommissionIntentCommon;
declare const decommissionIdentity: DecommissionOperationIdentity;
declare const decommissionMode: DecommissionOperationMode;
declare const decommissionRecordIdentity: DecommissionRecordIdentity;
declare const decommissionPhase: NormalDecommissionLifecyclePhase;
declare const plainWorkerRouteApi: PlainWorkerRouteApi;
declare const plainWorkerProvisioningApiShape: PlainWorkerProvisioningApi;
declare const receiptFence: ExternalMutationFence;
const databaseExportIntegrity: DatabaseExportIntegrity = {
  size: 1,
  sha256: 'a'.repeat(64),
};
const databaseExportReceiptIdentity: DatabaseExportReceiptIdentity = {
  version: 1,
  authority: 'memory://fleet-exports/receipts/v1',
  databaseId: '00000000-0000-0000-0000-000000000001',
  operationId: '00000000-0000-4000-8000-000000000002',
};
const legacyExportStore: DurableDatabaseExportStore = {
  async write() {
    return { location: 'memory://legacy', ...databaseExportIntegrity };
  },
};
const receiptExportStore: DurableDatabaseExportStore = {
  ...legacyExportStore,
  receiptAuthority: databaseExportReceiptIdentity.authority,
  async writeReceipt() {
    return { location: 'memory://receipt', ...databaseExportIntegrity };
  },
};
const storeReceiptAuthority: string | undefined =
  receiptExportStore.receiptAuthority;
const storeReceiptWrite: DurableDatabaseExportStore['writeReceipt'] =
  receiptExportStore.writeReceipt;
const plainWorkerBackendOptions: PlainWorkerBackendOptions = {
  api: plainWorkerProvisioningApiShape,
  identityCaller: 'PackedConsumer.seedDeploymentIdentity',
};
const plainWorkerBackend: ProvisioningBackend = new PlainWorkerBackend(
  plainWorkerBackendOptions,
);
const directClient = new CloudflareProvisioningClient({
  accountId: 'account',
  apiToken: 'token',
  plane: 'plain-worker',
  rateCoordinator: new ProcessLocalCloudflareApiRateCoordinator(),
});
const directBackendOptions: CloudflareApiPlainWorkerBackendOptions = {
  client: directClient,
};
const directBackend: ProvisioningBackend =
  new CloudflareApiPlainWorkerBackend(directBackendOptions);
const directReceiptAuthority: string | undefined =
  directClient.databaseExportReceiptAuthority;
const directReceiptExport:
  | ((identity: DatabaseExportReceiptIdentity) => Promise<DatabaseExport>)
  | undefined = directClient.exportDatabaseReceipt;
const plainReceiptAuthority: string | undefined =
  plainWorkerProvisioningApiShape.databaseExportReceiptAuthority;
const plainReceiptExport:
  | ((
      identity: DatabaseExportReceiptIdentity,
      fence: ExternalMutationFence,
    ) => Promise<PlainWorkerDatabaseExportResult>)
  | undefined = plainWorkerProvisioningApiShape.exportDatabaseReceipt;
const wfpReceiptAuthority: string | undefined =
  api.databaseExportReceiptAuthority;
const wfpReceiptExport:
  | ((identity: DatabaseExportReceiptIdentity) => Promise<DatabaseExport>)
  | undefined = api.exportDatabaseReceipt;
const backendReceiptAuthority: string | undefined =
  provisioningBackend.databaseExportReceiptAuthority;
const backendReceiptExport:
  | ((
      identity: DatabaseExportReceiptIdentity,
      fence: ExternalMutationFence,
    ) => Promise<DatabaseExport>)
  | undefined = provisioningBackend.exportDatabaseReceipt;
const decommissionScanResults: readonly DecommissionAttachmentScanResult[] = [
  {
    status: 'pending',
    progress: decommissionProgress,
    providerFetchAttemptsReserved: 9,
  },
  {
    status: 'attached',
    attachment: decommissionAttachment,
    providerFetchAttemptsReserved: 9,
  },
  {
    status: 'complete',
    evidenceSha256: decommissionEvidence.evidenceSha256,
    evidenceCount: decommissionEvidence.evidenceCount,
    providerFetchAttemptsReserved: 9,
  },
  { status: 'drift' },
];
const directDecommissionScan =
  directClient.advanceDecommissionAttachmentScan(decommissionScanInput);
const routeDecommissionScan =
  plainWorkerRouteApi.advanceDecommissionAttachmentScan?.(
    decommissionScanInput,
  );
const backendDecommissionScan =
  provisioningBackend.advanceDecommissionAttachmentScan?.(
    decommissionScanInput,
  );
const wfpDecommissionScan = api.advanceDecommissionAttachmentScan?.(
  decommissionScanInput,
);
const databaseResidualAssertion =
  provisioningBackend.assertDatabaseDeletionResidualsRemoved;
const decommissionActions: readonly DecommissionAdvanceAction[] = [
  { kind: 'start' },
  { kind: 'continue', token: decommissionToken },
  { kind: 'restart-blocked', token: decommissionToken },
];
const decommissionCapabilities: readonly DecommissionAdvanceCapability[] = [
  'attachment-scan',
  'database-residuals',
  'application-r2-inspection',
  'application-r2-empty',
  'application-r2-delete',
];
const decommissionAdvanceOptions: AdvanceDecommissionDeploymentOptions = {
  backend: provisioningBackend,
  store: fleetStateStore,
  spec: deploymentSpec,
  action: decommissionActions[0]!,
  maxProviderRequests: 12,
  randomUUID: () => '00000000-0000-4000-8000-000000000001',
};
const decommissionAdvanceResults: readonly DecommissionAdvanceResult[] = [
  { status: 'pending', token: decommissionToken },
  {
    status: 'blocked',
    token: decommissionToken,
    purpose: decommissionPurpose,
    attachment: decommissionAttachment,
  },
  {
    status: 'complete',
    token: decommissionToken,
    result: {
      record: fleetRecord,
      databaseExport: {
        databaseId: fleetRecord.databaseId,
        location: 'r2://exports/database.sql',
        sha256: 'a'.repeat(64),
        size: 1,
      },
    },
  },
];
const boundedDecommissionAdvance = advanceDecommissionDeployment(
  decommissionAdvanceOptions,
);
type PlainWorkerPortRecords = readonly [
  PlainWorkerCleanupOutcome,
  PlainWorkerDatabaseExportResult,
  PlainWorkerDatabaseInventoryEntry,
  PlainWorkerDeploymentStatus,
  PlainWorkerMutationOutcome,
  PlainWorkerUploadIntent,
  PlainWorkerUploadIntentBase,
  PlainWorkerUploadOutcome,
  PlainWorkerVersionBinding,
  PlainWorkerVersionDetail,
  PlainWorkerVersionSummary,
];
declare const plainWorkerPortRecords: PlainWorkerPortRecords;
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
const routeAttestationOptions: AttestConvergedActiveRouteOptions = {
  convergenceBudgetMs: 60_000,
};
const activeRouteRead: Promise<ActiveRouteAttestation> =
  provisioningBackend.attestActiveRoute(deploymentSpec);
type SettledSettlementKeyIsOptional = {} extends Pick<
  FleetRecord,
  'settledSettlementKey'
>
  ? true
  : false;
const settledSettlementKeyIsOptional: SettledSettlementKeyIsOptional = true;
const settledSettlementKey: string | undefined =
  fleetRecord.settledSettlementKey;
type DecommissionIntentIsOptional = {} extends Pick<
  FleetRecord,
  'decommissionIntent'
>
  ? true
  : false;
const decommissionIntentIsOptional: DecommissionIntentIsOptional = true;
const storedDecommissionIntent: DecommissionAdvanceIntent | undefined =
  fleetRecord.decommissionIntent;
void [
  decommissionIntent,
  decommissionToken,
  decommissionClassification,
  decommissionProgress,
  decommissionPurpose,
  decommissionEvidence,
  decommissionScanInput,
  decommissionScanResults,
  decommissionAttachment,
  decommissionCommon,
  decommissionIdentity,
  decommissionMode,
  decommissionRecordIdentity,
  decommissionPhase,
  decommissionIntentIsOptional,
  storedDecommissionIntent,
  directDecommissionScan,
  routeDecommissionScan,
  backendDecommissionScan,
  wfpDecommissionScan,
  databaseResidualAssertion,
  decommissionActions,
  decommissionCapabilities,
  decommissionAdvanceOptions,
  decommissionAdvanceResults,
  boundedDecommissionAdvance,
  databaseExportIntegrity,
  databaseExportReceiptIdentity,
  legacyExportStore,
  receiptExportStore,
  storeReceiptAuthority,
  storeReceiptWrite,
  receiptFence,
  directReceiptAuthority,
  directReceiptExport,
  plainReceiptAuthority,
  plainReceiptExport,
  wfpReceiptAuthority,
  wfpReceiptExport,
  backendReceiptAuthority,
  backendReceiptExport,
];
const customDomain: PlainWorkerCustomDomain = {
  id: 'domain-id',
  hostname: 'acme.example.test',
  service: 'acme-production',
};
const activePlainWorkerRoute =
  plainWorkerRouteApi.inspectActiveWorkerRoute(customDomain.service);
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
void CloudflareApiPlainWorkerBackend;
void CloudflareProvisioningClient;
void D1CloudflareApiRateCoordinator;
void FileSystemDatabaseExportStore;
void PlainWorkerBackend;
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
void deploymentSpec;
void provisioningBackend;
void fleetRecord;
void plainWorkerRouteApi;
void plainWorkerProvisioningApiShape;
void plainWorkerBackendOptions;
void plainWorkerBackend;
void directClient;
void directBackendOptions;
void directBackend;
void plainWorkerPortRecords;
void initialExecutionFenceState;
void lockedAtBirth;
void seedOptions;
void routeAttestation;
void routeExpectation;
void routeAttestationOptions;
void activeRouteRead;
void settledSettlementKeyIsOptional;
void settledSettlementKey;
void customDomain;
void activePlainWorkerRoute;
void observedRoute;
void settlementHost;
`,
  );
  await writeFile(
    join(consumerDirectory, 'runtime.mjs'),
    `import assert from 'node:assert/strict';
import {
  ActiveRouteAttestationError,
  CloudflareProvisioningClient,
  DecommissionAdvanceCapabilityError,
  DecommissionAdvanceRestartError,
  DecommissionAdvanceTokenDeploymentError,
  DecommissionAdvanceTokenError,
  DecommissionAdvanceTokenFutureError,
  DecommissionAdvanceTokenOperationError,
  ProcessLocalCloudflareApiRateCoordinator,
  FileSystemDatabaseExportStore,
  ProvisioningError,
  WorkersForPlatformsBackend,
  advanceDecommissionDeployment,
  attestConvergedActiveRoute,
  attestFleetRecordActiveRoute,
  deploymentSpecDigest,
  fleetSettlementKey,
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
assert.equal(typeof ActiveRouteAttestationError, 'function');
assert.ok(new ActiveRouteAttestationError('probe', {}) instanceof Error);
assert.equal(typeof attestConvergedActiveRoute, 'function');
assert.equal(typeof attestFleetRecordActiveRoute, 'function');
assert.equal(typeof fleetSettlementKey, 'function');
assert.equal(typeof advanceDecommissionDeployment, 'function');
const missingCapability = new DecommissionAdvanceCapabilityError(
  'attachment-scan',
);
assert.equal(missingCapability.name, 'DecommissionAdvanceCapabilityError');
assert.equal(missingCapability.capability, 'attachment-scan');
assert.equal(
  missingCapability.message,
  'backend cannot perform bounded decommission attachment scans',
);
const restartError = new DecommissionAdvanceRestartError();
assert.equal(restartError.name, 'DecommissionAdvanceRestartError');
assert.equal(
  restartError.message,
  'decommission advance restart requires a current blocked operation',
);
for (const ErrorClass of [
  DecommissionAdvanceTokenDeploymentError,
  DecommissionAdvanceTokenError,
  DecommissionAdvanceTokenFutureError,
  DecommissionAdvanceTokenOperationError,
]) {
  assert.ok(new ErrorClass() instanceof Error);
}
assert.equal(
  typeof CloudflareProvisioningClient.prototype
    .advanceDecommissionAttachmentScan,
  'function',
);
const legacyClient = new CloudflareProvisioningClient({
  accountId: 'a',
  apiToken: 't',
  plane: 'plain-worker',
  rateCoordinator: new ProcessLocalCloudflareApiRateCoordinator(),
  exportStore: {
    async write() {
      return { location: 'memory://legacy', size: 1, sha256: 'a'.repeat(64) };
    },
  },
});
assert.equal('databaseExportReceiptAuthority' in legacyClient, false);
assert.equal('exportDatabaseReceipt' in legacyClient, false);
const fileStore = new FileSystemDatabaseExportStore('/tmp/fleet-control-packed-receipts');
if (process.platform !== 'win32') {
  assert.equal(typeof fileStore.receiptAuthority, 'string');
  assert.equal(typeof fileStore.writeReceipt, 'function');
}
assert.throws(
  () =>
    new CloudflareProvisioningClient({
      accountId: 'a',
      apiToken: 't',
      plane: 'plain-worker',
      dispatchNamespace: 'x',
      rateCoordinator: new ProcessLocalCloudflareApiRateCoordinator(),
    }),
  /plain-worker plane cannot name a dispatch namespace/,
);
assert.throws(
  () =>
    new CloudflareProvisioningClient({
      accountId: 'a',
      apiToken: 't',
      rateCoordinator: new ProcessLocalCloudflareApiRateCoordinator(),
    }),
  /dispatchNamespace/,
);
assert.ok(
  new CloudflareProvisioningClient({
    accountId: 'a',
    apiToken: 't',
    plane: 'plain-worker',
    rateCoordinator: new ProcessLocalCloudflareApiRateCoordinator(),
  }),
);

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
