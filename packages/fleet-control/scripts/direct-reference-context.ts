// SPDX-License-Identifier: Apache-2.0

import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type {
  Crypto,
  D1Database,
  R2Bucket,
  FixedLengthStream as WorkerFixedLengthStream,
} from '@cloudflare/workers-types';
import {
  CloudflareApiPlainWorkerBackend,
  CloudflareProvisioningClient,
  D1CloudflareApiRateCoordinator,
  D1FleetInventoryRunStore,
  D1FleetStateStore,
} from '@proofoftech/fleet-control';
import {
  type CloudflareControlPlane,
  type CloudflareDeploymentSpec,
  createCloudflareControlPlane,
  D1FleetStateDatabase,
  type DeploymentSecrets,
  deploymentSpecDigest,
  type FleetRecord,
} from '@proofoftech/fleet-control/cloudflare-control-plane';
import type { DirectRunManifest } from './direct-credentialed-conformance-preflight.mjs';
import {
  type DirectFixtureRelease,
  type DirectFixtureRole,
  directDeploymentSpec,
} from './direct-credentialed-spec.js';
import { DirectReferenceExecutionError } from './direct-reference-http.js';
import { DirectReferenceJournal } from './direct-reference-journal.js';
import {
  DIRECT_REFERENCE_LEASE,
  DirectReferenceTransport,
} from './direct-reference-transport.js';

declare const crypto: Crypto;
declare const FixedLengthStream: typeof WorkerFixedLengthStream;

export interface DirectRunBinding {
  readonly version: 1;
  readonly accountId: string;
  readonly fleetDatabaseId: string;
  readonly quotaDatabaseId: string;
  readonly exportBucketName: string;
  readonly referenceModuleSetSha256: string;
  readonly accountWorkersDevSubdomain: string;
}

export interface DirectReferenceEnvironment {
  readonly FLEET_DB: D1Database;
  readonly QUOTA_DB: D1Database;
  readonly EXPORTS: R2Bucket;
  readonly CLOUDFLARE_API_TOKEN: string;
  readonly FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET?: string;
  readonly DIRECT_DEPLOYMENT_SECRETS: string;
  readonly DIRECT_RUN_BINDING: string;
}

export interface DirectReferenceContext {
  readonly binding: DirectRunBinding;
  readonly control: CloudflareControlPlane;
  readonly journal: DirectReferenceJournal;
  readonly inventoryStore: D1FleetInventoryRunStore;
  readonly transport: DirectReferenceTransport;
  readonly createForcePlane: () => Readonly<{
    store: D1FleetStateStore;
    client: CloudflareProvisioningClient;
    backend: CloudflareApiPlainWorkerBackend;
  }>;
  readonly recoveryClaimSetPresent: () => Promise<boolean>;
  readonly roleFor: (record: FleetRecord) => DirectFixtureRole;
  readonly spec: (
    role: DirectFixtureRole,
    release: DirectFixtureRelease,
  ) => CloudflareDeploymentSpec;
  readonly specFor: (record: FleetRecord) => CloudflareDeploymentSpec;
  readonly secrets: (role: DirectFixtureRole) => DeploymentSecrets;
}

function refused(): never {
  throw new DirectReferenceExecutionError();
}

function object(
  value: unknown,
  keys: readonly string[],
): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) refused();
  const actual = Object.keys(value);
  if (
    actual.length !== keys.length ||
    keys.some((key) => !Object.hasOwn(value, key))
  )
    refused();
  return value as Record<string, unknown>;
}

function json(value: unknown): unknown {
  if (typeof value !== 'string' || Buffer.byteLength(value) > 256 * 1024)
    refused();
  try {
    return JSON.parse(value);
  } catch {
    return refused();
  }
}

function identifier(value: unknown): string {
  if (
    typeof value !== 'string' ||
    !value ||
    value !== value.trim() ||
    value.length > 128 ||
    [...value].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  )
    refused();
  return value;
}

function parseBinding(
  raw: string,
  manifest: DirectRunManifest,
): DirectRunBinding {
  const value = object(json(raw), [
    'version',
    'accountId',
    'fleetDatabaseId',
    'quotaDatabaseId',
    'exportBucketName',
    'referenceModuleSetSha256',
    'accountWorkersDevSubdomain',
  ]);
  if (value.version !== 1) refused();
  const accountId = identifier(value.accountId);
  const fleetDatabaseId = identifier(value.fleetDatabaseId);
  const quotaDatabaseId = identifier(value.quotaDatabaseId);
  const exportBucketName = identifier(value.exportBucketName);
  const referenceModuleSetSha256 = identifier(value.referenceModuleSetSha256);
  const accountWorkersDevSubdomain = identifier(
    value.accountWorkersDevSubdomain,
  );
  if (
    fleetDatabaseId === quotaDatabaseId ||
    exportBucketName !== manifest.names.exportBucket ||
    !/^[a-f0-9]{64}$/u.test(referenceModuleSetSha256)
  )
    refused();
  return Object.freeze({
    version: 1,
    accountId,
    fleetDatabaseId,
    quotaDatabaseId,
    exportBucketName,
    referenceModuleSetSha256,
    accountWorkersDevSubdomain,
  });
}

function parseSecrets(value: unknown): DeploymentSecrets {
  const fields = object(value, [
    'deploymentIdentity',
    'maintenanceAdmin',
    'application',
  ]);
  const application = object(fields.application, ['APP_PROBE_TOKEN']);
  if (
    typeof fields.deploymentIdentity !== 'string' ||
    typeof fields.maintenanceAdmin !== 'string' ||
    typeof application.APP_PROBE_TOKEN !== 'string'
  )
    refused();
  return Object.freeze({
    deploymentIdentity: fields.deploymentIdentity,
    maintenanceAdmin: fields.maintenanceAdmin,
    application: Object.freeze({
      APP_PROBE_TOKEN: application.APP_PROBE_TOKEN,
    }),
  });
}

export async function createDirectReferenceContext(
  manifest: DirectRunManifest,
  environment: DirectReferenceEnvironment,
  invocation: Readonly<{
    startedAt: number;
    signal: AbortSignal;
    fetch?: typeof fetch;
  }>,
): Promise<DirectReferenceContext> {
  const binding = parseBinding(environment.DIRECT_RUN_BINDING, manifest);
  const apiToken = environment.CLOUDFLARE_API_TOKEN;
  if (typeof apiToken !== 'string' || !apiToken || apiToken !== apiToken.trim())
    refused();
  const rawSecrets = environment.DIRECT_DEPLOYMENT_SECRETS;
  const secretInput = object(json(rawSecrets), ['a', 'b', 'recovery']);
  const secrets: Readonly<Record<DirectFixtureRole, DeploymentSecrets>> =
    Object.freeze({
      a: parseSecrets(secretInput.a),
      b: parseSecrets(secretInput.b),
      recovery: parseSecrets(secretInput.recovery),
    });
  const specs = new Map<
    DirectFixtureRole,
    ReadonlyMap<DirectFixtureRelease, CloudflareDeploymentSpec>
  >();
  const digests = new Map<
    DirectFixtureRole,
    ReadonlyMap<string, CloudflareDeploymentSpec>
  >();
  for (const role of ['a', 'b', 'recovery'] as const) {
    const recipes = new Map<DirectFixtureRelease, CloudflareDeploymentSpec>();
    const byDigest = new Map<string, CloudflareDeploymentSpec>();
    for (const release of [
      'initial',
      'next',
      ...(role === 'recovery' ? ['failed-recovery' as const] : []),
    ] as const) {
      const spec = directDeploymentSpec(
        manifest,
        role,
        release,
        secrets[role],
        binding,
      );
      const digest = deploymentSpecDigest(spec);
      if (byDigest.has(digest)) refused();
      recipes.set(release, spec);
      byDigest.set(digest, spec);
    }
    specs.set(role, recipes);
    digests.set(role, byDigest);
  }
  const transport = new DirectReferenceTransport({
    runtime: manifest.referenceRuntime,
    startedAt: invocation.startedAt,
    signal: invocation.signal,
    fetch: invocation.fetch,
  });
  transport.assertWithinBudget();
  const journal = new DirectReferenceJournal(
    environment.FLEET_DB,
    manifest.resourcePrefix,
    JSON.stringify({
      configSha256: manifest.configSha256,
      binding,
      quotaScope: manifest.resourcePrefix,
      tenantSecretsSha256: createHash('sha256')
        .update(rawSecrets)
        .digest('hex'),
    }),
  );
  await journal.readInterruption();
  transport.assertWithinBudget();
  const control = createCloudflareControlPlane({
    accountId: binding.accountId,
    apiToken,
    fleetDatabase: environment.FLEET_DB,
    quotaDatabase: environment.QUOTA_DB,
    quotaScope: manifest.resourcePrefix,
    ...DIRECT_REFERENCE_LEASE,
    requestTimeoutMs: transport.effectiveRequestTimeoutMs,
    maintenanceRequestTimeoutMs: transport.effectiveRequestTimeoutMs,
    fetch: transport.providerFetch,
    maintenanceFetch: transport.maintenanceFetch,
    databaseExports: {
      bucket: environment.EXPORTS,
      bucketName: binding.exportBucketName,
      keyPrefix: `${manifest.resourcePrefix}/`,
      streams: { DigestStream: crypto.DigestStream, FixedLengthStream },
      randomUUID: () => crypto.randomUUID(),
    },
  });
  const inventoryStore = new D1FleetInventoryRunStore(
    new D1FleetStateDatabase(environment.FLEET_DB),
    { accountId: binding.accountId, ...DIRECT_REFERENCE_LEASE },
  );
  const roleFor = (record: FleetRecord): DirectFixtureRole => {
    if (
      record.backend !== 'plain-worker' ||
      record.environment !== manifest.environment ||
      record.backendSwitchIntent ||
      record.migrationIntent ||
      record.migrationPriorRelease ||
      record.rollbackRelease ||
      record.retiringRelease ||
      record.platformResources ||
      record.platformTarget
    )
      refused();
    const role = (['a', 'b', 'recovery'] as const).find(
      (role) => manifest.names.roles[role].tenantTag === record.tenantTag,
    );
    if (!role) refused();
    const names = manifest.names.roles[role];
    if (
      record.scriptName !== names.scriptName ||
      record.databaseName !== names.databaseName ||
      record.routeHostname !== names.routeHostname
    )
      refused();
    for (const release of [record.activeRelease, record.pendingRelease]) {
      if (!release) continue;
      const recipe = digests.get(role)?.get(release.specDigest);
      if (
        ![
          'migrating',
          'decommissioning',
          'traffic-removed',
          'credentials-revoked',
          'worker-deleted',
          'platform-credentials-revoked',
          'platform-resources-deleted',
          'application-resources-deleting',
          'application-resources-deleted',
          'database-exported',
          'database-deleting',
          'decommissioned',
        ].includes(record.decommissionIntent?.lifecyclePhase ?? record.phase) ||
        release.physicalScriptName !== record.scriptName ||
        !recipe ||
        release.releaseSchemaVersion !== recipe.schemaVersion ||
        !release.artifactVersion ||
        release.artifactVersion === 'pending' ||
        release.topology ||
        (release === record.activeRelease &&
          release.artifactVersion !== record.artifactVersion)
      )
        refused();
    }
    return role;
  };
  return Object.freeze({
    binding,
    control,
    journal,
    inventoryStore,
    transport,
    createForcePlane() {
      transport.assertWithinBudget();
      const store = new D1FleetStateStore(
        new D1FleetStateDatabase(environment.FLEET_DB),
        { accountId: binding.accountId, ...DIRECT_REFERENCE_LEASE },
      );
      const rateCoordinator = new D1CloudflareApiRateCoordinator(
        environment.QUOTA_DB,
        { quotaScope: manifest.resourcePrefix },
      );
      const client = new CloudflareProvisioningClient({
        accountId: binding.accountId,
        apiToken,
        plane: 'plain-worker',
        rateCoordinator,
        requestTimeoutMs: transport.effectiveRequestTimeoutMs,
        fetch: transport.providerFetch,
      });
      const backend = new CloudflareApiPlainWorkerBackend({
        client,
        fetch: transport.maintenanceFetch,
        maintenanceRequestTimeoutMs: transport.effectiveRequestTimeoutMs,
      });
      return { store, client, backend };
    },
    async recoveryClaimSetPresent() {
      transport.assertWithinBudget();
      const row = await environment.FLEET_DB.prepare(
        'SELECT EXISTS(SELECT 1 FROM anchorage_platform_plane_claims WHERE account_id=? AND resource_set_key=?) AS present',
      )
        .bind(
          binding.accountId,
          `deployment:${manifest.names.roles.recovery.tenantTag}:${manifest.environment}`,
        )
        .first<{ present: number }>();
      transport.assertWithinBudget();
      if (row?.present !== 0 && row?.present !== 1) refused();
      return row.present === 1;
    },
    roleFor,
    spec(role: DirectFixtureRole, release: DirectFixtureRelease) {
      return specs.get(role)?.get(release) ?? refused();
    },
    specFor(record: FleetRecord) {
      const role = roleFor(record);
      let digest =
        record.phase === 'migrating'
          ? record.pendingSpecDigest
          : record.desiredSpecDigest;
      if (record.cleanupIntent && record.decommissionIntent) refused();
      if (record.cleanupIntent?.authority.kind === 'provisioning-rollback')
        digest = record.cleanupIntent.authority.requestedSpecDigest;
      if (record.decommissionIntent) {
        const mode = record.decommissionIntent.identity.mode;
        if (mode.kind !== 'normal') refused();
        digest = mode.requestedSpecDigest;
      }
      return (digest && digests.get(role)?.get(digest)) || refused();
    },
    secrets(role: DirectFixtureRole) {
      return Object.hasOwn(secrets, role) ? secrets[role] : refused();
    },
  });
}
