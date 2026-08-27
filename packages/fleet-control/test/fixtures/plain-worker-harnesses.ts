// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CloudflareApiPlainWorkerBackend } from '../../src/cloudflare-api-plain-worker-backend.js';
import {
  CloudflareProvisioningClient,
  type DurableDatabaseExportStore,
} from '../../src/cloudflare-client.js';
import { plainWorkerIngressModule } from '../../src/plain-worker-backend.js';
import { uploadIntentToProviderBindings } from '../../src/provider-binding-inventory.js';
import { deploymentSpecDigest } from '../../src/spec-digest.js';
import type {
  DeploymentSecrets,
  DeploymentSpec,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  PlainWorkerUploadIntent,
  ProvisioningBackend,
} from '../../src/types.js';
import { WranglerLoopBackend } from '../../src/wrangler-loop-backend.js';
import {
  recordingFetch,
  restProjection,
  testRateCoordinator,
} from './cloudflare-fetch-fixture.js';
import { type ProviderWorld, providerWorld } from './provider-world.js';
import { cliProjection } from './wrangler-world-projection.js';

export const sharedSecrets: DeploymentSecrets = {
  deploymentIdentity: 'deployment-identity-secret-value-0001',
  maintenanceAdmin: 'maintenance-admin-secret-value-00001',
  application: {},
};

export const routeAttestation = {
  convergenceBudgetMs: 1_000,
  initialRetryDelayMs: 1,
};

export function initialSpec(): DeploymentSpec {
  return buildPlainWorkerSpec({
    schemaVersion: 1,
    migrations: [
      { version: 1, sql: 'CREATE TABLE example (id TEXT PRIMARY KEY)' },
    ],
  });
}

export function migrationSpec(
  overrides: Partial<DeploymentSpec> = {},
): DeploymentSpec {
  return buildPlainWorkerSpec({ previousDurableObjectTag: 'v1', ...overrides });
}

export async function captureFailure(
  promise: Promise<unknown>,
): Promise<unknown> {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  throw new Error('expected operation to fail');
}

export async function ignoreFailure(promise: Promise<unknown>): Promise<void> {
  await captureFailure(promise);
}

export function errorChain(error: unknown): string {
  return causeChain(error)
    .flatMap((cause) => (cause instanceof Error ? [cause.message] : []))
    .join(' | ');
}

export function causeChain(error: unknown): readonly unknown[] {
  const causes: unknown[] = [];
  let current = error;
  while (current instanceof Error) {
    causes.push(current);
    current = current.cause;
  }
  if (current !== undefined) causes.push(current);
  return causes;
}

export function buildPlainWorkerSpec(
  overrides: Partial<DeploymentSpec> = {},
): DeploymentSpec {
  return {
    tenantTag: 'acme',
    environment: 'production',
    scriptName: 'acme-production',
    databaseName: 'acme-production',
    compatibilityDate: '2026-08-27',
    compatibilityFlags: ['nodejs_compat'],
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'platform',
    schemaVersion: 2,
    migrations: [
      { version: 1, sql: 'CREATE TABLE example (id TEXT PRIMARY KEY)' },
      {
        version: 2,
        sql: 'ALTER TABLE example ADD COLUMN value TEXT',
        rollbackCompatible: true,
      },
    ],
    durableObjectMigrations: [{ tag: 'v1', newSqliteClasses: ['Maintenance'] }],
    durableObjectBindings: [{ name: 'MAINTENANCE', className: 'Maintenance' }],
    maintenanceBaseUrl: 'https://control-acme.example.test',
    routeHostname: 'acme.example.test',
    application: { vars: [], secrets: [], r2Buckets: [] },
    ...overrides,
  };
}

export function uploadIntentForSpec(
  spec: DeploymentSpec,
  databaseId: string,
  mode: 'initial' | 'staged',
  secrets: DeploymentSecrets = sharedSecrets,
): PlainWorkerUploadIntent {
  const ingress = plainWorkerIngressModule(spec);
  const base = {
    scriptName: spec.scriptName,
    candidateTag: deploymentSpecDigest(spec),
    mainModule: ingress.name,
    modules: [...spec.modules, ingress],
    compatibilityDate: spec.compatibilityDate,
    compatibilityFlags: spec.compatibilityFlags,
    bindings: {
      plainText: [
        { name: 'DEPLOYMENT_TENANT', value: spec.tenantTag },
        { name: 'FLEET_ENVIRONMENT', value: spec.environment },
        { name: 'FLEET_SCHEMA_VERSION', value: String(spec.schemaVersion) },
        { name: 'FLEET_SPEC_DIGEST', value: deploymentSpecDigest(spec) },
        { name: 'FLEET_INGRESS_CONTRACT', value: 'guarded-object-v1' },
        ...(spec.application?.vars ?? []),
      ],
      secrets: [
        {
          name: 'DEPLOYMENT_IDENTITY_SECRET',
          value: secrets.deploymentIdentity,
        },
        { name: 'MAINTENANCE_ADMIN_SECRET', value: secrets.maintenanceAdmin },
        ...Object.entries(secrets.application ?? {}).map(([name, value]) => ({
          name,
          value,
        })),
      ],
      d1: [{ name: 'DB', databaseId, databaseName: spec.databaseName }],
      durableObjects: spec.durableObjectBindings.map(({ name, className }) => ({
        name,
        className,
      })),
      services: spec.egressProxyService
        ? [{ name: 'EGRESS_PROXY', service: spec.egressProxyService }]
        : [],
      queueProducers: spec.queueProducer
        ? [
            {
              name: spec.queueProducer.binding,
              queueName: spec.queueProducer.queueName,
            },
          ]
        : [],
      r2Buckets: [],
    },
    limits: { cpuMs: spec.cpuLimitMs },
    publicAccess: {
      workersDevEnabled: true,
      previewUrlsEnabled: false,
    },
  };
  return mode === 'initial'
    ? {
        ...base,
        mode,
        durableObjectMigrations: spec.durableObjectMigrations,
      }
    : { ...base, mode };
}

export function seedWorkerFromSpec(
  world: ProviderWorld,
  options: {
    readonly spec?: DeploymentSpec;
    readonly databaseId?: string;
    readonly mode?: 'initial' | 'staged';
    readonly secrets?: DeploymentSecrets;
  } = {},
) {
  const spec = options.spec ?? buildPlainWorkerSpec();
  const databaseId = options.databaseId ?? 'database-1';
  if (!world.databases.some((database) => database.databaseId === databaseId)) {
    world.seedDatabase(spec.databaseName, { databaseId });
  }
  const intent = uploadIntentForSpec(
    spec,
    databaseId,
    options.mode ?? 'initial',
    options.secrets,
  );
  return world.applyUpload({
    scriptName: intent.scriptName,
    mode: intent.mode,
    tag: intent.candidateTag,
    bindings: uploadIntentToProviderBindings(intent),
    mainModule: intent.mainModule,
    modules: intent.modules,
    publicAccess: intent.publicAccess,
  });
}

export class HarnessFleetStore implements FleetStateStore {
  record: FleetRecord | undefined;
  failPutPhase: FleetRecord['phase'] | undefined;
  readonly phases: FleetRecord['phase'][] = [];
  readonly snapshots: Array<{
    readonly record: FleetRecord;
    readonly world: ProviderWorld;
  }> = [];
  #leased = false;
  readonly #snapshot: boolean;

  constructor(
    readonly world: ProviderWorld,
    initial?: FleetRecord,
    options: { readonly snapshot?: boolean } = {},
  ) {
    this.record = initial ? structuredClone(initial) : undefined;
    this.#snapshot = options.snapshot === true;
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    if (this.#leased) throw new Error('deployment is already being modified');
    this.#leased = true;
    try {
      return await operation({
        tenantTag,
        environment,
        mutationLeaseTtlMs: 15 * 60_000,
        assertOwned: async () => {},
        renew: async () => {},
        put: (record) => this.put(record),
        delete: () => this.delete(),
      });
    } finally {
      this.#leased = false;
    }
  }

  async get(): Promise<FleetRecord | undefined> {
    return this.record ? structuredClone(this.record) : undefined;
  }

  async list(): Promise<readonly FleetRecord[]> {
    return this.record ? [structuredClone(this.record)] : [];
  }

  async put(record: FleetRecord): Promise<void> {
    if (this.failPutPhase === record.phase) {
      this.failPutPhase = undefined;
      throw new Error(`failed state write at ${record.phase}`);
    }
    this.record = structuredClone(record);
    this.phases.push(record.phase);
    if (this.#snapshot) {
      this.snapshots.push({
        record: structuredClone(record),
        world: this.world.clone(),
      });
    }
  }

  async delete(): Promise<void> {
    this.record = undefined;
  }
}

export class HarnessExportStore implements DurableDatabaseExportStore {
  readonly exports = new Map<
    string,
    { readonly fileName: string; readonly bytes: Uint8Array }
  >();

  async write(input: {
    readonly databaseId: string;
    readonly fileName: string;
    readonly body: ReadableStream<Uint8Array>;
  }): Promise<{ location: string; size: number; sha256: string }> {
    const chunks: Uint8Array[] = [];
    const reader = input.body.getReader();
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      chunks.push(chunk.value);
    }
    const bytes = Buffer.concat(chunks);
    this.exports.set(input.databaseId, {
      fileName: input.fileName,
      bytes: new Uint8Array(bytes),
    });
    return {
      location: `memory://fleet-exports/${input.databaseId}/${input.fileName}`,
      size: bytes.byteLength,
      sha256: createHash('sha256').update(bytes).digest('hex'),
    };
  }
}

type ProjectedFetch = ReturnType<typeof recordingFetch>;

export function plainOnlyClient(
  projected: ProjectedFetch,
  exportStore: DurableDatabaseExportStore,
): CloudflareProvisioningClient {
  return new CloudflareProvisioningClient({
    accountId: 'account',
    apiToken: 'token',
    plane: 'plain-worker',
    rateCoordinator: testRateCoordinator(),
    fetch: projected.fetch,
    requestTimeoutMs: 10_000,
    exportStore,
  });
}

export interface PlainWorkerHarness {
  readonly backend: ProvisioningBackend;
  readonly world: ProviderWorld;
  readonly exportStore: HarnessExportStore;
  readonly store: HarnessFleetStore;
  readonly exportDirectory?: string;
}

interface PlainWorkerHarnessOptions {
  readonly maintenanceRequestTimeoutMs?: number;
  readonly snapshot?: boolean;
}

const liveWorlds = new Set<ProviderWorld>();

export function assertHarnessFailuresConsumed(): void {
  const pending = [...liveWorlds].flatMap((world, index) => {
    const hooks = world.pendingHookNames();
    return hooks.length === 0 ? [] : [`world ${index}: ${hooks.join(', ')}`];
  });
  liveWorlds.clear();
  if (pending.length > 0) {
    throw new Error(`unconsumed provider hooks: ${pending.join('; ')}`);
  }
}

export function wranglerHarness(
  world: ProviderWorld = providerWorld(),
  options: PlainWorkerHarnessOptions = {},
): PlainWorkerHarness & { readonly exportDirectory: string } {
  const exportStore = new HarnessExportStore();
  const projected = recordingFetch(restProjection(world));
  const client = plainOnlyClient(projected, exportStore);
  const exportDirectory = mkdtempSync(
    join(tmpdir(), 'fleet-conformance-export-'),
  );
  liveWorlds.add(world);
  return {
    backend: new WranglerLoopBackend({
      runner: cliProjection(world),
      routeApi: client,
      fetch: projected.fetch,
      exportDirectory,
      exportStore,
      maintenanceRequestTimeoutMs:
        options.maintenanceRequestTimeoutMs ?? 60_000,
    }),
    world,
    exportStore,
    store: new HarnessFleetStore(world, undefined, {
      snapshot: options.snapshot,
    }),
    exportDirectory,
  };
}

export function directHarness(
  world: ProviderWorld = providerWorld(),
  options: PlainWorkerHarnessOptions = {},
): PlainWorkerHarness {
  const exportStore = new HarnessExportStore();
  const projected = recordingFetch(restProjection(world));
  const client = plainOnlyClient(projected, exportStore);
  liveWorlds.add(world);
  return {
    backend: new CloudflareApiPlainWorkerBackend({
      client,
      fetch: projected.fetch,
      maintenanceRequestTimeoutMs:
        options.maintenanceRequestTimeoutMs ?? 60_000,
    }),
    world,
    exportStore,
    store: new HarnessFleetStore(world, undefined, {
      snapshot: options.snapshot,
    }),
  };
}

export function hostileCauseProxy(): object {
  return new Proxy(
    {},
    {
      getPrototypeOf() {
        throw new Error('hostile getPrototypeOf trap');
      },
    },
  );
}

export function malformedErrorsBody(): Response {
  return Response.json(
    { success: false, errors: { message: 'not an array' } },
    { status: 400 },
  );
}

export function throwingConstructorError(
  message = 'hostile constructor getter',
): Error {
  const error = new Error(message);
  Object.defineProperty(error, 'constructor', {
    get() {
      throw new Error('constructor access refused');
    },
  });
  return error;
}
