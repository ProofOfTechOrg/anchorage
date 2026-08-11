// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { chmod, mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Readable } from 'node:stream';
import {
  provisionDeploymentIdentityProtocol,
  readDeploymentIdentityProtocol,
} from '@proofoftech/flowsafe/deployment-identity-protocol';
import {
  applicationSecretValues,
  canonicalApplicationBindings,
} from './application-bindings.js';
import type { DurableDatabaseExportStore } from './cloudflare-client.js';
import { isSha256 } from './deployment-context.js';
import { WorkerDeploymentError } from './deployment-error.js';
import { maintenanceUrl, readMaintenanceHealth } from './maintenance-health.js';
import { applyMigrationsWithLedger } from './migration-ledger.js';
import { deploymentSpecDigest } from './spec-digest.js';
import type {
  D1Migration,
  DatabaseExport,
  DatabaseReference,
  DeploymentEgressPolicy,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
  ExternalPlatformResources,
  ExternalReleaseSnapshot,
  FleetRecord,
  LiveDeployment,
  MaintenanceHealth,
  PromotionGuard,
  ProvisioningBackend,
  WorkerZoneRoute,
} from './types.js';
import { targetDurableObjectTag } from './validation.js';
import type { CommandResult, CommandRunner } from './wrangler-runner.js';

const PLAIN_INGRESS_CONTRACT = 'guarded-object-v1';
const PLAIN_INGRESS_MODULE = '__anchorage_guarded_entry__.js';
const JAVASCRIPT_CONTENT_TYPES = new Set([
  'application/javascript',
  'application/javascript+module',
  'text/javascript',
]);

interface DeploymentVersion {
  readonly id: string;
  readonly percentage: number;
}

interface DeploymentStatus {
  readonly versions: readonly DeploymentVersion[];
}

export interface PlainWorkerCustomDomain {
  readonly id: string;
  readonly hostname: string;
  readonly service: string;
}

export interface PlainWorkerRouteApi {
  listWorkerDatabaseAttachments(databaseId: string): Promise<
    readonly Readonly<{
      scriptName: string;
      plane: 'ordinary' | 'dispatch';
      dispatchNamespace?: string;
    }>[]
  >;
  listWorkerR2Attachments?(bucketName: string): Promise<
    readonly Readonly<{
      scriptName: string;
      plane: 'ordinary' | 'dispatch';
      dispatchNamespace?: string;
    }>[]
  >;
  getR2Bucket?(
    bucketName: string,
    jurisdiction: import('./types.js').R2Jurisdiction,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined>;
  createR2Bucket?(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  assertR2BucketEmpty?(
    resource: import('./types.js').ApplicationR2Binding,
  ): Promise<void>;
  deleteR2Bucket?(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void>;
  listCustomDomains(): Promise<readonly PlainWorkerCustomDomain[]>;
  inspectOrdinaryWorkerFootprint(scriptName: string): Promise<{
    readonly scriptPresent: boolean;
    readonly workersDevEnabled?: boolean;
    readonly previewUrlsEnabled?: boolean;
    readonly customDomains: readonly PlainWorkerCustomDomain[];
    readonly zoneRoutes: readonly WorkerZoneRoute[];
  }>;
  listDurableObjectNamespaces(scriptName: string): Promise<readonly string[]>;
  listOrdinaryWorkerSecretNames(scriptName: string): Promise<readonly string[]>;
  attachCustomDomain(
    target: {
      readonly hostname: string;
      readonly service: string;
    },
    fence: ExternalMutationFence,
  ): Promise<void>;
  detachCustomDomain(
    domainId: string,
    fence: ExternalMutationFence,
  ): Promise<void>;
  disableOrdinaryWorkerPublicAccess(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<void>;
}

function parseJson(value: string, operation: string): unknown {
  try {
    return JSON.parse(value);
  } catch (cause) {
    throw new Error(`wrangler ${operation} returned invalid JSON`, { cause });
  }
}

function asArray(value: unknown): readonly unknown[] {
  if (Array.isArray(value)) return value;
  if (value && typeof value === 'object' && 'result' in value) {
    const result = (value as { result?: unknown }).result;
    return Array.isArray(result) ? result : result ? [result] : [];
  }
  return [];
}

function field(value: unknown, name: string): unknown {
  return value && typeof value === 'object'
    ? (value as Record<string, unknown>)[name]
    : undefined;
}

function isWranglerNotFound(error: unknown): boolean {
  return (
    error instanceof Error &&
    /not found|10090|does not exist|has no deployments/i.test(error.message)
  );
}

function versionId(value: unknown): string | undefined {
  const id = field(value, 'id') ?? field(value, 'version_id');
  return typeof id === 'string' ? id : undefined;
}

function versionTag(value: unknown): string | undefined {
  const annotations = field(value, 'annotations');
  const tag = field(annotations, 'workers/tag') ?? field(value, 'tag');
  return typeof tag === 'string' ? tag : undefined;
}

function sqlLiteral(value: unknown): string {
  if (value === null) return 'NULL';
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value === 'boolean') return value ? '1' : '0';
  if (typeof value === 'string') return `'${value.replaceAll("'", "''")}'`;
  throw new Error('Wrangler D1 bindings must be scalar values');
}

function bindSql(sql: string, bindings: readonly unknown[]): string {
  let index = 0;
  const bound = sql.replaceAll('?', () => {
    if (index >= bindings.length) {
      throw new Error('not enough SQL bindings');
    }
    return sqlLiteral(bindings[index++]);
  });
  if (index !== bindings.length) throw new Error('too many SQL bindings');
  return bound;
}

export function plainWorkerIngressModule(spec: DeploymentSpec): Readonly<{
  name: string;
  content: string;
}> {
  if (spec.modules.some((module) => module.name === PLAIN_INGRESS_MODULE)) {
    throw new Error(
      `Worker modules reserve '${PLAIN_INGRESS_MODULE}' for the guarded fleet entrypoint`,
    );
  }
  const main = spec.modules.find((module) => module.name === spec.mainModule);
  if (
    !main ||
    typeof main.content !== 'string' ||
    (main.contentType !== undefined &&
      !JAVASCRIPT_CONTENT_TYPES.has(main.contentType)) ||
    main.name.startsWith('/') ||
    main.name.split('/').includes('..') ||
    /[?#\\]/u.test(main.name)
  ) {
    throw new Error(
      'plain Worker mainModule must be an importable string JavaScript ES module',
    );
  }
  const importSpecifier = JSON.stringify(`./${main.name}`);
  const localClasses = [
    ...new Set(
      spec.durableObjectBindings.flatMap((binding) =>
        binding.scriptName === undefined ? [binding.className] : [],
      ),
    ),
  ].sort();
  if (
    localClasses.some(
      (className) => !/^[A-Za-z_$][A-Za-z0-9_$]*$/u.test(className),
    )
  ) {
    throw new Error(
      'plain Worker contains an invalid local Durable Object class',
    );
  }
  const reexports =
    localClasses.length > 0
      ? `export { ${localClasses.join(', ')} } from ${importSpecifier};\n`
      : '';
  const routeHostname = JSON.stringify(
    new URL(`https://${spec.routeHostname}`).hostname
      .toLowerCase()
      .replace(/\.$/u, ''),
  );
  const controlHostname = JSON.stringify(
    new URL(spec.maintenanceBaseUrl).hostname.toLowerCase().replace(/\.$/u, ''),
  );
  return {
    name: PLAIN_INGRESS_MODULE,
    content: `import __anchorageUserEntrypoint from ${importSpecifier};
${reexports}if (
  !__anchorageUserEntrypoint ||
  typeof __anchorageUserEntrypoint !== 'object' ||
  typeof __anchorageUserEntrypoint.fetch !== 'function'
) {
  throw new TypeError('plain Worker entrypoint must default-export an object with fetch');
}
const __anchorageUserFetch = __anchorageUserEntrypoint.fetch;
const __anchorageRouteHostname = ${routeHostname};
const __anchorageControlHostname = ${controlHostname};
const __anchorageEnsurePath = '/admin/ensure-maintenance';
const __anchorageStatusPath = '/admin/maintenance-status';
const __anchorageReject = () => new Response(null, { status: 404 });
export default {
  ...__anchorageUserEntrypoint,
  async fetch(request, env, context) {
    const url = new URL(request.url);
    const hostname = url.hostname.toLowerCase().replace(/\\.$/u, '');
    if (hostname === __anchorageRouteHostname) {
      if (request.headers.has('Cloudflare-Workers-Version-Overrides')) {
        return __anchorageReject();
      }
    } else {
      if (hostname !== __anchorageControlHostname) {
        return __anchorageReject();
      }
      const validOperation =
        (url.pathname === __anchorageEnsurePath && request.method === 'POST') ||
        (url.pathname === __anchorageStatusPath && request.method === 'GET');
      const authorization = request.headers.get('authorization');
      if (
        !validOperation ||
        url.search !== '' ||
        authorization === null ||
        !/^Bearer [\\x21-\\x7e]{32,256}$/u.test(authorization)
      ) {
        return __anchorageReject();
      }
    }
    return Reflect.apply(__anchorageUserFetch, __anchorageUserEntrypoint, [
      request,
      env,
      context,
    ]);
  },
};
`,
  };
}

export class WranglerLoopBackend implements ProvisioningBackend {
  readonly kind = 'plain-worker' as const;
  readonly #runner: CommandRunner;
  readonly #routeApi: PlainWorkerRouteApi;
  readonly #fetch: typeof fetch;
  readonly #exportDirectory: string;
  readonly #exportStore: DurableDatabaseExportStore;
  readonly #maintenanceRequestTimeoutMs: number;

  constructor(options: {
    readonly runner: CommandRunner;
    readonly routeApi: PlainWorkerRouteApi;
    readonly exportDirectory: string;
    readonly exportStore: DurableDatabaseExportStore;
    readonly fetch?: typeof fetch;
    readonly maintenanceRequestTimeoutMs?: number;
  }) {
    if (!options.exportDirectory)
      throw new Error('exportDirectory is required');
    if (!options.exportStore) throw new Error('exportStore is required');
    if (!options.routeApi) throw new Error('routeApi is required');
    const maintenanceRequestTimeoutMs =
      options.maintenanceRequestTimeoutMs ?? 30_000;
    if (
      !Number.isSafeInteger(maintenanceRequestTimeoutMs) ||
      maintenanceRequestTimeoutMs < 1
    ) {
      throw new Error('maintenance request timeout must be positive');
    }
    this.#runner = options.runner;
    this.#routeApi = options.routeApi;
    this.#fetch = options.fetch ?? fetch;
    this.#exportDirectory = resolve(options.exportDirectory);
    this.#exportStore = options.exportStore;
    this.#maintenanceRequestTimeoutMs = maintenanceRequestTimeoutMs;
  }

  async #assertMutationFence(fence: ExternalMutationFence): Promise<void> {
    if (
      !Number.isSafeInteger(fence.mutationLeaseTtlMs) ||
      fence.mutationLeaseTtlMs < 1
    ) {
      throw new Error('external mutation fence lease TTL must be positive');
    }
    if (
      !Number.isSafeInteger(this.#runner.maxDurationMs) ||
      this.#runner.maxDurationMs < 1
    ) {
      throw new Error('Wrangler command maximum duration must be positive');
    }
    if (this.#runner.maxDurationMs >= fence.mutationLeaseTtlMs) {
      throw new Error(
        'Wrangler command maximum duration must be below the external mutation fence lease TTL',
      );
    }
    await fence.assertOwned();
  }

  async #runMutation(
    fence: ExternalMutationFence,
    arguments_: readonly string[],
    options?: { readonly input?: string; readonly cwd?: string },
  ): Promise<CommandResult> {
    await this.#assertMutationFence(fence);
    return this.#runner.run(arguments_, options);
  }

  async findDatabase(
    spec: DeploymentSpec,
  ): Promise<DatabaseReference | undefined> {
    const listed = await this.#runner.run(['d1', 'list', '--json']);
    const matches = asArray(parseJson(listed.stdout, 'd1 list')).filter(
      (database) => field(database, 'name') === spec.databaseName,
    );
    if (matches.length > 1) {
      throw new Error(`multiple D1 databases are named '${spec.databaseName}'`);
    }
    if (matches[0]) {
      const id = field(matches[0], 'uuid');
      if (typeof id !== 'string') throw new Error('D1 list result has no uuid');
      return { id, name: spec.databaseName, created: false };
    }
    return undefined;
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    let result: CommandResult;
    try {
      result = await this.#runner.run(['d1', 'info', databaseId, '--json']);
    } catch (error) {
      if (isWranglerNotFound(error)) return undefined;
      throw error;
    }
    const parsed = parseJson(result.stdout, 'd1 info');
    const body = field(parsed, 'result') ?? parsed;
    const id = field(body, 'uuid');
    const name = field(body, 'name');
    if (id !== databaseId || typeof name !== 'string' || name.length === 0) {
      throw new Error('D1 info result has an invalid uuid or name');
    }
    return { id: databaseId, name, created: false };
  }

  async ensureDatabase(
    spec: DeploymentSpec,
    fence: ExternalMutationFence,
  ): Promise<DatabaseReference> {
    await this.#assertMutationFence(fence);
    let created: CommandResult;
    try {
      created = await this.#runner.run([
        'd1',
        'create',
        spec.databaseName,
        '--json',
      ]);
    } catch (cause) {
      const recovered = await this.findDatabase(spec);
      if (recovered) {
        const owner = await this.readDeploymentIdentity(recovered, fence);
        if (owner !== undefined) {
          throw new Error(
            `refusing authorized database reconciliation for '${recovered.id}' owned by '${owner}'`,
            { cause },
          );
        }
        return { ...recovered, created: true };
      }
      throw cause;
    }
    const body = parseJson(created.stdout, 'd1 create');
    const result = field(body, 'result') ?? body;
    const id = field(result, 'uuid');
    if (typeof id !== 'string') throw new Error('D1 create result has no uuid');
    return { id, name: spec.databaseName, created: true };
  }

  async #query(
    database: DatabaseReference,
    sql: string,
    fence: ExternalMutationFence,
    bindings: readonly unknown[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    const result = await this.#runMutation(fence, [
      'd1',
      'execute',
      database.id,
      '--remote',
      '--json',
      '--command',
      bindSql(sql, bindings),
    ]);
    const pages = asArray(parseJson(result.stdout, 'd1 execute'));
    const rows: Readonly<Record<string, unknown>>[] = [];
    for (const page of pages) {
      const pageRows = field(page, 'results');
      if (Array.isArray(pageRows)) {
        for (const row of pageRows) {
          if (row && typeof row === 'object') {
            rows.push(row as Readonly<Record<string, unknown>>);
          }
        }
      }
    }
    return rows;
  }

  async seedDeploymentIdentity(
    database: DatabaseReference,
    tenantTag: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await provisionDeploymentIdentityProtocol(
      (statement) =>
        this.#query(database, statement.sql, fence, statement.bindings),
      tenantTag,
      { caller: 'WranglerLoopBackend.seedDeploymentIdentity' },
    );
  }

  readDeploymentIdentity(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<string | undefined> {
    return readDeploymentIdentityProtocol((statement) =>
      this.#query(database, statement.sql, fence, statement.bindings),
    );
  }

  async applyMigrations(
    database: DatabaseReference,
    migrations: readonly D1Migration[],
    fence: ExternalMutationFence,
  ): Promise<void> {
    await applyMigrationsWithLedger(
      {
        query: (sql, bindings) => this.#query(database, sql, fence, bindings),
        batch: async (statements) => {
          const sql = statements
            .map((statement) =>
              bindSql(statement.sql, statement.bindings ?? []),
            )
            .join(';\n');
          await this.#query(database, sql, fence);
        },
      },
      migrations,
    );
  }

  async findApplicationR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined> {
    if (!this.#routeApi.getR2Bucket) {
      throw new Error('plain Worker route API does not support application R2');
    }
    const found = await this.#routeApi.getR2Bucket(
      resource.bucketName,
      resource.jurisdiction,
    );
    return found
      ? { ...resource, creationDate: found.creationDate }
      : undefined;
  }

  async ensureApplicationR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot> {
    if (!this.#routeApi.createR2Bucket) {
      throw new Error('plain Worker route API does not support application R2');
    }
    try {
      await this.#routeApi.createR2Bucket(resource, fence);
    } catch (error) {
      const reconciled = await this.findApplicationR2Bucket(resource);
      if (reconciled) return reconciled;
      if (
        error &&
        typeof error === 'object' &&
        'status' in error &&
        error.status === 409
      ) {
        throw new Error(
          `R2 bucket '${resource.bucketName}' conflicts with a foreign resource`,
        );
      }
      throw error;
    }
    const confirmed = await this.findApplicationR2Bucket(resource);
    if (!confirmed)
      throw new Error(
        `R2 bucket '${resource.bucketName}' is absent after create`,
      );
    return confirmed;
  }

  async assertApplicationR2Detached(
    resource: import('./types.js').ApplicationR2Binding,
    _fence: ExternalMutationFence,
  ): Promise<void> {
    if (!this.#routeApi.listWorkerR2Attachments) {
      throw new Error('plain Worker route API cannot scan R2 attachments');
    }
    const attachments = await this.#routeApi.listWorkerR2Attachments(
      resource.bucketName,
    );
    if (attachments.length > 0) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' remains attached to a Worker`,
      );
    }
  }

  async assertApplicationR2Empty(
    resource: import('./types.js').ApplicationR2Binding,
    _fence: ExternalMutationFence,
  ): Promise<void> {
    if (!this.#routeApi.assertR2BucketEmpty) {
      throw new Error('plain Worker route API cannot inspect R2 contents');
    }
    await this.#routeApi.assertR2BucketEmpty(resource);
  }

  async deleteApplicationR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void> {
    if (!this.#routeApi.deleteR2Bucket) {
      throw new Error('plain Worker route API cannot delete application R2');
    }
    const current = await this.findApplicationR2Bucket(resource);
    if (!current || current.creationDate !== resource.creationDate) {
      throw new Error(`R2 bucket '${resource.bucketName}' ownership changed`);
    }
    await this.#routeApi.deleteR2Bucket(resource, fence);
    if (await this.findApplicationR2Bucket(resource)) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' remains after delete`,
      );
    }
  }

  async #deploymentStatus(
    spec: DeploymentSpec,
  ): Promise<DeploymentStatus | undefined> {
    let result: CommandResult;
    try {
      result = await this.#runner.run([
        'deployments',
        'status',
        '--name',
        spec.scriptName,
        '--json',
      ]);
    } catch (error) {
      if (isWranglerNotFound(error)) return undefined;
      throw error;
    }
    const parsed = parseJson(result.stdout, 'deployments status');
    const body = field(parsed, 'result') ?? parsed;
    const versions = asArray(field(body, 'versions')).map((version) => {
      const id = versionId(version);
      const percentage = Number(field(version, 'percentage'));
      if (!id || !Number.isFinite(percentage) || percentage < 0) {
        throw new Error('wrangler deployment status has an invalid version');
      }
      return { id, percentage };
    });
    if (versions.length === 0) {
      throw new Error('wrangler deployment status has no versions');
    }
    return { versions };
  }

  async #listVersions(
    spec: DeploymentSpec,
  ): Promise<readonly unknown[] | undefined> {
    try {
      const listed = await this.#runner.run([
        'versions',
        'list',
        '--name',
        spec.scriptName,
        '--json',
      ]);
      return asArray(parseJson(listed.stdout, 'versions list'));
    } catch (error) {
      if (isWranglerNotFound(error)) return undefined;
      throw error;
    }
  }

  async #viewVersion(spec: DeploymentSpec, id: string): Promise<unknown> {
    const viewed = await this.#runner.run([
      'versions',
      'view',
      id,
      '--name',
      spec.scriptName,
      '--json',
    ]);
    return parseJson(viewed.stdout, 'versions view');
  }

  #plainTextBindings(version: unknown): ReadonlyMap<string, string> {
    const resources = field(version, 'resources');
    const bindings = asArray(field(resources, 'bindings'));
    return new Map(
      bindings.flatMap((binding) => {
        if (field(binding, 'type') !== 'plain_text') return [];
        const name = field(binding, 'name');
        const text = field(binding, 'text');
        return typeof name === 'string' && typeof text === 'string'
          ? [[name, text] as const]
          : [];
      }),
    );
  }

  async #matchingCandidateIds(
    spec: DeploymentSpec,
    versions?: readonly unknown[],
  ): Promise<readonly string[]> {
    const digest = deploymentSpecDigest(spec);
    const listed = versions ?? (await this.#listVersions(spec));
    if (!listed) return [];
    const tagged = listed.filter((version) => versionTag(version) === digest);
    const matches: string[] = [];
    for (const candidate of tagged) {
      const id = versionId(candidate);
      if (!id) {
        throw new Error('wrangler versions list result has no version id');
      }
      const version = await this.#viewVersion(spec, id);
      const plainText = this.#plainTextBindings(version);
      if (plainText.get('FLEET_SPEC_DIGEST') !== digest) {
        throw new Error(
          `Worker version '${id}' has a mismatched fleet specification digest`,
        );
      }
      if (plainText.get('FLEET_INGRESS_CONTRACT') === PLAIN_INGRESS_CONTRACT) {
        matches.push(id);
      }
    }
    return matches;
  }

  async #findCandidate(
    spec: DeploymentSpec,
    versions?: readonly unknown[],
  ): Promise<string | undefined> {
    const digest = deploymentSpecDigest(spec);
    const matches = await this.#matchingCandidateIds(spec, versions);
    if (matches.length > 1) {
      throw new Error(
        `multiple Worker versions use fleet specification tag '${digest}'`,
      );
    }
    return matches[0];
  }

  async #expectedCandidate(
    spec: DeploymentSpec,
    artifactVersion: string,
    versions?: readonly unknown[],
  ): Promise<string> {
    const listed = versions ?? (await this.#listVersions(spec));
    if (!listed?.some((version) => versionId(version) === artifactVersion)) {
      throw new Error(
        `Worker '${spec.scriptName}' is missing persisted artifact version '${artifactVersion}'`,
      );
    }
    const version = await this.#viewVersion(spec, artifactVersion);
    const plainText = this.#plainTextBindings(version);
    if (
      plainText.get('FLEET_SPEC_DIGEST') !== deploymentSpecDigest(spec) ||
      plainText.get('FLEET_INGRESS_CONTRACT') !== PLAIN_INGRESS_CONTRACT
    ) {
      throw new Error(
        `Worker '${spec.scriptName}' persisted artifact version '${artifactVersion}' has drifted`,
      );
    }
    return artifactVersion;
  }

  #databaseIds(version: unknown): readonly string[] {
    const resources = field(version, 'resources');
    return asArray(field(resources, 'bindings')).flatMap((binding) => {
      if (field(binding, 'type') !== 'd1') return [];
      const id = field(binding, 'id') ?? field(binding, 'database_id');
      return typeof id === 'string' ? [id] : [];
    });
  }

  async #assertExistingWorkerIdentity(
    spec: DeploymentSpec,
    databaseId: string,
    deployment: DeploymentStatus,
  ): Promise<void> {
    if (deployment.versions.length === 0) {
      throw new Error(
        `refusing to upload over existing Worker '${spec.scriptName}' without a deployed version`,
      );
    }
    for (const deployed of deployment.versions) {
      const version = await this.#viewVersion(spec, deployed.id);
      const plainText = this.#plainTextBindings(version);
      const databaseIds = this.#databaseIds(version);
      const digest = plainText.get('FLEET_SPEC_DIGEST');
      if (
        databaseIds.length !== 1 ||
        databaseIds[0] !== databaseId ||
        plainText.get('DEPLOYMENT_TENANT') !== spec.tenantTag ||
        plainText.get('FLEET_ENVIRONMENT') !== spec.environment ||
        !digest ||
        !isSha256(digest)
      ) {
        throw new Error(
          `refusing to upload over existing Worker '${spec.scriptName}' with drifted tenant, environment, or D1 ownership`,
        );
      }
    }
  }

  async #attestWorkerOwnership(
    spec: DeploymentSpec,
    persistedDatabaseId?: string,
  ): Promise<string | undefined> {
    const [status, versions] = await Promise.all([
      this.#deploymentStatus(spec),
      this.#listVersions(spec),
    ]);
    if (!status && (!versions || versions.length === 0)) return undefined;
    const candidateId = await this.#findCandidate(spec, versions);
    if (!candidateId) {
      throw new Error(
        `refusing to mutate Worker '${spec.scriptName}' without its exact fleet specification`,
      );
    }
    const [version, databaseId] = await Promise.all([
      this.#viewVersion(spec, candidateId),
      persistedDatabaseId
        ? Promise.resolve(persistedDatabaseId)
        : this.findDatabase(spec).then((database) => database?.id),
    ]);
    const digest = deploymentSpecDigest(spec);
    const plainText = this.#plainTextBindings(version);
    const databaseIds = this.#databaseIds(version);
    if (
      !databaseId ||
      databaseIds.length !== 1 ||
      databaseIds[0] !== databaseId ||
      plainText.get('DEPLOYMENT_TENANT') !== spec.tenantTag ||
      plainText.get('FLEET_ENVIRONMENT') !== spec.environment ||
      plainText.get('FLEET_SPEC_DIGEST') !== digest
    ) {
      throw new Error(
        `refusing to mutate Worker '${spec.scriptName}' with drifted tenant, environment, specification, or D1 ownership`,
      );
    }
    for (const deployed of status?.versions ?? []) {
      const deployedVersion = await this.#viewVersion(spec, deployed.id);
      const deployedPlainText = this.#plainTextBindings(deployedVersion);
      const deployedDatabaseIds = this.#databaseIds(deployedVersion);
      const deployedDigest = deployedPlainText.get('FLEET_SPEC_DIGEST');
      if (
        deployedDatabaseIds.length !== 1 ||
        deployedDatabaseIds[0] !== databaseId ||
        deployedPlainText.get('DEPLOYMENT_TENANT') !== spec.tenantTag ||
        deployedPlainText.get('FLEET_ENVIRONMENT') !== spec.environment ||
        !deployedDigest ||
        !isSha256(deployedDigest)
      ) {
        throw new Error(
          `refusing to mutate Worker '${spec.scriptName}' with a drifted deployed version`,
        );
      }
    }
    return candidateId;
  }

  async #attestPersistedWorkerOwnership(
    spec: DeploymentSpec,
    databaseId: string,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
  ): Promise<string | undefined> {
    const allowed = [activeRelease, ...(retainedReleases ?? [])].filter(
      (release): release is ExternalReleaseSnapshot => release !== undefined,
    );
    if (allowed.length === 0) {
      return this.#attestWorkerOwnership(spec, databaseId);
    }
    const [status, versions] = await Promise.all([
      this.#deploymentStatus(spec),
      this.#listVersions(spec),
    ]);
    if (!status && (!versions || versions.length === 0)) return undefined;
    if (!status || status.versions.length === 0) {
      throw new Error(
        `refusing to mutate Worker '${spec.scriptName}' without an attestable current deployment`,
      );
    }
    const releasesByVersion = new Map(
      allowed.map((release) => [release.artifactVersion, release]),
    );
    for (const deployed of status.versions) {
      const release = releasesByVersion.get(deployed.id);
      if (!release) {
        throw new Error(
          `refusing to mutate Worker '${spec.scriptName}' with a current deployment outside its persisted artifact set`,
        );
      }
      const version = await this.#viewVersion(spec, deployed.id);
      const plainText = this.#plainTextBindings(version);
      const databaseIds = this.#databaseIds(version);
      if (
        databaseIds.length !== 1 ||
        databaseIds[0] !== databaseId ||
        plainText.get('DEPLOYMENT_TENANT') !== spec.tenantTag ||
        plainText.get('FLEET_ENVIRONMENT') !== spec.environment ||
        plainText.get('FLEET_SPEC_DIGEST') !== release.specDigest ||
        plainText.get('FLEET_SCHEMA_VERSION') !==
          String(release.releaseSchemaVersion)
      ) {
        throw new Error(
          `refusing to mutate Worker '${spec.scriptName}' with drifted persisted artifact ownership`,
        );
      }
    }
    return status.versions[0]?.id;
  }

  async assertDatabaseDetached(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#assertMutationFence(fence);
    if (
      record.backend !== this.kind ||
      record.tenantTag !== spec.tenantTag ||
      record.environment !== spec.environment ||
      record.scriptName !== spec.scriptName ||
      record.databaseName !== spec.databaseName ||
      record.databaseId !== database.id ||
      record.databaseName !== database.name ||
      record.routeHostname !== spec.routeHostname ||
      record.desiredSpecDigest !== deploymentSpecDigest(spec)
    ) {
      throw new Error(
        `refusing to attest database detachment for mismatched fleet record '${record.tenantTag}:${record.environment}'`,
      );
    }
    const databaseAttachments =
      await this.#routeApi.listWorkerDatabaseAttachments(database.id);
    if (databaseAttachments.length > 0) {
      throw new Error(
        `database '${record.databaseId}' remains attached to ${databaseAttachments
          .map(
            (attachment) =>
              `${attachment.plane} Worker '${attachment.scriptName}'`,
          )
          .join(', ')}`,
      );
    }
    const [status, versions, domains, footprint, namespaceIds] =
      await Promise.all([
        this.#deploymentStatus(spec),
        this.#listVersions(spec),
        this.#routeApi.listCustomDomains(),
        this.#routeApi.inspectOrdinaryWorkerFootprint(spec.scriptName),
        this.#routeApi.listDurableObjectNamespaces(spec.scriptName),
      ]);
    const routeFootprint = domains.filter(
      (domain) =>
        domain.service === spec.scriptName ||
        domain.hostname.toLowerCase() === spec.routeHostname.toLowerCase(),
    );
    const hasWranglerVersions = Boolean(versions && versions.length > 0);
    if (!status && !hasWranglerVersions && !footprint.scriptPresent) {
      if (
        routeFootprint.length > 0 ||
        footprint.customDomains.length > 0 ||
        footprint.zoneRoutes.length > 0 ||
        namespaceIds.length > 0
      ) {
        throw new Error(
          `database '${record.databaseId}' has a residual route or Durable Object namespace footprint`,
        );
      }
      await this.#assertMutationFence(fence);
      return;
    }
    if (!status && !hasWranglerVersions && footprint.scriptPresent) {
      throw new Error(
        `database '${record.databaseId}' has an ordinary Worker footprint that Wrangler cannot attest`,
      );
    }
    if ((status || hasWranglerVersions) && !footprint.scriptPresent) {
      throw new Error(
        `database '${record.databaseId}' has inconsistent authoritative and Wrangler Worker footprints`,
      );
    }
    let ownedWorker: string | undefined;
    try {
      ownedWorker = await this.#attestWorkerOwnership(spec, record.databaseId);
    } catch (cause) {
      throw new Error(
        `database '${record.databaseId}' has a foreign or mismatched Worker footprint`,
        { cause },
      );
    }
    if (!ownedWorker) {
      const [
        reconciledStatus,
        reconciledVersions,
        reconciledDomains,
        reconciledFootprint,
        reconciledNamespaceIds,
      ] = await Promise.all([
        this.#deploymentStatus(spec),
        this.#listVersions(spec),
        this.#routeApi.listCustomDomains(),
        this.#routeApi.inspectOrdinaryWorkerFootprint(spec.scriptName),
        this.#routeApi.listDurableObjectNamespaces(spec.scriptName),
      ]);
      const reconciledRoute = reconciledDomains.some(
        (domain) =>
          domain.service === spec.scriptName ||
          domain.hostname.toLowerCase() === spec.routeHostname.toLowerCase(),
      );
      const hasReconciledVersions = Boolean(
        reconciledVersions && reconciledVersions.length > 0,
      );
      if (
        !reconciledStatus &&
        !hasReconciledVersions &&
        !reconciledRoute &&
        !reconciledFootprint.scriptPresent &&
        reconciledFootprint.customDomains.length === 0 &&
        reconciledFootprint.zoneRoutes.length === 0 &&
        reconciledNamespaceIds.length === 0
      ) {
        await this.#assertMutationFence(fence);
        return;
      }
      throw new Error(
        `database '${record.databaseId}' detachment changed during attestation`,
      );
    }
    throw new Error(
      `database '${record.databaseId}' remains attached to owned Worker '${spec.scriptName}'`,
    );
  }

  async #customDomain(
    hostname: string,
  ): Promise<PlainWorkerCustomDomain | undefined> {
    const normalized = hostname.toLowerCase();
    const matches = (await this.#routeApi.listCustomDomains()).filter(
      (domain) => domain.hostname.toLowerCase() === normalized,
    );
    if (matches.length > 1) {
      throw new Error(`custom domain '${hostname}' has duplicate ownership`);
    }
    return matches[0];
  }

  async #attestPromotionRoute(
    spec: DeploymentSpec,
    guard: PromotionGuard,
  ): Promise<PlainWorkerCustomDomain | undefined> {
    const current = await this.#customDomain(spec.routeHostname);
    if (!current) {
      if (!guard.allowUnrouted) {
        throw new Error(
          `custom domain '${spec.routeHostname}' is unexpectedly absent during promotion`,
        );
      }
      return undefined;
    }
    if (!guard.allowedCurrentScriptNames.includes(current.service)) {
      throw new Error(
        `custom domain '${spec.routeHostname}' is owned by unexpected Worker '${current.service}'`,
      );
    }
    return current;
  }

  async #deployCandidateAtZero(
    spec: DeploymentSpec,
    candidateId: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    const current = await this.#deploymentStatus(spec);
    if (!current) {
      throw new Error(
        `existing Worker '${spec.scriptName}' has no active deployment`,
      );
    }
    if (current.versions.some((version) => version.id === candidateId)) return;
    const versionSpecs = [
      ...current.versions.map(
        (version) => `${version.id}@${version.percentage}%`,
      ),
      `${candidateId}@0%`,
    ];
    try {
      await this.#runMutation(fence, [
        'versions',
        'deploy',
        ...versionSpecs,
        '--name',
        spec.scriptName,
        '-y',
      ]);
    } catch (error) {
      const reconciled = await this.#deploymentStatus(spec);
      if (!reconciled?.versions.some((version) => version.id === candidateId)) {
        throw error;
      }
    }
  }

  #requireMaintenanceDigest(
    spec: DeploymentSpec,
    maintenance: MaintenanceHealth,
  ): void {
    const expected = deploymentSpecDigest(spec);
    if (maintenance.deploymentSpecDigest !== expected) {
      throw new Error(
        `maintenance response did not attest fleet specification digest '${expected}'`,
      );
    }
  }

  async deployWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    secrets: DeploymentSecrets,
    _platformResources: ExternalPlatformResources | undefined,
    fence: ExternalMutationFence,
    expectedArtifactVersion?: string,
    application?: import('./types.js').ApplicationBindingTopology,
  ): Promise<{ artifactVersion: string; created: boolean }> {
    if (spec.authoredBy !== 'platform') {
      throw new Error('Wrangler loop refuses externally authored Workers');
    }
    if (
      spec.durableObjectBindings.some(
        (binding) =>
          binding.scriptName !== undefined ||
          binding.dispatchNamespace !== undefined,
      )
    ) {
      throw new Error(
        'Wrangler loop supports only local Durable Object bindings',
      );
    }
    const directory = await mkdtemp(join(tmpdir(), 'anchorage-fleet-'));
    try {
      const deployment = await this.#deploymentStatus(spec);
      const versions = await this.#listVersions(spec);
      const workerExisted = deployment !== undefined || versions !== undefined;
      const priorVersionIds = new Set(
        (versions ?? []).map((version) => {
          const id = versionId(version);
          if (!id) {
            throw new Error('wrangler versions list result has no version id');
          }
          return id;
        }),
      );
      let candidateId = expectedArtifactVersion
        ? await this.#expectedCandidate(spec, expectedArtifactVersion, versions)
        : undefined;
      const pendingDurableObjectMigration =
        targetDurableObjectTag(spec) !== spec.previousDurableObjectTag;
      if (deployment && pendingDurableObjectMigration) {
        throw new Error(
          `existing Worker '${spec.scriptName}' has a pending Durable Object lifecycle migration; plain-Worker updates require an immediate/manual migration boundary`,
        );
      }
      if (deployment) {
        await this.#assertExistingWorkerIdentity(spec, database.id, deployment);
      }
      if (
        candidateId &&
        deployment?.versions.some((version) => version.id === candidateId)
      ) {
        return { artifactVersion: candidateId, created: false };
      }
      for (const module of spec.modules) {
        const modulePath = resolve(directory, module.name);
        if (!modulePath.startsWith(`${directory}/`)) {
          throw new Error(
            `module '${module.name}' escapes the staging directory`,
          );
        }
        await mkdir(resolve(modulePath, '..'), { recursive: true });
        await writeFile(modulePath, module.content);
      }
      const ingressModule = plainWorkerIngressModule(spec);
      await writeFile(
        join(directory, ingressModule.name),
        ingressModule.content,
      );
      const configPath = join(directory, 'wrangler.candidate.json');
      await writeFile(
        configPath,
        JSON.stringify({
          name: spec.scriptName,
          main: ingressModule.name,
          workers_dev: true,
          preview_urls: false,
          compatibility_date: spec.compatibilityDate,
          compatibility_flags: spec.compatibilityFlags,
          vars: {
            DEPLOYMENT_TENANT: spec.tenantTag,
            FLEET_ENVIRONMENT: spec.environment,
            FLEET_SCHEMA_VERSION: String(spec.schemaVersion),
            FLEET_SPEC_DIGEST: deploymentSpecDigest(spec),
            FLEET_INGRESS_CONTRACT: PLAIN_INGRESS_CONTRACT,
            ...Object.fromEntries(
              (
                application?.vars ?? canonicalApplicationBindings(spec).vars
              ).map(({ name, value }) => [name, value]),
            ),
          },
          d1_databases: [
            {
              binding: 'DB',
              database_name: database.name,
              database_id: database.id,
            },
          ],
          durable_objects: {
            bindings: spec.durableObjectBindings.map((binding) => ({
              name: binding.name,
              class_name: binding.className,
            })),
          },
          services: spec.egressProxyService
            ? [
                {
                  binding: 'EGRESS_PROXY',
                  service: spec.egressProxyService,
                },
              ]
            : undefined,
          ...(deployment
            ? {}
            : {
                migrations: spec.durableObjectMigrations.map((migration) => ({
                  tag: migration.tag,
                  new_sqlite_classes: migration.newSqliteClasses,
                  new_classes: migration.newClasses,
                  deleted_classes: migration.deletedClasses,
                  renamed_classes: migration.renamedClasses,
                })),
              }),
          queues: spec.queueProducer
            ? {
                producers: [
                  {
                    binding: spec.queueProducer.binding,
                    queue: spec.queueProducer.queueName,
                  },
                ],
              }
            : undefined,
          r2_buckets: (application?.r2Buckets ?? []).map((binding) => ({
            binding: binding.name,
            bucket_name: binding.bucketName,
          })),
          limits: spec.cpuLimitMs ? { cpu_ms: spec.cpuLimitMs } : undefined,
        }),
      );
      const secretsPath = join(directory, 'wrangler.secrets.json');
      await writeFile(
        secretsPath,
        JSON.stringify({
          DEPLOYMENT_IDENTITY_SECRET: secrets.deploymentIdentity,
          MAINTENANCE_ADMIN_SECRET: secrets.maintenanceAdmin,
          ...applicationSecretValues(spec, secrets),
        }),
        { mode: 0o600 },
      );
      const digest = deploymentSpecDigest(spec);
      try {
        if (!candidateId) {
          const command = deployment ? ['versions', 'upload'] : ['deploy'];
          let mutationError: unknown;
          try {
            await this.#runMutation(fence, [
              ...command,
              '--config',
              configPath,
              '--secrets-file',
              secretsPath,
              '--tag',
              digest,
            ]);
          } catch (error) {
            mutationError = error;
          }
          const operationCandidates = (
            await this.#matchingCandidateIds(spec)
          ).filter((id) => !priorVersionIds.has(id));
          if (operationCandidates.length !== 1) {
            if (mutationError) throw mutationError;
            throw new Error(
              `wrangler ${command.join(' ')} did not create exactly one new tagged Worker version`,
            );
          }
          const operationCandidate = operationCandidates[0];
          if (!operationCandidate) {
            throw new Error('new Worker candidate has no artifact version');
          }
          candidateId = operationCandidate;
        }
        if (deployment) {
          await this.#deployCandidateAtZero(spec, candidateId, fence);
        } else {
          const initial = await this.#deploymentStatus(spec);
          const selected = initial?.versions.find(
            (version) => version.id === candidateId,
          );
          if (selected?.percentage !== 100) {
            throw new Error(
              `initial Worker '${spec.scriptName}' did not deploy its tagged version at 100%`,
            );
          }
        }
        return { artifactVersion: candidateId, created: !workerExisted };
      } catch (cause) {
        if (workerExisted) {
          throw new WorkerDeploymentError({
            message: `failed to update existing Worker '${spec.scriptName}'`,
            cause,
            createdByAttempt: false,
            resourceState: 'present',
          });
        }
        const cleanupErrors: unknown[] = [];
        try {
          await this.revokeCredentials(
            spec,
            undefined,
            undefined,
            database,
            fence,
          );
        } catch (cleanupError) {
          cleanupErrors.push(cleanupError);
        }
        try {
          await this.deleteWorker(spec, undefined, database, undefined, fence);
        } catch (cleanupError) {
          cleanupErrors.push(cleanupError);
        }
        if (cleanupErrors.length > 0) {
          throw new WorkerDeploymentError({
            message: `failed to install credentials and clean up '${spec.scriptName}'`,
            cause: new AggregateError([cause, ...cleanupErrors]),
            createdByAttempt: true,
            resourceState: 'unknown',
          });
        }
        throw new WorkerDeploymentError({
          message: `failed to install Worker '${spec.scriptName}'`,
          cause,
          createdByAttempt: true,
          resourceState: 'absent',
        });
      }
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  async promoteWorker(
    spec: DeploymentSpec,
    guard: PromotionGuard,
    _outboundPolicy: DeploymentEgressPolicy | undefined,
    fence: ExternalMutationFence,
    expectedArtifactVersion?: string,
  ): Promise<void> {
    if (!expectedArtifactVersion) {
      throw new Error(
        'plain Worker promotion requires a persisted artifact version',
      );
    }
    const candidateId = await this.#expectedCandidate(
      spec,
      expectedArtifactVersion,
    );
    if (!candidateId) {
      throw new Error(
        `Worker '${spec.scriptName}' has no version for the desired fleet specification`,
      );
    }
    const current = await this.#deploymentStatus(spec);
    if (!current?.versions.some((version) => version.id === candidateId)) {
      throw new Error(
        `Worker candidate '${candidateId}' is not in the current deployment`,
      );
    }
    await this.#attestPromotionRoute(spec, guard);
    const promoted =
      current.versions.length === 1 &&
      current.versions[0]?.id === candidateId &&
      current.versions[0].percentage === 100;
    if (!promoted) {
      try {
        await this.#runMutation(fence, [
          'versions',
          'deploy',
          `${candidateId}@100%`,
          '--name',
          spec.scriptName,
          '-y',
        ]);
      } catch (error) {
        const reconciled = await this.#deploymentStatus(spec);
        if (
          reconciled?.versions.length !== 1 ||
          reconciled.versions[0]?.id !== candidateId ||
          reconciled.versions[0].percentage !== 100
        ) {
          throw error;
        }
      }
    }
    const beforeAttach = await this.#attestPromotionRoute(spec, guard);
    if (beforeAttach?.service !== spec.scriptName) {
      await this.#assertMutationFence(fence);
      await this.#routeApi.attachCustomDomain(
        {
          hostname: spec.routeHostname,
          service: spec.scriptName,
        },
        fence,
      );
    }
    const attached = await this.#customDomain(spec.routeHostname);
    if (attached?.service !== spec.scriptName) {
      throw new Error(
        `custom domain '${spec.routeHostname}' did not attest Worker '${spec.scriptName}' after promotion`,
      );
    }
  }

  async ensureMaintenance(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
    fence: ExternalMutationFence,
    expectedArtifactVersion?: string,
  ): Promise<MaintenanceHealth> {
    if (!expectedArtifactVersion) {
      throw new Error(
        'plain Worker maintenance requires a persisted artifact version',
      );
    }
    const candidateId = await this.#expectedCandidate(
      spec,
      expectedArtifactVersion,
    );
    const current = await this.#deploymentStatus(spec);
    if (
      !candidateId ||
      !current?.versions.some((version) => version.id === candidateId)
    ) {
      throw new Error('desired Worker candidate is not deployed');
    }
    if (this.#maintenanceRequestTimeoutMs >= fence.mutationLeaseTtlMs) {
      throw new Error(
        'maintenance request timeout must be below the external mutation fence lease TTL',
      );
    }
    await this.#assertMutationFence(fence);
    const maintenance = await readMaintenanceHealth(
      await this.#fetch(maintenanceUrl(spec, '/admin/ensure-maintenance'), {
        method: 'POST',
        signal: AbortSignal.timeout(this.#maintenanceRequestTimeoutMs),
        headers: {
          authorization: `Bearer ${maintenanceAdminSecret}`,
          'Cloudflare-Workers-Version-Overrides': `${spec.scriptName}="${candidateId}"`,
        },
      }),
    );
    this.#requireMaintenanceDigest(spec, maintenance);
    return maintenance;
  }

  async inspect(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
    expectedArtifactVersion?: string,
  ): Promise<LiveDeployment | undefined> {
    const status = await this.#deploymentStatus(spec);
    if (!status) return undefined;
    const candidateId = expectedArtifactVersion
      ? await this.#expectedCandidate(spec, expectedArtifactVersion)
      : await this.#findCandidate(spec);
    const candidateDeployed =
      candidateId !== undefined &&
      status.versions.some((version) => version.id === candidateId);
    const active = status.versions.find((version) => version.percentage > 0);
    const artifactVersion = candidateDeployed ? candidateId : active?.id;
    if (!artifactVersion) {
      throw new Error('wrangler deployment status has no active version');
    }
    const version = await this.#viewVersion(spec, artifactVersion);
    const resources = field(version, 'resources');
    const bindings = asArray(field(resources, 'bindings'));
    const databaseIds = bindings.flatMap((binding) => {
      if (field(binding, 'type') !== 'd1') return [];
      const id = field(binding, 'id') ?? field(binding, 'database_id');
      return typeof id === 'string' ? [id] : [];
    });
    const durableObjectBindings = bindings.flatMap((binding) => {
      if (field(binding, 'type') !== 'durable_object_namespace') return [];
      const id = field(binding, 'namespace_id');
      const name = field(binding, 'name');
      const className = field(binding, 'class_name');
      return typeof id === 'string' &&
        typeof name === 'string' &&
        typeof className === 'string'
        ? [{ name, className, namespaceId: id }]
        : [];
    });
    const serviceBindings = bindings.flatMap((binding) => {
      if (field(binding, 'type') !== 'service') return [];
      const name = field(binding, 'name');
      const service = field(binding, 'service');
      return typeof name === 'string' && typeof service === 'string'
        ? [{ name, service }]
        : [];
    });
    const queueProducerBindings = bindings.flatMap((binding) => {
      if (field(binding, 'type') !== 'queue') return [];
      const name = field(binding, 'name');
      const queueName = field(binding, 'queue_name');
      return typeof name === 'string' && typeof queueName === 'string'
        ? [{ name, queueName }]
        : [];
    });
    const r2BucketBindings = bindings
      .flatMap((binding) => {
        if (field(binding, 'type') !== 'r2_bucket') return [];
        const name = field(binding, 'name');
        const bucketName = field(binding, 'bucket_name');
        return typeof name === 'string' && typeof bucketName === 'string'
          ? [{ name, bucketName, jurisdiction: 'default' as const }]
          : [];
      })
      .sort((left, right) => left.name.localeCompare(right.name));
    const plainText = this.#plainTextBindings(version);
    const expectedServiceBindings = spec.egressProxyService
      ? [{ name: 'EGRESS_PROXY', service: spec.egressProxyService }]
      : [];
    const expectedQueueProducerBindings = spec.queueProducer
      ? [
          {
            name: spec.queueProducer.binding,
            queueName: spec.queueProducer.queueName,
          },
        ]
      : [];
    if (
      databaseIds.length !== 1 ||
      plainText.get('DEPLOYMENT_TENANT') !== spec.tenantTag ||
      plainText.get('FLEET_ENVIRONMENT') !== spec.environment ||
      JSON.stringify(serviceBindings) !==
        JSON.stringify(expectedServiceBindings) ||
      JSON.stringify(queueProducerBindings) !==
        JSON.stringify(expectedQueueProducerBindings) ||
      canonicalApplicationBindings(spec).vars.some(
        ({ name, value }) => plainText.get(name) !== value,
      )
    ) {
      throw new Error(
        `script '${spec.scriptName}' has a different resource mapping`,
      );
    }
    const databaseId = databaseIds[0];
    if (!databaseId) throw new Error('D1 binding has no database id');
    const schemaVersion = Number(plainText.get('FLEET_SCHEMA_VERSION'));
    const desiredSpecDigest = plainText.get('FLEET_SPEC_DIGEST');
    if (
      !Number.isSafeInteger(schemaVersion) ||
      !desiredSpecDigest ||
      !isSha256(desiredSpecDigest)
    ) {
      throw new Error(
        `script '${spec.scriptName}' has no valid schema version`,
      );
    }
    const maintenance = await readMaintenanceHealth(
      await this.#fetch(maintenanceUrl(spec, '/admin/maintenance-status'), {
        headers: {
          authorization: `Bearer ${maintenanceAdminSecret}`,
          ...(candidateDeployed
            ? {
                'Cloudflare-Workers-Version-Overrides': `${spec.scriptName}="${artifactVersion}"`,
              }
            : {}),
        },
      }),
    );
    if (candidateDeployed) {
      this.#requireMaintenanceDigest(spec, maintenance);
    } else if (
      maintenance.deploymentSpecDigest !== undefined &&
      maintenance.deploymentSpecDigest !== desiredSpecDigest
    ) {
      throw new Error(
        `maintenance response does not match inspected Worker version '${artifactVersion}'`,
      );
    }
    return {
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      scriptName: spec.scriptName,
      databaseId,
      durableObjectBindings,
      serviceBindings,
      queueProducerBindings,
      ...(canonicalApplicationBindings(spec).vars.length > 0
        ? { plainTextBindings: Object.fromEntries(plainText) }
        : {}),
      ...(r2BucketBindings.length > 0 ? { r2BucketBindings } : {}),
      ...(canonicalApplicationBindings(spec).secrets.length > 0
        ? {
            secretNames: await this.#routeApi.listOrdinaryWorkerSecretNames(
              spec.scriptName,
            ),
          }
        : {}),
      artifactVersion,
      desiredSpecDigest,
      schemaVersion,
      maintenance,
    };
  }

  async revokeCredentials(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    if (
      !(await this.#attestPersistedWorkerOwnership(
        spec,
        database.id,
        retainedReleases,
        activeRelease,
      ))
    )
      return;
    const secretNames = await this.#routeApi.listOrdinaryWorkerSecretNames(
      spec.scriptName,
    );
    for (const name of secretNames) {
      try {
        await this.#runMutation(fence, [
          'secret',
          'delete',
          name,
          '--name',
          spec.scriptName,
        ]);
      } catch (error) {
        if (!isWranglerNotFound(error)) throw error;
      }
    }
    const remaining = await this.#routeApi.listOrdinaryWorkerSecretNames(
      spec.scriptName,
    );
    if (remaining.length > 0) {
      throw new Error(
        `ordinary Worker '${spec.scriptName}' failed exact secret revocation`,
      );
    }
  }

  async removeTraffic(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    const worker = await this.#attestPersistedWorkerOwnership(
      spec,
      database.id,
      retainedReleases,
      activeRelease,
    );
    const route = await this.#customDomain(spec.routeHostname);
    if (route && route.service !== spec.scriptName) {
      throw new Error(
        `refusing to remove custom domain '${spec.routeHostname}' owned by Worker '${route.service}'`,
      );
    }
    if (!worker && route) {
      throw new Error(
        `refusing to remove custom domain '${spec.routeHostname}' without an attestable Worker owner`,
      );
    }
    if (route) {
      await this.#assertMutationFence(fence);
      await this.#routeApi.detachCustomDomain(route.id, fence);
    }
    if (worker) {
      await this.#routeApi.disableOrdinaryWorkerPublicAccess(
        spec.scriptName,
        fence,
      );
    }
  }

  async assertTrafficRemoved(spec: DeploymentSpec): Promise<void> {
    const footprint = await this.#routeApi.inspectOrdinaryWorkerFootprint(
      spec.scriptName,
    );
    if (
      footprint.customDomains.length > 0 ||
      footprint.zoneRoutes.length > 0 ||
      footprint.workersDevEnabled === true ||
      footprint.previewUrlsEnabled === true
    ) {
      throw new Error(
        `ordinary Worker '${spec.scriptName}' retains public ingress after traffic removal`,
      );
    }
  }

  async deleteWorker(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    database: DatabaseReference,
    activeRelease: ExternalReleaseSnapshot | undefined,
    fence: ExternalMutationFence,
  ): Promise<void> {
    const worker = await this.#attestPersistedWorkerOwnership(
      spec,
      database.id,
      retainedReleases,
      activeRelease,
    );
    const [initialFootprint, initialNamespaceIds] = await Promise.all([
      this.#routeApi.inspectOrdinaryWorkerFootprint(spec.scriptName),
      this.#routeApi.listDurableObjectNamespaces(spec.scriptName),
    ]);
    await this.assertTrafficRemoved(spec);
    if (!worker) {
      if (
        initialFootprint.scriptPresent ||
        initialFootprint.customDomains.length > 0 ||
        initialFootprint.zoneRoutes.length > 0 ||
        initialNamespaceIds.length > 0
      ) {
        throw new Error(
          `refusing to delete an ordinary Worker footprint without an attestable Worker owner`,
        );
      }
      return;
    }
    if (
      !initialFootprint.scriptPresent ||
      initialFootprint.zoneRoutes.length > 0
    ) {
      throw new Error(
        `refusing to delete Worker '${spec.scriptName}' with an inconsistent script or zone-route footprint`,
      );
    }
    const unexpectedDomains = (await this.#routeApi.listCustomDomains()).filter(
      (domain) =>
        domain.service === spec.scriptName &&
        domain.hostname.toLowerCase() !== spec.routeHostname.toLowerCase(),
    );
    if (unexpectedDomains.length > 0) {
      throw new Error(
        `refusing to delete Worker '${spec.scriptName}' with unexpected custom domains`,
      );
    }
    if (
      !(await this.#attestPersistedWorkerOwnership(
        spec,
        database.id,
        retainedReleases,
        activeRelease,
      ))
    )
      return;
    try {
      await this.#runMutation(fence, [
        'delete',
        '--name',
        spec.scriptName,
        '--force',
      ]);
    } catch (error) {
      if (!isWranglerNotFound(error)) throw error;
    }
    const [
      status,
      versions,
      residualRoute,
      residualWorkerDomains,
      footprint,
      residualNamespaceIds,
    ] = await Promise.all([
      this.#deploymentStatus(spec),
      this.#listVersions(spec),
      this.#customDomain(spec.routeHostname),
      this.#routeApi
        .listCustomDomains()
        .then((domains) =>
          domains.filter((domain) => domain.service === spec.scriptName),
        ),
      this.#routeApi.inspectOrdinaryWorkerFootprint(spec.scriptName),
      this.#routeApi.listDurableObjectNamespaces(spec.scriptName),
    ]);
    if (
      status ||
      (versions && versions.length > 0) ||
      residualRoute ||
      residualWorkerDomains.length > 0 ||
      footprint.scriptPresent ||
      footprint.customDomains.length > 0 ||
      footprint.zoneRoutes.length > 0 ||
      residualNamespaceIds.length > 0
    ) {
      throw new Error(
        `Worker '${spec.scriptName}' or its custom domain remains after delete`,
      );
    }
  }

  async exportDatabase(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<DatabaseExport> {
    await mkdir(this.#exportDirectory, { recursive: true });
    const temporaryDirectory = await mkdtemp(
      join(this.#exportDirectory, '.wrangler-export-'),
    );
    const fileName = `${database.name}-${Date.now()}.sql`;
    const temporaryLocation = join(temporaryDirectory, fileName);
    try {
      await this.#runMutation(fence, [
        'd1',
        'export',
        database.id,
        '--remote',
        '--skip-confirmation',
        '--output',
        temporaryLocation,
      ]);
      await chmod(temporaryLocation, 0o600);
      const metadata = await stat(temporaryLocation);
      if (!metadata.isFile() || metadata.size === 0) {
        throw new Error('Wrangler database export is not a non-empty file');
      }
      const hash = createHash('sha256');
      for await (const chunk of createReadStream(temporaryLocation)) {
        hash.update(chunk);
      }
      const sha256 = hash.digest('hex');
      const stored = await this.#exportStore.write({
        databaseId: database.id,
        fileName,
        body: Readable.toWeb(
          createReadStream(temporaryLocation),
        ) as ReadableStream<Uint8Array>,
        contentLength: metadata.size,
      });
      if (
        !stored.location ||
        stored.size !== metadata.size ||
        stored.sha256 !== sha256
      ) {
        throw new Error(
          'durable database export store returned mismatched committed integrity',
        );
      }
      return {
        databaseId: database.id,
        location: stored.location,
        sha256,
        size: metadata.size,
      };
    } finally {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
  }

  async deleteDatabase(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    try {
      await this.#runMutation(fence, [
        'd1',
        'delete',
        database.id,
        '--skip-confirmation',
      ]);
    } catch (error) {
      if (!isWranglerNotFound(error)) throw error;
    }
  }
}
