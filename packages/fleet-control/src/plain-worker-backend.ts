// SPDX-License-Identifier: Apache-2.0

import {
  provisionDeploymentIdentityProtocol,
  readDeploymentIdentityProtocol,
} from '@proofoftech/flowsafe/deployment-identity-protocol';
import { ActiveRouteAttestationError } from './active-route.js';
import {
  applicationSecretValues,
  canonicalApplicationBindings,
} from './application-bindings.js';
import {
  captureDatabaseExportReceiptCapability,
  databaseExportReceiptIdentityFromUnknown,
} from './database-export-store.js';
import { isSha256 } from './deployment-context.js';
import { WorkerDeploymentError } from './deployment-error.js';
import { maintenanceUrl, readMaintenanceHealth } from './maintenance-health.js';
import { applyMigrationsWithLedger } from './migration-ledger.js';
import { assertSupportedPlainWorkerBindings } from './provider-binding-inventory.js';
import { deploymentSpecDigest } from './spec-digest.js';
import type {
  ActiveRouteAttestation,
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
  ForceDecommissionStep,
  LiveDeployment,
  MaintenanceHealth,
  PlainWorkerCustomDomain,
  PlainWorkerProvisioningApi,
  PlainWorkerUploadIntent,
  PlainWorkerUploadOutcome,
  PlainWorkerVersionDetail,
  PlainWorkerVersionSummary,
  PromotionGuard,
  ProvisioningBackend,
  SeedDeploymentIdentityOptions,
} from './types.js';
import { targetDurableObjectTag } from './validation.js';

const PLAIN_INGRESS_CONTRACT = 'guarded-object-v1';
const PLAIN_INGRESS_MODULE = '__anchorage_guarded_entry__.js';
const EXACT_DATABASE_DELETION_REQUIRED =
  'plain Worker route API does not support exact-ID D1 database deletion';
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

function restD1Bindings(
  bindings: readonly unknown[],
  operation: string,
): readonly string[] {
  if (bindings.some((binding) => typeof binding !== 'string')) {
    throw new Error(`${operation} D1 bindings must be strings`);
  }
  return bindings as readonly string[];
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

/**
 * Constructor options for the shared ordinary-Worker implementation.
 */
export interface PlainWorkerBackendOptions {
  /** Provider operations used by the shared ordinary-Worker policy. */
  readonly api: PlainWorkerProvisioningApi;
  /**
   * Log-hygiene token prefixed to deployment-identity protocol refusals. The
   * protocol uses `caller` as its diagnostic prefix and defaults to its own
   * implementation token when omitted. This value is never persisted.
   */
  readonly identityCaller: string;
  readonly fetch?: typeof fetch;
  readonly maintenanceRequestTimeoutMs?: number;
  /** Stamps `observedAt` on an attestation. Injected so it can be pinned. */
  readonly clock?: () => number;
}

/**
 * Validate the maintenance request timeout before the wrapper constructs its
 * adapter, preserving the historic constructor guard order. This helper is
 * exported for both built-in ordinary-Worker backends but intentionally
 * omitted from the package barrel.
 */
export function resolveMaintenanceRequestTimeoutMs(
  value: number | undefined,
): number {
  const maintenanceRequestTimeoutMs = value ?? 30_000;
  if (
    !Number.isSafeInteger(maintenanceRequestTimeoutMs) ||
    maintenanceRequestTimeoutMs < 1
  ) {
    throw new Error('maintenance request timeout must be positive');
  }
  return maintenanceRequestTimeoutMs;
}

/**
 * Shared provider-neutral ordinary-Worker implementation over
 * `PlainWorkerProvisioningApi`.
 *
 * Its constructor options are the stable integration surface for built-in
 * ordinary-Worker backends. Its public members are invoked by the class
 * itself — database and R2 reconciliation, failed-deployment rollback, and
 * teardown — so overriding any of them changes behavior the class depends on
 * internally; do not subclass it outside this package.
 *
 * Mutation duration has two independent bounds. Port-level operations use
 * `maxMutationDurationMs`, while maintenance requests use the injected request
 * timeout. Both must remain below the external mutation-fence lease lifetime.
 *
 * The route contract asserts the fence immediately before every provider
 * request. This class also retains historic pre-assertions before promotion's
 * custom-domain attach, normal traffic removal's detach, and the maintenance
 * request before dispatch. Force detach, public-access disable, and secret
 * deletion rely on the route contract.
 *
 * The port's explicit pre-dispatch assertions are also load-bearing for
 * outcome-valued database creation, candidate upload, and deployment creation.
 * Without them, a lost lease represented as `{ status: 'failed' }` could be
 * masked by the core's provider readback and reconciliation.
 */
export class PlainWorkerBackend implements ProvisioningBackend {
  readonly kind = 'plain-worker' as const;
  declare readonly advanceDecommissionAttachmentScan?: NonNullable<
    ProvisioningBackend['advanceDecommissionAttachmentScan']
  >;
  /** Canonical immutable receipt authority, present only with receipt export. */
  declare readonly databaseExportReceiptAuthority?: string;
  /** Forwards one canonical operation receipt through the plain-Worker port. */
  declare readonly exportDatabaseReceipt?: NonNullable<
    ProvisioningBackend['exportDatabaseReceipt']
  >;
  readonly #api: PlainWorkerProvisioningApi;
  readonly #identityCaller: string;
  readonly #fetch: typeof fetch;
  readonly #maintenanceRequestTimeoutMs: number;
  readonly #clock: () => number;

  constructor(options: PlainWorkerBackendOptions) {
    const maintenanceRequestTimeoutMs = resolveMaintenanceRequestTimeoutMs(
      options.maintenanceRequestTimeoutMs,
    );
    if (
      typeof options.identityCaller !== 'string' ||
      !/^[\x21-\x7e]{1,128}$/u.test(options.identityCaller)
    ) {
      throw new Error(
        'plain Worker backend identityCaller must be a 1-128 character single-line token',
      );
    }
    this.#api = options.api;
    this.#identityCaller = options.identityCaller;
    this.#fetch = options.fetch ?? fetch;
    this.#maintenanceRequestTimeoutMs = maintenanceRequestTimeoutMs;
    this.#clock = options.clock ?? Date.now;
    const advanceDecommissionAttachmentScan =
      options.api.advanceDecommissionAttachmentScan;
    if (typeof advanceDecommissionAttachmentScan === 'function') {
      this.advanceDecommissionAttachmentScan =
        advanceDecommissionAttachmentScan.bind(options.api);
    }
    const receiptCapability = captureDatabaseExportReceiptCapability(
      options.api,
      () => [
        options.api.databaseExportReceiptAuthority,
        options.api.exportDatabaseReceipt,
      ],
    );
    if (receiptCapability) {
      const exportDatabaseReceipt = receiptCapability.method as NonNullable<
        PlainWorkerProvisioningApi['exportDatabaseReceipt']
      >;
      this.databaseExportReceiptAuthority = receiptCapability.authority;
      this.exportDatabaseReceipt = async (identity, fence) => {
        this.#assertMutationDuration(fence);
        const canonical = databaseExportReceiptIdentityFromUnknown(
          identity,
          receiptCapability.authority,
        );
        const exported = await exportDatabaseReceipt(canonical, fence);
        return {
          databaseId: canonical.databaseId,
          location: exported.location,
          sha256: exported.sha256,
          size: exported.size,
        };
      };
    }
  }

  #assertMutationDuration(fence: ExternalMutationFence): void {
    if (
      !Number.isSafeInteger(fence.mutationLeaseTtlMs) ||
      fence.mutationLeaseTtlMs < 1
    ) {
      throw new Error('external mutation fence lease TTL must be positive');
    }
    if (
      !Number.isSafeInteger(this.#api.maxMutationDurationMs) ||
      this.#api.maxMutationDurationMs < 1
    ) {
      throw new Error('provider mutation maximum duration must be positive');
    }
    if (this.#api.maxMutationDurationMs >= fence.mutationLeaseTtlMs) {
      throw new Error(
        'provider mutation maximum duration must be below the external mutation fence lease TTL',
      );
    }
  }

  async #assertMutationFence(fence: ExternalMutationFence): Promise<void> {
    this.#assertMutationDuration(fence);
    await fence.assertOwned();
  }

  async findDatabase(
    spec: DeploymentSpec,
  ): Promise<DatabaseReference | undefined> {
    const listed = await this.#api.listDatabases({ name: spec.databaseName });
    // A name filter narrows the listing toward the name, so compare exactly.
    const matches = listed.filter(
      (database) => database.name === spec.databaseName,
    );
    if (matches.length > 1) {
      throw new Error(`multiple D1 databases are named '${spec.databaseName}'`);
    }
    if (matches[0]) {
      const id = matches[0].databaseId;
      if (!id) throw new Error('D1 list result has no uuid');
      return { id, name: spec.databaseName, created: false };
    }
    return undefined;
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    return this.#api.getDatabase(databaseId);
  }

  async ensureDatabase(
    spec: DeploymentSpec,
    fence: ExternalMutationFence,
  ): Promise<DatabaseReference> {
    this.#assertMutationDuration(fence);
    const outcome = await this.#api.createDatabase(spec.databaseName, fence);
    if (outcome.status === 'failed') {
      const recovered = await this.findDatabase(spec);
      if (recovered) {
        const owner = await this.readDeploymentIdentity(recovered, fence);
        if (owner !== undefined) {
          throw new Error(
            `refusing authorized database reconciliation for '${recovered.id}' owned by '${owner}'`,
            { cause: outcome.error },
          );
        }
        return { ...recovered, created: true };
      }
      throw outcome.error;
    }
    const resolved = await this.findDatabase(spec);
    if (!resolved) {
      throw new Error(
        `D1 database '${spec.databaseName}' is absent after successful creation`,
      );
    }
    return { ...resolved, created: true };
  }

  async #query(
    database: DatabaseReference,
    sql: string,
    fence: ExternalMutationFence,
    bindings: readonly unknown[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    return this.#api.withMutationFence(fence, () =>
      this.#api.queryDatabase(
        database.id,
        sql,
        restD1Bindings(bindings, 'plain Worker'),
      ),
    );
  }

  async seedDeploymentIdentity(
    database: DatabaseReference,
    tenantTag: string,
    fence: ExternalMutationFence,
    options: SeedDeploymentIdentityOptions,
  ): Promise<void> {
    await provisionDeploymentIdentityProtocol(
      (statement) =>
        this.#query(database, statement.sql, fence, statement.bindings),
      tenantTag,
      {
        caller: this.#identityCaller,
        initialExecutionFenceState: options.initialExecutionFenceState,
      },
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
        batch: (statements) =>
          this.#api.withMutationFence(fence, () =>
            this.#api.batchDatabase(
              database.id,
              statements.map((statement) => ({
                sql: statement.sql,
                bindings: restD1Bindings(
                  statement.bindings ?? [],
                  'plain Worker batch',
                ),
              })),
            ),
          ),
      },
      migrations,
    );
  }

  async findApplicationR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined> {
    if (!this.#api.getR2Bucket) {
      throw new Error('plain Worker route API does not support application R2');
    }
    const found = await this.#api.getR2Bucket(
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
    if (!this.#api.createR2Bucket) {
      throw new Error('plain Worker route API does not support application R2');
    }
    await fence.assertOwned();
    try {
      await this.#api.createR2Bucket(resource, fence);
    } catch (error) {
      await fence.assertOwned();
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
    if (!this.#api.listWorkerR2Attachments) {
      throw new Error('plain Worker route API cannot scan R2 attachments');
    }
    const attachments = await this.#api.listWorkerR2Attachments(
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
    if (!this.#api.assertR2BucketEmpty) {
      throw new Error('plain Worker route API cannot inspect R2 contents');
    }
    await this.#api.assertR2BucketEmpty(resource);
  }

  async deleteApplicationR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void> {
    if (!this.#api.deleteR2Bucket) {
      throw new Error('plain Worker route API cannot delete application R2');
    }
    const current = await this.findApplicationR2Bucket(resource);
    if (!current || current.creationDate !== resource.creationDate) {
      throw new Error(`R2 bucket '${resource.bucketName}' ownership changed`);
    }
    await this.#api.deleteR2Bucket(resource, fence);
    if (await this.findApplicationR2Bucket(resource)) {
      throw new Error(
        `R2 bucket '${resource.bucketName}' remains after delete`,
      );
    }
  }

  async #deploymentStatus(
    spec: DeploymentSpec,
  ): Promise<DeploymentStatus | undefined> {
    const status = await this.#api.deploymentStatus(spec.scriptName);
    if (!status) return undefined;
    const versions = status.versions.map(({ versionId: id, percentage }) => {
      if (
        !id ||
        percentage === undefined ||
        !Number.isFinite(percentage) ||
        percentage < 0
      ) {
        throw new Error(
          'plain Worker deployment status has an invalid version',
        );
      }
      return { id, percentage };
    });
    if (versions.length === 0) {
      throw new Error('plain Worker deployment status has no versions');
    }
    return { versions };
  }

  async #listVersions(
    spec: DeploymentSpec,
  ): Promise<readonly PlainWorkerVersionSummary[] | undefined> {
    return this.#api.listVersions(spec.scriptName);
  }

  #plainTextBindings(
    version: PlainWorkerVersionDetail,
  ): ReadonlyMap<string, string> {
    return new Map(
      version.bindings.flatMap((binding) =>
        binding.type === 'plain-text' &&
        typeof binding.name === 'string' &&
        typeof binding.value === 'string'
          ? [[binding.name, binding.value] as const]
          : [],
      ),
    );
  }

  async #matchingCandidateIds(
    spec: DeploymentSpec,
    versions?: readonly PlainWorkerVersionSummary[],
  ): Promise<readonly string[]> {
    const digest = deploymentSpecDigest(spec);
    const listed = versions ?? (await this.#listVersions(spec));
    if (!listed) return [];
    const tagged = listed.filter((version) => version.tag === digest);
    const matches: string[] = [];
    for (const candidate of tagged) {
      const id = candidate.versionId;
      if (!id) {
        throw new Error('plain Worker version inventory has no version id');
      }
      const version = await this.#api.viewVersion(spec.scriptName, id);
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
    versions?: readonly PlainWorkerVersionSummary[],
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
    versions?: readonly PlainWorkerVersionSummary[],
  ): Promise<string> {
    const listed = versions ?? (await this.#listVersions(spec));
    if (!listed?.some((version) => version.versionId === artifactVersion)) {
      throw new Error(
        `Worker '${spec.scriptName}' is missing persisted artifact version '${artifactVersion}'`,
      );
    }
    const version = await this.#api.viewVersion(
      spec.scriptName,
      artifactVersion,
    );
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

  #databaseIds(version: PlainWorkerVersionDetail): readonly string[] {
    return version.bindings.flatMap((binding) =>
      binding.type === 'd1' && typeof binding.databaseId === 'string'
        ? [binding.databaseId]
        : [],
    );
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
      const version = await this.#api.viewVersion(spec.scriptName, deployed.id);
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
      this.#api.viewVersion(spec.scriptName, candidateId),
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
      const deployedVersion = await this.#api.viewVersion(
        spec.scriptName,
        deployed.id,
      );
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
      const version = await this.#api.viewVersion(spec.scriptName, deployed.id);
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

  async #attestTeardownWorkerOwnership(
    spec: DeploymentSpec,
    databaseId: string,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
  ): Promise<string | undefined> {
    const releases = [activeRelease, ...(retainedReleases ?? [])].filter(
      (release): release is ExternalReleaseSnapshot => release !== undefined,
    );
    const allowed = releases.filter(
      (release) => release.physicalScriptName === spec.scriptName,
    );
    if (releases.length > 0 && allowed.length === 0) {
      throw new Error(
        `refusing to mutate Worker '${spec.scriptName}' without a matching persisted release`,
      );
    }
    const [status, versions, footprint, namespaceIds] = await Promise.all([
      this.#deploymentStatus(spec),
      this.#listVersions(spec),
      this.#api.inspectOrdinaryWorkerFootprint(spec.scriptName),
      this.#api.listDurableObjectNamespaces(spec.scriptName),
    ]);
    const listed = versions ?? [];
    if (!status && listed.length === 0) {
      if (
        footprint.scriptPresent ||
        footprint.customDomains.length > 0 ||
        footprint.zoneRoutes.length > 0 ||
        namespaceIds.length > 0
      ) {
        throw new Error(
          `refusing to mutate Worker '${spec.scriptName}' with an inconsistent script footprint`,
        );
      }
      return undefined;
    }
    if (!status || !footprint.scriptPresent || listed.length === 0) {
      throw new Error(
        `refusing to mutate Worker '${spec.scriptName}' without consistent live deployment, version, and provider footprints`,
      );
    }
    const validInventory = listed.map((version) => {
      if (!version.versionId) {
        throw new Error(
          `refusing to mutate Worker '${spec.scriptName}' with an invalid version inventory`,
        );
      }
      return { id: version.versionId, tag: version.tag };
    });
    const listedIds = validInventory.map(({ id }) => id);
    if (new Set(listedIds).size !== listedIds.length) {
      throw new Error(
        `refusing to mutate Worker '${spec.scriptName}' with a duplicate version inventory`,
      );
    }
    type ReleaseIdentity = Readonly<{
      specDigest: string;
      releaseSchemaVersion: number;
    }>;
    const desiredIdentity: ReleaseIdentity = {
      specDigest: deploymentSpecDigest(spec),
      releaseSchemaVersion: spec.schemaVersion,
    };
    const viewedVersions = new Map<string, PlainWorkerVersionDetail>();
    const remember = (
      id: string,
      version: PlainWorkerVersionDetail,
    ): PlainWorkerVersionDetail => {
      viewedVersions.set(id, version);
      return version;
    };
    const view = async (id: string): Promise<PlainWorkerVersionDetail> => {
      const cached = viewedVersions.get(id);
      if (cached !== undefined) return cached;
      const version = await this.#api.viewVersion(spec.scriptName, id);
      return remember(id, version);
    };
    const find = async (
      id: string,
    ): Promise<PlainWorkerVersionDetail | undefined> => {
      const cached = viewedVersions.get(id);
      if (cached !== undefined) return cached;
      const version = await this.#api.findVersion(spec.scriptName, id);
      return version ? remember(id, version) : undefined;
    };
    const assertIdentity = (
      version: PlainWorkerVersionDetail,
      expectedReleases: readonly ReleaseIdentity[],
    ): void => {
      const plainText = this.#plainTextBindings(version);
      const databaseIds = this.#databaseIds(version);
      const release = expectedReleases.find(
        (candidate) =>
          candidate.specDigest === plainText.get('FLEET_SPEC_DIGEST') &&
          String(candidate.releaseSchemaVersion) ===
            plainText.get('FLEET_SCHEMA_VERSION'),
      );
      if (
        !release ||
        databaseIds.length !== 1 ||
        databaseIds[0] !== databaseId ||
        plainText.get('DEPLOYMENT_TENANT') !== spec.tenantTag ||
        plainText.get('FLEET_ENVIRONMENT') !== spec.environment ||
        plainText.get('FLEET_INGRESS_CONTRACT') !== PLAIN_INGRESS_CONTRACT
      ) {
        throw new Error(
          `refusing to mutate Worker '${spec.scriptName}' with drifted live teardown ownership`,
        );
      }
    };
    let anchorId: string | undefined;
    if (allowed.length > 0) {
      for (const release of allowed) {
        const anchor = await find(release.artifactVersion);
        if (!anchor) continue;
        assertIdentity(anchor, [release]);
        anchorId = release.artifactVersion;
        break;
      }
    } else {
      const taggedAnchor = validInventory.find(
        ({ tag }) => tag === desiredIdentity.specDigest,
      );
      if (taggedAnchor) {
        assertIdentity(await view(taggedAnchor.id), [desiredIdentity]);
        anchorId = taggedAnchor.id;
      }
    }
    if (!anchorId) {
      throw new Error(
        `refusing to mutate Worker '${spec.scriptName}' without a trusted artifact version anchor`,
      );
    }
    const expectedReleases: readonly ReleaseIdentity[] =
      allowed.length > 0 ? allowed : [desiredIdentity];
    for (const deployed of status.versions) {
      assertIdentity(await view(deployed.id), expectedReleases);
    }
    return status.versions[0]?.id;
  }

  async #assertDatabaseResidualIdentity(
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
  }

  async #assertDatabaseResidualTail(
    spec: DeploymentSpec,
    record: FleetRecord,
    fence: ExternalMutationFence,
  ): Promise<void> {
    const [status, versions, domains, footprint, namespaceIds] =
      await Promise.all([
        this.#deploymentStatus(spec),
        this.#listVersions(spec),
        this.#api.listCustomDomains(),
        this.#api.inspectOrdinaryWorkerFootprint(spec.scriptName),
        this.#api.listDurableObjectNamespaces(spec.scriptName),
      ]);
    const routeFootprint = domains.filter(
      (domain) =>
        domain.service === spec.scriptName ||
        domain.hostname.toLowerCase() === spec.routeHostname.toLowerCase(),
    );
    const hasProviderVersions = Boolean(versions && versions.length > 0);
    if (!status && !hasProviderVersions && !footprint.scriptPresent) {
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
    if (!status && !hasProviderVersions && footprint.scriptPresent) {
      throw new Error(
        `database '${record.databaseId}' has an ordinary Worker footprint that the provider cannot attest`,
      );
    }
    if ((status || hasProviderVersions) && !footprint.scriptPresent) {
      throw new Error(
        `database '${record.databaseId}' has inconsistent authoritative and provider Worker footprints`,
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
        this.#api.listCustomDomains(),
        this.#api.inspectOrdinaryWorkerFootprint(spec.scriptName),
        this.#api.listDurableObjectNamespaces(spec.scriptName),
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

  async assertDatabaseDeletionResidualsRemoved(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#assertDatabaseResidualIdentity(spec, record, database, fence);
    await this.#assertDatabaseResidualTail(spec, record, fence);
  }

  async assertDatabaseDetached(
    spec: DeploymentSpec,
    record: FleetRecord,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#assertDatabaseResidualIdentity(spec, record, database, fence);
    const databaseAttachments = await this.#api.listWorkerDatabaseAttachments(
      database.id,
    );
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
    await this.#assertDatabaseResidualTail(spec, record, fence);
  }

  async #customDomain(
    hostname: string,
  ): Promise<PlainWorkerCustomDomain | undefined> {
    const normalized = hostname.toLowerCase();
    const matches = (await this.#api.listCustomDomains()).filter(
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
    this.#assertMutationDuration(fence);
    const outcome = await this.#api.createDeployment(
      spec.scriptName,
      [
        ...current.versions.map((version) => ({
          versionId: version.id,
          percentage: version.percentage,
        })),
        { versionId: candidateId, percentage: 0 },
      ],
      fence,
    );
    if (outcome.status === 'failed') {
      const reconciled = await this.#deploymentStatus(spec);
      if (!reconciled?.versions.some((version) => version.id === candidateId)) {
        throw outcome.error;
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
      throw new Error(
        'plain Worker backend refuses externally authored Workers',
      );
    }
    if (
      spec.durableObjectBindings.some(
        (binding) =>
          binding.scriptName !== undefined ||
          binding.dispatchNamespace !== undefined,
      )
    ) {
      throw new Error(
        'plain Worker backend supports only local Durable Object bindings',
      );
    }
    const deployment = await this.#deploymentStatus(spec);
    const versions = await this.#listVersions(spec);
    const workerExisted = deployment !== undefined || versions !== undefined;
    const priorVersionIds = new Set(
      (versions ?? []).map((version) => {
        if (!version.versionId) {
          throw new Error('plain Worker version inventory has no version id');
        }
        return version.versionId;
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
    const ingressModule = plainWorkerIngressModule(spec);
    const digest = deploymentSpecDigest(spec);
    const mode = deployment === undefined ? 'initial' : 'staged';
    const publicAccess: PlainWorkerUploadIntent['publicAccess'] = {
      workersDevEnabled: true,
      previewUrlsEnabled: false,
    };
    let uploadOutcome: PlainWorkerUploadOutcome | undefined;
    if (!candidateId) {
      this.#assertMutationDuration(fence);
      uploadOutcome = await this.#api.uploadCandidate(
        {
          scriptName: spec.scriptName,
          candidateTag: digest,
          mainModule: ingressModule.name,
          modules: [...spec.modules, ingressModule],
          compatibilityDate: spec.compatibilityDate,
          compatibilityFlags: spec.compatibilityFlags,
          bindings: {
            plainText: [
              { name: 'DEPLOYMENT_TENANT', value: spec.tenantTag },
              { name: 'FLEET_ENVIRONMENT', value: spec.environment },
              {
                name: 'FLEET_SCHEMA_VERSION',
                value: String(spec.schemaVersion),
              },
              { name: 'FLEET_SPEC_DIGEST', value: digest },
              {
                name: 'FLEET_INGRESS_CONTRACT',
                value: PLAIN_INGRESS_CONTRACT,
              },
              ...(application?.vars ?? canonicalApplicationBindings(spec).vars),
            ],
            secrets: [
              {
                name: 'DEPLOYMENT_IDENTITY_SECRET',
                value: secrets.deploymentIdentity,
              },
              {
                name: 'MAINTENANCE_ADMIN_SECRET',
                value: secrets.maintenanceAdmin,
              },
              ...Object.entries(applicationSecretValues(spec, secrets)).map(
                ([name, value]) => ({ name, value }),
              ),
            ],
            d1: [
              {
                name: 'DB',
                databaseName: database.name,
                databaseId: database.id,
              },
            ],
            durableObjects: spec.durableObjectBindings.map((binding) => ({
              name: binding.name,
              className: binding.className,
            })),
            services: spec.egressProxyService
              ? [
                  {
                    name: 'EGRESS_PROXY',
                    service: spec.egressProxyService,
                  },
                ]
              : [],
            queueProducers: spec.queueProducer
              ? [
                  {
                    name: spec.queueProducer.binding,
                    queueName: spec.queueProducer.queueName,
                  },
                ]
              : [],
            r2Buckets: (application?.r2Buckets ?? []).map((binding) => ({
              name: binding.name,
              bucketName: binding.bucketName,
            })),
          },
          limits: { cpuMs: spec.cpuLimitMs },
          publicAccess,
          ...(mode === 'initial'
            ? {
                mode,
                durableObjectMigrations: spec.durableObjectMigrations,
              }
            : { mode }),
        },
        fence,
      );
    }
    let settled:
      | Readonly<{
          ok: true;
          result: Readonly<{ artifactVersion: string; created: boolean }>;
        }>
      | Readonly<{
          ok: false;
          error: Readonly<{
            message: string;
            cause: unknown;
            createdByAttempt: boolean;
            resourceState: 'absent' | 'present' | 'unknown';
          }>;
        }>;
    try {
      if (!candidateId) {
        const operationCandidates = (
          await this.#matchingCandidateIds(spec)
        ).filter((id) => !priorVersionIds.has(id));
        if (operationCandidates.length !== 1) {
          if (uploadOutcome?.status === 'failed' && uploadOutcome.error) {
            throw uploadOutcome.error;
          }
          // A falsy rejection value carries no diagnostic; keep the rediscovery
          // error (pre-port parity).
          throw new Error(
            `${mode} Worker upload did not create exactly one new tagged Worker version`,
          );
        }
        const operationCandidate = operationCandidates[0];
        if (!operationCandidate) {
          throw new Error('new Worker candidate has no artifact version');
        }
        if (uploadOutcome?.status === 'failed') {
          const footprint = await this.#api.inspectOrdinaryWorkerFootprint(
            spec.scriptName,
          );
          if (
            footprint.workersDevEnabled !== publicAccess.workersDevEnabled ||
            footprint.previewUrlsEnabled !== publicAccess.previewUrlsEnabled
          ) {
            throw new Error(
              `reconciled Worker upload for '${spec.scriptName}' did not converge public access`,
            );
          }
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
      settled = {
        ok: true,
        result: { artifactVersion: candidateId, created: !workerExisted },
      };
    } catch (cause) {
      if (workerExisted) {
        settled = {
          ok: false,
          error: {
            message: `failed to update existing Worker '${spec.scriptName}'`,
            cause,
            createdByAttempt: false,
            resourceState: 'present',
          },
        };
      } else {
        const cleanupErrors: unknown[] = [];
        const cleanupRelease: ExternalReleaseSnapshot | undefined = candidateId
          ? {
              physicalScriptName: spec.scriptName,
              specDigest: digest,
              artifactVersion: candidateId,
              releaseSchemaVersion: spec.schemaVersion,
            }
          : undefined;
        let trafficRemoved = false;
        try {
          await this.removeTraffic(
            spec,
            undefined,
            cleanupRelease,
            database,
            fence,
          );
          await this.assertTrafficRemoved(spec);
          trafficRemoved = true;
        } catch (cleanupError) {
          cleanupErrors.push(cleanupError);
        }
        if (trafficRemoved) {
          try {
            await this.revokeCredentials(
              spec,
              undefined,
              cleanupRelease,
              database,
              fence,
            );
          } catch (cleanupError) {
            cleanupErrors.push(cleanupError);
          }
          try {
            await this.deleteWorker(
              spec,
              undefined,
              database,
              cleanupRelease,
              fence,
            );
          } catch (cleanupError) {
            cleanupErrors.push(cleanupError);
          }
        }
        settled = {
          ok: false,
          error:
            cleanupErrors.length > 0
              ? {
                  message: `failed to install credentials and clean up '${spec.scriptName}'`,
                  cause: new AggregateError([cause, ...cleanupErrors]),
                  createdByAttempt: true,
                  resourceState: 'unknown',
                }
              : {
                  message: `failed to install Worker '${spec.scriptName}'`,
                  cause,
                  createdByAttempt: true,
                  resourceState: 'absent',
                },
        };
      }
    }
    if (!settled.ok) {
      const record = settled.error;
      const cause =
        uploadOutcome?.cleanup.status === 'failed'
          ? new AggregateError(
              [record.cause, uploadOutcome.cleanup.error],
              'Worker upload and adapter scratch cleanup both failed',
            )
          : record.cause;
      throw new WorkerDeploymentError({ ...record, cause });
    }
    if (uploadOutcome?.cleanup.status === 'failed') {
      throw new WorkerDeploymentError({
        message: `installed Worker '${spec.scriptName}' but failed to clean up the adapter credential scratch`,
        cause: uploadOutcome.cleanup.error,
        createdByAttempt: !workerExisted,
        resourceState: 'present',
      });
    }
    return settled.result;
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
      this.#assertMutationDuration(fence);
      const outcome = await this.#api.createDeployment(
        spec.scriptName,
        [{ versionId: candidateId, percentage: 100 }],
        fence,
      );
      if (outcome.status === 'failed') {
        const reconciled = await this.#deploymentStatus(spec);
        if (
          reconciled?.versions.length !== 1 ||
          reconciled.versions[0]?.id !== candidateId ||
          reconciled.versions[0].percentage !== 100
        ) {
          throw outcome.error;
        }
      }
    }
    const beforeAttach = await this.#attestPromotionRoute(spec, guard);
    if (beforeAttach?.service !== spec.scriptName) {
      await this.#assertMutationFence(fence);
      await this.#api.attachCustomDomain(
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
      throw new Error('plain Worker deployment status has no active version');
    }
    const version = await this.#api.viewVersion(
      spec.scriptName,
      artifactVersion,
    );
    const databaseIds = this.#databaseIds(version);
    const durableObjectBindings = version.bindings.flatMap((binding) =>
      binding.type === 'durable-object' &&
      typeof binding.namespaceId === 'string' &&
      typeof binding.name === 'string' &&
      typeof binding.className === 'string'
        ? [
            {
              name: binding.name,
              className: binding.className,
              namespaceId: binding.namespaceId,
            },
          ]
        : [],
    );
    const serviceBindings = version.bindings.flatMap((binding) =>
      binding.type === 'service' &&
      typeof binding.name === 'string' &&
      typeof binding.service === 'string'
        ? [{ name: binding.name, service: binding.service }]
        : [],
    );
    const queueProducerBindings = version.bindings.flatMap((binding) =>
      binding.type === 'queue-producer' &&
      typeof binding.name === 'string' &&
      typeof binding.queueName === 'string'
        ? [{ name: binding.name, queueName: binding.queueName }]
        : [],
    );
    const r2BucketBindings = version.bindings
      .flatMap((binding) =>
        binding.type === 'r2-bucket' &&
        typeof binding.name === 'string' &&
        typeof binding.bucketName === 'string'
          ? [
              {
                name: binding.name,
                bucketName: binding.bucketName,
                jurisdiction: 'default' as const,
              },
            ]
          : [],
      )
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
    const versionBindingIdentities = assertSupportedPlainWorkerBindings(
      version.bindings,
      `plain Worker '${spec.scriptName}'`,
    );
    const secretNames = await this.#api.listOrdinaryWorkerSecretNames(
      spec.scriptName,
    );
    const versionSecretNames = versionBindingIdentities
      .filter(({ type }) => type === 'secret_text')
      .map(({ name }) => name)
      .sort();
    if (
      versionSecretNames.length > 0 &&
      JSON.stringify(versionSecretNames) !==
        JSON.stringify([...secretNames].sort())
    ) {
      throw new Error(
        `plain Worker '${spec.scriptName}' version and secret inventories disagree`,
      );
    }
    const providerBindingIdentities = [
      ...versionBindingIdentities.filter(({ type }) => type !== 'secret_text'),
      ...secretNames.map((name) => ({ type: 'secret_text', name }) as const),
    ].sort((left, right) =>
      `${left.type}\u0000${left.name}`.localeCompare(
        `${right.type}\u0000${right.name}`,
      ),
    );
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
      plainTextBindings: Object.fromEntries(plainText),
      ...(r2BucketBindings.length > 0 ? { r2BucketBindings } : {}),
      secretNames,
      providerBindingIdentities,
      artifactVersion,
      desiredSpecDigest,
      schemaVersion,
      maintenance,
    };
  }

  /**
   * Attest the version serving traffic, which for an ordinary Worker is the
   * one the deployment object holds at 100%.
   *
   * `inspect()` cannot answer this. Given an expected artifact version it pins
   * that candidate even while the candidate sits at 0%, because a converge has
   * to compare a staged upload against the specification before promoting it.
   * Reusing it here would report an unpromoted candidate as though it were
   * live, which is the exact failure this method exists to make impossible.
   *
   * The read goes through the port, so it is quota-coordinated like every other
   * provider read.
   *
   * `physicalScriptName` is the spec's script rather than a value read back
   * from the custom domain, because the hostname-to-script binding is already
   * enforced on the path that can change it: `promoteWorker` fails unless the
   * custom domain attests this exact script after every promotion, and
   * `#attestPromotionRoute` refuses a hostname owned by a Worker outside the
   * promotion guard. Re-reading the domain here would spend a third provider
   * call against the two-read budget this method documents and learn nothing
   * those two checks have not already established.
   */
  async attestActiveRoute(
    spec: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    const active = await this.#api.inspectActiveWorkerRoute(spec.scriptName);
    if (!active) {
      throw new ActiveRouteAttestationError(
        `Worker '${spec.scriptName}' has no deployment serving traffic`,
        {},
      );
    }
    if (!active.specDigest || !isSha256(active.specDigest)) {
      throw new ActiveRouteAttestationError(
        `routed version '${active.artifactVersion}' of Worker '${spec.scriptName}' carries no fleet specification digest`,
        {
          routedScriptName: spec.scriptName,
          artifactVersion: active.artifactVersion,
        },
      );
    }
    return {
      specDigest: active.specDigest,
      artifactVersion: active.artifactVersion,
      physicalScriptName: spec.scriptName,
      source: 'workers-deployments',
      observedAt: new Date(this.#clock()).toISOString(),
    };
  }

  async revokeCredentials(
    spec: DeploymentSpec,
    retainedReleases: readonly ExternalReleaseSnapshot[] | undefined,
    activeRelease: ExternalReleaseSnapshot | undefined,
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    const secretNames = [
      ...new Set(
        await this.#api.listOrdinaryWorkerSecretNames(spec.scriptName),
      ),
    ].sort();
    if (secretNames.length === 0) {
      await this.#attestTeardownWorkerOwnership(
        spec,
        database.id,
        retainedReleases,
        activeRelease,
      );
    }
    // Each secret deletion can publish a version, so re-attest before the next
    // irreversible mutation instead of treating the inventory as a batch.
    for (const secretName of secretNames) {
      if (
        !(await this.#attestTeardownWorkerOwnership(
          spec,
          database.id,
          retainedReleases,
          activeRelease,
        ))
      ) {
        throw new Error(
          `ordinary Worker '${spec.scriptName}' has secrets without an attestable Worker owner`,
        );
      }
      await this.#api.deleteControlSecrets(
        spec.scriptName,
        [secretName],
        fence,
      );
    }
    const remaining = await this.#api.listOrdinaryWorkerSecretNames(
      spec.scriptName,
    );
    if (remaining.length > 0) {
      throw new Error(
        `ordinary Worker '${spec.scriptName}' failed exact secret revocation`,
      );
    }
  }

  async forceDecommissionStep(
    record: FleetRecord,
    step: ForceDecommissionStep,
    fence: ExternalMutationFence,
  ): Promise<void> {
    if (record.backend !== this.kind) {
      throw new Error(
        `plain Worker backend cannot force-decommission '${record.backend}' resources`,
      );
    }
    if (step === 'remove-traffic') {
      const domains = await this.#api.listCustomDomains();
      for (const domain of domains.filter(
        ({ service }) => service === record.scriptName,
      )) {
        await this.#api.detachCustomDomain(domain.id, fence);
      }
      const initial = await this.#api.inspectOrdinaryWorkerFootprint(
        record.scriptName,
      );
      if (initial.scriptPresent) {
        await this.#api.disableOrdinaryWorkerPublicAccess(
          record.scriptName,
          fence,
        );
      }
      const [footprint, remainingDomains] = await Promise.all([
        this.#api.inspectOrdinaryWorkerFootprint(record.scriptName),
        this.#api.listCustomDomains(),
      ]);
      if (
        footprint.customDomains.length > 0 ||
        footprint.zoneRoutes.length > 0 ||
        footprint.workersDevEnabled === true ||
        footprint.previewUrlsEnabled === true ||
        remainingDomains.some(({ service }) => service === record.scriptName)
      ) {
        throw new Error(
          `ordinary Worker '${record.scriptName}' retains public ingress after force decommission`,
        );
      }
      return;
    }
    if (step === 'revoke-credentials') {
      const secretNames = [
        ...new Set(
          await this.#api.listOrdinaryWorkerSecretNames(record.scriptName),
        ),
      ].sort();
      for (const secretName of secretNames) {
        await this.#api.deleteControlSecrets(
          record.scriptName,
          [secretName],
          fence,
        );
      }
      const remaining = await this.#api.listOrdinaryWorkerSecretNames(
        record.scriptName,
      );
      if (remaining.length > 0) {
        throw new Error(
          `ordinary Worker '${record.scriptName}' failed exact secret revocation during force decommission`,
        );
      }
      return;
    }
    if (step !== 'delete-database') {
      throw new Error(`unsupported force-decommission step '${step}'`);
    }
    if (!this.#api.supportsExactDatabaseDeletion) {
      throw new Error(EXACT_DATABASE_DELETION_REQUIRED);
    }
    await this.#api.withMutationFence(fence, async () => {
      const database = await this.#api.getDatabase(record.databaseId);
      if (!database) return;
      if (
        database.id !== record.databaseId ||
        database.name !== record.databaseName
      ) {
        throw new Error(
          `persisted database '${record.databaseId}' resolved with unexpected identity '${database.id}:${database.name}' during force decommission`,
        );
      }
      await this.#api.deleteDatabaseFenced(database.id, fence);
      if (await this.#api.getDatabase(record.databaseId)) {
        throw new Error(
          `database '${record.databaseId}' remains after force decommission`,
        );
      }
    });
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
      await this.#api.detachCustomDomain(route.id, fence);
    }
    if (worker) {
      await this.#api.disableOrdinaryWorkerPublicAccess(spec.scriptName, fence);
    }
  }

  async assertTrafficRemoved(spec: DeploymentSpec): Promise<void> {
    const footprint = await this.#api.inspectOrdinaryWorkerFootprint(
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
    const [initialFootprint, initialNamespaceIds] = await Promise.all([
      this.#api.inspectOrdinaryWorkerFootprint(spec.scriptName),
      this.#api.listDurableObjectNamespaces(spec.scriptName),
    ]);
    await this.assertTrafficRemoved(spec);
    if (!initialFootprint.scriptPresent) {
      if (
        initialFootprint.customDomains.length > 0 ||
        initialFootprint.zoneRoutes.length > 0 ||
        initialNamespaceIds.length > 0
      ) {
        throw new Error(
          `ordinary Worker '${spec.scriptName}' has a script-absent footprint with residual routes, domains, or Durable Object namespaces`,
        );
      }
      const [status, versions] = await Promise.all([
        this.#deploymentStatus(spec),
        this.#listVersions(spec),
      ]);
      if (status || (versions && versions.length > 0)) {
        throw new Error(
          `ordinary Worker '${spec.scriptName}' remains after its footprint reported absence`,
        );
      }
      return;
    }
    if (initialFootprint.zoneRoutes.length > 0) {
      throw new Error(
        `refusing to delete Worker '${spec.scriptName}' with an inconsistent script or zone-route footprint`,
      );
    }
    const unexpectedDomains = (await this.#api.listCustomDomains()).filter(
      (domain) =>
        domain.service === spec.scriptName &&
        domain.hostname.toLowerCase() !== spec.routeHostname.toLowerCase(),
    );
    if (unexpectedDomains.length > 0) {
      throw new Error(
        `refusing to delete Worker '${spec.scriptName}' with unexpected custom domains`,
      );
    }
    // Secret mutations can create new version IDs, so this live check
    // validates the persisted anchor and deployed identity without
    // repeating artifact-set membership.
    if (
      !(await this.#attestTeardownWorkerOwnership(
        spec,
        database.id,
        retainedReleases,
        activeRelease,
      ))
    ) {
      throw new Error(
        `ordinary Worker '${spec.scriptName}' disappeared before deletion`,
      );
    }
    this.#assertMutationDuration(fence);
    const deletionOutcome = await this.#api.deleteWorkerScript(
      spec.scriptName,
      fence,
    );
    // Policy treats deleted and absent identically because the residual check
    // follows; satisfies is a widening tripwire for future adapter outcomes.
    deletionOutcome satisfies 'deleted' | 'absent';
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
      this.#api
        .listCustomDomains()
        .then((domains) =>
          domains.filter((domain) => domain.service === spec.scriptName),
        ),
      this.#api.inspectOrdinaryWorkerFootprint(spec.scriptName),
      this.#api.listDurableObjectNamespaces(spec.scriptName),
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
    this.#assertMutationDuration(fence);
    const exported = await this.#api.exportDatabase(database, fence);
    return {
      databaseId: database.id,
      location: exported.location,
      sha256: exported.sha256,
      size: exported.size,
    };
  }

  async deleteDatabase(
    database: DatabaseReference,
    fence: ExternalMutationFence,
  ): Promise<void> {
    if (!this.#api.supportsExactDatabaseDeletion) {
      throw new Error(EXACT_DATABASE_DELETION_REQUIRED);
    }
    await this.#api.withMutationFence(fence, async () => {
      await this.#api.deleteDatabaseFenced(database.id, fence);
      if (await this.#api.getDatabase(database.id)) {
        throw new Error(`database '${database.id}' remains after deletion`);
      }
    });
  }
}
