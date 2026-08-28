// SPDX-License-Identifier: Apache-2.0

import type {
  ApplicationR2Binding,
  ApplicationR2BucketSnapshot,
  DatabaseReference,
  ExternalMutationFence,
  PlainWorkerCleanupOutcome,
  PlainWorkerCustomDomain,
  PlainWorkerDatabaseExportResult,
  PlainWorkerDatabaseInventoryEntry,
  PlainWorkerDeploymentStatus,
  PlainWorkerMutationOutcome,
  PlainWorkerProvisioningApi,
  PlainWorkerUploadIntent,
  PlainWorkerUploadOutcome,
  PlainWorkerVersionDetail,
  PlainWorkerVersionSummary,
  R2Jurisdiction,
  WorkerZoneRoute,
} from '../../src/types.js';

/**
 * `entry` adds an assertion on `withMutationFence` entry in addition to the
 * per-request assertion. The probe fixture's entry-asserting shape instead
 * models one legacy entry assertion without per-request assertions.
 */
export type FenceAssertionMode = 'entry' | 'per-request';

export class PlainWorkerProvisioningApiFake
  implements PlainWorkerProvisioningApi
{
  readonly maxMutationDurationMs: number;
  readonly supportsExactDatabaseDeletion = true;
  readonly events: string[] = [];
  readonly failures = new Map<string, unknown>();
  // absent, buckets, exportResult, and createDeploymentOutcome's failure arm are
  // deliberately unexercised seed state for the direct-API conformance fixture.
  readonly absent = new Set<string>();
  readonly scripts = new Set<string>();
  readonly databases = new Map<string, DatabaseReference>();
  readonly listDatabaseFilters: (Readonly<{ name?: string }> | undefined)[] =
    [];
  readonly versions = new Map<string, PlainWorkerVersionDetail[]>();
  readonly deployments = new Map<string, PlainWorkerDeploymentStatus>();
  readonly domains: PlainWorkerCustomDomain[] = [];
  readonly buckets = new Map<string, ApplicationR2BucketSnapshot>();
  readonly secretNames = new Map<string, string[]>();
  readonly namespaces = new Map<string, string[]>();
  readonly footprints = new Map<
    string,
    {
      scriptPresent: boolean;
      workersDevEnabled?: boolean;
      previewUrlsEnabled?: boolean;
      customDomains: readonly PlainWorkerCustomDomain[];
      zoneRoutes: readonly WorkerZoneRoute[];
    }
  >();
  readonly queries: Array<{
    databaseId: string;
    sql: string;
    bindings: readonly string[];
  }> = [];
  uploadCleanup: PlainWorkerCleanupOutcome = { status: 'succeeded' };
  createDatabaseOutcome: PlainWorkerMutationOutcome = { status: 'succeeded' };
  uploadOutcome: PlainWorkerMutationOutcome = { status: 'succeeded' };
  createDeploymentOutcome: PlainWorkerMutationOutcome = {
    status: 'succeeded',
  };
  exportResult: PlainWorkerDatabaseExportResult = {
    location: 'memory://database-export',
    size: 0,
    sha256: '0'.repeat(64),
  };
  activeRoute:
    | Readonly<{
        artifactVersion: string;
        specDigest: string | undefined;
      }>
    | undefined;
  onUploadCandidate:
    | ((intent: PlainWorkerUploadIntent) => void | Promise<void>)
    | undefined;
  onDeleteControlSecrets:
    | ((secretNames: readonly string[]) => void | Promise<void>)
    | undefined;
  onDeleteWorkerScript: (() => void | Promise<void>) | undefined;

  #ambientFence: ExternalMutationFence | undefined;
  // A backend-owned assertion records `assertOwned`; suppress the port's own
  // assertion while it runs so the port records only `port-assert`.
  #portAssertionActive = false;

  constructor(
    readonly fenceAssertionMode: FenceAssertionMode = 'per-request',
    maxMutationDurationMs = 5 * 60_000,
  ) {
    this.maxMutationDurationMs = maxMutationDurationMs;
  }

  fence(): ExternalMutationFence {
    return {
      mutationLeaseTtlMs: 15 * 60_000,
      assertOwned: async () => {
        if (!this.#portAssertionActive) this.events.push('assertOwned');
      },
    };
  }

  async #assertPortOwned(fence: ExternalMutationFence): Promise<void> {
    this.events.push('port-assert');
    this.#portAssertionActive = true;
    try {
      await fence.assertOwned();
    } finally {
      this.#portAssertionActive = false;
    }
  }

  async #request(
    name: string,
    fence: ExternalMutationFence | undefined,
  ): Promise<void> {
    const activeFence = fence ?? this.#ambientFence;
    if (activeFence) {
      await this.#assertPortOwned(activeFence);
    }
    this.events.push(`mutation:${name}`);
    if (this.failures.has(name)) throw this.failures.get(name);
  }

  async withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T> {
    const prior = this.#ambientFence;
    this.#ambientFence = fence;
    try {
      if (this.fenceAssertionMode === 'entry') {
        await this.#assertPortOwned(fence);
      }
      return await operation();
    } finally {
      this.#ambientFence = prior;
    }
  }

  async queryDatabase(
    databaseId: string,
    sql: string,
    bindings: readonly string[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    await this.#request('queryDatabase', undefined);
    this.queries.push({ databaseId, sql, bindings });
    return [];
  }

  async batchDatabase(): Promise<void> {
    await this.#request('batchDatabase', undefined);
  }

  async listDatabases(
    filter?: Readonly<{ name?: string }>,
  ): Promise<readonly PlainWorkerDatabaseInventoryEntry[]> {
    this.listDatabaseFilters.push(filter);
    return [...this.databases.values()]
      .filter(({ name }) => {
        // Include partial matches to exercise the core's exact-name check.
        return filter?.name === undefined || name.includes(filter.name);
      })
      .map(({ id, name }) => ({
        databaseId: id,
        name,
      }));
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    return this.absent.has(`database:${databaseId}`)
      ? undefined
      : this.databases.get(databaseId);
  }

  async createDatabase(
    name: string,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerMutationOutcome> {
    await this.#request('createDatabase', fence);
    if (this.createDatabaseOutcome.status === 'succeeded') {
      this.databases.set(name, { id: name, name, created: false });
    }
    return this.createDatabaseOutcome;
  }

  async deleteDatabaseFenced(
    databaseId: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#request('deleteDatabaseFenced', fence);
    this.databases.delete(databaseId);
  }

  async deploymentStatus(
    scriptName: string,
  ): Promise<PlainWorkerDeploymentStatus | undefined> {
    return this.absent.has(`deployment:${scriptName}`)
      ? undefined
      : this.deployments.get(scriptName);
  }

  async listVersions(
    scriptName: string,
  ): Promise<readonly PlainWorkerVersionSummary[] | undefined> {
    if (this.absent.has(`versions:${scriptName}`)) return undefined;
    const versions = this.versions.get(scriptName);
    return versions?.map(({ versionId, tag }) => ({ versionId, tag }));
  }

  async viewVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail> {
    const found = this.versions
      .get(scriptName)
      ?.find((version) => version.versionId === versionId);
    if (!found) throw new Error(`version '${versionId}' is absent`);
    return found;
  }

  async findVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail | undefined> {
    return this.versions
      .get(scriptName)
      ?.find((version) => version.versionId === versionId);
  }

  async uploadCandidate(
    intent: PlainWorkerUploadIntent,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerUploadOutcome> {
    await this.#request('uploadCandidate', fence);
    await this.onUploadCandidate?.(intent);
    return { ...this.uploadOutcome, cleanup: this.uploadCleanup };
  }

  async createDeployment(
    scriptName: string,
    versions: readonly { versionId: string; percentage: number }[],
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerMutationOutcome> {
    await this.#request('createDeployment', fence);
    if (this.createDeploymentOutcome.status === 'succeeded') {
      this.scripts.add(scriptName);
      this.deployments.set(scriptName, {
        versions: versions.map(({ versionId, percentage }) => ({
          versionId,
          percentage,
        })),
      });
    }
    return this.createDeploymentOutcome;
  }

  async deleteWorkerScript(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<'deleted' | 'absent'> {
    await this.#request('deleteWorkerScript', fence);
    const hadVersions = this.versions.delete(scriptName);
    const hadDeployment = this.deployments.delete(scriptName);
    const hadScript = this.scripts.delete(scriptName);
    const existed = hadVersions || hadDeployment || hadScript;
    this.footprints.delete(scriptName);
    await this.onDeleteWorkerScript?.();
    return existed ? 'deleted' : 'absent';
  }

  async exportDatabase(
    _database: { readonly id: string; readonly name: string },
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerDatabaseExportResult> {
    await this.#request('exportDatabase', fence);
    return this.exportResult;
  }

  async listWorkerDatabaseAttachments(): Promise<readonly []> {
    return [];
  }

  async listWorkerR2Attachments(): Promise<readonly []> {
    return [];
  }

  async getR2Bucket(
    bucketName: string,
    _jurisdiction: R2Jurisdiction,
  ): Promise<ApplicationR2BucketSnapshot | undefined> {
    return this.buckets.get(bucketName);
  }

  async createR2Bucket(
    resource: ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#request('createR2Bucket', fence);
    this.buckets.set(resource.bucketName, {
      ...resource,
      creationDate: '2026-08-26T00:00:00.000Z',
    });
  }

  async assertR2BucketEmpty(): Promise<void> {}

  async deleteR2Bucket(
    resource: ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#request('deleteR2Bucket', fence);
    this.buckets.delete(resource.bucketName);
  }

  async inspectActiveWorkerRoute(): Promise<
    | Readonly<{
        artifactVersion: string;
        specDigest: string | undefined;
      }>
    | undefined
  > {
    return this.activeRoute;
  }

  async listCustomDomains(): Promise<readonly PlainWorkerCustomDomain[]> {
    return [...this.domains];
  }

  async inspectOrdinaryWorkerFootprint(scriptName: string): Promise<{
    readonly scriptPresent: boolean;
    readonly workersDevEnabled?: boolean;
    readonly previewUrlsEnabled?: boolean;
    readonly customDomains: readonly PlainWorkerCustomDomain[];
    readonly zoneRoutes: readonly WorkerZoneRoute[];
  }> {
    return (
      this.footprints.get(scriptName) ?? {
        scriptPresent:
          this.scripts.has(scriptName) ||
          this.versions.has(scriptName) ||
          this.deployments.has(scriptName),
        customDomains: this.domains.filter(
          (domain) => domain.service === scriptName,
        ),
        zoneRoutes: [],
      }
    );
  }

  async listDurableObjectNamespaces(
    scriptName: string,
  ): Promise<readonly string[]> {
    return this.namespaces.get(scriptName) ?? [];
  }

  async listOrdinaryWorkerSecretNames(
    scriptName: string,
  ): Promise<readonly string[]> {
    return this.secretNames.get(scriptName) ?? [];
  }

  async deleteControlSecrets(
    scriptName: string,
    secretNames: readonly string[],
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#request('deleteControlSecrets', fence);
    const remaining = (this.secretNames.get(scriptName) ?? []).filter(
      (name) => !secretNames.includes(name),
    );
    this.secretNames.set(scriptName, remaining);
    await this.onDeleteControlSecrets?.(secretNames);
  }

  async attachCustomDomain(
    target: { readonly hostname: string; readonly service: string },
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#request('attachCustomDomain', fence);
    this.domains.push({ id: target.hostname, ...target });
  }

  async detachCustomDomain(
    domainId: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#request('detachCustomDomain', fence);
    const index = this.domains.findIndex(({ id }) => id === domainId);
    if (index >= 0) this.domains.splice(index, 1);
  }

  async disableOrdinaryWorkerPublicAccess(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.#request('disableOrdinaryWorkerPublicAccess', fence);
    const footprint = await this.inspectOrdinaryWorkerFootprint(scriptName);
    this.footprints.set(scriptName, {
      ...footprint,
      workersDevEnabled: false,
      previewUrlsEnabled: false,
    });
  }
}
