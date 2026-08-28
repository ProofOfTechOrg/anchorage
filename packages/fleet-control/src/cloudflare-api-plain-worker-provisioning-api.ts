// SPDX-License-Identifier: Apache-2.0

import {
  CloudflareProviderRequestNotDispatchedError,
  type CloudflareProvisioningClient,
  withProviderDispatchTracking,
} from './cloudflare-client.js';
import type {
  DatabaseReference,
  ExternalMutationFence,
  OrdinaryWorkerDeploymentVersion,
  PlainWorkerDatabaseExportResult,
  PlainWorkerDatabaseInventoryEntry,
  PlainWorkerDeploymentStatus,
  PlainWorkerMutationOutcome,
  PlainWorkerProvisioningApi,
  PlainWorkerUploadIntent,
  PlainWorkerUploadOutcome,
  PlainWorkerVersionDetail,
  PlainWorkerVersionSummary,
} from './types.js';

export class CloudflareApiPlainWorkerProvisioningApi
  implements PlainWorkerProvisioningApi
{
  readonly #client: CloudflareProvisioningClient;
  readonly maxMutationDurationMs: number;
  readonly supportsExactDatabaseDeletion = true;
  readonly listWorkerR2Attachments: NonNullable<
    PlainWorkerProvisioningApi['listWorkerR2Attachments']
  >;
  readonly getR2Bucket: NonNullable<PlainWorkerProvisioningApi['getR2Bucket']>;
  readonly createR2Bucket: NonNullable<
    PlainWorkerProvisioningApi['createR2Bucket']
  >;
  readonly assertR2BucketEmpty: NonNullable<
    PlainWorkerProvisioningApi['assertR2BucketEmpty']
  >;
  readonly deleteR2Bucket: NonNullable<
    PlainWorkerProvisioningApi['deleteR2Bucket']
  >;

  constructor(options: { readonly client: CloudflareProvisioningClient }) {
    this.#client = options.client;
    this.maxMutationDurationMs = options.client.requestTimeoutMs;
    this.listWorkerR2Attachments = options.client.listWorkerR2Attachments.bind(
      options.client,
    );
    this.getR2Bucket = options.client.getR2Bucket.bind(options.client);
    this.createR2Bucket = options.client.createR2Bucket.bind(options.client);
    this.assertR2BucketEmpty = options.client.assertR2BucketEmpty.bind(
      options.client,
    );
    this.deleteR2Bucket = options.client.deleteR2Bucket.bind(options.client);
  }

  withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T> {
    return this.#client.withMutationFence(fence, operation);
  }

  queryDatabase(
    databaseId: string,
    sql: string,
    bindings?: readonly string[],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    return this.#client.queryDatabase(databaseId, sql, bindings);
  }

  batchDatabase(
    databaseId: string,
    statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly string[];
    }[],
  ): Promise<void> {
    return this.#client.batchDatabase(databaseId, statements);
  }

  listWorkerDatabaseAttachments(
    databaseId: string,
  ): ReturnType<PlainWorkerProvisioningApi['listWorkerDatabaseAttachments']> {
    return this.#client.listWorkerDatabaseAttachments(databaseId);
  }

  inspectActiveWorkerRoute(
    scriptName: string,
  ): ReturnType<PlainWorkerProvisioningApi['inspectActiveWorkerRoute']> {
    return this.#client.inspectActiveWorkerRoute(scriptName);
  }

  listCustomDomains(): ReturnType<
    PlainWorkerProvisioningApi['listCustomDomains']
  > {
    return this.#client.listCustomDomains();
  }

  inspectOrdinaryWorkerFootprint(
    scriptName: string,
  ): ReturnType<PlainWorkerProvisioningApi['inspectOrdinaryWorkerFootprint']> {
    return this.#client.inspectOrdinaryWorkerFootprint(scriptName);
  }

  listDurableObjectNamespaces(
    scriptName: string,
  ): ReturnType<PlainWorkerProvisioningApi['listDurableObjectNamespaces']> {
    return this.#client.listDurableObjectNamespaces(scriptName);
  }

  listOrdinaryWorkerSecretNames(
    scriptName: string,
  ): ReturnType<PlainWorkerProvisioningApi['listOrdinaryWorkerSecretNames']> {
    return this.#client.listOrdinaryWorkerSecretNames(scriptName);
  }

  deleteControlSecrets(
    scriptName: string,
    secretNames: readonly string[],
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#client.deleteControlSecrets(scriptName, secretNames, fence);
  }

  attachCustomDomain(
    target: { readonly hostname: string; readonly service: string },
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#client.attachCustomDomain(target, fence);
  }

  detachCustomDomain(
    domainId: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#client.detachCustomDomain(domainId, fence);
  }

  disableOrdinaryWorkerPublicAccess(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#client.disableOrdinaryWorkerPublicAccess(scriptName, fence);
  }

  listDatabases(
    filter?: Readonly<{ name?: string }>,
  ): Promise<readonly PlainWorkerDatabaseInventoryEntry[]> {
    return this.#client.listOrdinaryWorkerDatabases(filter);
  }

  getDatabase(databaseId: string): Promise<DatabaseReference | undefined> {
    return this.#client.getDatabase(databaseId);
  }

  async createDatabase(
    name: string,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerMutationOutcome> {
    await fence.assertOwned();
    return this.#client.withMutationFence(fence, async () => {
      try {
        await withProviderDispatchTracking(this.#client, () =>
          this.#client.createDatabase(name),
        );
        return { status: 'succeeded' };
      } catch (error) {
        if (error instanceof CloudflareProviderRequestNotDispatchedError) {
          throw error.cause;
        }
        await fence.assertOwned();
        return { status: 'failed', error };
      }
    });
  }

  deleteDatabaseFenced(
    databaseId: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    // No separate pre-assert: like the command adapter, the transport asserts
    // immediately before dispatch so assertion counts stay identical.
    return this.#client.withMutationFence(fence, () =>
      this.#client.deleteDatabase(databaseId),
    );
  }

  deploymentStatus(
    scriptName: string,
  ): Promise<PlainWorkerDeploymentStatus | undefined> {
    return this.#client.ordinaryWorkerDeploymentStatus(scriptName);
  }

  listVersions(
    scriptName: string,
  ): Promise<readonly PlainWorkerVersionSummary[] | undefined> {
    return this.#client.listOrdinaryWorkerVersions(scriptName);
  }

  viewVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail> {
    return this.#client.viewOrdinaryWorkerVersion(scriptName, versionId);
  }

  findVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail | undefined> {
    return this.#client.findOrdinaryWorkerVersion(scriptName, versionId);
  }

  async uploadCandidate(
    intent: PlainWorkerUploadIntent,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerUploadOutcome> {
    const prepared = await this.#client.prepareOrdinaryWorkerUpload(intent);
    await fence.assertOwned();
    return this.#client.withMutationFence(fence, async () => {
      try {
        await withProviderDispatchTracking(this.#client, () =>
          this.#client.dispatchOrdinaryWorkerUpload(prepared),
        );
        return { status: 'succeeded', cleanup: { status: 'succeeded' } };
      } catch (error) {
        if (error instanceof CloudflareProviderRequestNotDispatchedError) {
          throw error.cause;
        }
        await fence.assertOwned();
        return {
          status: 'failed',
          error,
          cleanup: { status: 'succeeded' },
        };
      }
    });
  }

  async createDeployment(
    scriptName: string,
    versions: readonly OrdinaryWorkerDeploymentVersion[],
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerMutationOutcome> {
    const prepared = this.#client.prepareOrdinaryWorkerDeployment(versions);
    await fence.assertOwned();
    return this.#client.withMutationFence(fence, async () => {
      try {
        await withProviderDispatchTracking(this.#client, () =>
          this.#client.dispatchOrdinaryWorkerDeployment(scriptName, prepared),
        );
        return { status: 'succeeded' };
      } catch (error) {
        if (error instanceof CloudflareProviderRequestNotDispatchedError) {
          throw error.cause;
        }
        await fence.assertOwned();
        return { status: 'failed', error };
      }
    });
  }

  async deleteWorkerScript(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<'deleted' | 'absent'> {
    await fence.assertOwned();
    return this.#client.withMutationFence(fence, () =>
      this.#client.deleteOrdinaryWorkerScript(scriptName),
    );
  }

  async exportDatabase(
    database: { readonly id: string; readonly name: string },
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerDatabaseExportResult> {
    await fence.assertOwned();
    // The SDK transport asserts on every export poll, unlike the command
    // adapter's single assertion. Its store filename is `${database.id}-…`
    // rather than `${database.name}-…`; shared tests compare neither fact.
    const exported = await this.#client.withMutationFence(fence, () =>
      this.#client.exportDatabase(database.id),
    );
    return {
      location: exported.location,
      size: exported.size,
      sha256: exported.sha256,
    };
  }
}
