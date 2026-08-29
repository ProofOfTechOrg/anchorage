// SPDX-License-Identifier: Apache-2.0

import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { chmod, mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Readable } from 'node:stream';
import type { DurableDatabaseExportStore } from './database-export-store.js';
import { readField, readStringField } from './json-field-reads.js';
import { providerBindingsToPlainWorkerShape } from './provider-binding-inventory.js';
import type {
  DatabaseReference,
  ExternalMutationFence,
  OrdinaryWorkerDeploymentVersion,
  PlainWorkerDatabaseExportResult,
  PlainWorkerDatabaseInventoryEntry,
  PlainWorkerDeploymentStatus,
  PlainWorkerMutationOutcome,
  PlainWorkerProvisioningApi,
  PlainWorkerRouteApi,
  PlainWorkerUploadIntent,
  PlainWorkerUploadOutcome,
  PlainWorkerVersionDetail,
  PlainWorkerVersionSummary,
} from './types.js';
import type { CommandResult, CommandRunner } from './wrangler-runner.js';

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

function isWranglerNotFound(error: unknown): boolean {
  return (
    error instanceof Error &&
    /not found|10090|does not exist|has no deployments/i.test(error.message)
  );
}

function readVersionId(value: unknown): string | undefined {
  const id = readField(value, 'id') ?? readField(value, 'version_id');
  return typeof id === 'string' ? id : undefined;
}

function versionTag(value: unknown): string | undefined {
  const annotations = readField(value, 'annotations');
  const tag = readField(annotations, 'workers/tag') ?? readField(value, 'tag');
  return typeof tag === 'string' ? tag : undefined;
}

function normalizeVersionSummary(value: unknown): PlainWorkerVersionSummary {
  return { versionId: readVersionId(value), tag: versionTag(value) };
}

export class WranglerPlainWorkerProvisioningApi
  implements PlainWorkerProvisioningApi
{
  readonly #runner: CommandRunner;
  readonly #routeApi: PlainWorkerRouteApi;
  readonly #exportDirectory: string;
  readonly #exportStore: DurableDatabaseExportStore;
  readonly #mutationFenceScope = new AsyncLocalStorage<ExternalMutationFence>();
  readonly #routeGetDatabase:
    | NonNullable<PlainWorkerRouteApi['getDatabase']>
    | undefined;
  readonly #routeDeleteDatabase:
    | NonNullable<PlainWorkerRouteApi['deleteDatabase']>
    | undefined;
  readonly maxMutationDurationMs: number;
  declare readonly advanceDecommissionAttachmentScan?: NonNullable<
    PlainWorkerRouteApi['advanceDecommissionAttachmentScan']
  >;
  readonly listWorkerR2Attachments:
    | NonNullable<PlainWorkerRouteApi['listWorkerR2Attachments']>
    | undefined;
  readonly getR2Bucket:
    | NonNullable<PlainWorkerRouteApi['getR2Bucket']>
    | undefined;
  readonly createR2Bucket:
    | NonNullable<PlainWorkerRouteApi['createR2Bucket']>
    | undefined;
  readonly assertR2BucketEmpty:
    | NonNullable<PlainWorkerRouteApi['assertR2BucketEmpty']>
    | undefined;
  readonly deleteR2Bucket:
    | NonNullable<PlainWorkerRouteApi['deleteR2Bucket']>
    | undefined;

  constructor(options: {
    readonly runner: CommandRunner;
    readonly routeApi: PlainWorkerRouteApi;
    readonly exportDirectory: string;
    readonly exportStore: DurableDatabaseExportStore;
  }) {
    this.#runner = options.runner;
    this.#routeApi = options.routeApi;
    this.#exportDirectory = resolve(options.exportDirectory);
    this.#exportStore = options.exportStore;
    this.#routeGetDatabase = options.routeApi.getDatabase?.bind(
      options.routeApi,
    );
    this.#routeDeleteDatabase = options.routeApi.deleteDatabase?.bind(
      options.routeApi,
    );
    this.maxMutationDurationMs = options.runner.maxDurationMs;
    const advanceDecommissionAttachmentScan =
      options.routeApi.advanceDecommissionAttachmentScan;
    if (typeof advanceDecommissionAttachmentScan === 'function') {
      this.advanceDecommissionAttachmentScan =
        advanceDecommissionAttachmentScan.bind(options.routeApi);
    }
    this.listWorkerR2Attachments =
      options.routeApi.listWorkerR2Attachments?.bind(options.routeApi);
    this.getR2Bucket = options.routeApi.getR2Bucket?.bind(options.routeApi);
    this.createR2Bucket = options.routeApi.createR2Bucket?.bind(
      options.routeApi,
    );
    this.assertR2BucketEmpty = options.routeApi.assertR2BucketEmpty?.bind(
      options.routeApi,
    );
    this.deleteR2Bucket = options.routeApi.deleteR2Bucket?.bind(
      options.routeApi,
    );
  }

  get supportsExactDatabaseDeletion(): boolean {
    return Boolean(this.#routeGetDatabase && this.#routeDeleteDatabase);
  }

  withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T> {
    // Tolerates the legacy entry-asserting double in
    // test/wrangler-loop-backend.test.ts:201-206; production asserts per request.
    if (this.#mutationFenceScope.getStore() === fence) return operation();
    return this.#routeApi.withMutationFence(fence, () =>
      this.#mutationFenceScope.run(fence, operation),
    );
  }

  queryDatabase(
    databaseId: string,
    sql: string,
    bindings?: readonly string[],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    return this.#routeApi.queryDatabase(databaseId, sql, bindings);
  }

  batchDatabase(
    databaseId: string,
    statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly string[];
    }[],
  ): Promise<void> {
    return this.#routeApi.batchDatabase(databaseId, statements);
  }

  listWorkerDatabaseAttachments(
    databaseId: string,
  ): ReturnType<PlainWorkerRouteApi['listWorkerDatabaseAttachments']> {
    return this.#routeApi.listWorkerDatabaseAttachments(databaseId);
  }

  inspectActiveWorkerRoute(
    scriptName: string,
  ): ReturnType<PlainWorkerRouteApi['inspectActiveWorkerRoute']> {
    return this.#routeApi.inspectActiveWorkerRoute(scriptName);
  }

  listCustomDomains(): ReturnType<PlainWorkerRouteApi['listCustomDomains']> {
    return this.#routeApi.listCustomDomains();
  }

  inspectOrdinaryWorkerFootprint(
    scriptName: string,
  ): ReturnType<PlainWorkerRouteApi['inspectOrdinaryWorkerFootprint']> {
    return this.#routeApi.inspectOrdinaryWorkerFootprint(scriptName);
  }

  listDurableObjectNamespaces(
    scriptName: string,
  ): ReturnType<PlainWorkerRouteApi['listDurableObjectNamespaces']> {
    return this.#routeApi.listDurableObjectNamespaces(scriptName);
  }

  listOrdinaryWorkerSecretNames(
    scriptName: string,
  ): ReturnType<PlainWorkerRouteApi['listOrdinaryWorkerSecretNames']> {
    return this.#routeApi.listOrdinaryWorkerSecretNames(scriptName);
  }

  deleteControlSecrets(
    scriptName: string,
    secretNames: readonly string[],
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#routeApi.deleteControlSecrets(scriptName, secretNames, fence);
  }

  attachCustomDomain(
    target: { readonly hostname: string; readonly service: string },
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#routeApi.attachCustomDomain(target, fence);
  }

  detachCustomDomain(
    domainId: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#routeApi.detachCustomDomain(domainId, fence);
  }

  disableOrdinaryWorkerPublicAccess(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    return this.#routeApi.disableOrdinaryWorkerPublicAccess(scriptName, fence);
  }

  async listDatabases(
    filter?: Readonly<{ name?: string }>,
  ): Promise<readonly PlainWorkerDatabaseInventoryEntry[]> {
    const listed = await this.#runner.run(['d1', 'list', '--json']);
    // The pinned Wrangler command has no name flag, so the adapter filters
    // the parsed inventory.
    return asArray(parseJson(listed.stdout, 'd1 list'))
      .map((database) => ({
        databaseId: readStringField(database, 'uuid'),
        name: readStringField(database, 'name'),
      }))
      .filter(
        (database) =>
          filter?.name === undefined || database.name === filter.name,
      );
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    if (this.#routeGetDatabase) return this.#routeGetDatabase(databaseId);
    let result: CommandResult;
    try {
      result = await this.#runner.run(['d1', 'info', databaseId, '--json']);
    } catch (error) {
      if (isWranglerNotFound(error)) return undefined;
      throw error;
    }
    const parsed = parseJson(result.stdout, 'd1 info');
    const body = readField(parsed, 'result') ?? parsed;
    const id = readField(body, 'uuid');
    const name = readField(body, 'name');
    if (id !== databaseId || typeof name !== 'string' || name.length === 0) {
      throw new Error('D1 info result has an invalid uuid or name');
    }
    return { id: databaseId, name, created: false };
  }

  async createDatabase(
    name: string,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerMutationOutcome> {
    await fence.assertOwned();
    try {
      await this.#runner.run(['d1', 'create', name]);
      return { status: 'succeeded' };
    } catch (error) {
      return { status: 'failed', error };
    }
  }

  async deleteDatabaseFenced(
    databaseId: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    const getDatabase = this.#routeGetDatabase;
    const deleteDatabase = this.#routeDeleteDatabase;
    if (!getDatabase || !deleteDatabase) {
      // Port self-enforcement; unreachable through WranglerLoopBackend, which preflights supportsExactDatabaseDeletion.
      throw new Error(
        'Wrangler plain Worker adapter requires immutable-ID D1 route methods',
      );
    }
    await this.withMutationFence(fence, () => deleteDatabase(databaseId));
  }

  async deploymentStatus(
    scriptName: string,
  ): Promise<PlainWorkerDeploymentStatus | undefined> {
    let result: CommandResult;
    try {
      result = await this.#runner.run([
        'deployments',
        'status',
        '--name',
        scriptName,
        '--json',
      ]);
    } catch (error) {
      if (isWranglerNotFound(error)) return undefined;
      throw error;
    }
    const parsed = parseJson(result.stdout, 'deployments status');
    const body = readField(parsed, 'result') ?? parsed;
    return {
      versions: asArray(readField(body, 'versions')).map((version) => {
        const rawPercentage = readField(version, 'percentage');
        return {
          versionId: readVersionId(version),
          percentage:
            rawPercentage === undefined ? undefined : Number(rawPercentage),
        };
      }),
    };
  }

  async listVersions(
    scriptName: string,
  ): Promise<readonly PlainWorkerVersionSummary[] | undefined> {
    try {
      const listed = await this.#runner.run([
        'versions',
        'list',
        '--name',
        scriptName,
        '--json',
      ]);
      return asArray(parseJson(listed.stdout, 'versions list')).map(
        normalizeVersionSummary,
      );
    } catch (error) {
      if (isWranglerNotFound(error)) return undefined;
      throw error;
    }
  }

  async viewVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail> {
    const viewed = await this.#runner.run([
      'versions',
      'view',
      versionId,
      '--name',
      scriptName,
      '--json',
    ]);
    const parsed = parseJson(viewed.stdout, 'versions view');
    const resources = readField(parsed, 'resources');
    return {
      versionId: readVersionId(parsed),
      tag: versionTag(parsed),
      bindings: providerBindingsToPlainWorkerShape(
        asArray(readField(resources, 'bindings')),
      ),
    };
  }

  async findVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail | undefined> {
    try {
      return await this.viewVersion(scriptName, versionId);
    } catch (error) {
      if (isWranglerNotFound(error)) return undefined;
      throw error;
    }
  }

  async uploadCandidate(
    intent: PlainWorkerUploadIntent,
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerUploadOutcome> {
    const directory = await mkdtemp(join(tmpdir(), 'anchorage-fleet-'));
    let settled:
      | Readonly<{ ok: true }>
      | Readonly<{ ok: false; error: unknown }>;
    let dispatched = false;
    let cleanup:
      | Readonly<{ status: 'succeeded' }>
      | Readonly<{ status: 'failed'; error: unknown }>;
    try {
      for (const module of intent.modules) {
        const modulePath = resolve(directory, module.name);
        if (!modulePath.startsWith(`${directory}/`)) {
          throw new Error(
            `module '${module.name}' escapes the staging directory`,
          );
        }
        await mkdir(resolve(modulePath, '..'), { recursive: true });
        await writeFile(modulePath, module.content);
      }
      const configPath = join(directory, 'wrangler.candidate.json');
      await writeFile(
        configPath,
        JSON.stringify({
          name: intent.scriptName,
          main: intent.mainModule,
          workers_dev: intent.publicAccess.workersDevEnabled,
          preview_urls: intent.publicAccess.previewUrlsEnabled,
          compatibility_date: intent.compatibilityDate,
          compatibility_flags: intent.compatibilityFlags,
          vars: Object.fromEntries(
            intent.bindings.plainText.map(({ name, value }) => [name, value]),
          ),
          d1_databases: intent.bindings.d1.map((binding) => ({
            binding: binding.name,
            database_name: binding.databaseName,
            database_id: binding.databaseId,
          })),
          durable_objects: {
            bindings: intent.bindings.durableObjects.map((binding) => ({
              name: binding.name,
              class_name: binding.className,
            })),
          },
          services:
            intent.bindings.services.length > 0
              ? intent.bindings.services.map((binding) => ({
                  binding: binding.name,
                  service: binding.service,
                }))
              : undefined,
          ...(intent.mode === 'initial'
            ? {
                migrations: intent.durableObjectMigrations.map((migration) => ({
                  tag: migration.tag,
                  new_sqlite_classes: migration.newSqliteClasses,
                  new_classes: migration.newClasses,
                  deleted_classes: migration.deletedClasses,
                  renamed_classes: migration.renamedClasses,
                })),
              }
            : {}),
          queues:
            intent.bindings.queueProducers.length > 0
              ? {
                  producers: intent.bindings.queueProducers.map((binding) => ({
                    binding: binding.name,
                    queue: binding.queueName,
                  })),
                }
              : undefined,
          r2_buckets: intent.bindings.r2Buckets.map((binding) => ({
            binding: binding.name,
            bucket_name: binding.bucketName,
          })),
          limits: intent.limits.cpuMs
            ? { cpu_ms: intent.limits.cpuMs }
            : undefined,
        }),
      );
      const secretsPath = join(directory, 'wrangler.secrets.json');
      await writeFile(
        secretsPath,
        JSON.stringify(
          Object.fromEntries(
            intent.bindings.secrets.map(({ name, value }) => [name, value]),
          ),
        ),
        { mode: 0o600 },
      );
      await fence.assertOwned();
      dispatched = true;
      await this.#runner.run([
        ...(intent.mode === 'initial' ? ['deploy'] : ['versions', 'upload']),
        '--config',
        configPath,
        '--secrets-file',
        secretsPath,
        '--tag',
        intent.candidateTag,
      ]);
      settled = { ok: true };
    } catch (error) {
      settled = { ok: false, error };
    } finally {
      try {
        await rm(directory, { recursive: true, force: true });
        cleanup = { status: 'succeeded' };
      } catch (error) {
        cleanup = { status: 'failed', error };
      }
    }
    if (!dispatched && !settled.ok) {
      throw cleanup.status === 'failed'
        ? new AggregateError(
            [settled.error, cleanup.error],
            'Worker upload preparation and adapter scratch cleanup both failed',
          )
        : settled.error;
    }
    return settled.ok
      ? { status: 'succeeded', cleanup }
      : { status: 'failed', error: settled.error, cleanup };
  }

  async createDeployment(
    scriptName: string,
    versions: readonly OrdinaryWorkerDeploymentVersion[],
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerMutationOutcome> {
    await fence.assertOwned();
    try {
      await this.#runner.run([
        'versions',
        'deploy',
        ...versions.map(
          ({ versionId, percentage }) => `${versionId}@${percentage}%`,
        ),
        '--name',
        scriptName,
        '-y',
      ]);
      return { status: 'succeeded' };
    } catch (error) {
      return { status: 'failed', error };
    }
  }

  async deleteWorkerScript(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<'deleted' | 'absent'> {
    await fence.assertOwned();
    try {
      await this.#runner.run(['delete', '--name', scriptName, '--force']);
      return 'deleted';
    } catch (error) {
      if (isWranglerNotFound(error)) return 'absent';
      throw error;
    }
  }

  async exportDatabase(
    database: { readonly id: string; readonly name: string },
    fence: ExternalMutationFence,
  ): Promise<PlainWorkerDatabaseExportResult> {
    await mkdir(this.#exportDirectory, { recursive: true });
    const temporaryDirectory = await mkdtemp(
      join(this.#exportDirectory, '.wrangler-export-'),
    );
    const fileName = `${database.name}-${Date.now()}.sql`;
    const temporaryLocation = join(temporaryDirectory, fileName);
    try {
      await fence.assertOwned();
      await this.#runner.run([
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
      return { location: stored.location, size: metadata.size, sha256 };
    } finally {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
  }
}
