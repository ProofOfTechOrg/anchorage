// SPDX-License-Identifier: Apache-2.0

import type Cloudflare from 'cloudflare';
import type { ScriptUpdateParams } from 'cloudflare/resources/workers/scripts/scripts';
import type { VersionCreateParams } from 'cloudflare/resources/workers/scripts/versions';
import { toFile } from 'cloudflare/uploads';
import { attestedActiveVersionId } from './active-route.js';
import {
  isNotFound,
  sanitizeProviderError,
} from './cloudflare-provider-errors.js';
import { namedWorkerUploadBody } from './cloudflare-worker-upload.js';
import {
  readArrayField,
  readField,
  readStringField,
} from './json-field-reads.js';
import {
  assertOrdinaryWorkerDeploymentVersions,
  providerBindingsToPlainWorkerShape,
  uploadIntentToProviderBindings,
} from './provider-binding-inventory.js';
import type {
  ExternalMutationFence,
  OrdinaryWorkerDeploymentVersion,
  PlainWorkerDatabaseInventoryEntry,
  PlainWorkerDeploymentStatus,
  PlainWorkerUploadIntent,
  PlainWorkerVersionDetail,
  PlainWorkerVersionSummary,
} from './types.js';

export const MAX_DATABASE_INVENTORY = 25_000;
const MAX_VERSION_INVENTORY = 5_000;

export interface OrdinaryWorkerFootprint {
  readonly scriptPresent: boolean;
  readonly workersDevEnabled?: boolean;
  readonly previewUrlsEnabled?: boolean;
  readonly customDomains: readonly Readonly<{
    id: string;
    hostname: string;
    service: string;
  }>[];
  readonly zoneRoutes: readonly import('./types.js').WorkerZoneRoute[];
}

export type CloudflareSdk = InstanceType<typeof Cloudflare>;
type StagedOrdinaryWorkerUploadMetadata = VersionCreateParams.Metadata & {
  readonly limits?: {
    readonly cpu_ms?: number;
    readonly subrequests?: number;
  };
};
type OrdinaryWorkerUploadMetadata =
  | (ScriptUpdateParams.Metadata & {
      readonly limits?: {
        readonly cpu_ms?: number;
        readonly subrequests?: number;
      };
    })
  | StagedOrdinaryWorkerUploadMetadata;
const PREPARED_ORDINARY_WORKER_UPLOAD: unique symbol = Symbol(
  'fleet-control.preparedOrdinaryWorkerUpload',
);
const PREPARED_ORDINARY_WORKER_DEPLOYMENT_VERSIONS: unique symbol = Symbol(
  'fleet-control.preparedOrdinaryWorkerDeploymentVersions',
);
/** @inline */
export type PreparedOrdinaryWorkerUpload = Readonly<{
  [PREPARED_ORDINARY_WORKER_UPLOAD]: true;
  intent: PlainWorkerUploadIntent;
  files: readonly File[];
  metadata: string;
  secretValues: readonly string[];
}>;
/** @inline */
export type PreparedOrdinaryWorkerDeploymentVersions = readonly Readonly<{
  percentage: number;
  version_id: string;
}>[] & {
  readonly [PREPARED_ORDINARY_WORKER_DEPLOYMENT_VERSIONS]: true;
};

function readWorkerVersionTag(version: unknown): string | undefined {
  return readStringField(readField(version, 'annotations'), 'workers/tag');
}

export function workerMigrations(
  migrations: readonly import('./types.js').DurableObjectMigration[],
  previousTag?: string,
) {
  if (migrations.length === 0) return undefined;
  let pending = migrations;
  if (previousTag !== undefined) {
    const previousIndex = migrations.findIndex(
      (migration) => migration.tag === previousTag,
    );
    if (previousIndex < 0) {
      throw new Error(
        `previous Durable Object tag '${previousTag}' is absent from the ordered migration history`,
      );
    }
    pending = migrations.slice(previousIndex + 1);
  }
  if (pending.length === 0) return undefined;
  return {
    new_tag: pending.at(-1)?.tag,
    old_tag: previousTag,
    steps: pending.map((migration) => ({
      new_sqlite_classes: migration.newSqliteClasses
        ? [...migration.newSqliteClasses]
        : undefined,
      new_classes: migration.newClasses ? [...migration.newClasses] : undefined,
      deleted_classes: migration.deletedClasses
        ? [...migration.deletedClasses]
        : undefined,
      renamed_classes: migration.renamedClasses?.map((renamed) => ({
        from: renamed.from,
        to: renamed.to,
      })),
    })),
  };
}

/**
 * Dependencies used by ordinary-Worker provider operations.
 *
 * `accountId` and `client` are values captured from the provisioning client;
 * the SDK transport reaches that client's request path. `schedule` enters its
 * operation queue, `collectBounded` walks inventory while leaving the default
 * bound on the client, `withMutationFence` establishes its mutation-fence
 * scope, and `workerRouteZoneIds` performs its zone-id lookup.
 *
 * `CloudflareProvisioningClient` creates one context in its constructor from
 * those values and bound arrows.
 */
export interface OrdinaryWorkerContext {
  readonly accountId: string;
  readonly client: CloudflareSdk;
  readonly inventoryClient: CloudflareSdk;
  readonly versionInventoryClient: CloudflareSdk;
  schedule<T>(operation: () => Promise<T>): Promise<T>;
  collectBounded<T>(
    iterable: AsyncIterable<T> | Iterable<T>,
    label: string,
    max?: number,
  ): AsyncGenerator<T>;
  withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T>;
  workerRouteZoneIds(): Promise<readonly string[]>;
}
type OrdinaryWorkerBaseContext = Pick<
  OrdinaryWorkerContext,
  'accountId' | 'client' | 'schedule'
>;
type OrdinaryWorkerPagedContext = Pick<
  OrdinaryWorkerContext,
  | 'accountId'
  | 'client'
  | 'inventoryClient'
  | 'versionInventoryClient'
  | 'schedule'
  | 'collectBounded'
>;
type OrdinaryWorkerFencedContext = Pick<
  OrdinaryWorkerContext,
  'accountId' | 'client' | 'schedule' | 'withMutationFence'
>;
type OrdinaryWorkerFootprintContext = Pick<
  OrdinaryWorkerContext,
  | 'accountId'
  | 'client'
  | 'inventoryClient'
  | 'schedule'
  | 'collectBounded'
  | 'workerRouteZoneIds'
>;
type OrdinaryWorkerCollectContext = Pick<
  OrdinaryWorkerContext,
  'accountId' | 'inventoryClient' | 'collectBounded'
>;

export async function ordinaryWorkerSubdomain(
  context: Pick<OrdinaryWorkerContext, 'accountId' | 'client'>,
  scriptName: string,
): Promise<Readonly<{ enabled: boolean; previews_enabled: boolean }>> {
  const response = await context.client.workers.scripts.subdomain
    .get(scriptName, { account_id: context.accountId })
    .asResponse();
  const media = response.headers
    .get('content-type')
    ?.split(';')[0]
    ?.trim()
    .toLowerCase();
  if (
    response.status !== 200 ||
    !(media === 'application/json' || media?.endsWith('+json'))
  ) {
    void response.body?.cancel().catch(() => undefined);
    throw new Error(
      `ordinary Worker '${scriptName}' returned incomplete public-access metadata`,
    );
  }
  const body: unknown = await response.json();
  const result = readField(body, 'result');
  const errors = readField(body, 'errors') ?? undefined;
  const enabled = readField(result, 'enabled');
  const previews = readField(result, 'previews_enabled');
  if (
    readField(body, 'success') !== true ||
    (errors !== undefined && (!Array.isArray(errors) || errors.length !== 0)) ||
    !result ||
    typeof result !== 'object' ||
    Array.isArray(result) ||
    !Object.hasOwn(result, 'enabled') ||
    !Object.hasOwn(result, 'previews_enabled') ||
    typeof enabled !== 'boolean' ||
    typeof previews !== 'boolean'
  )
    throw new Error(
      `ordinary Worker '${scriptName}' returned incomplete public-access metadata`,
    );
  return { enabled, previews_enabled: previews };
}

export async function listOrdinaryWorkerSecretNames(
  context: OrdinaryWorkerPagedContext,
  scriptName: string,
): Promise<readonly string[]> {
  return context.schedule(() => ordinaryWorkerSecretNames(context, scriptName));
}

export async function ordinaryWorkerSecretNames(
  context: OrdinaryWorkerCollectContext,
  scriptName: string,
): Promise<string[]> {
  const names: string[] = [];
  try {
    for await (const secret of context.collectBounded(
      context.inventoryClient.workers.scripts.secrets.list(scriptName, {
        account_id: context.accountId,
      }),
      'ordinary Worker secret inventory',
    )) {
      const name = readStringField(secret, 'name');
      if (!name) {
        throw new Error(
          `ordinary Worker '${scriptName}' returned a secret without a name`,
        );
      }
      names.push(name);
    }
  } catch (error) {
    if (isNotFound(error)) return [];
    throw error;
  }
  return names.sort();
}

export async function listOrdinaryWorkerDatabases(
  context: OrdinaryWorkerPagedContext,
  filter?: Readonly<{ name?: string }>,
): Promise<readonly PlainWorkerDatabaseInventoryEntry[]> {
  return context.schedule(async () => {
    const databases: PlainWorkerDatabaseInventoryEntry[] = [];
    for await (const database of context.collectBounded(
      context.inventoryClient.d1.database.list({
        account_id: context.accountId,
        per_page: 100,
        ...(filter?.name === undefined ? {} : { name: filter.name }),
      }),
      'D1 database inventory',
      MAX_DATABASE_INVENTORY,
    )) {
      const databaseId = readStringField(database, 'uuid');
      const name = readStringField(database, 'name');
      if (!databaseId || !name)
        throw new Error('D1 database inventory has an invalid uuid or name');
      databases.push({ databaseId, name });
    }
    return databases;
  });
}

export async function ordinaryWorkerDeploymentStatus(
  context: OrdinaryWorkerBaseContext,
  scriptName: string,
): Promise<PlainWorkerDeploymentStatus | undefined> {
  return context.schedule(async () => {
    try {
      const listed = await context.client.workers.scripts.deployments.list(
        scriptName,
        {
          account_id: context.accountId,
        },
      );
      const deployment = listed.deployments[0];
      if (!deployment) return undefined;
      return {
        versions: readArrayField(deployment, 'versions').map((version) => {
          const rawPercentage = readField(version, 'percentage');
          return {
            versionId:
              readStringField(version, 'id') ??
              readStringField(version, 'version_id'),
            percentage:
              rawPercentage === undefined ? undefined : Number(rawPercentage),
          };
        }),
      };
    } catch (error) {
      if (isNotFound(error)) return undefined;
      throw error;
    }
  });
}

export async function listOrdinaryWorkerVersions(
  context: OrdinaryWorkerPagedContext,
  scriptName: string,
): Promise<readonly PlainWorkerVersionSummary[] | undefined> {
  return context.schedule(async () => {
    try {
      const versions: PlainWorkerVersionSummary[] = [];
      for await (const version of context.collectBounded(
        context.versionInventoryClient.workers.scripts.versions.list(
          scriptName,
          {
            account_id: context.accountId,
            per_page: 100,
          },
        ),
        'ordinary Worker version inventory',
        MAX_VERSION_INVENTORY,
      )) {
        versions.push({
          versionId:
            readStringField(version, 'id') ??
            readStringField(version, 'version_id'),
          tag: readWorkerVersionTag(version),
        });
      }
      return versions;
    } catch (error) {
      // The SDK requests the page after the last item. A 404 observes the
      // script's absence between pages, so earlier pages are stale.
      if (isNotFound(error)) return undefined;
      throw error;
    }
  });
}

export async function viewOrdinaryWorkerVersion(
  context: OrdinaryWorkerBaseContext,
  scriptName: string,
  versionId: string,
): Promise<PlainWorkerVersionDetail> {
  return context.schedule(async () => {
    const version = await context.client.workers.scripts.versions.get(
      versionId,
      { account_id: context.accountId, script_name: scriptName },
    );
    return {
      versionId:
        readStringField(version, 'id') ??
        readStringField(version, 'version_id'),
      tag: readWorkerVersionTag(version),
      bindings: providerBindingsToPlainWorkerShape(
        readArrayField(readField(version, 'resources'), 'bindings'),
      ),
    };
  });
}

export async function findOrdinaryWorkerVersion(
  context: OrdinaryWorkerBaseContext,
  scriptName: string,
  versionId: string,
): Promise<PlainWorkerVersionDetail | undefined> {
  try {
    return await viewOrdinaryWorkerVersion(context, scriptName, versionId);
  } catch (error) {
    if (isNotFound(error)) return undefined;
    throw error;
  }
}

export async function prepareOrdinaryWorkerUpload(
  intent: PlainWorkerUploadIntent,
): Promise<PreparedOrdinaryWorkerUpload> {
  for (const module of intent.modules) {
    if (
      !module ||
      typeof module.name !== 'string' ||
      (typeof module.content !== 'string' &&
        !(module.content instanceof Uint8Array)) ||
      (module.contentType !== undefined &&
        typeof module.contentType !== 'string')
    ) {
      throw new TypeError(
        'ordinary Worker modules must contain valid upload data',
      );
    }
  }
  const bindings = uploadIntentToProviderBindings(intent);
  const secretValues = intent.bindings.secrets.map(({ value }) => value);
  const baseMetadata: StagedOrdinaryWorkerUploadMetadata = {
    main_module: intent.mainModule,
    bindings,
    compatibility_date: intent.compatibilityDate,
    compatibility_flags: intent.compatibilityFlags
      ? [...intent.compatibilityFlags]
      : undefined,
    limits:
      intent.limits.cpuMs === undefined &&
      intent.limits.subrequests === undefined
        ? undefined
        : {
            cpu_ms: intent.limits.cpuMs,
            subrequests: intent.limits.subrequests,
          },
    annotations: { 'workers/tag': intent.candidateTag },
  };
  const metadata: OrdinaryWorkerUploadMetadata =
    intent.mode === 'initial'
      ? {
          ...baseMetadata,
          migrations: workerMigrations(intent.durableObjectMigrations),
        }
      : baseMetadata;
  const encodedMetadata = JSON.stringify(metadata);
  const files = await Promise.all(
    intent.modules.map((module) =>
      toFile(
        typeof module.content === 'string'
          ? new TextEncoder().encode(module.content)
          : module.content,
        module.name,
        {
          type: module.contentType ?? 'application/javascript+module',
        },
      ),
    ),
  );
  return {
    [PREPARED_ORDINARY_WORKER_UPLOAD]: true,
    intent,
    files,
    metadata: encodedMetadata,
    secretValues,
  };
}

export async function dispatchOrdinaryWorkerUpload(
  context: OrdinaryWorkerBaseContext,
  prepared: PreparedOrdinaryWorkerUpload,
): Promise<void> {
  const { files, intent, metadata, secretValues } = prepared;
  await context.schedule(async () => {
    const subdomain = context.client.workers.scripts.subdomain;
    const uploadParameters = {
      account_id: context.accountId,
      // cloudflare/internal/uploads.mjs:102-129 bracket-flattens objects;
      // Wrangler 4.118.0 serializes the same metadata value as JSON.
      metadata: metadata as never,
    };
    const body = namedWorkerUploadBody(files, metadata);
    const send = async (call: () => Promise<unknown>): Promise<void> => {
      try {
        await call();
      } catch (error) {
        throw sanitizeProviderError(error, secretValues);
      }
    };
    if (intent.mode === 'initial') {
      await send(() =>
        context.client.workers.scripts.update(
          intent.scriptName,
          uploadParameters,
          {
            maxRetries: 0,
            body,
            headers: { 'Content-Type': null },
          },
        ),
      );
      // Sanitization is limited to the upload request that carries secrets.
      // Cloudflare rejects subdomain writes before the script exists. The
      // caller attests public access before adopting a reconciled upload.
      await subdomain.create(intent.scriptName, {
        account_id: context.accountId,
        enabled: intent.publicAccess.workersDevEnabled,
        previews_enabled: intent.publicAccess.previewUrlsEnabled,
      });
      return;
    }
    const current = await ordinaryWorkerSubdomain(context, intent.scriptName);
    if (
      current.enabled !== intent.publicAccess.workersDevEnabled ||
      current.previews_enabled !== intent.publicAccess.previewUrlsEnabled
    ) {
      // The staged path can converge public access first because the script
      // exists; write-on-difference moves it toward the constant intent.
      await subdomain.create(intent.scriptName, {
        account_id: context.accountId,
        enabled: intent.publicAccess.workersDevEnabled,
        previews_enabled: intent.publicAccess.previewUrlsEnabled,
      });
    }
    await send(() =>
      context.client.workers.scripts.versions.create(
        intent.scriptName,
        uploadParameters,
        { maxRetries: 0, body },
      ),
    );
  });
}

export function prepareOrdinaryWorkerDeployment(
  versions: readonly OrdinaryWorkerDeploymentVersion[],
): PreparedOrdinaryWorkerDeploymentVersions {
  assertOrdinaryWorkerDeploymentVersions(versions);
  return Object.assign(
    versions.map(({ versionId, percentage }) => ({
      percentage,
      version_id: versionId,
    })),
    { [PREPARED_ORDINARY_WORKER_DEPLOYMENT_VERSIONS]: true as const },
  );
}

export async function dispatchOrdinaryWorkerDeployment(
  context: OrdinaryWorkerBaseContext,
  scriptName: string,
  versions: PreparedOrdinaryWorkerDeploymentVersions,
): Promise<void> {
  await context.schedule(() =>
    context.client.workers.scripts.deployments.create(
      scriptName,
      {
        account_id: context.accountId,
        strategy: 'percentage',
        versions: [...versions],
      },
      { maxRetries: 0 },
    ),
  );
}

export async function deleteOrdinaryWorkerScript(
  context: OrdinaryWorkerBaseContext,
  scriptName: string,
): Promise<'deleted' | 'absent'> {
  return context.schedule(async () => {
    try {
      await context.client.workers.scripts.delete(scriptName, {
        account_id: context.accountId,
      });
      return 'deleted';
    } catch (error) {
      if (isNotFound(error)) return 'absent';
      throw error;
    }
  });
}

export async function disableOrdinaryWorkerPublicAccess(
  context: OrdinaryWorkerFencedContext,
  scriptName: string,
  fence: ExternalMutationFence,
): Promise<void> {
  await context.withMutationFence(fence, () =>
    context.schedule(async () => {
      try {
        await context.client.workers.scripts.subdomain.create(scriptName, {
          account_id: context.accountId,
          enabled: false,
          previews_enabled: false,
        });
      } catch (error) {
        if (!isNotFound(error)) throw error;
        return;
      }
      const subdomain = await (async () => {
        try {
          return await ordinaryWorkerSubdomain(context, scriptName);
        } catch (error) {
          if (!isNotFound(error)) throw error;
          return undefined;
        }
      })();
      if (!subdomain) return;
      if (subdomain.enabled === true || subdomain.previews_enabled === true) {
        throw new Error(
          `ordinary Worker '${scriptName}' retains public subdomain ingress`,
        );
      }
    }),
  );
}

export async function listCustomDomains(
  context: OrdinaryWorkerPagedContext,
): Promise<readonly OrdinaryWorkerFootprint['customDomains'][number][]> {
  return context.schedule(async () => {
    const domains: Array<OrdinaryWorkerFootprint['customDomains'][number]> = [];
    for await (const domain of context.collectBounded(
      context.inventoryClient.workers.domains.list({
        account_id: context.accountId,
      }),
      'custom domain inventory',
    )) {
      if (
        !readStringField(domain, 'id') ||
        !readStringField(domain, 'hostname') ||
        !readStringField(domain, 'service')
      ) {
        throw new Error(
          'Cloudflare returned incomplete custom-domain metadata',
        );
      }
      domains.push({
        id: domain.id,
        hostname: domain.hostname,
        service: domain.service,
      });
    }
    return domains;
  });
}

export function attachCustomDomain(
  context: OrdinaryWorkerFencedContext,
  target: { readonly hostname: string; readonly service: string },
  fence: ExternalMutationFence,
): Promise<void> {
  return context.withMutationFence(fence, () =>
    context.schedule(async () => {
      await context.client.workers.domains.update({
        account_id: context.accountId,
        hostname: target.hostname,
        service: target.service,
      });
    }),
  );
}

export function detachCustomDomain(
  context: OrdinaryWorkerFencedContext,
  domainId: string,
  fence: ExternalMutationFence,
): Promise<void> {
  return context.withMutationFence(fence, () =>
    context.schedule(async () => {
      try {
        await context.client.workers.domains.delete(domainId, {
          account_id: context.accountId,
        });
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
    }),
  );
}

export async function inspectActiveWorkerRoute(
  context: OrdinaryWorkerBaseContext,
  scriptName: string,
): Promise<
  | Readonly<{
      artifactVersion: string;
      specDigest: string | undefined;
    }>
  | undefined
> {
  return context.schedule(async () => {
    let deploymentList: Awaited<
      ReturnType<CloudflareSdk['workers']['scripts']['deployments']['list']>
    >;
    try {
      deploymentList = await context.client.workers.scripts.deployments.list(
        scriptName,
        { account_id: context.accountId },
      );
    } catch (error) {
      if (isNotFound(error)) return undefined;
      throw error;
    }
    const artifactVersion = attestedActiveVersionId(
      deploymentList.deployments[0],
      scriptName,
    );
    const version = await context.client.workers.scripts.versions.get(
      artifactVersion,
      { account_id: context.accountId, script_name: scriptName },
    );
    const specDigest = (
      version.resources.bindings === undefined ? [] : version.resources.bindings
    ).flatMap((binding) =>
      binding.type === 'plain_text' && binding.name === 'FLEET_SPEC_DIGEST'
        ? [binding.text]
        : [],
    )[0];
    return {
      artifactVersion,
      specDigest: typeof specDigest === 'string' ? specDigest : undefined,
    };
  });
}

export async function inspectOrdinaryWorkerFootprint(
  context: OrdinaryWorkerFootprintContext,
  scriptName: string,
): Promise<OrdinaryWorkerFootprint> {
  return context.schedule(async () => {
    let scriptPresent = false;
    for await (const script of context.collectBounded(
      context.inventoryClient.workers.scripts.list({
        account_id: context.accountId,
      }),
      'ordinary Worker script inventory',
    )) {
      const id = readStringField(script, 'id');
      if (!id)
        throw new Error('ordinary Worker script inventory has an invalid ID');
      if (id === scriptName) scriptPresent = true;
    }
    const customDomains: Array<{
      id: string;
      hostname: string;
      service: string;
    }> = [];
    for await (const domain of context.collectBounded(
      context.inventoryClient.workers.domains.list({
        account_id: context.accountId,
      }),
      'custom domain inventory',
    )) {
      const service = readStringField(domain, 'service');
      if (!service)
        throw new Error('custom domain inventory has an invalid service');
      if (service !== scriptName) continue;
      if (
        !readStringField(domain, 'id') ||
        !readStringField(domain, 'hostname')
      ) {
        throw new Error(
          `ordinary Worker '${scriptName}' has incomplete custom-domain metadata`,
        );
      }
      customDomains.push({
        id: domain.id,
        hostname: domain.hostname,
        service: domain.service,
      });
    }
    const zoneRoutes: import('./types.js').WorkerZoneRoute[] = [];
    for (const zoneId of await context.workerRouteZoneIds()) {
      for await (const route of context.collectBounded(
        context.inventoryClient.workers.routes.list({ zone_id: zoneId }),
        'Worker zone-route inventory',
      )) {
        const script = readField(route, 'script');
        if (
          script !== undefined &&
          script !== null &&
          typeof script !== 'string'
        )
          throw new Error('Worker zone-route inventory has an invalid script');
        if (script !== scriptName) continue;
        if (
          !readStringField(route, 'id') ||
          !readStringField(route, 'pattern')
        ) {
          throw new Error(
            `ordinary Worker '${scriptName}' has incomplete zone-route metadata`,
          );
        }
        zoneRoutes.push({
          zoneId,
          routeId: route.id,
          pattern: route.pattern,
        });
      }
    }
    const subdomain = scriptPresent
      ? await ordinaryWorkerSubdomain(context, scriptName)
      : undefined;
    return {
      scriptPresent,
      workersDevEnabled: subdomain?.enabled === true,
      previewUrlsEnabled: subdomain?.previews_enabled === true,
      customDomains,
      zoneRoutes,
    };
  });
}
