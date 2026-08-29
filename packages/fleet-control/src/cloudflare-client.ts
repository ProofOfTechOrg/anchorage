// SPDX-License-Identifier: Apache-2.0

import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';
import Cloudflare from 'cloudflare';
import { toFile } from 'cloudflare/uploads';
import PQueue from 'p-queue';
import { exactActiveVersionId } from './active-route.js';
import { canonicalApplicationBindings } from './application-bindings.js';
import {
  CLOUDFLARE_INVENTORY_BOUND,
  CLOUDFLARE_SDK_MAX_RETRIES,
} from './cloudflare-client-config.js';
import {
  attachCustomDomain,
  type CloudflareSdk,
  deleteOrdinaryWorkerScript,
  detachCustomDomain,
  disableOrdinaryWorkerPublicAccess,
  dispatchOrdinaryWorkerDeployment,
  dispatchOrdinaryWorkerUpload,
  findOrdinaryWorkerVersion,
  inspectActiveWorkerRoute,
  inspectOrdinaryWorkerFootprint,
  listCustomDomains,
  listOrdinaryWorkerDatabases,
  listOrdinaryWorkerSecretNames,
  listOrdinaryWorkerVersions,
  MAX_DATABASE_INVENTORY,
  type PreparedOrdinaryWorkerDeploymentVersions as OperationsPreparedOrdinaryWorkerDeploymentVersions,
  type PreparedOrdinaryWorkerUpload as OperationsPreparedOrdinaryWorkerUpload,
  type OrdinaryWorkerContext,
  type OrdinaryWorkerFootprint,
  ordinaryWorkerDeploymentStatus,
  ordinaryWorkerSecretNames,
  prepareOrdinaryWorkerDeployment,
  prepareOrdinaryWorkerUpload,
  viewOrdinaryWorkerVersion,
  workerMigrations,
} from './cloudflare-ordinary-worker-operations.js';
import {
  isNotFound,
  readErrorFieldSafely,
  sanitizedErrorName,
} from './cloudflare-provider-errors.js';
import type { CloudflareApiRateCoordinator } from './cloudflare-rate-coordinator.js';
import {
  advanceWorkerAttachmentScan,
  CloudflareAttachmentScanDriftError,
  type CloudflareWorkerAttachmentScanContext,
  listAllDispatchScripts,
  listAllWorkerAttachments,
  type WorkerAttachmentScanChunk,
  type WorkerAttachmentScanInput,
} from './cloudflare-worker-attachment-scan.js';
import type { DurableDatabaseExportStore } from './database-export-store.js';
import {
  type HostRoutingTarget,
  parseHostRoutingTarget,
} from './host-routing.js';
import {
  canonicalDeploymentEgressPolicy,
  externalPlatformResourceGroupId,
  externalStateScriptName,
  FLEET_AUDIT_PROXY_BINDING,
  FLEET_AUDIT_PROXY_CLASS_NAME,
  FLEET_AUDIT_PROXY_STATE_BINDING,
} from './platform-resources.js';
import {
  assertProviderBindingIdentitiesMatchInspection,
  assertSupportedProviderBindings,
} from './provider-binding-inventory.js';
import { deploymentSpecDigest } from './spec-digest.js';
import type {
  DatabaseExport,
  DatabaseReference,
  DecommissionAttachmentScanInput,
  DecommissionAttachmentScanResult,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
  FleetResourceInventory,
  OrdinaryWorkerDeploymentVersion,
  PlainWorkerDatabaseInventoryEntry,
  PlainWorkerDeploymentStatus,
  PlainWorkerRouteApi,
  PlainWorkerUploadIntent,
  PlainWorkerVersionDetail,
  PlainWorkerVersionSummary,
  PromotionGuard,
  ProviderBindingIdentity,
  ScriptInventoryTarget,
} from './types.js';

const AUDIT_CONSUMER_SETTINGS = Object.freeze({
  batch_size: 100,
  max_concurrency: 4,
  max_retries: 5,
  max_wait_time_ms: 5_000,
});
const SDK_TRANSPORT_TIMEOUT_MS = 2_147_483_647;

export interface CloudflareClientOptions {
  readonly accountId: string;
  readonly apiToken: string;
  readonly dispatchNamespace: string;
  readonly rateCoordinator: CloudflareApiRateCoordinator;
  readonly concurrency?: number;
  readonly requestTimeoutMs?: number;
  readonly fetch?: typeof fetch;
  readonly exportStore?: DurableDatabaseExportStore;
}

export type PlainWorkerCloudflareClientOptions = Omit<
  CloudflareClientOptions,
  'dispatchNamespace'
> & { readonly plane: 'plain-worker' };

export class CloudflarePlaneCapabilityError extends Error {
  readonly operation: string;
  readonly requiredPlane = 'workers-for-platforms' as const;

  constructor(operation: string) {
    super(
      `Cloudflare operation '${operation}' requires the workers-for-platforms plane`,
    );
    this.name = 'CloudflarePlaneCapabilityError';
    this.operation = operation;
  }
}

export type { DurableDatabaseExportStore } from './database-export-store.js';

export interface ControlWorkerSpec {
  readonly scriptName: string;
  readonly mainModule: string;
  readonly modules: readonly import('./types.js').WorkerModule[];
  readonly compatibilityDate: string;
  readonly compatibilityFlags?: readonly string[];
  readonly bindings: readonly Readonly<Record<string, unknown>>[];
  readonly migrations?: Readonly<Record<string, unknown>>;
  readonly tags?: readonly string[];
}

export interface ControlWorkerInspection {
  readonly artifactVersion: string;
  readonly databaseIds: readonly string[];
  readonly durableObjectBindings: readonly import('./types.js').DurableObjectBindingInventory[];
  readonly kvNamespaceBindings: readonly Readonly<{
    name: string;
    namespaceId: string;
  }>[];
  readonly serviceBindings: readonly Readonly<{
    name: string;
    service: string;
    entrypoint?: string;
  }>[];
  readonly queueProducerBindings: readonly Readonly<{
    name: string;
    queueName: string;
  }>[];
  readonly r2BucketBindings: readonly import('./types.js').ApplicationR2Binding[];
  readonly dispatchNamespaceBindings: readonly Readonly<{
    name: string;
    namespace: string;
    outbound: unknown;
  }>[];
  readonly secretNames: readonly string[];
  readonly plainTextBindings: Readonly<Record<string, string>>;
  readonly providerBindingIdentities: readonly ProviderBindingIdentity[];
  readonly workersDevEnabled: boolean;
  readonly previewUrlsEnabled: boolean;
  readonly routeHostnames: readonly string[];
  readonly zoneRoutes: readonly import('./types.js').WorkerZoneRoute[];
}

export type { OrdinaryWorkerFootprint } from './cloudflare-ordinary-worker-operations.js';

const SCRIPT_INVENTORY_PREFIX = '__anchorage_script__:';
const FLEET_SCRIPT_TAG = 'fleet:anchorage';

function tagValue(tags: readonly string[], prefix: string): string | undefined {
  return tags.find((tag) => tag.startsWith(prefix))?.slice(prefix.length);
}

/** @inline */
type PreparedOrdinaryWorkerUpload = OperationsPreparedOrdinaryWorkerUpload;
/** @inline */
type PreparedOrdinaryWorkerDeploymentVersions =
  OperationsPreparedOrdinaryWorkerDeploymentVersions;

export class CloudflareProviderRequestNotDispatchedError extends Error {
  constructor(cause: unknown) {
    super('Cloudflare provider request was not dispatched', { cause });
    this.name = 'CloudflareProviderRequestNotDispatchedError';
  }
}

function inventoryBoundExceeded(label: string, max: number): Error {
  return new Error(
    `${label} exceeded the supported inventory bound of ${max} items`,
  );
}

const REQUIRED_ZONE_PERMISSION_GROUPS = [
  ['Zone Read'],
  ['Workers Routes Read'],
  ['Workers Routes Edit', 'Workers Routes Write'],
] as const;

function policyPermissionNames(
  policy: Readonly<{
    permission_groups: readonly Readonly<{ name?: string }>[];
  }>,
): ReadonlySet<string> {
  return new Set(
    policy.permission_groups.flatMap((group) =>
      group.name ? [group.name] : [],
    ),
  );
}

function policyCoversAllAccountZones(
  resources:
    | Readonly<Record<string, string>>
    | Readonly<Record<string, Readonly<Record<string, string>>>>,
  accountId: string,
): boolean {
  if (resources['com.cloudflare.api.account.zone.*'] === '*') return true;
  const accountZones = resources[`com.cloudflare.api.account.${accountId}`];
  return (
    typeof accountZones === 'object' &&
    accountZones !== null &&
    accountZones['com.cloudflare.api.account.zone.*'] === '*'
  );
}

function policyRestrictsZoneAccess(
  resources:
    | Readonly<Record<string, string>>
    | Readonly<Record<string, Readonly<Record<string, string>>>>,
  accountId: string,
): boolean {
  for (const [resource, access] of Object.entries(resources)) {
    if (
      resource === 'com.cloudflare.api.account.zone.*' ||
      resource.startsWith('com.cloudflare.api.account.zone.')
    ) {
      return access === '*';
    }
    if (
      resource === `com.cloudflare.api.account.${accountId}` &&
      typeof access === 'object' &&
      access !== null &&
      Object.keys(access).some(
        (nested) =>
          nested === 'com.cloudflare.api.account.zone.*' ||
          nested.startsWith('com.cloudflare.api.account.zone.'),
      )
    ) {
      return true;
    }
  }
  return false;
}

function assertAccountWideZoneToken(options: {
  readonly accountId: string;
  readonly policies: readonly Readonly<{
    effect: 'allow' | 'deny';
    permission_groups: readonly Readonly<{ name?: string }>[];
    resources:
      | Readonly<Record<string, string>>
      | Readonly<Record<string, Readonly<Record<string, string>>>>;
  }>[];
}): void {
  for (const policy of options.policies) {
    const permissionNames = policyPermissionNames(policy);
    if (
      policy.effect === 'deny' &&
      policyRestrictsZoneAccess(policy.resources, options.accountId) &&
      REQUIRED_ZONE_PERMISSION_GROUPS.some((alternatives) =>
        alternatives.some((name) => permissionNames.has(name)),
      )
    ) {
      throw new Error(
        `Cloudflare API token has an explicit zone denial; account-wide route attestation for '${options.accountId}' is unavailable`,
      );
    }
  }
  for (const alternatives of REQUIRED_ZONE_PERMISSION_GROUPS) {
    const allowed = options.policies.some((policy) => {
      if (
        policy.effect !== 'allow' ||
        !policyCoversAllAccountZones(policy.resources, options.accountId)
      ) {
        return false;
      }
      const names = policyPermissionNames(policy);
      return alternatives.some((name) => names.has(name));
    });
    if (!allowed) {
      throw new Error(
        `Cloudflare API token must grant ${alternatives.join(' or ')} for every zone in account '${options.accountId}'`,
      );
    }
  }
}

function d1RestParameters(
  bindings: readonly string[],
  operation: string,
): string[] {
  if (bindings.some((binding) => typeof binding !== 'string')) {
    throw new Error(`${operation} bindings must be strings`);
  }
  return [...bindings];
}

function queueConsumerMatches(
  consumer: Readonly<{
    type?: string;
    script_name?: string;
    dead_letter_queue?: string;
    settings?: Readonly<{
      batch_size?: number;
      max_concurrency?: number;
      max_retries?: number;
      max_wait_time_ms?: number;
    }>;
  }>,
  options: Readonly<{ scriptName: string; deadLetterQueue?: string }>,
): boolean {
  return (
    consumer.type === 'worker' &&
    consumer.script_name === options.scriptName &&
    (consumer.dead_letter_queue ?? '') === (options.deadLetterQueue ?? '') &&
    consumer.settings?.batch_size === AUDIT_CONSUMER_SETTINGS.batch_size &&
    consumer.settings.max_concurrency ===
      AUDIT_CONSUMER_SETTINGS.max_concurrency &&
    consumer.settings.max_retries === AUDIT_CONSUMER_SETTINGS.max_retries &&
    consumer.settings.max_wait_time_ms ===
      AUDIT_CONSUMER_SETTINGS.max_wait_time_ms
  );
}

export function dispatchMigrations(spec: DeploymentSpec) {
  return workerMigrations(
    spec.durableObjectMigrations,
    spec.previousDurableObjectTag,
  );
}

async function hashExport(
  body: ReadableStream<Uint8Array>,
): Promise<{ sha256: string; size: number }> {
  const hash = createHash('sha256');
  const reader = body.getReader();
  let size = 0;
  for (;;) {
    const chunk = await reader.read();
    if (chunk.done) break;
    hash.update(chunk.value);
    size += chunk.value.byteLength;
  }
  return { sha256: hash.digest('hex'), size };
}

let trackProviderDispatch: <T>(
  client: CloudflareProvisioningClient,
  operation: () => Promise<T>,
) => Promise<T>;

let scanProviderAttachments: (
  client: CloudflareProvisioningClient,
  input: WorkerAttachmentScanInput,
) => Promise<WorkerAttachmentScanChunk>;

/**
 * Runs `operation`; rejects with
 * `CloudflareProviderRequestNotDispatchedError` (`cause` = the failure) when it
 * fails before any provider mutation request was invoked. Provider reads do
 * not count. Not re-entrant: one scope per outcome member. A nested scope
 * shadows the outer store, so a mutation dispatched inside it leaves the outer
 * tracker unmarked and a later outer failure is misclassified as pre-dispatch.
 */
export function withProviderDispatchTracking<T>(
  client: CloudflareProvisioningClient,
  operation: () => Promise<T>,
): Promise<T> {
  return trackProviderDispatch(client, operation);
}

/** @internal Package-private seam for the resumable lifecycle engine. */
export function advanceCloudflareWorkerAttachmentScan(
  client: CloudflareProvisioningClient,
  input: WorkerAttachmentScanInput,
): Promise<WorkerAttachmentScanChunk> {
  return scanProviderAttachments(client, input);
}

/** @internal Package-private conversion for the bounded lifecycle provider. */
export function mapDecommissionAttachmentScanChunk(
  chunk: WorkerAttachmentScanChunk,
): DecommissionAttachmentScanResult {
  switch (chunk.status) {
    case 'pending':
      if (chunk.attachments.length !== 0) {
        throw new Error(
          'bounded attachment scan returned unexpected accumulated attachments',
        );
      }
      return {
        status: 'pending',
        progress: chunk.progress,
        providerFetchAttemptsReserved: chunk.providerFetchAttemptsReserved,
      };
    case 'attached':
      if (chunk.attachment.plane === 'ordinary') {
        return {
          status: 'attached',
          attachment: {
            plane: 'ordinary',
            scriptName: chunk.attachment.scriptName,
          },
          providerFetchAttemptsReserved: chunk.providerFetchAttemptsReserved,
        };
      }
      if (
        chunk.attachment.plane !== 'dispatch' ||
        !chunk.attachment.dispatchNamespace
      ) {
        throw new Error(
          'bounded attachment scan returned malformed dispatch attachment',
        );
      }
      return {
        status: 'attached',
        attachment: {
          plane: 'dispatch',
          scriptName: chunk.attachment.scriptName,
          dispatchNamespace: chunk.attachment.dispatchNamespace,
        },
        providerFetchAttemptsReserved: chunk.providerFetchAttemptsReserved,
      };
    case 'complete':
      if (chunk.attachments.length !== 0) {
        throw new Error(
          'bounded attachment scan returned unexpected accumulated attachments',
        );
      }
      return {
        status: 'complete',
        evidenceSha256: chunk.evidenceSha256,
        evidenceCount: chunk.evidenceCount,
        providerFetchAttemptsReserved: chunk.providerFetchAttemptsReserved,
      };
    default: {
      const unknownChunk: never = chunk;
      void unknownChunk;
      throw new Error('bounded attachment scan returned unknown result');
    }
  }
}

export class CloudflareProvisioningClient implements PlainWorkerRouteApi {
  readonly #accountId: string;
  readonly #apiToken: string;
  readonly #dispatchNamespace: string | undefined;
  readonly #client: CloudflareSdk;
  readonly #ordinary: OrdinaryWorkerContext;
  readonly #attachmentScan: CloudflareWorkerAttachmentScanContext;
  readonly #operationQueue: PQueue;
  readonly #requestQueue: PQueue;
  readonly #rateCoordinator: CloudflareApiRateCoordinator;
  readonly #exportStore: DurableDatabaseExportStore | undefined;
  readonly #fetch: typeof fetch;
  readonly #dispatchTracker = new AsyncLocalStorage<{ dispatched: boolean }>();
  readonly #mutationFence = new AsyncLocalStorage<ExternalMutationFence>();
  readonly #requestTimeoutMs: number;

  static {
    // This module-private friend keeps dispatch classification out of the
    // public class; by convention only the direct ordinary-Worker adapter
    // enters it, and Workers for Platforms callers never do.
    trackProviderDispatch = (client, operation) =>
      client.#trackDispatch(operation);
    scanProviderAttachments = (client, input) =>
      advanceWorkerAttachmentScan(client.#attachmentScan, input);
  }

  constructor(
    options: CloudflareClientOptions | PlainWorkerCloudflareClientOptions,
  ) {
    if ('plane' in options) {
      if (options.plane !== 'plain-worker') {
        throw new Error('unsupported Cloudflare client plane');
      }
      if (!options.accountId || !options.apiToken) {
        throw new Error('accountId and apiToken are required');
      }
      if ('dispatchNamespace' in options) {
        throw new Error('plain-worker plane cannot name a dispatch namespace');
      }
    } else if (
      !options.accountId ||
      !options.apiToken ||
      !options.dispatchNamespace
    ) {
      throw new Error(
        'accountId, apiToken, and dispatchNamespace are required',
      );
    }
    if (!options.rateCoordinator) {
      throw new Error('rateCoordinator is required');
    }
    this.#accountId = options.accountId;
    this.#apiToken = options.apiToken;
    this.#dispatchNamespace =
      'plane' in options ? undefined : options.dispatchNamespace;
    const concurrency = options.concurrency ?? 8;
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new Error('concurrency must be a positive integer');
    }
    this.#operationQueue = new PQueue({ concurrency });
    this.#requestQueue = new PQueue({ concurrency });
    this.#rateCoordinator = options.rateCoordinator;
    this.#exportStore = options.exportStore;
    this.#fetch = options.fetch ?? fetch;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? 60_000;
    if (
      !Number.isSafeInteger(this.#requestTimeoutMs) ||
      this.#requestTimeoutMs < 1
    ) {
      throw new Error('requestTimeoutMs must be a positive integer');
    }
    const rateLimitedFetch: typeof fetch = (input, init) =>
      this.#request(input, init);
    this.#client = new Cloudflare({
      apiToken: options.apiToken,
      fetch: rateLimitedFetch,
      // An injected logger cannot be disabled independently, so the client
      // option must override CLOUDFLARE_LOG before credentials reach the SDK.
      logLevel: 'off',
      maxRetries: CLOUDFLARE_SDK_MAX_RETRIES,
      // The SDK timeout starts before its custom transport. Apply the real
      // timeout after shared quota acquisition so replica coordination cannot
      // consume the network request's lease-bounded execution budget.
      timeout: SDK_TRANSPORT_TIMEOUT_MS,
    });
    this.#ordinary = {
      accountId: this.#accountId,
      client: this.#client,
      schedule: (operation) => this.#schedule(operation),
      collectBounded: (iterable, label, max) =>
        this.#collectBounded(iterable, label, max),
      withMutationFence: (fence, operation) =>
        this.withMutationFence(fence, operation),
      workerRouteZoneIds: () => this.#workerRouteZoneIds(),
    };
    this.#attachmentScan = {
      accountId: this.#accountId,
      client: this.#client,
      dispatchNamespace: this.#dispatchNamespace,
      requestDispatchScriptPage: ({ namespace, cursor, perPage, signal }) => {
        const url = new URL(
          `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(this.#accountId)}/workers/dispatch/namespaces/${encodeURIComponent(namespace)}/scripts`,
        );
        url.searchParams.set('per_page', String(perPage));
        if (cursor) url.searchParams.set('cursor', cursor);
        return this.#request(url, {
          headers: { authorization: `Bearer ${this.#apiToken}` },
          signal,
        });
      },
    };
  }

  /** Configured provider request timeout in milliseconds. */
  get requestTimeoutMs(): number {
    return this.#requestTimeoutMs;
  }

  async #request(
    input: string | URL | Request,
    init?: RequestInit,
  ): Promise<Response> {
    const method = (
      init?.method ?? (input instanceof Request ? input.method : 'GET')
    ).toUpperCase();
    const mutation = method !== 'GET' && method !== 'HEAD';
    // These reads are deliberately redundant with the request-queue binding
    // for explicit call-time capture. The binding stays required for consumer
    // callbacks (injected fetch, assertOwned, and acquire); the 'runs a queued
    // request under its enqueuer context' case in
    // test/cloudflare-client-plain-worker.test.ts pins it.
    const fence = this.#mutationFence.getStore();
    const tracker = this.#dispatchTracker.getStore();
    const signal =
      init?.signal ?? (input instanceof Request ? input.signal : undefined);
    const response = await this.#requestQueue.add(
      this.#inEnqueuerContext(async () => {
        await this.#rateCoordinator.acquire(signal);
        if (mutation) {
          if (!fence) {
            throw new Error(
              `Cloudflare ${method} request requires an external mutation fence`,
            );
          }
          await fence.assertOwned();
        }
        const timeoutSignal = AbortSignal.timeout(this.#requestTimeoutMs);
        // Mark only after quota and fence checks. The SDK's `data:` FormData
        // probe is a GET, so it neither asserts nor marks.
        if (mutation && tracker) tracker.dispatched = true;
        return this.#fetch(input, {
          ...init,
          signal: signal
            ? AbortSignal.any([signal, timeoutSignal])
            : timeoutSignal,
        });
      }),
      { signal },
    );
    if (!response) {
      throw new Error('Cloudflare request queue returned no response');
    }
    return response;
  }

  async #schedule<T>(operation: () => Promise<T>): Promise<T> {
    return (await this.#operationQueue.add(
      this.#inEnqueuerContext(operation),
    )) as T;
  }

  #inEnqueuerContext<T>(operation: () => Promise<T>): () => Promise<T> {
    // p-queue starts a deferred task from the previous task's microtask, so a
    // bare callback would run under the previous operation's context. The
    // operation queue's hop precedes every read in #request; the request
    // queue's hop runs consumer callbacks that must observe their operation.
    const run = AsyncLocalStorage.snapshot();
    return () => run(operation);
  }

  async #trackDispatch<T>(operation: () => Promise<T>): Promise<T> {
    const tracker = { dispatched: false };
    try {
      return await this.#dispatchTracker.run(tracker, operation);
    } catch (error) {
      if (!tracker.dispatched) {
        throw new CloudflareProviderRequestNotDispatchedError(error);
      }
      throw error;
    }
  }

  async *#collectBounded<T>(
    iterable: AsyncIterable<T> | Iterable<T>,
    label: string,
    max = CLOUDFLARE_INVENTORY_BOUND,
  ): AsyncGenerator<T> {
    let count = 0;
    for await (const item of iterable) {
      count += 1;
      if (count > max) {
        throw inventoryBoundExceeded(label, max);
      }
      yield item;
    }
  }

  #requireDispatchNamespace(operation: string): string {
    if (this.#dispatchNamespace === undefined) {
      throw new CloudflarePlaneCapabilityError(operation);
    }
    return this.#dispatchNamespace;
  }

  async withMutationFence<T>(
    fence: ExternalMutationFence,
    operation: () => Promise<T>,
  ): Promise<T> {
    if (
      !Number.isSafeInteger(fence.mutationLeaseTtlMs) ||
      fence.mutationLeaseTtlMs < 1
    ) {
      throw new Error('external mutation fence lease TTL must be positive');
    }
    if (this.#requestTimeoutMs >= fence.mutationLeaseTtlMs) {
      throw new Error(
        'Cloudflare request timeout must be below the external mutation fence lease TTL',
      );
    }
    return this.#mutationFence.run(fence, operation);
  }

  async #workerRouteZoneIds(): Promise<readonly string[]> {
    const verification = await this.#client.user.tokens.verify();
    if (verification.status !== 'active') {
      throw new Error('Cloudflare API token is not active');
    }
    let token:
      | Awaited<ReturnType<CloudflareSdk['user']['tokens']['get']>>
      | undefined;
    try {
      token = await this.#client.accounts.tokens.get(verification.id, {
        account_id: this.#accountId,
      });
    } catch {
      try {
        token = await this.#client.user.tokens.get(verification.id);
      } catch {
        throw new Error(
          'Cloudflare API token policy is unavailable; API Tokens Read is required for account-wide zone attestation',
        );
      }
    }
    if (
      token.status !== 'active' ||
      !token.policies ||
      token.policies.length === 0
    ) {
      throw new Error(
        'Cloudflare API token returned no active policy for account-wide zone attestation',
      );
    }
    assertAccountWideZoneToken({
      accountId: this.#accountId,
      policies: token.policies,
    });
    const zoneIds: string[] = [];
    const seenZoneIds = new Set<string>();
    for await (const zone of this.#collectBounded(
      this.#client.zones.list({
        account: { id: this.#accountId },
        per_page: 50,
        type: ['full', 'partial', 'secondary', 'internal'],
      }),
      'zone inventory',
    )) {
      if (zone.account.id !== this.#accountId) {
        throw new Error(
          `Cloudflare returned zone '${zone.id}' outside account '${this.#accountId}'`,
        );
      }
      if (!zone.id || seenZoneIds.has(zone.id)) {
        throw new Error(
          'Cloudflare account-wide zone discovery returned incomplete or duplicate zone metadata',
        );
      }
      seenZoneIds.add(zone.id);
      zoneIds.push(zone.id);
    }
    return zoneIds;
  }

  platformPlaneScope(): Readonly<{
    accountId: string;
    dispatchNamespace: string;
  }> {
    const dispatchNamespace =
      this.#requireDispatchNamespace('platformPlaneScope');
    return {
      accountId: this.#accountId,
      dispatchNamespace,
    };
  }

  async #assertUntrustedDispatchNamespace(dispatchNamespace: string): Promise<
    Readonly<{
      name: string;
      namespaceId?: string;
      trustedWorkers: boolean;
      scriptCount: number;
    }>
  > {
    const namespace =
      await this.#client.workersForPlatforms.dispatch.namespaces.get(
        dispatchNamespace,
        { account_id: this.#accountId },
      );
    if (
      namespace.namespace_name !== dispatchNamespace ||
      namespace.trusted_workers !== false
    ) {
      throw new Error(
        `dispatch namespace '${dispatchNamespace}' must attest trusted_workers=false`,
      );
    }
    if (
      typeof namespace.script_count !== 'number' ||
      !Number.isSafeInteger(namespace.script_count) ||
      namespace.script_count < 0
    ) {
      throw new Error(
        `dispatch namespace '${dispatchNamespace}' returned no valid script_count`,
      );
    }
    return {
      name: namespace.namespace_name,
      ...(namespace.namespace_id
        ? { namespaceId: namespace.namespace_id }
        : {}),
      trustedWorkers: false,
      scriptCount: namespace.script_count,
    };
  }

  async assertUntrustedDispatchNamespace(): Promise<void> {
    const dispatchNamespace = this.#requireDispatchNamespace(
      'assertUntrustedDispatchNamespace',
    );
    await this.#schedule(async () => {
      await this.#assertUntrustedDispatchNamespace(dispatchNamespace);
    });
  }

  async advanceDecommissionAttachmentScan(
    input: DecommissionAttachmentScanInput,
  ): Promise<DecommissionAttachmentScanResult> {
    try {
      const chunk = await scanProviderAttachments(this, {
        target: input.progress.target,
        progress: input.progress,
        maxProviderRequests: input.maxProviderRequests,
        signal: input.signal,
        stopOnFirstAttachment: true,
      });
      return mapDecommissionAttachmentScanChunk(chunk);
    } catch (error) {
      if (error instanceof CloudflareAttachmentScanDriftError) {
        return { status: 'drift' };
      }
      throw error;
    }
  }

  async listWorkerDatabaseAttachments(databaseId: string): Promise<
    readonly Readonly<{
      scriptName: string;
      plane: 'ordinary' | 'dispatch';
      dispatchNamespace?: string;
    }>[]
  > {
    return listAllWorkerAttachments(this.#attachmentScan, {
      kind: 'd1',
      databaseId,
    });
  }

  async listWorkerR2Attachments(bucketName: string): Promise<
    readonly Readonly<{
      scriptName: string;
      plane: 'ordinary' | 'dispatch';
      dispatchNamespace?: string;
    }>[]
  > {
    return listAllWorkerAttachments(this.#attachmentScan, {
      kind: 'r2',
      bucketName,
    });
  }

  async getR2Bucket(
    bucketName: string,
    jurisdiction: import('./types.js').R2Jurisdiction,
  ): Promise<import('./types.js').ApplicationR2BucketSnapshot | undefined> {
    return this.#schedule(async () => {
      try {
        const bucket = await this.#client.r2.buckets.get(bucketName, {
          account_id: this.#accountId,
          jurisdiction,
        });
        if (
          bucket.name !== bucketName ||
          (bucket.jurisdiction !== undefined &&
            bucket.jurisdiction !== jurisdiction)
        ) {
          throw new Error(
            `R2 returned incomplete metadata for '${bucketName}'`,
          );
        }
        if (
          !bucket.creation_date ||
          !Number.isFinite(Date.parse(bucket.creation_date))
        ) {
          throw new Error(
            `R2 bucket '${bucketName}' has no valid creation date`,
          );
        }
        return {
          name: '',
          bucketName,
          jurisdiction,
          creationDate: new Date(bucket.creation_date).toISOString(),
        };
      } catch (error) {
        if (isNotFound(error)) return undefined;
        throw error;
      }
    });
  }

  async createR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.withMutationFence(fence, () =>
      this.#schedule(async () => {
        await this.#client.r2.buckets.create({
          account_id: this.#accountId,
          name: resource.bucketName,
          jurisdiction: resource.jurisdiction,
        });
      }),
    );
  }

  async assertR2BucketEmpty(
    resource: import('./types.js').ApplicationR2Binding,
  ): Promise<void> {
    await this.#schedule(async () => {
      for await (const object of this.#collectBounded(
        this.#client.r2.buckets.objects.list(resource.bucketName, {
          account_id: this.#accountId,
          jurisdiction: resource.jurisdiction,
          per_page: 1,
        }),
        'R2 object inventory',
      )) {
        if (object.key) {
          throw new Error(`R2 bucket '${resource.bucketName}' is not empty`);
        }
      }
    });
  }

  async deleteR2Bucket(
    resource: import('./types.js').ApplicationR2Binding,
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.withMutationFence(fence, () =>
      this.#schedule(async () => {
        await this.#client.r2.buckets.delete(resource.bucketName, {
          account_id: this.#accountId,
          jurisdiction: resource.jurisdiction,
        });
      }),
    );
  }

  async listOrdinaryWorkerSecretNames(
    scriptName: string,
  ): Promise<readonly string[]> {
    return listOrdinaryWorkerSecretNames(this.#ordinary, scriptName);
  }

  async #dispatchScripts(
    dispatchNamespace: string,
  ): Promise<readonly Readonly<{ id: string; tags: readonly string[] }>[]> {
    return listAllDispatchScripts(this.#attachmentScan, dispatchNamespace);
  }

  async listOrdinaryWorkerDatabases(
    filter?: Readonly<{ name?: string }>,
  ): Promise<readonly PlainWorkerDatabaseInventoryEntry[]> {
    return listOrdinaryWorkerDatabases(this.#ordinary, filter);
  }

  async ordinaryWorkerDeploymentStatus(
    scriptName: string,
  ): Promise<PlainWorkerDeploymentStatus | undefined> {
    return ordinaryWorkerDeploymentStatus(this.#ordinary, scriptName);
  }

  async listOrdinaryWorkerVersions(
    scriptName: string,
  ): Promise<readonly PlainWorkerVersionSummary[] | undefined> {
    return listOrdinaryWorkerVersions(this.#ordinary, scriptName);
  }

  async viewOrdinaryWorkerVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail> {
    return viewOrdinaryWorkerVersion(this.#ordinary, scriptName, versionId);
  }

  async findOrdinaryWorkerVersion(
    scriptName: string,
    versionId: string,
  ): Promise<PlainWorkerVersionDetail | undefined> {
    return findOrdinaryWorkerVersion(this.#ordinary, scriptName, versionId);
  }

  async prepareOrdinaryWorkerUpload(
    intent: PlainWorkerUploadIntent,
  ): Promise<PreparedOrdinaryWorkerUpload> {
    return prepareOrdinaryWorkerUpload(intent);
  }

  async dispatchOrdinaryWorkerUpload(
    prepared: PreparedOrdinaryWorkerUpload,
  ): Promise<void> {
    return dispatchOrdinaryWorkerUpload(this.#ordinary, prepared);
  }

  prepareOrdinaryWorkerDeployment(
    versions: readonly OrdinaryWorkerDeploymentVersion[],
  ): PreparedOrdinaryWorkerDeploymentVersions {
    return prepareOrdinaryWorkerDeployment(versions);
  }

  async dispatchOrdinaryWorkerDeployment(
    scriptName: string,
    versions: PreparedOrdinaryWorkerDeploymentVersions,
  ): Promise<void> {
    return dispatchOrdinaryWorkerDeployment(
      this.#ordinary,
      scriptName,
      versions,
    );
  }

  async deleteOrdinaryWorkerScript(
    scriptName: string,
  ): Promise<'deleted' | 'absent'> {
    return deleteOrdinaryWorkerScript(this.#ordinary, scriptName);
  }

  async findDatabase(name: string): Promise<DatabaseReference | undefined> {
    return this.#schedule(async () => {
      const matches: DatabaseReference[] = [];
      for await (const database of this.#collectBounded(
        this.#client.d1.database.list({
          account_id: this.#accountId,
          name,
        }),
        'D1 database inventory',
        MAX_DATABASE_INVENTORY,
      )) {
        if (database.name === name && database.uuid) {
          matches.push({ id: database.uuid, name, created: false });
        }
      }
      if (matches.length > 1) {
        throw new Error(`multiple D1 databases are named '${name}'`);
      }
      return matches[0];
    });
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    return this.#schedule(async () => {
      try {
        const database = await this.#client.d1.database.get(databaseId, {
          account_id: this.#accountId,
        });
        if (!database.uuid || !database.name) {
          throw new Error(
            `Cloudflare returned incomplete metadata for D1 database '${databaseId}'`,
          );
        }
        return {
          id: database.uuid,
          name: database.name,
          created: false,
        };
      } catch (error) {
        if (isNotFound(error)) return undefined;
        throw error;
      }
    });
  }

  async ensureDispatchNamespace(): Promise<void> {
    const dispatchNamespace = this.#requireDispatchNamespace(
      'ensureDispatchNamespace',
    );
    await this.#schedule(async () => {
      let found = false;
      for await (const namespace of this.#collectBounded(
        this.#client.workersForPlatforms.dispatch.namespaces.list({
          account_id: this.#accountId,
        }),
        'dispatch namespace inventory',
      )) {
        if (namespace.namespace_name === dispatchNamespace) {
          found = true;
          break;
        }
      }
      if (!found) {
        await this.#client.workersForPlatforms.dispatch.namespaces.create({
          account_id: this.#accountId,
          name: dispatchNamespace,
        });
      }
      await this.#assertUntrustedDispatchNamespace(dispatchNamespace);
    });
  }

  async putHostRouting(
    namespaceId: string,
    hostname: string,
    target: HostRoutingTarget,
    guard: PromotionGuard,
  ): Promise<void> {
    await this.#schedule(async () => {
      const existing = await this.#readHostRouting(namespaceId, hostname);
      if (existing === undefined) {
        if (!guard.allowUnrouted) {
          throw new Error(
            `host route '${hostname}' is unexpectedly absent during promotion`,
          );
        }
      } else {
        let owner: unknown;
        try {
          owner = JSON.parse(existing);
        } catch {
          throw new Error(`host route '${hostname}' has malformed ownership`);
        }
        if (
          !owner ||
          typeof owner !== 'object' ||
          !('scriptName' in owner) ||
          typeof owner.scriptName !== 'string' ||
          !guard.allowedCurrentScriptNames.includes(owner.scriptName) ||
          !('tenantTag' in owner) ||
          owner.tenantTag !== target.tenantTag ||
          !('environment' in owner) ||
          owner.environment !== target.environment
        ) {
          throw new Error(
            `host route '${hostname}' is already owned by another deployment`,
          );
        }
      }
      await this.#client.kv.namespaces.values.update(hostname.toLowerCase(), {
        account_id: this.#accountId,
        namespace_id: namespaceId,
        value: JSON.stringify(target),
      });
    });
  }

  async deleteHostRouting(
    namespaceId: string,
    hostname: string,
    allowedTargets: readonly HostRoutingTarget[],
  ): Promise<void> {
    await this.#schedule(async () => {
      const existing = await this.#readHostRouting(namespaceId, hostname);
      if (existing === undefined) return;
      try {
        JSON.parse(existing);
      } catch {
        throw new Error(
          `refusing to delete host route '${hostname}' with malformed ownership`,
        );
      }
      const allowed = allowedTargets.some(
        (target) => existing === JSON.stringify(target),
      );
      if (!allowed) {
        throw new Error(
          `refusing to delete host route '${hostname}' owned by another deployment`,
        );
      }
      await this.#client.kv.namespaces.values.delete(hostname.toLowerCase(), {
        account_id: this.#accountId,
        namespace_id: namespaceId,
      });
    });
  }

  async #readHostRouting(
    namespaceId: string,
    hostname: string,
  ): Promise<string | undefined> {
    try {
      const response = await this.#client.kv.namespaces.values.get(
        hostname.toLowerCase(),
        {
          account_id: this.#accountId,
          namespace_id: namespaceId,
        },
      );
      return await response.text();
    } catch (error) {
      if (isNotFound(error)) return undefined;
      throw error;
    }
  }

  async getHostRouting(
    namespaceId: string,
    hostname: string,
  ): Promise<string | undefined> {
    return this.#schedule(() => this.#readHostRouting(namespaceId, hostname));
  }

  async putScriptInventory(
    namespaceId: string,
    target: ScriptInventoryTarget,
  ): Promise<void> {
    const key = `${SCRIPT_INVENTORY_PREFIX}${target.scriptName}`;
    await this.#schedule(async () => {
      // KV visibility is eventual; the fenced fleet-state lease is the write
      // authority. This owner check detects settled conflicts, not a CAS.
      const existing = await this.#readHostRouting(namespaceId, key);
      if (existing !== undefined && existing !== JSON.stringify(target)) {
        throw new Error(
          `script inventory '${target.scriptName}' belongs to another deployment`,
        );
      }
      await this.#client.kv.namespaces.values.update(key, {
        account_id: this.#accountId,
        namespace_id: namespaceId,
        value: JSON.stringify(target),
      });
    });
  }

  async getScriptInventory(
    namespaceId: string,
    scriptName: string,
  ): Promise<ScriptInventoryTarget | undefined> {
    const key = `${SCRIPT_INVENTORY_PREFIX}${scriptName}`;
    return this.#schedule(async () => {
      const existing = await this.#readHostRouting(namespaceId, key);
      if (existing === undefined) return undefined;
      let candidate: unknown;
      try {
        candidate = JSON.parse(existing);
      } catch {
        throw new Error(`script inventory '${scriptName}' is malformed`);
      }
      if (
        !candidate ||
        typeof candidate !== 'object' ||
        !('scriptName' in candidate) ||
        candidate.scriptName !== scriptName ||
        !('tenantTag' in candidate) ||
        typeof candidate.tenantTag !== 'string' ||
        !('environment' in candidate) ||
        typeof candidate.environment !== 'string' ||
        !('databaseId' in candidate) ||
        typeof candidate.databaseId !== 'string' ||
        !('routeHostname' in candidate) ||
        typeof candidate.routeHostname !== 'string'
      ) {
        throw new Error(`script inventory '${scriptName}' is malformed`);
      }
      return {
        scriptName: candidate.scriptName,
        tenantTag: candidate.tenantTag,
        environment: candidate.environment,
        databaseId: candidate.databaseId,
        routeHostname: candidate.routeHostname,
      };
    });
  }

  async deleteScriptInventory(
    namespaceId: string,
    expected: ScriptInventoryTarget,
  ): Promise<void> {
    const key = `${SCRIPT_INVENTORY_PREFIX}${expected.scriptName}`;
    await this.#schedule(async () => {
      const existing = await this.#readHostRouting(namespaceId, key);
      if (existing === undefined) return;
      if (existing !== JSON.stringify(expected)) {
        throw new Error(
          `refusing to delete script inventory '${expected.scriptName}' owned by another deployment`,
        );
      }
      await this.#client.kv.namespaces.values.delete(key, {
        account_id: this.#accountId,
        namespace_id: namespaceId,
      });
    });
  }

  async collectFleetInventory(options: {
    readonly hostRoutingKvId?: string;
    readonly databaseNamePrefix: string;
    readonly scriptNamePrefix: string;
    readonly includeDispatchNamespace?: boolean;
    readonly includeR2Buckets?: boolean;
  }): Promise<FleetResourceInventory> {
    if (!options.databaseNamePrefix || !options.scriptNamePrefix) {
      throw new Error(
        'databaseNamePrefix and scriptNamePrefix are required for fleet inventory',
      );
    }
    const findings: FleetResourceInventory['findings'][number][] = [];
    const registrations: Array<
      ScriptInventoryTarget & { readonly keyOwned: boolean }
    > = [];
    const routes: FleetResourceInventory['routes'][number][] = [];
    if (options.hostRoutingKvId) {
      for await (const key of this.#collectBounded(
        this.#client.kv.namespaces.keys.list(options.hostRoutingKvId, {
          account_id: this.#accountId,
        }),
        'host-routing KV key inventory',
      )) {
        if (!key.name) continue;
        const isRegistration = key.name.startsWith(SCRIPT_INVENTORY_PREFIX);
        const registeredName = isRegistration
          ? key.name.slice(SCRIPT_INVENTORY_PREFIX.length)
          : undefined;
        const serialized = await this.#readHostRouting(
          options.hostRoutingKvId,
          key.name,
        );
        if (serialized === undefined) {
          findings.push({
            tenantTag: 'unknown',
            environment: 'unknown',
            kind: isRegistration ? 'stale-script-registration' : 'stale-route',
            detail: `fleet inventory key '${key.name}' disappeared while it was being read`,
          });
          continue;
        }
        let value: unknown;
        try {
          value = JSON.parse(serialized);
        } catch {
          findings.push({
            tenantTag: 'unknown',
            environment: 'unknown',
            kind: isRegistration
              ? 'malformed-script-registration'
              : 'malformed-route',
            detail: `fleet inventory key '${key.name}' is not valid JSON`,
          });
          continue;
        }
        if (!value || typeof value !== 'object') {
          findings.push({
            tenantTag: 'unknown',
            environment: 'unknown',
            kind: isRegistration
              ? 'malformed-script-registration'
              : 'malformed-route',
            detail: `fleet inventory key '${key.name}' is not an object`,
          });
          continue;
        }
        const candidate = value as Record<string, unknown>;
        if (isRegistration) {
          if (
            typeof candidate.scriptName !== 'string' ||
            typeof candidate.tenantTag !== 'string' ||
            typeof candidate.environment !== 'string' ||
            typeof candidate.databaseId !== 'string' ||
            typeof candidate.routeHostname !== 'string'
          ) {
            findings.push({
              tenantTag:
                typeof candidate.tenantTag === 'string'
                  ? candidate.tenantTag
                  : 'unknown',
              environment:
                typeof candidate.environment === 'string'
                  ? candidate.environment
                  : 'unknown',
              kind: 'malformed-script-registration',
              detail: `script inventory key '${key.name}' has incomplete ownership metadata`,
            });
            continue;
          }
          if (
            !candidate.scriptName.startsWith(options.scriptNamePrefix) &&
            !registeredName?.startsWith(options.scriptNamePrefix)
          ) {
            continue;
          }
          const keyOwned = registeredName === candidate.scriptName;
          registrations.push({
            scriptName: candidate.scriptName,
            tenantTag: candidate.tenantTag,
            environment: candidate.environment,
            databaseId: candidate.databaseId,
            routeHostname: candidate.routeHostname,
            keyOwned,
          });
          if (!keyOwned) {
            findings.push({
              tenantTag: candidate.tenantTag,
              environment: candidate.environment,
              kind: 'stale-script-registration',
              detail: `script inventory key '${key.name}' claims '${candidate.scriptName}'`,
            });
          }
          continue;
        }
        if (
          typeof candidate.scriptName !== 'string' ||
          typeof candidate.tenantTag !== 'string' ||
          typeof candidate.environment !== 'string' ||
          typeof candidate.policyId !== 'string' ||
          typeof candidate.policyDigest !== 'string' ||
          !Array.isArray(candidate.policyHosts) ||
          candidate.policyHosts.some((host) => typeof host !== 'string')
        ) {
          findings.push({
            tenantTag:
              typeof candidate.tenantTag === 'string'
                ? candidate.tenantTag
                : 'unknown',
            environment:
              typeof candidate.environment === 'string'
                ? candidate.environment
                : 'unknown',
            kind: 'malformed-route',
            detail: `host route '${key.name}' has incomplete ownership metadata`,
          });
          continue;
        }
        let policy: ReturnType<typeof canonicalDeploymentEgressPolicy>;
        try {
          policy = canonicalDeploymentEgressPolicy({
            policyId: candidate.policyId,
            tenantTag: candidate.tenantTag,
            environment: candidate.environment,
            allowedHosts: candidate.policyHosts as string[],
          });
        } catch {
          findings.push({
            tenantTag: candidate.tenantTag,
            environment: candidate.environment,
            kind: 'malformed-route',
            detail: `host route '${key.name}' has invalid policy metadata`,
          });
          continue;
        }
        if (
          candidate.policyDigest !== policy.policyDigest ||
          JSON.stringify(candidate.policyHosts) !==
            JSON.stringify(policy.policyHosts)
        ) {
          findings.push({
            tenantTag: candidate.tenantTag,
            environment: candidate.environment,
            kind: 'malformed-route',
            detail: `host route '${key.name}' has inconsistent policy metadata`,
          });
          continue;
        }
        let stateEgress: HostRoutingTarget['stateEgress'];
        try {
          stateEgress = (await parseHostRoutingTarget(serialized)).stateEgress;
        } catch {
          findings.push({
            tenantTag: candidate.tenantTag,
            environment: candidate.environment,
            kind: 'malformed-route',
            detail: `host route '${key.name}' has invalid state-egress metadata`,
          });
          continue;
        }
        if (!candidate.scriptName.startsWith(options.scriptNamePrefix))
          continue;
        routes.push({
          backend: 'workers-for-platforms',
          surface: 'host-registry',
          hostname: key.name,
          scriptName: candidate.scriptName,
          tenantTag: candidate.tenantTag,
          environment: candidate.environment,
          ...policy,
          ...(stateEgress ? { stateEgress } : {}),
        });
      }
    }

    const includeDispatchNamespace =
      options.includeDispatchNamespace ?? options.hostRoutingKvId !== undefined;
    const dispatchScripts = includeDispatchNamespace
      ? await this.#dispatchScripts(
          this.#requireDispatchNamespace('collectFleetInventory'),
        )
      : [];
    const dispatchScriptsByName = new Map(
      dispatchScripts.map((script) => [script.id, script]),
    );
    const deployments: FleetResourceInventory['deployments'][number][] = [];
    for (const registration of registrations) {
      const listed = dispatchScriptsByName.get(registration.scriptName);
      if (includeDispatchNamespace && !listed) {
        findings.push({
          tenantTag: registration.tenantTag,
          environment: registration.environment,
          kind: 'stale-script-registration',
          detail: `registered script '${registration.scriptName}' is absent from the dispatch namespace listing`,
        });
      }
      if (
        includeDispatchNamespace &&
        listed &&
        (!listed.tags.includes(FLEET_SCRIPT_TAG) ||
          tagValue(listed.tags, 'tenant:') !== registration.tenantTag ||
          tagValue(listed.tags, 'environment:') !== registration.environment)
      ) {
        findings.push({
          tenantTag: registration.tenantTag,
          environment: registration.environment,
          kind: 'stale-script-registration',
          detail: `registered script '${registration.scriptName}' does not match its live fleet tags`,
        });
      }
      let live: Awaited<ReturnType<typeof this.inspectDispatchWorker>>;
      try {
        live = await this.inspectDispatchWorker(registration.scriptName);
      } catch (error) {
        if (error instanceof CloudflarePlaneCapabilityError) throw error;
        findings.push({
          tenantTag: registration.tenantTag,
          environment: registration.environment,
          kind: 'stale-script-registration',
          detail: `registered script '${registration.scriptName}' could not be inspected: ${String(error)}`,
        });
        continue;
      }
      if (!live) {
        findings.push({
          tenantTag: registration.tenantTag,
          environment: registration.environment,
          kind: 'stale-script-registration',
          detail: `registered script '${registration.scriptName}' is missing`,
        });
        continue;
      }
      deployments.push({
        backend: 'workers-for-platforms',
        scriptName: registration.scriptName,
        tenantTag: live.tenantTag,
        environment: live.environment,
        databaseIds: live.databaseIds,
        durableObjectBindings: live.durableObjectBindings,
        serviceBindings: live.serviceBindings,
        queueProducerBindings: live.queueProducerBindings,
        r2BucketBindings: live.r2BucketBindings,
        plainTextBindings: live.plainTextBindings,
        secretNames: live.secretNames,
        routeHostnames: routes
          .filter(
            (route) =>
              route.backend === 'workers-for-platforms' &&
              route.scriptName === registration.scriptName,
          )
          .map((route) => route.hostname),
        artifactVersion: live.artifactVersion,
        desiredSpecDigest: live.desiredSpecDigest,
        schemaVersion: live.schemaVersion,
      });
      const ownerMatches =
        registration.keyOwned &&
        live.tenantTag === registration.tenantTag &&
        live.environment === registration.environment &&
        live.databaseIds.length === 1 &&
        live.databaseIds[0] === registration.databaseId;
      if (!ownerMatches && registration.keyOwned) {
        findings.push({
          tenantTag: registration.tenantTag,
          environment: registration.environment,
          kind: 'stale-script-registration',
          detail: `registered script '${registration.scriptName}' does not match its live tenant, environment, or database ownership`,
        });
      }
    }
    const registrationByScript = new Map(
      registrations.map((registration) => [
        registration.scriptName,
        registration,
      ]),
    );
    for (const script of dispatchScripts) {
      const registration = registrationByScript.get(script.id);
      if (registration?.keyOwned) continue;
      findings.push({
        tenantTag: tagValue(script.tags, 'tenant:') ?? 'unknown',
        environment: tagValue(script.tags, 'environment:') ?? 'unknown',
        kind: 'unknown-dispatch-scripts',
        detail: `dispatch script '${script.id}' has no valid owner-checked registry entry`,
      });
    }
    for (const route of routes) {
      if (route.backend !== 'workers-for-platforms') continue;
      const registration = registrationByScript.get(route.scriptName);
      if (
        !registration?.keyOwned ||
        registration.tenantTag !== route.tenantTag ||
        registration.environment !== route.environment ||
        registration.routeHostname !== route.hostname
      ) {
        findings.push({
          tenantTag: route.tenantTag,
          environment: route.environment,
          kind: 'stale-route',
          detail: `host route '${route.hostname}' does not match its script registration owner`,
        });
      }
    }

    let dispatchScriptCount: number | undefined;
    let dispatchNamespaceInventory: FleetResourceInventory['dispatchNamespace'];
    if (includeDispatchNamespace) {
      const dispatchNamespace = this.#requireDispatchNamespace(
        'collectFleetInventory',
      );
      const namespaceInventory =
        await this.#client.workersForPlatforms.dispatch.namespaces.get(
          dispatchNamespace,
          { account_id: this.#accountId },
        );
      dispatchScriptCount = namespaceInventory.script_count;
      if (
        typeof dispatchScriptCount !== 'number' ||
        !Number.isSafeInteger(dispatchScriptCount) ||
        dispatchScriptCount < 0
      ) {
        throw new Error(
          `dispatch namespace '${dispatchNamespace}' returned no valid script_count`,
        );
      }
      dispatchNamespaceInventory = {
        name: namespaceInventory.namespace_name ?? dispatchNamespace,
        ...(namespaceInventory.namespace_id
          ? { namespaceId: namespaceInventory.namespace_id }
          : {}),
        trustedWorkers: namespaceInventory.trusted_workers,
        scriptCount: dispatchScriptCount,
      };
      if (
        namespaceInventory.namespace_name !== dispatchNamespace ||
        namespaceInventory.trusted_workers !== false
      ) {
        findings.push({
          tenantTag: 'unknown',
          environment: 'unknown',
          kind: 'trusted-dispatch-namespace',
          detail: `dispatch namespace '${dispatchNamespace}' does not attest trusted_workers=false`,
        });
      }
      if (dispatchScriptCount > dispatchScripts.length) {
        findings.push({
          tenantTag: 'unknown',
          environment: 'unknown',
          kind: 'unknown-dispatch-scripts',
          detail: `dispatch namespace '${dispatchNamespace}' reports ${dispatchScriptCount - dispatchScripts.length} script(s) missing from the paginated listing`,
        });
      }
    }

    const customDomains = [];
    for await (const domain of this.#collectBounded(
      this.#client.workers.domains.list({ account_id: this.#accountId }),
      'custom domain inventory',
    )) {
      if (domain.service.startsWith(options.scriptNamePrefix)) {
        customDomains.push(domain);
      }
    }
    const zoneRoutes: Array<
      import('./types.js').WorkerZoneRoute & { readonly scriptName: string }
    > = [];
    const workerRouteZoneIds = await this.#workerRouteZoneIds();
    for (const zoneId of workerRouteZoneIds) {
      for await (const route of this.#collectBounded(
        this.#client.workers.routes.list({ zone_id: zoneId }),
        'Worker zone-route inventory',
      )) {
        if (
          route.script?.startsWith(options.scriptNamePrefix) &&
          route.id &&
          route.pattern
        ) {
          zoneRoutes.push({
            zoneId,
            routeId: route.id,
            pattern: route.pattern,
            scriptName: route.script,
          });
        }
      }
    }
    const plainIdentities = new Map<
      string,
      { readonly tenantTag: string; readonly environment: string }
    >();
    for await (const script of this.#collectBounded(
      this.#client.workers.scripts.list({ account_id: this.#accountId }),
      'ordinary Worker script inventory',
    )) {
      const scriptName = script.id;
      if (!scriptName?.startsWith(options.scriptNamePrefix)) continue;
      try {
        const deploymentList =
          await this.#client.workers.scripts.deployments.list(scriptName, {
            account_id: this.#accountId,
          });
        const activeDeployment = deploymentList.deployments[0];
        const artifactVersion = exactActiveVersionId(
          activeDeployment,
          `ordinary Worker '${scriptName}'`,
        );
        const [activeVersion, subdomain, secretNames] = await Promise.all([
          this.#client.workers.scripts.versions.get(artifactVersion, {
            account_id: this.#accountId,
            script_name: scriptName,
          }),
          this.#client.workers.scripts.subdomain.get(scriptName, {
            account_id: this.#accountId,
          }),
          ordinaryWorkerSecretNames(this.#ordinary, scriptName),
        ]);
        const bindings = activeVersion.resources.bindings ?? [];
        const databaseIds = bindings.flatMap((binding) =>
          binding.type === 'd1' && binding.database_id
            ? [binding.database_id]
            : [],
        );
        const durableObjectBindings = bindings.flatMap((binding) => {
          if (
            binding.type !== 'durable_object_namespace' ||
            !binding.namespace_id ||
            !binding.name ||
            !binding.class_name
          ) {
            return [];
          }
          return [
            {
              name: binding.name,
              className: binding.class_name,
              namespaceId: binding.namespace_id,
              ...(binding.script_name
                ? { scriptName: binding.script_name }
                : {}),
              ...(binding.dispatch_namespace
                ? { dispatchNamespace: binding.dispatch_namespace }
                : {}),
            },
          ];
        });
        const serviceBindings = bindings.flatMap((binding) =>
          binding.type === 'service' && binding.name && binding.service
            ? [
                {
                  name: binding.name,
                  service: binding.service,
                  ...(binding.entrypoint
                    ? { entrypoint: binding.entrypoint }
                    : {}),
                },
              ]
            : [],
        );
        const queueProducerBindings = bindings.flatMap((binding) =>
          binding.type === 'queue' && binding.name && binding.queue_name
            ? [{ name: binding.name, queueName: binding.queue_name }]
            : [],
        );
        const kvNamespaceBindings = bindings.flatMap((binding) =>
          binding.type === 'kv_namespace' &&
          binding.name &&
          binding.namespace_id
            ? [{ name: binding.name, namespaceId: binding.namespace_id }]
            : [],
        );
        const r2BucketBindings = bindings.flatMap((binding) =>
          binding.type === 'r2_bucket' && binding.name && binding.bucket_name
            ? [
                {
                  name: binding.name,
                  bucketName: binding.bucket_name,
                  jurisdiction: 'default' as const,
                },
              ]
            : [],
        );
        const plainText = new Map(
          bindings.flatMap((binding) =>
            binding.type === 'plain_text'
              ? [[binding.name, binding.text] as const]
              : [],
          ),
        );
        assertSupportedProviderBindings(
          bindings,
          new Set([
            'd1',
            'durable_object_namespace',
            'service',
            'queue',
            'kv_namespace',
            'dispatch_namespace',
            'r2_bucket',
            'plain_text',
            'secret_text',
          ]),
          `plain Worker '${scriptName}'`,
        );
        const tenantTag = plainText.get('DEPLOYMENT_TENANT');
        const environment = plainText.get('FLEET_ENVIRONMENT');
        const resourceRole = plainText.get('FLEET_RESOURCE_ROLE');
        const resourceGroupId = plainText.get('FLEET_RESOURCE_GROUP');
        const schemaVersion = Number(plainText.get('FLEET_SCHEMA_VERSION'));
        const scriptZoneRoutes = zoneRoutes.filter(
          (route) => route.scriptName === scriptName,
        );
        if (
          !tenantTag ||
          !environment ||
          !Number.isSafeInteger(schemaVersion)
        ) {
          throw new Error('active Worker identity settings are missing');
        }
        if (
          (resourceRole === 'platform-state' ||
            resourceRole === 'deployment-egress') &&
          (subdomain.enabled ||
            subdomain.previews_enabled ||
            scriptZoneRoutes.length > 0)
        ) {
          findings.push({
            tenantTag,
            environment,
            kind: 'incomplete-deployment',
            detail: `trusted Worker '${scriptName}' is publicly reachable on workers.dev, a preview URL, or a zone route`,
          });
        }
        plainIdentities.set(scriptName, { tenantTag, environment });
        deployments.push({
          backend: 'plain-worker',
          ...(resourceRole === 'platform-state' ||
          resourceRole === 'deployment-egress'
            ? { resourceRole, resourceGroupId }
            : {}),
          scriptName,
          tenantTag,
          environment,
          databaseIds,
          durableObjectBindings,
          serviceBindings,
          queueProducerBindings,
          kvNamespaceBindings,
          r2BucketBindings,
          secretNames,
          plainTextBindings: Object.fromEntries(plainText),
          routeHostnames: customDomains
            .filter((domain) => domain.service === scriptName)
            .map((domain) => domain.hostname),
          zoneRoutes: scriptZoneRoutes.map(
            ({ scriptName: _scriptName, ...route }) => route,
          ),
          artifactVersion,
          ...(plainText.get('FLEET_SPEC_DIGEST')
            ? { desiredSpecDigest: plainText.get('FLEET_SPEC_DIGEST') }
            : {}),
          schemaVersion,
        });
      } catch (error) {
        findings.push({
          tenantTag: 'unknown',
          environment: 'unknown',
          kind: 'incomplete-deployment',
          detail: `plain Worker '${scriptName}' could not be inventoried: ${String(error)}`,
        });
      }
    }
    for (const domain of customDomains) {
      const identity = plainIdentities.get(domain.service);
      routes.push({
        backend: 'plain-worker',
        surface: 'custom-domain',
        hostname: domain.hostname,
        scriptName: domain.service,
        tenantTag: identity?.tenantTag ?? 'unknown',
        environment: identity?.environment ?? 'unknown',
      });
      if (!identity) {
        findings.push({
          tenantTag: 'unknown',
          environment: 'unknown',
          kind: 'stale-route',
          detail: `custom domain '${domain.hostname}' points to a missing or incomplete plain Worker '${domain.service}'`,
        });
      }
    }
    for (const route of zoneRoutes) {
      const identity = plainIdentities.get(route.scriptName);
      routes.push({
        backend: 'plain-worker',
        surface: 'zone-route',
        zoneId: route.zoneId,
        routeId: route.routeId,
        hostname: route.pattern,
        scriptName: route.scriptName,
        tenantTag: identity?.tenantTag ?? 'unknown',
        environment: identity?.environment ?? 'unknown',
      });
      findings.push({
        tenantTag: identity?.tenantTag ?? 'unknown',
        environment: identity?.environment ?? 'unknown',
        kind: 'stale-route',
        detail: `zone route '${route.pattern}' exposes plain Worker '${route.scriptName}'`,
      });
    }

    const databaseIds: string[] = [];
    for await (const database of this.#collectBounded(
      this.#client.d1.database.list({ account_id: this.#accountId }),
      'D1 database inventory',
      MAX_DATABASE_INVENTORY,
    )) {
      if (
        database.uuid &&
        database.name?.startsWith(options.databaseNamePrefix)
      ) {
        databaseIds.push(database.uuid);
      }
    }
    const namespaceIds: string[] = [];
    const registeredScriptNames = new Set(
      registrations.map((registration) => registration.scriptName),
    );
    for await (const namespace of this.#collectBounded(
      this.#client.durableObjects.namespaces.list({
        account_id: this.#accountId,
      }),
      'Durable Object namespace inventory',
    )) {
      if (
        namespace.id &&
        namespace.script &&
        (registeredScriptNames.has(namespace.script) ||
          namespace.script.startsWith(options.scriptNamePrefix))
      ) {
        namespaceIds.push(namespace.id);
      }
    }
    const r2Buckets: Array<
      NonNullable<FleetResourceInventory['r2Buckets']>[number]
    > = [];
    for (const jurisdiction of options.includeR2Buckets
      ? (['default', 'eu', 'fedramp'] as const)
      : []) {
      let startAfter: string | undefined;
      for (;;) {
        const page = await this.#client.r2.buckets.list({
          account_id: this.#accountId,
          jurisdiction,
          name_contains: options.scriptNamePrefix,
          order: 'name',
          direction: 'asc',
          per_page: 1000,
          ...(startAfter ? { start_after: startAfter } : {}),
        });
        const buckets = page.buckets ?? [];
        for (const bucket of buckets) {
          if (!bucket.name?.startsWith(options.scriptNamePrefix)) continue;
          if (
            bucket.jurisdiction !== undefined &&
            bucket.jurisdiction !== jurisdiction
          ) {
            throw new Error(`R2 bucket '${bucket.name}' changed jurisdiction`);
          }
          if (
            !bucket.creation_date ||
            !Number.isFinite(Date.parse(bucket.creation_date))
          ) {
            throw new Error(
              `R2 bucket '${bucket.name}' has no valid creation date`,
            );
          }
          if (r2Buckets.length >= CLOUDFLARE_INVENTORY_BOUND) {
            // The bound counts only accepted fleet-owned buckets, not every
            // provider item scanned while filtering by prefix.
            throw inventoryBoundExceeded(
              'R2 bucket inventory',
              CLOUDFLARE_INVENTORY_BOUND,
            );
          }
          r2Buckets.push({
            bucketName: bucket.name,
            jurisdiction,
            creationDate: new Date(bucket.creation_date).toISOString(),
          });
        }
        if (buckets.length < 1000) break;
        const last = buckets.at(-1)?.name;
        if (!last || last === startAfter) {
          throw new Error('R2 bucket inventory pagination did not advance');
        }
        startAfter = last;
      }
    }
    return {
      findings,
      ...(options.hostRoutingKvId
        ? { hostRoutingKvId: options.hostRoutingKvId }
        : {}),
      dispatchScriptCount,
      ...(dispatchNamespaceInventory
        ? { dispatchNamespace: dispatchNamespaceInventory }
        : {}),
      scriptRegistrations: registrations.map(
        ({ keyOwned: _keyOwned, ...registration }) => registration,
      ),
      deployments,
      databaseIds,
      namespaceIds,
      r2Buckets,
      routes,
    };
  }

  async hasDurableObjectNamespace(namespaceId: string): Promise<boolean> {
    if (!namespaceId) throw new Error('namespaceId is required');
    for await (const namespace of this.#collectBounded(
      this.#client.durableObjects.namespaces.list({
        account_id: this.#accountId,
      }),
      'Durable Object namespace inventory',
    )) {
      if (namespace.id === namespaceId) return true;
    }
    return false;
  }

  async listDurableObjectNamespaces(
    scriptName: string,
  ): Promise<readonly string[]> {
    if (!scriptName) throw new Error('scriptName is required');
    const namespaceIds: string[] = [];
    for await (const namespace of this.#collectBounded(
      this.#client.durableObjects.namespaces.list({
        account_id: this.#accountId,
      }),
      'Durable Object namespace inventory',
    )) {
      if (namespace.script === scriptName && namespace.id) {
        namespaceIds.push(namespace.id);
      }
    }
    return [...new Set(namespaceIds)].sort();
  }

  async uploadControlWorker(spec: ControlWorkerSpec): Promise<string> {
    const files = await Promise.all(
      spec.modules.map((module) =>
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
    return this.#schedule(async () => {
      const result = await this.#client.workers.scripts.update(
        spec.scriptName,
        {
          account_id: this.#accountId,
          files,
          metadata: {
            bindings: spec.bindings as never,
            compatibility_date: spec.compatibilityDate,
            compatibility_flags: spec.compatibilityFlags
              ? [...spec.compatibilityFlags]
              : undefined,
            keep_bindings: ['secret_text'],
            main_module: spec.mainModule,
            migrations: spec.migrations as never,
            tags: spec.tags ? [...spec.tags] : undefined,
          },
        },
      );
      if (!result.etag) {
        throw new Error(`control Worker '${spec.scriptName}' returned no etag`);
      }
      return result.etag;
    });
  }

  async putControlSecrets(
    scriptName: string,
    secrets: Readonly<Record<string, string>>,
  ): Promise<void> {
    await this.#schedule(async () => {
      const currentSecretNames: string[] = [];
      for await (const secret of this.#collectBounded(
        this.#client.workers.scripts.secrets.list(scriptName, {
          account_id: this.#accountId,
        }),
        'ordinary Worker secret inventory',
      )) {
        if (!secret.name) {
          throw new Error(
            `control Worker '${scriptName}' returned a secret without a name`,
          );
        }
        currentSecretNames.push(secret.name);
      }
      const desiredSecretNames = Object.keys(secrets).sort();
      const secretUpdates = [
        ...Object.entries(secrets).map(
          ([name, value]) =>
            [
              name,
              { name, text: value, type: 'secret_text' as const },
            ] as const,
        ),
        ...currentSecretNames
          .filter((name) => !(name in secrets))
          .map((name) => [name, null] as const),
      ];
      if (secretUpdates.length > 0) {
        await this.#client.workers.scripts.secrets.bulkUpdate(scriptName, {
          account_id: this.#accountId,
          secrets: Object.fromEntries(secretUpdates),
        });
      }
      const secretNames: string[] = [];
      for await (const secret of this.#collectBounded(
        this.#client.workers.scripts.secrets.list(scriptName, {
          account_id: this.#accountId,
        }),
        'ordinary Worker secret inventory',
      )) {
        if (secret.name) secretNames.push(secret.name);
      }
      secretNames.sort();
      if (JSON.stringify(secretNames) !== JSON.stringify(desiredSecretNames)) {
        throw new Error(
          `control Worker '${scriptName}' failed exact secret convergence`,
        );
      }
    });
  }

  async deleteControlSecrets(
    scriptName: string,
    secretNames: readonly string[],
    fence: ExternalMutationFence,
  ): Promise<void> {
    await this.withMutationFence(fence, async () => {
      for (const name of [...new Set(secretNames)].sort()) {
        try {
          await this.#client.workers.scripts.secrets.delete(name, {
            account_id: this.#accountId,
            script_name: scriptName,
          });
        } catch (error) {
          if (!isNotFound(error)) throw error;
        }
      }
    });
  }

  async inspectControlWorker(
    scriptName: string,
  ): Promise<ControlWorkerInspection | undefined> {
    return this.#schedule(async () => {
      try {
        const deploymentList =
          await this.#client.workers.scripts.deployments.list(scriptName, {
            account_id: this.#accountId,
          });
        const activeDeployment = deploymentList.deployments[0];
        const artifactVersion = exactActiveVersionId(
          activeDeployment,
          `control Worker '${scriptName}'`,
        );
        const [activeVersion, subdomain, secretNames] = await Promise.all([
          this.#client.workers.scripts.versions.get(artifactVersion, {
            account_id: this.#accountId,
            script_name: scriptName,
          }),
          this.#client.workers.scripts.subdomain.get(scriptName, {
            account_id: this.#accountId,
          }),
          ordinaryWorkerSecretNames(this.#ordinary, scriptName),
        ]);
        const bindings = activeVersion.resources.bindings ?? [];
        const databaseIds = bindings.flatMap((binding) =>
          binding.type === 'd1' && binding.database_id
            ? [binding.database_id]
            : [],
        );
        const durableObjectBindings = bindings.flatMap((binding) => {
          if (
            binding.type !== 'durable_object_namespace' ||
            !binding.namespace_id ||
            !binding.name ||
            !binding.class_name
          ) {
            return [];
          }
          return [
            {
              name: binding.name,
              className: binding.class_name,
              namespaceId: binding.namespace_id,
              ...(binding.script_name
                ? { scriptName: binding.script_name }
                : {}),
              ...(binding.dispatch_namespace
                ? { dispatchNamespace: binding.dispatch_namespace }
                : {}),
            },
          ];
        });
        const serviceBindings = bindings.flatMap((binding) =>
          binding.type === 'service' && binding.name && binding.service
            ? [
                {
                  name: binding.name,
                  service: binding.service,
                  ...(binding.entrypoint
                    ? { entrypoint: String(binding.entrypoint) }
                    : {}),
                },
              ]
            : [],
        );
        const queueProducerBindings = bindings.flatMap((binding) =>
          binding.type === 'queue' && binding.name && binding.queue_name
            ? [{ name: binding.name, queueName: binding.queue_name }]
            : [],
        );
        const kvNamespaceBindings = bindings.flatMap((binding) =>
          binding.type === 'kv_namespace' &&
          binding.name &&
          binding.namespace_id
            ? [{ name: binding.name, namespaceId: binding.namespace_id }]
            : [],
        );
        const dispatchNamespaceBindings = bindings.flatMap((binding) => {
          const candidate = binding as unknown as Record<string, unknown>;
          return candidate.type === 'dispatch_namespace' &&
            typeof candidate.name === 'string' &&
            typeof candidate.namespace === 'string'
            ? [
                {
                  name: candidate.name,
                  namespace: candidate.namespace,
                  outbound: candidate.outbound,
                },
              ]
            : [];
        });
        const r2BucketBindings = bindings
          .flatMap((binding) =>
            binding.type === 'r2_bucket' && binding.name && binding.bucket_name
              ? [
                  {
                    name: binding.name,
                    bucketName: binding.bucket_name,
                    jurisdiction: 'default' as const,
                  },
                ]
              : [],
          )
          .sort((left, right) => left.name.localeCompare(right.name));
        const plainTextBindings = Object.fromEntries(
          bindings.flatMap((binding) =>
            binding.type === 'plain_text' && binding.name
              ? [[binding.name, String(binding.text ?? '')] as const]
              : [],
          ),
        );
        const providerBindingIdentities = assertSupportedProviderBindings(
          bindings,
          new Set([
            'd1',
            'durable_object_namespace',
            'service',
            'queue',
            'kv_namespace',
            'dispatch_namespace',
            'r2_bucket',
            'plain_text',
            'secret_text',
          ]),
          `control Worker '${scriptName}'`,
        );
        const routeHostnames: string[] = [];
        for await (const domain of this.#collectBounded(
          this.#client.workers.domains.list({ account_id: this.#accountId }),
          'custom domain inventory',
        )) {
          if (domain.service === scriptName)
            routeHostnames.push(domain.hostname);
        }
        const zoneRoutes: import('./types.js').WorkerZoneRoute[] = [];
        const workerRouteZoneIds = await this.#workerRouteZoneIds();
        for (const zoneId of workerRouteZoneIds) {
          for await (const route of this.#collectBounded(
            this.#client.workers.routes.list({ zone_id: zoneId }),
            'Worker zone-route inventory',
          )) {
            if (route.script !== scriptName) continue;
            zoneRoutes.push({
              zoneId,
              routeId: route.id,
              pattern: route.pattern,
            });
          }
        }
        const inspection = {
          artifactVersion,
          databaseIds,
          durableObjectBindings,
          kvNamespaceBindings,
          dispatchNamespaceBindings,
          queueProducerBindings,
          serviceBindings,
          r2BucketBindings,
          secretNames,
          plainTextBindings,
          providerBindingIdentities,
          workersDevEnabled: subdomain.enabled === true,
          previewUrlsEnabled: subdomain.previews_enabled === true,
          routeHostnames,
          zoneRoutes,
        };
        assertProviderBindingIdentitiesMatchInspection(
          inspection,
          `control Worker '${scriptName}'`,
        );
        return inspection;
      } catch (error) {
        if (isNotFound(error)) return undefined;
        throw error;
      }
    });
  }

  async revokeControlSecrets(scriptName: string): Promise<void> {
    await this.#schedule(async () => {
      const listNames = async (): Promise<string[]> => {
        try {
          const names: string[] = [];
          for await (const secret of this.#collectBounded(
            this.#client.workers.scripts.secrets.list(scriptName, {
              account_id: this.#accountId,
            }),
            'ordinary Worker secret inventory',
          )) {
            if (!secret.name) {
              throw new Error(
                `control Worker '${scriptName}' returned a secret without a name`,
              );
            }
            names.push(secret.name);
          }
          return names.sort();
        } catch (error) {
          if (isNotFound(error)) return [];
          throw error;
        }
      };
      const current = await listNames();
      if (current.length > 0) {
        try {
          await this.#client.workers.scripts.secrets.bulkUpdate(scriptName, {
            account_id: this.#accountId,
            secrets: Object.fromEntries(
              current.map((name) => [name, null] as const),
            ),
          });
        } catch (error) {
          if (!isNotFound(error)) throw error;
        }
      }
      if ((await listNames()).length !== 0) {
        throw new Error(
          `control Worker '${scriptName}' failed exact secret revocation`,
        );
      }
    });
  }

  async disableControlWorkerPublicAccess(scriptName: string): Promise<void> {
    await this.#schedule(async () => {
      const workerRouteZoneIds = await this.#workerRouteZoneIds();
      try {
        await this.#client.workers.scripts.subdomain.create(scriptName, {
          account_id: this.#accountId,
          enabled: false,
          previews_enabled: false,
        });
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
      for await (const domain of this.#collectBounded(
        this.#client.workers.domains.list({ account_id: this.#accountId }),
        'custom domain inventory',
      )) {
        if (domain.service !== scriptName || !domain.id) continue;
        try {
          await this.#client.workers.domains.delete(domain.id, {
            account_id: this.#accountId,
          });
        } catch (error) {
          if (!isNotFound(error)) throw error;
        }
      }
      for (const zoneId of workerRouteZoneIds) {
        for await (const route of this.#collectBounded(
          this.#client.workers.routes.list({ zone_id: zoneId }),
          'Worker zone-route inventory',
        )) {
          if (route.script !== scriptName) continue;
          try {
            await this.#client.workers.routes.delete(route.id, {
              zone_id: zoneId,
            });
          } catch (error) {
            if (!isNotFound(error)) throw error;
          }
        }
      }
    });
  }

  async disableOrdinaryWorkerPublicAccess(
    scriptName: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    return disableOrdinaryWorkerPublicAccess(this.#ordinary, scriptName, fence);
  }

  async listCustomDomains(): Promise<
    readonly OrdinaryWorkerFootprint['customDomains'][number][]
  > {
    return listCustomDomains(this.#ordinary);
  }

  attachCustomDomain(
    target: { readonly hostname: string; readonly service: string },
    fence: ExternalMutationFence,
  ): Promise<void> {
    return attachCustomDomain(this.#ordinary, target, fence);
  }

  detachCustomDomain(
    domainId: string,
    fence: ExternalMutationFence,
  ): Promise<void> {
    return detachCustomDomain(this.#ordinary, domainId, fence);
  }

  async inspectActiveWorkerRoute(scriptName: string): Promise<
    | Readonly<{
        artifactVersion: string;
        specDigest: string | undefined;
      }>
    | undefined
  > {
    return inspectActiveWorkerRoute(this.#ordinary, scriptName);
  }

  async inspectOrdinaryWorkerFootprint(
    scriptName: string,
  ): Promise<OrdinaryWorkerFootprint> {
    return inspectOrdinaryWorkerFootprint(this.#ordinary, scriptName);
  }

  async deleteControlWorker(scriptName: string): Promise<void> {
    await this.#schedule(async () => {
      try {
        await this.#client.workers.scripts.delete(scriptName, {
          account_id: this.#accountId,
        });
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
    });
  }

  async ensureQueueConsumer(options: {
    readonly queueName: string;
    readonly scriptName: string;
    readonly deadLetterQueue?: string;
  }): Promise<void> {
    await this.#schedule(async () => {
      const matches = [];
      for await (const queue of this.#collectBounded(
        this.#client.queues.list({ account_id: this.#accountId }),
        'queue inventory',
      )) {
        if (queue.queue_name === options.queueName && queue.queue_id) {
          matches.push(queue);
        }
      }
      if (matches.length !== 1) {
        throw new Error(
          `expected exactly one audit queue named '${options.queueName}', found ${matches.length}`,
        );
      }
      const queueId = matches[0]?.queue_id;
      if (!queueId) throw new Error('audit queue result has no queue_id');
      const consumers = [];
      for await (const consumer of this.#collectBounded(
        this.#client.queues.consumers.list(queueId, {
          account_id: this.#accountId,
        }),
        'queue consumer inventory',
      )) {
        consumers.push(consumer);
      }
      if (consumers.length > 1) {
        throw new Error(
          `expected at most one consumer for audit queue '${options.queueName}', found ${consumers.length}`,
        );
      }
      const existing = consumers[0];
      let consumerId: string;
      if (!existing) {
        const created = await this.#client.queues.consumers.create(queueId, {
          account_id: this.#accountId,
          type: 'worker',
          script_name: options.scriptName,
          dead_letter_queue: options.deadLetterQueue,
          settings: AUDIT_CONSUMER_SETTINGS,
        });
        if (!created.consumer_id) {
          throw new Error('created audit queue consumer has no consumer_id');
        }
        consumerId = created.consumer_id;
      } else {
        if (!existing.consumer_id) {
          throw new Error('audit queue consumer has no consumer_id');
        }
        consumerId = existing.consumer_id;
        if (!queueConsumerMatches(existing, options)) {
          await this.#client.queues.consumers.update(consumerId, {
            account_id: this.#accountId,
            queue_id: queueId,
            type: 'worker',
            script_name: options.scriptName,
            dead_letter_queue: options.deadLetterQueue,
            settings: AUDIT_CONSUMER_SETTINGS,
          });
        }
      }
      const attested = await this.#client.queues.consumers.get(consumerId, {
        account_id: this.#accountId,
        queue_id: queueId,
      });
      if (!queueConsumerMatches(attested, options)) {
        throw new Error(
          `audit queue consumer '${consumerId}' does not match the requested configuration`,
        );
      }
      const finalConsumers = [];
      for await (const consumer of this.#collectBounded(
        this.#client.queues.consumers.list(queueId, {
          account_id: this.#accountId,
        }),
        'queue consumer inventory',
      )) {
        finalConsumers.push(consumer);
      }
      if (
        finalConsumers.length !== 1 ||
        finalConsumers[0]?.consumer_id !== consumerId ||
        !queueConsumerMatches(finalConsumers[0], options)
      ) {
        throw new Error(
          `audit queue '${options.queueName}' does not have exactly one attested consumer`,
        );
      }
    });
  }

  async createDatabase(name: string): Promise<DatabaseReference> {
    return this.#schedule(async () => {
      const database = await this.#client.d1.database.create(
        {
          account_id: this.#accountId,
          name,
        },
        { maxRetries: 0 },
      );
      if (!database.uuid || database.name !== name) {
        throw new Error(
          `Cloudflare returned an invalid D1 create result for '${name}'`,
        );
      }
      return { id: database.uuid, name, created: true };
    });
  }

  async queryDatabase(
    databaseId: string,
    sql: string,
    bindings: readonly string[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    const params = d1RestParameters(bindings, 'D1 query');
    return this.#schedule(async () => {
      const rows: Readonly<Record<string, unknown>>[] = [];
      for await (const result of this.#collectBounded(
        this.#client.d1.database.query(
          databaseId,
          {
            account_id: this.#accountId,
            sql,
            params,
          },
          { maxRetries: 0 },
        ),
        'D1 query result inventory',
      )) {
        if (result.success === false) {
          throw new Error(`D1 query failed for database '${databaseId}'`);
        }
        for (const row of result.results ?? []) {
          if (row && typeof row === 'object') {
            rows.push(row as Readonly<Record<string, unknown>>);
          }
        }
      }
      return rows;
    });
  }

  async batchDatabase(
    databaseId: string,
    statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly string[];
    }[],
  ): Promise<void> {
    const batch = statements.map((statement) => ({
      sql: statement.sql,
      params: d1RestParameters(statement.bindings ?? [], 'D1 batch'),
    }));
    await this.#schedule(async () => {
      for await (const result of this.#collectBounded(
        this.#client.d1.database.query(
          databaseId,
          {
            account_id: this.#accountId,
            batch,
          },
          { maxRetries: 0 },
        ),
        'D1 batch result inventory',
      )) {
        if (result.success === false) {
          throw new Error(`D1 batch failed for database '${databaseId}'`);
        }
      }
    });
  }

  async uploadDispatchWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    physicalScriptName = spec.scriptName,
    platformResources?: import('./types.js').ExternalPlatformResources,
    application?: import('./types.js').ApplicationBindingTopology,
  ): Promise<{ artifactVersion: string }> {
    const dispatchNamespace = this.#requireDispatchNamespace(
      'uploadDispatchWorker',
    );
    if (spec.authoredBy === 'external' && !platformResources) {
      throw new Error('external dispatch upload requires platform resources');
    }
    const bindings: Array<Record<string, unknown>> = [
      { name: 'DB', type: 'd1', database_id: database.id },
      { name: 'DEPLOYMENT_TENANT', type: 'plain_text', text: spec.tenantTag },
      { name: 'FLEET_ENVIRONMENT', type: 'plain_text', text: spec.environment },
      {
        name: 'FLEET_SCHEMA_VERSION',
        type: 'plain_text',
        text: String(spec.schemaVersion),
      },
      {
        name: 'FLEET_SPEC_DIGEST',
        type: 'plain_text',
        text: deploymentSpecDigest(spec),
      },
      ...(spec.authoredBy === 'external'
        ? [
            {
              name: 'FLEET_MAINTENANCE_CAPABILITIES',
              type: 'plain_text',
              text: 'required',
            },
          ]
        : []),
      ...(spec.authoredBy === 'external' && spec.queueProducer
        ? [
            {
              name: 'FLEET_AUDIT_PROXY',
              type: 'plain_text',
              text: 'required',
            },
          ]
        : []),
      ...spec.durableObjectBindings.map((binding) => ({
        name: binding.name,
        type: 'durable_object_namespace',
        class_name: binding.className,
        ...(platformResources
          ? {
              script_name: platformResources.stateWorker.scriptName,
              ...(platformResources.stateWorker.dispatchNamespace
                ? {
                    dispatch_namespace:
                      platformResources.stateWorker.dispatchNamespace,
                  }
                : {}),
            }
          : binding.scriptName
            ? { script_name: binding.scriptName }
            : {}),
        ...(!platformResources && binding.dispatchNamespace
          ? { dispatch_namespace: binding.dispatchNamespace }
          : {}),
      })),
      ...(application?.vars ?? canonicalApplicationBindings(spec).vars).map(
        ({ name, value }) => ({
          name,
          type: 'plain_text',
          text: value,
        }),
      ),
      ...(application?.r2Buckets ?? []).map((binding) => ({
        name: binding.name,
        type: 'r2_bucket',
        bucket_name: binding.bucketName,
      })),
      ...(spec.authoredBy === 'external' && spec.queueProducer
        ? [
            {
              name: FLEET_AUDIT_PROXY_BINDING,
              type: 'durable_object_namespace',
              class_name: FLEET_AUDIT_PROXY_CLASS_NAME,
              script_name: platformResources?.stateWorker.scriptName,
              ...(platformResources?.stateWorker.dispatchNamespace
                ? {
                    dispatch_namespace:
                      platformResources.stateWorker.dispatchNamespace,
                  }
                : {}),
            },
          ]
        : []),
    ];
    if (spec.queueProducer && spec.authoredBy !== 'external') {
      bindings.push({
        name: spec.queueProducer.binding,
        type: 'queue',
        queue_name: spec.queueProducer.queueName,
      });
    }
    const egressProxyService = platformResources
      ? undefined
      : spec.egressProxyService;
    if (egressProxyService) {
      bindings.push({
        name: 'EGRESS_PROXY',
        type: 'service',
        service: egressProxyService,
      });
    }
    const files = await Promise.all(
      spec.modules.map((module) =>
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
    const migrations = dispatchMigrations(spec);

    return this.#schedule(async () => {
      await this.#assertUntrustedDispatchNamespace(dispatchNamespace);
      const result =
        await this.#client.workersForPlatforms.dispatch.namespaces.scripts.update(
          physicalScriptName,
          {
            account_id: this.#accountId,
            dispatch_namespace: dispatchNamespace,
            bindings_inherit: 'strict',
            files,
            metadata: {
              bindings: bindings as never,
              compatibility_date: spec.compatibilityDate,
              compatibility_flags: spec.compatibilityFlags
                ? [...spec.compatibilityFlags]
                : undefined,
              keep_bindings: ['secret_text'],
              limits: {
                cpu_ms: spec.cpuLimitMs,
                subrequests: spec.subrequestLimit,
              },
              main_module: spec.mainModule,
              migrations,
              tags: [
                FLEET_SCRIPT_TAG,
                `tenant:${spec.tenantTag}`,
                `environment:${spec.environment}`,
                `schema:${spec.schemaVersion}`,
                `spec:${deploymentSpecDigest(spec)}`,
                ...(spec.durableObjectMigrations.at(-1)?.tag
                  ? [`do:${spec.durableObjectMigrations.at(-1)?.tag}`]
                  : []),
              ],
            },
          },
        );
      if (!result.etag) {
        throw new Error(
          `Cloudflare did not return an artifact etag for '${physicalScriptName}'`,
        );
      }
      return { artifactVersion: result.etag };
    });
  }

  async uploadNamespacedStateWorker(options: {
    readonly spec: DeploymentSpec;
    readonly database: DatabaseReference;
    readonly artifact: import('./types.js').TrustedWorkerArtifact;
    readonly artifactDigest: string;
    readonly maintenanceCapabilityPublicKey: string;
    readonly auditQueueName?: string;
    readonly sharedOutboundWorkerName: string;
    readonly stateEgressCredentialDigest: string;
  }): Promise<{ artifactVersion: string }> {
    const dispatchNamespace = this.#requireDispatchNamespace(
      'uploadNamespacedStateWorker',
    );
    const { spec } = options;
    const scriptName = externalStateScriptName(spec);
    const resourceGroupId = externalPlatformResourceGroupId(spec);
    const bindings: Array<Record<string, unknown>> = [
      { name: 'DB', type: 'd1', database_id: options.database.id },
      { name: 'DEPLOYMENT_TENANT', type: 'plain_text', text: spec.tenantTag },
      { name: 'FLEET_ENVIRONMENT', type: 'plain_text', text: spec.environment },
      {
        name: 'FLEET_SCHEMA_VERSION',
        type: 'plain_text',
        text: String(spec.schemaVersion),
      },
      {
        name: 'FLEET_SPEC_DIGEST',
        type: 'plain_text',
        text: deploymentSpecDigest(spec),
      },
      {
        name: 'FLEET_RESOURCE_GROUP',
        type: 'plain_text',
        text: resourceGroupId,
      },
      {
        name: 'FLEET_RESOURCE_ROLE',
        type: 'plain_text',
        text: 'platform-state',
      },
      {
        name: 'FLEET_DEPLOYMENT_SCRIPT',
        type: 'plain_text',
        text: spec.scriptName,
      },
      {
        name: 'FLEET_MAINTENANCE_CAPABILITIES',
        type: 'plain_text',
        text: 'required',
      },
      {
        name: 'FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY',
        type: 'plain_text',
        text: options.maintenanceCapabilityPublicKey,
      },
      {
        name: 'FLEET_ARTIFACT_DIGEST',
        type: 'plain_text',
        text: options.artifactDigest,
      },
      {
        name: 'FLEET_RUNTIME_CONTRACT',
        type: 'plain_text',
        text: '1',
      },
      { name: 'OUTBOUND_TENANT_ID', type: 'plain_text', text: spec.tenantTag },
      {
        name: 'OUTBOUND_ENVIRONMENT',
        type: 'plain_text',
        text: spec.environment,
      },
      {
        name: 'OUTBOUND_RESOURCE_GROUP_ID',
        type: 'plain_text',
        text: resourceGroupId,
      },
      {
        name: 'OUTBOUND_STATE_SCRIPT_NAME',
        type: 'plain_text',
        text: scriptName,
      },
      {
        name: 'OUTBOUND_ROUTE_HOSTNAME',
        type: 'plain_text',
        text: spec.routeHostname.toLowerCase(),
      },
      {
        name: 'OUTBOUND_POLICY_ID',
        type: 'plain_text',
        text: resourceGroupId,
      },
      ...spec.durableObjectBindings.map((binding) => ({
        name: binding.name,
        type: 'durable_object_namespace',
        class_name: binding.className,
      })),
      ...(options.auditQueueName
        ? [
            {
              name: FLEET_AUDIT_PROXY_STATE_BINDING,
              type: 'durable_object_namespace',
              class_name: FLEET_AUDIT_PROXY_CLASS_NAME,
            },
          ]
        : []),
      {
        name: 'OUTBOUND_PROXY',
        type: 'service',
        service: options.sharedOutboundWorkerName,
        entrypoint: 'StateEgress',
      },
      ...(options.auditQueueName
        ? [
            {
              name: 'FLEET_AUDIT_PROXY_INGRESS',
              type: 'plain_text',
              text: 'required',
            },
            {
              name: 'AUDIT_QUEUE',
              type: 'queue',
              queue_name: options.auditQueueName,
            },
          ]
        : []),
    ];
    const files = await Promise.all(
      options.artifact.modules.map((module) =>
        toFile(
          typeof module.content === 'string'
            ? new TextEncoder().encode(module.content)
            : module.content,
          module.name,
          { type: module.contentType ?? 'application/javascript+module' },
        ),
      ),
    );
    const stateSpec: DeploymentSpec = {
      ...spec,
      authoredBy: 'platform',
      mainModule: options.artifact.mainModule,
      modules: options.artifact.modules,
      compatibilityDate: options.artifact.compatibilityDate,
      compatibilityFlags: options.artifact.compatibilityFlags,
    };
    return this.#schedule(async () => {
      await this.#assertUntrustedDispatchNamespace(dispatchNamespace);
      const result =
        await this.#client.workersForPlatforms.dispatch.namespaces.scripts.update(
          scriptName,
          {
            account_id: this.#accountId,
            dispatch_namespace: dispatchNamespace,
            bindings_inherit: 'strict',
            files,
            metadata: {
              bindings: bindings as never,
              compatibility_date: options.artifact.compatibilityDate,
              compatibility_flags: options.artifact.compatibilityFlags
                ? [...options.artifact.compatibilityFlags]
                : undefined,
              keep_bindings: ['secret_text'],
              main_module: options.artifact.mainModule,
              migrations: dispatchMigrations(stateSpec),
              tags: [
                FLEET_SCRIPT_TAG,
                'role:platform-state',
                `group:${resourceGroupId}`,
                `tenant:${spec.tenantTag}`,
                `environment:${spec.environment}`,
                `schema:${spec.schemaVersion}`,
                `spec:${deploymentSpecDigest(spec)}`,
                ...(spec.durableObjectMigrations.at(-1)?.tag
                  ? [`do:${spec.durableObjectMigrations.at(-1)?.tag}`]
                  : []),
                `artifact:${options.artifactDigest}`,
                `state-egress:${options.stateEgressCredentialDigest}`,
              ],
            },
          },
        );
      if (!result.etag) {
        throw new Error(
          `Cloudflare did not return an artifact etag for '${scriptName}'`,
        );
      }
      return { artifactVersion: result.etag };
    });
  }

  async putDispatchSecrets(
    scriptName: string,
    secrets: DeploymentSecrets,
    options: Readonly<{
      includeMaintenanceAdmin?: boolean;
      additionalSecrets?: Readonly<Record<string, string>>;
    }> = {},
  ): Promise<void> {
    const dispatchNamespace =
      this.#requireDispatchNamespace('putDispatchSecrets');
    await this.#schedule(async () => {
      const scripts =
        this.#client.workersForPlatforms.dispatch.namespaces.scripts;
      const listSecretNames = async (): Promise<string[]> => {
        const names: string[] = [];
        for await (const secret of this.#collectBounded(
          scripts.secrets.list(scriptName, {
            account_id: this.#accountId,
            dispatch_namespace: dispatchNamespace,
          }),
          'dispatch Worker secret inventory',
        )) {
          if (!secret.name) {
            throw new Error(
              `dispatch Worker '${scriptName}' returned a secret without a name`,
            );
          }
          names.push(secret.name);
        }
        return names.sort();
      };
      const desiredSecrets: Readonly<Record<string, string>> = {
        DEPLOYMENT_IDENTITY_SECRET: secrets.deploymentIdentity,
        ...(options.includeMaintenanceAdmin === false
          ? {}
          : { MAINTENANCE_ADMIN_SECRET: secrets.maintenanceAdmin }),
        ...(options.additionalSecrets ?? {}),
      };
      const desiredSecretNames = Object.keys(desiredSecrets).sort();
      const currentSecretNames = await listSecretNames();
      await this.#client.workersForPlatforms.dispatch.namespaces.scripts.secrets.bulkUpdate(
        scriptName,
        {
          account_id: this.#accountId,
          dispatch_namespace: dispatchNamespace,
          secrets: Object.fromEntries([
            ...Object.entries(desiredSecrets).map(
              ([name, text]) =>
                [name, { name, text, type: 'secret_text' as const }] as const,
            ),
            ...currentSecretNames
              .filter((name) => !(name in desiredSecrets))
              .map((name) => [name, null] as const),
          ]),
        },
      );
      const secretNames = await listSecretNames();
      if (JSON.stringify(secretNames) !== JSON.stringify(desiredSecretNames)) {
        throw new Error(
          `dispatch Worker '${scriptName}' failed exact secret convergence`,
        );
      }
    });
  }

  async inspectDispatchWorker(scriptName: string): Promise<
    | {
        artifactVersion: string;
        databaseIds: readonly string[];
        durableObjectBindings: readonly import('./types.js').DurableObjectBindingInventory[];
        serviceBindings: readonly Readonly<{
          name: string;
          service: string;
          entrypoint?: string;
        }>[];
        queueProducerBindings: readonly Readonly<{
          name: string;
          queueName: string;
        }>[];
        r2BucketBindings: readonly import('./types.js').ApplicationR2Binding[];
        secretNames: readonly string[];
        tenantTag: string;
        environment: string;
        schemaVersion: number;
        desiredSpecDigest: string;
        durableObjectTag?: string;
        plainTextBindings: Readonly<Record<string, string>>;
        providerBindingIdentities: readonly ProviderBindingIdentity[];
      }
    | undefined
  > {
    const dispatchNamespace = this.#requireDispatchNamespace(
      'inspectDispatchWorker',
    );
    return this.#schedule(async () => {
      try {
        const scripts =
          this.#client.workersForPlatforms.dispatch.namespaces.scripts;
        const [script, settings] = await Promise.all([
          scripts.get(scriptName, {
            account_id: this.#accountId,
            dispatch_namespace: dispatchNamespace,
          }),
          scripts.settings.get(scriptName, {
            account_id: this.#accountId,
            dispatch_namespace: dispatchNamespace,
          }),
        ]);
        const bindings = settings.bindings ?? [];
        const databaseIds = bindings.flatMap((binding) =>
          binding.type === 'd1' && binding.database_id
            ? [binding.database_id]
            : [],
        );
        const durableObjectBindings = bindings.flatMap((binding) => {
          if (
            binding.type !== 'durable_object_namespace' ||
            !binding.namespace_id ||
            !binding.name ||
            !binding.class_name
          ) {
            return [];
          }
          return [
            {
              name: binding.name,
              className: binding.class_name,
              namespaceId: binding.namespace_id,
              ...('script_name' in binding && binding.script_name
                ? { scriptName: String(binding.script_name) }
                : {}),
              ...('dispatch_namespace' in binding && binding.dispatch_namespace
                ? { dispatchNamespace: String(binding.dispatch_namespace) }
                : {}),
            },
          ];
        });
        const serviceBindings = bindings.flatMap((binding) =>
          binding.type === 'service' && binding.name && binding.service
            ? [
                {
                  name: binding.name,
                  service: binding.service,
                  ...('entrypoint' in binding && binding.entrypoint
                    ? { entrypoint: String(binding.entrypoint) }
                    : {}),
                },
              ]
            : [],
        );
        const queueProducerBindings = bindings.flatMap((binding) =>
          binding.type === 'queue' && binding.name && binding.queue_name
            ? [{ name: binding.name, queueName: binding.queue_name }]
            : [],
        );
        const r2BucketBindings = bindings
          .flatMap((binding) =>
            binding.type === 'r2_bucket' && binding.name && binding.bucket_name
              ? [
                  {
                    name: binding.name,
                    bucketName: binding.bucket_name,
                    jurisdiction: 'default' as const,
                  },
                ]
              : [],
          )
          .sort((left, right) => left.name.localeCompare(right.name));
        const secretNames = bindings
          .flatMap((binding) =>
            binding.type === 'secret_text' && binding.name
              ? [binding.name]
              : [],
          )
          .sort();
        const plainTextBindings = Object.fromEntries(
          bindings.flatMap((binding) =>
            binding.type === 'plain_text' && binding.name
              ? [[binding.name, String(binding.text ?? '')] as const]
              : [],
          ),
        );
        const providerBindingIdentities = assertSupportedProviderBindings(
          bindings,
          new Set([
            'd1',
            'durable_object_namespace',
            'service',
            'queue',
            'r2_bucket',
            'plain_text',
            'secret_text',
          ]),
          `dispatch Worker '${scriptName}'`,
        );
        const schemaTag = settings.tags?.find((tag) =>
          tag.startsWith('schema:'),
        );
        const desiredSpecDigest = settings.tags
          ?.find((tag) => tag.startsWith('spec:'))
          ?.slice('spec:'.length);
        const durableObjectTag = settings.tags
          ?.find((tag) => tag.startsWith('do:'))
          ?.slice('do:'.length);
        const tenantTag = settings.tags
          ?.find((tag) => tag.startsWith('tenant:'))
          ?.slice('tenant:'.length);
        const environment = settings.tags
          ?.find((tag) => tag.startsWith('environment:'))
          ?.slice('environment:'.length);
        const schemaVersion = Number(schemaTag?.slice('schema:'.length));
        const artifactVersion = script.script?.etag;
        if (
          !artifactVersion ||
          !tenantTag ||
          !environment ||
          !desiredSpecDigest ||
          !Number.isSafeInteger(schemaVersion)
        ) {
          throw new Error(
            `script '${scriptName}' has incomplete fleet metadata`,
          );
        }
        const inspection = {
          artifactVersion,
          databaseIds,
          durableObjectBindings,
          serviceBindings,
          queueProducerBindings,
          r2BucketBindings,
          secretNames,
          plainTextBindings,
          providerBindingIdentities,
          tenantTag,
          environment,
          schemaVersion,
          desiredSpecDigest,
          ...(durableObjectTag ? { durableObjectTag } : {}),
        };
        assertProviderBindingIdentitiesMatchInspection(
          inspection,
          `dispatch Worker '${scriptName}'`,
        );
        return inspection;
      } catch (error) {
        if (isNotFound(error)) return undefined;
        throw error;
      }
    });
  }

  async revokeDispatchSecrets(scriptName: string): Promise<void> {
    const dispatchNamespace = this.#requireDispatchNamespace(
      'revokeDispatchSecrets',
    );
    await this.#schedule(async () => {
      const scripts =
        this.#client.workersForPlatforms.dispatch.namespaces.scripts;
      const listNames = async (): Promise<string[]> => {
        try {
          const names: string[] = [];
          for await (const secret of this.#collectBounded(
            scripts.secrets.list(scriptName, {
              account_id: this.#accountId,
              dispatch_namespace: dispatchNamespace,
            }),
            'dispatch Worker secret inventory',
          )) {
            if (!secret.name) {
              throw new Error(
                `dispatch Worker '${scriptName}' returned a secret without a name`,
              );
            }
            names.push(secret.name);
          }
          return names.sort();
        } catch (error) {
          if (isNotFound(error)) return [];
          throw error;
        }
      };
      const current = await listNames();
      if (current.length > 0) {
        try {
          await scripts.secrets.bulkUpdate(scriptName, {
            account_id: this.#accountId,
            dispatch_namespace: dispatchNamespace,
            secrets: Object.fromEntries(
              current.map((name) => [name, null] as const),
            ),
          });
        } catch (error) {
          if (!isNotFound(error)) throw error;
        }
      }
      if ((await listNames()).length !== 0) {
        throw new Error(
          `dispatch Worker '${scriptName}' failed exact secret revocation`,
        );
      }
    });
  }

  async deleteDispatchWorker(scriptName: string): Promise<void> {
    const dispatchNamespace = this.#requireDispatchNamespace(
      'deleteDispatchWorker',
    );
    await this.#schedule(async () => {
      try {
        await this.#client.workersForPlatforms.dispatch.namespaces.scripts.delete(
          scriptName,
          {
            account_id: this.#accountId,
            dispatch_namespace: dispatchNamespace,
          },
        );
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
    });
  }

  async exportDatabase(databaseId: string): Promise<DatabaseExport> {
    if (!this.#exportStore) {
      throw new Error(
        'a durable exportStore is required before D1 can be exported for deletion',
      );
    }
    let bookmark: string | undefined;
    let pollCount = 0;
    let providerStatusError = false;
    let httpStatus: number | undefined;
    let safeDetail: string | undefined;
    const fail: (detail?: string) => never = (detail) => {
      if (detail !== undefined) safeDetail = detail;
      throw new Error('D1 export failed');
    };
    try {
      for (let attempt = 0; attempt < 120; attempt += 1) {
        pollCount += 1;
        const response = await this.#schedule(() =>
          this.#client.d1.database.export(
            databaseId,
            {
              account_id: this.#accountId,
              output_format: 'polling',
              current_bookmark: bookmark,
            },
            { maxRetries: 0 },
          ),
        );
        if (response.status === 'error') {
          providerStatusError = true;
          fail();
        }
        if (response.status === 'complete' && response.result?.signed_url) {
          const signedUrl = new URL(response.result.signed_url);
          if (signedUrl.protocol !== 'https:') {
            fail('export returned a non-HTTPS download URL');
          }
          const download = await this.#request(signedUrl, {
            redirect: 'error',
          });
          httpStatus = download.status;
          if (!download.ok) fail();
          const downloadBody = download.body;
          if (!downloadBody) fail();
          const [storeBody, hashBody] = downloadBody.tee();
          const contentLengthValue = download.headers.get('content-length');
          const contentLength = contentLengthValue
            ? Number(contentLengthValue)
            : undefined;
          const hasContentLength =
            contentLength !== undefined &&
            Number.isSafeInteger(contentLength) &&
            contentLength >= 0;
          const [stored, integrity] = await Promise.all([
            this.#exportStore.write({
              databaseId,
              fileName: `${databaseId}-${Date.now()}.sql`,
              body: storeBody,
              ...(hasContentLength ? { contentLength } : {}),
            }),
            hashExport(hashBody),
          ]);
          if (!stored.location || integrity.size === 0) {
            fail('durable D1 export is empty or has no location');
          }
          if (hasContentLength && integrity.size !== contentLength) {
            fail('durable D1 export size differs from the download');
          }
          if (
            stored.size !== integrity.size ||
            stored.sha256 !== integrity.sha256
          ) {
            fail(
              'committed durable D1 export integrity differs from the download',
            );
          }
          return { databaseId, location: stored.location, ...integrity };
        }
        if (!response.at_bookmark) {
          fail('export returned no polling bookmark');
        }
        bookmark = response.at_bookmark;
        await new Promise((resolve) => setTimeout(resolve, 1_000));
      }
      fail('export did not complete within the poll budget');
    } catch (error) {
      const errorStatus = readErrorFieldSafely(error, 'status');
      if (typeof errorStatus === 'number') httpStatus = errorStatus;
      const name = sanitizedErrorName(error);
      throw new Error(
        `${safeDetail ? `${safeDetail}: ` : ''}D1 export for '${databaseId}' failed after ${pollCount} poll(s)${
          providerStatusError ? " with provider status 'error'" : ''
        }${httpStatus === undefined ? '' : ` with HTTP ${httpStatus}`}`,
        { cause: { name } },
      );
    }
  }

  async deleteDatabase(databaseId: string): Promise<void> {
    await this.#schedule(async () => {
      try {
        await this.#client.d1.database.delete(databaseId, {
          account_id: this.#accountId,
        });
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
    });
  }
}
