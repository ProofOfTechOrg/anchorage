// SPDX-License-Identifier: Apache-2.0

import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';
import { BaseNamespaces } from 'cloudflare/resources/workers-for-platforms/dispatch/namespaces/namespaces';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  CloudflarePlaneCapabilityError,
  CloudflareProviderRequestNotDispatchedError,
  CloudflareProvisioningClient,
  type DurableDatabaseExportStore,
  withProviderDispatchTracking,
} from '../src/cloudflare-client.js';
import { databaseExportReceiptError } from '../src/database-export-store.js';
import {
  assertSupportedPlainWorkerBindings,
  plainWorkerBindingsToProviderShape,
  providerBindingsToPlainWorkerShape,
} from '../src/provider-binding-inventory.js';
import type {
  DatabaseExportReceiptIdentity,
  PlainWorkerUploadIntent,
  PlainWorkerVersionBinding,
} from '../src/types.js';
import {
  deferred,
  fenced,
  pageArray,
  pageItems,
  recordingFetch,
  restProjection,
  single,
  testRateCoordinator,
  zoneAuthorityResponse,
} from './fixtures/cloudflare-fetch-fixture.js';
import { errorChain } from './fixtures/plain-worker-harnesses.js';
import { providerWorld } from './fixtures/provider-world.js';

// Shared-client WFP-plane cases live here so the legacy WFP request and
// response pins remain byte-comparable to the pre-plain-worker client.

function apiFailure(status: number, message = 'provider failure'): Response {
  return Response.json(
    {
      success: false,
      errors: [{ code: 10_000 + status, message }],
      messages: [],
      result: null,
    },
    { status },
  );
}

const RECEIPT_AUTHORITY = 'memory://fleet-exports/receipts/v1';
const RECEIPT_IDENTITY: DatabaseExportReceiptIdentity = {
  version: 1,
  authority: RECEIPT_AUTHORITY,
  databaseId: '00000000-0000-0000-0000-000000000001',
  operationId: '00000000-0000-4000-8000-000000000002',
};

function uploadIntent(mode: 'initial' | 'staged'): PlainWorkerUploadIntent {
  const base = {
    scriptName: 'plain',
    candidateTag: 'candidate-tag',
    mainModule: 'worker.js',
    modules: [
      { name: 'worker.js', content: 'export default {}' },
      {
        name: 'data.txt',
        content: 'data',
        contentType: 'text/plain',
      },
    ],
    compatibilityDate: '2026-08-26',
    compatibilityFlags: ['nodejs_compat'],
    bindings: {
      plainText: [{ name: 'TEXT', value: 'value' }],
      secrets: [{ name: 'SECRET', value: 'super-secret' }],
      d1: [{ name: 'DB', databaseId: 'db-id', databaseName: 'db-name' }],
      durableObjects: [{ name: 'STATE', className: 'State' }],
      services: [{ name: 'SERVICE', service: 'upstream' }],
      queueProducers: [{ name: 'QUEUE', queueName: 'jobs' }],
      r2Buckets: [{ name: 'BUCKET', bucketName: 'objects' }],
    },
    limits: { cpuMs: 42 },
    publicAccess: {
      workersDevEnabled: false,
      previewUrlsEnabled: false,
    },
  };
  return mode === 'initial'
    ? {
        ...base,
        mode,
        durableObjectMigrations: [{ tag: 'v1', newSqliteClasses: ['State'] }],
      }
    : { ...base, mode };
}

function plainClient(
  options: {
    readonly fetch?: typeof fetch;
    readonly rateCoordinator?: { acquire(signal?: AbortSignal): Promise<void> };
    readonly exportStore?: DurableDatabaseExportStore;
    readonly concurrency?: number;
    readonly requestTimeoutMs?: number;
  } = {},
): CloudflareProvisioningClient {
  return new CloudflareProvisioningClient({
    accountId: 'account',
    apiToken: 'token',
    plane: 'plain-worker',
    rateCoordinator: options.rateCoordinator ?? testRateCoordinator(),
    fetch: options.fetch,
    exportStore: options.exportStore,
    concurrency: options.concurrency,
    requestTimeoutMs: options.requestTimeoutMs,
  });
}

function ownSerialization(value: unknown): string {
  const seen = new WeakSet<object>();
  return JSON.stringify(value, (_key, candidate) => {
    if (!candidate || typeof candidate !== 'object') return candidate;
    if (seen.has(candidate)) return '[circular]';
    seen.add(candidate);
    return Object.fromEntries(
      Object.getOwnPropertyNames(candidate).map((name) => [
        name,
        Reflect.get(candidate, name),
      ]),
    );
  });
}

function fact(value: unknown, name: string): unknown {
  return value && typeof value === 'object'
    ? Reflect.get(value, name)
    : undefined;
}

function hasFact(value: unknown, name: string): boolean {
  return Boolean(
    value && typeof value === 'object' && Reflect.has(value, name),
  );
}

function boundedErrorCauses(
  error: unknown,
  max = 20,
): { readonly errors: readonly Error[]; readonly terminal: unknown } {
  const errors: Error[] = [];
  let current = fact(error, 'cause');
  while (current instanceof Error && errors.length < max) {
    errors.push(current);
    current = current.cause;
  }
  return { errors, terminal: current };
}

async function failedInitialUpload(error: Error): Promise<unknown> {
  const fixture = recordingFetch(() => Promise.reject(error));
  const client = plainClient({ fetch: fixture.fetch });
  const prepared = await client.prepareOrdinaryWorkerUpload(
    uploadIntent('initial'),
  );
  return fenced(client, () =>
    client.dispatchOrdinaryWorkerUpload(prepared),
  ).catch((failure: unknown) => failure);
}

function nestedTransportFailure(leaf: Error): Error {
  // The SDK inspects the rejection and its immediate cause before wrapping
  // (client.mjs:378-390); depth 3 places the hostile leaf below both, so every
  // row reaches this boundary whichever operation is hostile. Some hazards
  // (a prototype trap, a cause or constructor accessor) may reach it from a
  // shallower depth; depth 3 is sufficient for every row.
  return new Error('outer transport failure', {
    cause: new Error('middle transport failure', { cause: leaf }),
  });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('CloudflareProvisioningClient plain-worker plane', () => {
  it('a queued mutation asserts its own lease', async () => {
    const response = deferred<Response>();
    const started = deferred<void>();
    const fixture = recordingFetch(() => {
      started.resolve();
      return response.promise;
    });
    const client = plainClient({
      concurrency: 1,
      fetch: fixture.fetch,
      requestTimeoutMs: 1_000,
    });
    const firstAssertOwned = vi.fn(async () => {});
    let secondOwned = true;
    const secondFenceError = new Error('second mutation lease lost');
    const secondAssertOwned = vi.fn(async () => {
      if (!secondOwned) throw secondFenceError;
    });
    const firstPrepared = client.prepareOrdinaryWorkerDeployment([
      { versionId: 'v1', percentage: 100 },
    ]);
    const secondPrepared = client.prepareOrdinaryWorkerDeployment([
      { versionId: 'v2', percentage: 100 },
    ]);
    const first = client.withMutationFence(
      { mutationLeaseTtlMs: 2_000, assertOwned: firstAssertOwned },
      () => client.dispatchOrdinaryWorkerDeployment('first', firstPrepared),
    );
    await started.promise;
    const second = client.withMutationFence(
      { mutationLeaseTtlMs: 2_000, assertOwned: secondAssertOwned },
      () => client.dispatchOrdinaryWorkerDeployment('second', secondPrepared),
    );
    secondOwned = false;
    response.resolve(single({ id: 'deployment' }));

    await expect(first).resolves.toBeUndefined();
    const failure = await second.catch((error: unknown) => error);
    expect(errorChain(failure)).toContain(secondFenceError.message);
    expect(fixture.requests).toHaveLength(1);
    expect(firstAssertOwned).toHaveBeenCalledOnce();
  });

  it('a queued operation classifies its own dispatch after p-queue handoff', async () => {
    const readResponse = deferred<Response>();
    const readStarted = deferred<void>();
    const fixture = recordingFetch(({ method }) => {
      if (method !== 'GET') return apiFailure(500);
      readStarted.resolve();
      return readResponse.promise;
    });
    const client = plainClient({ concurrency: 1, fetch: fixture.fetch });
    const secondSettled = deferred<void>();
    const sentinel = new Error('first operation failed locally');
    const first = withProviderDispatchTracking(client, async () => {
      await client.findOrdinaryWorkerVersion('plain', 'v1');
      await secondSettled.promise;
      throw sentinel;
    });
    await readStarted.promise;
    const prepared = client.prepareOrdinaryWorkerDeployment([
      { versionId: 'v2', percentage: 100 },
    ]);
    const second = withProviderDispatchTracking(client, () =>
      client.withMutationFence(
        { mutationLeaseTtlMs: 15 * 60_000, assertOwned: vi.fn(async () => {}) },
        () => client.dispatchOrdinaryWorkerDeployment('plain', prepared),
      ),
    );
    readResponse.resolve(single({ id: 'v1', resources: { bindings: [] } }));
    const secondFailure = await second.catch((error: unknown) => error);
    secondSettled.resolve();
    const firstFailure = await first.catch((error: unknown) => error);

    expect(secondFailure).not.toBeInstanceOf(
      CloudflareProviderRequestNotDispatchedError,
    );
    expect(fact(secondFailure, 'status')).toBe(500);
    expect(firstFailure).toBeInstanceOf(
      CloudflareProviderRequestNotDispatchedError,
    );
    expect(fact(firstFailure, 'cause')).toBe(sentinel);
  });

  it.each([
    [
      'local failure',
      (_client: CloudflareProvisioningClient) => async () =>
        Promise.reject(new Error('local preparation failed')),
      true,
      0,
      'local preparation failed',
    ],
    [
      'read failure',
      (client: CloudflareProvisioningClient) => () =>
        client.findOrdinaryWorkerVersion('plain', 'missing'),
      true,
      3,
      undefined,
    ],
    [
      'mutation failure',
      (client: CloudflareProvisioningClient) => {
        const prepared = client.prepareOrdinaryWorkerDeployment([
          { versionId: 'v1', percentage: 100 },
        ]);
        return () =>
          fenced(client, () =>
            client.dispatchOrdinaryWorkerDeployment('plain', prepared),
          );
      },
      false,
      1,
      undefined,
    ],
    [
      'transport fence failure',
      (client: CloudflareProvisioningClient) => {
        const prepared = client.prepareOrdinaryWorkerDeployment([
          { versionId: 'v1', percentage: 100 },
        ]);
        return () =>
          client.withMutationFence(
            {
              mutationLeaseTtlMs: 15 * 60_000,
              assertOwned: async () => {
                throw new Error('transport fence lost');
              },
            },
            () => client.dispatchOrdinaryWorkerDeployment('plain', prepared),
          );
      },
      true,
      0,
      'transport fence lost',
    ],
  ] as const)('tracks provider dispatch for %s', async (_kind, buildOperation, expectsWrapper, expectedRequestCount, expectedCauseFragment) => {
    const fixture = recordingFetch(() => apiFailure(500));
    const client = plainClient({ fetch: fixture.fetch });
    const failure = await withProviderDispatchTracking(
      client,
      buildOperation(client),
    ).catch((error: unknown) => error);

    expect(failure instanceof CloudflareProviderRequestNotDispatchedError).toBe(
      expectsWrapper,
    );
    expect(fact(failure, 'status')).toBe(expectsWrapper ? undefined : 500);
    expect(fixture.requests).toHaveLength(expectedRequestCount);
    if (expectedCauseFragment !== undefined) {
      expect(errorChain(fact(failure, 'cause'))).toContain(
        expectedCauseFragment,
      );
    }
  });

  it('runs a queued request under its enqueuer context', async () => {
    const testContext = new AsyncLocalStorage<string>();
    const stores: Array<readonly [string, string | undefined]> = [];
    const world = providerWorld();
    const bindings = [
      { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'acme' },
      {
        type: 'plain_text',
        name: 'FLEET_ENVIRONMENT',
        text: 'production',
      },
      { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '1' },
    ];
    world.seedScript('plain', {
      versions: [
        {
          versionId: 'v1',
          tag: undefined,
          bindings,
          mainModule: 'worker.js',
          modules: [{ name: 'worker.js', content: 'export default {}' }],
        },
      ],
      deployment: [{ versionId: 'v1', percentage: 100 }],
      subdomain: { enabled: false, previewsEnabled: false },
    });
    const project = restProjection(world);
    const versionResponse = deferred<Response>();
    const subdomainResponse = deferred<Response>();
    const fanStarted = deferred<void>();
    let blocked = 0;
    const hold = (response: ReturnType<typeof deferred<Response>>) => {
      blocked += 1;
      if (blocked === 2) fanStarted.resolve();
      return response.promise;
    };
    const fixture = recordingFetch((request) => {
      const { url } = request;
      const target = new URL(url);
      stores.push([target.pathname, testContext.getStore()]);
      if (target.pathname.endsWith('/versions/v1')) {
        return hold(versionResponse);
      }
      if (target.pathname.endsWith('/subdomain')) {
        return hold(subdomainResponse);
      }
      if (target.pathname.endsWith('/versions/v2')) {
        return single({ id: 'v2', resources: { bindings: [] } });
      }
      const authority = zoneAuthorityResponse(target, []);
      if (authority) return authority;
      if (target.pathname.endsWith('/workers/scripts')) {
        return pageArray([{ id: 'plain' }]);
      }
      if (target.pathname.endsWith('/secrets')) return pageArray([]);
      if (
        target.pathname.endsWith('/workers/domains') ||
        target.pathname.endsWith('/workers/durable_objects/namespaces')
      ) {
        return pageArray([]);
      }
      return project(request);
    });
    const client = plainClient({ concurrency: 2, fetch: fixture.fetch });
    const first = testContext.run('A', () =>
      // collectFleetInventory is A because its version/subdomain fan-out uses
      // raw SDK calls from one operation slot, so its held requests fill both
      // request slots and B queues there; a future #schedule around the fan-out
      // would silently make this test vacuous.
      client.collectFleetInventory({
        databaseNamePrefix: 'fleet-',
        scriptNamePrefix: 'plain',
        includeDispatchNamespace: false,
      }),
    );
    await fanStarted.promise;
    const second = testContext.run('B', () =>
      client.findOrdinaryWorkerVersion('plain', 'v2'),
    );
    // Let the SDK enqueue B while A still occupies both request slots.
    await new Promise<void>((resolve) => setImmediate(resolve));
    versionResponse.resolve(single({ id: 'v1', resources: { bindings } }));
    subdomainResponse.resolve(
      single({ enabled: false, previews_enabled: false }),
    );

    await expect(second).resolves.toMatchObject({ versionId: 'v2' });
    await expect(first).resolves.toMatchObject({
      deployments: [{ artifactVersion: 'v1' }],
    });
    expect(stores.find(([path]) => path.endsWith('/versions/v2'))?.[1]).toBe(
      'B',
    );
  });

  it('pins the four existing constructor validation messages', () => {
    expect(() =>
      Reflect.construct(CloudflareProvisioningClient, [
        { accountId: '', apiToken: '', dispatchNamespace: '' },
      ]),
    ).toThrow('accountId, apiToken, and dispatchNamespace are required');
    expect(() =>
      Reflect.construct(CloudflareProvisioningClient, [
        { accountId: 'a', apiToken: 't', dispatchNamespace: 'd' },
      ]),
    ).toThrow('rateCoordinator is required');
    expect(
      () =>
        new CloudflareProvisioningClient({
          accountId: 'a',
          apiToken: 't',
          dispatchNamespace: 'd',
          rateCoordinator: testRateCoordinator(),
          concurrency: 0,
        }),
    ).toThrow('concurrency must be a positive integer');
    expect(
      () =>
        new CloudflareProvisioningClient({
          accountId: 'a',
          apiToken: 't',
          dispatchNamespace: 'd',
          rateCoordinator: testRateCoordinator(),
          requestTimeoutMs: 0,
        }),
    ).toThrow('requestTimeoutMs must be a positive integer');
  });

  it('accepts exactly the valid WFP and plain option shapes', () => {
    expect(
      new CloudflareProvisioningClient({
        accountId: 'a',
        apiToken: 't',
        dispatchNamespace: 'd',
        rateCoordinator: testRateCoordinator(),
      }),
    ).toBeInstanceOf(CloudflareProvisioningClient);
    expect(plainClient().requestTimeoutMs).toBe(60_000);
    expect(() =>
      Reflect.construct(CloudflareProvisioningClient, [
        {
          accountId: 'a',
          apiToken: 't',
          plane: 'ordinary',
          rateCoordinator: testRateCoordinator(),
        },
      ]),
    ).toThrow('unsupported Cloudflare client plane');
    expect(() =>
      Reflect.construct(CloudflareProvisioningClient, [
        {
          accountId: '',
          apiToken: 't',
          plane: 'plain-worker',
          rateCoordinator: testRateCoordinator(),
        },
      ]),
    ).toThrow('accountId and apiToken are required');
    for (const dispatchNamespace of [undefined, '', 'named']) {
      expect(() =>
        Reflect.construct(CloudflareProvisioningClient, [
          {
            accountId: 'a',
            apiToken: 't',
            plane: 'plain-worker',
            dispatchNamespace,
            rateCoordinator: testRateCoordinator(),
          },
        ]),
      ).toThrow('plain-worker plane cannot name a dispatch namespace');
    }
  });

  it('rejects every unconditional WFP member before issuing a request', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>();
    const client = plainClient({ fetch });
    const cases: readonly [string, () => unknown][] = [
      ['platformPlaneScope', () => client.platformPlaneScope()],
      [
        'assertUntrustedDispatchNamespace',
        () => client.assertUntrustedDispatchNamespace(),
      ],
      ['ensureDispatchNamespace', () => client.ensureDispatchNamespace()],
      [
        'uploadDispatchWorker',
        () =>
          client.uploadDispatchWorker(
            {
              tenantTag: 't',
              environment: 'e',
              scriptName: 's',
              databaseName: 'd',
              compatibilityDate: '2026-08-26',
              mainModule: 'worker.js',
              modules: [],
              authoredBy: 'platform',
              schemaVersion: 1,
              migrations: [],
              durableObjectMigrations: [],
              durableObjectBindings: [],
              maintenanceBaseUrl: 'https://example.test',
              routeHostname: 'example.test',
            },
            { id: 'db', name: 'db', created: false },
          ),
      ],
      [
        'uploadNamespacedStateWorker',
        () =>
          client.uploadNamespacedStateWorker({
            spec: {
              tenantTag: 't',
              environment: 'e',
              scriptName: 's',
              databaseName: 'd',
              compatibilityDate: '2026-08-26',
              mainModule: 'worker.js',
              modules: [],
              authoredBy: 'platform',
              schemaVersion: 1,
              migrations: [],
              durableObjectMigrations: [],
              durableObjectBindings: [],
              maintenanceBaseUrl: 'https://example.test',
              routeHostname: 'example.test',
            },
            database: { id: 'db', name: 'db', created: false },
            artifact: {
              mainModule: 'worker.js',
              modules: [],
              compatibilityDate: '2026-08-26',
            },
            artifactDigest: 'digest',
            maintenanceCapabilityPublicKey: 'key',
            sharedOutboundWorkerName: 'outbound',
            stateEgressCredentialDigest: 'digest',
          }),
      ],
      [
        'putDispatchSecrets',
        () =>
          client.putDispatchSecrets('s', {
            deploymentIdentity: 'identity',
            maintenanceAdmin: 'maintenance',
          }),
      ],
      ['inspectDispatchWorker', () => client.inspectDispatchWorker('s')],
      ['revokeDispatchSecrets', () => client.revokeDispatchSecrets('s')],
      ['deleteDispatchWorker', () => client.deleteDispatchWorker('s')],
    ];
    for (const [operation, invoke] of cases) {
      let failure: unknown;
      try {
        await invoke();
      } catch (error) {
        failure = error;
      }
      expect(failure).toBeInstanceOf(CloudflarePlaneCapabilityError);
      expect(fact(failure, 'operation')).toBe(operation);
      expect(fact(failure, 'requiredPlane')).toBe('workers-for-platforms');
    }
    expect(fetch).not.toHaveBeenCalled();
  });

  it('uses collectFleetInventory without dispatch and guards dispatch after KV reads', async () => {
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.includes('/storage/kv/namespaces/kv/keys')) {
        return pageArray([]);
      }
      if (target.pathname.endsWith('/workers/domains')) {
        return pageArray([]);
      }
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/d1/database')) return pageArray([]);
      if (target.pathname.endsWith('/workers/durable_objects/namespaces')) {
        return pageArray([]);
      }
      const authority = zoneAuthorityResponse(target, []);
      if (authority) return authority;
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const client = plainClient({ fetch: fixture.fetch });
    await expect(
      client.collectFleetInventory({
        databaseNamePrefix: 'fleet-',
        scriptNamePrefix: 'fleet-',
        includeDispatchNamespace: false,
      }),
    ).resolves.toMatchObject({ deployments: [], databaseIds: [] });
    await expect(
      client.collectFleetInventory({
        hostRoutingKvId: 'kv',
        databaseNamePrefix: 'fleet-',
        scriptNamePrefix: 'fleet-',
        includeDispatchNamespace: true,
      }),
    ).rejects.toMatchObject({
      name: 'CloudflarePlaneCapabilityError',
      operation: 'collectFleetInventory',
    });
    expect(
      fixture.requests.filter((request) => request.url.includes('/kv/')).length,
    ).toBe(1);
    expect(
      fixture.requests.some((request) => request.url.includes('/dispatch/')),
    ).toBe(false);
  });

  it('does not convert a plain-only dispatch capability failure into a stale registration', async () => {
    const key = '__anchorage_script__:fleet-registered';
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      const pathname = decodeURIComponent(target.pathname);
      if (pathname.endsWith('/storage/kv/namespaces/kv/keys')) {
        return pageArray([{ name: key }]);
      }
      if (pathname.includes('/storage/kv/namespaces/kv/values/')) {
        return new Response(
          JSON.stringify({
            scriptName: 'fleet-registered',
            tenantTag: 'acme',
            environment: 'production',
            databaseId: 'db',
            routeHostname: 'app.example.test',
          }),
        );
      }
      if (
        pathname.endsWith('/workers/domains') ||
        pathname.endsWith('/workers/scripts') ||
        pathname.endsWith('/d1/database') ||
        pathname.endsWith('/workers/durable_objects/namespaces')
      ) {
        return pageArray([]);
      }
      const authority = zoneAuthorityResponse(target, []);
      if (authority) return authority;
      throw new Error(`unexpected request ${pathname}`);
    });

    await expect(
      plainClient({ fetch: fixture.fetch }).collectFleetInventory({
        hostRoutingKvId: 'kv',
        databaseNamePrefix: 'fleet-',
        scriptNamePrefix: 'fleet-',
        includeDispatchNamespace: false,
      }),
    ).rejects.toMatchObject({
      name: 'CloudflarePlaneCapabilityError',
      operation: 'inspectDispatchWorker',
    });
    expect(
      fixture.requests.some(({ url }) => url.includes('/workers/dispatch/')),
    ).toBe(false);
  });

  it.each([
    [
      'database',
      (client: CloudflareProvisioningClient) =>
        client.listWorkerDatabaseAttachments('db'),
    ],
    [
      'r2',
      (client: CloudflareProvisioningClient) =>
        client.listWorkerR2Attachments('bucket'),
    ],
  ] as const)('treats a plain-only first-page namespace 404 as no namespaces for the %s scanner', async (_name, scan) => {
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return apiFailure(404);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const client = plainClient({ fetch: fixture.fetch });

    await expect(scan(client)).resolves.toEqual([]);
    expect(
      fixture.requests.some((request) =>
        request.url.endsWith('/workers/dispatch/namespaces'),
      ),
    ).toBe(true);
  });

  it('keeps ordinary attachments when a plain-only namespace scan is exhaustively empty', async () => {
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) {
        return pageArray([{ id: 'ordinary' }]);
      }
      if (target.pathname.endsWith('/deployments')) {
        return single({
          deployments: [
            {
              versions: [{ version_id: 'v1', percentage: 100 }],
            },
          ],
        });
      }
      if (target.pathname.endsWith('/versions/v1')) {
        return single({
          id: 'v1',
          resources: {
            bindings: [
              { type: 'd1', name: 'DB', database_id: 'db' },
              { type: 'r2_bucket', name: 'BUCKET', bucket_name: 'bucket' },
            ],
          },
        });
      }
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([]);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const client = plainClient({ fetch: fixture.fetch });

    await expect(client.listWorkerDatabaseAttachments('db')).resolves.toEqual([
      { scriptName: 'ordinary', plane: 'ordinary' },
    ]);
    await expect(client.listWorkerR2Attachments('bucket')).resolves.toEqual([
      { scriptName: 'ordinary', plane: 'ordinary' },
    ]);
  });

  it.each([
    403, 404,
  ] as const)('propagates namespace status %s under Workers for Platforms for both scanners', async (status) => {
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return apiFailure(status);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      dispatchNamespace: 'fleet',
      rateCoordinator: testRateCoordinator(),
      fetch: fixture.fetch,
    });

    await expect(
      client.listWorkerDatabaseAttachments('db'),
    ).rejects.toMatchObject({ status });
    await expect(
      client.listWorkerR2Attachments('bucket'),
    ).rejects.toMatchObject({ status });
  });

  it('propagates a plain-only namespace 404 after the first yielded page item', async () => {
    vi.spyOn(BaseNamespaces.prototype, 'list').mockReturnValue(
      (async function* () {
        yield { namespace_name: 'first' };
        throw Object.assign(new Error('later namespace page missing'), {
          status: 404,
        });
      })() as never,
    );
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/namespaces/first/scripts')) {
        return pageArray([]);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });

    await expect(
      plainClient({ fetch: fixture.fetch }).listWorkerDatabaseAttachments('db'),
    ).rejects.toMatchObject({ status: 404 });
  });

  it.each([
    [
      'database',
      (client: CloudflareProvisioningClient) =>
        client.listWorkerDatabaseAttachments('db'),
    ],
    [
      'r2',
      (client: CloudflareProvisioningClient) =>
        client.listWorkerR2Attachments('bucket'),
    ],
  ] as const)('propagates a forbidden plain-only namespace scan for the %s scanner', async (_name, scan) => {
    const fixture = recordingFetch(({ url }) =>
      new URL(url).pathname.endsWith('/workers/scripts')
        ? pageArray([])
        : apiFailure(403),
    );
    const client = plainClient({ fetch: fixture.fetch });
    await expect(scan(client)).rejects.toMatchObject({ status: 403 });
  });

  it('forces SDK logging off even when the environment requests debug logs', async () => {
    const previous = process.env.CLOUDFLARE_LOG;
    process.env.CLOUDFLARE_LOG = 'debug';
    const debug = vi.spyOn(console, 'debug').mockImplementation(() => {});
    try {
      const fixture = recordingFetch(() => pageItems([]));
      await plainClient({ fetch: fixture.fetch }).listOrdinaryWorkerVersions(
        'plain',
      );
      expect(debug).not.toHaveBeenCalled();
    } finally {
      if (previous === undefined) delete process.env.CLOUDFLARE_LOG;
      else process.env.CLOUDFLARE_LOG = previous;
    }
  });

  it('reads undefined-tolerant database and deployment facts', async () => {
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/d1/database')) {
        if (target.searchParams.has('page')) return pageArray([]);
        return pageArray([{ uuid: 'db', name: 'name' }, { uuid: 4 }]);
      }
      if (target.pathname.endsWith('/deployments')) {
        return single({
          deployments: [
            {
              versions: [
                { version_id: 'newest', percentage: '25' },
                { id: 'older' },
              ],
            },
          ],
        });
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const client = plainClient({ fetch: fixture.fetch });
    await expect(client.listOrdinaryWorkerDatabases()).resolves.toEqual([
      { databaseId: 'db', name: 'name' },
      { databaseId: undefined, name: undefined },
    ]);
    await expect(
      client.ordinaryWorkerDeploymentStatus('plain'),
    ).resolves.toEqual({
      versions: [
        { versionId: 'newest', percentage: 25 },
        { versionId: 'older', percentage: undefined },
      ],
    });
  });

  it('models the provider-side D1 name filter', async () => {
    const world = providerWorld();
    world.seedDatabase('target', { databaseId: 'target-id' });
    world.seedDatabase('other', { databaseId: 'other-id' });
    const fixture = recordingFetch(restProjection(world));

    await expect(
      plainClient({ fetch: fixture.fetch }).findDatabase('target'),
    ).resolves.toEqual({ id: 'target-id', name: 'target', created: false });
    expect(
      new URL(fixture.requests[0]?.url ?? '').searchParams.get('name'),
    ).toBe('target');
  });

  it('returns undefined only for provider 404 deployment and version reads', async () => {
    const fixture = recordingFetch(() => apiFailure(404));
    const client = plainClient({ fetch: fixture.fetch });
    await expect(
      client.ordinaryWorkerDeploymentStatus('missing'),
    ).resolves.toBeUndefined();
    await expect(
      client.listOrdinaryWorkerVersions('missing'),
    ).resolves.toBeUndefined();
    await expect(
      client.findOrdinaryWorkerVersion('missing', 'v1'),
    ).resolves.toBeUndefined();
    await expect(
      client.viewOrdinaryWorkerVersion('missing', 'v1'),
    ).rejects.toThrow();
  });

  it.each([
    403, 500,
  ])('propagates provider %s from every deployment and version read', async (status) => {
    const fixture = recordingFetch(() => apiFailure(status));
    const client = plainClient({ fetch: fixture.fetch });
    const operations = [
      () => client.ordinaryWorkerDeploymentStatus('plain'),
      () => client.listOrdinaryWorkerVersions('plain'),
      () => client.findOrdinaryWorkerVersion('plain', 'v1'),
      () => client.viewOrdinaryWorkerVersion('plain', 'v1'),
    ];
    for (const operation of operations) {
      await expect(operation()).rejects.toMatchObject({ status });
    }
  }, 15_000);

  it('paginates versions through a terminal empty page and reserves quota per request', async () => {
    const events: string[] = [];
    const fixture = recordingFetch(({ url }) => {
      const page = Number(new URL(url).searchParams.get('page') ?? '1');
      return page <= 2
        ? pageItems(
            Array.from({ length: 100 }, (_, index) => ({
              id: `${page}-${index}`,
              ...(index === 0
                ? { annotations: { 'workers/tag': `tag-${page}` } }
                : {}),
            })),
          )
        : pageItems([]);
    }, events);
    const client = plainClient({
      fetch: fixture.fetch,
      rateCoordinator: testRateCoordinator(undefined, events),
    });
    const versions = await client.listOrdinaryWorkerVersions('plain');
    expect(versions).toHaveLength(200);
    expect(versions?.[0]).toEqual({ versionId: '1-0', tag: 'tag-1' });
    expect(versions?.[1]).toEqual({ versionId: '1-1', tag: undefined });
    expect(fixture.requests).toHaveLength(3);
    expect(events.filter((event) => event === 'quota:acquire')).toHaveLength(3);
  });

  it('propagates a version-list 404 after the first page yielded', async () => {
    let requests = 0;
    const fixture = recordingFetch(() => {
      requests += 1;
      return requests === 1
        ? pageItems([{ id: 'v1' }], {
            page: 1,
            per_page: 1,
            count: 1,
            total_count: 2,
            total_pages: 2,
          })
        : apiFailure(404);
    });

    await expect(
      plainClient({ fetch: fixture.fetch }).listOrdinaryWorkerVersions('plain'),
    ).rejects.toMatchObject({ status: 404 });
  });

  it('rejects version and inherited secret inventories above their item bounds', async () => {
    const versionFixture = recordingFetch(() =>
      pageItems(Array.from({ length: 5_001 }, (_, id) => ({ id: String(id) }))),
    );
    await expect(
      plainClient({
        fetch: versionFixture.fetch,
      }).listOrdinaryWorkerVersions('plain'),
    ).rejects.toThrow(
      'ordinary Worker version inventory exceeded the supported inventory bound of 5000 items',
    );
    const secretFixture = recordingFetch(() =>
      pageArray(
        Array.from({ length: 10_001 }, (_, id) => ({ name: `secret-${id}` })),
      ),
    );
    await expect(
      plainClient({
        fetch: secretFixture.fetch,
      }).listOrdinaryWorkerSecretNames('plain'),
    ).rejects.toThrow(
      'ordinary Worker secret inventory exceeded the supported inventory bound of 10000 items',
    );
  });

  it('maps every supported binding and all unsupported provider facts', async () => {
    const bindings: readonly unknown[] = [
      { type: 'd1', name: 'D1_ID', id: 'id-only' },
      { type: 'd1', name: 'D1_DATABASE', database_id: 'database-id' },
      {
        type: 'd1',
        name: 'D1_EQUAL',
        id: 'equal-id',
        database_id: 'equal-id',
      },
      {
        type: 'd1',
        name: 'D1_SENTINEL',
        id: '',
        database_id: 'sentinel-id',
      },
      {
        type: 'durable_object_namespace',
        name: 'DO',
        class_name: 'State',
        namespace_id: 'namespace',
        script_name: 'owner',
        dispatch_namespace: 'dispatch',
      },
      {
        type: 'service',
        name: 'SERVICE',
        service: 'upstream',
        entrypoint: 'Admin',
      },
      { type: 'queue', name: 'QUEUE', queue_name: 'jobs' },
      {
        type: 'r2_bucket',
        name: 'R2',
        bucket_name: 'objects',
        jurisdiction: 'eu',
      },
      { type: 'plain_text', name: 'TEXT', text: 'value' },
      { type: 'secret_text', name: 'SECRET' },
      {
        type: 'd1',
        name: 'D1_CONFLICT',
        id: 'left',
        database_id: 'right',
      },
      {
        type: 'durable_object_namespace',
        name: 'DO_ENV',
        class_name: 'State',
        namespace_id: 'namespace',
        environment: 'production',
      },
      {
        type: 'service',
        name: 'SERVICE_ENV',
        service: 'upstream',
        environment: 'production',
      },
      {
        type: 'r2_bucket',
        name: 'R2_JURISDICTION',
        bucket_name: 'objects',
        jurisdiction: 'fedramp-high',
      },
      ...[
        { type: 'd1', name: 'D1_EXTRA', database_id: 'db' },
        {
          type: 'durable_object_namespace',
          name: 'DO_EXTRA',
          class_name: 'State',
          namespace_id: 'namespace',
        },
        { type: 'service', name: 'SERVICE_EXTRA', service: 'upstream' },
        { type: 'queue', name: 'QUEUE_EXTRA', queue_name: 'jobs' },
        { type: 'r2_bucket', name: 'R2_EXTRA', bucket_name: 'objects' },
        { type: 'plain_text', name: 'TEXT_EXTRA', text: 'value' },
        { type: 'secret_text', name: 'SECRET_EXTRA' },
      ].map((binding) => ({ ...binding, extra: true })),
      null,
      { type: ' ', name: 'INVALID' },
      { type: 'ai', name: 'UNSUPPORTED' },
    ];
    const fixture = recordingFetch(() =>
      single({
        id: 'v1',
        annotations: { 'workers/tag': 'tag' },
        resources: { bindings },
      }),
    );
    const viewed = await plainClient({
      fetch: fixture.fetch,
    }).viewOrdinaryWorkerVersion('plain', 'v1');
    expect(viewed.versionId).toBe('v1');
    expect(viewed.tag).toBe('tag');
    expect(viewed.bindings).toEqual<readonly PlainWorkerVersionBinding[]>([
      { type: 'd1', name: 'D1_ID', databaseId: 'id-only' },
      { type: 'd1', name: 'D1_DATABASE', databaseId: 'database-id' },
      { type: 'd1', name: 'D1_EQUAL', databaseId: 'equal-id' },
      { type: 'd1', name: 'D1_SENTINEL', databaseId: 'sentinel-id' },
      {
        type: 'durable-object',
        name: 'DO',
        className: 'State',
        namespaceId: 'namespace',
        scriptName: 'owner',
        dispatchNamespace: 'dispatch',
      },
      {
        type: 'service',
        name: 'SERVICE',
        service: 'upstream',
        entrypoint: 'Admin',
      },
      { type: 'queue-producer', name: 'QUEUE', queueName: 'jobs' },
      {
        type: 'r2-bucket',
        name: 'R2',
        bucketName: 'objects',
        jurisdiction: 'eu',
      },
      { type: 'plain-text', name: 'TEXT', value: 'value' },
      { type: 'secret-text', name: 'SECRET' },
      ...[
        ['D1_CONFLICT', 'd1'],
        ['DO_ENV', 'durable_object_namespace'],
        ['SERVICE_ENV', 'service'],
        ['R2_JURISDICTION', 'r2_bucket'],
        ['D1_EXTRA', 'd1'],
        ['DO_EXTRA', 'durable_object_namespace'],
        ['SERVICE_EXTRA', 'service'],
        ['QUEUE_EXTRA', 'queue'],
        ['R2_EXTRA', 'r2_bucket'],
        ['TEXT_EXTRA', 'plain_text'],
        ['SECRET_EXTRA', 'secret_text'],
      ].map(([name, providerType]) => ({
        type: 'unsupported' as const,
        name,
        providerType: providerType as
          | 'd1'
          | 'durable_object_namespace'
          | 'service'
          | 'queue'
          | 'r2_bucket'
          | 'plain_text'
          | 'secret_text',
        issue: 'malformed-supported-binding' as const,
      })),
      { type: 'unsupported', name: undefined, issue: 'not-object' },
      {
        type: 'unsupported',
        name: 'INVALID',
        providerType: ' ',
        issue: 'invalid-type',
      },
      {
        type: 'unsupported',
        name: 'UNSUPPORTED',
        providerType: 'ai',
        issue: 'unsupported-type',
      },
    ]);
    expect(
      plainWorkerBindingsToProviderShape(viewed.bindings.slice(0, 10)),
    ).toEqual([
      { type: 'd1', name: 'D1_ID', id: 'id-only' },
      { type: 'd1', name: 'D1_DATABASE', id: 'database-id' },
      { type: 'd1', name: 'D1_EQUAL', id: 'equal-id' },
      { type: 'd1', name: 'D1_SENTINEL', id: 'sentinel-id' },
      {
        type: 'durable_object_namespace',
        name: 'DO',
        namespace_id: 'namespace',
        class_name: 'State',
        script_name: 'owner',
        dispatch_namespace: 'dispatch',
      },
      {
        type: 'service',
        name: 'SERVICE',
        service: 'upstream',
        entrypoint: 'Admin',
      },
      { type: 'queue', name: 'QUEUE', queue_name: 'jobs' },
      {
        type: 'r2_bucket',
        name: 'R2',
        bucket_name: 'objects',
        jurisdiction: 'eu',
      },
      { type: 'plain_text', name: 'TEXT', text: 'value' },
      { type: 'secret_text', name: 'SECRET' },
    ]);
    for (const binding of viewed.bindings.filter(
      (binding) =>
        binding.type === 'unsupported' &&
        binding.issue === 'malformed-supported-binding',
    )) {
      expect(() =>
        assertSupportedPlainWorkerBindings([binding], 'pending version'),
      ).toThrow(
        'pending version has an unsupported or malformed provider binding',
      );
      expect(plainWorkerBindingsToProviderShape([binding])).toEqual([
        undefined,
      ]);
    }
    for (const raw of [
      Object.assign(
        { type: 'secret_text', name: 'SYMBOL_SECRET' },
        { [Symbol('extra')]: true },
      ),
      Object.assign(
        { type: 'plain_text', name: 'SYMBOL_TEXT', text: 'value' },
        { [Symbol('extra')]: true },
      ),
    ]) {
      const [normalized] = providerBindingsToPlainWorkerShape([raw]);
      expect(normalized).toMatchObject({
        type: 'unsupported',
        issue: 'malformed-supported-binding',
      });
      expect(() =>
        assertSupportedPlainWorkerBindings(
          [normalized as PlainWorkerVersionBinding],
          'pending version',
        ),
      ).toThrow(
        'pending version has an unsupported or malformed provider binding',
      );
    }
    let bindingAccessorReads = 0;
    const accessorBinding = {
      type: 'service',
      name: 'ACCESSOR',
      get service() {
        bindingAccessorReads += 1;
        return 'upstream';
      },
    };
    expect(providerBindingsToPlainWorkerShape([accessorBinding])).toEqual([
      {
        type: 'unsupported',
        name: 'ACCESSOR',
        providerType: 'service',
        issue: 'malformed-supported-binding',
      },
    ]);
    expect(bindingAccessorReads).toBe(0);
  });

  it('uploads initial and staged versions with one JSON metadata part in provider order', async () => {
    const fixture = recordingFetch(({ method, url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/subdomain') && method === 'GET') {
        return single({ enabled: true, previews_enabled: true });
      }
      if (target.pathname.endsWith('/subdomain'))
        return single({ enabled: false });
      if (target.pathname.endsWith('/versions')) {
        return single({ id: 'v2', resources: {} });
      }
      if (target.pathname.endsWith('/workers/scripts/plain')) {
        return single({ id: 'plain' });
      }
      throw new Error(`unexpected request ${method} ${target.pathname}`);
    });
    const client = plainClient({ fetch: fixture.fetch });
    const initial = await client.prepareOrdinaryWorkerUpload(
      uploadIntent('initial'),
    );
    await fenced(client, () => client.dispatchOrdinaryWorkerUpload(initial));
    const staged = await client.prepareOrdinaryWorkerUpload(
      uploadIntent('staged'),
    );
    await fenced(client, () => client.dispatchOrdinaryWorkerUpload(staged));
    expect(
      fixture.requests.map(({ method, url }) => [
        method,
        new URL(url).pathname,
      ]),
    ).toEqual([
      ['PUT', '/client/v4/accounts/account/workers/scripts/plain'],
      ['POST', '/client/v4/accounts/account/workers/scripts/plain/subdomain'],
      ['GET', '/client/v4/accounts/account/workers/scripts/plain/subdomain'],
      ['POST', '/client/v4/accounts/account/workers/scripts/plain/subdomain'],
      ['POST', '/client/v4/accounts/account/workers/scripts/plain/versions'],
    ]);
    const initialBody = fixture.requests[0]?.body;
    const stagedBody = fixture.requests[4]?.body;
    const bindings = [
      { name: 'TEXT', type: 'plain_text', text: 'value' },
      { name: 'SECRET', type: 'secret_text', text: 'super-secret' },
      { name: 'DB', type: 'd1', database_id: 'db-id' },
      {
        name: 'STATE',
        type: 'durable_object_namespace',
        class_name: 'State',
      },
      { name: 'SERVICE', type: 'service', service: 'upstream' },
      { name: 'QUEUE', type: 'queue', queue_name: 'jobs' },
      { name: 'BUCKET', type: 'r2_bucket', bucket_name: 'objects' },
    ];
    const metadata = {
      main_module: 'worker.js',
      bindings,
      compatibility_date: '2026-08-26',
      compatibility_flags: ['nodejs_compat'],
      limits: { cpu_ms: 42 },
      annotations: { 'workers/tag': 'candidate-tag' },
    };
    expect(fact(initialBody, 'metadata')).toEqual({
      ...metadata,
      migrations: {
        new_tag: 'v1',
        steps: [{ new_sqlite_classes: ['State'] }],
      },
    });
    expect(fact(stagedBody, 'metadata')).toEqual(metadata);
    for (const body of [initialBody, stagedBody]) {
      const decoded = fact(body, 'metadata');
      for (const absent of [
        'keep_bindings',
        'tags',
        'force',
        'bindings_inherit',
      ]) {
        expect(hasFact(decoded, absent)).toBe(false);
      }
    }
    expect(fact(stagedBody, 'files')).toEqual([
      {
        name: 'worker.js',
        type: 'application/javascript+module',
        text: 'export default {}',
      },
      { name: 'data.txt', type: 'text/plain', text: 'data' },
    ]);
  });

  it.each([
    ['initial', 500],
    ['initial', 'throw'],
    ['staged', 500],
    ['staged', 'throw'],
  ] as const)('does not retry a %s ordinary Worker upload after %s and sanitizes the failure', async (mode, failureKind) => {
    const fixture = recordingFetch(({ method, url }) => {
      const target = new URL(url);
      if (
        mode === 'staged' &&
        method === 'GET' &&
        target.pathname.endsWith('/subdomain')
      ) {
        return single({ enabled: false, previews_enabled: false });
      }
      if (failureKind === 'throw') throw new Error('transport failed');
      return apiFailure(500);
    });
    const client = plainClient({ fetch: fixture.fetch });
    const prepared = await client.prepareOrdinaryWorkerUpload(
      uploadIntent(mode),
    );
    const failure = await fenced(client, () =>
      client.dispatchOrdinaryWorkerUpload(prepared),
    ).catch((error: unknown) => error);

    expect(failure).toMatchObject({
      name: 'CloudflareProviderError',
      message: 'Cloudflare Worker upload failed',
    });
    if (failureKind === 500) expect(fact(failure, 'status')).toBe(500);
    else
      expect(fact(fact(failure, 'cause'), 'name')).toBe('APIConnectionError');
    const uploadPath =
      mode === 'initial' ? '/workers/scripts/plain' : '/versions';
    expect(
      fixture.requests.filter(
        ({ method, url }) =>
          method !== 'GET' && new URL(url).pathname.endsWith(uploadPath),
      ),
    ).toHaveLength(1);
  });

  it('redacts secret values from upload API errors', async () => {
    const fixture = recordingFetch(() =>
      apiFailure(400, 'binding echoed super-secret'),
    );
    const client = plainClient({ fetch: fixture.fetch });
    const prepared = await client.prepareOrdinaryWorkerUpload(
      uploadIntent('initial'),
    );
    let failure: unknown;
    try {
      await fenced(client, () => client.dispatchOrdinaryWorkerUpload(prepared));
    } catch (error) {
      failure = error;
    }
    expect(ownSerialization(failure)).not.toContain('super-secret');
    expect(ownSerialization(failure)).toContain('[redacted]');
    expect(fact(failure, 'status')).toBe(400);
  });

  it('terminates a cyclic sanitized transport-error cause chain', async () => {
    const first = new Error('first transport failure');
    const second = new Error('second transport failure');
    const third = new Error('third transport failure');
    let nameReads = 0;
    Object.defineProperty(third, 'name', {
      get: () => {
        nameReads += 1;
        return 'TransportError';
      },
    });
    Object.defineProperty(first, 'cause', { value: second });
    Object.defineProperty(second, 'cause', { value: third });
    Object.defineProperty(third, 'cause', { value: third });

    const failure = await failedInitialUpload(first);
    const chain = boundedErrorCauses(failure);

    expect(chain.errors.map(({ message }) => message)).toEqual([
      expect.any(String),
      'first transport failure',
      'second transport failure',
      'third transport failure',
    ]);
    expect(nameReads).toBe(1);
    expect(chain.terminal).not.toBeInstanceOf(Error);
    expect(Object.getOwnPropertyNames(chain.terminal)).toEqual(['name']);
    expect(fact(chain.terminal, 'name')).toBe('TransportError');
  });

  it('redacts every nested transport-error cause message', async () => {
    const failure = await failedInitialUpload(
      new Error('outer super-secret', {
        cause: new Error('inner super-secret'),
      }),
    );
    const chain = boundedErrorCauses(failure);
    const messages = chain.errors.map(({ message }) => message);

    expect(messages).toContain('outer [redacted]');
    expect(messages).toContain('inner [redacted]');
    expect(messages.every((message) => !message.includes('super-secret'))).toBe(
      true,
    );
  });

  it('sanitizes a non-string nested transport-error message', async () => {
    const transportFailure = new Error('unused');
    Object.defineProperty(transportFailure, 'message', { value: 42 });

    const failure = await failedInitialUpload(transportFailure);
    const chain = boundedErrorCauses(failure);

    expect(chain.errors).toHaveLength(2);
    expect(chain.errors[1]?.message).toBe('');
    expect(chain.terminal).toBeUndefined();
  });

  it('sanitizes a nested transport error whose message accessor throws', async () => {
    const transportFailure = new Error('unused');
    Object.defineProperty(transportFailure, 'message', {
      get: () => {
        throw new Error('super-secret');
      },
    });

    const failure = await failedInitialUpload(
      nestedTransportFailure(transportFailure),
    );
    const chain = boundedErrorCauses(failure);

    expect(chain.errors).toHaveLength(4);
    expect(chain.errors[3]?.message).toBe('');
    expect(chain.terminal).toBeUndefined();
    expect(ownSerialization(failure)).not.toContain('super-secret');
  });

  it('terminates at a nested transport error whose cause accessor throws', async () => {
    const transportFailure = new Error('nested transport failure');
    Object.defineProperty(transportFailure, 'cause', {
      get: () => {
        throw new Error('cause access failed');
      },
    });

    const failure = await failedInitialUpload(
      nestedTransportFailure(transportFailure),
    );
    const chain = boundedErrorCauses(failure);

    expect(chain.errors.map(({ message }) => message)).toEqual([
      'Connection error.',
      'outer transport failure',
      'middle transport failure',
      'nested transport failure',
    ]);
    expect(chain.terminal).toBeUndefined();
  });

  it('reads a nested transport-error message accessor once', async () => {
    let reads = 0;
    const transportFailure = new Error('unused');
    Object.defineProperty(transportFailure, 'message', {
      get: () => {
        reads += 1;
        if (reads > 1) throw new Error('message read twice');
        return 'nested super-secret';
      },
    });

    const failure = await failedInitialUpload(
      nestedTransportFailure(transportFailure),
    );
    const chain = boundedErrorCauses(failure);

    expect(reads).toBe(1);
    expect(chain.errors[3]?.message).toBe('nested [redacted]');
  });

  it('uses an unknown name when nested name or constructor accessors throw', async () => {
    const transportFailure = new Error('nested transport failure');
    Object.defineProperties(transportFailure, {
      constructor: {
        get: () => {
          throw new Error('constructor access failed');
        },
      },
      name: {
        get: () => {
          throw new Error('name access failed');
        },
      },
    });

    const failure = await failedInitialUpload(
      nestedTransportFailure(transportFailure),
    );
    const chain = boundedErrorCauses(failure);

    expect(chain.errors[3]?.name).toBe('unknown');
  });

  it.each([
    [
      'throwing prototype',
      () =>
        new Proxy(
          {},
          {
            getPrototypeOf: () => {
              throw new Error('super-secret');
            },
          },
        ),
    ],
    [
      'revoked proxy',
      () => {
        const { proxy, revoke } = Proxy.revocable({}, {});
        revoke();
        return proxy;
      },
    ],
  ] as const)('terminates at a nested %s cause', async (_kind, cause) => {
    const failure = await failedInitialUpload(
      nestedTransportFailure(cause() as Error),
    );
    const chain = boundedErrorCauses(failure);

    expect(chain.errors).toHaveLength(3);
    expect(chain.terminal).toBeUndefined();
    expect(ownSerialization(failure)).not.toContain('super-secret');
  });

  it('keeps the SDK timeout subclass as a sanitized Error cause', async () => {
    // cloudflare/client.mjs:389 classifies this text as a timeout, then the SDK
    // constructs APIConnectionTimeoutError without the injected error as cause.
    const failure = await failedInitialUpload(new Error('transport timed out'));
    const chain = boundedErrorCauses(failure);

    expect(chain.errors.map(({ name }) => name)).toEqual([
      'APIConnectionTimeoutError',
    ]);
    expect(chain.errors[0]?.message).toBe('Request timed out.');
    expect(chain.terminal).toBeUndefined();
  });

  it('caps a deep sanitized transport-error cause chain at eight levels', async () => {
    let error = new Error('level 12');
    for (let level = 11; level >= 1; level -= 1) {
      error = new Error(`level ${level}`, { cause: error });
    }

    const failure = await failedInitialUpload(error);
    const chain = boundedErrorCauses(failure);

    // Eight sanitized levels = the SDK's APIConnectionError plus injected
    // levels 1-7 (depth 0 is the wrapper).
    expect(chain.errors).toHaveLength(8);
    expect(chain.terminal).not.toBeInstanceOf(Error);
    expect(Object.getOwnPropertyNames(chain.terminal)).toEqual(['name']);
  });

  it('drops nested upload error fields instead of retaining secret-bearing objects', async () => {
    const fixture = recordingFetch(() =>
      Response.json(
        {
          success: false,
          errors: [
            {
              code: { nested: 'super-secret' },
              message: { nested: 'super-secret' },
              detail: 'super-secret',
            },
          ],
          messages: [],
          result: null,
        },
        { status: 400 },
      ),
    );
    const client = plainClient({ fetch: fixture.fetch });
    const prepared = await client.prepareOrdinaryWorkerUpload(
      uploadIntent('initial'),
    );
    const failure = await fenced(client, () =>
      client.dispatchOrdinaryWorkerUpload(prepared),
    ).catch((error: unknown) => error);

    expect(fact(failure, 'errors')).toEqual([{}]);
    expect(ownSerialization(failure)).not.toContain('super-secret');
    expect(ownSerialization(failure)).not.toContain('nested');
  });

  it.each([
    ['a non-array', 'boom'],
    ['a null entry', [null]],
  ] as const)('sanitizes provider errors with %s errors field', async (_kind, errors) => {
    const fixture = recordingFetch(() =>
      Response.json(
        { success: false, errors, messages: [], result: null },
        { status: 500 },
      ),
    );
    const client = plainClient({ fetch: fixture.fetch });
    const prepared = await client.prepareOrdinaryWorkerUpload(
      uploadIntent('initial'),
    );
    const failure = await fenced(client, () =>
      client.dispatchOrdinaryWorkerUpload(prepared),
    ).catch((error: unknown) => error);

    expect(fact(failure, 'status')).toBe(500);
    expect(fact(failure, 'errors')).toEqual([]);
  });

  it('validates deployments before dispatch and sends the exact body once', async () => {
    const fixture = recordingFetch(() => single({ id: 'deployment' }));
    const client = plainClient({ fetch: fixture.fetch });
    const invalid = [
      [],
      [{ versionId: 'v1', percentage: Number.NaN }],
      [{ versionId: 'v1', percentage: 101 }],
      [
        { versionId: 'v1', percentage: 50 },
        { versionId: 'v1', percentage: 50 },
      ],
    ];
    for (const versions of invalid) {
      await expect(
        (async () => {
          const prepared = client.prepareOrdinaryWorkerDeployment(versions);
          await fenced(client, () =>
            client.dispatchOrdinaryWorkerDeployment('plain', prepared),
          );
        })(),
      ).rejects.toThrow();
    }
    expect(fixture.requests).toHaveLength(0);
    const prepared = client.prepareOrdinaryWorkerDeployment([
      { versionId: 'v1', percentage: 25 },
      { versionId: 'v2', percentage: 75 },
    ]);
    await fenced(client, () =>
      client.dispatchOrdinaryWorkerDeployment('plain', prepared),
    );
    expect(fixture.requests).toHaveLength(1);
    expect(fixture.requests[0]?.body).toEqual({
      strategy: 'percentage',
      versions: [
        { version_id: 'v1', percentage: 25 },
        { version_id: 'v2', percentage: 75 },
      ],
    });
  });

  it.each([
    'deployment',
    'create',
    'query',
    'batch',
    'export',
  ] as const)('does not retry a %s failure', async (operation) => {
    const fixture = recordingFetch(() => apiFailure(500));
    const client = plainClient({
      fetch: fixture.fetch,
      exportStore: {
        write: async () => ({ location: 'x', size: 1, sha256: 'x' }),
      },
    });
    const invoke = () => {
      switch (operation) {
        case 'deployment':
          return client.dispatchOrdinaryWorkerDeployment(
            'plain',
            client.prepareOrdinaryWorkerDeployment([
              { versionId: 'v1', percentage: 100 },
            ]),
          );
        case 'create':
          return client.createDatabase('db');
        case 'query':
          return client.queryDatabase('db', 'SELECT 1');
        case 'batch':
          return client.batchDatabase('db', [{ sql: 'SELECT 1' }]);
        case 'export':
          return client.exportDatabase('db');
        default:
          throw new Error(`unknown operation ${operation satisfies never}`);
      }
    };
    await expect(
      fenced(client, async () => {
        await invoke();
      }),
    ).rejects.toBeDefined();
    expect(fixture.requests).toHaveLength(1);
  });

  it('exposes receipt export only for a receipt-capable store', () => {
    const legacy = plainClient({
      exportStore: {
        async write() {
          throw new Error('legacy write must not run');
        },
      },
    });
    expect(legacy.databaseExportReceiptAuthority).toBeUndefined();
    expect(legacy.exportDatabaseReceipt).toBeUndefined();
    expect('databaseExportReceiptAuthority' in legacy).toBe(false);
    expect('exportDatabaseReceipt' in legacy).toBe(false);

    let authorityReads = 0;
    let methodReads = 0;
    const store: DurableDatabaseExportStore = {
      async write() {
        throw new Error('legacy write must not run');
      },
    };
    Object.defineProperties(store, {
      receiptAuthority: {
        configurable: true,
        get() {
          authorityReads += 1;
          return RECEIPT_AUTHORITY;
        },
      },
      writeReceipt: {
        configurable: true,
        get() {
          methodReads += 1;
          return async () => ({
            location: 'memory://receipt',
            size: 1,
            sha256: 'a'.repeat(64),
          });
        },
      },
    });
    const capable = plainClient({ exportStore: store });
    expect(capable.databaseExportReceiptAuthority).toBe(RECEIPT_AUTHORITY);
    expect(typeof capable.exportDatabaseReceipt).toBe('function');
    expect(Object.hasOwn(capable, 'databaseExportReceiptAuthority')).toBe(true);
    expect(Object.hasOwn(capable, 'exportDatabaseReceipt')).toBe(true);
    expect([authorityReads, methodReads]).toEqual([1, 1]);

    for (const malformed of [
      { receiptAuthority: RECEIPT_AUTHORITY },
      { writeReceipt: async () => undefined },
      { receiptAuthority: '', writeReceipt: async () => undefined },
      {
        receiptAuthority: 'x'.repeat(4_097),
        writeReceipt: async () => undefined,
      },
      { receiptAuthority: RECEIPT_AUTHORITY, writeReceipt: 'not-callable' },
    ]) {
      const failure = (() => {
        try {
          plainClient({
            exportStore: {
              async write() {
                throw new Error('legacy write must not run');
              },
              ...malformed,
            } as DurableDatabaseExportStore,
          });
        } catch (error) {
          return error;
        }
        throw new Error('expected malformed receipt capability to fail');
      })();
      expect(failure).toBeInstanceOf(Error);
      expect((failure as Error).message).toBe(
        'database export receipt capability is malformed',
      );
      expect((failure as Error).cause).toBeUndefined();
    }

    for (const property of ['receiptAuthority', 'writeReceipt'] as const) {
      const throwingStore: DurableDatabaseExportStore = {
        async write() {
          throw new Error('legacy write must not run');
        },
      };
      if (property === 'writeReceipt') {
        Object.defineProperty(throwingStore, 'receiptAuthority', {
          configurable: true,
          value: RECEIPT_AUTHORITY,
        });
      }
      Object.defineProperty(throwingStore, property, {
        configurable: true,
        get() {
          throw new Error(`${property} getter must not escape`);
        },
      });
      const failure = (() => {
        try {
          return plainClient({ exportStore: throwingStore });
        } catch (error) {
          return error;
        }
      })();
      expect(failure).toBeInstanceOf(Error);
      expect((failure as Error).message).toBe(
        'database export receipt capability is malformed',
      );
      expect((failure as Error).cause).toBeUndefined();
    }
  });

  it('streams one canonical receipt with independently verified direct integrity', async () => {
    const bytes = 'CREATE TABLE receipt (id TEXT);';
    const expectedSha256 = createHash('sha256').update(bytes).digest('hex');
    const events: string[] = [];
    let storeReceiver: unknown;
    let receivedIntegrity: Promise<unknown> | undefined;
    const store: DurableDatabaseExportStore = {
      receiptAuthority: RECEIPT_AUTHORITY,
      async write() {
        throw new Error('legacy write must not run');
      },
      async writeReceipt(input) {
        storeReceiver = this;
        events.push('store:start');
        expect(input.identity).toEqual(RECEIPT_IDENTITY);
        expect(input.contentLength).toBe(Buffer.byteLength(bytes));
        receivedIntegrity = input.expectedIntegrity;
        expect(input.expectedIntegrity).toBeInstanceOf(Promise);
        const body = Buffer.from(await new Response(input.body).arrayBuffer());
        events.push('store:body');
        const integrity = await input.expectedIntegrity;
        expect(integrity).toEqual({
          size: body.byteLength,
          sha256: expectedSha256,
        });
        return {
          location: 'memory://receipt',
          size: body.byteLength,
          sha256: expectedSha256,
        };
      },
    };
    const fixture = recordingFetch(({ url }) => {
      if (new URL(url).hostname === 'download.example.test') {
        return new Response(bytes, {
          headers: { 'content-length': String(Buffer.byteLength(bytes)) },
        });
      }
      return single({
        status: 'complete',
        result: { signed_url: 'https://download.example.test/receipt.sql' },
      });
    });
    const client = plainClient({ fetch: fixture.fetch, exportStore: store });
    const exportReceipt = client.exportDatabaseReceipt;
    if (!exportReceipt) throw new Error('expected receipt export capability');
    await expect(
      fenced(client, () => exportReceipt(RECEIPT_IDENTITY)),
    ).resolves.toEqual({
      databaseId: RECEIPT_IDENTITY.databaseId,
      location: 'memory://receipt',
      size: Buffer.byteLength(bytes),
      sha256: expectedSha256,
    });
    expect(storeReceiver).toBe(store);
    expect(receivedIntegrity).toBeInstanceOf(Promise);
    expect(events).toEqual(['store:start', 'store:body']);

    fixture.requests.length = 0;
    const authorityFailure = await Promise.resolve()
      .then(() =>
        exportReceipt({
          ...RECEIPT_IDENTITY,
          authority: 'memory://different/receipts/v1',
        }),
      )
      .catch((error: unknown) => error);
    expect(authorityFailure).toBeInstanceOf(Error);
    expect((authorityFailure as Error).message).toBe(
      'database export receipt authority differs from configured authority',
    );
    expect((authorityFailure as Error).cause).toBeUndefined();
    expect(fixture.requests).toHaveLength(0);

    const classified = databaseExportReceiptError('collision');
    let classifiedFieldReads = 0;
    for (const property of [
      'status',
      'name',
      'constructor',
      'cause',
    ] as const) {
      Object.defineProperty(classified, property, {
        configurable: true,
        get() {
          classifiedFieldReads += 1;
          throw new Error(`${property} must not be read`);
        },
      });
    }
    const classifiedStore: DurableDatabaseExportStore = {
      receiptAuthority: RECEIPT_AUTHORITY,
      async write() {
        throw classified;
      },
      async writeReceipt() {
        throw classified;
      },
    };
    const classifiedClient = plainClient({
      fetch: fixture.fetch,
      exportStore: classifiedStore,
    });
    const classifiedExportReceipt = classifiedClient.exportDatabaseReceipt;
    if (!classifiedExportReceipt) {
      throw new Error('expected classified receipt export capability');
    }
    await expect(
      fenced(classifiedClient, () => classifiedExportReceipt(RECEIPT_IDENTITY)),
    ).rejects.toBe(classified);
    expect(classifiedFieldReads).toBe(0);
    for (const property of [
      'status',
      'name',
      'constructor',
      'cause',
    ] as const) {
      Reflect.deleteProperty(classified, property);
    }
    const legacyFailure = await fenced(classifiedClient, () =>
      classifiedClient.exportDatabase(RECEIPT_IDENTITY.databaseId),
    ).catch((error: unknown) => error);
    expect(legacyFailure).not.toBe(classified);
    expect(String((legacyFailure as Error).message)).toContain('D1 export for');

    const forged = new Error(classified.message);
    let forgedStatusReads = 0;
    Object.defineProperty(forged, 'status', {
      configurable: true,
      get() {
        forgedStatusReads += 1;
        throw new Error('forged status must be sanitized');
      },
    });
    const forgedStore: DurableDatabaseExportStore = {
      receiptAuthority: RECEIPT_AUTHORITY,
      async write() {
        throw new Error('legacy write must not run');
      },
      async writeReceipt() {
        throw forged;
      },
    };
    const forgedClient = plainClient({
      fetch: fixture.fetch,
      exportStore: forgedStore,
    });
    const forgedExportReceipt = forgedClient.exportDatabaseReceipt;
    if (!forgedExportReceipt) {
      throw new Error('expected forged receipt export capability');
    }
    const forgedFailure = await fenced(forgedClient, () =>
      forgedExportReceipt(RECEIPT_IDENTITY),
    ).catch((error: unknown) => error);
    expect(forgedFailure).not.toBe(forged);
    expect((forgedFailure as Error).message).toContain('D1 export for');
    expect(forgedStatusReads).toBe(1);

    const dishonestStore: DurableDatabaseExportStore = {
      receiptAuthority: RECEIPT_AUTHORITY,
      async write() {
        throw new Error('legacy write must not run');
      },
      async writeReceipt(input) {
        await new Response(input.body).arrayBuffer();
        const expected = await input.expectedIntegrity;
        return {
          location: 'memory://dishonest-receipt',
          size: expected.size + 1,
          sha256: expected.sha256,
        };
      },
    };
    const dishonestClient = plainClient({
      fetch: fixture.fetch,
      exportStore: dishonestStore,
    });
    const dishonestExportReceipt = dishonestClient.exportDatabaseReceipt;
    if (!dishonestExportReceipt) {
      throw new Error('expected dishonest receipt export capability');
    }
    const dishonestFailure = await fenced(dishonestClient, () =>
      dishonestExportReceipt(RECEIPT_IDENTITY),
    ).catch((error: unknown) => error);
    expect((dishonestFailure as Error).message).toContain(
      'committed durable D1 export integrity differs from the download',
    );

    for (const mode of ['synchronous', 'deferred'] as const) {
      const lowerFailure = databaseExportReceiptError('collision');
      const cancellationReasons: unknown[] = [];
      const rejectingStore: DurableDatabaseExportStore = {
        receiptAuthority: RECEIPT_AUTHORITY,
        async write() {
          throw new Error('legacy write must not run');
        },
        writeReceipt(input): Promise<never> {
          Object.defineProperty(input.body, 'cancel', {
            configurable: true,
            value(reason: unknown) {
              cancellationReasons.push(reason);
              return Promise.resolve();
            },
          });
          if (mode === 'synchronous') throw lowerFailure;
          return Promise.resolve().then(() => {
            throw lowerFailure;
          });
        },
      };
      const rejectingClient = plainClient({
        fetch: fixture.fetch,
        exportStore: rejectingStore,
      });
      const rejectingExportReceipt = rejectingClient.exportDatabaseReceipt;
      if (!rejectingExportReceipt) {
        throw new Error('expected rejecting receipt export capability');
      }
      await expect(
        fenced(rejectingClient, () => rejectingExportReceipt(RECEIPT_IDENTITY)),
      ).rejects.toBe(lowerFailure);
      expect(cancellationReasons).toEqual([lowerFailure]);
    }
  });

  it.each([
    [404, 'absent'],
    [200, 'deleted'],
  ] satisfies ReadonlyArray<
    readonly [number, 'absent' | 'deleted']
  >)('classifies ordinary Worker delete status %s as %s', async (status, result) => {
    const fixture = recordingFetch(() =>
      status === 404 ? apiFailure(404) : single({}),
    );
    const client = plainClient({ fetch: fixture.fetch });
    await expect(
      fenced(client, () => client.deleteOrdinaryWorkerScript('plain')),
    ).resolves.toBe(result);
  });

  it.each([
    403, 500,
  ])('propagates ordinary Worker delete status %s', async (status) => {
    const fixture = recordingFetch(() => apiFailure(status));
    const client = plainClient({ fetch: fixture.fetch });
    await expect(
      fenced(client, () => client.deleteOrdinaryWorkerScript('plain')),
    ).rejects.toThrow();
  });

  it('rejects quota acquisition for reads and writes before fetch', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>();
    const client = plainClient({
      fetch,
      rateCoordinator: {
        acquire: async () => {
          throw new Error('quota unavailable');
        },
      },
    });
    let readFailure: unknown;
    let writeFailure: unknown;
    try {
      await client.listOrdinaryWorkerDatabases();
    } catch (error) {
      readFailure = error;
    }
    try {
      await fenced(client, () => client.deleteOrdinaryWorkerScript('plain'));
    } catch (error) {
      writeFailure = error;
    }
    expect(ownSerialization(readFailure)).toContain('quota unavailable');
    expect(ownSerialization(writeFailure)).toContain('quota unavailable');
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    [
      'invalid-url',
      () =>
        single({
          status: 'complete',
          result: {
            signed_url:
              'ht!tps://download.example.test/private/path?token=secret',
          },
        }),
      undefined,
    ],
    [
      'non-https-url',
      () =>
        single({
          status: 'complete',
          result: {
            signed_url:
              'http://download.example.test/private/path?token=secret',
          },
        }),
      'export returned a non-HTTPS download URL',
    ],
    [
      'fetch-rejection',
      () =>
        single({
          status: 'complete',
          result: {
            signed_url:
              'https://download.example.test/private/path?token=secret',
          },
        }),
      undefined,
    ],
    [
      'status-error',
      () =>
        single({
          status: 'error',
          error: 'https://download.example.test/private/path?token=secret',
        }),
      "provider status 'error'",
    ],
    [
      'sdk-error',
      () =>
        apiFailure(
          500,
          'https://download.example.test/private/path?token=secret',
        ),
      'HTTP 500',
    ],
    ['no-bookmark', () => single({ status: 'pending' }), 'no polling bookmark'],
  ] as const)('redacts a %s signed export URL failure', async (_kind, providerResponse, expectedFragment) => {
    const signedUrl = 'https://download.example.test/private/path?token=secret';
    const fixture = recordingFetch(async ({ url, headers, redirect }) => {
      if (url.startsWith('https://download.example.test/')) {
        expect(headers.has('authorization')).toBe(false);
        expect(redirect).toBe('error');
        const error = new Error(signedUrl);
        error.name = signedUrl;
        throw error;
      }
      return providerResponse();
    });
    const client = plainClient({
      fetch: fixture.fetch,
      exportStore: {
        write: async () => ({ location: 'x', size: 1, sha256: 'x' }),
      },
    });
    let failure: unknown;
    try {
      await fenced(client, () => client.exportDatabase('db'));
    } catch (error) {
      failure = error;
    }
    expect(ownSerialization(failure)).not.toContain('/private/path');
    expect(ownSerialization(failure)).not.toContain('token=secret');
    if (expectedFragment) {
      expect(String(fact(failure, 'message'))).toContain(expectedFragment);
    }
  });

  it('redacts a signed export URL from a throwing status accessor', async () => {
    const signedUrl = 'https://download.example.test/private/path?token=secret';
    const fixture = recordingFetch(({ url }) => {
      if (url === signedUrl) {
        const error = new Error('download failed');
        Object.defineProperty(error, 'status', {
          get: () => {
            throw new Error(`leak ${signedUrl}`);
          },
        });
        return Promise.reject(error);
      }
      return single({
        status: 'complete',
        result: { signed_url: signedUrl },
      });
    });
    const client = plainClient({
      fetch: fixture.fetch,
      exportStore: {
        write: async () => ({ location: 'x', size: 1, sha256: 'x' }),
      },
    });
    const failure = await fenced(client, () =>
      client.exportDatabase('db'),
    ).catch((error: unknown) => error);

    expect(ownSerialization(failure)).not.toContain('/private/path');
    expect(ownSerialization(failure)).not.toContain('token=secret');
    expect(fact(failure, 'message')).toBe(
      "D1 export for 'db' failed after 1 poll(s)",
    );
    expect(fact(failure, 'cause')).toEqual({ name: 'unknown' });
  });

  it('reports exhaustion of the D1 export poll budget safely', async () => {
    vi.useFakeTimers();
    try {
      let poll = 0;
      const fixture = recordingFetch(() => {
        poll += 1;
        return single({ status: 'pending', at_bookmark: `bookmark-${poll}` });
      });
      const client = plainClient({
        fetch: fixture.fetch,
        exportStore: {
          write: async () => ({ location: 'x', size: 1, sha256: 'x' }),
        },
      });
      const failurePromise = fenced(client, () => client.exportDatabase('db'))
        .then(() => undefined)
        .catch((error: unknown) => error);

      await vi.runAllTimersAsync();
      const failure = await failurePromise;

      expect(String(fact(failure, 'message'))).toContain(
        'export did not complete within the poll budget',
      );
      expect(ownSerialization(failure)).not.toContain('bookmark-');
      expect(fixture.requests).toHaveLength(120);
    } finally {
      vi.useRealTimers();
    }
  });

  it('executes a nested same-fence request with one transport assertion', async () => {
    const fixture = recordingFetch(() => single({}));
    const client = plainClient({ fetch: fixture.fetch });
    let executions = 0;
    const assertOwned = vi.fn(async () => {});
    const fence = { mutationLeaseTtlMs: 15 * 60_000, assertOwned };
    await client.withMutationFence(fence, () =>
      client.withMutationFence(fence, async () => {
        executions += 1;
        await client.deleteOrdinaryWorkerScript('plain');
      }),
    );
    expect(executions).toBe(1);
    expect(fixture.requests).toHaveLength(1);
    expect(assertOwned).toHaveBeenCalledTimes(1);
  });

  it('bounds WFP dispatch-script items across cursor pages', async () => {
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      const authority = zoneAuthorityResponse(target, []);
      if (authority) return authority;
      const pathname = target.pathname;
      if (pathname.endsWith('/workers/dispatch/namespaces/fleet/scripts')) {
        const offset = target.searchParams.has('cursor') ? 5_000 : 0;
        const count = offset === 0 ? 5_000 : 5_001;
        return Response.json({
          success: true,
          errors: [],
          messages: [],
          result: Array.from({ length: count }, (_, index) => ({
            id: `script-${offset + index}`,
            tags: [],
          })),
          result_info: offset === 0 ? { cursor: 'next-page' } : {},
        });
      }
      if (
        pathname.endsWith('/workers/domains') ||
        pathname.endsWith('/workers/scripts') ||
        pathname.endsWith('/d1/database') ||
        pathname.endsWith('/workers/durable_objects/namespaces')
      ) {
        return pageArray([]);
      }
      if (target.searchParams.has('page')) return pageArray([]);
      throw new Error(`unexpected Cloudflare request: ${target.href}`);
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: fixture.fetch,
    });

    await expect(
      client.collectFleetInventory({
        databaseNamePrefix: 'fleet-',
        scriptNamePrefix: 'fleet-',
        includeDispatchNamespace: true,
      }),
    ).rejects.toThrow(
      'dispatch script inventory exceeded the supported inventory bound of 10000 items',
    );
  });

  it('keeps WFP D1 create and query at one provider attempt', async () => {
    for (const operation of ['create', 'query']) {
      const fixture = recordingFetch(() => apiFailure(500));
      const client = new CloudflareProvisioningClient({
        accountId: 'account',
        apiToken: 'token',
        dispatchNamespace: 'fleet',
        rateCoordinator: testRateCoordinator(),
        fetch: fixture.fetch,
      });
      await expect(
        fenced(client, async () => {
          await (operation === 'create'
            ? client.createDatabase('db')
            : client.queryDatabase('db', 'SELECT 1'));
        }),
      ).rejects.toBeDefined();
      expect(fixture.requests).toHaveLength(1);
    }
  });
});
