// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { CloudflareApiPlainWorkerBackend } from '../src/cloudflare-api-plain-worker-backend.js';
import { CloudflareApiPlainWorkerProvisioningApi } from '../src/cloudflare-api-plain-worker-provisioning-api.js';
import { CloudflareProvisioningClient } from '../src/cloudflare-client.js';
import { initialWorkerAttachmentScan } from '../src/cloudflare-worker-attachment-scan-state.js';
import type {
  DecommissionAttachmentScanInput,
  DecommissionAttachmentScanResult,
  ExternalMutationFence,
  PlainWorkerUploadIntent,
} from '../src/types.js';
import {
  type CloudflareFixtureHandler,
  deferred,
  recordingFetch,
  restProjection,
  single,
  testRateCoordinator,
} from './fixtures/cloudflare-fetch-fixture.js';
import { errorChain } from './fixtures/plain-worker-harnesses.js';
import {
  memoryStore,
  mutationFence,
  rejectedValue,
} from './fixtures/plain-worker-port-probe.js';
import { providerWorld } from './fixtures/provider-world.js';

function uploadIntent(mode: 'initial' | 'staged'): PlainWorkerUploadIntent {
  const base = {
    scriptName: 'acme-production',
    candidateTag: 'a'.repeat(64),
    mainModule: 'worker.js',
    modules: [
      {
        name: 'worker.js',
        content: 'export default { fetch() {} }',
      },
    ],
    compatibilityDate: '2026-08-26',
    compatibilityFlags: ['nodejs_compat'],
    bindings: {
      plainText: [{ name: 'DEPLOYMENT_TENANT', value: 'acme' }],
      secrets: [{ name: 'SECRET', value: 'secret-value' }],
      d1: [
        {
          name: 'DB',
          databaseId: 'database-1',
          databaseName: 'acme-production',
        },
      ],
      durableObjects: [],
      services: [],
      queueProducers: [],
      r2Buckets: [],
    },
    limits: { cpuMs: 30_000 },
    publicAccess: {
      workersDevEnabled: true,
      previewUrlsEnabled: false,
    },
  } as const;
  return mode === 'initial'
    ? { ...base, mode, durableObjectMigrations: [] }
    : { ...base, mode };
}

function subject(
  handler: CloudflareFixtureHandler,
  options: {
    readonly events?: string[];
    readonly exportStore?: ConstructorParameters<
      typeof CloudflareProvisioningClient
    >[0]['exportStore'];
    readonly formDataProbe?: 'unsupported';
    readonly concurrency?: number;
  } = {},
) {
  const events = options.events ?? [];
  const fixture = recordingFetch(handler, events, {
    formDataProbe: options.formDataProbe,
  });
  const client = new CloudflareProvisioningClient({
    accountId: 'account',
    apiToken: 'token',
    plane: 'plain-worker',
    rateCoordinator: testRateCoordinator(undefined, events),
    fetch: fixture.fetch,
    requestTimeoutMs: 1_000,
    exportStore: options.exportStore,
    concurrency: options.concurrency,
  });
  return {
    api: new CloudflareApiPlainWorkerProvisioningApi({ client }),
    client,
    fixture,
  };
}

function emptyScriptWorld(
  subdomain = { enabled: false, previewsEnabled: false },
) {
  const world = providerWorld();
  world.seedScript('acme-production', {
    versions: [],
    subdomain,
  });
  return world;
}

function ownedFence(events?: string[]): ExternalMutationFence {
  return mutationFence(
    vi.fn(async () => {
      events?.push('assertOwned');
    }),
  );
}

function outcomeOperations(
  api: CloudflareApiPlainWorkerProvisioningApi,
  fence: ExternalMutationFence,
): Record<
  'createDatabase' | 'uploadCandidate' | 'createDeployment',
  () => Promise<unknown>
> {
  return {
    createDatabase: () => api.createDatabase('acme-production', fence),
    uploadCandidate: () => api.uploadCandidate(uploadIntent('initial'), fence),
    createDeployment: () =>
      api.createDeployment(
        'acme-production',
        [{ versionId: 'version-1', percentage: 100 }],
        fence,
      ),
  };
}

describe('CloudflareApiPlainWorkerProvisioningApi', () => {
  it('projects the REST world through the provider-neutral read port', async () => {
    const world = providerWorld();
    world.seedDatabase('acme-production', { databaseId: 'database-1' });
    world.seedScript('acme-production', {
      versions: [
        {
          versionId: 'version-1',
          tag: 'a'.repeat(64),
          bindings: [
            { type: 'd1', name: 'DB', database_id: 'database-1' },
            { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'acme' },
          ],
          mainModule: 'worker.js',
          modules: [{ name: 'worker.js', content: 'export default {}' }],
        },
      ],
      deployment: [{ versionId: 'version-1', percentage: 100 }],
      subdomain: { enabled: true, previewsEnabled: false },
    });
    const { api } = subject(restProjection(world));

    await expect(api.listDatabases()).resolves.toEqual([
      { databaseId: 'database-1', name: 'acme-production' },
    ]);
    await expect(api.getDatabase('database-1')).resolves.toEqual({
      id: 'database-1',
      name: 'acme-production',
      created: false,
    });
    await expect(api.deploymentStatus('acme-production')).resolves.toEqual({
      versions: [{ versionId: 'version-1', percentage: 100 }],
    });
    await expect(api.listVersions('acme-production')).resolves.toEqual([
      { versionId: 'version-1', tag: 'a'.repeat(64) },
    ]);
    await expect(
      api.viewVersion('acme-production', 'version-1'),
    ).resolves.toMatchObject({
      versionId: 'version-1',
      tag: 'a'.repeat(64),
      bindings: [
        { type: 'd1', name: 'DB', databaseId: 'database-1' },
        { type: 'plain-text', name: 'DEPLOYMENT_TENANT', value: 'acme' },
      ],
    });
    await expect(
      api.findVersion('acme-production', 'absent'),
    ).resolves.toBeUndefined();
  });

  it('forwards a D1 database name filter without changing unfiltered requests', async () => {
    const world = providerWorld();
    world.seedDatabase('acme-production', { databaseId: 'database-1' });
    world.seedDatabase('other-database', { databaseId: 'database-2' });
    const { api, fixture } = subject(restProjection(world));

    await expect(api.listDatabases()).resolves.toEqual([
      { databaseId: 'database-1', name: 'acme-production' },
      { databaseId: 'database-2', name: 'other-database' },
    ]);
    await expect(
      api.listDatabases({ name: 'acme-production' }),
    ).resolves.toEqual([{ databaseId: 'database-1', name: 'acme-production' }]);
    const firstPageListUrls = fixture.requests
      .map(({ url }) => new URL(url))
      .filter(
        (url) =>
          url.pathname.endsWith('/d1/database') &&
          !url.searchParams.has('page'),
      );
    expect(
      firstPageListUrls.map((url) => url.searchParams.get('name')),
    ).toEqual([null, 'acme-production']);
  });

  it.each([
    'createDatabase',
    'uploadCandidate',
    'createDeployment',
  ] as const)('rejects %s fence loss before dispatch', async (operation) => {
    const world = emptyScriptWorld();
    const { api, fixture } = subject(restProjection(world));
    const denied = new Error('lease lost');
    const fence = mutationFence(vi.fn(async () => Promise.reject(denied)));
    const selected = outcomeOperations(api, fence)[operation];

    await expect(selected()).rejects.toBe(denied);
    expect(fixture.requests).toHaveLength(0);
  });

  it.each([
    'createDatabase',
    'uploadCandidate',
    'createDeployment',
  ] as const)('rejects %s timeout validation instead of returning a failed outcome', async (operation) => {
    const world = emptyScriptWorld();
    const { api, fixture } = subject(restProjection(world));
    const fence = { mutationLeaseTtlMs: 0, assertOwned: vi.fn(async () => {}) };
    const selected = outcomeOperations(api, fence)[operation];

    await expect(selected()).rejects.toThrow(
      'external mutation fence lease TTL must be positive',
    );
    expect(fixture.requests).toHaveLength(0);
  });

  it('rejects upload and deployment preparation failures before dispatch', async () => {
    const world = emptyScriptWorld();
    const { api, fixture } = subject(restProjection(world));
    const malformed = {
      ...uploadIntent('initial'),
      modules: [{ name: 'worker.js', content: null }],
    } as unknown as PlainWorkerUploadIntent;
    const malformedBinding = {
      ...uploadIntent('initial'),
      bindings: {
        ...uploadIntent('initial').bindings,
        plainText: [null],
      },
    } as unknown as PlainWorkerUploadIntent;

    await expect(api.uploadCandidate(malformed, ownedFence())).rejects.toThrow(
      'valid upload data',
    );
    await expect(
      api.uploadCandidate(malformedBinding, ownedFence()),
    ).rejects.toThrow(TypeError);
    await expect(
      api.createDeployment(
        'acme-production',
        [{ versionId: 'version-1', percentage: Number.NaN }],
        ownedFence(),
      ),
    ).rejects.toThrow('finite values from 0 to 100');
    expect(fixture.requests).toHaveLength(0);
    expect(fixture.probes).toHaveLength(0);
  });

  it.each([
    ['initial', 0],
    ['staged', 1],
  ] as const)('rejects a %s upload when multipart preparation fails', async (mode, expectedReads) => {
    const world = emptyScriptWorld({
      enabled: true,
      previewsEnabled: false,
    });
    const { api, fixture } = subject(restProjection(world), {
      formDataProbe: 'unsupported',
    });

    await expect(
      api.uploadCandidate(uploadIntent(mode), ownedFence()),
    ).rejects.toThrow(TypeError);
    expect(fixture.requests).toHaveLength(expectedReads);
    expect(
      fixture.requests.filter(({ method }) => method !== 'GET'),
    ).toHaveLength(0);
    if (mode === 'staged') {
      expect(
        fixture.requests.map(({ method, url }) => [
          method,
          new URL(url).pathname,
        ]),
      ).toEqual([
        [
          'GET',
          '/client/v4/accounts/account/workers/scripts/acme-production/subdomain',
        ],
      ]);
    }
  });

  it('rejects when the fetch wrapper fails before issuing the staged read', async () => {
    const providerFixture = recordingFetch(restProjection(emptyScriptWorld()));
    const providerIssued = vi.fn(providerFixture.fetch);
    const transportFailure = new TypeError(
      'transport refused before issuing request',
    );
    const beforeIssue = (): void => {
      throw transportFailure;
    };
    const wrapperInvoked = vi.fn<typeof fetch>((input, init) => {
      beforeIssue();
      return providerIssued(input, init);
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      plane: 'plain-worker',
      rateCoordinator: testRateCoordinator(),
      fetch: wrapperInvoked,
      requestTimeoutMs: 1_000,
    });
    const api = new CloudflareApiPlainWorkerProvisioningApi({ client });

    const failure = await api
      .uploadCandidate(uploadIntent('staged'), ownedFence())
      .catch((error: unknown) => error);

    expect(errorChain(failure)).toContain(transportFailure.message);
    // Reads keep the SDK's two retries; only the listed non-idempotent
    // mutations disable them.
    expect(wrapperInvoked).toHaveBeenCalledTimes(3);
    expect(providerIssued).not.toHaveBeenCalled();
    expect(providerFixture.requests).toHaveLength(0);
  });

  it('returns failed when a staged public-access write fails', async () => {
    const { api, fixture } = subject(({ method, url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/subdomain') && method === 'GET') {
        return single({ enabled: false, previews_enabled: false });
      }
      if (target.pathname.endsWith('/subdomain') && method === 'POST') {
        return Response.json(
          { success: false, errors: [{ code: 1, message: 'failed' }] },
          { status: 500 },
        );
      }
      throw new Error(`unexpected request ${method} ${target.pathname}`);
    });

    await expect(
      api.uploadCandidate(uploadIntent('staged'), ownedFence()),
    ).resolves.toMatchObject({ status: 'failed' });
    // Idempotent subdomain writes keep the SDK's two retries; only the listed
    // non-idempotent mutations disable retries.
    expect(
      fixture.requests.filter(
        ({ method, url }) =>
          method === 'POST' && new URL(url).pathname.endsWith('/subdomain'),
      ),
    ).toHaveLength(3);
    expect(
      fixture.requests.some(({ url }) =>
        new URL(url).pathname.endsWith('/versions'),
      ),
    ).toBe(false);
  });

  it.each([
    'initial',
    'staged',
  ] as const)('returns failed and redacts a dispatched %s upload failure', async (mode) => {
    const secret = 'secret-value';
    const { api } = subject(({ method }) =>
      method === 'GET'
        ? single({ enabled: true, previews_enabled: false })
        : Response.json(
            {
              success: false,
              errors: [{ code: 1, message: `echoed ${secret}` }],
            },
            { status: 500 },
          ),
    );

    const outcome = await api.uploadCandidate(uploadIntent(mode), ownedFence());
    expect(outcome).toMatchObject({ status: 'failed' });
    expect(JSON.stringify(outcome)).not.toContain(secret);
    expect(JSON.stringify(outcome)).toContain('[redacted]');
  });

  it.each([
    'createDatabase',
    'uploadCandidate',
    'createDeployment',
  ] as const)('returns a failed value after a dispatched %s failure', async (operation) => {
    const { api, fixture } = subject(async () =>
      Response.json(
        { success: false, errors: [{ code: 1, message: 'failed' }] },
        { status: 500 },
      ),
    );
    const selected = outcomeOperations(api, ownedFence())[operation];
    const result = await selected();

    expect(result).toMatchObject({ status: 'failed' });
    expect(
      Boolean(result && typeof result === 'object' && 'cleanup' in result),
    ).toBe(operation === 'uploadCandidate');
    if (result && typeof result === 'object' && 'cleanup' in result) {
      expect(result.cleanup).toEqual({ status: 'succeeded' });
    }
    expect(fixture.requests).toHaveLength(1);
  });

  it.each([
    ['createDatabase', 'createDatabase'],
    ['uploadCandidate initial', 'uploadInitial'],
    ['uploadCandidate staged', 'uploadStaged'],
    ['createDeployment', 'createDeployment'],
  ] as const)('rejects %s when the transport fence fails before mutation dispatch', async (_label, operation) => {
    const world = emptyScriptWorld({
      enabled: true,
      previewsEnabled: false,
    });
    const { api, fixture } = subject(restProjection(world));
    const fenceError = new Error('transport lease assertion failed');
    let assertions = 0;
    const assertOwned = vi.fn(async () => {
      assertions += 1;
      if (assertions === 2) throw fenceError;
    });
    const fence = mutationFence(assertOwned);
    const invoke = (): Promise<unknown> => {
      switch (operation) {
        case 'createDatabase':
          return api.createDatabase('new-database', fence);
        case 'uploadInitial':
          return api.uploadCandidate(uploadIntent('initial'), fence);
        case 'uploadStaged':
          return api.uploadCandidate(uploadIntent('staged'), fence);
        case 'createDeployment':
          return api.createDeployment(
            'acme-production',
            [{ versionId: 'version-1', percentage: 100 }],
            fence,
          );
      }
    };

    const failure = await invoke().catch((error: unknown) => error);
    expect(errorChain(failure)).toContain(fenceError.message);
    expect(assertOwned).toHaveBeenCalledTimes(2);
    expect(
      fixture.requests.filter(
        ({ method }) => method !== 'GET' && method !== 'HEAD',
      ),
    ).toHaveLength(0);
    if (operation === 'uploadStaged') {
      expect(fixture.requests).toHaveLength(1);
      expect(fixture.requests[0]).toMatchObject({ method: 'GET' });
    } else {
      expect(fixture.requests).toHaveLength(0);
    }
  });

  it.each([
    'createDeployment',
    'createDatabase',
  ] as const)('classifies a queued dispatched %s failure', async (operation) => {
    const heldResponse = deferred<Response>();
    const heldStarted = deferred<void>();
    const { api, client } = subject(
      ({ method }) => {
        if (method === 'GET') {
          heldStarted.resolve();
          return heldResponse.promise;
        }
        return Response.json(
          { success: false, errors: [{ code: 1, message: 'failed' }] },
          { status: 500 },
        );
      },
      { concurrency: 1 },
    );
    const held = client.findOrdinaryWorkerVersion('hold', 'held-version');
    await heldStarted.promise;
    const queued =
      operation === 'createDatabase'
        ? api.createDatabase('new-database', ownedFence())
        : api.createDeployment(
            'acme-production',
            [{ versionId: 'version-1', percentage: 100 }],
            ownedFence(),
          );
    heldResponse.resolve(
      single({ id: 'held-version', resources: { bindings: [] } }),
    );

    await expect(held).resolves.toMatchObject({ versionId: 'held-version' });
    await expect(queued).resolves.toMatchObject({ status: 'failed' });
  });

  it('returns failed when a staged write precedes multipart preparation failure', async () => {
    const { api, fixture } = subject(
      ({ method, url }) => {
        const target = new URL(url);
        if (target.pathname.endsWith('/subdomain') && method === 'GET') {
          return single({ enabled: false, previews_enabled: false });
        }
        if (target.pathname.endsWith('/subdomain') && method === 'POST') {
          return single({ enabled: true, previews_enabled: false });
        }
        throw new Error(`unexpected request ${method} ${target.pathname}`);
      },
      { formDataProbe: 'unsupported' },
    );

    await expect(
      api.uploadCandidate(uploadIntent('staged'), ownedFence()),
    ).resolves.toMatchObject({ status: 'failed' });
    expect(
      fixture.requests.map(({ method, url }) => [
        method,
        new URL(url).pathname,
      ]),
    ).toEqual([
      [
        'GET',
        '/client/v4/accounts/account/workers/scripts/acme-production/subdomain',
      ],
      [
        'POST',
        '/client/v4/accounts/account/workers/scripts/acme-production/subdomain',
      ],
    ]);
  });

  it('pins the pre-assert, quota, transport re-attestation, and request order', async () => {
    const events: string[] = [];
    const world = providerWorld();
    const { api } = subject(restProjection(world), { events });

    await expect(
      api.createDatabase('acme-production', ownedFence(events)),
    ).resolves.toEqual({ status: 'succeeded' });
    expect(events).toEqual([
      'assertOwned',
      'quota:acquire',
      'assertOwned',
      'request:/client/v4/accounts/account/d1/database',
    ]);
  });

  it('runs deletion and export requests inside the fenced client scope', async () => {
    const world = emptyScriptWorld();
    world.seedDatabase('acme-production', { databaseId: 'database-1' });
    const projected = restProjection(world);
    const handler: CloudflareFixtureHandler = async (request) => {
      const url = new URL(request.url);
      if (url.hostname === 'download.example.test') {
        expect(request.headers.has('authorization')).toBe(false);
        return new Response('CREATE TABLE example (id TEXT);');
      }
      if (url.pathname.endsWith('/d1/database/database-1/export')) {
        return single({
          status: 'complete',
          result: {
            signed_url: 'https://download.example.test/export.sql?sig=secret',
          },
        });
      }
      return projected(request);
    };
    const { api } = subject(handler, { exportStore: memoryStore() });
    const fence = ownedFence();

    await expect(
      api.exportDatabase({ id: 'database-1', name: 'acme-production' }, fence),
    ).resolves.toMatchObject({ location: 'memory://export', size: 31 });
    await expect(
      api.deleteWorkerScript('acme-production', fence),
    ).resolves.toBe('deleted');
    await expect(
      api.deleteDatabaseFenced('database-1', fence),
    ).resolves.toBeUndefined();
  });

  it('classifies only provider 404 as an absent Worker', async () => {
    const absent = subject(async () =>
      Response.json({ success: false, errors: [] }, { status: 404 }),
    );
    await expect(
      absent.api.deleteWorkerScript('absent', ownedFence()),
    ).resolves.toBe('absent');

    const forbidden = subject(async () =>
      Response.json({ success: false, errors: [] }, { status: 403 }),
    );
    await expect(
      forbidden.api.deleteWorkerScript('forbidden', ownedFence()),
    ).rejects.toMatchObject({ status: 403 });
  });

  it('propagates durable export integrity failures', async () => {
    const handler: CloudflareFixtureHandler = async (request) => {
      const url = new URL(request.url);
      if (url.hostname === 'download.example.test') return new Response('sql');
      return single({
        status: 'complete',
        result: { signed_url: 'https://download.example.test/export.sql' },
      });
    };
    const { api } = subject(handler, {
      exportStore: {
        async write() {
          return {
            location: 'memory://bad',
            size: 999,
            sha256: '0'.repeat(64),
          };
        },
      },
    });

    await expect(
      api.exportDatabase(
        { id: 'database-1', name: 'acme-production' },
        ownedFence(),
      ),
    ).rejects.toThrow('committed durable D1 export integrity differs');
  });

  it('does not dispatch any mutation after lease takeover following reads', async () => {
    const world = emptyScriptWorld();
    world.seedDatabase('acme-production', { databaseId: 'database-1' });
    const { api, fixture } = subject(restProjection(world), {
      exportStore: memoryStore(),
    });
    await api.listDatabases();
    await api.listVersions('acme-production');
    fixture.requests.length = 0;
    const denied = new Error('lease taken over');
    const lost = mutationFence(vi.fn(async () => Promise.reject(denied)));

    const operations: Array<readonly [() => Promise<unknown>, boolean]> = [
      [() => api.createDatabase('new-database', lost), true],
      [() => api.uploadCandidate(uploadIntent('staged'), lost), true],
      [
        () =>
          api.createDeployment(
            'acme-production',
            [{ versionId: 'version-1', percentage: 100 }],
            lost,
          ),
        true,
      ],
      [() => api.deleteWorkerScript('acme-production', lost), true],
      [
        () => api.deleteDatabaseFenced('database-1', lost),
        // This row alone has no adapter pre-assert, so its lease error crosses
        // the SDK fetch boundary and arrives wrapped.
        false,
      ],
      [
        () =>
          api.exportDatabase(
            { id: 'database-1', name: 'acme-production' },
            lost,
          ),
        true,
      ],
    ];
    for (const [operation, preservesLeaseError] of operations) {
      const failure = await rejectedValue(operation());
      expect(failure).toBeDefined();
      if (preservesLeaseError) expect(failure).toBe(denied);
    }
    expect(fixture.requests).toHaveLength(0);
  });

  it('forwards bounded decommission scans through the direct public backend', async () => {
    const world = emptyScriptWorld();
    const { client } = subject(restProjection(world));
    const input: DecommissionAttachmentScanInput = {
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: 'database-1',
      }),
      maxProviderRequests: 12,
    };
    const result: DecommissionAttachmentScanResult = { status: 'drift' };
    const calls: Array<readonly [unknown, DecommissionAttachmentScanInput]> =
      [];
    Object.defineProperty(client, 'advanceDecommissionAttachmentScan', {
      configurable: true,
      value(this: unknown, actual: DecommissionAttachmentScanInput) {
        calls.push([this, actual]);
        return Promise.resolve(result);
      },
    });

    const api = new CloudflareApiPlainWorkerProvisioningApi({ client });
    const backend = new CloudflareApiPlainWorkerBackend({ client });
    await expect(api.advanceDecommissionAttachmentScan(input)).resolves.toBe(
      result,
    );
    await expect(
      backend.advanceDecommissionAttachmentScan?.(input),
    ).resolves.toBe(result);
    expect(calls).toEqual([
      [client, input],
      [client, input],
    ]);
  });

  it('sanitizes a falsy transport failure without changing cleanup classification', async () => {
    const { api } = subject(async () => {
      throw undefined;
    });
    const outcome = await api.uploadCandidate(
      uploadIntent('initial'),
      ownedFence(),
    );
    expect(outcome).toMatchObject({
      status: 'failed',
      cleanup: { status: 'succeeded' },
      error: {
        name: 'CloudflareProviderError',
        message: 'Cloudflare Worker upload failed',
      },
    });
  });
});
