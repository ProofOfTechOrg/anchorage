// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import {
  CloudflareProvisioningClient,
  type DurableDatabaseExportStore,
  dispatchMigrations,
} from '../src/cloudflare-client.js';
import { ProcessLocalCloudflareApiRateCoordinator } from '../src/cloudflare-rate-coordinator.js';
import { canonicalDeploymentEgressPolicy } from '../src/platform-resources.js';
import type {
  DeploymentSpec,
  ExternalPlatformResources,
} from '../src/types.js';

function deployment(overrides: Partial<DeploymentSpec> = {}): DeploymentSpec {
  return {
    tenantTag: 'acme',
    environment: 'production',
    scriptName: 'acme-production',
    databaseName: 'acme-production',
    compatibilityDate: '2026-08-10',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'external',
    schemaVersion: 1,
    migrations: [{ version: 1, sql: 'SELECT 1' }],
    durableObjectMigrations: [
      { tag: 'v1', newClasses: ['Legacy'] },
      { tag: 'v2', newSqliteClasses: ['Maintenance'] },
      { tag: 'v3', deletedClasses: ['Legacy'] },
    ],
    previousDurableObjectTag: 'v1',
    durableObjectBindings: [],
    egressProxyService: 'fleet-egress-proxy',
    maintenanceBaseUrl: 'https://control-acme.example.test',
    routeHostname: 'acme.example.test',
    ...overrides,
  };
}

function envelope(result: unknown): Response {
  return Response.json({
    success: true,
    errors: [],
    messages: [],
    result,
    result_info: {
      page: 1,
      per_page: 20,
      count: Array.isArray(result) ? result.length : 1,
      total_count: Array.isArray(result) ? result.length : 1,
      total_pages: 1,
    },
  });
}

function zoneAuthorityResponse(
  url: URL,
  zoneIds: readonly string[],
): Response | undefined {
  if (url.pathname.endsWith('/user/tokens/verify')) {
    return envelope({ id: 'token-id', status: 'active' });
  }
  if (url.pathname.endsWith('/accounts/account/tokens/token-id')) {
    return envelope({
      id: 'token-id',
      status: 'active',
      policies: [
        {
          id: 'zone-authority',
          effect: 'allow',
          permission_groups: [
            { id: 'zone-read', name: 'Zone Read' },
            { id: 'routes-read', name: 'Workers Routes Read' },
            { id: 'routes-write', name: 'Workers Routes Write' },
          ],
          resources: {
            'com.cloudflare.api.account.account': {
              'com.cloudflare.api.account.zone.*': '*',
            },
          },
        },
      ],
    });
  }
  if (url.pathname.endsWith('/zones')) {
    expect(url.searchParams.get('account.id')).toBe('account');
    if (url.searchParams.has('page')) return envelope([]);
    return envelope(zoneIds.map((id) => ({ id, account: { id: 'account' } })));
  }
  return undefined;
}

function fenced<T>(
  client: CloudflareProvisioningClient,
  operation: () => Promise<T>,
): Promise<T> {
  return client.withMutationFence(
    {
      mutationLeaseTtlMs: 15 * 60_000,
      assertOwned: async () => {},
    },
    operation,
  );
}

function testRateCoordinator(
  intervalCap?: number,
): ProcessLocalCloudflareApiRateCoordinator {
  return new ProcessLocalCloudflareApiRateCoordinator(intervalCap);
}

function errorChain(error: unknown): string {
  const messages: string[] = [];
  let current = error;
  while (current instanceof Error) {
    messages.push(current.message);
    current = current.cause;
  }
  return messages.join(' | ');
}

describe('CloudflareProvisioningClient', () => {
  it('fails closed for unfenced writes and request timeouts outside the lease TTL', async () => {
    let providerWrites = 0;
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (init?.method === 'POST') providerWrites += 1;
        if (url.pathname.endsWith('/workers/dispatch/namespaces')) {
          return envelope([]);
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });

    let failure: unknown;
    try {
      await client.ensureDispatchNamespace();
    } catch (error) {
      failure = error;
    }
    expect(errorChain(failure)).toContain(
      'requires an external mutation fence',
    );
    expect(providerWrites).toBe(0);

    let operationStarted = false;
    await expect(
      client.withMutationFence(
        {
          mutationLeaseTtlMs: 60_000,
          assertOwned: async () => {},
        },
        async () => {
          operationStarted = true;
        },
      ),
    ).rejects.toThrow(/request timeout must be below/);
    expect(operationStarted).toBe(false);
  });

  it('deletes HOSTS only for a byte-exact durable target', async () => {
    const policy = canonicalDeploymentEgressPolicy({
      policyId: 'policy-acme',
      tenantTag: 'acme',
      environment: 'production',
      allowedHosts: ['api.example.com'],
    });
    const target = {
      scriptName: 'release-acme',
      tenantTag: 'acme',
      environment: 'production',
      ...policy,
      stateEgress: {
        resourceGroupId: policy.policyId,
        stateScriptName: 'state-acme',
        credentialDigest: 'a'.repeat(64),
      },
    };
    let stored: string | undefined = JSON.stringify({
      ...target,
      extra: 'same-owner-drift',
    });
    let deletes = 0;
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.includes('/storage/kv/namespaces/routes/values/')) {
          if (init?.method === 'DELETE') {
            deletes += 1;
            stored = undefined;
            return new Response(null, { status: 200 });
          }
          return stored === undefined
            ? new Response(null, { status: 404 })
            : new Response(stored);
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });
    const remove = () =>
      fenced(client, () =>
        client.deleteHostRouting('routes', 'acme.example.test', [target]),
      );

    await expect(remove()).rejects.toThrow(/owned by another deployment/);
    expect(deletes).toBe(0);
    stored = JSON.stringify(target);
    await expect(remove()).resolves.toBeUndefined();
    expect(deletes).toBe(1);
    await expect(remove()).resolves.toBeUndefined();
    expect(deletes).toBe(1);
  });

  it('sends only Durable Object migrations after the deployed tag', () => {
    expect(dispatchMigrations(deployment())).toEqual({
      new_tag: 'v3',
      old_tag: 'v1',
      steps: [
        {
          new_sqlite_classes: ['Maintenance'],
          new_classes: undefined,
          deleted_classes: undefined,
          renamed_classes: undefined,
        },
        {
          new_sqlite_classes: undefined,
          new_classes: undefined,
          deleted_classes: ['Legacy'],
          renamed_classes: undefined,
        },
      ],
    });
    expect(
      dispatchMigrations(deployment({ previousDurableObjectTag: 'v3' })),
    ).toBeUndefined();
  });

  it('creates or reuses only a dispatch namespace that attests trusted_workers=false', async () => {
    const calls: string[] = [];
    const request = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        const method = init?.method ?? 'GET';
        calls.push(`${method}:${url.pathname}`);
        if (url.pathname.endsWith('/workers/dispatch/namespaces/fleet')) {
          return envelope({
            namespace_name: 'fleet',
            namespace_id: 'namespace-id',
            script_count: 0,
            trusted_workers: false,
          });
        }
        if (url.pathname.endsWith('/workers/dispatch/namespaces')) {
          return method === 'POST' ? envelope({}) : envelope([]);
        }
        throw new Error(`unexpected request ${method} ${url.pathname}`);
      },
    );
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
    });

    await expect(
      fenced(client, () => client.ensureDispatchNamespace()),
    ).resolves.toBeUndefined();
    expect(calls).toEqual([
      'GET:/client/v4/accounts/account/workers/dispatch/namespaces',
      'POST:/client/v4/accounts/account/workers/dispatch/namespaces',
      'GET:/client/v4/accounts/account/workers/dispatch/namespaces/fleet',
    ]);
  });

  it('blocks a dispatch upload when namespace trust is true', async () => {
    let uploaded = false;
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        if (init?.body instanceof FormData) uploaded = true;
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/workers/dispatch/namespaces/fleet')) {
          return envelope({
            namespace_name: 'fleet',
            script_count: 0,
            trusted_workers: true,
          });
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });

    await expect(
      client.uploadDispatchWorker(deployment({ authoredBy: 'platform' }), {
        id: 'database',
        name: 'database',
        created: false,
      }),
    ).rejects.toThrow(/trusted_workers=false/);
    expect(uploaded).toBe(false);
  });

  it('deletes and attests an inherited maintenance secret on external retry', async () => {
    let updateBody: unknown;
    let secretListReads = 0;
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/scripts/acme/secrets-bulk')) {
          updateBody = JSON.parse(String(init?.body));
          return envelope({});
        }
        if (url.pathname.endsWith('/scripts/acme/secrets')) {
          secretListReads += 1;
          return envelope(
            secretListReads === 1
              ? [
                  { name: 'DEPLOYMENT_IDENTITY_SECRET' },
                  { name: 'MAINTENANCE_ADMIN_SECRET' },
                  { name: 'STALE_SECRET' },
                ]
              : [{ name: 'DEPLOYMENT_IDENTITY_SECRET' }],
          );
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });

    await fenced(client, () =>
      client.putDispatchSecrets(
        'acme',
        {
          deploymentIdentity: 'deployment-identity-secret-value-0001',
          maintenanceAdmin: 'old-maintenance-admin-secret-value-0001',
        },
        { includeMaintenanceAdmin: false },
      ),
    );

    expect(updateBody).toMatchObject({
      secrets: {
        DEPLOYMENT_IDENTITY_SECRET: expect.objectContaining({
          type: 'secret_text',
        }),
        MAINTENANCE_ADMIN_SECRET: null,
        STALE_SECRET: null,
      },
    });
  });

  it('deletes and attests an extra secret for a platform-authored dispatch Worker', async () => {
    let updateBody: unknown;
    let secretListReads = 0;
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/scripts/acme/secrets-bulk')) {
          updateBody = JSON.parse(String(init?.body));
          return envelope({});
        }
        if (url.pathname.endsWith('/scripts/acme/secrets')) {
          secretListReads += 1;
          return envelope(
            secretListReads === 1
              ? [
                  { name: 'DEPLOYMENT_IDENTITY_SECRET' },
                  { name: 'MAINTENANCE_ADMIN_SECRET' },
                  { name: 'STALE_SECRET' },
                ]
              : [
                  { name: 'DEPLOYMENT_IDENTITY_SECRET' },
                  { name: 'MAINTENANCE_ADMIN_SECRET' },
                ],
          );
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });

    await fenced(client, () =>
      client.putDispatchSecrets('acme', {
        deploymentIdentity: 'deployment-identity-secret-value-0001',
        maintenanceAdmin: 'maintenance-admin-secret-value-00001',
      }),
    );

    expect(updateBody).toMatchObject({
      secrets: {
        DEPLOYMENT_IDENTITY_SECRET: expect.objectContaining({
          type: 'secret_text',
        }),
        MAINTENANCE_ADMIN_SECRET: expect.objectContaining({
          type: 'secret_text',
        }),
        STALE_SECRET: null,
      },
    });
  });

  it.each([
    'control',
    'dispatch',
  ] as const)('fails closed when %s secret revocation is a provider no-op and converges on retry', async (plane) => {
    let removeOnBulk = false;
    let names = ['DEPLOYMENT_IDENTITY_SECRET', 'ARBITRARY_STALE_SECRET'];
    const updateBodies: unknown[] = [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/scripts/acme/secrets')) {
          return envelope(names.map((name) => ({ name })));
        }
        if (url.pathname.endsWith('/scripts/acme/secrets-bulk')) {
          updateBodies.push(JSON.parse(String(init?.body)));
          if (removeOnBulk) names = [];
          return envelope({});
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });
    const revoke = () =>
      fenced(client, () =>
        plane === 'control'
          ? client.revokeControlSecrets('acme')
          : client.revokeDispatchSecrets('acme'),
      );

    await expect(revoke()).rejects.toThrow(/failed exact secret revocation/);
    removeOnBulk = true;
    await expect(revoke()).resolves.toBeUndefined();
    expect(updateBodies).toHaveLength(2);
    expect(updateBodies[0]).toMatchObject({
      secrets: {
        DEPLOYMENT_IDENTITY_SECRET: null,
        ARBITRARY_STALE_SECRET: null,
      },
    });
  });

  it('deletes and attests a removed platform-plane SIEM secret', async () => {
    let updateBody: unknown;
    let secretListReads = 0;
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (
          url.pathname.endsWith('/workers/scripts/fleet-audit/secrets-bulk')
        ) {
          updateBody = JSON.parse(String(init?.body));
          return envelope({});
        }
        if (url.pathname.endsWith('/workers/scripts/fleet-audit/secrets')) {
          secretListReads += 1;
          return envelope(
            secretListReads === 1 ? [{ name: 'SIEM_AUTH_HEADER' }] : [],
          );
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });

    await fenced(client, () => client.putControlSecrets('fleet-audit', {}));

    expect(updateBody).toEqual({ secrets: { SIEM_AUTH_HEADER: null } });
  });

  it('attests an empty exact secret set without sending an empty bulk update', async () => {
    let listReads = 0;
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/workers/scripts/fleet-dispatch/secrets')) {
          listReads += 1;
          return envelope([]);
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });

    await expect(
      fenced(client, () => client.putControlSecrets('fleet-dispatch', {})),
    ).resolves.toBeUndefined();
    expect(listReads).toBe(2);
  });

  it('deletes ordinary Worker secrets through the REST API and accepts an already-missing secret', async () => {
    const requests: Array<{ readonly method: string; readonly path: string }> =
      [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        requests.push({ method: init?.method ?? 'GET', path: url.pathname });
        if (url.pathname.endsWith('/secrets/ALREADY_GONE')) {
          return Response.json(
            {
              success: false,
              errors: [{ code: 10090, message: 'secret not found' }],
              messages: [],
              result: null,
            },
            { status: 404 },
          );
        }
        return envelope({});
      },
    });

    await expect(
      client.deleteControlSecrets(
        'plain worker',
        ['PRESENT_SECRET', 'ALREADY_GONE', 'PRESENT_SECRET'],
        {
          mutationLeaseTtlMs: 15 * 60_000,
          assertOwned: async () => {},
        },
      ),
    ).resolves.toBeUndefined();

    expect(requests).toEqual([
      {
        method: 'DELETE',
        path: '/client/v4/accounts/account/workers/scripts/plain%20worker/secrets/ALREADY_GONE',
      },
      {
        method: 'DELETE',
        path: '/client/v4/accounts/account/workers/scripts/plain%20worker/secrets/PRESENT_SECRET',
      },
    ]);
  });

  it('deletes D1 by database id through the REST API and accepts an already-missing database', async () => {
    const requests: Array<{ readonly method: string; readonly path: string }> =
      [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        requests.push({ method: init?.method ?? 'GET', path: url.pathname });
        if (url.pathname.endsWith('/database/already-gone')) {
          return Response.json(
            {
              success: false,
              errors: [{ code: 7404, message: 'database not found' }],
              messages: [],
              result: null,
            },
            { status: 404 },
          );
        }
        return envelope(null);
      },
    });

    await expect(
      fenced(client, () => client.deleteDatabase('database-id')),
    ).resolves.toBeUndefined();
    await expect(
      fenced(client, () => client.deleteDatabase('already-gone')),
    ).resolves.toBeUndefined();

    expect(requests).toEqual([
      {
        method: 'DELETE',
        path: '/client/v4/accounts/account/d1/database/database-id',
      },
      {
        method: 'DELETE',
        path: '/client/v4/accounts/account/d1/database/already-gone',
      },
    ]);
  });

  it('propagates a non-404 D1 REST deletion failure', async () => {
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async () =>
        Response.json(
          {
            success: false,
            errors: [{ code: 10000, message: 'D1 write denied' }],
            messages: [],
            result: null,
          },
          { status: 403 },
        ),
    });

    await expect(
      fenced(client, () => client.deleteDatabase('database-id')),
    ).rejects.toThrow();
  });

  it('applies the request cap to each SDK HTTP request', async () => {
    vi.useFakeTimers();
    try {
      const request = vi.fn(async () => envelope([]));
      const client = new CloudflareProvisioningClient({
        accountId: 'account',
        apiToken: 'token',
        rateCoordinator: testRateCoordinator(1),
        dispatchNamespace: 'fleet',
        concurrency: 2,
        fetch: request,
      });

      const first = client.findDatabase('one');
      const second = client.findDatabase('two');
      await vi.waitFor(() => expect(request).toHaveBeenCalledTimes(1));
      expect(await first).toBeUndefined();
      expect(request).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(5 * 60_000);
      await expect(second).resolves.toBeUndefined();
      expect(request).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('applies the request cap to SDK retries and iterator pages', async () => {
    vi.useFakeTimers();
    try {
      const urls: URL[] = [];
      const requestTimes: number[] = [];
      const responses = [
        Response.json(
          {
            success: false,
            errors: [{ code: 10_000, message: 'retry this request' }],
            messages: [],
            result: null,
          },
          { status: 429, headers: { 'retry-after-ms': '1' } },
        ),
        envelope([{ name: 'another-database', uuid: 'database-one' }]),
        envelope([{ name: 'target', uuid: 'database-target' }]),
        envelope([]),
      ];
      const request = vi.fn(async (input: string | URL | Request) => {
        requestTimes.push(Date.now());
        urls.push(
          new URL(
            typeof input === 'string'
              ? input
              : input instanceof URL
                ? input.href
                : input.url,
          ),
        );
        const response = responses.shift();
        if (!response) throw new Error('unexpected Cloudflare SDK request');
        return response;
      });
      const client = new CloudflareProvisioningClient({
        accountId: 'account',
        apiToken: 'token',
        rateCoordinator: testRateCoordinator(1),
        dispatchNamespace: 'fleet',
        concurrency: 2,
        fetch: request,
      });

      const found = client.findDatabase('target');
      await vi.advanceTimersByTimeAsync(0);
      expect(request).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(5 * 60_000 - 1);
      expect(request).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(1);
      expect(request).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(5 * 60_000);
      expect(request).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(5 * 60_000);
      expect(request).toHaveBeenCalledTimes(4);

      await expect(found).resolves.toEqual({
        id: 'database-target',
        name: 'target',
        created: false,
      });
      expect(urls.map((url) => url.searchParams.get('page'))).toEqual([
        null,
        null,
        '2',
        '3',
      ]);
      const firstRequestAt = requestTimes[0];
      expect(firstRequestAt).toBeDefined();
      expect(requestTimes).toEqual([
        firstRequestAt,
        Number(firstRequestAt) + 5 * 60_000,
        Number(firstRequestAt) + 10 * 60_000,
        Number(firstRequestAt) + 15 * 60_000,
      ]);
    } finally {
      vi.useRealTimers();
    }
  });

  it('downloads an export into durable storage and records integrity', async () => {
    const bytes = new TextEncoder().encode('CREATE TABLE durable(id TEXT);');
    const stored: Uint8Array[] = [];
    const exportStore: DurableDatabaseExportStore = {
      async write(input) {
        const reader = input.body.getReader();
        for (;;) {
          const chunk = await reader.read();
          if (chunk.done) break;
          stored.push(chunk.value);
        }
        return {
          location: 'r2://fleet-exports/database-id.sql',
          size: bytes.byteLength,
          sha256:
            'db66962de2ce4d66e51620edbf2570465660a78e570569578abeee4b10fb8ce2',
        };
      },
    };
    const request = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      if (url.hostname === 'download.example.test') {
        return new Response(bytes, {
          headers: { 'content-length': String(bytes.byteLength) },
        });
      }
      return envelope({
        status: 'complete',
        result: { signed_url: 'https://download.example.test/export.sql' },
      });
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
      exportStore,
    });

    await expect(
      fenced(client, () => client.exportDatabase('database-id')),
    ).resolves.toEqual({
      databaseId: 'database-id',
      location: 'r2://fleet-exports/database-id.sql',
      sha256:
        'db66962de2ce4d66e51620edbf2570465660a78e570569578abeee4b10fb8ce2',
      size: bytes.byteLength,
    });
    expect(Buffer.concat(stored).equals(bytes)).toBe(true);
  });

  it('rejects durable storage that reports different committed integrity', async () => {
    const bytes = new TextEncoder().encode('CREATE TABLE durable(id TEXT);');
    const exportStore: DurableDatabaseExportStore = {
      async write(input) {
        const reader = input.body.getReader();
        while (!(await reader.read()).done) {}
        return {
          location: 'r2://fleet-exports/database-id.sql',
          size: bytes.byteLength - 1,
          sha256: '0'.repeat(64),
        };
      },
    };
    const request = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      if (url.hostname === 'download.example.test') {
        return new Response(bytes, {
          headers: { 'content-length': String(bytes.byteLength) },
        });
      }
      return envelope({
        status: 'complete',
        result: { signed_url: 'https://download.example.test/export.sql' },
      });
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
      exportStore,
    });

    await expect(
      fenced(client, () => client.exportDatabase('database-id')),
    ).rejects.toThrow(
      'committed durable D1 export integrity differs from the download',
    );
  });

  it('reads a D1 database by persisted ID and treats 404 as absent', async () => {
    const request = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      if (url.pathname.endsWith('/database/database-present')) {
        return envelope({ uuid: 'database-present', name: 'fleet-acme' });
      }
      return Response.json(
        {
          success: false,
          errors: [{ code: 7404, message: 'database not found' }],
          messages: [],
          result: null,
        },
        { status: 404 },
      );
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
    });

    await expect(client.getDatabase('database-present')).resolves.toEqual({
      id: 'database-present',
      name: 'fleet-acme',
      created: false,
    });
    await expect(
      client.getDatabase('database-missing'),
    ).resolves.toBeUndefined();
  });

  it('collects owner-checked WfP and independent plain Worker inventory without aborting on stale KV entries', async () => {
    const routePolicy = canonicalDeploymentEgressPolicy({
      policyId: 'policy-acme',
      tenantTag: 'acme',
      environment: 'production',
      allowedHosts: ['api.example.com'],
    });
    const kvValues = new Map<string, string>([
      [
        '__anchorage_script__:fleet-wfp',
        JSON.stringify({
          scriptName: 'fleet-wfp',
          tenantTag: 'acme',
          environment: 'production',
          databaseId: 'db-wfp',
          routeHostname: 'wfp.example.test',
        }),
      ],
      [
        '__anchorage_script__:fleet-missing',
        JSON.stringify({
          scriptName: 'fleet-missing',
          tenantTag: 'missing',
          environment: 'production',
          databaseId: 'db-missing',
          routeHostname: 'missing.example.test',
        }),
      ],
      [
        '__anchorage_script__:fleet-wrong-key',
        JSON.stringify({
          scriptName: 'fleet-wrong-owner',
          tenantTag: 'wrong',
          environment: 'production',
          databaseId: 'db-wrong',
          routeHostname: 'wrong.example.test',
        }),
      ],
      ['__anchorage_script__:fleet-malformed', '{'],
      [
        'wfp.example.test',
        JSON.stringify({
          scriptName: 'fleet-wfp',
          tenantTag: 'acme',
          environment: 'production',
          ...routePolicy,
        }),
      ],
      [
        'stale.example.test',
        JSON.stringify({
          scriptName: 'fleet-wfp',
          tenantTag: 'other',
          environment: 'production',
          ...canonicalDeploymentEgressPolicy({
            policyId: 'policy-other',
            tenantTag: 'other',
            environment: 'production',
            allowedHosts: [],
          }),
        }),
      ],
      [
        'bad-policy.example.test',
        JSON.stringify({
          scriptName: 'fleet-wfp',
          tenantTag: 'acme',
          environment: 'production',
          policyId: routePolicy.policyId,
          policyHosts: routePolicy.policyHosts,
          policyDigest: '0'.repeat(64),
        }),
      ],
      ['malformed.example.test', '{'],
    ]);
    const dispatchSettings = (tenantTag: string, databaseId: string) => ({
      bindings: [{ type: 'd1', name: 'DB', database_id: databaseId }],
      tags: [
        `tenant:${tenantTag}`,
        'environment:production',
        'schema:2',
        `spec:${'a'.repeat(64)}`,
      ],
    });
    const request = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      const path = decodeURIComponent(url.pathname);
      const zoneAuthority = zoneAuthorityResponse(url, ['zone']);
      if (zoneAuthority) return zoneAuthority;
      if (url.searchParams.has('page')) return envelope([]);
      if (path.endsWith('/storage/kv/namespaces/routes/keys')) {
        return envelope([...kvValues.keys()].map((name) => ({ name })));
      }
      const valueMarker = '/storage/kv/namespaces/routes/values/';
      if (path.includes(valueMarker)) {
        const value = kvValues.get(
          path.slice(path.indexOf(valueMarker) + valueMarker.length),
        );
        return value === undefined
          ? new Response(null, { status: 404 })
          : new Response(value);
      }
      if (path.endsWith('/workers/dispatch/namespaces/fleet')) {
        return envelope({
          namespace_name: 'fleet',
          script_count: 3,
          trusted_workers: false,
        });
      }
      if (path.endsWith('/workers/dispatch/namespaces/fleet/scripts')) {
        return envelope([
          {
            id: 'fleet-wfp',
            tags: ['fleet:anchorage', 'tenant:acme', 'environment:production'],
          },
          {
            id: 'fleet-orphan',
            tags: [
              'fleet:anchorage',
              'tenant:orphan',
              'environment:production',
            ],
          },
        ]);
      }
      if (path.includes('/workers/dispatch/namespaces/fleet/scripts/')) {
        const scriptName = path.split('/scripts/')[1]?.replace('/settings', '');
        if (scriptName === 'fleet-missing') {
          return Response.json(
            {
              success: false,
              errors: [{ code: 10090, message: 'missing' }],
              messages: [],
              result: null,
            },
            { status: 404 },
          );
        }
        const tenantTag = scriptName === 'fleet-wfp' ? 'acme' : 'wrong';
        const databaseId = scriptName === 'fleet-wfp' ? 'db-wfp' : 'db-wrong';
        return path.endsWith('/settings')
          ? envelope(dispatchSettings(tenantTag, databaseId))
          : envelope({ script: { etag: `etag-${scriptName}` } });
      }
      if (path.endsWith('/workers/domains')) {
        return envelope([
          {
            id: 'domain-plain',
            cert_id: 'cert',
            environment: 'production',
            hostname: 'plain.example.test',
            service: 'fleet-plain',
            zone_id: 'zone',
            zone_name: 'example.test',
          },
          {
            id: 'domain-ghost',
            cert_id: 'cert',
            environment: 'production',
            hostname: 'ghost.example.test',
            service: 'fleet-ghost',
            zone_id: 'zone',
            zone_name: 'example.test',
          },
        ]);
      }
      if (path.endsWith('/workers/scripts/fleet-plain/secrets')) {
        return envelope([]);
      }
      if (path.endsWith('/zones/zone/workers/routes')) {
        return envelope([
          {
            id: 'route-plain',
            pattern: 'plain.example.test/*',
            script: 'fleet-plain',
          },
        ]);
      }
      if (path.endsWith('/workers/scripts')) {
        return envelope([{ id: 'fleet-plain', etag: 'content-etag-plain' }]);
      }
      if (path.endsWith('/workers/scripts/fleet-plain/deployments')) {
        return envelope({
          deployments: [
            {
              id: 'deployment-plain',
              created_on: '2026-08-10T00:00:00.000Z',
              source: 'wrangler',
              strategy: 'percentage',
              versions: [
                { percentage: 100, version_id: 'active-version-plain' },
              ],
            },
          ],
        });
      }
      if (path.endsWith('/workers/scripts/fleet-plain/subdomain')) {
        return envelope({ enabled: false, previews_enabled: false });
      }
      if (
        path.endsWith(
          '/workers/scripts/fleet-plain/versions/active-version-plain',
        )
      ) {
        return envelope({
          resources: {
            bindings: [
              { type: 'd1', name: 'DB', database_id: 'db-plain' },
              {
                type: 'durable_object_namespace',
                name: 'STATE',
                class_name: 'State',
                namespace_id: 'ns-plain',
              },
              {
                type: 'plain_text',
                name: 'DEPLOYMENT_TENANT',
                text: 'plain',
              },
              {
                type: 'plain_text',
                name: 'FLEET_ENVIRONMENT',
                text: 'staging',
              },
              {
                type: 'plain_text',
                name: 'FLEET_SCHEMA_VERSION',
                text: '4',
              },
            ],
          },
        });
      }
      if (path.endsWith('/d1/database')) {
        return envelope([
          { uuid: 'db-wfp', name: 'fleet-wfp' },
          { uuid: 'db-plain', name: 'fleet-plain' },
          { uuid: 'ignored', name: 'other' },
        ]);
      }
      if (path.endsWith('/workers/durable_objects/namespaces')) {
        return envelope([
          { id: 'ns-wfp', script: 'fleet-wfp' },
          { id: 'ns-plain', script: 'fleet-plain' },
        ]);
      }
      throw new Error(`unexpected Cloudflare request: ${url.href}`);
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
    });

    const inventory = await client.collectFleetInventory({
      hostRoutingKvId: 'routes',
      databaseNamePrefix: 'fleet-',
      scriptNamePrefix: 'fleet-',
    });

    expect(inventory.dispatchScriptCount).toBe(3);
    expect(inventory.dispatchNamespace).toMatchObject({
      name: 'fleet',
      trustedWorkers: false,
      scriptCount: 3,
    });
    expect(inventory.deployments).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          backend: 'workers-for-platforms',
          scriptName: 'fleet-wfp',
          tenantTag: 'acme',
          databaseIds: ['db-wfp'],
          artifactVersion: 'etag-fleet-wfp',
        }),
        expect.objectContaining({
          backend: 'plain-worker',
          scriptName: 'fleet-plain',
          tenantTag: 'plain',
          environment: 'staging',
          databaseIds: ['db-plain'],
          artifactVersion: 'active-version-plain',
          routeHostnames: ['plain.example.test'],
          zoneRoutes: [
            {
              zoneId: 'zone',
              routeId: 'route-plain',
              pattern: 'plain.example.test/*',
            },
          ],
        }),
      ]),
    );
    expect(inventory.routes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          backend: 'workers-for-platforms',
          hostname: 'wfp.example.test',
          tenantTag: 'acme',
          ...routePolicy,
        }),
        expect.objectContaining({
          backend: 'plain-worker',
          hostname: 'plain.example.test',
          tenantTag: 'plain',
          environment: 'staging',
        }),
        expect.objectContaining({
          backend: 'plain-worker',
          surface: 'zone-route',
          hostname: 'plain.example.test/*',
          scriptName: 'fleet-plain',
          zoneId: 'zone',
          routeId: 'route-plain',
        }),
      ]),
    );
    expect(inventory.databaseIds).toEqual(['db-wfp', 'db-plain']);
    expect(inventory.namespaceIds).toEqual(['ns-wfp', 'ns-plain']);
    await expect(client.hasDurableObjectNamespace('ns-wfp')).resolves.toBe(
      true,
    );
    await expect(client.hasDurableObjectNamespace('ns-absent')).resolves.toBe(
      false,
    );
    expect(inventory.findings.map((finding) => finding.kind)).toEqual(
      expect.arrayContaining([
        'malformed-script-registration',
        'stale-script-registration',
        'malformed-route',
        'stale-route',
        'unknown-dispatch-scripts',
      ]),
    );
    expect(inventory.findings).toContainEqual(
      expect.objectContaining({
        kind: 'malformed-route',
        detail: expect.stringContaining('inconsistent policy metadata'),
      }),
    );
    expect(
      inventory.findings.find(
        (finding) =>
          finding.kind === 'unknown-dispatch-scripts' &&
          finding.detail.includes('fleet-orphan'),
      )?.detail,
    ).toContain("dispatch script 'fleet-orphan'");
  });

  it('collects plain Workers before a dispatch namespace exists and flags preview exposure', async () => {
    const urls: URL[] = [];
    const request = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      urls.push(url);
      const path = url.pathname;
      const zoneAuthority = zoneAuthorityResponse(url, []);
      if (zoneAuthority) return zoneAuthority;
      if (url.searchParams.has('page')) return envelope([]);
      if (path.endsWith('/workers/domains')) return envelope([]);
      if (path.endsWith('/workers/scripts/plain-acme/secrets')) {
        return envelope([]);
      }
      if (path.endsWith('/workers/scripts')) {
        return envelope([{ id: 'plain-acme', etag: 'content-etag' }]);
      }
      if (
        path.endsWith(
          '/workers/scripts/plain-acme/versions/active-version-acme',
        )
      ) {
        return envelope({
          resources: {
            bindings: [
              { type: 'd1', name: 'DB', database_id: 'database-acme' },
              {
                type: 'plain_text',
                name: 'DEPLOYMENT_TENANT',
                text: 'acme',
              },
              {
                type: 'plain_text',
                name: 'FLEET_ENVIRONMENT',
                text: 'production',
              },
              {
                type: 'plain_text',
                name: 'FLEET_SCHEMA_VERSION',
                text: '1',
              },
              {
                type: 'plain_text',
                name: 'FLEET_RESOURCE_ROLE',
                text: 'platform-state',
              },
            ],
          },
        });
      }
      if (path.endsWith('/workers/scripts/plain-acme/deployments')) {
        return envelope({
          deployments: [
            {
              id: 'deployment-acme',
              created_on: '2026-08-10T00:00:00.000Z',
              source: 'wrangler',
              strategy: 'percentage',
              versions: [
                { percentage: 100, version_id: 'active-version-acme' },
              ],
            },
          ],
        });
      }
      if (path.endsWith('/workers/scripts/plain-acme/subdomain')) {
        return envelope({ enabled: false, previews_enabled: true });
      }
      if (path.endsWith('/d1/database')) return envelope([]);
      if (path.endsWith('/workers/durable_objects/namespaces')) {
        return envelope([]);
      }
      throw new Error(`unexpected Cloudflare request: ${url.href}`);
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'not-created',
      fetch: request,
    });

    const inventory = await client.collectFleetInventory({
      databaseNamePrefix: 'plain-',
      scriptNamePrefix: 'plain-',
      includeDispatchNamespace: false,
    });

    expect(inventory.dispatchScriptCount).toBeUndefined();
    expect(inventory.deployments).toEqual([
      expect.objectContaining({
        backend: 'plain-worker',
        scriptName: 'plain-acme',
        artifactVersion: 'active-version-acme',
      }),
    ]);
    expect(urls.every((url) => !url.pathname.includes('/dispatch/'))).toBe(
      true,
    );
    expect(inventory.findings).toContainEqual(
      expect.objectContaining({
        kind: 'incomplete-deployment',
        detail: expect.stringContaining('preview URL'),
      }),
    );
  });

  it('rejects split traffic and extra zero-percent versions during recurring inspection and inventory', async () => {
    const inspectedVersions: string[] = [];
    let versions = [
      { percentage: 50, version_id: 'trusted-first' },
      { percentage: 50, version_id: 'drifted-second' },
      { percentage: 0, version_id: 'inactive-candidate' },
    ];
    const request = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      const path = url.pathname;
      const zoneAuthority = zoneAuthorityResponse(url, []);
      if (zoneAuthority) return zoneAuthority;
      if (url.searchParams.has('page')) return envelope([]);
      if (path.endsWith('/workers/domains')) return envelope([]);
      if (path.endsWith('/workers/scripts')) {
        return envelope([{ id: 'plain-acme', etag: 'content-etag' }]);
      }
      if (path.endsWith('/workers/scripts/plain-acme/deployments')) {
        return envelope({
          deployments: [
            {
              versions,
            },
          ],
        });
      }
      if (path.includes('/workers/scripts/plain-acme/versions/')) {
        inspectedVersions.push(path.split('/versions/')[1] ?? '');
        return envelope({ resources: { bindings: [] } });
      }
      if (path.endsWith('/d1/database')) return envelope([]);
      if (path.endsWith('/workers/durable_objects/namespaces')) {
        return envelope([]);
      }
      throw new Error(`unexpected Cloudflare request: ${url.href}`);
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'not-created',
      fetch: request,
    });

    await expect(client.inspectControlWorker('plain-acme')).rejects.toThrow(
      /exactly one current version receiving 100%/,
    );
    const inventory = await client.collectFleetInventory({
      databaseNamePrefix: 'plain-',
      scriptNamePrefix: 'plain-',
      includeDispatchNamespace: false,
    });
    expect(inventory.deployments).toEqual([]);
    expect(inventory.findings).toContainEqual(
      expect.objectContaining({
        kind: 'incomplete-deployment',
        detail: expect.stringContaining(
          'exactly one current version receiving 100%',
        ),
      }),
    );
    expect(inspectedVersions).toEqual([]);

    versions = [
      { percentage: 100, version_id: 'trusted-first' },
      { percentage: 0, version_id: 'out-of-band-candidate' },
    ];
    await expect(client.inspectControlWorker('plain-acme')).rejects.toThrow(
      /exactly one current version receiving 100%/,
    );
    const extraCandidateInventory = await client.collectFleetInventory({
      databaseNamePrefix: 'plain-',
      scriptNamePrefix: 'plain-',
      includeDispatchNamespace: false,
    });
    expect(extraCandidateInventory.deployments).toEqual([]);
    expect(extraCandidateInventory.findings).toContainEqual(
      expect.objectContaining({
        kind: 'incomplete-deployment',
        detail: expect.stringContaining(
          'exactly one current version receiving 100%',
        ),
      }),
    );
    expect(inspectedVersions).toEqual([]);
  });

  it('repoints a host route only from an explicitly allowed persisted release', async () => {
    let stored = JSON.stringify({
      scriptName: 'acme-old',
      tenantTag: 'acme',
      environment: 'production',
    });
    const writes: string[] = [];
    const request = vi.fn(
      async (_input: string | URL | Request, init?: RequestInit) => {
        if (init?.method === 'PUT') {
          if (!(init.body instanceof FormData)) {
            throw new Error('expected multipart KV update');
          }
          const value = init.body.get('value');
          stored =
            typeof value === 'string'
              ? value
              : value instanceof Blob
                ? await value.text()
                : '';
          writes.push(stored);
          return envelope(null);
        }
        return new Response(stored);
      },
    );
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
    });

    await expect(
      fenced(client, () =>
        client.putHostRouting(
          'routes',
          'acme.example.test',
          {
            scriptName: 'acme-new',
            tenantTag: 'acme',
            environment: 'production',
            policyId: 'acme-production',
            policyDigest: 'a'.repeat(64),
            policyHosts: ['api.example.com'],
          },
          {
            allowedCurrentScriptNames: ['acme-old', 'acme-new'],
            allowUnrouted: false,
          },
        ),
      ),
    ).resolves.toBeUndefined();
    expect(writes).toHaveLength(1);
    expect(JSON.parse(writes[0] ?? '{}')).toMatchObject({
      scriptName: 'acme-new',
      tenantTag: 'acme',
      environment: 'production',
      policyId: 'acme-production',
      policyDigest: 'a'.repeat(64),
      policyHosts: ['api.example.com'],
    });

    stored = JSON.stringify({
      scriptName: 'acme-stale-release',
      tenantTag: 'acme',
      environment: 'production',
    });
    await expect(
      client.putHostRouting(
        'routes',
        'acme.example.test',
        {
          scriptName: 'acme-next',
          tenantTag: 'acme',
          environment: 'production',
          policyId: 'acme-production',
          policyDigest: 'a'.repeat(64),
          policyHosts: ['api.example.com'],
        },
        {
          allowedCurrentScriptNames: ['acme-new', 'acme-next'],
          allowUnrouted: false,
        },
      ),
    ).rejects.toThrow(/another deployment/);
    expect(writes).toHaveLength(1);
  });

  it('paginates and retries the authenticated dispatch-script inventory listing', async () => {
    let firstPageAttempts = 0;
    const rawRequests: Array<{ url: URL; authorization: string | null }> = [];
    const request = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        const path = url.pathname;
        const zoneAuthority = zoneAuthorityResponse(url, []);
        if (zoneAuthority) return zoneAuthority;
        if (path.endsWith('/workers/dispatch/namespaces/fleet/scripts')) {
          rawRequests.push({
            url,
            authorization: new Headers(init?.headers).get('authorization'),
          });
          if (!url.searchParams.has('cursor')) {
            firstPageAttempts += 1;
            if (firstPageAttempts === 1) {
              return new Response(null, { status: 429 });
            }
            return Response.json({
              success: true,
              errors: [],
              messages: [],
              result: [
                {
                  id: 'fleet-orphan-one',
                  tags: [
                    'fleet:anchorage',
                    'tenant:one',
                    'environment:production',
                  ],
                },
              ],
              result_info: { cursor: 'next-page' },
            });
          }
          return Response.json({
            success: true,
            errors: [],
            messages: [],
            result: [
              {
                id: 'fleet-orphan-two',
                tags: ['fleet:anchorage', 'tenant:two', 'environment:staging'],
              },
            ],
            result_info: {},
          });
        }
        if (path.endsWith('/workers/dispatch/namespaces/fleet')) {
          return envelope({
            namespace_name: 'fleet',
            script_count: 2,
            trusted_workers: false,
          });
        }
        if (
          path.endsWith('/workers/domains') ||
          path.endsWith('/workers/scripts') ||
          path.endsWith('/d1/database') ||
          path.endsWith('/workers/durable_objects/namespaces')
        ) {
          return envelope([]);
        }
        if (url.searchParams.has('page')) return envelope([]);
        throw new Error(`unexpected Cloudflare request: ${url.href}`);
      },
    );
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      concurrency: 1,
      fetch: request,
    });

    const inventory = await client.collectFleetInventory({
      databaseNamePrefix: 'fleet-',
      scriptNamePrefix: 'fleet-',
      includeDispatchNamespace: true,
    });

    expect(rawRequests).toHaveLength(3);
    expect(
      rawRequests.every((request) => request.authorization === 'Bearer token'),
    ).toBe(true);
    expect(rawRequests.at(-1)?.url.searchParams.get('cursor')).toBe(
      'next-page',
    );
    expect(inventory.dispatchScriptCount).toBe(2);
    expect(
      inventory.findings.map((finding) => [
        finding.tenantTag,
        finding.environment,
        finding.detail,
      ]),
    ).toEqual([
      ['one', 'production', expect.stringContaining('fleet-orphan-one')],
      ['two', 'staging', expect.stringContaining('fleet-orphan-two')],
    ]);
  });

  it('uploads an external candidate under its physical name with platform-owned DO metadata', async () => {
    let requestUrl: URL | undefined;
    let uploadBody: FormData | undefined;
    const requestOrder: string[] = [];
    const request = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (!(init?.body instanceof FormData)) {
          requestOrder.push('namespace-attestation');
          if (url.pathname.endsWith('/workers/dispatch/namespaces/fleet')) {
            return envelope({
              namespace_name: 'fleet',
              script_count: 0,
              trusted_workers: false,
            });
          }
          throw new Error('expected dispatch namespace attestation');
        }
        requestOrder.push('dispatch-upload');
        requestUrl = url;
        uploadBody = init.body;
        return envelope({ etag: 'etag-candidate' });
      },
    );
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
    });
    const candidate = deployment({
      egressProxyService: undefined,
      durableObjectMigrations: [],
      previousDurableObjectTag: undefined,
      durableObjectBindings: [
        {
          name: 'MAINTENANCE',
          className: 'Maintenance',
        },
      ],
      queueProducer: { binding: 'AUDIT_QUEUE', queueName: 'fleet-audit' },
    });
    const resources: ExternalPlatformResources = {
      maintenanceCapabilityPublicKey:
        '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
      stateWorker: {
        scriptName: 'fleet-maintenance-host',
        artifactVersion: 'state-v1',
        artifactDigest: 'a'.repeat(64),
        durableObjectBindings: [],
        namespaceIds: [],
        plane: 'dispatch',
        dispatchNamespace: 'fleet',
      },
      sharedOutboundWorkerName: 'fleet-shared-outbound',
      outboundPolicy: {
        policyId: 'policy-acme',
        policyHosts: ['api.example.com'],
        policyDigest: 'c'.repeat(64),
      },
    };

    await expect(
      fenced(client, () =>
        client.uploadDispatchWorker(
          candidate,
          { id: 'db-acme', name: 'acme-production', created: false },
          'acme-physical-candidate',
          resources,
        ),
      ),
    ).resolves.toEqual({ artifactVersion: 'etag-candidate' });
    expect(requestOrder.slice(-2)).toEqual([
      'namespace-attestation',
      'dispatch-upload',
    ]);
    expect(requestUrl?.pathname).toContain('/acme-physical-candidate');
    const entries = [...(uploadBody?.entries() ?? [])].map(
      ([name, value]) => [name, String(value)] as const,
    );
    expect(entries).toEqual(
      expect.arrayContaining([
        ['metadata[bindings][][name]', 'MAINTENANCE'],
        ['metadata[bindings][][type]', 'durable_object_namespace'],
        ['metadata[bindings][][class_name]', 'Maintenance'],
        ['metadata[bindings][][script_name]', 'fleet-maintenance-host'],
        ['metadata[bindings][][dispatch_namespace]', 'fleet'],
        ['metadata[bindings][][name]', 'AUDIT_PROXY'],
        ['metadata[bindings][][class_name]', 'FlowsafeFleetAuditProxy'],
        ['metadata[bindings][][name]', 'FLEET_SPEC_DIGEST'],
        ['metadata[bindings][][name]', 'FLEET_MAINTENANCE_CAPABILITIES'],
        ['metadata[tags][]', 'fleet:anchorage'],
      ]),
    );
    expect(entries).not.toEqual(
      expect.arrayContaining([['metadata[bindings][][name]', 'EGRESS_PROXY']]),
    );
    expect(entries).not.toEqual(
      expect.arrayContaining([['metadata[bindings][][type]', 'service']]),
    );
  });

  it('fails before a privacy mutation when token policies omit account-wide zone authority', async () => {
    const calls: string[] = [];
    const request = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        calls.push(`${init?.method ?? 'GET'}:${url.pathname}`);
        if (url.pathname.endsWith('/user/tokens/verify')) {
          return envelope({ id: 'token-id', status: 'active' });
        }
        if (url.pathname.endsWith('/accounts/account/tokens/token-id')) {
          return envelope({
            id: 'token-id',
            status: 'active',
            policies: [
              {
                id: 'zone-limited',
                effect: 'allow',
                permission_groups: [
                  { id: 'zone-read', name: 'Zone Read' },
                  { id: 'routes-read', name: 'Workers Routes Read' },
                  { id: 'routes-write', name: 'Workers Routes Write' },
                ],
                resources: {
                  'com.cloudflare.api.account.zone.visible-zone': '*',
                },
              },
            ],
          });
        }
        throw new Error(`unexpected Cloudflare request: ${url.href}`);
      },
    );
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
    });

    await expect(
      client.disableControlWorkerPublicAccess('fleet-state'),
    ).rejects.toThrow(/every zone/);
    expect(calls).toEqual([
      'GET:/client/v4/user/tokens/verify',
      'GET:/client/v4/accounts/account/tokens/token-id',
    ]);
  });

  it('updates the sole audit consumer by ID for every configuration difference and re-attests it', async () => {
    const desired = {
      consumer_id: 'consumer-id',
      type: 'worker' as const,
      script_name: 'fleet-audit',
      dead_letter_queue: 'fleet-audit-dlq',
      settings: {
        batch_size: 100,
        max_concurrency: 4,
        max_retries: 5,
        max_wait_time_ms: 5_000,
      },
    };
    const cases = [
      { script_name: 'old-audit' },
      { dead_letter_queue: 'old-dlq' },
      { settings: { ...desired.settings, batch_size: 99 } },
      { settings: { ...desired.settings, max_concurrency: 3 } },
      { settings: { ...desired.settings, max_retries: 4 } },
      { settings: { ...desired.settings, max_wait_time_ms: 4_999 } },
    ];

    for (const difference of cases) {
      let consumer = { ...desired, ...difference };
      let updateCount = 0;
      const request = vi.fn(
        async (input: string | URL | Request, init?: RequestInit) => {
          const url = new URL(
            typeof input === 'string'
              ? input
              : input instanceof URL
                ? input.href
                : input.url,
          );
          const method = init?.method ?? 'GET';
          if (url.pathname.endsWith('/accounts/account/queues')) {
            return envelope([
              { queue_id: 'queue-id', queue_name: 'fleet-audit' },
            ]);
          }
          if (
            url.pathname.endsWith('/accounts/account/queues/queue-id/consumers')
          ) {
            return envelope([consumer]);
          }
          if (
            url.pathname.endsWith(
              '/accounts/account/queues/queue-id/consumers/consumer-id',
            )
          ) {
            if (method === 'PUT') {
              updateCount += 1;
              const body = JSON.parse(String(init?.body));
              consumer = { consumer_id: 'consumer-id', ...body };
            }
            return envelope(consumer);
          }
          throw new Error(`unexpected Cloudflare request: ${url.href}`);
        },
      );
      const client = new CloudflareProvisioningClient({
        accountId: 'account',
        apiToken: 'token',
        rateCoordinator: testRateCoordinator(),
        dispatchNamespace: 'fleet',
        fetch: request,
      });

      await expect(
        fenced(client, () =>
          client.ensureQueueConsumer({
            queueName: 'fleet-audit',
            scriptName: 'fleet-audit',
            deadLetterQueue: 'fleet-audit-dlq',
          }),
        ),
      ).resolves.toBeUndefined();
      expect(updateCount).toBe(1);
      expect(consumer).toEqual(desired);
    }
  });

  it('discovers an omitted account zone before privatizing and inspecting a trusted Worker', async () => {
    const calls: Array<{ path: string; method: string }> = [];
    let publicEnabled = true;
    let previewsEnabled = true;
    let customDomainAttached = true;
    let zoneRouteAttached = true;
    let extraBindings: readonly Readonly<Record<string, unknown>>[] = [];
    let controlSecretNames: readonly string[] = [];
    const request = vi.fn(
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        const method = init?.method ?? 'GET';
        calls.push({ path: url.pathname, method });
        const zoneAuthority = zoneAuthorityResponse(url, [
          'zone-a',
          'zone-hidden',
        ]);
        if (zoneAuthority) return zoneAuthority;
        if (url.pathname.endsWith('/workers/scripts/fleet-state/deployments')) {
          return envelope({
            deployments: [
              {
                versions: [{ percentage: 100, version_id: 'state-version' }],
              },
            ],
          });
        }
        if (
          url.pathname.endsWith(
            '/workers/scripts/fleet-state/versions/state-version',
          )
        ) {
          return envelope({
            resources: {
              bindings: [
                { type: 'd1', name: 'DB', database_id: 'db-acme' },
                {
                  type: 'kv_namespace',
                  name: 'HOSTS',
                  namespace_id: 'kv-hosts',
                },
                {
                  type: 'plain_text',
                  name: 'DEPLOYMENT_TENANT',
                  text: 'acme',
                },
                {
                  type: 'service',
                  name: 'EGRESS_PROXY',
                  service: 'fleet-egress',
                },
                ...extraBindings,
              ],
            },
          });
        }
        if (url.pathname.endsWith('/workers/scripts/fleet-state/secrets')) {
          return envelope(controlSecretNames.map((name) => ({ name })));
        }
        if (url.pathname.endsWith('/workers/scripts/fleet-state/subdomain')) {
          if (method === 'POST') {
            const body = JSON.parse(String(init?.body)) as {
              enabled: boolean;
              previews_enabled: boolean;
            };
            publicEnabled = body.enabled;
            previewsEnabled = body.previews_enabled;
          }
          return envelope({
            enabled: publicEnabled,
            previews_enabled: previewsEnabled,
          });
        }
        if (url.pathname.endsWith('/workers/domains')) {
          return envelope(
            customDomainAttached
              ? [
                  {
                    id: 'domain-fleet-state',
                    hostname: 'state.example.test',
                    service: 'fleet-state',
                  },
                ]
              : [],
          );
        }
        if (
          url.pathname.endsWith('/workers/domains/domain-fleet-state') &&
          method === 'DELETE'
        ) {
          customDomainAttached = false;
          return envelope(null);
        }
        if (url.pathname.endsWith('/zones/zone-a/workers/routes')) {
          return envelope([
            {
              id: 'route-foreign',
              pattern: 'foreign.example.test/*',
              script: 'foreign-worker',
            },
          ]);
        }
        if (url.pathname.endsWith('/zones/zone-hidden/workers/routes')) {
          return envelope(
            zoneRouteAttached
              ? [
                  {
                    id: 'route-fleet-state',
                    pattern: 'state.example.test/*',
                    script: 'fleet-state',
                  },
                ]
              : [],
          );
        }
        if (
          url.pathname.endsWith(
            '/zones/zone-hidden/workers/routes/route-fleet-state',
          ) &&
          method === 'DELETE'
        ) {
          zoneRouteAttached = false;
          return envelope({ id: 'route-fleet-state' });
        }
        if (url.pathname.endsWith('/workers/scripts/fleet-state')) {
          return method === 'DELETE'
            ? envelope(null)
            : envelope({ etag: 'state-etag' });
        }
        throw new Error(`unexpected Cloudflare request: ${method} ${url.href}`);
      },
    );
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
    });

    await expect(
      fenced(client, () =>
        client.uploadControlWorker({
          scriptName: 'fleet-state',
          mainModule: 'state.js',
          modules: [{ name: 'state.js', content: 'export default {}' }],
          compatibilityDate: '2026-08-10',
          bindings: [],
        }),
      ),
    ).resolves.toBe('state-etag');
    const controlUpload = request.mock.calls.find(
      ([, init]) => init?.body instanceof FormData,
    )?.[1]?.body;
    expect(controlUpload).toBeInstanceOf(FormData);
    expect(
      [...((controlUpload as FormData).entries() ?? [])].map(
        ([name, value]) => [name, String(value)],
      ),
    ).toEqual(
      expect.arrayContaining([['metadata[keep_bindings][]', 'secret_text']]),
    );
    await fenced(client, () =>
      client.disableControlWorkerPublicAccess('fleet-state'),
    );
    await expect(client.inspectControlWorker('fleet-state')).resolves.toEqual({
      artifactVersion: 'state-version',
      databaseIds: ['db-acme'],
      durableObjectBindings: [],
      dispatchNamespaceBindings: [],
      kvNamespaceBindings: [{ name: 'HOSTS', namespaceId: 'kv-hosts' }],
      serviceBindings: [{ name: 'EGRESS_PROXY', service: 'fleet-egress' }],
      queueProducerBindings: [],
      r2BucketBindings: [],
      secretNames: [],
      plainTextBindings: { DEPLOYMENT_TENANT: 'acme' },
      providerBindingIdentities: [
        { type: 'd1', name: 'DB' },
        { type: 'kv_namespace', name: 'HOSTS' },
        { type: 'plain_text', name: 'DEPLOYMENT_TENANT' },
        { type: 'service', name: 'EGRESS_PROXY' },
      ],
      workersDevEnabled: false,
      previewUrlsEnabled: false,
      routeHostnames: [],
      zoneRoutes: [],
    });
    controlSecretNames = ['OUT_OF_BAND_SECRET'];
    await expect(client.inspectControlWorker('fleet-state')).rejects.toThrow(
      /unsupported or malformed provider binding/u,
    );
    controlSecretNames = [];
    extraBindings = [
      { type: 'hyperdrive', name: 'FUTURE_BINDING', id: 'config-id' },
    ];
    await expect(client.inspectControlWorker('fleet-state')).rejects.toThrow(
      /unsupported or malformed provider binding/u,
    );
    await fenced(client, () => client.deleteControlWorker('fleet-state'));
    expect(calls).toEqual(
      expect.arrayContaining([
        {
          path: '/client/v4/accounts/account/workers/scripts/fleet-state/subdomain',
          method: 'POST',
        },
        {
          path: '/client/v4/zones/zone-hidden/workers/routes/route-fleet-state',
          method: 'DELETE',
        },
        expect.objectContaining({
          path: expect.stringContaining('/domains/domain-fleet-state'),
          method: 'DELETE',
        }),
        expect.objectContaining({
          path: expect.stringContaining('/fleet-state'),
          method: 'DELETE',
        }),
      ]),
    );
  });

  it('rejects an unconsumed dispatch-provider binding', async () => {
    const request = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      if (url.pathname.endsWith('/scripts/candidate/settings')) {
        return envelope({
          bindings: [
            { type: 'd1', name: 'DB', database_id: 'db-acme' },
            { type: 'kv_namespace', name: 'OUT_OF_BAND', namespace_id: 'kv' },
          ],
          tags: [
            'tenant:acme',
            'environment:production',
            'schema:1',
            `spec:${'a'.repeat(64)}`,
          ],
        });
      }
      if (url.pathname.endsWith('/scripts/candidate')) {
        return envelope({ script: { etag: 'etag-v1' } });
      }
      throw new Error(`unexpected Cloudflare request: ${url.href}`);
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: request,
    });

    await expect(client.inspectDispatchWorker('candidate')).rejects.toThrow(
      /unsupported or malformed provider binding/u,
    );
  });

  it('stops cleanup when the mutation fence is lost between provider writes', async () => {
    const providerMutations: string[] = [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      requestTimeoutMs: 100,
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        const authority = zoneAuthorityResponse(url, ['zone']);
        if (authority) return authority;
        const method = init?.method ?? 'GET';
        if (url.pathname.endsWith('/workers/scripts/fleet-state/subdomain')) {
          if (method === 'POST') providerMutations.push('subdomain');
          return envelope({ enabled: false, previews_enabled: false });
        }
        if (url.pathname.endsWith('/workers/domains')) {
          return envelope([
            {
              id: 'domain-id',
              hostname: 'state.example.test',
              service: 'fleet-state',
            },
          ]);
        }
        if (url.pathname.endsWith('/workers/domains/domain-id')) {
          providerMutations.push('domain-delete');
          return envelope(null);
        }
        if (url.pathname.endsWith('/zones/zone/workers/routes')) {
          return envelope([]);
        }
        throw new Error(`unexpected request ${method} ${url.pathname}`);
      },
    });
    let assertions = 0;
    let failure: unknown;
    try {
      await client.withMutationFence(
        {
          mutationLeaseTtlMs: 1_000,
          assertOwned: async () => {
            assertions += 1;
            if (assertions > 1) throw new Error('lease takeover fenced write');
          },
        },
        () => client.disableControlWorkerPublicAccess('fleet-state'),
      );
    } catch (error) {
      failure = error;
    }

    expect(errorChain(failure)).toContain('lease takeover fenced write');
    expect(assertions).toBeGreaterThan(1);
    expect(providerMutations).toEqual(['subdomain']);
  });

  it('fenced-disables workers.dev and preview ingress for an ordinary Worker', async () => {
    const requests: Array<{ method: string; body?: unknown }> = [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      requestTimeoutMs: 100,
      fetch: async (_input, init) => {
        requests.push({
          method: init?.method ?? 'GET',
          ...(init?.body ? { body: JSON.parse(String(init.body)) } : {}),
        });
        return envelope({ enabled: false, previews_enabled: false });
      },
    });

    await client.disableOrdinaryWorkerPublicAccess('fleet-state', {
      mutationLeaseTtlMs: 60_000,
      assertOwned: async () => {},
    });

    expect(requests).toEqual([
      {
        method: 'POST',
        body: { enabled: false, previews_enabled: false },
      },
      { method: 'GET' },
    ]);
  });

  it('accepts already-absent ordinary ingress during fenced force teardown', async () => {
    const requests: Array<{ method: string; path: string }> = [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      requestTimeoutMs: 100,
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        requests.push({ method: init?.method ?? 'GET', path: url.pathname });
        return Response.json(
          {
            success: false,
            errors: [{ code: 10090, message: 'resource not found' }],
            messages: [],
            result: null,
          },
          { status: 404 },
        );
      },
    });
    const fence = {
      mutationLeaseTtlMs: 60_000,
      assertOwned: async () => {},
    };

    await expect(
      client.disableOrdinaryWorkerPublicAccess('already-gone', fence),
    ).resolves.toBeUndefined();
    await expect(
      client.detachCustomDomain('already-gone', fence),
    ).resolves.toBeUndefined();

    expect(requests).toEqual([
      {
        method: 'POST',
        path: '/client/v4/accounts/account/workers/scripts/already-gone/subdomain',
      },
      {
        method: 'DELETE',
        path: '/client/v4/accounts/account/workers/domains/already-gone',
      },
    ]);
  });

  it('reads exact script ownership and account-wide ordinary Worker footprint', async () => {
    const registration = {
      scriptName: 'fleet-state',
      tenantTag: 'acme',
      environment: 'production',
      databaseId: 'db-acme',
      routeHostname: 'state.example.test',
    };
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        const authority = zoneAuthorityResponse(url, ['zone']);
        if (authority) return authority;
        if (url.pathname.includes('/storage/kv/namespaces/routes/values/')) {
          if (decodeURIComponent(url.pathname).endsWith('missing')) {
            return new Response(null, { status: 404 });
          }
          return new Response(JSON.stringify(registration));
        }
        if (url.pathname.endsWith('/workers/scripts')) {
          return envelope([{ id: 'fleet-state' }, { id: 'foreign' }]);
        }
        if (url.pathname.endsWith('/workers/scripts/fleet-state/subdomain')) {
          return envelope({ enabled: true, previews_enabled: false });
        }
        if (url.pathname.endsWith('/workers/domains')) {
          return envelope([
            {
              id: 'domain-id',
              hostname: 'state.example.test',
              service: 'fleet-state',
            },
            {
              id: 'foreign-domain',
              hostname: 'foreign.example.test',
              service: 'foreign',
            },
          ]);
        }
        if (url.pathname.endsWith('/zones/zone/workers/routes')) {
          return envelope([
            {
              id: 'route-id',
              pattern: 'state.example.test/*',
              script: 'fleet-state',
            },
            {
              id: 'foreign-route',
              pattern: 'foreign.example.test/*',
              script: 'foreign',
            },
          ]);
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });

    await expect(
      client.getScriptInventory('routes', 'fleet-state'),
    ).resolves.toEqual(registration);
    await expect(
      client.getScriptInventory('routes', 'missing'),
    ).resolves.toBeUndefined();
    await expect(
      client.inspectOrdinaryWorkerFootprint('fleet-state'),
    ).resolves.toEqual({
      scriptPresent: true,
      workersDevEnabled: true,
      previewUrlsEnabled: false,
      customDomains: [
        {
          id: 'domain-id',
          hostname: 'state.example.test',
          service: 'fleet-state',
        },
      ],
      zoneRoutes: [
        {
          zoneId: 'zone',
          routeId: 'route-id',
          pattern: 'state.example.test/*',
        },
      ],
    });
  });

  it('finds account-wide D1 attachments in every current ordinary version and dispatch script', async () => {
    const inspectedOrdinaryVersions: string[] = [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        const path = decodeURIComponent(url.pathname);
        if (path.endsWith('/workers/scripts')) {
          return envelope([{ id: 'outside-fleet-prefix' }]);
        }
        if (path.endsWith('/workers/dispatch/namespaces')) {
          return envelope([
            { namespace_name: 'fleet' },
            { namespace_name: 'foreign' },
          ]);
        }
        if (
          path.endsWith('/workers/scripts/outside-fleet-prefix/deployments')
        ) {
          return envelope({
            deployments: [
              {
                versions: [
                  { percentage: 50, version_id: 'live-without-target-db' },
                  { percentage: 50, version_id: 'live-with-target-db' },
                  { percentage: 0, version_id: 'zero-percent-candidate' },
                ],
              },
            ],
          });
        }
        if (path.includes('/workers/scripts/outside-fleet-prefix/versions/')) {
          const versionId = path.split('/versions/')[1] ?? '';
          inspectedOrdinaryVersions.push(versionId);
          return envelope({
            resources: {
              bindings:
                versionId === 'live-with-target-db'
                  ? [{ type: 'd1', name: 'DB', database_id: 'db-target' }]
                  : versionId === 'zero-percent-candidate'
                    ? [{ type: 'd1', name: 'DB', database_id: 'db-zero' }]
                    : [{ type: 'd1', name: 'DB', database_id: 'db-other' }],
            },
          });
        }
        if (path.endsWith('/workers/dispatch/namespaces/fleet/scripts')) {
          return envelope([{ id: 'unrelated-dispatch-script', tags: [] }]);
        }
        if (path.endsWith('/workers/dispatch/namespaces/foreign/scripts')) {
          return envelope([{ id: 'foreign-dispatch-script', tags: [] }]);
        }
        if (
          path.endsWith(
            '/workers/dispatch/namespaces/fleet/scripts/unrelated-dispatch-script/settings',
          )
        ) {
          return envelope({
            bindings: [{ type: 'd1', name: 'DB', database_id: 'db-target' }],
          });
        }
        if (
          path.endsWith(
            '/workers/dispatch/namespaces/foreign/scripts/foreign-dispatch-script/settings',
          )
        ) {
          return envelope({
            bindings: [{ type: 'd1', name: 'DB', database_id: 'db-target' }],
          });
        }
        throw new Error(`unexpected request ${path}`);
      },
    });

    await expect(
      client.listWorkerDatabaseAttachments('db-target'),
    ).resolves.toEqual([
      {
        scriptName: 'unrelated-dispatch-script',
        plane: 'dispatch',
        dispatchNamespace: 'fleet',
      },
      {
        scriptName: 'foreign-dispatch-script',
        plane: 'dispatch',
        dispatchNamespace: 'foreign',
      },
      { scriptName: 'outside-fleet-prefix', plane: 'ordinary' },
    ]);
    await expect(
      client.listWorkerDatabaseAttachments('db-zero'),
    ).resolves.toEqual([
      { scriptName: 'outside-fleet-prefix', plane: 'ordinary' },
    ]);
    expect(inspectedOrdinaryVersions).toEqual([
      'live-without-target-db',
      'live-with-target-db',
      'live-without-target-db',
      'live-with-target-db',
      'zero-percent-candidate',
    ]);
  });

  it('fails closed when an enumerated Worker cannot be identified or its current deployment is malformed', async () => {
    const malformedScriptClient = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/workers/scripts')) {
          return envelope([{}]);
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });
    await expect(
      malformedScriptClient.listWorkerDatabaseAttachments('db-target'),
    ).rejects.toThrow(/without an id/);

    const malformedDeploymentClient = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/workers/scripts')) {
          return envelope([{ id: 'ordinary' }]);
        }
        if (url.pathname.endsWith('/workers/scripts/ordinary/deployments')) {
          return envelope({ deployments: [{ versions: [] }] });
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });
    await expect(
      malformedDeploymentClient.listWorkerDatabaseAttachments('db-target'),
    ).rejects.toThrow(/had no versions/);

    const malformedNamespaceClient = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/workers/scripts')) return envelope([]);
        if (url.pathname.endsWith('/workers/dispatch/namespaces')) {
          return envelope([{}]);
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });
    await expect(
      malformedNamespaceClient.listWorkerDatabaseAttachments('db-target'),
    ).rejects.toThrow(/unidentified namespace/);
  });

  it('forwards anonymous and numbered D1 parameters without rewriting SQL text', async () => {
    const bodies: unknown[] = [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        if (url.pathname.endsWith('/d1/database/database-id/query')) {
          bodies.push(JSON.parse(String(init?.body)));
          return envelope([{ success: true, results: [] }]);
        }
        throw new Error(`unexpected request ${url.pathname}`);
      },
    });
    const mixedSql = `SELECT ? AS anonymous, ?2 AS numbered, '?' AS literal
      -- comment ?3
      /* block ?4 */`;

    await fenced(client, () =>
      client.queryDatabase('database-id', mixedSql, ['7', 'value']),
    );
    await fenced(client, () =>
      client.batchDatabase('database-id', [
        { sql: 'SELECT ?1 AS first, ?2 AS second', bindings: ['1', ''] },
      ]),
    );

    expect(bodies).toEqual([
      { sql: mixedSql, params: ['7', 'value'] },
      {
        batch: [
          {
            sql: 'SELECT ?1 AS first, ?2 AS second',
            params: ['1', ''],
          },
        ],
      },
    ]);
  });

  it('rejects non-string REST D1 parameters instead of changing their SQL types', async () => {
    const fetch = vi.fn<typeof globalThis.fetch>();
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch,
    });

    await expect(
      fenced(client, () =>
        client.queryDatabase('database-id', 'SELECT ?', [
          null,
        ] as unknown as string[]),
      ),
    ).rejects.toThrow('D1 query bindings must be strings');
    await expect(
      fenced(client, () =>
        client.batchDatabase('database-id', [
          {
            sql: 'SELECT ?',
            bindings: [true] as unknown as string[],
          },
        ]),
      ),
    ).rejects.toThrow('D1 batch bindings must be strings');
    expect(fetch).not.toHaveBeenCalled();
  });

  it('exposes a fenced custom-domain adapter for the plain Worker backend', async () => {
    const mutations: string[] = [];
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      rateCoordinator: testRateCoordinator(),
      dispatchNamespace: 'fleet',
      fetch: async (input, init) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        const method = init?.method ?? 'GET';
        if (url.pathname.endsWith('/workers/domains') && method === 'GET') {
          return envelope([
            {
              id: 'domain-id',
              hostname: 'acme.example.test',
              service: 'acme-production',
            },
          ]);
        }
        if (url.pathname.endsWith('/workers/domains') && method === 'PUT') {
          mutations.push('attach');
          return envelope({
            id: 'domain-id',
            hostname: 'acme.example.test',
            service: 'acme-production',
          });
        }
        if (
          url.pathname.endsWith('/workers/domains/domain-id') &&
          method === 'DELETE'
        ) {
          mutations.push('detach');
          return envelope({ success: true, errors: [], messages: [] });
        }
        throw new Error(`unexpected request ${method} ${url.pathname}`);
      },
    });
    const fence = {
      mutationLeaseTtlMs: 15 * 60_000,
      assertOwned: vi.fn(async () => {}),
    };

    await expect(client.listCustomDomains()).resolves.toEqual([
      {
        id: 'domain-id',
        hostname: 'acme.example.test',
        service: 'acme-production',
      },
    ]);
    await client.attachCustomDomain(
      { hostname: 'acme.example.test', service: 'acme-production' },
      fence,
    );
    await client.detachCustomDomain('domain-id', fence);

    expect(mutations).toEqual(['attach', 'detach']);
    expect(fence.assertOwned).toHaveBeenCalledTimes(2);
  });
});
