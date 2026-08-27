// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { CloudflareApiPlainWorkerBackend } from '../src/cloudflare-api-plain-worker-backend.js';
import { CloudflareProvisioningClient } from '../src/cloudflare-client.js';
import { WorkerDeploymentError } from '../src/deployment-error.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  DatabaseReference,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
} from '../src/types.js';
import {
  type CloudflareFixtureHandler,
  type ProviderWorld,
  pageArray,
  providerWorld,
  recordingFetch,
  restProjection,
  single,
  testRateCoordinator,
  zoneAuthorityResponse,
} from './fixtures/cloudflare-fetch-fixture.js';
import { memoryStore } from './fixtures/plain-worker-port-probe.js';

const baseSpec: DeploymentSpec = {
  tenantTag: 'acme',
  environment: 'production',
  scriptName: 'acme-production',
  databaseName: 'acme-production',
  compatibilityDate: '2026-08-26',
  compatibilityFlags: ['nodejs_compat'],
  mainModule: 'worker.js',
  modules: [{ name: 'worker.js', content: 'export default { version: 1 }' }],
  authoredBy: 'platform',
  schemaVersion: 1,
  migrations: [],
  durableObjectMigrations: [],
  durableObjectBindings: [],
  maintenanceBaseUrl: 'https://control.example.test',
  routeHostname: 'app.example.test',
};

const nextSpec: DeploymentSpec = {
  ...baseSpec,
  modules: [{ name: 'worker.js', content: 'export default { version: 2 }' }],
};

const database: DatabaseReference = {
  id: 'database-1',
  name: baseSpec.databaseName,
  created: true,
};

const secrets: DeploymentSecrets = {
  deploymentIdentity: 'deployment-identity-secret-value-0001',
  maintenanceAdmin: 'maintenance-admin-secret-value-00001',
};

function fence(): ExternalMutationFence {
  return {
    mutationLeaseTtlMs: 15 * 60_000,
    assertOwned: vi.fn(async () => {}),
  };
}

function maintenanceResponse(spec: DeploymentSpec): Response {
  return Response.json({
    nextSweepAt: 2_000,
    nextPurgeAt: 3_000,
    alarmAt: 2_000,
    lastSweepAt: 1_000,
    deploymentSpecDigest: deploymentSpecDigest(spec),
  });
}

function projectedHandler(
  world: ProviderWorld,
  options: { readonly failInitialSubdomain?: boolean } = {},
): CloudflareFixtureHandler {
  const projected = restProjection(world);
  const domains: Array<{ id: string; hostname: string; service: string }> = [];
  return async (request) => {
    const url = new URL(request.url);
    const authority = zoneAuthorityResponse(url, []);
    if (authority) return authority;
    const body =
      request.body && typeof request.body === 'object' ? request.body : {};
    if (url.pathname.endsWith('/workers/domains')) {
      if (request.method === 'GET') return pageArray(domains);
      if (request.method === 'PUT') {
        const hostname = Reflect.get(body, 'hostname');
        const service = Reflect.get(body, 'service');
        if (typeof hostname === 'string' && typeof service === 'string') {
          domains.splice(0, domains.length, {
            id: 'domain-1',
            hostname,
            service,
          });
        }
        return single({ id: 'domain-1' });
      }
    }
    if (request.method === 'GET' && url.pathname.endsWith('/workers/scripts')) {
      return pageArray([...world.scripts.keys()].map((id) => ({ id })));
    }
    if (url.pathname.endsWith('/secrets') && request.method === 'GET') {
      if (url.searchParams.has('page')) return pageArray([]);
      const scriptName = url.pathname.split('/').at(-2);
      const script = scriptName ? world.scripts.get(scriptName) : undefined;
      const names = (script?.versions[0]?.bindings ?? []).flatMap((binding) =>
        binding &&
        typeof binding === 'object' &&
        Reflect.get(binding, 'type') === 'secret_text' &&
        typeof Reflect.get(binding, 'name') === 'string'
          ? [{ name: Reflect.get(binding, 'name') }]
          : [],
      );
      return pageArray(names);
    }
    const script = world.scripts.get(baseSpec.scriptName);
    if (
      options.failInitialSubdomain &&
      request.method === 'POST' &&
      url.pathname.endsWith('/subdomain') &&
      script?.versions.length === 1
    ) {
      return Response.json(
        { success: false, errors: [{ code: 1, message: 'subdomain failed' }] },
        { status: 500 },
      );
    }
    const response = await projected(request);
    const updatedScript = world.scripts.get(baseSpec.scriptName);
    if (
      request.method === 'PUT' &&
      url.pathname.endsWith(`/workers/scripts/${baseSpec.scriptName}`) &&
      updatedScript?.versions[0]
    ) {
      updatedScript.deployment = [
        { versionId: updatedScript.versions[0].versionId, percentage: 100 },
      ];
    }
    return response;
  };
}

function subject(
  handler: CloudflareFixtureHandler,
  maintenanceFetch: typeof fetch = async () => maintenanceResponse(nextSpec),
) {
  const fixture = recordingFetch(handler);
  const client = new CloudflareProvisioningClient({
    accountId: 'account',
    apiToken: 'token',
    plane: 'plain-worker',
    rateCoordinator: testRateCoordinator(),
    fetch: fixture.fetch,
    requestTimeoutMs: 1_000,
    exportStore: memoryStore(),
  });
  return {
    backend: new CloudflareApiPlainWorkerBackend({
      client,
      fetch: maintenanceFetch,
      maintenanceRequestTimeoutMs: 1_000,
    }),
    fixture,
  };
}

describe('CloudflareApiPlainWorkerBackend', () => {
  it('requires a Cloudflare client instance and exposes the plain-worker kind', () => {
    expect(
      () =>
        new CloudflareApiPlainWorkerBackend({
          client: {} as CloudflareProvisioningClient,
        }),
    ).toThrow('client must be a CloudflareProvisioningClient instance');
    const { backend } = subject(projectedHandler(providerWorld()));
    expect(backend.kind).toBe('plain-worker');
  });

  it('passes its identity caller token into deployment-identity refusals', async () => {
    const { backend, fixture } = subject(projectedHandler(providerWorld()));

    await expect(
      backend.seedDeploymentIdentity(database, 'INVALID TAG', fence(), {
        initialExecutionFenceState: 'open',
      }),
    ).rejects.toThrow('CloudflareApiPlainWorkerBackend.seedDeploymentIdentity');
    expect(fixture.requests).toHaveLength(0);
  });

  it('refuses a reconciled initial upload whose public access did not converge', async () => {
    const world = providerWorld();
    const { backend, fixture } = subject(
      projectedHandler(world, { failInitialSubdomain: true }),
      async () => maintenanceResponse(baseSpec),
    );

    const error = await backend
      .deployWorker(baseSpec, database, secrets, undefined, fence())
      .catch((cause: unknown) => cause);
    expect(error).toMatchObject<Partial<WorkerDeploymentError>>({
      name: 'WorkerDeploymentError',
      createdByAttempt: true,
    });
    expect(error).toBeInstanceOf(WorkerDeploymentError);
    const causes =
      error instanceof WorkerDeploymentError &&
      error.cause instanceof AggregateError
        ? error.cause.errors
        : [];
    expect(causes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          message: expect.stringContaining(
            "reconciled Worker upload for 'acme-production' did not converge public access",
          ),
        }),
      ]),
    );
    expect(
      fixture.requests.some(
        ({ method, url }) =>
          method === 'POST' && new URL(url).pathname.endsWith('/subdomain'),
      ),
    ).toBe(true);
    const script = world.scripts.get(baseSpec.scriptName);
    expect(script).toBeDefined();
    expect(script?.versions.length).toBeGreaterThan(0);
  });

  it('runs an initial deploy, staged maintenance, promotion, and ready inspection over REST', async () => {
    const world = providerWorld();
    const maintenanceFetch = vi.fn(async () => maintenanceResponse(nextSpec));
    const { backend, fixture } = subject(
      projectedHandler(world),
      maintenanceFetch,
    );
    const owned = fence();

    const initial = await backend.deployWorker(
      baseSpec,
      database,
      secrets,
      undefined,
      owned,
    );
    const staged = await backend.deployWorker(
      nextSpec,
      database,
      secrets,
      undefined,
      owned,
    );
    expect(world.scripts.get(baseSpec.scriptName)?.deployment).toEqual([
      { versionId: initial.artifactVersion, percentage: 100 },
      { versionId: staged.artifactVersion, percentage: 0 },
    ]);

    await backend.ensureMaintenance(
      nextSpec,
      secrets.maintenanceAdmin,
      owned,
      staged.artifactVersion,
    );
    await backend.promoteWorker(
      nextSpec,
      { allowedCurrentScriptNames: [nextSpec.scriptName], allowUnrouted: true },
      undefined,
      owned,
      staged.artifactVersion,
    );
    await expect(
      backend.inspect(
        nextSpec,
        secrets.maintenanceAdmin,
        staged.artifactVersion,
      ),
    ).resolves.toMatchObject({
      artifactVersion: staged.artifactVersion,
      desiredSpecDigest: deploymentSpecDigest(nextSpec),
      databaseId: database.id,
      maintenance: { armed: true },
    });

    expect(maintenanceFetch).toHaveBeenCalledTimes(2);
    const accountPrefix = '/client/v4/accounts/account';
    expect(
      fixture.requests.map(({ method, url }) => {
        const pathname = new URL(url).pathname;
        if (!pathname.startsWith(accountPrefix)) {
          throw new Error(
            `recorded provider path lacks account prefix: ${pathname}`,
          );
        }
        return `${method} ${pathname.slice(accountPrefix.length)}`;
      }),
    ).toEqual([
      'GET /workers/scripts/acme-production/deployments',
      'GET /workers/scripts/acme-production/versions',
      'PUT /workers/scripts/acme-production',
      'POST /workers/scripts/acme-production/subdomain',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions/version-1',
      'GET /workers/scripts/acme-production/deployments',
      'GET /workers/scripts/acme-production/deployments',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions/version-1',
      'GET /workers/scripts/acme-production/subdomain',
      'POST /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions/version-2',
      'GET /workers/scripts/acme-production/deployments',
      'POST /workers/scripts/acme-production/deployments',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions/version-2',
      'GET /workers/scripts/acme-production/deployments',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions/version-2',
      'GET /workers/scripts/acme-production/deployments',
      'GET /workers/domains',
      'POST /workers/scripts/acme-production/deployments',
      'GET /workers/domains',
      'PUT /workers/domains',
      'GET /workers/domains',
      'GET /workers/scripts/acme-production/deployments',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions',
      'GET /workers/scripts/acme-production/versions/version-2',
      'GET /workers/scripts/acme-production/versions/version-2',
      'GET /workers/scripts/acme-production/secrets',
    ]);
  });
});
