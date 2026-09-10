// SPDX-License-Identifier: Apache-2.0

import { fileURLToPath } from 'node:url';
import type { D1Database, R2Bucket } from '@cloudflare/workers-types';
import { seedDeploymentIdentity } from '@proofoftech/flowsafe/do-runner';
import { createTestHarness, type TestHarness } from 'wrangler';
import { deploymentSpecDigest } from '../../src/spec-digest.js';
import type { DeploymentSecrets, DeploymentSpec } from '../../src/types.js';
import dispatchWorker, {
  type FleetDispatchEnv,
} from '../../src/workers/dispatch.js';
import { externalReleaseScriptName } from '../../src/workers-for-platforms-backend.js';

export async function createWfpMaintenanceHarness(
  input: Readonly<{
    catalog: DeploymentSpec;
    external: DeploymentSpec;
    secrets: DeploymentSecrets;
    publicKey: string;
  }>,
) {
  let catalog = input.catalog;
  let catalogEnrolled = true;
  const workerNames = ['wfp-catalog', 'wfp-state', 'wfp-candidate'] as const;
  const main = fileURLToPath(
    new URL('../../scripts/direct-credentialed-tenant.ts', import.meta.url),
  );
  function options() {
    return {
      root: fileURLToPath(new URL('../..', import.meta.url)),
      workers: workerNames.map((name, index) => {
        const spec = name === 'wfp-catalog' ? catalog : input.external;
        const candidate = name === 'wfp-candidate';
        const state = name === 'wfp-state';
        const required = candidate || state || catalogEnrolled;
        return {
          config: {
            name,
            main,
            compatibility_date: '2026-08-06',
            vars: {
              DEPLOYMENT_TENANT: spec.tenantTag,
              FLEET_ENVIRONMENT: spec.environment,
              DEPLOYMENT_IDENTITY_SECRET: input.secrets.deploymentIdentity,
              ...(candidate
                ? {}
                : { MAINTENANCE_ADMIN_SECRET: input.secrets.maintenanceAdmin }),
              ...(required
                ? { FLEET_MAINTENANCE_CAPABILITIES: 'required' }
                : {}),
              FLEET_SPEC_DIGEST: state
                ? 'e'.repeat(64)
                : deploymentSpecDigest(spec),
              ...(!candidate && required
                ? {
                    FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY: input.publicKey,
                    FLEET_DEPLOYMENT_SCRIPT: state
                      ? 'external-stable-state'
                      : spec.scriptName,
                    FLEET_RESOURCE_ROLE: state
                      ? 'platform-state'
                      : 'platform-catalog',
                  }
                : {}),
            },
            d1_databases: [
              {
                binding: 'DB',
                database_name: name,
                database_id: `00000000-0000-0000-0000-00000000003${index}`,
              },
            ],
            durable_objects: {
              bindings: [
                { name: 'RUNNER', class_name: 'Runner' },
                {
                  name: 'MAINTENANCE',
                  class_name: 'Maintenance',
                  ...(candidate ? { script_name: 'wfp-state' } : {}),
                },
              ],
            },
            migrations: [
              {
                tag: 'v1',
                new_sqlite_classes: candidate
                  ? ['Runner']
                  : ['Runner', 'Maintenance'],
              },
            ],
          },
        };
      }),
    } satisfies Parameters<typeof createTestHarness>[0];
  }
  let server: TestHarness;
  let control: TestHarness;
  async function close() {
    const results = await Promise.allSettled([
      (async () => server?.close())(),
      (async () => control?.close())(),
    ]);
    const errors = results.flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : [],
    );
    if (errors.length)
      throw new AggregateError(errors, 'WFP native fixture cleanup failed');
  }
  const calls: Array<{ scriptName: string; options: unknown }> = [];
  const requests: Request[] = [];
  async function targetFetch(
    name: (typeof workerNames)[number],
    request: Request,
  ): Promise<Response> {
    return server.getWorker(name).fetch(request.url, {
      method: request.method,
      headers: [...request.headers],
      ...(request.method === 'GET' || request.method === 'HEAD'
        ? {}
        : { body: await request.arrayBuffer() }),
    });
  }
  const dispatcher: FleetDispatchEnv = {
    FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY: input.publicKey,
    TENANT_CPU_LIMIT_MS: '1000',
    TENANT_SUBREQUEST_LIMIT: '50',
    HOSTS: {
      async get() {
        throw new Error('maintenance unexpectedly consulted host routing');
      },
    },
    DISPATCH: {
      get(scriptName, _arguments, options) {
        calls.push({ scriptName, options });
        const name =
          scriptName === catalog.scriptName
            ? 'wfp-catalog'
            : scriptName === externalReleaseScriptName(input.external)
              ? 'wfp-candidate'
              : undefined;
        if (!name) throw new Error('unexpected fixture dispatch target');
        return { fetch: (request) => targetFetch(name, request) };
      },
    },
  };
  const requestFetch: typeof fetch = async (input, init) => {
    const request = new Request(input, init);
    requests.push(request.clone());
    return dispatchWorker.fetch(request, dispatcher);
  };
  try {
    server = createTestHarness(options());
    control = createTestHarness({
      root: fileURLToPath(new URL('../..', import.meta.url)),
      workers: [
        {
          config: {
            name: 'wfp-control',
            main: fileURLToPath(
              new URL('../../src/workers/dispatch.ts', import.meta.url),
            ),
            compatibility_date: '2026-08-06',
            d1_databases: [
              {
                binding: 'DB',
                database_name: 'wfp-control',
                database_id: '00000000-0000-0000-0000-000000000034',
              },
            ],
            r2_buckets: [{ binding: 'EXPORTS', bucket_name: 'fleet-exports' }],
          },
        },
      ],
    });
    await server.listen();
    await control.listen();
    for (const name of workerNames) {
      const env = await server.getWorker<{ DB: D1Database }>(name).getEnv();
      await seedDeploymentIdentity(
        env.DB,
        name === 'wfp-catalog' ? catalog.tenantTag : input.external.tenantTag,
        'open',
      );
    }
  } catch (error) {
    try {
      await close();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        'WFP fixture startup and cleanup failed',
      );
    }
    throw error;
  }
  return {
    fetch: requestFetch,
    requests,
    calls,
    catalogFetch: (request: Request) => targetFetch('wfp-catalog', request),
    async updateCatalog(spec: DeploymentSpec, enrolled = true) {
      catalog = spec;
      catalogEnrolled = enrolled;
      await server.update(options());
    },
    async fleetDatabase() {
      return (await control.getWorker<{ DB: D1Database }>().getEnv()).DB;
    },
    async exportBucket() {
      return (await control.getWorker<{ EXPORTS: R2Bucket }>().getEnv())
        .EXPORTS;
    },
    close,
  };
}
