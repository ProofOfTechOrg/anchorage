// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type {
  D1Database,
  DurableObjectNamespace,
  R2Bucket,
} from '@cloudflare/workers-types';
import {
  DEPLOYMENT_IDENTITY_HEADER,
  seedDeploymentIdentity,
} from '@proofoftech/flowsafe/do-runner';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  unstable_splitSqlQuery as splitSqlQuery,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';
import {
  directDeploymentSpec,
  generateDirectDeploymentSecrets,
} from '../scripts/direct-credentialed-spec.js';
import {
  DIRECT_TENANT_OBJECT_BODY,
  DIRECT_TENANT_OBJECT_KEY,
} from '../scripts/direct-credentialed-tenant-object.mjs';
import { plainWorkerIngressModule } from '../src/plain-worker-backend.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import {
  DIRECT_FIXTURE_PROVIDER,
  directFixtureManifest,
} from './fixtures/direct-credentialed-config.js';
import { createDirectReferenceHarness } from './fixtures/direct-reference-harness.js';

const manifest = directFixtureManifest();
const secrets = generateDirectDeploymentSecrets();
const initial = directDeploymentSpec(
  manifest,
  'a',
  'initial',
  secrets,
  DIRECT_FIXTURE_PROVIDER,
);
const next = directDeploymentSpec(
  manifest,
  'a',
  'next',
  secrets,
  DIRECT_FIXTURE_PROVIDER,
);
const applicationHeaders = {
  authorization: `Bearer ${secrets.application?.APP_PROBE_TOKEN}`,
};
const maintenanceHeaders = {
  authorization: `Bearer ${secrets.maintenanceAdmin}`,
};

interface HarnessBindings {
  DB: D1Database;
  PROBE_BUCKET: R2Bucket;
  MAINTENANCE: DurableObjectNamespace;
  RUNNER: DurableObjectNamespace;
}

let directory: string | undefined;
let entrypoint: string | undefined;

function options(
  release: '1' | '2',
  token = secrets.application?.APP_PROBE_TOKEN ?? '',
) {
  if (!entrypoint) throw new Error('test ingress is not prepared');
  return {
    root: fileURLToPath(new URL('..', import.meta.url)),
    workers: [
      {
        config: {
          name: 'direct-tenant-harness',
          main: entrypoint,
          compatibility_date: '2026-08-06',
          vars: {
            DEPLOYMENT_TENANT: initial.tenantTag,
            DEPLOYMENT_IDENTITY_SECRET: secrets.deploymentIdentity,
            MAINTENANCE_ADMIN_SECRET: secrets.maintenanceAdmin,
            APP_PROBE_TOKEN: token,
            APPLICATION_RELEASE: release,
            FLEET_SPEC_DIGEST: deploymentSpecDigest(
              release === '1' ? initial : next,
            ),
          },
          d1_databases: [
            {
              binding: 'DB',
              database_name: 'direct-tenant-harness',
              database_id: '00000000-0000-0000-0000-000000000000',
            },
          ],
          r2_buckets: [
            { binding: 'PROBE_BUCKET', bucket_name: 'direct-tenant-harness' },
          ],
          durable_objects: {
            bindings: [
              { name: 'RUNNER', class_name: 'Runner' },
              { name: 'MAINTENANCE', class_name: 'Maintenance' },
            ],
          },
          migrations: [
            { tag: 'v1', new_sqlite_classes: ['Maintenance', 'Runner'] },
          ],
        },
      },
    ],
  } satisfies Parameters<typeof createTestHarness>[0];
}

describe.sequential('direct tenant fixture in workerd', {
  timeout: 30_000,
}, () => {
  let server: TestHarness;
  let worker: WorkerHandle<HarnessBindings>;

  const appFetch = (
    path: string,
    init?: Parameters<WorkerHandle['fetch']>[1],
  ) => worker.fetch(new URL(path, `https://${initial.routeHostname}`), init);
  const controlFetch = (
    path: string,
    init?: Parameters<WorkerHandle['fetch']>[1],
  ) => worker.fetch(new URL(path, initial.maintenanceBaseUrl), init);

  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'fleet-direct-tenant-'));
    const ingress = plainWorkerIngressModule(initial);
    entrypoint = join(directory, ingress.name);
    const source = fileURLToPath(
      new URL('../scripts/direct-credentialed-tenant.ts', import.meta.url),
    );
    await writeFile(
      join(directory, initial.mainModule),
      `export {default, Maintenance, Runner} from ${JSON.stringify(source)};\n`,
    );
    await writeFile(entrypoint, ingress.content);
    server = createTestHarness(options('1'));
    await server.listen();
    worker = server.getWorker<HarnessBindings>();
    const env = await worker.getEnv();
    await seedDeploymentIdentity(env.DB, initial.tenantTag, 'open');
    for (const migration of initial.migrations)
      await env.DB.batch(
        splitSqlQuery(migration.sql).map((sql) => env.DB.prepare(sql)),
      );
  }, 30_000);

  afterAll(async () => {
    try {
      await server?.close();
    } finally {
      if (directory) await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);

  it('authenticates probes and reads real D1 state', async () => {
    for (const headers of [{}, { authorization: 'Bearer wrong' }]) {
      expect((await appFetch('/__direct/health', { headers })).status).toBe(
        401,
      );
      expect(
        (await appFetch('/__direct/object', { method: 'POST', headers }))
          .status,
      ).toBe(401);
    }
    const response = await appFetch('/__direct/health', {
      headers: applicationHeaders,
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ release: '1', marker: 'initial' });
    const env = await worker.getEnv();
    expect((await env.PROBE_BUCKET.list()).objects).toEqual([]);
  });

  it('uses the host maintenance authenticator and actual Durable Object', async () => {
    expect(
      (
        await controlFetch('/admin/ensure-maintenance', {
          method: 'POST',
          headers: applicationHeaders,
        })
      ).status,
    ).toBe(401);
    const ensured = await controlFetch('/admin/ensure-maintenance', {
      method: 'POST',
      headers: maintenanceHeaders,
    });
    expect(ensured.status).toBe(200);
    expect(await ensured.json()).toMatchObject({
      nextSweepAt: expect.any(Number),
      nextPurgeAt: expect.any(Number),
      alarmAt: expect.any(Number),
      deploymentSpecDigest: deploymentSpecDigest(initial),
    });
    const status = await controlFetch('/admin/maintenance-status', {
      headers: maintenanceHeaders,
    });
    expect(status.status).toBe(200);
    expect(await status.json()).toMatchObject({
      nextSweepAt: expect.any(Number),
      nextPurgeAt: expect.any(Number),
      alarmAt: expect.any(Number),
      deploymentSpecDigest: deploymentSpecDigest(initial),
    });
    expect(await worker.listDurableObjectIds('MAINTENANCE')).toHaveLength(1);
  });

  it('admits candidate maintenance on the control origin and rejects version selection on public ingress', async () => {
    const headers = {
      ...maintenanceHeaders,
      'Cloudflare-Workers-Version-Overrides': `${initial.scriptName}="11111111-1111-4111-8111-111111111111"`,
    };
    expect(
      (await appFetch('/admin/ensure-maintenance', { method: 'POST', headers }))
        .status,
    ).toBe(404);
    const response = await controlFetch('/admin/ensure-maintenance', {
      method: 'POST',
      headers,
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      deploymentSpecDigest: deploymentSpecDigest(initial),
      nextSweepAt: expect.any(Number),
    });
    expect(
      (await controlFetch('/__direct/health', { headers: applicationHeaders }))
        .status,
    ).toBe(404);
  });

  it('constructs the real Runner runtime behind its internal identity check', async () => {
    const env = await worker.getEnv();
    const runner = env.RUNNER.get(env.RUNNER.idFromName('fixture:run1'));
    const url = 'https://runner/runs/fixture/run1/start-liveness';
    expect((await runner.fetch(url)).status).toBe(503);
    const response = await runner.fetch(url, {
      headers: { [DEPLOYMENT_IDENTITY_HEADER]: secrets.deploymentIdentity },
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ live: false });
  });

  it('runs retained-token reference probes through the native tenant across reloads', async () => {
    const forwarded: {
      url: string;
      method: string;
      authorization: string | null;
    }[] = [];
    const reference = await createDirectReferenceHarness({
      applicationFetch: async (request) => {
        expect(request.body === undefined || request.body === '').toBe(true);
        forwarded.push({
          url: request.url,
          method: request.method,
          authorization: request.headers.get('authorization'),
        });
        return worker.fetch(request.url, {
          method: request.method,
          headers: [...request.headers],
        });
      },
    });
    const work = await Promise.allSettled([
      (async () => {
        const probeSpec = reference.specs.find(
          (spec) => spec.tenantTag === initial.tenantTag,
        );
        const token = reference.secrets.a.application?.APP_PROBE_TOKEN;
        if (!probeSpec || !token)
          throw new Error('reference fixture role is missing');
        expect(probeSpec.routeHostname).toBe(initial.routeHostname);
        const active = options('1', token);
        const configuration = active.workers[0]?.config;
        if (!configuration)
          throw new Error('test Worker configuration is missing');
        const databaseId = configuration.d1_databases[0]?.database_id;
        if (!databaseId) throw new Error('native tenant database is missing');
        await server.update(active);
        worker = server.getWorker<HarnessBindings>();
        await reference.fleetStore.withDeploymentLease(
          probeSpec.tenantTag,
          probeSpec.environment,
          (lease) =>
            lease.put({
              tenantTag: probeSpec.tenantTag,
              environment: probeSpec.environment,
              backend: 'plain-worker',
              scriptName: probeSpec.scriptName,
              databaseId,
              databaseName: probeSpec.databaseName,
              schemaVersion: probeSpec.schemaVersion,
              desiredSpecDigest: deploymentSpecDigest(probeSpec),
              artifactVersion: 'native-probe-fixture',
              durableObjectBindings: [],
              routeHostname: probeSpec.routeHostname,
              phase: 'ready',
              updatedAt: new Date().toISOString(),
            }),
        );
        const action = (
          operation: 'health' | 'object-put' | 'object-read' | 'object-delete',
        ) => ({ kind: 'tenant-probe', role: 'a', operation }) as const;
        expect(await reference.success(action('health'))).toEqual({
          role: 'a',
          operation: 'health',
          release: '1',
          marker: 'initial',
        });
        const put = await reference.call(action('object-put'));
        expect(reference.bridgeErrors).toEqual([]);
        expect(put.value).toEqual({
          contractVersion: 1,
          configSha256: reference.manifest.configSha256,
          action: 'tenant-probe',
          ok: true,
          result: { role: 'a', operation: 'object-put', returned: true },
        });
        expect(put.response.headers.get('X-Direct-Application-Attempts')).toBe(
          '1',
        );
        expect(put.response.headers.get('X-Direct-Provider-Attempts')).toBe(
          '0',
        );
        expect(put.response.headers.get('X-Direct-Maintenance-Attempts')).toBe(
          '0',
        );
        const expected = {
          role: 'a',
          operation: 'object-read',
          present: true,
          size: Buffer.byteLength(DIRECT_TENANT_OBJECT_BODY),
          sha256: createHash('sha256')
            .update(DIRECT_TENANT_OBJECT_BODY)
            .digest('hex'),
        };
        expect(await reference.success(action('object-read'))).toEqual(
          expected,
        );
        await reference.reload();
        expect(await reference.success(action('object-read'))).toEqual(
          expected,
        );
        await server.update(active);
        worker = server.getWorker<HarnessBindings>();
        expect(await reference.success(action('object-read'))).toEqual(
          expected,
        );
        await reference.success(action('object-delete'));
        expect(await reference.success(action('object-read'))).toEqual({
          role: 'a',
          operation: 'object-read',
          present: false,
        });
        expect(forwarded.length).toBeGreaterThan(0);
        for (const request of forwarded) {
          expect(request.authorization).toBe(`Bearer ${token}`);
          expect(new URL(request.url).origin).toBe(
            `https://${initial.routeHostname}`,
          );
        }
        expect(JSON.stringify(put.value)).not.toContain(token);
        await server.update(options('1', 'wrong-token'));
        worker = server.getWorker<HarnessBindings>();
        expect((await reference.call(action('health'))).value).toEqual({
          contractVersion: 1,
          ok: false,
          error: { code: 'operation-refused' },
        });
      })(),
    ]);
    const cleanup = await Promise.allSettled([
      reference.close(),
      (async () => {
        await server.update(options('1'));
        worker = server.getWorker<HarnessBindings>();
        await (await worker.getEnv()).PROBE_BUCKET.delete(
          DIRECT_TENANT_OBJECT_KEY,
        );
      })(),
    ]);
    const failures = [...work, ...cleanup].flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : [],
    );
    if (failures.length)
      throw new AggregateError(failures, 'native probe or cleanup failed');
  }, 60_000);

  it('writes fixed R2 bytes and retains them across reload and additive D1 migration', async () => {
    expect(
      (
        await appFetch('/__direct/object', {
          method: 'POST',
          headers: applicationHeaders,
          body: 'ignored request body',
        })
      ).status,
    ).toBe(204);
    const body = DIRECT_TENANT_OBJECT_BODY;
    const env = await worker.getEnv();
    expect(
      await (await env.PROBE_BUCKET.get(DIRECT_TENANT_OBJECT_KEY))?.text(),
    ).toBe(body);
    for (const migration of next.migrations.slice(initial.migrations.length))
      await env.DB.batch(
        splitSqlQuery(migration.sql).map((sql) => env.DB.prepare(sql)),
      );
    await server.update(options('2'));
    worker = server.getWorker<HarnessBindings>();
    const health = await appFetch('/__direct/health', {
      headers: applicationHeaders,
    });
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ release: '2', marker: 'next' });
    const object = await appFetch('/__direct/object', {
      headers: applicationHeaders,
    });
    expect(object.status).toBe(200);
    expect(await object.json()).toEqual({
      present: true,
      size: Buffer.byteLength(body),
      sha256: createHash('sha256').update(body).digest('hex'),
    });
    expect(
      (
        await appFetch('/__direct/object', {
          method: 'DELETE',
          headers: applicationHeaders,
        })
      ).status,
    ).toBe(204);
    expect(
      await (
        await appFetch('/__direct/object', { headers: applicationHeaders })
      ).json(),
    ).toEqual({ present: false });
  });

  it('keeps the host identity check before authenticated application probes', async () => {
    const changed = options('2');
    const input = changed.workers[0];
    if (!input) throw new Error('test Worker configuration is missing');
    input.config.vars.DEPLOYMENT_TENANT = manifest.names.roles.b.tenantTag;
    await server.update(changed);
    worker = server.getWorker<HarnessBindings>();
    expect(
      (await appFetch('/__direct/health', { headers: applicationHeaders }))
        .status,
    ).toBe(503);
    await server.update(options('2'));
    worker = server.getWorker<HarnessBindings>();
  });

  it('does not turn a missing application secret into a bearer credential', async () => {
    await server.update(options('2', ''));
    worker = server.getWorker<HarnessBindings>();
    for (const token of [secrets.application?.APP_PROBE_TOKEN, 'undefined', ''])
      expect(
        (
          await appFetch('/__direct/health', {
            headers: { authorization: `Bearer ${token}` },
          })
        ).status,
      ).toBe(401);
  });
});
