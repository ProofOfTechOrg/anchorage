// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type {
  D1Database,
  DurableObjectNamespace,
  R2Bucket,
} from '@cloudflare/workers-types';
import { DEPLOYMENT_IDENTITY_HEADER } from '@proofoftech/flowsafe/do-runner';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';
import type { DirectRunManifest } from '../../scripts/direct-credentialed-conformance-preflight.mjs';
import type { CloudflareDeploymentSpec } from '../../src/cloudflare-control-plane.js';
import { plainWorkerIngressModule } from '../../src/plain-worker-backend.js';
import { deploymentSpecDigest } from '../../src/spec-digest.js';
import type { CloudflareFixtureRequest } from './cloudflare-fetch-fixture.js';

interface NativeTenantBindings {
  DB: D1Database;
  PROBE_BUCKET: R2Bucket;
  MAINTENANCE: DurableObjectNamespace;
  RUNNER: DurableObjectNamespace;
}

export interface DirectNativeTenantGeneration {
  readonly databaseId: string;
  readonly maintenanceNamespaceId: string;
  readonly runnerNamespaceId: string;
  readonly workerName: string;
  readonly versionId: string;
  readonly release: '1' | '2';
}

const providerJson = (result: unknown) =>
  Response.json({ success: true, errors: [], messages: [], result });

function workerName(
  databaseId: string,
  maintenanceNamespaceId: string,
  runnerNamespaceId: string,
) {
  const digest = createHash('sha256')
    .update(
      JSON.stringify([databaseId, maintenanceNamespaceId, runnerNamespaceId]),
    )
    .digest('hex');
  return `direct-tenant-${digest.slice(0, 24)}`;
}

export async function createDirectNativeTenant(manifest: DirectRunManifest) {
  const directory = await mkdtemp(join(tmpdir(), 'direct-native-tenant-'));
  const storageMain = join(directory, 'storage.mjs');
  await writeFile(
    storageMain,
    "export default {fetch(){return new Response('storage-host')}};\n",
  );
  let server: TestHarness | undefined;
  let worker: WorkerHandle<NativeTenantBindings> | undefined;
  let databaseId: string | undefined;
  let generation: DirectNativeTenantGeneration | undefined;
  let bucketName: string | undefined;
  let closed = false;

  const storageOptions = (selectedDatabaseId: string) =>
    ({
      root: directory,
      workers: [
        {
          config: {
            name: `direct-storage-${createHash('sha256')
              .update(selectedDatabaseId)
              .digest('hex')
              .slice(0, 24)}`,
            main: storageMain,
            compatibility_date: manifest.deploymentRuntime.compatibilityDate,
            compatibility_flags: manifest.deploymentRuntime.compatibilityFlags,
            d1_databases: [
              {
                binding: 'DB',
                database_name: 'direct-native-tenant',
                database_id: selectedDatabaseId,
              },
            ],
            r2_buckets: [
              { binding: 'PROBE_BUCKET', bucket_name: 'direct-native-probe' },
            ],
          },
        },
      ],
    }) satisfies Parameters<typeof createTestHarness>[0];

  async function ensureStorage(selectedDatabaseId: string) {
    if (closed) throw new Error('native tenant is closed');
    if (databaseId && databaseId !== selectedDatabaseId) {
      await server?.close();
      server = undefined;
      worker = undefined;
      generation = undefined;
      bucketName = undefined;
    }
    if (!server) {
      server = createTestHarness(storageOptions(selectedDatabaseId));
      await server.listen();
      worker = server.getWorker<NativeTenantBindings>();
      databaseId = selectedDatabaseId;
    }
    return worker as WorkerHandle<NativeTenantBindings>;
  }

  async function activate(input: {
    spec: CloudflareDeploymentSpec;
    versionId: string;
    databaseId: string;
    maintenanceNamespaceId: string;
    runnerNamespaceId: string;
    deploymentIdentitySecret: string;
    maintenanceAdminSecret: string;
    applicationToken: string;
    release: '1' | '2';
    bucketName: string;
  }) {
    await ensureStorage(input.databaseId);
    const mainPath = join(directory, manifest.tenantModule.name);
    await writeFile(mainPath, manifest.tenantModule.source);
    for (const module of manifest.tenantWasm)
      await writeFile(join(directory, module.name), module.base64, 'base64');
    const ingress = plainWorkerIngressModule(input.spec);
    const ingressPath = join(directory, ingress.name);
    await writeFile(ingressPath, ingress.content);
    const selectedWorkerName = workerName(
      input.databaseId,
      input.maintenanceNamespaceId,
      input.runnerNamespaceId,
    );
    const options = {
      root: directory,
      workers: [
        {
          config: {
            name: selectedWorkerName,
            main: ingressPath,
            compatibility_date: input.spec.compatibilityDate,
            compatibility_flags: input.spec.compatibilityFlags,
            vars: {
              DEPLOYMENT_TENANT: input.spec.tenantTag,
              DEPLOYMENT_IDENTITY_SECRET: input.deploymentIdentitySecret,
              MAINTENANCE_ADMIN_SECRET: input.maintenanceAdminSecret,
              APP_PROBE_TOKEN: input.applicationToken,
              APPROVAL_ALLOW_SELF_DECISION: 'true',
              APPLICATION_RELEASE: input.release,
              FLEET_SPEC_DIGEST: deploymentSpecDigest(input.spec),
            },
            d1_databases: [
              {
                binding: 'DB',
                database_name: input.spec.databaseName,
                database_id: input.databaseId,
              },
            ],
            r2_buckets: [
              { binding: 'PROBE_BUCKET', bucket_name: input.bucketName },
            ],
            durable_objects: {
              bindings: [
                {
                  name: 'RUNNER',
                  class_name: 'Runner',
                },
                {
                  name: 'MAINTENANCE',
                  class_name: 'Maintenance',
                },
              ],
            },
            migrations: [
              {
                tag: 'v1',
                new_sqlite_classes: ['Maintenance', 'Runner'],
              },
            ],
          },
        },
      ],
    } satisfies Parameters<TestHarness['update']>[0];
    await server?.update(options);
    worker = server?.getWorker<NativeTenantBindings>();
    if (!worker) throw new Error('native tenant Worker is missing');
    databaseId = input.databaseId;
    bucketName = input.bucketName;
    generation = {
      databaseId: input.databaseId,
      maintenanceNamespaceId: input.maintenanceNamespaceId,
      runnerNamespaceId: input.runnerNamespaceId,
      workerName: selectedWorkerName,
      versionId: input.versionId,
      release: input.release,
    };
    return generation;
  }

  async function providerRequest(request: CloudflareFixtureRequest) {
    const url = new URL(request.url);
    const query = url.pathname.match(/\/d1\/database\/([^/]+)\/query$/u);
    if (request.method === 'POST' && query?.[1]) {
      const selectedWorker = await ensureStorage(query[1]);
      const environment = await selectedWorker.getEnv();
      const body = request.body as {
        sql?: unknown;
        params?: unknown;
        batch?: unknown;
      };
      if (Array.isArray(body.batch)) {
        const statements = body.batch.map((entry) => {
          const value = entry as { sql?: unknown; params?: unknown };
          if (typeof value.sql !== 'string' || !Array.isArray(value.params))
            throw new Error('invalid native D1 batch');
          return environment.DB.prepare(value.sql).bind(...value.params);
        });
        const results = await environment.DB.batch(statements);
        return providerJson(
          results.map((batchResult) => ({
            success: batchResult.success,
            results: batchResult.results ?? [],
          })),
        );
      }
      if (typeof body.sql !== 'string' || !Array.isArray(body.params))
        throw new Error('invalid native D1 query');
      const result = await environment.DB.prepare(body.sql)
        .bind(...body.params)
        .all();
      return providerJson([
        { success: result.success, results: result.results },
      ]);
    }
    const objects = url.pathname.match(/\/r2\/buckets\/([^/]+)\/objects$/u);
    if (
      request.method === 'GET' &&
      objects?.[1] &&
      decodeURIComponent(objects[1]) === bucketName &&
      worker
    ) {
      const environment = await worker.getEnv();
      const listed = await environment.PROBE_BUCKET.list({
        prefix: url.searchParams.get('prefix') ?? '',
        limit: 1,
      });
      return providerJson(listed.objects.map(({ key }) => ({ key })));
    }
    return undefined;
  }

  async function fetch(request: CloudflareFixtureRequest) {
    if (!worker || !generation)
      return new Response('native tenant unavailable', { status: 503 });
    return worker.fetch(request.url, {
      method: request.method,
      headers: [...request.headers],
      body:
        request.method === 'GET' || request.method === 'HEAD'
          ? undefined
          : typeof request.body === 'string'
            ? request.body
            : JSON.stringify(request.body),
    });
  }

  function deactivate() {
    generation = undefined;
  }

  async function close() {
    if (closed) return;
    closed = true;
    try {
      await server?.close();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  return {
    activate,
    providerRequest,
    fetch,
    deactivate,
    get generation() {
      return generation;
    },
    async database() {
      const selectedWorker = worker;
      if (!selectedWorker) throw new Error('native tenant storage is absent');
      return (await selectedWorker.getEnv()).DB;
    },
    async runnerLiveness(deploymentIdentitySecret: string) {
      const selectedWorker = worker;
      if (!selectedWorker) throw new Error('native tenant storage is absent');
      const environment = await selectedWorker.getEnv();
      const runner = environment.RUNNER.get(
        environment.RUNNER.idFromName('fixture:liveness'),
      );
      return runner.fetch(
        'https://runner/runs/fixture/liveness/start-liveness',
        {
          headers: {
            [DEPLOYMENT_IDENTITY_HEADER]: deploymentIdentitySecret,
          },
        },
      );
    },
    close,
  };
}

export type DirectNativeTenant = Awaited<
  ReturnType<typeof createDirectNativeTenant>
>;
