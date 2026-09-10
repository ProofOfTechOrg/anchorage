// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer, type Server } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import type { D1Database, R2Bucket } from '@cloudflare/workers-types';
import { expect } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';
import { directDeploymentSpec } from '../../scripts/direct-credentialed-spec.js';
import {
  DIRECT_REFERENCE_PATH,
  type DirectReferenceAction,
} from '../../scripts/direct-reference-contract.mjs';
import { DirectReferenceJournal } from '../../scripts/direct-reference-journal.js';
import { D1FleetStateDatabase } from '../../src/d1-fleet-state-database.js';
import { D1FleetStateStore } from '../../src/state-store.js';
import type { DeploymentSecrets } from '../../src/types.js';
import {
  type CloudflareFixtureRequest,
  recordingFetch,
  restProjection,
  single,
} from './cloudflare-fetch-fixture.js';
import { directFixtureManifest } from './direct-credentialed-config.js';
import { maintenanceResponder, providerWorld } from './provider-world.js';

export async function createDirectReferenceHarness(
  policy: Readonly<{
    maintenanceNow?: () => number;
    applicationFetch?: (request: CloudflareFixtureRequest) => Promise<Response>;
    providerResponse?: (
      request: CloudflareFixtureRequest,
      response: Response,
    ) => Promise<Response>;
  }> = {},
) {
  const manifest = directFixtureManifest();
  const roles = ['a', 'b', 'recovery'] as const;
  function fixtureSecrets(role: string): DeploymentSecrets {
    return {
      deploymentIdentity: `identity-${role}`.padEnd(40, 'i'),
      maintenanceAdmin: `maintenance-${role}`.padEnd(40, 'm'),
      application: { APP_PROBE_TOKEN: `probe-${role}`.padEnd(40, 'p') },
    };
  }
  const secrets = {
    a: fixtureSecrets('a'),
    b: fixtureSecrets('b'),
    recovery: fixtureSecrets('recovery'),
  };
  const binding = {
    version: 1,
    accountId: 'account',
    fleetDatabaseId: '00000000-0000-0000-0000-000000000011',
    quotaDatabaseId: '00000000-0000-0000-0000-000000000012',
    exportBucketName: manifest.names.exportBucket,
    referenceModuleSetSha256: 'c'.repeat(64),
    accountWorkersDevSubdomain: 'direct-fixture',
  };
  const specs = roles.map((role) =>
    directDeploymentSpec(manifest, role, 'initial', secrets[role], binding),
  );

  let directory: string;
  let server: TestHarness;
  let bridge: Server;
  let db: D1Database;
  let fleetStore: D1FleetStateStore;
  let applicationBytes: R2Bucket;
  let exportBytes: R2Bucket;
  const world = providerWorld('uuid');
  const bridgeErrors: unknown[] = [];
  const sqlFailures: string[] = [];
  const buckets = new Map<
    string,
    { name: string; jurisdiction: string; creation_date: string }
  >();
  const rest = restProjection(world);
  async function providerRest(
    request: CloudflareFixtureRequest,
  ): Promise<Response> {
    try {
      const response = await rest(request);
      return policy.providerResponse
        ? await policy.providerResponse(request, response)
        : response;
    } catch (error) {
      if (
        new URL(request.url).pathname.endsWith('/query') &&
        error instanceof Error &&
        'code' in error &&
        error.code === 'ERR_SQLITE_ERROR'
      ) {
        sqlFailures.push(error.message);
        return Response.json(
          {
            success: false,
            errors: [{ code: 1, message: 'fixture SQL query failed' }],
          },
          { status: 400 },
        );
      }
      throw error;
    }
  }
  const projection = recordingFetch(async (request) => {
    const url = new URL(request.url);
    if (
      policy.applicationFetch &&
      specs.some(
        (candidate) => url.origin === `https://${candidate.routeHostname}`,
      ) &&
      (url.pathname === '/__direct/health' ||
        url.pathname === '/__direct/object')
    )
      return policy.applicationFetch(request);
    const spec = specs.find(
      (candidate) => candidate.maintenanceBaseUrl === url.origin,
    );
    if (spec) {
      const override = request.headers.get(
        'Cloudflare-Workers-Version-Overrides',
      );
      const script = world.scripts.get(spec.scriptName);
      if (override) {
        const selected = override.match(/^([^=]+)="([^"]+)"$/u);
        if (
          selected?.[1] !== spec.scriptName ||
          !script?.versions.some((version) => version.versionId === selected[2])
        )
          return new Response('invalid fixture version', { status: 409 });
      }
      const view = new Proxy(world, {
        get(target, key) {
          if (key === 'maintenanceOrigin') return spec.maintenanceBaseUrl;
          if (key === 'routeOrigin') return `https://${spec.routeHostname}`;
          if (key === 'scripts')
            return new Map(script ? [[spec.scriptName, script]] : []);
          const value = Reflect.get(target, key);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
      const response =
        (await maintenanceResponder(view, request)) ??
        new Response('unknown fixture maintenance route', { status: 404 });
      if (!response.ok || !policy.maintenanceNow) return response;
      const body = (await response.json()) as Record<string, unknown>;
      const now = policy.maintenanceNow();
      return Response.json({
        ...body,
        alarmAt: now + 60_000,
        lastSweepAt: now,
        lastPurgeAt: now,
      });
    }
    if (url.origin === 'https://d1-export.example.test') {
      expect(request.headers.has('Authorization')).toBe(false);
      return providerRest(request);
    }
    if (url.origin !== 'https://api.cloudflare.com')
      throw new Error('unexpected fixture origin');
    const match = url.pathname.match(
      /^\/client\/v4\/accounts\/account\/r2\/buckets(?:\/([^/]+)(\/objects)?)?$/u,
    );
    if (!match) return providerRest(request);
    const jurisdiction = request.headers.get('cf-r2-jurisdiction') ?? 'default';
    const name = match[1] ? decodeURIComponent(match[1]) : undefined;
    if (!name && request.method === 'POST') {
      const requested = (request.body as { name?: unknown }).name;
      const records = await Promise.all(
        specs.map((spec) => fleetStore.get(spec.tenantTag, spec.environment)),
      );
      if (
        typeof requested !== 'string' ||
        !records.some((record) =>
          record?.applicationResources?.some(
            (resource) =>
              resource.bucketName === requested &&
              resource.jurisdiction === jurisdiction &&
              resource.state === 'create-authorized',
          ),
        )
      )
        throw new Error('unexpected fixture bucket');
      const key = `${jurisdiction}:${requested}`;
      if (buckets.has(key))
        return new Response('bucket exists', { status: 409 });
      const descriptor = {
        name: requested,
        jurisdiction,
        creation_date: new Date().toISOString(),
      };
      buckets.set(key, descriptor);
      return single(descriptor);
    }
    if (!name && request.method === 'GET') {
      const selected = [...buckets.values()]
        .filter(
          (bucket) =>
            bucket.jurisdiction === jurisdiction &&
            bucket.name.includes(url.searchParams.get('name_contains') ?? '') &&
            bucket.name > (url.searchParams.get('start_after') ?? ''),
        )
        .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
      return single({ buckets: selected });
    }
    const key = `${jurisdiction}:${name}`;
    if (
      !match[2] &&
      request.method === 'GET' &&
      world.consumeFailure('getApplicationR2Bucket')
    )
      return Response.json(
        {
          success: false,
          errors: [{ code: 10000, message: 'fixture bucket read denied' }],
        },
        { status: 403 },
      );
    const descriptor = buckets.get(key);
    if (!descriptor) return Response.json({ errors: [] }, { status: 404 });
    const prefix = `${key}/`;
    if (match[2] && request.method === 'GET') {
      expect(url.searchParams.get('per_page')).toBe('1');
      const objects = await applicationBytes.list({
        prefix,
        limit: 1,
        ...(url.searchParams.get('cursor')
          ? { cursor: url.searchParams.get('cursor') as string }
          : {}),
      });
      return Response.json({
        success: true,
        errors: [],
        messages: [],
        result: objects.objects.map((object) => ({
          key: object.key.slice(prefix.length),
        })),
        result_info: objects.truncated ? { cursor: objects.cursor } : {},
      });
    }
    if (!match[2] && request.method === 'GET') return single(descriptor);
    if (!match[2] && request.method === 'DELETE') {
      const failure = world.consumeFailure('deleteApplicationR2Bucket');
      const failed = () =>
        Response.json(
          {
            success: false,
            errors: [{ code: 1, message: 'fixture bucket deletion failed' }],
          },
          { status: 400 },
        );
      if (failure && !failure.dispatched) return failed();
      const objects = await applicationBytes.list({ prefix, limit: 1 });
      if (objects.objects.length)
        return new Response('bucket nonempty', { status: 409 });
      buckets.delete(key);
      await world.applyAfter('deleteApplicationR2Bucket');
      if (failure) return failed();
      return single({});
    }
    throw new Error('unexpected fixture R2 method');
  });

  let reload: () => Promise<void>;
  async function refreshBindings() {
    const env = await server
      .getWorker<{
        FLEET_DB: D1Database;
        EXPORTS: R2Bucket;
        APPLICATION_BYTES: R2Bucket;
      }>()
      .getEnv();
    db = env.FLEET_DB;
    fleetStore = new D1FleetStateStore(new D1FleetStateDatabase(db), {
      accountId: binding.accountId,
    });
    exportBytes = env.EXPORTS;
    applicationBytes = env.APPLICATION_BYTES;
  }
  async function close() {
    const closed = await Promise.allSettled([
      (async () => server?.close())(),
      (async () => {
        if (bridge) {
          bridge.closeAllConnections();
          await new Promise<void>((resolve, reject) =>
            bridge.close((error) => (error ? reject(error) : resolve())),
          );
        }
      })(),
    ]);
    const failures = closed.flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : [],
    );
    try {
      if (directory) await rm(directory, { recursive: true, force: true });
    } catch (error) {
      failures.push(error);
    }
    if (failures.length)
      throw new AggregateError(
        failures,
        'direct lifecycle fixture teardown failed',
      );
  }

  try {
    directory = await mkdtemp(join(tmpdir(), 'direct-lifecycle-'));
    bridge = createServer(async (incoming, outgoing) => {
      try {
        const original = incoming.headers['x-direct-fixture-url'];
        if (typeof original !== 'string' || incoming.url !== '/')
          throw new Error('invalid fixture bridge request');
        const headers = new Headers();
        for (const [name, values] of Object.entries(incoming.headers)) {
          if (
            name === 'x-direct-fixture-url' ||
            name === 'host' ||
            values === undefined
          )
            continue;
          for (const value of Array.isArray(values) ? values : [values])
            headers.append(name, value);
        }
        const method = incoming.method ?? 'GET';
        const init = {
          method,
          headers,
          body:
            method === 'GET' || method === 'HEAD'
              ? undefined
              : Readable.toWeb(incoming),
          duplex: 'half' as const,
        };
        const request = new Request(original, init as RequestInit);
        const body = request.body
          ? request.headers.get('content-type')?.includes('multipart/form-data')
            ? await request.formData()
            : await request.text()
          : undefined;
        const response = await projection.fetch(original, {
          method,
          headers,
          body,
          redirect: 'manual',
        });
        outgoing.writeHead(
          response.status,
          Object.fromEntries(response.headers),
        );
        outgoing.end(Buffer.from(await response.arrayBuffer()));
      } catch (error) {
        bridgeErrors.push(error);
        outgoing.statusCode = 500;
        outgoing.end('fixture handler failed');
      }
    });
    await new Promise<void>((resolve) =>
      bridge.listen(0, '127.0.0.1', resolve),
    );
    const address = bridge.address();
    if (!address || typeof address === 'string')
      throw new Error('missing fixture listener');
    const main = join(directory, 'worker.ts');
    const workerSource = fileURLToPath(
      new URL('../../scripts/direct-reference-worker.ts', import.meta.url),
    );
    await writeFile(
      main,
      `import {createDirectReferenceWorker} from ${JSON.stringify(workerSource)};
const worker=createDirectReferenceWorker(${JSON.stringify(manifest)},{fetch:async(input,init)=>{const request=new Request(input,init);if(request.url==='data:,')return fetch(request);const headers=new Headers(request.headers);headers.set('X-Direct-Fixture-Url',request.url);return fetch('http://127.0.0.1:${address.port}/',{method:request.method,headers,body:request.body,signal:request.signal,redirect:'manual'});}});
let instance; export default {async fetch(request,env){instance??=crypto.randomUUID();const response=await worker.fetch(request,{...env,CLOUDFLARE_API_TOKEN:'inert-provider-token',FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET:'inert-invoke',DIRECT_RUN_BINDING:${JSON.stringify(JSON.stringify(binding))},DIRECT_DEPLOYMENT_SECRETS:${JSON.stringify(JSON.stringify(secrets))}});response.headers.set('X-Fixture-Instance',instance);return response;}};`,
    );
    const options = {
      root: directory,
      workers: [
        {
          config: {
            name: 'direct-lifecycle-harness',
            main,
            compatibility_date: '2026-08-06',
            compatibility_flags: ['nodejs_compat'],
            d1_databases: [
              {
                binding: 'FLEET_DB',
                database_name: 'lifecycle-fleet',
                database_id: binding.fleetDatabaseId,
              },
              {
                binding: 'QUOTA_DB',
                database_name: 'lifecycle-quota',
                database_id: binding.quotaDatabaseId,
              },
            ],
            r2_buckets: [
              { binding: 'EXPORTS', bucket_name: binding.exportBucketName },
              {
                binding: 'APPLICATION_BYTES',
                bucket_name: 'fixture-application-bytes',
              },
            ],
          },
        },
      ],
    };
    server = createTestHarness(options);
    let revision = 0;
    reload = async () => {
      await server.update({
        ...options,
        workers: options.workers.map((worker) => ({
          ...worker,
          config: {
            ...worker.config,
            vars: { TEST_RELOAD: String(++revision) },
          },
        })),
      });
      await refreshBindings();
    };
    await server.listen();
    await refreshBindings();
  } catch (error) {
    try {
      await close();
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        'direct fixture startup and cleanup failed',
      );
    }
    throw error;
  }
  async function call(action: DirectReferenceAction) {
    const response = await server
      .getWorker()
      .fetch(`https://reference.test${DIRECT_REFERENCE_PATH}`, {
        method: 'POST',
        headers: { authorization: 'Bearer inert-invoke' },
        body: JSON.stringify({
          contractVersion: 1,
          configSha256: manifest.configSha256,
          action,
        }),
      });
    return {
      response,
      value: (await response.json()) as {
        ok: boolean;
        result?: unknown;
        error?: unknown;
      },
    };
  }

  async function success<T>(action: DirectReferenceAction): Promise<T> {
    const { response, value } = await call(action);
    expect(bridgeErrors).toEqual([]);
    expect({ status: response.status, value }).toMatchObject({
      status: 200,
      value: { ok: true },
    });
    return value.result as T;
  }

  function journal() {
    return new DirectReferenceJournal(
      db,
      manifest.resourcePrefix,
      JSON.stringify({
        configSha256: manifest.configSha256,
        binding,
        quotaScope: manifest.resourcePrefix,
        tenantSecretsSha256: createHash('sha256')
          .update(JSON.stringify(secrets))
          .digest('hex'),
      }),
    );
  }

  return {
    manifest,
    binding,
    secrets,
    specs,
    world,
    projection,
    get db() {
      return db;
    },
    get fleetStore() {
      return fleetStore;
    },
    get applicationBytes() {
      return applicationBytes;
    },
    get exportBytes() {
      return exportBytes;
    },
    buckets,
    bridgeErrors,
    sqlFailures,
    call,
    success,
    journal,
    reload,
    close,
  };
}
export type DirectReferenceHarness = Awaited<
  ReturnType<typeof createDirectReferenceHarness>
>;
