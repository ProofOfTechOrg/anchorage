// SPDX-License-Identifier: Apache-2.0

import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  Crypto,
  D1Database,
  R2Bucket,
  FixedLengthStream as WorkerFixedLengthStream,
} from '@cloudflare/workers-types';
import {
  type CloudflareDeploymentSpec,
  createCloudflareControlPlane,
  D1CloudflareApiRateCoordinator,
  D1FleetStateDatabase,
  ProvisioningError,
  R2DatabaseExportStore,
} from '@proofoftech/fleet-control/cloudflare-control-plane';

declare const crypto: Crypto;
declare const FixedLengthStream: typeof WorkerFixedLengthStream;

interface Env {
  readonly FLEET_DB: D1Database;
  readonly QUOTA_DB: D1Database;
  readonly FIXTURE_DB: D1Database;
  readonly TENANT_DB: D1Database;
  readonly EXPORTS: R2Bucket;
}

const DATABASE_ID = '11111111-1111-4111-8111-111111111111';
const RECEIPT_ID = '22222222-2222-4222-8222-222222222222';
const MIGRATION = 'CREATE TABLE packed_injected_failure (id TEXT PRIMARY KEY)';
const SECRETS = {
  deploymentIdentity: 'packed-identity-secret-0000000000000001',
  maintenanceAdmin: 'packed-maintenance-secret-000000000001',
  application: {},
};

function spec(tenantTag = 'packedlife'): CloudflareDeploymentSpec {
  return {
    tenantTag,
    environment: 'production',
    scriptName: tenantTag,
    databaseName: tenantTag,
    compatibilityDate: '2026-08-06',
    compatibilityFlags: [],
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'platform',
    schemaVersion: 1,
    migrations: [{ version: 1, sql: MIGRATION }],
    durableObjectMigrations: [{ tag: 'v1', newSqliteClasses: ['Maintenance'] }],
    durableObjectBindings: [{ name: 'MAINTENANCE', className: 'Maintenance' }],
    maintenanceBaseUrl: `https://control-${tenantTag}.example.test`,
    routeHostname: `${tenantTag}.example.test`,
    application: { vars: [], secrets: [], r2Buckets: [] },
  };
}

function errorInfo(error: unknown): unknown {
  if (!(error instanceof Error)) return { message: String(error) };
  return {
    name: error.name,
    message: error.message,
    ...(error.cause === undefined ? {} : { cause: errorInfo(error.cause) }),
    ...(error instanceof AggregateError
      ? { errors: error.errors.map(errorInfo) }
      : {}),
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

async function within<T>(operation: Promise<T>, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), 10_000);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

function enterGate(
  entered: Promise<void>,
  operation: Promise<unknown>,
): Promise<void> {
  return within(
    Promise.race([
      entered,
      operation.then((cause) => {
        throw new Error('operation completed before provider gate', { cause });
      }),
    ]),
    'provider gate timed out',
  );
}

async function waitFor(check: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!(await check())) {
    if (Date.now() >= deadline)
      throw new Error('packed probe barrier timed out');
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

function single(result: unknown): Response {
  return Response.json({ success: true, errors: [], messages: [], result });
}

function page(result: readonly unknown[]): Response {
  return Response.json({
    success: true,
    errors: [],
    messages: [],
    result,
    result_info: {
      page: 1,
      per_page: 100,
      count: result.length,
      total_count: result.length,
      total_pages: 1,
    },
  });
}

function failure(message: string, status = 400): Response {
  return Response.json(
    { success: false, errors: [{ code: 1, message }], result: null },
    { status },
  );
}

function exportOptions(env: Env) {
  return {
    bucket: env.EXPORTS,
    bucketName: 'packed-exports',
    keyPrefix: 'proof/',
    streams: { DigestStream: crypto.DigestStream, FixedLengthStream },
    randomUUID: () => crypto.randomUUID(),
  };
}

function plane(env: Env, providerFetch: typeof fetch) {
  return createCloudflareControlPlane({
    accountId: 'account',
    apiToken: 'packed-inert-provider-token',
    fleetDatabase: env.FLEET_DB,
    quotaDatabase: env.QUOTA_DB,
    quotaScope: 'packed-factory-provider',
    databaseExports: exportOptions(env),
    fetch: providerFetch,
    maintenanceFetch: async () => {
      throw new Error('packed failure scenario cannot invoke maintenance');
    },
    concurrency: 1,
    requestTimeoutMs: 10_000,
    leaseTtlMs: 120_000,
    leaseRenewalIntervalMs: 60_000,
  });
}

interface ProviderRequest {
  readonly method: string;
  readonly url: URL;
  readonly body: Record<string, unknown>;
}

async function provider(env: Env, invocationId: string) {
  await env.FIXTURE_DB.batch([
    env.FIXTURE_DB.prepare(
      'CREATE TABLE IF NOT EXISTS packed_provider_databases (id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL)',
    ),
    env.FIXTURE_DB.prepare(
      'CREATE TABLE IF NOT EXISTS packed_provider_requests (ordinal INTEGER PRIMARY KEY AUTOINCREMENT, invocation_id TEXT NOT NULL, method TEXT NOT NULL, url TEXT NOT NULL)',
    ),
  ]);
  let requests = 0;
  async function respond({
    method,
    url,
    body,
  }: ProviderRequest): Promise<Response> {
    if (url.origin !== 'https://api.cloudflare.com') {
      throw new Error(`unexpected inert-provider origin ${url.origin}`);
    }
    requests += 1;
    await env.FIXTURE_DB.prepare(
      'INSERT INTO packed_provider_requests (invocation_id, method, url) VALUES (?, ?, ?)',
    )
      .bind(invocationId, method, url.href)
      .run();
    const path = url.pathname;
    if (
      method === 'GET' &&
      (path === '/client/v4/accounts/account/tokens/verify' ||
        path === '/client/v4/user/tokens/verify')
    ) {
      return single({ id: 'token-id', status: 'active' });
    }
    if (
      method === 'GET' &&
      path === '/client/v4/accounts/account/tokens/token-id'
    ) {
      return single({
        id: 'token-id',
        status: 'active',
        policies: [
          {
            id: 'packed-zone-authority',
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
    if (method === 'GET' && path === '/client/v4/zones') return page([]);
    const account = '/client/v4/accounts/account';
    if (path === `${account}/d1/database`) {
      if (method === 'GET') {
        if (url.searchParams.has('page')) return page([]);
        const name = url.searchParams.get('name');
        const rows = await env.FIXTURE_DB.prepare(
          'SELECT id AS uuid, name FROM packed_provider_databases WHERE (? IS NULL OR name = ?)',
        )
          .bind(name, name)
          .all();
        return page(rows.results);
      }
      if (method === 'POST' && body.name === 'packedlife') {
        await env.FIXTURE_DB.prepare(
          'INSERT INTO packed_provider_databases (id, name) VALUES (?, ?)',
        )
          .bind(DATABASE_ID, body.name)
          .run();
        return single({ uuid: DATABASE_ID, name: body.name });
      }
    }
    if (path === `${account}/d1/database/${DATABASE_ID}`) {
      const row = await env.FIXTURE_DB.prepare(
        'SELECT id AS uuid, name FROM packed_provider_databases WHERE id = ?',
      )
        .bind(DATABASE_ID)
        .first();
      if (!row) return failure('logical database absent', 404);
      if (method === 'GET') return single(row);
      if (method === 'DELETE') {
        await env.FIXTURE_DB.prepare(
          'DELETE FROM packed_provider_databases WHERE id = ?',
        )
          .bind(DATABASE_ID)
          .run();
        return single({});
      }
    }
    if (
      method === 'POST' &&
      path === `${account}/d1/database/${DATABASE_ID}/query`
    ) {
      const statements = Array.isArray(body.batch) ? body.batch : [body];
      if (statements.some((statement) => statement.sql === MIGRATION)) {
        return failure('packed migration failure after identity seed');
      }
      const prepared = statements.map((statement) => {
        if (typeof statement.sql !== 'string')
          throw new Error('missing provider SQL');
        const values = Array.isArray(statement.params) ? statement.params : [];
        return env.TENANT_DB.prepare(statement.sql).bind(...values);
      });
      return page(await env.TENANT_DB.batch(prepared));
    }
    if (
      method === 'GET' &&
      [
        `${account}/workers/scripts`,
        `${account}/workers/domains`,
        `${account}/workers/durable_objects/namespaces`,
        `${account}/workers/dispatch/namespaces`,
      ].includes(path)
    )
      return page([]);
    if (
      method === 'GET' &&
      /^\/client\/v4\/accounts\/account\/workers\/scripts\/packedlife(?:\/(?:deployments|versions|settings|subdomain|secrets))?$/.test(
        path,
      )
    ) {
      return failure('logical Worker absent', 404);
    }
    throw new Error(`unexpected inert-provider request ${method} ${url.href}`);
  }
  return { respond, count: () => requests };
}

function transport(
  respond: (request: ProviderRequest) => Promise<Response>,
): typeof fetch {
  return async (input, init) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
    );
    const method = (
      init?.method ?? (input instanceof Request ? input.method : 'GET')
    ).toUpperCase();
    const raw = init?.body;
    const body = typeof raw === 'string' ? JSON.parse(raw) : {};
    return respond({ method, url, body });
  };
}

async function provisionFailure(
  control: ReturnType<typeof plane>,
  tenant = 'packedlife',
) {
  try {
    await control.provisionDeployment({
      spec: spec(tenant),
      secrets: SECRETS,
      initialExecutionFenceState: 'open',
    });
  } catch (error) {
    return error;
  }
  throw new Error('injected provisioning failure unexpectedly succeeded');
}

async function leaseWinner(env: Env, invocationId: string) {
  const inert = await provider(env, invocationId);
  const held = deferred<void>();
  const release = deferred<void>();
  let firstRead = true;
  const selectedFetch = transport(async (request) => {
    if (firstRead) {
      firstRead = false;
      held.resolve();
      await release.promise;
    }
    return inert.respond(request);
  });
  const first = plane(env, selectedFetch);
  const second = plane(env, selectedFetch);
  const winner = provisionFailure(first);
  let contender: unknown;
  let beforeRelease: number;
  try {
    await enterGate(held.promise, winner);
    contender = await within(
      provisionFailure(second),
      'contender did not release',
    );
    beforeRelease = inert.count();
  } finally {
    release.resolve();
  }
  const result = await within(winner, 'winner did not finish');
  if (
    !(result instanceof ProvisioningError) ||
    result.cleanup?.status !== 'pending'
  ) {
    throw new Error('provisioning did not admit bounded cleanup', {
      cause: result,
    });
  }
  return {
    contender: errorInfo(contender),
    beforeRelease,
    cleanup: result.cleanup,
    record: await first.getDeployment('packedlife', 'production'),
    requests: inert.count(),
  };
}

async function queuedAuthority(env: Env, stale: boolean) {
  const contexts = new AsyncLocalStorage<string>();
  const b = `packed${stale ? 'stale' : 'live'}b`;
  const a = `packed${stale ? 'stale' : 'live'}a`;
  const bHeld = deferred<void>();
  const releaseB = deferred<void>();
  const aHeld = deferred<void>();
  const releaseA = deferred<void>();
  const seen: Array<{
    method: string;
    name: string | null;
    context: string | undefined;
  }> = [];
  let heldB = false;
  const control = plane(
    env,
    transport(async ({ method, url, body }) => {
      if (
        url.origin !== 'https://api.cloudflare.com' ||
        url.pathname !== '/client/v4/accounts/account/d1/database'
      ) {
        throw new Error(`unexpected queue-probe URL ${url.href}`);
      }
      const name =
        method === 'GET' ? url.searchParams.get('name') : String(body.name);
      seen.push({ method, name, context: contexts.getStore() });
      if (method === 'GET' && name === b) {
        if (!heldB) {
          heldB = true;
          bHeld.resolve();
          await releaseB.promise;
        }
        return page([]);
      }
      if (method === 'GET' && name === a) {
        aHeld.resolve();
        await releaseA.promise;
        return failure('packed A held-read failure', 403);
      }
      if (method === 'POST' && name === b)
        return failure('packed B create reached transport');
      throw new Error(`unexpected queue-probe request ${method} ${name}`);
    }),
  );
  await control.getDeployment(b, 'production');
  await env.FLEET_DB.batch([
    env.FLEET_DB.prepare(
      'CREATE TABLE IF NOT EXISTS packed_lease_renewals (tenant_tag TEXT NOT NULL)',
    ),
    env.FLEET_DB.prepare(
      'CREATE TRIGGER IF NOT EXISTS packed_observe_lease_renewal AFTER UPDATE OF expires_at ON anchorage_fleet_leases BEGIN INSERT INTO packed_lease_renewals (tenant_tag) VALUES (NEW.tenant_tag); END',
    ),
  ]);
  const second = contexts.run('B', () => provisionFailure(control, b));
  try {
    await enterGate(bHeld.promise, second);
    const first = contexts.run('A', () => provisionFailure(control, a));
    releaseB.resolve();
    await enterGate(aHeld.promise, first);
    await waitFor(async () => {
      const row = await env.FLEET_DB.prepare(
        'SELECT COUNT(*) AS count FROM packed_lease_renewals WHERE tenant_tag = ?',
      )
        .bind(b)
        .first<{ count: number }>();
      return Number(row?.count) >= 2;
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    const queued = await control.getDeployment(b, 'production');
    const beforeRelease = seen.filter(
      (request) => request.method === 'POST' && request.name === b,
    ).length;
    if (stale) {
      await env.FLEET_DB.prepare(
        "UPDATE anchorage_fleet_leases SET owner_token = 'packed-takeover' WHERE tenant_tag = ? AND environment = 'production'",
      )
        .bind(b)
        .run();
    }
    releaseA.resolve();
    const [aError, bError] = await Promise.all([first, second]);
    return {
      queuedPhase: queued?.phase,
      beforeRelease,
      seen,
      aError: errorInfo(aError),
      bError: errorInfo(bError),
      record: await control.getDeployment(b, 'production'),
    };
  } finally {
    releaseB.resolve();
    releaseA.resolve();
  }
}

async function quota(env: Env) {
  const quotaScope = 'packed-independent-quota';
  let observedBatches = 0;
  const observed = new Proxy(env.QUOTA_DB, {
    get(database, property) {
      if (property === 'batch') {
        return async <T>(statements: Parameters<D1Database['batch']>[0]) => {
          const result = await database.batch<T>(statements);
          observedBatches += 1;
          return result;
        };
      }
      const value = Reflect.get(database, property, database);
      return typeof value === 'function' ? value.bind(database) : value;
    },
  });
  const first = new D1CloudflareApiRateCoordinator(env.QUOTA_DB, {
    quotaScope,
  });
  const second = new D1CloudflareApiRateCoordinator(observed, { quotaScope });
  await Promise.all([first.acquire(), second.acquire()]);
  await env.QUOTA_DB.prepare(
    'DELETE FROM anchorage_cloudflare_api_rate_reservations WHERE quota_scope = ?',
  )
    .bind(quotaScope)
    .run();
  await env.QUOTA_DB.prepare(
    "WITH RECURSIVE sequence(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 1099) INSERT INTO anchorage_cloudflare_api_rate_reservations (quota_scope, reservation_id, reserved_at) SELECT ?, 'seed-' || value, CAST(unixepoch('subsec') * 1000 AS INTEGER) FROM sequence",
  )
    .bind(quotaScope)
    .run();
  await first.acquire();
  const baseline = observedBatches;
  const abort = new AbortController();
  let acquired = false;
  const blocked = second.acquire(abort.signal).then(() => {
    acquired = true;
    return null;
  }, errorInfo);
  await waitFor(async () => observedBatches > baseline);
  abort.abort();
  const error = await blocked;
  const count = await env.QUOTA_DB.prepare(
    'SELECT COUNT(*) AS count FROM anchorage_cloudflare_api_rate_reservations WHERE quota_scope = ?',
  )
    .bind(quotaScope)
    .first<{ count: number }>();
  await env.QUOTA_DB.prepare(
    "DELETE FROM anchorage_cloudflare_api_rate_reservations WHERE quota_scope = ? AND reservation_id = 'seed-1'",
  )
    .bind(quotaScope)
    .run();
  await new D1CloudflareApiRateCoordinator(env.QUOTA_DB, {
    quotaScope,
  }).acquire();
  const after = await env.QUOTA_DB.prepare(
    'SELECT COUNT(*) AS count FROM anchorage_cloudflare_api_rate_reservations WHERE quota_scope = ?',
  )
    .bind(quotaScope)
    .first<{ count: number }>();
  return {
    acquired,
    error,
    count: Number(count?.count),
    countAfterFreshInstance: Number(after?.count),
    completedBlockedBatches: observedBatches - baseline,
  };
}

function bytes(seed = 17) {
  return Uint8Array.from(
    { length: 1_048_576 },
    (_, index) => (index * 31 + seed) % 256,
  );
}

function body(value: Uint8Array) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(value);
      controller.close();
    },
  });
}

function hex(digest: ArrayBuffer) {
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}

async function integrity(value: Uint8Array) {
  return {
    size: value.byteLength,
    sha256: hex(await crypto.subtle.digest('SHA-256', value)),
  };
}

async function r2(env: Env, action: string) {
  const store = new R2DatabaseExportStore(exportOptions(env));
  const source = bytes(action === 'receipt-mismatch' ? 29 : 17);
  const expected = await integrity(source);
  const identity = {
    version: 1 as const,
    authority: store.receiptAuthority,
    databaseId: DATABASE_ID,
    operationId: RECEIPT_ID,
  };
  let result: Awaited<ReturnType<R2DatabaseExportStore['write']>> | undefined;
  let error: unknown;
  try {
    result =
      action === 'r2-write'
        ? await store.write({
            databaseId: DATABASE_ID,
            fileName: 'export.sql',
            body: body(source),
            contentLength: source.length,
          })
        : await store.writeReceipt({
            identity,
            body: body(source),
            contentLength: source.length,
            expectedIntegrity: Promise.resolve(expected),
          });
  } catch (reason) {
    error = errorInfo(reason);
  }
  const key =
    action === 'r2-write' && result
      ? result.location.slice('r2://packed-exports/'.length)
      : `proof/receipts/v1/${DATABASE_ID}/${RECEIPT_ID}.sql`;
  const object = await env.EXPORTS.get(key);
  if (!object) throw new Error('packed R2 object absent', { cause: error });
  const stored = await object.bytes();
  const original = bytes();
  const readback = await integrity(stored);
  const listing = await env.EXPORTS.list({
    prefix:
      action === 'r2-write'
        ? `proof/${DATABASE_ID}/`
        : `proof/receipts/v1/${DATABASE_ID}/`,
  });
  return {
    result,
    error,
    expected,
    readback,
    size: object.size,
    bytesEqual:
      stored.length === original.length &&
      stored.every((byte, index) => byte === original[index]),
    objectCount: listing.objects.length,
    metadata: object.customMetadata,
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const input = (await request.json()) as { action: string; token?: unknown };
    const invocationId = crypto.randomUUID();
    try {
      let result: unknown;
      if (input.action === 'quota') result = await quota(env);
      else if (input.action === 'queue-live' || input.action === 'queue-stale')
        result = await queuedAuthority(env, input.action === 'queue-stale');
      else if (input.action === 'lease-winner')
        result = await leaseWinner(env, invocationId);
      else if (
        input.action === 'r2-write' ||
        input.action === 'receipt' ||
        input.action === 'receipt-mismatch'
      )
        result = await r2(env, input.action);
      else {
        const inert = await provider(env, invocationId);
        const control = plane(env, transport(inert.respond));
        if (input.action === 'schema') {
          const db = new D1FleetStateDatabase(env.FLEET_DB);
          await db.execute(
            'CREATE TABLE packed_adapter (id INTEGER PRIMARY KEY, value TEXT NOT NULL)',
          );
          const batch = await db.batch([
            {
              sql: 'INSERT INTO packed_adapter (value) VALUES (?) RETURNING value',
              bindings: ['native-d1'],
            },
          ]);
          const records = await Promise.all([
            control.getDeployment('packedlife', 'production'),
            plane(env, transport(inert.respond)).getDeployment(
              'packedlife',
              'production',
            ),
          ]);
          result = {
            batch,
            rows: await db.query('SELECT value FROM packed_adapter'),
            records,
            requests: inert.count(),
          };
        } else if (input.action === 'cleanup') {
          let outcome:
            | Awaited<ReturnType<typeof control.advanceCleanupDeployment>>
            | undefined;
          let error: unknown;
          try {
            outcome = await control.advanceCleanupDeployment({
              spec: spec(),
              action: { kind: 'continue', token: input.token },
              maxProviderRequests: 9,
            });
          } catch (reason) {
            error = errorInfo(reason);
          }
          result = {
            outcome,
            error,
            record: await control.getDeployment('packedlife', 'production'),
            requests: inert.count(),
            databases: (
              await env.FIXTURE_DB.prepare(
                'SELECT id FROM packed_provider_databases',
              ).all()
            ).results,
          };
        } else throw new Error(`unexpected packed action ${input.action}`);
      }
      return Response.json({ invocationId, result });
    } catch (error) {
      return Response.json(
        { invocationId, error: errorInfo(error) },
        { status: 500 },
      );
    }
  },
};
