// SPDX-License-Identifier: Apache-2.0

import type {
  Crypto,
  D1Database,
  D1PreparedStatement,
  D1Result,
  ExportedHandler,
  R2Bucket,
  FixedLengthStream as WorkerFixedLengthStream,
  Response as WorkerResponse,
} from '@cloudflare/workers-types';
import {
  type CloudflareDeploymentSpec,
  createCloudflareControlPlane,
  type FleetAuditAdvanceAction,
  type FleetOperationToken,
  type FleetRecord,
} from '@proofoftech/fleet-control/cloudflare-control-plane';

declare const crypto: Crypto;
declare const FixedLengthStream: typeof WorkerFixedLengthStream;
declare const Response: typeof WorkerResponse;

interface Env {
  FLEET_DB: D1Database;
  QUOTA_DB: D1Database;
  EXPORTS: R2Bucket;
}

const PROFILES = {
  representative: { count: 32, late: false, bytes: false, id: 1 },
  'page-boundary': { count: 1001, late: true, bytes: false, id: 2 },
  'record-ceiling': { count: 10000, late: true, bytes: false, id: 3 },
  'intake-byte-boundary': { count: 200, late: false, bytes: true, id: 4 },
} as const;
type Profile = keyof typeof PROFILES;
const NOW = Date.parse('2026-09-09T00:00:00.000Z');
const FINDING = {
  tenantTag: 'unknown',
  environment: 'unknown',
  kind: 'stale-route',
  detail: 'packed workload observation',
};
const OPTIONS = {
  databaseNamePrefix: 'workload-',
  scriptNamePrefix: 'workload-',
  includeDispatchNamespace: false,
  includeR2Buckets: false,
};
const ROW_KINDS = [
  'registration',
  'deployment',
  'finding',
  'database-id',
  'namespace-id',
  'r2-bucket',
  'route',
  'dispatch-script',
  'meta',
] as const;
const encoder = new TextEncoder();

function operationId(profile: Profile, variant = 0): string {
  return `11111111-1111-4111-8111-${String(PROFILES[profile].id * 10 + variant).padStart(12, '0')}`;
}

function record(profile: Profile, index: number): FleetRecord {
  const tenantTag = `workload${index}`;
  return {
    tenantTag,
    environment: 'production',
    backend: 'plain-worker',
    scriptName: `workload-${index}`,
    databaseName: `workload-db-${index}`,
    databaseId: `database-${index}`,
    schemaVersion: 1,
    artifactVersion: 'v1',
    desiredSpecDigest: 'a'.repeat(64),
    durableObjectBindings: [
      {
        name: 'RUNNER',
        className: 'Runner',
        namespaceId: `namespace-${index}`,
      },
    ],
    routeHostname: `${tenantTag}.example.test`,
    phase:
      PROFILES[profile].late && index < PROFILES[profile].count - 1
        ? 'ready'
        : 'worker-deployed',
    updatedAt: new Date(NOW).toISOString(),
  };
}

function specFor(value: FleetRecord): CloudflareDeploymentSpec {
  return {
    tenantTag: value.tenantTag,
    environment: value.environment,
    scriptName: value.scriptName,
    databaseName: value.databaseName,
    authoredBy: 'platform',
    compatibilityDate: '2026-08-06',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    schemaVersion: 1,
    migrations: [],
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: 'https://workload.example.test',
    routeHostname: value.routeHostname,
  };
}

function intake(
  profile: Profile,
  extraByte: boolean,
  count: number = PROFILES[profile].count,
) {
  const records = Array.from({ length: count }, (_, index) =>
    record(profile, index),
  );
  if (PROFILES[profile].bytes) {
    const target = 16 * 1024 * 1024 + Number(extraByte);
    const share = Math.floor(target / records.length);
    for (const [index, item] of records.entries()) {
      const desired = share + Number(index < target % records.length);
      const padding: Record<string, string> = {};
      Object.assign(item, { padding });
      for (let key = 0; ; key += 1) {
        const remaining =
          desired - encoder.encode(JSON.stringify(item)).byteLength;
        if (remaining === 0) break;
        const name = `p${key}`;
        const overhead = name.length + 5 + Number(key > 0);
        if (remaining < overhead)
          throw new Error('intake padding cannot reach target');
        let size = Math.min(4000, remaining - overhead);
        const tail = remaining - overhead - size;
        if (tail > 0 && tail < `p${key + 1}`.length + 6) size -= 16;
        padding[name] = 'x'.repeat(size);
      }
    }
  }
  const sizes = records.map(
    (item) => encoder.encode(JSON.stringify(item)).byteLength,
  );
  return {
    records,
    bytes: sizes.reduce((sum, size) => sum + size, 0),
    maximumItemBytes: Math.max(...sizes),
  };
}

function observer(binding: D1Database) {
  const metrics = {
    calls: 0,
    statements: 0,
    returnedRows: 0,
    inventoryRows: 0,
    inventoryFacts: 0,
    recordPages: 0,
    recordRows: 0,
    factPages: 0,
    factRows: 0,
    durationMs: 0,
    rowsRead: 0,
    rowsWritten: 0,
  };
  const native = new WeakMap<
    D1PreparedStatement,
    { statement: D1PreparedStatement; sql: string; bindings: unknown[] }
  >();
  function observe(sql: string, bindings: unknown[], result: D1Result) {
    const count = result.results?.length ?? 0;
    metrics.returnedRows += count;
    metrics.durationMs += result.meta.duration ?? 0;
    metrics.rowsRead += result.meta.rows_read ?? 0;
    metrics.rowsWritten += result.meta.rows_written ?? 0;
    if (
      sql.includes(
        'SELECT kind, ordinal, payload FROM anchorage_fleet_inventory_rows',
      )
    )
      metrics.inventoryRows += count;
    if (
      sql.includes(
        'SELECT deployment_ordinal, fact_kind, fact_ordinal, payload',
      )
    )
      metrics.inventoryFacts += count;
    if (
      sql.includes(
        'SELECT row_kind, ordinal, payload FROM anchorage_fleet_operation_rows',
      )
    ) {
      if (bindings[2] === 'record') {
        metrics.recordPages += 1;
        metrics.recordRows += count;
      }
      if (bindings[2] === 'fact') {
        metrics.factPages += 1;
        metrics.factRows += count;
      }
    }
  }
  function wrap(
    statement: D1PreparedStatement,
    sql: string,
    bindings: unknown[] = [],
  ): D1PreparedStatement {
    const proxy = new Proxy(statement, {
      get(target, key) {
        if (key === 'bind')
          return (...values: unknown[]) =>
            wrap(target.bind(...values), sql, values);
        if (key === 'all' || key === 'run')
          return async () => {
            metrics.calls += 1;
            metrics.statements += 1;
            const result = await target[key]();
            observe(sql, bindings, result);
            return result;
          };
        const value = Reflect.get(target, key, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    native.set(proxy, { statement, sql, bindings });
    return proxy;
  }
  const database = new Proxy(binding, {
    get(target, key) {
      if (key === 'prepare')
        return (sql: string) => wrap(target.prepare(sql), sql);
      if (key === 'batch')
        return async (statements: D1PreparedStatement[]) => {
          const entries = statements.map((statement) => {
            const entry = native.get(statement);
            if (!entry) throw new Error('unrecorded D1 statement');
            return entry;
          });
          metrics.calls += 1;
          metrics.statements += entries.length;
          const result = await target.batch(
            entries.map((entry) => entry.statement),
          );
          result.forEach((value, index) => {
            observe(entries[index].sql, entries[index].bindings, value);
          });
          return result;
        };
      const value = Reflect.get(target, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  return { database, metrics };
}

function controlPlane(env: Env, profile: Profile) {
  const observed = observer(env.FLEET_DB);
  let providerRequests = 0;
  const refuse = async () => {
    providerRequests += 1;
    throw new Error('materialization workload must not call a provider');
  };
  const control = createCloudflareControlPlane({
    accountId: `workload-${profile}`,
    apiToken: 'packed-inert-token',
    fleetDatabase: observed.database,
    quotaDatabase: env.QUOTA_DB,
    quotaScope: `workload-${profile}`,
    fetch: refuse,
    maintenanceFetch: refuse,
    databaseExports: {
      bucket: env.EXPORTS,
      bucketName: 'workload-exports',
      streams: { DigestStream: crypto.DigestStream, FixedLengthStream },
      randomUUID: () => crypto.randomUUID(),
    },
    leaseTtlMs: 120000,
    leaseRenewalIntervalMs: 60000,
  });
  return {
    control,
    metrics: observed.metrics,
    providerRequests: () => providerRequests,
  };
}

async function batch(db: D1Database, statements: D1PreparedStatement[]) {
  for (let offset = 0; offset < statements.length; offset += 100)
    await db.batch(statements.slice(offset, offset + 100));
}

async function digest(entries: Iterable<string>): Promise<string> {
  const stream = new crypto.DigestStream('SHA-256');
  const writer = stream.getWriter();
  for (const entry of entries) await writer.write(encoder.encode(entry));
  await writer.close();
  return Array.from(new Uint8Array(await stream.digest), (value) =>
    value.toString(16).padStart(2, '0'),
  ).join('');
}

async function generationRecord(
  profile: Profile,
  state: 'staging' | 'finalized',
) {
  const count = PROFILES[profile].count;
  const counts = Object.fromEntries(ROW_KINDS.map((kind) => [kind, 0]));
  Object.assign(counts, {
    deployment: count * 2,
    finding: 1,
    'database-id': count,
    'namespace-id': count,
    route: PROFILES[profile].late ? count - 1 : 0,
  });
  return {
    version: 1,
    operationId: operationId(profile, 1),
    optionsDigest: await digest([
      JSON.stringify(
        Object.entries(OPTIONS).sort(([a], [b]) => (a < b ? -1 : 1)),
      ),
    ]),
    options: OPTIONS,
    state,
    updatedAt: new Date(NOW).toISOString(),
    progress: {
      stage: { step: 'finalize' },
      generation: 1,
      revision: 1,
      stagedCounts: counts,
      factCount: count * 4,
      providerRequests: 0,
    },
  };
}

async function seedGeneration(env: Env, profile: Profile, offset: number) {
  const account = `workload-${profile}`;
  const db = env.FLEET_DB;
  const count = PROFILES[profile].count;
  if (
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    offset >= count ||
    offset % 100 !== 0
  )
    throw new Error('invalid seed offset');
  if (offset === 0) {
    const { control } = controlPlane(env, profile);
    await control.latestFinalizedInventoryGeneration();
    await control.pruneFleetOperations({ kind: 'audit', limit: 1 });
    const run = await generationRecord(profile, 'staging');
    await db.batch([
      db
        .prepare(
          'INSERT INTO anchorage_fleet_inventory_heads VALUES (?, ?, NULL, 2)',
        )
        .bind(account, run.operationId),
      db
        .prepare(
          'INSERT INTO anchorage_fleet_inventory_runs VALUES (?, ?, 1, ?, ?, ?, NULL)',
        )
        .bind(
          run.operationId,
          account,
          run.optionsDigest,
          JSON.stringify(run),
          NOW,
        ),
      db
        .prepare(
          'INSERT INTO anchorage_fleet_inventory_rows VALUES (?, 1, ?, 0, ?)',
        )
        .bind(
          account,
          'finding',
          JSON.stringify({ record: 'finding', ...FINDING }),
        ),
    ]);
  }
  const statements: D1PreparedStatement[] = [];
  const row = (kind: string, ordinal: number, payload: unknown) =>
    statements.push(
      db
        .prepare(
          'INSERT INTO anchorage_fleet_inventory_rows VALUES (?, 1, ?, ?, ?)',
        )
        .bind(account, kind, ordinal, JSON.stringify(payload)),
    );
  for (let index = offset; index < Math.min(count, offset + 100); index += 1) {
    const value = record(profile, index);
    row('deployment', index, {
      record: 'candidate-script',
      scriptName: value.scriptName,
    });
    row('deployment', count + index, {
      record: 'deployment',
      backend: 'plain-worker',
      scriptName: value.scriptName,
      tenantTag: value.tenantTag,
      environment: value.environment,
      artifactVersion: value.artifactVersion,
      schemaVersion: 1,
    });
    row('database-id', index, {
      record: 'database-id',
      databaseId: value.databaseId,
    });
    row('namespace-id', index, {
      record: 'namespace-id',
      namespaceId: `namespace-${index}`,
    });
    if (value.phase === 'ready')
      row('route', index, {
        record: 'route',
        backend: 'plain-worker',
        hostname: value.routeHostname,
        scriptName: value.scriptName,
        tenantTag: value.tenantTag,
        environment: value.environment,
      });
    for (const [kind, payload] of [
      ['database-id', { databaseId: value.databaseId }],
      ['durable-object-binding', value.durableObjectBindings[0]],
      ['secret-name', { secretName: `secret-${index}` }],
      ['route-hostname', { hostname: value.routeHostname }],
    ] as const)
      statements.push(
        db
          .prepare(
            'INSERT INTO anchorage_fleet_inventory_deployment_facts VALUES (?, 1, ?, ?, 0, ?)',
          )
          .bind(account, count + index, kind, JSON.stringify(payload)),
      );
  }
  await batch(db, statements);
  const next = Math.min(count, offset + 100);
  if (next === count) {
    const run = await generationRecord(profile, 'finalized');
    const physical = await db
      .prepare(
        'SELECT kind, COUNT(*) AS count, MIN(ordinal) AS first, MAX(ordinal) AS last FROM anchorage_fleet_inventory_rows WHERE account_id = ? AND generation = 1 GROUP BY kind',
      )
      .bind(account)
      .all<{ kind: string; count: number; first: number; last: number }>();
    for (const kind of ROW_KINDS) {
      const actual = physical.results.find((entry) => entry.kind === kind);
      const expected = run.progress.stagedCounts[kind];
      if (
        (actual?.count ?? 0) !== expected ||
        (expected > 0 && (actual?.first !== 0 || actual?.last !== expected - 1))
      )
        throw new Error('seed row manifest mismatch');
    }
    const factCount = await db
      .prepare(
        'SELECT COUNT(*) AS count FROM anchorage_fleet_inventory_deployment_facts WHERE account_id = ? AND generation = 1',
      )
      .bind(account)
      .first<number>('count');
    if (factCount !== run.progress.factCount)
      throw new Error('seed fact manifest mismatch');
    await db.batch([
      db
        .prepare(
          'UPDATE anchorage_fleet_inventory_runs SET run_record = ?, finalized_at_ms = ? WHERE operation_id = ? AND account_id = ?',
        )
        .bind(JSON.stringify(run), NOW, run.operationId, account),
      db
        .prepare(
          'UPDATE anchorage_fleet_inventory_heads SET active_operation_id = NULL, latest_finalized_generation = 1 WHERE account_id = ?',
        )
        .bind(account),
    ]);
    const payloads = await db
      .prepare(
        'SELECT COUNT(*) AS entries, SUM(length(CAST(payload AS BLOB))) AS payloadBytes, MAX(length(CAST(payload AS BLOB))) AS maximumPayloadBytes FROM (SELECT payload FROM anchorage_fleet_inventory_rows WHERE account_id = ? AND generation = 1 UNION ALL SELECT payload FROM anchorage_fleet_inventory_deployment_facts WHERE account_id = ? AND generation = 1)',
      )
      .bind(account, account)
      .first();
    return {
      next,
      done: true,
      summary: { rowManifest: run.progress.stagedCounts, factCount, payloads },
    };
  }
  return { next, done: false };
}

async function seedPriorFacts(env: Env, profile: Profile, offset: number) {
  const count = PROFILES[profile].count - 1;
  if (
    !PROFILES[profile].late ||
    !Number.isSafeInteger(offset) ||
    offset < 0 ||
    offset >= count ||
    offset % 100 !== 0
  )
    throw new Error('invalid fact seed offset');
  const account = `workload-${profile}`;
  const id = operationId(profile);
  const statements: D1PreparedStatement[] = [];
  for (let index = offset; index < Math.min(count, offset + 100); index += 1) {
    const value = record(profile, index);
    for (const [ordinal, factKind, key] of [
      [2 * index, 'database-owner', value.databaseId],
      [2 * index + 1, 'namespace-owner', `namespace-${index}`],
    ] as const)
      statements.push(
        env.FLEET_DB.prepare(
          'INSERT INTO anchorage_fleet_operation_rows VALUES (?, ?, ?, ?, ?)',
        ).bind(
          account,
          id,
          'fact',
          ordinal,
          JSON.stringify({
            factKind,
            key,
            tenantTag: value.tenantTag,
            environment: value.environment,
          }),
        ),
      );
  }
  await batch(env.FLEET_DB, statements);
  const next = Math.min(count, offset + 100);
  if (next === count) {
    const stored = await env.FLEET_DB.prepare(
      'SELECT op_record FROM anchorage_fleet_operations WHERE account_id = ? AND operation_id = ?',
    )
      .bind(account, id)
      .first<string>('op_record');
    if (!stored) throw new Error('missing seeded operation');
    const run = JSON.parse(stored);
    run.progress.stage = { step: 'per-record', recordOrdinal: count };
    run.progress.revision += 1;
    run.progress.findingCount = 1;
    run.progress.factCount = count * 2;
    await env.FLEET_DB.batch([
      env.FLEET_DB.prepare(
        'INSERT INTO anchorage_fleet_operation_rows VALUES (?, ?, ?, 0, ?)',
      ).bind(account, id, 'finding', JSON.stringify(FINDING)),
      env.FLEET_DB.prepare(
        'UPDATE anchorage_fleet_operations SET op_record = ? WHERE account_id = ? AND operation_id = ?',
      ).bind(JSON.stringify(run), account, id),
    ]);
    return {
      next,
      done: true,
      token: {
        version: 1,
        operationId: id,
        revision: run.progress.revision,
      } satisfies FleetOperationToken,
    };
  }
  return { next, done: false };
}

async function readback(env: Env, profile: Profile, id: string) {
  const row = await env.FLEET_DB.prepare(
    'SELECT op_record FROM anchorage_fleet_operations WHERE account_id = ? AND operation_id = ?',
  )
    .bind(`workload-${profile}`, id)
    .first<string>('op_record');
  const pins = await env.FLEET_DB.prepare(
    'SELECT COUNT(*) AS count FROM anchorage_fleet_inventory_pins WHERE account_id = ? AND pinned_by = ?',
  )
    .bind(`workload-${profile}`, `fleet-audit:${id}`)
    .first<number>('count');
  return { run: row ? JSON.parse(row) : null, pins };
}

export default {
  async fetch(request, env) {
    const input = (await request.json()) as {
      profile: Profile;
      action: string;
      offset?: number;
      token?: unknown;
      variant?: number;
      over?: boolean;
    };
    const invocationId = crypto.randomUUID();
    try {
      if (!Object.hasOwn(PROFILES, input.profile))
        throw new Error('unknown workload profile');
      const profile = input.profile;
      if (input.action === 'seed-generation')
        return Response.json({
          invocationId,
          result: await seedGeneration(env, profile, input.offset ?? 0),
        });
      if (input.action === 'seed-facts')
        return Response.json({
          invocationId,
          result: await seedPriorFacts(env, profile, input.offset ?? 0),
        });
      const { control, metrics, providerRequests } = controlPlane(env, profile);
      const id = operationId(profile, input.variant ?? 0);
      let result: unknown;
      if (input.action === 'read') {
        const inventory = await control.readFleetInventoryGeneration(1);
        function* identities() {
          for (const value of inventory.deployments)
            yield `${value.tenantTag}\0${value.scriptName}\0${value.databaseIds[0]}\0${value.durableObjectBindings[0]?.namespaceId}\n`;
        }
        result = {
          deployments: inventory.deployments.length,
          databases: inventory.databaseIds.length,
          namespaces: inventory.namespaceIds.length,
          routes: inventory.routes.length,
          findings: inventory.findings,
          identityDigest: await digest(identities()),
        };
      } else if (input.action === 'start' || input.action === 'continue') {
        const data =
          input.action === 'start'
            ? intake(
                profile,
                input.over ?? false,
                input.variant === 2 ? 1 : PROFILES[profile].count,
              )
            : undefined;
        const action: FleetAuditAdvanceAction = data
          ? {
              kind: 'start',
              operationId: id,
              records: data.records,
              generation: 1,
              staleAfterMs: 3600000,
            }
          : { kind: 'continue', token: input.token };
        const outcome = await control.advanceFleetAudit({
          action,
          specFor,
          maintenanceSecretFor: () => 'inert-secret',
          maxItemsPerCall: 500,
          auditClock: () => NOW,
        });
        result = {
          outcome,
          ...(data
            ? {
                intake: {
                  count: data.records.length,
                  bytes: data.bytes,
                  maximumItemBytes: data.maximumItemBytes,
                },
              }
            : {}),
          ...(await readback(env, profile, id)),
        };
      } else if (input.action === 'abandon') {
        await control.abandonFleetAuditOperation(id);
        result = await readback(env, profile, id);
      } else if (input.action === 'findings') {
        result = {
          page: await control.readFleetAuditFindingsPage({
            operationId: id,
            limit: 100,
          }),
          ...(await readback(env, profile, id)),
        };
      } else if (input.action === 'corrupt') {
        await env.FLEET_DB.prepare(
          'DELETE FROM anchorage_fleet_inventory_deployment_facts WHERE account_id = ? AND generation = 1 AND deployment_ordinal = ? AND fact_kind = ?',
        )
          .bind(`workload-${profile}`, PROFILES[profile].count, 'secret-name')
          .run();
        result = { corrupted: true };
      } else if (input.action === 'state')
        result = await readback(env, profile, id);
      else throw new Error('unknown workload action');
      return Response.json({
        invocationId,
        result,
        metrics,
        providerRequests: providerRequests(),
      });
    } catch (error) {
      return Response.json(
        {
          invocationId,
          error: {
            name: error instanceof Error ? error.name : 'Error',
            message: error instanceof Error ? error.message : String(error),
          },
        },
        { status: 500 },
      );
    }
  },
} satisfies ExportedHandler<Env>;
