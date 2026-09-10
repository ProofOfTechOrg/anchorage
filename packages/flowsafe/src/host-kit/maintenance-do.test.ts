// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  deploymentIdentityHeaders,
  type RunRetentionCursor,
} from '../do-runner/index.js';
import {
  START_IDEMPOTENCY_DDL,
  START_IDEMPOTENCY_TABLE,
} from '../do-runner/start-reservation-contract.js';
import {
  createFlowsafeMaintenanceDurableObject,
  type FlowsafeWorkerConfig,
  type FlowsafeWorkerEnv,
  MAINTENANCE_INSTANCE_NAME,
  type MaintenanceDurableObjectState,
  type MaintenanceHealth,
} from './flowsafe-worker.js';
import {
  MAINTENANCE_RECEIPT_HEADER,
  type MaintenanceCapabilityJwk,
  mintAsymmetricMaintenanceCapability,
  verifyMaintenanceReceipt,
} from './maintenance-capability.js';
import { staticTokenVerifier } from './verifier.js';

const NOW = Date.parse('2026-08-10T12:00:00.000Z');
const DEPLOYMENT_SECRET = 'test-deployment-identity-secret-0001';
const MAINTENANCE_SECRET = 'test-maintenance-capability-secret-0001';
const RETENTION_CURSOR_KEY = 'flowsafe:maintenance-run-retention-cursor:v1';
const CAPABILITY_PRIVATE_KEY = {
  kty: 'OKP',
  crv: 'Ed25519',
  alg: 'EdDSA',
  kid: 'fleet-maintenance-v1',
  x: 'Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo',
  d: 'gkXf8_b8kcCJxZ33fUYUac7yCsxZAxQXgsgPbwDpnlM',
} satisfies MaintenanceCapabilityJwk;
const CAPABILITY_PUBLIC_KEY = {
  kty: 'OKP',
  crv: 'Ed25519',
  alg: 'EdDSA',
  kid: 'fleet-maintenance-v1',
  x: 'Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo',
} satisfies MaintenanceCapabilityJwk;

interface TestEnv extends FlowsafeWorkerEnv {}

function environment(): TestEnv {
  const sqlite = openSqlite();
  sqlite.exec(`
    CREATE TABLE flowsafe_deployment (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      tenant_tag TEXT NOT NULL,
      provisioned_at TEXT NOT NULL
    );
    INSERT INTO flowsafe_deployment VALUES
      (1, 'acme', '2026-08-10T00:00:00.000Z');
  `);
  return {
    DB: sqliteUnitDatabase(sqlite) as TestEnv['DB'],
    DEPLOYMENT_TENANT: 'acme',
    FLEET_ENVIRONMENT: 'production',
    DEPLOYMENT_IDENTITY_SECRET: DEPLOYMENT_SECRET,
    RUNNER: {
      idFromName: (name: string) => name,
      get: () => ({ fetch: async () => new Response(null, { status: 204 }) }),
    },
    MAINTENANCE: {
      idFromName: (name: string) => name,
      get: () => ({ fetch: async () => new Response(null, { status: 204 }) }),
    },
  };
}

class FakeStorage {
  readonly events: string[] = [];
  readonly values = new Map<string, unknown>();
  alarmAt: number | null = null;
  failTransactionNumber?: number;
  failPutKey?: string;
  losePutResponseKey?: string;
  transactionCount = 0;

  async get<T>(key: string): Promise<T | undefined> {
    return structuredClone(this.values.get(key)) as T | undefined;
  }

  async put<T>(key: string, value: T): Promise<void> {
    this.events.push('direct-put');
    this.values.set(key, structuredClone(value));
  }

  async getAlarm(): Promise<number | null> {
    return this.alarmAt;
  }

  async setAlarm(value: number | Date): Promise<void> {
    this.alarmAt = Number(value);
  }

  async transaction<T>(
    closure: (transaction: {
      get<V>(key: string): Promise<V | undefined>;
      put<V>(key: string, value: V): Promise<void>;
      setAlarm(value: number | Date): Promise<void>;
    }) => Promise<T>,
  ): Promise<T> {
    this.transactionCount += 1;
    const transactionNumber = this.transactionCount;
    const writes = new Map<string, unknown>();
    let nextAlarm = this.alarmAt;
    const result = await closure({
      get: async <V>(key: string) => {
        this.events.push('transaction-get');
        return structuredClone(this.values.get(key)) as V | undefined;
      },
      put: async (key, value) => {
        this.events.push('transaction-put');
        writes.set(key, structuredClone(value));
      },
      setAlarm: async (value) => {
        this.events.push('transaction-alarm');
        nextAlarm = Number(value);
      },
    });
    if (
      this.failTransactionNumber === transactionNumber ||
      (this.failPutKey !== undefined && writes.has(this.failPutKey))
    ) {
      throw new Error('simulated crash after duty');
    }
    for (const [key, value] of writes) this.values.set(key, value);
    this.alarmAt = nextAlarm;
    if (
      this.losePutResponseKey !== undefined &&
      writes.has(this.losePutResponseKey)
    ) {
      throw new Error('simulated storage response loss');
    }
    return result;
  }
}

function harness(
  options: {
    throwSweep?: boolean;
    throwPurge?: boolean;
    withTick?: boolean;
    throwTick?: boolean;
    deadlineLimit?: number;
    config?: Partial<FlowsafeWorkerConfig<TestEnv>>;
  } = {},
) {
  const env = environment();
  const storage = new FakeStorage();
  const config = {
    workflows: [],
    systemPrincipalId: 'maintenance-test',
    buildVerifier: () => staticTokenVerifier(new Map()),
    maintenance: {
      sweepIntervalMs: 15 * 60 * 1_000,
      purgeIntervalMs: 60 * 60 * 1_000,
      ...(options.deadlineLimit
        ? { deadlineLimit: options.deadlineLimit }
        : {}),
      ...(options.withTick ? { tickIntervalMs: 60 * 1_000 } : {}),
    },
    ...(options.throwSweep
      ? {
          notify: () => {
            throw new Error('simulated sweep crash');
          },
        }
      : {}),
    ...(options.withTick
      ? {
          scheduleTick: () => async () => {
            storage.events.push('io');
            if (options.throwTick) throw new Error('simulated tick failure');
            return { fired: 0 };
          },
        }
      : {}),
    ...(options.throwPurge
      ? {
          extraPurgeDuties: async () => {
            throw new Error('simulated purge failure');
          },
        }
      : {}),
    ...options.config,
  } satisfies FlowsafeWorkerConfig<TestEnv>;
  const Maintenance = createFlowsafeMaintenanceDurableObject(config);
  const state = {
    id: { name: MAINTENANCE_INSTANCE_NAME },
    storage,
  } as unknown as MaintenanceDurableObjectState;
  const instance = new Maintenance(state, env);
  const internalRequest = (path: string, method: string) =>
    new Request(`http://maintenance${path}`, {
      method,
      headers: deploymentIdentityHeaders(DEPLOYMENT_SECRET),
    });
  const reconstruct = (
    overrides: Partial<FlowsafeWorkerConfig<TestEnv>> = {},
  ) => {
    const Reconstructed = createFlowsafeMaintenanceDurableObject({
      ...config,
      ...overrides,
    });
    return new Reconstructed(state, env);
  };
  return { env, instance, storage, internalRequest, reconstruct };
}

async function createRetentionSnapshots(
  env: TestEnv,
  prefix = '',
): Promise<void> {
  await env.DB.prepare(
    `CREATE TABLE ${prefix}mastra_workflow_snapshot (
      workflow_name TEXT NOT NULL,
      run_id TEXT NOT NULL,
      resourceId TEXT,
      snapshot TEXT NOT NULL,
      createdAt TEXT NOT NULL,
      updatedAt TEXT NOT NULL,
      UNIQUE(workflow_name, run_id)
    )`,
  ).run();
}

async function insertRetentionSnapshot(
  env: TestEnv,
  runId: string,
  prefix = '',
): Promise<void> {
  const old = new Date(NOW - 90 * 86_400_000).toISOString();
  await env.DB.prepare(
    `INSERT INTO ${prefix}mastra_workflow_snapshot
     (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
     VALUES ('wf', ?, NULL, ?, ?, ?)`,
  )
    .bind(
      runId,
      JSON.stringify({
        status: 'success',
        requestContext: {
          'flowsafe.runProvenance': {
            version: 2,
            startToken: `start-${runId}`,
          },
        },
      }),
      old,
      old,
    )
    .run();
}

async function insertRetentionReservation(
  env: TestEnv,
  key: string,
  state = 'terminal',
): Promise<void> {
  await env.DB.prepare(
    `INSERT INTO ${START_IDEMPOTENCY_TABLE}
     (key, owner_kind, owner_id, target_kind, target_id, run_id,
      thread_id, state, created_at, updated_at,
      start_token, start_table_prefix, start_workflow_id)
     VALUES (?, 'human', 'ada', 'workflow', 'wf', ?, NULL, ?, ?, ?, NULL, NULL, NULL)`,
  )
    .bind(
      key,
      `run-${key}`,
      state,
      NOW - 90 * 86_400_000,
      NOW - 90 * 86_400_000,
    )
    .run();
}

async function snapshotIds(env: TestEnv, prefix = ''): Promise<string[]> {
  const { results } = await env.DB.prepare(
    `SELECT run_id FROM ${prefix}mastra_workflow_snapshot ORDER BY rowid`,
  ).all<{ run_id: string }>();
  return results.map((row) => row.run_id);
}

async function reservationKeys(env: TestEnv): Promise<string[]> {
  const { results } = await env.DB.prepare(
    `SELECT key FROM ${START_IDEMPOTENCY_TABLE} ORDER BY rowid`,
  ).all<{ key: string }>();
  return results.map((row) => row.key);
}

async function nextPurge(
  instance: ReturnType<typeof harness>['instance'],
  internalRequest: ReturnType<typeof harness>['internalRequest'],
): Promise<MaintenanceHealth & { alarmAt: number | null }> {
  const before = await healthOf(instance, internalRequest('/status', 'GET'));
  vi.setSystemTime(before.nextPurgeAt);
  await instance.alarm();
  await instance.alarm();
  await instance.alarm();
  const after = await healthOf(instance, internalRequest('/status', 'GET'));
  expect(after.lastPurgeAttemptAt).toBe(before.nextPurgeAt);
  return after;
}

async function healthOf(
  instance: { fetch(request: Request): Promise<Response> },
  request: Request,
): Promise<MaintenanceHealth & { alarmAt: number | null }> {
  const response = await instance.fetch(request);
  expect(response.status).toBe(200);
  return (await response.json()) as MaintenanceHealth & {
    alarmAt: number | null;
  };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('alarm-driven deployment maintenance', () => {
  it('reconstructs independent retention positions and finishes a finite cycle past artifact failures', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const artifactCalls: string[] = [];
    const { env, instance, storage, internalRequest, reconstruct } = harness({
      config: {
        artifactStore: () => ({
          deleteRun: async (_workflowId, runId) => {
            artifactCalls.push(runId);
            if (runId.startsWith('poison-'))
              throw new Error('artifact unavailable');
            return 0;
          },
        }),
      },
    });
    await createRetentionSnapshots(env);
    await env.DB.prepare(START_IDEMPOTENCY_DDL).run();
    const poisonIds = Array.from(
      { length: 91 },
      (_, index) => `poison-${index}`,
    );
    for (const runId of poisonIds) {
      await insertRetentionSnapshot(env, runId);
    }
    const heldKeys = poisonIds.slice(0, 90);
    for (const runId of heldKeys) {
      await insertRetentionReservation(env, runId, 'started');
    }
    await insertRetentionSnapshot(env, 'eligible');
    await insertRetentionReservation(env, 'eligible-orphan');
    await instance.fetch(internalRequest('/ensure', 'POST'));

    await nextPurge(instance, internalRequest);

    expect(await storage.get(RETENTION_CURSOR_KEY)).toEqual({
      version: 1,
      tablePrefix: '',
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      snapshots: { afterRowId: 90, highWaterRowId: 92 },
      reservations: { afterRowId: 90, highWaterRowId: 91 },
    });
    expect(artifactCalls).toEqual(poisonIds.slice(0, 90));
    expect(await snapshotIds(env)).toEqual([...poisonIds, 'eligible']);
    expect(await reservationKeys(env)).toContain('eligible-orphan');

    await insertRetentionSnapshot(env, 'late');
    await insertRetentionReservation(env, 'late-orphan');
    artifactCalls.length = 0;
    const second = reconstruct();
    const secondHealth = await nextPurge(second, internalRequest);

    expect(artifactCalls).toEqual(['poison-90', 'eligible']);
    expect(await snapshotIds(env)).toEqual([...poisonIds, 'late']);
    expect(await reservationKeys(env)).toEqual([...heldKeys, 'late-orphan']);
    expect(await storage.get(RETENTION_CURSOR_KEY)).toEqual({
      version: 1,
      tablePrefix: '',
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
    });
    expect(secondHealth.alarmAt).toBeGreaterThan(
      secondHealth.lastPurgeAttemptAt ?? 0,
    );

    const recoveredArtifacts: string[] = [];
    const third = reconstruct({
      artifactStore: () => ({
        deleteRun: async (_workflowId, runId) => {
          recoveredArtifacts.push(runId);
          return 0;
        },
      }),
    });
    await nextPurge(third, internalRequest);
    expect(recoveredArtifacts).toEqual(poisonIds.slice(0, 90));
    const fourth = reconstruct({
      artifactStore: () => ({ deleteRun: async () => 0 }),
    });
    await nextPurge(fourth, internalRequest);
    expect(await snapshotIds(env)).toEqual([]);
    expect(await reservationKeys(env)).toEqual(heldKeys);
  });

  it.each([
    { tablePrefix: 'retired_', startIdempotencyTable: START_IDEMPOTENCY_TABLE },
    { tablePrefix: 'tenant_', startIdempotencyTable: 'retired_reservations' },
    { tablePrefix: 'tenant_' },
  ])('resets a valid stored scope when configuration changes: %j', async (scope) => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { env, instance, storage, internalRequest, reconstruct } = harness();
    await createRetentionSnapshots(env, 'tenant_');
    await createRetentionSnapshots(env, 'retired_');
    await insertRetentionSnapshot(env, 'eligible', 'tenant_');
    await insertRetentionSnapshot(env, 'other-scope', 'retired_');
    await instance.fetch(internalRequest('/ensure', 'POST'));
    await storage.put(RETENTION_CURSOR_KEY, {
      version: 1,
      ...scope,
      snapshots: { afterRowId: 100, highWaterRowId: 200 },
    });

    const changed = reconstruct({ storageTablePrefix: 'TeNaNt_' });
    const health = await nextPurge(changed, internalRequest);

    expect(health.lastPurgeError).toBeUndefined();
    expect(await snapshotIds(env, 'tenant_')).toEqual([]);
    expect(await snapshotIds(env, 'retired_')).toEqual(['other-scope']);
    expect(await storage.get(RETENTION_CURSOR_KEY)).toEqual({
      version: 1,
      tablePrefix: 'tenant_',
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
    });
  });

  it('preserves a matching canonical scope across reconstruction', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { env, instance, storage, internalRequest, reconstruct } = harness();
    await createRetentionSnapshots(env, 'tenant_');
    await insertRetentionSnapshot(env, 'already-scanned', 'tenant_');
    await insertRetentionSnapshot(env, 'eligible', 'tenant_');
    await instance.fetch(internalRequest('/ensure', 'POST'));
    await storage.put(RETENTION_CURSOR_KEY, {
      version: 1,
      tablePrefix: 'tenant_',
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      snapshots: { afterRowId: 1, highWaterRowId: 2 },
    });

    await nextPurge(
      reconstruct({ storageTablePrefix: 'TeNaNt_' }),
      internalRequest,
    );

    expect(await snapshotIds(env, 'tenant_')).toEqual(['already-scanned']);
    expect(
      await storage.get<RunRetentionCursor>(RETENTION_CURSOR_KEY),
    ).not.toHaveProperty('snapshots');
  });

  it.each([
    null,
    false,
    0,
    '',
    [],
    {},
    { version: 2, tablePrefix: '' },
    { version: 1, tablePrefix: '', snapshots: null },
    {
      version: 1,
      tablePrefix: '',
      snapshots: { afterRowId: 2, highWaterRowId: 1 },
    },
    {
      version: 1,
      tablePrefix: 'retired_',
      snapshots: { afterRowId: 0.5, highWaterRowId: 2 },
    },
    {
      version: 1,
      tablePrefix: '',
      reservations: { afterRowId: 1, highWaterRowId: 2 },
    },
  ])('refuses malformed stored retention state while preserving sibling duties: %j', async (stored) => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const artifactStore = vi.fn(() => ({ deleteRun: async () => 0 }));
    const extraPurgeDuties = vi.fn(async () => ({ extra: true }));
    const { env, instance, storage, internalRequest } = harness({
      config: { artifactStore, extraPurgeDuties },
    });
    await createRetentionSnapshots(env);
    await insertRetentionSnapshot(env, 'eligible');
    await instance.fetch(internalRequest('/ensure', 'POST'));
    await storage.put(RETENTION_CURSOR_KEY, stored);

    const health = await nextPurge(instance, internalRequest);

    expect(health.lastPurgeAt).toBeUndefined();
    expect(health.lastPurgeError).toContain('retention-purge');
    expect(health.lastSweepAt).toBe(NOW);
    expect(health.lastDeadlineAt).toBe(NOW);
    expect(health.alarmAt).toBeGreaterThan(NOW);
    expect(artifactStore).not.toHaveBeenCalled();
    expect(extraPurgeDuties).toHaveBeenCalledOnce();
    expect(await snapshotIds(env)).toEqual(['eligible']);
    expect(await storage.get(RETENTION_CURSOR_KEY)).toEqual(stored);
    expect(
      await env.DB.prepare(
        "SELECT name FROM sqlite_schema WHERE name = 'flowsafe_resource_owners'",
      ).all(),
    ).toMatchObject({ results: [] });
  });

  it.each([
    'rollback',
    'response loss',
  ] as const)('stops orphan retention after cursor persistence %s and safely resumes in a new instance', async (failure) => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const artifactCalls: string[] = [];
    const extraPurgeDuties = vi.fn(async () => ({ extra: true }));
    const { env, instance, storage, internalRequest, reconstruct } = harness({
      config: {
        extraPurgeDuties,
        artifactStore: () => ({
          deleteRun: async (_workflowId, runId) => {
            artifactCalls.push(runId);
            expect(storage.events.slice(-2)).toEqual([
              'transaction-put',
              'transaction-alarm',
            ]);
            expect(storage.alarmAt).toBeGreaterThan(NOW);
            return 0;
          },
        }),
      },
    });
    await createRetentionSnapshots(env);
    await insertRetentionSnapshot(env, 'eligible');
    await env.DB.prepare(START_IDEMPOTENCY_DDL).run();
    await insertRetentionReservation(env, 'orphan');
    const deadlineCursor = {
      workflowId: 'wf',
      runId: 'previous',
      deadlineAt: NOW - 1,
    };
    await storage.put(
      'flowsafe:maintenance-deadline-cursor:v1',
      deadlineCursor,
    );
    await instance.fetch(internalRequest('/ensure', 'POST'));
    if (failure === 'rollback') storage.failPutKey = RETENTION_CURSOR_KEY;
    else storage.losePutResponseKey = RETENTION_CURSOR_KEY;

    const health = await nextPurge(instance, internalRequest);

    expect(health.lastPurgeError).toContain('retention-purge');
    expect(health.lastPurgeAt).toBeUndefined();
    expect(health.alarmAt).toBeGreaterThan(NOW);
    expect(artifactCalls).toEqual(['eligible']);
    expect(await snapshotIds(env)).toEqual([]);
    expect(await reservationKeys(env)).toEqual(['orphan']);
    expect(extraPurgeDuties).toHaveBeenCalledOnce();
    expect(
      await storage.get('flowsafe:maintenance-deadline-cursor:v1'),
    ).toEqual(deadlineCursor);
    const cursor = await storage.get(RETENTION_CURSOR_KEY);
    if (failure === 'rollback') expect(cursor).toBeUndefined();
    else
      expect(cursor).toEqual({
        version: 1,
        tablePrefix: '',
        startIdempotencyTable: START_IDEMPOTENCY_TABLE,
      });

    storage.failPutKey = undefined;
    storage.losePutResponseKey = undefined;
    const recovered = await nextPurge(reconstruct(), internalRequest);
    expect(recovered.lastPurgeError).toBeUndefined();
    expect(recovered.lastPurgeAt).toBe(NOW + 60 * 60 * 1_000);
    expect(await reservationKeys(env)).toEqual([]);
    expect(artifactCalls).toEqual(['eligible']);
    expect(extraPurgeDuties).toHaveBeenCalledTimes(2);
  });

  it('does not checkpoint a committed D1 mutation with a lost response and retries safely after reconstruction', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    let retentionBatch = false;
    const extraPurgeDuties = vi.fn(async () => ({ extra: true }));
    const { env, instance, storage, internalRequest, reconstruct } = harness({
      config: {
        extraPurgeDuties,
        artifactStore: () => ({
          deleteRun: async () => {
            retentionBatch = true;
            return 0;
          },
        }),
      },
    });
    await createRetentionSnapshots(env);
    await insertRetentionSnapshot(env, 'eligible');
    await env.DB.prepare(START_IDEMPOTENCY_DDL).run();
    await insertRetentionReservation(env, 'orphan');
    const db = env.DB;
    const batch = db.batch?.bind(db);
    if (!batch) throw new Error('SQLite fixture requires batch');
    env.DB = {
      prepare: db.prepare.bind(db),
      batch: async (statements) => {
        const results = await batch(statements);
        if (retentionBatch) {
          retentionBatch = false;
          throw new Error('D1 response lost after commit');
        }
        return results;
      },
    };
    await instance.fetch(internalRequest('/ensure', 'POST'));

    const health = await nextPurge(instance, internalRequest);

    expect(health.lastPurgeError).toContain('D1 response lost after commit');
    expect(health.lastPurgeAt).toBeUndefined();
    expect(await storage.get(RETENTION_CURSOR_KEY)).toBeUndefined();
    expect(await snapshotIds(env)).toEqual([]);
    expect(await reservationKeys(env)).toEqual(['orphan']);
    expect(extraPurgeDuties).toHaveBeenCalledOnce();
    expect(health.alarmAt).toBeGreaterThan(NOW);

    const recovered = await nextPurge(reconstruct(), internalRequest);
    expect(recovered.lastPurgeError).toBeUndefined();
    expect(await reservationKeys(env)).toEqual([]);
    expect(await storage.get(RETENTION_CURSOR_KEY)).toEqual({
      version: 1,
      tablePrefix: '',
      startIdempotencyTable: START_IDEMPOTENCY_TABLE,
    });
  });

  it.each([
    'script',
    'digest',
  ] as const)('rejects a capability for another catalog %s before maintenance work', async (changed) => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    for (const operation of [
      'ensure-maintenance',
      'maintenance-status',
    ] as const) {
      const { env, instance, storage } = harness();
      env.MAINTENANCE_ADMIN_SECRET = MAINTENANCE_SECRET;
      env.FLEET_MAINTENANCE_CAPABILITIES = 'required';
      env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY = JSON.stringify(
        CAPABILITY_PUBLIC_KEY,
      );
      Object.assign(env, {
        FLEET_RESOURCE_ROLE: 'platform-catalog',
        FLEET_DEPLOYMENT_SCRIPT: 'acme-catalog',
        FLEET_SPEC_DIGEST: 'a'.repeat(64),
      });
      const minted = await mintAsymmetricMaintenanceCapability({
        privateKey: CAPABILITY_PRIVATE_KEY,
        operation,
        tenantTag: 'acme',
        environment: 'production',
        scriptName: changed === 'script' ? 'other-catalog' : 'acme-catalog',
        specDigest: changed === 'digest' ? 'b'.repeat(64) : 'a'.repeat(64),
        now: () => NOW,
      });
      const response = await instance.fetch(
        new Request(
          operation === 'ensure-maintenance'
            ? 'http://maintenance/ensure'
            : 'http://maintenance/status',
          {
            method: operation === 'ensure-maintenance' ? 'POST' : 'GET',
            headers: { authorization: `Bearer ${minted.token}` },
          },
        ),
      );
      expect(response.status).toBe(401);
      expect(response.headers.get(MAINTENANCE_RECEIPT_HEADER)).toBeNull();
      expect(
        await storage.get('flowsafe:maintenance-nonces:v1'),
      ).toBeUndefined();
      expect(storage.alarmAt).toBeNull();
    }
  });

  it.each([
    false,
    true,
  ])('consumes one-shot capabilities and signs a nonce-bound result (local catalog: %s)', async (catalog) => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const { env, instance } = harness();
    env.MAINTENANCE_ADMIN_SECRET = MAINTENANCE_SECRET;
    env.FLEET_MAINTENANCE_CAPABILITIES = 'required';
    env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY = JSON.stringify(
      CAPABILITY_PUBLIC_KEY,
    );
    if (catalog)
      Object.assign(env, {
        FLEET_RESOURCE_ROLE: 'platform-catalog',
        FLEET_DEPLOYMENT_SCRIPT: 'acme-release-a1b2',
        FLEET_SPEC_DIGEST: 'a'.repeat(64),
      });
    const minted = await mintAsymmetricMaintenanceCapability({
      privateKey: CAPABILITY_PRIVATE_KEY,
      operation: 'ensure-maintenance',
      tenantTag: 'acme',
      environment: 'production',
      scriptName: 'acme-release-a1b2',
      specDigest: 'a'.repeat(64),
      now: () => NOW,
      nonce: 'AAAAAAAAAAAAAAAAAAAAAA',
    });
    const request = () =>
      new Request('http://maintenance/ensure', {
        method: 'POST',
        headers: { authorization: `Bearer ${minted.token}` },
      });

    const first = await instance.fetch(request());
    expect(first.status).toBe(200);
    const receipt = first.headers.get(MAINTENANCE_RECEIPT_HEADER);
    expect(receipt).toBeTruthy();
    await expect(
      verifyMaintenanceReceipt({
        secret: MAINTENANCE_SECRET,
        token: receipt ?? '',
        capability: minted.claims,
        now: () => NOW,
      }),
    ).resolves.toMatchObject({ alarmAt: NOW });
    const replay = await instance.fetch(request());
    expect(replay.status).toBe(401);
  });

  it('rejects the reusable deployment identity in capability-required mode', async () => {
    const { env, instance, internalRequest } = harness();
    env.MAINTENANCE_ADMIN_SECRET = MAINTENANCE_SECRET;
    env.FLEET_MAINTENANCE_CAPABILITIES = 'required';
    env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY = JSON.stringify(
      CAPABILITY_PUBLIC_KEY,
    );

    const response = await instance.fetch(internalRequest('/ensure', 'POST'));

    expect(response.status).toBe(401);
  });

  it('rejects a same-tenant capability for another environment before storage mutation', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const { env, instance, storage } = harness();
    env.MAINTENANCE_ADMIN_SECRET = MAINTENANCE_SECRET;
    env.FLEET_MAINTENANCE_CAPABILITIES = 'required';
    env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY = JSON.stringify(
      CAPABILITY_PUBLIC_KEY,
    );
    const minted = await mintAsymmetricMaintenanceCapability({
      privateKey: CAPABILITY_PRIVATE_KEY,
      operation: 'ensure-maintenance',
      tenantTag: 'acme',
      environment: 'staging',
      scriptName: 'acme-release-a1b2',
      specDigest: 'a'.repeat(64),
      now: () => NOW,
      nonce: 'CCCCCCCCCCCCCCCCCCCCCC',
    });

    const response = await instance.fetch(
      new Request('http://maintenance/ensure', {
        method: 'POST',
        headers: { authorization: `Bearer ${minted.token}` },
      }),
    );

    expect(response.status).toBe(401);
    expect(await storage.get('flowsafe:maintenance-nonces:v1')).toBeUndefined();
  });

  it('keeps maintenance-status capabilities replay-safe and read-only', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const { env, instance, storage } = harness();
    env.MAINTENANCE_ADMIN_SECRET = MAINTENANCE_SECRET;
    env.FLEET_MAINTENANCE_CAPABILITIES = 'required';
    env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY = JSON.stringify(
      CAPABILITY_PUBLIC_KEY,
    );
    const minted = await mintAsymmetricMaintenanceCapability({
      privateKey: CAPABILITY_PRIVATE_KEY,
      operation: 'maintenance-status',
      tenantTag: 'acme',
      environment: 'production',
      scriptName: 'acme-release-a1b2',
      specDigest: 'a'.repeat(64),
      now: () => NOW,
      nonce: 'BBBBBBBBBBBBBBBBBBBBBB',
    });
    const request = () =>
      new Request('http://maintenance/status', {
        headers: { authorization: `Bearer ${minted.token}` },
      });

    expect((await instance.fetch(request())).status).toBe(200);
    expect((await instance.fetch(request())).status).toBe(200);
    expect(await storage.get('flowsafe:maintenance-nonces:v1')).toBeUndefined();
  });

  it('self-arms, persists health, and runs one tied duty per alarm in deterministic order', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { instance, storage, internalRequest } = harness({
      withTick: true,
    });

    const ensured = await healthOf(
      instance,
      internalRequest('/ensure', 'POST'),
    );
    expect(ensured).toMatchObject({
      nextDeadlineAt: NOW,
      nextSweepAt: NOW,
      nextPurgeAt: NOW,
      nextTickAt: NOW,
      alarmAt: NOW,
    });

    await instance.alarm();
    let status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastDeadlineAt).toBe(NOW);
    expect(status.lastSweepAt).toBeUndefined();
    expect(status.lastPurgeAt).toBeUndefined();
    expect(status.lastTickAt).toBeUndefined();
    expect(status.alarmAt).toBe(NOW);

    await instance.alarm();
    status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastSweepAt).toBe(NOW);
    expect(status.lastPurgeAt).toBeUndefined();
    expect(status.lastTickAt).toBeUndefined();
    expect(status.alarmAt).toBe(NOW);

    await instance.alarm();
    status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastPurgeAt).toBe(NOW);
    expect(status.lastTickAt).toBeUndefined();
    expect(status.alarmAt).toBe(NOW);

    storage.events.length = 0;
    await instance.alarm();
    status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastTickAt).toBe(NOW);
    expect(storage.events.slice(0, 3)).toEqual([
      'transaction-put',
      'transaction-alarm',
      'io',
    ]);
  });

  it('persists deadline scan progress so a poison head row cannot starve the next run', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { env, instance, storage, internalRequest } = harness({
      deadlineLimit: 1,
    });
    const db = env.DB;
    await db
      .prepare(
        `CREATE TABLE mastra_workflow_snapshot (
          workflow_name TEXT NOT NULL,
          run_id TEXT NOT NULL,
          resourceId TEXT,
          snapshot TEXT NOT NULL,
          createdAt TEXT NOT NULL,
          updatedAt TEXT NOT NULL,
          UNIQUE(workflow_name, run_id)
        )`,
      )
      .run();
    for (const [runId, deadlineAt] of [
      ['poison', NOW - 2],
      ['eligible', NOW - 1],
    ] as const) {
      const iso = new Date(deadlineAt).toISOString();
      await db
        .prepare(
          `INSERT INTO mastra_workflow_snapshot
           (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
           VALUES (?, ?, NULL, ?, ?, ?)`,
        )
        .bind(
          'wf',
          runId,
          JSON.stringify({
            status: 'suspended',
            requestContext: {
              'flowsafe.runLifecycle': {
                version: 1,
                revision: 1,
                deadlineAt,
              },
            },
          }),
          iso,
          iso,
        )
        .run();
    }
    const calls: string[] = [];
    env.RUNNER = {
      idFromName: (name: string) => name,
      get: (id: unknown) => ({
        fetch: async () => {
          const runId = String(id).split(':').at(-1) as string;
          calls.push(runId);
          return runId === 'poison'
            ? Response.json({ error: 'permanent failure' }, { status: 500 })
            : Response.json({ runId, status: 'timed_out' });
        },
      }),
    };

    await instance.fetch(internalRequest('/ensure', 'POST'));
    await instance.alarm();
    await instance.alarm();
    await instance.alarm();
    vi.setSystemTime(NOW + 15 * 60 * 1_000);
    await instance.alarm();

    expect(calls).toEqual(['poison', 'eligible']);
    expect(
      await storage.get('flowsafe:maintenance-deadline-cursor:v1'),
    ).toEqual({
      workflowId: 'wf',
      runId: 'eligible',
      deadlineAt: NOW - 1,
    });
  });

  it('keeps the chain armed when an invocation crashes after its duty', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { instance, storage, internalRequest } = harness();
    await instance.fetch(internalRequest('/ensure', 'POST'));
    storage.failTransactionNumber = storage.transactionCount + 2;

    await expect(instance.alarm()).rejects.toThrow(
      /simulated crash after duty/,
    );
    expect(storage.alarmAt).toBe(NOW);

    await instance.alarm();
    const status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastSweepAt).toBe(NOW);
    expect(status.alarmAt).toBe(NOW);
  });

  it('records a failed sweep attempt without advancing its last-success timestamp', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { instance, storage, internalRequest } = harness({
      throwSweep: true,
    });
    await instance.fetch(internalRequest('/ensure', 'POST'));

    await instance.alarm();
    await instance.alarm();
    expect(storage.alarmAt).toBe(NOW);

    let status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastSweepAt).toBeUndefined();
    expect(status.lastSweepAttemptAt).toBe(NOW);
    expect(status.lastSweepError).toContain('simulated sweep crash');

    await instance.alarm();
    status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastPurgeAt).toBe(NOW);
    expect(status.alarmAt).toBeGreaterThan(NOW);
  });

  it('records a partial purge failure without advancing purge success', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { instance, internalRequest } = harness({ throwPurge: true });
    await instance.fetch(internalRequest('/ensure', 'POST'));

    await instance.alarm();
    await instance.alarm();
    await instance.alarm();

    const status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastSweepAt).toBe(NOW);
    expect(status.lastPurgeAt).toBeUndefined();
    expect(status.lastPurgeAttemptAt).toBe(NOW);
    expect(status.lastPurgeError).toContain('simulated purge failure');
  });

  it('records a failed schedule tick without advancing tick success', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { instance, internalRequest } = harness({
      withTick: true,
      throwTick: true,
    });
    await instance.fetch(internalRequest('/ensure', 'POST'));

    await instance.alarm();
    await instance.alarm();
    await instance.alarm();
    await instance.alarm();

    const status = await healthOf(instance, internalRequest('/status', 'GET'));
    expect(status.lastTickAt).toBeUndefined();
    expect(status.lastTickAttemptAt).toBe(NOW);
    expect(status.lastTickError).toContain('simulated tick failure');
  });

  it('rejects non-singleton instances before touching maintenance state', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const { env, storage, internalRequest } = harness();
    const config = {
      workflows: [],
      systemPrincipalId: 'maintenance-test',
      buildVerifier: () => staticTokenVerifier(new Map()),
      maintenance: {
        sweepIntervalMs: 1_000,
        purgeIntervalMs: 2_000,
      },
    } satisfies FlowsafeWorkerConfig<TestEnv>;
    const Maintenance = createFlowsafeMaintenanceDurableObject(config);
    const instance = new Maintenance(
      {
        id: { name: 'caller-selected' },
        storage,
      } as unknown as MaintenanceDurableObjectState,
      env,
    );

    expect(
      (await instance.fetch(internalRequest('/ensure', 'POST'))).status,
    ).toBe(500);
    expect(storage.values.size).toBe(0);
  });
});
