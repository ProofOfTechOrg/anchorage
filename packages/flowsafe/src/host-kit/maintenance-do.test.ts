// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import { deploymentIdentityHeaders } from '../do-runner/index.js';
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
    if (this.failTransactionNumber === transactionNumber) {
      throw new Error('simulated crash after duty');
    }
    for (const [key, value] of writes) this.values.set(key, value);
    this.alarmAt = nextAlarm;
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
  return { env, instance, storage, internalRequest };
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
  it('consumes one-shot capabilities and signs a nonce-bound result', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const { env, instance } = harness();
    env.MAINTENANCE_ADMIN_SECRET = MAINTENANCE_SECRET;
    env.FLEET_MAINTENANCE_CAPABILITIES = 'required';
    env.FLEET_MAINTENANCE_CAPABILITY_PUBLIC_KEY = JSON.stringify(
      CAPABILITY_PUBLIC_KEY,
    );
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
          updatedAt TEXT NOT NULL
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
