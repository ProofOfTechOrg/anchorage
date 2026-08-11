// SPDX-License-Identifier: Apache-2.0
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';

const REPO_ROOT = new URL('..', import.meta.url).pathname;
const DATABASE = {
  binding: 'DB',
  database_name: 'flowsafe-demo',
  database_id: '00000000-0000-0000-0000-000000000000',
};
const MAINTENANCE_DATABASE = {
  binding: 'DB',
  database_name: 'flowsafe-maintenance-alarm-harness',
  database_id: '00000000-0000-0000-0000-000000000001',
};
const MAINTENANCE_ADMIN_SECRET = 'harness-maintenance-admin-secret-0001';
const DEPLOYMENT_IDENTITY_SECRET = 'harness-deployment-identity-secret-0001';
const MAINTENANCE_INSTANCE_NAME = 'deployment-maintenance';

function harnessOptions() {
  return {
    root: REPO_ROOT,
    workers: [
      { configPath: 'packages/flowsafe/spike/wrangler.jsonc' },
      {
        configPath: 'packages/flowsafe/deploy/wrangler.jsonc',
        vars: { DEPLOYMENT_TENANT: 'harness' },
        secrets: {
          DEPLOYMENT_IDENTITY_SECRET,
          MAINTENANCE_ADMIN_SECRET,
        },
      },
      {
        config: {
          name: 'flowsafe-maintenance-alarm-harness',
          main: `${REPO_ROOT}/packages/flowsafe/test-support/maintenance-alarm-harness-worker.ts`,
          compatibility_date: '2026-07-26',
          compatibility_flags: ['nodejs_compat'],
          durable_objects: {
            bindings: [
              { name: 'RUNNER', class_name: 'FlowsafeRunner' },
              { name: 'HUB', class_name: 'FlowsafeHub' },
              {
                name: 'MAINTENANCE',
                class_name: 'HarnessFlowsafeMaintenance',
              },
            ],
          },
          migrations: [
            {
              tag: 'v1',
              new_sqlite_classes: [
                'FlowsafeRunner',
                'FlowsafeHub',
                'HarnessFlowsafeMaintenance',
              ],
            },
          ],
          d1_databases: [MAINTENANCE_DATABASE],
          vars: {
            DEPLOYMENT_TENANT: 'harness',
            DEPLOYMENT_IDENTITY_SECRET,
            MAINTENANCE_ADMIN_SECRET,
            APPROVAL_SLA_SECONDS: '14400',
            RUN_RETENTION_DAYS: '30',
            APPROVAL_RETENTION_DAYS: '30',
          },
        },
      },
      {
        config: {
          name: 'flowsafe-harness-probe',
          main: `${REPO_ROOT}/packages/flowsafe/test-support/harness-probe.ts`,
          compatibility_date: '2025-06-01',
          compatibility_flags: ['nodejs_compat'],
          d1_databases: [DATABASE],
        },
      },
    ],
  } satisfies Parameters<typeof createTestHarness>[0];
}

async function result<T>(worker: WorkerHandle, path: string): Promise<T> {
  const response = await worker.fetch(path, { method: 'POST' });
  const body = await response.text();
  expect(response.status, body).toBe(200);
  return JSON.parse(body) as T;
}

describe.sequential('FlowSafe Wrangler test harness', () => {
  let server: TestHarness;
  let spike: WorkerHandle;
  let deploy: WorkerHandle;
  let alarmHarness: WorkerHandle;
  let probe: WorkerHandle;

  beforeAll(async () => {
    server = createTestHarness(harnessOptions());
    await server.listen();
  });

  beforeEach(async () => {
    await server.reset();
    spike = server.getWorker('flowsafe-do-runner-demo');
    deploy = server.getWorker('anchorage-flowsafe-replace-me');
    alarmHarness = server.getWorker('flowsafe-maintenance-alarm-harness');
    probe = server.getWorker('flowsafe-harness-probe');
    await result(probe, '/seed');
    for (const worker of [deploy, alarmHarness]) {
      const env = (await worker.getEnv()) as { DB: D1Database };
      await env.DB.prepare(
        `CREATE TABLE IF NOT EXISTS flowsafe_deployment (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          tenant_tag TEXT NOT NULL,
          provisioned_at TEXT NOT NULL
        )`,
      ).run();
      await env.DB.prepare(
        `INSERT OR REPLACE INTO flowsafe_deployment
         (id, tenant_tag, provisioned_at) VALUES (1, ?, ?)`,
      )
        .bind('harness', '2026-08-10T00:00:00.000Z')
        .run();
    }
  });

  afterAll(async () => {
    await server.close();
  });

  it('boots the full spike and initializes D1ApprovalStore', async () => {
    const catalog = await spike.fetch('/workflows', {
      headers: { authorization: 'Bearer spike-operator' },
    });
    expect(catalog.status).toBe(200);
    const approvals = await spike.fetch('/api/approvals', {
      headers: { authorization: 'Bearer spike-viewer' },
    });
    expect(approvals.status).toBe(200);
    expect(await approvals.json()).toEqual([]);
  });

  it('self-arms maintenance through the production Worker and fixed singleton DO', async () => {
    const unauthorized = await deploy.fetch('/admin/ensure-maintenance', {
      method: 'POST',
      headers: { authorization: 'Bearer wrong-secret' },
    });
    expect(unauthorized.status).toBe(401);

    const ensured = await deploy.fetch('/admin/ensure-maintenance', {
      method: 'POST',
      headers: {
        authorization: `Bearer ${MAINTENANCE_ADMIN_SECRET}`,
      },
    });
    const ensuredBody = (await ensured.json()) as {
      nextSweepAt?: number;
      nextPurgeAt?: number;
      alarmAt?: number;
    };
    expect(ensured.status, JSON.stringify(ensuredBody)).toBe(200);
    expect(ensuredBody.nextSweepAt).toEqual(expect.any(Number));
    expect(ensuredBody.nextPurgeAt).toEqual(expect.any(Number));
    expect(ensuredBody.alarmAt).toEqual(expect.any(Number));

    await expect
      .poll(
        async () => {
          const status = await deploy.fetch('/admin/maintenance-status', {
            headers: {
              authorization: `Bearer ${MAINTENANCE_ADMIN_SECRET}`,
            },
          });
          if (!status.ok) return false;
          const body = (await status.json()) as {
            lastSweepAt?: number;
            lastPurgeAt?: number;
            alarmAt?: number;
          };
          return Boolean(body.lastSweepAt && body.lastPurgeAt && body.alarmAt);
        },
        { timeout: 10_000 },
      )
      .toBe(true);
    expect(await deploy.listDurableObjectIds('MAINTENANCE')).toHaveLength(1);
  });

  it('persists a real-D1 sweep failure, stays armed, and recovers on the next sweep alarm', async () => {
    const env = (await alarmHarness.getEnv()) as {
      DB: D1Database;
      MAINTENANCE: DurableObjectNamespace;
    };
    await env.DB.prepare(
      'CREATE TABLE flowsafe_approvals (id TEXT PRIMARY KEY)',
    ).run();

    const ensured = await alarmHarness.fetch('/admin/ensure-maintenance', {
      method: 'POST',
      headers: {
        authorization: `Bearer ${MAINTENANCE_ADMIN_SECRET}`,
      },
    });
    expect(ensured.status, await ensured.clone().text()).toBe(200);

    type Health = {
      lastSweepAt?: number;
      lastSweepAttemptAt?: number;
      lastSweepError?: string;
      alarmAt?: number;
    };
    let failedHealth: Health | undefined;
    await expect
      .poll(
        async () => {
          const response = await alarmHarness.fetch(
            '/admin/maintenance-status',
            {
              headers: {
                authorization: `Bearer ${MAINTENANCE_ADMIN_SECRET}`,
              },
            },
          );
          if (!response.ok) return false;
          failedHealth = (await response.json()) as Health;
          return Boolean(
            failedHealth.lastSweepAttemptAt && failedHealth.lastSweepError,
          );
        },
        { timeout: 10_000 },
      )
      .toBe(true);
    if (!failedHealth) throw new Error('sweep failure health was not observed');
    expect(failedHealth).toMatchObject({
      lastSweepAttemptAt: expect.any(Number),
      lastSweepError: expect.stringContaining('no such column: workflow_id'),
      alarmAt: expect.any(Number),
    });
    expect(failedHealth).not.toHaveProperty('lastSweepAt');

    await env.DB.prepare('DROP TABLE flowsafe_approvals').run();
    const stub = env.MAINTENANCE.get(
      env.MAINTENANCE.idFromName(MAINTENANCE_INSTANCE_NAME),
    ) as unknown as {
      forceSweepAlarm(): Promise<void>;
      alarmTrace(): Promise<
        Array<{
          changedDuties: string[];
          events: string[];
          alarmAt: number | null;
        }>
      >;
    };
    await stub.forceSweepAlarm();

    const recovered = await alarmHarness.fetch('/admin/maintenance-status', {
      headers: {
        authorization: `Bearer ${MAINTENANCE_ADMIN_SECRET}`,
      },
    });
    const recoveredHealth = (await recovered.json()) as Health;
    expect(recovered.status, JSON.stringify(recoveredHealth)).toBe(200);
    expect(recoveredHealth.lastSweepAt).toEqual(expect.any(Number));
    expect(recoveredHealth.lastSweepAt).toBeGreaterThanOrEqual(
      failedHealth.lastSweepAttemptAt ?? 0,
    );
    expect(recoveredHealth.lastSweepAttemptAt).toBeGreaterThanOrEqual(
      failedHealth.lastSweepAttemptAt ?? 0,
    );
    expect(recoveredHealth).not.toHaveProperty('lastSweepError');
    expect(recoveredHealth.alarmAt).toEqual(expect.any(Number));

    const dutyInvocations = (await stub.alarmTrace()).filter(
      ({ changedDuties }) => changedDuties.length > 0,
    );
    expect(dutyInvocations.length).toBeGreaterThanOrEqual(2);
    for (const invocation of dutyInvocations) {
      expect(invocation.changedDuties).toHaveLength(1);
      expect(invocation.alarmAt).toEqual(expect.any(Number));
      expect(invocation.events).toEqual([
        'health-persisted',
        'alarm-armed',
        'health-persisted',
      ]);
    }
  });

  it('resolves approval open-create and transition races to one winner', async () => {
    const outcome = await result<{
      created: number;
      openIds: string[];
      transitionWinners: string[];
      stored: { status: string; claimedBy: string };
    }>(probe, '/approval');
    expect(outcome.created).toBe(1);
    expect(outcome.openIds).toHaveLength(1);
    expect(outcome.transitionWinners).toHaveLength(1);
    expect(outcome.stored).toMatchObject({
      status: 'claimed',
      claimedBy: outcome.transitionWinners[0],
    });
  });

  it('enforces schedule ownership, cap, claim, and delete rollback atomically', async () => {
    const outcome = await result<{
      capWinners: number;
      winnerId: string;
      loserId?: string;
      storedSchedules: string[];
      winnerOwner: { kind: string; id: string };
      loserOwner?: unknown;
      claimWinners: number;
      claimTriggers: unknown[];
      rollbackError: string;
      rollbackSchedule: { id: string };
      rollbackTriggers: unknown[];
      rollbackOwner: { kind: string; id: string };
      deleteResult: string;
      deletedSchedule: null;
      deletedTriggers: unknown[];
      deletedOwner?: unknown;
      ownerInsertError: string;
      ownerFailureSchedule: null;
      ownerFailureOwner?: unknown;
    }>(probe, '/schedule');
    expect(outcome.capWinners).toBe(1);
    expect(outcome.storedSchedules).toContain(outcome.winnerId);
    expect(outcome.storedSchedules).not.toContain(outcome.loserId);
    expect(outcome.winnerOwner).toEqual({ kind: 'human', id: 'opal' });
    expect(outcome.loserOwner).toBeUndefined();
    expect(outcome.claimWinners).toBe(1);
    expect(outcome.claimTriggers).toHaveLength(1);
    expect(outcome.rollbackError).toMatch(/injected owner delete failure/);
    expect(outcome.rollbackSchedule.id).toBe('schedule-rollback');
    expect(outcome.rollbackTriggers).toHaveLength(1);
    expect(outcome.rollbackOwner).toEqual({ kind: 'human', id: 'opal' });
    expect(outcome.deleteResult).toBe('deleted');
    expect(outcome.deletedSchedule).toBeNull();
    expect(outcome.deletedTriggers).toEqual([]);
    expect(outcome.deletedOwner).toBeUndefined();
    expect(outcome.ownerInsertError).toMatch(/injected owner insert failure/);
    expect(outcome.ownerFailureSchedule).toBeNull();
    expect(outcome.ownerFailureOwner).toBeUndefined();
  });

  it('coalesces notification maps through concurrent writers and rolls back a failed batch', async () => {
    const outcome = await result<{
      migrated: Array<{ id: string; insertionOrdinal: number }>;
      coalescedIds: string[];
      record: {
        coalescedCount: number;
        attributes: Record<string, boolean>;
        metadata: Record<string, boolean>;
      };
      rollbackError: string;
      rollbackSummary: string;
      ordinalColumns: number;
    }>(probe, '/notification');
    expect(outcome.migrated).toEqual([
      { id: 'physical-first', insertionOrdinal: 1 },
      { id: 'physical-second', insertionOrdinal: 2 },
    ]);
    expect(outcome.coalescedIds).toEqual(['notification-base']);
    expect(outcome.record).toMatchObject({
      coalescedCount: 3,
      attributes: { base: true, left: true, right: true },
      metadata: { base: true, left: true, right: true },
    });
    expect(outcome.ordinalColumns).toBe(1);
    expect(outcome.rollbackError).toMatch(/missing_notification_table/);
    expect(outcome.rollbackSummary).not.toBe('should-rollback');
  });

  it('preserves concurrent background workflow state and results in Mastra D1', async () => {
    const outcome = await result<{
      supportsConcurrentUpdates: boolean;
      stored: {
        status: string;
        context: { execute: { status: string; output: { ok: boolean } } };
        requestContext: { trace: string };
      };
    }>(probe, '/background');
    expect(outcome.supportsConcurrentUpdates).toBe(true);
    expect(outcome.stored).toMatchObject({
      status: 'running',
      context: { execute: { status: 'success', output: { ok: true } } },
      requestContext: { trace: 'yes' },
    });
  });

  it('keeps purge/write races safe and rolls back run-owner deletion together', async () => {
    const outcome = await result<{
      purged: number;
      updateChanges: number;
      racedRow: { updatedAt: string } | null;
      rollbackError: string;
      rollbackRow: { run_id: string };
      rollbackOwner: { kind: string; id: string };
      threadRetention: {
        purged: { threads: number; messages: number };
        threads: Array<{ id: string }>;
        messages: Array<{ id: string; thread_id: string }>;
        orphans: Array<{ id: string }>;
      };
    }>(probe, '/retention');
    if (outcome.updateChanges === 1) {
      expect(outcome.purged).toBe(0);
      expect(outcome.racedRow?.updatedAt).toBe('2026-08-10T12:00:00.000Z');
    } else {
      expect(outcome.purged).toBe(1);
      expect(outcome.racedRow).toBeNull();
    }
    expect(outcome.rollbackError).toMatch(/injected snapshot delete failure/);
    expect(outcome.rollbackRow).toEqual({ run_id: 'run-rollback' });
    expect(outcome.rollbackOwner).toEqual({
      kind: 'human',
      id: 'owner-retention',
    });
    expect(outcome.threadRetention).toEqual({
      purged: { threads: 0, messages: 1 },
      threads: [{ id: 'thread-resurrected' }, { id: 'thread-torn' }],
      messages: [
        { id: 'message-history', thread_id: 'thread-resurrected' },
        { id: 'message-just-sent', thread_id: 'thread-torn' },
        { id: 'message-resurrection', thread_id: 'thread-resurrected' },
      ],
      orphans: [],
    });
  });
});
