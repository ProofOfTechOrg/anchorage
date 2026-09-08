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

interface RetentionCursor {
  version: 1;
  tablePrefix: string;
  startIdempotencyTable?: string;
  snapshots?: { afterRowId: number; highWaterRowId: number };
  reservations?: { afterRowId: number; highWaterRowId: number };
}

interface RetentionState {
  snapshots: Array<{
    workflow_name: string;
    run_id: string;
    snapshot: string;
    updatedAt: string;
  }>;
  keys: Array<Record<string, string | number | null>>;
  owners: Array<Record<string, string | number | null>>;
}

interface RetentionMetrics {
  statements: number;
  batches: number;
  maxSqlBytes: number;
  maxBindings: number;
  maxBoundStringBytes: number;
  maxSelectorBytes: number;
  maxResultBytes: number;
  rowsRead: number;
  rowsWritten: number;
  sqlDurationMs: number;
}

interface HeldRetentionResult {
  purged?: number;
  error?: string;
  artifacts: number;
  advances: RetentionCursor[];
  intervened: RetentionState;
  after: RetentionState;
  metrics: RetentionMetrics;
}

const RETENTION_NOW = Date.parse('2026-08-10T12:00:00.000Z');
const RETENTION_SCOPE = {
  version: 1,
  tablePrefix: 'e_retention_',
  startIdempotencyTable: 'e_retention_start_requests',
};

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

  beforeEach(async ({ task }) => {
    if (task.name.startsWith('FS8 E retention')) {
      // Scenario cleanup preserves the surrounding namespace census across requests.
      probe = server.getWorker('flowsafe-harness-probe');
      await result(probe, '/seed');
      await result(probe, '/retention-e/cleanup');
      return;
    }
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

  it.each([
    'generation',
    'capsule',
    'boolean',
    'null',
    'duplicate',
    'legacy',
    'eligibility',
  ])('FS8 E retention preserves a held %s replacement and its owner/key while advancing', async (scenario) => {
    const outcome = await result<HeldRetentionResult>(
      probe,
      `/retention-e/${scenario}`,
    );
    expect(outcome.error).toBeUndefined();
    expect(outcome.purged).toBe(0);
    expect(outcome.artifacts).toBe(1);
    expect(outcome.intervened.snapshots).toHaveLength(1);
    expect(outcome.after).toEqual(outcome.intervened);
    expect(outcome.after.keys[0]).toMatchObject({
      key: 'held-key',
      state: 'started',
      start_token: 'generation-one',
    });
    expect(outcome.advances.at(-1)).toEqual(RETENTION_SCOPE);
  });

  it.each([
    'initial',
    'held',
  ])('FS8 E retention preserves ambiguous eligibility at %s observation and deletes ordinary controls', async (phase) => {
    const outcome = await result<{
      cases: Array<{
        variant: string;
        runId: string;
        snapshot: string;
        sqlEligibility: { status: string; cleanup: number | null };
      }>;
      purged: number;
      artifacts: string[];
      advances: RetentionCursor[];
      before: RetentionState;
      after: RetentionState;
      metrics: RetentionMetrics;
    }>(probe, `/retention-e/duplicate-eligibility/${phase}`);
    expect(outcome.cases.map(({ variant }) => variant)).toEqual([
      'status',
      'escaped-status',
      'request-context',
      'lifecycle',
      'terminal',
      'cleanup',
      'success-control',
      'cleanup-control',
    ]);
    const controls = new Set([
      'e-retention-eligibility-success-control',
      'e-retention-eligibility-cleanup-control',
    ]);
    for (const candidate of outcome.cases) {
      const decoded = JSON.parse(candidate.snapshot);
      expect(candidate.sqlEligibility).toEqual({
        status:
          candidate.variant === 'status' ||
          candidate.variant === 'escaped-status' ||
          candidate.variant === 'success-control'
            ? 'success'
            : 'cancelled',
        cleanup: 1,
      });
      if (!controls.has(candidate.runId)) {
        if (
          candidate.variant === 'status' ||
          candidate.variant === 'escaped-status'
        )
          expect(decoded.status).toBe('running');
        else
          expect(
            decoded.requestContext['flowsafe.runLifecycle'].terminal
              .cleanupCompletedAt,
          ).toBeNull();
      }
      expect(decoded.requestContext['flowsafe.runProvenance'].startToken).toBe(
        'generation-one',
      );
    }
    const replacements = new Map(
      outcome.cases.map(({ runId, snapshot }) => [runId, snapshot]),
    );
    expect(outcome.purged).toBe(controls.size);
    expect(outcome.artifacts.slice().sort()).toEqual(
      (phase === 'held'
        ? outcome.cases.map(({ runId }) => runId)
        : [...controls]
      ).sort(),
    );
    expect(outcome.after).toEqual({
      snapshots: outcome.before.snapshots
        .filter(({ run_id }) => !controls.has(run_id))
        .map((row) => ({
          ...row,
          snapshot: replacements.get(row.run_id),
        })),
      owners: outcome.before.owners.filter(
        ({ resource_id }) => !controls.has(String(resource_id)),
      ),
      keys: outcome.before.keys
        .filter(
          ({ run_id, state }) =>
            !controls.has(String(run_id)) || state === 'started',
        )
        .map((row) =>
          controls.has(String(row.run_id))
            ? { ...row, state: 'terminal', updated_at: RETENTION_NOW }
            : row,
        ),
    });
    expect(outcome.advances.at(-1)).toEqual(RETENTION_SCOPE);
    expect(outcome.metrics.statements).toBeLessThan(100);
    expect(outcome.metrics.maxSqlBytes).toBeLessThanOrEqual(90_000);
    expect(outcome.metrics.maxBindings).toBeLessThanOrEqual(100);
  });

  it.each([
    { format: 'modern', phase: 'initial' },
    { format: 'modern', phase: 'held' },
    { format: 'legacy', phase: 'initial' },
    { format: 'legacy', phase: 'held' },
    { format: 'legacy', phase: 'reread' },
  ])('FS8 E retention validates cleanup timestamps for $format snapshots at $phase observation', async ({
    format,
    phase,
  }) => {
    const outcome = await result<{
      cases: Array<{
        variant: string;
        runId: string;
        snapshot: string;
        accepted: boolean;
        sqlType: string;
      }>;
      purged: number;
      artifacts: string[];
      rawReads: string[];
      advances: RetentionCursor[];
      before: RetentionState;
      after: RetentionState;
      metrics: RetentionMetrics;
    }>(probe, `/retention-e/cleanup-time-${format}/${phase}`);
    expect(outcome.cases.map(({ variant }) => variant)).toEqual([
      'boolean',
      'string',
      'object',
      'negative',
      'fractional',
      'unsafe',
      'infinite',
      'zero-control',
      'real-control',
      'exponent-control',
      'safe-max-control',
    ]);
    expect(
      outcome.cases.map(
        ({ snapshot }) =>
          JSON.parse(snapshot).requestContext['flowsafe.runLifecycle'].terminal
            .cleanupCompletedAt,
      ),
    ).toEqual([
      false,
      'done',
      {},
      -1,
      0.5,
      9007199254740992,
      Number.POSITIVE_INFINITY,
      0,
      1,
      1,
      Number.MAX_SAFE_INTEGER,
    ]);
    expect(outcome.cases.map(({ sqlType }) => sqlType)).toEqual([
      'false',
      'text',
      'object',
      'integer',
      'real',
      'integer',
      'real',
      'integer',
      'real',
      'real',
      'integer',
    ]);
    const controls = new Set([
      'e-retention-time-zero-control',
      'e-retention-time-real-control',
      'e-retention-time-exponent-control',
      'e-retention-time-safe-max-control',
    ]);
    for (const candidate of outcome.cases)
      expect(candidate.accepted).toBe(controls.has(candidate.runId));
    const runIds = outcome.cases.map(({ runId }) => runId);
    expect(outcome.purged).toBe(controls.size);
    expect(outcome.artifacts.slice().sort()).toEqual(
      (phase === 'held' ? runIds.slice() : [...controls]).sort(),
    );
    expect(outcome.rawReads.slice().sort()).toEqual(
      (format === 'modern'
        ? []
        : phase === 'initial'
          ? [...controls]
          : runIds.slice()
      ).sort(),
    );
    const replacements = new Map(
      outcome.cases.map(({ runId, snapshot }) => [runId, snapshot]),
    );
    expect(outcome.after).toEqual({
      snapshots: outcome.before.snapshots
        .filter(({ run_id }) => !controls.has(run_id))
        .map((row) => ({ ...row, snapshot: replacements.get(row.run_id) })),
      owners: outcome.before.owners.filter(
        ({ resource_id }) => !controls.has(String(resource_id)),
      ),
      keys: outcome.before.keys
        .filter(
          ({ run_id, state }) =>
            !controls.has(String(run_id)) || state === 'started',
        )
        .map((row) =>
          format === 'modern' && controls.has(String(row.run_id))
            ? { ...row, state: 'terminal', updated_at: RETENTION_NOW }
            : row,
        ),
    });
    expect(outcome.advances.at(-1)).toEqual(RETENTION_SCOPE);
    expect(outcome.metrics.statements).toBeLessThan(100);
    expect(outcome.metrics.maxSqlBytes).toBeLessThanOrEqual(90_000);
    expect(outcome.metrics.maxBindings).toBeLessThanOrEqual(100);
  });

  it('FS8 E retention retries a changed namespace census and retains a malformed sibling owner', async () => {
    const outcome = await result<HeldRetentionResult>(
      probe,
      '/retention-e/schema',
    );
    expect(outcome.error).toBeUndefined();
    expect(outcome.purged).toBe(1);
    expect(outcome.artifacts).toBe(1);
    expect(outcome.after.snapshots).toEqual([]);
    expect(outcome.after.owners).toEqual(outcome.intervened.owners);
    expect(outcome.after.keys[0]).toMatchObject({
      state: 'terminal',
      updated_at: RETENTION_NOW,
    });
    expect(outcome.advances.at(-1)).toEqual(RETENTION_SCOPE);
    expect(outcome.metrics.batches).toBeGreaterThanOrEqual(2);
    console.info(
      'FS8 E retention schema-retry measurement',
      JSON.stringify(outcome.metrics),
    );
  });

  it('FS8 E retention pairs a reservation table created during the held artifact callback', async () => {
    const outcome = await result<HeldRetentionResult>(
      probe,
      '/retention-e/reservation-schema',
    );
    expect(outcome.error).toBeUndefined();
    expect(outcome.purged).toBe(1);
    expect(outcome.artifacts).toBe(1);
    expect(outcome.intervened.keys[0]?.state).toBe('started');
    expect(outcome.after.keys).toEqual([
      {
        ...outcome.intervened.keys[0],
        state: 'terminal',
        updated_at: RETENTION_NOW,
      },
    ]);
    expect(outcome.after.snapshots).toEqual([]);
    expect(outcome.after.owners).toEqual([]);
    expect(outcome.advances.at(-1)).toEqual(RETENTION_SCOPE);
  });

  it.each([
    'schema-churn',
    'schema-view',
  ])('FS8 E retention refuses %s without false progress or state loss', async (scenario) => {
    const outcome = await result<HeldRetentionResult>(
      probe,
      `/retention-e/${scenario}`,
    );
    expect(outcome.error).toMatch(/schema|namespace|view/i);
    expect(outcome.purged).toBeUndefined();
    expect(outcome.artifacts).toBe(1);
    expect(outcome.advances).toEqual([]);
    expect(outcome.after).toEqual(outcome.intervened);
  });

  it.each([
    'purge-first',
    'admission-first',
  ])('FS8 E retention orders %s against actual reused committed-owner admission', async (order) => {
    const outcome = await result<{
      claimed: boolean;
      reserved: boolean;
      beforeOwner: { owner_id: string; reservation_token: string | null };
      purged: number;
      advances: RetentionCursor[];
      admission: {
        reason?: { code: string; classification: string };
        execution?: { startToken: string };
      };
      definitive: boolean;
      owner: { kind: string; id: string } | null;
      snapshots: Array<{
        workflow_name: string;
        run_id: string;
        snapshot: string;
      }>;
    }>(probe, `/retention-e/${order}`);
    expect(outcome.claimed).toBe(true);
    expect(outcome.reserved).toBe(true);
    expect(outcome.beforeOwner).toEqual({
      owner_id: 'Resource owner',
      reservation_token: null,
    });
    expect(outcome.purged).toBe(1);
    expect(outcome.advances.at(-1)).toEqual({
      version: 1,
      tablePrefix: 'e_retention_',
    });
    if (order === 'purge-first') {
      expect(outcome.admission.reason).toEqual({
        code: 'RUN_ADMISSION_CONFLICT',
        classification: 'run-owner-changed',
      });
      expect(outcome.definitive).toBe(true);
      expect(outcome.owner).toBeNull();
      expect(outcome.snapshots).toEqual([]);
    } else {
      expect(outcome.admission.execution?.startToken).toBe('new-generation');
      expect(outcome.definitive).toBe(false);
      expect(outcome.owner).toEqual({ kind: 'human', id: 'Resource owner' });
      expect(outcome.snapshots).toHaveLength(1);
      expect(outcome.snapshots[0]).toMatchObject({
        workflow_name: 'new-physical-workflow',
        run_id: 'e-retention-reused',
      });
      expect(JSON.parse(outcome.snapshots[0]?.snapshot ?? '{}')).toMatchObject({
        status: 'pending',
        requestContext: {
          'flowsafe.runProvenance': {
            startToken: 'new-generation',
            startIdentity: { owner: { id: 'Initiating principal' } },
          },
        },
      });
    }
  });

  it('FS8 E retention preserves reserved owners and cross-workflow/malformed cross-namespace siblings', async () => {
    const outcome = await result<{
      purged: number;
      owners: Array<Record<string, unknown>>;
      snapshots: Array<Record<string, unknown>>;
      sibling: Array<Record<string, unknown>>;
    }>(probe, '/retention-e/siblings');
    expect(outcome.purged).toBe(4);
    expect(outcome.owners).toEqual([
      {
        resource_id: 'e-retention-cross-workflow',
        owner_id: 'Resource owner',
        reservation_token: null,
      },
      {
        resource_id: 'e-retention-malformed-sibling',
        owner_id: 'Resource owner',
        reservation_token: null,
      },
      {
        resource_id: 'e-retention-reserved',
        owner_id: 'Resource owner',
        reservation_token: 'held-reservation',
      },
    ]);
    expect(outcome.snapshots).toEqual([
      { workflow_name: 'other-workflow', run_id: 'e-retention-cross-workflow' },
    ]);
    expect(outcome.sibling).toEqual([
      { run_id: 'e-retention-malformed-sibling', snapshot: '{not-json' },
    ]);
  });

  it('FS8 E retention pairs full bound aliases while preserving mismatches, legacy, unbound and null namespaces', async () => {
    const outcome = await result<{
      purged: number;
      advances: RetentionCursor[];
      before: RetentionState;
      after: RetentionState;
    }>(probe, '/retention-e/bindings');
    expect(outcome.purged).toBe(1);
    expect(outcome.after.snapshots).toEqual([]);
    expect(outcome.after.owners).toEqual([]);
    expect(outcome.after.keys).toEqual(
      outcome.before.keys
        .filter(({ key }) => key !== 'alias-expired')
        .map((row) =>
          row.key === 'alias-started'
            ? { ...row, state: 'terminal', updated_at: RETENTION_NOW }
            : row,
        ),
    );
    expect(outcome.advances.at(-1)).toEqual(RETENTION_SCOPE);
  });

  it.each([
    0, 1, 2,
  ])('FS8 E retention handles stage %i legacy orphans conservatively', async (stage) => {
    const outcome = await result<{
      purged: number;
      advances: RetentionCursor[];
      before: RetentionState['keys'];
      keys: RetentionState['keys'];
    }>(probe, `/retention-e/partial-${stage}`);
    expect(outcome.purged).toBe(0);
    expect(outcome.keys).toEqual(
      outcome.before.filter(({ key }) => key !== 'legacy-orphan'),
    );
    expect(outcome.keys.some(({ key }) => key === 'legacy-present')).toBe(true);
    if (stage > 0)
      expect(outcome.keys.some(({ key }) => key === 'partial-nonnull')).toBe(
        true,
      );
    expect(outcome.advances.at(-1)).toEqual(RETENTION_SCOPE);
  });

  it('FS8 E retention expires an orphan without a configured snapshot table', async () => {
    const outcome = await result<{
      purged: number;
      advances: RetentionCursor[];
      keys: unknown[];
    }>(probe, '/retention-e/orphan-absent');
    expect(outcome.purged).toBe(0);
    expect(outcome.keys).toEqual([]);
    expect(outcome.advances.at(-1)).toEqual(RETENTION_SCOPE);
  });

  it('FS8 E retention rechecks held orphan generation observations before expiry', async () => {
    const outcome = await result<{
      first: number;
      second: number;
      held: number;
      heldKeys: RetentionState['keys'];
      keys: unknown[];
      snapshot: { snapshot: string };
    }>(probe, '/retention-e/orphan-replacement');
    expect(outcome.first).toBe(0);
    expect(outcome.held).toBe(1);
    expect(outcome.heldKeys).toHaveLength(1);
    expect(outcome.heldKeys[0]).toMatchObject({
      key: 'orphan-key',
      state: 'terminal',
      start_token: 'generation-one',
      updated_at: RETENTION_NOW - 8 * 86400_000,
    });
    expect(outcome.second).toBe(0);
    expect(outcome.keys).toEqual([]);
    expect(JSON.parse(outcome.snapshot.snapshot)).toMatchObject({
      status: 'running',
      requestContext: {
        'flowsafe.runProvenance': { startToken: 'generation-two' },
      },
    });
  });

  it('FS8 E retention enforces capsule bytes and UTF16 units while accepting long safe registry names', async () => {
    const outcome = await result<{
      purged: number;
      advances: RetentionCursor[];
      artifacts: string[];
      registryLength: number;
      remainingKeyLengths: Array<{ length: number }>;
      remaining: Array<{ run_id: string }>;
    }>(probe, '/retention-e/limits');
    expect(outcome.registryLength).toBeGreaterThan(63);
    expect(outcome.remainingKeyLengths).toEqual([{ length: 4096 }]);
    expect(outcome.purged).toBe(2);
    expect(outcome.artifacts.sort()).toEqual([
      'e-retention-capsule-boundary',
      'e-retention-unpaired-legacy',
    ]);
    expect(outcome.remaining).toEqual([
      { run_id: 'e-retention-capsule-overflow' },
      { run_id: 'e-retention-utf16-overflow' },
    ]);
    expect(outcome.advances.at(-1)?.snapshots).toBeUndefined();
    expect(outcome.advances.at(-1)?.reservations).toBeUndefined();
  });

  it('FS8 E retention reconstructs finite progress beyond oversize and artifact failures despite new inserts', async () => {
    const outcome = await result<{
      pages: Array<{
        purged?: number;
        error?: string;
        cursor: RetentionCursor;
      }>;
      artifactFailures: number;
      afterFirstCycle: Array<{ run_id: string }>;
      remainingEligible: unknown[];
    }>(probe, '/retention-e/progress');
    expect(outcome.pages.map(({ purged }) => purged)).toEqual([
      0,
      undefined,
      0,
      2,
    ]);
    expect(outcome.pages.map(({ error }) => error)).toEqual([
      undefined,
      expect.stringMatching(/artifact deletion failed \(unreadable error\)/),
      undefined,
      undefined,
    ]);
    expect(outcome.pages[0]?.cursor.snapshots).toEqual({
      afterRowId: 90,
      highWaterRowId: 93,
    });
    expect(outcome.pages[1]?.cursor.snapshots).toBeUndefined();
    expect(outcome.pages[2]?.cursor.snapshots).toEqual({
      afterRowId: 90,
      highWaterRowId: 94,
    });
    expect(outcome.pages[3]?.cursor.snapshots).toBeUndefined();
    expect(outcome.artifactFailures).toBe(1);
    expect(outcome.afterFirstCycle).toEqual([
      { run_id: 'e-retention-progress-91' },
      { run_id: 'e-retention-progress-new' },
    ]);
    expect(outcome.remainingEligible).toEqual([]);
  });

  it.each([
    { mode: 'modern', retry: false },
    { mode: 'legacy', retry: false },
    { mode: 'modern', retry: true },
  ])('FS8 E retention measures real D1 maximum namespaces and $mode pages (retry $retry) with large payloads', async ({
    mode,
    retry,
  }) => {
    try {
      const setup = await result<{
        namespaces: number;
        preexistingNamespaces: number;
        createdNamespaces: number;
        rows: number;
        maxSnapshotBytes: number;
        capsuleBytes: number | null;
        ownerUtf16Units: number | null;
      }>(probe, `/retention-e/maximum-${mode}/setup`);
      expect(setup.namespaces).toBe(64);
      expect(setup.preexistingNamespaces + setup.createdNamespaces).toBe(64);
      expect(setup.rows).toBe(90);
      expect(setup.maxSnapshotBytes).toBeGreaterThan(1_900_000);
      if (mode === 'modern') {
        expect(setup.capsuleBytes).toBe(4096);
        expect(setup.ownerUtf16Units).toBe(200);
        const overflow = await result<{
          error?: string;
          artifacts: number;
          advances: RetentionCursor[];
          before: unknown;
          after: unknown;
        }>(probe, '/retention-e/maximum-modern/overflow');
        expect(overflow.error).toMatch(/namespace|64/i);
        expect(overflow.artifacts).toBe(0);
        expect(overflow.advances).toEqual([]);
        expect(overflow.after).toEqual(overflow.before);
      }
      const outcome = await result<{
        purged?: number;
        error?: string;
        artifacts: number;
        advances: RetentionCursor[];
        metrics: RetentionMetrics;
        elapsedMs: number;
        snapshots: { count: number };
        census: { count: number };
        keys: Array<{ key: string; state: string; updated_at: number }>;
        owners: Array<{ resource_id: string; owner_id: string }>;
      }>(
        probe,
        `/retention-e/maximum-${mode}/${retry ? 'exercise-retry' : 'exercise'}`,
      );
      console.info(
        `FS8 E retention maximum-${mode}${retry ? '-retry' : ''} measurement`,
        JSON.stringify({
          setup,
          metrics: outcome.metrics,
          elapsedMs: outcome.elapsedMs,
        }),
      );
      expect(outcome.error).toBeUndefined();
      expect(outcome.artifacts).toBe(retry ? 90 : 0);
      expect(outcome.purged).toBe(90);
      expect(outcome.snapshots.count).toBe(0);
      expect(outcome.census.count).toBe(64);
      expect(outcome.owners).toEqual([
        {
          resource_id: 'e-retention-089'.padEnd(200, 'r'),
          owner_id: 'Resource owner',
        },
      ]);
      expect(outcome.keys).toEqual(
        mode === 'modern'
          ? Array.from({ length: 90 }, (_, index) => ({
              key: `alias-${String(index).padStart(3, '0')}`,
              state: 'terminal',
              updated_at: RETENTION_NOW,
            }))
          : [],
      );
      expect(outcome.advances.at(-1)?.snapshots).toBeUndefined();
      expect(outcome.advances.at(-1)?.reservations).toEqual(
        mode === 'modern' ? { afterRowId: 90, highWaterRowId: 180 } : undefined,
      );
      for (const value of Object.values(outcome.metrics))
        expect(Number.isFinite(value)).toBe(true);
      expect(outcome.metrics.statements).toBeGreaterThan(90);
      expect(outcome.metrics.statements).toBeLessThanOrEqual(1000);
      expect(outcome.metrics.maxSqlBytes).toBeLessThanOrEqual(90_000);
      expect(outcome.metrics.maxBindings).toBeLessThanOrEqual(100);
      expect(outcome.metrics.maxSelectorBytes).toBeGreaterThan(0);
      expect(outcome.metrics.maxSelectorBytes).toBeLessThanOrEqual(1_000_000);
      expect(outcome.metrics.maxBoundStringBytes).toBeLessThanOrEqual(
        2_000_000,
      );
      if (mode === 'modern') {
        expect(outcome.metrics.maxResultBytes).toBeLessThan(
          setup.maxSnapshotBytes,
        );
        expect(outcome.metrics.maxSelectorBytes).toBeGreaterThan(90 * 4096);
      } else {
        expect(outcome.metrics.maxResultBytes).toBeGreaterThan(
          setup.maxSnapshotBytes,
        );
        expect(outcome.metrics.maxResultBytes).toBeLessThan(
          setup.maxSnapshotBytes + 5000,
        );
      }
    } finally {
      expect(await result(probe, '/retention-e/cleanup')).toEqual({
        remaining: [],
      });
    }
  });
});
