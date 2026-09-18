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
  const p3 = path.startsWith('/epoch-p3/');
  if (p3) p3Isolation.reusable = false;
  const response = await worker.fetch(path, { method: 'POST' });
  const body = await response.text();
  expect(response.status, body).toBe(200);
  const parsed = JSON.parse(body);
  if (p3) {
    expect(parsed.quiescent, path).toBe(true);
    if (path.startsWith('/epoch-p3/run'))
      expect(parsed.checkedRun).toEqual({
        workflowId: 'workflow-schedule',
        runId: 'p3-run',
      });
    recordP3Measurements(path, parsed.metrics);
    p3Isolation.reusable = true;
  }
  return parsed as T;
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

interface P3Response {
  status: number;
  body: {
    reason?: { code: string; classification?: string; mutationEpoch?: number };
    schedule?: { id: string; status: string };
    result?: { value: string };
  };
}

interface P3Metrics {
  statements: number;
  batches: number;
  maxBatchStatements: number;
  maxSqlBytes: number;
  maxBindings: number;
  maxBoundStringBytes: number;
  maxResultBytes: number;
  rowsRead: number;
  rowsWritten: number;
}

const p3Measurements = new Map<
  string,
  {
    samples: number;
    minStatements: number;
    maxStatementScenario: string;
    maxima: P3Metrics;
  }
>();

const p3Isolation = { entered: false, reusable: false, resets: 0, cleanups: 0 };

function recordP3Measurements(
  path: string,
  value: P3Metrics | Record<string, P3Metrics>,
) {
  const stages =
    typeof value.statements === 'number'
      ? { cas: value as P3Metrics }
      : (value as Record<string, P3Metrics>);
  for (const [stage, metrics] of Object.entries(stages)) {
    const previous = p3Measurements.get(stage);
    if (!previous) {
      p3Measurements.set(stage, {
        samples: 1,
        minStatements: metrics.statements,
        maxStatementScenario: path,
        maxima: { ...metrics },
      });
      continue;
    }
    previous.samples++;
    previous.minStatements = Math.min(
      previous.minStatements,
      metrics.statements,
    );
    if (metrics.statements > previous.maxima.statements)
      previous.maxStatementScenario = path;
    for (const key of Object.keys(metrics) as Array<keyof P3Metrics>)
      previous.maxima[key] = Math.max(previous.maxima[key], metrics[key]);
  }
}

interface P3Fence {
  state: string;
  mutationEpoch: number;
  requireMutationEpoch: boolean;
  transitionRevision: number;
}

interface P3ScheduleState {
  schedules: Array<Record<string, unknown>>;
  triggers: Array<Record<string, unknown>>;
  owners: Array<Record<string, unknown>>;
}

interface P3ScheduleResult {
  operation: string;
  response: P3Response;
  gateHits: number;
  finalBatches: number;
  before: P3ScheduleState;
  intervened: P3ScheduleState;
  after: P3ScheduleState;
  reads?: { list: P3Response; get: P3Response; history: P3Response };
  positive?: P3Response;
  positiveState?: P3ScheduleState;
  settled?: P3ScheduleState;
  audit: Array<{ actorId: string; operation: string; outcome: string }>;
  fenceBeforeRelease: P3Fence;
  fenceAfter: P3Fence;
  bodyBytes: number;
  metrics: Record<string, P3Metrics>;
}

interface P3RunState {
  snapshots: Array<Record<string, unknown>>;
  keys: Array<Record<string, unknown>>;
  owners: Array<Record<string, unknown>>;
}

interface P3RunResult {
  response: P3Response;
  capturedEpoch: number | null;
  gateHits: number;
  initialBatches: number;
  before: P3RunState;
  intervened: P3RunState;
  after: P3RunState;
  positiveRows: P3RunState;
  effectsAfterRequest: number;
  effects: number;
  cachedAfterRequest: boolean;
  activeAfterRequest: boolean;
  positive?: { status: string; result: { value: string } };
  fenceBeforeRelease: P3Fence;
  fenceAfter: P3Fence;
  metrics: Record<string, P3Metrics>;
}

interface P3StructuralFence {
  rows: Array<Record<string, unknown>>;
  schema: Array<Record<string, unknown>>;
}

interface P3StructuralRunResult extends Omit<P3RunResult, 'fenceAfter'> {
  fenceAfter?: P3Fence;
  structural: {
    fixture: string;
    proof: boolean;
    responseLosses: number;
    initialBatchRows: number[];
    before: P3StructuralFence;
    intervened: P3StructuralFence;
    after: P3StructuralFence;
  };
}

function expectP3StructuralRefusal(
  outcome: P3StructuralRunResult,
  keyed: boolean,
  lostResponse = false,
) {
  expect(outcome.response.status, JSON.stringify(outcome.response.body)).toBe(
    503,
  );
  expect(outcome.response.body.reason).toEqual({
    code: 'EXECUTION_FENCE_UNREADABLE',
  });
  expect(outcome.gateHits).toBe(1);
  expect(outcome.initialBatches).toBe(1);
  expect(outcome.capturedEpoch).toBe(2);
  expect(outcome.effectsAfterRequest).toBe(0);
  expect(outcome.effects).toBe(0);
  expect(outcome.activeAfterRequest).toBe(false);
  expect(outcome.after.snapshots).toEqual([]);
  expect(outcome.after.owners).toEqual(outcome.before.owners);
  expect(JSON.stringify(outcome.structural.after)).toBe(
    JSON.stringify(outcome.structural.intervened),
  );
  expect(outcome.structural.initialBatchRows).toEqual(
    keyed ? [0, 0, 0] : [0, 0],
  );
  if (!lostResponse) expect(outcome.cachedAfterRequest).toBe(false);
  if (keyed) {
    expect(
      outcome.after.keys.find((row) => row.key === 'p3-key'),
    ).toMatchObject({
      owner_id: 'p3-owner',
      target_id: 'workflow-schedule',
      run_id: 'p3-run',
      state: lostResponse ? 'started' : 'reserved',
      start_token: '',
      start_table_prefix: null,
      start_workflow_id: null,
    });
    expect(outcome.after.keys.filter((row) => row.key !== 'p3-key')).toEqual(
      outcome.before.keys,
    );
  } else expect(outcome.after).toEqual(outcome.before);
  if (lostResponse) expect(outcome.after).toEqual(outcome.intervened);
  expectP3Metrics(outcome.metrics);
}

const P3_OPERATIONS = [
  'create',
  'update',
  'pause',
  'resume',
  'delete',
  'pause-noop',
  'resume-noop',
] as const;

function expectP3Metrics(metrics: Record<string, P3Metrics>) {
  for (const measured of Object.values(metrics)) {
    expect(measured.statements).toBeLessThanOrEqual(1000);
    expect(measured.maxSqlBytes).toBeLessThanOrEqual(90_000);
    expect(measured.maxBindings).toBeLessThanOrEqual(100);
    expect(measured.maxBoundStringBytes).toBeLessThanOrEqual(2_000_000);
  }
}

function expectP3EpochRefusal(
  response: P3Response,
  classification: string,
  epoch: number,
) {
  expect(response.status, JSON.stringify(response.body)).toBe(409);
  expect(response.body.reason).toEqual({
    code: 'MUTATION_EPOCH_MISMATCH',
    classification,
    mutationEpoch: epoch,
  });
}

function expectP3Preserved(scheduleResult: P3ScheduleResult) {
  expect(JSON.stringify(scheduleResult.after)).toBe(
    JSON.stringify(scheduleResult.intervened),
  );
  expect(scheduleResult.fenceAfter).toEqual(scheduleResult.fenceBeforeRelease);
  expectP3Metrics(scheduleResult.metrics);
}

function expectP3RunRefused(runResult: P3RunResult) {
  expect(JSON.stringify(runResult.after)).toBe(
    JSON.stringify(runResult.before),
  );
  expect(runResult.effectsAfterRequest).toBe(0);
  expect(runResult.cachedAfterRequest).toBe(false);
  expect(runResult.activeAfterRequest).toBe(false);
  expect(runResult.after.snapshots).toEqual([]);
  expect(runResult.positive).toMatchObject({
    status: 'success',
    result: { value: 'positive' },
  });
  expect(runResult.effects).toBe(1);
  expect(runResult.positiveRows.snapshots).toHaveLength(1);
  expect(runResult.fenceAfter).toEqual(runResult.fenceBeforeRelease);
  expectP3Metrics(runResult.metrics);
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

  async function prepareP3() {
    const reusable = p3Isolation.entered && p3Isolation.reusable;
    p3Isolation.reusable = false;
    if (reusable) {
      probe = server.getWorker('flowsafe-harness-probe');
      expect(await result(probe, '/p3-cleanup')).toEqual({ remaining: [] });
      p3Isolation.cleanups++;
    } else {
      await server.reset();
      p3Isolation.resets++;
    }
    p3Isolation.entered = true;
    probe = server.getWorker('flowsafe-harness-probe');
    await result(probe, '/seed');
  }

  beforeEach(async ({ task }) => {
    if (task.name.startsWith('FS8 P3')) {
      await prepareP3();
      return;
    }
    const leavingP3 = p3Isolation.entered;
    p3Isolation.entered = false;
    p3Isolation.reusable = false;
    if (task.name.startsWith('FS8 E retention')) {
      if (leavingP3) {
        await server.reset();
        p3Isolation.resets++;
      }
      // Scenario cleanup preserves the surrounding namespace census across requests.
      probe = server.getWorker('flowsafe-harness-probe');
      await result(probe, '/seed');
      await result(probe, '/retention-e/cleanup');
      return;
    }
    await server.reset();
    if (leavingP3) p3Isolation.resets++;
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
    try {
      if (p3Measurements.size)
        console.info(
          'FS8 P3 D1 metrics',
          JSON.stringify(Object.fromEntries(p3Measurements)),
        );
      if (p3Isolation.resets)
        console.info('FS8 P3 fixture isolation', JSON.stringify(p3Isolation));
    } finally {
      await server.close();
    }
  });

  it.each([
    'isolation-missing-witness',
    'unmatched',
  ])('FS8 P3 resets the fixture after %s and reuses a settled request', async (scenario) => {
    await expect(result(probe, `/epoch-p3/${scenario}`)).rejects.toThrow();
    expect(p3Isolation.reusable).toBe(false);
    const resets = p3Isolation.resets;
    await prepareP3();
    expect(p3Isolation.resets).toBe(resets + 1);
    const first = await result<{ before: P3Fence }>(
      probe,
      '/epoch-p3/cas-loss',
    );
    expect(first.before).toMatchObject({
      state: 'open',
      mutationEpoch: 0,
      transitionRevision: 0,
    });
    const cleanups = p3Isolation.cleanups;
    await prepareP3();
    expect(p3Isolation.cleanups).toBe(cleanups + 1);
    const second = await result<{ before: P3Fence }>(
      probe,
      '/epoch-p3/cas-loss',
    );
    expect(second.before).toEqual(first.before);
  });

  it.each(
    P3_OPERATIONS.flatMap((operation) =>
      ['auth', 'final'].flatMap((phase) =>
        ['missing', 'stale'].map((epoch) => ({ operation, phase, epoch })),
      ),
    ),
  )('FS8 P3 holds $operation at $phase across activation with $epoch epoch', async ({
    operation,
    phase,
    epoch,
  }) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=${operation}&phase=${phase}&epoch=${epoch}&action=activate`,
    );
    expectP3EpochRefusal(outcome.response, epoch, 1);
    expect(outcome.gateHits).toBe(1);
    expect(outcome.finalBatches).toBeGreaterThanOrEqual(
      phase === 'final' ? 1 : 0,
    );
    expect(outcome.fenceAfter).toMatchObject({
      state: 'open',
      mutationEpoch: 1,
      requireMutationEpoch: true,
    });
    expect(outcome.audit).toMatchObject([
      { actorId: 'p3-owner', outcome: 'rejected' },
    ]);
    expectP3Preserved(outcome);
  });

  it.each(
    P3_OPERATIONS.flatMap((operation) =>
      ['missing', 'stale', 'future', 'current'].map((epoch) => ({
        operation,
        epoch,
      })),
    ),
  )('FS8 P3 enforces activated $epoch epoch on HTTP $operation', async ({
    operation,
    epoch,
  }) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=${operation}&epoch=${epoch}`,
    );
    if (epoch !== 'current') {
      expectP3EpochRefusal(outcome.response, epoch, 2);
      expectP3Preserved(outcome);
    } else {
      expect(
        outcome.response.status,
        JSON.stringify(outcome.response.body),
      ).toBe(operation === 'create' ? 201 : 200);
      expect(outcome.audit).toMatchObject([
        { actorId: 'p3-owner', outcome: 'accepted' },
      ]);
      if (operation.endsWith('-noop')) expectP3Preserved(outcome);
      else
        expect(JSON.stringify(outcome.after)).not.toBe(
          JSON.stringify(outcome.before),
        );
      if (operation === 'create') {
        expect(outcome.after.schedules).toHaveLength(2);
        expect(outcome.after.owners).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              resource_id: outcome.response.body.schedule?.id,
              owner_id: 'p3-owner',
            }),
          ]),
        );
      }
      if (operation === 'delete')
        expect(outcome.after).toEqual({
          schedules: [],
          triggers: [],
          owners: [],
        });
    }
    expectP3Metrics(outcome.metrics);
  });

  it.each(
    P3_OPERATIONS,
  )('FS8 P3 preserves draining policy for exact HTTP %s', async (operation) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=${operation}&closed=true`,
    );
    expect(outcome.reads?.list.status).toBe(200);
    expect(outcome.reads?.get.status).toBe(operation === 'delete' ? 404 : 200);
    expect(outcome.reads?.history.status).toBe(
      operation === 'delete' ? 404 : 200,
    );
    if (
      operation === 'pause' ||
      operation === 'pause-noop' ||
      operation === 'delete'
    ) {
      expect(
        outcome.response.status,
        JSON.stringify(outcome.response.body),
      ).toBe(200);
    } else {
      expect(outcome.response.status).toBe(503);
      expect(outcome.response.body.reason?.code).toBe('EXECUTION_FENCED');
      expectP3Preserved(outcome);
    }
  });

  it.each(
    P3_OPERATIONS,
  )('FS8 P3 admits exact %s through the final gate without a transition', async (operation) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=${operation}&phase=final`,
    );
    expect(outcome.gateHits).toBe(1);
    expect(outcome.finalBatches).toBe(1);
    expect(outcome.response.status, JSON.stringify(outcome.response.body)).toBe(
      operation === 'create' ? 201 : 200,
    );
    if (operation.endsWith('-noop')) expectP3Preserved(outcome);
    expectP3Metrics(outcome.metrics);
  });

  it.each(
    P3_OPERATIONS,
  )('FS8 P3 refuses exact %s after a same-epoch final-frame cycle', async (operation) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=${operation}&phase=final&action=cycle`,
    );
    expect(outcome.gateHits).toBe(1);
    expect(outcome.response.status).toBe(409);
    expect(outcome.response.body.reason).toEqual({
      code: 'SCHEDULE_MUTATION_CONFLICT',
      classification: 'fence-changed',
    });
    expectP3Preserved(outcome);
  });

  it.each(
    P3_OPERATIONS,
  )('FS8 P3 refuses formerly exact %s after the next artifact activates', async (operation) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=${operation}&phase=final&action=advance`,
    );
    expectP3EpochRefusal(outcome.response, 'stale', 3);
    expect(outcome.gateHits).toBe(1);
    expectP3Preserved(outcome);
  });

  it.each([
    'create',
    'pause',
    'resume-noop',
  ])('FS8 P3 keeps the original missing actor epoch while %s waits', async (operation) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=${operation}&phase=auth&epoch=missing&action=capture`,
    );
    expectP3EpochRefusal(outcome.response, 'missing', 2);
    expect(outcome.gateHits).toBe(1);
    expect(outcome.audit).toMatchObject([
      { actorId: 'p3-owner', outcome: 'rejected' },
    ]);
    expectP3Preserved(outcome);
  });

  it('FS8 P3 keeps a captured current actor while the resolver result changes', async () => {
    const captured = await result<P3ScheduleResult>(
      probe,
      '/epoch-p3/schedule?operation=create&phase=auth&action=capture',
    );
    expect(
      captured.response.status,
      JSON.stringify(captured.response.body),
    ).toBe(201);
    expect(captured.audit).toMatchObject([
      { actorId: 'p3-owner', outcome: 'accepted' },
    ]);
    expect(captured.after.owners).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          resource_id: captured.response.body.schedule?.id,
          owner_id: 'p3-owner',
        }),
      ]),
    );
  });

  it.each(
    P3_OPERATIONS,
  )('FS8 P3 refuses an invalid trusted epoch on %s', async (operation) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=${operation}&epoch=invalid`,
    );
    expect(outcome.response.status).toBe(400);
    expect(outcome.response.body.reason?.code).toBe('INVALID_MUTATION_EPOCH');
    expectP3Preserved(outcome);
  });

  it.each([
    'body',
    'header',
    'stored',
  ])('FS8 P3 does not obtain schedule authority from %s input', async (injection) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=create&epoch=missing&injection=${injection}`,
    );
    if (injection === 'stored')
      expectP3EpochRefusal(outcome.response, 'missing', 2);
    else
      expect(outcome.response.status).toBe(injection === 'header' ? 403 : 400);
    expectP3Preserved(outcome);
  });

  it.each([
    16384, 16385,
  ])('FS8 P3 applies the HTTP UTF-8 body bound at %i bytes', async (bytes) => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      `/epoch-p3/schedule?operation=create&bytes=${bytes}`,
    );
    expect(outcome.bodyBytes).toBe(bytes);
    expect(outcome.response.status, JSON.stringify(outcome.response.body)).toBe(
      bytes === 16384 ? 201 : 413,
    );
    if (bytes > 16384) {
      expect(outcome.finalBatches).toBe(0);
      expectP3Preserved(outcome);
    }
    expectP3Metrics(outcome.metrics);
  });

  it('FS8 P3 does not grant draining authoring through a paused PATCH', async () => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      '/epoch-p3/schedule?operation=update&closed=true&patchPaused=true',
    );
    expect(outcome.response.status).toBe(503);
    expect(outcome.response.body.reason?.code).toBe('EXECUTION_FENCED');
    expectP3Preserved(outcome);
  });

  it('FS8 P3 distinguishes a current cap refusal from epoch refusal', async () => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      '/epoch-p3/schedule?operation=create&cap=true',
    );
    expect(outcome.response.status).toBe(400);
    expect(outcome.audit).toMatchObject([{ outcome: 'rejected' }]);
    expectP3Preserved(outcome);
  });

  it('FS8 P3 rejects a held resume after its cron configuration changes', async () => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      '/epoch-p3/schedule?operation=resume&phase=final&action=resume-race',
    );
    expect(outcome.response.status).toBe(409);
    expect(outcome.response.body.reason).toEqual({
      code: 'SCHEDULE_MUTATION_CONFLICT',
      classification: 'schedule-changed',
    });
    expect(outcome.intervened.schedules[0]).toMatchObject({
      cron: '*/5 * * * *',
      timezone: 'UTC',
      status: 'paused',
    });
    expectP3Preserved(outcome);
    expect(outcome.positive?.status).toBe(200);
    expect(outcome.positiveState?.schedules[0]).toMatchObject({
      cron: '*/5 * * * *',
      timezone: 'UTC',
      status: 'active',
    });
  });

  it('FS8 P3 preserves a held deferred deletion and its raw history across activation', async () => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      '/epoch-p3/schedule?operation=delete&phase=final&epoch=missing&action=activate&deferred=true',
    );
    expectP3EpochRefusal(outcome.response, 'missing', 1);
    expect(outcome.after.schedules[0]?.deletionRequestedAt).toBeNull();
    expect(outcome.after.triggers[0]?.outcome).toBe('deferred');
    expect(outcome.after.owners).toHaveLength(1);
    expectP3Preserved(outcome);
  });

  it('FS8 P3 settles an admitted deferred deletion after another epoch closes', async () => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      '/epoch-p3/schedule?operation=delete&closed=true&deferred=true',
    );
    expect(outcome.response.status).toBe(202);
    expect(outcome.after.schedules[0]).toMatchObject({
      status: 'paused',
      deletionRequestedAt: expect.any(Number),
    });
    expect(outcome.after.owners).toHaveLength(1);
    expect(outcome.settled).toEqual({
      schedules: [],
      triggers: [],
      owners: [],
    });
    expect(outcome.fenceAfter).toMatchObject({
      state: 'draining',
      mutationEpoch: 3,
      requireMutationEpoch: true,
    });
    expectP3Metrics(outcome.metrics);
  });

  it('FS8 P3 deletes populated trigger history with a bounded mutation result', async () => {
    const outcome = await result<P3ScheduleResult>(
      probe,
      '/epoch-p3/schedule?operation=delete&history=true',
    );
    expect(outcome.before.triggers).toHaveLength(120);
    expect(outcome.response.status).toBe(200);
    expect(outcome.after).toEqual({ schedules: [], triggers: [], owners: [] });
    expect(outcome.metrics.mutation?.maxResultBytes).toBeLessThan(10_000);
    expectP3Metrics(outcome.metrics);
  });

  it.each(
    ['auth', 'final'].flatMap((phase) =>
      ['missing', 'stale'].map((epoch) => ({ phase, epoch })),
    ),
  )('FS8 P3 holds real Runtime at $phase across activation with $epoch epoch', async ({
    phase,
    epoch,
  }) => {
    const outcome = await result<P3RunResult>(
      probe,
      `/epoch-p3/run?phase=${phase}&epoch=${epoch}&action=activate`,
    );
    expectP3EpochRefusal(outcome.response, epoch, 1);
    expect(outcome.gateHits).toBe(1);
    expect(outcome.initialBatches).toBe(phase === 'final' ? 1 : 0);
    expect(outcome.capturedEpoch).toBe(epoch === 'missing' ? null : 0);
    expectP3RunRefused(outcome);
  });

  it.each([
    'missing',
    'stale',
    'future',
    'current',
    'invalid',
  ])('FS8 P3 checks %s epoch through real Runtime and D1', async (epoch) => {
    const outcome = await result<P3RunResult>(
      probe,
      `/epoch-p3/run?epoch=${epoch}`,
    );
    if (epoch === 'current') {
      expect(
        outcome.response.status,
        JSON.stringify(outcome.response.body),
      ).toBe(200);
      expect(outcome.response.body.result).toEqual({ value: 'original' });
      expect(outcome.effectsAfterRequest).toBe(1);
      expect(outcome.after.snapshots).toHaveLength(1);
    } else if (epoch === 'invalid') {
      expect(outcome.response.status).toBe(400);
      expect(outcome.response.body.reason?.code).toBe('INVALID_MUTATION_EPOCH');
      expect(outcome.effects).toBe(0);
      expect(outcome.after).toEqual(outcome.before);
    } else {
      expectP3EpochRefusal(outcome.response, epoch, 2);
      expectP3RunRefused(outcome);
    }
    expectP3Metrics(outcome.metrics);
  });

  it.each([
    'auth',
    'final',
    'provider',
  ])('FS8 P3 executes the current Runtime through its %s gate', async (phase) => {
    const outcome = await result<P3RunResult>(
      probe,
      `/epoch-p3/run?phase=${phase}`,
    );
    expect(outcome.response.status, JSON.stringify(outcome.response.body)).toBe(
      200,
    );
    expect(outcome.gateHits).toBe(1);
    expect(outcome.initialBatches).toBe(1);
    expect(outcome.effectsAfterRequest).toBe(1);
    expect(outcome.response.body.result).toEqual({ value: 'original' });
    expectP3Metrics(outcome.metrics);
  });

  it('FS8 P3 prevents engine entry after a same-epoch final-frame cycle', async () => {
    const outcome = await result<P3RunResult>(
      probe,
      '/epoch-p3/run?phase=final&action=cycle',
    );
    expect(outcome.response.status).toBe(409);
    expect(outcome.response.body.reason).toEqual({
      code: 'RUN_ADMISSION_CONFLICT',
      classification: 'fence-changed',
    });
    expectP3RunRefused(outcome);
  });

  it.each(
    ['schema-extension', 'null-singleton'].flatMap((structure) =>
      [false, true].map((proof) => ({ structure, proof })),
    ),
  )('FS8 P3 structural Runtime refuses $structure at final D1 with proof $proof', async ({
    structure,
    proof,
  }) => {
    const outcome = await result<P3StructuralRunResult>(
      probe,
      `/epoch-p3/run?phase=final&structure=${structure}&proof=${proof}`,
    );
    expectP3StructuralRefusal(outcome, proof);
    const prior = outcome.structural.before;
    const changed = outcome.structural.intervened;
    if (structure === 'schema-extension') {
      expect(changed.schema).toHaveLength(prior.schema.length + 1);
      expect(changed.schema.at(-1)).toMatchObject({
        name: 'p3_shape',
        type: 'TEXT',
      });
      expect(changed.rows).toHaveLength(1);
    } else {
      expect(changed.schema).toEqual(prior.schema);
      expect(
        changed.schema.find((column) => column.name === 'id'),
      ).toMatchObject({ type: 'TEXT', notnull: 0, pk: 1 });
      expect(changed.rows).toHaveLength(2);
      expect(changed.rows.filter((row) => row.id === null)).toHaveLength(1);
    }
    const deployment = changed.rows.find((row) => row.id === 'deployment');
    expect(deployment).toMatchObject({
      mutation_epoch: 2,
      require_mutation_epoch: 1,
      proof_run_id: null,
      proof_start_token: null,
    });
    expect(outcome.structural.responseLosses).toBe(0);
  });

  it('FS8 P3 structural Runtime keeps refused batch participants unchanged after response loss', async () => {
    const outcome = await result<P3StructuralRunResult>(
      probe,
      '/epoch-p3/run?phase=final&structure=schema-extension&keyed=true&loss=true',
    );
    expectP3StructuralRefusal(outcome, true, true);
    expect(outcome.structural.responseLosses).toBe(1);
  });

  it.each([
    { proof: false, loss: false, afterAdmission: 'none' },
    { proof: true, loss: false, afterAdmission: 'none' },
    { proof: false, loss: true, afterAdmission: 'advance' },
    { proof: true, loss: true, afterAdmission: 'none' },
    { proof: true, loss: false, afterAdmission: 'advance' },
  ])('FS8 P3 structural Runtime admits valid current proof $proof loss $loss after $afterAdmission', async ({
    proof,
    loss,
    afterAdmission,
  }) => {
    const outcome = await result<P3StructuralRunResult>(
      probe,
      `/epoch-p3/run?phase=final&structure=none&proof=${proof}&loss=${loss}&afterAdmission=${afterAdmission}`,
    );
    expect(outcome.response.status, JSON.stringify(outcome.response.body)).toBe(
      200,
    );
    expect(outcome.response.body.result).toEqual({ value: 'original' });
    expect(outcome.gateHits).toBe(1);
    expect(outcome.initialBatches).toBe(1);
    expect(outcome.capturedEpoch).toBe(2);
    expect(outcome.effectsAfterRequest).toBe(1);
    expect(outcome.effects).toBe(1);
    expect(outcome.activeAfterRequest).toBe(false);
    expect(outcome.after.snapshots).toHaveLength(1);
    expect(outcome.structural.initialBatchRows).toEqual(
      proof ? [1, 1, 1] : [1, 0],
    );
    expect(outcome.structural.responseLosses).toBe(loss ? 1 : 0);
    const snapshot = JSON.parse(String(outcome.after.snapshots[0]?.snapshot));
    const provenance = snapshot.requestContext['flowsafe.runProvenance'];
    expect(provenance).toMatchObject({
      mutationEpoch: 2,
      requestedBy: 'p3-owner',
      startToken: expect.any(String),
    });
    if (proof) {
      expect(
        outcome.after.keys.find((row) => row.key === 'p3-key'),
      ).toMatchObject({
        state: 'terminal',
        start_token: provenance.startToken,
        start_table_prefix: 'p3_',
        start_workflow_id: 'workflow-schedule',
      });
      expect(outcome.after.keys.filter((row) => row.key !== 'p3-key')).toEqual(
        outcome.before.keys,
      );
    } else expect(outcome.after.keys).toEqual(outcome.before.keys);
    expect(outcome.after.owners).toEqual(outcome.before.owners);
    if (afterAdmission === 'advance') {
      expect(outcome.fenceAfter).toMatchObject({
        state: 'open',
        mutationEpoch: 3,
        requireMutationEpoch: true,
      });
    } else {
      expect(outcome.fenceAfter).toMatchObject({
        state: proof ? 'proof-only' : 'open',
        mutationEpoch: 2,
        requireMutationEpoch: true,
      });
      if (proof)
        expect(outcome.structural.after.rows[0]).toMatchObject({
          proof_run_id: 'p3-run',
          proof_start_token: provenance.startToken,
          proof_table_prefix: 'p3_',
          proof_workflow_id: 'workflow-schedule',
        });
    }
    expectP3Metrics(outcome.metrics);
  });

  it('FS8 P3 refuses a formerly exact Runtime after the next artifact activates', async () => {
    const outcome = await result<P3RunResult>(
      probe,
      '/epoch-p3/run?phase=final&action=advance',
    );
    expectP3EpochRefusal(outcome.response, 'stale', 3);
    expectP3RunRefused(outcome);
  });

  it('FS8 P3 keeps the missing Runtime epoch captured before host policy awaits', async () => {
    const outcome = await result<P3RunResult>(
      probe,
      '/epoch-p3/run?phase=auth&epoch=missing&action=capture',
    );
    expectP3EpochRefusal(outcome.response, 'missing', 2);
    expect(outcome.capturedEpoch).toBeNull();
    expectP3RunRefused(outcome);
  });

  it('FS8 P3 keeps captured Runtime options while the provider waits', async () => {
    const outcome = await result<P3RunResult>(
      probe,
      '/epoch-p3/run?phase=provider&action=capture',
    );
    expect(outcome.response.status, JSON.stringify(outcome.response.body)).toBe(
      200,
    );
    expect(outcome.capturedEpoch).toBe(2);
    expect(outcome.response.body.result).toEqual({ value: 'original' });
    expect(outcome.effectsAfterRequest).toBe(1);
    const snapshot = JSON.parse(String(outcome.after.snapshots[0]?.snapshot));
    expect(snapshot.requestContext['flowsafe.runProvenance']).toMatchObject({
      mutationEpoch: 2,
      requestedBy: 'p3-owner',
    });
  });

  it('FS8 P3 refuses a current Runtime start while draining', async () => {
    const outcome = await result<P3RunResult>(
      probe,
      '/epoch-p3/run?closed=true',
    );
    expect(outcome.response.status).toBe(503);
    expect(outcome.response.body.reason?.code).toBe('EXECUTION_FENCED');
    expect(outcome.effects).toBe(0);
    expect(outcome.after).toEqual(outcome.before);
  });

  it('FS8 P3 releases the original keyed claim after final-D1 epoch refusal', async () => {
    const outcome = await result<P3RunResult>(
      probe,
      '/epoch-p3/run?phase=final&epoch=missing&action=activate&keyed=true',
    );
    expectP3EpochRefusal(outcome.response, 'missing', 1);
    expect(outcome.effects).toBe(0);
    expect(outcome.after.snapshots).toEqual([]);
    expect(outcome.after.owners).toEqual(outcome.before.owners);
    expect(
      outcome.after.keys.find((row) => row.key === 'p3-key'),
    ).toMatchObject({
      key: 'p3-key',
      state: 'reserved',
      owner_id: 'p3-owner',
      run_id: 'p3-run',
      start_token: '',
    });
    expect(outcome.after.keys.filter((row) => row.key !== 'p3-key')).toEqual(
      outcome.before.keys,
    );
    expect(outcome.activeAfterRequest).toBe(false);
    expect(outcome.cachedAfterRequest).toBe(false);
    expectP3Metrics(outcome.metrics);
  });

  it('FS8 P3 settles a current keyed Runtime execution', async () => {
    const outcome = await result<P3RunResult>(
      probe,
      '/epoch-p3/run?keyed=true',
    );
    expect(outcome.response.status, JSON.stringify(outcome.response.body)).toBe(
      200,
    );
    expect(outcome.effects).toBe(1);
    expect(
      outcome.after.keys.find((row) => row.key === 'p3-key'),
    ).toMatchObject({
      key: 'p3-key',
      state: 'terminal',
      owner_id: 'p3-owner',
      run_id: 'p3-run',
      start_token: expect.any(String),
    });
    expect(outcome.after.keys.filter((row) => row.key !== 'p3-key')).toEqual(
      outcome.before.keys,
    );
    expectP3Metrics(outcome.metrics);
  });

  it('FS8 P3 converges a committed D1 CAS after response loss', async () => {
    const outcome = await result<{
      losses: number;
      before: P3Fence;
      afterCommit: P3Fence;
      retry: P3Fence;
      rawAfterCommit: unknown;
      rawAfterRetry: unknown;
      conflict: { code: string };
      metrics: P3Metrics;
    }>(probe, '/epoch-p3/cas-loss');
    expect(outcome.losses).toBe(1);
    expect(outcome.afterCommit).toMatchObject({
      state: 'draining',
      mutationEpoch: outcome.before.mutationEpoch + 1,
      transitionRevision: outcome.before.transitionRevision + 1,
      requireMutationEpoch: true,
    });
    expect(outcome.retry).toEqual(outcome.afterCommit);
    expect(JSON.stringify(outcome.rawAfterRetry)).toBe(
      JSON.stringify(outcome.rawAfterCommit),
    );
    expect(outcome.conflict.code).toBe('FENCE_CAS_CONFLICT');
    expectP3Metrics({ cas: outcome.metrics });
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
      rollbackError: { reason: { code: string }; cause: string };
      rollbackSchedule: { id: string };
      rollbackTriggers: unknown[];
      rollbackOwner: { kind: string; id: string };
      deleteResult: string;
      deletedSchedule: null;
      deletedTriggers: unknown[];
      deletedOwner?: unknown;
      ownerInsertError: { reason: { code: string }; cause: string };
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
    expect(outcome.rollbackError.reason).toEqual({
      code: 'SCHEDULE_MUTATION_OUTCOME_UNKNOWN',
    });
    expect(outcome.rollbackError.cause).toMatch(
      /injected owner delete failure/,
    );
    expect(outcome.rollbackSchedule.id).toBe('schedule-rollback');
    expect(outcome.rollbackTriggers).toHaveLength(1);
    expect(outcome.rollbackOwner).toEqual({ kind: 'human', id: 'opal' });
    expect(outcome.deleteResult).toBe('deleted');
    expect(outcome.deletedSchedule).toBeNull();
    expect(outcome.deletedTriggers).toEqual([]);
    expect(outcome.deletedOwner).toBeUndefined();
    expect(outcome.ownerInsertError.reason).toEqual({
      code: 'SCHEDULE_MUTATION_OUTCOME_UNKNOWN',
    });
    expect(outcome.ownerInsertError.cause).toMatch(
      /injected owner insert failure/,
    );
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

  it('F5 notification chronology selects ordinary due cursors before future expanded years under limit 1', async () => {
    const outcomes = await result<
      Array<{ cursor: string; boundedIds: string[]; dueIds: string[] }>
    >(probe, '/notification-chronology/future');
    expect(outcomes).toEqual([
      { cursor: 'deliverAt', boundedIds: ['due'], dueIds: ['due'] },
      { cursor: 'summaryAt', boundedIds: ['due'], dueIds: ['due'] },
    ]);
  });

  it('F5 notification chronology orders raw offsets and truncates fractions before the due limit', async () => {
    const outcomes = await result<
      Array<{
        cursor: string;
        boundedIds: string[];
        due: Array<{ id: string; at: number | null }>;
        rawCursor: { cursor: string } | null;
      }>
    >(probe, '/notification-chronology/offsets');
    expect(outcomes).toEqual(
      ['deliverAt', 'summaryAt'].map((cursor) => ({
        cursor,
        boundedIds: ['first'],
        due: [
          {
            id: 'first',
            at: new Date('2026-09-09T10:30:00.123Z').getTime(),
          },
          {
            id: 'second',
            at: new Date('2026-09-09T10:45:00.123Z').getTime(),
          },
          {
            id: 'third',
            at: new Date('2026-09-09T11:00:00.123Z').getTime(),
          },
        ],
        rawCursor: { cursor: '2026-09-09T12:30:00.1239+02:00' },
      })),
    );
  });

  it('F5 notification chronology compares negative cycles and Date endpoints with millisecond precision', async () => {
    const outcomes = await result<
      Array<{
        name: string;
        boundedIds: string[];
        due: Array<{
          id: string;
          deliverAt: number | null;
          summaryAt: number | null;
        }>;
      }>
    >(probe, '/notification-chronology/bounds');
    expect(outcomes).toEqual(
      [
        {
          name: 'negative',
          past: new Date('-000800-01-01T00:00:00.001Z').getTime(),
          equal: new Date('-000400-01-01T00:00:00.000Z').getTime(),
        },
        {
          name: 'minimum',
          past: -8_640_000_000_000_000,
          equal: -8_639_999_999_999_999,
        },
        {
          name: 'maximum',
          past: 8_639_999_999_999_998,
          equal: 8_639_999_999_999_999,
        },
      ].map((fixture) => ({
        name: fixture.name,
        boundedIds: ['past'],
        due: [
          { id: 'past', deliverAt: fixture.past, summaryAt: null },
          { id: 'equal', deliverAt: null, summaryAt: fixture.equal },
        ],
      })),
    );
  });

  it('F5 notification chronology lists updated instants across offsets and expanded years before the limit', async () => {
    const outcome = await result<{
      boundedIds: string[];
      records: Array<{ id: string; updatedAt: number }>;
    }>(probe, '/notification-chronology/list');
    expect(outcome).toEqual({
      boundedIds: ['maximum', 'expanded', 'ordinary'],
      records: [
        { id: 'maximum', updatedAt: 8_640_000_000_000_000 },
        {
          id: 'expanded',
          updatedAt: new Date('+010000-01-01T00:00:00.000Z').getTime(),
        },
        {
          id: 'ordinary',
          updatedAt: new Date('2026-09-09T11:00:00.000Z').getTime(),
        },
        {
          id: 'offset',
          updatedAt: new Date('2026-09-09T10:30:00.000Z').getTime(),
        },
        {
          id: 'negative',
          updatedAt: new Date('-000001-01-01T00:00:00.000Z').getTime(),
        },
        { id: 'minimum', updatedAt: -8_640_000_000_000_000 },
      ],
    });
  });

  it('F5 notification chronology TTL preserves future receipts, cutoff equality and pending work', async () => {
    const outcome = await result<{
      purged: number;
      after: Array<{ id: string; status: string; updatedAt: string }>;
      futureSignalId: string | null;
      repeated: number;
    }>(probe, '/notification-chronology/ttl');
    expect(outcome).toEqual({
      purged: 1,
      after: [
        {
          id: 'equal',
          status: 'delivered',
          updatedAt: '2026-09-09T10:00:00.000-0100',
        },
        {
          id: 'future',
          status: 'delivered',
          updatedAt: '+010000-01-01T00:00:00.000Z',
        },
        {
          id: 'pending',
          status: 'pending',
          updatedAt: '2020-01-01T00:00:00.000Z',
        },
      ],
      futureSignalId: 'signal-future',
      repeated: 0,
    });
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
