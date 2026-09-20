// SPDX-License-Identifier: Apache-2.0
import type { DurableObjectState } from '@cloudflare/workers-types';
import type { MastraCompositeStore } from '@mastra/core/storage';
import type {
  DefaultEngineType,
  ExecuteFunction,
  WorkflowRunState,
} from '@mastra/core/workflows';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import {
  deploymentIdentityDatabase,
  deploymentIdentityRequest,
  TEST_DEPLOYMENT_IDENTITY_SECRET,
} from '../../test-support/deployment-identity.js';
import { durableKeyValueStorageFixture } from '../../test-support/durable-key-value-storage.js';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  ApprovalService,
  D1ResourceOwnershipStore,
  type ExecutionPrincipal,
  encodeExecutionPrincipal,
  InMemoryApprovalStore,
  InMemoryResourceOwnershipStore,
  type ResourceOwnershipDatabase,
} from '../approval-api/index.js';
import { reconcileApprovalsForSummary } from '../host-kit/approval-bridge.js';
import {
  createDoRunTopology,
  type RunnerNamespaceLike,
} from '../host-kit/do-run-topology.js';
import type { DurableKeyValueStorage } from './cf-types.js';
import {
  createD1Storage,
  type SnapshotDatabase,
  sweepExpiredRunDeadlines,
} from './d1-storage.js';
import type { DeploymentIdentityEnv } from './deployment-identity.js';
import {
  type DurableObjectRunLifecycleHooks,
  DurableObjectRunner,
  type DurableObjectRunOwner,
  type DurableObjectRunOwnershipStore,
  nextDutyAlarmAt,
} from './durable-object.js';
import { MUTATION_EPOCH_HEADER } from './execution-admission.js';
import type { ExecutionFenceDatabase } from './execution-fence.js';
import { ExecutionFenceStore } from './execution-fence.js';
import { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
import {
  FENCED_WORKFLOW_STORAGE,
  type FencedWorkflowAdmissionCapability,
} from './fenced-workflow-capability.js';
import type { FencedWorkflowsStorageD1 } from './fenced-workflows-d1.js';
import { init } from './init.js';
import {
  type RequestContextProvider,
  type RunnerRuntime,
  RunStateUnreadableError,
  type RunSummary,
} from './runtime.js';
import type { ScheduleSourceStore } from './schedule-source.js';
import { StartIdempotencyStore } from './start-idempotency.js';
import {
  isSuspensionTimeoutResumeData,
  MAX_SUSPENSION_DEADLINE_ATTEMPTS,
  MAX_SUSPENSION_DEADLINE_MS,
  MIN_SUSPENSION_DEADLINE_MS,
  SUSPENSION_DEADLINE_PAYLOAD_KEY,
  SUSPENSION_DEADLINE_PRINCIPAL_ID,
  SUSPENSION_DEADLINE_STORAGE_KEY,
  SUSPENSION_TIMEOUT_RESUME_KEY,
  type SuspensionDeadlineEntry,
  type SuspensionDeadlineRecord,
} from './suspension-deadline.js';

const testStorageDatabases = new WeakMap<
  MastraCompositeStore,
  ExecutionFenceDatabase & ResourceOwnershipDatabase
>();
function testStorage(): MastraCompositeStore {
  const binding = deploymentIdentityDatabase() as ExecutionFenceDatabase &
    ResourceOwnershipDatabase;
  const storage = createD1Storage({ binding });
  testStorageDatabases.set(storage, binding);
  return storage;
}
function testDatabase(
  storage: MastraCompositeStore,
): ExecutionFenceDatabase & ResourceOwnershipDatabase {
  const binding = testStorageDatabases.get(storage);
  if (!binding) throw new Error('test storage database was not registered');
  return binding;
}

interface TestEnv extends DeploymentIdentityEnv {
  storage: MastraCompositeStore;
  runtime?: RunnerRuntime;
  /**
   * The deployment execution fence the built runtime is wired to. Always
   * present on a production-shaped env — DurableObjectRunner refuses to serve
   * from a fence-less RunnerRuntime while a DB binding is bound — and shared
   * with the test so it can move the fence under a live object.
   */
  fence?: ExecutionFenceStore;
  owners: DurableObjectRunOwnershipStore;
  schedules?: ScheduleSourceStore;
  lifecycle?: DurableObjectRunLifecycleHooks;
}

interface OwnerHooks {
  reserve(
    runId: string,
    owner: DurableObjectRunOwner,
    token: string,
  ): Promise<boolean>;
  settle(token: string, runId: string, release: boolean): Promise<void>;
  owner?: (runId: string) => Promise<DurableObjectRunOwner | undefined>;
  release?: (runId: string, owner: DurableObjectRunOwner) => Promise<boolean>;
}

function makeProductionEnv(
  storage = testStorage(),
  hooks?: OwnerHooks,
): TestEnv {
  const registry = new D1ResourceOwnershipStore(
    testDatabase(storage) as ResourceOwnershipDatabase,
  );
  const attempts = new Map<string, string>();
  const customOwner = hooks?.owner;
  const owners: DurableObjectRunOwnershipStore = hooks
    ? {
        owner: customOwner
          ? (_kind, resourceId) => customOwner(resourceId)
          : (kind, resourceId) => registry.owner(kind, resourceId),
        reserveAll: async (claims, owner, token) => {
          const runId = claims[0]?.resourceId;
          if (!runId) throw new Error('run claim missing');
          attempts.set(token, runId);
          if (!(await hooks.reserve(runId, owner, token))) return false;
          return registry.reserveAll(claims, owner, token);
        },
        settleReservation: async (token, release) => {
          const runId = attempts.get(token);
          if (!runId) throw new Error('unknown run ownership attempt');
          await hooks.settle(token, runId, release.length > 0);
          await registry.settleReservation(token, release);
          attempts.delete(token);
        },
        release: async (_kind, resourceId, owner) => {
          if (hooks.release) return hooks.release(resourceId, owner);
          return registry.release('run', resourceId, owner);
        },
      }
    : registry;
  const db = testDatabase(storage);
  return {
    storage,
    owners,
    DEPLOYMENT_TENANT: 'acme',
    DEPLOYMENT_IDENTITY_SECRET: TEST_DEPLOYMENT_IDENTITY_SECRET,
    DB: db,
    // The fence lives in the SAME database as the deployment sentinel, exactly
    // as it does in production. Absent-table reads as 'open', so every test
    // that does not move it is byte-identical to before the fence existed.
    fence: new ExecutionFenceStore(db),
  };
}

async function durableOwnerRecovery(
  runtime: RunnerRuntime,
  workflowId: string,
  runId: string,
  token: string,
) {
  const state = await runtime.authoritativeStartState(workflowId, runId);
  if (!state) throw new Error('missing durable execution for journal fixture');
  return {
    version: 2,
    phase: 'prepared',
    owner: { kind: 'human', id: 'owner-1' },
    workflowId,
    runId,
    token,
    execution: state.execution,
  };
}

/** Distinct spies keep public-status and authoritative-read assertions independent. */
function statusStub(read: RunnerRuntime['status']) {
  return {
    status: vi.fn(read),
    authoritativeStatus: vi.fn(read),
    isRunActive: vi.fn(() => false),
    assertExistingRunAllowed: vi.fn(async () => undefined),
  };
}

async function hostR1WorkflowFixture(
  mode: 'custom-null' | 'actual-prefix',
  wired = true,
) {
  const { InMemoryStore } = await import('@mastra/core/storage');
  const sql = openSqlite();
  const binding = sqliteUnitDatabase(sql) as ExecutionFenceDatabase &
    ResourceOwnershipDatabase;
  const storage =
    mode === 'custom-null'
      ? new InMemoryStore()
      : createD1Storage({ binding, tablePrefix: 'host_r1_' });
  await storage.init();
  const reservations = new StartIdempotencyStore(binding);
  const app = init(
    { storage },
    { executionFence: 'none', startIdempotency: wired ? reservations : 'none' },
  );
  let effects = 0;
  app
    .createWorkflow({
      id: 'host-r1',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
    .then(
      app.createStep({
        id: 'gate',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        execute: async ({ suspend }) => {
          effects++;
          return suspend({ reason: 'held' });
        },
      }),
    )
    .commit();
  const owners = new D1ResourceOwnershipStore(binding);
  const hostRuntime = new Proxy({} as RunnerRuntime, {
    get: (_target, property) => {
      const value: unknown = Reflect.get(app.runtime, property, app.runtime);
      return typeof value === 'function' ? value.bind(app.runtime) : value;
    },
  });
  const env: TestEnv = {
    storage,
    owners,
    runtime: hostRuntime,
    DB: deploymentIdentityDatabase(),
    DEPLOYMENT_TENANT: 'acme',
    DEPLOYMENT_IDENTITY_SECRET: TEST_DEPLOYMENT_IDENTITY_SECRET,
  };
  const journal = durableKeyValueStorageFixture();
  const runner = new TestRunner(journal.state, env);
  const workflows = await storage.getStore('workflows');
  if (!workflows) throw new Error('missing workflow domain');
  return {
    app,
    storage,
    workflows,
    reservations,
    owners,
    env,
    journal,
    runner,
    sql,
    effects: () => effects,
  };
}

/**
 * A fence over its own throwaway database, for the runtime builders that are
 * handed a storage instance and no env. Every read finds no table and answers
 * 'open', so these runners behave exactly as they did before the fence — what
 * it buys is that they are FENCED runtimes, which is what DurableObjectRunner
 * asserts of anything it serves from while a DB binding is bound.
 */
function newTestExecutionFence(
  storage: MastraCompositeStore,
): ExecutionFenceStore {
  return new ExecutionFenceStore(testDatabase(storage));
}
function newTestStartIdempotency(
  storage: MastraCompositeStore,
): StartIdempotencyStore {
  return new StartIdempotencyStore(testDatabase(storage));
}

function gatedRuntime(
  storage: MastraCompositeStore,
  executionFence: ExecutionFenceStore = newTestExecutionFence(storage),
  requestContextForRun?: RequestContextProvider,
): RunnerRuntime {
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    {
      executionFence,
      startIdempotency: newTestStartIdempotency(storage),
      requestContextForRun,
    },
  );
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string(), approvedBy: z.string() }),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({ approvedBy: z.string() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'awaiting approval' });
      return { topic: inputData.topic, approvedBy: resumeData.approvedBy };
    },
  });
  createWorkflow({
    id: 'gated',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string(), approvedBy: z.string() }),
  })
    .then(gate)
    .commit();
  return runtime;
}

class TestRunner extends DurableObjectRunner<TestEnv> {
  constructor(
    state: DurableObjectState | undefined,
    env: TestEnv,
    withStorage = true,
  ) {
    super(
      withStorage
        ? ({
            ...state,
            storage:
              state?.storage ?? durableKeyValueStorageFixture().state.storage,
          } as DurableObjectState)
        : state,
      env,
    );
  }
  protected runOwnership(env: TestEnv): DurableObjectRunOwnershipStore {
    return env.owners;
  }

  protected scheduleSource(env: TestEnv): ScheduleSourceStore | undefined {
    return env.schedules;
  }

  protected runLifecycle(env: TestEnv) {
    return env.lifecycle ?? { abandonApprovals: async () => undefined };
  }

  protected build(env: TestEnv): RunnerRuntime {
    if (env.runtime) {
      if (
        env.runtime.constructor.name !== 'RunnerRuntime' &&
        !env.runtime.authoritativeStartState
      ) {
        const selected = new Map<
          string,
          import('./runtime.js').AuthoritativeStartState
        >();
        const nativeStart = env.runtime.start?.bind(env.runtime);
        env.runtime.authoritativeStartState = vi.fn(
          async (_workflow, runId) => selected.get(runId) ?? null,
        );
        env.runtime.settleStartExecution = vi.fn(async () => {});
        env.runtime.isRunActive ??= vi.fn(() => false);
        env.runtime.assertExistingRunAllowed ??= vi.fn(async () => undefined);
        if (nativeStart)
          env.runtime.start = vi.fn(async (workflowId, options) => {
            const summary = await nativeStart(workflowId, options);
            const execution = {
              tablePrefix: null,
              workflowId,
              runId: summary.runId,
              startToken: 'stub-generation',
            } as const;
            await options?.onPreparedStartIdentity?.(execution);
            const provenance = {
              version: 2 as const,
              startToken: execution.startToken,
              attemptToken: options?.attemptToken ?? 'stub-attempt',
              resumeCounts: [],
              startIdentity: options?.startIdentity,
            };
            selected.set(summary.runId, {
              kind: 'result',
              storage: 'unfenced',
              execution,
              provenance,
              summary,
              snapshot: {
                runId: summary.runId,
                status: summary.status,
                context: {},
                value: {},
                serializedStepGraph: [],
                activePaths: [],
                activeStepsPath: {},
                suspendedPaths: {},
                resumeLabels: {},
                waitingPaths: {},
                timestamp: 1,
                requestContext: {},
              },
            } as import('./runtime.js').AuthoritativeStartState);
            return summary;
          });
      }
      return env.runtime;
    }
    return gatedRuntime(env.storage, env.fence);
  }
}

function makeRunner(): TestRunner {
  return new TestRunner(undefined, makeProductionEnv());
}

const OWNER_PRINCIPAL: ExecutionPrincipal = {
  kind: 'human',
  id: 'owner-1',
  role: 'operator',
};

function post(
  path: string,
  body: unknown,
  principal: ExecutionPrincipal = OWNER_PRINCIPAL,
): Request {
  const payload =
    path.endsWith('/resume') &&
    body !== null &&
    typeof body === 'object' &&
    !Array.isArray(body)
      ? {
          ...body,
          requestedBy:
            (body as { requestedBy?: unknown }).requestedBy ?? 'reviewer-1',
          requestedByKind:
            (body as { requestedByKind?: unknown }).requestedByKind ?? 'human',
        }
      : body;
  const headers: Record<string, string> = {
    'content-type': 'application/json',
  };
  if (
    path === '/runs' ||
    path.endsWith('/terminate') ||
    path.endsWith('/terminate-replay') ||
    path.endsWith('/deadline')
  ) {
    headers[EXECUTION_PRINCIPAL_HEADER] = encodeExecutionPrincipal(principal);
  }
  return deploymentIdentityRequest(`http://do${path}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
  });
}

function preparedScheduleSource(input: {
  scheduleId: string;
  dispatchId: string;
  runId: string;
  target: Awaited<ReturnType<ScheduleSourceStore['resolveScheduleTarget']>>;
}): ScheduleSourceStore {
  return {
    resolveScheduleTarget: async (scheduleId, dispatchId, runId) =>
      scheduleId === input.scheduleId &&
      dispatchId === input.dispatchId &&
      runId === input.runId
        ? input.target
        : undefined,
  };
}

async function startGated(runner: TestRunner): Promise<RunSummary> {
  const response = await runner.fetch(
    post('/runs', {
      workflowId: 'gated',
      runId: crypto.randomUUID(),
      inputData: { topic: 't' },
    }),
  );
  return (await response.json()) as RunSummary;
}

function cDeferred() {
  let resolve = () => {};
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function cWorkflowFixture(owned = false, provider?: RequestContextProvider) {
  const events: string[] = [];
  const journal = durableKeyValueStorageFixture(events);
  const env = makeProductionEnv();
  const binding = env.DB;
  if (!binding) throw new Error('missing managed test database');
  const storage = owned ? createD1Storage({ binding }) : env.storage;
  testStorageDatabases.set(
    storage,
    binding as ExecutionFenceDatabase & ResourceOwnershipDatabase,
  );
  const runtime = gatedRuntime(storage, env.fence, provider);
  env.runtime = runtime;
  const runner = new TestRunner(journal.state, env);
  const start = vi.spyOn(runtime, 'start');
  const reserve = vi.spyOn(env.owners, 'reserveAll');
  return { events, journal, env, storage, runtime, runner, start, reserve };
}

function cHoldWorkflowVerification(env: TestEnv) {
  const entered = cDeferred();
  const release = cDeferred();
  const identity = deploymentIdentityDatabase();
  env.DB = {
    prepare(query) {
      const statement = identity.prepare(query);
      return {
        ...statement,
        async all<T>() {
          const result = await statement.all<T>();
          entered.resolve();
          await release.promise;
          return result;
        },
      };
    },
  };
  return { entered, release };
}

const C_WORKFLOW_BODY = {
  workflowId: 'gated',
  runId: 'c-run',
  inputData: { topic: 'original' },
};
const C_REPLACEMENT_PRINCIPAL: ExecutionPrincipal = {
  kind: 'service',
  id: 'replacement',
  purpose: 'replacement start',
};

async function cWorkflowBarrier(runner: TestRunner) {
  const response = await runner.fetch(
    deploymentIdentityRequest('http://do/runs/gated/c-run/start-liveness'),
  );
  expect(response.status).toBe(200);
}

async function cHoldWorkflowFifo(fixture: ReturnType<typeof cWorkflowFixture>) {
  const entered = cDeferred();
  const release = cDeferred();
  const nativeStatus = fixture.runtime.status.bind(fixture.runtime);
  const status = vi
    .spyOn(fixture.runtime, 'status')
    .mockImplementationOnce(async (...args) => {
      entered.resolve();
      await release.promise;
      return nativeStatus(...args);
    });
  const holder = fixture.runner.fetch(
    deploymentIdentityRequest('http://do/runs/gated/c-run/dispatch-status'),
  );
  await entered.promise;
  return { release, holder, status };
}

function cObserveFifo() {
  const NativePromise = Promise;
  const signal = cDeferred();
  let count = 0;
  const Observed = new Proxy(NativePromise, {
    construct(target, args) {
      const stack = new Error().stack ?? '';
      const value = Reflect.construct(target, args, target);
      if (stack.includes('#withOperationLock')) {
        count++;
        signal.resolve();
      }
      return value;
    },
  });
  vi.stubGlobal('Promise', Observed);
  return {
    enqueued: signal.promise,
    count: () => count,
    restore: () => vi.stubGlobal('Promise', NativePromise),
  };
}

async function cRequireEpoch2(fence: ExecutionFenceStore | undefined) {
  if (!fence) throw new Error('missing managed execution fence');
  await fence.seed('open');
  for (let index = 0; index < 2; index++) {
    const before = await fence.read();
    const draining = await fence.transition({
      expected: 'open',
      next: 'draining',
      expectedMutationEpoch: before.mutationEpoch,
      expectedRevision: before.transitionRevision,
      advanceMutationEpoch: true,
    });
    await fence.transition({
      expected: 'draining',
      next: 'open',
      expectedMutationEpoch: draining.mutationEpoch,
      expectedRevision: draining.transitionRevision,
    });
  }
  expect(await fence.read()).toMatchObject({
    state: 'open',
    mutationEpoch: 2,
    requireMutationEpoch: true,
  });
}

describe('C workflow ingress capture', () => {
  it.each(
    (['human', 'service', 'system'] as const).flatMap((kind) =>
      (['alternate', 'second-throw'] as const).flatMap((mode) =>
        (kind === 'human' ? [false, true] : [true]).map((scheduled) => ({
          kind,
          mode,
          scheduled,
        })),
      ),
    ),
  )('C workflow DO captures body and distinct source owner before waits ($kind, $mode, scheduled=$scheduled)', async ({
    kind,
    mode,
    scheduled,
  }) => {
    const fixture = cWorkflowFixture();
    const principal: ExecutionPrincipal =
      kind === 'human'
        ? { kind, id: 'initiator', role: 'operator' }
        : { kind, id: `${kind}-initiator`, purpose: 'schedule execution' };
    const fence = fixture.env.fence;
    if (!fence) throw new Error('missing workflow test fence');
    const fenceEntered = cDeferred();
    const fenceRelease = cDeferred();
    const sourceEntered = cDeferred();
    const sourceRelease = cDeferred();
    const copied = cDeferred();
    const copyRelease = cDeferred();
    const readFence = fence.read.bind(fence);
    vi.spyOn(fence, 'read').mockImplementationOnce(async () => {
      fenceEntered.resolve();
      await fenceRelease.promise;
      return readFence();
    });
    const targetInput = { topic: 'scheduled-original' };
    const targetState = {};
    const targetContext = { 'test.source': 'original' };
    const target = {
      type: 'workflow' as const,
      workflowId: 'gated',
      inputData: targetInput,
      initialState: targetState,
      requestContext: targetContext,
    };
    const resolveTarget = vi.fn<ScheduleSourceStore['resolveScheduleTarget']>(
      async () => {
        sourceEntered.resolve();
        await sourceRelease.promise;
        return target;
      },
    );
    fixture.env.schedules = { resolveScheduleTarget: resolveTarget };
    const ownerValues: DurableObjectRunOwner = {
      kind: 'human',
      id: 'schedule-owner',
    };
    const ownerReads = {
      kind: vi.fn(() => ownerValues.kind),
      id: vi.fn(() => ownerValues.id),
    };
    const owner: DurableObjectRunOwner = {
      get kind() {
        return ownerReads.kind();
      },
      get id() {
        return ownerReads.id();
      },
    };
    const nativeOwner = fixture.env.owners.owner.bind(fixture.env.owners);
    vi.spyOn(fixture.env.owners, 'owner').mockImplementation(
      async (resourceKind, id) =>
        resourceKind === 'schedule' ? owner : nativeOwner(resourceKind, id),
    );
    const nativeGet = fixture.journal.storage.get.bind(fixture.journal.storage);
    let held = false;
    vi.spyOn(fixture.journal.storage, 'get').mockImplementation(
      async <T>(key: string) => {
        if (!held && key === 'flowsafe:run-owner-recovery:v1') {
          held = true;
          copied.resolve();
          await copyRelease.promise;
        }
        return nativeGet<T>(key);
      },
    );
    const bodyInput = { topic: 'body-original' };
    const bodyState = {};
    const bodyContext = { 'test.source': 'body-original' };
    const expectedOwner = scheduled
      ? { kind: 'human', id: 'schedule-owner' }
      : { kind: principal.kind, id: principal.id };
    const values = {
      workflowId: 'gated',
      runId: 'c-run',
      inputData: bodyInput,
      initialState: bodyState,
      requestContext: bodyContext,
      scheduleId: scheduled ? 'original-schedule' : undefined,
      dispatchId: scheduled ? 'original-dispatch' : undefined,
      deadlineMs: 1000,
      idempotencyKey: 'original-key',
    };
    const reads = new Map<string, number>();
    const body = {};
    for (const key of Object.keys(values) as Array<keyof typeof values>)
      Object.defineProperty(body, key, {
        enumerable: true,
        configurable: true,
        get() {
          const count = (reads.get(key) ?? 0) + 1;
          reads.set(key, count);
          if (count > 1 && mode === 'second-throw')
            throw new Error(`second body read: ${key}`);
          return values[key];
        },
      });
    const request = post('/runs', C_WORKFLOW_BODY, principal);
    request.headers.set(MUTATION_EPOCH_HEADER, '2');
    vi.spyOn(request, 'json').mockResolvedValue(body);
    const writes: Array<[string, unknown]> = [];
    const nativePut = fixture.journal.storage.put.bind(fixture.journal.storage);
    vi.spyOn(fixture.journal.storage, 'put').mockImplementation(
      async (key, value) => {
        writes.push([key, structuredClone(value)]);
        await nativePut(key, value);
      },
    );
    const pending = fixture.runner.fetch(request);
    const outcome = pending.then(
      () => false,
      () => false,
    );
    try {
      expect(
        await Promise.race([fenceEntered.promise.then(() => true), outcome]),
      ).toBe(true);
      expect([...reads.keys()].sort()).toEqual(Object.keys(values).sort());
      for (const count of reads.values()) expect(count).toBe(1);
      Object.assign(values, {
        workflowId: 'replacement',
        runId: 'replacement',
        inputData: { topic: 'replacement' },
        initialState: { replaced: true },
        requestContext: { replaced: true },
        scheduleId: 'replacement',
        dispatchId: 'replacement',
        deadlineMs: 9000,
        idempotencyKey: 'replacement',
      });
      fenceRelease.resolve();
      if (scheduled) {
        expect(
          await Promise.race([sourceEntered.promise.then(() => true), outcome]),
        ).toBe(true);
        expect(resolveTarget).toHaveBeenCalledWith(
          'original-schedule',
          'original-dispatch',
          'c-run',
        );
        sourceRelease.resolve();
      }
      expect(
        await Promise.race([copied.promise.then(() => true), outcome]),
      ).toBe(true);
      if (!scheduled) expect(resolveTarget).not.toHaveBeenCalled();
      expect(ownerReads.kind).toHaveBeenCalledTimes(scheduled ? 1 : 0);
      expect(ownerReads.id).toHaveBeenCalledTimes(scheduled ? 1 : 0);
      Object.assign(ownerValues, { kind: 'service', id: 'replacement-owner' });
      Object.assign(target, {
        workflowId: 'replacement',
        inputData: { topic: 'replacement-target' },
        initialState: { replaced: true },
        requestContext: { replaced: true },
      });
    } finally {
      fenceRelease.resolve();
      sourceRelease.resolve();
      copyRelease.resolve();
      await outcome;
    }
    const response = await pending;
    expect(response.status).toBe(200);
    expect(fixture.start).toHaveBeenCalledOnce();
    expect(fixture.start.mock.calls[0]?.[0]).toBe('gated');
    const options = fixture.start.mock.calls[0]?.[1];
    expect(options).toMatchObject({
      runId: 'c-run',
      deadlineMs: 1000,
      idempotencyKey: 'original-key',
      mutationEpoch: 2,
      requestedBy: principal.id,
      requestedByKind: principal.kind,
      startIdentity: {
        owner: { kind: principal.kind, id: principal.id },
        target: { kind: 'workflow', id: 'gated' },
      },
      runOwnerGuard: { owner: expectedOwner },
    });
    expect(options?.inputData).toBe(scheduled ? targetInput : bodyInput);
    expect(options?.initialState).toBe(scheduled ? targetState : bodyState);
    expect(options?.storedRequestContext).toBe(
      scheduled ? targetContext : bodyContext,
    );
    expect(options?.runOwnerGuard?.reservationToken).toBe(
      options?.attemptToken,
    );
    expect(Object.hasOwn(options ?? {}, 'onPreparedStartIdentity')).toBe(true);
    expect(options?.onPreparedStartIdentity).toBeTypeOf('function');
    expect(fixture.reserve.mock.calls[0]).toEqual([
      [{ kind: 'run', resourceId: 'c-run' }],
      expectedOwner,
      options?.attemptToken,
    ]);
    const journals = writes.filter(([key]) =>
      key.startsWith('flowsafe:run-owner-recovery'),
    );
    expect(journals).toHaveLength(2);
    expect(journals[0]?.[1]).toMatchObject({
      version: 2,
      phase: 'preparing',
      workflowId: 'gated',
      runId: 'c-run',
      token: options?.attemptToken,
      owner: expectedOwner,
    });
    expect(journals[1]?.[1]).toMatchObject({
      version: 2,
      phase: 'prepared',
      execution: {
        tablePrefix: '',
        workflowId: 'gated',
        runId: 'c-run',
        startToken: expect.any(String),
      },
    });
    for (const count of reads.values()) expect(count).toBe(1);
    expect(ownerReads.kind).toHaveBeenCalledTimes(scheduled ? 1 : 0);
    expect(ownerReads.id).toHaveBeenCalledTimes(scheduled ? 1 : 0);
    expect(Object.isFrozen(body)).toBe(false);
    expect(Object.isFrozen(owner)).toBe(false);
  });

  it.each(
    [
      'mutationEpoch',
      'startIdentity',
      'agentStart',
      'execution',
      'tablePrefix',
      'startToken',
      'attemptToken',
      'runOwnerGuard',
      'onPreparedStartIdentity',
    ].flatMap((field) => [null, 2].map((value) => ({ field, value }))),
  )('C workflow DO refuses internal JSON authority before effects ($field, $value)', async ({
    field,
    value,
  }) => {
    const fixture = cWorkflowFixture();
    const build = vi.spyOn(
      fixture.runner as unknown as { build: () => RunnerRuntime },
      'build',
    );
    const response = await fixture.runner.fetch(
      post('/runs', { ...C_WORKFLOW_BODY, [field]: value }),
    );
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: 'start authority is derived from trusted provenance',
    });
    expect(build).not.toHaveBeenCalled();
    expect(fixture.start).not.toHaveBeenCalled();
    expect(fixture.reserve).not.toHaveBeenCalled();
    expect(fixture.events).toEqual([]);
    expect(fixture.journal.values.size).toBe(0);
    expect(
      (await fixture.runner.fetch(post('/runs', C_WORKFLOW_BODY))).status,
    ).toBe(200);
    expect(fixture.start).toHaveBeenCalledOnce();
  });

  it.each([
    'normal',
    'failure',
    'recovery',
  ] as const)('FS8 D3 host activation workflow preparation and ownership: %s', async (phase) => {
    const failure = new Error('C managed provider failed');
    const fixture = cWorkflowFixture(
      true,
      phase === 'failure'
        ? () => {
            throw failure;
          }
        : undefined,
    );
    await fixture.storage.init();
    await cRequireEpoch2(fixture.env.fence);
    const workflows = (await fixture.storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const native = workflows[FENCED_WORKFLOW_STORAGE];
    if (!native) throw new Error('missing managed owned workflow capability');
    const counts = { admission: 0, terminalization: 0 };
    const capability: FencedWorkflowAdmissionCapability = {
      ...native,
      withInitialAdmission: (input, create) => {
        counts.admission++;
        return native.withInitialAdmission(input, create);
      },
      terminalizeInitialAdmission: (input) => {
        counts.terminalization++;
        return native.terminalizeInitialAdmission(input);
      },
    };
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: capability,
      configurable: true,
    });
    const writes: Array<[string, unknown]> = [];
    const put = fixture.journal.storage.put.bind(fixture.journal.storage);
    vi.spyOn(fixture.journal.storage, 'put').mockImplementation(
      async (key, value) => {
        writes.push([key, structuredClone(value)]);
        await put(key, value);
      },
    );
    const settle = fixture.env.owners.settleReservation.bind(
      fixture.env.owners,
    );
    if (phase === 'recovery')
      vi.spyOn(fixture.env.owners, 'settleReservation').mockImplementationOnce(
        async (...args) => {
          await settle(...args);
          throw new Error('C lost settlement receipt');
        },
      );
    const request = post('/runs', C_WORKFLOW_BODY);
    request.headers.set(MUTATION_EPOCH_HEADER, '2');
    try {
      const response = await fixture.runner.fetch(request);
      expect(response.status).toBe(phase === 'failure' ? 500 : 200);
      expect(fixture.start).toHaveBeenCalledOnce();
      const options = fixture.start.mock.calls[0]?.[1];
      expect(options).toHaveProperty(
        'onPreparedStartIdentity',
        expect.any(Function),
      );
      expect(Object.hasOwn(options ?? {}, 'onPreparedStartIdentity')).toBe(
        true,
      );
      const journals = writes.filter(([key]) =>
        key.startsWith('flowsafe:run-owner-recovery'),
      );
      expect(journals).toHaveLength(phase === 'failure' ? 1 : 2);
      expect(journals[0]?.[1]).toMatchObject({
        version: 2,
        phase: 'preparing',
        token: options?.attemptToken,
      });
      if (phase !== 'failure')
        expect(journals[1]?.[1]).toMatchObject({
          phase: 'prepared',
          execution: { startToken: expect.any(String) },
        });
      const snapshot = await workflows.loadWorkflowSnapshot({
        workflowName: 'gated',
        runId: 'c-run',
      });
      if (phase === 'failure') expect(snapshot).toBeNull();
      else {
        expect(
          snapshot?.requestContext?.['flowsafe.runProvenance'],
        ).toMatchObject({
          version: 2,
          requestedBy: OWNER_PRINCIPAL.id,
          requestedByKind: OWNER_PRINCIPAL.kind,
          startToken: expect.any(String),
          attemptToken: options?.attemptToken,
          resumeCounts: [],
        });
        for (const key of [
          'mutationEpoch',
          'startIdentity',
          'agentStart',
          'execution',
          'flowsafe.initialAdmission',
        ])
          expect(snapshot?.requestContext).not.toHaveProperty(key);
      }
      if (phase === 'recovery')
        expect(
          fixture.journal.values.has('flowsafe:run-owner-recovery:v1'),
        ).toBe(false);
      await fixture.runner.alarm();
      expect(fixture.journal.values.has('flowsafe:run-owner-recovery:v1')).toBe(
        false,
      );
      if (phase !== 'failure') {
        const resumed = await fixture.runner.fetch(
          post('/runs/gated/c-run/resume', {
            step: 'gate',
            resumeData: { approvedBy: 'reviewer-1' },
          }),
        );
        expect(resumed.status).toBe(200);
        expect(await resumed.json()).toMatchObject({ status: 'success' });
        await fixture.runner.alarm();
      }
      expect(
        writes.filter(([key]) => key.startsWith('flowsafe:run-owner-recovery')),
      ).toEqual(journals);
    } finally {
      expect(counts).toEqual({
        admission: phase === 'failure' ? 0 : 1,
        terminalization: 0,
      });
    }
  });

  it.each([
    undefined,
    1,
    2,
    3,
  ])('FS8 D3 host activation enforces active mutation epoch at workflow start: %s', async (epoch) => {
    const fixture = cWorkflowFixture(true);
    await fixture.storage.init();
    await cRequireEpoch2(fixture.env.fence);
    const request = post('/runs', C_WORKFLOW_BODY);
    if (epoch !== undefined)
      request.headers.set(MUTATION_EPOCH_HEADER, String(epoch));
    const response = await fixture.runner.fetch(request);
    expect(response.status).toBe(epoch === 2 ? 200 : 409);
    if (epoch !== 2) {
      expect(await response.json()).toMatchObject({
        reason: { code: 'MUTATION_EPOCH_MISMATCH' },
      });
      return;
    }
    expect(await response.json()).toMatchObject({
      runId: 'c-run',
      status: 'suspended',
    });
    expect(fixture.start).toHaveBeenCalledOnce();
    const options = fixture.start.mock.calls[0]?.[1];
    expect(options?.mutationEpoch).toBe(epoch);
    expect(options).toHaveProperty(
      'onPreparedStartIdentity',
      expect.any(Function),
    );
    expect(options?.startIdentity).toEqual({
      owner: { kind: OWNER_PRINCIPAL.kind, id: OWNER_PRINCIPAL.id },
      target: { kind: 'workflow', id: 'gated' },
    });
    const workflows = await fixture.storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'c-run',
    });
    expect(snapshot?.requestContext?.['flowsafe.runProvenance']).toMatchObject({
      version: 2,
      requestedBy: OWNER_PRINCIPAL.id,
      requestedByKind: OWNER_PRINCIPAL.kind,
      startToken: expect.any(String),
      attemptToken: options?.attemptToken,
      resumeCounts: [],
    });
    expect(snapshot?.requestContext).not.toHaveProperty(
      'flowsafe.initialAdmission',
    );
    await cWorkflowBarrier(fixture.runner);
  });

  it.each([
    undefined,
    0,
    2,
    Number.MAX_SAFE_INTEGER,
  ])('C captures workflow headers before deployment verification: %s', async (epoch) => {
    const fixture = cWorkflowFixture();
    const hold = cHoldWorkflowVerification(fixture.env);
    const request = post('/runs', C_WORKFLOW_BODY);
    if (epoch !== undefined)
      request.headers.set(MUTATION_EPOCH_HEADER, String(epoch));
    const pending = fixture.runner.fetch(request);
    try {
      await hold.entered.promise;
      expect(fixture.start).not.toHaveBeenCalled();
      expect(fixture.events).toEqual([]);
      request.headers.set(
        EXECUTION_PRINCIPAL_HEADER,
        encodeExecutionPrincipal(C_REPLACEMENT_PRINCIPAL),
      );
      request.headers.set(MUTATION_EPOCH_HEADER, '3');
    } finally {
      hold.release.resolve();
      await pending;
    }
    expect((await pending).status).toBe(200);
    expect(fixture.start).toHaveBeenCalledTimes(1);
    const options = fixture.start.mock.calls[0]?.[1];
    expect(options).toMatchObject({
      requestedBy: OWNER_PRINCIPAL.id,
      requestedByKind: OWNER_PRINCIPAL.kind,
      startIdentity: {
        owner: { kind: 'human', id: 'owner-1' },
        target: { kind: 'workflow', id: 'gated' },
      },
      runOwnerGuard: {
        owner: { kind: 'human', id: 'owner-1' },
        reservationToken: options?.attemptToken,
      },
    });
    expect(options?.mutationEpoch).toBe(epoch);
    expect(Object.hasOwn(options ?? {}, 'onPreparedStartIdentity')).toBe(true);
    expect(options?.onPreparedStartIdentity).toBeTypeOf('function');
  });

  it.each([
    undefined,
    2,
  ])('C preserves queued workflow authority through the operation FIFO: %s', async (epoch) => {
    const fixture = cWorkflowFixture();
    const hold = await cHoldWorkflowFifo(fixture);
    const request = post('/runs', C_WORKFLOW_BODY);
    if (epoch !== undefined)
      request.headers.set(MUTATION_EPOCH_HEADER, String(epoch));
    const body = vi.spyOn(request, 'json');
    const observation = cObserveFifo();
    const pending = fixture.runner.fetch(request);
    try {
      expect(
        await Promise.race([
          observation.enqueued.then(() => 'queued'),
          pending.then(() => 'response'),
        ]),
      ).toBe('queued');
      expect(observation.count()).toBe(1);
      expect(body).not.toHaveBeenCalled();
      expect(fixture.start).not.toHaveBeenCalled();
      request.headers.set(
        EXECUTION_PRINCIPAL_HEADER,
        encodeExecutionPrincipal(C_REPLACEMENT_PRINCIPAL),
      );
      request.headers.set(MUTATION_EPOCH_HEADER, '3');
    } finally {
      observation.restore();
      hold.release.resolve();
      await Promise.allSettled([hold.holder, pending]);
    }
    expect((await pending).status).toBe(200);
    expect(fixture.start.mock.calls[0]?.[1]).toMatchObject({
      requestedBy: 'owner-1',
      requestedByKind: 'human',
    });
    expect(fixture.start.mock.calls[0]?.[1].mutationEpoch).toBe(epoch);
    expect(body).toHaveBeenCalledTimes(1);
  });

  it.each([
    null,
    'invalid',
  ])('C refuses invalid workflow principal before joining the FIFO: %s', async (principal) => {
    const fixture = cWorkflowFixture();
    const hold = await cHoldWorkflowFifo(fixture);
    const request = post('/runs', C_WORKFLOW_BODY);
    if (principal === null) request.headers.delete(EXECUTION_PRINCIPAL_HEADER);
    else request.headers.set(EXECUTION_PRINCIPAL_HEADER, principal);
    const body = vi.spyOn(request, 'json');
    const observation = cObserveFifo();
    const pending = fixture.runner.fetch(request);
    try {
      expect(
        await Promise.race([
          pending.then(() => 'response'),
          observation.enqueued.then(() => 'queued'),
        ]),
        'refusal completes without joining the held FIFO',
      ).toBe('response');
      expect(observation.count()).toBe(0);
      expect(body).not.toHaveBeenCalled();
      expect(fixture.start).not.toHaveBeenCalled();
      expect(fixture.reserve).not.toHaveBeenCalled();
    } finally {
      observation.restore();
      hold.release.resolve();
      await Promise.allSettled([hold.holder, pending]);
    }
    expect((await pending).status).toBe(403);
    expect(await (await pending).json()).toEqual({
      error: 'run request carries no valid trusted execution principal',
    });
  });

  it.each([
    ['credential', 'wrong-secret', 'globex', 503, 'credential'],
    [
      'deployment',
      TEST_DEPLOYMENT_IDENTITY_SECRET,
      'globex',
      503,
      "belongs to 'globex'",
    ],
    [
      'epoch',
      TEST_DEPLOYMENT_IDENTITY_SECRET,
      'acme',
      400,
      'mutationEpoch must be a nonnegative safe integer or undefined',
    ],
  ] as const)('C workflow ingress refuses combined invalid authority before route: %s', async (_label, secret, tag, status, error) => {
    const fixture = cWorkflowFixture();
    fixture.env.DB = deploymentIdentityDatabase(tag);
    const request = deploymentIdentityRequest(
      'http://do/runs',
      { method: 'POST', body: JSON.stringify(C_WORKFLOW_BODY) },
      secret,
    );
    request.headers.set(MUTATION_EPOCH_HEADER, '01');
    request.headers.set(EXECUTION_PRINCIPAL_HEADER, 'invalid');
    const build = vi.spyOn(
      fixture.runner as unknown as { build: () => RunnerRuntime },
      'build',
    );
    const response = await fixture.runner.fetch(request);
    expect(response.status).toBe(status);
    const result = (await response.json()) as { error: string };
    expect(result.error).toContain(error);
    if (status === 400)
      expect(result).toEqual({
        error,
        reason: { code: 'INVALID_MUTATION_EPOCH' },
      });
    expect(build).not.toHaveBeenCalled();
    expect(fixture.events).toEqual([]);
    expect(fixture.start).not.toHaveBeenCalled();
    expect(fixture.reserve).not.toHaveBeenCalled();
  });

  it('C workflow status and liveness remain principal-free', async () => {
    const fixture = cWorkflowFixture();
    const started = await fixture.runner.fetch(post('/runs', C_WORKFLOW_BODY));
    expect(started.status).toBe(200);
    const response = await fixture.runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/c-run'),
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      runId: 'c-run',
      status: 'suspended',
    });
    await cWorkflowBarrier(fixture.runner);
  });

  it.each([
    'terminate',
    'deadline',
  ] as const)('C workflow %s uses the pre-verifier principal string', async (action) => {
    const fixture = cWorkflowFixture();
    const hold = cHoldWorkflowVerification(fixture.env);
    const cancel = vi
      .spyOn(fixture.runtime, 'cancelActiveExecution')
      .mockResolvedValue(false);
    const request = post(`/runs/gated/c-run/${action}`, {
      expectedRevision: 0,
      expectedDeadlineAt: null,
    });
    const pending = fixture.runner.fetch(request);
    try {
      await hold.entered.promise;
      request.headers.set(
        EXECUTION_PRINCIPAL_HEADER,
        'invalid-after-verification',
      );
    } finally {
      hold.release.resolve();
      await pending;
    }
    expect((await pending).status).not.toBe(403);
    if (action === 'terminate')
      expect(cancel.mock.calls[0]?.[3]?.[0]).toEqual(OWNER_PRINCIPAL);
  });
});

describe('DurableObjectRunner.fetch', () => {
  it.each([
    undefined,
    {},
    { 'app.attribution': { workspaceId: 'workspace-1', origin: 'api' } },
  ])('forwards ordinary start context to Runtime: %j', async (requestContext) => {
    const fixture = cWorkflowFixture();
    const response = await fixture.runner.fetch(
      post('/runs', { ...C_WORKFLOW_BODY, requestContext }),
    );

    expect(response.status).toBe(200);
    expect(fixture.start).toHaveBeenCalledOnce();
    const options = fixture.start.mock.calls[0]?.[1];
    if (requestContext === undefined) {
      expect(options).not.toHaveProperty('storedRequestContext');
    } else {
      expect(options?.storedRequestContext).toEqual(requestContext);
    }
    expect(options).toMatchObject({
      requestedBy: OWNER_PRINCIPAL.id,
      requestedByKind: OWNER_PRINCIPAL.kind,
      startIdentity: {
        owner: { kind: OWNER_PRINCIPAL.kind, id: OWNER_PRINCIPAL.id },
        target: { kind: 'workflow', id: 'gated' },
      },
    });
  });

  it('rejects a start without a trusted execution principal before runtime or ownership work', async () => {
    const reserve = vi.fn(async () => true);
    const runtime = {
      ...statusStub(async () => null),
      start: vi.fn(),
    } as unknown as RunnerRuntime;
    const runner = new TestRunner(undefined, {
      ...makeProductionEnv(testStorage(), {
        reserve,
        settle: vi.fn(async () => undefined),
      }),
      runtime,
    });

    const response = await runner.fetch(
      deploymentIdentityRequest('http://do/runs', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          workflowId: 'gated',
          runId: 'run-missing-principal',
          inputData: { topic: 't' },
        }),
      }),
    );

    expect(response.status).toBe(403);
    await expect(response.json()).resolves.toEqual({
      error: 'run request carries no valid trusted execution principal',
    });
    expect(reserve).not.toHaveBeenCalled();
    expect(runtime.status).not.toHaveBeenCalled();
    expect(runtime.start).not.toHaveBeenCalled();
  });

  it('uses a lifecycle-neutral missing-principal diagnostic on terminal routes', async () => {
    const runner = new TestRunner(undefined, makeProductionEnv());
    for (const path of [
      '/runs/gated/run-missing-principal/terminate',
      '/runs/gated/run-missing-principal/deadline',
    ]) {
      const response = await runner.fetch(
        deploymentIdentityRequest(`http://do${path}`, { method: 'POST' }),
      );

      expect(response.status).toBe(403);
      await expect(response.json()).resolves.toEqual({
        error: 'run request carries no valid trusted execution principal',
      });
    }
  });

  it('rejects a new resume leg without requester kind before runtime work', async () => {
    const runtime = {
      resume: vi.fn(),
      ...statusStub(async () => null),
    } as unknown as RunnerRuntime;
    const runner = new TestRunner(undefined, {
      ...makeProductionEnv(),
      runtime,
    });

    const response = await runner.fetch(
      deploymentIdentityRequest(
        'http://do/runs/gated/run-missing-kind/resume',
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            requestedBy: 'reviewer-1',
            resumeData: { approved: true },
          }),
        },
      ),
    );

    expect(response.status).toBe(400);
    expect(runtime.resume).not.toHaveBeenCalled();
  });

  it('accepts a resume requester exactly at the principal-id bound', async () => {
    const resume = vi.fn(
      async (
        _workflowId: string,
        runId: string,
        options: NonNullable<Parameters<RunnerRuntime['resume']>[2]>,
      ) => ({
        runId,
        status: 'success' as const,
        requestedBy: options.requestedBy,
        requestedByKind: options.requestedByKind,
      }),
    );
    const runner = new TestRunner(undefined, {
      ...makeProductionEnv(),
      runtime: { resume } as unknown as RunnerRuntime,
    });
    const requestedBy = 'r'.repeat(200);

    const response = await runner.fetch(
      post('/runs/gated/run-bounded-requester/resume', {
        requestedBy,
        requestedByKind: 'human',
      }),
    );

    expect(response.status).toBe(200);
    expect(resume).toHaveBeenCalledWith(
      'gated',
      'run-bounded-requester',
      expect.objectContaining({ requestedBy, requestedByKind: 'human' }),
    );
  });

  it.each([
    ['an overlong requester', 'r'.repeat(201)],
    ['an all-whitespace requester', ' '.repeat(200)],
    ['a control-bearing requester', 'reviewer\u000aforged'],
  ])('rejects a resume with %s before runtime work', async (_label, requestedBy) => {
    const resume = vi.fn();
    const runner = new TestRunner(undefined, {
      ...makeProductionEnv(),
      runtime: { resume } as unknown as RunnerRuntime,
    });

    const response = await runner.fetch(
      post('/runs/gated/run-invalid-requester/resume', {
        requestedBy,
        requestedByKind: 'human',
      }),
    );

    expect(response.status).toBe(400);
    expect(resume).not.toHaveBeenCalled();
  });

  it.each([
    'human',
    'service',
    'agent',
    'system',
  ] as const)("accepts the '%s' resume requester kind", async (requestedByKind) => {
    const resume = vi.fn(async () => ({
      runId: 'run-valid-requester-kind',
      status: 'success' as const,
    }));
    const runner = new TestRunner(undefined, {
      ...makeProductionEnv(),
      runtime: { resume } as unknown as RunnerRuntime,
    });

    const response = await runner.fetch(
      post('/runs/gated/run-valid-requester-kind/resume', {
        requestedBy: 'requester',
        requestedByKind,
      }),
    );

    expect(response.status).toBe(200);
    expect(resume).toHaveBeenCalledWith(
      'gated',
      'run-valid-requester-kind',
      expect.objectContaining({ requestedByKind }),
    );
  });

  it.each([
    'operator',
    '',
    1,
  ])('rejects an invalid resume requester kind (%s)', async (requestedByKind) => {
    const resume = vi.fn();
    const runner = new TestRunner(undefined, {
      ...makeProductionEnv(),
      runtime: { resume } as unknown as RunnerRuntime,
    });

    const response = await runner.fetch(
      post('/runs/gated/run-invalid-requester-kind/resume', {
        requestedBy: 'requester',
        requestedByKind,
      }),
    );

    expect(response.status).toBe(400);
    expect(resume).not.toHaveBeenCalled();
  });

  it('ignores forged start-body owner and requester fields in favor of the trusted principal', async () => {
    const env = makeProductionEnv();
    const runner = new TestRunner(undefined, env);

    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-forged-provenance',
        inputData: { topic: 't' },
        resourceOwner: { kind: 'system', id: 'forged-owner' },
        requestedBy: 'forged-requester',
        requestedByKind: 'system',
      }),
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      requestedBy: 'owner-1',
      requestedByKind: 'human',
    });
    await expect(
      env.owners.owner('run', 'run-forged-provenance'),
    ).resolves.toEqual({ kind: 'human', id: 'owner-1' });
  });

  it('binds a scheduled workflow run to the committed schedule owner and the header requester', async () => {
    const env = makeProductionEnv();
    const owners = new D1ResourceOwnershipStore(
      testDatabase(env.storage) as ResourceOwnershipDatabase,
    );
    const scheduleOwner = { kind: 'human' as const, id: 'schedule-owner' };
    await owners.claim('schedule', 'schedule-gated', scheduleOwner);
    env.owners = owners;
    env.schedules = preparedScheduleSource({
      scheduleId: 'schedule-gated',
      dispatchId: 'dispatch-gated',
      runId: 'run-scheduled-owner',
      target: {
        type: 'workflow',
        workflowId: 'gated',
        inputData: { topic: 'stored' },
      },
    });
    const runner = new TestRunner(undefined, env);

    const response = await runner.fetch(
      post(
        '/runs',
        {
          workflowId: 'gated',
          runId: 'run-scheduled-owner',
          scheduleId: 'schedule-gated',
          dispatchId: 'dispatch-gated',
          inputData: { topic: 'forged' },
        },
        {
          kind: 'system',
          id: 'schedule-runner',
          purpose: 'scheduled-workflow-execution',
        },
      ),
    );

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      requestedBy: 'schedule-runner',
      requestedByKind: 'system',
    });
    await expect(owners.owner('run', 'run-scheduled-owner')).resolves.toEqual(
      scheduleOwner,
    );
  });

  it.each([
    undefined,
    { source: 'stored-context' },
  ])('executes the prepared schedule target payload with context %j', async (requestContext) => {
    const owners = new InMemoryResourceOwnershipStore();
    await owners.claim('schedule', 'schedule-payload', {
      kind: 'human',
      id: 'schedule-owner',
    });
    const start = vi.fn(
      async (
        _workflowId: string,
        startOptions: Parameters<RunnerRuntime['start']>[1],
      ) => ({
        runId: startOptions.runId,
        status: 'success' as const,
        requestedBy: startOptions.requestedBy,
        requestedByKind: startOptions.requestedByKind,
      }),
    );
    const runtime = {
      start,
      ...statusStub(async () => null),
    } as unknown as RunnerRuntime;
    const env = makeProductionEnv();
    env.owners = owners;
    env.runtime = runtime;
    env.schedules = preparedScheduleSource({
      scheduleId: 'schedule-payload',
      dispatchId: 'dispatch-payload',
      runId: 'run-schedule-payload',
      target: {
        type: 'workflow',
        workflowId: 'gated',
        inputData: { topic: 'stored-input' },
        initialState: { phase: 'stored-state' },
        ...(requestContext === undefined ? {} : { requestContext }),
      },
    });
    const runner = new TestRunner(undefined, env);

    const response = await runner.fetch(
      post(
        '/runs',
        {
          workflowId: 'gated',
          runId: 'run-schedule-payload',
          scheduleId: 'schedule-payload',
          dispatchId: 'dispatch-payload',
          inputData: { topic: 'forged-input' },
          initialState: { phase: 'forged-state' },
          requestContext: { source: 'forged-context' },
        },
        {
          kind: 'system',
          id: 'schedule-runner',
          purpose: 'scheduled-workflow-execution',
        },
      ),
    );

    expect(response.status).toBe(200);
    expect(start).toHaveBeenCalledWith(
      'gated',
      expect.objectContaining({
        runId: 'run-schedule-payload',
        inputData: { topic: 'stored-input' },
        initialState: { phase: 'stored-state' },
        requestedBy: 'schedule-runner',
        requestedByKind: 'system',
      }),
    );
    const options = start.mock.calls[0]?.[1];
    if (requestContext === undefined) {
      expect(options).not.toHaveProperty('storedRequestContext');
    } else {
      expect(options?.storedRequestContext).toEqual(requestContext);
    }
  });

  it.each([
    ['missing dispatch id', { runId: 'run-authorized-trigger' }],
    [
      'different dispatch id',
      { runId: 'run-authorized-trigger', dispatchId: 'dispatch-borrowed' },
    ],
    [
      'different run id',
      { runId: 'run-borrowed-trigger', dispatchId: 'dispatch-authorized' },
    ],
  ] as const)('rejects a schedule id paired with a %s before runtime or ownership work', async (_label, attempted) => {
    const owners = new InMemoryResourceOwnershipStore();
    await owners.claim('schedule', 'schedule-trigger', {
      kind: 'human',
      id: 'schedule-owner',
    });
    const runtime = {
      start: vi.fn(),
      ...statusStub(async () => null),
    } as unknown as RunnerRuntime;
    const env = makeProductionEnv();
    env.owners = owners;
    env.runtime = runtime;
    env.schedules = preparedScheduleSource({
      scheduleId: 'schedule-trigger',
      dispatchId: 'dispatch-authorized',
      runId: 'run-authorized-trigger',
      target: { type: 'workflow', workflowId: 'gated' },
    });
    const runner = new TestRunner(undefined, env);

    const response = await runner.fetch(
      post(
        '/runs',
        {
          workflowId: 'gated',
          scheduleId: 'schedule-trigger',
          inputData: { topic: 'forged' },
          ...attempted,
        },
        {
          kind: 'system',
          id: 'schedule-runner',
          purpose: 'scheduled-workflow-execution',
        },
      ),
    );

    expect(response.status).toBe(400);
    expect(runtime.start).not.toHaveBeenCalled();
    await expect(owners.owner('run', attempted.runId)).resolves.toBeUndefined();
  });

  it.each([
    ['missing schedule row', undefined, true],
    [
      'mismatched workflow target',
      { type: 'workflow', workflowId: 'another-workflow' },
      true,
    ],
    [
      'pending schedule owner',
      { type: 'workflow', workflowId: 'gated' },
      false,
    ],
  ] as const)('rejects a scheduled start with a %s before creating the run', async (_label, stored, committed) => {
    const owners = new InMemoryResourceOwnershipStore();
    const scheduleOwner = { kind: 'human' as const, id: 'schedule-owner' };
    if (committed) {
      await owners.claim('schedule', 'schedule-source', scheduleOwner);
    } else {
      await owners.reserveAll(
        [{ kind: 'schedule', resourceId: 'schedule-source' }],
        scheduleOwner,
        'pending-schedule-token',
      );
    }
    const env = makeProductionEnv();
    env.owners = owners;
    env.schedules = preparedScheduleSource({
      scheduleId: 'schedule-source',
      dispatchId: 'dispatch-source',
      runId: 'run-invalid-schedule-source',
      target: stored,
    });
    const runner = new TestRunner(undefined, env);

    const response = await runner.fetch(
      post(
        '/runs',
        {
          workflowId: 'gated',
          runId: 'run-invalid-schedule-source',
          scheduleId: 'schedule-source',
          dispatchId: 'dispatch-source',
          inputData: { topic: 't' },
        },
        {
          kind: 'system',
          id: 'schedule-runner',
          purpose: 'scheduled-workflow-execution',
        },
      ),
    );

    expect(response.status).toBe(400);
    await expect(
      owners.owner('run', 'run-invalid-schedule-source'),
    ).resolves.toBeUndefined();
  });

  it('pre-arms owner recovery before deployment identity I/O', async () => {
    const events: string[] = [];
    const { state } = durableKeyValueStorageFixture(events);
    const identity = deploymentIdentityDatabase('globex');
    const env = makeProductionEnv();
    env.DB = {
      prepare(query: string) {
        events.push('identity');
        return identity.prepare(query);
      },
    } as TestEnv['DB'];
    const runner = new TestRunner(state, env);

    await expect(runner.alarm()).rejects.toThrow("belongs to 'globex'");

    expect(events[0]).toBe('setAlarm');
    // The watchdog is armed before the identity read and is the ONLY arm on
    // this path: re-arming after the failure would let a due suspension
    // deadline pull the next wake to the one-second floor, on a wake whose
    // deadline duty cannot run at all.
    expect(events.filter((event) => event === 'setAlarm')).toHaveLength(1);
    expect(events).not.toContain('deleteAlarm');
  });

  it('clears the prearmed watchdog when no recovery journal exists', async () => {
    const events: string[] = [];
    const { state } = durableKeyValueStorageFixture(events);
    const runner = new TestRunner(state, makeProductionEnv());

    await runner.alarm();

    expect(events[0]).toBe('setAlarm');
    expect(events.at(-1)).toBe('deleteAlarm');
  });

  it('arms recovery before reserving and commits the reservation after persistence', async () => {
    const events: string[] = [];
    const { state } = durableKeyValueStorageFixture(events);
    const reserve = vi.fn(async () => {
      events.push('reserve');
      return true;
    });
    const settle = vi.fn(async () => {
      events.push('settle');
    });
    const runner = new TestRunner(
      state,
      makeProductionEnv(testStorage(), { reserve, settle }),
    );

    const response = await runner.fetch(
      post(
        '/runs',
        {
          workflowId: 'gated',
          runId: 'run-recovery-order',
          inputData: { topic: 't' },
        },
        {
          kind: 'human',
          id: 'initiator',
          role: 'operator',
        },
      ),
    );

    expect(response.status).toBe(200);
    expect(events.indexOf('put:flowsafe:run-owner-recovery:v1')).toBeLessThan(
      events.indexOf('reserve'),
    );
    expect(settle).toHaveBeenCalledWith(
      expect.any(String),
      'run-recovery-order',
      false,
    );
    expect((await response.json()) as RunSummary).toMatchObject({
      status: 'suspended',
      requestedBy: 'initiator',
    });
  });

  it('journals nothing on a storage-less host and refuses to prepare the claim', async () => {
    // #given — a run object whose state carries no storage at all
    const runner = new TestRunner(undefined, makeProductionEnv(), false);

    // #when — a start walks the whole run-owner journal protocol
    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-storage-less',
        inputData: { topic: 't' },
      }),
    );

    // #then — both halves of the one absence policy: arming journaled nothing
    // and said nothing, so the start reached the prepared phase, which no wake
    // could recover from an unwritten journal and which refuses by name. The
    // message is pinned because a bare 500 is also what a storage without
    // alarms produces, and this case would pass while the prepared-phase guard
    // had stopped firing.
    expect(response.status).toBe(500);
    expect(await response.json()).toMatchObject({
      error: expect.stringContaining(
        'run owner recovery requires durable storage',
      ),
    });
  });

  it('returns a persisted start after a lost settlement receipt and clears recovery on retry', async () => {
    const events: string[] = [];
    const { state, values } = durableKeyValueStorageFixture(events);
    const env = makeProductionEnv();
    const committed = new D1ResourceOwnershipStore(
      testDatabase(env.storage) as ResourceOwnershipDatabase,
    );
    let loseReceipt = true;
    const owners: DurableObjectRunOwnershipStore = {
      owner: (kind, resourceId) => committed.owner(kind, resourceId),
      reserveAll: (claims, owner, token) =>
        committed.reserveAll(claims, owner, token),
      settleReservation: async (token, release) => {
        await committed.settleReservation(token, release);
        if (loseReceipt) {
          loseReceipt = false;
          throw new Error('settlement receipt lost');
        }
      },
    };
    env.owners = owners;
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      const response = await runner.fetch(
        post('/runs', {
          workflowId: 'gated',
          runId: 'run-lost-settlement-receipt',
          inputData: { topic: 't' },
        }),
      );

      expect(response.status).toBe(200);
      expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
      expect(
        await committed.owner('run', 'run-lost-settlement-receipt'),
      ).toEqual({ kind: 'human', id: 'owner-1' });

      await runner.alarm();

      expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
      expect(events.at(-1)).toBe('deleteAlarm');
      expect(
        await committed.owner('run', 'run-lost-settlement-receipt'),
      ).toEqual({ kind: 'human', id: 'owner-1' });
    } finally {
      log.mockRestore();
    }
  });

  it('rolls back only the attempt reservation when start has no snapshot', async () => {
    const { state } = durableKeyValueStorageFixture();
    const reserve = vi.fn(async () => true);
    const settle = vi.fn(async () => undefined);
    const runtime = {
      start: vi.fn(async () => {
        throw new Error('injected pre-snapshot failure');
      }),
      ...statusStub(async () => null),
      recoverStartAttempt: vi.fn(async () => null),
    } as unknown as RunnerRuntime;
    const runner = new TestRunner(state, {
      ...makeProductionEnv(testStorage(), { reserve, settle }),
      runtime,
    });

    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-no-snapshot',
        inputData: { topic: 't' },
      }),
    );

    expect(response.status).toBe(500);
    await runner.alarm();
    expect(settle).toHaveBeenCalledWith(
      expect.any(String),
      'run-no-snapshot',
      true,
    );
  });

  it('rolls back preparing bookkeeping without querying a failed Runtime reader', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const reserve = vi.fn(async () => true);
    const settle = vi.fn(async () => undefined);
    const runtime = {
      start: vi.fn(async () => {
        throw new Error('injected pre-snapshot failure');
      }),
      ...statusStub(async () => null),
      recoverStartAttempt: vi.fn(async () => {
        throw new RunStateUnreadableError('gated', 'run-blind-start');
      }),
    } as unknown as RunnerRuntime;
    const runner = new TestRunner(state, {
      ...makeProductionEnv(testStorage(), { reserve, settle }),
      runtime,
    });
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    const response = await runner
      .fetch(
        post('/runs', {
          workflowId: 'gated',
          runId: 'run-blind-start',
          inputData: { topic: 't' },
        }),
      )
      .finally(() => log.mockRestore());

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toMatchObject({
      error: 'injected pre-snapshot failure',
    });
    expect(logged).toEqual([]);
    expect(settle).toHaveBeenCalledWith(
      expect.any(String),
      'run-blind-start',
      true,
    );
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 60_000);
  });

  it('does not execute when the owner reservation conflicts', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const reserve = vi.fn(async () => false);
    const settle = vi.fn(async () => undefined);
    const env = makeProductionEnv(testStorage(), { reserve, settle });
    const runner = new TestRunner(state, env);

    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-owner-conflict',
        inputData: { topic: 't' },
      }),
    );

    expect(response.status).toBe(500);
    expect(
      (
        await runner.fetch(
          deploymentIdentityRequest('http://do/runs/gated/run-owner-conflict'),
        )
      ).status,
    ).toBe(404);
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    await runner.alarm();
    expect(settle).toHaveBeenCalledWith(
      expect.any(String),
      'run-owner-conflict',
      true,
    );
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
  });

  it('rejects a malformed stored recovery journal before touching ownership', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const settle = vi.fn(async () => undefined);
    values.set('flowsafe:run-owner-recovery:v1', {
      version: 2,
      phase: 'preparing',
      owner: { kind: 'human', id: 'owner-1' },
      workflowId: 'gated/forged',
      runId: 'run-recovery',
      token: 'attempt-token',
    });
    const runner = new TestRunner(
      state,
      makeProductionEnv(testStorage(), {
        reserve: vi.fn(async () => true),
        settle,
      }),
    );

    await expect(runner.alarm()).rejects.toThrow(
      'stored run owner recovery is malformed',
    );
    expect(settle).not.toHaveBeenCalled();
  });

  it('serializes dispatch status behind an in-flight start before declaring absence', async () => {
    let resolveStart: ((summary: RunSummary) => void) | undefined;
    let enteredStart: (() => void) | undefined;
    const entered = new Promise<void>((resolve) => {
      enteredStart = resolve;
    });
    let persisted: RunSummary | null = null;
    const runtime = {
      start: vi.fn(async () => {
        enteredStart?.();
        const summary = await new Promise<RunSummary>((resolve) => {
          resolveStart = resolve;
        });
        persisted = summary;
        return summary;
      }),
      ...statusStub(async () => persisted),
    } as unknown as RunnerRuntime;
    const runner = new TestRunner(undefined, {
      ...makeProductionEnv(),
      runtime,
    });
    const start = runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-dispatch-race',
        inputData: { topic: 't' },
      }),
    );
    await entered;

    let statusSettled = false;
    const status = runner
      .fetch(
        deploymentIdentityRequest(
          'http://do/runs/gated/run-dispatch-race/dispatch-status',
        ),
      )
      .finally(() => {
        statusSettled = true;
      });
    await Promise.resolve();
    expect(statusSettled).toBe(false);

    resolveStart?.({
      runId: 'run-dispatch-race',
      status: 'success',
      result: 'done',
    });
    expect((await start).status).toBe(200);
    const statusResponse = await status;
    expect(statusResponse.status).toBe(200);
    expect(await statusResponse.json()).toMatchObject({
      runId: 'run-dispatch-race',
      status: 'success',
    });
  });

  it('retains requester kind through eviction and status reconciliation', async () => {
    const { state } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    const before = new TestRunner(state, env);
    const started = await before.fetch(
      post(
        '/runs',
        {
          workflowId: 'gated',
          runId: 'run-origin-eviction',
          inputData: { topic: 't' },
        },
        {
          kind: 'system',
          id: 'schedule-system',
          purpose: 'scheduled-workflow-execution',
        },
      ),
    );
    expect(started.status).toBe(200);
    expect((await started.clone().json()) as RunSummary).toMatchObject({
      requestedBy: 'schedule-system',
      requestedByKind: 'system',
    });

    const after = new TestRunner(state, env);
    const status = await after.fetch(
      deploymentIdentityRequest('http://do/runs/gated/run-origin-eviction'),
    );

    const summary = (await status.json()) as RunSummary;
    expect(summary).toMatchObject({
      status: 'suspended',
      requestedBy: 'schedule-system',
      requestedByKind: 'system',
    });

    const approvalStore = new InMemoryApprovalStore();
    const filed = await reconcileApprovalsForSummary(
      // In-memory store, no database to fence against: the opt-out is written down
      // rather than defaulted — see ExecutionFenceWiring.
      new ApprovalService({ store: approvalStore, executionFence: 'none' }),
      'gated',
      summary,
      'approval-reconciler',
    );
    expect(filed).toHaveLength(1);
    expect(filed[0]).toMatchObject({
      requestedBy: 'schedule-system',
      requestedByKind: 'system',
    });
  });

  it('rejects a caller from another deployment before creating the named run', async () => {
    const state = {
      id: { name: 'gated:run-cross-script' },
    } as unknown as DurableObjectState;
    const runner = new TestRunner(state, makeProductionEnv());
    const body = {
      workflowId: 'gated',
      runId: 'run-cross-script',
      inputData: { topic: 't' },
    };

    const denied = await runner.fetch(
      deploymentIdentityRequest(
        'http://do/runs',
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(body),
        },
        'different-deployment-identity-secret',
      ),
    );
    expect(denied.status).toBe(503);

    const accepted = await runner.fetch(post('/runs', body));
    expect(accepted.status).toBe(200);
  });

  it('starts, reports, and resumes a run over the HTTP surface', async () => {
    // #given
    const runner = makeRunner();

    // #when — start (the Worker mints the runId; the DO never generates)
    const startResponse = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-http-1',
        inputData: { topic: 't' },
      }),
    );

    // #then
    expect(startResponse.status).toBe(200);
    const started = (await startResponse.json()) as RunSummary;
    expect(started.status).toBe('suspended');

    // #when — status
    const statusResponse = await runner.fetch(
      deploymentIdentityRequest(`http://do/runs/gated/${started.runId}`),
    );

    // #then
    expect(((await statusResponse.json()) as RunSummary).status).toBe(
      'suspended',
    );

    // #when — resume
    const resumeResponse = await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { approvedBy: 'bob' },
      }),
    );

    // #then
    expect(resumeResponse.status).toBe(200);
    const resumed = (await resumeResponse.json()) as RunSummary;
    expect(resumed.status).toBe('success');
    expect(resumed.result).toEqual({ topic: 't', approvedBy: 'bob' });
  });

  it('returns 400 when workflowId is missing from a start request', async () => {
    // #when
    const response = await makeRunner().fetch(post('/runs', { inputData: {} }));

    // #then
    expect(response.status).toBe(400);
  });

  it('returns 404 for an unknown workflow', async () => {
    // #when
    const response = await makeRunner().fetch(
      post('/runs', { workflowId: 'nope', runId: 'r-nope' }),
    );

    // #then
    expect(response.status).toBe(404);
  });

  it('returns 404 when resuming an unknown run', async () => {
    // #when
    const response = await makeRunner().fetch(
      post('/runs/gated/absent/resume', {}),
    );

    // #then
    expect(response.status).toBe(404);
  });

  it('returns 404 for unknown routes', async () => {
    // #when
    const response = await makeRunner().fetch(
      deploymentIdentityRequest('http://do/other'),
    );

    // #then
    expect(response.status).toBe(404);
  });

  it('returns 409 when resuming a run that already completed', async () => {
    // #given
    const runner = makeRunner();
    const started = await startGated(runner);
    await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { approvedBy: 'b' },
      }),
    );

    // #when
    const again = await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { approvedBy: 'b' },
      }),
    );

    // #then
    expect(again.status).toBe(409);
  });

  it('returns 409 when starting a run with an already-used runId', async () => {
    // #given
    const runner = makeRunner();
    await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'dup',
        inputData: { topic: 'x' },
      }),
    );

    // #when
    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'dup',
        inputData: { topic: 'y' },
      }),
    );

    // #then
    expect(response.status).toBe(409);
  });

  it('refuses to adopt an existing snapshot without a committed owner', async () => {
    const reserve = vi.fn(async () => true);
    const settle = vi.fn(async () => undefined);
    const runner = new TestRunner(
      undefined,
      makeProductionEnv(testStorage(), {
        reserve,
        settle,
        owner: async () => undefined,
      }),
    );
    await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'unowned-existing',
        inputData: { topic: 'x' },
      }),
    );

    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'unowned-existing',
        inputData: { topic: 'y' },
      }),
    );

    expect(response.status).toBe(500);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringContaining('no matching committed owner'),
    });
    expect(reserve).toHaveBeenCalledOnce();
  });

  it('returns 400 when the resume body fails the resume schema', async () => {
    // #given
    const runner = makeRunner();
    const started = await startGated(runner);

    // #when — an array body yields no resumeData, failing the zod schema
    const response = await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, [1, 2, 3]),
    );

    // #then — client error, not a 500
    expect(response.status).toBe(400);
  });

  it('serializes concurrent resumes: one 200, one 409', async () => {
    // #given
    const runner = makeRunner();
    const started = await startGated(runner);

    // #when — two racing HTTP resumes for the same run
    const [first, second] = await Promise.all([
      runner.fetch(
        post(`/runs/gated/${started.runId}/resume`, {
          step: 'gate',
          resumeData: { approvedBy: 'alice' },
        }),
      ),
      runner.fetch(
        post(`/runs/gated/${started.runId}/resume`, {
          step: 'gate',
          resumeData: { approvedBy: 'bob' },
        }),
      ),
    ]);

    // #then
    expect([first.status, second.status].sort()).toEqual([200, 409]);
  });

  it('returns 400 when starting with a non-path-safe runId', async () => {
    // #given — ids that would break the URL path, the ':'-joined DO name,
    // percent-encoding, or normalize away as dot-segments, leaving the run
    // unaddressable
    const runner = makeRunner();

    // #when / #then — rejected at the mint boundary, before any run exists
    for (const runId of ['a b', 'a/b', 'a%2Fb', 'a:b', '.', '..']) {
      const response = await runner.fetch(
        post('/runs', {
          workflowId: 'gated',
          runId,
          inputData: { topic: 't' },
        }),
      );
      expect(response.status).toBe(400);
    }
  });

  it('returns 400 when starting with a non-string runId', async () => {
    // #given — a JSON start body reaches runtime.start through an unchecked
    // `as` cast, so runId can be any JSON type at runtime. RegExp.test() coerces
    // its argument (123 → '123', ['run-1'] → 'run-1'), so without a type guard
    // these pass the pattern and mint a run keyed by the raw value —
    // unreachable by the string the URL path carries (reported repro:
    // POST {runId:123} → 200, then GET /runs/gated/123 → 404).
    const runner = makeRunner();

    // #when / #then — every supplied non-string is rejected before a run exists
    for (const runId of [123, true, ['run-1']]) {
      const response = await runner.fetch(
        post('/runs', {
          workflowId: 'gated',
          runId,
          inputData: { topic: 't' },
        }),
      );
      expect(response.status).toBe(400);
    }
  });

  it('400s a start without a runId — the DO never generates one', async () => {
    // #given — the runId is host-owned and minted by the run router. A DO-side
    // generation fallback would let any caller that skips the router create an
    // unowned run outside the request budget and audit boundary.
    const runner = makeRunner();

    // #when / #then — omitted and JSON-null both refuse
    for (const body of [
      { workflowId: 'gated', inputData: { topic: 't' } },
      { workflowId: 'gated', runId: null, inputData: { topic: 't' } },
    ]) {
      const response = await runner.fetch(post('/runs', body));
      expect(response.status).toBe(400);
    }
  });

  it('refuses to act outside its own identity when id.name is present', async () => {
    // #given — a runner whose DO identity names a DIFFERENT run than the
    // request. id.name is set by the trusted Worker via idFromName and is
    // unforgeable at this boundary, so a mismatch means someone routed
    // around the name join.
    const state = {
      id: { name: 'gated:run-A' },
    } as unknown as DurableObjectState;
    const runner = new TestRunner(state, makeProductionEnv());

    // #when — the request names run-B on the instance whose identity is run-A
    const mismatched = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-B',
        inputData: { topic: 't' },
      }),
    );

    // #then — refused loudly; and the matching id works normally
    expect(mismatched.status).toBe(500);
    expect(
      (((await mismatched.json()) as { error?: string }).error ?? '').includes(
        'identity mismatch',
      ),
    ).toBe(true);
    const matching = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-A',
        inputData: { topic: 't' },
      }),
    );
    expect(matching.status).toBe(200);

    // #then — status and resume enforce the same identity
    expect(
      (
        await runner.fetch(
          deploymentIdentityRequest('http://do/runs/gated/run-B'),
        )
      ).status,
    ).toBe(500);
    expect(
      (await runner.fetch(post('/runs/gated/run-B/resume', {}))).status,
    ).toBe(500);
  });

  it('reads the snapshot-backed resume ordinal after a DO instance swap', async () => {
    // #given — a runner whose D1-shaped storage survives instance replacement.
    // The gate re-suspends on a falsy resume, so snapshot provenance accrues a
    // resume ordinal.
    class ResuspendRunner extends DurableObjectRunner<TestEnv> {
      protected runOwnership(
        runtimeEnv: TestEnv,
      ): DurableObjectRunOwnershipStore {
        return runtimeEnv.owners;
      }

      protected build(runtimeEnv: TestEnv): RunnerRuntime {
        const { createWorkflow, createStep, runtime } = init(
          { storage: runtimeEnv.storage },
          {
            executionFence:
              runtimeEnv.fence ?? newTestExecutionFence(runtimeEnv.storage),
            startIdempotency: newTestStartIdempotency(runtimeEnv.storage),
          },
        );
        const gate = createStep({
          id: 'gate',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          suspendSchema: z.object({ reason: z.string() }),
          execute: async ({ resumeData, suspend }) =>
            resumeData ? {} : suspend({ reason: 'wait' }),
        });
        createWorkflow({
          id: 'resuspend',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
        })
          .then(gate)
          .commit();
        return runtime;
      }
    }
    // Object-local storage for run-owner recovery. Resume provenance lives in
    // env.storage, not here.
    const { state } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();

    // #given — instance A re-suspends the run once (falsy resume)
    const before = new ResuspendRunner(state, env);
    const startResponse = await before.fetch(
      post('/runs', {
        workflowId: 'resuspend',
        runId: 'run-provenance-1',
        inputData: {},
      }),
    );
    const started = (await startResponse.json()) as RunSummary;
    expect(started.status).toBe('suspended');
    const reSuspendResponse = await before.fetch(
      post(`/runs/resuspend/${started.runId}/resume`, { step: 'gate' }),
    );
    const reSuspended = (await reSuspendResponse.json()) as RunSummary;
    expect(reSuspended.status).toBe('suspended');
    expect(reSuspended.resumeCount?.gate).toBe(1);

    // #when — "eviction": a new runner instance reads the same D1-shaped
    // snapshot storage and projects the run's status
    const after = new ResuspendRunner(state, env);
    const statusResponse = await after.fetch(
      deploymentIdentityRequest(`http://do/runs/resuspend/${started.runId}`),
    );
    const status = (await statusResponse.json()) as RunSummary;

    // #then — the ordinal came back from persisted snapshot provenance
    expect(status.status).toBe('suspended');
    expect(status.resumeCount?.gate).toBe(1);

    // #then — the swapped instance completes the run normally
    const doneResponse = await after.fetch(
      post(`/runs/resuspend/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { go: true },
      }),
    );
    expect(((await doneResponse.json()) as RunSummary).status).toBe('success');
  });

  it('round-trips a path-safe custom runId over status and resume', async () => {
    // #given — a caller-supplied URL-safe id must stay addressable end-to-end
    const runner = makeRunner();
    const startResponse = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-1',
        inputData: { topic: 't' },
      }),
    );
    const started = (await startResponse.json()) as RunSummary;
    expect(started.runId).toBe('run-1');

    // #when — address the run by its custom id
    const statusResponse = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/run-1'),
    );
    const resumeResponse = await runner.fetch(
      post('/runs/gated/run-1/resume', {
        step: 'gate',
        resumeData: { approvedBy: 'bob' },
      }),
    );

    // #then
    expect(((await statusResponse.json()) as RunSummary).status).toBe(
      'suspended',
    );
    expect(((await resumeResponse.json()) as RunSummary).status).toBe(
      'success',
    );
  });

  it('persists cancellation before cleanup and authorizes only the original principal on replay', async () => {
    const storage = testStorage();
    const abandonApprovals = vi.fn(async () => undefined);
    const env = makeProductionEnv(storage);
    env.lifecycle = { abandonApprovals };
    const ownership = env.owners;
    const originalRelease = ownership.release?.bind(ownership);
    if (!originalRelease) throw new Error('release hook missing');
    const release = vi
      .spyOn(ownership, 'release')
      .mockImplementation(async (kind, runId, owner) => {
        const snapshot = await (
          await storage.getStore('workflows')
        )?.loadWorkflowSnapshot({ workflowName: 'gated', runId });
        expect(snapshot?.status).toBe('cancelled');
        return originalRelease(kind, runId, owner);
      });
    const runner = new TestRunner(undefined, env);
    await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'terminate-route',
        inputData: { topic: 'cancel' },
      }),
    );

    const first = await runner.fetch(
      post('/runs/gated/terminate-route/terminate', {}),
    );
    expect(first.status).toBe(200);
    const firstSummary = (await first.json()) as RunSummary;
    expect(firstSummary).toEqual({
      runId: 'terminate-route',
      status: 'cancelled',
      requestedBy: 'owner-1',
      requestedByKind: 'human',
      createdAt: expect.any(String),
      updatedAt: expect.any(String),
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    });
    expect(release).toHaveBeenCalledOnce();
    await expect(
      env.owners.owner('run', 'terminate-route'),
    ).resolves.toBeUndefined();

    const replay = await runner.fetch(
      post('/runs/gated/terminate-route/terminate-replay', {}),
    );
    expect(replay.status).toBe(200);
    expect(await replay.json()).toEqual(firstSummary);
    expect(release).toHaveBeenCalledOnce();
    expect(abandonApprovals).toHaveBeenCalledOnce();

    const stranger = await runner.fetch(
      post(
        '/runs/gated/terminate-route/terminate-replay',
        {},
        {
          kind: 'human',
          id: 'stranger',
          role: 'operator',
        },
      ),
    );
    expect(stranger.status).toBe(404);
  });

  it('returns a structured 409 and retains ownership for a persisted disputed settlement', async () => {
    const storage = testStorage();
    const env = makeProductionEnv(storage);
    const runner = new TestRunner(undefined, env);
    await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'disputed-route',
        inputData: { topic: 'held' },
      }),
    );
    const workflows = await storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'disputed-route',
    });
    if (!workflows || !snapshot) throw new Error('snapshot missing');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'disputed-route',
      snapshot: {
        ...snapshot,
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runLifecycle': {
            version: 1,
            revision: 1,
            economicOperations: [
              { id: 'charge-1', settlementState: 'disputed' },
            ],
          },
        },
      },
    });

    const response = await runner.fetch(
      post('/runs/gated/disputed-route/terminate', {}),
    );
    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({
      error:
        'run termination is blocked while an economic operation is disputed',
      reason: {
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      },
    });
    await expect(env.owners.owner('run', 'disputed-route')).resolves.toEqual({
      kind: 'human',
      id: 'owner-1',
    });
  });

  it('re-drives deadline cleanup after a crash between ownership release and cleanup completion', async () => {
    const storage = testStorage();
    const runtime = gatedRuntime(storage);
    const complete = runtime.completeTerminalCleanup.bind(runtime);
    let wedge = true;
    const env = makeProductionEnv(storage);
    env.runtime = new Proxy(runtime, {
      get(target, property) {
        if (property === 'completeTerminalCleanup') {
          return async (
            ...args: Parameters<RunnerRuntime['completeTerminalCleanup']>
          ) => {
            if (wedge) {
              wedge = false;
              throw new Error('cleanup completion wedged');
            }
            return complete(...args);
          };
        }
        // Receiver is the TARGET, not the proxy: RunnerRuntime's accessors read
        // private fields, and a private-field read against a proxy receiver
        // throws. Every function is re-bound to the target below for the same
        // reason, so this only makes the getters agree with the methods.
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    env.lifecycle = { abandonApprovals: async () => undefined };
    const runner = new TestRunner(undefined, env);
    const start = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'deadline-cleanup-wedge',
        inputData: { topic: 'expired' },
        deadlineMs: 0,
      }),
    );
    expect(start.status).toBe(200);
    const workflows = await storage.getStore('workflows');
    const before = await workflows?.loadWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'deadline-cleanup-wedge',
    });
    const lifecycle = before?.requestContext?.['flowsafe.runLifecycle'] as
      | { revision: number; deadlineAt: number }
      | undefined;
    if (!lifecycle) throw new Error('deadline lifecycle missing');
    const requestBody = {
      expectedRevision: lifecycle.revision,
      expectedDeadlineAt: lifecycle.deadlineAt,
    };

    const interrupted = await runner.fetch(
      post('/runs/gated/deadline-cleanup-wedge/deadline', requestBody, {
        kind: 'system',
        id: 'maintenance',
        purpose: 'run-deadline-maintenance',
      }),
    );
    expect(interrupted.status).toBe(500);
    await expect(
      env.owners.owner('run', 'deadline-cleanup-wedge'),
    ).resolves.toBeUndefined();

    const resumed = await runner.fetch(
      post('/runs/gated/deadline-cleanup-wedge/deadline', requestBody, {
        kind: 'system',
        id: 'maintenance',
        purpose: 'run-deadline-maintenance',
      }),
    );
    expect(resumed.status).toBe(200);
    expect(await resumed.json()).toMatchObject({
      status: 'timed_out',
      errorEnvelope: { code: 'TIMED_OUT' },
    });
    const after = await workflows?.loadWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'deadline-cleanup-wedge',
    });
    expect(
      (
        after?.requestContext?.['flowsafe.runLifecycle'] as {
          terminal?: { cleanupCompletedAt?: number };
        }
      )?.terminal?.cleanupCompletedAt,
    ).toEqual(expect.any(Number));
  });

  it('replays a post-intent core-canceled deadline from the scanner through a fresh owner object', async () => {
    const storage = testStorage();
    const env = makeProductionEnv(storage);
    const original = new TestRunner(undefined, env);
    const started = await original.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'deadline-intent-eviction',
        inputData: { topic: 'expired' },
        deadlineMs: 0,
      }),
    );
    expect(started.status).toBe(200);
    const workflows = await storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'deadline-intent-eviction',
    });
    const lifecycle = snapshot?.requestContext?.['flowsafe.runLifecycle'] as
      | { revision: number; deadlineAt: number }
      | undefined;
    if (!workflows || !snapshot || !lifecycle) {
      throw new Error('deadline snapshot missing');
    }
    const precursor = {
      ...snapshot,
      status: 'canceled' as const,
      requestContext: {
        ...snapshot.requestContext,
        'flowsafe.runLifecycle': {
          ...lifecycle,
          revision: lifecycle.revision + 1,
          transitionIntent: {
            status: 'timed_out' as const,
            requestedAt: Date.now(),
            replayPrincipals: [{ kind: 'system' as const, id: 'maintenance' }],
            expectedRevision: lifecycle.revision,
            expectedDeadlineAt: lifecycle.deadlineAt,
          },
        },
      },
    };
    await workflows.persistWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'deadline-intent-eviction',
      snapshot: precursor,
    });

    const sqlite = openSqlite();
    sqlite
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
    const iso = new Date().toISOString();
    sqlite
      .prepare(
        `INSERT INTO mastra_workflow_snapshot
       (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
       VALUES (?, ?, NULL, ?, ?, ?)`,
      )
      .run(
        'gated',
        'deadline-intent-eviction',
        JSON.stringify(precursor),
        iso,
        iso,
      );

    env.runtime = gatedRuntime(storage);
    const evicted = new TestRunner(undefined, env);
    const namespace: RunnerNamespaceLike<string> = {
      idFromName: (name) => name,
      get: () => ({
        fetch: (url, requestInit) =>
          evicted.fetch(new Request(url, requestInit as RequestInit)),
      }),
    };
    const topology = createDoRunTopology(
      namespace,
      TEST_DEPLOYMENT_IDENTITY_SECRET,
    );
    let emittedRevision: number | undefined;
    await expect(
      sweepExpiredRunDeadlines(sqliteUnitDatabase(sqlite) as SnapshotDatabase, {
        now: () => Date.now(),
        transition: async (candidate) => {
          emittedRevision = candidate.revision;
          await topology.timeOut(
            candidate.workflowId,
            candidate.runId,
            {
              expectedRevision: candidate.revision,
              expectedDeadlineAt: candidate.deadlineAt,
            },
            {
              kind: 'system',
              id: 'maintenance',
              purpose: 'run-deadline-maintenance',
            },
          );
        },
      }),
    ).resolves.toBe(1);
    expect(emittedRevision).toBe(lifecycle.revision);
    await expect(
      env.runtime.status('gated', 'deadline-intent-eviction'),
    ).resolves.toMatchObject({
      status: 'timed_out',
      errorEnvelope: { code: 'TIMED_OUT' },
    });
    await expect(
      env.owners.owner('run', 'deadline-intent-eviction'),
    ).resolves.toBeUndefined();
    const terminal = await workflows.loadWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'deadline-intent-eviction',
    });
    expect(
      (
        terminal?.requestContext?.['flowsafe.runLifecycle'] as {
          terminal?: { cleanupCompletedAt?: number };
        }
      ).terminal?.cleanupCompletedAt,
    ).toEqual(expect.any(Number));
  });

  it('returns a 426 non-WS fallback on the stream route when the runtime has no hibernation API', async () => {
    // #given — a node runner (state undefined ⇒ no acceptWebSocket). The per-run
    // WS stream is workerd-only; off workerd it must degrade, never 500. The
    // WebSocket runtime behavior itself is proven by the workerd spike.
    const runner = makeRunner();

    // #when — a websocket upgrade attempt on the stream route
    const response = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/run-1/stream', {
        headers: { Upgrade: 'websocket' },
      }),
    );

    // #then — 426 Upgrade Required (poll GET /runs/:wf/:runId instead)
    expect(response.status).toBe(426);
  });

  it('broadcasts the authoritative RunSummary to run-channel sockets after start and resume', async () => {
    // #given — a runner whose DO exposes a hibernatable-socket stub;
    // #broadcastRunSummary reads getWebSockets() and send()s each the frame.
    const sent: string[] = [];
    const socket = {
      send: (data: string) => sent.push(data),
    };
    const state = {
      id: { name: 'gated:run-A' },
      getWebSockets: () => [socket],
    } as unknown as DurableObjectState;
    const runner = new TestRunner(state, makeProductionEnv());

    // #when — start
    const startResponse = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-A',
        inputData: { topic: 't' },
      }),
    );

    // #then — one broadcast frame carrying the suspended summary
    expect(startResponse.status).toBe(200);
    expect(sent).toHaveLength(1);
    const startFrame = JSON.parse(sent[0] ?? '{}') as {
      type: string;
      summary: RunSummary;
    };
    expect(startFrame.type).toBe('run');
    expect(startFrame.summary.status).toBe('suspended');

    // #when — resume to completion
    const resumeResponse = await runner.fetch(
      post('/runs/gated/run-A/resume', {
        step: 'gate',
        resumeData: { approvedBy: 'bob' },
      }),
    );

    // #then — a second broadcast carrying the success summary
    expect(resumeResponse.status).toBe(200);
    expect(sent).toHaveLength(2);
    const resumeFrame = JSON.parse(sent[1] ?? '{}') as {
      type: string;
      summary: RunSummary;
    };
    expect(resumeFrame.type).toBe('run');
    expect(resumeFrame.summary.status).toBe('success');
  });
});

const TIMED_DEADLINE_MS = 900_000;
const RETRY_DEADLINE_MS = 1_800_000;
const DOTTED_STEP = 'wait.signal';

// The one execute shape every single-step timed workflow shares, so the
// fixtures below pass the behaviour and nothing else.
type TimedStepExecute = ExecuteFunction<
  unknown,
  { topic: string },
  { topic: string; settledBy: string },
  unknown,
  unknown,
  DefaultEngineType
>;

// A gate that arms a deadline on its suspension and reports which kind of
// resume woke it, so the DO tests below can prove the timeout envelope reached
// the step rather than only that a resume happened. `onSettle` counts the
// settling gates' post-suspension executions for countedTimedRuntime.
function timedRuntime(
  storage: MastraCompositeStore,
  onSettle?: () => void,
  executionFence: ExecutionFenceStore = newTestExecutionFence(storage),
): RunnerRuntime {
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    { executionFence, startIdempotency: newTestStartIdempotency(storage) },
  );
  const timedStep = (id: string, execute: TimedStepExecute) =>
    createStep({
      id,
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string(), settledBy: z.string() }),
      execute,
    });
  const singleStepWorkflow = (
    id: string,
    step: ReturnType<typeof timedStep>,
  ): void => {
    createWorkflow({
      id,
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string(), settledBy: z.string() }),
    })
      .then(step)
      .commit();
  };
  const settling: TimedStepExecute = async ({
    inputData,
    resumeData,
    suspend,
  }) => {
    if (!resumeData) {
      return suspend({
        reason: 'awaiting approval',
        [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS,
      });
    }
    onSettle?.();
    return {
      topic: inputData.topic,
      settledBy: isSuspensionTimeoutResumeData(resumeData)
        ? 'timeout'
        : 'signal',
    };
  };
  const relaying =
    (firstDeadlineMs: number, resumedDeadlineMs: number): TimedStepExecute =>
    async ({ resumeData, suspend }) =>
      suspend({
        reason: resumeData ? 'awaiting the next signal' : 'awaiting approval',
        [SUSPENSION_DEADLINE_PAYLOAD_KEY]: resumeData
          ? resumedDeadlineMs
          : firstDeadlineMs,
      });
  singleStepWorkflow('timed', timedStep('gate', settling));
  singleStepWorkflow(
    'timed-escalating',
    timedStep('gate', async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({
          reason: 'awaiting approval',
          [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS,
        });
      }
      if (isSuspensionTimeoutResumeData(resumeData)) {
        return suspend({
          reason: 'escalated after timeout',
          [SUSPENSION_DEADLINE_PAYLOAD_KEY]: RETRY_DEADLINE_MS,
        });
      }
      return { topic: inputData.topic, settledBy: 'signal' };
    }),
  );
  // A TOP-LEVEL step id that contains a dot. The rehydrated projection the
  // alarm reads splits the stored key on every dot, so this is the id whose
  // entry the wake has to recognize from [['wait','signal']].
  singleStepWorkflow('timed-dotted', timedStep(DOTTED_STEP, settling));
  // Arms a value below MIN_SUSPENSION_DEADLINE_MS on every suspension, so each
  // lifecycle boundary re-derives the identical rejection.
  singleStepWorkflow(
    'timed-unarmable',
    timedStep('gate', async ({ suspend }) =>
      suspend({
        reason: 'awaiting approval',
        [SUSPENSION_DEADLINE_PAYLOAD_KEY]: MIN_SUSPENSION_DEADLINE_MS - 1,
      }),
    ),
  );
  // Suspends again on every resume, so a RESUME boundary — not only a start —
  // can be the first one this run ever has a deadline to arm.
  singleStepWorkflow(
    'timed-relay',
    timedStep('gate', relaying(TIMED_DEADLINE_MS, RETRY_DEADLINE_MS)),
  );
  // The reverse relay: the re-suspension's deadline is SHORTER than the first
  // one's, so a stale record left by a failed resume-boundary write points at
  // a far-future time while the current suspension is due much sooner.
  singleStepWorkflow(
    'timed-relay-shortening',
    timedStep('gate', relaying(RETRY_DEADLINE_MS, TIMED_DEADLINE_MS)),
  );
  return runtime;
}

// A top-level step id containing a dot, alongside a nested workflow whose inner
// step makes the SAME dot-joined key: 'a.b' as a step id, and 'a' > 'b' as a
// path. Mastra keys its snapshot namespace by that joined path, so the two
// suspensions are indistinguishable there, and an entry armed for one of them
// could resume the other.
function collidingRuntime(storage: MastraCompositeStore): {
  runtime: RunnerRuntime;
  settled: () => string[];
} {
  const settled: string[] = [];
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    {
      executionFence: newTestExecutionFence(storage),
      startIdempotency: newTestStartIdempotency(storage),
    },
  );
  const suspending = (id: string, label: string) =>
    createStep({
      id,
      inputSchema: z.looseObject({}),
      outputSchema: z.looseObject({}),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) {
          return suspend({
            reason: 'awaiting approval',
            [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS,
          });
        }
        settled.push(label);
        return { settledBy: label };
      },
    });
  const inner = createWorkflow({
    id: 'a',
    inputSchema: z.looseObject({}),
    outputSchema: z.looseObject({}),
  })
    .then(suspending('b', 'nested'))
    .commit();
  createWorkflow({
    id: 'timed-collision',
    inputSchema: z.looseObject({}),
    outputSchema: z.looseObject({}),
  })
    .parallel([suspending('a.b', 'top-level'), inner])
    .commit();
  return { runtime, settled: () => settled };
}

// A `foreach` whose every iteration arms a deadline, recording the iterations a
// timeout resume actually reached. Mastra reports the whole foreach as ONE
// suspended path with one fence, so bounded work per wake has to be judged on
// the iterations rather than on the path.
function foreachRuntime(
  storage: MastraCompositeStore,
  workflowId: string,
  options?: { concurrency: number },
): { runtime: RunnerRuntime; timedOut: () => number[] } {
  const timedOut: number[] = [];
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    {
      executionFence: newTestExecutionFence(storage),
      startIdempotency: newTestStartIdempotency(storage),
    },
  );
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({ item: z.number() }),
    outputSchema: z.object({ item: z.number(), settledBy: z.string() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({
          reason: 'awaiting approval',
          // Concurrent iterations each ask for a DIFFERENT deadline, so the
          // claim that only the first suspended iteration's value is read per
          // batch is measured rather than assumed. A sequential foreach arms
          // one iteration at a time and needs no such spread.
          [SUSPENSION_DEADLINE_PAYLOAD_KEY]:
            TIMED_DEADLINE_MS + (options ? inputData.item * 60_000 : 0),
        });
      }
      if (isSuspensionTimeoutResumeData(resumeData)) {
        timedOut.push(inputData.item);
      }
      return { item: inputData.item, settledBy: 'timeout' };
    },
  });
  const workflow = createWorkflow({
    id: workflowId,
    inputSchema: z.array(z.object({ item: z.number() })),
    outputSchema: z.array(
      z.object({ item: z.number(), settledBy: z.string() }),
    ),
  });
  (options ? workflow.foreach(gate, options) : workflow.foreach(gate)).commit();
  return { runtime, timedOut: () => timedOut };
}

function timedEnv(): TestEnv {
  const env = makeProductionEnv();
  env.runtime = timedRuntime(env.storage, undefined, env.fence);
  return env;
}

// The same `timed` gate, counting how often its post-suspension body ran, so a
// race between a wake and a real signal can be judged on the one thing that
// matters: the gated action must not execute twice.
function countedTimedRuntime(storage: MastraCompositeStore): {
  runtime: RunnerRuntime;
  settled: () => number;
} {
  let settled = 0;
  return {
    runtime: timedRuntime(storage, () => {
      settled += 1;
    }),
    settled: () => settled,
  };
}

function storedDeadlines(
  values: Map<string, unknown>,
): SuspensionDeadlineRecord | undefined {
  return values.get(SUSPENSION_DEADLINE_STORAGE_KEY) as
    | SuspensionDeadlineRecord
    | undefined;
}

/** One stored entry by step, for a record asserted entry by entry. */
function storedEntry(
  values: Map<string, unknown>,
  step: string,
): SuspensionDeadlineEntry | undefined {
  return storedDeadlines(values)?.entries.find((entry) => entry.step === step);
}

/** Rewind every armed deadline into the past — the wake condition, no clock. */
function elapseDeadlines(
  values: Map<string, unknown>,
  entry: Partial<SuspensionDeadlineEntry> = {},
): void {
  const stored = storedDeadlines(values);
  if (!stored) throw new Error('no suspension deadline is armed');
  values.set(SUSPENSION_DEADLINE_STORAGE_KEY, {
    ...stored,
    entries: stored.entries.map((current) => ({
      ...current,
      deadlineAt: Date.now() - 1,
      ...entry,
    })),
  });
}

async function startTimed(
  runner: TestRunner,
  runId: string,
  workflowId = 'timed',
): Promise<RunSummary> {
  const response = await runner.fetch(
    post('/runs', { workflowId, runId, inputData: { topic: 't' } }),
  );
  if (response.status !== 200) {
    throw new Error(`start failed: ${await response.text()}`);
  }
  return (await response.json()) as RunSummary;
}

function suspendedFence(runId: string, suspendedAt: number): RunSummary {
  return {
    runId,
    status: 'suspended',
    suspended: [['gate']],
    suspendPayload: {
      gate: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
    },
    suspendedAt: { gate: suspendedAt },
  };
}

/** The entry a boundary derives from suspendedFence's shape of suspension. */
function armedEntry(
  step: string,
  suspendedAt: number,
): SuspensionDeadlineEntry {
  return {
    step,
    deadlineAt: suspendedAt + TIMED_DEADLINE_MS,
    suspendedAt,
    resumeCount: 0,
  };
}

/** Seed the stored deadline record exactly as a prior boundary would have. */
function seedDeadlines(
  values: Map<string, unknown>,
  runId: string,
  entries: SuspensionDeadlineEntry[],
  workflowId = 'timed',
): void {
  values.set(SUSPENSION_DEADLINE_STORAGE_KEY, {
    version: 1,
    workflowId,
    runId,
    entries,
  });
}

/**
 * A DurableObjectState over `storage` whose put fails for the deadline key
 * only. `once` lets the first matching write fail and the rest pass; `armed:
 * false` starts with writes passing until fail() arms the failure; stop()
 * ends a persistent failure.
 */
function deadlineWriteFailures(
  storage: DurableKeyValueStorage,
  options: { name?: string; once?: boolean; armed?: boolean } = {},
): { state: DurableObjectState; fail: () => void; stop: () => void } {
  let failing = options.armed ?? true;
  const state = {
    ...(options.name === undefined ? {} : { id: { name: options.name } }),
    storage: {
      ...storage,
      put: async (key: string, value: unknown) => {
        if (failing && key === SUSPENSION_DEADLINE_STORAGE_KEY) {
          if (options.once) failing = false;
          throw new Error('injected deadline write failure');
        }
        return storage.put(key, value);
      },
    },
  } as unknown as DurableObjectState;
  return {
    state,
    fail: () => {
      failing = true;
    },
    stop: () => {
      failing = false;
    },
  };
}

/**
 * Blind the workflows store's row read — the exact seam Mastra falls back from
 * — and hand back the restore. Deliberately NOT the Workflow method: stubbing
 * that would FABRICATE the degraded state, and the point of the tests below is
 * that a REAL suspended run held by a REAL runtime produces it. File-local
 * rather than shared: runtime.test.ts keeps its own copy beside its own
 * fixtures, which is cheaper than a shared module for eight lines.
 */
async function blindWorkflowRow(
  storage: MastraCompositeStore,
): Promise<() => void> {
  const store = (await storage.getStore('workflows')) as unknown as {
    getWorkflowRunById: (args: unknown) => Promise<unknown>;
  };
  const domain = store as unknown as FencedWorkflowsStorageD1;
  const capability = domain[FENCED_WORKFLOW_STORAGE];
  if (capability)
    Object.defineProperty(domain, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...capability,
        readSnapshot: async () => {
          throw new Error('selected workflow row unavailable');
        },
      },
      configurable: true,
    });
  const original = store.getWorkflowRunById;
  store.getWorkflowRunById = async () => null;
  return () => {
    store.getWorkflowRunById = original;
    if (capability)
      Object.defineProperty(domain, FENCED_WORKFLOW_STORAGE, {
        value: capability,
        configurable: true,
      });
  };
}

/** Count the real row deletions a wake performs, and restore the store. */
async function countRowDeletes(
  storage: MastraCompositeStore,
): Promise<{ calls: () => number; restore: () => void }> {
  const store = (await storage.getStore('workflows')) as unknown as {
    deleteWorkflowRunById: (args: unknown) => Promise<unknown>;
  };
  const original = store.deleteWorkflowRunById;
  let calls = 0;
  store.deleteWorkflowRunById = async (args: unknown) => {
    calls += 1;
    return original.call(store, args);
  };
  return {
    calls: () => calls,
    restore: () => {
      store.deleteWorkflowRunById = original;
    },
  };
}

describe('DurableObjectRunner suspension deadlines', () => {
  it('arms the record and the alarm at the suspension fence plus the deadline', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const runner = new TestRunner(state, timedEnv());

    const started = await startTimed(runner, 'run-armed');

    const suspendedAt = started.suspendedAt?.gate as number;
    expect(started.status).toBe('suspended');
    expect(storedDeadlines(values)).toEqual({
      version: 1,
      workflowId: 'timed',
      runId: 'run-armed',
      entries: [
        {
          step: 'gate',
          deadlineAt: suspendedAt + TIMED_DEADLINE_MS,
          suspendedAt,
          resumeCount: 0,
        },
      ],
    });
    expect(alarms.at(-1)).toBe(suspendedAt + TIMED_DEADLINE_MS);
  });

  it('arms nothing for a suspension without the reserved key', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const runner = new TestRunner(state, makeProductionEnv());

    await startGated(runner);

    expect(storedDeadlines(values)).toBeUndefined();
    expect(alarms).not.toHaveLength(0);
  });

  it('keeps the alarm armed on a wake with no recovery journal but a pending deadline', async () => {
    const events: string[] = [];
    const { state, values, alarms } = durableKeyValueStorageFixture(events);
    const runner = new TestRunner(state, timedEnv());
    const started = await startTimed(runner, 'run-pending');
    const dueAt = (started.suspendedAt?.gate as number) + TIMED_DEADLINE_MS;
    events.length = 0;

    // #when — the watchdog wake fires while no recovery journal exists. Before
    // the alarm was multiplexed this branch deleted the alarm outright, which
    // dropped the still-pending suspension deadline.
    await runner.alarm();

    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    expect(storedDeadlines(values)?.entries).toHaveLength(1);
    expect(events).not.toContain('deleteAlarm');
    expect(alarms.at(-1)).toBe(dueAt);
  });

  it('arms the recovery watchdog when it falls due before a far-future deadline', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const farFuture = Date.now() + 10 * 24 * 60 * 60 * 1_000;
    seedDeadlines(values, 'run-far-future', [
      { ...armedEntry('gate', Date.now()), deadlineAt: farFuture },
    ]);
    const runner = new TestRunner(state, timedEnv());

    const before = Date.now();
    await startTimed(runner, 'run-far-future');

    // #then — the recovery arm that opens the start route took the earlier of
    // the two due times, and did not push the far-future deadline out.
    expect(alarms[0]).toBeGreaterThanOrEqual(before + 60_000);
    expect(alarms[0]).toBeLessThan(farFuture);
  });

  it('re-arms to the pending deadline when run-owner recovery clears', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const dueAt = Date.now() + TIMED_DEADLINE_MS;
    seedDeadlines(values, 'run-cleared', [
      { ...armedEntry('gate', 1), deadlineAt: dueAt },
    ]);
    values.set('flowsafe:run-owner-recovery:v1', {
      version: 2,
      phase: 'preparing',
      owner: { kind: 'human', id: 'owner-1' },
      workflowId: 'timed',
      runId: 'run-cleared',
      token: 'attempt-token',
    });
    const env = makeProductionEnv();
    // An abandoned start attempt: recovery releases the claim and clears the
    // journal, the branch closest to deleting the alarm outright.
    env.runtime = {
      recoverStartAttempt: vi.fn(async () => null),
      ...statusStub(async () => null),
    } as unknown as RunnerRuntime;
    env.owners = {
      owner: async () => undefined,
      reserveAll: async () => true,
      settleReservation: async () => undefined,
    };
    const runner = new TestRunner(state, env);

    await runner.alarm();

    // #then — clearing the journal must not delete the alarm the deadline needs
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    expect(alarms.at(-1)).toBe(dueAt);
  });

  it('resumes the suspended step with the timeout envelope under a system principal', async () => {
    const sent: string[] = [];
    const { storage, values } = durableKeyValueStorageFixture();
    const state = {
      id: { name: 'timed:run-timeout' },
      storage,
      getWebSockets: () => [{ send: (data: string) => sent.push(data) }],
    } as unknown as DurableObjectState;
    const runner = new TestRunner(state, timedEnv());
    await startTimed(runner, 'run-timeout');
    elapseDeadlines(values);

    await runner.alarm();

    const frame = JSON.parse(sent.at(-1) ?? '{}') as {
      type: string;
      summary: RunSummary;
    };
    expect(frame.type).toBe('run');
    expect(frame.summary).toMatchObject({
      status: 'success',
      // The step branched on isSuspensionTimeoutResumeData, so the envelope
      // survived the resume path intact.
      result: { settledBy: 'timeout' },
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
      requestedByKind: 'system',
    });
    // #then — a terminal run has no suspension left to arm
    expect(storedDeadlines(values)).toBeUndefined();
  });

  it('drops a stale entry without resuming when the suspension fence moved', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const resume = vi.fn();
    const movedAt = Date.now() - TIMED_DEADLINE_MS + 60_000;
    const runtime = {
      // A real signal already resumed and re-suspended this step, so its
      // suspendedAt is not the one the entry was armed against.
      ...statusStub(async () => suspendedFence('run-stale', movedAt)),
      resume,
    } as unknown as RunnerRuntime;
    // Armed against suspendedAt 1, so its long-past deadline is due and its
    // fence no longer matches the moved-on suspension the status reports.
    seedDeadlines(values, 'run-stale', [armedEntry('gate', 1)]);
    const runner = new TestRunner(state, { ...timedEnv(), runtime });
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    expect(resume).not.toHaveBeenCalled();
    // #then — the stale entry is gone, and the wake re-derived from the summary
    // it already held rather than only deleting: the CURRENT suspension is what
    // ends up armed. A wake that merely dropped would leave a suspended run
    // with a derivable deadline, no record and no further wake.
    expect(storedDeadlines(values)?.entries).toEqual([
      armedEntry('gate', movedAt),
    ]);
    // #then — a discarded deadline is observable, not silent
    expect(
      logged.some((message) =>
        message.includes(
          "suspension deadline for step 'gate' of run 'run-stale' dropped",
        ),
      ),
    ).toBe(true);
  });

  it('records a backoff attempt and never rethrows when the timeout resume fails', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const runtime = {
      ...statusStub(async () => suspendedFence('run-retry', 1)),
      resume: vi.fn(async () => {
        throw new Error('injected resume failure');
      }),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-retry', [armedEntry('gate', 1)]);
    const runner = new TestRunner(state, { ...timedEnv(), runtime });
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      // A thrown alarm() is retried by workerd, which would re-run the resume.
      await expect(runner.alarm()).resolves.toBeUndefined();
    } finally {
      log.mockRestore();
    }

    const entry = storedDeadlines(values)?.entries[0];
    expect(entry?.attempts).toBe(1);
    expect(entry?.nextAttemptAt).toBeGreaterThanOrEqual(Date.now() + 59_000);
    expect(alarms.at(-1)).toBe(entry?.nextAttemptAt);
  });

  it('abandons a spent entry as a tombstone for exactly its own suspension', async () => {
    const events: string[] = [];
    const { state, values, alarms } = durableKeyValueStorageFixture(events);
    let resumeFails = true;
    const resume = vi.fn(async () => {
      if (resumeFails) throw new Error('injected resume failure');
      return {
        ...suspendedFence('run-exhausted', 1),
        resumeCount: { gate: 1 },
      };
    });
    const runtime = {
      ...statusStub(async () => suspendedFence('run-exhausted', 1)),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-exhausted', [
      {
        ...armedEntry('gate', 1),
        attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS - 1,
        nextAttemptAt: Date.now() - 1,
      },
    ]);
    const runner = new TestRunner(state, { ...timedEnv(), runtime });
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    try {
      await runner.alarm();

      // #then — the budget is spent, and the entry stays as a TOMBSTONE: kept
      // for its suspension with the floor dropped, never selected again.
      expect(logged).toContain(
        `suspension deadline for step 'gate' of run 'run-exhausted' dropped after ${MAX_SUSPENSION_DEADLINE_ATTEMPTS} failed wakes`,
      );
      expect(storedDeadlines(values)?.entries).toEqual([
        {
          ...armedEntry('gate', 1),
          attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
        },
      ]);
      expect(resume).toHaveBeenCalledTimes(1);

      // #when — the next nothing-due wake re-derives from authoritative state,
      // which still shows the abandoned suspension's armed payload. Removing
      // the entry instead of tombstoning it made this the resurrection path:
      // a clean-ledger entry, armed at the one-second floor, retried forever.
      const armsBefore = alarms.length;
      const before = Date.now();
      await runner.alarm();

      // #then — no resurrection: the merge carried the tombstone, the step
      // did not run again, and nothing was armed at the floor for it.
      expect(resume).toHaveBeenCalledTimes(1);
      expect(storedDeadlines(values)?.entries).toEqual([
        {
          ...armedEntry('gate', 1),
          attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
        },
      ]);
      for (const at of alarms.slice(armsBefore)) {
        expect(at).toBeGreaterThanOrEqual(before + 59_000);
      }
      expect(events.at(-1)).toBe('deleteAlarm');

      // #when — a real signal resumes the step and it suspends again
      resumeFails = false;
      const response = await runner.fetch(
        post('/runs/timed/run-exhausted/resume', {
          step: 'gate',
          resumeData: { approvedBy: 'bob' },
        }),
      );

      // #then — the moved fence dropped the tombstone: the LATER suspension of
      // the same step is a fresh entry with a fresh budget.
      expect(response.status).toBe(200);
      expect(storedDeadlines(values)?.entries).toEqual([
        { ...armedEntry('gate', 1), resumeCount: 1 },
      ]);
    } finally {
      log.mockRestore();
    }
  });

  it("quiets a foreign record by charging it and then reclaims the record from the object's own name", async () => {
    // #given — this object is `run-mine`, and the deadline key holds a record
    // written for `run-other`: a stale record left by a namespace reused under
    // another id, or one hand-written into storage.
    const events: string[] = [];
    const { storage, values } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-mine' },
      storage,
    } as unknown as DurableObjectState;
    const mineAt = Date.now();
    const foreignAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const resume = vi.fn();
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => suspendedFence('run-mine', mineAt)),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-other', [armedEntry('foreign', foreignAt)]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const causes = new Map<string, unknown>();
    const stamped: (number | undefined)[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
        causes.set(String(args[0]), args[1]);
      });

    // #when — the foreign entry falls due on every wake, with its backoff
    // rewound so the whole budget is spent inside the test
    try {
      for (let wake = 0; wake < MAX_SUSPENSION_DEADLINE_ATTEMPTS + 1; wake++) {
        const record = storedDeadlines(values);
        if (record) {
          values.set(SUSPENSION_DEADLINE_STORAGE_KEY, {
            ...record,
            entries: record.entries.map((entry) =>
              entry.nextAttemptAt === undefined
                ? entry
                : { ...entry, nextAttemptAt: Date.now() - 1 },
            ),
          });
        }
        await expect(runner.alarm()).resolves.toBeUndefined();
        for (const entry of storedDeadlines(values)?.entries ?? []) {
          stamped.push(entry.unreadableSince);
        }
      }
    } finally {
      log.mockRestore();
    }

    // #then — the identity assert refused before any workflow body ran, and it
    // refused with the entry in hand: the failure is CHARGED to the foreign
    // entry, wake after wake, which is what walks that record to a tombstone
    // instead of leaving it due forever.
    expect(resume).not.toHaveBeenCalled();
    expect(causes.get('suspension deadline wake failed')).toBeInstanceOf(Error);
    expect(
      (causes.get('suspension deadline wake failed') as Error).message,
    ).toContain('DO identity mismatch');
    for (
      let attempt = 1;
      attempt < MAX_SUSPENSION_DEADLINE_ATTEMPTS;
      attempt += 1
    ) {
      expect(logged).toContain(
        `suspension deadline wake for step 'foreign' of run 'run-other' failed (attempt ${attempt})`,
      );
    }
    expect(logged).toContain(
      `suspension deadline for step 'foreign' of run 'run-other' dropped after ${MAX_SUSPENSION_DEADLINE_ATTEMPTS} failed wakes`,
    );
    // #then — and never stamped: an identity mismatch is not an unreadable
    // read, so the 24 h clock never starts on it.
    expect(stamped).not.toHaveLength(0);
    expect(stamped.filter((mark) => mark !== undefined)).toEqual([]);
    // #then — the spent record arms nothing, and the wake after it re-derives
    // from `id.name` rather than from the record: the object reclaims the key
    // for its OWN run and the foreign tombstone goes with the fence it named.
    expect(events).toContain('deleteAlarm');
    expect(storedDeadlines(values)).toEqual({
      version: 1,
      workflowId: 'timed',
      runId: 'run-mine',
      entries: [armedEntry('gate', mineAt)],
    });
  });

  it("starts a clean ledger when a foreign record's tombstone collides with this run's own suspension", async () => {
    // #given — this object is `run-mine` and the deadline key holds a record
    // stamped for `run-other` whose SPENT entry matches every key the merge
    // carries a ledger on: the same step, the same fence, the same deadline.
    const events: string[] = [];
    const { storage, values, alarms } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-mine' },
      storage,
    } as unknown as DurableObjectState;
    const mineAt = Date.now();
    const resume = vi.fn();
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => suspendedFence('run-mine', mineAt)),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-other', [
      {
        ...armedEntry('gate', mineAt),
        attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
      },
    ]);
    const runner = new TestRunner(state, env);

    // #when — a tombstone is never due, so the wake reconciles from `id.name`
    // and rewrites the key under this object's own run
    await expect(runner.alarm()).resolves.toBeUndefined();

    // #then — the reclaimed record carries this run's own entry and nothing
    // else: a live deadline born at the budget would never fire, and its own
    // arm would delete the alarm that was its last chance to.
    expect(storedDeadlines(values)).toEqual({
      version: 1,
      workflowId: 'timed',
      runId: 'run-mine',
      entries: [armedEntry('gate', mineAt)],
    });
    expect(resume).not.toHaveBeenCalled();
    expect(alarms.at(-1)).toBe(mineAt + TIMED_DEADLINE_MS);
    expect(events.at(-1)).toBe('setAlarm');
  });

  it('deletes a foreign record when the reclaiming run derives no deadline of its own', async () => {
    // #given — this object is `run-mine`, the deadline key holds a record
    // stamped for `run-other` whose entry is far from due, and this run has
    // already finished: the reconcile has nothing to write in its place.
    const events: string[] = [];
    const { storage, values } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-mine' },
      storage,
    } as unknown as DurableObjectState;
    const resume = vi.fn();
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => ({ runId: 'run-mine', status: 'success' })),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-other', [
      armedEntry('foreign', Date.now() + 10 * TIMED_DEADLINE_MS),
    ]);
    const runner = new TestRunner(state, env);

    // #when — nothing is due, so the wake reconciles rather than resuming
    await expect(runner.alarm()).resolves.toBeUndefined();

    // #then — the write decides on the record that is STORED, not on the one
    // this run may inherit a ledger from: a foreign record is present, so the
    // empty derivation DELETES the key and the alarm goes with it. Deciding
    // that on the inherited record instead would read as nothing-stored and
    // skip the write, stranding the foreign entry armed forever and waking
    // this object for a run it does not own.
    expect(storedDeadlines(values)).toBeUndefined();
    expect(events).toContain(`delete:${SUSPENSION_DEADLINE_STORAGE_KEY}`);
    expect(events.at(-1)).toBe('deleteAlarm');
    expect(resume).not.toHaveBeenCalled();
  });

  it('keeps the record and its wake when a nothing-due wake reads Mastra in-memory fallback state', async () => {
    const events: string[] = [];
    const {
      storage: doStorage,
      values,
      alarms,
    } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-blinded-idle' },
      storage: doStorage,
    } as unknown as DurableObjectState;
    const env = timedEnv();
    const runner = new TestRunner(state, env);
    await startTimed(runner, 'run-blinded-idle');
    const armed = storedDeadlines(values)?.entries;
    expect(armed).toHaveLength(1);

    // #when — the real producer of the degraded read: the row lookup comes
    // back empty (a replica behind, a store handle gone) while this isolate
    // still holds the suspended Run, so Mastra answers from memory instead.
    const restore = await blindWorkflowRow(env.storage);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    events.length = 0;
    const before = Date.now();

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
      restore();
    }

    // #then — nothing concluded from a read that never reached storage. The
    // fallback reports 'pending' for a run that has never been resumed, so the
    // self-consistency backstop calls it readable: reconciling from it derived
    // nothing and DELETED the record and the alarm of a run that is still
    // suspended, silently.
    expect(storedDeadlines(values)?.entries).toEqual(armed);
    expect(events).not.toContain('deleteAlarm');
    expect(events.filter((event) => event.startsWith('delete:'))).toEqual([]);
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 60_000);
    expect(
      logged.filter((message) =>
        message.includes('could not read authoritative state'),
      ),
    ).toHaveLength(1);

    // #then — and the run really was suspended the whole time
    const status = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed/run-blinded-idle'),
    );
    expect(((await status.json()) as RunSummary).status).toBe('suspended');
  });

  it('charges nothing and runs nothing when a due wake reads Mastra in-memory fallback state', async () => {
    const events: string[] = [];
    const {
      storage: doStorage,
      values,
      alarms,
    } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-blinded-due' },
      storage: doStorage,
    } as unknown as DurableObjectState;
    const env = timedEnv();
    const runner = new TestRunner(state, env);
    const started = await startTimed(runner, 'run-blinded-due');
    const suspendedAt = started.suspendedAt?.gate as number;
    elapseDeadlines(values);
    const due = storedDeadlines(values)?.entries[0];
    const restore = await blindWorkflowRow(env.storage);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const before = Date.now();

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
      restore();
    }

    // #then — the entry is intact and UNCHARGED, and the wake kept the
    // watchdog cadence: charging here would walk a live deadline to its
    // tombstone inside a quarter of an hour of degraded reads.
    const entry = storedDeadlines(values)?.entries[0];
    expect(entry).toMatchObject({ ...due });
    expect(entry).not.toHaveProperty('attempts');
    expect(entry?.unreadableSince).toBeGreaterThanOrEqual(before);
    expect(events).not.toContain('deleteAlarm');
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 60_000);

    // #then — nothing resumed: the run is still suspended at the same fence
    const mid = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed/run-blinded-due'),
    );
    const midSummary = (await mid.json()) as RunSummary;
    expect(midSummary.status).toBe('suspended');
    expect(midSummary.suspendedAt?.gate).toBe(suspendedAt);

    // #when — the read heals, which is all this wake was ever waiting for
    await runner.alarm();

    // #then — the deadline it declined to spend still fires
    const settled = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed/run-blinded-due'),
    );
    expect(await settled.json()).toMatchObject({
      status: 'success',
      result: { settledBy: 'timeout' },
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
    });
    expect(storedDeadlines(values)).toBeUndefined();
  });

  it('classifies a read that threw as unreadable state and keeps the thrown error as its cause', async () => {
    // #given — a due entry whose authoritative read throws something that is
    // not already a RunStateUnreadableError: a storage fault, as a driver
    // surfaces one.
    const events: string[] = [];
    const { storage, values } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-read-threw' },
      storage,
    } as unknown as DurableObjectState;
    const thrown = new Error('D1_ERROR: network connection lost');
    const resume = vi.fn();
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => {
        throw thrown;
      }),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-read-threw', [
      armedEntry('gate', Date.now() - TIMED_DEADLINE_MS - 1),
    ]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const causes: unknown[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
        causes.push(args[1]);
      });

    // #when
    try {
      await expect(runner.alarm()).resolves.toBeUndefined();
    } finally {
      log.mockRestore();
    }

    // #then — the wake reported the one conclusion it may draw from a failed
    // read, and the error it reported carries the original as `cause`: the
    // classification is what the alarm acts on, and the fault underneath it is
    // still the thing an operator has to diagnose.
    const raised =
      causes[
        logged.indexOf(
          'suspension deadline wake could not read authoritative state',
        )
      ];
    expect(raised).toBeInstanceOf(RunStateUnreadableError);
    expect((raised as RunStateUnreadableError).cause).toBe(thrown);
    expect(resume).not.toHaveBeenCalled();
  });

  it('deletes no run row and settles nothing when recovery reads in-memory fallback state', async () => {
    const events: string[] = [];
    const {
      storage: doStorage,
      values,
      alarms,
    } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-blinded-recovery' },
      storage: doStorage,
    } as unknown as DurableObjectState;
    const settle = vi.fn(async () => undefined);
    const env = makeProductionEnv(testStorage(), {
      reserve: async () => true,
      settle,
    });
    env.runtime = timedRuntime(env.storage);
    const token = 'attempt-token';
    // The interrupted start, exactly as one is left behind: claim reserved and
    // journaled, Mastra's suspension persisted, no boundary left but recovery.
    await env.owners.reserveAll(
      [{ kind: 'run', resourceId: 'run-blinded-recovery' }],
      OWNER_PRINCIPAL,
      token,
    );
    await (env.runtime as RunnerRuntime).start('timed', {
      runId: 'run-blinded-recovery',
      inputData: { topic: 't' },
      requestedBy: OWNER_PRINCIPAL.id,
      requestedByKind: OWNER_PRINCIPAL.kind,
      attemptToken: token,
    });
    values.set(
      'flowsafe:run-owner-recovery:v1',
      await durableOwnerRecovery(
        env.runtime as RunnerRuntime,
        'timed',
        'run-blinded-recovery',
        token,
      ),
    );
    const runner = new TestRunner(state, env);
    const deletes = await countRowDeletes(env.storage);
    const restore = await blindWorkflowRow(env.storage);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      // #then — the wake does not rethrow: workerd retries a thrown alarm up
      // to six times, which would be a retry storm inside the very incident
      // that caused it.
      await expect(runner.alarm()).resolves.toBeUndefined();
    } finally {
      log.mockRestore();
      restore();
      deletes.restore();
    }

    // #then — no conclusion drawn from a read that never reached storage. The
    // fallback presents no provenance and a 'pending' status, the shape the
    // abandoned-shell branch would DELETE a live row and its snapshot from
    // behind a lagging read.
    expect(deletes.calls()).toBe(0);
    expect(settle).not.toHaveBeenCalled();
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(true);
    expect(
      logged.filter((message) =>
        message.includes('could not read authoritative state'),
      ),
    ).toHaveLength(2);
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 60_000);

    // #then — the run survived, and the recovery it is still owed happens on a
    // later wake once the read heals
    const status = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed/run-blinded-recovery'),
    );
    expect(((await status.json()) as RunSummary).status).toBe('suspended');

    await runner.alarm();

    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    expect(settle).toHaveBeenCalledTimes(1);
  });

  it('answers dispatch-status with a retryable 503 while the run state cannot be read', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => null),
      recoverStartAttempt: vi.fn(async () => {
        throw new RunStateUnreadableError('timed', 'run-unreadable-route');
      }),
    } as unknown as RunnerRuntime;
    // The interrupted start this route settles, with the read that would
    // settle it refusing to answer from state it could not reach.
    values.set('flowsafe:run-owner-recovery:v1', {
      version: 2,
      phase: 'prepared',
      owner: { kind: 'human', id: 'owner-1' },
      execution: {
        tablePrefix: '',
        workflowId: 'timed',
        runId: 'run-unreadable-route',
        startToken: 'test-generation',
      },
      workflowId: 'timed',
      runId: 'run-unreadable-route',
      token: 'attempt-token',
    });
    const runner = new TestRunner(state, env);

    const response = await runner.fetch(
      deploymentIdentityRequest(
        'http://do/runs/timed/run-unreadable-route/dispatch-status',
      ),
    );

    // #then — the operator's problem and retryable, like a misprovisioned
    // deployment: a 500 would read as a code fault, and answering 200 from the
    // fabricated summary is the failure the guard exists to prevent.
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringContaining('state is not readable'),
    });
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(true);
  });

  it('answers a start with a retryable 503 while the pending recovery cannot read authoritative state', async () => {
    // #given — a start whose object still holds an interrupted start's
    // journal, with the read that would settle it refusing to answer from
    // state it could not reach. The recovery runs BEFORE the existing-run
    // check, so nothing downstream of it sees a fabricated read.
    const { state, values } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    const start = vi.fn();
    env.runtime = {
      ...statusStub(async () => null),
      start,
      recoverStartAttempt: vi.fn(async () => {
        throw new RunStateUnreadableError('timed', 'run-start-unreadable');
      }),
    } as unknown as RunnerRuntime;
    values.set('flowsafe:run-owner-recovery:v1', {
      version: 2,
      phase: 'prepared',
      owner: { kind: 'human', id: 'owner-1' },
      execution: {
        tablePrefix: '',
        workflowId: 'timed',
        runId: 'run-start-unreadable',
        startToken: 'test-generation',
      },
      workflowId: 'timed',
      runId: 'run-start-unreadable',
      token: 'attempt-token',
    });
    const runner = new TestRunner(state, env);

    // #when
    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'timed',
        runId: 'run-start-unreadable',
        inputData: { topic: 't' },
      }),
    );

    // #then — a retryable 503 (`state is not readable`), and no second run.
    // The journal survives for a wake that can read it.
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringContaining('state is not readable'),
    });
    expect(start).not.toHaveBeenCalled();
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(true);
  });

  it('stamps the unreadable clock once and clears it on the first read that succeeds', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    let readable = false;
    const movedAt = Date.now();
    const env = makeProductionEnv();
    const resume = vi.fn();
    env.runtime = {
      ...statusStub(async () => {
        if (!readable) throw new Error('D1: network error');
        // A real signal moved the suspension on while reads were failing, so
        // the wake reconciles instead of resuming — which is what lets the
        // stamp's fate be observed on an entry that survives.
        return suspendedFence('run-clock', movedAt);
      }),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-clock', [armedEntry('gate', armedAt)]);
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await runner.alarm();
      const stamped = storedDeadlines(values)?.entries[0]?.unreadableSince;
      expect(stamped).toBeGreaterThan(0);

      // #when — the read keeps failing
      await runner.alarm();

      // #then — the clock is not restamped: rewriting it every wake would push
      // the day it allows out forever, and nothing would ever bound the run.
      expect(storedDeadlines(values)?.entries[0]?.unreadableSince).toBe(
        stamped,
      );

      readable = true;
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — a read that SUCCEEDED is the clearing event: the reconcile
    // rebuilds the entry from what it derived, and the stamp does not ride
    // along.
    expect(resume).not.toHaveBeenCalled();
    expect(storedDeadlines(values)?.entries).toEqual([
      armedEntry('gate', movedAt),
    ]);
  });

  it('abandons an entry whose run state has been unreadable for a day', async () => {
    const events: string[] = [];
    const { state, values, alarms } = durableKeyValueStorageFixture(events);
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const env = makeProductionEnv();
    const resume = vi.fn();
    env.runtime = {
      ...statusStub(async () => {
        throw new Error('unknown workflow');
      }),
      resume,
    } as unknown as RunnerRuntime;
    // A day of wakes that never charge anything: the workflow registration a
    // deploy dropped for good, which no read will ever heal.
    seedDeadlines(values, 'run-day', [
      {
        ...armedEntry('gate', armedAt),
        unreadableSince: Date.now() - 86_400_000 - 1,
      },
    ]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      await runner.alarm();
      const armsAfterAbandon = alarms.length;

      // #then — a tombstone, under its own log: this entry was never charged a
      // single failed resume, so the drop log would name a budget it never
      // spent.
      expect(logged).toContain(
        "suspension deadline for step 'gate' of run 'run-day' abandoned after 24 h of unreadable run state",
      );
      expect(storedDeadlines(values)?.entries).toEqual([
        {
          ...armedEntry('gate', armedAt),
          attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
        },
      ]);
      // #then — and the heartbeat ENDS, which is the whole point of the bound:
      // giving up on the entry and then waking for it every minute forever
      // would be no bound at all. Nothing else is armed, so the alarm goes.
      expect(events.at(-1)).toBe('deleteAlarm');

      // #when — a wake is forced anyway (workerd redelivery, or another duty)
      await runner.alarm();

      // #then — the tombstone is never selected, so nothing is retried and
      // nothing is armed at the one-second floor for it
      expect(resume).not.toHaveBeenCalled();
      for (const at of alarms.slice(armsAfterAbandon)) {
        expect(at).toBeGreaterThanOrEqual(before + 60_000);
      }
    } finally {
      log.mockRestore();
    }
  });

  it('restarts a future-dated unreadable clock at the wake that reads it', async () => {
    // #given — a stamp AHEAD of the wake reading it: a clock that stepped
    // backwards between two wakes, or a hand-written record. Left as read, its
    // elapsed time can never pass the limit and the entry keeps an uncharged
    // heartbeat forever.
    const { state, values } = durableKeyValueStorageFixture();
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => {
        throw new Error('unknown workflow');
      }),
      resume: vi.fn(),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-future-clock', [
      {
        ...armedEntry('gate', armedAt),
        unreadableSince: Date.now() + 10 * 86_400_000,
      },
    ]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      // #when — a wake reads the future stamp
      await runner.alarm();

      // #then — it is corrected to this wake, so the day the bound allows
      // actually starts running
      const restarted = storedDeadlines(values)?.entries[0]?.unreadableSince;
      expect(restarted).toBeGreaterThanOrEqual(before);
      expect(restarted).toBeLessThanOrEqual(Date.now());

      // #when — that day passes with the read still failing
      values.set(SUSPENSION_DEADLINE_STORAGE_KEY, {
        ...storedDeadlines(values),
        entries: [
          {
            ...armedEntry('gate', armedAt),
            unreadableSince: Date.now() - 86_400_000 - 1,
          },
        ],
      });
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — the entry matures and is abandoned like any other, instead of
    // heart-beating for a run nothing will ever read
    expect(logged).toContain(
      "suspension deadline for step 'gate' of run 'run-future-clock' abandoned after 24 h of unreadable run state",
    );
    expect(storedDeadlines(values)?.entries).toEqual([
      {
        ...armedEntry('gate', armedAt),
        attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
      },
    ]);
  });

  it('runs one unreadable-state clock over the whole due batch and leaves entries that are not due alone', async () => {
    const events: string[] = [];
    const { state, values, alarms } = durableKeyValueStorageFixture(events);
    const now = Date.now();
    const alphaAt = now - TIMED_DEADLINE_MS - 10_000;
    const bravoAt = now - TIMED_DEADLINE_MS - 5_000;
    const alpha = armedEntry('alpha', alphaAt);
    // Due as well, and carrying a live ledger from an earlier failed resume:
    // the stamp must ride alongside a ledger without disturbing it.
    const bravo = {
      ...armedEntry('bravo', bravoAt),
      attempts: 2,
      nextAttemptAt: now - 1,
    };
    const future = armedEntry('future', now);
    const env = makeProductionEnv();
    const resume = vi.fn();
    env.runtime = {
      ...statusStub(async () => {
        throw new Error('D1: network error');
      }),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-batch', [alpha, bravo, future]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    try {
      await runner.alarm();

      // #then — one failed read is one fact about the run, so every entry due
      // at this wake starts its day together. A per-entry clock would make an
      // N-entry record take N days to clear instead of one.
      const clock = storedEntry(values, 'alpha')?.unreadableSince as number;
      expect(clock).toBeGreaterThanOrEqual(now);
      expect(storedEntry(values, 'bravo')).toEqual({
        ...bravo,
        unreadableSince: clock,
      });
      // #then — and the entry that is NOT due is byte-identical: its own day
      // begins when it falls due, not when a sibling's did.
      expect(storedEntry(values, 'future')).toEqual(future);
      expect(resume).not.toHaveBeenCalled();

      // #when — the read never heals and the day the bound allows passes
      const stored = storedDeadlines(values) as SuspensionDeadlineRecord;
      values.set(SUSPENSION_DEADLINE_STORAGE_KEY, {
        ...stored,
        entries: stored.entries.map((entry) =>
          entry.unreadableSince === undefined
            ? entry
            : { ...entry, unreadableSince: entry.unreadableSince - 86_400_001 },
        ),
      });
      await runner.alarm();

      // #then — the whole batch is abandoned in ONE wake, each under its own
      // log, while the entry that never came due is untouched and still holds
      // the object's alarm.
      expect(storedEntry(values, 'alpha')).toEqual({
        ...alpha,
        attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
      });
      expect(storedEntry(values, 'bravo')).toEqual({
        ...armedEntry('bravo', bravoAt),
        attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
      });
      expect(storedEntry(values, 'future')).toEqual(future);
      expect(
        logged.filter((message) => message.includes('abandoned after 24 h')),
      ).toHaveLength(2);
      expect(events).not.toContain('deleteAlarm');
      expect(alarms.at(-1)).toBe(future.deadlineAt);
    } finally {
      log.mockRestore();
    }
  });

  it('clears the unreadable stamp from a lifecycle boundary while the wake read is still failing', async () => {
    const { storage, values } = durableKeyValueStorageFixture();
    const state = {
      id: { name: 'timed:run-boundary' },
      storage,
    } as unknown as DurableObjectState;
    const now = Date.now();
    const alphaAt = now - TIMED_DEADLINE_MS - 1;
    const bravoAt = now;
    // The summary the resume of the SIBLING step hands back: a live projection
    // of a run still suspended at both fences, which is evidence about the run
    // even while its row read keeps failing.
    const bothSuspended: RunSummary = {
      runId: 'run-boundary',
      status: 'suspended',
      suspended: [['alpha'], ['bravo']],
      suspendPayload: {
        alpha: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
        bravo: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
      },
      suspendedAt: { alpha: alphaAt, bravo: bravoAt },
    };
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => {
        throw new Error('D1: network error');
      }),
      resume: vi.fn(async () => bothSuspended),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-boundary', [
      armedEntry('alpha', alphaAt),
      armedEntry('bravo', bravoAt),
    ]);
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);

    try {
      await runner.alarm();
      expect(storedEntry(values, 'alpha')?.unreadableSince).toBeGreaterThan(0);

      // #when — a boundary lands that this wake had nothing to do with: an
      // HTTP resume of the sibling step
      const response = await runner.fetch(
        post('/runs/timed/run-boundary/resume', {
          step: ['bravo'],
          resumeData: { approvedBy: 'ops' },
        }),
      );

      // #then — the boundary's own summary is evidence the run is real, so the
      // merge rebuilds both entries without the stamp: the day is not counted
      // through a run a host keeps touching.
      expect(response.status).toBe(200);
      expect(storedDeadlines(values)?.entries).toEqual([
        armedEntry('alpha', alphaAt),
        armedEntry('bravo', bravoAt),
      ]);
    } finally {
      log.mockRestore();
    }
  });

  it('keeps its cadence when the unreadable stamp itself cannot be written', async () => {
    const { storage, values, alarms } = durableKeyValueStorageFixture();
    const { state } = deadlineWriteFailures(storage, {
      name: 'timed:run-stamp-write-fail',
    });
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const armed = armedEntry('gate', armedAt);
    const env = makeProductionEnv();
    const resume = vi.fn();
    env.runtime = {
      ...statusStub(async () => {
        throw new Error('D1: network error');
      }),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-stamp-write-fail', [armed]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — the 24 h bound is conditional on this write landing, and a
    // storage layer that cannot write the key converges nothing anyway: the
    // failure is logged, the record is untouched, nothing is charged, and the
    // wake keeps the watchdog cadence it would have kept regardless.
    expect(storedDeadlines(values)?.entries).toEqual([armed]);
    expect(resume).not.toHaveBeenCalled();
    expect(
      logged.filter((message) =>
        message.includes('unreadable-state marker failed'),
      ),
    ).toHaveLength(3);
    expect(logged.some((message) => message.includes('failed (attempt'))).toBe(
      false,
    );
    for (const at of alarms) {
      expect(at).toBeGreaterThanOrEqual(before + 60_000);
    }
  });

  it('reaches the tombstone in five charges when readable and unreadable wakes alternate', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const armed = armedEntry('gate', armedAt);
    let readable = false;
    const env = makeProductionEnv();
    const resume = vi.fn(async () => {
      throw new Error('resume failed');
    });
    env.runtime = {
      ...statusStub(async () => {
        readable = !readable;
        if (!readable) throw new Error('D1: network error');
        return suspendedFence('run-oscillating', armedAt);
      }),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-oscillating', [armed]);
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);

    try {
      for (let wake = 0; wake < 24; wake += 1) {
        // Only the backoff a charge wrote is rewound, so the entry stays due
        // at its original fence: the question is whether an interleaved stamp
        // ever resets the ledger, not how long the backoff takes.
        const stored = storedDeadlines(values) as SuspensionDeadlineRecord;
        values.set(SUSPENSION_DEADLINE_STORAGE_KEY, {
          ...stored,
          entries: stored.entries.map((entry) =>
            entry.nextAttemptAt === undefined
              ? entry
              : { ...entry, nextAttemptAt: Date.now() - 1 },
          ),
        });
        await runner.alarm();
      }
    } finally {
      log.mockRestore();
    }

    // #then — a stamp is not a reprieve: the failed resumes still add up to
    // the budget, the entry ends as a tombstone carrying no clock, and no
    // sixth resume runs after it.
    expect(resume).toHaveBeenCalledTimes(MAX_SUSPENSION_DEADLINE_ATTEMPTS);
    expect(storedDeadlines(values)?.entries).toEqual([
      { ...armed, attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS },
    ]);
  });

  it('spends no abandonment budget on a wake that cannot build its runtime', async () => {
    const events: string[] = [];
    const { state, values, alarms } = durableKeyValueStorageFixture(events);
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const armed = armedEntry('gate', armedAt);
    // A misconfigured binding: build(env) throws on EVERY wake and nothing
    // memoizes the failure, so this is the shape that would walk a live
    // deadline to its tombstone in five wakes if it were charged.
    class UnbuildableRunner extends TestRunner {
      protected build(): RunnerRuntime {
        throw new Error('WORKFLOWS binding is not configured');
      }
    }
    seedDeadlines(values, 'run-unbuildable', [armed]);
    const runner = new UnbuildableRunner(state, makeProductionEnv());
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — a fault that says nothing about this entry spends nothing on it:
    // the record is untouched, no ledger, no clock, no tombstone.
    expect(storedDeadlines(values)?.entries).toEqual([armed]);
    // #then — and the wake keeps the watchdog cadence until the binding is
    // fixed, never the one-second arm floor a still-due entry would take.
    expect(events).not.toContain('deleteAlarm');
    for (const at of alarms) {
      expect(at).toBeGreaterThanOrEqual(before + 60_000);
    }
    expect(
      logged.filter((message) => message === 'suspension deadline wake failed'),
    ).toHaveLength(3);
  });

  it('keeps an unwritable record and its cadence on a nothing-due wake', async () => {
    const { storage, values, alarms } = durableKeyValueStorageFixture();
    const { state } = deadlineWriteFailures(storage, {
      name: 'timed:run-idle-write-fail',
    });
    const armedAt = Date.now();
    const armed = armedEntry('gate', armedAt);
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => suspendedFence('run-idle-write-fail', armedAt)),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-idle-write-fail', [armed]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — the re-derive read fine and derived the same entry; only the
    // write failed. Nothing is due, so there is no entry to charge and none is
    // charged: the record survives untouched on the watchdog cadence, never at
    // the arm floor.
    expect(storedDeadlines(values)?.entries).toEqual([armed]);
    expect(logged).toContain('suspension deadline wake failed');
    expect(logged.some((message) => message.includes('failed (attempt'))).toBe(
      false,
    );
    for (const at of alarms) {
      expect(at).toBeGreaterThanOrEqual(before + 60_000);
    }
  });

  it('discards a malformed stored record and converges instead of throwing every wake', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    values.set(SUSPENSION_DEADLINE_STORAGE_KEY, {
      version: 1,
      workflowId: 'timed/forged',
      runId: 'run-malformed',
      entries: [],
    });
    const runner = new TestRunner(state, timedEnv());
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await expect(runner.alarm()).resolves.toBeUndefined();
    } finally {
      log.mockRestore();
    }

    expect(values.has(SUSPENSION_DEADLINE_STORAGE_KEY)).toBe(false);
  });

  it('re-arms at the new fence when the timeout resume suspends again', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const runner = new TestRunner(state, timedEnv());
    const started = await startTimed(
      runner,
      'run-escalating',
      'timed-escalating',
    );
    const firstSuspendedAt = started.suspendedAt?.gate as number;
    elapseDeadlines(values);

    await runner.alarm();

    // #then — the entry is fenced on the ESCALATED suspension: a new resume
    // ordinal, and the escalation's own deadline offset. Not `suspendedAt`
    // alone: the re-suspension can land in the same millisecond as the first.
    const entry = storedDeadlines(values)?.entries[0];
    expect(entry?.suspendedAt).toBeGreaterThanOrEqual(firstSuspendedAt);
    expect(entry?.resumeCount).toBe(1);
    expect(entry?.deadlineAt).toBe(
      (entry?.suspendedAt as number) + RETRY_DEADLINE_MS,
    );
    expect(alarms.at(-1)).toBe(entry?.deadlineAt);
  });

  it('clears the record when the run is terminated', async () => {
    const events: string[] = [];
    const { state, values } = durableKeyValueStorageFixture(events);
    const runner = new TestRunner(state, timedEnv());
    await startTimed(runner, 'run-terminated');
    expect(storedDeadlines(values)?.entries).toHaveLength(1);

    const response = await runner.fetch(
      post('/runs/timed/run-terminated/terminate', {}),
    );

    expect(response.status).toBe(200);
    expect(storedDeadlines(values)).toBeUndefined();
    expect(events.at(-1)).toBe('deleteAlarm');
  });

  it('clears the record on a terminal deadline route that transitions nothing', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    // A deadline request whose compare-and-swap does not match: the route
    // returns the current terminal summary without finalizing. The cleanup
    // prevents that counterfactual path from retaining an armed record.
    env.runtime = {
      cancelActiveExecution: vi.fn(async () => undefined),
      ...statusStub(async () => ({ runId: 'run-noop', status: 'timed_out' })),
      timeOutAsPrincipal: vi.fn(async () => ({
        summary: { runId: 'run-noop', status: 'timed_out' },
        transitioned: false,
        casMatched: false,
        cleanup: { revision: 2, status: 'timed_out', cleanupCompleted: true },
      })),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-noop', [armedEntry('gate', Date.now())]);
    const runner = new TestRunner(state, env);

    const response = await runner.fetch(
      post(
        '/runs/timed/run-noop/deadline',
        { expectedRevision: 1 },
        {
          kind: 'system',
          id: 'maintenance',
          purpose: 'run-deadline-maintenance',
        },
      ),
    );

    expect(response.status).toBe(200);
    expect(storedDeadlines(values)).toBeUndefined();
  });

  it('charges nothing and keeps the watchdog when a due wake cannot read authoritative state', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const resume = vi.fn();
    const env = makeProductionEnv();
    // A run whose workflow a deploy unregistered, or a D1 fault: the read
    // throws before the fence can be checked, so nothing consumes the entry
    // and it stays due.
    env.runtime = {
      ...statusStub(async () => {
        throw new Error('unknown workflow');
      }),
      resume,
    } as unknown as RunnerRuntime;
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const armed = armedEntry('gate', armedAt);
    seedDeadlines(values, 'run-hot-loop', [armed]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — Cloudflare fires an alarm set in the past immediately, so a past
    // arm anywhere in this sequence is the observable hot loop. Every arm here
    // is the watchdog's instead: the wake converged nothing.
    expect(alarms.filter((at) => at <= before)).toEqual([]);
    for (const at of alarms) {
      expect(at).toBeGreaterThanOrEqual(before + 60_000);
    }
    expect(resume).not.toHaveBeenCalled();
    // #then — and NOT charged. A ~15 minute incident would otherwise walk this
    // entry through its five failures and tombstone it, abandoning a live
    // deadline permanently for the suspension it belongs to.
    const entry = storedDeadlines(values)?.entries[0];
    expect(entry).toMatchObject({ ...armed });
    expect(entry).not.toHaveProperty('attempts');
    expect(entry).not.toHaveProperty('nextAttemptAt');
    // #then — the only bound on an unreadable run: stamped by the first such
    // wake, and by that one only, so the day it allows cannot be pushed out.
    expect(entry?.unreadableSince).toBeGreaterThanOrEqual(before);
    expect(entry?.unreadableSince).toBeLessThanOrEqual(Date.now());
    expect(
      logged.filter((message) =>
        message.includes('could not read authoritative state'),
      ),
    ).toHaveLength(3);
    expect(logged).not.toContain('suspension deadline wake failed');
  });

  it('does not charge a bookkeeping failure to a resume that succeeded', async () => {
    const { storage, values, alarms } = durableKeyValueStorageFixture();
    const { state, fail } = deadlineWriteFailures(storage, {
      once: true,
      armed: false,
    });
    const runner = new TestRunner(state, timedEnv());
    const started = await startTimed(
      runner,
      'run-reconcile-fail',
      'timed-escalating',
    );
    const armedAt = started.suspendedAt?.gate as number;
    elapseDeadlines(values);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    try {
      // #when — the resume runs the step and re-suspends it, and only the
      // bookkeeping write that records the NEW deadline fails.
      fail();
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — the failure belongs to the reconciliation, not to the resume:
    // charging it would record a failed resume that in fact succeeded, and the
    // next wake could run the step a second time.
    expect(logged).toContain('suspension deadline reconciliation failed');
    expect(logged.some((message) => message.includes('failed (attempt'))).toBe(
      false,
    );
    const entry = storedDeadlines(values)?.entries[0];
    expect(entry?.suspendedAt).toBe(armedAt);
    expect(entry).not.toHaveProperty('attempts');
    // #then — the stale entry still has a wake, and it is not in the past
    expect(alarms.at(-1)).toBeGreaterThan(armedAt);

    // #when — nothing else happens to this run: no client call, only its own
    // next wake, which is all a suspended run is guaranteed to get.
    const recovery = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      await runner.alarm();
    } finally {
      recovery.mockRestore();
    }

    // #then — the escalated suspension the failed write lost is armed now. The
    // wake found its own entry stale and reconciled from authoritative state
    // instead of merely deleting it, which is what makes the loss temporary
    // rather than permanent.
    const escalated = storedDeadlines(values)?.entries;
    expect(escalated).toHaveLength(1);
    expect(escalated?.[0]?.suspendedAt).toBeGreaterThanOrEqual(armedAt);
    expect(escalated?.[0]?.resumeCount).toBe(1);
    expect(escalated?.[0]?.deadlineAt).toBe(
      (escalated?.[0]?.suspendedAt as number) + RETRY_DEADLINE_MS,
    );
    const status = await runner.fetch(
      deploymentIdentityRequest(
        'http://do/runs/timed-escalating/run-reconcile-fail',
      ),
    );
    expect(((await status.json()) as RunSummary).status).toBe('suspended');
  });

  it('does not charge a broadcast failure to a resume that ran the step', async () => {
    // #given — a subscribed run-channel socket, and a step whose timeout
    // branch completes the run with a value JSON cannot encode. The frame is
    // built by JSON.stringify outside safeSend's per-socket tolerance, so
    // building it throws after the resume has already run the step.
    const events: string[] = [];
    const { storage, values } = durableKeyValueStorageFixture(events);
    const sent: string[] = [];
    const state = {
      id: { name: 'timed:run-broadcast-throws' },
      storage,
      getWebSockets: () => [
        {
          send: (frame: string) => {
            sent.push(frame);
          },
        },
      ],
    } as unknown as DurableObjectState;
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    let current: RunSummary = suspendedFence('run-broadcast-throws', armedAt);
    const env = makeProductionEnv();
    const resume = vi.fn(async () => {
      current = {
        runId: 'run-broadcast-throws',
        status: 'success',
        result: { amount: 10n },
      };
      return current;
    });
    env.runtime = {
      ...statusStub(async () => current),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-broadcast-throws', [
      armedEntry('gate', armedAt),
    ]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    // #when — the deadline fires, and the run keeps waking afterwards
    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — the step ran exactly once. Charging the broadcast would record a
    // failed resume that in fact succeeded, and the next wake would find the
    // entry still due and run the expired step's body again.
    expect(resume).toHaveBeenCalledTimes(1);
    expect(sent).toEqual([]);
    expect(logged).toContain('suspension deadline broadcast failed');
    expect(logged).not.toContain('suspension deadline wake failed');
    expect(logged.some((message) => message.includes('failed (attempt'))).toBe(
      false,
    );
    // #then — and the bookkeeping after it still ran: the run is terminal, so
    // the record is cleared and the object stops waking for it.
    expect(storedDeadlines(values)).toBeUndefined();
    expect(events.at(-1)).toBe('deleteAlarm');
  });

  it('retries rather than dropping the entry when the run is momentarily unreadable', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const resume = vi.fn();
    const env = makeProductionEnv();
    // A read replica that has not caught up with a snapshot this object wrote
    // looks exactly like "no such run"; treating it as proof that a signal got
    // there first would discard a live deadline.
    env.runtime = {
      ...statusStub(async () => null),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-unreadable', [armedEntry('gate', 1)]);
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    expect(resume).not.toHaveBeenCalled();
    const entry = storedDeadlines(values)?.entries[0];
    expect(entry?.attempts).toBe(1);
    expect(alarms.at(-1)).toBe(entry?.nextAttemptAt);
  });

  it('drops the entry when only the resumeCount fence moved', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const resume = vi.fn();
    const env = makeProductionEnv();
    // Same step, same suspension time, one resume further on: a real signal
    // resumed and re-suspended within the same millisecond.
    env.runtime = {
      ...statusStub(async () => ({
        ...suspendedFence('run-ordinal', 1),
        resumeCount: { gate: 1 },
      })),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-ordinal', [armedEntry('gate', 1)]);
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    expect(resume).not.toHaveBeenCalled();
    // #then — the ordinal alone settles it: the entry armed at resumeCount 0 is
    // gone, replaced by one fenced on the resume that superseded it.
    expect(storedDeadlines(values)?.entries).toEqual([
      { ...armedEntry('gate', 1), resumeCount: 1 },
    ]);
  });

  it('resumes one due entry per wake and keeps the other armed', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const gateSuspendedAt = Date.now() - TIMED_DEADLINE_MS - 2;
    const otherSuspendedAt = gateSuspendedAt + 1;
    const bothSuspended: RunSummary = {
      runId: 'run-pair',
      status: 'suspended',
      suspended: [['gate'], ['other']],
      suspendPayload: {
        gate: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
        other: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
      },
      suspendedAt: { gate: gateSuspendedAt, other: otherSuspendedAt },
    };
    const resumedSteps: unknown[] = [];
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => bothSuspended),
      resume: vi.fn(
        async (_workflowId, _runId, options: { step?: unknown }) => {
          resumedSteps.push(options.step);
          // The step the first wake did not touch is still suspended, so its
          // deadline must survive the wake that consumed the other one.
          return resumedSteps.length === 1
            ? { ...bothSuspended, suspended: [['other']] }
            : { runId: 'run-pair', status: 'success' };
        },
      ),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-pair', [
      armedEntry('gate', gateSuspendedAt),
      armedEntry('other', otherSuspendedAt),
    ]);
    const runner = new TestRunner(state, env);
    const before = Date.now();

    await runner.alarm();

    // #then — bounded work per wake: the earliest entry only, and the entry
    // left behind is re-armed at the floor rather than at its own past
    // deadline, which Cloudflare would fire immediately.
    expect(resumedSteps).toEqual([['gate']]);
    expect(storedDeadlines(values)?.entries).toEqual([
      armedEntry('other', otherSuspendedAt),
    ]);
    expect(alarms.at(-1)).toBeGreaterThan(before);
    expect(alarms.at(-1)).toBeLessThanOrEqual(Date.now() + 1_000);

    await runner.alarm();

    expect(resumedSteps).toEqual([['gate'], ['other']]);
    expect(storedDeadlines(values)).toBeUndefined();
  });

  it('serves the deadline duty in a wake whose recovery journal is poisoned', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const runner = new TestRunner(state, timedEnv());
    await startTimed(runner, 'run-both-duties');
    elapseDeadlines(values);
    // A journal this instance cannot parse: run-owner recovery throws, and its
    // throw is what asks workerd to retry the wake. The deadline duty must
    // still have run, or one poisoned journal would strand every deadline the
    // run ever arms.
    values.set('flowsafe:run-owner-recovery:v1', { version: 2 });
    const before = Date.now();

    await expect(runner.alarm()).rejects.toThrow(
      'stored run owner recovery is malformed',
    );

    const status = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed/run-both-duties'),
    );
    expect(await status.json()).toMatchObject({
      status: 'success',
      result: { settledBy: 'timeout' },
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
    });
    expect(storedDeadlines(values)).toBeUndefined();
    // #then — the recovery duty still owns its own retry wake
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 60_000);
  });

  it('runs the expired step body once when a real resume races the wake', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    const counted = countedTimedRuntime(env.storage);
    env.runtime = counted.runtime;
    const runner = new TestRunner(state, env);
    await startTimed(runner, 'run-race');
    elapseDeadlines(values);

    // #when — the wake and a genuine signal arrive together. The operation
    // lock is a FIFO mutex held across the whole alarm body, so the resume
    // cannot land between the fence check and the timeout resume.
    const wake = runner.alarm();
    const signal = runner.fetch(
      post('/runs/timed/run-race/resume', {
        step: 'gate',
        resumeData: { approvedBy: 'bob' },
      }),
    );
    const [, response] = await Promise.all([wake, signal]);

    // #then — the gated action ran exactly once, and the loser is told so
    expect(counted.settled()).toBe(1);
    expect(response.status).toBe(409);
    expect(storedDeadlines(values)).toBeUndefined();
  });

  it('arms, fences and resumes a top-level step id containing a dot', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const runner = new TestRunner(state, timedEnv());

    const started = await startTimed(runner, 'run-dotted', 'timed-dotted');
    const suspendedAt = started.suspendedAt?.[DOTTED_STEP] as number;

    // #then — the live summary reports the id whole, and the entry is keyed by
    // the same joined path the payload and the fence are keyed by.
    expect(started.suspended).toEqual([DOTTED_STEP.split('.')]);
    expect(storedDeadlines(values)?.entries).toEqual([
      armedEntry(DOTTED_STEP, suspendedAt),
    ]);

    // #when — the wake fences against the REHYDRATED projection, which splits
    // that id back into segments.
    const rehydrated = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed-dotted/run-dotted'),
    );
    expect(((await rehydrated.json()) as RunSummary).suspended).toEqual([
      ['wait', 'signal'],
    ]);
    elapseDeadlines(values);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — the deadline fired: the entry is recognized, not treated as a
    // moved fence and dropped, which is how it disappears in silence.
    const status = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed-dotted/run-dotted'),
    );
    expect(await status.json()).toMatchObject({
      status: 'success',
      result: { settledBy: 'timeout' },
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
    });
    expect(logged).toEqual([]);
    expect(storedDeadlines(values)).toBeUndefined();
  });

  it('arms the deadline of a run whose start was interrupted', async () => {
    const events: string[] = [];
    const { state, values } = durableKeyValueStorageFixture(events);
    const env = timedEnv();
    const token = 'attempt-token';
    // The start leg as an interrupted one leaves it: the claim is reserved and
    // journaled, Mastra has persisted the suspension, and the isolate died
    // before the route could reconcile. Recovery is the only boundary left.
    await env.owners.reserveAll(
      [{ kind: 'run', resourceId: 'run-interrupted' }],
      OWNER_PRINCIPAL,
      token,
    );
    const started = await (env.runtime as RunnerRuntime).start('timed', {
      runId: 'run-interrupted',
      inputData: { topic: 't' },
      requestedBy: OWNER_PRINCIPAL.id,
      requestedByKind: OWNER_PRINCIPAL.kind,
      attemptToken: token,
    });
    expect(started.status).toBe('suspended');
    values.set(
      'flowsafe:run-owner-recovery:v1',
      await durableOwnerRecovery(
        env.runtime as RunnerRuntime,
        'timed',
        'run-interrupted',
        token,
      ),
    );
    const runner = new TestRunner(state, env);

    await runner.alarm();

    // #then — the recovery wake armed from the summary recoverStartAttempt read
    // back. Without it the journal clears, #armNextAlarm finds no record and
    // DELETES the alarm, and a suspended run is left with no wake at all.
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    expect(storedDeadlines(values)?.entries).toEqual([
      armedEntry('gate', started.suspendedAt?.gate as number),
    ]);
    expect(events.at(-1)).not.toBe('deleteAlarm');
  });

  it('keeps the recovery cadence when a wake cannot verify its deployment', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const env = timedEnv();
    // A namespace bound to another deployment's database: verification throws on
    // every wake, so the deadline duty never runs and its ledger can never
    // converge. Arming at the past deadline would then wake this object every
    // second for as long as the misbinding lasts.
    env.DB = deploymentIdentityDatabase('globex');
    const armed = armedEntry('gate', 1);
    seedDeadlines(values, 'run-misbound', [armed]);
    const runner = new TestRunner(state, env);
    const before = Date.now();

    for (let wake = 0; wake < 3; wake += 1) {
      await expect(runner.alarm()).rejects.toThrow("belongs to 'globex'");
    }

    // #then — every arm is the pre-feature 60s recovery watchdog, never the
    // one-second arm floor, and the untouched entry is still armed for when the
    // binding is fixed.
    expect(alarms).toHaveLength(3);
    for (const at of alarms) {
      expect(at).toBeGreaterThanOrEqual(before + 60_000);
    }
    expect(storedDeadlines(values)?.entries).toEqual([armed]);
  });

  it('charges the entry the wake was working on when its own re-arm fails', async () => {
    const { storage, values } = durableKeyValueStorageFixture();
    let setAlarmCalls = 0;
    const state = {
      storage: {
        ...storage,
        setAlarm: async (at: number | Date) => {
          setAlarmCalls += 1;
          // 1 = the watchdog, 2 = the re-arm inside the FIRST entry's charge.
          if (setAlarmCalls === 2) throw new Error('transient alarm failure');
          return storage.setAlarm?.(at);
        },
      },
    } as unknown as DurableObjectState;
    const gateSuspendedAt = Date.now() - TIMED_DEADLINE_MS - 2;
    const otherSuspendedAt = gateSuspendedAt + 1;
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => ({
        runId: 'run-pair',
        status: 'suspended',
        suspended: [['gate'], ['other']],
        suspendPayload: {
          gate: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
          other: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
        },
        suspendedAt: { gate: gateSuspendedAt, other: otherSuspendedAt },
      })),
      resume: vi.fn(async () => {
        throw new Error('injected resume failure for gate');
      }),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-pair', [
      armedEntry('gate', gateSuspendedAt),
      armedEntry('other', otherSuspendedAt),
    ]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    try {
      await expect(runner.alarm()).resolves.toBeUndefined();
    } finally {
      log.mockRestore();
    }

    // #then — 'gate' failed, so 'gate' pays. Re-selecting a due entry after the
    // failure would charge 'other' for a resume it never attempted: pushed a
    // minute late, a fifth of its budget spent, and named in the operator log.
    const entries = storedDeadlines(values)?.entries ?? [];
    expect(entries.find((entry) => entry.step === 'other')).toEqual(
      armedEntry('other', otherSuspendedAt),
    );
    expect(entries.find((entry) => entry.step === 'gate')?.attempts).toBe(1);
    expect(
      logged.filter((message) => message.includes('failed (attempt 1)')),
    ).toEqual([
      "suspension deadline wake for step 'gate' of run 'run-pair' failed (attempt 1)",
    ]);
  });

  it('touches no deadline storage for a run that arms nothing', async () => {
    const events: string[] = [];
    const { state } = durableKeyValueStorageFixture(events);
    const runner = new TestRunner(state, makeProductionEnv());
    const started = await startGated(runner);

    await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, {
        step: 'gate',
        resumeData: { approvedBy: 'bob' },
      }),
    );
    await runner.fetch(post(`/runs/gated/${started.runId}/terminate`, {}));

    // #then — a consumer that never arms a deadline pays reads, never writes:
    // an unconditional delete at each lifecycle boundary would charge every
    // existing runner user for a feature it does not use.
    expect(
      events.filter(
        (event) =>
          event.endsWith(SUSPENSION_DEADLINE_STORAGE_KEY) &&
          !event.startsWith('get:'),
      ),
    ).toEqual([]);
  });

  it('reports an unarmable deadline once, not at every boundary', async () => {
    const { state } = durableKeyValueStorageFixture();
    const runner = new TestRunner(state, timedEnv());
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    try {
      // The step re-suspends with the same out-of-range value on every resume,
      // so start and resume derive the identical rejection.
      const started = await startTimed(
        runner,
        'run-unarmable',
        'timed-unarmable',
      );
      expect(started.status).toBe('suspended');
      const resumed = await runner.fetch(
        post('/runs/timed-unarmable/run-unarmable/resume', {
          step: 'gate',
          resumeData: { approvedBy: 'bob' },
        }),
      );
      expect(resumed.status).toBe(200);
    } finally {
      log.mockRestore();
    }

    // #then — the operator is told once. Repeating an unchanged rejection at
    // every boundary buries the rest of the log under one author mistake.
    expect(
      logged.filter((message) =>
        message.startsWith('suspension deadline not armed for step'),
      ),
    ).toEqual([
      `suspension deadline not armed for step 'gate' of run 'run-unarmable': ${SUSPENSION_DEADLINE_PAYLOAD_KEY} must be between ${MIN_SUSPENSION_DEADLINE_MS} and ${MAX_SUSPENSION_DEADLINE_MS} ms`,
    ]);
  });

  it('refuses a client resume that forges the timeout envelope', async () => {
    const runner = makeRunner();
    const started = await startGated(runner);

    const response = await runner.fetch(
      post(`/runs/gated/${started.runId}/resume`, {
        step: 'gate',
        resumeData: {
          [SUSPENSION_TIMEOUT_RESUME_KEY]: {
            step: 'gate',
            deadlineAt: 1,
            expiredAt: 2,
          },
        },
      }),
    );

    // #then — a caller allowed to resume must not be able to drive the timeout
    // branch while provenance still names them as the requester
    expect(response.status).toBe(400);
    expect((await response.json()) as { error: string }).toMatchObject({
      error: expect.stringContaining(SUSPENSION_TIMEOUT_RESUME_KEY),
    });
    const status = await runner.fetch(
      deploymentIdentityRequest(`http://do/runs/gated/${started.runId}`),
    );
    expect(((await status.json()) as RunSummary).status).toBe('suspended');
  });

  it('leaves a retry wake when the first deadline write of a start fails', async () => {
    const events: string[] = [];
    const { storage, values, alarms } = durableKeyValueStorageFixture(events);
    const { state } = deadlineWriteFailures(storage, {
      name: 'timed:run-first-arm',
      once: true,
    });
    const runner = new TestRunner(state, timedEnv());
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const before = Date.now();
    let started: RunSummary;

    try {
      started = await startTimed(runner, 'run-first-arm');
    } finally {
      log.mockRestore();
    }
    const suspendedAt = started.suspendedAt?.gate as number;

    // #then — the start succeeded, the run is suspended with a derivable
    // deadline, and nothing recorded it. Settling the reservation first would
    // re-arm from storage, find neither record nor journal and DELETE the
    // alarm: no record, no wake, deadline lost forever.
    expect(started.status).toBe('suspended');
    expect(storedDeadlines(values)).toBeUndefined();
    expect(events).not.toContain('deleteAlarm');
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 60_000);

    // #when — that retry wake fires, which is all this run is guaranteed
    const recovery = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      await runner.alarm();
    } finally {
      recovery.mockRestore();
    }

    // #then — with no record to work from, the wake re-derived from the run's
    // own authoritative state and armed what the failed write lost
    expect(storedDeadlines(values)?.entries).toEqual([
      armedEntry('gate', suspendedAt),
    ]);
    expect(alarms.at(-1)).toBe(suspendedAt + TIMED_DEADLINE_MS);
  });

  it('leaves a retry wake when a resume boundary is the first arm and it fails', async () => {
    const events: string[] = [];
    const { storage, values, alarms } = durableKeyValueStorageFixture(events);
    const { state, stop } = deadlineWriteFailures(storage, {
      name: 'timed-relay:run-relay',
    });
    const runner = new TestRunner(state, timedEnv());
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const before = Date.now();
    let resumed: RunSummary;

    try {
      await startTimed(runner, 'run-relay', 'timed-relay');
      // #when — a real signal resumes and the step suspends again. Nothing was
      // ever armed for this run, so unlike a later boundary there is no stale
      // record whose own alarm would heal the write that fails here.
      const response = await runner.fetch(
        post('/runs/timed-relay/run-relay/resume', {
          step: 'gate',
          resumeData: { approvedBy: 'bob' },
        }),
      );
      expect(response.status).toBe(200);
      resumed = (await response.json()) as RunSummary;
    } finally {
      log.mockRestore();
    }
    const suspendedAt = resumed.suspendedAt?.gate as number;

    expect(resumed.status).toBe('suspended');
    expect(storedDeadlines(values)).toBeUndefined();
    expect(events).not.toContain('deleteAlarm');
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 60_000);

    stop();
    await runner.alarm();

    // #then — the wake armed the re-suspension's own deadline, fenced on the
    // resume ordinal the signal produced
    expect(storedDeadlines(values)?.entries).toEqual([
      {
        step: 'gate',
        deadlineAt: suspendedAt + RETRY_DEADLINE_MS,
        suspendedAt,
        resumeCount: 1,
      },
    ]);
    expect(alarms.at(-1)).toBe(suspendedAt + RETRY_DEADLINE_MS);
  });

  it('keeps a wake when an interrupted start cannot record its deadline', async () => {
    const events: string[] = [];
    const { storage, values, alarms } = durableKeyValueStorageFixture(events);
    const { state, stop } = deadlineWriteFailures(storage, {
      name: 'timed:run-recovery-arm',
    });
    const env = timedEnv();
    const token = 'attempt-token';
    await env.owners.reserveAll(
      [{ kind: 'run', resourceId: 'run-recovery-arm' }],
      OWNER_PRINCIPAL,
      token,
    );
    const started = await (env.runtime as RunnerRuntime).start('timed', {
      runId: 'run-recovery-arm',
      inputData: { topic: 't' },
      requestedBy: OWNER_PRINCIPAL.id,
      requestedByKind: OWNER_PRINCIPAL.kind,
      attemptToken: token,
    });
    values.set(
      'flowsafe:run-owner-recovery:v1',
      await durableOwnerRecovery(
        env.runtime as RunnerRuntime,
        'timed',
        'run-recovery-arm',
        token,
      ),
    );
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const before = Date.now();

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — recovery cleared the journal it exists to clear, and left the
    // wake the failed write needs instead of re-arming from a record that was
    // never written, which would have deleted the alarm outright
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    expect(storedDeadlines(values)).toBeUndefined();
    expect(events).not.toContain('deleteAlarm');
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 60_000);

    stop();
    await runner.alarm();

    const suspendedAt = started.suspendedAt?.gate as number;
    expect(storedDeadlines(values)?.entries).toEqual([
      armedEntry('gate', suspendedAt),
    ]);
  });

  it('writes nothing on a no-record wake for a run that is not suspended', async () => {
    const events: string[] = [];
    const { storage, values } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-finished' },
      storage,
    } as unknown as DurableObjectState;
    const env = makeProductionEnv();
    const reads = statusStub(async () => ({
      runId: 'run-finished',
      status: 'success',
    }));
    env.runtime = { ...reads } as unknown as RunnerRuntime;
    const runner = new TestRunner(state, env);

    await runner.alarm();

    // #then — the wake re-derives from the run this object is named for, and a
    // run that is not suspended derives nothing: no write, and no alarm left
    // for an object with no duty pending.
    expect(reads.authoritativeStatus).toHaveBeenCalledWith(
      'timed',
      'run-finished',
    );
    expect(storedDeadlines(values)).toBeUndefined();
    expect(
      events.filter(
        (event) =>
          event.endsWith(SUSPENSION_DEADLINE_STORAGE_KEY) &&
          !event.startsWith('get:'),
      ),
    ).toEqual([]);
    // #then — the record key is read exactly TWICE: the duty's own read, and
    // the converged arm at the end of the body. The reconcile in between pays
    // no read of its own, because it is told this wake already looked and
    // found nothing — which `??` could not tell from "no record supplied".
    expect(
      events.filter(
        (event) => event === `get:${SUSPENSION_DEADLINE_STORAGE_KEY}`,
      ),
    ).toHaveLength(2);
    expect(events.at(-1)).toBe('deleteAlarm');
  });

  it('keeps the recovery cadence when the retry ledger itself cannot be written', async () => {
    const { storage, values, alarms } = durableKeyValueStorageFixture();
    const { state } = deadlineWriteFailures(storage);
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const armed = armedEntry('gate', armedAt);
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => suspendedFence('run-ledger', armedAt)),
      resume: vi.fn(async () => {
        throw new Error('injected resume failure');
      }),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-ledger', [armed]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — the ledger that would back this entry off is the write that
    // keeps failing, so the wake converges nothing and keeps the recovery
    // cadence. Arming from a still-due entry instead would land on the
    // one-second floor and wake this object every second while storage is out.
    expect(alarms.filter((at) => at < before + 60_000)).toEqual([]);
    expect(logged).toContain('suspension deadline ledger update failed');
    expect(storedDeadlines(values)?.entries).toEqual([armed]);
  });

  it('keeps the recovery cadence when an unwritable ledger shares a wake with a poisoned journal', async () => {
    const { storage, values, alarms } = durableKeyValueStorageFixture();
    const { state } = deadlineWriteFailures(storage);
    const armedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => suspendedFence('run-poisoned-ledger', armedAt)),
      resume: vi.fn(async () => {
        throw new Error('injected resume failure');
      }),
    } as unknown as RunnerRuntime;
    // The recovery duty rethrows to ask workerd for a retry, and the re-arm on
    // that path takes the min with the stored entry — so it is the second way
    // into the same one-second spin.
    values.set('flowsafe:run-owner-recovery:v1', { version: 2 });
    seedDeadlines(values, 'run-poisoned-ledger', [armedEntry('gate', armedAt)]);
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const before = Date.now();

    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).rejects.toThrow(
          'stored run owner recovery is malformed',
        );
      }
    } finally {
      log.mockRestore();
    }

    expect(alarms.filter((at) => at < before + 60_000)).toEqual([]);
  });

  it('arms nothing and runs no step body when two suspensions share one key', async () => {
    const { storage, values } = durableKeyValueStorageFixture();
    const state = {
      id: { name: 'timed-collision:run-collision' },
      storage,
    } as unknown as DurableObjectState;
    const env = makeProductionEnv();
    const colliding = collidingRuntime(env.storage);
    env.runtime = colliding.runtime;
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    try {
      const started = await startTimed(
        runner,
        'run-collision',
        'timed-collision',
      );
      expect(started.status).toBe('suspended');
      // #when — the only wake this run gets fires, and it reads the OTHER
      // projection: the live boundary above saw two paths joining to 'a.b',
      // this one sees 'a.b' and the nested 'a' whose marker implies it.
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — refused on both projections, so no entry exists to deliver one
    // suspension's timeout envelope to the other suspension's step
    expect(storedDeadlines(values)).toBeUndefined();
    expect(colliding.settled()).toEqual([]);
    expect(logged).toContain(
      "suspension deadline not armed for step 'a.b' of run 'run-collision': ambiguous suspended step path",
    );
  });

  it('resumes one foreach iteration per wake and re-arms from the new fence', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    const each = foreachRuntime(env.storage, 'timed-foreach');
    env.runtime = each.runtime;
    const runner = new TestRunner(state, env);
    const before = Date.now();

    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'timed-foreach',
        runId: 'run-foreach',
        inputData: [{ item: 0 }, { item: 1 }],
      }),
    );
    expect(((await response.json()) as RunSummary).status).toBe('suspended');
    elapseDeadlines(values);

    await runner.alarm();

    // #then — a default foreach is sequential: one iteration is suspended at a
    // time, so the expired deadline belongs to iteration 0, and the iteration
    // that follows suspends as a NEW suspension with its own fence and its own
    // deadline. Clearing a whole foreach by timeout therefore takes one
    // deadline per item.
    expect(each.timedOut()).toEqual([0]);
    const entry = storedDeadlines(values)?.entries[0];
    expect(entry?.resumeCount).toBe(1);
    expect(entry?.deadlineAt).toBe(
      (entry?.suspendedAt as number) + TIMED_DEADLINE_MS,
    );
    expect(alarms.at(-1)).toBe(entry?.deadlineAt);

    await runner.alarm();
    await runner.alarm();

    // #then — the consumed suspension never fires again, and no wake lands on
    // the one-second arm floor, which is what a repeat fire would look like
    expect(each.timedOut()).toEqual([0]);
    expect(storedDeadlines(values)?.entries).toEqual([entry]);
    expect(alarms.filter((at) => at <= before)).toEqual([]);
  });

  it('resumes every suspended iteration of a concurrent foreach in one wake', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    const each = foreachRuntime(env.storage, 'timed-foreach-concurrent', {
      concurrency: 3,
    });
    env.runtime = each.runtime;
    const runner = new TestRunner(state, env);

    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'timed-foreach-concurrent',
        runId: 'run-foreach-concurrent',
        inputData: [{ item: 0 }, { item: 1 }, { item: 2 }],
      }),
    );
    expect(((await response.json()) as RunSummary).status).toBe('suspended');
    // #then — all three iterations are suspended, and both projections report
    // ONE path with one fence and iteration 0's payload, so one entry is armed
    // for the whole foreach — at ITERATION 0's deadline, even though every
    // iteration asked for a different one.
    const armed = storedDeadlines(values)?.entries;
    expect(armed).toHaveLength(1);
    expect(armed?.[0]?.deadlineAt).toBe(
      (armed?.[0]?.suspendedAt as number) + TIMED_DEADLINE_MS,
    );
    elapseDeadlines(values);

    await runner.alarm();
    await runner.alarm();

    // #then — that one resume delivers the envelope to every iteration still
    // suspended, so the foreach clears in a single wake rather than one
    // iteration at a time, and the settled entry cannot fire again
    expect(each.timedOut()).toEqual([0, 1, 2]);
    expect(storedDeadlines(values)).toBeUndefined();
    const status = await runner.fetch(
      deploymentIdentityRequest(
        'http://do/runs/timed-foreach-concurrent/run-foreach-concurrent',
      ),
    );
    expect(((await status.json()) as RunSummary).status).toBe('success');
  });

  it('clears a concurrent foreach with more items than concurrency batch by batch', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    const each = foreachRuntime(env.storage, 'timed-foreach-batched', {
      concurrency: 2,
    });
    env.runtime = each.runtime;
    const runner = new TestRunner(state, env);

    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'timed-foreach-batched',
        runId: 'run-foreach-batched',
        inputData: [{ item: 0 }, { item: 1 }, { item: 2 }],
      }),
    );
    expect(((await response.json()) as RunSummary).status).toBe('suspended');
    const firstFence = storedDeadlines(values)?.entries[0];
    elapseDeadlines(values);

    await runner.alarm();

    // #then — the timeout resume delivered the envelope to every iteration
    // suspended AT THAT MOMENT: the first batch of `concurrency` items, not
    // the whole foreach. Iteration 2 had not started, so it suspends afresh
    // with its own fence and its own deadline, and the run stays suspended.
    expect(each.timedOut()).toEqual([0, 1]);
    const entry = storedDeadlines(values)?.entries[0];
    expect(entry?.resumeCount).toBe(1);
    expect(entry).not.toEqual(firstFence);
    // #then — and the new entry carries the value ITERATION 2 asked for: each
    // batch reads the deadline of the first iteration suspended in it, not the
    // one the first batch was armed with.
    expect(firstFence?.deadlineAt).toBe(
      (firstFence?.suspendedAt as number) + TIMED_DEADLINE_MS,
    );
    expect(entry?.deadlineAt).toBe(
      (entry?.suspendedAt as number) + TIMED_DEADLINE_MS + 2 * 60_000,
    );
    const mid = await runner.fetch(
      deploymentIdentityRequest(
        'http://do/runs/timed-foreach-batched/run-foreach-batched',
      ),
    );
    expect(((await mid.json()) as RunSummary).status).toBe('suspended');
    elapseDeadlines(values);

    await runner.alarm();

    // #then — clearing 3 items at concurrency 2 took ceil(3 / 2) deadlines
    expect(each.timedOut()).toEqual([0, 1, 2]);
    expect(storedDeadlines(values)).toBeUndefined();
    const status = await runner.fetch(
      deploymentIdentityRequest(
        'http://do/runs/timed-foreach-batched/run-foreach-batched',
      ),
    );
    expect(((await status.json()) as RunSummary).status).toBe('success');
  });

  it.each([
    ['a path-unsafe segment', 'a/b:c'],
    ['three segments', 'timed:run:extra'],
    ['no separator', 'nocolon'],
    ['an empty runId', 'timed:'],
    ['an empty workflowId', ':run'],
  ])('never lets an object name with %s steer a status read on a no-record wake', async (_label, name) => {
    // 'a/b:c' splits into status('a/b', 'c') unless the name is validated;
    // a record written from an unvalidated name would discard itself on
    // read-back. The other four shapes are skipped — regression pins.
    const events: string[] = [];
    const { storage } = durableKeyValueStorageFixture(events);
    const state = { id: { name }, storage } as unknown as DurableObjectState;
    const env = makeProductionEnv();
    const reads = statusStub(async () => null);
    env.runtime = { ...reads } as unknown as RunnerRuntime;
    const runner = new TestRunner(state, env);

    await runner.alarm();

    // The doubles expose both read methods, so malformed parsed ids must reach
    // neither one.
    expect(reads.authoritativeStatus).not.toHaveBeenCalled();
    expect(reads.status).not.toHaveBeenCalled();
    expect(events.at(-1)).toBe('deleteAlarm');
  });

  it('converges to no alarm on a no-record wake without an object name', async () => {
    const events: string[] = [];
    const { state, values } = durableKeyValueStorageFixture(events);
    const env = makeProductionEnv();
    const reads = statusStub(async () => null);
    env.runtime = { ...reads } as unknown as RunnerRuntime;
    const runner = new TestRunner(state, env);

    await runner.alarm();

    // #then — without a name and without a record there is no identity to
    // re-derive from, so the retry wake ends in a delete and a deadline whose
    // record was never written is lost. Production is safe from this: the
    // exported topology always addresses the runner object by idFromName.
    expect(reads.authoritativeStatus).not.toHaveBeenCalled();
    expect(reads.status).not.toHaveBeenCalled();
    expect(storedDeadlines(values)).toBeUndefined();
    expect(events.at(-1)).toBe('deleteAlarm');
  });

  it('keeps the record and its wake when a nothing-due wake reads a degraded summary', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    // Mastra's in-memory fallback: storage unavailable while the isolate still
    // holds the run — 'suspended' with no suspended paths and no fences.
    const reads = statusStub(async () => ({
      runId: 'run-degraded',
      status: 'suspended',
      suspended: [],
    }));
    const resume = vi.fn();
    env.runtime = { ...reads, resume } as unknown as RunnerRuntime;
    const entry = armedEntry('gate', Date.now());
    seedDeadlines(values, 'run-degraded', [entry]);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const runner = new TestRunner(state, env);

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — a degraded read is not evidence: reconciling from it would have
    // derived nothing and wiped the record of a run that is still suspended.
    // The record stays, the alarm re-arms at the entry's own deadline, and the
    // operator is told once.
    expect(reads.authoritativeStatus).toHaveBeenCalledWith(
      'timed',
      'run-degraded',
    );
    // A silent degradation to the watchdog path would look the same from the
    // record alone, so the resume is pinned too: nothing ran.
    expect(resume).not.toHaveBeenCalled();
    expect(storedDeadlines(values)?.entries).toEqual([entry]);
    expect(alarms.at(-1)).toBe(entry.deadlineAt);
    // #then — under its own wording: this refusal charges nothing, and the
    // resume path's identically-shaped refusal charges the entry's budget, so
    // the two must not read the same in an operator's log.
    expect(
      logged.filter((message) => message.includes('self-inconsistent')),
    ).toEqual([
      "run 'run-degraded' of workflow 'timed' state read back self-inconsistent; keeping the record",
    ]);
  });

  it('charges the ledger instead of wiping the record when a due wake reads a degraded summary', async () => {
    const { state, values } = durableKeyValueStorageFixture();
    const env = makeProductionEnv();
    const reads = statusStub(async () => ({
      runId: 'run-degraded-due',
      status: 'suspended',
      suspended: [],
    }));
    const resume = vi.fn();
    env.runtime = { ...reads, resume } as unknown as RunnerRuntime;
    const due = armedEntry('gate', Date.now() - TIMED_DEADLINE_MS - 1);
    const unrelated = armedEntry('other', Date.now());
    seedDeadlines(values, 'run-degraded-due', [due, unrelated]);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();
    const runner = new TestRunner(state, env);

    try {
      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — the degraded summary matches no fence, so without the guard the
    // 'moved on' reconcile would have deleted the due entry AND the unrelated
    // far-future one. Instead the wake is charged as unreadable and retried.
    expect(resume).not.toHaveBeenCalled();
    expect(logged.some((message) => message.includes('has moved on'))).toBe(
      false,
    );
    const entries = storedDeadlines(values)?.entries ?? [];
    expect(entries.find((entry) => entry.step === 'other')).toEqual(unrelated);
    const charged = entries.find((entry) => entry.step === 'gate');
    expect(charged?.attempts).toBe(1);
    expect(charged?.nextAttemptAt).toBeGreaterThanOrEqual(before + 59_000);
  });

  it('re-arms to the current suspension when a retry wake holds only a stale record', async () => {
    const { storage, values, alarms } = durableKeyValueStorageFixture();
    const { state, fail } = deadlineWriteFailures(storage, {
      name: 'timed-relay-shortening:run-shortening',
      once: true,
      armed: false,
    });
    const runner = new TestRunner(state, timedEnv());
    const started = await startTimed(
      runner,
      'run-shortening',
      'timed-relay-shortening',
    );
    const firstSuspendedAt = started.suspendedAt?.gate as number;
    const staleDeadlineAt = firstSuspendedAt + RETRY_DEADLINE_MS;
    expect(storedDeadlines(values)?.entries).toEqual([
      { ...armedEntry('gate', firstSuspendedAt), deadlineAt: staleDeadlineAt },
    ]);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    let resumed: RunSummary;

    try {
      // #when — a real signal re-suspends the step with a SHORTER deadline and
      // the write recording it fails once: the record still holds the old
      // far-future entry, and the failed reconcile left a 60 s retry wake.
      fail();
      const response = await runner.fetch(
        post('/runs/timed-relay-shortening/run-shortening/resume', {
          step: 'gate',
          resumeData: { approvedBy: 'bob' },
        }),
      );
      expect(response.status).toBe(200);
      resumed = (await response.json()) as RunSummary;

      await runner.alarm();
    } finally {
      log.mockRestore();
    }

    // #then — the retry wake found nothing due and re-derived anyway: without
    // that, it would have re-armed to the stale far-future entry and the
    // 15-minute deadline would never fire.
    const suspendedAt = resumed.suspendedAt?.gate as number;
    expect(storedDeadlines(values)?.entries).toEqual([
      { ...armedEntry('gate', suspendedAt), resumeCount: 1 },
    ]);
    expect(alarms.at(-1)).toBe(suspendedAt + TIMED_DEADLINE_MS);
    expect(alarms.at(-1)).toBeLessThan(staleDeadlineAt);
  });

  it('arms a parallel suspension missed by a failed write on the next nothing-due wake', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const gateSuspendedAt = Date.now();
    const otherSuspendedAt = Date.now() + 1;
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => ({
        runId: 'run-parallel',
        status: 'suspended',
        suspended: [['gate'], ['other']],
        suspendPayload: {
          gate: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
          other: { [SUSPENSION_DEADLINE_PAYLOAD_KEY]: TIMED_DEADLINE_MS },
        },
        suspendedAt: { gate: gateSuspendedAt, other: otherSuspendedAt },
      })),
    } as unknown as RunnerRuntime;
    // Only 'gate' ever made it into the record: 'other' suspended in parallel
    // and the boundary write that would have armed it failed.
    seedDeadlines(values, 'run-parallel', [
      armedEntry('gate', gateSuspendedAt),
    ]);
    const runner = new TestRunner(state, env);

    await runner.alarm();

    expect(storedDeadlines(values)?.entries).toEqual([
      armedEntry('gate', gateSuspendedAt),
      armedEntry('other', otherSuspendedAt),
    ]);
    expect(alarms.at(-1)).toBe(gateSuspendedAt + TIMED_DEADLINE_MS);
  });

  it('clears a stale record and its alarm when the run moved on with nothing to arm', async () => {
    const events: string[] = [];
    const { state, values } = durableKeyValueStorageFixture(events);
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => ({
        runId: 'run-moved-on',
        status: 'success',
      })),
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-moved-on', [armedEntry('gate', Date.now())]);
    const runner = new TestRunner(state, env);

    await runner.alarm();

    // #then — no permanent heartbeat: a run with no duty left converges to no
    // record and no alarm on its first nothing-due wake.
    expect(storedDeadlines(values)).toBeUndefined();
    expect(events.at(-1)).toBe('deleteAlarm');
  });

  it('keeps the backoff of a re-derived entry instead of the past deadline or the floor', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const suspendedAt = Date.now() - TIMED_DEADLINE_MS - 1;
    const nextAttemptAt = Date.now() + 300_000;
    const entry = {
      ...armedEntry('gate', suspendedAt),
      attempts: 3,
      nextAttemptAt,
    };
    const resume = vi.fn();
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => suspendedFence('run-backed-off', suspendedAt)),
      resume,
    } as unknown as RunnerRuntime;
    seedDeadlines(values, 'run-backed-off', [entry]);
    const runner = new TestRunner(state, env);

    await runner.alarm();

    // #then — the nothing-due re-derive carried the ledger across the merge:
    // the entry keeps its three failures and arms at its own backoff floor,
    // never at the long-past deadline the clean re-derived entry would carry.
    expect(resume).not.toHaveBeenCalled();
    expect(storedDeadlines(values)?.entries).toEqual([entry]);
    expect(alarms.at(-1)).toBe(nextAttemptAt);
  });

  it('reconciles idempotently on a wake just before the deadline and resumes once after it', async () => {
    const { state, values, alarms } = durableKeyValueStorageFixture();
    // Authoritative state whose derived deadline lands 150 ms ahead of the
    // wake — workerd can deliver an alarm marginally early.
    const suspendedAt = Date.now() + 150 - TIMED_DEADLINE_MS;
    const resume = vi.fn(async () => ({
      runId: 'run-early',
      status: 'success',
    }));
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => suspendedFence('run-early', suspendedAt)),
      resume,
    } as unknown as RunnerRuntime;
    const entry = armedEntry('gate', suspendedAt);
    seedDeadlines(values, 'run-early', [entry]);
    const before = Date.now();
    const runner = new TestRunner(state, env);

    await runner.alarm();

    // #then — no early resume and no ledger charge: the wake re-derived the
    // byte-identical entry and armed at the one-second floor past it.
    expect(resume).not.toHaveBeenCalled();
    expect(storedDeadlines(values)?.entries).toEqual([entry]);
    expect(alarms.at(-1)).toBeGreaterThanOrEqual(before + 1_000);

    elapseDeadlines(values);
    await runner.alarm();

    // #then — the deadline still fires exactly once
    expect(resume).toHaveBeenCalledTimes(1);
    expect(storedDeadlines(values)).toBeUndefined();
  });

  it('keeps the recovery cadence when a no-record wake cannot read authoritative state', async () => {
    const events: string[] = [];
    const { storage, alarms } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-status-throws' },
      storage,
    } as unknown as DurableObjectState;
    const env = makeProductionEnv();
    const reads = statusStub(async () => {
      throw new Error('workflow no longer registered');
    });
    const resume = vi.fn();
    env.runtime = { ...reads, resume } as unknown as RunnerRuntime;
    const runner = new TestRunner(state, env);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    const before = Date.now();

    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — one status read per wake at the watchdog cadence: never the
    // one-second floor, never a delete, nothing executed. A throw keeps the
    // cadence because it cannot distinguish an unregistered workflow — which
    // the next deploy self-heals — from a storage fault.
    expect(reads.authoritativeStatus).toHaveBeenCalledTimes(3);
    expect(alarms).toHaveLength(3);
    for (const at of alarms) {
      expect(at).toBeGreaterThanOrEqual(before + 60_000);
    }
    expect(events).not.toContain('deleteAlarm');
    expect(resume).not.toHaveBeenCalled();
  });

  it('keeps a valid record on the recovery cadence when its nothing-due wake cannot read state', async () => {
    const events: string[] = [];
    const { storage, values, alarms } = durableKeyValueStorageFixture(events);
    const state = {
      id: { name: 'timed:run-throws-recorded' },
      storage,
    } as unknown as DurableObjectState;
    const env = makeProductionEnv();
    env.runtime = {
      ...statusStub(async () => {
        throw new Error('workflow no longer registered');
      }),
    } as unknown as RunnerRuntime;
    const entry = armedEntry('gate', Date.now());
    seedDeadlines(values, 'run-throws-recorded', [entry]);
    const runner = new TestRunner(state, env);
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    try {
      for (let wake = 0; wake < 3; wake += 1) {
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — the re-derive read is wrapped exactly as the resume path's is,
    // so a wake that charges nothing says so: an operator greps one line for
    // an unreadable window, not the charged-failure wording.
    expect(logged).toEqual([
      'suspension deadline wake could not read authoritative state',
      'suspension deadline wake could not read authoritative state',
      'suspension deadline wake could not read authoritative state',
    ]);

    // #then — the record SURVIVES: the re-derive could not read authoritative
    // state, so nothing overwrote or dropped the valid entry, and every arm is
    // the watchdog's, not the entry's far-future deadline it could not verify.
    expect(storedDeadlines(values)?.entries).toEqual([entry]);
    expect(alarms).toHaveLength(3);
    for (const at of alarms) {
      expect(at).toBeGreaterThanOrEqual(before + 60_000);
    }
    expect(events).not.toContain('deleteAlarm');
  });
});

describe('nextDutyAlarmAt', () => {
  const NOW = 1_751_882_400_000;

  it('has no arm when neither duty is pending', () => {
    expect(nextDutyAlarmAt(undefined, undefined, NOW)).toBeUndefined();
  });

  it('arms the one pending duty', () => {
    expect(nextDutyAlarmAt(NOW + 5_000, undefined, NOW)).toBe(NOW + 5_000);
    expect(nextDutyAlarmAt(undefined, NOW + 60_000, NOW)).toBe(NOW + 60_000);
  });

  it('takes the earlier of the two duties', () => {
    expect(nextDutyAlarmAt(NOW + 5_000, NOW + 60_000, NOW)).toBe(NOW + 5_000);
    expect(nextDutyAlarmAt(NOW + 90_000, NOW + 60_000, NOW)).toBe(NOW + 60_000);
  });

  it('floors an already-due time a second out instead of arming in the past', () => {
    // Cloudflare fires a past alarm immediately; the floor is the anti-spin
    // guarantee for an entry that is already due.
    expect(nextDutyAlarmAt(NOW - 5_000, undefined, NOW)).toBe(NOW + 1_000);
    expect(nextDutyAlarmAt(NOW - 5_000, NOW + 60_000, NOW)).toBe(NOW + 1_000);
  });
});

describe('DurableObjectRunner and the deployment execution fence', () => {
  it('refuses a fenced start before ANY of its own storage writes', async () => {
    // #given — a locked deployment and a start that would otherwise journal a
    // recovery record, arm an alarm, and reserve the run's owner.
    const events: string[] = [];
    const { state } = durableKeyValueStorageFixture(events);
    const reserve = vi.fn(async () => true);
    const env = makeProductionEnv(testStorage(), {
      reserve,
      settle: vi.fn(async () => undefined),
    });
    await env.fence?.seed('migration-locked');
    const runner = new TestRunner(state, env);
    events.length = 0;

    // #when
    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'fenced-start',
        inputData: { topic: 't' },
      }),
    );

    // #then — the refusal carries the taxonomy's retryable status and code.
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      error:
        "deployment execution is fenced ('migration-locked'): run start is refused",
      reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
    });

    // #then — and NOTHING was written on the way to saying no: no owner
    // reservation in D1, and no DO-storage mutation at all (the recovery
    // journal, its alarm, or any delete). A deployment whose state is being
    // copied must not leave a run half-claimed behind the copy.
    expect(reserve).not.toHaveBeenCalled();
    expect(
      events.filter(
        (event) =>
          event.startsWith('put:') ||
          event.startsWith('delete:') ||
          event === 'setAlarm' ||
          event === 'deleteAlarm',
      ),
    ).toEqual([]);
  });

  it('keeps reads open while locked', async () => {
    // #given — a run started before the lock.
    const env = timedEnv();
    const { state } = durableKeyValueStorageFixture();
    const runner = new TestRunner(state, env);
    await startTimed(runner, 'fenced-read');
    await env.fence?.seed('open');
    await env.fence?.transition({
      expected: 'open',
      next: 'migration-locked',
    });

    // #when / #then — status still answers. An operator proving a deployment
    // drained needs to read it, and a read moves nothing.
    const response = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed/fenced-read'),
    );
    expect(response.status).toBe(200);
    expect((await response.json()) as RunSummary).toMatchObject({
      runId: 'fenced-read',
      status: 'suspended',
    });
  });

  it('leaves a due deadline uncharged and unconverged under a locked fence, then fires it after reopen', async () => {
    // #given — a suspended run with a due deadline on a locked deployment.
    const env = timedEnv();
    const { state, values, alarms } = durableKeyValueStorageFixture();
    const runner = new TestRunner(state, env);
    await startTimed(runner, 'fenced-deadline');
    elapseDeadlines(values);
    const armed = storedEntry(values, 'gate');
    await env.fence?.seed('open');
    await env.fence?.transition({
      expected: 'open',
      next: 'migration-locked',
    });
    // Only the wakes below are under test; the start's own arm is not.
    alarms.length = 0;
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });
    const before = Date.now();

    // #when — three wakes under the lock.
    try {
      for (let wake = 0; wake < 3; wake += 1) {
        // #then — an alarm NEVER rethrows: workerd retries a thrown alarm up
        // to six times, which would answer a deliberate operational state with
        // a wake storm.
        await expect(runner.alarm()).resolves.toBeUndefined();
      }
    } finally {
      log.mockRestore();
    }

    // #then — every arm is the 60 s watchdog, never the floored re-arm a
    // still-due entry would produce. The wake converged nothing, so it has no
    // arm of its own to compute.
    expect(alarms.filter((at) => at <= before)).toEqual([]);
    for (const at of alarms) {
      expect(at).toBeGreaterThanOrEqual(before + 60_000);
    }
    // #then — and the entry is untouched: not charged (five charged wakes
    // would tombstone it in about sixteen minutes of lock), not tombstoned,
    // and not stamped with the unreadable clock either — the read SUCCEEDED,
    // the deployment simply refused, and that clock's day-long abandonment
    // budget exists for a run whose state is permanently unreadable.
    const entry = storedEntry(values, 'gate');
    expect(entry).toEqual(armed);
    expect(entry).not.toHaveProperty('attempts');
    expect(entry).not.toHaveProperty('nextAttemptAt');
    expect(entry).not.toHaveProperty('unreadableSince');
    expect(
      logged.filter((message) =>
        message.includes('refused by the deployment execution fence'),
      ),
    ).toHaveLength(3);
    expect(logged).not.toContain('suspension deadline wake failed');

    // #when — the migration finishes and the operator reopens the fence.
    await env.fence?.transition({ expected: 'migration-locked', next: 'open' });
    await runner.alarm();

    // #then — the deadline fires. Nothing was lost while the fence was closed.
    const settled = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/timed/fenced-deadline'),
    );
    expect((await settled.json()) as RunSummary).toMatchObject({
      status: 'success',
      result: { settledBy: 'timeout' },
    });
  });

  it('refuses to serve from a fence-less runtime while a database is bound', async () => {
    // #given — a host that built a RunnerRuntime by hand inside build() and
    // forgot the fence, on a deployment that HAS a database.
    const env = makeProductionEnv();
    env.runtime = init(
      { storage: env.storage },
      { startIdempotency: 'none', executionFence: 'none' },
    ).runtime;
    const runner = new TestRunner(undefined, env);

    // #then — refused at the first request rather than silently executing
    // straight through a migration lock. Every other surface would report the
    // fence as wired, so nothing else would catch this. The MESSAGE is pinned
    // too: a bare 500 could be any fault, and this test would still pass while
    // the guard it exists for had stopped firing.
    const response = await runner.fetch(
      post('/runs', { workflowId: 'gated', runId: 'no-fence' }),
    );
    expect(response.status).toBe(500);
    expect(await response.json()).toMatchObject({
      error: expect.stringContaining(
        'returned a runtime with no execution fence',
      ),
    });
  });

  it('serves from a fence-less runtime when DB is an RPC binding, not a database', async () => {
    // #given — the OTHER direction of the same discrimination, and the one that
    // fails loudly for every host if it is wrong. `DB` here is a service
    // binding with a named entrypoint (fleet trusted state binds exactly this
    // beside its D1), which is an RPC proxy: it answers EVERY property with a
    // callable, so a bare `prepare` test says yes to it. There is no database
    // to fence against, so a fence-less runtime is correct — insisting on a
    // fence would refuse the first request every such Worker ever serves.
    const env = makeProductionEnv();
    env.DB = new Proxy(
      {},
      { get: () => () => undefined },
    ) as unknown as typeof env.DB;
    env.runtime = init(
      { storage: env.storage },
      { startIdempotency: 'none', executionFence: 'none' },
    ).runtime;
    const runner = new TestRunner(undefined, env, false);

    // #then — past the guard. 404 is this bare runtime answering for a workflow
    // it was never given; what matters is that it ANSWERED, where the D1-shaped
    // binding above produced the guard's 500.
    const response = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/rpc-db/start-liveness'),
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ live: false });
  });
});

// ---------------------------------------------------------------------------
// What the run object contributes to an idempotent start: it carries the
// key on the internal channel, and it answers the liveness probe that separates
// a run still working from a claim nobody is holding.
// ---------------------------------------------------------------------------

describe('DurableObjectRunner — idempotent start plumbing', () => {
  it('answers the liveness probe false for a run it is not executing', async () => {
    // #given a run object with nothing in flight
    const runner = makeRunner();

    // #when
    const response = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/run-idle/start-liveness'),
    );

    // #then. `false` is the fail-closed direction here: it produces the
    // refusal that asks a human to investigate, where a default of `true`
    // would answer a permanently dead run with a permanently retryable 503.
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ live: false });
  });

  it('answers the liveness probe true WHILE the start is executing, without queuing behind it', async () => {
    // #given a workflow whose first step blocks until the probe has answered.
    // This is the whole point of the route: the start holds the operation lock
    // for its entire first leg, so a probe that took that lock would block for
    // exactly as long as the run it was trying to describe.
    const storage = testStorage();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      {
        executionFence: newTestExecutionFence(storage),
        startIdempotency: newTestStartIdempotency(storage),
      },
    );
    let probed!: (value: unknown) => void;
    const probeAnswered = new Promise((resolve) => {
      probed = resolve;
    });
    let running!: (value: unknown) => void;
    const stepRunning = new Promise((resolve) => {
      running = resolve;
    });
    const blocking = createStep({
      id: 'blocking',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => {
        running(undefined);
        await probeAnswered;
        return {};
      },
    });
    createWorkflow({
      id: 'gated',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(blocking)
      .commit();
    const env = { ...makeProductionEnv(storage), runtime };
    const runner = new TestRunner(undefined, env);

    // #when the start is in flight and a probe arrives
    const started = runner.fetch(
      post('/runs', { workflowId: 'gated', runId: 'run-live', inputData: {} }),
    );
    await stepRunning;
    const probe = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/run-live/start-liveness'),
    );
    probed(undefined);
    await started;

    // #then the probe answered — promptly, and truthfully
    expect(await probe.json()).toEqual({ live: true });

    // #and once the start is done, so is the liveness
    const after = await runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/run-live/start-liveness'),
    );
    expect(await after.json()).toEqual({ live: false });
  });

  it('refuses an idempotency key that is not path-safe', async () => {
    // #given — the same string is compared against the fence's proof key and
    // stored as a reservation's primary key, so an unvalidated one reaches both
    const runner = makeRunner();

    // #when
    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-bad-key',
        inputData: { topic: 't' },
        idempotencyKey: 'key/../escape',
      }),
    );

    // #then
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({
      error: expect.stringContaining('idempotencyKey'),
    });
  });

  it('admits exactly the proof-only start that carries the nominated key, end to end', async () => {
    // #given a deployment fenced into proof-only, addressed through the route
    // a trusted Worker actually uses
    const storage = testStorage();
    const env = makeProductionEnv(storage);
    const fence = env.fence as ExecutionFenceStore;
    await fence.seed('migration-locked');
    await fence.transition({
      expected: 'migration-locked',
      next: 'proof-only',
      proofKey: 'proof-key-1',
    });
    const runner = new TestRunner(undefined, env);

    // #then a start with NO key is refused at the route, before this object
    // writes anything of its own
    const unkeyed = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-unkeyed',
        inputData: { topic: 't' },
      }),
    );
    expect(unkeyed.status).toBe(503);
    expect(await unkeyed.json()).toMatchObject({
      reason: { code: 'EXECUTION_FENCED', state: 'proof-only' },
    });

    // #and a start carrying the WRONG key is refused the same way
    const guessed = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-guessed',
        inputData: { topic: 't' },
        idempotencyKey: 'guessed-key',
      }),
    );
    expect(guessed.status).toBe(503);

    // #and the nominated start is admitted AND binds the proof run
    const admitted = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-proof',
        inputData: { topic: 't' },
        idempotencyKey: 'proof-key-1',
      }),
    );
    expect(admitted.status).toBe(200);
    await expect(fence.read()).resolves.toMatchObject({
      state: 'proof-only',
      proofExecution: {
        tablePrefix: '',
        workflowId: 'gated',
        runId: 'run-proof',
        startToken: expect.any(String),
      },
      proofKey: 'proof-key-1',
      proofRunId: 'run-proof',
      mutationEpoch: 0,
      requireMutationEpoch: false,
      transitionRevision: 1,
    });

    // #and a SECOND start under the same key is refused: the proof is one run,
    // and recordProofRun's CAS is what says so.
    const second = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-proof-2',
        inputData: { topic: 't' },
        idempotencyKey: 'proof-key-1',
      }),
    );
    expect(second.status).toBe(503);
  });

  it('refuses an admitted start whose fence MOVED before the write-back landed', async () => {
    // #given a fence that reads proof-only and then, between the admitting
    // read and the write-back, has been transitioned away — the 0-row case
    // recordProofRun's CAS exists for
    const storage = testStorage();
    const env = makeProductionEnv(storage);
    const fence = env.fence as ExecutionFenceStore;
    await fence.seed('migration-locked');
    await fence.transition({
      expected: 'migration-locked',
      next: 'proof-only',
      proofKey: 'proof-key-1',
    });
    let moved = false;
    const moving = new Proxy(fence, {
      get(target, property, receiver) {
        if (property === 'read') {
          return async () => {
            const reading = await target.read();
            // The operator moves the fence ONCE, right after the read that
            // admitted the start. Every later read sees the moved fence, which
            // is exactly what an admitted-then-moved start observes.
            if (!moved) {
              moved = true;
              await target.transition({
                expected: 'proof-only',
                next: 'migration-locked',
              });
            }
            return reading;
          };
        }
        const value = Reflect.get(target, property, receiver) as unknown;
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const runner = new TestRunner(undefined, {
      ...env,
      fence: moving as ExecutionFenceStore,
    });

    // #when
    const response = await runner.fetch(
      post('/runs', {
        workflowId: 'gated',
        runId: 'run-proof',
        inputData: { topic: 't' },
        idempotencyKey: 'proof-key-1',
      }),
    );

    // #then refused, and nothing ran: the deployment is no longer the one this
    // start read, so its admission is void.
    expect(response.status).toBe(503);
    await expect(fence.read()).resolves.toEqual({
      state: 'migration-locked',
      mutationEpoch: 0,
      requireMutationEpoch: false,
      transitionRevision: 2,
    });
  });
});

describe('FS8 D3 host activation workflow recovery barriers', () => {
  async function preparedFixture() {
    const fixture = cWorkflowFixture(true);
    const writes: unknown[] = [];
    const put = fixture.journal.storage.put.bind(fixture.journal.storage);
    vi.spyOn(fixture.journal.storage, 'put').mockImplementation(
      async (key, value) => {
        if (key === 'flowsafe:run-owner-recovery:v1')
          writes.push(structuredClone(value));
        await put(key, value);
      },
    );
    const response = await fixture.runner.fetch(post('/runs', C_WORKFLOW_BODY));
    expect(response.status).toBe(200);
    const journal = writes.at(-1) as Record<string, unknown>;
    expect(journal).toMatchObject({
      version: 2,
      phase: 'prepared',
      owner: { kind: 'human', id: 'owner-1' },
    });
    fixture.journal.values.set(
      'flowsafe:run-owner-recovery:v1',
      structuredClone(journal),
    );
    return { ...fixture, journalValue: journal };
  }

  it.each([
    'phase',
    'generation',
    'owner',
  ] as const)('preserves a replacement journal with repeated H after recovery read: %s', async (field) => {
    const fixture = await preparedFixture();
    const replacement = structuredClone(fixture.journalValue);
    if (field === 'phase') replacement.phase = 'prepared-unfenced';
    if (field === 'generation')
      (replacement.execution as { startToken: string }).startToken =
        'replacement-generation';
    if (field === 'owner')
      replacement.owner = { kind: 'human', id: 'replacement-owner' };
    const nativeRecover = fixture.runtime.recoverStartAttempt.bind(
      fixture.runtime,
    );
    vi.spyOn(fixture.runtime, 'recoverStartAttempt').mockImplementation(
      async (...args) => {
        const result = await nativeRecover(...args);
        fixture.journal.values.set(
          'flowsafe:run-owner-recovery:v1',
          replacement,
        );
        return result;
      },
    );
    const outcome = await fixture.runner
      .alarm()
      .catch((error: unknown) => error);
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toEqual(replacement);
    expect(await fixture.env.owners.owner('run', 'c-run')).toEqual({
      kind: 'human',
      id: 'owner-1',
    });
    expect(fixture.journal.alarms.at(-1)).toBeGreaterThan(Date.now());
    expect(outcome).toBeInstanceOf(Error);
  });

  it.each([
    'strict settlement',
    'approval hook',
    'dispatch discard',
    'ownership settlement',
    'ownership release',
    'completion',
    'journal delete',
  ] as const)('retains a terminal journal until the ordered cleanup confirms: %s', async (boundary) => {
    const fixture = await preparedFixture();
    const terminal = await fixture.runtime.terminateAsPrincipal(
      'gated',
      'c-run',
      OWNER_PRINCIPAL,
      OWNER_PRINCIPAL,
    );
    expect(terminal.summary.status).toBe('cancelled');
    const failure = new Error('held cleanup boundary');
    const hooks = {
      abandonApprovals: vi.fn(async () => {}),
      discardScheduleDispatch: vi.fn(async () => {}),
    };
    fixture.env.lifecycle = hooks;
    if (boundary === 'dispatch discard') {
      const workflows = await fixture.storage.getStore('workflows');
      const snapshot = await workflows?.loadWorkflowSnapshot({
        workflowName: 'gated',
        runId: 'c-run',
      });
      if (!snapshot?.requestContext)
        throw new Error('missing terminal snapshot');
      const lifecycle = snapshot.requestContext[
        'flowsafe.runLifecycle'
      ] as Record<string, unknown>;
      lifecycle.scheduleDispatch = {
        scheduleId: 'barrier-schedule',
        dispatchId: 'barrier-dispatch',
      };
      await workflows?.persistWorkflowSnapshot({
        workflowName: 'gated',
        runId: 'c-run',
        snapshot,
      });
    }
    const completion = vi.spyOn(fixture.runtime, 'completeTerminalCleanup');
    const releaseOwner = vi.spyOn(fixture.env.owners, 'release');
    let restore = () => {};
    if (boundary === 'strict settlement') {
      const spy = vi
        .spyOn(fixture.runtime, 'settleStartExecution')
        .mockRejectedValue(failure);
      restore = () => spy.mockRestore();
    }
    if (boundary === 'approval hook') {
      hooks.abandonApprovals.mockRejectedValue(failure);
      restore = () => {
        hooks.abandonApprovals.mockResolvedValue();
      };
    }
    if (boundary === 'dispatch discard') {
      hooks.discardScheduleDispatch.mockRejectedValue(failure);
      restore = () => {
        hooks.discardScheduleDispatch.mockResolvedValue();
      };
    }
    if (boundary === 'ownership settlement') {
      const spy = vi
        .spyOn(fixture.env.owners, 'settleReservation')
        .mockRejectedValue(failure);
      restore = () => spy.mockRestore();
    }
    if (boundary === 'ownership release') {
      releaseOwner.mockRejectedValue(failure);
      restore = () => releaseOwner.mockRestore();
    }
    if (boundary === 'completion') {
      const spy = vi
        .spyOn(fixture.runtime, 'completeTerminalCleanup')
        .mockRejectedValue(failure);
      restore = () => spy.mockRestore();
    }
    if (boundary === 'journal delete') {
      const native = fixture.journal.storage.delete.bind(
        fixture.journal.storage,
      );
      const spy = vi
        .spyOn(fixture.journal.storage, 'delete')
        .mockImplementation(async (key) => {
          if (key === 'flowsafe:run-owner-recovery:v1') throw failure;
          return native(key);
        });
      restore = () => spy.mockRestore();
    }
    const outcome = await fixture.runner
      .alarm()
      .catch((error: unknown) => error);
    if (
      boundary === 'dispatch discard' ||
      boundary === 'ownership settlement' ||
      boundary === 'ownership release'
    ) {
      expect(await fixture.env.owners.owner('run', 'c-run')).toEqual({
        kind: 'human',
        id: 'owner-1',
      });
      expect(completion).not.toHaveBeenCalled();
      if (boundary === 'ownership settlement')
        expect(hooks.abandonApprovals).not.toHaveBeenCalled();
      if (boundary === 'dispatch discard')
        expect(releaseOwner).not.toHaveBeenCalled();
    }
    const durable = await fixture.runtime.authoritativeStartState(
      'gated',
      'c-run',
    );
    expect(durable).toMatchObject({
      kind: 'result',
      summary: { status: 'cancelled' },
    });
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toEqual(fixture.journalValue);
    expect(fixture.journal.alarms.at(-1)).toBeGreaterThan(Date.now());
    if (boundary === 'strict settlement')
      expect(hooks.abandonApprovals).not.toHaveBeenCalled();
    if (boundary === 'strict settlement')
      expect(outcome).toMatchObject({ status: 503, cause: failure });
    else expect(outcome).toBe(failure);
    restore();
    await fixture.runner.alarm();
    expect(fixture.journal.values.has('flowsafe:run-owner-recovery:v1')).toBe(
      false,
    );
  });

  it('converges a prepared journal put whose response was lost before admission', async () => {
    const fixture = cWorkflowFixture(true);
    const put = fixture.journal.storage.put.bind(fixture.journal.storage);
    vi.spyOn(fixture.journal.storage, 'put').mockImplementation(
      async (key, value) => {
        await put(key, value);
        if (
          key === 'flowsafe:run-owner-recovery:v1' &&
          (value as { phase?: string }).phase === 'prepared'
        )
          throw new Error('preparation receipt lost');
      },
    );
    const response = await fixture.runner.fetch(post('/runs', C_WORKFLOW_BODY));
    const state = await fixture.runtime.authoritativeStartState(
      'gated',
      'c-run',
    );
    expect(state).toMatchObject({
      kind: 'result',
      summary: { status: 'suspended' },
    });
    expect(fixture.journal.values.has('flowsafe:run-owner-recovery:v1')).toBe(
      false,
    );
    expect(response.status).toBe(200);
  });

  it('resets only unreadability stamps when a due deadline observes known pending', async () => {
    const fixture = await preparedFixture();
    const snapshot = await fixture.storage.getStore('workflows');
    const selected = await fixture.runtime.authoritativeStartState(
      'gated',
      'c-run',
    );
    if (!selected) throw new Error('missing selected state');
    const pending = {
      ...selected.snapshot,
      status: 'pending' as const,
      requestContext: { ...selected.snapshot.requestContext },
    };
    await snapshot?.persistWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'c-run',
      snapshot: pending,
    });
    const record: SuspensionDeadlineRecord = {
      version: 1,
      workflowId: 'gated',
      runId: 'c-run',
      entries: [
        {
          step: 'gate',
          suspendedAt: 1,
          resumeCount: 0,
          deadlineAt: 1,
          attempts: 2,
          nextAttemptAt: 2,
          unreadableSince: 1,
        },
        {
          step: 'future',
          suspendedAt: 3,
          resumeCount: 0,
          deadlineAt: Date.now() + 1_000_000,
          attempts: 1,
          nextAttemptAt: Date.now() + 1_000_000,
          unreadableSince: 2,
        },
      ],
    };
    fixture.journal.values.delete('flowsafe:run-owner-recovery:v1');
    fixture.journal.values.set(SUSPENSION_DEADLINE_STORAGE_KEY, record);
    const resume = vi.spyOn(fixture.runtime, 'resume');
    await fixture.runner.alarm();
    expect(fixture.journal.values.get(SUSPENSION_DEADLINE_STORAGE_KEY)).toEqual(
      {
        ...record,
        entries: record.entries.map(
          ({ unreadableSince: _stamp, ...entry }) => entry,
        ),
      },
    );
    expect(
      (await fixture.runtime.authoritativeStartState('gated', 'c-run'))?.kind,
    ).toBe('initial');
    expect(resume).not.toHaveBeenCalled();
  });
});

describe('FS8 D3 protected replay selected workflow value', () => {
  it('pairs the original selected generation with its value after durable replacement', async () => {
    const fixture = cWorkflowFixture(true);
    expect(
      (await fixture.runner.fetch(post('/runs', C_WORKFLOW_BODY))).status,
    ).toBe(200);
    const workflows = await fixture.storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'c-run',
    });
    if (!workflows || !snapshot) throw new Error('missing real snapshot');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'c-run',
      // The upstream snapshot type narrows results to records; persisted JSON can carry scalars.
      snapshot: {
        ...snapshot,
        status: 'success',
        result: 'first-value',
      } as unknown as WorkflowRunState,
    });
    const originalRead = fixture.runtime.authoritativeStartState.bind(
      fixture.runtime,
    );
    const first = await originalRead('gated', 'c-run');
    if (!first) throw new Error('missing original generation');
    const read = vi
      .spyOn(fixture.runtime, 'authoritativeStartState')
      .mockImplementation(async (...args) => {
        const selected = await originalRead(...args);
        await workflows.persistWorkflowSnapshot({
          workflowName: 'gated',
          runId: 'c-run',
          snapshot: {
            ...snapshot,
            status: 'success',
            result: 'replacement-value',
            requestContext: {
              ...snapshot.requestContext,
              'flowsafe.runProvenance': {
                ...(snapshot.requestContext?.[
                  'flowsafe.runProvenance'
                ] as object),
                startToken: 'replacement-generation',
              },
            },
          } as unknown as WorkflowRunState,
        });
        return selected;
      });
    const response = await fixture.runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/c-run?replay=1'),
    );
    expect(
      (
        await workflows.loadWorkflowSnapshot({
          workflowName: 'gated',
          runId: 'c-run',
        })
      )?.result,
    ).toBe('replacement-value');
    expect(await response.json()).toMatchObject({
      kind: 'result',
      execution: { startToken: first.execution.startToken },
      value: { result: 'first-value' },
    });
    expect(read).toHaveBeenCalledOnce();
  });
});

describe('FS8 D3 host activation original claim preflight', () => {
  it.each([
    false,
    true,
  ])('releases only its exact captured claim on a local fence refusal (replacement=%s)', async (replacement) => {
    const fixture = cWorkflowFixture(true);
    const store = fixture.runtime.startIdempotency;
    if (!store || !fixture.env.fence)
      throw new Error('missing test authority wiring');
    await fixture.env.fence.seed('open');
    const reserved = await store.reserve({
      key: 'host-key',
      owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
      targetKind: 'workflow',
      targetId: 'gated',
      mintRunId: () => 'c-run',
    });
    const claim = await store.claimReservation(reserved.reservation);
    if (!claim) throw new Error('test did not win its reservation');
    await fixture.env.fence.transition({ expected: 'open', next: 'draining' });
    if (replacement) {
      const native = fixture.env.fence.read.bind(fixture.env.fence);
      vi.spyOn(fixture.env.fence, 'read').mockImplementationOnce(async () => {
        const reading = await native();
        await store.releaseReservation(claim);
        const released = await store.readForAdmission('host-key');
        if (!released) throw new Error('missing released reservation');
        expect(await store.claimReservation(released)).toBeDefined();
        return reading;
      });
    }
    const response = await fixture.runner.fetch(
      post('/runs', {
        ...C_WORKFLOW_BODY,
        idempotencyKey: 'host-key',
        startReservation: claim,
      }),
    );
    const current = await store.readForAdmission('host-key');
    expect(current?.state).toBe(replacement ? 'started' : 'reserved');
    expect(current?.binding).toEqual({ kind: 'unbound' });
    expect(current?.updatedAt).toBeGreaterThan(claim.updatedAt);
    expect(fixture.journal.values.has('flowsafe:run-owner-recovery:v1')).toBe(
      false,
    );
    expect(fixture.reserve).not.toHaveBeenCalled();
    expect(fixture.start).not.toHaveBeenCalled();
    expect(response.status).toBe(503);
  });
});

describe('FS8 D3 host activation managed workflow expectation', () => {
  it('rejects an agent role at the same physical workflow generation before B2 or cleanup', async () => {
    const fixture = cWorkflowFixture(true);
    const put = fixture.journal.storage.put.bind(fixture.journal.storage);
    let journal: unknown;
    vi.spyOn(fixture.journal.storage, 'put').mockImplementation(
      async (key, value) => {
        if (
          key === 'flowsafe:run-owner-recovery:v1' &&
          (value as { phase?: string }).phase === 'prepared'
        )
          journal = structuredClone(value);
        await put(key, value);
      },
    );
    expect(
      (await fixture.runner.fetch(post('/runs', C_WORKFLOW_BODY))).status,
    ).toBe(200);
    const domain = (await fixture.storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const original = await domain.loadWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'c-run',
    });
    if (!original?.requestContext || !journal)
      throw new Error('missing actual prepared workflow');
    const prior = original.requestContext['flowsafe.runProvenance'] as Record<
      string,
      unknown
    >;
    const snapshot = {
      runId: 'c-run',
      status: 'pending' as const,
      value: {},
      context: {},
      serializedStepGraph: [],
      activePaths: [],
      activeStepsPath: {},
      suspendedPaths: {},
      resumeLabels: {},
      waitingPaths: {},
      timestamp: 123,
      requestContext: {
        'flowsafe.runProvenance': {
          ...prior,
          initialAdmission: true,
          resumeCounts: [],
          startIdentity: {
            owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
            target: {
              kind: 'agent',
              id: 'foreign-agent',
              threadId: 'foreign-thread',
            },
          },
          agentStart: { threaded: true },
        },
      },
    };
    await domain.persistWorkflowSnapshot({
      workflowName: 'gated',
      runId: 'c-run',
      snapshot,
    });
    fixture.journal.values.set('flowsafe:run-owner-recovery:v1', journal);
    const native = domain[FENCED_WORKFLOW_STORAGE];
    if (!native) throw new Error('missing actual capability');
    const rawReads = vi.fn(native.readSnapshot.bind(native));
    const terminalize = vi.fn(native.terminalizeInitialAdmission.bind(native));
    Object.defineProperty(domain, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...native,
        readSnapshot: rawReads,
        terminalizeInitialAdmission: terminalize,
      },
      configurable: true,
    });
    const before = await native.readSnapshot({
      workflowId: 'gated',
      runId: 'c-run',
    });
    const settle = vi.spyOn(fixture.runtime, 'settleStartExecution');
    const response = await fixture.runner.fetch(
      deploymentIdentityRequest('http://do/runs/gated/c-run/dispatch-status'),
    );
    expect(
      await native.readSnapshot({ workflowId: 'gated', runId: 'c-run' }),
    ).toEqual(before);
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toEqual(journal);
    expect(terminalize).not.toHaveBeenCalled();
    expect(settle).not.toHaveBeenCalled();
    expect(rawReads).toHaveBeenCalledOnce();
    expect(response.status).toBe(503);
  });
});

describe('FS8 D3 host activation prepared absence and local zero', () => {
  it.each([
    true,
    false,
  ])('retains a cold prepared workflow journal across same-ID retries after absence (keyed=%s)', async (keyed) => {
    const fixture = cWorkflowFixture(true);
    const reservations = fixture.runtime.startIdempotency;
    if (!reservations) throw new Error('missing reservations');
    let claim:
      | import('./start-reservation-contract.js').StartReservationReading
      | undefined;
    if (keyed) {
      const reserved = await reservations.reserve({
        key: 'absent-key',
        owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
        targetKind: 'workflow',
        targetId: 'gated',
        mintRunId: () => 'c-run',
      });
      claim = await reservations.claimReservation(reserved.reservation);
      if (!claim) throw new Error('missing winning claim');
    }
    let journal: unknown;
    const put = fixture.journal.storage.put.bind(fixture.journal.storage);
    vi.spyOn(fixture.journal.storage, 'put').mockImplementation(
      async (key, value) => {
        if (
          key === 'flowsafe:run-owner-recovery:v1' &&
          (value as { phase?: string }).phase === 'prepared'
        )
          journal = structuredClone(value);
        await put(key, value);
      },
    );
    expect(
      (
        await fixture.runner.fetch(
          post('/runs', {
            ...C_WORKFLOW_BODY,
            ...(claim
              ? { idempotencyKey: claim.key, startReservation: claim }
              : {}),
          }),
        )
      ).status,
    ).toBe(200);
    const claimed = keyed
      ? await reservations.readForAdmission('absent-key')
      : undefined;
    fixture.journal.values.set('flowsafe:run-owner-recovery:v1', journal);
    await testDatabase(fixture.storage)
      .prepare(
        'DELETE FROM mastra_workflow_snapshot WHERE workflow_name = ? AND run_id = ?',
      )
      .bind('gated', 'c-run')
      .run();
    fixture.env.runtime = gatedRuntime(fixture.storage, fixture.env.fence);
    const enter = vi
      .spyOn(fixture.env.runtime, 'start')
      .mockRejectedValue(new Error('unexpected new engine entry'));
    const evicted = new TestRunner(fixture.journal.state, fixture.env);
    const error = await evicted.alarm().catch((cause: unknown) => cause);
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toEqual(journal);
    expect(
      await fixture.env.runtime.authoritativeStartState('gated', 'c-run'),
    ).toBeNull();
    expect(
      keyed ? await reservations.readForAdmission('absent-key') : undefined,
    ).toEqual(claimed);
    expect(error).toMatchObject({ status: 503 });
    for (let retry = 0; retry < 2; retry++) {
      const response = await evicted.fetch(
        post('/runs', {
          ...C_WORKFLOW_BODY,
          ...(claim
            ? { idempotencyKey: claim.key, startReservation: claim }
            : {}),
        }),
      );
      expect(
        fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
      ).toEqual(journal);
      expect(enter).not.toHaveBeenCalled();
      expect(response.status).toBe(503);
    }
  });

  async function zeroFixture() {
    const env = makeProductionEnv();
    const reservations = newTestStartIdempotency(env.storage);
    const app = init(
      { storage: env.storage },
      { executionFence: env.fence ?? 'none', startIdempotency: reservations },
    );
    let effects = 0;
    app
      .createWorkflow({
        id: 'zero',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
      .then(
        app.createStep({
          id: 'effect',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          execute: async () => {
            effects++;
            return {};
          },
        }),
      )
      .commit();
    env.runtime = app.runtime;
    if (!env.fence) throw new Error('missing native fence');
    await env.fence.seed('open');
    const domain = (await env.storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const native = domain[FENCED_WORKFLOW_STORAGE];
    if (!native) throw new Error('missing native capability');
    let closeOnce = true;
    Object.defineProperty(domain, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...native,
        withInitialAdmission: async (
          input: Parameters<typeof native.withInitialAdmission>[0],
          create: Parameters<typeof native.withInitialAdmission>[1],
        ) => {
          if (closeOnce) {
            closeOnce = false;
            await env.fence?.transition({ expected: 'open', next: 'draining' });
          }
          return native.withInitialAdmission(input, create);
        },
      },
      configurable: true,
    });
    const journal = durableKeyValueStorageFixture();
    const runner = new TestRunner(journal.state, env);
    const makeClaim = async () => {
      const reserved = await reservations.reserve({
        key: 'zero-key',
        owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
        targetKind: 'workflow',
        targetId: 'zero',
        mintRunId: () => 'zero-run',
      });
      const claim = await reservations.claimReservation(reserved.reservation);
      if (!claim) throw new Error('missing winning claim');
      return claim;
    };
    return {
      env,
      reservations,
      app,
      domain,
      native,
      journal,
      runner,
      makeClaim,
      effects: () => effects,
      allowNext: () => {
        closeOnce = false;
      },
      refuseNext: () => {
        closeOnce = true;
      },
    };
  }

  it.each([
    true,
    false,
  ])('clears only a local native validated-zero admission and retries once (keyed=%s)', async (keyed) => {
    const { env, reservations, native, journal, runner, makeClaim, effects } =
      await zeroFixture();
    if (!env.fence) throw new Error('missing native fence');
    const firstClaim = keyed ? await makeClaim() : undefined;
    const first = await runner.fetch(
      post('/runs', {
        workflowId: 'zero',
        runId: 'zero-run',
        inputData: {},
        ...(firstClaim
          ? { idempotencyKey: firstClaim.key, startReservation: firstClaim }
          : {}),
      }),
    );
    expect(
      await native.readSnapshot({ workflowId: 'zero', runId: 'zero-run' }),
    ).toBeUndefined();
    expect(journal.values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    expect(await env.owners.owner('run', 'zero-run')).toBeUndefined();
    expect(effects()).toBe(0);
    if (keyed)
      expect(await reservations.readForAdmission('zero-key')).toMatchObject({
        state: 'reserved',
        binding: { kind: 'unbound' },
      });
    expect(first.status).toBe(503);
    await env.fence.transition({ expected: 'draining', next: 'open' });
    const nextClaim = keyed ? await makeClaim() : undefined;
    const retry = await runner.fetch(
      post('/runs', {
        workflowId: 'zero',
        runId: 'zero-run',
        inputData: {},
        ...(nextClaim
          ? { idempotencyKey: nextClaim.key, startReservation: nextClaim }
          : {}),
      }),
    );
    expect(effects()).toBe(1);
    expect(journal.values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
    expect(retry.status).toBe(200);
  });
  it.each([
    'lookalike',
    'serialized',
    'other generation',
    'foreign row',
    'evicted frame',
  ] as const)('retains prepared absence without its exact local zero authority: %s', async (mode) => {
    const fixture = await zeroFixture();
    const nativeStart = fixture.app.runtime.start.bind(fixture.app.runtime);
    let originalFailure: unknown;
    let replacementRaw: unknown;
    vi.spyOn(fixture.app.runtime, 'start').mockImplementationOnce(
      async (...args) => {
        try {
          return await nativeStart(...args);
        } catch (error) {
          originalFailure = error;
          if (mode === 'other generation') {
            await fixture.env.fence?.transition({
              expected: 'draining',
              next: 'open',
            });
            fixture.refuseNext();
            await nativeStart('zero', {
              runId: 'other-zero-run',
              inputData: {},
              requestedBy: OWNER_PRINCIPAL.id,
              requestedByKind: 'human',
            });
            throw new Error('second native zero did not refuse');
          }
          if (mode === 'foreign row') {
            await fixture.domain.persistWorkflowSnapshot({
              workflowName: 'zero',
              runId: 'zero-run',
              snapshot: {
                runId: 'zero-run',
                status: 'success',
                result: {},
                value: {},
                context: {},
                serializedStepGraph: [],
                activePaths: [],
                activeStepsPath: {},
                suspendedPaths: {},
                resumeLabels: {},
                waitingPaths: {},
                timestamp: 1,
                requestContext: {
                  'flowsafe.runProvenance': {
                    version: 2,
                    startToken: 'replacement-generation',
                    attemptToken: 'replacement-attempt',
                    resumeCounts: [],
                    requestedBy: OWNER_PRINCIPAL.id,
                    requestedByKind: 'human',
                    startIdentity: {
                      owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
                      target: { kind: 'workflow', id: 'zero' },
                    },
                  },
                },
              },
            });
            replacementRaw = await fixture.native.readSnapshot({
              workflowId: 'zero',
              runId: 'zero-run',
            });
            throw error;
          }
          if (mode === 'lookalike') {
            const { ExecutionFencedError } = await import(
              './execution-fence.js'
            );
            throw new ExecutionFencedError('draining', 'run start');
          }
          throw Object.assign(
            new Error('serialized refusal'),
            JSON.parse(
              JSON.stringify({
                status: (error as { status?: number }).status,
                reason: (error as { reason?: unknown }).reason,
              }),
            ),
          );
        }
      },
    );
    const first = await fixture.runner.fetch(
      post('/runs', { workflowId: 'zero', runId: 'zero-run', inputData: {} }),
    );
    const journal = structuredClone(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    );
    expect(journal).toMatchObject({
      phase: 'prepared',
      execution: { workflowId: 'zero', runId: 'zero-run' },
    });
    expect(fixture.effects()).toBe(0);
    if (mode === 'foreign row')
      expect(
        await fixture.native.readSnapshot({
          workflowId: 'zero',
          runId: 'zero-run',
        }),
      ).toEqual(replacementRaw);
    else
      expect(
        await fixture.native.readSnapshot({
          workflowId: 'zero',
          runId: 'zero-run',
        }),
      ).toBeUndefined();
    expect(first.status).toBe(
      mode === 'serialized' || mode === 'evicted frame' ? 500 : 503,
    );
    if (mode === 'evicted frame') {
      const { isDefinitiveInitialAdmissionRefusal } = await import(
        './initial-admission-refusal.js'
      );
      expect(
        isDefinitiveInitialAdmissionRefusal(
          originalFailure,
          (
            journal as {
              execution: import('./execution-admission.js').D1RunExecutionIdentity;
            }
          ).execution,
        ),
      ).toBe(true);
      const error = await fixture.runner
        .alarm()
        .catch((cause: unknown) => cause);
      expect(
        fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
      ).toEqual(journal);
      expect(error).toMatchObject({ status: 503 });
    }
    await fixture.env.fence?.transition({ expected: 'draining', next: 'open' });
    const retry = await fixture.runner.fetch(
      post('/runs', { workflowId: 'zero', runId: 'zero-run', inputData: {} }),
    );
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toEqual(journal);
    expect(fixture.effects()).toBe(0);
    expect(retry.status).toBe(503);
  });
  it('retains a paid unknown outcome when a fence lookalike follows real engine entry', async () => {
    const fixture = await zeroFixture();
    fixture.allowNext();
    const nativeStart = fixture.app.runtime.start.bind(fixture.app.runtime);
    vi.spyOn(fixture.app.runtime, 'start').mockImplementationOnce(
      async (...args) => {
        await nativeStart(...args);
        await testDatabase(fixture.env.storage)
          .prepare(
            'DELETE FROM mastra_workflow_snapshot WHERE workflow_name = ? AND run_id = ?',
          )
          .bind('zero', 'zero-run')
          .run();
        const { ExecutionFencedError } = await import('./execution-fence.js');
        throw new ExecutionFencedError('draining', 'run start');
      },
    );
    const response = await fixture.runner.fetch(
      post('/runs', { workflowId: 'zero', runId: 'zero-run', inputData: {} }),
    );
    expect(fixture.effects()).toBe(1);
    expect(
      await fixture.native.readSnapshot({
        workflowId: 'zero',
        runId: 'zero-run',
      }),
    ).toBeUndefined();
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toMatchObject({
      phase: 'prepared',
      execution: { workflowId: 'zero', runId: 'zero-run' },
    });
    expect(response.status).toBe(503);
  });
});

describe('FS8 D3 host R1 ordinary legacy workflow lifecycle', () => {
  it.each([
    ['v1', 'terminate'],
    ['absent', 'terminate'],
    ['v1', 'deadline'],
    ['absent', 'deadline'],
  ] as const)('converges actual Core cleanup and replay with retained key (%s %s)', async (version, operation) => {
    const fixture = await hostR1WorkflowFixture('custom-null');
    const runId = 'legacy-run';
    const reserved = await fixture.reservations.reserve({
      key: 'retained-legacy-key',
      owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
      targetKind: 'workflow',
      targetId: 'host-r1',
      mintRunId: () => runId,
    });
    const claim = await fixture.reservations.claimReservation(
      reserved.reservation,
    );
    if (!claim) throw new Error('missing retained claim');
    expect(
      (
        await fixture.runner.fetch(
          post('/runs', {
            workflowId: 'host-r1',
            runId,
            inputData: {},
            deadlineMs: 0,
            idempotencyKey: claim.key,
            startReservation: claim,
          }),
        )
      ).status,
    ).toBe(200);
    const snapshot = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: 'host-r1',
      runId,
    });
    if (!snapshot?.requestContext)
      throw new Error('missing actual Core snapshot');
    if (version === 'v1')
      snapshot.requestContext['flowsafe.runProvenance'] = {
        version: 1,
        attemptToken: 'host-r1-legacy-leg',
        requestedBy: OWNER_PRINCIPAL.id,
        requestedByKind: 'human',
        resumeCounts: [],
      };
    else delete snapshot.requestContext['flowsafe.runProvenance'];
    await fixture.workflows.persistWorkflowSnapshot({
      workflowName: 'host-r1',
      runId,
      snapshot,
    });
    const bound = await fixture.reservations.readForAdmission(claim.key);
    const lifecycle = snapshot.requestContext['flowsafe.runLifecycle'] as {
      revision: number;
      deadlineAt: number;
    };
    const events: string[] = [];
    const approvals = vi.fn(async () => {
      events.push('approvals');
    });
    fixture.env.lifecycle = { abandonApprovals: approvals };
    const release = fixture.owners.release.bind(fixture.owners);
    vi.spyOn(fixture.owners, 'release').mockImplementation(async (...args) => {
      events.push('owner');
      return release(...args);
    });
    const complete = fixture.app.runtime.completeTerminalCleanup.bind(
      fixture.app.runtime,
    );
    vi.spyOn(fixture.app.runtime, 'completeTerminalCleanup').mockImplementation(
      async (...args) => {
        events.push('completion');
        return complete(...args);
      },
    );
    const settle = vi.spyOn(fixture.app.runtime, 'settleStartExecution');
    const b2 = vi.spyOn(fixture.app.runtime, 'recoverStartAttempt');
    const body =
      operation === 'deadline'
        ? {
            expectedRevision: lifecycle.revision,
            expectedDeadlineAt: lifecycle.deadlineAt,
          }
        : {};
    const principal: ExecutionPrincipal =
      operation === 'deadline'
        ? {
            kind: 'system',
            id: 'maintenance',
            purpose: 'run-deadline-maintenance',
          }
        : OWNER_PRINCIPAL;
    const first = await fixture.runner.fetch(
      post(`/runs/host-r1/${runId}/${operation}`, body, principal),
    );
    expect(await fixture.owners.owner('run', runId)).toBeUndefined();
    expect(await fixture.reservations.readForAdmission(claim.key)).toEqual(
      bound,
    );
    expect(events).toEqual(['approvals', 'owner', 'completion']);
    expect(settle).not.toHaveBeenCalled();
    expect(b2).not.toHaveBeenCalled();
    expect(first.status).toBe(200);
    const terminal = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: 'host-r1',
      runId,
    });
    expect(terminal).toMatchObject({
      status: operation === 'deadline' ? 'timed_out' : 'cancelled',
      requestContext: {
        'flowsafe.runLifecycle': {
          terminal: { cleanupCompletedAt: expect.any(Number) },
        },
      },
    });
    const bytes = JSON.stringify(terminal);
    const replay = await fixture.runner.fetch(
      post(
        `/runs/host-r1/${runId}/${operation === 'deadline' ? 'deadline' : 'terminate-replay'}`,
        body,
        principal,
      ),
    );
    expect(
      JSON.stringify(
        await fixture.workflows.loadWorkflowSnapshot({
          workflowName: 'host-r1',
          runId,
        }),
      ),
    ).toBe(bytes);
    expect(await fixture.reservations.readForAdmission(claim.key)).toEqual(
      bound,
    );
    expect(events).toEqual(['approvals', 'owner', 'completion']);
    expect(fixture.effects()).toBe(1);
    expect(replay.status).toBe(200);
  });

  it.each([
    'v1',
    'absent',
  ] as const)('retains actual legacy ownership after hook failure and completes one retry (%s)', async (version) => {
    const fixture = await hostR1WorkflowFixture('custom-null');
    expect(
      (
        await fixture.runner.fetch(
          post('/runs', {
            workflowId: 'host-r1',
            runId: 'legacy-retry',
            inputData: {},
          }),
        )
      ).status,
    ).toBe(200);
    const snapshot = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: 'host-r1',
      runId: 'legacy-retry',
    });
    if (!snapshot?.requestContext) throw new Error('missing Core snapshot');
    if (version === 'v1')
      snapshot.requestContext['flowsafe.runProvenance'] = {
        version: 1,
        attemptToken: 'host-r1-legacy-leg',
        requestedBy: OWNER_PRINCIPAL.id,
        requestedByKind: 'human',
        resumeCounts: [],
      };
    else delete snapshot.requestContext['flowsafe.runProvenance'];
    await fixture.workflows.persistWorkflowSnapshot({
      workflowName: 'host-r1',
      runId: 'legacy-retry',
      snapshot,
    });
    const approvals = vi
      .fn(async () => {})
      .mockRejectedValueOnce(new Error('approval receipt lost'));
    fixture.env.lifecycle = { abandonApprovals: approvals };
    const complete = vi.spyOn(fixture.app.runtime, 'completeTerminalCleanup');
    const first = await fixture.runner.fetch(
      post('/runs/host-r1/legacy-retry/terminate', {}),
    );
    expect(await fixture.owners.owner('run', 'legacy-retry')).toEqual({
      kind: 'human',
      id: OWNER_PRINCIPAL.id,
    });
    expect(complete).not.toHaveBeenCalled();
    expect(
      (
        await fixture.workflows.loadWorkflowSnapshot({
          workflowName: 'host-r1',
          runId: 'legacy-retry',
        })
      )?.status,
    ).toBe('cancelled');
    expect(first.status).toBe(500);
    const retry = await fixture.runner.fetch(
      post('/runs/host-r1/legacy-retry/terminate-replay', {}),
    );
    expect(await fixture.owners.owner('run', 'legacy-retry')).toBeUndefined();
    expect(approvals).toHaveBeenCalledTimes(2);
    expect(complete).toHaveBeenCalledOnce();
    expect(fixture.effects()).toBe(1);
    expect(retry.status).toBe(200);
  });
});

describe('FS8 D3 host R1 workflow unfenced recovery', () => {
  it.each([
    ['custom-null', 'missing'],
    ['custom-null', 'pending'],
    ['custom-null', 'suspended'],
    ['actual-prefix', 'missing'],
    ['actual-prefix', 'pending'],
    ['actual-prefix', 'suspended'],
  ] as const)('retains keyed journal and ownership before missing-store refusal (%s %s)', async (mode, status) => {
    const fixture = await hostR1WorkflowFixture(mode, false);
    const runId = 'unfenced-run';
    const execution = {
      tablePrefix: mode === 'custom-null' ? null : 'host_r1_',
      workflowId: 'host-r1',
      runId,
      startToken: 'unfenced-generation',
    };
    const reserved = await fixture.reservations.reserve({
      key: 'unfenced-key',
      owner: { kind: 'human' as const, id: OWNER_PRINCIPAL.id },
      targetKind: 'workflow',
      targetId: 'host-r1',
      mintRunId: () => runId,
    });
    const claim = await fixture.reservations.claimReservation(
      reserved.reservation,
    );
    if (!claim) throw new Error('missing claim');
    await fixture.reservations.bindPreparedStart(claim, {
      ...execution,
      owner: { kind: 'human' as const, id: OWNER_PRINCIPAL.id },
      target: { kind: 'workflow', id: 'host-r1' },
    });
    const journal = {
      version: 2,
      phase: 'prepared-unfenced',
      workflowId: 'host-r1',
      runId,
      token: 'unfenced-attempt',
      owner: { kind: 'human' as const, id: OWNER_PRINCIPAL.id },
      execution,
      startReservation: claim,
    };
    fixture.journal.values.set('flowsafe:run-owner-recovery:v1', journal);
    await fixture.owners.reserveAll(
      [{ kind: 'run', resourceId: runId }],
      journal.owner,
      journal.token,
    );
    if (status !== 'missing')
      await fixture.workflows.persistWorkflowSnapshot({
        workflowName: 'host-r1',
        runId,
        snapshot: {
          runId,
          status,
          value: {},
          context: {},
          serializedStepGraph: [],
          activePaths: [],
          activeStepsPath: {},
          suspendedPaths: {},
          resumeLabels: {},
          waitingPaths: {},
          timestamp: 1,
          requestContext: {
            'flowsafe.runProvenance': {
              version: 2,
              startToken: execution.startToken,
              attemptToken: journal.token,
              resumeCounts: [],
              startIdentity: {
                owner: journal.owner,
                target: { kind: 'workflow', id: 'host-r1' },
              },
            },
          },
        },
      });
    const before = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: 'host-r1',
      runId,
    });
    const bound = await fixture.reservations.readForAdmission('unfenced-key');
    const settle = vi.spyOn(fixture.owners, 'settleReservation');
    const b2 = vi.spyOn(fixture.app.runtime, 'recoverStartAttempt');
    const outcome = await fixture.runner
      .alarm()
      .catch((error: unknown) => error);
    expect(settle).not.toHaveBeenCalled();
    expect(
      await fixture.owners.reserveAll(
        [{ kind: 'run', resourceId: runId }],
        { kind: 'human', id: 'foreign' },
        'foreign-attempt',
      ),
    ).toBe(false);
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toEqual(journal);
    expect(await fixture.reservations.readForAdmission('unfenced-key')).toEqual(
      bound,
    );
    expect(
      await fixture.workflows.loadWorkflowSnapshot({
        workflowName: 'host-r1',
        runId,
      }),
    ).toEqual(before);
    expect(fixture.journal.alarms.at(-1)).toBeGreaterThan(Date.now());
    expect(b2).not.toHaveBeenCalled();
    expect(outcome).toMatchObject({ status: 503 });
  });

  it.each([
    ['custom-null', 'missing'],
    ['custom-null', 'pending'],
    ['actual-prefix', 'missing'],
    ['actual-prefix', 'pending'],
  ] as const)('retains actual unkeyed unfenced absence or pending without B2 (%s %s)', async (mode, status) => {
    const fixture = await hostR1WorkflowFixture(mode);
    const runId = 'unkeyed-unfenced';
    const execution = {
      tablePrefix: mode === 'custom-null' ? null : 'host_r1_',
      workflowId: 'host-r1',
      runId,
      startToken: 'unkeyed-generation',
    };
    const journal = {
      version: 2,
      phase: 'prepared-unfenced',
      workflowId: 'host-r1',
      runId,
      token: 'unkeyed-attempt',
      owner: { kind: 'human' as const, id: OWNER_PRINCIPAL.id },
      execution,
    };
    fixture.journal.values.set('flowsafe:run-owner-recovery:v1', journal);
    await fixture.owners.reserveAll(
      [{ kind: 'run', resourceId: runId }],
      journal.owner,
      journal.token,
    );
    if (status === 'pending')
      await fixture.workflows.persistWorkflowSnapshot({
        workflowName: 'host-r1',
        runId,
        snapshot: {
          runId,
          status,
          value: {},
          context: {},
          serializedStepGraph: [],
          activePaths: [],
          activeStepsPath: {},
          suspendedPaths: {},
          resumeLabels: {},
          waitingPaths: {},
          timestamp: 1,
          requestContext: {
            'flowsafe.runProvenance': {
              version: 2,
              startToken: execution.startToken,
              attemptToken: journal.token,
              resumeCounts: [],
              startIdentity: {
                owner: journal.owner,
                target: { kind: 'workflow', id: 'host-r1' },
              },
            },
          },
        },
      });
    const b2 = vi.spyOn(fixture.app.runtime, 'recoverStartAttempt');
    const settle = vi.spyOn(fixture.owners, 'settleReservation');
    const outcome = await fixture.runner
      .alarm()
      .catch((error: unknown) => error);
    expect(settle).not.toHaveBeenCalled();
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toEqual(journal);
    expect(fixture.journal.alarms.at(-1)).toBeGreaterThan(Date.now());
    expect(b2).not.toHaveBeenCalled();
    if (status === 'pending') expect(outcome).toMatchObject({ status: 503 });
    else expect(outcome).toBeUndefined();
  });
});

describe('FS8 D3 host R1 workflow recovery object address', () => {
  it.each([
    ['preparing', 'workflow'],
    ['preparing', 'run'],
    ['prepared', 'workflow'],
    ['prepared', 'run'],
    ['prepared-unfenced', 'workflow'],
    ['prepared-unfenced', 'run'],
  ] as const)('preserves valid foreign address before Runtime and ownership (%s %s)', async (phase, mismatch) => {
    const fixture = cWorkflowFixture(true);
    const reservations = fixture.runtime.startIdempotency;
    if (!reservations) throw new Error('missing foreign reservation store');
    const reserved = await reservations.reserve({
      key: 'foreign-address-key',
      owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
      targetKind: 'workflow',
      targetId: 'gated',
      mintRunId: () => 'c-run',
    });
    const claim = await reservations.claimReservation(reserved.reservation);
    if (!claim) throw new Error('missing foreign claim');
    expect(
      (
        await fixture.runner.fetch(
          post('/runs', {
            ...C_WORKFLOW_BODY,
            idempotencyKey: claim.key,
            startReservation: claim,
          }),
        )
      ).status,
    ).toBe(200);
    const bound = await reservations.readForAdmission(claim.key);
    const selected = await fixture.runtime.authoritativeStartState(
      'gated',
      'c-run',
    );
    if (!selected) throw new Error('missing selected foreign row');
    const workflowId = 'gated';
    const runId = 'c-run';
    const token = selected.provenance.attemptToken;
    const snapshot = { ...selected.snapshot, status: 'pending' as const };
    const workflows = await fixture.storage.getStore('workflows');
    await workflows?.persistWorkflowSnapshot({
      workflowName: workflowId,
      runId,
      snapshot,
    });
    const journal = {
      version: 2,
      phase,
      workflowId,
      runId,
      owner: { kind: 'human' as const, id: OWNER_PRINCIPAL.id },
      token,
      startReservation: claim,
      ...(phase === 'preparing' ? {} : { execution: selected.execution }),
    };
    fixture.journal.values.set('flowsafe:run-owner-recovery:v1', journal);
    await testDatabase(fixture.storage)
      .prepare(
        'UPDATE flowsafe_resource_owners SET reservation_token = ? WHERE resource_kind = ? AND resource_id = ?',
      )
      .bind(token, 'run', runId)
      .run();
    const rows = await testDatabase(fixture.storage)
      .prepare('SELECT * FROM flowsafe_resource_owners')
      .all();
    const recover = vi.spyOn(fixture.runtime, 'recoverStartAttempt');
    const observe = vi.spyOn(fixture.runtime, 'authoritativeStartState');
    const settle = vi.spyOn(fixture.env.owners, 'settleReservation');
    const release = vi.spyOn(fixture.env.owners, 'release');
    const hooks = { abandonApprovals: vi.fn(async () => {}) };
    fixture.env.lifecycle = hooks;
    const state = {
      ...fixture.journal.state,
      id: {
        name:
          mismatch === 'workflow' ? 'other-workflow:c-run' : 'gated:other-run',
      },
    } as DurableObjectState;
    const foreign = new TestRunner(state, fixture.env);
    const outcome = await foreign.alarm().catch((error: unknown) => error);
    expect(
      await testDatabase(fixture.storage)
        .prepare('SELECT * FROM flowsafe_resource_owners')
        .all(),
    ).toEqual(rows);
    expect(
      await workflows?.loadWorkflowSnapshot({
        workflowName: workflowId,
        runId,
      }),
    ).toEqual(snapshot);
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toEqual(journal);
    expect(await reservations.readForAdmission(claim.key)).toEqual(bound);
    expect(recover).not.toHaveBeenCalled();
    expect(observe).not.toHaveBeenCalled();
    expect(settle).not.toHaveBeenCalled();
    expect(release).not.toHaveBeenCalled();
    expect(hooks.abandonApprovals).not.toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(Error);
  });

  it.each([
    'own',
    'undefined',
  ] as const)('keeps supported workflow object name recovery (%s)', async (name) => {
    const fixture = cWorkflowFixture(true);
    expect(
      (await fixture.runner.fetch(post('/runs', C_WORKFLOW_BODY))).status,
    ).toBe(200);
    const selected = await fixture.runtime.authoritativeStartState(
      'gated',
      'c-run',
    );
    if (!selected) throw new Error('missing own row');
    const journal = await durableOwnerRecovery(
      fixture.runtime,
      'gated',
      'c-run',
      selected.provenance.attemptToken,
    );
    fixture.journal.values.set('flowsafe:run-owner-recovery:v1', journal);
    const state = {
      ...fixture.journal.state,
      ...(name === 'own' ? { id: { name: 'gated:c-run' } } : {}),
    } as DurableObjectState;
    await new TestRunner(state, fixture.env).alarm();
    expect(fixture.journal.values.has('flowsafe:run-owner-recovery:v1')).toBe(
      false,
    );
    expect(await fixture.env.owners.owner('run', 'c-run')).toEqual({
      kind: 'human',
      id: OWNER_PRINCIPAL.id,
    });
  });
});

describe('FS8 D3 host R1 workflow keyed finalization preflight', () => {
  it('retains the original journal claim during cold normal termination and replay without its store', async () => {
    const fixture = await hostR1WorkflowFixture('custom-null');
    const runId = 'cold-terminate';
    const reserved = await fixture.reservations.reserve({
      key: 'cold-terminate-key',
      owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
      targetKind: 'workflow',
      targetId: 'host-r1',
      mintRunId: () => runId,
    });
    const claim = await fixture.reservations.claimReservation(
      reserved.reservation,
    );
    if (!claim) throw new Error('missing original claim');
    let prepared: unknown;
    const put = fixture.journal.values.set.bind(fixture.journal.values);
    const writes = vi
      .spyOn(fixture.journal.values, 'set')
      .mockImplementation((key, value) => {
        if (
          key === 'flowsafe:run-owner-recovery:v1' &&
          value &&
          typeof value === 'object' &&
          (value as { phase?: unknown }).phase === 'prepared-unfenced'
        )
          prepared = structuredClone(value);
        return put(key, value);
      });
    expect(
      (
        await fixture.runner.fetch(
          post('/runs', {
            workflowId: 'host-r1',
            runId,
            inputData: {},
            idempotencyKey: claim.key,
            startReservation: claim,
          }),
        )
      ).status,
    ).toBe(200);
    writes.mockRestore();
    expect(prepared).toMatchObject({
      phase: 'prepared-unfenced',
      startReservation: claim,
    });
    fixture.journal.values.set('flowsafe:run-owner-recovery:v1', prepared);
    const cold = init(
      { storage: fixture.storage },
      {
        executionFence: 'none',
        startIdempotency: 'none',
      },
    );
    const effects = vi.fn(async () => ({}));
    cold
      .createWorkflow({
        id: 'host-r1',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
      .then(
        cold.createStep({
          id: 'gate',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          execute: effects,
        }),
      )
      .commit();
    const hostRuntime = new Proxy({} as RunnerRuntime, {
      get: (_target, property) => {
        const value: unknown = Reflect.get(
          cold.runtime,
          property,
          cold.runtime,
        );
        return typeof value === 'function' ? value.bind(cold.runtime) : value;
      },
    });
    const approvals = vi.fn(async () => {});
    const runner = new TestRunner(fixture.journal.state, {
      ...fixture.env,
      runtime: hostRuntime,
      lifecycle: { abandonApprovals: approvals },
    });
    const before = await fixture.reservations.readForAdmission(claim.key);
    const settle = vi.spyOn(fixture.owners, 'settleReservation');
    const release = vi.spyOn(fixture.owners, 'release');
    const completion = vi.spyOn(cold.runtime, 'completeTerminalCleanup');
    for (const action of ['terminate', 'terminate-replay']) {
      const response = await runner.fetch(
        post(`/runs/host-r1/${runId}/${action}`, {}),
      );
      expect(await fixture.owners.owner('run', runId)).toEqual({
        kind: 'human',
        id: OWNER_PRINCIPAL.id,
      });
      expect(
        fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
      ).toEqual(prepared);
      expect(await fixture.reservations.readForAdmission(claim.key)).toEqual(
        before,
      );
      expect(settle).not.toHaveBeenCalled();
      expect(release).not.toHaveBeenCalled();
      expect(approvals).not.toHaveBeenCalled();
      expect(completion).not.toHaveBeenCalled();
      expect(effects).not.toHaveBeenCalled();
      expect(response.status).toBe(503);
    }
    expect(
      (
        await fixture.workflows.loadWorkflowSnapshot({
          workflowName: 'host-r1',
          runId,
        })
      )?.status,
    ).toBe('cancelled');
    expect(fixture.effects()).toBe(1);
  });

  it('retains prepared nonterminal ownership when the store disappears after native persistence', async () => {
    const fixture = await hostR1WorkflowFixture('custom-null');
    const reserved = await fixture.reservations.reserve({
      key: 'workflow-finalizer-key',
      owner: { kind: 'human', id: OWNER_PRINCIPAL.id },
      targetKind: 'workflow',
      targetId: 'host-r1',
      mintRunId: () => 'finalizer-run',
    });
    const claim = await fixture.reservations.claimReservation(
      reserved.reservation,
    );
    if (!claim) throw new Error('missing finalizer claim');
    const native = fixture.app.runtime.start.bind(fixture.app.runtime);
    vi.spyOn(fixture.app.runtime, 'start').mockImplementation(
      async (...args) => {
        const summary = await native(...args);
        vi.spyOn(
          fixture.app.runtime,
          'startIdempotency',
          'get',
        ).mockReturnValue(undefined);
        return summary;
      },
    );
    const settle = vi.spyOn(fixture.owners, 'settleReservation');
    const response = await fixture.runner.fetch(
      post('/runs', {
        workflowId: 'host-r1',
        runId: 'finalizer-run',
        inputData: {},
        idempotencyKey: claim.key,
        startReservation: claim,
      }),
    );
    expect(settle).not.toHaveBeenCalled();
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
    ).toMatchObject({ phase: 'prepared-unfenced', startReservation: claim });
    expect(
      (
        await fixture.workflows.loadWorkflowSnapshot({
          workflowName: 'host-r1',
          runId: 'finalizer-run',
        })
      )?.status,
    ).toBe('suspended');
    expect(
      await fixture.reservations.readForAdmission(claim.key),
    ).toMatchObject({ state: 'started', binding: { kind: 'bound' } });
    expect(fixture.journal.alarms.at(-1)).toBeGreaterThan(Date.now());
    expect(response.status).toBe(503);
  });
});

describe('FS8 D3 host R1 workflow legacy cleanup wait guards', () => {
  it.each([
    'dispatch',
    'owner release',
    'completion',
  ] as const)('retains a journal appearing during the final legacy %s wait', async (boundary) => {
    const fixture = await hostR1WorkflowFixture('custom-null');
    const runId = 'late-journal';
    expect(
      (
        await fixture.runner.fetch(
          post('/runs', {
            workflowId: 'host-r1',
            runId,
            inputData: {},
          }),
        )
      ).status,
    ).toBe(200);
    const snapshot = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: 'host-r1',
      runId,
    });
    if (!snapshot?.requestContext) throw new Error('missing stored run');
    delete snapshot.requestContext['flowsafe.runProvenance'];
    snapshot.requestContext['flowsafe.runLifecycle'] = {
      version: 1,
      revision: 1,
      scheduleDispatch: {
        scheduleId: 'late-schedule',
        dispatchId: 'late-dispatch',
      },
    };
    await fixture.workflows.persistWorkflowSnapshot({
      workflowName: 'host-r1',
      runId,
      snapshot,
    });
    const journal = { version: 1 };
    const replace = () =>
      fixture.journal.values.set('flowsafe:run-owner-recovery:v1', journal);
    const dispatch = vi.fn(async () => {
      if (boundary === 'dispatch') replace();
    });
    fixture.env.lifecycle = {
      abandonApprovals: async () => {},
      discardScheduleDispatch: dispatch,
    };
    const nativeRelease = fixture.owners.release.bind(fixture.owners);
    const release = vi
      .spyOn(fixture.owners, 'release')
      .mockImplementation(async (...args) => {
        const result = await nativeRelease(...args);
        if (boundary === 'owner release') replace();
        return result;
      });
    const nativeComplete = fixture.app.runtime.completeTerminalCleanup.bind(
      fixture.app.runtime,
    );
    const complete = vi
      .spyOn(fixture.app.runtime, 'completeTerminalCleanup')
      .mockImplementation(async (...args) => {
        const result = await nativeComplete(...args);
        if (boundary === 'completion') replace();
        return result;
      });
    const response = await fixture.runner.fetch(
      post(`/runs/host-r1/${runId}/terminate`, {}),
    );
    expect(
      fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
      await response.clone().text(),
    ).toEqual(journal);
    expect(await fixture.owners.owner('run', runId)).toEqual(
      boundary === 'dispatch'
        ? { kind: 'human', id: OWNER_PRINCIPAL.id }
        : undefined,
    );
    expect(dispatch).toHaveBeenCalledOnce();
    expect(release).toHaveBeenCalledTimes(boundary === 'dispatch' ? 0 : 1);
    expect(complete).toHaveBeenCalledTimes(boundary === 'completion' ? 1 : 0);
    expect(response.status).toBe(503);
  });

  it.each([
    'journal',
    'active execution',
  ] as const)('retains owner after approval wait changes legacy authority (%s)', async (change) => {
    const fixture = await hostR1WorkflowFixture('custom-null');
    expect(
      (
        await fixture.runner.fetch(
          post('/runs', {
            workflowId: 'host-r1',
            runId: 'legacy-wait',
            inputData: {},
          }),
        )
      ).status,
    ).toBe(200);
    const snapshot = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: 'host-r1',
      runId: 'legacy-wait',
    });
    if (!snapshot?.requestContext) throw new Error('missing Core row');
    delete snapshot.requestContext['flowsafe.runProvenance'];
    await fixture.workflows.persistWorkflowSnapshot({
      workflowName: 'host-r1',
      runId: 'legacy-wait',
      snapshot,
    });
    fixture.env.lifecycle = {
      abandonApprovals: async () => {
        if (change === 'journal')
          fixture.journal.values.set('flowsafe:run-owner-recovery:v1', {
            version: 1,
          });
        else vi.spyOn(fixture.app.runtime, 'isRunActive').mockReturnValue(true);
      },
    };
    const release = vi.spyOn(fixture.owners, 'release');
    const complete = vi.spyOn(fixture.app.runtime, 'completeTerminalCleanup');
    const response = await fixture.runner.fetch(
      post('/runs/host-r1/legacy-wait/terminate', {}),
    );
    expect(await fixture.owners.owner('run', 'legacy-wait')).toEqual({
      kind: 'human',
      id: OWNER_PRINCIPAL.id,
    });
    expect(release).not.toHaveBeenCalled();
    expect(complete).not.toHaveBeenCalled();
    if (change === 'journal')
      expect(
        fixture.journal.values.get('flowsafe:run-owner-recovery:v1'),
      ).toEqual({ version: 1 });
    expect(response.status).toBe(503);
  });
});
