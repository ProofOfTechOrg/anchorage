// SPDX-License-Identifier: Apache-2.0
import type { DurableObjectState } from '@cloudflare/workers-types';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import {
  deploymentIdentityDatabase,
  deploymentIdentityRequest,
  TEST_DEPLOYMENT_IDENTITY_SECRET,
} from '../../test-support/deployment-identity.js';
import {
  ApprovalService,
  type ExecutionPrincipal,
  encodeExecutionPrincipal,
  InMemoryApprovalStore,
  InMemoryResourceOwnershipStore,
} from '../approval-api/index.js';
import { reconcileApprovalsForSummary } from '../host-kit/approval-bridge.js';
import type { DurableKeyValueStorage } from './cf-types.js';
import type { DeploymentIdentityEnv } from './deployment-identity.js';
import {
  DurableObjectRunner,
  type DurableObjectRunOwner,
  type DurableObjectRunOwnershipStore,
} from './durable-object.js';
import { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
import { init } from './init.js';
import type { RunnerRuntime, RunSummary } from './runtime.js';
import type { ScheduleSourceStore } from './schedule-source.js';

interface TestEnv extends DeploymentIdentityEnv {
  storage: InMemoryStore;
  runtime?: RunnerRuntime;
  owners: DurableObjectRunOwnershipStore;
  schedules?: ScheduleSourceStore;
}

interface OwnerHooks {
  reserve(
    runId: string,
    owner: DurableObjectRunOwner,
    token: string,
  ): Promise<boolean>;
  settle(token: string, runId: string, release: boolean): Promise<void>;
  owner?: (runId: string) => Promise<DurableObjectRunOwner | undefined>;
}

function makeProductionEnv(
  storage = new InMemoryStore(),
  hooks?: OwnerHooks,
): TestEnv {
  const registry = new InMemoryResourceOwnershipStore();
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
      }
    : registry;
  return {
    storage,
    owners,
    DEPLOYMENT_TENANT: 'acme',
    DEPLOYMENT_IDENTITY_SECRET: TEST_DEPLOYMENT_IDENTITY_SECRET,
    DB: deploymentIdentityDatabase(),
  };
}

class TestRunner extends DurableObjectRunner<TestEnv> {
  protected runOwnership(env: TestEnv): DurableObjectRunOwnershipStore {
    return env.owners;
  }

  protected scheduleSource(env: TestEnv): ScheduleSourceStore | undefined {
    return env.schedules;
  }

  protected build(env: TestEnv): RunnerRuntime {
    if (env.runtime) return env.runtime;
    const { createWorkflow, createStep, runtime } = init({
      storage: env.storage,
    });
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
  if (path === '/runs') {
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

function recoveryStorage(events: string[] = []): {
  state: DurableObjectState;
  storage: DurableKeyValueStorage;
  values: Map<string, unknown>;
} {
  const values = new Map<string, unknown>();
  const storage: DurableKeyValueStorage = {
    async get<T>(key: string): Promise<T | undefined> {
      events.push(`get:${key}`);
      return values.get(key) as T | undefined;
    },
    async put<T>(key: string, value: T): Promise<void> {
      events.push(`put:${key}`);
      values.set(key, value);
    },
    async delete(key: string): Promise<boolean> {
      events.push(`delete:${key}`);
      return values.delete(key);
    },
    async setAlarm(): Promise<void> {
      events.push('setAlarm');
    },
    async deleteAlarm(): Promise<void> {
      events.push('deleteAlarm');
    },
  };
  return {
    state: { storage } as unknown as DurableObjectState,
    storage,
    values,
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

describe('DurableObjectRunner.fetch', () => {
  it('rejects a start without a trusted execution principal before runtime or ownership work', async () => {
    const reserve = vi.fn(async () => true);
    const runtime = {
      status: vi.fn(async () => null),
      start: vi.fn(),
    } as unknown as RunnerRuntime;
    const runner = new TestRunner(undefined, {
      ...makeProductionEnv(new InMemoryStore(), {
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
    expect(reserve).not.toHaveBeenCalled();
    expect(runtime.status).not.toHaveBeenCalled();
    expect(runtime.start).not.toHaveBeenCalled();
  });

  it('rejects a new resume leg without requester kind before runtime work', async () => {
    const runtime = {
      resume: vi.fn(),
      status: vi.fn(async () => null),
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
    const owners = new InMemoryResourceOwnershipStore();
    const scheduleOwner = { kind: 'human' as const, id: 'schedule-owner' };
    await owners.claim('schedule', 'schedule-gated', scheduleOwner);
    const env = makeProductionEnv();
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

  it('executes the prepared schedule target payload instead of forged start-body payload', async () => {
    const owners = new InMemoryResourceOwnershipStore();
    await owners.claim('schedule', 'schedule-payload', {
      kind: 'human',
      id: 'schedule-owner',
    });
    const start = vi.fn(
      async (
        _workflowId: string,
        options: Parameters<RunnerRuntime['start']>[1],
      ) => ({
        runId: options.runId,
        status: 'success' as const,
        requestedBy: options.requestedBy,
        requestedByKind: options.requestedByKind,
      }),
    );
    const runtime = {
      start,
      status: vi.fn(async () => null),
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
        requestContext: { source: 'stored-context' },
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
        storedRequestContext: { source: 'stored-context' },
        requestedBy: 'schedule-runner',
        requestedByKind: 'system',
      }),
    );
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
      status: vi.fn(async () => null),
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
    const { state } = recoveryStorage(events);
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
    expect(events.filter((event) => event === 'setAlarm')).toHaveLength(2);
    expect(events).not.toContain('deleteAlarm');
  });

  it('clears the prearmed watchdog when no recovery journal exists', async () => {
    const events: string[] = [];
    const { state } = recoveryStorage(events);
    const runner = new TestRunner(state, makeProductionEnv());

    await runner.alarm();

    expect(events[0]).toBe('setAlarm');
    expect(events.at(-1)).toBe('deleteAlarm');
  });

  it('arms recovery before reserving and commits the reservation after persistence', async () => {
    const events: string[] = [];
    const { state } = recoveryStorage(events);
    const reserve = vi.fn(async () => {
      events.push('reserve');
      return true;
    });
    const settle = vi.fn(async () => {
      events.push('settle');
    });
    const runner = new TestRunner(
      state,
      makeProductionEnv(new InMemoryStore(), { reserve, settle }),
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

  it('returns a persisted start after a lost settlement receipt and clears recovery on retry', async () => {
    const events: string[] = [];
    const { state, values } = recoveryStorage(events);
    const committed = new InMemoryResourceOwnershipStore();
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
    const env = makeProductionEnv();
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
      expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(true);
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
    const { state } = recoveryStorage();
    const reserve = vi.fn(async () => true);
    const settle = vi.fn(async () => undefined);
    const runtime = {
      start: vi.fn(async () => {
        throw new Error('injected pre-snapshot failure');
      }),
      status: vi.fn(async () => null),
      recoverStartAttempt: vi.fn(async () => null),
    } as unknown as RunnerRuntime;
    const runner = new TestRunner(state, {
      ...makeProductionEnv(new InMemoryStore(), { reserve, settle }),
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

  it('does not execute when the owner reservation conflicts', async () => {
    const { state, values } = recoveryStorage();
    const reserve = vi.fn(async () => false);
    const settle = vi.fn(async () => undefined);
    const env = makeProductionEnv(new InMemoryStore(), { reserve, settle });
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
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(true);
    await runner.alarm();
    expect(settle).toHaveBeenCalledWith(
      expect.any(String),
      'run-owner-conflict',
      true,
    );
    expect(values.has('flowsafe:run-owner-recovery:v1')).toBe(false);
  });

  it('rejects a malformed stored recovery journal before touching ownership', async () => {
    const { state, values } = recoveryStorage();
    const settle = vi.fn(async () => undefined);
    values.set('flowsafe:run-owner-recovery:v1', {
      version: 1,
      workflowId: 'gated/forged',
      runId: 'run-recovery',
      token: 'attempt-token',
    });
    const runner = new TestRunner(
      state,
      makeProductionEnv(new InMemoryStore(), {
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
      status: vi.fn(async () => persisted),
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
    const { state } = recoveryStorage();
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
      new ApprovalService({ store: approvalStore }),
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
      makeProductionEnv(new InMemoryStore(), {
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

  it('400s a start without a runId — the DO never generates one (INV-1)', async () => {
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

  it('refuses to act outside its own identity when id.name is present (INV-1)', async () => {
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
      protected runOwnership(env: TestEnv): DurableObjectRunOwnershipStore {
        return env.owners;
      }

      protected build(env: TestEnv): RunnerRuntime {
        const { createWorkflow, createStep, runtime } = init({
          storage: env.storage,
        });
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
    const persisted = new Map<string, unknown>();
    const objectStorage: DurableKeyValueStorage = {
      async get<T>(key: string): Promise<T | undefined> {
        return persisted.get(key) as T | undefined;
      },
      async put<T>(key: string, value: T): Promise<void> {
        persisted.set(key, value);
      },
      async delete(key: string): Promise<boolean> {
        return persisted.delete(key);
      },
      async setAlarm(): Promise<void> {},
      async deleteAlarm(): Promise<void> {},
    };
    // Minimal Durable Object storage stub for run-owner recovery. Resume
    // provenance lives in env.storage, not in this object-local storage.
    const state = {
      storage: objectStorage,
    } as unknown as DurableObjectState;
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

  it('returns a 426 non-WS fallback on the stream route when the runtime has no hibernation API', async () => {
    // #given — a node runner (state undefined ⇒ no acceptWebSocket). The per-run
    // WS stream is workerd-only; off workerd it must degrade, never 500. The WS
    // runtime behavior itself is proven by the workerd spike (M-009).
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

  it('broadcasts the authoritative RunSummary to run-channel sockets after start and resume (DL-018)', async () => {
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
