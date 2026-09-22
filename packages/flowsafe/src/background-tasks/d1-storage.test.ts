// SPDX-License-Identifier: Apache-2.0
// D1 execution adapters: serialized workflow updates and deployment-wide task
// listing/deletion over the same storage composition seam hosts use.

import {
  BackgroundTasksStorageD1,
  type D1DomainConfig,
} from '@mastra/cloudflare-d1';
import type { BackgroundTask } from '@mastra/core/background-tasks';
import type { Mastra } from '@mastra/core/mastra';
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  createD1Storage,
  ExecutionFenceStore,
  FENCED_WORKFLOW_STORAGE,
  FencedWorkflowsStorageD1,
  type InitialAdmissionDatabase,
  type InitialTerminalizationRequest,
} from '../do-runner/index.js';
import {
  backgroundTasksStore,
  createBackgroundTaskD1Domains,
  DurableObjectBackgroundTasksStorageD1,
  DurableObjectWorkflowsStorageD1,
} from './d1-storage.js';

async function queuedInitial(withIntent = true) {
  const sql = openSqlite();
  const binding = sqliteUnitDatabase(sql) as InitialAdmissionDatabase;
  const workflows = new DurableObjectWorkflowsStorageD1({
    binding: binding as never,
  });
  await workflows.init();
  const capability = workflows[FENCED_WORKFLOW_STORAGE];
  if (!capability) throw new Error('queued capability missing');
  const execution = {
    tablePrefix: '',
    workflowId: 'workflow',
    runId: 'run',
    startToken: 'generation',
  };
  const fence = new ExecutionFenceStore(binding);
  const { witness } = await capability.withInitialAdmission(
    {
      execution,
      attemptToken: 'correlation',
      fence,
      requestContext: {
        app: { keep: 'λ' },
        'flowsafe.runProvenance': {
          version: 2,
          startToken: 'generation',
          attemptToken: 'correlation',
          resumeCounts: [],
        },
        'flowsafe.runLifecycle': {
          version: 1,
          revision: 2,
          scheduleDispatch: { scheduleId: 'schedule', dispatchId: 'dispatch' },
          ...(withIntent
            ? {
                transitionIntent: {
                  status: 'cancelled',
                  requestedAt: 1,
                  replayPrincipals: [{ kind: 'human', id: 'original' }],
                },
              }
            : {}),
        },
      },
      onInitialWriteAttempt: () => undefined,
    },
    () =>
      workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: createEmptyWorkflowSnapshot('run'),
      }),
  );
  return {
    sql,
    binding,
    workflows,
    capability,
    execution,
    expected: witness.row,
  };
}

describe('initial terminalization in the real background queue', () => {
  const callerFields = [
    'request.expected',
    'request.execution',
    'request.attemptToken',
    'request.nowMs',
    'expected.tablePrefix',
    'expected.workflowId',
    'expected.runId',
    'expected.resourceId',
    'expected.snapshot',
    'expected.createdAt',
    'expected.updatedAt',
    'execution.tablePrefix',
    'execution.workflowId',
    'execution.runId',
    'execution.startToken',
  ] as const;

  function holdStatement(
    h: Awaited<ReturnType<typeof queuedInitial>>,
    pattern: RegExp,
  ) {
    let release = () => {};
    let entered = () => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ready = new Promise<void>((resolve) => {
      entered = resolve;
    });
    const prepare = h.binding.prepare.bind(h.binding);
    let holdNext = true;
    const boundCalls: Array<{ sql: string; values: unknown[] }> = [];
    const calls = vi.spyOn(h.binding, 'prepare').mockImplementation((sql) => {
      const statement = prepare(sql);
      const bind = statement.bind.bind(statement);
      vi.spyOn(statement, 'bind').mockImplementation((...values) => {
        boundCalls.push({ sql, values });
        const bound = bind(...values);
        if (holdNext && pattern.test(sql)) {
          holdNext = false;
          const all = bound.all.bind(bound);
          vi.spyOn(bound, 'all').mockImplementation(async () => {
            entered();
            await held;
            return all();
          });
        }
        return bound;
      });
      return statement;
    });
    return { release, ready, calls, boundCalls };
  }

  it.each(
    callerFields,
  )('preserves the first %s getter fault before the held queue is released', async (field) => {
    const h = await queuedInitial();
    const barrier = holdStatement(h, /^INSERT INTO/i);
    const inflight: Promise<unknown>[] = [];
    try {
      const holder = h.workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: JSON.parse(h.expected.snapshot),
        updatedAt: new Date(h.expected.updatedAt),
      });
      inflight.push(holder);
      await barrier.ready;
      barrier.calls.mockClear();
      const request: InitialTerminalizationRequest = {
        expected: { ...h.expected },
        execution: { ...h.execution },
        attemptToken: 'correlation',
        nowMs: 1_700_000_000_123,
      };
      const [part, key] = field.split('.');
      if (!key) throw new Error('getter key missing');
      const target =
        part === 'request'
          ? request
          : part === 'expected'
            ? request.expected
            : request.execution;
      const fault = new Error(`first caller getter: ${field}`);
      const getter = vi.fn(() => {
        throw fault;
      });
      Object.defineProperty(target, key, { enumerable: true, get: getter });
      let outcome: unknown;
      const operation = h.capability.terminalizeInitialAdmission(request).then(
        (result) => {
          outcome = result;
        },
        (error: unknown) => {
          outcome = error;
        },
      );
      inflight.push(operation);
      await Promise.resolve();
      expect(outcome).toBe(fault);
      expect(getter).toHaveBeenCalledTimes(1);
      expect(barrier.calls).not.toHaveBeenCalled();
      expect(
        h.sql
          .prepare(
            "SELECT snapshot FROM mastra_workflow_snapshot WHERE run_id = 'run'",
          )
          .all(),
      ).toEqual([{ snapshot: h.expected.snapshot }]);
    } finally {
      barrier.release();
      await Promise.allSettled(inflight);
      vi.restoreAllMocks();
    }
  });

  it('keeps changed pending bytes after a held partial writer instead of refreshing repair', async () => {
    const h = await queuedInitial();
    const barrier = holdStatement(h, /^INSERT INTO/i);
    const inflight: Promise<unknown>[] = [];
    try {
      const holder = h.workflows.updateWorkflowState({
        workflowName: 'workflow',
        runId: 'run',
        opts: {
          status: 'pending',
          tracingContext: { traceId: 'changed-pending', spanId: 'partial' },
        },
      });
      inflight.push(holder);
      await barrier.ready;
      const operation = h.capability.terminalizeInitialAdmission({
        expected: h.expected,
        execution: h.execution,
        attemptToken: 'correlation',
        nowMs: 1_700_000_000_123,
      });
      inflight.push(operation);
      void operation.catch(() => undefined);
      await h.workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'unrelated',
        snapshot: createEmptyWorkflowSnapshot('unrelated'),
      });
      expect(
        barrier.calls.mock.calls.filter(([sql]) => sql.startsWith('UPDATE "')),
      ).toEqual([]);
      barrier.release();
      await holder;
      const result = await operation;
      expect(result.kind).toBe('conflict');
      expect(result).not.toHaveProperty('cleanup');
      expect(
        barrier.calls.mock.calls.filter(([sql]) => sql.startsWith('UPDATE "')),
      ).toHaveLength(1);
      const row = await h.capability.readSnapshot(h.execution);
      expect(result.row).toEqual(row);
      const snapshot = JSON.parse(row?.snapshot ?? '');
      expect(snapshot.status).toBe('pending');
      expect(snapshot.tracingContext).toEqual({
        traceId: 'changed-pending',
        spanId: 'partial',
      });
      expect(
        snapshot.requestContext['flowsafe.runProvenance'].initialAdmission,
      ).toBe(true);
      expect(snapshot.requestContext['flowsafe.runLifecycle'].revision).toBe(2);
    } finally {
      barrier.release();
      await Promise.allSettled(inflight);
      vi.restoreAllMocks();
    }
  });

  it('holds a later partial update behind repair and preserves the terminal projection', async () => {
    const h = await queuedInitial(false);
    const barrier = holdStatement(h, /^UPDATE "/);
    const inflight: Promise<unknown>[] = [];
    try {
      const operation = h.capability.terminalizeInitialAdmission({
        expected: h.expected,
        execution: h.execution,
        attemptToken: 'correlation',
        nowMs: 1_700_000_000_123,
      });
      inflight.push(operation);
      void operation.catch(() => undefined);
      await barrier.ready;
      let updated = false;
      const partial = h.workflows
        .updateWorkflowState({
          workflowName: 'workflow',
          runId: 'run',
          opts: {
            status: 'failed',
            tracingContext: {
              traceId: 'retained-after-repair',
              spanId: 'partial',
            },
          },
        })
        .finally(() => {
          updated = true;
        });
      inflight.push(partial);
      void partial.catch(() => undefined);
      await h.workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'unrelated',
        snapshot: createEmptyWorkflowSnapshot('unrelated'),
      });
      expect(updated).toBe(false);
      expect(
        barrier.boundCalls.filter(
          ({ sql, values }) => /^SELECT/i.test(sql) && values.includes('run'),
        ),
      ).toEqual([]);
      barrier.release();
      const result = await operation;
      expect(result.kind).toBe('terminalized');
      const state = await partial;
      expect(state).toMatchObject({
        status: 'failed',
        tracingContext: { traceId: 'retained-after-repair', spanId: 'partial' },
        error: { name: 'StartOutcomeUnknown' },
        requestContext: {
          app: { keep: 'λ' },
          'flowsafe.runLifecycle': { revision: 2 },
        },
      });
      const row = await h.capability.readSnapshot(h.execution);
      const snapshot = JSON.parse(row?.snapshot ?? '');
      expect(snapshot).toEqual(state);
      expect(
        Object.hasOwn(
          snapshot.requestContext['flowsafe.runProvenance'],
          'initialAdmission',
        ),
      ).toBe(false);
      expect(
        barrier.calls.mock.calls.filter(([sql]) => sql.startsWith('UPDATE "')),
      ).toHaveLength(1);
    } finally {
      barrier.release();
      await Promise.allSettled(inflight);
      vi.restoreAllMocks();
    }
  });

  it.each([
    ...callerFields,
    'capability-before',
    'capability-during',
    'late-getters',
  ])('captures %s before an ordinary persist holder yields the run lock', async (variant) => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(1_700_000_000_000);
    let release = () => {};
    const inflight: Promise<unknown>[] = [];
    try {
      const h = await queuedInitial();
      const foreign = await queuedInitial();
      const foreignBefore = foreign.sql
        .prepare('SELECT * FROM mastra_workflow_snapshot')
        .all();
      const saved = h.capability;
      if (variant === 'capability-before')
        Object.defineProperty(h.workflows, FENCED_WORKFLOW_STORAGE, {
          value: foreign.capability,
        });
      let entered = () => {};
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const enteredPromise = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const prepare = h.binding.prepare.bind(h.binding);
      let holdNext = true;
      const casValues: unknown[][] = [];
      const calls = vi.spyOn(h.binding, 'prepare').mockImplementation((sql) => {
        const statement = prepare(sql);
        const bind = statement.bind.bind(statement);
        vi.spyOn(statement, 'bind').mockImplementation((...values) => {
          const bound = bind(...values);
          if (sql.startsWith('UPDATE "')) casValues.push(values);
          if (/^INSERT INTO/i.test(sql) && holdNext) {
            holdNext = false;
            const all = bound.all.bind(bound);
            vi.spyOn(bound, 'all').mockImplementation(async () => {
              entered();
              await held;
              return all();
            });
          }
          return bound;
        });
        return statement;
      });
      const holder = h.workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: JSON.parse(h.expected.snapshot),
        updatedAt: new Date(h.expected.updatedAt),
      });
      inflight.push(holder);
      await enteredPromise;
      const reads = new Map<string, number>();
      function observed<T extends object>(source: T, label: string): T {
        const copy = { ...source };
        for (const key of Object.keys(source))
          Object.defineProperty(copy, key, {
            enumerable: true,
            configurable: true,
            get() {
              const name = `${label}.${key}`;
              const count = (reads.get(name) ?? 0) + 1;
              reads.set(name, count);
              if (variant === 'late-getters' && count > 1)
                throw new Error(`late caller getter: ${name}`);
              return source[key as keyof T];
            },
          });
        return copy;
      }
      const raw = { ...h.expected };
      const execution = { ...h.execution };
      const request = {
        expected: observed(raw, 'expected'),
        execution: observed(execution, 'execution'),
        attemptToken: 'correlation',
        nowMs: 1_700_000_000_123,
      };
      let done = false;
      const operation = saved
        .terminalizeInitialAdmission(observed(request, 'request'))
        .finally(() => {
          done = true;
        });
      inflight.push(operation);
      void operation.catch(() => undefined);
      expect(reads.size).toBe(15);
      expect([...reads.values()]).toEqual(new Array(15).fill(1));
      if (variant === 'capability-during')
        Object.defineProperty(h.workflows, FENCED_WORKFLOW_STORAGE, {
          value: foreign.capability,
        });
      else if (
        !variant.startsWith('capability') &&
        variant !== 'late-getters'
      ) {
        const [part, key] = variant.split('.');
        const target =
          part === 'request' ? request : part === 'expected' ? raw : execution;
        if (!key) throw new Error('mutation key missing');
        Object.defineProperty(target, key, {
          value: null,
          configurable: true,
          enumerable: true,
        });
      }
      await h.workflows.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'unrelated',
        snapshot: createEmptyWorkflowSnapshot('unrelated'),
      });
      expect(done).toBe(false);
      expect(casValues).toEqual([]);
      expect(
        calls.mock.calls.filter(([sql]) => sql.startsWith('UPDATE "')),
      ).toEqual([]);
      release();
      await holder;
      const result = await operation;
      expect(result.kind).toBe('terminalized');
      if (result.kind === 'conflict') throw new Error('unexpected conflict');
      expect(result.row.updatedAt).toBe('2023-11-14T22:13:20.123Z');
      expect(casValues).toEqual([
        [
          result.row.snapshot,
          result.row.updatedAt,
          'workflow',
          'run',
          h.expected.snapshot,
          h.expected.createdAt,
          h.expected.updatedAt,
          h.expected.resourceId,
        ],
      ]);
      expect(result.cleanup).toEqual({
        revision: 3,
        status: 'cancelled',
        cleanupCompleted: false,
        scheduleDispatch: { scheduleId: 'schedule', dispatchId: 'dispatch' },
      });
      const snapshot = JSON.parse(result.row.snapshot);
      expect(snapshot.timestamp).toBe(1_700_000_000_123);
      expect(snapshot.requestContext.app).toEqual({ keep: 'λ' });
      expect(snapshot.requestContext['flowsafe.runProvenance']).toEqual({
        version: 2,
        startToken: 'generation',
        attemptToken: 'correlation',
        resumeCounts: [],
      });
      expect([...reads.values()]).toEqual(new Array(15).fill(1));
      expect(
        foreign.sql.prepare('SELECT * FROM mastra_workflow_snapshot').all(),
      ).toEqual(foreignBefore);
    } finally {
      release();
      await Promise.allSettled(inflight);
      vi.restoreAllMocks();
      vi.useRealTimers();
    }
  });

  it('waits for a real partial update and preserves its resulting progress', async () => {
    const h = await queuedInitial();
    const prepare = h.binding.prepare.bind(h.binding);
    let entered = () => {};
    let release = () => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const enteredPromise = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let holdNext = true;
    vi.spyOn(h.binding, 'prepare').mockImplementation((sql) => {
      const statement = prepare(sql);
      const bind = statement.bind.bind(statement);
      vi.spyOn(statement, 'bind').mockImplementation((...values) => {
        const bound = bind(...values);
        if (/^INSERT INTO/i.test(sql) && holdNext) {
          holdNext = false;
          const all = bound.all.bind(bound);
          vi.spyOn(bound, 'all').mockImplementation(async () => {
            entered();
            await held;
            return all();
          });
        }
        return bound;
      });
      return statement;
    });
    try {
      const holder = h.workflows.updateWorkflowState({
        workflowName: 'workflow',
        runId: 'run',
        opts: { status: 'running' },
      });
      await enteredPromise;
      const request: InitialTerminalizationRequest = {
        expected: h.expected,
        execution: h.execution,
        attemptToken: 'correlation',
        nowMs: 123,
      };
      const operation = h.capability.terminalizeInitialAdmission(request);
      release();
      await holder;
      const result = await operation;
      expect(result.kind).toBe('progressed');
      const row = await h.capability.readSnapshot(h.execution);
      expect(row).toEqual(result.row);
      expect(JSON.parse(row?.snapshot ?? '').status).toBe('running');
      expect(
        JSON.parse(row?.snapshot ?? '').requestContext['flowsafe.runProvenance']
          .initialAdmission,
      ).toBe(true);
    } finally {
      release();
      vi.restoreAllMocks();
    }
  });
});

describe('backgroundTasksStore — fail-closed accessor', () => {
  it('throws a clear message when the hosting Mastra has no storage', async () => {
    const mastra = { getStorage: () => undefined } as unknown as Mastra;
    await expect(backgroundTasksStore(mastra)).rejects.toThrow(
      /has no storage configured/,
    );
  });

  it("throws when the storage adapter does not implement the 'backgroundTasks' domain", async () => {
    // A composite store whose getStore('backgroundTasks') resolves null — a
    // storage adapter that does not ship the domain the manager reads through.
    const mastra = {
      getStorage: () => ({ getStore: async () => null }),
    } as unknown as Mastra;
    await expect(backgroundTasksStore(mastra)).rejects.toThrow(
      /does not implement the 'backgroundTasks' domain/,
    );
  });
});

describe('D1 execution domains', () => {
  it.each([
    'owned',
    'queued',
    'tasks',
  ])('preserves selected adapter mode through the %s constructor', async (kind) => {
    const client = {
      query: vi.fn(async () => ({ result: [{ success: true, results: [] }] })),
    };
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    const construct = (config: D1DomainConfig) =>
      kind === 'owned'
        ? new FencedWorkflowsStorageD1(config)
        : kind === 'queued'
          ? new DurableObjectWorkflowsStorageD1(config)
          : new DurableObjectBackgroundTasksStorageD1(
              config,
              new DurableObjectWorkflowsStorageD1({ binding }),
            );
    const read = (storageDomain: ReturnType<typeof construct>) =>
      storageDomain instanceof BackgroundTasksStorageD1
        ? storageDomain.getTask('task')
        : storageDomain.loadWorkflowSnapshot({
            workflowName: 'workflow',
            runId: 'run',
          });
    for (const mode of ['own', 'inherited', 'non-enumerable']) {
      let discarded = 0;
      const config = mode === 'inherited' ? Object.create({ client }) : {};
      if (mode !== 'inherited')
        Object.defineProperty(config, 'client', {
          value: client,
          enumerable: mode === 'own',
        });
      Object.defineProperty(config, 'binding', {
        enumerable: true,
        get() {
          discarded += 1;
          throw new Error('discarded binding');
        },
      });
      Object.defineProperty(config, 'apiToken', {
        enumerable: true,
        get() {
          discarded += 1;
          throw new Error('discarded credential');
        },
      });
      const domain = construct(config);
      await read(domain);
      if (domain instanceof FencedWorkflowsStorageD1)
        expect(domain[FENCED_WORKFLOW_STORAGE]).toBeUndefined();
      expect(discarded).toBe(0);
    }
    expect(client.query).toHaveBeenCalledTimes(3);
    const invalid = {
      client: undefined,
      get binding() {
        throw new Error('binding fallback');
      },
    } as unknown as D1DomainConfig;
    const domain = construct(invalid);
    if (domain instanceof FencedWorkflowsStorageD1)
      expect(domain[FENCED_WORKFLOW_STORAGE]).toBeUndefined();
    await expect(read(domain)).rejects.not.toThrow('binding fallback');
    let bindingReads = 0;
    let prefixReads = 0;
    const selected = construct({
      get binding() {
        bindingReads += 1;
        return binding;
      },
      get tablePrefix() {
        prefixReads += 1;
        return 'First_';
      },
      get apiToken() {
        throw new Error('unused REST');
      },
    } as D1DomainConfig);
    if (selected instanceof FencedWorkflowsStorageD1)
      expect(selected[FENCED_WORKFLOW_STORAGE]?.tablePrefix).toBe('first_');
    expect([bindingReads, prefixReads]).toEqual([1, 1]);
    for (const mode of ['inherited', 'non-enumerable']) {
      const config =
        mode === 'inherited'
          ? Object.create({ binding })
          : Object.defineProperty({}, 'binding', { value: binding });
      const supportedDomain = construct(config);
      if (supportedDomain instanceof FencedWorkflowsStorageD1)
        expect(supportedDomain[FENCED_WORKFLOW_STORAGE]?.database).toBe(
          binding,
        );
    }
    let clientReads = 0;
    const capturedClient = construct({
      get client() {
        clientReads += 1;
        if (clientReads !== 1) throw new Error('client reread');
        return client;
      },
    });
    await read(capturedClient);
    expect(clientReads).toBe(1);
    const restReads: string[] = [];
    const rest = construct({
      get accountId() {
        restReads.push('accountId');
        return 'account';
      },
      get apiToken() {
        restReads.push('apiToken');
        return 'test-token';
      },
      get databaseId() {
        restReads.push('databaseId');
        return 'database';
      },
      get tablePrefix() {
        restReads.push('tablePrefix');
        return 'rest_';
      },
      get unused() {
        throw new Error('unused REST field');
      },
    } as D1DomainConfig);
    expect(restReads).toEqual([
      'accountId',
      'apiToken',
      'databaseId',
      'tablePrefix',
    ]);
    if (rest instanceof FencedWorkflowsStorageD1)
      expect(rest[FENCED_WORKFLOW_STORAGE]).toBeUndefined();
  });

  it('keeps the initial ALS scope through the lock and preserves its stamp on ordinary updates', async () => {
    const binding = sqliteUnitDatabase(
      openSqlite(),
    ) as InitialAdmissionDatabase;
    const workflows = new DurableObjectWorkflowsStorageD1({
      binding: binding as never,
    });
    await workflows.init();
    const capability = workflows[FENCED_WORKFLOW_STORAGE];
    if (!capability) throw new Error('missing queued capability');
    const fence = new ExecutionFenceStore(binding);
    const execution = {
      tablePrefix: '',
      workflowId: 'workflow',
      runId: 'run',
      startToken: 'generation',
    };
    const snapshot = createEmptyWorkflowSnapshot('run');
    const admitted = await capability.withInitialAdmission(
      {
        execution,
        attemptToken: 'correlation',
        fence,
        requestContext: {
          'flowsafe.runProvenance': {
            version: 2,
            startToken: 'generation',
            attemptToken: 'correlation',
            resumeCounts: [],
          },
        },
        onInitialWriteAttempt: () => undefined,
      },
      () =>
        workflows.persistWorkflowSnapshot({
          workflowName: 'workflow',
          runId: 'run',
          snapshot,
        }),
    );
    expect(admitted.witness.execution).toEqual(execution);
    expect(workflows.supportsConcurrentUpdates()).toBe(true);
    await workflows.updateWorkflowState({
      workflowName: 'workflow',
      runId: 'run',
      opts: { status: 'running' },
    });
    expect(
      (
        await workflows.loadWorkflowSnapshot({
          workflowName: 'workflow',
          runId: 'run',
        })
      )?.status,
    ).toBe('running');
    const updated = await capability.readSnapshot(execution);
    expect(updated?.snapshot).not.toBe(admitted.witness.row.snapshot);
    expect(
      JSON.parse(updated?.snapshot ?? '').requestContext[
        'flowsafe.runProvenance'
      ].initialAdmission,
    ).toBe(true);
  });
  it('constructs deployment-wide background-task domains', () => {
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    expect(() => createBackgroundTaskD1Domains({ binding })).not.toThrow();
  });

  it('serializes partial workflow updates and reports concurrent-update support', async () => {
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    const storage = createD1Storage({
      binding,
      domains: createBackgroundTaskD1Domains({
        binding,
      }),
    });
    await storage.init();
    const workflows = await storage.getStore('workflows');
    expect(workflows?.supportsConcurrentUpdates()).toBe(true);
    const snapshot = createEmptyWorkflowSnapshot('task-1');
    await workflows?.persistWorkflowSnapshot({
      workflowName: '__background-task',
      runId: 'task-1',
      snapshot,
    });
    await Promise.all([
      workflows?.updateWorkflowState({
        workflowName: '__background-task',
        runId: 'task-1',
        opts: { status: 'running' },
      }),
      workflows?.updateWorkflowResults({
        workflowName: '__background-task',
        runId: 'task-1',
        stepId: 'execute',
        result: {
          status: 'success',
          output: { ok: true },
          payload: {},
          startedAt: 1,
          endedAt: 2,
        },
        requestContext: { trace: 'yes' },
      }),
    ]);
    const stored = await workflows?.loadWorkflowSnapshot({
      workflowName: '__background-task',
      runId: 'task-1',
    });
    expect(stored).toMatchObject({
      status: 'running',
      context: { execute: { status: 'success', output: { ok: true } } },
      requestContext: { trace: 'yes' },
    });
  });

  it('paginates deployment tasks and cascades internal snapshot deletion', async () => {
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    const storage = createD1Storage({
      binding,
      domains: createBackgroundTaskD1Domains({
        binding,
      }),
    });
    await storage.init();
    const tasks = await storage.getStore('backgroundTasks');
    const workflows = await storage.getStore('workflows');
    const raw = new BackgroundTasksStorageD1({ binding });
    const task = (
      id: string,
      runId: string,
      threadId: string,
      resourceId: string,
    ): BackgroundTask => ({
      id,
      runId,
      resourceId,
      threadId,
      status: 'pending',
      toolName: 'work',
      toolCallId: `call-${id}`,
      args: {},
      agentId: 'agent',
      createdAt: new Date(),
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 1000,
    });
    await raw.createTask(task('task-1', 'run-1', 'thread-1', 'resource-1'));
    await tasks?.createTask(task('task-2', 'run-2', 'thread-2', 'resource-2'));
    await tasks?.createTask(task('task-3', 'run-3', 'thread-3', 'resource-3'));
    const snapshot = createEmptyWorkflowSnapshot('task-2');
    await workflows?.persistWorkflowSnapshot({
      workflowName: '__background-task',
      runId: 'task-2',
      snapshot,
    });

    const firstPage = await tasks?.listTasks({ page: 0, perPage: 1 });
    expect(firstPage?.total).toBe(3);
    expect(firstPage?.tasks).toHaveLength(1);
    const resourceFiltered = await tasks?.listTasks({
      resourceId: 'resource-2',
    });
    expect(resourceFiltered?.tasks.map((entry) => entry.id)).toEqual([
      'task-2',
    ]);

    await tasks?.deleteTask('task-1');
    expect(await raw.getTask('task-1')).toBeNull();
    await tasks?.deleteTask('task-2');
    expect(
      await workflows?.loadWorkflowSnapshot({
        workflowName: '__background-task',
        runId: 'task-2',
      }),
    ).toBeNull();
  });
});
