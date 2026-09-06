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
} from '../do-runner/index.js';
import {
  backgroundTasksStore,
  createBackgroundTaskD1Domains,
  DurableObjectBackgroundTasksStorageD1,
  DurableObjectWorkflowsStorageD1,
} from './d1-storage.js';

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
    const read = (domain: ReturnType<typeof construct>) =>
      domain instanceof BackgroundTasksStorageD1
        ? domain.getTask('task')
        : domain.loadWorkflowSnapshot({
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
      const domain = construct(config);
      if (domain instanceof FencedWorkflowsStorageD1)
        expect(domain[FENCED_WORKFLOW_STORAGE]?.database).toBe(binding);
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
