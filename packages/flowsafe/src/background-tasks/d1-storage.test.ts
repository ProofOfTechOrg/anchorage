// SPDX-License-Identifier: Apache-2.0
// D1 execution adapters: serialized workflow updates and deployment-wide task
// listing/deletion over the same storage composition seam hosts use.

import { BackgroundTasksStorageD1 } from '@mastra/cloudflare-d1';
import type { BackgroundTask } from '@mastra/core/background-tasks';
import type { Mastra } from '@mastra/core/mastra';
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import { createD1Storage } from '../do-runner/index.js';
import {
  backgroundTasksStore,
  createBackgroundTaskD1Domains,
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
  it('constructs deployment-wide background-task domains', () => {
    const binding = d1DatabaseLike(openSqlite()) as never;
    expect(() => createBackgroundTaskD1Domains({ binding })).not.toThrow();
  });

  it('serializes partial workflow updates and reports concurrent-update support', async () => {
    const binding = d1DatabaseLike(openSqlite()) as never;
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
    const binding = d1DatabaseLike(openSqlite()) as never;
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
