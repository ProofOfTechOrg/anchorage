// SPDX-License-Identifier: Apache-2.0
// The tenant-bound read routes (DL-014): list/stream REQUIRE a runId/threadId
// filter and validate its salted prefix; getTask 404s a missing OR foreign task
// with no oracle; a foreign filter is refused; the raw manager is never exposed.

import type {
  BackgroundTask,
  BackgroundTaskManager as BackgroundTaskManagerType,
  TaskFilter,
} from '@mastra/core/background-tasks';
import { BackgroundTaskManager } from '@mastra/core/background-tasks';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import { createHostPubSub } from '../do-runner/index.js';
import { backgroundTasksStore } from './d1-storage.js';
import { createBackgroundTaskRoutes } from './routes.js';

// A minimal stand-in for BackgroundTaskManager exposing only what the routes
// read. The routes wrap this — they must never return it over the wire.
function stubManager(tasks: BackgroundTask[]): BackgroundTaskManagerType {
  return {
    getTask: async (taskId: string) =>
      tasks.find((task) => task.id === taskId) ?? null,
    listTasks: async (filter: TaskFilter) => {
      const matched = tasks.filter(
        (task) =>
          (filter.runId === undefined || task.runId === filter.runId) &&
          (filter.threadId === undefined || task.threadId === filter.threadId),
      );
      return { tasks: matched, total: matched.length };
    },
    stream: () => new ReadableStream({ start: (c) => c.close() }),
  } as unknown as BackgroundTaskManagerType;
}

function task(overrides: Partial<BackgroundTask>): BackgroundTask {
  return {
    id: 't1',
    status: 'running',
    toolName: 'longResearch',
    toolCallId: 'call-1',
    args: {},
    agentId: 'agent-1',
    runId: 'abc_r1',
    createdAt: new Date(),
    retryCount: 0,
    maxRetries: 0,
    timeoutMs: 300_000,
    ...overrides,
  } as BackgroundTask;
}

const TENANT = 'abc';

function get(path: string): Request {
  return new Request(`http://do${path}`);
}

describe('createBackgroundTaskRoutes', () => {
  it('returns null for a non-matching path or a non-GET method', async () => {
    const routes = createBackgroundTaskRoutes({ manager: stubManager([]) });
    expect(await routes(get('/other'), TENANT)).toBeNull();
    expect(
      await routes(
        new Request('http://do/background-tasks', { method: 'POST' }),
        TENANT,
      ),
    ).toBeNull();
  });

  describe('list', () => {
    it('400s when no runId/threadId filter is supplied', async () => {
      const routes = createBackgroundTaskRoutes({ manager: stubManager([]) });
      const res = await routes(get('/background-tasks'), TENANT);
      expect(res?.status).toBe(400);
    });

    it('404s a filter naming another tenant (no oracle)', async () => {
      const routes = createBackgroundTaskRoutes({ manager: stubManager([]) });
      const res = await routes(get('/background-tasks?runId=xyz_r1'), TENANT);
      expect(res?.status).toBe(404);
    });

    it('lists the tasks for a tenant-owned runId', async () => {
      const routes = createBackgroundTaskRoutes({
        manager: stubManager([
          task({ id: 't1', runId: 'abc_r1' }),
          task({ id: 't2', runId: 'abc_r2' }),
        ]),
      });
      const res = await routes(get('/background-tasks?runId=abc_r1'), TENANT);
      expect(res?.status).toBe(200);
      const body = (await res?.json()) as {
        tasks: BackgroundTask[];
        total: number;
      };
      expect(body.total).toBe(1);
      expect(body.tasks[0]?.id).toBe('t1');
    });

    it('lists by a tenant-owned threadId', async () => {
      const routes = createBackgroundTaskRoutes({
        manager: stubManager([task({ id: 't1', threadId: 'abc_thread' })]),
      });
      const res = await routes(
        get('/background-tasks?threadId=abc_thread'),
        TENANT,
      );
      expect(res?.status).toBe(200);
    });

    it('drops a row a regressed core filter leaks that is not the requested scope (per-row parity with the stream guard, DL-014)', async () => {
      // #given — a manager whose listTasks IGNORES the filter (a future core
      // filter regression) and returns a foreign row alongside the owned one
      const leaky = {
        listTasks: async () => ({
          tasks: [
            task({ id: 'mine', runId: 'abc_r1' }),
            task({ id: 'leaked', runId: 'xyz_r1' }),
          ],
          total: 2,
        }),
      } as unknown as BackgroundTaskManager;
      const routes = createBackgroundTaskRoutes({ manager: leaky });
      // #when — listing a tenant-owned run
      const res = await routes(get('/background-tasks?runId=abc_r1'), TENANT);
      // #then — only the in-scope row survives, and total reflects the owned count
      expect(res?.status).toBe(200);
      const body = (await res?.json()) as {
        tasks: BackgroundTask[];
        total: number;
      };
      expect(body.tasks.map((t) => t.id)).toEqual(['mine']);
      expect(body.total).toBe(1);
    });
  });

  describe('getTask', () => {
    it('404s a missing task', async () => {
      const routes = createBackgroundTaskRoutes({ manager: stubManager([]) });
      const res = await routes(get('/background-tasks/task/nope'), TENANT);
      expect(res?.status).toBe(404);
    });

    it("404s another tenant's task with the SAME response as a missing one (no oracle)", async () => {
      const routes = createBackgroundTaskRoutes({
        manager: stubManager([task({ id: 'foreign', runId: 'xyz_r1' })]),
      });
      const foreign = await routes(
        get('/background-tasks/task/foreign'),
        TENANT,
      );
      const missing = await routes(get('/background-tasks/task/nope'), TENANT);
      expect(foreign?.status).toBe(404);
      expect(missing?.status).toBe(404);
      expect(await foreign?.text()).toBe(await missing?.text());
    });

    it('returns a tenant-owned task', async () => {
      const routes = createBackgroundTaskRoutes({
        manager: stubManager([task({ id: 'mine', runId: 'abc_r9' })]),
      });
      const res = await routes(get('/background-tasks/task/mine'), TENANT);
      expect(res?.status).toBe(200);
      const body = (await res?.json()) as { task: BackgroundTask };
      expect(body.task.id).toBe('mine');
    });

    it('404s a malformed percent-encoded taskId (no decodeURIComponent throw)', async () => {
      // A lone '%' would make bare decodeURIComponent throw a URIError out of
      // the DO handler; safeDecodeSegment routes it to the no-oracle 404 instead.
      const routes = createBackgroundTaskRoutes({ manager: stubManager([]) });
      const res = await routes(get('/background-tasks/task/%'), TENANT);
      expect(res?.status).toBe(404);
    });

    it('404s a taskId containing a path separator (never splits into a subpath)', async () => {
      const routes = createBackgroundTaskRoutes({ manager: stubManager([]) });
      const res = await routes(get('/background-tasks/task/a%2Fb'), TENANT);
      expect(res?.status).toBe(404);
    });
  });

  describe('stream (SSE)', () => {
    it('400s without a filter and 404s a foreign filter', async () => {
      const routes = createBackgroundTaskRoutes({ manager: stubManager([]) });
      expect(
        (await routes(get('/background-tasks/stream'), TENANT))?.status,
      ).toBe(400);
      expect(
        (await routes(get('/background-tasks/stream?runId=xyz_r1'), TENANT))
          ?.status,
      ).toBe(404);
    });

    it('opens an event-stream for a tenant-owned runId', async () => {
      const routes = createBackgroundTaskRoutes({ manager: stubManager([]) });
      const res = await routes(
        get('/background-tasks/stream?runId=abc_r1'),
        TENANT,
      );
      expect(res?.status).toBe(200);
      expect(res?.headers.get('content-type')).toBe('text/event-stream');
      await res?.body?.cancel();
    });

    it('the transform DROPS a chunk whose runId is not the requested scope (belt over core, DL-014)', async () => {
      // #given — a manager stream emitting an owned chunk and a foreign one
      const manager = {
        stream: () =>
          new ReadableStream<Record<string, unknown>>({
            start(controller) {
              controller.enqueue({
                type: 'background-task-running',
                payload: { runId: 'abc_r1', taskId: 'mine' },
              });
              controller.enqueue({
                type: 'background-task-running',
                payload: { runId: 'xyz_r9', taskId: 'foreign' },
              });
              controller.close();
            },
          }),
      } as unknown as BackgroundTaskManagerType;
      const routes = createBackgroundTaskRoutes({ manager });
      // #when — the SSE body is drained
      const res = await routes(
        get('/background-tasks/stream?runId=abc_r1'),
        TENANT,
      );
      const text = await new Response(res?.body).text();
      // #then — only the in-scope chunk reaches the wire; the foreign one is gone
      expect(text).toContain('abc_r1');
      expect(text).toContain('mine');
      expect(text).not.toContain('xyz_r9');
      expect(text).not.toContain('foreign');
    });

    it('drops malformed and flat legacy chunks fail-closed', async () => {
      const manager = {
        stream: () =>
          new ReadableStream<Record<string, unknown>>({
            start(controller) {
              controller.enqueue({
                type: 'background-task-running',
                runId: 'abc_r1',
                taskId: 'flat',
              });
              controller.enqueue({
                type: 'background-task-running',
                payload: [],
              });
              controller.enqueue({
                type: 'background-task-running',
                payload: null,
              });
              controller.close();
            },
          }),
      } as unknown as BackgroundTaskManagerType;
      const routes = createBackgroundTaskRoutes({ manager });
      const res = await routes(
        get('/background-tasks/stream?runId=abc_r1'),
        TENANT,
      );

      expect(await new Response(res?.body).text()).toBe('');
    });

    it('forwards the exact nested event shape emitted by the real manager stream', async () => {
      const storage = new InMemoryStore();
      const mastra = new Mastra({ storage });
      const manager = new BackgroundTaskManager({ enabled: true });
      manager.__registerMastra(mastra);
      await manager.init(createHostPubSub());
      const stored = await backgroundTasksStore(mastra);
      await stored.createTask(
        task({ id: 'real', runId: 'abc_r1', status: 'running' }),
      );
      const routes = createBackgroundTaskRoutes({ manager });

      try {
        const res = await routes(
          get('/background-tasks/stream?runId=abc_r1'),
          TENANT,
        );
        const reader = res?.body?.getReader();
        const first = await reader?.read();
        await reader?.cancel();
        const text = new TextDecoder().decode(first?.value);
        const serialized = text.slice('data: '.length).trim();
        expect(JSON.parse(serialized)).toMatchObject({
          type: 'background-task-running',
          payload: {
            taskId: 'real',
            runId: 'abc_r1',
          },
        });
      } finally {
        await manager.shutdown();
      }
    });
  });
});
