// SPDX-License-Identifier: Apache-2.0
// Track B host wiring + the B-S2 recovery seam (R-002 pin: the PUBLIC async
// init(pubsub) fires recoverStaleTasks internally — no private method is
// called). Execution-mode recovery is proven over the serialized, tenant-bound
// D1 domains: workers subscribe before init publishes the recovered dispatch,
// and a static executor takes the stranded task through to completion.

import type { ToolExecutor } from '@mastra/core/background-tasks';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryStore, type MastraCompositeStore } from '@mastra/core/storage';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import { createD1Storage, createHostPubSub, init } from '../do-runner/index.js';
import {
  backgroundTasksStore,
  createBackgroundTaskD1Domains,
} from './d1-storage.js';
import { BackgroundTaskHost } from './host.js';

function baseTask(overrides: Record<string, unknown>) {
  const now = new Date();
  return {
    id: 'task-1',
    status: 'running' as const,
    toolName: 'longResearch',
    toolCallId: 'call-1',
    args: {},
    agentId: 'agent-1',
    runId: 'abc_r1',
    createdAt: now,
    startedAt: now,
    retryCount: 0,
    maxRetries: 0,
    timeoutMs: 300_000,
    ...overrides,
  };
}

// createD1Storage creates the mastra_* tables (incl. mastra_background_tasks)
// eagerly on the first persisted run — the same seed the schema-guard test uses.
async function seededD1(): Promise<MastraCompositeStore> {
  const sqlite = openSqlite();
  const storage = createD1Storage({ binding: d1DatabaseLike(sqlite) as never });
  const { createWorkflow, createStep, runtime } = init({ storage });
  const step = createStep({
    id: 'noop',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  createWorkflow({
    id: 'seed',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(step)
    .commit();
  await runtime.start('seed', { runId: 'seed_r0', inputData: {} });
  return storage;
}

async function executionHostDependencies(tenantId = 'acme') {
  const binding = d1DatabaseLike(openSqlite()) as never;
  const storage = createD1Storage({
    binding,
    domains: createBackgroundTaskD1Domains({ binding, tenantId }),
  });
  await storage.init();
  const pubsub = createHostPubSub();
  const mastra = new Mastra({ storage, pubsub });
  return { mastra, pubsub };
}

describe('BackgroundTaskHost — wiring', () => {
  it.each([
    { globalConcurrency: -1 },
    { perAgentConcurrency: 1.5 },
    { defaultTimeoutMs: 0 },
    { progressThrottleMs: Number.NaN },
    { waitTimeoutMs: Number.POSITIVE_INFINITY },
    { defaultRetries: { maxRetries: Number.MAX_SAFE_INTEGER + 1 } },
    { defaultRetries: { retryDelayMs: -1 } },
    { defaultRetries: { maxRetryDelayMs: 0.5 } },
    { defaultRetries: { backoffMultiplier: -0.1 } },
    { cleanup: { completedTtlMs: -1 } },
    { cleanup: { failedTtlMs: Number.NaN } },
    { cleanup: { cleanupIntervalMs: 0 } },
  ])('rejects invalid manager configuration synchronously: %o', (manager) => {
    expect(
      () =>
        new BackgroundTaskHost({
          mastra: new Mastra({ storage: new InMemoryStore() }),
          pubsub: createHostPubSub(),
          executors: {},
          manager,
        }),
    ).toThrow(RangeError);
  });

  it('accepts deliberate zero concurrency, retry, TTL, and throttle values', () => {
    expect(
      () =>
        new BackgroundTaskHost({
          mastra: new Mastra({ storage: new InMemoryStore() }),
          pubsub: createHostPubSub(),
          executors: {},
          manager: {
            globalConcurrency: 0,
            perAgentConcurrency: 0,
            progressThrottleMs: 0,
            defaultRetries: {
              maxRetries: 0,
              retryDelayMs: 0,
              maxRetryDelayMs: 0,
              backoffMultiplier: 0,
            },
            cleanup: { completedTtlMs: 0, failedTtlMs: 0 },
          },
        }),
    ).not.toThrow();
  });

  it('boot() re-registers the static executors (survives DO eviction, DL-015)', async () => {
    // #given
    const executor: ToolExecutor = { execute: async () => ({ done: true }) };
    const host = new BackgroundTaskHost({
      mastra: new Mastra({ storage: new InMemoryStore() }),
      pubsub: createHostPubSub(),
      executors: { longResearch: executor },
    });

    // #when
    await host.boot();

    // #then — resolvable by name (the path a recovered task's step takes)
    expect(host.manager.getStaticExecutor('longResearch')).toBe(executor);
  });

  it('boot() is idempotent — a second call resolves without re-init', async () => {
    // #given
    const host = new BackgroundTaskHost({
      mastra: new Mastra({ storage: new InMemoryStore() }),
      pubsub: createHostPubSub(),
      executors: {},
    });

    // #when / #then
    await expect(host.boot()).resolves.toBeUndefined();
    await expect(host.boot()).resolves.toBeUndefined();
  });

  it('the recovery SEAM fires via init(): a stranded running task (maxRetries 0) becomes failed, on a concurrent-update store too', async () => {
    // #given — an InMemoryStore carrying a task left 'running' by an evicted
    // instance, no retry budget (so recovery resolves it at the storage layer)
    const storage = new InMemoryStore();
    const mastra = new Mastra({ storage });
    const store = await backgroundTasksStore(mastra);
    await store.createTask(baseTask({ id: 'stranded', maxRetries: 0 }));
    const host = new BackgroundTaskHost({
      mastra,
      pubsub: createHostPubSub(),
      executors: {},
    });

    // #when — a fresh instance boots
    await host.boot();

    // #then — the PUBLIC init() seam fired recoverStaleTasks and marked it failed
    const recovered = await store.getTask('stranded');
    expect(recovered?.status).toBe('failed');
    expect(recovered?.error?.message).toMatch(/terminated/i);
  });

  it('the recovery seam RE-DRIVES a stranded running task WITH retry budget: maxRetries>0 becomes pending, not failed', async () => {
    // #given — a task left 'running' WITH retry budget, so recovery re-queues it
    // (the maxRetries>0 branch, distinct from the maxRetries=0 -> failed branch).
    // globalConcurrency:0 makes checkConcurrency refuse the re-dispatch, freezing
    // the task at the pending hand-off so the transition is observable WITHOUT the
    // R-B2-blocked execution re-dispatching it straight back to running.
    const storage = new InMemoryStore();
    const mastra = new Mastra({ storage });
    const store = await backgroundTasksStore(mastra);
    await store.createTask(baseTask({ id: 'retryable', maxRetries: 3 }));
    const host = new BackgroundTaskHost({
      mastra,
      pubsub: createHostPubSub(),
      executors: {},
      manager: { globalConcurrency: 0 },
    });

    // #when — a fresh instance boots
    await host.boot();

    // #then — re-queued to pending via the public init() seam (NOT failed: it had
    // budget), proving the re-drive branch, not only the give-up branch
    const recovered = await store.getTask('retryable');
    expect(recovered?.status).toBe('pending');
  });

  it('onAlarm() boots then runs the manager cleanup (reaps a stale completed row)', async () => {
    // #given — a completed task older than the configured 1h TTL
    const storage = new InMemoryStore();
    const mastra = new Mastra({ storage });
    const store = await backgroundTasksStore(mastra);
    const old = new Date(Date.now() - 2 * 3_600_000);
    await store.createTask(
      baseTask({
        id: 'old-complete',
        status: 'completed',
        completedAt: old,
        startedAt: old,
        createdAt: old,
      }),
    );
    const host = new BackgroundTaskHost({
      mastra,
      pubsub: createHostPubSub(),
      executors: {},
      manager: { cleanup: { completedTtlMs: 3_600_000 } },
    });

    // #when
    await host.onAlarm();

    // #then
    expect(await store.getTask('old-complete')).toBeNull();
  });
});

describe('BackgroundTaskHost — execution lifecycle', () => {
  it('unwinds a partially failed worker start without initializing or shutting down the manager', async () => {
    // #given
    const { mastra, pubsub } = await executionHostDependencies();
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      execution: { tenantId: 'acme' },
      executors: {},
    });
    const primary = new Error('partial worker start');
    const calls: string[] = [];
    vi.spyOn(mastra, 'startWorkers').mockImplementation(async () => {
      calls.push('start-workers');
      throw primary;
    });
    vi.spyOn(mastra, 'stopWorkers').mockImplementation(async () => {
      calls.push('stop-workers');
    });
    const init = vi.spyOn(host.manager, 'init');
    const managerShutdown = vi.spyOn(host.manager, 'shutdown');

    // #when / #then
    await expect(host.boot()).rejects.toBe(primary);
    expect(calls).toEqual(['start-workers', 'stop-workers']);
    expect(init).not.toHaveBeenCalled();
    expect(managerShutdown).not.toHaveBeenCalled();

    await expect(host.shutdown()).resolves.toBeUndefined();
    expect(calls).toEqual(['start-workers', 'stop-workers']);
    expect(managerShutdown).not.toHaveBeenCalled();
  });

  it('unwinds a failed manager init in reverse order and preserves the primary error', async () => {
    // #given
    const { mastra, pubsub } = await executionHostDependencies();
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      execution: { tenantId: 'acme' },
      executors: {},
    });
    const primary = new Error('manager init');
    const calls: string[] = [];
    vi.spyOn(mastra, 'startWorkers').mockImplementation(async () => {
      calls.push('start-workers');
    });
    vi.spyOn(host.manager, 'init').mockImplementation(async () => {
      calls.push('manager-init');
      throw primary;
    });
    vi.spyOn(host.manager, 'shutdown').mockImplementation(async () => {
      calls.push('manager-shutdown');
    });
    vi.spyOn(mastra, 'stopWorkers').mockImplementation(async () => {
      calls.push('stop-workers');
    });

    // #when / #then
    await expect(host.boot()).rejects.toBe(primary);
    expect(calls).toEqual([
      'start-workers',
      'manager-init',
      'manager-shutdown',
      'stop-workers',
    ]);
  });

  it('aggregates boot cleanup failures after the primary error in cleanup order', async () => {
    // #given
    const { mastra, pubsub } = await executionHostDependencies();
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      execution: { tenantId: 'acme' },
      executors: {},
    });
    const primary = new Error('manager init');
    const managerCleanup = new Error('manager cleanup');
    const workerCleanup = new Error('worker cleanup');
    const calls: string[] = [];
    vi.spyOn(mastra, 'startWorkers').mockImplementation(async () => {
      calls.push('start-workers');
    });
    vi.spyOn(host.manager, 'init').mockImplementation(async () => {
      calls.push('manager-init');
      throw primary;
    });
    vi.spyOn(host.manager, 'shutdown').mockImplementation(async () => {
      calls.push('manager-shutdown');
      throw managerCleanup;
    });
    vi.spyOn(mastra, 'stopWorkers').mockImplementation(async () => {
      calls.push('stop-workers');
      throw workerCleanup;
    });

    // #when
    const failure = await host.boot().catch((error: unknown) => error);

    // #then
    expect(failure).toBeInstanceOf(AggregateError);
    expect((failure as AggregateError).errors).toEqual([
      primary,
      managerCleanup,
      workerCleanup,
    ]);
    expect((failure as Error).cause).toBe(primary);
    expect(calls).toEqual([
      'start-workers',
      'manager-init',
      'manager-shutdown',
      'stop-workers',
    ]);
  });

  it('shuts the manager down before stopping workers', async () => {
    // #given
    const { mastra, pubsub } = await executionHostDependencies();
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      execution: { tenantId: 'acme' },
      executors: {},
    });
    vi.spyOn(mastra, 'startWorkers').mockResolvedValue();
    vi.spyOn(host.manager, 'init').mockResolvedValue();
    await host.boot();
    const calls: string[] = [];
    vi.spyOn(host.manager, 'shutdown').mockImplementation(async () => {
      calls.push('manager-shutdown');
    });
    vi.spyOn(mastra, 'stopWorkers').mockImplementation(async () => {
      calls.push('stop-workers');
    });

    // #when
    await host.shutdown();

    // #then
    expect(calls).toEqual(['manager-shutdown', 'stop-workers']);
  });

  it('closes the enqueue gate synchronously once boot has settled', async () => {
    // #given
    const storage = new InMemoryStore();
    const pubsub = createHostPubSub();
    const host = new BackgroundTaskHost({
      mastra: new Mastra({ storage, pubsub }),
      pubsub,
      executors: {},
    });
    await host.boot();

    // #when — do not await shutdown before racing a new enqueue against it.
    const shutdown = host.shutdown();
    const enqueue = host.manager.enqueue({
      runId: 'acme_r1',
      toolName: 'late',
      toolCallId: 'call-late',
      args: {},
      agentId: 'agent-1',
    });

    // #then — BackgroundTaskManager.shutdown flips its guard before its first
    // await, so the task cannot persist or publish after consumers disappear.
    await expect(enqueue).rejects.toThrow(/shutting down/i);
    await expect(shutdown).resolves.toBeUndefined();
    const tasks = await storage.getStore('backgroundTasks');
    await expect(tasks?.listTasks({ runId: 'acme_r1' })).resolves.toMatchObject(
      {
        tasks: [],
      },
    );
  });

  it('treats shutdown as terminal even when boot was never started', async () => {
    const host = new BackgroundTaskHost({
      mastra: new Mastra({ storage: new InMemoryStore() }),
      pubsub: createHostPubSub(),
      executors: {},
    });

    await host.shutdown();

    await expect(host.boot()).rejects.toThrow(/shutting down/i);
  });

  it('waits for an in-flight boot before tearing its initialized components down', async () => {
    // #given
    const { mastra, pubsub } = await executionHostDependencies();
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      execution: { tenantId: 'acme' },
      executors: {},
    });
    let releaseStart: () => void = () => undefined;
    const startReleased = new Promise<void>((resolve) => {
      releaseStart = resolve;
    });
    let signalStart: () => void = () => undefined;
    const startEntered = new Promise<void>((resolve) => {
      signalStart = resolve;
    });
    const calls: string[] = [];
    vi.spyOn(mastra, 'startWorkers').mockImplementation(async () => {
      calls.push('start-workers');
      signalStart();
      await startReleased;
    });
    vi.spyOn(host.manager, 'init').mockImplementation(async () => {
      calls.push('manager-init');
    });
    vi.spyOn(host.manager, 'shutdown').mockImplementation(async () => {
      calls.push('manager-shutdown');
    });
    vi.spyOn(mastra, 'stopWorkers').mockImplementation(async () => {
      calls.push('stop-workers');
    });

    // #when
    const boot = host.boot();
    const shutdown = host.shutdown();
    await startEntered;
    expect(calls).toEqual(['start-workers']);
    releaseStart();
    await Promise.all([boot, shutdown]);

    // #then
    expect(calls).toEqual([
      'start-workers',
      'manager-init',
      'manager-shutdown',
      'stop-workers',
    ]);
  });

  it('still stops workers and preserves a lone manager shutdown failure', async () => {
    // #given
    const { mastra, pubsub } = await executionHostDependencies();
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      execution: { tenantId: 'acme' },
      executors: {},
    });
    vi.spyOn(mastra, 'startWorkers').mockResolvedValue();
    vi.spyOn(host.manager, 'init').mockResolvedValue();
    await host.boot();
    const primary = new Error('manager shutdown');
    const calls: string[] = [];
    vi.spyOn(host.manager, 'shutdown').mockImplementation(async () => {
      calls.push('manager-shutdown');
      throw primary;
    });
    vi.spyOn(mastra, 'stopWorkers').mockImplementation(async () => {
      calls.push('stop-workers');
    });

    // #when / #then
    await expect(host.shutdown()).rejects.toBe(primary);
    expect(calls).toEqual(['manager-shutdown', 'stop-workers']);
  });

  it('aggregates manager and worker shutdown failures in operation order', async () => {
    // #given
    const { mastra, pubsub } = await executionHostDependencies();
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      execution: { tenantId: 'acme' },
      executors: {},
    });
    vi.spyOn(mastra, 'startWorkers').mockResolvedValue();
    vi.spyOn(host.manager, 'init').mockResolvedValue();
    await host.boot();
    const primary = new Error('manager shutdown');
    const workerCleanup = new Error('worker shutdown');
    vi.spyOn(host.manager, 'shutdown').mockRejectedValue(primary);
    vi.spyOn(mastra, 'stopWorkers').mockRejectedValue(workerCleanup);

    // #when
    const failure = await host.shutdown().catch((error: unknown) => error);

    // #then
    expect(failure).toBeInstanceOf(AggregateError);
    expect((failure as AggregateError).errors).toEqual([
      primary,
      workerCleanup,
    ]);
    expect((failure as Error).cause).toBe(primary);
  });
});

describe('BackgroundTaskHost — on D1 (R-B1: persistence + recovery seam, no body execution)', () => {
  let storage: MastraCompositeStore;

  beforeEach(async () => {
    storage = await seededD1();
  });

  it('warns once at boot that dispatched bodies cannot execute on a non-concurrent store', async () => {
    // #given
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const host = new BackgroundTaskHost({
      mastra: new Mastra({ storage }),
      pubsub: createHostPubSub(),
      executors: {},
    });

    // #when
    await host.boot();

    // #then — the R-B1 limitation is loud, not a stray async throw at dispatch
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('concurrent updates'),
    );
    warn.mockRestore();
  });

  it('the recovery seam transitions a stranded running task on the REAL D1 domain, surviving the "eviction"', async () => {
    // #given — a task the evicted instance left 'running', no retry budget
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const mastra = new Mastra({ storage });
    const store = await backgroundTasksStore(mastra);
    await store.createTask(baseTask({ id: 'stranded', maxRetries: 0 }));
    const host = new BackgroundTaskHost({
      mastra,
      pubsub: createHostPubSub(),
      executors: {},
    });

    // #when — a fresh instance boots
    await host.boot();

    // #then — recovered at the storage layer via the public init() seam
    const recovered = await store.getTask('stranded');
    expect(recovered?.status).toBe('failed');
    expect(recovered?.error?.message).toMatch(/terminated/i);
    warn.mockRestore();
  });

  it('the D1 backgroundTasks persistence domain round-trips a task (create/get/list)', async () => {
    // #given
    const mastra = new Mastra({ storage });
    const store = await backgroundTasksStore(mastra);

    // #when
    await store.createTask(baseTask({ id: 'persisted', status: 'suspended' }));

    // #then — durable persistence works on D1 regardless of execution (R-B1)
    const got = await store.getTask('persisted');
    expect(got?.id).toBe('persisted');
    expect(got?.status).toBe('suspended');
    const listed = await store.listTasks({ runId: 'abc_r1' });
    expect(listed.tasks.map((t) => t.id)).toContain('persisted');
  });
});

describe('BackgroundTaskHost — execution-mode recovery on D1', () => {
  it('subscribes workflow workers before recovery re-dispatches a retryable task', async () => {
    const binding = d1DatabaseLike(openSqlite()) as never;
    const seedStorage = createD1Storage({
      binding,
      domains: createBackgroundTaskD1Domains({
        binding,
        tenantId: 'acme',
      }),
    });
    await seedStorage.init();
    const seedStore = await backgroundTasksStore(
      new Mastra({ storage: seedStorage }),
    );
    await seedStore.createTask(
      baseTask({
        id: 'retryable-execution',
        runId: 'acme_r1',
        maxRetries: 1,
      }),
    );

    const storage = createD1Storage({
      binding,
      domains: createBackgroundTaskD1Domains({
        binding,
        tenantId: 'acme',
      }),
    });
    await storage.init();
    const pubsub = createHostPubSub();
    const mastra = new Mastra({ storage, pubsub });
    const execute = vi.fn(async () => ({ recovered: true }));
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      execution: { tenantId: 'acme' },
      executors: { longResearch: { execute } },
    });

    let booted = false;
    try {
      expect(mastra.pubsub).not.toBe(pubsub);
      await host.boot();
      booted = true;
      await vi.waitFor(
        async () => {
          expect(
            (await host.manager.getTask('retryable-execution'))?.status,
          ).toBe('completed');
        },
        { timeout: 5_000, interval: 10 },
      );
      expect(execute).toHaveBeenCalledTimes(1);
    } finally {
      if (booted) await host.shutdown();
    }
  });
});
