// SPDX-License-Identifier: Apache-2.0
// Track B host wiring + the B-S2 recovery seam (R-002 pin: the PUBLIC async
// init(pubsub) fires recoverStaleTasks internally — no private method is
// called). Execution-mode recovery is proven over the serialized, deployment-bound
// D1 domains: workers subscribe before init publishes the recovered dispatch,
// and a static executor takes the stranded task through to completion.

import type { ToolExecutor } from '@mastra/core/background-tasks';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryStore, type MastraCompositeStore } from '@mastra/core/storage';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  createD1Storage,
  createHostPubSub,
  type ExecutionFenceDatabase,
  ExecutionFencedError,
  type ExecutionFenceState,
  ExecutionFenceStore,
  init,
} from '../do-runner/index.js';
import {
  backgroundTasksStore,
  createBackgroundTaskD1Domains,
} from './d1-storage.js';
import {
  BackgroundTaskHost,
  type BackgroundTaskHostOptions,
  backgroundTaskManagerForTests,
  FENCE_RESUME_SCAN_PAGE,
  MAX_FENCE_RESUME_SCANS,
  MAX_FENCE_RESUMES_PER_PASS,
} from './host.js';

/**
 * The host under test with the fence defaulted. `'none'` is the honest wiring
 * for the InMemoryStore / unit-sqlite hosts these cases build — the fence cases
 * at the bottom of this file pass a real store, and pass it through this same
 * helper, so an explicit fence always wins.
 */
function newBackgroundTaskHost(
  options: Omit<BackgroundTaskHostOptions, 'executionFence'> &
    Partial<Pick<BackgroundTaskHostOptions, 'executionFence'>>,
): BackgroundTaskHost {
  return new BackgroundTaskHost({
    ...options,
    executionFence: options.executionFence ?? 'none',
  });
}

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
  const storage = createD1Storage({
    binding: sqliteUnitDatabase(sqlite) as never,
  });
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    { startIdempotency: 'none', executionFence: 'none' },
  );
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

async function executionHostDependencies() {
  const binding = sqliteUnitDatabase(openSqlite()) as never;
  const storage = createD1Storage({
    binding,
    domains: createBackgroundTaskD1Domains({ binding }),
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
    expect(() =>
      newBackgroundTaskHost({
        mastra: new Mastra({ storage: new InMemoryStore() }),
        pubsub: createHostPubSub(),
        executors: {},
        manager,
      }),
    ).toThrow(RangeError);
  });

  it('accepts deliberate zero concurrency, retry, TTL, and throttle values', () => {
    expect(() =>
      newBackgroundTaskHost({
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
    const executed: unknown[] = [];
    const executor: ToolExecutor = {
      execute: async (args) => {
        executed.push(args);
        return { done: true };
      },
    };
    const host = newBackgroundTaskHost({
      mastra: new Mastra({ storage: new InMemoryStore() }),
      pubsub: createHostPubSub(),
      executors: { longResearch: executor },
    });

    // #when
    await host.boot();

    // #then — resolvable by name (the path a recovered task's step takes), and
    // it DELEGATES to the registered executor. Identity is deliberately not
    // asserted: what is registered is the fence-gated wrapper (host.ts
    // `#gated`), which is the seam that stops a locked deployment executing a
    // task body — including one a recovery re-drive resolved by name.
    const registered =
      backgroundTaskManagerForTests(host).getStaticExecutor('longResearch');
    expect(registered).toBeDefined();
    await expect(registered?.execute({ topic: 'ai' })).resolves.toEqual({
      done: true,
    });
    expect(executed).toEqual([{ topic: 'ai' }]);
  });

  it('boot() is idempotent — a second call resolves without re-init', async () => {
    // #given
    const host = newBackgroundTaskHost({
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
    const host = newBackgroundTaskHost({
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
    const host = newBackgroundTaskHost({
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
    const host = newBackgroundTaskHost({
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
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
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
    const init = vi.spyOn(backgroundTaskManagerForTests(host), 'init');
    const managerShutdown = vi.spyOn(
      backgroundTaskManagerForTests(host),
      'shutdown',
    );

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
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
      executors: {},
    });
    const primary = new Error('manager init');
    const calls: string[] = [];
    vi.spyOn(mastra, 'startWorkers').mockImplementation(async () => {
      calls.push('start-workers');
    });
    vi.spyOn(backgroundTaskManagerForTests(host), 'init').mockImplementation(
      async () => {
        calls.push('manager-init');
        throw primary;
      },
    );
    vi.spyOn(
      backgroundTaskManagerForTests(host),
      'shutdown',
    ).mockImplementation(async () => {
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
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
      executors: {},
    });
    const primary = new Error('manager init');
    const managerCleanup = new Error('manager cleanup');
    const workerCleanup = new Error('worker cleanup');
    const calls: string[] = [];
    vi.spyOn(mastra, 'startWorkers').mockImplementation(async () => {
      calls.push('start-workers');
    });
    vi.spyOn(backgroundTaskManagerForTests(host), 'init').mockImplementation(
      async () => {
        calls.push('manager-init');
        throw primary;
      },
    );
    vi.spyOn(
      backgroundTaskManagerForTests(host),
      'shutdown',
    ).mockImplementation(async () => {
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
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
      executors: {},
    });
    vi.spyOn(mastra, 'startWorkers').mockResolvedValue();
    vi.spyOn(backgroundTaskManagerForTests(host), 'init').mockResolvedValue();
    await host.boot();
    const calls: string[] = [];
    vi.spyOn(
      backgroundTaskManagerForTests(host),
      'shutdown',
    ).mockImplementation(async () => {
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
    const host = newBackgroundTaskHost({
      mastra: new Mastra({ storage, pubsub }),
      pubsub,
      executors: {},
    });
    await host.boot();

    // #when — do not await shutdown before racing a new enqueue against it.
    const shutdown = host.shutdown();
    const enqueue = host.enqueue({
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

  it('retries a registration that failed on a transient storage fault', async () => {
    // #given — a host whose first registration cannot reach storage. The read
    // routes hang off this memo, so caching that answer would keep the whole
    // host answering for a queue it can already reach again, until an eviction
    // nobody can schedule.
    const storage = new InMemoryStore();
    const mastra = new Mastra({ storage });
    const executor: ToolExecutor = { execute: async () => ({ done: true }) };
    let reads = 0;
    vi.spyOn(mastra, 'getStorage').mockImplementation(() => {
      reads += 1;
      if (reads === 1) throw new Error('D1_ERROR: storage unavailable');
      return storage as never;
    });
    const host = newBackgroundTaskHost({
      mastra,
      pubsub: createHostPubSub(),
      executors: { longResearch: executor },
    });

    // #when — the first boot fails on that read.
    await expect(host.boot()).rejects.toThrow(/storage unavailable/);

    // #then — the next boot registers for real rather than replaying the
    // rejection: the executors a recovered task resolves by name are there.
    await expect(host.boot()).resolves.toBeUndefined();
    expect(
      backgroundTaskManagerForTests(host).getStaticExecutor('longResearch'),
    ).toBeDefined();
  });

  it('keeps a deterministic configuration failure memoized', async () => {
    // #given — execution mode over a store that is not the serialized D1
    // workflows domain. Nothing about that answer can change without a new
    // host, so re-validating it on every boot would only re-read storage to
    // reach the same refusal.
    const storage = new InMemoryStore();
    const mastra = new Mastra({ storage });
    const host = newBackgroundTaskHost({
      mastra,
      pubsub: createHostPubSub(),
      execution: true,
      executors: {},
    });
    await expect(host.boot()).rejects.toThrow(
      /requires DurableObjectWorkflowsStorageD1/,
    );

    // #when — a second boot, watched from here so only the RETRY's reads count.
    const getStorage = vi.spyOn(mastra, 'getStorage');
    await expect(host.boot()).rejects.toThrow(
      /requires DurableObjectWorkflowsStorageD1/,
    );

    // #then — the same refusal, served from the memo without touching storage.
    expect(getStorage).not.toHaveBeenCalled();
  });

  it('treats shutdown as terminal even when boot was never started', async () => {
    const host = newBackgroundTaskHost({
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
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
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
    vi.spyOn(backgroundTaskManagerForTests(host), 'init').mockImplementation(
      async () => {
        calls.push('manager-init');
      },
    );
    vi.spyOn(
      backgroundTaskManagerForTests(host),
      'shutdown',
    ).mockImplementation(async () => {
      calls.push('manager-shutdown');
    });
    vi.spyOn(mastra, 'stopWorkers').mockImplementation(async () => {
      calls.push('stop-workers');
    });

    // #when — the shutdown arrives once boot is already PAST admission and
    // inside startWorkers. (A shutdown that lands before admission is the
    // separate fence-read race below, where boot starts nothing at all.)
    const boot = host.boot();
    await startEntered;
    const shutdown = host.shutdown();
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
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
      executors: {},
    });
    vi.spyOn(mastra, 'startWorkers').mockResolvedValue();
    vi.spyOn(backgroundTaskManagerForTests(host), 'init').mockResolvedValue();
    await host.boot();
    const primary = new Error('manager shutdown');
    const calls: string[] = [];
    vi.spyOn(
      backgroundTaskManagerForTests(host),
      'shutdown',
    ).mockImplementation(async () => {
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
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
      executors: {},
    });
    vi.spyOn(mastra, 'startWorkers').mockResolvedValue();
    vi.spyOn(backgroundTaskManagerForTests(host), 'init').mockResolvedValue();
    await host.boot();
    const primary = new Error('manager shutdown');
    const workerCleanup = new Error('worker shutdown');
    vi.spyOn(backgroundTaskManagerForTests(host), 'shutdown').mockRejectedValue(
      primary,
    );
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
    const host = newBackgroundTaskHost({
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
    const host = newBackgroundTaskHost({
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
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    const seedStorage = createD1Storage({
      binding,
      domains: createBackgroundTaskD1Domains({
        binding,
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
      }),
    });
    await storage.init();
    const pubsub = createHostPubSub();
    const mastra = new Mastra({ storage, pubsub });
    const execute = vi.fn(async () => ({ recovered: true }));
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
      executors: { longResearch: { execute } },
    });

    let booted = false;
    try {
      expect(mastra.pubsub).not.toBe(pubsub);
      await host.boot();
      booted = true;
      await vi.waitFor(
        async () => {
          expect((await host.getTask('retryable-execution'))?.status).toBe(
            'completed',
          );
        },
        { timeout: 5_000, interval: 10 },
      );
      expect(execute).toHaveBeenCalledTimes(1);
    } finally {
      if (booted) await host.shutdown();
    }
  });
});

describe('BackgroundTaskHost and the deployment execution fence', () => {
  async function fenceAt(
    state: ExecutionFenceState,
  ): Promise<ExecutionFenceStore> {
    const fence = new ExecutionFenceStore(
      sqliteUnitDatabase(openSqlite()) as ExecutionFenceDatabase,
    );
    await fence.seed(state);
    return fence;
  }

  function hostWith(
    executionFence: ExecutionFenceStore,
    executor: ToolExecutor,
  ): BackgroundTaskHost {
    return newBackgroundTaskHost({
      mastra: new Mastra({ storage: new InMemoryStore() }),
      pubsub: createHostPubSub(),
      executors: { longResearch: executor },
      executionFence,
    });
  }

  it('starts nothing when shutdown arrives during the fence read', async () => {
    // #given — the window the fence read opens: `boot()` is suspended reading
    // the fence, so it is past `boot()`'s own shutdown guard but has not
    // admitted dispatching yet. Subscribing and claiming from here would hand
    // work to a manager that is already being torn down — and `#doShutdown`
    // waits on this very attempt, so it would be waiting for the claim it is
    // trying to prevent.
    const { mastra, pubsub } = await executionHostDependencies();
    let releaseFence: () => void = () => undefined;
    const fenceRead = new Promise<void>((resolve) => {
      releaseFence = resolve;
    });
    const open = await fenceAt('open');
    const slowFence = {
      read: async () => {
        await fenceRead;
        return open.read();
      },
    } as unknown as ExecutionFenceStore;
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
      executors: {},
      executionFence: slowFence,
    });
    const calls: string[] = [];
    vi.spyOn(mastra, 'startWorkers').mockImplementation(async () => {
      calls.push('start-workers');
    });
    vi.spyOn(backgroundTaskManagerForTests(host), 'init').mockImplementation(
      async () => {
        calls.push('manager-init');
      },
    );
    vi.spyOn(
      backgroundTaskManagerForTests(host),
      'shutdown',
    ).mockImplementation(async () => {
      calls.push('manager-shutdown');
    });
    vi.spyOn(mastra, 'stopWorkers').mockImplementation(async () => {
      calls.push('stop-workers');
    });

    // #when — shutdown arrives while the fence read is still outstanding.
    const boot = host.boot();
    const shutdown = host.shutdown();
    expect(calls).toEqual([]);
    releaseFence();
    await Promise.all([boot, shutdown]);

    // #then — the boot re-checks after the read and bails: nothing was started,
    // so there is nothing to stop and nothing left running behind the teardown.
    expect(calls).toEqual([]);
  });

  it('abandons the alarm duty when shutdown arrives during its boot', async () => {
    // #given — the alarm's own boot, held inside the fence read. `onAlarm`
    // sweeps and then runs core's TTL cleanup, and both would land on a manager
    // that is already being torn down.
    const { mastra, pubsub } = await executionHostDependencies();
    let releaseFence: () => void = () => undefined;
    const fenceRead = new Promise<void>((resolve) => {
      releaseFence = resolve;
    });
    const open = await fenceAt('open');
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      executors: {},
      executionFence: {
        read: async () => {
          await fenceRead;
          return open.read();
        },
      } as unknown as ExecutionFenceStore,
    });
    const cleanup = vi
      .spyOn(backgroundTaskManagerForTests(host), 'cleanup')
      .mockResolvedValue(undefined as never);

    // #when — teardown is requested while the alarm's boot is still reading.
    const alarm = host.onAlarm();
    const shutdown = host.shutdown();
    releaseFence();
    await Promise.all([alarm, shutdown]);

    // #then — the duty is dropped rather than run against a shut-down manager.
    expect(cleanup).not.toHaveBeenCalled();
  });

  it('starts the dispatcher ONCE when two boots race the fence read', async () => {
    // #given — the request path and onAlarm() call boot() independently, so
    // two attempts genuinely interleave. Both are held inside the fence read,
    // which is the window `#dispatching` cannot cover: it is assigned only once
    // that read RESUMES.
    const { mastra, pubsub } = await executionHostDependencies();
    let releaseFence: () => void = () => undefined;
    const fenceRead = new Promise<void>((resolve) => {
      releaseFence = resolve;
    });
    const open = await fenceAt('open');
    const slowFence = {
      read: async () => {
        await fenceRead;
        return open.read();
      },
    } as unknown as ExecutionFenceStore;
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      execution: true,
      executors: {},
      executionFence: slowFence,
    });
    const startWorkers = vi
      .spyOn(mastra, 'startWorkers')
      .mockImplementation(async () => undefined);
    const init = vi
      .spyOn(backgroundTaskManagerForTests(host), 'init')
      .mockImplementation(async () => undefined);

    // #when — both boots pass the `#dispatching` guard before either read ends.
    const first = host.boot();
    const second = host.boot();
    releaseFence();
    await Promise.all([first, second]);

    // #then — ONE dispatcher. A second manager.init subscribes handleDispatch a
    // second time, and every dispatch is then handled twice: double claim,
    // double body execution — with `#managerNeedsShutdown`/`#workersNeedStop`
    // being booleans, teardown would stop only one of the two.
    expect(init).toHaveBeenCalledTimes(1);
    expect(startWorkers).toHaveBeenCalledTimes(1);

    // #and — a later boot still rides the settled memo rather than re-starting.
    await host.boot();
    expect(init).toHaveBeenCalledTimes(1);
    expect(startWorkers).toHaveBeenCalledTimes(1);
  });

  /**
   * Seed `count` suspended rows and make `resume()` do what core's handler
   * does to the row — leave the suspended set — so a sweep that re-reads it
   * sees the truth rather than a stub that never moves.
   */
  async function parkedHost(
    tasks: ReadonlyArray<{ id: string; suspendedAt: Date; parked: boolean }>,
  ) {
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    const storage = createD1Storage({
      binding,
      domains: createBackgroundTaskD1Domains({ binding }),
    });
    await storage.init();
    const pubsub = createHostPubSub();
    const mastra = new Mastra({ storage, pubsub });
    const store = await backgroundTasksStore(mastra);
    for (const task of tasks) {
      await store.createTask(
        baseTask({
          id: task.id,
          status: 'suspended',
          suspendedAt: task.suspendedAt,
          suspendPayload: task.parked
            ? { 'flowsafe.executionFenced': { state: 'migration-locked' } }
            : { awaiting: 'webhook' },
        }),
      );
    }
    const host = newBackgroundTaskHost({
      mastra,
      pubsub,
      executors: {},
      executionFence: await fenceAt('open'),
    });
    const resume = vi
      .spyOn(backgroundTaskManagerForTests(host), 'resume')
      .mockImplementation(async (taskId: string) => {
        await store.updateTask(taskId, { status: 'running' });
        return (await store.getTask(taskId)) as never;
      });
    const stillSuspended = async () =>
      (await store.listTasks({ status: 'suspended' })).tasks.map((t) => t.id);
    return { host, resume, stillSuspended };
  }

  it('drains a parked cohort larger than the boot budget, without eviction', async () => {
    // #given — 30 tasks parked by the backstop, more than one boot budget.
    const parked = Array.from({ length: 30 }, (_, index) => ({
      id: `parked-${String(index).padStart(2, '0')}`,
      suspendedAt: new Date(1_800_000_000_000 + index * 1_000),
      parked: true,
    }));
    const { host, resume, stillSuspended } = await parkedHost(parked);

    try {
      // #when — the boot after reopening.
      await host.boot();

      // #then — exactly one boot budget. The rest are NOT dropped: still
      // suspended, still marked, still inspectable.
      expect(resume).toHaveBeenCalledTimes(MAX_FENCE_RESUMES_PER_PASS);
      expect(await stillSuspended()).toHaveLength(
        30 - MAX_FENCE_RESUMES_PER_PASS,
      );

      // #when — the alarm the host already arms for cleanup fires. THIS is the
      // lane convergence rides: nothing waits behind it, so it carries the
      // larger budget and keeps scanning until a scan resumes nothing.
      await host.onAlarm();

      // #then — drained. No eviction, no operator action, no fresh instance:
      // the earlier shape swept only from #startDispatching, so every later
      // boot returned the settled memo and these five stayed parked forever.
      expect(resume).toHaveBeenCalledTimes(30);
      expect(await stillSuspended()).toEqual([]);
    } finally {
      await host.shutdown();
    }
  });

  it('reaches a stale parked cohort sitting behind a full page of newer suspensions', async () => {
    // #given — the cross-cycle case: five rows parked by an EARLIER lock, then
    // more than a full scan page of newer tool-suspends (a tool awaiting a
    // webhook is suspended on purpose and must stay that way). Scanning newest
    // first, page 0 is all tool-suspends, so an unpaged sweep would never see
    // the old cohort again.
    const stale = Array.from({ length: 5 }, (_, index) => ({
      id: `stale-${index}`,
      suspendedAt: new Date(1_700_000_000_000 + index * 1_000),
      parked: true,
    }));
    const newer = Array.from({ length: 120 }, (_, index) => ({
      id: `tool-${String(index).padStart(3, '0')}`,
      suspendedAt: new Date(1_900_000_000_000 + index * 1_000),
      parked: false,
    }));
    const { host, resume, stillSuspended } = await parkedHost([
      ...stale,
      ...newer,
    ]);

    try {
      // #when
      await host.boot();

      // #then — every stale row reached, and not one tool-suspended row
      // touched.
      expect(resume).toHaveBeenCalledTimes(5);
      expect(resume.mock.calls.map((call) => call[0] as string).sort()).toEqual(
        stale.map((task) => task.id),
      );
      expect((await stillSuspended()).sort()).toEqual(
        newer.map((task) => task.id).sort(),
      );
    } finally {
      await host.shutdown();
    }
  });

  it("reaches a cohort past one wake's scan bound on the NEXT alarm", async () => {
    // #given — the stranding case paging alone cannot fix: a parked cohort
    // sitting behind more newer tool-suspends than one wake can page through
    // (MAX_FENCE_RESUME_SCANS * FENCE_RESUME_SCAN_PAGE rows). Every wake used
    // to restart at page 0, so every wake re-read the same marker-free prefix
    // and stopped in the same place — the cohort was unreachable forever.
    //
    // The set is served through a stubbed `listTasks` rather than seeded into
    // D1: the point under test is which PAGES successive wakes ask for, and
    // materializing thousands of rows to observe that would only make the test
    // slower to lie in the same way.
    const beyondOneWake = MAX_FENCE_RESUME_SCANS * FENCE_RESUME_SCAN_PAGE;
    const remaining = [
      ...Array.from({ length: beyondOneWake }, (_, index) =>
        baseTask({
          id: `tool-${String(index).padStart(4, '0')}`,
          status: 'suspended',
          suspendedAt: new Date(1_900_000_000_000 + index),
          suspendPayload: { awaiting: 'webhook' },
        }),
      ),
      ...Array.from({ length: 3 }, (_, index) =>
        baseTask({
          id: `parked-${index}`,
          status: 'suspended',
          suspendedAt: new Date(1_700_000_000_000 + index),
          suspendPayload: {
            'flowsafe.executionFenced': { state: 'migration-locked' },
          },
        }),
      ),
    ];
    const host = newBackgroundTaskHost({
      mastra: new Mastra({ storage: new InMemoryStore() }),
      pubsub: createHostPubSub(),
      executors: {},
      executionFence: await fenceAt('open'),
    });
    const manager = backgroundTaskManagerForTests(host);
    const pagesRead: number[] = [];
    vi.spyOn(manager, 'listTasks').mockImplementation(async (filter) => {
      const page = filter?.page ?? 0;
      pagesRead.push(page);
      const from = page * FENCE_RESUME_SCAN_PAGE;
      return {
        tasks: remaining.slice(from, from + FENCE_RESUME_SCAN_PAGE),
      } as never;
    });
    const resume = vi
      .spyOn(manager, 'resume')
      .mockImplementation(async (taskId: string) => {
        // What core's handler does to the row: it leaves the suspended set.
        remaining.splice(
          remaining.findIndex((task) => task.id === taskId),
          1,
        );
        return undefined as never;
      });

    try {
      // #when — the first wake spends its whole scan budget finding nothing.
      await host.boot();

      // #then — it reached the bound without a single resume, and stopped one
      // page short of the cohort.
      expect(resume).not.toHaveBeenCalled();
      expect(pagesRead).toEqual(
        Array.from({ length: MAX_FENCE_RESUME_SCANS }, (_, page) => page),
      );

      // #when — the next alarm fires. Nothing changed on the deployment; the
      // only thing carried across is where the last wake stopped reading.
      pagesRead.length = 0;
      await host.onAlarm();

      // #then — it CONTINUES there rather than restarting, and the cohort that
      // was unreachable is drained.
      expect(pagesRead[0]).toBe(MAX_FENCE_RESUME_SCANS);
      expect(resume).toHaveBeenCalledTimes(3);
      expect(resume.mock.calls.map((call) => call[0] as string).sort()).toEqual(
        ['parked-0', 'parked-1', 'parked-2'],
      );
    } finally {
      await host.shutdown();
    }
  });

  it('claims nothing while locked, then completes the queued work after reopen', async () => {
    // #given — a real execution-mode host on D1 with work already queued: one
    // PENDING row waiting for a dispatcher, and one row a previous instance
    // left stranded in 'running' with the DEFAULT maxRetries of 0.
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    const domains = () => createBackgroundTaskD1Domains({ binding });
    const seedStorage = createD1Storage({ binding, domains: domains() });
    await seedStorage.init();
    const seedStore = await backgroundTasksStore(
      new Mastra({ storage: seedStorage }),
    );
    await seedStore.createTask(
      baseTask({ id: 'queued', status: 'pending', startedAt: undefined }),
    );
    await seedStore.createTask(baseTask({ id: 'stranded', status: 'running' }));

    const storage = createD1Storage({ binding, domains: domains() });
    await storage.init();
    const pubsub = createHostPubSub();
    const executionFence = await fenceAt('migration-locked');
    const execute = vi.fn(async () => ({ ran: true }));
    const host = newBackgroundTaskHost({
      mastra: new Mastra({ storage, pubsub }),
      pubsub,
      execution: true,
      executors: { longResearch: { execute } },
      executionFence,
    });

    try {
      // #when — the fenced boot (a request, or the DO alarm that woke it).
      await host.boot();
      // Nothing is asynchronous about "did not happen", so give the dispatch
      // topic a real chance to deliver before concluding it did not.
      await new Promise((resolve) => setTimeout(resolve, 50));

      // #then — NOTHING was claimed. The pending row is still pending, so it
      // is still visible to a nonterminal census and still owned by the queue.
      expect(await host.getTask('queued')).toMatchObject({
        status: 'pending',
      });
      // #and — the stranded row was NOT settled. This is the destructive path:
      // recoverStaleTasks marks every stranded maxRetries-0 row 'failed'
      // outright, before any executor is consulted, so an executor-level gate
      // could never have prevented it.
      expect(await host.getTask('stranded')).toMatchObject({
        status: 'running',
      });
      expect(execute).not.toHaveBeenCalled();

      // #when — the migration finishes and the operator reopens the fence. No
      // operator action beyond that: the next boot is a request or an alarm.
      await executionFence.transition({
        expected: 'migration-locked',
        next: 'open',
      });
      await host.boot();

      // #then — the queued work runs, exactly once.
      await vi.waitFor(
        async () => {
          expect((await host.getTask('queued'))?.status).toBe('completed');
        },
        { timeout: 5_000, interval: 10 },
      );
      expect(execute).toHaveBeenCalledTimes(1);
    } finally {
      await host.shutdown();
    }
  });

  it('re-drives parked tasks in BOUNDED passes, newest first', async () => {
    // #given — a long lock parked more tasks than one boot should re-drive. The
    // sweep runs on the boot path every request and every alarm waits behind,
    // so an unbounded serial resume would stall the first request after a
    // reopen by the whole queue length.
    const PER_PASS = 25;
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    const domains = () => createBackgroundTaskD1Domains({ binding });
    const seedStorage = createD1Storage({ binding, domains: domains() });
    await seedStorage.init();
    const seedStore = await backgroundTasksStore(
      new Mastra({ storage: seedStorage }),
    );
    const parked = PER_PASS + 5;
    for (let index = 0; index < parked; index += 1) {
      await seedStore.createTask(
        baseTask({
          id: `parked-${String(index).padStart(2, '0')}`,
          status: 'suspended',
          suspendedAt: new Date(1_000 + index),
          suspendPayload: {
            'flowsafe.executionFenced': { state: 'migration-locked' },
          },
        }),
      );
    }
    // #and — one task its own TOOL suspended. It carries no fence marker, so it
    // must survive every pass untouched.
    await seedStore.createTask(
      baseTask({
        id: 'tool-suspended',
        status: 'suspended',
        suspendedAt: new Date(9_999),
        suspendPayload: { awaitingWebhook: true },
      }),
    );

    const storage = createD1Storage({ binding, domains: domains() });
    await storage.init();
    const pubsub = createHostPubSub();
    const host = newBackgroundTaskHost({
      mastra: new Mastra({ storage, pubsub }),
      pubsub,
      executors: { longResearch: { execute: async () => ({ ran: true }) } },
      executionFence: await fenceAt('open'),
    });
    const resumed: string[] = [];
    vi.spyOn(backgroundTaskManagerForTests(host), 'resume').mockImplementation(
      async (taskId: string) => {
        resumed.push(taskId);
        return undefined as never;
      },
    );

    // #when
    try {
      await host.boot();
    } finally {
      await host.shutdown();
    }

    // #then — exactly one pass' worth, and the NEWEST suspensions, which is the
    // cohort a fence just parked. The remainder stays suspended and marked for
    // the next boot or alarm to take.
    expect(resumed).toHaveLength(PER_PASS);
    expect(resumed).not.toContain('tool-suspended');
    expect(resumed[0]).toBe(`parked-${String(parked - 1).padStart(2, '0')}`);
  });

  it('dispatches normally while draining', async () => {
    // #given — the queue IS the work a drain exists to finish, so a drain must
    // keep dispatching or it can never complete.
    const binding = sqliteUnitDatabase(openSqlite()) as never;
    const domains = () => createBackgroundTaskD1Domains({ binding });
    const seedStorage = createD1Storage({ binding, domains: domains() });
    await seedStorage.init();
    const seedStore = await backgroundTasksStore(
      new Mastra({ storage: seedStorage }),
    );
    await seedStore.createTask(
      baseTask({ id: 'draining', status: 'pending', startedAt: undefined }),
    );

    const storage = createD1Storage({ binding, domains: domains() });
    await storage.init();
    const pubsub = createHostPubSub();
    const host = newBackgroundTaskHost({
      mastra: new Mastra({ storage, pubsub }),
      pubsub,
      execution: true,
      executors: { longResearch: { execute: async () => ({ ran: true }) } },
      executionFence: await fenceAt('draining'),
    });

    // #then
    try {
      await host.boot();
      await vi.waitFor(
        async () => {
          expect((await host.getTask('draining'))?.status).toBe('completed');
        },
        { timeout: 5_000, interval: 10 },
      );
    } finally {
      await host.shutdown();
    }
  });

  it('parks rather than fails when the backstop catches an in-flight dispatch', async () => {
    // #given — the narrow race the wrapper exists for: a subscriber started
    // while the fence was open reaches a task body after it closed. Core has
    // already written status:'running' by then, so the only question is how the
    // body EXITS.
    const executionFence = await fenceAt('migration-locked');
    const ran = vi.fn(async () => ({ done: true }));
    const host = hostWith(executionFence, { execute: ran });
    await host.boot();
    const registered =
      backgroundTaskManagerForTests(host).getStaticExecutor('longResearch');
    const suspend = vi.fn(async (_data?: unknown) => undefined);

    // #when
    await expect(registered?.execute({}, { suspend })).resolves.toBeUndefined();

    // #then — SUSPENDED, not failed. A throw becomes outcome:'retry', and core
    // retries only while retryCount < maxRetries — the default is 0, so the
    // first throw would settle the row 'failed' and the work would be gone.
    expect(ran).not.toHaveBeenCalled();
    expect(suspend).toHaveBeenCalledTimes(1);
    expect(suspend).toHaveBeenCalledWith({
      'flowsafe.executionFenced': { state: 'migration-locked' },
    });
  });

  it('refuses loudly when core supplies no suspend to park with', async () => {
    // #given — `suspend` is optional on core's ToolExecutor contract. With no
    // way to park, refusing beats running a task body on a deployment whose
    // state is being copied.
    const host = hostWith(await fenceAt('migration-locked'), {
      execute: async () => ({ done: true }),
    });
    await host.boot();

    // #then
    await expect(
      backgroundTaskManagerForTests(host)
        .getStaticExecutor('longResearch')
        ?.execute({}),
    ).rejects.toBeInstanceOf(ExecutionFencedError);
  });

  it('boots and serves read routes while locked', async () => {
    // #given — boot() is deliberately NOT fence-gated: `#booted` memoizes it,
    // so a refusal there would be cached for the life of the isolate and would
    // take down the read routes every host serves after booting.
    const host = hostWith(await fenceAt('migration-locked'), {
      execute: async () => ({ done: true }),
    });

    // #then
    await expect(host.boot()).resolves.toBeUndefined();
    await expect(host.listTasks({ runId: 'abc_r1' })).resolves.toEqual({
      tasks: [],
      total: 0,
    });
  });

  it('accepts an enqueue while draining and refuses one once locked', async () => {
    // #given — refusing an enqueue during a drain would fail exactly the runs
    // that are draining, because the caller is a tool call inside one.
    const executionFence = await fenceAt('draining');
    const host = hostWith(executionFence, {
      execute: async () => ({ done: true }),
    });
    await host.boot();
    const payload = {
      toolName: 'longResearch',
      toolCallId: 'call-1',
      args: {},
      agentId: 'agent-1',
      runId: 'abc_r1',
    };

    // #then
    await expect(
      host.enqueue(payload, { executor: { execute: async () => ({}) } }),
    ).resolves.toMatchObject({
      task: expect.objectContaining({ id: expect.any(String) }),
    });

    // #when — locked
    await executionFence.transition({
      expected: 'draining',
      next: 'migration-locked',
    });

    // #then — a row written now is work the migration would have to carry.
    await expect(
      host.enqueue(payload, { executor: { execute: async () => ({}) } }),
    ).rejects.toBeInstanceOf(ExecutionFencedError);
  });
});
