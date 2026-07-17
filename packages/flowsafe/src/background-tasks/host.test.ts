// SPDX-License-Identifier: Apache-2.0
// Track B host wiring + the B-S2 recovery SEAM (R-002 pin: the PUBLIC async
// init(pubsub) fires recoverStaleTasks internally — no private method is
// called). The seam is proven at the storage layer, where it is deterministic
// and store-agnostic: a task an evicted instance left 'running' with no retry
// budget is resolved to 'failed' by a fresh instance's init(). That transition
// is the recovery seam firing; it needs no workflow execution.
//
// DISPATCH -> EXECUTE -> COMPLETE is NOT unit-proven here, on purpose. Core runs
// task bodies on the EVENTED execution engine, which (a) refuses to run unless
// the workflows store reports supportsConcurrentUpdates() — @mastra/cloudflare-d1
// returns false (R-B1) — and (b) drives step progress through an event loop the
// bare manager does not stand up in-process, so even on a concurrent-update
// store (InMemoryStore) a dispatched task never reaches terminal in a unit test
// (R-B2). Both findings are documented in host.ts; durable background-task
// EXECUTION on the Cloudflare substrate is a P9 follow-up, not shipped here. The
// persistence domain, recovery seam, purge/TTL, tenant-bound routes, and the
// _background defense — everything Track B actually adds — all work regardless.

import type { ToolExecutor } from '@mastra/core/background-tasks';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryStore, type MastraCompositeStore } from '@mastra/core/storage';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import { createD1Storage, createHostPubSub, init } from '../do-runner/index.js';
import { backgroundTasksStore } from './d1-storage.js';
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

describe('BackgroundTaskHost — wiring', () => {
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
