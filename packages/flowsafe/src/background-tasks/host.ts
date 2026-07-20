// SPDX-License-Identifier: Apache-2.0
// Track B (M-003): host a Mastra BackgroundTaskManager on a Durable Object, and
// survive DO eviction BY CONSTRUCTION (DL-015).
//
// THE RECOVERY SEAM (spike B-S2, pinned against @mastra/core 1.50.0 dist —
// validation finding R-002). `recoverStaleTasks()` and `handleResume` are
// PRIVATE and `getStorage()` is async, so the alarm CANNOT call
// `recoverStaleTasks()` directly. But it does not need to: the PUBLIC async
// `manager.init(pubsub)` fires `recoverStaleTasks()` internally (manager.d.ts:21
// -> chunk .init -> `await this.recoverStaleTasks()`), guarded by its own
// initPromise so it runs once per manager INSTANCE. A DO evicted mid-task leaves
// its task row 'running'/'pending' in D1; when the DO is next instantiated (a
// FRESH manager), `boot()` re-registers the static tool executors and calls
// `init(pubsub)`, whose recovery resets the stranded 'running' task (maxRetries
// > 0) to 'pending' and re-dispatches it — its workflow step resolves the
// executor by tool name via the re-registered static registry (the cross-process
// path core ships `registerStaticExecutor` for). The DO ALARM is what WAKES an
// evicted DO so this happens without waiting for a request. No private method is
// ever called; the seam is `registerStaticExecutor` + `init(pubsub)`, both
// public.
//
// v1 policy (DL-005/P8): connectors are foreground-only, so approval-carrying
// tools never enter this suspend/resume topology. Background suspend/resume
// stays available for NON-gated tools (e.g. a long research tool awaiting an
// external webhook); such a task's resume mints no capability.
//
// Execution is opt-in. The host accepts it only with the serialized D1 workflow
// adapter and a task domain bound to the same tenant. One DO isolate owns that
// manager and both domains, so core's tenant-blind recovery sees only that
// tenant's rows. `boot()` then starts Mastra's public evented workers on the
// manager's pubsub identity. Persistence-only hosts retain the earlier warning.
//
// Core still keys an internal `__background-task` workflow by the unsalted task
// ID. The tenant-scoped store, TTL purge, and offboarding paths therefore delete
// the internal snapshot before deleting the task row that supplies its tenant
// association.

import {
  BackgroundTaskManager,
  type BackgroundTaskManagerConfig,
  type ToolExecutor,
} from '@mastra/core/background-tasks';
import type { Mastra } from '@mastra/core/mastra';

import type { HostPubSub } from '../do-runner/index.js';
import {
  backgroundTasksStore,
  SERIALIZED_WORKFLOWS_D1,
  TENANT_SCOPED_BACKGROUND_TASKS_D1,
  type TenantScopedBackgroundTasksStorageD1,
} from './d1-storage.js';

export interface BackgroundTaskHostOptions {
  /**
   * The DO's Mastra — must carry a storage adapter whose
   * `getStore('backgroundTasks')` resolves (D1Store does). The manager reads
   * every task through it.
   */
  mastra: Mastra;
  /**
   * The DO's ONE pubsub identity (DL-001, `createHostPubSub()`). REQUIRED: the
   * manager both publishes dispatch events and subscribes the worker callback on
   * it, and inside a DO one isolate serializes publisher and subscriber, so an
   * in-process `EventEmitterPubSub` is exactly right (no cross-process bus). A
   * host that streams lifecycle events to its hub passes the SAME instance it
   * gave `init()`.
   */
  pubsub: HostPubSub;
  /**
   * Static tool executors, keyed by tool name, RE-REGISTERED at every boot so a
   * task recovered on a fresh DO instance (post-eviction) resolves its executor
   * by name (DL-015). Tools are static — their executor closures are safe to
   * rebuild deterministically at boot, unlike per-task closures which core
   * documents are "never serialized".
   */
  executors: Record<string, ToolExecutor>;
  /**
   * Manager config (core `BackgroundTaskManagerConfig` minus `enabled`, which is
   * forced on): concurrency, backpressure ('queue'|'reject'|'fallback-sync'),
   * timeouts, retries, and `cleanup` TTLs. Omitted => core defaults.
   */
  manager?: Omit<BackgroundTaskManagerConfig, 'enabled'>;
  /**
   * Enable real evented-worker execution for one tenant DO. Omit to preserve
   * the persistence/recovery-only behavior.
   */
  execution?: { tenantId: string };
}

/**
 * A BackgroundTaskManager bound to a hosting DO's Mastra + pubsub, with the
 * boot/alarm lifecycle that makes DO eviction survivable. The hosting DO owns
 * alarm arming (it needs `ctx.storage.setAlarm`); this class owns the manager
 * wiring, the recovery-firing `boot()`, and the alarm `cleanup()` duty. The raw
 * manager is reachable as `.manager` for the tenant-bound routes to wrap — never
 * expose it directly over HTTP (DL-014).
 */
export class BackgroundTaskHost {
  readonly manager: BackgroundTaskManager;
  readonly #mastra: Mastra;
  readonly #pubsub: HostPubSub;
  readonly #executors: Record<string, ToolExecutor>;
  readonly #execution?: { tenantId: string };
  #booted?: Promise<void>;

  constructor(options: BackgroundTaskHostOptions) {
    this.#mastra = options.mastra;
    this.#pubsub = options.pubsub;
    this.#executors = options.executors;
    this.#execution = options.execution;
    this.manager = new BackgroundTaskManager({
      enabled: true,
      ...options.manager,
    });
    // Must precede init(): the manager reads its Mastra for storage, the
    // internal background-task workflow registration, and id generation.
    this.manager.__registerMastra(options.mastra);
  }

  /**
   * DO-boot wiring (DL-015). Fail-fast if the backgroundTasks storage domain is
   * missing, re-register the static executors, THEN `init(pubsub)` — whose
   * internal `recoverStaleTasks()` re-drives any task the evicted instance left
   * mid-flight. Executors go in BEFORE init so a recovered task's workflow step
   * can resolve its executor by name. Memoized per instance: `init` is itself
   * idempotent (initPromise), and `boot()` from both `fetch()` and `alarm()`
   * must not double-register.
   */
  boot(): Promise<void> {
    if (!this.#booted) {
      this.#booted = this.#doBoot();
    }
    return this.#booted;
  }

  async #doBoot(): Promise<void> {
    // Fail-fast with a clear message before any dispatch could surface core's
    // terser error deep in a lifecycle callback.
    const tasks = await backgroundTasksStore(this.#mastra);
    // Execution mode validates every required ownership and concurrency seam.
    if (this.#execution) {
      const workflows = await this.#mastra.getStorage()?.getStore('workflows');
      if (
        !workflows ||
        (workflows as unknown as Record<symbol, unknown>)[
          SERIALIZED_WORKFLOWS_D1
        ] !== true
      ) {
        throw new Error(
          'background-tasks: execution requires DurableObjectWorkflowsStorageD1',
        );
      }
      const tenantTasks = tasks as TenantScopedBackgroundTasksStorageD1;
      if (
        (tenantTasks as unknown as Record<symbol, unknown>)[
          TENANT_SCOPED_BACKGROUND_TASKS_D1
        ] !== true ||
        tenantTasks.tenantId !== this.#execution.tenantId
      ) {
        throw new Error(
          'background-tasks: execution requires a task store branded for the same tenant',
        );
      }
      if (this.#mastra.pubsub !== this.#pubsub) {
        throw new Error(
          'background-tasks: manager, Mastra, and host must share one pubsub identity',
        );
      }
    } else {
      await this.#warnIfBodiesCannotExecute();
    }
    for (const [toolName, executor] of Object.entries(this.#executors)) {
      this.manager.registerStaticExecutor(toolName, executor);
    }
    await this.manager.init(this.#pubsub);
    if (this.#execution) await this.#mastra.startWorkers();
  }

  async #warnIfBodiesCannotExecute(): Promise<void> {
    const workflows = await this.#mastra.getStorage()?.getStore('workflows');
    // Optional-chained: a store that predates the capability method reads as
    // "cannot" (fail loud, never silently assume support).
    const supports = workflows?.supportsConcurrentUpdates?.() ?? false;
    if (!supports) {
      console.warn(
        'background-tasks: persistence-only mode cannot execute task bodies because the workflows domain does not support concurrent updates; configure createBackgroundTaskD1Domains and execution.tenantId on one single-tenant Durable Object to enable execution',
      );
    }
  }

  /**
   * DO-alarm duty: ensure the DO is booted (a FRESH post-eviction instance
   * recovers HERE, since the alarm is what woke it — see the recovery seam
   * above), then run the manager's TTL cleanup. The hosting DO re-arms the alarm
   * after this returns. Cleanup is core's own `cleanup()` — the belt to the
   * storage-layer `purgeExpiredBackgroundTasks` cron braces (that one needs no
   * live manager); running both is harmless (each only deletes rows past the
   * TTL).
   */
  async onAlarm(): Promise<void> {
    await this.boot();
    await this.manager.cleanup();
  }

  /** Graceful teardown — drains the manager's cleanup interval + in-flight state. */
  async shutdown(): Promise<void> {
    if (this.#execution) await this.#mastra.stopWorkers();
    await this.manager.shutdown();
  }
}
