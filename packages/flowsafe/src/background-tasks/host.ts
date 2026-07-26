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
// FRESH manager), `boot()` re-registers the static tool executors, starts the
// workflow workers, and only then calls `init(pubsub)`. Init's recovery resets a
// stranded 'running' task (maxRetries > 0) to 'pending' and re-dispatches it;
// starting workers first guarantees that the workflow event published by that
// dispatch has a subscriber. Its workflow step resolves the executor by tool
// name via the re-registered static registry (the cross-process path core ships
// `registerStaticExecutor` for). The DO ALARM is what WAKES an evicted DO so
// this happens without waiting for a request. No private method is ever called;
// the seam is `registerStaticExecutor` + `startWorkers()` + `init(pubsub)`, all
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
// raw pubsub instance the caller passed to Mastra. Persistence-only hosts retain
// the earlier warning.
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
  finiteNonnegativeNumber,
  nonnegativeSafeInteger,
  positiveSafeInteger,
} from '../numeric-config.js';
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
   * The Durable Object's single raw pubsub instance from `createHostPubSub()`. Pass this
   * same object to `new Mastra({ pubsub })`; Mastra 1.50 exposes a proxy from
   * its public `pubsub` getter, so that getter is not the constructor identity.
   * The manager both publishes dispatch events and subscribes the worker
   * callback on the raw instance, and inside a DO one isolate serializes
   * publisher and subscriber, so an in-process `EventEmitterPubSub` is exactly
   * right (no cross-process bus). A host that streams lifecycle events to its
   * hub also passes this same raw instance to `init()`.
   */
  pubsub: HostPubSub;
  /**
   * Static tool executors, keyed by tool name, RE-REGISTERED at every boot so a
   * task recovered on a fresh DO instance (post-eviction) resolves its executor
   * by name. Tools are static — their executor closures are safe to
   * rebuild deterministically at boot, unlike per-task closures which core
   * documents are "never serialized".
   */
  executors: Record<string, ToolExecutor>;
  /**
   * Manager config (core `BackgroundTaskManagerConfig` minus `enabled`, which is
   * forced on): concurrency, backpressure ('queue'|'reject'|'fallback-sync'),
   * timeouts, retries, and `cleanup` TTLs. Numeric values are validated before
   * constructing core's manager: concurrency/retry delays/TTLs/throttle are
   * nonnegative, timeout/cleanup intervals are positive, and all integer
   * quantities must be safe integers. Omitted => core defaults.
   */
  manager?: Omit<BackgroundTaskManagerConfig, 'enabled'>;
  /**
   * Enable real evented-worker execution for one tenant DO. Omit to preserve
   * the persistence/recovery-only behavior.
   */
  execution?: { tenantId: string };
}

function validateManagerConfig(
  config: Omit<BackgroundTaskManagerConfig, 'enabled'> | undefined,
): void {
  if (!config) return;
  if (config.globalConcurrency !== undefined) {
    nonnegativeSafeInteger(
      config.globalConcurrency,
      'backgroundTasks.manager.globalConcurrency',
    );
  }
  if (config.perAgentConcurrency !== undefined) {
    nonnegativeSafeInteger(
      config.perAgentConcurrency,
      'backgroundTasks.manager.perAgentConcurrency',
    );
  }
  if (config.defaultTimeoutMs !== undefined) {
    positiveSafeInteger(
      config.defaultTimeoutMs,
      'backgroundTasks.manager.defaultTimeoutMs',
    );
  }
  if (config.progressThrottleMs !== undefined) {
    nonnegativeSafeInteger(
      config.progressThrottleMs,
      'backgroundTasks.manager.progressThrottleMs',
    );
  }
  if (config.waitTimeoutMs !== undefined) {
    positiveSafeInteger(
      config.waitTimeoutMs,
      'backgroundTasks.manager.waitTimeoutMs',
    );
  }
  const retries = config.defaultRetries;
  if (retries?.maxRetries !== undefined) {
    nonnegativeSafeInteger(
      retries.maxRetries,
      'backgroundTasks.manager.defaultRetries.maxRetries',
    );
  }
  if (retries?.retryDelayMs !== undefined) {
    nonnegativeSafeInteger(
      retries.retryDelayMs,
      'backgroundTasks.manager.defaultRetries.retryDelayMs',
    );
  }
  if (retries?.maxRetryDelayMs !== undefined) {
    nonnegativeSafeInteger(
      retries.maxRetryDelayMs,
      'backgroundTasks.manager.defaultRetries.maxRetryDelayMs',
    );
  }
  if (retries?.backoffMultiplier !== undefined) {
    finiteNonnegativeNumber(
      retries.backoffMultiplier,
      'backgroundTasks.manager.defaultRetries.backoffMultiplier',
    );
  }
  const cleanup = config.cleanup;
  if (cleanup?.completedTtlMs !== undefined) {
    nonnegativeSafeInteger(
      cleanup.completedTtlMs,
      'backgroundTasks.manager.cleanup.completedTtlMs',
    );
  }
  if (cleanup?.failedTtlMs !== undefined) {
    nonnegativeSafeInteger(
      cleanup.failedTtlMs,
      'backgroundTasks.manager.cleanup.failedTtlMs',
    );
  }
  if (cleanup?.cleanupIntervalMs !== undefined) {
    positiveSafeInteger(
      cleanup.cleanupIntervalMs,
      'backgroundTasks.manager.cleanup.cleanupIntervalMs',
    );
  }
}

/**
 * A BackgroundTaskManager bound to a hosting DO's Mastra + pubsub, with the
 * boot/alarm lifecycle that makes DO eviction survivable. The hosting DO owns
 * alarm arming (it needs `ctx.storage.setAlarm`); this class owns the manager
 * wiring, the recovery-firing `boot()`, and the alarm `cleanup()` duty. The raw
 * manager is reachable as `.manager` for the tenant-bound routes to wrap — never
 * expose it directly over HTTP.
 */
export class BackgroundTaskHost {
  readonly manager: BackgroundTaskManager;
  readonly #mastra: Mastra;
  readonly #pubsub: HostPubSub;
  readonly #executors: Record<string, ToolExecutor>;
  readonly #execution?: { tenantId: string };
  #booted?: Promise<void>;
  #bootSettled = false;
  #managerNeedsShutdown = false;
  #workersNeedStop = false;
  #shutdownRequested = false;
  #shutdown?: Promise<void>;

  constructor(options: BackgroundTaskHostOptions) {
    validateManagerConfig(options.manager);
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
   * Durable-Object boot wiring. Fail fast if the backgroundTasks storage domain is
   * missing, re-register the static executors, start execution-mode workflow
   * workers, THEN call `init(pubsub)` — whose internal `recoverStaleTasks()`
   * re-drives any task the evicted instance left mid-flight. Executors and the
   * workflow subscriber both go in BEFORE recovery publishes work. A failed
   * startup unwinds attempted components in reverse order. Memoized per
   * instance: `init` is itself idempotent (initPromise), and `boot()` from both
   * `fetch()` and `alarm()` must not double-register.
   */
  boot(): Promise<void> {
    if (this.#shutdownRequested) {
      return Promise.reject(
        new Error('background-tasks: host is shutting down'),
      );
    }
    if (!this.#booted) {
      this.#booted = this.#doBoot().finally(() => {
        this.#bootSettled = true;
      });
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
    } else {
      await this.#warnIfBodiesCannotExecute();
    }
    for (const [toolName, executor] of Object.entries(this.#executors)) {
      this.manager.registerStaticExecutor(toolName, executor);
    }
    try {
      if (this.#execution) {
        this.#workersNeedStop = true;
        await this.#mastra.startWorkers();
      }
      this.#managerNeedsShutdown = true;
      await this.manager.init(this.#pubsub);
    } catch (primary) {
      const cleanupErrors: unknown[] = [];
      if (this.#managerNeedsShutdown) {
        try {
          await this.manager.shutdown();
          this.#managerNeedsShutdown = false;
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
      if (this.#workersNeedStop) {
        try {
          await this.#mastra.stopWorkers();
          this.#workersNeedStop = false;
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
      if (cleanupErrors.length === 0) throw primary;
      throw new AggregateError(
        [primary, ...cleanupErrors],
        'background-tasks: boot failed and cleanup also failed',
        { cause: primary },
      );
    }
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

  /**
   * Graceful teardown. Stop the manager first: its shutdown flag rejects new
   * enqueues synchronously before the workflow consumers disappear. Worker
   * teardown is still attempted if manager teardown fails.
   */
  async #doShutdown(): Promise<void> {
    if (this.#booted && !this.#bootSettled) {
      try {
        await this.#booted;
      } catch {
        // #doBoot already unwound every component it managed to stop. Any
        // component whose cleanup failed remains flagged for the retry below.
      }
    }
    const errors: unknown[] = [];
    if (this.#managerNeedsShutdown) {
      try {
        await this.manager.shutdown();
        this.#managerNeedsShutdown = false;
      } catch (error) {
        errors.push(error);
      }
    }
    if (this.#workersNeedStop) {
      try {
        await this.#mastra.stopWorkers();
        this.#workersNeedStop = false;
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) {
      throw new AggregateError(errors, 'background-tasks: shutdown failed', {
        cause: errors[0],
      });
    }
  }

  async shutdown(): Promise<void> {
    this.#shutdownRequested = true;
    if (this.#shutdown) return this.#shutdown;
    const pending = this.#doShutdown();
    this.#shutdown = pending;
    try {
      await pending;
    } finally {
      if (this.#shutdown === pending) this.#shutdown = undefined;
    }
  }
}
