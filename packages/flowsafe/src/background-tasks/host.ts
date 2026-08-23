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
// THE FENCE SPLITS THAT SEAM IN TWO (F1). `registerStaticExecutor` claims
// nothing and always runs; `startWorkers()` + `init(pubsub)` are what make this
// instance a dispatcher, and behind a closed deployment execution fence they
// are held back entirely — init's recovery would otherwise fail every stranded
// row outright and re-claim every pending one. The held-back phase is retried
// on every later boot (request or alarm), so reopening the fence is all it
// takes to resume. See #ensureDispatching.
//
// v1 policy (DL-005/P8): connectors are foreground-only, so approval-carrying
// tools never enter this suspend/resume topology. Background suspend/resume
// stays available for NON-gated tools (e.g. a long research tool awaiting an
// external webhook); such a task's resume mints no capability.
//
// Execution is opt-in. The host accepts it only with the serialized D1 workflow
// adapter and the paired task domain. One DO isolate owns that manager and both
// domains. `boot()` then starts Mastra's public evented workers on the
// raw pubsub instance the caller passed to Mastra. Persistence-only hosts retain
// the earlier warning.
//
// Core keys an internal `__background-task` workflow separately from the task
// row. The specialized store and TTL purge therefore delete the internal
// snapshot before deleting the task row.

import {
  BackgroundTaskManager,
  type BackgroundTaskManagerConfig,
  type EnqueueResult,
  type TaskContext,
  type TaskPayload,
  type ToolExecutor,
} from '@mastra/core/background-tasks';
import type { Mastra } from '@mastra/core/mastra';

import {
  admitsDrainableExecution,
  ExecutionFencedError,
  type ExecutionFenceStore,
  type HostPubSub,
  OPEN_EXECUTION_FENCE,
} from '../do-runner/index.js';
import {
  finiteNonnegativeNumber,
  nonnegativeSafeInteger,
  positiveSafeInteger,
} from '../numeric-config.js';
import {
  backgroundTasksStore,
  DURABLE_OBJECT_BACKGROUND_TASKS_D1,
  type DurableObjectBackgroundTasksStorageD1,
  SERIALIZED_WORKFLOWS_D1,
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
   * Enable real evented-worker execution for this Durable Object. Omit to preserve
   * the persistence/recovery-only behavior.
   */
  execution?: boolean;
  /**
   * The deployment execution fence. Task bodies are an execution family that
   * runs BELOW RunnerRuntime, so the runtime's start/resume gate does not see
   * them; this is where they are gated instead.
   *
   * The gate is at the DISPATCHER, read once per boot pass: behind a closed
   * fence this instance never subscribes and never runs stale-task recovery,
   * so nothing is claimed — queued rows stay pending and stranded rows stay
   * stranded (#ensureDispatching explains why anything later than that is too
   * late). The executor wrapper remains as a fail-closed backstop for the
   * in-flight race, and parks rather than fails. Absent ⇒ unfenced.
   *
   * `draining` still dispatches and still accepts enqueues: that queue is the
   * work a drain exists to finish, and refusing an enqueue would fail the very
   * runs that are draining.
   */
  executionFence?: ExecutionFenceStore;
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
 * The suspend-payload key the executor backstop stamps on a task it parked
 * because the deployment was fenced mid-dispatch. Namespaced so it cannot
 * collide with a tool's own suspend payload, and read back by
 * #resumeFenceSuspendedTasks — which is what makes the parking reversible
 * rather than a quieter kind of loss.
 */
const EXECUTION_FENCE_SUSPEND_KEY = 'flowsafe.executionFenced';

/**
 * A BackgroundTaskManager bound to a hosting DO's Mastra + pubsub, with the
 * boot/alarm lifecycle that makes DO eviction survivable. The hosting DO owns
 * alarm arming (it needs `ctx.storage.setAlarm`); this class owns the manager
 * wiring, the recovery-firing `boot()`, and the alarm `cleanup()` duty. The raw
 * manager is reachable as `.manager` for the read-only routes to wrap — never
 * expose it directly over HTTP.
 */
export class BackgroundTaskHost {
  readonly manager: BackgroundTaskManager;
  readonly #mastra: Mastra;
  readonly #pubsub: HostPubSub;
  readonly #executors: Record<string, ToolExecutor>;
  readonly #execution: boolean;
  readonly #executionFence?: ExecutionFenceStore;
  #booted?: Promise<void>;
  #bootSettled = false;
  /** Phase B (workers + init), memoized only once it has actually run. */
  #dispatching?: Promise<void>;
  #dispatchSettled = false;
  #managerNeedsShutdown = false;
  #workersNeedStop = false;
  #shutdownRequested = false;
  #shutdown?: Promise<void>;

  constructor(options: BackgroundTaskHostOptions) {
    validateManagerConfig(options.manager);
    this.#mastra = options.mastra;
    this.#pubsub = options.pubsub;
    this.#executors = options.executors;
    this.#execution = options.execution ?? false;
    this.#executionFence = options.executionFence;
    this.manager = new BackgroundTaskManager({
      enabled: true,
      ...options.manager,
    });
    // Must precede init(): the manager reads its Mastra for storage, the
    // internal background-task workflow registration, and id generation.
    this.manager.__registerMastra(options.mastra);
  }

  /**
   * Durable-Object boot wiring, in two phases with different lifetimes.
   *
   * REGISTRATION (memoized for the life of the instance): validate the storage
   * domains and re-register the static executors. Nothing here subscribes,
   * claims, or recovers, so it is safe in every fence state — and it must be,
   * because every host boots before routing and the read routes hang off this
   * memo.
   *
   * DISPATCHING (fence-gated, re-attempted on every boot until it is admitted):
   * `startWorkers()` plus `manager.init(pubsub)`. Those two are what make this
   * instance CLAIM work — see #ensureDispatching for why they cannot run behind
   * a closed fence. A refused attempt is deliberately not memoized, so the next
   * `boot()` from a request or an alarm starts dispatching once the fence
   * reopens, with no operator action.
   */
  boot(): Promise<void> {
    if (this.#shutdownRequested) {
      return Promise.reject(
        new Error('background-tasks: host is shutting down'),
      );
    }
    if (!this.#booted) {
      this.#booted = this.#doRegister().finally(() => {
        this.#bootSettled = true;
      });
    }
    const registered = this.#booted;
    return registered.then(() => this.#ensureDispatching());
  }

  async #doRegister(): Promise<void> {
    // Fail-fast with a clear message before any dispatch could surface core's
    // terser error deep in a lifecycle callback.
    const tasks = await backgroundTasksStore(this.#mastra);
    // Execution mode validates every required storage and concurrency seam.
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
      const durableTasks = tasks as DurableObjectBackgroundTasksStorageD1;
      if (
        (durableTasks as unknown as Record<symbol, unknown>)[
          DURABLE_OBJECT_BACKGROUND_TASKS_D1
        ] !== true
      ) {
        throw new Error(
          'background-tasks: execution requires DurableObjectBackgroundTasksStorageD1',
        );
      }
    } else {
      await this.#warnIfBodiesCannotExecute();
    }
    for (const [toolName, executor] of Object.entries(this.#executors)) {
      this.manager.registerStaticExecutor(toolName, this.#gated(executor));
    }
  }

  /**
   * THE gate that matters: whether this instance becomes a DISPATCHER at all.
   *
   * `manager.init(pubsub)` is the only thing that subscribes `handleDispatch`,
   * and `handleDispatch` is what writes `status: 'running'` — the CLAIM. It
   * also runs `recoverStaleTasks()`, which on @mastra/core 1.53.0 does two
   * destructive things behind a closed fence:
   *
   *   1. every stranded `running` row with `maxRetries === 0` (the DEFAULT) is
   *      marked `failed` outright, before any executor is consulted; and
   *   2. every `pending` row is re-dispatched, i.e. claimed.
   *
   * Neither is reachable from inside an executor, which is why the gate lives
   * here and not there. Skipping init leaves pending rows pending and stranded
   * rows stranded — untouched, uncharged, and still visible to any nonterminal
   * census — and the published dispatch event simply has no subscriber. One
   * fence read per boot PASS, never memoized on refusal.
   *
   * `draining` still dispatches: the queue is exactly the work a drain exists
   * to finish.
   */
  async #ensureDispatching(): Promise<void> {
    if (this.#dispatching) return this.#dispatching;
    const fence = this.#executionFence
      ? await this.#executionFence.read()
      : OPEN_EXECUTION_FENCE;
    if (!admitsDrainableExecution(fence)) {
      console.warn(
        JSON.stringify({
          type: 'background-tasks.dispatch-fenced',
          state: fence.state,
          reason:
            'workers and stale-task recovery are held back; queued tasks stay pending until the fence reopens',
        }),
      );
      return;
    }
    this.#dispatchSettled = false;
    this.#dispatching = this.#startDispatching()
      .catch((error: unknown) => {
        // Not memoized on failure, for the same reason a refusal is not: the
        // next boot must be able to try again.
        this.#dispatching = undefined;
        throw error;
      })
      .finally(() => {
        this.#dispatchSettled = true;
      });
    return this.#dispatching;
  }

  async #startDispatching(): Promise<void> {
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
    // AFTER init, because resume() publishes onto the topic init subscribes.
    await this.#resumeFenceSuspendedTasks();
  }

  /**
   * Re-drive the tasks the executor backstop parked, once dispatching resumes.
   *
   * `recoverStaleTasks()` re-drives `running` and `pending` rows and knows
   * nothing about `suspended` ones, so without this a task the backstop saved
   * from destruction would be saved into a state nothing ever leaves. Scoped by
   * the backstop's own marker: a task suspended by its TOOL (awaiting a
   * webhook, say) is a different thing entirely and must stay suspended.
   */
  async #resumeFenceSuspendedTasks(): Promise<void> {
    try {
      const { tasks } = await this.manager.listTasks({ status: 'suspended' });
      for (const task of tasks) {
        const payload = task.suspendPayload;
        if (
          typeof payload !== 'object' ||
          payload === null ||
          !(EXECUTION_FENCE_SUSPEND_KEY in payload)
        ) {
          continue;
        }
        await this.manager.resume(task.id);
      }
    } catch (error) {
      // Best effort: a failure here leaves the rows suspended and inspectable,
      // which is where they already were. Never fails the boot that was about
      // to start serving.
      console.error(
        JSON.stringify({
          type: 'background-tasks.fence-resume-error',
          reason: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  }

  /**
   * The fail-closed BACKSTOP, not the gate. #ensureDispatching stops this
   * instance claiming work behind a closed fence; this only covers the narrow
   * race where a subscriber started while the fence was open consumes a
   * dispatch event published just before it closed. By then core has ALREADY
   * written `status: 'running'`, so the claim cannot be undone from here — only
   * exited well.
   *
   * SUSPEND, never throw. A thrown executor becomes `outcome: 'retry'`, and
   * core retries only while `retryCount < maxRetries` — the default maxRetries
   * is 0, so the very first throw settles the row `failed`: work destroyed, and
   * silently absent from any nonterminal census. `suspend()` instead persists
   * `status: 'suspended'` with a payload naming the fence, which is nonterminal,
   * inspectable, and re-driven by #resumeFenceSuspendedTasks after reopen.
   * Throwing survives only as the last resort for a core that supplied no
   * `suspend` (it is optional on the interface): refusing loudly beats running
   * a task body on a deployment whose state is being copied.
   */
  #gated(executor: ToolExecutor): ToolExecutor {
    return {
      execute: async (args, executeOptions) => {
        const fence = this.#executionFence
          ? await this.#executionFence.read()
          : OPEN_EXECUTION_FENCE;
        if (!admitsDrainableExecution(fence)) {
          const suspend = executeOptions?.suspend;
          if (!suspend) {
            throw new ExecutionFencedError(fence.state, 'background task');
          }
          await suspend({
            [EXECUTION_FENCE_SUSPEND_KEY]: { state: fence.state },
          });
          // Discarded on the suspend path — see core's ToolExecutor contract.
          return undefined;
        }
        return executor.execute(args, executeOptions);
      },
    };
  }

  /**
   * Enqueue a background task through the fence.
   *
   * A drain deliberately still ACCEPTS enqueues: the caller is an agent's tool
   * call inside a run the drain is trying to finish, and refusing would fail
   * exactly the runs that are draining. Locked and proof-only refuse, because
   * a row written then is work the migration would have to carry.
   *
   * The per-task executor is wrapped with the same gate as the static ones, so
   * a task enqueued while open cannot execute its body after a transition.
   */
  async enqueue(
    payload: TaskPayload,
    context?: TaskContext,
  ): Promise<EnqueueResult> {
    const fence = this.#executionFence
      ? await this.#executionFence.read()
      : OPEN_EXECUTION_FENCE;
    if (!admitsDrainableExecution(fence)) {
      throw new ExecutionFencedError(fence.state, 'background task enqueue');
    }
    return this.manager.enqueue(
      payload,
      context
        ? { ...context, executor: this.#gated(context.executor) }
        : undefined,
    );
  }

  async #warnIfBodiesCannotExecute(): Promise<void> {
    const workflows = await this.#mastra.getStorage()?.getStore('workflows');
    // Optional-chained: a store that predates the capability method reads as
    // "cannot" (fail loud, never silently assume support).
    const supports = workflows?.supportsConcurrentUpdates?.() ?? false;
    if (!supports) {
      console.warn(
        'background-tasks: persistence-only mode cannot execute task bodies because the workflows domain does not support concurrent updates; configure createBackgroundTaskD1Domains and execution: true on one Durable Object to enable execution',
      );
    }
  }

  /**
   * DO-alarm duty: ensure the DO is booted (a FRESH post-eviction instance
   * recovers HERE, since the alarm is what woke it — see the recovery seam
   * above), then run the manager's TTL cleanup. The hosting DO re-arms the alarm
   * after this returns. Cleanup is core's own `cleanup()` — the belt to the
   * storage-layer `purgeExpiredBackgroundTasks` maintenance duty (which needs no
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
        // #doRegister only validates and registers; nothing to unwind.
      }
    }
    // Awaited ONLY while still in flight — the same shape, and the same
    // reason, as the registration guard above: manager.shutdown() flips its
    // enqueue guard before its first await, and that guarantee survives only
    // if nothing suspends this function before it is invoked.
    if (this.#dispatching && !this.#dispatchSettled) {
      try {
        await this.#dispatching;
      } catch {
        // #startDispatching already unwound every component it managed to
        // stop. Any component whose cleanup failed remains flagged for the
        // retry below.
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
