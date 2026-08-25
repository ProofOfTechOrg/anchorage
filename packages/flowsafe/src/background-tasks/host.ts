// SPDX-License-Identifier: Apache-2.0
// Host a Mastra BackgroundTaskManager on a Durable Object and survive eviction
// BY CONSTRUCTION.
//
// THE RECOVERY SEAM, pinned against @mastra/core 1.50.0 dist.
// `recoverStaleTasks()` and `handleResume` are PRIVATE, and `getStorage()` is
// async, so the alarm CANNOT call `recoverStaleTasks()` directly. It does not
// need to: the PUBLIC async `manager.init(pubsub)` fires `recoverStaleTasks()`
// internally. Its manager.d.ts:21 -> chunk .init path awaits
// `this.recoverStaleTasks()`, guarded by its own initPromise so it runs once
// per manager INSTANCE. A DO evicted mid-task leaves its task row
// 'running'/'pending' in D1. When the DO is next instantiated with a FRESH
// manager, `boot()` re-registers the static tool executors, starts the workflow
// workers, and only then calls `init(pubsub)`. Init's recovery resets a
// stranded 'running' task (maxRetries > 0) to 'pending' and re-dispatches it;
// starting workers first guarantees that the workflow event has a subscriber.
// Its workflow step resolves the executor by tool name via the re-registered
// static registry: the cross-process path core ships `registerStaticExecutor`
// for this. The DO ALARM WAKES an evicted DO so
// this happens without waiting for a request. No private method is ever called;
// the seam is `registerStaticExecutor` + `startWorkers()` + `init(pubsub)`, all
// public.
//
// THE FENCE SPLITS THAT SEAM IN TWO. `registerStaticExecutor` claims
// nothing and always runs; `startWorkers()` + `init(pubsub)` are what make this
// instance a dispatcher, and behind a closed deployment execution fence they
// are held back entirely — init's recovery would otherwise fail every stranded
// row outright and re-claim every pending one. The held-back phase is retried
// on every later boot (request or alarm), so reopening the fence is all it
// takes to resume. See #ensureDispatching.
//
// v1 policy: connectors are foreground-only, so approval-carrying
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
  type BackgroundTask,
  BackgroundTaskManager,
  type BackgroundTaskManagerConfig,
  type EnqueueResult,
  type TaskContext,
  type TaskFilter,
  type TaskListResult,
  type TaskPayload,
  type ToolExecutor,
} from '@mastra/core/background-tasks';
import type { Mastra } from '@mastra/core/mastra';
import { EXECUTION_FENCE_SUSPEND_KEY } from '../do-runner/execution-fence.js';
import {
  admitsDrainableExecution,
  ExecutionFencedError,
  type ExecutionFenceWiring,
  type HostPubSub,
  readExecutionFence,
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

export { EXECUTION_FENCE_SUSPEND_KEY } from '../do-runner/execution-fence.js';

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
   * The deployment execution fence, or `'none'` for a host with no database
   * behind it. Task bodies are an execution family that runs BELOW
   * RunnerRuntime, so the runtime's start/resume gate does not see them; this
   * is where they are gated instead.
   *
   * The gate is at the DISPATCHER, read once per boot pass: behind a closed
   * fence this instance never subscribes and never runs stale-task recovery,
   * so nothing is claimed — queued rows stay pending and stranded rows stay
   * stranded (#ensureDispatching explains why anything later than that is too
   * late). The executor wrapper remains as a fail-closed backstop for the
   * in-flight race, and parks rather than fails.
   *
   * REQUIRED: this is the ONLY gate task bodies pass, so an unfenced host runs
   * them straight through a migration lock — and it looks wired, because every
   * other surface of the same deployment reports the fence. See
   * ExecutionFenceWiring.
   *
   * `draining` still dispatches and still accepts enqueues: that queue is the
   * work a drain exists to finish, and refusing an enqueue would fail the very
   * runs that are draining.
   */
  executionFence: ExecutionFenceWiring;
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
 * Was this row parked by the backstop, rather than suspended by its own tool?
 * The distinction is the whole safety of the sweep: a task awaiting a webhook
 * is suspended on purpose and must stay that way.
 */
function isFenceParked(task: BackgroundTask): boolean {
  const payload = task.suspendPayload;
  return (
    typeof payload === 'object' &&
    payload !== null &&
    EXECUTION_FENCE_SUSPEND_KEY in payload
  );
}

/**
 * How many fence-parked tasks ONE BOOT re-drives.
 *
 * Sized as a boot-path budget rather than a queue-drain rate: each resume is a
 * publish onto the topic the manager just subscribed to, and the boot this
 * blocks is what every request on the deployment is waiting behind. The
 * remainder is not dropped — it stays `suspended`, marked, and inspectable
 * through the read routes — and the ALARM lane below is what drains it, so a
 * deployment reopening with a large parked queue starts moving on its first
 * request without that request paying for the whole queue.
 *
 * Exported for the test that pins the boot/alarm budget boundary, and kept off
 * `./index.js` — the only export map entry this directory has — so it stays
 * package-internal rather than becoming a number a consumer can depend on.
 */
export const MAX_FENCE_RESUMES_PER_PASS = 25;

/**
 * How many one ALARM re-drives — the lane convergence actually rides on.
 *
 * Ten boot-budgets, because an alarm is a background duty and nothing is
 * waiting behind it, unlike a boot. This is what makes the sweep terminate
 * rather than merely make progress: the alarm recurs (the host arms it for
 * `cleanup()` already), each wake drains up to this many, and the invocation
 * itself keeps scanning until a scan finds nothing left to resume.
 */
const MAX_FENCE_RESUMES_PER_ALARM = 250;

/**
 * The hard bound on how many LIST queries one sweep invocation may issue.
 *
 * The sweep pages, and paging is what keeps an old parked cohort reachable
 * once newer tool-suspends sit ahead of it in the scan order. Bounding the
 * scans rather than only the resumes is what keeps a pathological mix — many
 * thousands of tool-suspended rows and one stale parked cohort behind them —
 * from turning a single wake into an unbounded read loop.
 *
 * A wake that hits this bound WITHOUT resuming anything has proved only that
 * the first `MAX_FENCE_RESUME_SCANS * FENCE_RESUME_SCAN_PAGE` rows carry no
 * marker, so it records where it stopped (`#sweepScanFloor`) and the next wake
 * continues from there. Restarting every wake at page 0 instead would re-read
 * the same prefix forever, and a cohort parked behind that many newer
 * tool-suspends would never be reached at all.
 *
 * Exported alongside FENCE_RESUME_SCAN_PAGE for the same narrow reason
 * MAX_FENCE_RESUMES_PER_PASS is: the test that proves a cohort past one wake's
 * scan bound is still reached has to build a set that large, and a size
 * hard-coded there would drift from these. Both stay off `./index.js` — they
 * are tuning, not contract.
 */
export const MAX_FENCE_RESUME_SCANS = 20;

/**
 * How many `suspended` rows one pass READS to find that many.
 *
 * Larger than the resume budget on purpose. The fence marker lives in the
 * suspend PAYLOAD, which no filter can express, so the pass has to read rows
 * and sort them itself — and a page sized exactly to the resume budget would
 * let a handful of tool-suspended tasks (awaiting a webhook, say) crowd the
 * parked ones out of every pass and stall recovery indefinitely. Reading wider
 * than it acts costs one query and buys headroom for that mix, while still
 * bounding what a boot pulls out of D1.
 */
export const FENCE_RESUME_SCAN_PAGE = 100;

/**
 * A registration failure this host CHOSE, from wiring it can re-check without
 * touching storage: the domains a host passed are the wrong ones.
 *
 * The distinction is what lets `boot()` memoize one kind of failure and retry
 * the other. A configuration fault is deterministic — the next boot re-reads
 * the same host wiring and fails identically — so caching it costs nothing and
 * keeps the error stable. Everything else reaching #doRegister comes from a
 * storage read (`getStorage()`, `getStore(...)`), and a transient D1 fault
 * there must not lock this instance's read routes out until eviction.
 *
 * The split is deliberately conservative in one direction: a deterministic
 * fault raised from INSIDE a storage read (a Mastra configured with no storage
 * at all) is retried like a transient one, because from here the two are the
 * same call. That costs one repeated read per boot and nothing else, whereas
 * mistaking a transient fault for a permanent one costs the isolate's routes.
 *
 * Not exported: a caller cannot act on the difference, only this class can.
 */
class BackgroundTasksConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BackgroundTasksConfigurationError';
  }
}

/**
 * Raw-manager access for THIS PACKAGE'S TESTS, and nothing else.
 *
 * A side table rather than a property ON the host, because a property is
 * reachable whatever it is keyed by: an own symbol key is enumerable through
 * `Object.getOwnPropertySymbols(host)[0]`, so "unreachable from the published
 * package" was true of the NAME and false of the manager. Nothing outside this
 * module can produce this WeakMap, so the accessor below — absent from
 * `./index.js`, the only export map entry this directory has — is the only way
 * in, and the claim is now literally true.
 *
 * It exists because the host's lifecycle contract IS its interleaving with
 * core's manager (init before shutdown before stopWorkers, the enqueue guard
 * flipping before the first await), and there is no way to observe that
 * ordering from outside without a handle on the manager those calls land on.
 *
 * Production code must not use it: everything a host legitimately does goes
 * through `enqueue` (fence-gated) or the three reads on the host itself.
 */
const hostManagers = new WeakMap<BackgroundTaskHost, BackgroundTaskManager>();

/** @see hostManagers — tests only, and never re-exported. */
export function backgroundTaskManagerForTests(
  host: BackgroundTaskHost,
): BackgroundTaskManager {
  const manager = hostManagers.get(host);
  if (!manager) {
    throw new Error('background-tasks: host has no manager registered');
  }
  return manager;
}

/**
 * A BackgroundTaskManager bound to a hosting DO's Mastra + pubsub, with the
 * boot/alarm lifecycle that makes DO eviction survivable. The hosting DO owns
 * alarm arming (it needs `ctx.storage.setAlarm`); this class owns the manager
 * wiring, the recovery-firing `boot()`, and the alarm `cleanup()` duty.
 *
 * The manager itself is PRIVATE. It carries `enqueue`, `registerTaskContext`,
 * `registerStaticExecutor`, `resume`, and `restart` — every one of which puts a
 * task body on this deployment WITHOUT passing the fence, because the gate that
 * stops a locked deployment executing is the `#gated` wrapper this host puts
 * around an executor on the way in. A caller holding the manager could enqueue
 * an unwrapped executor and defeat both. What this class forwards instead is
 * the fence-gated `enqueue` and the three READS the host route adapter serves.
 */
export class BackgroundTaskHost {
  readonly #manager: BackgroundTaskManager;
  readonly #mastra: Mastra;
  readonly #pubsub: HostPubSub;
  readonly #executors: Record<string, ToolExecutor>;
  readonly #execution: boolean;
  readonly #executionFence: ExecutionFenceWiring;
  #booted?: Promise<void>;
  /** Phase B (workers + init), memoized only once it has actually run. */
  #dispatching?: Promise<void>;
  /**
   * The in-flight phase-B ATTEMPT, including the fence read that precedes
   * admission — recorded synchronously by #ensureDispatching and cleared when
   * it settles. `#dispatching` cannot serve this purpose: it is unset for the
   * whole duration of the fence read, which is exactly the window two
   * concurrent boots interleave in.
   */
  #dispatchAttempt?: Promise<void>;
  /**
   * The whole in-flight `boot()` — registration, the fence read, and phase B if
   * the fence admitted it — recorded SYNCHRONOUSLY by `boot()`.
   *
   * Distinct from `#dispatching`, which is the memo of an ADMITTED phase and so
   * is still unset while the fence read is in flight. Teardown waits on THIS:
   * `shutdown()` called on a boot that has not reached admission yet would
   * otherwise find nothing to wait for, return having stopped nothing, and
   * leave behind the workers and the subscribed manager that boot went on to
   * start.
   */
  #bootAttempt?: Promise<void>;
  #bootAttemptSettled = false;
  /**
   * Where the NEXT sweep starts paging — the sweep's only state that outlives
   * one invocation.
   *
   * Zero means "from the newest suspension", which is right whenever the last
   * wake resumed something or reached the end of the set. It is non-zero only
   * after a wake spent its whole scan budget finding no markers: that wake
   * examined a prefix and proved it marker-free, so the next one continues
   * past it instead of re-reading it. Instance state rather than storage
   * because it is an optimisation, not a fact: an evicted DO simply starts
   * from the top again, which is correct, only slower.
   */
  #sweepScanFloor = 0;
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
    this.#manager = new BackgroundTaskManager({
      enabled: true,
      ...options.manager,
    });
    hostManagers.set(this, this.#manager);
    // Must precede init(): the manager reads its Mastra for storage, the
    // internal background-task workflow registration, and id generation.
    this.#manager.__registerMastra(options.mastra);
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
   *
   * A registration that FAILED is memoized only when it can only fail again:
   * the same non-memoized-refusal reasoning #ensureDispatching applies to the
   * fence. A transient storage fault on the first request into a fresh isolate
   * would otherwise be the permanent answer for the life of that isolate — the
   * read routes hang off this memo, so the host would keep 500ing a queue it
   * can now reach, until an eviction nobody can schedule.
   */
  boot(): Promise<void> {
    if (this.#shutdownRequested) {
      return Promise.reject(
        new Error('background-tasks: host is shutting down'),
      );
    }
    if (!this.#booted) {
      const registration: Promise<void> = this.#doRegister().catch(
        (error: unknown) => {
          // Identity-checked before clearing, the idiom `#ensureDispatching`
          // and `shutdown()` use: a later registration must never be cleared
          // by an earlier one's settlement.
          if (
            !(error instanceof BackgroundTasksConfigurationError) &&
            this.#booted === registration
          ) {
            this.#booted = undefined;
          }
          throw error;
        },
      );
      this.#booted = registration;
    }
    const registered = this.#booted;
    // Recorded before this method returns, so a `shutdown()` on the very next
    // statement already has the whole attempt to wait on.
    this.#bootAttemptSettled = false;
    const attempt = registered
      .then(() => this.#ensureDispatching())
      .finally(() => {
        this.#bootAttemptSettled = true;
      });
    this.#bootAttempt = attempt;
    return attempt;
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
        throw new BackgroundTasksConfigurationError(
          'background-tasks: execution requires DurableObjectWorkflowsStorageD1',
        );
      }
      const durableTasks = tasks as DurableObjectBackgroundTasksStorageD1;
      if (
        (durableTasks as unknown as Record<symbol, unknown>)[
          DURABLE_OBJECT_BACKGROUND_TASKS_D1
        ] !== true
      ) {
        throw new BackgroundTasksConfigurationError(
          'background-tasks: execution requires DurableObjectBackgroundTasksStorageD1',
        );
      }
    } else {
      await this.#warnIfBodiesCannotExecute();
    }
    for (const [toolName, executor] of Object.entries(this.#executors)) {
      this.#manager.registerStaticExecutor(toolName, this.#gated(executor));
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
  #ensureDispatching(): Promise<void> {
    if (this.#dispatching) return this.#dispatching;
    // DELIBERATELY NOT `async`, and the memo below is assigned before this
    // method's first await can exist.
    //
    // `#dispatching` alone cannot collapse concurrent callers, because it is
    // set only once the fence read RESUMES: two boots that both reach the
    // guard above before either read completes would both go on to assign it,
    // and both would run #startDispatching — `startWorkers()` twice and
    // `manager.init(pubsub)` twice, which subscribes handleDispatch TWICE, so
    // every dispatch is handled twice: double claim, double body execution.
    // `#managerNeedsShutdown`/`#workersNeedStop` are booleans, so teardown
    // would then stop one of the two. That is not hypothetical here: the
    // request path and `onAlarm()` call `boot()` independently (a host's own
    // memo collapses only the request path), so the two genuinely interleave.
    //
    // Recorded SYNCHRONOUSLY instead — the same shape `boot()` uses for
    // `#bootAttempt`, for the same reason — and cleared when it settles, so a
    // refusal still retries on the next boot.
    const inFlight = this.#dispatchAttempt;
    if (inFlight) return inFlight;
    const attempt: Promise<void> = this.#attemptDispatching().finally(() => {
      // Identity-checked before clearing, the idiom `shutdown()` uses below: a
      // later attempt must never be cleared by an earlier one's settlement.
      if (this.#dispatchAttempt === attempt) this.#dispatchAttempt = undefined;
    });
    this.#dispatchAttempt = attempt;
    return attempt;
  }

  async #attemptDispatching(): Promise<void> {
    const fence = await readExecutionFence(this.#executionFence);
    // The fence read is an await, and `boot()`'s shutdown guard ran before it.
    // A `shutdown()` that arrived in between has already decided this instance
    // is going away; subscribing and claiming for the duration of an in-flight
    // teardown would hand work to a manager that is about to stop — and
    // `#doShutdown` waits on this very attempt, so it would wait for the claim
    // it is trying to prevent.
    if (this.#shutdownRequested) return;
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
    this.#dispatching ??= this.#startDispatching().catch((error: unknown) => {
      // Not memoized on failure, for the same reason a refusal is not: the
      // next boot must be able to try again.
      this.#dispatching = undefined;
      throw error;
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
      await this.#manager.init(this.#pubsub);
    } catch (primary) {
      const cleanupErrors: unknown[] = [];
      if (this.#managerNeedsShutdown) {
        try {
          await this.#manager.shutdown();
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
    // The BOOT budget: enough to start moving, small enough that the request
    // that triggered this boot does not wait for the whole parked queue.
    // onAlarm() is where the rest drains.
    await this.#resumeFenceSuspendedTasks(MAX_FENCE_RESUMES_PER_PASS);
  }

  /**
   * Re-drive the tasks the executor backstop parked, once dispatching resumes.
   *
   * `recoverStaleTasks()` re-drives `running` and `pending` rows and knows
   * nothing about `suspended` ones, so without this a task the backstop saved
   * from destruction would be saved into a state nothing ever leaves. Scoped by
   * the backstop's own marker: a task suspended by its TOOL (awaiting a
   * webhook, say) is a different thing entirely and must stay suspended.
   *
   * BUDGETED, not one-shot. It runs on two lanes with two budgets: `boot()`
   * spends MAX_FENCE_RESUMES_PER_PASS so the first request after a reopen is
   * not stalled behind the whole parked queue, and `onAlarm()` spends
   * MAX_FENCE_RESUMES_PER_ALARM on a lane nothing is waiting behind. The alarm
   * lane is what makes this CONVERGE: one invocation keeps scanning until a
   * scan resumes nothing, and the alarm recurs, so a queue larger than one
   * alarm's budget still drains over the next few wakes with no eviction and no
   * operator action. (An earlier shape swept only from #startDispatching, which
   * runs once — every later boot returned the settled memo and never swept
   * again, so anything past the first cap stayed parked indefinitely.)
   *
   * PAGES, because the scan order alone cannot reach a stale cohort. Newest
   * suspensions first is right for the common case — the parked cohort is the
   * one that just parked — but leftovers from an earlier lock carry an OLD
   * `suspendedAt`, so after a lock/reopen/lock cycle plus a page-worth of newer
   * tool-suspends, page 0 would never contain them again. So: a page that
   * resumed nothing and came back FULL means the markers are deeper, and the
   * sweep advances; a page that resumed something restarts at the top, because
   * resuming removes rows from the set being paged and any fixed offset would
   * then skip rows. `#resumedIds` makes that restart cheap and terminating —
   * each pass strictly grows it.
   *
   * And the paging SURVIVES the wake, because within one wake it is bounded
   * (MAX_FENCE_RESUME_SCANS). A cohort sitting behind more newer tool-suspends
   * than one wake can page through would otherwise be stranded outright: every
   * wake would re-read the same marker-free prefix and stop in the same place.
   * So a wake that spends its whole scan budget finding nothing leaves
   * `#sweepScanFloor` where it stopped and the next one resumes there, while
   * any wake that resumes something — or reaches the end of the set — puts it
   * back to zero, since both mean the prefix is worth re-reading. A floor left
   * past the end of a shrunken set reads a short page and resets itself.
   *
   * The cost of that is bounded and the right way round: while the floor is
   * deep, a cohort parked FRESH at the top waits the few wakes it takes to page
   * to the end of the set (which resets the floor), instead of the old shape's
   * "waits forever". Delaying the reachable cohort by a wake or two is the
   * cheaper error than stranding the unreachable one permanently.
   *
   * `perPage`/`orderBy` are ADAPTER-DEPENDENT: honoured by
   * @mastra/cloudflare-d1's SQL builder and by this package's D1 domain, but an
   * adapter that ignores them returns the whole list — which is why the loop
   * counts its own resumes rather than trusting the page size to bound them.
   *
   * Answers how many it resumed, so the caller can log a lane that is making
   * progress distinctly from one that has drained.
   */
  async #resumeFenceSuspendedTasks(budget: number): Promise<number> {
    // Resuming is asynchronous at the storage layer (resume() publishes; the
    // handler is what writes 'running'), so a row can still read `suspended` on
    // the next scan. Without this the sweep could re-resume the same rows —
    // wasted publishes at best, and core throws outright once the row HAS
    // moved, which would abort the whole invocation.
    const resumedIds = new Set<string>();
    let page = this.#sweepScanFloor;
    // The floor the NEXT wake starts from. Any wake that resumed something has
    // changed the set it was paging, so its prefix is worth re-reading and the
    // floor goes back to zero whatever this is called with.
    const finish = (nextFloor: number): number => {
      this.#sweepScanFloor = resumedIds.size > 0 ? 0 : nextFloor;
      return resumedIds.size;
    };
    for (let scan = 0; scan < MAX_FENCE_RESUME_SCANS; scan += 1) {
      let tasks: readonly BackgroundTask[];
      try {
        ({ tasks } = await this.#manager.listTasks({
          status: 'suspended',
          orderBy: 'suspendedAt',
          orderDirection: 'desc',
          page,
          perPage: FENCE_RESUME_SCAN_PAGE,
        }));
      } catch (error) {
        this.#logFenceResumeFailure(error);
        // This page proved nothing, so the next wake retries it rather than
        // stepping over rows it never read.
        return finish(page);
      }
      let resumedThisScan = 0;
      for (const task of tasks) {
        if (resumedIds.size >= budget) return finish(0);
        if (resumedIds.has(task.id) || !isFenceParked(task)) continue;
        try {
          await this.#manager.resume(task.id);
        } catch (error) {
          // Expected at capacity: core refuses a resume that would exceed the
          // concurrency limit. Stopping is the honest response — the rows stay
          // parked and the next wake retries once slots free — and it keeps a
          // saturated deployment from burning the scan budget on refusals.
          this.#logFenceResumeFailure(error);
          return finish(page);
        }
        resumedIds.add(task.id);
        resumedThisScan += 1;
      }
      if (resumedThisScan > 0) {
        page = 0;
        continue;
      }
      // Nothing here to resume. A SHORT page is the end of the set, so there is
      // nothing deeper and this sweep has drained. A full one means the markers
      // may simply be further down.
      if (tasks.length < FENCE_RESUME_SCAN_PAGE) return finish(0);
      page += 1;
    }
    // Out of scans with nothing found: `page` is the first row range this wake
    // never read, and the next one starts there.
    return finish(page);
  }

  #logFenceResumeFailure(error: unknown): void {
    // Best effort: a failure here leaves the rows suspended and inspectable,
    // which is where they already were. Never fails the boot that was about to
    // start serving, nor the alarm duty that follows.
    console.error(
      JSON.stringify({
        type: 'background-tasks.fence-resume-error',
        reason: error instanceof Error ? error.message : String(error),
      }),
    );
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
        const fence = await readExecutionFence(this.#executionFence);
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
    const fence = await readExecutionFence(this.#executionFence);
    if (!admitsDrainableExecution(fence)) {
      throw new ExecutionFencedError(fence.state, 'background task enqueue');
    }
    return this.#manager.enqueue(
      payload,
      context
        ? { ...context, executor: this.#gated(context.executor) }
        : undefined,
    );
  }

  // The read surface, and only the read surface. These three are what
  // `createBackgroundTaskRoutes` serves and what a host inspecting its own
  // queue needs; they claim nothing, so they stay open in every fence state
  // (the semantics matrix keeps reads answering through a lock — an operator
  // proving a deployment is drained has to be able to look).

  /** One task by id, or null. */
  getTask(taskId: string): Promise<BackgroundTask | null> {
    return this.#manager.getTask(taskId);
  }

  /** Tasks matching a filter. The route adapter re-checks scope per row. */
  listTasks(filter?: TaskFilter): Promise<TaskListResult> {
    return this.#manager.listTasks(filter);
  }

  /** Lifecycle-event stream, for the route adapter's SSE response. */
  stream(
    options?: Parameters<BackgroundTaskManager['stream']>[0],
  ): ReadableStream<Record<string, unknown>> {
    return this.#manager.stream(options);
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
    // Teardown wins over the duty. A `shutdown()` that arrived while boot() was
    // in flight got past its guard, so without this the alarm would sweep
    // mid-teardown — publishing resumes onto a manager that is stopping — and
    // then call cleanup() on a manager that has already shut down.
    if (this.#shutdownRequested) return;
    // The convergence lane. boot() has resolved, so a set `#dispatching` means
    // init actually landed and resume() has a subscriber to publish to. Nothing
    // is waiting behind an alarm, so this lane carries the larger budget and is
    // what drains a parked queue the boot budget only dented — including on the
    // very wake that admitted dispatching, since the alarm recurs regardless.
    //
    // Convergence therefore rides the same alarm the host already arms for the
    // cleanup below; a host that arms none gets neither duty.
    if (this.#dispatching) {
      await this.#resumeFenceSuspendedTasks(MAX_FENCE_RESUMES_PER_ALARM);
    }
    await this.#manager.cleanup();
  }

  /**
   * Graceful teardown. Stop the manager first: its shutdown flag rejects new
   * enqueues synchronously before the workflow consumers disappear. Worker
   * teardown is still attempted if manager teardown fails.
   */
  async #doShutdown(): Promise<void> {
    // ONE wait, on the whole boot attempt: registration, the fence read, and
    // phase B all hang off it, so nothing a racing boot is about to start can
    // slip past this point and outlive the shutdown. Waiting on `#dispatching`
    // instead would miss exactly the window the fence read opens — it is unset
    // until admission, so a shutdown that arrived during the read would stop
    // nothing and return.
    //
    // Awaited ONLY while still in flight: manager.shutdown() below flips its
    // enqueue guard before its first await, and that guarantee survives only if
    // nothing suspends this function before it is invoked.
    if (this.#bootAttempt && !this.#bootAttemptSettled) {
      try {
        await this.#bootAttempt;
      } catch {
        // #doRegister only validates and registers, and #startDispatching
        // already unwound every component it managed to stop. Any component
        // whose cleanup failed remains flagged for the retry below.
      }
    }
    const errors: unknown[] = [];
    if (this.#managerNeedsShutdown) {
      try {
        await this.#manager.shutdown();
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
