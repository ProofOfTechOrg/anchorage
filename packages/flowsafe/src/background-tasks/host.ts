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
// SUBSTRATE LIMITATIONS + THE P9 UNBLOCK CONTRACT (spike B-S1). Durable
// background-task EXECUTION does not run on the Cloudflare substrate yet. Core's
// `__background-task` workflow runs on the EVENTED execution engine; the items
// below block it, and each MUST be closed BEFORE execution is enabled — the
// first two make dispatch impossible, R-B3 is a SILENT tenant-isolation leak
// that only surfaces once R-B1 is fixed.
//
//   R-B1 — the evented engine refuses to `createRun` unless the workflows store
//   reports `supportsConcurrentUpdates()` (chunk-JGDMZZAO.js
//   `ATOMIC_STORAGE_OPERATIONS_NOT_SUPPORTED`). At @mastra/cloudflare-d1 1.1.1
//   `WorkflowsStorageD1.supportsConcurrentUpdates()` returns **false** AND its
//   `updateWorkflowResults`/`updateWorkflowState` are UNIMPLEMENTED THROWS
//   ("D1 does not support atomic read-modify-write operations…", index.cjs). So
//   R-B1 is NOT a flag to flip: overriding `supportsConcurrentUpdates` to true
//   passes core's gate and then THROWS on the first per-step update, stranding
//   the task at `running` (reproduced). Closing R-B1 is a P9 adapter that
//   IMPLEMENTS atomic partial-updates — the DO's single-threaded lease makes that
//   implementation safe (serialized read-modify-write), it does NOT make an
//   override safe. `InMemoryStore`/libsql return true, so the manager, the
//   recovery seam, and the dispatch->complete path are real on a concurrent-update
//   store — but see R-B2.
//
//   R-B2 — even on a concurrent-update store the evented engine drives step
//   progress through workers the bare manager does not stand up. The host must
//   ALSO call `mastra.startWorkers()`. R-B1 and R-B2 close TOGETHER: a store that
//   persists concurrent updates with no workers running still never completes a
//   body.
//
//   R-B3 — LATENT tenant-isolation residual, inert TODAY only because R-B1 blocks
//   execution (no `__background-task` snapshot row is ever written). Core keys the
//   internal `__background-task` run by the UNSALTED `taskId`
//   (`mastra.generateId()`, chunk-PPPKTVCG.js; then `createRun({ runId: taskId })`).
//   That run's `mastra_workflow_snapshot` row therefore has an UNSALTED `run_id`,
//   so it ESCAPES both tenant offboarding (`purgeTenant` reaps
//   `mastra_workflow_snapshot` over the salted `[tid_, tid\x60)` `run_id` range)
//   AND, while suspended (non-terminal), `purgeExpiredWorkflowRuns`
//   (terminal-status-only). The task's OWN `mastra_background_tasks` row is fine —
//   it carries the salted parent `run_id` and purgeTenant covers it — but the
//   internal engine run it spawns does not. The moment a P9 concurrent-update
//   adapter lands (R-B1 closed), this goes LIVE and SILENT: an offboarded /
//   right-to-be-forgotten tenant's background-task engine runs would leak.
//   Unblocking execution REQUIRES closing R-B3 in the SAME change — salt the
//   internal `__background-task` runId with the parent tenant, or reap the
//   snapshot by the task's salted originating `run_id`. An enforcement guard
//   (background-tasks/d1-storage.test.ts) FAILS CI the instant
//   `supportsConcurrentUpdates()` returns true, so execution cannot be enabled
//   without closing R-B3.
//
//   L3 — core's `recoverStaleTasks()` (fired by `init()` below) is TENANT-BLIND:
//   it lists ALL 'running' tasks and re-dispatches ALL 'pending' tasks with no
//   tenant filter (chunk-PPPKTVCG.js). Harmless while a DO hosts one tenant, but
//   a multi-tenant manager would recover across tenants; closing this needs a
//   tenant filter or a host-topology revisit before execution is enabled.
//
// Everything else Track B adds works on D1 regardless of all the above: the
// `mastra_background_tasks` PERSISTENCE domain, tenant-range purge + TTL cleanup,
// the tenant-bound read routes, and the `_background` defense. `boot()` WARNS
// (once) when the store cannot execute bodies so R-B1 is loud rather than a
// cryptic async throw at first dispatch.

import {
  BackgroundTaskManager,
  type BackgroundTaskManagerConfig,
  type ToolExecutor,
} from '@mastra/core/background-tasks';
import type { Mastra } from '@mastra/core/mastra';

import type { HostPubSub } from '../do-runner/index.js';
import { backgroundTasksStore } from './d1-storage.js';

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
  #booted?: Promise<void>;

  constructor(options: BackgroundTaskHostOptions) {
    this.#mastra = options.mastra;
    this.#pubsub = options.pubsub;
    this.#executors = options.executors;
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
    await backgroundTasksStore(this.#mastra);
    // Surface R-B1 loudly: a store whose workflows domain cannot do concurrent
    // updates (e.g. @mastra/cloudflare-d1) will let init()/recovery run and rows
    // persist, but a dispatched task's evented workflow cannot execute — warn
    // once so an operator learns it here, not from a stray async throw.
    await this.#warnIfBodiesCannotExecute();
    for (const [toolName, executor] of Object.entries(this.#executors)) {
      this.manager.registerStaticExecutor(toolName, executor);
    }
    await this.manager.init(this.#pubsub);
  }

  async #warnIfBodiesCannotExecute(): Promise<void> {
    const workflows = await this.#mastra.getStorage()?.getStore('workflows');
    // Optional-chained: a store that predates the capability method reads as
    // "cannot" (fail loud, never silently assume support).
    const supports = workflows?.supportsConcurrentUpdates?.() ?? false;
    if (!supports) {
      console.warn(
        'background-tasks: the workflows storage domain does not support concurrent updates, so DISPATCHED task bodies cannot execute (core runs them on the evented engine). Persistence, recovery, purge, and the read routes still work; execution needs a concurrent-update adapter (e.g. libsql). @mastra/cloudflare-d1 does not yet qualify — see R-B1.',
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
    await this.manager.shutdown();
  }
}
