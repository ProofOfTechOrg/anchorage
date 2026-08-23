// SPDX-License-Identifier: Apache-2.0
// init()-based import-swap, the pattern @mastra/inngest and @mastra/temporal
// ship: init(...) returns backend-bound createWorkflow/createStep imported in
// place of the @mastra/core/workflows versions, leaving workflow definition
// code unchanged. Like @mastra/temporal, createStep passes through unmodified;
// createWorkflow registers the workflow on the runner runtime.
//
// init takes the Workers env (conventional `DB` D1 binding) or an explicit
// { storage } (tests, other adapters) — rather than raw env only — because
// binding names are deploy-specific and Workers env objects only exist inside
// handlers/DO constructors, never at module scope.

import type { MastraCompositeStore } from '@mastra/core/storage';
import type { AnyWorkflow } from '@mastra/core/workflows';
import {
  createWorkflow as coreCreateWorkflow,
  createStep,
} from '@mastra/core/workflows';

import type { D1DatabaseBinding } from './cf-types.js';
import { createD1Storage } from './d1-storage.js';
import type {
  ExecutionFenceDatabase,
  ExecutionFenceWiring,
} from './execution-fence.js';
import { ExecutionFenceStore } from './execution-fence.js';
import type { HostPubSub } from './pubsub.js';
import type { RequestContextProvider } from './runtime.js';
import { RunnerRuntime } from './runtime.js';

/** Workers env shape init() understands directly. */
export interface DORunnerEnv {
  DB: D1DatabaseBinding;
}

/** Explicit storage takes precedence over a DB binding when both are present. */
export type InitSource = { storage: MastraCompositeStore } | DORunnerEnv;

export interface InitOptions {
  /** Storage instance id when init builds the D1 store. Default: 'flowsafe'. */
  id?: string;
  /** Table name prefix when init builds the D1 store. */
  tablePrefix?: string;
  /**
   * Server-side requestContext source consulted on every start/resume — the
   * grant-minting seam. See RequestContextProvider in runtime.ts.
   */
  requestContextForRun?: RequestContextProvider;
  /**
   * The host Durable Object's single pubsub identity — `createHostPubSub()` for the
   * default in-process emitter, or any PubSub the host built. init() is where a
   * DO's identity is established: it echoes the instance on InitResult, so every
   * consumer in the isolate takes THAT one rather than building its own (core
   * defaults a fresh emitter per createRun, and two such feeds never see each
   * other's events — see pubsub.ts).
   *
   * OPT-IN: absent, InitResult.pubsub is undefined and no consumer has one to
   * pass, so the host is byte-identical to before this seam existed (polling
   * stays the fallback).
   */
  pubsub?: HostPubSub;
  /**
   * The deployment execution fence (do-runner/execution-fence.ts).
   *
   * OPTIONAL only for a `{ DB }` source, where init builds one from that same
   * binding — the fence must live in the database it fences, and init is the
   * one place that holds both. Passing a store here overrides the auto-build
   * (a host sharing one instance across its Durable Objects). There is
   * deliberately NO `'none'` on this branch: a host that hands init a database
   * cannot end up with a fence-less runtime, whatever it forgets or opts out
   * of, which is the fail-closed-by-construction half of the contract.
   *
   * REQUIRED, and widened to include the opt-out, for a `{ storage }` source —
   * see ExecutionFenceWiring, which spells out why every fence option that CAN
   * be required is.
   */
  executionFence?: ExecutionFenceStore;
}

/**
 * InitOptions for a `{ storage }` source: the fence wiring is mandatory, and
 * widened to admit the opt-out. Written as an intersection rather than an
 * `extends`, because a subtype may not WIDEN an inherited property's type —
 * and the widening is the point: `'none'` exists only on this branch.
 */
export type StorageInitOptions = Omit<InitOptions, 'executionFence'> & {
  executionFence: ExecutionFenceWiring;
};

export interface InitResult {
  createWorkflow: typeof coreCreateWorkflow;
  createStep: typeof createStep;
  runtime: RunnerRuntime;
  /**
   * The host pubsub identity, or undefined when unconfigured. THE accessor for
   * it: consumers take the identity from here instead of constructing their own,
   * which is what keeps it single per DO.
   */
  pubsub?: HostPubSub;
  /**
   * The deployment execution fence, or undefined for an explicitly unfenced
   * host. THE accessor for it, for the same reason as `pubsub`: a route that
   * built its own store could be gating a different database than the runtime
   * it sits in front of. Thread-DO signal routes read it off `scope.init`.
   */
  executionFence?: ExecutionFenceStore;
}

export function init(source: DORunnerEnv, options?: InitOptions): InitResult;
export function init(
  source: { storage: MastraCompositeStore },
  options: StorageInitOptions,
): InitResult;
/**
 * A source whose shape is only known at runtime (a host that accepts either).
 * The wiring is REQUIRED here for the same reason it is on the `{ storage }`
 * branch: the compiler cannot tell which branch this call will take, so it
 * cannot know whether init would have built a fence. `'none'` still applies
 * only to a `{ storage }` source — a `{ DB }` one is fenced regardless, so
 * choosing the opt-out cannot leave a database-backed runtime unfenced.
 */
export function init(
  source: InitSource,
  options: StorageInitOptions,
): InitResult;
export function init(
  source: InitSource,
  options: Omit<InitOptions, 'executionFence'> & {
    executionFence?: ExecutionFenceWiring;
  } = {},
): InitResult {
  let storage: MastraCompositeStore;
  let executionFence: ExecutionFenceStore | undefined;
  if ('storage' in source) {
    if (options.id !== undefined || options.tablePrefix !== undefined) {
      // Silently ignoring these would mask a misconfiguration: they only
      // apply when init builds the D1 store itself.
      throw new Error(
        "init: 'id'/'tablePrefix' apply only when init builds the D1 store from env.DB — configure the passed storage instance directly",
      );
    }
    storage = source.storage;
    executionFence =
      options.executionFence === 'none' ? undefined : options.executionFence;
  } else {
    storage = createD1Storage({
      binding: source.DB,
      id: options.id,
      tablePrefix: options.tablePrefix,
    });
    // Fail-closed by construction: a host that hands init a database gets a
    // fenced runtime whether or not it remembered to ask for one, and the
    // option's type on this branch admits no opt-out. The cast is the same
    // boundary widening createD1Storage makes on the identical value
    // (d1-storage.ts) — D1DatabaseBinding deliberately types `prepare` as
    // returning `unknown` so the shared env shape needs no statement type.
    // The overload for this source types `executionFence` as a store, so the
    // opt-out cannot be written here; the narrowing is what makes that visible
    // to the implementation signature, which sees both branches' options.
    const configured = options.executionFence;
    executionFence =
      configured === undefined || configured === 'none'
        ? new ExecutionFenceStore(
            source.DB as unknown as ExecutionFenceDatabase,
          )
        : configured;
  }
  const runtime = new RunnerRuntime({
    storage,
    requestContextForRun: options.requestContextForRun,
    // Threaded directly because every DO subclass
    // returns THIS runtime from build(), so a host that configures a pubsub
    // reaches the runtime's createRun sites with no host change (Track A wires
    // those). Handing it only to InitResult would strand it — build() returns a
    // RunnerRuntime, not an InitResult, so the run-DO path would drop it.
    pubsub: options.pubsub,
    // Same reasoning, and the reason the fence is a construction-time argument
    // rather than a per-call one: the runtime IS the closure guarantee, so it
    // must not be possible to reach start()/resume() with the fence left off.
    executionFence,
  });

  // Cast preserves core's generic call-site inference (6 type params); the
  // wrapper body is inference-erased but only registers and returns.
  const boundCreateWorkflow = ((
    params: Parameters<typeof coreCreateWorkflow>[0],
  ) => {
    const workflow = coreCreateWorkflow(params);
    runtime.register(workflow as AnyWorkflow);
    return workflow;
  }) as typeof coreCreateWorkflow;

  return {
    createWorkflow: boundCreateWorkflow,
    createStep,
    runtime,
    pubsub: options.pubsub,
    executionFence,
  };
}
