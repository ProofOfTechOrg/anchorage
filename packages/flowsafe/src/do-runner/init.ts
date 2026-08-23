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
import type {
  StartIdempotencyDatabase,
  StartIdempotencyStore,
  StartIdempotencyWiring,
} from './start-idempotency.js';
import { startIdempotencyFor } from './start-idempotency.js';

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
  /**
   * The deployment's start reservations, for a `{ storage }` source only.
   *
   * A `{ DB }` source ignores this and always gets the store built from its own
   * binding — the reservation table must live in the database the runs live in,
   * and init is the one place that holds both.
   *
   * OPTIONAL on THIS branch only, where it is ignored. `StorageInitOptions`
   * requires it, and widens it to include the opt-out — see there for why a
   * `{ storage }` host must write the answer down.
   */
  startIdempotency?: StartIdempotencyWiring;
}

/**
 * The reservation store this runtime settles against.
 *
 * A `{ DB }` source gets one built from its own binding, with no option and no
 * opt-out — the reservation table must live in the database the runs live in,
 * and init is the one place that holds both, so there is no third answer for a
 * host to get wrong. A `{ storage }` source has no binding to derive one from,
 * so it gets whatever it was handed and `undefined` otherwise.
 *
 * `startIdempotencyFor` is the per-binding memo, so the store the runtime
 * settles against and the store the run router reserved into are the same
 * object whenever both were built from the same `env.DB`.
 */
function startIdempotencyForSource(
  source: InitSource,
  configured: StartIdempotencyWiring | undefined,
): StartIdempotencyStore | undefined {
  if ('storage' in source) {
    return configured === undefined || configured === 'none'
      ? undefined
      : configured;
  }
  // The same boundary widening createD1Storage makes on this identical value:
  // D1DatabaseBinding types `prepare` as returning `unknown` so the shared env
  // shape needs no statement type.
  return startIdempotencyFor(source.DB as unknown as StartIdempotencyDatabase);
}

/**
 * InitOptions for a `{ storage }` source: the fence wiring AND the reservation
 * wiring are mandatory, and both are widened to admit the opt-out. Written as
 * an intersection rather than an `extends`, because a subtype may not WIDEN an
 * inherited property's type — and the widening is the point: `'none'` exists
 * only on this branch.
 *
 * `startIdempotency` is required HERE and optional on `InitOptions` because the
 * two branches fail differently. A `{ DB }` host cannot get this wrong: init
 * builds the store from the binding, and `DurableObjectRunner.build` refuses to
 * serve a runtime that lacks one while a DB binding is present. A `{ storage }`
 * host has no binding to derive one from, and its failure is SILENT and split:
 * such a host can still wire a real store into its run router (the router takes
 * its own), so keys reserve and claim normally — and then nothing ever settles
 * them, because the runtime that sees every terminal transition was never given
 * the store. The reservation stays `started` forever, and the next retry of
 * that key is told UNRESOLVABLE instead of replaying a run that completed
 * perfectly well. Making the host WRITE `'none'` turns that into a decision
 * someone made; nothing about an omission could have said it.
 */
export type StorageInitOptions = Omit<
  InitOptions,
  'executionFence' | 'startIdempotency'
> & {
  executionFence: ExecutionFenceWiring;
  startIdempotency: StartIdempotencyWiring;
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
  /**
   * The deployment's start reservations, or undefined for a `{ storage }` host
   * with no database to reserve against. THE accessor for it, for the same
   * reason as `executionFence`: a surface that built its own store could be
   * reserving into a different database than the runtime that settles.
   */
  startIdempotency?: StartIdempotencyStore;
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
  const startIdempotency = startIdempotencyForSource(
    source,
    options.startIdempotency,
  );
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
    // Same construction-time reasoning: the runtime is the one layer that sees
    // every terminal transition, so it must not be possible to build one that
    // executes runs but cannot mark their reservations spent.
    startIdempotency,
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
    startIdempotency,
  };
}
