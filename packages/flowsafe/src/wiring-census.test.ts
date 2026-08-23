// SPDX-License-Identifier: Apache-2.0
// The WIRING CENSUS: every published option type that carries a
// deployment-wide control must carry it as a REQUIRED field whose type
// includes the written opt-out.
//
// Two controls are censused here — the execution fence and the start
// reservation — in one file because they share the property that makes a
// census necessary at all.
//
// Why a census rather than a convention. Each control is only as good as its
// least-wired surface, and a missing one is invisible in exactly the way a
// wrong one is not: an unfenced router behaves identically to a fenced one
// until the day an operator closes the fence, and a router whose reservation
// store was never wired answers keyed starts exactly like a wired one right up
// to the moment a retry needed the reservation. No test written against a
// healthy deployment can catch either. What CAN catch them is the type — a
// required field is one a host cannot forget, and a union that includes the
// opt-out is one whose absence a host has to write down. This file asserts that
// property holds for every leaf at once, so a new surface cannot quietly ship
// with `executionFence?:` or `startIdempotency?:`.
//
// It is a COMPILE-TIME test. The assertions below are type aliases; `tsc`
// (pnpm --filter @proofoftech/flowsafe typecheck) is what evaluates them, and
// the negative controls are `@ts-expect-error`s — which this repo's config
// turns into errors when they are UNUSED, so a tsc that exits 0 has proved both
// directions.
//
// ADDING A SURFACE: if you add an exported `*Options` type (or any published
// wiring shape) with an `executionFence` or `startIdempotency` field, add it to
// the matching leaf list below. Nothing here can discover it for you.
//
// NOT in scope: the internal shapes that thread ONE already-taken reading down
// a request (`executionFence: ExecutionFenceReading`, thread-do-routes). Those
// carry an observation, not a wiring — the surface that took the reading is the
// leaf, and it is in the census.

import { describe, expect, it } from 'vitest';

import type { AgentThreadTopologyOptions } from './agent-host/index.js';
import type { ApprovalServiceOptions } from './approval-api/index.js';
import type { BackgroundTaskHostOptions } from './background-tasks/index.js';
import type {
  ExecutionFenceStore,
  ExecutionFenceWiring,
  InitOptions,
  RunnerRuntimeOptions,
  StartIdempotencyStore,
  StartIdempotencyWiring,
  StorageInitOptions,
} from './do-runner/index.js';
import { readExecutionFence } from './do-runner/index.js';
import type { ObjectiveRouterOptions } from './goals/index.js';
import type {
  HostApprovalServiceOptions,
  RunRouterOptions,
  RunRouterStartIdempotency,
} from './host-kit/index.js';
import type {
  ScheduleRouterOptions,
  ScheduleTickOptions,
} from './schedules/index.js';
import type {
  SignalProviderHostWiring,
  WebhookRouterOptions,
} from './signal-providers/index.js';
import type { NotificationDispatchTickOptions } from './signals/index.js';

/** Accepts only `true`; anything else is a compile error at the use site. */
type Assert<T extends true> = T;

/**
 * Exact type identity, not mutual assignability. The two differ here in the way
 * that matters: `ExecutionFenceStore` IS assignable to `ExecutionFenceWiring`,
 * so an assignability check would pass a leaf that dropped the `'none'` arm and
 * left every database-less host without a way to say so.
 */
type Equals<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;

// ---------------------------------------------------------------------------
// The execution fence
// ---------------------------------------------------------------------------

/**
 * Does `T` carry the fence the way every leaf must?
 *
 * The `extends { executionFence: infer F }` does double duty. A type whose
 * field is OPTIONAL does not satisfy a required one, so it fails here without a
 * separate requiredness test; a type with no fence at all fails for the same
 * reason. `F` is then compared exactly, which is what rejects a leaf typed as
 * the store alone.
 */
type FenceWiringLeafOk<T> = T extends { executionFence: infer F }
  ? Equals<F, ExecutionFenceWiring>
  : false;

/**
 * THE FENCE CENSUS. Every exported shape that names a fence, checked in one
 * place.
 *
 * Exported because an unused local declaration is itself a lint error here, and
 * nothing consumes this one: it is checked by being written. It erases
 * entirely, and the file is a test, so it reaches no published surface.
 */
export type ExecutionFenceWiringLeaves = [
  // The last leaf to be required — the service that COMMITS a decision and only
  // then resumes it.
  Assert<FenceWiringLeafOk<ApprovalServiceOptions>>,
  // The composer every fenced host builds that service through.
  Assert<FenceWiringLeafOk<HostApprovalServiceOptions>>,
  // Task bodies run below RunnerRuntime, so this is their only gate.
  Assert<FenceWiringLeafOk<BackgroundTaskHostOptions>>,
  // The `{ storage }` init branch, where init cannot build a fence itself.
  Assert<FenceWiringLeafOk<StorageInitOptions>>,
  // Authoring and claiming schedule work.
  Assert<FenceWiringLeafOk<ScheduleRouterOptions>>,
  Assert<FenceWiringLeafOk<ScheduleTickOptions>>,
  // Notification dispatch — queued work a drain still finishes.
  Assert<FenceWiringLeafOk<NotificationDispatchTickOptions>>,
  // Objectives: standing configuration that arms future work.
  Assert<FenceWiringLeafOk<ObjectiveRouterOptions>>,
  // Provider ingress and the provider poller behind it.
  Assert<FenceWiringLeafOk<WebhookRouterOptions>>,
  Assert<FenceWiringLeafOk<SignalProviderHostWiring>>,
  // The agent surface's keyed start, which re-binds a proof-only fence on a
  // replay. The census's own instruction demanded this one and did not have it.
  Assert<FenceWiringLeafOk<AgentThreadTopologyOptions>>,
  // The run router's keyed start, same job. Censused through the object arm
  // because the wiring itself is a union with `'none'`, and the `'none'` arm
  // carries no fields at all — see RunRouterStartIdempotencyOk below.
  Assert<FenceWiringLeafOk<Exclude<RunRouterStartIdempotency, 'none'>>>,
];

/**
 * The two published shapes that deliberately do NOT take the fence wiring,
 * pinned as exceptions rather than left to be read as omissions.
 *
 * `InitOptions` is the `{ DB }` branch: init builds the fence from that binding
 * itself, so the option is an OVERRIDE and admits no `'none'` — a host that
 * hands init a database cannot end up unfenced. `RunnerRuntimeOptions` is
 * construction-time state for a runtime that init always supplies, never a host
 * surface. Both are typed as the store for that reason.
 *
 * If either is ever tightened to the required wiring, MOVE its entry up into
 * the census; do not delete the check.
 */
export type DeliberateFenceWiringExceptions = [
  Assert<Equals<FenceWiringLeafOk<InitOptions>, false>>,
  Assert<Equals<FenceWiringLeafOk<RunnerRuntimeOptions>, false>>,
];

/**
 * The negative controls: the three shapes the fence census exists to reject.
 * Each is an `@ts-expect-error`, so an assertion that stopped biting would
 * surface as an UNUSED expect-error — an error in this repo's configuration —
 * rather than as a census that silently passes everything.
 */
type OptionalFence = { executionFence?: ExecutionFenceWiring };
type StoreOnlyFence = { executionFence: ExecutionFenceStore };
type NoFence = { store: string };

export type ExecutionFenceWiringRejections = [
  // @ts-expect-error an OPTIONAL fence is exactly what the census forbids
  Assert<FenceWiringLeafOk<OptionalFence>>,
  // @ts-expect-error a store-only fence leaves a database-less host no opt-out
  Assert<FenceWiringLeafOk<StoreOnlyFence>>,
  // @ts-expect-error and a shape with no fence at all is not a wired leaf
  Assert<FenceWiringLeafOk<NoFence>>,
];

// ---------------------------------------------------------------------------
// The start reservation
// ---------------------------------------------------------------------------

/**
 * Does `T` carry the reservation wiring the way every leaf must?
 *
 * TWO properties, not one exact type — and that is the difference from the
 * fence, not a weaker rule. The fence's leaves all carry the same shape; the
 * reservation's do not. The agent topology takes the bare
 * `StartIdempotencyWiring`, while the run router's arm BUNDLES the store with
 * the liveness probe its replay decision cannot be made without, and demanding
 * one exact type would force one of those two to be wrong.
 *
 * What must hold for both:
 *
 *   REQUIRED. `T extends { startIdempotency: infer F }` fails for an optional
 *   field — a required property must be declared — and for a missing one, so
 *   requiredness needs no separate test.
 *
 *   THE OPT-OUT IS IN THE TYPE. `'none' extends F` is what makes a host with no
 *   store say so; without it, "no reservations here" would have no spelling and
 *   the field would have to go back to being optional.
 *
 *   NO `undefined`. Checked separately, because an explicitly `| undefined`
 *   field is required at the type level yet reachable by writing nothing
 *   meaningful — the same hole an optional field opens, spelled differently.
 */
type StartIdempotencyLeafOk<T> = T extends { startIdempotency: infer F }
  ? 'none' extends F
    ? Equals<Extract<F, undefined>, never>
    : false
  : false;

/**
 * THE RESERVATION CENSUS. Exported for the same reason the fence's list is.
 *
 * Both keyed-start surfaces are here and nothing else is, because these are the
 * only two places a caller's idempotency key enters the system. Everything else
 * that touches the table — the runtime that settles, the purge that reaps —
 * receives a store it was constructed with rather than being wired for one.
 */
export type StartIdempotencyWiringLeaves = [
  // The workflow surface: POST /runs with an idempotencyKey.
  Assert<StartIdempotencyLeafOk<RunRouterOptions>>,
  // The agent surface: a trusted seam starting an agent run under a key.
  Assert<StartIdempotencyLeafOk<AgentThreadTopologyOptions>>,
  // The `{ storage }` init branch, where init cannot build a store itself.
  Assert<StartIdempotencyLeafOk<StorageInitOptions>>,
];

/**
 * Whatever shape a leaf's reservation wiring takes, the STORE inside it must be
 * the real one and the opt-out must be the written `'none'`. Checked separately
 * because `StartIdempotencyLeafOk` deliberately does not compare F exactly.
 */
export type RunRouterStartIdempotencyOk = [
  Assert<
    Equals<
      Exclude<RunRouterStartIdempotency, 'none'>['store'],
      StartIdempotencyStore
    >
  >,
  Assert<Equals<Extract<RunRouterStartIdempotency, 'none'>, 'none'>>,
];

/**
 * The two published shapes that deliberately do NOT take the reservation
 * wiring. Their rationale is NOT the fence's, and copying the fence's comment
 * here would have been false.
 *
 * `InitOptions` is the `{ DB }` branch, where the option is IGNORED rather than
 * overridden: init always builds the store from that binding, because the
 * reservation table has to live in the database the runs live in. There is no
 * third answer for a host to choose, so there is nothing for it to write. (The
 * fence's `{ DB }` option, by contrast, IS honoured as an override — a host may
 * share one fence store across its Durable Objects.)
 *
 * `RunnerRuntimeOptions` is construction-time state for a runtime init always
 * supplies, never a host surface — the one point the two exceptions share.
 *
 * The `{ storage }` branch is deliberately NOT an exception any more: it is in
 * the census above, because that is the one branch where a host CAN wire a
 * store into its router and still leave the runtime unable to settle what the
 * router reserved.
 */
export type DeliberateStartIdempotencyExceptions = [
  Assert<Equals<StartIdempotencyLeafOk<InitOptions>, false>>,
  Assert<Equals<StartIdempotencyLeafOk<RunnerRuntimeOptions>, false>>,
];

/** The three shapes the reservation census exists to reject. */
type OptionalStartIdempotency = { startIdempotency?: StartIdempotencyWiring };
type StoreOnlyStartIdempotency = { startIdempotency: StartIdempotencyStore };
type UndefinedableStartIdempotency = {
  startIdempotency: StartIdempotencyWiring | undefined;
};

export type StartIdempotencyWiringRejections = [
  // @ts-expect-error an OPTIONAL reservation wiring is what the census forbids
  Assert<StartIdempotencyLeafOk<OptionalStartIdempotency>>,
  // @ts-expect-error a store-only field leaves a store-less host no opt-out
  Assert<StartIdempotencyLeafOk<StoreOnlyStartIdempotency>>,
  // @ts-expect-error and `| undefined` is an optional field wearing a disguise
  Assert<StartIdempotencyLeafOk<UndefinedableStartIdempotency>>,
];

describe('wiring census', () => {
  it('resolves a written opt-out to the same reading an absent fence gives', async () => {
    // #given — the two ways a surface ends up unfenced: a leaf that WROTE the
    // opt-out the census forces it to write, and an internal seam that simply
    // holds nothing.
    // #when — both are resolved through the one resolver every gate uses.
    const written = await readExecutionFence('none');
    const absent = await readExecutionFence(undefined);

    // #then — identical readings. This is the premise the whole census rests
    // on: requiring the field costs a database-less host nothing but the words.
    expect(written).toEqual(absent);
    expect(written).toEqual({ state: 'open' });
  });
});
