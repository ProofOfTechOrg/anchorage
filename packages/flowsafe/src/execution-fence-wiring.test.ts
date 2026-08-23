// SPDX-License-Identifier: Apache-2.0
// The execution-fence wiring CENSUS: every published option type that carries
// a fence must carry it as a REQUIRED ExecutionFenceWiring.
//
// Why a census rather than a convention. The fence is only as good as its
// least-wired surface, and a missing fence is invisible in exactly the way a
// wrong one is not: an unfenced router, tick, or service behaves identically to
// a fenced one until the day an operator closes the fence, so no test written
// against an open deployment can catch it. What CAN catch it is the type — a
// required field is one a host cannot forget, and a union that includes 'none'
// is one whose opt-out a host has to write down. This file asserts that
// property holds for every leaf at once, so a new surface cannot quietly ship
// with `executionFence?:`.
//
// It is a COMPILE-TIME test. The assertions below are type aliases; `tsc`
// (pnpm --filter @proofoftech/flowsafe typecheck) is what evaluates them, and
// the negative controls are `@ts-expect-error`s — which this repo's config
// turns into errors when they are UNUSED, so a tsc that exits 0 has proved both
// directions.
//
// ADDING A SURFACE: if you add an exported `*Options` type (or any published
// wiring shape) with an `executionFence` field, add it to
// `ExecutionFenceWiringLeaves` below. Nothing here can discover it for you.
//
// NOT in scope: the internal shapes that thread ONE already-taken reading down
// a request (`executionFence: ExecutionFenceReading`, thread-do-routes). Those
// carry an observation, not a wiring — the surface that took the reading is the
// leaf, and it is in the census.

import { describe, expect, it } from 'vitest';

import type { ApprovalServiceOptions } from './approval-api/index.js';
import type { BackgroundTaskHostOptions } from './background-tasks/index.js';
import type {
  ExecutionFenceStore,
  ExecutionFenceWiring,
  InitOptions,
  RunnerRuntimeOptions,
  StorageInitOptions,
} from './do-runner/index.js';
import { readExecutionFence } from './do-runner/index.js';
import type { ObjectiveRouterOptions } from './goals/index.js';
import type { HostApprovalServiceOptions } from './host-kit/index.js';
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
 * THE CENSUS. Every exported shape that names a fence, checked in one place.
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
];

/**
 * The two published shapes that deliberately do NOT take the wiring, pinned as
 * exceptions rather than left to be read as omissions.
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
 * The negative controls: the three shapes the census exists to reject. Each is
 * an `@ts-expect-error`, so an assertion that stopped biting would surface as
 * an UNUSED expect-error — an error in this repo's configuration — rather than
 * as a census that silently passes everything.
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

describe('execution-fence wiring census', () => {
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
