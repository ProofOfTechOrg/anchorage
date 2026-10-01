// SPDX-License-Identifier: Apache-2.0
// The host Durable Object shares one pub/sub identity across its consumers.
//
// A configured instance passes through init() to RunnerRuntime, which passes it
// to core when creating runs and Mastra. Consumers in the same isolate use the
// instance from InitResult so publishing and replay share a feed.
//
// FlowsafeDurableAgent installs its own `pubsub` option, else the runtime's
// pub/sub, else its own stream bus on the wrapped agent at construction.
//
// A Durable Object keeps its publisher and subscriber in the same isolate. A
// host that needs a durable or cache-backed feed injects its own PubSub.
//
// Without a host pub/sub, init() passes undefined to the runtime; the wrapper
// still installs its own bus.

import { EventEmitterPubSub, type PubSub } from '@mastra/core/events';

/**
 * The pubsub seam do-runner passes around — core's `PubSub` base, so a host may
 * substitute any implementation (CachingPubSub, a custom bus) for the default.
 */
export type HostPubSub = PubSub;

/**
 * The default host pubsub: core's in-process `EventEmitterPubSub`, which is
 * exactly right inside a DO (one isolate, no cross-process delivery to lose)
 * and is what core itself would default to per createRun — the difference is
 * that this one instance is SHARED, so publish and replay agree.
 *
 * A host opts in with `init(env, { pubsub: createHostPubSub() })`, the same
 * instance-or-absent shape every other InitOptions seam takes (`storage`,
 * `requestContextForRun`).
 */
export function createHostPubSub(): HostPubSub {
  return new EventEmitterPubSub();
}
