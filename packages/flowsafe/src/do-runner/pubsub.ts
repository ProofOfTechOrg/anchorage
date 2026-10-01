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

// First: stores core's events nonce; workerd refuses module-scope UUIDs.
import './core-events-nonce.js';
import { EventEmitterPubSub, type PubSub } from '@mastra/core/events';

/**
 * The pub/sub seam do-runner passes around: core's `PubSub`, so a host may
 * substitute its own implementation. While core generates its events nonce at
 * module scope, a Worker whose code loads `@mastra/core/events` (for example
 * for `CachingPubSub`) before Flowsafe's do-runner fails startup; such a Worker
 * builds its bus with `createHostPubSub()`.
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
