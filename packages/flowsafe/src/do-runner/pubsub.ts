// SPDX-License-Identifier: Apache-2.0
// The host DO's ONE pubsub identity (DL-001).
//
// Core creates a fresh EventEmitterPubSub per createRun() when none is passed,
// so two call sites that each let it default publish to DIFFERENT emitters:
// events published on one are invisible to an observe()/replay on the other,
// and a reconnecting stream silently replays nothing. The fix is identity, not
// configuration — ONE instance per host DO, constructed here and taken from
// init()'s InitResult by every consumer rather than each building its own.
//
// SCOPE (Track 0): this establishes the identity and the seam that carries it.
// Nothing hands it to core yet — RunnerRuntime's two createRun sites still let
// core default their emitter, and Track A threads this instance into them
// (CI-M-002-002). Until then a configured pubsub is an identity the host holds,
// not a feed core publishes on.
//
// A Durable Object IS the scope that makes an in-process emitter sufficient:
// every leg of a run (or of a thread's agent loop) is serialized onto one
// instance by idFromName, so publisher and subscriber are already in the same
// isolate — the reason no Redis/cross-process bus is needed (DL-002). A host
// that needs a durable or cache-backed feed injects its own PubSub (e.g. core's
// CachingPubSub) through the same seam.
//
// OPT-IN: absent, init() resolves no pubsub, nothing is passed to core, and
// every host behaves byte-identically to before this module existed — polling
// stays the fallback, the same posture HUB + STREAM_TICKET_SECRET take for
// live streaming.

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
