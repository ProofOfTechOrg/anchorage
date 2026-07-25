// SPDX-License-Identifier: Apache-2.0
// A routing primitive shared by every P6 ingestion router: decode a URL path
// segment without letting malformed percent-encoding crash the handler.
//
// A zero-dependency leaf, imported directly by the routers that need it
// (signals / goals / schedules / signal-providers / background-tasks) rather
// than re-exported from the host-kit barrel — it is internal plumbing, not a
// public host-kit API, and staying a leaf keeps it cycle-free (the same reason
// RunRouteError is a leaf).

/**
 * Decode a URL path segment, returning `undefined` on malformed percent-encoding
 * (a lone `%`) instead of THROWING the way bare `decodeURIComponent` does.
 *
 * `new URL(...).pathname` passes an invalid `%` through unnormalized, so an
 * unguarded `decodeURIComponent` inside a router would be a PRE-AUTH fault: the
 * `URIError` escapes the router before authentication. This is the PRIMARY,
 * source-level defense — callers treat `undefined` as "not a real route target"
 * and return route-absent (`null`), byte-identical to a non-matching path, so a
 * malformed id is indistinguishable from a path that was never ours (strictly
 * better than surfacing a generic 500). `createFlowsafeWorker`'s fetch handler
 * additionally wraps the router calls in a try/catch as defense-in-depth, so any
 * future unguarded decode is still contained as a generic 500 rather than
 * rejecting out of `fetch()`.
 */
export function safeDecodeSegment(
  segment: string | undefined,
): string | undefined {
  if (segment === undefined) return undefined;
  try {
    return decodeURIComponent(segment);
  } catch {
    return undefined;
  }
}
