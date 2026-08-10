// SPDX-License-Identifier: Apache-2.0
// Path-safe id pattern — a leaf module (zero imports) the do-runner owns.
//
// The pattern lives HERE, not in runtime.ts, so consumers that only need the
// validation (artifacts/ keys R2 objects by these same ids) can import it
// without transitively loading runtime.ts — which value-imports @mastra/core
// and the whole RunnerRuntime. Same reasoning as breakwater-keys.ts: a shared
// constant belongs in a leaf so it never forces a heavy import.
//
// A runId or workflowId flows into three addressing contexts that must agree
// with zero encoding: the D1 snapshot key, the Durable Object name join
// (idFromName(`${workflowId}:${runId}`)), and the `/runs/:workflowId/:runId`
// URL path. Restricting both ids to RFC 3986 unreserved characters keeps them
// unambiguous in all three: no '/' (path split), no '%' (percent-encoding),
// no ':' (name join), no whitespace. The leading (?!\.\.?$) also rejects the
// bare dot-segments '.' and '..', which the URL parser normalizes out of the
// status/resume path — a run created under one would never be addressable
// again. Generated UUIDs, ULIDs, nanoids, and slugs all satisfy it.
export const PATH_SAFE_ID_PATTERN = /^(?!\.\.?$)[A-Za-z0-9._~-]{1,200}$/;

/** RegExp.test coerces values; public boundaries must first prove a string. */
export function isPathSafeId(value: unknown): value is string {
  return typeof value === 'string' && PATH_SAFE_ID_PATTERN.test(value);
}
