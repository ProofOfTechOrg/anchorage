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

// INV-3: the tenant id charset — STRICTER than PATH_SAFE_ID_PATTERN, and the
// two must never be conflated. Every runId is minted as `${tenantId}_${uuid}`
// and the tenant is recovered by prefix, so the charset is what makes the
// prefix EXACT. It is `0x30–0x39 ∪ 0x61–0x7A`, which contains no character in
// `[0x5F, 0x60]` — neither `_` (0x5F) nor backtick (0x60). Two consequences,
// both load-bearing:
//
//   1. Ownership is exact: `runId.startsWith(tenantId + '_')` cannot match
//      another tenant ('acme' vs 'acmecorp': at index 4 one has '_' 0x5F, the
//      other 'c' 0x63 — the delimiter cannot occur inside a tenantId).
//   2. The range purge is exact: `run_id >= '<tid>_' AND run_id < '<tid>' ||
//      CHAR(0x60)` selects exactly one tenant's rows under BINARY collation
//      (any other tenant's character at the delimiter position is < 0x5F or
//      > 0x60, falling outside the range).
//
// Loosening this charset silently breaks BOTH properties —
// path-safe-id.test.ts pins it character-by-character.
export const TENANT_ID_PATTERN = /^[a-z0-9]{3,32}$/;

// Ids that collide with the TCB's own audit identity: the cron maintenance
// actor attributes sweeps and purges as tenantId 'system'. Never a valid
// tenantId ANYWHERE, enforced twice: toApprovalActor drops it at token
// verification, and createTenantResolver re-refuses it before any store binds
// or runId mints — a custom TokenVerifier (a supported seam) or a hand-built
// actor map handed to staticTokenVerifier never crosses toApprovalActor.
// (Audit-attribution collision, not privilege escalation: a
// SystemApprovalStore comes only from `.system()`, never from naming a
// tenant.) Lives in this leaf, not host-kit/tenant-registry, so approval-api
// can enforce it without importing host-kit against the layering; host-kit
// re-exports it for the public surface.
export const RESERVED_TENANT_IDS: readonly string[] = ['system'];

/**
 * The single place the `${tenantId}_${uuid}` carrier is decoded. Returns the
 * validated tenant prefix, or undefined when the runId carries none.
 *
 * Centralized for the same reason RunnerRuntime composes `#runKey` in one
 * place: this parse IS the tenant boundary, so four hand-rolled
 * `indexOf('_')` decodes would drift (and did — one skipped the pattern
 * check). Callers apply their own policy on undefined: the DO throws (an
 * unscoped grant store is a cross-tenant mint), the grant provider mints an
 * empty list, the runtime mints no isolation scope.
 *
 * Safe: the tenant-ID pattern excludes '_' from tenantId, so the first underscore is always
 * the boundary regardless of what the uuid half contains.
 */
export function tenantOfRunId(runId: string): string | undefined {
  const separator = runId.indexOf('_');
  if (separator <= 0) return undefined;
  const tenantId = runId.slice(0, separator);
  return TENANT_ID_PATTERN.test(tenantId) ? tenantId : undefined;
}

/**
 * Exact tenant-salted ownership: `id.startsWith(`${tenantId}_`)`. Exact because
 * TENANT_ID_PATTERN excludes '_' (0x5F) and backtick (0x60), so the
 * FIRST '_' delimiter can never occur inside a tenantId — 'acme' can never own
 * 'acmecorp_...'. The trailing '_' is load-bearing: it is the same delimiter the
 * `[tid_, tid\x60)` range purge keys on, so dropping it would make 'acme' match
 * 'acmecorp'. The ONE ownership predicate for runIds and memory ids.
 */
export function tenantOwnsSaltedId(tenantId: string, id: string): boolean {
  return id.startsWith(`${tenantId}_`);
}

/**
 * The MINT-path precondition: throws a GENERIC Error — the programmer-error
 * contract, since a client never reaches a mint with a bad tenant — when
 * `tenantId` fails TENANT_ID_PATTERN or names a RESERVED_TENANT_IDS
 * identity. Deliberately NOT a TenantResolutionError: createTenantResolver owns
 * that request-facing 403 contract; the mints are internal. Shared by every
 * salted-id mint (runIds and memory ids).
 */
export function assertMintableTenantId(tenantId: string, caller: string): void {
  if (typeof tenantId !== 'string' || !TENANT_ID_PATTERN.test(tenantId)) {
    throw new Error(
      `${caller}: tenantId '${tenantId}' violates INV-3 (^[a-z0-9]{3,32}$) — the prefix decode and range purge are only exact over that charset`,
    );
  }
  if (RESERVED_TENANT_IDS.includes(tenantId)) {
    throw new Error(
      `${caller}: tenantId '${tenantId}' is reserved (the TCB's own audit identity) — refusing to mint a tenant-salted id for it`,
    );
  }
}

/**
 * Mint a tenant-salted id `${tenantId}_${suffix}` after asserting the tenant is
 * mintable. The single salted-id constructor shared by runIds and memory ids; a
 * caller with its own suffix precondition (memory-id's PATH_SAFE resourceKey
 * check) validates that BEFORE delegating here.
 *
 * The suffix may be a thunk (a uuid generator): it is evaluated ONLY after the
 * tenant passes assertMintableTenantId, so a caller-supplied producer can never
 * run its side effects — or throw and mask tenant validation — for an
 * invalid tenant. Callers with a callback MUST pass the function, not its call,
 * or JS argument evaluation would defeat this ordering.
 */
export function mintSaltedId(
  tenantId: string,
  suffix: string | (() => string),
  caller: string,
): string {
  assertMintableTenantId(tenantId, caller);
  return `${tenantId}_${typeof suffix === 'function' ? suffix() : suffix}`;
}
