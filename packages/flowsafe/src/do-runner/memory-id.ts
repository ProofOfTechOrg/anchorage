// SPDX-License-Identifier: Apache-2.0
// Agent-memory id discipline — the INV-1 carrier extended to Mastra memory
// (design: docs/agent-memory-tenancy.md). Mastra keys agent memory by
// caller-chosen threadId/resourceId, which two tenants can legitimately
// share (a user email, a lead id): unsalted, tenant B's agent would recall
// tenant A's messages. These mints make memory ids tenant-disjoint BY
// CONSTRUCTION — the same `${tenantId}_${suffix}` shape as runIds, so the
// INV-3 charset argument (exact prefix decode, exact range purge) transfers
// verbatim and purgeTenant's [`${tid}_`, `${tid}\x60`) predicate covers the
// memory tables with zero new reasoning.
//
// Chokepoint rule (INV-2 style): hosts NEVER accept a client-supplied
// threadId/resourceId — mint server-side from the AUTHENTICATED tenant
// (TenantContext.newThreadId()/newResourceId() wrap these), assert ownership
// on every read path (404 on a foreign id — no existence oracle), and treat
// these helpers as the only constructors. Message ids need no salting: the
// purge and every scoped query ride the salted thread_id/resourceId.

import {
  mintSaltedId,
  PATH_SAFE_ID_PATTERN,
  tenantOfRunId,
  tenantOwnsSaltedId,
} from './path-safe-id.js';

/** Mint a tenant-salted agent-memory threadId: `${tenantId}_${uuid}`. */
export function mintThreadId(
  tenantId: string,
  mintUuid: () => string = () => crypto.randomUUID(),
): string {
  // Pass mintUuid LAZILY (not mintUuid()): mintSaltedId validates the tenant
  // BEFORE evaluating the suffix, so a caller-supplied uuid callback never runs
  // (side effects) or throws (masking the INV-3/reserved rejection) for an
  // invalid tenant.
  return mintSaltedId(tenantId, mintUuid, 'mintThreadId');
}

/**
 * Mint a tenant-salted agent-memory resourceId: `${tenantId}_${resourceKey}`.
 * The key is the host's business identity for the memory owner (a user id, a
 * lead id) and is deliberately stable across runs — two tenants using the
 * SAME key stay disjoint because the prefix differs. The key must match the
 * do-runner's PATH_SAFE_ID_PATTERN (RFC 3986 unreserved); '_' inside it is
 * fine — the tenant-ID pattern excludes '_' from tenant ids, so the first underscore is
 * always the tenant boundary.
 */
export function mintResourceId(tenantId: string, resourceKey: string): string {
  if (!PATH_SAFE_ID_PATTERN.test(resourceKey)) {
    throw new Error(
      `mintResourceId: resourceKey '${resourceKey}' must match PATH_SAFE_ID_PATTERN (RFC 3986 unreserved chars, 1-200 long, not '.' or '..')`,
    );
  }
  return mintSaltedId(tenantId, resourceKey, 'mintResourceId');
}

/**
 * Recover the validated tenant prefix of a memory id (threadId or
 * resourceId), or undefined when it carries none. Memory ids share the runId
 * carrier deliberately, so this delegates to the ONE decode (`tenantOfRunId`)
 * instead of growing a second parse that could drift.
 */
export function tenantOfMemoryId(id: string): string | undefined {
  return tenantOfRunId(id);
}

/**
 * Exact tenant ownership of a memory id — delegates to the ONE salted-id
 * ownership predicate (`tenantOwnsSaltedId`) so memory and run ownership can
 * never drift. Exact because the '_' delimiter cannot occur
 * inside a tenantId, so 'acme' can never own 'acmecorp_...'. Assert on EVERY
 * memory read/write path.
 */
export function tenantOwnsMemoryId(tenantId: string, id: string): boolean {
  return tenantOwnsSaltedId(tenantId, id);
}
