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
  PATH_SAFE_ID_PATTERN,
  RESERVED_TENANT_IDS,
  TENANT_ID_PATTERN,
  tenantOfRunId,
} from './path-safe-id.js';

function assertTenantId(tenantId: string, caller: string): void {
  if (!TENANT_ID_PATTERN.test(tenantId)) {
    throw new Error(
      `${caller}: tenantId '${tenantId}' violates INV-3 (^[a-z0-9]{3,32}$) — the prefix decode and range purge are only exact over that charset`,
    );
  }
  if (RESERVED_TENANT_IDS.includes(tenantId)) {
    throw new Error(
      `${caller}: tenantId '${tenantId}' is reserved (the TCB's own audit identity) — refusing to mint memory ids for it`,
    );
  }
}

/** Mint a tenant-salted agent-memory threadId: `${tenantId}_${uuid}`. */
export function mintThreadId(
  tenantId: string,
  mintUuid: () => string = () => crypto.randomUUID(),
): string {
  assertTenantId(tenantId, 'mintThreadId');
  return `${tenantId}_${mintUuid()}`;
}

/**
 * Mint a tenant-salted agent-memory resourceId: `${tenantId}_${resourceKey}`.
 * The key is the host's business identity for the memory owner (a user id, a
 * lead id) and is deliberately stable across runs — two tenants using the
 * SAME key stay disjoint because the prefix differs. The key must match the
 * do-runner's PATH_SAFE_ID_PATTERN (RFC 3986 unreserved); '_' inside it is
 * fine — INV-3 excludes '_' from tenant ids, so the FIRST underscore is
 * always the tenant boundary.
 */
export function mintResourceId(tenantId: string, resourceKey: string): string {
  assertTenantId(tenantId, 'mintResourceId');
  if (!PATH_SAFE_ID_PATTERN.test(resourceKey)) {
    throw new Error(
      `mintResourceId: resourceKey '${resourceKey}' must match PATH_SAFE_ID_PATTERN (RFC 3986 unreserved chars, 1-200 long, not '.' or '..')`,
    );
  }
  return `${tenantId}_${resourceKey}`;
}

/**
 * Recover the INV-3-validated tenant prefix of a memory id (threadId or
 * resourceId), or undefined when it carries none. Memory ids share the runId
 * carrier deliberately, so this delegates to the ONE decode (`tenantOfRunId`)
 * instead of growing a second parse that could drift.
 */
export function tenantOfMemoryId(id: string): string | undefined {
  return tenantOfRunId(id);
}

/**
 * Exact ownership: `id.startsWith(`${tenantId}_`)` — exact for the same
 * INV-3 reason as run ownership (the delimiter cannot occur inside a
 * tenantId, so 'acme' can never own 'acmecorp_...'). Assert on EVERY memory
 * read/write path.
 */
export function tenantOwnsMemoryId(tenantId: string, id: string): boolean {
  return id.startsWith(`${tenantId}_`);
}
