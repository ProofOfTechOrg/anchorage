// SPDX-License-Identifier: Apache-2.0
// Agent-memory id construction. Mastra keys agent memory by caller-chosen
// threadId/resourceId; flowsafe mints the thread id and validates a host-owned
// resource business key so a client can never smuggle an id that collides with
// (or probes for) another user's memory keys.
//
// Chokepoint rule (INV-2 style): hosts NEVER accept a client-supplied
// threadId/resourceId — mint the thread server-side and validate the resource
// key from trusted host data. ActorContext.newThreadId() and
// ActorContext.resourceIdFromKey() wrap these constructors. Message ids need no
// separate constructor: every scoped query rides the trusted thread_id/resourceId.

import { isPathSafeId } from './path-safe-id.js';

/** Mint a server-owned agent-memory threadId (a uuid by default). */
export function mintThreadId(
  mintUuid: () => string = () => crypto.randomUUID(),
): string {
  const threadId = mintUuid();
  if (!isPathSafeId(threadId)) {
    throw new Error(
      `mintThreadId: generated threadId '${threadId}' must match PATH_SAFE_ID_PATTERN (RFC 3986 unreserved chars, 1-200 long, not '.' or '..') — it becomes a DO name and a D1 key`,
    );
  }
  return threadId;
}

/**
 * Validate a host business key (a user id, a lead id) into an agent-memory
 * resourceId. The key is deliberately stable across runs — it IS the memory
 * owner's identity inside this deployment. The validator enforces do-runner's
 * PATH_SAFE_ID_PATTERN so the id stays unambiguous in every context it keys
 * (D1 rows, DO names, URL paths).
 */
export function resourceIdFromKey(resourceKey: string): string {
  if (!isPathSafeId(resourceKey)) {
    throw new Error(
      `resourceIdFromKey: resourceKey '${resourceKey}' must match PATH_SAFE_ID_PATTERN (RFC 3986 unreserved chars, 1-200 long, not '.' or '..')`,
    );
  }
  return resourceKey;
}
