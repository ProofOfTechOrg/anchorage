// SPDX-License-Identifier: Apache-2.0
// The execution principal's IDENTITY vocabulary — the kind list, the two
// predicates that police it, and the bound every principal-authored string
// obeys. Nothing else.
//
// WHY IT IS ITS OWN FILE. This module imports NOTHING, and that is the whole
// point of it rather than a happy accident. `principal.ts` imports `contract.ts`
// and carries the trusted-automation machinery; `start-idempotency.ts` declares
// six `DoStatusError` subclasses at module-eval time and needs exactly these
// three names to validate a stored row's owner. Reaching them through
// `principal.ts` puts a module-eval-time class hierarchy behind an import graph
// that is one edge away from cycling back — the same temporal-dead-zone hazard
// that forced `do-status-error.ts` out on its own. A leaf with no imports cannot
// participate in a cycle at all, so the hazard is closed by construction rather
// than by watching the graph.
//
// `principal.ts` re-exports every name here, so this split is invisible to
// existing importers and nothing outside this package can tell it happened.

/**
 * Mirrors breakwater's `PrincipalKind` by value, for the same reason
 * contract.ts mirrors the request-context keys: flowsafe does not import
 * breakwater at runtime. The cross-package contract test pins the equality.
 */
export const EXECUTION_PRINCIPAL_KINDS = [
  'human',
  'service',
  'agent',
  'system',
] as const;

export type ExecutionPrincipalKind = (typeof EXECUTION_PRINCIPAL_KINDS)[number];

/** Internal runtime guard for values crossing storage and request boundaries. */
export function isExecutionPrincipalKind(
  value: unknown,
): value is ExecutionPrincipalKind {
  return (
    typeof value === 'string' &&
    (EXECUTION_PRINCIPAL_KINDS as readonly string[]).includes(value)
  );
}

/** Upper bound on the free-text provenance fields, so audit rows stay bounded. */
export const MAX_PURPOSE_LENGTH = 200;
export const MAX_PRINCIPAL_ID_LENGTH = 200;

/**
 * Bounded, non-empty, and free of control characters.
 *
 * Not an injection barrier for the wire — the principal travels through
 * `JSON.stringify`, which escapes U+0000–U+001F. It matters because these
 * fields do not stop at the wire: `id` becomes `requestedBy`/`decidedBy` in D1
 * and the actor on every audit row, and `purpose` rides into the SIEM export.
 * The bounds keep an audit row bounded; the control-character refusal keeps
 * those strings clean at the boundary rather than downstream.
 *
 * Exported for `principal.ts`'s own use only, and deliberately off the package
 * barrel: it is one half of an internal validation rule, not public API.
 */
export function boundedText(value: unknown, max: number): value is string {
  if (typeof value !== 'string' || value.trim() === '' || value.length > max) {
    return false;
  }
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (code <= 0x1f || code === 0x7f) return false;
  }
  return true;
}

/**
 * Canonical execution-principal identifier rule shared by every request and
 * storage hydration boundary. Kept off the package barrel deliberately.
 */
export function isExecutionPrincipalId(value: unknown): value is string {
  return boundedText(value, MAX_PRINCIPAL_ID_LENGTH);
}
