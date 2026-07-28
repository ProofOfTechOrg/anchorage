// SPDX-License-Identifier: Apache-2.0
// Principal kinds — the human/automated distinction the RBAC gates evaluate.
//
// A leaf rather than part of rbac/index.ts so `assertPrincipalKinds`, which is
// shared plumbing between the middleware and the guarded-agent factory, does
// not become public API through the `@proofoftech/breakwater/rbac` subpath.
// The four symbols hosts genuinely need are re-exported from that barrel; this
// one is internal.
//
// @internal

/**
 * What KIND of principal an actor is. Roles are a human vocabulary — a
 * scheduled job, a service call, and an agent delegating to another agent have
 * no logged-in person behind them, and giving them a human role to satisfy an
 * authorization gate is how automated execution silently inherits human
 * authority.
 *
 * Breakwater does not resolve principals; the host does (see
 * `breakwater-purpose-and-boundaries.md`). It only needs to tell a person from
 * a process so a guarded agent can refuse the latter by default.
 */
export type PrincipalKind = 'human' | 'service' | 'agent' | 'system';

export const PRINCIPAL_KINDS: readonly PrincipalKind[] = [
  'human',
  'service',
  'agent',
  'system',
];

/** The only kind authorized when a caller does not opt in to automation. */
export const DEFAULT_ALLOWED_PRINCIPAL_KINDS: readonly PrincipalKind[] = [
  'human',
];

/**
 * An actor's effective kind, treating the absent field as its default.
 *
 * Structurally typed rather than taking `Actor` so this leaf does not depend on
 * the barrel that re-exports it.
 */
export function principalKindOf(actor: {
  kind?: PrincipalKind;
}): PrincipalKind {
  return actor.kind ?? 'human';
}

/**
 * Validate a caller-supplied kind allowlist, defaulting to humans only. Shared
 * by `RBACMiddleware` and `createGuardedAgent` so the processor gate and the
 * direct-call gate cannot be configured differently.
 *
 * @internal
 */
export function assertPrincipalKinds(
  kinds: readonly PrincipalKind[] | undefined,
  label: string,
): readonly PrincipalKind[] {
  if (kinds === undefined) return DEFAULT_ALLOWED_PRINCIPAL_KINDS;
  if (!Array.isArray(kinds) || kinds.length === 0) {
    throw new TypeError(
      `${label}: allowedPrincipalKinds must be a non-empty array`,
    );
  }
  const seen = new Set<PrincipalKind>();
  for (const kind of kinds) {
    if (!(PRINCIPAL_KINDS as readonly unknown[]).includes(kind)) {
      throw new TypeError(`${label}: unknown principal kind '${String(kind)}'`);
    }
    if (seen.has(kind)) {
      throw new TypeError(`${label}: duplicate principal kind '${kind}'`);
    }
    seen.add(kind);
  }
  return Object.freeze([...kinds]);
}
