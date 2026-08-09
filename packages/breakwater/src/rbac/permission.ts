// SPDX-License-Identifier: Apache-2.0
// Permission identifiers — the host-owned, fine-grained authorization
// vocabulary shared by agent entry (flowsafe's catalog) and connector
// invocation (the connector SDK's required-permissions gate).
//
// Breakwater does not resolve principals to permissions; the host does. This
// leaf defines the identifier grammar, the trusted request-context projection
// that carries a principal's effective permissions, and the validation both
// sides of that contract apply — so the minting host and the enforcing gate
// cannot drift apart.

/**
 * A canonical server-owned permission identifier.
 *
 * The runtime form is two or more lowercase ASCII segments separated by dots,
 * with each segment starting with a letter and continuing with letters or
 * digits. Identifiers are bounded to 200 characters.
 */
export type Permission = string;

const PERMISSION_IDENTIFIER_PATTERN =
  /^(?=.{3,200}$)[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)+$/;

/** Whether a value is a canonical permission identifier. */
export function isPermissionIdentifier(value: unknown): value is Permission {
  return typeof value === 'string' && PERMISSION_IDENTIFIER_PATTERN.test(value);
}

/**
 * requestContext key carrying the current principal's server-resolved
 * effective permissions. Only trusted host/runtime code may populate it —
 * like `breakwater.actor` and the connector grant keys, whoever can write
 * this value decides authorization. A client, model, signal, workflow input,
 * or raw resume must never reach it; flowsafe reserves the complete
 * `breakwater.*` namespace at its execution-context boundary.
 */
export const PRINCIPAL_PERMISSIONS_CONTEXT_KEY =
  'breakwater.principalPermissions';

/**
 * The trusted projection stored under {@link PRINCIPAL_PERMISSIONS_CONTEXT_KEY}:
 * the exact permissions a server-owned policy snapshot granted the executing
 * principal, plus the version of that snapshot for audit correlation.
 */
export interface PrincipalPermissions {
  /** Exact permission identifiers granted by the policy snapshot. */
  permissions: readonly Permission[];
  /**
   * Stable version or hash identifying the policy snapshot used. Must be
   * non-blank, at most 200 characters, and free of ASCII control characters.
   */
  policyVersion: string;
}

const MAX_POLICY_VERSION_LENGTH = 200;

function boundedPolicyVersion(value: unknown): value is string {
  if (
    typeof value !== 'string' ||
    value.trim() === '' ||
    value.length > MAX_POLICY_VERSION_LENGTH
  ) {
    return false;
  }
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (code <= 0x1f || code === 0x7f) return false;
  }
  return true;
}

/**
 * Whether a value is a well-formed principal-permissions projection: an
 * object whose `permissions` is an array of canonical identifiers and whose
 * `policyVersion` is bounded. Duplicate identifiers are tolerated rather than
 * treated as malformed — the consuming check has set semantics, so a host
 * that unions role bundles must not take an availability hit for a repeat
 * that cannot change any decision.
 *
 * Both halves of the composed control run this predicate: the flowsafe host
 * validates resolver output before minting the projection, and the connector
 * gate validates the projection before trusting it. Anything else fails
 * closed.
 */
export function isPrincipalPermissions(
  value: unknown,
): value is PrincipalPermissions {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }
  const candidate = value as { permissions?: unknown; policyVersion?: unknown };
  return (
    Array.isArray(candidate.permissions) &&
    candidate.permissions.every(isPermissionIdentifier) &&
    boundedPolicyVersion(candidate.policyVersion)
  );
}
