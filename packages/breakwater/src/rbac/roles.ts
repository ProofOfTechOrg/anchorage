// SPDX-License-Identifier: Apache-2.0
// Role vocabulary and the role-allowlist reader. A leaf rather than part of
// rbac/index.ts so the reader takes `ROLES` without importing the barrel that
// imports it back, and stays internal while the barrel re-exports `ROLES`.

import { describeEntry, readFrozenList } from '../host-input.js';
import type { Role } from './actor.js';

/** All role labels accepted by `RBACMiddleware`. */
export const ROLES: readonly Role[] = [
  'admin',
  'builder',
  'operator',
  'reviewer',
  'viewer',
];

/**
 * Read a role allowlist into a frozen copy: a non-empty array of distinct
 * {@link ROLES} members. Shared, so the processor gate and the direct-call
 * gate cannot accept different lists. `label` names the caller in refusals.
 *
 * @internal
 */
export function readAllowedRoles(
  label: string,
  roles: unknown,
): readonly Role[] {
  const subject = `${label}: allowedRoles`;
  const seen = new Set<Role>();
  const copy = readFrozenList(subject, roles, (entry, index) => {
    if (!(ROLES as readonly unknown[]).includes(entry)) {
      throw new TypeError(
        `${subject} entry ${index} is an unknown allowed role (got ${describeEntry(entry)})`,
      );
    }
    const role = entry as Role;
    if (seen.has(role)) {
      throw new TypeError(
        `${subject} entry ${index} is a duplicate allowed role (got ${describeEntry(role)})`,
      );
    }
    seen.add(role);
    return role;
  });
  if (copy.length === 0) {
    throw new TypeError(`${subject} must be a non-empty array`);
  }
  return copy;
}
