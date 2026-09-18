// SPDX-License-Identifier: Apache-2.0
// Actor contract — the identity RBAC authorizes and audit attributes.
//
// A type-only leaf, so `authorize.ts` and `audit/index.ts` take these two
// declarations without importing the barrel that imports them back.

import type { PrincipalKind } from './principal.js';

/** Role labels accepted by the built-in actor contract. */
export type Role = 'admin' | 'builder' | 'operator' | 'reviewer' | 'viewer';

/** Authenticated identity evaluated by RBAC and attached to audit events. */
export interface Actor {
  /** Stable actor identifier from the host authentication system. */
  id: string;
  /**
   * Role used by the middleware's exact allowlist. Meaningful only for the
   * 'human' kind; for automated kinds the role allowlist is not consulted at
   * all and hosts should project the least-privileged label. See
   * `authorizeActor`.
   */
  role: Role;
  /** Absent means 'human', so an existing host keeps its exact behavior. */
  kind?: PrincipalKind;
}
