// The tenant-binding brand (INV-2). TypeScript is structural: a plain
// `readonly tenantId: string` discriminator is satisfied by any object
// literal, and a #private field is nominal PER CLASS — which breaks here
// because two concrete stores (D1ApprovalStore, InMemoryApprovalStore) must
// both satisfy the bound type. A shared `unique symbol` property is satisfied
// by both classes and by nothing that does not deliberately import this
// symbol and stamp it.
//
// The symbol is exported because declaration emit (.d.ts) cannot reference a
// module-private name from an exported type. The residual risk is explicit
// and grep-visible: forging a bound store requires
// `import { TENANT_BOUND }` + `[TENANT_BOUND]: true` — a deliberate TCB
// bypass on par with an `as TenantBoundApprovalStore` cast, which no brand
// can prevent. What the brand DOES eliminate: accidental structural
// satisfaction, and the rushed fix that passes an unbound or system store
// into a request handler.

import type { ApprovalPatch, ApprovalStore } from './store.js';
import type { ApprovalListFilter, ApprovalRecord } from './types.js';

export const TENANT_BOUND: unique symbol = Symbol('flowsafe.tenantBound');

/**
 * An ApprovalStore constructed bound to exactly one tenant: every
 * SELECT/UPDATE/DELETE it issues carries `tenant_id = <ctor field>`. The only
 * store type request-scoped code (ApprovalService, approvalGrantProvider) may
 * touch.
 */
export type TenantBoundApprovalStore = ApprovalStore & {
  readonly tenantId: string;
  readonly [TENANT_BOUND]: true;
};

/**
 * The cron-only cross-tenant view (SLA sweep). Deliberately NOT an
 * ApprovalStore (no create/get) and NOT brandable (`[TENANT_BOUND]?: never`),
 * so it is unassignable wherever a TenantBoundApprovalStore is required —
 * "system stores never reach a request handler" is a compile error, not a
 * convention.
 */
export interface SystemApprovalStore {
  readonly [TENANT_BOUND]?: never;
  list(filter?: ApprovalListFilter): Promise<ApprovalRecord[]>;
  transition(
    id: string,
    from: readonly ApprovalRecord['status'][],
    patch: ApprovalPatch,
  ): Promise<ApprovalRecord | null>;
  /**
   * Deletes TERMINAL (approved/rejected) records whose terminal timestamp —
   * decidedAt, or updatedAt when a decided record was persisted without one
   * — is strictly before cutoffIso, bounded to at most `limit` deletions.
   * The retention-purge primitive behind purgeExpiredApprovals
   * (retention.ts) — cron-only by type, exactly like list/transition: a
   * tenant-bound store never gains this method.
   */
  purgeExpired(cutoffIso: string, limit: number): Promise<number>;
}
