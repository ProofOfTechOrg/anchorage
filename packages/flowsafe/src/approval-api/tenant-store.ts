// Store factories — the ONLY way hosts obtain approval stores (INV-2: "no
// caller can obtain an approval store that is not bound to exactly one tenant
// at construction"). Each factory owns its backend handle and its schema-init
// memo, so a per-request forTenant() does not re-run DDL; system() hands the
// cron the cross-tenant view, distinctly TYPED so it cannot flow into
// request-scoped code (SystemApprovalStore is not an ApprovalStore).

import { TENANT_ID_PATTERN } from '../do-runner/path-safe-id.js';
import {
  type ApprovalDatabase,
  createApprovalSchema,
  D1ApprovalStore,
  d1SystemApprovalStore,
} from './d1-store.js';
import type { ApprovalPatch } from './store.js';
import { byQueueOrder, InMemoryApprovalStore, matchesFilter } from './store.js';
import type {
  SystemApprovalStore,
  TenantBoundApprovalStore,
} from './tenant-brand.js';
import type {
  ApprovalListFilter,
  ApprovalRecord,
  ApprovalStatus,
} from './types.js';

export interface ApprovalStoreFactory {
  /** Bind a store to one tenant. Throws unless tenantId satisfies INV-3. */
  forTenant(tenantId: string): TenantBoundApprovalStore;
  /** The cron-only cross-tenant view (SLA sweep). Never request-scoped. */
  system(): SystemApprovalStore;
}

function assertTenantId(tenantId: string): void {
  if (!TENANT_ID_PATTERN.test(tenantId)) {
    throw new Error(
      `forTenant: tenantId '${tenantId}' violates INV-3 (^[a-z0-9]{3,32}$)`,
    );
  }
}

export class D1ApprovalStoreFactory implements ApprovalStoreFactory {
  readonly #db: ApprovalDatabase;
  #schemaReady?: Promise<void>;

  constructor(db: ApprovalDatabase) {
    this.#db = db;
  }

  // One memoized DDL pass per factory (per isolate), shared by every bound
  // store and the system view; a failed attempt clears the memo so the next
  // call retries instead of pinning everything to a dead promise.
  #ready = (): Promise<void> => {
    this.#schemaReady ??= createApprovalSchema(this.#db).catch(
      (error: unknown) => {
        this.#schemaReady = undefined;
        throw error;
      },
    );
    return this.#schemaReady;
  };

  forTenant(tenantId: string): TenantBoundApprovalStore {
    assertTenantId(tenantId);
    return new D1ApprovalStore(this.#db, { tenantId, ready: this.#ready });
  }

  system(): SystemApprovalStore {
    return d1SystemApprovalStore(this.#db, this.#ready);
  }
}

/**
 * In-memory factory over ONE shared Map. Not decoration: two independent
 * `new InMemoryApprovalStore(...)` instances hold separate Maps, so a
 * cross-tenant get/transition test against them passes trivially without
 * exercising the tenant predicate — the exact "InMemory hides what D1 does"
 * divergence the shared contract suite exists to close. Tenant A and tenant B
 * views minted here share the backend, like two bound D1 stores share the
 * table.
 */
export class InMemoryApprovalStoreFactory implements ApprovalStoreFactory {
  readonly #records = new Map<string, ApprovalRecord>();

  forTenant(tenantId: string): TenantBoundApprovalStore {
    assertTenantId(tenantId);
    return new InMemoryApprovalStore(tenantId, this.#records);
  }

  /**
   * Delete every record stamped with this tenant, returning the count — the
   * in-memory mirror of the D1 `purgeTenant` approvals delete (in-memory hosts
   * have no D1 for that function to run against). Factory-level like
   * `system()`: offboarding is not a request-scoped capability.
   */
  purgeTenant(tenantId: string): number {
    assertTenantId(tenantId);
    let purged = 0;
    for (const [id, record] of this.#records) {
      if (record.tenantId === tenantId) {
        this.#records.delete(id);
        purged += 1;
      }
    }
    return purged;
  }

  system(): SystemApprovalStore {
    const records = this.#records;
    return {
      // Identical to the bound store's list() minus the tenant predicate —
      // hence the shared matchesFilter/byQueueOrder rather than a copy.
      async list(filter: ApprovalListFilter = {}): Promise<ApprovalRecord[]> {
        return [...records.values()]
          .filter((record) => matchesFilter(record, filter))
          .sort(byQueueOrder)
          .map((record) => structuredClone(record));
      },
      async transition(
        id: string,
        from: readonly ApprovalStatus[],
        patch: ApprovalPatch,
      ): Promise<ApprovalRecord | null> {
        const current = records.get(id);
        if (!current || !from.includes(current.status)) return null;
        const updated: ApprovalRecord = { ...current };
        for (const [field, value] of Object.entries(patch)) {
          if (value !== undefined) {
            (updated as unknown as Record<string, unknown>)[field] = value;
          }
        }
        records.set(id, updated);
        return structuredClone(updated);
      },
    };
  }
}
