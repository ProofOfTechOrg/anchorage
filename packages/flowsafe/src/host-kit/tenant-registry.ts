// The tenantId allocation authority. Every isolation invariant treats
// tenantId as axiomatically unique — the salted runId prefix, the bound
// store's tenant_id predicate, the R2 key segment — and NOTHING else enforces
// that. An ops-side collision (two paying clients both slugged 'acme') would
// silently merge runs, approvals, rate limits, and artifacts, so provisioning
// must INSERT here — insert-or-fail on the PRIMARY KEY — BEFORE any token
// naming that tenantId is issued. The demo worker's ephemeral tenants go
// through the same gate (its demo_tenants registry references this table).

import {
  RESERVED_TENANT_IDS,
  TENANT_ID_PATTERN,
} from '../do-runner/path-safe-id.js';

/** Structural D1 subset (tests back it with node:sqlite, Workers pass env.DB). */
export interface TenantRegistryDatabase {
  prepare(query: string): TenantRegistryStatement;
}

export interface TenantRegistryStatement {
  bind(...values: unknown[]): TenantRegistryStatement;
  run(): Promise<unknown>;
}

const CREATE_TENANTS_TABLE = `CREATE TABLE IF NOT EXISTS tenants (
  tenant_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  created_at TEXT NOT NULL
)`;

/**
 * The TCB's own audit identity ('system'). Defined in the path-safe-id leaf —
 * approval-api's createTenantResolver enforces it too, and importing host-kit
 * from there would invert the layering — and re-exported here so the public
 * surface stays `./host-kit`. Full rationale at the definition.
 */
export { RESERVED_TENANT_IDS } from '../do-runner/path-safe-id.js';

/**
 * Slugs that can never be ALLOCATED to a client, for three distinct reasons:
 * the TCB identity above; shared-infrastructure subdomains the commercial
 * host's subdomain cross-check treats as non-tenant hosts (rejecting them AT
 * PROVISIONING is the robust half of that check — a tenant literally named
 * 'www' could otherwise satisfy `host-tenant === token-tenant` on a shared
 * host); and 'default', the conventional single-tenant id (deploy/README.md),
 * reserved so a community host running it can adopt this registry later
 * without finding 'default' sold to a client. Of these only
 * RESERVED_TENANT_IDS bites at authentication — a single-tenant host whose
 * tenant is named 'api' or 'default' still authenticates. 'default' is
 * deliberately a list entry, not an exported constant: a blessed
 * DEFAULT_TENANT value would invite the `tenantId ?? DEFAULT_TENANT`
 * fail-open this design exists to prevent.
 */
export const RESERVED_FOR_ALLOCATION: readonly string[] = [
  ...RESERVED_TENANT_IDS,
  'app',
  'www',
  'api',
  'docs',
  'admin',
  'status',
  'default',
];

/** A provisioning attempt for a tenantId that already exists. */
export class TenantCollisionError extends Error {
  constructor(tenantId: string) {
    super(
      `tenant '${tenantId}' is already provisioned — tenant ids are never reused; pick a different slug`,
    );
    this.name = 'TenantCollisionError';
  }
}

export interface ProvisionTenantOptions {
  tenantId: string;
  /** 'commercial' for paying clients, 'demo' for ephemeral demo tenants. */
  kind: 'commercial' | 'demo';
  /** Clock override for tests. */
  now?: () => number;
}

function isUniqueViolation(error: unknown): boolean {
  return (
    error instanceof Error &&
    /UNIQUE constraint failed|PRIMARY KEY constraint/i.test(error.message)
  );
}

/**
 * Allocate a tenantId, or throw. INV-3 is validated here too — a tenant that
 * cannot be range-purged or prefix-matched must never exist in the first
 * place. Insert-or-fail: a collision throws TenantCollisionError instead of
 * silently adopting the existing row, because "provisioned twice" means two
 * parties believe they own the slug.
 */
export async function provisionTenant(
  db: TenantRegistryDatabase,
  options: ProvisionTenantOptions,
): Promise<void> {
  const { tenantId, kind } = options;
  if (!TENANT_ID_PATTERN.test(tenantId)) {
    throw new Error(
      `tenantId '${tenantId}' violates INV-3 (^[a-z0-9]{3,32}$) — not provisioned`,
    );
  }
  if (RESERVED_FOR_ALLOCATION.includes(tenantId)) {
    throw new Error(
      `tenantId '${tenantId}' is a reserved slug — not provisioned`,
    );
  }
  await db.prepare(CREATE_TENANTS_TABLE).run();
  const createdAt = new Date((options.now ?? Date.now)()).toISOString();
  try {
    await db
      .prepare(
        'INSERT INTO tenants (tenant_id, kind, created_at) VALUES (?, ?, ?)',
      )
      .bind(tenantId, kind, createdAt)
      .run();
  } catch (error) {
    if (isUniqueViolation(error)) throw new TenantCollisionError(tenantId);
    throw error;
  }
}
