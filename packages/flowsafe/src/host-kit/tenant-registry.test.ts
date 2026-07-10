// The tenants table is the tenantId allocation authority — insert-or-fail is
// what stops two clients silently merging under one slug. Exercised against
// real SQLite (node:sqlite), same posture as the approval store's tests.

import { describe, expect, it } from 'vitest';

import { openSqlite, type SqliteDatabase } from '../../test-support/sqlite.js';
import {
  provisionTenant,
  TenantCollisionError,
  type TenantRegistryDatabase,
  type TenantRegistryStatement,
} from './tenant-registry.js';

function d1Like(db: SqliteDatabase): TenantRegistryDatabase {
  function statement(sql: string, params: unknown[]): TenantRegistryStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      run: async () => db.prepare(sql).run(...params),
    };
  }
  return { prepare: (sql: string) => statement(sql, []) };
}

describe('provisionTenant', () => {
  it('allocates a fresh tenant and records kind + created_at', async () => {
    // #given
    const sqlite = openSqlite();

    // #when
    await provisionTenant(d1Like(sqlite), {
      tenantId: 'acme',
      kind: 'commercial',
      now: () => 1_751_000_000_000,
    });

    // #then
    expect(sqlite.prepare('SELECT * FROM tenants').all()).toEqual([
      {
        tenant_id: 'acme',
        kind: 'commercial',
        created_at: new Date(1_751_000_000_000).toISOString(),
      },
    ]);
  });

  it('throws TenantCollisionError on a duplicate slug — never silently merges', async () => {
    // #given
    const db = d1Like(openSqlite());
    await provisionTenant(db, { tenantId: 'acme', kind: 'commercial' });

    // #when / #then — a second party claiming 'acme' must fail loudly
    await expect(
      provisionTenant(db, { tenantId: 'acme', kind: 'demo' }),
    ).rejects.toBeInstanceOf(TenantCollisionError);
  });

  it.each([
    'Acme',
    'a_b',
    'ab',
    'a'.repeat(33),
    'a-b',
    '',
  ])("rejects the non-INV-3 tenantId '%s' before touching the table", async (tenantId) => {
    // #when / #then — a tenant that cannot be range-purged or
    // prefix-matched must never exist
    await expect(
      provisionTenant(d1Like(openSqlite()), {
        tenantId,
        kind: 'commercial',
      }),
    ).rejects.toThrow(/INV-3/);
  });
});
