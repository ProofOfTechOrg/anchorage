// The subdomain <-> tenant cross-check: defense in depth over INV-2. The
// interesting property is the DENY — tenant A's token presented on tenant
// B's branded host must not quietly operate on A's data — plus the pass-
// through carve-outs (apex, reserved infra subs, hosts outside the apex).

import { describe, expect, it } from 'vitest';

import {
  type ApprovalService,
  createTenantResolver,
  InMemoryApprovalStoreFactory,
  TenantResolutionError,
} from '../approval-api/index.js';
import { ApprovalService as Service } from '../approval-api/index.js';
import { provisionTenant } from './tenant-registry.js';
import {
  subdomainTenantOf,
  withSubdomainCrossCheck,
} from './subdomain-check.js';

const APEX = { apexDomain: 'example.com' };

describe('subdomainTenantOf', () => {
  it.each([
    ['acme.example.com', 'acme'],
    ['ACME.EXAMPLE.COM', 'acme'],
    ['dm0011223344556677aa.example.com', 'dm0011223344556677aa'],
  ])('%s addresses tenant %s', (host, tenant) => {
    expect(subdomainTenantOf(host, APEX)).toBe(tenant);
  });

  it.each([
    ['the apex itself', 'example.com'],
    ['outside the apex', 'evil.test'],
    ['a lookalike suffix', 'evilexample.com'],
    ['a deeper level', 'a.b.example.com'],
    ['reserved: www', 'www.example.com'],
    ['reserved: api', 'api.example.com'],
    ['reserved: app', 'app.example.com'],
    ['reserved: admin', 'admin.example.com'],
  ])('%s is NOT tenant-addressed', (_label, host) => {
    expect(subdomainTenantOf(host, APEX)).toBeUndefined();
  });
});

describe('withSubdomainCrossCheck', () => {
  function makeResolve(tenantId: string) {
    const backend = new InMemoryApprovalStoreFactory();
    return createTenantResolver({
      authenticate: () => ({ id: 'u1', role: 'operator', tenantId }),
      storeFactory: backend,
      buildService: (store): ApprovalService => new Service({ store }),
    });
  }

  it("denies tenant A's token on tenant B's subdomain (the routers' 403)", async () => {
    // #given
    const resolve = withSubdomainCrossCheck(makeResolve('acme'), APEX);

    // #when / #then
    await expect(
      resolve(new Request('https://bravo.example.com/workflows')),
    ).rejects.toBeInstanceOf(TenantResolutionError);
  });

  it("passes the token on its OWN tenant's subdomain and on non-tenant hosts", async () => {
    // #given
    const resolve = withSubdomainCrossCheck(makeResolve('acme'), APEX);

    // #when / #then — own subdomain, apex, reserved infra, and preview hosts
    for (const url of [
      'https://acme.example.com/workflows',
      'https://example.com/workflows',
      'https://api.example.com/workflows',
      'https://preview.workers.dev/workflows',
    ]) {
      expect((await resolve(new Request(url)))?.tenantId).toBe('acme');
    }
  });

  it('still yields undefined (401) for unauthenticated requests — the check never mints identity', async () => {
    // #given — an authenticate that finds nobody
    const backend = new InMemoryApprovalStoreFactory();
    const resolve = withSubdomainCrossCheck(
      createTenantResolver({
        authenticate: () => undefined,
        storeFactory: backend,
        buildService: (store) => new Service({ store }),
      }),
      APEX,
    );

    // #when / #then
    expect(
      await resolve(new Request('https://acme.example.com/workflows')),
    ).toBeUndefined();
  });
});

describe('reserved tenant slugs (the provisioning half of the check)', () => {
  it.each(['www', 'api', 'app', 'docs', 'admin', 'status'])(
    "provisionTenant refuses the reserved slug '%s'",
    async (slug) => {
      // #given — a registry db stub that must never be reached for the insert
      const statements: string[] = [];
      const db = {
        prepare: (query: string) => {
          statements.push(query);
          return {
            bind: () => db.prepare(query),
            run: async () => ({}),
          };
        },
      };

      // #when / #then — a tenant named like shared infrastructure could
      // satisfy host-tenant === token-tenant on a shared host
      await expect(
        provisionTenant(db, { tenantId: slug, kind: 'commercial' }),
      ).rejects.toThrow(/reserved/);
      expect(statements.filter((s) => s.startsWith('INSERT'))).toEqual([]);
    },
  );
});
