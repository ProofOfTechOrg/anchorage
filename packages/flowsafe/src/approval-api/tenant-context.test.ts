// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import type { TenantContext } from './tenant-context.js';
import { createTenantResolver } from './tenant-context.js';
import { InMemoryApprovalStoreFactory } from './tenant-store.js';

// The resolver's refusal paths (non-INV-3 tenants, the reserved 'system'
// identity) are pinned through the routers in run-router.test.ts; these
// tests cover the bound context's id mints.
async function resolveTenant(tenantId = 'acme'): Promise<TenantContext> {
  const resolve = createTenantResolver({
    authenticate: () => ({ id: 'actor-1', role: 'admin', tenantId }),
    storeFactory: new InMemoryApprovalStoreFactory(),
    buildService: () => {
      throw new Error('service() untouched by these tests');
    },
    newRunId: () => 'uuid-1',
  });
  const tenant = await resolve(new Request('https://host.example/'));
  if (!tenant) throw new Error('expected an authenticated tenant');
  return tenant;
}

describe('TenantContext memory-id mints', () => {
  it('mints salted thread ids over the SAME uuid seam as runIds', async () => {
    // #given
    const tenant = await resolveTenant();
    // #when / #then — one injected generator drives both mints
    expect(tenant.newRunId()).toBe('acme_uuid-1');
    expect(tenant.newThreadId()).toBe('acme_uuid-1');
  });

  it('salts the resource business key with the authenticated tenant', async () => {
    // #given
    const tenant = await resolveTenant();
    // #when / #then
    expect(tenant.newResourceId('user-1')).toBe('acme_user-1');
    expect(() => tenant.newResourceId('a/b')).toThrow(/PATH_SAFE_ID_PATTERN/);
  });

  it('ownsMemoryId is exact at the tenant boundary', async () => {
    // #given — the acme vs acmecorp pin, through the bound context
    const tenant = await resolveTenant();
    // #when / #then
    expect(tenant.ownsMemoryId('acme_thread-1')).toBe(true);
    expect(tenant.ownsMemoryId('acmecorp_thread-1')).toBe(false);
    expect(tenant.ownsMemoryId('acme')).toBe(false);
  });
});
