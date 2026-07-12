// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import type {
  CreateTenantResolverOptions,
  TenantContext,
} from './tenant-context.js';
import { createTenantResolver } from './tenant-context.js';
import { InMemoryApprovalStoreFactory } from './tenant-store.js';

// The resolver's refusal paths (non-INV-3 tenants, the reserved 'system'
// identity) are pinned through the routers in run-router.test.ts; these
// tests cover the bound context's id mints and its canSelfDecide display hint.
async function resolveTenant(
  tenantId = 'acme',
  allowSelfDecision?: CreateTenantResolverOptions['allowSelfDecision'],
): Promise<TenantContext> {
  const resolve = createTenantResolver({
    authenticate: () => ({ id: 'actor-1', role: 'admin', tenantId }),
    storeFactory: new InMemoryApprovalStoreFactory(),
    buildService: () => {
      throw new Error('service() untouched by these tests');
    },
    newRunId: () => 'uuid-1',
    allowSelfDecision,
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

describe('TenantContext canSelfDecide (display hint)', () => {
  it('is false for every role when no exemption policy is set (SoD on)', async () => {
    // #given — the default: no allowSelfDecision
    const tenant = await resolveTenant();
    // #when / #then — the hint mirrors the fail-closed decide() default
    expect(tenant.canSelfDecide('admin')).toBe(false);
    expect(tenant.canSelfDecide('reviewer')).toBe(false);
    expect(tenant.canSelfDecide('viewer')).toBe(false);
  });

  it('echoes true only for a decider role the policy exempts', async () => {
    // #given — the single-operator config exempts admin
    const tenant = await resolveTenant('acme', { roles: ['admin'] });
    // #when / #then
    expect(tenant.canSelfDecide('admin')).toBe(true);
    expect(tenant.canSelfDecide('reviewer')).toBe(false);
    expect(tenant.canSelfDecide('viewer')).toBe(false);
  });

  it('never echoes true for a non-decider role, even if the policy names it', async () => {
    // #given — a nonsensical policy exempting builder, which cannot decide
    const tenant = await resolveTenant('acme', { roles: ['builder'] });
    // #when / #then — the DECIDER_ROLES intersection keeps it false
    expect(tenant.canSelfDecide('builder')).toBe(false);
  });
});
