// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';
import {
  THREAD_ACTOR_HEADER,
  THREAD_ACTOR_ROLE_HEADER,
  THREAD_TENANT_HEADER,
} from '../do-runner/thread-header.js';
import type {
  CreateTenantResolverOptions,
  TenantContext,
} from './tenant-context.js';
import {
  createTenantResolver,
  TenantResolutionError,
} from './tenant-context.js';
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

describe('createTenantResolver — non-string tenantId refusal (INV-3 belt, F2)', () => {
  // The bound context's mints live above; this pins the resolver's OWN belt
  // (otherwise exercised via run-router.test.ts) against a NON-string tenantId
  // a custom TokenVerifier could smuggle past its own typeof check. RegExp.test
  // coerces undefined -> 'undefined' (an INV-3-valid slug) and would collapse
  // every such principal into ONE shared bucket; the typeof guard refuses it.
  it.each<[string, unknown]>([
    ['undefined', undefined],
    ['null', null],
    // The sneakiest case: String(['acme']) === 'acme', so a bare
    // TENANT_ID_PATTERN.test(['acme']) COERCES to a valid slug and would scope
    // the request to tenant 'acme'. Only the typeof guard refuses it.
    ['an array coercing to a valid-looking slug', ['acme']],
  ])('rejects a verifier returning a %s tenantId with TenantResolutionError, never a scoped request', async (_label, tenantId) => {
    // #given — a verifier whose actor carries a non-string tenantId
    const resolve = createTenantResolver({
      authenticate: () => ({
        id: 'actor-x',
        role: 'admin',
        tenantId: tenantId as unknown as string,
      }),
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('service() must never be reached for a refused tenant');
      },
    });

    // #when / #then — a 403-class refusal, not a bound context
    await expect(
      resolve(new Request('https://host.example/')),
    ).rejects.toBeInstanceOf(TenantResolutionError);
  });
});

describe('createTenantResolver authenticated actor validation', () => {
  it.each([
    ['empty actor id', { id: '', role: 'admin', tenantId: 'acme' }],
    ['whitespace actor id', { id: '   ', role: 'admin', tenantId: 'acme' }],
    [
      'header-invalid actor id',
      { id: 'actor-1\r\nx-forged: yes', role: 'admin', tenantId: 'acme' },
    ],
    ['unknown actor role', { id: 'actor-1', role: 'root', tenantId: 'acme' }],
  ])('rejects an %s from a custom authenticator', async (_label, actor) => {
    const resolve = createTenantResolver({
      authenticate: () => actor as never,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('service must not be built');
      },
    });

    await expect(
      resolve(new Request('https://host.example/')),
    ).rejects.toBeInstanceOf(TenantResolutionError);
  });
});

describe('createTenantResolver server-stamped header boundary', () => {
  it.each([
    THREAD_TENANT_HEADER,
    THREAD_ACTOR_HEADER,
    THREAD_ACTOR_ROLE_HEADER,
  ])('refuses a mixed-case inbound %s header before authentication', async (header) => {
    const authenticate = vi.fn(() => ({
      id: 'actor-1',
      role: 'admin' as const,
      tenantId: 'acme',
    }));
    const resolve = createTenantResolver({
      authenticate,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('service must not be built');
      },
    });
    const request = new Request('https://host.example/', {
      headers: { [header.toUpperCase()]: 'forged' },
    });

    await expect(resolve(request)).rejects.toBeInstanceOf(
      TenantResolutionError,
    );
    expect(authenticate).not.toHaveBeenCalled();
  });
});
