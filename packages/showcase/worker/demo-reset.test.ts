// The demo-reset router's gate order, pinned seam-by-seam: exact path ->
// method -> authenticate -> admin role -> demo tenant -> purge. The purge
// seam must never fire on a refusal — the whole point of the ordering.

import type {
  ApprovalRole,
  TenantContext,
  TenantResolver,
} from '@proofoftech/flowsafe/approval-api';
import { TenantResolutionError } from '@proofoftech/flowsafe/approval-api';
import type { PurgeTenantResult } from '@proofoftech/flowsafe/do-runner';
import { describe, expect, it, vi } from 'vitest';
import { createDemoResetRouter } from '#worker/demo-reset';

const PURGED: PurgeTenantResult = {
  snapshots: 2,
  threads: 0,
  messages: 0,
  resources: 0,
  backgroundTasks: 0,
  approvals: 1,
  artifacts: 0,
};

function tenantContext(
  role: ApprovalRole,
  tenantId = 'dm0a1b2c3d',
): TenantContext {
  return {
    actor: { id: `demo-${role}`, role, tenantId },
    tenantId,
    service: () => {
      throw new Error('service() must not be touched by the reset route');
    },
    newRunId: () => `${tenantId}_unused`,
    ownsRun: (runId: string) => runId.startsWith(`${tenantId}_`),
    newThreadId: () => `${tenantId}_unused-thread`,
    newResourceId: (resourceKey: string) => `${tenantId}_${resourceKey}`,
    ownsMemoryId: (id: string) => id.startsWith(`${tenantId}_`),
    // The reset route never reads this; a constant false satisfies the
    // widened TenantContext interface.
    canSelfDecide: () => false,
  };
}

interface Seams {
  resolve?: TenantResolver;
  isDemo?: boolean;
  purge?: () => Promise<PurgeTenantResult>;
}

function makeRouter(seams: Seams = {}) {
  const isDemoTenant = vi.fn(async () => seams.isDemo ?? true);
  const purgeTenantData = vi.fn(seams.purge ?? (async () => PURGED));
  const router = createDemoResetRouter({
    resolve: seams.resolve ?? (async () => tenantContext('admin')),
    isDemoTenant,
    purgeTenantData,
  });
  return { router, isDemoTenant, purgeTenantData };
}

function post(path = '/demo/reset'): Request {
  return new Request(`https://demo.example${path}`, { method: 'POST' });
}

describe('createDemoResetRouter', () => {
  it('returns null for paths it does not own, without touching any seam', async () => {
    // #given
    const { router, isDemoTenant, purgeTenantData } = makeRouter();

    // #when
    const responses = await Promise.all([
      router(post('/demo/reset/extra')),
      router(post('/demo')),
      router(new Request('https://demo.example/runs', { method: 'POST' })),
    ]);

    // #then — unowned paths fall through to the host's next router
    expect(responses).toEqual([null, null, null]);
    expect(isDemoTenant).not.toHaveBeenCalled();
    expect(purgeTenantData).not.toHaveBeenCalled();
  });

  it('405s a non-POST on the owned path before authenticating', async () => {
    // #given — a resolve that would throw if consulted
    const resolve = vi.fn(async () => {
      throw new Error('must not authenticate a GET');
    });
    const { router } = makeRouter({
      resolve: resolve as unknown as TenantResolver,
    });

    // #when
    const response = await router(
      new Request('https://demo.example/demo/reset', { method: 'GET' }),
    );

    // #then
    expect(response?.status).toBe(405);
    expect(resolve).not.toHaveBeenCalled();
  });

  it('401s an unauthenticated POST without purging', async () => {
    // #given
    const { router, purgeTenantData } = makeRouter({
      resolve: async () => undefined,
    });

    // #when
    const response = await router(post());

    // #then
    expect(response?.status).toBe(401);
    expect(purgeTenantData).not.toHaveBeenCalled();
  });

  it.each([
    'operator',
    'reviewer',
    'viewer',
  ] as const)("403s the '%s' role, names the admin requirement, and never purges", async (role) => {
    // #given
    const { router, isDemoTenant, purgeTenantData } = makeRouter({
      resolve: async () => tenantContext(role),
    });

    // #when
    const response = await router(post());

    // #then — refused BEFORE the demo-tenant lookup
    expect(response?.status).toBe(403);
    const body = (await response?.json()) as { error: string };
    expect(body.error).toContain('admin');
    expect(body.error).toContain(role);
    expect(isDemoTenant).not.toHaveBeenCalled();
    expect(purgeTenantData).not.toHaveBeenCalled();
  });

  it('403s an admin of a NON-demo tenant without purging', async () => {
    // #given — a commercial/static-token tenant
    const { router, purgeTenantData } = makeRouter({
      resolve: async () => tenantContext('admin', 'acme'),
      isDemo: false,
    });

    // #when
    const response = await router(post());

    // #then
    expect(response?.status).toBe(403);
    expect(purgeTenantData).not.toHaveBeenCalled();
  });

  it('200s an admin demo tenant with the purge counts passed through verbatim', async () => {
    // #given
    const { router, isDemoTenant, purgeTenantData } = makeRouter();

    // #when
    const response = await router(post());

    // #then — envelope carries the seam's exact counts for the UI to narrate
    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual({
      ok: true,
      tenantId: 'dm0a1b2c3d',
      purged: PURGED,
    });
    expect(isDemoTenant).toHaveBeenCalledWith('dm0a1b2c3d');
    expect(purgeTenantData).toHaveBeenCalledWith('dm0a1b2c3d');
  });

  it('maps TenantResolutionError to 403 (reserved/malformed tenant claims)', async () => {
    // #given — the resolver refusing a reserved identity
    const { router, purgeTenantData } = makeRouter({
      resolve: async () => {
        throw new TenantResolutionError('reserved tenantId');
      },
    });

    // #when
    const response = await router(post());

    // #then
    expect(response?.status).toBe(403);
    expect(purgeTenantData).not.toHaveBeenCalled();
  });

  it('500s when the purge itself throws', async () => {
    // #given
    const { router } = makeRouter({
      purge: async () => {
        throw new Error('D1 unavailable');
      },
    });

    // #when
    const response = await router(post());

    // #then
    expect(response?.status).toBe(500);
    const body = (await response?.json()) as { error: string };
    expect(body.error).toContain('D1 unavailable');
  });
});
