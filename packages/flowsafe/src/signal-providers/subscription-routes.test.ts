// SPDX-License-Identifier: Apache-2.0
// createSubscriptionRouter — the human-only HTTP subscribe/unsubscribe surface
// (RA-009: NEVER exposed as model tools; nothing here mints capability, P8). The
// gate order mirrors createSignalRouter: resolve → role → thread-ownership →
// memory-id refusal → mutate, every outcome audited.
import { describe, expect, it, vi } from 'vitest';

import type { ApprovalRole, TenantContext } from '../approval-api/index.js';
import { mintResourceId, tenantOwnsMemoryId } from '../do-runner/index.js';
import { InMemorySubscriptionStoreFactory } from './subscription-d1.js';
import {
  createSubscriptionRouter,
  type SignalProviderAuditEvent,
} from './webhook-route.js';

function ctx(tenantId: string, role: ApprovalRole): TenantContext {
  return {
    actor: { id: 'op', role, tenantId },
    tenantId,
    newResourceId: (key: string) => mintResourceId(tenantId, key),
    ownsMemoryId: (id: string) => tenantOwnsMemoryId(tenantId, id),
  } as unknown as TenantContext;
}

function setup(
  opts: { resolveTo?: TenantContext | undefined; role?: ApprovalRole } = {},
) {
  const factory = new InMemorySubscriptionStoreFactory();
  const events: SignalProviderAuditEvent[] = [];
  const resolved =
    'resolveTo' in opts ? opts.resolveTo : ctx('acme', opts.role ?? 'operator');
  const router = createSubscriptionRouter({
    resolve: async () => resolved,
    subscriptions: factory,
    knownProviders: ['github'],
    audit: (e) => {
      events.push(e);
    },
  });
  return { router, factory, events };
}

function req(method: string, threadId: string, body?: unknown): Request {
  return new Request(`http://host/api/threads/${threadId}/subscriptions`, {
    method,
    headers: { 'content-type': 'application/json' },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
}

describe('createSubscriptionRouter', () => {
  it('returns null for a path it does not own', async () => {
    const { router } = setup();
    expect(await router(new Request('http://host/api/other'))).toBeNull();
  });

  it('401s an unauthenticated request', async () => {
    const { router } = setup({ resolveTo: undefined });
    const res = await router(req('POST', 'acme_t1', {}));
    expect(res?.status).toBe(401);
  });

  it('403s a read-only role and audits it', async () => {
    const { router, events } = setup({ role: 'viewer' });
    const res = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'owner',
      }),
    );
    expect(res?.status).toBe(403);
    expect(events).toEqual([
      expect.objectContaining({
        outcome: 'rejected',
        reason: 'forbidden-role',
      }),
    ]);
  });

  it('404s a foreign threadId (no existence oracle) and audits it', async () => {
    const { router, events } = setup();
    const res = await router(
      req('POST', 'globex_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'owner',
      }),
    );
    expect(res?.status).toBe(404);
    expect(events).toEqual([
      expect.objectContaining({
        outcome: 'rejected',
        reason: 'foreign-thread',
      }),
    ]);
  });

  it('400s a body that names a client memory id', async () => {
    const { router, events } = setup();
    const res = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'owner',
        resourceId: 'acme_smuggled', // banned
      }),
    );
    expect(res?.status).toBe(400);
    expect(events).toEqual([
      expect.objectContaining({
        outcome: 'rejected',
        reason: 'client-memory-id',
      }),
    ]);
  });

  it('400s an unknown provider', async () => {
    const { router } = setup();
    const res = await router(
      req('POST', 'acme_t1', {
        providerId: 'gitlab', // not in knownProviders
        externalResourceId: 'x',
        resourceKey: 'owner',
      }),
    );
    expect(res?.status).toBe(400);
  });

  it('subscribes, lists, and unsubscribes — server-minting the resourceId', async () => {
    const { router, events } = setup();
    // #when subscribe
    const sub = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'user-42',
      }),
    );
    // #then — resourceId minted server-side (tenant-salted), audited
    expect(sub?.status).toBe(200);
    const created = (await sub?.json()) as {
      subscription: { resourceId: string };
    };
    expect(created.subscription.resourceId).toBe('acme_user-42');
    expect(events.at(-1)).toMatchObject({
      action: 'subscribe',
      outcome: 'accepted',
    });

    // #when list
    const list = await router(req('GET', 'acme_t1'));
    expect(
      ((await list?.json()) as { subscriptions: unknown[] }).subscriptions,
    ).toHaveLength(1);

    // #when unsubscribe
    const del = await router(
      req('DELETE', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
      }),
    );
    expect(await del?.json()).toEqual({ removed: true });
    expect(events.at(-1)).toMatchObject({ action: 'unsubscribe' });
  });

  it('405s a method it does not handle', async () => {
    const { router } = setup();
    const res = await router(req('PUT', 'acme_t1', {}));
    expect(res?.status).toBe(405);
  });

  it('400s a malformed percent-encoded threadId (no decode crash)', async () => {
    const { router } = setup();
    const res = await router(
      new Request('http://host/api/threads/%/subscriptions', { method: 'GET' }),
    );
    expect(res?.status).toBe(400);
  });

  it('audits a GET as a `list` read — never mislabeled a subscribe', async () => {
    const { router, events } = setup();
    // A successful list is audited as `list`, accepted.
    const ok = await router(req('GET', 'acme_t1'));
    expect(ok?.status).toBe(200);
    expect(events.at(-1)).toMatchObject({
      action: 'list',
      outcome: 'accepted',
    });
  });

  it('audits a DENIED GET as a `list` rejection (role) — never a subscribe', async () => {
    const { router, events } = setup({ role: 'viewer' });
    const res = await router(req('GET', 'acme_t1'));
    expect(res?.status).toBe(403);
    expect(events.at(-1)).toMatchObject({
      action: 'list',
      outcome: 'rejected',
      reason: 'forbidden-role',
    });
  });

  it('returns a generic internal error, logs detail, and audits the rejection', async () => {
    const events: SignalProviderAuditEvent[] = [];
    const logged: string[] = [];
    const log = vi.spyOn(console, 'error').mockImplementation((value) => {
      logged.push(String(value));
    });
    const router = createSubscriptionRouter({
      resolve: async () => ctx('acme', 'operator'),
      subscriptions: {
        forTenant: () => ({
          listForThread: async () => {
            throw new Error('private database detail');
          },
        }),
      } as never,
      knownProviders: ['github'],
      audit: (event) => {
        events.push(event);
      },
    });

    try {
      const response = await router(req('GET', 'acme_t1'));
      expect(response?.status).toBe(500);
      expect(await response?.json()).toEqual({ error: 'internal error' });
      expect(response?.headers.get('cache-control')).toBe('no-store');
      expect(events.at(-1)).toMatchObject({
        outcome: 'rejected',
        reason: 'internal-error',
      });
      expect(logged.join('\n')).toContain('private database detail');
    } finally {
      log.mockRestore();
    }
  });
});
