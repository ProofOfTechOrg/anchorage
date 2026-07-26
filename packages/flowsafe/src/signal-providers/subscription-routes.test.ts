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
  type SignalProviderAuditSink,
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
  opts: {
    resolveTo?: TenantContext | undefined;
    role?: ApprovalRole;
    maxBodyBytes?: number;
    reconcilePolling?: (tenant: TenantContext) => Promise<void>;
    audit?: SignalProviderAuditSink;
  } = {},
) {
  const factory = new InMemorySubscriptionStoreFactory();
  const events: SignalProviderAuditEvent[] = [];
  const resolved =
    'resolveTo' in opts ? opts.resolveTo : ctx('acme', opts.role ?? 'operator');
  const router = createSubscriptionRouter({
    resolve: async () => resolved,
    subscriptions: factory,
    knownProviders: ['github'],
    audit:
      opts.audit ??
      ((e) => {
        events.push(e);
      }),
    ...(opts.reconcilePolling
      ? { reconcilePolling: opts.reconcilePolling }
      : {}),
    ...(opts.maxBodyBytes !== undefined
      ? { maxBodyBytes: opts.maxBodyBytes }
      : {}),
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
  it.each([
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
  ])('rejects an invalid body cap synchronously: %s', (maxBodyBytes) => {
    expect(() => setup({ maxBodyBytes })).toThrow(RangeError);
  });

  it('accepts a zero body cap and rejects every non-empty mutation body', async () => {
    const { router } = setup({ maxBodyBytes: 0 });
    expect(
      (
        await router(
          req('POST', 'acme_t1', {
            providerId: 'github',
            externalResourceId: 'github:acme/repo',
            resourceKey: 'owner',
          }),
        )
      )?.status,
    ).toBe(413);
  });

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
    expect(events.at(-1)).not.toHaveProperty('pollingLifecycle');

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

  it('reconciles polling after subscribe and every idempotent unsubscribe', async () => {
    const reconcilePolling = vi.fn(async (_tenant: TenantContext) => {});
    const { router, events } = setup({ reconcilePolling });

    const subscribed = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'owner',
      }),
    );
    expect(subscribed?.status).toBe(200);
    expect(reconcilePolling).toHaveBeenCalledTimes(1);
    expect(reconcilePolling.mock.calls[0]?.[0]).toMatchObject({
      tenantId: 'acme',
      actor: { tenantId: 'acme' },
    });
    expect(events.at(-1)).toMatchObject({
      action: 'subscribe',
      outcome: 'accepted',
      pollingLifecycle: 'reconciled',
    });

    const removed = await router(
      req('DELETE', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
      }),
    );
    expect(await removed?.json()).toEqual({ removed: true });

    const alreadyAbsent = await router(
      req('DELETE', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
      }),
    );
    expect(await alreadyAbsent?.json()).toEqual({ removed: false });
    expect(reconcilePolling).toHaveBeenCalledTimes(3);
    expect(events.at(-1)).toMatchObject({
      action: 'unsubscribe',
      outcome: 'accepted',
      pollingLifecycle: 'reconciled',
    });
  });

  it('reports a post-commit reconcile failure as retry-safe 502 while retaining the row', async () => {
    const reconcilePolling = vi.fn(async () => {
      throw new Error('private provider-host failure');
    });
    const logged: string[] = [];
    const log = vi.spyOn(console, 'error').mockImplementation((value) => {
      logged.push(String(value));
    });
    const { router, factory, events } = setup({ reconcilePolling });

    try {
      const response = await router(
        req('POST', 'acme_t1', {
          providerId: 'github',
          externalResourceId: 'github:acme/repo',
          resourceKey: 'owner',
        }),
      );

      expect(response?.status).toBe(502);
      const payload = await response?.json();
      expect(payload).toEqual({
        error: 'polling lifecycle unavailable',
        mutationApplied: true,
      });
      expect(
        await factory.forTenant('acme').listForThread('acme_t1'),
      ).toHaveLength(1);
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        action: 'subscribe',
        outcome: 'accepted',
        pollingLifecycle: 'failed',
        reason: 'polling-reconcile-failed',
      });
      expect(logged.join('\n')).toContain('private provider-host failure');
      expect(JSON.stringify(payload)).not.toContain(
        'private provider-host failure',
      );
    } finally {
      log.mockRestore();
    }
  });

  it('does not reconcile GETs or requests rejected before a mutation', async () => {
    const reconcilePolling = vi.fn(async () => {});
    const { router } = setup({ reconcilePolling });

    expect((await router(req('GET', 'acme_t1')))?.status).toBe(200);
    expect(
      (
        await router(
          req('POST', 'acme_t1', {
            providerId: 'unknown',
            externalResourceId: 'resource',
            resourceKey: 'owner',
          }),
        )
      )?.status,
    ).toBe(400);
    expect(reconcilePolling).not.toHaveBeenCalled();
  });

  it('contains an audit failure after commit instead of returning a false mutation failure', async () => {
    const audit = vi.fn(async (_event: SignalProviderAuditEvent) => {
      throw new Error('audit sink unavailable');
    });
    const logged: string[] = [];
    const log = vi.spyOn(console, 'error').mockImplementation((value) => {
      logged.push(String(value));
    });
    const { router, factory } = setup({ audit });

    try {
      const response = await router(
        req('POST', 'acme_t1', {
          providerId: 'github',
          externalResourceId: 'github:acme/repo',
          resourceKey: 'owner',
        }),
      );

      expect(response?.status).toBe(200);
      expect(
        await factory.forTenant('acme').listForThread('acme_t1'),
      ).toHaveLength(1);
      expect(audit).toHaveBeenCalledTimes(1);
      expect(audit.mock.calls[0]?.[0]).toMatchObject({
        action: 'subscribe',
        outcome: 'accepted',
      });
      expect(logged.join('\n')).toContain('audit sink unavailable');
    } finally {
      log.mockRestore();
    }
  });

  it('does not reconcile when the durable row mutation fails', async () => {
    const reconcilePolling = vi.fn(async () => {});
    const router = createSubscriptionRouter({
      resolve: async () => ctx('acme', 'operator'),
      subscriptions: {
        forTenant: () => ({
          subscribe: async () => {
            throw new Error('database unavailable');
          },
        }),
      } as never,
      knownProviders: ['github'],
      reconcilePolling,
    });

    const response = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'owner',
      }),
    );
    expect(response?.status).toBe(500);
    expect(reconcilePolling).not.toHaveBeenCalled();
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
