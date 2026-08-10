// SPDX-License-Identifier: Apache-2.0
// createSubscriptionRouter — the human-only HTTP subscribe/unsubscribe surface
// (RA-009: NEVER exposed as model tools; nothing here mints capability, P8). The
// gate order mirrors createSignalRouter: resolve → thread-ownership → role →
// memory-id refusal → mutate. Committed mutations and probe-like post-auth
// denials are audited.
import { describe, expect, it, vi } from 'vitest';

import type { ActorContext, ApprovalRole } from '../approval-api/index.js';
import { resourceIdFromKey } from '../do-runner/index.js';
import { RunRouteError } from '../host-kit/index.js';
import {
  InMemorySubscriptionStoreFactory,
  MAX_EXTERNAL_RESOURCE_ID_BYTES,
} from './subscription-d1.js';
import {
  createSubscriptionRouter,
  type SignalProviderAuditEvent,
  type SignalProviderAuditSink,
  type SubscriptionRouterOptions,
} from './webhook-route.js';

function ctx(role: ApprovalRole): ActorContext {
  return {
    actor: { id: 'op', role },
    principal: { kind: 'human', id: 'op', role },
    resourceOwner: { kind: 'human', id: 'op' },
    service: () => {
      throw new Error('approval service is not used in subscription tests');
    },
    newRunId: () => 'run-1',
    newThreadId: () => 'thread-1',
    resourceIdFromKey: resourceIdFromKey,
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async (kind, id) =>
      (kind === 'thread' && id === 'acme_t1') ||
      (kind === 'resource' && (id === 'owner' || id === 'user-42'))
        ? { kind: 'human', id: 'op' }
        : undefined,
    canAccessResource: async (kind, id) =>
      (kind === 'thread' && id === 'acme_t1') ||
      (kind === 'resource' && (id === 'owner' || id === 'user-42')),
    canSelfDecide: () => false,
  };
}

function setup(
  opts: {
    resolveTo?: ActorContext | undefined;
    role?: ApprovalRole;
    maxBodyBytes?: number;
    reconcilePolling?: () => Promise<void>;
    audit?: SignalProviderAuditSink;
    validateThreadTarget?: SubscriptionRouterOptions['validateThreadTarget'];
  } = {},
) {
  const factory = new InMemorySubscriptionStoreFactory();
  const events: SignalProviderAuditEvent[] = [];
  const resolved =
    'resolveTo' in opts ? opts.resolveTo : ctx(opts.role ?? 'operator');
  const router = createSubscriptionRouter({
    resolve: async () => resolved,
    subscriptions: factory,
    validateThreadTarget: opts.validateThreadTarget ?? (async () => undefined),
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

  it('404s a path-safe thread owned by another actor before parsing', async () => {
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
        reason: 'resource-not-found',
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

  it.each([
    ['ASCII control', 'github:acme\nrepo'],
    [
      'oversized UTF-8 value',
      'é'.repeat(MAX_EXTERNAL_RESOURCE_ID_BYTES / 2 + 1),
    ],
  ])('400s an %s external resource id before mutation', async (_label, externalResourceId) => {
    const { router, factory } = setup();
    const res = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId,
        resourceKey: 'owner',
      }),
    );

    expect(res?.status).toBe(400);
    expect(await factory.store().listForThread('acme_t1')).toEqual([]);
  });

  it('subscribes, lists, and unsubscribes with a validated resource key', async () => {
    const { router, events } = setup();
    // #when subscribe
    const sub = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'user-42',
      }),
    );
    // #then — resourceId validated through the server context, audited
    expect(sub?.status).toBe(200);
    const created = (await sub?.json()) as {
      subscription: { resourceId: string };
    };
    expect(created.subscription.resourceId).toBe('user-42');
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

  it('lets an admin manage a foreign bound thread without changing its owner', async () => {
    const admin = ctx('admin');
    const foreignOwner = { kind: 'human' as const, id: 'alice' };
    admin.resourceOwnerFor = async (kind, id) =>
      (kind === 'thread' && id === 'acme_t1') ||
      (kind === 'resource' && id === 'user-42')
        ? foreignOwner
        : undefined;
    admin.canAccessResource = async () => true;
    const { router } = setup({ resolveTo: admin });

    const response = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'user-42',
      }),
    );

    expect(response?.status).toBe(200);
  });

  it('refuses an authorized resource that is not the thread binding', async () => {
    const validateThreadTarget = vi.fn(async () => {
      throw new RunRouteError(404, 'agent not found');
    });
    const { router, factory } = setup({ validateThreadTarget });

    const response = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'user-42',
      }),
    );

    expect(response?.status).toBe(404);
    expect(validateThreadTarget).toHaveBeenCalledWith(expect.anything(), {
      threadId: 'acme_t1',
      resourceId: 'user-42',
    });
    expect(await factory.store().listForThread('acme_t1')).toEqual([]);
  });

  it('refuses a thread and resource owned by different principals', async () => {
    const base = ctx('operator');
    const resolved: ActorContext = {
      ...base,
      resourceOwnerFor: async (kind) =>
        kind === 'thread'
          ? { kind: 'human', id: 'op' }
          : { kind: 'human', id: 'other' },
    };
    const validateThreadTarget = vi.fn(async () => undefined);
    const { router, factory } = setup({
      resolveTo: resolved,
      validateThreadTarget,
    });

    const response = await router(
      req('POST', 'acme_t1', {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        resourceKey: 'owner',
      }),
    );

    expect(response?.status).toBe(404);
    expect(validateThreadTarget).not.toHaveBeenCalled();
    expect(await factory.store().listForThread('acme_t1')).toEqual([]);
  });

  it('reconciles polling after subscribe and every idempotent unsubscribe', async () => {
    const reconcilePolling = vi.fn(async () => {});
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
    expect(reconcilePolling.mock.calls[0]).toEqual([]);
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

  it('returns the committed mutation when post-commit polling reconciliation fails', async () => {
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

      expect(response?.status).toBe(200);
      const payload = await response?.json();
      expect(payload).toMatchObject({ subscription: { providerId: 'github' } });
      expect(await factory.store().listForThread('acme_t1')).toHaveLength(1);
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
      expect(await factory.store().listForThread('acme_t1')).toHaveLength(1);
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
      resolve: async () => ctx('operator'),
      subscriptions: {
        store: () => ({
          subscribe: async () => {
            throw new Error('database unavailable');
          },
        }),
      } as never,
      knownProviders: ['github'],
      validateThreadTarget: async () => undefined,
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
      resolve: async () => ctx('operator'),
      subscriptions: {
        store: () => ({
          listForThread: async () => {
            throw new Error('private database detail');
          },
        }),
      } as never,
      knownProviders: ['github'],
      validateThreadTarget: async () => undefined,
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
