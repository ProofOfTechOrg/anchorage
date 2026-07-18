// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  createThreadTopology,
  type ThreadNamespaceLike,
} from '../host-kit/index.js';
import type { SignalProviderAdapter } from './provider.js';
import {
  InMemorySubscriptionStoreFactory,
  type SubscriptionStoreFactory,
} from './subscription-d1.js';
import {
  createWebhookRouter,
  type SignalProviderAuditEvent,
} from './webhook-route.js';

// A stub thread namespace whose real topology delivery either reaches the stub
// DO (200) or is 404'd by the topology's ownership check BEFORE it. Records what
// threadId was addressed and the tenant header stamped, so we can prove delivery
// binds to the ROW, never the payload.
function stubThreads(): {
  namespace: ThreadNamespaceLike<string>;
  addressed: string[];
  headers: Array<string | undefined>;
} {
  const addressed: string[] = [];
  const headers: Array<string | undefined> = [];
  return {
    addressed,
    headers,
    namespace: {
      idFromName: (name) => name,
      get: (name) => ({
        fetch: (
          input: Request | string,
          init?: { headers?: Record<string, string> },
        ) => {
          addressed.push(name);
          headers.push(init?.headers?.['x-flowsafe-tenant']);
          void input;
          return Promise.resolve(
            new Response(JSON.stringify({ record: {} }), { status: 200 }),
          );
        },
      }),
    },
  };
}

// A deterministic provider: `x-sig: good` verifies, anything else is forged.
function testProvider(
  overrides: Partial<SignalProviderAdapter> = {},
): SignalProviderAdapter {
  return {
    id: 'test',
    verifyWebhookSignature: (_raw, headers) => headers.get('x-sig') === 'good',
    extractResourceIds: () => ['res:1'],
    buildNotification: () => ({ source: 'test', kind: 'k', summary: 's' }),
    ...overrides,
  };
}

async function seed(
  factory: SubscriptionStoreFactory,
  tenant: string,
  threadId: string,
): Promise<void> {
  await factory.forTenant(tenant).subscribe({
    providerId: 'test',
    externalResourceId: 'res:1',
    threadId,
    resourceId: `${tenant}_owner`,
  });
}

function webhookRequest(sig: string, body: unknown): Request {
  return new Request('http://host/api/signal-providers/test/webhook', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-sig': sig },
    body: JSON.stringify(body),
  });
}

describe('createWebhookRouter — verify before parse', () => {
  it('rejects a forged signature BEFORE any lookup or delivery, and audits it', async () => {
    // #given
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const systemStore = factory.system();
    const listSpy = vi.spyOn(systemStore, 'listByResource');
    const threads = stubThreads();
    const events: SignalProviderAuditEvent[] = [];
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: systemStore,
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      audit: (e) => {
        events.push(e);
      },
    });

    // #when — a forged signature
    const res = await router(webhookRequest('bad', { any: 'payload' }));

    // #then — 401, and NOTHING downstream ran (no lookup, no delivery)
    expect(res?.status).toBe(401);
    expect(listSpy).not.toHaveBeenCalled();
    expect(threads.addressed).toEqual([]);
    expect(events).toEqual([
      expect.objectContaining({
        type: 'signal-provider.webhook',
        outcome: 'rejected',
        reason: 'forged-signature',
      }),
    ]);
  });

  it('delivers a valid webhook to the ROW’s tenant/thread — never the payload’s', async () => {
    // #given — the row is (acme, acme_t1); the payload LIES about tenant/thread
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const threads = stubThreads();
    const events: SignalProviderAuditEvent[] = [];
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.system(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      audit: (e) => {
        events.push(e);
      },
    });

    // #when — payload claims a foreign tenant/thread; it must be ignored
    const res = await router(
      webhookRequest('good', {
        tenantId: 'globex',
        threadId: 'globex_evil',
      }),
    );

    // #then — delivered to the ROW's thread, stamped with the ROW's tenant
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ matched: 1, delivered: 1 });
    expect(threads.addressed).toEqual(['acme_t1']);
    expect(threads.headers).toEqual(['acme']);
    expect(events).toEqual([
      expect.objectContaining({
        outcome: 'accepted',
        matched: 1,
        delivered: 1,
      }),
    ]);
  });

  it('is route-absent (null) when the provider or its secret is not configured', async () => {
    // #given — provider registered but no secret
    const factory = new InMemorySubscriptionStoreFactory();
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.system(),
      topology: createThreadTopology(stubThreads().namespace),
      secretForProvider: () => undefined,
    });
    // #then — null (falls through to the worker's 404 — byte-identical)
    expect(await router(webhookRequest('good', {}))).toBeNull();
    // and an UNKNOWN provider is also route-absent
    const other = new Request(
      'http://host/api/signal-providers/unknown/webhook',
      { method: 'POST', body: '{}' },
    );
    expect(await router(other)).toBeNull();
  });

  it('400s a malformed body AFTER a valid signature (parse only once authentic)', async () => {
    // #given
    const factory = new InMemorySubscriptionStoreFactory();
    const events: SignalProviderAuditEvent[] = [];
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.system(),
      topology: createThreadTopology(stubThreads().namespace),
      secretForProvider: () => 'secret',
      audit: (e) => {
        events.push(e);
      },
    });
    // #when — valid sig, invalid JSON
    const res = await router(
      new Request('http://host/api/signal-providers/test/webhook', {
        method: 'POST',
        headers: { 'x-sig': 'good' },
        body: 'not json',
      }),
    );
    // #then
    expect(res?.status).toBe(400);
    expect(events[0]).toMatchObject({ reason: 'malformed-body' });
  });

  it('skips an over-cap tenant’s deliveries (per provider+tenant rate cap)', async () => {
    // #given — two tenants matched; the cap refuses acme only
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    await seed(factory, 'globex', 'globex_t1');
    const threads = stubThreads();
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.system(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      rateLimit: (_provider, tenantId) => tenantId !== 'acme',
    });
    // #when
    const res = await router(webhookRequest('good', {}));
    // #then — only globex delivered
    expect(await res?.json()).toEqual({ matched: 2, delivered: 1 });
    expect(threads.addressed).toEqual(['globex_t1']);
  });

  it('bounds the forgery audit so a flood cannot amplify the log', async () => {
    // #given — cap forgery audits at 2 per window
    const factory = new InMemorySubscriptionStoreFactory();
    const events: SignalProviderAuditEvent[] = [];
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.system(),
      topology: createThreadTopology(stubThreads().namespace),
      secretForProvider: () => 'secret',
      audit: (e) => {
        events.push(e);
      },
      maxForgeryAuditsPerWindow: 2,
      forgeryAuditWindowMs: 60_000,
      now: () => 1_000,
    });
    // #when — five forged webhooks in one window
    for (let i = 0; i < 5; i += 1) {
      const res = await router(webhookRequest('bad', {}));
      expect(res?.status).toBe(401); // ALWAYS rejected
    }
    // #then — only 2 audited (the log is bounded; the reject is not)
    expect(events).toHaveLength(2);
    expect(events.every((e) => e.reason === 'forged-signature')).toBe(true);
  });
});

describe('createWebhookRouter — robustness', () => {
  function router(secret: string | undefined, provider = testProvider()) {
    const factory = new InMemorySubscriptionStoreFactory();
    return {
      factory,
      run: createWebhookRouter({
        providers: { test: provider },
        subscriptions: factory.system(),
        topology: createThreadTopology(stubThreads().namespace),
        secretForProvider: () => secret,
      }),
    };
  }

  it('does not crash on a malformed percent-encoded path — route-absent, not a URIError', async () => {
    // #given / #when — a lone '%' in the id segment (decodeURIComponent would throw)
    const res = await router('secret').run(
      new Request('http://host/api/signal-providers/%/webhook', {
        method: 'POST',
        body: '{}',
      }),
    );
    // #then — safe-decoded to route-absent, never a thrown URIError
    expect(res).toBeNull();
  });

  it('is route-absent when the secret is EMPTY (never verifies with a zero-length key)', async () => {
    // #then — '' is treated like undefined (fail-closed): no verify, no crash, no bypass
    expect(await router('').run(webhookRequest('good', {}))).toBeNull();
  });

  it('maps a THROWING provider callback to 500, never an unhandled crash', async () => {
    // #given — a matched, verified webhook whose buildNotification throws
    const { factory, run } = router('secret', {
      ...testProvider(),
      buildNotification: () => {
        throw new Error('provider bug');
      },
    });
    await seed(factory, 'acme', 'acme_t1');
    // #when
    const res = await run(webhookRequest('good', {}));
    // #then — caught at the top-level, surfaced as 500 (not an unhandled rejection)
    expect(res?.status).toBe(500);
  });
});

describe('createWebhookRouter — cross-tenant fail-closed', () => {
  it('never delivers to a foreign thread even for a valid signature (tampered row)', async () => {
    // #given — a row whose tenant is acme but whose threadId belongs to globex
    // (a tampered/inconsistent row). The topology must 404 it.
    const factory = new InMemorySubscriptionStoreFactory();
    await factory.forTenant('acme').subscribe({
      providerId: 'test',
      externalResourceId: 'res:1',
      threadId: 'globex_victim', // NOT owned by acme
      resourceId: 'acme_owner',
    });
    const threads = stubThreads();
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.system(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
    });
    // #when — a perfectly valid webhook
    const res = await router(webhookRequest('good', {}));
    // #then — matched, but the topology ownership check 404'd it: nothing delivered,
    // the foreign thread DO never addressed (fail closed).
    expect(await res?.json()).toEqual({ matched: 1, delivered: 0 });
    expect(threads.addressed).toEqual([]);
  });
});
