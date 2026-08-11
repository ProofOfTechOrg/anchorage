// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';
import { EXECUTION_PRINCIPAL_HEADER } from '../do-runner/index.js';
import {
  createThreadTopology as createThreadTopologyWithSecret,
  type ThreadNamespaceLike,
  type ThreadTopology,
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

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

function createThreadTopology<Id>(
  namespace: ThreadNamespaceLike<Id>,
): ThreadTopology {
  return createThreadTopologyWithSecret(namespace, DEPLOYMENT_IDENTITY_SECRET);
}

// A stub thread namespace whose real topology delivery either reaches the stub
// DO (200) or is rejected by path validation before it. Records what threadId
// was addressed and the server-stamped principal, so we can prove delivery
// binds to the ROW, never the payload.
function stubThreads(): {
  namespace: ThreadNamespaceLike<string>;
  addressed: string[];
  paths: string[];
  principals: Array<string | undefined>;
} {
  const addressed: string[] = [];
  const paths: string[] = [];
  const principals: Array<string | undefined> = [];
  return {
    addressed,
    paths,
    principals,
    namespace: {
      idFromName: (name) => name,
      get: (name) => ({
        fetch: (
          input: Request | string,
          init?: { headers?: Record<string, string> },
        ) => {
          addressed.push(name);
          paths.push(typeof input === 'string' ? input : input.url);
          principals.push(init?.headers?.[EXECUTION_PRINCIPAL_HEADER]);
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
  ownerKey: string,
  threadId: string,
): Promise<void> {
  await factory.store().subscribe({
    providerId: 'test',
    externalResourceId: 'res:1',
    threadId,
    resourceId: `${ownerKey}_owner`,
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
  it('rejects authentic invalid UTF-8 before payload policy runs', async () => {
    const extractResourceIds = vi.fn(() => []);
    const router = createWebhookRouter({
      providers: {
        test: testProvider({
          verifyWebhookSignature: () => true,
          extractResourceIds,
        }),
      },
      subscriptions: new InMemorySubscriptionStoreFactory().store(),
      topology: createThreadTopology(stubThreads().namespace),
      secretForProvider: () => 'secret',
    });
    const response = await router(
      new Request('http://host/api/signal-providers/test/webhook', {
        method: 'POST',
        body: new Uint8Array([
          0x7b, 0x22, 0x78, 0x22, 0x3a, 0x22, 0xff, 0x22, 0x7d,
        ]),
      }),
    );

    expect(response?.status).toBe(400);
    expect(await response?.json()).toEqual({
      error: 'a JSON body is required',
    });
    expect(extractResourceIds).not.toHaveBeenCalled();
  });

  it.each([
    { maxBodyBytes: -1 },
    { maxBodyBytes: 1.5 },
    { maxForgeryAuditsPerWindow: Number.NaN },
    { maxForgeryAuditsPerWindow: Number.POSITIVE_INFINITY },
    { forgeryAuditWindowMs: 0 },
    { forgeryAuditWindowMs: Number.MAX_SAFE_INTEGER + 1 },
  ])('rejects invalid numeric configuration synchronously: %o', (numeric) => {
    const factory = new InMemorySubscriptionStoreFactory();
    const threads = stubThreads();
    expect(() =>
      createWebhookRouter({
        providers: { test: testProvider() },
        subscriptions: factory.store(),
        topology: createThreadTopology(threads.namespace),
        secretForProvider: () => 'secret',
        ...numeric,
      }),
    ).toThrow(RangeError);
  });

  it('supports zero body and forgery-audit caps', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    const threads = stubThreads();
    const audit = vi.fn();
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      maxBodyBytes: 0,
      maxForgeryAuditsPerWindow: 0,
      audit,
    });

    expect((await router(webhookRequest('bad', {})))?.status).toBe(413);
    expect(audit).toHaveBeenCalledWith(
      expect.objectContaining({ reason: 'payload-too-large' }),
    );

    const noForgeryAudit = vi.fn();
    const forgeryRouter = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      maxForgeryAuditsPerWindow: 0,
      audit: noForgeryAudit,
    });
    expect((await forgeryRouter(webhookRequest('bad', {})))?.status).toBe(401);
    expect(noForgeryAudit).not.toHaveBeenCalled();
  });

  it('rejects a forged signature BEFORE any lookup or delivery, and audits it', async () => {
    // #given
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const systemStore = factory.store();
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

  it('delivers a valid webhook to the row’s thread — never the payload’s', async () => {
    // #given — the row names acme_t1; the payload lies about the thread
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const threads = stubThreads();
    const events: SignalProviderAuditEvent[] = [];
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      audit: (e) => {
        events.push(e);
      },
    });

    // #when — payload claims another thread; it must be ignored
    const res = await router(
      webhookRequest('good', {
        threadId: 'globex_evil',
      }),
    );

    // #then — delivered to the row's thread with a trusted service principal
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ matched: 1, delivered: 1 });
    expect(threads.addressed).toEqual(['acme_t1']);
    expect(threads.paths[0]).toContain(
      '/signal/notification?resourceId=acme_owner',
    );
    expect(
      threads.principals.map((value) => JSON.parse(value ?? '{}')),
    ).toEqual([
      {
        kind: 'service',
        id: 'signal-provider-delivery',
        purpose: 'signal-provider-delivery',
      },
    ]);
    expect(events).toEqual([
      expect.objectContaining({
        outcome: 'accepted',
        matched: 1,
        delivered: 1,
      }),
    ]);
  });

  it('returns the committed delivery when the accepted audit sink fails', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const threads = stubThreads();
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      audit: async () => {
        throw new Error('audit unavailable');
      },
    });

    const response = await router(webhookRequest('good', {}));

    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual({ matched: 1, delivered: 1 });
    expect(threads.addressed).toEqual(['acme_t1']);
    expect(logged).toHaveBeenCalledWith(
      expect.stringContaining('signal-provider.webhook-audit-error'),
    );
    logged.mockRestore();
  });

  it('is route-absent (null) when the provider or its secret is not configured', async () => {
    // #given — provider registered but no secret
    const factory = new InMemorySubscriptionStoreFactory();
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
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
      subscriptions: factory.store(),
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

  it('skips all matched deliveries when the provider deployment cap is exceeded', async () => {
    // #given — two subscriptions match; the provider cap refuses the ingest
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    await seed(factory, 'globex', 'globex_t1');
    const threads = stubThreads();
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      rateLimit: () => false,
    });
    // #when
    const res = await router(webhookRequest('good', {}));
    // #then — no thread is addressed
    expect(await res?.json()).toEqual({ matched: 2, delivered: 0 });
    expect(threads.addressed).toEqual([]);
  });

  it('bounds the forgery audit so a flood cannot amplify the log', async () => {
    // #given — cap forgery audits at 2 per window
    const factory = new InMemorySubscriptionStoreFactory();
    const events: SignalProviderAuditEvent[] = [];
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
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
        subscriptions: factory.store(),
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

  it('contains a throwing notification builder as a per-delivery failure', async () => {
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
    // #then — acknowledge the authentic webhook without inviting a retry.
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ matched: 1, delivered: 0 });
  });

  it('does not turn an earlier applied delivery into a retryable failure', async () => {
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    await seed(factory, 'globex', 'globex_t1');
    const threads = stubThreads();
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const router = createWebhookRouter({
      providers: {
        test: testProvider({
          buildNotification: (_payload, row) => {
            if (row.threadId === 'globex_t1') throw new Error('provider bug');
            return { source: 'test', kind: 'k', summary: 's' };
          },
        }),
      },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
    });

    try {
      const response = await router(webhookRequest('good', {}));

      expect(response?.status).toBe(200);
      expect(await response?.json()).toEqual({ matched: 2, delivered: 1 });
      expect(threads.addressed).toEqual(['acme_t1']);
      expect(logged).toHaveBeenCalledWith(
        expect.stringContaining('signal-provider.webhook-delivery-error'),
      );
    } finally {
      logged.mockRestore();
    }
  });
});

describe('createWebhookRouter — deployment-wide routing', () => {
  it('delivers to any path-safe thread named by an authoritative row', async () => {
    // #given — a row names another path-safe thread in the deployment
    const factory = new InMemorySubscriptionStoreFactory();
    await factory.store().subscribe({
      providerId: 'test',
      externalResourceId: 'res:1',
      threadId: 'globex_victim',
      resourceId: 'acme_owner',
    });
    const threads = stubThreads();
    const router = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
    });
    // #when — a perfectly valid webhook
    const res = await router(webhookRequest('good', {}));
    // #then — the signed payload maps to the authoritative row and is delivered
    expect(await res?.json()).toEqual({ matched: 1, delivered: 1 });
    expect(threads.addressed).toEqual(['globex_victim']);
  });
});
