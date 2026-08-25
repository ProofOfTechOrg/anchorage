// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  EXECUTION_PRINCIPAL_HEADER,
  type ExecutionFenceDatabase,
  type ExecutionFenceState,
  ExecutionFenceStore,
} from '../do-runner/index.js';
import {
  createThreadTopology as createThreadTopologyWithSecret,
  RunRouteError,
  type ThreadNamespaceLike,
  type ThreadTopology,
} from '../host-kit/index.js';
import type { SignalProviderAdapter } from './provider.js';
import {
  InMemorySubscriptionStoreFactory,
  type SubscriptionStoreFactory,
} from './subscription-d1.js';
import {
  createWebhookRouter as createWebhookRouterImpl,
  type SignalProviderAuditEvent,
  type WebhookRouterOptions,
} from './webhook-route.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

/**
 * The router under test with the fence defaulted to the honest wiring for these
 * cases: the subscription store is in-memory, so there is no database to fence.
 * The fence cases at the bottom of this file pass a real store.
 */
function createWebhookRouter(
  options: Omit<WebhookRouterOptions, 'executionFence'> &
    Partial<Pick<WebhookRouterOptions, 'executionFence'>>,
) {
  return createWebhookRouterImpl({
    ...options,
    executionFence: options.executionFence ?? 'none',
  });
}

/**
 * The parsed JSON log line of one event type — and THE only one of that type.
 *
 * Reading `terminal` off this entry is what binds the verdict to the event: two
 * independent `stringContaining` asserts, one for the type and one for
 * `"terminal":false`, both pass when a different line happens to carry the flag.
 * Requiring exactly one match is what makes "binds to the event" literally true
 * rather than merely likely — several lanes here emit the same type, so a test
 * that logged two of them would otherwise assert against whichever came first.
 */
function loggedEvent(
  logged: { mock: { calls: readonly unknown[][] } },
  type: string,
): Record<string, unknown> {
  const entries = logged.mock.calls.map(
    ([line]) => JSON.parse(String(line)) as Record<string, unknown>,
  );
  const seen = entries.map((entry) => String(entry.type));
  const matches = entries.filter((entry) => entry.type === type);
  expect(
    matches.length,
    `expected exactly one ${type} log line, saw [${seen.join(', ')}] — with more than one, reading a field off "the" entry asserts against whichever came first`,
  ).toBe(1);
  return matches[0] as Record<string, unknown>;
}

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

// Like stubThreads, but every delivery answers with a chosen status and the
// posted bodies are captured — so a test can drive the thread route's terminal
// (422) and transient (5xx) refusals, or inspect what an accepted delivery
// actually carried.
function stubThreadsWith(status: number): {
  namespace: ThreadNamespaceLike<string>;
  addressed: string[];
  bodies: Array<Record<string, unknown>>;
} {
  const addressed: string[] = [];
  const bodies: Array<Record<string, unknown>> = [];
  return {
    addressed,
    bodies,
    namespace: {
      idFromName: (name) => name,
      get: (name) => ({
        fetch: (input: Request | string, init?: { body?: string }) => {
          addressed.push(name);
          void input;
          bodies.push(
            JSON.parse(init?.body ?? '{}') as Record<string, unknown>,
          );
          return Promise.resolve(
            new Response(
              JSON.stringify(
                status >= 200 && status < 300
                  ? { record: {} }
                  : { error: 'refused' },
              ),
              { status },
            ),
          );
        },
      }),
    },
  };
}

// Like stubThreadsWith, but the delivery THROWS instead of answering — the
// lane that has only a thrown value to classify, never a status.
function stubThreadsThatThrow(error: unknown): {
  namespace: ThreadNamespaceLike<string>;
  addressed: string[];
} {
  const addressed: string[] = [];
  return {
    addressed,
    namespace: {
      idFromName: (name) => name,
      get: (name) => ({
        fetch: (input: Request | string) => {
          addressed.push(name);
          void input;
          return Promise.reject(error);
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
    // #then — acknowledge the authentic webhook without inviting a retry: a
    // provider bug is deterministic, so redelivering it only repeats it.
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({
      matched: 1,
      delivered: 0,
      failed: 1,
    });
  });

  // The thread route answers a refusal two different ways, and only one of
  // them may make the whole webhook retryable.
  it('acknowledges a content denial without inviting a redelivery', async () => {
    // #given — the thread DO refuses this content terminally (422)
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const threads = stubThreadsWith(422);
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const run = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
    });

    try {
      // #when
      const res = await run(webhookRequest('good', {}));

      // #then — 2xx: redelivering the identical bytes would be denied again
      expect(res?.status).toBe(200);
      expect(await res?.json()).toEqual({
        matched: 1,
        delivered: 0,
        denied: 1,
      });
      expect(
        loggedEvent(logged, 'signal-provider.webhook-delivery-rejected')
          .terminal,
      ).toBe(true);
    } finally {
      logged.mockRestore();
    }
  });

  it('asks the sender to redeliver when the deployment could not decide', async () => {
    // #given — the thread DO is unavailable (503), e.g. a policy evaluator down
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const threads = stubThreadsWith(503);
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const run = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
    });

    try {
      // #when
      const res = await run(webhookRequest('good', {}));

      // #then — a 5xx, so the provider's own at-least-once retry recovers the
      // event instead of it being silently dropped
      expect(res?.status).toBe(503);
      expect(await res?.json()).toEqual({
        matched: 1,
        delivered: 0,
        deferred: 1,
      });
      expect(
        loggedEvent(logged, 'signal-provider.webhook-delivery-rejected')
          .terminal,
      ).toBe(false);
    } finally {
      logged.mockRestore();
    }
  });

  it('gives each matched row its own dedupe key and keeps a provider-supplied one', async () => {
    // #given — one event matching two rows, and a provider that names its own
    // key for the second
    const factory = new InMemorySubscriptionStoreFactory();
    await factory.store().subscribe({
      providerId: 'test',
      externalResourceId: 'res:1',
      threadId: 'acme_t1',
      resourceId: 'acme_owner',
    });
    await factory.store().subscribe({
      providerId: 'test',
      externalResourceId: 'res:2',
      threadId: 'acme_t1',
      resourceId: 'acme_owner',
    });
    await factory.store().subscribe({
      providerId: 'test',
      externalResourceId: 'res:3',
      threadId: 'acme_t1',
      resourceId: 'acme_owner',
    });
    const threads = stubThreadsWith(200);
    const run = createWebhookRouter({
      providers: {
        test: testProvider({
          extractResourceIds: () => ['res:1', 'res:2', 'res:3'],
          buildNotification: (_payload, row) => ({
            source: 'test',
            kind: 'k',
            summary: row.externalResourceId,
            ...(row.externalResourceId === 'res:2'
              ? { dedupeKey: 'provider-owned' }
              : {}),
          }),
        }),
      },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
    });

    // #when — the same event delivered twice, as an at-least-once sender would
    await run(webhookRequest('good', {}));
    await run(webhookRequest('good', {}));

    // #then — rows 1 and 3 both take a DERIVED key, and they differ: a key
    // built from the event alone would collapse these two into one
    // notification, because coalescing matches on thread + resource.
    const keys = threads.bodies.map((body) => body.dedupeKey);
    expect(keys[0]).not.toBe(keys[2]);
    expect(new Set(keys.slice(0, 3)).size).toBe(3);
    // ...the provider's own key is never overwritten...
    expect(keys[1]).toBe('provider-owned');
    // ...and a redelivery of the same event reuses the same keys, so it
    // coalesces into the still-pending row instead of duplicating it.
    expect(keys.slice(3)).toEqual(keys.slice(0, 3));
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
      expect(await response?.json()).toEqual({
        matched: 2,
        delivered: 1,
        failed: 1,
      });
      expect(threads.addressed).toEqual(['acme_t1']);
      // A code defect is permanent: nothing about redelivering these bytes
      // could make the provider build them successfully.
      expect(
        loggedEvent(logged, 'signal-provider.webhook-delivery-error').terminal,
      ).toBe(true);
    } finally {
      logged.mockRestore();
    }
  });

  // The same two directions again, but for a delivery that THROWS rather than
  // answering — the lane whose log line carried no verdict at all.
  it('marks an undecided delivery throw as non-terminal', async () => {
    // #given — the delivery itself fails, e.g. the thread DO is unreachable
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const threads = stubThreadsThatThrow(new Error('socket hang up'));
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const run = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
    });

    try {
      // #when
      const res = await run(webhookRequest('good', {}));

      // #then — a 5xx, and the log says the event may yet land
      expect(res?.status).toBe(503);
      expect(await res?.json()).toEqual({
        matched: 1,
        delivered: 0,
        deferred: 1,
      });
      expect(
        loggedEvent(logged, 'signal-provider.webhook-delivery-error').terminal,
      ).toBe(false);
    } finally {
      logged.mockRestore();
    }
  });

  it('marks a routed content refusal thrown at delivery as terminal', async () => {
    // #given — the topology throws the refusal instead of returning it
    const factory = new InMemorySubscriptionStoreFactory();
    await seed(factory, 'acme', 'acme_t1');
    const threads = stubThreadsThatThrow(new RunRouteError(422, 'denied'));
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const run = createWebhookRouter({
      providers: { test: testProvider() },
      subscriptions: factory.store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
    });

    try {
      // #when
      const res = await run(webhookRequest('good', {}));

      // #then — 2xx, and the log distinguishes this throw from the one above
      expect(res?.status).toBe(200);
      expect(await res?.json()).toEqual({
        matched: 1,
        delivered: 0,
        denied: 1,
      });
      expect(
        loggedEvent(logged, 'signal-provider.webhook-delivery-error').terminal,
      ).toBe(true);
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

describe('createWebhookRouter and the deployment execution fence', () => {
  async function fenceAt(
    state: ExecutionFenceState,
  ): Promise<ExecutionFenceStore> {
    const fence = new ExecutionFenceStore(
      sqliteUnitDatabase(openSqlite()) as ExecutionFenceDatabase,
    );
    await fence.seed(state);
    return fence;
  }

  function unreadableFence(): ExecutionFenceStore {
    // Storage that faults on every query — NOT the "no such table" a pre-0.20
    // database answers with, which legitimately reads as open.
    return new ExecutionFenceStore({
      prepare: () => ({
        bind: () => ({
          bind: () => {
            throw new Error('unreachable');
          },
          run: () => Promise.reject(new Error('D1_ERROR: network')),
          all: () => Promise.reject(new Error('D1_ERROR: network')),
        }),
        run: () => Promise.reject(new Error('D1_ERROR: network')),
        all: () => Promise.reject(new Error('D1_ERROR: network')),
      }),
    } as unknown as ExecutionFenceDatabase);
  }

  it('degrades closed with 503 when the fence cannot be read', async () => {
    // #given
    const threads = stubThreads();
    const router = createWebhookRouter({
      providers: {
        test: testProvider({
          verifyWebhookSignature: () => true,
          extractResourceIds: () => ['ext-1'],
        }),
      },
      subscriptions: new InMemorySubscriptionStoreFactory().store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      executionFence: unreadableFence(),
    });
    const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);

    // #then — 503 so the provider's own at-least-once redelivery recovers the
    // event, exactly as for a refusal.
    try {
      const response = await router(webhookRequest('good', { id: 'evt-1' }));
      expect(response?.status).toBe(503);
      expect(await response?.json()).toMatchObject({
        reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
      });
    } finally {
      log.mockRestore();
    }
    expect(threads.addressed).toEqual([]);
  });

  it('refuses an authentic delivery with 503 once locked, so the provider redelivers', async () => {
    // #given — a locked deployment and a webhook whose signature is genuine.
    const threads = stubThreads();
    const extractResourceIds = vi.fn(() => ['ext-1']);
    const audit = vi.fn();
    const router = createWebhookRouter({
      providers: {
        test: testProvider({
          verifyWebhookSignature: () => true,
          extractResourceIds,
        }),
      },
      subscriptions: new InMemorySubscriptionStoreFactory().store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      audit,
      executionFence: await fenceAt('migration-locked'),
    });

    // #when
    const response = await router(webhookRequest('good', { id: 'evt-1' }));

    // #then — 503 is the status every provider retries on, so the event
    // survives the migration in the PROVIDER's queue rather than half-landing
    // in a database that is being copied.
    expect(response?.status).toBe(503);
    expect(await response?.json()).toEqual({
      error: expect.stringContaining("fenced ('migration-locked')"),
      reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
    });
    // #and — the fence runs AFTER the signature check, so it is no oracle for
    // an unauthenticated caller and nothing was parsed or delivered.
    expect(extractResourceIds).not.toHaveBeenCalled();
    expect(threads.addressed).toEqual([]);
    expect(audit).toHaveBeenCalledWith(
      expect.objectContaining({
        outcome: 'rejected',
        reason: 'execution-fenced',
      }),
    );
  });

  it('still rejects a FORGED signature with 401 while locked', async () => {
    // #given — the verify stays first, so a forgery never learns the fence
    // state and never spends the delivery path.
    const router = createWebhookRouter({
      providers: {
        test: testProvider({ verifyWebhookSignature: () => false }),
      },
      subscriptions: new InMemorySubscriptionStoreFactory().store(),
      topology: createThreadTopology(stubThreads().namespace),
      secretForProvider: () => 'secret',
      executionFence: await fenceAt('migration-locked'),
    });

    // #then
    const response = await router(webhookRequest('bad', { id: 'evt-1' }));
    expect(response?.status).toBe(401);
  });

  it('keeps delivering while draining', async () => {
    // #given — draining still delivers: the thread routes degrade a wake to a
    // persist there, so the inbox drains without minting.
    const threads = stubThreads();
    const router = createWebhookRouter({
      providers: {
        test: testProvider({
          verifyWebhookSignature: () => true,
          extractResourceIds: () => [],
        }),
      },
      subscriptions: new InMemorySubscriptionStoreFactory().store(),
      topology: createThreadTopology(threads.namespace),
      secretForProvider: () => 'secret',
      executionFence: await fenceAt('draining'),
    });

    // #then
    const response = await router(webhookRequest('good', { id: 'evt-1' }));
    expect(response?.status).toBe(200);
  });
});
