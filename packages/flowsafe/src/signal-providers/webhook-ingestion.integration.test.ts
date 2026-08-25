// SPDX-License-Identifier: Apache-2.0
// One webhook through the FULL delivery chain with NO LLM: createWebhookRouter
// (verify → row lookup) → the real topology → the thread object and its
// principal assertion → production signal routes → a real Agent registered
// with a Mastra whose D1 storage composes the notifications domain. The
// notification LANDS in mastra_notifications, visible on the notifications
// read path. The unit suites each mock a seam; this wires the real seams so the
// ingestion boundary has one end-to-end proof of the webhook→inbox landing.
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { describe, expect, it } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  createD1Storage,
  type InitResult,
  init,
  mintThreadId,
  resourceIdFromKey,
  ThreadDurableObject,
  type ThreadScope,
} from '../do-runner/index.js';
import {
  createThreadTopology as createThreadTopologyWithSecret,
  type ThreadNamespaceLike,
  type ThreadTopology,
} from '../host-kit/index.js';
import {
  createSignalStorageDomains,
  createThreadSignalRoutes,
  D1NotificationsStorage,
  type SignalContentPolicy,
  type SignalContentPolicyInput,
} from '../signals/index.js';
import { githubSignalProvider } from './github-provider.js';
import { D1SubscriptionStoreFactory } from './subscription-d1.js';
import { createWebhookRouter } from './webhook-route.js';

const encoder = new TextEncoder();
const SECRET = 'webhook-secret';
const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

function createThreadTopology<Id>(
  namespace: ThreadNamespaceLike<Id>,
): ThreadTopology {
  return createThreadTopologyWithSecret(namespace, DEPLOYMENT_IDENTITY_SECRET);
}

async function githubSign(secret: string, body: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const sig = new Uint8Array(
    await crypto.subtle.sign('HMAC', key, encoder.encode(body)),
  );
  return `sha256=${[...sig].map((b) => b.toString(16).padStart(2, '0')).join('')}`;
}

// Wire the production chain over one in-memory D1, optionally behind the
// structural content policy the thread routes accept.
async function wireWebhookChain(
  contentPolicy?: SignalContentPolicy,
  options: { recordOnly?: boolean } = {},
) {
  // #given — one shared D1 with the composed signal domains
  const d1 = sqliteUnitDatabase(openSqlite()) as never;
  const storage = createD1Storage({
    binding: d1,
    domains: createSignalStorageDomains(d1),
  });
  // A real agent, registered with Mastra so sendNotificationSignal resolves the
  // D1 notifications store (chunk: mastra.getStorage().getStore('notifications')).
  const bareAgent = new Agent({
    id: 'sig-agent',
    name: 'sig-agent',
    instructions: 'webhook delivery target',
    model: 'openai/gpt-4o-mini', // never invoked — no LLM in this proof
    memory: new MockMemory(),
  });
  const mastra = new Mastra({ storage, agents: { 'sig-agent': bareAgent } });
  const agent = mastra.getAgent('sig-agent');
  const notifications = new D1NotificationsStorage(d1);

  // A thread DO hosting the production signal routes over that agent.
  class TestThread extends ThreadDurableObject<unknown> {
    readonly #threadName: string;

    constructor(threadName: string) {
      super(undefined, {});
      this.#threadName = threadName;
    }

    protected override get threadId(): string {
      return this.#threadName;
    }

    #routes = createThreadSignalRoutes({
      resolveAgent: () => agent,
      resolveResourceId: () => resourceIdFromKey('owner'),
      ...(options.recordOnly
        ? {
            canPersist: () => false,
            resolveNotificationsStorage: () => notifications,
          }
        : {}),
      ...(contentPolicy !== undefined ? { contentPolicy } : {}),
    });
    protected build(): InitResult {
      return init(
        { storage },
        { executionFence: 'none', startIdempotency: 'none' },
      );
    }
    protected async route(
      request: Request,
      scope: ThreadScope,
    ): Promise<Response> {
      return (
        (await this.#routes(request, scope)) ??
        new Response(JSON.stringify({ error: 'not found' }), { status: 404 })
      );
    }
  }
  const instances = new Map<string, TestThread>();
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: (name) => name,
    get: (name) => {
      let inst = instances.get(name);
      if (!inst) {
        inst = new TestThread(name);
        instances.set(name, inst);
      }
      const instance = inst;
      return {
        fetch: (input: Request | string, reqInit?: RequestInit) =>
          instance.fetch(
            typeof input === 'string'
              ? new Request(input, reqInit as never)
              : input,
          ),
      };
    },
  };

  const threadId = mintThreadId(() => 'e1');
  const factory = new D1SubscriptionStoreFactory(d1 as never);
  await factory.store().subscribe({
    providerId: 'github',
    externalResourceId: 'github:acme/repo',
    threadId,
    resourceId: resourceIdFromKey('owner'), // matches the thread DO's resolver
  });

  const router = createWebhookRouter({
    providers: { github: githubSignalProvider() },
    subscriptions: factory.store(),
    topology: createThreadTopology(namespace),
    secretForProvider: () => SECRET,
    // Unfenced, matching the thread DO this harness drives into (its init is
    // `executionFence: 'none'` above): the subject here is ingestion through to
    // an owned thread, and the fence's own behavior is pinned in
    // webhook-route.test.ts against a real store.
    executionFence: 'none',
  });

  return { router, threadId, notifications };
}

/** A correctly signed GitHub webhook for the subscribed repo. */
async function signedWebhook(): Promise<Request> {
  const body = JSON.stringify({
    action: 'opened',
    repository: { full_name: 'acme/repo' },
    issue: { number: 5 },
  });
  return new Request('http://host/api/signal-providers/github/webhook', {
    method: 'POST',
    headers: { 'x-hub-signature-256': await githubSign(SECRET, body) },
    body,
  });
}

describe('webhook ingestion — full chain (router → topology → thread DO → inbox)', () => {
  it('lands a signed webhook in mastra_notifications, keyed to the subscribed thread', async () => {
    const { router, threadId, notifications } = await wireWebhookChain();

    // #when
    const res = await router(await signedWebhook());

    // #then — matched + delivered
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ matched: 1, delivered: 1 });

    // ...and the notification is VISIBLE in mastra_notifications for the thread.
    const inbox = await notifications.listNotifications({ threadId });
    expect(inbox).toHaveLength(1);
    expect(inbox[0]).toMatchObject({
      threadId,
      source: 'github',
      summary: expect.stringContaining('github:acme/repo'),
    });
  });

  it('records a non-owner webhook for the host notification-dispatch tick', async () => {
    const { router, threadId, notifications } = await wireWebhookChain(
      undefined,
      { recordOnly: true },
    );

    const res = await router(await signedWebhook());

    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ matched: 1, delivered: 1 });
    const inbox = await notifications.listNotifications({ threadId });
    expect(inbox).toHaveLength(1);
    expect(inbox[0]).toMatchObject({
      threadId,
      source: 'github',
      status: 'pending',
      deliverAt: expect.any(Date),
    });
  });

  // The provider lane converges on the same thread-DO gate as direct ingestion
  // and schedules; a matched, authentic delivery is still content-inspected —
  // under the least-privileged service principal the delivery mints.
  it('refuses a matched delivery the content policy denied, leaving the inbox empty', async () => {
    const inspected: SignalContentPolicyInput[] = [];
    const { router, threadId, notifications } = await wireWebhookChain(
      (input) => {
        inspected.push(input);
        return { allowed: false, outcome: 'denied' };
      },
    );

    // #when
    const res = await router(await signedWebhook());

    // #then — matched, not delivered, and nothing persisted
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({
      matched: 1,
      delivered: 0,
      denied: 1,
    });
    expect(await notifications.listNotifications({ threadId })).toHaveLength(0);
    expect(inspected).toHaveLength(1);
    expect(inspected[0]).toMatchObject({
      threadId,
      entryPath: 'signal.notification',
      principal: { kind: 'service', id: 'signal-provider-delivery' },
    });
    expect(inspected[0]?.text).toContain('github:acme/repo');
  });
});
