// SPDX-License-Identifier: Apache-2.0
// Track E (M-007) E-S1 (SHOULD-FIX): one webhook through the FULL delivery chain
// with NO LLM — createWebhookRouter (verify → row lookup) → real
// createThreadTopology → real ThreadDurableObject (its tenant assertion) → the
// production Track C thread signal routes → a real Agent registered with a Mastra
// whose D1 storage composes the notifications domain — and the notification LANDS
// in mastra_notifications, visible on the notifications read path. The unit suites
// each mock a seam; this wires the real seams so the ingestion boundary has one
// end-to-end proof of the webhook→inbox landing.
import type { DurableObjectState } from '@cloudflare/workers-types';
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { describe, expect, it } from 'vitest';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import {
  createD1Storage,
  type InitResult,
  init,
  mintResourceId,
  mintThreadId,
  ThreadDurableObject,
  type ThreadScope,
} from '../do-runner/index.js';
import {
  createThreadTopology,
  type ThreadNamespaceLike,
} from '../host-kit/index.js';
import {
  createSignalStorageDomains,
  createThreadSignalRoutes,
  D1NotificationsStorage,
} from '../signals/index.js';
import { githubSignalProvider } from './github-provider.js';
import { D1SubscriptionStoreFactory } from './subscription-d1.js';
import { createWebhookRouter } from './webhook-route.js';

const encoder = new TextEncoder();
const SECRET = 'webhook-secret';

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

describe('webhook ingestion — full chain (router → topology → thread DO → inbox)', () => {
  it('lands a signed webhook in mastra_notifications, keyed to the subscribed thread', async () => {
    // #given — one shared D1 with the composed signal domains
    const d1 = d1DatabaseLike(openSqlite()) as never;
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
    });
    const mastra = new Mastra({ storage, agents: { 'sig-agent': bareAgent } });
    const agent = mastra.getAgent('sig-agent');

    // A thread DO hosting the production Track C signal routes over that agent.
    class TestThread extends ThreadDurableObject<unknown> {
      #routes = createThreadSignalRoutes({
        resolveAgent: () => agent,
        resolveResourceId: (scope: ThreadScope) =>
          mintResourceId(scope.tenantId, 'owner'),
      });
      protected build(): InitResult {
        return init({ storage });
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
          inst = new TestThread(
            { id: { name } } as unknown as DurableObjectState,
            {},
          );
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

    const threadId = mintThreadId('acme', () => 'e1'); // 'acme_e1'
    const factory = new D1SubscriptionStoreFactory(d1 as never);
    await factory.forTenant('acme').subscribe({
      providerId: 'github',
      externalResourceId: 'github:acme/repo',
      threadId,
      resourceId: mintResourceId('acme', 'owner'), // MATCHES the thread DO's resolveResourceId
    });

    const router = createWebhookRouter({
      providers: { github: githubSignalProvider() },
      subscriptions: factory.system(),
      topology: createThreadTopology(namespace),
      secretForProvider: () => SECRET,
    });

    // #when — a correctly signed GitHub webhook for the subscribed repo
    const body = JSON.stringify({
      action: 'opened',
      repository: { full_name: 'acme/repo' },
      issue: { number: 5 },
    });
    const res = await router(
      new Request('http://host/api/signal-providers/github/webhook', {
        method: 'POST',
        headers: { 'x-hub-signature-256': await githubSign(SECRET, body) },
        body,
      }),
    );

    // #then — matched + delivered
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ matched: 1, delivered: 1 });

    // ...and the notification is VISIBLE in mastra_notifications for the thread.
    const notifications = new D1NotificationsStorage(d1 as never);
    const inbox = await notifications.listNotifications({ threadId });
    expect(inbox).toHaveLength(1);
    expect(inbox[0]).toMatchObject({
      threadId,
      source: 'github',
      summary: expect.stringContaining('github:acme/repo'),
    });
  });
});
