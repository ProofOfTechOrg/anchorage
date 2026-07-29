// SPDX-License-Identifier: Apache-2.0
import type { DurableObjectState } from '@cloudflare/workers-types';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import { encodeExecutionPrincipal } from '../approval-api/index.js';

import { type InitResult, init } from './init.js';
import { mintThreadId } from './memory-id.js';
import { ThreadDurableObject, type ThreadScope } from './thread-do.js';
import {
  THREAD_PRINCIPAL_HEADER,
  THREAD_TENANT_HEADER,
} from './thread-header.js';

// A host subclass: build() is its init() wiring, route() its thread routes.
// This one echoes the ASSERTED scope, so every test below reads exactly what
// the base decided before dispatch.
class TestThread extends ThreadDurableObject {
  builds = 0;
  protected build(): InitResult {
    this.builds += 1;
    return init({ storage: new InMemoryStore() });
  }
  protected route(_request: Request, scope: ThreadScope): Promise<Response> {
    return Promise.resolve(
      new Response(
        JSON.stringify({
          threadId: scope.threadId,
          tenantId: scope.tenantId,
          actor: scope.actor,
          requestedBy: scope.requestedBy,
        }),
      ),
    );
  }
}

function threadWith(name: string | undefined): TestThread {
  // The thread DO reads only id.name (its identity) here; storage is absent in
  // node, where the runtime keeps its in-memory resume ledger.
  return new TestThread({ id: { name } } as unknown as DurableObjectState, {});
}

function request(
  tenantId?: string,
  requestedBy: string | null = 'operator',
): Request {
  const headers = new Headers();
  if (tenantId !== undefined) headers.set(THREAD_TENANT_HEADER, tenantId);
  // The topology stamps this on every send; the DO refuses a request without
  // it rather than rebuilding the caller as a human.
  if (requestedBy !== null) {
    headers.set(
      THREAD_PRINCIPAL_HEADER,
      encodeExecutionPrincipal({
        kind: 'human',
        id: requestedBy,
        tenantId: tenantId ?? 'acme',
        role: 'operator',
      }),
    );
  }
  return new Request('http://thread/messages', {
    method: 'POST',
    headers,
  });
}

describe('ThreadDurableObject tenant assertion', () => {
  it('serves a request whose authenticated tenant is the one its name carries', async () => {
    // #given — the DO is addressed idFromName(threadId) with a MINTED id
    const threadId = mintThreadId('acme', () => 't1');
    const thread = threadWith(threadId);

    // #when — the trusted Worker forwards the request stamped with 'acme'
    const response = await thread.fetch(request('acme'));

    // #then — routed, with the identity decoded from the DO's OWN name
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      threadId: 'acme_t1',
      tenantId: 'acme',
      actor: {
        id: 'operator',
        role: 'operator',
        tenantId: 'acme',
      },
      requestedBy: 'operator',
    });
  });

  it('refuses a valid token for ANOTHER tenant’s thread (the C-S4 cross-tenant send)', async () => {
    // #given — tenant 'globex' authenticates fine and aims a request at
    // acme's thread. Name and path agree — only the tenant prefix vs the
    // AUTHENTICATED tenant catches this, which is why the check exists.
    const thread = threadWith(mintThreadId('acme', () => 't1'));

    // #when
    const response = await thread.fetch(request('globex'));

    // #then — fails closed at the prefix assertion, before route() runs
    expect(response.status).toBe(403);
    expect(await response.text()).toMatch(/thread identity mismatch/);
  });

  it('refuses a request that states no authenticated tenant', async () => {
    // #given — no header: the request did not come through the trusted
    // Worker's authentication, so "trust the name" would forfeit the check
    const thread = threadWith(mintThreadId('acme', () => 't1'));

    // #when
    const response = await thread.fetch(request());

    // #then
    expect(response.status).toBe(403);
    expect(await response.text()).toMatch(/authenticates as '<none>'/);
  });

  it('is exact at the tenant boundary (the acme vs acmecorp pin)', async () => {
    // #given — a prefix neighbor: 'acme' must not pass for 'acmecorp's thread
    const thread = threadWith(mintThreadId('acmecorp', () => 't1'));

    // #when / #then
    expect((await thread.fetch(request('acme'))).status).toBe(403);
    expect((await thread.fetch(request('acmecorp'))).status).toBe(200);
  });

  it('refuses a name carrying no INV-3 tenant segment', async () => {
    // #given — a hand-built/client-chosen threadId that never crossed the mint
    const thread = threadWith('legacy-thread-1');

    // #when
    const response = await thread.fetch(request('acme'));

    // #then — unscoped, so it refuses to serve at all rather than guess
    expect(response.status).toBe(403);
    expect(await response.text()).toMatch(/carries no INV-3 tenant segment/);
  });

  it('refuses when the DO has no id.name (not addressed via idFromName)', async () => {
    // #given
    const thread = threadWith(undefined);

    // #when
    const response = await thread.fetch(request('acme'));

    // #then
    expect(response.status).toBe(403);
    expect(await response.text()).toMatch(/no id\.name/);
  });

  it('builds its wiring ONCE per instance — the pubsub identity depends on it', async () => {
    // #given — a second build() would mint a second pubsub identity, and
    // publish/replay would address different feeds (pubsub.ts)
    const thread = threadWith(mintThreadId('acme', () => 't1'));

    // #when — three requests through the same instance
    await thread.fetch(request('acme'));
    await thread.fetch(request('acme'));
    await thread.fetch(request('acme'));

    // #then
    expect(thread.builds).toBe(1);
  });

  it('does not build its wiring for a REFUSED request', async () => {
    // #given — the assertion runs before route(), so a foreign caller never
    // reaches storage
    const thread = threadWith(mintThreadId('acme', () => 't1'));

    // #when
    await thread.fetch(request('globex'));

    // #then
    expect(thread.builds).toBe(0);
  });
});
