// SPDX-License-Identifier: Apache-2.0
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import type { TenantContext } from '../approval-api/index.js';
import {
  type InitResult,
  init,
  mintThreadId,
  THREAD_TENANT_HEADER,
  ThreadDurableObject,
  type ThreadScope,
  tenantOwnsMemoryId,
} from '../do-runner/index.js';
import { RunRouteError } from './run-route-error.js';
import {
  createThreadTopology,
  type ThreadNamespaceLike,
  type ThreadStubLike,
} from './thread-topology.js';

function tenantContext(tenantId: string): TenantContext {
  return {
    actor: { id: 'operator-1', role: 'operator', tenantId },
    tenantId,
    // The production predicate, not a hand-copy — the exactness pins below ride
    // the real one (see memory-boundary.test.ts).
    ownsMemoryId: (id: string) => tenantOwnsMemoryId(tenantId, id),
  } as unknown as TenantContext;
}

interface Captured {
  name: string;
  url: string;
  method: string;
  tenantHeader: string | null;
  otherHeader: string | null;
  body: string;
}

/** A namespace that records what reached the stub, without a DO behind it. */
function recordingNamespace(): {
  namespace: ThreadNamespaceLike<string>;
  calls: Captured[];
} {
  const calls: Captured[] = [];
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: (name) => name,
    get: (name): ThreadStubLike =>
      ({
        fetch: async (
          input: Request | string,
          init?: { headers?: Record<string, string> },
        ) => {
          const request =
            typeof input === 'string'
              ? new Request(input, init as RequestInit)
              : input;
          // Capture the whole forwarded envelope — method, body, and a
          // non-tenant header — so send's `...init` pass-through is PINNED:
          // drop it and these assertions fail, where before a real caller
          // silently lost its method, body, and content-type.
          calls.push({
            name,
            url: request.url,
            method: request.method,
            tenantHeader: request.headers.get(THREAD_TENANT_HEADER),
            otherHeader: request.headers.get('x-other'),
            body: await request.text(),
          });
          return new Response('{}');
        },
      }) as ThreadStubLike,
  };
  return { namespace, calls };
}

async function statusOf(
  run: () => Promise<unknown>,
): Promise<number | undefined> {
  try {
    await run();
  } catch (error) {
    return error instanceof RunRouteError ? error.status : undefined;
  }
  return undefined;
}

describe('createThreadTopology', () => {
  it('stamps the AUTHENTICATED tenant and addresses idFromName(threadId)', async () => {
    // #given
    const { namespace, calls } = recordingNamespace();
    const topology = createThreadTopology(namespace);
    const threadId = mintThreadId('acme', () => 't1');

    // #when
    await topology.send(tenantContext('acme'), threadId, '/messages', {
      method: 'POST',
      body: '{}',
    });

    // #then — the DO's name IS the thread, the header carries the tenant the
    // resolver authenticated, and the caller's method/body reach the stub intact
    expect(calls).toEqual([
      {
        name: 'acme_t1',
        url: 'http://thread/messages',
        method: 'POST',
        tenantHeader: 'acme',
        otherHeader: null,
        body: '{}',
      },
    ]);
  });

  it('OVERWRITES a caller-supplied tenant header rather than letting it win', async () => {
    // #given — the value must come from the tenant context, never the call site
    const { namespace, calls } = recordingNamespace();
    const topology = createThreadTopology(namespace);

    // #when — a route tries to pass its own spelling of the header
    await topology.send(
      tenantContext('acme'),
      mintThreadId('acme', () => 't1'),
      '/messages',
      { headers: { [THREAD_TENANT_HEADER]: 'globex', 'x-other': 'kept' } },
    );

    // #then — the tenant is overwritten, but the route's OWN header rides
    // through untouched (send's ...init pass-through)
    expect(calls[0]?.tenantHeader).toBe('acme');
    expect(calls[0]?.otherHeader).toBe('kept');
  });

  it('beats a caller-supplied header at ANY casing, not just the canonical spelling', async () => {
    // #given — HTTP header names are case-insensitive but a plain-object spread
    // is NOT: the pre-fix code kept `X-Flowsafe-Tenant` as a second property and
    // Headers then appended both into "globex, acme", which the DO reads as a
    // mismatch — a 403 that looks like an identity attack. The stamp (Headers.set)
    // must win at every spelling; this fails on the object-spread implementation.
    const { namespace, calls } = recordingNamespace();
    const topology = createThreadTopology(namespace);

    // #when — a DIFFERENT casing than the lowercase constant
    await topology.send(
      tenantContext('acme'),
      mintThreadId('acme', () => 't1'),
      '/messages',
      { headers: { 'X-Flowsafe-Tenant': 'globex' } },
    );

    // #then — exactly the authenticated tenant, no appended forgery
    expect(calls[0]?.tenantHeader).toBe('acme');
  });

  it('OVERWRITES a FORGED client header on a forwarded request (the trap the house idiom sets)', async () => {
    // #given — this is the whole reason this module exists. hub-topology's
    // `forwardSubscribe: (tenantId, request) => stub(tenantId).fetch(request)`
    // forwards the CLIENT's Request verbatim; a thread route copying that shape
    // would hand the client the header the thread DO authenticates on.
    const { namespace, calls } = recordingNamespace();
    const topology = createThreadTopology(namespace);
    const forged = new Request('http://host/api/threads/subscribe', {
      headers: {
        [THREAD_TENANT_HEADER]: 'globex',
        upgrade: 'websocket',
      },
    });

    // #when — tenant 'acme' forwards it
    await topology.forward(
      tenantContext('acme'),
      mintThreadId('acme', () => 't1'),
      forged,
    );

    // #then — the forgery is gone, replaced (not appended) by the real tenant
    expect(calls[0]?.tenantHeader).toBe('acme');
  });

  it('preserves the upgrade handshake while overwriting the tenant', async () => {
    // #given — the overwrite must not cost the forward its reason to exist
    const calls: Request[] = [];
    const namespace: ThreadNamespaceLike<string> = {
      idFromName: (name) => name,
      get: (): ThreadStubLike =>
        ({
          fetch: (input: Request | string) => {
            if (typeof input !== 'string') calls.push(input);
            return Promise.resolve(new Response('{}'));
          },
        }) as ThreadStubLike,
    };
    const request = new Request('http://host/subscribe', {
      headers: { upgrade: 'websocket', [THREAD_TENANT_HEADER]: 'globex' },
    });

    // #when
    await createThreadTopology(namespace).forward(
      tenantContext('acme'),
      mintThreadId('acme', () => 't1'),
      request,
    );

    // #then
    expect(calls[0]?.headers.get('upgrade')).toBe('websocket');
    expect(calls[0]?.headers.get(THREAD_TENANT_HEADER)).toBe('acme');
  });

  it('404s a foreign threadId BEFORE the DO is addressed — no oracle, no wake', async () => {
    // #given — tenant B aims at tenant A's thread with a valid token (C-S4)
    const { namespace, calls } = recordingNamespace();
    const topology = createThreadTopology(namespace);
    const foreign = mintThreadId('acme', () => 't1');

    // #when / #then — both surfaces refuse, and neither touched the namespace
    await expect(
      topology.send(tenantContext('globex'), foreign, '/messages'),
    ).rejects.toThrow(/threadId not found/);
    await expect(
      topology.forward(
        tenantContext('globex'),
        foreign,
        new Request('http://host/subscribe'),
      ),
    ).rejects.toThrow(/threadId not found/);
    expect(calls).toEqual([]);
  });

  it('refuses with 404, never 403 (the run-router rule)', async () => {
    // #given
    const { namespace } = recordingNamespace();
    const topology = createThreadTopology(namespace);

    // #when / #then — a 403 would confirm another tenant's thread EXISTS
    expect(
      await statusOf(() =>
        topology.send(
          tenantContext('globex'),
          mintThreadId('acme', () => 't1'),
          '/messages',
        ),
      ),
    ).toBe(404);
  });

  it('REJECTS rather than throwing synchronously out of its Promise-typed call', async () => {
    // #given — a Promise-returning function that throws at call time is not
    // catchable by the `.catch()` its signature invites, so the 404 would sail
    // past the handler meant to map it. Pin the rejection contract.
    const { namespace } = recordingNamespace();
    const topology = createThreadTopology(namespace);
    const foreign = mintThreadId('acme', () => 't1');

    // #when / #then — a bare call must not throw; the returned promise rejects
    let rejected: unknown;
    const promise = topology.send(
      tenantContext('globex'),
      foreign,
      '/messages',
    );
    await promise.catch((error: unknown) => {
      rejected = error;
    });
    expect(rejected).toBeInstanceOf(RunRouteError);
  });

  it('is exact at the tenant boundary (the acme vs acmecorp pin)', async () => {
    // #given
    const { namespace, calls } = recordingNamespace();
    const topology = createThreadTopology(namespace);

    // #when / #then
    await expect(
      topology.send(
        tenantContext('acme'),
        mintThreadId('acmecorp', () => 't1'),
        '/messages',
      ),
    ).rejects.toThrow(/threadId not found/);
    expect(calls).toEqual([]);
  });
});

describe('createThreadTopology <-> ThreadDurableObject (mint meets verify)', () => {
  // The point of shipping the pair together: what the minter stamps is what the
  // verifier accepts. A drift on either side — a renamed header, a value read
  // from the wrong place — must fail HERE rather than in production, where the
  // symptom is either a dead route or an open door.
  class EchoThread extends ThreadDurableObject {
    protected build(): InitResult {
      return init({ storage: new InMemoryStore() });
    }
    protected route(_request: Request, scope: ThreadScope): Promise<Response> {
      return Promise.resolve(new Response(scope.tenantId));
    }
  }

  /** A namespace whose stubs are REAL thread DOs named by idFromName. */
  function threadNamespace(): ThreadNamespaceLike<string> {
    return {
      idFromName: (name) => name,
      get: (name): ThreadStubLike => {
        const thread = new EchoThread(
          { id: { name } } as never,
          {},
        ) as unknown as { fetch(request: Request): Promise<Response> };
        return {
          fetch: (input: Request | string, init?: RequestInit) =>
            thread.fetch(
              typeof input === 'string' ? new Request(input, init) : input,
            ),
        } as ThreadStubLike;
      },
    };
  }

  it("a forged client header does not survive the minter, so the DO's assertion holds", async () => {
    // #given — the end-to-end attack: tenant 'acme' is authenticated, but the
    // client's own request claims to be 'globex' (or vice versa). Whatever the
    // client wrote, the DO must see the authenticated tenant.
    const topology = createThreadTopology(threadNamespace());
    const forged = new Request('http://host/x', {
      headers: { [THREAD_TENANT_HEADER]: 'globex' },
    });

    // #when
    const response = await topology.forward(
      tenantContext('acme'),
      mintThreadId('acme', () => 't1'),
      forged,
    );

    // #then — routed (the assertion passed) as the AUTHENTICATED tenant
    expect(response.status).toBe(200);
    expect(await response.text()).toBe('acme');
  });

  it('what the minter stamps is what the verifier accepts (no header drift)', async () => {
    // #given / #when
    const response = await createThreadTopology(threadNamespace()).send(
      tenantContext('acme'),
      mintThreadId('acme', () => 't1'),
      '/x',
    );

    // #then — a renamed header on either side would 403 here
    expect(response.status).toBe(200);
    expect(await response.text()).toBe('acme');
  });

  it('the DO still refuses a request that did not come through the minter', async () => {
    // #given — the defense in depth: the topology's ownership 404 is not the
    // only barrier, so a route that addresses the namespace directly (the hole
    // this module exists to close) still fails closed at the DO.
    const namespace = threadNamespace();

    // #when — a raw forward with the client's forged header, no minter
    const response = await namespace
      .get(namespace.idFromName(mintThreadId('acme', () => 't1')))
      .fetch(
        new Request('http://thread/x', {
          headers: { [THREAD_TENANT_HEADER]: 'globex' },
        }),
      );

    // #then — the DO's own assertion catches it
    expect(response.status).toBe(403);
  });
});
