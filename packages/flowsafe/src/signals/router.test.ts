// SPDX-License-Identifier: Apache-2.0
// The P6 ingestion trust boundary (createSignalRouter): the gate ORDER (401 →
// role → ownership → memory-id → allowlist/size/rate → audit → forward), each
// fail-closed, over mock resolve + topology seams.

import { describe, expect, it, vi } from 'vitest';

import type { ApprovalActor, TenantContext } from '../approval-api/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import {
  createInMemorySignalRateLimiter,
  createSignalRouter,
  type SignalIngestAuditEvent,
} from './router.js';

const OWNED_THREAD = 'acme_t1';

function tenantCtx(
  role: ApprovalActor['role'],
  ownedThread = OWNED_THREAD,
): TenantContext {
  const actor: ApprovalActor = { id: 'opal', role, tenantId: 'acme' };
  return {
    actor,
    tenantId: 'acme',
    ownsMemoryId: (id: string) => id === ownedThread,
  } as unknown as TenantContext;
}

function recordingTopology(): {
  topology: ThreadTopology;
  calls: Array<{ threadId: string; path: string; body?: string }>;
} {
  const calls: Array<{ threadId: string; path: string; body?: string }> = [];
  const topology = {
    send: async (
      _tenant: TenantContext,
      threadId: string,
      path: string,
      init?: { body?: string },
    ) => {
      calls.push({ threadId, path, body: init?.body });
      return new Response(
        JSON.stringify({ decision: { action: 'deliver', runId: 'r' } }),
        { status: 200, headers: { 'content-type': 'application/json' } },
      );
    },
    forward: async () => new Response(null),
  } as unknown as ThreadTopology;
  return { topology, calls };
}

function post(path: string, body: unknown): Request {
  return new Request(`http://host${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: typeof body === 'string' ? body : JSON.stringify(body),
  });
}

describe('createSignalRouter — the P6 ingestion gate', () => {
  it('returns null for a non-signal path (composes ahead of others)', async () => {
    const { topology } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
    });
    expect(await router(new Request('http://host/workflows'))).toBeNull();
  });

  it('is route-absent on a malformed percent-encoded threadId (no pre-auth URIError)', async () => {
    // A lone '%' in the threadId segment — bare decodeURIComponent would THROW
    // out of the handler BEFORE auth; safeDecodeSegment makes it route-absent.
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
    });
    const res = await router(
      new Request('http://host/api/threads/%/message', {
        method: 'POST',
        body: '{}',
      }),
    );
    expect(res).toBeNull();
    expect(calls).toHaveLength(0); // never resolved, never addressed
  });

  it('401 when unauthenticated', async () => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => undefined,
      topology,
    });
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'hi' }),
    );
    expect(res?.status).toBe(401);
    expect(calls).toHaveLength(0);
  });

  it('403 for a read-only role (viewer/reviewer may not signal)', async () => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('viewer'),
      topology,
    });
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'hi' }),
    );
    expect(res?.status).toBe(403);
    expect(calls).toHaveLength(0);
  });

  it('404 for a foreign threadId (no existence oracle) — before the DO', async () => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
    });
    const res = await router(
      post('/api/threads/other_t9/message', { contents: 'hi' }),
    );
    expect(res?.status).toBe(404);
    expect(calls).toHaveLength(0); // never addressed
  });

  it('400 when the body names a memory id (assertNoClientMemoryIds)', async () => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
    });
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, {
        contents: 'hi',
        resourceId: 'acme_hax',
      }),
    );
    expect(res?.status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  it('413 for an oversized payload', async () => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
      maxContentBytes: 64,
    });
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, {
        contents: 'x'.repeat(200),
      }),
    );
    expect(res?.status).toBe(413);
    expect(calls).toHaveLength(0);
  });

  it('400 for a non-allowlisted attribute key', async () => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
      attributeAllowlist: ['severity'],
    });
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/signal`, {
        contents: 'hi',
        attributes: { severity: 'high', evil: 'x' },
      }),
    );
    expect(res?.status).toBe(400);
    expect(calls).toHaveLength(0);
  });

  it('429 when the per-tenant rate cap is exceeded, and audits the rejection', async () => {
    const { topology } = recordingTopology();
    const events: SignalIngestAuditEvent[] = [];
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
      rateLimit: createInMemorySignalRateLimiter({
        limit: 1,
        windowMs: 60_000,
      }),
      audit: (e) => {
        events.push(e);
      },
    });
    const first = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'a' }),
    );
    expect(first?.status).toBe(200);
    const second = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'b' }),
    );
    expect(second?.status).toBe(429);
    expect(events.some((e) => e.outcome === 'accepted')).toBe(true);
    expect(
      events.some(
        (e) => e.outcome === 'rejected' && e.reason === 'rate-limited',
      ),
    ).toBe(true);
  });

  it('audits the role-403 rejection (an authenticated actor lacking the role)', async () => {
    // #given — a viewer (read-only) with an audit sink wired
    const { topology, calls } = recordingTopology();
    const events: SignalIngestAuditEvent[] = [];
    const router = createSignalRouter({
      resolve: async () => tenantCtx('viewer'),
      topology,
      audit: (e) => {
        events.push(e);
      },
    });

    // #when
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'hi' }),
    );

    // #then — refused before the DO, and the rejection is audited (POST-auth)
    expect(res?.status).toBe(403);
    expect(calls).toHaveLength(0);
    expect(events).toEqual([
      expect.objectContaining({
        type: 'signal.ingest',
        outcome: 'rejected',
        reason: 'forbidden-role',
        tenantId: 'acme',
        actorId: 'opal',
        threadId: OWNED_THREAD,
      }),
    ]);
  });

  it('audits the cross-tenant 404 (a probe for another tenant’s threadId)', async () => {
    // #given — an operator probing a threadId it does not own
    const { topology, calls } = recordingTopology();
    const events: SignalIngestAuditEvent[] = [];
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
      audit: (e) => {
        events.push(e);
      },
    });

    // #when
    const res = await router(
      post('/api/threads/other_t9/message', { contents: 'hi' }),
    );

    // #then — 404 (no oracle), never addressed, and the probe is audited
    expect(res?.status).toBe(404);
    expect(calls).toHaveLength(0);
    expect(events).toEqual([
      expect.objectContaining({
        type: 'signal.ingest',
        outcome: 'rejected',
        reason: 'foreign-thread',
        tenantId: 'acme',
        threadId: 'other_t9',
      }),
    ]);
  });

  it('audits the memory-id-smuggle 400 (a body naming resourceId/threadId)', async () => {
    // #given — an operator smuggling a TCB-only memory id in the body
    const { topology, calls } = recordingTopology();
    const events: SignalIngestAuditEvent[] = [];
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
      audit: (e) => {
        events.push(e);
      },
    });

    // #when
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, {
        contents: 'hi',
        resourceId: 'acme_hax',
      }),
    );

    // #then — 400, never addressed, and the smuggle is audited
    expect(res?.status).toBe(400);
    expect(calls).toHaveLength(0);
    expect(events).toEqual([
      expect.objectContaining({
        type: 'signal.ingest',
        outcome: 'rejected',
        reason: 'client-memory-id',
        tenantId: 'acme',
        threadId: OWNED_THREAD,
      }),
    ]);
  });

  it('does NOT audit a pre-auth 401 (unauthenticated — no flood amplification)', async () => {
    // #given — resolve returns no tenant (unauthenticated) with an audit sink
    const { topology } = recordingTopology();
    const events: SignalIngestAuditEvent[] = [];
    const router = createSignalRouter({
      resolve: async () => undefined,
      topology,
      audit: (e) => {
        events.push(e);
      },
    });

    // #when
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'hi' }),
    );

    // #then — 401 and NO audit event (auditing pre-auth would let a flood write it)
    expect(res?.status).toBe(401);
    expect(events).toHaveLength(0);
  });

  it('forwards an accepted ingest through the topology and audits it', async () => {
    const { topology, calls } = recordingTopology();
    const audit = vi.fn();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
      audit,
    });
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/notification`, {
        source: 'github',
        kind: 'pr',
        summary: 'opened',
      }),
    );
    expect(res?.status).toBe(200);
    expect(calls).toEqual([
      {
        threadId: OWNED_THREAD,
        path: '/signal/notification',
        body: expect.any(String),
      },
    ]);
    expect(audit).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'signal.ingest',
        tenantId: 'acme',
        actorId: 'opal',
        threadId: OWNED_THREAD,
        channel: 'notification',
        outcome: 'accepted',
      }),
    );
  });

  it('405 for a non-POST method on a signal path', async () => {
    const { topology } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => tenantCtx('operator'),
      topology,
    });
    const res = await router(
      new Request(`http://host/api/threads/${OWNED_THREAD}/message`, {
        method: 'GET',
      }),
    );
    expect(res?.status).toBe(405);
  });
});
