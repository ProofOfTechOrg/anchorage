// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';

import {
  type ActorContext,
  ActorResolutionError,
  type ApprovalActor,
} from '../approval-api/index.js';
import { RunRouteError, type ThreadTopology } from '../host-kit/index.js';
import {
  createInMemorySignalRateLimiter,
  createSignalRouter,
  type SignalIngestAuditEvent,
  type SignalRouterOptions,
} from './router.js';

const OWNED_THREAD = 'acme_t1';

function actorContext(
  role: ApprovalActor['role'],
  ownedThread = OWNED_THREAD,
): ActorContext {
  const actor: ApprovalActor = { id: 'opal', role };
  return {
    actor,
    principal: { kind: 'human', id: actor.id, role },
    resourceOwner: { kind: 'human', id: actor.id },
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => 'run-1',
    newThreadId: () => ownedThread,
    resourceIdFromKey: (key) => key,
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async (kind, id) =>
      kind === 'thread' && id === ownedThread,
    canSelfDecide: () => false,
  };
}

function recordingTopology(): {
  topology: ThreadTopology;
  calls: Array<{ threadId: string; path: string; body?: string }>;
} {
  const calls: Array<{ threadId: string; path: string; body?: string }> = [];
  const topology = {
    send: async (
      _context: ActorContext,
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

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe('createSignalRouter — the P6 ingestion gate', () => {
  it.each([
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
  ])('rejects an invalid body cap synchronously: %s', (maxContentBytes) => {
    const { topology } = recordingTopology();
    expect(() =>
      createSignalRouter({
        resolve: async () => actorContext('operator'),
        topology,
        maxContentBytes,
      }),
    ).toThrow(RangeError);
  });

  it('accepts a zero body cap and rejects every non-empty body', async () => {
    const { topology } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
      maxContentBytes: 0,
    });
    expect(
      (
        await router(
          post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'hi' }),
        )
      )?.status,
    ).toBe(413);
  });

  it('returns null for a non-signal path (composes ahead of others)', async () => {
    const { topology } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
    });
    expect(await router(new Request('http://host/workflows'))).toBeNull();
  });

  it('is route-absent on a malformed percent-encoded threadId (no pre-auth URIError)', async () => {
    // A lone '%' in the threadId segment — bare decodeURIComponent would THROW
    // out of the handler BEFORE auth; safeDecodeSegment makes it route-absent.
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
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
      resolve: async () => actorContext('viewer'),
      topology,
    });
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'hi' }),
    );
    expect(res?.status).toBe(403);
    expect(calls).toHaveLength(0);
  });

  it('404s a path-safe thread owned by another actor', async () => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
    });
    const res = await router(
      post('/api/threads/other_t9/message', { contents: 'hi' }),
    );
    expect(res?.status).toBe(404);
    expect(calls).toHaveLength(0);
  });

  it('400 when the body names a memory id (assertNoClientMemoryIds)', async () => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
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
      resolve: async () => actorContext('operator'),
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
      resolve: async () => actorContext('operator'),
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

  it('429 when the deployment rate cap is exceeded, and audits the rejection', async () => {
    const { topology } = recordingTopology();
    const events: SignalIngestAuditEvent[] = [];
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
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
      resolve: async () => actorContext('viewer'),
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
        actorId: 'opal',
        threadId: OWNED_THREAD,
      }),
    ]);
  });

  it('audits an accepted owned thread', async () => {
    // #given — an operator addressing its owned thread
    const { topology, calls } = recordingTopology();
    const events: SignalIngestAuditEvent[] = [];
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
      audit: (e) => {
        events.push(e);
      },
    });

    // #when
    const res = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'hi' }),
    );

    // #then — accepted, addressed, and audited
    expect(res?.status).toBe(200);
    expect(calls).toEqual([
      expect.objectContaining({ threadId: OWNED_THREAD }),
    ]);
    expect(events).toEqual([
      expect.objectContaining({
        type: 'signal.ingest',
        outcome: 'accepted',
        threadId: OWNED_THREAD,
      }),
    ]);
  });

  it('audits the memory-id-smuggle 400 (a body naming resourceId/threadId)', async () => {
    // #given — an operator smuggling a TCB-only memory id in the body
    const { topology, calls } = recordingTopology();
    const events: SignalIngestAuditEvent[] = [];
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
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
        threadId: OWNED_THREAD,
      }),
    ]);
  });

  it('does NOT audit a pre-auth 401 (unauthenticated — no flood amplification)', async () => {
    // #given — resolve returns no actor (unauthenticated) with an audit sink
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
      resolve: async () => actorContext('operator'),
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
      resolve: async () => actorContext('operator'),
      topology,
    });
    const res = await router(
      new Request(`http://host/api/threads/${OWNED_THREAD}/message`, {
        method: 'GET',
      }),
    );
    expect(res?.status).toBe(405);
  });

  it.each([
    'constructor',
    'toString',
    '__proto__',
    'hasOwnProperty',
  ])('does not resolve an inherited channel: %s', async (channel) => {
    const { topology, calls } = recordingTopology();
    const resolve = vi.fn(async () => actorContext('operator'));
    const audit = vi.fn();
    const router = createSignalRouter({ resolve, topology, audit });

    expect(
      await router(post(`/api/threads/${OWNED_THREAD}/${channel}`, {})),
    ).toBeNull();
    expect(resolve).not.toHaveBeenCalled();
    expect(audit).not.toHaveBeenCalled();
    expect(calls).toEqual([]);
  });

  it.each([
    ['signal', '/signal'],
    ['message', '/signal/message'],
    ['queue', '/signal/queue'],
    ['state', '/signal/state'],
    ['notification', '/signal/notification'],
  ])('forwards the own channel %s to %s', async (channel, path) => {
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
    });
    expect(
      (await router(post(`/api/threads/${OWNED_THREAD}/${channel}`, {})))
        ?.status,
    ).toBe(200);
    expect(calls).toEqual([{ threadId: OWNED_THREAD, path, body: '{}' }]);
  });

  it.each([
    'admin',
    'viewer',
  ] as const)('makes strict foreign and missing threads indistinguishable for %s before body or rate work', async (role) => {
    const context = actorContext(role);
    context.canAccessResource = vi.fn(async (_kind, id) => id !== 'missing');
    const validateThreadTarget = vi.fn(async () => {
      throw new RunRouteError(404, 'private binding ownership detail');
    });
    const { topology, calls } = recordingTopology();
    const rateLimit = vi.fn(() => true);
    const audit = vi.fn();
    const router = createSignalRouter({
      resolve: async () => context,
      topology,
      validateThreadTarget,
      rateLimit,
      audit,
    });
    const responses = [];
    for (const threadId of ['foreign', 'missing']) {
      const request = post(`/api/threads/${threadId}/message`, '{');
      const response = await router(request);
      expect(request.bodyUsed).toBe(false);
      responses.push({
        status: response?.status,
        headers: [...(response?.headers ?? [])],
        body: await response?.text(),
      });
    }
    expect(responses[0]).toEqual(responses[1]);
    expect(responses[0]).toEqual({
      status: 404,
      headers: [
        ['cache-control', 'no-store'],
        ['content-type', 'application/json'],
      ],
      body: '{"error":"thread not found"}',
    });
    expect(validateThreadTarget).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ actor: { id: 'opal', role } }),
      { threadId: 'foreign' },
    );
    expect(rateLimit).not.toHaveBeenCalled();
    expect(calls).toEqual([]);
    expect(audit.mock.calls.map(([event]) => event)).toEqual(
      ['foreign', 'missing'].map((threadId) =>
        expect.objectContaining({
          threadId,
          outcome: 'rejected',
          reason: 'invalid-thread',
          contentBytes: 0,
        }),
      ),
    );
  });

  it('preserves an admin-permissive registry policy when the validator is omitted', async () => {
    const context = actorContext('admin');
    context.canAccessResource = async () => true;
    const { topology, calls } = recordingTopology();
    const router = createSignalRouter({
      resolve: async () => context,
      topology,
    });

    const response = await router(post('/api/threads/foreign/message', {}));

    expect(response?.status).toBe(200);
    expect(calls).toEqual([
      { threadId: 'foreign', path: '/signal/message', body: '{}' },
    ]);
  });

  it('captures validator and audit callbacks while preserving their options receiver', async () => {
    const { topology } = recordingTopology();
    const validateThreadTarget = vi.fn(async function (
      this: SignalRouterOptions,
    ) {
      expect(this).toBe(options);
    });
    let auditReceiver: SignalRouterOptions | undefined;
    const audit = vi.fn(function (this: SignalRouterOptions) {
      auditReceiver = this;
    });
    const options: SignalRouterOptions = {
      resolve: async () => actorContext('operator'),
      topology,
      validateThreadTarget,
      audit,
    };
    const router = createSignalRouter(options);
    const replacementValidator = vi.fn();
    const replacementAudit = vi.fn();
    options.validateThreadTarget = replacementValidator;
    options.audit = replacementAudit;

    expect(
      (await router(post(`/api/threads/${OWNED_THREAD}/message`, {})))?.status,
    ).toBe(200);
    expect(validateThreadTarget).toHaveBeenCalledOnce();
    expect(audit).toHaveBeenCalledOnce();
    expect(auditReceiver).toBe(options);
    expect(replacementValidator).not.toHaveBeenCalled();
    expect(replacementAudit).not.toHaveBeenCalled();
  });

  it.each([
    'ownership',
    'validator',
  ] as const)('captures actor, principal, deployment and mutation epoch before the %s await', async (boundary) => {
    const entered = deferred<void>();
    const held = deferred<void>();
    const actor = { id: 'opal', role: 'operator' as ApprovalActor['role'] };
    const principal = { kind: 'human' as const, ...actor };
    const context = {
      ...actorContext('operator'),
      actor,
      principal,
      mutationEpoch: 7,
      deploymentTag: 'acme',
      async canAccessResource() {
        expect(this).toBe(context);
        if (boundary === 'ownership') {
          entered.resolve();
          await held.promise;
        }
        return true;
      },
      newThreadId() {
        expect(this).toBe(context);
        return OWNED_THREAD;
      },
    };
    const validateThreadTarget = vi.fn(async (captured: ActorContext) => {
      if (boundary === 'validator') {
        entered.resolve();
        await held.promise;
      }
      expect(captured).not.toBe(context);
      expect(captured.actor).toEqual({ id: 'opal', role: 'operator' });
      expect(captured.principal).toEqual({
        kind: 'human',
        id: 'opal',
        role: 'operator',
      });
      expect(captured.mutationEpoch).toBe(7);
      expect(captured.newThreadId()).toBe(OWNED_THREAD);
    });
    const { topology } = recordingTopology();
    const send = vi.spyOn(topology, 'send');
    const audit = vi.fn();
    const router = createSignalRouter({
      resolve: async () => context,
      topology,
      validateThreadTarget,
      audit,
    });
    const request = post(`/api/threads/${OWNED_THREAD}/message`, {});
    const result = router(request);
    await entered.promise;
    expect(request.bodyUsed).toBe(false);
    expect(send).not.toHaveBeenCalled();
    expect(audit).not.toHaveBeenCalled();
    actor.id = 'mallory';
    actor.role = 'viewer';
    principal.id = 'mallory';
    principal.role = 'admin';
    context.mutationEpoch = 8;
    context.deploymentTag = 'other-deployment';
    context.newThreadId = () => 'other-thread';
    held.resolve();

    expect((await result)?.status).toBe(200);
    expect(send).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        actor: { id: 'opal', role: 'operator' },
        principal: { kind: 'human', id: 'opal', role: 'operator' },
        mutationEpoch: 7,
        deploymentTag: 'acme',
      }),
      OWNED_THREAD,
      '/signal/message',
      expect.any(Object),
    );
    expect(audit).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        actorId: 'opal',
        deploymentTag: 'acme',
        outcome: 'accepted',
      }),
    );
  });

  it('waits for the downstream decision before auditing acceptance', async () => {
    const decision = deferred<Response>();
    const entered = deferred<void>();
    const { topology } = recordingTopology();
    vi.spyOn(topology, 'send').mockImplementation(async () => {
      entered.resolve();
      return decision.promise;
    });
    const audit = vi.fn();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
      audit,
    });
    const pending = router(post(`/api/threads/${OWNED_THREAD}/message`, {}));
    await entered.promise;
    expect(audit).not.toHaveBeenCalled();
    const response = new Response('delivered', {
      status: 202,
      headers: { 'x-delivery': 'accepted' },
    });
    decision.resolve(response);
    expect(await pending).toBe(response);
    expect(audit).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ outcome: 'accepted', contentBytes: 2 }),
    );
  });

  it('retains a completed delivery while its pending audit rejects', async () => {
    const entered = deferred<void>();
    const held = deferred<void>();
    const { topology } = recordingTopology();
    const response = new Response('delivered', { status: 202 });
    const send = vi.spyOn(topology, 'send').mockResolvedValue(response);
    const log = vi.spyOn(console, 'error').mockImplementation(() => {
      throw new Error('logger unavailable');
    });
    const audit = vi.fn(async () => {
      entered.resolve();
      await held.promise;
      throw new ActorResolutionError('audit refused');
    });
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
      audit,
    });
    try {
      const pending = router(post(`/api/threads/${OWNED_THREAD}/message`, {}));
      await entered.promise;
      expect(send).toHaveBeenCalledOnce();
      expect(audit).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ outcome: 'accepted' }),
      );
      held.resolve();
      expect(await pending).toBe(response);
      expect(audit).toHaveBeenCalledOnce();
      expect(send).toHaveBeenCalledOnce();
    } finally {
      held.resolve();
      log.mockRestore();
    }
  });

  it.each([
    403, 404, 422, 503,
  ])('audits one downstream %s rejection and preserves its public response', async (status) => {
    const { topology } = recordingTopology();
    const response = new Response('private downstream refusal', {
      status,
      headers: {
        'content-type': 'text/plain',
        'retry-after': '5',
        'x-owner': 'foreign-owner',
      },
    });
    vi.spyOn(topology, 'send').mockResolvedValue(response);
    const audit = vi.fn();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
      audit,
    });
    const result = await router(
      post(`/api/threads/${OWNED_THREAD}/message`, {}),
    );
    expect(result?.status).toBe(status);
    expect(audit).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        outcome: 'rejected',
        reason: status === 404 ? 'invalid-thread' : `downstream-${status}`,
      }),
    );
    expect(response.bodyUsed).toBe(false);
    if (status === 404) {
      expect(await result?.text()).toBe('{"error":"thread not found"}');
      expect([...(result?.headers ?? [])]).toEqual([
        ['cache-control', 'no-store'],
        ['content-type', 'application/json'],
      ]);
    } else {
      expect(result).toBe(response);
      expect(await result?.text()).toBe('private downstream refusal');
    }
  });

  it.each([
    [
      new RunRouteError(404, 'private missing binding'),
      404,
      'invalid-thread',
      'thread not found',
    ],
    [
      new RunRouteError(503, 'temporarily unavailable'),
      503,
      'route-error-503',
      'temporarily unavailable',
    ],
    [
      new ActorResolutionError('private principal detail'),
      403,
      'forbidden',
      'forbidden',
    ],
    [
      new Error('private backend detail'),
      500,
      'internal-error',
      'internal error',
    ],
  ] as const)('audits one final outcome when forwarding throws %s', async (error, status, reason, message) => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const { topology } = recordingTopology();
    vi.spyOn(topology, 'send').mockRejectedValue(error);
    const audit = vi.fn();
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
      audit,
    });
    try {
      const response = await router(
        post(`/api/threads/${OWNED_THREAD}/message`, {}),
      );
      expect(response?.status).toBe(status);
      expect(await response?.json()).toEqual({ error: message });
      expect(audit).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ outcome: 'rejected', reason }),
      );
    } finally {
      log.mockRestore();
    }
  });

  it.each([
    new ActorResolutionError('invalid claims'),
    new Error('authentication backend unavailable'),
  ])('does not audit a resolver exception: %s', async (error) => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const { topology, calls } = recordingTopology();
    const audit = vi.fn();
    const router = createSignalRouter({
      resolve: async () => {
        throw error;
      },
      topology,
      audit,
    });
    try {
      const response = await router(
        post(`/api/threads/${OWNED_THREAD}/message`, {}),
      );
      expect(response?.status).toBe(
        error instanceof ActorResolutionError ? 403 : 500,
      );
      expect(audit).not.toHaveBeenCalled();
      expect(calls).toEqual([]);
    } finally {
      log.mockRestore();
    }
  });

  it.each([
    ['accepted', '{}', 'operator', 200, undefined],
    ['role rejection', '{}', 'viewer', 403, 'forbidden-role'],
    ['parse refusal', '{', 'operator', 400, 'malformed-body'],
    ['non-object refusal', 'null', 'operator', 400, 'malformed-body'],
    [
      'memory-id refusal',
      '{"threadId":"foreign"}',
      'operator',
      400,
      'client-memory-id',
    ],
  ] as const)('preserves %s when the audit sink throws a typed error', async (_label, body, role, status, reason) => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => {
      throw new Error('logger unavailable');
    });
    const { topology, calls } = recordingTopology();
    const audit = vi.fn(() => {
      throw new RunRouteError(503, 'sink unavailable');
    });
    const router = createSignalRouter({
      resolve: async () => actorContext(role),
      topology,
      audit,
    });
    try {
      const response = await router(
        post(`/api/threads/${OWNED_THREAD}/message`, body),
      );
      expect(response?.status).toBe(status);
      expect(audit).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          outcome: status === 200 ? 'accepted' : 'rejected',
          ...(reason === undefined ? {} : { reason }),
        }),
      );
      expect(calls).toHaveLength(status === 200 ? 1 : 0);
      if (status === 403)
        expect(await response?.json()).toEqual({ error: 'forbidden' });
      if (reason === 'malformed-body')
        expect(await response?.json()).toEqual({
          error: 'a JSON object body is required',
        });
    } finally {
      log.mockRestore();
    }
  });

  it.each([
    'actor-error',
    'message-getter',
    'toString',
  ] as const)('contains an audit %s failure without changing the downstream response', async (failure) => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const error =
      failure === 'actor-error'
        ? new ActorResolutionError('sink refused')
        : failure === 'message-getter'
          ? Object.defineProperty(new Error(), 'message', {
              get() {
                throw new Error('unreadable message');
              },
            })
          : {
              toString() {
                throw new Error('unreadable value');
              },
            };
    const { topology } = recordingTopology();
    const response = new Response('unavailable', {
      status: 503,
      headers: { 'retry-after': '9' },
    });
    vi.spyOn(topology, 'send').mockResolvedValue(response);
    const audit = vi.fn(async () => {
      throw error;
    });
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology,
      audit,
    });
    try {
      expect(
        await router(post(`/api/threads/${OWNED_THREAD}/message`, {})),
      ).toBe(response);
      expect(audit).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          outcome: 'rejected',
          reason: 'downstream-503',
        }),
      );
      expect(log).toHaveBeenCalledOnce();
      expect(JSON.parse(log.mock.calls[0]?.[0])).toMatchObject({
        type: 'signal.ingest-audit-error',
        reason: failure === 'actor-error' ? 'sink refused' : 'unreadable error',
      });
    } finally {
      log.mockRestore();
    }
  });

  it('returns a generic 500 while retaining internal detail in structured logs', async () => {
    const logged: string[] = [];
    const log = vi.spyOn(console, 'error').mockImplementation((value) => {
      logged.push(String(value));
    });
    const router = createSignalRouter({
      resolve: async () => actorContext('operator'),
      topology: {
        send: async () => {
          throw new Error('private signal backend detail');
        },
      } as unknown as ThreadTopology,
    });

    try {
      const response = await router(
        post(`/api/threads/${OWNED_THREAD}/message`, { contents: 'hi' }),
      );
      expect(response?.status).toBe(500);
      expect(await response?.json()).toEqual({ error: 'internal error' });
      expect(response?.headers.get('cache-control')).toBe('no-store');
      expect(logged.join('\n')).toContain('private signal backend detail');
    } finally {
      log.mockRestore();
    }
  });
});
