// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type {
  ActorContext,
  ApprovalActor,
  ApprovalRole,
} from '../approval-api/index.js';
import {
  ActorResolutionError,
  createActorResolver,
  humanPrincipal,
  InMemoryApprovalStoreFactory,
} from '../approval-api/index.js';
import { RunRouteError } from '../host-kit/index.js';
import { createAgentRouter } from './router.js';
import type { AgentThreadTopology } from './thread-topology.js';
import type { AgentRunEnvelope } from './types.js';

const agents = [
  {
    id: 'writer',
    title: 'Writer',
    description: 'Writes an approved record',
    allowedRoles: ['admin', 'operator'] as const,
  },
];

function context(role: ApprovalRole = 'operator'): ActorContext {
  const actor: ApprovalActor = { id: `${role}-1`, role };
  const principal = humanPrincipal(actor);
  return {
    actor,
    principal,
    resourceOwner: { kind: principal.kind, id: principal.id },
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => 'acme_run',
    newThreadId: () => 'acme_thread',
    resourceIdFromKey: () => 'acme_resource',
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async (kind, id) =>
      (kind === 'thread' && id === 'acme_thread') ||
      (kind === 'run' && id === 'acme_run') ||
      kind === 'resource',
    canSelfDecide: (candidate) => candidate === 'admin',
  };
}

function resourceContext(
  actorId: string,
  threadId: string,
  runId: string,
): ActorContext {
  const scoped = context();
  const actor: ApprovalActor = { id: actorId, role: 'operator' };
  const principal = humanPrincipal(actor);
  return {
    ...scoped,
    actor,
    principal,
    resourceOwner: { kind: principal.kind, id: principal.id },
    canAccessResource: async (kind, id) =>
      (kind === 'thread' && id === threadId) ||
      (kind === 'run' && id === runId),
  };
}

function envelope(): AgentRunEnvelope {
  return {
    agentId: 'writer',
    threadId: 'acme_thread',
    resourceId: 'acme_resource',
    runId: 'acme_run',
    summary: { runId: 'acme_run', status: 'success', result: 'done' },
  };
}

function topology() {
  return {
    start: vi.fn(async () => envelope()),
    status: vi.fn<AgentThreadTopology['status']>(async () => envelope()),
    observe: vi.fn<AgentThreadTopology['observe']>(
      async () =>
        new Response('{"offset":1,"event":{}}\n', {
          headers: {
            'content-type': 'application/x-ndjson; charset=utf-8',
          },
        }),
    ),
    terminate: vi.fn<NonNullable<AgentThreadTopology['terminate']>>(async () =>
      envelope(),
    ),
    resume: vi.fn(async () => envelope()),
  } satisfies AgentThreadTopology;
}

async function payload(response: Response): Promise<Record<string, unknown>> {
  return (await response.json()) as Record<string, unknown>;
}

describe('createAgentRouter', () => {
  it('lists metadata and the authenticated actor for every role', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => context('viewer'),
      topology: host,
    });
    const response = await router(new Request('https://host/agents'));
    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual({
      agents,
      actor: {
        id: 'viewer-1',
        role: 'viewer',
        canSelfDecide: false,
      },
    });
  });

  it('resolves the agent before applying mutation authorization', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => context('reviewer'),
      topology: host,
    });
    const unknown = await router(
      new Request('https://host/agents/missing/runs', {
        method: 'POST',
        body: JSON.stringify({ prompt: 'go' }),
      }),
    );
    expect(unknown?.status).toBe(404);
    const known = await router(
      new Request('https://host/agents/writer/runs', {
        method: 'POST',
        body: JSON.stringify({ prompt: 'go' }),
      }),
    );
    expect(known?.status).toBe(403);
    expect(host.start).not.toHaveBeenCalled();
  });

  it('preserves the prompt and delegates an authorized start', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });
    const prompt = '  preserve this whitespace  ';
    const response = await router(
      new Request('https://host/agents/writer/runs', {
        method: 'POST',
        body: JSON.stringify({ prompt }),
      }),
    );
    expect(response?.status).toBe(200);
    expect(host.start).toHaveBeenCalledWith(expect.objectContaining({}), {
      agentId: 'writer',
      prompt,
      entryPath: 'http.start',
    });
  });

  it.each([
    [{}, 400],
    [{ prompt: ' ' }, 400],
    [{ prompt: 'ok', runId: 'forged' }, 400],
    [{ prompt: 'ok', requestContext: {} }, 400],
    [{ prompt: 'ok', __proto__: null, constructor: 'forged' }, 400],
    [[], 400],
  ])('rejects invalid start body %#', async (body, status) => {
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: topology(),
    });
    const response = await router(
      new Request('https://host/agents/writer/runs', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    );
    expect(response?.status).toBe(status);
  });

  it('cancels bounded body consumption on overflow', async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(16_385));
      },
      cancel() {
        cancelled = true;
      },
    });
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: topology(),
    });
    const response = await router(
      new Request('https://host/agents/writer/runs', {
        method: 'POST',
        body,
        duplex: 'half',
      } as RequestInit & { duplex: 'half' }),
    );
    expect(response?.status).toBe(413);
    expect(cancelled).toBe(true);
  });

  it('allows every authenticated role to read deployment-local status and streams', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => context('viewer'),
      topology: host,
    });
    const status = await router(
      new Request('https://host/agents/writer/runs/acme_thread/acme_run'),
    );
    expect(status?.status).toBe(200);
    const stream = await router(
      new Request(
        'https://host/agents/writer/runs/acme_thread/acme_run/stream?offset=7',
      ),
    );
    expect(stream?.status).toBe(200);
    expect(host.observe).toHaveBeenCalledWith(
      expect.objectContaining({}),
      expect.objectContaining({ offset: 7 }),
    );
  });

  it('attaches only to the requesting operator stream without a foreign-resource oracle', async () => {
    const host = topology();
    const alice = resourceContext('alice', 'alice_thread', 'alice_run');
    const bob = resourceContext('bob', 'bob_thread', 'bob_run');
    host.status.mockImplementation(async (_, input) => ({
      agentId: input.agentId,
      threadId: input.threadId,
      resourceId: `${input.threadId}_resource`,
      runId: input.runId,
      summary: { runId: input.runId, status: 'running' },
    }));
    host.observe.mockImplementation(
      async (scoped, input) =>
        new Response(
          `${JSON.stringify({
            offset: input.offset + 1,
            event: {
              actorId: scoped.actor.id,
              threadId: input.threadId,
              runId: input.runId,
            },
          })}\n`,
          {
            headers: {
              'content-type': 'application/x-ndjson; charset=utf-8',
            },
          },
        ),
    );
    const router = createAgentRouter({
      agents,
      resolve: async (request) =>
        request.headers.get('authorization') === 'Bearer alice' ? alice : bob,
      topology: host,
    });
    const stream = (actor: 'alice' | 'bob', threadId: string, runId: string) =>
      router(
        new Request(
          `https://host/agents/writer/runs/${threadId}/${runId}/stream`,
          { headers: { authorization: `Bearer ${actor}` } },
        ),
      );

    const [aliceOwn, bobOwn, ...foreign] = await Promise.all([
      stream('alice', 'alice_thread', 'alice_run'),
      stream('bob', 'bob_thread', 'bob_run'),
      stream('alice', 'bob_thread', 'bob_run'),
      stream('bob', 'alice_thread', 'alice_run'),
      stream('alice', 'alice_thread', 'bob_run'),
      stream('alice', 'bob_thread', 'alice_run'),
      stream('bob', 'bob_thread', 'alice_run'),
      stream('bob', 'alice_thread', 'bob_run'),
    ]);

    expect([aliceOwn?.status, bobOwn?.status]).toEqual([200, 200]);
    await expect(aliceOwn?.json()).resolves.toEqual({
      offset: 1,
      event: {
        actorId: 'alice',
        threadId: 'alice_thread',
        runId: 'alice_run',
      },
    });
    await expect(bobOwn?.json()).resolves.toEqual({
      offset: 1,
      event: {
        actorId: 'bob',
        threadId: 'bob_thread',
        runId: 'bob_run',
      },
    });
    expect(foreign.map((response) => response?.status)).toEqual(
      Array.from({ length: 6 }, () => 404),
    );
    await Promise.all(
      foreign.map(async (response) =>
        expect(await response?.json()).toEqual({ error: 'run not found' }),
      ),
    );
    expect(host.status).toHaveBeenCalledTimes(2);
    expect(host.status).toHaveBeenCalledWith(
      alice,
      expect.objectContaining({
        agentId: 'writer',
        threadId: 'alice_thread',
        runId: 'alice_run',
      }),
    );
    expect(host.status).toHaveBeenCalledWith(
      bob,
      expect.objectContaining({
        agentId: 'writer',
        threadId: 'bob_thread',
        runId: 'bob_run',
      }),
    );
    expect(host.observe).toHaveBeenCalledTimes(2);
    expect(host.observe).toHaveBeenCalledWith(
      alice,
      expect.objectContaining({
        agentId: 'writer',
        threadId: 'alice_thread',
        runId: 'alice_run',
        offset: 0,
      }),
    );
    expect(host.observe).toHaveBeenCalledWith(
      bob,
      expect.objectContaining({
        agentId: 'writer',
        threadId: 'bob_thread',
        runId: 'bob_run',
        offset: 0,
      }),
    );
  });

  it('delegates owner-release replay authorization to the topology', async () => {
    const host = topology();
    const owned = context();
    const replay = {
      ...owned,
      canAccessResource: vi.fn(async () => false),
    } satisfies ActorContext;
    let actor = owned;
    const router = createAgentRouter({
      agents,
      resolve: async () => actor,
      topology: host,
    });
    const request = () =>
      new Request(
        'https://host/agents/writer/runs/acme_thread/acme_run/terminate',
        { method: 'POST' },
      );

    const first = await router(request());
    expect(first?.status).toBe(200);
    expect(host.terminate).toHaveBeenLastCalledWith(
      owned,
      expect.objectContaining({
        agentId: 'writer',
        threadId: 'acme_thread',
        runId: 'acme_run',
      }),
      false,
    );

    actor = replay;
    const retried = await router(request());
    expect(retried?.status).toBe(200);
    expect(host.terminate).toHaveBeenLastCalledWith(
      replay,
      expect.objectContaining({ runId: 'acme_run' }),
      true,
    );
  });

  it('preserves a structured disputed-settlement reason from the agent topology', async () => {
    const host = topology();
    host.terminate.mockRejectedValueOnce(
      new RunRouteError(409, 'run termination is blocked', {
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      }),
    );
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });

    const response = await router(
      new Request(
        'https://host/agents/writer/runs/acme_thread/acme_run/terminate',
        { method: 'POST' },
      ),
    );

    expect(response?.status).toBe(409);
    await expect(response?.json()).resolves.toEqual({
      error: 'run termination is blocked',
      reason: {
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      },
    });
  });

  it('rejects invalid termination methods and query fields without invoking the topology', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });

    const wrongMethod = await router(
      new Request(
        'https://host/agents/writer/runs/acme_thread/acme_run/terminate',
      ),
    );
    expect(wrongMethod?.status).toBe(405);
    expect(wrongMethod?.headers.get('allow')).toBe('POST');

    const query = await router(
      new Request(
        'https://host/agents/writer/runs/acme_thread/acme_run/terminate?forged=1',
        { method: 'POST' },
      ),
    );
    expect(query?.status).toBe(400);
    expect(host.terminate).not.toHaveBeenCalled();
  });

  it.each([
    '-1',
    '1.5',
    '01',
    '9007199254740992',
  ])('rejects invalid stream offset %s', async (offset) => {
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: topology(),
    });
    const response = await router(
      new Request(
        `https://host/agents/writer/runs/acme_thread/acme_run/stream?offset=${offset}`,
      ),
    );
    expect(response?.status).toBe(400);
  });

  it.each([
    [
      'status query',
      'https://host/agents/writer/runs/acme_thread/acme_run?forged=1',
    ],
    [
      'stream offset',
      'https://host/agents/writer/runs/acme_thread/acme_run/stream?offset=-1',
    ],
  ])('returns a binding-mismatch 404 before validating an invalid %s', async (_label, url) => {
    const host = topology();
    host.status.mockImplementation(async () => undefined);
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });

    const response = await router(new Request(url));

    expect(response?.status).toBe(404);
    expect(host.status).toHaveBeenCalledTimes(1);
    expect(host.observe).not.toHaveBeenCalled();
  });

  it('passes a 5xx refusal through with its status and reason intact', async () => {
    // #given — the run DO refusing because the deployment is fenced. This is a
    // 5xx, which this router used to collapse into a bare 500 — turning "retry
    // after the migration" into "I am broken" for every agent caller.
    const host = topology();
    host.start.mockRejectedValueOnce(
      new RunRouteError(
        503,
        "deployment execution is fenced ('migration-locked'): run start is refused",
        { code: 'EXECUTION_FENCED', state: 'migration-locked' },
      ),
    );
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });

    // #when
    const response = await router(
      new Request('https://host/agents/writer/runs', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ prompt: 'go' }),
      }),
    );

    // #then
    expect(response?.status).toBe(503);
    expect(await response?.json()).toEqual({
      error:
        "deployment execution is fenced ('migration-locked'): run start is refused",
      reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
    });
  });

  it('still collapses a 5xx with NO structured reason into a generic 500', async () => {
    // #given — the passthrough is narrow on purpose: only a code the DO
    // deliberately published survives a 5xx; an unclassified fault must not
    // start leaking its message to callers.
    const host = topology();
    host.start.mockRejectedValueOnce(
      new RunRouteError(502, 'upstream exploded with connection details'),
    );
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });

    // #then
    const response = await router(
      new Request('https://host/agents/writer/runs', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ prompt: 'go' }),
      }),
    );
    expect(response?.status).toBe(500);
    expect(await response?.json()).toEqual({ error: 'internal server error' });
  });

  it('maps an asynchronously rejected observation to its route status', async () => {
    const host = topology();
    host.observe.mockRejectedValue(
      new RunRouteError(
        409,
        'stream replay is unavailable; inspect status instead',
      ),
    );
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });

    const response = await router(
      new Request(
        'https://host/agents/writer/runs/acme_thread/acme_run/stream',
      ),
    );

    expect(response?.status).toBe(409);
    expect(await response?.json()).toEqual({
      error: 'stream replay is unavailable; inspect status instead',
    });
  });

  it('404s foreign path-safe IDs and rejects any public resume path', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });
    const foreign = await router(
      new Request('https://host/agents/writer/runs/globex_thread/globex_run'),
    );
    expect(foreign?.status).toBe(404);
    expect(host.status).not.toHaveBeenCalled();
    const resume = await router(
      new Request(
        'https://host/agents/writer/runs/acme_thread/acme_run/resume',
        { method: 'POST' },
      ),
    );
    expect(resume?.status).toBe(404);
  });

  it('resolves ownership before method and binding errors', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });
    const foreign = await router(
      new Request('https://host/agents/writer/runs/globex_thread/globex_run', {
        method: 'POST',
      }),
    );
    expect(foreign?.status).toBe(404);
    expect(host.status).not.toHaveBeenCalled();

    host.status.mockImplementation(async () => undefined);
    const mismatched = await router(
      new Request('https://host/agents/writer/runs/acme_thread/acme_run', {
        method: 'POST',
      }),
    );
    expect(mismatched?.status).toBe(404);
    expect(host.status).toHaveBeenCalledOnce();
  });

  it.each([
    'https://host/agents/writer/runs/acme_%2F/acme_run',
    'https://host/agents/writer/runs/acme_thread/acme_%00',
  ])('returns 404 for a path-unsafe encoded run reference', async (url) => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: host,
    });

    const response = await router(new Request(url));

    expect(response?.status).toBe(404);
    expect(host.status).not.toHaveBeenCalled();
    expect(host.observe).not.toHaveBeenCalled();
  });

  it('returns 405 with Allow for a known route and null outside its prefix', async () => {
    const router = createAgentRouter({
      agents,
      resolve: async () => context(),
      topology: topology(),
    });
    const response = await router(
      new Request('https://host/agents', { method: 'POST' }),
    );
    expect(response?.status).toBe(405);
    expect(response?.headers.get('allow')).toBe('GET');
    await expect(
      router(new Request('https://host/workflows')),
    ).resolves.toBeNull();
  });

  it('maps malformed claims to 403 and hides internal error details', async () => {
    const malformed = createAgentRouter({
      agents,
      resolve: async () => {
        throw new ActorResolutionError('private claim details');
      },
      topology: topology(),
    });
    const denied = await malformed(new Request('https://host/agents'));
    expect(denied?.status).toBe(403);
    expect(await denied?.text()).not.toContain('private claim details');

    const broken = createAgentRouter({
      agents,
      resolve: async () => {
        throw new Error('database password leaked');
      },
      topology: topology(),
    });
    const failed = await broken(new Request('https://host/agents'));
    expect(failed?.status).toBe(500);
    expect(await payload(failed as Response)).toEqual({
      error: 'internal server error',
    });
  });

  it('returns 403 instead of echoing malformed authenticated actor claims', async () => {
    const resolve = createActorResolver({
      authenticate: () =>
        ({
          id: '',
          role: 'root',
        }) as never,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('service must not be built');
      },
    });
    const router = createAgentRouter({
      agents,
      resolve,
      topology: topology(),
    });

    const response = await router(new Request('https://host/agents'));

    expect(response?.status).toBe(403);
    expect(await response?.text()).not.toContain('root');
  });
});
