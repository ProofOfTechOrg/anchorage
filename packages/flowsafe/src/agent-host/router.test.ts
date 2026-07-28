// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import type {
  ApprovalActor,
  ApprovalRole,
  TenantContext,
} from '../approval-api/index.js';
import {
  createTenantResolver,
  InMemoryApprovalStoreFactory,
  TenantResolutionError,
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

function tenant(role: ApprovalRole = 'operator'): TenantContext {
  const actor: ApprovalActor = { id: `${role}-1`, role, tenantId: 'acme' };
  return {
    actor,
    tenantId: actor.tenantId,
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => 'acme_run',
    ownsRun: (id) => id.startsWith('acme_'),
    newThreadId: () => 'acme_thread',
    newResourceId: () => 'acme_resource',
    ownsMemoryId: (id) => id.startsWith('acme_'),
    canSelfDecide: (candidate) => candidate === 'admin',
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
    observe: vi.fn(
      async () =>
        new Response('{"offset":1,"event":{}}\n', {
          headers: {
            'content-type': 'application/x-ndjson; charset=utf-8',
          },
        }),
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
      resolve: async () => tenant('viewer'),
      topology: host,
    });
    const response = await router(new Request('https://host/agents'));
    expect(response?.status).toBe(200);
    expect(await response?.json()).toEqual({
      agents,
      actor: {
        id: 'viewer-1',
        role: 'viewer',
        tenantId: 'acme',
        canSelfDecide: false,
      },
    });
  });

  it('resolves the agent before applying mutation authorization', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => tenant('reviewer'),
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
      resolve: async () => tenant(),
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
    expect(host.start).toHaveBeenCalledWith(
      expect.objectContaining({ tenantId: 'acme' }),
      {
        agentId: 'writer',
        prompt,
        entryPath: 'http.start',
      },
    );
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
      resolve: async () => tenant(),
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
      resolve: async () => tenant(),
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

  it('allows every authenticated role to read same-tenant status and streams', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => tenant('viewer'),
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
      expect.objectContaining({ tenantId: 'acme' }),
      expect.objectContaining({ offset: 7 }),
    );
  });

  it.each([
    '-1',
    '1.5',
    '01',
    '9007199254740992',
  ])('rejects invalid stream offset %s', async (offset) => {
    const router = createAgentRouter({
      agents,
      resolve: async () => tenant(),
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
      resolve: async () => tenant(),
      topology: host,
    });

    const response = await router(new Request(url));

    expect(response?.status).toBe(404);
    expect(host.status).toHaveBeenCalledTimes(1);
    expect(host.observe).not.toHaveBeenCalled();
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
      resolve: async () => tenant(),
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

  it('returns 404 for foreign IDs and any public resume path', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => tenant(),
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

  it('returns 404 before method errors for foreign and binding-mismatched run IDs', async () => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => tenant(),
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
    expect(host.status).toHaveBeenCalledTimes(1);
  });

  it.each([
    'https://host/agents/writer/runs/acme_%2F/acme_run',
    'https://host/agents/writer/runs/acme_thread/acme_%00',
  ])('returns 404 for a path-unsafe encoded run reference', async (url) => {
    const host = topology();
    const router = createAgentRouter({
      agents,
      resolve: async () => tenant(),
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
      resolve: async () => tenant(),
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
        throw new TenantResolutionError('private claim details');
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
    const resolve = createTenantResolver({
      authenticate: () =>
        ({
          id: '',
          role: 'root',
          tenantId: 'acme',
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
