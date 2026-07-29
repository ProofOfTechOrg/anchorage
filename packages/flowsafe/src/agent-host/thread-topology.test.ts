// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { ApprovalRecord, TenantContext } from '../approval-api/index.js';
import type {
  ThreadNamespaceLike,
  ThreadRequestInit,
} from '../host-kit/index.js';

import {
  type AgentThreadTopology,
  createAgentThreadTopology,
} from './thread-topology.js';

interface Hit {
  threadId: string;
  url: string;
  init?: ThreadRequestInit;
}

function harness(): {
  topology: AgentThreadTopology;
  hits: Hit[];
} {
  const hits: Hit[] = [];
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: (name) => name,
    get: (threadId) => ({
      fetch: (async (request: Request | string, init?: ThreadRequestInit) => {
        const url = typeof request === 'string' ? request : request.url;
        hits.push({ threadId, url, init });
        if (url.endsWith('/start')) {
          const body = JSON.parse(init?.body ?? '{}') as Record<string, string>;
          return Response.json({
            agentId: body.agentId,
            threadId: body.threadId,
            resourceId: body.resourceId,
            runId: body.runId,
            summary: { runId: body.runId, status: 'success' },
          });
        }
        if (url.endsWith('/resume')) {
          const body = JSON.parse(init?.body ?? '{}') as Record<string, string>;
          return Response.json({
            agentId: body.agentId,
            threadId: body.threadId,
            resourceId: body.resourceId,
            runId: body.runId,
            summary: { runId: body.runId, status: 'success' },
          });
        }
        if (url.includes('/stream?')) {
          return new Response('{"offset":1,"event":{}}\n');
        }
        return Response.json({
          agentId: 'writer',
          threadId,
          resourceId: `acme_resource_${threadId}`,
          runId: 'acme_run',
          summary: { runId: 'acme_run', status: 'success' },
        });
      }) as ReturnType<ThreadNamespaceLike<string>['get']>['fetch'],
    }),
  };
  return { topology: createAgentThreadTopology(namespace), hits };
}

function tenant() {
  let runMints = 0;
  let threadMints = 0;
  const value: TenantContext = {
    actor: { id: 'operator-1', role: 'operator', tenantId: 'acme' },
    principal: {
      kind: 'human',
      id: 'operator-1',
      tenantId: 'acme',
      role: 'operator',
    },
    tenantId: 'acme',
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => `acme_run_${++runMints}`,
    ownsRun: (id) => id.startsWith('acme_'),
    newThreadId: () => `acme_thread_${++threadMints}`,
    newResourceId: (threadId) => `acme_resource_${threadId}`,
    ownsMemoryId: (id) => id.startsWith('acme_'),
    canSelfDecide: () => false,
  };
  return {
    value,
    runMints: () => runMints,
    threadMints: () => threadMints,
  };
}

describe('createAgentThreadTopology', () => {
  it('mints each HTTP start identity exactly once and stamps the principal', async () => {
    const { topology, hits } = harness();
    const scoped = tenant();
    const result = await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start',
    });
    expect(result.runId).toBe('acme_run_1');
    expect(scoped.runMints()).toBe(1);
    expect(scoped.threadMints()).toBe(1);
    expect(hits[0]?.init?.headers).toMatchObject({
      'x-flowsafe-tenant': 'acme',
      // The principal is the sole identity channel; the DO projects the actor
      // from it rather than trusting a second header.
      'x-flowsafe-principal':
        '{"kind":"human","id":"operator-1","role":"operator"}',
    });
  });

  it('accepts an already server-minted run for schedules without minting twice', async () => {
    const { topology, hits } = harness();
    const scoped = tenant();
    const result = await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      runId: 'acme_scheduled-run',
      threadId: 'acme_thread_existing',
      resourceId: 'acme_resource_acme_thread_existing',
      requestContext: {
        inherited: 'request',
        overridden: 'request',
        runId: 'forged',
      },
      streamRequestContext: {
        streamed: 'yes',
        overridden: 'stream',
        'breakwater.actor': { id: 'forged' },
      },
    });
    expect(result.runId).toBe('acme_scheduled-run');
    expect(scoped.runMints()).toBe(0);
    expect(scoped.threadMints()).toBe(0);
    const body = JSON.parse(hits[0]?.init?.body ?? '{}') as {
      safeContext: Record<string, unknown>;
    };
    expect(body.safeContext).toEqual({
      inherited: 'request',
      overridden: 'stream',
      streamed: 'yes',
    });
  });

  it('mints an ephemeral thread for unthreaded schedules and rejects a supplied thread', async () => {
    const { topology } = harness();
    const scoped = tenant();
    await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      threaded: false,
    });
    expect(scoped.threadMints()).toBe(1);
    await expect(
      topology.start(scoped.value, {
        agentId: 'writer',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
        threaded: false,
        threadId: 'acme_existing',
      }),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('routes status, observation, and approval resume through the owning thread', async () => {
    const { topology, hits } = harness();
    const scoped = tenant();
    await topology.status(scoped.value, {
      agentId: 'writer',
      threadId: 'acme_thread',
      runId: 'acme_run',
    });
    await topology.observe(scoped.value, {
      agentId: 'writer',
      threadId: 'acme_thread',
      runId: 'acme_run',
      offset: 4,
    });
    const record = {
      workflowId: 'durable-agentic-loop',
      runId: 'acme_run',
      tenantId: 'acme',
      stepPath: ['tool'],
      resumeTarget: {
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource_acme_thread',
        principal: scoped.value.actor,
      },
    } as ApprovalRecord;
    await topology.resume(scoped.value, record, 'approve');
    expect(hits.map((hit) => hit.threadId)).toEqual([
      'acme_thread',
      'acme_thread',
      'acme_thread',
    ]);
    expect(hits[1]?.url).toContain('offset=4');
  });

  it('refuses foreign trusted IDs before addressing a DO', async () => {
    const { topology, hits } = harness();
    const scoped = tenant();
    await expect(
      topology.status(scoped.value, {
        agentId: 'writer',
        threadId: 'globex_thread',
        runId: 'globex_run',
      }),
    ).rejects.toMatchObject({ status: 404 });
    await expect(
      topology.status(scoped.value, {
        agentId: 'writer',
        threadId: 'acme_thread',
        runId: 'globex_run',
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(hits).toEqual([]);
  });
});
