// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import type { ActorContext, ApprovalRecord } from '../approval-api/index.js';
import type {
  ThreadNamespaceLike,
  ThreadRequestInit,
} from '../host-kit/index.js';

import {
  type AgentThreadDispatchTopology,
  createAgentThreadTopology,
} from './thread-topology.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

interface Hit {
  threadId: string;
  url: string;
  init?: ThreadRequestInit;
}

function harness(): {
  topology: AgentThreadDispatchTopology;
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
        if (url.includes('/disputed_run/terminate?')) {
          return Response.json(
            {
              error: 'run termination is blocked',
              reason: {
                code: 'DISPUTED_SETTLEMENT',
                message:
                  'run termination is blocked while an economic operation is disputed',
              },
            },
            { status: 409 },
          );
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
  return {
    topology: createAgentThreadTopology(namespace, DEPLOYMENT_IDENTITY_SECRET),
    hits,
  };
}

function context() {
  let runMints = 0;
  let threadMints = 0;
  const value: ActorContext = {
    actor: { id: 'operator-1', role: 'operator' },
    principal: {
      kind: 'human',
      id: 'operator-1',
      role: 'operator',
    },
    resourceOwner: { kind: 'human', id: 'operator-1' },
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => `acme_run_${++runMints}`,
    newThreadId: () => `acme_thread_${++threadMints}`,
    resourceIdFromKey: (threadId) => `acme_resource_${threadId}`,
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async () => true,
    canSelfDecide: () => false,
  };
  return {
    value,
    runMints: () => runMints,
    threadMints: () => threadMints,
  };
}

describe('createAgentThreadTopology', () => {
  it('validates a standing target through its owning thread DO binding', async () => {
    const { topology, hits } = harness();
    const scoped = context();

    await topology.requireBoundThread(scoped.value, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: 'acme_resource_acme_thread',
    });

    expect(hits).toEqual([
      expect.objectContaining({
        threadId: 'acme_thread',
        url: expect.stringContaining(
          '/_flowsafe/agent-host/binding?resourceId=acme_resource_acme_thread&agentId=writer',
        ),
      }),
    ]);
  });

  it('mints each server-owned HTTP start id once and stamps the principal', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    const result = await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start',
    });
    expect(result.runId).toBe('acme_run_1');
    expect(scoped.runMints()).toBe(1);
    expect(scoped.threadMints()).toBe(1);
    expect(hits[0]?.init?.headers).toMatchObject({
      // The principal is the sole identity channel; the DO projects the actor
      // from it rather than trusting a second header.
      'x-flowsafe-principal':
        '{"kind":"human","id":"operator-1","role":"operator"}',
    });
  });

  it('accepts an already server-minted run for schedules without minting twice', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    const result = await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      scheduleId: 'acme_schedule',
      dispatchId: 'acme_dispatch',
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
    const scoped = context();
    await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      scheduleId: 'acme_schedule',
      dispatchId: 'acme_dispatch',
      threaded: false,
    });
    expect(scoped.threadMints()).toBe(1);
    await expect(
      topology.start(scoped.value, {
        agentId: 'writer',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
        scheduleId: 'acme_schedule',
        dispatchId: 'acme_dispatch',
        threaded: false,
        threadId: 'acme_existing',
      }),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('routes status, observation, and approval resume through the owning thread', async () => {
    const { topology, hits } = harness();
    const scoped = context();
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
      decidedBy: 'reviewer-1',
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

  it('routes path-safe ids after context authorization', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    await topology.status(scoped.value, {
      agentId: 'writer',
      threadId: 'thread-1',
      runId: 'run-1',
    });
    await topology.status(scoped.value, {
      agentId: 'writer',
      threadId: 'thread-2',
      runId: 'run-2',
    });
    expect(hits.map((hit) => hit.threadId)).toEqual(['thread-1', 'thread-2']);
  });

  it('refuses status before addressing a run the context cannot read', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async () => false;

    await expect(
      topology.status(scoped.value, {
        agentId: 'writer',
        threadId: 'foreign-thread',
        runId: 'foreign-run',
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(hits).toHaveLength(0);
  });

  it('refuses observation before addressing a stream the context cannot read', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async () => false;

    await expect(
      topology.observe(scoped.value, {
        agentId: 'writer',
        threadId: 'foreign-thread',
        runId: 'foreign-run',
        offset: 0,
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(hits).toHaveLength(0);
  });

  it('recovers a dispatched run after its ephemeral thread ownership is released', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async (kind) => kind === 'run';

    await expect(
      topology.dispatchStatus(scoped.value, {
        agentId: 'writer',
        threadId: 'ephemeral-thread',
        runId: 'owned-run',
      }),
    ).resolves.toMatchObject({ summary: { status: 'success' } });
    expect(hits).toEqual([
      expect.objectContaining({
        threadId: 'ephemeral-thread',
        url: expect.stringContaining('&dispatch=1'),
      }),
    ]);
  });

  it('refuses dispatch recovery before addressing a run the context does not own', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async () => false;

    await expect(
      topology.dispatchStatus(scoped.value, {
        agentId: 'writer',
        threadId: 'foreign-thread',
        runId: 'foreign-run',
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(hits).toHaveLength(0);
  });

  it('replays a terminal run after ephemeral ownership is released', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async () => false;
    const terminate = topology.terminate;
    if (!terminate) throw new Error('terminate topology is unavailable');

    await expect(
      terminate(
        scoped.value,
        {
          agentId: 'writer',
          threadId: 'ephemeral-thread',
          runId: 'acme_run',
        },
        true,
      ),
    ).resolves.toMatchObject({ summary: { status: 'success' } });
    expect(hits).toEqual([
      expect.objectContaining({
        threadId: 'ephemeral-thread',
        url: expect.stringContaining('&replay=1'),
      }),
    ]);
  });

  it('preserves the structured disputed-settlement reason from the Thread DO', async () => {
    const { topology } = harness();
    const scoped = context();
    const terminate = topology.terminate;
    if (!terminate) throw new Error('terminate topology is unavailable');

    await expect(
      terminate(scoped.value, {
        agentId: 'writer',
        threadId: 'acme_thread',
        runId: 'disputed_run',
      }),
    ).rejects.toMatchObject({
      status: 409,
      reason: {
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      },
    });
  });
});
