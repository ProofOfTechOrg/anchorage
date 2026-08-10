// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  type ActorContext,
  type ExecutionPrincipal,
  principalActor,
} from '../approval-api/index.js';
import { EXECUTION_PRINCIPAL_HEADER } from '../do-runner/index.js';
import {
  createThreadTopology,
  type ThreadNamespaceLike,
  type ThreadRequestInit,
} from './thread-topology.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

function context(
  principal: ExecutionPrincipal = {
    kind: 'human',
    id: 'operator-1',
    role: 'operator',
  },
): ActorContext {
  return {
    actor: principalActor(principal),
    principal,
    resourceOwner: { kind: principal.kind, id: principal.id },
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => 'run-1',
    newThreadId: () => 'thread-1',
    resourceIdFromKey: (key) => key,
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async () => true,
    canSelfDecide: () => false,
  };
}

function harness() {
  const hits: Array<{
    threadId: string;
    request: Request | string;
    init?: ThreadRequestInit;
  }> = [];
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: vi.fn((name) => name),
    get: (threadId) => ({
      fetch: (async (request: Request | string, init?: ThreadRequestInit) => {
        hits.push({ threadId, request, init });
        return new Response('ok');
      }) as ReturnType<ThreadNamespaceLike<string>['get']>['fetch'],
    }),
  };
  return {
    topology: createThreadTopology(namespace, DEPLOYMENT_IDENTITY_SECRET),
    namespace,
    hits,
  };
}

describe('createThreadTopology', () => {
  it('stamps the canonical human principal and strips retired identity headers', async () => {
    const { topology, hits } = harness();

    await topology.send(context(), 'thread-1', '/messages', {
      method: 'POST',
      headers: {
        'X-Flowsafe-Tenant': 'forged',
        'x-flowsafe-actor': 'forged',
        'x-flowsafe-role': 'admin',
        [EXECUTION_PRINCIPAL_HEADER]: 'forged',
      },
    });

    const headers = new Headers(hits[0]?.init?.headers);
    expect(headers.get(EXECUTION_PRINCIPAL_HEADER)).toBe(
      '{"kind":"human","id":"operator-1","role":"operator"}',
    );
    expect(headers.has('x-flowsafe-tenant')).toBe(false);
    expect(headers.has('x-flowsafe-actor')).toBe(false);
    expect(headers.has('x-flowsafe-role')).toBe(false);
    expect(hits[0]?.threadId).toBe('thread-1');
  });

  it('preserves an automated principal without projecting it to a human', async () => {
    const { topology, hits } = harness();
    const principal: ExecutionPrincipal = {
      kind: 'service',
      id: 'scheduler',
      purpose: 'schedule.fire',
    };

    await topology.send(context(principal), 'thread-1', '/start');

    expect(
      new Headers(hits[0]?.init?.headers).get(EXECUTION_PRINCIPAL_HEADER),
    ).toBe('{"kind":"service","id":"scheduler","purpose":"schedule.fire"}');
  });

  it('refuses a malformed thread before addressing the namespace', async () => {
    const { topology, namespace } = harness();

    await expect(
      topology.send(context(), 'bad/thread', '/messages'),
    ).rejects.toMatchObject({ status: 404 });
    expect(namespace.idFromName).not.toHaveBeenCalled();
  });

  it('clones a forwarded request and overwrites its principal header', async () => {
    const { topology, hits } = harness();
    const request = new Request('https://host/stream', {
      headers: {
        [EXECUTION_PRINCIPAL_HEADER]: 'forged',
        'x-flowsafe-tenant': 'forged',
      },
    });

    await topology.forward(context(), 'thread-1', request);

    const forwarded = hits[0]?.request;
    expect(forwarded).toBeInstanceOf(Request);
    const headers = (forwarded as Request).headers;
    expect(headers.get(EXECUTION_PRINCIPAL_HEADER)).toBe(
      '{"kind":"human","id":"operator-1","role":"operator"}',
    );
    expect(headers.has('x-flowsafe-tenant')).toBe(false);
    expect(request.headers.get(EXECUTION_PRINCIPAL_HEADER)).toBe('forged');
  });
});
