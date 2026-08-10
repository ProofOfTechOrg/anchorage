// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';

import {
  type ActorContext,
  type ApprovalRecord,
  type ExecutionPrincipal,
  principalActor,
} from '../approval-api/index.js';
import { createAgentApprovalResumer } from './approval-resumer.js';
import type { AgentThreadTopology } from './thread-topology.js';

const principal: ExecutionPrincipal = {
  kind: 'human',
  id: 'operator-1',
  role: 'operator',
};

function record(overrides: Partial<ApprovalRecord> = {}): ApprovalRecord {
  return {
    id: 'approval-1',
    workflowId: 'durable-agentic-loop',
    runId: 'acme_run',
    title: 'Approve',
    connectors: ['write'],
    priority: 'normal',
    status: 'approved',
    createdAt: '2026-07-26T00:00:00.000Z',
    updatedAt: '2026-07-26T00:00:00.000Z',
    resumeTarget: {
      kind: 'agent-thread',
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      principal,
    },
    ...overrides,
  };
}

function contextFor(principal: ExecutionPrincipal): ActorContext {
  const actor = principalActor(principal);
  return {
    actor,
    principal,
    resourceOwner: { kind: principal.kind, id: principal.id },
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => 'acme_new-run',
    newThreadId: () => 'acme_new-thread',
    resourceIdFromKey: () => 'acme_resource',
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async () => true,
    canSelfDecide: () => false,
  };
}

function topology(): AgentThreadTopology {
  return {
    start: vi.fn(),
    status: vi.fn(),
    observe: vi.fn(),
    resume: vi.fn(async () => ({
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      runId: 'acme_run',
      summary: { runId: 'acme_run', status: 'success' as const },
    })),
  };
}

const agents = [
  {
    id: 'writer',
    title: 'Writer',
    description: 'Writes an approved record',
    allowedRoles: ['operator'] as const,
  },
];

describe('createAgentApprovalResumer', () => {
  it('restores the original principal, not the reviewer', async () => {
    const agentTopology = topology();
    const contextForPrincipal = vi.fn(async (actor: ExecutionPrincipal) =>
      contextFor(actor),
    );
    const fallback = vi.fn();
    const resume = createAgentApprovalResumer({
      fallback,
      agents,
      topology: agentTopology,
      contextForPrincipal,
    });
    await expect(resume(record(), 'approve')).resolves.toMatchObject({
      status: 'success',
    });
    expect(contextForPrincipal).toHaveBeenCalledWith(
      principal,
      expect.anything(),
    );
    expect(agentTopology.resume).toHaveBeenCalledWith(
      expect.objectContaining({ principal }),
      expect.objectContaining({ runId: 'acme_run' }),
      'approve',
    );
    expect(fallback).not.toHaveBeenCalled();
  });

  it('delegates generic workflow records to the fallback', async () => {
    const fallback = vi.fn(async () => ({
      runId: 'acme_workflow',
      status: 'success' as const,
    }));
    const resume = createAgentApprovalResumer({
      fallback,
      agents,
      topology: topology(),
      contextForPrincipal: async (principal) => contextFor(principal),
    });
    await expect(
      resume(
        record({
          workflowId: 'workflow',
          runId: 'acme_workflow',
          resumeTarget: { kind: 'thread', threadId: 'acme_thread' },
        }),
        'approve',
      ),
    ).resolves.toMatchObject({ runId: 'acme_workflow' });
    expect(fallback).toHaveBeenCalledOnce();
  });

  it('fails closed on legacy targets and currently disallowed principals', async () => {
    const legacy = createAgentApprovalResumer({
      fallback: vi.fn(),
      agents,
      topology: topology(),
      contextForPrincipal: async (principal) => contextFor(principal),
    });
    await expect(
      legacy(
        record({
          resumeTarget: { kind: 'thread', threadId: 'acme_thread' },
        }),
        'approve',
      ),
    ).rejects.toThrow('legacy durable-agent approvals');

    const restricted = createAgentApprovalResumer({
      fallback: vi.fn(),
      agents: [
        {
          id: 'writer',
          title: 'Writer',
          description: 'Writes an approved record',
          allowedRoles: ['admin'],
        },
      ],
      topology: topology(),
      contextForPrincipal: async (principal) => contextFor(principal),
    });
    await expect(restricted(record(), 'approve')).rejects.toThrow(
      'may no longer resume',
    );
  });

  it('rejects a context adapter that changes the stored principal', async () => {
    const resume = createAgentApprovalResumer({
      fallback: vi.fn(),
      agents,
      topology: topology(),
      contextForPrincipal: async () =>
        contextFor({ ...principal, id: 'reviewer-1', role: 'reviewer' }),
    });
    await expect(resume(record(), 'approve')).rejects.toThrow(
      'must preserve the stored agent execution principal exactly',
    );
  });
});
