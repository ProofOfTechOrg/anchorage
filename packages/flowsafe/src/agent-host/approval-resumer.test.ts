// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';

import {
  type ApprovalRecord,
  type ExecutionPrincipal,
  principalActor,
  type TenantContext,
} from '../approval-api/index.js';
import { createAgentApprovalResumer } from './approval-resumer.js';
import type { AgentThreadTopology } from './thread-topology.js';

const principal: ExecutionPrincipal = {
  kind: 'human',
  id: 'operator-1',
  tenantId: 'acme',
  role: 'operator',
};

function record(overrides: Partial<ApprovalRecord> = {}): ApprovalRecord {
  return {
    id: 'approval-1',
    tenantId: 'acme',
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

function tenantFor(principal: ExecutionPrincipal): TenantContext {
  const actor = principalActor(principal);
  return {
    actor,
    principal,
    tenantId: principal.tenantId,
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => 'acme_new-run',
    ownsRun: (id) => id.startsWith('acme_'),
    newThreadId: () => 'acme_new-thread',
    newResourceId: () => 'acme_resource',
    ownsMemoryId: (id) => id.startsWith('acme_'),
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
    const tenantForPrincipal = vi.fn(async (actor: ExecutionPrincipal) =>
      tenantFor(actor),
    );
    const fallback = vi.fn();
    const resume = createAgentApprovalResumer({
      fallback,
      agents,
      topology: agentTopology,
      tenantForPrincipal,
    });
    await expect(resume(record(), 'approve')).resolves.toMatchObject({
      status: 'success',
    });
    expect(tenantForPrincipal).toHaveBeenCalledWith(
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
      tenantForPrincipal: async (principal) => tenantFor(principal),
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
      tenantForPrincipal: async (principal) => tenantFor(principal),
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
      tenantForPrincipal: async (principal) => tenantFor(principal),
    });
    await expect(restricted(record(), 'approve')).rejects.toThrow(
      'may no longer resume',
    );
  });

  it('rejects a tenant adapter that changes the stored principal', async () => {
    const resume = createAgentApprovalResumer({
      fallback: vi.fn(),
      agents,
      topology: topology(),
      tenantForPrincipal: async () =>
        tenantFor({ ...principal, id: 'reviewer-1', role: 'reviewer' }),
    });
    await expect(resume(record(), 'approve')).rejects.toThrow(
      'must preserve the stored agent execution principal exactly',
    );
  });
});
