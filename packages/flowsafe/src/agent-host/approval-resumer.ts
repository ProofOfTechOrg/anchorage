// SPDX-License-Identifier: Apache-2.0

import { DURABLE_AGENTIC_LOOP_WORKFLOW_ID } from '../agent-runner/index.js';
import type {
  ApprovalActor,
  ApprovalRecord,
  TenantContext,
} from '../approval-api/index.js';
import type { ResumeRunFn } from '../host-kit/index.js';
import { createAgentCatalog } from './catalog.js';
import type { AgentThreadTopology } from './thread-topology.js';
import type { AgentMeta } from './types.js';

export interface AgentApprovalResumerOptions {
  fallback: ResumeRunFn;
  agents: readonly AgentMeta[];
  topology: AgentThreadTopology;
  tenantForActor: (
    actor: ApprovalActor,
    record: ApprovalRecord,
  ) => TenantContext | Promise<TenantContext>;
}

export type AgentApprovalResumer = ResumeRunFn;

export function createAgentApprovalResumer(
  options: AgentApprovalResumerOptions,
): AgentApprovalResumer {
  const catalog = createAgentCatalog(options.agents);
  return async (record, decision) => {
    if (record.workflowId !== DURABLE_AGENTIC_LOOP_WORKFLOW_ID) {
      if (record.resumeTarget?.kind === 'agent-thread') {
        throw new Error(
          'agent-thread resume targets are valid only for durable agent runs',
        );
      }
      return options.fallback(record, decision);
    }

    const target = record.resumeTarget;
    if (target?.kind !== 'agent-thread') {
      throw new Error(
        'legacy durable-agent approvals cannot be resumed without an agent-thread target',
      );
    }
    const meta = catalog.get(target.agentId);
    const roles = catalog.allowedRoles(target.agentId);
    if (!meta || !roles?.includes(target.principal.role)) {
      throw new Error(
        `principal role '${target.principal.role}' may no longer resume agent '${target.agentId}'`,
      );
    }
    if (
      target.principal.tenantId !== record.tenantId ||
      target.principal.id.trim() === ''
    ) {
      throw new Error(
        'agent approval principal does not match the record tenant',
      );
    }

    const tenant = await options.tenantForActor(target.principal, record);
    if (
      tenant.tenantId !== record.tenantId ||
      tenant.actor.id !== target.principal.id ||
      tenant.actor.role !== target.principal.role ||
      tenant.actor.tenantId !== target.principal.tenantId
    ) {
      throw new Error(
        'tenantForActor must preserve the stored agent execution principal exactly',
      );
    }
    const envelope = await options.topology.resume(tenant, record, decision);
    if (
      envelope.agentId !== target.agentId ||
      envelope.threadId !== target.threadId ||
      envelope.resourceId !== target.resourceId ||
      envelope.runId !== record.runId
    ) {
      throw new Error(
        'agent resume topology returned a mismatched run envelope',
      );
    }
    return envelope.summary;
  };
}
