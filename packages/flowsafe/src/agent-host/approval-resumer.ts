// SPDX-License-Identifier: Apache-2.0

import { DURABLE_AGENTIC_LOOP_WORKFLOW_ID } from '../agent-runner/index.js';
import {
  type ApprovalRecord,
  type ExecutionPrincipal,
  samePrincipal,
  type TenantContext,
} from '../approval-api/index.js';
import type { ResumeRunFn } from '../host-kit/index.js';
import { createAgentCatalog } from './catalog.js';
import type { AgentThreadTopology } from './thread-topology.js';
import type { AgentMeta } from './types.js';

export interface AgentApprovalResumerOptions {
  fallback: ResumeRunFn;
  agents: readonly AgentMeta[];
  topology: AgentThreadTopology;
  /**
   * Builds the tenant context the resume runs under, from the STORED principal.
   * It must return that principal unchanged; createAgentApprovalResumer
   * enforces that with an exact comparison after calling this.
   */
  tenantForPrincipal: (
    principal: ExecutionPrincipal,
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
    const principal = target.principal;
    // Re-authorize the STORED principal against the CURRENT catalog: a decision
    // taken yesterday must not resume an agent whose policy has since narrowed.
    // Split by kind for the same reason the entry gate is: a human resumes
    // under the role rules, automation under its declared entry paths, and
    // neither may answer for the other. Note this authorizes the principal that
    // STARTED the run, never the reviewer who approved it — a human decision
    // does not transfer that human's authority into the resumed leg.
    const authorized =
      meta !== undefined &&
      (principal.kind === 'human'
        ? catalog.allowedRoles(target.agentId)?.includes(principal.role) ===
          true
        : catalog.automationAllowed(
            target.agentId,
            principal,
            'approval.resume',
          ));
    if (!authorized) {
      throw new Error(
        `principal kind '${principal.kind}' may no longer resume agent '${target.agentId}'`,
      );
    }
    if (principal.tenantId !== record.tenantId || principal.id.trim() === '') {
      throw new Error(
        'agent approval principal does not match the record tenant',
      );
    }

    const tenant = await options.tenantForPrincipal(principal, record);
    if (
      tenant.tenantId !== record.tenantId ||
      !samePrincipal(tenant.principal, principal)
    ) {
      throw new Error(
        'tenantForPrincipal must preserve the stored agent execution principal exactly',
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
