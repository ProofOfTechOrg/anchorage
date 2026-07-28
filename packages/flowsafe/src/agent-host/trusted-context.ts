// SPDX-License-Identifier: Apache-2.0

import { RequestContext } from '@mastra/core/request-context';
import { AGENT_AUDIT_CONTEXT_KEY } from '@proofoftech/breakwater/audit';

import { BREAKWATER_ACTOR_KEY } from '../approval-api/index.js';
import {
  assertNoReservedExecutionContext,
  stripReservedExecutionContext,
} from '../do-runner/index.js';
import type { TrustedAgentExecution } from './types.js';

export { AGENT_AUDIT_CONTEXT_KEY };

export function rejectReservedAgentContext(
  context: Record<string, unknown>,
  label = 'agent request context',
): Record<string, unknown> {
  assertNoReservedExecutionContext(context, label);
  return { ...context };
}

export function sanitizeStoredAgentContext(
  context: Record<string, unknown> | undefined,
): Record<string, unknown> {
  return stripReservedExecutionContext(context);
}

export function deriveTrustedAgentContext(
  execution: TrustedAgentExecution,
  context?: Record<string, unknown>,
): Record<string, unknown> {
  return {
    ...stripReservedExecutionContext({
      ...execution.safeContext,
      ...context,
    }),
    runId: execution.runId,
    threadId: execution.threadId,
    resourceId: execution.resourceId,
    [BREAKWATER_ACTOR_KEY]: {
      id: execution.actor.id,
      role: execution.actor.role,
    },
    [AGENT_AUDIT_CONTEXT_KEY]: {
      agentId: execution.agentId,
      tenantId: execution.actor.tenantId,
      runId: execution.runId,
      threadId: execution.threadId,
      resourceId: execution.resourceId,
      entryPath: execution.entryPath,
    },
  };
}

export function createTrustedAgentRequestContext(
  execution: TrustedAgentExecution,
  context?: Record<string, unknown>,
): RequestContext {
  return new RequestContext(
    Object.entries(deriveTrustedAgentContext(execution, context)),
  );
}
