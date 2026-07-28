// SPDX-License-Identifier: Apache-2.0

import { RequestContext } from '@mastra/core/request-context';
import { AGENT_AUDIT_CONTEXT_KEY } from '@proofoftech/breakwater/audit';

import {
  BREAKWATER_ACTOR_KEY,
  breakwaterActorFor,
  principalAuditFields,
} from '../approval-api/index.js';
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
  const { principal } = execution;
  return {
    ...stripReservedExecutionContext({
      ...execution.safeContext,
      ...context,
    }),
    runId: execution.runId,
    threadId: execution.threadId,
    resourceId: execution.resourceId,
    // `kind` is what breakwater's mandatory gate authorizes on; the projection
    // rule itself lives in breakwaterActorFor so this and the approval-facing
    // actor cannot drift apart.
    [BREAKWATER_ACTOR_KEY]: breakwaterActorFor(principal),
    [AGENT_AUDIT_CONTEXT_KEY]: {
      agentId: execution.agentId,
      tenantId: principal.tenantId,
      runId: execution.runId,
      threadId: execution.threadId,
      resourceId: execution.resourceId,
      entryPath: execution.entryPath,
      ...principalAuditFields(principal),
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
