// SPDX-License-Identifier: Apache-2.0

import { RequestContext } from '@mastra/core/request-context';
import { AGENT_AUDIT_CONTEXT_KEY } from '@proofoftech/breakwater/audit';

import {
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_PRINCIPAL_PERMISSIONS_KEY,
  breakwaterActorFor,
  principalAuditFields,
} from '../approval-api/index.js';
import { stripReservedExecutionContext } from '../do-runner/index.js';
import type { TrustedAgentExecution } from './types.js';

export { AGENT_AUDIT_CONTEXT_KEY };

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
    // Always present, even as null: a resume leg merges over the persisted
    // start-time context, so an OMITTED key would let a projection minted
    // under an older policy snapshot survive the merge. An explicit null
    // overwrites it, and breakwater's connector gate fails closed on null.
    [BREAKWATER_PRINCIPAL_PERMISSIONS_KEY]:
      execution.principalPermissions ?? null,
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
