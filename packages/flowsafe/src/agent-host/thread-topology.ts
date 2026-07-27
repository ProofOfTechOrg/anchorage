// SPDX-License-Identifier: Apache-2.0

import {
  type ApprovalDecision,
  type ApprovalRecord,
  defaultResumeData,
  type TenantContext,
} from '../approval-api/index.js';
import { PATH_SAFE_ID_PATTERN } from '../do-runner/index.js';
import {
  createThreadTopology,
  type ThreadNamespaceLike,
  type ThreadTopology,
} from '../host-kit/index.js';
import { RunRouteError } from '../host-kit/run-route-error.js';
import { sanitizeStoredAgentContext } from './trusted-context.js';
import type { AgentEntryPath, AgentRunEnvelope } from './types.js';

export const AGENT_HOST_ROUTE_PREFIX = '/_flowsafe/agent-host';

export interface AgentThreadStartInput {
  agentId: string;
  prompt: string;
  entryPath: AgentEntryPath;
  runId?: string;
  threadId?: string;
  resourceId?: string;
  threaded?: boolean;
  /**
   * Trusted stored context, used by schedule and wake adapters only.
   * The topology strips runtime-owned keys before forwarding it to the host.
   */
  requestContext?: Record<string, unknown>;
  /**
   * Trusted stream-call context layered over requestContext.
   * Public HTTP starts do not expose either context field.
   */
  streamRequestContext?: Record<string, unknown>;
}

export interface AgentThreadRunRef {
  agentId: string;
  threadId: string;
  runId: string;
}

export interface AgentThreadObserveInput extends AgentThreadRunRef {
  offset: number;
}

export interface AgentThreadTopology {
  start(
    tenant: TenantContext,
    input: AgentThreadStartInput,
  ): Promise<AgentRunEnvelope>;
  status(
    tenant: TenantContext,
    input: AgentThreadRunRef,
  ): Promise<AgentRunEnvelope | undefined>;
  observe(
    tenant: TenantContext,
    input: AgentThreadObserveInput,
  ): Promise<Response>;
  resume(
    tenant: TenantContext,
    record: ApprovalRecord,
    decision: ApprovalDecision,
  ): Promise<AgentRunEnvelope>;
}

async function errorFrom(response: Response): Promise<RunRouteError> {
  let message = `agent request failed with status ${response.status}`;
  try {
    const payload = (await response.json()) as { error?: unknown };
    if (typeof payload?.error === 'string') message = payload.error;
  } catch {
    // Keep the status-only fallback.
  }
  return new RunRouteError(response.status, message);
}

async function envelope(response: Response): Promise<AgentRunEnvelope> {
  if (!response.ok) throw await errorFrom(response);
  return (await response.json()) as AgentRunEnvelope;
}

function ownedThread(tenant: TenantContext, threadId: string): string {
  if (!PATH_SAFE_ID_PATTERN.test(threadId) || !tenant.ownsMemoryId(threadId)) {
    throw new RunRouteError(404, 'run not found');
  }
  return threadId;
}

function ownedRun(tenant: TenantContext, runId: string): string {
  if (!PATH_SAFE_ID_PATTERN.test(runId) || !tenant.ownsRun(runId)) {
    throw new RunRouteError(404, 'run not found');
  }
  return runId;
}

function expectedResource(
  tenant: TenantContext,
  threadId: string,
  supplied?: string,
): string {
  const resourceId = tenant.newResourceId(threadId);
  if (
    supplied !== undefined &&
    (supplied !== resourceId || !tenant.ownsMemoryId(supplied))
  ) {
    throw new RunRouteError(404, 'run not found');
  }
  return resourceId;
}

export function createAgentThreadTopology<Id>(
  namespace: ThreadNamespaceLike<Id>,
): AgentThreadTopology {
  const threads: ThreadTopology = createThreadTopology(namespace);
  return {
    start: async (tenant, input) => {
      const threaded = input.threaded !== false;
      if (
        !threaded &&
        (input.threadId !== undefined || input.resourceId !== undefined)
      ) {
        throw new RunRouteError(
          409,
          'unthreaded starts cannot target a pre-existing thread or resource',
        );
      }
      const threadId =
        input.threadId === undefined || !threaded
          ? tenant.newThreadId()
          : ownedThread(tenant, input.threadId);
      const resourceId = expectedResource(tenant, threadId, input.resourceId);
      const runId =
        input.runId === undefined
          ? tenant.newRunId()
          : PATH_SAFE_ID_PATTERN.test(input.runId) &&
              tenant.ownsRun(input.runId)
            ? input.runId
            : (() => {
                throw new RunRouteError(404, 'run not found');
              })();
      const safeContext = sanitizeStoredAgentContext({
        ...input.requestContext,
        ...input.streamRequestContext,
      });
      return envelope(
        await threads.send(
          tenant,
          threadId,
          `${AGENT_HOST_ROUTE_PREFIX}/start`,
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              agentId: input.agentId,
              threadId,
              resourceId,
              runId,
              prompt: input.prompt,
              entryPath: input.entryPath,
              threaded,
              safeContext,
            }),
          },
        ),
      );
    },
    status: async (tenant, input) => {
      ownedThread(tenant, input.threadId);
      ownedRun(tenant, input.runId);
      const resourceId = expectedResource(tenant, input.threadId);
      const response = await threads.send(
        tenant,
        input.threadId,
        `${AGENT_HOST_ROUTE_PREFIX}/runs/${encodeURIComponent(
          input.agentId,
        )}/${encodeURIComponent(input.runId)}?resourceId=${encodeURIComponent(
          resourceId,
        )}`,
      );
      if (response.status === 404) return undefined;
      return envelope(response);
    },
    observe: async (tenant, input) => {
      ownedThread(tenant, input.threadId);
      ownedRun(tenant, input.runId);
      const resourceId = expectedResource(tenant, input.threadId);
      const response = await threads.send(
        tenant,
        input.threadId,
        `${AGENT_HOST_ROUTE_PREFIX}/runs/${encodeURIComponent(
          input.agentId,
        )}/${encodeURIComponent(input.runId)}/stream?resourceId=${encodeURIComponent(
          resourceId,
        )}&offset=${input.offset}`,
      );
      if (!response.ok) throw await errorFrom(response);
      return response;
    },
    resume: async (tenant, record, decision) => {
      const target = record.resumeTarget;
      if (target?.kind !== 'agent-thread') {
        throw new RunRouteError(409, 'agent approval has no resumable target');
      }
      ownedThread(tenant, target.threadId);
      ownedRun(tenant, record.runId);
      expectedResource(tenant, target.threadId, target.resourceId);
      return envelope(
        await threads.send(
          tenant,
          target.threadId,
          `${AGENT_HOST_ROUTE_PREFIX}/resume`,
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              agentId: target.agentId,
              threadId: target.threadId,
              resourceId: target.resourceId,
              runId: record.runId,
              step: record.stepPath,
              resumeData: defaultResumeData(record, decision),
              entryPath: 'approval.resume',
            }),
          },
        ),
      );
    },
  };
}
