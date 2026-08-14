// SPDX-License-Identifier: Apache-2.0

import {
  type ActorContext,
  type ApprovalDecision,
  type ApprovalRecord,
  defaultResumeData,
} from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/index.js';
import {
  type BoundThreadTarget,
  type BoundThreadTargetValidator,
  createThreadTopology,
  requireResourceAccess,
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
  /** Trusted host-only DO address for an ephemeral unthreaded execution. */
  topologyThreadId?: string;
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
  /** Trusted, JSON-safe model provider options from an internal schedule. */
  providerOptions?: Record<string, unknown>;
  /** Required provenance for a schedule.fire target. */
  scheduleId?: string;
  /** Prepared schedule trigger authorizing this exact run. */
  dispatchId?: string;
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
    context: ActorContext,
    input: AgentThreadStartInput,
  ): Promise<AgentRunEnvelope>;
  status(
    context: ActorContext,
    input: AgentThreadRunRef,
  ): Promise<AgentRunEnvelope | undefined>;
  observe(
    context: ActorContext,
    input: AgentThreadObserveInput,
  ): Promise<Response>;
  terminate?(
    context: ActorContext,
    input: AgentThreadRunRef,
    replayOnly?: boolean,
  ): Promise<AgentRunEnvelope>;
  resume(
    context: ActorContext,
    record: ApprovalRecord,
    decision: ApprovalDecision,
  ): Promise<AgentRunEnvelope>;
}

/** Agent topology plus target-side proof for standing memory state. */
export interface AgentThreadBoundTopology extends AgentThreadTopology {
  requireBoundThread: BoundThreadTargetValidator;
}

/**
 * Host-only dispatch recovery. Unlike public status, the ephemeral thread and
 * resource may already have been released after a terminal unthreaded run, so
 * this seam authorizes on the durable run owner before the target host proves
 * the addressed thread/resource/run correlation from its snapshot.
 */
export interface AgentThreadDispatchTopology extends AgentThreadBoundTopology {
  dispatchStatus(
    context: ActorContext,
    input: AgentThreadRunRef,
  ): Promise<AgentRunEnvelope | undefined>;
}

async function errorFrom(response: Response): Promise<RunRouteError> {
  let message = `agent request failed with status ${response.status}`;
  let reason: unknown;
  try {
    const payload = (await response.json()) as {
      error?: unknown;
      reason?: unknown;
    };
    if (typeof payload?.error === 'string') message = payload.error;
    reason = payload?.reason;
  } catch {
    // Keep the status-only fallback.
  }
  return new RunRouteError(response.status, message, reason);
}

async function envelope(response: Response): Promise<AgentRunEnvelope> {
  if (!response.ok) throw await errorFrom(response);
  return (await response.json()) as AgentRunEnvelope;
}

function validThread(threadId: string): string {
  if (!isPathSafeId(threadId)) {
    throw new RunRouteError(404, 'run not found');
  }
  return threadId;
}

function validRun(runId: string): string {
  if (!isPathSafeId(runId)) {
    throw new RunRouteError(404, 'run not found');
  }
  return runId;
}

function expectedResource(
  context: ActorContext,
  threadId: string,
  supplied?: string,
): string {
  const resourceId = context.resourceIdFromKey(threadId);
  if (
    supplied !== undefined &&
    (supplied !== resourceId || !isPathSafeId(supplied))
  ) {
    throw new RunRouteError(404, 'run not found');
  }
  return resourceId;
}

export function createAgentThreadTopology<Id>(
  namespace: ThreadNamespaceLike<Id>,
  deploymentIdentitySecret: string,
): AgentThreadDispatchTopology {
  const threads: ThreadTopology = createThreadTopology(
    namespace,
    deploymentIdentitySecret,
  );
  const fetchStatus = async (
    context: ActorContext,
    input: AgentThreadRunRef,
    resourceId: string,
    dispatch: boolean,
  ): Promise<AgentRunEnvelope | undefined> => {
    const response = await threads.send(
      context,
      input.threadId,
      `${AGENT_HOST_ROUTE_PREFIX}/runs/${encodeURIComponent(
        input.agentId,
      )}/${encodeURIComponent(input.runId)}?resourceId=${encodeURIComponent(
        resourceId,
      )}${dispatch ? '&dispatch=1' : ''}`,
    );
    if (response.status === 404) return undefined;
    return envelope(response);
  };
  const statusFromHost = async (
    context: ActorContext,
    input: AgentThreadRunRef,
  ): Promise<AgentRunEnvelope | undefined> => {
    validThread(input.threadId);
    validRun(input.runId);
    const resourceId = expectedResource(context, input.threadId);
    await requireResourceAccess(
      context,
      'thread',
      input.threadId,
      'read',
      'run',
    );
    await requireResourceAccess(context, 'resource', resourceId, 'read', 'run');
    await requireResourceAccess(context, 'run', input.runId, 'read', 'run');
    return fetchStatus(context, input, resourceId, false);
  };
  const dispatchStatusFromHost = async (
    context: ActorContext,
    input: AgentThreadRunRef,
  ): Promise<AgentRunEnvelope | undefined> => {
    validThread(input.threadId);
    validRun(input.runId);
    await requireResourceAccess(context, 'run', input.runId, 'read', 'run');
    const resourceId = expectedResource(context, input.threadId);
    return fetchStatus(context, input, resourceId, true);
  };
  return {
    requireBoundThread: async (context, target: BoundThreadTarget) => {
      const threadId = validThread(target.threadId);
      const resourceId = expectedResource(context, threadId, target.resourceId);
      await requireResourceAccess(
        context,
        'thread',
        threadId,
        'write',
        'thread',
      );
      await requireResourceAccess(
        context,
        'resource',
        resourceId,
        'write',
        'resource',
      );
      const response = await threads.send(
        context,
        threadId,
        `${AGENT_HOST_ROUTE_PREFIX}/binding?resourceId=${encodeURIComponent(
          resourceId,
        )}${
          target.agentId === undefined
            ? ''
            : `&agentId=${encodeURIComponent(target.agentId)}`
        }`,
        { method: 'GET' },
      );
      if (!response.ok) throw await errorFrom(response);
    },
    start: async (context, input) => {
      const threaded = input.threaded !== false;
      if (
        (input.entryPath === 'schedule.fire') !==
          (input.scheduleId !== undefined) ||
        (input.entryPath === 'schedule.fire') !==
          (input.dispatchId !== undefined)
      ) {
        throw new RunRouteError(404, 'run not found');
      }
      if (input.scheduleId !== undefined) validRun(input.scheduleId);
      if (input.dispatchId !== undefined) validRun(input.dispatchId);
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
        !threaded && input.topologyThreadId !== undefined
          ? validThread(input.topologyThreadId)
          : input.threadId === undefined || !threaded
            ? context.newThreadId()
            : validThread(input.threadId);
      const resourceId = expectedResource(context, threadId, input.resourceId);
      const runId =
        input.runId === undefined
          ? context.newRunId()
          : isPathSafeId(input.runId)
            ? input.runId
            : (() => {
                throw new RunRouteError(404, 'run not found');
              })();
      if (input.threadId !== undefined && threaded) {
        await requireResourceAccess(
          context,
          'thread',
          threadId,
          'write',
          'run',
        );
        await requireResourceAccess(
          context,
          'resource',
          resourceId,
          'write',
          'run',
        );
      }
      const safeContext = sanitizeStoredAgentContext({
        ...input.requestContext,
        ...input.streamRequestContext,
      });
      return envelope(
        await threads.send(
          context,
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
              providerOptions: input.providerOptions,
              ...(input.scheduleId !== undefined
                ? { scheduleId: input.scheduleId }
                : {}),
              ...(input.dispatchId !== undefined
                ? { dispatchId: input.dispatchId }
                : {}),
            }),
          },
        ),
      );
    },
    status: statusFromHost,
    dispatchStatus: dispatchStatusFromHost,
    observe: async (context, input) => {
      validThread(input.threadId);
      validRun(input.runId);
      const resourceId = expectedResource(context, input.threadId);
      await requireResourceAccess(
        context,
        'thread',
        input.threadId,
        'read',
        'run',
      );
      await requireResourceAccess(
        context,
        'resource',
        resourceId,
        'read',
        'run',
      );
      await requireResourceAccess(context, 'run', input.runId, 'read', 'run');
      const response = await threads.send(
        context,
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
    terminate: async (context, input, replayOnly = false) => {
      validThread(input.threadId);
      validRun(input.runId);
      const resourceId = expectedResource(context, input.threadId);
      if (!replayOnly) {
        await requireResourceAccess(
          context,
          'thread',
          input.threadId,
          'write',
          'run',
        );
        await requireResourceAccess(
          context,
          'resource',
          resourceId,
          'write',
          'run',
        );
      }
      const response = await threads.send(
        context,
        input.threadId,
        `${AGENT_HOST_ROUTE_PREFIX}/runs/${encodeURIComponent(
          input.agentId,
        )}/${encodeURIComponent(input.runId)}/terminate?resourceId=${encodeURIComponent(
          resourceId,
        )}${replayOnly ? '&replay=1' : ''}`,
        { method: 'POST' },
      );
      return envelope(response);
    },
    resume: async (context, record, decision) => {
      const target = record.resumeTarget;
      if (target?.kind !== 'agent-thread') {
        throw new RunRouteError(409, 'agent approval has no resumable target');
      }
      validThread(target.threadId);
      validRun(record.runId);
      expectedResource(context, target.threadId, target.resourceId);
      await requireResourceAccess(
        context,
        'thread',
        target.threadId,
        'write',
        'run',
      );
      await requireResourceAccess(
        context,
        'resource',
        target.resourceId,
        'write',
        'run',
      );
      await requireResourceAccess(context, 'run', record.runId, 'write', 'run');
      if (!record.decidedBy) {
        throw new RunRouteError(409, 'approval has no decision actor');
      }
      return envelope(
        await threads.send(
          context,
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
              requestedBy: record.decidedBy,
            }),
          },
        ),
      );
    },
  };
}
