// SPDX-License-Identifier: Apache-2.0

import { captureActorContext } from '../approval-api/actor-context.js';
import {
  type ActorContext,
  type ApprovalDecision,
  type ApprovalRecord,
  defaultResumeData,
} from '../approval-api/index.js';
import {
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';
import {
  APPROVAL_PRIORITIES,
  APPROVAL_STATUSES,
  canonicalApprovalResumeTarget,
} from '../approval-api/types.js';
import type { StartExecutionIdentity } from '../do-runner/execution-admission.js';
import {
  beginIdempotentStart,
  type ExecutionFenceWiring,
  isPathSafeId,
  type PersistedStartResult,
  requireStartIdempotency,
  resourceIdFromKey,
  type StartIdempotencyWiring,
  type StartReservation,
  type StartReservationReading,
} from '../do-runner/index.js';
import {
  captureReservation,
  type StartReservationOwner,
} from '../do-runner/start-reservation-contract.js';
import {
  doStartLiveness,
  persistedStartExecution,
  persistedStartRecord,
  publicRunSummary,
  publicStartFields,
} from '../host-kit/do-response.js';
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
  /**
   * Makes this start exactly-once for this principal: a retry carrying the same
   * key converges onto the run the first call made, on whatever thread it made
   * it, instead of starting a second one.
   *
   * TRUSTED-CALLER ONLY, and not by omission: the public agent router rejects
   * every body field but `prompt`, so a key can only arrive from a host seam —
   * a schedule adapter, a delegating agent, a host's own start path. That is
   * the same posture `requestContext` and `providerOptions` take here, and for
   * the same reason: a tenant able to name a key could converge onto, or
   * collide with, a run it did not make. (Ownership makes the collision refuse
   * rather than succeed, but a tenant-reachable key would still be a probe.)
   */
  idempotencyKey?: string;
}

/**
 * How the agent topology honours `idempotencyKey`, or the typed opt-out.
 *
 * REQUIRED on `createAgentThreadTopology` for the same reason the run router's
 * is: a topology that silently ignored a key would answer an exactly-once
 * request with at-least-once behaviour, and the caller would have no way to
 * find out. Writing `'none'` is honest — it makes every keyed start refuse with
 * IDEMPOTENT_START_UNSUPPORTED, and leaves unkeyed starts byte-identical.
 */
export interface AgentThreadTopologyOptions {
  startIdempotency: StartIdempotencyWiring;
  /**
   * The deployment execution fence, so a REPLAY can re-assert a proof-only
   * fence's binding to the run this key already made. Must be the fence over
   * the SAME database the reservations live in; `'none'` is the honest answer
   * for a host with no fence.
   */
  executionFence: ExecutionFenceWiring;
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
  options: AgentThreadTopologyOptions,
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
  /**
   * A reserved agent run's persisted envelope, read from the thread the
   * reservation recorded — which is NOT necessarily the thread this retry would
   * have minted. That indirection is the whole reason the reservation stores a
   * thread at all: an unthreaded retry mints a fresh thread every time, and
   * without the recorded one it would be asking an empty object about a run it
   * never had.
   *
   * The dispatch variant of the status route, because a run that already
   * reached a terminal state has had its run record and ownership released, and
   * the public variant answers 404 for exactly that case — which a replay must
   * not confuse with "no run".
   *
   * NO `requireResourceAccess` here, deliberately. Ownership on this path is
   * the RESERVATION's: the reserve call already refused every principal but the
   * key's owner, and the resource registry it would consult has legitimately
   * forgotten a settled run. Re-checking there would make a completed run's
   * replay a 404 — the one answer that would send a caller off to start a
   * second one.
   */
  const reservedRunEnvelope = async (
    context: ActorContext,
    agentId: string,
    reservation: StartReservation,
  ): Promise<PersistedStartResult<AgentRunEnvelope> | undefined> => {
    const threadId = reservation.threadId;
    if (threadId === undefined || !isPathSafeId(threadId))
      throw new RunRouteError(503, 'persisted start is not readable');
    const response = await threads.send(
      context,
      threadId,
      `${AGENT_HOST_ROUTE_PREFIX}/runs/${encodeURIComponent(
        agentId,
      )}/${encodeURIComponent(
        reservation.runId,
      )}?resourceId=${encodeURIComponent(
        context.resourceIdFromKey(threadId),
      )}&dispatch=1&replay=1`,
    );
    if (response.status === 404) return undefined;
    return persistedEnvelope(response, {
      agentId,
      threadId,
      resourceId: context.resourceIdFromKey(threadId),
      runId: reservation.runId,
      owner: reservation.owner,
    });
  };
  const reservedRunLive = async (
    context: ActorContext,
    agentId: string,
    reservation: StartReservation,
  ): Promise<boolean> => {
    const threadId = reservation.threadId;
    if (threadId === undefined || !isPathSafeId(threadId)) return false;
    const response = await threads.send(
      context,
      threadId,
      `${AGENT_HOST_ROUTE_PREFIX}/runs/${encodeURIComponent(
        agentId,
      )}/${encodeURIComponent(reservation.runId)}/start-liveness`,
    );
    return doStartLiveness(response);
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
    start: async (sourceContext, sourceInput) => {
      const context = captureActorContext(sourceContext);
      const { principal, mutationEpoch } = context;
      const {
        agentId,
        entryPath,
        runId: suppliedRunId,
        threadId: suppliedThreadId,
        resourceId: suppliedResourceId,
        topologyThreadId,
        threaded: suppliedThreaded,
        scheduleId,
        dispatchId,
        idempotencyKey,
        prompt,
        requestContext,
        streamRequestContext,
        providerOptions,
      } = sourceInput;
      const input = {
        agentId,
        entryPath,
        threadId: suppliedThreadId,
        resourceId: suppliedResourceId,
        topologyThreadId,
        scheduleId,
        dispatchId,
        idempotencyKey,
        prompt,
        requestContext,
        streamRequestContext,
        providerOptions,
      };
      const threaded = suppliedThreaded !== false;
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
      // The mint, as a THUNK. Two things depend on the delay: the reservation
      // only spends an id when it wins the insert, and — for the
      // caller-influenced `input.runId` path this closes the gap on — a retry
      // that loses the insert takes the WINNER's id rather than re-proposing
      // its own, which is what makes two same-key starts converge instead of
      // becoming two runs.
      const mintRunId = (): string =>
        suppliedRunId === undefined
          ? context.newRunId()
          : isPathSafeId(suppliedRunId)
            ? suppliedRunId
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
      const sendStart = async (
        targetThreadId: string,
        targetRunId: string,
        requestIdempotencyKey?: string,
        suppliedReservation?: StartReservationReading,
      ): Promise<AgentRunEnvelope> => {
        const startReservation =
          suppliedReservation === undefined
            ? undefined
            : captureReservation(suppliedReservation, 'started');
        return envelope(
          await threads.send(
            { principal, mutationEpoch },
            targetThreadId,
            `${AGENT_HOST_ROUTE_PREFIX}/start`,
            {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({
                agentId: input.agentId,
                threadId: targetThreadId,
                resourceId: context.resourceIdFromKey(targetThreadId),
                runId: targetRunId,
                prompt: input.prompt,
                entryPath: input.entryPath,
                threaded,
                safeContext,
                providerOptions: input.providerOptions,
                // The key rides the internal Worker-to-DO channel only, so the
                // fence's proof-only state can match it inside the runtime.
                ...(requestIdempotencyKey === undefined
                  ? {}
                  : { idempotencyKey: requestIdempotencyKey }),
                ...(startReservation === undefined ? {} : { startReservation }),
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
      };
      if (input.idempotencyKey === undefined) {
        // Unkeyed starts take the path they always took, on the thread this
        // call resolved.
        return sendStart(threadId, mintRunId());
      }
      const store = requireStartIdempotency(options.startIdempotency);
      const decision = await beginIdempotentStart<AgentRunEnvelope>(
        store,
        {
          key: input.idempotencyKey,
          owner: {
            kind: principal.kind,
            id: principal.id,
          },
          targetKind: 'agent',
          targetId: input.agentId,
          threadId,
          mintRunId,
        },
        {
          persisted: (persistedReservation) =>
            reservedRunEnvelope(context, input.agentId, persistedReservation),
          live: (liveReservation) =>
            reservedRunLive(context, input.agentId, liveReservation),
        },
        options.executionFence,
        mutationEpoch,
      );
      if (decision.kind === 'replay') return decision.persisted;
      // The RESERVATION's thread and run, not this call's: on a re-claim of a
      // reservation an earlier crashed caller left behind, the recorded thread
      // is where that run belongs and this call's freshly minted one is not.
      const { reservation } = decision;
      const startThreadId = reservation.threadId;
      if (!isPathSafeId(startThreadId))
        throw new RunRouteError(503, 'persisted start is not readable');
      return sendStart(
        startThreadId,
        reservation.runId,
        reservation.key,
        reservation,
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

function publicApproval(
  value: unknown,
  execution: StartExecutionIdentity,
): ApprovalRecord {
  const record = publicStartFields(value, [
    'id',
    'workflowId',
    'runId',
    'stepPath',
    'title',
    'summary',
    'payload',
    'connectors',
    'grantScope',
    'toolCallId',
    'priority',
    'status',
    'requestedBy',
    'requestedByKind',
    'claimedBy',
    'decidedBy',
    'decision',
    'comment',
    'delegatedTo',
    'createdAt',
    'updatedAt',
    'claimedAt',
    'decidedAt',
    'escalatedAt',
    'slaDeadlineAt',
    'suspendedAt',
    'resumedAt',
    'resumeCount',
    'runScoped',
    'resumeTarget',
  ]);
  const invalid = (): never => {
    throw new RunRouteError(503, 'persisted start is not readable');
  };
  if (
    !isPathSafeId(record.id) ||
    record.workflowId !== execution.workflowId ||
    record.runId !== execution.runId ||
    typeof record.title !== 'string' ||
    typeof record.createdAt !== 'string' ||
    typeof record.updatedAt !== 'string' ||
    !(APPROVAL_PRIORITIES as readonly unknown[]).includes(record.priority) ||
    !(APPROVAL_STATUSES as readonly unknown[]).includes(record.status)
  )
    invalid();
  for (const field of ['connectors', 'stepPath']) {
    if (
      (field === 'connectors' || record[field] !== undefined) &&
      (!Array.isArray(record[field]) ||
        (record[field] as unknown[]).some((part) => typeof part !== 'string'))
    )
      invalid();
  }
  for (const field of [
    'summary',
    'toolCallId',
    'claimedBy',
    'decidedBy',
    'comment',
    'delegatedTo',
    'claimedAt',
    'decidedAt',
    'escalatedAt',
    'slaDeadlineAt',
  ]) {
    if (record[field] !== undefined && typeof record[field] !== 'string')
      invalid();
  }
  if (
    record.requestedBy !== undefined &&
    !isExecutionPrincipalId(record.requestedBy)
  )
    invalid();
  if (
    record.requestedByKind !== undefined &&
    (!isExecutionPrincipalKind(record.requestedByKind) ||
      record.requestedBy === undefined)
  )
    invalid();
  if (
    record.grantScope !== undefined &&
    !['tool-call', 'suspension', 'run'].includes(record.grantScope as string)
  )
    invalid();
  if (
    record.decision !== undefined &&
    record.decision !== 'approve' &&
    record.decision !== 'reject'
  )
    invalid();
  if (record.runScoped !== undefined && typeof record.runScoped !== 'boolean')
    invalid();
  for (const field of ['suspendedAt', 'resumedAt', 'resumeCount']) {
    const fieldValue = record[field];
    if (
      fieldValue !== undefined &&
      (typeof fieldValue !== 'number' ||
        !Number.isFinite(fieldValue) ||
        (field === 'resumeCount' &&
          (!Number.isSafeInteger(fieldValue) || fieldValue < 0)))
    )
      invalid();
  }
  if (record.resumeTarget !== undefined) {
    const target = canonicalApprovalResumeTarget(record.resumeTarget);
    if (!target) invalid();
    if (
      target?.kind === 'agent-thread' &&
      (execution.target.kind !== 'agent' ||
        target.agentId !== execution.target.id ||
        target.threadId !== execution.target.threadId ||
        target.resourceId !== resourceIdFromKey(target.threadId))
    )
      invalid();
    record.resumeTarget = target;
  }
  return record as unknown as ApprovalRecord;
}

/** @internal Project the private replay envelope without execution authority. */
export function publicAgentRunEnvelope(
  value: unknown,
  execution: StartExecutionIdentity,
  expected: {
    agentId: string;
    threadId: string;
    resourceId: string;
    runId: string;
  },
): AgentRunEnvelope {
  const source = publicStartFields(value, [
    'agentId',
    'threadId',
    'resourceId',
    'runId',
    'summary',
    'approval',
    'approvals',
  ]);
  if (
    source.agentId !== expected.agentId ||
    source.threadId !== expected.threadId ||
    source.resourceId !== expected.resourceId ||
    source.runId !== expected.runId
  )
    throw new RunRouteError(503, 'persisted start is not readable');
  const result: AgentRunEnvelope = {
    agentId: expected.agentId,
    threadId: expected.threadId,
    resourceId: expected.resourceId,
    runId: expected.runId,
    summary: publicRunSummary(source.summary, expected.runId),
  };
  if (source.approval !== undefined)
    result.approval = publicApproval(source.approval, execution);
  if (source.approvals !== undefined) {
    if (!Array.isArray(source.approvals))
      throw new RunRouteError(503, 'persisted start is not readable');
    result.approvals = source.approvals.map((approval) =>
      publicApproval(approval, execution),
    );
  }
  return result;
}

async function persistedEnvelope(
  response: Response,
  expected: {
    agentId: string;
    threadId: string;
    resourceId: string;
    runId: string;
    owner: StartReservationOwner;
  },
): Promise<PersistedStartResult<AgentRunEnvelope>> {
  if (!response.ok) throw await errorFrom(response);
  try {
    const payload = persistedStartRecord(await response.json());
    const execution = persistedStartExecution(payload.execution);
    if (
      execution.runId !== expected.runId ||
      execution.owner.kind !== expected.owner.kind ||
      execution.owner.id !== expected.owner.id ||
      execution.target.kind !== 'agent' ||
      execution.target.id !== expected.agentId ||
      execution.target.threadId !== expected.threadId
    )
      throw new Error('selector mismatch');
    if (payload.kind === 'initial' && !Object.hasOwn(payload, 'value'))
      return { kind: 'initial', execution };
    if (payload.kind !== 'result' || !Object.hasOwn(payload, 'value'))
      throw new Error('invalid result');
    return {
      kind: 'result',
      execution,
      value: publicAgentRunEnvelope(payload.value, execution, expected),
    };
  } catch {
    throw new RunRouteError(503, 'persisted start is not readable');
  }
}
