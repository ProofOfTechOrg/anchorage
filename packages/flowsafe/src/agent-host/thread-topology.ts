// SPDX-License-Identifier: Apache-2.0

import { captureActorContext } from '../approval-api/actor-context.js';
import {
  type ActorContext,
  type ApprovalDecision,
  type ApprovalRecord,
  defaultResumeData,
  expectedSuspensionFor,
} from '../approval-api/index.js';
import {
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
  samePrincipal,
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
  RunNotSuspendedError,
  requireStartIdempotency,
  resourceIdFromKey,
  type StartIdempotencyWiring,
  type StartReservation,
  type StartReservationReading,
} from '../do-runner/index.js';
import type {
  RecordedRemoteStepResult,
  RemoteStepInvocation,
  RemoteStepJournal,
  RemoteStepPermit,
  WorkflowRemoteStepInvocation,
} from '../do-runner/remote-step-journal.js';
import {
  isTerminalRunStatus,
  type RunStatus,
} from '../do-runner/run-terminal-state.js';
import type { RunSummary } from '../do-runner/runtime.js';
import {
  captureReservation,
  type StartReservationOwner,
} from '../do-runner/start-reservation-contract.js';
import {
  doStartLiveness,
  objectAnswerError,
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
  remoteSteps?: RemoteStepContinuationReader;
  remoteStepDispatch?: RemoteStepDispatchReader;
  workflowRemoteStepDispatch?: WorkflowRemoteStepDispatchReader;
}

export type WorkflowRemoteStepDispatchReader = Pick<
  RemoteStepJournal,
  | 'deploymentId'
  | 'tablePrefix'
  | 'registerWorkflow'
  | 'getWorkflow'
  | 'readWorkflowDispatchRequestJson'
  | 'issuePermit'
  | 'consumePermit'
  | 'recordResult'
>;

export interface WorkflowRemoteStepDispatchTicket {
  invocationId: string;
  deploymentId: string;
  workflowId: string;
  runId: string;
  stepPath: readonly string[];
  suspendedAt: number;
  resumeCount?: number;
  deadlineAt: number;
  requestJson: string;
  requestDigest: string;
  permit: RemoteStepPermit;
}

export interface RemoteStepDispatchReader {
  deploymentId: string;
  tablePrefix: string;
  get(invocationId: string): Promise<RemoteStepInvocation | undefined>;
  readDispatchRequestJson(invocationId: string): Promise<string>;
  issuePermit(
    invocationId: string,
    lifetimeMs: number,
  ): Promise<RemoteStepPermit>;
  consumePermit(
    permit: RemoteStepPermit,
  ): Promise<
    { kind: 'accepted'; rawPermit: string } | { kind: 'already-consumed' }
  >;
}

export interface RemoteStepDispatchTicket {
  invocationId: string;
  deploymentId: string;
  workflowId: string;
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
  stepPath: readonly string[];
  suspendedAt: number;
  resumeCount?: number;
  deadlineAt: number;
  requestJson: string;
  permit: RemoteStepPermit;
}

export interface RemoteStepContinuationReader {
  deploymentId: string;
  tablePrefix: string;
  get(invocationId: string): Promise<RemoteStepInvocation | undefined>;
  readRetainedResult(
    invocationId: string,
  ): Promise<RecordedRemoteStepResult | undefined>;
  claimResume(invocationId: string, leaseMs: number): Promise<string>;
  settleAfterResume(
    invocationId: string,
    leaseOwner: string,
    witnessed?: RunSummary,
  ): Promise<
    | 'retry'
    | 'applied'
    | 'terminal-not-success'
    | 'deadline-expired'
    | 'advanced-without-proof'
  >;
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
  /**
   * Resumes a decided agent approval's run through its thread object. An
   * implementation sends `expectedSuspensionFor(record)` with the resume, so a
   * decision for an earlier suspension of the step is refused; without it the
   * resume is unchecked. A run that has ended rejects with a 409 whose
   * `reason.code` is `RUN_NOT_SUSPENDED` when the context's principal started
   * it, and with a 404 `RunRouteError` otherwise.
   */
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
  resumeRemoteStep(
    context: ActorContext,
    invocationId: string,
  ): Promise<AgentRunEnvelope>;
  issueRemoteStepDispatch(
    context: ActorContext,
    invocationId: string,
    lifetimeMs: number,
  ): Promise<RemoteStepDispatchTicket>;
  consumeRemoteStepDispatch(
    context: ActorContext,
    permit: RemoteStepPermit,
  ): Promise<
    { kind: 'accepted'; rawPermit: string } | { kind: 'already-consumed' }
  >;
  registerWorkflowRemoteStep(
    context: ActorContext,
    workflowId: string,
    runId: string,
    hostDeadlineAt: number,
  ): Promise<WorkflowRemoteStepInvocation>;
  getWorkflowRemoteStep(
    context: ActorContext,
    invocationId: string,
  ): Promise<WorkflowRemoteStepInvocation | undefined>;
  issueWorkflowRemoteStepDispatch(
    context: ActorContext,
    invocationId: string,
    lifetimeMs: number,
  ): Promise<WorkflowRemoteStepDispatchTicket>;
  consumeWorkflowRemoteStepDispatch(
    context: ActorContext,
    permit: RemoteStepPermit,
  ): Promise<
    { kind: 'accepted'; rawPermit: string } | { kind: 'already-consumed' }
  >;
  recordWorkflowRemoteStepResult(
    context: ActorContext,
    invocationId: string,
    rawPermit: string,
    resultJson: string,
    evidenceJson: string,
  ): ReturnType<RemoteStepJournal['recordResult']>;
}

function errorFrom(response: Response): Promise<RunRouteError> {
  return objectAnswerError('thread-object', response, 'agent');
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
  const dispatchInvocation = async (
    context: ActorContext,
    invocationId: string,
  ) => {
    const reader = options.remoteStepDispatch;
    if (!reader) throw new RunRouteError(503, 'remote steps are unavailable');
    if (!isPathSafeId(invocationId))
      throw new RunRouteError(404, 'invocation not found');
    const invocation = await reader.get(invocationId);
    if (
      !invocation ||
      invocation.deploymentId !== reader.deploymentId ||
      invocation.tablePrefix !== reader.tablePrefix ||
      !samePrincipal(invocation.principal, context.principal)
    )
      throw new RunRouteError(404, 'invocation not found');
    expectedResource(context, invocation.threadId, invocation.resourceId);
    await requireResourceAccess(
      context,
      'thread',
      invocation.threadId,
      'write',
      'run',
    );
    await requireResourceAccess(
      context,
      'resource',
      invocation.resourceId,
      'write',
      'run',
    );
    await requireResourceAccess(
      context,
      'run',
      invocation.runId,
      'write',
      'run',
    );
    return { reader, invocation };
  };
  const workflowDispatchInvocation = async (
    context: ActorContext,
    invocationId: string,
    access: 'read' | 'write',
  ) => {
    const reader = options.workflowRemoteStepDispatch;
    if (!reader) throw new RunRouteError(503, 'remote steps are unavailable');
    if (!isPathSafeId(invocationId))
      throw new RunRouteError(404, 'invocation not found');
    const invocation = await reader.getWorkflow(invocationId);
    if (
      invocation?.targetKind !== 'workflow' ||
      invocation.invocationId !== invocationId ||
      invocation.deploymentId !== reader.deploymentId ||
      invocation.tablePrefix !== reader.tablePrefix ||
      !samePrincipal(invocation.principal, context.principal)
    )
      throw new RunRouteError(404, 'invocation not found');
    await requireResourceAccess(
      context,
      'run',
      invocation.runId,
      access,
      'run',
    );
    return { reader, invocation };
  };
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
   * The owner-checked replay read: the thread object answers it only for the
   * principal that started the run, and without the claim registry.
   *
   * The dispatch variant of the status route, because a run that already
   * reached a terminal state has had its run record and ownership released, and
   * the public variant answers 404 for exactly that case — which a replay must
   * not confuse with "no run".
   */
  const replayRead = async (
    context: ActorContext,
    agentId: string,
    threadId: string,
    runId: string,
    owner: StartReservationOwner,
  ): Promise<PersistedStartResult<AgentRunEnvelope> | undefined> => {
    const resourceId = context.resourceIdFromKey(threadId);
    const response = await threads.send(
      context,
      threadId,
      `${AGENT_HOST_ROUTE_PREFIX}/runs/${encodeURIComponent(
        agentId,
      )}/${encodeURIComponent(runId)}?resourceId=${encodeURIComponent(
        resourceId,
      )}&dispatch=1&replay=1`,
    );
    if (response.status === 404) return undefined;
    return persistedEnvelope(response, {
      agentId,
      threadId,
      resourceId,
      runId,
      owner,
    });
  };
  /**
   * A reserved agent run's persisted envelope, read from the thread the
   * reservation recorded — which is NOT necessarily the thread this retry would
   * have minted. That indirection is the whole reason the reservation stores a
   * thread at all: an unthreaded retry mints a fresh thread every time, and
   * without the recorded one it would be asking an empty object about a run it
   * never had.
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
    return replayRead(
      context,
      agentId,
      threadId,
      reservation.runId,
      reservation.owner,
    );
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
  /**
   * The workflow and status of an ended run the context's principal started,
   * read once the run's claims are released: an unthreaded run releases its
   * thread and resource when it ends, and cancellation and timeout cleanup
   * release its run claim. Every other outcome of the read, a failure
   * included, is undefined, so the caller keeps the 404 it already has.
   */
  const endedRun = async (
    context: ActorContext,
    agentId: string,
    threadId: string,
    runId: string,
  ): Promise<{ workflowId: string; status: RunStatus } | undefined> => {
    try {
      const replayed = await replayRead(context, agentId, threadId, runId, {
        kind: context.principal.kind,
        id: context.principal.id,
      });
      if (replayed?.kind !== 'result') return undefined;
      const { status } = replayed.value.summary;
      return isTerminalRunStatus(status)
        ? { workflowId: replayed.execution.workflowId, status }
        : undefined;
    } catch {
      return undefined;
    }
  };
  return {
    registerWorkflowRemoteStep: async (
      context,
      workflowId,
      runId,
      hostDeadlineAt,
    ) => {
      const reader = options.workflowRemoteStepDispatch;
      if (!reader) throw new RunRouteError(503, 'remote steps are unavailable');
      if (!isPathSafeId(workflowId))
        throw new RunRouteError(404, 'workflow not found');
      await requireResourceAccess(context, 'run', runId, 'write', 'run');
      return reader.registerWorkflow(
        {
          targetKind: 'workflow',
          deploymentId: reader.deploymentId,
          tablePrefix: reader.tablePrefix,
          workflowId,
          runId,
        },
        hostDeadlineAt,
        context.principal,
      );
    },
    getWorkflowRemoteStep: async (context, invocationId) => {
      const reader = options.workflowRemoteStepDispatch;
      if (!reader) throw new RunRouteError(503, 'remote steps are unavailable');
      if (!isPathSafeId(invocationId))
        throw new RunRouteError(404, 'invocation not found');
      const invocation = await reader.getWorkflow(invocationId);
      if (!invocation) return undefined;
      return (await workflowDispatchInvocation(context, invocationId, 'read'))
        .invocation;
    },
    issueWorkflowRemoteStepDispatch: async (
      context,
      invocationId,
      lifetimeMs,
    ) => {
      const { reader } = await workflowDispatchInvocation(
        context,
        invocationId,
        'write',
      );
      const permit = await reader.issuePermit(invocationId, lifetimeMs);
      const { invocation } = await workflowDispatchInvocation(
        context,
        invocationId,
        'write',
      );
      if (
        invocation.status !== 'dispatched' ||
        invocation.permitGeneration !== permit.generation ||
        invocation.deadlineAt < permit.expiresAt
      )
        throw new RunRouteError(409, 'remote step dispatch changed');
      const requestJson =
        await reader.readWorkflowDispatchRequestJson(invocationId);
      return {
        invocationId,
        deploymentId: invocation.deploymentId,
        workflowId: invocation.workflowId,
        runId: invocation.runId,
        stepPath: invocation.stepPath,
        suspendedAt: invocation.suspendedAt,
        ...(invocation.resumeCount === undefined
          ? {}
          : { resumeCount: invocation.resumeCount }),
        deadlineAt: invocation.deadlineAt,
        requestJson,
        requestDigest: invocation.requestDigest,
        permit,
      };
    },
    consumeWorkflowRemoteStepDispatch: async (context, permit) => {
      const { reader } = await workflowDispatchInvocation(
        context,
        permit.invocationId,
        'write',
      );
      return reader.consumePermit(permit);
    },
    recordWorkflowRemoteStepResult: async (
      context,
      invocationId,
      rawPermit,
      resultJson,
      evidenceJson,
    ) => {
      const { reader } = await workflowDispatchInvocation(
        context,
        invocationId,
        'write',
      );
      return reader.recordResult(
        invocationId,
        rawPermit,
        resultJson,
        evidenceJson,
      );
    },
    issueRemoteStepDispatch: async (context, invocationId, lifetimeMs) => {
      await dispatchInvocation(context, invocationId);
      const reader = options.remoteStepDispatch;
      if (!reader) throw new RunRouteError(503, 'remote steps are unavailable');
      const permit = await reader.issuePermit(invocationId, lifetimeMs);
      const { invocation } = await dispatchInvocation(context, invocationId);
      if (
        invocation.status !== 'dispatched' ||
        invocation.permitGeneration !== permit.generation ||
        invocation.deadlineAt < permit.expiresAt
      )
        throw new RunRouteError(409, 'remote step dispatch changed');
      const requestJson = await reader.readDispatchRequestJson(invocationId);
      return {
        invocationId,
        deploymentId: invocation.deploymentId,
        workflowId: invocation.workflowId,
        agentId: invocation.agentId,
        threadId: invocation.threadId,
        resourceId: invocation.resourceId,
        runId: invocation.runId,
        stepPath: invocation.stepPath,
        suspendedAt: invocation.suspendedAt,
        ...(invocation.resumeCount === undefined
          ? {}
          : { resumeCount: invocation.resumeCount }),
        deadlineAt: invocation.deadlineAt,
        requestJson,
        permit,
      };
    },
    consumeRemoteStepDispatch: async (context, permit) => {
      const { reader } = await dispatchInvocation(context, permit.invocationId);
      return reader.consumePermit(permit);
    },
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
      try {
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
        await requireResourceAccess(
          context,
          'run',
          record.runId,
          'write',
          'run',
        );
      } catch (error) {
        if (error instanceof RunRouteError && error.status === 404) {
          const ended = await endedRun(
            context,
            target.agentId,
            target.threadId,
            record.runId,
          );
          if (ended)
            throw new RunNotSuspendedError(
              ended.workflowId,
              record.runId,
              ended.status,
            );
        }
        throw error;
      }
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
              ...expectedSuspensionFor(record),
              entryPath: 'approval.resume',
              requestedBy: record.decidedBy,
            }),
          },
        ),
      );
    },
    resumeRemoteStep: async (context, invocationId) => {
      const reader = options.remoteSteps;
      if (!reader) throw new RunRouteError(503, 'remote steps are unavailable');
      if (!isPathSafeId(invocationId))
        throw new RunRouteError(404, 'invocation not found');
      const invocation = await reader.get(invocationId);
      if (invocation?.status === 'resuming')
        throw new RunRouteError(
          503,
          'remote step resume awaits executor quiescence',
        );
      if (
        invocation?.status !== 'result-retained' ||
        invocation.deploymentId !== reader.deploymentId ||
        invocation.tablePrefix !== reader.tablePrefix ||
        !isPathSafeId(invocation.agentId) ||
        !isPathSafeId(invocation.runId) ||
        !(await reader.readRetainedResult(invocationId))
      )
        throw new RunRouteError(404, 'invocation not found');
      expectedResource(context, invocation.threadId, invocation.resourceId);
      return envelope(
        await threads.send(
          {
            principal: invocation.principal,
            mutationEpoch: context.mutationEpoch,
          },
          invocation.threadId,
          `${AGENT_HOST_ROUTE_PREFIX}/remote-step-resume`,
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ invocationId }),
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
