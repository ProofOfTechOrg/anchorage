// SPDX-License-Identifier: Apache-2.0

import type { ExecutionPrincipalKind } from '../approval-api/principal.js';

/** The ownership identity returned by the deployment resource registry. */
export interface ScheduleSourceOwner {
  readonly kind: ExecutionPrincipalKind;
  readonly id: string;
}

export interface ScheduleSourceWorkflowTarget {
  readonly type: 'workflow';
  readonly workflowId: string;
  readonly inputData?: unknown;
  readonly initialState?: unknown;
  readonly requestContext?: Record<string, unknown>;
}

export interface ScheduleSourceAgentTarget {
  readonly type: 'agent';
  readonly agentId: string;
  readonly prompt: string;
  readonly threadId?: string;
  readonly resourceId?: string;
  readonly signalType?: string;
  readonly tagName?: string;
  readonly attributes?: Record<string, unknown>;
  readonly providerOptions?: Record<string, unknown>;
  readonly ifActive?: unknown;
  readonly ifIdle?: unknown;
  readonly requestContext?: Record<string, unknown>;
}

export type ScheduleSourceTarget =
  | ScheduleSourceWorkflowTarget
  | ScheduleSourceAgentTarget;

/**
 * Target-verification seam implemented by the schedules package. Keeping it
 * structural avoids a do-runner -> schedules dependency while ensuring hosts
 * consume the schedules domain's canonical validation and prepared-trigger
 * lookup instead of recreating either policy here.
 */
export interface ScheduleSourceStore {
  resolveScheduleTarget(
    scheduleId: string,
    dispatchId: string,
    runId: string,
  ): Promise<ScheduleSourceTarget | undefined>;
}

/** Committed ownership reads used to bind a fire to its stored source. */
export interface ScheduleSourceOwnershipStore {
  owner(
    kind: 'schedule' | 'thread' | 'resource',
    resourceId: string,
  ): Promise<ScheduleSourceOwner | undefined>;
}

export type ScheduleStartTarget =
  | {
      readonly type: 'workflow';
      readonly workflowId: string;
    }
  | {
      readonly type: 'agent';
      readonly mode: 'threadless-start';
      readonly agentId: string;
    }
  | {
      readonly type: 'agent';
      readonly mode: 'threaded-wake';
      readonly agentId: string;
      readonly threadId: string;
      readonly resourceId: string;
    }
  | {
      readonly type: 'agent';
      readonly mode: 'threaded-signal';
      readonly agentId: string;
      readonly threadId: string;
      readonly resourceId: string;
    };

export interface ResolvedScheduleStart<
  Target extends ScheduleSourceTarget = ScheduleSourceTarget,
> {
  readonly owner: ScheduleSourceOwner;
  readonly target: Target;
}

function sameOwner(
  left: ScheduleSourceOwner,
  right: ScheduleSourceOwner,
): boolean {
  return left.kind === right.kind && left.id === right.id;
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

/**
 * Resolve the committed owner of an exact stored schedule target.
 *
 * `owner()` deliberately hides pending reservations, so an uncommitted
 * schedule can never lend authority to a run start. Agent mode is part of the
 * target: a threadless start cannot be replayed against a standing thread, and
 * a threaded wake must match both fixed memory identifiers and their common
 * committed owner.
 */
export async function resolveScheduleStartOwner(
  schedules: ScheduleSourceStore,
  ownership: ScheduleSourceOwnershipStore,
  scheduleId: string,
  dispatchId: string,
  runId: string,
  expected: Extract<ScheduleStartTarget, { type: 'workflow' }>,
): Promise<ResolvedScheduleStart<ScheduleSourceWorkflowTarget> | undefined>;
export async function resolveScheduleStartOwner(
  schedules: ScheduleSourceStore,
  ownership: ScheduleSourceOwnershipStore,
  scheduleId: string,
  dispatchId: string,
  runId: string,
  expected: Extract<ScheduleStartTarget, { type: 'agent' }>,
): Promise<ResolvedScheduleStart<ScheduleSourceAgentTarget> | undefined>;
export async function resolveScheduleStartOwner(
  schedules: ScheduleSourceStore,
  ownership: ScheduleSourceOwnershipStore,
  scheduleId: string,
  dispatchId: string,
  runId: string,
  expected: ScheduleStartTarget,
): Promise<ResolvedScheduleStart | undefined> {
  const sourceTarget = await schedules.resolveScheduleTarget(
    scheduleId,
    dispatchId,
    runId,
  );
  const target = record(sourceTarget);
  if (!target || target.type !== expected.type) return undefined;

  const scheduleOwner = await ownership.owner('schedule', scheduleId);
  if (!scheduleOwner) return undefined;

  if (expected.type === 'workflow') {
    return target.workflowId === expected.workflowId
      ? {
          owner: scheduleOwner,
          target: sourceTarget as ScheduleSourceWorkflowTarget,
        }
      : undefined;
  }

  if (target.agentId !== expected.agentId) return undefined;
  if (expected.mode === 'threadless-start') {
    return target.threadId === undefined &&
      target.resourceId === undefined &&
      target.signalType === undefined &&
      target.ifActive === undefined &&
      target.ifIdle === undefined
      ? {
          owner: scheduleOwner,
          target: sourceTarget as ScheduleSourceAgentTarget,
        }
      : undefined;
  }

  if (
    target.threadId !== expected.threadId ||
    target.resourceId !== expected.resourceId
  ) {
    return undefined;
  }
  const idle = record(target.ifIdle);
  if (
    expected.mode === 'threaded-wake' &&
    target.ifIdle !== undefined &&
    idle?.behavior !== 'wake'
  ) {
    return undefined;
  }
  const [threadOwner, resourceOwner] = await Promise.all([
    ownership.owner('thread', expected.threadId),
    ownership.owner('resource', expected.resourceId),
  ]);
  return threadOwner &&
    resourceOwner &&
    sameOwner(scheduleOwner, threadOwner) &&
    sameOwner(scheduleOwner, resourceOwner)
    ? {
        owner: scheduleOwner,
        target: sourceTarget as ScheduleSourceAgentTarget,
      }
    : undefined;
}
