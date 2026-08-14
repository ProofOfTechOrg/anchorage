// SPDX-License-Identifier: Apache-2.0

import {
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';
import { isPathSafeId } from './path-safe-id.js';

/** Runtime-owned request-context key for durable run lifecycle metadata. */
export const RUN_LIFECYCLE_CONTEXT_KEY = 'flowsafe.runLifecycle';

export type RunTerminalStatus = 'cancelled' | 'timed_out';

export interface RunTerminalErrorEnvelope {
  code: 'CANCELLED' | 'TIMED_OUT';
  message: string;
}

/**
 * Trusted settlement projection supplied by an economic-operation host.
 * Flowsafe only interprets `disputed`; every other state remains host-defined.
 */
export interface RunEconomicOperation {
  id: string;
  settlementState: string;
}

export interface RunScheduleDispatch {
  scheduleId: string;
  dispatchId: string;
}

export interface RunLifecyclePrincipal {
  kind: ExecutionPrincipalKind;
  id: string;
}

export interface RunLifecycleState {
  version: 1;
  /** Monotonic compare-and-swap revision owned by the run's Durable Object. */
  revision: number;
  /** Epoch milliseconds. */
  deadlineAt?: number;
  economicOperations?: RunEconomicOperation[];
  scheduleDispatch?: RunScheduleDispatch;
  transitionIntent?: {
    status: RunTerminalStatus;
    requestedAt: number;
    replayPrincipals: RunLifecyclePrincipal[];
    expectedRevision?: number;
    expectedDeadlineAt?: number;
  };
  terminal?: {
    status: RunTerminalStatus;
    error: RunTerminalErrorEnvelope;
    transitionedAt: number;
    /** Exact identities allowed to replay this terminal transition after ownership release. */
    replayPrincipals: RunLifecyclePrincipal[];
    /** Set only after approval/dispatch/ownership cleanup has completed. */
    cleanupCompletedAt?: number;
  };
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function validTime(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function economicOperations(
  value: unknown,
): RunEconomicOperation[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value))
    throw new Error('stored run lifecycle is malformed');
  return value.map((entry) => {
    const operation = record(entry);
    if (
      !operation ||
      !isPathSafeId(operation.id) ||
      typeof operation.settlementState !== 'string' ||
      operation.settlementState.length === 0 ||
      operation.settlementState.length > 100
    ) {
      throw new Error('stored run lifecycle is malformed');
    }
    return {
      id: operation.id,
      settlementState: operation.settlementState,
    };
  });
}

function scheduleDispatch(value: unknown): RunScheduleDispatch | undefined {
  if (value === undefined) return undefined;
  const dispatch = record(value);
  if (
    !dispatch ||
    !isPathSafeId(dispatch.scheduleId) ||
    !isPathSafeId(dispatch.dispatchId)
  ) {
    throw new Error('stored run lifecycle is malformed');
  }
  return {
    scheduleId: dispatch.scheduleId,
    dispatchId: dispatch.dispatchId,
  };
}

function terminal(value: unknown): RunLifecycleState['terminal'] | undefined {
  if (value === undefined) return undefined;
  const stored = record(value);
  const error = record(stored?.error);
  if (
    !stored ||
    (stored.status !== 'cancelled' && stored.status !== 'timed_out') ||
    !validTime(stored.transitionedAt) ||
    !error ||
    (error.code !== 'CANCELLED' && error.code !== 'TIMED_OUT') ||
    typeof error.message !== 'string' ||
    error.message.length === 0 ||
    error.message.length > 500 ||
    (stored.status === 'cancelled' && error.code !== 'CANCELLED') ||
    (stored.status === 'timed_out' && error.code !== 'TIMED_OUT') ||
    !Array.isArray(stored.replayPrincipals) ||
    (stored.cleanupCompletedAt !== undefined &&
      !validTime(stored.cleanupCompletedAt))
  ) {
    throw new Error('stored run lifecycle is malformed');
  }
  const replayPrincipals = stored.replayPrincipals.map((value) => {
    const principal = record(value);
    if (
      !principal ||
      !isExecutionPrincipalKind(principal.kind) ||
      !isExecutionPrincipalId(principal.id)
    ) {
      throw new Error('stored run lifecycle is malformed');
    }
    return { kind: principal.kind, id: principal.id };
  });
  if (replayPrincipals.length === 0 || replayPrincipals.length > 2) {
    throw new Error('stored run lifecycle is malformed');
  }
  return {
    status: stored.status,
    error: { code: error.code, message: error.message },
    transitionedAt: stored.transitionedAt,
    replayPrincipals,
    ...(stored.cleanupCompletedAt === undefined
      ? {}
      : { cleanupCompletedAt: stored.cleanupCompletedAt }),
  };
}

function transitionIntent(
  value: unknown,
): RunLifecycleState['transitionIntent'] | undefined {
  if (value === undefined) return undefined;
  const stored = record(value);
  if (
    !stored ||
    (stored.status !== 'cancelled' && stored.status !== 'timed_out') ||
    !validTime(stored.requestedAt) ||
    !Array.isArray(stored.replayPrincipals) ||
    (stored.expectedRevision !== undefined &&
      (!Number.isSafeInteger(stored.expectedRevision) ||
        (stored.expectedRevision as number) < 1)) ||
    (stored.expectedDeadlineAt !== undefined &&
      !validTime(stored.expectedDeadlineAt))
  ) {
    throw new Error('stored run lifecycle is malformed');
  }
  const replayPrincipals = canonicalReplayPrincipals(
    stored.replayPrincipals as RunLifecyclePrincipal[],
  );
  return {
    status: stored.status,
    requestedAt: stored.requestedAt,
    replayPrincipals,
    ...(stored.expectedRevision === undefined
      ? {}
      : { expectedRevision: stored.expectedRevision as number }),
    ...(stored.expectedDeadlineAt === undefined
      ? {}
      : { expectedDeadlineAt: stored.expectedDeadlineAt as number }),
  };
}

function sameLifecyclePrincipal(
  left: RunLifecyclePrincipal,
  right: RunLifecyclePrincipal,
): boolean {
  return left.kind === right.kind && left.id === right.id;
}

export function canonicalReplayPrincipals(
  values: readonly RunLifecyclePrincipal[],
): RunLifecyclePrincipal[] {
  const principals: RunLifecyclePrincipal[] = [];
  for (const value of values) {
    if (
      !isExecutionPrincipalKind(value.kind) ||
      !isExecutionPrincipalId(value.id)
    ) {
      throw new Error('run lifecycle principal is malformed');
    }
    if (!principals.some((stored) => sameLifecyclePrincipal(stored, value))) {
      principals.push({ kind: value.kind, id: value.id });
    }
  }
  if (principals.length === 0 || principals.length > 2) {
    throw new Error('run lifecycle requires one or two replay principals');
  }
  return principals;
}

export function parseRunLifecycle(
  value: unknown,
): RunLifecycleState | undefined {
  if (value === undefined) return undefined;
  const stored = record(value);
  if (
    stored?.version !== 1 ||
    !Number.isSafeInteger(stored.revision) ||
    (stored.revision as number) < 1 ||
    (stored.deadlineAt !== undefined && !validTime(stored.deadlineAt))
  ) {
    throw new Error('stored run lifecycle is malformed');
  }
  return {
    version: 1,
    revision: stored.revision as number,
    ...(stored.deadlineAt === undefined
      ? {}
      : { deadlineAt: stored.deadlineAt as number }),
    ...(stored.economicOperations === undefined
      ? {}
      : { economicOperations: economicOperations(stored.economicOperations) }),
    ...(stored.scheduleDispatch === undefined
      ? {}
      : { scheduleDispatch: scheduleDispatch(stored.scheduleDispatch) }),
    ...(stored.transitionIntent === undefined
      ? {}
      : { transitionIntent: transitionIntent(stored.transitionIntent) }),
    ...(stored.terminal === undefined
      ? {}
      : { terminal: terminal(stored.terminal) }),
  };
}

export function lifecycleFromRequestContext(
  requestContext: Record<string, unknown> | undefined,
): RunLifecycleState | undefined {
  return parseRunLifecycle(requestContext?.[RUN_LIFECYCLE_CONTEXT_KEY]);
}

export function canonicalEconomicOperations(
  value: readonly RunEconomicOperation[] | undefined,
): RunEconomicOperation[] | undefined {
  return economicOperations(value);
}

export function canonicalScheduleDispatch(
  value: RunScheduleDispatch | undefined,
): RunScheduleDispatch | undefined {
  return scheduleDispatch(value);
}

export function hasDisputedSettlement(
  lifecycle: RunLifecycleState | undefined,
): boolean {
  return (
    lifecycle?.economicOperations?.some(
      (operation) => operation.settlementState === 'disputed',
    ) ?? false
  );
}
