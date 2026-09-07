// SPDX-License-Identifier: Apache-2.0

import type { ExecutionPrincipalKind } from '../approval-api/principal-identity.js';
import {
  InvalidExecutionIdentityError,
  normalizeStartIdentity,
  type RunExecutionIdentity,
} from './execution-admission.js';
import { isPathSafeId } from './path-safe-id.js';

export const START_RESERVATION_STATES = [
  'reserved',
  'started',
  'terminal',
] as const;

/**
 * Where a reservation is in its life:
 *
 *   reserved  the key means this runId, and nobody has started it yet
 *   started   one caller won the claim and is (or was) executing
 *   terminal  the run reached a terminal state; the key is spent
 *
 * The states only ever move forward, with ONE exception: a start refused by the
 * execution fence rolls `started` back to `reserved` (see `release`), because a
 * fence refusal is the one failure that provably executed nothing.
 */
export type StartReservationState = (typeof START_RESERVATION_STATES)[number];

export const START_TARGET_KINDS = ['workflow', 'agent'] as const;

/** Which execution family a key names — a workflow run, or an agent run. */
export type StartTargetKind = (typeof START_TARGET_KINDS)[number];

/**
 * WHO a key belongs to. An execution principal, projected to the same two
 * fields `ResourceOwner` carries, and for the same reason: a key is a
 * capability to converge on somebody's run, so it must be scoped to whoever
 * created it and unforgeable from tenant traffic.
 */
export interface StartReservationOwner {
  readonly kind: ExecutionPrincipalKind;
  readonly id: string;
}

/** One reservation row, as every surface reads it. */
export interface StartReservation {
  readonly key: string;
  readonly owner: StartReservationOwner;
  readonly targetKind: StartTargetKind;
  readonly targetId: string;
  readonly runId: string;
  /**
   * The agent run's thread, when the target is an agent. It is the run's
   * ADDRESS: a workflow run is reachable from (workflowId, runId) alone, but an
   * agent run lives in a thread object and a retry that minted a fresh thread
   * would otherwise have no way back to the original. Absent for workflows,
   * where storing a derivable address would be a second source of truth.
   */
  readonly threadId?: string;
  readonly state: StartReservationState;
  /**
   * Epoch ms of the reserve that created this row. Provenance only: the purge
   * horizon is measured from `updatedAt`, so that a key's validity runs from
   * the moment it was SPENT rather than from the moment it was first used —
   * a long run must not age its own reservation out while it is still running.
   */
  readonly createdAt: number;
  /**
   * Epoch ms of the last state change — `pendingSince` on a live claim, and the
   * column the purge horizon is measured from once the row is terminal.
   */
  readonly updatedAt: number;
  readonly binding?: StartReservationBinding;
}

export type StartReservationBinding =
  | { readonly kind: 'legacy' }
  | { readonly kind: 'unbound' }
  | { readonly kind: 'bound'; readonly execution: RunExecutionIdentity };

export interface StartReservationReading extends StartReservation {
  readonly binding: StartReservationBinding;
}

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new InvalidExecutionIdentityError('admission');
  return value as Record<string, unknown>;
}

export function captureReservation(
  value: StartReservationReading,
  expectedState: 'reserved' | 'started' | 'nonterminal',
): StartReservationReading {
  const {
    key,
    owner,
    targetKind,
    targetId,
    runId,
    threadId,
    state,
    createdAt,
    updatedAt,
    binding,
  } = record(value);
  const identity = normalizeStartIdentity({
    owner,
    target: { kind: targetKind, id: targetId, threadId },
  });
  if (
    record(binding).kind !== 'unbound' ||
    (state !== 'reserved' && state !== 'started') ||
    (expectedState !== 'nonterminal' && state !== expectedState) ||
    !isPathSafeId(key) ||
    !isPathSafeId(runId) ||
    typeof createdAt !== 'number' ||
    !Number.isFinite(createdAt) ||
    typeof updatedAt !== 'number' ||
    !Number.isFinite(updatedAt)
  )
    throw new InvalidExecutionIdentityError('admission');
  return Object.freeze({
    key,
    runId,
    owner: identity.owner,
    targetKind: identity.target.kind,
    targetId: identity.target.id,
    ...(identity.target.kind === 'agent'
      ? { threadId: identity.target.threadId }
      : {}),
    state,
    createdAt,
    updatedAt,
    binding: Object.freeze({ kind: 'unbound' as const }),
  });
}

export function sameReservationIdentity(
  actual: StartReservationReading,
  expected: StartReservationReading,
): boolean {
  return (
    actual.key === expected.key &&
    actual.runId === expected.runId &&
    actual.owner.kind === expected.owner.kind &&
    actual.owner.id === expected.owner.id &&
    actual.targetKind === expected.targetKind &&
    actual.targetId === expected.targetId &&
    actual.threadId === expected.threadId &&
    actual.createdAt === expected.createdAt
  );
}
