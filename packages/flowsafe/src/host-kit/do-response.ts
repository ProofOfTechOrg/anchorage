// SPDX-License-Identifier: Apache-2.0
// Reading a Durable Object's answer back as a RunSummary.
//
// Hosts that reach their runs through a DO stub (the showcase, the deploy
// template) all need this: the DO already mapped the runtime's typed errors to
// 404/409/400, so a non-ok answer must carry that status through the run
// router's error mapping rather than collapse into a generic 500.

import {
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';
import {
  normalizeStartExecutionIdentity,
  type StartExecutionIdentity,
} from '../do-runner/execution-admission.js';
import type { RunSummary } from '../do-runner/index.js';
import { isRunStatus } from '../do-runner/run-terminal-state.js';
import type { PersistedStartResult } from '../do-runner/start-idempotency.js';
import { RunRouteError } from './run-route-error.js';

/** The subset of a DO fetch Response this reader touches. */
export interface DoResponseLike {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}

/** @internal */
export async function doStartLiveness(
  response: DoResponseLike,
): Promise<boolean> {
  try {
    if (response.status !== 200) throw new Error('unexpected status');
    const payload = await response.json();
    if (
      payload === null ||
      typeof payload !== 'object' ||
      Array.isArray(payload)
    )
      throw new Error('invalid liveness');
    const live = Object.getOwnPropertyDescriptor(payload, 'live')?.value;
    if (typeof live !== 'boolean') throw new Error('invalid liveness');
    return live;
  } catch {
    throw new RunRouteError(503, 'run start liveness is not readable');
  }
}

/**
 * Parse a DO response as a RunSummary, translating a non-ok answer into a
 * RunRouteError carrying the DO's own status and message.
 */
export async function doSummary(response: DoResponseLike): Promise<RunSummary> {
  const payload = await response.json();
  if (!response.ok) {
    const message =
      payload !== null &&
      typeof payload === 'object' &&
      typeof (payload as { error?: unknown }).error === 'string'
        ? (payload as { error: string }).error
        : `run request failed with status ${response.status}`;
    const reason =
      payload !== null && typeof payload === 'object'
        ? (payload as { reason?: unknown }).reason
        : undefined;
    throw new RunRouteError(response.status, message, reason);
  }
  return payload as RunSummary;
}

const EXECUTION_AUTHORITY_FIELDS = new Set([
  'execution',
  'startIdentity',
  'startReservation',
  'startToken',
  'attemptToken',
  'runOwnerGuard',
  'onPreparedStartIdentity',
  'mutationEpoch',
  'agentStart',
  'tablePrefix',
  'initialAdmission',
  'resumeCounts',
  'requestContext',
  'snapshot',
  'provenance',
  'raw',
  'flowsafe.runProvenance',
  'flowsafe.runLifecycle',
]);

/** @internal Strict private transport objects never invoke accessors. */
export function persistedStartRecord(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new RunRouteError(503, 'persisted start is not readable');
  }
  const result: Record<string, unknown> = Object.create(null);
  for (const [key, descriptor] of Object.entries(
    Object.getOwnPropertyDescriptors(value),
  )) {
    if (!('value' in descriptor)) {
      throw new RunRouteError(503, 'persisted start is not readable');
    }
    result[key] = descriptor.value;
  }
  return result;
}

/** @internal Canonical complete identity for protected replay only. */
export function persistedStartExecution(
  value: unknown,
): StartExecutionIdentity {
  const source = persistedStartRecord(value);
  const owner = persistedStartRecord(source.owner);
  const target = persistedStartRecord(source.target);
  const execution = normalizeStartExecutionIdentity({
    ...source,
    owner,
    target,
  });
  if (execution.tablePrefix !== source.tablePrefix) {
    throw new RunRouteError(503, 'persisted start is not readable');
  }
  return execution;
}

/** @internal Project only public structure; application payloads remain opaque. */
export function publicStartFields(
  value: unknown,
  keys: readonly string[],
): Record<string, unknown> {
  const source = persistedStartRecord(value);
  if (Object.keys(source).some((key) => EXECUTION_AUTHORITY_FIELDS.has(key))) {
    throw new RunRouteError(503, 'persisted start is not readable');
  }
  return Object.fromEntries(
    keys
      .filter((key) => Object.hasOwn(source, key))
      .map((key) => [key, source[key]]),
  );
}

/** @internal The same projection is used by private producers and consumers. */
export function publicRunSummary(value: unknown, runId: string): RunSummary {
  const summary = publicStartFields(value, [
    'runId',
    'status',
    'requestedBy',
    'requestedByKind',
    'result',
    'error',
    'errorEnvelope',
    'deadlineAt',
    'suspended',
    'suspendPayload',
    'suspendedAt',
    'resumedAt',
    'resumeCount',
    'createdAt',
    'updatedAt',
  ]);
  const invalid = (): never => {
    throw new RunRouteError(503, 'persisted start is not readable');
  };
  if (
    summary.runId !== runId ||
    !isRunStatus(summary.status) ||
    summary.status === 'pending'
  )
    invalid();
  if (
    (summary.requestedBy !== undefined ||
      summary.requestedByKind !== undefined) &&
    (!isExecutionPrincipalId(summary.requestedBy) ||
      !isExecutionPrincipalKind(summary.requestedByKind))
  )
    invalid();
  for (const key of ['error', 'createdAt', 'updatedAt']) {
    if (summary[key] !== undefined && typeof summary[key] !== 'string')
      invalid();
  }
  if (
    summary.deadlineAt !== undefined &&
    (typeof summary.deadlineAt !== 'number' ||
      !Number.isFinite(summary.deadlineAt))
  )
    invalid();
  if (
    summary.suspended !== undefined &&
    (!Array.isArray(summary.suspended) ||
      summary.suspended.some(
        (path) =>
          !Array.isArray(path) || path.some((part) => typeof part !== 'string'),
      ))
  )
    invalid();
  for (const key of ['suspendedAt', 'resumedAt', 'resumeCount']) {
    if (summary[key] === undefined) continue;
    const map = persistedStartRecord(summary[key]);
    if (
      Object.values(map).some(
        (item) =>
          typeof item !== 'number' ||
          !Number.isFinite(item) ||
          (key === 'resumeCount' && (!Number.isSafeInteger(item) || item < 0)),
      )
    )
      invalid();
    summary[key] = { ...map };
  }
  if (summary.errorEnvelope !== undefined) {
    const error = persistedStartRecord(summary.errorEnvelope);
    if (
      (error.code !== 'CANCELLED' && error.code !== 'TIMED_OUT') ||
      typeof error.message !== 'string'
    )
      invalid();
    summary.errorEnvelope = { code: error.code, message: error.message };
  }
  return summary as unknown as RunSummary;
}

export async function doPersistedStart(
  response: DoResponseLike,
  expected: { workflowId: string; runId: string },
): Promise<PersistedStartResult<RunSummary>> {
  if (!response.ok) {
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      /* Preserve the status-only fallback. */
    }
    const error =
      payload !== null && typeof payload === 'object'
        ? (payload as { error?: unknown; reason?: unknown })
        : undefined;
    throw new RunRouteError(
      response.status,
      typeof error?.error === 'string'
        ? error.error
        : `run request failed with status ${response.status}`,
      error?.reason,
    );
  }
  try {
    const payload = persistedStartRecord(await response.json());
    const execution = persistedStartExecution(payload.execution);
    if (
      execution.workflowId !== expected.workflowId ||
      execution.runId !== expected.runId ||
      execution.target.kind !== 'workflow' ||
      execution.target.id !== expected.workflowId
    )
      throw new Error('selector mismatch');
    if (payload.kind === 'initial' && !Object.hasOwn(payload, 'value'))
      return { kind: 'initial', execution };
    if (payload.kind !== 'result' || !Object.hasOwn(payload, 'value'))
      throw new Error('invalid result');
    return {
      kind: 'result',
      execution,
      value: publicRunSummary(payload.value, expected.runId),
    };
  } catch {
    throw new RunRouteError(503, 'persisted start is not readable');
  }
}
