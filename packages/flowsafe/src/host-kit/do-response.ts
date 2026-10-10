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
import { isPublishedRefusal } from '../do-runner/do-status-error.js';
import {
  normalizeStartExecutionIdentity,
  type StartExecutionIdentity,
} from '../do-runner/execution-admission.js';
import type { RunSummary } from '../do-runner/index.js';
import { isRunTerminalErrorCode } from '../do-runner/run-lifecycle.js';
import { isRunStatus } from '../do-runner/run-terminal-state.js';
import type { PersistedStartResult } from '../do-runner/start-idempotency.js';
import {
  internalErrorMessage,
  internalErrorResponse,
} from '../internal-error-response.js';
import { RunRouteError } from './run-route-error.js';

/** The subset of a DO fetch Response this reader touches. */
export interface DoResponseLike {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}

/**
 * The answer's message and reason. A body without a message gets a status-only
 * message naming `subject`.
 */
async function readObjectAnswer(
  response: DoResponseLike,
  subject: string,
): Promise<{ message: string; reason?: unknown }> {
  const fallback = `${subject} request failed with status ${response.status}`;
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    return { message: fallback };
  }
  if (payload === null || typeof payload !== 'object')
    return { message: fallback };
  const { error, reason } = payload as { error?: unknown; reason?: unknown };
  return { message: typeof error === 'string' ? error : fallback, reason };
}

/**
 * @internal A run or thread object's non-ok answer as a RunRouteError. The
 * object answers a fault it did not classify with the fault's own message,
 * which can name storage or deployment detail, so a 5xx without a reason code
 * keeps its status, its message is logged under `route`, and the error carries
 * the generic message. Any other answer keeps its message, or a status-only
 * message naming `subject` when the body has none, and its reason. A body that
 * is not JSON keeps the status.
 */
export async function objectAnswerError(
  route: string,
  response: DoResponseLike,
  subject: string,
): Promise<RunRouteError> {
  const { message, reason } = await readObjectAnswer(response, subject);
  const { status } = response;
  return isPublishedRefusal(status, reason)
    ? new RunRouteError(status, message, reason)
    : new RunRouteError(status, internalErrorMessage(route, message));
}

/**
 * @internal A run or thread object's answer, forwarded as it is unless it is a
 * 5xx without a reason code, which objectAnswerError's rule replaces with the
 * generic answer at the same status. The original response is left unread.
 */
export async function objectAnswerResponse(
  route: string,
  response: Response,
  subject: string,
): Promise<Response> {
  if (response.status < 500) return response;
  const { message, reason } = await readObjectAnswer(response.clone(), subject);
  return isPublishedRefusal(response.status, reason)
    ? response
    : internalErrorResponse(route, message, response.status);
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
 * RunRouteError carrying the DO's own status, message and reason. A 5xx
 * without a reason code is a fault the DO did not classify: its message is
 * logged and the error carries `internal error`. A non-ok answer whose body is
 * not JSON keeps its status.
 */
export async function doSummary(response: DoResponseLike): Promise<RunSummary> {
  if (!response.ok) {
    throw await objectAnswerError('run-object', response, 'run');
  }
  return (await response.json()) as RunSummary;
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
    'suspensionTimers',
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
  if (
    summary.suspensionTimers !== undefined &&
    (!Array.isArray(summary.suspensionTimers) ||
      summary.suspensionTimers.some((step) => typeof step !== 'string'))
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
      !isRunTerminalErrorCode(error.code) ||
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
    throw await objectAnswerError('run-object', response, 'run');
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
