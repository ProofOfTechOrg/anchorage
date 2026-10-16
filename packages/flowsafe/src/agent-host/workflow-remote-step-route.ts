// SPDX-License-Identifier: Apache-2.0

import { isOwnedBy, samePrincipal } from '../approval-api/principal.js';
import { doErrorResponse } from '../do-runner/do-error-response.js';
import { isPathSafeId } from '../do-runner/path-safe-id.js';
import type { RemoteStepPermit } from '../do-runner/remote-step-journal.js';
import type {
  FlowsafeWorkerConfig,
  FlowsafeWorkerEnv,
} from '../host-kit/flowsafe-worker.js';
import { requireResourceAccess } from '../host-kit/resource-access.js';
import {
  RunRouteError,
  runRouteErrorBody,
} from '../host-kit/run-route-error.js';
import { readBoundedBody } from '../http-body.js';
import type {
  AgentThreadDispatchTopology,
  WorkflowRemoteStepDispatchReader,
} from './thread-topology.js';

export const WORKFLOW_REMOTE_STEP_ROUTE = '/_flowsafe/workflow-remote-step';

export type WorkflowDispatchPort = Pick<
  AgentThreadDispatchTopology,
  | 'registerWorkflowRemoteStep'
  | 'issueWorkflowRemoteStepDispatch'
  | 'consumeWorkflowRemoteStepDispatch'
  | 'recordWorkflowRemoteStepResult'
>;

type Operation = 'register' | 'ticket' | 'consume' | 'result' | 'resume';

const fields: Record<Operation, readonly string[]> = {
  register: ['workflowId', 'runId', 'hostDeadlineAt'],
  ticket: ['invocationId', 'lifetimeMs'],
  consume: ['permit'],
  result: ['invocationId', 'rawPermit', 'resultJson', 'evidenceJson'],
  resume: ['invocationId'],
};

function json(value: unknown, status = 200): Response {
  return Response.json(value, {
    status,
    headers: { 'cache-control': 'no-store' },
  });
}

function exactBody(
  value: unknown,
  operation: Operation,
): Record<string, unknown> | undefined {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    return undefined;
  const body = value as Record<string, unknown>;
  const keys = Object.keys(body);
  const expected = fields[operation];
  return keys.length === expected.length &&
    keys.every((key) => expected.includes(key))
    ? body
    : undefined;
}

function permit(value: unknown): RemoteStepPermit | undefined {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    return undefined;
  const candidate = value as Record<string, unknown>;
  if (
    Object.keys(candidate).sort().join(',') !==
      'expiresAt,generation,invocationId,mutationEpoch,transitionRevision' ||
    typeof candidate.invocationId !== 'string' ||
    !isPathSafeId(candidate.invocationId) ||
    !['generation', 'mutationEpoch', 'transitionRevision', 'expiresAt'].every(
      (key) =>
        Number.isSafeInteger(candidate[key]) && (candidate[key] as number) >= 0,
    )
  )
    return undefined;
  return candidate as unknown as RemoteStepPermit;
}

/** A host mounts this route through preRoutes on its private state-script Worker. */
export function createWorkflowRemoteStepPreRoute(
  journal: WorkflowRemoteStepDispatchReader,
  dispatch: WorkflowDispatchPort,
): NonNullable<FlowsafeWorkerConfig<FlowsafeWorkerEnv>['preRoutes']> {
  return async (request, _env, _ctx, kit) => {
    const url = new URL(request.url);
    if (!url.pathname.startsWith(`${WORKFLOW_REMOTE_STEP_ROUTE}/`)) return null;
    const operation = url.pathname.slice(WORKFLOW_REMOTE_STEP_ROUTE.length + 1);
    if (!Object.hasOwn(fields, operation) || url.search)
      return json({ error: 'not found' }, 404);
    if (request.method !== 'POST')
      return json({ error: 'method not allowed' }, 405);
    const unauthorized = await kit.authorizePrivateWorkflowDispatch(request);
    if (unauthorized) return unauthorized;
    const raw = await readBoundedBody(request, 65_536);
    if (!raw.ok)
      return json(
        { error: 'invalid request body' },
        raw.reason === 'payload-too-large' ? 413 : 400,
      );
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw.text);
    } catch {
      return json({ error: 'invalid request body' }, 400);
    }
    const body = exactBody(parsed, operation as Operation);
    if (!body) return json({ error: 'invalid request body' }, 400);
    try {
      if (operation === 'register') {
        if (
          typeof body.workflowId !== 'string' ||
          !isPathSafeId(body.workflowId) ||
          typeof body.runId !== 'string' ||
          !isPathSafeId(body.runId) ||
          !Number.isSafeInteger(body.hostDeadlineAt) ||
          (body.hostDeadlineAt as number) <= 0
        )
          return json({ error: 'invalid request body' }, 400);
        const context = await kit.contextForStoredWorkflowStart(
          body.workflowId,
          body.runId,
        );
        if (!context) return json({ error: 'run not found' }, 404);
        const invocation = await dispatch.registerWorkflowRemoteStep(
          context,
          body.workflowId,
          body.runId,
          body.hostDeadlineAt as number,
        );
        return json({ invocationId: invocation.invocationId });
      }
      const invocationId =
        operation === 'consume'
          ? permit(body.permit)?.invocationId
          : body.invocationId;
      if (typeof invocationId !== 'string' || !isPathSafeId(invocationId))
        return json({ error: 'invalid request body' }, 400);
      const context = await kit.contextForStoredWorkflowInvocation(
        journal,
        invocationId,
      );
      if (!context) return json({ error: 'invocation not found' }, 404);
      if (operation === 'resume') {
        const invocation = await journal.getWorkflow(invocationId);
        if (
          invocation?.targetKind !== 'workflow' ||
          invocation.invocationId !== invocationId ||
          invocation.deploymentId !== journal.deploymentId ||
          invocation.tablePrefix !== journal.tablePrefix ||
          !samePrincipal(invocation.principal, context.principal)
        )
          return json({ error: 'invocation not found' }, 404);
        const owner = await context.resourceOwnerFor('run', invocation.runId);
        if (owner !== undefined) {
          if (!isOwnedBy(owner, invocation.principal))
            return json({ error: 'run not found' }, 404);
          await requireResourceAccess(
            context,
            'run',
            invocation.runId,
            'write',
            'run',
          );
        } else if (
          invocation.status !== 'settled' ||
          invocation.settlementOutcome !== 'applied'
        ) {
          return json({ error: 'run not found' }, 404);
        }
        return json(
          await kit.topology.resumeWorkflowRemoteStep(
            invocation.workflowId,
            invocation.runId,
            invocationId,
            context.principal,
            context.mutationEpoch,
          ),
        );
      }
      if (operation === 'ticket') {
        if (
          !Number.isSafeInteger(body.lifetimeMs) ||
          (body.lifetimeMs as number) <= 0
        )
          return json({ error: 'invalid request body' }, 400);
        return json(
          await dispatch.issueWorkflowRemoteStepDispatch(
            context,
            invocationId,
            body.lifetimeMs as number,
          ),
        );
      }
      if (operation === 'consume') {
        const submitted = permit(body.permit);
        if (!submitted) return json({ error: 'invalid request body' }, 400);
        return json(
          await dispatch.consumeWorkflowRemoteStepDispatch(context, submitted),
        );
      }
      if (
        typeof body.rawPermit !== 'string' ||
        typeof body.resultJson !== 'string' ||
        typeof body.evidenceJson !== 'string'
      )
        return json({ error: 'invalid request body' }, 400);
      return json(
        await dispatch.recordWorkflowRemoteStepResult(
          context,
          invocationId,
          body.rawPermit,
          body.resultJson,
          body.evidenceJson,
        ),
      );
    } catch (error) {
      if (error instanceof RunRouteError) {
        const refusal = runRouteErrorBody(error);
        return refusal
          ? json(refusal, error.status)
          : json({ error: 'private workflow dispatch failed' }, 500);
      }
      const response = doErrorResponse(error);
      if (response.status === 500)
        return json({ error: 'private workflow dispatch failed' }, 500);
      response.headers.set('cache-control', 'no-store');
      return response;
    }
  };
}
