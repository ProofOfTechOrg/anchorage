// SPDX-License-Identifier: Apache-2.0
// HTTP surface for the run catalog + run lifecycle, shared by every host.
//
// Mirrors createApprovalRouter's contract — plain fetch routing, an injected
// `authenticate`, and `null` for paths outside its ownership so a host Worker
// can compose it after the approval router. (Its two paths are fixed rather
// than configurable: unlike the approval surface, a host mounts exactly one run
// surface.) What it owns that the hosts used to triplicate is the route-specific
// AUTHORIZATION order:
//
//   start:   authenticate -> coarse role -> workflow role -> host policy
//   resume:    authenticate -> catalog -> ownership -> coarse role -> workflow
//              role -> host policy
//   terminate: authenticate -> catalog -> ownership -> coarse role -> workflow
//              role
//
// and the suspension bridge: a start that suspends queues its approval
// attributed to the STARTING actor, so that actor cannot later decide their own
// run (separation of duties).
//
// The resume route deliberately carries NO grants. A forged `resumeData.approved`
// can flip a workflow boolean, but capability comes only from the server-derived
// grant the runtime mints per leg (approval-api/grants.ts), so a side-effecting
// step re-checks and fails closed. Approve through the queue, not this route.

import {
  type ActorContext,
  ActorResolutionError,
  type ActorResolver,
  type ApprovalRecord,
  type ExecutionPrincipal,
  type ExecutionPrincipalKind,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunLifecycleBlockedError,
  RunNotSuspendedError,
  type RunSummary,
  RunTerminalConflictError,
  UnknownRunError,
  UnknownWorkflowError,
} from '../do-runner/index.js';
import { readBoundedBody } from '../http-body.js';
import { queueApprovalForSuspension } from './approval-bridge.js';
import { requireResourceAccess } from './resource-access.js';
import { RunRouteError } from './run-route-error.js';
import type { WorkflowMeta } from './workflow-meta.js';

export interface RunRouterOptions {
  /**
   * The catalog: GET /workflows lists these; POST /runs resolves against them
   * and enforces each one's `allowedRoles`.
   *
   * Metadata, not WorkflowModules: the router registers nothing, so depending
   * on the registration machinery would force every host to build a
   * WorkflowModuleContext (and its AuditLogger) just to expose a route. Hosts
   * pass `modules.map((m) => m.meta)`; that the ids match what was actually
   * committed is asserted at registration (see buildShowcaseRuntime).
   */
  workflows: ReadonlyArray<WorkflowMeta>;
  /**
   * Authenticates the request and resolves the actor-scoped approval service
   * and server-owned id minters. undefined yields 401.
   */
  resolve: ActorResolver;
  /**
   * System-principal id used to authorize bridge bookkeeping. Approval
   * requester provenance comes from the run summary, so this id need not be
   * globally disjoint from human ids. Default: 'flowsafe-system'.
   */
  systemPrincipalId?: string;
  /** Host topology: in-process runtime, or a DO stub fetch. */
  start: (input: RunStartInput) => Promise<RunSummary>;
  status: (
    workflowId: string,
    runId: string,
  ) => Promise<RunSummary | undefined>;
  resume: (
    workflowId: string,
    runId: string,
    body: unknown,
    requestedBy: string,
    requestedByKind: ExecutionPrincipalKind,
  ) => Promise<RunSummary>;
  /** Optional for compatibility; when supplied, mounts POST .../terminate. */
  terminate?: (
    workflowId: string,
    runId: string,
    principal: ExecutionPrincipal,
    replayOnly: boolean,
  ) => Promise<RunSummary>;
  /** Host policy that must pass immediately before a validated run start. */
  beforeStart?: (
    context: ActorContext,
    workflowId: string,
    inputData: unknown,
  ) => Promise<void>;
  /** Host policy that must pass immediately before a validated raw resume. */
  beforeResume?: (
    context: ActorContext,
    workflowId: string,
    runId: string,
    body: unknown,
  ) => Promise<void>;
  /**
   * Self-healing hook invoked after a status() read reports the run
   * suspended, so every status poll of a stuck run doubles as a check for a
   * gate whose approval never made it into the queue (see
   * reconcileApprovalsForSummary in approval-bridge.ts). Awaited rather than
   * fire-and-forget by default: this host-agnostic layer has no ctx.waitUntil
   * of its own to keep a detached promise alive past the response, so a
   * plain awaited call is what it can offer on its own. Reconciliation pages
   * the run's full approval history and may supersede stale open records
   * before filing a fresh one, so the two ctx-capable hosts
   * (deploy/worker.ts and the showcase worker) hand this hook a wrapper that
   * detaches the real work
   * via ctx.waitUntil and resolves immediately — this option's contract (an
   * awaited function of this exact shape) is unchanged either way, only
   * what a given host's function actually blocks on. A throw is caught and
   * logged here, never surfaced to the caller: a broken reconcile must not
   * turn a working status read into a 500; the next poll simply retries.
   * Absent => today's behavior (no reconciliation). Hosts wire
   * reconcileApprovalsOnStatus(systemPrincipalId) here, optionally wrapped for
   * waitUntil-detachment.
   */
  reconcileApprovals?: (
    context: ActorContext,
    workflowId: string,
    summary: RunSummary,
  ) => Promise<void>;
}

export interface RunStartInput {
  workflowId: string;
  runId: string;
  inputData: unknown;
  /** Full trusted identity stamped onto the target Durable Object request. */
  principal: ExecutionPrincipal;
  /** Present only for a target-verifiable schedule fire. */
  scheduleId?: string;
  /** Relative deadline measured from the accepted start. */
  deadlineMs?: number;
}

export type RunRouter = (request: Request) => Promise<Response | null>;

interface StartBody {
  workflowId?: string;
  runId?: string;
  inputData?: unknown;
  deadlineMs?: unknown;
}

// no-store is defense in depth: this is an authenticated API served from the
// same origin as (and under) the SPA's `assets` block, and a run projection
// names the actors and payloads of an approval.
function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

function errorResponse(error: unknown): Response {
  if (error instanceof RunRouteError) {
    return json(
      {
        error: error.message,
        ...(error.reason === undefined ? {} : { reason: error.reason }),
      },
      error.status,
    );
  }
  if (error instanceof ActorResolutionError) {
    // Authenticated but malformed claims are a verifier bug, surfaced as
    // forbidden rather than a retryable 500.
    return json({ error: 'forbidden' }, 403);
  }
  if (
    error instanceof UnknownWorkflowError ||
    error instanceof UnknownRunError
  ) {
    return json({ error: error.message }, 404);
  }
  if (
    error instanceof RunNotSuspendedError ||
    error instanceof RunAlreadyExistsError ||
    error instanceof RunTerminalConflictError
  ) {
    return json({ error: error.message }, 409);
  }
  if (error instanceof RunLifecycleBlockedError) {
    return json({ error: error.message, reason: error.reason }, 409);
  }
  if (error instanceof InvalidRunRequestError) {
    return json({ error: error.message }, 400);
  }
  return json(
    { error: error instanceof Error ? error.message : String(error) },
    500,
  );
}

const MAX_RUN_BODY_BYTES = 1_048_576;

async function readJson(request: Request): Promise<unknown> {
  const raw = await readBoundedBody(
    request,
    MAX_RUN_BODY_BYTES,
    'run body exceeds limit',
  );
  if (!raw.ok && raw.reason === 'payload-too-large') {
    throw new RunRouteError(413, 'payload too large');
  }
  if (!raw.ok) return null;
  try {
    return raw.text === '' ? null : JSON.parse(raw.text);
  } catch {
    return null;
  }
}

export function createRunRouter(options: RunRouterOptions): RunRouter {
  const { workflows, resolve } = options;
  const systemPrincipalId = options.systemPrincipalId ?? 'flowsafe-system';
  const metaById = new Map(workflows.map((meta) => [meta.id, meta]));

  function metaFor(workflowId: string): WorkflowMeta | undefined {
    return metaById.get(workflowId);
  }

  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    if (url.pathname !== '/workflows' && segments[0] !== 'runs') return null;

    try {
      const context = await resolve(request);
      if (!context) return json({ error: 'authentication required' }, 401);
      const actor = context.actor;

      if (request.method === 'GET' && url.pathname === '/workflows') {
        // The catalog echoes the AUTHENTICATED identity so the SPA renders
        // role gates from the server's view of the actor — a client that
        // guesses its own role from a local token table is fail-open (an
        // unknown token must not default to admin).
        return json({
          workflows,
          actor: {
            id: actor.id,
            role: actor.role,
            // Display hint: whether THIS caller may decide its own request.
            // The resolver built it — the DECIDER_ROLES guard plus the
            // deployment's SoD policy, fed the SAME allowSelfDecision the
            // service enforces — so the echo can only reflect the server's
            // decide() verdict. The SPA uses it to suppress a now-false "you
            // will be refused" hint. Enforced server-side regardless
            // (ApprovalService.decide).
            canSelfDecide: context.canSelfDecide(actor.role),
          },
        });
      }
      if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);

      const [, workflowId, runId, action] = segments;

      if (request.method === 'POST' && segments.length === 1) {
        if (!RUN_START_ROLES.includes(actor.role)) {
          return json({ error: 'forbidden' }, 403);
        }
        const body = (await readJson(request)) as StartBody | null;
        if (!body || typeof body.workflowId !== 'string') {
          return json({ error: 'workflowId is required' }, 400);
        }
        // A client may never choose the runId. 400 (not silent override) so a
        // caller pinning ids finds out.
        if (body.runId !== undefined) {
          return json({ error: 'runId is server-assigned' }, 400);
        }
        const meta = metaFor(body.workflowId);
        if (!meta) {
          return json({ error: `unknown workflow '${body.workflowId}'` }, 404);
        }
        // Per-workflow RBAC: a workflow may restrict who can START it — a finer
        // gate than the coarse "can start any run" check above.
        const { allowedRoles } = meta;
        if (allowedRoles && !allowedRoles.includes(actor.role)) {
          return json(
            {
              error: `role '${actor.role}' may not start '${body.workflowId}'`,
            },
            403,
          );
        }
        await options.beforeStart?.(context, body.workflowId, body.inputData);
        const ownedRunId = context.newRunId();
        const summary = await options.start({
          workflowId: body.workflowId,
          runId: ownedRunId,
          inputData: body.inputData,
          principal: context.principal,
          ...(body.deadlineMs === undefined
            ? {}
            : { deadlineMs: body.deadlineMs as number }),
        });
        if (summary.status !== 'suspended') return json(summary);
        let approvals: ApprovalRecord[] = [];
        try {
          approvals = await queueApprovalForSuspension(
            context.service(),
            body.workflowId,
            summary,
            actor.id,
            systemPrincipalId,
          );
        } catch (error) {
          console.error(
            JSON.stringify({
              type: 'approval-filing-error',
              workflowId: body.workflowId,
              runId: summary.runId,
              error: error instanceof Error ? error.message : String(error),
            }),
          );
        }
        // `approval` remains the single-gate response contract (what the SPA
        // links); `approvals` carries every record a parallel multi-step
        // suspension filed, so no gate is invisible to the caller.
        return json({ ...summary, approval: approvals[0], approvals });
      }

      // Status, resume, and terminate validate the workflow against the
      // catalog: the start route already did, and a passthrough that did not
      // would answer for workflow ids this host never registered.
      if (workflowId && runId && !metaFor(workflowId)) {
        return json({ error: `unknown workflow '${workflowId}'` }, 404);
      }

      // Ownership is resolved before role gates or runner access. A missing id
      // and a resource owned by another principal are the same 404.
      const isTerminate =
        request.method === 'POST' &&
        segments.length === 4 &&
        action === 'terminate' &&
        options.terminate !== undefined;
      let replayOnly = false;
      if (workflowId && runId && segments.length >= 3) {
        try {
          await requireResourceAccess(
            context,
            'run',
            runId,
            request.method === 'GET' ? 'read' : 'write',
            'run',
          );
        } catch (error) {
          if (
            !isTerminate ||
            !(error instanceof RunRouteError) ||
            error.status !== 404
          ) {
            throw error;
          }
          // Ownership is deliberately gone after terminal cleanup. Only the
          // owner DO may authorize this replay against persisted exact
          // principals; every other case remains the same opaque 404.
          replayOnly = true;
        }
      }

      if (request.method === 'POST' && !RUN_START_ROLES.includes(actor.role)) {
        return json({ error: 'forbidden' }, 403);
      }

      // Per-workflow RBAC, mirrored from the start route: a module that
      // narrows who may START a workflow also narrows who may ADVANCE it —
      // resume drives the same state machine. Hoisted above dispatch so a
      // future mutating route cannot forget it; GETs stay coarse
      // (reviewer/viewer inspect runs they may not drive). Placed after the
      // catalog 404 above so an unknown workflow does not become a role oracle.
      if (request.method !== 'GET' && workflowId) {
        const allowedRoles = metaFor(workflowId)?.allowedRoles;
        if (allowedRoles && !allowedRoles.includes(actor.role)) {
          return json(
            { error: `role '${actor.role}' may not advance '${workflowId}'` },
            403,
          );
        }
      }

      if (
        request.method === 'GET' &&
        segments.length === 3 &&
        workflowId &&
        runId
      ) {
        const summary = await options.status(workflowId, runId);
        if (!summary) return json({ error: 'run not found' }, 404);
        if (summary.status === 'suspended' && options.reconcileApprovals) {
          try {
            await options.reconcileApprovals(context, workflowId, summary);
          } catch (error) {
            console.error(
              JSON.stringify({
                type: 'reconcile-error',
                workflowId,
                runId,
                error: error instanceof Error ? error.message : String(error),
              }),
            );
          }
        }
        return json(summary);
      }

      if (isTerminate && workflowId && runId && options.terminate) {
        return json(
          await options.terminate(
            workflowId,
            runId,
            context.principal,
            replayOnly,
          ),
        );
      }

      if (
        request.method === 'POST' &&
        segments.length === 4 &&
        action === 'resume' &&
        workflowId &&
        runId
      ) {
        const body = (await readJson(request)) ?? {};
        await options.beforeResume?.(context, workflowId, runId, body);
        return json(
          await options.resume(
            workflowId,
            runId,
            body,
            context.principal.id,
            'human',
          ),
        );
      }

      return json({ error: 'not found' }, 404);
    } catch (error) {
      return errorResponse(error);
    }
  };
}
