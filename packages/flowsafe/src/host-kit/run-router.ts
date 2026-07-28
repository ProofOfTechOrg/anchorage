// SPDX-License-Identifier: Apache-2.0
// HTTP surface for the run catalog + run lifecycle, shared by every host.
//
// Mirrors createApprovalRouter's contract — plain fetch routing, an injected
// `authenticate`, and `null` for paths outside its ownership so a host Worker
// can compose it after the approval router. (Its two paths are fixed rather
// than configurable: unlike the approval surface, a host mounts exactly one run
// surface.) What it owns that the hosts used to triplicate is the AUTHORIZATION
// order:
//
//   1. authenticate                        -> 401
//   2. any POST, coarse RUN_START_ROLES    -> 403   (reviewer/viewer are read-only)
//   3. per-workflow meta.allowedRoles      -> 403   (a module may narrow further;
//                                                    applied to EVERY mutating
//                                                    route — start via the body's
//                                                    workflowId, resume and any
//                                                    future POST via the path's)
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
  RUN_START_ROLES,
  type TenantContext,
  TenantResolutionError,
  type TenantResolver,
} from '../approval-api/index.js';
import {
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunNotSuspendedError,
  type RunSummary,
  UnknownRunError,
  UnknownWorkflowError,
} from '../do-runner/index.js';
import { queueApprovalForSuspension } from './approval-bridge.js';
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
   * Authenticates the request and binds the tenant scope: the
   * approval service, the runId mint, and the ownership predicate all come
   * from the resolved TenantContext. undefined yields 401.
   */
  resolve: TenantResolver;
  /**
   * Creator identity for bridge-queued approval records. Only the id is
   * configurable — the role is 'operator' (create-capable) and the tenant is
   * ALWAYS the request's resolved tenant, so the record lands in the same
   * tenant as the run it gates. Must differ from human actor ids or the
   * separation-of-duties check can never fire. Default: 'flowsafe-system'.
   */
  systemActorId?: string;
  /** Host topology: in-process runtime, or a DO stub fetch. */
  start: (
    workflowId: string,
    runId: string,
    inputData: unknown,
  ) => Promise<RunSummary>;
  status: (
    workflowId: string,
    runId: string,
  ) => Promise<RunSummary | undefined>;
  resume: (
    workflowId: string,
    runId: string,
    body: unknown,
  ) => Promise<RunSummary>;
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
   * reconcileApprovalsOnStatus(systemActorId) here, optionally wrapped for
   * waitUntil-detachment.
   */
  reconcileApprovals?: (
    tenant: TenantContext,
    workflowId: string,
    summary: RunSummary,
  ) => Promise<void>;
}

export type RunRouter = (request: Request) => Promise<Response | null>;

interface StartBody {
  workflowId?: string;
  runId?: string;
  inputData?: unknown;
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
    return json({ error: error.message }, error.status);
  }
  if (error instanceof TenantResolutionError) {
    // Authenticated but with a malformed tenant claim — a verifier bug,
    // surfaced as forbidden rather than a retryable 500.
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
    error instanceof RunAlreadyExistsError
  ) {
    return json({ error: error.message }, 409);
  }
  if (error instanceof InvalidRunRequestError) {
    return json({ error: error.message }, 400);
  }
  return json(
    { error: error instanceof Error ? error.message : String(error) },
    500,
  );
}

async function readJson(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    return null;
  }
}

export function createRunRouter(options: RunRouterOptions): RunRouter {
  const { workflows, resolve } = options;
  const systemActorId = options.systemActorId ?? 'flowsafe-system';
  const metaById = new Map(workflows.map((meta) => [meta.id, meta]));

  function metaFor(workflowId: string): WorkflowMeta | undefined {
    return metaById.get(workflowId);
  }

  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    if (url.pathname !== '/workflows' && segments[0] !== 'runs') return null;

    try {
      // Resolution is authentication + INV-3 validation + tenant binding in
      // one step (tenant-context.ts): a malformed tenant claim throws (403
      // below), never concatenates into a runId or an ownership prefix.
      const tenant = await resolve(request);
      if (!tenant) return json({ error: 'authentication required' }, 401);
      const actor = tenant.actor;

      // Coarse gate: reviewers and viewers may inspect runs, never advance
      // them. Applied before any route so a new POST route cannot forget it.
      if (request.method === 'POST' && !RUN_START_ROLES.includes(actor.role)) {
        return json({ error: 'forbidden' }, 403);
      }

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
            tenantId: actor.tenantId,
            // Display hint: whether THIS caller may decide its own request.
            // The resolver built it — the DECIDER_ROLES guard plus the
            // deployment's SoD policy, fed the SAME allowSelfDecision the
            // service enforces — so the echo can only reflect the server's
            // decide() verdict. The SPA uses it to suppress a now-false "you
            // will be refused" hint. Enforced server-side regardless
            // (ApprovalService.decide).
            canSelfDecide: tenant.canSelfDecide(actor.role),
          },
        });
      }
      if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);

      const [, workflowId, runId, action] = segments;

      if (request.method === 'POST' && segments.length === 1) {
        const body = (await readJson(request)) as StartBody | null;
        if (!body || typeof body.workflowId !== 'string') {
          return json({ error: 'workflowId is required' }, 400);
        }
        // INV-1: the runId IS the tenant carrier; a client may never choose
        // it. 400 (not silent override) so a caller pinning ids finds out.
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
        // The router mints the tenant-salted runId and hands it to the host,
        // so a DO-routing host has its instance key up front.
        const summary = await options.start(
          body.workflowId,
          tenant.newRunId(),
          body.inputData,
        );
        if (summary.status !== 'suspended') return json(summary);
        const approvals = await queueApprovalForSuspension(
          tenant.service(),
          body.workflowId,
          summary,
          actor.id,
          systemActorId,
        );
        // `approval` remains the single-gate response contract (what the SPA
        // links); `approvals` carries every record a parallel multi-step
        // suspension filed, so no gate is invisible to the caller.
        return json({ ...summary, approval: approvals[0], approvals });
      }

      // Status and resume both validate the workflow against the catalog: the
      // start route already did, and a passthrough that did not would answer
      // for workflow ids this host never registered.
      if (workflowId && runId && !metaFor(workflowId)) {
        return json({ error: `unknown workflow '${workflowId}'` }, 404);
      }

      // Ownership: attribution is not authorization. A run belongs to the
      // tenant its runId carries; any other tenant gets 404 — not 403, so the
      // route is not an existence oracle for other tenants' runIds.
      if (
        workflowId &&
        runId &&
        segments.length >= 3 &&
        !tenant.ownsRun(runId)
      ) {
        return json({ error: 'run not found' }, 404);
      }

      // Per-workflow RBAC, mirrored from the start route: a module that
      // narrows who may START a workflow also narrows who may ADVANCE it —
      // resume drives the same state machine. Hoisted above dispatch so a
      // future mutating route cannot forget it; GETs stay coarse
      // (reviewer/viewer inspect runs they may not drive). Placed after the
      // 404s above so another tenant's probe learns nothing it could not read
      // from its own /workflows catalog.
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
            await options.reconcileApprovals(tenant, workflowId, summary);
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

      if (
        request.method === 'POST' &&
        segments.length === 4 &&
        action === 'resume' &&
        workflowId &&
        runId
      ) {
        return json(
          await options.resume(
            workflowId,
            runId,
            (await readJson(request)) ?? {},
          ),
        );
      }

      return json({ error: 'not found' }, 404);
    } catch (error) {
      return errorResponse(error);
    }
  };
}
