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
//   3. per-workflow meta.allowedRoles      -> 403   (a module may narrow further)
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
  type ApprovalActor,
  RUN_START_ROLES,
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
   * Authenticates the request and binds the tenant scope (INV-2): the
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

  function metaFor(workflowId: string): WorkflowMeta | undefined {
    return workflows.find((candidate) => candidate.id === workflowId);
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
          actor: { id: actor.id, role: actor.role, tenantId: actor.tenantId },
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
        const systemActor: ApprovalActor = {
          id: systemActorId,
          role: 'operator',
          tenantId: tenant.tenantId,
        };
        const record = await queueApprovalForSuspension(
          tenant.service(),
          body.workflowId,
          summary,
          actor.id,
          systemActor,
        );
        return json({ ...summary, approval: record });
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

      if (
        request.method === 'GET' &&
        segments.length === 3 &&
        workflowId &&
        runId
      ) {
        const summary = await options.status(workflowId, runId);
        return summary ? json(summary) : json({ error: 'run not found' }, 404);
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
