// HTTP surface for the approval queue (flowsafe-architecture.md endpoint
// table). Plain fetch routing like the DO runner — no HTTP framework.
//
// The router returns null for paths outside its base so a host Worker can
// compose it ahead of its other routes. Authentication is injected: the
// authenticate option maps a Request to the acting principal and is part of
// the trusted computing base (it asserts identity; the service enforces
// roles). No actor -> 401 before any service call.
//
// The create route is OFF by default (allowCreate) and, when mounted, cannot
// author capability: it rejects every TCB_ONLY_CREATE_FIELDS member and
// force-attributes the request to the authenticated actor. Approval records
// that carry grants are minted in-process from an observed suspension, never
// from a request body.

import type { ApprovalActor } from './contract.js';
import {
  ApprovalAuthzError,
  ApprovalConflictError,
  type ApprovalService,
  InvalidApprovalInputError,
  UnknownApprovalError,
} from './service.js';
import {
  APPROVAL_STATUSES,
  type ApprovalDecision,
  type ApprovalListFilter,
  type ApprovalStatus,
  type CreateApprovalInput,
} from './types.js';

/**
 * Fields on CreateApprovalInput that select CAPABILITY or ATTRIBUTION, and so
 * belong to the trusted computing base alone (security-threat-model.md, trust
 * boundary 6). A request body that names any of them is rejected outright:
 *
 * - `connectors` IS the minted grant — an approved record's connectors become
 *   the requestContext grant the write gate checks.
 * - `runScoped` turns a step-less record into a standing grant on every leg.
 * - `stepPath`, `suspendedAt`, `resumedAt`, `resumeCount` select WHICH leg a
 *   grant mints on. A step-keyed body with no `suspendedAt` would fall into
 *   grants.ts's legacy decidedAt-after fallback and mint, so rejecting
 *   `connectors` alone is insufficient — reject the whole set.
 * - `requestedBy` is the field decide()'s separation-of-duties check compares
 *   against; spoofing it lets one principal approve their own request.
 */
export const TCB_ONLY_CREATE_FIELDS = [
  'connectors',
  'stepPath',
  'suspendedAt',
  'resumedAt',
  'resumeCount',
  'runScoped',
  'requestedBy',
] as const;

export interface ApprovalRouterOptions {
  service: ApprovalService;
  /**
   * Maps the request to the acting principal (session, JWT, API key —
   * deployment-specific). Returning undefined yields 401.
   */
  authenticate: (
    request: Request,
  ) => ApprovalActor | undefined | Promise<ApprovalActor | undefined>;
  /** Route prefix. Default: '/api/approvals'. */
  basePath?: string;
  /**
   * Mount `POST <basePath>` (create). Default false — every first-party host
   * creates records in-process from an observed suspension (host-kit's
   * approval bridge), so the HTTP route is an inert "file a request"
   * affordance at best. When enabled it force-sets `requestedBy` to the
   * authenticated actor and 400s on any TCB_ONLY_CREATE_FIELDS member, so it
   * can never author capability.
   */
  allowCreate?: boolean;
}

export type ApprovalRouter = (request: Request) => Promise<Response | null>;

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function errorResponse(error: unknown): Response {
  if (error instanceof InvalidApprovalInputError) {
    return json({ error: error.message }, 400);
  }
  if (error instanceof ApprovalAuthzError) {
    return json({ error: error.message }, 403);
  }
  if (error instanceof UnknownApprovalError) {
    return json({ error: error.message }, 404);
  }
  if (error instanceof ApprovalConflictError) {
    return json(
      { error: error.message, currentStatus: error.currentStatus },
      409,
    );
  }
  const message = error instanceof Error ? error.message : String(error);
  return json({ error: message }, 500);
}

async function readJsonObject(
  request: Request,
): Promise<Record<string, unknown>> {
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    throw new InvalidApprovalInputError('request body must be JSON');
  }
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    throw new InvalidApprovalInputError('request body must be a JSON object');
  }
  return body as Record<string, unknown>;
}

function parseListFilter(url: URL): ApprovalListFilter {
  const filter: ApprovalListFilter = {};
  const status = url.searchParams.get('status');
  if (status !== null) {
    const statuses = status.split(',').map((value) => value.trim());
    for (const value of statuses) {
      if (!(APPROVAL_STATUSES as readonly string[]).includes(value)) {
        throw new InvalidApprovalInputError(
          `unknown status '${value}' (expected one of [${APPROVAL_STATUSES.join(', ')}])`,
        );
      }
    }
    filter.status = statuses as ApprovalStatus[];
  }
  const workflowId = url.searchParams.get('workflowId');
  if (workflowId !== null) filter.workflowId = workflowId;
  const runId = url.searchParams.get('runId');
  if (runId !== null) filter.runId = runId;
  const claimedBy = url.searchParams.get('claimedBy');
  if (claimedBy !== null) filter.claimedBy = claimedBy;
  return filter;
}

export function createApprovalRouter(
  options: ApprovalRouterOptions,
): ApprovalRouter {
  const { service, authenticate } = options;
  const basePath = options.basePath ?? '/api/approvals';
  const allowCreate = options.allowCreate ?? false;

  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url);
    if (url.pathname !== basePath && !url.pathname.startsWith(`${basePath}/`)) {
      return null;
    }
    const segments = url.pathname
      .slice(basePath.length)
      .split('/')
      .filter(Boolean);

    try {
      const actor = await authenticate(request);
      if (!actor) return json({ error: 'authentication required' }, 401);

      if (request.method === 'GET') {
        if (segments.length === 0) {
          return json(await service.list(parseListFilter(url), actor));
        }
        if (segments.length === 1 && segments[0] === 'metrics') {
          return json(await service.metrics(actor));
        }
        if (segments.length === 1 && segments[0]) {
          return json(await service.get(segments[0], actor));
        }
      }

      if (request.method === 'POST') {
        if (segments.length === 0) {
          if (!allowCreate) return json({ error: 'not found' }, 404);
          const body = await readJsonObject(request);
          for (const field of TCB_ONLY_CREATE_FIELDS) {
            if (field in body) {
              throw new InvalidApprovalInputError(
                `${field} may not be set over HTTP`,
              );
            }
          }
          const { record, created } = await service.create(
            // Attribution is the authenticated identity, never the body:
            // service.create still honours input.requestedBy for the
            // in-process bridge (which legitimately attributes the human who
            // advanced the run), so the tightening lives here at the HTTP
            // boundary alone.
            {
              ...body,
              requestedBy: actor.id,
            } as unknown as CreateApprovalInput,
            actor,
          );
          return json(record, created ? 201 : 200);
        }
        if (
          segments.length === 2 &&
          segments[0] === 'sla' &&
          segments[1] === 'sweep'
        ) {
          return json({ escalated: await service.sweepSLA(actor) });
        }
        const [id, action] = segments;
        if (segments.length === 2 && id && action === 'claim') {
          return json(await service.claim(id, actor));
        }
        if (segments.length === 2 && id && action === 'decide') {
          const body = await readJsonObject(request);
          return json(
            await service.decide(
              id,
              {
                decision: body.decision as ApprovalDecision,
                comment: body.comment as string | undefined,
              },
              actor,
            ),
          );
        }
        if (segments.length === 2 && id && action === 'delegate') {
          const body = await readJsonObject(request);
          return json(
            await service.delegate(id, { to: body.to as string }, actor),
          );
        }
      }

      return json({ error: 'not found' }, 404);
    } catch (error) {
      return errorResponse(error);
    }
  };
}
