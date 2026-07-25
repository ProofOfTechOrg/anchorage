// SPDX-License-Identifier: Apache-2.0
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

import {
  ApprovalAuthzError,
  ApprovalConflictError,
  InvalidApprovalInputError,
  UnknownApprovalError,
} from './service.js';
import {
  TenantResolutionError,
  type TenantResolver,
} from './tenant-context.js';
import {
  APPROVAL_LIST_ORDERS,
  APPROVAL_STATUSES,
  type ApprovalDecision,
  type ApprovalListFilter,
  type ApprovalListOrder,
  type ApprovalStatus,
  approvalListOrder,
  type CreateApprovalInput,
  MAX_APPROVAL_LIST_LIMIT,
  parseApprovalCursor,
  parseApprovalTimeBound,
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
  'resumeTarget',
] as const;

/**
 * The complement: what a request body MAY set. The create route copies ONLY
 * these fields off the body — an allowlist, so a field added to
 * CreateApprovalInput later is INERT over HTTP until it is deliberately
 * classified here (fail closed by construction; a body spread would instead
 * hand every future field to service.create unless someone remembered to
 * extend the denylist above). TCB_ONLY_CREATE_FIELDS stays as the
 * 400-with-a-reason layer; router.test.ts pins at the type level that the two
 * lists exactly cover CreateApprovalInput.
 */
export const CLIENT_CREATE_FIELDS = [
  'workflowId',
  'runId',
  'title',
  'summary',
  'payload',
  'priority',
  'slaSeconds',
] as const satisfies readonly Exclude<
  keyof CreateApprovalInput,
  (typeof TCB_ONLY_CREATE_FIELDS)[number]
>[];

export interface ApprovalRouterOptions {
  /**
   * Authenticates the request and binds the tenant-scoped service (INV-2).
   * The router's first line is `resolve(request)`; the store the service
   * wraps is constructed AFTER authentication, bound to the actor's tenant —
   * there is no pre-auth service to leak through. undefined yields 401.
   */
  resolve: TenantResolver;
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
  if (error instanceof TenantResolutionError) {
    // An authenticated actor with a malformed tenant claim is a verifier
    // bug, not a client 4xx it can fix — but it must not become a 500 that
    // reads as "try again". Forbidden, with the reason in the body.
    return json({ error: 'forbidden' }, 403);
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
  const requestedBy = url.searchParams.get('requestedBy');
  if (requestedBy !== null) filter.requestedBy = requestedBy;
  // Eager 400 on unparseable time bounds (the cursor convention):
  // parseApprovalTimeBound is the exact gate both stores re-apply.
  for (const field of ['createdBefore', 'createdAfter'] as const) {
    const value = url.searchParams.get(field);
    if (value === null) continue;
    try {
      parseApprovalTimeBound(value, field);
    } catch (error) {
      throw new InvalidApprovalInputError(
        error instanceof Error ? error.message : String(error),
      );
    }
    filter[field] = value;
  }
  const limit = url.searchParams.get('limit');
  if (limit !== null) {
    const parsed = Number(limit);
    if (
      !Number.isInteger(parsed) ||
      parsed < 1 ||
      parsed > MAX_APPROVAL_LIST_LIMIT
    ) {
      throw new InvalidApprovalInputError(
        `limit must be an integer between 1 and ${MAX_APPROVAL_LIST_LIMIT} (got '${limit}')`,
      );
    }
    filter.limit = parsed;
  }
  const after = url.searchParams.get('after');
  if (after !== null) {
    // Validate eagerly so a malformed cursor is a 400 here, not a 500 (or a
    // silently-empty page) once it reaches the store — parseApprovalCursor
    // is the one shared decoder (types.ts), so this is the exact check
    // store.ts/d1-store.ts will apply again when they actually page with it.
    try {
      parseApprovalCursor(after);
    } catch {
      throw new InvalidApprovalInputError(
        `after is not a valid approval cursor (got '${after}')`,
      );
    }
    filter.after = after;
  }
  const orderBy = url.searchParams.get('orderBy');
  if (orderBy !== null) {
    if (!(APPROVAL_LIST_ORDERS as readonly string[]).includes(orderBy)) {
      throw new InvalidApprovalInputError(
        `unknown orderBy '${orderBy}' (expected one of [${APPROVAL_LIST_ORDERS.join(', ')}])`,
      );
    }
    filter.orderBy = orderBy as ApprovalListOrder;
  }
  // Same eager-validation rationale as the cursor above: the reviewer/after
  // incoherence must 400 here, not surface as a store throw mapped to 500 —
  // approvalListOrder is the one shared rule the stores re-apply.
  try {
    approvalListOrder(filter);
  } catch (error) {
    throw new InvalidApprovalInputError(
      error instanceof Error ? error.message : String(error),
    );
  }
  return filter;
}

export function createApprovalRouter(
  options: ApprovalRouterOptions,
): ApprovalRouter {
  const { resolve } = options;
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
      const tenant = await resolve(request);
      if (!tenant) return json({ error: 'authentication required' }, 401);
      const actor = tenant.actor;
      const service = tenant.service();

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
          // Attribution is the authenticated identity, never the body:
          // service.create still honours input.requestedBy for the
          // in-process bridge (which legitimately attributes the human who
          // advanced the run), so the tightening lives here at the HTTP
          // boundary alone.
          const input: Record<string, unknown> = { requestedBy: actor.id };
          for (const field of CLIENT_CREATE_FIELDS) {
            if (field in body) input[field] = body[field];
          }
          const { record, created } = await service.create(
            input as unknown as CreateApprovalInput,
            actor,
          );
          return json(record, created ? 201 : 200);
        }
        // NOTE: there is deliberately NO /sla/sweep route. The sweep is an
        // unfiltered cross-tenant read+write; it lives in the cron-owned
        // sweepSLA() function over a SystemApprovalStore, unreachable from
        // request scope by type.
        //
        // Batch decide matches BEFORE the generic [id, action] arms — those
        // would otherwise resolve this path as decide('batch') and 404.
        // Record ids are server-minted UUIDs, so 'batch' can never collide
        // with a real id. Always mounted: it is decide fan-out, not create.
        if (
          segments.length === 2 &&
          segments[0] === 'batch' &&
          segments[1] === 'decide'
        ) {
          const body = await readJsonObject(request);
          return json(
            await service.decideBatch(
              body.ids as string[],
              {
                decision: body.decision as ApprovalDecision,
                comment: body.comment as string | undefined,
              },
              actor,
            ),
          );
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
