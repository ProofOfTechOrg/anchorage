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
// force-attributes the request to the authenticated actor. It also requires
// write access to the named run before touching the approval store. Approval
// records that carry grants are minted in-process from an observed suspension,
// never from a request body.

import { isExecutionFenceRefusal } from '../do-runner/execution-fence.js';
import { isPathSafeId } from '../do-runner/path-safe-id.js';
import { readBoundedBody } from '../http-body.js';
import { ActorResolutionError, type ActorResolver } from './actor-context.js';
import {
  ApprovalAuthzError,
  ApprovalConflictError,
  InvalidApprovalInputError,
  UnknownApprovalError,
} from './service.js';
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
 * - `grantScope` is persisted capability metadata. It is record-only, but is
 *   named here so an attempted body field is rejected rather than ignored.
 * - `toolCallId` narrows a durable-agent grant to one persisted tool call.
 * - `runScoped` turns a step-less record into a standing grant on every leg.
 * - `stepPath`, `suspendedAt`, `resumedAt`, `resumeCount` select WHICH leg a
 *   grant mints on. Rejecting `connectors` alone is insufficient because a
 *   future trusted merge must never inherit client-selected identity.
 * - `requestedBy` is the field decide()'s separation-of-duties check compares
 *   against; spoofing it lets one principal approve their own request.
 * - `requestedByKind` distinguishes a human requester from automation with
 *   the same id and is therefore equally trust-sensitive.
 */
export const TCB_ONLY_CREATE_FIELDS = [
  'connectors',
  'grantScope',
  'toolCallId',
  'stepPath',
  'suspendedAt',
  'resumedAt',
  'resumeCount',
  'runScoped',
  'requestedBy',
  'requestedByKind',
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
   * Authenticates the request before exposing the service. undefined yields
   * 401.
   */
  resolve: ActorResolver;
  /** Route prefix. Default: '/api/approvals'. */
  basePath?: string;
  /**
   * Mount `POST <basePath>` (create). Default false — every first-party host
   * creates records in-process from an observed suspension (host-kit's
   * approval bridge), so the HTTP route is an inert "file a request"
   * affordance at best. When enabled it force-sets `requestedBy` to the
   * authenticated actor and 400s on any TCB_ONLY_CREATE_FIELDS member, so it
   * can never author capability. The actor must also have write access to the
   * named run; missing and foreign runs both yield 404.
   */
  allowCreate?: boolean;
}

export type ApprovalRouter = (request: Request) => Promise<Response | null>;

class ApprovalPayloadTooLargeError extends Error {}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function errorResponse(error: unknown): Response {
  // The deployment execution fence refusing, or failing to answer. 503 with
  // its reason code, never the generic 500 below: a decision refused because
  // this deployment is being migrated is retryable, and a reviewer's client
  // must be able to tell that from a broken queue.
  if (isExecutionFenceRefusal(error)) {
    return json({ error: error.message, reason: error.reason }, error.status);
  }
  if (error instanceof ApprovalPayloadTooLargeError) {
    return json({ error: error.message }, 413);
  }
  if (error instanceof InvalidApprovalInputError) {
    return json({ error: error.message }, 400);
  }
  if (error instanceof ActorResolutionError) {
    // Invalid authenticated claims are a verifier bug, not a client 4xx it
    // can fix. Fail closed without echoing claim details.
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
  const raw = await readBoundedBody(
    request,
    1_048_576,
    'approval body exceeds limit',
  );
  if (!raw.ok && raw.reason === 'payload-too-large') {
    throw new ApprovalPayloadTooLargeError('payload too large');
  }
  if (!raw.ok) {
    throw new InvalidApprovalInputError('request body must be JSON');
  }
  let body: unknown;
  try {
    body = JSON.parse(raw.text);
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
      const context = await resolve(request);
      if (!context) return json({ error: 'authentication required' }, 401);
      const actor = context.actor;
      const service = context.service();

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
          const input: Record<string, unknown> = {
            requestedBy: actor.id,
            requestedByKind: 'human',
          };
          for (const field of CLIENT_CREATE_FIELDS) {
            if (field in body) input[field] = body[field];
          }
          const runId = input.runId;
          if (typeof runId !== 'string' || runId.length === 0) {
            throw new InvalidApprovalInputError('runId is required');
          }
          if (!isPathSafeId(runId)) {
            throw new InvalidApprovalInputError(
              `runId '${runId}' is not path-safe — approvals bind to server-minted runs`,
            );
          }
          // The optional public filing route may only attach a request to a
          // run the authenticated principal may advance. Resolve ownership
          // before touching the approval store: an unregistered id and a
          // foreign id are the same 404, so this cannot become a run oracle.
          if (!(await context.canAccessResource('run', runId, 'write'))) {
            return json({ error: 'run not found' }, 404);
          }
          const { record, created } = await service.create(
            input as unknown as CreateApprovalInput,
            actor,
          );
          return json(record, created ? 201 : 200);
        }
        // NOTE: there is deliberately no /sla/sweep route. Maintenance calls
        // sweepSLA() from trusted host code.
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
