// SPDX-License-Identifier: Apache-2.0

import {
  ActorResolutionError,
  type ActorResolver,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/index.js';
import { RunRouteError, requireResourceAccess } from '../host-kit/index.js';
import { readBoundedBody } from '../http-body.js';
import { createAgentCatalog } from './catalog.js';
import type { AgentThreadTopology } from './thread-topology.js';
import type { AgentMeta } from './types.js';

const MAX_BODY_BYTES = 16_384;
const MAX_PROMPT_CODE_UNITS = 10_000;

export interface AgentRouterOptions {
  agents: readonly AgentMeta[];
  resolve: ActorResolver;
  topology: AgentThreadTopology;
}

export type AgentRouter = (request: Request) => Promise<Response | null>;

type MatchedRoute =
  | { kind: 'catalog'; allow: 'GET' }
  | { kind: 'start'; allow: 'POST'; agentId: string }
  | {
      kind: 'status';
      allow: 'GET';
      agentId: string;
      threadId: string;
      runId: string;
    }
  | {
      kind: 'stream';
      allow: 'GET';
      agentId: string;
      threadId: string;
      runId: string;
    }
  | { kind: 'not-found' };

function json(payload: unknown, status = 200, allow?: string): Response {
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    'cache-control': 'no-store',
    'x-content-type-options': 'nosniff',
  };
  if (allow) headers.allow = allow;
  return new Response(JSON.stringify(payload), { status, headers });
}

function decoded(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  try {
    return decodeURIComponent(value);
  } catch {
    return undefined;
  }
}

function match(url: URL): MatchedRoute | undefined {
  const segments = url.pathname.split('/').filter(Boolean);
  if (segments[0] !== 'agents') return undefined;
  if (segments.length === 1) return { kind: 'catalog', allow: 'GET' };
  const agentId = decoded(segments[1]);
  if (agentId === undefined) return { kind: 'not-found' };
  if (segments.length === 3 && segments[2] === 'runs') {
    return { kind: 'start', allow: 'POST', agentId };
  }
  const threadId = decoded(segments[3]);
  const runId = decoded(segments[4]);
  if (segments[2] === 'runs' && threadId !== undefined && runId !== undefined) {
    if (segments.length === 5) {
      return {
        kind: 'status',
        allow: 'GET',
        agentId,
        threadId,
        runId,
      };
    }
    if (segments.length === 6 && segments[5] === 'stream') {
      return {
        kind: 'stream',
        allow: 'GET',
        agentId,
        threadId,
        runId,
      };
    }
  }
  return { kind: 'not-found' };
}

async function readStartBody(
  request: Request,
): Promise<{ prompt: string } | Response> {
  const rawBody = await readBoundedBody(
    request,
    MAX_BODY_BYTES,
    'agent start body exceeds limit',
  );
  if (!rawBody.ok && rawBody.reason === 'payload-too-large') {
    return json({ error: 'payload too large' }, 413);
  }
  if (!rawBody.ok) {
    return json({ error: 'a JSON object body is required' }, 400);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawBody.text);
  } catch {
    return json({ error: 'a JSON object body is required' }, 400);
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return json({ error: 'a JSON object body is required' }, 400);
  }
  const body = parsed as Record<string, unknown>;
  const fields = Object.keys(body);
  const forbidden = fields.find((field) => field !== 'prompt');
  if (forbidden !== undefined) {
    return json({ error: `field '${forbidden}' is not allowed` }, 400);
  }
  if (
    fields.length !== 1 ||
    typeof body.prompt !== 'string' ||
    body.prompt.trim() === '' ||
    body.prompt.length > MAX_PROMPT_CODE_UNITS
  ) {
    return json(
      {
        error:
          'prompt is required and must be a non-whitespace string of at most 10000 characters',
      },
      400,
    );
  }
  return { prompt: body.prompt };
}

function offsetFor(url: URL): number | Response {
  for (const key of url.searchParams.keys()) {
    if (key !== 'offset') {
      return json({ error: `query field '${key}' is not allowed` }, 400);
    }
  }
  const values = url.searchParams.getAll('offset');
  if (values.length === 0) return 0;
  const value = values[0];
  if (
    values.length !== 1 ||
    value === undefined ||
    !/^(?:0|[1-9]\d*)$/.test(value)
  ) {
    return json({ error: 'offset must be a nonnegative safe integer' }, 400);
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed)
    ? parsed
    : json({ error: 'offset must be a nonnegative safe integer' }, 400);
}

function internalError(error: unknown, route: MatchedRoute): Response {
  if (error instanceof ActorResolutionError) {
    return json({ error: 'forbidden' }, 403);
  }
  if (error instanceof RunRouteError) {
    if (error.status < 500) return json({ error: error.message }, error.status);
  }
  console.error(
    JSON.stringify({
      type: 'agent-route-error',
      route: route.kind,
      reason: error instanceof Error ? error.message : String(error),
    }),
  );
  return json({ error: 'internal server error' }, 500);
}

export function createAgentRouter(options: AgentRouterOptions): AgentRouter {
  const catalog = createAgentCatalog(options.agents);
  return async (request) => {
    const url = new URL(request.url);
    const route = match(url);
    if (route === undefined) return null;
    if (route.kind === 'not-found') return json({ error: 'not found' }, 404);

    try {
      const context = await options.resolve(request);
      if (!context) return json({ error: 'authentication required' }, 401);

      if (route.kind === 'catalog') {
        if (request.method !== route.allow) {
          return json({ error: 'method not allowed' }, 405, route.allow);
        }
        return json({
          agents: catalog.agents,
          actor: {
            id: context.actor.id,
            role: context.actor.role,
            canSelfDecide: context.canSelfDecide(context.actor.role),
          },
        });
      }

      const meta = catalog.get(route.agentId);
      if (!meta) return json({ error: 'agent not found' }, 404);

      if (route.kind === 'start') {
        if (request.method !== route.allow) {
          return json({ error: 'method not allowed' }, 405, route.allow);
        }
        const roles = catalog.allowedRoles(route.agentId);
        if (
          !RUN_START_ROLES.includes(context.actor.role) ||
          !roles?.includes(context.actor.role)
        ) {
          return json({ error: 'forbidden' }, 403);
        }
        const body = await readStartBody(request);
        if (body instanceof Response) return body;
        return json(
          await options.topology.start(context, {
            agentId: route.agentId,
            prompt: body.prompt,
            entryPath: 'http.start',
          }),
        );
      }

      if (!isPathSafeId(route.threadId) || !isPathSafeId(route.runId)) {
        return json({ error: 'run not found' }, 404);
      }

      await requireResourceAccess(
        context,
        'thread',
        route.threadId,
        'read',
        'run',
      );
      await requireResourceAccess(context, 'run', route.runId, 'read', 'run');

      const resolved = await options.topology.status(context, route);
      if (!resolved) return json({ error: 'run not found' }, 404);
      if (request.method !== route.allow) {
        return json({ error: 'method not allowed' }, 405, route.allow);
      }

      if (route.kind === 'status') {
        if (url.search !== '') {
          return json({ error: 'query fields are not allowed' }, 400);
        }
        return json(resolved);
      }

      const offset = offsetFor(url);
      if (offset instanceof Response) return offset;
      return await options.topology.observe(context, { ...route, offset });
    } catch (error) {
      return internalError(error, route);
    }
  };
}
