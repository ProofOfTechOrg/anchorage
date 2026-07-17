// SPDX-License-Identifier: Apache-2.0
// Track B (M-003): the READ-ONLY, tenant-bound HTTP surface over background
// tasks (DL-014). Background-task rows carry tenant-prefixed run_id / threadId /
// resourceId (INV-1 + minted memory ids), but core's `listTasks` / `getTask` /
// `stream` are NOT tenant-bound — exposing the raw manager would be a
// cross-tenant existence/content oracle. So this router:
//   - REQUIRES a `runId` or `threadId` filter on list AND stream, and validates
//     its salted prefix against the authenticated tenant (400 if absent, 404 if
//     foreign — no oracle);
//   - loads a task for `getTask` and 404s if its `runId` is not tenant-owned,
//     whether the task is missing or another tenant's (one response, no oracle);
//   - never returns or accepts the raw BackgroundTaskManager.
//
// The hosting DO passes the already-authenticated tenant (its own idFromName
// identity), exactly as the run/thread DOs recover theirs. Mutating the queue
// (dispatch/cancel/resume) is NOT here: v1 keeps background dispatch server-side
// (an agent's tool call), and a suspended non-gated task's resume, if ever
// exposed, is a separate role-gated route that mints no capability (P8).

import type {
  BackgroundTaskManager,
  TaskFilter,
} from '@mastra/core/background-tasks';

import { tenantOwnsSaltedId } from '../do-runner/index.js';

export interface BackgroundTaskRoutesOptions {
  /** The manager to read through — WRAPPED, never exposed over the wire. */
  manager: BackgroundTaskManager;
  /** Route prefix. Default '/background-tasks'. */
  basePath?: string;
}

/**
 * A router `(request, tenantId) => Response | null`. `null` means the path is
 * not one of ours (the DO falls through). `tenantId` is the DO's own asserted
 * tenant — never read from the request.
 */
export type BackgroundTaskRouter = (
  request: Request,
  tenantId: string,
) => Promise<Response | null>;

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

/**
 * Resolve the tenant-scoped filter (runId XOR threadId) from the query, or a
 * fail-closed Response. Both ids are salted `${tenantId}_...`, so
 * `tenantOwnsSaltedId` is exact for either (INV-3). Absent => 400; foreign =>
 * 404 (no oracle — a foreign id gets the same answer as a nonexistent one).
 */
function resolveScopedFilter(
  url: URL,
  tenantId: string,
): { filter: TaskFilter; scopeValue: string } | Response {
  const runId = url.searchParams.get('runId') ?? undefined;
  const threadId = url.searchParams.get('threadId') ?? undefined;
  if (runId === undefined && threadId === undefined) {
    return json(
      {
        error:
          'a runId or threadId filter is required (background-task queries are tenant-scoped)',
      },
      400,
    );
  }
  const scopeValue = runId ?? (threadId as string);
  if (!tenantOwnsSaltedId(tenantId, scopeValue)) {
    return json({ error: 'not found' }, 404);
  }
  return {
    filter: runId !== undefined ? { runId } : { threadId },
    scopeValue,
  };
}

/**
 * The scope-ownership predicate shared by list AND stream (DL-014): a row is in
 * scope iff its runId or threadId equals the validated, tenant-owned scope value
 * the request was filtered by. Defense-in-depth OVER core's own filter — a future
 * regression in core's `listTasks`/`stream` scoping cannot leak a foreign or
 * out-of-scope row past this. One predicate so list and stream can never drift.
 */
function ownsScope(
  row: { runId?: unknown; threadId?: unknown },
  scopeValue: string,
): boolean {
  return row.runId === scopeValue || row.threadId === scopeValue;
}

export function createBackgroundTaskRoutes(
  options: BackgroundTaskRoutesOptions,
): BackgroundTaskRouter {
  const { manager } = options;
  const base = options.basePath ?? '/background-tasks';

  return async (request, tenantId) => {
    if (request.method !== 'GET') return null;
    const url = new URL(request.url);
    const path = url.pathname;
    if (path !== base && !path.startsWith(`${base}/`)) return null;

    // GET {base}/stream — SSE of lifecycle events for one tenant-owned run/thread.
    if (path === `${base}/stream`) {
      const resolved = resolveScopedFilter(url, tenantId);
      if (resolved instanceof Response) return resolved;
      return streamResponse(manager, resolved, request.signal);
    }

    // GET {base}/task/:taskId — one task, 404 on missing OR foreign (no oracle).
    if (path.startsWith(`${base}/task/`)) {
      const taskId = decodeURIComponent(path.slice(`${base}/task/`.length));
      if (taskId === '' || taskId.includes('/'))
        return json({ error: 'not found' }, 404);
      const task = await manager.getTask(taskId);
      if (!task || !tenantOwnsSaltedId(tenantId, task.runId)) {
        return json({ error: 'not found' }, 404);
      }
      return json({ task });
    }

    // GET {base} — list, filtered to one tenant-owned run/thread.
    if (path === base) {
      const resolved = resolveScopedFilter(url, tenantId);
      if (resolved instanceof Response) return resolved;
      const result = await manager.listTasks(resolved.filter);
      // Per-row parity with the stream guard (DL-014): re-check every returned
      // row against the requested scope, so a future regression in core's
      // listTasks filter cannot leak a foreign or out-of-scope task. `total` is
      // recomputed from the surviving rows — never report a count that includes
      // a row we would not return.
      const tasks = result.tasks.filter((t) =>
        ownsScope(t, resolved.scopeValue),
      );
      return json({ tasks, total: tasks.length });
    }

    return null;
  };
}

/**
 * Wrap `manager.stream(options)` (an object-chunk ReadableStream) as an SSE
 * Response. The stream is passed the validated filter so core scopes both its
 * on-connect snapshot and its live events; a second exact-match guard in the
 * transform drops any chunk whose runId/threadId is not the requested one, so a
 * cross-tenant leak is impossible even if core's filter ever loosened
 * (defense-in-depth, DL-014). `abortSignal` is the request's own, so a client
 * disconnect closes the upstream subscription.
 */
function streamResponse(
  manager: BackgroundTaskManager,
  scoped: { filter: TaskFilter; scopeValue: string },
  abortSignal: AbortSignal,
): Response {
  const { filter, scopeValue } = scoped;
  const source = manager.stream({ ...filter, abortSignal });
  const encoder = new TextEncoder();
  const guarded = source.pipeThrough(
    new TransformStream<Record<string, unknown>, Uint8Array>({
      transform(chunk, controller) {
        // Same per-row scope guard the list route applies — one predicate so the
        // two surfaces never drift (DL-014, defense-in-depth over core's filter).
        if (!ownsScope(chunk, scopeValue)) return;
        controller.enqueue(
          encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`),
        );
      },
    }),
  );
  return new Response(guarded, {
    status: 200,
    headers: {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    },
  });
}
