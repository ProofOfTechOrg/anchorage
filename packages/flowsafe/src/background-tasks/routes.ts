// SPDX-License-Identifier: Apache-2.0
// Track B (M-003): the read-only HTTP surface over background tasks (DL-014).
// Core's `listTasks` / `getTask` / `stream` are not run-bound, so this router:
//   - requires a path-safe `runId` or `threadId` filter on list and stream;
//   - loads a task for `getTask` and 404s when missing;
//   - never returns or accepts the raw BackgroundTaskManager.
// Mutating the queue
// (dispatch/cancel/resume) is NOT here: v1 keeps background dispatch server-side
// (an agent's tool call), and a suspended non-gated task's resume, if ever
// exposed, is a separate role-gated route that mints no capability (P8).

import type {
  BackgroundTaskManager,
  TaskFilter,
} from '@mastra/core/background-tasks';

import { isPathSafeId } from '../do-runner/index.js';
import { safeDecodeSegment } from '../host-kit/route-path.js';

/**
 * The three reads this router makes, as a type of their own.
 *
 * Narrower than `BackgroundTaskManager` on purpose. The manager also carries
 * `enqueue`, `registerStaticExecutor`, `registerTaskContext`, `resume`, and
 * `restart` — each of which puts a task body on the deployment without passing
 * the execution fence — so a route handler holding one is a single property
 * access away from the thing the fence exists to stop. Typing the option as the
 * reads instead lets `BackgroundTaskHost` hand over its own fence-preserving
 * forwarding surface, while a plain manager still satisfies it structurally for
 * a host that has no fence to preserve.
 */
export interface BackgroundTaskReads {
  getTask: BackgroundTaskManager['getTask'];
  listTasks: BackgroundTaskManager['listTasks'];
  stream: BackgroundTaskManager['stream'];
}

export interface BackgroundTaskRoutesOptions {
  /** The read surface to serve — WRAPPED, never exposed over the wire. */
  manager: BackgroundTaskReads;
  /** Host-owned authorization for the run/thread scope of every returned row. */
  authorize(scope: { runId?: string; threadId?: string }): Promise<boolean>;
  /** Route prefix. Default '/background-tasks'. */
  basePath?: string;
}

/**
 * A router `(request) => Response | null`. `null` means the path is not one of
 * ours (the DO falls through).
 */
export type BackgroundTaskRouter = (
  request: Request,
) => Promise<Response | null>;

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

/**
 * Resolve the run/thread filter from the query, or a fail-closed Response.
 * Absent means 400; malformed means 404.
 */
function resolveScopedFilter(
  url: URL,
): { filter: TaskFilter; scopeValue: string } | Response {
  const runId = url.searchParams.get('runId') ?? undefined;
  const threadId = url.searchParams.get('threadId') ?? undefined;
  if (runId === undefined && threadId === undefined) {
    return json(
      {
        error:
          'a runId or threadId filter is required (background-task queries are run-scoped)',
      },
      400,
    );
  }
  const scopeValue = runId ?? (threadId as string);
  if (!isPathSafeId(scopeValue)) {
    return json({ error: 'not found' }, 404);
  }
  return {
    filter: runId !== undefined ? { runId } : { threadId },
    scopeValue,
  };
}

/**
 * The scope-ownership predicate shared by list and stream: a row is in
 * scope iff its runId or threadId equals the validated scope value
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

  return async (request) => {
    if (request.method !== 'GET') return null;
    const url = new URL(request.url);
    const path = url.pathname;
    if (path !== base && !path.startsWith(`${base}/`)) return null;

    // GET {base}/stream — SSE of lifecycle events for one run/thread.
    if (path === `${base}/stream`) {
      const resolved = resolveScopedFilter(url);
      if (resolved instanceof Response) return resolved;
      if (!(await options.authorize(resolved.filter))) {
        return json({ error: 'not found' }, 404);
      }
      return streamResponse(manager, resolved, request.signal);
    }

    // GET {base}/task/:taskId — one task, 404 when missing.
    if (path.startsWith(`${base}/task/`)) {
      // Malformed percent-encoding in the taskId is not a real task — 404 (the
      // no-oracle response), never a decodeURIComponent throw out of the DO
      // handler (this route is post-auth, but the same fail-shut posture).
      const taskId = safeDecodeSegment(path.slice(`${base}/task/`.length));
      if (taskId === undefined || taskId === '' || taskId.includes('/'))
        return json({ error: 'not found' }, 404);
      const task = await manager.getTask(taskId);
      if (!task) {
        return json({ error: 'not found' }, 404);
      }
      if (
        !(await options.authorize({
          runId: task.runId,
          ...(task.threadId !== undefined ? { threadId: task.threadId } : {}),
        }))
      ) {
        return json({ error: 'not found' }, 404);
      }
      return json({ task });
    }

    // GET {base} — list, filtered to one run/thread.
    if (path === base) {
      const resolved = resolveScopedFilter(url);
      if (resolved instanceof Response) return resolved;
      if (!(await options.authorize(resolved.filter))) {
        return json({ error: 'not found' }, 404);
      }
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
 * out-of-scope leak is blocked even if core's filter ever loosened. This is
 * defense in depth. `abortSignal` is the request's own, so a client
 * disconnect closes the upstream subscription.
 */
function streamResponse(
  manager: BackgroundTaskReads,
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
        const payload =
          typeof chunk.payload === 'object' &&
          chunk.payload !== null &&
          !Array.isArray(chunk.payload)
            ? (chunk.payload as Record<string, unknown>)
            : undefined;
        if (!payload || !ownsScope(payload, scopeValue)) return;
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
