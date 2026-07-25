// SPDX-License-Identifier: Apache-2.0
// The agent-memory host boundary (docs/agent-memory-tenancy.md item 5) — the
// memory analogue of createRunRouter's "runId is server-assigned" 400 and its
// ownership 404, factored out because memory ids reach the boundary through
// MANY routes (Track A's agent runs, Track C's signals, Track D's threaded
// schedule fires) where a runId reaches exactly one.
//
// Why a shared seam rather than the check inline in each router: threadId and
// resourceId are Mastra's CALLER-CHOSEN business identity (a user id, an email,
// a conversation slug), so the natural route body carries them and the natural
// handler trusts them — that is the leak, and it reads like ordinary code on
// every new route. One named, tested guard that every memory-touching route
// calls is the only version of this that survives contact with the fifth route.
//
// It lives in host-kit, not do-runner/memory-id.ts (which owns the mints and the
// pure ownership predicate), for two reasons: the guard's contract IS its HTTP
// status — RunRouteError carries the 400/404 the doctrine names, so a route
// gets the posture by calling it rather than by remembering to map an error —
// and TenantContext lives in approval-api, which already imports do-runner
// (a do-runner guard taking a TenantContext would invert that layering).

import type { TenantContext } from '../approval-api/index.js';
import { RunRouteError } from './run-route-error.js';

/**
 * The memory-id fields no client body may name — the TCB_ONLY_CREATE_FIELDS
 * doctrine applied to memory: a client that picks its own threadId/resourceId
 * picks whose memory it reads, so these are minted server-side or not at all.
 */
export const TCB_ONLY_MEMORY_FIELDS = ['threadId', 'resourceId'] as const;

export type TcbOnlyMemoryField = (typeof TCB_ONLY_MEMORY_FIELDS)[number];

/**
 * Refuse a request body that names a memory id ANYWHERE in it. 400 rather than
 * a silent override, for the same reason the run router 400s a client runId: a
 * caller pinning ids must find out, not watch its value get quietly replaced and
 * conclude the field works.
 *
 * NESTED, not just top-level, because the shape the first real consumer sends is
 * nested: an agent run starts through `POST /runs` as
 * `{workflowId, inputData: {...}}`, so the natural body for a threaded run is
 * `{inputData: {threadId, prompt}}` — a top-level-only check would pass exactly
 * the request this guard exists to refuse. A legitimate payload never carries a
 * memory id (that is the whole doctrine: they are minted server-side and travel
 * through requestContext, never a body), so refusing the name at any depth costs
 * nothing real and closes the shape the leak actually arrives in.
 *
 * `undefined` is refused along with a value — a body carrying the KEY is a
 * caller that believes it may choose, and the day someone writes
 * `threadId: maybeUndefined` the tolerant reading is a leak waiting on one
 * truthiness bug. Non-object bodies name no field and pass; body SHAPE is each
 * route's own schema business.
 *
 * The walk is ITERATIVE and UNBOUNDED in depth. A depth cap would be a bypass
 * with a number on it — nest one level past it and the id sails through the
 * check — and depth is the attacker's choice, not the route's. Recursion is what
 * a hostile 10k-deep body would actually break (the stack), so the fix is to not
 * recurse: an explicit worklist costs heap proportional to a body JSON.parse has
 * already materialized. A `seen` set keeps a cyclic in-process object (never a
 * parsed body, which cannot cycle) from looping forever.
 *
 * Covers what a JSON body can express. A `Map`'s entries and a non-enumerable
 * property are invisible to it — neither survives JSON.parse, so no HTTP caller
 * can reach them; a route handing this a hand-built in-process object is outside
 * the contract.
 */
export function assertNoClientMemoryIds(body: unknown): void {
  const worklist: unknown[] = [body];
  const seen = new Set<object>();
  while (worklist.length > 0) {
    const value = worklist.pop();
    if (typeof value !== 'object' || value === null) continue;
    if (seen.has(value)) continue;
    seen.add(value);
    if (Array.isArray(value)) {
      worklist.push(...value);
      continue;
    }
    for (const field of TCB_ONLY_MEMORY_FIELDS) {
      if (field in value) {
        throw new RunRouteError(
          400,
          `${field} is server-assigned (agent-memory ids are minted from the authenticated tenant — see docs/agent-memory-tenancy.md)`,
        );
      }
    }
    worklist.push(...Object.values(value));
  }
}

/**
 * Assert the authenticated tenant owns a memory id, returning it for use.
 * 404 — never 403 — on a foreign id: the run router's rule, for the same
 * reason. A 403 would confirm that another tenant's threadId EXISTS, turning
 * every read path into an existence oracle over ids a caller can guess (a
 * resourceId is business identity — an email, a user id — so guessing is the
 * expected case, not a stretch).
 *
 * This is the ONLY thing between a foreign threadId and its history — nothing
 * downstream re-checks. Mastra's recall filters by `resourceId` only when the
 * caller passes one (`if (resourceId) query += ' AND resourceId = ?'`), and core
 * ships recall sites that pass a threadId alone, so a foreign thread recalls in
 * FULL past this point (pinned in do-runner/memory-recall-tenancy.test.ts). Call
 * it on EVERY memory read path; there is no second line.
 *
 * `tenant.ownsMemoryId` is exact rather than a prefix startsWith by luck: INV-3
 * excludes '_' from tenant ids, so 'acme' can never own 'acmecorp_...'
 * (do-runner/path-safe-id.ts).
 */
export function requireOwnedMemoryId(
  tenant: TenantContext,
  id: string,
  label: TcbOnlyMemoryField = 'threadId',
): string {
  if (typeof id !== 'string' || !tenant.ownsMemoryId(id)) {
    throw new RunRouteError(404, `${label} not found`);
  }
  return id;
}
