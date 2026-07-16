// SPDX-License-Identifier: Apache-2.0
// The per-thread DO topology seam — and the MINTER for the tenant header
// ThreadDurableObject verifies (DL-002). Mint and verify ship together, the same
// posture stream-ticket.ts takes, because a verifier whose input the caller can
// write is not a check.
//
// THE TRAP THIS EXISTS TO CLOSE. The house idiom for forwarding into a DO is
// hub-topology.ts's `forwardSubscribe: (tenantId, request) => stub(tenantId)
// .fetch(request)` — the CLIENT's Request, headers and all. That is safe for the
// hub, which trusts the forwarded request for nothing (it re-asserts
// event.record.tenantId against its own id.name from server-side data). A thread
// route copying that shape would hand the client the very header the thread DO
// authenticates on: `x-flowsafe-tenant: <victim>` and the assertion passes. So
// this module, not the caller, decides the header's value — on EVERY path,
// including forwarded upgrades, where it OVERWRITES rather than defaults.
//
// Reach a thread DO through here. A route that addresses the namespace itself
// re-opens the hole, so the namespace-shaped seam is deliberately not re-exported
// for ad-hoc use: `send`/`forward` are the sanctioned surface, and both refuse a
// threadId the caller's tenant does not own BEFORE the DO is ever addressed.
//
// Structural namespace/stub types (method syntax, so a real
// DurableObjectNamespace satisfies them under TS method-parameter bivariance),
// keeping host-kit free of @cloudflare/workers-types — same convention as
// RunnerNamespaceLike / HubNamespaceLike.

import type { TenantContext } from '../approval-api/index.js';
import { THREAD_TENANT_HEADER } from '../do-runner/index.js';
import { requireOwnedMemoryId } from './memory-boundary.js';

/**
 * The subset of a DurableObjectStub the thread topology uses. The raw-`Request`
 * overload is what lets a WebSocket UPGRADE forward through the stub unchanged;
 * the string/init overload carries ordinary JSON routes.
 */
export interface ThreadStubLike {
  fetch(request: Request): Promise<Response>;
  fetch(
    url: string,
    init?: {
      method?: string;
      headers?: Record<string, string>;
      body?: string;
    },
  ): Promise<Response>;
}

/** The subset of a DurableObjectNamespace the thread topology uses. */
export interface ThreadNamespaceLike<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id): ThreadStubLike;
}

/** What `send` carries beyond the sanctioned header — a route's own payload. */
export interface ThreadRequestInit {
  method?: string;
  headers?: Record<string, string>;
  body?: string;
}

export interface ThreadTopology {
  /**
   * Send an authenticated request to a thread DO. `path` is the DO-side route
   * (e.g. `/messages`). Throws RunRouteError(404) when the tenant does not own
   * `threadId` — before the DO is addressed, so a foreign thread is never even
   * woken, and the caller surfaces the 404 its router already maps.
   */
  send(
    tenant: TenantContext,
    threadId: string,
    path: string,
    init?: ThreadRequestInit,
  ): Promise<Response>;
  /**
   * Forward a client Request (e.g. a verified WebSocket upgrade) to a thread DO,
   * with the tenant header OVERWRITTEN from the authenticated context. Same
   * ownership 404 as `send`. Use this instead of `stub.fetch(request)`: a
   * verbatim forward carries the client's own headers, including a forged
   * `x-flowsafe-tenant`.
   */
  forward(
    tenant: TenantContext,
    threadId: string,
    request: Request,
  ): Promise<Response>;
}

export function createThreadTopology<Id>(
  namespace: ThreadNamespaceLike<Id>,
): ThreadTopology {
  // One DO per thread: id.name IS the tenant-minted threadId, so the instance
  // carries its tenant exactly like a runId carries its own (DL-002).
  const stub = (threadId: string): ThreadStubLike =>
    namespace.get(namespace.idFromName(threadId));

  // Ownership FIRST, then address. Both barriers are load-bearing and neither
  // subsumes the other: this 404 is what stops tenant B reaching tenant A's
  // thread at all, and the header the DO re-asserts is what stops a routing bug
  // here from being the only thing between them.
  const owned = (tenant: TenantContext, threadId: string): string =>
    requireOwnedMemoryId(tenant, threadId, 'threadId');

  // Both surfaces are `async` so the ownership refusal REJECTS rather than
  // throwing synchronously out of a Promise-typed call: a caller writing
  // `topology.send(...).catch(handle)` would never see a sync throw, and the
  // 404 would escape past the very handler meant to map it.
  return {
    send: async (tenant, threadId, path, init = {}) => {
      // Merge through Headers so the stamp wins by case-INSENSITIVE name. A
      // plain-object spread keeps a caller's `X-Flowsafe-Tenant` as a SECOND
      // property; Headers' fill algorithm then appends both into `globex, acme`,
      // which the DO reads as claimed !== tenantId — a 403 that looks like an
      // identity attack (fail-closed, but the wrong error and a false
      // "spelling cannot win" story). `set` is the primitive `forward` already
      // relies on; it overwrites at every spelling. The value is the RESOLVED
      // tenant context (authenticate -> INV-3 -> bind), never a header or body.
      const merged = new Headers(init.headers);
      merged.set(THREAD_TENANT_HEADER, tenant.tenantId);
      const headers: Record<string, string> = {};
      merged.forEach((value, key) => {
        headers[key] = value;
      });
      return stub(owned(tenant, threadId)).fetch(`http://thread${path}`, {
        ...init,
        headers,
      });
    },
    forward: async (tenant, threadId, request) => {
      const addressed = owned(tenant, threadId);
      // A cloned Request has MUTABLE headers where an inbound one does not, so
      // this is what lets the overwrite happen at all — and `set` (not `append`)
      // is what makes a forged client value vanish rather than ride along as a
      // second value the DO might read.
      const forwarded = new Request(request);
      forwarded.headers.set(THREAD_TENANT_HEADER, tenant.tenantId);
      return stub(addressed).fetch(forwarded);
    },
  };
}
