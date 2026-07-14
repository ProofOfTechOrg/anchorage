// SPDX-License-Identifier: Apache-2.0
// The per-tenant hub topology seam (DL-009, DL-016), mirroring do-run-topology.ts.
//
// A per-tenant hub Durable Object (HubDurableObject, do-runner/hub-do.ts) fans
// approval stream events out to that tenant's open dashboard sockets. The Worker
// reaches it two ways: it POSTs each ApprovalStreamEvent to the hub's
// `/internal/event` route (publish), and it forwards a verified WebSocket
// upgrade to the hub's `/subscribe` route (forwardSubscribe). Both address the
// hub by idFromName(tenantId), so the DO's id.name IS the bare tenant and the
// fan-out is tenant-disjoint by construction.
//
// The namespace/stub types are STRUCTURAL subsets (method syntax, so a real
// DurableObjectNamespace satisfies them under TS method-parameter bivariance),
// keeping host-kit free of any @cloudflare/workers-types import — the same
// convention RunnerNamespaceLike follows. The dev plugin (M-008) injects an
// in-memory HubNamespaceLike behind the same shape.

import type { ApprovalStreamEvent } from '../approval-api/index.js';

/**
 * The subset of a DurableObjectStub the hub topology uses. The raw-`Request`
 * overload is what lets a WebSocket UPGRADE forward through the stub unchanged;
 * the string/init overload carries the `/internal/event` JSON POST.
 */
export interface HubStubLike {
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

/** The subset of a DurableObjectNamespace the hub topology uses. */
export interface HubNamespaceLike<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id): HubStubLike;
}

export interface HubTopology {
  /**
   * Forward one approval stream event to its tenant's hub, POSTing it to the
   * hub's `/internal/event` route. Routed by `event.record.tenantId`, which the
   * hub re-asserts against its own id.name (defense in depth).
   */
  publish(event: ApprovalStreamEvent): Promise<void>;
  /**
   * Forward an already-verified WebSocket upgrade Request to a tenant's hub. The
   * caller (stream-router.ts) has rewritten it to `/subscribe` with the ticket's
   * actorId/role as query params; this only routes it to idFromName(tenantId).
   */
  forwardSubscribe(tenantId: string, request: Request): Promise<Response>;
}

export function createHubTopology<Id>(
  namespace: HubNamespaceLike<Id>,
): HubTopology {
  // One DO per tenant: id.name === tenantId, so the hub owns exactly one
  // tenant's feed with no per-run isolation code (DL-009).
  const stub = (tenantId: string): HubStubLike =>
    namespace.get(namespace.idFromName(tenantId));

  return {
    publish: async (event) => {
      await stub(event.record.tenantId).fetch('http://hub/internal/event', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(event),
      });
    },
    forwardSubscribe: (tenantId, request) => stub(tenantId).fetch(request),
  };
}
