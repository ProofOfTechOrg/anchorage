// SPDX-License-Identifier: Apache-2.0
// The deployment hub topology seam (DL-009, DL-016), mirroring do-run-topology.ts.
//
// A singleton hub Durable Object (HubDurableObject, do-runner/hub-do.ts) fans
// approval stream events out to the deployment's open dashboard sockets. The Worker
// reaches it two ways: it POSTs each ApprovalStreamEvent to the hub's
// `/internal/event` route (publish), and it forwards a verified WebSocket
// upgrade to the hub's `/subscribe` route (forwardSubscribe). Both address the
// hub by idFromName(HUB_INSTANCE_NAME), so only one sanctioned hub exists per
// physically isolated deployment.
//
// The namespace/stub types are STRUCTURAL subsets (method syntax, so a real
// DurableObjectNamespace satisfies them under TS method-parameter bivariance),
// keeping host-kit free of any @cloudflare/workers-types import — the same
// convention RunnerNamespaceLike follows. The dev plugin (M-008) injects an
// in-memory HubNamespaceLike behind the same shape.

import type { ApprovalStreamEvent } from '../approval-api/index.js';
import {
  deploymentIdentityHeaders,
  HUB_INSTANCE_NAME,
  stampDeploymentIdentityRequest,
} from '../do-runner/index.js';

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
   * Forward one approval stream event to the deployment hub, POSTing it to the
   * hub's `/internal/event` route.
   */
  publish(event: ApprovalStreamEvent): Promise<void>;
  /**
   * Forward an already-verified WebSocket upgrade Request to the deployment hub. The
   * caller (stream-router.ts) has rewritten it to `/subscribe` with the ticket's
   * actorId/role as query params.
   */
  forwardSubscribe(request: Request): Promise<Response>;
}

export function createHubTopology<Id>(
  namespace: HubNamespaceLike<Id>,
  deploymentIdentitySecret: string,
): HubTopology {
  const stub = (): HubStubLike =>
    namespace.get(namespace.idFromName(HUB_INSTANCE_NAME));

  return {
    publish: async (event) => {
      const response = await stub().fetch('http://hub/internal/event', {
        method: 'POST',
        headers: deploymentIdentityHeaders(deploymentIdentitySecret, {
          'content-type': 'application/json',
        }),
        body: JSON.stringify(event),
      });
      if (!response.ok) {
        throw new Error(`hub publish failed with status ${response.status}`);
      }
    },
    forwardSubscribe: (request) =>
      stub().fetch(
        stampDeploymentIdentityRequest(request, deploymentIdentitySecret),
      ),
  };
}
