// SPDX-License-Identifier: Apache-2.0
// The internal Worker -> per-tenant signal-provider host topology.
//
// Subscription mutations and polling alarms are separate durable resources:
// the row commits in D1, then this seam asks the tenant's provider-host DO to
// reconcile its alarm. The callback therefore reports a post-commit lifecycle
// failure to the route; it does not pretend the row mutation rolled back.

import type { TenantContext } from '../approval-api/index.js';
import { RESERVED_TENANT_IDS, TENANT_ID_PATTERN } from '../do-runner/index.js';

/** The subset of a Durable Object stub the provider-host topology uses. */
export interface SignalProviderHostStubLike {
  fetch(
    url: string,
    init?: {
      method?: string;
    },
  ): Promise<Response>;
}

/** The subset of a Durable Object namespace the provider-host topology uses. */
export interface SignalProviderHostNamespaceLike<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id): SignalProviderHostStubLike;
}

/**
 * Reconcile polling after a committed subscription mutation. Implementations
 * must reject when the provider host cannot confirm the alarm lifecycle.
 */
export type ReconcileSignalProviderPolling = (
  tenant: TenantContext,
) => Promise<void>;

export interface SignalProviderHostTopology {
  /**
   * Address exactly the authenticated tenant's provider-host DO and ask it to
   * arm, move, keep, or delete its polling alarm from durable subscription
   * state. No client request, provider id, body, or tenant header is forwarded.
   */
  reconcilePolling: ReconcileSignalProviderPolling;
}

function validatedTenantId(tenant: TenantContext): string {
  const tenantId = tenant.tenantId;
  if (
    typeof tenantId !== 'string' ||
    !TENANT_ID_PATTERN.test(tenantId) ||
    RESERVED_TENANT_IDS.includes(tenantId)
  ) {
    throw new Error(
      'signal-provider host topology requires an INV-3, non-reserved tenantId',
    );
  }
  if (
    typeof tenant.actor?.tenantId !== 'string' ||
    tenant.actor.tenantId !== tenantId
  ) {
    throw new Error(
      'signal-provider host topology requires actor.tenantId to match tenantId',
    );
  }
  return tenantId;
}

export function createSignalProviderHostTopology<Id>(
  namespace: SignalProviderHostNamespaceLike<Id>,
): SignalProviderHostTopology {
  return {
    reconcilePolling: async (tenant) => {
      const tenantId = validatedTenantId(tenant);
      const id = namespace.idFromName(tenantId);
      const response = await namespace
        .get(id)
        .fetch('http://provider-host/arm', { method: 'POST' });
      if (!response.ok) {
        throw new Error(
          `signal-provider host reconcile failed with status ${response.status}`,
        );
      }
    },
  };
}
