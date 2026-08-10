// SPDX-License-Identifier: Apache-2.0
// Internal Worker -> deployment signal-provider host topology.

import { deploymentIdentityHeaders } from '../do-runner/index.js';

/** One provider host per physically isolated deployment. */
export const SIGNAL_PROVIDER_HOST_INSTANCE_NAME =
  'deployment-signal-provider-host';

export interface SignalProviderHostStubLike {
  fetch(
    url: string,
    init?: {
      method?: string;
      headers?: Record<string, string>;
    },
  ): Promise<Response>;
}

export interface SignalProviderHostNamespaceLike<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id): SignalProviderHostStubLike;
}

export type ReconcileSignalProviderPolling = () => Promise<void>;

export interface SignalProviderHostTopology {
  reconcilePolling: ReconcileSignalProviderPolling;
}

export function createSignalProviderHostTopology<Id>(
  namespace: SignalProviderHostNamespaceLike<Id>,
  deploymentIdentitySecret: string,
): SignalProviderHostTopology {
  return {
    reconcilePolling: async () => {
      const id = namespace.idFromName(SIGNAL_PROVIDER_HOST_INSTANCE_NAME);
      const response = await namespace
        .get(id)
        .fetch('http://provider-host/arm', {
          method: 'POST',
          headers: deploymentIdentityHeaders(deploymentIdentitySecret),
        });
      if (!response.ok) {
        throw new Error(
          `signal-provider host reconcile failed with status ${response.status}`,
        );
      }
    },
  };
}
