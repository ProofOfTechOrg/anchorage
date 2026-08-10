// SPDX-License-Identifier: Apache-2.0
// Route matched provider notifications through the sanctioned per-thread
// topology with a least-privileged service principal.

import type { SendNotificationSignalInput } from '@mastra/core/notifications';

import type { ExecutionPrincipal } from '../approval-api/index.js';
import type {
  ThreadPrincipalContext,
  ThreadTopology,
} from '../host-kit/index.js';
import type { StoredSubscription } from './subscription-d1.js';

function deliveryPrincipalContext(): ThreadPrincipalContext {
  const principal: ExecutionPrincipal = {
    kind: 'service',
    id: 'signal-provider-delivery',
    purpose: 'signal-provider-delivery',
  };
  return { principal };
}

export async function deliverNotification(
  topology: ThreadTopology,
  subscription: StoredSubscription,
  notification: SendNotificationSignalInput,
): Promise<Response> {
  return topology.send(
    deliveryPrincipalContext(),
    subscription.threadId,
    `/signal/notification?resourceId=${encodeURIComponent(
      subscription.resourceId,
    )}`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(notification),
    },
  );
}
