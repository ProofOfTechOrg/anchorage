// SPDX-License-Identifier: Apache-2.0
// Route matched provider notifications through the sanctioned per-thread
// topology with a least-privileged service principal.

import type { SendNotificationSignalInput } from '@mastra/core/notifications';

import type { ExecutionPrincipal } from '../approval-api/index.js';
import type {
  ThreadPrincipalContext,
  ThreadTopology,
} from '../host-kit/index.js';
import { RunRouteError } from '../host-kit/run-route-error.js';
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

/**
 * What became of one provider delivery. Both provider lanes classify the same
 * way — the webhook route turns `deferred` into a 5xx so the sender redelivers,
 * and the poll host counts it for the next poll — so the rule lives here, with
 * the transport it describes, rather than being written out twice.
 */
export type DeliveryOutcome = 'delivered' | 'denied' | 'failed' | 'deferred';

/** Statuses that are transient even though they are below 500. */
const RETRYABLE_CLIENT_STATUSES = new Set([408, 429]);

/**
 * Classify a thread-route response. 422 is the content policy's terminal
 * refusal — redelivering identical bytes could only be denied again — and any
 * other 4xx is a structural defect in the notification or the stored row, which
 * is equally permanent. A timeout or a rate limit is transient despite its 4xx,
 * and every 5xx means the deployment could not decide, so the event must come
 * back rather than be dropped.
 */
export function classifyDeliveryResponse(status: number): DeliveryOutcome {
  if (status >= 200 && status < 300) return 'delivered';
  if (status === 422) return 'denied';
  if (status < 500 && !RETRYABLE_CLIENT_STATUSES.has(status)) return 'failed';
  return 'deferred';
}

/**
 * Classify a thrown delivery failure. Only `RunRouteError` carries a status the
 * topology assigned deliberately; anything else — a network failure, a bug, a
 * platform error that merely happens to have a `status` property — is treated
 * as undecided, because dropping an authentic event is worse than delivering it
 * twice.
 */
export function classifyDeliveryError(error: unknown): DeliveryOutcome {
  if (error instanceof RunRouteError) {
    const outcome = classifyDeliveryResponse(error.status);
    // A throw is never a delivery, whatever status rode along with it.
    return outcome === 'delivered' ? 'deferred' : outcome;
  }
  return 'deferred';
}
