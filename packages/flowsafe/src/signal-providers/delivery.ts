// SPDX-License-Identifier: Apache-2.0
// Track E (M-007) — the delivery seam: a matched subscription's notification is
// routed into its thread through host-kit's `createThreadTopology.send`, the
// SANCTIONED reach into a per-thread DO (DL-002). NEVER the raw namespace: the
// topology ownership-404s the threadId against the tenant BEFORE the DO is
// addressed AND stamps the `x-flowsafe-tenant` header the thread DO
// authenticates on — a forged tenant can never ride in (thread-topology.ts).
//
// The ROW is the tenant authority. A subscription carries its own `tenantId`
// (server-stamped at subscribe, never from a webhook payload), so delivery binds
// to `deliveryTenantContext(subscription.tenantId)`. If a row were tampered so
// its `threadId` belonged to another tenant, the topology's `ownsMemoryId` check
// fails and the send 404s — fail closed, no cross-tenant delivery (the cross-
// tenant probe).

import type { SendNotificationSignalInput } from '@mastra/core/notifications';

import type { ApprovalActor, TenantContext } from '../approval-api/index.js';
import {
  assertMintableTenantId,
  mintResourceId,
  mintSaltedId,
  mintThreadId,
  tenantOwnsSaltedId,
} from '../do-runner/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import type { StoredSubscription } from './subscription-d1.js';

/**
 * A TenantContext for the SERVER-DERIVED delivery path — built from a trusted,
 * validated tenantId recovered from a subscription row, never from client input. The
 * topology reads only `.tenantId` and `.ownsMemoryId`; the pure id helpers are
 * provided for completeness, and the request-scoped `service()` throws (a
 * delivery has no request, no store) so a misuse is loud rather than silent.
 */
export function deliveryTenantContext(tenantId: string): TenantContext {
  assertMintableTenantId(tenantId, 'deliveryTenantContext');
  const actor: ApprovalActor = {
    id: 'signal-provider-delivery',
    role: 'operator',
    tenantId,
  };
  return {
    actor,
    tenantId,
    service: () => {
      throw new Error(
        'deliveryTenantContext: no approval service on the signal-provider delivery path',
      );
    },
    newRunId: () =>
      mintSaltedId(
        tenantId,
        () => crypto.randomUUID(),
        'deliveryTenantContext',
      ),
    ownsRun: (runId: string) => tenantOwnsSaltedId(tenantId, runId),
    newThreadId: () => mintThreadId(tenantId, () => crypto.randomUUID()),
    newResourceId: (resourceKey: string) =>
      mintResourceId(tenantId, resourceKey),
    ownsMemoryId: (id: string) => tenantOwnsSaltedId(tenantId, id),
    // A delivery decides nothing; the display hint is irrelevant on this path.
    canSelfDecide: () => false,
  };
}

/**
 * Deliver ONE notification to a matched subscription's thread through the
 * topology. Returns the thread DO's Response (a 404 when the row's threadId is
 * not owned by its tenant — the fail-closed cross-tenant case). The whole
 * `SendNotificationSignalInput` is forwarded; the thread-DO `/signal/notification`
 * route reads what it needs (source/kind/summary/priority/payload/dedupeKey/
 * coalesceKey/attributes) and derives the resourceId from ITS own scope (a thread
 * has one owner — the row's resourceId — so delivery keys the inbox identically
 * whether the signal arrived from a client ingest or a provider webhook).
 */
export async function deliverNotification(
  topology: ThreadTopology,
  subscription: StoredSubscription,
  notification: SendNotificationSignalInput,
): Promise<Response> {
  return topology.send(
    deliveryTenantContext(subscription.tenantId),
    subscription.threadId,
    '/signal/notification',
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(notification),
    },
  );
}
