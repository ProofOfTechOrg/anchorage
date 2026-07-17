// SPDX-License-Identifier: Apache-2.0
// Track C (M-004) — signals, subscriptions, notifications. Subpath-only
// (`@proofoftech/flowsafe/signals`), like agent-runner and background-tasks:
// host-side wiring a consumer opts into, not in the root barrel.

// The DOM-free SignalClient (CI-M-004-005).
export {
  type SendMessageBody,
  type SendNotificationBody,
  type SendSignalBody,
  type SendStateBody,
  SignalApiError,
  SignalClient,
  type SignalClientOptions,
  type SignalFetchLike,
  type SignalResponseLike,
} from './client.js';

// D1 storage domains + the injectable composition helper (CI-M-004-002/003).
export type { SignalDatabase, SignalStatement } from './d1-shared.js';
export { D1NotificationsStorage } from './notifications-d1.js';
// The P6 ingestion trust boundary router (CI-M-004-004).
export {
  createInMemorySignalRateLimiter,
  createSignalRouter,
  type SignalAuditSink,
  type SignalChannel,
  type SignalIngestAuditEvent,
  type SignalRateLimiter,
  type SignalRouter,
  type SignalRouterOptions,
} from './router.js';
export { createSignalStorageDomains } from './storage.js';
// Thread-DO signal routes (CI-M-004-001).
export {
  ACTIVE_BEHAVIORS,
  type ActiveBehavior,
  createThreadSignalRoutes,
  IDLE_BEHAVIORS,
  type IdleBehavior,
  type RunCapConsult,
  type ThreadSignalRouter,
  type ThreadSignalRoutesOptions,
} from './thread-do-routes.js';
export { D1ThreadStateStorage } from './thread-state-d1.js';
