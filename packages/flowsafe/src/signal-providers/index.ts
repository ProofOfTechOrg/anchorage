// SPDX-License-Identifier: Apache-2.0
// Track E (M-007) — signal providers: host providers on a DO with alarm-driven
// polling, terminate provider webhooks on the Worker, persist subscriptions in a
// flowsafe-owned D1 table. Subpath-only (`@proofoftech/flowsafe/signal-providers`),
// like agent-runner/background-tasks/signals/schedules/goals: host-side wiring a
// consumer opts into, never the root barrel.

// The delivery seam (into a thread via the topology).
export { deliverNotification, deliveryTenantContext } from './delivery.js';
// The GitHub reference provider (binding-gated showcase connector).
export {
  buildGithubNotification,
  extractGithubResourceIds,
  type GithubSignalProviderOptions,
  githubSignalProvider,
  verifyGithubSignature,
} from './github-provider.js';
// The provider host DO (CI-M-007-001).
export {
  type AlarmStorage,
  type PollResult,
  SignalProviderHost,
  SignalProviderHostIdentityError,
  type SignalProviderHostState,
  type SignalProviderHostWiring,
} from './host-do.js';
// The Worker -> provider-host lifecycle seam.
export {
  createSignalProviderHostTopology,
  type ReconcileSignalProviderPolling,
  type SignalProviderHostNamespaceLike,
  type SignalProviderHostStubLike,
  type SignalProviderHostTopology,
} from './host-topology.js';
// The provider contract (CI-M-007-001).
export {
  createWebhookSignalProvider,
  normalizeResourceIds,
  PROVIDER_ID_PATTERN,
  type ProviderDelivery,
  type SendNotificationSignalInput,
  type SignalProviderAdapter,
  type SignalSubscription,
  type WebhookHeaders,
  type WebhookSignalProviderConfig,
} from './provider.js';
// The D1 subscription store (CI-M-007-002).
export {
  D1SubscriptionStoreFactory,
  InMemorySubscriptionStoreFactory,
  SIGNAL_SUBSCRIPTIONS_TABLE,
  type StoredSubscription,
  SUBSCRIPTION_TENANT_BOUND,
  type SubscribeInput,
  type SubscriptionStoreFactory,
  type SystemSubscriptionStore,
  type TenantBoundSubscriptionStore,
} from './subscription-d1.js';
// The webhook ingress + human-only subscription CRUD (CI-M-007-003).
export {
  createSubscriptionRouter,
  createWebhookRouter,
  type SignalProviderAuditEvent,
  type SignalProviderAuditSink,
  type SubscriptionAuditEvent,
  type SubscriptionRouter,
  type SubscriptionRouterOptions,
  type WebhookAuditEvent,
  type WebhookRateLimiter,
  type WebhookRouter,
  type WebhookRouterOptions,
} from './webhook-route.js';
