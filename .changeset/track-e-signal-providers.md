---
"@proofoftech/flowsafe": minor
---

Track E (M-007) — signal providers: a new subpath `@proofoftech/flowsafe/signal-providers`
(additive, opt-in, subpath-only). Host external-event providers on a Durable
Object with alarm-driven polling, terminate provider webhooks on the Worker, and
persist subscriptions in a flowsafe-owned D1 table.

- `SignalProviderHost` — a per-tenant provider host DO (`idFromName(tenantId)`)
  whose alarm rehydrates subscriptions from D1 (core's registry is in-memory,
  lost on eviction) and polls each of the tenant's providers with per-provider +
  per-delivery failure isolation, delivering through Track C's thread-DO topology.
- `D1SubscriptionStoreFactory` — a flowsafe-owned, tenant-columned
  `flowsafe_signal_subscriptions` store mirroring the approval store's INV-2
  posture (`.forTenant()` tenant-bound, `.system().listByResource()` the webhook's
  cross-tenant authority). Registered in `purgeTenant` (`PurgeTenantResult.subscriptions`);
  retention is `none` (standing config reaped only at offboarding).
- `createWebhookRouter` — webhook ingress that verifies the provider signature
  over the RAW bytes BEFORE parsing, maps the payload to a tenant via the
  subscription ROW only (never the payload), rate-caps per provider+tenant, and
  audits every ingest with a bounded forgery audit. `createSubscriptionRouter` —
  the human-only HTTP subscribe/unsubscribe surface (never exposed as model
  tools; mints no capability).
- `githubSignalProvider` — a binding-gated GitHub reference provider
  (`X-Hub-Signature-256` verified constant-time via WebCrypto). `createWebhookSignalProvider`
  is the generic path.

Also adds a `subscriptions` counter to `PurgeTenantResult` (the DL-003 offboarding
coverage for the new flowsafe-owned table).
