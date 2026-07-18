# signal-providers/

Track E (M-007): signal providers — host external-event providers on a Durable
Object with alarm-driven polling, terminate provider webhooks on the Worker, and
persist subscriptions in a flowsafe-owned D1 table. Additive, opt-in, subpath-only
(`@proofoftech/flowsafe/signal-providers`, like agent-runner / background-tasks /
signals / schedules / goals): host wiring a consumer opts into, never the root
barrel.

**The pre-flight B/D finding (DL-017).** Core's `SignalProvider`
(`@mastra/core/signals`) delivers IN-PROCESS — `poll()` / `handleWebhook()` call
`this.notify()` → `this.agent.sendNotificationSignal()` on a CONNECTED agent, and
match webhooks against an in-memory subscription `Map`. Neither fits a Cloudflare
provider host: the agent loop runs on a DIFFERENT (per-thread) DO, and the
in-memory registry is empty after eviction and tenant-blind. So flowsafe drives
providers through the `SignalProviderAdapter` seam and routes every delivery
through host-kit's `createThreadTopology.send` into Track C's
`/signal/notification` thread-DO route — the DO alarm replaces core's
`startPolling` timer, and the D1 subscription store replaces its registry. A
provider built here still IS a core `SignalProvider` (it extends the base), so
`isSignalProvider(p)` holds and `new Agent({ signals: [p] })` still merges it for
IN-PROCESS use; the DO host simply does not take that path.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `provider.ts` | `SignalProviderAdapter` (the DO-host + webhook contract: `verifyWebhookSignature` over RAW bytes before parse, `extractResourceIds`, `buildNotification`, `pollForDeliveries`) + `createWebhookSignalProvider` (the generic path — extends core's `SignalProvider`, presence-accurate optional seams so a webhook-only provider has no `pollForDeliveries` and a poll-only one no verify/extract) + `PROVIDER_ID_PATTERN` (lowercase slug, NO `_` — it delimits the tenant in the host-DO name). Core's `SignalSubscription`/`SendNotificationSignalInput` are used DIRECTLY (both exports-reachable), so nothing is mirrored | Building a provider, changing the adapter contract |
| `github-provider.ts` | `githubSignalProvider` — the binding-gated showcase reference. `X-Hub-Signature-256` (`sha256=<hex HMAC-SHA256>`) verified over the RAW bytes, CONSTANT-TIME via `crypto.subtle.verify` (never a string compare); `github:<full_name>` + `github:<full_name>#<number>` resource extraction; a GitHub notification shape. Webhook-only (no poll). Mints no capability (P8) | Changing the GitHub provider or its signature scheme |
| `subscription-d1.ts` (CI-M-007-002) | The flowsafe-OWNED `flowsafe_signal_subscriptions` D1 store (NOT a `mastra_*` table): `D1SubscriptionStoreFactory` (`.forTenant()` tenant-bound INV-2 via the `SUBSCRIPTION_TENANT_BOUND` brand + `.system()` the cross-tenant webhook view) mirroring the approval store; `InMemorySubscriptionStoreFactory` (+ `purgeTenant`); `StoredSubscription` (core shape + `tenantId`); `listForProvider` (the host-DO rehydration query), `listByResource` (the webhook's tenant authority — a webhook names no tenant, the ROW does). Offboarding is do-runner's `purgeTenant` (`WHERE tenant_id = ?`, the `flowsafe_approvals` leg, `PurgeTenantResult.subscriptions`); retention is `none` — standing config reaped only at offboarding, so absent from the run/thread TTL purges | Changing the subscription store, its schema, or the INV-2 binding |
| `delivery.ts` | `deliverNotification` (routes one notification through `createThreadTopology.send` into `/signal/notification`) + `deliveryTenantContext` (a SERVER-DERIVED TenantContext from a trusted row `tenantId` — `service()` throws, delivery never has a request). The row IS the authority; a row tampered to a foreign threadId 404s at the topology (fail closed, no cross-tenant delivery) | Changing the delivery path |
| `host-do.ts` (CI-M-007-001) | `SignalProviderHost` — the per-tenant provider host DO (`idFromName(tenantId)`, the HubDurableObject pattern; id.name IS the bare tenantId). Alarm-driven `poll()` across the tenant's providers, each provider's poll+delivery in its OWN try/catch (PER-PROVIDER failure isolation) and each delivery isolated (a tampered/failing one never aborts the batch); rehydrates subscriptions from D1 each poll (eviction-survivable). `POST /arm` (boot + arm), `POST /poll` (the deterministic E-S3 probe), `alarm()` (poll + re-arm; self-terminates via `deleteAlarm` when nothing is left to poll). Node-loadable (classic ctor+fetch, no `cloudflare:workers`), the ThreadDurableObject posture | Building/changing the host DO, the alarm lifecycle, the isolation |
| `webhook-route.ts` (CI-M-007-003) | `createWebhookRouter` — webhook ingress: verify-BEFORE-parse → row lookup → per provider+tenant rate cap → audit (accepted + a BOUNDED forgery audit so a flood cannot amplify the log) → deliver (per-delivery isolated). Route-absent (null) when the provider/secret is unconfigured. `createSubscriptionRouter` — the human-only HTTP subscribe/unsubscribe surface (RA-009: NEVER model tools; P8: no capability): the createSignalRouter gate order (resolve → role → thread-ownership 404 → `assertNoClientMemoryIds` → mutate), `resourceKey` (the thread owner business key, NOT a memory id) server-minted into the resourceId, every outcome audited | Changing the webhook gate, the subscription CRUD, or the audit surface |
| `index.ts` | Subpath barrel (`@proofoftech/flowsafe/signal-providers`) | Finding the export surface |
| `*.test.ts` | `subscription-d1` (both backends, forTenant isolation, `listByResource` cross-tenant, two-tenant purge), `github-provider` (signature verify incl. wrong-secret/tampered/malformed, extraction, notification), `webhook-route` (verify-before-parse + no-lookup-on-forgery, payload→tenant-via-row, rate cap, bounded forgery audit, byte-identity, cross-tenant fail-closed), `host-do` (rehydration, per-provider + per-delivery isolation, alarm re-arm, self-terminate, identity 403), `subscription-routes` (the gate matrix), `webhook-ingestion.integration` (E-S1: the FULL chain — webhook → topology → real ThreadDurableObject → a Mastra-registered agent → mastra_notifications, visible) | Changing any seam |

## Delivery keying (core's one-owner-per-thread model)

A thread has exactly ONE owner `resourceId` in core's memory model. A
subscription's `resourceId` IS that owner, and the thread DO's
`resolveResourceId(scope)` returns the SAME value, so a delivered notification
keys the inbox identically whether the signal arrived from a client ingest
(Track C) or a provider webhook/poll (here). A host wires both consistently (the
spike uses `mintResourceId(tenant, 'demo-thread')` for both).

## Known limitation — subscribe-time resource authorization (shared-secret fan-out)

The subscribe route gates on tenant + role + thread ownership, but does NOT (and
cannot, from a webhook alone) verify that a tenant OWNS the external resource it
names — any allowed-role actor can subscribe its own thread to any
`externalResourceId` string. Combined with the webhook's cross-tenant fan-out
(`system().listByResource`, the row-is-authority design), this means: under a
SINGLE shared provider secret/endpoint, if two tenants both point their real-world
assets (e.g. GitHub repos) at the same webhook URL, tenant A subscribing to tenant
B's resource string receives a copy of B's authentic events. The verify-before-parse
+ per-row ownership-404 guarantees hold (no tenant receives anything but its OWN
subscriptions' notifications, and never into a foreign thread) — the exposure is
purely that subscribing to a string is unauthenticated w.r.t. the external world.

The mitigation is a DEPLOYMENT choice, not a code change here: a multi-tenant host
that must isolate this uses PER-TENANT webhook endpoints (a tenant-scoped path) with
per-tenant secrets, so a webhook is bound to one tenant by URL + signing key before
the row lookup. The `secretForProvider` seam is per-provider by design (the signature
is verified before the tenant is known); per-tenant binding is layered above it by
the host. Documented as a residual for the program close-out.

## Deploy note

The minimal `deploy/` template has NO per-thread DO (Track C/D/F kept it free of
signals/schedules/goals surfaces too), and signal-provider delivery REQUIRES a
thread DO. So signal-providers is wired by hosts that have one — proven
end-to-end on the workerd spike (`spike/worker.ts`: `DemoSignalProviderHost` +
the `/sigp/*` probes; `scripts/spike-verify.mjs` steps P–S). A host adopting it
binds the provider host DO under a new migration tag and mounts the webhook +
subscribe routes (the webhook unauthenticated/signature-authed, subscribe through
the resolver).
