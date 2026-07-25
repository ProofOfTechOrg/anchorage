# signals/

Track C (M-004): signals, subscriptions, notifications — additive, opt-in
ingestion for the long-running-agents program. Subpath-only export
`@proofoftech/flowsafe/signals` (like `agent-runner`/`background-tasks`): host
wiring a consumer opts into, never the root barrel.

The two halves mirror the run surface's Worker-gate → DO-execution split:
`createSignalRouter` is the Worker-side P6 ingestion trust boundary (DL-006);
`createThreadSignalRoutes` runs on the per-thread DO after its tenant identity is
asserted (DL-002). Between them, `createThreadTopology` overwrites both trusted
tenant and actor headers from the resolved `TenantContext`.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `router.ts` | `createSignalRouter` — the P6 ingestion gate (DL-006): resolve → role → thread-prefix ownership (404, no oracle) → size-cap → memory-id refusal → attribute-key allowlist → per-tenant rate cap → audit → forward via the topology. Every ingest is audited (`signal.ingest`), accepted OR rejected, INCLUDING the three post-auth denials (role 403, cross-tenant 404, memory-id 400); pre-auth failures (401 / resolver throw) are not audited. `createInMemorySignalRateLimiter` is the single-instance default limiter | Changing the ingestion gate, its order, the audit surface, or the rate seam |
| `thread-do-routes.ts` | `createThreadSignalRoutes`: message, queue, signal, state, notification, and trusted due-notification routes. Active sends use core's public APIs. Idle wakes serialize an active-state recheck, consult the run cap, mint a salted run ID, and invoke `startIdleRun`; an absent or refused seam persists without starting. A separate notification promise lane serializes creation and the complete dispatch transaction without deadlocking the wake lane. Dispatch accepts at most 100 non-empty IDs, deduplicates them, priority-sorts summary and individual logical items together, re-fetches individuals immediately before handoff, and closes rows only after the destination handoff (including optional `persisted`) completes. One batch-initial active/idle snapshot is carried across tick chunks; only a summarized `high` row is suppressed while that snapshot is active. All-low summaries persist without consulting the cap or waking. Internal model/config failures return a generic 502 | Changing thread routes, wake serialization, affinity, or trusted notification delivery |
| `notification-dispatch.ts` | `createNotificationDispatchTick`: system-only global due read, independent tenant derivation from thread and resource IDs, four-part `(tenant, thread, resource, agent)` grouping, and the shared Mastra-parity logical-item planner (priority then `createdAt`). Item-aware packing retains the 100-id route bound without splitting summaries of at most 100; larger summaries become consecutive standalone 100/100/remainder fragments. The route echoes one batch-initial thread state across every group chunk, while failures remain isolated per request. The configured read limit may exceed 100; zero is an intentional no-op | Wiring scheduled notification delivery |
| `notifications-d1.ts` | `D1NotificationsStorage` over `mastra_notifications` (the AGENT inbox), mirroring core's InMemory reference. Every keyed create, including an explicit id, uses atomic conditional insert plus a complete-identity guarded coalescing CAS; omitted fallback fields are absent from the CAS `SET`, so concurrent partial updates compose. An internal monotonic `insertionOrdinal` (idempotently migrated/backfilled from physical legacy order and allocated through a prefix-scoped singleton sequence) makes the first coalescable target match core's `Map` insertion order; same-id upserts retain their ordinal. The ordinal migration runs in one required D1 transactional batch, so rollback writers see either the old schema or the fully backfilled sequence + triggers, never an exposed half-migration. Compatibility triggers put later rollback-era inserts into the same sequence and turn their same-id `INSERT OR REPLACE` into an in-place update. Due ordering/limits run in SQL | Changing the notifications domain, coalescing, schema migration, or update concurrency |
| `thread-state-d1.ts` | `D1ThreadStateStorage` over `mastra_thread_state` — the state-signal lanes (snapshot/delta, cacheKey dedupe) and, reused by Track F, the goal record | Changing the thread-state domain |
| `d1-shared.ts` | The structural `SignalDatabase`/`SignalStatement` D1 subset (workers-types-free) both domains share, plus the ISO/JSON column helpers | Changing the D1 seam or column encodings |
| `storage.ts` | `createSignalStorageDomains(binding)` — packages the two D1 domains for injection into `createD1Storage({ domains })` (do-runner cannot import signals directly — it would cycle) | Wiring D1-durable notifications + thread-state into a host |
| `client.ts` | `SignalClient`: a DOM-free, import-light client with injected fetch. Browser consumers import `@proofoftech/flowsafe/signals/client` | Changing the client wire surface |
| `index.ts` | Subpath barrel (`@proofoftech/flowsafe/signals`) | Finding the signals export surface |
| `router.test.ts` | The P6 gate: order, each fail-closed status, and the audit assertions (accepted + the three post-auth rejections; 401 NOT audited) | Changing the gate or its audit |
| `thread-do-routes.test.ts` | The thread routes: affinity stamp, channel routing, the wake gate (runtime-driven vs plain agent), the tagName defense, state de-dupe — plus the C-S5 `signalToXmlMarkup` render pin (core neutralizes contents/attribute values) | Changing the thread routes or the C-S5 pin |
| `notifications-d1.test.ts` | Round-trip, explicit-id coalescing, insertion-order migration, two-adapter first-create/map-merge/lost-update/replaced-identity races, disjoint updates, empty-array filters, and `listDue` (`summaryAt`, `<=` boundary, SQL ordering + limit) | Changing the notifications domain |
| `thread-state-d1.test.ts` | Thread-state round-trip / snapshot-replace / delete | Changing the thread-state domain |
| `client.test.ts` | `SignalClient` wire-format + error mapping | Changing the client |
| `signal-ingestion.integration.test.ts` | The FULL chain, no LLM: `createSignalRouter` → real `createThreadTopology` → real `ThreadDurableObject` → a runtime-driven reserve agent — idle wake with the run cap allowing and capping, and a foreign threadId 404'd at the topology | Changing any seam on the ingestion boundary |

## Retained live-subscription requirement

- **Phase-2 live subscribe must be scoped SERVER-side per thread / per
  resourceId** — NOT the per-tenant Part B hub + client-side threadId filter,
  which would put one end-user's thread bytes on every same-tenant operator's
  socket (a within-tenant confidentiality leak). See `client.ts` (DL-016
  corrected).

## Browser-clean barrel (F3-arch)

The `signals` **barrel** (`index.ts`) transitively pulls the Node graph — `router`
imports host-kit + do-runner, `thread-do-routes` imports `agent-runner` (which
drags the durable `Agent` and `@mastra`'s Node built-ins). So the SPA MUST import
`SignalClient` from `@proofoftech/flowsafe/signals/client`, never the barrel. The
packed-consumer test resolves this export under browser-oriented module
resolution and verifies that it imports no Node-only runtime modules. This mirrors the
breakwater browser-clean-subpath rule (only `/policy-engine` `/rbac` `/audit` are
bundle-safe). `client.ts` imports nothing heavy; keep it that way.
