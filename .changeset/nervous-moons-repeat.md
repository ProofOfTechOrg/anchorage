---
'@proofoftech/flowsafe': minor
---

Track 0 (substrate for the long-running-agents program): close the agent-memory
tenancy obligations and add the seams the agent tracks build on. All additive —
a host that configures none of it is byte-identical.

- **Agent-memory host boundary** (`@proofoftech/flowsafe/host-kit`):
  `assertNoClientMemoryIds(body)` rejects (400) any request body naming
  `threadId`/`resourceId` — memory ids are minted server-side from the
  authenticated tenant via `TenantContext.newThreadId()/newResourceId()`, never
  chosen by a client — and `requireOwnedMemoryId(tenant, id)` answers 404 (not
  403, so no existence oracle) on a foreign id. Every memory-touching route
  calls both.
- **Recall-path proof**: core's own `MastraMemory` implementation over the real
  D1 store, two tenants keyed by the SAME business key, pinning that `recall()`,
  `listThreads({filter:{resourceId}})`, and resource-scoped `getWorkingMemory()`
  never cross tenants.
- **Thread TTL**: `purgeExpiredThreads(db, { ttlMs, limit, tablePrefix })`, wired
  into `createFlowsafeWorker`'s purge cron behind the new `THREAD_RETENTION_DAYS`
  var with its own failure isolation. Keyed on `mastra_threads.updatedAt`
  (threads are not per-run and have no terminal status); messages go with their
  thread and before it; working-memory rows are untouched. Unset by default — no
  thread expires until an operator names a number.
- **Extensible purge/guard inventory** (`TENANT_RANGE_PURGE_TABLES`,
  `TenantRangePurgeTable`, `TenantRangePurgeCounter`): adopting a `mastra_*`
  domain is now one additive row plus the counter/result pair the types force in
  the same change; the schema guard still trips on any silently added table.
- **Host pubsub identity** (`createHostPubSub`, `HostPubSub`): the seam for one
  in-process `EventEmitterPubSub` per host DO — passed to `init()` (new
  `InitOptions.pubsub`) and taken back off `InitResult.pubsub`, so every consumer
  in the isolate shares one emitter instead of each letting core default its own
  (two such feeds never see each other's events). The identity and the seam only:
  nothing passes it to core's `createRun` yet, so a configured pubsub is an
  identity the host holds, not yet a feed core publishes on. Opt-in; absent
  leaves polling as the fallback.
- **`ThreadDurableObject`**: per-thread DO base addressed `idFromName(threadId)`
  where the threadId is tenant-minted, so its name carries the tenant like a
  runId. Every request must state its authenticated tenant
  (`THREAD_TENANT_HEADER`) and is asserted against that prefix before the
  subclass's `route()` runs — fail closed (403).
