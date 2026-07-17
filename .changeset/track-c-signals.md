---
'@proofoftech/flowsafe': minor
---

Track C (long-running-agents program, M-004): signals, subscriptions, and
notifications. All additive and opt-in — a host that configures none of it is
byte-identical.

New subpath `@proofoftech/flowsafe/signals`:

- **Thread-DO signal routes** (`createThreadSignalRoutes`) — the
  message/queue/signal/state/notification surface hosted on the per-thread DO.
  Each call stamps the DO's one pubsub identity (`scope.init.pubsub`) onto the
  agent so a send drains IN-PROCESS into an active loop: core keys its signal
  registry by the pubsub instance, so affinity needs both the DO isolate
  (idFromName(threadId)) and the shared pubsub — the DL-002 thesis, proven on
  workerd (spike C-S2). An idle-thread WAKE starts a run, so it requires a
  runtime-driven durable agent (`createFlowsafeDurableAgent`, which carries the
  new `RUNTIME_DRIVEN_AGENT` brand from `@proofoftech/flowsafe/agent-runner`) —
  its stream re-enters RunnerRuntime rather than the default engine; a wake
  requested on a plain agent is refused fail-closed (degraded to a durable
  persist), and every allowed wake consults the per-tenant run cap (DL-007). The
  routes drive the public Agent methods only (`agentThreadStreamRuntime` is not
  on core's exports map).
- **D1 storage domains** — `D1NotificationsStorage` (over `mastra_notifications`,
  mirroring core's InMemory reference incl. coalescing) and
  `D1ThreadStateStorage` (over `mastra_thread_state`, the state-signal lanes and
  the goal record). `@mastra/cloudflare-d1` ships neither, so they are
  flowsafe-owned; `createSignalStorageDomains` composes them into
  `createD1Storage` (which now accepts injected `domains`) so
  `agent.sendNotificationSignal` persists to D1. Both tables are registered in
  the tenant-range offboarding purge (`purgeTenant`), the schema-guard inventory,
  and their own opt-in TTL purges (`purgeExpiredNotifications`,
  `purgeExpiredThreadState`) — the DL-003 triad, in one change. `PurgeTenantResult`
  gains `notifications` + `threadState` counters (a breaking change only for
  callers that build a `PurgeTenantResult` literal).
- **P6 ingestion trust boundary** (`createSignalRouter`) — every ingest is
  authenticated → role-gated → thread-prefix-ownership-checked (404, no oracle) →
  size-capped → memory-id-refused → attribute-allowlisted → per-tenant rate-capped
  → forwarded via `createThreadTopology` (which overwrites the tenant header, so a
  forged one cannot ride along; cross-tenant sends fail closed at both the topology
  404 and the DO 403, spike C-S4). Every ingest is audited (`signal.ingest`),
  accepted OR rejected — including the three post-auth denials that read like an
  attack on this channel (the role 403, the cross-tenant thread 404, the
  memory-id 400); pre-auth failures (401 / resolver throw) are not audited. XML
  injection is neutralized by core's `signalToXmlMarkup`, which entity-escapes
  contents and attribute values and re-validates tag/attribute names — a single
  layer over a soft-pinned core, so a C-S5 render test pins it (a core `escapeXml`
  regression fails flowsafe CI); the route adds its own line at ingest —
  `tagName` XML-name validation, the attribute-key allowlist, and the size cap —
  but does not re-escape the contents. Signals never mint capability —
  `sendToolApproval` is not an approval surface (P8).
- **`SignalClient`** — a DOM-free client in the `ApprovalApiClient` mold.

`createFlowsafeWorker` gains an opt-in signal stage (`buildSignalRouter` seam) and
two opt-in TTL cron duties (`NOTIFICATION_RETENTION_DAYS`,
`THREAD_STATE_RETENTION_DAYS`).
