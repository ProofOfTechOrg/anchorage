# @proofoftech/flowsafe

## 0.7.0

### Minor Changes

- def3b37: Complete and document the public flowsafe surface. The root entry point now
  re-exports the approval API, Durable Object runner, artifacts, and audit export
  surfaces with parity tests. Signal-provider hosts gain a tenant-safe topology,
  stable polling alarms, and automatic post-mutation polling reconciliation.
  Publish comprehensive package, deployment, approval, durable-agent, operations,
  and API-reference documentation, plus a full advanced starter host.

## 0.6.0

> **Final 0.6.0 state:** The `eca3b6e` closeout entry below supersedes earlier
> statements in this release section that agent schedule targets and durable D1
> background-task execution were deferred. Both are supported in 0.6.0 when
> their opt-in host wiring is configured. Durable-agent restart resume and
> notification dispatch are also closed and verified.

### Minor Changes

- eca3b6e: Close the durable-agent, agent-schedule, notification-dispatch, and D1 background-task execution residuals with tenant-safe thread routing and eviction-safe approval resume. Harden stored schedule context and core schedule-contract validation; make D1 notification creation preserve explicit-id coalescing, insertion-order targets, rollback-safe atomic migration, and concurrent partial updates; priority-plan summary and individual delivery across state-stable 100-id batches; accept Mastra's raw constructor pubsub with rollback-safe workers and a synchronous enqueue shutdown gate; close failed resume streams; validate public numeric configuration synchronously; preserve nested Mastra background-task SSE events; and require a proven process shutdown before spike restart.
- d54d2be: Track D — schedules (`@proofoftech/flowsafe/schedules`, additive, opt-in). A new
  subpath ships the D1 schedules domain, a CAS-driven tick we own, and a tenant
  facade — all on the single DO + D1 RunnerRuntime substrate (P1), no new
  `ApprovalRecord` shape or existing signature changed, unconfigured hosts
  byte-identical.

  - `D1SchedulesStorage` + `createScheduleStorageDomains` — the flowsafe-owned D1
    domain over `mastra_schedules` / `mastra_schedule_triggers` (the
    `@mastra/cloudflare-d1` adapter ships neither), mirroring core's
    `SchedulesStorage` contract incl. the CAS `updateScheduleNextFire`. Composed
    into `createD1Storage` via the injected `domains` seam.
  - `createScheduleTick` — we OWN the tick (DL-012): `listDueSchedules` → CAS claim
    → workflow targets mint a fresh INV-1 runId and fire through the host's
    run-start seam; agent targets are GUARDED OFF (their only public fire path,
    `schedules.run(id)`, enqueues onto core's pubsub worker loop we do not run, so
    firing is a fail-closed audited skip — agent-target execution is deferred). An
    injectable run-cap seam (DL-007) skips a capped tenant while the schedule stays
    healthy. The P4 stored-context barrier strips reserved keys before any leg.
  - `createScheduleRouter` — the tenant facade (DL-013): server-minted ids,
    `metadata.tenantId` stamping, tenant-filtered reads, ownership 404s (no
    oracle), per-tenant count + fire-rate caps, and P4 reserved-key rejection on
    create/update (the whole `breakwater.` namespace + `mastra:goal`).
  - Storage triad (DL-003): both tables register in the schema-guard inventory
    (8 → 10) with a new metadata-filtered `purgeTenant` kind
    (`TENANT_METADATA_PURGE_TABLES`), plus `purgeExpiredScheduleTriggers` for the
    trigger-history TTL. `createFlowsafeWorker` gains an opt-in `scheduleTick` seam
    (its own failure-isolated cron duty) and a `SCHEDULE_TRIGGER_RETENTION_DAYS`
    purge duty.

- 0f4f70a: Track E (M-007) — signal providers: a new subpath `@proofoftech/flowsafe/signal-providers`
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

- 6c80e92: Track F (M-005) — goals. New subpath-only export `@proofoftech/flowsafe/goals`:
  `createObjectiveRouter`, a role-gated + audited objective HTTP surface
  (set/get/update/clear over `/api/threads/:threadId/goal`) that writes the
  thread-scoped goal record in Track C's `mastra_thread_state` domain
  (`GOAL_STATE_TYPE` 'goal'). The write path is a P6-lite ingestion boundary
  (auth → coarse role → thread-prefix ownership 404 → size cap →
  `assertNoClientMemoryIds` → field allowlist → maxRuns host cap → audit) and
  persists through `@mastra/core`'s own `writeObjective`/`readObjective`/
  `clearObjective`, so a record it writes is byte-identical to what the durable
  goal step reads via `resolveGoalStore` (DL-018 — no thread-DO affinity needed
  for the write). A requested `maxRuns` above the host cap is rejected, not
  clamped (default the core `DEFAULT_GOAL_MAX_RUNS`, 50; DL-007). Goals never mint
  capability (P8) and Track F starts no runs — per-tenant run budgets stay
  enforced at the existing seams. `GOAL_REQUEST_CONTEXT_KEY` ('mastra:goal') is
  reserved with a no-collision pin against the runtime's requestContext base keys.
  Hosts mount the surface opt-in through `createFlowsafeWorker`'s new
  `buildObjectiveRouter` seam; absent config is byte-identical. Additive only — no
  new table, schema-guard, purge, or TTL change (reuses the Track C thread-state
  domain), and no existing signature or `ApprovalRecord` shape changes.

### Patch Changes

- 8e3562f: Make approval filing atomic across terminal and open records for the same captured suspension fingerprint, preventing stale reconciliation from filing over a decision.
- 97cb097: Harden the P6 ingestion routers against a pre-auth malformed-path fault. The
  signal, goal, and schedule routers decoded the threadId/schedule-id path
  segment with bare `decodeURIComponent` before authentication, and
  `createFlowsafeWorker`'s fetch handler did not wrap the router calls — so an
  unauthenticated request with malformed percent-encoding (e.g.
  `POST /api/threads/%/message`) threw a `URIError` out of `fetch()` as a
  per-request 500. The three routers now use a shared `safeDecodeSegment`
  (host-kit) that treats malformed encoding as route-absent (byte-identical to a
  non-matching path), matching the Track E webhook router; the worker fetch
  handler gains a top-level try/catch that contains any handler throw as a
  generic 500 without leaking `error.message`. The same helper closes the whole
  class: the background-tasks read route (post-auth, DO-mounted) adopts it too, so
  a malformed taskId returns the no-oracle 404 instead of throwing. Additive and
  behavior-preserving for all valid paths.

## Unpublished 0.5.0 draft (included in 0.6.0)

No `@proofoftech/flowsafe@0.5.0` package was published. These changes shipped in
0.6.0 and remain here as their original generated release notes.

### Minor Changes

- 281c6d1: Track 0 (substrate for the long-running-agents program): close the agent-memory
  tenancy obligations and add the seams the agent tracks build on. All additive —
  a host that configures none of it is byte-identical.

  - **Agent-memory host boundary** (`@proofoftech/flowsafe/host-kit`):
    `assertNoClientMemoryIds(body)` rejects (400) any request body naming
    `threadId`/`resourceId` — memory ids are minted server-side from the
    authenticated tenant via `TenantContext.newThreadId()/newResourceId()`, never
    chosen by a client — and `requireOwnedMemoryId(tenant, id)` answers 404 (not
    403, so no existence oracle) on a foreign id. Every memory-touching route MUST
    call both (the rule the agent-domain routes in later tracks adopt).
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
    the same change; the schema guard still trips on any silently added table, and
    its inventory now also forces each table's retention story — where "no TTL"
    demands a written reason, so an absent decision cannot read as "none needed".
  - **Host pubsub identity** (`createHostPubSub`, `HostPubSub`): the seam for one
    in-process `EventEmitterPubSub` per host DO — passed to `init()` (new
    `InitOptions.pubsub`), taken back off `InitResult.pubsub`, and threaded into
    the runtime (new `RunnerRuntimeOptions.pubsub`, readable as
    `RunnerRuntime.pubsub`) so a host reaches it with no host change. Every
    consumer in the isolate then shares one emitter instead of each letting core
    default its own (two such feeds never see each other's events). The identity
    and the seam only: nothing passes it to core's `createRun` yet, so a configured
    pubsub is an identity the host holds, not yet a feed core publishes on. Opt-in;
    absent leaves polling as the fallback.
  - **`ThreadDurableObject`**: per-thread DO base addressed `idFromName(threadId)`
    where the threadId is tenant-minted, so its name carries the tenant like a
    runId. Every request must state its authenticated tenant
    (`THREAD_TENANT_HEADER`) and is asserted against that prefix before the
    subclass's `route()` runs — fail closed (403). Everything else it throws rides
    the shared `doErrorResponse` taxonomy, so a run driven from a thread route
    keeps its 404/409/400 instead of collapsing to a 500.
  - **`createThreadTopology`** (`@proofoftech/flowsafe/host-kit`): the sanctioned
    way to reach a per-thread DO, and the MINTER for the header
    `ThreadDurableObject` verifies. `send`/`forward` refuse (404) a threadId the
    authenticated tenant does not own — before the DO is addressed — and stamp
    `x-flowsafe-tenant` from the resolved `TenantContext`, `forward` OVERWRITING
    whatever a client's own request carried. Mint and verify ship together:
    forwarding a client Request verbatim (the existing hub idiom) would otherwise
    let the client write the very header the thread DO authenticates on.

- 4ea35fd: Track A (durable agents): drive Mastra's durable-agent loop through the one
  RunnerRuntime chokepoint so agent legs inherit the substrate's invariants —
  additive and opt-in, no existing signature or `ApprovalRecord` shape changed.

  - `@proofoftech/flowsafe/agent-runner` (new subpath): `createFlowsafeDurableAgent`
    returns a `DurableAgent` subclass whose `executeWorkflow(runId, workflowInput)`
    calls `runtime.start('durable-agentic-loop', { runId, inputData })` instead of
    the base `createRun + start` (DL-001/DL-010), so INV-1 (server-minted runId),
    the per-leg `requestContextForRun` grant derivation, and the resume ledger
    apply to agent legs. `stream()`/`generate()`/`prepare()` are overridden to
    REQUIRE a caller-minted runId — closing every inherited minting entry point's
    upstream `crypto.randomUUID()` fallback (INV-1: a durable-agent run must carry
    its tenant everywhere it becomes a key). The shared loop workflow is registered on
    the runtime idempotently; the agent's stream pubsub defaults to the runtime's
    identity (one feed per DO). `resume()` stays non-client-facing — resume flows
    only through the approval-decision path (grant-only doctrine, P8).
  - The durable tool-call step hands `tool.execute` the ENGINE-LEG requestContext
    from its step params (spike S1, verified against `@mastra/core` 1.50.0 dist),
    so the `breakwater.approvedConnectors` grant reaches the connector write gate
    with zero extra wiring and a forged/self resume fails closed there.
  - R-003: the record-creation/bridge path parses BOTH durable approval-suspend
    shapes — nested `{ type:'approval', requireToolApproval:{ toolCallId, toolName,
args } }` and flat `{ type:'approval', toolCallId, toolName, args }` — and
    derives `connectors:[toolName]` (the connector id the write gate checks) so an
    approved agent gate mints exactly that grant. The resume-routing
    `threadId`-capture seam (DL-002) is deferred to Track C, where the thread-DO
    consumes it.
  - `RunnerRuntime` now threads the host pubsub identity into both `createRun`
    sites (`createRun({ runId, pubsub })`); undefined leaves behavior
    byte-identical (polling fallback).
  - The workerd `spike:verify` grows agent-gate scenarios proving the R-003
    round-trip and forged-resume fail-closed on real workerd + D1.

- 15d4ec3: Track B (background tasks): the additive, opt-in substrate + defenses for
  Mastra background tasks on the one Durable-Object + D1 chokepoint. No existing
  signature or the `ApprovalRecord` shape changed; hosts stay byte-identical with
  background tasks unconfigured.

  - **breakwater `_background` model-override defense (DL-005), the ONE breakwater
    change (MINOR).** `createConnector`'s wrapped `execute`/`dryRunExecute` reject
    tool-call args carrying a `_background` field (core `LLMBackgroundOverride`)
    unless the manifest opts in via `permissions.background` — the argv-flag-
    smuggling posture of the agent-cli `buildFlags` defense. `background: true` is
    allowed only on a read-only connector (a write-class opt-in throws at
    construction); v1 keeps write/approval-carrying connectors foreground-only.
    Plus a `backgroundExecution` tool-policy evaluator (deny-by-default for the
    write class) as the defense-in-depth counterpart at the gate loop. Both are
    DEFENSE-IN-DEPTH for DIRECT / NESTED calls, NOT the agent-path guard: on the
    agent path core deletes `_background` from the args before dispatch (schema or
    not), and core's own `resolveBackgroundConfig` baseEnabled gate — a breakwater
    connector sets no background config — already prevents the model from
    backgrounding an ineligible tool, so the breakwater reads see stripped args and
    fire on nothing there. The real write boundary on every path (including inside
    the background executor) is the requestContext grant.
  - **`mastra_background_tasks` adopted into the D1 substrate in ONE change
    (DL-003).** Registered in the schema-guard inventory (coverage `tenant-range`,
    a new `background-task-ttl` retention kind), in `purgeTenant` (ranged over the
    INV-1 salted `run_id`; new `PurgeTenantResult.backgroundTasks`), and given a
    storage-layer TTL cleanup `purgeExpiredBackgroundTasks` (+
    `BACKGROUND_TASK_TTL_PURGE_TABLES`) mirroring core's two-window
    `BackgroundTaskManager.cleanup` so a purge cron reaps terminal rows without a
    live manager. Surfaced through `FlowsafeWorkerConfig.backgroundTasks` as the
    purge cron's own failure-isolated duty (undefined = no duty, byte-identical).
  - **`@proofoftech/flowsafe/background-tasks` (new subpath):** `backgroundTasksStore`
    (the async accessor onto @mastra/cloudflare-d1's `BackgroundTasksStorageD1` —
    the D1 domain the adapter already ships; not reimplemented, per "what NOT to
    build"), `BackgroundTaskHost` (hosts a `BackgroundTaskManager` on a DO with the
    DL-015 boot/alarm lifecycle), and `createBackgroundTaskRoutes` (READ-only,
    tenant-bound by construction, DL-014: list/stream REQUIRE a runId/threadId
    filter and validate its salted prefix; `getTask` 404s a missing OR foreign
    task with no oracle; the raw manager is never exposed).
  - **Recovery seam pinned (R-002, spike B-S2):** DO eviction is survived by
    re-registering the static tool executors and calling the PUBLIC async
    `manager.init(pubsub)` at DO boot — which fires the manager's own (private)
    `recoverStaleTasks()` internally. No private method is ever called.

  **Known substrate limitation (spike B-S1 findings R-B1/R-B2/R-B3, documented in
  `background-tasks/host.ts`):** durable background-task _execution_ does not yet
  run on the Cloudflare substrate. Core runs task bodies on the _evented_
  execution engine, which refuses to `createRun` unless the workflows store
  reports `supportsConcurrentUpdates()`. `@mastra/cloudflare-d1` returns `false`
  AND leaves `updateWorkflowResults`/`updateWorkflowState` as unimplemented throws
  ("D1 does not support atomic read-modify-write") — so R-B1 is NOT a flag to
  flip: overriding it passes core's gate then throws on the first step-update,
  stranding the task at `running`. The P9 fix is an adapter that _implements_
  atomic partial-updates (the DO's single-threaded lease makes that safe), plus
  `mastra.startWorkers()` to run the evented workers (R-B2 — the two close
  together). A latent tenant-isolation residual (R-B3) rides along: core keys the
  internal `__background-task` run by the UNSALTED `taskId`, so its snapshot row
  escapes tenant offboarding — inert while execution is blocked, but it MUST be
  closed in the same change that enables execution, and a CI guard
  (`background-tasks/d1-storage.test.ts`) fails the instant
  `supportsConcurrentUpdates()` returns true. Persistence, the recovery seam,
  tenant purge + TTL, the read routes, and the `_background` defense all work
  regardless. `BackgroundTaskHost.boot()` warns once so the limitation is loud,
  not a stray async throw.

- 4b953d4: Track C (long-running-agents program, M-004): signals, subscriptions, and
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

## 0.4.0

### Minor Changes

- 0c108fa: Harden seven defects found in the dev whole-codebase review (2026-07-13). Every fix removes a root cause across its whole class and fails closed.

  flowsafe:

  - **F1 (security): close the cross-gate separation-of-duties race.** `ApprovalService.decide` now enforces the SoD guarantee from the run's own approved history instead of relying on `requestedBy` attribution: a non-exempt decider who already approved an earlier gate of the same run (any prior approval whose `decidedAt` is at or before this gate's `createdAt`) is refused. This is immune to the reconcile path filing the next gate as the system actor, which previously let one reviewer clear both gates. The approved-history read pages to exhaustion (fails closed past the list default) and the causal anchor never over-blocks independent parallel gates or a reject then re-review by the same reviewer. **Behavior change for operators:** with `allowSelfDecision` off, a single reviewer can no longer advance a sequential multi-gate run alone, and a multi-round same-step review needs a fresh reviewer per round; set `allowSelfDecision` (the demo uses `{ roles: ['admin'] }`) to permit one operator to clear multiple gates. An unparseable timestamp bars (fail-closed) rather than passing.
  - **F4 (durability): pair R2 artifact deletion with the retention purge.** `FlowsafeWorkerConfig` gains an optional `artifactStore` seam that `runPurgeMaintenance` threads into the built-in purge, so each expired run's artifacts are deleted before its snapshot row (the only enumerable record of their keys). The deploy template comment now points copiers at this field instead of `extraPurgeDuties`, which runs after the rows are gone.
  - **F2 (security): reject a non-string tenantId before INV-3 coercion.** A `typeof` guard now precedes `TENANT_ID_PATTERN.test` at every externally-typed site (the resolver belt, `assertMintableTenantId`, `assertTenantId`, both store constructors, and the exported `provisionTenant` and `purgeTenant`), so a non-string principal can no longer coerce to a matching slug and collapse into a shared tenant bucket.
  - **F3 (availability): survive a create-vs-decide race in D1.** `D1ApprovalStore.create` retries the insert once when a concurrent decision closes the conflicting open row between the failed insert and the open-row lookup, honouring the idempotent-create contract instead of surfacing a raw unique violation.
  - **F6 (correctness): validate list time bounds eagerly in memory.** Both in-memory approval-store list paths now reject an unparseable `createdBefore`/`createdAfter` even with zero matching records, matching D1.

  breakwater:

  - **F5 (correctness): make the high-entropy candidate floor track the configured threshold.** The candidate length floor is now derived from the effective `entropyThreshold` (`max(20, ceil(2 ** threshold))`) instead of a constant tuned to the 4.5 default, so lowering the threshold no longer silently drops short-secret detection. Default behavior is unchanged.
  - **F7 (correctness): reject a connector id containing a colon at construction.** `createConnector` throws when `id` contains `:`, which would otherwise collide two distinct tuples on the shared idempotency and rate-limit store keys. No shipped id is affected.

- dbe6a93: Add live streaming over WebSocket-over-Durable-Object so the approval dashboard and run-status views update within one round-trip instead of on the 3s/5s poll, tenant-isolated, with polling retained as a graceful fallback. Streaming is opt-in: a host that wires no hub binding or ticket secret keeps working unchanged on poll-only.

  - **approval-api**: a new `ApprovalStreamEvent`/`ApprovalStreamSink` seam (distinct from the reviewer-facing notification sink). `ApprovalService` fires it fire-and-forget on every successful create/claim/decide/delegate/supersede, and `sweepSLA` on each escalation; a throwing or rejecting sink never fails the mutation and is audited.
  - **do-runner**: a new per-tenant `HubDurableObject` base that accepts hibernatable WebSocket subscribers, fans out approval events, and tracks a presence roster, asserting `id.name` equals the event tenant; and a per-run WebSocket route on the runner DO that broadcasts the authoritative `RunSummary` at each lifecycle boundary. The structural DO state types are widened for the Hibernatable-WebSocket API with an `AssertTrue` compile pin, and no `cloudflare:workers` import enters the node/vitest graph. Every fan-out is per-socket isolated, so one closing socket never starves the rest.
  - **host-kit**: `mintStreamTicket`/`verifyStreamTicket` (a short-lived HMAC addressing ticket bound to tenant, channel, run, actor, and expiry over the existing HS256 primitives), a structural `HubNamespaceLike` seam plus `createHubTopology`, and `createStreamRouter` (mounts `POST /api/stream/ticket` and the ticket-verified hub/run WebSocket upgrade routes). `createFlowsafeWorker` gains an optional stream stage that mounts only when both a hub binding and `STREAM_TICKET_SECRET` are present, threading the fetch-scope hub sink through `ctx.waitUntil` and the cron sink through the sweep's collected keepalive. The ticket carries only addressing, never a grant, and is verified solely at the Worker; the DOs re-bind by their own `idFromName` identity, so the run channel rides INV-1 and the hub rides `id.name` equals the tenant.
  - **approval-ui**: a DOM-free injected `StreamTransport` (structural, like the fetch seam) with pure live-merge/optimistic-decide/reconcile reducers, an optimistic decide that reconciles against the authoritative event and surfaces a conflict when a different reviewer decided first (and rolls back on failure), a client-side liveness heartbeat that detects a silently half-open socket, and additive optional `Toast`/`PresenceIndicator` slots. A browser-WebSocket transport factory lives in the UI pass only; the interval poll stays as the fallback and periodic reconciler. The library stays DOM-free, styling-agnostic, subpath-only, and React 18+.
  - **deploy template + spike**: a copy-me hub wiring reference, and a workerd spike proof that a subscriber receives a fanned-out event, survives DO eviction and hibernation, and that an expired or cross-tenant ticket is refused fail-closed.

  None of these change the `ApprovalRecord` shape or any existing signature; a host that does not opt into streaming is byte-identical to before.

## 0.3.0

### Minor Changes

- 19ad5c4: Agent-memory tenancy chokepoints (docs/agent-memory-tenancy.md). Mastra agent memory keys threads/messages/resources by caller-chosen `threadId`/`resourceId`, which two tenants can legitimately share — unsalted, tenant B's agent would recall tenant A's messages. The INV-1 carrier now extends to memory ids: new `@proofoftech/flowsafe/do-runner` exports `mintThreadId` (`${tenantId}_${uuid}`), `mintResourceId` (`${tenantId}_${resourceKey}`, key validated against `PATH_SAFE_ID_PATTERN`), `tenantOfMemoryId` (delegates to the one salted-id decode), and `tenantOwnsMemoryId` (exact prefix ownership). `TenantContext` grew the request-scoped constructors `newThreadId()`, `newResourceId(resourceKey)`, and `ownsMemoryId(id)` — BREAKING for custom `TenantContext` implementations (hand-built resolver contexts must add the three members; contexts from `createTenantResolver` get them automatically). `purgeTenant` now also range-deletes the tenant's `mastra_messages` (by salted `thread_id`), `mastra_threads`, and `mastra_resources` rows — missing tables read as empty — and `PurgeTenantResult` grew `threads`/`messages`/`resources` counters. The schema guard pins the memory-table column names and proves two tenants sharing a business key stay disjoint and purge independently.
- 4fbc0be: Reviewed cleanup batch across the egress guard, tenant-id primitives, and approval self-decision paths - no observable contract changes and all 1119+ tests preserved.

  breakwater (patch): the egress host matcher and the allowlist validator are each a single shared definition (domainAllowed + assertEgressHostList, both driven by the one egressDomainAllowed match semantics), the normalized allowlist is computed once per construction instead of per hop, and the per-connector egress guard is built once at createConnector. egressFetch also treats an async-iterable (Node Readable) request body as one-shot so a 307/308 redirect no longer re-sends a consumed body, validates maxRedirects at construction, and fails closed on a browser opaque status-0 redirect response.

  flowsafe (minor): the tenant-salted ownership predicate and the id-mint rigor are hoisted into tenantOwnsSaltedId / assertMintableTenantId / mintSaltedId in do-runner/path-safe-id, and every live copy (runId and memory ownership, plus the approval write-path INV-1 belt) routes through them; mintSaltedId validates the tenant before evaluating a lazy suffix, so a caller-supplied uuid callback (mintThreadId's) can no longer run its side effects or throw ahead of the INV-3/reserved rejection. purgeTenant runs its three agent-memory deletes concurrently. The self-decision policy is threaded through createTenantResolver so TenantContext.canSelfDecide(role) is the single display hint the /workflows echo reads, and parseSelfDecision is memoized per deployment value. TenantContext gains a required canSelfDecide(role) member, BREAKING for hand-built TenantContext implementations (contexts from createTenantResolver get it automatically), hence the minor bump.

- 85a1ec8: Add a role-scoped separation-of-duties exemption. `ApprovalService`'s
  `allowSelfDecision` option now accepts `boolean | { roles }` — `true` exempts
  every decider, `{ roles }` exempts only the listed roles (a single-operator
  deployment sets e.g. `{ roles: ['admin'] }`). Composed hosts reach it through
  the new `APPROVAL_ALLOW_SELF_DECISION` env var (a `false` spelling, a CSV of
  roles, or `true`; any invalid value falls back to OFF — SoD stays on).
  A permitted self-decision is audited with `detail.selfDecision: true`, and the
  run catalog echoes `actor.canSelfDecide` so a UI can drop its "the server will
  refuse your decision" hint for an exempt role. Default behavior is unchanged
  (SoD on).

### Patch Changes

- 5f0a57e: Widen the optional `@proofoftech/breakwater` peer range from `^0.2.0` to `>=0.2.0 <1.0.0`. Future breakwater 0.x minors stay in-range, so changesets no longer escalates flowsafe to a spurious MAJOR on every breakwater minor release.

## 0.2.0

### Minor Changes

- 94d6b84: Content inspection, metrics adapter, notification seam, and queue triage.

  breakwater: `piiSecrets()` joins the policy engine — regex + entropy + Luhn PII/secret detectors (email, ssn, phone, creditCard, awsAccessKey, privateKey, jwt, secretAssignment, highEntropy) with allowlist exemptions, incremental streaming-window scanning, and zero-leak hold-back hints; `classifierPolicy()` is the pluggable async-classifier seam (streaming cadence, authoritative result-phase gate, fail-closed timeout). `metricsAuditSink()` + `combineAuditSinks()` adapt the audit stream onto any counters/histograms client via the `MetricsRecorder` interface.

  flowsafe: `ApprovalNotificationSink` — the notification transport seam (fired on created records and SLA escalations, contained fire-and-forget, failures audited as `approval.notify`) threaded through `ApprovalService`, `sweepSLA`, and the host-kit assembly; approval list filters `requestedBy` + `createdBefore`/`createdAfter` (strict chronological bounds on both store backends and the HTTP surface); `ApprovalService.decideBatch` + `POST /api/approvals/batch/decide` — one decision fanned out over up to 100 records through the existing per-record CAS/SoD/audit path, partial failure reported in the envelope; dashboard triage — `FilterBar`, batch selection with derived pruning, `decideSelected`, and the `Checkbox`/`Select` slots (OPTIONAL members of `ApprovalUIComponents`, so full-interface adapters written before 0.2.0 keep compiling; the provider merge fills them from `htmlComponents`, and views consume the new `ResolvedApprovalUIComponents`); `createFlowsafeWorker()` — the composed production Worker (fetch pipeline, two-cron maintenance dispatch, audit-export consumer) the deploy template and showcase host now consume as thin shells; a react-18 peer-floor typecheck probe for the emitted approval-ui types. SPDX license headers on every source file in both packages.

### Patch Changes

- 3bed052: Harden the 0.1.0 cut against the three audit residuals:

  - **breakwater (D2):** bind idempotency `put`/`release` to an opaque reservation
    lease token minted by `reserve()` (rotated on a stale-pending takeover), so a
    slow holder that was taken over as stale can no longer delete or finalize the
    new holder's claim.
  - **flowsafe (D3):** a bare tenant `ApprovalStore.list()` / `ApprovalService.list()`
    / `GET /api/approvals` now defaults to `MAX_APPROVAL_LIST_LIMIT` instead of an
    unbounded scan (page complete history with an explicit `after` cursor); the
    cron SLA sweep pages the system view explicitly so no unbounded query remains.
  - **breakwater (D1):** `PolicyEngine` now rejects an object-only policy
    (`channels: ['object']` without `'answer'`) constructed without an audit sink,
    rather than silently no-op'ing under @mastra/core 1.50.0.

  Also: the approval dashboard hook re-sorts into reviewer order only when the
  filter requests it, so a FIFO/`after`-paged caller is no longer client-resorted
  against the server's paging.

- Updated dependencies [3bed052]
- Updated dependencies [94d6b84]
  - @proofoftech/breakwater@0.2.0

## 0.1.0 — 2026-07-11

First publishable cut. Approval UX + Cloudflare-native durable execution for
Mastra workflows: Durable Object runner (`init()` import-swap, server-minted
tenant-prefixed run ids, durable resume ledger), approval queue API (CAS-guarded
D1/in-memory stores, tenant-bound factories, separation-of-duties service,
SLA sweep, derivation-based grant minting), styling-agnostic React approval
dashboard (headless hook + slot components, optional react peer), host-kit
(run router, tenant resolver, bearer auth seam, approval bridge), Cloudflare
Queues audit export, R2 artifact store, and a copy-ready production Worker
template in `deploy/`.

`@proofoftech/breakwater` is an optional peer: only the `./host-kit/module`
subpath references its types. Install it when wiring `WorkflowModule` audit
contexts; every other subpath works without it.

Requires `@mastra/core` ^1.50.0 (peer), Node >= 22, ESM only
(`moduleResolution` `node16`/`nodenext`/`bundler`). React 18 or 19 only for
`./approval-ui`.
