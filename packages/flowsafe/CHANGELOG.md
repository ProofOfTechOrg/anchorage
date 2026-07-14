# @proofoftech/flowsafe

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
