# @proofoftech/flowsafe

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
