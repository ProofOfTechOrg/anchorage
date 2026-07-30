# @proofoftech/breakwater

## 0.8.0

### Minor Changes

- cb0f861: Replace connector ID approval arrays with structured connector grants. Durable-agent approvals now bind to the exact Mastra tool call, workflow approvals bind to the exact suspension, and standing grants require explicit run scope.

  This is intentionally breaking: `APPROVED_CONNECTORS_CONTEXT_KEY`, `BREAKWATER_APPROVED_CONNECTORS_KEY`, and `approvedConnectorsForLeg()` are removed. Legacy arrays and approval rows without explicit scope fail closed. Migrate trusted hosts to `CONNECTOR_GRANTS_CONTEXT_KEY`, `CONNECTOR_EXECUTION_CONTEXT_KEY`, and `connectorGrantsForLeg()`.

## 0.7.0

### Minor Changes

- 3a259b8: Add first-class execution principals so automated work stops impersonating people.

  Every automated path previously fabricated a human to satisfy the one identity the platform had: the schedule tick, cron SLA maintenance, signal-provider delivery, and the suspension-reconcile bridge all minted `role: 'operator'`. That lost provenance and gave autonomous execution an operator's authority.

  Breakwater's `Actor` gains an optional `kind` (`human` | `service` | `agent` | `system`, absent meaning human), and both `RBACMiddleware` and `createGuardedAgent` gain `allowedPrincipalKinds`, defaulting to `['human']`. The gate checks kind before role and does not consult the role allowlist for a non-human kind, because an automated principal carries a role only to satisfy the required field — consulting it would either admit whatever role the host projected, or force hosts to allow that role and thereby admit real humans holding it. Both the processor gate and the direct-call gate enforce it. **An existing agent therefore denies every automated principal without a config change.**

  Flowsafe adds `ExecutionPrincipal`, with `purpose` required on every automated kind, and persists it in agent-run state and approval resume targets. `AgentMeta.allowedAutomation` declares which principal kinds may enter on which entry paths; absent or empty denies all automated entry, and an optional host authorizer can only narrow it further. `ApprovalActor` is unchanged and still means an authenticated human at the HTTP boundary or a reviewer deciding an approval — a human approval never transfers the decider's authority into the resumed run.

  The `@proofoftech/flowsafe/agent-host` entry point exports its automation policy types, including `AgentAutomationRule`, `AutomationCheck`, `AutomatedEntryRequest`, and `AutomatedEntryAuthorizer`, so public catalog and host signatures never require deep imports.

  `ApprovalService` gains `createAsPrincipal` and `supersedeStaleAsPrincipal` for trusted platform bridges. They replace the human role gate with a kind-and-tenant check rather than widening it. There is deliberately no principal-taking `decide`, `claim`, or `delegate`.

  `trustAutomationPrincipal()` returns a branded, frozen canonical clone rather than the caller's own object. Validating a principal and handing the same reference back left the vouch time-of-check/time-of-use: the caller kept a mutable alias and could rewrite a vouched `system` principal into `{kind:'human', role:'admin'}` before the service read `kind`. The trusted entries now recheck the own brand, the automated shape, the kind, and that every field is a plain data property — an accessor survives `Object.freeze` and would reopen the same hole — instead of trusting a parameter type that does not exist at runtime. `ExecutionPrincipal` fields are `readonly`.

  `AutomatedExecutionPrincipal` is added for duties that want provenance but derive no authority from the principal, so the trust brand is demanded only where it is read. `sweepSLA` and `SlaSweepMaintenanceOptions` take it, and `sweepSLA` refuses a human or malformed principal outright: it writes across every tenant, and a human there would stamp `principalKind: 'human'` onto cron escalations. `TRUSTED_AUTOMATION` is not on the package barrel — `trustAutomationPrincipal` is the sanctioned constructor.

  Audit correlation now carries `principalKind`, `principalId`, `purpose`, and `delegatedBy` alongside the existing tenant, run, thread, and entry-path fields.

  `x-flowsafe-actor` and `x-flowsafe-role` are retired from the wire. The principal is now the sole identity channel: a thread Durable Object projects `scope.actor` from it, so a host's separate `TenantContext.actor` can no longer disagree with what executes. Both header constants are removed from `@proofoftech/flowsafe/do-runner`; the topology strips the names on send and forward, and `createTenantResolver` still refuses them on inbound requests so a mixed-version client fails loudly.

  `queueApprovalForSuspension`, `reconcileApprovalsForSummary`, and `resumeRunWithRequeue` take a `systemActorId` string instead of a principal, and mint their own bookkeeping identity against the service's tenant binding. Hosts no longer perform a trust assertion for the platform's own bookkeeping. `ApprovalService` exposes its `tenantId` for that.

  The principal travels to a Durable Object in a trusted `x-flowsafe-principal` header that `createThreadTopology` stamps on every send and forward. A thread DO refuses a request that carries none rather than treating the caller as a human, and `createTenantResolver` refuses the header on inbound requests exactly as it does the tenant, actor, and role headers.

  BREAKING for in-flight state, deliberately and without an upgrade path: `AgentRunRecord` is version 2 and `agent-thread` resume targets now store an `ExecutionPrincipal`. Records written by the previous release fail closed, so a suspended agent run started before this upgrade cannot resume. A version-1 record cannot be upgraded honestly — a `schedule.fire` run stored `role: 'operator'`, so reading it back as a human would launder exactly the authority this change removes. Flowsafe's breakwater peer floor moves to `>=0.7.0`. `rejectReservedAgentContext` is removed from `@proofoftech/flowsafe/agent-host`; it was exported but never called on any path, and every real caller uses `sanitizeStoredAgentContext`.

  A thread Durable Object now requires the principal header on every request, so a deployment whose Worker and Durable Object resolve different `@proofoftech/flowsafe` versions returns 403 until both sides ship this release. Cloudflare's single-bundle model makes that skew unlikely, but there is no negotiation.

## 0.6.0

### Minor Changes

- 09a4406: Add guarded Breakwater agents and Flowsafe's authenticated, catalog-driven agent host. Agent starts now derive trusted identity and execution context, agent resumes require an approval-bound capability, and status and NDJSON observation remain tenant-bound.

## 0.5.0

### Minor Changes

- def3b37: Harden public connector and Agent CLI boundaries for the first public release.
  Agent CLI connectors now expose structured, redacted errors, pass workspace-edit
  permission flags to Claude Code and Codex, and keep prompts and option values out
  of diagnostics and audit events. Connector, policy-evaluator, and actor-lookup
  failures now emit static safe audit reasons. Add exhaustive export sentinels and
  a packed-tarball consumer test, move Zod to runtime dependencies, and publish
  complete package and connector guides.

## 0.4.0

> **Scope correction:** This package release added the `_background` permission
> and the `backgroundExecution` policy described in the first bullet below. The
> flowsafe storage, Durable Object host, recovery, and execution-status material
> was included by the shared changeset but is not part of breakwater. See the
> [flowsafe changelog](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/flowsafe/CHANGELOG.md)
> for that package's final behavior.

### Minor Changes

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

## 0.3.1

### Patch Changes

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

## 0.3.0

### Minor Changes

- 5011013: Fetch-level egress enforcement. `createConnector()` now hands `execute`/`dryRunExecute` a third argument, `ConnectorRuntime`, whose `fetch` is bound to the manifest's declared `egress`: every actual request — redirect hops included — must resolve to a declared host or it is denied (`ConnectorPolicyError`, policy `egress-fetch`) and audited before any bytes leave. Redirects are followed manually with a per-hop allowlist check, credential headers are stripped on cross-origin hops, non-http(s) schemes and unparseable URLs fail closed, and a manifest with no `egress` gets a fetch that denies everything. New exports: `egressFetch()` (standalone guard factory), `EgressDeniedError`, the structural fetch seam types (`EgressResponse`, `EgressRequestInit`, `EgressFetchBase`, `EgressGuardedFetch`, `EgressDenial`, `EgressFetchOptions`, `EgressResponseHeaders`), `ConnectorRuntime`, `ConnectorPolicies.fetch` (base-fetch injection seam for tests/instrumentation), and `egressDomainAllowed` (the shared host matcher). Existing connectors are unaffected — the third argument is additive and two-parameter `execute` implementations keep compiling; traffic that does not go through `runtime.fetch` (e.g. a vendor SDK's own HTTP stack) keeps the previous declaration-only posture, documented in `CONNECTORS.md`.

### Patch Changes

- df413da: `egressFetch` now releases each intermediate redirect response before following it or throwing. The manual redirect follower cancels the discarded 3xx's body stream, so a followed, hop-capped, egress-denied, or one-shot-refused redirect can no longer retain its connection until GC (Node/Undici, workerd) under sustained redirected traffic. Disposal is best-effort (a locked/errored stream's cancel rejection and an injected transport's synchronous throw are both swallowed) and never touches the response returned to the caller.
- 4fbc0be: Reviewed cleanup batch across the egress guard, tenant-id primitives, and approval self-decision paths - no observable contract changes and all 1119+ tests preserved.

  breakwater (patch): the egress host matcher and the allowlist validator are each a single shared definition (domainAllowed + assertEgressHostList, both driven by the one egressDomainAllowed match semantics), the normalized allowlist is computed once per construction instead of per hop, and the per-connector egress guard is built once at createConnector. egressFetch also treats an async-iterable (Node Readable) request body as one-shot so a 307/308 redirect no longer re-sends a consumed body, validates maxRedirects at construction, and fails closed on a browser opaque status-0 redirect response.

  flowsafe (minor): the tenant-salted ownership predicate and the id-mint rigor are hoisted into tenantOwnsSaltedId / assertMintableTenantId / mintSaltedId in do-runner/path-safe-id, and every live copy (runId and memory ownership, plus the approval write-path INV-1 belt) routes through them; mintSaltedId validates the tenant before evaluating a lazy suffix, so a caller-supplied uuid callback (mintThreadId's) can no longer run its side effects or throw ahead of the INV-3/reserved rejection. purgeTenant runs its three agent-memory deletes concurrently. The self-decision policy is threaded through createTenantResolver so TenantContext.canSelfDecide(role) is the single display hint the /workflows echo reads, and parseSelfDecision is memoized per deployment value. TenantContext gains a required canSelfDecide(role) member, BREAKING for hand-built TenantContext implementations (contexts from createTenantResolver get it automatically), hence the minor bump.

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

## 0.1.0 — 2026-07-11

First publishable cut. Mastra safety middleware: policy engine (output channels,
deny patterns, opt-in hold-back buffering), RBAC processor, audit sink, connector
SDK (permission manifests, grant-only write approval, network-egress declaration
gate, idempotent replay with in-memory/atomic/D1 stores, fixed-window rate
limiting, dry-run, tenant isolation scoping), and approval-gated Claude Code /
Codex CLI connectors.

Publish order: this package publishes BEFORE `@proofoftech/flowsafe` (flowsafe's
`./host-kit/module` subpath types reference it as an optional peer).

Requires `@mastra/core` ^1.50.0 (peer), Node >= 22, ESM only
(`moduleResolution` `node16`/`nodenext`/`bundler`).
