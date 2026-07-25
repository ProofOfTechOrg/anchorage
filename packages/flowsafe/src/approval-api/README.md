# approval-api

## Overview

Human-in-the-loop gate for DO-runner workflows. A workflow suspension becomes
a queue record; a reviewer decision resumes the run; the decision becomes an
in-run capability (a breakwater connector grant) by derivation from the store
— never by transport. No single file shows this loop; this README records the
cross-file contract and the rules that are not enforced by the compiler.

## Architecture

- `store.ts` / `d1-store.ts`: persistence. The store is the authoritative
  record of decisions — the grant provider reads it, so it sits inside the
  trust boundary. Every mutation is a status-guarded compare-and-swap, and
  every read and write carries a `tenant_id` predicate sourced from the
  store's constructor.
- `tenant-brand.ts` / `tenant-store.ts` / `tenant-context.ts`: the tenancy
  seam. Stores come only from a factory's `forTenant()`; the bound type carries
  a `unique symbol` brand, so an unbound store or the cron-only
  `SystemApprovalStore` is a compile error in request scope. `TenantResolver`
  authenticates, validates the tenant against INV-3, and binds — which is what
  makes "bind at construction from the authenticated actor" have a valid call
  site at all.
- `service.ts`: business rules (roles, SLA, audit, self-approval). Writes the
  store under RBAC; triggers the post-decision resume via an injected
  `resumeRun` callback so it stays deployment-agnostic (same-process =
  `resumeViaRuntime`, cross-Worker = DO stub fetch).
- `router.ts`: REST surface. Returns null off-prefix so a host Worker
  composes it ahead of its own routes. It takes a `TenantResolver` (injected,
  part of the trusted computing base) and resolves it as its first act, so no
  route body ever sees an unbound service. There is no `/sla/sweep` route.
- `grants.ts`: the seam between the queue and the runner. Plugs into the
  DO runner's `requestContextForRun`; on every start/resume it derives the
  breakwater grant key from APPROVED records.
- The create "bridge" is deployment glue (see `spike/worker.ts`): whatever
  observes a suspension creates the queue record, carrying the suspended
  step's path and the connectors that approval should unlock.

Flow: suspend → bridge creates record (idempotent per open step) → reviewer
decides (CAS) → resume through the run's DO → runner consults the provider →
provider mints grants from the store → the gated connector call sees the
grant in its requestContext.

## Design Decisions

- **Notifications are a seam, not a transport.** `ApprovalNotificationSink`
  (contract.ts) fires on the two moments a reviewer is NOT already looking at
  the dashboard: a record actually entering the queue (`created: true` only —
  the idempotent re-observation of an open step never re-notifies) and an SLA
  escalation (per record, from the cron sweep). Same
  availability-over-delivery containment as the audit sink: a throwing or
  rejecting transport is recorded as `approval.notify`/'error' and the
  approval action proceeds. flowsafe ships NO transport (email/Slack adapters
  stay with hosts); Workers hosts whose send must outlive the response wrap
  it in `ctx.waitUntil` themselves. Rejected: notifying on decisions (the
  decider is looking at the dashboard) and awaiting the sink (a slow
  transport must never hold an approval hostage).
- **Batch decide is fan-out, not a new decision model.** `decideBatch` runs
  the EXISTING `decide()` per unique id (≤`MAX_APPROVAL_BATCH_DECIDE`),
  sequentially — per-record CAS, separation-of-duties, audit, and resume
  semantics untouched, so the one-decision-per-suspension model is not
  widened. Partial failure is data (`BatchDecideItem.code`), never an HTTP
  error; only record-independent problems (role, cap, malformed input)
  reject the whole batch. Rejected: a store-level bulk transition (would
  fork the CAS semantics) and `Promise.all` (audit-order scrambling + D1/DO
  write contention for a path that is a reviewer clicking once).
- **Grants by derivation, never transport.** The DO's public resume route
  carries only `{step, resumeData}`. Anything a proxying Worker forwards can
  be forged; a grant that never crosses HTTP cannot be. Rejected: signed
  grant tokens (needless crypto — the store is already the authoritative,
  RBAC-gated record).
- **Suspension-scoped minting.** A step-keyed approval mints only for the leg
  resuming its step AND only when `decidedAt` is strictly after that step's
  current `suspendedAt` (passed in by the runner from the snapshot). This
  closes two leak shapes: approving connector X at gate A
  unlocking X at gate B, and a re-suspension of the same step riding the
  earlier approval — including the window before the bridge creates the new
  request. Rejected: latest-request-wins (leaves that window open);
  a step-aware breakwater gate (tool calls cannot see workflow steps).
- **Strictly-after, not at-or-after.** Under a shared clock, a decision
  stamped before a suspension chronologically can never satisfy strictly-
  after, so the deny direction is deterministic even at millisecond ties.
  The allow direction relies on real reviewer latency; in-process tests need
  a few milliseconds between suspension and decision (`settleClock` in the
  e2e suite).
- **No runtime dependency on breakwater.** The repo gate runs typecheck
  before build, so flowsafe source must never need breakwater's dist. The
  wire contract (key literals, role union, audit-event shape) is mirrored in
  `contract.ts` and pinned by literal-equality tests in
  `end-to-end.test.ts`, which resolve breakwater FROM SOURCE (vitest alias +
  tsconfig.test paths). Drift is a test failure, not a runtime surprise.
- **CAS-only mutation.** There is deliberately no unconditional update path;
  racing writers resolve to one winner and losers surface as HTTP 409. The
  one exception is `delegate`: last-writer-wins by design, because
  reassignment moves a pointer and guards no side effect.
- **Decision durable even when the resume fails.** `decide()` persists
  before resuming; a failed resume is reported (`DecideResult.resume`), not
  rolled back. Retry is safe: the suspension timestamp is unchanged until a
  resume succeeds, so the same decision still mints on the retried leg.
- **Self-approval denied by default.** `requestedBy` is attributed
  server-side to the creating actor; `decide()` rejects when the decider is
  the requester unless the service opts in via `allowSelfDecision`. An
  absent `requestedBy` passes by design — only records injected straight
  into the store (trusted code) can lack it.
- **D1 SQL tested against real SQLite** via `node:sqlite`
  (`process.getBuiltinModule` sidesteps both vite resolution and
  @types/node); workerd-level verification lives in the demo spike.
  Rejected: vitest-pool-workers (heavy config), better-sqlite3 (native dep
  through the supply-chain age gate).

## Tenancy invariants

- **No caller can obtain a store that is not bound to exactly one tenant.**
  `D1ApprovalStore` is not exported; `forTenant()` throws on a non-INV-3
  tenant. `create()` STAMPS the tenant from the binding — `CreateApprovalInput`
  has no `tenantId`, and a field that cannot be supplied cannot be spoofed.
- **`tenantId` is not an `ApprovalListFilter` member.** An omissible tenant
  filter is the canonical fail-open: an empty filter would scan every tenant.
  The bound store seeds `tenant_id = ?` before every optional clause.
- **A wrong-tenant id behaves exactly like an unknown id.** `get`/`transition`
  return null and reuse the existing 404/409 paths, so the API is not an
  oracle for another tenant's record ids.
- **Open-uniqueness and captured-fingerprint uniqueness are per tenant.** The
  partial open-step index leads with `tenant_id`. It had to be `DROP`ped and
  recreated under a NEW name:
  `CREATE UNIQUE INDEX IF NOT EXISTS` matches on name alone, so redefining it
  in place is a silent no-op on any database that already has it — and tenant
  B's create would collapse into tenant A's open record.
- **A pre-tenant table refuses to serve.** `ALTER TABLE … ADD COLUMN tenant_id
  TEXT NOT NULL` has no valid backfill (SQLite rejects it even on an empty
  table, and a NULL or `''` tenant is an isolation hole), so the store throws a
  loud error naming the fix rather than half-upgrading.
- **The cross-tenant sweep is a type, not a comment.** `sweepSLA` is a
  standalone function over `SystemApprovalStore`, which declares
  `[TENANT_BOUND]?: never` and is therefore unassignable wherever a bound store
  is required.
- **The grant query's `runId` predicate is load-bearing.** Under INV-1 a salted
  runId belongs to exactly one tenant, so the mint is tenant-safe even from a
  mis-bound store. The store binding is defense in depth, not the fix. A spy
  test pins the exact filter, because "optimizing away" that predicate would
  reopen the leak with a green build.

## Invariants

- The requestContext capability keys (`breakwater.approvedConnectors`,
  `breakwater.actor`) must never be populated from client input, model
  output, or tool results. Grant-minting code — the provider, the service,
  and any bridge — is inside the trust boundary.
- `service.create` asserts the input `runId` carries the store's tenant prefix.
  Not a leak (every read filters on the `tenant_id` column, never by parsing
  `run_id`), but it turns an orphan row into a loud error at the only write
  path.
- Every state change goes through `transition(id, from[], patch)`.
- At most one OPEN request per (workflowId, runId, stepKey) — enforced by a
  partial unique index. For a captured step suspension, at most one record of
  ANY status exists per (workflowId, runId, stepKey, suspendedAt, resumeCount):
  terminal records atomically block stale reconciliation from re-filing the
  same suspension, while a changed fingerprint opens the re-suspension fresh.
- The provider returns the grant key on EVERY leg (empty when nothing
  applies): Mastra merges resume-provided context over the persisted
  snapshot, so omission would inherit a previous leg's grants instead of
  retiring them.
- Bridges must set `stepPath` when creating from a suspension. Run-scope is
  EXPLICIT: a step-less record mints on every leg only when it also carries
  `runScoped: true`, and mints nothing otherwise. "Absent `stepPath` implies
  run-wide privilege" was an inverted default.
- The HTTP create route is OFF by default (`createApprovalRouter`'s
  `allowCreate`), and when a host deliberately mounts it, it cannot author
  capability: it 400s on any body naming a `TCB_ONLY_CREATE_FIELDS` member
  (`connectors`, `stepPath`, `suspendedAt`, `resumedAt`, `resumeCount`,
  `runScoped`, `requestedBy`) and forces `requestedBy` to the authenticated
  actor. `service.create` still honours an explicit `requestedBy` — the
  in-process bridge attributes the human who advanced the run, which is exactly
  what makes the separation-of-duties check fireable. The tightening is at the
  HTTP boundary only.
- Records are JSON-safe end to end (validated at create) so the two store
  implementations cannot diverge on exotic payloads.
- `decidedAt` (service clock) is compared against `suspendedAt` (engine
  clock): deployments must keep them on a shared clock (true same-Worker and
  on Cloudflare). The clock-free hardening — capturing the
  `(suspendedAt, resumeCount)` pair into the record at create time and matching
  both exactly — is implemented as the exact-match suspension binding.
  `resumeCount` is the runtime-owned monotonic per-(run,step) resume ordinal
  (undefined on a first suspension, `1,2,…` on re-suspensions), incremented on
  every resume regardless of payload, so it keeps same-step suspensions distinct
  when their `suspendedAt` collide — even a no-payload re-suspension (which
  Mastra leaves without a `resumedAt`) — and, being strictly increasing, never
  collides across deep chains. `resumedAt` is retained as informational only.
- `escalated` stays decidable; `approved`/`rejected` are terminal.
