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
  trust boundary. Every mutation is a status-guarded compare-and-swap.
- `service.ts`: business rules (roles, SLA, audit, self-approval). Writes the
  store under RBAC; triggers the post-decision resume via an injected
  `resumeRun` callback so it stays deployment-agnostic (same-process =
  `resumeViaRuntime`, cross-Worker = DO stub fetch).
- `router.ts`: REST surface. Returns null off-prefix so a host Worker
  composes it ahead of its own routes. `authenticate` is injected and is
  part of the trusted computing base.
- `grants.ts`: the seam between the queue and the runner. Plugs into the
  DO runner's `requestContextForRun`; on every start/resume it derives the
  breakwater grant key from APPROVED records.
- The create "bridge" is deployment glue (see `demo/worker.ts`): whatever
  observes a suspension creates the queue record, carrying the suspended
  step's path and the connectors that approval should unlock.

Flow: suspend → bridge creates record (idempotent per open step) → reviewer
decides (CAS) → resume through the run's DO → runner consults the provider →
provider mints grants from the store → the gated connector call sees the
grant in its requestContext.

## Design Decisions

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

## Invariants

- The requestContext capability keys (`breakwater.approvedConnectors`,
  `breakwater.actor`) must never be populated from client input, model
  output, or tool results. Grant-minting code — the provider, the service,
  and any bridge — is inside the trust boundary.
- Every state change goes through `transition(id, from[], patch)`.
- At most one OPEN request per (workflowId, runId, stepKey) — enforced by a
  partial unique index; decided records never block a re-suspension's fresh
  request.
- The provider returns the grant key on EVERY leg (empty when nothing
  applies): Mastra merges resume-provided context over the persisted
  snapshot, so omission would inherit a previous leg's grants instead of
  retiring them.
- Bridges must set `stepPath` when creating from a suspension; a step-less
  record is an explicitly run-scoped standing grant, not a default.
- Records are JSON-safe end to end (validated at create) so the two store
  implementations cannot diverge on exotic payloads.
- `decidedAt` (service clock) is compared against `suspendedAt` (engine
  clock): deployments must keep them on a shared clock (true same-Worker and
  on Cloudflare). The clock-free hardening — capturing the snapshot's
  `suspendedAt` into the record at create time and matching exactly — is
  implemented as the exact-match suspension binding.
- `escalated` stays decidable; `approved`/`rejected` are terminal.
