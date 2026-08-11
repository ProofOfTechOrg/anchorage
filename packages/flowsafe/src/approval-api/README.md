# Understand the approval API

The approval API is the human-in-the-loop gate for Durable Object workflows. A
workflow suspension creates a queue record. A reviewer decision resumes the
run, and the runner derives a Breakwater connector grant from the stored
decision. Grants never cross an HTTP boundary.

## Follow an approval through the system

- `store.ts` / `d1-store.ts`: deployment persistence. The store is the authoritative record of decisions because the grant provider reads it. Every mutation is a status-guarded compare-and-swap. Tables contain no request-level organization predicate.
- `store-factory.ts` / `actor-context.ts`: store and request assembly. A factory's `.store()` returns the deployment store. `createActorResolver()` authenticates and validates an actor, then constructs `ActorContext` with lazy service access, server-owned id minters, and trusted resource-key validation.
- `service.ts`: business rules for roles, service-level agreement (SLA)
  deadlines, audit, notifications, streaming, and separation of duties. It
  resumes a decided run through an injected `resumeRun` callback. Use
  `resumeViaRuntime` in one process or a Durable Object stub across Workers.
- `router.ts`: REST surface. Returns null off-prefix so a host Worker composes it ahead of its own routes. It takes an `ActorResolver` and resolves it before reading a route body. There is no `/sla/sweep` route.
- `grants.ts`: the seam between the queue and the runner. Plugs into the
  Durable Object runner's `requestContextForRun`. On every start or resume, it
  derives structured Breakwater grants from `approved` records.
- The host bridge is deployment glue (see `spike/worker.ts`): whatever
  observes a suspension creates the queue record, carrying the suspended
  step's path and the connectors that approval should unlock.

The approval flow has six steps:

1. The workflow suspends.
2. The host bridge creates one record for each suspended step.
3. A reviewer decides the record through a compare-and-swap transition.
4. The service resumes the run through its Durable Object.
5. The runner derives grants from the deployment store.
6. The gated connector reads the grant from `requestContext`.

## Use the HTTP API

`createApprovalRouter()` uses `/api/approvals` by default. Every authenticated
role may read approvals and metrics. Only `reviewer` and `admin` may claim,
decide, or delegate records.

| Method and route | Behavior |
| --- | --- |
| `GET /api/approvals` | List deployment records |
| `GET /api/approvals/metrics` | Return deployment queue metrics |
| `GET /api/approvals/:id` | Return one record or 404 |
| `POST /api/approvals/:id/claim` | Claim a `pending` or `escalated` record |
| `POST /api/approvals/:id/decide` | Approve or reject an open record and resume it only when trusted resumability provenance is present |
| `POST /api/approvals/:id/delegate` | Assign an open record to another reviewer |
| `POST /api/approvals/batch/decide` | Apply one decision to at most 100 unique IDs |
| `POST /api/approvals` | Create a decision-only, non-capability record for a write-accessible run only when `allowCreate: true` |

The list route accepts `status`, `workflowId`, `runId`, `claimedBy`,
`requestedBy`, `createdBefore`, `createdAfter`, `limit`, `after`, and
`orderBy`. `limit` must be an integer from 1 through 500. `status` accepts a
comma-separated list. `orderBy=reviewer` sorts by priority, SLA deadline, then
creation time. It cannot be combined with the creation-order `after` cursor.

First-party hosts leave `allowCreate` false. Their bridges create approval
records only after observing a real suspension. If you enable HTTP creation,
only `operator`, `builder`, and `admin` with write access to the named run may
use it, and the body cannot set connector grants, step binding, requester
attribution, or resume topology. The resulting record is decision-only and
never resumes a run.

## Understand the design choices

- **Inject notifications instead of choosing a transport**:
  `ApprovalNotificationSink` fires when a record first enters the queue and
  when the SLA sweep escalates it. Re-observing the same open suspension does
  not notify again. A transport failure records an `approval.notify` error but
  does not fail the approval action. Flowsafe does not ship an email or chat
  adapter; a Worker host can keep a send alive with `ctx.waitUntil`.
- **Process batch decisions through the single-record path**: `decideBatch()`
  deduplicates at most 100 IDs and calls `decide()` sequentially for each one.
  Every record retains the same compare-and-swap, separation-of-duties, audit,
  and resume behavior. Per-record failures appear in the HTTP 200 response;
  malformed input, an unauthorized role, or more than 100 unique IDs rejects
  the whole request.
- **Derive grants instead of transporting them**: The public resume route
  carries only `{ step, resumeData }`. The runner reads approved records from
  the deployment store and writes structured grants plus the current
  execution identity into `requestContext`. A proxy cannot forge a value that
  never appears in HTTP.
- **Bind a decision to one exact suspension**: The bridge captures
  `(suspendedAt, resumeCount)` when it creates a step approval. Grant
  derivation requires both values to match the leg being resumed. Approval at
  one step cannot unlock another step, and an earlier approval cannot unlock a
  later suspension of the same step.
- **Fail closed on legacy identity**: Capability-bearing rows need an explicit
  `grantScope` and exact suspension identity. Durable-agent rows also need one
  connector and a non-empty `toolCallId`. Older rows without these fields mint
  nothing.
- **Keep Breakwater out of the runtime dependency graph**: `contract.ts`
  mirrors the shared request-context keys, role union, and audit shape.
  Literal-equality tests compare those values against Breakwater source and
  fail if the two packages drift.
- **Use compare-and-swap for guarded state changes**: A claim, decision, SLA
  escalation, or supersede applies only when the current status is eligible.
  Racing writers produce one winner, while losers receive HTTP 409. Delegation
  deliberately uses last-writer-wins because it only changes the assignee.
- **Keep the decision when resume fails**: `decide()` persists before it
  invokes `resumeRun`. `DecideResult.resume` reports any resume failure without
  rolling back the decision. Retry the run resume through the host's runtime
  or Durable Object path; do not submit the decision again. The stored decision
  still derives the grant for that suspension.
- **Enforce separation of duties by default**: `decide()` rejects the
  requester and a reviewer who approved an earlier sequential gate in the same
  run. Parallel gates filed before either decision do not trigger the
  cross-gate bar. `allowSelfDecision` may exempt every decider or named roles
  from both checks, and permitted requester self-decisions are audited.
- **Test D1 behavior against SQLite and Workers**: The unit suite runs the D1
  SQL against `node:sqlite`. The Workerd spike verifies suspend, persistence,
  restart, decision, and grant-derived resume on the Workers runtime.

## Use one deployment store

- **The physical deployment is the organization boundary.** Each organization gets its own Worker, D1 database, and Durable Object namespaces. The Worker and Durable Objects verify `DEPLOYMENT_TENANT` against the D1 sentinel before this store is reachable.
- **The factory exposes one store.** `D1ApprovalStore` remains internal; `D1ApprovalStoreFactory.store()` memoizes the store and schema gate for the bound database.
- **Approval rows contain no deployment claim.** `CreateApprovalInput`, `ApprovalRecord`, and list filters cannot select an organization. The provisioning boundary chooses the database before request routing.
- **Uniqueness is deployment-local.** The open-step index covers `(workflow_id, run_id, step_key)`. The captured-suspension index adds `suspended_at` and `resume_count`, so stale reconciliation cannot re-file the same leg.
- **Legacy pooled schemas fail closed.** Schema initialization refuses a `flowsafe_approvals` table containing `tenant_id`. Provision a fresh per-organization database instead of rebuilding pooled rows in place.
- **Maintenance uses the same store contract.** `sweepSLA()` scans the deployment queue. It remains a standalone alarm-invoked operation rather than a public route.
- **Grant queries require `workflowId` and `runId`.** Run ids are opaque and server-minted. Exact workflow, run, connector, suspension, and tool-call identity bind the grant.

## Preserve the security guarantees

- The `requestContext` capability keys (`breakwater.connectorGrants`,
  `breakwater.connectorExecution`, `breakwater.actor`) must never be populated
  from client input, model output, or tool results. Only the provider, service,
  trusted runtime, and trusted host bridge may set them.
- `service.create()` validates path-safe server-owned identifiers. Hosts must mint run ids through `ActorContext.newRunId()` or an equivalent trusted runtime seam.
- Every state change goes through `transition(id, from[], patch)`.
- At most one open request exists per `(workflowId, runId, stepKey)`. For a
  captured step suspension, at most one record of any status exists per
  `(workflowId, runId, stepKey, suspendedAt, resumeCount)`.
  Terminal records atomically block stale reconciliation from re-filing the
  same suspension, while a changed fingerprint opens the re-suspension fresh.
- The provider returns the grant key on every leg, using an empty list when
  nothing applies. Mastra merges resume-provided context over the persisted
  snapshot, so omission would inherit a previous leg's grants instead of
  retiring them.
- Bridges must set `stepPath` when creating from a suspension. Run scope is
  explicit: a step-less record mints on every leg only when it also carries
  `runScoped: true`, and mints nothing otherwise. "Absent `stepPath` implies
  run-wide privilege" was an inverted default.
- Durable-agent bridges persist `toolCallId` and derive `tool-call` scope.
  Workflow gates derive `suspension` scope. Explicit `runScoped: true` records
  derive `run` scope.
- The HTTP create route is off by default through
  `createApprovalRouter({ allowCreate: false })`. When a host enables it, the
  route returns 400 for any body containing a server-only field
  (`connectors`, `grantScope`, `toolCallId`, `stepPath`, `suspendedAt`,
  `resumedAt`, `resumeCount`, `runScoped`, `requestedBy`, `requestedByKind`, or
  `resumeTarget`), requires write access to the named run, and forces
  `requestedBy` to the authenticated actor. A record without `stepPath`,
  explicit `runScoped: true`, or a server-authored `resumeTarget` is
  decision-only. `service.create()` still honors an explicit `requestedBy` and
  `requestedByKind`: the in-process bridge attributes the execution principal
  that advanced the run, while a human approval resume is attributed to its
  decider. That attribution makes the separation-of-duties check effective.
- Records are JSON-safe end to end (validated at create) so the two store
  implementations cannot diverge on exotic payloads.
- Current bridges capture `(suspendedAt, resumeCount)`, and grant derivation
  matches both exactly. `resumeCount` is undefined on the first suspension,
  then increases on every resume. It distinguishes repeated suspensions even
  if their millisecond timestamps match. `resumedAt` remains informational
  because Mastra records it only for payload-bearing resumes. Legacy rows
  without an exact identity mint nothing.
- `escalated` stays decidable; `approved`/`rejected` are terminal.
