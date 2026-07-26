# Understand the approval API

The approval API is the human-in-the-loop gate for Durable Object workflows. A
workflow suspension creates a queue record. A reviewer decision resumes the
run, and the runner derives a Breakwater connector grant from the stored
decision. Grants never cross an HTTP boundary.

## Follow an approval through the system

- `store.ts` / `d1-store.ts`: persistence. The store is the authoritative
  record of decisions because the grant provider reads it. Every mutation is
  a status-guarded compare-and-swap, and
  every read and write carries a `tenant_id` predicate sourced from the
  store's constructor.
- `tenant-brand.ts` / `tenant-store.ts` / `tenant-context.ts`: the tenancy
  seam. Stores come only from a factory's `forTenant()`; the bound type carries
  a `unique symbol` brand, so an unbound store or the cron-only
  `SystemApprovalStore` is a compile error in request scope. `TenantResolver`
  authenticates the request, validates `tenantId` against
  `^[a-z0-9]{3,32}$`, then constructs the tenant-bound service.
- `service.ts`: business rules for roles, service-level agreement (SLA)
  deadlines, audit, notifications, streaming, and separation of duties. It
  resumes a decided run through an injected `resumeRun` callback. Use
  `resumeViaRuntime` in one process or a Durable Object stub across Workers.
- `router.ts`: REST surface. Returns null off-prefix so a host Worker
  composes it ahead of its own routes. It takes a `TenantResolver` (injected,
  identity-verifying code) and resolves it before reading a route body. There
  is no `/sla/sweep` route.
- `grants.ts`: the seam between the queue and the runner. Plugs into the
  Durable Object runner's `requestContextForRun`. On every start or resume, it
  derives the Breakwater grant key from `approved` records.
- The host bridge is deployment glue (see `spike/worker.ts`): whatever
  observes a suspension creates the queue record, carrying the suspended
  step's path and the connectors that approval should unlock.

The approval flow has six steps:

1. The workflow suspends.
2. The host bridge creates one record for each suspended step.
3. A reviewer decides the record through a compare-and-swap transition.
4. The service resumes the run through its Durable Object.
5. The runner derives grants from the tenant-bound store.
6. The gated connector reads the grant from `requestContext`.

## Use the HTTP API

`createApprovalRouter()` uses `/api/approvals` by default. Every authenticated
role may read approvals and metrics. Only `reviewer` and `admin` may claim,
decide, or delegate records.

| Method and route | Behavior |
| --- | --- |
| `GET /api/approvals` | List tenant-bound records |
| `GET /api/approvals/metrics` | Return tenant-bound queue metrics |
| `GET /api/approvals/:id` | Return one record or 404 |
| `POST /api/approvals/:id/claim` | Claim a `pending` or `escalated` record |
| `POST /api/approvals/:id/decide` | Approve or reject an open record and attempt resume |
| `POST /api/approvals/:id/delegate` | Assign an open record to another reviewer |
| `POST /api/approvals/batch/decide` | Apply one decision to at most 100 unique IDs |
| `POST /api/approvals` | Create a non-capability record only when `allowCreate: true` |

The list route accepts `status`, `workflowId`, `runId`, `claimedBy`,
`requestedBy`, `createdBefore`, `createdAfter`, `limit`, `after`, and
`orderBy`. `limit` must be an integer from 1 through 500. `status` accepts a
comma-separated list. `orderBy=reviewer` sorts by priority, SLA deadline, then
creation time. It cannot be combined with the creation-order `after` cursor.

First-party hosts leave `allowCreate` false. Their bridges create approval
records only after observing a real suspension. If you enable HTTP creation,
only `operator`, `builder`, and `admin` may use it, and the body cannot set
connector grants, step binding, requester attribution, or resume topology.

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
  the tenant-bound store and writes the resulting connector list into
  `requestContext`. A proxy cannot forge a value that never appears in HTTP.
- **Bind a decision to one exact suspension**: The bridge captures
  `(suspendedAt, resumeCount)` when it creates a step approval. Grant
  derivation requires both values to match the leg being resumed. Approval at
  one step cannot unlock another step, and an earlier approval cannot unlock a
  later suspension of the same step.
- **Retain the timestamp comparison only for legacy rows**: Records created by
  older bridges may lack a captured `suspendedAt`. Those rows use the
  transitional rule `decidedAt > suspendedAt` and require the service and
  runner clocks to agree. Current bridges always write the exact suspension
  fingerprint, so new records do not depend on decision timing.
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

## Isolate every tenant

- **No caller can obtain a store that is not bound to exactly one tenant.**
  `D1ApprovalStore` is not exported; `forTenant()` throws when the tenant ID
  does not match `^[a-z0-9]{3,32}$`. The `create()` method stamps the tenant
  from the binding. `CreateApprovalInput`
  has no `tenantId`, and a field that cannot be supplied cannot be spoofed.
- **`tenantId` is not an `ApprovalListFilter` member.** An omissible tenant
  filter is the canonical fail-open: an empty filter would scan every tenant.
  The bound store seeds `tenant_id = ?` before every optional clause.
- **A wrong-tenant ID behaves like an unknown ID.** `get()` and `transition()`
  return null, and the service returns 404. The API does not reveal whether
  another tenant owns the record.
- **Uniqueness includes the tenant.** The open-step and captured-suspension
  indexes begin with `tenant_id`. Identical workflow, run, and step values in
  two tenants therefore remain independent. Schema initialization recreates
  the legacy non-tenant index under a new name because SQLite does not change
  an existing index definition when `CREATE INDEX IF NOT EXISTS` reuses its
  name.
- **A pre-tenant table refuses to serve.** `ALTER TABLE … ADD COLUMN tenant_id
  TEXT NOT NULL` has no valid backfill (SQLite rejects it even on an empty
  table, and a NULL or `''` tenant is an isolation hole), so the store throws a
  configuration error rather than serving partially migrated data.
- **Only scheduled maintenance can scan tenants.** `sweepSLA()` is a
  standalone function over `SystemApprovalStore`, which declares
  `[TENANT_BOUND]?: never` and is therefore unassignable wherever a bound store
  is required.
- **Grant queries require `workflowId` and `runId`.** A tenant-salted run ID
  belongs to exactly one tenant, so grant derivation remains tenant-safe even
  from a mis-bound store. The store binding provides a second independent
  check. A spy test pins both predicates on every page.

## Preserve the security guarantees

- The `requestContext` capability keys (`breakwater.approvedConnectors`,
  `breakwater.actor`) must never be populated from client input, model
  output, or tool results. Only the provider, service, and trusted host bridge
  may set them.
- `service.create()` requires the input `runId` to carry the store's tenant
  prefix. Reads still filter on `tenant_id`; the prefix check prevents an
  orphan approval record at the write boundary.
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
- The HTTP create route is off by default through
  `createApprovalRouter({ allowCreate: false })`. When a host enables it, the
  route returns 400 for any body containing a server-only field
  (`connectors`, `stepPath`, `suspendedAt`, `resumedAt`, `resumeCount`,
  `runScoped`, `requestedBy`, or `resumeTarget`) and forces `requestedBy` to
  the authenticated actor. `service.create()` still honors an explicit
  `requestedBy`: the in-process bridge attributes the human who advanced the
  run, which is what makes the separation-of-duties check effective.
- Records are JSON-safe end to end (validated at create) so the two store
  implementations cannot diverge on exotic payloads.
- Current bridges capture `(suspendedAt, resumeCount)`, and grant derivation
  matches both exactly. `resumeCount` is undefined on the first suspension,
  then increases on every resume. It distinguishes repeated suspensions even
  if their millisecond timestamps match. `resumedAt` remains informational
  because Mastra records it only for payload-bearing resumes. Only legacy rows
  without a captured `suspendedAt` use the shared-clock
  `decidedAt > suspendedAt` fallback.
- `escalated` stays decidable; `approved`/`rejected` are terminal.
