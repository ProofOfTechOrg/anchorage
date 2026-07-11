# Flowsafe Architecture

Flowsafe is the approval UX and durable execution package (`@proofoftech/flowsafe`). It wraps Mastra workflows for Cloudflare-native durable execution and provides an approval queue REST API plus React dashboard.

## Components

```
┌──────────────────────────────────────────────┐
│             Flowsafe                           │
│                                                │
│  ┌─────────────────┐  ┌────────────────────┐  │
│  │ Approval API     │  │ React Dashboard    │  │
│  │ (REST, Workers)  │  │ (queue, detail,    │  │
│  │                  │  │  decision form)    │  │
│  └────────┬────────┘  └─────────┬──────────┘  │
│           │                    │              │
│  ┌────────┴────────────────────┴──────────┐  │
│  │         DO Runner                       │  │
│  │  (Durable Object import-swap pattern)   │  │
│  └──────────────────────────────────────────┘  │
└────────────────────────────────────────────────┘
```

## Approval REST API

Endpoints (implemented in `packages/flowsafe/src/approval-api/router.ts`):

| Method | Path | Description |
|---|---|---|
| GET | `/api/approvals` | List approvals (`?status=&workflowId=&runId=&claimedBy=&requestedBy=&createdBefore=&createdAfter=&limit=&after=&orderBy=` — time bounds are strict ISO-8601 instants, `after` is an opaque FIFO cursor, `orderBy=reviewer` ranks priority → SLA → FIFO before the page cut) |
| GET | `/api/approvals/:id` | Approval detail with full context |
| POST | `/api/approvals` | Create a request — **off by default** (`allowCreate`), and it can never author capability |
| POST | `/api/approvals/:id/claim` | Claim an approval for review |
| POST | `/api/approvals/:id/decide` | Approve or reject with comment; resumes the run |
| POST | `/api/approvals/batch/decide` | One decision over ≤100 unique ids, fanned through the same per-record CAS/SoD/audit/resume path; partial failure reported per record in the envelope (HTTP 200) |
| POST | `/api/approvals/:id/delegate` | Delegate to another reviewer |
| GET | `/api/approvals/metrics` | SLA and resolution metrics (scoped to the caller's tenant) |

There is deliberately **no HTTP SLA-sweep route**. The sweep is an unfiltered
cross-tenant read *and* write, so it ships as a standalone `sweepSLA(store, …)`
over a `SystemApprovalStore` — a type request-scoped code cannot obtain — and a
Workers cron calls it.

Reviewer-facing pushes ride the `ApprovalNotificationSink` seam: fired once
per record actually entering the queue and once per SLA escalation, contained
fire-and-forget (a failing transport audits as `approval.notify` and never
blocks the approval action). flowsafe ships no transport — hosts wire email,
chat, or pagers, and must project/redact the record for lower-trust channels.

Every state change is a status-guarded compare-and-swap in the store (D1
partial-unique-index + `UPDATE ... RETURNING`), so racing reviewers resolve to
one winner and the loser gets 409. Every read and write additionally carries a
`tenant_id` predicate sourced from the store's constructor (see
[Multi-tenancy](#multi-tenancy)). The router takes a `TenantResolver`, not a
bare `authenticate`: it authenticates the request, validates the tenant claim,
and binds the service to that tenant before any route body runs. The service
enforces the RBAC role policy (reviewers/admins decide, operators/builders/
admins create, every role reads) and emits audit events shaped for
breakwater's `AuditLogger`, each carrying `tenantId` in `detail`.

### Grant minting (trust boundary 6)

A decision becomes an in-run capability by DERIVATION, not transport:
`approvalGrantProvider(store)` plugs into the DO runner's
`requestContextForRun` and recomputes `breakwater.approvedConnectors` from
APPROVED records on every start/resume. Grants never travel in HTTP bodies —
the DO's public resume route stays grant-free.

Grants are suspension-scoped: the runner passes the resumed step AND that
step's current suspension timestamp to the provider, and a step-keyed
approval unlocks its connectors only when its decision landed strictly
after that suspension began. Run-scope is explicit: a step-less approval is a
standing grant on every leg only when it carries `runScoped: true`, and mints
nothing otherwise. The provider returns the grant key on every leg — empty
when nothing applies — so the resume-context merge overwrites, rather than
inherits, the previous leg's grants. A resume that bypasses `decide()` for
its suspension therefore finds no grant and the breakwater write gate fails
closed — even when the same connector was approved at an earlier gate of
the run, and even when the same step's earlier suspension was approved (a
re-suspension needs its own decision).

The runner resolves the provider context BEFORE `createRun`, so a provider
failure (e.g. the approval store briefly unreachable) rejects the call
without persisting anything — the minted runId stays retryable.

The resume ordinal that anchors this binding lives in a `ResumeLedger` backed
by the Durable Object's `ctx.storage`, so it survives eviction, hibernation,
and code deploys. An in-memory ledger fails closed rather than leaking, but a
lost ordinal turns an already-approved action into a silent no-op — an
availability defect, not a confidentiality one.

Separation of duties: `decide()` denies the requester deciding their own
request (`requestedBy` is attributed server-side to the creating actor);
deployments opt out only via the explicit `allowSelfDecision` service option.

## Multi-tenancy

One Worker, one D1, one Durable Object namespace, many tenants — with no
per-tenant database. Three invariants carry it; the threat model
([`security-threat-model.md`](security-threat-model.md), trust boundary 7)
states them in full, with the residuals each one does not cover.

- **INV-1** — every `runId` is minted server-side as `` `${tenantId}_${uuid}` ``
  from the authenticated tenant, and no code path accepts or generates one
  without a tenant. The Mastra snapshot row, the DO instance name, the per-run
  lock, the grant-derivation predicate, and the R2 artifact key all key off
  `runId`, so they become tenant-disjoint for free — no schema change, and no
  callback signature grows a `tenantId` parameter that a host could forget to
  thread (TypeScript assigns a fewer-parameter function to a more-parameter
  slot, so such an omission would compile clean and run cross-tenant).
- **INV-2** — approval stores are bound to one tenant at construction, via a
  factory, behind a `unique symbol` brand. Request handlers receive the bound
  type; the cron sweep receives a distinctly-typed `SystemApprovalStore`.
  "Cross-tenant reads happen only inside the trusted computing base" is a
  compile-time property.
- **INV-3** — `tenantId` matches `^[a-z0-9]{3,32}$`, a charset containing
  neither `_` nor `` ` ``, which is what makes the run-ownership prefix check
  and the tenant range-purge exact.

Tenant ids are allocated by the `tenants` registry (insert-or-fail) before any
token naming them is issued. Offboarding is `purgeTenant(db, { tenantId })`,
which reaps snapshot rows of *any* status — a visitor who abandons a run at an
approval gate leaves a `suspended` row the terminal-only retention purge can
never reap at any age — plus the tenant's approvals and R2 artifacts.

## React Dashboard

Minimal dashboard with three views:
- Queue view: list of pending approvals with priority, SLA, and time remaining
- Detail view: workflow context, input/output summary, decision form
- Metrics view: SLA attainment, resolution times, escalation counts

Built as a standalone React app. Communicates only with the Approval API. No dependency on Mastra Studio.

## DO Runner (Durable Object Import-Swap Pattern)

Mastra ships the import-swap pattern in its two execution-backend integrations, `@mastra/inngest` (production-oriented) and `@mastra/temporal` (experimental): both expose an `init()` that returns backend-bound `createWorkflow`/`createStep`, imported in place of the `@mastra/core/workflows` versions -- workflow definition code is unchanged. Flowsafe applies the same mechanism for Cloudflare Durable Objects:

```
Standard workflow definition:
  import { createWorkflow, createStep } from '@mastra/core/workflows'

DO-swapped workflow (same definition code, different import):
  const { createWorkflow, createStep } = init(env)
  // init from @proofoftech/flowsafe/do-runner
```

The Durable Object:
- Holds persistent workflow state across Worker CPU limit boundaries
- Handles suspend/resume across DO lifecycle events (alarm, hibernation)
- Maps Mastra step state to DO storage
- Calls back to Mastra's API for step execution

This is the Cloudflare-native counterpart of `@mastra/temporal` -- Mastra itself has no Durable-Object execution backend; its Cloudflare packages cover deploys and storage only (`@mastra/deployer-cloudflare`, `@mastra/cloudflare` KV/DO storage, `@mastra/cloudflare-d1`).

## Dependencies

- Mastra `createWorkflow()` for step definitions
- Cloudflare Durable Objects for durable state
- D1 for workflow state, R2 for artifacts
- React for dashboard UI
