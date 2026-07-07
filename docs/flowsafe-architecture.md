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
| GET | `/api/approvals` | List approvals (`?status=&workflowId=&runId=&claimedBy=`) |
| GET | `/api/approvals/:id` | Approval detail with full context |
| POST | `/api/approvals` | Create a request (idempotent: one open request per workflowId/runId/step) |
| POST | `/api/approvals/:id/claim` | Claim an approval for review |
| POST | `/api/approvals/:id/decide` | Approve or reject with comment; resumes the run |
| POST | `/api/approvals/:id/delegate` | Delegate to another reviewer |
| GET | `/api/approvals/metrics` | SLA and resolution metrics |
| POST | `/api/approvals/sla/sweep` | Escalate breached open requests (cron-triggered) |

Every state change is a status-guarded compare-and-swap in the store (D1
partial-unique-index + `UPDATE ... RETURNING`), so racing reviewers resolve to
one winner and the loser gets 409. Authentication is injected
(`authenticate(request) -> actor`); the service enforces the RBAC role policy
(reviewers/admins decide, operators/builders/admins create and sweep, every
role reads) and emits audit events shaped for breakwater's `AuditLogger`.

### Grant minting (trust boundary 6)

A decision becomes an in-run capability by DERIVATION, not transport:
`approvalGrantProvider(store)` plugs into the DO runner's
`requestContextForRun` and recomputes `breakwater.approvedConnectors` from
APPROVED records on every start/resume. Grants never travel in HTTP bodies —
the DO's public resume route stays grant-free.

Grants are suspension-scoped: the runner passes the resumed step AND that
step's current suspension timestamp to the provider, and a step-keyed
approval unlocks its connectors only when its decision landed strictly
after that suspension began (step-less approvals are explicitly run-scoped
standing grants). The provider returns the grant key on every leg — empty
when nothing applies — so the resume-context merge overwrites, rather than
inherits, the previous leg's grants. A resume that bypasses `decide()` for
its suspension therefore finds no grant and the breakwater write gate fails
closed — even when the same connector was approved at an earlier gate of
the run, and even when the same step's earlier suspension was approved (a
re-suspension needs its own decision).

The runner resolves the provider context BEFORE `createRun`, so a provider
failure (e.g. the approval store briefly unreachable) rejects the call
without persisting anything — a caller-supplied runId stays retryable.

Separation of duties: `decide()` denies the requester deciding their own
request (`requestedBy` is attributed server-side to the creating actor);
deployments opt out only via the explicit `allowSelfDecision` service option.

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
