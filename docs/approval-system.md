# Approval system

Flowsafe turns a Mastra suspension into a durable approval request inside one physically isolated deployment. An approval resumes the run through its owning Durable Object and gives the resumed leg only the connector capabilities stored on that record. A rejection resumes with `approved: false` and no connector grant so the workflow can handle the denial.

Use this system for human authorization of side effects. Do not use a client-provided boolean, a raw resume body, or a model response as proof of approval.

## Lifecycle

```text
workflow or agent reaches a gate
        |
        v
run snapshot records suspendedAt + resumeCount
        |
        v
trusted host bridge creates one open approval
        |
        +--> notification sink and live-stream sink
        |
reviewer claims, delegates, approves, or rejects
        |
        v
CAS commits the terminal decision
        |
        +--> approved: rehydrate agent registry if needed, derive grants, resume with approved: true
        |
        +--> rejected: resume with approved: false and no grant
        |
        v
another suspension creates a new record; terminal run ends the loop
```

The database decision and the resume are separate operations. Once a CAS commits an approval or rejection, a failed resume does not roll the decision back. Redrive the decided record through the trusted resume path.

## Approval states

| Status | Meaning | Decidable |
| --- | --- | --- |
| `pending` | Open and unclaimed | Yes |
| `claimed` | Assigned to a reviewer | Yes |
| `escalated` | SLA expired; still open with higher visibility | Yes |
| `approved` | Terminal approval | No |
| `rejected` | Terminal rejection | No |

Only terminal records are eligible for approval retention. An old open request is still a live authorization decision and is never age-purged.

## Create records at the trusted boundary

`queueApprovalForSuspension()` is the normal creation path. It observes the authoritative run summary and records:

- `workflowId` and server-minted `runId`
- the suspended `stepPath`
- `suspendedAt` and the runtime-owned `resumeCount`
- `requestedBy` and `requestedByKind` from the execution principal that advanced the run; a human approval resume is attributed to its decider
- `connectors` from the server-authored suspend payload
- `grantScope` derived by the service
- `toolCallId` from a durable-agent approval suspension
- an optional server-authored durable-agent `resumeTarget`

The HTTP create route is disabled by default. If a host enables it, the router requires write access to the named run and rejects every field that could select a capability, change attribution, or choose a resume target. An HTTP-created request can collect a human decision, but cannot mint a connector grant or resume execution.

Do not derive a workflow suspension's `connectors` array from workflow input, model output, signal attributes, or another client-controlled value. Durable-agent `toolName` and `toolCallId` come from Mastra's persisted approval payload in one of its two supported shapes.

## Derive grants from stored decisions

`approvalGrantProvider(deploymentStore)` reads approved records on every start or resume. A step-bound grant is available only when these values match the leg being resumed:

- workflow and run
- step path
- `suspendedAt`
- `resumeCount`

This pairing keeps multiple suspensions of the same step distinct even when their millisecond timestamps collide. An older approval is spent when the step suspends again.

A durable-agent approval also binds the connector's Mastra `toolCallId`. Mastra persists that ID in the durable tool-call input and reproduces it in `context.agent.toolCallId` after resume or Durable Object reconstruction. The grant scope is `tool-call`.

An arbitrary workflow gate has no reproducible tool-call identity. Its grant scope is `suspension`, which uses the exact leg fields above. A trusted host can create an explicit `runScoped` record for `run` scope. A step-less record without `runScoped: true` mints nothing. The suspend bridge never creates run-scoped grants.

The provider writes the structured grants to `breakwater.connectorGrants`. `RunnerRuntime` writes the current leg to `breakwater.connectorExecution`. Neither value crosses the public API in a resume payload. Missing scope, legacy connector ID arrays, and malformed records fail closed.

Retries of the same durable tool call reuse `toolCallId` and remain authorized. A new model tool call receives a new ID and requires approval. The grant does not make retries idempotent; configure the connector's idempotency policy when duplicate side effects are unsafe.

## Enforce separation of duties

`ApprovalService` denies self-decision by default. The current decider cannot:

- decide a record they requested;
- decide a later gate when they approved an earlier gate that led to it.

The second check pages the complete approved history for the run, so an older gate cannot disappear behind a bounded list.

Set `APPROVAL_ALLOW_SELF_DECISION` only when your operating model has no independent reviewer. Prefer a role list such as `admin` over `true`. Every permitted self-decision is marked in audit detail.

Roles are enforced server-side:

- `reviewer` and `admin` can perform review operations according to the service rules.
- Other roles may list or inspect only where the host's configured policy permits it.
- Per-workflow start, resume, and terminate authorization belongs to the run router, not the approval record.

Termination and deadline expiration never decide or resume an approval. They atomically fence new decisions against the terminal intent, then abandon every open record through system-attributed stale supersession. SLA escalation remains independent and never cancels a run.

Authentication and actor-to-role mapping remain host responsibilities.

## REST API

The default base path is `/api/approvals`.

| Method and path | Purpose |
| --- | --- |
| `GET /api/approvals` | List deployment records visible to the actor's role |
| `GET /api/approvals/metrics` | Return queue metrics |
| `GET /api/approvals/:id` | Read one record |
| `POST /api/approvals/:id/claim` | Claim an open record |
| `POST /api/approvals/:id/delegate` | Delegate to another reviewer |
| `POST /api/approvals/:id/decide` | Approve or reject one record |
| `POST /api/approvals/batch/decide` | Decide up to 100 records through the same per-record checks |
| `POST /api/approvals` | Optional, capability-free create route; disabled by default |

List filters include `status`, `workflowId`, `runId`, `claimedBy`, `requestedBy`, strict `createdBefore` and `createdAfter` bounds, `limit`, `after`, and `orderBy`.

`orderBy=created` is FIFO and supports cursor pagination. `orderBy=reviewer` sorts by priority, nearest SLA deadline, then FIFO; it cannot be combined with `after`.

Batch decisions return per-record outcomes. They are not a transaction across the batch, and partial success is expected when an id is unknown, record status conflicts, validation fails, or authorization and separation-of-duties checks differ.

## Concurrency behavior

Both D1 and in-memory stores implement compare-and-swap transitions:

- one open request exists for a suspension;
- competing claims cannot both win;
- a terminal decision cannot be overwritten;
- a delegate or decision that loses a race receives the current state;
- record creation retries the D1 decision race instead of returning a phantom conflict.

The service always routes batch work through the same single-record methods. Batch APIs do not bypass authorization, attribution, notification, audit, or resume logic.

## SLA and notifications

`sweepSLA(store, options)` reads the deployment store and transitions overdue open requests to `escalated`. Run it through the maintenance Durable Object duty, never an HTTP route.

`ApprovalNotificationSink` receives contained callbacks when a record is created or escalated. A transport failure does not undo the approval mutation; flowsafe writes an `approval.notify` audit failure.

`ApprovalStreamSink` carries mutation events to the live hub. It is also contained and must not be treated as the source of record truth.

## Dashboard and live updates

`@proofoftech/flowsafe/approval-ui` provides:

- `ApprovalApiClient`, which has no DOM dependency;
- `createApprovalDashboard()` for a mounted plain-HTML dashboard;
- `useApprovalDashboard()` for headless React integration;
- queue, detail, filters, metrics, and batch-decision views;
- injected slots through `ApprovalUIComponents`;
- WebSocket transport, optimistic decision state, live merge, presence, toast slots, and polling reconciliation.

React and React DOM are optional peers required only for this subpath. The component contract supports React 18 and 19.

Live streaming is optional. When the host has both a `HUB` Durable Object binding and `STREAM_TICKET_SECRET`, an authenticated client can mint a short-lived HMAC ticket and open:

- a deployment queue channel at `/api/stream/hub`;
- a run channel at `/api/stream/run/:workflowId/:runId`.

The ticket carries addressing data, not approval authority. The Worker verifies it. The singleton hub and each run Durable Object rebind the addressed channel through `idFromName()`. Polling remains the reconciliation path if a socket fails or streaming is absent.

## Resume failure recovery

An approval may be terminal while its resume result reports failure. Causes include a transient Durable Object failure, an evicted durable-agent isolate that needs registry rehydration, or a downstream step error.

Recovery rules:

1. Read the stored record and authoritative run status.
2. Do not create another approval for the same suspension.
3. For a workflow, invoke the trusted resume bridge again.
4. For a durable agent, validate the persisted memory binding and redrive the trusted approval-resume bridge. It derives fresh trusted context and invokes only RBAC's `processInput` hook during rehydration. It then restores both Mastra registries with the complete runtime processor lists, observes and registers the stream, and resumes through `RunnerRuntime`.
5. Let `approvalGrantProvider()` derive the same approved capability from D1.
6. If the run immediately suspends at another gate, queue a new approval for the new fingerprint.

Do not call public `prepare()` for durable-agent recovery. It is an initial-execution API that runs application and policy input processors. Never copy `breakwater.connectorGrants` or `breakwater.connectorExecution` into a recovery request.

## Retention and audit

- `purgeExpiredApprovals()` deletes only `approved` and `rejected` records past the configured age.
- Deployment decommissioning deletes the bound D1 database after credentials and traffic have been revoked.
- Approval audit events use the same structural sink as Breakwater. Decision and connector-approval events identify `tool-call`, `suspension`, or `run` scope without recording connector inputs.
- The full `ApprovalRecord` may contain reviewer context. Treat notification and stream sinks as organization-confidential channels.

See [Deployment reference](deployment-reference.md), [Operations runbook](operations-runbook.md), and [Security threat model](security-threat-model.md) before production.
