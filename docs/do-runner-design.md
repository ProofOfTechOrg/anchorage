# Durable Object runner design

Flowsafe supplies a Cloudflare execution backend for Mastra workflows. The Durable Object serializes calls for one run; Mastra's D1 snapshot is the durable workflow state.

## Import swap

`init()` returns backend-bound workflow factories:

```typescript
import {
  DurableObjectRunner,
  init,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import { approvalStoreFactoryFor } from '@proofoftech/flowsafe/host-kit';
import { z } from 'zod';

interface Env {
  DB: D1Database;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
}

export class WorkflowRunner extends DurableObjectRunner<Env> {
  protected runOwnership(env: Env) {
    return approvalStoreFactoryFor(env.DB).resources();
  }

  protected build(env: Env): RunnerRuntime {
    const { createStep, createWorkflow, runtime } = init(env);

    const prepare = createStep({
      id: 'prepare',
      inputSchema: z.object({ subject: z.string() }),
      outputSchema: z.object({ subject: z.string() }),
      execute: async ({ inputData }) => inputData,
    });

    createWorkflow({
      id: 'example',
      inputSchema: z.object({ subject: z.string() }),
      outputSchema: z.object({ subject: z.string() }),
    })
      .then(prepare)
      .commit();

    return runtime;
  }
}
```

Workflow definitions retain Mastra's schema and builder API. The difference is where `createRun()`, `start()`, `resume()`, and storage are driven.

## One object per run

The Worker addresses:

```text
idFromName(`${workflowId}:${runId}`)
```

The object checks every request's workflow and run against its own `id.name`. The run router validates both path segments before it addresses the namespace.

Inside the object, `RunnerRuntime` keeps a FIFO lock keyed by the same `workflowId:runId`. Cloudflare sends one object's requests to one instance, and the in-instance lock prevents two concurrent resumes from both passing the suspended precondition.

This single-writer property is load-bearing because the Mastra snapshot store uses ordinary writes, not an application-level version CAS. A future path that lets a second object or external process write the same run must add optimistic concurrency before it is safe.

## Run identity

`workflowId` and `runId` must match the exported path-safe pattern. Connector ids use a separate contract and cannot contain the key delimiter that would make stored tuple keys collide.

The authenticated host mints an opaque run id. `RunnerRuntime.start()` requires it and never generates one. `DurableObjectRunner` recovers the authoritative workflow and run pair from its own object name.

A run id carries no organization identity. Physical deployment bindings provide that boundary, so ordinary application code should treat the id as opaque.

## Runtime lifecycle

Start:

1. The Worker verifies deployment identity, authenticates the actor, authorizes the workflow, and mints the run id.
2. The run topology addresses the owning Durable Object and overwrites the internal deployment-identity header.
3. The object authenticates the caller credential, validates its target sentinel, and checks that no snapshot already exists.
4. It obtains the trusted per-leg request context.
5. It calls Mastra `createRun({ runId, pubsub })` and starts with input.
6. Mastra persists its workflow snapshot after engine boundaries.
7. The runtime projects the outcome to a JSON-safe `RunSummary`.
8. The object broadcasts the summary to connected run sockets.

Resume:

1. The approval service or trusted recovery path reaches the same object.
2. The FIFO lock reads the current snapshot and requires `suspended`.
3. The runtime increments the selected step's ordinal in trusted snapshot provenance.
4. The runtime recomputes trusted request context, including workflow scope, run ID, current execution identity, and structured connector grants.
5. Mastra resumes the selected step from D1.
6. The new summary is persisted and broadcast.
7. A new suspension is bridged to a new approval.

No JavaScript promise remains alive while a reviewer waits. The persisted suspension is the wait.

Terminate:

1. The Worker checks run ownership and sends the trusted principal to the owner object.
2. The object reads persisted lifecycle state and refuses a disputed settlement before it cancels active work.
3. A live Mastra run receives cancellation only after the object persists a terminal intent.
4. The runtime replaces Mastra's matching `canceled` precursor with `cancelled` and a `CANCELLED` envelope.
5. Cleanup abandons open approvals, settles an executing agent-schedule receipt as discarded, and releases run ownership.
6. The runtime records cleanup completion after the preceding steps succeed.

A repeated request reads the terminal snapshot and returns the same summary. After ownership release, the public router delegates replay authorization to the owner object. The object accepts only a principal recorded by the original transition.

### Fence execution during a deployment migration

The execution fence controls one physical deployment, which is also one tenant boundary. It is never scoped to an actor or run. Each admission reads the current row without memoization, and storage failures fail closed.

The four states apply this matrix:

| Entry family | `open` | `draining` | `migration-locked` | `proof-only` |
| --- | --- | --- | --- | --- |
| New run start | Admit | Refuse | Refuse | Admit only when `idempotencyKey` matches `proofKey` |
| Resume, approval decision, or delivery to an existing run | Admit | Admit | Refuse | Admit only for the bound `proofRunId` |
| Schedule or objective authoring and due-fire claims | Admit | Refuse | Refuse | Refuse |
| Background tasks: new enqueue | Admit | Admit | Refuse | Refuse |
| Background tasks: dispatch or stale re-drive | Admit | Admit | Refuse | Refuse |
| Notification dispatch, provider polling, and webhook ingress | Admit | Admit | Refuse | Refuse |

Fence-parked task rows are still swept for resume under the lock and simply re-park. This churn is self-limiting, and no task body executes.

Schedule pause and delete, objective clear, reads, termination, cancellation, and timeout remain available in every state because they remove work or observe state. During `draining`, signal wakes that would mint a run degrade to persistence, so the next deployment can deliver them. The fence never preempts compute already in flight. Drain first, prove the work inventory empty, and only then transition to `migration-locked`.

A refused request returns `503` with `reason.code: 'EXECUTION_FENCED'` and the current state. `proof-only` admits only proof-bound operations: webhook and poll ingress cannot identify the target run and remain blocked. The package enforces state names, compare-and-set (CAS), and proof-key requirements, while the host owns transition policy.

### Start a run idempotently

An idempotent start stores a caller-supplied `idempotencyKey` before execution and binds it to one server-minted run ID. Callers never supply a run ID. `POST /runs` continues to return `400` when its body contains `runId`; the key is the request handle, not the run identity.

The key is accepted by `POST /runs`, trusted `AgentThreadStartInput` calls, and the internal `streamUntilPersisted()` start. `RunRouterOptions`, `AgentThreadTopologyOptions`, and `StorageInitOptions` require `startIdempotency` wiring. Pass `'none'` only when the host has no reservation database; a keyed start on that host returns `IDEMPOTENT_START_UNSUPPORTED`.

Retries use these outcomes:

- A persisted snapshot returns the same run and state without executing again
- A `reserved` row with no snapshot reclaims the same reserved run ID
- A live `started` row with no snapshot returns `IDEMPOTENT_START_PENDING` and `pendingSince`
- A non-live `started` row with no snapshot returns `IDEMPOTENT_START_UNRESOLVABLE`
- A `terminal` row whose snapshot expired returns `IDEMPOTENT_START_ALREADY_SETTLED`

`IDEMPOTENT_START_UNRESOLVABLE` is a point-in-time probe. A read can occur between the Worker-side claim and the run object learning that execution started, so re-probe before investigating or choosing a fresh key. Flowsafe never starts another run automatically. A replayed suspended start returns the persisted `RunSummary` without the start response's `approval` and `approvals` fields; read `GET /runs/:workflowId/:runId` to reconcile approval state.

The reservation remains valid for its retention horizon. Once the terminal reservation is purged, the same key is fresh and can start another run. Keep the horizon at least as long as callers can retry.

## Durable state

### D1 snapshot

Mastra's `mastra_workflow_snapshot` row is authoritative for:

- run status;
- step state and results;
- suspend payload and selected step paths;
- timestamps;
- durable-agent message-list state where applicable.

Flowsafe does not maintain a parallel custom workflow state object.

### Execution fence and start reservations

`flowsafe_execution_fence` stores the deployment's singleton fence state, optional proof key, and optional bound proof run. State transitions compare the caller's `expected` state before they write. A database created by Flowsafe 0.19 has no row or table, which reads as `open`; provisioning from 0.20 onward writes an explicit initial row.

`flowsafe_start_idempotency` stores owner, target, server-minted run ID, reservation state, and timestamps. The claim from `reserved` to `started` is the cross-isolate serializer. Terminal run cleanup pairs snapshot and reservation retention so a spent key remains distinguishable from a fresh key until its configured horizon expires.

### Snapshot provenance

Flowsafe stores trusted run provenance under reserved request-context keys in the same authoritative snapshot as the workflow state. Provenance records the initiating actor, a per-leg attempt token, and a monotonic ordinal per run and step:

- absent on the first suspension;
- `1` after the first resume;
- `2` after the second;
- continuing on every resume, including a resume without payload.

Approvals bind `suspendedAt` with this ordinal. Because workflow state and provenance commit together, recovery can distinguish a committed resume from a pre-snapshot failure without a parallel counter in Durable Object storage.

The optional `flowsafe.runLifecycle` record stores deadlines, trusted economic-settlement projections, agent-schedule dispatch references, terminal intents, terminal reasons, replay principals, and cleanup completion. Ordinary runs omit this record. Start or resume adds it only when the trusted caller supplies lifecycle data.

Economic settlement projections enter through internal `StartRunOptions.economicOperations` or `ResumeRunOptions.economicOperations`. The host fixes them at the execution-leg boundary before the leg becomes cancellable. Public HTTP bodies cannot supply them. The runtime exposes no dynamic mid-leg mutation because an external snapshot update could race Mastra's own snapshot writes and lose a disputed marker.

The run's Durable Object alarm serves two duties: reconciling an interrupted run-owner reservation, and the per-suspension deadlines described below. It is armed at the earlier of the two due times. Starts and approval resumes still arrive by request; the alarm drives execution only when a suspension deadline expires.

### Per-suspension deadlines

A suspended step can carry its own deadline. When it elapses before the awaited signal arrives, the run's own Durable Object resumes the run.

A step arms one by adding the reserved key to the payload it hands Mastra's `suspend()`:

```typescript
import {
  isSuspensionTimeoutResumeData,
  SUSPENSION_DEADLINE_PAYLOAD_KEY,
} from '@proofoftech/flowsafe/do-runner';
import { z } from 'zod';

const gate = createStep({
  id: 'gate',
  inputSchema: z.object({ topic: z.string() }),
  outputSchema: z.object({ topic: z.string(), settledBy: z.string() }),
  execute: async ({ inputData, resumeData, suspend }) => {
    if (!resumeData) {
      return suspend({
        reason: 'awaiting approval',
        [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000,
      });
    }
    return {
      topic: inputData.topic,
      settledBy: isSuspensionTimeoutResumeData(resumeData)
        ? 'timeout'
        : 'signal',
    };
  },
});
```

The value is relative milliseconds, a safe integer between `MIN_SUSPENSION_DEADLINE_MS` and `MAX_SUSPENSION_DEADLINE_MS` (365 days). A step declaring a Zod `suspendSchema` must declare the reserved field, or use a loose object (`z.looseObject()`, or `.passthrough()` on a `z.object()`): Mastra validates the suspend payload and substitutes the parsed output, so a strict `z.object()` strips the key and nothing arms. A malformed or out-of-range value never fails the suspension — the suspension is already persisted — it is logged and left unarmed.

Only a top-level suspended step can arm one. Mastra reports a step suspended inside a nested workflow under its nested path, but keys the suspend payload, `suspendedAt`, and `resumeCount` by the enclosing top-level step, so a nested suspension has no fence of its own and an entry that cannot be fenced must never be armed. A nested path carrying the reserved key is therefore refused and logged, not armed. The refusal holds on both projections of that suspension: the live result reports the nested path, while the snapshot-rehydrated one collapses it to the enclosing step and is recognized by the nesting marker Mastra persists in the payload (`__workflow_meta.path`, pinned by a tripwire test).

A top-level step id that contains a dot does arm normally. Entries are keyed by the dot-joined suspended path, which is exactly how both projections key the payload and the two fence fields, so `['a.b']` from a live result and `['a','b']` from a rehydrated snapshot describe the same entry. Such an id must not coincide with a nested path, though: a top-level step `'a.b'` suspended alongside a nested workflow `'a'` whose inner step `'b'` suspends produces one key for two suspensions, which Mastra's own snapshot namespace also collides. Neither projection can then say which suspension an entry belongs to, so every deadline on that key is refused rather than armed. A refusal is logged by whichever boundary reads a projection that shows it, so a request visible on only one projection is still refused everywhere and reported where it shows. The constraint is run-design-wide, not deadline-specific: the same dot-joined key space is what the approval bridge fences approval records on, so a top-level id that coincides with a nested path muddies every consumer of that namespace. A payload that carries `__workflow_meta.path` itself implies an ambiguous or nested key — that field is Mastra's nesting marker, and an author must not set it.

The expiring resume delivers one flowsafe-defined envelope as its resume data:

```json
{
  "flowsafe.suspensionTimeout": {
    "step": "gate",
    "deadlineAt": 1751883300000,
    "expiredAt": 1751883300118
  }
}
```

Branch on `isSuspensionTimeoutResumeData()` rather than the literal key. A step that declares a `resumeSchema` must accept this shape as well as its signal shape: Mastra validates resume data before the engine runs, so a schema that rejects the envelope makes every timeout resume throw, and the deadline is dropped once the retry budget is spent. The envelope is the runner's to mint — a resume request that carries the reserved key is refused with a 400, so a caller cannot drive a step's timeout branch while provenance still names them as the requester.

Behavior worth knowing before relying on it:

- The deadline is `suspendedAt + deadlineMs`, taken from the suspension itself, so repeated reconciliation cannot walk it forward.
- Before resuming, the object re-reads authoritative state and proceeds only while the run is still suspended at that step with the same `suspendedAt` and `resumeCount`. A real signal that arrived first drops the entry. Only a read that SUCCEEDED is evidence about the run. Mastra answers a state read from the in-memory run an isolate still holds whenever storage is unavailable or the row lookup comes back empty, and that answer carries the run object's own status — `pending` until something updates it — with no suspended paths and no request context. Mastra marks it (`isFromInMemory`), a persisted row never carries the mark, and the runner's authoritative read refuses it, so neither a lagging read replica nor a storage fault can discard a live deadline or spend its retry budget. A summary that reads back self-inconsistently — `suspended` with no suspended paths at all — is refused too, as a second line of defence for that shape whatever produced it.
- A failure INSIDE the deadline duty that leaves an entry unconsumed — a failing timeout resume, a run that reads back absent or self-inconsistent at the fence check — is charged to that entry's retry ledger, backed off, and abandoned after five failures; every alarm is armed at least a second out, so an entry that stays due backs off instead of re-arming at its own past deadline, which Cloudflare would fire immediately. Only a wake whose authoritative read SUCCEEDED may spend that budget, with one standing exception in the other direction: a wake whose stored record names a run other than the object's own `idFromName` identity is refused before it reads anything, and that refusal IS charged — charging is what walks a foreign record to a tombstone in five wakes and quiets it, and no object reachable through the exported topology (`host-kit/do-run-topology.ts`) can be addressed under a name its record disagrees with. A read that did not succeed — Mastra's in-memory fallback, or a read that threw, whether from a workflow a later deploy unregistered or from a storage fault — charges nothing and keeps the 60 second watchdog cadence until it heals: bounded to one wake and one read per minute, never the floor, never a delete. A read that succeeded and found NOTHING is the other case: the store answered, and staleness windows are sub-second against a fifteen-minute budget, so a genuinely absent run is charged and terminates quietly as a tombstone. The uncharged side has one bound of its own — an entry that has been DUE for 24 hours with the run's state continuously unreadable is abandoned under its own log (`abandoned after 24 h of unreadable run state`), and abandoning it converges the wake, so an object with nothing else armed deletes its alarm instead of heart-beating forever for a registration a deploy dropped for good. That clock starts when an entry falls due, never when it is armed, and one failed read stamps every entry due at that wake: a record clears about a day after its last entry came due, not a day per entry, while an entry armed further out is left untouched until its own turn. A wake that cannot read its record, BUILD its runtime, or write its ledger keeps the same cadence for the same reason: none of those faults is evidence about any entry, and a misconfigured binding throws from `build(env)` on every wake, so charging it would tombstone every live deadline of the run in five wakes.
- A wake with a record in hand that cannot read authoritative state keeps that record: it re-arms nothing, keeps the 60 second cadence, and the entry is charged only once a wake that could read finds it due. On a wake with NO record, a read that succeeds with null converges instead — there is nothing to keep, so the wake arms nothing and deletes the alarm. Both of those are bounded and self-healing, though during a storage incident the affected population is every suspended run whose object takes a wake. Two cases are neither. A wake with NO record whose read THROWS has no record to keep, no entry to charge and none to stamp, so it keeps the 60 second heartbeat with no terminator until the read heals or the isolate is evicted. That is accepted rather than converged — converging it would delete the alarm of a run whose deadline record a failed boundary write never landed, which is the failure that retry wake exists for — and reaching it takes a retry wake followed by the run's workflow itself going unreadable for good. A wake whose `build(env)` throws is the other, and it needs no record at all: a misconfigured binding fails before any entry is in hand, so the wake charges nothing and stamps nothing — not even the 24 hour clock, which runs only on entries a failed READ found due — and it too keeps the 60 second heartbeat with no terminator until the deployment is fixed. That is deliberate: the fault is the host's and says nothing about any deadline, and the alternative is tombstoning every live deadline of the run over a configuration mistake.
- The resume records `requestedByKind: 'system'` and the reserved principal id `flowsafe-suspension-deadline`, and broadcasts the new summary like any other resume.
- A timeout resume is NOT an approval decision. It mints no grant and records no reviewer. A step that gates a privileged action must treat the timeout branch as a denial, an escalation, or a no-op — never as consent.
- An armed deadline is not observable through `RunSummary` in v1. The record lives in the run object's own storage, no route projects it, and the bounds that describe it are exported so a consumer can validate its own `deadlineMs` before arming, while `MAX_SUSPENSION_DEADLINES_PER_RUN` is exported as an operational figure rather than as something to check a deadline against. The workerd spike needs a test-only introspection route on its Durable Object precisely because nothing else can see the armed state.
- A timeout resume takes no host route, so the hooks a resume normally passes through do not fire for it: `RunRouterOptions.beforeResume` cannot vet it (the run object resumes itself), and `RunRouterOptions.reconcileApprovals` / `reconcileApprovalsForSummary` do not run at that moment. An approval record filed for the expired suspension therefore stays open until a later host status read reconciles it, where the `(suspendedAt, resumeCount)` binding it already carries shows the suspension moved on. Nothing decides that approval, and nothing acts on a decision arriving late.
- A `foreach` step arms one deadline for the step as a whole, because one suspended path with one fence is all either projection reports for it. A default `foreach` is sequential: one iteration is suspended at a time, so each iteration's suspension gets its own deadline and clearing a whole `foreach` by timeout takes one deadline per item. With `concurrency` above 1, up to `concurrency` iterations suspend at once behind that one path, one `suspendedAt`, and the FIRST suspended iteration's payload, so only that iteration's `deadlineMs` is read per batch; the timeout resume delivers the envelope to every iteration suspended at that moment (at most `concurrency` of them), iterations not yet started then suspend afresh with their own deadline, and clearing the whole `foreach` takes `ceil(items / concurrency)` deadlines.
- Wake precision is the Durable Object alarm's, near the requested time rather than exact. There is no maintenance-sweep backstop, so a lost alarm is a lost deadline; run-level `deadlineMs` remains the swept mechanism. The record itself is a separate best-effort write made after Mastra has persisted the suspension, not part of it: a write that fails leaves a 60 second retry wake. Every wake that does not resume — that retry wake included — ends by re-deriving the run's deadlines from an authoritative read; the stored record is only the identity fallback for an object that carries no name, never the source the wake trusts over authoritative state. One re-derivation is not made from an authoritative read: the terminal deadline route's non-finalizing branch — the CAS-stale answer, and the already-cleaned-up replay — reconciles from `result.summary`, which `timeOutAsPrincipal` produced with a post-persist `getWorkflowRunById`, the read Mastra can answer from its in-memory fallback. A marked answer there would derive nothing and clear a record for a run that is still suspended. It is not guarded mechanically because reaching it takes that second read flipping to the fallback inside one held run lock, after the snapshot read the branch opens with has already succeeded on the same store; and if it ever did, the run's own next boundary or wake re-derives what was dropped, since the record is bookkeeping about a suspension the snapshot still holds.
- One entry fires per wake, at most 32 entries are armed per run, and a failing wake backs off and is abandoned after five failures — abandoned for that suspension: the spent entry stays in the record as a tombstone, never selected and never armed again, so reconciliation cannot re-derive the same suspension a fresh budget; a later suspension of the same step starts a fresh budget. A tombstone counts against the 32-entry cap, which stays bounded per run, and abandonment is therefore cap-conditional: entries are capped in deadline order, a tombstone belongs to an older suspension than the live ones around it, so past 32 concurrently armed suspensions a tombstone holds its slot while the newest live deadline is the one refused — and a tombstone that the cap does splice out re-derives with a fresh budget. A record left holding only tombstones arms no alarm at all and is cleared by the next lifecycle boundary or wake that reads a run which is no longer suspended there; for a run whose state is gone entirely, nothing reads it again and the record persists (about 150 bytes per entry, bounded by the cap). Both are accepted. The stored record, its parser, and the wake arithmetic are not exported: they are the run object's own state, and only the object that owns the alarm can act on them.
- Scope is `DurableObjectRunner`-hosted workflow runs. The durable-agent runner has its own resume path and does not arm suspension deadlines.

## Run summary

`RunSummary` is the public projection:

```typescript
import type { ExecutionPrincipalKind, RunStatus } from '@proofoftech/flowsafe';

interface RunSummary {
  runId: string;
  status: RunStatus;
  result?: unknown;
  error?: string;
  errorEnvelope?: {
    code: 'CANCELLED' | 'TIMED_OUT';
    message: string;
  };
  deadlineAt?: number;
  suspended?: string[][];
  suspendPayload?: unknown;
  suspendedAt?: Record<string, number>;
  resumedAt?: Record<string, number>;
  resumeCount?: Record<string, number>;
  requestedBy?: string;
  requestedByKind?: ExecutionPrincipalKind;
  createdAt?: string;
  updatedAt?: string;
}
```

The runtime maps serialized errors from existing failed runs to `error`. Flowsafe-owned `cancelled` and `timed_out` summaries use `errorEnvelope` without changing that failed-run shape. `resumedAt` is informational because Mastra does not stamp it for every resume shape. Grant binding uses `resumeCount`. `requestedByKind` is paired with `requestedBy`; it is absent only on legacy snapshots written before principal kinds were persisted.

Run WebSockets send the entire authoritative summary at start, resume, terminate, deadline expiry, and connection. Consumers can replace their cached summary rather than reconstructing state from deltas.

## HTTP surface inside the object

The base object accepts:

```text
POST /runs
GET  /runs/:workflowId/:runId
GET  /runs/:workflowId/:runId/dispatch-status
GET  /runs/:workflowId/:runId/stream
POST /runs/:workflowId/:runId/resume
POST /runs/:workflowId/:runId/terminate
POST /runs/:workflowId/:runId/terminate-replay
POST /runs/:workflowId/:runId/deadline
```

The composed Worker also exposes these control-plane routes before tenant routers:

```text
GET  /admin/execution-fence
POST /admin/execution-fence
GET  /admin/inventory
```

All three require a bearer token matching `MAINTENANCE_ADMIN_SECRET`, which must differ from `DEPLOYMENT_IDENTITY_SECRET`. The deployment-identity gate still runs first. A mis-provisioned deployment therefore returns `503` before an operator can read or move its fence.

`GET /admin/execution-fence` returns `{ state, proofKey?, proofRunId? }`. `POST /admin/execution-fence` accepts `{ expected, next, proofKey? }`; a stale expectation returns `409` with `reason.code: 'FENCE_CAS_CONFLICT'` and the current state.

`GET /admin/inventory` returns an index or one keyset-paginated category selected with `?category&cursor&limit`. The work categories are `runs`, `approvals-waiting`, `schedule-deferred-dispatches`, `pending-notifications`, `background-tasks`, `resource-owners`, and `start-reservations`. The standing categories are `schedules` and `signal-subscriptions`; they are reported for reconciliation and never required to empty.

`INVENTORY_DRAIN_PROOF` defines a proof as two consecutive full sweeps with every work category empty, at least 60 seconds apart, while the fence remains `draining`. Each reading is a point-in-time observation rather than a snapshot and can move in either direction because draining still admits work. Empty results cannot over-count, and keyset pagination never skips a row that existed before the sweep began.

If a host needs a hard guarantee, it can re-sweep once after transitioning to `migration-locked`. An empty post-lock sweep is conclusive. A non-empty sweep means work is still outstanding: either it entered after the proof or the lock parked it before it finished. Return to `draining`, let it finish, and repeat the proof before locking again. An inventory read taken under `migration-locked` measures what the fence parked rather than what the deployment would otherwise be doing.

`INVENTORY_UNENUMERABLE` declares two visibility gaps instead of hiding them. The run-owner recovery journal can remain outside D1 for one 60-second alarm cadence. Persisted idle signals have no enumerable consumption marker and deliberately survive the migration. `FLOWSAFE_TABLES` accounts for every Flowsafe-owned table, including justified exclusions, so a new table cannot silently escape the inventory census.

The public Worker normally exposes its own authenticated route facade and forwards to these internal routes. A stream request requires a WebSocket upgrade and workerd's hibernatable socket API; otherwise it returns 426 and the client polls status.

The raw resume surface carries no approval grant. A protected connector still requires a matching stored decision.

Start and resume bodies accept an optional nonnegative `deadlineMs`. Resume replaces the previous deadline relative to the accepted leg. The maintenance scanner reads expired candidates in bounded passes, then sends each revision-and-deadline compare-and-swap to the run's owner object. It never writes snapshots directly from maintenance.

The maintenance Durable Object persists a rotating tuple cursor after every selected row, including failures, so a permanently failing earliest row cannot starve later deadlines. Rows in `timed_out` with incomplete cleanup and any matching timeout intent remain eligible after an interrupted pass.

## Request context

Before every create or resume, the runtime sets:

- run id;
- workflow id;
- `breakwater.workflowScope`;
- values returned by the host's `requestContextForRun` provider.

Runtime-derived base keys win over stored or client-provided context. `breakwater.isolationScope` remains reserved and is dropped from provider values because connector keys are deployment-wide. Schedules additionally reject these reserved namespaces when data is written.

`approvalGrantProvider()` is the normal provider. A provider failure happens before `createRun()` or resume, so a failed start leaves its run id retryable.

## Import-safe workflow modules

`@proofoftech/flowsafe/host-kit/module` defines a workflow module that receives the factories and deployment dependencies rather than importing a singleton runtime. This supports:

- registering many modules on one runtime;
- testing a module with in-memory dependencies;
- keeping Cloudflare bindings out of module scope;
- avoiding a build-order dependency on generated `dist/`.

`assertWorkflowsRegistered()` compares host metadata with the committed runtime definitions.

## Error taxonomy

The runner preserves stable statuses and structured refusal reasons across Durable Object and host-router boundaries:

| Error | HTTP status | `reason.code` | Meaning |
| --- | --- | --- | --- |
| `ExecutionFencedError` | `503` | `EXECUTION_FENCED` | The current state refuses this execution entry |
| `ExecutionFenceUnreadableError` | `503` | `EXECUTION_FENCE_UNREADABLE` | Storage did not provide a trustworthy fence state |
| `FenceTransitionConflictError` | `409` | `FENCE_CAS_CONFLICT` | The caller's expected state is stale |
| `InvalidExecutionFenceRequestError` | `400` | `INVALID_EXECUTION_FENCE_REQUEST` | A transition body contains an invalid state or proof key |
| `StartReservationOwnerMismatchError` | `403` | `IDEMPOTENT_START_OWNER_MISMATCH` | Another principal owns the key |
| `StartReservationTargetMismatchError` | `409` | `IDEMPOTENT_START_TARGET_MISMATCH` | The caller's key names another workflow or agent |
| `IdempotentStartPendingError` | `503` | `IDEMPOTENT_START_PENDING` | The keyed run is live but has no persisted summary yet |
| `IdempotentStartUnresolvableError` | `409` | `IDEMPOTENT_START_UNRESOLVABLE` | No snapshot or live execution currently resolves the claimed run |
| `IdempotentStartAlreadySettledError` | `409` | `IDEMPOTENT_START_ALREADY_SETTLED` | The run settled and its summary expired |
| `StartIdempotencyUnsupportedError` | `503` | `IDEMPOTENT_START_UNSUPPORTED` | The host did not wire reservation storage |
| `StartReservationUnreadableError` | `503` | `IDEMPOTENT_START_UNREADABLE` | The reservation store or row is unreadable |
| `InvalidStartIdempotencyRequestError` | `400` | `INVALID_START_IDEMPOTENCY_REQUEST` | The key or reservation request is malformed |
| `InvalidInventoryRequestError` | `400` | `INVALID_INVENTORY_REQUEST` | The category, cursor, or limit is invalid |

`isStartReservationRefusal()` recognizes the five reservation decisions, unsupported wiring, and malformed input. It excludes `StartReservationUnreadableError`, which propagates as an operational storage failure.

The runtime also distinguishes unknown workflows, unknown runs, duplicate runs, runs that are not suspended, client-fixable input or resume-data errors, and internal execution or storage failures.

The Durable Object maps known errors to stable HTTP status codes through `doErrorResponse()`. Unknown failures return an internal error without copying arbitrary thrown data to an audit sink.

The runner does not provide an administrative “reset to last good state” API. Recovery uses authoritative D1 state, the approval redrive path, or deployment decommissioning.

## Retention

`purgeExpiredWorkflowRuns(db, options)`:

- selects only terminal statuses;
- uses a bounded batch;
- deletes paired R2 artifacts before the snapshot row;
- keeps a failed row as a retry anchor while allowing later rows to proceed;
- aggregates per-run failures after the pass;
- treats a not-yet-created snapshot table as empty.

The composed Worker resolves its optional artifact purger from the current maintenance invocation's environment inside this failure boundary. A factory or deletion failure keeps the enumerable snapshot row and does not stop approval or other domain purges.

When runtime storage uses `tablePrefix`, configure the same `storageTablePrefix` on `createFlowsafeWorker()`. It threads that validated prefix through every prefix-aware built-in purge. Direct callers of any exported low-level purge receive the same fail-fast validation before D1 preparation. Fixed-schema Flowsafe tables remain unprefixed.

Running and suspended rows are never age-purged. Retention treats `cancelled` and `timed_out` as terminal after lifecycle cleanup releases ownership.

Decommissioning deletes the bound storage after credentials and traffic are revoked. There is no in-database tenant purge in the single-organization data plane.

See [Deployment reference](deployment-reference.md) for the full domain lifecycle.

## Worker constraints

- Initialize Mastra after bindings exist, not at module load.
- Use D1-backed storage on the execution path.
- Keep workflows and schemas compatible with the Workers bundle.
- Route every writer for a run through its one Durable Object.
- Add Durable Object migrations as new append-only tags.
- Keep WebSocket polling fallback because sockets and isolates can close.

The deterministic `spike:verify` command kills and restarts workerd around a suspension, then proves resume, forged-resume denial, deployment-sentinel mismatch refusal, stream recovery, and the advanced agent domains.
