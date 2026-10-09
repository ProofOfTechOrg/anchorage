# @proofoftech/flowsafe

## 0.26.2

### Patch Changes

- 65f58d8: A durable-agent run cancelled or timed out while an approval resume prepares its leg now ends its run stream with `RunCancelledError` (`RunTimedOutError` for a timeout), whether the thread object or another instance recorded the transition; before this change the stream ended with `RunTerminalConflictError`, whose message named the run's stored status: `suspended` while only the transition's intent was recorded, or the settled status for another instance's terminal record. A resumed leg that a cancellation or timeout cut in this isolate and that then throws now ends its stream the same way, instead of with the thrown error. A resumed leg that another instance's settlement aborts, such as a terminate the new instance handles during a deploy, now ends its run stream with the error of the run's stored outcome, `RunCancelledError` for a terminate, instead of `RunSettledConflictError`; when the run's row is gone or its status cannot be read, the stream ends with the leg's own error. The approval resume's response is unchanged.
- 65f58d8: A durable-agent run whose start leg failed after its run was stored, and which a terminate then ended within Mastra's 30-second cleanup delay, no longer gets a second terminal `error` event on its run stream. When a start leg's own publication of its cancellation or timeout error fails, Mastra's retry publishes that error instead of the publication failure.

  With a host-supplied pub/sub whose `publish` can fail, a run gets no terminal `error` event, and a later release does not publish one, when Mastra's publication of the error its start leg threw fails, or when both the leg's own publication of its cancellation or timeout error and Mastra's retry fail.

- 31faca1: `FencedWorkflowAdmissionCapability.patchRunLifecycle` accepts an optional `expected.lifecycle`, the run lifecycle the caller read, and then writes only while the stored lifecycle is exactly that value. Pass it as read from the row, with its `lifecycleRevision`. `FencedWorkflowsStorageD1` implements it by comparing the stored JSON text, so a lifecycle rebuilt from parsed fields misses, and the runtime passes it. A cancellation or timeout intent therefore no longer replaces a run lifecycle that a resume on another instance advanced to the same revision. Before, that resume's deadline and economic-operation states were lost, and a run-deadline timeout whose deadline check the resume had made stale could still time the run out. A custom capability that ignores the field keeps the earlier behavior. During a deploy or a rollback, a terminate or a run-deadline timeout run by an earlier flowsafe version can still record its intent over a lifecycle it did not read.
- 31faca1: On `FencedWorkflowsStorageD1` storage, the runtime's own end-of-leg write now follows the storage's rules for a recorded cancellation or timeout intent, which matters for a workflow whose `shouldPersistSnapshot` skips the run's terminal snapshot. When the leg's write lands between an intent recorded on another instance and the terminal record, a run whose resume extended its deadline after a run-deadline timeout intent was recorded keeps its result, and the maintenance sweep decides on the new deadline; before, the run could be recorded `timed_out` with its `result` cleared and its cleanup run. A resume that records a disputed economic operation removes a cancellation or timeout intent recorded on another instance, and a terminate then answers `409` with `DISPUTED_SETTLEMENT`; before, the intent could stay beside the dispute, so the run object's wake refused to settle the run, or, after a second intent, the run was recorded `cancelled` or `timed_out` over the dispute with its `result` cleared. A terminate or deadline route that writes its terminal record in the same request decides from the stored row, so a leg whose write has not landed by then does not change its outcome. During a deploy or a rollback, a leg run by an earlier flowsafe version can still leave an intent beside a dispute or a moved deadline, or, at a lower revision than the stored lifecycle, drop its resume's dispute and extended deadline, so the run is recorded `cancelled` or `timed_out` with its `result` cleared and no dispute stored.
- e3935f9: `FencedWorkflowAdmissionCapability` gains an optional `withStoredRun`, which `FencedWorkflowsStorageD1` implements: inside it, a leg's write never inserts the run row the leg already stored. The runtime runs each leg's workflow writes inside it, aborts a leg whose run row is gone after the leg stored it at the leg's next liveness touch, and answers such a leg's writes with `RunSettledConflictError` (HTTP 409). Before, with a short `RUN_RETENTION_DAYS` such as `0`, a leg whose run another instance terminated, timed out or interrupted kept running its steps after retention removed the run's row, and its next write stored the run again as unsettled; a leg whose workflow skips its terminal snapshot answered 500. During a deploy or a rollback, a leg run by an earlier flowsafe version still stores a removed run again as unsettled.

  A custom capability without `withStoredRun` keeps the earlier behavior until the host's capability implements the member.

  With `RUN_RETENTION_DAYS` of `0`, a 409 from a start or a resume can also mean that the run completed and retention removed its own terminal row before the end-of-leg write read it, so check the run's effects before treating it as cancelled.

- 2b5b89c: `RunNotSuspendedError` extends `DoStatusError` with `status` 409 and `reason.code` `RUN_NOT_SUSPENDED`. The run object's resume route and `createRunRouter()` include the reason in the 409 body, and `DecideResult.resume.code` reports it when a decided approval's resume finds a workflow run not suspended, for example because it ended. Before this change that refusal carried no code, and a caller could tell it from a retryable resume failure only by parsing the message.
- 6fa6d8c: A threadless scheduled agent start whose outcome the tick could not learn, such as one whose start response was lost, stayed deferred forever. The drain inventory's `schedule-deferred-dispatches` category kept counting it, and deleting its schedule stayed pending (`202`).

  The tick now records such a fire as `failed` with reason `dispatch-unresolved` once the fire is an hour old and the `status` lookup throws with status 404. This includes a fire an earlier version deferred after a refusal, which the 0.26.0 notes said would stay deferred. A `status` seam must answer with the thread Durable Object's own status: a 404 relayed from another hop now fails a threadless start an hour after its fire.

  The fire of a run that started and was then cancelled, timed out or purged before any reconcile pass saw it is recorded `failed` too, and so is the fire of one that is still running with its ownership commit stuck behind a start recovery fault. The run's own status is authoritative, so check the agent's external effects before running its work again.

  A deferred threaded fire whose target had already stored its receipt stayed deferred, holding a slot in every reconcile pass, when its redelivery was refused and the `status` lookup kept failing. The tick now records it from that receipt.

## 0.26.1

### Patch Changes

- 5a7afd1: One schedule could stop every schedule in a deployment. The schedule router stored a target of any depth, and the tick copies the target into the fire's trigger metadata; a value nested about 1,000 levels deep (a request body of a few kilobytes) left a deferred fire whose metadata SQLite's JSON functions cannot parse, after which every tick failed before firing any schedule. Schedule create and update now refuse with HTTP 400, audited as `input-too-deep`, a target whose `inputData`, `initialState`, request-context value, `providerOptions` value or `ifIdle.streamOptions.requestContext` value nests more than 256 levels deep, the bound runs already apply. `providerOptions` values and stream request-context values were not bounded before: an agent schedule with such a value nested more than 256 levels now fails each fire. The tick records a fire whose stored target nests that deep as `failed` without dispatching it, whichever writer stored the target; settles a deferred fire whose stored dispatch nests that deep as `failed` (`invalid-deferred-dispatch`) without a status lookup, so a deployment an earlier version stopped recovers on upgrade; lists such a fire instead of failing the listing; and fires due schedules when reconciling deferred fires fails, then fails the pass with that error. The run router now answers HTTP 400 for a start's `deadlineMs`, or any field of a resume body, nested more than 256 levels deep, before it reserves a start key or forwards the body; before, such a field nested deeper than the Worker could serialize answered 500, and a keyed start left its key's reservation claimed until a retry.

## 0.26.0

### Minor Changes

- a51a1e7: Breaking: on `FencedWorkflowsStorageD1` storage a leg's write can no longer remove a recorded cancellation or timeout intent. Over a row that is not settled, a write whose run lifecycle has a lower revision than the stored one, or none, keeps the stored lifecycle, and a write of the same revision without an intent keeps the stored intent; the rest of the write lands. A write over a recorded intent that records a disputed economic operation is stored as written instead, so the terminate answers `409` with `DISPUTED_SETTLEMENT` rather than leaving the intent beside the dispute, and the capability's `patchRunLifecycle` resolves `false` over a stored dispute, so an intent does not land over one either. A run-deadline intent does not join a write of the same revision that moved its deadline. A run that completes after a terminate's intent landed is therefore recorded as cancelled, or as timed out on the run-deadline route, wherever its leg runs; before this change a completion written by a leg on another instance kept its result and the terminate answered `409`. The intent does not stop a leg on another instance, so such a run may have run every step with its effects, and its summary carries no `result`: check a cancelled run's effects before running its work again. A terminate that answers `503` because such a leg kept writing leaves the intent recorded, so approval resumes of the run answer `409` until a retry of the terminate completes the cancellation. It also closes a window in which a leg's write, serialized just before the intent landed, removed it, so a run whose isolate was then lost settled as `INTERRUPTED`, or stayed suspended, instead of cancelled. A run that completed before the intent landed still keeps its result and the terminate answers `409`. A leg run by an earlier flowsafe version during a deploy or rollback can still remove the intent.
- 6b77765: Breaking: start and resume now refuse run input nested more than 256 levels deep with HTTP 400: a workflow's `inputData`, `initialState`, each request-context value and `resumeData` (`InvalidRunRequestError`), and an agent start's caller messages and client context, scheduled starts included. `FencedWorkflowsStorageD1` refuses to store a run snapshot that SQLite cannot parse as JSON with the new `RunStateNotStorableError` (HTTP 422, `reason.code` `RUN_STATE_NOT_STORABLE`), unless the stored row is already one SQLite cannot parse. SQLite's JSON functions refuse text nested more than 1,000 levels deep, and the settled-row guard, the liveness touch, the run-deadline sweep, retention, the drain inventory and the approval run fence read run state through them, so such a run had no deadline, was never purged and could block a drain proof. A run whose leg writes state too deep to store now fails at once: it reads `failed` with `errorEnvelope.code` `RUN_STATE_NOT_STORABLE` over the last state it stored, and its leg is aborted. On a durable agent the runner publishes the run's terminal `error` event and the thread is released; the refused write can come after the loop has finished, so the agent's stream can end with a normal `finish` event, and the run's status is authoritative. `RunTerminalErrorEnvelope['code']` gains `RUN_STATE_NOT_STORABLE`. A start whose initial row is too deep, which only a host's `requestContextForRun` values can produce, is refused with `RunStateNotStorableError` and creates no run. The run router answers `400` for `inputData`, a request-context value or `resumeData` nested more than 256 levels deep, at any depth, before it reserves a start key or forwards the body; such a field nested deeper than the Worker could serialize used to answer `500` and leave a keyed start's reservation claimed. A scheduled agent fire that the host refuses with `400`, `403`, `404` or `422` is now recorded as failed instead of staying deferred: a threaded signal always, and a threadless start once the `status` lookup states that the run does not exist, at that fire or on a later reconcile pass. A signal refused with `422` for its content, which every reconcile pass used to send again, now fails the same way. `ScheduleTickSignalAgent` and `ScheduleTickStartAgent` document the statuses a custom seam must report, and deferred fires that an earlier version recorded stay deferred. Rows an earlier version stored stay writable, but the deadline sweep and retention skip them and the drain inventory counts each as outstanding work: the operations runbook gives the query that lists them. During a gradual deployment, a Worker on an earlier version answers a keyed start replay of a run that failed this way with `503`.

### Patch Changes

- 44d594d: A settlement or terminate abort now reaches a durable-agent start leg's model or tool call that was already in flight when Mastra's isolate-wide run registry evicted the run's entry, when the run has a total time budget. Before this change the eviction unlinked the leg's abort from the signal that call held. Calls a leg starts after such an eviction take Mastra's rebuilt entry and are not covered.
- 44d594d: A durable-agent run cancelled while its leg runs in the thread object, or terminated while suspended there, now ends its run stream with a terminal `error` event (`RunCancelledError`, or `RunTimedOutError` for a timeout) and releases the run's local state. A model or tool call that such a cancellation cuts sees an abort reason named `AbortError` whose `cause` is that error. Before this change observers of the run waited indefinitely, the run's Mastra registry entries stayed in memory, and a threaded run kept its thread active in the object, so a signal sent to the thread was queued into the cancelled run and could be lost.
- 995cca1: The agent thread object's alarm recovers every start journal even when one journal's recovery fails for a reason other than a pending start. The failing journal is kept and logged as an `agent-start-recovery-failed` line naming its run, and the wake reports the failure after the other journals are done. Before this change such a failure stopped recovery of every journal listed after it until it cleared.
- 44d594d: A durable-agent run resumed in the same thread object no longer leaks the previous leg's Mastra abort-request subscription, which kept that leg's run state in memory for the object's lifetime.
- 3fe8b05: `reconcileApprovalsForSummary` no longer supersedes or files a step's approval when the step already has an approval bound to a later suspension, one with a greater `resumeCount`, than the summary it was given. A status read whose summary predated a timeout resume closed the approval the run object had just filed for the step's next suspension, and, unless the earlier suspension was a timer, filed one for the suspension that had ended; the new gate was left with no approval that could be filed again. Runs already left that way are not repaired: the operations runbook says how to end or resume them.
- 98f88cb: A decided approval resumes its step only while the step is still at the suspension the approval was filed for. `resumeRecord`, `resumeViaRuntime` and the agent approval resumer pass the record's suspension to `RunnerRuntime.resume` through the new `ResumeRunOptions.expectedSuspension`, and the runtime refuses a mismatch with the new `SuspensionChangedError` (HTTP 409, `reason.code` `SUSPENSION_CHANGED`) before the step runs. The decision stays recorded and the decide response reports the refused resume, with the new `ResumeOutcome.code` set to `SUSPENSION_CHANGED` so a caller can tell it from a resume worth retrying. The refused approval still counts toward the separation-of-duties check across gates, so its reviewer cannot decide the step's current suspension unless an exemption applies to that reviewer. A workflow run's Durable Object files the step's current suspension at once, and other hosts file it at their next reconciling status read. A resumer that builds its own resume call or body gets the same check by spreading the new `expectedSuspensionFor(record)` into it. Before this change a decision on an approval for an earlier suspension resumed the step's current suspension with that decision's `resumeData`. Records without a captured suspension time, from earlier versions, resume as before.
- 8d2cebc: `FencedWorkflowsStorageD1` no longer writes over a stored run snapshot that SQLite cannot parse as JSON, such as one nested more than 1,000 levels deep, without deciding it first. The settled-row guard refuses such a write in SQL; the storage then reads the row, evaluates the guard's rule on the run lifecycle it parses from it, and writes an admitted snapshot with a compare-and-set against that row. The liveness touch decides such a row the same way. A leg still running on another instance therefore can no longer overwrite the terminate, timeout, interruption or start repair of such a run, and its next touch aborts it. Before this change the guard wrote over such a row and the touch reported it live, so the leg kept running and its next write replaced the settlement. Stored bytes that are not JSON at all hold no settlement and are still written over. A write over such a row that keeps missing a changing row answers `503` (`EXECUTION_FENCE_UNREADABLE`). A leg run by an earlier flowsafe version during a deploy or rollback still writes over such a row.

## 0.25.0

### Minor Changes

- e50aaa6: The agent thread host's start recovery now waits, as `DurableObjectRunner`'s does, until a pending agent start's run row has gone six minutes without a write before it repairs the start as `StartOutcomeUnknown`, and it stamps the repair so the settled-row guard refuses a later write from that start's leg. Before this change the host repaired a pending start about a minute after admission whether or not its leg was still running, and that leg's later writes overwrote the repair. A dead agent start now reads `StartOutcomeUnknown` about six to seven minutes after its last touch. Until then the thread stays blocked, the run's status and terminate routes and a schedule dispatch status read answer `503 RUN_START_PENDING`, and a keyed start replay answers `409 IDEMPOTENT_START_UNRESOLVABLE`, or `503 IDEMPOTENT_START_PENDING` while the start is live in the thread object's current isolate. Keep re-probing an agent start for at least seven minutes after its last activity before you choose a fresh key. When the start route sees its own leg fail in the object, it still repairs at once. A pending start no longer fails the thread object's alarm: recovery keeps its journal and wake, logs an `agent-start-recovery-pending` line, and goes on with the other journals.

  Two cases repair a start whose leg is still running on another instance:

  - A deploy from a version earlier than 0.25.0 leaves outgoing agent legs that do not touch their row. If one goes more than six minutes without a write, the thread object repairs and stamps it. From 0.24.x the settled-row guard refuses the outgoing leg's final write. From a version earlier than 0.24.0 the leg writes without the guard, so its write can land over the repair after the journal and ownership are gone; drain agent starts before such a deploy.
  - A leg whose touches fail to reach D1 for six minutes, for example during a D1 outage, is repaired and stamped. A later touch that reaches D1 aborts the leg; otherwise the settled-row guard refuses its next write. A leg in the thread object's current isolate is never repaired.

- 9c66e63: A leg whose run another Durable Object instance has settled is now aborted within about 30 seconds instead of ending at its next workflow write. The liveness touch a running leg makes every 30 seconds on `FencedWorkflowsStorageD1` storage now reports whether its run row is settled: terminated, timed out, interrupted, or repaired by start recovery. When it is, the runtime aborts the leg's Mastra run: the engine starts no further step, the step in flight sees its `abortSignal` aborted, and the leg's next write is refused as before. A step that calls external services should pass its `abortSignal` to them so the call in flight stops; a step that ignores it runs to completion. Mastra re-runs a step with `retries` that throws after the abort, so such a step should check `abortSignal.aborted` before its side effects and throw `MastraNonRetryableError` (from `@mastra/core/error`) once it is aborted. A call the abort cuts mid-flight may already have taken effect, and its abort error does not show whether it did. A run whose stored snapshot SQLite cannot parse, for example one nested past its JSON depth limit, is neither guarded nor reported settled, so its leg is not aborted. The touch no longer moves `updatedAt` on a settled row.

  The touch moved from `DurableObjectRunner` into `RunnerRuntime.start()` and `resume()`, so durable-agent legs on the agent thread host, and legs an in-process driver runs through a shared `RunnerRuntime`, now touch their run row as well. `FencedWorkflowAdmissionCapability.touchRun` may resolve `'live'`, `'settled'` or `'absent'`; a custom capability that resolves nothing keeps working and reports no settlement, so its legs are not aborted. A capability reports `'settled'` only when its storage refuses the aborted leg's later writes over that row. Legs running a flowsafe version without this change are not aborted.

  On a durable agent the abort also reaches the model call and the tool calls in flight, on start and resumed legs, and a terminate that the agent's own thread object handles cuts them too. A caller's `abortSignal` still aborts a start leg. A tool should pass the `abortSignal` it receives to its own I/O, and a tool that retries after an error should check `abortSignal.aborted` first, because a cut call's error does not always show that an abort cut it. With the matching Breakwater patch, a keyed connector call cut this way keeps its idempotency key reserved until stale takeover or operator recovery, and a call with the same key meanwhile is refused with `IDEMPOTENCY_CONFLICT`; earlier Breakwater releases free the key.

- fdda499: After every suspension-deadline resume, the run object now attempts to close an approval still open for the suspension that resume ended, whether the run then ended or suspended at another step. Before this change such a record stayed open and the SLA sweep escalated it; a reviewer could still decide it, but the decision resumed nothing. When the run has ended, every open approval of the run is closed, because none of them can be decided usefully. The close is one best-effort attempt. A failed read or hook, an eviction between the resume and the reconcile, a resume that throws after its result was saved, or a host status read that files from a summary taken before the resume leaves the record open, and the SLA sweep escalates it as before. Only suspensions a timeout resume ends after the upgrade are covered: a record left open earlier stays open, including one left open by an instance still running the earlier version.

  `DurableObjectRunLifecycleHooks.reconcileApprovals` receives the ended suspension as an optional third argument, `{ step, suspendedAt, resumeCount }`, and is now called after a timeout resume whatever the run's status, so a custom implementation must accept a summary that is not suspended. An implementation honours the argument by superseding each open record whose step, `suspendedAt` and `resumeCount` equal it exactly, counting an absent `resumeCount` as 0. `createFlowsafeRunnerLifecycle()` reconciles as before, then supersedes each open record bound to exactly that suspension, or every open record of a run that has ended. A hook that ignores the third argument leaves such a record open, as before, and now also receives summaries of finished runs.

### Patch Changes

- 9e35407: `DurableObjectWorkflowsStorageD1.updateWorkflowState`, the workflow domain `createBackgroundTaskD1Domains()` composes, now applies `expectedStatus` as the compare-and-set guard `@mastra/core` defines: when the stored snapshot's status does not match, the update writes nothing and resolves `undefined`, and the guard is never stored in the snapshot. Before this change the domain ignored the guard and stored it. Because the domain reports concurrent-update support, Mastra relies on that guard to let only one of two concurrent resumes of a suspended run claim it; without it both could proceed.
- 9e35407: `FencedWorkflowsStorageD1`'s `replaceSnapshot` capability member refuses a replacement whose `snapshot` is not a JSON object or whose `updatedAt` is not an ISO-8601 time in `Date.prototype.toISOString()` form, and writes nothing. Run retention compares the stored `updatedAt` with its cutoff as text, so any other form, including a valid time without milliseconds, can keep a run from being purged on time, and text that is not a time also makes every later settlement read of that run fail as unreadable.
- 4c673b4: A durable-agent leg resumed through `resumeViaRuntime()` now enforces the total time budget of its run. Before this change the resumed leg ran with no total budget, so a tool or model call that held past it was never aborted. The leg arms the `modelSettings.timeout.totalMs` the run's start call set, else the one in the agent's default options when the run started (or the current default when the start resolved none), and the budget starts again on each resumed leg. When it elapses, the leg's model and tool calls in flight see their `abortSignal` aborted with a `MastraTimeoutError` whose `timeoutType` is `'total'`, and `resumeViaRuntime()` resolves with `status: 'success'` and `result.stepResult.reason: 'error'`. A host that treats the resumed leg's success as completion of the approved action must check that reason. A start leg whose budget elapses ends its run the same way, but its `streamUntilPersisted()` rejects with the `MastraTimeoutError`. A settlement or terminate abort of the leg still reaches those calls with its own reason.
- 12e9df9: A terminate, and a run-deadline timeout from the maintenance sweep, now write their cancellation intent and terminal record without overwriting progress a leg wrote since they read the run, on `FencedWorkflowsStorageD1` storage. Before this change both writes replaced the run's row without checking that it was still the one they read, so a leg's write that landed between the read and the write was lost: a run that completed in that window could be recorded `cancelled` or `timed_out` without its result. The intent now changes only the run's lifecycle and timestamp, and only while the run's status and lifecycle revision are the ones it read, so a leg's step progress cannot make it miss and a leg in the same Durable Object still takes the cancellation however often it writes. The terminal record is a compare-and-set against the exact row it read. A write that finds the row changed reads the run again and decides from what it finds: a run that completed before the intent landed keeps its result and the request answers `409`. A write that finds the row changed on five attempts in a row answers `503`; retry the request. On `FencedWorkflowsStorageD1` that happens only while a leg on another instance writes continuously. `FencedWorkflowAdmissionCapability` gains an optional `patchRunLifecycle` member for the intent write. Storage that provides `replaceSnapshot` but not `patchRunLifecycle` writes the intent with the exact-row compare-and-set, and a leg in its own object that keeps writing then makes the request answer `503` without stopping the leg; retry it. Storage with neither keeps the unconditional write.

## 0.24.0

### Minor Changes

- b96f8dd: Breaking: Guarded durable calls refuse more call-level options. `stream()`, `streamUntilPersisted()`, `generate()`, and `prepare()` on a wrapped Breakwater guarded agent refuse, with a `TypeError`, the call-level options that would change what the guarded agent fixes at construction: tools, hooks, processors, scorers, instructions and system or context messages, step preparation, delegation and sub-agent versions, chunk callbacks and stream transforms, per-step saving, and an own `__proto__` property. See the [durable call-option restrictions](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/durable-agents.md#durable-call-options) for the list. `maxSteps`, `toolChoice`, and `disableBackgroundTasks` are accepted only with the guarded agent's own values. `memory` accepts only the thread and resource on these entries and on `resumeViaRuntime()`, and the wrapper forwards a frozen copy. The wrapper's constructor throws when the guarded agent's default options carry a refused option, lack `maxSteps`, `toolChoice`, or `disableBackgroundTasks`, or set `disableBackgroundTasks` to anything but `true`.

  Security: Before this change, host code that passed such an option to a guarded durable call could bypass the guarded agent's input or output policies, tool set, or memory configuration: for example, a per-call client tool whose mapped result the input policies never read, a call-level output processor list that replaced the output policies, or a working-memory template added to the system prompt. The check closes these per-call routes; the documented limits of durable results for guarded agents are unchanged. The check reads option values as data; the restrictions page names what it leaves to host code.

  Migration: Configure tools, instructions, memory, processors, hooks, and scorers on the guarded agent, not per call. Pass `maxSteps`, `toolChoice`, and `disableBackgroundTasks` only with the agent's own values, or omit them. Flowsafe's thread host, signal routes, and schedules pass none of the refused options.

- 21df7fb: A `DurableObjectRunner` run whose execution leg stops mid-step no longer stays `running`. A start, resume or suspension-deadline leg can stop this way when the platform ends the Durable Object invocation, for example about 15 minutes after the client disconnected. While a leg runs, it sets its run row's `updatedAt` in D1 every 30 seconds without writing the snapshot. Once the row has gone six minutes without a write, the run's own object settles the run on its next wake, with a compare-and-set against the row it read silent. The run becomes `failed` with `errorEnvelope: { code: 'INTERRUPTED', message }`, and its idempotent-start reservation is settled. A retry of the same key returns that result instead of `503 persisted start is not readable`. Flowsafe never re-executes the interrupted step, because it may already have had external effects. A run with a recorded cancellation or timeout completes that transition, with its cleanup, instead.

  A deploy does not interrupt a leg: the outgoing instance keeps running it, and its touches keep the run from being settled. A Workers runtime update gives in-flight requests at most 30 seconds, so it interrupts a longer leg. A wake no longer queues behind a leg that is still executing in the object. A leg frame older than two hours is reset with `ctx.abort()`, which releases the locks of a promise the platform stopped without settling. A leg that a client keeps connected for longer than two hours is reset the same way.

  Settlement needs the `FencedWorkflowsStorageD1` workflow domain that `createD1Storage()` composes. A live leg whose touches fail to reach D1 for six minutes is settled while it runs, and its next workflow write is refused. A run on other storage, including Mastra's own D1 workflow storage, or one whose leg ran on a flowsafe version before this one, is not settled automatically; `POST /runs/:workflowId/:runId/terminate` ends it.

  `RunTerminalErrorEnvelope.code` and `RunSummary.errorEnvelope.code` gain `'INTERRUPTED'`. Code that switches exhaustively on the code needs a branch for it. `FencedWorkflowAdmissionCapability` gains optional `touchRun` and `replaceSnapshot`; a custom capability without both never settles a run automatically.

  Each step must finish within one invocation. Split long work into steps, and wait in a suspension with a deadline rather than in an in-memory polling loop.

- 9feb010: Require `@mastra/core` `1.73.0` exactly and `@proofoftech/breakwater` `>=0.17.0 <1.0.0` when used. Pin the D1 storage dependency to `@mastra/cloudflare-d1` `1.4.0`.

  Breaking: Guarded durable calls refuse call-level `errorProcessors`, accepted in 0.23, alongside the existing structured-output restriction. See the [durable call-option restrictions](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/durable-agents.md#durable-call-options).

  Breaking: When a state read falls back to Mastra's in-memory run, `status()` and the run status, dispatch-status and stream routes report that run's lifecycle status, `suspended` with no suspended paths or `running`, instead of `pending`. The fallback state remains marked `isFromInMemory`; authoritative reads used by resume, deadline and terminate paths still refuse it.

  Core 1.73.0's `@mastra/core/events` entry calls `crypto.randomUUID()` while it loads, which Cloudflare Workers refuse during startup. Flowsafe's do-runner supplies that value before the entry loads, so Workers that build their pub/sub from `createHostPubSub()` start. A Worker whose bundle evaluates `@mastra/core/events` before Flowsafe's do-runner fails at startup with `Disallowed operation called within global scope`, for example a Worker importing `CachingPubSub` or `withCaching` from that entry, since import sorters order `@mastra/*` before `@proofoftech/*`. On core 1.73.0, such a Worker should not import `@mastra/core/events` itself.

  Storage initialization (`init()`) automatically adds `ownerId` and `leaseExpiresAt` to `mastra_background_tasks` with additive `ALTER TABLE … ADD COLUMN` statements. No manual migration is needed. Mastra's background-task manager claims and renews task leases, skips live leases during recovery, reclaims running tasks with expired or absent leases, and clears leases on suspension. Flowsafe continues applying the `resourceId` filter omitted by the D1 adapter's `listTasks`.

  Guarded agents inherit Breakwater's [input policy coverage](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#input-policy-coverage), [input policies and memory](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#input-policies-and-memory), [application processor rules](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#application-processors), and [streaming rules](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#understand-streaming-hold-back).

  Migration: Upgrade `@mastra/core` to `1.73.0`, `@mastra/cloudflare-d1` to `1.4.0`, and `@proofoftech/breakwater` to `>=0.17.0 <1.0.0` when used. Remove call-level `errorProcessors` from guarded durable calls. Build the bus with `createHostPubSub()` and do not import `@mastra/core/events` in Worker code on core 1.73.0.

- 3d5bd5b: A threaded schedule fire that the thread refuses permanently is recorded as `failed` with the refusal's message, and the schedule keeps firing. The trigger and audit carry the reason `dispatch-refused`. This covers an invalid dispatch or host input, an agent that does not allow the scheduled automation, and no matching thread binding or claimed dispatch. A 403 from a host automation policy fails that fire, not the schedule. Conflicts, server errors and other failures keep being retried. Fires already waiting in `deferred` close the same way on their next retry.

  A permission-requiring agent whose `resolvePrincipalPermissions` throws, rejects or returns malformed output refuses the entry with 503 `permission resolution unavailable` instead of 403 `forbidden`. Automated callers, including the schedule tick, retry it. Human callers of agent starts receive this status too. A missing resolver or unsatisfied permissions still answer 403.

  A fire refused because its thread binding does not exist yet is lost and is visible in trigger history.

  Signal routes answer a request the thread refuses as malformed, including host input refusals, with 400 `{ "error": "bad request" }` instead of 502 `{ "error": "internal error" }`. Public signal callers receive this response too.

  In `D1SchedulesStorage`, permanently refused fires no longer hold schedule deletion open while they wait for retries. A custom store decides its own deletion handling.

  Migration: The required `failDeferredTrigger` member on `ScheduleTickStore` is a breaking interface change. If you supply a custom store, implement this signature:

  ```typescript
  failDeferredTrigger(
    id: string,
    scheduleId: string,
    error: string,
    metadata: Record<string, unknown>,
  ): Promise<boolean>;
  ```

  Merge `metadata` into the stored metadata, as `touchDeferredTrigger` does. Record the failure only while the trigger is still `deferred`, and resolve `true` when a row changes or `false` otherwise. Leave a trigger whose target already settled a receipt, or that another tick resolved, unchanged.

  A custom `signalAgent` seam must throw an error with the thread route's status in a numeric `status` property, as `ScheduleTickSignalAgent` documents. An adapter that reaches the route through another hop must not report that hop's own status as a refusal. A seam that throws without a status keeps every refusal retrying.

- 4d0fe16: `FencedWorkflowsStorageD1` refuses to overwrite a settled run row. A row is settled once its run lifecycle records a terminal transition (terminate or timeout), an interruption, or a start-recovery repair. A workflow snapshot write over such a row lands only when it advances the lifecycle revision and keeps those records; any other write throws the new `RunSettledConflictError`, which the run routes answer with 409. This fixes a defect in earlier releases. When a deploy left a run's leg executing on the outgoing Durable Object instance, a terminate or timeout on the new instance did not stop that leg: it went on running the run's remaining steps, with their external effects, and its writes replaced the terminal record. The cancelled or timed-out run could end `success`, `failed`, `running` or `suspended`, and a run left `suspended` could be resumed after its cancellation. Now the leg's next write is refused and the leg stops there.

  The guard applies on a D1 binding with `batch()`, where unscoped persistence is now one conditional upsert that writes the same columns as `@mastra/cloudflare-d1`. Standalone client and REST configurations keep the adapter's write and have no guard. Code that rewrites a settled row through `persistWorkflowSnapshot` without advancing its lifecycle revision is refused. Legs running a flowsafe version without the guard still overwrite: during this upgrade, after a rollback, and in a mixed-version deploy. A refused leg stops at its next write; a workflow whose `shouldPersistSnapshot` skips `running` writes nothing between steps, so its steps run until that write. A durable agent persists nothing between tool calls by default, so a terminated agent's leg on an outgoing instance keeps calling tools until it suspends or finishes, and that write is refused. The agent thread host's start recovery does not stamp its repair, so a durable-agent leg that outlives it still overwrites it.

  A `DurableObjectRunner` start whose leg stopped before its first write is now repaired as `StartOutcomeUnknown` only once its row has gone six minutes without a write, about six to seven minutes after it stopped instead of about one minute, unless the start route sees its own leg fail in the object. The repair stamps the run lifecycle so the guard refuses that leg's later writes; the run's summary is unchanged. `InitialTerminalizationRequest` gains an optional `markOutcomeUnknown`.

- 65f7715: A `DurableObjectRunner` workflow step can wait without an approval. It suspends with `SUSPENSION_TIMER_PAYLOAD_KEY` (`'flowsafe.timer'`, exported from `@proofoftech/flowsafe/do-runner/constants` and `@proofoftech/flowsafe/do-runner`) set to `true` beside its `flowsafe.deadlineMs`, and the run's Durable Object resumes it with the timeout envelope when the deadline expires. A run that must outlast one Durable Object invocation waits this way between steps.

  The run object lists such steps in the new `RunSummary.suspensionTimers` on its start, resume, status, dispatch-status and protected-replay responses. It lists a step only when the current suspension derives an armable deadline for it, the marker is exactly `true`, the entry has not been abandoned, and an alarm is scheduled. `queueApprovalForSuspension` files nothing for a listed step, and `reconcileApprovalsForSummary` supersedes its stale open records without filing a new one. Every other case files an ordinary approval, so a timer step accepts an approval decision's `{ approved, comment?, decidedBy? }` as well as the timeout envelope and re-checks its wait condition on every resume. During a rollout, a timer step gets an ordinary approval wherever a Worker or run object without this change handles the run. Reconciliation supersedes that record once the step has suspended again.

  The run object files the approvals of a run it leaves suspended with no host request behind it, from its alarm: after a timeout resume (a timer that waits again, the gate after it, or a deadline gate that suspends again, whose expired approval is superseded and filed afresh, notifying reviewers), after it abandons a timer, after its alarm recovers a start, and after it finds a stopped leg's run suspended. `DurableObjectRunLifecycleHooks` gains an optional `reconcileApprovals(workflowId, summary)`, which `createFlowsafeRunnerLifecycle()` fills with `reconcileApprovalsForSummary`. It is best effort; a failure is logged as `reconcile-error`, and a host status read with approval reconciliation, as `createFlowsafeWorker` does, files what it missed. Hooks without the member, and a timer abandoned after 24 hours of unreadable run state, leave those approvals to that host read. An approval open for a suspension that a timeout resume ended stays open.

  A suspension-deadline resume now keeps the run's recorded `requestedBy` and `requestedByKind` instead of recording `SUSPENSION_DEADLINE_PRINCIPAL_ID` with kind `system`, so separation of duties applies to the gate after a timeout as it would without the timeout. A legacy requester without a kind is kept with kind `human`. The reserved principal is recorded only for a run without a requester, and a run whose recorded requester is already the reserved principal keeps it. A step that recognized a timeout resume by that principal id should test its resume data with `isSuspensionTimeoutResumeData` instead; a `RunSummary` no longer shows that a timeout advanced the run.

  A step declaring a Zod `suspendSchema` must declare `flowsafe.timer` as well as `flowsafe.deadlineMs`, or use a loose object; a schema that strips the marker files an approval on every wait.

  `DurableKeyValueStorage` gains an optional `getAlarm`. A storage without it lists no timer steps, so their approvals are filed.

### Patch Changes

- 5b467f2: A registered agent start whose caller Breakwater's RBAC gate refuses during preparation is refused with status 403 before the runtime starts and creates no run. An automated fire the agent's RBAC gate refuses now records `failed` instead of a run that did nothing. A created signal the gate refuses for a missing actor is preserved even when Mastra's isolate-wide run registry evicts the start's entry.

  Security: Before this change, if Mastra's isolate-wide run registry evicted the start's entry between preparation and the first step, the loop called the model for the refused caller on the thread's history and returned its output.

  No migration is needed.

- 3b5b0ad: A guarded `resumeViaRuntime()` whose memory does not resolve is refused before registry installation, observation, or the resumed step. The run stays suspended and can be redriven once memory resolves. A failure while Breakwater resolves the memory processors, including a memory processor lookup error or function-valued memory that resolves a title-enabled `Memory` there, stops the audited `breakwater-memory` step with `input processor failed` and one `agent.input.processor` error event; the resume rejects with `Durable agent registry rehydration denied: input processor failed`. If durable preparation's first memory lookup throws or resolves a title-enabled `Memory`, the resume rejects with that error, including the title `TypeError`, and writes no audit event.

  Security: Before this change, such a resume ran the approved tool and failed only at the next model step. A leg with no further model step finished and saved through the memory.

  No migration is needed.

- 5b467f2: A run resumed through `resumeViaRuntime()` that suspends at another approval keeps blocking its thread until that approval is decided, even while an output processor has not processed or drops the approval. Under the default cache, the resumed observer starts from the current stream position without replaying earlier legs' events. Earlier resumed legs' thread registrations complete when the run ends or a resume fails after rehydration; the wrapper then publishes a terminal error.

  Availability: Before this change, the thread was released while the run awaited approval. Signals to the thread were refused as blocked, or a host without a durable blocking check could start a second run.

  No migration is needed.

- aa48c2c: When you set `canPersistSchedule`, its answer governs persistence for every threaded schedule fire in place of `canPersist`'s answer. This includes a wake that falls back to persistence during a deployment drain, under a run cap, or without a start seam. Delivered signals whose schedule persistence is denied carry the persistence-forbidden marker, preventing the runner from persisting them after the run ends. For those fires, `canPersist` is not called, so a throwing `canPersist` no longer fails them and hosts see fewer `canPersist` calls.

  An authorized schedule wake with memory available persists, instead of being discarded, when a drain keeps it from starting a run. A fire that `canPersistSchedule` refuses is never persisted through a wake fallback, even when `canPersist` allows the system principal.

  A throwing `canPersistSchedule` fails every fire on which it is consulted, including wake and deliver targets, and the schedule tick retries the fire.

- 049971f: Message, signal, notification-dispatch, and schedule routes now re-read the thread's blocking run when they wake. If that run ends while the route checks authorization or content, the idle thread wakes instead of answering `blocked`. A schedule fire no longer settles a permanent `blocked`/`skipped` receipt in that case.

  This matters for hosts whose `resolveBlockingRun` answer can change while the route holds `serializeDispatch`, such as a host whose start returns while the run keeps executing. Hosts that execute a run's steps under the dispatch lock see no change. Each wake that found a blocking run makes one more `resolveBlockingRun` call, per send in a notification dispatch batch. No migration is required.

## 0.23.0

### Minor Changes

- 23eee3f: `createFlowsafeDurableAgent()` fixes the wrapper's agent-level pub/sub at construction to `pubsub ?? runtime.pubsub`, or its own stream bus when both are absent. HTTP-started threaded runs register on the thread Durable Object's pub/sub from their first start. Co-located thread Durable Objects keep separate active-run state, signals sent into those runs are drained by them, and each run publishes one stream to the thread's pub/sub topic from its first start. A `threadRuntime` object passed to the wrapper receives `registerRun` only for runs resumed through `resumeViaRuntime()`; core registers started runs. The constructor throws a `TypeError` when the wrapped agent has a different pub/sub of its own.

  `FlowsafeDurableAgent` refuses `__setMemory()`, `__setPubSub()`, `abortRunStream()`, `abortThreadStream()` and `discoverThreadPeers()`.

  Security: a controller's service installation reached the wrapped agent before the Mastra refusal. Direct aborts bypassed the terminate route's ownership and disputed-settlement checks and could reach another thread's run in the isolate. Peer discovery returned every advertised thread identity on the pub/sub.

  Migration: the signal routes set pub/sub only on agents that are not runtime-driven and answer `503` when a runtime-driven agent's pub/sub is missing or differs from the thread's. Construct the wrapper with the thread Durable Object's pub/sub, as the thread host does, and use a fresh wrapped agent for each thread Durable Object. Configure memory and pub/sub at construction. Cancel runs through the terminate route; the stream result's `abort()` still works.

- 850f95b: The thread host refuses durable starts containing caller system messages or call-level provider options rejected by Breakwater's `assertAcceptedCallProviderOptions()` with `400`, before authorization or any write. A threadless fire whose stored options are refused records `failed`. The optional `scheduleProviderOptionsPolicy` runs on every threaded schedule fire before signal creation; a denial settles a discard, while an error leaves the lease for a later tick. The optional Breakwater peer minimum rises to `0.16.0`, which supplies these checks.

  Security: before this release, a caller `role: 'system'` message passed to the thread host's durable start, and a stored schedule's call-level provider options reached the model outside the guarded agent's input policies.

  Migration: raise `@proofoftech/breakwater` to `0.16.0`. Move per-agent provider options from schedules and topology starts to the agent's model entry. Wire `scheduleProviderOptionsPolicy` with both `providerOptionsCarryContent()` and `assertAcceptedCallProviderOptions()` so threaded fires are refused when either check rejects their stored options.

- ad61e7a: `createThreadSignalRoutes()` accepts an optional `notificationDispatchAllowed` callback for non-owner notification ingestion. `ThreadAgentHost` gains a required `notificationDispatchAllowed()` member that checks the target agent's catalog declaration for `system` on `notification.dispatch`; hosts can wire it to the router callback. A refused non-owner notification receives `409` with `notification-dispatch-forbidden` before an inbox row is created. This is BREAKING for hand-built `ThreadAgentHost` implementations; hosts from `createThreadAgentHost()` get the member automatically, hence the minor bump.
- ccffeca: `FlowsafeDurableAgent` now refuses `setChannels()`, `__setDeclaredSchedules()`, `__setMastra()` and `__registerMastra()`, so the wrapper cannot be registered on a Mastra: `Mastra.addAgent()` and constructing a `Mastra` or an `MCPServer` with the wrapper in `agents` throw; an `AgentController` throws during service installation when configured with memory or pub/sub that the wrapped agent does not own, and otherwise throws when `init()` registers the wrapper on its Mastra through `__setMastra()`. Its constructor throws a `TypeError`, and the guarded agent catalog refuses the module, when the wrapped agent has channels configured, declares schedules, sets the `durable` option, or is already a durable agent. `RunnerRuntime` refuses a workflow object that another Mastra has registered, including one registered while a start, resume or start recovery is in progress, up to the point the runtime creates the run.

  Security: channels on the wrapped agent, or bound through the wrapper, dispatched inbound messages and tool approvals to the wrapped agent outside `RunnerRuntime`, and a Mastra schedule worker fired its declared schedules the same way. A wrapped agent with the `durable` option, or a Mastra `DurableAgent` passed as the wrapped agent, was registered on the runtime's Mastra as a durable agent whose recovery and run listing the runner does not guard. Registering the wrapper on a Mastra bound every later leg to that Mastra, and once the runtime had built its own Mastra it also repointed the runtime's loop workflow to the other Mastra's storage, where `status()` read a stored run as absent. A runtime workflow added to another Mastra did the same.

  Migration: pass `createFlowsafeDurableAgent()` and the agent catalog a plain `Agent` without `channels`, declared schedules or the `durable` option. Do not register the wrapper on a Mastra, pass it to an `AgentController` or an `MCPServer`, or configure it as a sub-agent: every run of a parent agent registered on a Mastra fails when the parent converts its tools. Register each workflow object on one runtime, and do not add a runtime's workflows, or the wrapper's `getWorkflow()`, to another Mastra.

### Patch Changes

- 68c4c61: Signal routes, agent-host start and resume routes, and the runner's start route read the request body before waiting for the thread's lock, so a request whose sender disconnects while it waits is still processed.
- 84bb66a: An owner `/signal/notification` to a runtime-driven agent is recorded in the notifications inbox and delivered at ingestion under the owner's principal: into the owner's running run, or persisted to agent memory when the thread is idle or the owner's run is not running. It never wakes a run, and no dispatch tick selects its row. The response is `{ record, delivery }`: the settled `NotificationRecord`, and Mastra's delivery decision with the delivered `signalId`. A `FlowsafeDurableAgent` registered on no Mastra uses this route-owned delivery path because Mastra's `sendNotificationSignal()` requires a notifications store on the agent's own Mastra.

  Before writing anything, the route answers `409` with a `reason` when another principal's run holds the thread (`principal-mismatch`, with `retry: true`), when the delivery would need agent memory the agent lacks (`memory-unavailable`), or when a `dedupeKey` or `coalesceKey` matches a pending row the dispatch tick will deliver (`notification-pending`). After the write, the row is discarded when Mastra reports the thread blocked (`409`, `thread-blocked`), when the content policy denies the stored row's signal (`422`) or fails (`503`), and when storage or the send fails before acceptance (`502`). A failed delivered receipt write after an accepted send is retried; if the retry fails, the route answers `502` and the row stays pending with no due time. A row whose isolate dies or whose settle write the execution fence refuses also stays pending with no due time; clear it with a direct write that sets it `discarded`, or delete it. A matching keyed non-owner notification receives `notification-pending` instead of merging into this residue.

  When a host passes no `canPersist`, every principal takes this owner path, signal providers included, and a provider treats a `409` as a permanent drop.

  A non-owner notification is still recorded for `createNotificationDispatchTick()`, which delivers each row individually as `system` on `notification.dispatch`; no Mastra delivery policy applies, neither the default nor a configured one. On an idle thread the tick wakes a run whose principal and approval requester is `system`: the self-approval bar does not cover the notification's author, the row stores no author, and the run's permission projection and audit are `system`'s. Idle starts are limited only by `consultRunCap` where the host wires it. The tick counts a failed attempt when the run cap refuses the wake, when another principal's run holds the thread, and when the agent does not declare `system` on `notification.dispatch` in `allowedAutomation`, and it discards the row after `maxDeliveryAttempts` failed attempts.

  An agent without the runtime-driven brand keeps Mastra's delivery path for owner notifications and its `degraded: 'not-runtime-driven'` marker. When its own Mastra has no notifications store, the route answers `409` before calling Mastra's sender.

  `BlockingAgentRun` and the `resolveBlockingRun` result of `createThreadSignalRoutes()` take an optional `status`, which the thread host fills from the run's stored status.

  Migration: a host that accepts owner notifications for a runtime-driven agent must pass `resolveNotificationsStorage` to `createThreadSignalRoutes()`, or the route returns `409`. A host with its own `resolveBlockingRun` reports each run's `status`, so that an owner notification is not queued into a run that is not executing; a run with no status is treated as running. A caller that read an owner response's `record.record` or `record.decision` reads the settled record from `record` and the delivery from `delivery`.

- 52cc784: A run resumed through `resumeViaRuntime()`, as an approval decision resumes one, registers with Mastra's thread runtime in a form whose `status` Mastra can read. Before this release, reading it threw `Cannot read private member #status`: Mastra's completion check failed after every threaded resume and never released the finished run's registration, and on a thread host built with `cache: false`, or with a `CachingPubSub` as its pub/sub, later signals, messages and schedule fires to that thread answered `502`.
- cf7ad72: `/signal` answers 409 when a thread has no wired resource, matching the sibling routes. Signals and messages sent into suspended runs persist to thread memory instead of remaining in the isolate, including wakes, notification dispatch, and agent-schedule fires.
- 20cabc3: `FlowsafeDurableAgent` preserves the input of a run that the host start seam did not register by the guarded input chain's verdict, before it refuses the run. Such a run starts when Mastra drains a signal still queued after the run it was delivered to completes, or when a caller streams into the agent directly.

  - When no input processor refused the run, its prepared input is saved.
  - When the run's input is a created signal, the call carries neither a host ticket nor a request context, and Breakwater's RBAC gate refuses it, the signal is saved as received. That is the drain after a run resumed through `resumeViaRuntime()`, whose options carry no request context: the gate refuses it for its missing actor without reading content, and on a guarded agent before any application input processor runs. Breakwater removes a refused call's input from Mastra's prepared message list, so the signal would otherwise be lost. Host code that calls `stream()` the same way, with a created signal or an object carrying the signal brand as input, has that input saved when the gate refuses it.
  - After any other refusal nothing is saved.
  - A transient signal is never saved.

  Security: the input of an unregistered run that an input policy or processor refused was saved to the thread, where later calls load it as history without the input policies reading it.

## 0.22.0

### Minor Changes

- 36c60c8: Require `@mastra/core` 1.67.0 exactly (previously 1.53.0). The peer is exact, so every consumer must move to 1.67.0 as well; this is breaking for consumers pinned to 1.53.0. 1.67.0 bundles for Cloudflare Workers and Vite again — mastra-ai/mastra#20638, the dynamic-import regression that held the pin at 1.53.0, closed upstream.

  `@mastra/cloudflare-d1` moves from 1.1.1 to 1.3.2, whose own peer requires a core newer than 1.53.0. FlowSafe's `@proofoftech/breakwater` peer floor rises to `>=0.15.0 <1.0.0` in step, that being the first Breakwater release built against the same core.

  The `@mastra/core` patch FlowSafe shipped under `patches/` is retired: 1.67.0 carries both fixes upstream (mastra-ai/mastra#23693, mastra-ai/mastra#23694). The patch file is gone from the published package, and so are the two refusals that required it — FlowSafe no longer refuses to construct a delivering notification dispatch tick, nor notification ingestion and dispatch requests, on an install whose core lacks the patch. Consequence for anyone running a core outside the declared peer without having applied the patch: a notification `source` named after an `Object.prototype` member is miscounted in the summary core renders, and its source delivery policy resolves the inherited member instead of the configured priority or default action. That configuration was unsupported before and remains so, but it now fails silently rather than loudly. Application roots that copied the patch into their own `patches/` should drop it along with the `patchedDependencies` entry or `postinstall` script that applied it.

  `createD1Storage({ domains })` composes the two storage domains 1.67.0 adds, `workflowDefinitions` and `knowledge`, through the same override seam as the other domains. `@mastra/cloudflare-d1` backs neither, so each resolves `undefined` unless a host supplies one through that seam.

  The workspace lockfile behind this release was resolved once with pnpm's seven-day minimum-release-age gate overridden for that resolution only. Five newly resolved versions were younger than the gate at resolution on 2026-09-19: `@mastra/core` 1.67.0 and `@mastra/schema-compat` 1.3.10 (published 2026-09-15), which the workspace's standing `@mastra/*` exclude admits with the gate on, and `posthog-node` 5.52.4 (2026-09-15), `@posthog/types` 1.412.2 (2026-09-17) and `@posthog/core` 1.55.0 (2026-09-18), which the override alone admitted. None of the five declares a lifecycle script or ships a native component; all five carry npm provenance attestations (SLSA v1, published from GitHub Actions); and `posthog-node`, with `@posthog/core` and `@posthog/types` beneath it, is a hard dependency of `@mastra/core` that ships inside a bundled Worker.

  The private `showcase` and `anchorage-agent-starter` packages move to the same core.

### Patch Changes

- 36cfdc2: Keep `flowsafe-provision` inert when imported from stdin, eval or print programs, and virtual workers, even when an entry name aliases the script. Direct execution, file and directory symlinks, and file workers under eval parents continue to invoke the CLI.

## 0.21.0

### Minor Changes

- c8c5039: The pending-notifications inventory lists pending agent-inbox notifications whether due, scheduled for later, or carrying no due timestamp. Its count includes these rows, its notDue total identifies those not yet due, and entry details expose summaryAt alongside deliverAt.

  Pending notifications keep the deployment drain proof open until delivered, discarded, or deleted. Rescheduling alone does not clear the proof; a pending notification with neither timestamp requires a direct write or deletion because dispatch and retention leave it pending.

- 4cb59a1: Bound notification delivery to ten failed attempts by default, configurable through `maxDeliveryAttempts` on tick and thread-route factories. Discard exhausted rows before another send, retain their error/count receipts, and remove them from due scans while preserving retry delays below the bound.

  Require conditional failure writes through `NotificationDeliveryStorage` for dispatch. `D1NotificationsStorage` implements the atomic operation; custom stores must adopt it. Preserve newer summary, delivery and content-denial receipts after response loss, and count each local outcome once without inferring unconfirmed success. Ordinary Core storage still serves notification ingestion; the `@mastra/core` patch is required for either path. Require a custom `SignalDatabase` or `ScheduleDatabase` `batch()` to resolve elements carrying `results`, as a real `D1Result` does; `SnapshotDatabase` and `InitialAdmissionDatabase` `batch()` declare the same element.

  Compare notification dates chronologically before bounded selection and retention, including expanded years and numeric offsets. Direct database writers must use ISO dates or explicitly zoned ISO date-times; conditional failure writes reject raw timestamp text outside that grammar, and neither due selection nor retention matches such a value.

  Ship the `@mastra/core@1.53.0` patch under `patches/`. Application roots must apply it for own-property-safe summary source counts and source delivery policies; `@mastra/core@1.53.0` otherwise reads inherited `Object.prototype` members at both sites (mastra-ai/mastra#23693, mastra-ai/mastra#23694). The getting-started guide documents the pnpm, npm and Yarn routes. Flowsafe refuses to construct a notification dispatch tick that does delivery work, and refuses notification ingestion and dispatch requests, when the installed `@mastra/core` lacks the patch.

- f05e598: Refuse two `Agent` entry points that `@mastra/core` releases newer than the declared peer expose. `FlowsafeDurableAgent.listActiveThreadRuns()` throws instead of returning the run, thread and resource ids of every thread on the pubsub instance with a run in flight, which Core scopes by neither principal nor agent. `FlowsafeDurableAgent.__setThreadRuntimeAgent()` throws instead of installing another agent as the target the thread-runtime paths resolve through — on `@mastra/core` 1.67.0 these are `subscribeToThread()`, `claimThreadOwnership()`, `sendMessage()`, `queueMessage()`, `sendStateSignal()`, `sendNotificationSignal()` and `sendSignal()` — where one call would move every run those paths start, and `subscribeToThread()`'s replay target with them, onto an agent that carries none of the wrapper's guards. Both refusals carry the reason table's message; an installed 1.53.0 exposes neither member on Core, so no call that resolves today changes, and a caller that feature-detects either member now finds it on the wrapper and takes the refusal where the call was a `TypeError` before.

  The durable-agent surface inventory now holds against the pinned peer and against newer 1.x releases together.

- 6f54bc6: Require the retention cursor seam on the purge duty. `runMaintenanceDuty('purge', env, context)` takes the new `MaintenancePurgeDutyContext`, whose `advanceRetentionCursor` is required, matching the `advanceCursor` the run-retention purge itself requires; the other duties keep the optional `MaintenanceDutyContext`. `FlowsafeWorker.runMaintenanceDuty` declares that split as two overloads — `'purge'` with a required `MaintenancePurgeDutyContext`, and `Exclude<MaintenanceDuty, 'purge'>` with the optional `MaintenanceDutyContext` — so a caller holding a union-typed `duty` narrows it to one branch before calling: a single call spanning the whole union matches neither overload and no longer compiles. A purge invocation whose context omits the callback is refused under a `config-error` naming `maintenance.purge.advanceRetentionCursor` before any purge surface runs, rather than purging the remaining surfaces and reporting a `retention-purge` failure.
- 6f54bc6: Capture the conditional-delivery capability when a notification dispatch tick is built, whatever its `limit`. `createNotificationDispatchTick()` reads `storage` and refuses one without `updateNotificationDeliveryIfUnchanged` for every configuration, including `limit: 0`, so the `NotificationDeliveryStorage` requirement no longer depends on the limit. A `limit: 0` tick still resolves `{ due: 0, delivered: 0, failed: 0 }` without reading due rows, calling storage, or needing the `@mastra/core` patch; invalid numeric policy still fails ahead of the capture.

  The notification ingestion route refuses when the installed `@mastra/core` lacks the delivery-policy half of that patch. `createThreadSignalRoutes()` probes `resolveNotificationDeliveryDecision` at the ingestion gate, where delivery runs through `agent.sendNotificationSignal` and reaches the delivery-policy lookup, and answers 502 with the message naming the patch on the server log. The dispatch route carries the synchronous source-key probe its summaries need, and delivers through `agent.sendSignal`, which never reaches that lookup. The probe resolves once per isolate; tick construction keeps its synchronous probe and is unaffected.

- 647092e: Expose `isArmableSuspensionDeadlineMs` and `suspensionDeadlinesOf` from `do-runner`, together with the `SuspensionDeadlineEntry` and `RejectedSuspensionDeadline` types that projection returns. Add the lightweight `do-runner/constants` entry for deadline values and timeout detection, and `do-runner/testing` for constructing fixtures with the same timeout envelope as the alarm path.

  Reuse the existing arming bounds, derivation and alarm payload factory. The test helper does not authorize a resume or mint an approval grant.

- e79b92a: Accept optional non-reserved `requestContext` on authenticated run starts and carry it through the protected Durable Object topology into stored application context. Expose the validated value to router and Worker start-policy hooks while preserving shorter hook signatures.

  Reject malformed context and reserved keys with HTTP 400. Verified schedule targets retain precedence, including absent context; provider application values override stored values and trusted capabilities retain their authority. Application context survives resume. Keyed replay validates input and runs host policy again, then preserves the first writer's context without comparing or overwriting it.

  Correct public agent status, stream and ordinary termination lookups to return not found for a coherent snapshot belonging to another agent or thread. Private replay, proof and recovery retain strict failures so a foreign snapshot cannot be treated as absent.

- 6bd8bfc: Add versioned execution-fence administration with artifact epochs, a sticky epoch requirement, transition revisions and exact last-command retry receipts. Upgrade supported legacy schemas additively without changing existing state or timestamps. Missing rows in new-format schemas fail closed. Admin responses omit receipts, proof execution identity and tokens; legacy commands remain compatible only while the epoch requirement is optional.

  Activate v2 Runtime generations with independently generated execution tokens and preserved original principal, logical target, caller epoch and agent mode across resume legs. Fenced starts require the actual D1 domain's positive initial-write witness before engine entry, binding the winning reservation and proof in the same admission transaction. Capable D1 without a fence keeps its actual namespace and ordinary persistence options; custom storage explicitly asserts no D1 namespace. Unfenced keyed starts bind their prepared identity before creation and retain uncertain outcomes.

  Persist preparing, prepared and prepared-unfenced journals in both managed hosts. Recover exact owned initial generations through the existing raw-row conditional repair without replaying effects or deleting tokenless snapshots. Require strict terminal reservation settlement before approval, dispatch, owner and lifecycle cleanup, then clear only the matching journal. Cold agent alarms initialize actual wrappers with the verified instance scope. Legacy journals and uncertain unfenced pending/absent outcomes remain unresolved.

  Keep the thread blocking-run check and run-record installation under the same lock. Validate workflow journals against the owning object's address and recheck complete agent journals after recovery waits before releasing reservations. Keyed recovery requires its configured reservation store before bookkeeping, including nonterminal outcomes.

  Share cold agent-wrapper initialization across concurrent requests. Probe the owning execution's liveness before reclaiming an existing reserved key, so a stream awaiting Core cleanup keeps retries pending without stranding the key. Unreadable liveness replies refuse the retry instead of authorizing a claim.

  Require a matching nonpending durable observation before modern start/resume success or the agent persistence acknowledgement. Return `RUN_START_PENDING` for a valid initial generation, preserving journals, watchdogs and pending schedule/deadline budgets. Project root-local summaries from the same selected observation, retaining detailed nested resume preparation and legacy compatibility.

  Preserve valid v1 and absent-provenance ordinary status, resume and lifecycle completion through one authoritative observation. Apply the existing binding, canonical-record and principal checks to legacy status. Legacy terminal cleanup requires no recovery journal and a confirmed raw terminal outcome; it never manufactures generation identity or spends a start key. Normal termination retains canonical agent records until lifecycle completion confirms.

  Create modern-unbound reservations and replace run-only claim, release and settlement with exact observed-row operations. Claim/release stamps advance without serving as generation tokens; only the caller's own valid write result proves a winning claim. Private replay compares the full generation before pending/result classification and preserves the value from its one authoritative read. Alias binding and prepared binding retain their distinct response-loss rules. Remove `claim`, `release`, `settleRun` and `rollbackFencedStart`; custom router wiring must provide the private `persistedStart` callback.

  Guard replay proof nomination with its original proof round/caller epoch, current exact snapshot and bound reservation at the final SQL write. Legacy proof setters cannot overwrite modern identity. Runtime, workflow-host, approval and signal re-entry gates compare complete physical generations and retain their original expectation through relevant waits. Direct approval compositions now pass an explicit trusted workflow namespace. External effects are not transactional with these checks.

  Capture trusted authority before asynchronous work across Worker configuration, protected JSON/header transport and the eighth agent-start authority argument. Keep public bodies and application context from supplying a winning claim. Preserve source-owner versus initiating-principal attribution, exact lifecycle counter exhaustion checks and rejection of sparse economic-operation lists.

  Make workflow retention generation-aware, protect run owners across supported snapshot namespaces, and pair reservation cleanup with the complete bound execution. Preserve reserved owners, uncertain generations and legacy keys that cannot be safely associated. Recheck schema and selected identity at the mutation; keep artifact deletion ahead of D1 cleanup.

  Custom `purgeExpiredWorkflowRuns` callers must now provide transactional `database.batch()` and an `advanceCursor` callback, retain its exported `RunRetentionCursor`, and supply that cursor on the next call. Composed maintenance persists the cursor across alarms and restarts. Finite scan cycles revisit skipped candidates without letting continuous inserts extend the current cycle; unproved D1 outcomes and failed cursor writes do not advance progress.

  Enforce captured caller epochs and semantic fence/schema observations in final D1 schedule mutations. Guard owned deletion participants independently, preserve admitted trigger settlement, and provide fixed pause/resume methods plus guarded no-op observations. Resume rejects a concurrent cron/timezone change. Fenced custom facades require the same-binding `FENCED_SCHEDULE_STORAGE` capability before activation; direct D1 authoring requires `batch()` and refuses omitted epochs once enforcement is active. Publish structured schedule conflict and unknown-outcome errors, retain server-side causes, and contain route audit failures without changing the selected response.

  Apply the shared final schema, singleton and typed semantic fence check to Runtime initial admission. Refuse unreadable authority before snapshot or dependent reservation/proof writes while preserving exact committed-write recovery.

- 323c2ce: Add optional `SignalRouterOptions.validateThreadTarget` using the existing bound-thread validator contract, with captured actor context before asynchronous validation. Hosts can enforce strict ownership before forwarding. Normalize thread refusals with status 404 and audit the final downstream result.

  Contain audit and diagnostic failures in signal, objective, subscription and webhook routes so they preserve the selected response. Use own-property lookup for signal channels, objective methods and webhook provider configuration.

### Patch Changes

- a027f13: Return a generic internal-error response for unexpected run-router failures while retaining the original error in server diagnostics. Preserve typed refusal status, message and reason contracts. Contain diagnostic conversion and logging failures so they cannot prevent the generic HTTP response.
- a086f24: `flowsafe-provision` sets a 64 MiB `maxBuffer` on the `wrangler d1 execute --json` child process whose output it parses. Node's default is 1 MiB counted across the captured stdout and stderr together, so a response past that was truncated and the run surfaced as `failed to execute Wrangler 4` with an `ENOBUFS` cause instead of the parsed rows.
- 8c43533: Support signed maintenance for platform-authored Workers for Platforms catalogs. Catalog signing profiles can supply the maintenance keys without external-state artifacts. Catalog uploads receive their public verifier and local identity; FlowSafe relays capabilities while retaining the local receipt secret and validates the catalog script and digest before maintenance work.

  Existing catalog artifacts need a rebuilt FlowSafe runtime and explicit maintenance enrollment. The host configures the matching global dispatcher verifier.

  Persist catalog ownership explicitly in Fleet records and preserve it through native D1 migration and export-backed teardown. Catalog cleanup checks its own script and namespace authority. Force re-entry on completed or reserved records uses claim-releasing deletion when the store supports it.

  Preserve the prior mutable Worker schema identity while D1 advances and retain migration authority through compatibility teardown retries. Permit declared catalog binding changes with exact owner and uploaded-target checks.

  Allow ordinary spec-free force recovery after a candidate upload by clearing migration-only scalar fields when teardown begins, while preserving the recorded resource identity.

## 0.20.0

### Minor Changes

- 1212ba5: Flowsafe now exposes deployment-wide execution fencing, owner-bound idempotent starts, and a read-only drain inventory for physical deployment migrations.

  This changes the public runtime and host contract:

  - The execution fence has `open`, `draining`, `migration-locked`, and `proof-only` states. Transitions use compare-and-set through `POST /admin/execution-fence`; a stale expected state returns `409` with `FENCE_CAS_CONFLICT`, and invalid state or proof-key values return `400` with `INVALID_EXECUTION_FENCE_REQUEST`. `GET /admin/execution-fence` reads the current state. Both routes require `MAINTENANCE_ADMIN_SECRET`.
  - Fence refusals return `503` with `EXECUTION_FENCED`. `open` admits every entry. `draining` refuses new run mints and future-work authoring, but admits work on existing runs, dispatch of queued background tasks, and new background-task enqueues. `migration-locked` refuses every execution entry, including background-task enqueue, dispatch, and stale-task re-drive. `proof-only` admits only the start carrying the nominated proof key and later work addressed to its bound proof run; it also refuses background-task enqueue, dispatch, and stale-task re-drive. Reads, termination, cancellation, timeout, objective clear, and schedule pause or delete remain available. In-flight compute is not preempted.
  - The new `flowsafe_execution_fence` table stores one deployment-wide row. The deployment-identity protocol treats it as protocol-owned state during ownership checks. A database created before this release reads an absent table or row as `open`.
  - **BREAKING:** `ExecutionFenceWiring` is required on the non-`'none'` `RunRouterStartIdempotency` arm of `RunRouterOptions.startIdempotency`, and on `HostApprovalServiceOptions`, `ScheduleTickOptions`, `ScheduleRouterOptions`, `SignalProviderHostWiring`, `WebhookRouterOptions`, `ApprovalServiceOptions`, `NotificationDispatchTickOptions`, `StorageInitOptions`, `AgentThreadTopologyOptions`, `ObjectiveRouterOptions`, and `BackgroundTaskHostOptions`.
  - **BREAKING:** `provisionDeploymentIdentityProtocol()`, `seedDeploymentIdentity()`, and the `flowsafe-provision --initial-fence-state` CLI require an initial state. Provisioning accepts only `open` or `migration-locked`; there is no default.
  - **BREAKING:** `BackgroundTaskHost.manager` is removed. Use `enqueue()`, `getTask()`, `listTasks()`, and `stream()`, and type read-only consumers with `BackgroundTaskReads`.
  - **BEHAVIOR CHANGE:** the agent-host and host-kit stream routers preserve structured Durable Object refusals. Clients now receive the original `503` or `409` and a `reason` body instead of a bare `500` for structured server-side refusals.
  - Refusal and denial message text no longer exposes internal plan identifiers. Reason codes and statuses are unchanged.
  - `idempotencyKey` makes `POST /runs`, trusted agent-host starts, and `streamUntilPersisted()` converge on the same server-minted run. Callers still cannot supply `runId` to `POST /runs`; doing so returns `400`.
  - Idempotent-start decisions return `IDEMPOTENT_START_OWNER_MISMATCH` (`403`), `IDEMPOTENT_START_TARGET_MISMATCH` (`409`), `IDEMPOTENT_START_PENDING` (`503`), `IDEMPOTENT_START_UNRESOLVABLE` (`409`), or `IDEMPOTENT_START_ALREADY_SETTLED` (`409`). Operational failures return `IDEMPOTENT_START_UNSUPPORTED` (`503`) or `IDEMPOTENT_START_UNREADABLE` (`503`), and malformed input returns `INVALID_START_IDEMPOTENCY_REQUEST` (`400`). `isStartReservationRefusal()` recognizes the five decision refusals, unsupported wiring, and malformed input; unreadable storage propagates separately.
  - A replay returns the original run's persisted state. `IDEMPOTENT_START_PENDING` includes `pendingSince`. `IDEMPOTENT_START_UNRESOLVABLE` is a point-in-time answer, so re-probe before acting. A replayed suspended start omits the start response's `approval` and `approvals` fields. The `flowsafe_start_idempotency` row remains valid for the configured reservation-retention horizon.
  - **BREAKING:** `RunRouterOptions`, `AgentThreadTopologyOptions`, and `StorageInitOptions` require `startIdempotency` wiring. `START_IDEMPOTENCY_RETENTION_DAYS` controls how long spent keys remain valid and defaults to run retention.
  - `GET /admin/inventory` reports work categories `runs`, `approvals-waiting`, `schedule-deferred-dispatches`, `pending-notifications`, `background-tasks`, `resource-owners`, and `start-reservations`, plus standing categories `schedules` and `signal-subscriptions`. Standing categories are never required to empty.
  - `INVENTORY_DRAIN_PROOF` requires every work category to be empty across two consecutive full sweeps at least one 60-second alarm cadence apart, while the fence remains `draining`. Inventory readings are point-in-time observations rather than snapshots and can move in either direction while draining admits work. Empty results cannot over-count, and keyset pagination never skips a row that existed before the sweep began. A host that needs a hard guarantee can re-sweep once after transitioning to `migration-locked`: an empty post-lock sweep is conclusive, while a non-empty one means work is still outstanding, either because it entered after the proof or because the lock parked it before it finished. Return to `draining` and repeat the proof. An inventory read taken under `migration-locked` measures what the fence parked rather than what the deployment would otherwise be doing. `INVENTORY_UNENUMERABLE` declares the run-owner recovery journal and persisted idle signals; the latter deliberately survive migration. `FLOWSAFE_TABLES` keeps every Flowsafe-owned table accounted for.

  When upgrading, pass `'none'` only for leaf hosts with no database, select `open` or `migration-locked` during provisioning, replace `host.manager` reads with the host methods, preserve structured `503` and `409` refusal bodies, and re-probe `IDEMPOTENT_START_UNRESOLVABLE` before deciding whether to use a fresh key.

## 0.19.0

### Minor Changes

- fa0d11d: Add an optional content-policy boundary for agent signals. Breakwater exposes `createContentPolicyGate()`, a reusable opaque input-policy gate for host code outside Mastra's processor chain, and FlowSafe's thread signal routes accept a structural `contentPolicy` callback that inspects Mastra's canonical escaped XML before delivery, persistence, wake, or run start — covering direct ingestion, providers, schedules, and notification dispatch. Denial is terminal and evaluator failure stays recoverable on every lane; neither exposes policy names, reasons, content, or causes.

  Signal attributes whose keys are not XML names are now dropped when a signal is ingested, and a schedule whose stored target cannot be rendered settles a terminal discard receipt instead of failing every later tick with the same broken target.

  Provider deliveries now distinguish a terminal refusal from one the deployment could not decide: an undecided webhook is answered with 503 so the sender redelivers, and every delivery carries a dedupe key derived from the signed bytes and the subscription so a redelivery coalesces into a still-pending notification instead of duplicating it. Webhook and poll results report `denied`, `failed`, and `deferred` counts.

- 0447466: Signal delivery through a Flowsafe durable agent no longer starts an unowned
  run below the host seam; an unbranded agent on an active thread keeps core's own
  behavior as a degraded configuration.

  This changes the public signal contract:

  - `/signal/queue` persists in both active and idle states. Success now returns
    `decision.action: 'persist'` without a `runId`; active-thread auto-drain is
    removed, so the message surfaces on the next host-started turn.
  - `/signal/state` now applies the queue route's owner gates and can return
    `principal-mismatch` or `persistence-forbidden`.
  - `/signal/notification` creates the notification record for every accepted
    provider delivery. Owners receive core's `{ record, decision, ... }` result
    under the top-level `record` field, with the signal-routing decision exposed
    separately as `delivery` when core returns one. Non-owners receive a flat
    `NotificationRecord` under `record` plus
    `delivery: { action: 'deferred', reason: 'dispatcher' }`; they never send a
    signal directly. Low-priority owner notifications use summarize-later and
    have no immediate `delivery`.
  - Unbranded agents return `degraded: 'not-runtime-driven'` from successful,
    non-skipped state and owner-notification responses, regardless of thread state.
    Skipped state and an early `memory-unavailable` state response carry no marker.
  - `/signal/message`, `/signal`, `/signal/schedule`, and
    `/signal/notifications/dispatch` now persist on a stale-active-id fall-through
    instead of waking. A forbidden fallback returns `persistence-forbidden`; a
    memory-less fallback returns `memory-unavailable`. The notification dispatch
    lane counts either discard as failed and performs no persisted write. A
    non-owner `/signal` request for `ifActive: 'persist'` degrades to `discard`
    for active delivery: when the thread was active, the response is
    `persistence-forbidden` without a `signalId` because the gate refused and
    nothing was delivered; when the thread was idle, the caller's own `ifIdle`
    outcome is returned unchanged with `signalId`. Owners still forward
    `persist`. Non-owner active deliveries carry non-rendered metadata so a
    completion drain cannot preserve a leftover through the terminal path.
  - Persist outcomes return a `memory-unavailable` discard decision when the
    resolved agent has no memory, after the content gate. A default or
    `ifIdle: 'persist'` message or signal is delivered into an active run without
    memory; an active persist that no memory could write answers
    `memory-unavailable`. A persist-behavior `/signal/schedule` fire instead
    settles a canonical `discard` receipt with `outcome: 'discarded'` and no
    reason, where it previously settled `persisted`.
    Owner `/signal/notification` is the other exception: its model-visible memory
    write is best-effort because the inbox record is already durable. The shipped
    starter host does not configure agent memory, so its other persist outcomes
    return `memory-unavailable` until the host adds memory configuration.
  - Non-owner `/signal/notification` ingestion now requires notification storage
    and returns `409` without it. Those rows bypass the agent's delivery policy and
    readiness hook; the host must run `createNotificationDispatchTick()` to
    deliver them. The starter runs it every 60 seconds, giving up to one tick of
    latency. A host without the tick records but never delivers them; the spike
    has no tick and its provider probes assert only the inbox row.
  - The durable-agent runner terminally fails every run that was not registered
    through `streamUntilPersisted()`. Direct `stream()` resolves to a failed
    output; direct `generate()` rejects. `stream()`, `generate()`, `prepare()`, and
    `streamUntilPersisted()` synchronously refuse a live id, and `prepare(X)`
    keeps `X` live until cleanup. `streamUntilPersisted()` also refuses
    `untilIdle`. If the runner's two terminal-publication attempts and core's own
    fire-and-forget attempt all fail, the output never closes and the thread stays
    active until eviction or a new host start.
  - The public `signals/router.ts` state and notification channels carry these new
    response shapes.

  Migrate run starts to the host routes or `streamUntilPersisted()`. Treat queue
  success as `{ action: 'persist' }` without a `runId`, and read queued messages on
  the next host-started turn.

- 8f4daae: Require `@mastra/core` 1.53.0 exactly (previously 1.50.0). The peer is exact, so every consumer must move to 1.53.0 as well; this is breaking for consumers pinned to 1.50.0. 1.53.0 is the newest release whose published output still bundles for Cloudflare Workers and Vite: 1.54.0 through 1.60.0 inline Node-only dynamic imports (`execa`, `@ast-grep/napi`) that fail to bundle (mastra-ai/mastra#20638). `@mastra/cloudflare-d1` stays at 1.1.1. FlowSafe's `@proofoftech/breakwater` peer floor rises to `>=0.13.0` in step, that being the first Breakwater release built against the same core.

  FlowSafe's durable agent runner now refuses every inherited entry point that can drive execution outside `RunnerRuntime`, mint a run id below the caller, or hand back runs the caller does not own: the run-recovery entry points 1.53.0 adds to `DurableAgent` (`recover`, `recoverActiveRuns`, `listActiveRuns`); the resume family (`resume`, `resumeStream`, `resumeGenerate`, `approveToolCall`, `declineToolCall`, `approveToolCallGenerate`, `declineToolCallGenerate`), which since 1.53.0 rehydrate from snapshot storage on a run-registry miss; the agent-level discovery member `listSuspendedRuns`; the network family (`network`, `resumeNetwork`, `approveNetworkToolCall`, `declineNetworkToolCall`), which drives the multi-agent loop's own workflow on the default engine; the AI SDK v4 legacy pair (`generateLegacy`, `streamLegacy`), which runs the agent's tools while skipping the authorization check every supported entry point calls; and `sendToolApproval`, whose continuation branch starts a run under a generated run id rather than resuming. `deleteRunSnapshots` is refused on a separate ground: the snapshot rows it deletes belong to deployment-scoped retention rather than to any caller. Nineteen entry points in all. That leaves `resumeViaRuntime` as the only resume path and the guarded `stream`/`generate`/`prepare` as the only execution entry points. Surface tripwires now classify every `DurableAgent` prototype member and every inherited `Agent` member, so a future peer bump surfaces new entry points on either.

  This is a behavior change for any consumer that called those methods on a FlowSafe durable agent: they now throw instead of executing. Their TYPE signatures narrow too — the overridden members return `Promise<never>`, and the generic overloads several of them carried (`network`, `generateLegacy`, `streamLegacy`, `sendToolApproval`) collapse to a single refusing signature, so a call that no longer type-checks is the intended signal rather than a regression. Nothing in the supported agent-host surface reaches them — route clients through the agent-host run routes.

### Patch Changes

- 80a801c: Signal-provider delivery-error log events now carry the same `terminal` flag as their delivery-rejected siblings, so a dropped-forever throw is distinguishable from a deferred one without re-deriving the classification.
- da6a0aa: Export deployment identity headers from the protocol leaf so external candidates do not import the Durable Object runner barrel.
- 66c19f1: Clean generated output at the packaging boundary so deleted source modules cannot remain in published tarballs.
- 5cbe01d: Align the package and documented Node.js runtime floor with the required `@mastra/core` peer dependency.

## 0.18.0

### Minor Changes

- e7fb658: Time out one suspension. A workflow step can now arm a deadline on the suspension it creates by adding the reserved `flowsafe.deadlineMs` key to the payload it passes Mastra's `suspend()`. When that deadline elapses before the awaited signal arrives, the run's own Durable Object resumes the run itself and delivers the `flowsafe.suspensionTimeout` envelope to the expired step, which tells it apart from a real signal with the exported `isSuspensionTimeoutResumeData()` guard. The deadline is persisted in the run's Durable Object storage, so it survives eviction and hibernation, and the resume is fenced on the exact suspension it was armed against: a genuine signal that arrives first drops the deadline instead of resuming twice.

  This is backward compatible. Nothing arms unless the reserved key is present, so existing runs and existing suspensions behave exactly as before. A host that forwards a `timeoutMs` field today is unaffected: that field was ignored and remains ignored.

  Authoring notes. A step that declares a Zod `suspendSchema` must declare the reserved field, or use a loose object (`z.looseObject()`, or `.passthrough()` on a `z.object()`): Mastra validates the suspend payload and substitutes the parsed result, so a strict `z.object()` strips the key and no deadline arms. A step that declares a `resumeSchema` must accept the timeout envelope as well as its own signal shape, or every timeout resume fails that validation and the deadline is abandoned. Only a top-level suspended step can arm a deadline: a step suspended inside a nested workflow is reported under its nested path while its suspension time is recorded against the enclosing step, so there is no way to fence the resume, and the deadline is refused and logged instead of armed. A top-level step id containing a dot arms normally, but it must not coincide with a nested path: a step `'a.b'` suspended alongside a nested workflow `'a'` whose inner step `'b'` suspends gives one key for two suspensions, and every deadline on that key is refused and logged rather than armed against the wrong one. A `foreach` step arms one deadline for the step as a whole, because that is all the run summary reports for it: run sequentially, which is the default, each iteration suspends on its own and gets its own deadline, so clearing a whole loop by timeout takes one deadline per item; with `concurrency` above 1 the timeout resume delivers the envelope to every iteration suspended at that moment (at most `concurrency` of them), iterations not yet started suspend afresh with their own deadline, only the first suspended iteration's value is read per batch, and clearing the whole loop takes one deadline per batch of `concurrency` items. Values must be a whole number of milliseconds between `MIN_SUSPENSION_DEADLINE_MS` and `MAX_SUSPENSION_DEADLINE_MS` (365 days); a value outside that range is reported in the runner log and left unarmed rather than failing the suspension that Mastra has already persisted.

  Operational caveats. Wake precision is a Durable Object alarm's — near the requested time, not exact — and there is no maintenance-sweep backstop for suspension deadlines, so a lost alarm is a lost deadline. Run-level `deadlineMs` remains the swept mechanism when a hard expiry is required. One deadline fires per wake, a run arms at most `MAX_SUSPENSION_DEADLINES_PER_RUN` of them, and a failing timeout resume backs off before it is abandoned for that suspension — the spent entry is kept, never retried and never re-armed, so the same suspension cannot earn itself a fresh budget, while a later suspension of the same step starts one. Only a wake whose authoritative read succeeded spends that budget: a read that did not succeed keeps a 60 second retry cadence and charges nothing, so a storage incident cannot abandon a live deadline — unless an entry has been due for a full day with the state unreadable throughout, after which that entry is abandoned under its own log (`abandoned after 24 h of unreadable run state`). A run whose state has been readable and genuinely absent across five wakes is charged and abandoned the ordinary way, and either abandonment is permanent for the suspension it was armed against. The routes that must not answer from a fabricated read fail closed instead: while a pending owner recovery cannot read authoritative state, `POST /runs` and `GET /runs/:workflowId/:runId/dispatch-status` return 503 for that run. Each previously answered from the fabricated read in its own way: `dispatch-status` returned 200 with a fabricated summary after deleting the very row it could not see, and `POST /runs` refused the run with a 500 (`existing run … has no matching committed owner`) — the same recovery had deleted the row and released its claim, while the fabricated read still reported the run as existing. A start whose own body throws is the one path that answers otherwise: it logs the recovery read it could not make, leaves the journal armed for a later wake, and rethrows the original start failure, so the caller sees that error's own status — a 500 for an unclassified start fault, or the error's own code such as a 400 for a malformed `deadlineMs` — rather than the 503. The same guard covers agent-host owner recovery, which now retries fail-closed — keeping its journal, its reservation and its run record — instead of possibly deleting a row behind a lagging read. The agent-host routes that drive that recovery fail closed in the taxonomy of the surface they escape to rather than reporting a run they could not read. Those that escape to the thread Durable Object's own shell, such as the dispatch read (`GET /_flowsafe/agent-host/runs/:agentId/:runId?resourceId=…&dispatch=1`) and the start route (`POST /_flowsafe/agent-host/start`), answer 503 from that shell; the schedule-wake route (`POST /signal/schedule`, which recovers a lost dispatch receipt through that same recovery) answers 502 with an `internal error` body from the signals router instead of settling a dispatch receipt from a read it could not make. An armed deadline is not observable through `RunSummary` in this release: the record is the run object's own wake state, no route projects it, and the exported bounds exist so a consumer can validate its own `deadlineMs` before arming rather than to read back what is armed, while `MAX_SUSPENSION_DEADLINES_PER_RUN` is exported as the operational figure a host plans against.

  A timeout resume is not an approval decision. It records `requestedByKind: 'system'` with the reserved `flowsafe-suspension-deadline` principal id, mints no connector grant, and names no reviewer. Only the runner mints the envelope: a resume request whose resume data carries the reserved `flowsafe.suspensionTimeout` key is rejected with a 400 on both the workflow run route and the agent-host resume route, so no caller can present itself to a step as an expired deadline. A step that gates a privileged action must treat its timeout branch as a denial, an escalation, or a no-op — never as consent. Because the run object resumes itself, a timeout resume passes through no host route: `RunRouterOptions.beforeResume` does not vet it and approval reconciliation does not run at that moment, so an approval record filed for the expired suspension stays open until a later host status read supersedes it against the suspension it was bound to. Suspension deadlines cover workflow runs hosted by `DurableObjectRunner`; durable agents keep their own resume path.

## 0.17.0

### Minor Changes

- 37175fa: Fail closed on structured-output coverage gaps. `createGuardedAgent()` rejects structured output before model execution because Mastra exposes parsed values to messages, persistence, and observability hooks before a post-generation wrapper could inspect them. It also rejects object-only policies that no supported guarded invocation can cover.

  Processor-visible object chunks are validated as JSON, evaluated through their canonical serialization, and replaced with the same canonical clone. Standalone object-only policies abort when an invocation exposes no object to the processor. Policy lists and decision-driving descriptors are snapshotted at construction, evaluator callables retain their original receiver, and per-stream audit metadata stays bounded by configured policies and channels.

  Flowsafe recognizes the new guarded-agent host protocol and rejects structured output on durable stream, generate, and prepare before Mastra can bypass the narrow handle. Durable entry points snapshot data-property call options before validation and delegation, reject accessors, and use the same snapshot for later run registration. Both packages pin their tested `@mastra/core` 1.50.0 contract.

  Hold-back cost under large streams is measured by opt-in evidence tests (`BREAKWATER_PERF=1`) and recorded in the policy-engine design guide.

## 0.16.1

### Patch Changes

- 296207f: Use one approval-service assembly for public approval requests and terminal cleanup so SLA, resume, audit, notification, streaming, and separation-of-duties policy cannot drift between those paths.

  Make the Durable Object principal diagnostic lifecycle-neutral and document termination, deadline expiry, approval abandonment, and cleanup ordering across the architecture and observability guides.

## 0.16.0

### Minor Changes

- 1f6a13a: Add idempotent run termination and deadline maintenance to the Durable Object host kit.

  `createRunRouter()` now exposes authenticated workflow termination, and the agent host exposes the equivalent agent-run route. Cancellation is legal from every nonterminal wait, retry, suspension, and running state. It persists a structured `CANCELLED` envelope before cleanup, abandons open approvals without minting or resuming, discards an executing agent-schedule receipt, and releases ownership only after the terminal snapshot commits. Retries and Durable Object eviction re-drive the same persisted intent. A disputed economic settlement returns a structured `409` before active work is interrupted.

  Starts and resumes accept `deadlineMs`. `RunSummary` reports the resulting deadline, and the maintenance Durable Object runs a bounded, cursor-resumable deadline duty. Each overdue run uses an owner-Durable-Object compare-and-swap transition to `timed_out` with a `TIMED_OUT` envelope. Deadline expiry abandons open approvals, while the existing SLA sweep remains escalation-only. Maintenance health now reports deadline scheduling and completion timestamps.

  Hosts composed with `createFlowsafeWorker()` receive the workflow route and deadline duty. Hosts that expose lifecycle termination through a custom `DurableObjectRunner` must override `runLifecycle()` with approval-abandonment hooks and implement `release()` on their `runOwnership()` store. Agent hosts with begun schedule-dispatch leases must also provide the exact-run discard hook. Ordinary starts and resumes remain lifecycle-metadata-free unless they use a deadline or another trusted lifecycle projection.

  The release adds no required D1 table and keeps the existing deployment-identity header, trusted execution-principal header, separation-of-duties policy, snapshot authority, and 39-character storage-prefix limit.

## 0.15.0

### Minor Changes

- 34e8ae0: Require host-provided Wrangler `>=4.118 <5` without installing it as a Flowsafe peer. Hosts that use `flowsafe-provision` must now install a compatible Wrangler version directly.

  Fleet Control now supports an explicit Wrangler command, creates D1 databases through Wrangler 4's current output contract, and revokes plain-Worker credentials through the Workers API without creating untracked Worker versions.

  Custom `PlainWorkerRouteApi` implementations must add `deleteControlSecrets(scriptName, secretNames, fence)`. Implement it with the Workers script-secret DELETE API, treat an HTTP 404 as already deleted, and retain a final authoritative secret-list check.

### Patch Changes

- 2c097d8: Stop treating an RPC binding as the deployment database.

  `isDatabaseBinding` accepted any binding whose `prepare` was a function. A
  service binding with a named `entrypoint`, and a Durable Object stub, are
  proxies that answer every property with a callable, so the deployment-sentinel
  scan adopted them as databases and the request failed with `The RPC receiver
does not implement the method "prepare"`.

  Any Worker holding both a `DB` binding and an RPC binding was affected. Fleet
  control's trusted state scripts are exactly that shape — `DB` beside
  `OUTBOUND_PROXY` bound to the shared outbound Worker's `StateEgress`
  entrypoint — so they failed on the first Durable Object request they served.
  Fetcher-shaped bindings are now excluded; `D1Database` has no `fetch`.

## 0.14.0

### Minor Changes

- fa12c05: Rename the maintenance receipt audience from `anchorage-fleet-control` to `flowsafe-maintenance-receipt`, matching the protocol naming of the sibling capability audience instead of tracking a package name.

  This is a wire change. `mintMaintenanceReceipt` and `verifyMaintenanceReceipt` both pin the audience, and the issuer runs inside each deployed Worker while the verifier runs at the control plane, so a host minting receipts on an earlier FlowSafe fails verification against a host on this release, and the reverse. Upgrade the issuer and the verifier together, before a fleet exists. A test now pins the literal on the `aud` claim, and the constant records that a later rotation needs a verifier-side accept-set rather than another lockstep break.

## 0.13.1

### Patch Changes

- 352b38c: Enforce the shared D1 table-prefix syntax and length contract in every public signal, schedule, and background-task storage constructor, subclass, and factory. Correct the FlowSafe 0.13.0 release notes so shipped changes are no longer duplicated under `Unreleased`.

## 0.13.0

### Minor Changes

- 4f0fc9d: Create artifact purgers from the current Worker environment, apply the configured D1 table prefix to built-in maintenance, enforce the shared 39-character prefix limit at every storage and low-level purge boundary, and pin the D1 adapter compatible with the minimum supported Mastra core.

  Migrate a configured artifact purger from `artifactStore: store` to `artifactStore: () => store`. For R2, use `artifactStore: (env) => new R2ArtifactStore(env.ARTIFACTS)`. If runtime storage uses `tablePrefix`, keep it at 39 characters or fewer and set the identical `storageTablePrefix` on `createFlowsafeWorker()`.

## 0.12.0

### Minor Changes

- 3276c2a: Use `jose` for actor JWT verification and stream-ticket signing. Stream tickets are now standard three-segment JWTs with a dedicated audience and `typ`, so actor tokens and stream tickets cannot cross-verify even if a deployment reuses a secret. Existing verifier injection, actor validation, issuer, audience, expiry, and key-selection behavior remains fail closed.

  GitHub webhook signatures now require the exact `sha256=<64 hex>` shape before raw-byte WebCrypto verification. Malformed signatures and invalid UTF-8 JSON return stable client errors without throwing, while empty and non-ASCII payloads remain byte-exact.

  Deployment identity provisioning now recognizes Cloudflare D1's exact `_cf_KV` and `_cf_METADATA` internal tables on a fresh database while continuing to reject arbitrary or lookalike pre-existing application tables.

- b3b4b55: Replace pooled request-level tenancy with one physically isolated organization per deployment.

  This breaking release requires `DEPLOYMENT_TENANT`, a matching D1 sentinel seeded before application migrations, and a per-deployment `DEPLOYMENT_IDENTITY_SECRET`. The Worker validates the strict singleton sentinel, and production Durable Objects also authenticate every Worker caller before reading storage.

  `ActorContext`, `ActorResolver`, and `createActorResolver()` replace their tenant-named equivalents. Approval and subscription factories now expose `.store()`, run, thread, schedule, and subscription ids are opaque and server-minted, and live hubs, provider hosts, and background-task hosts are deployment singletons.

  Tenant-branded stores, tenant-prefixed id helpers, the tenant registry, subdomain cross-check, and in-database tenant purge APIs have been removed. First-time sentinel provisioning refuses any database with pre-existing application tables; provision a fresh database for each organization.

  `createFlowsafeWorker()` no longer exposes the unused `wrapResolve` hook. Authenticate and validate actors in `buildVerifier`, mount deployment-specific routes through `preRoutes`, and enforce final mutation policy through `beforeStart` or `beforeResume`.

  `createFlowsafeWorker()` now delegates sweep, purge, and optional schedule-tick duties to `createFlowsafeMaintenanceDurableObject()`. Tenant Workers no longer export `scheduled()` or `queue()` handlers and contain no cron triggers or queue consumers. Provisioning must set a distinct `MAINTENANCE_ADMIN_SECRET`, call `POST /admin/ensure-maintenance`, and monitor `GET /admin/maintenance-status`.

  The maintenance singleton persists and re-arms its next alarm before running one due duty. A failed or terminated duty cannot break the alarm chain, starve another due duty, or update the last-success timestamp.

  Externally authored fleet releases no longer receive the reusable maintenance administrator secret or a shared audit Queue producer. They relay operation-bound, short-lived maintenance capabilities to trusted state and require signed results, and send untrusted audit events through an authenticated trusted-state proxy that supplies canonical infrastructure attribution.

  Server-minted runs, threads, and schedules plus validated host-owned resource keys retain per-principal authorization through the deployment-local resource-owner registry. Inaccessible resources return `404` before role checks.

  The package now ships `flowsafe-provision` for strict D1 sentinel provisioning. The CLI resolves a consumer-installed Wrangler `>=4 <5` optional peer, rejects other Wrangler majors, and maps preview provisioning to Wrangler's remote preview target.

  The deployment-identity protocol is exported as `@proofoftech/flowsafe/deployment-identity-protocol` for trusted fleet provisioners that must apply the same sentinel rules through another Cloudflare API client.

  Opt-in HTTP approval creation now requires write access to the named run. Those client-filed records remain decision-only; only trusted suspension-bound, run-scoped, or server-targeted approvals can resume execution.

  Subscription stores now snapshot caller input before persistence and use the same JSON metadata semantics in D1 and memory. External provider resource ids reject ASCII controls and values larger than 1,024 UTF-8 bytes.

## 0.11.0

### Minor Changes

- 52d6836: Add server-owned, fine-grained permission resolution to the guarded agent host.

  Agents can declare `requiredPermissions` with all-of semantics. The thread host resolves effective permissions from the trusted human or automated principal, records the required identifiers and policy version in authorization audit detail, and denies execution when any permission is missing. Agents that use only `allowedRoles` and `allowedAutomation` keep their existing behavior and do not require or invoke a resolver.

- d78e779: Project server-resolved principal permissions into trusted agent-run context.

  The thread agent host now runs a configured `resolvePrincipalPermissions` on every authorized entry — role-only agents included — and mints the resolution into derived request context as `breakwater.principalPermissions` on every start and resume leg, where breakwater's connector `requiredPermissions` gate enforces it. When no resolution exists the host projects an explicit `null`, so a resume retires a stale persisted projection and permission-declaring connectors fail closed. A failed resolution still denies a permission-requiring agent; on a role-only agent it is audited as a new `agent.permissions.resolve` error event and the run proceeds without a projection.

  `TrustedAgentExecution` gains a required `principalPermissions` field, `Permission` and `isPermissionIdentifier` are now re-exported from `@proofoftech/breakwater/rbac`, and the root/approval-api barrels export `BREAKWATER_PRINCIPAL_PERMISSIONS_KEY`. The optional `@proofoftech/breakwater` peer range moves to `>=0.9.0 <1.0.0` for the shared permission vocabulary.

## 0.10.0

### Minor Changes

- cb0f861: Replace connector ID approval arrays with structured connector grants. Durable-agent approvals now bind to the exact Mastra tool call, workflow approvals bind to the exact suspension, and standing grants require explicit run scope.

  This is intentionally breaking: `APPROVED_CONNECTORS_CONTEXT_KEY`, `BREAKWATER_APPROVED_CONNECTORS_KEY`, and `approvedConnectorsForLeg()` are removed. Legacy arrays and approval rows without explicit scope fail closed. Migrate trusted hosts to `CONNECTOR_GRANTS_CONTEXT_KEY`, `CONNECTOR_EXECUTION_CONTEXT_KEY`, and `connectorGrantsForLeg()`.

### Patch Changes

- f654696: Register runtime-driven durable agents with the runtime-owned Mastra instance so approved runs can resolve their agent and resume after isolate eviction on newer Mastra versions.

## 0.9.0

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

## 0.8.0

### Minor Changes

- 09a4406: Add guarded Breakwater agents and Flowsafe's authenticated, catalog-driven agent host. Agent starts now derive trusted identity and execution context, agent resumes require an approval-bound capability, and status and NDJSON observation remain tenant-bound.

### Patch Changes

- 6670285: Prevent approval resume after isolate eviction from rerunning application input processors or input policy evaluation. Durable-agent recovery now reauthorizes the stored principal, restores both Mastra run registries with complete runtime processor chains, and fails before resumed tool execution when authorization is denied.

## 0.7.0

### Minor Changes

- def3b37: Complete and document the public flowsafe surface. The root entry point now
  re-exports the approval API, Durable Object runner, artifacts, and audit export
  surfaces with parity tests. Signal-provider hosts gain a tenant-safe topology,
  stable polling alarms, and automatic post-mutation polling reconciliation.
  Publish comprehensive package, deployment, approval, durable-agent, operations,
  and API-reference documentation, plus a full advanced starter host.

## 0.6.0

> **Final 0.6.0 state:** The `eca3b6e` closeout entry below supersedes earlier
> statements in this release section that agent schedule targets and durable D1
> background-task execution were deferred. Both are supported in 0.6.0 when
> their opt-in host wiring is configured. Durable-agent restart resume and
> notification dispatch are also closed and verified.

### Minor Changes

- eca3b6e: Close the durable-agent, agent-schedule, notification-dispatch, and D1 background-task execution residuals with tenant-safe thread routing and eviction-safe approval resume. Harden stored schedule context and core schedule-contract validation; make D1 notification creation preserve explicit-id coalescing, insertion-order targets, rollback-safe atomic migration, and concurrent partial updates; priority-plan summary and individual delivery across state-stable 100-id batches; accept Mastra's raw constructor pubsub with rollback-safe workers and a synchronous enqueue shutdown gate; close failed resume streams; validate public numeric configuration synchronously; preserve nested Mastra background-task SSE events; and require a proven process shutdown before spike restart.
- d54d2be: Track D — schedules (`@proofoftech/flowsafe/schedules`, additive, opt-in). A new
  subpath ships the D1 schedules domain, a CAS-driven tick we own, and a tenant
  facade — all on the single DO + D1 RunnerRuntime substrate (P1), no new
  `ApprovalRecord` shape or existing signature changed, unconfigured hosts
  byte-identical.

  - `D1SchedulesStorage` + `createScheduleStorageDomains` — the flowsafe-owned D1
    domain over `mastra_schedules` / `mastra_schedule_triggers` (the
    `@mastra/cloudflare-d1` adapter ships neither), mirroring core's
    `SchedulesStorage` contract incl. the CAS `updateScheduleNextFire`. Composed
    into `createD1Storage` via the injected `domains` seam.
  - `createScheduleTick` — we OWN the tick (DL-012): `listDueSchedules` → CAS claim
    → workflow targets mint a fresh INV-1 runId and fire through the host's
    run-start seam; agent targets are GUARDED OFF (their only public fire path,
    `schedules.run(id)`, enqueues onto core's pubsub worker loop we do not run, so
    firing is a fail-closed audited skip — agent-target execution is deferred). An
    injectable run-cap seam (DL-007) skips a capped tenant while the schedule stays
    healthy. The P4 stored-context barrier strips reserved keys before any leg.
  - `createScheduleRouter` — the tenant facade (DL-013): server-minted ids,
    `metadata.tenantId` stamping, tenant-filtered reads, ownership 404s (no
    oracle), per-tenant count + fire-rate caps, and P4 reserved-key rejection on
    create/update (the whole `breakwater.` namespace + `mastra:goal`).
  - Storage triad (DL-003): both tables register in the schema-guard inventory
    (8 → 10) with a new metadata-filtered `purgeTenant` kind
    (`TENANT_METADATA_PURGE_TABLES`), plus `purgeExpiredScheduleTriggers` for the
    trigger-history TTL. `createFlowsafeWorker` gains an opt-in `scheduleTick` seam
    (its own failure-isolated cron duty) and a `SCHEDULE_TRIGGER_RETENTION_DAYS`
    purge duty.

- 0f4f70a: Track E (M-007) — signal providers: a new subpath `@proofoftech/flowsafe/signal-providers`
  (additive, opt-in, subpath-only). Host external-event providers on a Durable
  Object with alarm-driven polling, terminate provider webhooks on the Worker, and
  persist subscriptions in a flowsafe-owned D1 table.

  - `SignalProviderHost` — a per-tenant provider host DO (`idFromName(tenantId)`)
    whose alarm rehydrates subscriptions from D1 (core's registry is in-memory,
    lost on eviction) and polls each of the tenant's providers with per-provider +
    per-delivery failure isolation, delivering through Track C's thread-DO topology.
  - `D1SubscriptionStoreFactory` — a flowsafe-owned, tenant-columned
    `flowsafe_signal_subscriptions` store mirroring the approval store's INV-2
    posture (`.forTenant()` tenant-bound, `.system().listByResource()` the webhook's
    cross-tenant authority). Registered in `purgeTenant` (`PurgeTenantResult.subscriptions`);
    retention is `none` (standing config reaped only at offboarding).
  - `createWebhookRouter` — webhook ingress that verifies the provider signature
    over the RAW bytes BEFORE parsing, maps the payload to a tenant via the
    subscription ROW only (never the payload), rate-caps per provider+tenant, and
    audits every ingest with a bounded forgery audit. `createSubscriptionRouter` —
    the human-only HTTP subscribe/unsubscribe surface (never exposed as model
    tools; mints no capability).
  - `githubSignalProvider` — a binding-gated GitHub reference provider
    (`X-Hub-Signature-256` verified constant-time via WebCrypto). `createWebhookSignalProvider`
    is the generic path.

  Also adds a `subscriptions` counter to `PurgeTenantResult` (the DL-003 offboarding
  coverage for the new flowsafe-owned table).

- 6c80e92: Track F (M-005) — goals. New subpath-only export `@proofoftech/flowsafe/goals`:
  `createObjectiveRouter`, a role-gated + audited objective HTTP surface
  (set/get/update/clear over `/api/threads/:threadId/goal`) that writes the
  thread-scoped goal record in Track C's `mastra_thread_state` domain
  (`GOAL_STATE_TYPE` 'goal'). The write path is a P6-lite ingestion boundary
  (auth → coarse role → thread-prefix ownership 404 → size cap →
  `assertNoClientMemoryIds` → field allowlist → maxRuns host cap → audit) and
  persists through `@mastra/core`'s own `writeObjective`/`readObjective`/
  `clearObjective`, so a record it writes is byte-identical to what the durable
  goal step reads via `resolveGoalStore` (DL-018 — no thread-DO affinity needed
  for the write). A requested `maxRuns` above the host cap is rejected, not
  clamped (default the core `DEFAULT_GOAL_MAX_RUNS`, 50; DL-007). Goals never mint
  capability (P8) and Track F starts no runs — per-tenant run budgets stay
  enforced at the existing seams. `GOAL_REQUEST_CONTEXT_KEY` ('mastra:goal') is
  reserved with a no-collision pin against the runtime's requestContext base keys.
  Hosts mount the surface opt-in through `createFlowsafeWorker`'s new
  `buildObjectiveRouter` seam; absent config is byte-identical. Additive only — no
  new table, schema-guard, purge, or TTL change (reuses the Track C thread-state
  domain), and no existing signature or `ApprovalRecord` shape changes.

### Patch Changes

- 8e3562f: Make approval filing atomic across terminal and open records for the same captured suspension fingerprint, preventing stale reconciliation from filing over a decision.
- 97cb097: Harden the P6 ingestion routers against a pre-auth malformed-path fault. The
  signal, goal, and schedule routers decoded the threadId/schedule-id path
  segment with bare `decodeURIComponent` before authentication, and
  `createFlowsafeWorker`'s fetch handler did not wrap the router calls — so an
  unauthenticated request with malformed percent-encoding (e.g.
  `POST /api/threads/%/message`) threw a `URIError` out of `fetch()` as a
  per-request 500. The three routers now use a shared `safeDecodeSegment`
  (host-kit) that treats malformed encoding as route-absent (byte-identical to a
  non-matching path), matching the Track E webhook router; the worker fetch
  handler gains a top-level try/catch that contains any handler throw as a
  generic 500 without leaking `error.message`. The same helper closes the whole
  class: the background-tasks read route (post-auth, DO-mounted) adopts it too, so
  a malformed taskId returns the no-oracle 404 instead of throwing. Additive and
  behavior-preserving for all valid paths.

## Unpublished 0.5.0 draft (included in 0.6.0)

No `@proofoftech/flowsafe@0.5.0` package was published. These changes shipped in
0.6.0 and remain here as their original generated release notes.

### Minor Changes

- 281c6d1: Track 0 (substrate for the long-running-agents program): close the agent-memory
  tenancy obligations and add the seams the agent tracks build on. All additive —
  a host that configures none of it is byte-identical.

  - **Agent-memory host boundary** (`@proofoftech/flowsafe/host-kit`):
    `assertNoClientMemoryIds(body)` rejects (400) any request body naming
    `threadId`/`resourceId` — memory ids are minted server-side from the
    authenticated tenant via `TenantContext.newThreadId()/newResourceId()`, never
    chosen by a client — and `requireOwnedMemoryId(tenant, id)` answers 404 (not
    403, so no existence oracle) on a foreign id. Every memory-touching route MUST
    call both (the rule the agent-domain routes in later tracks adopt).
  - **Recall-path proof**: core's own `MastraMemory` implementation over the real
    D1 store, two tenants keyed by the SAME business key, pinning that `recall()`,
    `listThreads({filter:{resourceId}})`, and resource-scoped `getWorkingMemory()`
    never cross tenants.
  - **Thread TTL**: `purgeExpiredThreads(db, { ttlMs, limit, tablePrefix })`, wired
    into `createFlowsafeWorker`'s purge cron behind the new `THREAD_RETENTION_DAYS`
    var with its own failure isolation. Keyed on `mastra_threads.updatedAt`
    (threads are not per-run and have no terminal status); messages go with their
    thread and before it; working-memory rows are untouched. Unset by default — no
    thread expires until an operator names a number.
  - **Extensible purge/guard inventory** (`TENANT_RANGE_PURGE_TABLES`,
    `TenantRangePurgeTable`, `TenantRangePurgeCounter`): adopting a `mastra_*`
    domain is now one additive row plus the counter/result pair the types force in
    the same change; the schema guard still trips on any silently added table, and
    its inventory now also forces each table's retention story — where "no TTL"
    demands a written reason, so an absent decision cannot read as "none needed".
  - **Host pubsub identity** (`createHostPubSub`, `HostPubSub`): the seam for one
    in-process `EventEmitterPubSub` per host DO — passed to `init()` (new
    `InitOptions.pubsub`), taken back off `InitResult.pubsub`, and threaded into
    the runtime (new `RunnerRuntimeOptions.pubsub`, readable as
    `RunnerRuntime.pubsub`) so a host reaches it with no host change. Every
    consumer in the isolate then shares one emitter instead of each letting core
    default its own (two such feeds never see each other's events). The identity
    and the seam only: nothing passes it to core's `createRun` yet, so a configured
    pubsub is an identity the host holds, not yet a feed core publishes on. Opt-in;
    absent leaves polling as the fallback.
  - **`ThreadDurableObject`**: per-thread DO base addressed `idFromName(threadId)`
    where the threadId is tenant-minted, so its name carries the tenant like a
    runId. Every request must state its authenticated tenant
    (`THREAD_TENANT_HEADER`) and is asserted against that prefix before the
    subclass's `route()` runs — fail closed (403). Everything else it throws rides
    the shared `doErrorResponse` taxonomy, so a run driven from a thread route
    keeps its 404/409/400 instead of collapsing to a 500.
  - **`createThreadTopology`** (`@proofoftech/flowsafe/host-kit`): the sanctioned
    way to reach a per-thread DO, and the MINTER for the header
    `ThreadDurableObject` verifies. `send`/`forward` refuse (404) a threadId the
    authenticated tenant does not own — before the DO is addressed — and stamp
    `x-flowsafe-tenant` from the resolved `TenantContext`, `forward` OVERWRITING
    whatever a client's own request carried. Mint and verify ship together:
    forwarding a client Request verbatim (the existing hub idiom) would otherwise
    let the client write the very header the thread DO authenticates on.

- 4ea35fd: Track A (durable agents): drive Mastra's durable-agent loop through the one
  RunnerRuntime chokepoint so agent legs inherit the substrate's invariants —
  additive and opt-in, no existing signature or `ApprovalRecord` shape changed.

  - `@proofoftech/flowsafe/agent-runner` (new subpath): `createFlowsafeDurableAgent`
    returns a `DurableAgent` subclass whose `executeWorkflow(runId, workflowInput)`
    calls `runtime.start('durable-agentic-loop', { runId, inputData })` instead of
    the base `createRun + start` (DL-001/DL-010), so INV-1 (server-minted runId),
    the per-leg `requestContextForRun` grant derivation, and the resume ledger
    apply to agent legs. `stream()`/`generate()`/`prepare()` are overridden to
    REQUIRE a caller-minted runId — closing every inherited minting entry point's
    upstream `crypto.randomUUID()` fallback (INV-1: a durable-agent run must carry
    its tenant everywhere it becomes a key). The shared loop workflow is registered on
    the runtime idempotently; the agent's stream pubsub defaults to the runtime's
    identity (one feed per DO). `resume()` stays non-client-facing — resume flows
    only through the approval-decision path (grant-only doctrine, P8).
  - The durable tool-call step hands `tool.execute` the ENGINE-LEG requestContext
    from its step params (spike S1, verified against `@mastra/core` 1.50.0 dist),
    so the `breakwater.approvedConnectors` grant reaches the connector write gate
    with zero extra wiring and a forged/self resume fails closed there.
  - R-003: the record-creation/bridge path parses BOTH durable approval-suspend
    shapes — nested `{ type:'approval', requireToolApproval:{ toolCallId, toolName,
args } }` and flat `{ type:'approval', toolCallId, toolName, args }` — and
    derives `connectors:[toolName]` (the connector id the write gate checks) so an
    approved agent gate mints exactly that grant. The resume-routing
    `threadId`-capture seam (DL-002) is deferred to Track C, where the thread-DO
    consumes it.
  - `RunnerRuntime` now threads the host pubsub identity into both `createRun`
    sites (`createRun({ runId, pubsub })`); undefined leaves behavior
    byte-identical (polling fallback).
  - The workerd `spike:verify` grows agent-gate scenarios proving the R-003
    round-trip and forged-resume fail-closed on real workerd + D1.

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

- 4b953d4: Track C (long-running-agents program, M-004): signals, subscriptions, and
  notifications. All additive and opt-in — a host that configures none of it is
  byte-identical.

  New subpath `@proofoftech/flowsafe/signals`:

  - **Thread-DO signal routes** (`createThreadSignalRoutes`) — the
    message/queue/signal/state/notification surface hosted on the per-thread DO.
    Each call stamps the DO's one pubsub identity (`scope.init.pubsub`) onto the
    agent so a send drains IN-PROCESS into an active loop: core keys its signal
    registry by the pubsub instance, so affinity needs both the DO isolate
    (idFromName(threadId)) and the shared pubsub — the DL-002 thesis, proven on
    workerd (spike C-S2). An idle-thread WAKE starts a run, so it requires a
    runtime-driven durable agent (`createFlowsafeDurableAgent`, which carries the
    new `RUNTIME_DRIVEN_AGENT` brand from `@proofoftech/flowsafe/agent-runner`) —
    its stream re-enters RunnerRuntime rather than the default engine; a wake
    requested on a plain agent is refused fail-closed (degraded to a durable
    persist), and every allowed wake consults the per-tenant run cap (DL-007). The
    routes drive the public Agent methods only (`agentThreadStreamRuntime` is not
    on core's exports map).
  - **D1 storage domains** — `D1NotificationsStorage` (over `mastra_notifications`,
    mirroring core's InMemory reference incl. coalescing) and
    `D1ThreadStateStorage` (over `mastra_thread_state`, the state-signal lanes and
    the goal record). `@mastra/cloudflare-d1` ships neither, so they are
    flowsafe-owned; `createSignalStorageDomains` composes them into
    `createD1Storage` (which now accepts injected `domains`) so
    `agent.sendNotificationSignal` persists to D1. Both tables are registered in
    the tenant-range offboarding purge (`purgeTenant`), the schema-guard inventory,
    and their own opt-in TTL purges (`purgeExpiredNotifications`,
    `purgeExpiredThreadState`) — the DL-003 triad, in one change. `PurgeTenantResult`
    gains `notifications` + `threadState` counters (a breaking change only for
    callers that build a `PurgeTenantResult` literal).
  - **P6 ingestion trust boundary** (`createSignalRouter`) — every ingest is
    authenticated → role-gated → thread-prefix-ownership-checked (404, no oracle) →
    size-capped → memory-id-refused → attribute-allowlisted → per-tenant rate-capped
    → forwarded via `createThreadTopology` (which overwrites the tenant header, so a
    forged one cannot ride along; cross-tenant sends fail closed at both the topology
    404 and the DO 403, spike C-S4). Every ingest is audited (`signal.ingest`),
    accepted OR rejected — including the three post-auth denials that read like an
    attack on this channel (the role 403, the cross-tenant thread 404, the
    memory-id 400); pre-auth failures (401 / resolver throw) are not audited. XML
    injection is neutralized by core's `signalToXmlMarkup`, which entity-escapes
    contents and attribute values and re-validates tag/attribute names — a single
    layer over a soft-pinned core, so a C-S5 render test pins it (a core `escapeXml`
    regression fails flowsafe CI); the route adds its own line at ingest —
    `tagName` XML-name validation, the attribute-key allowlist, and the size cap —
    but does not re-escape the contents. Signals never mint capability —
    `sendToolApproval` is not an approval surface (P8).
  - **`SignalClient`** — a DOM-free client in the `ApprovalApiClient` mold.

  `createFlowsafeWorker` gains an opt-in signal stage (`buildSignalRouter` seam) and
  two opt-in TTL cron duties (`NOTIFICATION_RETENTION_DAYS`,
  `THREAD_STATE_RETENTION_DAYS`).

## 0.4.0

### Minor Changes

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

- dbe6a93: Add live streaming over WebSocket-over-Durable-Object so the approval dashboard and run-status views update within one round-trip instead of on the 3s/5s poll, tenant-isolated, with polling retained as a graceful fallback. Streaming is opt-in: a host that wires no hub binding or ticket secret keeps working unchanged on poll-only.

  - **approval-api**: a new `ApprovalStreamEvent`/`ApprovalStreamSink` seam (distinct from the reviewer-facing notification sink). `ApprovalService` fires it fire-and-forget on every successful create/claim/decide/delegate/supersede, and `sweepSLA` on each escalation; a throwing or rejecting sink never fails the mutation and is audited.
  - **do-runner**: a new per-tenant `HubDurableObject` base that accepts hibernatable WebSocket subscribers, fans out approval events, and tracks a presence roster, asserting `id.name` equals the event tenant; and a per-run WebSocket route on the runner DO that broadcasts the authoritative `RunSummary` at each lifecycle boundary. The structural DO state types are widened for the Hibernatable-WebSocket API with an `AssertTrue` compile pin, and no `cloudflare:workers` import enters the node/vitest graph. Every fan-out is per-socket isolated, so one closing socket never starves the rest.
  - **host-kit**: `mintStreamTicket`/`verifyStreamTicket` (a short-lived HMAC addressing ticket bound to tenant, channel, run, actor, and expiry over the existing HS256 primitives), a structural `HubNamespaceLike` seam plus `createHubTopology`, and `createStreamRouter` (mounts `POST /api/stream/ticket` and the ticket-verified hub/run WebSocket upgrade routes). `createFlowsafeWorker` gains an optional stream stage that mounts only when both a hub binding and `STREAM_TICKET_SECRET` are present, threading the fetch-scope hub sink through `ctx.waitUntil` and the cron sink through the sweep's collected keepalive. The ticket carries only addressing, never a grant, and is verified solely at the Worker; the DOs re-bind by their own `idFromName` identity, so the run channel rides INV-1 and the hub rides `id.name` equals the tenant.
  - **approval-ui**: a DOM-free injected `StreamTransport` (structural, like the fetch seam) with pure live-merge/optimistic-decide/reconcile reducers, an optimistic decide that reconciles against the authoritative event and surfaces a conflict when a different reviewer decided first (and rolls back on failure), a client-side liveness heartbeat that detects a silently half-open socket, and additive optional `Toast`/`PresenceIndicator` slots. A browser-WebSocket transport factory lives in the UI pass only; the interval poll stays as the fallback and periodic reconciler. The library stays DOM-free, styling-agnostic, subpath-only, and React 18+.
  - **deploy template + spike**: a copy-me hub wiring reference, and a workerd spike proof that a subscriber receives a fanned-out event, survives DO eviction and hibernation, and that an expired or cross-tenant ticket is refused fail-closed.

  None of these change the `ApprovalRecord` shape or any existing signature; a host that does not opt into streaming is byte-identical to before.

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
