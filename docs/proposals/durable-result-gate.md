# Proposal: durable result gate

> Status: design proposal; not implemented. Shipped behavior is documented in [Durable agents](../durable-agents.md), [Breakwater architecture](../breakwater-architecture.md), and [Policy engine design](../policy-engine-design.md).

FlowSafe needs a producer-owned output decision before Mastra persists or returns a durable agent’s result. Breakwater’s current result processor throws on denial, but Mastra’s durable finalizer catches that exception and continues. This proposal compares interception points, recommends an incremental finalization gate, and defines the additional producer changes needed for a guarantee covering persistence and replay.

Content type: design proposal. Audience: FlowSafe and Breakwater maintainers and deployment operators assessing durable output confidentiality.

Baseline: `dev`, commit `5b467f2`; source reading on 2026-10-02 against installed `@mastra/core` 1.73.0 and `@mastra/cloudflare-d1` 1.4.0. This proposal records source-derived behavior, not new execution results. The probes below must confirm storage and lifecycle behavior before implementation claims change.

## Define the property before choosing the interception point

The acceptance oracle is: no generated output denied by an output policy appears on any of these surfaces. Test each surface independently, including nested copies rather than checking only the top-level answer:

| Surface | What the test inspects |
| --- | --- |
| Final thread save | Assistant text, reasoning, and generated content in messages recalled from the run’s thread |
| Earlier thread saves | Messages written before tool-approval suspension, background-task callbacks, and `savePerStep` where reachable |
| Run result | `RunSummary.result`, including `output.text`, `output.steps`, `messageListState`, and nested generated-content copies |
| Live observation | Text, reasoning, objects, and content-bearing events from `stream()` and `observe()`, plus returned message APIs: `messageList`, `response.dbMessages`, and `getFullOutput().messages` |
| Other memory stores | Generated content in working-memory resources and semantic-recall vector metadata, including writes before finalization |
| Workflow snapshots | Raw D1 `mastra_workflow_snapshot` JSON during execution, suspension, failure, and retained terminal success, including nested workflow state |
| Terminal stream payloads | Generated text in `finish` and `step-finish`, including `output`, steps, and `_durableStepContent` |
| Topic history and replay | Published events in the configured pub/sub history and replay after disconnect or registry eviction |
| Initial thread history | Saved messages exposed by thread streaming with `withInitialHistory` |

“Denied” includes an explicit policy refusal, evaluator failure, missing decision, timeout, and hold-back’s terminal classification. Withholding remains the default until the applicable policies positively allow the candidate. A classifier exception is not permission to release a candidate.

The strict oracle includes output rejected only when the final aggregate is evaluated. A provisional step-level allow does not authorize durable storage or publication of text that a later result-level decision can deny. Previously allowed, independent conversation turns remain outside the new run’s output transaction; identify run-generated messages explicitly rather than rewriting remembered history.

The oracle requires a distinction between two release modes:

- **Incremental release:** apply the existing stream policies and retain their bounded tails; a final denial cannot retract an already released prefix
- **Final release:** stage generated content until the run-wide result decision allows it; no content-bearing step event, thread write, snapshot copy, or topic event bypasses that decision

Incremental release can satisfy the existing subscriber contract. It cannot satisfy the strict result-level oracle for arbitrary policies. Final release changes latency and durable recovery; it is a separate proposed execution contract, not another name for current `holdBack`.

See [the security threat model](../security-threat-model.md) for the existing trust boundaries. The proposed property concerns generated output; it does not authorize suppressing caller input, tool authorization records, or lifecycle metadata needed to settle a run.

## Current execution and storage order

The current durable path exposes output before the result processor runs. The following order comes from reading `@mastra/core` 1.73.0, not from a new instrumented run.

### Model publication and step processing

`createDurableLLMExecutionStep` constructs its producer-side `MastraModelOutput` without output processors. It publishes model text and reasoning chunks through `emitChunkEvent` while accumulating the step. It defers `step-finish`; the subscriber-side `createDurableAgentStream` constructs a different `MastraModelOutput` with the registry’s output processors.

After materializing the streamed messages, `createDurableLLMExecutionStep` calls `ProcessorRunner.runProcessOutputStep`. This runs after model text publication and before ordinary server-tool execution. For a step with tools, `createDurableToolCallStep` performs approval handling and `flushMessagesBeforeSuspension`; `createDurableLLMMappingStep` emits the deferred `step-finish` after tool results, before its optional per-step save and the next iteration.

Core 1.73.0’s durable continuation predicate also publishes text and tool data through `emitIterationCompleteEvent`. That event belongs in live-observation and topic-history assertions. A publication gate must cover content-bearing lifecycle payloads as well as model deltas and terminal events.

A non-retry `TripWire` from `processOutputStep` does not fail the workflow. The LLM step retains the current message list, suppresses its tool calls, sets a terminal `tripwire` reason, and continues toward final mapping. A retry rolls the list back to the step boundary when retries remain, but cannot retract published text. A non-`TripWire` error enters model retry/fallback handling; exhaustion calls `emitFatalErrorBail`, which publishes error events but returns an LLM output with reason `error`, an empty text field, and the retained list. It does not throw out of that step; see core 1.73.0’s `createDurableLLMExecutionStep`.

Breakwater’s [PolicyEngine](../../packages/breakwater/src/policy-engine/index.ts), class `PolicyEngine`, currently implements `processOutputStream` and `processOutputResult`, not `processOutputStep`. Its result hook evaluates `result.text` as the answer and joins per-step `reasoningText` for reasoning policies. The guarded application processor contract also refuses `processOutputStep`; see [guarded agent construction](../../packages/breakwater/src/agent/index.ts), `OUTPUT_PROCESSOR_FORBIDDEN_HOOK_SET`.

Core 1.73.0 also runs `processToolResult` on durable tool-result paths. That hook can block a tool result, but it is not a result-phase answer gate. Breakwater refuses application `processToolResult` hooks because of their ordering; this proposal preserves that restriction.

### Final result processing does not enforce denial

Core 1.73.0’s `map-final-output` step calls `runDurableFinishSideEffects`. That function calls `ProcessorRunner.runOutputProcessors`, catches every thrown error including `TripWire`, logs a warning, derives output text from the shared `MessageList`, and then flushes that list to memory. A result-phase denial does not reach the subscriber as a result-phase tripwire.

The result hook can mutate the same list. `ProcessOutputResultArgs` exposes `messages`, `messageList`, `result`, and `abort`; `ProcessorRunner.runOutputProcessors` reconciles a returned message array into the list by removing omitted response IDs and replacing returned messages. Direct list mutations survive a subsequent throw: the runner stops recording and rethrows without rollback, and the durable finalizer catches the throw.

Returning a replacement array and then throwing cannot work because a thrown invocation returns no array. Mutating `args.messageList` before `args.abort()` can work: the standard loop retains its tripwire while the durable finalizer sees the modified list. This is source-derived behavior in core 1.73.0’s `ProcessorRunner.runOutputProcessors`; a real-loop probe must establish source tagging and saved-message behavior for the chosen rewrite.

Removing the response is insufficient for the result envelope. `map-final-output` replaces `finalText` only when `finishResult.outputText` is truthy. An empty replacement leaves the original step text in `output.text`; a nonempty replacement changes the last step’s text but leaves earlier `output.steps` text and other generated fields. Its `finish` event includes `finalOutput.output` and `stepResult`, so it can expose those copies too.

Breakwater currently does not perform this rewrite. Its `PolicyEngine.processOutputResult` calls `args.abort()` for a denial and throws a fixed error for an evaluation failure. [GuardedAgent.listOutputProcessors](../../packages/breakwater/src/agent/index.ts) resolves application output processors, then the policy engine, then memory output processors. A preceding processor can throw before the policy engine runs; relying on the policy hook alone therefore does not establish positive authorization.

### Saves before the result decision

Core 1.73.0’s `flushMessagesBeforeSuspension` saves the current list before approval suspension. The background callbacks in `createDurableToolCallStep` also flush it, and `createDurableLLMMappingStep` can flush on continuation when `savePerStep` is enabled. All precede `runDurableFinishSideEffects`; the suspension helper swallows save exceptions.

`SaveQueueManager.persistUnsavedMessages` drains unsaved messages before calling `memory.saveMessages`, and `enqueueSave` catches storage failures. A memory wrapper can prevent an unsafe database write, but throwing there is not a reliable workflow-failure signal. Silently discarding a provisional write also consumes the queue’s unsaved tracking; final commit must explicitly write or re-enqueue the approved projection.

For current guarded FlowSafe runs, `savePerStep` is unreachable through the accepted configuration. [GUARDED_DURABLE_CALL_OPTIONS](../../packages/flowsafe/src/agent-runner/durable-agent-runner.ts) refuses it per call and the constructor validates static defaults through the same mapper. [GuardedAgent](../../packages/breakwater/src/agent/index.ts) constructs defaults without it. Core preparation merges defaults and call options, then reads `execOptions.savePerStep`; raw unguarded agents can enable it.

Guarded construction also disables background dispatch, and FlowSafe’s runtime does not install a background-task manager; see [the durable-agent guide](../durable-agents.md#add-background-tasks). That limits current reachability rather than making callback saves a valid future bypass. A new guard must intercept their shared memory-save boundary or keep background dispatch refused.

The `saveMessages` boundary does not cover working-memory resources or vector metadata. In core 1.73.0, `Agent.listMemoryTools` resolves memory through `getMemory` and exposes its `listTools` entries. `MockMemory.listTools` supplies `updateWorkingMemory` when enabled; `MockMemory.updateWorkingMemory` calls the memory store’s `updateResource` before finalization. The base `MastraMemory.listTools` returns no tools, so reachability depends on the concrete memory binding.

Core 1.73.0’s `MastraMemory.getOutputProcessors` adds `SemanticRecall` when semantic recall, a vector store, and an embedder are configured. `SemanticRecall.processOutputResult` upserts message text as metadata `content`, independently of `saveMessages`. Breakwater’s `GuardedAgent.getMemory` checks title generation, not these features; `listOutputProcessors` retains memory output processors. Guarded agents can therefore reach both stores with a configured memory binding. The repository uses core `MockMemory` and has no declared or locked `@mastra/memory` dependency; these paths do not require that package. This reachability assessment comes from source reading, not a configured real-loop run.

The strict oracle includes both stores. FlowSafe must stage or refuse generated working-memory writes until the run-wide decision, and run semantic indexing only on the approved projection. A thread-save wrapper alone cannot make either promise. The bounded release must refuse unsupported working-memory writers rather than claim that its save gate intercepts them.

### FlowSafe retains workflow results in D1

Plain core `DurableAgent.executeWorkflow` deletes non-suspended run snapshots after settlement. FlowSafe does not take that execution path. [FlowsafeDurableAgent.executeWorkflow](../../packages/flowsafe/src/agent-runner/durable-agent-runner.ts) calls `RunnerRuntime.start`, and its `deleteRunSnapshots` override throws to preserve deployment-scoped retention ownership.

Core’s default durable persistence predicate excludes terminal statuses, but [RunnerRuntime.#reconcileTerminalState](../../packages/flowsafe/src/do-runner/runtime.ts) repairs the retained snapshot with `terminalStateUpdate`. On success, [terminalStateUpdate](../../packages/flowsafe/src/do-runner/run-terminal-state.ts) writes `result.result`. `summaryFromSelectedSnapshot` and `summarizeState` read that value back as `RunSummary.result`.

[FencedWorkflowsStorageD1.persistWorkflowSnapshot](../../packages/flowsafe/src/do-runner/fenced-workflows-d1.ts) delegates ordinary post-admission writes to `WorkflowsStorageD1`. Installed `@mastra/cloudflare-d1` 1.4.0’s `WorkflowsStorageD1.persistWorkflowSnapshot` stores the supplied snapshot JSON. Core 1.73.0’s `persistStepUpdate` writes workflow state and step context; `pruneAgentLoopSnapshot` retains the final result rather than serving as a confidentiality filter.

Thus a successful FlowSafe durable run retains its result, including `output.text`, `output.steps`, and `messageListState`, until configured retention or operator deletion removes it. Running and suspended snapshots are also at-rest surfaces. This conclusion follows the write/read paths; confirm it with the D1 probe below. [purgeExpiredWorkflowRuns](../../packages/flowsafe/src/do-runner/d1-storage.ts) owns terminal workflow retention; `purgeExpiredThreads` separately deletes expired thread messages and threads.

### Observation and history use different stores

[createHostPubSub](../../packages/flowsafe/src/do-runner/pubsub.ts) returns core’s in-process `EventEmitterPubSub`. A host can inject a durable or cache-backed `PubSub`; the producer still publishes raw model chunks before subscriber processing. Core 1.73.0’s `CachingPubSub.subscribe` replays events from `getHistory`, including their original payloads.

[The host stream route](../../packages/flowsafe/src/agent-host/thread-host.ts), `createAgentThreadHost`’s stream branch, permits cached replay when a live registry entry is absent. Inherited core 1.73.0 `DurableAgent.observe` supplies output processors only from its instance registry. With that entry, replay traverses subscriber processors; without it, observation has no output processor list and does not reconstruct one. Stream policy processing also does not mean that arbitrary nested `finish` or `step-finish` fields receive a result-level decision.

Core 1.73.0’s `DurableAgent.stream` and `observe` also pass the producer’s live `MessageList` to `createDurableAgentStream`. Its subscriber `MastraModelOutput` exposes `messageList`, resolves `response.dbMessages` from that list on tripwire, and returns its messages through `getFullOutput().messages`. These in-process APIs can expose denied content beyond the released prefix without another publication. The FlowSafe host stream branch serializes only `observed.fullStream` as NDJSON; that HTTP route does not serialize the returned message APIs. A separate approved projection is required for those APIs, and no current placement supplies it.

[FlowsafeDurableAgent.resumeViaRuntime](../../packages/flowsafe/src/agent-runner/durable-agent-runner.ts) rebuilds the registry through `#rehydrateRegistry`, preserving the resolved output processors. It observes from the current history length, so that internal resume observer processes future events, not the earlier history. A later observer starting at an older offset can process that history while the rebuilt instance entry remains present.

Thread `withInitialHistory` instead calls `memory.recall` through core 1.73.0’s `AgentThreadStreamRuntime.#loadThreadHistory`. It can expose saved suspension, callback, per-step, and final messages. Topic trimming on successful thread completion in `#trimSavedRun` concerns the thread topic, not D1 snapshot retention or a proof that run-topic replay contains no denied text.

## Compare placements against the oracle

No current finalization-only placement closes the full oracle. The table describes each placement implemented correctly at its stated boundary, not an assertion that it ships today.

Legend: **Y** closes that surface; **P** covers only part, or requires the condition explained below; **N** leaves it open. Snapshot coverage includes earlier snapshots; event coverage includes both `finish` and `step-finish`. A final gate cannot retract earlier writes or events.

| Placement | Final thread | Earlier thread | Run result | Live observe | D1 snapshots | Terminal payloads | Topic history | Initial history | Other memory stores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Before final persistence only | Y | N | N | N | N | N | N | P | N |
| Before host result envelope only | N | N | P | N | N | N | N | N | N |
| Before final persistence and result construction | Y | N | Y | N | P | P | N | P | N |
| Shared `MessageList` rewrite in result hook | P | N | P | N | P | P | N | P | P |
| Shared list rewrite before result-hook abort | P | N | P | N | P | P | N | P | P |
| Producer `processOutputStep` tripwire only | N | P | N | N | N | N | N | N | P |
| FlowSafe gate ahead of or replacing `map-final-output` | Y | N | Y | N | P | P | N | P | P |
| Guarded memory-save gate on every flush | P | Y | N | N | N | N | N | P | N |
| FlowSafe projection of `RunSummary.result` | N | N | Y | N | N | N | N | N | N |
| Snapshot-store projection before D1 writes | N | N | P | N | Y | N | N | N | N |
| Refuse replay without verified registry processors | N | N | N | P | N | N | P | N | N |
| Subscriber stream gate with bounded hold-back | N | N | N | P | N | P | N | N | N |
| Producer pub/sub gate before publish and caching | N | N | N | P | N | Y | Y | N | N |
| Upstream failure or skipped flush on result `TripWire` | Y | N | P | N | P | P | N | P | P |
| Producer commit barrier across release and storage | Y | Y | Y | P | Y | Y | Y | Y | Y |

The **Y** entries assume a finalized producer verdict for the applicable policies and fail-closed admission at that boundary, including evaluator failures. They are not promises about core’s current exception handling. The final row requires staging and a defined recovery contract; it is not available by adding an ordinary output processor.

Both shared-list rewrite rows have partial final-thread coverage: an earlier application result processor can throw before the rewrite runs, and `runDurableFinishSideEffects` then flushes the raw list. Publication filtering and a storage/release commit barrier have partial live-observation coverage because returned message APIs still reference the producer list. They require FlowSafe’s separate message-API projection below. Other-memory coverage is partial where a placement can stop final semantic indexing but cannot undo an earlier working-memory write; the complete barrier must include resource and vector writes. The memory-save row has partial final-thread and initial-history coverage because core 1.73.0’s `MessageHistory` output processor writes the final messages through its storage’s `saveMessages`, not through the memory’s.

Skipping the final flush alone has partial run-result coverage: it leaves `map-final-output` result construction reachable. Only producer failure before that construction prevents a success result; either option still leaves earlier snapshots and events exposed.

### Before persistence, before the envelope, or both

A gate before the final `saveQueueManager.flushMessages` in core 1.73.0’s `runDurableFinishSideEffects` protects that write only. A gate in FlowSafe’s host protects the projected response only. Combining them must sanitize the entire final result, not merely `text`; otherwise `map-final-output` retains raw steps and message state.

Even that combination misses `flushMessagesBeforeSuspension`, callback saves, earlier `persistStepUpdate` snapshots, deferred step events, and already-published topic chunks. Final-only snapshot coverage is partial because the safe terminal write cannot erase exposure through an earlier stored version. Initial-history coverage is partial until every memory-write path is guarded.

### Rewrite the shared list, alone or before abort

A result processor can replace denied assistant content with a fixed, nonempty refusal message before aborting. In core 1.73.0, the list mutation affects the final memory flush despite the catch, while abort preserves the standard loop’s tripwire. Returning sanitized messages without abort instead lets both loops report apparent success unless a separate denial outcome exists.

Neither form cleans the earlier accumulated steps, reasoning, deferred payloads, or snapshots. Removing messages triggers the `map-final-output` empty-text fallback; a fixed replacement avoids that fallback for the last text but does not make the surrounding result safe. An earlier application processor’s error can also prevent this policy hook from running. This is a useful defensive rewrite, not the primary result gate.

### Producer step gate

Core 1.73.0’s `processOutputStep` is early enough to stop ordinary server-tool execution and its ensuing approval save when it blocks that step. It is late for model chunk publication. Its non-retry tripwire retains the list and reaches finalization, so a tripwire alone protects neither the eventual saved response nor the result.

A step processor that throws a safe ordinary error does not establish workflow failure either. After retry/fallback exhaustion, `emitFatalErrorBail` returns an error-reason output that the iteration graph accepts; `map-final-output` does not reject that reason before finish side effects. Source reading therefore indicates possible subscriber failure alongside workflow success and a final memory flush. The authoritative-outcome probe below must pin the complete FlowSafe outcome. Adding explicit step redaction requires sanitizing raw LLM output, step content, and subsequent serialized state; mutating only the list does not change `textDeltas` in `createDurableLLMExecutionStep`.

### FlowSafe workflow replacement

Core 1.73.0 declares `DurableAgent.createWorkflow` and `executeWorkflow` as protected, `@internal` methods. FlowSafe already overrides execution; it can additionally supply a workflow with an uncaught final gate before finish side effects. Wrapping `executeWorkflow` after `RunnerRuntime.start` returns is too late: the core map has already flushed and emitted `finish`.

A gate placed before the final map can fail the workflow and prevent its final save, result, and `finish`. It cannot sanitize a workflow snapshot recorded before that gate or a deferred `step-finish` already published. It must also prevent the later caught result-processor pass from changing or invalidating the approved candidate; finalization must consume the approved projection once rather than re-evaluate asynchronously inside core’s swallowed-error path.

Replacing the final map gives FlowSafe control of empty answers, all step copies, denial status, and publication order. It couples FlowSafe to core’s workflow topology, serialized state schemas, registry reconstruction, processor order, and finish-side effects. Do not imply that the protected factory offers a public “insert step” operation; the exact replacement mechanism needs the compatibility probe below.

In core 1.73.0, `map-final-output` is an inline map in `DurableAgenticLoopBuilder.build()`. The builder is not exported from `@mastra/core/agent/durable`, and its protected iteration hooks do not include a final-map hook. Replacement requires recomposing the outer initialization, iteration, continuation, finalization, and scorer graph, or mutating the built graph; neither is a supported finalizer extension. Preserve `init-iteration-state`, the iteration `dowhile` and continuation predicate, and workflow options including `shouldPersistSnapshot`, `pruneSnapshot`, and `allowUnclaimedResumes`.

Preserve workflow id `durable-agentic-loop` and step id `map-final-output`. Core’s `DurableAgent.resume`, `#loadRecoverableSnapshot`, and `listActiveRuns` use `DurableStepIds.AGENTIC_LOOP`; `Agent.listSuspendedRuns` and suspended-tool-call lookup also search that workflow name. Recovery’s `finishPublishedBeforeCrash` reads the final-map step status. Renaming either id breaks those contracts.

The inline final map reads the run’s pub/sub from `PUBSUB_SYMBOL`, which `@mastra/core/workflows` does not export. In contrast, `@mastra/core/agent/durable` publicly exports `runDurableFinishSideEffects`, `emitFinishEvent`, `executeDurableAgentScorers`, and the LLM-execution, tool-call, LLM-mapping, and background-task step factories. These exports reduce duplicated step logic, but do not expose the missing final-map or pub/sub hook.

### Memory, host, and snapshot projections

A gate on the guarded agent’s resolved memory `saveMessages` intercepts the common `SaveQueueManager` flush paths. It does not intercept core 1.73.0’s `MessageHistory` output processor, whose `processOutputResult` calls `persistMessages` and writes with its storage’s `saveMessages`; the gate covers the final save only if it also intercepts that writer, or if memory output processors run only after approval, as the recommended finalizer orders them. It must withhold provisional generated content unless the relevant output transaction positively allows it, and preserve approved caller input and required tool-call structure. It does not mutate the producer’s result or serialized workflow state. Core’s queue swallows failures, so a separate producer failure must stop the run when the guard cannot evaluate.

[envelopeFor and statusFor](../../packages/flowsafe/src/agent-host/thread-host.ts) currently return the summary without output redaction. [publicRunSummary](../../packages/flowsafe/src/host-kit/do-response.ts) permits `result` rather than checking generated fields. A proposed host projection must omit the whole unapproved result, and the runtime’s direct summary path needs the same rule; an HTTP-only fix leaves lower-level consumers exposed.

The gated finalizer constructs the approved workflow result. Keep [runtime.ts](../../packages/flowsafe/src/do-runner/runtime.ts)’s `summarizeState` workflow-generic: the proposed content-free contract/verdict marker lets it omit legacy unverified agent-loop results without parsing answer, steps, or message state. Other workflows keep their result contracts.

A projection in `FencedWorkflowsStorageD1.persistWorkflowSnapshot` can remove generated content before each D1 write. It must cover ordinary writes, initial admission, terminal repair, nested step state, and any write path bypassing that method. Merely redacting `snapshot.result` misses `context`, `value`, and serialized message state from core 1.73.0’s `persistStepUpdate`.

Stripping unapproved assistant state from resumable snapshots can destroy model context and tool continuation. A store-only filter must not advertise transparent durable resume without proving it. Throwing from the store also cannot establish the desired terminal outcome by itself: core may retain an earlier snapshot and FlowSafe’s authoritative terminal repair still requires storage. Prefer producer-approved state over schema-wide storage redaction.

### Replay refusal and publication gates

Refusing cached replay without verified output processors closes the registry-absence bypass in core 1.73.0’s `DurableAgent.observe`. Rebuilding processors must use trusted run identity and current authorization, not replay application input hooks. Processor presence alone does not certify arbitrary terminal payloads or reconstruct a result-only decision from an offset in the middle of a stream.

That refusal covers cached-event delivery, not access to the producer list through an existing returned object. FlowSafe must return a detached, withheld message projection from guarded `stream()` and `observe()`, including `messageList`, `response.dbMessages`, and `getFullOutput().messages`. Populate it only from the exact approved projection; never expose or later reconnect it to the live producer list. This adapter is a proposed FlowSafe responsibility, not current subscriber behavior.

A producer-side pub/sub wrapper can withhold content before `emitChunkEvent`, `emitFinishEvent`, `emitIterationCompleteEvent`, and caching. It must also cover the thread broadcast and preserve lifecycle events needed for settlement. Filtering a subscriber or `getHistory` after caching leaves raw stored history; filtering publication closes that storage surface only when every producer uses the wrapper.

Changing core to fail before finalization on a result `TripWire` prevents final thread persistence, success result construction, and final `finish`. Skipping only the final flush protects that write but leaves result and event construction reachable. Both leave pre-gate saves, running/suspended snapshots, deferred step events, and raw topic history. Ordinary evaluator errors must receive the same safe outcome rather than remaining in the broad catch.

## Recommended delivery without a core change

Ship a bounded finalization improvement first, using FlowSafe’s internal workflow override and a Breakwater-owned result evaluator. Keep the full at-rest and replay guarantee explicitly unimplemented until producer staging and recovery work pass the strict oracle.

The proposed local integration has these responsibilities:

1. Breakwater evaluates a detached final candidate containing the result answer, generated response messages, and per-step reasoning. It exposes a host-compatible positive verdict without weakening the guarded handle. Reuse `PolicyEngine`’s policy selection, fixed error reasons, auditing, and ordered evaluation rather than adding a second policy implementation.
2. FlowSafe replaces final mapping with a gate that owns candidate selection, sanitization, and denial settlement. It runs application result processors, completes producer terminal classification, and then evaluates the detached candidate with a deadline. After positive allow, memory output processors consume the approved projection, followed by the explicit thread flush and safe `finish` publication. A denial or error fails the workflow with a fixed safe error before those writes. Error-reason LLM outputs also refuse finalization; an emitted subscriber error is not an allowed candidate.
3. Breakwater owns the withholding memory wrapper at `GuardedAgent.getMemory`; FlowSafe activates it only for a trusted, host-bound durable run. Standard-loop resolutions use ordinary memory and need no FlowSafe commit. Core preparation, FlowSafe’s `#rehydrateRegistry` proxy, and finish-time `resolveRuntimeDependencies` all resolve this seam, so the binding must survive each path. FlowSafe’s finalizer privately holds the commit capability that bypasses withholding; applications receive no commit API or caller-settable activation flag. It explicitly writes approved messages because earlier withheld saves may drain core’s queue. Generated working-memory writes require staging or admission refusal.
4. FlowSafe constructs approved workflow results and returns detached approved message-API projections. Runtime and host summaries use the persisted marker to omit unapproved or legacy results. Guarded observation refuses replay when processor reconstruction and trustworthy context are unavailable, and excludes unapproved terminal payload fields rather than assuming `processOutputStream` evaluates them.
5. Breakwater owns a durable-scoped defensive mask-before-evaluation processor if the caught result path remains reachable. Its synchronous mask runs before application result processors, so their throws leave the producer list safe. Breakwater routes those hooks to the detached candidate before the gate evaluates it; inserting a mask into the existing list alone would instead give the hooks masked input. Restoration requires positive authorization of that exact candidate. This processor is excluded from the already-approved finalization pass; it is defense in depth rather than permission to use the original final map.

Core 1.73.0’s public `runDurableFinishSideEffects` accepts no processor-list override. It reads the registry’s full `outputProcessors` list inside its catch, so calling it after approval repeats application hooks, policy evaluation, and any mask processor. FlowSafe’s guarded finalizer instead performs the applicable thread-creation and queue-flush side effects directly, without that processor pass. Breakwater’s `GuardedAgent.resolveTitleGenerationConfig` disables title generation; preserve that behavior. Unguarded runs pass through core’s finalization and title side effects. Memory processors must not change approved generated content; reject unsupported content-changing processors before admission rather than let saved and returned projections diverge.

These are proposed changes, not existing extension guarantees. The local factory replacement must use the same workflow identity registered in `RunnerRuntime`; resume must reconstruct the same gated graph. No lower-level `generate`, `stream`, preparation, signal drain, or resume entry may choose the old finalizer for a guarded run.

Current `createFlowsafeDurableAgent` registers only when `runtime.workflowIds()` lacks the loop id, so the first registered graph wins. That graph can come from core or another agent, including through `init()`’s runtime-bound `createWorkflow`. Registration must verify the existing graph’s gated contract and refuse a mismatch; matching the id alone is insufficient. The shared gated graph passes through, per run, agents without the guarded protocol while enforcing the gate for guarded agents.

Denial must remain sticky for a candidate generation. A subscriber’s stream or held-tail denial cannot be overwritten by a later positive re-evaluation, especially with nondeterministic classifiers. Publication acknowledgment only enqueues a subscriber chunk; it does not await policy processing. `PolicyEngine.#forwardUngated` can classify held tails on `finish`, which core emits after finish side effects, so waiting for an arbitrary observer is not a commit barrier.

FlowSafe owns a producer terminal-classification barrier before the final result decision and commit. The producer applies the stream policies and classifies remaining answer/reasoning tails before sealing the candidate generation. Terminal classification and aggregate evaluation share a configured host deadline; timeout, missing decision, or incomplete coordination denies that generation. Observers reuse the finalized producer verdict instead of independently authorizing it, including late observers whose chunks process after final evaluation. Any content transformation creates a new candidate generation and requires its own decision.

Persist content-free coordination state with the run in snapshot lifecycle metadata: contract version, candidate generation, policy/context identity, sticky denied state, terminal-classification completion, and final verdict (`withheld`, `allowed`, or `denied`). A final result is approved only when its generation matches an `allowed` marker with completed terminal classification. Preserve that marker through suspension, terminal repair, and pruning; restore it before resume or observation. `resumeViaRuntime` rebuilds fresh processor state and observes from the current history length, so in-memory state is insufficient. Missing or incompatible metadata remains withheld; a valid persisted allow survives eviction.

The marker contains no candidate text or serialized messages. FlowSafe binds it to the workflow’s approved projection and run identity; a marker copied from another run or candidate generation cannot authorize release. Legacy snapshots have no marker and remain unverified.

Subject to the replacement and coordination probes, this layer protects final thread writes, approved `RunSummary.result`, and final `finish` content. The scoped memory interception protects pre-gate thread writes and new `withInitialHistory` content; returned message APIs require the separate detached projection. Replay refusal prevents absent-registry delivery, and terminal-event projection protects those subscriber-facing fields. Approved-only semantic indexing protects new vector metadata; unsupported working-memory writers are refused. The layer does not remove earlier raw D1 snapshots, raw topic history, earlier deferred step payloads at the producer, or released stream prefixes.

For deployments requiring the strict oracle immediately, refuse admission of guarded durable runs with applicable output policies until the complete barrier is available. Do not offer a “strict” switch that silently means final-result filtering while persisting unapproved intermediate state. Existing unguarded runs keep their documented contract.

### Why the other placements lose

Each alternative has a narrower role than the primary finalization gate:

| Alternative | Reason it cannot be the primary solution |
| --- | --- |
| Result-hook abort alone | Core catches it and proceeds with the original list and result |
| List mutation alone | Empty replacement falls back to raw text; accumulated steps and earlier exposure remain |
| Non-retry step tripwire alone | Ends iteration but retains messages and still reaches final flush |
| Memory gate alone | Protects thread storage, not results, snapshots, or topic publication; queue errors are swallowed |
| Host projection alone | Executes after persistence and event publication; direct runtime consumers can bypass an HTTP projection |
| Snapshot redaction alone | Leaves threads and streams exposed and may make resume incorrect |
| Registry-presence check alone | Does not certify payload fields, stored history, or earlier output decisions |
| Hold-back alone | Operates at subscribers; the producer topic remains raw |
| Final core tripwire fix alone | Prevents finalization but misses earlier saves, snapshots, and events |

The workflow replacement requires unsupported outer-graph recomposition or graph mutation, stable workflow and step ids, pub/sub access, and guarded flush replication. Public step and event exports reduce duplication, but the maintenance cost cannot be bounded until the replacement probe establishes a viable mechanism. A shared-list rewrite avoids graph replacement but cannot establish producer failure or full result safety. If the replacement probe fails, refuse guarded durable admission with applicable output policies; retain memory withholding, result/message projections, and replay refusal as independent defenses, not as a successful-run gate. Record the internal dependencies and compatibility probes in [the maintainer guide](../maintainer-guide.md), following its existing coupling records. A passing newest-core canary does not expand the declared supported peer range.

## Complete the guarantee at the producer

The upstream design must gate publication and persistence, not merely change the final exception handler. It needs a producer-owned output transaction understood by the durable loop, memory saves, snapshot serialization, and topic publication.

The proposed transaction starts withheld, records candidate identity and policy context, and permits release only for the exact evaluated projection. A mutation after allow invalidates the verdict. Application processors complete before policy evaluation; memory processors and finish side effects consume the approved projection without another content-changing pass. Evaluation errors produce safe terminal metadata, never classifier exception text or the candidate.

For strict run-wide final release, keep generated text, reasoning, step payload copies, and raw messages out of durable snapshots and topic history until allow. Preserve non-content lifecycle events so observers and thread control can settle. Publish sanitized step and finish payloads and commit the approved thread projection after the final decision; never cache raw chunks and then rely on later deletion.

The producer transaction also controls working-memory resource updates and semantic-recall vector upserts. Stage generated resource changes or refuse the memory tool until approved; index approved messages after evaluation. Separate message-API projections remain necessary even when publication and storage use this barrier, because the internal producer list contains withheld context.

This conflicts with transparent recovery when core needs unapproved generated context to resume after eviction. An encrypted or hidden quarantine is still at-rest storage and does not satisfy this proposal’s strict oracle. Choose an explicit recovery contract: reconstruct from safe committed context without repeating authorized side effects, or refuse resume when required withheld context is lost. The recovery probe must resolve this before promising strict durable execution.

An alternative is a narrower per-step commit contract in which each allowed step becomes permanent before a later result decision. That preserves more recovery behavior, but weakens the run-wide oracle and must be a separately documented mode. Do not redefine final denial after the fact to exclude already committed output.

### Hold-back changes release timing, not producer storage

Current `holdBack: true` retains the policy-requested trailing window per answer or reasoning segment. `PolicyEngine.processOutputStream` classifies held tails at channel end or `finish`, drops them on error, and releases previously allowed prefixes. Even an infinite segment window can release at a segment end before a later run-wide result denial; see [streaming hold-back](../../packages/breakwater/README.md#understand-streaming-hold-back).

On core 1.73.0’s durable path the topic contains raw model chunks regardless of hold-back. Subscriber-held text cannot prevent its storage there. Strict final release therefore buffers across segments and tool steps, adds memory cost proportional to the withheld output, delays caller-visible content until allow, and needs a configured size limit that refuses oversized candidates safely.

The existing durable-loop regression in [durable-agent-runner.test.ts](../../packages/flowsafe/src/agent-runner/durable-agent-runner.test.ts), `durable caller-visible text`, confirms that stream denial leaves only the released prefix in subscriber text and result. It does not establish safe producer persistence. Text already published to a topic or released to a subscriber cannot be retracted by a later gate.

## Gate errors must settle and remain withheld

The producer must own a bounded evaluation lifetime. Use the existing evaluator timeout semantics where applicable, plus a host deadline for the aggregate gate. On timeout, cancel evaluator work where supported, invalidate the candidate generation, and refuse later completion; racing a promise without rejecting late mutations is insufficient.

Current and proposed error behavior differ by placement:

| Placement of a gate error | Current core/runtime outcome | Required fail-closed mechanism |
| --- | --- | --- |
| `processOutputResult` | Core 1.73.0’s `runDurableFinishSideEffects` catches it; workflow normally succeeds and flushes the current list | Mask before any await; restore only on positive allow; separately fail the producer finalizer and suppress raw result/step copies |
| `processOutputStep` | Core rethrows into model retry/fallback; exhaustion publishes error but returns error-reason output, so final mapping remains reachable | Use a fixed safe error and make the producer final gate reject error-reason output; keep earlier saves and publication withheld; bound retries and evaluation time |
| FlowSafe final workflow gate | An uncaught step error can produce workflow `failed`; `executeWorkflow` emits terminal error from the returned failed summary | Gate before finish side effects; persist safe terminal state through runtime reconciliation; publish terminal error even after a subscriber detaches |
| FlowSafe post-run host projection | The producer may already be `success`; projection failure does not undo storage or release | Return no unapproved result; use producer-owned failure for the actual run rather than treating an HTTP error as settlement |
| Memory or snapshot write gate | Core queue catches memory failures; snapshot failures can leave an older authoritative state | Withhold content at the write boundary; use runtime lifecycle failure and recovery, not a swallowed write exception as the completion signal |

Core 1.73.0’s `MastraModelOutput` error branch rejects pending result promises and marks the subscriber failed. Its tripwire branch instead resolves subscriber output from the released prefix and terminates that subscriber; neither behavior alone proves the workflow’s terminal outcome.

An ordinary subscriber processor exception is weaker still: core 1.73.0’s `ProcessorRunner.processPart` logs it and continues with the part. Breakwater converts stream evaluator errors to `abort` in `PolicyEngine.#evaluateStreamChannel`. A new host or producer gate must explicitly preserve fail-closed conversion rather than relying on generic stream exceptions.

[RunnerRuntime.start](../../packages/flowsafe/src/do-runner/runtime.ts) reconciles terminal state, reads the authoritative outcome, settles start reservations, and clears active-run bookkeeping in `finally`. [FlowsafeDurableAgent.streamUntilPersisted and executeWorkflow](../../packages/flowsafe/src/agent-runner/durable-agent-runner.ts) resolve the persistence waiter when runtime returns a summary, including a failed summary; execution exceptions reject it. Temporary waiter state clears in `finally`. The method’s name does not promise a final-result memory commit.

Core 1.73.0’s `AgentThreadStreamRuntime.#watchThreadRunCompletion` waits for stream completion, publishes `run-completed`, removes active records, and releases the thread lease or transfers it to pending work. It computes `persisted` from subscriber status; ordinary active suspension follows its earlier `run-suspended` branch and retains the blocking run. [FlowsafeDurableAgent.resumeViaRuntime](../../packages/flowsafe/src/agent-runner/durable-agent-runner.ts) binds resumed thread legs to the runtime outcome, preserving re-suspension and completing terminal or failed legs.

[createAgentThreadHost.finishLifecycle](../../packages/flowsafe/src/agent-host/thread-host.ts) settles the resource reservation, abandons approvals, releases run ownership, finalizes journal bookkeeping, and calls `completeTerminalCleanup`. The proposed gate must use that lifecycle path, including recovery alarms when cleanup cannot complete immediately. Do not clean registry state before publishing terminal error or mark cleanup complete before ownership release succeeds.

With readable/writable storage and a functioning pub/sub, the proposed denial path must reach `failed`, release the lease and ownership, remove blocking records, and settle waiters and observers. Existing source paths support those steps, but the gate-specific lifecycle probe remains required. If storage or publication itself is unavailable, persist or retain recoverable cleanup intent, refuse further content release, and bound observer waiting; do not claim synchronous settlement through an infrastructure outage.

## Migration and operator procedure

Historical records lack a result-gate verdict. A saved assistant message cannot prove which output policies, evaluator version, principal context, or terminal classification applied when it was produced. Current policies can identify present violations, but cannot reconstruct the historical decision reliably.

Before enabling the stronger contract, operators should perform a deployment-scoped procedure:

1. Pause new affected starts and identify active or suspended runs before touching their stored state. Separate historical terminal records from records needed for resume.
2. Inventory thread messages, terminal and suspended workflow snapshots, working-memory resources, semantic-recall vector metadata, and injected pub/sub history. Use run/thread/resource identifiers and trusted deployment ownership; timestamps alone do not prove a message belongs to a particular run.
3. Re-evaluate saved assistant content with current policies and reconstructed trusted context when available. Treat missing context, evaluator errors, timeouts, and incomplete generated-field extraction as unverified. Keep report output free of candidate content.
4. Remove or replace violating thread content using an operator-controlled memory procedure. Preserve tool-call/result consistency; when targeted attribution is unreliable, retire the affected thread instead of inventing a message-to-run mapping. Re-evaluate or retire affected working-memory resources and delete or rebuild affected vector entries through their storage owners. Resource-scoped memory is shared across threads, so thread retirement alone does not remove it.
5. Apply configured `purgeExpiredWorkflowRuns` and `purgeExpiredThreads` for their eligible records. They are TTL mechanisms, not content classifiers or immediate per-run redactors. Handle active/suspended snapshots through explicit cancellation and lifecycle cleanup before deletion; purge injected pub/sub storage separately.
6. Invalidate replay caches and application result caches, then enable the gate for new run generations. Unverified legacy status reads lack a matching approved contract/verdict marker and omit the result or refuse inspection rather than returning it as approved. Do not assign an allow marker merely because a historical workflow succeeded.

The retention functions live in [d1-storage.ts](../../packages/flowsafe/src/do-runner/d1-storage.ts); operational boundaries are described in [FlowSafe memory and retention](../../packages/flowsafe/README.md#memory-and-retention). Snapshot retention does not delete thread messages, and idle-thread retention does not release permanent thread/resource ownership. `purgeExpiredThreads` leaves `mastra_resources` working memory untouched; neither thread nor workflow TTL cleanup purges an external vector store.

Deletion cannot retract responses already delivered or copies outside deployment control. Re-evaluating a terminal result cannot repair raw running snapshots or historical topic events that were previously exposed. A rollout may restrict the new guarantee to new generations only if the docs explicitly exclude unverified historical data.

## Test strategy and rollout

Implementation starts with real durable-loop regressions against the current code. Use unique forbidden markers in deterministic model output and fixed safe expected results. A red run must fail because the marker appears on the named surface, not because construction, imports, or authorization failed. Record the pre-fix log before implementing each closure.

Each surface has an owning observable regression:

| Oracle surface | Real-loop regression and harness foundation |
| --- | --- |
| Final thread save | Result-only denial after stream allows; recall the thread and inspect assistant parts. Extend [durable-agent-runner.test.ts](../../packages/flowsafe/src/agent-runner/durable-agent-runner.test.ts) and confirm persisted behavior in the FlowSafe harness |
| Earlier thread saves | Emit marked answer/reasoning before approval, inspect memory while suspended, then deny on resume. Extend [agent-gate-round-trip.test.ts](../../packages/flowsafe/src/agent-runner/agent-gate-round-trip.test.ts); keep guarded background and `savePerStep` refusals, and exercise shared callback saves with a deliberately unguarded core fixture |
| Run result | Inspect direct runtime summary and authenticated status/start envelopes, including earlier steps and message state. Require a matching approved marker; omit legacy results and refuse unknown contracts, mismatched generations, or markers from another run. Extend [thread-do-routes.real-agent.test.ts](../../packages/flowsafe/src/signals/thread-do-routes.real-agent.test.ts) with the real durable loop |
| Live observation | Record initial subscriber and concurrent `observe()` content under stream denial, result-only denial, and terminal hold-back denial. Inspect returned `messageList`, `response.dbMessages` on tripwire, and `getFullOutput().messages`, including content beyond the released prefix; separately verify the host’s NDJSON `fullStream`. Extend the existing `durable caller-visible text` cases without replacing their released-prefix contract |
| Other memory stores | Enable working memory and semantic recall on a guarded core memory binding; inspect resource writes before suspension/finalization and vector metadata on denial and allow. Verify safe refusal or staging of the memory tool, approved-only indexing, and migration cleanup independently of thread TTL |
| D1 snapshots | Inspect raw rows after model execution, suspension, denial, success, and fresh-runtime readback. Extend [fenced-workflows-d1.test.ts](../../packages/flowsafe/src/do-runner/fenced-workflows-d1.test.ts) for storage-unit behavior and the real D1 harness for fidelity; inspect nested state rather than only returned summaries |
| Terminal stream payloads | Collect complete `finish` and `step-finish` events across a tool step and denial, including `_durableStepContent`, reasoning, and steps. Extend real-agent route tests; do not infer event safety from subscriber `.text` |
| Topic history and replay | Read the actual publication/cache history before cleanup; replay with the registry present, absent, and rebuilt, and from a midstream offset. Extend durable-runner rehydration cases with a recording cache-backed pub/sub that stores the producer’s actual events |
| Initial thread history | Subscribe with `withInitialHistory` after suspension and completion; assert no forbidden marker in recalled messages. Extend real-agent thread route tests using the same memory as the durable loop |

Cross those regressions with explicit denial, ordinary evaluator throw, missing decision, never-resolving evaluator under a fake deadline, and hold-back terminal classification. Include allowed output, empty allowed answer, reasoning-only denial, multiple tool steps, retry exhaustion, read-only memory, fresh-isolate resume, and legacy result inspection where their contracts differ. Do not add a test-only production export to reach a boundary.

For lifecycle coverage, await bounded settlement of `streamUntilPersisted`, result promises, and concurrent observers. Inspect authoritative terminal status, approval cleanup, run ownership, lease release, and ability to start the next thread run. Include resume failure and re-suspension, and prove that a subscriber denial or disconnect does not independently settle a still-running workflow.

Reuse [policy-engine.test.ts](../../packages/breakwater/src/policy-engine/policy-engine.test.ts) for evaluator and hold-back behavior and [agent.test.ts](../../packages/breakwater/src/agent/agent.test.ts) for standard-loop tripwire preservation. These unit contracts complement the producer regressions; they do not substitute for them.

Use [vitest.flowsafe-harness.config.ts](../../vitest.flowsafe-harness.config.ts) and [scripts/flowsafe-harness.test.ts](../../scripts/flowsafe-harness.test.ts) for the full Mastra Worker graph and real D1/eviction behavior. [vitest.flowsafe-workers.config.ts](../../vitest.flowsafe-workers.config.ts) fits lightweight storage-module fidelity tests; [vitest.workerd-lifecycle.config.ts](../../vitest.workerd-lifecycle.config.ts) covers the shared server lifecycle. Extend the deterministic [spike driver](../../packages/flowsafe/scripts/spike-verify.mjs) for restart and persisted-state assertions; live-model credentials are unnecessary for the gate oracle.

### Release the bounded and strict contracts separately

The [pre-1.0 compatibility policy](../api-reference.md#compatibility-policy) treats documented behavior and exported types as public contracts. Use minor changesets for Breakwater’s host-compatible result evaluator and FlowSafe’s default-on guarded finalization, memory, result, and replay changes: denied durable runs change outcome, callers lose unapproved results, and some replay requests become refusals.

Keep `Symbol.for('@proofoftech/breakwater/guarded-agent-host/v1')` and bump the protocol’s `version` field for the stronger contract. Current FlowSafe’s `breakwaterGuardedAgentHostProtocol` treats an absent symbol as unguarded but refuses `version !== 1`; moving to a new symbol would silently drop guarded call restrictions in older FlowSafe. Publish coordinated versions, raise FlowSafe’s Breakwater peer floor from the current `>=0.17.0 <1.0.0` range to the evaluator-capable release, and refuse a detected guarded protocol without the required evaluator.

The design proposal itself needs no package release. A later correction entirely within an already published guarantee can use a patch changeset; introducing strict final-release behavior, changed suspension recovery, or a new configuration contract requires a minor changeset. Publish an upstream-core-dependent release only after updating the supported peer and running the compatibility proofs.

Enable the bounded gate by default for guarded agents after its local probes pass. Do not permit per-call bypasses. Keep unguarded agents outside that promise, and make strict admission opt-in until the full producer contract passes its oracle.

Ship a precursor FlowSafe release before the gate that refuses resume of snapshots carrying an unknown content-free contract marker. The gated release preserves workflow and step ids, so an older release without that refusal can resume through the ungated map. Permit downgrade only to a release with that marker refusal, or require operators to drain or cancel suspended stronger-contract runs before downgrading. Do not claim that an arbitrary historical FlowSafe release can refuse those snapshots.

Update the shipped-gap statements in [Durable agents](../durable-agents.md), [Breakwater architecture](../breakwater-architecture.md), [Policy engine design](../policy-engine-design.md), [Breakwater README](../../packages/breakwater/README.md), and the `PolicyEngine` source header together. Add the precise persistence/replay limits to [the security threat model](../security-threat-model.md) and [FlowSafe README](../../packages/flowsafe/README.md). Record the internal coupling in the maintainer guide and migration in the changelogs.

Until the producer transaction controls storage, publication, and returned message projections, those docs must still state that raw durable intermediate output can remain in D1 snapshots and topic history, and that released stream content cannot be retracted. Include working-memory and vector limits for supported bindings. After the bounded layer ships, replace the blanket final-save/result gap with its narrower remaining limits; do not claim full durable output enforcement from a passing final-result test.

## Open questions with bounded probes

The following probes resolve implementation and runtime uncertainty. They do not reopen the source-confirmed result-hook catch or registry-dependent observation behavior.

| Question | Smallest probe that settles it |
| --- | --- |
| Does terminal FlowSafe result retention match the source-derived write path? | Run one allowed durable generation with real D1, inspect `mastra_workflow_snapshot.result` immediately after success, evict the host, and read `RunSummary.result` again before retention runs |
| Which shared-list rewrite preserves standard denial and durable safe persistence? | In one standard/durable pair, mutate response IDs with `MessageList.removeByIds` and `add`, then abort; inspect tripwire, saved parts, empty-text fallback, and all earlier step copies |
| Can the protected factory replace final mapping without duplicating the durable loop? | Build deterministic allowed/denied tool runs through outer-graph recomposition or graph mutation. Preserve `durable-agentic-loop` and `map-final-output`, establish pub/sub access without an exported `PUBSUB_SYMBOL`, then suspend, evict, resume, and recover. Inspect graph identity, snapshot options, processor/flush order, and one-pass evaluation; pre-register an ungated graph and require refusal, then share the gated graph with an unguarded run |
| What terminal events reach an already blocked subscriber? | Record full subscriber chunks and producer history for a stream denial followed by deferred `step-finish` and final `finish`; repeat with a result-only denial and terminal hold-back denial |
| What replay can be safely served after registry eviction? | Preserve cache history, remove both run registries, call the host stream route, rebuild via trusted resume, and replay from zero and a middle offset; inspect payload fields as well as text |
| How does terminal classification coordinate with the producer final decision? | Make a deterministic classifier deny a held tail but allow aggregate evaluation. Delay subscriber processing until after final evaluation; require producer tail classification to block commit under the deadline and late observers to reuse its verdict. Cover deny-then-evict and allow-then-evict before resume, persisted marker restoration, and refusal when coordination state is unavailable |
| Can an all-flush memory gate preserve tool state and final commit? | Intercept the real memory `saveMessages` during approval suspension and finalization, withhold generated parts, then explicitly commit approved output; verify recall, queue draining, and tool-call/result consistency after resume |
| Which memory writers bypass thread-save withholding? | Bind guarded core `MockMemory` with working memory, semantic recall, a recording vector store, and an embedder. Capture `updateResource`, `saveMessages`, and vector upserts before suspension and after allow/deny; verify resource staging or refusal, approved metadata, standard-loop saves, private finalizer commit, and cleanup after thread purge |
| Does the precursor release prevent unsafe downgrade resume? | Persist a suspended gated run with the stronger marker, load it under the precursor release, and require refusal before core final mapping. Exercise the operator drain/cancel procedure before loading a release without marker refusal |
| What recovery remains when unapproved output is absent from snapshots? | Suspend a marked multi-step run, store a content-free snapshot projection, evict the isolate, and attempt resume; verify model context and that approved side effects are not repeated, or establish a safe refusal contract |
| Do gate throws and deadlines release every lifecycle owner? | Inject ordinary result-hook, step-hook, and FlowSafe final-gate errors plus a never-resolving evaluator; inspect authoritative workflow status separately from subscriber status, final flushes, persistence waiter, observers, thread lease, run owner, reservation, and next-run admission |
| Which direct writers bypass the selected wrappers? | Instrument actual memory, snapshot, and pub/sub writes for initial start, suspension, resume, callback, signal drain, and terminal repair; exercise raw `savePerStep` separately while asserting guarded refusal |
| What happens when terminal publication or storage fails? | Fail one terminal publish and one terminal snapshot repair in the real host harness; restore the dependency, fire recovery, and verify safe refusal before recovery and completed cleanup afterward |

No probe in this proposal has been executed as part of authoring it. Full-oracle success, transparent strict resume, and the exact internal workflow replacement remain unclaimed until those probes pass.
