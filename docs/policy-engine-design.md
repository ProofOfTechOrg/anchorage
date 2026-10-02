# Policy engine design

Breakwater policy runs at the boundary that has enough information to enforce it:

- `PolicyEngine` is a Mastra processor for agent input and output content.
- Tool-policy evaluators run inside `createConnector()` immediately before a tool executes.
- Data lifecycle enforcement runs against persistent storage from flowsafe maintenance duties.

There is no YAML policy loader or standalone policy service. Applications construct typed evaluators in code.

## Content policy

`PolicyEngine` takes an ordered array of `PolicyEvaluator` values:

```typescript
interface PolicyEvaluator {
  name: string;
  phases?: readonly ('input' | 'output')[];
  channels?: readonly ('answer' | 'reasoning' | 'object')[];
  holdBackChars?: number;
  evaluate(context: PolicyContext):
    | PolicyDecision
    | Promise<PolicyDecision>;
}
```

Each decision is either `{ allowed: true }` or `{ allowed: false, reason }`. `PolicyEngine` aborts every denial with `policy '<name>' denied the <phase>`, built by `policyDenialReason(policyName, phase)`, discarding the evaluator's reason. An evaluator that throws, or returns anything else, has failed. The engine records an error event and aborts with `policy evaluation failed` at input and in-stream on both Mastra agent loops. At the final result it throws an `Error` with that fixed message and no `cause`, which stops Mastra's standard loop.

Policies run in array order. The engine snapshots the list and each evaluator's
name, phase/channel selectors, hold-back hint, and evaluator reference at
construction. Class-based evaluators keep their original receiver, so private
fields, instance fields, TypeScript parameter properties and helper methods
continue to work. Later replacement of the evaluator
method or mutation of caller-owned selector arrays does not change enforcement;
mutable state owned by the evaluator instance or its closures remains the
application's responsibility. The first denial aborts the phase.

Construction refuses a `PolicyEngine` option its type does not declare, a
field outside `PolicyEvaluator` on a plain-object evaluator (a literal, a
spread of a factory's output, or a null-prototype object), and a present
`audit` without a callable `record`. A class-based evaluator's own fields are
its state and are not checked. A present `holdBackChars` must be a number of
at least 0, or `Infinity`. Each included evaluator throws on a `text` that is
not a string, so a direct call with malformed context fails instead of
allowing it.

### Host content gate

`createContentPolicyGate()` reuses the same snapshotted, declaration-ordered
evaluation contract for a host boundary that already has the exact
model-visible text. It evaluates an input-phase `answer` context and accepts an
optional trusted `RequestContext`. A policy this gate could never evaluate —
one whose declared phases exclude `input`, or whose declared channels exclude
`answer` — is rejected at construction rather than silently skipped, because a
policy that never runs is a hole at a security boundary. Of the included
policies only `maxTextLength` is affected: it declares the output phase by
default and needs an explicit `phases: ['input']` here. The gate also rejects
an empty policy list, which would allow every input.

The function the gate returns checks its call input before any policy runs.
An input that is not an object, carries a field other than `text` and
`requestContext`, has a `text` that is not a string, or has a present
`requestContext` that is not a `RequestContext` returns the error outcome and
records the static error event, so every policy answers a malformed input the
same way.

The result is deliberately opaque: `{ allowed: true }`,
`{ allowed: false, outcome: 'denied' }`, or
`{ allowed: false, outcome: 'error' }`. Policy names, denial reasons, inspected
text, and evaluator exceptions are not returned. The configured audit sink
retains the same static allow, denial, and error event vocabulary as
`PolicyEngine`.

This is a narrow adapter for framework paths that do not re-enter Mastra's
input processor chain. The host remains responsible for supplying the exact
downstream representation and for acting on denial before any model-visible
side effect.

## Phases and channels

Input processing evaluates, under the `answer` channel, the text Mastra renders into the model prompt from the call's own messages. It builds that text with Mastra's public `convertMessages()` conversion and applies the stored tool-output substitution Mastra's prompt builder makes, so replayed tool calls, tool results, reasoning and signals that remain caller input after memory processing are evaluated like new input. Mastra substitutes a stored output into any tool result with its call id, in memory-loaded history and in later loop steps too, so the engine also reads every stored output the messages carry, whether or not one of their tool results receives it. Inside a guarded agent, this text includes a client-only tool's mapped output for messages of the call's input because the guarded input chain runs `toModelOutput` before the policy engine. A raw `PolicyEngine` does not see that mapped output because Mastra computes it after input processors. Memory input processors receive the caller's text after RBAC and before the input policies on every guarded loop; semantic recall sends that text to its embedder. The messages come from the processor's `messageList`: memory filtering and client tool outcomes merged into stored messages follow the [input and memory rules](../packages/breakwater/README.md#input-policies-and-memory). Other history that memory loads is not evaluated unless a guarded agent's application input processor changes it or keeps its message carrying the caller's client tool outcome but moves it out of the call's input, so content must be checked where it is written to memory or returned by a tool. Without a `messageList`, every message counts as the call's own. Inside a guarded agent, the engine also evaluates what the agent's application input processors added to the prompt or changed in it outside the call's messages, recorded per call: each such system message as its text parts joined as Mastra joins them, with the values the same provider-option rules read from the options Mastra sends with it, and each such message of another source, memory-loaded history a processor rewrites through any source included, read as the call's messages are, and a remembered message carrying the caller's client tool outcome a processor keeps but moves out of the call's input, read whole, stored history included. Such a moved message gets no new `toModelOutput` mapping; cached output is read and sent. Each such message's text is in `text` once for each version a processor leaves it in and for the version the `breakwater-client-tool-output` step leaves after mapping that message's client tool results if it remains in the call's input: on `generate()` and `stream()` a context or response message a processor added is also in `messages` and read there, and on Mastra's durable loop `messages` holds the input messages only; a refused option or unclassified content aborts input as it does in a call's message. The agent's instructions are evaluated only when such a processor changes them. The application input processor wrapper compares the list before its processor runs with the list once the processor's returned promise settles, so a change made after that, or content served through a Proxy that shows the comparison other values than Mastra renders, is not evaluated. A standalone engine has no such record and reads the call's messages alone. The guarded agent's input asset check governs user file and image network URLs; a raw `PolicyEngine` has no origin check. A call the input chain refuses has its input, its response messages, and every other non-system message its application input processors added or changed, the refusing processor's own included, and a remembered message carrying the caller's client tool outcome a processor keeps but moves out of the input, even before aborting or throwing, removed from Mastra's message list before it stops. It saves none of them to memory, Mastra's durable loop generates no thread title from them, and on `generate()` and `stream()` the result's `messages` and `rememberedMessages` omit them, a history message such a processor changed included. Provider options that a model adapter bundled with Mastra renders into its request as message content or role, and that no genuine replay carries, abort input; those that replayed responses store as content, such as Anthropic citations, are read. Tool-call ids, provider metadata such as signatures, cache control and provider-held references like an OpenAI Responses `itemId`, and binary file and image data are not inspected; neither is the base64 data of a tool output's content items, which the adapters that send tool output as JSON text send as text. File and image media types and decoded `text/*` payloads, including `data:` URL payloads, are read as text; for `text/*` network URL data, the engine reads the URL string, not the downloaded content. A provider-executed tool result that Mastra places in an assistant message is read without the encrypted payload, file reference, base64 document or generated image that the provider returned, such as Anthropic web search's `encryptedContent`, where the root of the result has the shape a model adapter bundled with Mastra stores for that tool. The engine matches a shape only at the root because an adapter can send a result's nested members as they stand: the same field elsewhere in a result is read, such as in the tool definitions of an OpenAI Responses `tool_search` result, and so is the whole of a result that names a Google server-tool call or answers an Anthropic MCP call, which those adapters send on as content. Tool messages and stored model outputs are read whole. Call-level provider options are not part of the messages and the policies do not read them; the guarded handle accepts none, and `assertAcceptedCallProviderOptions()` refuses those outside a list of generation settings, which Flowsafe's durable start applies. The Vercel AI Gateway adapter forwards every provider option to a hosted service whose rendering Breakwater does not know. A message whose role, part type or tool-invocation state the engine does not classify, a rendered part or tool output it does not classify, a refused provider option, or content Mastra's conversion throws on aborts input with a static reason and one error event, rather than throwing, because Mastra's durable preparation continues to the model after an input processor error that is not a tripwire; `extractMessageText()` throws a `TypeError` for the same content, with the conversion's error as its `cause` when there is one. The guarded handle's `generate()` and `stream()` refuse a caller message with `role: 'system'`, at the top level or in the nested list Mastra flattens, which Mastra would pass to the model as a system message outside the input policies, and a list nested deeper than Mastra accepts. A host that starts the durable loop itself applies the same check through `assertNoGuardedSystemMessages()`, as Flowsafe's durable start does. Output processing maintains independent accumulated text for:

| Channel | Source |
| --- | --- |
| `answer` | Client-visible answer text |
| `reasoning` | Reasoning stream deltas |
| `object` | Canonical JSON structured-output snapshots |

A policy defaults to both phases and the `answer` channel. Set `phases` and `channels` when a policy applies more narrowly.

A text or reasoning delta whose text is not a string, and a result step whose reasoning text is present but not a string, abort the stream with a static reason and an audit error event. No policy could read such a chunk, so forwarding it would release text unseen.

Under the supported core version, the engine sees the `object` channel only for object chunks that flow through the processor chain (model-native streaming). It requires those values to be JSON data, evaluates the canonical serialization, and forwards the same canonical clone. A `generate()` result's parsed object and core's `StructuredOutputProcessor` chunks never pass through the chain, and Mastra may expose the parsed value before `generate()` returns. `createGuardedAgent` therefore rejects structured output and rejects object-only policies at construction. Mastra's thread title model call also bypasses input and output processing, so a guarded agent refuses `Memory` configuration that enables title generation. JSON carried in answer text is still inspected by policies that include `answer`. A standalone engine with an object-only policy requires an audit sink and aborts at the result boundary unless a processor-visible object chunk provided coverage.

## Built-in content policies

### Deny patterns

`denyPatterns(patterns, options)` performs literal or regular-expression-style configured matching according to its exported options. Its streaming implementation scans only the new suffix plus the largest pattern overlap. The default channels cover answer, reasoning, and object so a forbidden string cannot move to another output surface.

The evaluator's denial reason identifies the configured pattern, not the matched input span. `PolicyEngine` and `createContentPolicyGate()` discard that reason; their audit events use the static `policy denied` reason and identify the policy in `detail.policy`.

### Maximum length

`maxTextLength(limit, options)` denies accumulated text beyond a configured bound, a finite number of at least 0. Apply different policies per channel when answer and reasoning budgets differ.

### PII and secrets

`piiSecrets(options)` combines:

- email, phone, and US Social Security number patterns;
- Luhn-validated payment-card candidates;
- AWS access key, JWT, PEM private-key header, and secret-assignment patterns;
- high-Shannon-entropy token detection with a minimum candidate floor;
- allowlist exemptions;
- streaming overlap windows sized to the enabled detectors.

`entropyThreshold` must be a number greater than 0 and at most the entropy of
a candidate that uses each of the 67 characters a candidate is drawn from
equally often (about 6.066 bits per character), the highest entropy a candidate
can reach. The bound is the value the detector computes for that candidate, so
a threshold at the maximum still detects it.

The detector is a guardrail, not a semantic data-loss-prevention system. Encodings, fragmented values beyond configured windows, domain-specific identifiers, and adversarial transformations can evade pattern detectors.

### Asynchronous classifier

`classifierPolicy(options)` adapts a synchronous or asynchronous classification function:

```typescript
const moderation = classifierPolicy({
  name: 'moderation',
  classify: async (text, { phase, channel }) => {
    const result = await classify(text, { phase, channel });
    return result.allowed
      ? { allowed: true }
      : { allowed: false, reason: result.category };
  },
  evaluateEveryChars: 512,
  timeoutMs: 2_000,
});
```

`PolicyEngine` discards `result.category`; a host that needs it records it inside its own `classify` function.

Input and final-result phases always classify. During append-only streaming, the evaluator runs when accumulated text grows by the configured cadence, `evaluateEveryChars`, a positive safe integer; object snapshots classify individually. Under hold-back, a text or reasoning segment's end, or `finish` without an end chunk, also classifies text left below the cadence before releasing it.

A timeout, a classifier failure or a classifier that returns no decision fails closed at input and in-stream on both of Mastra's agent loops, and at the final result on Mastra's standard loop. On Mastra's durable loop, output policies, including hold-back's terminal classification, stop the stream a subscriber receives. Mastra logs a result-phase refusal; the saved thread message and returned result come from model output and are not filtered by output policies. No fail-open option is provided.

## Hold-back and leakage

Without hold-back, a streaming policy can detect a violation only after enough of the matching span has arrived. Earlier clean-looking characters may already have reached the client.

`new PolicyEngine({ holdBack: true, ... })` retains a trailing window per answer and reasoning channel. The largest `holdBackChars` hint among applicable policies wins. Once a passing buffer exceeds the window, the older portion is emitted. At a channel end or stream finish, the engine classifies any text still below `classifierPolicy()`'s cadence before releasing the held tail. With `holdBackChars: Infinity`, a denied segment emits nothing; with a finite window, text already released remains visible and only the held suffix waits for terminal classification. A host-written cadence evaluator does not receive this terminal classification behavior.

Object snapshots are replacement values rather than append-only text, so intermediate snapshots are suppressed and only a passing result is emitted.

Properties:

- The guarantee is per segment, because end markers flush each segment.
- A policy with no hint adds no window.
- A finite pattern policy can provide its maximum match span minus one.
- A classifier that must see the full output should opt into `holdBackChars: Infinity`, accepting full buffering.
- Hold-back changes delta boundaries. Consumers must treat text deltas as chunks, not semantic tokens.

Measured cost (2026-08-15, Node 22.22.0, `@mastra/core` 1.50.0; opt-in evidence tests in `packages/breakwater/src/policy-engine/policy-engine.test.ts`, run with `BREAKWATER_PERF=1`): a 4 MB stream in 2 KB deltas against string-pattern policies processes at roughly 1 MB/s including per-chunk harness overhead, with peak held text of 17 characters — the pattern-bound window, not the stream length. Any RegExp policy forces the unbounded window: a 1 MB stream keeps all of it pending and releases nothing until the channel ends. Prefer string patterns, overriding `holdBackChars` when the match bound is known, for large-stream leak prevention.

## Tool policy

`ToolPolicyEvaluator` receives the connector manifest, input, request context, and connector identity before execution.

### Declared network egress

`networkEgress({ allowedDomains })` compares every declared connector hostname with a deployment allowlist. An empty allowlist denies every egress declaration; omit the policy when the deployment does not apply an organization declaration gate. Invalid host declarations and allowlist entries fail at construction.

The connector SDK separately builds `runtime.fetch` from the manifest. It checks actual HTTP(S) requests and redirect hops against the declared list.

### Approval required

`approvalRequired(writePolicy)` determines whether a connector needs approval from:

- explicit `permissions.requiresApproval`;
- destructive side-effect classification;
- deployment write-permission patterns.

The connector reads `breakwater.connectorGrants` and `breakwater.connectorExecution` from request context. It compares connector, workflow, run, optional opaque isolation scope, and exact suspension identity. A `tool-call` grant also must match Mastra's `context.agent.toolCallId`. Flowsafe derives grants without an isolation scope from approved records and authoritative runtime state. A dry run bypasses the capability because its configured implementation must have no side effect.

### Cross-workflow isolation

`crossWorkflowIsolation({ targetScopeOf })` reads the trusted caller scope from `breakwater.workflowScope` and compares it with the connector-specific target extracted from input.

- No target means the connector is not addressing workflow-scoped state.
- A target with no caller scope fails closed.
- A different target fails closed.

### Opaque isolation scope

`tenantIsolation()` is the Breakwater API for requiring a non-empty opaque `breakwater.isolationScope`. The same scope segments idempotency and rate-limit keys.

Use it only in a host that has another trusted logical partition and mints the scope on every path, including dry runs. Breakwater does not parse the value, but the connector wrapper denies a present value that is not a non-empty string with `ISOLATION_SCOPE_INVALID`, whether or not `tenantIsolation()` is installed. Flowsafe's physically isolated data plane deliberately mints no isolation scope and drops provider attempts to add one, so its connector budgets are deployment-wide.

### Background execution

`backgroundExecution()` and the connector wrapper protect Mastra's `_background` model override. A connector is foreground-only unless its manifest declares `background: true`, and only a read-only connector can opt in. Write, destructive, and idempotent connectors remain foreground-only. A read-only connector may opt in even when its manifest separately requires approval; the grant check still runs at execution.

`backgroundExecution()` denies a write-class call whose arguments carry `_background`, whatever its value, using the connector wrapper's presence test, and denies a call whose `sideEffect` is not a side-effect member. Mastra's standard agent loop removes a truthy `_background` before dispatch, so a call Mastra runs as a background task carries no key; the wrapper refuses that call instead, with `BACKGROUND_TASK_DENIED`, on every connector without `background: true`. That refusal reaches only the standard loop's in-process dispatch: Mastra's durable agent loop, static background executors, calls nested inside background work, and Flowsafe `BackgroundTaskHost` executors pass no background flag. It is not final either: Mastra retries a refused task, and a Mastra that starts on the same storage can recover a task that is still queued, or running with retries left, through a static executor. `createGuardedAgent()` disables background dispatch, and Flowsafe's `RunnerRuntime` and agent thread host run no background-task manager. On a raw Mastra agent, do not make a connector without `background: true` background-eligible, and do not register one as a `BackgroundTaskHost` executor. See the [connector `background` contract](connector-interface.md#background).

### Custom evaluators

Add evaluators through `ConnectorPolicies.evaluators`. Keep them deterministic and side-effect-free; they run before the connector and before a dry-run return.

An evaluator may inspect trusted request-context values, but must never promote client input into an approval grant or isolation scope.

Tool evaluators can return `{ allowed: false, reason }` or add a `ConnectorDenialCode` and its code-specific `details`. Built-in codes are independent of renamed evaluators. Legacy custom denials use `EVALUATOR_DENIED`; malformed new metadata and evaluator exceptions use `EVALUATOR_FAILED`. The connector SDK validates and copies those fields before emitting its error and audit event. See the [decision-code contract](connector-interface.md#connector-decision-codes). Agent and content policy seams keep their existing result and abort behavior.

## Connector execution order

The SDK uses this order:

```text
input validation
  -> declared egress
  -> custom evaluators
  -> required permissions against the trusted projection
  -> dry-run selection
  -> approval grant
  -> legacy idempotency inspection and migration gate
  -> v2 idempotency reserve/replay
  -> rate-limit increment
  -> execute with guarded fetch
  -> output validation
  -> idempotency commit
```

Only real executions consume rate budget. D1 commits the increment and
expired-window cleanup in one transaction, so a cleanup failure cannot consume
quota for a rejected execution. An execution or rate-limit failure before a
successful side effect releases an owned idempotency reservation so a later
attempt can retry. Output-validation failure after execution leaves an atomic
reservation pending until stale takeover or operator recovery because an
immediate release could duplicate the completed side effect.

SDK events that reach the configured audit wrapper include `decisionCode`, `policyKind` and `retryable`. Arbitrary thrown values use static audit reasons rather than their exception text.

## Data lifecycle policy

Retention cannot be enforced by an in-process call evaluator because persisted data outlives the call. Flowsafe exports storage helpers:

- terminal workflow snapshot purge;
- approved/rejected approval purge;
- idle thread and message purge;
- terminal notification purge;
- thread-state and goal purge;
- schedule-trigger purge;
- terminal background-task purge;
- physical deployment decommissioning.

Live runs and open approvals are not age-purged. Schedules, resources, and subscriptions are standing state and delete with the physical deployment.

See [Deployment reference](deployment-reference.md) and [Operations runbook](operations-runbook.md).

## Choosing the boundary

| Requirement | Correct boundary |
| --- | --- |
| Deny a prompt before the model | Input processor |
| Deny framework-owned input that bypasses processors | Host content gate over the exact model-visible representation |
| Inspect answer/reasoning/object output | Output processor |
| Require a grant on every tool invocation path | Connector wrapper |
| Require a permission on every tool invocation path | Connector wrapper (`requiredPermissions` against the trusted projection) |
| Restrict actual connector HTTP redirects | `ConnectorRuntime.fetch` |
| Enforce opaque logical and workflow call scope | Tool evaluator plus trusted runtime context |
| Suspend an agent for review | Mastra native approval predicate compiled by connector |
| Mint the resumed connector capability | Flowsafe approval provider |
| Expire persisted state | Scheduled storage purge |
| Prevent any process socket from reaching the internet | Deployment infrastructure |

Read [Breakwater architecture](breakwater-architecture.md) and [Connector interface](connector-interface.md) for the surrounding contracts.
