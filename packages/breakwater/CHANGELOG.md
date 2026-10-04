# @proofoftech/breakwater

## 0.17.0

### Minor Changes

- b79f67b: The guarded agent checks user file and image URLs in the call's input, memory-loaded history, and processor additions before each model step on the message list as it stands. This checks signals Mastra drains into a running loop before it downloads their assets. On the durable loop, signals drained before the first model step join the prompt after that step's check and bypass it. Each step converts the message list once more for the check.

  The client tool output step checks network URLs with the input asset check's rules, reasons, and audit events. It checks outputs it maps through a client-only tool's `toModelOutput` in caller input or a message an application input processor changes, and every stored model output on a caller message whose id memory does not hold, including server-tool outputs in replayed transcripts. Stored outputs on remembered messages are not checked, including a completed call the caller re-sends. A tool-output refusal's tripwire carries `processorId: 'breakwater-client-tool-output'`; a mapper error still stops the call with `input processor failed` first.

  A mid-run refusal removes the call's input, including drained signals and remembered messages carrying the caller's client tool outcome, and its response messages from Mastra's message list. Nothing more of the refused call is saved to thread memory, but input and earlier steps Mastra saved at a durable suspension for tool approval, or steps saved under `savePerStep`, stay saved. The call's input and response messages are removed regardless of what added them; other messages an application input processor added or changed outside those sources stay on the list but are not saved. On the durable loop, a signal drained after a step refusal continues the run with the refused call's input and responses removed, and the arriving signal is checked at its step. Resumed durable legs check before each step and refuse a thread whose history names an origin absent from the configured list.

  A stored output an application input processor writes on a remembered message or outside the call's input, or a caller-supplied stored output it moves out of that input, is not origin-checked; input policies still read both. A model provider may fetch URLs in these unchecked outputs or remembered outputs. An unmarked stored client result in a message an application input processor changes is mapped and URL-checked. Inline `data:` URLs and base64 data remain inline data. See [Input asset origins](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#input-asset-origins) for the tool-output content items the check reads.

  Security: A signal's file URL or a URL in a mapped or caller-supplied tool output could name an origin absent from the configured list, such as a link-local cloud metadata address, which Mastra or a model provider then fetched.

  Migration: List in `allowedInputAssetOrigins` the origins signals and client tool outputs may name, including origins in replayed transcripts' tool outputs, or keep history in memory instead of replaying it. Hosts reading the tripwire chunk's `processorId` see `breakwater-client-tool-output` for a tool-output refusal.

- c3fdee4: Every `PolicyEngine` policy denial uses the tripwire reason `policy '<name>' denied the input|output`. This reason appears in `result.tripwire.reason`, the stream's `tripwire` chunk on both Mastra agent loops, and Mastra's logs and spans.

  When a policy evaluator throws or returns no decision at the final result, `PolicyEngine` throws a plain `Error` with the fixed message `policy evaluation failed` and no `cause`. Mastra's standard loop surfaces it to the `generate()` caller; the durable loop's finish step runs output processors but only logs what they throw. A missing decision no longer surfaces as `TypeError('policy evaluator returned no decision')`, so hosts checking `instanceof TypeError` stop matching.

  Use `policyDenialReason(policyName, phase)`, exported from both `@proofoftech/breakwater` and `@proofoftech/breakwater/policy-engine`, to construct the denial reason.

  Security: For `PolicyEngine` and `createContentPolicyGate` denials, evaluator reasons, classifier echoes, configured patterns, length limits, and detector names reach neither the caller nor the policy audit record. Audit events identify the policy in `detail.policy` and use static denial or failure reasons.

  Migration: Hosts that parse denial reasons match `policyDenialReason(...)`, read the audit event's `detail.policy`, or record the diagnostic information they need inside their own evaluator.

- 9feb010: Require `@mastra/core` `1.73.0` exactly. The Node.js runtime floor remains `22.13.0`.

  On a thread with stored memory, Mastra keeps input after the last assistant message, or that message's trailing client tool outcomes when the input ends with an assistant message. A caller message reusing a stored id takes the stored copy; only client tool outcomes that advance a stored pending call are merged, not replacement text. This happens before input policies, so dropped caller content reaches neither the model nor the saved thread and produces no tripwire or audit event. Send the new turn rather than re-sending history.

  Security: Results, errors and denials a caller sends for a stored pending client tool call under the stored assistant message's id are evaluated as caller input after memory merges them. Breakwater records client tool outcomes before memory processing and evaluates their merged parts. Stored outcomes not re-sent are not re-evaluated; those re-sent in the same state are. Two client tool outcomes for one tool call in a caller message stop with `input processor failed`. A standalone policy engine on a plain Mastra agent with memory evaluates every client tool outcome of a merged message, including stored ones.

  Guarded agents disable Mastra's default error processors, so provider-history rewriting, prefill retry and transient stream retry do not run. Construction refuses the error-processor options described in the [application processor rules](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#application-processors). Transient stream failures and prefill rejections receive no automatic retry from those processors.

  Guarded `stream()` disables eager tool execution: server tools wait until the step's stream-phase policies judge it. The durable loop runs tools after the model step regardless.

  Breaking: Application input and output processors implementing `processToolResult` are refused at construction because Mastra runs that hook after Breakwater's policies on both standard and durable loops.

  Migration: Move `processToolResult` logic from application input and application output processors into a tool's own result handling or an allowed processor hook. Upgrade `@mastra/core` to `1.73.0`.

### Patch Changes

- 549ab96: `redirect: 'error'` on the guarded fetch now works on Cloudflare Workers. The base fetch receives `redirect: 'manual'` in every mode. In `'error'` mode, a redirect response rejects with `TypeError('fetch failed')` whose cause is `Error('unexpected redirect')`, as on Node. Any other status, including 304, passes through unchanged. No migration is needed.
- 8a8ef3a: `createGuardedAgent` reads `policies`, `applicationInputProcessors`, `applicationOutputProcessors`, and `allowedPrincipalKinds` once, by index. `RBACMiddleware` also reads `allowedPrincipalKinds` once, by index. A list whose iterator differs from its elements cannot change what is enforced after validation.

  A non-array `allowedPrincipalKinds` is refused with "must be an array". No migration is needed.

- 3b5b0ad: A guarded agent checks every memory resolution for thread title generation, so a title-enabled function-valued or inherited `Memory` is refused wherever Mastra resolves it. At the `generate()`/`stream()` entry check, the standard loops' execution lookup, and the durable loop's first lookup, the call rejects with the `generateTitle` `TypeError` before the model runs and writes no audit event. A title-enabled `Memory` resolved for the memory processors still stops the call with `input processor failed` and an audited `breakwater-memory` error. A lookup at Mastra's finish runs after the model. Before this change, a durable call whose first lookup alone resolved a title-enabled `Memory` answered.

  Security: Before this change, the durable loop's first memory resolution was not checked, even though that `Memory` backs message saving. Thread title generation itself stayed disabled.

  Migration: Disable `generateTitle` on every `Memory` a guarded agent can resolve.

- 92be9d1: The input policies read a remembered message carrying the caller's client tool outcome that an application input processor keeps but moves out of the call's input through `MessageList` methods while returning the list or nothing. A refused call removes that message, also when the processor aborts or throws after moving it.

  This closes a security gap where the client tool result reached the model without input policy evaluation, and a move to `response` could also save it to memory.

  The moved message is read whole, stored history included, so a policy hit there refuses the call, as for a history message a processor rewrites. Its results get no new `toModelOutput` mapping. No migration is needed.

- 8a3c108: On streamed answer and reasoning text, a `piiSecrets` policy whose only detectors are `ssn` and/or `awsAccessKey` no longer denies an SSN- or AWS-access-key-shaped run that the incremental scan cut from a longer word, such as `INV123-45-6789`. With any other detector enabled, such a run still denies.

  This change adds no new denials. No migration is needed.

- f8d0851: A guarded agent maps, through `toModelOutput` and before the input policies, a client-only tool result in a message an application input processor changes. For example, a processor can merge the result into a remembered assistant message. The policies read the mapped output the model receives.

  This closes a security gap where mapped output reaches the model without input policy evaluation.

  A mapper error on any result in such a message, stored results included, stops the call with `input processor failed` and an `agent.input.processor` error event naming `breakwater-client-tool-output`. The policies read such a message once more in its mapped version, which `maxTextLength` counts. No migration is needed.

- 9d13a9c: A returned message Mastra holds as both remembered and call input stays both when an application input processor returns an array or a `{ messages, systemMessages }` pair.

  This closes a security gap where the client tool result a caller sends for a remembered assistant message reaches the model without input policy evaluation, and could leave the stored tool call pending, when an application input processor returns its messages, as Mastra's `UnicodeNormalizer` does.

  Such a processor's change to that message is now saved, as when it returns the message list. A mapper error on a stored result in a message it changes stops the call with `input processor failed`. No migration is needed.

## 0.16.0

### Minor Changes

- 11e8080: A guarded agent maps a replayed client-only tool result with the tool's `toModelOutput` during input processing, after application input processors and the input asset check. The mapper runs once per result part, and input policies read the mapped output the model receives. The reserved processor id `breakwater-client-tool-output` is refused for application processors.

  `toModelOutput` now runs before the client tool's `onOutput` hook, a behavior change hosts can observe. A mapper, tool-resolution, or mapped-output normalization error stops the call with `input processor failed` and one `agent.input.processor` error event whose detail names `breakwater-client-tool-output`. URLs inside mapped tool results are not checked against `allowedInputAssetOrigins`; a model provider may fetch them.

  Security: before this change, a client-only tool's mapped output reached the model without input policy evaluation, and a mapper error let the raw result reach the model.

- 850f95b: Export `assertNoGuardedSystemMessages()` and `assertAcceptedCallProviderOptions()` for hosts that start a guarded agent's durable loop. The call-level check accepts only a closed list of generation settings and refuses everything else before Mastra prepares the call. Export `providerOptionsCarryContent()` for hosts that carry provider options in a message without running input policies, such as a schedule signal.
- 2dbe0a5: The guarded durable loop loads the agent's memory, including thread history, working memory, and semantic recall, as the standard loops do. A memory resolution failure or memory input processor error, such as a failed thread-history read, stops a durable call with `input processor failed` and one `agent.input.processor` error event naming the processor's id (`message-history` for history). On `generate()` and `stream()`, Mastra rejects a memory input processor error and Breakwater writes no audit event. `createGuardedAgent()` refuses title generation on any `Memory` a call resolves, including function-valued and inherited memory. The guarded agent never generates a thread title, even when a host starts the durable loop with call-level `memory.options.generateTitle`.

  Migration: Expect durable runs to see thread history and memory context. Disable title generation on any `Memory` the guarded agent resolves.

  Security: Memory input processors, such as semantic recall's embedder, see the caller's text after RBAC and before the input policies on every loop. Mastra's thread title model call bypasses the guarded agent's input and output policies.

- a885488: `createGuardedAgent()` accepts `allowedInputAssetOrigins` for user file and image network URLs. After application input processors and before input policy, the reserved `breakwater-input-assets` step checks the call's input, memory-loaded history, and processor additions. It refuses non-`http(s)` network schemes, URLs with credentials, and origins not listed; absent or empty origins refuse every network URL, while `data:` URLs remain inline data. Entries must resolve to `http(s)` origins without paths, credentials, queries, fragments, or wildcards. A refusal stops the call and records one `agent.input.asset` event without the URL, using `input asset URL scheme is not allowed`, `input asset URL credentials are not allowed`, or `input asset URL origin is not allowed`. Input the step cannot read stops the call with `input message content is not classified` and one `agent.input.policy` error event, as the policy engine's reading does. Guarded agents that accept remote file or image URLs, including URLs stored in thread history, must list their origins. This option is independent of connector egress allowlists.

  Input policies, raw `PolicyEngine` instances, and `extractMessageText()` read file and image media types and decoded `text/*` payloads as text, including `data:` URL payloads. For network URL data declared as `text/*`, they read the URL string rather than the downloaded content. Binary file and image data remain unread; an undecodable `text/*` payload stops input as unclassified content, and `extractMessageText()` throws a `TypeError`.

  Security: without these checks, a caller's file or image URL can make the host fetch any address, including host-internal addresses such as `169.254.169.254` and `127.0.0.1`, after the input policies allow the call. A downloaded or inline `text/*` payload can reach the model as user text that the input policies never read.

- ccffeca: `createGuardedAgent()` now refuses the `channels` construction option, and `GuardedAgentConfig` omits it.

  Security: Mastra dispatches a channel's inbound messages and tool approvals to the agent the channel is configured on, outside the host that drives the guarded handle. A guarded agent already refused each such dispatch at its call-option check, but a Mastra that registered it still initialized the channels and started their listeners.

  Migration: remove `channels` from the options passed to `createGuardedAgent()`. TypeScript code that passes it no longer compiles.

- 20cabc3: Breakwater refuses malformed host policy inputs, and connectors refuse a call that Mastra's standard agent loop runs as an unopted background task.

  - A connector refuses a call that Mastra's standard agent loop runs as a background task, with the new `BACKGROUND_TASK_DENIED` code (policy kind `background`), unless its manifest sets `permissions.background: true`. The check runs before the evaluators, the dry-run branch, the approval grant and idempotency. It does not reach Mastra's durable agent loop, static background executors, calls nested inside background work, or a Flowsafe `BackgroundTaskHost` executor, none of which passes a background flag to the connector. The refusal is not final. The code's `retryable: false` is Breakwater's flag for the caller, which Mastra's task runner does not read: Mastra retries a refused task up to its retry count, which the model can raise through `_background.maxRetries`. While the task is still queued, or running with retries left, a Mastra that starts on the same storage can recover it through a static executor, which the refusal does not reach. `permissions.background` must be a boolean.
  - `backgroundExecution()` denies a write-class call whose arguments carry `_background`, whatever its value, including `{ enabled: false }`, `null` and a scalar. `writeClass`, when present, must be a non-empty array of side-effect members, and the evaluator uses its own copy.
  - `createConnector()` refuses an empty or non-string connector id and an id containing whitespace or a control or format character, as well as a `sideEffect` outside `read`, `write`, `destructive` and `idempotent`, and a `writePermissions` field other than `requireApproval` and `destructiveRequiresApproval`.
  - `createConnector()`, `approvalRequired()` and `singleTenantConnectorPolicies()` refuse an approval pattern that is empty or contains `:`, whitespace, or a control or format character. `approvalRequired()` validates its whole input before deciding: a non-string id, a policy that is not an object or has a field other than `requireApproval` and `destructiveRequiresApproval`, a non-array pattern list, a non-string pattern, a non-boolean `destructiveRequiresApproval` and an unknown side effect throw for every connector. A `*` in a pattern now also matches line terminators.
  - `PolicyEngine` and `createContentPolicyGate()` refuse a policy list that is not an array, and a `phases` or `channels` selector that is not a non-empty array of known members.
  - `denyPatterns()` refuses an empty list, and `denyPatterns()` and `piiSecrets({ allowlist })` refuse an entry that is neither a string nor a RegExp. Each keeps its own copy of a RegExp entry. `piiSecrets({ detectors })`, when present, must be a non-empty array of `PII_SECRETS_DETECTOR_IDS` members.
  - `RBACMiddleware` refuses an `allowedRoles` value that is not a non-empty array of distinct `ROLES` members, the rule `createGuardedAgent()` applies, and keeps its own copy. Its refusals are `TypeError`s; an empty list reads `RBACMiddleware: allowedRoles must be a non-empty array`. For both, a non-array reads `allowedRoles must be an array`, and an unknown or duplicate role names its entry index.
  - `writeClass`, the approval pattern lists of `approvalRequired()` and `createConnector()`, the policy lists and selectors of `PolicyEngine` and `createContentPolicyGate()`, `denyPatterns()` patterns, `piiSecrets()` `allowlist` and `detectors`, `permissions.requiredPermissions` and `allowedRoles` are copied when they are read, and a list whose `length` is not a non-negative safe integer is refused instead of coerced. Breakwater validates these inputs as data; host code that builds adversarial objects, such as a list with its own iterator, is inside the host trust boundary the security threat model describes.
  - `D1RateLimitStore` fails a call closed with `STORE_UNAVAILABLE` when its table returns a count that is not a positive safe integer.
  - Unknown fields. These options objects refuse an own field that their type does not declare, with a `TypeError` that names the field and lists the valid ones: `createConnector()`'s configuration, `permissions` and `policies`; `approvalRequired()`'s `manifest`, which accepts every `PermissionManifest` field; the agent CLI connectors' options and `createAgentCliConnector()`'s definition; the options of `PolicyEngine`, `createContentPolicyGate()`, `RBACMiddleware`, `piiSecrets()`, `maxTextLength()`, `classifierPolicy()`, `AuditLogger`, `D1IdempotencyStore` and `InMemoryIdempotencyStore`; each plain-object policy in a `PolicyEngine` or `createContentPolicyGate()` list, meaning a literal, a spread of a factory's output or a null-prototype object; and `createGuardedAgent()`'s configuration, whose valid fields are Breakwater's own and the `AgentConfig` fields of `@mastra/core` 1.67.0 that it keeps. A class-based policy keeps its own instance fields, parameter properties and private fields, which are its state. Symbol keys are not read. The `writePermissions` refusal lists its valid fields in the same form.
  - Wrong types and out-of-range numbers. `createConnector()` refuses a `policies` or `policies.networkEgress` that is not an object, a `policies.evaluators` that is not an array of evaluators with a string `name` and an `evaluate` function, and a non-boolean `permissions.requiresApproval`, `dryRun` or `idempotencyKey`; `null` still reads as omitted for the three `policies` fields. The agent CLI connectors refuse a non-boolean `idempotencyKey` and a `definition.egress` that is not an array. `piiSecrets()` refuses an `entropyThreshold` that is not a number greater than 0 and at most log2 of its 67-character candidate alphabet (about 6.066), a numeric string included. The maximum is the entropy the detector computes for a candidate that uses every character equally often, 6.066089190457767, and a threshold at that maximum detects such a candidate; `Math.log2(67)` itself is a few units in the last place higher and is refused. A `holdBackChars` hint on `piiSecrets()`, `denyPatterns()` or any policy given to `PolicyEngine` or `createContentPolicyGate()` must be a number of at least 0, or `Infinity`. `InMemoryIdempotencyStore` `maxEntries` and `classifierPolicy()` `evaluateEveryChars` must be positive safe integers, `maxTextLength()` `maxChars` a finite number of at least 0, and `AuditLogger` `maxBuffered` a non-negative safe integer.
  - Empty content-policy gate. `createContentPolicyGate()` refuses an empty policy list. A `PolicyEngine` may still have no policies.
  - Audit sink. `AuditLogger` refuses a `sink` or `onSinkError` that is not a function, and `hasExternalSink()` answers `true` only for a function sink. `PolicyEngine` and `RBACMiddleware` refuse an `audit` without a callable `record`, `null` included. `combineAuditSinks()` refuses an empty sink list and a sink that is not a function, naming its index, and `metricsAuditSink()` refuses a recorder whose `increment` or `observe` is not a function.
  - Request-context values. The connector wrapper denies a present `breakwater.isolationScope` that is not a non-empty string with the new `ISOLATION_SCOPE_INVALID` code (policy kind `tenant-isolation`), and a present `breakwater.dryRun` other than `true` or `false` with the new `DRY_RUN_INVALID` code (policy kind `dry-run`); both are checked before the evaluators and are not retryable. The wrapper reads the scope once and uses that read for grant matching, the rate budget and replay. An empty-string scope, which read as unscoped, is denied too. `actorFromRequestContext()` resolves no actor for a value with a field other than `id`, `role` and `kind`, so the existing "no actor" denial follows. A present `breakwater.auditContext` that is not an object, lacks a non-empty `agentId` or `entryPath`, has an unknown field, or has an optional field that is neither `undefined` nor a non-empty string makes the connector wrapper, guarded preauthorization, `RBACMiddleware`, the content gate and the `PolicyEngine` input and result processors record an `audit.context` error event; nothing is denied, and the reader still copies the fields it accepts.
  - Call-time inputs. The function `createContentPolicyGate()` returns gives the error outcome, with a static error event, for an input that is not an object, has a field other than `text` and `requestContext`, has a non-string `text`, or has a `requestContext` that is not a `RequestContext`. `invokeConnector()` refuses options that are not an object, an option outside `ConnectorInvocationOptions`, and a `requestContext` that is not a `RequestContext`, with `CONNECTOR_INVOCATION_OPTIONS_INVALID`. A guarded agent's `generate()` and `stream()` refuse a caller message with `role: 'system'`, at the top level or inside the one level of nested list that Mastra flattens, and a list nested deeper than that, with a `TypeError`. They also refuse a `memory` call option other than a non-empty `thread` id, or an object whose only field is that id, with a non-empty `resource`, so `options`, `onTitleGenerated` and thread fields such as `metadata` are refused; Mastra receives a frozen copy, and `GuardedAgentCallOptions['memory']` declares that shape. `extractMessageText()`, and so `PolicyEngine` input evaluation, reads the text Mastra renders into the model prompt from the messages: it converts them with Mastra's `convertMessages()`, applies the stored `modelOutput` substitution Mastra's prompt builder makes, and reads every stored `modelOutput` the messages carry, whether or not one of their tool results receives it, so replayed tool calls, tool results, reasoning and signals are checked like new input. It reads every field of a tool output's content items except base64 `data`, and the text of the provider options that replayed responses store as content, Anthropic citations and OpenRouter reasoning details, without their signatures, ids and encrypted values. A provider-executed tool result that Mastra places in an assistant message is read without the encrypted payload, file reference, base64 document or generated image that the provider returned, such as Anthropic web search's `encryptedContent`, where the root of the result has the shape a model adapter bundled with Mastra stores for that tool. The same field elsewhere in a result is read, such as in the tool definitions of an OpenAI Responses `tool_search` result, which that adapter sends as given when `store` is `false`, and so is the whole of a result that names a Google server-tool call or answers an Anthropic MCP call, which those adapters send on as content. `PolicyEngine` evaluates the call's own messages, and inside a guarded agent what its application input processors add or change outside them, as the next entries describe: history that memory loads into the processor's `messageList`, including the tool calls and tool results an earlier guarded call saved, is not passed to policies as `messages` and is not evaluated unless such a processor changes it. Tool-call ids, other provider options, and file and image data are not read, and neither are call-level provider options, which a guarded agent's handle refuses. The Vercel AI Gateway adapter forwards every provider option to a hosted service whose rendering Breakwater does not know. `extractMessageText()` throws a `TypeError`, and `PolicyEngine` aborts input with a static reason and one error event, for a message whose role, part type or tool-invocation state it does not classify, whose rendered prompt part or tool output it does not classify, that carries a provider option a model adapter bundled with Mastra renders into its request as message content or role and no genuine replay carries, or whose conversion throws; the `TypeError` then carries the conversion's error as its `cause`. `PolicyEngine` aborts rather than throws there because Mastra's durable preparation runs the model after an input processor error that is not a tripwire. `denyPatterns()`, `maxTextLength()`, `piiSecrets()` and `classifierPolicy()` throw on a `text` that is not a string. `PolicyEngine` aborts a stream on a text or reasoning delta whose text is not a string, and on a result step whose `reasoningText` is present but not a string.
  - Input failures. An input evaluator or `classifierPolicy()` classifier that throws, times out or returns no decision, and a guarded agent's application input processor that throws other than through its `abort` or a `TripWire`, an error thrown after its own abort included, or returns a value that cannot be applied, now end the call in a tripwire with a static reason, `policy evaluation failed` or `input processor failed`, and one error event, instead of a thrown `MastraError`. The application input processor's event is the new `agent.input.processor` action, with the processor's id in its detail. A policy that returns no decision is an evaluator failure in-stream too, where the stream aborts, and at the final result, where the engine rethrows a `TypeError`; the function `createContentPolicyGate()` returns gives its error outcome for one. `RBACMiddleware` denies an actor lookup that throws, an actor whose fields throw when read and an actor whose `kind` is not a `PRINCIPAL_KINDS` member, with a static reason and, when it has an audit logger, one audit record, instead of throwing; a guarded agent's direct authorization still throws its authorization error.
  - Application input processors. A guarded agent applies an application input processor's return value to the call's message list itself, with the steps of Mastra's durable loop, on both loops. On `generate()` and `stream()`, a `role: 'system'` entry in a returned `{ messages, systemMessages }` pair now becomes a system message, as on the durable loop, and one in a returned array or pair no longer removes the input message whose id it reuses; `{ messages }` without `systemMessages`, which Mastra's standard loop ignored, stops the call with `input processor failed`. `null` and other falsy values leave the list unchanged, and a pair is applied whatever its prototype, as Mastra applies them. `GuardedInputProcessor['processInput']` admits a processor that returns nothing or `null`. The input policies now also evaluate what a guarded agent's application input processors add to the prompt or change in it outside the call's input: each system message they add or change, tagged or not, with the provider options Mastra sends with it, and each message of another source, such as a memory, context or response message or a history message a processor rewrites through any source, read as a caller message is. A system message such a processor adds or changes whose content is not a string or a list of text parts, or that holds one of those fields in an accessor, and a message such a processor adds or changes that `structuredClone` cannot copy, stop the call with `input processor failed`. The agent's instructions and loaded history are evaluated only when such a processor changes them. A standalone `PolicyEngine` reads the call's messages alone.
  - Refused input. A call that `RBACMiddleware`, a `PolicyEngine` input policy or a guarded agent's application input processor refuses has its input, its response messages, and every other non-system message its application input processors added or changed, the refusing processor's own included, removed from Mastra's message list before it stops. It saves none of them to memory, Mastra's durable loop generates no thread title from them, and on `generate()` and `stream()` the result's `messages` and `rememberedMessages` omit them, a history message such a processor changed included. `PolicyEngine` and `RBACMiddleware` on a raw agent remove a refused call's input and response messages the same way. The tripwire, `text` and audit events are unchanged.
  - Helper arguments. `approvalRequired()` refuses a present `requiresApproval`, `dryRun`, `idempotencyKey` or `background` that is not a boolean, as `createConnector()` does. `backgroundExecution()` denies a call whose `sideEffect` is not a side-effect member with `BACKGROUND_EXECUTION_DENIED`. `principalKindOf()` throws on a present `kind`, `null` included, that is not a `PRINCIPAL_KINDS` member. `egressDomainAllowed()` answers `false` for a host that does not match the hostname pattern and skips list entries that do not.
  - Agent CLI working directory. `createClaudeCodeConnector()`, `createCodexConnector()` and `createAgentCliConnector()` accept a `cwd` option, a non-empty string from trusted host configuration. When it is set, every call runs in it, `cwd` leaves the model's input schema, and a `cwd` in a call's input is dropped by validation. Connectors without it keep taking `cwd` from the call's input.

  Security: the inputs below were accepted and failed open. A selector that selected nothing, an empty detector or deny list, or an empty `writeClass` made a policy that never ran. An allowlist entry object whose own `test` answered `true` exempted every detection, and a pattern object whose own `test` answered `false` never denied. An approval pattern that could never match, such as one with a trailing space, a pattern object with its own `split`, or a mistyped `sideEffect` stopped requiring approval for a write-class connector. A connector without the background opt-in ran as a Mastra background task when a raw agent made it eligible. `RBACMiddleware` given a string as `allowedRoles` authorized every role that is a substring of it, such as `viewer` for `'reviewer'`. A `D1RateLimitStore` table that returned a `NULL` count never reached the limit, and one that returned a negative count under-counted spend. A misspelled field in the options objects listed above was ignored, which dropped what it configured wherever the default was weaker. For example, `requireApproval` for `requiresApproval` ran a write connector with no grant, a misspelled `policies` or `evaluators` ran it without its evaluators, `inputschema` passed model input to `execute` unvalidated, `binarypath` spawned the bare CLI instead of the host's wrapper, `holdback` turned zero-leak buffering off, `auditLogger` left decisions unaudited, and `applicationOutputProcessor` dropped a guarded agent's output processor. A `policies` or `policies.networkEgress` of `''`, `0` or `false`, a string `policies`, or an empty-string `policies.evaluators` ran a connector without those gates. An agent CLI connector given `''`, `0` or `NaN` as `requiresApproval` spawned the CLI without a grant, and one whose definition had no `egress` passed the organization egress gate. An `entropyThreshold` that was not a number, or was `NaN`, `Infinity` or above about 6.066, turned the `highEntropy` detector off. A `holdBackChars` of `''`, `[]`, `false` or a negative number, or `null` on a hand-built policy, released text before the policy had seen it whole. A `maxEntries` below 1, or a value such as `''` or `false` that coerces to 0, replayed nothing. `Infinity` as `maxChars` never denied, and as `evaluateEveryChars` let a stream through before any classification. `createContentPolicyGate({ policies: [] })` allowed every input. An `AuditLogger` sink that was not a function counted as external, so the single-tenant production preset accepted a logger that exported nothing; a negative, empty-string, empty-list or `false` `maxBuffered` kept no event; and `PolicyEngine({ audit: null })` passed the object-only policies' audit requirement. `combineAuditSinks()` with no sinks or a non-function sink, and `metricsAuditSink()` with a recorder lacking `increment` or `observe`, built a sink the production preset accepted as external while it exported nothing. A `breakwater.isolationScope` that was not a non-empty string, such as `42` and `43`, an object or `''`, read as no scope, so one tenant replayed another's stored result, shared its rate budget, and matched a scope-less grant. A `breakwater.dryRun` such as `'true'`, `1` or `'yes'` ran the real side effect instead of the simulation, on a granted approval connector too. An actor whose kind was misspelled, such as `principalKind: 'service'`, was authorized as a human. A content gate passed PII to `piiSecrets()` when `text` was misspelled, missing, `null`, a message-content object or a part list, and `maxTextLength()` allowed a list; the built-in evaluators allowed some non-string `text` when called directly. A misspelled `invokeConnector()` `requestContext` ran the real side effect where a simulation was requested. A guarded agent passed a caller's system-role message, at the top level or in a nested list, and replayed tool-call arguments, tool results, error text, denial reasons, stored model outputs, reasoning, and signal contents and attributes, to the model without the input policies. A caller's `openaiCompatible` provider options replaced a message's content or role in the request of the model adapter Mastra uses for custom endpoints and most catalog providers, which turned caller text into an unread system message. A stored model output that no tool result in the call received reached the prompt unread when Mastra substituted it into a pending call's placeholder result or a later step's tool result. The URLs, file ids and provider options of a tool output's content items reached the model unread through the adapters that send tool output as JSON text. The guarded `memory` call option put caller text into the system prompt through thread metadata and working-memory options. `approvalRequired()` answered "no approval" for a write manifest whose `requiresApproval` was `''`, `0`, `NaN` or `null`. `backgroundExecution()` allowed a background override on a call whose `sideEffect` was misspelled or missing. A non-string stream delta from a model adapter was forwarded unevaluated, ahead of held text. The agent CLI connectors ran in the working directory the model chose. A malformed `breakwater.auditContext` dropped correlation without any error. `egressDomainAllowed('', [''])` answered `true`. A guarded agent's application input processor that moved the caller's text into a system message, into a system message's provider options, or into a message of another source, such as a memory or context message or a rewritten history message, sent it to the model without the input policies, and a response message it added was saved to the thread of a refused call. On Mastra's durable loop, an input evaluator or `classifierPolicy()` classifier that threw, timed out or returned no decision, an application input processor that threw or returned a value Mastra could not apply, and an `RBACMiddleware` actor lookup that threw let the model run on the unevaluated input, skipping every later processor, and an `RBACMiddleware` whose `audit` could not record let a denied actor through; a call refused on input had its input saved to the thread and sent to the model for a thread title.

  Migration:

  - Use connector ids without whitespace or control or format characters, and approval patterns that follow the same rule.
  - Drop empty `phases`, `channels`, `writeClass`, `detectors` and `denyPatterns()` lists, or give them members.
  - Pass real RegExps or strings as `denyPatterns()` and allowlist entries, not RegExp-like objects.
  - Remove unknown fields from `writePermissions` and from the options objects listed above; a misspelled field used to be ignored. Each refusal lists the valid fields. A plain-object policy may carry only `PolicyEvaluator` fields; a class-based policy's instance state is not an unknown field and needs no change.
  - Give numeric options a number in range, not a numeric string: `entropyThreshold` greater than 0 and at most about 6.066, `holdBackChars` at least 0 or `Infinity`, `maxEntries` and `evaluateEveryChars` positive safe integers, `maxChars` a finite number of at least 0, and `maxBuffered` a non-negative safe integer.
  - Pass a boolean, or omit the field, for `permissions.requiresApproval`, `dryRun` and `idempotencyKey`, and for the agent CLI connectors' `requiresApproval` and `idempotencyKey`.
  - Pass `policies` and `policies.networkEgress` as objects and `policies.evaluators` as an array of evaluators, and give an agent CLI definition an `egress` array.
  - Give `createContentPolicyGate()` at least one policy, or leave the gate out.
  - Give `AuditLogger` functions as `sink` and `onSinkError`, and give `PolicyEngine` and `RBACMiddleware` an `AuditLogger` as `audit` or omit it.
  - Handle an input evaluator, classifier or application input processor failure as a tripwire with the reason `policy evaluation failed` or `input processor failed`, not as a thrown `MastraError`; the thrown value no longer reaches the caller, and a direct `processInput` call rejects with its `abort`'s error. Handle a failed `RBACMiddleware` actor lookup as a denial with the reason `actor lookup failed`, and an undeclared `kind` as one with the reason `principal kind is not declared`.
  - Read a refused call's input from the messages you sent, not from the result's `messages`.
  - Return an array, a `{ messages, systemMessages }` pair, the message list or nothing from an application input processor; `{ messages }` without `systemMessages` now stops the call.
  - Expect every input policy to evaluate host context an application input processor adds as system text, the whole of any system message it edits, the agent's instructions included, and any message it adds or changes outside the call's input: `piiSecrets()` refuses such context when it carries personal data or a secret, `maxTextLength()` counts it, text a processor adds is evaluated even when a later processor removes it, and a refused provider option on such a system message stops the call. Give a system message a processor adds a string or a list of text parts as its content. A custom policy finds that text in `text`, once; on `generate()` and `stream()` `messages` also holds the context and response messages such a processor adds, and on Mastra's durable loop it holds the call's input messages alone. An application input processor that rewrites the whole history, such as Mastra's `UnicodeNormalizer` with its default options, has every history message it changes evaluated by the input policies on each `generate()` or `stream()` call.
  - Pass `RBACMiddleware` a non-empty array of distinct `ROLES` members, and handle its refusals as `TypeError`s.
  - Pass arrays whose `length` is a non-negative safe integer; only a Proxy can report another.
  - Give a `D1RateLimitStore` table created outside the store a `count INTEGER NOT NULL` column, and remove rows with a `NULL` or negative count.
  - Set `permissions.background: true` on a read connector that should run as a Mastra background task.
  - On a raw Mastra agent, do not make a connector without `background: true` background-eligible, and do not register one as a `BackgroundTaskHost` executor: Mastra can retry and recover the refused task. `recoverStaleTasksOnStart: false` stops Mastra's start-up recovery of every task.
  - Handle the new `BACKGROUND_TASK_DENIED`, `ISOLATION_SCOPE_INVALID` and `DRY_RUN_INVALID` codes in exhaustive `ConnectorDenialCode` or `ConnectorDecisionCode` handlers.
  - Set `breakwater.isolationScope` to a non-empty string or leave it unset; an empty string no longer reads as unscoped. Set `breakwater.dryRun` to `true`, `false`, or leave it unset.
  - Give `breakwater.actor` only `id`, `role` and `kind`, and give a custom `getActor` result a `kind` that is a `PRINCIPAL_KINDS` member or none.
  - Give `breakwater.auditContext` only `AgentAuditContext` fields, with each optional field a non-empty string or `undefined`, or each boundary records an `audit.context` error event.
  - Call a content gate with `{ text, requestContext? }` and a string `text`.
  - Pass `invokeConnector()` only `requestContext`, `abortSignal`, `observe` and `toolCallId`, with a Mastra `RequestContext`.
  - Move system instructions from a guarded agent's call messages into its `instructions`, and pass messages in a flat list or one nested level. Expect a replayed history in the call, reasoning, signals and stored model outputs included, to be checked by the input policies, and a message whose role, part type or tool-invocation state Breakwater does not classify, or that Mastra's conversion cannot read, to stop the call; a caller of `extractMessageText()` receives a `TypeError` for it.
  - Remove `openaiCompatible` provider options, Anthropic document `title` and `context`, and OpenRouter `filename` and message-level `annotations` from caller messages, in any message form; they stop the call, and `extractMessageText()` throws its `TypeError` on them. Flowsafe's thread host start check refuses an idle wake carrying one of these options; delivery into a running run is refused only by Flowsafe's `scheduleProviderOptionsPolicy` route option when the host wires it.
  - Check content where it is written to an agent's memory or returned by a tool: the input policies no longer evaluate the history memory loads into a call, including the tool calls and tool results an earlier guarded call saved, unless a guarded agent's application input processor changes it, and a custom policy's `messages` no longer holds it.
  - Pass a guarded agent's `memory` call option as `{ thread, resource }`, with `thread` an id or `{ id }`. Set memory configuration and thread metadata on the agent's `Memory` instead of per call; `options`, `onTitleGenerated` and other thread fields are refused.
  - Give `combineAuditSinks()` at least one sink function, and `metricsAuditSink()` a recorder with `increment` and `observe` functions.
  - Pass booleans for the boolean manifest fields given to `approvalRequired()`.
  - Use `6.066089190457767`, not `Math.log2(67)`, for the highest `entropyThreshold`.
  - Emit string text deltas from a custom model adapter.
  - Set `cwd` on agent CLI connectors from trusted host configuration, so the model no longer chooses the workspace.

### Patch Changes

- def857f: `createConnector()` now throws a `TypeError` naming the connector when `permissions.egress` is present but is not an array. A string used to be spread into one-character entries, so a string of letters and digits registered its characters as hosts, and a dotted, hyphenated or wildcard host failed on its first `.`, `-` or `*`. A `Set` or another non-array iterable, which registered its members, must now be passed as an array (`[...iterable]`). An omitted or `null` `egress` still registers an empty list.
- d8846de: `createConnector()`, `networkEgress()` and `egressFetch()` now throw a `TypeError` naming the field and the index when a host-list entry is not a string, and each reads the list once, so the hosts it enforces are the hosts it validated. `networkEgress()` and `egressFetch()` also name the field when the list is not an array.

  Security: a non-string entry used to pass hostname validation by string coercion. An entry object with its own string methods could then make the runtime egress guard, the `networkEgress()` policy and `assertConnectorConformance()` accept hosts the entry does not name. A list whose reads change could pass validation with one entry and enforce another in `networkEgress()` and `egressFetch()`.

  Migration: pass each host as a plain string. A `String` object and a hole in the list are refused. `egressDomainAllowed()` ignores a non-string entry, which matches no host. It now returns `false` for a domain that is not a string or a list that is not an array, where a non-array list used to throw unless it had its own `map`, and it reads the list by index, so no method of the list decides the match.

- d8846de: `runtime.fetch` and `egressFetch()` now refuse an `init.redirect` that is not `'follow'`, `'manual'` or `'error'` with `EGRESS_INPUT_INVALID` before any request, and pass the base fetch the redirect mode they checked. In every redirect mode, the guard forwards request-init members inherited by a class instance, an `Object.create` object or a `Request` passed as `init`; copying only own enumerable members had dropped values such as method, headers, body and signal.

  Security: the guard compared `init.redirect` with the string `'follow'`, then handed the caller's `init` to the base fetch, which read and converted `redirect` again. A value the base fetch converts to `'follow'`, such as the JSON array `["follow"]`, a `String` object or an object with its own `toString`, or a getter that answered `'manual'` to the guard and `'follow'` to the base, made the base fetch follow redirects itself with no hop check, so a redirect from an allowed host could reach any host. This affects every release with the runtime guard.

  Migration: pass `redirect` as one of the strings `'follow'`, `'manual'` or `'error'`, or omit it.

- f15367f: Hold-back classifies the remaining text of a streaming segment before releasing it at a channel end or stream finish. A denied segment held with `holdBackChars: Infinity` emits no text.

  Security: A denied segment shorter than the classifier cadence could previously be released under hold-back before the result-phase refusal.

- 8c519ee: Correct the connector authoring guide on when the conformance harness reports `no-egress-declaration`: the guide now states that the cause applies to any successfully parsed host reached through a supplied base transport with no bound subject, which is the probe's transport at any time and a case's transport until its subject binds, not only while a factory is constructing.

## 0.15.0

### Minor Changes

- 3f30616: `ConnectorConformanceEscape` gains a required `cause` discriminator. Code that constructs escape records must supply the refusal cause. `NETWORK_IO_OUTSIDE_RUNTIME_FETCH` findings from `policies.fetch` identify URL parsing, host parsing, a missing egress declaration, or an undeclared host as the refused check. Trapped entry points record `outside-runtime-fetch` and retain their existing finding text.
- 36c60c8: Require `@mastra/core` 1.67.0 exactly (previously 1.53.0). The peer is exact, so every consumer must move to 1.67.0 as well; this is breaking for consumers pinned to 1.53.0. 1.67.0 bundles for Cloudflare Workers and Vite again — mastra-ai/mastra#20638, the dynamic-import regression that held the pin at 1.53.0, closed upstream.

  `@mastra/cloudflare-d1` moves from 1.1.1 to 1.3.2, whose own peer requires a core newer than 1.53.0. FlowSafe's `@proofoftech/breakwater` peer floor rises to `>=0.15.0 <1.0.0` in step, that being the first Breakwater release built against the same core.

  The `@mastra/core` patch FlowSafe shipped under `patches/` is retired: 1.67.0 carries both fixes upstream (mastra-ai/mastra#23693, mastra-ai/mastra#23694). The patch file is gone from the published package, and so are the two refusals that required it — FlowSafe no longer refuses to construct a delivering notification dispatch tick, nor notification ingestion and dispatch requests, on an install whose core lacks the patch. Consequence for anyone running a core outside the declared peer without having applied the patch: a notification `source` named after an `Object.prototype` member is miscounted in the summary core renders, and its source delivery policy resolves the inherited member instead of the configured priority or default action. That configuration was unsupported before and remains so, but it now fails silently rather than loudly. Application roots that copied the patch into their own `patches/` should drop it along with the `patchedDependencies` entry or `postinstall` script that applied it.

  `createD1Storage({ domains })` composes the two storage domains 1.67.0 adds, `workflowDefinitions` and `knowledge`, through the same override seam as the other domains. `@mastra/cloudflare-d1` backs neither, so each resolves `undefined` unless a host supplies one through that seam.

  The workspace lockfile behind this release was resolved once with pnpm's seven-day minimum-release-age gate overridden for that resolution only. Five newly resolved versions were younger than the gate at resolution on 2026-09-19: `@mastra/core` 1.67.0 and `@mastra/schema-compat` 1.3.10 (published 2026-09-15), which the workspace's standing `@mastra/*` exclude admits with the gate on, and `posthog-node` 5.52.4 (2026-09-15), `@posthog/types` 1.412.2 (2026-09-17) and `@posthog/core` 1.55.0 (2026-09-18), which the override alone admitted. None of the five declares a lifecycle script or ships a native component; all five carry npm provenance attestations (SLSA v1, published from GitHub Actions); and `posthog-node`, with `@posthog/core` and `@posthog/types` beneath it, is a hard dependency of `@mastra/core` that ships inside a bundled Worker.

  The private `showcase` and `anchorage-agent-starter` packages move to the same core.

### Patch Changes

- 06b52a2: Read required agent audit context values once before validating and recording them. Accessor-backed context cannot replace a validated `agentId` or `entryPath` during the copy.

  Document that `idempotencyKeyMigration` is validated at connector construction. Caller-held policies are read again when an execution reaches the absent-legacy migration gate and when a validated, ambiguous legacy migration reaches the acknowledgement gate; the single-tenant preset retains its frozen construction snapshot. Also clarify that the conformance harness reports `no-egress-declaration` after URL and host parsing succeeds before a subject is bound.

## 0.14.0

### Minor Changes

- 8fc708a: Say what the connector conformance harness established about an entry point a case deleted outright.
  An `INSTRUMENTATION_REPLACED` finding for a property that is gone at verification now reads
  `globalThis.fetch descriptor differs from the one the harness installed: absent property; calls made
after the replacement were not observed`. The finding stopped at `absent property` before, though a
  read after the deletion resolves through the prototype chain or to `undefined` and never to the trap.
  A finding for a replacement that is itself an accessor still says the harness did not check, and one
  for a data property that still holds the trap still carries no such clause.
- 0e14950: Build the connector conformance report even when a case throws a value that cannot say what it is.
  The harness classified a thrown value with four separate `instanceof` reads; a value whose
  `getPrototypeOf` is a trap made the classification itself throw, so the run rejected with the trap's
  error and produced no report. One `classifyInvocationError` call now answers `boundary`, `policy`,
  `refusal` or `foreign` once, and a value it cannot read is `foreign`: the report carries the named
  `CASE_INVOCATION_FAILED` finding the contract requires. `createConnector` guards every read it makes
  of the connector's own thrown value — seven guards over eight reads, and none of them in
  `invokeConnector` — so that value reaches the harness intact and the execute error is still audited.

  Say what each diagnostic observed, and no more. A refusal on the supplied base transport reports the
  host the registered egress declaration does not cover, instead of asserting a bypass of
  `runtime.fetch` that did not happen — the connector used the transport the harness gave it. A
  `POLICIES_NOT_WIRED` `audit` finding for a case whose invocation failed says the invocation ended
  before a witness could be recorded, instead of asserting the subject reached its gate boundary. An
  `INSTRUMENTATION_REPLACED` finding for a replacement that is itself an accessor says the harness did
  not check whether later calls reached the trap, instead of leaving a silence that read as though it
  had. `SUBJECT_UNREGISTERED` names this copy of `createConnector()`, which is what a connector from a
  second copy of the package fails against. A thrown function is described as `a function` rather than
  by its source text. An escape whose address cannot be parsed — including one the URL global's
  disappearance makes unparseable — is recorded with a null host instead of raising inside the
  connector's own call.

  Record an escape observed after its case settled. A connector that keeps the supplied base transport
  or a trap reference alive past its case used to append to the case result's `escapes` array with no
  finding beside it, and — once the report was built — beside a `conformant: true` the caller was
  already holding. A case result now carries a snapshot of its own escapes, taken where those escapes
  become findings; a later observation is a run-level `NETWORK_IO_OUTSIDE_RUNTIME_FETCH` finding whose
  reason names the settled case and carries no `case`. The report snapshots `findings` and `cases` and
  computes `conformant` from the snapshot, and an escape observed after that is dropped.

  `CONFORMANCE_LIMIT` points at
  [Conformance limits](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits)
  in the connector authoring guide, which states what a settled case's retained transport or trap
  reaches, what a read of the restored global after the run closes bypasses, and what a timed-out
  case's abandoned work never reaches.

- 7fec4af: Name the settled case on a late conformance finding as a field, not only in prose.
  `ConnectorConformanceFinding` gains an optional `observedAfterCase`, which carries the name of the
  case whose abandoned work produced a run-level finding — an escape or a finding that arrived after
  that case settled. Such a finding still carries no `case`, because the case's own result is already
  on the report, and its `reason` is unchanged, so a host that reads the sentence keeps reading it. A
  finding the probe phase produces carries no `observedAfterCase`: no case had settled.
- f48a525: Point the connector conformance report's `limit` at the documented channel list. The field carried a
  paragraph naming the channels a run does not observe; it is now one sentence naming the permanent
  URL of
  [Conformance limits](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits),
  a section of the connector authoring guide that ships with the package and describes channels a run
  observes and channels it does not. A host that displays or stores `report.limit` sees the shorter
  text, and a host that asserts on its content asserts on the new sentence.
  `CONFORMANCE_LIMIT` is not exported from the package entry points, so consumers read it only as
  `report.limit`. Nothing about what the harness traps, records, snapshots or drops changes.
- aa81302: Sample a caller's connector definition once at construction. `createConnector()` read several members
  of the `config` and `policies` objects it was handed more than once while validating them, so a
  definition whose members are accessors could answer a refusal check with one value and the
  construction that followed with another. Construction now reads `policies.idempotencyStore`,
  `policies.idempotencyKeyMigration`, `policies.rateLimitStore`, `policies.networkEgress`,
  `config.permissions`, `config.inputSchema`, `config.outputSchema` and `config.dryRunExecute` once
  each; for an accessor-backed definition, the value that reaches the gate, the audit `detail` and the
  store is the first read. `invokeConnector()` reads `options.toolCallId` once, so the value it
  validates is the value it records on the call. The refusal messages and the order they fire in are
  unchanged, and a definition built from plain data properties behaves as it did before.

  The dry-run branch and the legacy key migrator still read `config.dryRunExecute` and
  `policies.idempotencyKeyMigration` on each call, so a simulation or a migration acknowledgement
  supplied after construction still takes effect from the next call.

- 8b7f087: Export `assertConnectorConformance`, a case-scoped harness a consumer runs in its own suite. For each
  supplied case it builds the connector through a factory, hands it a trusted inert base transport and
  an audit logger to wire, and replaces `globalThis.fetch` and any supplied transport entry point with
  a trap that records the attempt and refuses. A case that reaches one fails with the named result
  `NETWORK_IO_OUTSIDE_RUNTIME_FETCH`, including when the connector catches the refusal. The supplied
  base transport refuses any host the registered manifest does not declare, so calling it around the
  guard fails the same way.

  On an absent or configurable entry point, the harness installs an accessor whose getter returns the trap. An assignment to that instrumented entry point is recorded when it happens as `INSTRUMENTATION_REPLACED` and is not applied; the trap stays in place. A writable non-configurable data property uses assignment installation, which offers no defence against assignments during execution. A redefinition or assignment still in place when the case settles or times out, or when the probe factory returns, is reported as `INSTRUMENTATION_REPLACED`. A redefinition or deletion the case itself reverses before it settles is one of the channels listed under [Conformance limits](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits). When verification fails, the case is ineligible for the outcome check and proves `nothing`. An `INSTRUMENTATION_UNSUPPORTED` or `INSTRUMENTATION_REPLACED` finding during probe construction refuses the run.

  A case reports `CASE_INVOCATION_FAILED` when invocation setup fails or the invocation itself fails; the reason names the error's constructor, uses `unknown` when that name is unavailable, unreadable, or not a plain identifier of at most 64 characters, or describes a thrown non-Error value by type, without the message or value. During invocation, policy denials, boundary errors, and harness refusals retain their existing classifications, and a setup failure carrying a harness refusal is reported as the escape behind it rather than as an invocation failure.

  The harness requires `TextEncoder` with its other host globals before a run starts, reports an entry point replaced before an install failure unwinds it, and names the phase — a settled case, or the probe factory — when an attempt or a finding arrives after it.

  Instrumentation is restored after the case settles, after a throw, after a partial install, and after
  a per-case timeout; a restoration that cannot be proved fails the run and ends it, rather than
  running later cases over a property the harness knows it could not put back. Entry points are
  instrumented **one at a time, in order, `globalThis.fetch` first**, and every property the harness
  writes is checked before it is written and verified after — both its own descriptor and the value a
  caller actually reads. An accessor, an inherited accessor, a locked property descriptor, a target
  that accepts a write and ignores it, and a target whose descriptor holds the replacement while the
  property still resolves to the original are all refused rather than skipped — for a supplied entry
  point the case is refused; for `globalThis.fetch` the whole run is when the refusal comes before any
  case runs, and the case is when a case's own work makes the global uninstrumentable mid-run. Either
  way the finding names the descriptor shape it found, and a failure at any entry point restores the
  whole rollback stack: a (d) install or (e) verification failure includes the failing entry point; an
  (a) validation or descriptor-read failure precedes capture and push, so the stack holds only the
  entry points attempted before it. A restore the target silently ignores
  is reported as a failed restoration. Two entry points naming one
  property is refused before any case runs, and so are two cases sharing a name, two entry points
  sharing a label, and an overlapping or nested run. A factory that throws is reported as a
  finding, not an opaque rejection. Each case is bounded by `timeoutMs`, 2000 ms by default; a case
  that times out ends the run and no further run is accepted in that isolate, because work abandoned by
  one case would otherwise be recorded against a later one. Put a test that expects a timeout last in
  its file, or in a file of its own.

  The harness certifies only a connector declaring `egressEnforcement: 'enforced'`. An empty case set
  is reported as an empty case set, and — for a connector that declares `egress` — a set whose cases
  never reach the supplied transport is reported as no transport evidence; neither is a pass, and a run
  with no cases raises one finding, not both. A case whose expectations depend on policies the factory
  did not wire is reported as a wiring failure naming the member, alongside the escape record, which is
  kept. Every report states the finite-case limit:

  > conformance covers only the supplied cases, in this isolate, for the duration of each case; channels a run observes, and channels it does not, are described under Conformance limits in the CONNECTORS.md that ships with this package, at https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md#conformance-limits

  The harness itself uses no Node built-ins, no `vm`, and no filesystem, and runs on workerd. The
  barrel it ships from imports `@mastra/core/tools`, whose bundled chunks statically import Node
  built-ins under unprefixed specifiers, so a Worker importing the barrel needs Node.js compatibility
  enabled — the `nodejs_compat` compatibility flag, or a `compatibility_date` recent enough that your
  Workers runtime turns it on by default; check your runtime's compatibility-date documentation for
  that date. The flag is a necessary condition, not a sufficient one: the workerd build behind the
  runtime also has to load the barrel. Loading it crashed `workerd@1.20260730.1` during module
  resolution in this package's own workerd test pool, and `workerd@1.20260903.1` loads it, which is
  the build this repository pins through a pnpm override on `miniflare@5.20260730.0-alpha`.

- 7eb450f: Add an egress posture to the connector manifest. `permissions.egressEnforcement` declares whether
  the declared hosts bind the connector's actual traffic — `'enforced'` when every **HTTP** request
  leaves through `ConnectorRuntime.fetch`, `'declaration-only'` when a vendor SDK or child process
  carries its own transport. It is a claim about HTTP traffic, not about platform bindings (D1, KV, R2,
  service bindings), which the guard never sees, so a connector that issues no HTTP request at all is
  `'enforced'`. An omitted field resolves to `'declaration-only'`.

  `connectorEgressPosture(tool)` reads the resolved posture beside `connectorManifest(tool)`, and every
  connector audit event carries it as `detail.egressEnforcement`, so an operator can answer from the
  log which connectors declare enforcement. The logged value is the author's declaration resolved
  against the omitted-field default, not an observation of the connector's traffic.

  `policies.requireEgressEnforcement` refuses, at construction, a connector whose posture is not
  `'enforced'`; the single-tenant preset accepts and pins the same flag. Construction also rejects an
  `egressEnforcement` value outside the two literals. The Agent CLI adapters declare
  `'declaration-only'`, matching their documented child-process boundary — so a `createConnector()`
  call whose `policies` set `requireEgressEnforcement` cannot register an Agent CLI adapter, by
  design. Put the child behind a host network boundary, and pass that flag on the calls whose
  connectors declare `'enforced'`.

  Migration: `connectorManifest(tool)` returns `egressEnforcement` for every connector built by an
  Agent CLI factory, which the adapters declare as `'declaration-only'`. An assertion comparing a
  returned manifest for exact equality with a literal fails until that key is added to the expected
  object.

- 37c0fee: Add stable connector decision codes, canonical policy categories, retryability and safe structured details to authored errors and audit events. Preserve custom policy names and the three-string policy-error constructor.

  Wrap store and evaluator failures with typed errors. Pre-execution store failures retain their original cause at the base connector boundary; Agent CLI adapters preserve the classification without exposing raw causes. Post-effect commit and best-effort release failures retain their existing suppressed disposition and non-retryable audit codes. Direct invocation and validation errors gain stable tags, and egress refusals retain their transport checks.

  Contain audit error-observer failures so they cannot replace completed connector results, release a pending reservation after failed result storage, or replace an original execution error. Rejected observer promises stay isolated too.

## 0.13.0

### Minor Changes

- b85a872: Add a supported connector invocation boundary for trusted hosts and workflows. Direct calls now preserve Mastra validation and Breakwater grants without fabricated tool contexts, and validation failures expose no rejected values or schema messages.
- fa0d11d: Add an optional content-policy boundary for agent signals. Breakwater exposes `createContentPolicyGate()`, a reusable opaque input-policy gate for host code outside Mastra's processor chain, and FlowSafe's thread signal routes accept a structural `contentPolicy` callback that inspects Mastra's canonical escaped XML before delivery, persistence, wake, or run start — covering direct ingestion, providers, schedules, and notification dispatch. Denial is terminal and evaluator failure stays recoverable on every lane; neither exposes policy names, reasons, content, or causes.

  Signal attributes whose keys are not XML names are now dropped when a signal is ingested, and a schedule whose stored target cannot be rendered settles a terminal discard receipt instead of failing every later tick with the same broken target.

  Provider deliveries now distinguish a terminal refusal from one the deployment could not decide: an undecided webhook is answered with 503 so the sender redelivers, and every delivery carries a dedupe key derived from the signed bytes and the subscription so a redelivery coalesces into a still-pending notification instead of duplicating it. Webhook and poll results report `denied`, `failed`, and `deferred` counts.

- 8f4daae: Require `@mastra/core` 1.53.0 exactly (previously 1.50.0). The peer is exact, so every consumer must move to 1.53.0 as well; this is breaking for consumers pinned to 1.50.0. 1.53.0 is the newest release whose published output still bundles for Cloudflare Workers and Vite: 1.54.0 through 1.60.0 inline Node-only dynamic imports (`execa`, `@ast-grep/napi`) that fail to bundle (mastra-ai/mastra#20638). `@mastra/cloudflare-d1` stays at 1.1.1. FlowSafe's `@proofoftech/breakwater` peer floor rises to `>=0.13.0` in step, that being the first Breakwater release built against the same core.

  FlowSafe's durable agent runner now refuses every inherited entry point that can drive execution outside `RunnerRuntime`, mint a run id below the caller, or hand back runs the caller does not own: the run-recovery entry points 1.53.0 adds to `DurableAgent` (`recover`, `recoverActiveRuns`, `listActiveRuns`); the resume family (`resume`, `resumeStream`, `resumeGenerate`, `approveToolCall`, `declineToolCall`, `approveToolCallGenerate`, `declineToolCallGenerate`), which since 1.53.0 rehydrate from snapshot storage on a run-registry miss; the agent-level discovery member `listSuspendedRuns`; the network family (`network`, `resumeNetwork`, `approveNetworkToolCall`, `declineNetworkToolCall`), which drives the multi-agent loop's own workflow on the default engine; the AI SDK v4 legacy pair (`generateLegacy`, `streamLegacy`), which runs the agent's tools while skipping the authorization check every supported entry point calls; and `sendToolApproval`, whose continuation branch starts a run under a generated run id rather than resuming. `deleteRunSnapshots` is refused on a separate ground: the snapshot rows it deletes belong to deployment-scoped retention rather than to any caller. Nineteen entry points in all. That leaves `resumeViaRuntime` as the only resume path and the guarded `stream`/`generate`/`prepare` as the only execution entry points. Surface tripwires now classify every `DurableAgent` prototype member and every inherited `Agent` member, so a future peer bump surfaces new entry points on either.

  This is a behavior change for any consumer that called those methods on a FlowSafe durable agent: they now throw instead of executing. Their TYPE signatures narrow too — the overridden members return `Promise<never>`, and the generic overloads several of them carried (`network`, `generateLegacy`, `streamLegacy`, `sendToolApproval`) collapse to a single refusing signature, so a call that no longer type-checks is the intended signal rather than a regression. Nothing in the supported agent-host surface reaches them — route clients through the agent-host run routes.

### Patch Changes

- 66c19f1: Clean generated output at the packaging boundary so deleted source modules cannot remain in published tarballs.
- 5cbe01d: Align the package and documented Node.js runtime floor with the required `@mastra/core` peer dependency.

## 0.12.0

### Minor Changes

- 37175fa: Fail closed on structured-output coverage gaps. `createGuardedAgent()` rejects structured output before model execution because Mastra exposes parsed values to messages, persistence, and observability hooks before a post-generation wrapper could inspect them. It also rejects object-only policies that no supported guarded invocation can cover.

  Processor-visible object chunks are validated as JSON, evaluated through their canonical serialization, and replaced with the same canonical clone. Standalone object-only policies abort when an invocation exposes no object to the processor. Policy lists and decision-driving descriptors are snapshotted at construction, evaluator callables retain their original receiver, and per-stream audit metadata stays bounded by configured policies and channels.

  Flowsafe recognizes the new guarded-agent host protocol and rejects structured output on durable stream, generate, and prepare before Mastra can bypass the narrow handle. Durable entry points snapshot data-property call options before validation and delegation, reject accessors, and use the same snapshot for later run registration. Both packages pin their tested `@mastra/core` 1.50.0 contract.

  Hold-back cost under large streams is measured by opt-in evidence tests (`BREAKWATER_PERF=1`) and recorded in the policy-engine design guide.

## 0.11.1

### Patch Changes

- a16ed60: Correct the Breakwater 0.11.0 release notes so shipped changes are no longer duplicated under `Unreleased`.

## 0.11.0

### Minor Changes

- 4f0fc9d: Use collision-proof connector idempotency keys with a fail-closed legacy
  migration boundary. Validate D1 pending TTLs and rate-limit counts, commit D1
  rate increments with cleanup atomically, and add trusted run correlation to
  connector audits. Agent CLI timeouts now terminate the descendant process tree,
  confirm POSIX group disappearance or Windows taskkill completion, and report a
  stable failure if termination cannot finish. Connector output is validated and
  transformed before replay commit so an invalid result is not stored under the
  idempotency key. D1 refuses non-JSON-native results that would change during
  persistence instead of creating a type-changing replay. Windows resolves
  taskkill from a drive-absolute local `SystemRoot` or `WINDIR` before starting
  the CLI rather than searching the working directory or `PATH`.

  This changes keyed-connector construction and rollout: hosts must acknowledge
  that legacy writers are drained before an absent legacy key may execute, and
  custom atomic stores must add non-mutating `inspect()` support. Safe legacy
  records still replay; ambiguous records remain denied until an operator maps
  them to one proven v2 identity. The connector-bound D1 migration helper
  validates the exact inventoried output through the connector schema and moves
  the guarded v1 row to v2 atomically without exposing storage keys. Custom
  `RateLimitDatabase` adapters must also
  provide D1-compatible transactional `batch()` semantics so cleanup failure can
  roll back the associated increment.

## 0.10.0

### Minor Changes

- af29901: Add `singleTenantConnectorPolicies()` for physically isolated connector hosts.

  The validated preset requires shipped D1 idempotency and rate-limit stores when a manifest declares those controls, an external production audit sink or an explicit development-only opt-out, organization egress policy, configured principal permissions, and the safe background-execution policy.

  Construction rejects tenant-isolation scope, in-memory durability stores, egress outside the organization allowlist, weakened destructive approval, contradictory audit settings, and modified branded presets. Existing unbranded `ConnectorPolicies` behavior remains unchanged.

## 0.9.0

### Minor Changes

- d78e779: Add optional connector invocation authorization.

  `PermissionManifest.requiredPermissions` declares an all-of list of canonical permission identifiers, validated at construction. The compiled execute path enforces it against the trusted `breakwater.principalPermissions` request-context projection before the dry-run branch and before approval-grant consumption, so a simulation still needs an authorized principal and a valid approval cannot elevate an unauthorized one. A missing, null, or malformed projection fails closed. A pass records a new `connector.authorize` audit event; it and the `required-permissions` denials record the required identifiers and the policy snapshot version, never the effective permission set.

  The `rbac` subpath now owns the shared permission vocabulary: `Permission`, `isPermissionIdentifier`, the `PrincipalPermissions` projection type, its `isPrincipalPermissions` guard, and `PRINCIPAL_PERMISSIONS_CONTEXT_KEY`.

## 0.8.0

### Minor Changes

- cb0f861: Replace connector ID approval arrays with structured connector grants. Durable-agent approvals now bind to the exact Mastra tool call, workflow approvals bind to the exact suspension, and standing grants require explicit run scope.

  This is intentionally breaking: `APPROVED_CONNECTORS_CONTEXT_KEY`, `BREAKWATER_APPROVED_CONNECTORS_KEY`, and `approvedConnectorsForLeg()` are removed. Legacy arrays and approval rows without explicit scope fail closed. Migrate trusted hosts to `CONNECTOR_GRANTS_CONTEXT_KEY`, `CONNECTOR_EXECUTION_CONTEXT_KEY`, and `connectorGrantsForLeg()`.

## 0.7.0

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

## 0.6.0

### Minor Changes

- 09a4406: Add guarded Breakwater agents and Flowsafe's authenticated, catalog-driven agent host. Agent starts now derive trusted identity and execution context, agent resumes require an approval-bound capability, and status and NDJSON observation remain tenant-bound.

## 0.5.0

### Minor Changes

- def3b37: Harden public connector and Agent CLI boundaries for the first public release.
  Agent CLI connectors now expose structured, redacted errors, pass workspace-edit
  permission flags to Claude Code and Codex, and keep prompts and option values out
  of diagnostics and audit events. Connector, policy-evaluator, and actor-lookup
  failures now emit static safe audit reasons. Add exhaustive export sentinels and
  a packed-tarball consumer test, move Zod to runtime dependencies, and publish
  complete package and connector guides.

## 0.4.0

> **Scope correction:** This package release added the `_background` permission
> and the `backgroundExecution` policy described in the first bullet below. The
> flowsafe storage, Durable Object host, recovery, and execution-status material
> was included by the shared changeset but is not part of breakwater. See the
> [flowsafe changelog](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/flowsafe/CHANGELOG.md)
> for that package's final behavior.

### Minor Changes

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

## 0.3.1

### Patch Changes

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

## 0.3.0

### Minor Changes

- 5011013: Fetch-level egress enforcement. `createConnector()` now hands `execute`/`dryRunExecute` a third argument, `ConnectorRuntime`, whose `fetch` is bound to the manifest's declared `egress`: every actual request — redirect hops included — must resolve to a declared host or it is denied (`ConnectorPolicyError`, policy `egress-fetch`) and audited before any bytes leave. Redirects are followed manually with a per-hop allowlist check, credential headers are stripped on cross-origin hops, non-http(s) schemes and unparseable URLs fail closed, and a manifest with no `egress` gets a fetch that denies everything. New exports: `egressFetch()` (standalone guard factory), `EgressDeniedError`, the structural fetch seam types (`EgressResponse`, `EgressRequestInit`, `EgressFetchBase`, `EgressGuardedFetch`, `EgressDenial`, `EgressFetchOptions`, `EgressResponseHeaders`), `ConnectorRuntime`, `ConnectorPolicies.fetch` (base-fetch injection seam for tests/instrumentation), and `egressDomainAllowed` (the shared host matcher). Existing connectors are unaffected — the third argument is additive and two-parameter `execute` implementations keep compiling; traffic that does not go through `runtime.fetch` (e.g. a vendor SDK's own HTTP stack) keeps the previous declaration-only posture, documented in `CONNECTORS.md`.

### Patch Changes

- df413da: `egressFetch` now releases each intermediate redirect response before following it or throwing. The manual redirect follower cancels the discarded 3xx's body stream, so a followed, hop-capped, egress-denied, or one-shot-refused redirect can no longer retain its connection until GC (Node/Undici, workerd) under sustained redirected traffic. Disposal is best-effort (a locked/errored stream's cancel rejection and an injected transport's synchronous throw are both swallowed) and never touches the response returned to the caller.
- 4fbc0be: Reviewed cleanup batch across the egress guard, tenant-id primitives, and approval self-decision paths - no observable contract changes and all 1119+ tests preserved.

  breakwater (patch): the egress host matcher and the allowlist validator are each a single shared definition (domainAllowed + assertEgressHostList, both driven by the one egressDomainAllowed match semantics), the normalized allowlist is computed once per construction instead of per hop, and the per-connector egress guard is built once at createConnector. egressFetch also treats an async-iterable (Node Readable) request body as one-shot so a 307/308 redirect no longer re-sends a consumed body, validates maxRedirects at construction, and fails closed on a browser opaque status-0 redirect response.

  flowsafe (minor): the tenant-salted ownership predicate and the id-mint rigor are hoisted into tenantOwnsSaltedId / assertMintableTenantId / mintSaltedId in do-runner/path-safe-id, and every live copy (runId and memory ownership, plus the approval write-path INV-1 belt) routes through them; mintSaltedId validates the tenant before evaluating a lazy suffix, so a caller-supplied uuid callback (mintThreadId's) can no longer run its side effects or throw ahead of the INV-3/reserved rejection. purgeTenant runs its three agent-memory deletes concurrently. The self-decision policy is threaded through createTenantResolver so TenantContext.canSelfDecide(role) is the single display hint the /workflows echo reads, and parseSelfDecision is memoized per deployment value. TenantContext gains a required canSelfDecide(role) member, BREAKING for hand-built TenantContext implementations (contexts from createTenantResolver get it automatically), hence the minor bump.

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

## 0.1.0 — 2026-07-11

First publishable cut. Mastra safety middleware: policy engine (output channels,
deny patterns, opt-in hold-back buffering), RBAC processor, audit sink, connector
SDK (permission manifests, grant-only write approval, network-egress declaration
gate, idempotent replay with in-memory/atomic/D1 stores, fixed-window rate
limiting, dry-run, tenant isolation scoping), and approval-gated Claude Code /
Codex CLI connectors.

Publish order: this package publishes BEFORE `@proofoftech/flowsafe` (flowsafe's
`./host-kit/module` subpath types reference it as an optional peer).

Requires `@mastra/core` ^1.50.0 (peer), Node >= 22, ESM only
(`moduleResolution` `node16`/`nodenext`/`bundler`).
