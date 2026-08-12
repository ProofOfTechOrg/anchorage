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

Each decision is either `{ allowed: true }` or `{ allowed: false, reason }`. An evaluator exception is a policy-engine failure, is audited as an error, and fails the request closed.

Policies run in array order. The first denial aborts the phase.

## Phases and channels

Input processing joins textual message parts and evaluates them under the `answer` channel. Output processing maintains independent accumulated text for:

| Channel | Source |
| --- | --- |
| `answer` | Client-visible answer text |
| `reasoning` | Reasoning stream deltas |
| `object` | JSON-stringified structured-output snapshots |

A policy defaults to both phases and the `answer` channel. Set `phases` and `channels` when a policy applies more narrowly.

Mastra exposes structured objects to the processor on the streaming path. Under the supported core version, the non-streaming output result has no separate object field. JSON carried in answer text is still inspected by policies that include `answer`; an object-only policy has no final `generate()` coverage.

## Built-in content policies

### Deny patterns

`denyPatterns(patterns, options)` performs literal or regular-expression-style configured matching according to its exported options. Its streaming implementation scans only the new suffix plus the largest pattern overlap. The default channels cover answer, reasoning, and object so a forbidden string cannot move to another output surface.

Denial reasons identify the configured pattern, not the matched input span. Do not configure a literal secret itself as a pattern if the reason will enter a lower-trust audit sink.

### Maximum length

`maxTextLength(limit, options)` denies accumulated text beyond a configured bound. Apply different policies per channel when answer and reasoning budgets differ.

### PII and secrets

`piiSecrets(options)` combines:

- email, phone, and US Social Security number patterns;
- Luhn-validated payment-card candidates;
- AWS access key, JWT, PEM private-key header, and secret-assignment patterns;
- high-Shannon-entropy token detection with a minimum candidate floor;
- allowlist exemptions;
- streaming overlap windows sized to the enabled detectors.

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

Input and final-result phases always classify. During append-only streaming, the evaluator runs when accumulated text grows by the configured cadence; object snapshots classify individually.

A timeout or classifier failure fails closed. No fail-open option is provided.

## Hold-back and leakage

Without hold-back, a streaming policy can detect a violation only after enough of the matching span has arrived. Earlier clean-looking characters may already have reached the client.

`new PolicyEngine({ holdBack: true, ... })` retains a trailing window per answer and reasoning channel. The largest `holdBackChars` hint among applicable policies wins. Once a passing buffer exceeds the window, the older portion is emitted. The tail is reprocessed at the channel end or stream finish.

Object snapshots are replacement values rather than append-only text, so intermediate snapshots are suppressed and only a passing result is emitted.

Properties:

- The guarantee is per segment, because end markers flush each segment.
- A policy with no hint adds no window.
- A finite pattern policy can provide its maximum match span minus one.
- A classifier that must see the full output should opt into `holdBackChars: Infinity`, accepting full buffering.
- Hold-back changes delta boundaries. Consumers must treat text deltas as chunks, not semantic tokens.

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

Use it only in a host that has another trusted logical partition and mints the scope on every path, including dry runs. Breakwater does not parse the value. Flowsafe's physically isolated data plane deliberately mints no isolation scope and drops provider attempts to add one, so its connector budgets are deployment-wide.

### Background execution

`backgroundExecution()` and the connector wrapper protect Mastra's `_background` model override. A connector is foreground-only unless its manifest declares `background: true`, and only a read-only connector can opt in. Write, destructive, and idempotent connectors remain foreground-only. A read-only connector may opt in even when its manifest separately requires approval; the grant check still runs at execution.

Schema validation may strip `_background` on some agent paths. The connector check still protects no-schema, passthrough, workflow, and direct calls.

### Custom evaluators

Add evaluators through `ConnectorPolicies.evaluators`. Keep them deterministic and side-effect-free; they run before the connector and before a dry-run return.

An evaluator may inspect trusted request-context values, but must never promote client input into an approval grant or isolation scope.

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

Every allow, denial, and gate failure emits structured audit. Arbitrary thrown values are mapped to static safe audit reasons rather than copied into audit output.

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
