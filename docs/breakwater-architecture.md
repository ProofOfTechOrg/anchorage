# Breakwater architecture

Breakwater is the guardrail package in Anchorage. It integrates with Mastra at two different boundaries:

- Mastra processors authorize and inspect an agent's input and output.
- `createConnector()` wraps a Mastra tool so permission checks run immediately before every execution path.

These boundaries solve different problems. A processor cannot protect a tool invoked directly or from an ordinary workflow step, while a tool wrapper cannot inspect the model's full response stream.

## Public modules

| Module | Responsibility |
| --- | --- |
| `@proofoftech/breakwater/agent` | Guarded agent construction, mandatory processor composition, and narrow direct execution |
| `@proofoftech/breakwater/policy-engine` | Input/output processor, content policies, and tool-policy evaluators |
| `@proofoftech/breakwater/rbac` | Actor contract, five roles, and agent input authorization |
| `@proofoftech/breakwater/audit` | Shared audit logger, metrics adapter, and sink fan-out |
| `@proofoftech/breakwater/connector-sdk` | Enforced permission manifest, guarded fetch, replay, dry-run, rate limit, and stores |
| `@proofoftech/breakwater/agent-cli` | Approval-gated Claude Code and Codex workspace connectors |

The package root re-exports all six modules, including the guarded agent and agent CLI. Prefer the explicit `@proofoftech/breakwater/agent-cli` subpath for CLI connectors so their Node-specific process boundary remains visible at the import site.

## Enforcement flow

```text
verified host identity
        |
        v
Mastra RequestContext
        |
        +--> guarded preauthorization -> allow or abort before app code
        |
        +--> application input -------> initial-input-only processing
        |
        +--> PolicyEngine input ------> inspect all input text
        |
        v
model and agent loop
        |
        +--> createConnector tool ----> declaration egress
        |                              custom evaluators
        |                              required permissions
        |                              approval grant
        |                              idempotency reservation/replay
        |                              shared rate budget
        |                              runtime.fetch per-hop egress
        |
        v
PolicyEngine output stream/result ----> inspect answer, reasoning, object
        |
        v
client

Every gate ---------------------------> shared AuditLogger
```

Array order determines processor order. A denial aborts the chain, so `AuditLogger` is an injected sink rather than a processor placed after the gates.

## Agent-boundary integration

`createGuardedAgent()` is the supported construction path for protected agent execution:

```typescript
import {
  AuditLogger,
  createGuardedAgent,
  denyPatterns,
  piiSecrets,
} from '@proofoftech/breakwater';

const audit = new AuditLogger();
export const agent = createGuardedAgent({
  id: 'guarded-agent',
  name: 'Guarded agent',
  instructions: 'Act only within the supplied permissions.',
  model: 'openai/gpt-5',
  allowedRoles: ['operator', 'admin'],
  policies: [
    denyPatterns(['ignore previous instructions']),
    piiSecrets(),
  ],
  audit,
  maxSteps: 4,
  toolChoice: 'auto',
});
```

The factory constructs a protected `Agent` subclass but returns `GuardedAgentHandle`. Its package-local brand lets the Flowsafe agent host validate the object before its internal Mastra cast. No public unwrap operation exists.

Direct calls preauthorize the `breakwater.actor`, then pass exact application and mandatory processor arrays to Mastra. Initial durable preparation uses the subclass processor listing. Approval resume after isolate eviction uses Flowsafe's registry rehydration path, which authorizes the fresh trusted resume context without replaying initial application or policy input. These paths have these orders:

```text
direct: RBAC preauthorization -> app input -> policy input
initial durable: RBAC processor -> app input -> policy input
durable resume processInput during rehydration: RBAC only
output: model/tools -> app output -> policy output
```

The durable resume line describes which `processInput` hook runs during rehydration. Before installing the registries, Flowsafe restores the complete input and LLM-request processor lists for later loop hooks. It also restores the same tools, memory, model, application output processors, and mandatory policy output processor as initial preparation. An RBAC denial stops rehydration before registry installation or resumed tool execution.

The call allowlist contains `requestContext`, `runId`, `memory`, and `abortSignal`. Calls are copied into frozen allowlisted snapshots, and unknown own properties fail even when their value is `undefined`. Construction fixes `maxSteps` and `toolChoice`, enables policy hold-back, and disables background continuations.

Application input processors may implement only `processInput`. Application output processors must implement both stream and final-result enforcement. Processor workflows and the reserved IDs `breakwater-rbac` and `breakwater-policy-engine` fail construction.

`RBACMiddleware` remains available for lower-level composition and reads `breakwater.actor` by default. The host must verify identity before creating that context. Its flat role membership check is intentionally smaller than an identity provider or general access-control list service.

`PolicyEngine` can gate:

- input message text;
- answer deltas and final text;
- reasoning deltas;
- structured-object stream snapshots;
- custom synchronous or asynchronous policy evaluators.

`createContentPolicyGate()` exposes the same ordered input-policy decision as
an opaque host callback for content paths that do not traverse Mastra's input
processors. The host supplies the exact model-visible text and trusted request
context, then must stop before model, persistence, wake, or run-start side
effects on denial or evaluator failure.

Under the supported Mastra version, structured objects parsed by `generate()` and the chunks core's `StructuredOutputProcessor` emits never pass through the agent's output processors. Mastra also copies a parsed value into messages and may send it to persistence and observability hooks before `generate()` returns. A post-generation wrapper gate is therefore not a containment boundary. The guarded agent rejects structured output before execution and rejects object-only policies at construction. A standalone `PolicyEngine` validates processor-visible object chunks as JSON, evaluates their canonical snapshots, forwards the same canonical clones, and aborts at the result boundary when an object-only policy saw no such chunk.

The guarded handle also carries a versioned host protocol. Flowsafe checks that protocol before durable wrapping and rejects guarded structured output on every durable entry point, preserving the narrow handle's refusal even though Mastra's durable runner invokes the raw agent through processor lists.

## Streaming behavior

The engine accumulates each output channel independently. Built-in incremental policies keep scan cursors so work grows with the new data rather than rescanning the entire stream.

With `holdBack: true`, the engine retains the largest trailing window requested by policies on each answer or reasoning segment. Clean text behind the window is released; the tail is reprocessed at the channel end. Structured-object intermediate snapshots are withheld until a passing result.

The guarantee is per stream segment. A custom policy participates only when it supplies an appropriate `holdBackChars` hint. An asynchronous classifier has no natural bounded window; use `Infinity` when the full segment must remain buffered until classification.

## Tool-boundary integration

`createConnector()` returns a Mastra `Tool` with a permission manifest compiled into its `execute` path. The gates apply to agent calls, workflow calls, nested calls, and direct calls.

The execution order is:

1. Validate connector construction and input.
2. Check declared egress against the host's organization allowlist.
3. Run custom tool evaluators such as opaque logical-scope and workflow isolation.
4. Require the trusted principal-permissions projection to hold every declared `requiredPermissions` identifier — before dry-run selection and before the approval grant, so an approval cannot elevate an unauthorized principal.
5. Select dry-run or real execution.
6. Require the server-derived connector grant when policy demands approval.
7. Reserve or replay an idempotency key.
8. Consume a shared fixed-window rate budget.
9. Call the connector with a `ConnectorRuntime.fetch` bound to declared egress.
10. Validate output and commit the replay record.
11. Audit the final decision or contained error at the gate that observed it.

Mastra's native `requireApproval` predicate is also set so an agent loop can pause for a reviewer. It does not replace the request-context grant. The wrapper remains the authority on every invocation path.

Read [Connector interface](connector-interface.md) for the complete contract.

## Egress boundary

The manifest declaration and injected fetch are separate checks:

- `networkEgress()` compares the connector's declared hosts with deployment policy before execution.
- `runtime.fetch` checks the actual URL and every redirect hop against the connector declaration.

Cross-origin redirect hops strip credential headers. A 307/308 redirect with a one-shot stream body is refused because replaying it safely is impossible.

This is not socket interception. Code that calls global `fetch`, opens a socket, or uses an SDK with an independent HTTP stack bypasses runtime-fetch enforcement. Inject `runtime.fetch` into compatible SDKs and apply infrastructure egress controls around the process.

## Replay and rate budgets

An idempotency record can be:

- absent, so the caller reserves it;
- pending, so a same-key caller waits or a durable store later takes over a stale reservation;
- complete, so the stored result is replayed without execution.

`AtomicIdempotencyStore` uses lease tokens for release and stale takeover. `D1IdempotencyStore` uses an insert claim and compare-and-swap updates across isolates. Set its pending TTL longer than the maximum connector execution.

The connector derives opaque v2 idempotency keys from tagged, encoded tuple
components. Before using v2 it inspects the exact legacy key. Safe unscoped
records replay, ambiguous scoped or colon-bearing records deny, and an absent
legacy row requires the host's explicit drained-writer acknowledgement.
`InspectableIdempotencyStore` makes pending legacy state visible without a
mutating reserve/release probe.

Rate limits use fixed windows. A burst across adjacent windows can approach twice the nominal count, and clock skew can amplify this across isolates. Use `D1RateLimitStore` when the budget must be shared across per-run Durable Objects.

Only actual executions consume the budget. Denials, dry-runs, stored replays, and joined in-flight twins do not.

## Isolation context

Breakwater remains organization-agnostic. Its `Actor` has no organization id.

A trusted generic host can write an opaque value to `breakwater.isolationScope`. The connector SDK uses it to segment idempotency and rate keys, and `tenantIsolation()` refuses a call without it. Breakwater does not parse the scope.

Similarly, `breakwater.workflowScope` identifies the current runtime leg for `crossWorkflowIsolation()`. A connector-specific `targetScopeOf` extracts the workflow a call wants to access. Missing caller scope or a different target fails closed.

Flowsafe mints the workflow value on every run leg. It reserves and strips `breakwater.isolationScope` because each data plane serves one organization; connector keys remain deployment-wide.

## Audit and metrics

Every gate writes an `AuditEvent` with timestamp, actor, action, resource, decision, optional reason, and structured detail.

Guarded agents use `agent:<agentId>` as the RBAC and policy resource. A trusted host may set `breakwater.auditContext` with `agentId`, `tenantId`, `runId`, `threadId`, `resourceId`, `entryPath`, and principal correlation. Breakwater copies only these scalar fields into agent, policy, and connector audit detail. Trusted context wins over same-named boundary detail. Flowsafe maps its verified deployment tag to the legacy `tenantId` audit field; request claims cannot set it. Breakwater does not copy prompts, tool inputs, URLs, secrets, or model output.

`AuditLogger` keeps a bounded in-memory ring and invokes its sink without making export availability part of the agent path. Supply `onSinkError` to surface failures.

`metricsAuditSink()` maps events to:

- `breakwater.audit.decision`, tagged by action and decision;
- `breakwater.audit.duration_seconds`, when a non-negative finite duration is present.

`combineAuditSinks()` runs every sink and aggregates failures after all settle. Flowsafe's Queue sink can carry the same events to a SIEM.

## Agent CLI boundary

The Claude Code and Codex adapters are write-class connectors. They preserve the connector grant, idempotency, rate-limit, audit, and dry-run behavior, then add:

- an argv array with no shell;
- an explicit `--` before the prompt;
- option values bound with `--flag=value`;
- a trusted working directory;
- timeout and bounded UTF-8 output capture;
- prompt-free commands, errors, validation, replay output, and audit summaries;
- static workspace-edit permission flags for the supported vendor CLIs.

The default runner requires Node. Infrastructure must still isolate the child process, workspace, credentials, and network.

See [Agent CLI connectors](agent-cli-connectors.md).

## Ownership split

Breakwater does not implement:

- authentication or OAuth;
- workflow route authorization;
- a deployment provisioner or organization registry;
- approval queue persistence or reviewer UX;
- durable workflow execution;
- data retention scheduling;
- network perimeter isolation.

Flowsafe owns workflow route authorization, durable approvals, runtime-derived grants, actor execution context, deployment-sentinel verification, D1 retention, and Cloudflare deployment helpers. The host owns identity, one-to-one resource provisioning, business policy, secrets, infrastructure boundaries, and which optional features are exposed.

See [Product boundaries](breakwater-purpose-and-boundaries.md) for the Mastra comparison.
