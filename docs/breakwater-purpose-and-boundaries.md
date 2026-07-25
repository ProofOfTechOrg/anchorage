# Breakwater: Purpose, Mastra Comparison, and Enforcement Boundaries

This document answers two recurring questions:

1. What RBAC does breakwater actually provide for agents?
2. What is breakwater's full purpose, and what does it add beyond Mastra?

The short answer is:

> Mastra builds and runs agents, tools, and workflows. Breakwater adds reusable,
> fail-closed safety middleware and enforced connector contracts. Flowsafe
> supplies the trusted multi-tenant host, durable execution, approval service,
> workflow-route authorization, and retention jobs that make those contracts
> operational.

Breakwater is not a replacement agent runtime, identity provider, complete IAM
system, or standalone policy service. It is an Apache-2.0 library that uses
Mastra's extension points and wraps Mastra tools so safety checks remain in
force at the boundary where side effects occur.

## Scope and Version Context

This repository currently targets `@mastra/core@^1.50.0`. The comparison below
also accounts for Mastra features announced through 2026-07-20, including core
auth plumbing, Enterprise RBAC/ACL, native tool approval, built-in processors,
observability, and tool hooks.

Mastra changes quickly. The durable distinction is not that Mastra has no
guardrails. It is that breakwater packages Anchorage's policies into a
self-hostable enforcement layer, especially around connectors, and applies
them outside the agent loop as well as inside it.

## The Three-Layer Model

| Layer | Primary responsibility | Examples |
|---|---|---|
| **Mastra** | Agent and workflow execution primitives | Models, agents, tools, memory, processors, workflows, suspend/resume, snapshots, tracing, evals |
| **breakwater** | In-process safety gates and enforced connector contracts | Agent-turn RBAC, policy evaluators, connector manifests, approval-grant checks, egress, idempotency, dry-run, rate limits, decision audit |
| **flowsafe** | Trusted production host and durable control plane | Authentication adapters, tenant context, workflow HTTP RBAC, approval queue, separation of duties, durable run/resume, grant minting, retention and tenant purge, audit export |

This split matters because several controls commonly described as "breakwater"
are actually composed controls:

- Mastra supplies the native agent/tool/workflow primitive.
- Breakwater defines and enforces the local safety contract.
- Flowsafe authenticates the caller, mints trusted context, persists approval
  state, and drives durable execution.

## What Mastra Already Provides

Mastra is the execution substrate. It already provides:

- Agent model routing and tool calling.
- Input and output processor chains with abort/tripwire behavior.
- Built-in processors for categories such as PII, moderation, prompt
  injection, token/cost controls, and structured output.
- Tools with input/output schemas and `requireApproval`.
- Agent-level tool approval and approve/decline/resume mechanics.
- Workflows with branching, parallelism, retries, suspend/resume, and persisted
  snapshots.
- Memory, MCP integration, tracing, logs, metrics, and eval scorers.
- Pluggable authentication contracts in core.
- Enterprise RBAC/ACL, SSO/IAM integrations, and hosted Enterprise audit logs.
- Agent and tool hooks that can observe, validate, audit, or block tool calls
  made through the agent path.

Breakwater must therefore not be described as providing these primitives from
scratch. It builds on them.

## What Breakwater Adds

### 1. Agent-Boundary RBAC

`RBACMiddleware` is a Mastra input processor that runs before the model call.
It provides:

- Five recognized role labels: `admin`, `builder`, `operator`, `reviewer`, and
  `viewer`.
- An `Actor` shape of `{ id, role }`.
- Default actor lookup from `requestContext['breakwater.actor']`.
- A custom `getActor` seam for host-provided identity resolution.
- A per-agent flat `allowedRoles` allowlist.
- Fail-closed denial when the actor is absent, malformed, or not allowed.
- Audit events for allowed, denied, and errored authorization decisions.

The authorization rule is intentionally small:

```ts
if (!allowedRoles.includes(actor.role)) abort();
```

It answers only:

> May an actor carrying this role begin this agent model turn?

It does **not** provide:

- Authentication, token validation, user lookup, or session management.
- A role hierarchy; `admin` does not implicitly inherit another role.
- Permissions such as `reports.read` or `customers.write`.
- User-, group-, team-, tenant-, record-, or resource-level grants.
- Per-tool authorization based on the human actor's role.
- A centralized agent catalog or agent-to-role policy store.
- Automatic installation on every `Agent`.
- Tenant identity in the breakwater `Actor`; the host owns tenancy.

Every protected agent must explicitly install `RBACMiddleware` in its
`inputProcessors`, or invoke the same processor through a wrapper that cannot
be stripped by per-call overrides. The flowsafe durable-agent wrapper preserves
processors already present on its input `Agent`, but does not inject RBAC on
its own.

In the current repository, the middleware is thoroughly tested and is also
demonstrated by the scripted showcase control-room scenario. It is a shipped
library primitive, not yet a centrally auto-wired access-control plane for all
agents.

### 2. Agent Input and Output Policy Evaluation

`PolicyEngine` is registered in Mastra's input and/or output processor arrays.
It provides composable evaluators over:

- Input text.
- Client-visible answer text.
- Reasoning text.
- Streaming structured-object snapshots.

Shipped policies include:

- Denied text patterns.
- Maximum text length.
- PII and secret inspection using regex detectors, Luhn validation, and
  entropy checks.
- A pluggable asynchronous classifier with fail-closed timeout/error behavior.
- Optional streaming hold-back so a forbidden value spanning chunk boundaries
  can be detected before the trailing text is released.

This overlaps with Mastra's built-in processors. Breakwater's incremental
value is its specific detector set, channel semantics, hold-back behavior,
custom evaluator contract, and shared decision-audit path. It should not be
positioned as the only way to implement content guardrails in Mastra.

Content inspection remains best-effort against adversarial encoding or
obfuscation. It is not a formal data-loss-prevention guarantee.

### 3. Enforced Connector Permission Manifests

The connector SDK is breakwater's strongest differentiator.

A normal Mastra tool defines its schema and `execute` function. A breakwater
connector is still a real Mastra tool, but it additionally declares an
enforced manifest:

```ts
permissions: {
  sideEffect: 'write',
  egress: ['api.salesforce.com'],
  idempotencyKey: true,
  dryRun: true,
  rateLimit: '100/min',
  requiresApproval: true,
}
```

`createConnector()` wraps `execute`, so the safety checks apply when the tool
is called by:

- An agent.
- A workflow step.
- Another tool or connector.
- Direct application code.

This caller-independent placement is important. Mastra agent processors and
agent-level tool hooks guard calls traveling through the agent loop. A wrapper
around `execute` guards the side-effect boundary regardless of how execution
arrived there.

The manifest provides:

- `sideEffect`: `read`, `write`, `destructive`, or `idempotent`.
- `egress`: the network hosts the connector declares it may contact.
- `idempotencyKey`: require and store a replay key.
- `dryRun`: require an explicit side-effect-free simulation implementation.
- `rateLimit`: a connector-local fixed-window budget.
- `requiresApproval`: require a trusted approval grant before execution.

The wrapper also emits truthful MCP annotations, but those annotations remain
descriptive. Enforcement is performed by the wrapper and policy evaluators.

### 4. Network-Egress Enforcement

Breakwater implements egress in two layers:

1. A declaration gate verifies that every hostname in a connector manifest is
   allowed by the organization policy.
2. `ConnectorRuntime.fetch` checks each actual HTTP(S) request against the
   connector declaration, including redirect hops.

The guarded fetch rejects malformed URLs, non-HTTP(S) protocols, undeclared
hosts, and unsafe redirects before the base fetch sends the request. It also
strips credential headers on cross-origin redirects.

The boundary is important: this is not a process-level network sandbox. A
connector that uses global `fetch`, raw sockets, or a vendor SDK with its own
unadapted transport can bypass the runtime fetch. Connector authors must route
all network traffic through `runtime.fetch`; otherwise enforcement degrades to
checking only what the manifest claims.

### 5. Approval as a Trusted Capability

Mastra's native `requireApproval` supplies the agent pause and approve/decline
mechanics. Breakwater compiles its approval decision into that native option,
but does not treat a resumed agent run as proof that a write is authorized.

Immediately before `execute`, the connector also requires its ID in:

```text
requestContext['breakwater.approvedConnectors']
```

This second check makes the approval a trusted capability:

- A forged resume does not authorize execution.
- Direct and nested calls cannot bypass the approval rule.
- Each retry or resumed leg must carry the grant again.
- Destructive connectors require approval by default unless explicitly
  configured otherwise.
- Organization policy can require approval for connector patterns such as
  `salesforce.*` or `github.*`.

Breakwater checks the grant, but does not mint it. Flowsafe's approval service
derives grants from approved, persisted records and injects them into the
trusted per-leg request context. Client bodies, model output, tool input, and
stored untrusted context must never be allowed to populate this key.

### 6. Idempotency and Rate Limiting

For idempotent connectors, breakwater stores results under a key derived from
the connector ID, caller-provided idempotency key, and optional isolation
scope. It provides:

- In-memory stores for development and tests.
- D1-backed durable stores.
- Atomic reservation where supported.
- Concurrent duplicate suppression.
- Completed-result replay.
- Failure release so failed attempts remain retryable.
- Stale-pending takeover protection with lease tokens.

Rate limiting uses fixed, epoch-aligned windows and can be backed by in-memory
or D1 stores. It counts real executions, not denials, cached replays, or
same-attempt joins.

Fixed windows can admit close to twice the nominal budget across adjacent
window boundaries. A hard smooth-rate guarantee requires a different
algorithm such as token bucket or GCRA.

### 7. Workflow and Tenant Isolation Inputs

Breakwater is intentionally tenant-agnostic. It consumes opaque, host-minted
context rather than owning tenant identity:

- `breakwater.workflowScope` identifies the calling workflow scope.
- `breakwater.isolationScope` segments idempotency and rate-limit state.

Optional evaluators provide:

- Cross-workflow isolation: deny a connector call targeting another workflow's
  state.
- Tenant isolation: deny any connector call that lacks an isolation scope.

The host must mint these values on every start and resume leg. In this
repository, flowsafe owns that responsibility. A multi-tenant deployment must
also register `tenantIsolation()`; merely passing a scope when convenient is
not a fail-closed tenant boundary.

### 8. Decision Audit and Metrics

`AuditLogger` records structured gate decisions containing:

- Timestamp.
- Actor, when available.
- Action.
- Resource.
- `allowed`, `denied`, or `error` decision.
- Reason and optional detail.

It provides an in-memory ring buffer and an injectable sink. Sink failures are
contained so an audit-export outage does not take down the agent path.
`combineAuditSinks` supports fan-out, and `metricsAuditSink` converts decisions
into counters and optional duration histograms.

This differs from ordinary tracing: a trace answers what executed and how long
it took; a breakwater audit event records the verdict at a specific security
gate. Mastra now provides extensive observability and offers Enterprise audit
logs, so the unique value here is the open, self-hosted, gate-specific event
contract—not the general existence of logs or traces.

The default in-memory buffer is not durable. Flowsafe supplies the queue/SIEM
export path used by a production host.

### 9. Agent CLI Connectors

Breakwater includes Claude Code and Codex CLI adapters implemented as
connectors. They are write-class and approval-gated by default, support dry-run
command previews, impose time and output bounds, avoid shell execution, and
place caller-controlled prompts after an end-of-options marker.

These adapters are Node-only at execution time. They inherit the same caveats
and trusted-context requirements as every other connector.

## Mastra Versus Breakwater: Exact Comparison

| Concern | Mastra | breakwater |
|---|---|---|
| Execute an agent | Yes | No; wraps Mastra's agent boundary |
| Execute workflows durably | Yes, with its supported engines/storage | No |
| Input/output extension seam | Processor API | Concrete policy and RBAC processors using that seam |
| Built-in content guardrails | Yes | Additional detector/evaluator semantics and common audit integration |
| Tool approval UX | Native `requireApproval` and resume APIs | Uses native approval, then re-checks a trusted server-minted grant at execution |
| Tool-call hooks | Agent/tool hooks, including blocking and audit use cases | Connector wrapper enforces policy independent of caller path |
| Tool permission manifest | Schemas and descriptive annotations | Enforced side-effect, egress, approval, idempotency, dry-run, and rate-limit manifest |
| Tool egress restriction | No general process-level connector allowlist | Guarded runtime fetch plus declaration policy, subject to transport caveat |
| Tool idempotency | No equivalent enforced connector result store in this integration | In-memory and D1 replay/reservation stores |
| Authentication | Pluggable core auth | No identity provider; consumes a host-resolved actor |
| RBAC | Current Enterprise RBAC/ACL; app code can implement its own | Open-source but deliberately simple per-agent role membership check |
| Tracing/logging/evals | Extensive built-in and hosted support | Security-decision audit events and audit-derived metrics |
| Tenant isolation | Application/host responsibility | Opaque scope consumption and optional fail-closed connector evaluator |
| Retention | Storage/provider dependent | Not implemented in breakwater; flowsafe owns purge helpers and cron wiring |

## Which Layer Answers Which Authorization Question?

| Question | Enforcement owner |
|---|---|
| Is this HTTP caller authenticated and tenant-valid? | Flowsafe host/verifier |
| May this role start or resume any workflow? | Flowsafe coarse run-router roles |
| May this role start or advance this specific workflow? | Flowsafe `WorkflowMeta.allowedRoles` |
| May this role begin this particular agent model turn? | Breakwater `RBACMiddleware`, only when explicitly installed |
| May this actor approve or reject an approval record? | Flowsafe approval-service decision roles |
| May the requester approve their own action? | Flowsafe separation-of-duties policy |
| Does this connector require approval? | Breakwater manifest plus organization write policy |
| Does this execution leg possess proof of approval? | Breakwater grant check; grant minted by flowsafe |
| Which hosts may this connector call? | Breakwater network-egress policy and `runtime.fetch` |
| Can a retry safely avoid duplicating a side effect? | Breakwater idempotency store, with context supplied by the runtime |
| Is connector state segmented by tenant? | Flowsafe-minted scope plus breakwater keying/evaluator |
| When are old workflow snapshots and tenant data removed? | Flowsafe retention and purge jobs |

## Important Non-Goals and Caveats

Breakwater does not currently provide:

- A standalone policy server or policy administration UI.
- Complete IAM, user provisioning, SSO, or JWT verification.
- Granular permission-based RBAC or ABAC.
- Automatic protection of every agent or tool.
- A process/container-level network sandbox.
- A durable approval queue or human-review UI.
- Workflow start/resume authorization.
- Durable workflow execution.
- Workflow-output TTL deletion.
- Tenant identity or tenant lifecycle management.
- Guaranteed detection of every encoded or obfuscated secret.

Several features are opt-in and fail closed only when correctly composed:

- An agent receives RBAC only if the middleware is installed and cannot be
  removed through a per-call processor override.
- Actual egress is constrained only when connector traffic uses
  `runtime.fetch`.
- Multi-tenant connector calls fail on missing scope only when
  `tenantIsolation()` is registered.
- Durable idempotency and rate limits require durable stores.
- Approval grants are trustworthy only when reserved context keys are populated
  exclusively by trusted runtime code.
- Durable audit export requires a sink; the default is only an in-memory ring
  buffer.

## Documentation Clarifications

Some broad package descriptions refer to breakwater as wrapping "tool and
workflow surfaces." The currently shipped boundary is more precise:

- Breakwater wraps connectors/tools and runs processors around agent model
  calls.
- It does not ship a general workflow RBAC wrapper.
- Flowsafe's HTTP run router enforces workflow start/resume roles.
- Retention is not a breakwater policy evaluator; flowsafe performs
  storage-layer purge.

Likewise, the five-role table in the security threat model describes intended
platform responsibilities, not permission semantics implemented by
`RBACMiddleware`. The middleware does not know that an `operator` should start
runs or a `reviewer` should approve requests. Flowsafe implements those
host-level meanings through separate role lists and route/service gates.

## Recommended Positioning

Use this sentence when explaining the package:

> Breakwater is open, self-hostable safety middleware for Mastra that enforces
> agent-turn authorization, content policy, and connector permissions—including
> trusted write grants, network egress, idempotency, dry-run, rate limits, and
> isolation—at the point of execution.

Avoid claims such as:

- "Breakwater is our complete RBAC system."
- "Breakwater authenticates users."
- "Breakwater provides workflow RBAC."
- "Breakwater provides durable approvals or retention."
- "Mastra has no guardrails, approval, auth, or audit capabilities."
- "The egress policy is a complete network sandbox."

The defensible value proposition is:

> Without breakwater, the same controls could be rebuilt using Mastra
> processors, hooks, approval APIs, and application code, but every connector
> and invocation path would have to implement them consistently. Breakwater
> makes those rules reusable, testable, auditable, and caller-independent at
> the side-effect boundary.

## Primary References

Repository references:

- [`breakwater-architecture.md`](breakwater-architecture.md)
- [`policy-engine-design.md`](policy-engine-design.md)
- [`connector-interface.md`](connector-interface.md)
- [`security-threat-model.md`](security-threat-model.md)
- [`flowsafe-architecture.md`](flowsafe-architecture.md)
- [`packages/breakwater/src/rbac/index.ts`](../packages/breakwater/src/rbac/index.ts)
- [`packages/breakwater/src/policy-engine/index.ts`](../packages/breakwater/src/policy-engine/index.ts)
- [`packages/breakwater/src/policy-engine/tool-policy.ts`](../packages/breakwater/src/policy-engine/tool-policy.ts)
- [`packages/breakwater/src/connector-sdk/index.ts`](../packages/breakwater/src/connector-sdk/index.ts)
- [`packages/breakwater/src/audit/index.ts`](../packages/breakwater/src/audit/index.ts)

Current Mastra references checked on 2026-07-20:

- [End-to-end auth and Enterprise RBAC/ACL](https://mastra.ai/blog/changelog-2026-03-04)
- [Native tool approval](https://mastra.ai/blog/tool-approval)
- [Tool-level versus agent-level approval](https://mastra.ai/blog/human-in-the-loop-when-to-use-agent-approval)
- [Input processors and built-in guardrails](https://mastra.ai/blog/building-fast-reliable-input-processors)
- [Output processors](https://mastra.ai/blog/introducing-output-processors)
- [Tool hooks](https://mastra.ai/blog/introducing-tool-hooks)
- [Observability](https://mastra.ai/ai-agent-observability)
- [Enterprise audit logs and RBAC](https://mastra.ai/platform-observability)
