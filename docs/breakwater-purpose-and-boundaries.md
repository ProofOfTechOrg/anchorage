# Breakwater purpose and boundaries

Breakwater is an in-process safety layer for Mastra agents and tools. It
implements concrete Mastra processors and tools, so it does not replace the
agent runtime. It makes authorization, content policy, and connector
permissions reusable and enforceable at the boundary where a model turn starts
or a side effect occurs.

Use breakwater when the same control must apply whether a connector is called
by an agent, workflow, nested tool, or application code. A processor or agent
hook sees only the path on which it is installed. `createConnector()` wraps the
tool's own execution path.

## Product split

| Layer | Responsibility in this repository |
| --- | --- |
| Mastra | Agents, processors, tools, workflows, request context, suspension, and storage interfaces |
| breakwater | Agent-turn RBAC, content policy, connector manifests and gates, guarded fetch, replay/rate stores, and decision audit |
| flowsafe | Authenticated tenant context, Durable Object execution, D1 persistence, approval records, grant derivation, long-running agents, retention, and offboarding |

Several controls are composed across these layers. For example, Mastra pauses a
tool call, flowsafe records and authorizes the human decision, and breakwater
checks the resulting server-derived connector grant immediately before the
write.

## What breakwater provides

### Agent-turn authorization

`RBACMiddleware` is a Mastra input processor. It accepts five role labels:
`admin`, `builder`, `operator`, `reviewer`, and `viewer`. Its default lookup
reads an `{ id, role }` actor from the `breakwater.actor` request-context key.
A custom `getActor` can adapt another trusted host source.

The middleware answers one question:

> May this actor's exact role begin this agent model turn?

It uses an exact per-agent allowlist. There is no role inheritance. A missing,
malformed, disallowed, or throwing actor lookup fails closed and can be
audited.

The middleware does not authenticate a request, choose a tenant, authorize an
HTTP workflow route, or decide an approval record. Install it on every agent
that needs this boundary.

### Input and output policy

`PolicyEngine` is a Mastra input and output processor. Shipped policies cover:

- denied text patterns;
- maximum input or output length;
- PII and secret detection using patterns, entropy checks, Luhn validation,
  allowlists, and streaming windows;
- a synchronous or asynchronous classifier seam with fail-closed timeout
  behavior;
- answer, reasoning, and structured-object output channels;
- optional per-segment hold-back so a streaming denial does not release the
  unsafe suffix first.

Content detection is best-effort. Encoded or adversarially transformed data
can evade pattern and entropy detectors. Use the classifier seam and
infrastructure controls when the data policy requires more.

### Connector permission manifests

`createConnector()` returns a real Mastra tool and requires a permission
manifest:

```typescript
permissions: {
  sideEffect: 'write',
  egress: ['api.example.com'],
  requiresApproval: true,
  idempotencyKey: true,
  dryRun: true,
  rateLimit: '100/min',
}
```

The wrapper enforces:

- side-effect classification;
- organization policy over declared egress when
  `policies.networkEgress` is configured;
- a trusted approval grant when the manifest or deployment write policy
  requires approval;
- a required side-effect-free simulation when dry-run is declared;
- keyed replay when idempotency is declared;
- a fixed-window execution budget when a rate limit is declared;
- optional workflow and tenant isolation evaluators;
- background-execution eligibility.

Tool annotations mirror the manifest for discovery, but annotations are not the
enforcement mechanism.

### Network egress

Egress has two layers:

1. When configured, the optional declaration gate checks the connector's host
   list against organization policy.
2. `ConnectorRuntime.fetch` checks the actual URL and every followed redirect
   against the connector's declaration.

The guarded fetch rejects non-HTTP(S) URLs, undeclared hosts, excessive
redirects, and non-replayable 307/308 stream bodies. It removes credential
headers on cross-origin redirects.

This is not a process firewall. Global `fetch`, raw sockets, vendor SDKs using
their own transport, and child processes do not pass through
`ConnectorRuntime.fetch`. Route supported SDKs through the injected fetch and
enforce the remaining network boundary in the host.

### Approval grants

Mastra-native `requireApproval` provides the pause experience. When the
manifest or deployment write policy requires approval, Breakwater also checks
the connector id in the server-authored `breakwater.approvedConnectors`
request-context value before executing. The native resume event alone is not
authority.

This second gate protects direct and nested calls and makes a forged resume
fail closed. Breakwater consumes the grant; it does not mint it. Flowsafe
derives grants from approved D1 records for the exact suspended leg. Never
accept the grant key from a client, model, tool argument, schedule row, or
other untrusted stored context.

### Idempotency and rate limits

Breakwater includes in-memory and D1-backed stores.

Idempotency supports completed-result replay, concurrent same-key joining,
atomic reservation, stale-pending takeover, and lease-bound completion or
release. The isolation scope participates in durable keys.

Rate limiting uses fixed epoch-aligned windows. Only an execution that reaches
the inner connector consumes budget. Denials, dry runs, completed replays, and
same-attempt joins do not. A fixed window can admit bursts across a window
boundary; use another algorithm outside breakwater if you require a smooth
rate.

### Workflow and tenant scope

Breakwater remains tenant-agnostic. It consumes opaque host-minted context:

- `breakwater.workflowScope` identifies the current workflow;
- `breakwater.isolationScope` separates idempotency and rate keys.

`crossWorkflowIsolation()` compares the current workflow to a requested target.
`tenantIsolation()` refuses calls with no isolation scope. Flowsafe mints both
values on every start and resume leg.

### Audit and metrics

`AuditLogger` records timestamp, actor, action, resource, decision, reason, and
optional structured detail. It has an in-memory ring and an optional sink.
`combineAuditSinks()` fans out events, and `metricsAuditSink()` adapts decisions
to counters and duration histograms.

The in-memory ring is not durable. Use the flowsafe queue exporter or another
sink for production evidence. Do not place secrets in connector ids, policy
names, idempotency keys, or custom audit detail.

### Coding-agent connectors

The Agent CLI subpath exposes Claude Code and Codex as write-class connectors.
They require approval by default, provide a no-spawn dry-run preview, pass
prompts as the final positional value after `--`, select workspace-edit
permissions, bound execution time and retained output, and expose redacted
diagnostics.

The child process is still capable of reading credentials, editing its
workspace, and running commands available to it. Run it in a dedicated
checkout, container, or remote executor. See
[Agent CLI connectors](agent-cli-connectors.md).

## Boundary matrix

| Question | Owner |
| --- | --- |
| Is an HTTP caller authenticated and tenant-valid? | Application or flowsafe host |
| May a role start this workflow? | Flowsafe run router and `WorkflowMeta.allowedRoles` |
| May a role begin this agent turn? | Installed breakwater `RBACMiddleware` |
| May an actor decide this approval? | Flowsafe `ApprovalService` |
| Does a connector require approval? | Breakwater manifest and organization policy |
| Does this exact leg have a valid grant? | Breakwater check over a flowsafe-derived context |
| Which hosts may this connector fetch? | Breakwater declaration policy and guarded runtime fetch |
| Can a retry replay a completed result? | Breakwater idempotency store |
| Is durable state separated and later removed by tenant? | Flowsafe runtime, stores, and purge duties |

## Non-goals

Breakwater is not:

- an identity provider, session system, SSO integration, or complete IAM;
- granular permission RBAC, ABAC, or a role hierarchy;
- a workflow runtime or durable approval queue;
- a general workflow-route authorization wrapper;
- a process, filesystem, or network sandbox;
- a hosted policy administration service;
- guaranteed detection of every secret or policy-violating output;
- durable audit storage by itself.

## Required composition

A production deployment must satisfy these conditions:

1. Authenticate callers before minting actor, tenant, workflow, or isolation
   context.
2. Install processors on every agent path that needs them.
3. Wrap side-effecting tools with `createConnector()`.
4. Keep reserved request-context keys server-only.
5. Use shared D1 stores when a rate or replay guarantee must span isolates.
6. Route network calls through `ConnectorRuntime.fetch` where possible.
7. Apply host-level process and network controls to traffic outside that seam.
8. Persist audit events outside the in-memory ring.

For package selection and installation, continue with
[Getting started](getting-started.md). For every manifest field and store
contract, read the [Connector interface](connector-interface.md) and
[Connector authoring guide](../packages/breakwater/CONNECTORS.md).
