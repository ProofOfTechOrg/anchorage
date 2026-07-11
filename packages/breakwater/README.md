# breakwater

Safety middleware for Mastra agents, shipping as Mastra processors and tool/workflow wrappers.

## Purpose

breakwater implements Mastra's Processor interface at the agent boundary and
wraps tool and workflow surfaces to enforce policy, RBAC, and connector-level
controls at runtime. Each subpackage targets a distinct enforcement point in
the agent execution lifecycle.

## Subpackages

| Subpackage | Enforcement point | Role |
|---|---|---|
| `policy-engine` | Agent-boundary processors + connector execute wrapper | Egress, output-channel, retention, isolation gates around model and tool calls |
| `rbac` | Workflow and tool invocation | Resolve caller identity -> role -> permissions |
| `audit` | Shared sink used by every gate | `AuditLogger` — buffered, sink-isolated structured audit events (re-exported from `rbac` for compat) |
| `connector-sdk` | `createConnector()` wrapping `createTool()` | Permission manifests: egress allowlist, write-approval gate, idempotent replay, dry-run simulation, fixed-window rate limits |
| `agent-cli` | `createClaudeCodeConnector` / `createCodexConnector` | Claude Code / Codex CLIs as approval-gated connectors (Node-only execution) |

## Installation

```
npm install @proofoftech/breakwater
```

## Status

Implemented and tested. `PolicyEngine` and `RBACMiddleware` are real Mastra
`Processor` implementations with `AuditLogger` as the shared sink both gates
write to; the policy engine gates output per channel (answer / reasoning /
object) with opt-in zero-leak hold-back buffering, and ships network-egress,
retention, cross-workflow-isolation, and cross-tenant-isolation tool-boundary
policies.

`createConnector()` wraps Mastra `createTool()` with an enforced permission
manifest — network-egress allowlisting, write-approval gating (a
requestContext grant gates every path; Mastra's native `requireApproval` is
compiled so agent runs pause for the decision, but it never substitutes for
the grant), keyed idempotent replay (in-memory + D1 atomic stores), per-call
dry-run simulation, and fixed-window rate limiting. Custom tool-boundary
evaluators register via `policies.evaluators`. Enforcement contract:
`docs/connector-interface.md`; authoring guide: `CONNECTORS.md`.

Agent CLI adapters (`@proofoftech/breakwater/agent-cli`) ship Claude Code and
Codex as approval-gated connectors — Node-only at execution time.

**Egress and rate-limit caveats.** `networkEgress` gates what a connector's
manifest *declares* it calls, not the actual socket — there is no fetch-level
interception (yet). It catches misconfiguration and org-policy drift, not a
connector (or a compromised dependency) that lies about its egress; treat it
as a declaration/allowlist control, not a network sandbox. Fixed-window
`rateLimit` can admit up to ~2x the declared budget across two adjacent
windows (amplified by cross-isolate clock skew) — an accepted characteristic
of fixed windows, not a bug. See `CONNECTORS.md`'s "Known limits" for detail.

## Usage

```typescript
import { Agent } from '@mastra/core/agent';
import { AuditLogger, denyPatterns, PolicyEngine, RBACMiddleware } from '@proofoftech/breakwater';

const audit = new AuditLogger();
const policy = new PolicyEngine({ policies: [denyPatterns(['rm -rf'])], audit });

const guarded = new Agent({
  id: 'guarded-agent',
  name: 'guarded-agent',
  instructions: 'Domain agent guarded by breakwater.',
  model: 'openai/gpt-4o',
  // Array order is evaluation order; a denial aborts the chain (tripwire)
  // and is recorded by whichever gate fired.
  inputProcessors: [new RBACMiddleware({ allowedRoles: ['operator', 'admin'], audit }), policy],
  outputProcessors: [policy],
});
```

The actor is read from `requestContext` under `ACTOR_CONTEXT_KEY`
(`breakwater.actor`), or supply `getActor` for custom sourcing (JWT, API
keys). Audit events buffer in memory with an injectable `sink`
(`@proofoftech/breakwater/audit`); flowsafe's approval service emits
structurally-compatible events, so one `AuditLogger` can carry both. For
durable export, flowsafe ships a Cloudflare Queues → SIEM sink
(`@proofoftech/flowsafe/audit-export`).

Write-approval grants are minted by flowsafe's approval queue: an approved
request's connector ids are derived into
`requestContext['breakwater.approvedConnectors']` at resume time
(store-derived, never body-carried) — see
`@proofoftech/flowsafe/approval-api`.

Breakwater is **tenant-agnostic**: its `Actor` has no tenant, and no gate needs
one. A multi-tenant host passes an opaque scope string through
`requestContext['breakwater.isolationScope']`, which breakwater never parses.
The scope segments the connector SDK's idempotency and rate-limit keys — so one
tenant cannot replay another's cached result or exhaust its budget — and the
optional `tenantIsolation()` evaluator denies a call that arrives without one,
including on dry-run. Absent scope reproduces the single-tenant keys exactly;
there is no flag to forget.

See `docs/breakwater-architecture.md` for design details.
