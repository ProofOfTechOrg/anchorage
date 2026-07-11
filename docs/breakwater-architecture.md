# Breakwater Architecture

Breakwater is the safety middleware package (`@proofoftech/breakwater`). It plugs into Mastra as input/output processors (agent boundary), providing open-source RBAC and audit logging as Mastra middleware. The tool-boundary connector wrapper is specified in [`connector-interface.md`](connector-interface.md). Workflow-level role gating is not a breakwater wrapper: it is realized at the HTTP boundary by flowsafe's host-kit run router (`WorkflowMeta.allowedRoles`), because Mastra processors run only around agent/model calls, not arbitrary workflow steps.

## Package Structure

Breakwater contains three subpackages. RBACMiddleware and PolicyEngine compose as a Mastra processor chain around an agent's model call, with AuditLogger as a shared sink both gates write to:

```
┌───────────────────────────────────────────────────────┐
│                     Breakwater                          │
│  ┌─────────────────────────────────────────────────┐  │
│  │  RBACMiddleware                                  │  │
│  │  Roles, scopes, authorization                    │  │
│  │  (@proofoftech/breakwater/rbac)                  │  │
│  └──────────────────────┬────────────────────────────┘  │
│                         │                              │
│  ┌──────────────────────┴────────────────────────────┐  │
│  │  AuditLogger                                     │  │
│  │  Structured audit logging                        │  │
│  │  (@proofoftech/breakwater/rbac)                  │  │
│  └──────────────────────┬────────────────────────────┘  │
│                         │                              │
│  ┌──────────────────────┴────────────────────────────┐  │
│  │  PolicyEngine                                    │  │
│  │  Pre/post gate evaluation                        │  │
│  │  (@proofoftech/breakwater/policy-engine)         │  │
│  └─────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────┘
```

## Integration with Mastra

Breakwater's agent-boundary controls integrate as a Mastra processor chain (`@mastra/core/processors`, part of `@mastra/core@^1.50.0`):

```typescript
import { Agent } from '@mastra/core/agent';
import { AuditLogger, RBACMiddleware } from '@proofoftech/breakwater/rbac';
import { denyPatterns, PolicyEngine } from '@proofoftech/breakwater/policy-engine';

// RBAC and PolicyEngine implement Mastra's Processor interface and run as input
// (pre-gate) and/or output (post-gate) processors, sequentially in array order.
// AuditLogger is a shared sink injected into each gate -- NOT a peer processor:
// a denial aborts the chain, so a processor placed after the gate could never
// observe it. Each gate emits an audit event as it evaluates, so a denial is
// recorded at whichever gate fires. A shared PolicyEngine spans both gates so
// pre/post state is shared.
const audit = new AuditLogger();
const policy = new PolicyEngine({ policies: [denyPatterns(['rm -rf'])], audit });

const guardedAgent = new Agent({
  id: 'guarded-agent',
  name: 'guarded-agent',
  instructions: 'Domain agent guarded by breakwater.',
  model: 'openai/gpt-4o',
  inputProcessors: [new RBACMiddleware({ allowedRoles: ['operator', 'admin'], audit }), policy],
  outputProcessors: [policy],
});
```

Input processors run before the model call (pre-gate); output processors run after the response is produced (post-gate), each in array order. A processor runs only in the phase(s) it is registered for; the shared PolicyEngine sits in both arrays, so it gates pre and post. Because a denial aborts the chain, AuditLogger is a shared sink each gate writes to rather than a peer processor — a denial is recorded at whichever gate fires, which a processor placed after that gate could not observe.

This snippet shows breakwater's **agent-boundary** integration: processors gate an Agent's model call. **Workflow-level role gating** is a separate, realized mechanism that lives in flowsafe, not breakwater: the host-kit run router checks the authenticated actor's role against each workflow's `WorkflowMeta.allowedRoles` before a run starts or resumes, because Mastra processors run only around agent/model calls, not arbitrary workflow steps. The [`custom-workflow-scoping` sketch](examples/custom-workflow-scoping.ts) is a non-runnable design sketch of that idea, not a shipped breakwater API.

## Subpackages

### RBACMiddleware (@proofoftech/breakwater/rbac)

Five roles: admin, builder, operator, reviewer, viewer. The check is a deliberate flat `allowedRoles` membership test on the actor, read from `requestContext` (`ACTOR_CONTEXT_KEY`) or a custom `getActor` seam — API keys and JWTs plug in through that seam (flowsafe's host-kit ships the bearer-token and HS256 verifiers); no OIDC provider ships today. As a Mastra input processor it authorizes an agent's model call; per-workflow role gating happens at the HTTP boundary in flowsafe's host-kit run router (`WorkflowMeta.allowedRoles`), not in this package.

### AuditLogger (@proofoftech/breakwater/audit)

Structured audit log for every action: who, what, when, result, reason. Buffers in memory with an injectable `sink` seam (re-exported from `/rbac` for compatibility); the durable Cloudflare Queues → SIEM export path ships in `@proofoftech/flowsafe/audit-export` and plugs into that seam.

### PolicyEngine (@proofoftech/breakwater/policy-engine)

Pre-gate and post-gate content evaluation over the answer/reasoning/object output channels (deny patterns, length limits, PII/secret content inspection via `piiSecrets` — regex + entropy + Luhn detectors with allowlist exemptions — a pluggable async classifier via `classifierPolicy`, and opt-in hold-back buffering), with custom policies as evaluator functions returning `{ allowed: boolean, reason?: string }`. Tool-boundary policies (network-egress declaration gate, write approval, tenant and cross-workflow isolation) live in `tool-policy.ts` and are enforced by the connector SDK before a connector executes — pre-execute deny, not post-execute redaction.

## Tenancy

Breakwater is **tenant-agnostic by design**: it is a standalone Apache-2.0
library, and no gate needs tenant identity — `RBACMiddleware` decides on
`actor.role`, `PolicyEngine` on message content. Its `Actor` therefore has no
`tenantId`, and its audit events carry none.

A multi-tenant host passes one **opaque scope string** through requestContext
(`breakwater.isolationScope`), which breakwater never interprets — the same
arrangement `crossWorkflowIsolation` already uses for
`breakwater.workflowScope`. The scope segments the connector SDK's idempotency
and rate-limit keys, and the optional `tenantIsolation()` evaluator denies a
call that arrives without one. Absent scope reproduces the single-tenant
behaviour exactly. See [`connector-interface.md`](connector-interface.md).

## Dependencies

- Requires `@mastra/core@^1.50.0` (Processor API)
- No direct dependency on `flowsafe` or Cloudflare DO
- Works in any Mastra deployment target (Node.js, Workers, Vercel, etc.)
