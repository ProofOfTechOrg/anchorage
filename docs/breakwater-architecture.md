# Breakwater Architecture

Breakwater is the safety middleware package (`@proofoftech/breakwater`). It plugs into Mastra as input/output processors (agent boundary) plus a deployment-time workflow-scope wrapper, providing open-source RBAC and audit logging as Mastra middleware. The tool-boundary connector wrapper is specified in [`connector-interface.md`](connector-interface.md).

## Package Structure

Breakwater contains three subpackages. RBACMiddleware and PolicyEngine compose as a Mastra processor chain around an agent's model call, with AuditLogger as a shared sink both gates write to; RBAC also acts as a workflow-scope wrapper applied at deployment:

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

Breakwater's agent-boundary controls integrate as a Mastra processor chain (`@mastra/core/processors`, part of `@mastra/core@^1.49.0`):

```typescript
import { Agent } from '@mastra/core/agent';
import { RBACMiddleware, AuditLogger } from '@proofoftech/breakwater/rbac';
import { PolicyEngine } from '@proofoftech/breakwater/policy-engine';

// RBAC and PolicyEngine implement Mastra's Processor interface and run as input
// (pre-gate) and/or output (post-gate) processors, sequentially in array order.
// AuditLogger is a shared sink injected into each gate -- NOT a peer processor:
// a denial aborts the chain, so a processor placed after the gate could never
// observe it. Each gate emits an audit event as it evaluates, so a denial is
// recorded at whichever gate fires. A shared PolicyEngine spans both gates so
// pre/post state is shared.
const audit = new AuditLogger();
const policy = new PolicyEngine({ audit });

const guardedAgent = new Agent({
  id: 'guarded-agent',
  name: 'guarded-agent',
  instructions: 'Domain agent guarded by breakwater.',
  model: 'openai/gpt-4o',
  inputProcessors: [new RBACMiddleware({ audit }), policy],
  outputProcessors: [policy],
});
```

Input processors run before the model call (pre-gate); output processors run after the response is produced (post-gate), each in array order. A processor runs only in the phase(s) it is registered for; the shared PolicyEngine sits in both arrays, so it gates pre and post. Because a denial aborts the chain, AuditLogger is a shared sink each gate writes to rather than a peer processor — a denial is recorded at whichever gate fires, which a processor placed after that gate could not observe.

This snippet shows breakwater's **agent-boundary** integration: processors gate an Agent's model call. Breakwater's **workflow-level RBAC scoping** (see the [`custom-workflow-scoping` example](examples/custom-workflow-scoping.ts)) is a separate mechanism — a wrapper applied at workflow deployment that authorizes runs and mutations before they reach Mastra, since Mastra processors run only around agent/model calls, not arbitrary workflow steps.

## Subpackages

### RBACMiddleware (@proofoftech/breakwater/rbac)

Five roles: admin, builder, operator, reviewer, viewer. Supports per-workflow scoping and pluggable auth providers (API keys, JWT, OIDC). RBAC runs in two places: as a Mastra input processor it authorizes an agent's model call, and as the workflow-scope wrapper (applied at deployment) it evaluates the role check before every workflow run and mutation.

### AuditLogger (@proofoftech/breakwater/rbac)

Structured audit log for every action: who, what, when, result, reason. Writes to D1 with a Cloudflare Queues export path for SIEM ingestion.

### PolicyEngine (@proofoftech/breakwater/policy-engine)

Pre-gate and post-gate policy evaluation (network egress, budget, data retention). Policies are expressed as a set of evaluator functions. Each function returns `{ allowed: boolean, reason?: string }`.

## Dependencies

- Requires `@mastra/core@^1.49.0` (Processor API)
- No direct dependency on `flowsafe` or Cloudflare DO
- Works in any Mastra deployment target (Node.js, Workers, Vercel, etc.)
