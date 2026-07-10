# Policy Engine Design

The policy engine covers four gap domains that Mastra's processor pipeline does not cover. It ships as part of `@proofoftech/breakwater/policy-engine`.

## Four Gap Domains

| Domain | What Mastra Covers | What Anchorage Adds |
|---|---|---|
| Network egress | Nothing | Restrict which domains tools and connectors can call |
| Write permission gates | Per-tool `requireApproval` flag (boolean or predicate) -- no classification, no org policy | Classify write vs read vs destructive; gate writes behind org-level approval policy |
| Data retention | Nothing | Enforce workflow output TTLs; auto-expire after retention period |
| Cross-workflow isolation | Nothing | Prevent workflow A from reading workflow B's state; on a multi-tenant host, also require a tenant scope on every connector call |

## Architecture

The policy engine runs at two seams, matching `breakwater-architecture.md`:

```
Agent boundary (Mastra processor chain):
  inputProcessors:  RBAC → PolicyEngine (pre) → model call
  outputProcessors: model response → PolicyEngine (post)
  AuditLogger = shared sink each gate emits to (a denial aborts the
  chain, so audit cannot be a downstream peer processor)

Tool boundary (breakwater connector wrapper):
  egress + write checks (pre) → tool execute → retention + isolation (post)
```

Mastra's processor seam is the agent loop -- `processOutputStep` can inspect pending tool calls and abort before they execute, and per-tool `requireApproval` gates individual tools -- but nothing chains around workflow steps or tools invoked outside an agent loop (`createStep(tool)`, direct calls). Caller-independent tool-boundary gates therefore live in the connector SDK's `execute` wrapper.

Each policy is an evaluator function registered by name:

```typescript
interface PolicyEvaluator {
  name: string;
  evaluate(context: PolicyContext): PolicyResult;
}

interface PolicyResult {
  allowed: boolean;
  reason?: string;
}
```

Policy evaluation order is configurable. By default: network egress, write permissions, data retention, cross-workflow isolation (plus cross-tenant isolation on a multi-tenant host).

## Policy Configuration

Policies are defined per-workflow or globally:

```yaml
policies:
  networkEgress:
    allowedDomains: ['api.openai.com', 'api.anthropic.com']
  writePermissions:
    requireApproval: ['salesforce.*', 'github.*']
  dataRetention:
    workflowOutputTTL: '90d'
  crossWorkflowIsolation:
    enabled: true
    namespace: 'workflow-${workflowId}'
```

## Evaluation Flow

1. Pre-gate: evaluate network egress + write permissions before tool execution. Deny blocks execution.
2. Execution: Mastra tool runs normally.
3. Post-gate: evaluate data retention + cross-workflow isolation on output storage. Policy violation logs a warning and redacts the output.

## Implementation

The policy engine is a set of evaluator functions, not a standalone service. It runs in-process -- as Mastra processors at the agent boundary and inside the connector SDK's `execute` wrapper at the tool boundary -- with no external dependencies beyond the breakwater package.

All four domains are implemented. Two tool-boundary domains -- `networkEgress` (evaluator in
`policy-engine/tool-policy.ts`, declaration-based against the manifest's
`egress` hostnames) and write-permission gating (`approvalRequired()`,
enforced by the connector wrapper's requestContext grant check on every
caller; also compiled to Mastra's native `requireApproval` so agent runs
pause for the decision -- the native outcome never substitutes for the
grant; see `connector-interface.md`). Custom tool-boundary evaluators
register via the connector's `policies.evaluators`.

The remaining two domains are each enforced where enforcement naturally lives:

- **Cross-workflow isolation** is a tool-boundary evaluator,
  `crossWorkflowIsolation({ targetScopeOf })` in `tool-policy.ts`, registered
  through `policies.evaluators`. The caller's scope comes from requestContext
  `breakwater.workflowScope` (`WORKFLOW_SCOPE_CONTEXT_KEY`), which flowsafe's
  `RunnerRuntime` mints on EVERY start/resume leg (trusted-runtime-only —
  trust boundary 6; provider values can deliberately override). The
  connector-specific `targetScopeOf` extractor names the workflow a call
  addresses; calls without a target pass, calls targeting another workflow's
  state — or targeting workflow state without a minted caller scope — fail
  closed.
- **Cross-tenant isolation** is its sibling, `tenantIsolation()`, reading the
  opaque `breakwater.isolationScope` key. It deliberately does not re-qualify
  the workflow-scope key's value (a consumer may parse that as a bare
  `workflowId`). A multi-tenant platform adds it to its policy set, and any
  connector call arriving without a scope is denied — including a dry-run,
  because the evaluator runs in the pre-execute gates loop while the dry-run
  branch returns before the idempotency and rate-limit paths. The scope also
  segments those two keys per tenant. See `connector-interface.md`.
- **Data retention** is deliberately NOT a policy evaluator: TTL enforcement
  is a storage-layer property (the data outlives any single call, so a
  call-time gate cannot expire it). It ships as flowsafe's
  `purgeExpiredWorkflowRuns(db, { ttlMs, tablePrefix })` — a callable helper
  that deletes TERMINAL runs (success/failed/tripwire/canceled/bailed/
  skipped) older than the `workflowOutputTTL` from `mastra_workflow_snapshot`
  and never touches live runs (expiring a suspended run would kill a pending
  approval). Its counterpart `purgeTenant(db, { tenantId, artifactStore })`
  reaps a departing tenant's snapshots of ANY status, its approval records,
  and its R2 artifacts — the only path that reclaims a run abandoned at an
  approval gate. Scheduling stays with the caller (a Worker cron).
