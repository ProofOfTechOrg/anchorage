# Proposal: model gateway policy

> Status: partially implemented. `createGuardedAgent()` ships the non-overridable guarded-agent boundary, and `PolicyEngine` ships content inspection. Provider allowlisting, D1 spend budgets, model-endpoint egress policy, guarded structured-output validation, and an Anchorage scoring gate remain design only. Supported behavior is documented in [Breakwater architecture](../breakwater-architecture.md) and [Policy engine](../policy-engine-design.md).

Mastra's `Agent` class handles model routing, provider normalization, and tool calling natively. This document covers the policy layer Anchorage adds on top of `Agent.generate()`.

## Planned pre-gate policies

| Policy | Description |
|---|---|
| Provider allowlist | Restrict which providers workflows can use |
| Budget check | Per-workflow or per-team spend limits in D1 (extends Mastra's `CostGuardProcessor`, which caps cost per run/resource/thread) |
| Network egress check | Verify target model endpoint is in allowlist |

## Planned and partial post-gate policies

| Policy | Status | Description |
|---|---|---|
| Output schema validation | Planned | The guarded handle currently rejects structured-output call options |
| Content moderation | Partial | `PolicyEngine`, `piiSecrets()`, and `classifierPolicy()` ship; default prompt-injection and toxicity policies do not |
| Quality scoring | Planned | Mastra construction-time scorers can pass through, but Anchorage adds no scoring policy or post-gate contract |

## Relationship to Mastra processors

The shipped guarded path uses Breakwater's `RBACMiddleware` and `PolicyEngine` as mandatory Mastra processors. `createGuardedAgent()` fixes the model, processors, execution limits, and tool choice at construction, then rejects per-call overrides that could remove those controls.

No separate model-gateway export exists. The guarded handle does not implement the planned provider allowlist, budget lookup, model-endpoint check, structured-output validation, or scoring gate.

## Planned budget enforcement

The proposed budget state lives in D1 with eventual consistency. Hard limits would run before invocation, while soft limits would emit warnings. The proposed records include provider, model, prompt hash, token count, and estimated cost. No such store or gate currently ships.
