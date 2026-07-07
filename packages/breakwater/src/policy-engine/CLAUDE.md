# policy-engine/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | `PolicyEngine` Mastra processor + agent-boundary evaluators (`denyPatterns`, `maxTextLength`, `extractMessageText`). Output is gated per channel (`OutputChannel`: answer/reasoning/object) with per-stream state accumulation, incremental `denyPatterns` scans (`PolicyContext.streamState`), and opt-in zero-leak hold-back buffering (`PolicyEngineOptions.holdBack`, per-policy `holdBackChars` hints, end-chunk/finish flush via the runner's reprocess key). Re-exports the tool-boundary evaluators from `tool-policy.ts`; policy/decision/phase/channel types | Adding an agent-boundary policy, gating model input/output, debugging streaming, channel, hold-back, or result gating |
| `tool-policy.ts` | Tool-boundary evaluators (`networkEgress`, `approvalRequired`, `crossWorkflowIsolation` + `WORKFLOW_SCOPE_CONTEXT_KEY`) with `ToolCallContext` / `ToolPolicyEvaluator` / `SideEffect` / `WritePermissionsPolicy` types, applied at connector execute | Enforcing egress/write/isolation policy at the tool boundary, extending the tool-policy contract |
| `policy-engine.test.ts` | `PolicyEngine` processor tests (input/output gating, tripwire, streaming, channels, hold-back buffering) | Adding processor tests, debugging gate behavior |
| `tool-policy.test.ts` | Tool-boundary evaluator tests (egress, approval, isolation decision table) | Adding tool-policy tests |
