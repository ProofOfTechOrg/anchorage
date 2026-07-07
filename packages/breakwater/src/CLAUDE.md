# src/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | Package barrel — re-exports the policy-engine, rbac, audit, and connector-sdk public API | Finding the public export surface, adding a public symbol |
| `chain.test.ts` | Integration test: RBAC + policy processors composed in a Mastra Agent input/output chain | Verifying processor composition, debugging chain-order or tripwire behavior |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `policy-engine/` | `PolicyEngine` processor + policy evaluators (deny patterns, max length, network egress, approval-required) | Adding a policy, gating model/tool I/O, debugging a tripwire denial |
| `rbac/` | `RBACMiddleware` processor, `ROLES`, actor resolution (re-exports the audit surface for compat) | Changing roles/permissions or actor sourcing |
| `audit/` | `AuditLogger` shared sink + audit event types — every gate writes here | Changing audit event shape, buffering, or sink behavior |
| `connector-sdk/` | `createConnector()` wrapping Mastra `createTool()` with an enforced permission manifest | Wrapping a tool with egress/write-approval/idempotency enforcement |
| `agent-cli/` | Claude Code / Codex CLIs as approval-gated connectors (Phase 4): write-class manifest, dry-run command preview, injectable `exec` seam, Node-only default runner via `getBuiltinModule` | Adding an agent CLI adapter, changing spawn/timeout/parse behavior |
