# Examples

Design sketches illustrating Mastra `createWorkflow()` patterns for Anchorage. These are not runnable -- they illustrate the API shape and integration points with breakwater and flowsafe. Each sketch's **runnable twin** is listed below: the showcase package runs full versions of these workflows behind real enforcement (`packages/showcase/worker/workflows/`), and flowsafe ships one self-verifying end-to-end example (`packages/flowsafe/examples/gtm-outbound.e2e.test.ts`, run with `pnpm --filter @proofoftech/flowsafe example:gtm`).

## Index

| File | Pattern | Highlights | Runnable twin |
|---|---|---|---|
| `gtm-outbound.ts` | Serial pipeline | Approval gate via flowsafe | `packages/flowsafe/examples/gtm-outbound.e2e.test.ts` (real seams, one Node process) and `packages/showcase/worker/workflows/gtm-outbound.ts` |
| `content-pipeline.ts` | Parallel branches | Parallel step execution | `packages/showcase/worker/workflows/content-pipeline.ts` |
| `lead-generation.ts` | Conditional branching | `.branch()` with hot/cold routing | `packages/showcase/worker/workflows/lead-generation.ts` |
| `product-launch.ts` | Approval checkpoints | Multi-step serial approval | `packages/showcase/worker/workflows/product-launch.ts` |
| `custom-workflow-scoping.ts` | Role-gated workflow access | Design sketch only — the shipped mechanism is flowsafe host-kit's run router checking `WorkflowMeta.allowedRoles`, not a breakwater wrapper | `packages/showcase/worker/workflows/access-request.ts` (role-gated approval workflow) |

The showcase runs two workflows with no direct sketch here:

- `access-request` is an access-grant approval flow demonstrating per-workflow
  `allowedRoles` and approval-gated provisioning.
- `wire-transfer` is the server-backed control-room scenario. It joins
  prompt-injection defense to a real durable approval and exact-suspension
  grant.

## Source

Each file demonstrates a workflow pattern using Mastra's code-first `createWorkflow()` API.
