# Examples

Design sketches illustrating Mastra `createWorkflow()` patterns for Anchorage. These are not runnable -- they illustrate the API shape and integration points with breakwater and flowsafe.

## Index

| File | Pattern | Highlights |
|---|---|---|
| `gtm-outbound.ts` | Serial pipeline | Approval gate via flowsafe |
| `content-pipeline.ts` | Parallel branches | Parallel step execution |
| `lead-generation.ts` | Conditional branching | `.branch()` with hot/cold routing |
| `product-launch.ts` | Approval checkpoints | Multi-step serial approval |
| `custom-workflow-scoping.ts` | RBAC scoping | breakwater deployment-time RBAC wrapper |

## Source

Each file demonstrates a workflow pattern using Mastra's code-first `createWorkflow()` API.
