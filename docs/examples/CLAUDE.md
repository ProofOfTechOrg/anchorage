# examples/

TypeScript `createWorkflow()` design sketches for Anchorage — illustrative, not runnable; they show API shape and breakwater/flowsafe integration points.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `README.md` | Index of the sketches with pattern + highlights and their YAML origin | Choosing an example, understanding what each demonstrates |
| `gtm-outbound.ts` | Serial pipeline with a flowsafe approval gate | Implementing a serial approval pipeline |
| `content-pipeline.ts` | Parallel step execution | Implementing parallel workflow branches |
| `lead-generation.ts` | Conditional branching (`.branch()` hot/cold routing) | Implementing conditional routing |
| `product-launch.ts` | Multi-step serial approval checkpoints | Implementing multi-checkpoint approval |
| `custom-workflow-scoping.ts` | Deployment-time RBAC scoping with breakwater | Implementing role-scoped workflow access |
