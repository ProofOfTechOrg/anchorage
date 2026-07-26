# Showcase Worker navigation

- `worker.ts`: thin `createFlowsafeWorker()` host, authentication, budgets, and cron hooks
- `runtime.ts`: registers the six workflow modules
- `workflows/`: five launcher workflows plus the control-room wire transfer
- `demo-auth.ts`: OAuth, short-lived tenant JWTs, run budgets, and sandbox lifecycle
- `demo-reset.ts`: authenticated tenant reset
- `demo-actors.ts`: local-only bearer identities
- `*.test.ts`: fetch, workflow, tenancy, grant, auth, budget, and reset proofs

```bash
pnpm --filter showcase test
pnpm --filter showcase typecheck
pnpm --filter showcase dev:worker
```
