# Showcase Worker navigation

- `worker.ts`: thin `createFlowsafeWorker()` host, authentication, budgets, and cron hooks
- `runtime.ts`: registers the six workflow modules
- `workflows/`: six launcher workflows; wire transfer also powers the control room
- `demo-auth.ts`: OAuth, expiring visitor sessions, role JWTs, and run budgets
- `demo-actors.ts`: local-only bearer identities
- `*.test.ts`: fetch, workflow, deployment identity, grant, auth, and budget proofs

```bash
pnpm --filter showcase test
pnpm --filter showcase typecheck
pnpm --filter showcase dev:worker
```
