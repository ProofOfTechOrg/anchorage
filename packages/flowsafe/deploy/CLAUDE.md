# Baseline deployment navigation

- [`README.md`](README.md): configuration and deployment checklist
- `worker.ts`: workflow definitions, runner class, verifier, and host composition
- `wrangler.jsonc`: D1, Durable Objects, crons, observability, and optional Queue
- `worker.e2e.test.ts`: baseline host proof

```bash
pnpm --filter @proofoftech/flowsafe deploy:dev
pnpm --filter @proofoftech/flowsafe typecheck
pnpm --filter @proofoftech/flowsafe test
```
