# agent-starter navigation

Private consumer template. Public behavior:
[`README.md`](README.md).

Standalone agent host:

- `src/worker.ts`: the composed Worker; every route comes from `createFlowsafeWorker`
- `src/durable-objects.ts`, `src/workflows.ts`, `src/agent.ts`: runner, thread, hub, background tasks, and the approval-gated connector
- `src/config.ts`, `src/principal-context.ts`, `src/storage.ts`: verifier, execution principals, D1 domains

```bash
pnpm --filter anchorage-agent-starter check
```
