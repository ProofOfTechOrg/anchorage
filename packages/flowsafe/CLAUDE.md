# flowsafe navigation

Public behavior:

- [`README.md`](README.md)
- [`../../docs/approval-system.md`](../../docs/approval-system.md)
- [`../../docs/flowsafe-architecture.md`](../../docs/flowsafe-architecture.md)
- [`../../docs/do-runner-design.md`](../../docs/do-runner-design.md)
- [`../../docs/durable-agents.md`](../../docs/durable-agents.md)
- [`../../docs/deployment-reference.md`](../../docs/deployment-reference.md)

Operational examples:

- [`deploy/README.md`](deploy/README.md): baseline Worker
- [`../../packages/agent-starter/README.md`](../agent-starter/README.md): advanced agent host
- [`spike/`](spike/): workerd verification

```bash
pnpm --filter @proofoftech/flowsafe lint
pnpm --filter @proofoftech/flowsafe typecheck
pnpm --filter @proofoftech/flowsafe test
pnpm --filter @proofoftech/flowsafe build
pnpm --filter @proofoftech/flowsafe spike:verify
```
