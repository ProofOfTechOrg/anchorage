# breakwater navigation

Public behavior:

- [`README.md`](README.md)
- [`CONNECTORS.md`](CONNECTORS.md)
- [`../../docs/breakwater-architecture.md`](../../docs/breakwater-architecture.md)
- [`../../docs/policy-engine-design.md`](../../docs/policy-engine-design.md)
- [`../../docs/connector-interface.md`](../../docs/connector-interface.md)
- [`../../docs/agent-cli-connectors.md`](../../docs/agent-cli-connectors.md)

Source navigation is in [`src/CLAUDE.md`](src/CLAUDE.md). `dist/` is generated.

```bash
pnpm --filter @proofoftech/breakwater lint
pnpm --filter @proofoftech/breakwater typecheck
pnpm --filter @proofoftech/breakwater test
pnpm --filter @proofoftech/breakwater build
pnpm --filter @proofoftech/breakwater test:packed-consumer
```
