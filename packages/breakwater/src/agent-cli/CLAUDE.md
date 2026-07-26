# Agent CLI navigation

- `index.ts`: generic, Claude Code, and Codex adapters
- `tail-accumulator.ts`: bounded UTF-8 output capture
- `agent-cli.test.ts`: argv, approval, dry-run, timeout, output, and diagnostics coverage

Public usage is in [`../../../../docs/agent-cli-connectors.md`](../../../../docs/agent-cli-connectors.md).

```bash
pnpm --filter @proofoftech/breakwater test -- src/agent-cli/agent-cli.test.ts
```
