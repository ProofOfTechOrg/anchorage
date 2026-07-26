# Workerd spike navigation

The spike is an executable integration proof, not a consumer template. Use [`../../agent-starter/README.md`](../../agent-starter/README.md) for application wiring.

- `worker.ts`: Worker and Durable Object proof host
- `wrangler.jsonc`: local bindings and append-only migrations
- `llm-*.ts`: optional provider-backed proof
- `../scripts/spike-verify.mjs`: deterministic restart and security driver
- `../scripts/spike-verify-llm.mjs`: credentialed live-model driver

```bash
pnpm --filter @proofoftech/flowsafe spike:verify
pnpm --filter @proofoftech/flowsafe spike:verify:llm
```
