# agent-starter navigation

Private consumer template with two roles. Public behavior:
[`README.md`](README.md).

Standalone agent host:

- `src/worker.ts`: the composed Worker; every route comes from `createFlowsafeWorker`
- `src/durable-objects.ts`, `src/workflows.ts`, `src/agent.ts`: runner, thread, hub, background tasks, and the approval-gated connector
- `src/config.ts`, `src/principal-context.ts`, `src/storage.ts`: verifier, execution principals, D1 domains

Workers for Platforms conformance artifacts (deletable in one step — see the
README's "Delete it"):

- `src/conformance/contract.json`: the only place binding names, paths, classes, and Durable Object instance names are written
- `src/conformance/candidate.ts`: the external candidate entry; owns no Durable Object class
- `src/conformance/state-v1.ts`, `state-v2.ts`: the trusted state entries, v2 adding exactly one class
- `conformance/`: build-only wrangler configurations and the emitted operator configuration
- `scripts/build-conformance.mjs`, `scripts/emit-conformance-config.mjs`, `scripts/conformance-verify.mjs`: artifact build, configuration emitter, and the local workerd harness. The rest of `scripts/` belongs to the standalone host

The contract is [`../../docs/fleet-control.md`](../../docs/fleet-control.md)
under "Implement the artifact contract"; `packages/fleet-control` enforces it.

```bash
pnpm --filter anchorage-agent-starter check
pnpm --filter anchorage-agent-starter build:conformance
pnpm --filter anchorage-agent-starter conformance:verify
pnpm test:conformance-config
```
