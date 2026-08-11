# Durable Object runner navigation

- `init.ts`, `runtime.ts`, `d1-storage.ts`: import swap, execution, D1, and retention
- `deployment-identity.ts`: environment binding and D1 sentinel verification
- `durable-object.ts`, `thread-do.ts`, `hub-do.ts`: Durable Object hosts
- `path-safe-id.ts`, `memory-id.ts`, `execution-principal-header.ts`: run, memory, and execution identity
- `runtime.ts`, `pubsub.ts`: resume provenance and observation state
- `mastra-schema-guard.test.ts`: adopted-domain inventory and retention coverage

See [`../../../../docs/do-runner-design.md`](../../../../docs/do-runner-design.md).
