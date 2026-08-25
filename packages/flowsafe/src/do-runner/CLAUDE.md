# Durable Object runner navigation

- `init.ts`, `runtime.ts`, `d1-storage.ts`: import swap, execution, D1, and retention
- `deployment-identity.ts`: environment binding and D1 sentinel verification
- `execution-fence.ts`: deployment-wide fence storage, CAS transitions, refusals, and admission predicates
- `start-idempotency.ts`: owner-bound start reservations, claims, replay decisions, and retention state
- `inventory.ts`: read-only drain categories, proof contract, unenumerables, and owned-table census
- `notification-predicate.ts`: import-safe due-notification predicate shared by inventory and notification storage
- `cause-chain.ts`: bounded root-cause inspection for narrowly recognized missing-table reads
- `do-status-error.ts`, `do-error-response.ts`: Durable Object refusal base class and structured HTTP rendering
- `durable-object.ts`, `thread-do.ts`, `hub-do.ts`: Durable Object hosts
- `path-safe-id.ts`, `memory-id.ts`, `execution-principal-header.ts`: run, memory, and execution identity
- `runtime.ts`, `pubsub.ts`: resume provenance and observation state
- `mastra-schema-guard.test.ts`: adopted-domain inventory and retention coverage

See [`../../../../docs/do-runner-design.md`](../../../../docs/do-runner-design.md).
