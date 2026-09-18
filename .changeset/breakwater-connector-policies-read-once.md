---
'@proofoftech/breakwater': minor
---

Sample a caller's connector definition once at construction. `createConnector()` read several members
of the `config` and `policies` objects it was handed more than once while validating them, so a
definition whose members are accessors could answer a refusal check with one value and the
construction that followed with another. Construction now reads `policies.idempotencyStore`,
`policies.idempotencyKeyMigration`, `policies.rateLimitStore`, `policies.networkEgress`,
`config.permissions`, `config.inputSchema`, `config.outputSchema` and `config.dryRunExecute` once
each; for an accessor-backed definition, the value that reaches the gate, the audit `detail` and the
store is the first read. `invokeConnector()` reads `options.toolCallId` once, so the value it
validates is the value it records on the call. The refusal messages and the order they fire in are
unchanged, and a definition built from plain data properties behaves as it did before.

The dry-run branch and the legacy key migrator still read `config.dryRunExecute` and
`policies.idempotencyKeyMigration` on each call, so a simulation or a migration acknowledgement
supplied after construction still takes effect from the next call.
