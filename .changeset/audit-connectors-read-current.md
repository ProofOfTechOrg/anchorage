---
"@proofoftech/breakwater": patch
---

Read required agent audit context values once before validating and recording them. Accessor-backed context cannot replace a validated `agentId` or `entryPath` during the copy.

Document that `idempotencyKeyMigration` is validated at connector construction and read again for each execution or legacy migration. Also clarify that the conformance harness reports `no-egress-declaration` after URL and host parsing succeeds before a subject is bound.
