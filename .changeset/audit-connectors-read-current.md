---
"@proofoftech/breakwater": patch
---

Read required agent audit context values once before validating and recording them. Accessor-backed context cannot replace a validated `agentId` or `entryPath` during the copy.

Document that `idempotencyKeyMigration` is validated at connector construction. Caller-held policies are read again when an execution reaches the absent-legacy migration gate and when a validated, ambiguous legacy migration reaches the acknowledgement gate; the single-tenant preset retains its frozen construction snapshot. Also clarify that the conformance harness reports `no-egress-declaration` after URL and host parsing succeeds before a subject is bound.
