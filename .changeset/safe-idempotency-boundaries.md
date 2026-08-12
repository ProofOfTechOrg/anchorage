---
"@proofoftech/breakwater": minor
---

Use collision-proof connector idempotency keys with a fail-closed legacy
migration boundary. Validate D1 pending TTLs and rate-limit counts, commit D1
rate increments with cleanup atomically, and add trusted run correlation to
connector audits. Agent CLI timeouts now terminate the descendant process tree,
confirm POSIX group disappearance or Windows taskkill completion, and report a
stable failure if termination cannot finish. Connector output is validated and
transformed before replay commit so an invalid result is not stored under the
idempotency key. D1 refuses non-JSON-native results that would change during
persistence instead of creating a type-changing replay.

This changes keyed-connector construction and rollout: hosts must acknowledge
that legacy writers are drained before an absent legacy key may execute, and
custom atomic stores must add non-mutating `inspect()` support. Safe legacy
records still replay; ambiguous records remain denied until an operator maps
them to one proven v2 identity. Custom `RateLimitDatabase` adapters must also
provide D1-compatible transactional `batch()` semantics so cleanup failure can
roll back the associated increment.
