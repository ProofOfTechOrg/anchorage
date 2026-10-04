---
"@proofoftech/breakwater": patch
---

A keyed connector call that throws after its abort signal fired now keeps its idempotency reservation pending, as an output validation failure already does, instead of releasing it. A call cut by an abort may already have taken effect at the provider, so releasing the key let a later call with the same business key run the effect again. The key stays reserved until stale takeover or operator recovery, and a call with the same key meanwhile is refused with `IDEMPOTENCY_CONFLICT`. An execute that throws while its signal is not aborted still releases its reservation, so the key stays retryable.
