---
"@proofoftech/breakwater": patch
---

`createGuardedAgent` reads `policies`, `applicationInputProcessors`, `applicationOutputProcessors`, and `allowedPrincipalKinds` once, by index. `RBACMiddleware` also reads `allowedPrincipalKinds` once, by index. A list whose iterator differs from its elements cannot change what is enforced after validation.

A non-array `allowedPrincipalKinds` is refused with "must be an array". No migration is needed.
