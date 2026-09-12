---
'@proofoftech/flowsafe': minor
---

Add optional `SignalRouterOptions.validateThreadTarget` using the existing bound-thread validator contract, with captured actor context before asynchronous validation. Hosts can enforce strict ownership before forwarding. Normalize thread refusals with status 404 and audit the final downstream result.

Contain audit and diagnostic failures in signal, objective, subscription and webhook routes so they preserve the selected response. Use own-property lookup for signal channels, objective methods and webhook provider configuration.
