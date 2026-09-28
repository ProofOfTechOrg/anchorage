---
"@proofoftech/flowsafe": minor
---

`createThreadSignalRoutes()` accepts an optional `notificationDispatchAllowed` callback for non-owner notification ingestion. `ThreadAgentHost` gains a required `notificationDispatchAllowed()` member that checks the target agent's catalog declaration for `system` on `notification.dispatch`; hosts can wire it to the router callback. A refused non-owner notification receives `409` with `notification-dispatch-forbidden` before an inbox row is created. This is BREAKING for hand-built `ThreadAgentHost` implementations; hosts from `createThreadAgentHost()` get the member automatically, hence the minor bump.
