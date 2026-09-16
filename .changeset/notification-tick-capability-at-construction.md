---
'@proofoftech/flowsafe': patch
---

Capture the conditional-delivery capability when a notification dispatch tick is built, whatever its `limit`. `createNotificationDispatchTick()` reads `storage` and refuses one without `updateNotificationDeliveryIfUnchanged` for every configuration, including `limit: 0`, so the `NotificationDeliveryStorage` requirement no longer depends on the limit. A `limit: 0` tick still resolves `{ due: 0, delivered: 0, failed: 0 }` without reading due rows, calling storage, or needing the `@mastra/core` patch; invalid numeric policy still fails ahead of the capture.

The notification routes refuse when the installed `@mastra/core` lacks the delivery-policy half of that patch. `createThreadSignalRoutes()` probes `resolveNotificationDeliveryDecision` at both the ingestion gate and the dispatch route, and answers 502 with the message naming the patch on the server log. The probe resolves once per isolate; tick construction keeps its synchronous probe and is unaffected.
