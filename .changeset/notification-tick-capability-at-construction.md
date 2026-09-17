---
'@proofoftech/flowsafe': minor
---

Capture the conditional-delivery capability when a notification dispatch tick is built, whatever its `limit`. `createNotificationDispatchTick()` reads `storage` and refuses one without `updateNotificationDeliveryIfUnchanged` for every configuration, including `limit: 0`, so the `NotificationDeliveryStorage` requirement no longer depends on the limit. A `limit: 0` tick still resolves `{ due: 0, delivered: 0, failed: 0 }` without reading due rows, calling storage, or needing the `@mastra/core` patch; invalid numeric policy still fails ahead of the capture.

The notification ingestion route refuses when the installed `@mastra/core` lacks the delivery-policy half of that patch. `createThreadSignalRoutes()` probes `resolveNotificationDeliveryDecision` at the ingestion gate, where delivery runs through `agent.sendNotificationSignal` and reaches the delivery-policy lookup, and answers 502 with the message naming the patch on the server log. The dispatch route carries the synchronous source-key probe its summaries need, and delivers through `agent.sendSignal`, which never reaches that lookup. The probe resolves once per isolate; tick construction keeps its synchronous probe and is unaffected.
