---
"@proofoftech/flowsafe": patch
---

An owner `/signal/notification` to a runtime-driven agent is now recorded in the notifications inbox and delivered at ingestion under the owner's principal: into the owner's running run, or persisted to agent memory when the thread is idle or the owner's run is not running. It never wakes a run, and no dispatch tick selects its row. The response is `{ record, delivery }`: the settled `NotificationRecord`, and Mastra's delivery decision with the delivered `signalId`. On a `FlowsafeDurableAgent` registered on no Mastra, the composition the shipped hosts use, the request failed with `502` before: the route called Mastra's `sendNotificationSignal()`, whose in-process delivery reads the notifications store from the agent's own Mastra.

Before writing anything, the route answers `409` with a `reason` when another principal's run holds the thread (`principal-mismatch`, with `retry: true`), when the delivery would need agent memory the agent lacks (`memory-unavailable`), or when a `dedupeKey` or `coalesceKey` matches a pending row the dispatch tick will deliver (`notification-pending`). After the write, the row is discarded when Mastra reports the thread blocked (`409`, `thread-blocked`), when the content policy denies the stored row's signal (`422`) or fails (`503`), and when storage or the send fails (`502`). A row whose isolate dies, or whose settle write the execution fence refuses, stays pending with no due time; clear it with a direct write that sets it `discarded`, or delete it.

When a host passes no `canPersist`, every principal takes this owner path, signal providers included, and a provider treats a `409` as a permanent drop.

A non-owner notification is still recorded for `createNotificationDispatchTick()`, which delivers each row individually as `system` on `notification.dispatch`; no Mastra delivery policy applies, neither the default nor a configured one. On an idle thread the tick wakes a run whose principal and approval requester is `system`: the self-approval bar does not cover the notification's author, the row stores no author, and the run's permission projection and audit are `system`'s. Idle starts are limited only by `consultRunCap` where the host wires it. The tick counts a failed attempt when the run cap refuses the wake, when another principal's run holds the thread, and when the agent does not declare `system` on `notification.dispatch` in `allowedAutomation`, and it discards the row after `maxDeliveryAttempts` failed attempts.

An agent without the runtime-driven brand keeps Mastra's delivery path for owner notifications and its `degraded: 'not-runtime-driven'` marker.

`BlockingAgentRun` and the `resolveBlockingRun` result of `createThreadSignalRoutes()` take an optional `status`, which the thread host fills from the run's stored status.

Migration: a host that accepts owner notifications for a runtime-driven agent must pass `resolveNotificationsStorage` to `createThreadSignalRoutes()`, or the route returns `409`. A host with its own `resolveBlockingRun` reports each run's `status`, so that an owner notification is not queued into a run that is not executing; a run with no status is treated as running. A caller that read an owner response's `record.record` or `record.decision` reads the settled record from `record` and the delivery from `delivery`.
