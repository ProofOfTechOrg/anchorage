---
"@proofoftech/flowsafe": minor
---

Add trusted mutation-epoch inputs for schedule ticks and signal providers, and atomic admission for schedule claims, notification writes and subscriptions. Preserve original epochs across waits and deferred recovery. Typed epoch refusals defer provider delivery without spending notification delivery attempts; accepted receipts remain available for settlement and replay.

Custom notification stores can implement `MutationEpochNotificationsStorage` for authoritative enqueue admission. Legacy epoch-zero draining still permits notification data enqueue. A retained, atomically admitted remote notification operation can complete its matching receipt during proof-only or cutover. Required-epoch mode refuses immediate owner notification delivery and delayed dispatch until their execution admission can be established atomically. Owner ingestion refuses before creating an inbox record, including the generic Mastra path. Pending records remain retained. Upgrade and verify deployment writers before activation; this change does not establish a production cutover proof.
