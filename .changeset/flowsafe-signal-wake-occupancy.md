---
"@proofoftech/flowsafe": patch
---

Message, signal, notification-dispatch, and schedule routes now re-read the thread's blocking run when they wake. If that run ends while the route checks authorization or content, the idle thread wakes instead of answering `blocked`. A schedule fire no longer settles a permanent `blocked`/`skipped` receipt in that case.

This matters for hosts whose `resolveBlockingRun` answer can change while the route holds `serializeDispatch`, such as a host whose start returns while the run keeps executing. Hosts that execute a run's steps under the dispatch lock see no change. Each wake that found a blocking run makes one more `resolveBlockingRun` call, per send in a notification dispatch batch. No migration is required.
