---
"@proofoftech/flowsafe": patch
---

`/signal` answers 409 when a thread has no wired resource, matching the sibling routes. Signals and messages sent into suspended runs persist to thread memory instead of remaining in the isolate, including wakes, notification dispatch, and agent-schedule fires.
