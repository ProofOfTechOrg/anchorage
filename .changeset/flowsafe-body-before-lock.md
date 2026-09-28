---
"@proofoftech/flowsafe": patch
---

Signal routes, agent-host start and resume routes, and the runner's start route read the request body before waiting for the thread's lock, so a request whose sender disconnects while it waits is still processed.
