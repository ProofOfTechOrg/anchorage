---
'@proofoftech/flowsafe': patch
---

Refuse two `Agent` entry points that `@mastra/core` releases newer than the declared peer expose. `FlowsafeDurableAgent.listActiveThreadRuns()` throws instead of returning the run, thread and resource ids of every thread tracked on the pub/sub instance, which Core scopes by neither principal nor agent. `FlowsafeDurableAgent.__setThreadRuntimeAgent()` throws instead of installing another agent as the target the thread runtime drives for `subscribeToThread()`, `claimThreadOwnership()`, `sendMessage()`, `queueMessage()` and `sendStateSignal()` — an agent that would carry none of the wrapper's guards. Both refusals carry the reason table's message, and an installed 1.53.0 exposes neither member, so no call that resolves today changes.

The durable-agent surface inventory now holds against the pinned peer and against newer 1.x releases together. It records, per member whose presence differs between them, the prototype level each core places it on, and still fails on any member no list classifies.
