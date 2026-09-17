---
'@proofoftech/flowsafe': patch
---

Refuse two `Agent` entry points that `@mastra/core` releases newer than the declared peer expose. `FlowsafeDurableAgent.listActiveThreadRuns()` throws instead of returning the run, thread and resource ids of every thread on the pubsub instance with a run in flight, which Core scopes by neither principal nor agent. `FlowsafeDurableAgent.__setThreadRuntimeAgent()` throws instead of installing another agent as the target every thread-runtime path resolves through — `subscribeToThread()`, `claimThreadOwnership()`, `sendMessage()`, `queueMessage()`, `sendStateSignal()`, `sendNotificationSignal()` and `sendSignal()` — where one call would move every run those paths start, and `subscribeToThread()`'s replay target with them, onto an agent that carries none of the wrapper's guards. Both refusals carry the reason table's message; an installed 1.53.0 exposes neither member on Core, so no call that resolves today changes, and a caller that feature-detects either member now finds it on the wrapper and takes the refusal where the call was a `TypeError` before.

The durable-agent surface inventory now holds against the pinned peer and against newer 1.x releases together.
