---
"@proofoftech/flowsafe": patch
---

A durable-agent run cancelled while its leg runs in the thread object, or terminated while suspended there, now ends its run stream with a terminal `error` event (`RunCancelledError`, or `RunTimedOutError` for a timeout) and releases the run's local state. A model or tool call that such a cancellation cuts sees an abort reason named `AbortError` whose `cause` is that error. Before this change observers of the run waited indefinitely, the run's Mastra registry entries stayed in memory, and a threaded run kept its thread active in the object, so a signal sent to the thread was queued into the cancelled run and could be lost.
