---
"@proofoftech/flowsafe": patch
---

A durable-agent run whose start leg failed after its run was stored, and which a terminate then ended within Mastra's 30-second cleanup delay, no longer gets a second terminal `error` event on its run stream. When a start leg's own publication of its cancellation or timeout error fails, Mastra's retry publishes that error instead of the publication failure.

With a host-supplied pub/sub whose `publish` can fail, a run gets no terminal `error` event, and a later release does not publish one, when Mastra's publication of the error its start leg threw fails, or when both the leg's own publication of its cancellation or timeout error and Mastra's retry fail.
