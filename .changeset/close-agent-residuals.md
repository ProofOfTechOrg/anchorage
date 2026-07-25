---
'@proofoftech/flowsafe': minor
---

Close the durable-agent, agent-schedule, notification-dispatch, and D1 background-task execution residuals with tenant-safe thread routing and eviction-safe approval resume. Harden stored schedule context and core schedule-contract validation, make D1 notification coalescing and partial updates concurrency-safe, serialize due delivery with 100-id batching, close failed resume streams, validate public numeric configuration synchronously, preserve nested Mastra background-task SSE events, and require a proven process shutdown before spike restart.
