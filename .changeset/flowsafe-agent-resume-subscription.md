---
"@proofoftech/flowsafe": patch
---

A durable-agent run resumed in the same thread object no longer leaks the previous leg's Mastra abort-request subscription, which kept that leg's run state in memory for the object's lifetime.
