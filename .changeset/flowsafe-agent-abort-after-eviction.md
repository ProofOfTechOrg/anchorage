---
"@proofoftech/flowsafe": patch
---

A settlement or terminate abort now reaches a durable-agent start leg's model or tool call that was already in flight when Mastra's isolate-wide run registry evicted the run's entry, when the run has a total time budget. Before this change the eviction unlinked the leg's abort from the signal that call held. Calls a leg starts after such an eviction take Mastra's rebuilt entry and are not covered.
