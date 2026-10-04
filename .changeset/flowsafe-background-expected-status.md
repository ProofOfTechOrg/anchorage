---
"@proofoftech/flowsafe": patch
---

`DurableObjectWorkflowsStorageD1.updateWorkflowState`, the workflow domain `createBackgroundTaskD1Domains()` composes, now applies `expectedStatus` as the compare-and-set guard `@mastra/core` defines: when the stored snapshot's status does not match, the update writes nothing and resolves `undefined`, and the guard is never stored in the snapshot. Before this change the domain ignored the guard and stored it. Because the domain reports concurrent-update support, Mastra relies on that guard to let only one of two concurrent resumes of a suspended run claim it; without it both could proceed.
