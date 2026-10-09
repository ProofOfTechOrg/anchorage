---
'@proofoftech/flowsafe': patch
---

A workflow start on a runtime without an execution fence, whose `shouldPersistSnapshot` stores none of the statuses the run passes through (for example only `suspended`, for a run that completes without suspending), now writes the run's outcome as its first stored row and answers with its summary. Before this change such a start answered `500` ('completed without a durable snapshot') after the run had done its work, and the run then read as unknown. On storage without `withStoredRun`, such as Mastra's in-memory store, a resume whose run row is removed while it runs, and a fenced start whose run row is removed, now answer `409` with `RunSettledConflictError` when the workflow stores nothing after the removal; both answered `500` before. A workflow that can suspend must store `suspended`: the runtime cannot rebuild a suspension the engine did not store, so such a run cannot be resumed.
