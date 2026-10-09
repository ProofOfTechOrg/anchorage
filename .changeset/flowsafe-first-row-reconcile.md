---
'@proofoftech/flowsafe': patch
---

A workflow start on a runtime without an execution fence, whose workflow's `shouldPersistSnapshot` stores none of the statuses the run passes through (for example only `suspended`, for a run that completes without suspending), now writes the run's outcome as its first stored row and answers with its summary, on storage whose workflow capability has `withStoredRun`, as `FencedWorkflowsStorageD1`'s has. Before this change such a start answered `500` ('completed without a durable snapshot') after the run had done its work, and the run then read as unknown.

On storage without `withStoredRun`, such as Mastra's in-memory store, such a start still answers `500`, because the runtime cannot tell a row the start never stored from one removed after another instance settled the run; a resume whose run row is removed while it runs, and a fenced start whose run row is removed, now answer `409` with `RunSettledConflictError` when the workflow stores nothing after the removal, where both answered `500` before.

A workflow that can suspend must store `suspended`: the runtime cannot rebuild a suspension the engine did not store, so such a run cannot be resumed.
