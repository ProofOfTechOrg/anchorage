---
"@proofoftech/flowsafe": patch
---

`FencedWorkflowAdmissionCapability` gains an optional `withStoredRun`, which `FencedWorkflowsStorageD1` implements: inside it, a leg's write never inserts the run row the leg already stored. The runtime runs each leg's workflow writes inside it, aborts a leg whose run row is gone after the leg stored it at the leg's next liveness touch, and answers such a leg's writes with `RunSettledConflictError` (HTTP 409). Before, with a short `RUN_RETENTION_DAYS` such as `0`, a leg whose run another instance terminated, timed out or interrupted kept running its steps after retention removed the run's row, and its next write stored the run again as unsettled; a leg whose workflow skips its terminal snapshot answered 500. During a deploy or a rollback, a leg run by an earlier flowsafe version still stores a removed run again as unsettled.

A custom capability without `withStoredRun` keeps the earlier behavior until the host's capability implements the member.

With `RUN_RETENTION_DAYS` of `0`, a 409 from a start or a resume can also mean that the run completed and retention removed its own terminal row before the end-of-leg write read it, so check the run's effects before treating it as cancelled.
