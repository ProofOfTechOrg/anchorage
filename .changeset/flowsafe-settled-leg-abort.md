---
'@proofoftech/flowsafe': patch
---

On `@mastra/core` 1.75.0, a run leg that the liveness touch aborts because another instance settled the run, or because retention removed the run's row, answers `409` with `RunSettledConflictError`, or with the settled summary for a start whose stored row still carries that start, when the workflow's `shouldPersistSnapshot` does not store `waiting`, as the durable-agent loop's does not. Core 1.75.0 reports such an abort as `waiting` instead of `canceled`, so before this change those legs answered `503` with `RunStateUnreadableError`, an agent start or resume whose run row was removed ended its stream with that error, and the resume of a run with legacy (version 1) run provenance answered with status `waiting`.
