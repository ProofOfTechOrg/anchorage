---
'@proofoftech/flowsafe': minor
---

Breaking: `RunStateUnreadableError` now extends `DoStatusError` with `status` `503` and `reason.code` `RUN_STATE_UNREADABLE`, and `AgentRunSelectorMismatchError` inherits both. The run object's `503` for a run whose state cannot be read carries the code, and `DecideResult.resume.code` reports it when a decided approval's resume cannot read the run. `createRunRouter()` now classifies an error through the run object's own mapping, so over an in-process `RunnerRuntime` such a run answers `503` with the code instead of `500` with `{ "error": "internal error" }`, and the agent and stream routers answer it with `503` instead of `500`.
