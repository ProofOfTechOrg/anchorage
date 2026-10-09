---
"@proofoftech/flowsafe": patch
---

`RunNotSuspendedError` extends `DoStatusError` with `status` 409 and `reason.code` `RUN_NOT_SUSPENDED`. The run object's resume route and `createRunRouter()` include the reason in the 409 body, and `DecideResult.resume.code` reports it when a decided approval's resume finds a workflow run not suspended, for example because it ended. Before this change that refusal carried no code, and a caller could tell it from a retryable resume failure only by parsing the message.
