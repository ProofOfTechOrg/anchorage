---
"@proofoftech/flowsafe": minor
---

A `DurableObjectRunner` workflow step can wait without an approval. It suspends with `SUSPENSION_TIMER_PAYLOAD_KEY` (`'flowsafe.timer'`, exported from `@proofoftech/flowsafe/do-runner/constants` and `@proofoftech/flowsafe/do-runner`) set to `true` beside its `flowsafe.deadlineMs`, and the run's Durable Object resumes it with the timeout envelope when the deadline expires. A run that must outlast one Durable Object invocation waits this way between steps.

The run object lists such steps in the new `RunSummary.suspensionTimers` on its start, resume, status, dispatch-status and protected-replay responses. It lists a step only when the current suspension derives an armable deadline for it, the marker is exactly `true`, the entry has not been abandoned, and an alarm is scheduled. `queueApprovalForSuspension` files nothing for a listed step, and `reconcileApprovalsForSummary` supersedes its stale open records without filing a new one. Every other case files an ordinary approval, so a timer step accepts an approval decision's `{ approved, comment?, decidedBy? }` as well as the timeout envelope and re-checks its wait condition on every resume. During a rollout, a timer step gets an ordinary approval wherever a Worker or run object without this change handles the run. Reconciliation supersedes that record once the step has suspended again.

Approvals after a timer, for an abandoned timer or the gate the run reaches next, are filed when a host reads the run's status with approval reconciliation, as `createFlowsafeWorker` does.

A suspension-deadline resume now keeps the run's recorded `requestedBy` and `requestedByKind` instead of recording `SUSPENSION_DEADLINE_PRINCIPAL_ID` with kind `system`, so separation of duties applies to the gate after a timeout as it would without the timeout. A legacy requester without a kind is kept with kind `human`. The reserved principal is recorded only for a run without a requester, and a run whose recorded requester is already the reserved principal keeps it. A step that recognized a timeout resume by that principal id should test its resume data with `isSuspensionTimeoutResumeData` instead; a `RunSummary` no longer shows that a timeout advanced the run.

A step declaring a Zod `suspendSchema` must declare `flowsafe.timer` as well as `flowsafe.deadlineMs`, or use a loose object; a schema that strips the marker files an approval on every wait.

`DurableKeyValueStorage` gains an optional `getAlarm`. A storage without it lists no timer steps, so their approvals are filed.
