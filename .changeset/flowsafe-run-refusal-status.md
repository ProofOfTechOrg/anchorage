---
'@proofoftech/flowsafe': minor
---

The runtime's run refusals carry the HTTP status the run object answers them with as `status`: `UnknownWorkflowError` and `UnknownRunError` `404`, `RunAlreadyExistsError`, `RunTerminalConflictError` and `RunSettledConflictError` `409`, and `InvalidRunRequestError` `400`. The run object and `createRunRouter()` answer them as before. They do not extend `DoStatusError`, so a route that renders a `DoStatusError` with its own status, such as a thread object's signal routes, answers them as before.

Breaking: an approval decision whose resume runs in process through `resumeViaRuntime(runtime)` reports such a refusal's message in `DecideResult.resume.error`, and in each batch decide result, instead of `internal error`, as a run object's answer already does; for example `no run '<runId>' found for workflow '<workflowId>'` for a run that no longer exists. Such a refusal is no longer logged as a `route-internal-error` line with route `approvals.resume`; the `approval.resume` audit event still records its message.

Breaking: a schedule tick whose `startAgent` seam starts a threadless agent through `RunnerRuntime` in process records a start the runtime refuses with `InvalidRunRequestError` or `UnknownWorkflowError` as `failed` with reason `dispatch-refused` once the `status` lookup states that the run does not exist, as it records the thread object's `400` and `404` answers. Before this change such a fire stayed deferred and was recorded `dispatch-unresolved` an hour after it fired, or stayed deferred while the lookup kept failing. A `status` lookup that lets the runtime's `UnknownWorkflowError` or `UnknownRunError` escape now states that a threadless start's run does not exist. A `signalAgent` seam that calls the thread host in process and lets the runtime's `InvalidRunRequestError` or `UnknownWorkflowError` escape now fails its fire as `dispatch-refused`, where the fire was retried on a later pass before this change; the same refusal through a thread object's signal route still answers `502` and is retried.

Breaking: code that reads `status` from a thrown value now finds it on these six classes.
