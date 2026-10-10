---
'@proofoftech/flowsafe': minor
---

Breaking: an approval resume of a durable-agent run that has ended, cancelled and timed-out runs included, now answers `409` with `reason.code` `RUN_NOT_SUSPENDED` for the principal that started the run, threaded or unthreaded, and `DecideResult.resume.code` reports it, as for a workflow run. Before this change it answered `404 run not found`, from the thread object or, once the run's ownership claims were released, from `createAgentThreadTopology().resume()`, which now asks the thread object's owner-checked replay read before it answers `404`. Another principal still gets `404`, and so does every principal for an ended run from an earlier version that recorded no execution owner. `RunNotSuspendedError` accepts any `RunStatus`.
