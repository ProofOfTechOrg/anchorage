---
'@proofoftech/flowsafe': patch
---

`RunnerRuntime.start()` and `resume()` refuse with `InvalidRunRequestError` (`400`) carrying Mastra's message only for these failures: a start whose input data, initial state or request context fails the workflow's schema, a resume whose data fails the suspended step's resume schema, a resume that names a step that is not suspended, and a resume of a run Mastra finds no longer suspended or no longer stored. Any other error from the start or resume leg whose message merely contains one of the phrases the runtime used to match, such as `Invalid input` from a validation library failing inside the leg, now propagates as an internal error (`500`, `internal error` to callers) instead of a `400` that published its text, which could name internal schema paths. A start whose initial state or request context fails its schema now always answers `400`; before, it did so only when an issue's text contained `Invalid input`.
