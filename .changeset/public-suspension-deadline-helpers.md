---
'@proofoftech/flowsafe': minor
---

Expose `isArmableSuspensionDeadlineMs` and `suspensionDeadlinesOf` from `do-runner`, together with the `SuspensionDeadlineEntry` and `RejectedSuspensionDeadline` types that projection returns. Add the lightweight `do-runner/constants` entry for deadline values and timeout detection, and `do-runner/testing` for constructing fixtures with the same timeout envelope as the alarm path.

Reuse the existing arming bounds, derivation and alarm payload factory. The test helper does not authorize a resume or mint an approval grant.
