---
'@proofoftech/flowsafe': patch
---

Require the retention cursor seam on the purge duty. `runMaintenanceDuty('purge', env, context)` takes the new `MaintenancePurgeDutyContext`, whose `advanceRetentionCursor` is required, matching the `advanceCursor` the run-retention purge itself requires; the other duties keep the optional `MaintenanceDutyContext`. A purge invocation whose context omits the callback is refused under a `config-error` naming `maintenance.purge.advanceRetentionCursor` before any purge surface runs, rather than purging the remaining surfaces and reporting a `retention-purge` failure.
