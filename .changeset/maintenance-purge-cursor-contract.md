---
'@proofoftech/flowsafe': minor
---

Require the retention cursor seam on the purge duty. `runMaintenanceDuty('purge', env, context)` takes the new `MaintenancePurgeDutyContext`, whose `advanceRetentionCursor` is required, matching the `advanceCursor` the run-retention purge itself requires; the other duties keep the optional `MaintenanceDutyContext`. `FlowsafeWorker.runMaintenanceDuty` declares that split as two overloads — `'purge'` with a required `MaintenancePurgeDutyContext`, and `Exclude<MaintenanceDuty, 'purge'>` with the optional `MaintenanceDutyContext` — so a caller holding a union-typed `duty` narrows it to one branch before calling: a single call spanning the whole union matches neither overload and no longer compiles. A purge invocation whose context omits the callback is refused under a `config-error` naming `maintenance.purge.advanceRetentionCursor` before any purge surface runs, rather than purging the remaining surfaces and reporting a `retention-purge` failure.
