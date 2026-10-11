---
'@proofoftech/flowsafe': patch
---

The schedule tick audits a deferred fire it settles as `failed` with reason `invalid-deferred-dispatch` as a `schedule.fire` event with outcome `failed` and that reason. Before this change only the trigger history recorded that outcome: a fire whose stored dispatch record does not parse, a fire left claimed without a dispatch record after its run-cap check, claim or claim bookkeeping failed, and a fire whose stored dispatch target nests past the run-input bound and whose lookup throws reached no audit sink. The event names the stored dispatch's target, or its schedule's target as the store reads it when no dispatch record parses. When that schedule cannot be read, has a target kind other than `workflow` or `agent`, or is no longer stored (as when it is pending deletion), the tick logs `schedule-tick-bookkeeping-error` instead of auditing the fire.
