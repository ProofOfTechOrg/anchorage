---
'@proofoftech/flowsafe': patch
---

The schedule tick audits a schedule whose stored target kind is neither `workflow` nor `agent` with `target: 'workflow'`, as the tick dispatches it. Before this change the `schedule.fire` events the tick emitted for such a schedule while firing it carried the stored kind in `target`, outside the `'workflow' | 'agent'` type the audit sink receives.
