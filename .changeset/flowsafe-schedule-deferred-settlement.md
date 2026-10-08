---
"@proofoftech/flowsafe": patch
---

A threadless scheduled agent start whose outcome the tick could not learn, such as one whose start response was lost, stayed deferred forever. The drain inventory's `schedule-deferred-dispatches` category kept counting it, and deleting its schedule stayed pending (`202`).

The tick now records such a fire as `failed` with reason `dispatch-unresolved` once the fire is an hour old and the `status` lookup throws with status 404. This includes a fire an earlier version deferred after a refusal, which the 0.26.0 notes said would stay deferred. A `status` seam must answer with the thread Durable Object's own status: a 404 relayed from another hop now fails a threadless start an hour after its fire.

The fire of a run that started and was then cancelled, timed out or purged before any reconcile pass saw it is recorded `failed` too, and so is the fire of one that is still running with its ownership commit stuck behind a start recovery fault. The run's own status is authoritative, so check the agent's external effects before running its work again.

A deferred threaded fire whose target had already stored its receipt stayed deferred, holding a slot in every reconcile pass, when its redelivery was refused and the `status` lookup kept failing. The tick now records it from that receipt.
