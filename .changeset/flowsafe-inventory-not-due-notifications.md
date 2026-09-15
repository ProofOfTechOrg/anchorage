---
'@proofoftech/flowsafe': minor
---

The pending-notifications inventory lists pending agent-inbox notifications whether due, scheduled for later, or carrying no due timestamp. Its count includes these rows, its notDue total identifies those not yet due, and entry details expose summaryAt alongside deliverAt.

Pending notifications keep the deployment drain proof open until delivered, discarded, or deleted. Rescheduling alone does not clear the proof; a pending notification with neither timestamp requires a direct write or deletion because dispatch and retention leave it pending.
