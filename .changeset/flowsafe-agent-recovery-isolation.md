---
"@proofoftech/flowsafe": patch
---

The agent thread object's alarm recovers every start journal even when one journal's recovery fails for a reason other than a pending start. The failing journal is kept and logged as an `agent-start-recovery-failed` line naming its run, and the wake reports the failure after the other journals are done. Before this change such a failure stopped recovery of every journal listed after it until it cleared.
