---
"@proofoftech/flowsafe": patch
---

`reconcileApprovalsForSummary` no longer supersedes or files a step's approval when the step already has an approval bound to a later suspension, one with a greater `resumeCount`, than the summary it was given. A status read whose summary predated a timeout resume closed the approval the run object had just filed for the step's next suspension, and, unless the earlier suspension was a timer, filed one for the suspension that had ended; the new gate was left with no approval that could be filed again. Runs already left that way are not repaired: the operations runbook says how to end or resume them.
