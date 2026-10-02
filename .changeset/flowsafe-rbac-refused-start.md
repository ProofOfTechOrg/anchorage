---
"@proofoftech/flowsafe": patch
---

A registered agent start whose caller Breakwater's RBAC gate refuses during preparation is refused with status 403 before the runtime starts and creates no run. An automated fire the agent's RBAC gate refuses now records `failed` instead of a run that did nothing. A created signal the gate refuses for a missing actor is preserved even when Mastra's isolate-wide run registry evicts the start's entry.

Security: Before this change, if Mastra's isolate-wide run registry evicted the start's entry between preparation and the first step, the loop called the model for the refused caller on the thread's history and returned its output.

No migration is needed.
