---
'@proofoftech/flowsafe': patch
---

Signal-provider delivery-error log events now carry the same `terminal` flag as their delivery-rejected siblings, so a dropped-forever throw is distinguishable from a deferred one without re-deriving the classification.
