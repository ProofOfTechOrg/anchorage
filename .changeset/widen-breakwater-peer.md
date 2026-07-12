---
'@proofoftech/flowsafe': patch
---

Widen the optional `@proofoftech/breakwater` peer range from `^0.2.0` to `>=0.2.0 <1.0.0`. Future breakwater 0.x minors stay in-range, so changesets no longer escalates flowsafe to a spurious MAJOR on every breakwater minor release.
