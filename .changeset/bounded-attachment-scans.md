---
'@proofoftech/fleet-control': patch
---

Harden account-wide D1 and R2 attachment scans with a request-bounded, page-independent resumable engine. Rechecked inventory drift, malformed provider metadata, non-string or repeated dispatch cursors, and page or item overflows now fail closed instead of allowing an incomplete absence proof.
