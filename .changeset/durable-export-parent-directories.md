---
'@proofoftech/fleet-control': patch
---

Synchronize the parent directories of filesystem exports before returning a durable location, including when an earlier attempt left a newly created directory behind.

Cancel abandoned export streams when filesystem setup or a supplied export store fails.
