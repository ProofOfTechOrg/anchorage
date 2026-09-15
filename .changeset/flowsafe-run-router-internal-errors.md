---
'@proofoftech/flowsafe': patch
---

Return a generic internal-error response for unexpected run-router failures while retaining the original error in server diagnostics. Preserve typed refusal status, message and reason contracts. Contain diagnostic conversion and logging failures so they cannot prevent the generic HTTP response.
