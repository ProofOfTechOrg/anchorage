---
'@proofoftech/fleet-control': minor
---

`advanceFleetMigration()` accepts an `AbortSignal` and a completion callback, matching the audit advance. `signal` is call-local and never persisted: it is checked at the public entry, before the action branch, and again at the head of each item advance, outside the step's failure handler — so a cancellation leaves the operation and its items exactly as they were and a later continue resumes, rather than durably failing the item.

`onComplete` runs on every call that returns `complete` — the call that finalizes the operation, a later continue on the finalized operation, and a replayed start of the same operation id — after the finalization is durable and before that call returns. Delivery is therefore at least once, and the host deduplicates on `operationId`; the alternative that fires only on the running-to-finalized transition loses the notification when the process dies between the durable finalize and the callback. A rejection propagates to the caller and leaves the durable finalization intact.

`CloudflareAdvanceFleetMigrationOptions` carries both, forwarding `signal` unbound and `onComplete` bound to the caller's options object. Both additions are optional members, so existing callers are unchanged.
