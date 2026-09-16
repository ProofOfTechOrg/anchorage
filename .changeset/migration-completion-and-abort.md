---
'@proofoftech/fleet-control': minor
---

`advanceFleetMigration()` accepts an `AbortSignal`, matching the audit advance, and a completion callback. `signal` is call-local and never persisted, and cancellation is cooperative: the composition checks it before dispatch and before each item advance, and work already started runs to completion. A cancellation this composition observes leaves the operation and its items exactly as they were and a later continue resumes; a signal the host also wires into its own provider or store surfaces inside the item step and durably fails the item like any other step failure.

`onComplete` is delivered at least once for a finalized operation, after the finalization is durable and before the call that observed it returns, so the host deduplicates on `operationId`. `AdvanceFleetMigrationOptions.onComplete` names the calls that deliver it. The alternative that fires only on the running-to-finalized transition loses the notification when the process dies between the durable finalize and the callback. A rejection propagates to the caller and leaves the durable finalization intact.

`CloudflareAdvanceFleetMigrationOptions` carries both, forwarding `signal` unbound and `onComplete` bound to the caller's options object. Both additions are optional members, so existing callers are unchanged.
