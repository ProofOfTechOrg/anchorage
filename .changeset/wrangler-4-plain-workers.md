---
'@proofoftech/flowsafe': minor
'@proofoftech/fleet-control': minor
---

Require host-provided Wrangler `>=4.118 <5` without installing it as a Flowsafe peer. Hosts that use `flowsafe-provision` must now install a compatible Wrangler version directly.

Fleet Control now supports an explicit Wrangler command, creates D1 databases through Wrangler 4's current output contract, and revokes plain-Worker credentials through the Workers API without creating untracked Worker versions.

Custom `PlainWorkerRouteApi` implementations must add `deleteControlSecrets(scriptName, secretNames, fence)`. Implement it with the Workers script-secret DELETE API, treat an HTTP 404 as already deleted, and retain a final authoritative secret-list check.
