---
'@proofoftech/flowsafe': minor
---

Rename the maintenance receipt audience from `anchorage-fleet-control` to `flowsafe-maintenance-receipt`, matching the protocol naming of the sibling capability audience instead of tracking a package name.

This is a wire change. `mintMaintenanceReceipt` and `verifyMaintenanceReceipt` both pin the audience, and the issuer runs inside each deployed Worker while the verifier runs at the control plane, so a host minting receipts on an earlier FlowSafe fails verification against a host on this release, and the reverse. Upgrade the issuer and the verifier together, before a fleet exists. A test now pins the literal on the `aud` claim, and the constant records that a later rotation needs a verifier-side accept-set rather than another lockstep break.
