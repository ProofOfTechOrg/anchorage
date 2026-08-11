---
"@proofoftech/breakwater": minor
---

Add `singleTenantConnectorPolicies()` for physically isolated connector hosts.

The validated preset requires shipped D1 idempotency and rate-limit stores when a manifest declares those controls, an external production audit sink or an explicit development-only opt-out, organization egress policy, configured principal permissions, and the safe background-execution policy.

Construction rejects tenant-isolation scope, in-memory durability stores, egress outside the organization allowlist, weakened destructive approval, contradictory audit settings, and modified branded presets. Existing unbranded `ConnectorPolicies` behavior remains unchanged.
