---
'@proofoftech/fleet-control': patch
'@proofoftech/flowsafe': patch
---

Support signed maintenance for platform-authored Workers for Platforms catalogs. Catalog signing profiles can supply the maintenance keys without external-state artifacts. Catalog uploads receive their public verifier and local identity; FlowSafe relays capabilities while retaining the local receipt secret and validates the catalog script and digest before maintenance work.

Existing catalog artifacts need a rebuilt FlowSafe runtime and explicit maintenance enrollment. The host configures the matching global dispatcher verifier.

Persist catalog ownership explicitly in Fleet records and preserve it through native D1 migration and export-backed teardown. Catalog cleanup checks its own script and namespace authority. Force re-entry on completed or reserved records uses claim-releasing deletion when the store supports it.

Preserve the prior mutable Worker schema identity while D1 advances and retain migration authority through compatibility teardown retries. Permit declared catalog binding changes with exact owner and uploaded-target checks.
