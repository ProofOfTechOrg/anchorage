---
'@proofoftech/fleet-control': patch
---

State three requirements a `FleetOperationStore` implementation must meet, and hold the shipped D1 store to them.

- `withAccountOperationLease` requires that the promise it returns settle only after the lease release completes, so a composition that awaits one call holds no lease when the next one takes it. `D1FleetOperationStore` already awaits its release before returning; the requirement now sits on the port both compositions depend on instead of being restated per caller.
- **BEHAVIOR CHANGE:** `readOperationRowsPage` requires a page of at most `limit` rows and serves a `limit` above 1,000 at 1,000, the one documented ceiling and the maximum page `D1FleetOperationStore` already enforced. An over-large `limit` costs the caller the rows beyond the ceiling rather than the read, where the D1 store refused it before. `readFleetAuditFindingsPage` and `readFleetMigrationItemsPage` forward a caller's `limit` unchanged, so they answer the same way against any conforming store. A `limit` that is not an integer of at least 1 is refused.
- **BEHAVIOR CHANGE:** `failOperation` reports an update target missing at its post-batch readback as a conflict, the classification `commitProgress` convergence already uses for a missing row, rather than as a divergence. Divergence keeps its narrower meaning: landed bytes that differ from the intended ones.
