---
'@proofoftech/fleet-control': patch
---

Accept successful Cloudflare list responses with `errors: null` or `result_info: null`. Retry plain-Worker maintenance requests answered by a workers.dev platform 404 or 500 text page, for up to 60 seconds at 2-second intervals by default. Both `PlainWorkerBackendOptions` and `CloudflareApiPlainWorkerBackendOptions` expose `maintenanceRouteReadyTimeoutMs` and `maintenanceRouteReadyIntervalMs` to configure these bounds, and `wait` to delay reconciled mutation retries. Host Workers that fetch tenant maintenance origins on the same account's workers.dev subdomain require the `global_fetch_strictly_public` compatibility flag. Verify account-owned API tokens through the account endpoint, with fallback to user-token verification when the account endpoint is unavailable.

Record first-page R2 access refusals (403, error 10003) for non-default jurisdictions in the required `FleetResourceInventory.unavailableR2Jurisdictions` array, preserving failures for default, later pages, and other errors.

Export `FleetInventoryR2Jurisdiction` from the root and Cloudflare control-plane entries, and expose the root's reachable `D1FleetInventoryRunStoreOptions`, `D1FleetOperationStoreOptions`, `FleetInventoryDeploymentFactKind`, `FleetInventoryFailureReason`, `FleetInventoryGeneration`, `FleetInventoryRowKind`, `FleetInventoryRunProgress`, `FleetInventoryRunRecord`, `FleetInventoryStage`, `FleetInventoryStagedFact`, `FleetInventoryStagedRow`, `FleetInventoryStageInput`, `FleetInventoryStageResult`, `OrdinaryWorkerDeploymentVersion`, `PreparedOrdinaryWorkerDeploymentVersions`, and `PreparedOrdinaryWorkerUpload` types.

Wait within the maintenance readiness deadline when a plain-Worker maintenance response attests the previous deployment specification, including version-override requests.

Retry transient ordinary Worker provisioning failures up to three total backend attempts when provider reconciliation confirms that the upload, database or bucket creation, or deployment change did not take effect.
