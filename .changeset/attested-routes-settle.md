---
'@proofoftech/fleet-control': minor
---

Fleet Control now attests the release that is actually serving traffic after every package-owned promotion and supports lease-held, idempotent settlement callbacks.

This changes the public control-plane contract:

- **BREAKING:** `ProvisioningBackend.attestActiveRoute()` is required. Public `PlainWorkerRouteApi` implementations must also provide `inspectActiveWorkerRoute()`.
- **BREAKING:** the fourth argument to `ProvisioningBackend.seedDeploymentIdentity()` is now `SeedDeploymentIdentityOptions`, shaped as `{ initialExecutionFenceState }`. Provisioning exports now include `InitialExecutionFenceState`.
- **BREAKING:** `provisionDeployment()` requires `initialExecutionFenceState`, is asynchronous even when entry validation fails, and accepts `routeAttestation?`. Its ready record uses the attested routed `artifactVersion`, which can differ from the inspected candidate value recorded by earlier releases.
- **BEHAVIOR CHANGE:** `provisionDeployment()`, every `migrateFleet()` promotion branch, and `rollbackExternalRelease()` attest after promotion and fail closed when routing is absent, split across two versions, or does not match the expected release. Earlier releases could complete from desired-state inspection alone.
- Active-route exports now include `attestFleetRecordActiveRoute`, `attestConvergedActiveRoute`, `ActiveRouteAttestation`, `ActiveRouteAttestationError`, `ActiveRouteExpectation`, `AttestConvergedActiveRouteOptions`, and `ObservedActiveRoute`.
- Settlement exports now include `fleetSettlementKey`, `FleetSettlementContext`, `FleetSettlementEntry`, and `FleetSettlementHost`. `migrateFleet()` accepts `settlementFor?` and `routeAttestation?`; `rollbackExternalRelease()` accepts `settlement?` and `routeAttestation?`.
- Settlement callbacks run at least once under the deployment lease and are keyed by `settlementKey`. Hosts must deduplicate external effects by that key. The package records `FleetRecord.settledSettlementKey` so routine convergence skips an already recorded settlement while still attesting the route. Existing fleet databases add the nullable column automatically on first open.
- Both backend constructors accept `clock?` for attestation timestamps.
- The runtime dependency on `@proofoftech/flowsafe` is published as the exact matching release. A host pinned to an older Flowsafe must upgrade or its package manager can install a second copy, which the deployment boundary does not support.

Implement the new backend methods before upgrading. Treat route ambiguity as a refusal, cache host-facing attestations instead of reading them per request, pass an explicit initial execution-fence state to provisioning and seeding, and make every settlement effect idempotent on `settlementKey`.
