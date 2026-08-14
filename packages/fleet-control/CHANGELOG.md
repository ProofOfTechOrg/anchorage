# @proofoftech/fleet-control

## 0.3.0

### Minor Changes

- 1f6a13a: Add a fenced, replay-safe `forceDecommissionDeployment()` escape hatch for ordinary deployments whose retained specification inputs are unavailable.

  The operation always runs under `withDeploymentLease()` and persists the normal teardown phases. It removes control secrets, disables and verifies public ingress, detaches the custom domain, and deletes the exact persisted D1 database ID after asserting its fleet-owned name. Provider `404` responses converge as already absent, so retries resume after any completed mutation. Success deletes the fleet ledger row and emits the normal audit surface with `forced: true`.

  Force decommission never fetches an artifact, rebuilds a specification digest, or enters the `FLEET_SPEC_DIGEST` or version-ownership attestation path. It does not delete the ordinary Worker script, application R2 buckets, or control-plane retention data. After success, the host remains responsible for deleting its separate retention row and revoking its gateway key.

  The built-in `WranglerLoopBackend` implements the required spec-free route-API operations. Custom ordinary-Worker backends must implement `forceDecommissionStep()` with equivalent fenced checks. Workers for Platforms deployments fail closed because their dispatch and trusted-resource topology cannot use the ordinary-Worker route API. A `database-create-authorized` record also fails closed because its durable row cannot prove the exact database ID after a lost create response.

### Patch Changes

- Updated dependencies [1f6a13a]
  - @proofoftech/flowsafe@0.16.0

## 0.2.2

### Patch Changes

- 3df645d: Delete plain-worker D1 databases by immutable ID through Cloudflare's REST API instead of passing the UUID to `wrangler d1 delete`. Teardown now accepts provider 404 as an already-absent database, confirms exact-ID absence, and fails closed when a custom `PlainWorkerRouteApi` does not provide the required `getDatabase` and `deleteDatabase` capabilities.

## 0.2.1

### Patch Changes

- bec0029: Allow plain-Worker decommission to finish when Cloudflare secret deletion creates new Worker versions.

  Fleet Control now treats exact persisted artifact membership as the pre-mutation teardown gate during traffic removal. Later teardown steps accept provider-created version IDs, resolve the persisted artifact directly even after it leaves Wrangler's ten-entry list output, and validate every deployed version's tenant, environment, database, specification, schema, and ingress identity. Worker deletion retains resource-footprint checks and verifies full absence.

## 0.2.0

### Minor Changes

- 34e8ae0: Require host-provided Wrangler `>=4.118 <5` without installing it as a Flowsafe peer. Hosts that use `flowsafe-provision` must now install a compatible Wrangler version directly.

  Fleet Control now supports an explicit Wrangler command, creates D1 databases through Wrangler 4's current output contract, and revokes plain-Worker credentials through the Workers API without creating untracked Worker versions.

  Custom `PlainWorkerRouteApi` implementations must add `deleteControlSecrets(scriptName, secretNames, fence)`. Implement it with the Workers script-secret DELETE API, treat an HTTP 404 as already deleted, and retain a final authoritative secret-list check.

### Patch Changes

- Updated dependencies [2c097d8]
- Updated dependencies [34e8ae0]
  - @proofoftech/flowsafe@0.15.0

## 0.1.0

### Minor Changes

- fa12c05: Publish fleet control to npm as `@proofoftech/fleet-control`. Version 0.1.0 is the first release on the registry; 0.0.1 through 0.0.4 were repository-internal and were never published under the previous unscoped `anchorage-fleet-control` name.

  Fleet control remains control-plane software. It holds account credentials, routing ownership, billing policy, and tenant lifecycle, so a data-plane Worker must never import it. The registry no longer enforces that boundary, so enforce it in your build: confine the dependency declaration and every import to one provisioning service, and match subpath specifiers as well as the bare package name, because the three `./workers/*` entry points are importable on their own. Inside this repository the `fleet-control-is-control-plane-only` architecture rule fails the build when any other package under `packages/` reaches it.

### Patch Changes

- Updated dependencies [fa12c05]
  - @proofoftech/flowsafe@0.14.0

## 0.0.4

### Patch Changes

- Updated dependencies [352b38c]
  - @proofoftech/flowsafe@0.13.1

## 0.0.3

### Patch Changes

- Made provider-binding attestation consume every binding entry across plain, dispatch, backend-switch, and control Workers, and made durable Cloudflare API quota coordination use the Workers D1 binding interface. Production hosts must pass a direct binding so coordination traffic cannot consume the quota it protects; runtime structural validation cannot prove adapter provenance.

- Fixed expected-empty plain-worker binding attestation, replaced Wrangler SQL interpolation with fenced provider-native D1 parameters and batches, made fleet schema initialization retryable and concurrent-upgrade safe, and added durable cross-replica Cloudflare API quota coordination through an explicit nonsecret scope.

- Updated dependencies [4f0fc9d]
  - @proofoftech/flowsafe@0.13.0

## 0.0.2

### Patch Changes

- Updated dependencies [3276c2a]
- Updated dependencies [b3b4b55]
  - @proofoftech/flowsafe@0.12.0
