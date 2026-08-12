# @proofoftech/fleet-control

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
