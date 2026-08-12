---
'@proofoftech/fleet-control': minor
---

Publish fleet control to npm as `@proofoftech/fleet-control`. Version 0.1.0 is the first release on the registry; 0.0.1 through 0.0.4 were repository-internal and were never published under the previous unscoped `anchorage-fleet-control` name.

Fleet control remains control-plane software. It holds account credentials, routing ownership, billing policy, and tenant lifecycle, so a data-plane Worker must never import it. The registry no longer enforces that boundary, so enforce it in your build: confine the dependency declaration and every import to one provisioning service, and match subpath specifiers as well as the bare package name, because the three `./workers/*` entry points are importable on their own. Inside this repository the `fleet-control-is-control-plane-only` architecture rule fails the build when any other package under `packages/` reaches it.
