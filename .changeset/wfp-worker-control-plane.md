---
'@proofoftech/fleet-control': minor
'@proofoftech/flowsafe': minor
---

Add `createCloudflareWorkersForPlatformsControlPlane` to the Worker-safe control-plane entry for external artifacts with dispatch-native trusted state. It composes native D1 fleet/quota stores, R2 export receipts, lifecycle continuations, and exact-spec release rollback without exposing provider or storage capabilities.

Inventory continuations bind the configured namespace and routing KV, and deployment leases check persisted platform ownership. Use one Fleet database per immutable platform configuration. Provisioning and rollback retain their single-deployment execution model; ordinary-state adoption and platform catalogs remain on the root APIs.

Expose FlowSafe's existing maintenance capability and receipt implementation through `@proofoftech/flowsafe/host-kit/maintenance-capability`, allowing control-plane bundles to import it without the host composition graph.

Accept Cloudflare’s omitted namespace trust flag as its default untrusted mode during provisioning and inventory. Trusted and malformed trust values remain refused.

Handle Cloudflare’s `script` response field when verifying audit queue consumers, avoiding false convergence failures.
