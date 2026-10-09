---
'@proofoftech/fleet-control': minor
'@proofoftech/flowsafe': minor
---

Add `createCloudflareWorkersForPlatformsControlPlane` to the Worker-safe control-plane entry for external artifacts with dispatch-native trusted state. It composes native D1 fleet/quota stores, R2 export receipts, lifecycle continuations, and exact-spec release rollback without exposing provider or storage capabilities.

Inventory continuations bind the configured namespace and routing KV, and deployment leases check persisted platform ownership. Use one Fleet database per immutable platform configuration. Provisioning and rollback retain their single-deployment execution model; ordinary-state adoption and platform catalogs remain on the root APIs.

Expose FlowSafe's existing maintenance capability and receipt implementation through `@proofoftech/flowsafe/host-kit/maintenance-capability`, allowing control-plane bundles to import it without the host composition graph.

Accept Cloudflare’s omitted namespace trust flag as its default untrusted mode during provisioning and inventory. Trusted and malformed trust values remain refused.

Handle Cloudflare’s `script` response field when verifying audit queue consumers, avoiding false convergence failures.

Allow Cloudflare’s empty prebuilt-pipeline metadata when attesting the shared dispatcher’s outbound binding.

Preserve referenced Wasm sidecars in WFP conformance artifacts, verify their digests before upload, and run the exact upload modules in the local verifier.

Select trusted-state Durable Object migrations from the owned live Worker, preserving initial class creation and retries after provider commits.

Match remote Durable Object bindings to owned namespace IDs when Cloudflare omits dispatch-namespace metadata, while rejecting conflicting targets. Allow external deployments with reserved application R2 buckets to converge.

Relay conformance candidate maintenance requests to trusted state and verify signed requests, receipts, and replay refusal against the built artifacts locally.

Project trusted state ownership into dispatch inventory. Remove WFP traffic using complete persisted policy and state-egress authority, retaining migration evidence through bounded teardown and using durable bridge identity during switch rollback.

Preserve published deployments when the final ready-state write fails or its response is lost, keeping retry and export-backed decommission available without destructive provisioning rollback.

Export `StateEgress` as a named HTTP handler so trusted-state service bindings resolve it in the Workers runtime. Direct JavaScript callers must replace `new StateEgress(context, env).fetch(request)` with `StateEgress.fetch(request, env)`. The service-binding entrypoint name and HTTP contract remain unchanged.
