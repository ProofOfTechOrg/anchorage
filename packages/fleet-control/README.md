<!-- Content type: Reference -->

# Operate the isolated deployment fleet

`anchorage-fleet-control` is the repository-private control-plane package for physically isolated deployments. It contains staged Wrangler Versions and Workers for Platforms backends, fenced durable fleet state, resumable provisioning and decommissioning, content-addressed external release promotion and schema-compatible rollback, authoritative bidirectional inventory, durable export sinks, and the shared platform Workers.

Read [Provision physically isolated deployments](../../docs/fleet-control.md) for the supported lifecycle, security boundary, and credentialed conformance gate.

Construct `WorkersForPlatformsBackend` with a dispatch namespace, one named shared outbound Worker, and a state-egress root secret. All three values are mandatory. The constructor rejects an incomplete dispatch-native configuration before it can call a provider.

An ordinary state Worker can exist only as the finalized result of the dedicated plain-to-Workers-for-Platforms switch. Pass its narrow finalized-state provider back to provision, migration, and rollback operations. That provider exact-inspects and advances the retained bridge without allowing the normal backend to originate ordinary state resources.

The state-egress credential digest is immutable after a deployment adopts or creates trusted state. Rotating `FLEET_STATE_EGRESS_ROOT_SECRET` for an existing deployment requires a coordinated credential migration that updates trusted state and host routing together. Fleet control attests the derived digest and the exact secret-name inventory, but Cloudflare does not expose secret values for comparison.

Construct `CloudflareProvisioningClient` with a `CloudflareApiRateCoordinator`. Production replicas must construct `D1CloudflareApiRateCoordinator` with one shared direct Workers `D1Database` binding and an explicit, nonsecret `quotaScope` for the Cloudflare user or account-token quota. The coordinator uses only the in-Worker D1 binding API; a REST-backed database adapter is rejected because its storage requests would consume the same Cloudflare Client API quota being coordinated. Every caller that shares the provider quota must share the binding and scope. The coordinator reserves at most 1,100 Anchorage-originated requests in each rolling five-minute window across replicas. Never derive the scope from, persist, or log the API token. A non-Worker control plane must call a separately deployed coordinator service instead of passing a remote database adapter. `ProcessLocalCloudflareApiRateCoordinator` is only for local tests and the single-process credentialed runner; it does not coordinate replicas.

Plain-worker, dispatch-worker, backend-switch, and control-worker inspection consumes every provider binding entry before exact attestation. Unknown types, malformed entries, duplicate names, unrepresented bindings, and missing complete inventories fail closed, including expected-empty groups. Secret names come from the authoritative secret-list API when ordinary version resources omit them. Wrangler-backed D1 ownership and migrations use Cloudflare's parameterized query and atomic batch APIs under the active mutation fence. SQLite recognizes anonymous `?` and numbered `?NNN` parameters, literals, quoted identifiers, and comments without string replacement. D1 does not support named SQLite parameters.

Run the package checks with:

```bash
pnpm fleet-control:check
```

The paid namespace gate uses [`scripts/credentialed-conformance.example.json`](scripts/credentialed-conformance.example.json) as its configuration shape. The configuration must declare `contractVersion: 1`, two trusted state profiles, the audit queue, positive CPU and subrequest limits, and allowed and denied upstream URLs. Structural validation checks the versioned configuration, required environment values, and private-key shape before artifact reads or fleet imports. The runner then builds both deployment specifications and trusted profiles. Production specification, secret, profile, migration, route, date, and canonical JSON Web Key (JWK) validators check both releases before the runner constructs a Cloudflare client or provisioning backend.

Supply separate bundles for the external candidate and both trusted state versions. Each routed candidate must implement the v1 action endpoint at `conformance.httpPath` and the WebSocket endpoint at `conformance.webSocketPath`. The action endpoint accepts JSON with `contractVersion: 1`, an `action`, and the fields listed in [Implement the artifact contract](../../docs/fleet-control.md#implement-the-artifact-contract). Every JSON response repeats the exact version and action. The WebSocket endpoint accepts the same envelope as its first frame and echoes the nonce in its response frame.

The first trusted state profile owns the original FlowSafe Durable Object classes. The second profile repeats that migration history exactly, appends a migration for `conformance.newDurableObjectBinding`, and exports the new class. Both state artifacts must support `state-marker-put`, `state-marker-get`, `state-new-class`, and the state-egress actions through candidate bindings. External bindings must not select a script, namespace, or egress service.

The gate provisions two scratch deployments and validates the complete external resource group. It performs application variable and secret HMAC challenges, R2 write/read/delete/absence checks, audit ingress, HTTP and state-egress allow/deny probes, a WebSocket nonce echo, CPU limit and recovery checks, and a FlowSafe approval suspended across the v1 to v2 release update. It also proves that a nonempty R2 bucket blocks decommission before traffic or credentials change. Only the candidate can delete that fixture before the successful retry.

Set `FLEET_CONFORMANCE_CONFIG`, `CLOUDFLARE_API_TOKEN`,
`CLOUDFLARE_ACCOUNT_ID`, and the fleet-private Ed25519 signing JWK in
`FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK`. Set
`FLEET_STATE_EGRESS_ROOT_SECRET` to the shared state-egress derivation secret
and `FLEET_CONFORMANCE_APPLICATION_SECRET` to disposable application-secret
plaintext for the fixed `applicationSecretBinding` in the configuration. Keep
both plaintext values out of the JSON configuration and logs. Keep only the
matching public JWK in the JSON configuration.

```bash
pnpm fleet-control:credentialed
```

The command exits before loading the Cloudflare client when any credential or
configuration path is absent. Run it only against a disposable account and
namespace: the final namespace script-count assertion intentionally requires
that scratch namespace to be empty after cleanup. The token must expose its
policy through API Tokens Read and grant Zone Read, Workers Routes Read, and
Workers Routes Write across every zone in the selected account. Fleet control
discovers that complete account-filtered zone set and rejects partial token
scope instead of accepting a configured zone list.

The package is private because account credentials, routing ownership, billing policy, and tenant lifecycle belong to the hosted control plane. Do not import it into a data-plane Worker.
