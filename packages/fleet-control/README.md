<!-- Content type: Reference -->

# Operate the isolated deployment fleet

`@proofoftech/fleet-control` is the trusted control-plane package for physically isolated deployments. It contains staged Wrangler Versions and Workers for Platforms backends, fenced durable fleet state, resumable provisioning and decommissioning, content-addressed external release promotion and schema-compatible rollback, authoritative bidirectional inventory, durable export sinks, and the shared platform Workers.

Read [Provision physically isolated deployments](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/fleet-control.md) for the supported lifecycle, security boundary, and credentialed conformance gate.

Version 0.1.0 is the first published release. Versions 0.0.1 through 0.0.4 in [`CHANGELOG.md`](CHANGELOG.md) are repository-internal history from before the package was published, and were never on the registry.

## Install

Install it only in the one service that owns provisioning. Read [Import it only from a trusted control plane](#import-it-only-from-a-trusted-control-plane) before adding the dependency anywhere else.

```bash
pnpm add @proofoftech/fleet-control
```

The package is ESM only and requires Node `>=22.22.0`. It depends on `@proofoftech/flowsafe`, which supplies the deployment identity protocol, the maintenance capability, and the audit export contract that fleet control provisions against.

That dependency is pinned to one exact FlowSafe release, deliberately. If you also depend on FlowSafe directly, pin it to the same release rather than letting a range resolve a second copy: FlowSafe's Durable Object classes are nominal and its maintenance receipt audience is fixed on both the minting and verifying side, so two copies fail closed at the maintenance boundary with no local signal.

Hosts that use `WranglerLoopBackend` must provide Wrangler `>=4.118 <5`. `WranglerCommandRunner` defaults to `['pnpm', 'exec', 'wrangler']`. Pass `wranglerCommand` to select an explicit executable or wrapper and any fixed arguments:

```typescript
const runner = new WranglerCommandRunner({
  apiToken,
  accountId,
  wranglerCommand: ['/opt/cloudflare/wrangler'],
});
```

Fleet Control does not install a runtime Wrangler dependency. Keep the selected command under the trusted host's control.

## Entry points

| Export | Contents |
| --- | --- |
| `@proofoftech/fleet-control` | Provisioning, migration, promotion, rollback, decommission, inventory, fleet state, and the Cloudflare client and rate coordinator. |
| `@proofoftech/fleet-control/workers/dispatch` | Platform dispatch Worker that routes to a deployment's user script under a verified maintenance capability. |
| `@proofoftech/fleet-control/workers/outbound` | Shared outbound Worker: the declared-egress proxy and the named `StateEgress` entrypoint. |
| `@proofoftech/fleet-control/workers/audit-consumer` | Control-plane queue consumer for backend-owned deployment audit events. |

The three Worker exports are deployment artifacts for the platform's own Workers, not helpers to import into an application Worker.

Construct `WorkersForPlatformsBackend` with a dispatch namespace, one named shared outbound Worker, and a state-egress root secret. All three values are mandatory. The constructor rejects an incomplete dispatch-native configuration before it can call a provider.

An ordinary state Worker can exist only as the finalized result of the dedicated plain-to-Workers-for-Platforms switch. Pass its narrow finalized-state provider back to provision, migration, and rollback operations. That provider exact-inspects and advances the retained bridge without allowing the normal backend to originate ordinary state resources.

The state-egress credential digest is immutable after a deployment adopts or creates trusted state. Rotating `FLEET_STATE_EGRESS_ROOT_SECRET` for an existing deployment requires a coordinated credential migration that updates trusted state and host routing together. Fleet control attests the derived digest and the exact secret-name inventory, but Cloudflare does not expose secret values for comparison.

Construct `CloudflareProvisioningClient` with a `CloudflareApiRateCoordinator`. Production replicas must construct `D1CloudflareApiRateCoordinator` with one shared direct Workers `D1Database` binding and an explicit, nonsecret `quotaScope` for the Cloudflare user or account-token quota. The coordinator calls only the binding's `prepare()` and `batch()` methods. Its runtime guard rejects objects without that interface, but JavaScript cannot prove whether a structurally compatible object is a direct binding or a remote facade. The trusted host must enforce the direct-binding requirement because remote coordination queries would consume the same Cloudflare Client API quota being coordinated. Every caller that shares the provider quota must share the binding and scope. The coordinator reserves at most 1,100 Anchorage-originated requests in each rolling five-minute window across replicas. Never derive the scope from, persist, or log the API token. A non-Worker control plane must call a separately deployed coordinator service instead of passing a remote database adapter. `ProcessLocalCloudflareApiRateCoordinator` is only for local tests and the single-process credentialed runner; it does not coordinate replicas.

Plain-worker, dispatch-worker, backend-switch, and control-worker inspection consumes every provider binding entry before exact attestation. Unknown types, malformed entries, duplicate names, unrepresented bindings, and missing complete inventories fail closed, including expected-empty groups. Secret names come from the authoritative secret-list API when ordinary version resources omit them. Wrangler-backed D1 ownership, migrations, exact-ID lookup, and deletion use Cloudflare's direct APIs. Every mutation runs under the active mutation fence. D1 deletion treats only provider 404 as absence and confirms that the immutable ID is absent without spawning `wrangler d1 delete`. A custom `PlainWorkerRouteApi` must provide `getDatabase` and `deleteDatabase` before destructive D1 teardown; Fleet Control fails closed when either capability is absent. SQLite recognizes anonymous `?` and numbered `?NNN` parameters, literals, quoted identifiers, and comments without string replacement. D1 does not support named SQLite parameters.

Use `forceDecommissionDeployment()` only when the host has lost the retained credentials or artifact required to rebuild a `DeploymentSpec`. The operation accepts the durable tenant and environment key instead of a specification. It runs under the deployment lease, removes every ordinary custom domain for the persisted script, disables and verifies public ingress, deletes the script’s current secrets, and deletes D1 by its persisted immutable ID after matching the persisted database name. It then removes the fleet ledger row.

Provider 404 responses converge as already absent, so retry the same call after an interrupted teardown. A `database-reserved` row has not authorized provider creation and can be removed without a provider call. A `database-create-authorized` row has an unresolved creation outcome and only a synthetic ID, so force decommission fails closed and retains that row for spec-aware recovery.

Pass `options.audit` to receive a `DecommissionAuditEvent`. Normal decommission emits `forced: false`; force decommission emits the same event with `forced: true`. Fleet Control emits the force event before it deletes the ledger row. If the sink fails, the terminal record remains and the next call retries audit delivery without repeating provider mutations.

The function never reads an artifact, computes a specification digest, deletes host-retained control-plane secrets, or deletes application R2 buckets. `WranglerLoopBackend` requires `PlainWorkerRouteApi.getDatabase` and `deleteDatabase`; it never falls back to Wrangler for force deletion. Other backends fail closed unless they implement the narrow `forceDecommissionStep` contract for equivalent provider primitives.

Run the package checks with:

```bash
pnpm fleet-control:check
```

The paid namespace gate uses [`scripts/credentialed-conformance.example.json`](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/fleet-control/scripts/credentialed-conformance.example.json) as its configuration shape. The configuration must declare `contractVersion: 1`, two trusted state profiles, the audit queue, positive CPU and subrequest limits, and allowed and denied upstream URLs. Structural validation checks the versioned configuration, required environment values, and private-key shape before artifact reads or fleet imports. The runner then builds both deployment specifications and trusted profiles. Production specification, secret, profile, migration, route, date, and canonical JSON Web Key (JWK) validators check both releases before the runner constructs a Cloudflare client or provisioning backend.

Supply separate bundles for the external candidate and both trusted state versions. Each routed candidate must implement the v1 action endpoint at `conformance.httpPath` and the WebSocket endpoint at `conformance.webSocketPath`. The action endpoint accepts JSON with `contractVersion: 1`, an `action`, and the fields listed in [Implement the artifact contract](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/fleet-control.md#implement-the-artifact-contract). Every JSON response repeats the exact version and action. The WebSocket endpoint accepts the same envelope as its first frame and echoes the nonce in its response frame.

The first trusted state profile owns the original FlowSafe Durable Object classes. The second profile repeats that migration history exactly, appends a migration for `conformance.newDurableObjectBinding`, and exports the new class. Both state artifacts must support `state-marker-put`, `state-marker-get`, `state-new-class`, and the state-egress actions through candidate bindings. External bindings must not select a script, namespace, or egress service.

The gate provisions two scratch deployments and validates the complete external resource group. It performs application variable and secret HMAC challenges, R2 write/read/delete/absence checks, audit ingress, HTTP and state-egress allow/deny probes, a WebSocket nonce echo, CPU limit and recovery checks, and a FlowSafe approval suspended across the v1 to v2 release update. It also proves that a nonempty R2 bucket blocks decommission before traffic or credentials change. Only the candidate can delete that fixture before the successful retry.

After both namespace deployments reach terminal decommission, the gate reuses the first released route for one platform-authored plain Worker driven by `WranglerCommandRunner` and `WranglerLoopBackend`. The host must provide Wrangler `>=4.118 <5`. The gate requires valid, nonempty version-ID observations before credential revocation, after revocation, and before deletion, while allowing provider-created versions and Wrangler's rolling list window. It then requires the terminal `decommissioned` phase and confirms that Cloudflare no longer resolves the exported database's immutable ID. Fleet Control checks exact persisted artifact membership before traffic removal, then uses a version-churn-tolerant live identity check before secret mutation and Worker deletion. If the assertion fails after the owned Worker's control-secret mutation begins, failure cleanup uses that same live teardown identity before deleting the exact uniquely named Worker and resuming normal artifact cleanup, so the regression cannot wedge the scratch account.

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

## Import it only from a trusted control plane

Account credentials, routing ownership, billing policy, and tenant lifecycle belong to the hosted control plane. Do not import this package into a data-plane Worker, and do not give a Worker that serves tenant requests a Cloudflare API token that reaches it.

The package is published, so the registry no longer enforces that boundary. Three things do:

- Fleet control is inert without an account-scoped Cloudflare API token. Every backend constructor rejects an incomplete trusted configuration before it can call a provider, so importing the package grants no capability on its own.
- Inside this repository, the `fleet-control-is-control-plane-only` rule in [`.dependency-cruiser.cjs`](https://github.com/ProofOfTechOrg/anchorage/blob/main/.dependency-cruiser.cjs) fails `pnpm architecture:check` if any other package under `packages/` reaches it.
- A consuming repository must enforce the same rule against its own tree. Confine the dependency declaration and every import to the one provisioning service, and fail the build when either appears anywhere else. Match subpath specifiers, not just the bare package name: the three `./workers/*` entry points are importable on their own.
