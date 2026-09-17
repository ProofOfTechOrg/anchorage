# fleet-control navigation

Control-plane only. Never import this package from a Worker that serves tenant
requests; the `fleet-control-is-control-plane-only` rule in
[`../../.dependency-cruiser.cjs`](../../.dependency-cruiser.cjs) enforces that
inside this repository, and a consuming repository must enforce it in its own
build.

Public behavior:

- [`README.md`](README.md)
- [`../../docs/fleet-control.md`](../../docs/fleet-control.md)

Source map:

- `provision.ts`, `decommission-advance.ts`, `backend-switch.ts`, `fleet.ts`: deployment lifecycle state machines (the Worker-safe bounded normal coordinator is isolated in `decommission-advance.ts`; the root-only bounded switch coordinator remains in `backend-switch.ts`)
- `workers-for-platforms-backend.ts`, `plain-worker-backend.ts`, `wrangler-loop-backend.ts`, `cloudflare-api-plain-worker-backend.ts`: provisioning backends (the Workers for Platforms backend, the shared ordinary-Worker core, and its Wrangler and direct-API adapters)
- `cloudflare-client.ts`, `cloudflare-ordinary-worker-operations.ts`, `cloudflare-provider-errors.ts`, `cloudflare-rate-coordinator.ts`: provider API (the client, the ordinary-Worker operations behind it, the SDK-error helpers, and `refuseRedirectStatus`, the redirect refusal the provider transports share with the tenant-Worker maintenance transport) and its shared quota fence
- `cloudflare-client-config.ts`, `strict-plain-data.ts`, `cloudflare-worker-attachment-scan-state.ts`, `cloudflare-worker-attachment-scan.ts`: shared SDK retry bounds, strict resumable state, and the request-bounded account-wide D1/R2 attachment scanner
- `decommission-intent.ts`: strict durable decommission shell and continuation-token codecs
- `decommission-database.ts`: provider-neutral bounded D1 reference, receipt, export-result, and deletion-settlement choreography
- `json-field-reads.ts`: JSON field readers shared by provider adapters and error sanitization
- `state-store.ts`, `migration-ledger.ts`, `d1-fleet-state-database.ts`, `database-export-store.ts`, `export-file-name.ts`, `export-store.ts`, `r2-export-store.ts`: durable fleet state (`d1-fleet-state-database.ts` adapts a Workers D1 binding to the state store's database port; `database-export-store.ts` declares `DurableDatabaseExportStore`, which `export-store.ts` implements over the filesystem and `r2-export-store.ts` over an R2 binding, and declares `cancelBodyWithoutAwait`, the response-body cancellation shared across the package's provider, backend and export-store modules; `export-file-name.ts` holds the portable-segment check both stores use)
- `fleet-operation-state.ts`, `d1-fleet-operation-store.ts`: bounded operation ports, codecs and D1 storage
- `fleet-audit-state.ts`, `fleet-audit-advance.ts`: bounded audit state and coordinator
- `fleet-inventory-state.ts`, `fleet-inventory-advance.ts`, `d1-fleet-inventory-run-store.ts`: inventory state, coordinator and generation storage
- `fleet-migration-state.ts`, `fleet-migration-advance.ts`: migration state and coordinator
- `workers/`: the platform's own deployed Workers, published as separate export entries
- `scripts/`: repository conformance tooling; `direct-credentialed-conformance.mjs` is the direct-API CLI entry and composes the `direct-credentialed-*` modules beside it

```bash
pnpm fleet-control:check
pnpm test:packed-fleet-control
pnpm fleet-control:credentialed
pnpm fleet-control:credentialed:direct
```
