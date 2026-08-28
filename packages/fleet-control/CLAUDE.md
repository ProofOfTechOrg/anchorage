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

- `provision.ts`, `backend-switch.ts`, `fleet.ts`: deployment lifecycle state machines
- `workers-for-platforms-backend.ts`, `plain-worker-backend.ts`, `wrangler-loop-backend.ts`, `cloudflare-api-plain-worker-backend.ts`: provisioning backends (the Workers for Platforms backend, the shared ordinary-Worker core, and its Wrangler and direct-API adapters)
- `cloudflare-client.ts`, `cloudflare-ordinary-worker-operations.ts`, `cloudflare-provider-errors.ts`, `cloudflare-rate-coordinator.ts`: provider API (the client, the ordinary-Worker operations behind it, the SDK-error helpers) and its shared quota fence
- `state-store.ts`, `migration-ledger.ts`, `d1-fleet-state-database.ts`, `export-store.ts`: durable fleet state (`d1-fleet-state-database.ts` adapts a Workers D1 binding to the state store's database port)
- `workers/`: the platform's own deployed Workers, published as separate export entries

```bash
pnpm fleet-control:check
pnpm test:packed-fleet-control
pnpm fleet-control:credentialed
```
