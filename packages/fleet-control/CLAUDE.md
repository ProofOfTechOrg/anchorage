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
- `workers-for-platforms-backend.ts`, `wrangler-loop-backend.ts`: the two provisioning backends
- `cloudflare-client.ts`, `cloudflare-rate-coordinator.ts`: provider API and its shared quota fence
- `state-store.ts`, `migration-ledger.ts`, `export-store.ts`: durable fleet state
- `workers/`: the platform's own deployed Workers, published as separate export entries

```bash
pnpm fleet-control:check
pnpm test:packed-fleet-control
pnpm fleet-control:credentialed
```
