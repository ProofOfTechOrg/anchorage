# fleet-control navigation

Control-plane only. Never import this package from a Worker that serves tenant
requests; the `fleet-control-is-control-plane-only` rule in
[`../../.dependency-cruiser.cjs`](../../.dependency-cruiser.cjs) enforces that
inside this repository, and a consuming repository must enforce it in its own
build.

Public behavior:

- [`README.md`](README.md)
- [`../../docs/fleet-control.md`](../../docs/fleet-control.md)

Entry points:

- `src/index.ts`: the package root export; `src/cloudflare-control-plane.ts`: the `./cloudflare-control-plane` export
- `src/workers/`: the platform's own deployed Workers, published as separate export entries
- `scripts/`: repository conformance tooling, not published; `credentialed-conformance.mjs` and `direct-credentialed-conformance.mjs` are the CLIs behind `pnpm fleet-control:credentialed` and `pnpm fleet-control:credentialed:direct`, and `direct-credentialed-purge.mjs` is the operator purge script `docs/fleet-control.md` documents
- `test/`: package suites; `test/fixtures/` holds the fakes and harnesses they share

The direct conformance harness (`scripts/direct-credentialed-*`, `scripts/direct-reference-*`, `test/direct-*`, `test/fixtures/direct-*` and `vitest.direct-scenario*.config.ts`) is frozen: change it only to fix a live-run failure or a CI break.

```bash
pnpm fleet-control:check
pnpm test:packed-fleet-control
pnpm fleet-control:credentialed
pnpm fleet-control:credentialed:direct
```
