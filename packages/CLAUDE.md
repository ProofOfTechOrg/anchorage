# packages/

Implementation packages — everything shippable lives here. Two libraries and
one product: `breakwater` + `flowsafe` are the published safety layer;
`showcase` is the private demo app built on them.

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `breakwater/` | `@proofoftech/breakwater` — safety middleware (policy engine, RBAC + audit, connector SDK) shipping as Mastra processors and tool/workflow wrappers | Implementing or modifying policy, RBAC, audit, or connector enforcement |
| `flowsafe/` | `@proofoftech/flowsafe` — approval UX + Cloudflare-native durable execution: DO workflow runner, approval queue API (CAS store, SLA, grant minting), styling-agnostic React dashboard (slot components + headless hook), the workerd spike (`spike/`), and the copy-ready deploy template (`deploy/`) | Implementing or modifying the DO runner or approval surface |
| `showcase/` | `showcase` (private) — the Anchorage demo product: Vite React SPA at the package root + the Cloudflare host in `worker/` (five workflows, demo auth, sandbox reset), single-deploy to anchorage.proofoftech.org. Mandatory absolute imports (`@/*`, `#worker/*`, `@flowsafe/*`), react-doctor-gated | Changing the demo app, its worker, or a showcased workflow |
