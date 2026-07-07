# packages/

Implementation packages — everything shippable lives here.

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `breakwater/` | `@proofoftech/breakwater` — safety middleware (policy engine, RBAC + audit, connector SDK) shipping as Mastra processors and tool/workflow wrappers | Implementing or modifying policy, RBAC, audit, or connector enforcement |
| `flowsafe/` | `@proofoftech/flowsafe` — approval UX + Cloudflare-native durable execution: DO workflow runner, approval queue API (CAS store, SLA, grant minting), styling-agnostic React dashboard (slot components + headless hook) + a runnable Vite app (`app/`) that injects an Astryx adapter | Implementing or modifying the DO runner or approval surface |
