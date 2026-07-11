# @proofoftech/flowsafe

## 0.1.0 (unreleased)

First publishable cut. Approval UX + Cloudflare-native durable execution for
Mastra workflows: Durable Object runner (`init()` import-swap, server-minted
tenant-prefixed run ids, durable resume ledger), approval queue API (CAS-guarded
D1/in-memory stores, tenant-bound factories, separation-of-duties service,
SLA sweep, derivation-based grant minting), styling-agnostic React approval
dashboard (headless hook + slot components, optional react peer), host-kit
(run router, tenant resolver, bearer auth seam, approval bridge), Cloudflare
Queues audit export, R2 artifact store, and a copy-ready production Worker
template in `deploy/`.

`@proofoftech/breakwater` is an optional peer: only the `./host-kit/module`
subpath references its types. Install it when wiring `WorkflowModule` audit
contexts; every other subpath works without it.

Requires `@mastra/core` ^1.50.0 (peer), Node >= 22, ESM only
(`moduleResolution` `node16`/`nodenext`/`bundler`). React 18 or 19 only for
`./approval-ui`.
