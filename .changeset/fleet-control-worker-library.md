---
'@proofoftech/fleet-control': minor
---

Expose `@proofoftech/fleet-control/cloudflare-control-plane` for a dedicated trusted Cloudflare Worker. The factory composes direct Cloudflare operations with durable Fleet D1 state, shared D1 quota coordination, and private R2 database exports. Continuation tokens resume bounded lifecycle operations against stored authority.

Install the required `@cloudflare/workers-types >=5.20260730.1 <6` peer when typechecking consumers. Keep the Cloudflare token and control-plane bindings outside tenant-serving Workers, and authenticate and authorize operations before calling the library.

The packed verification uses an unminified namespace-import Worker with raw and gzip regression budgets of 4,128,768 and 589,824 bytes. Those budgets apply 25% headroom to the initial packed measurements, rounded up to 64 KiB. They are repository regression limits; size inventory and audit workloads for the documented Worker resource envelope.
