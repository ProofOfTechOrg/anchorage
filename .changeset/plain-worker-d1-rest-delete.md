---
'@proofoftech/fleet-control': patch
---

Delete plain-worker D1 databases by immutable ID through Cloudflare's REST API instead of passing the UUID to `wrangler d1 delete`. Teardown now accepts provider 404 as an already-absent database, confirms exact-ID absence, and fails closed when a custom `PlainWorkerRouteApi` does not provide the required `getDatabase` and `deleteDatabase` capabilities.
