---
'@proofoftech/fleet-control': minor
---

`PlainWorkerProvisioningApi.listDatabases` accepts an optional name filter. The direct Cloudflare API adapter forwards it as the D1 list query and the Wrangler adapter filters its parsed inventory locally; `PlainWorkerBackend.findDatabase` passes the deployment's database name and keeps its exact-name comparison and its duplicate-name and missing-UUID refusals.
