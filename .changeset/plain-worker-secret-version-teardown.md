---
"@proofoftech/fleet-control": patch
---

Allow plain-Worker decommission to finish when Cloudflare secret deletion creates new Worker versions.

Fleet Control now treats exact persisted artifact membership as the pre-mutation teardown gate during traffic removal. Later teardown steps accept provider-created version IDs, resolve the persisted artifact directly even after it leaves Wrangler's ten-entry list output, and validate every deployed version's tenant, environment, database, specification, schema, and ingress identity. Worker deletion retains resource-footprint checks and verifies full absence.
