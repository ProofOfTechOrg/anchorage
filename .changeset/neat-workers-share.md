---
'@proofoftech/fleet-control': minor
---

Export the shared `PlainWorkerBackend`, its `PlainWorkerProvisioningApi` port, and the port's ordinary-Worker record types. Port adapters must verify database exports independently against the durable store's committed size and digest. `WranglerLoopBackend` now extends this core while retaining the same constructor options and provisioning members.

Harden ordinary-Worker provisioning and teardown:

- **BEHAVIOR CHANGE:** Surface a lost external mutation lease as a failure instead of masking it behind a post-dispatch readback.
- Preserve both operation and scratch-cleanup failures without masking either.
- **BEHAVIOR CHANGE:** Refuse D1 bindings and database inventory entries with an empty primary identifier instead of accepting a fallback field or malformed inventory.
- **BEHAVIOR CHANGE:** Reject malformed Worker version inventory that omits an identifier instead of treating the entry as provider absence.
- **BEHAVIOR CHANGE:** Keep lease denials distinct from provider absence during Worker deletion.
- Allocate adapter-owned upload scratch only when an upload is required.
- **BEHAVIOR CHANGE:** Classify post-install scratch-cleanup failures so callers remove only Workers created by the failed attempt.

Provider-neutral error messages now describe plain-Worker and provider operations instead of Wrangler. Error-message text compatibility is not claimed by this release.

When upgrading, consumers that matched `deployWorker` rejections by identity or message text should catch `WorkerDeploymentError` and read `createdByAttempt` and `resourceState`.
