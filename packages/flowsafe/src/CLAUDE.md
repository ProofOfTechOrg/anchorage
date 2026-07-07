# src/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | Package barrel — re-exports the do-runner and approval-api surfaces (approval-ui is subpath-only, so root consumers never pull React) | Finding the public export surface, adding a public symbol |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `do-runner/` | Cloudflare Durable Object workflow runner: `init()` import-swap, `RunnerRuntime` (with the `requestContextForRun` grant-minting seam + widened `status()`), `DurableObjectRunner`, D1 storage | Implementing or modifying durable execution, suspend/resume, or DO HTTP routing |
| `approval-api/` | Phase 3 approval queue: CAS store (D1 + in-memory), role-authorized service (SLA/escalation/audit), REST router, grant derivation | Implementing or modifying the approval queue or grant minting |
| `approval-ui/` | React approval dashboard (queue/detail/metrics) + DOM-free API client; compiles in its own tsc pass | Implementing or modifying the dashboard |
| `audit-export/` | Cloudflare Queues audit export (Phase 4): `queueAuditSink` producer adapter + `createAuditQueueConsumer` batch→SIEM HTTP export (NDJSON, transform envelope, ack-on-2xx/retry-otherwise) | Implementing or modifying audit export to SIEMs |
| `artifacts/` | R2 workflow artifact storage (Phase 4): `R2ArtifactStore` over a structural `ArtifactBucket` seam (+ `InMemoryArtifactBucket`), keys `[prefix/]workflowId/runId/name` validated by the shared `PATH_SAFE_ID_PATTERN`, `deleteRun` pairs with retention purge | Implementing or modifying artifact storage |
