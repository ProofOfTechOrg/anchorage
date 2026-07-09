# src/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | Package barrel — re-exports the do-runner and approval-api surfaces (approval-ui and host-kit are subpath-only) | Finding the public export surface, adding a public symbol |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `do-runner/` | Cloudflare Durable Object workflow runner: `init()` import-swap, `RunnerRuntime` (with the `requestContextForRun` grant-minting seam + widened `status()`), `DurableObjectRunner`, D1 storage | Implementing or modifying durable execution, suspend/resume, or DO HTTP routing |
| `approval-api/` | Phase 3 approval queue: CAS store (D1 + in-memory), role-authorized service (SLA/escalation/audit), REST router, grant derivation | Implementing or modifying the approval queue or grant minting |
| `approval-ui/` | React approval dashboard (queue/detail/metrics) + DOM-free API client; compiles in its own tsc pass | Implementing or modifying the dashboard |
| `host-kit/` | Host-agnostic glue every host shares. `./host-kit` (barrel, deliberately **breakwater-free** so route-mounting consumers need no breakwater): the bearer auth seam (`parseActorTokens`, `bearerActorAuthenticator`), `createRunRouter` (the `/workflows` + `/runs` surface and its 401 → coarse `RUN_START_ROLES` → per-workflow `allowedRoles` gate order), `RunRouteError` + `doSummary` (the DO-response reader), `assertWorkflowsRegistered`, the approval bridge (`queueApprovalForSuspension`, `resumeRunWithRequeue`), and `WorkflowMeta`. `./host-kit/module` (separate subpath): the `WorkflowModule`/`WorkflowModuleContext` authoring contract, which carries breakwater's `AuditLogger`. Hosts inject only their resume topology (DO stub vs in-process) | Changing the shared run routes, auth seam, or the suspension→approval bridge |
| `audit-export/` | Cloudflare Queues audit export (Phase 4): `queueAuditSink` producer adapter + `createAuditQueueConsumer` batch→SIEM HTTP export (NDJSON, transform envelope, ack-on-2xx/retry-otherwise) | Implementing or modifying audit export to SIEMs |
| `artifacts/` | R2 workflow artifact storage (Phase 4): `R2ArtifactStore` over a structural `ArtifactBucket` seam (+ `InMemoryArtifactBucket`), keys `[prefix/]workflowId/runId/name` validated by the shared `PATH_SAFE_ID_PATTERN`, `deleteRun` pairs with retention purge | Implementing or modifying artifact storage |
