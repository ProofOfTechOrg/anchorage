# do-runner/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `init.ts` | `init(env)` import-swap — builds D1-backed Mastra storage from the `DB` binding (or an injected `storage`), returns swapped `createWorkflow`/`createStep`/`runtime` factories, threads `requestContextForRun`; `DORunnerEnv`/`InitOptions`/`InitResult`/`InitSource` | Wiring workflow definitions into the runner, changing how storage and factories are built |
| `runtime.ts` | `RunnerRuntime` — register/start/resume/status/workflowIds, per-run FIFO lock, `PATH_SAFE_ID_PATTERN` validation (runId at `start()`, workflowId at `register()`), error taxonomy, the `requestContextForRun` provider seam (`RunLeg` with resumed step + `suspendedAt`; every leg mints the breakwater workflow-scope key, provider values merge over it), widened `status()` projection incl. `RunSummary.suspendedAt` (per-step suspension timestamps); rationale lives in the module's doc comments | Changing run lifecycle, id validation, locking, error mapping, or the requestContext seam |
| `durable-object.ts` | `DurableObjectRunner` abstract DO — `fetch()` HTTP routing for `POST /runs`, `GET /runs/:workflowId/:runId`, `POST .../resume`, and JSON helpers | Adding or changing DO HTTP routes, request parsing, or error responses |
| `d1-storage.ts` | `createD1Storage()` — D1-backed Mastra storage wrapper (`D1StorageOptions`) — and `purgeExpiredWorkflowRuns()` — terminal-status-only TTL purge of `mastra_workflow_snapshot` (structural `SnapshotDatabase`; scheduling stays with the caller) | Changing the D1 storage adapter, its options, or retention purge semantics |
| `breakwater-keys.ts` | The three `BREAKWATER_*` requestContext key literals (approved-connectors, actor, workflow-scope), mirrored by value — the do-runner-owned leaf `approval-api/contract.ts` re-exports; literal equality with breakwater enforced by the e2e tripwires | Changing a cross-package requestContext key |
| `index.ts` | Subpackage barrel — public do-runner API and types | Finding the do-runner export surface |
| `runtime.test.ts` | `RunnerRuntime` lifecycle / lock / validation / scope-minting tests | Adding runtime tests, debugging lifecycle behavior |
| `durable-object.test.ts` | `DurableObjectRunner` HTTP-routing + suspend/resume tests | Adding DO tests, debugging routing or resume |
| `d1-storage.test.ts` | `purgeExpiredWorkflowRuns` against real SQLite: terminal-vs-live statuses, TTL boundary, table prefix | Adding retention tests, debugging the purge SQL |
