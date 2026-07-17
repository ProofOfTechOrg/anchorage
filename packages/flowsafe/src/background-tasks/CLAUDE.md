# background-tasks/

Track B (M-003) — Mastra background tasks on the DO + D1 chokepoint. Subpath-only
(`@proofoftech/flowsafe/background-tasks`), like agent-runner: host-side wiring a
consumer opts into, not in the root barrel.

**Substrate limitations + the P9 unblock contract (R-B1/R-B2/R-B3, pinned
against @mastra/core 1.50.0 + @mastra/cloudflare-d1 1.1.1; full detail in
`host.ts`):** durable background-task EXECUTION does not run on D1. Core runs task
bodies on the *evented* engine, which refuses to `createRun` unless the workflows
store reports `supportsConcurrentUpdates()`.

- **R-B1** — cloudflare-d1 returns `false` AND leaves
  `updateWorkflowResults`/`updateWorkflowState` as unimplemented throws ("D1 does
  not support atomic read-modify-write"). So R-B1 is NOT a flag to flip:
  overriding `supportsConcurrentUpdates` passes core's gate then THROWS on the
  first step-update, stranding the task at `running`. The P9 fix is an adapter
  that *implements* atomic partial-updates — the DO's single-threaded lease makes
  that implementation safe, not an override.
- **R-B2** — the evented engine also needs the workers `mastra.startWorkers()`
  stands up, which the bare manager does not. R-B1 and R-B2 close TOGETHER.
- **R-B3** (latent tenant-isolation residual, inert only while R-B1 blocks
  execution) — core keys the internal `__background-task` run by the UNSALTED
  `taskId` (`createRun({ runId: taskId })`), so that engine run's
  `mastra_workflow_snapshot` row escapes purgeTenant's salted `[tid_, tid\x60)`
  `run_id` range and (while suspended) `purgeExpiredWorkflowRuns`. The task's own
  `mastra_background_tasks` row is covered; the internal run it spawns is not.
  Unblocking execution MUST close R-B3 in the same change (salt the internal
  runId, or reap by the salted originating `run_id`) — a CI guard in
  `d1-storage.test.ts` fails the instant `supportsConcurrentUpdates()` returns
  true.
- **L3** — `recoverStaleTasks` is tenant-blind today (lists ALL running,
  re-dispatches ALL pending, no tenant filter); a multi-tenant manager needs a
  tenant filter or a host-topology revisit before execution is enabled.

PERSISTENCE, the recovery seam, tenant purge + TTL, the read routes, and the
`_background` defense all work regardless. This falsifies the program plan's
"background tasks execute AS default-engine workflow runs" premise — they use the
evented engine.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `d1-storage.ts` | `backgroundTasksStore(mastra)` — the async accessor onto @mastra/cloudflare-d1's `BackgroundTasksStorageD1` (the D1 domain the ADAPTER already ships over `mastra_background_tasks`; NOT reimplemented — "what NOT to build: custom state store"). Re-exports the tenant-range purge coverage + `purgeExpiredBackgroundTasks` / `BACKGROUND_TASK_TTL_PURGE_TABLES` from do-runner (coupled there to the schema guard) | Reaching the D1 background-tasks domain; the TTL purge |
| `host.ts` | `BackgroundTaskHost` — hosts a `BackgroundTaskManager` on a DO. `boot()` re-registers static executors then calls the PUBLIC `manager.init(pubsub)` (which fires the manager's own private `recoverStaleTasks` internally — the R-002 seam, NO private call), and warns once when the store cannot execute bodies (R-B1). `onAlarm()` boots + runs the manager TTL cleanup. Survives DO eviction by construction (DL-015) | Hosting the manager on a DO, the recovery seam |
| `routes.ts` | `createBackgroundTaskRoutes({ manager })` — READ-only, tenant-bound by construction (DL-014): list/stream REQUIRE a runId/threadId filter and validate its salted prefix; `getTask` 404s a missing OR foreign task with the SAME response (no oracle); the raw manager is never exposed. `(request, tenantId) => Response \| null`; the DO passes its own asserted tenant | The tenant-bound read surface |
| `index.ts` | Subpackage barrel | The export surface |
| `d1-storage`/`host`/`routes` `.test.ts` | The `backgroundTasksStore` fail-closed accessor (no storage / no `backgroundTasks` domain) + the **R-B3 execution-unblock enforcement guard** (fails the instant `supportsConcurrentUpdates()` returns true); the recovery seam via init on both InMemoryStore and REAL D1 (maxRetries=0 → failed AND maxRetries>0 → re-queued pending), the R-B1 warn + D1 persistence round-trip; the tenant-binding 400/404-no-oracle matrix + list/stream per-row scope-guard parity. Full dispatch→complete is NOT unit-proven (R-B2 needs the evented event loop); B-S3 + B-S2 are workerd-proven in `scripts/spike-verify.mjs` | Adding tests |

## The `_background` defense (breakwater, not here)

DL-005's model-override defense lives in breakwater: `createConnector` rejects a
`_background` arg unless `permissions.background` opts in (read-only tools only),
and the `backgroundExecution` tool-policy evaluator denies the write class. It is
DEFENSE-IN-DEPTH for direct / nested programmatic calls: on the AGENT path core
deletes `_background` from the args before dispatch AND its own
`resolveBackgroundConfig` baseEnabled gate already keeps the model from
backgrounding a connector the developer never made eligible (a breakwater
connector sets no background config), so the breakwater reads see stripped args
and catch nothing there. The real write boundary on every path — including inside
the background executor — is the requestContext grant. See
`packages/breakwater/src/connector-sdk/` and `policy-engine/tool-policy.ts`.
