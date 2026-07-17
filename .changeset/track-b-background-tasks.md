---
'@proofoftech/flowsafe': minor
'@proofoftech/breakwater': minor
---

Track B (background tasks): the additive, opt-in substrate + defenses for
Mastra background tasks on the one Durable-Object + D1 chokepoint. No existing
signature or the `ApprovalRecord` shape changed; hosts stay byte-identical with
background tasks unconfigured.

- **breakwater `_background` model-override defense (DL-005), the ONE breakwater
  change (MINOR).** `createConnector`'s wrapped `execute`/`dryRunExecute` reject
  tool-call args carrying a `_background` field (core `LLMBackgroundOverride`)
  unless the manifest opts in via `permissions.background` — the argv-flag-
  smuggling posture of the agent-cli `buildFlags` defense. `background: true` is
  allowed only on a read-only connector (a write-class opt-in throws at
  construction); v1 keeps write/approval-carrying connectors foreground-only.
  Plus a `backgroundExecution` tool-policy evaluator (deny-by-default for the
  write class) as the defense-in-depth counterpart at the gate loop. Both are
  DEFENSE-IN-DEPTH for DIRECT / NESTED calls, NOT the agent-path guard: on the
  agent path core deletes `_background` from the args before dispatch (schema or
  not), and core's own `resolveBackgroundConfig` baseEnabled gate — a breakwater
  connector sets no background config — already prevents the model from
  backgrounding an ineligible tool, so the breakwater reads see stripped args and
  fire on nothing there. The real write boundary on every path (including inside
  the background executor) is the requestContext grant.
- **`mastra_background_tasks` adopted into the D1 substrate in ONE change
  (DL-003).** Registered in the schema-guard inventory (coverage `tenant-range`,
  a new `background-task-ttl` retention kind), in `purgeTenant` (ranged over the
  INV-1 salted `run_id`; new `PurgeTenantResult.backgroundTasks`), and given a
  storage-layer TTL cleanup `purgeExpiredBackgroundTasks` (+
  `BACKGROUND_TASK_TTL_PURGE_TABLES`) mirroring core's two-window
  `BackgroundTaskManager.cleanup` so a purge cron reaps terminal rows without a
  live manager. Surfaced through `FlowsafeWorkerConfig.backgroundTasks` as the
  purge cron's own failure-isolated duty (undefined = no duty, byte-identical).
- **`@proofoftech/flowsafe/background-tasks` (new subpath):** `backgroundTasksStore`
  (the async accessor onto @mastra/cloudflare-d1's `BackgroundTasksStorageD1` —
  the D1 domain the adapter already ships; not reimplemented, per "what NOT to
  build"), `BackgroundTaskHost` (hosts a `BackgroundTaskManager` on a DO with the
  DL-015 boot/alarm lifecycle), and `createBackgroundTaskRoutes` (READ-only,
  tenant-bound by construction, DL-014: list/stream REQUIRE a runId/threadId
  filter and validate its salted prefix; `getTask` 404s a missing OR foreign
  task with no oracle; the raw manager is never exposed).
- **Recovery seam pinned (R-002, spike B-S2):** DO eviction is survived by
  re-registering the static tool executors and calling the PUBLIC async
  `manager.init(pubsub)` at DO boot — which fires the manager's own (private)
  `recoverStaleTasks()` internally. No private method is ever called.

**Known substrate limitation (spike B-S1 findings R-B1/R-B2/R-B3, documented in
`background-tasks/host.ts`):** durable background-task *execution* does not yet
run on the Cloudflare substrate. Core runs task bodies on the *evented*
execution engine, which refuses to `createRun` unless the workflows store
reports `supportsConcurrentUpdates()`. `@mastra/cloudflare-d1` returns `false`
AND leaves `updateWorkflowResults`/`updateWorkflowState` as unimplemented throws
("D1 does not support atomic read-modify-write") — so R-B1 is NOT a flag to
flip: overriding it passes core's gate then throws on the first step-update,
stranding the task at `running`. The P9 fix is an adapter that *implements*
atomic partial-updates (the DO's single-threaded lease makes that safe), plus
`mastra.startWorkers()` to run the evented workers (R-B2 — the two close
together). A latent tenant-isolation residual (R-B3) rides along: core keys the
internal `__background-task` run by the UNSALTED `taskId`, so its snapshot row
escapes tenant offboarding — inert while execution is blocked, but it MUST be
closed in the same change that enables execution, and a CI guard
(`background-tasks/d1-storage.test.ts`) fails the instant
`supportsConcurrentUpdates()` returns true. Persistence, the recovery seam,
tenant purge + TTL, the read routes, and the `_background` defense all work
regardless. `BackgroundTaskHost.boot()` warns once so the limitation is loud,
not a stray async throw.
