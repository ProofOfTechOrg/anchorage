# background-tasks/

Track B (M-003) hosts Mastra background tasks on one single-tenant Durable Object
(DO) per tenant. The subpath-only export is
`@proofoftech/flowsafe/background-tasks`.

D1 execution is enabled only through `createBackgroundTaskD1Domains`. Its
`DurableObjectWorkflowsStorageD1` serializes all updates for each workflow run in
one DO isolate. Its `TenantScopedBackgroundTasksStorageD1` filters recovery and
all task access by the salted parent `runId`. `BackgroundTaskHost.execution`
verifies those two stores and the shared pubsub identity before starting Mastra's
public workers. Execution boot registers static executors and starts the workflow
subscriber before `manager.init(pubsub)` publishes recovery work; reversing that
order strands the recovered workflow event on an in-process pubsub.

Never host execution on a global manager or one manager per thread. Address one
background-task DO with `idFromName(tenantId)`. The serialized workflow adapter
is not a cross-isolate transaction mechanism.

Internal `__background-task` workflow snapshots use unsalted task IDs. Store
deletion, task time-to-live (TTL) purge, and tenant offboarding delete those
snapshots before their task rows. Tenant offboarding also includes the snapshots
in artifact cleanup and the returned snapshot count.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `d1-storage.ts` | `DurableObjectWorkflowsStorageD1`, `TenantScopedBackgroundTasksStorageD1`, `createBackgroundTaskD1Domains`, and `backgroundTasksStore`. The tenant store filters before pagination and couples task deletion to internal snapshot deletion | Wiring D1 execution or changing tenant isolation and cleanup |
| `host.ts` | `BackgroundTaskHost`. Persistence-only mode retains the existing recovery behavior. Execution mode validates the serialized and tenant-scoped domains, registers executors, starts public Mastra workers on the same pubsub, then initializes the manager so recovery publishes only after the workflow subscriber is ready | Hosting the manager or changing its boot and shutdown lifecycle |
| `routes.ts` | `createBackgroundTaskRoutes({ manager })` — READ-only, tenant-bound by construction (DL-014): list/stream REQUIRE a runId/threadId filter and validate its salted prefix; `getTask` 404s a missing OR foreign task with the SAME response (no oracle); the raw manager is never exposed. `(request, tenantId) => Response \| null`; the DO passes its own asserted tenant | The tenant-bound read surface |
| `index.ts` | Subpackage barrel | The export surface |
| `d1-storage`/`host`/`routes` `.test.ts` | Serialized partial-update parity, tenant-filter-before-pagination, scoped recovery, deletion cascade, host execution validation, restart recovery, and the tenant-bound route matrix. The workerd spike proves dispatch to completion and post-kill recovery on D1 | Adding tests |

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
