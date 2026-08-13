# Anchorage advanced agent starter

This private workspace package shows how to compose the public `@proofoftech/flowsafe` and `@proofoftech/breakwater` APIs into a durable, approval-gated Cloudflare agent host for one organization.

Use it when an agent must:

- pause before a write and require a human decision;
- survive Worker or Durable Object eviction;
- resume with a server-derived connector grant that never crosses a client request;
- persist memory, signals, goals, notifications, schedules, subscriptions, approvals, tasks, and run snapshots in D1;
- stream run and approval changes;
- accept signed provider webhooks and alarm-driven polling; or
- recover non-gated background work.

This is an advanced composition example, not another published package. Copy it into your application, replace the example agent and connector, and preserve the boundaries below.

## Understand the package boundary

`@proofoftech/breakwater` governs what model output and tools may do. The `starter_recordAction` connector is write-class, requires approval, uses a shared D1 rate store, and refuses execution unless Flowsafe derives a matching grant for the current leg.

`@proofoftech/flowsafe` governs where and when work runs. It supplies storage composition, Durable Object execution, the approval queue, durable agent resume, live streams, memory helpers, signals, goals, schedules, provider subscriptions and webhooks, background tasks, and retention.

The critical flow is:

```text
authenticated actor
  -> host mints opaque run and thread ids and derives a resource id from a validated host key
  -> thread Durable Object starts the runtime-driven agent
  -> Breakwater write tool requires approval and the runtime suspends
  -> Flowsafe records the exact suspension in D1
  -> a different authorized human approves
  -> approval service routes back to the thread Durable Object
  -> registry rehydration -> observe/register -> runtime resume
  -> Flowsafe derives the connector grant from D1 for that exact leg
  -> Breakwater executes the write
```

No route accepts a grant. No public route exposes raw agent resume.

## Architecture

The Worker binds six Durable Object classes:

| Binding | Identity | Responsibility |
| --- | --- | --- |
| `RUNNER` | `workflowId:runId` | Generic workflow execution and per-run progress |
| `THREAD` | Server-minted `threadId` | Durable agent, memory binding, signal affinity, and approval resume |
| `HUB` | Fixed deployment singleton | Hibernatable approval fan-out and presence |
| `SIGNAL_PROVIDER_HOST` | Fixed deployment singleton | D1 subscription rehydration and provider alarm lifecycle |
| `BACKGROUND_TASKS` | `deployment-background-tasks` singleton | Background manager, recovery, cleanup, and read/SSE facade |
| `MAINTENANCE` | `deployment-maintenance` singleton | Alarm-owned SLA sweep, retention purge, and schedule or notification ticks |

All six use the deployment's D1 database. `createComposedStorage()` overlays notification, thread-state, and schedule domains on Mastra storage. The background-task Durable Object composes its own task and serialized-workflow domains for the same database.

`createFlowsafeWorker()` owns deployment verification, health, the guarded agent catalog, stream tickets, signals, goals, schedules, approvals, workflows, SLA sweep, retention, and schedule or notification ticks. `preRoutes` adds provider webhooks, subscriptions, and the background-task facade.

## Provision one physical deployment

Each organization needs a dedicated Worker, D1 database, Durable Object namespaces, and internal Durable Object credential. Replace every `replace-me` segment in `wrangler.jsonc` with the stable lowercase deployment tag before creating resources. For tag `acme`, use Worker `anchorage-agent-starter-acme` and D1 database `anchorage-agent-starter-acme`; the unique Worker name creates the deployment's Durable Object namespaces. Then stamp the same tag into the new D1 database before any application schema or traffic:

```bash
pnpm --dir packages/flowsafe provision:deployment -- \
  --database anchorage-agent-starter-acme \
  --tag acme \
  --remote \
  --config ../agent-starter/wrangler.jsonc
pnpm --filter anchorage-agent-starter exec wrangler secret put \
  DEPLOYMENT_IDENTITY_SECRET
pnpm --filter anchorage-agent-starter exec wrangler secret put \
  MAINTENANCE_ADMIN_SECRET
```

The provisioning command verifies the exact singleton sentinel schema. It is idempotent for the same tag, refuses a database already stamped for another deployment, and refuses an unowned database with application tables. The Worker validates the tag and sentinel. Every Worker-to-object topology also stamps the internal credential, which the target compares before reading storage; this prevents a cross-script binding from reaching another deployment's objects.

Do not expose provisioning as a public route. Do not share or reuse this resource set between organizations. A control plane may route hostnames to deployments, but request identity never chooses the database or namespaces. A physical-isolation cutover must use a new tag-suffixed Worker name, D1 database, and Durable Object namespaces; an in-place deploy under a legacy script name retains its old namespaces.

## Quick start

Requirements:

- Node.js 22 or newer;
- pnpm;
- a Cloudflare account with Workers, Durable Objects, and D1;
- a Mastra model provider for live runs.

From the repository root:

```bash
pnpm install
cp packages/agent-starter/.dev.vars.example packages/agent-starter/.dev.vars
pnpm --filter anchorage-agent-starter check
```

The deterministic smoke test uses an in-process model and never contacts a provider. It also invokes the write connector without a grant and proves denial happens before D1. Live runs require a real `MODEL_ID` and `MODEL_API_KEY`; the committed `provider/model` placeholder fails closed.

Create the production database and paste its id into `wrangler.jsonc`:

```bash
pnpm --filter anchorage-agent-starter exec wrangler d1 create anchorage-agent-starter-acme
```

Set `DEPLOYMENT_TENANT`, run the provisioning command above, then configure distinct secrets:

```bash
cd packages/agent-starter
pnpm exec wrangler secret put DEPLOYMENT_IDENTITY_SECRET
pnpm exec wrangler secret put MAINTENANCE_ADMIN_SECRET
pnpm exec wrangler secret put AUTH_HMAC_SECRET
pnpm exec wrangler secret put STREAM_TICKET_SECRET
pnpm exec wrangler secret put MODEL_API_KEY
pnpm exec wrangler secret put GITHUB_WEBHOOK_SECRET
pnpm run deploy
```

After deployment, authenticate `POST /admin/ensure-maintenance` with `MAINTENANCE_ADMIN_SECRET`. Confirm `GET /admin/maintenance-status` returns a non-null `alarmAt` before routing traffic.

For local work:

```bash
pnpm --dir packages/flowsafe provision:deployment -- \
  --database anchorage-agent-starter-acme \
  --tag acme \
  --local \
  --config ../agent-starter/wrangler.jsonc \
  --persist-to ../agent-starter/.wrangler/state
pnpm --filter anchorage-agent-starter dev
```

Set `DEPLOYMENT_TENANT` to `acme` before running that command. Wrangler reads `.dev.vars` and opens the seeded local state directory. Use different values for the internal Durable Object credential, authentication, and stream-ticket signing.

## Authenticate actors

The starter accepts HS256 bearer JWTs through `hmacVerifier()`. Required claims are:

- `sub`: non-empty actor id;
- `role`: `admin`, `builder`, `operator`, `reviewer`, or `viewer`;
- `iss`, `aud`, and a future `exp`.

The token does not carry deployment identity. Mint a one-hour development token:

```bash
export AUTH_HMAC_SECRET='the same value used in .dev.vars'
export AUTH_JWT_ISSUER='anchorage-agent-starter'
export AUTH_JWT_AUDIENCE='anchorage-agent-starter-api'
TOKEN="$(pnpm --filter anchorage-agent-starter token operator alice)"
REVIEWER_TOKEN="$(pnpm --filter anchorage-agent-starter token reviewer bob)"
```

Strict separation of duties is the default. The actor who requested a gate cannot decide it, and an actor who approved an earlier gate cannot approve a later gate in the same run.

## Run the durable agent

Start a run. IDs are intentionally absent from the body:

```bash
curl -sS -X POST http://localhost:8787/agents/anchorage-agent/runs \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"prompt":"Record that the launch checklist was completed"}'
```

The response contains `agentId`, `threadId`, `resourceId`, `runId`, a suspended summary, and its approval record. The host mints opaque run and thread ids and derives the resource id from trusted configuration. Treat the returned ids as whole identifiers; do not parse deployment or ownership from them.

List and decide the approval:

```bash
curl -sS http://localhost:8787/api/approvals \
  -H "authorization: Bearer $REVIEWER_TOKEN"

curl -sS -X POST \
  http://localhost:8787/api/approvals/APPROVAL_ID/decide \
  -H "authorization: Bearer $REVIEWER_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"decision":"approve","comment":"reviewed"}'
```

The decision route rehydrates the agent after eviction, validates the persisted thread, resource, run, agent, and principal bindings, then resumes through `RunnerRuntime`.

Query status and observe newline-delimited events:

```bash
curl -sS \
  "http://localhost:8787/agents/anchorage-agent/runs/THREAD_ID/RUN_ID" \
  -H "authorization: Bearer $TOKEN"

curl -sS \
  "http://localhost:8787/agents/anchorage-agent/runs/THREAD_ID/RUN_ID/stream?offset=0" \
  -H "authorization: Bearer $TOKEN"
```

Replay is short-lived and in memory. If the stream returns `409` after eviction or restart, read the durable status route. To prove restart safety, stop `wrangler dev` after suspension, restart it, then decide the approval.

## HTTP surface

Every route except `/healthz`, a correctly signed provider webhook, and WebSocket upgrades requires a bearer token. `/healthz` still verifies the deployment sentinel. A WebSocket upgrade requires a short-lived ticket minted by the authenticated `POST /api/stream/ticket` route.

| Method and route | Purpose |
| --- | --- |
| `GET /healthz` | Deployment identity and liveness |
| `GET /agents` | Agent metadata and authenticated actor |
| `POST /agents/:agentId/runs` | Mint ids and start the guarded durable agent |
| `GET /agents/:agentId/runs/:threadId/:runId` | Durable agent status |
| `GET /agents/:agentId/runs/:threadId/:runId/stream?offset=N` | Authenticated NDJSON observation |
| `GET /workflows` | Generic workflow catalog |
| `POST /runs` | Start a generic workflow with a server-minted id |
| `GET /runs/:workflowId/:runId` | Generic workflow status |
| `POST /runs/:workflowId/:runId/resume` | Grant-free generic workflow resume |
| `GET /api/approvals` | Filtered deployment approval queue |
| `GET /api/approvals/metrics` | Deployment queue metrics |
| `GET /api/approvals/:id` | Approval detail |
| `POST /api/approvals/:id/claim` | Claim for review |
| `POST /api/approvals/:id/decide` | Decide and route restart-safe resume |
| `POST /api/approvals/:id/delegate` | Delegate a claimed record |
| `POST /api/approvals/batch/decide` | Decide up to 100 records through the same CAS and separation-of-duties path |
| `POST /api/stream/ticket` | Mint a short-lived channel ticket |
| `GET /api/stream/hub?ticket=...` | Deployment approval and presence WebSocket |
| `GET /api/stream/run/:workflowId/:runId?ticket=...` | Per-run progress WebSocket |
| `POST /api/threads/:threadId/{signal,message,queue,state,notification}` | Rate-limited, audited signal ingestion |
| `PUT/GET/PATCH/DELETE /api/threads/:threadId/goal` | Persistent agent objective |
| `POST/GET /api/threads/:threadId/subscriptions` | Subscribe or list provider resources |
| `DELETE /api/threads/:threadId/subscriptions` | Unsubscribe |
| `POST /api/signal-providers/github/webhook` | Verify GitHub HMAC over raw bytes, then deliver |
| `POST/GET /api/schedules` | Create or list workflow and agent schedules |
| `GET/PATCH/DELETE /api/schedules/:id` | Read, update, or delete a schedule |
| `POST /api/schedules/:id/{pause,resume}` | Control schedule state |
| `GET /api/schedules/:id/triggers` | Read fire history |
| `GET /api/background-tasks?runId=...` | Task list |
| `GET /api/background-tasks/task/:taskId` | Task detail |
| `GET /api/background-tasks/stream?runId=...` | Task lifecycle SSE |

HTTP creation of approval records is disabled. Records are created only after the runtime observes a suspension.

## Signals, goals, subscriptions, and webhooks

Signal bodies are model input, not capabilities. The Worker enforces a 16 KiB body cap, an isolate-local rate limit, thread access checks, and `SIGNAL_ATTRIBUTE_ALLOWLIST` before forwarding to the thread Durable Object. Replace the example limiter with shared durable state when the limit is contractual.

Goals are standing instructions stored in `mastra_thread_state`. The starter caps `maxRuns` at 50. A goal write does not start a run.

`GITHUB_RESOURCE_ALLOWLIST` is a deployment-owned JSON array of exact repository resources:

```text
GITHUB_RESOURCE_ALLOWLIST=["github:OWNER/REPOSITORY"]
```

An empty or invalid array denies new GitHub subscriptions. A repository entry also permits its `#issue-or-pull-number` children. Populate it only from trusted configuration.

```bash
curl -sS -X POST \
  "http://localhost:8787/api/threads/THREAD_ID/subscriptions" \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{
    "providerId":"github",
    "externalResourceId":"github:OWNER/REPOSITORY",
    "resourceKey":"THREAD_ID"
  }'
```

The starter requires `resourceKey` to equal the opaque thread id, checks actor role and resource configuration, then validates and stores that key as the resource id. Removing a repository from configuration blocks new subscriptions; explicitly delete existing rows when revoking access.

A committed mutation calls `reconcilePolling()` on the singleton provider-host Durable Object. GitHub is webhook-only, so the host removes an unnecessary alarm. Polling providers arm the earliest required alarm and rehydrate subscriptions from D1 after eviction.

Configure the GitHub webhook at:

```text
https://YOUR_WORKER/api/signal-providers/github/webhook
```

The route verifies `X-Hub-Signature-256` over raw bytes before parsing or subscription lookup. Payload routing ids are not authoritative; the stored subscription determines delivery.

## Schedules and unattended work

The one-minute tick claims due schedules with D1 CAS, starts generic workflows or the same runtime-driven thread agent, and dispatches due notifications through the owning thread Durable Object.

Agent schedules must name `agentId: "anchorage-agent"`. A threaded schedule uses a `threadId` and `resourceId` returned by the start route. Stored request context cannot contain Breakwater grant keys or runtime-reserved keys. Every fire gets a fresh opaque run id.

The starter caps the deployment at 100 schedules and rejects crons faster than one minute. A metered host should wire schedule and signal starts to the same durable quota store.

## Background tasks

`StarterBackgroundTasks` is a deployment singleton. It composes serialized workflow and task storage domains, registers static executors on boot, starts Mastra workers before recovery, and uses a Durable Object alarm to recover work after eviction. The included `starter_echo` executor is a wiring example.

Public background routes are read-only. Enqueue, cancel, and resume are server-side operations. Keep approval-carrying writes in the foreground agent topology.

## Alarm-driven maintenance

The fixed `deployment-maintenance` Durable Object schedules three independent duties:

- five-minute approval SLA sweep;
- hourly approval, terminal run, memory, notification, thread-state, schedule-trigger, and background-task retention;
- one-minute schedule firing and due-notification dispatch.

The object persists the next alarm before each duty and runs one due duty per invocation. Provider and background-task maintenance use separate Durable Object alarms. Provisioning must authenticate `/admin/ensure-maintenance` after deployment and monitor `/admin/maintenance-status`.

## Configuration

| Name | Kind | Purpose |
| --- | --- | --- |
| `DEPLOYMENT_TENANT` | variable | Required provisioning tag matching the D1 sentinel |
| `DEPLOYMENT_IDENTITY_SECRET` | secret | Required internal Worker-to-Durable-Object caller credential |
| `MAINTENANCE_ADMIN_SECRET` | secret | Required control-plane credential for maintenance bootstrap and status |
| `AUTH_HMAC_SECRET` | secret | HS256 actor verification; absent means protected routes return `401` |
| `AUTH_JWT_ISSUER` | variable | Required JWT issuer |
| `AUTH_JWT_AUDIENCE` | variable | Required JWT audience |
| `STREAM_TICKET_SECRET` | secret | Short-lived stream tickets; keep distinct from authentication |
| `MODEL_ID` | variable | Mastra `provider/model` id; the committed placeholder fails closed |
| `MODEL_API_KEY` | secret | Live model credential |
| `MODEL_BASE_URL` | optional secret | OpenAI-compatible base URL |
| `GITHUB_WEBHOOK_SECRET` | secret | GitHub webhook signature verification |
| `GITHUB_RESOURCE_ALLOWLIST` | variable | JSON array of allowed `github:owner/repository` resources |
| `SIGNAL_ATTRIBUTE_ALLOWLIST` | variable | CSV of accepted signal attribute keys |
| `APPROVAL_SLA_SECONDS` | variable | Default approval SLA |
| `RUN_RETENTION_DAYS` | variable | Terminal workflow and agent snapshot TTL |
| `APPROVAL_RETENTION_DAYS` | variable | Decided approval TTL |
| `THREAD_RETENTION_DAYS` | variable | Inactive thread and message TTL |
| `NOTIFICATION_RETENTION_DAYS` | variable | Terminal inbox row TTL |
| `THREAD_STATE_RETENTION_DAYS` | variable | Signal state and goal TTL |
| `SCHEDULE_TRIGGER_RETENTION_DAYS` | variable | Schedule fire-history TTL |

## Security properties to preserve

- Allocate one physical data-plane resource set per organization, verify its sentinel before every entry surface, and authenticate every Worker-to-Durable-Object call.
- Mint run, thread, subscription, schedule, and task ids on the server. Derive resource ids from validated host-owned keys. Never accept full memory ids in a create body.
- Preserve the principal ownership record for each run, thread, resource, and schedule; inaccessible resources must return `404` before role errors.
- Reach thread Durable Objects only through `createThreadTopology()` or `createAgentThreadTopology()`. The topology strips retired identity headers and writes the trusted execution-principal header.
- Declare every automated entry in `allowedAutomation`. Omission denies unattended entry.
- Keep approval grants server-derived through `approvalGrantProvider()` and agent resume behind `ApprovalService.decide()`.
- Authorize provider subscriptions against server-owned resource configuration before persisting them.
- Verify webhooks over raw bytes before parsing and route through stored subscriptions.
- Use shared stores for cross-isolate budgets and idempotency. Flowsafe's connector context is deployment-wide and does not mint a logical organization scope.
- Keep connector ids server-authored. Model or client input must not choose approval capabilities.
- Decommission the whole Worker, D1, Durable Object, R2, Queue, and secret resource set. There is no in-database organization purge.

## Customize the starter

1. Replace `createStarterAgentModule()` instructions, tools, metadata, and allowed roles.
2. Keep every external side effect behind `createConnector()`.
3. Add each connector's real egress hosts to its permission manifest and use the guarded `fetch`.
4. Add workflow metadata and committed workflows together; registration fails fast if they drift.
5. Add provider adapters to both the webhook map and provider-host list, then add their ids to the subscription allowlist.
6. Replace console audit sinks with your Queue or SIEM transport.
7. Add a durable run-cap implementation before exposing unattended execution commercially.

Every Anchorage import in `src/` uses a documented package export. `check:imports` rejects source or distribution deep imports and relative reaches into sibling packages.

## Verify the starter

Run:

```bash
pnpm --filter anchorage-agent-starter typecheck
pnpm --filter anchorage-agent-starter test
pnpm --filter anchorage-agent-starter build
```

The tests include token verification, a deterministic model smoke, an approval-denial/no-D1-side-effect assertion, and the public-import boundary. For a live proof, run the start, stop, restart, decide, and status sequence above with real model configuration.

## Submit it as a Workers for Platforms artifact

The same code has a second role. `packages/fleet-control` provisions physically isolated deployments through Workers for Platforms, and an externally authored agent reaches customers only as a **candidate** script running beside a platform-owned **trusted state** script. `src/conformance/` builds that pair out of this starter, so "passes the conformance gate" and "is a legal submission" describe one artifact rather than two.

Read [`../../docs/fleet-control.md`](../../docs/fleet-control.md) under "Implement the artifact contract" for the contract itself; it is versioned and it is the authority.

### The two roles

| Role | Built from | Owns Durable Object classes | Receives |
| --- | --- | --- | --- |
| External candidate | `src/conformance/candidate.ts` | No — every binding resolves to the trusted state script | Application variables, application secrets, application R2, D1, and remote Durable Object bindings |
| Trusted state | `src/conformance/state-v1.ts` and `state-v2.ts` | Yes, locally | The outbound proxy service binding, the audit queue producer, and the maintenance secret |

The split is the lesson: an approval must survive its candidate being replaced. The gate starts a FlowSafe run on release one, uploads release two, and only then approves, so anything durable held in candidate isolate memory loses the run. Every durable action here is a Durable Object call into trusted state.

### Build and verify the artifacts

```bash
pnpm --filter anchorage-agent-starter build:conformance
pnpm --filter anchorage-agent-starter conformance:verify
pnpm test:conformance-config
```

`build:conformance` emits `dist/conformance/{candidate,trusted-state-v1,trusted-state-v2}.mjs` and refuses a bundle that is not one self-contained module, whose exported class set differs from its migrations, or that stops parsing when the gate appends its release-two comment. `conformance:verify` runs it first, so both are one command in CI.

`conformance:verify` runs all three under real workerd and drives the contract in the gate's order, including the release update with one approval suspended across it. Four things it cannot prove — platform CPU termination, platform-layer egress denial, namespace and secret retention across a same-name upload, and decommission refusal on a non-empty bucket — are printed at the end of every run.

`test:conformance-config` validates `conformance/anchorage-starter.conformance.json` with fleet control's own validators — both the structural pass and the production deployment-spec pass — and checks the harness wrangler configurations against the same contract. All three commands run in CI.

### Run the paid gate

Not yet run: it needs a scratch Cloudflare account with a Workers for Platforms subscription. When one exists:

1. `pnpm --filter anchorage-agent-starter build:conformance`.
2. Copy `conformance/anchorage-starter.conformance.json` and replace the account-specific values, all of which ship as placeholders:
   - `hostRoutingKvId`
   - `routeHostnames.tenanta` and `routeHostnames.tenantb`
   - `maintenanceBaseUrls.tenanta` and `maintenanceBaseUrls.tenantb`
   - `platformProfile.maintenanceCapabilityPublicKey`
3. Point `conformance.allowedUpstreamUrl` at an origin that really answers 2xx, set `platformProfile.organizationEgressHosts` to exactly that hostname, and point `deniedUpstreamUrl` at a hostname absent from that list. The placeholder `.example` hosts do not resolve, the gate requires an actual upstream status from both, and stage-one validation rejects a configuration whose allowed hostname is not in `organizationEgressHosts`.
4. Export `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, `FLEET_CONFORMANCE_CONFIG`, `FLEET_MAINTENANCE_CAPABILITY_PRIVATE_JWK`, `FLEET_STATE_EGRESS_ROOT_SECRET`, and `FLEET_CONFORMANCE_APPLICATION_SECRET`. Bundle paths in the configuration resolve from `packages/fleet-control`, which is the runner's working directory.
5. `pnpm fleet-control:credentialed > proof.json`. The proof goes to standard output; retain it with the release evidence.

Regenerate the configuration with `pnpm --filter anchorage-agent-starter conformance:config` after any change to `src/conformance/contract.json`; that file is the only place binding names, paths, and class names are written.

### Delete it

No non-test module outside `src/conformance/` imports any of it, so the standalone agent host keeps building and deploying unchanged once these are gone:

- `src/conformance/` and `conformance/`
- `test/conformance-routes.test.ts`, `test/conformance-websocket.test.ts`, `test/conformance-replay.test.ts`
- `scripts/build-conformance.mjs`, `scripts/emit-conformance-config.mjs`, `scripts/conformance-verify.mjs`
- the `build:conformance`, `conformance:config`, and `conformance:verify` scripts in this package's `package.json`
- at the repository root: `scripts/conformance-config-check.test.mjs`, the `test:conformance-config` and `conformance:verify` scripts in `package.json`, and their two CI steps

`scripts/workerd-server-lifecycle.mjs` at the repository root stays: the FlowSafe workerd spike uses it too.

## Agent host composition

The starter uses only `@proofoftech/flowsafe/agent-host`. The Worker router receives `STARTER_AGENT_META`; each `StarterThread` constructs the complete module from instance-scoped model, storage, runtime, pub/sub, connector, and database objects.

`createThreadAgentHost()` owns the internal start, status, observe, and resume topology. Application code does not author private Durable Object URLs or keep mutable current-request scope. `createAgentApprovalResumer()` restores the original execution principal from the approval target and delegates generic workflow records to the run topology.

### Declare allowed automation

Unattended work carries a non-human execution principal. The agent accepts none by default. `STARTER_AGENT_META` names each allowed kind and exact entry path:

```typescript
allowedAutomation: [
  { kind: 'system', entryPaths: ['schedule.fire', 'notification.dispatch'] },
  { kind: 'service', entryPaths: ['signal.notification'] },
],
```

The guarded agent declares matching `allowedPrincipalKinds: ['human', 'system', 'service']`. Catalog construction refuses a mismatch. Removing a kind from either side stops that automation.
