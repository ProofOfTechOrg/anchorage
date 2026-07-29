# Anchorage advanced agent starter

This private workspace package is a consumer-sized Cloudflare Worker showing how
to compose the public `@proofoftech/flowsafe` and
`@proofoftech/breakwater` APIs into a durable, multi-tenant agent host.

Use it when an agent must do more than answer a request:

- pause before a write and require a human decision;
- survive Worker or Durable Object eviction while it waits;
- resume with a server-derived connector grant that never crosses a client
  request;
- keep agent memory, signals, goals, notifications, schedules, subscriptions,
  approvals, and run snapshots in D1;
- stream run and approval changes over tenant-isolated WebSockets;
- accept signed provider webhooks and alarm-driven provider polling;
- run and recover non-gated background tasks; and
- apply the same authenticated tenant boundary to every route.

This is an advanced composition example, not another published package. Copy it
into your application, replace the example agent and connector, and retain the
security boundaries called out below.

## What the two packages do

`@proofoftech/breakwater` governs what model output and tools may do. Its
connector wrapper enforces a permission manifest at execution time. The
`starter_recordAction` example is write-class, requires approval, has a
tenant-scoped shared D1 rate limit, and cannot run when Flowsafe has not derived
the current approval grant.

`@proofoftech/flowsafe` governs where and when work runs. It supplies the D1
storage composition, Durable Object runtime, approval queue, restart-safe
durable agent wrapper, live hub, memory-tenancy helpers, signals, goals,
schedules, provider subscriptions/webhooks, background-task host, and retention
jobs.

The important boundary is:

```text
authenticated request
  -> server mints tenant-salted run/thread/resource IDs
  -> thread Durable Object starts the runtime-driven agent
  -> Breakwater write tool asks for approval and the runtime suspends
  -> Flowsafe records the exact suspension in D1
  -> a different authorized human approves
  -> approval service routes back to the thread Durable Object
  -> prepare -> observe/register -> runtime resume
  -> Flowsafe derives the connector grant from D1 for that exact leg
  -> Breakwater executes the write
```

No route accepts a grant. No public route exposes raw agent resume.

## Architecture

The Worker binds five Durable Object classes:

| Binding | Identity | Responsibility |
| --- | --- | --- |
| `RUNNER` | `workflowId:runId` | Generic Flowsafe workflow execution and per-run WebSocket progress |
| `THREAD` | server-minted `threadId` | Runtime-driven durable agent, memory binding, signal affinity, approval resume |
| `HUB` | authenticated `tenantId` | Hibernatable tenant approval fan-out and presence |
| `SIGNAL_PROVIDER_HOST` | authenticated `tenantId` | D1 subscription rehydration and provider alarm lifecycle |
| `BACKGROUND_TASKS` | authenticated `tenantId` | Tenant-scoped background manager, recovery, cleanup, read/SSE facade |

All five share one D1 database. `createComposedStorage()` overlays Flowsafe's
notifications, thread-state, and schedules domains on Mastra's D1 store. The
background-task Durable Object deliberately uses a separate tenant-scoped
domain composition because Mastra's recovery scan is otherwise tenant-blind.

The Worker itself uses `createFlowsafeWorker()` for health, the guarded agent catalog, live stream tickets, signals, goals, schedules, approvals, workflows/runs, SLA sweep, retention, and schedule/notification ticks. `buildAgentRouter` mounts the metadata-only catalog, while `buildResumeRun` composes approval-only agent resume with the generic workflow path. `preRoutes` remains for provider webhook, subscription, and background-task facades.

## Quick start

Requirements:

- Node.js 22 or newer;
- pnpm;
- a Cloudflare account with Workers, Durable Objects, and D1;
- a configured Mastra model provider for a live agent run.

From the repository root:

```bash
pnpm install
cp packages/agent-starter/.dev.vars.example packages/agent-starter/.dev.vars
pnpm --filter anchorage-agent-starter check
```

The deterministic smoke test uses an in-process model and never contacts a
provider. It also invokes the write connector without a grant and proves the
denial happens before D1. Live runs require a real `MODEL_ID` and
`MODEL_API_KEY`; the committed `provider/model` placeholder throws a clear
configuration error.

Create the production database:

```bash
pnpm --filter anchorage-agent-starter exec wrangler d1 create anchorage-agent-starter
```

Put the returned ID in `wrangler.jsonc`, then set secrets:

```bash
cd packages/agent-starter
pnpm exec wrangler secret put AUTH_HMAC_SECRET
pnpm exec wrangler secret put STREAM_TICKET_SECRET
pnpm exec wrangler secret put MODEL_API_KEY
pnpm exec wrangler secret put GITHUB_WEBHOOK_SECRET
pnpm run deploy
```

Use different random values for the auth and stream-ticket secrets. A stream
ticket is addressing-only, but sharing its signing key with session JWTs would
collapse two trust domains.

For local work:

```bash
pnpm --filter anchorage-agent-starter dev
```

Wrangler reads `.dev.vars`. D1 tables are created lazily by the public storage
adapters.

## Authentication and tenants

The starter accepts HS256 bearer JWTs through Flowsafe's `hmacVerifier()`.
Required claims are:

- `sub`: non-empty actor ID;
- `role`: `admin`, `builder`, `operator`, `reviewer`, or `viewer`;
- `tenantId`: lowercase 3–32 character tenant slug;
- `iss`, `aud`, and a future `exp`.

Provision every production tenant before issuing any token that names it. The
tenant registry is the allocation authority that prevents two customers from
being assigned the same slug—and therefore the same runs, approvals, budgets,
and memory. Call the starter's `provisionCommercialTenant(env, tenantId)` from
your private deployment control plane or one-off authenticated administration
Worker:

```ts
import { provisionCommercialTenant } from './src/provisioning.js';

await provisionCommercialTenant(env, 'acme');
```

It delegates to Flowsafe's insert-or-fail `provisionTenant()` API. A duplicate
or reserved slug fails instead of adopting an existing tenant. The starter
deliberately does not mount public tenant-provisioning HTTP. Keep the registry
record as an offboarding tombstone and never recycle a tenant ID.

Mint a one-hour development token:

```bash
export AUTH_HMAC_SECRET='the same value used in .dev.vars'
export AUTH_JWT_ISSUER='anchorage-agent-starter'
export AUTH_JWT_AUDIENCE='anchorage-agent-starter-api'
TOKEN="$(pnpm --filter anchorage-agent-starter token acme operator alice)"
```

Mint a separate reviewer token for separation of duties:

```bash
REVIEWER_TOKEN="$(pnpm --filter anchorage-agent-starter token acme reviewer bob)"
```

The default is strict separation of duties: the actor who requested a gate may
not decide it, and an actor who approved an earlier gate may not approve a later
gate in the same run. Do not use one actor for the walkthrough.

Set `TENANT_APEX_DOMAIN` for tenant-per-subdomain deployments. A request to
`acme.example.com` then fails unless the verified token tenant is `acme`.
Authorization still comes from the verified token and tenant-bound store; the
host name is only a defense-in-depth cross-check.

## Run the durable agent

Start a run. IDs are intentionally absent from the body:

```bash
curl -sS -X POST http://localhost:8787/agents/anchorage-agent/runs \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"prompt":"Record that the launch checklist was completed"}'
```

The response envelope contains `agentId`, `threadId`, `resourceId`, `runId`, a suspended `summary`, and its approval record. Keep all three ids. The resource id is deterministically minted from the server-minted thread id, so notification delivery and restart resume can recover the same memory binding without trusting client state.

List the queue:

```bash
curl -sS http://localhost:8787/api/approvals \
  -H "authorization: Bearer $REVIEWER_TOKEN"
```

Approve the returned record:

```bash
curl -sS -X POST \
  http://localhost:8787/api/approvals/APPROVAL_ID/decide \
  -H "authorization: Bearer $REVIEWER_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"decision":"approve","comment":"reviewed"}'
```

The decision route rehydrates the agent after eviction, validates the snapshot's
thread/resource binding, and resumes through `RunnerRuntime`. Query status:

```bash
curl -sS \
  "http://localhost:8787/agents/anchorage-agent/runs/THREAD_ID/RUN_ID" \
  -H "authorization: Bearer $TOKEN"
```

Observe newline-delimited JSON events with a reconnect cursor:

```bash
curl -sS \
  "http://localhost:8787/agents/anchorage-agent/runs/THREAD_ID/RUN_ID/stream?offset=0" \
  -H "authorization: Bearer $TOKEN"
```

Each line contains `offset`, the next reconnect cursor, and `event`. Replay is short-lived and in memory by default. If the stream returns 409 after eviction or restart, use the status route because the durable summary remains authoritative.

To prove restart safety locally, stop `wrangler dev` after the suspended
response, start it again, then make the approval decision.

## HTTP surface

All routes except `/healthz`, a correctly signed provider webhook, and the two
WebSocket upgrade routes require a bearer token. A WebSocket upgrade instead
requires a valid short-lived ticket minted by the bearer-authenticated
`POST /api/stream/ticket` route.

| Method and route | Purpose |
| --- | --- |
| `GET /healthz` | Liveness |
| `GET /agents` | Registered agent metadata and authenticated actor |
| `POST /agents/:agentId/runs` | Server-mint a thread/resource/run and start the guarded durable agent |
| `GET /agents/:agentId/runs/:threadId/:runId` | Authoritative durable-agent status |
| `GET /agents/:agentId/runs/:threadId/:runId/stream?offset=N` | Authenticated NDJSON observation |
| `GET /workflows` | Generic workflow catalog (`starter-echo` included) |
| `POST /runs` | Start a server-ID'd generic workflow |
| `GET /runs/:workflowId/:runId` | Generic workflow status |
| `POST /runs/:workflowId/:runId/resume` | Raw generic workflow resume; never grants a connector |
| `GET /api/approvals` | Filtered/paginated approval queue |
| `GET /api/approvals/metrics` | Queue metrics |
| `GET /api/approvals/:id` | Approval detail |
| `POST /api/approvals/:id/claim` | Claim for review |
| `POST /api/approvals/:id/decide` | Approve or reject and route restart-safe resume |
| `POST /api/approvals/:id/delegate` | Delegate a claimed record |
| `POST /api/approvals/batch/decide` | Decide up to 100 records through the same CAS/SoD path |
| `POST /api/stream/ticket` | Mint a short-lived tenant/channel WebSocket ticket |
| `GET /api/stream/hub?ticket=...` | Tenant approval/presence WebSocket |
| `GET /api/stream/run/:workflowId/:runId?ticket=...` | Per-run progress WebSocket |
| `POST /api/threads/:threadId/{signal,message,queue,state,notification}` | Rate-limited, audited signal ingestion |
| `PUT/GET/PATCH/DELETE /api/threads/:threadId/goal` | Persistent agent objective |
| `POST/GET /api/threads/:threadId/subscriptions` | Subscribe/list provider resources |
| `DELETE /api/threads/:threadId/subscriptions` | Unsubscribe |
| `POST /api/signal-providers/github/webhook` | Verify GitHub HMAC over raw bytes, then deliver |
| `POST/GET /api/schedules` | Create/list workflow or agent schedules |
| `GET/PATCH/DELETE /api/schedules/:id` | Read/update/delete a schedule |
| `POST /api/schedules/:id/{pause,resume}` | Control schedule state |
| `GET /api/schedules/:id/triggers` | Read fire history |
| `GET /api/background-tasks?runId=...` | Tenant-scoped task list |
| `GET /api/background-tasks/task/:taskId` | Tenant-owned task detail |
| `GET /api/background-tasks/stream?runId=...` | Tenant-scoped lifecycle SSE |

HTTP creation of approval records is deliberately disabled. Records are created
only after the runtime observes an actual suspension.

## Signals, goals, subscriptions, and webhooks

Signal bodies are model input, not capabilities. The Worker enforces a 16 KiB
body cap, a per-tenant rate limit, tenant ownership, and the
`SIGNAL_ATTRIBUTE_ALLOWLIST` before forwarding to the thread Durable Object.
`sendToolApproval` is not exposed; decisions remain on the approval surface.

Goals are standing instructions stored in `mastra_thread_state`. The starter
caps `maxRuns` at 50. A goal write does not start a run.

To subscribe a returned thread to a GitHub repository:

First provision ownership in `GITHUB_RESOURCE_ALLOWLIST`. It is a JSON object
from tenant ID to exact repository resource IDs:

```text
GITHUB_RESOURCE_ALLOWLIST={"acme":["github:OWNER/REPOSITORY"]}
```

An empty or invalid map denies all new GitHub subscriptions. A repository
entry also permits its `#issue-or-pull-number` children. This is a control-plane
ownership assertion: never populate it from a webhook or client request.

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

`resourceKey` must be the returned thread ID in this starter. Flowsafe mints the
tenant-salted resource ID server-side. The Worker checks authentication, role,
thread ownership, and the external-resource ownership map before committing the
row. Removing a repository from the map blocks new subscriptions; explicitly
delete existing rows when revoking access. A committed subscription calls
`reconcilePolling()` on the tenant's provider-host Durable Object; GitHub itself
is webhook-only, so the host removes an unnecessary alarm. Add polling
providers to `StarterSignalProviderHost.providers` and the same reconciliation
seam arms the earliest required alarm. Subscriptions live in D1 and are
rehydrated after eviction.

Configure the GitHub webhook URL as:

```text
https://YOUR_WORKER/api/signal-providers/github/webhook
```

The route verifies `X-Hub-Signature-256` over raw bytes before JSON parsing or
subscription lookup. Payload tenant/thread IDs are ignored; the persisted
subscription row is the delivery authority.

## Schedules and unattended work

The one-minute tick does two independent jobs:

- claims due schedules with D1 CAS and starts generic workflows or the same
  runtime-driven thread agent;
- dispatches due notifications through the owning thread Durable Object.

Agent schedules must name `agentId: "anchorage-agent"`. For a threaded schedule,
use a `threadId` and `resourceId` returned by `POST /agents/anchorage-agent/runs`. Stored
request context cannot contain Breakwater grant keys or runtime-reserved keys.
The tick mints a fresh tenant-salted run ID for every fire.

The starter caps each tenant at 100 schedules and rejects crons faster than one
minute. For a metered product, wire `createScheduleTick({ runCap })` and
`createThreadSignalRoutes({ consultRunCap })` to the same durable quota store;
the example intentionally does not invent a billing policy.

## Background tasks

`StarterBackgroundTasks` composes the serialized workflow domain and a
tenant-scoped task domain, registers static executors on every boot, starts
Mastra workers before recovery, and uses a Durable Object alarm to recover work
after eviction. The included `starter_echo` executor is a wiring example.

The public background routes are read-only. Enqueueing, cancelling, and
resuming are server-side operations because they change execution and must not
be mistaken for an approval capability. Wire your read-only Breakwater
connectors to the manager inside the tenant Durable Object. Approval-carrying
write connectors stay foreground-only.

## Scheduled maintenance

Keep the three cron strings in `wrangler.jsonc` and `src/config.ts` exactly
equal:

- `*/5 * * * *`: approval SLA sweep;
- `17 * * * *`: approval, terminal run, memory, notification, thread-state,
  schedule-trigger, and background-task retention;
- `* * * * *`: schedule firing and due-notification dispatch.

The sweep, purge, and tick use separate invocations so a CPU-limit termination
in one duty cannot starve another. Thread, notification, thread-state, and
schedule-trigger retention are explicit vars because those records are not
terminal by definition.

## Configuration

| Name | Kind | Purpose |
| --- | --- | --- |
| `AUTH_HMAC_SECRET` | secret | HS256 bearer JWT verification; absent means every protected route returns 401 |
| `AUTH_JWT_ISSUER` | var | Required JWT issuer |
| `AUTH_JWT_AUDIENCE` | var | Required JWT audience |
| `STREAM_TICKET_SECRET` | secret | Enables short-lived live-stream tickets; keep distinct from auth |
| `MODEL_ID` | var | Required Mastra `provider/model` ID; the committed placeholder fails closed |
| `MODEL_API_KEY` | secret | Live model credential |
| `MODEL_BASE_URL` | secret/optional | OpenAI-compatible base URL |
| `GITHUB_WEBHOOK_SECRET` | secret | Enables the GitHub webhook route |
| `GITHUB_RESOURCE_ALLOWLIST` | var | JSON tenant → owned `github:owner/repository` resources; empty/invalid denies new subscriptions |
| `TENANT_APEX_DOMAIN` | var/optional | Tenant subdomain cross-check |
| `SIGNAL_ATTRIBUTE_ALLOWLIST` | var | CSV of accepted signal attribute keys |
| `APPROVAL_SLA_SECONDS` | var | Default approval SLA |
| `RUN_RETENTION_DAYS` | var | Terminal workflow/agent snapshot TTL |
| `APPROVAL_RETENTION_DAYS` | var | Decided approval TTL |
| `THREAD_RETENTION_DAYS` | var | Inactive thread/message TTL |
| `NOTIFICATION_RETENTION_DAYS` | var | Terminal inbox row TTL |
| `THREAD_STATE_RETENTION_DAYS` | var | Signal state and goal TTL |
| `SCHEDULE_TRIGGER_RETENTION_DAYS` | var | Schedule fire-history TTL |

Replace `MODEL_ID=provider/model` with the provider and model ID configured for
your Mastra deployment. Set `MODEL_BASE_URL` when that provider uses a custom
OpenAI-compatible endpoint. Configure provider-specific model behavior when
constructing the guarded agent; its `generate()` and `stream()` call options
remain deliberately narrow.

## Security properties to preserve

- Mint run, thread, and resource IDs after authentication. Never accept them in
  a create body.
- Reach thread Durable Objects only through `createThreadTopology()`. It stamps
  the internal tenant and execution-principal headers and checks ownership
  before addressing the namespace.
- Declare every automated entry the agent should accept in
  `allowedAutomation`. Enabling a duty is not enough, and an omitted list
  denies all automated entry.
- Keep approval grants server-derived through `approvalGrantProvider()`. Do not
  copy grants from a decision body, schedule row, model output, or signal.
- Keep agent resume behind `ApprovalService.decide()`. A public raw resume path
  would bypass capability minting and must remain absent.
- Bind D1 stores per authenticated tenant before constructing request-scoped
  services.
- Allocate every named production tenant through `provisionTenant()` before
  credentials exist; never infer ownership from a customer-supplied slug.
- Keep write connectors foreground-only. Background approval resume is a
  different topology.
- Verify webhooks over raw bytes before parsing and derive tenancy from
  subscription rows.
- Authorize provider subscriptions against a server-owned external-resource
  map before persisting them.
- Use shared stores for cross-isolate limits. The connector uses
  `D1RateLimitStore`; the example signal limiter is isolate-local and must be
  replaced for a hard commercial quota.
- Keep server-authored connector IDs static. Model or client input must never
  choose what approval grants.

## Customize the starter

1. Replace `createStarterAgentModule()` instructions, tools, metadata, and matching allowed roles.
2. Keep every external side effect behind `createConnector()`.
3. Add each connector's actual egress hosts to its permission manifest and pass
   outbound traffic through the connector runtime's guarded `fetch`.
4. Add workflow metadata and committed workflows together; the registration
   assertion fails fast if they drift.
5. Add provider adapters to both the webhook/provider maps and the provider-host
   list, then add their IDs to the subscription allowlist.
6. Replace structured console audit sinks with your queue/SIEM transport.
7. Add a durable run-cap implementation before exposing unattended execution
   commercially.

Every import from Anchorage in `src/` uses a documented package export. The
`check:imports` gate rejects source/dist deep imports and relative reaches into
the sibling packages.

## Verification

Run:

```bash
pnpm --filter anchorage-agent-starter typecheck
pnpm --filter anchorage-agent-starter test
pnpm --filter anchorage-agent-starter build
```

`test` includes a deterministic model smoke, an approval-denial/no-D1-side-effect
assertion, and the public-import boundary check. `build` asks Wrangler to bundle
the complete Worker without deploying.

For a live proof, run the start → stop/restart → decide → status sequence above
with real `MODEL_ID`/`MODEL_API_KEY` values.

## Agent host composition

The starter uses only the public `@proofoftech/flowsafe/agent-host` subpath. The Worker router receives `STARTER_AGENT_META`, while each `StarterThread` constructs its complete module from instance-scoped model, storage, runtime, pub/sub, connector, and database objects.

`createThreadAgentHost()` owns the internal start/status/observe/resume topology. Application code does not author private `/agent/*` Durable Object URLs or keep mutable current-request scope. `createAgentApprovalResumer()` restores the original execution principal from the persisted approval target and delegates generic workflow records to the existing run topology.

### Declare which automation may run the agent

Unattended work arrives as a non-human execution principal, and an agent accepts none of it by default. `STARTER_AGENT_META` names each admitted kind together with the exact entry paths it may arrive on:

```typescript
allowedAutomation: [
  { kind: 'system', entryPaths: ['schedule.fire', 'notification.dispatch'] },
  { kind: 'service', entryPaths: ['signal.notification'] },
],
```

That covers the starter's three automated paths: the schedule tick, the notification dispatch tick, and signal-provider delivery. The guarded agent declares the matching `allowedPrincipalKinds: ['human', 'system', 'service']`, and catalog construction refuses the module if the two ever drift. Removing a kind from either half stops that automation at the host, with no other change needed.
