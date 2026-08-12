# Deployment reference

Anchorage is a library, not a hosted control plane. Deploy one uniquely named Worker, D1 database, and set of Durable Object namespaces for each organization. The Worker script name is the Durable Object namespace boundary: replace the template's `replace-me` segment with the deployment tag and never reuse that script name for another organization. You also own optional R2 and Queue resources, the identity verifier, policies, maintenance schedules, and the provisioning system that keeps every resource set one-to-one.

Choose one starting point:

| Starting point | Use it for |
| --- | --- |
| [`packages/flowsafe/deploy/`](../packages/flowsafe/deploy/README.md) | Workflow execution, approvals, live queue/run updates, retention, and audit export |
| [`packages/agent-starter/`](../packages/agent-starter/README.md) | The baseline plus durable agents, threads, memory, signals, goals, schedules, background tasks, and signal providers |

The baseline is intentionally smaller. Advanced features are supported and opt-in: they are tested and covered by package compatibility guarantees, but the host must explicitly wire the required routes, bindings, storage domains, or scheduled duties.

## Cloudflare compatibility

Use the Worker runtime with `nodejs_compat`, D1, and SQLite-backed Durable Objects. Treat Durable Object migration tags as append-only. Add a new migration tag when introducing the hub, thread, or provider-host class; never edit an already deployed tag.

The checked-in configurations pin a compatibility date that the repository verifies. Review Cloudflare release notes before changing it.

Do not perform a physical-isolation cutover as an in-place update of a pooled Worker. Allocate a new tag-suffixed script name, database, and Durable Object namespaces, deploy without traffic, verify the sentinel and internal credential, then move the route. Replacing only the D1 binding under an old script leaves the old Durable Object namespaces attached.

## Required baseline bindings

| Binding | Kind | Purpose |
| --- | --- | --- |
| `DB` | D1 | Deployment sentinel, Mastra workflow snapshots, and Flowsafe records |
| `DEPLOYMENT_TENANT` | Variable | Stable provisioning tag that must match the singleton D1 sentinel |
| `DEPLOYMENT_IDENTITY_SECRET` | Secret | Internal Worker-to-Durable-Object caller credential; 32–256 visible ASCII characters |
| `RUNNER` | Durable Object namespace | One runner object per workflow run |
| `MAINTENANCE` | Durable Object namespace | Fixed deployment singleton for sweep, purge, and optional schedule tick |
| `MAINTENANCE_ADMIN_SECRET` | Secret | Control-plane credential for maintenance bootstrap and status; distinct from deployment identity |

Provision the sentinel before application migrations or traffic. Install Wrangler 4 in the application, run `npx flowsafe-provision --database <database> --tag <tag> --remote --config wrangler.jsonc`, then set distinct `DEPLOYMENT_IDENTITY_SECRET` and `MAINTENANCE_ADMIN_SECRET` values with `wrangler secret put`. The CLI is published with Flowsafe. It verifies the exact singleton schema, refuses to re-home an owned database, and refuses to adopt an unowned database that already contains application tables.

The Worker compares `DEPLOYMENT_TENANT` with `flowsafe_deployment.tenant_tag`. Worker topologies stamp the internal credential on every Durable Object fetch, and the target compares it in constant time before reading storage. This additional caller check prevents an external or cross-script namespace binding from reaching another deployment's objects. Alarms validate the target environment and sentinel because they have no caller request.

The runner class extends `DurableObjectRunner`. Its identity is derived from `workflowId:runId`, and it refuses requests that disagree with its own `id.name`. Run ids are opaque, path-safe values minted by the host.

## Optional baseline bindings

| Binding or secret | Kind | Enables |
| --- | --- | --- |
| `HUB` | Durable Object namespace | Deployment-wide live approval fan-out |
| `STREAM_TICKET_SECRET` | Secret | HMAC ticket mint and WebSocket routes; requires `HUB` |
| `AUDIT_QUEUE` | Queue producer | Durable audit export to the shared control-plane consumer |
| R2 bucket chosen by the host | R2 | `R2ArtifactStore` and artifact-aware purge |

Streaming remains poll-only unless both `HUB` and `STREAM_TICKET_SECRET` are present. Use a dedicated ticket secret, not an OAuth, session, or model-provider key.

## Advanced bindings

The advanced starter adds:

| Binding | Kind | Purpose |
| --- | --- | --- |
| `THREAD` | Durable Object namespace | One runtime-driven agent host per opaque thread id |
| `SIGNAL_PROVIDER_HOST` | Durable Object namespace | Singleton alarm-driven provider host |
| `BACKGROUND_TASKS` | Durable Object namespace | Singleton recoverable background-task manager |

Binding names are host choices; these are the names in the advanced starter.
Schedules, notifications, goals, and background tasks share D1 through composed storage domains. No additional database is required. If a high-volume deployment splits domains into separate D1 databases, seed and verify the same deployment tag in every database and decommission the complete resource set together.

## Deployment and actor identity

`createFlowsafeWorker()` takes `buildVerifier(env)`. The baseline uses `parseActorTokens()` and `staticTokenVerifier()` as an inspectable seam. Replace it with verified JWT, OIDC, mTLS, or another mechanism that returns:

```typescript
{
  id: string;
  role: 'admin' | 'builder' | 'operator' | 'reviewer' | 'viewer';
}
```

The authenticated actor identifies a person inside the deployment. Internal `ExecutionPrincipal` values represent either a human actor or an authorized automated principal. Neither chooses the organization or a storage partition. The provisioning tag must match `^[a-z0-9]{3,32}$`; it comes only from `DEPLOYMENT_TENANT` and is used for the sentinel and audit attribution.

`ActorContext` records the owner of every server-minted run, thread, and schedule, plus validated host-owned resource keys, in `flowsafe_resource_owners`. Runs fired by a schedule inherit the schedule owner; runs woken by a signal inherit the thread owner instead of the automated delivery principal. Operators and builders can access only resources owned by their principal. Reviewers and viewers can read existing resources, and admins can administer them. Routes resolve resource access before role errors and return `404` when the principal cannot see the resource.

Do not:

- accept deployment identity from a token, query, body, model result, signal, schedule, or forwarded header;
- use client-selected run, thread, subscription, or schedule ids, or accept a full resource id from an untrusted request;
- bind more than one organization's traffic to the same data-plane resource set;
- expose raw Durable Object namespaces or deployment stores from request scope.

Host routing belongs to the provisioning control plane. It must resolve a hostname to exactly one physical deployment before Flowsafe sees the request.

## Baseline configuration

| Name | Default | Behavior |
| --- | --- | --- |
| `DEPLOYMENT_TENANT` | None | Required provisioning tag. Protected routes return `503` unless it matches the D1 sentinel |
| `DEPLOYMENT_IDENTITY_SECRET` | None | Required internal credential. Worker-to-Durable-Object requests fail before storage unless it matches |
| `MAINTENANCE_ADMIN_SECRET` | None | Required control-plane credential. Maintenance admin routes return `503` when absent or malformed |
| `APPROVAL_ACTOR_TOKENS` | Empty | Static verifier map. Empty means every authenticated route returns 401 |
| `APPROVAL_SLA_SECONDS` | `14400` | SLA assigned to new approval records |
| `APPROVAL_ALLOW_SELF_DECISION` | Unset | Separation of duties enabled. Accepts `true` or a comma-separated role list |
| `RUN_RETENTION_DAYS` | `30` | Age for terminal workflow snapshot purge; `0` means immediate eligibility |
| `APPROVAL_RETENTION_DAYS` | `30` | Age for approved/rejected approval purge |
| `THREAD_RETENTION_DAYS` | Unset | Idle thread and message purge. Unset keeps conversations |

Invalid optional retention variables leave the destructive duty disabled and emit configuration audit rather than selecting a fallback TTL.

The checked-in advanced starter additionally configures:

| Name | Starter value | Behavior |
| --- | --- | --- |
| `THREAD_RETENTION_DAYS` | `90` | Idle thread and message purge |
| `NOTIFICATION_RETENTION_DAYS` | `30` | Terminal notification purge |
| `THREAD_STATE_RETENTION_DAYS` | `90` | Signal state and goal purge |
| `SCHEDULE_TRIGGER_RETENTION_DAYS` | `30` | Schedule fire-history purge |

## Routes

`createFlowsafeWorker()` composes host-supplied routes before its approval and run stages. Mount only the stages you configure.

### Baseline routes

```text
GET  /healthz
GET  /workflows
POST /runs
GET  /runs/:workflowId/:runId
POST /runs/:workflowId/:runId/resume
GET  /api/approvals
GET  /api/approvals/metrics
GET  /api/approvals/:id
POST /api/approvals/:id/claim
POST /api/approvals/:id/decide
POST /api/approvals/:id/delegate
POST /api/approvals/batch/decide
POST /api/stream/ticket
GET  /api/stream/hub
GET  /api/stream/run/:workflowId/:runId
```

The stream routes mount only when streaming is configured. The approval create route is off unless the host explicitly enables its capability-free form.

### Advanced routes

```text
GET    /agents
POST   /agents/:agentId/runs
GET    /agents/:agentId/runs/:threadId/:runId
GET    /agents/:agentId/runs/:threadId/:runId/stream?offset=N
POST   /api/threads/:threadId/message
POST   /api/threads/:threadId/queue
POST   /api/threads/:threadId/signal
POST   /api/threads/:threadId/state
POST   /api/threads/:threadId/notification
PUT    /api/threads/:threadId/goal
GET    /api/threads/:threadId/goal
PATCH  /api/threads/:threadId/goal
DELETE /api/threads/:threadId/goal
... schedule CRUD and trigger-history routes under the configured schedule base
... background-task routes under the configured task base
... subscription CRUD under the configured subscription base
... provider webhook routes under the configured webhook base
```

The agent host has no public resume route. An approval decision resumes the original authorized requester through the persisted `agent-thread` target. The agent event stream uses authenticated newline-delimited JSON with an offset cursor. If its short-lived replay cache is unavailable, the stream returns 409 and the client reads the authoritative status route.

Other route factories accept a `basePath` when the exact public prefix is host-specific. The generated [API reference](https://proofoftechorg.github.io/anchorage/) documents each option.

## Host composition

`createFlowsafeWorker()` owns the common fetch and maintenance-duty pipeline. It verifies deployment identity before protected routes or maintenance administration. Supply deployment-specific behavior through its typed seams:

- `workflows` and `systemPrincipalId`
- `buildVerifier`
- `maintenance` intervals
- `preRoutes` for deployment-specific routes
- `beforeStart` and `beforeResume` for budgets or other final mutation policy
- `buildAgentRouter` for the metadata-only public catalog and run routes
- `buildResumeRun` to compose approval-only agent resume with generic workflow resume
- `notify` for reviewer delivery
- `artifactStore(env)` to build the R2 purger from the current invocation and pair artifact deletion with snapshot retention
- `storageTablePrefix` to target the same prefix-aware D1 tables as runtime storage
- `backgroundTasks` for task cleanup
- `buildSignalRouter`
- `buildObjectiveRouter`
- `buildScheduleRouter`
- `scheduleTick`
- `extraPurgeDuties` for deployment-owned domains

Use the exported router and topology factories rather than recreating their gate order.

## Alarm-driven maintenance

The fixed maintenance Durable Object schedules three independent duties:

1. Approval SLA sweep
2. Retention purge
3. Schedule fire tick, when enabled

Each alarm persists its successor before running exactly one due duty. If several duties are due, the object schedules an immediate follow-up alarm. A termination during one duty cannot starve another duty or break the alarm chain.

Within the purge invocation, each domain is failure-isolated:

- terminal workflow runs, with paired artifacts when configured;
- decided approvals;
- idle threads and their messages;
- terminal notifications;
- thread state and goals;
- schedule trigger history;
- terminal background tasks;
- host-owned extra duties.

Provisioning must authenticate `POST /admin/ensure-maintenance` after deployment. The drift watchdog reads `GET /admin/maintenance-status` and re-arms a missing or stale alarm. Provider polling, background recovery, and notification dispatch also use Durable Object alarms.

## Storage lifecycle

TTL retention, authorized domain deletion, and deployment decommissioning are separate mechanisms. A TTL removes only eligible terminal or idle data. An authorized API operation can delete standing configuration. Decommissioning deletes the physical resource set after traffic and credentials are revoked.

| Domain | Retention rule |
| --- | --- |
| Workflow snapshots | Terminal-only TTL; suspended and running rows stay |
| Approvals | Approved/rejected-only TTL; open rows stay |
| Threads and messages | Opt-in idle-thread TTL; messages delete with the thread |
| Resources and working memory | No TTL; explicit host teardown or deployment decommissioning |
| Notifications | Opt-in terminal-row TTL; pending rows stay |
| Thread state and goals | Opt-in `updatedAt` TTL |
| Schedules | No TTL; authorized deletion or deployment decommissioning |
| Schedule triggers | Opt-in fire-history TTL |
| Background tasks | Terminal-state TTL |
| Provider subscriptions | No TTL; authorized deletion or deployment decommissioning |
| `flowsafe_resource_owners` | Run retention and schedule deletion release their claims. Thread and resource claims require explicit host teardown or deployment decommissioning |
| R2 artifacts | Delete with the owning snapshot purge and deployment decommissioning |

Decommission the whole physical resource set after revoking traffic and credentials. There is no in-database organization purge. Build the retention artifact store from the current invocation's R2 binding and use the same bucket as runtime writes. Artifact deletion precedes snapshot deletion; a factory or deletion failure preserves the row for retry.

When `init()` or `createD1Storage()` receives `tablePrefix`, pass the same value as `storageTablePrefix` to `createFlowsafeWorker()`. The shared rule accepts an empty prefix or requires an ASCII letter or underscore first, followed by ASCII letters, numbers, or underscores. The prefix can contain at most 39 characters so every final Mastra table identifier stays within its 63-character limit. Maintenance applies it to workflow snapshots, threads/messages, background tasks and their snapshots, notifications, thread state, and schedule-trigger history. All six exported low-level purge functions validate the same contract before preparing D1 statements. Approval, resource-owner, deployment-sentinel, subscription, and other fixed-schema tables remain unprefixed. A valid but mismatched value targets a different table family; Flowsafe does not auto-discover it.

Idle-thread retention does not release thread or resource ownership. A thread can still have an agent binding, schedule, subscription, goal, or other standing wake source after its idle memory rows expire. An explicit host teardown must remove every authoritative standing record before it calls `ActorContext.releaseResource()` for the thread and resource claims.

## Egress and process execution

Breakwater's `runtime.fetch` protects only requests sent through that injected fetch. Route compatible SDK transports through it. Enforce socket-level policy with Cloudflare account controls, network architecture, or a separate proxy.

Claude Code and Codex connectors execute on Node, edit the configured workspace, and can run commands allowed by those CLIs. The built-in timeout tears down the inherited POSIX process group or Windows process tree, but it is not a process sandbox. Put them in a dedicated checkout or container, apply filesystem and process boundaries, pass `cwd` from trusted host configuration, and keep human approval enabled. See [Agent CLI connectors](agent-cli-connectors.md).

## Deployment validation

Before exposing traffic:

1. Run repository typecheck, tests, build, docs checks, and workerd spike.
2. Run the starter smoke test against a deterministic model.
3. Verify no route works without authentication except `/healthz` and
   explicitly configured signature-verified webhook routes.
4. Verify forged deployment, actor, role, and retired `x-flowsafe-tenant` headers cannot influence trusted context.
5. Verify a foreign run, thread, agent, or binding mismatch returns 404 without model or connector execution.
6. Confirm no public agent raw-resume route exists.
7. Kill the local Worker while an agent run is suspended, restart it, approve as a different reviewer, and confirm the connector runs once as the original requester.
8. Evict the agent stream cache and confirm the stream returns 409 while status remains available.
9. Verify the maintenance, provider, and background-task alarms emit a success or contained failure event.
10. Confirm Queue retries and dead-letter handling against a failing SIEM endpoint.
11. Confirm retention deletes matching artifacts and leaves live rows.
12. Exercise deployment decommissioning against a non-production resource set and confirm no Worker, route, D1 database, Durable Object namespace, R2 bucket, Queue binding, or secret remains attached.
13. Bind a scratch Worker to a database carrying a different sentinel and confirm protected requests fail with `503` before any application record is read.
14. Bind a scratch Worker namespace to another deployment's Durable Objects and confirm its different internal credential is rejected before object storage is read.

Operational procedures are in [Operations runbook](operations-runbook.md).
