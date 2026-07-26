# Deployment reference

Anchorage is a library, not a hosted control plane. You deploy the Worker, Durable Objects, D1 database, optional R2 bucket and Queue, identity verifier, policies, and maintenance schedules.

Choose one starting point:

| Starting point | Use it for |
| --- | --- |
| [`packages/flowsafe/deploy/`](../packages/flowsafe/deploy/README.md) | Workflow execution, approvals, live queue/run updates, retention, and audit export |
| [`packages/agent-starter/`](../packages/agent-starter/README.md) | The baseline plus durable agents, threads, memory, signals, goals, schedules, background tasks, and signal providers |

The baseline is intentionally smaller. Advanced features are supported and opt-in: they are tested and covered by package compatibility guarantees, but the host must explicitly wire the required routes, bindings, storage domains, or scheduled duties.

## Cloudflare compatibility

Use the Worker runtime with `nodejs_compat`, D1, and SQLite-backed Durable Objects. Treat Durable Object migration tags as append-only. Add a new migration tag when introducing the hub, thread, or provider-host class; never edit an already deployed tag.

The checked-in configurations pin a compatibility date that the repository verifies. Review Cloudflare release notes before changing it.

## Required baseline bindings

| Binding | Kind | Purpose |
| --- | --- | --- |
| `DB` | D1 | Mastra workflow snapshots and flowsafe approval records |
| `RUNNER` | Durable Object namespace | One runner object per workflow run |

The runner class extends `DurableObjectRunner`. Its identity is derived from `workflowId:runId`, and it refuses requests that disagree with its own `id.name`.

## Optional baseline bindings

| Binding or secret | Kind | Enables |
| --- | --- | --- |
| `HUB` | Durable Object namespace | Per-tenant live approval fan-out |
| `STREAM_TICKET_SECRET` | Secret | HMAC ticket mint and WebSocket routes; requires `HUB` |
| `AUDIT_QUEUE` | Queue producer/consumer | Durable audit export |
| `SIEM_ENDPOINT` | Variable | NDJSON collector destination |
| `SIEM_AUTH_HEADER` | Secret | Collector authorization |
| R2 bucket chosen by the host | R2 | `R2ArtifactStore` and artifact-aware purge |

Streaming remains poll-only unless both `HUB` and `STREAM_TICKET_SECRET` are present. Use a dedicated ticket secret, not an OAuth, session, or model-provider key.

## Advanced bindings

The advanced starter adds:

| Binding | Kind | Purpose |
| --- | --- | --- |
| `THREAD` | Durable Object namespace | One runtime-driven agent host per tenant-minted thread |
| `SIGNAL_PROVIDER_HOST` | Durable Object namespace | One alarm-driven provider host per tenant |
| `BACKGROUND_TASKS` | Durable Object namespace | One recoverable background-task manager per tenant |

Binding names are host choices; these are the names in the advanced starter.
Schedules, notifications, goals, and background tasks share D1 through
composed storage domains. No additional database is required, although
high-volume deployments can choose separate databases if they preserve tenant
binding and coordinated offboarding.

## Identity and tenant resolution

`createFlowsafeWorker()` takes `buildVerifier(env)`. The baseline uses `parseActorTokens()` and `staticTokenVerifier()` as an inspectable seam. Replace it with verified JWT, OIDC, mTLS, or another mechanism that returns:

```typescript
{
  id: string;
  role: 'admin' | 'builder' | 'operator' | 'reviewer' | 'viewer';
  tenantId: string;
}
```

The tenant id must match `^[a-z0-9]{3,32}$`. It cannot contain `_`, which is the delimiter used by exact ownership and range-purge operations. `system` is reserved for maintenance attribution. `default` is reserved for a conventional single-tenant host and is not provisionable.

Provision a named tenant before issuing credentials for it. `provisionTenant()` is insert-or-fail, which prevents two customers from sharing one slug.

Do not:

- accept `tenantId` from a query, body, model result, signal, or forwarded header;
- make a token's tenant optional;
- use client-selected run, thread, resource, subscription, or schedule ids;
- create a tenant-bound store before authentication;
- expose a system store from request scope.

For tenant-specific subdomains, wrap the resolver with `withSubdomainCrossCheck()` as defense in depth. The bound store remains the primary isolation boundary.

## Baseline configuration

| Name | Default | Behavior |
| --- | --- | --- |
| `APPROVAL_ACTOR_TOKENS` | Empty | Static verifier map. Empty means every authenticated route returns 401 |
| `APPROVAL_SLA_SECONDS` | `14400` | SLA assigned to new approval records |
| `APPROVAL_ALLOW_SELF_DECISION` | Unset | Separation of duties enabled. Accepts `true` or a comma-separated role list |
| `RUN_RETENTION_DAYS` | `30` | Age for terminal workflow snapshot purge; `0` means immediate eligibility |
| `APPROVAL_RETENTION_DAYS` | `30` | Age for approved/rejected approval purge |
| `THREAD_RETENTION_DAYS` | Unset | Idle thread and message purge. Unset keeps conversations |
| `TENANT_APEX_DOMAIN` | Unset | Optional subdomain-to-token-tenant cross-check |

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

`createFlowsafeWorker()` owns the common fetch, cron, and queue pipeline. Supply deployment-specific behavior through its typed seams:

- `workflows` and `systemActorId`
- `buildVerifier`
- `crons`
- `preRoutes` for deployment-specific routes
- `wrapResolve` for subdomain or additional identity checks
- `wrapStart` and `wrapResume` for budgets
- `buildAgentRouter` for the metadata-only public catalog and run routes
- `buildResumeRun` to compose approval-only agent resume with generic workflow resume
- `notify` for reviewer delivery
- `artifactStore` to pair R2 deletion with snapshot retention
- `backgroundTasks` for task cleanup
- `buildSignalRouter`
- `buildObjectiveRouter`
- `buildScheduleRouter`
- `scheduleTick`
- `extraPurgeDuties` for deployment-owned domains

Use the exported router and topology factories rather than recreating their gate order.

## Scheduled invocations

Use separate cron expressions for workloads with independent availability requirements:

1. Approval SLA sweep
2. Retention purge
3. Schedule fire tick, when enabled

The composed Worker dispatches by exact cron expression. An unrecognized expression runs safe fallback duties and records a configuration error, but exact matching is the supported configuration.

Within the purge invocation, each domain is failure-isolated:

- terminal workflow runs, with paired artifacts when configured;
- decided approvals;
- idle threads and their messages;
- terminal notifications;
- thread state and goals;
- schedule trigger history;
- terminal background tasks;
- host-owned extra duties.

Provider polling uses Durable Object alarms rather than a global cron. Notification dispatch can run from a schedule tick when delayed notification delivery is enabled.

## Storage ownership

| Domain | Retention rule |
| --- | --- |
| Workflow snapshots | Terminal-only TTL; suspended and running rows stay |
| Approvals | Approved/rejected-only TTL; open rows stay |
| Threads and messages | Opt-in idle-thread TTL; messages delete with the thread |
| Resources and working memory | No TTL; delete at tenant offboarding |
| Notifications | Opt-in terminal-row TTL; pending rows stay |
| Thread state and goals | Opt-in `updatedAt` TTL |
| Schedules | No TTL; standing configuration deletes at offboarding |
| Schedule triggers | Opt-in fire-history TTL |
| Background tasks | Terminal-state TTL |
| Provider subscriptions | No TTL; standing configuration deletes at offboarding |
| R2 artifacts | Delete with the owning snapshot purge and tenant offboarding |

Call `purgeTenant()` only after revoking the tenant's credentials and stopping new starts. Pass the same artifact store used for runtime writes.

## Egress and process execution

Breakwater's `runtime.fetch` protects only requests sent through that injected fetch. Route compatible SDK transports through it. Enforce socket-level policy with Cloudflare account controls, network architecture, or a separate proxy.

Claude Code and Codex connectors execute on Node, edit the configured workspace, and can run commands allowed by those CLIs. Put them in a dedicated checkout or container, apply filesystem and process boundaries, pass `cwd` from trusted host configuration, and keep human approval enabled. See [Agent CLI connectors](agent-cli-connectors.md).

## Deployment validation

Before exposing traffic:

1. Run repository typecheck, tests, build, docs checks, and workerd spike.
2. Run the starter smoke test against a deterministic model.
3. Verify no route works without authentication except `/healthz` and
   explicitly configured signature-verified webhook routes.
4. Verify forged tenant, actor, and role headers return 403.
5. Verify a foreign run, thread, agent, or binding mismatch returns 404 without model or connector execution.
6. Confirm no public agent raw-resume route exists.
7. Kill the local Worker while an agent run is suspended, restart it, approve as a different reviewer, and confirm the connector runs once as the original requester.
8. Evict the agent stream cache and confirm the stream returns 409 while status remains available.
9. Verify every configured cron and provider alarm emits a success or contained failure event.
10. Confirm Queue retries and dead-letter handling against a failing SIEM endpoint.
11. Confirm retention deletes matching artifacts and leaves live rows.
12. Exercise tenant offboarding in a non-production database.

Operational procedures are in [Operations runbook](operations-runbook.md).
