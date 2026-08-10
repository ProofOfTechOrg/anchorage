# Flowsafe architecture

Flowsafe is the durable execution and approval package in Anchorage. It hosts Mastra workflows and agents on Cloudflare, persists state in D1, turns suspensions into reviewer decisions, and re-enters execution with capabilities derived from stored records.

## Component map

```text
browser or API client
        |
        v
Cloudflare Worker
  deployment sentinel -> authentication -> ActorContext -> role/resource gates
        |
        +--> approval router ----------> deployment approval service
        |                                    |
        |                                    +--> D1 approvals
        |                                    +--> notification/audit/stream sinks
        |
        +--> run router --------------> runner topology
        |                                    |
        |                                    +--> one runner DO per run
        |                                              |
        |                                              +--> RunnerRuntime
        |                                              +--> Mastra D1 snapshots
        |
        +--> agent router ------------> thread topology
        |                                    |
        |                                    +--> guarded catalog module
        |                                    +--> principal permission resolver
        |                                    +--> durable agent run/status/stream
        |
        +--> thread routers ----------> thread topology
        |                                    |
        |                                    +--> one thread DO per thread
        |                                              |
        |                                              +--> durable agent
        |                                              +--> signals/inbox/state
        |
        +--> stream router -----------> one singleton hub DO
        |
        +--> schedule/task/provider routes and scheduled duties
```

The Worker is the public authentication boundary. It verifies the infrastructure deployment tag against the D1 sentinel before protected work. Worker topologies authenticate internal Durable Object calls, and each target repeats the sentinel check before building storage or serving a route.

## Public modules

The package root mirrors approval API, do-runner, artifacts, and audit export for compatibility. Other features are subpath-only:

| Area | Modules |
| --- | --- |
| Core workflow approvals | `approval-api`, `do-runner`, `host-kit`, `host-kit/module` |
| UI and live updates | `approval-ui`, hub and stream helpers |
| Data integrations | `artifacts`, `audit-export` |
| Long-running agents | `agent-host`, `agent-runner`, `signals`, `goals`, `schedules`, `background-tasks`, `signal-providers` |

See [API reference map](api-reference.md) for exact import paths.

## Runner and storage

`init(env, options)` creates a `RunnerRuntime` and import-swapped `createWorkflow`/`createStep` factories. Definitions retain Mastra's builder shape; start and resume go through the runtime.

The runtime:

- validates path-safe workflow and run ids;
- requires a caller-minted run id;
- derives workflow scope and connector execution identity;
- obtains per-leg request context before `createRun()`;
- serializes start and resume per run;
- persists snapshots through the D1 Mastra store;
- publishes authoritative `RunSummary` values at lifecycle boundaries;
- persists requester identity, attempt tokens, and monotonic resume ordinals in the authoritative workflow snapshot.

`DurableObjectRunner` binds one object to `workflowId:runId`. Every request must name the same values as the object's `id.name`.

The runtime does not mint `breakwater.isolationScope`. One deployment serves one organization, so connector idempotency and rate limits are deployment-wide. The key remains reserved, and provider-supplied values are dropped.

The snapshot remains Mastra's workflow state. Flowsafe does not invent a second workflow-state document inside Durable Object storage.

## Approval architecture

`D1ApprovalStoreFactory.store()` returns one memoized store for the deployment database. Request routes, runtime grant derivation, and scheduled maintenance share that store.

`createActorResolver()` runs authentication first, validates the actor, then constructs the service lazily. Actor claims contain `id` and `role`, not tenant identity.

The normal host bridge observes a suspension and creates one approval with the server-authored connector ids and attribution. Reviewer mutations use compare-and-swap transitions.

`approvalGrantProvider()` recomputes `breakwater.connectorGrants` on every runtime leg, while `RunnerRuntime` writes `breakwater.connectorExecution`. A workflow approval matches the exact workflow, run, step, `suspendedAt`, and `resumeCount`. A durable-agent approval also matches Mastra `toolCallId`. Re-suspending the same step creates a distinct approval. Explicit `runScoped` records are the only standing grants.

See [Approval system](approval-system.md) for endpoints, separation of duties, live updates, and recovery.

## Deployment isolation architecture

Flowsafe uses one Worker, D1 database, and set of Durable Object namespaces per organization. Pooled tenant storage is unsupported.

### Provisioning sentinel

Provisioning supplies one stable organization tag in two independent places:

- the `DEPLOYMENT_TENANT` Worker variable
- the singleton `flowsafe_deployment.tenant_tag` D1 row

`seedDeploymentIdentity()` creates and verifies the strict singleton table before application migrations or traffic. First-time seeding refuses a database that already contains application tables. `ensureDeploymentIdentity()` validates the environment-to-D1 pair at Worker entry points.

Every Worker topology stamps a deployment-specific `DEPLOYMENT_IDENTITY_SECRET` on its Durable Object requests. A production object compares that credential in constant time, then `verifyDurableObjectDeploymentIdentity()` validates the target environment-to-D1 pair. This prevents a cross-script namespace binding from reaching a correctly provisioned object in another deployment. Alarms validate the target pair without a caller credential.

Missing configuration, a missing sentinel, malformed values, and mismatches fail closed. The tag is available as `deploymentTag` for audit attribution only. It never authorizes a request or scopes a query.

### Opaque runtime identity

The host mints opaque path-safe run and thread ids. A run id scopes the Mastra snapshot, runner Durable Object, approval queries, live channel, R2 keys, and retention. It contains no customer identity.

### Deployment stores

Approvals, subscriptions, schedules, tasks, notifications, and Mastra state use deployment-wide tables without tenant predicates. Sentinel provisioning refuses every unowned database with pre-existing application tables, including pooled schemas with `tenant_id`. Upgrade by provisioning a fresh per-organization database.

## Thread and memory architecture

Threads and resource ids use the shared path-safe identity contract:

- the server mints thread ids and validates trusted host business keys as resource ids through `ActorContext`;
- public bodies may not name memory ids;
- `flowsafe_resource_owners` records the creating human or automated principal for each run, thread, resource, and schedule;
- unattended schedule and signal starts inherit the registered schedule or thread owner rather than their execution principal;
- resource routes return `404` before role errors when the principal cannot access an existing resource;
- `createThreadTopology()` validates the thread id before addressing a Durable Object;
- the topology overwrites the internal execution-principal header and strips retired actor, role, and tenant headers;
- `ThreadDurableObject` reconstructs the complete principal and uses its own `id.name` as the authoritative thread id.

D1-backed memory recall uses those ids through Mastra's own memory implementation. See [Agent memory isolation](agent-memory-isolation.md).

## Durable agent architecture

`createFlowsafeDurableAgent()` remains the lower-level compatibility wrapper around Mastra's durable-agent workflow seam. It does not make an arbitrary raw agent guarded.

`@proofoftech/flowsafe/agent-host` is the supported protected host. It validates Breakwater's package-local guarded-handle brand, resolves each agent from a server-owned catalog, and constructs the complete module inside the thread Durable Object. The Worker receives metadata only.

`AgentMeta.requiredPermissions` declares an all-of list of canonical lowercase dotted `Permission` identifiers. Catalog validation rejects a non-array, an empty list, duplicates, and malformed identifiers. Agents that omit the field keep the existing role and automation path.

`ThreadAgentHostOptions.resolvePrincipalPermissions` accepts a server-owned `PrincipalPermissionResolver`. It receives only the trusted `ExecutionPrincipal` and returns a `PrincipalPermissionResolution` containing effective permissions and `policyVersion`. The host calls it on every authorized entry, after the human-role or automated-entry gate succeeds. Missing configuration, resolver failure, and malformed output deny an agent that requires permissions; a role-only agent proceeds without a projection and the failure is audited.

The resolution is projected into the run's derived request context as `breakwater.principalPermissions` on every start and resume leg — an explicit `null` when no resolution exists, so a resume retires a stale persisted value. Breakwater's connector wrapper enforces `PermissionManifest.requiredPermissions` against that projection before its dry-run branch and approval grant, so a valid approval cannot elevate a principal that may not invoke the connector.

Permission authorization audit detail records `requiredPermissions` and `permissionPolicyVersion`. It omits effective permissions and identity-provider groups.

Public starts mint the thread and run ids after authentication and derive the resource id from a validated host-owned key. Status and NDJSON observation recheck the stored agent/thread/run binding and return 404 for foreign or mismatched ids. Stream replay lasts only as long as Mastra's configured cache; authoritative status remains available after replay eviction.

An agent has no public raw-resume route. Approval records persist the original authorized principal, and an approval decision resumes as that principal after re-authorizing it against the current catalog. The host checks a human against the agent's roles and an automated principal against its `allowedAutomation` declaration. It then checks any `requiredPermissions` through the current resolver policy. The reviewer remains the actor on the approval decision event.

After Durable Object eviction, the in-process tool registry is gone while D1 state remains. The agent host validates the memory binding, reconstructs the guarded module, and derives fresh trusted resume context. It then rehydrates Mastra's registries by invoking only Breakwater's reserved RBAC `processInput` hook. Before installation, it restores the complete input, LLM-request, application output, and mandatory output-processor lists for resumed loop execution. Initial application and policy `processInput` hooks do not run again. The host then starts observation and resumes through `RunnerRuntime`.

See [Durable agents](durable-agents.md).

## Signals, goals, schedules, tasks, and providers

These are supported opt-in host domains.

- Signals deliver messages, queues, named signals, state, and notifications through the owned thread Durable Object.
- Goals store Mastra-compatible objectives in the thread-state domain.
- Schedules store workflow or agent targets, claim due fires with CAS, and start them through the correct runtime topology.
- Background tasks persist serialized workflow/task state and execute through one deployment manager Durable Object.
- Signal providers store subscriptions in D1, verify webhooks at the Worker, poll through one provider-host alarm, and deliver through the thread topology.

Each public router repeats the same order: resolve identity, resolve the resource, authorize the role, validate and cap untrusted input, apply the deployment budget where configured, audit, then forward.

None of these paths can approve a connector. The approval dashboard remains the capability decision surface.

## Live architecture

Live streaming is optional:

- one singleton `HubDurableObject` fans out approval mutations and presence;
- the runner Durable Object broadcasts whole authoritative run summaries;
- an authenticated REST route mints a short-lived HMAC stream ticket;
- the Worker verifies the ticket and forwards to a topology-bound object;
- tickets carry addressing only;
- polling remains fallback and reconciliation.

Without both a hub binding and ticket secret, the same Worker remains poll-only.

## Retention and decommissioning

Flowsafe uses three distinct lifecycle mechanisms: TTL purges terminal or idle data, authorized domain operations delete standing records, and deployment decommissioning deletes the remaining physical resource set.

| Data | Lifecycle |
| --- | --- |
| Workflow snapshots | Terminal-only TTL |
| Approvals | Approved/rejected-only TTL |
| Threads and messages | Optional idle-thread TTL |
| Notifications | Optional terminal TTL |
| Thread state and goals | Optional updated-time TTL |
| Schedule trigger history | Optional fire-time TTL |
| Background tasks | Terminal-state TTL |
| Resources and working memory | Explicit host teardown or deployment decommissioning |
| Schedules and subscriptions | Authorized deletion or deployment decommissioning |
| `flowsafe_resource_owners` | Run retention and schedule deletion release their claims. Thread and resource claims require explicit host teardown or deployment decommissioning |
| R2 artifacts | Paired with snapshot deletion or deployment decommissioning |

Decommission a deployment by revoking credentials and traffic, exporting required records, deleting the Worker, then deleting its D1 database, Durable Object namespaces, R2 bucket, queues, and secrets. There is no in-database organization purge because the database itself is the isolation boundary.

Idle-thread retention deletes Mastra thread and message rows only. It deliberately preserves the thread and resource ownership claims because agent bindings, schedules, subscriptions, goals, or other standing wake sources can still address that thread. A host that removes those standing records must delete the authoritative records first, then release the thread and resource claims through `ActorContext.releaseResource()`.

## Host composition

`createFlowsafeWorker()` owns the common:

- health, approval, run, and stream route ordering;
- agent catalog routing between deployment `preRoutes` and other optional feature routers;
- actor resolver construction;
- suspension-to-approval and resume-to-requeue bridges;
- separate SLA, purge, and optional schedule-tick cron dispatch;
- Queue consumer.

Hosts inject identity verification, workflow and agent metadata, optional feature routers, approval-resume composition, budget wrappers, notification transport, artifact store, schedule tick, and deployment-owned purge duties.

The [baseline deployment](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/flowsafe/deploy) uses only core workflows and approvals. The [advanced starter](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/agent-starter) composes the long-running agent features.

## Runtime dependencies

- Mastra for workflows, agents, memory, goals, signals, and storage contracts
- Cloudflare Workers and SQLite Durable Objects for hosting and serialization
- D1 for snapshots and flowsafe-owned domains
- Optional R2 for artifacts
- Optional Cloudflare Queues for SIEM export
- Optional React 18 or 19 for the approval UI

Flowsafe uses structural Cloudflare interfaces in the published library, so consumers do not inherit a hard `@cloudflare/workers-types` dependency.
