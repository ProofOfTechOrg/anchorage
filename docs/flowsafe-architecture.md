# Flowsafe architecture

Flowsafe is the durable execution and approval package in Anchorage. It hosts Mastra workflows and agents on Cloudflare, persists state in D1, turns suspensions into reviewer decisions, and re-enters execution with capabilities derived from stored records.

## Component map

```text
browser or API client
        |
        v
Cloudflare Worker
  authentication -> TenantContext -> role and ownership gates
        |
        +--> approval router ----------> tenant-bound approval service
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
        +--> stream router -----------> one hub DO per tenant
        |
        +--> schedule/task/provider routes and scheduled duties
```

The Worker is the public authentication boundary. Durable Objects reassert their own identities so a Worker routing defect is not the only tenant check.

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
- derives workflow and tenant isolation scopes;
- obtains per-leg request context before `createRun()`;
- serializes start and resume per run;
- persists snapshots through the D1 Mastra store;
- publishes authoritative `RunSummary` values at lifecycle boundaries;
- maintains a monotonic resume ordinal through the owning Durable Object's ledger.

`DurableObjectRunner` binds one object to `workflowId:runId`. Every request must name the same values as the object's `id.name`.

The snapshot remains Mastra's workflow state. Flowsafe does not invent a second workflow-state document inside Durable Object storage.

## Approval architecture

`D1ApprovalStoreFactory` creates:

- a branded tenant-bound store for request and run scope;
- a distinct system store for scheduled cross-tenant maintenance.

`createTenantResolver()` runs authentication first, validates the tenant claim, then constructs the bound service. There is no unbound request-scoped store.

The normal host bridge observes a suspension and creates one approval with the server-authored connector ids and attribution. Reviewer mutations use compare-and-swap transitions.

`approvalGrantProvider()` recomputes `breakwater.connectorGrants` on every runtime leg, while `RunnerRuntime` writes `breakwater.connectorExecution`. A workflow approval matches the exact tenant, workflow, run, step, `suspendedAt`, and `resumeCount`. A durable-agent approval also matches Mastra `toolCallId`. Re-suspending the same step creates a distinct approval. Explicit `runScoped` records are the only standing grants.

See [Approval system](approval-system.md) for endpoints, separation of duties, live updates, and recovery.

## Tenant architecture

Flowsafe uses one Worker, database, and set of namespaces for many tenants. Isolation starts from three invariants.

### Run identity

Every run id is minted from authenticated context as `${tenantId}_${uuid}`. The run id scopes:

- the Mastra snapshot;
- the runner Durable Object;
- workflow and tenant connector context;
- approval grant queries;
- run live-stream channel;
- R2 artifact keys;
- terminal-run retention and tenant purge.

Status and resume return 404 for a run not owned by the actor's tenant.

### Bound stores

Tenant-scoped stores carry the tenant at construction and include it in every predicate. Cross-tenant maintenance requires a separately typed system view.

The same pattern is used for approvals and provider subscriptions. Other D1 domains use tenant-minted thread/run ids or exact tenant metadata predicates.

### Restricted tenant charset

Tenant ids match `^[a-z0-9]{3,32}$`. Excluding `_` makes the ownership prefix exact and enables range purge without matching another tenant.

Provision named tenants before issuing credentials. Treat all generated ids as opaque outside enforcement code.

## Thread and memory architecture

Threads and resource ids extend the run-id pattern:

- the server mints them through `TenantContext`;
- public bodies may not name memory ids;
- foreign stored ids return 404;
- `createThreadTopology()` checks ownership before addressing a Durable Object;
- the topology overwrites internal tenant, actor, and role headers;
- `ThreadDurableObject` reconstructs the complete actor and compares the stamped tenant with its own name.

D1-backed memory recall uses the salted ids through Mastra's own memory implementation. See [Agent-memory tenancy](agent-memory-tenancy.md).

## Durable agent architecture

`createFlowsafeDurableAgent()` remains the lower-level compatibility wrapper around Mastra's durable-agent workflow seam. It does not make an arbitrary raw agent guarded.

`@proofoftech/flowsafe/agent-host` is the supported protected host. It validates Breakwater's package-local guarded-handle brand, resolves each agent from a server-owned catalog, and constructs the complete module inside the thread Durable Object. The Worker receives metadata only.

`AgentMeta.requiredPermissions` declares an all-of list of canonical lowercase dotted `Permission` identifiers. Catalog validation rejects a non-array, an empty list, duplicates, and malformed identifiers. Agents that omit the field keep the existing role and automation path.

`ThreadAgentHostOptions.resolvePrincipalPermissions` accepts a server-owned `PrincipalPermissionResolver`. It receives only the trusted `ExecutionPrincipal` and returns a `PrincipalPermissionResolution` containing effective permissions and `policyVersion`. The host calls it on every authorized entry, after the human-role or automated-entry gate succeeds. Missing configuration, resolver failure, and malformed output deny an agent that requires permissions; a role-only agent proceeds without a projection and the failure is audited.

The resolution is projected into the run's derived request context as `breakwater.principalPermissions` on every start and resume leg — an explicit `null` when no resolution exists, so a resume retires a stale persisted value. Breakwater's connector wrapper enforces `PermissionManifest.requiredPermissions` against that projection before its dry-run branch and approval grant, so a valid approval cannot elevate a principal that may not invoke the connector.

Permission authorization audit detail records `requiredPermissions` and `permissionPolicyVersion`. It omits effective permissions and identity-provider groups.

Public starts mint the thread, resource, and run ids after authentication. Status and NDJSON observation recheck the stored agent/thread/run binding and return 404 for foreign or mismatched ids. Stream replay lasts only as long as Mastra's configured cache; authoritative status remains available after replay eviction.

An agent has no public raw-resume route. Approval records persist the original authorized principal, and an approval decision resumes as that principal after re-authorizing it against the current catalog. The host checks a human against the agent's roles and an automated principal against its `allowedAutomation` declaration. It then checks any `requiredPermissions` through the current resolver policy. The reviewer remains the actor on the approval decision event.

After Durable Object eviction, the in-process tool registry is gone while D1 state remains. The agent host validates the memory binding, reconstructs the guarded module, and derives fresh trusted resume context. It then rehydrates Mastra's registries by invoking only Breakwater's reserved RBAC `processInput` hook. Before installation, it restores the complete input, LLM-request, application output, and mandatory output-processor lists for resumed loop execution. Initial application and policy `processInput` hooks do not run again. The host then starts observation and resumes through `RunnerRuntime`.

See [Durable agents](durable-agents.md).

## Signals, goals, schedules, tasks, and providers

These are supported opt-in host domains.

- Signals deliver messages, queues, named signals, state, and notifications through the owned thread Durable Object.
- Goals store Mastra-compatible objectives in the thread-state domain.
- Schedules store workflow or agent targets, claim due fires with CAS, and start them through the correct runtime topology.
- Background tasks persist serialized workflow/task state per tenant and execute through a tenant Durable Object manager.
- Signal providers store subscriptions in D1, verify webhooks at the Worker, poll through per-tenant alarms, and deliver through the thread topology.

Each public router repeats the same order: resolve identity, authorize role, verify tenant ownership, validate/cap untrusted input, apply a tenant budget where configured, audit, then forward.

None of these paths can approve a connector. The approval dashboard remains the capability decision surface.

## Live architecture

Live streaming is optional:

- one `HubDurableObject` per tenant fans out approval mutations and presence;
- the runner Durable Object broadcasts whole authoritative run summaries;
- an authenticated REST route mints a short-lived HMAC stream ticket;
- the Worker verifies the ticket and forwards to a topology-bound object;
- tickets carry addressing only;
- polling remains fallback and reconciliation.

Without both a hub binding and ticket secret, the same Worker remains poll-only.

## Retention and offboarding

Flowsafe differentiates terminal data, idle data, and standing configuration:

| Data | Lifecycle |
| --- | --- |
| Workflow snapshots | Terminal-only TTL |
| Approvals | Approved/rejected-only TTL |
| Threads and messages | Optional idle-thread TTL |
| Notifications | Optional terminal TTL |
| Thread state and goals | Optional updated-time TTL |
| Schedule trigger history | Optional fire-time TTL |
| Background tasks | Terminal-state TTL |
| Resources, schedules, subscriptions | Offboarding only |
| R2 artifacts | Paired with snapshot deletion and offboarding |

`purgeTenant()` removes adopted state across all statuses after credentials have been revoked. The schema guard tests require each adopted Mastra domain to declare its offboarding and retention treatment.

## Host composition

`createFlowsafeWorker()` owns the common:

- health, approval, run, and stream route ordering;
- agent catalog routing between deployment `preRoutes` and other optional feature routers;
- tenant resolver construction;
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
