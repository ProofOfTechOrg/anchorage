# @proofoftech/flowsafe

Durable approvals and tenant-safe long-running execution for Mastra on Cloudflare.

Flowsafe runs Mastra workflows and agents through Cloudflare Durable Objects, stores their snapshots in D1, turns suspensions into human approval requests, and derives connector grants from stored decisions when a run resumes.

[Documentation](https://github.com/ProofOfTechOrg/anchorage/tree/main/docs) · [Getting started](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/getting-started.md) · [API reference](https://proofoftechorg.github.io/anchorage/) · [Live demo](https://anchorage.proofoftech.org/)

## Install

```bash
npm install @mastra/core@^1.50.0 @proofoftech/flowsafe
```

Install `@proofoftech/breakwater` when resumed steps call approval-protected connectors:

```bash
npm install @proofoftech/breakwater
```

Install React and React DOM only when you import `@proofoftech/flowsafe/approval-ui`.

Compatibility:

- Node.js 22 or later (engine range `>=22`)
- ESM only
- TypeScript `moduleResolution: "NodeNext"`, `"Node16"`, or `"Bundler"`
- `@mastra/core` in the declared `^1.50.0` peer range
- `react` and `react-dom` `>=18 <20` (React 18 or 19) for the optional approval UI
- `@proofoftech/breakwater` `>=0.7.0 <1.0.0` when used

## Choose an export

| Import | Purpose |
| --- | --- |
| `@proofoftech/flowsafe` | Compatibility barrel for approval API, runner, artifacts, and audit export |
| `@proofoftech/flowsafe/agent-host` | Server-only guarded-agent catalogs, authenticated run routes, thread hosting, NDJSON observation, and approval-only resume |
| `@proofoftech/flowsafe/approval-api` | Approval records, service, tenant-bound stores, REST router, grants, SLA, retention, notifications, and stream events |
| `@proofoftech/flowsafe/do-runner` | Durable Object runner, D1 storage, run summaries, identities, pub/sub, resume ledger, retention, and offboarding |
| `@proofoftech/flowsafe/approval-ui` | Styling-library-agnostic React dashboard, DOM-free API client, headless hook, and live transport |
| `@proofoftech/flowsafe/host-kit` | Auth seams, tenant resolver, topologies, run/stream routers, approval bridges, and composed Worker |
| `@proofoftech/flowsafe/host-kit/module` | Import-safe workflow module contract |
| `@proofoftech/flowsafe/artifacts` | R2 artifact store and in-memory test bucket |
| `@proofoftech/flowsafe/audit-export` | Cloudflare Queue producer sink and NDJSON SIEM consumer |
| `@proofoftech/flowsafe/agent-runner` | Runtime-driven Mastra durable agents and restart-safe approval resume |
| `@proofoftech/flowsafe/signals` | Thread signal routes, ingestion router, D1 notification/state domains, dispatch, and client |
| `@proofoftech/flowsafe/signals/client` | DOM-free signal client without Worker code |
| `@proofoftech/flowsafe/goals` | Durable objective router |
| `@proofoftech/flowsafe/schedules` | D1 schedules, tenant facade, reserved-context controls, and CAS tick |
| `@proofoftech/flowsafe/background-tasks` | Tenant task host, D1 task domains, routes, and cleanup |
| `@proofoftech/flowsafe/signal-providers` | Alarm-driven provider host, topology, subscriptions, verified webhooks, and GitHub reference provider |

New host-side and React features are subpath-only. This keeps the root import free of Breakwater, durable-agent-host, and UI dependency graphs.

## Start with the baseline Worker

Copy the [reference deployment](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/flowsafe/deploy). It supplies:

- one Durable Object per run;
- D1-backed Mastra snapshots and approvals;
- bearer-authentication seam and tenant resolver;
- workflow catalog, start, status, and resume routes;
- suspension-to-approval bridge;
- exact suspension-bound breakwater grant derivation;
- role checks and separation of duties;
- optional live approval/run WebSockets;
- cron-owned SLA sweep and retention purge;
- structured audit and optional Queue-to-SIEM export;
- a sample gated workflow.

Replace the example workflow and identity verifier. Keep the trusted host-kit composition and grant provider.

```typescript
import {
  approvalGrantProvider,
} from '@proofoftech/flowsafe/approval-api';
import {
  DurableObjectRunner,
  init,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import { approvalStoreFactoryFor } from '@proofoftech/flowsafe/host-kit';

export class AppRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    const approvals = approvalStoreFactoryFor(env.DB).forTenant(this.tenantId);
    const { createWorkflow, createStep, runtime } = init(env, {
      requestContextForRun: approvalGrantProvider(approvals),
    });

    // Define and commit workflows through the import-swapped factories.

    return runtime;
  }
}
```

`init()` creates D1-backed Mastra storage from the conventional `DB` binding unless you inject storage. Workflow definitions use the same `createWorkflow()` and `createStep()` shape as Mastra.

## Approval lifecycle

When a workflow or durable agent suspends:

1. The authoritative run summary captures `stepPath`, `suspendedAt`, and `resumeCount`.
2. The trusted host bridge creates one open approval in a tenant-bound store.
3. The record's connector IDs and optional Mastra `toolCallId` come from a server-authored suspend payload.
4. A reviewer claims, delegates, approves, or rejects through the service or REST router.
5. A compare-and-swap commits the mutation.
6. An approval re-enters the owning runtime.
7. `approvalGrantProvider()` reads D1 and writes only the matching structured grants into the resumed leg's request context.
8. Another suspension creates a new approval. A completed or failed run ends the loop.

Durable-agent records produce `tool-call` scope. Workflow gates produce exact `suspension` scope. Trusted `runScoped` records produce explicit `run` scope. Grants never travel in public request bodies. A raw or forged resume finds no stored capability and fails at the Breakwater connector gate.

The same durable tool call may retry with its persisted `toolCallId`; a new model tool call requires approval. Legacy records without explicit scope and legacy connector ID arrays fail closed.

### REST surface

The default approval base is `/api/approvals`.

```text
GET  /
GET  /metrics
GET  /:id
POST /:id/claim
POST /:id/delegate
POST /:id/decide
POST /batch/decide
```

The create route is disabled by default. If enabled, it rejects capability, attribution, fingerprint, and resume-target fields.

Queue filters cover status, workflow, run, claimant, requester, strict creation-time bounds, bounded pagination, and reviewer-priority ordering. Batch decisions contain per-record outcomes and preserve all single-record checks.

### Separation of duties

Self-decision is denied by default. A non-exempt actor also cannot approve a later gate after approving an earlier gate that advanced the run. Set a narrow role exemption only for an installation that cannot supply an independent reviewer.

### SLA, notifications, and retention

`sweepSLA()` accepts the system-only cross-tenant store and escalates overdue open records. It is intended for a scheduled invocation and is not an HTTP service method.

`ApprovalNotificationSink` receives creation and escalation events. Failures are contained and audited. `ApprovalStreamSink` publishes mutations to the optional tenant hub.

`purgeExpiredApprovals()` deletes only approved and rejected records past the configured age. Pending, claimed, and escalated records remain live at any age.

Read the [approval-system guide](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/approval-system.md) for exact grant binding and recovery behavior.

## Approval dashboard

The optional UI exports a plain-HTML default and an injected component contract:

```typescript
import {
  ApprovalApiClient,
  createApprovalDashboard,
} from '@proofoftech/flowsafe/approval-ui';

const client = new ApprovalApiClient({
  headers: {
    authorization: `Bearer ${token}`,
  },
});

createApprovalDashboard(document.getElementById('root')!, {
  client,
});
```

The package includes:

- queue, detail, metrics, filter, and batch-decision views;
- `useApprovalDashboard()` for headless composition;
- `ApprovalUIProvider` and `ApprovalUIComponents` slots;
- optional checkbox, select, toast, and presence slots;
- optimistic decisions with conflict reporting;
- polling reconciliation;
- WebSocket live merge and liveness heartbeat;
- `ApprovalApiClient` without a DOM dependency.

Flowsafe has no Astryx or CSS dependency. The repository showcase injects Astryx in the application only.

## Multi-tenancy

Flowsafe applies three base invariants.

### The run id carries the tenant

The host mints `${tenantId}_${uuid}` from authenticated context. `RunnerRuntime.start()` requires a run id and has no generation fallback. Status and resume return 404 for a foreign run.

The run id scopes the snapshot, Durable Object, connector isolation context, approval lookup, and artifact path.

### Stores bind at construction

`D1ApprovalStoreFactory.forTenant()` returns a branded tenant-bound store. Request handlers cannot pass an unbound or system store where a tenant store is required.

Construct the store after authentication through `createTenantResolver()`. The system store exists only for scheduled cross-tenant maintenance.

### The tenant charset makes prefix ownership exact

Tenant ids match `^[a-z0-9]{3,32}$`. This excludes the `_` run and memory delimiter, making prefix ownership and D1 range deletion exact.

Provision each named tenant before issuing credentials. Do not infer customer identity from a slug without the registry.

## Memory, retention, and offboarding

Mastra memory ids are caller-chosen by default. In a multi-tenant host:

- mint threads with `TenantContext.newThreadId()`;
- mint resources with `TenantContext.newResourceId(key)`;
- reject client bodies that name memory ids;
- return 404 for foreign stored ids;
- reach a thread Durable Object only through `createThreadTopology()`.

Retention helpers cover:

- terminal workflow snapshots;
- decided approvals;
- idle threads and their messages;
- terminal notifications;
- thread state and goals;
- schedule trigger history;
- terminal background tasks.

Schedules, resources/working memory, and provider subscriptions are standing state and delete at offboarding, not by default TTL.

`purgeTenant()` deletes every adopted tenant domain. Pass the artifact store so R2 objects delete with their owning run rows.

## Artifacts

`R2ArtifactStore` uses keys shaped as:

```text
[prefix/]workflowId/runId/name
```

Workflow and run ids use the runner's path-safe pattern. Artifact names are validated segment by segment. `deleteRun()` pairs with terminal-run retention; tenant purge removes the remaining tenant artifacts.

`InMemoryArtifactBucket` is available for tests and offline demos.

## Audit export

`queueAuditSink(queue)` turns the shared audit contract into Cloudflare Queue messages. `createAuditQueueConsumer()` batches records as NDJSON, posts them to the configured collector, acknowledges the batch on 2xx, and retries otherwise.

The types are structural and do not require `@cloudflare/workers-types`. A transform seam can map internal events to your SIEM schema.

## Live streaming

Live updates are opt-in behind a `HUB` Durable Object binding and `STREAM_TICKET_SECRET`.

An authenticated REST request mints a short-lived HMAC ticket. The browser presents it when opening a queue or run WebSocket. The Worker is the only verifier, and each Durable Object rebinds the addressed tenant or run through its own identity.

Tickets carry addressing information only. They contain no connector grant or approval decision. Polling remains available as fallback and reconciliation.

## Long-running agents

The following surfaces are supported and opt-in: they are tested and covered by package compatibility guarantees, but the host must explicitly wire the required routes, bindings, storage domains, or scheduled duties.

### Durable agents

Use `@proofoftech/flowsafe/agent-host` for a public protected surface. `createAgentRouter()` lists server-owned metadata and exposes authenticated start, status, and newline-delimited JSON observation routes. The router mints every ID and rejects trusted context and execution overrides. Authenticated human starts must satisfy both the global start roles and the selected agent's `allowedRoles`. Automated entry uses trusted host paths instead: it never consults human roles and requires a matching principal kind and entry path in the selected agent's `allowedAutomation`.

Agents can add `AgentMeta.requiredPermissions` as an all-of list of `Permission` identifiers. Each identifier uses canonical lowercase dotted form. Catalog construction rejects a non-array, an empty list, duplicates, and malformed identifiers.

Configure `ThreadAgentHostOptions.resolvePrincipalPermissions` with a server-owned `PrincipalPermissionResolver`. The resolver receives only the trusted `ExecutionPrincipal` and returns a `PrincipalPermissionResolution` containing effective permissions and `policyVersion`. The host evaluates permissions after the existing human-role or automated-entry gate. A missing resolver, a resolver failure, or malformed output denies an agent that requires permissions.

Agents that use only `allowedRoles` and `allowedAutomation` do not invoke or require the resolver. Permission authorization audit detail records `requiredPermissions` and `permissionPolicyVersion`; it does not record effective permissions or identity-provider groups.

`createThreadAgentHost()` validates Breakwater's guarded-handle brand before it registers the agent with Mastra. It persists the thread/agent binding and original run principal, so eviction recovery and approval resume cannot switch agents or actors.

Agent resume is approval-only. `createAgentApprovalResumer()` rejects legacy agent targets without the original principal, then rechecks that principal against the current catalog. A human must still satisfy the selected agent's `allowedRoles`. An automated principal's kind must still appear in `allowedAutomation`; `approval.resume` is implied for a declared kind. The thread host then enforces any `requiredPermissions` through the current resolver policy. The resumer delegates non-agent workflow records to the existing resume function.

`createFlowsafeDurableAgent()` remains the lower-level compatibility API. It routes Mastra's durable-agent workflow through `RunnerRuntime`, but it does not guard an arbitrary raw agent. Use `agent-host` when an HTTP surface must enforce catalog and Breakwater invariants.

Agent event replay lasts only as long as the configured Mastra cache. The default in-memory cache does not survive process restart. A 409 stream response means the client must read the authoritative status route.

### Signals and notifications

`createThreadSignalRoutes()` hosts message, queue, signal, state, and notification delivery in the thread Durable Object. `createSignalRouter()` is the Worker trust boundary: authenticate, authorize, ownership-check, cap, parse, reject memory ids, allowlist attributes, rate-limit, audit, then forward.

`D1NotificationsStorage` and `D1ThreadStateStorage` mirror Mastra's in-memory domains on D1. `SignalClient` is available from the browser-safe `signals/client` export.

### Goals

`createObjectiveRouter()` manages one durable objective per thread through Mastra's own goal read/write shape. Writes are role-gated, size- and run-budget-capped, and audited.

### Schedules

`D1SchedulesStorage` implements the schedule domain. `createScheduleRouter()` provides tenant-filtered CRUD and read-only trigger history. `createScheduleTick()` claims due fires with CAS, protects reserved context keys, checks unattended-run caps, mints a tenant run id, and starts a workflow or agent through the correct topology.

### Background tasks

`createBackgroundTaskD1Domains()` supplies serialized workflow and tenant task storage. `BackgroundTaskHost` owns one manager per tenant Durable Object, while `createBackgroundTaskRoutes()` exposes its host surface. Terminal task cleanup is opt-in through the composed Worker.

### Signal providers

Flowsafe providers route polling and webhook deliveries through the thread topology instead of Mastra's in-process registry.

`SignalProviderHost` runs one alarm-driven host per tenant. `D1SubscriptionStoreFactory` persists subscriptions. Human-only subscription routes reconcile provider alarms after each committed mutation. Webhook routes verify raw bytes before parsing, derive the tenant from the stored subscription, apply a provider-and-tenant rate cap, and bound forgery audit.

`githubSignalProvider()` is the reference provider.

Complete wiring is in the [durable-agents guide](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/durable-agents.md) and [advanced starter](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/agent-starter).

## Deployment and operations

The composed `createFlowsafeWorker()` owns the shared route, scheduled, and Queue pipeline. Hosts inject workflows, identity verification, topology-backed optional routers, budget wrappers, notification transport, artifact pairing, schedule tick, and extra purge duties.

Use separate cron expressions for approval SLA sweep, retention purge, and schedule fire. Provider polling uses per-tenant Durable Object alarms.

Read:

- [Deployment reference](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/deployment-reference.md)
- [Operations runbook](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/operations-runbook.md)
- [Security threat model](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/security-threat-model.md)

## Security boundaries

Flowsafe does not authenticate users, execute an identity-provider protocol, provide a network sandbox, or decide which business action is safe. It supplies enforcement seams after your host has verified identity and selected policy.

Critical host obligations:

- derive tenants and actors from verified credentials;
- mint run, thread, resource, schedule, and subscription identities server-side;
- keep connector lists and agent resume targets server-authored;
- persist the original authorized agent requester and resume execution as that principal;
- expose decisions only through the approval path;
- configure shared stores for cross-isolate connector budgets;
- keep raw Durable Object namespaces behind the exported topologies;
- schedule every enabled domain's retention and offboarding path;
- protect model, provider, stream-ticket, webhook, and SIEM secrets separately.

## Verification

The repository's deterministic workerd spike proves suspend, process restart, resume, forged-resume denial, repeated gates, tenant isolation, live streaming, durable-agent recovery, signals, goals, schedules, providers, notifications, and background-task restart.

```bash
pnpm --filter @proofoftech/flowsafe spike:verify
```

The optional live-model proof requires `SPIKE_LLM_MODEL_ID` and `SPIKE_LLM_API_KEY`:

```bash
pnpm --filter @proofoftech/flowsafe spike:verify:llm
```

## License

Apache-2.0.
