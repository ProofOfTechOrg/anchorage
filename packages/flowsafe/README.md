# @proofoftech/flowsafe

Durable approvals and physically isolated long-running execution for Mastra on Cloudflare.

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
- `@proofoftech/breakwater` `>=0.9.0 <1.0.0` when used
- host-provided Wrangler `>=4.118 <5` for the optional `flowsafe-provision` CLI

## Choose an export

| Import | Purpose |
| --- | --- |
| `@proofoftech/flowsafe` | Compatibility barrel for approval API, runner, artifacts, and audit export |
| `@proofoftech/flowsafe/agent-host` | Server-only guarded-agent catalogs, authenticated run routes, thread hosting, NDJSON observation, and approval-only resume |
| `@proofoftech/flowsafe/approval-api` | Approval records, actor resolver, service, deployment store, REST router, grants, SLA, retention, notifications, and stream events |
| `@proofoftech/flowsafe/do-runner` | Durable Object runner, D1 storage, deployment sentinel, run summaries, snapshot provenance, identities, pub/sub, and retention |
| `@proofoftech/flowsafe/approval-ui` | Styling-library-agnostic React dashboard, DOM-free API client, headless hook, and live transport |
| `@proofoftech/flowsafe/host-kit` | Authenticator and verifier seams, topologies, run/stream routers, approval bridges, and composed Worker |
| `@proofoftech/flowsafe/host-kit/module` | Import-safe workflow module contract |
| `@proofoftech/flowsafe/artifacts` | R2 artifact store and in-memory test bucket |
| `@proofoftech/flowsafe/audit-export` | Cloudflare Queue producer sink and NDJSON SIEM consumer |
| `@proofoftech/flowsafe/agent-runner` | Runtime-driven Mastra durable agents and restart-safe approval resume |
| `@proofoftech/flowsafe/signals` | Thread signal routes, ingestion router, D1 notification/state domains, dispatch, and client |
| `@proofoftech/flowsafe/signals/client` | DOM-free signal client without Worker code |
| `@proofoftech/flowsafe/goals` | Durable objective router |
| `@proofoftech/flowsafe/schedules` | D1 schedules, deployment router, reserved-context controls, and CAS tick |
| `@proofoftech/flowsafe/background-tasks` | Deployment task host, D1 task domains, routes, and cleanup |
| `@proofoftech/flowsafe/signal-providers` | Alarm-driven provider host, topology, subscriptions, verified webhooks, and GitHub reference provider |

New host-side and React features are subpath-only. This keeps the root import free of Breakwater, durable-agent-host, and UI dependency graphs.

## Start with the baseline Worker

Copy the [reference deployment](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/flowsafe/deploy). It supplies:

- one Durable Object per run;
- D1-backed Mastra snapshots and approvals;
- bearer-authentication seam and actor resolver;
- strict D1 deployment sentinel plus authenticated Worker-to-Durable-Object calls;
- workflow catalog, start, status, and resume routes;
- suspension-to-approval bridge;
- exact suspension-bound breakwater grant derivation;
- role checks and separation of duties;
- optional live approval/run WebSockets;
- alarm-owned SLA sweep and retention purge;
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
  protected runOwnership(env: Env) {
    return approvalStoreFactoryFor(env.DB).resources();
  }

  protected build(env: Env): RunnerRuntime {
    const approvals = approvalStoreFactoryFor(env.DB).store();
    const { createWorkflow, createStep, runtime } = init(env, {
      requestContextForRun: approvalGrantProvider(approvals),
    });

    // Define and commit workflows through the import-swapped factories.

    return runtime;
  }
}
```

`init()` creates D1-backed Mastra storage from the conventional `DB` binding unless you inject storage. Workflow definitions use the same `createWorkflow()` and `createStep()` shape as Mastra. Flowsafe pins `@mastra/cloudflare-d1` 1.1.1 so the minimum supported `@mastra/core` 1.50.0 peer can import and bundle without a consumer override.

If the deployment uses a table prefix, pass one shared constant to storage and host maintenance:

```typescript
const storageTablePrefix = 'agent_';

const { runtime } = init(env, { tablePrefix: storageTablePrefix });
const worker = createFlowsafeWorker({
  ...workerConfig,
  storageTablePrefix,
});
```

The prefix may be empty or must start with an ASCII letter or underscore and continue with ASCII letters, numbers, or underscores. It can contain at most 39 characters because Mastra limits final table identifiers to 63 characters and `mastra_workflow_snapshot` uses the remaining 24. All six exported low-level purge functions validate the same contract before preparing D1 statements. `storageTablePrefix` is not auto-discovered and must match the `tablePrefix` used by `init()` or `createD1Storage()`.

## Approval lifecycle

When a workflow or durable agent suspends:

1. The authoritative run summary captures `stepPath`, `suspendedAt`, and `resumeCount`.
2. The trusted host bridge creates one open approval in the deployment store.
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

`sweepSLA()` accepts the deployment store and escalates overdue open records. It is intended for a scheduled invocation and is not an HTTP service method.

`ApprovalNotificationSink` receives creation and escalation events. Failures are contained and audited. `ApprovalStreamSink` publishes mutations to the optional deployment hub.

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

## Physical deployment isolation

Flowsafe serves one organization per Worker, D1 database, and set of Durable Object namespaces. It does not provide a pooled multi-tenant mode.

### Deployment identity fails closed

Provisioning writes the same stable tag to the `DEPLOYMENT_TENANT` binding and the singleton `flowsafe_deployment` D1 row. The Worker checks the pair before protected routes and maintenance work. Every Worker topology stamps `DEPLOYMENT_IDENTITY_SECRET` on Durable Object requests; the target compares the credential and validates its own tag and sentinel before reading storage.

Seed the sentinel before application migrations or traffic. First-time provisioning refuses any database that already contains application tables. A missing binding, malformed sentinel schema, non-singleton row set, malformed tag, credential mismatch, or tag mismatch returns `503` at the Worker or prevents Durable Object initialization.

The package ships the `flowsafe-provision` CLI. Wrangler is not a package peer. The CLI resolves a host-provided Wrangler `>=4.118 <5` from the application instead of downloading it at runtime:

```bash
npm install --save-dev "wrangler@>=4.118 <5"
npx flowsafe-provision \
  --database <database> \
  --tag <tag> \
  --remote \
  --config wrangler.jsonc
```

Fleet control planes can import the same fail-closed sentinel implementation from `@proofoftech/flowsafe/deployment-identity-protocol` instead of duplicating its schema or race handling.

### Runtime ids are opaque

The host mints opaque, path-safe run and thread ids. `RunnerRuntime.start()` requires a host-owned run id and has no generation fallback. The id scopes the snapshot, Durable Object, approval lookup, stream address, and artifact path, but it carries no customer identity.

### Stores are deployment-wide

`D1ApprovalStoreFactory.store()` and `D1SubscriptionStoreFactory.store()` return the store for the bound database. Tables and indexes contain no tenant column. Legacy pooled schemas require a fresh database.

`D1ApprovalStoreFactory.resources()` records ownership of server-minted runs, threads, and schedules plus validated host-owned resource keys. Operators and builders can access only their principal's resources; reviewers and viewers can read existing resources; admins can administer them. Inaccessible resources return `404` before role errors.

`deploymentTag` is audit attribution only. It comes from verified infrastructure and never authorizes a request or scopes a query.

## Memory and retention

Mastra memory ids are caller-chosen by default. A Flowsafe host must:

- mint threads with `ActorContext.newThreadId()`;
- validate host-owned resource keys with `ActorContext.resourceIdFromKey(key)`;
- reject client bodies that name memory ids;
- reach a thread Durable Object only through `createThreadTopology()`.

TTL retention helpers cover:

- terminal workflow snapshots;
- decided approvals;
- idle threads and their messages;
- terminal notifications;
- thread state and goals;
- schedule trigger history;
- terminal background tasks.

Schedules, resources, working memory, and provider subscriptions are standing state. The schedule and subscription routes support authorized deletion. Flowsafe does not ship a public resource or permanent thread-deletion route: a host that adds one must remove every authoritative binding and wake source before it calls `ActorContext.releaseResource()` for the thread or resource claim. Run retention and schedule deletion release their ownership claims; idle-thread retention deliberately does not. Deployment decommissioning removes any remainder. TTL retention, explicit teardown, and decommissioning are separate lifecycle mechanisms.

## Artifacts

`R2ArtifactStore` uses keys shaped as:

```text
[prefix/]workflowId/runId/name
```

Workflow and run ids use the runner's path-safe pattern. Artifact names are validated segment by segment. `deleteRun()` pairs with terminal-run retention. Deployment decommissioning removes the remaining bucket data.

Construct the retention store from the current maintenance invocation's environment. A module-scoped Worker configuration can safely retain the factory, but must not retain an invocation-specific R2 binding:

```typescript
const worker = createFlowsafeWorker({
  ...workerConfig,
  artifactStore: (env) => new R2ArtifactStore(env.ARTIFACTS),
});
```

The factory and artifact deletion run inside the snapshot-retention failure boundary. Either failure preserves the snapshot row for retry and does not stop sibling maintenance duties. Omitting the factory or returning `undefined` keeps row-only retention.

`InMemoryArtifactBucket` is available for tests and offline demos.

## Audit export

`queueAuditSink(queue)` turns the shared audit contract into Cloudflare Queue messages. `createAuditQueueConsumer()` batches records as NDJSON, posts them to the configured collector, acknowledges the batch on 2xx, and retries otherwise.

For an externally authored fleet release, do not expose that shared Queue binding to the candidate. Bind the candidate to the trusted state Worker as `AUDIT_PROXY` and enable the matching candidate and trusted-ingress fleet markers. The adapter authenticates with the deployment identity; trusted state supplies canonical tenant, environment, and script attribution before enqueueing. The envelope marks the candidate event itself as untrusted, so its action, decision, resource, and detail remain claims rather than infrastructure attestations.

The types are structural and do not require `@cloudflare/workers-types`. A transform seam can map internal events to your SIEM schema.

## Live streaming

Live updates are opt-in behind a `HUB` Durable Object binding and `STREAM_TICKET_SECRET`.

An authenticated REST request mints a short-lived HMAC ticket. The browser presents it when opening a queue or run WebSocket. The Worker is the only verifier. The singleton hub and each run Durable Object rebind the addressed channel through their own identities.

Tickets carry addressing information only. They contain no connector grant or approval decision. Polling remains available as fallback and reconciliation.

## Long-running agents

The following surfaces are supported and opt-in: they are tested and covered by package compatibility guarantees, but the host must explicitly wire the required routes, bindings, storage domains, or scheduled duties.

### Durable agents

Use `@proofoftech/flowsafe/agent-host` for a public protected surface. `createAgentRouter()` lists server-owned metadata and exposes authenticated start, status, and newline-delimited JSON observation routes. The router mints every ID and rejects trusted context and execution overrides. Authenticated human starts must satisfy both the global start roles and the selected agent's `allowedRoles`. Automated entry uses trusted host paths instead: it never consults human roles and requires a matching principal kind and entry path in the selected agent's `allowedAutomation`.

Agents can add `AgentMeta.requiredPermissions` as an all-of list of `Permission` identifiers. Each identifier uses canonical lowercase dotted form. Catalog construction rejects a non-array, an empty list, duplicates, and malformed identifiers.

Configure `ThreadAgentHostOptions.resolvePrincipalPermissions` with a server-owned `PrincipalPermissionResolver`. The resolver receives only the trusted `ExecutionPrincipal` and returns a `PrincipalPermissionResolution` containing effective permissions and `policyVersion`. The host evaluates permissions after the existing human-role or automated-entry gate. A missing resolver, a resolver failure, or malformed output denies an agent that requires permissions.

A configured resolver runs on every authorized entry, and its resolution is projected into the run's derived request context as `breakwater.principalPermissions` on every start and resume leg (an explicit `null` when no resolution exists). Breakwater connectors that declare `PermissionManifest.requiredPermissions` enforce their own all-of list against that projection before their dry-run branch and approval grant. Agents that use only `allowedRoles` and `allowedAutomation` invoke a configured resolver but do not require it: a failed resolution still starts the run, is audited as `agent.permissions.resolve`, and leaves the projection `null`, so permission-declaring connectors inside that run fail closed.

Permission authorization audit detail records `requiredPermissions` and `permissionPolicyVersion`; it does not record effective permissions or identity-provider groups.

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

`D1SchedulesStorage` implements the schedule domain. `createScheduleRouter()` provides deployment-wide role-gated CRUD and read-only trigger history. `createScheduleTick()` claims due fires with compare-and-swap (CAS), protects reserved context keys, consults the configured unattended-run cap, mints an opaque run id, and starts a workflow or agent through the correct topology. Without the optional cap seam, unattended starts are uncapped.

### Background tasks

`createBackgroundTaskD1Domains()` supplies serialized workflow and deployment task storage. `BackgroundTaskHost` owns one singleton manager Durable Object, while `createBackgroundTaskRoutes()` exposes its host surface. Terminal task cleanup is opt-in through the composed Worker.

### Signal providers

Flowsafe providers route polling and webhook deliveries through the thread topology instead of Mastra's in-process registry.

`SignalProviderHost` runs one alarm-driven host per deployment. `D1SubscriptionStoreFactory` persists subscriptions. External resource ids are opaque, but must be non-empty, contain no ASCII control characters, and fit within 1,024 UTF-8 bytes. Human-only subscription routes reconcile provider alarms after each committed mutation. Webhook routes verify raw bytes before parsing, look up the stored subscription, apply a provider rate cap, and bound forgery audit.

`githubSignalProvider()` is the reference provider.

Complete wiring is in the [durable-agents guide](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/durable-agents.md) and [advanced starter](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/agent-starter).

## Deployment and operations

The composed `createFlowsafeWorker()` owns the shared route and maintenance-duty pipeline. Hosts inject workflows, identity verification, topology-backed optional routers, budget wrappers, notification transport, an invocation-scoped artifact-store factory, the storage table prefix, schedule tick, and extra purge duties.

`createFlowsafeMaintenanceDurableObject()` runs approval service-level agreement (SLA) sweep, retention purge, and optional schedule fire as separate alarm invocations. Provider polling and background recovery use their own Durable Object alarms.

Read:

- [Deployment reference](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/deployment-reference.md)
- [Operations runbook](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/operations-runbook.md)
- [Security threat model](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/security-threat-model.md)

## Security boundaries

Flowsafe does not authenticate users, execute an identity-provider protocol, provide a network sandbox, or decide which business action is safe. It supplies enforcement seams after your host has verified identity and selected policy.

Critical host obligations:

- deploy one Worker, D1 database, and set of Durable Object namespaces per organization;
- seed `flowsafe_deployment` before application migrations, configure `DEPLOYMENT_TENANT`, and keep the values identical;
- set a distinct `DEPLOYMENT_IDENTITY_SECRET` for internal Durable Object calls;
- set a separate `MAINTENANCE_ADMIN_SECRET`, bootstrap the fixed maintenance singleton after deploy, and monitor its alarm status; for externally authored releases, keep that secret only in trusted state and relay short-lived signed capabilities through the candidate;
- derive actors and roles from verified credentials;
- mint run, thread, schedule, and subscription identities server-side, validate host-owned resource keys, and preserve their principal ownership records;
- keep connector lists and agent resume targets server-authored;
- persist the original authorized agent requester and resume execution as that principal;
- expose decisions only through the approval path;
- configure shared stores for cross-isolate connector budgets;
- keep raw Durable Object namespaces behind the exported topologies;
- schedule every enabled domain's retention path and define deployment decommissioning;
- protect model, provider, stream-ticket, webhook, and SIEM secrets separately.

## Verification

The repository's deterministic workerd spike proves suspend, process restart, resume, forged-resume denial, repeated gates, deployment-sentinel mismatch refusal, live streaming, durable-agent recovery, signals, goals, schedules, providers, notifications, and background-task restart. Durable Object conformance suites separately cover wrong and missing internal caller credentials.

```bash
pnpm --filter @proofoftech/flowsafe spike:verify
```

The optional live-model proof requires `SPIKE_LLM_MODEL_ID` and `SPIKE_LLM_API_KEY`:

```bash
pnpm --filter @proofoftech/flowsafe spike:verify:llm
```

## License

Apache-2.0.
