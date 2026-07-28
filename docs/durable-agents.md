# Durable agents

Flowsafe can run Mastra durable agents through the same `RunnerRuntime` that drives workflows. This keeps agent legs inside the run-id, request-context, approval-grant, resume-ledger, audit, and retention boundaries instead of creating a second execution path.

This surface is supported and opt-in: it is tested and covered by package compatibility guarantees, but the host must explicitly wire the required routes, bindings, storage domains, or scheduled duties.

Use the advanced starter in [`packages/agent-starter/`](../packages/agent-starter/README.md) for a consumer-sized composition. The baseline Worker in [`packages/flowsafe/deploy/`](../packages/flowsafe/deploy/README.md) remains the smaller workflow-and-approval starting point.

## Runtime topology

```text
authenticated Worker
  |
  +-- run routes ------------> one runner DO per run
  |
  +-- thread topology -------> one thread DO per tenant-minted thread
  |                               |
  |                               +-- runtime-driven durable agent
  |                               +-- message/signal/state/notification routes
  |                               +-- active-run registry and idle wake
  |
  +-- provider topology -----> one provider host DO per tenant
  |
  +-- live routes -----------> one hub DO per tenant
  |
  +-- D1 --------------------> snapshots, memory, inbox, state, schedules,
                                  tasks, subscriptions, approvals
```

Address thread and provider Durable Objects through the exported topologies. Do not forward the inbound request directly to a raw namespace stub.

## Host a guarded agent catalog

Use `@proofoftech/flowsafe/agent-host` for a public agent surface. Each catalog entry couples public metadata to a Breakwater `GuardedAgentHandle`:

```typescript
interface AgentModule {
  meta: {
    id: string;
    title: string;
    description: string;
    allowedRoles?: readonly ApprovalRole[];
  };
  agent: GuardedAgentHandle;
}
```

The Worker receives metadata only. The thread Durable Object constructs the complete module because its model, storage, runtime, pub/sub, connector, and database objects belong to that instance.

Catalog construction rejects path-unsafe or duplicate ids, empty descriptions, invalid role lists, metadata/handle id mismatches, and metadata roles that differ from the guarded handle. An omitted role list uses `RUN_START_ROLES`.

Mount `createAgentRouter()` through `createFlowsafeWorker({ buildAgentRouter })`. It exposes:

| Method and route | Purpose |
| --- | --- |
| `GET /agents` | List the registered metadata and authenticated actor |
| `POST /agents/:agentId/runs` | Mint ids and start a guarded durable run |
| `GET /agents/:agentId/runs/:threadId/:runId` | Read authoritative durable status |
| `GET /agents/:agentId/runs/:threadId/:runId/stream?offset=N` | Observe authenticated newline-delimited JSON events |

Every authenticated role may list agents and inspect same-tenant runs. Starts require both `RUN_START_ROLES` and the agent's effective roles. There is no public agent-resume route.

The start body accepts only `{"prompt":"..."}`. The router caps the raw UTF-8 body at 16,384 bytes, requires non-whitespace prompt content, preserves that content, and rejects ids, trusted context, overrides, unknown fields, and prototype meta-keys.

Each stream line contains the next reconnect cursor and one event. Replay depends on the configured Mastra cache and is not process-restart durable. When the durable run exists but its replay cache does not, the stream route returns 409 and the client must use the status route.

Approval records store an `agent-thread` target with the agent, thread, resource, and original authorized principal. `createAgentApprovalResumer()` rechecks the current catalog roles, reconstructs the guarded module after eviction, and resumes as that original principal. Before resume, the wrapper rebuilds Mastra's local and global run registries from fresh trusted context. It invokes only Breakwater's reserved RBAC `processInput` hook during rehydration, then installs the complete input, LLM-request, and output processor lists for resumed loop execution. It does not replay application or policy `processInput` hooks. An authorization denial stops before registry installation, observation, or tool execution. The reviewer identity remains attached to the approval decision.

## Use the lower-level durable wrapper

`createFlowsafeDurableAgent()` remains available for compatibility. Create an ordinary Mastra `Agent`, then wrap it:

```typescript
import { Agent } from '@mastra/core/agent';
import {
  createFlowsafeDurableAgent,
  DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
} from '@proofoftech/flowsafe/agent-runner';

const baseAgent = new Agent({
  id: 'operations-agent',
  name: 'Operations agent',
  instructions: 'Act only through the supplied connectors.',
  model,
  tools,
});

const durableAgent = createFlowsafeDurableAgent({
  agent: baseAgent,
  runtime,
  threadRuntime: mastra.agentThreadStreamRuntime,
  maxSteps: 12,
});
```

`createFlowsafeDurableAgent()` registers Mastra's `durable-agentic-loop` workflow on the supplied runtime. Its `stream()`, `generate()`, and `prepare()` entry points require a caller-minted run id. The host must mint `${tenantId}_${uuid}`; flowsafe does not fall back to a tenant-less UUID. `prepare()` remains an initial-execution API and runs the full initial processor chain. `resumeViaRuntime()` uses the dedicated registry rehydration behavior described above.

The runtime's pub/sub identity is reused by default. This lets the durable loop, observer, and active-thread signal delivery share one feed inside the thread Durable Object.

This wrapper does not add the guarded-agent brand or catalog authorization to a raw agent. Use `agent-host` for the supported protected public surface. Do not expose inherited `resume()`, `approveToolCall()`, or `declineToolCall()` methods as client routes.

## Mint and protect memory identities

Mastra memory accepts caller-selected thread and resource ids. A multi-tenant host must replace those business ids at its boundary:

```typescript
const threadId = tenant.newThreadId();
const resourceId = tenant.newResourceId(customerKey);
```

The underlying `mintThreadId()`, `mintResourceId()`, `tenantOfMemoryId()`, and `tenantOwnsMemoryId()` helpers are exported from `@proofoftech/flowsafe/do-runner`.

Host rules:

1. Reject bodies that name `threadId` or `resourceId` with `assertNoClientMemoryIds()`.
2. Resolve the authenticated `TenantContext`.
3. Return 404 for a foreign stored id with `requireOwnedMemoryId()`.
4. Address the thread Durable Object through `createThreadTopology()`.
5. Let the topology overwrite `x-flowsafe-tenant`, `x-flowsafe-actor`, `x-flowsafe-role`, and `x-flowsafe-principal` from the resolved context. The Durable Object refuses a request that carries no principal header rather than treating the caller as a human.
6. Have `ThreadDurableObject` reconstruct the actor and verify the stamped tenant against its own `id.name` prefix.

The D1 recall-path tests use one database and the same business key for two tenants. They prove isolated `recall`, `listThreads`, and working memory behavior through Mastra's own memory implementation.

See [Agent-memory tenancy](agent-memory-tenancy.md) for the exact invariants and purge behavior.

## Compose D1 storage domains

`createD1Storage()` accepts injected Mastra composite domains. Add only the features you host:

| Feature | Composition helper | Storage |
| --- | --- | --- |
| Signals, notifications, thread state, goals | `createSignalStorageDomains()` | `mastra_notifications`, `mastra_thread_state` |
| Schedules | `createScheduleStorageDomains()` | `mastra_schedules`, `mastra_schedule_triggers` |
| Background tasks | `createBackgroundTaskD1Domains()` | serialized workflow and tenant-scoped task domains |

The signals helper is injected into do-runner rather than imported by it, which avoids a package cycle. The schedule store mirrors Mastra's schedule contract because the Cloudflare D1 adapter does not ship that domain. Flowsafe owns both signal tables and the subscription table.

When you adopt a storage domain, wire its offboarding and retention duties in the same change. The package schema guard pins that correspondence internally; your host must still schedule the exported purge.

## Host the thread Durable Object

Subclass `ThreadDurableObject`, construct the catalog modules for that instance, and install `createThreadAgentHost()` and `createThreadSignalRoutes()` inside the subclass route. These factories share the asserted `ThreadScope` instead of mutable module or request-global state.

- stamps the runtime pub/sub identity onto the agent before each call;
- serializes delivery into the thread;
- checks whether a run is already active;
- applies active and idle behavior;
- starts an idle run only through the injected `startIdleRun` seam;
- requires a runtime-driven agent before an idle wake;
- mints no approval capability.

The host's idle-start seam must:

1. revalidate tenant ownership;
2. consult the unattended-run cap;
3. mint a fresh tenant-salted run id;
4. start the durable agent through `RunnerRuntime`;
5. persist and report the authoritative run id.

The agent host persists a thread-to-agent binding and per-run principal record in Durable Object storage. It rejects a second simultaneous operation for the same run with 409 instead of replacing the active execution context.

Thread delivery is priority-planned across summaries and individual notifications, remains stable across 100-record chunks, and suppresses summarized high-priority rows while the thread was active.

## Expose signal ingestion

Mount `createSignalRouter()` through `createFlowsafeWorker({ buildSignalRouter })`. The default prefix is `/api/threads`.

| Channel | Route | Purpose |
| --- | --- | --- |
| `message` | `POST /api/threads/:threadId/message` | Send a user-like message |
| `queue` | `POST /api/threads/:threadId/queue` | Queue a message for the agent loop |
| `signal` | `POST /api/threads/:threadId/signal` | Send a named signal |
| `state` | `POST /api/threads/:threadId/state` | Update durable thread state |
| `notification` | `POST /api/threads/:threadId/notification` | Deliver a notification |

The Worker applies this order: authentication, coarse role, thread ownership, byte cap, JSON parse, client-memory-id rejection, attribute-key allowlist, tenant rate cap, audit, then topology forwarding.

Signals are untrusted model input. Core escapes the XML representation, while the route validates tag/attribute names and caps payload size. Apply breakwater input policy to the receiving agent for domain-specific content controls.

`SignalClient` is DOM-free and is also exported from `@proofoftech/flowsafe/signals/client`.

## Add objectives

Mount `createObjectiveRouter()` through `createFlowsafeWorker({ buildObjectiveRouter })`. It exposes:

```text
PUT    /api/threads/:threadId/goal
GET    /api/threads/:threadId/goal
PATCH  /api/threads/:threadId/goal
DELETE /api/threads/:threadId/goal
```

Objectives are standing instructions injected into future turns. The router therefore uses the signal-ingestion trust posture for writes: authenticate, authorize, ownership-check, reject client memory ids, cap size and `maxRuns`, then audit every accepted or post-auth rejected mutation.

The router writes through Mastra's objective helpers into the goal lane of `mastra_thread_state`, so the durable goal step reads the identical shape. Updates are within-tenant last-write-wins rather than a serialized thread lease.

## Add schedules

Create a `D1SchedulesStorage`, expose `createScheduleRouter()`, and run `createScheduleTick()` from its own cron expression.

The router:

- mints schedule ids server-side;
- filters reads by tenant metadata;
- limits schedule count and fire rate;
- rejects reserved request-context keys on workflow and agent targets;
- exposes trigger history as read-only data.

The tick:

- lists due schedules;
- claims each fire with a compare-and-swap update that also checks active status;
- revalidates `metadata.tenantId`;
- mints a tenant-salted run id;
- consults the unattended-run cap;
- starts workflow targets through `RunnerRuntime`;
- starts agent targets through the injected thread topology callback;
- isolates each schedule's failure and records the actual joined run id.

The shared execution-context boundary strips reserved keys from persisted compatibility paths and rejects them at external HTTP boundaries. Reserved keys include every `breakwater.*` key, `mastra:goal`, run/thread/resource ids, and JavaScript prototype meta-keys.

Runtime workflow and isolation values overwrite sanitized context. The exact-leg connector grant then overwrites any prior grant, including with an empty list, and trusted actor/audit correlation is merged last. A row planted directly in D1 cannot override a grant, principal, workflow scope, isolation scope, run id, thread id, resource id, or goal context.

Schedules are standing configuration and have no TTL. Trigger history has an opt-in retention duty.

## Add background tasks

`createBackgroundTaskD1Domains()` supplies the serialized workflow adapter and tenant-scoped task storage. Host one `BackgroundTaskHost` per tenant Durable Object and expose its routes with `createBackgroundTaskRoutes()`.

The host manager:

- validates tenant and configuration synchronously;
- accepts the runtime's original pub/sub identity;
- unwinds partially started workers and subscriptions on boot failure;
- closes enqueue before workers on shutdown;
- scopes nested Mastra SSE payloads to the owning tenant.

Pass `backgroundTasks` to `createFlowsafeWorker()` to add terminal-task TTL cleanup to the purge cron. The default cleanup windows are package-defined; set explicit values when your data policy differs.

Only connectors whose permission manifest is read-only may opt into model-requested background execution. Write, destructive, and idempotent connectors stay foreground-only. A read-only connector may separately require approval; its grant check still runs when the background task executes.

## Add signal providers

Core signal providers deliver through an in-process agent registry, which is not durable or tenant-aware enough for this topology. Flowsafe preserves the provider contract while routing delivery through the thread topology.

Wire:

- `D1SubscriptionStoreFactory` for tenant-bound subscriptions;
- `createSubscriptionRouter()` for human-only subscribe and unsubscribe;
- `createWebhookRouter()` for raw-body verified webhooks;
- `createSignalProviderHostTopology()` for one provider host Durable Object per tenant;
- `SignalProviderHost` for alarm-driven polling;
- `deliverNotification()` to send each delivery through the owned thread;
- a `reconcilePolling` callback so subscription mutations arm or cancel provider polling after the database commit.

Webhook processing verifies the signature before parsing, finds the tenant only through the subscription row, rate-limits by provider and tenant, and bounds forgery audit. A webhook contains no trusted tenant assertion.

Polling reconciliation is post-commit. If the lifecycle callback fails, the route returns a failure that states the mutation was applied; it does not roll back the subscription. A retry reconciles the committed truth.

The host keeps an earlier alarm rather than postponing it. It deletes the alarm
when no pollable subscriptions remain. A zero or absent interval means
manual-only; negative, fractional, non-finite, or unsafe intervals are
rejected. Choose a production-safe positive cadence for each polling provider.

`githubSignalProvider()` is the reference WebCrypto HMAC implementation. Provide an ownership allowlist that maps each external resource to threads the authenticated tenant owns.

## Run scheduled duties

Keep independent duties in separate failure boundaries. CPU termination is not a catchable JavaScript exception.

| Duty | When to enable |
| --- | --- |
| Approval SLA sweep | Always for approval hosts |
| Workflow snapshot and decided-approval purge | Always with an explicit retention policy |
| Thread purge | When conversations have an idle TTL |
| Notification purge | When terminal inbox rows have a TTL |
| Thread-state purge | When signal state and goals have a TTL |
| Schedule tick | When schedules are enabled |
| Schedule-trigger purge | When trigger history has a TTL |
| Background-task purge | When background tasks are enabled |
| Notification dispatch tick | When delayed notifications are enabled |
| Provider polling alarm | Per tenant when a pollable subscription exists |

The advanced starter makes these responsibilities visible in one host. The [Deployment reference](deployment-reference.md) lists bindings and configuration, and the [Operations runbook](operations-runbook.md) covers recovery and offboarding.
