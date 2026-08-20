# Durable agents

Flowsafe can run Mastra durable agents through the same `RunnerRuntime` that drives workflows. This keeps agent legs inside the run-id, request-context, approval-grant, snapshot-provenance, audit, and retention boundaries instead of creating a second execution path.

This surface is supported and opt-in: it is tested and covered by package compatibility guarantees, but the host must explicitly wire the required routes, bindings, storage domains, or scheduled duties.

Use the advanced starter in [`packages/agent-starter/`](../packages/agent-starter/README.md) for a consumer-sized composition. The baseline Worker in [`packages/flowsafe/deploy/`](../packages/flowsafe/deploy/README.md) remains the smaller workflow-and-approval starting point.

## Runtime topology

```text
authenticated Worker
  |
  +-- run routes ------------> one runner DO per run
  |
  +-- thread topology -------> one thread DO per server-minted thread
  |                               |
  |                               +-- runtime-driven durable agent
  |                               +-- message/signal/state/notification routes
  |                               +-- active-run registry and idle wake
  |
  +-- provider topology -----> one singleton provider host DO
  |
  +-- live routes -----------> one singleton hub DO
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
    requiredPermissions?: readonly Permission[];
    allowedAutomation?: readonly {
      kind: 'service' | 'agent' | 'system';
      entryPaths: readonly AgentEntryPath[];
    }[];
  };
  agent: GuardedAgentHandle;
}
```

`allowedRoles` governs authenticated humans. `allowedAutomation` governs everything else, and **an omitted or empty list denies every automated entry.** A schedule tick, a signal-provider delivery, a notification dispatch, or a delegating agent reaches an agent only if that agent names the principal kind together with the exact entry path. Naming the path and not just the kind is what stops an agent that may fire on a schedule from also accepting webhook-delivered signals.

`AgentMeta.requiredPermissions` adds server-derived authorization for both human and automated principals. The list uses all-of semantics: the principal must hold every listed `Permission`. Each identifier contains two or more lowercase ASCII segments separated by dots. Every segment starts with a letter and continues with letters or digits. The complete identifier contains 3 to 200 characters.

Catalog construction rejects `requiredPermissions` when it is not an array, is empty, contains duplicates, or contains a malformed identifier. Omit the field to preserve role and automation authorization without a permission resolver.

`approval.resume` is never declared: resuming is implied by the kind that started the run. Requiring hosts to list it would mean an automated run that suspends for approval is stranded the moment a human approves it. A kind removed from the declaration entirely can still no longer resume.

The guarded handle must agree. `createGuardedAgent({ allowedPrincipalKinds })` decides which kinds may execute at all, and catalog construction refuses a module whose declared automation kinds differ from it — so a host cannot advertise automation Breakwater will refuse, or register an automation-capable agent its catalog will never route to.

```typescript
// An agent driven by a schedule and by provider deliveries.
allowedAutomation: [
  { kind: 'system', entryPaths: ['schedule.fire', 'notification.dispatch'] },
  { kind: 'service', entryPaths: ['signal.notification'] },
],
// and on the guarded agent:
allowedPrincipalKinds: ['human', 'system', 'service'],
```

Pass a server-owned `PrincipalPermissionResolver` through `ThreadAgentHostOptions.resolvePrincipalPermissions`. The resolver receives only the trusted `ExecutionPrincipal`. It returns a `PrincipalPermissionResolution` with effective permissions and the policy snapshot's `policyVersion`:

```typescript
import {
  type AgentModule,
  createThreadAgentHost,
  type PrincipalPermissionResolver,
} from '@proofoftech/flowsafe/agent-host';

const reportAgent: AgentModule = {
  meta: {
    id: 'report-agent',
    title: 'Report agent',
    description: 'Builds and reads reports',
    allowedRoles: ['operator'],
    requiredPermissions: ['agents.report.run', 'reports.read'],
  },
  agent,
};

const resolvePrincipalPermissions: PrincipalPermissionResolver =
  (principal) => ({
    permissions: principal.kind === 'human'
      ? permissionsForRole(principal.role)
      : permissionsForPrincipal(principal),
    policyVersion: accessPolicyVersion,
  });
```

The host installs the resolver beside its catalog module builder:

```typescript
const agentHost = createThreadAgentHost({
  ...threadHostOptions,
  buildModules: () => [reportAgent],
  resolvePrincipalPermissions,
});
```

The resolver can map a human role or an automated identity to the same `Permission` vocabulary. Keep `permissionsForRole`, `permissionsForPrincipal`, and `accessPolicyVersion` in trusted host configuration.

The thread host evaluates required permissions only after the existing human-role or automated-entry gate succeeds. A missing resolver, a thrown or rejected call, or malformed resolver output fails closed for a permission-requiring agent. Malformed output is a non-object resolution, a permission set that is not an array of canonical identifiers, or a `policyVersion` that is blank, longer than 200 characters, or contains ASCII control characters. Duplicate identifiers in the resolved set are tolerated because a repeat cannot change an all-of decision. A resolver failure surfaces only the generic audit reason `permission resolution failed`, so log failures inside the resolver itself.

A configured resolver runs on every authorized entry, including role-only agents, because its resolution is also the input to connector authorization. The host projects it into the run's derived request context as `breakwater.principalPermissions` on every start and resume leg — an explicit `null` when no resolution exists, so a resume retires a stale persisted projection instead of inheriting it. A connector that declares `PermissionManifest.requiredPermissions` enforces its own all-of list against that projection inside Breakwater, before its dry-run branch and approval grant. A role-only agent does not require the resolver: a failed resolution still starts the run, records an `agent.permissions.resolve` error event, and leaves the projection `null`, so permission-declaring connectors inside that run fail closed.

Permission authorization audit detail includes `requiredPermissions` and `permissionPolicyVersion`. The policy version is `null` when no valid resolution exists. The event does not include the effective permission set or identity-provider groups.

The Worker receives metadata only. The thread Durable Object constructs the complete module because its model, storage, runtime, pub/sub, connector, and database objects belong to that instance.

Catalog construction also rejects path-unsafe or duplicate ids, empty descriptions, invalid role lists, metadata/handle id mismatches, metadata roles that differ from the guarded handle, and automation declarations that name a human kind, an unknown entry path, `approval.resume`, a repeated kind, or a kind set differing from the guarded handle. An omitted role list uses `RUN_START_ROLES`; an omitted automation list denies all automated entry.

Mount `createAgentRouter()` through `createFlowsafeWorker({ buildAgentRouter })`. It exposes:

| Method and route | Purpose |
| --- | --- |
| `GET /agents` | List the registered metadata and authenticated actor |
| `POST /agents/:agentId/runs` | Mint ids and start a guarded durable run |
| `GET /agents/:agentId/runs/:threadId/:runId` | Read authoritative durable status |
| `POST /agents/:agentId/runs/:threadId/:runId/terminate` | Cancel a durable agent run |
| `GET /agents/:agentId/runs/:threadId/:runId/stream?offset=N` | Observe authenticated newline-delimited JSON events |

Every authenticated role may list agents. Run inspection follows resource ownership: the owning principal, reviewers, viewers, and admins may read a run; another operator or builder receives `404`. Starts require both `RUN_START_ROLES` and the agent's effective roles. There is no public agent-resume route.

The start body accepts only `{"prompt":"..."}`. The router caps the raw UTF-8 body at 16,384 bytes, requires non-whitespace prompt content, preserves that content, and rejects ids, trusted context, overrides, unknown fields, and prototype meta-keys.

Each stream line contains the next reconnect cursor and one event. Replay depends on the configured Mastra cache and is not process-restart durable. When the durable run exists but its replay cache does not, the stream route returns 409 and the client must use the status route.

Approval records store an `agent-thread` target with the agent, thread, resource, and original authorized principal. `createAgentApprovalResumer()` re-authorizes that stored principal against the current catalog: a human against the agent's roles and an automated principal against its `allowedAutomation` declaration on the `approval.resume` entry path. The thread host then enforces any `requiredPermissions` through the current resolver policy. It reconstructs the guarded module after eviction and resumes as the original principal. Before resume, the wrapper rebuilds Mastra's local and global run registries from fresh trusted context. It invokes only Breakwater's reserved RBAC `processInput` hook during rehydration, then installs the complete input, LLM-request, and output processor lists for resumed loop execution. It does not replay application or policy `processInput` hooks. An authorization denial stops before registry installation, observation, or tool execution. The reviewer identity remains attached to the approval decision.

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

`createFlowsafeDurableAgent()` registers Mastra's `durable-agentic-loop` workflow on the supplied runtime. Its `stream()`, `generate()`, and `prepare()` entry points require a host-minted opaque run id. Flowsafe does not fall back to an unowned UUID. `prepare()` remains an initial-execution API and runs the full initial processor chain. `resumeViaRuntime()` uses the dedicated registry rehydration behavior described above.

The runtime's pub/sub identity is reused by default. This lets the durable loop, observer, and active-thread signal delivery share one feed inside the thread Durable Object.

This wrapper does not add the guarded-agent brand or catalog authorization to a raw agent. Use `agent-host` for the supported protected public surface. Do not expose inherited `resume()`, `approveToolCall()`, or `declineToolCall()` methods as client routes.

## Create and protect memory identities

Mastra memory accepts caller-selected thread and resource ids. A Flowsafe host replaces those identities at its boundary:

```typescript
const threadId = context.newThreadId();
const resourceId = context.resourceIdFromKey(customerKey);
```

The exported `mintThreadId()` helper generates a thread id. `resourceIdFromKey()` validates a trusted host business key and returns that key unchanged; it does not generate a resource id.

Host rules:

1. Reject bodies that name `threadId` or `resourceId` with `assertNoClientMemoryIds()`.
2. Resolve the authenticated `ActorContext`.
3. Resolve existing resources before role errors when the route's 404 contract requires it.
4. Address the thread Durable Object through `createThreadTopology()`.
5. Let the topology stamp `x-flowsafe-principal` from the resolved context. It strips retired tenant, actor, and role headers on send and forward. `createActorResolver()` refuses an inbound request carrying any server-stamped identity header.
6. Let `ThreadDurableObject` project the actor from the stamped principal and use its own `id.name` as the authoritative thread id.

See [Agent memory isolation](agent-memory-isolation.md) for the exact identity, retention, and decommissioning rules.

## Compose D1 storage domains

`createD1Storage()` accepts injected Mastra composite domains. Add only the features you host:

| Feature | Composition helper | Storage |
| --- | --- | --- |
| Signals, notifications, thread state, goals | `createSignalStorageDomains()` | `mastra_notifications`, `mastra_thread_state` |
| Schedules | `createScheduleStorageDomains()` | `mastra_schedules`, `mastra_schedule_triggers` |
| Background tasks | `createBackgroundTaskD1Domains()` | serialized workflow and deployment task domains |

The signals helper is injected into do-runner rather than imported by it, which avoids a package cycle. The schedule store mirrors Mastra's schedule contract because the Cloudflare D1 adapter does not ship that domain. Flowsafe owns both signal tables and the subscription table.

When you adopt a storage domain, declare its retention or standing-state lifecycle in the same change. The package schema guard pins that correspondence internally; your host must still schedule each exported retention duty.

## Host the thread Durable Object

Subclass `ThreadDurableObject`, construct the catalog modules for that instance, and install `createThreadAgentHost()` and `createThreadSignalRoutes()` inside the subclass route. These factories share the asserted `ThreadScope` instead of mutable module or request-global state.

- stamps the runtime pub/sub identity onto the agent before each call;
- serializes delivery into the thread;
- checks whether a run is already active;
- applies active and idle behavior;
- starts an idle run only through the injected `startIdleRun` seam;
- requires a runtime-driven agent before an idle wake;
- mints no approval capability.

Before production use, the host's idle-start seam must:

1. revalidate the stored thread and agent binding;
2. consult the unattended-run cap;
3. mint a fresh opaque run id;
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

The Worker applies this order: authentication, coarse role, thread lookup, byte cap, JSON parse, client-memory-id rejection, attribute-key allowlist, the configured rate-limit seam, audit, then topology forwarding. The starter's limiter is isolate-local example protection; use shared durable state when the limit is contractual across the deployment.

Signals are untrusted model input. Core escapes the XML representation, while the route validates tag and attribute names and caps payload size. A receiving agent's ordinary `processInput` policy is not a complete signal boundary: Mastra can drain queued signals after the initiating input processor has run. Configure `createThreadSignalRoutes({ contentPolicy })` to inspect Mastra's canonical escaped XML inside the Thread Durable Object before delivery, persistence, wake, or run start. The same boundary covers direct routes, providers, schedules, and notification dispatch.

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

The router writes through Mastra's objective helpers into the goal lane of `mastra_thread_state`, so the durable goal step reads the identical shape. Updates are deployment-local last-write-wins rather than a serialized thread lease.

## Add schedules

Create a `D1SchedulesStorage`, expose `createScheduleRouter()`, and pass `createScheduleTick()` to the maintenance singleton with a dedicated tick interval.

The router:

- mints schedule ids server-side;
- lists deployment schedules under role checks;
- limits schedule count and fire rate;
- rejects reserved request-context keys on workflow and agent targets;
- exposes trigger history as read-only data.

The tick:

- lists due schedules;
- claims each fire with a compare-and-swap update that also checks active status;
- attributes the fire from the infrastructure deployment tag;
- mints an opaque run id;
- consults the unattended-run cap when the host configures one;
- starts workflow targets through `RunnerRuntime`;
- starts agent targets through the injected thread topology callback;
- isolates each schedule's failure and records the actual joined run id.

Threaded agent schedule delivery is at-least-once across a target-DO crash.
Every retry carries the same `dispatchId` as the signal id, waits for the
current target lease, and replays a settled receipt. If the target accepted the
signal immediately before an isolate loss but had not yet stored that receipt,
lease takeover can deliver it again. Scheduled instructions and any tools they
invoke must therefore use `dispatchId` as their idempotency key.

The cap callbacks are optional library seams. An omitted callback means
uncapped execution; the reference starter labels that posture explicitly and
requires a shared durable quota before commercial unattended execution.

The shared execution-context boundary strips reserved keys from persisted compatibility paths and rejects them at external HTTP boundaries. Reserved keys include every `breakwater.*` key, `mastra:goal`, run/thread/resource ids, and JavaScript prototype meta-keys.

Runtime workflow and execution values overwrite sanitized context. The exact-leg connector grant then overwrites any prior grant, including with an empty list, and trusted actor/audit correlation is merged last. The runtime drops provider-supplied isolation scope. A row planted directly in D1 cannot override a grant, principal, workflow scope, run id, thread id, resource id, or goal context.

Schedules are standing configuration and have no TTL. Trigger history has an opt-in retention duty.

## Add background tasks

`createBackgroundTaskD1Domains()` supplies the serialized workflow adapter and deployment task storage. Host one singleton `BackgroundTaskHost` Durable Object and expose its routes with `createBackgroundTaskRoutes()`.

The host manager:

- validates deployment configuration synchronously;
- accepts the runtime's original pub/sub identity;
- unwinds partially started workers and subscriptions on boot failure;
- closes enqueue before workers on shutdown;
- scopes nested Mastra server-sent event payloads to the deployment host.

Pass `backgroundTasks` to `createFlowsafeWorker()` to add terminal-task TTL cleanup to the maintenance purge duty. The default cleanup windows are package-defined; set explicit values when your data policy differs.

Only connectors whose permission manifest is read-only may opt into model-requested background execution. Write, destructive, and idempotent connectors stay foreground-only. A read-only connector may separately require approval; its grant check still runs when the background task executes.

## Add signal providers

Provider deliveries arrive as a `service` principal on the `signal.notification` entry path. The target agent must declare that pair in `allowedAutomation`, or delivery is refused.

Core signal providers deliver through an in-process agent registry, which is not durable enough for this topology. Flowsafe preserves the provider contract while routing delivery through the thread topology.

Wire:

- `D1SubscriptionStoreFactory` for deployment subscriptions;
- `createSubscriptionRouter()` for human-only subscribe and unsubscribe;
- `createWebhookRouter()` for raw-body verified webhooks;
- `createSignalProviderHostTopology()` for the singleton provider host Durable Object;
- `SignalProviderHost` for alarm-driven polling;
- `deliverNotification()` to send each delivery through the owned thread;
- a `reconcilePolling` callback so subscription mutations arm or cancel provider polling after the database commit.

Webhook processing verifies the signature before parsing, looks up the subscription row, rate-limits by provider, and bounds forgery audit. A webhook contains no trusted actor assertion.

Polling reconciliation is post-commit. If the lifecycle callback fails, the route returns the committed mutation, logs the failure, and marks the audit event with `pollingLifecycle: 'failed'`. It does not roll back the subscription. A retry reconciles the committed truth.

The host keeps an earlier alarm rather than postponing it. It deletes the alarm
when no pollable subscriptions remain. A zero or absent interval means
manual-only; negative, fractional, non-finite, or unsafe intervals are
rejected. Choose a production-safe positive cadence for each polling provider.

`githubSignalProvider()` is the reference WebCrypto HMAC implementation. Provide an ownership allowlist that maps each external resource to deployment threads.

## Run alarm-driven duties

Keep independent duties in separate alarm invocations. CPU termination is not a catchable JavaScript exception. The maintenance singleton persists its successor before each duty and schedules an immediate follow-up when another duty is due.

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
| Provider polling alarm | Singleton host when a pollable subscription exists |

Each duty that reaches an agent carries an automated principal: the schedule tick fires as `system` on `schedule.fire`, the notification dispatch tick as `system` on `notification.dispatch`, and provider delivery as `service` on `signal.notification`. Enabling a duty is not enough — the target agent must declare that kind and entry path in `allowedAutomation`, or the run is refused at the host.

The advanced starter makes these responsibilities visible in one host. The [Deployment reference](deployment-reference.md) lists bindings and configuration, and the [Operations runbook](operations-runbook.md) covers recovery and decommissioning.
