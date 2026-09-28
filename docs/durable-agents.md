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

The thread host evaluates required permissions only after the human-role or automated-entry gate succeeds. A missing resolver, a thrown or rejected call, or malformed resolver output fails closed for a permission-requiring agent. Malformed output is a non-object resolution, a permission set that is not an array of canonical identifiers, or a `policyVersion` that is blank, longer than 200 characters, or contains ASCII control characters. Duplicate identifiers in the resolved set are tolerated because a repeat cannot change an all-of decision. A resolver failure surfaces only the generic audit reason `permission resolution failed`, so log failures inside the resolver itself.

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

`createFlowsafeDurableAgent()` is available for compatibility. Create an ordinary Mastra `Agent`, then wrap it:

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

`createFlowsafeDurableAgent()` registers Mastra's `durable-agentic-loop` workflow on the supplied runtime. Its `stream()`, `generate()`, and `prepare()` entry points require a host-minted opaque run id. Only a start registered through `streamUntilPersisted()` reaches `RunnerRuntime`; an unregistered start fails terminally after a bounded, best-effort attempt to preserve its input, decided by the guarded input chain's verdict. When no input processor refused the start, its prepared input is preserved. When the start's input is a created signal, the call carries neither a host ticket nor a request context, and Breakwater's RBAC gate refuses the start, the signal is kept as received. Mastra starts such a call when it drains a signal after a run registered through `resumeViaRuntime()` completes: the drain inherits that run's options, which carry no request context, and the gate refuses it for its missing actor without reading content. On a guarded agent such a start has no actor, so the gate refuses it before any application input processor runs. Host code that calls `stream()` the same way, with a created signal or an object carrying the signal brand as input, has that input kept when the gate refuses it. After any other refusal nothing is preserved, so on a guarded agent content an input policy or processor refused never reaches the thread. Preservation is skipped when the thread is missing, memory is explicitly read-only, or the signal is transient. `prepare()` remains an initial-execution API and runs the full initial processor chain. `resumeViaRuntime()` uses the dedicated registry rehydration behavior described above.

The trusted host calls `streamUntilPersisted(messages, options, requestedBy, requestedByKind, attemptToken, scheduleDispatch, idempotencyKey, authority)`. The eighth argument is required; pass explicit `undefined` for unused positional options. Its `AgentStartAuthority` type is exported only from `agent-runner`. It carries the initiating owner, agent and thread identity, threaded mode, optional caller epoch, separate resource-owner guard and original successful start-reservation claim when keyed.

The bridge captures that authority before streaming and keeps it out of Core input, stream options and public JSON. It preserves the original method caller's requester identity even when a schedule or thread has a different resource owner. Mutable caller objects cannot replace the captured values after an await.

The `onPreparedStartIdentity` property must exist on the authority. Managed hosts supply an awaited callback that persists the prepared execution identity before initial admission; direct integrations may explicitly omit the callback with `undefined`. A placeholder callback does not establish a durable journal. Runtime generates a v2 execution token independently of the host's leg token and acknowledges the persistence waiter only after observing the matching nonpending durable outcome.

Both threaded and ephemeral agent starts use exact preparation journals and preserve the actual source owner separately from the initiating principal. Recovery after eviction reconstructs the actual wrapper before reading its workflow, without a synthetic request principal. A valid pending generation stays unresolved; prepared-unfenced absence or pending state cannot authorize initial repair. Terminal reservation settlement precedes approval, dispatch, owner and lifecycle cleanup, with exact journal clearing last.

The blocking-run scan and new run-record installation share the same lock, including on an already bound thread whose ownership is committed. Recovery validates a keyed journal's reservation-store configuration before bookkeeping and rechecks the complete journal after storage waits before releasing reservations.

Ordinary v1 and absent-provenance runs retain validated status, resume and lifecycle completion. Their mode, binding, canonical run record and saved principal pass the same normal host checks. Only a confirmed raw terminal outcome with no recovery journal and no active owning execution permits legacy record cleanup. These compatibility reads supply no generation identity or start-key settlement authority. During termination, the canonical record remains until lifecycle completion confirms.

Protected keyed replay reads the actual wrapper's workflow once and pairs its public envelope with that observation's identity. A terminal ephemeral run does not need a retained thread binding to replay. Optional stored context may be pruned, but present selectors must agree with the original agent/thread/mode. The execution token, raw snapshot, claim and recovery journal remain internal. Proof-only signal delivery retains the selected generation through policy and memory waits and checks the active run again immediately before delivery.

The runtime's pub/sub identity is reused by default. This lets the durable loop, observer, and active-thread signal delivery share one feed inside the thread Durable Object.

This wrapper does not add the guarded-agent brand or catalog authorization to a raw agent. Use `agent-host` for the supported protected public surface. Route clients through its authenticated run routes, which start each run at the host start seam. Direct `stream()` with an unregistered id resolves to a failed output; direct `generate()` rejects. `stream()`, `generate()`, `prepare()`, and `streamUntilPersisted()` synchronously refuse a live id. A successful `prepare({ runId: X })` keeps `X` live until core cleans up that prepared run.

The wrapper's constructor throws a `TypeError` when the agent it wraps has channels configured, declares Mastra agent schedules, sets the `durable` option, or is already a durable agent, such as a Mastra `DurableAgent` or another Flowsafe wrapper. Channels dispatch inbound messages and tool approval decisions to the wrapped agent outside `RunnerRuntime`, a Mastra schedule worker fires declared schedules on it outside `RunnerRuntime`, and a Mastra that registers a Mastra `DurableAgent`, or an agent with the `durable` option, exposes that agent's own recovery and run listing, which the runner does not guard. The guarded agent catalog applies the same checks to each module, so the thread host refuses such a module before its Mastra registers the agent. The wrapper itself cannot be registered on any Mastra; the Mastra host features that register it are listed after the grounds below.

`RunnerRuntime` refuses a workflow object that another Mastra has registered. Register each workflow object on one runtime, and do not add a runtime's workflows, or the wrapper's `getWorkflow()`, to another Mastra.

The runner cannot reach the raw agent you still hold. Channels bound to it after wrapping dispatch to it outside `RunnerRuntime`, and schedules set on it fire on it from the schedule worker of any Mastra that registers it and runs `startWorkers()`. Registering the raw agent on another Mastra moves its memory, when that memory has no storage of its own, and its pub/sub fallback to that Mastra. Do not bind channels or declare schedules on the raw agent after wrapping it, and do not register it on another Mastra.

The runner refuses these inherited entry points, on these grounds:

- **Re-drives a persisted run below `RunnerRuntime`.** The recovery entries `recover()` and `recoverActiveRuns()`, and the resume family `resume()`, `resumeStream()`, `resumeGenerate()`, `approveToolCall()`, `declineToolCall()`, `approveToolCallGenerate()`, and `declineToolCallGenerate()`, which rehydrate from snapshot storage on a run-registry miss.
- **Discovers runs without the host topology's per-principal ownership checks**, returning run, thread, and resource ids the caller does not own: the recovery discovery API `listActiveRuns()`, the agent-level `listSuspendedRuns()`, and `listActiveThreadRuns()`, which takes no arguments and returns those ids for every thread on the pub/sub instance with a run in flight, scoped by neither principal nor agent.
- **Deletes snapshot rows that deployment-scoped retention owns**: `deleteRunSnapshots()`.
- **Is a second execution surface outside `RunnerRuntime`, or mints a run id below the caller.** The network family `network()`, `resumeNetwork()`, `approveNetworkToolCall()`, and `declineNetworkToolCall()` compile and drive the multi-agent loop's own workflow with `createRun` plus `run.stream` or `run.resumeStream` on the default engine. The AI SDK v4 legacy entries `generateLegacy()` and `streamLegacy()` run the agent's tools through Mastra's legacy handler, skipping the authorization check every supported entry point calls. And `sendToolApproval()` reads like a resume but starts a run through the thread runtime's continuation when given messages. `network()`, `generateLegacy()`, `streamLegacy()`, and `sendToolApproval()` through the thread runtime's continuation generate a run id when the caller omits one, which is the unowned-run-id fallback Flowsafe refuses, so blocking them extends the host-minted run id rule that `stream()`, `generate()`, and `prepare()` enforce. `__setThreadRuntimeAgent()`, `setChannels()`, and `__setDeclaredSchedules()` are refused on the same ground without starting anything themselves: each installs something that later runs the agent outside `RunnerRuntime`, whether a thread-runtime execution target, a channel dispatch target, or a schedule that a Mastra schedule worker fires.
- **Binds the runner to a Mastra the runtime did not build.** `__setMastra()` and `__registerMastra()` bind the wrapper and its wrapped agent to the Mastra they are given, so every later leg is prepared against that Mastra. Registered through `Mastra.addAgent()` once the runtime has built its own Mastra, they also repoint the runtime's loop workflow, and with it run state, to that Mastra's storage.

All blocked inherited entries throw. The runner's resume path is `resumeViaRuntime()`, and host starts use `streamUntilPersisted()`.

Mastra's own host features fail closed on the wrapper, because each registers it on a Mastra. `Mastra.addAgent()` throws, and so does constructing a `Mastra` or an `MCPServer` with the wrapper in `agents`. An `AgentController` over the wrapper throws at `init()`, where it registers each backing agent on the controller's Mastra. A wrapper configured as a static sub-agent of an agent registered on a Mastra makes every run of that parent fail when the parent converts its tools, so do not use the wrapper as a sub-agent. The signal and message senders remain inherited because every run outcome they can produce lands on the runner's terminal path. The `queue` and `state` routes persist rather than wake on idle. An owner notification is recorded in the durable inbox and delivered at ingestion under the owner's principal, into the owner's running run or persisted to memory; it never wakes a run. A non-owner notification is recorded for the trusted dispatch tick, which delivers each row as the dispatch principal. Persist outcomes require agent memory and return `memory-unavailable` without it. A default or `ifIdle: 'persist'` message or signal still delivers into an active run without memory; the memory gate responds only when core's outcome for the request replaced a persist (an idle discard substituted for a requested persist, or an active persist that no memory could write). If a direct sender call or Mastra completion drain reaches core's run-id mint, the runner persists the input when allowed, emits a terminal error, and lets Mastra clean up the thread state without entering `RunnerRuntime`.

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

The signals helper is injected into do-runner rather than imported by it, which avoids a package cycle. The schedule store mirrors Mastra's schedule contract because the Cloudflare D1 adapter does not ship that domain. Flowsafe owns the signal tables and the subscription table.

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

Thread delivery is priority-planned across summaries and individual notifications, remains stable across chunks, and suppresses summarized high-priority rows while the thread was active. Summary items arise only from rows written by Mastra's delivery policy, which means an unbranded agent's owner notifications.

### Bound notification delivery

`createNotificationDispatchTick()` and `createThreadSignalRoutes()` accept `maxDeliveryAttempts`, a positive safe integer defaulting to `DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS`. Use the same value on both factories. They capture the policy at construction; request bodies cannot change it. A tick with `limit: 0` performs no delivery or storage work; invalid numeric policy still fails at construction, and storage without the conditional delivery operation fails construction at every `limit`.

`deliveryAttempts` counts persisted failed rounds. At the bound, the conditional write sets `discarded`, `deliveryReason: "delivery-attempts-exhausted"` and cleared delivery cursors. A row already at the bound is not sent; its conditional discard preserves the previous count, error and attempt time. Retry delays below the bound keep their backoff. Malformed counters remain unmodified and produce an unresolved failure. A due row whose other scalars cannot be read is not written: it is counted as a failed outcome, re-selected on every pass, holds its place in the bounded window and in the `pending-notifications` inventory category, and must be repaired or deleted directly.

Terminal receipts remain available through `getNotification()` and `listNotifications()` until the host's configured retention removes them. They include the last error/count and discard timestamp. The due scan excludes terminal notifications so a persistently refused target can release its place in the bounded dispatch window.

The tick requires `NotificationDeliveryStorage`. `D1NotificationsStorage` provides its `updateNotificationDeliveryIfUnchanged()` operation. Custom implementations must atomically compare the supplied `NotificationDeliveryObservation` and apply the narrow `NotificationDeliveryFailure` patch against their other writers. The observation uses detached scalar values, ISO timestamps and encoded JSON; the public types define its fields. A method implemented as an asynchronous read followed by an unconditional update does not satisfy that contract. Ordinary Core storage can still serve notification ingestion; driving dispatch requires the conditional operation.

Failure bookkeeping preserves newer summary, delivery and content-denial receipts. A conditional write with an uncertain response can be confirmed by an exact target readback. General delivery responses that are lost remain conservatively counted as failed; the dispatcher does not invent successful deliveries from another writer's state. Invocation counters do not promise exactly-once signal delivery: a signal can succeed before its receipt write fails.

Attempt time and retry cursors use the dispatch clock. Updated/discarded timestamps use a captured wall-clock value for retention. Identical same-ID replacements with no observable difference have no separate generation identity under this contract.

D1 compares notification timestamps as instants when selecting due rows, ordering lists and applying retention. Public storage methods accept finite `Date` values. Direct database writers must use ISO dates or ISO date-times with an explicit UTC or numeric offset. A date-time with no zone, or non-ISO text, is outside that grammar: such a value in `deliverAt` or `summaryAt` never matches due selection, and such a value in `updatedAt` never matches retention and sorts its row last in an unlimited `listNotifications` page, so repair or delete the row that carries it directly; supported raw encodings retain their stored bytes.

Summary source counts and a configured source delivery policy read own properties: the declared `@mastra/core` peer guards both lookups, so a source named after an `Object.prototype` member is counted under its own key and resolves the configured priority or default action. A core outside the declared peer range that predates [mastra-ai/mastra#23693](https://github.com/mastra-ai/mastra/issues/23693) and [mastra-ai/mastra#23694](https://github.com/mastra-ai/mastra/issues/23694) miscounts such a source and selects the inherited policy entry instead.

## Expose signal ingestion

Mount `createSignalRouter()` through `createFlowsafeWorker({ buildSignalRouter })`. The default prefix is `/api/threads`.

| Channel | Route | Purpose |
| --- | --- | --- |
| `message` | `POST /api/threads/:threadId/message` | Send a user-like message |
| `queue` | `POST /api/threads/:threadId/queue` | Queue a message for the next host-started turn (persisted; never wakes) |
| `signal` | `POST /api/threads/:threadId/signal` | Send a named signal |
| `state` | `POST /api/threads/:threadId/state` | Persist owner-authorized thread state for the next host-started turn; may return `principal-mismatch`, `persistence-forbidden`, or `memory-unavailable` |
| `notification` | `POST /api/threads/:threadId/notification` | Record a notification in the durable inbox. For a runtime-driven agent, an owner's notification is delivered at ingestion and a non-owner's waits for the host dispatch tick; see [Run alarm-driven duties](#run-alarm-driven-duties) |

Without agent memory, persist outcomes return `memory-unavailable`, except that a default or `ifIdle: 'persist'` message or signal still delivers into an active run (an active persist that no memory could write still answers `memory-unavailable`), and a persist-behavior agent-schedule fire settles a canonical `discard` receipt.

Configure `SignalRouterOptions.validateThreadTarget` with the `BoundThreadTargetValidator` type to apply host-specific restrictions before body parsing or signal forwarding. `createAgentThreadTopology().requireBoundThread` verifies a durable binding. For strict ownership, compare the captured principal's `kind` and `id` with the owner returned by `await context.resourceOwnerFor('thread', target.threadId)`, and throw `RunRouteError` with status 404 on refusal. Omitting the callback keeps the router's resource-access policy, including its administrator access.

The router records acceptance after the downstream response succeeds and normalizes thread-not-found refusals from registry access, the validator and the receiving Durable Object. Audit-sink and diagnostic failures retain the selected response. The starter's limiter is isolate-local example protection; use shared durable state when the limit is contractual across the deployment.

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

Objectives are standing instructions injected into future turns. Mutations require authenticated thread access. Audit-sink failures retain the selected mutation result.

The router writes through Mastra's objective helpers into the goal lane of `mastra_thread_state`, so the durable goal step reads the identical shape. Updates are deployment-local last-write-wins rather than a serialized thread lease.

## Add schedules

Create a `D1SchedulesStorage`, expose `createScheduleRouter()`, and pass `createScheduleTick()` to the maintenance singleton with a dedicated tick interval.

Use the same database object for the schedule store and its `ExecutionFenceStore`. A fenced custom facade must advertise `FENCED_SCHEDULE_STORAGE` before serving requests, including before epoch activation. `D1SchedulesStorage` and `createScheduleStorageDomains()` provide that capability. An explicit `executionFence: 'none'` supports a custom facade without the atomic storage contract.

Supply the artifact epoch through trusted resolver configuration or the composed Worker's `mutationEpoch` option. The router retains that authenticated value through asynchronous work. Direct D1 authoring methods accept a trailing `MutationEpochContext` and require transactional `batch()`. Context-free calls through Core refuse once the epoch requirement is active. Request bodies and external headers cannot supply this authority.

Create, update and resume require an open fence. Pause and delete retain their state allowance but require the current epoch after activation. The router checks the epoch even when the requested pause/resume status matches the row. Direct `pauseSchedule` accepts no patch; `resumeSchedule` takes the observed cron/timezone with the computed next fire and rejects a concurrent configuration change. Admitted trigger settlement can finish a pending deletion after the fence changes.

`SCHEDULE_MUTATION_CONFLICT` is a 409 for a changed fence frame or resume configuration. `SCHEDULE_MUTATION_OUTCOME_UNKNOWN` is a 503 when the write cannot be confirmed; it can follow a committed write and supplies no rollback authority. A later matching row is not an invocation receipt. See the [API reference](api-reference.md#flowsafe-subpath-exports) for the schedules entry.

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

Only connectors whose permission manifest is read-only may opt into background execution with `background: true`. Write, destructive, and idempotent connectors stay foreground-only. A read-only connector may separately require approval; its grant check still runs when the background task executes.

The connector wrapper refuses a call that Mastra's standard agent loop runs as a background task in the dispatching process, on every connector without the opt-in. The refusal does not reach a `BackgroundTaskHost` executor, Mastra's durable agent loop, a static background executor running a task queued or recovered after a restart or on a separate worker, or a connector called from inside background work: none of them passes a background flag to the connector. The refusal is not final: Mastra retries a refused task, and a Mastra that starts on the same storage can recover a task that is still queued, or running with retries left, through a static executor. `createGuardedAgent()` disables background dispatch, and Flowsafe's `RunnerRuntime` and agent thread host run no background-task manager. Do not register a connector without `background: true` as a `BackgroundTaskHost` executor, and do not make one background-eligible on a raw Mastra agent. See the [connector `background` contract](connector-interface.md#background).

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
| Notification dispatch tick | When notifications are enabled |
| Provider polling alarm | Singleton host when a pollable subscription exists |

Each duty that reaches an agent carries an automated principal: the schedule tick fires as `system` on `schedule.fire`, the notification dispatch tick as `system` on `notification.dispatch`, and provider delivery as `service` on `signal.notification`. Enabling a duty is not enough — the target agent must declare that kind and entry path in `allowedAutomation`, or the run is refused at the host.

`/signal/notification` ingestion to a runtime-driven agent, or from a principal that `canPersist` refuses, requires the notifications storage domain and returns `409` without it. What follows the inbox write depends on the principal:

- **An owner notification to a runtime-driven agent** is delivered at ingestion under the owner's principal: into the owner's running run, or persisted to agent memory when the thread is idle or the owner's run is not running. It never wakes a run, and no dispatch tick selects its row. A delivery that the run does not drain before it ends reaches the runner's terminal refusal, which publishes an error and keeps the signal in agent memory, when memory exists, unless the guarded agent's input chain refuses its content. Before writing anything, the route answers `409` with a `reason` when another principal's run holds the thread (`principal-mismatch`, with `retry: true`), when the delivery would need agent memory the agent lacks (`memory-unavailable`), or when a `dedupeKey` or `coalesceKey` matches a pending row the tick will deliver (`notification-pending`). After the write, the row is discarded when Mastra reports the thread blocked (`409`, `thread-blocked`), when the content policy denies the signal (`422`) or fails (`503`), and when storage or the send fails (`502`).
- **A non-owner notification** is written due now. The host must run `createNotificationDispatchTick()`, which delivers each row individually as the dispatch principal; no Mastra delivery policy applies, neither the default nor a configured `notifications.deliveryPolicy`. On an idle thread the tick wakes a run whose principal and approval requester is the dispatch principal: the self-approval bar does not cover the notification's author, the row stores no author, and the run's permission projection and audit are `system`'s. Idle starts are limited only by `consultRunCap` where the host wires it. The tick counts a failed attempt when the run cap refuses the wake, when another principal's run holds the thread, and when the agent does not declare `system` on `notification.dispatch` in `allowedAutomation`; the row is discarded after `maxDeliveryAttempts` failed attempts. The advanced starter runs the tick on an interval, so a row waits at least until the next tick, and longer while failed attempts back off. A host without the tick records these rows but never delivers them.
- **An unbranded agent's owner notification** follows Mastra's delivery policy, which needs the notifications store on the agent's own Mastra.

When a host passes no `canPersist`, every principal is an owner, signal providers included, and a provider treats a `409` as a permanent drop.

An owner row stays pending with neither `deliverAt` nor `summaryAt` when the isolate dies, or the execution fence refuses, between the inbox write and the write that settles it. No dispatch pass selects it, the `pending-notifications` inventory category counts it, and a later non-owner notification with the same key can coalesce into it and give it a due time. Clear it with a direct write that sets it `discarded`, or delete it, as the [drain inventory](do-runner-design.md#http-surface-inside-the-object) describes for pending rows without a due time.

The advanced starter makes these responsibilities visible in one host. The [Deployment reference](deployment-reference.md) lists bindings and configuration, and the [Operations runbook](operations-runbook.md) covers recovery and decommissioning.
