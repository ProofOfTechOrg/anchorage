# Security threat model

Anchorage adds enforcement to a Mastra application, but the application owner remains responsible for identity, business authorization, deployment isolation, provider credentials, and incident response.

This document covers breakwater and flowsafe at their supported public boundaries. It does not treat a model, vendor CLI, third-party connector, OAuth provider, Cloudflare account, or Mastra itself as trusted merely because Anchorage integrates with it.

## Security objectives

Anchorage is designed to preserve these properties:

1. A connector side effect runs only after all configured tool policies pass.
2. A human approval grants only the named connector on the exact authorized suspension.
3. A client, model, signal, workflow input, or raw resume cannot mint a capability.
4. A deployment cannot serve D1 or R2 resources provisioned for another organization, and supported data-plane routes cannot cross a physical deployment boundary.
5. Concurrent reviewers and workers do not commit two conflicting transitions.
6. A restart does not erase the run snapshot or resume fingerprint.
7. Enforcement errors and audit failures do not expose raw prompts, process output, secrets, or URLs with credential-bearing queries.
8. Retention never deletes live authorization work merely because it is old.

Availability is secondary to these properties. When context, storage, identity, or configuration is missing, the supported path fails closed.

## Assets

| Asset | Location | Impact if compromised |
| --- | --- | --- |
| Workflow and agent definitions | Worker bundle | Unauthorized behavior or policy bypass |
| Mastra workflow snapshots | D1 | Run disclosure, corruption, replay, or denial of service |
| Approvals and reviewer context | D1 | Unauthorized capability or sensitive context disclosure |
| Memory threads, messages, resources, goals, notifications | D1 | Organization conversation or instruction disclosure |
| Schedules, background tasks, provider subscriptions | D1 | Unauthorized unattended execution |
| Artifacts | R2 | Business-data disclosure or loss |
| Actor identity and deployment tag | Verified host context and infrastructure bindings | Authorization, attribution, or physical isolation can fail |
| Approval, workflow, run, thread, resource context | Mastra `RequestContext` and topology headers | Capability or namespace forgery |
| Resume ordinals | Durable Object storage | Approved run can strand or mis-bind |
| Model, connector, OAuth, webhook, stream, SIEM, and token keys | Cloudflare secrets or external secret manager | Provider or control-plane compromise |
| Audit and metrics stream | Memory, Logs, Queue, SIEM | Evidence loss or organization-sensitive metadata disclosure |
| Agent CLI prompt and workspace | Child-process argv and filesystem | Source, secret, or code-integrity compromise |

After RBAC, memory input processors such as semantic recall's embedder receive the caller's text before input policies on every guarded loop; a memory resolution failure fails closed. A memory input processor error, such as a failed thread-history read, stops a durable call with `input processor failed` and one `agent.input.processor` error event naming the processor's id (`message-history` for history); on `generate()` and `stream()`, Mastra rejects the call and Breakwater writes no audit event.

## Trust boundaries

### Public client to Worker

The public host must verify deployment identity, authenticate the request, validate actor claims, enforce coarse and resource-specific roles, enforce server-owned agent permission requirements, reject client-selected trusted ids/context, cap untrusted bodies, and return 404 before role errors where the resource contract requires a non-enumeration boundary.

The library supplies verifier and router seams, not an identity provider. A static bearer map in the reference deployment is an inspectable example, not production identity guidance.

The guarded agent router resolves route syntax, authenticates, resolves the server catalog, checks stored agent/thread/run bindings, authorizes mutations, validates input, then invokes the trusted topology. This order prevents a disallowed role from using error differences to enumerate resources inside the organization.

### Worker to request context

Breakwater context keys are capabilities or trusted namespace claims:

```text
breakwater.actor
breakwater.connectorGrants
breakwater.connectorExecution
breakwater.principalPermissions
breakwater.idempotencyKey
breakwater.dryRun
breakwater.workflowScope
breakwater.isolationScope
```

Only trusted host/runtime code may populate actor, grant, principal-permission, or workflow values. Idempotency keys and dry-run selection may originate from authorized application logic, but must not overwrite the other keys. `breakwater.isolationScope` remains an opaque Breakwater policy input, a non-empty string; Flowsafe does not mint it for the single-organization data plane. A value of the wrong type in these keys is data an application can produce, so the connector denies a malformed isolation scope or dry-run selection instead of reading it as absent.

[`isReservedExecutionContextKey()`](../packages/flowsafe/src/do-runner/execution-context.ts) defines the reserved namespace. API-start and schedule `requestContext` records reject those keys at ingestion. Persisted compatibility paths strip reserved entries before trusted derivation.

An API start may carry non-reserved application context after authentication, start-role checks and workflow authorization. The validated value is visible to the host's `beforeStart` policy and travels on the existing authenticated Worker-to-Durable-Object channel into the stored application tier. Host policy must verify any business meaning attributed to those values. They cannot supply execution identity or connector grants; `inputData` is not a context transport.

Trusted merges apply sanitized external or stored context first, then workflow, run, current execution identity, structured connector grants, and trusted actor/audit correlation. Provider-supplied isolation scope is dropped. An empty grant array overwrites any stale value, and the agent host projects the principal-permission resolution or an explicit `null` on every leg so a stale persisted projection cannot survive a resume.

For an API start, provider application values override matching stored values. A verified schedule target controls its context even when it supplies none. Persisted application values survive resume, while trusted providers refresh capability state. Keyed replay revalidates input and host policy but preserves the winning run's context.

### Host code to Breakwater configuration

Breakwater validates host configuration as data: what a JSON or YAML file, an environment variable or a typo can produce. Each option's reference entry states what it refuses.

Host code that builds configuration objects is trusted and runs with Breakwater's own privileges; like caller-provided callbacks and store ports, it remains a trusted executable capability. Breakwater does not defend against host code that builds adversarial objects: getters that change their answers, Proxies, own iterators or methods, or configuration reassigned or mutated after construction.

### Worker to Durable Object

The Worker chooses the object name through `idFromName()`. Each object reasserts the addressed identity:

- runner object: `workflowId:runId` — and the runner object also derives `workflowId:runId` from that same `id.name` on any alarm wake that does not resume, in preference to its own stored record, which is consulted only when the object carries no name; the name is therefore a trusted input to its wake, not only an assertion target;
- thread object: server-minted `threadId` and internal principal header;
- hub object: fixed deployment singleton name;
- provider host object: fixed deployment singleton name.

Use exported topologies. A direct `stub.fetch(request)` into a thread object forwards attacker-controlled headers and is outside the supported boundary.

### Deployment to deployment

Each organization receives a separate Worker, D1 database, and set of Durable Object namespaces. Cloudflare's resource bindings enforce the data-plane boundary; Flowsafe does not implement pooled tenant predicates.

Provisioning is part of the trusted computing base. It must bind each Worker to exactly one organization's resources and stamp the same deployment tag into the Worker configuration and D1 sentinel. Attribution alone is not authorization: routes must still enforce per-user roles and resource rules inside the deployment.

### Runtime to persistent storage

D1 is authoritative for snapshots and flowsafe-owned domains. Compare-and-swap protects approval decisions, schedule fire claims, subscription updates where applicable, and idempotency leases.

The runner snapshot is single-writer by topology rather than version-CAS. Any new writer outside the owning runner Durable Object invalidates that assumption.

### Connector to external system

Breakwater enforces declared hosts and actual HTTP hops only for requests made through `ConnectorRuntime.fetch`. Global fetch, sockets, child processes, and private SDK transports require deployment controls.

Provider output and successful connector output are untrusted data even after transport policy passes.

### Remote executor to workflow host

The host owns the isolation and evidence needed by the [workflow completion verifier](do-runner-design.md#continue-a-retained-workflow-result). Customer code, its result, and its claim to have stopped cannot certify that boundary. Verify completion through trusted evidence bound to the actual executor and invocation before admitting the result.

When accepting permanent effect closure, the host must enforce the isolation profile throughout the invocation’s lifetime. Changes to bindings, egress policy, capabilities or deployment identity must not restore its authority, directly or through retained state. Host notification and capability records remain authoritative; isolated executor data cannot replace them. Residual logging, computation and storage remain subject to confidentiality, accounting and cleanup controls even when workflow effects are permanently closed. Empty remote-step drain inventory does not certify those separate obligations.

### Agent CLI to workspace and host

Claude Code and Codex run as child processes with workspace-edit permissions. Approval authorizes dispatch, not every file or command the child will choose.

The supported adapter avoids a shell, separates prompt from flags, bounds time/output, and sanitizes diagnostics. A timeout terminates the inherited POSIX process group or the Windows process tree and fails distinctly if termination cannot complete. A descendant that deliberately creates a separate POSIX session can leave that group. Operating-system, container, credential, filesystem, process-count, and network isolation therefore remain host responsibilities.

### Notifications, live streams, audit, and SIEM

Approval notifications and live events can carry a full `ApprovalRecord`, including reviewer context. Send them only to an organization-confidential channel or project a lower-sensitivity shape.

Stream tickets authorize an address for a short period. They do not carry a connector grant. The Worker verifies them and each target object rebinds identity.

Agent observation uses Bearer-authenticated newline-delimited JSON. Its offset cursor reconnects only while Mastra's configured replay cache survives. Durable status remains authoritative; the default in-memory event replay does not survive process restart.

Audit events can contain actor, workflow, run, deployment, connector, and denial metadata. A fleet-level audit consumer can co-batch deployments, so the security information and event management (SIEM) system must enforce its own access policy.

## Provisioning boundary

Provisioning belongs to the deployer's trusted control plane. Keep provider credentials and resource allocation outside tenant Workers. Flowsafe verifies the deployment sentinel and internal caller identity at runtime.

### Deployment sentinel

Provisioning writes the same stable tag to independent locations:

- the Worker's `DEPLOYMENT_TENANT` variable
- the singleton `flowsafe_deployment.tenant_tag` row in D1

`seedDeploymentIdentity()` accepts only a fresh database or an already valid sentinel for the same tag. It validates the exact table shape and singleton row, and it refuses to adopt an unowned database with any application table. The insert-or-ignore followed by a read means concurrent provisioning cannot replace an existing owner. Seed before application migrations.

`ensureDeploymentIdentity()` validates the environment-to-D1 pair before protected Worker routes and maintenance work. Worker topologies also attach a deployment-specific `DEPLOYMENT_IDENTITY_SECRET` to every Durable Object fetch. Each production object compares that credential in constant time and validates its own environment-to-D1 pair before building storage or serving a route. This caller attestation closes cross-script namespace binding errors. An alarm has no caller request, so it validates only the target pair.

Run termination keeps the same boundary. The Worker sends the trusted execution principal to the owner Durable Object, which persists the exact replay principals before it releases ownership. A post-cleanup retry succeeds only for a persisted principal; other callers receive an opaque `404`. Deadline maintenance uses a system principal and a persisted revision-and-deadline compare-and-swap through the same owner object. A persisted disputed settlement returns `409` before active execution receives a cancellation signal.

External maintenance is a separate, narrower channel. The candidate may relay a fleet-private Ed25519 capability, but it cannot mint one or read `MAINTENANCE_ADMIN_SECRET`. The global dispatcher verifies the public signature and the bound operation, tenant, environment, physical script, specification digest, expiry, and nonce before it invokes customer code. The trusted maintenance object verifies the same capability against its static tenant and environment, rejects deployment-identity authorization for maintenance routes in this mode, consumes mutation nonces atomically, and signs the exact result with its per-state HMAC secret. The trusted provisioning control plane accepts only that signed result. Status verification is read-only, and maintenance mutation is bounded by a request timeout shorter than the active lease.

Platform-authored WFP catalogs use the signed dispatcher channel with a local Maintenance object. The catalog receives the public verifier and retains its receipt secret; its runtime verifies script and digest claims against the local managed bindings. Required-capability maintenance relays the capability even when that receipt secret is configured. Static administrative routes retain their shared-secret boundary. The control-plane host owns the global dispatcher identity and verifier configuration.

The execution-fence and inventory admin routes use a stricter `MAINTENANCE_ADMIN_SECRET` boundary than the maintenance routes: they return `503` when the secret is absent and never delegate authentication to fleet capabilities. The fence can halt every execution entry in the deployment, so the host owns and audits transition policy. The deployment-identity gate still runs first; a mismatched binding or sentinel cannot use fence administration to reach an unverified database.

A missing binding, invalid sentinel schema, missing or extra owner row, malformed tag, caller-credential mismatch, or tag mismatch fails closed. The Worker returns `503`; Durable Object initialization refuses the request.

### Opaque server-minted ids

The host mints path-safe run and thread ids. `RunnerRuntime` requires a run id, and each Durable Object checks request identity against its own `id.name`. These ids scope local records and object addresses but contain no organization identity.

### Deployment-wide stores

Approval, subscription, schedule, task, notification, memory, and runtime tables contain deployment records without tenant predicates. First-time sentinel provisioning refuses every pre-existing application table, including a legacy pooled schema. An operator must provision a fresh database rather than preserve pooled rows under a weaker schema.

Physical isolation does not replace authorization between users in one deployment. `flowsafe_resource_owners` binds each server-minted run, thread, and schedule, plus each validated host-owned resource key, to its creating human or automated principal. Schedule-fired runs inherit the schedule owner, and signal-woken runs inherit the thread owner; unattended execution does not silently transfer ownership to its system or service principal. Operators and builders cannot see another principal's resources. Reviewers and viewers can read existing resources, and admins can administer them. Resource routes return `404` before a role error when access is denied.

### Audit attribution is not authority

`deploymentTag` comes only from the verified infrastructure binding. Approval audit sinks record it as `detail.deploymentTag`. Breakwater's agent audit contract retains the field name `tenantId`; Flowsafe populates that field only from the verified deployment tag. Externally authored Workers send audit events through a private trusted-state service. That service authenticates the deployment identity and places the entire untrusted event below a canonical envelope whose tenant, environment, and script attribution comes only from static bindings. The envelope explicitly marks event semantics as untrusted: action, decision, resource, and detail remain candidate claims. Forged top-level or nested attribution never becomes queue attribution.

No route accepts deployment identity from a bearer claim, header, query, body, schedule metadata, signal, model output, or run id.

## Approval capability invariant

The trusted suspension bridge records:

- workflow and run;
- step path;
- `suspendedAt`;
- `resumeCount`;
- server-authored connector ids;
- an explicit grant scope;
- Mastra `toolCallId` for durable-agent tool-call approvals;
- requester attribution;
- optional server-authored durable-agent resume target.

It files no record for a step in `RunSummary.suspensionTimers`. Only the run's Durable Object fills that list, from its own wake state.

`approvalGrantProvider()` reads only approved records. A durable-agent record produces `tool-call` scope and binds connector, workflow, run, step path, `suspendedAt`, `resumeCount`, and `toolCallId`. A workflow record produces `suspension` scope and binds every field except `toolCallId`, which Mastra cannot reproduce for an arbitrary workflow gate. The runtime-owned resume count distinguishes repeated same-step suspensions even when timestamps collide.

An agent resume target contains the agent, thread, resource, and original authorized principal. A reviewer decision resumes execution as that principal after re-authorizing it against the current catalog. The host checks a human against the agent's roles and an automated principal against its `allowedAutomation` declaration. It then checks any `requiredPermissions` through the current server-owned resolver policy. The reviewer cannot replace the principal. Legacy agent approvals without this principal fail closed.

An explicit trusted `runScoped: true` record produces `run` scope and is a standing grant. A step-less record without that flag grants nothing. Legacy rows without explicit scope and malformed grants fail closed.

The capability is retryable, not one-shot. Mastra persists the durable tool-call ID and reuses it while retrying that call. A new model call receives a new ID and requires approval. Connector idempotency remains responsible for replaying a completed side effect.

Input digests are not part of the grant because canonical serialization and redaction behavior are undefined. One-shot nonces are also excluded because consuming them atomically without breaking retry or eviction recovery requires a connector-execution transaction that Mastra does not expose.

The HTTP create route is disabled by default. When enabled, it requires write access to the named run and cannot set connectors, attribution, fingerprint, run scope, or a resume target. The resulting record can collect a decision but cannot resume execution.

The provider returns the grant key on every leg, including an empty array, so prior context is overwritten rather than inherited.

## Separation of duties

`ApprovalService` denies:

- an actor deciding a request they requested;
- a non-exempt actor deciding a later gate after approving an earlier gate that advanced the run.

The earlier-gate check pages complete approved history. It also counts an approval whose resume was refused because the step had left that suspension, so that reviewer is barred from the step's current gate although the approval advanced nothing; the check fails closed. A deployment can configure a role or global exemption, and permitted self-decision is audited.

A host with only one human reviewer must consciously choose availability or separation of duties.

## Threats and controls

| Threat | Control | Residual |
| --- | --- | --- |
| Client forges approval in resume body | Stored grant derivation; raw resume is grant-free | A workflow must protect the actual side effect with a connector, not trust `resumeData.approved` |
| Old approval reused at another gate | Exact step, `suspendedAt`, and `resumeCount` match for grant minting and for the decision's resume | Trusted run-scoped grants intentionally span legs; a resumer that builds its own resume without `expectedSuspensionFor(record)`, and a step record without a captured suspension time (one from an earlier version, or one trusted code filed without `suspendedAt` and without connectors), resume the step's current suspension unchecked |
| Reviewer races another reviewer | Store CAS and terminal-state immutability | Batch decisions are partial, not globally transactional |
| Reviewer approves their own action | Requester and cross-gate history checks | Explicit exemptions weaken this control |
| Requester decides the gate after a timeout | A suspension-deadline resume keeps the run's recorded requester, so separation of duties applies to the next gate as it would without the timeout | A run that records no requester, or whose recorded requester is the reserved deadline principal, resumes under that principal |
| Gate waits without an approval | The run object lists a timer step only for an armable deadline it holds a wake for; the bridge files every unlisted step, and an unfiled gate mints no grant | Untrusted data spread into a suspend payload can delay a gate until its deadline |
| Run state nested past the depth SQLite's JSON functions parse escapes the settled-row guard, the deadline sweep and retention | Start and resume refuse tenant JSON nested more than 256 levels with 400, an agent start its caller's messages and context; schedule create and update refuse a target value nested that deep, and the tick fails such a stored target without writing it into a dispatch record; `FencedWorkflowsStorageD1` refuses to store a snapshot SQLite cannot parse and the run fails at once; the guard and the liveness touch decide a row stored earlier by reading it; the tick lists and settles a deferred schedule fire whose metadata SQLite cannot parse, and a reconcile failure does not stop the due fires | Rows stored by an earlier version stay invisible to the deadline sweep and retention, and keep a drain proof non-empty, until an operator terminates and removes them; a leg run by an earlier flowsafe version during a deploy or rollback is not aborted over such a row, and its next write lands over the settlement; a deferred schedule fire an earlier version stored with such a target is settled `failed` when its status lookup throws, even if its run started; an agent thread whose memory recalls such a value fails each start, and a model could emit tool arguments nested that deep after a prompt injection in content the agent reads |
| Caller JSON nested past what the Workers runtime can serialize is stored, or answers an unaudited `500`, `502` or `503` | Schedule `metadata` values, the `payload` of an approval created over HTTP, a state signal's `value` or `delta`, a notification signal's `payload`, subscription `metadata` values, an agent start's `providerOptions` values at the thread host, and an event at the audit proxy are refused with `400` when nested more than 256 levels | An approval filed in process is not bounded, including the bridge's approval for a suspension, whose payload can carry tenant input or tool arguments a model emitted; the approval store keeps it as text. Values an earlier version stored keep their nesting |
| A leg's stale write removes a recorded cancellation or timeout intent | The guarded upsert keeps the stored run lifecycle when a write carries an older revision or none, and the stored intent when the write has the same revision without one, over an unsettled row. A write over a recorded intent that records a disputed economic operation, or one of the same revision that moved the deadline a run-deadline intent was recorded for, is stored as written, and the runtime's own end-of-leg write applies the same two exceptions before it goes through the guarded upsert; the intent patch lands only over the run lifecycle it read and never over a stored dispute, so storage does not pair an intent with a dispute or a deadline it was not checked against | A leg run by an earlier flowsafe version can still remove the intent during a deploy or rollback, or leave it beside a dispute or a moved deadline. At a lower revision than the stored lifecycle, such a leg drops its own resume's dispute and extended deadline, so the run can be recorded `cancelled` or `timed_out` with its `result` cleared and no dispute stored. A terminate or a run-deadline timeout run by an earlier version can still land an intent over a lifecycle it did not read. A stale write stored as written over an intent replaces the whole lifecycle, so it can restore a dispute or an earlier deadline that a later resume had replaced |
| A leg on another instance keeps running, and stores its run again or writes over a later run started under the same id, after the run was settled elsewhere and retention removed it | The liveness touch aborts a leg whose row is settled, and, only on a storage whose capability implements `withStoredRun`, a leg whose row is gone after the leg stored it; the settled-row guard refuses its writes over a settled row, and any write whose run provenance names another start than the stored row's; on `FencedWorkflowsStorageD1` a leg's write never inserts a run row it already stored, and its end-of-leg write answers `409`; run ids are single-use | A step in flight that does not pass its `abortSignal` runs to its end, and a workflow with no write between steps runs its next step until a touch reaches D1; legs run by an earlier flowsafe version, and storage whose capability lacks `withStoredRun`, store a removed run again as unsettled, and legs run by an earlier flowsafe version write over a later run of the same id; a host that starts a removed run's id again leaves a leg of the earlier run running until its next write, and a write or row without a start token is not refused for it, nor is a write over a row SQLite cannot parse (`writeOverUnreadableRow`), which compares no start token, nor a read-modify-write such as `updateWorkflowState`, which keeps the stored row's run provenance and so its start token; the `withStoredRun` guard covers only the leg's own run address, so writes of a nested workflow a step runs, and of another run a step drives directly through Mastra rather than through `RunnerRuntime`, are outside it (a run a step starts through the runtime gets its own scope) |
| Retention removes a run's terminal row before the object that started the run has read it | Retention keeps a terminal snapshot while its run's `run` ownership claim is reserved; the run object and the agent thread object reserve that claim before admission and commit it once they have read the start's outcome, recording that read in the start journal first, so a row missing after the read clears the journal | A purge by an earlier flowsafe version during a deploy, by a direct caller that passes no `resourceOwnerTable` or whose registry table does not exist yet, or of a run whose host records its ownership claims outside the purged registry table, can remove such a row before the read, as can a purge after the object's recovery found no row and released the claim before an admission on an outgoing instance landed. A start journal an earlier flowsafe version wrote records no read, so a purge of its run before the object's first wake on this version reads the run has the same effect, and a journal already in that state when the deployment upgrades stays in it. Such a start answers `409` or `503`, its object keeps the journal and wakes every minute, the run's `dispatch-status` answers `503` so a scheduled fire of the run stays deferred, and an agent thread stays blocked by the run; flowsafe does not clear such a journal. A `run` claim that stays reserved, because a start's journal was lost or never resolves, keeps the run's terminal snapshot and its artifacts past the retention horizon, and the drain inventory lists it under `resource-owners` |
| Approval resume fails after commit | Decision stays durable; trusted redrive/registry rehydration invokes [only Breakwater's reserved `processInput` steps](durable-agents.md#host-a-guarded-agent-catalog), then restores complete processor lists for resumed loop hooks | Operator may need to redrive; no automatic rollback |
| Approval id from another deployment read or changed | Physical deployment identity selects the approval store; every authenticated approval role may read its deployment queue, while review-role, separation-of-duties, and CAS checks guard mutations; exact workflow and run predicates bind grants | A mis-provisioned database is detected by the deployment sentinel before routes are served |
| Foreign run or thread reached | Exported topology, opaque object names, path-safe id checks, and exact Durable Object identity checks | Infrastructure with access to another deployment's bindings can cross the physical boundary |
| Foreign or mismatched agent binding reached | Server catalog plus persisted thread/run/agent binding checks before mutation authorization | Raw namespace access outside the agent topology is unsupported |
| Client smuggles memory id | Recursive body rejection, server-minted thread ids, and validated resource keys | Application-specific aliases must not bypass the constructors |
| Schedule row plants grant/context | Reserved-key rejection and runtime-last merge | Direct database writers remain part of the trusted computing base |
| Reviewer becomes agent execution principal | Persist the original execution principal in the approval target and re-authorize it on resume: roles for a human, `allowedAutomation` for an automated principal, then any `requiredPermissions` | The stored principal is an authorization snapshot, not a dynamic identity-provider lookup |
| Automation acquires a human's authority | Automated work carries a non-human `ExecutionPrincipal` with a required `purpose`; Breakwater gates on `allowedPrincipalKinds` before roles and never consults the role allowlist for a non-human kind; the agent host requires an `allowedAutomation` declaration naming the kind and the exact entry path | Host code inside the trusted computing base still vouches for its own automated principals |
| Client supplies effective permissions | `AgentMeta`, `PermissionManifest.requiredPermissions`, and `PrincipalPermissionResolver` remain server-owned; the resolver receives only the trusted `ExecutionPrincipal`, and the `breakwater.principalPermissions` projection is reserved at every external boundary | A faulty host resolver can grant excessive permissions |
| Required permission policy is missing or unavailable | The thread host denies a permission-requiring agent when no resolver is configured, the resolver fails, or its output is malformed; any other run proceeds with a `null` projection, so a permission-declaring connector inside it denies | A resolver outage makes permission-protected agents and connectors unavailable |
| Approval elevates an unauthorized principal | The connector wrapper checks `requiredPermissions` against the trusted projection before the dry-run branch and before approval-grant consumption | Delegated execution, where an approval should confer authority, is not modeled |
| Automated principal mutated after it is vouched | `trustAutomationPrincipal()` returns a branded, frozen canonical clone; the trusted service entries recheck the own brand, the shape, the kind, and that every field is a plain data property, rather than trusting the erased parameter type | In-process code can recover the brand by reflection from any vouched principal and stamp a frozen object of its own; this boundary does not defend against hostile code in the same process |
| Vouched principal answers differently on a later read | Accessor properties are refused outright: a getter survives `Object.freeze`, and the trusted entries read a principal several times per call | A caller inside the trusted computing base can still pass a plain object built to its own liking |
| Webhook names another subscription | Verify raw bytes first; resolve routing from the stored subscription and provider configuration | Provider secret compromise can forge provider events |
| Provider alarm lost after subscription | Post-commit reconcile callback and retryable mutation-applied response | Hosts that omit reconciliation must arm polling themselves |
| Duplicate connector side effect | Collision-proof v2 keys, fail-closed legacy inspection, atomic idempotency lease, and shared store | Poor business keys or too-short pending TTL can still duplicate |
| One workload exhausts the deployment budget | Deployment-wide D1 rate state and host-set limits | Fixed-window boundary burst remains |
| Connector redirects to attacker host | Manual per-hop guarded fetch | Transport outside runtime fetch is invisible; such a connector declares `egressEnforcement: 'declaration-only'`, readable through `connectorEgressPosture()` and audited as `detail.egressEnforcement` |
| Credentials forwarded on redirect | Cross-origin credential-header stripping | Connector body may itself contain secrets |
| Prompt becomes CLI flag | Wrapper-owned `--` and `--flag=value` | Vendor semantics can change; packed consumer tests pin current definitions |
| Prompt/output leaks in error or audit | Static errors, redacted command, bounded metadata, safe audit registry | Successful functional text remains sensitive and caller-owned |
| Internal failure text, such as a storage or deployment-identity message, reaches an HTTP caller | Public routes answer an unclassified failure with a fixed message, `internal error` or the agent router's `internal server error`, and log the original; the readers of a run or thread object's answer, the signal ingestion router and the stream router's WebSocket upgrade routes replace the message of a `5xx` that carries no reason code before a router forwards it; the approval router, batch decide and `DecideResult.resume.error` report an unexpected failure the same way | A `RunRouteError` a host authors is forwarded as written; the run object answers the original message to the Worker that called it; a thrown value with a 4xx `status`, such as an HTTP-client error a host's resume function lets escape, keeps its message in `DecideResult.resume.error`; a failed run's own error, which its steps produce, reaches callers in its summary's `error`, in `DecideResult.resume.summary`, in the agent run envelope and in the agent stream's terminal error event |
| PII or secret in model output | Multi-channel detectors, classifier seam, optional hold-back | Encodings and adversarial transformations can evade detection |
| Untrusted signal bypasses the initiating input processor | Optional Thread Durable Object content policy over Mastra's canonical XML before delivery; providers, schedules, and notification dispatch converge there | A host that omits the optional policy accepts structurally valid signal content; application policy quality remains host-owned |
| Open approval or suspended run age-purged | Terminal-only retention | Abandoned live records require operator disposition |
| R2 artifacts stranded | Artifact deletion paired before snapshot row | A host that omits the artifact store from decommissioning can strand objects |
| Application Worker binds another deployment's R2 bucket | The trusted provisioning control plane must derive names, retain ownership claims, authorize creation, and verify exact bindings | Control-plane compromise remains inside the trusted computing base |
| Application secret changed outside the trusted provisioning control plane | The control plane must verify plaintext against declared digests before upload and attest the secret-name set during inventory | Cloudflare exposes no value digest, so recurring audit cannot detect value-only drift |
| Deployment decommissioning races new work | Revoke traffic and credentials before deleting the resource set | In-flight work can return errors during the drain |
| Provisioning credential follows a redirect to an attacker host | The trusted provisioning control plane must refuse redirects before sending credentials to a new origin | A transport that follows redirects before the control plane inspects the response can expose the credential |
| Traffic served by an unattested version or script | The trusted provisioning control plane must attest routing after promotion and refuse to record a release as ready while it disagrees | Traffic can reach a promoted version before attestation completes; another authorized provider writer can change routing after observation |
| Audit sink outage blocks agent | Sink failure containment and ring buffer | In-memory buffer is not durable and can drop old events |
| Notification exposes reviewer payload | Host projection obligation | Flowsafe cannot know the trust level of a transport |
| Stream ticket replay | Short TTL, HMAC, address binding, Worker verification | Ticket in logs or browser history is usable until expiry |
| Agent event replay requested after restart | Return 409 and direct the client to durable status | Event history is available only for the configured cache window |

## Guarded agent boundary

`createGuardedAgent()` fixes RBAC, policy, execution limits, tool choice, and application processor ordering at construction. Its public handle exposes only unstructured `generate()` and `stream()`, both with a mandatory `RequestContext`. It rejects structured output because Mastra exposes parsed values to messages and persistence before a wrapper could inspect them.

The handle rejects unknown call options even when a key is present with `undefined`, and the factory rejects object-only policies that no supported invocation can cover. Policy selectors and evaluator callables are captured at construction so later caller mutation cannot change channel coverage or replace the evaluator; mutable state internal to an evaluator remains trusted application state. The handle disables background continuations, forces policy hold-back, and keeps the raw Mastra agent package-private. Flowsafe validates an unforgeable package-local brand and a versioned host protocol before accepting a catalog module. Its lower-level durable methods snapshot data-property options before validation and delegation, reject accessors, and apply the [durable call-option restrictions](durable-agents.md#durable-call-options) before Mastra can bypass the narrow handle.

This boundary protects trusted application code from accidental or unsupported invocation paths. It is not a sandbox against hostile code running in the same process; such code can import Mastra, construct another agent, access host credentials, or bypass the supported HTTP topology.

The guarded agent admits user file and image network URLs, and network URLs in the tool outputs a call supplies, only from the `http(s)` origins listed in `allowedInputAssetOrigins`; `data:` URLs are inline data. Mastra or a model provider may fetch an admitted URL, or a URL the check leaves unchecked: Mastra itself downloads the assets of signals the durable loop drains before its first model step. The host trusts listed origins: input policies do not read downloaded content, and Mastra follows redirects, so an open redirect on a listed origin reaches its target. See [Input asset origins](../packages/breakwater/README.md#input-asset-origins) for coverage, limits, and refusal behavior.

## RBAC model

Breakwater exports these role labels:

```text
admin
builder
operator
reviewer
viewer
```

`RBACMiddleware` performs only a configured `allowedRoles` membership check around an agent model turn.

Flowsafe applies separate role sets in its run router, approval service, signal/objective/schedule/subscription routers, and deployment configuration. The exact allowed operation is defined by that service, not an implied global hierarchy.

The role names do not themselves implement:

- user provisioning;
- role inheritance;
- organization membership;
- resource ACLs;
- OIDC or session validation;
- business-specific authorization.

## Agent and connector permission model

Flowsafe adds explicit host-owned permissions to the guarded agent boundary without adding role inheritance. `Permission` identifiers use canonical lowercase dotted form and are defined by breakwater, so the agent-entry gate and the connector invocation gate share one grammar. `AgentMeta.requiredPermissions` uses all-of semantics, so the principal must hold every identifier in the list.

Catalog validation rejects `requiredPermissions` when it is not an array, is empty, contains duplicates, or contains malformed identifiers. Agents that omit the field keep their existing `allowedRoles` and `allowedAutomation` behavior.

Configure `ThreadAgentHostOptions.resolvePrincipalPermissions` with a `PrincipalPermissionResolver`. The resolver receives only the trusted human, service, agent, or system `ExecutionPrincipal`. It returns a `PrincipalPermissionResolution` containing effective permissions and `policyVersion`.

The thread host calls the resolver on every authorized entry, after the existing human-role or automated-entry gate succeeds. A required-permission agent fails closed when the resolver is absent, throws, rejects, or returns malformed permissions or policy version. A role-only agent invokes a configured resolver but does not require it: a failed resolution starts the run with a `null` projection and an `agent.permissions.resolve` error event.

Connector invocation authorization is the composed second half. `PermissionManifest.requiredPermissions` declares the all-of list a connector demands; breakwater enforces it inside `createConnector()`'s execute path against the trusted `breakwater.principalPermissions` projection, before the dry-run branch and before approval-grant consumption — a valid approval never elevates a principal that may not invoke the connector at all. Flowsafe's thread host mints the projection from the resolver's output on every start and resume leg (an explicit `null` when no resolution exists, so a stale persisted projection is retired rather than inherited); a workflow host may mint the key from its own trusted `RequestContextProvider`. A path with no projection denies every permission-declaring connector.

## Audit contract

Breakwater's event shape is:

```typescript
interface AuditEvent {
  timestamp: string;
  actor: { id: string; role: Role } | null;
  action: string;
  resource: string;
  decision: 'allowed' | 'denied' | 'error';
  reason?: string;
  detail?: Record<string, unknown>;
}
```

Flowsafe's `ApprovalAuditEvent` is structurally compatible with the input
accepted by `AuditLogger.record()` and includes deployment tag, workflow, run,
approval, or operation detail where applicable. It intentionally has no `timestamp`;
`AuditLogger.record()` adds one. The default Flowsafe host sink logs and queues
the raw, unstamped event.

Example after routing a Flowsafe event through `AuditLogger.record()`:

```json
{
  "timestamp": "2026-07-26T10:00:00.000Z",
  "actor": {
    "id": "reviewer-42",
    "role": "reviewer"
  },
  "action": "approval.decide",
  "resource": "approval:7d4b",
  "decision": "allowed",
  "detail": {
    "deploymentTag": "acme",
    "workflowId": "release",
    "runId": "9d3f4a21"
  }
}
```

Do not assume every event has the same action vocabulary or resource object. Use the string contract and documented detail for each producer.

An `agent.entry.authorize` event for a permission-protected agent records `requiredPermissions` and `permissionPolicyVersion`. The version is `null` when no valid resolution exists. A failed resolution on a role-only agent emits a separate `agent.permissions.resolve` error event. Breakwater's connector gate mirrors the contract: `connector.authorize` and its denial events record the manifest's `requiredPermissions` and the projection's `permissionPolicyVersion`. None of these events include effective permissions or identity-provider groups.

## Secrets

Anchorage does not include a generic secret resolver.

Store deployment secrets in Cloudflare Secrets or an external manager and inject only what each component needs. Keep separate keys for:

- internal Worker-to-Durable-Object caller attestation;
- identity/session verification;
- stream tickets;
- OAuth client credentials;
- webhook providers;
- model providers;
- business connectors;
- SIEM authorization.

A trusted provisioning control plane must verify plaintext secrets against the intended binding digests before provider access and keep plaintext out of durable records and logs. A secret rotation changes the deployment specification, but provider inventory can verify only the secret name after upload.

Do not persist raw keys in workflow input, suspend payload, approval context, schedule request context, notification body, audit detail, artifact metadata, or fleet state.

An Agent CLI inherits its environment unless an injected executor changes it. Supply an allowlisted environment in the process/container boundary.

## Deployment obligations

Before a public endpoint:

1. Replace the static token example with a verified production identity seam.
2. Set `DEPLOYMENT_TENANT`, seed the same deployment sentinel in every new bound D1 database before application migrations, and prove a mismatch returns `503` before serving protected routes.
3. Configure and version `resolvePrincipalPermissions` before registering an agent or connector with `requiredPermissions`; permission-declaring connectors deny on any path without a trusted projection.
4. Use D1-backed deployment-wide connector stores across per-run objects.
5. Treat connector budgets and idempotency as deployment-wide unless the application defines a separate, non-tenant logical isolation scope.
6. Before enabling v2 idempotency writes, stop and drain all legacy writers sharing the store. Never replay an ambiguous scoped or colon-bearing legacy row without external proof of its exact tuple; migrate proven D1 rows only through the connector-bound atomic helper so output validation, exact-row guards, and v1 deletion share the supported transaction.
7. Route connector HTTP through `runtime.fetch` and add infrastructure egress policy.
8. Keep approval connector lists and durable-agent resume targets server-authored.
9. Mount only configured optional routers.
10. Configure retention and a resource-set decommissioning procedure for every adopted domain. Delete application Workers before R2, prove every bucket is detached and empty, and never auto-purge application objects.
11. Protect Durable Object namespaces behind the Worker topologies and set a distinct `DEPLOYMENT_IDENTITY_SECRET` for caller attestation.
12. Project notifications and audit to the receiving channel's trust level.
13. Isolate Agent CLI workspaces and review their diffs. On Windows, preserve a drive-absolute local `SystemRoot` or `WINDIR`; the built-in executor rejects relative, root-relative, UNC, and device paths before launch rather than searching the workspace or `PATH` for taskkill.
14. Run the deterministic workerd restart, forgery, and deployment-sentinel mismatch proof.
15. Keep application KV unsupported, bind only fleet-owned application R2 resources, and treat secret inventory as name-only attestation.

Report vulnerabilities through [`SECURITY.md`](../SECURITY.md), not a public issue.
