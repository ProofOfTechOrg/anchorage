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

Only trusted host/runtime code may populate actor, grant, principal-permission, or workflow values. Idempotency keys and dry-run selection may originate from authorized application logic, but must not overwrite the other keys. `breakwater.isolationScope` remains an opaque Breakwater policy input; Flowsafe does not mint it for the single-organization data plane.

Flowsafe's shared execution-context boundary reserves every `breakwater.*` key, `mastra:goal`, `runId`, `threadId`, `resourceId`, `__proto__`, `constructor`, and `prototype`. External HTTP bodies reject these fields. Persisted compatibility paths strip them before trusted derivation.

Trusted merges apply sanitized external or stored context first, then workflow, run, current execution identity, structured connector grants, and trusted actor/audit correlation. Provider-supplied isolation scope is dropped. An empty grant array overwrites any stale value, and the agent host projects the principal-permission resolution or an explicit `null` on every leg so a stale persisted projection cannot survive a resume.

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

### Agent CLI to workspace and host

Claude Code and Codex run as child processes with workspace-edit permissions. Approval authorizes dispatch, not every file or command the child will choose.

The supported adapter avoids a shell, separates prompt from flags, bounds time/output, and sanitizes diagnostics. A timeout terminates the inherited POSIX process group or the Windows process tree and fails distinctly if termination cannot complete. A descendant that deliberately creates a separate POSIX session can leave that group. Operating-system, container, credential, filesystem, process-count, and network isolation therefore remain host responsibilities.

### Notifications, live streams, audit, and SIEM

Approval notifications and live events can carry a full `ApprovalRecord`, including reviewer context. Send them only to an organization-confidential channel or project a lower-sensitivity shape.

Stream tickets authorize an address for a short period. They do not carry a connector grant. The Worker verifies them and each target object rebinds identity.

Agent observation uses Bearer-authenticated newline-delimited JSON. Its offset cursor reconnects only while Mastra's configured replay cache survives. Durable status remains authoritative; the default in-memory event replay does not survive process restart.

Audit events can contain actor, workflow, run, deployment, connector, and denial metadata. A fleet-level audit consumer can co-batch deployments, so the security information and event management (SIEM) system must enforce its own access policy.

## Provisioning boundary

Physical isolation replaces request-level tenant predicates. The following invariants define the boundary.

### One organization per resource set

A data-plane Worker serves one organization. Its D1 database, Durable Object namespaces, fleet-owned application R2 buckets, and secrets must not be shared with another organization. A shared audit queue is allowed only behind trusted infrastructure that derives attribution from static deployment bindings; externally authored code receives neither its producer binding nor reusable control-plane credentials.

The control plane can remain multi-tenant because it provisions and audits the fleet. It must keep a one-to-one mapping from organization to data-plane resources. It derives physical R2 names and persists permanent ownership claims before binding a bucket. Application code cannot select a provider bucket name or identifier.

Only an application Worker receives application variables, application secrets, or application R2 bindings. Trusted state, shared outbound, dispatcher, and audit Workers receive none. A legacy backend-switch bridge retains the prior plain release's bindings only while it still serves application fetches. The target external candidate receives the target release's bindings, and finalization removes all application bindings from the state-only bridge.

Fleet control rejects application KV bindings. Cloudflare's [1,000-namespace account limit](https://developers.cloudflare.com/kv/platform/limits/) cannot support the 10,000-deployment horizon with one namespace per deployment. A shared application namespace would make logical key partitioning the tenant boundary. The shared `HOSTS` KV namespace remains a control-plane routing index and is not exposed to application code.

### Deployment sentinel

Provisioning writes the same stable tag to two independent locations:

- the Worker's `DEPLOYMENT_TENANT` variable
- the singleton `flowsafe_deployment.tenant_tag` row in D1

`seedDeploymentIdentity()` accepts only a fresh database or an already valid sentinel for the same tag. It validates the exact table shape and singleton row, and it refuses to adopt an unowned database with any application table. The insert-or-ignore followed by a read means concurrent provisioning cannot replace an existing owner. Seed before application migrations.

`ensureDeploymentIdentity()` validates the environment-to-D1 pair before protected Worker routes and maintenance work. Worker topologies also attach a deployment-specific `DEPLOYMENT_IDENTITY_SECRET` to every Durable Object fetch. Each production object compares that credential in constant time and validates its own environment-to-D1 pair before building storage or serving a route. This caller attestation closes cross-script namespace binding errors. An alarm has no caller request, so it validates only the target pair.

Run termination keeps the same boundary. The Worker sends the trusted execution principal to the owner Durable Object, which persists the exact replay principals before it releases ownership. A post-cleanup retry succeeds only for a persisted principal; other callers receive an opaque `404`. Deadline maintenance uses a system principal and a persisted revision-and-deadline compare-and-swap through the same owner object. A persisted disputed settlement returns `409` before active execution receives a cancellation signal.

External maintenance is a separate, narrower channel. The candidate may relay a fleet-private Ed25519 capability, but it cannot mint one or read `MAINTENANCE_ADMIN_SECRET`. The global dispatcher verifies the public signature and the bound operation, tenant, environment, physical script, specification digest, expiry, and nonce before it invokes customer code. The trusted maintenance object verifies the same capability against its static tenant and environment, rejects deployment-identity authorization for maintenance routes in this mode, consumes mutation nonces atomically, and signs the exact result with its per-state HMAC secret. Fleet control accepts only that signed result. Status verification is read-only, and maintenance mutation is bounded by a request timeout shorter than the active lease.

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

The earlier-gate check pages complete approved history. A deployment can configure a role or global exemption, and permitted self-decision is audited.

A host with only one human reviewer must consciously choose availability or separation of duties.

## Threats and controls

| Threat | Control | Residual |
| --- | --- | --- |
| Client forges approval in resume body | Stored grant derivation; raw resume is grant-free | A workflow must protect the actual side effect with a connector, not trust `resumeData.approved` |
| Old approval reused at another gate | Exact step, `suspendedAt`, and `resumeCount` match | Trusted run-scoped grants intentionally span legs |
| Reviewer races another reviewer | Store CAS and terminal-state immutability | Batch decisions are partial, not globally transactional |
| Reviewer approves their own action | Requester and cross-gate history checks | Explicit exemptions weaken this control |
| Approval resume fails after commit | Decision stays durable; trusted redrive/registry rehydration invokes only RBAC's initial `processInput` hook, then restores complete processor lists for resumed loop hooks | Operator may need to redrive; no automatic rollback |
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
| Connector redirects to attacker host | Manual per-hop guarded fetch | Transport outside runtime fetch is invisible |
| Credentials forwarded on redirect | Cross-origin credential-header stripping | Connector body may itself contain secrets |
| Prompt becomes CLI flag | Wrapper-owned `--` and `--flag=value` | Vendor semantics can change; packed consumer tests pin current definitions |
| Prompt/output leaks in error or audit | Static errors, redacted command, bounded metadata, safe audit registry | Successful functional text remains sensitive and caller-owned |
| PII or secret in model output | Multi-channel detectors, classifier seam, optional hold-back | Encodings and adversarial transformations can evade detection |
| Open approval or suspended run age-purged | Terminal-only retention | Abandoned live records require operator disposition |
| R2 artifacts stranded | Artifact deletion paired before snapshot row | A host that omits the artifact store from decommissioning can strand objects |
| Application Worker binds another deployment's R2 bucket | Fleet-derived names, permanent ownership claims, persisted create authorization, and exact binding inventory | Control-plane compromise remains inside the trusted computing base |
| Application secret changed outside fleet control | Trusted plaintext input is digest-checked before upload; inventory attests the exact secret-name set | Cloudflare exposes no value digest, so recurring audit cannot detect value-only drift |
| Deployment decommissioning races new work | Revoke traffic and credentials before deleting the resource set | In-flight work can return errors during the drain |
| Audit sink outage blocks agent | Sink failure containment and ring buffer | In-memory buffer is not durable and can drop old events |
| Notification exposes reviewer payload | Host projection obligation | Flowsafe cannot know the trust level of a transport |
| Stream ticket replay | Short TTL, HMAC, address binding, Worker verification | Ticket in logs or browser history is usable until expiry |
| Agent event replay requested after restart | Return 409 and direct the client to durable status | Event history is available only for the configured cache window |

## Guarded agent boundary

`createGuardedAgent()` fixes RBAC, policy, execution limits, tool choice, and application processor ordering at construction. Its public handle exposes only unstructured `generate()` and `stream()`, both with a mandatory `RequestContext`. It rejects structured output because Mastra exposes parsed values to messages and persistence before a wrapper could inspect them.

The handle rejects unknown call options even when a key is present with `undefined`, and the factory rejects object-only policies that no supported invocation can cover. Policy selectors and evaluator callables are captured at construction so later caller mutation cannot change channel coverage or replace the evaluator; mutable state internal to an evaluator remains trusted application state. The handle disables background continuations, forces policy hold-back, and keeps the raw Mastra agent package-private. Flowsafe validates an unforgeable package-local brand and a versioned host protocol before accepting a catalog module. Its lower-level durable methods snapshot data-property options before validation and delegation, reject accessors, and reject guarded structured output before Mastra can bypass the narrow handle.

This boundary protects trusted application code from accidental or unsupported invocation paths. It is not a sandbox against hostile code running in the same process; such code can import Mastra, construct another agent, access host credentials, or bypass the supported HTTP topology.

## RBAC model

Breakwater exports five role labels:

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

Application binding descriptors contain a name and the UTF-8 SHA-256 of the intended value. Supply the plaintext map only through the trusted fleet-control invocation seam. Fleet control requires exact descriptor keys, verifies every digest before provider access, and excludes plaintext from durable records, release identity, logs, and errors. A secret rotation changes the specification digest, but provider inventory can verify only the secret name after upload.

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
