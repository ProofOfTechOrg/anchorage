# Security threat model

Anchorage adds enforcement to a Mastra application, but the application owner remains responsible for identity, business authorization, deployment isolation, provider credentials, and incident response.

This document covers breakwater and flowsafe at their supported public boundaries. It does not treat a model, vendor CLI, third-party connector, OAuth provider, Cloudflare account, or Mastra itself as trusted merely because Anchorage integrates with it.

## Security objectives

Anchorage is designed to preserve these properties:

1. A connector side effect runs only after all configured tool policies pass.
2. A human approval grants only the named connector on the exact authorized suspension.
3. A client, model, signal, workflow input, or raw resume cannot mint a capability.
4. One tenant cannot read, resume, signal, schedule, replay, throttle, or delete another tenant's state through supported routes.
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
| Memory threads, messages, resources, goals, notifications | D1 | Cross-tenant conversation or instruction disclosure |
| Schedules, background tasks, provider subscriptions | D1 | Unauthorized unattended execution |
| Artifacts | R2 | Business-data disclosure or loss |
| Actor and tenant identity | Verified host context | All authorization and isolation can fail |
| Approval, workflow, tenant, run, thread, resource context | Mastra `RequestContext` and topology headers | Capability or namespace forgery |
| Resume ordinals | Durable Object storage | Approved run can strand or mis-bind |
| Model, connector, OAuth, webhook, stream, SIEM, and token keys | Cloudflare secrets or external secret manager | Provider or control-plane compromise |
| Audit and metrics stream | Memory, Logs, Queue, SIEM | Evidence loss or tenant-sensitive metadata disclosure |
| Agent CLI prompt and workspace | Child-process argv and filesystem | Source, secret, or code-integrity compromise |

## Trust boundaries

### Public client to Worker

The Worker must authenticate the request, validate actor and tenant claims, enforce coarse and resource-specific roles, reject client-selected trusted ids/context, cap untrusted bodies, and return 404 for foreign resource ids.

The library supplies verifier and router seams, not an identity provider. A static bearer map in the reference deployment is an inspectable example, not production identity guidance.

The guarded agent router resolves route syntax, authenticates, resolves the server catalog, checks stored tenant/agent/thread/run bindings, authorizes mutations, validates input, then invokes the trusted topology. This order prevents a disallowed role from using error differences to enumerate foreign agents or runs.

### Worker to request context

Breakwater context keys are capabilities or trusted namespace claims:

```text
breakwater.actor
breakwater.approvedConnectors
breakwater.idempotencyKey
breakwater.dryRun
breakwater.workflowScope
breakwater.isolationScope
```

Only trusted host/runtime code may populate actor, grant, workflow, or tenant isolation values. Idempotency keys and dry-run selection may originate from authorized application logic, but must not overwrite the other keys.

Flowsafe's shared execution-context boundary reserves every `breakwater.*` key, `mastra:goal`, `runId`, `threadId`, `resourceId`, `__proto__`, `constructor`, and `prototype`. External HTTP bodies reject these fields. Persisted compatibility paths strip them before trusted derivation.

Trusted merges apply sanitized external or stored context first, then workflow and isolation scope, the exact-leg connector grant, and trusted actor/audit correlation. An empty connector grant overwrites any stale value.

### Worker to Durable Object

The Worker chooses the object name through `idFromName()`. Each object reasserts the addressed identity:

- runner object: `workflowId:runId`;
- thread object: tenant-minted `threadId` and internal tenant header;
- hub object: `tenantId`;
- provider host object: `tenantId`.

Use exported topologies. A direct `stub.fetch(request)` into a thread object forwards attacker-controlled headers and is outside the supported boundary.

### Tenant to tenant

One deployment can share D1 and namespaces. Isolation rests on exact server-minted identities, branded tenant-bound stores, metadata tenant predicates where needed, and topology checks before object addressing.

Attribution alone is not authorization. Every route must still check ownership.

### Runtime to persistent storage

D1 is authoritative for snapshots and flowsafe-owned domains. Compare-and-swap protects approval decisions, schedule fire claims, subscription updates where applicable, and idempotency leases.

The runner snapshot is single-writer by topology rather than version-CAS. Any new writer outside the owning runner Durable Object invalidates that assumption.

### Connector to external system

Breakwater enforces declared hosts and actual HTTP hops only for requests made through `ConnectorRuntime.fetch`. Global fetch, sockets, child processes, and private SDK transports require deployment controls.

Provider output and successful connector output are untrusted data even after transport policy passes.

### Agent CLI to workspace and host

Claude Code and Codex run as child processes with workspace-edit permissions. Approval authorizes dispatch, not every file or command the child will choose.

The supported adapter avoids a shell, separates prompt from flags, bounds time/output, and sanitizes diagnostics. Operating-system, container, credential, filesystem, and network isolation remain host responsibilities.

### Notifications, live streams, audit, and SIEM

Approval notifications and live events can carry a full `ApprovalRecord`, including reviewer context. Send them only to a tenant-confidential channel or project a lower-sensitivity shape.

Stream tickets authorize an address for a short period. They do not carry a connector grant. The Worker verifies them and each target object rebinds identity.

Agent observation uses Bearer-authenticated newline-delimited JSON. Its offset cursor reconnects only while Mastra's configured replay cache survives. Durable status remains authoritative; the default in-memory event replay does not survive process restart.

Audit events can contain actor, workflow, run, tenant, connector, and denial metadata. Queue export deliberately co-batches tenants; the SIEM must enforce its own access policy.

## Tenant invariants

### Server-minted run identity

The host mints `${tenantId}_${uuid}` from authenticated context. `RunnerRuntime` requires it, and `DurableObjectRunner` checks it against `id.name`.

This identity scopes snapshots, runner objects, grants, live run channels, connector isolation, and artifact keys.

### Tenant-bound stores

`D1ApprovalStoreFactory.forTenant()` and `D1SubscriptionStoreFactory.forTenant()` return branded request-scope stores. Their tenant value comes from construction and appears in every predicate.

System views are distinct types used by cron or verified webhook lookup. Request routes cannot obtain them through the normal resolver.

### Exact charset

Tenant ids match `^[a-z0-9]{3,32}$`. This excludes `_` and makes `${tenantId}_` ownership and the D1 range-purge bounds exact. Loosening the charset requires redesigning both properties.

### Tenant-minted memory

Threads and resources use the same prefix discipline. The public boundary rejects bodies naming `threadId` or `resourceId`; the path may reference an owned thread and receives 404 for a foreign one.

Mastra recall-path tests use the real D1 memory store and adversarial same-business-key tenants.

### Metadata-scoped domains

Schedules carry validated `metadata.tenantId` because their slug ids cannot use the salted range pattern. Purge and query use exact JSON metadata predicates.

Webhook ingress derives tenant only from the stored subscription row because a provider webhook has no authenticated Anchorage tenant.

## Approval capability invariant

The trusted suspension bridge records:

- tenant, workflow, and run;
- step path;
- `suspendedAt`;
- `resumeCount`;
- server-authored connector ids;
- requester attribution;
- optional server-authored durable-agent resume target.

`approvalGrantProvider()` reads only approved records and requires an exact match on the current leg. The runtime-owned resume count distinguishes repeated same-step suspensions even when timestamps collide.

An agent resume target contains the agent, thread, resource, and original authorized principal. A reviewer decision resumes execution as that principal after current catalog-role validation; the reviewer cannot replace it. Legacy agent approvals without this principal fail closed.

An explicit trusted `runScoped: true` record is a standing grant. A step-less record without that flag grants nothing.

The HTTP create route is disabled by default and cannot set connectors, attribution, fingerprint, run scope, or resume target when enabled.

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
| Foreign approval id read or changed | Tenant-bound store predicates and 404 | A verifier that assigns the wrong tenant defeats the boundary |
| Foreign run or thread reached | Exact prefix ownership, topology check, DO identity check, 404 | Raw namespace access outside exported topology is unsupported |
| Foreign or mismatched agent binding reached | Server catalog plus persisted thread/run/agent binding checks before mutation authorization | Raw namespace access outside the agent topology is unsupported |
| Client smuggles memory id | Recursive body rejection and server minters | Application-specific aliases must not bypass the minter |
| Schedule row plants grant/context | Reserved-key rejection and runtime-last merge | Direct database writers remain part of the trusted computing base |
| Reviewer becomes agent execution principal | Persist original requester in the approval target and recheck current catalog roles on resume | The stored principal is an authorization snapshot, not a dynamic identity-provider lookup |
| Webhook claims victim tenant | Verify raw bytes first; tenant from subscription row | Provider secret compromise can forge provider events |
| Provider alarm lost after subscription | Post-commit reconcile callback and retryable mutation-applied response | Hosts that omit reconciliation must arm polling themselves |
| Duplicate connector side effect | Atomic idempotency lease and shared store | Poor business keys or too-short pending TTL can still duplicate |
| One tenant exhausts another's budget | Runtime isolation scope in D1 rate key; tenant evaluator | Fixed-window boundary burst remains |
| Connector redirects to attacker host | Manual per-hop guarded fetch | Transport outside runtime fetch is invisible |
| Credentials forwarded on redirect | Cross-origin credential-header stripping | Connector body may itself contain secrets |
| Prompt becomes CLI flag | Wrapper-owned `--` and `--flag=value` | Vendor semantics can change; packed consumer tests pin current definitions |
| Prompt/output leaks in error or audit | Static errors, redacted command, bounded metadata, safe audit registry | Successful functional text remains sensitive and caller-owned |
| PII or secret in model output | Multi-channel detectors, classifier seam, optional hold-back | Encodings and adversarial transformations can evade detection |
| Open approval or suspended run age-purged | Terminal-only retention | Abandoned live records require offboarding |
| R2 artifacts stranded | Artifact deletion paired before snapshot row | A host that omits the artifact store from purge can strand objects |
| Tenant offboarding races new work | Revoke credentials first; delete every adopted domain | In-flight work can return errors during the drain |
| Audit sink outage blocks agent | Sink failure containment and ring buffer | In-memory buffer is not durable and can drop old events |
| Notification exposes reviewer payload | Host projection obligation | Flowsafe cannot know the trust level of a transport |
| Stream ticket replay | Short TTL, HMAC, address binding, Worker verification | Ticket in logs or browser history is usable until expiry |
| Agent event replay requested after restart | Return 409 and direct the client to durable status | Event history is available only for the configured cache window |

## Guarded agent boundary

`createGuardedAgent()` fixes RBAC, policy, execution limits, tool choice, and application processor ordering at construction. Its public handle exposes only unstructured `generate()` and `stream()` calls with a mandatory `RequestContext`.

The handle rejects unknown call options even when a key is present with `undefined`. It disables background continuations, forces policy hold-back, and keeps the raw Mastra agent package-private. Flowsafe validates an unforgeable package-local brand before accepting a catalog module.

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
accepted by `AuditLogger.record()` and includes tenant, workflow, run, approval,
or operation detail where applicable. It intentionally has no `timestamp`;
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
    "tenantId": "acme",
    "workflowId": "release",
    "runId": "acme_9d3f"
  }
}
```

Do not assume every event has the same action vocabulary or resource object. Use the string contract and documented detail for each producer.

## Secrets

Anchorage does not include a generic secret resolver.

Store deployment secrets in Cloudflare Secrets or an external manager and inject only what each component needs. Keep separate keys for:

- identity/session verification;
- stream tickets;
- OAuth client credentials;
- webhook providers;
- model providers;
- business connectors;
- SIEM authorization.

Do not persist raw keys in workflow input, suspend payload, approval context, schedule request context, notification body, audit detail, or artifact metadata.

An Agent CLI inherits its environment unless an injected executor changes it. Supply an allowlisted environment in the process/container boundary.

## Deployment obligations

Before a public endpoint:

1. Replace the static token example with a verified production identity seam.
2. Provision tenant ids and test foreign-resource 404 behavior.
3. Use D1-backed connector stores across per-run objects.
4. Register tenant isolation on every multi-tenant connector.
5. Route connector HTTP through `runtime.fetch` and add infrastructure egress policy.
6. Keep approval connector lists and durable-agent resume targets server-authored.
7. Mount only configured optional routers.
8. Configure all retention and offboarding duties for adopted domains.
9. Protect Durable Object namespaces behind the Worker topologies.
10. Project notifications and audit to the receiving channel's trust level.
11. Isolate Agent CLI workspaces and review their diffs.
12. Run the deterministic workerd restart, forgery, and cross-tenant proof.

Report vulnerabilities through [`SECURITY.md`](../SECURITY.md), not a public issue.
