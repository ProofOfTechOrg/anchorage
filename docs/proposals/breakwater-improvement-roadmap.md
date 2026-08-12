# Proposal: breakwater improvement roadmap

> This document records roadmap decisions and remaining uncommitted work. Phase A, Phase B steps 1 through 3, Phase C steps 1 through 3, Phase D step 1, and physical-deployment Slices A through E are shipped. The [single-tenant physical-isolation decision](single-tenant-physical-isolation.md) supersedes pooled-host assumptions in the historical analysis below. Supported behavior is documented in [Breakwater architecture](../breakwater-architecture.md), [Durable agents](../durable-agents.md), [Policy engine](../policy-engine-design.md), and [Connector interface](../connector-interface.md).

This document turns the limitations and ownership boundaries described in
[`breakwater-purpose-and-boundaries.md`](../breakwater-purpose-and-boundaries.md)
into an actionable improvement plan.

It distinguishes among:

- **Must do:** close an enforcement or wiring gap before exposing general
  production agent execution.
- **Should do:** materially improve security, operability, or the clarity of
  the public contract.
- **Could do:** useful expansion when a concrete product requirement appears.
- **Should not do:** preserve an intentional boundary instead of making
  breakwater into a second identity, tenancy, workflow, or infrastructure
  platform.

The goal is not to maximize the number of RBAC features. The goal is to make
the controls we claim unavoidable, correctly attributed, independently
testable, and appropriately placed.

## Executive Priorities

| Priority | Improvement | Why |
|---|---|---|
| **Shipped** | Authenticated agent catalog and host-level agent run router | Flowsafe resolves metadata before mutation authorization and exposes no public agent resume |
| **Shipped** | Mandatory agent processors that callers cannot omit or override | `createGuardedAgent()` exposes only the validated unstructured invocation surface |
| **Shipped** | Central reserved request-context stripping and server derivation | Actor, workflow, approval, permissions, and audit correlation remain runtime-owned; Flowsafe reserves and omits logical isolation scope |
| **Shipped** | Agent/run/deployment audit correlation | Guarded RBAC and policy events use `agent:<id>` plus the infrastructure-verified deployment tag |
| **Shipped** | Structured connector approval grants | Durable agents use exact tool-call scope, workflow gates use exact suspension scope, and standing grants use explicit run scope |
| **Shipped** | End-to-end invariant tests for supported invocation paths | The deterministic workerd proof covers restart, approval resume, forgery, deployment-sentinel mismatch, and exactly-once execution |
| **Shipped** | Public documentation aligned with guarded agent hosting | The architecture, durable-agent, deployment, threat-model, operations, starter, and API guides define the supported boundary |
| **Shipped** | Agent permission authorization without role inheritance | Flowsafe resolves trusted principals through a versioned host policy and enforces all required permissions |
| **Shipped** | Optional connector invocation permissions | `PermissionManifest.requiredPermissions` is enforced against the trusted principal-permissions projection before dry-run and approval — authorization and approval answer different questions |
| **Shipped** | Secure single-deployment policy preset | `singleTenantConnectorPolicies()` validates durable stores, audit, egress, permission wiring, background policy, and deployment-wide isolation posture |
| **P1** | Add manifest-conformance tooling and stronger egress posture | Runtime safety depends on honest manifests and use of `runtime.fetch` |
| **P1** | Close known agent-output enforcement gaps | Structured output and override seams must not silently weaken mandatory policy |
| **P1** | Introduce stable decision codes and richer audit correlation | Reason strings are not a durable API for alerts, metrics, or incident response |
| **P2** | Add policy bundles, versioning, and drift reporting | Useful once several agents/connectors share centrally managed policy |
| **P2** | Add stricter rate-limit algorithms and operational maintenance | Fixed windows and durable stores need production-scale alternatives and cleanup |
| **P2** | Rationalize overlap with new Mastra features on every upgrade | Mastra now supplies auth, processors, approval, hooks, and observability that should be reused where appropriate |

## Phase A decision record

The following sections preserve the investigation and acceptance criteria that produced the shipped guarded-agent boundary. Their “current state” subsections describe the pre-Phase A baseline, not current package behavior.

### 1. Add a Central Agent Catalog and Host-Level Authorization Boundary

#### Current state

Breakwater has a per-instance `RBACMiddleware({ allowedRoles })`, but there is
no shipped agent catalog or authenticated agent route equivalent to flowsafe's
workflow catalog and run router. The durable-agent wrapper accepts an existing
Mastra `Agent`; it does not decide who may invoke it.

#### Improvement

Add a flowsafe-owned agent module contract:

```ts
interface AgentMeta {
  id: string;
  title: string;
  description: string;
  allowedRoles?: readonly ApprovalRole[];
  requiredPermissions?: readonly Permission[];
}

interface AgentModule {
  meta: AgentMeta;
  agent: GuardedAgentHandle;
}
```

Add an authenticated agent router with the same gate-order discipline as the
workflow run router:

1. Verify deployment identity, then authenticate and validate the actor.
2. Apply the coarse agent-run role/permission gate.
3. Resolve the agent from the server catalog.
4. Check in-deployment access for any thread, resource, or run identifiers.
5. Apply `AgentMeta.allowedRoles` and any configured `requiredPermissions`.
6. Mint run and thread identifiers server-side and derive resource keys from trusted host data.
7. Derive trusted request context.
8. Invoke only the guarded agent handle.

Start, resume, signal/wake, and any future mutating agent route must share the
same authorization helper. A role allowed to start an agent must not be able to
bypass policy through a weaker resume or wake route.

#### Ownership

- **flowsafe:** catalog, authenticated route, deployment verification, IDs, and route-level authorization.
- **breakwater:** agent-turn defense-in-depth middleware.

#### Acceptance criteria

- Unknown agents return 404.
- Unauthenticated callers return 401.
- Inaccessible run/thread/resource identifiers return 404 before role errors.
- Disallowed roles cannot start, resume, signal, or wake the agent.
- Reads have an explicit documented policy instead of inheriting mutation
  behavior accidentally.
- The router never accepts a client-selected run or thread ID, or a full resource ID from an untrusted request.
- Every route invokes the same catalog entry and guarded handle; the raw
  Mastra agent is not exposed from the host registry.

### 2. Make Mandatory Agent Guards Non-Optional

#### Current state

An agent is protected only when its constructor includes the breakwater
processors. Mastra generation/stream options may supply per-call processor
overrides. [`model-gateway-policy.md`](model-gateway-policy.md) describes a
mandatory gateway that merges or strips those overrides, but no implementation
currently ships.

#### Improvement

Implement one supported guarded-agent construction path. Prefer a handle that
does not expose the raw `Agent`:

```ts
const guarded = createGuardedAgent({
  agent,
  agentId: meta.id,
  authorization,
  policies,
  audit,
});

await guarded.stream(messages, safeOptions);
```

The wrapper must:

- Install RBAC and mandatory input/output policies once.
- Reject or safely merge per-call processor overrides.
- Preserve mandatory processor ordering.
- Prevent duplicate installation of the same mandatory gate.
- Cover `generate`, `stream`, durable `prepare`, resume/re-hydration, and any
  background or signal-driven entry point.
- Expose optional application processors only through an explicitly safe
  composition seam.
- Keep the underlying raw agent private to the module.

Route-level authorization remains the primary access boundary. Agent
processors are defense in depth for internal call sites and future routing
mistakes.

#### Acceptance criteria

- A caller cannot remove RBAC or policy processors using per-call options.
- Direct use of the public guarded handle produces the same verdict as the
  authenticated route.
- All agent entry points emit an authorization decision carrying the agent ID.
- A CI test fails if a new Mastra agent entry point is added without a guarded
  wrapper method or an explicit non-applicability decision.

### 3. Create One Reserved-Context Security Boundary

#### Current state

Flowsafe already derives breakwater context inside the runner and strips the
entire `breakwater.*` namespace from scheduled stored context. That posture
must become a shared invariant for every external agent and workflow ingress,
not a route-by-route convention.

#### Improvement

Provide canonical host helpers such as:

```ts
sanitizeExternalRequestContext(input)
deriveTrustedExecutionContext({ actor, workflow, run, approvalLeg })
```

The sanitizer must reject or strip:

- Every `breakwater.*` key, including future keys.
- Mastra/internal execution-control keys owned by the runtime.
- Client-selected run/thread/resource IDs.
- Approval, idempotency, isolation, and workflow-scope values from stored or
  resumed client context.

The derivation helper must be the only normal producer of:

- `breakwater.actor`.
- `breakwater.connectorGrants`.
- `breakwater.connectorExecution`.
- `breakwater.workflowScope`.
- `breakwater.isolationScope` only in a generic host that owns a separate logical partition. Flowsafe reserves and omits it.
- Runtime-derived idempotency context.

Stored context must always merge first and runtime-derived context last.

#### Acceptance criteria

- Property tests prove that any `breakwater.`-prefixed input key is removed or
  rejected on every external create/update/start/resume path.
- Future breakwater keys are covered without editing a static list.
- Direct internal APIs that accept a `RequestContext` are explicitly marked
  trusted and are not reachable from request bodies.
- Start and resume derive context independently; a trusted grant from one leg
  cannot persist merely because it was present in a previous snapshot.

### 4. Make Authorization Audit Events Resource-Specific

#### Current state

`RBACMiddleware` records `resource: 'breakwater-rbac'`. That proves the gate ran but does not identify which agent was requested. Breakwater's `Actor` is organization-agnostic, so deployment/run/thread correlation must come from the host.

#### Improvement

Extend the middleware configuration or process context with safe attribution:

```ts
new RBACMiddleware({
  allowedRoles,
  resource: `agent:${agentId}`,
  audit,
});
```

Add stable audit correlation fields where known:

- Agent ID.
- Workflow ID.
- Run ID.
- Thread/resource ID.
- Deployment tag, added by the Flowsafe audit adapter from the verified infrastructure binding rather than Breakwater's actor.
- Policy bundle version/hash.
- Tool call ID and connector ID for connector decisions.
- Approval record and suspension fingerprint for grant-derived execution.

Do not put raw prompts, secrets, full connector URLs, or sensitive tool inputs
in authorization audit detail.

#### Acceptance criteria

- Every agent authorization event answers who, which agent, which deployment, which run/thread when available, what decision, and why.
- Denials and gate errors have the same correlation quality as allows.
- Audit-event schemas distinguish stable machine-readable fields from safe
  human-readable reasons.

### 5. Tighten approval-grant scope (shipped)

#### Shipped implementation

Flowsafe persists an explicit `grantScope` on every capability-bearing approval. Durable-agent approvals also persist Mastra `toolCallId`. Breakwater accepts only structured grants in `breakwater.connectorGrants` and compares them with the runtime-owned `breakwater.connectorExecution` identity.

The shipped scopes are:

- `tool-call`: connector, workflow, run, exact suspension, and `toolCallId`
- `suspension`: connector, workflow, run, and exact suspension
- `run`: connector, workflow, and run

Durable-agent suspensions use `tool-call` scope. Mastra persists `toolCallId` in the durable tool-call input and reproduces it in `context.agent.toolCallId` across resume, retry, background dispatch, and Durable Object reconstruction. Arbitrary workflow gates use `suspension` scope because Mastra exposes no corresponding tool-call identity there. Only an explicit trusted `runScoped: true` record produces `run` scope.

Legacy connector ID arrays and records without explicit scope fail closed. The public approval and resume bodies cannot set connector, scope, suspension, tool-call, or runtime-execution identity.

#### Retry and rejected alternatives

The same durable tool call can retry with the same `toolCallId`; a new model tool call requires approval. Connector idempotency remains a separate control and prevents repeated side effects when configured.

Input digests were rejected because canonical serialization and redaction behavior are not defined across Mastra and connector schemas. One-shot nonces were rejected because Mastra does not expose an atomic consume-and-execute transaction; consuming before execution breaks retry and eviction recovery, while consuming after execution permits duplicate races.

#### Residual

Workflow approvals remain suspension-scoped because no narrower reproducible identity exists. Code inside an approved connector remains trusted and can repeat its own connector operation under the same tool-call identity. Use connector idempotency when repeated execution must replay.

### 6. Define execution principals (shipped)

#### Shipped implementation

Flowsafe defines `ExecutionPrincipal` for human, service, agent, and system execution. Human principals carry a role. Every automated principal requires `purpose`, and agent principals may also carry `delegatedBy`.

Breakwater accepts the projected principal kind but does not resolve identity. Its `allowedPrincipalKinds` gate runs before role authorization and never consults human roles for an automated principal. Flowsafe's agent catalog uses `allowedAutomation` to constrain each automated kind to declared entry paths; human starts continue to use `allowedRoles`.

`trustAutomationPrincipal()` canonicalizes and freezes principals used by trusted platform entries. Audit events preserve the verified deployment tag, principal kind, principal ID, purpose, and delegation provenance. Approval decisions remain attributed to the human decider.

#### Shipped guarantees

- Scheduled or service execution never masquerades as an arbitrary human.
- Agent entry and approval-maintenance audit events implemented in this phase carry deployment tag, principal kind, principal ID, and delegation provenance when applicable.
- Automated principals cannot derive authority from administrative human roles.
- Human approval remains attributable to the human decider even when the requester is a service or agent.

### 7. Add an End-to-End Enforcement Matrix

Unit tests already cover individual breakwater gates well. Add system tests
that prove composition across every supported invocation path.

| Path | RBAC/route auth | Connector policy | Trusted grant | Deployment guard | Audit |
|---|---|---|---|---|---|
| Authenticated agent start | Required | Required when a connector runs | Per decision | Sentinel verified | Correlated |
| Agent resume after approval | Rechecked | Required | Exact current leg | Sentinel verified | Correlated |
| Workflow tool step | Workflow route/in-step policy | Required | Per decision | Sentinel verified | Correlated |
| Nested connector call | Inherited principal policy | Required | Must not broaden | Sentinel verified | Correlated |
| Direct trusted internal call | Explicit trusted API | Required for writes | Required for writes | Bound resource set | Correlated |
| Scheduled/service execution | Service principal policy | Required | Explicit standing or interactive policy | Sentinel verified | Correlated |
| Signal/background execution | Principal and resource policy | Required | No stale forwarded grant | Sentinel verified | Correlated |

Tests must include negative cases: forged reserved context (the
principal-permissions projection included), stripped processor overrides,
wrong deployment sentinel, inaccessible resource, stale approval, an approved-but-unauthorized principal at a
permission-declaring connector, a permission-declaring connector on a path
that mints no projection, same connector with changed arguments, missing
isolation scope, undeclared egress, duplicate idempotency key, and audit
sink failure.

### Cross-Cutting: Make Implementation Status Unambiguous

The documentation currently contains a mixture of shipped contracts, design
documents, sketches, and composed flowsafe behavior. Some broad descriptions
still say breakwater wraps workflow surfaces even though workflow route RBAC is
implemented by flowsafe. The model gateway is described as an enforcement
mechanism but is not yet a shipped breakwater API.

Apply a consistent status marker to every architecture claim:

- **Shipped in breakwater.** Name the exported API and its enforcement seam.
- **Shipped in flowsafe.** Name the host/runtime API breakwater depends on.
- **Composed control.** State both halves and which side mints trusted context.
- **Design only.** State that no runtime API currently enforces it.
- **Known residual.** State the exact bypass or weaker path.

Update the package README, architecture diagrams, examples, API docs, and
security claims together. Add a release checklist that rejects an unqualified
claim such as “workflow wrapper,” “full RBAC,” “network sandbox,” or “durable
audit” unless the referenced implementation and deployment dependency are
named.

Acceptance criteria:

- A reader can identify shipped versus planned behavior without reading source.
- Every security claim links to an API, test, or explicitly named host control.
- The comparison date and pinned Mastra version are visible.
- Design-only documents cannot be confused with package exports.
- The generated policy coverage report becomes the evidence behind global
  deployment claims.

## P1: Security and Authorization Improvements

### 8. Add permissions, not role inheritance (agent host shipped)

#### Shipped agent-host implementation

Flowsafe exports the canonical lowercase dotted `Permission` identifier type. `AgentMeta.requiredPermissions` declares a non-empty all-of list. Catalog construction rejects a non-array, an empty list, duplicate identifiers, and malformed identifiers.

`ThreadAgentHostOptions.resolvePrincipalPermissions` accepts a server-owned `PrincipalPermissionResolver`. The resolver receives only the trusted human, service, agent, or system `ExecutionPrincipal`. It returns a `PrincipalPermissionResolution` containing effective permissions and `policyVersion`.

The thread-host authorization gate calls the resolver after the existing human-role or automated-entry gate succeeds. Missing configuration, resolver failure, and malformed output fail closed for an agent that requires permissions. Agents that use only `allowedRoles` and `allowedAutomation` do not require the resolver; since the connector-authorization phase below, a configured resolver runs on their entries too so its resolution can be projected for connector-level checks.

Every permission-protected agent decision records `requiredPermissions` and `permissionPolicyVersion`. Audit events omit the effective permission set and identity-provider groups.

#### Preserved boundary

Flowsafe maps authenticated roles, groups, and service identities to permissions. Breakwater does not own user or group membership, and this change adds no role inheritance.

Connector invocation permissions shipped in the next section. Resource authorization evaluators remain planned.

### 9. Add Optional Connector Invocation Authorization (shipped)

Approval and authorization answer different questions:

- **Authorization:** may this principal ever invoke `payments.release`?
- **Approval:** may this particular proposed release proceed now?

A reviewer approving a payment should not automatically mean every requester
is authorized to propose or execute that connector.

#### Shipped implementation

Breakwater owns the permission vocabulary: `Permission`, `isPermissionIdentifier`, the `PrincipalPermissions` projection type with its `isPrincipalPermissions` guard, and `PRINCIPAL_PERMISSIONS_CONTEXT_KEY` (`breakwater.principalPermissions`) live in `@proofoftech/breakwater/rbac`, and flowsafe re-exports the identifier vocabulary.

`PermissionManifest.requiredPermissions` declares the connector's all-of list:

```ts
permissions: {
  sideEffect: 'write',
  requiredPermissions: ['payments.release'],
  requiresApproval: true,
}
```

Construction rejects a non-array, an empty list, duplicates, and malformed identifiers. The compiled execute path evaluates principal authorization before the dry-run branch and before approval-grant consumption, against the trusted `breakwater.principalPermissions` projection. A valid approval does not elevate an otherwise unauthorized principal; delegated execution is not modeled.

Flowsafe's thread agent host runs its configured `PrincipalPermissionResolver` on every authorized entry and projects the resolution — or an explicit `null` — into derived request context on every start and resume leg, so a resume retires a stale persisted projection. A workflow host may mint the key from its own trusted `RequestContextProvider`; a path that mints no projection denies every permission-declaring connector.

#### Shipped guarantees

- A missing, `null`, or malformed projection fails closed for a permission-declaring connector on every invocation path, dry-runs included.
- An approved-but-unauthorized principal is denied before the grant is consulted.
- Authorization audit records `requiredPermissions` and `permissionPolicyVersion` and never the effective permission set.
- Connectors without `requiredPermissions` ignore the projection entirely.

Avoid hard-coding human role names into connector manifests. Permissions make
the connector reusable across hosts and service principals.

### 10. Support Resource-Level Policy Through Explicit Evaluators

Do not build a generic record-level ACL database inside breakwater. Resource
ownership lives in domain stores and often requires a database lookup.

Instead, strengthen the existing evaluator seam so a connector can ask its
domain authorization provider:

```ts
authorizeResource({ principal, connectorId, input, requestContext })
```

Requirements:

- Fail closed on provider error or timeout.
- Make lookup caching explicit and deployment/resource-scoped.
- Do not pass secret-rich raw input into generic audit logs.
- Bind the authorization result to the input/resource identity actually used
  by `execute`.
- Re-check on retry when the underlying authorization may have changed, or
  document the snapshot semantics.

### 11. Publish Secure Policy Presets (shipped)

#### Shipped implementation

`singleTenantConnectorPolicies()` builds a frozen connector-policy set for one physically isolated deployment. `createConnector()` revalidates the preset against each manifest and refuses policy replacement or removal after construction.

The preset requires:

- D1-backed idempotency and rate-limit stores when a manifest declares the matching control.
- Audit sink configuration or an explicit development-only opt-out.
- Network-egress policy for connectors that declare hosts.
- Permission-resolution wiring for permission-declaring connectors.
- Background-execution policy.
- No `tenantIsolation()` evaluator or Flowsafe-provided isolation scope, because connector keys are deployment-wide.

Construction rejects contradictory or incomplete configurations before the first call. See [Connector interface](../connector-interface.md#apply-the-physical-deployment-preset) for the supported contract.

### 12. Improve Manifest Honesty and Egress Assurance

Runtime egress enforcement is strong only when connector network traffic uses
`ConnectorRuntime.fetch`.

Add:

- A connector conformance test kit that exercises declared hosts, undeclared
  hosts, redirect hops, dry-run, approval, idempotency, and rate limits.
- A lint rule or review check forbidding global `fetch` in connector modules.
- Documented adapters for common vendor SDK custom-fetch/transport options.
- A manifest inventory command that lists side-effect class, egress, approval,
  idempotency, dry-run, and rate-limit posture for every connector.
- CI failure when a write/destructive connector lacks its required policy
  stores or approval posture.
- Policy-drift reporting when a connector declaration no longer fits the
  organization allowlist.

Infrastructure must still supply process/container/network controls for raw
sockets, DNS/IP restrictions, and compromised dependencies. Do not market
`runtime.fetch` as a complete sandbox.

### 13. Close Agent Output Coverage Gaps

Known processor/API limitations must remain loud:

- Under the pinned Mastra surface, non-streaming structured output does not
  expose a parsed object to the output-result processor.
- Object-only policy therefore has weaker non-streaming coverage.
- The stream hold-back implementation depends on a Mastra reprocessing state
  key guarded by compatibility tests.

Improvements:

- Gate the final parsed object in the guarded-agent wrapper when Mastra returns
  it outside the processor result.
- Refuse configurations whose chosen invocation mode cannot expose a required
  channel.
- Keep an upgrade tripwire around Mastra's processor and stream semantics.
- Add leak tests proving no forbidden prefix is emitted before a late detector
  fires.
- Measure hold-back memory and latency under large streams.

### 14. Add Stable Decision Codes and Safe Error Surfaces

Human-readable reason strings should not be used as programmatic identifiers.
Add stable codes such as:

```text
RBAC_ACTOR_MISSING
RBAC_ROLE_DENIED
AUTHZ_PERMISSION_DENIED
CONNECTOR_APPROVAL_MISSING
CONNECTOR_EGRESS_DENIED
CONNECTOR_ISOLATION_SCOPE_MISSING
CONNECTOR_RATE_LIMITED
POLICY_CLASSIFIER_FAILED_CLOSED
```

Separate:

- A stable internal code.
- A safe public message.
- Restricted audit detail.
- The original contained error, when appropriate for server diagnostics.

Do not echo detected secrets, full URLs with query strings, private policy
configuration, or unnecessary allowed-role lists to untrusted clients.

### 15. Strengthen RBAC Configuration Validation

Small improvements to the current middleware:

- Require a non-empty, normalized actor ID at the default context seam.
- Runtime-validate `allowedRoles`, not only through TypeScript types.
- Reject duplicate or invalid configured roles at construction.
- Accept a resource/agent identifier for audit attribution.
- Make role-denial reasons safe for public propagation.
- Define behavior when `getActor` returns a mutable object; copy only validated
  fields.
- Add an optional permission/authorization evaluator without changing the organization-agnostic `Actor` contract.

Treat validation tightening as a versioned behavior change because JavaScript
callers may currently pass values TypeScript would reject.

## P2: Operability, Scale, and Product Ergonomics

### 16. Add Versioned Policy Bundles

Today policies are bound mostly in code at agent/connector construction.
Introduce a compile step only when centralized management is needed:

```ts
interface PolicyBundle {
  id: string;
  version: string;
  issuedAt: string;
  agentAuthorization: ...;
  contentPolicies: ...;
  connectorPolicies: ...;
}
```

The compiler should:

- Validate all connector patterns and egress hosts eagerly.
- Resolve precedence deterministically.
- Produce an immutable runtime policy object.
- Emit a hash/version into every audit decision.
- Support dry validation before deployment.
- Report which agents/connectors are uncovered.
- Retain the previous bundle for rollback.

Do not make runtime authorization depend synchronously on an external policy
service unless its failure, caching, revocation, and consistency semantics are
explicitly designed.

### 17. Add Policy Coverage and Drift Reports

Generate a build artifact answering:

- Which agents have route authorization?
- Which agents have mandatory RBAC and content processors?
- Which connectors are read/write/destructive/idempotent?
- Which connectors require approval?
- Which connectors have unguarded or empty egress declarations?
- Which write connectors lack durable idempotency?
- Which connectors require an opaque logical scope, and which trusted host mints it?
- Which decisions lack a durable audit sink?

Fail CI only for declared security invariants; warn for advisory improvements.
This prevents a reporting tool from claiming controls are globally installed
when only the library primitive exists.

### 18. Improve Rate-Limit and Store Operations

Keep the existing fixed-window contract for compatibility, but allow pluggable
algorithms for stricter workloads:

- Token bucket or GCRA for smoother hard limits.
- Separate principal, workflow, connector, and deployment budgets.
- Explicit retry-after metadata.
- Store cleanup/compaction for old rate buckets, completed idempotency records,
  and abandoned pending reservations.
- Metrics for replay, join, reserve contention, stale takeover, denial, and
  store latency.
- Capacity and failure-mode documentation for D1.

Rate-limit failure posture should be configurable only at policy definition:
security-sensitive connectors normally fail closed; low-risk reads may choose
a documented availability-first mode.

### 19. Make Audit Durability Observable

Breakwater correctly contains sink failures so an exporter outage does not
stop agent execution, but operators need to know when durable evidence is at
risk.

Add:

- Queue depth and oldest-event-age metrics.
- Sink failure counters and alerts.
- Dropped/ring-buffer-eviction counts.
- A deployment health check for required durable audit configuration.
- Backpressure and retry guidance for the flowsafe queue/SIEM path.
- Redaction tests over every audit event type.
- A documented choice between availability-first and compliance-stop behavior
  for deployments whose regulation requires durable audit before execution.

The last option belongs at the host/policy level; changing `AuditLogger` to
always block execution on sink availability would be a breaking availability
decision.

### 20. Review Mastra Overlap on Every Upgrade

Mastra now supplies more of the surrounding surface than when breakwater was
first designed: core auth, Enterprise RBAC/ACL, native approval, built-in
processors, observability, and tool hooks.

For each Mastra upgrade:

1. Inventory new native capabilities.
2. Prefer native lifecycle mechanics when they satisfy the full invariant.
3. Retain breakwater's execute wrapper where caller-independent enforcement is
   still required.
4. Remove duplicate code only after direct/workflow/nested/resume tests prove
   equivalent coverage.
5. Update the comparison and compatibility date in
   `breakwater-purpose-and-boundaries.md`.
6. Run processor, output-channel, approval-shape, background-task, and durable
   resume tripwires against the new version.

Do not replace an enforced connector boundary with an agent-only hook merely
because both can block a normal agent tool call.

### 21. Harden Agent CLI Execution at the Infrastructure Boundary

The CLI connectors already avoid a shell, gate writes, support dry-run, and
bound time/output. Higher-assurance deployments should additionally provide:

- A dedicated container/VM or operating-system sandbox.
- Workspace path allowlists and canonicalization.
- Read-only mounts by default with explicit writable roots.
- Network policy outside the process.
- CPU, memory, process-count, and filesystem quotas.
- A minimal environment-variable allowlist.
- Secret-brokered credentials rather than inherited ambient credentials.
- Artifact/change capture tied to the approval and audit record.

These are host/infrastructure improvements. They should not be simulated with
claims that a Node child-process wrapper is itself a sandbox.

## Improvements We Should Deliberately Avoid

### Do not move authentication into Breakwater

Keep JWT/session/SSO/provider verification in the host. Breakwater should
consume a validated actor/principal through a narrow interface.

### Do not add `tenantId` to Breakwater's base `Actor`

Organization identity and lifecycle belong to provisioning and Flowsafe's physical deployment guard. Breakwater should continue to consume an opaque isolation scope when a generic host needs another logical partition. Flowsafe does not mint that scope. Host audit adapters can add verified deployment correlation.

### Do not build a generic resource ACL database in Breakwater

Resource ownership belongs to domain services. Breakwater should provide the
fail-closed evaluator seam and common decision shape.

### Do not make role inheritance the default authorization model

Explicit permissions are easier to review and safer for service/agent
principals. A host may implement role inheritance when required, but
breakwater should receive the resulting permissions rather than reproduce the
directory model.

### Do not move workflow RBAC or retention into the processor package

Workflow start/resume authorization belongs at Flowsafe's authenticated HTTP boundary. TTL deletion belongs at the storage layer; organization decommissioning deletes the physical deployment.

### Do not claim complete egress isolation

Keep the guarded fetch and manifest enforcement, but rely on infrastructure
for raw-socket, DNS/IP, dependency, and process-level containment.

### Do not require an online central policy call for every tool execution

Breakwater's in-process, immutable enforcement is a strength. If centralized
policy management is added, distribute validated versioned bundles and design
revocation explicitly rather than introducing an accidental availability
dependency.

## Recommended Delivery Sequence

### Phase A: Agent access foundation (shipped)

1. Define `AgentMeta`/`AgentModule` and a server-owned catalog.
2. Implement authenticated start/status/stream routes with in-deployment resource checks and route-level role enforcement.
3. Keep agent resume approval-only and restore the original authorized principal.
4. Implement `createGuardedAgent` with a narrow, non-overridable handle.
5. Centralize reserved-context sanitization and trusted derivation.
6. Add resource-specific audit attribution.
7. Ship the end-to-end enforcement matrix.

The shipped host deliberately omits a public raw-resume route. It accepts structured grants derived from approved records and authoritative runtime identity.

### Phase B: Approval capability and principal hardening

1. ~~Define human, service, agent, and system principals beyond the Phase A human-role snapshot.~~ Shipped. `ExecutionPrincipal` carries a kind, a required `purpose` on every automated kind, and optional delegation. Breakwater's `Actor` gained `kind`, and `RBACMiddleware`/`createGuardedAgent` gate on `allowedPrincipalKinds` before roles. The agent host routes automated entry through each agent's `allowedAutomation` declaration.
2. ~~Choose connector/leg/tool-call/input/nonce grant scope for structured grants.~~ Shipped. Durable agents use `tool-call`; workflow gates use `suspension`; trusted standing grants use explicit `run`.
3. ~~Prove scheduled, signal, background, and nested execution cannot inherit a stale or broader grant.~~ Shipped. Reserved-context boundaries reject client and stored capabilities, every leg overwrites grants, and Breakwater compares exact connector and execution identity.
4. Add dynamic principal re-resolution only when a concrete identity-provider contract exists.

### Phase C: Fine-grained authorization

1. ~~Introduce explicit permissions and a host role-to-permission mapper.~~ Shipped for the agent host. `PrincipalPermissionResolver` maps a trusted `ExecutionPrincipal`, including a human role, to effective permissions and `policyVersion`.
2. ~~Add `AgentMeta.requiredPermissions`.~~ Shipped with all-of semantics, catalog validation, fail-closed resolver enforcement, versioned audit attribution, and unchanged role-only behavior.
3. ~~Add optional connector `requiredPermissions`.~~ Shipped. Breakwater enforces the manifest list against the trusted `breakwater.principalPermissions` projection before dry-run and approval; flowsafe's thread host projects its resolver output on every leg.
4. Add resource authorization evaluators for concrete use cases.
5. Keep role-only configuration as the compatibility path.

### Phase D: Connector assurance

1. ~~Publish secure policy presets.~~ Shipped as `singleTenantConnectorPolicies()`.
2. Add connector conformance tests and global-fetch linting.
3. Generate the manifest/policy coverage report.
4. Close structured-output policy coverage.
5. Add stable decision codes.

### Phase E: Operations and scale

1. Version policy bundles and report drift.
2. Add stricter rate algorithms where required.
3. Add store maintenance and contention metrics.
4. Make audit-export health and loss visible.
5. Add infrastructure sandbox guidance and reference deployment for agent
   CLIs.

## Definition of Done for “Breakwater-Protected Agent”

An agent should not be described as breakwater-protected until all applicable
statements are true:

- It is registered in a server-owned catalog.
- Every external mutation authenticates the principal and validates in-deployment resource access.
- Every public entry verifies the physical deployment sentinel; every Worker-to-Durable-Object fetch also authenticates the calling deployment.
- Start, resume, signal, wake, and background paths share the same authorization
  policy.
- Mandatory processors cannot be removed by per-call options.
- The actor/principal and all breakwater context keys are server-derived.
- Its audit events name the agent, verified deployment, run/thread, principal, and policy version where available.
- Every connector it can call has a reviewed permission manifest.
- Write/destructive connectors require authorization and, where configured,
  a correctly scoped approval grant.
- Connector network traffic uses `runtime.fetch` or has an explicitly accepted
  degraded posture.
- Flowsafe connectors use deployment-wide keys; a separate generic host requires an opaque isolation scope only when it explicitly defines another logical partition.
- Idempotency and rate limits use stores appropriate to the deployment.
- Negative end-to-end tests cover agent, workflow, direct, nested, retry, and
  resume paths.
- Durable audit export and its failure posture are configured and monitored.
- Known processor/output coverage limitations are either closed or explicitly
  rejected by configuration.

## Success Measures

Track outcomes rather than feature count:

- Percentage of registered agents reachable only through guarded handles.
- Percentage of mutating routes using the canonical reserved-context boundary.
- Percentage of connector calls carrying agent/run/deployment/principal audit correlation.
- Percentage of write connectors with required authorization, approval policy,
  durable idempotency, and reviewed egress.
- Number of policy-bypass paths found by the end-to-end matrix.
- Audit export lag, sink failures, and dropped-event count.
- Idempotency replay/duplicate-prevention rate and stale-reservation takeover.
- Rate-limit denials and budget contention by deployment/connector.
- Mastra upgrade tripwire pass rate and time to certify a new core version.

The desired end state is not “Breakwater has enterprise RBAC.” It is:

> Every agent and connector enters through a documented, host-authenticated,
> non-bypassable boundary; every side effect is authorized under least
> authority; every denial and grant is attributable; and each intentional
> residual is explicit rather than accidental.
