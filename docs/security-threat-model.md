# Security Threat Model

Agentic workflows can read sensitive data, produce persuasive content, and write to business systems. Security is designed in. This threat model covers Anchorage's components layered on Mastra; Mastra's own security is assumed as the foundation.

## Assets

| Asset | Sensitivity | Location |
|---|---|---|
| Workflow definitions | Medium | D1 |
| Workflow state | Medium | D1 |
| Approval decisions | High | D1 (`flowsafe_approvals`) |
| Model API keys | Critical | Cloudflare Secrets |
| Connector credentials | Critical | Cloudflare Secrets |
| Workflow outputs | Variable | D1, R2 |
| Approval grants (`breakwater.approvedConnectors`) | High | Mastra RequestContext (in-process, per run) |
| Actor identity (`breakwater.actor`) | High | Mastra RequestContext (in-process, per run) |
| Tenant identity (`ApprovalActor.tenantId`, the `runId` prefix) | Critical | Bearer map / JWT claim, then every store predicate |
| Token-signing secret (`DEMO_JWT_SECRET`, or your IdP's key) | Critical | Cloudflare Secrets |
| Resume ordinals (the grant-binding tie-breaker) | Medium | Durable Object `ctx.storage` |

## Trust Boundaries

1. **End user <-> Approval dashboard** -- authenticated via OIDC/JWT
2. **Approval dashboard <-> flowsafe API** -- requires API key or session
3. **flowsafe API <-> Durable Object** -- Cloudflare internal network
4. **breakwater middleware <-> Mastra runtime** -- in-process
5. **Mastra runtime <-> External APIs** -- model providers, business connectors
6. **Untrusted input <-> Mastra RequestContext** -- the requestContext is a
   server-side trusted channel. breakwater reads capability tokens from it:
   `breakwater.actor` (RBAC identity) and `breakwater.approvedConnectors`
   (write-approval grants -- whoever can write this key can authorize any
   write-class connector). Any code that constructs or mutates a run's
   requestContext is part of the trusted computing base. No client input,
   model output, or tool result may ever be written into these keys. The
   flowsafe DO runner forwards no requestContext values from request bodies;
   the only requestContext source is the `requestContextForRun` provider
   (`RunnerRuntime`), and the approval-api implements it by
   DERIVATION: `approvalGrantProvider(store)` recomputes grants from
   APPROVED approval records on every start/resume, so grants never cross an
   HTTP boundary. Grants are SUSPENSION-SCOPED: a step-keyed approval
   unlocks its connectors only for the leg that resumes that step, and only
   when the decision binds to the step's CURRENT suspension. The primary
   binding is EXACT and clock-free on the `(suspendedAt, resumeCount)`
   fingerprint: the creating bridge captures both into the record
   (`CreateApprovalInput.{suspendedAt,resumeCount}`, observed from
   `RunSummary.{suspendedAt,resumeCount}`), and minting requires both to equal
   the values the runner passes to the provider. `suspendedAt` comes from the
   core clock; `resumeCount` is the runtime-owned monotonic per-(run,step)
   resume ordinal, so no cross-clock comparison exists and clock skew between a
   split approval service and runner cannot mis-mint. `resumeCount` is the
   categorical tie-breaker -- undefined on a step's first suspension, `1,2,...`
   on re-suspensions, and incremented by the runtime on EVERY resume regardless
   of payload -- so a spent first-suspension approval never mints into a
   re-suspension even if the two `suspendedAt` stamps collide within a
   millisecond (possible only on the synchronous in-process path; production's
   HTTP+D1 round-trips keep them seconds apart), and a no-payload re-suspension
   (which Mastra leaves without a `resumedAt`, the reason the earlier
   `resumedAt`-based binding leaked) stays distinguishable. So approving a
   connector at one approval point never unlocks it at another point of the run,
   and when the same step suspends again the earlier approval is spent -- the
   new suspension needs its own decision, and a rejected re-request never falls
   back to an old approval. Because the ordinal strictly increments it never
   collides, so this binding also closes the deep-chain (three-plus
   re-suspension) residual the prior `(suspendedAt, resumedAt)` binding left
   open; `resumedAt` is retained as informational audit metadata only. The
   ordinal lives in a `ResumeLedger`; the Durable Object shell backs it with
   `ctx.storage`, which survives eviction, hibernation, and code deploys. A
   LOST ledger was never a leak -- the leg would read `resumeCount: undefined`
   against a re-suspension record's defined count, mismatch, and deny -- but it
   was an AVAILABILITY defect: an already-approved action would silently
   no-op after a ~70-second idle eviction. Non-DO hosts keep the in-memory
   default, where the mutual-exclusion argument still holds (a same-ms
   `suspendedAt` collision requires a synchronous store with no I/O between the
   two suspends, so same-ms-collision and surviving-a-restart cannot co-occur).
   Records created WITHOUT
   capturing the suspension (legacy/pre-capture bridges) fall back to
   chronology (`decidedAt` strictly after `suspendedAt` -- deny-deterministic
   under a shared clock); only that fallback carries the same-clock
   constraint, so deployments splitting the approval service and the runner
   across machines must either capture `suspendedAt` at create time (the
   demo bridge does) or keep the clocks in sync. Run-scope is EXPLICIT: a
   step-less approval is a standing grant on every leg only when it carries
   `runScoped: true`, and mints nothing otherwise -- bridges always set
   `stepPath` and never `runScoped`. (Treating an absent `stepPath` as
   run-wide privilege was an inverted default: any record whose step was
   merely omitted became a standing capability.) Mastra merges resume-provided
   context OVER
   the persisted snapshot (pinned by test), so the provider returns the
   grant key on EVERY leg -- an empty list when nothing applies -- and the
   overwrite retires the previous leg's grants. Net invariant (proven in
   `approval-api/end-to-end.test.ts`, including the two-gate and
   re-suspension cases, and the workerd spike): a resume that bypasses
   `decide()` for the targeted suspension finds no grant and fails closed
   at the connector gate. The `connectors`
   list on an approval request is asserted by the CAN_CREATE caller
   (operator/builder/admin) -- creation is part of the trusted computing
   base; requests must be created from a trusted suspend-observation bridge
   (as the demo Worker does) rather than hand-built payloads.

   **The create route can never author capability.** An approval record's
   `connectors` list IS the grant a decision mints, and its `requestedBy` is
   the field the separation-of-duties check compares against -- so both are
   TCB-only. `createApprovalRouter`'s `POST <basePath>` is therefore
   **off by default** (`allowCreate`), and when a host deliberately mounts it as
   a "file a request" affordance it 400s on any body naming a
   `TCB_ONLY_CREATE_FIELDS` member -- `connectors`, `runScoped`, `stepPath`,
   `suspendedAt`, `resumedAt`, `resumeCount`, `requestedBy` -- and forces
   `requestedBy` to the authenticated actor. `stepPath` and the binding pair are
   on that list because they select WHICH leg a grant mints on: a step-keyed
   body with no `suspendedAt` would otherwise ride the legacy
   `decidedAt`-after fallback and mint. Without these controls one principal
   holding both `CAN_CREATE` and `CAN_REVIEW` (i.e. `admin`) could file a
   request naming an arbitrary connector, spoof `requestedBy` so the
   self-decision check compared the wrong identity, approve it alone, and --
   because the step-less record was implicitly run-scoped -- mint that
   capability on every leg of an arbitrary run. Regression-pinned in
   `approval-api/end-to-end.test.ts`.

7. **Tenant <-> tenant** -- one deployment serves many tenants on one Worker,
   one D1, and one Durable Object namespace. There is no per-tenant database.
   Isolation rests on three invariants, each with exactly one enforcement
   chokepoint, and each fails closed:

   **INV-1 -- the run id is the tenant carrier.** Every `runId` is minted
   server-side as `` `${tenantId}_${uuid}` `` from the AUTHENTICATED tenant.
   Three enforcement points, all required: `createRunRouter` rejects a
   client-supplied `body.runId` (400) and mints from `actor.tenantId`;
   `RunnerRuntime.start` REQUIRES a `runId` and has no generation fallback (a
   fallback would mint a bare, tenant-less uuid whose snapshot row is
   unreachable by tenant purge and un-ownable by everyone); and the Durable
   Object asserts the request's `(workflowId, runId)` equals the identity it
   was addressed with (`ctx.id.name`, set by the trusted Worker via
   `idFromName` and unforgeable at that boundary). Because `runId` is the key
   everything derives from, the Mastra snapshot row (`UNIQUE (workflow_name,
   run_id)`), the DO instance, the per-run lock, the grant-derivation
   predicate, and the R2 key are tenant-disjoint with no schema change. Run
   status and resume additionally verify ownership and answer **404** rather
   than 403, so the route is not an existence oracle for another tenant's run
   ids. Attribution is not authorization.

   **INV-2 -- the store is bound to one tenant at construction, and the tenant
   comes from the authenticated actor.** INV-1 does not cover
   `flowsafe_approvals`, whose rows are addressed by opaque `id` — that is
   where an IDOR would live. Approval stores are therefore obtainable only
   from a factory's `forTenant(tenantId)`, and every `SELECT`/`UPDATE`/`DELETE`
   carries `tenant_id = ?` sourced from that constructor field, never from a
   request parameter. `tenantId` is deliberately NOT a member of
   `ApprovalListFilter`: an omissible tenant filter is the canonical fail-open,
   because an empty filter would scan every tenant. The bound type carries a
   module-private `unique symbol` brand, so an unbranded store — or the
   cross-tenant `SystemApprovalStore` — is a COMPILE ERROR wherever request
   scope expects a bound one. Requests reach the store only through a
   `TenantResolver` (authenticate -> validate -> bind), which is what makes the
   binding constructible: the store cannot exist before the actor does.
   Cross-tenant reads are legitimate inside the TCB, so that distinction is a
   TYPE (`SystemApprovalStore`), not a comment: the SLA sweep left
   `ApprovalService` for a standalone cron-only function and its HTTP route was
   DELETED. It had been an unfiltered cross-tenant read *and write* reachable
   by any actor holding the sweep role.

   **INV-3 -- the charset makes the prefix exact.** `tenantId` matches
   `^[a-z0-9]{3,32}$`. That set is `0x30-0x39` union `0x61-0x7A` and contains
   no character in `[0x5F, 0x60]` -- neither `_` (0x5F) nor `` ` `` (0x60).
   So the ownership check `runId.startsWith(tenantId + '_')` cannot match
   another tenant (`acme` vs `acmecorp`: at index 4 one has `_`, the other
   `c`), and the offboarding range delete
   `` run_id >= '<tid>_' AND run_id < '<tid>`' `` selects exactly one tenant's
   rows under BINARY collation. Any other tenant's character at the delimiter
   position sorts below 0x5F or above 0x60. A later loosening of this charset
   silently breaks BOTH properties, so a character-exhaustive test pins it.
   The `runId` -> tenant decode has exactly one implementation
   (`tenantOfRunId`), which validates the prefix; four hand-rolled copies had
   already drifted apart.

   **What the invariants do not cover** (and how each residual is closed):
   connector idempotency keys are caller-supplied and legitimately cross-run
   (`never email this lead twice`), so two tenants can collide on one key --
   the runtime mints an opaque `breakwater.isolationScope` and breakwater
   prefixes both the idempotency and the rate-limit key with it, so one
   tenant can neither read another's cached result nor exhaust its budget.
   Five of the six `mastra_*` tables (`threads`, `messages`, `resources`,
   `scorers`, `background_tasks`) are keyed by ids INV-1 does not salt; they
   are empty today because flowsafe persists only workflow snapshots, and a
   table-inventory test fails CI if that changes, forcing a tenancy decision
   rather than a silent leak. R2 keys put `workflowId` -- a tenant-SHARED
   literal -- outermost, so isolation holds only because every artifact
   operation demands the full `(workflowId, runId)` pair; a
   "list artifacts for workflow W" API would enumerate every tenant, and its
   absence is pinned by test. Tenant-id uniqueness is enforced by the
   `tenants` registry (insert-or-fail); nothing else does, and two clients
   slugged `acme` would merge entirely. breakwater's single audit ring buffer
   is shared across tenants, but no host exposes `events()` over HTTP, so it
   is not an exfiltration path -- a known limitation, recorded here rather
   than assumed away.

## Threats and Controls

| Threat | Severity | Control |
|---|---|---|
| Unauthorized approval/rejection | High | RBAC + audit log |
| Self-approval (requester decides own request) | High | Separation-of-duties default: `decide()` denies when decider == `requestedBy` (server-attributed); opt-out only via explicit `allowSelfDecision` |
| Approval decision tampering | High | Durable Object strongly consistent state |
| Approval-grant forgery via requestContext injection | High | Trust boundary 6: grants minted only by trusted server-side code; requestContext never populated from client/model/tool input; `context.agent` never bypasses the grant check (forwardable) |
| Secret leakage in logs | High | Secret resolver strips from event payloads |
| API key theft | Critical | Short-lived tokens, OIDC |
| Workflow data cross-contamination | Medium | `crossWorkflowIsolation` evaluator over the runtime-minted `breakwater.workflowScope` |
| Cross-tenant read of approvals (IDOR on an opaque record id) | Critical | INV-2: `tenant_id = ?` on every store predicate, sourced from the constructor; a wrong tenant matches zero rows and reuses the 404 path |
| Cross-tenant read/resume of a run | Critical | INV-1: ownership check on the salted `runId`, answering 404 (not 403) so the route is no existence oracle |
| Cross-tenant grant mint | Critical | INV-1: the grant query filters on `runId`, which belongs to one tenant by construction; INV-2 binds the store as defense in depth |
| Cross-tenant enumeration via an empty list filter | High | `tenantId` is not an `ApprovalListFilter` member; the bound store seeds `tenant_id = ?` before every optional clause |
| Cross-tenant escalation via the SLA sweep | High | The HTTP sweep route is removed; the sweep takes a `SystemApprovalStore`, unobtainable in request scope |
| Cross-tenant connector replay or budget exhaustion | High | `breakwater.isolationScope` segments the idempotency and rate-limit keys; `tenantIsolation` denies a scope-less call, including on dry-run |
| Tenant-id collision merging two clients | Critical | `provisionTenant` insert-or-fail against the `tenants` registry, before any token naming that tenant is issued |
| Confused deputy: tenant A's token used on tenant B's subdomain | Medium | Optional `withSubdomainCrossCheck` (routing-level; INV-2 remains the enforcing layer) |
| Data surviving offboarding | High | `purgeTenant` reaps snapshots of any status, approvals, and R2 artifacts; run only after the tenant's tokens expire |

## RBAC Model

Five roles in `@proofoftech/breakwater`. flowsafe's `ApprovalActor` is
`{ id, role, tenantId }` — breakwater's own `Actor` stays tenant-agnostic (it
is a standalone library with no notion of tenancy). A bearer-map entry or JWT
claim carrying no INV-3-valid `tenantId` is dropped, and its token 401s.
`ApprovalService` additionally asserts the acting principal's tenant equals
its store's binding — a belt that can only fire on a wiring bug, which is
exactly when it must fail closed.


| Role | Scope | Actions |
|---|---|---|
| `admin` | Global | Manage users, roles, policies |
| `builder` | Per-workflow | Create, update, deploy |
| `operator` | Global | Start, stop, retry, cancel runs |
| `reviewer` | Per-workflow | View and approve/reject |
| `viewer` | Global | Read-only access |

## Audit Log Schema

Every action records: actor, action, resource, result, timestamp, reason.

```
{
  actor: string,
  action: 'approve' | 'reject' | 'start' | 'cancel' | 'retry' | 'configure',
  resource: { type: 'workflow' | 'run' | 'approval', id: string },
  result: 'allowed' | 'denied' | 'error',
  reason?: string,
  timestamp: ISO8601
}
```

Approval-service events carry `tenantId` in `detail` alongside
`workflowId`/`runId`. This matters most for the SLA sweep: it is the one
cross-tenant writer, so an escalation without a tenant in its detail would be
unattributable.

## Secret Lifecycle

Secrets are stored in Cloudflare Secrets, never in D1. Workflow definitions reference secrets by name; the runtime resolves them at execution time. Raw secrets never appear in event payloads, logs, or workflow state.
