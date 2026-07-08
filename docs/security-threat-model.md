# Security Threat Model

Agentic workflows can read sensitive data, produce persuasive content, and write to business systems. Security is designed in. This threat model covers Anchorage's components layered on Mastra; Mastra's own security is assumed as the foundation.

## Assets

| Asset | Sensitivity | Location |
|---|---|---|
| Workflow definitions | Medium | D1 |
| Workflow state | Medium | D1 |
| Approval decisions | High | Durable Object storage |
| Model API keys | Critical | Cloudflare Secrets |
| Connector credentials | Critical | Cloudflare Secrets |
| Workflow outputs | Variable | D1, R2 |
| Approval grants (`breakwater.approvedConnectors`) | High | Mastra RequestContext (in-process, per run) |
| Actor identity (`breakwater.actor`) | High | Mastra RequestContext (in-process, per run) |

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
   ledger is in-memory, so across a DO restart it resets -- but this can never
   leak: a same-ms `suspendedAt` collision (the only case the ordinal guards)
   requires a synchronous in-memory store with no I/O between the two suspends,
   so same-ms-collision and surviving-a-restart are mutually exclusive. A
   durable (D1) deployment gives the two suspensions distinct `suspendedAt`, so
   the exact-match still denies fail-closed (a re-deny, never a leak). Records
   created WITHOUT
   capturing the suspension (legacy/pre-capture bridges) fall back to
   chronology (`decidedAt` strictly after `suspendedAt` -- deny-deterministic
   under a shared clock); only that fallback carries the same-clock
   constraint, so deployments splitting the approval service and the runner
   across machines must either capture `suspendedAt` at create time (the
   demo bridge does) or keep the clocks in sync. Step-less approvals are
   explicitly run-scoped standing grants -- the deliberate opt-out; bridges
   should always set `stepPath`. Mastra merges resume-provided context OVER
   the persisted snapshot (pinned by test), so the provider returns the
   grant key on EVERY leg -- an empty list when nothing applies -- and the
   overwrite retires the previous leg's grants. Net invariant (proven in
   `approval-api/end-to-end.test.ts`, including the two-gate and
   re-suspension cases, and the workerd spike): a resume that bypasses
   `decide()` for the targeted suspension finds no grant and fails closed
   at the connector gate. The `connectors`
   list on an approval request is asserted by the CAN_CREATE caller
   (operator/builder/admin) -- creation is part of the trusted computing
   base; prefer creating requests from a trusted suspend-observation bridge
   (as the demo Worker does) rather than hand-built payloads.

## Threats and Controls

| Threat | Severity | Control |
|---|---|---|
| Unauthorized approval/rejection | High | RBAC + audit log |
| Self-approval (requester decides own request) | High | Separation-of-duties default: `decide()` denies when decider == `requestedBy` (server-attributed); opt-out only via explicit `allowSelfDecision` |
| Approval decision tampering | High | Durable Object strongly consistent state |
| Approval-grant forgery via requestContext injection | High | Trust boundary 6: grants minted only by trusted server-side code; requestContext never populated from client/model/tool input; `context.agent` never bypasses the grant check (forwardable) |
| Secret leakage in logs | High | Secret resolver strips from event payloads |
| API key theft | Critical | Short-lived tokens, OIDC |
| Workflow data cross-contamination | Medium | Per-workflow D1 namespace or key prefix |

## RBAC Model

Five roles in `@proofoftech/breakwater`:

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

## Secret Lifecycle

Secrets are stored in Cloudflare Secrets, never in D1. Workflow definitions reference secrets by name; the runtime resolves them at execution time. Raw secrets never appear in event payloads, logs, or workflow state.
