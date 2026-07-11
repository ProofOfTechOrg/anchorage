# Operations Runbook

Procedures for operating an Anchorage deployment on Mastra plus Cloudflare Workers.

## Operating Roles

- Workflow owner: accountable for business outcome and acceptance criteria
- Builder: designs workflow, prompts, policies, and connectors using Mastra SDK
- Operator: manages deployments, monitors runs, handles incidents
- Reviewer: approves or rejects workflow tasks via flowsafe dashboard

## Build

```
pnpm install
pnpm build
pnpm test
```

## Deploy

### Mastra on Cloudflare Workers

Configure `CloudflareDeployer` (`@mastra/deployer-cloudflare`) on the Mastra instance, build, then deploy the generated output with wrangler:

```
npx mastra build
npx wrangler deploy
```

`mastra deploy` is a real CLI command but targets Mastra's hosted platform, not Cloudflare.

### flowsafe (DO runner + approval queue)

Spike-verified on workerd. A copy-ready reference
deployment lives in `packages/flowsafe/deploy/` — a production-shaped Worker
wiring one Durable Object per run, the D1-backed approval queue, the tenant
resolver over a bearer-token verifier, and a cron trigger owning the SLA sweep
and snapshot retention purge. Deploy checklist and configuration table:
`packages/flowsafe/deploy/README.md`.

```
# from packages/flowsafe/
pnpm deploy:cf     # wrangler deploy --config deploy/wrangler.jsonc
pnpm deploy:dev    # local workerd, no Cloudflare account
```

Audit export to a SIEM is optional (`@proofoftech/flowsafe/audit-export` via a
Cloudflare Queue binding) and workflow artifacts persist to R2
(`@proofoftech/flowsafe/artifacts`).

### Provisioning a tenant

Tenant ids are allocated by the `tenants` registry, and **nothing else enforces
their uniqueness**. Two clients slugged `acme` would merge their runs,
approvals, rate limits, and artifacts.

1. `provisionTenant(db, { tenantId, kind: 'commercial' })` (import from
   `@proofoftech/flowsafe/host-kit`) — insert-or-fail.
   `tenantId` must match `^[a-z0-9]{3,32}$` and must not be reserved from
   allocation: the infrastructure subdomains (`app`, `www`, `api`, `docs`,
   `admin`, `status`), `system` (the cron maintenance actor's audit identity —
   also rejected at token verification), and `default` (the conventional
   single-tenant id, kept unallocatable so a single-tenant host can adopt the
   registry later without a collision).
2. Only then issue tokens naming that tenant. A bearer-map entry or JWT claim
   without an INV-3-valid `tenantId` is dropped, and its token 401s.
3. Tenant ids are never reused. The registry is append-only: a purged tenant's
   row remains as a tombstone.

### Offboarding a tenant

`purgeTenant(db, { tenantId, artifactStore })` (import from
`@proofoftech/flowsafe/do-runner` or the package root) deletes the tenant's
snapshot rows of any status, its approval records, and its R2 artifacts.

**Revoke the tenant's tokens first and wait for the last one to expire.** The
purge deletes `suspended` rows, so a reviewer approving at that moment would
resume against a vanishing row. The outcome is absorbed (the resume fails, no
workflow re-executes) but the tenant sees an error rather than a clean
offboarding. `purgeExpiredWorkflowRuns` — the ordinary retention purge — never
touches a suspended row at any age, which is precisely why abandoned runs
require `purgeTenant`.

**If you store artifacts in R2, pass the same `artifactStore` to
`purgeExpiredWorkflowRuns` too.** Offboarding enumerates a tenant's artifacts
from its surviving snapshot rows; an unpaired retention purge deletes rows —
the only record of those runs' artifact keys — and strands their artifacts
where no later `purgeTenant` can find them.

### breakwater Middleware

Shipped as npm package `@proofoftech/breakwater` -- consumers add as a dependency.

## Run (Local Development)

```
pnpm dev            # the showcase app: Vite dev server + in-process API host (:4321)
pnpm -r dev         # instead: tsc --watch on both libraries (no app)
```

## Cron

The reference Worker declares **two** cron expressions and dispatches on
`controller.cron`, so the SLA sweep and the purge never share an invocation:

- `*/15 * * * *` — `sweepSLA(factory.system(), …)`. Escalates every open
  approval past its `slaDeadlineAt`, across all tenants. This is the only
  legitimate cross-tenant writer; it is cron-only and unreachable over HTTP.
- `7 * * * *` — `purgeExpiredWorkflowRuns` (terminal snapshots past
  `RUN_RETENTION_DAYS`), plus any demo-tenant reaping.

They are split because a Workers **CPU-limit termination kills the isolate and
is not a catchable JS exception**. Wrapping both in `try/catch` inside one
invocation does not help: a slow sweep would starve the purge permanently.
Keep the sweep interval at or below your SLA granularity.

### Approval queue retention

`purgeExpiredApprovals(factory.system(), …)` runs in the same `PURGE_CRON`
firing as `purgeExpiredWorkflowRuns`, in its own isolated try/catch — a
failure in either purge never stops the other. It deletes DECIDED approval
records (status `approved` or `rejected`) whose terminal timestamp
(`decidedAt`, falling back to `updatedAt` for a decided record persisted
without one) is older than `APPROVAL_RETENTION_DAYS` (var; default `30`;
`0` purges decided approvals immediately, same convention as
`RUN_RETENTION_DAYS`), LIMIT-batched per firing. Open requests
(`pending`/`claimed`/`escalated`) are never purged at any age — an approval
still awaiting a decision is not garbage, exactly like
`purgeExpiredWorkflowRuns` never touches a live run. An abandoned tenant's
still-open approvals are reclaimed only by `purgeTenant()` at offboarding.

**Self-healing recovery (D4, 2026-07-11 audit):** unlike
`purgeExpiredWorkflowRuns`, this purge never checks whether the run a decided
record's grant belongs to is still live — only the approval record's own
age; that omission is accepted by design (see `retention.ts`'s header
comment). A decided record whose resume attempt failed and has not yet been
retried can still be purged before the retry, which then fails CLOSED (no
leak — see `retention.ts`) and leaves the run suspended with no approval
record. Recovery is now automatic: the next `status()` read of that run (a
dashboard poll, or an operator's own `GET`) re-files a FRESH approval bound
to the run's current suspension, requiring a NEW decision — deliberately,
since an approval that aged past retention should not silently re-arm the
grant it already spent. One separation-of-duties relaxation applies only in
this recovery path: the re-filed record's requester is the system actor, not
a human, so the reviewer who decided the purged record may decide its
replacement. Set `APPROVAL_RETENTION_DAYS` well beyond any expected
resume-retry window regardless — reconciliation heals the wedge, but a
re-decision is real reviewer work, not a free retry.

**Stale OPEN records are healed too, not just purged-decided ones
(2026-07-11 audit, follow-up):** a re-suspension of the same step — e.g. via
the raw grant-free resume route — while an earlier request for that step is
still open (pending/claimed/escalated) leaves that record bound to a
fingerprint the step has already moved past. Left alone, this record wedges
forever: the store's open-step uniqueness index makes every later filing
attempt collapse back into it unchanged, so the fingerprint never heals and
every status poll repeats the same no-op list-then-create. The same
reconcile pass now SUPERSEDES every such stale open record first — a CAS
transition straight to `rejected`, attributed to the system actor, audited
as `approval.supersede`, never routed through `decide()` so it never touches
the run — before filing the fresh one. A concurrent real decision that wins
the CAS race is left alone; reconcile backs off filing for that step and
re-evaluates on the next `status()` read rather than double-filing or
clobbering the decision. A superseded record is excluded from grant
derivation by its terminal `rejected` status alone (grant derivation reads
only `status: 'approved'` records), not merely by the fingerprint mismatch
that triggered the supersede.

`GET /api/approvals` and the flowsafe dashboard are now paginated:
`ApprovalListFilter` accepts `limit` (1–500), an opaque `after` cursor
(derive the next page's cursor from the last record of the current page
with `approvalCursor()`), and `orderBy` (`created` — FIFO, the default —
or `reviewer`: priority → nearest SLA deadline → FIFO, applied before
`limit`; incompatible with `after`, since cursors only page the monotonic
FIFO order). The dashboard defaults to open statuses with `limit: 100` and
`orderBy: 'reviewer'`, so an open dashboard neither issues an unfiltered
full-table scan on every poll nor loses a fresh critical request past the
oldest 100 records to a FIFO page cut.

## Incident Response

| Symptom | Likely Cause | Action |
|---|---|---|
| Workflow stuck in "running" | DO CPU limit hit | Check Workers CPU limit logs; restart Durable Object |
| Approval not delivered | Cloudflare Queues backpressure | Check queue depth and consumer health |
| Suspended workflow cannot resume | Corrupted or orphaned run snapshot | Inspect the run's D1 snapshot; restore a consistent state |
| RBAC permission denied | Role missing or scope mismatch | Check audit log for `action: denied` entries |
| Every authenticated route 401s | `APPROVAL_ACTOR_TOKENS` unset, unparseable, or its entries lack an INV-3 `tenantId` | Check Workers Logs for `{"type":"config-error"}`; entries without a valid tenant are dropped by design |
| One tenant 403s everywhere | The actor's tenant claim fails `^[a-z0-9]{3,32}$`, or (with `TENANT_APEX_DOMAIN` set) the token's tenant does not match the host's subdomain | Fix the verifier's claim mapping; the routers refuse to concatenate an invalid tenant into a run id |
| A tenant sees 404 on a run it started | The run id does not carry that tenant's prefix — it was minted for another tenant, or predates tenant-salted ids | 404 is deliberate (not 403), so the route is no existence oracle. Legacy runs are not addressable; there is no drain runbook for a run-id format change |
| The Worker throws on every approval query | The D1 `flowsafe_approvals` table predates the `tenant_id` column | The store refuses to serve a tenant-less table rather than half-upgrading it. `ALTER TABLE … ADD COLUMN … NOT NULL` has no valid backfill, and a NULL tenant is an isolation hole. Recreate the database (nothing is deployed on the old schema) |
| An approved action silently does nothing after an idle period | Pre-fix behaviour: the resume ordinal was lost to Durable Object eviction | The ledger is now `ctx.storage`-backed. If it recurs, confirm the DO is constructed with `state` |
| SLA escalations stop, purge still runs (or vice versa) | The two crons share an invocation, or a cron expression drifted from the worker's constants | Check `{"type":"config-error","var":"triggers.crons"}` in Workers Logs; the worker logs and runs both surfaces when it sees an unknown expression |

## Tuning

- Durable Object alarm interval: reserved (no alarm is scheduled today; resume is driven by the approval decision)
- Approval SLA defaults: 4 business hours, escalate at 8 hours
- flowsafe dashboard polling: 10 second default, 30 seconds for viewer role
- Public-demo caps (`showcase/`): per-sandbox run cap, a **global daily run
  ceiling**, and a kill switch checked in the auth middleware so already-issued
  tokens die with it. Per-identity limits are a speed bump — someone with N
  free accounts mints N sandboxes — so size the daily ceiling for the spend you
  can tolerate and pair it with a billing alert.
- Demo self-reset (`showcase/`): visitors can wipe their own sandbox with
  `POST /demo/reset` (admin token + demo tenant only; logs
  `{"type":"demo-reset","tenantId":…}` with the purge counts). It reuses the
  reaper's `purgeTenant` but never touches `run_count`/`demo_daily`, so a
  reset cannot refill the spend budget.
