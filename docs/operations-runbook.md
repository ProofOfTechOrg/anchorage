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

1. `provisionTenant(db, { tenantId, kind: 'commercial' })` — insert-or-fail.
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

`purgeTenant(db, { tenantId, artifactStore })` deletes the tenant's snapshot
rows of any status, its approval records, and its R2 artifacts.

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
pnpm dev
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
