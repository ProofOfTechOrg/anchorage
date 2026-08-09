# Proposal: single-tenant packages with physical tenant isolation

> This document is a proposal. It is not implemented or supported product behavior. It records the analysis, decision, delivery plan, and Cloudflare cost model for moving the packages to a single-tenant runtime contract with physical (per-deployment) tenant isolation. Shipped behavior is documented in [Flowsafe architecture](../flowsafe-architecture.md), [Breakwater architecture](../breakwater-architecture.md), and the [security threat model](../security-threat-model.md). If accepted, this proposal supersedes the pooled multi-tenant posture assumed in parts of the [breakwater improvement roadmap](breakwater-improvement-roadmap.md).

Baseline: branch `dev`, commit `d78e779` ("feat: add connector invocation authorization"), written 2026-08-09. Line numbers and package behavior below are as of this commit and must be re-confirmed before editing. All Cloudflare pricing and limits were verified against the linked Cloudflare docs on 2026-08-09; re-verify before relying on them for a purchase decision.

## Decision summary

Adopt **tenant = deployment**:

1. Remove pooled multi-tenancy from the packages. Each customer organization gets its own Worker, its own D1 database(s), and its own Durable Object namespace instances via per-deployment bindings.
2. Keep every per-principal and per-run control: authentication seams, roles, permissions, approval grants, reserved-context stripping, egress, idempotency, rate limits, audit.
3. Replace logical tenant isolation with a small deployment-identity guard plus a provisioning control plane that is treated as security-critical.
4. Sequence the removal **before** the remaining roadmap work (secure preset, provisioning control plane), so later features land once, on the final foundation. Connector `requiredPermissions` shipped at this baseline; its per-principal projection (`breakwater.principalPermissions`) is tenancy-independent and survives the removal.
5. **Commit to plain Workers (one Worker per tenant) as the initial fleet topology**, and move all deployment maintenance from cron triggers onto Durable Object alarms so no per-tenant platform trigger exists. Workers for Platforms becomes a pre-planned escape hatch behind defined triggers — a control-plane change, not a data-plane migration (see "Switching to Workers for Platforms later").

Status: proposed. The decisive inputs are that the packages currently have **no consumers and no production deployments**, so a breaking simplification is free today and only gets more expensive.

## Why now

The shipped packages implement full logical multi-tenancy: tenant-branded stores, tenant claims in every authenticated router, tenant-prefixed Durable Object identities, tenant predicates on schedules, and tenant-scoped approval grants. That machinery is correct and tested — and it is also the single largest recurring engineering tax in the codebase:

- Every new flowsafe surface (signals, goals, schedules, notifications, artifacts) must re-implement tenant discipline plus adversarial same-key tests, forever. Each miss is a cross-tenant confidentiality vulnerability.
- The [security threat model](../security-threat-model.md) itself names the residuals: a host verifier that assigns the wrong tenant defeats every layer below it; the tenant-binding brand is recoverable by reflection in-process; every query on every surface must carry the tenant predicate correctly.
- For an AI agent product, cross-tenant memory recall is the catastrophic failure mode. Physical separation makes it structurally impossible instead of predicate-guaranteed.

With zero consumers, the usual counterarguments (breaking a published contract, existing pooled deployments) do not apply.

## Analysis

### The case for physical isolation

**Isolation belongs at the strongest available boundary.** SQL predicates and branded stores are the weakest isolation Cloudflare offers. Per-tenant Workers, D1 databases, and Durable Objects are stronger and carry no adversarial-correctness burden in our code. The packages should enforce only what infrastructure cannot: grants, approvals, permissions, egress, idempotency, rate limits, audit.

**Deleted code cannot regress.** The tenant tax stops accruing. The threat model's "tenant to tenant" trust boundary collapses to "separate deployments; the boundary is Cloudflare's".

**Cloudflare is the one platform where tenant-per-deployment scales down to a free tier.** Verified facts (details and sources in the cost appendix):

- 50,000 D1 databases per account, raisable by request to "millions to tens-of-millions".
- Workers for Platforms removes per-account script limits; user Workers are isolated by default.
- Workers and D1 scale to zero; an empty D1 database occupies roughly 12 KB; hibernating Durable Objects accrue no duration charge.
- The only tenancy-count-driven costs are the Workers for Platforms base fee and $0.02 per script per month beyond the included thousand. An idle tenant rounds to two cents a month.

**Operational wins.** Offboarding becomes "delete the Worker and database" instead of the carefully bounded `${tenantId}_` range purge. Blast radius (bad migration, corrupted store, stuck object) is one customer. Canary rollouts happen per tenant. Per-tenant data residency becomes possible with Durable Object jurisdictions and D1 location hints. The D1 10 GB per-database cap becomes a natural per-tenant quota instead of a shared ceiling.

### What we give up, and the replacing controls

**Defense in depth against mis-wiring.** Today, if two customers were somehow pointed at one database, tenant predicates would still separate their rows. After removal, a provisioning bug that binds tenant B's Worker to tenant A's database is a full cross-tenant breach. The provisioner becomes the security-critical component. Controls:

- **Deployment-identity sentinel.** Every deployment receives its tenant identifier as an environment binding. A sentinel row in its D1 database (and a check at Durable Object initialization) asserts ownership at startup and fails closed on mismatch. One cheap invariant replaces predicates-everywhere, and it keeps audit tenant tags honest because the tag comes from infrastructure configuration, not from a forgeable request claim.
- **Fleet drift audit.** A control-plane job enumerates user Workers and verifies the tenant-to-database-to-namespace mapping is one-to-one, continuously.
- Net effect: the isolation trusted computing base shrinks from "every query on every surface forever" to a small, rarely-changing, reviewable provisioning path. Residual confidentiality risk converts into availability/operations work, which is the right direction for a security product.

**Fleet operations become a first-class product concern.** Schema migrations must loop across every tenant database with per-tenant version state; rollouts need canary tenants; secrets and bindings are managed per user Worker through the API. This is deterministic engineering, but it is not optional: without it the foundation claim is hollow. The delivery plan makes it an explicit slice.

**Cross-tenant queries become pipelines.** Fleet-wide analytics, abuse detection, and "which tenants have stuck approvals" require aggregation (the audit export queue already provides the spine) instead of one SQL query.

### The bet and its tripwire

This bets that **pooled tenancy never returns**. Re-adding it later is a redesign, not a patch: retrofitting tenant predicates into code that assumed one tenant is exactly how cross-tenant bugs are born. On Cloudflare the bet is unusually safe because physical separation scales to millions of tenants. The tripwire that forces a revisit: a product that needs cross-tenant *shared data structures* (a shared marketplace, cross-organization collaboration on a single record). Agent runtimes for customer organizations do not have that shape; a shared workspace is simply its own deployment.

One boundary against over-rotating: "maximum clean" applies to the data plane. The control plane (signup, billing, provisioning, aggregated audit) is inherently multi-tenant and always will be. Keep it thin; do not pretend it away.

### Rejected alternatives

- **Keep both modes (single- and multi-tenant).** Correct answer while consumers existed; rejected now because dual-mode doubles the test matrix and documentation surface, keeps the recurring tenant tax on every new feature, and invites accidental pooled deployments. Recorded as the previous recommendation so a future session does not re-litigate it without the "no consumers" premise changing.
- **One Worker with thousands of D1 bindings, routed per request.** Approximately 5,000 bindings per script make it technically possible. Rejected: request-time binding selection reinstates logical routing as the isolation boundary, which is the exact failure mode this proposal eliminates.
- **Account-per-tenant.** The hardest boundary Cloudflare offers (separates even account-level limits), but operationally heavy (billing, tokens, dashboards per account). Reserve for a future regulated-customer tier; not the default.
- **Freeze multi-tenant code but keep it shipped.** Frozen code still costs test matrix, docs, and invariant discipline, and its presence invites pooled deployments. Delete instead; the design record survives in git history, the threat model, and this document.

## What tenancy removal deletes, and what stays

A tenant was never one user. The removal targets *tenant* machinery only; every *user*-facing control inside a deployment stays. Verify each anchor before editing (read-before-edit; paths are as of the baseline commit).

Deleted from flowsafe:

| Mechanism | Anchor |
| --- | --- |
| Tenant claim validation in authenticated routers (malformed claim → 403) | `packages/flowsafe/src/host-kit/run-router.ts`, `stream-router.ts` |
| Tenant-branded store factories (`forTenant()`), brand, tenant context | `packages/flowsafe/src/approval-api/tenant-store.ts`, `tenant-brand.ts`, `tenant-context.ts`, `d1-store.ts` |
| Internal tenant header on the thread topology | `packages/flowsafe/src/host-kit/thread-topology*` (`THREAD_TENANT_HEADER`) |
| `${tenantId}_${uuid}` Durable Object identity minting and `id.name` recheck | `packages/flowsafe/src/do-runner/` (see "Tenant invariants" in the [threat model](../security-threat-model.md)) |
| Tenant id charset invariant `^[a-z0-9]{3,32}$` and `${tenantId}_` range-purge bounds | same threat-model section |
| Tenant metadata predicates on schedules; webhook tenant derivation from subscription rows | `packages/flowsafe/src/schedules/schedules-d1.ts`, signal-provider webhook ingress |
| Tenant component of structured approval grants | `packages/flowsafe/src/approval-api/grants.ts` (`connectorGrantsForLeg`) |
| Tenant offboarding range purge | storage layer + operations runbook |

Stays (unchanged or trivially re-keyed):

- **All of breakwater.** Its `Actor` is already tenant-agnostic and `ISOLATION_SCOPE_CONTEXT_KEY` is an opaque scope (`packages/breakwater/src/policy-engine/tool-policy.ts`); the "do not add `tenantId` to `Actor`" boundary in the roadmap turns out to be exactly right for this future. `crossWorkflowIsolation`, egress enforcement, idempotency and rate-limit stores (re-keyed without a tenant component), approval-grant matching, audit.
- **Authentication seams, roles, `requiredPermissions`, the agent catalog, guarded agents, reserved-context stripping.** Users within one organization still attack each other.
- **404-before-role-error resource checks** inside a deployment (user-to-user and run-ownership probing).
- **Grant binding to workflow + run + suspension + tool-call identity.**

Added:

- Deployment identity: environment-configured tenant tag, D1 sentinel-row check at startup, Durable Object initialization check, fail closed on mismatch; audit events carry the tag from configuration.
- Control-plane reference implementation (provisioning, migration loop, drift audit) — see the delivery plan.

## Deployment topology

### What Workers for Platforms is

[Workers for Platforms](https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/) is Cloudflare's product for running large fleets of Workers under one account, designed for platforms that deploy code on behalf of their customers. Components:

- **Dispatch namespace.** A container for "user Workers" (one per tenant, here). Scripts in a namespace do not count against the per-account Worker limit — the namespace is documented as supporting an unlimited number of Workers, with no limit on Durable Object namespaces either.
- **Dynamic dispatch Worker.** A router we own. It receives every request, resolves the tenant (for example from the hostname), and invokes the tenant's user Worker via `env.DISPATCHER.get(name)`. Requests across the dispatch → user → outbound chain bill as a single request.
- **User Workers.** Per-tenant instances of the flowsafe host. Uploaded via API (not wrangler config), with per-script bindings: each tenant's D1 database, Durable Object namespaces, queue producers, R2 buckets, and secrets are attached only to that tenant's Worker. This binding-level attachment is the physical isolation mechanism.
- **Outbound Workers (optional).** Intercept `fetch()` egress from user Workers. For us this is defense-in-depth under breakwater's `runtime.fetch` manifest enforcement: an infrastructure-level egress choke point that connector code cannot bypass, closing part of the "do not market `runtime.fetch` as a complete sandbox" residual.
- **Isolation modes.** Namespaces default to "untrusted" (isolated caches, no `request.cf`). Since we author all tenant Workers, "trusted" mode is acceptable; untrusted is a defensible default anyway.
- **Custom limits.** Per-script CPU caps let the platform bound a runaway tenant — a denial-of-wallet control that plain Workers do not offer per-Worker.

### Committed topology: plain Workers, one per tenant

The initial fleet runs on plain Workers: one normal Worker per tenant, deployed by a control-plane loop with generated per-tenant configuration, each with its own D1/DO/queue-producer bindings, routed by hostname or route pattern. No dispatch hop, no platform subscription, no per-script fee.

Verified constraints on the Workers Paid plan ([limits](https://developers.cloudflare.com/workers/platform/limits/)), after the maintenance redesign below:

| Constraint | Value | Consequence |
| --- | --- | --- |
| Workers per account | 500 | The remaining hard tenant ceiling; the documented trigger for Workers for Platforms |
| Cron Triggers per account | 250 | **Removed as a constraint** by the alarm redesign (previously the binding limit: two crons per deployment capped the fleet near 125 tenants) |
| Environment variables per Worker | 128 | Fine per tenant |
| Worker size | 10 MB | Fine |
| No per-script fee | — | Tenant count itself costs nothing on plain Workers |

### Agents per deployment

The deployment unit is the client, not the agent. A client's Worker hosts that client's entire agent catalog: `createAgentModuleCatalog()` (`packages/flowsafe/src/agent-host/catalog.ts`) takes a plural module list and indexes it by id, the thread host resolves `agentId` per request and applies that agent's own `allowedRoles`/`allowedAutomation`/`requiredPermissions`, and workflows register the same way (the baseline Worker's `WORKFLOWS` array). Adding an agent to a client is a catalog entry plus a redeploy of that client's Worker — no new Worker, database, or namespaces. Durable runs of every agent are instances of the same Durable Object classes, and instances are unlimited.

The 500-Workers ceiling therefore bounds **client count, not agent count** (and on Workers for Platforms even that bound disappears). The practical per-Worker bounds on catalog size are generous: the 10 MB script limit (packages and Mastra are shared; per-agent cost is instructions, schemas, and any unique connector code), the 1-second startup limit (the thread host already constructs the catalog lazily), and the 128-secret budget for per-connector credentials.

Two intra-client consequences are deliberate design points, not defects:

- **Shared version, shared blast radius.** All of a client's agents deploy atomically; a bad deploy affects all of them, and only them. A client wanting isolation between agent tiers (production versus experimental) shards into two deployments — deployment unit = client × environment — spending one extra Worker from the budget.
- **Shared stores, shared budgets by default.** One D1 per client means its agents share idempotency and rate-limit stores. Rate budgets key per connector plus scope, and the opaque isolation scope with `crossWorkflowIsolation` remains available to partition budgets per workflow or agent when one noisy agent must not starve the client's others — an in-deployment policy concern, exactly where per-principal authorization already lives.

### Maintenance on Durable Object alarms, not cron triggers

The baseline deployment currently declares two cron expressions per deployment (`packages/flowsafe/deploy/wrangler.jsonc` `"crons": ["*/15 * * * *", "7 * * * *"]`) for the SLA sweep and the retention purge. `deploy/crons.ts` documents why they never share an invocation: a CPU-limit kill is uncatchable, so a slow sweep sharing an invocation could permanently starve the purge. Any replacement must preserve that failure-isolation property, not just the schedule.

Design: one singleton maintenance Durable Object per deployment (fixed instance name), storing `nextSweepAt`/`nextPurgeAt` and holding one alarm at `min(next due)`.

- **One due task per alarm invocation.** The handler re-arms first (persist the next alarm before doing work), then runs exactly one due task; if the other task is also due it sets an immediate follow-up alarm instead of running both. This reproduces the cron contract's guarantee — a CPU kill during the sweep cannot take the purge down with it — and Durable Object single-threading additionally guarantees the two tasks never run concurrently, which the `*/15` versus `7 * * * *` minute offsets only made likely.
- **Re-arm-first + built-in retry.** Alarms retry automatically on exception; persisting the schedule before executing work means a crash mid-task cannot break the chain.
- **Bootstrap and watchdog.** Provisioning calls an authenticated `ensure-maintenance` admin endpoint once after deploy; the control-plane drift audit also polls a health field (`lastSweepAt`/`lastPurgeAt`) and re-arms any deployment whose maintenance has gone stale. No path depends on platform triggers.
- **Cost.** Sweep every 15 minutes plus hourly purge ≈ 3,600 alarm invocations per deployment per month, each an included-tier Durable Object request plus one row written per `setAlarm`. At 200 tenants that is ~0.72 million DO requests and rows written per month — roughly $0.11 beyond the included tier at the margin; negligible at any scale this proposal contemplates.
- **Deletions.** The `scheduled()` handler, `deploy/crons.ts`, and the wrangler `triggers` block go away; the deploy e2e test asserts alarm-driven sweep/purge instead of the cron byte-equality contract.

This also removes a real Workers for Platforms incompatibility rather than an assumed one: `triggers.crons` is silently dropped when deploying into a dispatch namespace (see "Resolved platform questions"). With no cron dependency anywhere, the fleet is trigger-portable by construction.

### Switching to Workers for Platforms later: easy? necessary? code changes?

**Is it easy?** Yes — by design, provided the plain-Workers phase holds four portability invariants (all adopted by this proposal):

1. **No platform triggers.** No `scheduled()` handler, no cron expressions; all timing lives in Durable Object alarms inside the deployment (the alarm redesign above).
2. **Tenant Workers produce to queues but never consume them.** The audit pipeline is producer-only in the data plane; the shared audit queue's consumer lives in the control plane. (The current baseline Worker consumes the audit queue itself — `queue()` in `packages/flowsafe/deploy/worker.ts`; the fleet reference moves that consumer out, since dispatch-namespace scripts cannot be queue-consumer targets.)
3. **Host-based routing only.** Tenant resolution comes from the hostname, so a dispatch Worker can replicate routing with a lookup table; nothing depends on per-Worker routes or `workers.dev` URLs.
4. **Provisioning behind an interface.** The control plane deploys tenants through a `ProvisioningBackend` seam (create/update script, attach bindings, run migrations). The wrangler-loop backend and the Workers for Platforms Upload-API backend are two implementations of the same interface.

Additionally, avoid `request.cf` in the data plane (untrusted namespaces disable it) — the packages do not depend on it today; keep it that way or accept trusted mode later.

**What code changes at switch time?** The packages and the per-tenant Worker: **zero changes** — a user Worker's handler signature and bindings model are identical to a plain Worker's. Durable state needs **no migration**: flowsafe persists runs, approvals, schedules, and memory in each tenant's D1 database, and the D1 databases, DO namespaces, and queues simply get re-bound to the new scripts. The switch consists of new, additive components: a small dispatch Worker (hostname → tenant script → `env.DISPATCHER.get(name).fetch(request)`), a second `ProvisioningBackend` implementation targeting the Upload User Worker API (multipart metadata bindings, `keep_bindings`, DO migration metadata), route/custom-hostname re-pointing from per-tenant Workers to the dispatch Worker, and optionally an outbound Worker for infrastructure-level egress interception. Migration can run tenant-by-tenant (deploy user Worker with identical bindings, flip the hostname, delete the plain Worker), with rollback being the reverse flip.

**Is it necessary?** Only when one of these binds — none is expected soon:

- Fleet size approaching the 500-Workers-per-account ceiling (the one plain-Workers limit the alarm redesign does not remove).
- Per-tenant CPU caps for denial-of-wallet containment (custom limits exist only on Workers for Platforms).
- The outbound-Worker egress layer as defense-in-depth under breakwater's `runtime.fetch`.
- Fleet-deploy ergonomics: hundreds of sequential wrangler deploys against API rate limits versus namespace uploads.

If none binds, plain Workers remain the permanent topology; the $25 base fee and dispatch hop buy nothing at small fleet sizes.

### Cost comparison between the topologies

| | Plain Workers (Paid) | Workers for Platforms |
| --- | --- | --- |
| Base fee | $5/month/account | $25/month/account |
| Included requests | 10 million/month | 20 million/month |
| Included CPU time | 30 million CPU-ms | 60 million CPU-ms |
| Overage (requests, CPU) | $0.30/million; $0.02/million CPU-ms | Same rates |
| Scripts | 500 max, no fee | 1,000 included, then $0.02/script/month, no hard cap |
| Per-tenant CPU caps | No | Yes (custom limits) |
| Egress interception | No | Outbound Workers |
| Deploy mechanism | wrangler per Worker | Upload API into namespace |
| Routing | Routes/custom domains per Worker | Dispatch Worker we write |

D1, Durable Objects, and Queues bill identically under both (they are account-level products); the topology choice does not change storage or compute-adjacent costs.

## Cost model

### Verified pricing facts (2026-08-09)

Sources: [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/) (page dated 2026-07-07), [Workers for Platforms pricing](https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/platform/pricing/) (2026-04-21), [D1 pricing](https://developers.cloudflare.com/d1/platform/pricing/) (2026-04-21), [D1 limits](https://developers.cloudflare.com/d1/platform/limits/), [Queues pricing](https://developers.cloudflare.com/queues/platform/pricing/) (2026-04-21), [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) (2026-07-28).

Workers (both plans): inbound requests only — subrequests are free; no duration charge for Workers themselves.

Durable Objects (Workers Paid; all our agent runs, signal hosts, and alarm-driven maintenance live here):

| Meter | Included/month | Overage |
| --- | --- | --- |
| Requests (HTTP, RPC sessions, WebSocket messages at 20:1, **alarm invocations**) | 1 million | $0.15/million |
| Duration (wall-clock while active or non-hibernatable, billed at the full 128 MB = 0.125 GB) | 400,000 GB-s | $12.50/million GB-s |
| SQLite rows read | 25 billion (shared metric family with D1) | $0.001/million |
| SQLite rows written (each `setAlarm` = one row written; deletes count) | 50 million | $1.00/million |
| SQL stored data | 5 GB-month | $0.20/GB-month |

The duration meter is the one agent workloads must respect: a Durable Object awaiting an LLM or tool `fetch()` is active (not hibernation-eligible), so **every second of model latency inside a run is billed wall-clock at 0.125 GB-s**. Hibernating between approvals/signals is free; WebSocket hold-open should use the hibernation API.

D1 (per account, all databases summed): rows read 25 billion included then $0.001/million; rows written 50 million included then $1.00/million; storage 5 GB included then $0.75/GB-month. Scale-to-zero; empty database ≈ 12 KB; 10 GB max per database; 50,000 databases per account, raisable by request; 1 TB account storage, raisable.

Queues (audit export): roughly 3 operations per delivered message (write, read, delete), 64 KB units; 1 million operations included, then $0.40/million.

### Cost formulas

With `max(0, x)` overage semantics against the included amounts above:

```text
fixed        = $5 (plain) or $25 (WfP)
scripts      = WfP only: max(0, tenant_workers + dispatch_workers − 1000) × $0.02
requests     = max(0, inbound_requests − included) / 1M × $0.30
cpu          = max(0, Σ(requests × avg_cpu_ms) − included) / 1M × $0.02
do_requests  = max(0, (run_invocations + alarms + ws_messages/20) − 1M) / 1M × $0.15
do_duration  = max(0, Σ(active_seconds) × 0.125 − 400k) / 1M × $12.50
rows_read    = max(0, total_rows_read − 25B) / 1M × $0.001
rows_written = max(0, total_rows_written − 50M) / 1M × $1.00
storage      = max(0, D1_GB − 5) × $0.75 + max(0, DO_SQL_GB − 5) × $0.20
queues       = max(0, audit_messages × 3 − 1M) / 1M × $0.40
```

The structural insight: **every meter except `fixed` and `scripts` scales with usage, not with tenant count.** Physical separation is therefore approximately free on this platform; splitting one pooled deployment into a thousand dedicated ones moves the same reads, writes, and seconds into different buckets and adds only the script fee.

### How to measure our variables

There are no production deployments yet, so the variables must come from a measurement pass, not from guesses: deploy the `packages/agent-starter` baseline plus the flowsafe deploy Worker, drive synthetic load through representative agent runs (including an approval suspension and resume), and read:

- `rows_read` / `rows_written` per query from the D1 `meta` object, aggregated in the dashboard or GraphQL analytics.
- Durable Object duration (GB-s) and request counts from the dashboard/GraphQL analytics per namespace.
- Worker `cpuTime` and `wallTime` from invocation logs.
- Queue operation counts from queue metrics.

Fill this table per "run" (one durable agent execution including its tool calls) and per interactive request, then plug into the formulas:

| Variable | Meaning | Measured value |
| --- | --- | --- |
| `T` | tenants | — |
| `R` | agent runs / tenant / month | — |
| `I` | inbound HTTP requests / run (start, stream, status, approval UI) | — |
| `S` | Durable Object active seconds / run (≈ total model + tool latency) | — |
| `cpu` | CPU-ms / request | — |
| `dr`, `dw` | rows read / written per run (workflow state + memory + approval store) | — |
| `a` | audit events / run | — |

### Worked examples

Illustrative assumptions (replace with measured values): 15 ms CPU per request, 5–10 requests per run, 45 s of model/tool latency per run, 500 rows read and 60 rows written per run, 10 audit events per run.

**Scenario A — pilot, plain Workers, 50 tenants, 2,000 runs/tenant/month (100k runs).**
Requests ≈ 1M (included). CPU ≈ 7.5M CPU-ms (included). DO requests ≈ 0.6M (included). DO duration = 100k × 45 s × 0.125 = 562.5k GB-s → 162.5k over → **$2.03**. D1 reads 50M (included), writes 6M (included). Queue ops 3M → 2M over → **$0.80**. **Total ≈ $8/month** on a $5 base. Platform cost is noise; model-token spend will dominate by orders of magnitude.

**Scenario B — free-tier heavy, Workers for Platforms, 2,000 tenants, 100 active.**
Scripts: 2,001 → 1,001 over → **$20.02**. Active usage similar to Scenario A → a few dollars. Storage: 2,000 mostly-empty databases ≈ 24 MB (negligible). **Total ≈ $50/month.** Marginal idle tenant ≈ $0.02/month — the number that makes physical separation viable down to a free tier.

**Scenario C — heavy production, 200 tenants, 10k runs/tenant/month (2M runs).**
Requests 20M → 10M over (plain) → $3.00. CPU 300M CPU-ms → 270M over → $5.40. DO requests 12M → 11M over → $1.65. DO duration 2M × 45 s × 0.125 = 11.25M GB-s → **$135.63** (dominant). D1 reads 1B (included); writes 120M → 70M over → **$70.00**. Queue ops 60M → 59M over → $23.60. **Total ≈ $244/month on plain Workers, ≈ $260 on Workers for Platforms** (higher base, larger included allotments, script fee zero at 201 scripts). Alarm-driven maintenance adds ~0.72M Durable Object requests and rows written at this scale (~$0.11 + $0.72) — noise.

Conclusions: Durable Object duration and D1 row writes are the meters to engineer against (hibernate aggressively; batch writes; keep audit events lean). The plain-vs-platforms delta is small and operational, not financial. Tenancy count is financially irrelevant below thousands of tenants.

## Delivery plan

Ordered slices; each is a reviewable unit with its own verification. Quality-gate review lanes apply to every slice.

**Slice A — tenancy removal + deployment identity (do first).**
Remove the deleted-list mechanisms from flowsafe; re-key stores without tenant components; introduce the deployment-identity binding, sentinel-row startup check, and Durable Object initialization check; rewrite the threat model's tenant sections as a provisioning-boundary section; update the roadmap, operations runbook, deployment reference, and package READMEs; changesets for both packages (breaking for flowsafe's public types, breakwater expected unchanged).
*Verification:* `pnpm lint && pnpm typecheck && pnpm test && pnpm build && pnpm docs:check`, `pnpm --filter @proofoftech/flowsafe spike:verify` (spike scenarios rewritten from cross-tenant forgery to sentinel-mismatch fail-closed), pack tests.

**Slice B — maintenance onto Durable Object alarms.** Implement the singleton maintenance Durable Object described above (re-arm-first, one due task per invocation, `ensure-maintenance` bootstrap endpoint, health fields); delete the `scheduled()` handler, `deploy/crons.ts`, and the wrangler `triggers` block; move the audit-queue consumer out of the tenant Worker template so the data plane is queue-producer-only (the portability invariant). Removes the cron-count ceiling and every platform-trigger dependency.
*Verification:* deploy Worker e2e test asserts alarm-driven sweep/purge, failure isolation between the tasks (a throwing sweep does not starve the purge), and re-arm after a crashed invocation; spike scenario covers alarm-chain recovery.

**Slice C — single-tenant secure preset.** The reduced roadmap section 11: one validated `singleTenantConnectorPolicies()`-style builder that requires durable stores, audit posture, and egress policy, and rejects contradictory configuration at construction. The multi-tenant preset item is dropped. (Connector `requiredPermissions`, previously planned here, shipped at the baseline commit — roadmap Phase C step 3 — so the preset can require permission wiring from day one.)

**Slice D — provisioning control plane (reference implementation, plain Workers first).** A `ProvisioningBackend` seam with the wrangler-loop implementation as the shipped backend: create tenant (D1 create → migrate → seed sentinel → deploy Worker with generated per-tenant config and bindings → call `ensure-maintenance`), fleet migration loop with per-tenant schema-version state and canary ordering, drift audit (one-to-one tenant/database/namespace mapping plus maintenance-staleness watchdog), decommission runbook (revoke credentials → delete Worker → export-then-delete database), fleet version-drift report, and the shared audit-queue consumer. The Workers for Platforms backend is specified as a playbook (dispatch Worker, Upload-API backend, hostname re-pointing) but not built until a switch trigger binds.
*Verification:* an end-to-end provisioning test against a scratch Cloudflare account: create two tenants, prove sentinel mismatch fails closed, prove maintenance self-arms, prove decommission leaves no orphan resources; throttle the loop within the global API budget (1,200 requests per 5 minutes per account token).

## Out of scope — must not change

- **Breakwater's tenant-agnostic `Actor` and opaque isolation scope.** Already correct for this future; do not delete `ISOLATION_SCOPE_CONTEXT_KEY` or `crossWorkflowIsolation` — they scope non-tenant concerns.
- **Per-user authorization, approval grants, reserved-context stripping, 404-before-role checks.** These protect users from each other inside one deployment; they are not tenancy code.
- **The control plane's own multi-tenancy.** Signup, billing, and provisioning inherently span tenants; keep that plane thin rather than pretending it away.
- **LLM/provider cost controls.** Model-token spend dominates platform spend at every scale above; it is a separate workstream.

## Resolved platform questions

Verified 2026-08-09; kept here so future sessions do not re-investigate:

- **Cron triggers do not work on dispatch-namespace user Workers.** Wrangler silently drops `triggers.crons` when deploying with `--dispatch-namespace`, and the namespace scripts API has no `/schedules` subresource ([workers-sdk #13840](https://github.com/cloudflare/workers-sdk/issues/13840)). Irrelevant to us after Slice B removes cron dependence everywhere.
- **User Workers cannot be queue consumers.** The consumer registration API does not accept namespace scripts and the Upload API's `queue` binding type attaches producers only ([workers-sdk #6758](https://github.com/cloudflare/workers-sdk/issues/6758)). Slice B's producer-only data plane matches this by design.
- **Fleet API budget.** The global Cloudflare API limit is 1,200 requests per 5 minutes per account token ([Workers for Platforms limits](https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/platform/limits/)); the Slice D migration loop must throttle within it.
- **No gradual deployments for user Workers** (all-at-once per script). Acceptable: the fleet canaries per tenant, not per script version.

## Open questions to verify before the switch (not before build)

1. **Workers for Platforms + Durable Object bindings mechanics.** The bindings documentation lists Durable Objects among attachable resources; confirm the exact API shape for namespace-class bindings on user Workers (including migrations for new classes) on a scratch account when executing the switch playbook.
2. **Data-residency options per tenant** (Durable Object jurisdictions, D1 location hints) if a regulated-customer tier materializes.

## Appendix: current-state anchors

Evidence that pins the claims above to the baseline commit; re-read before editing.

Baseline deployment declares two crons per deployment — `packages/flowsafe/deploy/wrangler.jsonc`:

```jsonc
"triggers": { "crons": ["*/15 * * * *", "7 * * * *"] },
```

Connector invocation authorization is shipped at this baseline — `PermissionManifest.requiredPermissions` and the trusted `breakwater.principalPermissions` projection exist in `packages/breakwater/src/connector-sdk/index.ts`, enforced before dry-run and approval-grant consumption (roadmap Phase C step 3). Both are per-principal and survive Slice A unchanged.

Tenant claim handling that Slice A deletes — `packages/flowsafe/src/host-kit/run-router.ts` treats a malformed tenant claim as a verifier bug (403) before resource resolution; `packages/flowsafe/src/host-kit/stream-router.ts` mirrors it.

Opaque isolation scope that Slice A keeps — `packages/breakwater/src/policy-engine/tool-policy.ts`:

```ts
export const ISOLATION_SCOPE_CONTEXT_KEY = 'breakwater.isolationScope';
```

Tenant invariants being replaced (identity minting, charset, branded stores, range purge) are specified in the "Tenant invariants" section of the [security threat model](../security-threat-model.md).

Cron-contract rationale the alarm redesign must preserve — `packages/flowsafe/deploy/crons.ts`:

```ts
 * Maintenance runs on TWO cron expressions, dispatched on controller.cron so
 * the SLA sweep and the retention purge NEVER share an invocation — a
 * CPU-limit kill is uncatchable, so sharing one would let a slow sweep
 * permanently starve the purge.
```
