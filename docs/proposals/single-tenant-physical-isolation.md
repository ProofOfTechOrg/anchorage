# Proposal: single-tenant packages with physical tenant isolation

> Status: implemented. Slices A through D shipped on 2026-08-10, and Slice E shipped on 2026-08-11. The paid Workers for Platforms namespace conformance remains an operator-run release gate because it requires Cloudflare account credentials and billable resources. Shipped behavior is documented in [Fleet control](../fleet-control.md), [Flowsafe architecture](../flowsafe-architecture.md), [Breakwater architecture](../breakwater-architecture.md), [Connector interface](../connector-interface.md#apply-the-physical-deployment-preset), and the [security threat model](../security-threat-model.md). This decision supersedes the pooled multi-tenant posture assumed in parts of the [breakwater improvement roadmap](breakwater-improvement-roadmap.md).

The product target is the **Super platform**: a hosted agent cloud where customers author agent projects locally and deploy them onto the platform, which runs them durably with metered capability access. These packages supply the Super platform's execution-security slice — breakwater as the policy-enforcement point, flowsafe as the durable approval-gated runtime. The capability marketplace, provider adapters, MCP/SDK surfaces, build service, developer studio, and the economic plane (payment authorization and settlement) are Super platform components maintained outside this repository.

Baseline: branch `dev`, commit `d78e779` ("feat: add connector invocation authorization"), written 2026-08-09. Line numbers and package behavior below are as of this commit and must be re-confirmed before editing. All Cloudflare pricing and limits were verified against the linked Cloudflare docs on 2026-08-09; re-verify before relying on them for a purchase decision.

## Historical post-adoption baseline for Slices B through E

This section records the handoff after mature package adoption and before Slices B through E were implemented. All four slices subsequently shipped; the current contracts are in the public guides linked in the status banner.

Start the next implementation session after [mature package adoption PR #55](https://github.com/ProofOfTechOrg/anchorage/pull/55) is merged into `dev`. Commit `3276c2a` contains the code baseline for this handoff. This section supersedes the older `d78e779` source baseline for Slices B through E. The older appendix remains historical evidence for Slice A and the original decisions.

The package-adoption work did not change the scope, order, or acceptance criteria of Slices B through E. It changed the implementation and verification foundation:

- **Real Cloudflare tests:** extend `vitest.flowsafe-harness.config.ts` and `scripts/flowsafe-harness.test.ts` for full deployment behavior. Extend `vitest.flowsafe-workers.config.ts` only for lightweight production modules that load reliably in the Workers pool. The full Mastra-backed Worker graph crashes the pool's bundled workerd during module loading; Wrangler `createTestHarness` is the verified path for that graph.
- **No D1 fidelity facades:** `d1DatabaseLike`, its serialized `batchTail`, and fake `raw`, `exec`, and `dump` capabilities are deleted. Keep SQLite adapters restricted to explicitly non-fidelity unit tests. Run concurrency, rollback, schema, ownership, retention, and deployment-identity claims against real D1.
- **Provisioning behavior:** fresh D1 databases contain Cloudflare-owned `_cf_KV` and `_cf_METADATA` tables. `deployment-identity-protocol.mjs` excludes only those exact names while rejecting lookalikes and application tables. Slice E must reuse this protocol and its packed CLI instead of reproducing sentinel logic.
- **Worker type boundary:** root Cloudflare harnesses and showcase use `@cloudflare/workers-types` v5. FlowSafe and agent-starter retain v4 because `@mastra/cloudflare-d1@1.1.1` peers on v4. Keep compatibility proofs in `scripts/r2-type-compatibility.ts` and `packages/flowsafe/test-support/r2-type-compatibility.ts`; do not collapse the two ambient type programs.
- **Runtime topology:** showcase development and production builds now use Cloudflare's Vite plugin and the real Wrangler binding graph. The custom Worker and Durable Object emulator is gone. WebSocket delivery, reconnection, and no-stream polling fallback are covered by `packages/showcase/worker/worker.harness.test.ts`.
- **Security protocols:** actor tokens and stream tickets now use `jose`, with separate audiences and protected types. GitHub webhook verification retains raw-byte WebCrypto with an exact signature grammar. OAuth and OpenID Connect mechanics live behind the existing showcase provider seam. Slices B through E must preserve these boundaries rather than reintroducing protocol code.
- **Architecture and publication gates:** dependency-cruiser scans the complete FlowSafe production graph and has a positive control for every rule. Publint and Are the Types Wrong (ATTW) validate real package archives while repository-specific consumer probes remain. New slice code must pass these gates through the root test and packed-package commands.

Before editing a slice:

1. Run `git fetch --prune origin` and confirm PR #55 is merged into `origin/dev`.
2. Record the resulting `origin/dev` commit as that slice's base and re-confirm every source anchor below.
3. Run `pnpm install --frozen-lockfile`, `pnpm typecheck`, and `pnpm test` before changing code.
4. Use the real-runtime harness named above for every Cloudflare fidelity claim.

Slice-specific handoff:

- **Slice B:** add alarm behavior to the full Wrangler harness. Prove re-arm-first ordering, one due task per invocation, failure isolation, crash recovery, and maintenance health fields through the production Worker graph.
- **Slice C:** reuse the full spike and real D1 harness for the deterministic failure matrix. Compare one durability authority at a time; do not use the SQLite unit adapters as recovery evidence.
- **Slice D:** preserve the existing package boundaries and public declaration checks. Hono is currently private to showcase `/auth/*`; FlowSafe has no Hono runtime dependency and the secure preset must not introduce one without a separate deletion-based package decision.
- **Slice E:** reuse the deployment-identity protocol, seed CLI, D1 ownership stores, full harness, and packed CLI probe. The provisioner must tolerate exact D1-owned internal tables, reject database drift, and preserve the v4/v5 type boundary.

## Decision summary

Adopt **tenant = deployment**:

1. Remove pooled multi-tenancy from the packages. Each customer organization gets its own Worker, its own D1 database(s), and its own Durable Object namespace instances via per-deployment bindings.
2. Keep every per-principal and per-run control: authentication seams, roles, permissions, approval grants, reserved-context stripping, egress, idempotency, rate limits, audit.
3. Replace logical tenant isolation with a small deployment-identity guard plus a provisioning control plane that is treated as security-critical.
4. Sequence the removal **before** the remaining roadmap work (secure preset, provisioning control plane), so later features land once, on the final foundation. Connector `requiredPermissions` shipped at this baseline; its per-principal projection (`breakwater.principalPermissions`) is tenancy-independent and survives the removal.
5. **Run a two-stage topology.** Pre-launch, the fleet runs on plain Workers ($5/month account) because every deployed artifact is team-authored. From the first externally authored artifact, the execution plane runs on Workers for Platforms untrusted dispatch namespaces. The boundary is a security rule, not a scaling threshold: **no customer-built artifact ever executes on a plain Worker or in a trusted namespace.** Move all deployment maintenance from cron triggers onto Durable Object alarms now, so no per-tenant platform trigger exists in either stage (see "The launch switch").
6. **Benchmark the durable-run protocol before the control plane hardens.** Flowsafe's Durable Object runner and Cloudflare Workflows are competing durability authorities; the Super platform requires exactly one. It becomes its own delivery slice.
7. **Bind application configuration without weakening physical isolation.** Canonical application variables and secret descriptors belong to release identity. Fleet-owned R2 buckets belong to immutable deployment identity. Application KV is rejected because the account limit cannot serve the 10,000-deployment horizon without shared tenant storage.

Status: implemented. The decisive inputs were that the packages had **no consumers and no production deployments**, so the breaking simplification was cheapest before adoption.

## Why now

At the proposal baseline, the shipped packages implemented full logical multi-tenancy: tenant-branded stores, tenant claims in every authenticated router, tenant-prefixed Durable Object identities, tenant predicates on schedules, and tenant-scoped approval grants. That machinery was correct and tested, and it was also the single largest recurring engineering tax in the codebase:

- Every new flowsafe surface (signals, goals, schedules, notifications, artifacts) must re-implement tenant discipline plus adversarial same-key tests, forever. Each miss is a cross-tenant confidentiality vulnerability.
- The baseline [security threat model](../security-threat-model.md) named the residuals: a host verifier that assigned the wrong tenant defeated every layer below it; the tenant-binding brand was recoverable by reflection in-process; every query on every surface had to carry the tenant predicate correctly.
- For an AI agent product, cross-tenant memory recall is the catastrophic failure mode. Physical separation makes it structurally impossible instead of predicate-guaranteed.

With zero consumers, the usual counterarguments (breaking a published contract, existing pooled deployments) do not apply.

## Analysis

### The case for physical isolation

**Isolation belongs at the strongest available boundary.** SQL predicates and branded stores are the weakest isolation Cloudflare offers. Per-tenant Workers, D1 databases, and Durable Objects are stronger and carry no adversarial-correctness burden in our code. The packages should enforce only what infrastructure cannot: grants, approvals, permissions, egress, idempotency, rate limits, audit.

**Deleted code cannot regress.** The tenant tax stops accruing. The threat model's "tenant to tenant" trust boundary collapses to "separate deployments; the boundary is Cloudflare's".

**Cloudflare is the one platform where tenant-per-deployment scales down to a free tier.** Verified facts (details and sources in the cost appendix):

- The first capacity target is 10,000 project environments per account. The documented D1 database limit is 50,000 per account, but the design does not claim that a single account can serve millions without measured provider validation.
- Workers for Platforms removes per-account script limits; user Workers are isolated by default.
- Workers and D1 scale to zero; an empty D1 database occupies roughly 12 KB; hibernating Durable Objects accrue no duration charge.
- The only tenancy-count-driven costs are the Workers for Platforms base fee and $0.02 per script per month beyond the included thousand. An idle tenant rounds to two cents a month.

**Operational wins.** Offboarding becomes "delete the Worker and database" instead of the carefully bounded `${tenantId}_` range purge. Blast radius (bad migration, corrupted store, stuck object) is one customer. Canary rollouts happen per tenant. Per-tenant data residency becomes possible with Durable Object jurisdictions and D1 location hints. The D1 10 GB per-database cap becomes a natural per-tenant quota instead of a shared ceiling.

### What we give up, and the replacing controls

**Defense in depth against mis-wiring.** Today, if two customers were somehow pointed at one database, tenant predicates would still separate their rows. After removal, a provisioning bug that binds tenant B's Worker to tenant A's database is a full cross-tenant breach. The provisioner becomes the security-critical component. Controls:

- **Deployment identity and caller attestation.** Every deployment receives its tenant identifier as an environment binding and a distinct internal credential. A strict singleton sentinel in D1 asserts database ownership, while Worker-to-Durable-Object requests authenticate the calling deployment before the target reads storage. These invariants fail closed on database and cross-script namespace drift, and they keep audit tags honest because attribution comes from infrastructure configuration rather than a forgeable request claim.
- **Fleet drift audit.** A control-plane job enumerates user Workers and verifies the tenant-to-database-to-namespace mapping is one-to-one, continuously.
- Net effect: the isolation trusted computing base shrinks from "every query on every surface forever" to a small, rarely-changing, reviewable provisioning path. Residual confidentiality risk converts into availability/operations work, which is the right direction for a security product.

**Fleet operations become a first-class product concern.** Schema migrations must loop across every tenant database with per-tenant version state; rollouts need canary tenants; secrets and bindings are managed per user Worker through the API. This is deterministic engineering, but it is not optional: without it the foundation claim is hollow. The delivery plan makes it an explicit slice.

**Cross-tenant queries become pipelines.** Fleet-wide analytics, abuse detection, and "which tenants have stuck approvals" require aggregation (the audit export queue already provides the spine) instead of one SQL query.

### The bet and its tripwire

This bets that **pooled tenancy never returns**. Re-adding it later is a redesign, not a patch: retrofitting tenant predicates into code that assumed one tenant is exactly how cross-tenant bugs are born. The first capacity gate is 10,000 project environments per account. Higher targets require measured API, D1, queue, and operational validation rather than extrapolation from adjustable limits. The tripwire that forces a revisit is a product that needs cross-tenant *shared data structures*, such as a shared marketplace or cross-organization collaboration on one record. Agent runtimes for customer organizations do not have that shape; a shared workspace is its own deployment.

One boundary against over-rotating: "maximum clean" applies to the data plane. The control plane (signup, billing, provisioning, aggregated audit) is inherently multi-tenant and always will be. Keep it thin; do not pretend it away.

### Rejected alternatives

- **Keep both modes (single- and multi-tenant).** Correct answer while consumers existed; rejected now because dual-mode doubles the test matrix and documentation surface, keeps the recurring tenant tax on every new feature, and invites accidental pooled deployments. Recorded as the previous recommendation so a future session does not re-litigate it without the "no consumers" premise changing.
- **One Worker with thousands of D1 bindings, routed per request.** Approximately 5,000 bindings per script make it technically possible. Rejected: request-time binding selection reinstates logical routing as the isolation boundary, which is the exact failure mode this proposal eliminates.
- **Account-per-tenant.** The hardest boundary Cloudflare offers (separates even account-level limits), but operationally heavy (billing, tokens, dashboards per account). Reserve for a future regulated-customer tier; not the default.
- **Freeze multi-tenant code but keep it shipped.** Frozen code still costs test matrix, docs, and invariant discipline, and its presence invites pooled deployments. Delete instead; the design record survives in git history, the threat model, and this document.

## What tenancy removal deletes, and what stays

A tenant was never one user. The removal targets *tenant* machinery only; every *user*-facing control inside a deployment stays. Verify each anchor before editing (read-before-edit; paths are as of the baseline commit).

Slice A deleted from Flowsafe:

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

Slice A added:

- Deployment identity: environment-configured tenant tag, strict D1 sentinel check at startup, authenticated Worker-to-Durable-Object calls, and fail-closed mismatch handling; audit events carry the tag from configuration.
- Deployment-local resource ownership: runs, threads, resources, and schedules remain authorized per human or automated principal, with `404` before role errors.

## Deployment topology

### What Workers for Platforms is

[Workers for Platforms](https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/) is Cloudflare's product for running large fleets of Workers under one account, designed for platforms that deploy code on behalf of their customers. Components:

- **Dispatch namespace.** A container for user Workers. [Workers for Platforms limits](https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/reference/limits/) document unlimited scripts and no Durable Object namespace count limit. Every fresh external deployment contains a stable platform-authored state script and content-addressed candidate scripts in this namespace.
- **Dynamic dispatch Worker.** A router we own. It receives every request, resolves the project from the hostname, and invokes the candidate through `env.DISPATCHER.get(name)`. Requests across the dispatch, user, and outbound chain bill as one request.
- **User Workers.** The candidate receives its D1 binding, exact remote Durable Object bindings to the state script, canonical application variables and secret names, fleet-owned application R2 bindings, and no privileged service, queue, or maintenance credential. The trusted invocation seam verifies each secret value against its descriptor before upload. The platform-authored state script owns Durable Object namespaces, the audit queue producer, the maintenance secret, and one named service binding to the shared outbound Worker.
- **Outbound Worker.** The one shared platform outbound Worker intercepts candidate `fetch()` through the namespace outbound contract and exposes a separate named `StateEgress` entrypoint for trusted state. The state adapter overwrites reserved context headers. `StateEgress` verifies the exact context and credential digest from the canonical host record before enforcing the host policy.
- **Isolation modes.** Namespaces default to "untrusted" (isolated caches, no `request.cf`). Under the Super platform target, customer-built artifacts make untrusted mode mandatory, not merely a defensible default.
- **Custom limits.** Per-script CPU caps let the platform bound a runaway tenant — a denial-of-wallet control that plain Workers do not offer per-Worker.

### Committed topology: two stages, one security boundary

**Stage one (pre-launch): plain Workers, one per tenant deployment.** Every artifact is team-authored, so trusted execution on the $5/month Workers Paid plan is safe and sufficient: one normal Worker per deployment via a control-plane loop with generated configuration, each with its own D1/DO/queue-producer bindings, routed by hostname. A generated guarded entry module rejects version-override requests on the customer hostname before application code runs. It limits the exact control hostname to authenticated maintenance endpoints and rejects every unknown hostname, including unconfigured `workers.dev` hosts.

Stage one has no dispatch hop, platform subscription, or per-script fee.

**Stage two (from the first external artifact): Workers for Platforms untrusted dispatch namespaces.** The trigger is not scale. It is the first time a customer-built artifact must run. The platform requires untrusted-mode isolation, per-script CPU caps, shared outbound enforcement beneath breakwater's `runtime.fetch`, and Upload API deploys for customer-initiated deployment frequency. Design partners and beta users count as external because artifact authorship defines the boundary.

Fresh stage-two deployments consume zero ordinary Workers per project environment. Their stable state and candidate scripts both use the Workers for Platforms namespace. The account retains ordinary Workers only for the shared dispatcher, outbound, and audit-consumer plane, plus temporary adopted bridges during a legacy switch rollback window.

Existing stage-one deployments switch one at a time without moving D1 data, Durable Object namespaces, or application R2 buckets. Before upload, the control plane persists the complete same-name bridge mutation identity, including its artifact digest, append-only Durable Object history, prior and target migration tags, and expected secret names. An authorized retry inspects the live Worker, derives the remaining migration suffix from its authoritative tag, and converges exact metadata and secrets. The composite bridge preserves the prior application module graph and bindings, then appends the trusted state exports. The control plane maintenance-arms a content-addressed external candidate with the target application's bindings, publishes and verifies the complete serialized host record, detaches the custom domain, verifies dispatch traffic, and makes the bridge private. Rollback reverses traffic first, drains dispatch, and restores the supplied prior application and secret values while retaining append-only Durable Object exports. Finalization replaces the bridge's fetch surface with state-only code under the same script and namespace ownership and removes its application bindings.

The packages already satisfy the untrusted-namespace constraints at zero cost: nothing in `packages/` touches `request.cf` or the Cache API (verified at the baseline commit), and after the alarm redesign nothing depends on platform triggers.

Verified constraints on the Workers Paid plan for stage one ([limits](https://developers.cloudflare.com/workers/platform/limits/)), after the maintenance redesign below:

| Constraint | Value | Consequence |
| --- | --- | --- |
| Workers per account | 500 | Ample for a pre-launch fleet of team-authored deployments; irrelevant after the switch |
| Cron Triggers per account | 250 | **Removed as a constraint** by the alarm redesign (previously the binding limit: two crons per deployment capped the fleet near 125 tenants) |
| Environment variables per Worker | 128 | Fine per tenant |
| Worker size | 10 MB | Fine |
| No per-script fee | — | Tenant count itself costs nothing on plain Workers |

Application KV is not part of this topology. [Workers KV limits](https://developers.cloudflare.com/kv/platform/limits/) cap an account at 1,000 namespaces, one tenth of the first capacity target. Sharing a namespace would restore key-prefix correctness as the tenant-isolation boundary. The platform's single `HOSTS` namespace remains a trusted routing index. Application object storage instead uses fleet-owned R2 buckets with immutable per-deployment mappings.

### Agents per deployment

For platform-authored work, the deployment unit is the client, not the agent. A client's Worker hosts that client's entire agent catalog: `createAgentModuleCatalog()` (`packages/flowsafe/src/agent-host/catalog.ts`) takes a plural module list and indexes it by id, the thread host resolves `agentId` per request and applies that agent's own `allowedRoles`/`allowedAutomation`/`requiredPermissions`, and workflows register the same way (the baseline Worker's `WORKFLOWS` array). Adding an agent to a client is a catalog entry plus a redeploy of that client's Worker — no new Worker, database, or namespaces. Durable runs of every agent are instances of the same Durable Object classes, and instances are unlimited.

The 500-Workers ceiling therefore bounds **client count, not agent count** (and on Workers for Platforms even that bound disappears). The practical per-Worker bounds on catalog size are generous: the 10 MB script limit (packages and Mastra are shared; per-agent cost is instructions, schemas, and any unique connector code), the 1-second startup limit (the thread host already constructs the catalog lazily), and the 128-secret budget for per-connector credentials.

**Customer-deployed projects use a finer unit: one user Worker per project × environment.** The Super platform contract requires immutable builds, independent promotion, and rollback per project, which a shared mutable catalog Worker cannot provide — redeploying a client-wide catalog Worker to update one project would violate the others' version pins. Workers for Platforms does not expose user-script version selection, so each external specification becomes a distinct content-addressed script. Promotion and rollback publish a new hostname lookup while retaining the prior script through KV propagation and the rollback window. Script counts then scale with projects × environments × retained releases, which only the platform topology supports (unlimited scripts at $0.02/month each past the included thousand).

External candidates do not own Durable Object classes or physical R2 identities. Their Durable Object bindings name a stable platform-authored state script, which owns FlowSafe runs, approvals, alarms, and connector egress adaptation across project release flips. Their application R2 descriptors resolve to fleet-owned deployment buckets shared only across that deployment's retained releases. `AUDIT_PROXY` is a remote Durable Object binding to the exact `FlowsafeFleetAuditProxy` class and fixed singleton. Fresh state adds the dispatch namespace to that binding; an adopted ordinary bridge omits it. External candidates share the deployment's D1 database, so migrations applied during the rollback window must be explicitly marked rollback-compatible and use expand-only changes. Platform-authored client catalogs keep a stable script name and own their Durable Object migration sequence. Both units are provisioning profiles over the same packages, but their release mechanics differ.

Two intra-client consequences are deliberate design points, not defects:

- **Shared version, shared blast radius.** All of a client's agents deploy atomically; a bad deploy affects all of them, and only them. A client wanting isolation between agent tiers (production versus experimental) shards into two deployments — deployment unit = client × environment — spending one extra Worker from the budget.
- **Shared stores, shared budgets by default.** One D1 per client means its agents share idempotency and rate-limit stores. Rate budgets key per connector plus scope, and the opaque isolation scope with `crossWorkflowIsolation` remains available to partition budgets per workflow or agent when one noisy agent must not starve the client's others — an in-deployment policy concern, exactly where per-principal authorization already lives.

### Maintenance on Durable Object alarms, not cron triggers

The baseline deployment declared two cron expressions per deployment for the SLA sweep and retention purge. `deploy/crons.ts` documented why they never shared an invocation: a CPU-limit kill is uncatchable, so a slow sweep sharing an invocation could permanently starve the purge. The alarm implementation preserves that failure-isolation property.

The shipped design uses one singleton maintenance Durable Object per deployment. It stores `nextSweepAt` and `nextPurgeAt`, then holds one alarm at the earliest due time.

- **One due task per alarm invocation.** The handler re-arms first (persist the next alarm before doing work), then runs exactly one due task; if the other task is also due it sets an immediate follow-up alarm instead of running both. This reproduces the cron contract's guarantee — a CPU kill during the sweep cannot take the purge down with it — and Durable Object single-threading additionally guarantees the two tasks never run concurrently, which the `*/15` versus `7 * * * *` minute offsets only made likely.
- **Re-arm-first + built-in retry.** Alarms retry automatically on exception; persisting the schedule before executing work means a crash mid-task cannot break the chain.
- **Bootstrap and watchdog.** Provisioning calls an authenticated `ensure-maintenance` admin endpoint once after deploy; the control-plane drift audit also polls health and re-arms stale maintenance. An external candidate receives only an operation-, tenant-, physical-script-, specification-, expiry-, and nonce-bound capability. The trusted state Worker consumes mutation nonces and signs the exact result; it never accepts the candidate's deployment-identity credential for maintenance. No path depends on platform triggers.
- **Cost.** Sweep every 15 minutes plus hourly purge ≈ 3,600 alarm invocations per deployment per month, each an included-tier Durable Object request plus one row written per `setAlarm`. At 200 tenants that is ~0.72 million DO requests and rows written per month — roughly $0.11 beyond the included tier at the margin; negligible at any scale this proposal contemplates.
- **Deletions.** Tenant Workers have no `scheduled()` or `queue()` handler, cron trigger, or queue-consumer registration. The deployment tests assert alarm-driven duties and producer-only audit bindings.

This also removes a real Workers for Platforms incompatibility rather than an assumed one: `triggers.crons` is silently dropped when deploying into a dispatch namespace (see "Resolved platform questions"). With no cron dependency anywhere, the fleet is trigger-portable by construction.

### The launch switch: scheduled, not contingent

Under the Super platform target the move to Workers for Platforms is a **scheduled launch step**, not a contingency. Stage one exists to avoid the $25/month subscription while every artifact is team-authored; the switch date is pinned to the first external artifact, not to any scale threshold.

**Why the deferral is safe.** The stage-one fleet holds five portability invariants (all adopted by this proposal), which make the switch a control-plane change with zero data-plane migration:

1. **No platform triggers.** No `scheduled()` handler, no cron expressions; all timing lives in Durable Object alarms inside the deployment (the alarm redesign above).
2. **Tenant Workers never consume queues, and external Workers never produce directly.** Platform-authored Workers may hold the shared producer. External Workers call a private trusted-state audit proxy, which overwrites attribution from static bindings before enqueueing. The shared audit queue's only consumer lives in the control plane. The accepted baseline consumed the audit queue in each tenant Worker, but dispatch-namespace scripts cannot be queue-consumer targets.
3. **Host-based routing only.** Tenant resolution comes from the hostname, so a dispatch Worker can replicate routing with a lookup table; nothing depends on per-Worker routes or `workers.dev` URLs.
4. **Provisioning behind an interface.** The control plane deploys tenants through a `ProvisioningBackend` seam (create/update script, attach bindings, run migrations). The wrangler-loop backend and the Workers for Platforms Upload-API backend are two implementations of the same interface.
5. **No `request.cf`, no Cache API.** Untrusted namespaces disable both; the packages use neither today (verified at the baseline commit), and CI should keep it that way.

**What changes at switch time.** The package-level handler and binding contracts remain the same. Durable state needs **no data migration**: FlowSafe persists runs, approvals, schedules, and memory in each deployment's D1 database, platform-owned Durable Object namespaces, and fleet-owned application R2 buckets. The switch adds a dispatch Worker (hostname → physical script → `env.DISPATCHER.get(name).fetch(request)`), the Upload-API `ProvisioningBackend`, content-addressed external release scripts, a stable platform state Worker for their Durable Object bindings, host-lookup publication, and the outbound Worker for infrastructure-level egress interception. Existing team-authored deployments migrate one at a time: preserve the prior bindings on the bridge, deploy the target bindings on the candidate, verify through the maintenance-only dispatcher path, publish the hostname, then retire the plain application's fetch surface after the rollback window. R2 physical mappings do not change.

**Switch timeline and budget.** Buy the subscription roughly one integration month before the first external artifact, not at marketing launch. The pre-purchase window can use local simulation where wrangler supports dispatch-namespace bindings in dev, but the conformance items below need a real namespace. Total pre-launch platform spend is therefore $5/month, plus one to two months of $25 for the integration window.

Conformance items to prove on the real namespace before any external artifact runs:

- Durable Object class bindings and migrations via the Upload API metadata.
- WebSocket upgrades traversing the dispatch hop (FlowSafe's approval hub and live streams are WebSocket-based).
- Custom CPU limits enforced per user Worker, with the platform's chosen defaults.
- Outbound-Worker interception composing correctly under breakwater's `runtime.fetch` (egress denials still attribute to the connector manifest).
- The full FlowSafe spike/e2e suite passing against a namespaced deployment.

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

Conclusions: Durable Object duration and D1 row writes are the meters to engineer against (hibernate aggressively; batch writes; keep audit events lean). The plain-vs-platforms delta is small and operational, not financial. Tenancy count is financially irrelevant below thousands of tenants. Pre-launch platform spend under the two-stage topology is $5/month, rising to $25/month plus usage only from the switch integration window onward.

## Delivery plan

Ordered slices; each is a reviewable unit with its own verification. Quality-gate review lanes apply to every slice.

**Slice A — tenancy removal + deployment identity (implemented 2026-08-10).**
Removed the deleted-list mechanisms from Flowsafe; re-keyed stores without tenant components; introduced the deployment-identity binding, strict sentinel check, Worker-to-Durable-Object credential, and deployment-local resource-owner registry; rewrote the threat model's tenant sections as a provisioning-boundary section; updated the roadmap, operations runbook, deployment reference, and package READMEs; added a breaking Flowsafe changeset. Breakwater remained unchanged and therefore has no changeset in this slice.
*Verification:* `pnpm lint && pnpm typecheck && pnpm test && pnpm build && pnpm docs:check`, `pnpm --filter @proofoftech/flowsafe spike:verify` (spike scenarios rewritten from cross-tenant forgery to sentinel-mismatch fail-closed), pack tests.

**Slice B: maintenance onto Durable Object alarms (implemented 2026-08-10).** `createFlowsafeMaintenanceDurableObject()` owns sweep, purge, and optional schedule-tick timing. The authenticated `/admin/ensure-maintenance` and `/admin/maintenance-status` routes address the fixed `deployment-maintenance` instance. Platform-authored tenant Workers retain audit queue producers; external candidates use the trusted state audit proxy. Neither exposes a cron or queue-consumer handler.
*Verification:* the production Wrangler harness proves re-arm-first ordering, one duty per invocation, deterministic ties, failure isolation, durable health, and alarm recovery. FlowSafe, agent-starter, and showcase configurations contain no cron triggers.

**Slice C: durable-run-protocol benchmark (implemented 2026-08-10).** The benchmark in `packages/flowsafe/spike/durability-benchmark.worker.ts` hand-maps one approval-gated graph onto FlowSafe and Cloudflare Workflows. [The recorded decision](../../packages/flowsafe/spike/durability-authority-decision.md) selects FlowSafe as the sole production authority and keeps Cloudflare Workflows benchmark-only.
*Verification:* the local Wrangler and workerd benchmark kills and restarts three runtime generations. It covers process loss before and after approval, duplicate delivery and resume, cross-run identifiers, D1 contention, exact grant reconstruction, and one external effect.

**Slice D: single-tenant secure preset (implemented 2026-08-10).** `singleTenantConnectorPolicies()` requires D1-backed durability, production audit export or an explicit development opt-out, organization egress policy, permission wiring, and background execution policy. Construction rejects tenant-isolation scope, in-memory stores, contradictory audit settings, weakened destructive approval, and manifests that exceed the organization allowlist.

**Slice E: provisioning control plane (implemented 2026-08-11; quota coordination corrected 2026-08-12).** The private `anchorage-fleet-control` package implements `WranglerLoopBackend` and `WorkersForPlatformsBackend` behind `ProvisioningBackend`. Shared code owns renewable deployment leases that fence each provider write, immutable mappings, staged ordinary Worker Versions, D1 migration ledgers with canary ordering, and content-addressed external releases. A single D1 batch atomically commits the fleet row with exact ordinary-Worker, dispatch-script, and application-R2 claims, then fails the transaction if the lease is no longer live. It canonicalizes application variables, opaque secret descriptors, and R2 descriptors into release identity while keeping plaintext secret values at the trusted invocation seam. Fleet-derived R2 mappings remain immutable deployment identity. External migration records the candidate, release-owned application topology, trusted state artifact, Durable Object history and namespace snapshot, complete D1 history, schema, outbound policy, backend-owned audit queue, state-egress credential digest, and immutable maintenance verifier before mutation. It persists recovery subphases through route publication. Platform-only migrations reuse the active release without inventing a rollback entry. Fresh external deployments place stable trusted state in the dispatch namespace and use the one shared outbound Worker's named `StateEgress` entrypoint. The package also owns fleet-private Ed25519 maintenance capabilities, one-shot mutation nonces, per-state HMAC result receipts, a remote Durable Object audit proxy, bidirectional exact-binding and R2 inventory drift, per-duty maintenance watchdogs, reverse D1 and R2 attachment scans, one reusable create-authorized-through-delete-authorized per-bucket R2 state machine across every cleanup path, integrity-verified D1 decommissioning, version reports, one control-plane audit consumer, and a durable shared-D1 coordinator that caps Anchorage-originated Cloudflare API requests at 1,100 per rolling five minutes across replicas sharing an explicit nonsecret provider-quota scope. Dedicated switch, rollback, finalization, and decommission state machines adopt existing plain deployments without D1, R2, or Durable Object namespace migration. Decommission persists the exact canonical set of host targets allowed by the current durable publishing, migration, rollback, or teardown phase, effective bridge plan, and complete current release graph before it removes traffic. It then persists a traffic-removed phase, reasserts zero public ingress, and repeats exact R2 emptiness inspection before it revokes credentials or deletes a Worker. Finalized ordinary state remains under that dedicated lifecycle for later exact provisioning, release rollback, and append-only trusted-state migration; the normal Workers for Platforms backend remains dispatch-only. The verifier key and state-egress credential digest remain immutable for an existing fleet. Either rotation requires a coordinated migration. Application KV remains unsupported because the 1,000-namespace account cap cannot meet the 10,000-deployment horizon without shared storage.
*Verification:* unit, contract, and real-D1 harness tests cover concurrent lifecycle exclusion and stale-writer fencing; retry and cleanup at D1 and R2 resource boundaries; canonical ordering, secret-key and value-digest validation before provider calls, and absence of secret plaintext from durable state; digest-attested candidate validation, promotion, crash recovery, and schema-compatible rollback; stable dispatch-native state recovery; local and remote Durable Object target resolution; immutable fleet-owned R2 resolution; context-bound shared outbound enforcement; canary stop; D1 ledger atomicity and schema resumption; incremental Durable Object migrations; exact application, platform, and custom-domain bindings; independent plain-Worker inventory plus Workers for Platforms registry and orphan inventory; maintenance bootstrap and per-duty failure; shared audit consumption; attached and nonempty R2 deletion refusal; durable export integrity; backend switch crash phases; and Worker, R2, then D1 decommission ordering. `pnpm fleet-control:credentialed` is a mandatory fail-closed release gate for Workers for Platforms changes. Its version 1 operator-artifact contract requires audit and runtime limits, application variable and secret HMAC proof, candidate and state egress allow/deny requests, WebSocket nonce echo, R2 write/read/delete/absence, CPU termination and recovery, append-only trusted-state migration, same-name `keep_bindings` secret preservation, and one FlowSafe approval suspended across a release update through terminal exactly-once state and duplicate-decision and duplicate-resume rejection. The command was not run during repository implementation because no scratch-account credentials, conforming operator artifacts, or Workers for Platforms subscription were available.

The 2026-08-12 correction also requires every plain, dispatch, backend-switch, and control Worker inspection to consume the complete provider binding inventory. Unknown, malformed, duplicate, unrepresented, or omitted entries fail closed. Production quota coordination requires a direct Workers D1 binding so its storage transport cannot consume the Cloudflare Client API quota it protects. The runtime validates the binding interface; the trusted host enforces direct-binding provenance because JavaScript structural typing cannot distinguish a remote facade.

## Out of scope — must not change

- **Breakwater's tenant-agnostic `Actor` and opaque isolation scope.** Already correct for this future; do not delete `ISOLATION_SCOPE_CONTEXT_KEY` or `crossWorkflowIsolation` — they scope non-tenant concerns.
- **Per-user authorization, approval grants, reserved-context stripping, 404-before-role checks.** These protect users from each other inside one deployment; they are not tenancy code.
- **The control plane's own multi-tenancy.** Signup, billing, and provisioning inherently span tenants; keep that plane thin rather than pretending it away.
- **The Super platform planes.** The capability marketplace and gateway, provider adapters, remote MCP and SDK surfaces, build service, developer studio, and the economic plane (quote/reserve/capture/reconcile, two-ledger accounting) are Super platform components maintained outside this repository. These packages supply the execution-security slice they compose with; connector manifests are the enforcement seam for capability calls, and the external economic authority owns the money.
- **LLM/provider cost controls.** Model-token spend dominates platform spend at every scale above; it is a separate workstream.

## Resolved platform questions

Verified 2026-08-09; kept here so future sessions do not re-investigate:

- **Cron triggers do not work on dispatch-namespace user Workers.** Wrangler silently drops `triggers.crons` when deploying with `--dispatch-namespace`, and the namespace scripts API has no `/schedules` subresource ([workers-sdk #13840](https://github.com/cloudflare/workers-sdk/issues/13840)). Irrelevant to us after Slice B removes cron dependence everywhere.
- **User Workers cannot be queue consumers.** The consumer registration API does not accept namespace scripts and the Upload API's `queue` binding type attaches producers only ([workers-sdk #6758](https://github.com/cloudflare/workers-sdk/issues/6758)). Slice B's producer-only data plane matches this by design.
- **Fleet API budget.** Cloudflare documents 1,200 client API requests per five minutes per user or account token, cumulative across callers ([API limits](https://developers.cloudflare.com/fundamentals/api/reference/limits/)). Production clients use a durable coordinator over a shared direct Workers D1 binding and an explicit nonsecret quota scope to cap Anchorage-originated requests at 1,100 per rolling five minutes across replicas. The trusted host must not pass a structurally compatible REST-backed facade because its own queries would consume the Client API quota being coordinated. The remaining 100 is reserve for uncoordinated dashboard and out-of-band traffic, not a guarantee that such traffic is observable.
- **No gradual deployments or selectable deployments for user Workers.** [Workers for Platforms limits](https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/reference/limits/) states that a user-Worker change deploys all-at-once to 100 percent, and the [dispatch namespace API](https://developers.cloudflare.com/api/resources/workers_for_platforms/subresources/dispatch/) exposes no versions or deployments subresource. External releases therefore use separate content-addressed scripts and publish the dispatch lookup only after exact-candidate validation. The previous script remains available for rollback; Workers KV propagation can temporarily send requests to either compatible release.
- **Application KV cannot preserve this deployment horizon.** [Workers KV limits](https://developers.cloudflare.com/kv/platform/limits/) allow 1,000 namespaces per account. Per-deployment KV stops at one tenth of the 10,000-project-environment target, while shared application KV restores logical tenant partitioning. Fleet control therefore rejects application KV and reserves KV for the shared `HOSTS` control-plane index.

## Paid integration checks before the switch

1. Build candidate and trusted-state v1/v2 artifacts that implement the strict contract in [Fleet control](../fleet-control.md#implement-the-artifact-contract).
2. Run `pnpm fleet-control:credentialed` against a scratch account and an untrusted dispatch namespace before releasing any Workers for Platforms lifecycle change.
3. Select Durable Object jurisdictions and D1 location hints before offering a regulated-customer tier.

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
