---
"@proofoftech/flowsafe": minor
---

Replace pooled request-level tenancy with one physically isolated organization per deployment.

This breaking release requires `DEPLOYMENT_TENANT`, a matching D1 sentinel seeded before application migrations, and a per-deployment `DEPLOYMENT_IDENTITY_SECRET`. The Worker validates the strict singleton sentinel, and production Durable Objects also authenticate every Worker caller before reading storage.

`ActorContext`, `ActorResolver`, and `createActorResolver()` replace their tenant-named equivalents. Approval and subscription factories now expose `.store()`, run, thread, schedule, and subscription ids are opaque and server-minted, and live hubs, provider hosts, and background-task hosts are deployment singletons.

Tenant-branded stores, tenant-prefixed id helpers, the tenant registry, subdomain cross-check, and in-database tenant purge APIs have been removed. First-time sentinel provisioning refuses any database with pre-existing application tables; provision a fresh database for each organization.

`createFlowsafeWorker()` no longer exposes the unused `wrapResolve` hook. Authenticate and validate actors in `buildVerifier`, mount deployment-specific routes through `preRoutes`, and enforce final mutation policy through `beforeStart` or `beforeResume`.

Server-minted runs, threads, and schedules plus validated host-owned resource keys retain per-principal authorization through the deployment-local resource-owner registry. Inaccessible resources return `404` before role checks.

The package now ships `flowsafe-provision` for strict D1 sentinel provisioning. The CLI resolves a consumer-installed Wrangler `>=4 <5` optional peer, rejects other Wrangler majors, and maps preview provisioning to Wrangler's remote preview target.

Opt-in HTTP approval creation now requires write access to the named run. Those client-filed records remain decision-only; only trusted suspension-bound, run-scoped, or server-targeted approvals can resume execution.

Subscription stores now snapshot caller input before persistence and use the same JSON metadata semantics in D1 and memory. External provider resource ids reject ASCII controls and values larger than 1,024 UTF-8 bytes.
