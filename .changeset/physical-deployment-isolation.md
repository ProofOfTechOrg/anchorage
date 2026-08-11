---
"@proofoftech/flowsafe": minor
---

Replace pooled request-level tenancy with one physically isolated organization per deployment.

This breaking release requires `DEPLOYMENT_TENANT`, a matching D1 sentinel seeded before application migrations, and a per-deployment `DEPLOYMENT_IDENTITY_SECRET`. The Worker validates the strict singleton sentinel, and production Durable Objects also authenticate every Worker caller before reading storage.

`ActorContext`, `ActorResolver`, and `createActorResolver()` replace their tenant-named equivalents. Approval and subscription factories now expose `.store()`, run, thread, schedule, and subscription ids are opaque and server-minted, and live hubs, provider hosts, and background-task hosts are deployment singletons.

Tenant-branded stores, tenant-prefixed id helpers, the tenant registry, subdomain cross-check, and in-database tenant purge APIs have been removed. First-time sentinel provisioning refuses any database with pre-existing application tables; provision a fresh database for each organization.

`createFlowsafeWorker()` no longer exposes the unused `wrapResolve` hook. Authenticate and validate actors in `buildVerifier`, mount deployment-specific routes through `preRoutes`, and enforce final mutation policy through `beforeStart` or `beforeResume`.

`createFlowsafeWorker()` now delegates sweep, purge, and optional schedule-tick duties to `createFlowsafeMaintenanceDurableObject()`. Tenant Workers no longer export `scheduled()` or `queue()` handlers and contain no cron triggers or queue consumers. Provisioning must set a distinct `MAINTENANCE_ADMIN_SECRET`, call `POST /admin/ensure-maintenance`, and monitor `GET /admin/maintenance-status`.

The maintenance singleton persists and re-arms its next alarm before running one due duty. A failed or terminated duty cannot break the alarm chain, starve another due duty, or update the last-success timestamp.

Externally authored fleet releases no longer receive the reusable maintenance administrator secret or a shared audit Queue producer. They relay operation-bound, short-lived maintenance capabilities to trusted state and require signed results, and send untrusted audit events through an authenticated trusted-state proxy that supplies canonical infrastructure attribution.

Server-minted runs, threads, and schedules plus validated host-owned resource keys retain per-principal authorization through the deployment-local resource-owner registry. Inaccessible resources return `404` before role checks.

The package now ships `flowsafe-provision` for strict D1 sentinel provisioning. The CLI resolves a consumer-installed Wrangler `>=4 <5` optional peer, rejects other Wrangler majors, and maps preview provisioning to Wrangler's remote preview target.

The deployment-identity protocol is exported as `@proofoftech/flowsafe/deployment-identity-protocol` for trusted fleet provisioners that must apply the same sentinel rules through another Cloudflare API client.

Opt-in HTTP approval creation now requires write access to the named run. Those client-filed records remain decision-only; only trusted suspension-bound, run-scoped, or server-targeted approvals can resume execution.

Subscription stores now snapshot caller input before persistence and use the same JSON metadata semantics in D1 and memory. External provider resource ids reject ASCII controls and values larger than 1,024 UTF-8 bytes.
