---
"@proofoftech/flowsafe": minor
---

Track D — schedules (`@proofoftech/flowsafe/schedules`, additive, opt-in). A new
subpath ships the D1 schedules domain, a CAS-driven tick we own, and a tenant
facade — all on the single DO + D1 RunnerRuntime substrate (P1), no new
`ApprovalRecord` shape or existing signature changed, unconfigured hosts
byte-identical.

- `D1SchedulesStorage` + `createScheduleStorageDomains` — the flowsafe-owned D1
  domain over `mastra_schedules` / `mastra_schedule_triggers` (the
  `@mastra/cloudflare-d1` adapter ships neither), mirroring core's
  `SchedulesStorage` contract incl. the CAS `updateScheduleNextFire`. Composed
  into `createD1Storage` via the injected `domains` seam.
- `createScheduleTick` — we OWN the tick (DL-012): `listDueSchedules` → CAS claim
  → workflow targets mint a fresh INV-1 runId and fire through the host's
  run-start seam; agent targets are GUARDED OFF (their only public fire path,
  `schedules.run(id)`, enqueues onto core's pubsub worker loop we do not run, so
  firing is a fail-closed audited skip — agent-target execution is deferred). An
  injectable run-cap seam (DL-007) skips a capped tenant while the schedule stays
  healthy. The P4 stored-context barrier strips reserved keys before any leg.
- `createScheduleRouter` — the tenant facade (DL-013): server-minted ids,
  `metadata.tenantId` stamping, tenant-filtered reads, ownership 404s (no
  oracle), per-tenant count + fire-rate caps, and P4 reserved-key rejection on
  create/update (the whole `breakwater.` namespace + `mastra:goal`).
- Storage triad (DL-003): both tables register in the schema-guard inventory
  (8 → 10) with a new metadata-filtered `purgeTenant` kind
  (`TENANT_METADATA_PURGE_TABLES`), plus `purgeExpiredScheduleTriggers` for the
  trigger-history TTL. `createFlowsafeWorker` gains an opt-in `scheduleTick` seam
  (its own failure-isolated cron duty) and a `SCHEDULE_TRIGGER_RETENTION_DAYS`
  purge duty.
