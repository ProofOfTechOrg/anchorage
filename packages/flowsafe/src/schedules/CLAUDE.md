# schedules/

Track D (M-006): cron schedules on the DO + D1 RunnerRuntime substrate —
additive, opt-in. Subpath-only export `@proofoftech/flowsafe/schedules` (like
agent-runner / background-tasks / signals / goals): host wiring a consumer opts
into, never the root barrel.

**WE OWN THE TICK (DL-012).** Core exposes schedule execution as a CAS-claim
storage domain (`listDueSchedules` + `updateScheduleNextFire`) whose default
consumer is a pubsub worker loop. We do NOT adopt that loop (P1, one chokepoint):
a cron-triggered `scheduled()` drives `listDueSchedules` → CAS claim → fire.
Workflow targets mint a fresh INV-1 runId and fire through the host's run-start
seam (the DO topology / `RunnerRuntime.start`), inheriting INV-1, the per-leg
`requestContextForRun` derivation, and the resume ledger.

Agent targets use the optional `startAgent` seam to reach the runtime-driven
per-thread DO. The tick mints the tenant-owned run ID before the compare-and-swap
(CAS) claim, validates stored thread and resource IDs, strips reserved context
keys, and applies runtime-derived context last. Threadless targets use an
ephemeral tenant-owned topology thread without enabling memory. Hosts that omit
`startAgent` retain the audited `agent-target-unsupported` skip.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `schedules-d1.ts` | `D1SchedulesStorage` — ONE domain over BOTH `mastra_schedules` + `mastra_schedule_triggers` (mirrors core's single `SchedulesStorage` / `InMemorySchedulesStorage`, NOT the two classes the milestone named), incl. the CAS `updateScheduleNextFire` (the exactly-once claim). Tenant-AGNOSTIC (persists rows verbatim); tenancy is one layer up. INTEGER ms-epoch timestamps (core types them as `number`, unlike the ISO-TEXT signal domains) | Changing the domain, the CAS, or the DDL |
| `storage.ts` | `createScheduleStorageDomains(binding)` — packages the domain for `createD1Storage({ domains })` (do-runner cannot import schedules — it would cycle); registers under the `schedules` domain key core resolves via `getStore('schedules')` | Wiring D1-durable schedules into a host |
| `tick.ts` | `createScheduleTick` (DL-012): CAS claim, workflow and optional agent dispatch, run-cap seam, tenant memory checks, and reserved-context barrier. A lost claim reloads the row and records `concurrent-claim`, `paused`, or `disappeared`. Post-dispatch failures log `schedule-tick-bookkeeping-error` without reclassifying a successful fire | Changing dispatch, claim accounting, the cap seam, or the context barrier |
| `router.ts` | `createScheduleRouter` — the tenant facade (DL-013): server-minted `${prefix}${uuid}` ids (INV-1 posture; client ids are not honored — avoids a slugify-drift + existence oracle), `metadata.tenantId` stamping, tenant-filtered reads (post-filter, scale caveat), ownership 404s (no oracle), per-tenant COUNT + fire-RATE caps (DL-007), and the P4 reserved-key rejection on create/update (both kinds). The P6-lite gate order (resolve → role on mutations → ownership 404 → size cap → validation/caps → reserved-key → audit + persist), same as `createSignalRouter`/`createObjectiveRouter`. Persists through core's exported `computeNextFireAt`/`validateCron` + `toScheduleView` (row/view shape can't drift) | Changing the CRUD gate, the caps, or the reserved-key barrier |
| `index.ts` | Subpath barrel (`@proofoftech/flowsafe/schedules`) | The export surface |
| `schedules-d1.test.ts` | The domain: CAS win/lose (the concurrent-tick loser), listDue predicate, trigger history newest-first + limit, delete-with-history, round-trip | Changing the domain |
| `tick.test.ts` | INV-1 minting, context stripping, cap behavior, agent start present and absent, memory ownership, CAS loss classification, post-dispatch bookkeeping, exactly-once claims, and per-schedule failure isolation | Changing the tick |
| `router.test.ts` | The facade: gate order (401/403/404 no-oracle), create (server-id, tenant-stamp, target-ambiguous, reserved-key incl. `mastra:goal`, count + fire-rate caps, field allowlist), tenant-filtered list, pause/resume/delete/triggers, audit coverage | Changing the facade |

## The DL-003 storage triad (do-runner/d1-storage.ts + mastra-schema-guard.test.ts)

Both tables key on slugified ids (`agent_`/`schedule_`), NOT tenant-salted ones,
so the existing `[tid_, tid\x60)` range predicate cannot reach them. Track D adds
a SECOND offboarding KIND — `metadata-tenant`, `TENANT_METADATA_PURGE_TABLES`,
reaping via `json_extract(metadata, '$.tenantId') = ?` (the tenant the facade
stamps at create + the tick stamps on every trigger). The schema-guard inventory
grows 8 → 10 with a `metadata-tenant` coverage kind, `mastra_schedules` retention
`none` (standing config, reaped only at offboarding) and `mastra_schedule_triggers`
retention `schedule-trigger-ttl` (`purgeExpiredScheduleTriggers`, opt-in via
`SCHEDULE_TRIGGER_RETENTION_DAYS`). All three legs land in the same change (the
guard trips on a silent addition or an unwired coverage).

## Host wiring

`createFlowsafeWorker` mounts the tick opt-in through the `scheduleTick` seam +
a `crons.tick` cron (its own failure-isolated invocation, absent ⇒
byte-identical). A thread-capable host also passes `startAgent` and routes it
through `createThreadTopology`. The CRUD facade mounts opt-in through the `buildScheduleRouter`
seam (mirrors `buildSignalRouter`/`buildObjectiveRouter`; the host builds
`createScheduleRouter` closed over the request's resolver — absent ⇒ unmounted,
byte-identical). The deterministic workerd spike proves D-S1 and D-S2. The
credentialed `spike:verify:llm` command proves D-S3 through the real durable
agent loop and approval queue.
