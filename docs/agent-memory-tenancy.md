# Agent-Memory Tenancy

**Status: COMPLETE (2026-07-16, Track 0).** Both halves are built. The
library-side chokepoints (2026-07-12): `do-runner/memory-id.ts`
(mint/decode/ownership), `TenantContext.newThreadId()/newResourceId()/
ownsMemoryId()`, `purgeTenant` coverage of the three memory tables, and the
schema-guard column pins. The obligations they were built for (2026-07-16,
items 5–7 below): the host boundary
(`host-kit/memory-boundary.ts`), the agent-level recall-path proof
(`do-runner/memory-recall-tenancy.test.ts`), and the thread TTL
(`purgeExpiredThreads` on the purge cron). Enabling Mastra agent memory
around these chokepoints (client-chosen ids, unsalted writes) is a
cross-tenant leak by default, not a degraded mode.

## The hazard, precisely

Mastra agent memory persists conversation state in tables keyed by ids that
INV-1 does **not** salt. `createD1Storage` eagerly creates six tables, and
the multi-tenant invariants cover exactly one of them:

| Table | Key | Covered today? |
| ----- | --- | -------------- |
| `mastra_workflow_snapshot` | `run_id` | Yes — INV-1: runIds are server-minted `${tenantId}_${uuid}`, so rows are tenant-disjoint and `purgeTenant`'s range delete is exact |
| `mastra_threads` | `id` (threadId), `resourceId` | No — empty today; agent memory writes it |
| `mastra_messages` | `id`, `thread_id` | No — empty today; agent memory writes it |
| `mastra_resources` | `id` (resourceId) | No — empty today; agent-memory working memory writes it |
| `mastra_scorers` | scorer run ids | No — empty today |
| `mastra_background_tasks` | task ids | No — empty today |

In Mastra's memory API, `threadId` and `resourceId` are **caller-chosen
business identity** (a user id, an email, a conversation slug). Two tenants
naming the same `resourceId` — trivially likely when hosts derive it from
user identity — would read and write the *same* memory: semantic recall
would retrieve tenant A's messages into tenant B's agent context. That is a
fail-open cross-tenant leak on both the write and the read path. Before this
design shipped, no chokepoint intercepted it: the flowsafe surface only
persisted workflow snapshots, so none of INV-1/2/3 ever saw a thread or
resource id.

The standing tripwire is the table-inventory pin in
`packages/flowsafe/src/do-runner/mastra-schema-guard.test.ts`: it fails CI
when a `@mastra/core` bump changes the persistence inventory (a seventh
table, a rename), forcing a re-read of this design. It **cannot** detect a
feature that writes the memory tables around the chokepoints — that
protection is doctrinal (this doc, root `CLAUDE.md`, and review).

## Design: extend INV-1 to memory ids

One sentence: **salt memory ids at mint exactly like runIds, validate them
at the boundary, and purge them by the same range predicate.** No new
tenancy dimension, no flag, no post-read filtering.

### Identity shape (implemented)

- `threadId` is server-minted `${tenantId}_${uuid}` — the INV-1 shape,
  minted from the AUTHENTICATED tenant (never accepted from a client body,
  mirroring `createRunRouter`'s 400 on client runIds). Constructor:
  `mintThreadId(tenantId, mintUuid?)` in
  `packages/flowsafe/src/do-runner/memory-id.ts`, or
  `TenantContext.newThreadId()` in request scope.
- `resourceId` is `${tenantId}_${resourceKey}` where `resourceKey` is the
  host's business identity for the memory owner (user id, lead id),
  validated against the do-runner's exported `PATH_SAFE_ID_PATTERN`.
  Constructor: `mintResourceId(tenantId, resourceKey)` /
  `TenantContext.newResourceId(resourceKey)`.
- The INV-3 charset argument transfers verbatim and must not be re-derived:
  `TENANT_ID_PATTERN = /^[a-z0-9]{3,32}$/` excludes `_` (0x5F) and backtick
  (0x60), so the first `_` in any salted id is unambiguously the tenant
  separator — prefix decode is exact (`tenantOfMemoryId` delegates to
  `tenantOfRunId`, the ONE decode), and the `[`${tenantId}_`,
  `${tenantId}\x60`)` range predicate `purgeTenant` uses for `run_id` is
  exact over thread/resource/message keys too. Message ids themselves stay
  unsalted by design: every purge and scoped query rides the salted
  `thread_id`/`resourceId`.

### Chokepoints (one per resource class, INV-2 style — implemented)

- **Minting** lives in one module (`do-runner/memory-id.ts`), exporting
  `mintThreadId`/`mintResourceId` + `tenantOfMemoryId` +
  `tenantOwnsMemoryId`. Both mints re-refuse non-INV-3 and reserved
  (`system`) tenants. Hosts and the runtime import it; nothing else
  constructs memory ids.
- **Ownership assertion** on every memory read/write path: derive the tenant
  from the authenticated actor (`TenantResolver` output, never the payload)
  and require `threadId`/`resourceId` to carry that prefix
  (`TenantContext.ownsMemoryId` / `tenantOwnsMemoryId`). This is the memory
  analogue of the run-router's INV-1 ownership check, and 404 (not 403) on
  foreign ids — no existence oracle.
- **Runtime propagation** rides the seams that already exist: the DO runner
  mints ids inside the trusted computing base and passes them through
  requestContext (trust boundary 6 — never populated from client input,
  model output, or tool results). `ISOLATION_SCOPE_CONTEXT_KEY` already
  carries the opaque tenant scope per leg; memory id minting slots into the
  same `requestContextForRun` seam the grant provider uses.
- **Offboarding**: `purgeTenant`
  (`packages/flowsafe/src/do-runner/d1-storage.ts`) range-deletes
  `mastra_messages.thread_id`, `mastra_threads.id`, and
  `mastra_resources.id` (children first) with the identical `[lower, upper)`
  bounds it computes for `run_id`, reporting
  `PurgeTenantResult.{threads,messages,resources}`. A missing table reads as
  zero rows (same posture as the snapshot table), so memory-less deployments
  purge unchanged.

### Shipped 2026-07-12 (library chokepoints)

1. ✅ Mint/validate module + decoders (`memory-id.ts`, exported from
   `@proofoftech/flowsafe/do-runner` and the root barrel) with exhaustive
   tests (`memory-id.test.ts`).
2. ✅ `TenantContext.newThreadId()/newResourceId()/ownsMemoryId()` — minted
   over the same uuid seam as `newRunId` (`tenant-context.test.ts`).
3. ✅ `purgeTenant` coverage for the three memory tables + result counters,
   incl. the missing-table posture (`d1-storage.test.ts`).
4. ✅ Schema-guard extension: column pins for `mastra_threads.id`/
   `.resourceId`, `mastra_messages.id`/`.thread_id`/`.resourceId`,
   `mastra_resources.id`, plus the two-tenant same-key adversarial case:
   disjoint rows, purge exactly one tenant, survivor readable
   (`mastra-schema-guard.test.ts`).

### Shipped 2026-07-16 (Track 0 — the obligations)

5. ✅ Host boundary: `host-kit/memory-boundary.ts` —
   `assertNoClientMemoryIds(body)` 400s any body naming a
   `TCB_ONLY_MEMORY_FIELDS` member (`threadId`/`resourceId`, the
   `TCB_ONLY_CREATE_FIELDS` doctrine applied to memory: a client that picks
   its own ids picks whose memory it reads), and `requireOwnedMemoryId(tenant,
   id)` 404s a foreign id on read paths — no existence oracle. Every
   memory-touching route calls both; ids are minted server-side via
   `TenantContext.newThreadId()/newResourceId()`. It lives in host-kit, not
   memory-id.ts, because the guard's contract IS its HTTP status
   (`RunRouteError`) and `TenantContext` lives in approval-api, which already
   imports do-runner. Tests: `host-kit/memory-boundary.test.ts`.
6. ✅ Recall-path proof: `do-runner/memory-recall-tenancy.test.ts` drives core's
   own `MastraMemory` implementation (`MockMemory`) over the REAL
   `@mastra/cloudflare-d1` D1Store, with both tenants keyed by the SAME
   business key `'user-1'`, and pins all three recall surfaces an agent turn
   uses — `recall()`, `listThreads({filter:{resourceId}})`, and
   resource-scoped `getWorkingMemory()`. Disjoint rows (the schema guard's
   pin) are worthless if the recall API scopes by something else; this is what
   says it does not.
7. ✅ Thread TTL: `purgeExpiredThreads(db, { ttlMs, limit, tablePrefix })` in
   `do-runner/d1-storage.ts`, wired into `createFlowsafeWorker`'s purge cron
   behind `THREAD_RETENTION_DAYS` with its own try/catch (a wedged thread
   purge costs the snapshot purge, the approval purge, and the extra duties
   nothing). Keyed on `updatedAt`, since threads are not per-run and have no
   terminal status — time since last write is the only signal one is done.
   Messages go with their thread and BEFORE it (a message has `createdAt` but
   no `updatedAt`, so no per-message idleness signal exists and thread-first
   would strand them), enforced by a `NOT EXISTS` guard rather than statement
   order — the writer is not atomic either, so an `updatedAt`-only guard would
   delete a thread out from under a message that just landed; `mastra_resources` is
   untouched (the owner's, not the thread's — it goes at offboarding). UNSET
   by default: a conversation is meant to be kept, so there is no safe number
   to pick on an operator's behalf.

## Rejected alternatives (do not re-explore)

- **Per-tenant D1 databases.** Reverses the shipped row-level architecture
  (INV-2 decided one database + tenant-bound stores after weighing this);
  multiplies migration/ops surface; still needs id discipline for DO names
  and R2 keys, so it does not even remove the salting work.
- **Post-read filtering** (a processor/middleware that drops foreign rows).
  Fail-open: forgetting the filter on one new read path is a silent leak.
  Salting is fail-closed by construction — foreign rows cannot be addressed
  at all.
- **Trusting Mastra memory scoping options.** Mastra scopes recall by
  thread/resource id equality; with shared ids there is nothing to scope.
  Upstream adds no tenant concept (verified against `@mastra/core` 1.50.0's
  memory/storage surface — threads/messages carry no tenant column).
- **A tenancy flag** (`memoryTenancy: true`). Same doctrine as the isolation
  scope: absence of a flag means there is no default-false switch to forget;
  the salted path must be the only path.

## Out of scope (deliberately)

- Building the agents-with-memory feature itself — everything above is its
  rails, not the feature. Track A brings the first agent runs; they consume
  `memory-boundary.ts` at their routes and mint through `TenantContext`.
- `mastra_scorers` / `mastra_background_tasks` tenancy: same treatment IF a
  feature ever writes them; the inventory pin covers their appearance in
  the meantime.

## Verification

The gate is the existing one (`pnpm lint && pnpm typecheck && pnpm test &&
pnpm build`) — the schema-guard suite carries the column pins (incl.
`mastra_threads.updatedAt`, which the TTL rides), the table-inventory
structure, and the two-tenant adversarial case; `memory-recall-tenancy.test.ts`
carries the recall proof; `memory-boundary.test.ts` carries the boundary; and
`spike:verify` stays green (memory writes ride the same D1 binding the spike
exercises).
