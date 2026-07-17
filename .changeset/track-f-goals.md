---
"@proofoftech/flowsafe": minor
---

Track F (M-005) — goals. New subpath-only export `@proofoftech/flowsafe/goals`:
`createObjectiveRouter`, a role-gated + audited objective HTTP surface
(set/get/update/clear over `/api/threads/:threadId/goal`) that writes the
thread-scoped goal record in Track C's `mastra_thread_state` domain
(`GOAL_STATE_TYPE` 'goal'). The write path is a P6-lite ingestion boundary
(auth → coarse role → thread-prefix ownership 404 → size cap →
`assertNoClientMemoryIds` → field allowlist → maxRuns host cap → audit) and
persists through `@mastra/core`'s own `writeObjective`/`readObjective`/
`clearObjective`, so a record it writes is byte-identical to what the durable
goal step reads via `resolveGoalStore` (DL-018 — no thread-DO affinity needed
for the write). A requested `maxRuns` above the host cap is rejected, not
clamped (default the core `DEFAULT_GOAL_MAX_RUNS`, 50; DL-007). Goals never mint
capability (P8) and Track F starts no runs — per-tenant run budgets stay
enforced at the existing seams. `GOAL_REQUEST_CONTEXT_KEY` ('mastra:goal') is
reserved with a no-collision pin against the runtime's requestContext base keys.
Hosts mount the surface opt-in through `createFlowsafeWorker`'s new
`buildObjectiveRouter` seam; absent config is byte-identical. Additive only — no
new table, schema-guard, purge, or TTL change (reuses the Track C thread-state
domain), and no existing signature or `ApprovalRecord` shape changes.
