# goals/

Track F (M-005): the goal objective HTTP surface — additive, opt-in.
Subpath-only export `@proofoftech/flowsafe/goals` (like
agent-runner/background-tasks/signals): host wiring a consumer opts into, never
the root barrel.

A goal is a durable, thread-scoped objective record (`@mastra/core`'s
`GoalObjectiveRecord`, stored in the Track C `mastra_thread_state` domain under
`type: 'goal'`) that an agent's in-loop judge reads to decide whether to keep
working. In the durable path the goal step reads the objective from D1 via
`resolveGoalStore` → `readObjective` (verified against the 1.50.0 dist:
`resolveGoalStore(mastra)` = `mastra.getStorage().getStore('threadState')`,
`readObjective` = `getState({ threadId, type: 'goal' })`), NOT an in-process
registry (DL-018) — so this surface writes the D1 domain DIRECTLY at the Worker
level and no thread-DO affinity is needed for the record write (update is consequently
a non-atomic read-modify-write and clear an unconditional delete at the Worker —
core-parity semantics, within-tenant last-write-wins under concurrency). Track F reuses
Track C's `D1ThreadStateStorage`; it adds NO table, no schema-guard/purge/TTL
change (`mastra_thread_state` is fully registered since Track C).

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `objective-routes.ts` | `createObjectiveRouter` — the role-gated + audited objective surface (set=PUT / get=GET / update=PATCH / clear=DELETE) over `/api/threads/:threadId/goal`. The write path is a P6-lite ingestion boundary (DL-006), same gate order as `createSignalRouter`: resolve → coarse role (mutations only) → thread-prefix ownership 404 → size cap → `assertNoClientMemoryIds` → field allowlist → maxRuns host cap (DL-007, over-cap REJECTED not clamped, default `DEFAULT_GOAL_MAX_RUNS` 50) → audit (`goal.objective`, accept + every post-auth denial; benign GET not audited; pre-auth 401 not audited) → persist through core's OWN `writeObjective`/`readObjective`/`clearObjective` (byte-identical record shape). Records are built to match core's `Agent.setObjective`/`updateObjectiveOptions` exactly. `GOAL_REQUEST_CONTEXT_KEY` ('mastra:goal') is mirrored (not exports-reachable), reserved for the no-collision pin, and drift-pinned against the core dist declaration. Goals NEVER touch requestContext (P8) and Track F starts NO runs (run budgets stay at the existing seams) | Changing the objective gate, its order, the audit surface, the maxRuns cap, or the record shape |
| `index.ts` | Subpath barrel (`@proofoftech/flowsafe/goals`) | Finding the goals export surface |
| `objective-routes.test.ts` | The P6-lite gate: order + each fail-closed status, the audit assertions (accept + post-auth denials, GET-accept not audited, 401 not audited), the maxRuns cap, the set/get/update/clear round-trip (byte shape, options-only update, prose-preserved), and the `GOAL_REQUEST_CONTEXT_KEY` no-collision pin against the `#requestContextFor` base keys + the core-dist drift pin | Changing the gate, the round-trip, or the no-collision pin |

## Host wiring

The worker mounts the surface opt-in through `createFlowsafeWorker`'s
`buildObjectiveRouter` seam (mirrors Track C's `buildSignalRouter`): the host
builds `createObjectiveRouter({ resolve, store, audit?, maxRunsCap? })` with the
thread-state store from its D1 domains
(`createSignalStorageDomains(binding).threadState`) and returns it closed over
the request's resolver. Absent seam ⇒ unmounted, byte-identical.
