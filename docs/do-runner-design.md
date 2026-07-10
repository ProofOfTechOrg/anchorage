# DO Runner Design

The Durable Object runner is the Cloudflare-native equivalent of `@mastra/temporal`. It wraps Mastra workflows in Durable Objects for durable execution that survives Worker CPU time limits.

## Import-Swap Pattern

Mastra ships this pattern in `@mastra/inngest` and the experimental `@mastra/temporal`: an `init()` call returns backend-bound `createWorkflow`/`createStep`, imported in place of the `@mastra/core/workflows` versions -- the swap happens at the import, and the workflow definition code is unchanged. The DO runner follows the same pattern:

```typescript
import { z } from 'zod';
import { init } from '@proofoftech/flowsafe/do-runner';

// Import-swap: DO-bound factories in place of @mastra/core/workflows.
const { createWorkflow, createStep } = init(env);

const research = createStep({ id: 'research', inputSchema: z.object({}), outputSchema: z.object({}), execute: async ({ inputData }) => ({}) });
const generate = createStep({ id: 'generate', inputSchema: z.object({}), outputSchema: z.object({}), execute: async ({ inputData }) => ({}) });
const approve = createStep({ id: 'approve', inputSchema: z.object({}), outputSchema: z.object({}), execute: async ({ inputData }) => ({}) });

// Standard Mastra definition -- builder chain committed in place.
const myWorkflow = createWorkflow({
  id: 'my-workflow',
  inputSchema: z.object({}),
  outputSchema: z.object({}),
})
  .then(research)
  .then(generate)
  .then(approve)
  .commit();

// Execution and suspend/resume state live in a Durable Object. Definition
// code is unchanged; STARTING is not: the runtime requires a server-minted
// runId (`${tenantId}_${uuid}`, INV-1) — in the flowsafe hosts createRunRouter
// mints it and passes it in; there is no generation fallback. See "Run
// Identity and Tenant Scoping" below.
const run = await myWorkflow.createRun({ runId });
await run.start({ inputData: {} });
```

## Durable Object Lifecycle

```
1. Workflow started: DO created, initial state persisted
2. Step A: DO calls Mastra step, stores result, sets alarm for next step
3. Worker CPU limit reached: DO hibernates, state in DO storage
4. Alarm fires: DO wakes, loads state, calls next step
5. Step B (requires approval): DO sets suspense state, waits for flowsafe API
6. Reviewer approves: flowsafe API calls DO, DO resumes step execution
7. Final step completes: DO marks terminal, cleans up alarm
```

## State Schema

```
{
  workflowId: string,
  runId: string,
  status: 'running' | 'suspended' | 'completed' | 'failed',
  currentStep: string,
  stepResults: Record<string, unknown>,
  approvalRequests: ApprovalRequest[],
  createdAt: number,
  updatedAt: number,
}
```

## Run Identity and Tenant Scoping

The `runId` is the tenant carrier, not an opaque token: it is minted
server-side as `` `${tenantId}_${uuid}` `` from the authenticated tenant, and
the runner enforces that at three points.

- `RunnerRuntime.start()` **requires** a `runId`. There is deliberately no
  `?? crypto.randomUUID()` fallback: a caller that omitted one would mint a
  bare, tenant-less run whose snapshot row no tenant purge can reach and no
  actor can own.
- The DO's HTTP surface refuses a start without a `runId`, and every route
  asserts the request's `(workflowId, runId)` equals the identity the instance
  was addressed with. `ctx.id.name` is populated only for `idFromName`-created
  ids and is unforgeable at the DO boundary, so a request that disagrees with
  it routed around the name join.
- `DurableObjectRunner.tenantId` recovers the tenant from that same identity
  and **throws** when it cannot. Defaulting would hand the instance an
  unscoped grant store — a cross-tenant capability mint. The parse is safe
  because `PATH_SAFE_ID_PATTERN` excludes `:` from `workflowId` (so the first
  `:` is the join) and the tenant charset excludes `_` (so the first `_` in
  the runId is the tenant boundary). It has exactly one implementation,
  `tenantOfRunId`.

Every leg also mints two opaque requestContext scopes the trusted runtime
owns: `breakwater.workflowScope` (always) and `breakwater.isolationScope` (for
tenant-salted run ids). breakwater never parses either.

## Storage Beyond the Snapshot

Run state lives in D1, which is what lets a run survive a restart. Two things
live in the DO's own `ctx.storage`:

- **The resume ledger** — the monotonic per-`(run, step)` resume ordinal that
  the approval grant binding uses as its collision-free tie-breaker. It must be
  durable: in-memory state dies with the isolate on idle eviction (~70–140 s),
  hibernation, and code deploys, and a lost ordinal turns an already-approved
  action into a silent no-op. Losing it fails closed for *grants* — never a
  leak — but it is an availability defect, so the DO shell adopts a
  `ctx.storage`-backed ledger automatically rather than making each host
  remember to thread one.
- **Alarms** — reserved. The DO captures `state` for a future alarm-chained
  engine (`setAlarm()` + an `alarm()` handler); no alarm is scheduled today,
  and resume is driven by the approval decision rather than by polling.

## Retention and Offboarding

Two purges, deliberately different:

- `purgeExpiredWorkflowRuns(db, { ttlMs, artifactStore })` — the retention
  purge. Deletes only TERMINAL snapshot rows older than the TTL. A suspended
  run is never eligible at any age, because expiring one would kill a pending
  approval. Hosts that store artifacts in R2 must pass the same
  `artifactStore` here as to `purgeTenant`: the snapshot row is the only
  enumerable record of a run's artifact keys (R2 keys lead with `workflowId`,
  so there is no run-level listing), and each run's artifacts are deleted
  BEFORE its row so a crash between the two retries instead of stranding them.
  The paired path is `limit`-batched per firing (default 100 runs — each run
  costs ~2+N subrequests, so an unbounded backlog would blow the Workers
  per-invocation cap); the shrinking eligible set is the cursor, and per-run
  failures are isolated (a wedged run keeps its row as its own retry cursor
  while the pass continues, then the failures re-throw aggregated so the
  cron's error surface fires).
- `purgeTenant(db, { tenantId, artifactStore })` — complete offboarding.
  Deletes snapshot rows of *any* status via an INV-3 range predicate over
  `run_id`, the tenant's approval records, and its R2 artifacts (enumerated
  from the *surviving* snapshot rows — hence the retention pairing above).
  This is the only way an abandoned-at-a-gate run is ever reclaimed. It races
  a live resume, so purge only tenants whose tokens have already expired; the
  resume then fails against the vanished row without re-executing the
  workflow (pinned by regression test).

Both purges treat a missing snapshot table as empty: Mastra creates it lazily
with the first persisted run, and a tenant (or a whole fresh deployment) that
never started one must still offboard cleanly — approvals are reaped even
when there are no snapshots.

## Error Handling

- Step execution failure: DO retries per workflow retry policy, marks step failed after exhausting retries
- DO storage failure: Cloudflare auto-handles via DO storage replication
- State corruption: DO can be reset to last known good state via admin API

## Concurrency and D1 Consistency

Run state is persisted with plain, unconditional writes — last-write-wins, no
optimistic-concurrency check in the storage layer. That is safe **only** because
every run has a single writer:

- One Durable Object instance per run (`idFromName(workflowId:runId)`) serializes
  all start/resume calls for that run across the whole fleet.
- Within the instance, the `RunnerRuntime` FIFO run-lock (`#withRunLock`, keyed
  `workflowId:runId`) serializes concurrent calls. The in-process lock granularity
  deliberately matches the cross-instance routing granularity, so two writers never
  race on one run's rows.

Invariant — this single-writer-per-run guarantee is load-bearing, not incidental.
Any future path that lets more than one writer touch a run's state (multi-instance
fan-in, or an external durable resume that bypasses the owning DO) must add
optimistic concurrency itself — e.g. a `version`/etag column with a conditional
`UPDATE … WHERE version = ?` — because the current storage layer performs no atomic
compare-and-swap.

## Cloudflare Workers Runtime Constraints

The Workers runtime differs from Node.js in ways that shape the DO runner design:
no `new Function()` (so tool validation stays pure-Zod rather than JSON Schema);
no Node built-ins such as `@mastra/pg` (D1 is the storage path); a bundle-size
ceiling on the paid tier; and no code at module load time, since env bindings are
undefined until a request/handler runs (so all initialization happens inside
`init(env)`).

Design implications for the DO runner:

- Pure-Zod schemas only; no JSON Schema tools until #17301 is fixed
- D1 for storage, never `@mastra/pg`
- Track bundle size against the 10MB Workers limit in CI
- All Mastra initialization inside `init(env)` / request handlers, nothing at
  module scope -- this constraint is why `init()` takes `env`
