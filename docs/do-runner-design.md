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

// Execution and suspend/resume state live in a Durable Object;
// starting a run is unchanged Mastra API.
const run = await myWorkflow.createRun();
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

## Alarm Handling

The DO uses storage alarms (`ctx.storage.setAlarm()` plus the class's `alarm()` handler) to wake itself after a timeout. The alarm interval is configurable and determines the polling cadence for approval resolution. Default: 60 seconds.

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
