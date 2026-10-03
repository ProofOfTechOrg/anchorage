---
"@proofoftech/flowsafe": minor
---

A `DurableObjectRunner` run whose execution leg stops mid-step no longer stays `running`. A start, resume or suspension-deadline leg can stop this way when the platform ends the Durable Object invocation, for example about 15 minutes after the client disconnected. While a leg runs, it sets its run row's `updatedAt` in D1 every 30 seconds without writing the snapshot. Once the row has gone six minutes without a write, the run's own object settles the run on its next wake. The run becomes `failed` with `errorEnvelope: { code: 'INTERRUPTED', message }`, and its idempotent-start reservation is settled. A retry of the same key returns that result instead of `503 persisted start is not readable`. Flowsafe never re-executes the interrupted step, because it may already have had external effects. A run with a recorded cancellation or timeout completes that transition, with its cleanup, instead.

A deploy does not interrupt a leg: the outgoing instance keeps running it, and its touches keep the run from being settled. A Workers runtime update gives in-flight requests at most 30 seconds, so it interrupts a longer leg. A wake no longer queues behind a leg that is still executing in the object. A leg frame older than two hours is reset with `ctx.abort()`, which releases the locks of a promise the platform stopped without settling. A leg that a client keeps connected for longer than two hours is reset the same way.

Settlement needs the `FencedWorkflowsStorageD1` workflow domain that `createD1Storage()` composes. A live leg whose touches fail to reach D1 for six minutes is settled while it runs, and its next workflow write replaces the settlement. A run on other storage, including Mastra's own D1 workflow storage, or one whose leg ran on a flowsafe version before this one, is not settled automatically; `POST /runs/:workflowId/:runId/terminate` ends it.

`RunTerminalErrorEnvelope.code` and `RunSummary.errorEnvelope.code` gain `'INTERRUPTED'`. Code that switches exhaustively on the code needs a branch for it. `FencedWorkflowAdmissionCapability` gains an optional `touchRun`; a custom capability without it never settles a run automatically.

Each step must finish within one invocation. Split long work into steps, and wait in a suspension with a deadline rather than in an in-memory polling loop.
