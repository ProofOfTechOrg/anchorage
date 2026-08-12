# Durable Object runner design

Flowsafe supplies a Cloudflare execution backend for Mastra workflows. The Durable Object serializes calls for one run; Mastra's D1 snapshot is the durable workflow state.

## Import swap

`init()` returns backend-bound workflow factories:

```typescript
import {
  DurableObjectRunner,
  init,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import { approvalStoreFactoryFor } from '@proofoftech/flowsafe/host-kit';
import { z } from 'zod';

interface Env {
  DB: D1Database;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
}

export class WorkflowRunner extends DurableObjectRunner<Env> {
  protected runOwnership(env: Env) {
    return approvalStoreFactoryFor(env.DB).resources();
  }

  protected build(env: Env): RunnerRuntime {
    const { createStep, createWorkflow, runtime } = init(env);

    const prepare = createStep({
      id: 'prepare',
      inputSchema: z.object({ subject: z.string() }),
      outputSchema: z.object({ subject: z.string() }),
      execute: async ({ inputData }) => inputData,
    });

    createWorkflow({
      id: 'example',
      inputSchema: z.object({ subject: z.string() }),
      outputSchema: z.object({ subject: z.string() }),
    })
      .then(prepare)
      .commit();

    return runtime;
  }
}
```

Workflow definitions retain Mastra's schema and builder API. The difference is where `createRun()`, `start()`, `resume()`, and storage are driven.

## One object per run

The Worker addresses:

```text
idFromName(`${workflowId}:${runId}`)
```

The object checks every request's workflow and run against its own `id.name`. The run router validates both path segments before it addresses the namespace.

Inside the object, `RunnerRuntime` keeps a FIFO lock keyed by the same `workflowId:runId`. Cloudflare sends one object's requests to one instance, and the in-instance lock prevents two concurrent resumes from both passing the suspended precondition.

This single-writer property is load-bearing because the Mastra snapshot store uses ordinary writes, not an application-level version CAS. A future path that lets a second object or external process write the same run must add optimistic concurrency before it is safe.

## Run identity

`workflowId` and `runId` must match the exported path-safe pattern. Connector ids use a separate contract and cannot contain the key delimiter that would make stored tuple keys collide.

The authenticated host mints an opaque run id. `RunnerRuntime.start()` requires it and never generates one. `DurableObjectRunner` recovers the authoritative workflow and run pair from its own object name.

A run id carries no organization identity. Physical deployment bindings provide that boundary, so ordinary application code should treat the id as opaque.

## Runtime lifecycle

Start:

1. The Worker verifies deployment identity, authenticates the actor, authorizes the workflow, and mints the run id.
2. The run topology addresses the owning Durable Object and overwrites the internal deployment-identity header.
3. The object authenticates the caller credential, validates its target sentinel, and checks that no snapshot already exists.
4. It obtains the trusted per-leg request context.
5. It calls Mastra `createRun({ runId, pubsub })` and starts with input.
6. Mastra persists its workflow snapshot after engine boundaries.
7. The runtime projects the outcome to a JSON-safe `RunSummary`.
8. The object broadcasts the summary to connected run sockets.

Resume:

1. The approval service or trusted recovery path reaches the same object.
2. The FIFO lock reads the current snapshot and requires `suspended`.
3. The runtime increments the selected step's ordinal in trusted snapshot provenance.
4. The runtime recomputes trusted request context, including workflow scope, run ID, current execution identity, and structured connector grants.
5. Mastra resumes the selected step from D1.
6. The new summary is persisted and broadcast.
7. A new suspension is bridged to a new approval.

No JavaScript promise remains alive while a reviewer waits. The persisted suspension is the wait.

## Durable state

### D1 snapshot

Mastra's `mastra_workflow_snapshot` row is authoritative for:

- run status;
- step state and results;
- suspend payload and selected step paths;
- timestamps;
- durable-agent message-list state where applicable.

Flowsafe does not maintain a parallel custom workflow state object.

### Snapshot provenance

Flowsafe stores trusted run provenance under a reserved request-context key in the same authoritative snapshot as the workflow state. It records the initiating actor, a per-leg attempt token, and a monotonic ordinal per run and step:

- absent on the first suspension;
- `1` after the first resume;
- `2` after the second;
- continuing on every resume, including a resume without payload.

Approvals bind `suspendedAt` with this ordinal. Because workflow state and provenance commit together, recovery can distinguish a committed resume from a pre-snapshot failure without a parallel counter in Durable Object storage.

The runner uses a short-lived Durable Object alarm only to reconcile an interrupted run-owner reservation. Alarms do not drive workflow execution; starts and approval resumes arrive by request.

## Run summary

`RunSummary` is the public projection:

```typescript
import type { ExecutionPrincipalKind } from '@proofoftech/flowsafe/approval-api';

interface RunSummary {
  runId: string;
  status: WorkflowRunStatus;
  result?: unknown;
  error?: string;
  suspended?: string[][];
  suspendPayload?: unknown;
  suspendedAt?: Record<string, number>;
  resumedAt?: Record<string, number>;
  resumeCount?: Record<string, number>;
  requestedBy?: string;
  requestedByKind?: ExecutionPrincipalKind;
  createdAt?: string;
  updatedAt?: string;
}
```

The runtime maps serialized error objects to a useful error string. `resumedAt` is informational because Mastra does not stamp it for every resume shape. Grant binding uses `resumeCount`. `requestedByKind` is paired with `requestedBy`; it is absent only on legacy snapshots written before principal kinds were persisted.

Run WebSockets send the entire authoritative summary at start, resume, and connection. Consumers can replace their cached summary rather than reconstructing state from deltas.

## HTTP surface inside the object

The base object accepts:

```text
POST /runs
GET  /runs/:workflowId/:runId
GET  /runs/:workflowId/:runId/stream
POST /runs/:workflowId/:runId/resume
```

The public Worker normally exposes its own authenticated route facade and forwards to these internal routes. A stream request requires a WebSocket upgrade and workerd's hibernatable socket API; otherwise it returns 426 and the client polls status.

The raw resume surface carries no approval grant. A protected connector still requires a matching stored decision.

## Request context

Before every create or resume, the runtime sets:

- run id;
- workflow id;
- `breakwater.workflowScope`;
- values returned by the host's `requestContextForRun` provider.

Runtime-derived base keys win over stored or client-provided context. `breakwater.isolationScope` remains reserved and is dropped from provider values because connector keys are deployment-wide. Schedules additionally reject these reserved namespaces when data is written.

`approvalGrantProvider()` is the normal provider. A provider failure happens before `createRun()` or resume, so a failed start leaves its run id retryable.

## Import-safe workflow modules

`@proofoftech/flowsafe/host-kit/module` defines a workflow module that receives the factories and deployment dependencies rather than importing a singleton runtime. This supports:

- registering many modules on one runtime;
- testing a module with in-memory dependencies;
- keeping Cloudflare bindings out of module scope;
- avoiding a build-order dependency on generated `dist/`.

`assertWorkflowsRegistered()` compares host metadata with the committed runtime definitions.

## Error taxonomy

The runtime distinguishes:

- unknown workflow;
- unknown run;
- duplicate run;
- run not suspended;
- client-fixable input, resume-data, or step errors;
- internal execution or storage errors.

The Durable Object maps known errors to stable HTTP status codes through `doErrorResponse()`. Unknown failures return an internal error without copying arbitrary thrown data to an audit sink.

The runner does not provide an administrative “reset to last good state” API. Recovery uses authoritative D1 state, the approval redrive path, or deployment decommissioning.

## Retention

`purgeExpiredWorkflowRuns(db, options)`:

- selects only terminal statuses;
- uses a bounded batch;
- deletes paired R2 artifacts before the snapshot row;
- keeps a failed row as a retry anchor while allowing later rows to proceed;
- aggregates per-run failures after the pass;
- treats a not-yet-created snapshot table as empty.

The composed Worker resolves its optional artifact purger from the current maintenance invocation's environment inside this failure boundary. A factory or deletion failure keeps the enumerable snapshot row and does not stop approval or other domain purges.

When runtime storage uses `tablePrefix`, configure the same `storageTablePrefix` on `createFlowsafeWorker()`. It threads that validated prefix through every prefix-aware built-in purge. Direct callers of any exported low-level purge receive the same fail-fast validation before D1 preparation. Fixed-schema Flowsafe tables remain unprefixed.

Running and suspended rows are never age-purged.

Decommissioning deletes the bound storage after credentials and traffic are revoked. There is no in-database tenant purge in the single-organization data plane.

See [Deployment reference](deployment-reference.md) for the full domain lifecycle.

## Worker constraints

- Initialize Mastra after bindings exist, not at module load.
- Use D1-backed storage on the execution path.
- Keep workflows and schemas compatible with the Workers bundle.
- Route every writer for a run through its one Durable Object.
- Add Durable Object migrations as new append-only tags.
- Keep WebSocket polling fallback because sockets and isolates can close.

The deterministic `spike:verify` command kills and restarts workerd around a suspension, then proves resume, forged-resume denial, deployment-sentinel mismatch refusal, stream recovery, and the advanced agent domains.
