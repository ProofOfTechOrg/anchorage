# flowsafe

Approval UX and durable execution for Mastra workflows, running on Cloudflare
Workers and Durable Objects.

## Purpose

flowsafe provides human-in-the-loop approval gates for Mastra workflows and
durable execution via the Durable Object import-swap pattern. It ships as a
standalone service with a React approval UI and a DO-based workflow runner.

## Subpackages

| Subpackage | Role |
|---|---|
| `approval-api` | REST API for submitting and reviewing approval requests |
| `approval-ui` | Styling-agnostic React UI surfacing pending approvals and history |
| `do-runner` | Durable Object workflow runner (import-swap pattern) |
| `audit-export` | Cloudflare Queues → SIEM audit export (producer sink + batch consumer) |
| `artifacts` | R2-backed workflow artifact storage keyed by run identity |

A copy-ready production deployment lives in [`deploy/`](deploy/) — a Worker
wiring all of the above with cron-owned SLA enforcement and retention purge.

## Installation

```
npm install @proofoftech/flowsafe
```

## Status

Implemented, tested, and spike-verified. The do-runner + D1 adapter run a
Mastra workflow on Workers + DO under `wrangler dev`, suspend at an approval
step, survive a full dev-server restart, and resume to completion from the
D1 snapshot — proven as a pass/fail command by
`pnpm --filter @proofoftech/flowsafe spike:verify`.

The approval queue + dashboard + SLA are implemented, tested, and
spike-verified on workerd: a suspension queues a D1-backed approval request
(CAS-guarded store, one open request per suspended step); a reviewer decision
— role-checked, audited, SLA-tracked, self-approval denied by default —
resumes the run through its Durable Object; and the runtime's
`requestContextForRun` provider derives the breakwater connector grant
(`breakwater.approvedConnectors`) from APPROVED records on every
start/resume, leg-scoped to the resumed step. Grants never travel in HTTP
bodies: a resume that bypasses `decide()` for its step finds no grant and
fails closed at the connector gate — even when the same connector was
approved at an earlier gate of the run.

## Usage

```typescript
import { z } from 'zod';
import { DurableObjectRunner, init, type RunnerRuntime } from '@proofoftech/flowsafe/do-runner';

// Inside the Durable Object: init(env) builds D1-backed storage from the
// conventional `DB` binding (or pass { storage } explicitly, e.g. in tests)
// and returns import-swapped factories. Workflow definition code is
// unchanged Mastra code.
export class MyRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init(env);
    const gate = createStep({
      id: 'approval',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string(), approvedBy: z.string() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ approvedBy: z.string() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'approval required' });
        return { topic: inputData.topic, approvedBy: resumeData.approvedBy };
      },
    });
    createWorkflow({
      id: 'gated',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string(), approvedBy: z.string() }),
    })
      .then(gate)
      .commit();
    return runtime;
  }
}
```

The DO exposes `POST /runs`, `GET /runs/:workflowId/:runId`, and
`POST /runs/:workflowId/:runId/resume`; route one DO instance per run
(`idFromName(workflowId + ':' + runId)`) from your Worker. See
`demo/worker.ts` for a complete Worker, and run the end-to-end spike with
`pnpm spike` (wrangler dev on `demo/wrangler.jsonc`).

### Approval queue (`@proofoftech/flowsafe/approval-api`)

```typescript
import {
  approvalGrantProvider,
  ApprovalService,
  createApprovalRouter,
  D1ApprovalStore,
  resumeViaRuntime,
} from '@proofoftech/flowsafe/approval-api';

const store = new D1ApprovalStore(env.DB); // shares the runner's D1 database

// 1. Grant-minting seam: wire the provider into the runner so every
//    start/resume derives `breakwater.approvedConnectors` from APPROVED
//    records — grants never travel in request bodies.
const { createWorkflow, createStep, runtime } = init(env, {
  requestContextForRun: approvalGrantProvider(store),
});

// 2. The queue: create on suspend, decide to resume.
const service = new ApprovalService({
  store,
  defaultSlaSeconds: 15 * 60,
  resumeRun: resumeViaRuntime(runtime), // or a DO-stub fetch across Workers
  audit: (event) => auditLogger.record(event), // breakwater AuditLogger fits
});

// 3. REST surface (list/get/create/claim/decide/delegate/metrics/sla-sweep)
//    mounted ahead of your other routes; authenticate() is your session/JWT
//    mapping and is part of the trusted computing base.
const router = createApprovalRouter({ service, authenticate });
```

Escalation: run `POST /api/approvals/sla/sweep` from a Workers cron trigger;
breached open requests transition to `escalated` (still decidable) and fire
`onEscalation`.

Grant scope: a step-keyed approval unlocks its connectors only for the leg
that resumes that step, and only for the suspension it was decided during —
when the same step suspends again, the earlier approval is spent and the new
suspension needs its own decision. Create a step-less approval for
deliberately run-scoped standing grants. Self-approval (decider ==
requester) is denied unless the service is constructed with
`allowSelfDecision: true`. If a decision's
resume attempt fails (`resume.ok === false`), the decision is already
durable — re-drive the run by POSTing its resume route with the record's
`stepPath` and the `defaultResumeData` shape; grants re-derive from the
store.

### Dashboard (`@proofoftech/flowsafe/approval-ui`)

A React dashboard over the REST API — queue, detail + decision form, metrics.
It is **styling-library agnostic**: the views render through injected slot
components, so you plug in your own design system — or use the built-in unstyled
HTML default. `react`/`react-dom` are optional peers needed only for this
subpath, and it runs on React 18+.

```typescript
import { ApprovalApiClient, createApprovalDashboard } from '@proofoftech/flowsafe/approval-ui';

const client = new ApprovalApiClient({ headers: { authorization: `Bearer ${token}` } });

// Unstyled HTML default — zero extra deps; style via the flowsafe-* class hooks.
const dashboard = createApprovalDashboard(document.getElementById('root')!, { client });

// …or supply a design-system adapter for the ApprovalUIComponents slots:
// createApprovalDashboard(el, { client, components: myAdapter });
```

For a headless integration, drive `useApprovalDashboard(client)` yourself and
render any UI you like. A ready-to-run reference app lives in `app/` (`pnpm
--filter @proofoftech/flowsafe app:dev`): a full Vite build that injects an
[Astryx](https://astryx.atmeta.com) adapter, bundles that library's CSS, and —
in dev — mounts a live seeded approval-api at `/api/approvals` so
claim/decide/delegate drive real state.

### Deployment & ops

Copy [`deploy/`](deploy/) as a production starting point: a Worker wiring one
DO per run, the D1 approval queue, bearer-token auth, and a cron trigger that
owns the SLA sweep and snapshot retention purge. `pnpm deploy:dev` runs it on
local workerd (no Cloudflare account); `pnpm deploy:cf` deploys it. Checklist
and configuration table: [`deploy/README.md`](deploy/README.md).

Audit export ships as a Cloudflare Queues sink + SIEM consumer
(`@proofoftech/flowsafe/audit-export`): `queueAuditSink(env.QUEUE)` plugs into
any breakwater/approval audit sink, and `createAuditQueueConsumer({ endpoint })`
POSTs each batch as NDJSON, acking on 2xx and retrying otherwise. Workflow
artifacts persist to R2 via `R2ArtifactStore`
(`@proofoftech/flowsafe/artifacts`), keyed `workflowId/runId/name`.

See `docs/flowsafe-architecture.md` and `docs/do-runner-design.md` for design details.
