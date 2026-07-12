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
| `approval-api` | REST API for submitting and reviewing approval requests: claim/decide/delegate, batch decide, triage filters (requester + age bounds), SLA tracking + escalation, and the `ApprovalNotificationSink` transport seam |
| `approval-ui` | Styling-agnostic React UI surfacing pending approvals and history, with queue triage (filter bar + batch selection/decide) |
| `do-runner` | Durable Object workflow runner (import-swap pattern) |
| `audit-export` | Cloudflare Queues → SIEM audit export (producer sink + batch consumer) |
| `artifacts` | R2-backed workflow artifact storage keyed by run identity |
| `host-kit` | Host-agnostic glue: the identity seam (`TokenVerifier`), the tenant resolver, the shared `/workflows` + `/runs` routes, the tenants registry, the suspension→approval bridge, and `createFlowsafeWorker()` — the composed production Worker hosts consume as thin shells |

A copy-ready production deployment lives in [`deploy/`](deploy/) — a thin
shell over `createFlowsafeWorker()` with cron-owned SLA enforcement and
retention purge.

## Installation

```
npm install @proofoftech/flowsafe
```

Support matrix: Node >= 22, ESM only (TypeScript `moduleResolution`
`node16`/`nodenext`/`bundler`; no CJS build), `@mastra/core` ^1.50.0 as a
peer. React 18 or 19 is an optional peer used only by `./approval-ui`;
`@proofoftech/breakwater` is an optional peer whose types only the
`./host-kit/module` subpath references.

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

flowsafe is **multi-tenant by construction** (see
[Multi-tenancy](#multi-tenancy) below): run ids carry their tenant, approval
stores are bound to one tenant at construction, and the cross-tenant SLA sweep
is cron-only code that a request handler cannot reach — enforced by the type
system, not by convention.

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
`spike/worker.ts` for a complete Worker, and run the end-to-end spike with
`pnpm spike` (wrangler dev on `spike/wrangler.jsonc`).

### Approval queue (`@proofoftech/flowsafe/approval-api`)

```typescript
import {
  approvalGrantProvider,
  ApprovalService,
  createApprovalRouter,
  createTenantResolver,
  D1ApprovalStoreFactory,
  resumeViaRuntime,
  sweepSLA,
} from '@proofoftech/flowsafe/approval-api';
import { bearerActorAuthenticator, staticTokenVerifier, parseActorTokens }
  from '@proofoftech/flowsafe/host-kit';

// Hoist the factory to module scope: it owns one memoized schema-init pass,
// so rebuilding it per request re-runs the DDL every time.
const factory = new D1ApprovalStoreFactory(env.DB); // shares the runner's D1

// 1. Grant-minting seam. Inside a Durable Object, bind the store to THIS
//    instance's tenant (recovered from its own idFromName identity), so the
//    mint can only ever read its own tenant's records.
const { createWorkflow, createStep, runtime } = init(env, {
  requestContextForRun: approvalGrantProvider(factory.forTenant(this.tenantId)),
});

// 2. Authenticate FIRST, then construct. The resolver validates the actor's
//    tenant and binds the store to it, so there is no pre-auth service for a
//    later refactor to reach for.
const resolve = createTenantResolver({
  authenticate: bearerActorAuthenticator(
    staticTokenVerifier(parseActorTokens(env.APPROVAL_ACTOR_TOKENS)),
  ),
  storeFactory: factory,
  buildService: (store) =>
    new ApprovalService({
      store,                                  // tenant-bound; required
      defaultSlaSeconds: 15 * 60,
      resumeRun: resumeViaRuntime(runtime),   // or a DO-stub fetch
      audit: (event) => auditLogger.record(event),
    }),
});

// 3. REST surface (list/get/claim/decide/delegate/metrics). The create route
//    is off unless you pass allowCreate.
const router = createApprovalRouter({ resolve });
```

Escalation is **cron-only**. There is deliberately no HTTP sweep route: the
sweep is an unfiltered cross-tenant read *and write*, so it lives in a
standalone `sweepSLA(factory.system(), { systemActor, audit, onEscalation })`
that takes a `SystemApprovalStore` — a type a request handler cannot obtain.
Call it from `scheduled()`. Breached open requests transition to `escalated`
(still decidable) and fire `onEscalation`.

Grant scope: a step-keyed approval unlocks its connectors only for the leg
that resumes that step, and only for the suspension it was decided during —
when the same step suspends again, the earlier approval is spent and the new
suspension needs its own decision. Create a step-less approval for
deliberately run-scoped standing grants. Self-approval (decider ==
requester) is denied unless the service is constructed with
`allowSelfDecision` — `true` exempts every decider, or `{ roles }` exempts
only the listed roles (a single-operator deployment sets e.g.
`{ roles: ['admin'] }`; composed hosts pass `APPROVAL_ALLOW_SELF_DECISION`).
A permitted self-decision is audited with `detail.selfDecision: true`. If a decision's
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
render any UI you like. A ready-to-run reference app lives in the repo's
`showcase` package (`packages/showcase/` — `pnpm --filter showcase dev`, or
`pnpm dev` at the root): a full Vite build that injects an
[Astryx](https://astryx.atmeta.com) adapter, bundles that library's CSS, and —
in dev — mounts a live seeded approval-api at `/api/approvals` so
claim/decide/delegate drive real state.

## Multi-tenancy

Three invariants, each enforced at a single chokepoint. Everything else is a
corollary.

**INV-1 — the run id carries the tenant.** Every `runId` is minted
server-side as `` `${tenantId}_${uuid}` `` from the *authenticated* tenant.
`createRunRouter` rejects a client-supplied `body.runId` with 400,
`RunnerRuntime.start` requires a `runId` (there is no generation fallback),
and the Durable Object refuses any request whose `(workflowId, runId)`
disagrees with its own `ctx.id.name`. Because `runId` is the key everything
else derives from, this makes the Mastra snapshot row, the DO instance, the
per-run lock, the grant-mint predicate, and the R2 key tenant-disjoint with
no schema change and no signature change. Status and resume additionally
check ownership and answer **404** — not 403 — for another tenant's run, so
the route is not an existence oracle.

**INV-2 — the store is bound to one tenant at construction.** Approval
stores come only from a factory (`D1ApprovalStoreFactory.forTenant()`), and
the bound type carries a `unique symbol` brand: an unbound store, or the
cron-only `SystemApprovalStore`, is a *compile error* wherever a request
handler expects a bound one. `tenantId` is deliberately not a member of
`ApprovalListFilter` — an omissible tenant filter is the canonical fail-open.
Requests flow through a `TenantResolver` (authenticate → validate → bind), so
no pre-auth store exists.

**INV-3 — the charset makes the prefix exact.** `tenantId` matches
`^[a-z0-9]{3,32}$`. That set contains no character in `[0x5F, 0x60]` —
neither `_` nor `` ` `` — so `runId.startsWith(tenantId + '_')` cannot match
another tenant (`acme` never matches `acmecorp`), and the offboarding range
delete `` run_id >= '<tid>_' AND run_id < '<tid>`' `` selects exactly one
tenant's rows. Loosening the charset silently breaks both; a
character-exhaustive test pins it.

Consequences worth knowing:

- **Provision before you issue tokens.** `provisionTenant(db, { tenantId, kind })`
  (import from `@proofoftech/flowsafe/host-kit`)
  inserts into the `tenants` registry or fails. Nothing else enforces tenant-id
  uniqueness, and two clients slugged `acme` would merge entirely.
- **Every actor carries a tenant.** `ApprovalActor` is `{ id, role, tenantId }`;
  a bearer-map entry or JWT claim without an INV-3-valid `tenantId` is dropped
  and its token 401s. breakwater's own `Actor` stays tenant-agnostic.
- **Connector budgets are per-tenant.** The runtime mints an opaque
  `breakwater.isolationScope` on every leg of a tenant-salted run, and
  breakwater segments its idempotency and rate-limit keys by it — so one
  tenant exhausting a connector's budget cannot throttle another, and two
  tenants sharing a business-level idempotency key do not replay each other's
  results. Absent scope preserves the single-tenant keys exactly; there is no
  flag to forget.
- **Offboarding is one call.** `purgeTenant(db, { tenantId, artifactStore })`
  (import from `@proofoftech/flowsafe/do-runner` or the package root)
  reaps snapshots of *any* status (a run abandoned at a gate is never eligible
  for the terminal-only retention purge), the tenant's approval records, and
  its R2 artifacts. Only purge tenants whose tokens have already expired.

### Deployment & ops

Copy [`deploy/`](deploy/) as a production starting point: a Worker wiring one
DO per run, the D1 approval queue, the tenant resolver over a bearer-token
verifier, an optional subdomain↔tenant cross-check, and a cron trigger that
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
