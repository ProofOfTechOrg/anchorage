# Getting started

This guide takes a Mastra application from package installation to one guarded connector and one durable approval. Use breakwater by itself when a request-scoped agent is enough. Add flowsafe when execution must survive restarts, wait for a human, or retain state in a physically isolated deployment.

## Prerequisites

- Node.js 22.13.0 or later for Breakwater and Flowsafe; the complete repository and private Fleet Control package require Node.js 22.22.0 or later
- An ESM TypeScript project using `moduleResolution: "NodeNext"`, `"Node16"`, or `"Bundler"`
- `@mastra/core` `1.50.0`
- A Cloudflare account, D1 database, and Durable Objects only when deploying flowsafe

React is not required by flowsafe unless you import `@proofoftech/flowsafe/approval-ui`.

## Install breakwater

```bash
npm install @mastra/core@1.50.0 @proofoftech/breakwater
```

### Add agent-boundary policy

Construct one `AuditLogger`, then create the agent through the guarded factory. The returned handle exposes only unstructured `generate()` and `stream()` calls with a mandatory request context.

```typescript
import {
  AuditLogger,
  classifierPolicy,
  createGuardedAgent,
  denyPatterns,
  piiSecrets,
} from '@proofoftech/breakwater';

const audit = new AuditLogger({
  sink: (event) => {
    console.log(JSON.stringify(event));
  },
  onSinkError: (error) => {
    console.error('audit export failed', error);
  },
});

const model = process.env.MASTRA_MODEL_ID;
if (!model) throw new Error('MASTRA_MODEL_ID is required');

export const agent = createGuardedAgent({
  id: 'guarded-agent',
  name: 'Guarded agent',
  instructions: 'Complete the task within the supplied permissions.',
  model,
  allowedRoles: ['operator', 'admin'],
  audit,
  policies: [
    denyPatterns(['ignore previous instructions']),
    piiSecrets(),
    classifierPolicy({
      classify: async (text) => ({
        allowed: !text.includes('disallowed-category'),
      }),
      timeoutMs: 2_000,
    }),
  ],
  maxSteps: 8,
  toolChoice: 'auto',
});
```

`createGuardedAgent()` forces policy hold-back, disables background continuations, and rejects per-call processor, tool, model, callback, hook, structured-output, and execution-limit overrides. Under the supported Mastra version, parsed structured output reaches messages and persistence before a wrapper gate could inspect it, so guarded structured output remains unavailable. Object-only policies are rejected at construction. Application input processors can enforce only initial input. Application output processors must enforce both streamed and final results.

The narrow handle prevents accidental bypass through Mastra's larger `Agent` API. It is a trusted in-process API, not a sandbox against hostile code running with the same imports and credentials.

### Supply the authenticated actor

Authentication remains a host responsibility. Put the verified identity in the Mastra request context under the exported key:

```typescript
import { RequestContext } from '@mastra/core/request-context';
import { ACTOR_CONTEXT_KEY } from '@proofoftech/breakwater/rbac';

const requestContext = new RequestContext();
requestContext.set(ACTOR_CONTEXT_KEY, {
  id: 'operator-42',
  role: 'operator',
});

const response = await agent.generate('Prepare a deployment summary', {
  requestContext,
});
```

The five breakwater roles are `admin`, `builder`, `operator`, `reviewer`, and `viewer`. `RBACMiddleware` checks membership in `allowedRoles`; it is not a general ACL database or identity provider.

## Create a guarded connector

Use `createConnector()` for any Mastra tool that can reach a network or change state. The permission manifest is compiled into the returned tool's execution path.

```typescript
import {
  createConnector,
  D1IdempotencyStore,
  D1RateLimitStore,
  type IdempotencyDatabase,
  type RateLimitDatabase,
} from '@proofoftech/breakwater/connector-sdk';
import { z } from 'zod';

export function createPublisher(
  db: IdempotencyDatabase & RateLimitDatabase,
) {
  return createConnector({
    id: 'release.publish',
    description: 'Publish a prepared release',
    inputSchema: z.object({
      releaseId: z.string(),
      notes: z.string(),
    }),
    outputSchema: z.object({
      status: z.enum(['published', 'simulated']),
    }),
    permissions: {
      sideEffect: 'write',
      egress: ['releases.example.com'],
      requiresApproval: true,
      idempotencyKey: true,
      dryRun: true,
      rateLimit: '10/hour',
    },
    policies: {
      idempotencyStore: new D1IdempotencyStore(db),
      idempotencyKeyMigration: 'legacy-writers-drained',
      rateLimitStore: new D1RateLimitStore(db),
    },
    execute: async ({ releaseId, notes }, _context, runtime) => {
      const response = await runtime.fetch(
        `https://releases.example.com/releases/${releaseId}`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ notes }),
        },
      );
      if (!response.ok) throw new Error(`release API returned ${response.status}`);
      return { status: 'published' };
    },
    dryRunExecute: async () => ({ status: 'simulated' }),
  });
}
```

The structural D1 types accept a Cloudflare `D1Database`; type the example's `db` accordingly in your Worker. The migration acknowledgement is valid for this new empty deployment only. Before setting it on an upgraded shared store, stop and drain every legacy writer, inventory through `inspectLegacyConnectorIdempotency()`, prove each ambiguous row's tuple from business or audit evidence, and move proven rows with `migrateLegacyConnectorIdempotency()`, as described in [Connector interface](connector-interface.md#idempotency). The migration helper requires D1-compatible transactional `batch()` semantics. Use shared D1 stores when a rate limit or idempotency key must hold across isolates. In-memory stores cover only one isolate. Flowsafe connector budgets and idempotency are deployment-wide. A different generic host can use Breakwater's opaque isolation scope when it owns another trusted logical partition.

Read [Connector interface](connector-interface.md) before writing production connectors. It explains the required request-context keys, fixed-window behavior, redirect handling, and failure semantics.

## Add flowsafe

```bash
npm install @proofoftech/flowsafe
npm install --save-dev "wrangler@>=4.118 <5"
```

Start from [`packages/flowsafe/deploy/`](../packages/flowsafe/deploy/README.md). Copy that directory into your application and preserve its host-kit composition:

1. Choose a stable lowercase deployment tag. Replace the checked-in `replace-me` segment in both the Worker `name` and D1 `database_name`, create that uniquely named D1 database, and put its id in `wrangler.jsonc`. The unique Worker name is also the deployment's Durable Object namespace boundary.
2. Set `DEPLOYMENT_TENANT` to the same tag. The checked-in invalid placeholder must never reach deployment.
3. Before application migrations, run `npx flowsafe-provision --database <database> --tag <tag> --remote --config wrangler.jsonc` from your application. The CLI is published with Flowsafe and uses the host-provided Wrangler `>=4.118 <5` installation in your project. Wrangler is not installed as a Flowsafe peer.
4. Set distinct `DEPLOYMENT_IDENTITY_SECRET` and `MAINTENANCE_ADMIN_SECRET` values with `wrangler secret put`. Every Worker-to-Durable-Object request requires the first credential. Only the provisioning control plane uses the second.
5. Register the runner and maintenance Durable Object migrations. Add the hub Durable Object and `STREAM_TICKET_SECRET` when you want live updates.
6. Replace the example workflow, but keep `approvalGrantProvider()` in `init()`.
7. Replace the static bearer verifier with your identity provider. Return a validated actor with `id` and `role`; do not accept a tenant claim.
8. Deploy, then call `POST /admin/ensure-maintenance` with the maintenance credential. Confirm `GET /admin/maintenance-status` returns a non-null `alarmAt`.
9. Start a run, approve its queued suspension as a different actor, and inspect the terminal status.

The baseline template deliberately exposes a raw resume route for generic
workflow recovery and testing, but raw resume data never carries a connector
grant. Side-effecting steps remain protected by the server-derived approval
context. The agent host has no public resume route; an agent run advances only
through an approval decision.

## Define an approval gate

A gate suspends with a static connector list. The host bridge copies that server-authored list into the approval record, and `approvalGrantProvider()` derives the matching capability on resume.

```typescript
const gate = createStep({
  id: 'approveRelease',
  inputSchema: z.object({ releaseId: z.string() }),
  outputSchema: z.object({
    releaseId: z.string(),
    approved: z.boolean(),
  }),
  suspendSchema: z.object({
    reason: z.string(),
    connectors: z.array(z.string()),
  }),
  resumeSchema: z.object({
    approved: z.boolean(),
    decidedBy: z.string().optional(),
  }),
  execute: async ({ inputData, resumeData, suspend }) => {
    if (!resumeData) {
      return suspend({
        reason: `Publish release ${inputData.releaseId}`,
        connectors: ['release.publish'],
      });
    }
    return { ...inputData, approved: resumeData.approved };
  },
});
```

Do not derive `connectors` from client input. That array selects the capability a later decision may mint. The next step must branch on `approved` and skip the write when it is `false`; a rejection is a completed decision, not a reason to suspend the same gate again.

## Exercise the approval loop

The baseline host exposes these routes:

```text
GET  /workflows
POST /runs
GET  /runs/:workflowId/:runId
POST /runs/:workflowId/:runId/resume
POST /runs/:workflowId/:runId/terminate
GET  /api/approvals
POST /api/approvals/:id/claim
POST /api/approvals/:id/decide
POST /api/approvals/:id/delegate
POST /api/approvals/batch/decide
POST /api/stream/ticket
GET  /api/stream/hub
GET  /api/stream/run/:workflowId/:runId
```

Start and approve:

```bash
curl -X POST https://worker.example/runs \
  -H 'authorization: Bearer operator-token' \
  -H 'content-type: application/json' \
  -d '{"workflowId":"release","inputData":{"releaseId":"r-42"}}'

curl https://worker.example/api/approvals \
  -H 'authorization: Bearer reviewer-token'

curl -X POST https://worker.example/api/approvals/APPROVAL_ID/decide \
  -H 'authorization: Bearer reviewer-token' \
  -H 'content-type: application/json' \
  -d '{"decision":"approve","comment":"Release review passed"}'
```

By default, the actor who requested or advanced a gate cannot decide it. For a single-operator installation, configure the narrowest `APPROVAL_ALLOW_SELF_DECISION` role exemption that matches your operating model.

## Add the dashboard

Install React only for this subpath:

```bash
npm install react@^19 react-dom@^19
```

```typescript
import {
  ApprovalApiClient,
  createApprovalDashboard,
} from '@proofoftech/flowsafe/approval-ui';

const client = new ApprovalApiClient({
  headers: {
    authorization: `Bearer ${token}`,
  },
});

createApprovalDashboard(document.getElementById('root')!, {
  client,
});
```

The default renderer is plain HTML. Inject `ApprovalUIComponents` or use `useApprovalDashboard()` when you need your own design system.

## Choose the next guide

- [Approval system](approval-system.md) for queue semantics, live streaming, and recovery
- [Deployment reference](deployment-reference.md) for bindings, secrets, routes, and alarm-driven maintenance
- [Durable agents](durable-agents.md) for threads, memory, signals, goals, schedules, tasks, and providers
- [Security threat model](security-threat-model.md) before exposing a public endpoint
- [Operations runbook](operations-runbook.md) before production
