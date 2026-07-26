# Getting started

This guide takes a Mastra application from package installation to one guarded connector and one durable approval. Use breakwater by itself when a request-scoped agent is enough. Add flowsafe when execution must survive restarts, wait for a human, or retain tenant-safe state.

## Prerequisites

- Node.js 22 or later
- An ESM TypeScript project using `moduleResolution: "NodeNext"`, `"Node16"`, or `"Bundler"`
- `@mastra/core` in the `^1.50.0` peer range
- A Cloudflare account, D1 database, and Durable Objects only when deploying flowsafe

React is not required by flowsafe unless you import `@proofoftech/flowsafe/approval-ui`.

## Install breakwater

```bash
npm install @mastra/core@^1.50.0 @proofoftech/breakwater
```

### Add agent-boundary policy

Construct one `AuditLogger` and give it to every gate. A denial aborts the processor chain, so each gate records its own decision rather than relying on a trailing audit processor.

```typescript
import { Agent } from '@mastra/core/agent';
import {
  AuditLogger,
  classifierPolicy,
  denyPatterns,
  piiSecrets,
  PolicyEngine,
  RBACMiddleware,
} from '@proofoftech/breakwater';

const audit = new AuditLogger({
  sink: (event) => {
    console.log(JSON.stringify(event));
  },
  onSinkError: (error) => {
    console.error('audit export failed', error);
  },
});

const policy = new PolicyEngine({
  audit,
  holdBack: true,
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
});

const model = process.env.MASTRA_MODEL_ID;
if (!model) throw new Error('MASTRA_MODEL_ID is required');

export const agent = new Agent({
  id: 'guarded-agent',
  name: 'Guarded agent',
  instructions: 'Complete the task within the supplied permissions.',
  model,
  inputProcessors: [
    new RBACMiddleware({
      allowedRoles: ['operator', 'admin'],
      audit,
    }),
    policy,
  ],
  outputProcessors: [policy],
});
```

`holdBack: true` keeps each policy's declared trailing window from being emitted until the engine can determine that it is safe. The guarantee applies per stream segment. Policies that do not declare `holdBackChars` do not gain a window automatically.

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
import { tenantIsolation } from '@proofoftech/breakwater/policy-engine';
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
      evaluators: [tenantIsolation()],
      idempotencyStore: new D1IdempotencyStore(db),
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

The structural D1 types accept a Cloudflare `D1Database`; type the example's `db` accordingly in your Worker. Use shared D1 stores when a rate limit or idempotency key must hold across isolates. In-memory stores cover only one isolate.

Read [Connector interface](connector-interface.md) before writing production connectors. It explains the required request-context keys, fixed-window behavior, redirect handling, and failure semantics.

## Add flowsafe

```bash
npm install @proofoftech/flowsafe
```

Start from [`packages/flowsafe/deploy/`](../packages/flowsafe/deploy/README.md). Copy that directory into your application and preserve its host-kit composition:

1. Create a D1 database and put its id in `wrangler.jsonc`.
2. Register the runner Durable Object migration. Add the hub Durable Object and `STREAM_TICKET_SECRET` when you want live updates.
3. Replace the example workflow, but keep `approvalGrantProvider()` in `init()`.
4. Replace the static bearer verifier with your identity provider. Return an actor with a validated `tenantId`.
5. Provision every tenant before issuing credentials that name it.
6. Configure the sweep and purge cron expressions.
7. Deploy, start a run, approve its queued suspension as a different actor, and inspect the terminal status.

The baseline template deliberately exposes a raw resume route for recovery and testing, but raw resume data never carries a connector grant. Side-effecting steps remain protected by the server-derived approval context.

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
- [Deployment reference](deployment-reference.md) for bindings, secrets, routes, and crons
- [Durable agents](durable-agents.md) for threads, memory, signals, goals, schedules, tasks, and providers
- [Security threat model](security-threat-model.md) before exposing a public endpoint
- [Operations runbook](operations-runbook.md) before production
