# Anchorage

Anchorage adds enforceable guardrails, durable approvals, and tenant-safe long-running execution to [Mastra](https://mastra.ai) on Cloudflare.

[![CI](https://github.com/ProofOfTechOrg/anchorage/actions/workflows/ci.yml/badge.svg)](https://github.com/ProofOfTechOrg/anchorage/actions/workflows/ci.yml)
[![npm: breakwater](https://img.shields.io/npm/v/%40proofoftech%2Fbreakwater?label=breakwater)](https://www.npmjs.com/package/@proofoftech/breakwater)
[![npm: flowsafe](https://img.shields.io/npm/v/%40proofoftech%2Fflowsafe?label=flowsafe)](https://www.npmjs.com/package/@proofoftech/flowsafe)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

[Try the live demo](https://anchorage.proofoftech.org/) · [Read the docs](docs/README.md) · [Browse the API reference](https://proofoftechorg.github.io/anchorage/) · [Get support](SUPPORT.md)

## Why Anchorage

Mastra supplies the agent and workflow runtime. Anchorage adds the controls that become necessary when those agents can change real systems or remain active beyond one request:

- Every side-effecting connector declares its permissions, then enforces egress, approval, idempotency, dry-run, rate-limit, and isolation policy at the tool boundary.
- Runs suspend without keeping compute alive, survive Worker and Durable Object restarts, and resume from authoritative D1 snapshots.
- Approval grants are derived from stored decisions and bound to the exact suspension. A client cannot mint a capability by forging a resume body.
- Tenant identity is carried through run, thread, memory, schedule, task, subscription, notification, artifact, and approval storage.
- Structured audit events can feed metrics, Cloudflare Queues, and a SIEM.

Use Anchorage when you already use Mastra and Cloudflare, and your agents perform writes, need human review, run for a long time, accept signals, or operate across tenants.

Anchorage is not a model provider, identity provider, hosted SaaS, generic process sandbox, or complete network perimeter. You provide authentication and deployment policy. Socket-level egress control still belongs in your infrastructure.

## Choose a package

| Package | Install when you need | Main surfaces |
| --- | --- | --- |
| [`@proofoftech/breakwater`](packages/breakwater/README.md) | Guardrails around Mastra agents and tools | Guarded agent handles, policy processors, RBAC, audit and metrics, connector SDK, Claude Code and Codex CLI connectors |
| [`@proofoftech/flowsafe`](packages/flowsafe/README.md) | Durable execution and human approval on Cloudflare | Guarded agent catalog/host, Durable Object runner, approval API and React UI, signals, goals, schedules, background tasks, provider subscriptions, R2 artifacts, SIEM export |

Both packages are ESM-only, require Node 22 or later, and declare
`@mastra/core` `^1.50.0` as their peer range. React 18 or 19 is needed only
for the optional flowsafe approval UI.

## Start with breakwater

```bash
npm install @mastra/core@^1.50.0 @proofoftech/breakwater
```

Create a guarded agent and pass the authenticated actor through Mastra's `RequestContext`:

```typescript
import { RequestContext } from '@mastra/core/request-context';
import {
  ACTOR_CONTEXT_KEY,
  AuditLogger,
  createGuardedAgent,
  denyPatterns,
} from '@proofoftech/breakwater';

const audit = new AuditLogger({
  sink: async (event) => {
    console.log(JSON.stringify(event));
  },
});

const model = process.env.MASTRA_MODEL_ID;
if (!model) throw new Error('MASTRA_MODEL_ID is required');

const agent = createGuardedAgent({
  id: 'release-agent',
  name: 'Release agent',
  instructions: 'Prepare releases without exposing secrets.',
  model,
  allowedRoles: ['operator', 'admin'],
  policies: [denyPatterns(['private key', 'ignore previous instructions'])],
  audit,
  maxSteps: 8,
  toolChoice: 'auto',
});

const requestContext = new RequestContext();
requestContext.set(ACTOR_CONTEXT_KEY, {
  id: 'operator-42',
  role: 'operator',
});

const result = await agent.generate('Prepare the release notes', {
  requestContext,
});
```

For writes, wrap the Mastra tool with `createConnector()`. The manifest is enforced on direct calls, workflow calls, and agent calls:

```typescript
import {
  createConnector,
  InMemoryIdempotencyStore,
  InMemoryRateLimitStore,
} from '@proofoftech/breakwater/connector-sdk';
import { z } from 'zod';

export const publishRelease = createConnector({
  id: 'releases.publish',
  description: 'Publish one release',
  inputSchema: z.object({ releaseId: z.string() }),
  outputSchema: z.object({ published: z.boolean() }),
  permissions: {
    sideEffect: 'write',
    egress: ['api.example.com'],
    requiresApproval: true,
    idempotencyKey: true,
    dryRun: true,
    rateLimit: '10/hour',
  },
  policies: {
    idempotencyStore: new InMemoryIdempotencyStore(),
    rateLimitStore: new InMemoryRateLimitStore(),
  },
  execute: async ({ releaseId }, _context, runtime) => {
    const response = await runtime.fetch(
      `https://api.example.com/releases/${releaseId}`,
      { method: 'POST' },
    );
    return { published: response.ok };
  },
  dryRunExecute: async () => ({ published: false }),
});
```

The in-memory stores make this definition runnable in one process. Use the D1
stores when replay or rate limits must span Workers or Durable Objects. The
connector authoring guide explains every permission, required store, and
accepted limit: [`packages/breakwater/CONNECTORS.md`](packages/breakwater/CONNECTORS.md).

## Add durable approvals

```bash
npm install @mastra/core@^1.50.0 @proofoftech/breakwater @proofoftech/flowsafe
```

Start from the copy-ready baseline Worker in [`packages/flowsafe/deploy/`](packages/flowsafe/deploy/README.md). It wires one Durable Object per run, D1 snapshots and approvals, authenticated run routes, server-derived connector grants, live-streaming opt-in, SLA sweep, retention, audit export, and a sample gated workflow.

For a long-running agent host with signals, goals, schedules, provider subscriptions, and background tasks, use the private workspace starter in [`packages/agent-starter/`](packages/agent-starter/README.md). The starter composes only published package entry points, so it also serves as a consumer-level compatibility test.

The full path from install to first approval is in [`docs/getting-started.md`](docs/getting-started.md).

## Feature map

### Guardrails and connectors

- Mastra input and output processors with answer, reasoning, and structured-object channels
- Streaming deny-pattern inspection with optional zero-leak hold-back
- PII and secret detection using structured patterns, entropy checks, Luhn validation, allowlists, and streaming windows
- Pluggable asynchronous classifier with fail-closed timeout behavior
- Agent-boundary RBAC and workflow-scoped host authorization
- Permission manifests for side effects, declared egress, approval, idempotency, dry-run, background eligibility, and fixed-window rate limits
- Per-hop egress enforcement for calls made through the injected connector runtime, including redirect checks and cross-origin credential stripping
- In-memory and D1-backed idempotency and rate-limit stores
- Workflow and tenant isolation evaluators
- Approval-gated Claude Code and Codex connectors with argv separation, bounded execution, dry-run previews, and safe diagnostics
- Structured audit sinks, metrics adapters, and sink fan-out

### Durable approvals and execution

- D1-backed Mastra workflow snapshots with a one-run-per-Durable-Object topology
- CAS-guarded approval claim, decision, delegation, batch decision, SLA escalation, and notification
- Separation of duties across repeated gates and exact suspension-bound grant derivation
- Styling-library-agnostic React 18/19 dashboard, headless hook, polling fallback, optimistic decisions, live WebSocket merge, presence, and injected UI slots
- Per-tenant live approval and run streams using short-lived HMAC addressing tickets
- R2 artifact storage, lifecycle pairing, terminal-run retention, thread retention, and complete tenant offboarding
- Cloudflare Queues to SIEM NDJSON export

### Long-running agents

The following surfaces are supported and opt-in: they are tested and covered by package compatibility guarantees, but the host must explicitly wire the required routes, bindings, storage domains, or scheduled duties.

- Server-owned guarded-agent catalog with authenticated list, start, status, and NDJSON observation routes
- Approval-only durable agent resume that restores the original authorized principal
- Runtime-driven Mastra durable agents with restart-safe approval resume
- Tenant-minted thread and resource identities with D1-backed recall isolation
- Thread signals, messages, state, notifications, idle wake, and a DOM-free client
- Durable objectives and bounded goal updates
- Workflow and agent schedules with CAS fire claims and reserved-context protection
- Tenant-scoped background task execution and cleanup
- Alarm-driven signal providers, human-managed subscriptions, verified webhooks, and GitHub reference integration

See [`docs/durable-agents.md`](docs/durable-agents.md) for the wiring and lifecycle.

## Demo

The [public showcase](https://anchorage.proofoftech.org/) runs the open-source stack against isolated, short-lived tenants. It includes six server-side workflows and seven deterministic guardrail scenarios covering PII, secrets, prompt injection, RBAC, egress, workflow isolation, and tenant isolation. External side effects are simulated unless the corresponding binding is configured; the approval, grant, audit, tenancy, and suspend/resume paths are real.

Run it locally:

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm build
pnpm dev
```

## Documentation

- [Documentation index](docs/README.md)
- [Getting started](docs/getting-started.md)
- [Connector SDK contract](docs/connector-interface.md)
- [Agent CLI connectors](docs/agent-cli-connectors.md)
- [Approval system](docs/approval-system.md)
- [Durable agents](docs/durable-agents.md)
- [Deployment reference](docs/deployment-reference.md)
- [Security threat model](docs/security-threat-model.md)
- [Operations runbook](docs/operations-runbook.md)
- [Generated API reference](https://proofoftechorg.github.io/anchorage/)

## Development

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm docs:check
pnpm docs:api
pnpm --filter @proofoftech/flowsafe spike:verify
```

The deterministic workerd spike proves suspension, restart, resume, forged-resume denial, tenancy, live streaming, durable agent recovery, schedules, signals, goals, provider delivery, and background tasks without an external model. The separate `spike:verify:llm` gate is optional and requires provider credentials.

## Support, security, and contributing

Read [`SUPPORT.md`](SUPPORT.md) for help, [`SECURITY.md`](SECURITY.md) for private vulnerability reporting, and [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening a pull request. Contributions are accepted under Apache-2.0 without a CLA or DCO requirement.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
