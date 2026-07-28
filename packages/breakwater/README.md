# breakwater

Runtime guardrails for Mastra agents.

`@proofoftech/breakwater` adds enforcement at the places where an agent can
accept data, emit data, or cause a side effect. It provides Mastra processors
for policy and role checks, an enforced connector wrapper for tools, a shared
audit stream, and approval-gated adapters for Claude Code and Codex.

## Why use breakwater

Model instructions are not an authorization boundary. A production agent also
needs code that can:

- reject an unauthorized caller before the model runs;
- inspect input, answer text, reasoning, and streaming object snapshots;
- stop undeclared network access and unapproved writes at the tool boundary;
- make retries idempotent and enforce execution budgets;
- produce structured evidence for every allowed, denied, and failed decision.

These controls operate at different boundaries and are designed to be used
together.

| Boundary | Public API | What it enforces |
| --- | --- | --- |
| Guarded agent entry | `createGuardedAgent()` | Mandatory role and policy order, fixed execution limits, and a narrow call-option allowlist |
| Agent input and output | `PolicyEngine` | Content policies across input, final output, and streaming channels |
| Agent input | `RBACMiddleware` | Actor presence and an explicit role allowlist |
| Tool execution | `createConnector()` | Permission manifests, egress, approvals, dry runs, idempotency, rate limits, isolation, and custom policies |
| Every gate | `AuditLogger` | Structured, attributed audit events with sink failure isolation |
| Local coding agents | Agent CLI adapters | The connector controls plus safe argv construction, timeouts, bounded output capture, and redacted diagnostics |

breakwater is safety middleware, not an agent runtime. Use
[`@proofoftech/flowsafe`](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/flowsafe)
when runs must survive restarts, wait for human approval, stream live state,
schedule work, or run durable agent loops on Cloudflare.

## Install it

breakwater is ESM-only, requires Node.js 22 or newer (engine range `>=22`),
and supports `@mastra/core` in its declared `^1.50.0` peer range.

```bash
npm install @proofoftech/breakwater @mastra/core
```

Connector authors who define Zod schemas should also declare Zod directly:

```bash
npm install zod
```

The package supports the root import and six focused subpaths:

```typescript
import { PolicyEngine } from '@proofoftech/breakwater';
import { createGuardedAgent } from '@proofoftech/breakwater/agent';
import { AuditLogger } from '@proofoftech/breakwater/audit';
import { createConnector } from '@proofoftech/breakwater/connector-sdk';
import { RBACMiddleware } from '@proofoftech/breakwater/rbac';
import { createCodexConnector } from '@proofoftech/breakwater/agent-cli';
import { denyPatterns } from '@proofoftech/breakwater/policy-engine';
```

## Guard an agent

Use `createGuardedAgent()` for supported protected agent execution. The factory installs role-based access control (RBAC) before application input processors and policy, then installs policy after application output processors.

```typescript
import { RequestContext } from '@mastra/core/request-context';
import {
  ACTOR_CONTEXT_KEY,
  AGENT_AUDIT_CONTEXT_KEY,
  AuditLogger,
  createGuardedAgent,
  denyPatterns,
  piiSecrets,
} from '@proofoftech/breakwater';

const audit = new AuditLogger();
const agent = createGuardedAgent({
  id: 'guarded-agent',
  name: 'Guarded agent',
  instructions: 'Answer only from approved business data.',
  model: 'openai/gpt-5',
  allowedRoles: ['operator', 'admin'],
  policies: [
    denyPatterns(['private signing key']),
    piiSecrets({ detectors: ['privateKey', 'awsAccessKey', 'jwt'] }),
  ],
  audit,
  maxSteps: 4,
  toolChoice: 'auto',
});
```

The host must authenticate the caller and derive trusted request context before invoking the handle:

```typescript
const requestContext = new RequestContext();
requestContext.set(ACTOR_CONTEXT_KEY, {
  id: 'user-42',
  role: 'operator',
});
requestContext.set(AGENT_AUDIT_CONTEXT_KEY, {
  agentId: agent.id,
  runId: 'server_minted_run_id',
  entryPath: 'http-start',
});

const result = await agent.generate('Summarize the account.', {
  requestContext,
});
```

The handle exposes only unstructured `generate()` and `stream()`. Each call requires `requestContext` and may accept only `runId`, `memory`, and `abortSignal`. The factory fixes `maxSteps` and `toolChoice`, forces streaming policy hold-back, disables background continuations, and rejects processor or model overrides.

`allowedRoles` is an exact allowlist with no role hierarchy. Application input processors may implement only `processInput`. Application output processors must implement both `processOutputStream` and `processOutputResult`.

The narrow handle prevents accidental use of raw Mastra execution methods. It is not a sandbox against hostile code in the same JavaScript process. Use the authenticated Flowsafe agent host when callers cross an HTTP or tenant boundary.

## Choose agent policies

`PolicyEngine` accepts any `PolicyEvaluator`. Policies can select the
`input` or `output` phase and the output channels they inspect:

- `answer` is client-visible text and is also the only input channel.
- `reasoning` is the model reasoning trace.
- `object` is the latest JSON-stringified structured-output snapshot.

The included policies are:

| Policy | Default coverage | Purpose |
| --- | --- | --- |
| `denyPatterns(patterns)` | Input and output; answer, reasoning, and object | Deny case-insensitive string matches or caller-supplied regular expressions |
| `maxTextLength(maxChars)` | Output answer | Cap accumulated output length |
| `piiSecrets(options)` | Input and output; answer, reasoning, and object | Detect email, SSN, phone, Luhn-valid cards, AWS keys, PEM headers, JWTs, secret assignments, and high-entropy tokens |
| `classifierPolicy(options)` | Input and output answer | Delegate to a synchronous or asynchronous classifier, with cadence and timeout controls |

`piiSecrets()` supports detector selection, exact or regular-expression
allowlist exemptions, an entropy threshold, phase and channel selection, and a
hold-back hint override. Its detectors and `denyPatterns()` are best-effort
text inspection. Unicode tricks, alternate encodings, or deliberately shaped
secrets can evade pattern-based controls.

`classifierPolicy()` fails closed when the classifier throws or exceeds its
configured timeout. It has no automatic hold-back window because a classifier
has no bounded match length. To buffer a whole channel until classification
finishes, set `holdBackChars: Number.POSITIVE_INFINITY` on the returned
evaluator.

### Understand streaming hold-back

Without `holdBack`, each accumulated chunk is evaluated before that chunk is
emitted, but text from earlier chunks may already be visible when a later
chunk completes a forbidden pattern.

With `holdBack: true`, the engine retains each policy's declared trailing
window and releases only evaluated text. String patterns and the built-in
secret detectors provide bounded windows. A regular expression in
`denyPatterns()` defaults to buffering the full segment unless
`holdBackChars` supplies a safe bound. The guarantee is per stream segment;
text released at the end of an earlier segment cannot be withdrawn if a match
completes in a later segment.

Under `@mastra/core` 1.50, the `object` channel is available on
`agent.stream()` but not as a distinct field in the final non-streaming
`generate()` result. JSON carried as answer text is still inspected by
answer-inclusive policies. An object-only policy therefore requires an audit
sink, and the engine emits a one-time coverage warning on the non-streaming
path.

## Enforce tool permissions

Create tools through `createConnector()` when policy must hold on agent,
workflow, nested, and direct calls.

```typescript
import { createConnector } from '@proofoftech/breakwater/connector-sdk';
import { z } from 'zod';

const accountLookup = createConnector({
  id: 'crm.get-account',
  description: 'Read one CRM account',
  inputSchema: z.object({ accountId: z.string().min(1) }),
  outputSchema: z.object({ name: z.string() }),
  permissions: {
    sideEffect: 'read',
    egress: ['api.example-crm.com'],
  },
  policies: {
    networkEgress: { allowedDomains: ['api.example-crm.com'] },
  },
  execute: async ({ accountId }, _context, runtime) => {
    const response = await runtime.fetch(
      `https://api.example-crm.com/accounts/${accountId}`,
    );
    const account = (await response.json()) as { name: string };
    return { name: account.name };
  },
});
```

The permission manifest is enforced:

- `sideEffect` classifies the connector as `read`, `write`, `destructive`, or
  `idempotent`.
- `egress` lists exact hosts or leading `*.` wildcards. The organization
  allowlist gates the declaration, and `runtime.fetch` gates each actual
  request and redirect hop.
- `requiresApproval` makes the server-minted approval grant mandatory.
- `dryRun` requires `dryRunExecute` and allows callers to request a
  side-effect-free simulation.
- `idempotencyKey` requires a per-call key and a configured store.
- `rateLimit` declares a fixed-window budget and requires a configured store.
- `background` permits background overrides only for read-only connectors.
  Mastra still owns actual background-task eligibility.

Custom `ToolPolicyEvaluator` instances run before execution. Included
evaluators cover declared network egress, cross-workflow scope, required
tenant scope, and the direct-call background override defense. The
[connector authoring guide](./CONNECTORS.md) contains the complete manifest,
storage, invocation, egress, and testing contract.

## Set request context at trusted boundaries

The connector wrapper reads these keys:

| Constant | Runtime key | Value | Who should set it |
| --- | --- | --- | --- |
| `ACTOR_CONTEXT_KEY` | `breakwater.actor` | `{ id, role }` | Authenticated host or `getActor` |
| `APPROVED_CONNECTORS_CONTEXT_KEY` | `breakwater.approvedConnectors` | Connector ID array | Trusted approval service only |
| `DRY_RUN_CONTEXT_KEY` | `breakwater.dryRun` | `true` | Caller requesting simulation |
| `IDEMPOTENCY_KEY_CONTEXT_KEY` | `breakwater.idempotencyKey` | Non-empty string | Host-derived operation identity |
| `ISOLATION_SCOPE_CONTEXT_KEY` | `breakwater.isolationScope` | Opaque non-empty string | Multi-tenant runtime only |
| `WORKFLOW_SCOPE_CONTEXT_KEY` | `breakwater.workflowScope` | Current workflow ID | Workflow runtime only |

Approval and isolation values are capabilities. Never accept them from a
request body, model output, tool result, or client-controlled header.
flowsafe derives approval grants from approved records and mints workflow and
tenant scopes on each run leg.

Mastra's native `requireApproval` pauses an agent run, but it does not replace
the breakwater grant. Every execution path checks
`APPROVED_CONNECTORS_CONTEXT_KEY`.

## Choose replay and rate-limit stores

Development stores keep state in one JavaScript isolate:

- `InMemoryIdempotencyStore` provides atomic same-isolate reservations and
  bounded replay storage.
- `InMemoryRateLimitStore` provides fixed windows per isolate.

Production Cloudflare deployments can use:

- `D1IdempotencyStore`, which uses an atomic insert claim, lease tokens, and
  stale-pending takeover across isolates.
- `D1RateLimitStore`, which atomically increments shared fixed-window rows.

The store's reach is the control's reach. In a Durable Object per run,
in-memory rate limits become per-run budgets and in-memory replay protection
does not cross isolates. Use shared stores when the declared behavior must
hold across runs.

Set `D1IdempotencyStore.pendingTtlMs` above the longest possible execution.
The default is 900,000 ms. Agent CLI connectors reject a configured pending
TTL that is not greater than their execution timeout.

A fixed window can admit traffic on both sides of a boundary, approaching
twice the nominal count in a short interval. Use another `RateLimitStore`
implementation if you require token-bucket or GCRA semantics.

## Record audit and metrics

Every gate writes `AuditEvent` records with a timestamp, actor, action,
resource, decision, optional reason, and optional detail.

```typescript
import {
  AuditLogger,
  combineAuditSinks,
  metricsAuditSink,
} from '@proofoftech/breakwater/audit';

const audit = new AuditLogger({
  maxBuffered: 2_000,
  sink: combineAuditSinks(
    metricsAuditSink(metricsRecorder),
    async (event) => auditQueue.send(event),
  ),
  onSinkError: (error, event) => {
    reportAuditExportFailure(error, event.action);
  },
});
```

`AuditLogger` keeps an in-memory ring buffer, defaulting to 1,000 events.
Synchronous and asynchronous sink failures do not abort the guarded operation;
they are reported through `onSinkError`. `combineAuditSinks()` runs every sink
and aggregates failures. `metricsAuditSink()` emits the
`breakwater.audit.decision` counter and observes
`breakwater.audit.duration_seconds` when an event contains a finite,
non-negative `detail.durationSeconds`.

Generic connector exceptions are rethrown to the caller, but their arbitrary
messages are not copied into connector audit events. Audit reasons remain
static unless breakwater created a private, safe error summary.

For durable Cloudflare Queues to SIEM export, use the flowsafe
[`audit-export`](https://github.com/ProofOfTechOrg/anchorage/tree/main/packages/flowsafe/src/audit-export)
subpath.

## Run coding agents through the same gate

The agent CLI subpath wraps Claude Code and Codex as write-class,
approval-required connectors.

```typescript
import { RequestContext } from '@mastra/core/request-context';
import {
  createCodexConnector,
  DRY_RUN_CONTEXT_KEY,
} from '@proofoftech/breakwater';

const codex = createCodexConnector();
const requestContext = new RequestContext();
requestContext.set(DRY_RUN_CONTEXT_KEY, true);

const preview = await codex.execute?.(
  {
    prompt: 'Add unit tests.',
    cwd: '/srv/workspace',
    model: 'your-model-id',
  },
  { requestContext } as ToolExecutionContext,
);
```

The default runner is Node-only and:

- spawns an argv array without a shell;
- appends `--` and the real prompt as the final positional argument;
- selects workspace editing with Claude Code `acceptEdits` or Codex
  `workspace-write`;
- kills the child after the configured timeout;
- retains only the tail of stdout and stderr, capped by UTF-8 bytes;
- returns the CLI stdout as `text` and a prompt-redacted display `command`;
- exposes static `AgentCliError` messages plus structured, non-secret metadata.

The default timeout is 600,000 ms and the default retained output is 1 MiB per
stream. An injected `exec` implementation is responsible for its own sandbox,
timeout, and output limits.

The returned `text` is functional agent output and may contain sensitive data.
The `command` redacts the prompt and `--flag=value` option values. Validation
failures, error messages, error metadata, and
breakwater-generated audit reasons do not contain the prompt or captured
stdout and stderr.

The adapter does not sandbox the child. It inherits the parent environment and
credentials, and `cwd` is the workspace the CLI may modify. Its manifest
declares provider hosts, but a child process does not use
`ConnectorRuntime.fetch`; actual socket enforcement belongs in the container,
VM, or host firewall. See the [CLI section of the connector guide](./CONNECTORS.md#wrap-an-agent-cli)
before enabling real execution.

## Know the egress boundary

`runtime.fetch` accepts an absolute HTTP(S) URL string or URL object, not a
`Request`. It checks the initial host and every followed redirect. It strips
`authorization`, `cookie`, and `proxy-authorization` on a cross-origin
redirect, rewrites methods according to fetch redirect rules, refuses to
replay one-shot bodies across 307 or 308 redirects, and defaults to 20 hops.

This enforcement cannot see:

- global `fetch` calls made around `runtime.fetch`;
- vendor SDKs that do not accept the injected fetch;
- raw sockets or child-process network traffic.

Route every connector request through `runtime.fetch`. Use host-level network
controls when code outside that seam must also be constrained.

## Public API

The root entry point re-exports the supported APIs from every subpath.

### Policy engine exports

| Runtime exports | Purpose |
| --- | --- |
| `PolicyEngine` | Mastra input, stream-output, and final-output processor |
| `denyPatterns`, `maxTextLength`, `piiSecrets`, `classifierPolicy` | Included agent-boundary evaluators |
| `PII_SECRETS_DETECTOR_IDS` | Stable detector ID list |
| `extractMessageText` | Extract policy text from Mastra messages |
| `networkEgress`, `crossWorkflowIsolation`, `tenantIsolation`, `backgroundExecution` | Included tool-boundary evaluators |
| `approvalRequired` | Resolve approval from a manifest and organization policy |
| `egressDomainAllowed` | One-shot normalized exact or wildcard host match |
| `ISOLATION_SCOPE_CONTEXT_KEY`, `WORKFLOW_SCOPE_CONTEXT_KEY`, `LLM_BACKGROUND_OVERRIDE_KEY` | Stable scope and override keys |

Type exports: `PolicyEngineOptions`, `PolicyEvaluator`, `PolicyContext`,
`PolicyDecision`, `PolicyPhase`, `OutputChannel`, `PiiSecretsOptions`,
`PiiSecretsDetectorId`, `ClassifierPolicyOptions`, `ToolPolicyEvaluator`,
`ToolCallContext`, `SideEffect`, `NetworkEgressOptions`,
`CrossWorkflowIsolationOptions`, `BackgroundExecutionOptions`, and
`WritePermissionsPolicy`.

### RBAC and audit exports

| Runtime exports | Purpose |
| --- | --- |
| `RBACMiddleware`, `ROLES`, `ACTOR_CONTEXT_KEY`, `actorFromRequestContext` | Actor authorization and lookup |
| `AuditLogger`, `combineAuditSinks`, `metricsAuditSink` | Buffered audit, sink fan-out, and metrics adaptation |

Type exports: `Actor`, `Role`, `RBACMiddlewareOptions`, `AuditEvent`,
`AuditSink`, `AuditLoggerOptions`, and `MetricsRecorder`.
The `rbac` subpath also re-exports `AuditLogger` and its original audit types
for compatibility.

### Connector SDK exports

| Runtime exports | Purpose |
| --- | --- |
| `createConnector`, `connectorManifest` | Build an enforced Mastra tool and inspect its immutable manifest |
| `ConnectorPolicyError` | Structured policy denial |
| `APPROVED_CONNECTORS_CONTEXT_KEY`, `DRY_RUN_CONTEXT_KEY`, `IDEMPOTENCY_KEY_CONTEXT_KEY` | Stable connector request-context keys |
| `InMemoryIdempotencyStore`, `D1IdempotencyStore` | Development and durable replay stores |
| `InMemoryRateLimitStore`, `D1RateLimitStore` | Development and durable fixed-window stores |
| `egressFetch`, `EgressDeniedError` | Standalone fetch guard and its default denial |

Type exports: `PermissionManifest`, `ConnectorConfig`, `ConnectorPolicies`,
`ConnectorRuntime`, `IdempotencyStore`, `AtomicIdempotencyStore`,
`IdempotencyRecord`, `IdempotencyReservation`, `RateLimitStore`,
`D1IdempotencyStoreOptions`, `IdempotencyDatabase`,
`IdempotencyStatement`, `D1RateLimitStoreOptions`, `RateLimitDatabase`,
`RateLimitStatement`, `EgressDenial`, `EgressFetchOptions`,
`EgressFetchBase`, `EgressGuardedFetch`, `EgressRequestInit`,
`EgressResponse`, and `EgressResponseHeaders`.

### Agent CLI exports

| Runtime exports | Purpose |
| --- | --- |
| `createAgentCliConnector` | Wrap another positional-prompt CLI |
| `createClaudeCodeConnector`, `createCodexConnector` | Built-in adapters |
| `CLAUDE_CODE_CLI`, `CODEX_CLI` | Reusable adapter definitions |
| `AgentCliError` | Safe structured execution failure |

Type exports: `AgentCliInput`, `AgentCliOutput`, `AgentCliExec`,
`AgentCliExecResult`, `AgentCliDefinition`, `AgentCliConnectorOptions`,
`AgentCliErrorCode`, and `AgentCliErrorMetadata`.

## Read the design and operational docs

- [Connector authoring guide](./CONNECTORS.md)
- [Connector enforcement contract](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/connector-interface.md)
- [breakwater architecture](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/breakwater-architecture.md)
- [Policy engine design](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/policy-engine-design.md)
- [Security threat model](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/security-threat-model.md)
- [Release notes](./CHANGELOG.md)
- [Repository contribution guide](https://github.com/ProofOfTechOrg/anchorage/blob/main/CONTRIBUTING.md)

## Verify a checkout

```bash
pnpm --filter @proofoftech/breakwater lint
pnpm --filter @proofoftech/breakwater typecheck
pnpm --filter @proofoftech/breakwater test
pnpm --filter @proofoftech/breakwater build
pnpm --filter @proofoftech/breakwater test:packed-consumer
```

Apache-2.0. See [LICENSE](./LICENSE).
