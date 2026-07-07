# Connector Interface

Connectors integrate workflow tasks with business systems. Built on Mastra's `createTool()` -- each connector is a tool plus a permission manifest declaring its safety properties. Connectors are separate from agent adapters because connector behavior is structured, scoped, and side-effectful.

## Connector as Mastra Tool

Mastra's `createTool()` (`@mastra/core/tools`) has no permission-manifest field, so the manifest cannot ride on the tool config itself. Breakwater's connector SDK provides `createConnector()`, a wrapper factory: the tool fields pass through to a real `createTool()` call, while the `permissions` manifest is stripped and enforced by wrapping `execute` (network egress, idempotency, dry-run, rate limiting). `requiresApproval` compiles to Mastra's native `requireApproval` tool option.

```typescript
import { z } from 'zod';
import { createConnector } from '@proofoftech/breakwater/connector-sdk';

// Compiles to a real Mastra createTool() call. `permissions` is the
// Anchorage manifest: stripped before the tool is created, enforced by
// wrapping execute; requiresApproval maps to Mastra's requireApproval.
export const createContact = createConnector({
  id: 'salesforce.createContact',
  description: 'Create a Salesforce contact',
  inputSchema: z.object({ name: z.string(), email: z.string() }),
  outputSchema: z.object({ id: z.string() }),
  execute: async (inputData, context) => ({ id: '003...' }),
  // Anchorage extension -- permission manifest (not a createTool() field)
  permissions: {
    sideEffect: 'write',
    egress: ['api.salesforce.com'],
    idempotencyKey: true,
    dryRun: true,
    rateLimit: '100/min',
    requiresApproval: true,
  },
});
```

What Mastra provides vs. what breakwater adds: Mastra `createTool()` takes `id`, `description`, `inputSchema`/`outputSchema`, `execute(inputData, context)`, optional suspend/resume schemas, and a native `requireApproval` option (boolean or per-call predicate); MCP-style annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`) exist only as descriptive MCP metadata that nothing enforces. Breakwater adds the enforced manifest: side-effect classification evaluated by the policy engine, idempotency-key storage, dry-run simulation, and rate limiting.

## Permission Manifest

| Field | Type | Values |
|---|---|---|
| `sideEffect` | enum | `read`, `write`, `destructive`, `idempotent` |
| `egress` | string[] | Hostnames the connector calls (`api.example.com`); checked against the org `networkEgress` allowlist |
| `idempotencyKey` | boolean | Requires caller to provide an idempotency key |
| `dryRun` | boolean | Supports simulation without side effects |
| `rateLimit` | string | Connector-level rate limit expression |
| `requiresApproval` | boolean | Gates execution on human approval (compiled to a Mastra-native `requireApproval` per-call predicate that exempts dry-run requests) |

Every manifest field is implemented and enforced: `sideEffect`, `egress`, `idempotencyKey`, `requiresApproval`, `dryRun`, and `rateLimit`.

- **`dryRun`** declares the connector supports side-effect-free simulation and
  requires `ConnectorConfig.dryRunExecute` (same output shape as `execute`);
  declaring one without the other is a definition-time `TypeError`. A caller
  requests a simulation per call by setting requestContext
  `breakwater.dryRun` (`DRY_RUN_CONTEXT_KEY`) to `true`: pre-execute gates
  (egress + custom evaluators) still apply, then the wrapper skips the
  approval grant, rate-limit consumption, and all idempotency machinery (no
  side effect to protect; a simulation must not poison the replay store) and
  runs `dryRunExecute`, auditing `{ dryRun: true }`. Mastra's native approval
  pause is skipped too: the compiled `requireApproval` predicate returns
  false for dry-run requests on the standard agent path (runtime paths that
  evaluate the predicate without a context — network tool execution, the
  durable agent — still pause, fail closed). A dry-run request
  against a connector without `dryRun` support is DENIED (policy `dry-run`),
  never silently executed for real.
- **`rateLimit`** is `'<count>/<unit>'` — count >= 1, unit one of the singular
  `s|sec|second|m|min|minute|h|hour|d|day` (e.g. `'100/min'`); malformed
  expressions are a definition-time `TypeError`, and the manifest requires
  `policies.rateLimitStore` (`InMemoryRateLimitStore` for dev/tests).
  Semantics are FIXED WINDOWS (epoch-aligned buckets; a burst may span two
  adjacent windows), and the budget counts actual executions only: calls
  denied by other gates, cached replays, and shared in-flight joins never
  consume it — which is why enforcement is an internal gate immediately
  before `execute`, not a pre-execute evaluator. Over-budget calls deny with
  policy `rate-limit`.

## Enforcement

`createConnector()` binds org policy at definition time
(`policies: { networkEgress, writePermissions, evaluators, idempotencyStore,
rateLimitStore, audit }`) and wraps `execute` with the gates above plus
three more, in order:

1. **Network egress** -- every manifest-declared `egress` hostname must match
   the org allowlist (exact or `*.wildcard`, subdomains only). Declaration-based:
   it guards against misconfiguration and policy drift, not a connector that
   lies about its egress surface.
2. **Write approval** -- `approvalRequired()` (manifest `requiresApproval`,
   destructive-by-default, or org `writePermissions.requireApproval` connector-id
   globs). Enforcement is the wrapper's grant check on **every** path: the call
   is denied unless the request carries a grant -- requestContext
   `breakwater.approvedConnectors` containing the connector id. The predicate
   also compiles to Mastra's native `requireApproval` -- a per-call predicate
   that requires approval unless the request is a dry-run (simulations never
   reach a side effect, so there is nothing to approve) -- so agent runs pause
   for a decision and an unapproved resume never reaches `execute` -- but the
   native outcome never substitutes for the grant: the runtime strips the pure
   `{approved: true}` resume before invoking the tool (no in-band signal at
   execute time), and an agent-shaped context is forwardable into nested or
   direct calls, so `context.agent` proves nothing about approval. Whatever
   approves a call must mint the grant into the requestContext the resumed
   call executes under; the flowsafe approval API does this on
   resume. Minting code is part of the trusted computing base: the grant is a
   capability token, and requestContext must never be populated from client
   input, model output, or tool results (see `security-threat-model.md`,
   trust boundary 6). Grants are checked on every attempt: a retried or
   resumed call must carry the grant again, or it fails closed.
3. **Idempotency** -- the per-call key comes from requestContext
   `breakwater.idempotencyKey`; results are stored per `${connectorId}:${key}`.
   A stored key replays the stored result without re-executing; concurrent
   duplicates share one attempt; failures are never cached, so retries
   re-execute. Nothing injects keys on the agent path, so
   idempotency-keyed connectors fail closed under an agent until the flowsafe
   runtime supplies keys.

Custom `ToolPolicyEvaluator`s registered via `policies.evaluators` run
pre-execute after the built-in egress gate, in order -- the slot later policy
domains (retention/isolation pre-checks) plug into.

Denials throw `ConnectorPolicyError` (`connector`, `policy`, `reason`); every
decision is recorded to the shared `AuditLogger` as action `connector.execute`.
The manifest also compiles to truthful MCP annotations (`readOnlyHint`,
`destructiveHint`, `idempotentHint`, `openWorldHint`) -- descriptive metadata;
enforcement stays in the wrapper.

## Idempotency

Write connectors must bind idempotency storage
(`policies.idempotencyStore`; `createConnector()` rejects an
`idempotencyKey: true` manifest without one). The breakwater/flowsafe runtime
provides the idempotency key via requestContext -- Mastra has no idempotency
machinery, only the descriptive MCP `idempotentHint` annotation; the wrapper
stores the result by key and returns the stored result on replay. This
prevents duplicate side effects across retries and DO lifecycle boundaries.
The store interface grew the atomic reserve the durable path required:
`AtomicIdempotencyStore` extends `get`/`put` with `reserve(key)` — a
compare-and-set claim returning `'reserved'` (this caller executes),
`'replay'` (a completed record to return), or `'pending'` (another isolate is
executing; the call denies honestly and a retry replays the winner's result)
— and `release(key)` so failed executions stay retryable. The wrapper prefers
the reserve path whenever a store implements it; plain `get`/`put` stores
keep the legacy same-isolate-only flow. `D1IdempotencyStore` is the shipped
durable implementation (structural D1 typing — no `@cloudflare/workers-types`
dependency): the claim is `INSERT ... ON CONFLICT DO NOTHING RETURNING`, and
a stale-pending takeover (default TTL 300s; must exceed the longest expected
execute) recovers keys a crashed isolate would otherwise poison forever.
`InMemoryIdempotencyStore` implements the same atomic shape for dev/tests.
