# Authoring connectors

A breakwater connector is a Mastra tool created with `createConnector()`
instead of `createTool()`. It has the same input, output, and execution model,
plus a permission manifest that the wrapper enforces on agent, workflow,
nested, and direct calls.

Use a connector for any tool that reads external data, changes state, calls a
network service, or needs a durable retry or execution budget. The manifest is
not documentation metadata. A mismatch between the manifest and the
implementation is a security defect.

The authoritative lower-level contract is the
[connector interface](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/connector-interface.md).
This guide explains how to implement and operate it.

## Install the authoring dependencies

```bash
npm install @proofoftech/breakwater @mastra/core zod
```

Import from the focused subpath:

```typescript
import {
  createConnector,
  type IdempotencyStore,
  type RateLimitStore,
} from '@proofoftech/breakwater/connector-sdk';
```

## Build a complete connector

This example declares every control required for a write connector with
network access, approval, idempotency, dry-run support, and a rate limit.
Stores are injected because their lifecycle is a deployment decision.

```typescript
import type { AuditLogger } from '@proofoftech/breakwater/audit';
import {
  createConnector,
  type IdempotencyStore,
  type RateLimitStore,
} from '@proofoftech/breakwater/connector-sdk';
import { z } from 'zod';

interface SlackPosterOptions {
  audit?: AuditLogger;
  idempotencyStore: IdempotencyStore;
  idempotencyKeyMigration: 'legacy-writers-drained';
  rateLimitStore: RateLimitStore;
  webhookHost?: string;
}

export function createSlackPoster(options: SlackPosterOptions) {
  const webhookHost = options.webhookHost ?? 'hooks.slack.com';

  return createConnector({
    id: 'slack.post-message',
    description: 'Post a message through a Slack incoming webhook',
    inputSchema: z.object({
      webhookPath: z.string().startsWith('/services/'),
      text: z.string().min(1),
    }),
    outputSchema: z.object({
      delivered: z.boolean(),
      simulated: z.boolean().optional(),
    }),
    permissions: {
      sideEffect: 'write',
      egress: [webhookHost],
      requiresApproval: true,
      idempotencyKey: true,
      dryRun: true,
      rateLimit: '60/min',
    },
    policies: {
      audit: options.audit,
      idempotencyStore: options.idempotencyStore,
      idempotencyKeyMigration: options.idempotencyKeyMigration,
      rateLimitStore: options.rateLimitStore,
      networkEgress: {
        allowedDomains: ['hooks.slack.com'],
      },
    },
    execute: async (input, _context, runtime) => {
      const response = await runtime.fetch(
        `https://${webhookHost}${input.webhookPath}`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ text: input.text }),
        },
      );
      if (!response.ok) {
        throw new Error(`Slack webhook returned ${response.status}`);
      }
      return { delivered: true };
    },
    dryRunExecute: async () => ({
      delivered: false,
      simulated: true,
    }),
  });
}
```

Keep connector IDs stable and colon-free. Dotted IDs such as
`slack.post-message` work with approval globs and cannot collide with the
unchanged `[scope:]connector` rate-limit keys. `createConnector()` rejects an
ID containing `:`. Idempotency uses a separate collision-proof v2 encoding.

## Understand the execution order

Mastra validates the input schema before the connector wrapper runs. The
wrapper then applies:

1. The foreground-only `_background` override check.
2. The organization egress policy and custom evaluators, in registration
   order.
3. The required-permissions authorization check, when declared.
4. The dry-run branch, when requested.
5. The approval grant check.
6. The idempotency lookup or atomic reservation, when declared.
7. The rate-limit increment for an actual execution.
8. Your `execute()` function.
9. Per-request and per-redirect host checks whenever `runtime.fetch()` runs.
10. Wrapper output-schema validation and transformation before a replay result
    is committed.
11. The idempotency result commit for a valid fresh keyed result.
12. Mastra consumes that captured Standard Schema result without rerunning a
    stateful validator.

New v2 replay records store that exact validated/transformed public result, so
a replay does not execute or transform it again. A safe legacy v1 result still
crosses the schema boundary once because it predates this invariant.

Dry runs pass through the pre-execute evaluators and the
required-permissions check but skip approval, idempotency, and rate-limit
consumption. Replays and same-isolate in-flight joins do not consume rate
budget.

An execution or rate-limit failure before a successful side effect releases an
owned atomic reservation. If output validation fails after `execute()`
returns, the result is not committed and the reservation stays pending until
stale takeover or operator recovery; releasing it immediately could duplicate
the completed side effect.

Calls that reach the wrapper record the relevant allow, deny, or failure events
through the configured `AuditLogger`. Secondary events can also report a
degraded store or a stale idempotency reservation takeover.

## Declare the manifest honestly

| Field | Meaning | Enforced behavior |
| --- | --- | --- |
| `sideEffect` | The worst state change the connector can cause | `read` is read-only. `write`, `destructive`, and `idempotent` are write-class. `destructive` requires approval by default. Mastra MCP hints are derived from this value. |
| `egress` | Every hostname the connector contacts | Entries must be bare hosts or leading `*.` wildcards. The organization policy gates the declared list. `runtime.fetch` gates actual HTTP(S) requests and redirect hops against the declaration. An empty or absent list means no network through that fetch. |
| `egressEnforcement` | Whether the declared list binds actual traffic | `enforced` asserts every HTTP request leaves through `runtime.fetch`, and covers a connector that issues no HTTP request at all: it is a claim about HTTP traffic, not about platform bindings (D1, KV, R2, service bindings), which the guard never sees. `declaration-only` states a vendor SDK or child process carries its own transport. An omitted field resolves to `declaration-only`. `connectorEgressPosture()` reads the resolved value and every connector audit event carries it. |
| `requiresApproval` | This connector always needs human approval | Real execution requires a matching structured grant in `breakwater.connectorGrants`, regardless of call path. Mastra's native approval pause is also enabled, but the grant remains the authorization token. |
| `dryRun` | A side-effect-free simulation exists | Requires `dryRunExecute`. The wrapper rejects a `dryRunExecute` that the manifest does not declare. A dry-run request never falls through to real execution. |
| `idempotencyKey` | Repeated operation identities must replay | Requires `policies.idempotencyStore` and a non-empty `breakwater.idempotencyKey` for each real call. |
| `rateLimit` | Fixed-window execution budget | Uses `<count>/<unit>`, where unit is `s`, `sec`, `second`, `m`, `min`, `minute`, `h`, `hour`, `d`, or `day`. Requires `policies.rateLimitStore`. |
| `background` | The connector permits a model background override | Allowed only on `read` connectors. Write-class connectors are foreground-only in v1. This permission does not itself configure Mastra background-task eligibility. |
| `requiredPermissions` | Server-derived permissions the principal must hold to invoke the connector at all | All-of list of canonical dotted identifiers, validated at construction. Every call — dry-runs included — requires a trusted `breakwater.principalPermissions` projection holding every identifier, checked before the approval grant so an approval cannot elevate an unauthorized principal. Missing or malformed projections fail closed. |

Classify by the worst operation reachable from `execute()`. A create-or-replace
operation is destructive if it can overwrite existing state. An idempotent
write is still write-class; `idempotent` describes safe repetition, not lack
of side effects.

`connectorManifest(tool)` returns the immutable manifest associated with a
breakwater connector, or `undefined` for another Mastra tool. The wrapper also
derives MCP `readOnlyHint`, `destructiveHint`, `idempotentHint`, and
`openWorldHint` annotations from this manifest. These hints describe the tool;
the wrapper remains the enforcement boundary.

`connectorEgressPosture(tool)` reads the resolved egress posture, defaulting to
`declaration-only` when the manifest omits it and returning `undefined` for
a tool that `createConnector()` did not build.

`background: true` only tells the breakwater wrapper that a read connector can
accept background intent. Mastra owns whether an agent or tool is eligible for
background execution. On the normal agent path, Mastra removes `_background`
from tool arguments before dispatch and rejects backgrounding a tool that has
not been enabled. The breakwater field and `backgroundExecution()` evaluator
provide defense in depth for direct or nested calls whose raw arguments still
contain `_background`.

## Configure policies and stores

`ConnectorPolicies` supplies the deployment environment:

| Policy field | Purpose | Required when |
| --- | --- | --- |
| `networkEgress` | Organization allowlist for declared connector hosts | Optional. Omit it for no organization-level declaration gate. |
| `writePermissions` | Connector-ID globs that require approval, plus the destructive default | Optional. `permissions.requiresApproval` works without it. |
| `evaluators` | Additional `ToolPolicyEvaluator` instances | Optional. They run after `networkEgress` and before all execution branches. |
| `idempotencyStore` | Replay records and atomic reservations | `permissions.idempotencyKey` is true. |
| `idempotencyKeyMigration` | Acknowledge that legacy writers sharing the store are stopped and drained | A missing legacy record may execute under the v2 key format. |
| `rateLimitStore` | Atomic fixed-window counters | `permissions.rateLimit` is present. |
| `audit` | Structured decision sink | Optional but recommended for every production deployment. |
| `fetch` | Base fetch wrapped by `runtime.fetch` | Optional. Inject vendor mocks in tests or a platform fetch in nonstandard runtimes. |
| `requireEgressEnforcement` | Refuse a connector whose posture is not `enforced` | Optional. Set it where the guarded fetch is the only network boundary. |

The included tool evaluators are:

- `networkEgress()` denies a declaration outside an organization allowlist.
- `crossWorkflowIsolation()` compares a connector-specific target with the
  runtime-minted workflow scope.
- `tenantIsolation()` denies calls without a non-empty isolation scope,
  including dry runs.
- `backgroundExecution()` denies direct or nested write-class calls that try
  to enable `_background`.

`approvalRequired()` is the shared resolver used by the wrapper. It combines
the connector's `requiresApproval`, destructive-by-default behavior, and
organization `requireApproval` globs.

`WritePermissionsPolicy.requireApproval` accepts connector-ID patterns whose
only wildcard token is `*`, such as `salesforce.*`. Every other character is
literal. `destructiveRequiresApproval` defaults to `true`; set it to `false`
only when another explicit policy owns every destructive call.

## Invoke a connector from a trusted host

Direct and workflow calls must carry the same context that an approved agent
resume would receive.

```typescript
import { RequestContext } from '@mastra/core/request-context';
import {
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
  invokeConnector,
} from '@proofoftech/breakwater/connector-sdk';
import { WORKFLOW_SCOPE_CONTEXT_KEY } from '@proofoftech/breakwater/policy-engine';
import { PRINCIPAL_PERMISSIONS_CONTEXT_KEY } from '@proofoftech/breakwater/rbac';

const requestContext = new RequestContext();
requestContext.set(WORKFLOW_SCOPE_CONTEXT_KEY, 'incident-response');
requestContext.set('runId', 'server_minted_run_id');
const suspension = {
  stepPath: ['approve-notice'],
  suspendedAt: 1751882400000,
};
requestContext.set(CONNECTOR_EXECUTION_CONTEXT_KEY, {
  kind: 'resume',
  workflowId: 'incident-response',
  runId: 'server_minted_run_id',
  suspension,
});
requestContext.set(CONNECTOR_GRANTS_CONTEXT_KEY, [{
  scope: 'suspension',
  connectorId: 'slack.post-message',
  workflowId: 'incident-response',
  runId: 'server_minted_run_id',
  suspension,
}]);
// Only when the connector declares requiredPermissions: the executing
// principal's server-resolved permissions and the policy snapshot version.
requestContext.set(PRINCIPAL_PERMISSIONS_CONTEXT_KEY, {
  permissions: ['incidents.notify'],
  policyVersion: 'access-policy-2026-08',
});
requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, 'incident-481:notice');

const result = await invokeConnector(
  slackPoster,
  {
    webhookPath: '/services/...',
    text: 'Incident 481 is being investigated.',
  },
  { requestContext },
);
```

`invokeConnector()` accepts only an unmodified connector returned by `createConnector()`. It calls the connector's public Mastra execution boundary, so input and output schemas and every Breakwater gate still run. It creates the required observation context and forwards an optional `RequestContext`, abort signal, observer, and trusted tool-call ID.

This example shows the values a trusted runtime must produce. Application routes must not construct them from client data. Do not accept `CONNECTOR_GRANTS_CONTEXT_KEY`, `CONNECTOR_EXECUTION_CONTEXT_KEY`, `PRINCIPAL_PERMISSIONS_CONTEXT_KEY`, `ISOLATION_SCOPE_CONTEXT_KEY`, `runId`, or `WORKFLOW_SCOPE_CONTEXT_KEY` from clients. A multi-tenant host should also register `tenantIsolation()` so a missing scope becomes a denial instead of a shared cache or budget.

For a `tool-call` grant, pass the exact runtime-owned identity separately:

```typescript
await invokeConnector(connector, input, {
  requestContext,
  toolCallId: trustedToolCallId,
});
```

Do not store this value in `RequestContext` or build a partial Mastra agent context. Concurrent calls can share a `RequestContext` while carrying different tool-call identities. A validation failure throws `ConnectorValidationError` with a stable kind/code, connector ID and phase, `input` or `output`. It never exposes Mastra's message, schema issue text, invalid value, or cause.

To request simulation:

```typescript
import {
  DRY_RUN_CONTEXT_KEY,
} from '@proofoftech/breakwater/connector-sdk';

requestContext.set(DRY_RUN_CONTEXT_KEY, true);
```

A dry-run call does not need an approval grant or idempotency key. It still
passes the organization egress and custom evaluator gates, and — when the
manifest declares `requiredPermissions` — the authorization gate: a
simulation still needs an authorized principal.

## Use the right idempotency store

`InMemoryIdempotencyStore` is bounded, atomic within one isolate, and suitable
for tests or a single long-lived process. It cannot protect a write across
isolates or restarts. Its optional `maxEntries` defaults to 1,000 completed
records; pending reservations are not evicted.

`D1IdempotencyStore` provides:

- lazy table creation;
- an atomic insert claim so only one isolate reserves a key;
- an opaque lease token on finalize and release;
- completed-result replay;
- stale-pending takeover after a crashed holder.

```typescript
import {
  D1IdempotencyStore,
  D1RateLimitStore,
  inspectLegacyConnectorIdempotency,
  migrateLegacyConnectorIdempotency,
} from '@proofoftech/breakwater/connector-sdk';

const idempotencyStore = new D1IdempotencyStore(env.DB, {
  table: 'breakwater_idempotency',
  pendingTtlMs: 15 * 60 * 1_000,
});
const rateLimitStore = new D1RateLimitStore(env.DB, {
  table: 'breakwater_rate_limit',
});
```

`pendingTtlMs` must exceed the longest real execution. A takeover that fires
while the original holder is still running can duplicate a write. The default
is 900,000 ms. The Agent CLI wrapper checks this against its own timeout when
both idempotency and a store exposing `pendingTtlMs` are configured.
The constructor accepts only positive safe integers up to
8,640,000,000,000,000 ms.
`D1IdempotencyStoreOptions.now` is a clock override for deterministic tests;
production should use the default clock.

Durable custom stores must implement `AtomicIdempotencyStore` and
`InspectableIdempotencyStore`. The non-mutating `inspect()` call distinguishes
an absent legacy key from pending work during migration. A durable get-then-put
implementation can let two isolates miss and execute the same key at once.

Store failure handling is designed around side-effect safety:

- A read or reservation failure occurs before execution and fails closed.
- A final `put()` failure after the side effect is audited, but the successful
  result is returned. Failing the call would invite a duplicate retry.
- A reservation release failure is audited and recovered through stale
  takeover.

`D1IdempotencyStore` accepts JSON-native results plus the established top-level
`undefined` result. It rejects values that JSON would silently change, such as
`Date`, `Map`, repeated object references, non-finite numbers, sparse arrays,
or nested `undefined`, before finalizing the row. The wrapper returns the
successful public result, audits the failed `put()`, and leaves an atomic
reservation pending instead of storing a replay with a different type or
structure.

New idempotency records use an opaque, versioned composite key. Do not construct
or parse it. The previous `[isolationScope:]connectorId:idempotencyKey` form is
read only during migration. An unscoped legacy key without a colon remains
safe to replay. Every scoped legacy key and every unscoped business key that
contains a colon is ambiguous and fails closed instead of replaying into a
possibly different identity.

Before setting `idempotencyKeyMigration: 'legacy-writers-drained'`, stop and
drain every old writer sharing the store. Inventory legacy rows, and map any
ambiguous row to exactly one business identity using external evidence. Leave
an unproven row in place: affected calls continue to fail closed. Without the
acknowledgement, an absent legacy key cannot execute because an old writer
could still create it after inspection.

For the shipped D1 store, inventory and migrate through the connector-bound
helpers instead of constructing storage keys or writing ad hoc SQL:

```typescript
const identity = {
  idempotencyKey: 'invoice:2026-08-12',
  isolationScope: 'acme',
};
const inventory = await inspectLegacyConnectorIdempotency(connector, identity);

if (inventory.state === 'replay') {
  const migrated = await migrateLegacyConnectorIdempotency(connector, {
    ...identity,
    expectedRecord: inventory.record,
  });
}
```

The operator must first prove from business or audit evidence that exactly this
tuple owns the ambiguous row. `expectedRecord` binds the write to the inventoried
value but cannot prove ownership. The helper validates and transforms the legacy
result through the connector's output schema, guards the exact source value,
writes the validated value under v2, and deletes v1 in one D1 transaction. A
pending, changed, unproven, invalid, or conflicting row remains in place.
Repeating a completed migration returns `already-migrated`.

Breakwater stores only `{ result }`. It does not compare a retried request body
with the original input. A gateway that promises mismatch detection must own a
canonical request representation and store its fingerprint atomically with the
idempotency result. The same key and fingerprint may replay; the same key with
a different fingerprint must be rejected by that gateway.

## Use the right rate-limit store

`InMemoryRateLimitStore` counts one epoch-aligned fixed window in one isolate.
`D1RateLimitStore` shares atomic counters across every isolate using the same
database.

Only actual execution consumes budget. Denials, dry runs, stored replays, and
same-isolate in-flight joins do not increment it. A rate-store failure denies
execution rather than allowing an unbudgeted call.

Rate-limit counts must be safe integers from 1 through
`Number.MAX_SAFE_INTEGER`. `D1RateLimitStore` batches the increment and expired
window cleanup in one D1 transaction. A cleanup failure therefore rejects
before quota commits, and a retry starts at the correct count.

A custom structural `RateLimitDatabase` adapter must provide D1-compatible
transactional `batch()` behavior. Sequentially executing its statements without
rollback does not satisfy the store contract.

The store's reach is the budget's reach. Under one Durable Object per run, an
in-memory limit is effectively per run. Use D1 or another shared implementation
for a tenant-wide or deployment-wide budget.

Fixed windows are not hard rolling caps. A burst can use one window immediately
before a boundary and another immediately after it. Implement
`RateLimitStore` with another algorithm if you require token-bucket or GCRA
semantics.

## Route all HTTP through the guarded fetch

The declaration gate and actual-request gate form this invariant:

```text
actual host through runtime.fetch
  ⊆ connector manifest egress
  ⊆ organization allowed domains
```

`runtime.fetch`:

- accepts an absolute HTTP(S) URL string or URL object, not a `Request`;
- validates exact hosts and leading `*.` wildcards on DNS label boundaries;
- follows redirects manually and validates every hop;
- strips `authorization`, `cookie`, and `proxy-authorization` across origins;
- applies normal 301, 302, and 303 method rewriting;
- preserves bodies for 307 and 308 only when they can be replayed;
- refuses one-shot stream bodies on a 307 or 308 redirect;
- defaults to 20 redirect hops and permits a lower non-negative limit through
  standalone `egressFetch()`.

Standalone `egressFetch(allowedHosts, options)` accepts an injected base
`fetch`, a denial-to-error mapper, and `maxRedirects`. The guarded fetch returns
the base response object unchanged, so runtime-specific response members remain
available on the underlying value even though the portable TypeScript surface
models only common response methods.

If `redirect` is `manual`, the caller receives the 3xx and any follow-up fetch
must go through the guard again. If it is `error`, the base fetch owns the
redirect failure.

The guard cannot see global `fetch`, a vendor SDK with its own transport, a raw
socket, or child-process traffic. Pass `runtime.fetch` into SDKs that support a
custom fetch or transport. Use a container, VM, or network policy when traffic
outside this seam must also be denied.
Declare `egressEnforcement: 'declaration-only'` when traffic bypasses the
guard; the posture makes that degradation explicit and auditable.

## Handle errors and audit safely

Use `ConnectorPolicyError.code` for machine handling and keep `policy`/`reason` for diagnostics. The error also exposes a stable kind, canonical `policyKind`, retryability and code-specific safe details. Custom policy names and the three-string constructor remain supported. The legacy constructor uses `EVALUATOR_DENIED`; it does not infer a code from the name.

Read the [decision-code catalogue](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/connector-interface.md#connector-decision-codes) for coverage and retry semantics. `CONNECTOR_DECISIONS`, `isConnectorDecisionCode` and `connectorDecisionRetryable` are exported from the SDK and root entries. SDK audit records carry matching `decisionCode`, `policyKind` and `retryable` fields.

Before execution, rate-limit and idempotency store failures throw `ConnectorStoreError` with `STORE_UNAVAILABLE`, the actual operation, and the original exception on native `cause`. Post-effect commit and best-effort release failures remain audited and suppressed; their codes are non-retryable. Evaluator failures use `ConnectorEvaluatorError`. The Agent CLI adapter preserves these classifications while omitting raw causes.

Standalone `egressFetch()` emits coded `EgressDeniedError` and redirect `EgressGuardError` values. A custom `denied()` mapper receives the code and owns the error it returns.

The connector wrapper rethrows errors from your `execute()` implementation so
your caller can handle the original failure. It does not copy arbitrary thrown
messages into audit events. Generic execution failures use a static audit
reason. Built-in errors can register a private safe summary containing only
approved structured fields.

This boundary does not sanitize errors emitted by your application after they
leave the connector. Do not put secrets in thrown messages, connector IDs,
policy names, idempotency keys, or custom audit detail.

The egress denial object includes only the host and hop, never the full URL,
because paths and query strings often contain credentials.

## Wrap an agent CLI

Use `createAgentCliConnector()` for a positional-prompt CLI. The package
includes `createClaudeCodeConnector()` and `createCodexConnector()`.

```typescript
import {
  createAgentCliConnector,
  type AgentCliDefinition,
} from '@proofoftech/breakwater/agent-cli';

const MY_CLI: AgentCliDefinition = {
  id: 'agent-cli.my-cli',
  description: 'Run My CLI non-interactively',
  binary: 'my-cli',
  egress: ['api.example.com'],
  buildFlags: (input) => [
    'run',
    ...(input.model ? [`--model=${input.model}`] : []),
  ],
  parseOutput: (stdout) => stdout,
};

const myCli = createAgentCliConnector(MY_CLI);
```

The callback receives a frozen copy of `AgentCliInput` whose `prompt` is
`<prompt:redacted>`. Return subcommands and option flags only. The wrapper
appends the real prompt as:

```text
-- <real prompt>
```

This keeps a prompt beginning with `-` from becoming a flag. Bind
caller-controlled option values in one token, such as `--model=value`, for the
same reason. The returned display command redacts the prompt and every
`--flag=value` option value. The real argv still carries the original values.

The built-in definitions execute:

```text
claude -p --output-format=json --permission-mode=acceptEdits [--model=value] -- <real prompt>
codex exec --sandbox=workspace-write [--model=value] -- <real prompt>
```

Claude Code parses the JSON `result` field and falls back to raw stdout if the
envelope changes. Codex returns raw stdout.

The built-in definitions deliberately select workspace-edit permissions.
Claude Code uses `acceptEdits`; Codex uses `workspace-write`. These are coding
agents, so a read-only default would contradict the connector's purpose. The
flags do not sandbox the child process from the rest of its host.

`AgentCliConnectorOptions` exposes the whole adapter configuration:

| Option | Behavior |
| --- | --- |
| `exec` | Replace the default child-process runner with a container, remote executor, or test seam |
| `binaryPath` | Replace the definition's binary name with a path or alternate command |
| `timeoutMs` | Set the execution deadline, default 600,000 ms |
| `maxOutputBytes` | Set the retained stdout and stderr tail per stream, default 1 MiB; applies only to the default runner |
| `requiresApproval` | Override the default `true` approval requirement |
| `rateLimit` | Add the connector manifest fixed-window budget |
| `idempotencyKey` | Require keyed replay and a policy store |
| `id` | Override the connector ID for parallel configurations |
| `policies` | Supply audit, organization policies, evaluators, and stores to `createConnector()` |

### Know the CLI runtime boundary

The default runner:

- requires Node.js and resolves `node:child_process` at execution time;
- spawns without a shell;
- inherits the parent process environment and CLI authentication;
- runs in the caller-supplied `cwd`;
- terminates the process tree after `timeoutMs`, default 600,000, using a
  dedicated POSIX process group or absolute
  `taskkill.exe /T /F` argv from a drive-absolute local `%SystemRoot%` or
  `%WINDIR%` on Windows;
- retains the UTF-8 tail of stdout and stderr, default 1 MiB per stream;
- supports `maxOutputBytes: 0` for exit-code-only use.

`timeoutMs` must be an integer from 1 through 2,147,483,647.

The Windows helper path is resolved before the CLI starts. A missing, relative,
root-relative, UNC, or device system root fails with `runtime-unavailable`, so a
writable current directory, network share, device path, or `PATH` cannot replace
taskkill. The timeout result is not returned until Windows taskkill completes or
the POSIX group is confirmed absent. POSIX waits up to five seconds; `ESRCH`
means the group is gone. Permission failures, unavailable process IDs, a group
that remains present after that bounded wait, and Windows taskkill spawn or
nonzero-exit failures surface as the stable `termination-failed` category with
only sanitized system code, numeric exit code, and `process-group` or
`taskkill` method metadata. No termination path invokes a shell. A process that
deliberately leaves its inherited POSIX session is outside this containment
boundary, so the surrounding container or host process limit remains required.
`maxOutputBytes` must be a non-negative safe integer. Invalid values fail at
connector construction, including when an injected `exec` would ignore them.

The adapter does not sandbox the child. The CLI can read credentials, run
commands, and modify everything available to its process and `cwd`. Run it in
an appropriately scoped container or VM. `requiresApproval: false` removes the
human gate; use it only when another trusted boundary makes real execution
safe.

The child does not use `ConnectorRuntime.fetch`. Its provider egress list is
therefore enforced as a declaration against the organization policy, not as
socket-level interception. Apply host network controls for actual child
traffic. The adapter declares `egressEnforcement: 'declaration-only'`.
A deployment that sets `policies.requireEgressEnforcement` cannot register an
Agent CLI adapter: construction throws a `TypeError`. Put the child behind an
infrastructure boundary, or leave the flag off for that deployment.

### Know the CLI data boundary

`AgentCliOutput.text` is functional CLI output and can contain sensitive data.
`AgentCliOutput.command` is a diagnostic display string with the prompt
replaced by `<prompt:redacted>` and `--flag=value` option values replaced by
`<value:redacted>`.

Internally created `AgentCliError` values have static messages and structured
metadata:

- `code`, `connectorId`, and the redacted `command`;
- safe numeric values such as `exitCode` and `timeoutMs`;
- a validated operating-system `systemCode`;
- the fixed `process-group` or `taskkill` termination method when termination
  itself failed;
- booleans stating whether stdout or stderr contained data.

They do not copy the prompt, captured stdout or stderr, parser exceptions,
executor exceptions, or Mastra validation payloads into errors or audit
events. The error codes are:

```text
unknown
runtime-unavailable
codec-unavailable
flags-failed
invalid-flags
spawn-failed
timeout
termination-failed
exec-failed
invalid-exec-result
nonzero-exit
parse-output-failed
connector-failed
```

An injected `AgentCliExec` is responsible for sandboxing, process limits, and
bounded capture. It must return string `stdout`, string `stderr`, and a safe
integer `exitCode`. A custom `parseOutput` must return a string or `undefined`.

## Test the enforcement contract

Test through the returned Mastra tool, not by calling your inner
`execute()` function. Inject a vendor mock as `policies.fetch`; do not mock the
breakwater wrapper.

Cover the paths your manifest declares:

1. A valid call reaches the declared host and returns output matching the
   schema.
2. A declaration outside `networkEgress.allowedDomains` is denied before
   execution.
3. A `runtime.fetch` request or redirect to an undeclared host is denied before
   the vendor mock receives that hop.
4. A write-class call without a grant is denied, and the same call with a
   trusted grant executes.
5. A declared `requiredPermissions` list denies a call without a valid
   `breakwater.principalPermissions` projection — a valid grant included —
   and executes once the projection holds every identifier.
6. A dry run returns the simulation without approval, vendor calls,
   idempotency activity, or budget consumption.
7. The same idempotency key replays without a second side effect.
8. Concurrent same-key calls produce one execution.
9. Rate-limit exhaustion denies the next actual execution.
10. Store and evaluator failures fail closed and produce static audit reasons.
11. Tenant and workflow scopes cannot cross boundaries.
12. A no-schema or passthrough direct call whose arguments reach the wrapper
    cannot enable `_background` on a write connector, while an opted-in read
    connector follows the intended host behavior.
13. Audit and error surfaces do not contain prompt, credential, request-body,
    stdout, or stderr sentinels — nor the principal's effective permission
    set.

For an Agent CLI adapter, also pin:

- the exact argv array, including the final `--` and real prompt;
- the redacted display command;
- flag-shaped prompt and model inputs;
- real and dry-run `buildFlags` failures;
- executor rejection, malformed results, parser failures, timeout, process-tree
  termination failure, nonzero exit, and spawn failure;
- validation errors and legacy cached result sanitization;
- the default runner's UTF-8 byte cap.

The repository examples use `#given`, `#when`, and `#then` comments. See
[`agent-cli.test.ts`](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/src/agent-cli/agent-cli.test.ts)
and
[`connector-sdk.test.ts`](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/src/connector-sdk/connector-sdk.test.ts).

### Assert connector conformance

Run `assertConnectorConformance(factory, { manifest, cases, entryPoints? })` in your connector's test suite. It certifies the supplied cases for a connector declaring `egressEnforcement: 'enforced'`. A non-conformant run throws `ConnectorConformanceError`; its `.report` contains the evidence and findings.

Supply a synchronous factory that constructs a fresh connector using both members of `runtime.policies`: the inert `fetch` transport and the harness's `audit` logger. For example:

```typescript
import {
  assertConnectorConformance,
  type ConnectorConformanceFactory,
  createConnector,
} from '@proofoftech/breakwater/connector-sdk';

const manifest = {
  sideEffect: 'read',
  egress: ['api.vendor.example'],
  egressEnforcement: 'enforced',
} as const;
const factory: ConnectorConformanceFactory<unknown, { ok: boolean }> =
  (runtime) => createConnector({
  id: 'vendor.read',
  description: 'Read a vendor record',
  permissions: manifest,
  policies: runtime.policies,
  execute: async (_input, _context, { fetch }) => {
    const response = await fetch('https://api.vendor.example');
    return { ok: response.ok };
  },
});
```

Name the case and the outcome it must demonstrate:

```typescript
const report = await assertConnectorConformance(factory, {
  manifest,
  cases: [{
    name: 'reads the declared vendor',
    input: {},
    expect: {
      outcome: 'guarded-request',
      hosts: ['api.vendor.example'],
    },
  }],
});
```

When using `singleTenantConnectorPolicies`, pass the supplied members into the preset. Spreading them around an already validated preset fails its tamper checks. Replace the factory's `policies` value with:

```typescript
singleTenantConnectorPolicies({
  audit: { mode: 'production', logger: runtime.policies.audit },
  egress: { allowedDomains: ['api.vendor.example'] },
  permissions: { principalPermissions: 'not-configured' },
  fetch: runtime.policies.fetch,
})
```

Import `singleTenantConnectorPolicies` from the connector SDK. Use the production audit arm: the development arm drops `audit` and silences the decisions the harness measures. The supplied logger already has the external sink that production requires; pass it through.

Read failures using the report's finding codes:

- `NETWORK_IO_OUTSIDE_RUNTIME_FETCH`: a trapped entry point received a request, or the supplied base transport received an undeclared host. The attempt is recorded before refusal, including when the connector catches it. An attempt observed after its case has settled carries no `case` name: the reason names the case whose transport or trap it reached, and the finding is recorded at run level until the report is built and dropped after.
- `MANIFEST_MISMATCH`: the registered manifest differs from your claim, or the case's subject differs from the probe.
- `POSTURE_NOT_ENFORCED`: the connector declares a declaration-only posture, or its posture changes between the probe and a case.
- `SUBJECT_UNREGISTERED`: the factory returned an unregistered value, such as `undefined`, `null`, a plain Mastra tool, or a connector from a second copy of Breakwater.
- `CASE_EXPECTATION_UNMET`: the observed outcome, hosts, or decision code did not match the case, or the invocation never reached the connector's gate boundary.
- `CASE_INVOCATION_FAILED`: Invocation failed before or during invocation; the reason names the error's constructor, uses `unknown` when that name is unavailable or unreadable, or describes a thrown non-Error value by type, without the message or value. During invocation, policy denials, boundary errors, and harness refusals retain their existing classifications.
- `CASE_TIMEOUT`: the invocation exceeded its per-case bound. The run ends and names any skipped cases.
- `NO_TRANSPORT_EVIDENCE`: the connector declares egress, but no case called the harness's transport.
- `POLICIES_NOT_WIRED`: the report names `fetch`, `audit`, or `both`. A `fetch` finding cannot distinguish a factory that omitted `policies.fetch` from a connector calling ambient fetch directly for a declared host. Both leave an escape and zero harness transport calls. Read the escape record beside the finding for the authoritative request evidence. An `audit` finding means the subject reached its gate boundary without recording a witness on the supplied logger.
- `FACTORY_FAILED`: construction threw. For an `Error` with a string message, the reason is that message. An unreadable error becomes `unreadable error`; a non-Error object becomes `a non-Error object` or `null`. Other thrown values use their string representation.
- `INSTRUMENTATION_UNSUPPORTED`: an entry point could not be instrumented, or two entry points name the same property.
- `INSTRUMENTATION_REPLACED`: assignment to a harness-installed accessor was recorded without applying it, or the descriptor or effective value differs from the installed instrumentation, or could not be read, before restoration. When verification fails, the case proves `nothing` and skips expectation checks. Either finding during probe construction refuses the run.
- `INSTRUMENTATION_NOT_RESTORED`: restoration threw or its descriptor verification failed. The run ends and names any skipped cases.
- `RUN_OVERLAPPING`: another harness run owns instrumentation in this isolate.
- `ISOLATE_POISONED`: an earlier case timed out in this isolate; no further run is accepted.
- `NO_CASES`: the supplied case set is empty. This is one finding, without an additional absence-of-transport finding.

Choose expectations according to the path exercised:

| Expectation | Evidence checked |
| --- | --- |
| `guarded-request` with `hosts` | No subject denial, and the transport reached matching declared hosts |
| `guarded-denial` with `code` | A subject denial with `policyKind: 'egress-fetch'`, plus the expected subject decision code |
| `policy-denied` with `code` | A subject denial with another policy kind, plus the expected subject decision code |
| `no-network` | No subject denial and no allowed harness transport call |

A recorded denial takes precedence over a guarded request. An egress-fetch denial takes precedence over another denial. Without a denial, a case that reached an allowed host proves `guarded-request`; otherwise it proves `no-network`. An expected code must occur among the subject's witness events, independently of the outcome check.

`guarded-request` requires at least one host. Each host is validated at parse time using the manifest's hostname pattern; an empty list or malformed host throws `TypeError`. Matching uses the manifest's case-insensitive hostname and wildcard rules. Every expected host must match an observed host; extra observed hosts are allowed.

An egress-declaring connector needs an observed transport call somewhere in the run. The rule reads `transportCalls`, not an expectation's classification. A `guarded-request` case ordinarily supplies that evidence; a case that reaches a declared host and then gets denied on another call also supplies it. Denials before transport do not: an evaluator can emit an egress-fetch denial without making a request, and `EGRESS_HOST_NOT_DECLARED` occurs before the transport is called. `policy-denied` and `no-network` also supply no transport evidence by themselves.

Audit witnesses come from this case's supplied logger, during its invocation, with a `decisionCode` and `resource` equal to the subject connector's id. Setup logs, agent-policy records, and another connector's decisions cannot establish the subject's audit wiring or change its result. This attribution separates ordinary composition; it does not authenticate an arbitrary logger caller. `decisionCodes` preserves invocation-window events for diagnosis, including nested connector codes and `undefined` for events without a code.

Input-schema failures and `invokeConnector` pre-flight refusals have no expectation arm. Test them in ordinary connector tests. Supplied here, they report `proved: 'nothing'` with `CASE_EXPECTATION_UNMET`, whatever the case declared, because the gate boundary was never reached. Cases refused before invocation by instrumentation, factory construction, registration, posture, or manifest checks also prove `nothing`, as do cases with an invocation setup failure. These cases add no expectation or wiring failure. Any escape recorded during their setup still becomes `NETWORK_IO_OUTSIDE_RUNTIME_FETCH`.

The harness always instruments `globalThis.fetch`. Add other transports as `entryPoints: [{ label, target, property }]`. Entries are processed one at a time, global fetch first, so a supplied target's property callbacks run under the global trap. A consumer-supplied accessor, inherited accessor, locked data descriptor, ignored write, or effective property read that still returns the original causes refusal. A configurable entry point must start as a data property the harness can replace; the harness installs its own accessor in place of that data property. Each write is verified through both its own descriptor and its effective value; each restore is verified against the captured descriptor. A failed write or verification rolls back the stack including that attempted entry. A validation or descriptor-read failure occurs before capture, so rollback covers the preceding entries.

A duplicate `(target, property)` pair refuses the run before any case. Duplicate case names, duplicate labels, and the reserved labels `globalThis.fetch` and `policies.fetch` are malformed options and reject with `TypeError`, without a report. A supplied entry's property refusal is case-scoped. The global fetch descriptor is checked before probe construction and again for each case: an initially unsupported global refuses the run, while a global made unsupported mid-run refuses the affected case. A finding's `case` identifies the latter. Fix the target or options, stop earlier work redefining the global, or use a runtime whose global fetch is a writable or configurable data property.

Supplied targets are your own test fixtures. Verification detects concrete property mediation such as ignored writes and overriding getters; it does not defend against a target adapting to those checks. Supply no extra entry points when you need the guarantee for global fetch alone.

On an absent or configurable entry point, the harness installs an accessor whose getter returns the trap. An assignment to that instrumented entry point is recorded when it happens as `INSTRUMENTATION_REPLACED` and is not applied; the trap stays in place. A writable non-configurable data property uses assignment installation, which offers no defence against assignments during execution. A redefinition still in place when the case settles or times out, or when the probe factory returns, is reported as `INSTRUMENTATION_REPLACED`. A redefinition or deletion that the case itself reverses before it settles, like a reference to `fetch` captured before the run, is outside what the harness observes. It reports the requests that pass through its trap. Restoration follows invocation settlement or timeout. It restores entry points before clearing the timer, attempts the remaining restores after a failure, and ends a run whose restoration fails.

Each case has `timeoutMs`, defaulting to 2000 ms and accepting positive integers up to 2,147,483,647. Your test timeout must exceed `cases.length × timeoutMs` plus setup. Vitest defaults to 5000 ms per test, so three cases at the harness's default bound require a higher test timeout or lower per-case bounds. Cases must await their own work.

A timed-out case proves `nothing`, with no expectation check or `CASE_EXPECTATION_UNMET`. It ends the run and permanently refuses later runs in the isolate, preventing abandoned work from being attributed to another case. Once the case times out, its abandoned work runs against the restored global: a request it issues after the case ends is neither trapped nor recorded and leaves the process. Put a test that expects a timeout last in its file, or give it its own file. Vitest reuses a file's module graph, so later runs in that file receive `ISOLATE_POISONED`. The package's suite places its timeout test and then its poisoned-isolate assertion last. There is no reset API.

Use `respond(request)` to return `{ status?, headers?, body? }` for guarded traffic; the default is status 200 with an empty body. The request includes the exact URL, hostname, and uppercase method. The URL stays inside your test process and is not copied into harness-generated diagnostics, which use hostnames. `FACTORY_FAILED` is different: when a factory throws an `Error` with a readable string message, that message can contain URLs or other request data your code interpolated. String representations of other thrown values can also contain request data. Inspect those messages before sharing a report.

Every report states its limit:

> conformance covers only the supplied cases, in this isolate, for the duration of each case: it does not prove every reachable network path, a captured fetch reference, a request through an entry point the subject redefines and restores inside a case, an uninstrumented transport, another isolate, work continuing outside a case lifetime, a call the connector makes on the supplied base transport for a host the manifest already declares, or, for a connector declaring no egress, a transport the factory supplied in place of the harness's. A case that times out ends the run, because its abandoned work would otherwise be attributed to a later case, and no further run is accepted in this isolate; that abandoned work then runs against the RESTORED global, so a request it issues after the case ends is neither trapped nor recorded and leaves the process. A request that work abandoned by any settled case issues on the supplied base transport for a host the manifest does not declare, or through a trap reference it captured during that case, is recorded as a run-level finding naming that case until the report is built, and is dropped after. Entry-point targets you supply are your own test fixtures: each install is verified by its own descriptor, by an effective property read, and by the restore, against the mediations that verification names, and not against a target that adapts to those checks. Instrument globalThis.fetch alone for that guarantee.

## Contribute a connector

1. Add the implementation and tests under
   `packages/breakwater/src/<connector-name>/`.
2. Export the supported public surface through the intended package entry
   point.
3. Explain the worst-case side-effect classification, every egress host, the
   credential source, dry-run behavior, retry identity, and production store.
4. Run:

   ```bash
   pnpm --filter @proofoftech/breakwater lint
   pnpm --filter @proofoftech/breakwater typecheck
   pnpm --filter @proofoftech/breakwater test
   pnpm --filter @proofoftech/breakwater build
   pnpm --filter @proofoftech/breakwater test:packed-consumer
   ```

5. Follow the
   [repository contribution guide](https://github.com/ProofOfTechOrg/anchorage/blob/main/CONTRIBUTING.md)
   when opening the pull request.
