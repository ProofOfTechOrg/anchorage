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
import type { ToolExecutionContext } from '@mastra/core/tools';
import {
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
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

const result = await slackPoster.execute?.(
  {
    webhookPath: '/services/...',
    text: 'Incident 481 is being investigated.',
  },
  { requestContext } as ToolExecutionContext,
);
```

This example shows the values a trusted runtime must produce. Application routes must not construct them from client data. Do not accept `CONNECTOR_GRANTS_CONTEXT_KEY`, `CONNECTOR_EXECUTION_CONTEXT_KEY`, `PRINCIPAL_PERMISSIONS_CONTEXT_KEY`, `ISOLATION_SCOPE_CONTEXT_KEY`, `runId`, or `WORKFLOW_SCOPE_CONTEXT_KEY` from clients. A multi-tenant host should also register `tenantIsolation()` so a missing scope becomes a denial instead of a shared cache or budget.

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

## Handle errors and audit safely

Policy denials throw `ConnectorPolicyError` with `connector`, `policy`, and
`reason`. Standalone `egressFetch()` throws `EgressDeniedError` by default or
uses the caller's `denied()` mapper.

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
  dedicated POSIX process group or direct `taskkill.exe /T /F` argv on Windows;
- retains the UTF-8 tail of stdout and stderr, default 1 MiB per stream;
- supports `maxOutputBytes: 0` for exit-code-only use.

`timeoutMs` must be an integer from 1 through 2,147,483,647.

The timeout result is not returned until Windows taskkill completes or the
POSIX group is confirmed absent. POSIX waits up to five seconds; `ESRCH` means
the group is gone. Permission failures, unavailable process IDs, a group that
remains present after that bounded wait, and Windows taskkill spawn or
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
traffic.

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
