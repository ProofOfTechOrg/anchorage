# Connector interface

A breakwater connector is a Mastra tool whose safety declaration is enforced in the returned tool's execution path.

Use `createConnector()` instead of `createTool()` for code that can reach a network, change state, require human review, need replay protection, or consume a shared budget.

## Basic connector

```typescript
import { createConnector } from '@proofoftech/breakwater/connector-sdk';
import { z } from 'zod';

export const createContact = createConnector({
  id: 'salesforce.create-contact',
  description: 'Create one Salesforce contact',
  inputSchema: z.object({
    name: z.string(),
    email: z.string().email(),
  }),
  outputSchema: z.object({
    id: z.string(),
    simulated: z.boolean().optional(),
  }),
  permissions: {
    sideEffect: 'write',
    egress: ['api.salesforce.com'],
    requiresApproval: true,
    idempotencyKey: true,
    dryRun: true,
    rateLimit: '100/min',
  },
  execute: async (input, _context, runtime) => {
    const response = await runtime.fetch(
      'https://api.salesforce.com/services/data/contacts',
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(input),
      },
    );
    if (!response.ok) {
      throw new Error(`Salesforce returned ${response.status}`);
    }
    const body = (await response.json()) as { id: string };
    return { id: body.id };
  },
  dryRunExecute: async () => ({
    id: 'not-created',
    simulated: true,
  }),
});
```

`execute` and `dryRunExecute` receive `ConnectorRuntime` as their third argument. Its fetch is bound to the manifest's declared hosts.

## Permission manifest

```typescript
interface PermissionManifest {
  sideEffect: 'read' | 'write' | 'destructive' | 'idempotent';
  egress?: readonly string[];
  idempotencyKey?: boolean;
  requiresApproval?: boolean;
  dryRun?: boolean;
  rateLimit?: string;
  background?: boolean;
  requiredPermissions?: readonly Permission[];
}
```

### `sideEffect`

Classify by the strongest possible effect:

| Value | Meaning |
| --- | --- |
| `read` | No external or durable state change |
| `write` | Creates or updates state |
| `destructive` | Deletes or irreversibly changes state |
| `idempotent` | A write-class effect that is safe to repeat according to the connector's domain contract |

Destructive connectors require approval by default. Deployment write-policy patterns can require approval for any write-class connector.

The classification also produces truthful MCP annotations. Those annotations are descriptive; the wrapper is the enforcement.

### `egress`

List bare hostnames the connector may contact. An omitted or empty list gives `runtime.fetch` no network permission.

Hostnames:

- may use an exact name such as `api.example.com`;
- may use a leading wildcard such as `*.googleapis.com`;
- may not include a scheme, port, path, or allow-all token;
- are normalized for case and one trailing dot;
- use label-boundary wildcard matching, and a wildcard does not include its apex.

Declare redirect and regional hosts. Actual redirect hops must also remain within the list.

### `idempotencyKey`

When `true`, every real call needs a non-empty key in `breakwater.idempotencyKey`. The wrapper stores the successful result and replays it on another call with the same scoped key.

The application chooses the business key. Flowsafe supplies the trusted tenant isolation scope but does not invent a connector-specific idempotency key.

### `requiresApproval`

When `true`, every real call needs a structured grant in `breakwater.connectorGrants`. Breakwater compares the grant with the runtime-owned `breakwater.connectorExecution` identity and the connector's actual Mastra `toolCallId` when the grant uses `tool-call` scope.

The same decision is compiled into Mastra's native `requireApproval` predicate so an agent can suspend. The native approval signal never replaces the request-context capability.

### `dryRun`

When `true`, `dryRunExecute` is required. A caller sets `breakwater.dryRun` to `true`.

Dry-run:

- runs declaration egress and custom evaluators;
- runs the `requiredPermissions` authorization gate;
- uses the same egress-bound runtime fetch;
- skips the approval grant;
- skips Mastra's approval pause on the standard agent path;
- skips idempotency;
- skips rate-budget consumption;
- audits the simulation;
- never falls back to real execution.

Declaring `dryRun` without an implementation, or providing an implementation without the declaration, fails at construction.

### `rateLimit`

Use `<count>/<unit>`, where count is positive and unit is a supported singular second, minute, hour, or day spelling such as `100/min`.

The store atomically increments an epoch-aligned fixed window. Only a real, non-replayed execution consumes one count.

Fixed windows can admit a burst near twice the nominal count across adjacent windows. A hard rolling cap requires a different store contract.

### `background`

The default is foreground-only. `background: true` is allowed only for a read-only connector.

A write, destructive, or idempotent-write connector cannot opt into Mastra background execution because moving it off the foreground path changes its timing and approval topology.

The wrapper also rejects direct or passthrough `_background` overrides on connectors that did not opt in. Core may strip that field on schema-controlled agent paths; its own tool eligibility keeps the connector foreground there.

### `requiredPermissions`

Authorization and approval answer different questions: authorization asks whether this principal may ever invoke the connector, approval asks whether this particular proposed call may proceed now.

A declared list uses all-of semantics over canonical `Permission` identifiers (lowercase dotted form). Construction rejects a non-array, an empty list, duplicates, and malformed identifiers.

Every real call and every dry-run then requires the trusted `breakwater.principalPermissions` projection — `{ permissions, policyVersion }`, validated by `isPrincipalPermissions()` — to contain every listed identifier. The gate runs before the dry-run branch and before the approval grant, so a valid approval cannot elevate a principal that is not authorized to invoke the connector at all. A missing, `null`, or malformed projection fails closed.

Only trusted host or runtime code may mint the projection. Flowsafe's agent thread host derives it from its configured `PrincipalPermissionResolver` on every start and resume leg; a workflow host may return the key from its own `RequestContextProvider`. A path that mints no projection denies every permission-declaring connector.

Authorization audit records the required identifiers and the resolution's `policyVersion`, never the principal's effective permission set. Avoid hard-coding human role names into manifests; permissions keep the connector reusable across hosts and service principals.

## Deployment policy

```typescript
interface ConnectorPolicies {
  networkEgress?: NetworkEgressOptions;
  writePermissions?: WritePermissionsPolicy;
  evaluators?: readonly ToolPolicyEvaluator[];
  idempotencyStore?: IdempotencyStore;
  rateLimitStore?: RateLimitStore;
  audit?: AuditLogger;
  fetch?: EgressFetchBase;
}
```

The connector definition binds these values once. `fetch` is the underlying HTTP implementation wrapped by guarded fetch, and is the normal test seam.

## Execution order

```text
schema validation
  -> declared egress against organization allowlist
  -> custom evaluators in registration order
  -> required permissions against the trusted projection
  -> dry-run branch
  -> approval grant
  -> idempotency reservation or replay
  -> rate-limit increment for a new attempt
  -> connector execute with guarded fetch
  -> output validation
  -> idempotency commit
```

A denial throws `ConnectorPolicyError` with connector id, policy name, and reason. Every gate records its decision through the supplied audit logger.

An arbitrary execution, store, evaluator, or parser throw is not copied verbatim into audit. Safe built-in errors can register a static reason and bounded metadata.

## Network egress

Egress has two nested policies:

```text
actual request host <= connector declaration <= deployment allowlist
```

### Declaration gate

`networkEgress({ allowedDomains })` compares every manifest host with the deployment list before execution. It catches a connector that declares more access than deployment policy permits.

Omit the policy when the deployment intentionally does not restrict declarations. An empty allowlist denies every declared host.

### Runtime fetch

`runtime.fetch`:

- accepts HTTP and HTTPS only;
- rejects invalid URLs;
- checks the initial request and every redirect hop;
- follows redirects manually;
- strips authorization, cookie, and proxy credential headers on a cross-origin redirect;
- rewrites method/body according to redirect status semantics;
- refuses a 307/308 that would require replaying a one-shot stream body;
- enforces a bounded redirect count;
- reports the hostname, not a full query-bearing URL, in denial audit.

The base fetch never runs for a denied hop.

### Residual boundary

The wrapper does not intercept:

- global `fetch`;
- sockets;
- a vendor SDK's private transport;
- the child process used by an Agent CLI connector.

Inject `runtime.fetch` into compatible SDKs. Apply infrastructure network policy for process-wide enforcement.

## Approval context

`breakwater.connectorGrants` is a structured capability array in Mastra's `RequestContext`. `breakwater.connectorExecution` identifies the current runtime leg. `breakwater.principalPermissions` carries the executing principal's server-resolved permissions for the `requiredPermissions` gate. Whoever can write any of these values can affect authorization, so only trusted runtime code may populate them.

Flowsafe's approval provider reads approved D1 records and derives grants for each runtime leg, and its agent thread host projects the permission resolution the same way. A public resume body, signal, model output, workflow input, tool result, schedule row, or background task cannot supply any of these keys.

Breakwater supports three explicit scopes:

- `tool-call`: connector, tenant, workflow, run, exact suspension, and Mastra `toolCallId`
- `suspension`: connector, tenant, workflow, run, and exact suspension
- `run`: connector, tenant, workflow, and run for a trusted standing grant

Durable-agent approvals use `tool-call` scope because Mastra persists and reproduces `toolCallId` through retry and reconstruction. Workflow gates use `suspension` scope because an arbitrary workflow suspension has no reproducible tool-call identity.

Grant checks run on every attempt. A retry of the same durable tool call keeps the same identity and remains authorized. A new model tool call receives a new ID and requires approval. Use `breakwater.idempotencyKey` when retrying a side effect must replay instead of execute again.

Legacy connector ID arrays and malformed structured grants fail closed. Breakwater does not hash inputs or consume one-shot nonces: canonical serialization, redaction, retry, and atomic consumption semantics are not established at this boundary.

The dry-run branch is the only approval exemption because its separate implementation is required to have no side effect.

## Idempotency

The effective key is:

```text
[isolationScope:]connectorId:idempotencyKey
```

The connector id cannot contain the internal tuple delimiter. The isolation prefix appears when the trusted host sets `breakwater.isolationScope`.

### In-memory store

`InMemoryIdempotencyStore` supports atomic reservation and same-isolate in-flight joining. It is bounded and evictable. It does not protect two Worker isolates or two per-run Durable Objects.

### D1 store

`D1IdempotencyStore` claims a key through an atomic insert. A reservation carries a lease token:

- `reserved`: this caller executes;
- `replay`: return the stored result;
- `pending`: another owner still executes.

A stale pending row can be taken over after `pendingTtlMs`. Token-guarded commit and release stop a stale holder from overwriting or deleting the new lease.

Set the TTL above the longest connector or Agent CLI timeout.

Execution failures release the reservation. A successful side effect followed by a failed result-store write returns the result and audits degraded replay protection; throwing at that point would invite a duplicate retry.

## Rate-limit store

`InMemoryRateLimitStore` is per isolate. In a one-Durable-Object-per-run host, that is effectively per run.

`D1RateLimitStore` shares the window across isolates. Use it for per-tenant or deployment-wide enforcement.

Both idempotency and rate keys include the isolation scope when present. Add `tenantIsolation()` to every multi-tenant connector so a missing scope becomes a denial rather than silently shared storage.

## Workflow isolation

Flowsafe writes the current workflow id to `breakwater.workflowScope`.

Register:

```typescript
crossWorkflowIsolation({
  targetScopeOf: ({ input }) => {
    if (
      typeof input === 'object' &&
      input !== null &&
      'workflowId' in input
    ) {
      return String(input.workflowId);
    }
    return undefined;
  },
});
```

No target means the call is not accessing workflow state. A target with a missing or different caller scope is denied.

The target extractor must identify a server-relevant state namespace. It must not transform a client value into trusted caller scope.

## Direct, workflow, and agent calls

The wrapped `execute` is the common enforcement point:

- direct application call;
- Mastra workflow step;
- nested tool call;
- ordinary agent tool call;
- durable-agent tool call;
- idempotent replay;
- dry-run.

Mastra input/output processors are complementary and do not replace this wrapper.

## Authoring and tests

Follow the [connector authoring guide](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md).

At minimum, test:

1. permitted execution;
2. declaration egress denial;
3. actual fetch denial, including redirect;
4. required-permission denial without a valid projection, where declared;
5. approval denial and grant success;
6. dry-run with zero side effects;
7. idempotent replay and concurrent callers;
8. rate-limit boundary;
9. missing tenant scope in a multi-tenant host;
10. workflow target mismatch where relevant;
11. audit safety for arbitrary throws;
12. packed npm consumer behavior.

For Agent CLIs, also read [Agent CLI connectors](agent-cli-connectors.md).
